/**
 * ZulipMcplServer — the JSON-RPC main loop over an `McplConnection`.
 *
 * Speaks plain MCP to any client (initialize, tools/*, resources/*) and MCPL
 * 0.5 to hosts that advertise `experimental.mcpl` in their initialize
 * capabilities: featureSets/update (the §5.3 policy exchange), mcpl/manifest,
 * channels/*, and context/beforeInference.
 *
 * One connection at a time. `serve()` resolves when the peer disconnects.
 */

import {
  ManifestTracker,
  McplConnection,
  method,
  type ChannelsCloseParams,
  type ChannelsIncomingResult,
  type ChannelsOpenParams,
  type ChannelsPublishParams,
  type ChannelsRegisterResult,
  type ChannelDescriptor,
  type ContextBeforeInferenceParams,
  type FeatureSetsUpdateParams,
  type IncomingChannelMessage,
  type InitializeCapabilities,
  type JsonRpcNotification,
  type JsonRpcRequest,
  type McplInitializeParams,
  type McplInitializeResult,
} from '@animalabs/mcpl-core';
import { ChannelManager, type HostClient } from './channels.js';
import { ContextProvider } from './context.js';
import { McplRpcError, capabilityDenied } from './errors.js';
import { buildServerCapabilities, featureSetForTool } from './feature-sets.js';
import { CapabilityGrant } from './grant.js';
import type { PlatformAdapter } from './platforms/adapter.js';
import { toolDefinitions } from './tools.js';
import { toToolCallResult, type ZulipToolRuntime } from './tool-runtime.js';

/** MCP protocol revisions this server answers with verbatim. Anything else
 *  is answered with the oldest, which every client can speak. */
const KNOWN_MCP_PROTOCOL_VERSIONS = new Set(['2024-11-05', '2025-03-26', '2025-06-18']);
const FALLBACK_MCP_PROTOCOL_VERSION = '2024-11-05';

export interface ZulipMcplServerOptions {
  /** Server name/version reported in `initialize`. */
  serverInfo: { name: string; version: string };
  /** `false` forces plain-MCP mode even for hosts that advertise MCPL. */
  mcplEnabled?: boolean;
  /** channels/incoming batching window. */
  batchWindowMs?: number;
  /** Messages injected per open channel on context/beforeInference. */
  contextHistorySize?: number;
}

export class ZulipMcplServer {
  private conn: McplConnection | null = null;
  private mcplActive = false;

  readonly adapters: Map<string, PlatformAdapter>;
  readonly grant: CapabilityGrant;
  readonly manifestTracker: ManifestTracker;
  readonly channelManager: ChannelManager;
  readonly contextProvider: ContextProvider;

  private detachManifest: (() => void) | null = null;
  private eventsStarted = false;

  constructor(
    private readonly adapter: PlatformAdapter,
    private readonly tools: ZulipToolRuntime,
    private readonly options: ZulipMcplServerOptions,
  ) {
    this.adapters = new Map([[adapter.type, adapter]]);

    // Derived from the adapter rather than restated, so `channels.typing`
    // cannot be advertised when the adapter does not implement it (§6.4).
    const manifest = buildServerCapabilities({ typing: typeof adapter.sendTyping === 'function' });

    // The manifest is what `initialize` presents (§5.1) and what `mcpl/manifest`
    // returns (§17.4). Building it through the tracker stamps the canonical
    // content digest (§17.2) onto the same snapshot both paths serve.
    this.manifestTracker = new ManifestTracker(manifest);

    // The effective capability grant for this connection (§5.4). It starts
    // empty: until the initial policy exchange completes, every
    // capability-dependent behavior is unavailable (§5.3).
    this.grant = new CapabilityGrant(
      typeof manifest.featureSets === 'object' ? manifest.featureSets : {},
    );

    const host: HostClient = {
      registerChannels: (channels) => this.registerChannelsWithHost(channels),
      sendIncoming: (messages) => this.sendIncomingToHost(messages),
    };
    this.channelManager = new ChannelManager(host, this.adapters, this.grant, options.batchWindowMs);
    this.contextProvider = new ContextProvider(this.channelManager, this.grant, options.contextHistorySize);
  }

  /** True when the connected peer negotiated MCPL. */
  get mcplMode(): boolean {
    return this.mcplActive;
  }

  // ── Serve loop ──

  async serve(conn: McplConnection): Promise<void> {
    this.conn = conn;

    try {
      await this.run(conn);
    } catch (err) {
      if ((err as Error).name !== 'ConnectionClosedError') {
        console.error('[zulip-mcp] Connection error:', err);
      }
    }

    this.detachManifest?.();
    this.detachManifest = null;
    this.conn = null;
  }

  private async run(conn: McplConnection): Promise<void> {
    const ok = await this.handleInitialize(conn);
    if (!ok) return;

    if (this.mcplActive) {
      // Seeded from the handshake so a fresh connection does not fire a
      // redundant `mcpl/manifestChanged` (§17.10).
      this.detachManifest = this.manifestTracker.attach(conn);

      // §5.3: registration waits for the initial policy exchange, not for a
      // timer. Until `featureSets/update` arrives the grant is empty and
      // `channels.register` is denied, so registering earlier would be acting
      // on a capability nobody has granted yet. A host that never sends it
      // leaves this server inert by design — absence is denial.
      //
      // Runs concurrently with the loop: the policy arrives as a Request the
      // loop must read and answer, and channels/register is itself a
      // server→host Request the host answers only once policy is settled.
      void this.grant.whenReady().then(async () => {
        if (this.conn !== conn) return;
        try {
          await this.channelManager.registerChannels();
        } catch (error) {
          console.error('Failed to register channels after initial policy:', error);
        }
      });

      this.startEvents();
    }

    while (!conn.isClosed) {
      const msg = await conn.nextMessage();
      if (msg.type === 'request') {
        await this.handleRequest(conn, msg.request);
      } else {
        await this.handleNotification(conn, msg.notification);
      }
    }
  }

  /** Stop platform event delivery. Idempotent. */
  shutdown(): void {
    if (this.eventsStarted) {
      this.adapter.stopEvents();
      this.eventsStarted = false;
    }
    this.channelManager.destroy();
  }

  // ── Handshake ──

  private async handleInitialize(conn: McplConnection): Promise<boolean> {
    const msg = await conn.nextMessage();
    if (msg.type !== 'request' || msg.request.method !== method.INITIALIZE) {
      console.error('[zulip-mcp] Expected initialize request, got:', msg);
      conn.close();
      return false;
    }

    const params = msg.request.params as McplInitializeParams | undefined;
    const clientMcpl = params?.capabilities?.experimental?.mcpl;
    this.mcplActive = clientMcpl !== undefined && this.options.mcplEnabled !== false;

    const requested = params?.protocolVersion;
    const protocolVersion =
      typeof requested === 'string' && KNOWN_MCP_PROTOCOL_VERSIONS.has(requested)
        ? requested
        : FALLBACK_MCP_PROTOCOL_VERSION;

    const capabilities: InitializeCapabilities = {
      tools: {},
      resources: {},
      ...(this.mcplActive ? { experimental: { mcpl: this.manifestTracker.snapshot() } } : {}),
    };

    const result: McplInitializeResult = {
      protocolVersion,
      capabilities,
      serverInfo: this.options.serverInfo,
    };
    conn.sendResponse(msg.request.id, result);

    // The client's `notifications/initialized` (or, from a lax client, its
    // first request) follows. Anything that is not the initialized
    // notification is handed to the main loop untouched.
    const next = await conn.nextMessage();
    if (next.type === 'notification' && next.notification.method === 'notifications/initialized') {
      console.error(`[zulip-mcp] Client initialized (${this.mcplActive ? 'MCPL' : 'MCP'} mode)`);
    } else if (next.type === 'request') {
      await this.handleRequest(conn, next.request);
    } else {
      await this.handleNotification(conn, next.notification);
    }
    return true;
  }

  // ── Requests ──

  private async handleRequest(conn: McplConnection, req: JsonRpcRequest): Promise<void> {
    const params = (req.params ?? {}) as Record<string, unknown>;
    try {
      switch (req.method) {
        case 'ping':
          conn.sendResponse(req.id, {});
          break;

        case 'tools/list':
          conn.sendResponse(req.id, { tools: toolDefinitions });
          break;

        case 'tools/call': {
          const name = String(params.name ?? '');
          const args = (params.arguments ?? {}) as Record<string, unknown>;
          conn.sendResponse(req.id, await this.callTool(name, args));
          break;
        }

        case 'resources/list':
          conn.sendResponse(req.id, { resources: this.tools.listResources() });
          break;

        case 'resources/read': {
          const uri = String(params.uri ?? '');
          try {
            conn.sendResponse(req.id, await this.tools.readResource(uri));
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            throw new McplRpcError(-32602, `Failed to read resource: ${message}`);
          }
          break;
        }

        case 'prompts/list':
          conn.sendResponse(req.id, { prompts: [] });
          break;

        case method.FEATURE_SETS_UPDATE: {
          // §6.7: featureSets/update is a Request carrying the effective grant,
          // and its response is a degradation receipt — what this server WILL
          // DO under the grant it was given. It is testimony about
          // consequences, never a claim of entitlement, and it asks for
          // nothing. Only this form can establish a ready state.
          this.requireMcpl();
          const receipt = this.grant.apply(params as unknown as FeatureSetsUpdateParams, 'request');
          conn.sendResponse(req.id, receipt);
          break;
        }

        case method.MCPL_MANIFEST:
          // §17.4: the complete current manifest, never a delta, in the same
          // shape initialize carries. Not gated on any capability path.
          this.requireMcpl();
          conn.sendResponse(req.id, this.manifestTracker.handleManifestRequest());
          break;

        case method.CONTEXT_BEFORE_INFERENCE: {
          this.requireMcpl();
          const result = await this.contextProvider.handleBeforeInference(
            params as unknown as ContextBeforeInferenceParams,
          );
          conn.sendResponse(req.id, result);
          break;
        }

        case method.CHANNELS_LIST:
          this.requireMcpl();
          conn.sendResponse(req.id, this.channelManager.listChannels());
          break;

        case method.CHANNELS_OPEN: {
          this.requireMcpl();
          const open = params as unknown as ChannelsOpenParams;
          conn.sendResponse(req.id, this.channelManager.openChannel(open));
          break;
        }

        case method.CHANNELS_CLOSE: {
          this.requireMcpl();
          const close = params as unknown as ChannelsCloseParams;
          conn.sendResponse(req.id, this.channelManager.closeChannel(close));
          break;
        }

        case method.CHANNELS_PUBLISH: {
          this.requireMcpl();
          const publish = params as unknown as ChannelsPublishParams;
          conn.sendResponse(req.id, await this.channelManager.publish(publish));
          break;
        }

        case method.CHANNELS_TYPING: {
          this.requireMcpl();
          await this.typing(params);
          conn.sendResponse(req.id, {});
          break;
        }

        default:
          conn.sendError(req.id, -32601, `Method not found: ${req.method}`);
      }
    } catch (err) {
      if (err instanceof McplRpcError) {
        conn.sendError(req.id, err.code, err.message, err.data);
        return;
      }
      const e = err as Error;
      console.error(`[zulip-mcp] handleRequest error: method=${req.method}`, e.stack ?? e.message);
      conn.sendError(req.id, -32603, e.message ?? String(err));
    }
  }

  // ── Notifications ──

  private async handleNotification(conn: McplConnection, notif: JsonRpcNotification): Promise<void> {
    const params = (notif.params ?? {}) as Record<string, unknown>;
    try {
      switch (notif.method) {
        case 'notifications/initialized':
        case 'notifications/cancelled':
        case 'notifications/roots/list_changed':
          break;

        case method.FEATURE_SETS_UPDATE:
          // §6.7 Notification form: descriptive metadata only. Grant-bearing
          // updates (including the §5.3 initial policy) arrive as a Request.
          if (this.mcplActive) this.grant.apply(params as unknown as FeatureSetsUpdateParams, 'notification');
          break;

        case method.CHANNELS_TYPING:
        case 'notifications/typing':
          if (this.mcplActive) await this.typing(params);
          break;

        default:
          break;
      }
    } catch (err) {
      // Notifications cannot be answered; a failing one is logged, never fatal.
      const e = err as Error;
      console.error(`[zulip-mcp] notification ${notif.method} failed:`, e.message ?? String(err));
    }
    void conn;
  }

  // ── Helpers ──

  private requireMcpl(): void {
    if (!this.mcplActive) {
      throw new McplRpcError(-32601, 'MCPL is not negotiated on this connection');
    }
  }

  private async typing(params: Record<string, unknown>): Promise<void> {
    const channelId = typeof params.channelId === 'string' ? params.channelId : undefined;
    if (!channelId) throw new McplRpcError(-32602, 'channels/typing requires channelId');
    const metadata =
      typeof params.metadata === 'object' && params.metadata !== null
        ? (params.metadata as Record<string, unknown>)
        : undefined;
    const op = params.op === 'stop' ? 'stop' : 'start';
    await this.channelManager.sendTyping(channelId, metadata, op);
  }

  /**
   * Execute a tool for `tools/call`. In MCPL mode the tool surface is gated:
   * `tools` must be in the effective grant (§14.1 / §6.2), and a tool owned
   * by a feature set the host disabled is unavailable with it (§6.7). Plain
   * MCP clients are not subject to a grant — there is none to consult.
   */
  private async callTool(name: string, args: Record<string, unknown>) {
    if (this.mcplActive) {
      if (!this.grant.has('tools')) throw capabilityDenied('tools');
      const owner = featureSetForTool(name);
      if (owner && !this.grant.isFeatureSetActive(owner)) {
        return {
          content: [{ type: 'text' as const, text: `Feature set '${owner}' is not enabled` }],
          isError: true,
        };
      }
    }
    try {
      return toToolCallResult(await this.tools.handleToolCall(name, args));
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return { content: [{ type: 'text' as const, text: `Error: ${message}` }], isError: true };
    }
  }

  private startEvents(): void {
    if (this.eventsStarted) return;
    this.eventsStarted = true;
    this.adapter.startEvents(
      (message) => {
        this.channelManager.onIncomingMessage(message.channelId, message);
      },
      (event) => {
        // Delivery gaps / degraded polling: surface to the agent as a
        // synthetic system message on the platform's open channels.
        this.channelManager.broadcastSystemEvent(this.adapter.type, event);
      },
    );
  }

  // ── Server → host ──

  private async registerChannelsWithHost(channels: ChannelDescriptor[]): Promise<ChannelsRegisterResult | undefined> {
    const conn = this.conn;
    if (!conn) throw new Error('not connected');
    return (await conn.sendRequest(method.CHANNELS_REGISTER, { channels })) as ChannelsRegisterResult | undefined;
  }

  private async sendIncomingToHost(messages: IncomingChannelMessage[]): Promise<ChannelsIncomingResult | undefined> {
    const conn = this.conn;
    if (!conn) throw new Error('not connected');
    return (await conn.sendRequest(method.CHANNELS_INCOMING, { messages })) as ChannelsIncomingResult | undefined;
  }
}
