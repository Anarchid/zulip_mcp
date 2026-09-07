/**
 * ZulipMcplServer — the JSON-RPC main loop over an `McplConnection`.
 *
 * Speaks plain MCP to any client (initialize, tools/*, resources/*) and MCPL
 * 0.5 to hosts that advertise `experimental.mcpl` in their initialize
 * capabilities: featureSets/update (the §5.3 policy exchange), mcpl/manifest,
 * channels/*, push/event, and context/beforeInference.
 *
 * Delivery model (per message the adapter hands over):
 *   - channel open by the host  → channels/incoming (batched)
 *   - channel closed, addressed → push/event with the closed-channel origin,
 *                                  carrying the missed-ambient tally so the
 *                                  host can show what staying out has cost
 *   - channel closed, ambient   → dropped and tallied (`channel_missed`)
 *
 * Every forward the host accepts advances a persisted per-channel
 * watermark; what the host was offered but has not accepted holds the
 * watermark below it (the undelivered floor) and is offered again — by a
 * live replay from history once the host answers, by the catch-up sweep on
 * the next connection, or by the gap recovery a Zulip event-queue expiry
 * triggers. Nothing is lost to a later acceptance.
 *
 * One connection at a time. `serve()` resolves when the peer disconnects;
 * every connection starts from a fresh grant, registration and sweep.
 */

import {
  ERR_CHANNEL_OPEN_FAILED,
  ManifestTracker,
  McplConnection,
  method,
  type ChannelsChangedParams,
  type ChannelsCloseParams,
  type ChannelsIncomingResult,
  type ChannelsOpenParams,
  type ChannelsOpenResult,
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
  type PushEventParams,
  type StateRollbackParams,
  type StateRollbackResult,
  type TextContent,
} from '@animalabs/mcpl-core';
import { ChannelManager, type HostClient } from './channels.js';
import { ContextProvider } from './context.js';
import { DeliveryState, renderMissedBlock, selectMissed, viewOf, DEFAULT_MISSED_BLOCK_MAX_CHARS } from './delivery.js';
import { McplRpcError, capabilityDenied } from './errors.js';
import { MESSAGING_FEATURE_SET, buildServerCapabilities, featureSetForTool } from './feature-sets.js';
import { isDmChannelId } from './history.js';
import type { FiltersPlane } from './filters.js';
import { buildAttachmentBlocks, type AttachmentSource, type InlineOptions } from './attachments.js';
import type { AttachmentRef } from './content.js';
import { formatAgentDateTime, resolveAgentTimeZone, resolveTimestampStyle } from './timezone.js';
import { CapabilityGrant } from './grant.js';
import { StateTracker } from './state.js';
import type { PlatformAdapter, PlatformSystemEvent, ReactionEvent } from './platforms/adapter.js';
import { CHAT_TAGS } from '@animalabs/mcpl-core';
import type { ReactionSummary } from './history.js';
import { toolDefinitions } from './tools.js';
import { toToolCallResult, type ToolCallResult, type ZulipToolRuntime } from './tool-runtime.js';

/** MCP protocol revisions this server answers with verbatim. Anything else
 *  is answered with the oldest, which every client can speak. */
const KNOWN_MCP_PROTOCOL_VERSIONS = new Set(['2024-11-05', '2025-03-26', '2025-06-18']);
const FALLBACK_MCP_PROTOCOL_VERSION = '2024-11-05';

/** Messages kept around each mention in a catch-up block for a closed channel. */
const MISSED_VICINITY = 7;
/** Hard ceiling on what one channel's catch-up fetches. */
const CATCHUP_HARD_CAP = 10_000;
export const DEFAULT_CATCHUP_LIMIT = 3000;
const HISTORY_ON_OPEN_CAP = 500;
/** Live events received before the catch-up sweep has run are held, so the
 *  sweep's "everything after the watermark" is not pre-empted by a live
 *  delivery that would jump the watermark over the offline gap. Beyond the
 *  cap the oldest are dropped from the buffer but stay held below the
 *  watermark, so the replay after go-live recovers them from history. */
const PRE_LIVE_BUFFER_CAP = 5000;
/** How long shutdown waits for deliveries still building their content
 *  (attachment fetches) before flushing what has been queued. */
const SHUTDOWN_INFLIGHT_GRACE_MS = 5000;

export interface ZulipMcplServerOptions {
  /** Server name/version reported in `initialize`. */
  serverInfo: { name: string; version: string };
  /** `false` forces plain-MCP mode even for hosts that advertise MCPL. */
  mcplEnabled?: boolean;
  /** channels/incoming batching window. */
  batchWindowMs?: number;
  /** Messages injected per open channel on context/beforeInference. */
  contextHistorySize?: number;
  /** Where delivery state (watermarks, tallies) persists. null = in-memory. */
  stateDir?: string | null;
  /** Session id the delivery state file is keyed by. */
  sessionId?: string;
  /** Per-channel ceiling for the reconnect sweep and gap recovery. */
  catchupLimit?: number;
  /** Renders timestamps in agent-visible catch-up lines. Default: AGENT_TIMEZONE / AGENT_TIMESTAMP_STYLE. */
  formatTime?: (d: Date) => string;
  /** The filters plane (stream/DM allowlists, mutes, reaction policy). Optional: without it nothing is filtered. */
  filters?: FiltersPlane;
  /** Where attachment bytes come from, and how much of them to inline on live delivery. */
  attachments?: { source: AttachmentSource; inline: InlineOptions };
  /** Size cap (characters) on one `<missed>` catch-up block; the oldest lines are elided. */
  missedBlockMaxChars?: number;
}

export class ZulipMcplServer {
  private conn: McplConnection | null = null;
  private mcplActive = false;

  readonly adapters: Map<string, PlatformAdapter>;
  readonly grant: CapabilityGrant;
  readonly manifestTracker: ManifestTracker;
  readonly channelManager: ChannelManager;
  readonly contextProvider: ContextProvider;
  readonly delivery: DeliveryState;
  readonly stateTracker = new StateTracker();

  private detachManifest: (() => void) | null = null;
  private eventsStarted = false;
  private sweepDone = false;
  /** Live delivery is gated until registration and the catch-up sweep are done. */
  private live = false;
  private preLive: { message: IncomingChannelMessage; newChannel?: ChannelDescriptor }[] = [];
  private preLiveDropped = 0;
  /** Watermarks as they stood when the connection began: what the sweep
   *  catches up from, whatever moves them meanwhile (a channel_open with
   *  backscroll in the first seconds must not hide the offline gap). */
  private sweepAnchors = new Map<string, number>();
  /** Deliveries still building their content, so shutdown can wait for them. */
  private inFlight = new Set<Promise<void>>();
  private healing = false;
  private readonly catchupLimit: number;
  private readonly missedBlockMaxChars: number;
  private readonly formatTime: (d: Date) => string;
  private readonly filters: FiltersPlane | null;

  constructor(
    private readonly adapter: PlatformAdapter,
    private readonly tools: ZulipToolRuntime,
    private readonly options: ZulipMcplServerOptions,
  ) {
    this.adapters = new Map([[adapter.type, adapter]]);
    this.catchupLimit = Math.min(CATCHUP_HARD_CAP, Math.max(0, options.catchupLimit ?? DEFAULT_CATCHUP_LIMIT));
    this.missedBlockMaxChars = Math.max(1000, options.missedBlockMaxChars ?? DEFAULT_MISSED_BLOCK_MAX_CHARS);
    this.formatTime = options.formatTime ?? defaultTimeFormatter();
    this.filters = options.filters ?? null;
    this.delivery = new DeliveryState(options.stateDir ?? null, options.sessionId ?? 'default');
    // A widened stream allowlist means channels the host has never seen:
    // make them known. A narrowed one is enforced on every delivery surface
    // (`isAllowed`); the host keeps its descriptors (a reopen would
    // re-announce nothing). Either way, what is held for a channel that may
    // no longer reach the agent is let go.
    this.filters?.onChange((next, prev) => {
      const before = new Set(prev.streams ?? []);
      const widened = !next.streams || (next.streams ?? []).some((name) => prev.streams && !before.has(name));
      if (widened) void this.applyFilterChange();
      void this.healUndelivered();
    });

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
      channelsChanged: (params) => this.channelsChangedWithHost(params),
      sendIncoming: (messages) => this.sendIncomingToHost(messages),
    };
    this.channelManager = new ChannelManager(host, this.adapters, this.grant, options.batchWindowMs, {
      // The only place an open channel's watermark moves: on the host's
      // itemized acceptance of a channels/incoming batch.
      onDelivered: (channelId, accepted, rejected) => this.onDelivered(channelId, accepted, rejected),
      // Policy is rechecked when the batch goes out, not only when it was
      // queued: a mute, a narrowed allowlist or a disabled feature set that
      // landed during the batch window applies to what is still waiting.
      deliverable: (message) => this.mayDeliver(message),
      onWithheld: (channelId, messages) => this.onWithheld(channelId, messages),
      onGivenUp: (channelId, messages) => this.onGivenUp(channelId, messages),
    });
    this.contextProvider = new ContextProvider(this.channelManager, this.grant, options.contextHistorySize, {
      excludeChannel: (channelId) => this.isMuted(channelId) || !this.isAllowed(channelId),
    });
    // Sends made through the tool surface are part of the rollback record.
    this.tools.onSent = (sent) => this.stateTracker.recordSent(sent.messageId, sent.channelId, sent.content);
  }

  /** True when the connected peer negotiated MCPL. */
  get mcplMode(): boolean {
    return this.mcplActive;
  }

  /** True once registration and the catch-up sweep are done and live
   *  delivery is flowing (before that, inbound events are held). */
  get isLive(): boolean {
    return this.live;
  }

  // ── Serve loop ──

  async serve(conn: McplConnection): Promise<void> {
    this.conn = conn;
    this.beginConnection();

    try {
      await this.run(conn);
    } catch (err) {
      if ((err as Error).name !== 'ConnectionClosedError') {
        console.error('[zulip-mcp] Connection error:', err);
      }
    }

    // The peer is gone: stop events, let what is in flight settle, try the
    // last flush (it fails harmlessly on a closed connection), persist.
    await this.quiesce();
    this.detachManifest?.();
    this.detachManifest = null;
    this.conn = null;
  }

  /**
   * Every connection is a fresh session over the persisted state: an empty
   * grant until this peer's §5.3 policy arrives, nothing registered or open,
   * no live delivery until this peer's sweep has run. Nothing of a previous
   * peer's grant, registration or sweep carries over.
   */
  private beginConnection(): void {
    this.grant.reset();
    this.channelManager.reset();
    this.live = false;
    this.sweepDone = false;
    this.preLive = [];
    this.preLiveDropped = 0;
    this.sweepAnchors = new Map(this.delivery.watermarkedChannels().map((id) => [id, this.delivery.watermark(id)!]));
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
      // The catch-up sweep follows registration so its pushes land on
      // registered channels inside the granted window; live delivery opens
      // only after the sweep, so nothing received meanwhile can advance a
      // watermark over the offline gap the sweep is about to fetch.
      void this.grant.whenReady().then(async () => {
        if (this.conn !== conn) return;
        try {
          await this.channelManager.registerChannels();
        } catch (error) {
          console.error('Failed to register channels after initial policy:', error);
        }
        try {
          await this.runReconnectSweep();
        } catch (error) {
          console.error('[zulip-mcp] Reconnect catch-up sweep failed:', error);
        }
        await this.goLive();
      });

      // The event queue is registered at once — Zulip delivers nothing that
      // predates the queue, so every second of delay here is a second of
      // messages that no sweep can recover for a never-watermarked channel.
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

  /**
   * Stop platform event delivery, wait (bounded) for deliveries still
   * building their content, push out anything still batched, and persist
   * delivery state. A flush over a connection that is already gone fails
   * harmlessly: those watermarks stay put and the next sweep re-fetches.
   * Idempotent.
   */
  async shutdown(): Promise<void> {
    await this.quiesce();
  }

  private async quiesce(): Promise<void> {
    if (this.eventsStarted) {
      this.adapter.stopEvents();
      this.eventsStarted = false;
    }
    if (this.inFlight.size > 0) {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const grace = new Promise<void>((resolve) => {
        timer = setTimeout(resolve, SHUTDOWN_INFLIGHT_GRACE_MS);
        (timer as { unref?: () => void }).unref?.();
      });
      await Promise.race([Promise.allSettled([...this.inFlight]), grace]);
      clearTimeout(timer);
      if (this.inFlight.size > 0) {
        console.error(`[zulip-mcp] ${this.inFlight.size} delivery(ies) still in flight at shutdown; left for the next catch-up sweep`);
      }
    }
    try {
      await this.channelManager.flush();
    } catch (error) {
      console.error('[zulip-mcp] final flush failed:', (error as Error).message ?? error);
    }
    this.channelManager.destroy();
    this.delivery.save();
  }

  /**
   * Release the pre-live buffer through the normal routing, then open live
   * delivery. Held messages the sweep already covered — id at or below the
   * channel's watermark as it stood when the sweep finished — are skipped;
   * that boundary is taken once, before draining, because draining itself
   * moves watermarks. Delivery stays gated until the buffer is empty, so a
   * live message arriving mid-drain (while a held one waits on an
   * attachment fetch) joins the queue behind the held ones instead of
   * overtaking them and burying them under its own acceptance.
   */
  private async goLive(): Promise<void> {
    if (this.live) return;
    const boundary = new Map(this.delivery.watermarkedChannels().map((id) => [id, this.delivery.watermark(id)!]));
    while (this.preLive.length > 0) {
      const held = this.preLive;
      this.preLive = [];
      for (const { message, newChannel } of held) {
        const watermark = boundary.get(message.channelId);
        const id = Number(message.messageId);
        if (watermark !== undefined && Number.isFinite(id) && id <= watermark) continue;
        try {
          await this.route(message, newChannel);
        } catch (err) {
          console.error('[zulip-mcp] delivery of a held message failed:', (err as Error).message);
        }
      }
    }
    this.live = true;
    if (this.preLiveDropped > 0) {
      console.error(`[zulip-mcp] ${this.preLiveDropped} live message(s) exceeded the pre-live buffer; held below the watermark for replay`);
      this.preLiveDropped = 0;
    }
    void this.healUndelivered();
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
          // A feature set enabled again: what was withheld under the
          // reduction is offered again.
          void this.healUndelivered();
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
          conn.sendResponse(req.id, await this.handleChannelOpen(open));
          break;
        }

        case method.CHANNELS_CLOSE: {
          this.requireMcpl();
          const close = params as unknown as ChannelsCloseParams;
          conn.sendResponse(req.id, this.handleChannelClose(close));
          break;
        }

        case method.CHANNELS_PUBLISH: {
          this.requireMcpl();
          const publish = params as unknown as ChannelsPublishParams;
          const result = await this.channelManager.publish(publish);
          if (result.delivered) {
            // Part of the rollback record. No checkpoint is minted here: the
            // publish result has no field to carry one (§14.6), so a host can
            // only learn checkpoints from tool results — see callTool.
            const text = publish.content
              .filter((b): b is TextContent => b.type === 'text')
              .map((b) => b.text)
              .join('\n');
            for (const id of (result as { messageIds?: string[] }).messageIds ?? (result.messageId ? [result.messageId] : [])) {
              this.stateTracker.recordSent(id, publish.channelId, text);
            }
          }
          conn.sendResponse(req.id, { delivered: result.delivered, ...(result.messageId ? { messageId: result.messageId } : {}) });
          break;
        }

        case method.CHANNELS_ACKNOWLEDGE: {
          this.requireMcpl();
          if (!this.grant.has('channels.acknowledge')) throw capabilityDenied('channels.acknowledge');
          const ack = params as { channelId?: string; messageId?: string; intent?: string; value?: string };
          if (!ack.channelId || !ack.messageId) throw new McplRpcError(-32602, 'channels/acknowledge requires channelId and messageId');
          if (!this.adapter.acknowledge) {
            conn.sendResponse(req.id, { acknowledged: false, reason: 'this surface has no acknowledgment representation' });
            break;
          }
          try {
            const representation = await this.adapter.acknowledge(ack.channelId, ack.messageId, ack.value);
            conn.sendResponse(req.id, { acknowledged: true, representation });
          } catch (error) {
            conn.sendResponse(req.id, { acknowledged: false, reason: (error as Error).message });
          }
          break;
        }

        case method.STATE_ROLLBACK: {
          this.requireMcpl();
          conn.sendResponse(req.id, await this.handleRollback(params as unknown as StateRollbackParams));
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

        case method.CHANNELS_OUTGOING_CHUNK:
        case method.CHANNELS_OUTGOING_COMPLETE:
          // Advisory stream of what the host is about to publish. Delivery is
          // NEVER a side effect of a lifecycle event (§14.5): the only send
          // path is channels/publish. Nothing to finalize here.
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

  // ── Channel lifecycle ──

  private async handleChannelOpen(params: ChannelsOpenParams): Promise<ChannelsOpenResult> {
    if (!this.grant.has('channels.lifecycle')) throw capabilityDenied('channels.lifecycle');
    const descriptor = this.channelManager.findChannel(params);
    const result: ChannelsOpenResult = { channel: descriptor };

    // Zulip only delivers stream events to subscribers; an open channel the
    // bot is not subscribed to would be listening to silence. So the
    // subscription comes FIRST, and a refusal (private stream, permission,
    // rate limit) fails the open: the host records the operation as failed
    // and the agent's tool result says why, instead of an open channel that
    // hears nothing.
    if (this.adapter.ensureSubscribed) {
      try {
        await this.adapter.ensureSubscribed(descriptor.id);
      } catch (err) {
        throw new McplRpcError(
          ERR_CHANNEL_OPEN_FAILED,
          `Cannot open ${descriptor.id}: ${(err as Error).message}`,
          { channelId: descriptor.id },
        );
      }
    }

    // Requested history is fetched BEFORE the lifecycle is committed, so a
    // failed open cannot leave the channel open while the host records the
    // operation as failed. A muted stream, or one outside the allowlist,
    // yields none: nothing from it reaches the agent, backscroll included.
    const requested = Math.max(0, Math.floor(params.history?.limit ?? 0));
    if (requested > 0 && this.adapter.fetchHistory && (this.isMuted(descriptor.id) || !this.isAllowed(descriptor.id))) {
      result.history = [];
      result.historyTruncated = false;
    } else if (requested > 0 && this.adapter.fetchHistory) {
      const cap = Math.min(HISTORY_ON_OPEN_CAP, historyCapOf(descriptor));
      const limit = Math.min(requested, cap);
      const watermark = this.delivery.watermark(descriptor.id);
      const page = await this.adapter.fetchHistory(descriptor.id, {
        limit,
        beforeMessageId: params.history?.beforeMessageId,
        afterMessageId:
          params.history?.sinceLastSeen && watermark !== undefined ? String(watermark) : undefined,
      });
      result.history = this.projectHistoryReactions(page.messages);
      result.historyTruncated = requested > limit;
      // Handed to the host in this very response: forwarded.
      for (const m of page.messages) this.accepted(descriptor.id, Number(m.messageId));
    }

    this.channelManager.markOpen(descriptor.id);
    this.delivery.markOpen(descriptor.id);
    this.delivery.save();
    return result;
  }

  private handleChannelClose(params: ChannelsCloseParams): { closed: boolean } {
    const result = this.channelManager.closeChannel(params);
    this.delivery.markClosed(params.channelId);
    this.delivery.save();
    return result;
  }

  // ── Inbound delivery ──

  /** A muted stream: nothing from it reaches the agent on any surface. */
  private isMuted(channelId: string): boolean {
    if (!this.filters || isDmChannelId(channelId) || !channelId.startsWith('zulip:')) return false;
    return this.filters.streamMuted(channelId.slice('zulip:'.length));
  }

  /**
   * The allowlists, on every surface: a stream outside `streams`, or a DM
   * conversation none of whose parties may DM the bot, delivers nothing —
   * live, replayed, swept, injected or backscrolled. With a message in hand
   * a DM is judged by its sender; without one (a per-channel surface) by the
   * conversation's registered participants, and a conversation this server
   * has not described is left to the per-message check in the adapter.
   */
  private isAllowed(channelId: string, message?: IncomingChannelMessage): boolean {
    if (!this.filters || !channelId.startsWith('zulip:')) return true;
    if (!isDmChannelId(channelId)) return this.filters.streamAllowed(channelId.slice('zulip:'.length));
    const sender = message ? senderOf(message) : null;
    if (sender) return this.filters.dmAllowed(sender);
    const meta = this.channelManager.getChannel(channelId)?.metadata as { participants?: unknown } | undefined;
    const participants = Array.isArray(meta?.participants) ? (meta!.participants as { id?: unknown; email?: unknown }[]) : [];
    if (participants.length === 0) return true;
    return participants.some((p) => this.filters!.dmAllowed({ id: Number(p.id), email: typeof p.email === 'string' ? p.email : '' }));
  }

  /**
   * Send-time policy for a queued message (see the ChannelManager hooks). A
   * system marker (a delivery gap, degraded polling) is about the
   * connection, not the stream it rides on: it passes mute and allowlist,
   * and only a disabled feature set withholds it.
   */
  private mayDeliver(message: IncomingChannelMessage): boolean {
    if (!this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) return false;
    const meta = (typeof message.metadata === 'object' && message.metadata !== null ? message.metadata : {}) as Record<string, unknown>;
    if (meta.system === true) return true;
    return !this.isMuted(message.channelId) && this.isAllowed(message.channelId, message);
  }

  /** The host accepted (or was handed) a message: its hold is released and the watermark may advance. */
  private accepted(channelId: string, id: number): boolean {
    if (!Number.isFinite(id) || id <= 0) return false;
    const released = this.delivery.release(channelId, id);
    return this.delivery.advance(channelId, id) || released;
  }

  /**
   * The host answered a channels/incoming batch. Accepted messages release
   * their holds and the watermark advances towards the highest of them —
   * never past a message still held: one the host rejected here, one
   * queued behind, or one given up on earlier. A rejected message stays
   * below the watermark until something delivers it again.
   */
  private onDelivered(channelId: string, accepted: IncomingChannelMessage[], rejected: IncomingChannelMessage[]): void {
    let moved = false;
    let high = 0;
    for (const m of accepted) {
      const id = numericId(m);
      if (id === null) continue;
      moved = this.delivery.release(channelId, id) || moved;
      high = Math.max(high, id);
    }
    for (const m of rejected) {
      const id = numericId(m);
      if (id !== null) this.delivery.hold(channelId, id, 'rejected');
    }
    if (high > 0) moved = this.delivery.advance(channelId, high) || moved;
    if (moved) this.delivery.save();
    if (accepted.length > 0) void this.healUndelivered();
  }

  /** A batch outlived its transport retry: the host never answered. Held for replay. */
  private onGivenUp(channelId: string, messages: IncomingChannelMessage[]): void {
    for (const m of messages) {
      const id = numericId(m);
      if (id !== null) this.delivery.hold(channelId, id, 'failed');
    }
  }

  /**
   * Policy withheld queued messages. A mute or a narrowed allowlist means
   * they may never reach the agent: let them go. A disabled feature set
   * means "not now": they stay held, replayed once it is enabled again.
   */
  private onWithheld(channelId: string, messages: IncomingChannelMessage[]): void {
    let moved = false;
    for (const m of messages) {
      const id = numericId(m);
      if (id === null) continue;
      if (this.isMuted(channelId) || !this.isAllowed(channelId, m)) moved = this.delivery.release(channelId, id) || moved;
      else this.delivery.hold(channelId, id, 'failed');
    }
    if (moved) this.delivery.save();
  }

  /** Queue a message for an open channel, holding its id until the host accepts it. */
  private deliverToOpen(channelId: string, message: IncomingChannelMessage): boolean {
    if (!this.channelManager.isOpen(channelId)) return false;
    const id = numericId(message);
    if (id !== null) this.delivery.hold(channelId, id, 'queued');
    this.channelManager.onIncomingMessage(channelId, message);
    return true;
  }

  /**
   * One message from the adapter. Before live delivery opens (registration
   * and the sweep pending) it is held, in order, bounded — the oldest beyond
   * the cap leave the buffer but stay held below the watermark, and the
   * replay after go-live fetches them back. Live, it is routed at once.
   */
  private async onIncoming(message: IncomingChannelMessage, newChannel?: ChannelDescriptor): Promise<void> {
    if (!this.live) {
      if (this.preLive.length >= PRE_LIVE_BUFFER_CAP) {
        const dropped = this.preLive.shift()!;
        const id = numericId(dropped.message);
        if (id !== null) this.delivery.hold(dropped.message.channelId, id, 'failed');
        this.preLiveDropped++;
      }
      this.preLive.push({ message, newChannel });
      return;
    }
    await this.route(message, newChannel);
  }

  /**
   * Route one message. See the delivery model in the file header. A forward
   * advances the watermark once the host has accepted it; an ambient message
   * dropped on a closed channel does not, so the next sweep can still find
   * it if the host opens the channel meanwhile.
   */
  private async route(message: IncomingChannelMessage, newChannel?: ChannelDescriptor): Promise<void> {
    const channelId = message.channelId;
    const id = Number(message.messageId);
    const meta = (typeof message.metadata === 'object' && message.metadata !== null ? message.metadata : {}) as Record<string, unknown>;
    const addressed = meta.mentioned === true || meta.isDM === true;

    // Muted stream, or outside the allowlist: nothing reaches the agent —
    // ambient AND mentions — and nothing is tallied, before any other
    // routing. Rechecked here (not only at the event) for what was held.
    if (this.isMuted(channelId) || !this.isAllowed(channelId, message)) return;

    // §6.7: a disabled zulip.messaging stops its traffic at once — incoming
    // and push alike. Not watermarked: the host asked not to hear it now,
    // which is not the same as having heard it.
    if (!this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) return;

    // A conversation the host has never seen (a DM from someone new): make
    // it a registered channel first, so the host can open it and route a
    // reply back to it.
    if (newChannel && !this.channelManager.getChannel(channelId)) {
      await this.channelManager.registerAdditional([newChannel]);
    }

    // Live delivery shows what was shared: images downsampled to model-max,
    // small text files inline. The reference note stays so anything not
    // inlined can still be fetched.
    const refs = Array.isArray(meta.attachments) ? (meta.attachments as AttachmentRef[]) : [];
    if (refs.length > 0 && this.options.attachments) {
      const blocks = await buildAttachmentBlocks(refs, this.options.attachments.source, this.options.attachments.inline);
      if (blocks.length > 0) message = { ...message, content: [...message.content, ...blocks] };
    }

    // The first message of a DM conversation carries an explicit reply
    // affordance: DMs have no subscription semantics, and the agent should
    // not have to discover the send path by trial.
    if (meta.isDM === true && this.delivery.watermark(channelId) === undefined) {
      const authorId = message.author.id;
      const note = `<system>Direct message from ${message.author.name} (user id ${authorId}). ` +
        `To reply, use send_dm(["${authorId}"]) or publish to channel ${channelId}. ` +
        `DMs always reach you; there is nothing to subscribe to.</system>`;
      message = { ...message, content: [{ type: 'text', text: note }, ...message.content] };
    }

    // The watermark moves in onDelivered, once the host has accepted it.
    if (this.deliverToOpen(channelId, message)) return;

    if (addressed) {
      this.delivery.hold(channelId, id, 'queued');
      const delivered = await this.pushEvent(message, `zulip_msg_${message.messageId}`);
      if (delivered) {
        if (this.accepted(channelId, id)) this.delivery.save();
        void this.healUndelivered();
      } else {
        this.delivery.hold(channelId, id, 'failed');
      }
      return;
    }

    if (this.delivery.countMissed(channelId, { id, text: viewOf(message).text })) this.delivery.save();
  }

  /**
   * Offer again what the host was offered and has not accepted. Runs once
   * the host has shown it is answering (a batch accepted, a push accepted,
   * go-live) and tries each held message once: an open channel gets exactly
   * those messages back from history through channels/incoming; a closed
   * one gets its catch-up block from the watermark. What is still refused
   * stays below the watermark for the next connection's sweep. A channel
   * that may no longer reach the agent (muted, outside the allowlist)
   * releases its holds instead.
   */
  private async healUndelivered(): Promise<void> {
    if (this.healing || !this.live || !this.conn || !this.mcplActive) return;
    const channels = this.delivery.heldChannels();
    if (channels.length === 0) return;
    this.healing = true;
    try {
      for (const channelId of channels) {
        if (this.isMuted(channelId) || !this.isAllowed(channelId)) {
          this.delivery.releaseAll(channelId);
          continue;
        }
        const ids = this.delivery.heldIds(channelId, { replayable: true });
        if (ids.length === 0) continue;
        // Not now, not never: tried again once the host enables it.
        if (!this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) continue;
        this.delivery.markReplayed(channelId, ids);
        try {
          if (this.channelManager.isOpen(channelId)) {
            await this.replayHeld(channelId, ids);
          } else {
            const watermark = this.delivery.watermark(channelId);
            await this.catchUpClosedChannel(channelId, this.delivery.wasOpen(channelId), watermark ?? ids[0] - 1);
          }
        } catch (err) {
          console.error(`[zulip-mcp] replay of undelivered messages on ${channelId} failed:`, (err as Error).message);
        }
      }
      this.delivery.save();
    } finally {
      this.healing = false;
    }
  }

  /** Fetch exactly the held ids of an open channel back from history and queue them again. */
  private async replayHeld(channelId: string, ids: number[]): Promise<void> {
    if (!this.adapter.fetchHistory || ids.length === 0) return;
    const wanted = new Set(ids);
    const fetched = await this.fetchAfter(channelId, ids[0] - 1, ids[ids.length - 1]);
    const found = new Set<number>();
    const msgs = fetched.messages.filter((m) => {
      const id = numericId(m);
      if (id === null || !wanted.has(id)) return false;
      found.add(id);
      return true;
    });
    const replayed = this.replayOnOpen(channelId, msgs);
    // A held id the scan covered but did not find is gone from history (or
    // no longer visible to the agent): nothing left to deliver.
    const scanned = fetched.scannedThrough ?? 0;
    for (const id of ids) if (!found.has(id) && (id <= scanned || fetched.complete)) this.delivery.release(channelId, id);
    if (replayed > 0) console.error(`[zulip-mcp] ${channelId}: replaying ${replayed} undelivered message(s) from history`);
  }

  /** Queue history back to an open channel as recovered delivery. Returns how many. */
  private replayOnOpen(channelId: string, msgs: IncomingChannelMessage[]): number {
    let n = 0;
    for (const m of this.projectHistoryReactions(msgs)) {
      // Watermarks move in onDelivered, once the host accepts the replay.
      const queued = this.deliverToOpen(channelId, {
        ...m,
        tags: [...(m.tags ?? []), 'zulip:missed'],
        metadata: { ...(m.metadata as Record<string, unknown>), backscroll: undefined, recovered: true },
      });
      if (queued) n++;
    }
    return n;
  }

  /**
   * Addressed message on a closed channel → push/event. Requires the
   * `pushEvents` capability and an active zulip.messaging. The origin carries
   * the MCPL channel id (what the host registers and routes replies to) and
   * the missed-ambient tally, so the host's closed-channel invitation can
   * show what staying out has cost.
   */
  private async pushEvent(message: IncomingChannelMessage, eventId: string, extraOrigin: Record<string, unknown> = {}): Promise<boolean> {
    const conn = this.conn;
    if (!conn || !this.mcplActive) return false;
    if (!this.grant.has('pushEvents') || !this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) {
      console.error(`[zulip-mcp] pushEvents not granted; dropping addressed message ${message.messageId} on closed ${message.channelId}`);
      return false;
    }
    const meta = (typeof message.metadata === 'object' && message.metadata !== null ? message.metadata : {}) as Record<string, unknown>;
    const missed = this.delivery.tally(message.channelId);
    const params: PushEventParams = {
      featureSet: MESSAGING_FEATURE_SET,
      eventId,
      timestamp: message.timestamp,
      origin: {
        source: 'zulip',
        mcplChannelId: message.channelId,
        messageId: message.messageId,
        stream: !isDmChannelId(message.channelId) && message.channelId.startsWith('zulip:')
          ? message.channelId.slice('zulip:'.length)
          : undefined,
        topic: meta.topic,
        authorId: message.author.id,
        authorName: message.author.name,
        isMention: meta.mentioned === true,
        isDM: meta.isDM === true,
        ...(missed ? { missedMessages: missed.messages, missedCharacters: missed.characters } : {}),
        ...extraOrigin,
      },
      tags: message.tags,
      payload: { content: message.content },
    };
    try {
      const result = (await conn.sendRequest(method.PUSH_EVENT, params)) as { accepted?: boolean; reason?: string } | undefined;
      if (result && result.accepted === false) {
        console.error(`[zulip-mcp] push/event ${eventId} not accepted by host${result.reason ? `: ${result.reason}` : ''}`);
        return false;
      }
      return true;
    } catch (err) {
      console.error('[zulip-mcp] push/event failed:', (err as Error).message);
      return false;
    }
  }

  /**
   * On (re)connect, deliver what arrived while the server was offline.
   * Channels the host had open get their full missed backscroll; every other
   * watermarked channel gets each mention with its vicinity. One `<missed>`
   * block per channel, as a push event. Runs at most once per process.
   */
  private async runReconnectSweep(): Promise<void> {
    if (this.sweepDone) return;
    this.sweepDone = true;
    if (!this.conn || !this.mcplActive || !this.adapter.fetchHistory || this.catchupLimit === 0) return;
    if (!this.grant.has('pushEvents') || !this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) return;

    const candidates = new Set<string>([
      ...this.sweepAnchors.keys(),
      ...this.delivery.lastOpenChannels(),
    ]);
    let delivered = 0;
    for (const channelId of candidates) {
      // From the watermark as it stood when this connection began: a
      // channel_open with backscroll answered meanwhile has moved the live
      // one past the offline gap this sweep exists to deliver.
      if (await this.catchUpClosedChannel(channelId, this.delivery.wasOpen(channelId), this.sweepAnchors.get(channelId))) delivered++;
    }

    await this.backfillMissedTallies();
    this.delivery.save();
    if (delivered > 0) {
      console.error(`[zulip-mcp] Reconnect catch-up: delivered missed messages from ${delivered} channel(s)`);
    }
  }

  /**
   * Everything after `afterId` on a channel (through `throughId` when
   * given), oldest first, paginated up to the catch-up ceiling. The cursor
   * advances on what each page SCANNED, not on what it handed back: a page
   * shorter than asked, or empty of anything for the agent, is not "no
   * more" — the adapter clamps to the platform's page size and withholds
   * the bot's own messages and disallowed senders after fetching. The walk
   * ends when the platform reports the newest message reached, when the
   * page is empty, or at the bound.
   */
  private async fetchAfter(
    channelId: string,
    afterId: number,
    throughId?: number,
  ): Promise<{ messages: IncomingChannelMessage[]; truncated: boolean; complete: boolean; scannedThrough: number | null }> {
    const out: IncomingChannelMessage[] = [];
    let cursor = afterId;
    let complete = false;
    while (out.length < this.catchupLimit) {
      const want = this.catchupLimit - out.length;
      const page = await this.adapter.fetchHistory!(channelId, { limit: want, afterMessageId: String(cursor) });
      if (page.scannedThrough === null) {
        complete = true;
        break;
      }
      out.push(...page.messages);
      if (page.scannedThrough <= cursor) break;
      cursor = page.scannedThrough;
      if (page.reachedNewest || (throughId !== undefined && cursor >= throughId)) {
        complete = true;
        break;
      }
    }
    // Not complete: the ceiling (or a bound) stopped the walk with more beyond the newest id scanned.
    return { messages: out, truncated: !complete, complete, scannedThrough: cursor > afterId ? cursor : null };
  }

  /**
   * Deliver what a closed (or not-yet-reopened) channel accumulated past its
   * watermark as one `<missed>` push event: the full backscroll when
   * `keepAll` (the host had it open, or it is a DM), else each mention with
   * its vicinity. Advances the watermark past everything scanned once the
   * host has accepted the push. Returns true when something was delivered.
   */
  private async catchUpClosedChannel(channelId: string, keepAllHint: boolean, afterId?: number): Promise<boolean> {
    if (!this.adapter.fetchHistory || this.catchupLimit === 0) return false;
    if (!this.channelManager.getChannel(channelId)) return false;
    if (this.isMuted(channelId) || !this.isAllowed(channelId)) return false;
    if (!this.grant.has('pushEvents') || !this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) return false;
    const after = afterId ?? this.delivery.watermark(channelId);
    if (after === undefined) return false;
    let fetched: Awaited<ReturnType<ZulipMcplServer['fetchAfter']>>;
    try {
      fetched = await this.fetchAfter(channelId, after);
    } catch (err) {
      console.error(`[zulip-mcp] catch-up: history fetch failed for ${channelId}:`, (err as Error).message);
      return false;
    }
    if (fetched.scannedThrough === null) return false;
    const newestId = fetched.scannedThrough;
    const msgs = fetched.messages;
    const views = msgs.map(viewOf);
    const keepAll = keepAllHint || isDmChannelId(channelId);
    const kept = selectMissed(views, { keepAll, vicinity: MISSED_VICINITY });
    if (kept.length === 0) {
      // Nothing to deliver, but advance the anchor so these are not re-scanned.
      this.delivery.advanceThrough(channelId, newestId);
      return false;
    }
    const mentionCount = views.filter((v) => v.mentioned).length;
    const streamName = isDmChannelId(channelId)
      ? (this.channelManager.getChannel(channelId)?.label ?? channelId)
      : channelId.slice('zulip:'.length);
    const block = renderMissedBlock(kept, {
      streamName,
      channelId,
      reason: keepAll ? 'backscroll' : 'mention',
      count: keepAll ? kept.length : mentionCount,
      formatTime: this.formatTime,
      maxChars: this.missedBlockMaxChars,
      moreBeyond: fetched.truncated,
      newestScannedId: newestId,
    });
    const synthetic: IncomingChannelMessage = {
      channelId,
      messageId: String(newestId),
      author: { id: 'system', name: 'zulip catch-up' },
      timestamp: new Date().toISOString(),
      content: [{ type: 'text', text: block } satisfies TextContent],
      tags: ['zulip:missed', ...(mentionCount > 0 ? ['chat:mention'] : ['chat:ambient'])],
      metadata: { missed: true, topic: undefined, mentioned: mentionCount > 0, isDM: isDmChannelId(channelId) },
    };
    const ok = await this.pushEvent(synthetic, `zulip_missed_${channelId}_${newestId}`, {
      missed: true,
      reason: keepAll ? 'backscroll' : 'mention',
      messages: kept.length,
      ...(fetched.truncated ? { truncated: true } : {}),
    });
    if (ok) {
      // Advance past everything scanned, not just what was delivered, so a
      // mention-only channel does not re-surface its non-mention tail; the
      // block covered every held id in the range, so their holds go too.
      this.delivery.advanceThrough(channelId, newestId);
    }
    return ok;
  }

  /** Count the ambient that arrived on tallied channels during downtime. */
  private async backfillMissedTallies(): Promise<void> {
    if (!this.adapter.fetchHistory) return;
    for (const channelId of this.delivery.talliedChannels()) {
      if (this.isMuted(channelId) || !this.isAllowed(channelId)) continue;
      const tally = this.delivery.tally(channelId)!;
      if (!tally.talliedThrough) continue;
      let fetched: Awaited<ReturnType<ZulipMcplServer['fetchAfter']>>;
      try {
        fetched = await this.fetchAfter(channelId, tally.talliedThrough);
      } catch {
        continue;
      }
      if (fetched.scannedThrough === null) continue;
      const views = fetched.messages.map(viewOf);
      // Only ambient counts as missed: mentions are delivered by the sweep.
      const ambient = views.filter((v) => !v.mentioned);
      this.delivery.backfillMissed(channelId, ambient, fetched.scannedThrough);
    }
  }

  /**
   * A Zulip event queue died and its replacement starts from "now". Heal
   * the gap from history (watermark → now) before the gap marker itself is
   * delivered, so the agent gets the messages, not advice to go looking for
   * them: open channels are replayed through channels/incoming; every other
   * watermarked channel gets its mentions as a `<missed>` push, exactly as
   * after a restart.
   */
  private async onSystemEvent(event: PlatformSystemEvent): Promise<void> {
    let recovered = 0;
    let closedCaughtUp = 0;
    // §6.7 holds for replayed traffic as much as for live: nothing is
    // recovered into a feature set the host has disabled.
    if (event.kind === 'gap' && this.adapter.fetchHistory && this.live && this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) {
      for (const channelId of this.channelManager.getOpenChannels()) {
        if (this.isMuted(channelId) || !this.isAllowed(channelId)) continue;
        const watermark = this.delivery.watermark(channelId);
        if (watermark === undefined) continue;
        try {
          const msgs = (await this.fetchAfter(channelId, watermark)).messages;
          recovered += this.replayOnOpen(channelId, msgs);
        } catch (err) {
          console.error(`[zulip-mcp] gap recovery failed for ${channelId}:`, (err as Error).message);
        }
      }
      for (const channelId of this.delivery.watermarkedChannels()) {
        if (this.channelManager.isOpen(channelId)) continue;
        if (await this.catchUpClosedChannel(channelId, false)) closedCaughtUp++;
      }
      if (recovered > 0 || closedCaughtUp > 0) this.delivery.save();
    }
    const notes = [
      recovered > 0 ? `${recovered} message(s) on open channels were recovered from history and delivered above.` : null,
      closedCaughtUp > 0 ? `Mentions on ${closedCaughtUp} closed channel(s) were delivered as catch-up events.` : null,
    ].filter(Boolean);
    const text = notes.length > 0 ? `${event.text} ${notes.join(' ')}` : event.text;
    this.channelManager.broadcastSystemEvent(this.adapter.type, {
      ...event,
      text,
      metadata: { ...event.metadata, ...(event.kind === 'gap' ? { recoveredMessages: recovered, closedChannelsCaughtUp: closedCaughtUp } : {}) },
    });
  }

  // ── Rollback ──

  private async handleRollback(params: StateRollbackParams): Promise<StateRollbackResult> {
    if (params.featureSet !== MESSAGING_FEATURE_SET) {
      return { checkpoint: params.checkpoint, success: false, reason: `Feature set '${params.featureSet}' does not support rollback` };
    }
    const toDelete = this.stateTracker.rollback(params.checkpoint);
    if (toDelete === null) {
      return { checkpoint: params.checkpoint, success: false, reason: 'Checkpoint not found' };
    }
    let deleted = 0;
    for (const msg of toDelete) {
      if (!this.adapter.deleteMessage) break;
      try {
        await this.adapter.deleteMessage(msg.channelId, msg.messageId);
        deleted++;
      } catch {
        // Best-effort — the message may already be gone, or past the realm's delete window.
      }
    }
    return {
      checkpoint: params.checkpoint,
      success: true,
      ...(deleted < toDelete.length ? { reason: `Rolled back (${deleted}/${toDelete.length} messages deleted)` } : {}),
    };
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
  private async callTool(name: string, args: Record<string, unknown>): Promise<ToolCallResult> {
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
      const own = await this.serverTool(name, args);
      const result = toToolCallResult(own !== undefined ? own : await this.tools.handleToolCall(name, args));
      // §8: a tool of the rollback-capable feature set mints a checkpoint and
      // hands it back in `state`, which is the only way a host ever learns
      // one. A rollback to this checkpoint undoes every send made after it
      // (this tool's own send is before it, on the record). Error results
      // carry none: nothing was sent, there is nothing new to return to.
      if (this.mcplActive && featureSetForTool(name) === MESSAGING_FEATURE_SET) {
        const checkpoint = this.stateTracker.createCheckpoint();
        const parent = this.stateTracker.getCheckpointState()?.parent ?? null;
        return { ...result, state: { featureSet: MESSAGING_FEATURE_SET, checkpoint, parent } };
      }
      return result;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return { content: [{ type: 'text' as const, text: `Error: ${message}` }], isError: true };
    }
  }

  /**
   * Re-enumerate what the adapter can see and announce anything the host
   * does not know yet — after a filters change widened the allowlist, or on
   * request (refresh_channels).
   */
  async applyFilterChange(): Promise<{ visible: number; added: string[] }> {
    if (!this.conn || !this.mcplActive) return { visible: 0, added: [] };
    const descriptors = await this.adapter.discoverChannels();
    const added = await this.channelManager.registerAdditional(descriptors);
    if (added.length > 0) console.error(`[zulip-mcp] registered ${added.length} newly visible channel(s): ${added.join(', ')}`);
    return { visible: descriptors.length, added };
  }

  private requireFilters(): FiltersPlane {
    if (!this.filters) throw new Error('No filters plane is configured on this server.');
    return this.filters;
  }

  private streamArg(value: unknown): string {
    const raw = String(value ?? '').trim().replace(/^#/, '');
    if (!raw) throw new Error('channel is required');
    const name = raw.startsWith('zulip:') ? raw.slice('zulip:'.length) : raw;
    if (isDmChannelId(`zulip:${name}`) || name.startsWith('dm:')) throw new Error('DM conversations cannot be muted or filtered by stream; use the dmUsers allowlist.');
    return name;
  }

  /** Tools that read the server's own delivery state. undefined = not ours. */
  private async serverTool(name: string, args: Record<string, unknown>): Promise<unknown> {
    switch (name) {
      case 'filters_get': {
        const plane = this.requireFilters();
        const f = plane.current();
        return {
          streams: f.streams ?? null,
          dmUsers: f.dmUsers ?? null,
          mutedStreams: f.mutedStreams ?? [],
          reactionChannels: f.reactionChannels ?? [],
          plane: plane.planeStatus(),
          reactionSuppression: plane.suppressionStatus(),
          note: 'null = unrestricted. Filters gate delivery only; the bot must also be able to see a stream. ' +
            'suppressedReactionEmojis is operator-owned and reported as a count/digest, never the entries.',
        };
      }
      case 'filters_update': {
        const plane = this.requireFilters();
        const addStreams = Array.isArray(args.addStreams) ? args.addStreams.map((s) => this.streamArg(s)) : [];
        const removeStreams = Array.isArray(args.removeStreams) ? args.removeStreams.map((s) => this.streamArg(s)) : [];
        const setDmUsers = Array.isArray(args.setDmUsers) ? args.setDmUsers.map(String) : undefined;
        if (addStreams.length === 0 && removeStreams.length === 0 && setDmUsers === undefined) {
          throw new Error('Nothing to change: pass addStreams, removeStreams, and/or setDmUsers.');
        }
        let materialized = false;
        const result = plane.update((f) => {
          let streams = f.streams ? [...f.streams] : null;
          if (removeStreams.length > 0 && streams === null) {
            // Removing from "everything" first materializes the list as every
            // stream currently registered, so nothing silently drops.
            streams = this.channelManager.listChannels().channels
              .filter((c) => !isDmChannelId(c.id))
              .map((c) => c.id.slice('zulip:'.length));
            materialized = true;
          }
          if (streams !== null) {
            for (const name of addStreams) if (!streams.includes(name)) streams.push(name);
            streams = streams.filter((name) => !removeStreams.includes(name));
            // An empty allowlist means UNRESTRICTED. Removing the last
            // allowed stream would therefore re-open every stream — the
            // opposite of what was asked. Refuse; "nothing" is what mutes are for.
            if (streams.length === 0) {
              throw new Error(
                'Refusing to remove the last allowed stream: an empty allowlist means unrestricted, which would ' +
                  'deliver EVERY stream. Add another stream first, or mute streams to hear nothing from them.',
              );
            }
          }
          return {
            ...f,
            ...(streams !== null ? { streams } : {}),
            ...(setDmUsers !== undefined ? { dmUsers: setDmUsers } : {}),
          };
        });
        if (!result.ok) throw new Error(`filters_update refused: ${result.reason}`);
        const { added } = await this.applyFilterChange();
        return {
          streams: result.filters.streams ?? null,
          dmUsers: result.filters.dmUsers ?? null,
          registered: added,
          note: [
            materialized ? 'The stream allowlist was unrestricted; it was materialized as the full current list before removing.' : null,
            setDmUsers !== undefined && (result.filters.dmUsers ?? null) === null ? 'dmUsers is now UNRESTRICTED (anyone may DM the bot).' : null,
            'Applied immediately and persisted.',
          ].filter(Boolean).join(' '),
        };
      }
      case 'mute_channel':
      case 'unmute_channel': {
        const plane = this.requireFilters();
        const stream = this.streamArg(args.channel);
        const mute = name === 'mute_channel';
        const result = plane.update((f) => {
          const muted = new Set(f.mutedStreams ?? []);
          if (mute) muted.add(stream);
          else muted.delete(stream);
          return { ...f, mutedStreams: [...muted] };
        });
        if (!result.ok) throw new Error(`${name} refused: ${result.reason}`);
        return {
          channelId: `zulip:${stream}`,
          muted: mute,
          mutedStreams: result.filters.mutedStreams ?? [],
          note: mute
            ? 'Nothing from this stream reaches you now — not even mentions — and nothing is tallied. Persisted; reverse with unmute_channel.'
            : 'Messages from this stream reach you again by the usual rules (mentions always; ambient when the channel is open).',
        };
      }
      case 'set_reaction_visibility': {
        const plane = this.requireFilters();
        const channelId = this.channelIdArg(args.channel);
        const visible = args.visible === true;
        const result = plane.update((f) => {
          const set = new Set(f.reactionChannels ?? []);
          if (visible) set.add(channelId);
          else set.delete(channelId);
          return { ...f, reactionChannels: [...set] };
        });
        if (!result.ok) throw new Error(`set_reaction_visibility refused: ${result.reason}`);
        return {
          channelId,
          visible,
          note: visible
            ? 'Reaction visibility ON: reactions in this channel now appear in your context as they happen (they never wake you). Persisted.'
            : 'Reaction visibility OFF for this channel. Persisted.',
        };
      }
      case 'refresh_channels': {
        const { visible, added } = await this.applyFilterChange();
        return {
          visible,
          added,
          note: added.length > 0 ? `Registered ${added.length} newly visible channel(s).` : 'No new channels — the host already knows about every visible channel.',
        };
      }
      case 'channel_missed': {
        const channelId = this.channelIdArg(args.channel);
        const tally = this.delivery.tally(channelId);
        if (!tally) {
          return {
            channelId,
            tracked: false,
            open: this.channelManager.isOpen(channelId),
            note: this.channelManager.isOpen(channelId)
              ? 'This channel is open: everything is delivered, nothing is missed.'
              : 'Not tracked: the host has not closed this channel since delivery began, so there is no baseline to count from.',
          };
        }
        return {
          channelId,
          tracked: true,
          missedMessages: tally.messages,
          missedCharacters: tally.characters,
          sinceMessageId: tally.anchorId || null,
          talliedThroughMessageId: tally.talliedThrough || null,
          note: 'Ambient messages dropped since the host closed this channel. Mentions were delivered and are not counted.',
        };
      }
      default:
        return undefined;
    }
  }

  private channelIdArg(value: unknown): string {
    const raw = String(value ?? '').trim();
    if (!raw) throw new Error('channel is required');
    return raw.startsWith('zulip:') ? raw : `zulip:${raw.replace(/^#/, '')}`;
  }

  private startEvents(): void {
    if (this.eventsStarted) return;
    this.eventsStarted = true;
    this.adapter.startEvents(
      (message, newChannel) => {
        const delivery = this.onIncoming(message, newChannel).catch((err) => {
          console.error('[zulip-mcp] inbound delivery failed:', (err as Error).message);
        });
        this.inFlight.add(delivery);
        void delivery.finally(() => this.inFlight.delete(delivery));
      },
      (event) => {
        void this.onSystemEvent(event).catch((err) => {
          console.error('[zulip-mcp] system event handling failed:', (err as Error).message);
        });
      },
      (reaction) => {
        void this.onReaction(reaction).catch((err) => {
          console.error('[zulip-mcp] reaction handling failed:', (err as Error).message);
        });
      },
    );
  }

  /**
   * Reaction visibility is a per-channel opt-in (default off). A reaction
   * carries only its reaction tag, so a wake policy keyed on tags leaves
   * the agent asleep (one that wakes on everything in a channel does not);
   * it never advances a watermark and lands in context for the agent's next
   * turn. Suppression is decided before any model-visible text or the event
   * id exists, so a suppressed reaction leaves no glyph or name anywhere.
   */
  private async onReaction(ev: ReactionEvent): Promise<void> {
    if (!this.filters || !this.filters.reactionsVisible(ev.channelId)) return;
    if (this.isMuted(ev.channelId) || !this.isAllowed(ev.channelId)) return;
    if (this.filters.reactionSuppressed(ev.emoji, ev.emojiCode, ev.emojiType)) return;
    if (!this.mcplActive || !this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) return;
    const verb = ev.action === 'add' ? 'reacted' : 'removed a reaction';
    const target = ev.onOwnMessage ? 'your message' : `message ${ev.messageId}`;
    const quoted = ev.messageSnippet ? ` — "${ev.messageSnippet}"` : '';
    const line = `[reaction] ${ev.reactorName} ${verb} :${ev.emoji}: on ${target}${quoted}`;
    const message: IncomingChannelMessage = {
      channelId: ev.channelId,
      messageId: `reaction:${ev.action}:${ev.messageId}:${ev.reactorId}:${ev.timestamp.getTime()}`,
      author: { id: ev.reactorId, name: ev.reactorName },
      timestamp: ev.timestamp.toISOString(),
      content: [{ type: 'text', text: line }],
      tags: [ev.action === 'add' ? CHAT_TAGS.reaction : CHAT_TAGS.reactionRemove],
      metadata: {
        reaction: true,
        action: ev.action,
        emoji: ev.emoji,
        ...(ev.emojiCode ? { emojiCode: ev.emojiCode } : {}),
        targetMessageId: ev.messageId,
        onOwnMessage: ev.onOwnMessage,
        isDM: false,
        mentioned: false,
      },
    };
    if (this.channelManager.isOpen(ev.channelId)) {
      this.channelManager.onIncomingMessage(ev.channelId, message);
    } else {
      await this.pushEvent(message, `zulip_reaction_${ev.action}_${ev.messageId}_${ev.reactorId}_${ev.timestamp.getTime()}`, {
        reaction: true,
        action: ev.action,
        onOwnMessage: ev.onOwnMessage,
      });
    }
  }

  /** Apply reaction suppression to replayed history (channels/open, gap recovery). */
  private projectHistoryReactions(messages: IncomingChannelMessage[]): IncomingChannelMessage[] {
    if (!this.filters) return messages;
    const plane = this.filters;
    return messages.map((m) => {
      const meta = (typeof m.metadata === 'object' && m.metadata !== null ? m.metadata : {}) as Record<string, unknown>;
      const reactions = Array.isArray(meta.reactions) ? (meta.reactions as ReactionSummary[]) : null;
      if (!reactions) return m;
      return { ...m, metadata: { ...meta, reactions: reactions.filter((r) => !plane.reactionSuppressed(r.name, r.code, r.type)) } };
    });
  }

  // ── Server → host ──

  private async registerChannelsWithHost(channels: ChannelDescriptor[]): Promise<ChannelsRegisterResult | undefined> {
    const conn = this.conn;
    if (!conn) throw new Error('not connected');
    return (await conn.sendRequest(method.CHANNELS_REGISTER, { channels })) as ChannelsRegisterResult | undefined;
  }

  private async channelsChangedWithHost(params: ChannelsChangedParams): Promise<ChannelsRegisterResult | undefined> {
    const conn = this.conn;
    if (!conn) throw new Error('not connected');
    return (await conn.sendRequest(method.CHANNELS_CHANGED, params)) as ChannelsRegisterResult | undefined;
  }

  private async sendIncomingToHost(messages: IncomingChannelMessage[]): Promise<ChannelsIncomingResult | undefined> {
    const conn = this.conn;
    if (!conn) throw new Error('not connected');
    return (await conn.sendRequest(method.CHANNELS_INCOMING, { messages })) as ChannelsIncomingResult | undefined;
  }
}

function defaultTimeFormatter(): (d: Date) => string {
  const zone = resolveAgentTimeZone();
  const style = resolveTimestampStyle();
  return (d) => formatAgentDateTime(d, zone, style);
}

/** A message's cursor id; null for reactions and system markers, which have none. */
function numericId(m: IncomingChannelMessage): number | null {
  const n = Number(m.messageId);
  return Number.isFinite(n) && n > 0 ? n : null;
}

/** The human sender of a real message; null for synthetic ones (system markers, reactions, catch-up blocks). */
function senderOf(m: IncomingChannelMessage): { id: number; email: string } | null {
  const meta = (typeof m.metadata === 'object' && m.metadata !== null ? m.metadata : {}) as Record<string, unknown>;
  if (meta.system === true || meta.reaction === true || meta.missed === true) return null;
  const id = Number(m.author?.id);
  if (!Number.isFinite(id)) return null;
  return { id, email: typeof meta.senderEmail === 'string' ? meta.senderEmail : '' };
}

function historyCapOf(descriptor: ChannelDescriptor): number {
  const max = descriptor.capabilities?.history?.maxMessages;
  return typeof max === 'number' && max > 0 ? max : HISTORY_ON_OPEN_CAP;
}
