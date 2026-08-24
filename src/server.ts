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
 * Every forward advances a persisted per-channel watermark; on the next
 * connection a catch-up sweep delivers what arrived in between, and a Zulip
 * event-queue expiry is healed from history instead of merely reported.
 *
 * One connection at a time. `serve()` resolves when the peer disconnects.
 */

import {
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
import { DeliveryState, renderMissedBlock, selectMissed, viewOf } from './delivery.js';
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
  private readonly catchupLimit: number;
  private readonly formatTime: (d: Date) => string;
  private readonly filters: FiltersPlane | null;

  constructor(
    private readonly adapter: PlatformAdapter,
    private readonly tools: ZulipToolRuntime,
    private readonly options: ZulipMcplServerOptions,
  ) {
    this.adapters = new Map([[adapter.type, adapter]]);
    this.catchupLimit = Math.min(CATCHUP_HARD_CAP, Math.max(0, options.catchupLimit ?? DEFAULT_CATCHUP_LIMIT));
    this.formatTime = options.formatTime ?? defaultTimeFormatter();
    this.filters = options.filters ?? null;
    this.delivery = new DeliveryState(options.stateDir ?? null, options.sessionId ?? 'default');
    // A widened stream allowlist means channels the host has never seen:
    // make them known. A narrowed one is enforced at delivery; the host
    // keeps its descriptors (a reopen would re-announce nothing).
    this.filters?.onChange((next, prev) => {
      const before = new Set(prev.streams ?? []);
      const widened = !next.streams || (next.streams ?? []).some((name) => prev.streams && !before.has(name));
      if (widened) void this.applyFilterChange();
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
      // The catch-up sweep follows registration so its pushes land on
      // registered channels inside the granted window.
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
    this.delivery.save();
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
            const text = publish.content
              .filter((b): b is TextContent => b.type === 'text')
              .map((b) => b.text)
              .join('\n');
            for (const id of (result as { messageIds?: string[] }).messageIds ?? (result.messageId ? [result.messageId] : [])) {
              this.stateTracker.recordSent(id, publish.channelId, text);
            }
            this.stateTracker.createCheckpoint();
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

    // Requested history is fetched BEFORE the lifecycle is committed, so a
    // failed open cannot leave the channel open while the host records the
    // operation as failed.
    const requested = Math.max(0, Math.floor(params.history?.limit ?? 0));
    if (requested > 0 && this.adapter.fetchHistory) {
      const cap = Math.min(HISTORY_ON_OPEN_CAP, historyCapOf(descriptor));
      const limit = Math.min(requested, cap);
      const watermark = this.delivery.watermark(descriptor.id);
      const history = await this.adapter.fetchHistory(descriptor.id, {
        limit,
        beforeMessageId: params.history?.beforeMessageId,
        afterMessageId:
          params.history?.sinceLastSeen && watermark !== undefined ? String(watermark) : undefined,
      });
      result.history = this.projectHistoryReactions(history);
      result.historyTruncated = requested > limit;
      for (const m of history) this.delivery.advance(descriptor.id, Number(m.messageId));
    }

    this.channelManager.markOpen(descriptor.id);
    this.delivery.markOpen(descriptor.id);
    this.delivery.save();

    // Zulip only delivers stream events to subscribers; an open channel the
    // bot is not subscribed to would be listening to silence.
    if (this.adapter.ensureSubscribed) {
      await this.adapter.ensureSubscribed(descriptor.id).catch(() => {});
    }
    return result;
  }

  private handleChannelClose(params: ChannelsCloseParams): { closed: boolean } {
    const result = this.channelManager.closeChannel(params);
    this.delivery.markClosed(params.channelId);
    this.delivery.save();
    return result;
  }

  // ── Inbound delivery ──

  /**
   * Route one message from the adapter. See the delivery model in the file
   * header. Every forward advances the watermark; a dropped ambient message
   * does not, so the next sweep can still find it if the host opens the
   * channel meanwhile.
   */
  private async onIncoming(message: IncomingChannelMessage, newChannel?: ChannelDescriptor): Promise<void> {
    const channelId = message.channelId;
    const id = Number(message.messageId);
    const meta = (typeof message.metadata === 'object' && message.metadata !== null ? message.metadata : {}) as Record<string, unknown>;
    const addressed = meta.mentioned === true || meta.isDM === true;

    // Muted stream: nothing reaches the agent — ambient AND mentions — and
    // nothing is tallied, before any other routing.
    if (this.filters && !isDmChannelId(channelId) && this.filters.streamMuted(channelId.slice('zulip:'.length))) return;

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

    if (this.channelManager.isOpen(channelId)) {
      this.channelManager.onIncomingMessage(channelId, message);
      if (this.delivery.advance(channelId, id)) this.delivery.save();
      return;
    }

    if (addressed) {
      const delivered = await this.pushEvent(message, `zulip_msg_${message.messageId}`);
      if (delivered && this.delivery.advance(channelId, id)) this.delivery.save();
      return;
    }

    if (this.delivery.countMissed(channelId, { id, text: viewOf(message).text })) this.delivery.save();
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
      await conn.sendRequest(method.PUSH_EVENT, params);
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
      ...this.delivery.watermarkedChannels(),
      ...this.delivery.lastOpenChannels(),
    ]);
    let delivered = 0;
    for (const channelId of candidates) {
      if (!this.channelManager.getChannel(channelId)) continue;
      const watermark = this.delivery.watermark(channelId);
      if (watermark === undefined) continue;
      let msgs: IncomingChannelMessage[];
      try {
        msgs = await this.adapter.fetchHistory(channelId, { limit: this.catchupLimit, afterMessageId: String(watermark) });
      } catch (err) {
        console.error(`[zulip-mcp] sweep: history fetch failed for ${channelId}:`, (err as Error).message);
        continue;
      }
      if (msgs.length === 0) continue;
      const newestId = Number(msgs[msgs.length - 1].messageId);
      const views = msgs.map(viewOf);
      const keepAll = this.delivery.wasOpen(channelId) || isDmChannelId(channelId);
      const kept = selectMissed(views, { keepAll, vicinity: MISSED_VICINITY });
      if (kept.length === 0) {
        // Nothing to deliver, but advance the anchor so these are not re-scanned.
        this.delivery.advance(channelId, newestId);
        continue;
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
      });
      if (ok) {
        // Advance past everything scanned, not just what was delivered, so a
        // mention-only channel does not re-surface its non-mention tail.
        this.delivery.advance(channelId, newestId);
        delivered++;
      }
    }

    await this.backfillMissedTallies();
    this.delivery.save();
    if (delivered > 0) {
      console.error(`[zulip-mcp] Reconnect catch-up: delivered missed messages from ${delivered} channel(s)`);
    }
  }

  /** Count the ambient that arrived on tallied channels during downtime. */
  private async backfillMissedTallies(): Promise<void> {
    if (!this.adapter.fetchHistory) return;
    for (const channelId of this.delivery.talliedChannels()) {
      const tally = this.delivery.tally(channelId)!;
      if (!tally.talliedThrough) continue;
      let msgs: IncomingChannelMessage[];
      try {
        msgs = await this.adapter.fetchHistory(channelId, { limit: this.catchupLimit, afterMessageId: String(tally.talliedThrough) });
      } catch {
        continue;
      }
      if (msgs.length === 0) continue;
      const views = msgs.map(viewOf);
      // Only ambient counts as missed: mentions are delivered by the sweep.
      const ambient = views.filter((v) => !v.mentioned);
      this.delivery.backfillMissed(channelId, ambient, views[views.length - 1].id);
    }
  }

  /**
   * A Zulip event queue died and its replacement starts from "now". Heal
   * the gap for open channels from history (watermark → now) before the
   * gap marker itself is delivered, so the agent gets the messages, not
   * advice to go looking for them.
   */
  private async onSystemEvent(event: PlatformSystemEvent): Promise<void> {
    let recovered = 0;
    if (event.kind === 'gap' && this.adapter.fetchHistory) {
      for (const channelId of this.channelManager.getOpenChannels()) {
        const watermark = this.delivery.watermark(channelId);
        if (watermark === undefined) continue;
        try {
          const msgs = await this.adapter.fetchHistory(channelId, { limit: this.catchupLimit, afterMessageId: String(watermark) });
          for (const m of msgs) {
            this.channelManager.onIncomingMessage(channelId, {
              ...m,
              tags: [...(m.tags ?? []), 'zulip:missed'],
              metadata: { ...(m.metadata as Record<string, unknown>), backscroll: undefined, recovered: true },
            });
            this.delivery.advance(channelId, Number(m.messageId));
            recovered++;
          }
        } catch (err) {
          console.error(`[zulip-mcp] gap recovery failed for ${channelId}:`, (err as Error).message);
        }
      }
      if (recovered > 0) this.delivery.save();
    }
    const text = recovered > 0
      ? `${event.text} ${recovered} message(s) on open channels were recovered from history and delivered above.`
      : event.text;
    this.channelManager.broadcastSystemEvent(this.adapter.type, {
      ...event,
      text,
      metadata: { ...event.metadata, ...(event.kind === 'gap' ? { recoveredMessages: recovered } : {}) },
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
      return toToolCallResult(own !== undefined ? own : await this.tools.handleToolCall(name, args));
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
        void this.onIncoming(message, newChannel).catch((err) => {
          console.error('[zulip-mcp] inbound delivery failed:', (err as Error).message);
        });
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
   * Reaction visibility is a per-channel opt-in (default off). Reactions
   * never wake the agent — the reaction tags match no wake policy — and
   * never advance a watermark; they land in context so the agent sees them
   * when next active. Suppression is decided before any model-visible text
   * or the event id exists, so a suppressed reaction leaves no glyph or
   * name anywhere.
   */
  private async onReaction(ev: ReactionEvent): Promise<void> {
    if (!this.filters || !this.filters.reactionsVisible(ev.channelId)) return;
    if (this.filters.suppressAllReactions() || this.filters.reactionSuppressed(ev.emoji)) return;
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
      if (plane.suppressAllReactions()) {
        const { reactions: _dropped, ...rest } = meta;
        return { ...m, metadata: { ...rest, reactionsUnavailable: true } };
      }
      const reactions = Array.isArray(meta.reactions) ? (meta.reactions as ReactionSummary[]) : null;
      if (!reactions) return m;
      return { ...m, metadata: { ...meta, reactions: reactions.filter((r) => !plane.reactionSuppressed(r.name)) } };
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

function historyCapOf(descriptor: ChannelDescriptor): number {
  const max = descriptor.capabilities?.history?.maxMessages;
  return typeof max === 'number' && max > 0 ? max : HISTORY_ON_OPEN_CAP;
}
