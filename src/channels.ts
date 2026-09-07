/**
 * Channel Manager — Maps platform channels to MCPL channels.
 *
 * Routes every operation to a PlatformAdapter by the channel ID prefix (the
 * part before the first ':'). Channel ID formats are owned by the adapters
 * (zulip:{stream_name}).
 *
 * Handles registration, open/close lifecycle, incoming message batching,
 * publish routing (with last-incoming thread tracking for in-thread replies),
 * and channel listing.
 */

import { ERR_UNKNOWN_CHANNEL } from '@animalabs/mcpl-core';
import type {
  ChannelDescriptor,
  ChannelsChangedParams,
  ChannelsCloseParams,
  ChannelsIncomingResult,
  ChannelsListResult,
  ChannelsPublishParams,
  ChannelsPublishResult,
  ChannelsRegisterResult,
  IncomingChannelMessage,
} from '@animalabs/mcpl-core';
import type { CapabilityGrant } from './grant.js';
import { McplRpcError, capabilityDenied } from './errors.js';
import type { PlatformAdapter, PlatformSystemEvent, RoutingHints } from './platforms/adapter.js';

const DEFAULT_BATCH_WINDOW_MS = 500;

/**
 * The server→host calls the manager makes. Implemented over `McplConnection`
 * by the server; kept as an interface so the manager is testable without a
 * transport.
 */
export interface HostClient {
  registerChannels(channels: ChannelDescriptor[]): Promise<ChannelsRegisterResult | undefined>;
  /** `channels/changed`, Request form (§14.5) — itemized like register. */
  channelsChanged(params: ChannelsChangedParams): Promise<ChannelsRegisterResult | undefined>;
  sendIncoming(messages: IncomingChannelMessage[]): Promise<ChannelsIncomingResult | undefined>;
}

/** Pre-0.5 hosts answered `channels/register` with a flat list of accepted ids. */
type RegisterResultCompat = Partial<ChannelsRegisterResult> & { registered?: string[] };

/** A batch that failed as a whole is retried this many times before it is
 *  given up on (the watermark then stays put and the next catch-up sweep
 *  re-fetches what was lost). */
const MAX_BATCH_ATTEMPTS = 2;

interface Queued {
  message: IncomingChannelMessage;
  attempts: number;
}

export interface ChannelManagerHooks {
  /**
   * Called per channel after one `channels/incoming` round trip with what
   * the host ACCEPTED and what it REJECTED. A batch that never got an answer
   * reports nothing — this is the only signal on which a delivery watermark
   * may advance.
   */
  onDelivered?: (channelId: string, accepted: IncomingChannelMessage[], rejected: IncomingChannelMessage[]) => void;
  /**
   * Decides at send time whether a queued message may still go out. A mute,
   * an allowlist narrowing or a feature-set reduction that landed while the
   * message sat in the batch window (or waited for its retry) is honoured
   * here, not only at enqueue.
   */
  deliverable?: (message: IncomingChannelMessage) => boolean;
  /** Messages removed from a batch by policy — `deliverable`, or a missing
   *  `channels.incoming` grant — grouped per channel. Not sent, not answered. */
  onWithheld?: (channelId: string, messages: IncomingChannelMessage[]) => void;
  /** Messages given up on after the transport retry, per channel: never
   *  accepted, never rejected — the host simply did not answer. */
  onGivenUp?: (channelId: string, messages: IncomingChannelMessage[]) => void;
}

export class ChannelManager {
  private allChannels = new Map<string, ChannelDescriptor>();
  private openChannels = new Set<string>();
  private batchBuffer = new Map<string, Queued[]>();
  private batchTimer: ReturnType<typeof setTimeout> | null = null;
  private batchWindowMs: number;
  private flushing: Promise<void> | null = null;
  /** Per-channel routing hints from the most recent incoming message,
   *  so publishes can land in the active thread/topic. */
  private lastIncoming = new Map<string, RoutingHints>();

  /**
   * @param grant the effective capability grant for this connection (§5.4).
   *   Required: there is no ungated construction, because a default would be a
   *   default-allow and absence of a capability is denial.
   */
  constructor(
    private host: HostClient,
    private adapters: Map<string, PlatformAdapter>,
    private grant: CapabilityGrant,
    batchWindowMs?: number,
    private hooks: ChannelManagerHooks = {},
  ) {
    this.batchWindowMs = batchWindowMs ?? DEFAULT_BATCH_WINDOW_MS;
  }

  /**
   * Discover all available channels across adapters and register them with the
   * host (`channels/register`, §14.3).
   *
   * Descriptors are recorded locally only once the host has accepted them.
   * §14.5 authorizes each descriptor independently and the Request form
   * answers itemwise, so a rejected descriptor must not sit in `allChannels`
   * pretending to exist.
   */
  async registerChannels(): Promise<void> {
    if (!this.grant.has('channels.register')) {
      console.error('channels.register not granted; skipping channel registration');
      return;
    }

    const channels: ChannelDescriptor[] = [];
    const discovered = new Map<string, ChannelDescriptor>();

    for (const adapter of this.adapters.values()) {
      try {
        for (const descriptor of await adapter.discoverChannels()) {
          channels.push(descriptor);
          discovered.set(descriptor.id, descriptor);
        }
      } catch (error) {
        console.error(`Failed to discover ${adapter.type} channels:`, error);
      }
    }

    if (channels.length === 0) return;

    try {
      const result = await this.host.registerChannels(channels);
      const accepted = this.acceptedIds(result, channels);
      for (const id of accepted) {
        const descriptor = discovered.get(id);
        if (descriptor) this.allChannels.set(id, descriptor);
      }
      const rejected = channels.length - accepted.size;
      console.error(
        `Registered ${accepted.size} channels with host` +
          (rejected > 0 ? ` (${rejected} rejected)` : ''),
      );
    } catch (error) {
      // The registration never landed, so no descriptor is registered.
      console.error('Failed to register channels:', error);
    }
  }

  /**
   * Register channels that appeared after startup (a DM from a new
   * conversation, a stream the bot was just added to) via `channels/changed`
   * (§14.5). Descriptors already known are refreshed locally and NOT
   * re-announced, so repeat calls do not spam the host. Returns the ids the
   * host accepted.
   */
  async registerAdditional(descriptors: ChannelDescriptor[]): Promise<string[]> {
    const added: ChannelDescriptor[] = [];
    for (const d of descriptors) {
      if (this.allChannels.has(d.id)) {
        this.allChannels.set(d.id, d);
      } else {
        added.push(d);
      }
    }
    if (added.length === 0) return [];
    if (!this.grant.has('channels.register')) {
      console.error(`channels.register not granted; ${added.length} new channel(s) stay unregistered`);
      return [];
    }
    try {
      const result = await this.host.channelsChanged({ added });
      const accepted = this.acceptedIds(result, added);
      for (const d of added) if (accepted.has(d.id)) this.allChannels.set(d.id, d);
      return [...accepted];
    } catch (error) {
      console.error('Failed to announce new channels:', error);
      return [];
    }
  }

  /**
   * Read an itemized `channels/register` / `channels/changed` result (§14.5).
   *
   * A host that answers with neither `results` nor `registered` has not
   * itemized anything; the submitted set stands. That is the pre-0.5 shape,
   * not a policy statement, and it is not read as approval of anything the
   * host explicitly rejected.
   */
  private acceptedIds(
    result: RegisterResultCompat | undefined,
    submitted: ChannelDescriptor[],
  ): Set<string> {
    if (result && Array.isArray(result.results)) {
      return new Set(result.results.filter((r) => r?.accepted).map((r) => r.id));
    }
    if (result && Array.isArray(result.registered)) {
      return new Set(result.registered);
    }
    return new Set(submitted.map((d) => d.id));
  }

  /**
   * Handle channels/open from the host. Requires `channels.lifecycle` (§14.1);
   * a denied capability behaves as if never advertised (§5.4), so this
   * answers with an error rather than acting.
   *
   * An exact `channelId` is preferred; otherwise the first registered channel
   * of the requested type whose address matches every key the host supplied.
   */
  openChannel(params: { channelId?: string; type: string; address?: unknown }): { channel: ChannelDescriptor } {
    if (!this.grant.has('channels.lifecycle')) throw capabilityDenied('channels.lifecycle');
    const descriptor = this.findChannel(params);
    this.openChannels.add(descriptor.id);
    return { channel: descriptor };
  }

  /**
   * Find the descriptor channels/open names without opening it — for callers
   * that must do work (fetch history) before committing the lifecycle.
   */
  findChannel(params: { channelId?: string; type: string; address?: unknown }): ChannelDescriptor {
    if (params.channelId) {
      const descriptor = this.allChannels.get(params.channelId);
      if (!descriptor) {
        throw new McplRpcError(ERR_UNKNOWN_CHANNEL, `Unknown channel: ${params.channelId}`, {
          channelId: params.channelId,
        });
      }
      return descriptor;
    }
    const wanted = isRecord(params.address) ? params.address : null;
    for (const descriptor of this.allChannels.values()) {
      if (descriptor.type !== params.type) continue;
      const have = isRecord(descriptor.address) ? descriptor.address : {};
      if (!wanted || Object.entries(wanted).every(([k, v]) => have[k] === v)) return descriptor;
    }
    throw new McplRpcError(ERR_UNKNOWN_CHANNEL, `No channel found matching type=${params.type}`, {
      type: params.type,
      address: params.address,
    });
  }

  /** Commit the open half of the lifecycle. Requires `channels.lifecycle`. */
  markOpen(channelId: string): void {
    if (!this.grant.has('channels.lifecycle')) throw capabilityDenied('channels.lifecycle');
    this.openChannels.add(channelId);
  }

  isOpen(channelId: string): boolean {
    return this.openChannels.has(channelId);
  }

  /**
   * Handle channels/close from the host. Requires `channels.lifecycle` (§14.1).
   */
  closeChannel(params: ChannelsCloseParams): { closed: boolean } {
    if (!this.grant.has('channels.lifecycle')) throw capabilityDenied('channels.lifecycle');
    const existed = this.openChannels.delete(params.channelId);
    return { closed: existed };
  }

  /**
   * Handle channels/list from the host. §14.1 keys `channels/list` in either
   * direction on `channels.register`.
   */
  listChannels(): ChannelsListResult {
    if (!this.grant.has('channels.register')) throw capabilityDenied('channels.register');
    return { channels: Array.from(this.allChannels.values()) };
  }

  /**
   * Called when a new message arrives from a platform.
   * Buffers messages and flushes in batches.
   */
  onIncomingMessage(channelId: string, message: IncomingChannelMessage): void {
    if (!this.openChannels.has(channelId)) return; // channel not opened by host

    // Remember where the conversation is, so publishes reply in-thread.
    this.lastIncoming.set(channelId, {
      threadId: message.threadId,
      metadata: isRecord(message.metadata) ? message.metadata : undefined,
    });

    this.enqueue(channelId, message);
  }

  /**
   * Broadcast a platform system event (delivery gap, degraded polling) to
   * every open channel of that platform as a synthetic incoming message, so
   * the host/agent learns about it instead of it dying in stderr.
   *
   * Deliberately does NOT update lastIncoming: a system marker must not
   * clobber the thread/topic routing hints of the real conversation.
   */
  broadcastSystemEvent(platformType: string, event: PlatformSystemEvent): void {
    const prefix = `${platformType}:`;
    const targets = Array.from(this.openChannels).filter(id => id.startsWith(prefix));

    if (targets.length === 0) {
      // No open channel to carry the marker — at least leave a trace.
      console.error(`[system:${platformType}] ${event.kind}: ${event.text} (no open channels to notify)`);
      return;
    }

    const timestamp = new Date().toISOString();
    for (const channelId of targets) {
      this.enqueue(channelId, {
        channelId,
        messageId: `system:${platformType}:${event.kind}:${Date.now()}`,
        author: { id: 'system', name: `${platformType} connection` },
        timestamp,
        content: [{ type: 'text', text: event.text }],
        // Spread adapter metadata FIRST so it can never clobber the
        // discriminators consumers filter on (system / kind).
        metadata: { ...event.metadata, system: true, kind: event.kind },
      });
    }
  }

  /**
   * Handle channels/publish from the host — route to the owning adapter.
   */
  async publish(params: ChannelsPublishParams): Promise<ChannelsPublishResult> {
    if (!this.grant.has('channels.publish')) throw capabilityDenied('channels.publish');

    const channelId = params.channelId;
    const adapter = this.adapterFor(channelId);
    if (!adapter) {
      throw new McplRpcError(ERR_UNKNOWN_CHANNEL, `Unknown channel format: ${channelId}`, { channelId });
    }

    return adapter.publish(
      channelId,
      this.allChannels.get(channelId),
      params.content,
      this.lastIncoming.get(channelId),
    );
  }

  /**
   * Handle channels/typing — best-effort typing indicator, routed to the
   * owning adapter when it supports one.
   */
  async sendTyping(
    channelId: string,
    metadata?: Record<string, unknown>,
    op: 'start' | 'stop' = 'start',
  ): Promise<void> {
    if (!this.grant.has('channels.typing')) throw capabilityDenied('channels.typing');
    const adapter = this.adapterFor(channelId);
    if (!adapter?.sendTyping) return;
    await adapter.sendTyping(channelId, this.allChannels.get(channelId), metadata, op);
  }

  /**
   * Get the set of currently open channel IDs.
   */
  getOpenChannels(): Set<string> {
    return this.openChannels;
  }

  /**
   * Get a channel descriptor by ID.
   */
  getChannel(id: string): ChannelDescriptor | undefined {
    return this.allChannels.get(id);
  }

  /**
   * Resolve the adapter owning a channel ID by its prefix.
   */
  adapterFor(channelId: string): PlatformAdapter | undefined {
    const prefix = channelId.split(':', 1)[0];
    return this.adapters.get(prefix);
  }

  /**
   * Type of the first registered adapter — used as a fallback when an
   * operation can't be attributed to a single platform.
   */
  firstAdapterType(): string {
    const first = this.adapters.values().next().value as PlatformAdapter | undefined;
    return first?.type ?? 'unknown';
  }

  /**
   * Send whatever is buffered now, without waiting for the batch window.
   * Awaits an in-flight flush first so batches never interleave. Used at
   * shutdown so a message received in the last window is not stranded with
   * its watermark unadvanced — and, if the connection is already gone, the
   * failure leaves the watermark alone for the next sweep.
   */
  async flush(): Promise<void> {
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }
    if (this.flushing) await this.flushing;
    await this.flushBatch();
  }

  /** Buffered messages not yet delivered (for tests and diagnostics). */
  pendingCount(): number {
    let n = 0;
    for (const q of this.batchBuffer.values()) n += q.length;
    return n;
  }

  /**
   * Cleanup timers. Does not flush — call `flush()` first when the
   * connection is still usable.
   */
  destroy(): void {
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }
  }

  /**
   * Forget everything that belonged to a connection: the descriptors the
   * peer accepted, what it had open, what was buffered for it. A new peer
   * registers and opens afresh; whatever was buffered is re-offered by the
   * catch-up sweep, not sent to a host that never asked for it.
   */
  reset(): void {
    this.destroy();
    this.allChannels.clear();
    this.openChannels.clear();
    this.batchBuffer.clear();
    this.lastIncoming.clear();
  }

  // -- Private --

  private enqueue(channelId: string, message: IncomingChannelMessage, attempts = 0): void {
    let buffer = this.batchBuffer.get(channelId);
    if (!buffer) {
      buffer = [];
      this.batchBuffer.set(channelId, buffer);
    }
    buffer.push({ message, attempts });

    this.scheduleBatchFlush();
  }

  private scheduleBatchFlush(): void {
    if (this.batchTimer) return; // already scheduled
    this.batchTimer = setTimeout(() => {
      this.batchTimer = null;
      if (this.flushing) {
        // A flush is still on the wire; run after it rather than alongside.
        this.scheduleBatchFlush();
        return;
      }
      this.flushing = this.flushBatch().finally(() => {
        this.flushing = null;
      });
    }, this.batchWindowMs);
  }

  /**
   * One `channels/incoming` round trip for everything buffered.
   *
   * The buffer is taken, not cleared: on a failed request the batch goes
   * back (front of the queue, attempts + 1) for one more try, and only
   * messages the host itemizes as accepted reach `onDelivered`. A host that
   * rejects a message has refused it (§14.5) — it is logged and reported
   * rejected, and a batch that outlives the retry is reported given up; in
   * neither case does a watermark move, so the message can be offered again.
   */
  private async flushBatch(): Promise<void> {
    const taken = new Map(this.batchBuffer);
    this.batchBuffer.clear();
    let queued: Queued[] = [];
    for (const items of taken.values()) queued.push(...items);
    if (queued.length === 0) return;

    // Policy is rechecked at send time, not only at enqueue: what changed
    // during the batch window (a mute, a narrowed allowlist, a reduced
    // grant) applies to what is still waiting.
    if (this.hooks.deliverable) {
      const withheld = new Map<string, IncomingChannelMessage[]>();
      queued = queued.filter((q) => {
        if (this.hooks.deliverable!(q.message)) return true;
        const list = withheld.get(q.message.channelId) ?? [];
        list.push(q.message);
        withheld.set(q.message.channelId, list);
        return false;
      });
      for (const [channelId, messages] of withheld) this.report(this.hooks.onWithheld, channelId, messages);
      if (queued.length === 0) return;
    }
    const allMessages = queued.map((q) => q.message);

    // §14.1: `channels/incoming` is server→host content injection plus wake
    // authority — a write. Without the grant the batch is dropped, not queued:
    // a reduction must be respected immediately (§6.7), and holding messages
    // for a grant that may never arrive would deliver them out of time.
    if (!this.grant.has('channels.incoming')) {
      console.error(
        `channels.incoming not granted; dropping ${allMessages.length} inbound message(s)`,
      );
      for (const [channelId, messages] of groupByChannel(allMessages)) this.report(this.hooks.onWithheld, channelId, messages);
      return;
    }

    let result: ChannelsIncomingResult | undefined;
    try {
      result = await this.host.sendIncoming(allMessages);
    } catch (error) {
      const retry = queued.filter((q) => q.attempts + 1 < MAX_BATCH_ATTEMPTS);
      const givenUp = queued.filter((q) => q.attempts + 1 >= MAX_BATCH_ATTEMPTS).map((q) => q.message);
      console.error(
        `Failed to send ${allMessages.length} incoming message(s) to host` +
          (retry.length > 0 ? `; retrying ${retry.length}` : '') +
          (givenUp.length > 0 ? `; giving up on ${givenUp.length} (held below the watermark for a later replay)` : '') +
          ':',
        (error as Error).message ?? error,
      );
      for (const [channelId, messages] of groupByChannel(givenUp)) this.report(this.hooks.onGivenUp, channelId, messages);
      // Put the retry set back ahead of anything buffered meanwhile.
      const requeued = new Map<string, Queued[]>();
      for (const q of retry) {
        const list = requeued.get(q.message.channelId) ?? [];
        list.push({ message: q.message, attempts: q.attempts + 1 });
        requeued.set(q.message.channelId, list);
      }
      for (const [channelId, later] of this.batchBuffer) {
        const list = requeued.get(channelId) ?? [];
        list.push(...later);
        requeued.set(channelId, list);
      }
      this.batchBuffer = requeued;
      if (retry.length > 0) this.scheduleBatchFlush();
      return;
    }

    const accepted = this.acceptedMessageIds(result, allMessages);
    const byChannel = new Map<string, { accepted: IncomingChannelMessage[]; rejected: IncomingChannelMessage[] }>();
    let rejected = 0;
    for (const m of allMessages) {
      const entry = byChannel.get(m.channelId) ?? { accepted: [], rejected: [] };
      if (accepted.has(m.messageId)) entry.accepted.push(m);
      else {
        entry.rejected.push(m);
        rejected++;
      }
      byChannel.set(m.channelId, entry);
    }
    if (rejected > 0) {
      console.error(`Host rejected ${rejected} of ${allMessages.length} incoming message(s); they are not delivered`);
    }
    for (const [channelId, entry] of byChannel) {
      try {
        this.hooks.onDelivered?.(channelId, entry.accepted, entry.rejected);
      } catch (error) {
        console.error('onDelivered hook failed:', (error as Error).message ?? error);
      }
    }
  }

  private report(
    hook: ((channelId: string, messages: IncomingChannelMessage[]) => void) | undefined,
    channelId: string,
    messages: IncomingChannelMessage[],
  ): void {
    if (!hook || messages.length === 0) return;
    try {
      hook(channelId, messages);
    } catch (error) {
      console.error('channel manager hook failed:', (error as Error).message ?? error);
    }
  }

  /** Itemized `channels/incoming` result (§14.5); a host that does not
   *  itemize has accepted the batch as a whole. */
  private acceptedMessageIds(
    result: ChannelsIncomingResult | undefined,
    sent: IncomingChannelMessage[],
  ): Set<string> {
    if (result && Array.isArray(result.results)) {
      return new Set(result.results.filter((r) => r?.accepted).map((r) => r.messageId));
    }
    return new Set(sent.map((m) => m.messageId));
  }
}

function groupByChannel(messages: IncomingChannelMessage[]): Map<string, IncomingChannelMessage[]> {
  const out = new Map<string, IncomingChannelMessage[]>();
  for (const m of messages) {
    const list = out.get(m.channelId) ?? [];
    list.push(m);
    out.set(m.channelId, list);
  }
  return out;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
