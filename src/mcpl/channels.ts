/**
 * Channel Manager — Maps platform channels to MCPL channels.
 *
 * Platform-agnostic: routes every operation to a PlatformAdapter by the
 * channel ID prefix (the part before the first ':'). Channel ID formats are
 * owned by the adapters (e.g. zulip:{stream_name}, discord:{guildId}:{channelId},
 * slack:{channelId}).
 *
 * Handles registration, open/close lifecycle, incoming message batching,
 * publish routing (with last-incoming thread tracking for in-thread replies),
 * and channel listing.
 */

import type {
  ChannelDescriptor,
  ChannelIncomingMessage,
  ChannelsPublishParams,
  ChannelsOpenParams,
  ChannelsCloseParams,
  ChannelsListResult,
  ChannelsRegisterResult,
} from './types.js';
import type { McplClient } from './client.js';
import type { CapabilityGrant } from './grant.js';
import { McplRpcError, capabilityDenied } from './errors.js';
import type { PlatformAdapter, PlatformSystemEvent, RoutingHints } from '../platforms/adapter.js';

const DEFAULT_BATCH_WINDOW_MS = 500;

export class ChannelManager {
  private allChannels = new Map<string, ChannelDescriptor>();
  private openChannels = new Set<string>();
  private batchBuffer = new Map<string, ChannelIncomingMessage[]>();
  private batchTimer: ReturnType<typeof setTimeout> | null = null;
  private batchWindowMs: number;
  /** Per-channel routing hints from the most recent incoming message,
   *  so publishes can land in the active thread/topic. */
  private lastIncoming = new Map<string, RoutingHints>();

  /**
   * @param grant the effective capability grant for this connection (§5.4).
   *   Required: there is no ungated construction, because a default would be a
   *   default-allow and absence of a capability is denial.
   */
  constructor(
    private mcplClient: McplClient,
    private adapters: Map<string, PlatformAdapter>,
    private grant: CapabilityGrant,
    batchWindowMs?: number,
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
      const result = await this.mcplClient.registerChannels(channels);
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
   * Read an itemized `channels/register` / `channels/changed` result (§14.5).
   *
   * A host that answers with neither `results` nor `registered` has not
   * itemized anything; the submitted set stands. That is the pre-0.5 shape,
   * not a policy statement, and it is not read as approval of anything the
   * host explicitly rejected.
   */
  private acceptedIds(
    result: ChannelsRegisterResult | undefined,
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
   */
  openChannel(params: ChannelsOpenParams): { channel: ChannelDescriptor } {
    if (!this.grant.has('channels.lifecycle')) throw capabilityDenied('channels.lifecycle');

    // Find channel by type and address
    for (const [id, descriptor] of this.allChannels) {
      if (descriptor.type === params.type) {
        const matchesAddress = !params.address ||
          Object.entries(params.address).every(([k, v]) => descriptor.address?.[k] === v);
        if (matchesAddress) {
          this.openChannels.add(id);
          return { channel: descriptor };
        }
      }
    }
    throw new Error(`No channel found matching type=${params.type}`);
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
  onIncomingMessage(channelId: string, message: ChannelIncomingMessage): void {
    if (!this.openChannels.has(channelId)) return; // channel not opened by host

    // Remember where the conversation is, so publishes reply in-thread.
    this.lastIncoming.set(channelId, {
      threadId: message.threadId,
      metadata: message.metadata,
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
  async publish(params: ChannelsPublishParams): Promise<{ delivered: boolean; messageId?: string }> {
    if (!this.grant.has('channels.publish')) throw capabilityDenied('channels.publish');

    const channelId = params.channelId;
    const adapter = this.adapterFor(channelId);
    if (!adapter) {
      throw new McplRpcError(-32023, `Unknown channel format: ${channelId}`, { channelId });
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
   * Cleanup timers.
   */
  destroy(): void {
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }
  }

  // -- Private --

  private enqueue(channelId: string, message: ChannelIncomingMessage): void {
    let buffer = this.batchBuffer.get(channelId);
    if (!buffer) {
      buffer = [];
      this.batchBuffer.set(channelId, buffer);
    }
    buffer.push(message);

    this.scheduleBatchFlush();
  }

  private scheduleBatchFlush(): void {
    if (this.batchTimer) return; // already scheduled
    this.batchTimer = setTimeout(() => {
      this.batchTimer = null;
      this.flushBatch();
    }, this.batchWindowMs);
  }

  private async flushBatch(): Promise<void> {
    const allMessages: ChannelIncomingMessage[] = [];

    for (const [, messages] of this.batchBuffer) {
      allMessages.push(...messages);
    }
    this.batchBuffer.clear();

    if (allMessages.length === 0) return;

    // §14.1: `channels/incoming` is server→host content injection plus wake
    // authority — a write. Without the grant the batch is dropped, not queued:
    // a reduction must be respected immediately (§6.7), and holding messages
    // for a grant that may never arrive would deliver them out of time.
    if (!this.grant.has('channels.incoming')) {
      console.error(
        `channels.incoming not granted; dropping ${allMessages.length} inbound message(s)`,
      );
      return;
    }

    try {
      await this.mcplClient.sendIncoming(allMessages);
    } catch (error) {
      console.error('Failed to send incoming messages to host:', error);
    }
  }
}
