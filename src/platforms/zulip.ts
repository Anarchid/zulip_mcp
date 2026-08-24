/**
 * ZulipAdapter — Zulip implementation of PlatformAdapter.
 *
 * Channel ids:
 *   zulip:{stream_name}      a stream; threads map to topics (incoming carries
 *                            the topic as threadId; publishes route to the
 *                            topic of the most recent incoming message, else
 *                            'mcpl')
 *   zulip:dm:{ids}           a direct-message conversation, keyed by the
 *                            other parties' sorted user ids (see history.ts)
 *
 * DM conversations are discovered from recent DM history at startup and
 * described on the fly when a message from a new conversation arrives.
 */

import type {
  ChannelDescriptor,
  ContentBlock,
  ContextInjection,
  IncomingChannelMessage,
  TextContent,
} from '@animalabs/mcpl-core';
import type {
  ChannelHistoryQuery,
  OnIncomingMessage,
  OnSystemEvent,
  PlatformAdapter,
  PublishResult,
  RoutingHints,
} from './adapter.js';
import { ZulipEventLoop } from './zulip-events.js';
import {
  assertApiSuccess,
  channelIdOf,
  dmCounterparts,
  dmDescriptor,
  fetchHistory,
  normalizeMessage,
  parseDmChannelId,
  toIncoming,
  type ZulipIdentity,
  type ZulipMessage,
  type ZulipRawMessage,
} from '../history.js';

/** The address every stream descriptor carries. */
export interface ZulipChannelAddress {
  stream_name: string;
  stream_id: number;
}

export function zulipChannelId(streamName: string): string {
  return `zulip:${streamName}`;
}

/** The stream behind a `zulip:{stream_name}` channel id. */
export function streamNameOf(channelId: string): string {
  return channelId.slice('zulip:'.length);
}

function addressOf(descriptor: ChannelDescriptor | undefined): Partial<ZulipChannelAddress> {
  const address = descriptor?.address;
  return typeof address === 'object' && address !== null ? (address as Partial<ZulipChannelAddress>) : {};
}

/** The live filters the adapter consults — the plane, or a stand-in. */
export interface FilterView {
  streamAllowed(streamName: string): boolean;
  dmAllowed(sender: { id: number; email: string }): boolean;
}

const ALLOW_ALL: FilterView = { streamAllowed: () => true, dmAllowed: () => true };

export interface ZulipAdapterOptions {
  /** Backscroll cap advertised per channel and enforced on channels/open. */
  backscrollDefault?: number;
  /** Per-stream overrides of the backscroll cap. */
  backscrollLimits?: ReadonlyMap<string, number>;
  /** Stream allowlist + DM allowlist, read live on every event. */
  filters?: FilterView;
  /** How many recent DMs to scan for conversations at startup. */
  dmDiscoveryLimit?: number;
}

export const DEFAULT_BACKSCROLL = 500;
const DEFAULT_DM_DISCOVERY_LIMIT = 300;

export class ZulipAdapter implements PlatformAdapter {
  readonly type = 'zulip';

  private eventLoop: ZulipEventLoop | null = null;
  private readonly identity: ZulipIdentity;
  private readonly backscrollDefault: number;
  private readonly backscrollLimits: ReadonlyMap<string, number>;
  private readonly filters: FilterView;
  private readonly dmDiscoveryLimit: number;
  /** Streams this process has confirmed a subscription for. */
  private subscribed = new Set<string>();
  /** DM conversations already described to the server, by channel id. */
  private knownDms = new Map<string, ChannelDescriptor>();

  constructor(
    private zulipClient: any,
    selfUserId: number | null,
    sessionId: string,
    options: ZulipAdapterOptions = {},
  ) {
    this.identity = { selfUserId, sessionId };
    this.backscrollDefault = options.backscrollDefault ?? DEFAULT_BACKSCROLL;
    this.backscrollLimits = options.backscrollLimits ?? new Map();
    this.filters = options.filters ?? ALLOW_ALL;
    this.dmDiscoveryLimit = options.dmDiscoveryLimit ?? DEFAULT_DM_DISCOVERY_LIMIT;
  }

  /** The history cap for a stream (descriptor `capabilities.history.maxMessages`). */
  backscrollLimitFor(streamName: string): number {
    return this.backscrollLimits.get(streamName) ?? this.backscrollDefault;
  }

  get selfUserId(): number | null {
    return this.identity.selfUserId;
  }

  async discoverChannels(): Promise<ChannelDescriptor[]> {
    const channels: ChannelDescriptor[] = [];
    try {
      const result = await this.zulipClient.streams.retrieve({
        include_public: true,
        include_subscribed: true,
      });
      const streams = result.streams || [];
      for (const stream of streams) {
        if (!this.filters.streamAllowed(stream.name)) continue;
        const address: ZulipChannelAddress = { stream_name: stream.name, stream_id: stream.stream_id };
        channels.push({
          id: zulipChannelId(stream.name),
          type: 'zulip',
          label: `#${stream.name}`,
          direction: 'bidirectional',
          address,
          metadata: {
            subscriber_count: stream.subscriber_count,
            is_public: !stream.invite_only,
          },
          capabilities: {
            history: {
              maxMessages: this.backscrollLimitFor(stream.name),
              supportsBeforeMessage: true,
              supportsSinceLastSeen: true,
            },
          },
        });
      }
    } catch (error) {
      console.error('Failed to discover Zulip streams:', error);
    }
    channels.push(...(await this.discoverDmChannels()));
    return channels;
  }

  /**
   * DM conversations the bot has been part of recently. Zulip has no
   * "list my DM conversations" call; the recent DM history is the source.
   */
  private async discoverDmChannels(): Promise<ChannelDescriptor[]> {
    if (this.dmDiscoveryLimit <= 0) return [];
    try {
      const result = await this.zulipClient.messages.retrieve({
        anchor: 'newest',
        num_before: this.dmDiscoveryLimit,
        num_after: 0,
        narrow: [['is', 'dm']],
        apply_markdown: false,
        include_anchor: true,
      });
      assertApiSuccess(result, 'recent direct messages');
      for (const raw of (result?.messages ?? []) as ZulipRawMessage[]) {
        const m = normalizeMessage(raw);
        if (!m.isDm) continue;
        if (!this.filters.dmAllowed({ id: m.authorId, email: m.authorEmail }) && m.authorId !== this.identity.selfUserId) continue;
        this.describeDm(m);
      }
    } catch (error) {
      console.error('Failed to discover Zulip DM conversations:', (error as Error).message);
    }
    return [...this.knownDms.values()];
  }

  /** The descriptor for a DM's conversation, remembered once described. */
  private describeDm(m: ZulipMessage): { descriptor: ChannelDescriptor; isNew: boolean } {
    const counterparts = dmCounterparts(m.recipients, this.identity.selfUserId);
    const descriptor = dmDescriptor(counterparts, this.backscrollDefault);
    const isNew = !this.knownDms.has(descriptor.id);
    this.knownDms.set(descriptor.id, descriptor);
    return { descriptor, isNew };
  }

  async publish(
    channelId: string,
    _descriptor: ChannelDescriptor | undefined,
    content: ContentBlock[],
    hints?: RoutingHints,
  ): Promise<PublishResult> {
    const textContent = content
      .filter((c): c is TextContent => c.type === 'text')
      .map(c => c.text)
      .join('\n');
    if (!textContent) return { delivered: false };

    const dmIds = parseDmChannelId(channelId);
    if (dmIds) {
      const result = await this.zulipClient.messages.send({ type: 'private', to: dmIds, content: textContent });
      assertApiSuccess(result, `direct message to ${channelId}`);
      return { delivered: true, messageId: String(result.id) };
    }

    const streamName = streamNameOf(channelId);

    // Route to the topic of the most recent incoming message on this channel
    // (in-thread answers); fall back to the 'mcpl' topic when the agent
    // initiates the conversation.
    const topic =
      (typeof hints?.metadata?.topic === 'string' ? hints.metadata.topic : undefined) ??
      hints?.threadId ??
      'mcpl';

    const result = await this.zulipClient.messages.send({
      type: 'stream',
      to: streamName,
      topic,
      content: textContent,
    });
    assertApiSuccess(result, `message to #${streamName}`);

    return { delivered: true, messageId: String(result.id) };
  }

  /**
   * Best-effort typing indicator.
   *
   * Routing metadata travels with the notification; the host (via whatever
   * inference logic it uses — most commonly the most recent incoming message
   * on this channel) provides a `topic` key pointing at the active Zulip
   * thread. Falls back to 'mcpl' if the host didn't provide one.
   *
   * Zulip typing events auto-expire server-side (~15s), so there's no stop op;
   * the host refreshes every 7s while inference is active.
   *
   * Note: zulip-js's `typing.send` unconditionally dereferences `params.to.length`,
   * so we must pass `to: []` even for the stream form — otherwise the library
   * throws a TypeError before the HTTP request is made. The Zulip server ignores
   * `to` when `type:'stream'` is set.
   */
  async sendTyping(
    channelId: string,
    descriptor: ChannelDescriptor | undefined,
    metadata: Record<string, unknown> | undefined,
    op: 'start' | 'stop',
  ): Promise<void> {
    const send = this.zulipClient.typing.send as (p: unknown) => Promise<{ result?: string; msg?: string }>;
    const dmIds = parseDmChannelId(channelId);
    try {
      let result: { result?: string; msg?: string };
      if (dmIds) {
        result = await send({ type: 'direct', to: dmIds, op });
      } else {
        const streamId = addressOf(descriptor).stream_id;
        if (!streamId) {
          console.error(`[zulip-mcp] sendTyping: no stream_id for ${channelId} (descriptor=${descriptor ? 'present' : 'missing'})`);
          return;
        }
        const topic = typeof metadata?.topic === 'string' ? metadata.topic : 'mcpl';
        result = await send({ type: 'stream', stream_id: streamId, topic, op, to: [] });
      }
      if (result?.result && result.result !== 'success') {
        console.error(`[zulip-mcp] typing.send(${op}) non-success: ${result.result} ${result.msg ?? ''}`);
      }
    } catch (err) {
      // Best-effort — swallow errors so typing never breaks the agent.
      console.error(`[zulip-mcp] typing.send(${op}) failed:`, (err as Error).message);
    }
  }

  async fetchContext(
    channelId: string,
    _descriptor: ChannelDescriptor | undefined,
    historySize: number,
  ): Promise<ContextInjection | null> {
    const dmIds = parseDmChannelId(channelId);
    const page = await fetchHistory(this.zulipClient, dmIds
      ? { dmUserIds: dmIds, limit: historySize }
      : { streamName: streamNameOf(channelId), limit: historySize });
    if (page.messages.length === 0) return null;

    const formatted = page.messages.map((m) => {
      const time = m.timestamp.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
      return dmIds
        ? `[${time}] ${m.authorName}: ${m.cleanContent}`
        : `[${time}] [${m.topic}] ${m.authorName}: ${m.cleanContent}`;
    }).join('\n');

    const label = dmIds ? `the direct-message conversation ${channelId}` : `Zulip #${streamNameOf(channelId)}`;
    return {
      namespace: channelId,
      position: 'beforeUser',
      content: `Recent messages from ${label}:\n${formatted}`,
    };
  }

  /**
   * History as incoming-shaped messages, oldest first, the bot's own
   * messages excluded (they are already in the agent's own record as its
   * turns). Marked `backscroll: true` so consumers can tell replayed history
   * from live delivery.
   */
  async fetchHistory(channelId: string, query: ChannelHistoryQuery): Promise<IncomingChannelMessage[]> {
    const dmIds = parseDmChannelId(channelId);
    const page = await fetchHistory(this.zulipClient, {
      ...(dmIds ? { dmUserIds: dmIds } : { streamName: streamNameOf(channelId) }),
      limit: query.limit,
      before: query.beforeMessageId !== undefined ? Number(query.beforeMessageId) : undefined,
      after: query.afterMessageId !== undefined ? Number(query.afterMessageId) : undefined,
    });
    return page.messages
      .filter((m) => this.identity.selfUserId === null || m.authorId !== this.identity.selfUserId)
      .map((m) => toIncoming(channelId, m, this.identity, { backscroll: true }));
  }

  /**
   * Zulip delivers stream events only to subscribers — even with
   * `all_public_streams` on the queue — so opening a channel must also
   * subscribe the bot, or the host would be listening to silence.
   * Idempotent; subscription persists server-side. DMs need nothing.
   */
  async ensureSubscribed(channelId: string): Promise<void> {
    if (parseDmChannelId(channelId)) return;
    const streamName = streamNameOf(channelId);
    if (this.subscribed.has(streamName)) return;
    try {
      const result = await this.zulipClient.users.me.subscriptions.add({
        subscriptions: [{ name: streamName }],
      });
      if (result?.result === 'success') {
        this.subscribed.add(streamName);
        const fresh = result.subscribed && Object.keys(result.subscribed).length > 0;
        if (fresh) console.error(`[zulip-mcp] subscribed to #${streamName} for channel ${channelId}`);
      } else {
        console.error(`[zulip-mcp] could not subscribe to #${streamName}: ${result?.msg ?? 'unknown error'}`);
      }
    } catch (err) {
      console.error(`[zulip-mcp] subscribe to #${streamName} failed:`, (err as Error).message);
    }
  }

  startEvents(onMessage: OnIncomingMessage, onSystemEvent?: OnSystemEvent): void {
    this.eventLoop = new ZulipEventLoop();
    this.eventLoop.start(this.zulipClient, (_streamName, msg, flags) => {
      if (this.identity.selfUserId !== null && msg.sender_id === this.identity.selfUserId) return;
      const m = normalizeMessage({ ...(msg as ZulipRawMessage), flags });
      if (m.isDm) {
        if (!this.filters.dmAllowed({ id: m.authorId, email: m.authorEmail })) {
          console.error(`[zulip-mcp] dropping DM from ${m.authorEmail} (${m.authorId}): not in the dmUsers allowlist`);
          return;
        }
        const { descriptor, isNew } = this.describeDm(m);
        onMessage(toIncoming(descriptor.id, m, this.identity), isNew ? descriptor : undefined);
        return;
      }
      if (m.streamName !== null && !this.filters.streamAllowed(m.streamName)) return;
      onMessage(toIncoming(channelIdOf(m, this.identity.selfUserId), m, this.identity));
    }, onSystemEvent).catch(error => {
      console.error('Zulip event loop failed:', error);
    });
  }

  stopEvents(): void {
    this.eventLoop?.stop();
    this.eventLoop = null;
  }
}
