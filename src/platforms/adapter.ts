/**
 * PlatformAdapter — the seam between the platform-agnostic MCPL layer
 * (ChannelManager, ContextProvider) and the platform-specific client.
 *
 * One implementer today (Zulip), but the seam is the documented shape a
 * platform plugs into, and it costs nothing to keep. An adapter owns one
 * platform connection and is responsible for:
 *   - discovering channels and describing them (ChannelDescriptor)
 *   - delivering outgoing publishes (with thread routing where supported)
 *   - fetching recent history for context/beforeInference injections
 *   - streaming real-time incoming messages (self-filtered)
 *
 * Channel IDs are prefixed with the adapter's `type` ('zulip:...'); the MCPL
 * layer routes purely on that prefix and never inspects the remainder.
 */

import type {
  ChannelDescriptor,
  ChannelsPublishResult,
  ContentBlock,
  ContextInjection,
  IncomingChannelMessage,
} from '@animalabs/mcpl-core';

export type PublishResult = ChannelsPublishResult;

/**
 * Routing hints for outgoing messages, derived by the MCPL layer from the
 * most recent incoming message on the target channel. The host's
 * channels/publish carries no thread information, so "reply where the
 * conversation is" is reconstructed server-side: Zulip maps threadId to a
 * topic.
 */
export interface RoutingHints {
  /** threadId of the last incoming message on this channel, if any. */
  threadId?: string;
  /** metadata of the last incoming message on this channel, if any. */
  metadata?: Record<string, unknown>;
}

/**
 * Delivery callback. `newChannel` is set when the message belongs to a
 * channel the adapter has not described before (a DM from a new
 * conversation) — the server registers it with the host before routing.
 */
export type OnIncomingMessage = (message: IncomingChannelMessage, newChannel?: ChannelDescriptor) => void;

/**
 * Out-of-band condition on a platform connection that the host/agent should
 * know about, surfaced as a synthetic system message on the platform's open
 * channels:
 *   - 'gap': real-time delivery lost coverage (e.g. a Zulip event queue
 *     expired and was re-registered from "now") — messages may have been
 *     missed and the agent should consult channel history if it matters.
 *   - 'degraded': the event source is failing repeatedly; delivery is
 *     unreliable until it recovers.
 *   - 'recovered': the event source resumed after a 'degraded' condition;
 *     real-time delivery is healthy again. Pairs with 'degraded' so the agent
 *     isn't left believing delivery is broken forever.
 */
export interface PlatformSystemEvent {
  kind: 'gap' | 'degraded' | 'recovered';
  /** Human-readable description, addressed to the agent. */
  text: string;
  metadata?: Record<string, unknown>;
}

export type OnSystemEvent = (event: PlatformSystemEvent) => void;

/** A reaction added to or removed from a message, resolved to its channel. */
export interface ReactionEvent {
  action: 'add' | 'remove';
  channelId: string;
  messageId: string;
  /** Emoji name in the platform's vocabulary (Zulip: 'thumbs_up'). */
  emoji: string;
  reactorId: string;
  reactorName: string;
  /** The reacted-to message was authored by the bot. */
  onOwnMessage: boolean;
  /** One-line snippet of the reacted-to message, or null when unknown. */
  messageSnippet: string | null;
  timestamp: Date;
}

export type OnReaction = (event: ReactionEvent) => void;

/** History request against one channel, in the platform's own id space. */
export interface ChannelHistoryQuery {
  limit: number;
  /** Exclusive: only messages older than this id. */
  beforeMessageId?: string;
  /** Exclusive: only messages newer than this id. */
  afterMessageId?: string;
}

export interface PlatformAdapter {
  /** Channel ID prefix and ChannelDescriptor.type, e.g. 'zulip'. */
  readonly type: string;

  /** Discover all channels visible to this connection. */
  discoverChannels(): Promise<ChannelDescriptor[]>;

  /**
   * Deliver content to a channel. `descriptor` is the registered descriptor
   * when known; adapters fall back to parsing the channelId.
   */
  publish(
    channelId: string,
    descriptor: ChannelDescriptor | undefined,
    content: ContentBlock[],
    hints?: RoutingHints,
  ): Promise<PublishResult>;

  /**
   * Best-effort typing indicator. Optional — platforms without a usable
   * typing API simply omit it.
   */
  sendTyping?(
    channelId: string,
    descriptor: ChannelDescriptor | undefined,
    metadata: Record<string, unknown> | undefined,
    op: 'start' | 'stop',
  ): Promise<void>;

  /**
   * Fetch recent channel history formatted as a context injection for
   * context/beforeInference. Returns null when there's nothing to inject.
   */
  fetchContext(
    channelId: string,
    descriptor: ChannelDescriptor | undefined,
    historySize: number,
  ): Promise<ContextInjection | null>;

  /**
   * Channel history as incoming-shaped messages, oldest first. Optional —
   * without it channels/open cannot return backscroll and the reconnect
   * sweep has nothing to scan.
   */
  fetchHistory?(channelId: string, query: ChannelHistoryQuery): Promise<IncomingChannelMessage[]>;

  /**
   * Make sure the platform delivers events for this channel — Zulip only
   * sends stream events to subscribers. Optional; idempotent; best-effort.
   */
  ensureSubscribed?(channelId: string): Promise<void>;

  /**
   * Start delivering real-time messages. Adapters filter the bot's own
   * messages before invoking the callback.
   *
   * `onSystemEvent` (optional, and optional for adapters to use) receives
   * out-of-band conditions — delivery gaps, degraded polling — so the host
   * can surface them to the agent instead of losing them in stderr.
   */
  startEvents(onMessage: OnIncomingMessage, onSystemEvent?: OnSystemEvent, onReaction?: OnReaction): void;

  /** Stop event delivery and release platform resources. */
  stopEvents(): void;
}
