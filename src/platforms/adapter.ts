/**
 * PlatformAdapter — the seam between the platform-agnostic MCPL layer
 * (ChannelManager, ContextProvider) and platform-specific clients
 * (Zulip, Discord, Slack, ...).
 *
 * Each adapter owns one platform connection and is responsible for:
 *   - discovering channels and describing them (ChannelDescriptor)
 *   - delivering outgoing publishes (with thread routing where supported)
 *   - fetching recent history for context/beforeInference injections
 *   - streaming real-time incoming messages (self-filtered)
 *
 * Channel IDs are prefixed with the adapter's `type` ('zulip:...',
 * 'discord:...', 'slack:...'); the MCPL layer routes purely on that prefix
 * and never inspects the remainder.
 */

import type {
  ChannelDescriptor,
  ChannelIncomingMessage,
  McplContentBlock,
  McplContextInjection,
} from '../mcpl/types.js';

export interface PublishResult {
  delivered: boolean;
  messageId?: string;
}

/**
 * Routing hints for outgoing messages, derived by the MCPL layer from the
 * most recent incoming message on the target channel. The host's
 * channels/publish carries no thread information, so "reply where the
 * conversation is" is reconstructed server-side: Zulip maps threadId to a
 * topic, Slack to a thread_ts.
 */
export interface RoutingHints {
  /** threadId of the last incoming message on this channel, if any. */
  threadId?: string;
  /** metadata of the last incoming message on this channel, if any. */
  metadata?: Record<string, unknown>;
}

export type OnIncomingMessage = (message: ChannelIncomingMessage) => void;

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
    content: McplContentBlock[],
    hints?: RoutingHints,
  ): Promise<PublishResult>;

  /**
   * Best-effort typing indicator. Optional — platforms without a usable
   * typing API (e.g. Slack bots) simply omit it.
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
  ): Promise<McplContextInjection | null>;

  /**
   * Start delivering real-time messages. Adapters filter the bot's own
   * messages before invoking the callback.
   *
   * `onSystemEvent` (optional, and optional for adapters to use) receives
   * out-of-band conditions — delivery gaps, degraded polling — so the host
   * can surface them to the agent instead of losing them in stderr.
   */
  startEvents(onMessage: OnIncomingMessage, onSystemEvent?: OnSystemEvent): void;

  /** Stop event delivery and release platform resources. */
  stopEvents(): void;
}
