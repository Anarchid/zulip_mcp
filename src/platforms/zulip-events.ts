/**
 * Zulip Event Loop — Real-time message delivery via long-polling.
 *
 * Registers an event queue for message events, polls for new messages,
 * and routes them through a callback. Handles queue expiry recovery
 * and graceful shutdown.
 *
 * Failure semantics:
 *   - Queue expiry (BAD_EVENT_QUEUE_ID) re-registers a fresh queue. Zulip
 *     offers no gap recovery for a dead queue, so any events between the
 *     last delivered event and re-registration are lost — we surface that
 *     as a 'gap' system event so the host/agent knows to check history.
 *   - Poll errors back off exponentially (base 2s, cap 60s) instead of
 *     retrying on a tight fixed interval. Non-JSON responses (proxy/HTML
 *     error pages, e.g. a 502 from a reverse proxy) are classified
 *     separately so they don't masquerade as Zulip API errors. After
 *     several consecutive failures a 'degraded' system event is emitted.
 *   - A malformed but non-throwing response (missing `events` array) also
 *     backs off rather than hot-spinning the poll.
 */

import type { PlatformSystemEvent, OnSystemEvent } from './adapter.js';

export interface ZulipEventMessage {
  id: number;
  sender_id: number;
  sender_full_name: string;
  sender_email: string;
  display_recipient: string | { email: string; full_name: string; id: number }[];
  subject: string;
  content: string;
  timestamp: number;
  type: string;
}

/** `flags` are the receiving user's message flags from the event envelope
 * (e.g. 'mentioned', 'wildcard_mentioned') — computed server-side by Zulip. */
export type OnZulipMessage = (streamName: string, message: ZulipEventMessage, flags: string[]) => void;

export interface ZulipEventLoopOptions {
  /** First retry delay after a poll failure. Default 2000ms. */
  baseBackoffMs?: number;
  /** Upper bound for the exponential backoff. Default 60000ms. */
  maxBackoffMs?: number;
  /** Consecutive failures before a 'degraded' system event is emitted. Default 3. */
  degradedThreshold?: number;
  /** Injectable sleep for tests. */
  sleep?: (ms: number) => Promise<void>;
}

const DEFAULT_BASE_BACKOFF_MS = 2000;
const DEFAULT_MAX_BACKOFF_MS = 60_000;
const DEFAULT_DEGRADED_THRESHOLD = 3;

export class ZulipEventLoop {
  private stopped = false;
  private queueId: string | null = null;
  /** Set when a queue dies so the gap can be reported once the replacement
   *  queue is registered (even if registration itself takes retries). */
  private pendingGap: { queueId: string; lastEventId: number } | null = null;

  private readonly baseBackoffMs: number;
  private readonly maxBackoffMs: number;
  private readonly degradedThreshold: number;
  private readonly sleepFn: (ms: number) => Promise<void>;

  constructor(options: ZulipEventLoopOptions = {}) {
    this.baseBackoffMs = options.baseBackoffMs ?? DEFAULT_BASE_BACKOFF_MS;
    this.maxBackoffMs = options.maxBackoffMs ?? DEFAULT_MAX_BACKOFF_MS;
    this.degradedThreshold = options.degradedThreshold ?? DEFAULT_DEGRADED_THRESHOLD;
    this.sleepFn = options.sleep ?? ((ms) => new Promise(resolve => setTimeout(resolve, ms)));
  }

  /**
   * Start the long-polling event loop.
   * This method runs indefinitely until stop() is called.
   *
   * `onSystemEvent` (optional) receives out-of-band conditions the agent
   * should know about: 'gap' (messages may have been missed across a queue
   * re-register) and 'degraded' (polling is failing repeatedly).
   */
  async start(zulipClient: any, onMessage: OnZulipMessage, onSystemEvent?: OnSystemEvent): Promise<void> {
    while (!this.stopped) {
      try {
        await this.pollLoop(zulipClient, onMessage, onSystemEvent);
      } catch (error) {
        if (this.stopped) return;
        console.error('Zulip event loop error, restarting in 5s:', error);
        await this.sleep(5000);
      }
    }
  }

  /**
   * Stop the event loop gracefully.
   */
  stop(): void {
    this.stopped = true;
  }

  private async pollLoop(zulipClient: any, onMessage: OnZulipMessage, onSystemEvent?: OnSystemEvent): Promise<void> {
    // Register event queue.
    // Two zulip-js quirks to work around:
    //   - Booleans crash FormData serialization; pass "true"/"false" as strings.
    //   - Arrays must be raw JS arrays (the library JSON.stringifies them);
    //     pre-stringified JSON produces "event_types is not a list" at Zulip.
    const registration = await zulipClient.queues.register({
      event_types: ['message'],
      all_public_streams: 'true',
      apply_markdown: 'false',
    });

    this.queueId = registration.queue_id;
    let lastEventId = registration.last_event_id;

    console.error(`Zulip event queue registered: ${this.queueId}`);

    // A previous queue died and the replacement starts from *now* — Zulip
    // has no gap recovery for dead queues, so events between the old
    // queue's last delivered id and this registration are gone. Tell the
    // host instead of dropping them silently.
    if (this.pendingGap) {
      const gap = this.pendingGap;
      this.pendingGap = null;
      this.emitSystemEvent(onSystemEvent, {
        kind: 'gap',
        text:
          `Zulip event queue ${gap.queueId} expired and was re-registered as ${this.queueId}. ` +
          `Messages arriving after event id ${gap.lastEventId} and before re-registration may have been missed. ` +
          `Check recent channel history if continuity matters.`,
        metadata: {
          platform: 'zulip',
          expiredQueueId: gap.queueId,
          lastEventId: gap.lastEventId,
          newQueueId: this.queueId,
        },
      });
    }

    let consecutiveFailures = 0;

    while (!this.stopped) {
      try {
        const response = await zulipClient.events.retrieve({
          queue_id: this.queueId,
          last_event_id: lastEventId,
        });

        if (!response || !Array.isArray(response.events)) {
          // Malformed but non-throwing response — without a delay this
          // would hot-spin the poll and peg a CPU core.
          consecutiveFailures++;
          const delay = this.backoffDelay(consecutiveFailures);
          console.error(
            `Zulip events.retrieve returned no events array (attempt ${consecutiveFailures}); retrying in ${delay}ms`,
          );
          this.maybeEmitDegraded(onSystemEvent, consecutiveFailures, 'events.retrieve returned a malformed response (no events array)');
          await this.sleep(delay);
          continue;
        }

        if (consecutiveFailures >= this.degradedThreshold) {
          console.error(`Zulip event polling recovered after ${consecutiveFailures} consecutive failures`);
        }
        consecutiveFailures = 0;

        for (const event of response.events) {
          lastEventId = event.id;

          if (event.type === 'message' && event.message) {
            const msg = event.message as ZulipEventMessage;
            // display_recipient is a string for stream messages, array for DMs
            const streamName = typeof msg.display_recipient === 'string'
              ? msg.display_recipient
              : null;

            if (streamName && msg.type === 'stream') {
              onMessage(streamName, msg, event.flags ?? []);
            }
          }
        }
      } catch (error: any) {
        if (this.stopped) return;

        const errMsg = error?.message || String(error);

        // Queue not found — record the gap and re-register in the outer loop.
        if (errMsg.includes('BAD_EVENT_QUEUE_ID') || errMsg.includes('queue_id')) {
          console.error('Zulip event queue expired, re-registering...');
          this.pendingGap = { queueId: this.queueId ?? 'unknown', lastEventId };
          return; // Exit inner loop to re-register in outer loop
        }

        // Other error — back off exponentially instead of a fixed tight
        // retry. Non-JSON responses (HTML error pages from a proxy, e.g.
        // a 502) surface as parse errors and never match the queue-expiry
        // check above; classify them so the logs say what actually broke.
        consecutiveFailures++;
        const delay = this.backoffDelay(consecutiveFailures);
        const nonJson = this.isNonJsonError(error, errMsg);
        if (nonJson) {
          console.error(
            `Zulip event poll got a non-JSON response (proxy/HTML error page?) (attempt ${consecutiveFailures}); retrying in ${delay}ms: ${errMsg}`,
          );
        } else {
          console.error(`Zulip event poll error (attempt ${consecutiveFailures}); retrying in ${delay}ms:`, error);
        }
        this.maybeEmitDegraded(
          onSystemEvent,
          consecutiveFailures,
          nonJson ? 'upstream is returning non-JSON responses (proxy/HTML error pages)' : errMsg,
        );
        await this.sleep(delay);
      }
    }
  }

  /** Exponential backoff: base * 2^(n-1), capped. */
  private backoffDelay(consecutiveFailures: number): number {
    const exp = Math.min(consecutiveFailures - 1, 31); // avoid 2**huge
    return Math.min(this.baseBackoffMs * 2 ** exp, this.maxBackoffMs);
  }

  /** Heuristic: did this error come from parsing a non-JSON (HTML/proxy) body? */
  private isNonJsonError(error: unknown, errMsg: string): boolean {
    return (
      error instanceof SyntaxError ||
      /unexpected token|not valid json|<html|<!doctype/i.test(errMsg)
    );
  }

  /** Emit a 'degraded' event exactly once per outage (when crossing the threshold). */
  private maybeEmitDegraded(
    onSystemEvent: OnSystemEvent | undefined,
    consecutiveFailures: number,
    reason: string,
  ): void {
    if (consecutiveFailures !== this.degradedThreshold) return;
    this.emitSystemEvent(onSystemEvent, {
      kind: 'degraded',
      text:
        `Zulip event polling has failed ${consecutiveFailures} times in a row (${reason}); ` +
        `real-time message delivery is degraded until it recovers.`,
      metadata: { platform: 'zulip', consecutiveFailures, reason },
    });
  }

  private emitSystemEvent(onSystemEvent: OnSystemEvent | undefined, event: PlatformSystemEvent): void {
    if (!onSystemEvent) return;
    try {
      onSystemEvent(event);
    } catch (error) {
      // System-event delivery must never take down the poll loop.
      console.error('Zulip event loop: onSystemEvent callback threw:', error);
    }
  }

  private sleep(ms: number): Promise<void> {
    return this.sleepFn(ms);
  }
}
