/**
 * Zulip Event Loop — Real-time message delivery via long-polling.
 *
 * Registers an event queue for message events, polls for new messages,
 * and routes them through a callback. Handles queue expiry recovery
 * and graceful shutdown.
 *
 * Failure semantics — grounded in what the vendored client stack
 * (`zulip-js@2.1.0` → `isomorphic-fetch` → `node-fetch@2.7.0`) actually does:
 *
 *   - Queue expiry (BAD_EVENT_QUEUE_ID) is an HTTP 400 with a *valid* JSON
 *     body `{ result: 'error', code: 'BAD_EVENT_QUEUE_ID', msg: ... }`.
 *     node-fetch does NOT throw on 4xx and the body parses fine, so zulip-js
 *     `retrieve` RETURNS this object — it never throws. (zulip-js's own
 *     `events_wrapper.js` checks `res.result === 'error'` for exactly this.)
 *     We classify on the returned `code` field and re-register a fresh queue.
 *     Zulip offers no gap recovery for a dead queue, so any events between the
 *     last delivered message and re-registration are lost — we surface that as
 *     a 'gap' system event anchored on the last delivered *message* id/time
 *     (not the queue-local event id, which is meaningless to the new queue or
 *     the messages API) so the host/agent can consult history from there.
 *   - Transport failures (ECONNRESET, timeouts, and non-JSON proxy/HTML error
 *     bodies) DO throw — as a node-fetch `FetchError`. A 502 HTML page rejects
 *     `response.json()` with `FetchError('invalid json response body at <url>
 *     reason: ...', 'invalid-json')`; a dropped connection rejects with
 *     `FetchError('request to <url> failed, reason: ...', 'system')`. Both
 *     embed the full request URL (including `?queue_id=...`) in the message,
 *     which is precisely why we must NOT sniff the message for 'queue_id' —
 *     doing so misclassifies every transient blip as expiry. These back off
 *     exponentially (base 2s, cap 60s); after several consecutive failures a
 *     'degraded' system event fires, and a 'recovered' event fires when
 *     polling succeeds again.
 *   - A malformed but non-throwing response (missing `events` array and not an
 *     error object) also backs off rather than hot-spinning the poll.
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

/**
 * The shape zulip-js `events.retrieve` resolves to. On success it carries an
 * `events` array; on a queue-level failure (e.g. BAD_EVENT_QUEUE_ID) it
 * resolves — NOT rejects — to a `{ result: 'error', code, msg }` object.
 */
export interface ZulipRetrieveResponse {
  events?: { id: number; type: string; message?: ZulipEventMessage; flags?: string[] }[];
  result?: string;
  code?: string;
  msg?: string;
}

/**
 * Minimal structural view of the two zulip-js methods this loop drives.
 * zulip-js is untyped; this pins the contract that matters — crucially that
 * `retrieve` *returns* errors rather than throwing them (see file header).
 */
export interface ZulipEventClient {
  queues: {
    register(params: Record<string, unknown>): Promise<{ queue_id: string; last_event_id: number }>;
  };
  events: {
    retrieve(params: { queue_id: string; last_event_id: number }): Promise<ZulipRetrieveResponse>;
  };
}

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
   *  queue is registered (even if registration itself takes retries). The
   *  anchor is the last delivered *message* (id + unix timestamp), which is
   *  the only value usable as a "since" against the messages API. */
  private pendingGap: {
    queueId: string;
    lastMessageId: number | null;
    lastMessageTimestamp: number | null;
  } | null = null;

  /** Last delivered message anchor — the only "since" value usable against
   *  the messages API once a queue is replaced (see pendingGap). */
  private lastMessageId: number | null = null;
  private lastMessageTimestamp: number | null = null;

  /** Consecutive poll failures. Instance-scoped so it survives an outer-loop
   *  restart and cannot be reset by re-entering pollLoop — no path bypasses
   *  backoff. Reset only on a successful, well-formed poll. */
  private consecutiveFailures = 0;
  /** True while a 'degraded' event is outstanding, so 'recovered' fires once. */
  private degradedActive = false;

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
   * re-register), 'degraded' (polling is failing repeatedly), and 'recovered'
   * (polling is healthy again).
   */
  async start(zulipClient: ZulipEventClient, onMessage: OnZulipMessage, onSystemEvent?: OnSystemEvent): Promise<void> {
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

  private async pollLoop(zulipClient: ZulipEventClient, onMessage: OnZulipMessage, onSystemEvent?: OnSystemEvent): Promise<void> {
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
    // queue's last delivered message and this registration are gone. Tell the
    // host instead of dropping them silently.
    if (this.pendingGap) {
      const gap = this.pendingGap;
      this.pendingGap = null;
      const since = gap.lastMessageId != null
        ? `after message id ${gap.lastMessageId}` +
          (gap.lastMessageTimestamp != null
            ? ` (${new Date(gap.lastMessageTimestamp * 1000).toISOString()})`
            : '')
        : 'since the last delivered message';
      this.emitSystemEvent(onSystemEvent, {
        kind: 'gap',
        text:
          `Zulip event queue ${gap.queueId} expired and was re-registered as ${this.queueId}. ` +
          `Messages arriving ${since} and before re-registration may have been missed. ` +
          `Check recent channel history from that point if continuity matters.`,
        metadata: {
          platform: 'zulip',
          expiredQueueId: gap.queueId,
          lastMessageId: gap.lastMessageId,
          lastMessageTimestamp: gap.lastMessageTimestamp,
          newQueueId: this.queueId,
        },
      });
    }

    while (!this.stopped) {
      try {
        const response = await zulipClient.events.retrieve({
          queue_id: this.queueId,
          last_event_id: lastEventId,
        });

        // Queue died server-side. This is a *returned* error object, not a
        // thrown exception — node-fetch doesn't throw on 4xx and the JSON
        // error body parses cleanly, so zulip-js hands it back as a value.
        // Classify on the structured `code` before treating the missing
        // `events` array as a malformed response.
        if (response?.result === 'error' && response.code === 'BAD_EVENT_QUEUE_ID') {
          console.error('Zulip event queue expired, re-registering...');
          this.pendingGap = {
            queueId: this.queueId ?? 'unknown',
            lastMessageId: this.lastMessageId,
            lastMessageTimestamp: this.lastMessageTimestamp,
          };
          return; // Exit inner loop to re-register in the outer loop.
        }

        if (!response || !Array.isArray(response.events)) {
          // Malformed but non-throwing response — without a delay this
          // would hot-spin the poll and peg a CPU core.
          this.consecutiveFailures++;
          const delay = this.backoffDelay(this.consecutiveFailures);
          console.error(
            `Zulip events.retrieve returned no events array (attempt ${this.consecutiveFailures}); retrying in ${delay}ms`,
          );
          this.maybeEmitDegraded(onSystemEvent, 'events.retrieve returned a malformed response (no events array)');
          await this.sleep(delay);
          continue;
        }

        this.maybeEmitRecovered(onSystemEvent);
        this.consecutiveFailures = 0;

        for (const event of response.events) {
          lastEventId = event.id;

          if (event.type === 'message' && event.message) {
            const msg = event.message as ZulipEventMessage;
            // Anchor future gap markers on the last delivered message — the
            // event id is queue-local and useless once the queue is replaced.
            this.lastMessageId = msg.id;
            this.lastMessageTimestamp = msg.timestamp;

            // display_recipient is a string for stream messages, array for DMs
            const streamName = typeof msg.display_recipient === 'string'
              ? msg.display_recipient
              : null;

            if (streamName && msg.type === 'stream') {
              // A throwing onMessage is a handler bug, not a poll failure —
              // isolate it so it neither aborts the rest of the batch nor
              // inflates consecutiveFailures toward a bogus 'degraded' marker.
              // The event is already acked (lastEventId advanced); Zulip won't
              // redeliver it, so we log and move on (at-most-once).
              try {
                onMessage(streamName, msg, event.flags ?? []);
              } catch (handlerError) {
                console.error('Zulip event loop: onMessage handler threw:', handlerError);
              }
            }
          }
        }
      } catch (error: unknown) {
        if (this.stopped) return;

        // Only transport-level failures reach here — node-fetch `FetchError`s
        // for dropped connections, timeouts, and non-JSON (HTML/proxy) bodies.
        // Queue expiry is NOT among them (it's a returned object, handled
        // above), so there is deliberately no message-sniffing for 'queue_id':
        // every FetchError embeds the request URL (incl. ?queue_id=...), and
        // matching on that abandoned healthy queues on every transient blip.
        const errMsg = error instanceof Error ? error.message : String(error);

        this.consecutiveFailures++;
        const delay = this.backoffDelay(this.consecutiveFailures);
        const nonJson = this.isNonJsonError(error, errMsg);
        if (nonJson) {
          console.error(
            `Zulip event poll got a non-JSON response (proxy/HTML error page?) (attempt ${this.consecutiveFailures}); retrying in ${delay}ms: ${errMsg}`,
          );
        } else {
          console.error(`Zulip event poll error (attempt ${this.consecutiveFailures}); retrying in ${delay}ms:`, error);
        }
        this.maybeEmitDegraded(
          onSystemEvent,
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

  /**
   * Heuristic: did this error come from parsing a non-JSON (HTML/proxy) body?
   * node-fetch@2 rejects `response.json()` with a `FetchError` (name
   * 'FetchError', type 'invalid-json') whose message is
   * `invalid json response body at <url> reason: ...` — NOT a `SyntaxError`
   * (node-fetch wraps the parse error before it ever escapes). Match that
   * real shape.
   */
  private isNonJsonError(error: unknown, errMsg: string): boolean {
    const name = (error as { name?: string })?.name;
    const type = (error as { type?: string })?.type;
    return (
      type === 'invalid-json' ||
      (name === 'FetchError' && /invalid json response body/i.test(errMsg)) ||
      /invalid json response body|unexpected token|not valid json|<html|<!doctype/i.test(errMsg)
    );
  }

  /** Emit a 'degraded' event exactly once per outage (when crossing the threshold). */
  private maybeEmitDegraded(onSystemEvent: OnSystemEvent | undefined, reason: string): void {
    if (this.consecutiveFailures !== this.degradedThreshold) return;
    this.degradedActive = true;
    this.emitSystemEvent(onSystemEvent, {
      kind: 'degraded',
      text:
        `Zulip event polling has failed ${this.consecutiveFailures} times in a row (${reason}); ` +
        `real-time message delivery is degraded until it recovers.`,
      metadata: { platform: 'zulip', consecutiveFailures: this.consecutiveFailures, reason },
    });
  }

  /**
   * Emit a 'recovered' event once, when a poll succeeds after a 'degraded'
   * marker was raised — so the agent is told delivery is healthy again rather
   * than left holding the "degraded" belief forever.
   */
  private maybeEmitRecovered(onSystemEvent: OnSystemEvent | undefined): void {
    if (!this.degradedActive) return;
    const failures = this.consecutiveFailures;
    this.degradedActive = false;
    console.error(`Zulip event polling recovered after ${failures} consecutive failures`);
    this.emitSystemEvent(onSystemEvent, {
      kind: 'recovered',
      text:
        `Zulip event polling recovered after ${failures} consecutive failures; ` +
        `real-time message delivery is healthy again.`,
      metadata: { platform: 'zulip', recoveredAfter: failures },
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
