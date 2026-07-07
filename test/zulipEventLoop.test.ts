/**
 * Tests for ZulipEventLoop failure handling:
 *   - non-JSON (HTML/proxy 502) poll errors back off exponentially instead
 *     of retrying on a tight fixed interval, and emit a 'degraded' event;
 *   - queue expiry + re-register emits a 'gap' system event carrying the
 *     dead queue's last delivered event id;
 *   - a malformed non-throwing response (no `events` array) sleeps instead
 *     of hot-spinning the poll.
 *
 * Run: node --import tsx --test test/zulipEventLoop.test.ts
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { ZulipEventLoop } from '../src/platforms/zulip-events.ts';
import type { PlatformSystemEvent } from '../src/platforms/adapter.ts';

/** Sleep recorder that resolves instantly and can stop the loop after N sleeps. */
function makeSleepRecorder(loop: () => ZulipEventLoop, stopAfter: number) {
  const delays: number[] = [];
  const sleep = async (ms: number) => {
    delays.push(ms);
    if (delays.length >= stopAfter) loop().stop();
  };
  return { delays, sleep };
}

test('HTML/proxy 502 parse errors back off exponentially (no tight 2s loop)', async () => {
  let loop: ZulipEventLoop;
  const { delays, sleep } = makeSleepRecorder(() => loop, 6);
  const systemEvents: PlatformSystemEvent[] = [];

  loop = new ZulipEventLoop({ sleep });

  const zulipClient = {
    queues: {
      register: async () => ({ queue_id: 'q1', last_event_id: -1 }),
    },
    events: {
      // What zulip-js surfaces when a reverse proxy answers a long-poll
      // with an HTML 502 page: a JSON parse error, not a Zulip API error.
      retrieve: async () => {
        throw new SyntaxError('Unexpected token \'<\', "<html><hea"... is not valid JSON');
      },
    },
  };

  await loop.start(zulipClient, () => {}, (e) => systemEvents.push(e));

  // Six failures → six growing delays: 2s, 4s, 8s, 16s, 32s, 60s (capped).
  assert.deepEqual(delays, [2000, 4000, 8000, 16000, 32000, 60000]);
  for (let i = 1; i < delays.length - 1; i++) {
    assert.ok(delays[i] > delays[i - 1], `delay ${i} should grow`);
  }

  // Degraded condition surfaced exactly once (at the threshold), naming the cause.
  const degraded = systemEvents.filter(e => e.kind === 'degraded');
  assert.equal(degraded.length, 1);
  assert.match(degraded[0].text, /non-JSON/);
});

test('backoff caps at maxBackoffMs', async () => {
  let loop: ZulipEventLoop;
  const { delays, sleep } = makeSleepRecorder(() => loop, 5);

  loop = new ZulipEventLoop({ sleep, baseBackoffMs: 100, maxBackoffMs: 250 });

  const zulipClient = {
    queues: { register: async () => ({ queue_id: 'q1', last_event_id: -1 }) },
    events: { retrieve: async () => { throw new Error('ECONNRESET'); } },
  };

  await loop.start(zulipClient, () => {});
  assert.deepEqual(delays, [100, 200, 250, 250, 250]);
});

test('queue expiry + re-register emits a gap marker with the dead queue lastEventId', async () => {
  const systemEvents: PlatformSystemEvent[] = [];
  let registerCalls = 0;
  let retrieveCalls = 0;

  const loop = new ZulipEventLoop({ sleep: async () => {} });

  const zulipClient = {
    queues: {
      register: async () => {
        registerCalls++;
        return { queue_id: `q${registerCalls}`, last_event_id: -1 };
      },
    },
    events: {
      retrieve: async () => {
        retrieveCalls++;
        if (retrieveCalls === 1) {
          // Deliver one event so lastEventId advances to 42.
          return { events: [{ id: 42, type: 'heartbeat' }] };
        }
        if (retrieveCalls === 2) {
          // Queue died server-side.
          throw new Error('Zulip API error: BAD_EVENT_QUEUE_ID: Bad event queue ID: q1');
        }
        // First poll on the fresh queue — end the test.
        loop.stop();
        return { events: [] };
      },
    },
  };

  await loop.start(zulipClient, () => {}, (e) => systemEvents.push(e));

  assert.equal(registerCalls, 2, 'should re-register after expiry');

  const gaps = systemEvents.filter(e => e.kind === 'gap');
  assert.equal(gaps.length, 1, 'exactly one gap marker');
  assert.match(gaps[0].text, /may have been missed/);
  assert.equal(gaps[0].metadata?.expiredQueueId, 'q1');
  assert.equal(gaps[0].metadata?.lastEventId, 42);
  assert.equal(gaps[0].metadata?.newQueueId, 'q2');
});

test('malformed non-throwing response sleeps instead of hot-spinning', async () => {
  let loop: ZulipEventLoop;
  const { delays, sleep } = makeSleepRecorder(() => loop, 3);
  let retrieveCalls = 0;

  loop = new ZulipEventLoop({ sleep });

  const zulipClient = {
    queues: { register: async () => ({ queue_id: 'q1', last_event_id: -1 }) },
    events: {
      retrieve: async () => {
        retrieveCalls++;
        return {}; // no `events` array — previously `continue`d with zero delay
      },
    },
  };

  await loop.start(zulipClient, () => {});

  // Every malformed poll must be followed by a sleep — poll count tracks
  // sleep count instead of running away.
  assert.equal(retrieveCalls, 3);
  assert.deepEqual(delays, [2000, 4000, 8000]);
});

test('well-formed response resets the backoff counter', async () => {
  let loop: ZulipEventLoop;
  const { delays, sleep } = makeSleepRecorder(() => loop, 4);
  let retrieveCalls = 0;

  loop = new ZulipEventLoop({ sleep });

  const zulipClient = {
    queues: { register: async () => ({ queue_id: 'q1', last_event_id: -1 }) },
    events: {
      retrieve: async () => {
        retrieveCalls++;
        // fail, fail, succeed, then fail again — backoff must restart at base.
        if (retrieveCalls === 3) return { events: [] };
        throw new Error('ECONNRESET');
      },
    },
  };

  await loop.start(zulipClient, () => {});
  assert.deepEqual(delays, [2000, 4000, 2000, 4000]);
});
