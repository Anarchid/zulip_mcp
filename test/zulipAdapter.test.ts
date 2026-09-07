/**
 * ZulipAdapter against a fake zulip-js client — what the allowlists do to
 * history reads, what a history page reports about what it scanned, and
 * that opening a channel asks Zulip about the subscription every time.
 *
 * Run: node --import tsx --test test/zulipAdapter.test.ts
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { ZulipAdapter, type FilterView } from '../src/platforms/zulip.ts';
import type { ZulipRawMessage } from '../src/history.ts';

const SELF = 790;

function raw(id: number, over: Partial<ZulipRawMessage> = {}): ZulipRawMessage {
  return {
    id, sender_id: 7, sender_full_name: 'Ann', sender_email: 'ann@example.com',
    display_recipient: 'general', subject: 'deploys', content: `m${id}`, timestamp: 1_700_000_000 + id, type: 'stream', flags: [],
    ...over,
  };
}

function dm(id: number, senderId: number): ZulipRawMessage {
  const people = [
    { id: SELF, full_name: 'Bot', email: 'bot@example.com' },
    { id: 7, full_name: 'Ann', email: 'ann@example.com' },
    { id: 42, full_name: 'Bo', email: 'bo@example.com' },
  ];
  const sender = people.find((p) => p.id === senderId)!;
  return raw(id, { type: 'private', display_recipient: people, sender_id: senderId, sender_full_name: sender.full_name, sender_email: sender.email });
}

function fakeClient(rows: ZulipRawMessage[]) {
  const calls: { retrieve: Record<string, unknown>[]; subscribe: unknown[] } = { retrieve: [], subscribe: [] };
  const client = {
    messages: {
      async retrieve(params: Record<string, unknown>) {
        calls.retrieve.push(params);
        const after = typeof params.anchor === 'number' && params.num_after ? params.anchor : null;
        const matching = rows
          .filter((m) => after === null || m.id > after)
          .filter((m) => {
            const narrow = params.narrow as unknown[][];
            const stream = narrow.find((n) => n[0] === 'stream')?.[1];
            const dmIds = narrow.find((n) => n[0] === 'dm')?.[1] as number[] | undefined;
            if (stream) return m.type === 'stream' && m.display_recipient === stream;
            if (dmIds) return m.type === 'private';
            return true;
          });
        const limit = Number(params.num_after || params.num_before);
        const page = after !== null ? matching.slice(0, limit) : matching.slice(-limit);
        return { result: 'success', messages: page, found_newest: page[page.length - 1] === matching[matching.length - 1], found_oldest: false };
      },
    },
    users: { me: { subscriptions: { add: async (p: unknown) => { calls.subscribe.push(p); return { result: 'success', subscribed: {}, already_subscribed: { 'bot@example.com': ['general'] } }; } } } },
  };
  return { client, calls };
}

const onlyDev: FilterView = { streamAllowed: (s) => s === 'dev', dmAllowed: (u) => u.id === 42 };

test('fetchHistory withholds the bot\'s own messages and disallowed senders, and reports the newest id it scanned', async () => {
  const { client } = fakeClient([raw(1, { sender_id: SELF }), raw(2, { sender_id: SELF }), raw(3), dm(4, 7), dm(5, 42), dm(6, SELF)]);
  const adapter = new ZulipAdapter(client, SELF, 's', { filters: onlyDev });

  // A stream outside the allowlist: nothing, without a fetch.
  const none = await adapter.fetchHistory('zulip:general', { limit: 10 });
  assert.deepEqual(none, { messages: [], scannedThrough: null, reachedNewest: true });

  // A DM conversation: the bot's own and the excluded sender's messages are
  // withheld, but the page scanned through all of them.
  const page = await adapter.fetchHistory('zulip:dm:7+42', { limit: 10, afterMessageId: '3' });
  assert.deepEqual(page.messages.map((m) => m.messageId), ['5']);
  assert.equal(page.scannedThrough, 6, 'the cursor covers the withheld rows');
  assert.equal(page.reachedNewest, true);
  assert.equal((page.messages[0].metadata as { backscroll: boolean }).backscroll, true);

  // A page of nothing but the bot's own messages is not the end of history.
  const allowAll: FilterView = { streamAllowed: () => true, dmAllowed: () => true };
  const wide = new ZulipAdapter(client, SELF, 's', { filters: allowAll });
  const own = await wide.fetchHistory('zulip:general', { limit: 2, afterMessageId: '0' });
  assert.deepEqual(own.messages, []);
  assert.equal(own.scannedThrough, 2);
  assert.equal(own.reachedNewest, false);
});

test('fetchContext injects nothing for a stream outside the allowlist, and only allowed senders of a DM', async () => {
  const { client, calls } = fakeClient([raw(3), dm(4, 7), dm(5, 42), dm(6, SELF)]);
  const adapter = new ZulipAdapter(client, SELF, 's', { filters: onlyDev });
  assert.equal(await adapter.fetchContext('zulip:general', undefined, 5), null);
  assert.equal(calls.retrieve.length, 0, 'not even fetched');
  const injection = await adapter.fetchContext('zulip:dm:7+42', undefined, 5);
  assert.ok(injection);
  assert.match(injection!.content, /Bo: m5/);
  assert.match(injection!.content, /Bot: m6/, 'the bot\'s own turns stay in context');
  assert.doesNotMatch(injection!.content, /Ann: m4/);
});

test('ensureSubscribed asks Zulip every time — a subscription dropped by unlisten is not remembered as live', async () => {
  const { client, calls } = fakeClient([]);
  const adapter = new ZulipAdapter(client, SELF, 's');
  await adapter.ensureSubscribed('zulip:general');
  await adapter.ensureSubscribed('zulip:general');
  assert.equal(calls.subscribe.length, 2);
  await adapter.ensureSubscribed('zulip:dm:42');
  assert.equal(calls.subscribe.length, 2, 'DMs need nothing');
});
