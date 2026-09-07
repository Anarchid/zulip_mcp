/**
 * History — the one message shape every reader shares, the RFC-001 tags it
 * carries, and the cursor semantics of the fetch helpers against a fake
 * zulip-js client.
 *
 * Run: node --import tsx --test test/history.test.ts
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import {
  attachmentNote,
  channelIdOf,
  dmChannelIdFor,
  dmCounterparts,
  dmDescriptor,
  fetchAround,
  fetchHistory,
  isDmChannelId,
  normalizeMessage,
  parseDmChannelId,
  renderReactions,
  summarizeReactions,
  tagsFor,
  toIncoming,
  type ZulipRawMessage,
} from '../src/history.ts';

function raw(over: Partial<ZulipRawMessage> = {}): ZulipRawMessage {
  return {
    id: 42,
    sender_id: 7,
    sender_full_name: 'Ann',
    sender_email: 'ann@example.com',
    display_recipient: 'general',
    subject: 'deploys',
    content: 'ship it? [log](/user_uploads/1/ab/cd/log.txt) ![shot](/user_uploads/1/ab/ce/shot.png)',
    timestamp: 1_700_000_000,
    type: 'stream',
    flags: ['mentioned'],
    ...over,
  };
}

test('normalizeMessage reads the fields the rest of the server relies on', () => {
  const m = normalizeMessage(raw());
  assert.equal(m.id, 42);
  assert.equal(m.streamName, 'general');
  assert.equal(m.topic, 'deploys');
  assert.equal(m.isDm, false);
  assert.equal(m.mentioned, true);
  assert.equal(m.wildcardMentioned, false);
  assert.equal(m.timestamp.toISOString(), '2023-11-14T22:13:20.000Z');
  assert.deepEqual(m.attachments.map((a) => [a.name, a.isImage]), [['log.txt', false], ['shot.png', true]]);

  const dm = normalizeMessage(raw({ type: 'private', display_recipient: [{ email: 'x', full_name: 'X', id: 1 }], flags: [] }));
  assert.equal(dm.isDm, true);
  assert.equal(dm.streamName, null);
  assert.equal(dm.mentioned, false);
});

test('tagsFor emits the most specific addressing tag plus sender and content tags', () => {
  assert.deepEqual(tagsFor(normalizeMessage(raw())), ['chat:mention', 'chat:from-human', 'chat:has-image', 'chat:has-file']);
  assert.deepEqual(
    tagsFor(normalizeMessage(raw({ flags: ['wildcard_mentioned'], content: 'hi', sender_email: 'clerk-bot@example.com' }))),
    ['chat:ambient', 'zulip:wildcard-mention', 'chat:from-bot'],
  );
  assert.deepEqual(
    tagsFor(normalizeMessage(raw({ type: 'private', display_recipient: [], flags: [], content: 'hey' }))),
    ['chat:dm', 'chat:private', 'chat:from-human'],
  );
});

test('toIncoming renders the incoming shape, with the attachment note as a second block', () => {
  const m = normalizeMessage(raw());
  const incoming = toIncoming('zulip:general', m, { selfUserId: 790, sessionId: 's' }, { backscroll: true });
  assert.equal(incoming.channelId, 'zulip:general');
  assert.equal(incoming.messageId, '42');
  assert.equal(incoming.threadId, 'deploys');
  assert.deepEqual(incoming.author, { id: '7', name: 'Ann' });
  assert.equal(incoming.content.length, 2);
  assert.equal(incoming.content[0].type, 'text');
  assert.match((incoming.content[1] as { text: string }).text, /^\[attachments: 2\]/);
  const meta = incoming.metadata as Record<string, unknown>;
  assert.equal(meta.mentioned, true);
  assert.equal(meta.isDM, false);
  assert.equal(meta.topic, 'deploys');
  assert.equal(meta.botUserId, '790');
  assert.equal(meta.backscroll, true);
  assert.equal(attachmentNote([]), null);

  // Without a known self id the session id stands in, so a consumer can
  // still express "not from me".
  const anon = toIncoming('zulip:general', m, { selfUserId: null, sessionId: 'sess' });
  assert.equal((anon.metadata as Record<string, unknown>).botUserId, 'sess');
});

/** A zulip-js stand-in that records the query and answers with a fixed page. */
function fakeClient(rows: ZulipRawMessage[], extra: Record<string, unknown> = {}) {
  const calls: Record<string, unknown>[] = [];
  const byId: Record<string, unknown>[] = [];
  return {
    calls,
    byId,
    messages: {
      async retrieve(params: Record<string, unknown>) {
        calls.push(params);
        return { messages: rows, ...extra };
      },
      async getById(params: Record<string, unknown>) {
        byId.push(params);
        const found = rows.find((r) => r.id === params.message_id);
        return found ? { result: 'success', message: found } : { result: 'error', msg: 'Invalid message(s)' };
      },
    },
  };
}

test('fetchHistory: newest page by default, exclusive cursors, raw markdown, oldest first', async () => {
  const rows = [raw({ id: 3 }), raw({ id: 1 }), raw({ id: 2 })];
  const client = fakeClient(rows, { found_newest: true, found_oldest: false });

  const page = await fetchHistory(client, { streamName: 'general', topic: 'deploys', limit: 50 });
  assert.deepEqual(page.messages.map((m) => m.id), [1, 2, 3]);
  assert.equal(page.foundNewest, true);
  assert.equal(page.foundOldest, false);
  assert.deepEqual(client.calls[0], {
    anchor: 'newest',
    num_before: 50,
    num_after: 0,
    narrow: [['stream', 'general'], ['topic', 'deploys']],
    apply_markdown: false,
    include_anchor: true,
  });

  await fetchHistory(client, { streamName: 'general', limit: 10, after: 100 });
  assert.deepEqual(client.calls[1], {
    anchor: 100, num_before: 0, num_after: 10, narrow: [['stream', 'general']], apply_markdown: false, include_anchor: false,
  });

  await fetchHistory(client, { streamName: 'general', limit: 10, before: 100 });
  assert.deepEqual(client.calls[2], {
    anchor: 100, num_before: 10, num_after: 0, narrow: [['stream', 'general']], apply_markdown: false, include_anchor: false,
  });

  // limit 0 never hits the API; oversize limits are clamped to Zulip's page cap.
  const empty = await fetchHistory(client, { streamName: 'general', limit: 0 });
  assert.deepEqual(empty.messages, []);
  assert.equal(client.calls.length, 3);
  await fetchHistory(client, { streamName: 'general', limit: 99_999 });
  assert.equal(client.calls[3].num_before, 5000);
});

test('fetchAround centres a window on the anchor within its own conversation', async () => {
  const client = fakeClient([raw({ id: 41 }), raw({ id: 42 }), raw({ id: 43 })]);
  const page = await fetchAround(client, 42, 50);
  assert.deepEqual(page.messages.map((m) => m.id), [41, 42, 43]);
  assert.deepEqual(client.byId[0], { message_id: 42, apply_markdown: false });
  assert.deepEqual(client.calls[0], {
    anchor: 42, num_before: 25, num_after: 25, narrow: [['stream', 'general'], ['topic', 'deploys']], apply_markdown: false, include_anchor: true,
  });

  // A DM anchor narrows on its conversation.
  const dm = fakeClient([raw({ id: 5, type: 'private', display_recipient: [{ id: 790, full_name: 'Bot', email: 'b' }, { id: 42, full_name: 'Bo', email: 'bo' }] })]);
  await fetchAround(dm, 5, 10);
  assert.deepEqual(dm.calls[0].narrow, [{ operator: 'dm', operand: [790, 42] }]);

  // An unreadable anchor fails loudly instead of widening to the realm timeline.
  await assert.rejects(fetchAround(client, 999, 10), /Invalid message/);
});

test('DM channel ids are the sorted counterpart ids, bot excluded, and round-trip', () => {
  const recipients = [
    { id: 790, full_name: 'Bot', email: 'x-bot@example.com' },
    { id: 42, full_name: 'Bo', email: 'bo@example.com' },
    { id: 7, full_name: 'Al', email: 'al@example.com' },
  ];
  assert.deepEqual(dmCounterparts(recipients, 790).map((r) => r.id), [7, 42]);
  assert.equal(dmChannelIdFor([42, 7]), 'zulip:dm:7+42');
  assert.deepEqual(parseDmChannelId('zulip:dm:7+42'), [7, 42]);
  assert.equal(parseDmChannelId('zulip:general'), null);
  assert.equal(parseDmChannelId('zulip:dm:'), null);
  assert.equal(parseDmChannelId('zulip:dm:x'), null);
  assert.equal(isDmChannelId('zulip:dm:42'), true);

  // A self-DM is its own conversation rather than an empty one.
  assert.deepEqual(dmCounterparts([recipients[0]], 790).map((r) => r.id), [790]);

  const m = normalizeMessage(raw({ type: 'private', display_recipient: recipients, flags: [] }));
  assert.equal(channelIdOf(m, 790), 'zulip:dm:7+42');
  assert.equal(channelIdOf(normalizeMessage(raw()), 790), 'zulip:general');

  const single = dmDescriptor(dmCounterparts(recipients.slice(0, 2), 790), 100);
  assert.equal(single.id, 'zulip:dm:42');
  assert.equal(single.label, 'DM: Bo');
  assert.deepEqual(single.address, { dm: true, user_ids: [42], emails: ['bo@example.com'] });
  assert.equal((single.metadata as { recipientId: string }).recipientId, '42');
  // The host's conversation router keys on the Slack-derived spelling.
  assert.equal((single.metadata as { is_im?: boolean }).is_im, true);
  assert.equal((single.metadata as { is_mpim?: boolean }).is_mpim, undefined);
  const group = dmDescriptor(dmCounterparts(recipients, 790), 100);
  assert.equal(group.label, 'Group DM: Al, Bo');
  assert.equal((group.metadata as { recipientId?: string }).recipientId, undefined);
  assert.equal((group.metadata as { is_mpim?: boolean }).is_mpim, true);
});

test('fetchHistory narrows on a DM conversation when asked', async () => {
  const client = fakeClient([]);
  await fetchHistory(client, { dmUserIds: [7, 42], limit: 5 });
  // The object form: a pair with a list operand is refused by Zulip.
  assert.deepEqual(client.calls[0].narrow, [{ operator: 'dm', operand: [7, 42] }]);
});

test('reactions are bucketed by emoji and rendered with counts and self-marking', () => {
  const raw_ = raw({
    reactions: [
      { emoji_name: 'thumbs_up', emoji_code: '1f44d', reaction_type: 'unicode_emoji', user_id: 7 },
      { emoji_name: 'thumbs_up', emoji_code: '1f44d', reaction_type: 'unicode_emoji', user_id: 790 },
      { emoji_name: 'eyes', emoji_code: '1f440', reaction_type: 'unicode_emoji', user_id: 9 },
    ],
  });
  const m = normalizeMessage(raw_);
  assert.deepEqual(m.reactions, [
    { name: 'thumbs_up', code: '1f44d', type: 'unicode_emoji', count: 2, userIds: [7, 790] },
    { name: 'eyes', code: '1f440', type: 'unicode_emoji', count: 1, userIds: [9] },
  ]);
  assert.equal(renderReactions(m.reactions, 790), ' [reactions: :thumbs_up: x2 (incl. me), :eyes: x1]');
  assert.equal(renderReactions(m.reactions, null), ' [reactions: :thumbs_up: x2, :eyes: x1]');
  assert.equal(renderReactions([], 790), '');
  assert.deepEqual(summarizeReactions(undefined), []);
  assert.deepEqual((toIncoming('zulip:general', m, { selfUserId: 790, sessionId: 's' }).metadata as { reactions: unknown }).reactions, m.reactions);
  assert.equal((toIncoming('zulip:general', normalizeMessage(raw()), { selfUserId: 790, sessionId: 's' }).metadata as { reactions?: unknown }).reactions, undefined);
});
