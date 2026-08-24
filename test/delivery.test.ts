/**
 * Delivery bookkeeping — watermarks, the open mirror, missed tallies, and
 * the pure catch-up helpers (mention vicinity, <missed> rendering).
 *
 * Run: node --import tsx --test test/delivery.test.ts
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { IncomingChannelMessage } from '@animalabs/mcpl-core';
import { DeliveryState, renderMissedBlock, selectMissed, viewOf, type MissedView } from '../src/delivery.ts';

function tmp(): string {
  return mkdtempSync(join(tmpdir(), 'zulip-delivery-'));
}

test('watermarks only advance, and survive a restart', () => {
  const dir = tmp();
  try {
    const a = new DeliveryState(dir, 's1');
    assert.equal(a.watermark('zulip:general'), undefined);
    assert.equal(a.advance('zulip:general', 10), true);
    assert.equal(a.advance('zulip:general', 7), false, 'never retreats');
    assert.equal(a.advance('zulip:general', 12), true);
    a.save();

    const b = new DeliveryState(dir, 's1');
    assert.equal(b.watermark('zulip:general'), 12);
    assert.deepEqual(b.watermarkedChannels(), ['zulip:general']);

    // Sessions are isolated.
    const other = new DeliveryState(dir, 's2');
    assert.equal(other.watermark('zulip:general'), undefined);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('closing a channel starts a tally anchored at the watermark; reopening clears it', () => {
  const state = new DeliveryState(null, 'mem');
  state.advance('zulip:dev', 100);
  state.markOpen('zulip:dev');
  assert.equal(state.tally('zulip:dev'), undefined);
  assert.equal(state.countMissed('zulip:dev', { id: 101, text: 'x' }), false, 'open channels are not tallied');

  state.markClosed('zulip:dev');
  assert.deepEqual(state.tally('zulip:dev'), { anchorId: 100, talliedThrough: 100, messages: 0, characters: 0 });
  assert.equal(state.countMissed('zulip:dev', { id: 101, text: 'hello' }), true);
  assert.equal(state.countMissed('zulip:dev', { id: 103, text: 'world!' }), true);
  assert.deepEqual(state.tally('zulip:dev'), { anchorId: 100, talliedThrough: 103, messages: 2, characters: 11 });

  // Closing a channel that was never open changes nothing.
  state.markClosed('zulip:never');
  assert.equal(state.tally('zulip:never'), undefined);

  state.markOpen('zulip:dev');
  assert.equal(state.tally('zulip:dev'), undefined);
  assert.equal(state.wasOpen('zulip:dev'), true);
});

test('backfill folds fetched ambient into the tally and moves the cursor', () => {
  const state = new DeliveryState(null, 'mem');
  state.markOpen('zulip:dev');
  state.markClosed('zulip:dev');
  state.backfillMissed('zulip:dev', [{ id: 5, text: 'ab' }, { id: 6, text: 'cde' }], 9);
  assert.deepEqual(state.tally('zulip:dev'), { anchorId: 0, talliedThrough: 9, messages: 2, characters: 5 });
});

test('the state file is round-tripped, sorted, and tolerant of garbage', () => {
  const dir = tmp();
  try {
    const a = new DeliveryState(dir, 's');
    a.advance('zulip:b', 2);
    a.advance('zulip:a', 1);
    a.markOpen('zulip:b');
    a.markOpen('zulip:a');
    a.markClosed('zulip:a');
    a.countMissed('zulip:a', { id: 3, text: 'xyz' });
    a.save();

    const raw = JSON.parse(readFileSync(join(dir, 's.delivery.json'), 'utf-8'));
    assert.deepEqual(Object.keys(raw.watermarks), ['zulip:a', 'zulip:b']);
    assert.deepEqual(raw.lastOpen, ['zulip:b']);
    assert.deepEqual(raw.missed['zulip:a'], { anchorId: 1, talliedThrough: 3, messages: 1, characters: 3 });

    const b = new DeliveryState(dir, 's');
    assert.equal(b.watermark('zulip:b'), 2);
    assert.equal(b.wasOpen('zulip:b'), true);
    assert.equal(b.wasOpen('zulip:a'), false);
    assert.deepEqual(b.tally('zulip:a'), { anchorId: 1, talliedThrough: 3, messages: 1, characters: 3 });

    // A corrupt file is reported and ignored, not fatal.
    const dirty = tmp();
    try {
      const path = join(dirty, 'x.delivery.json');
      writeFileSync(path, '{not json');
      const c = new DeliveryState(dirty, 'x');
      assert.deepEqual(c.watermarkedChannels(), []);
    } finally {
      rmSync(dirty, { recursive: true, force: true });
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

function msg(id: number, mentioned: boolean, text = `m${id}`): IncomingChannelMessage {
  return {
    channelId: 'zulip:general',
    messageId: String(id),
    threadId: 'topic-a',
    author: { id: '9', name: 'Ann' },
    timestamp: new Date(1_700_000_000_000 + id * 1000).toISOString(),
    content: [{ type: 'text', text }, ...(id % 2 ? [] : [{ type: 'text' as const, text: '[attachments: 1]\n- shot.png' }])],
    tags: [],
    metadata: { topic: 'topic-a', mentioned, attachments: id % 2 ? undefined : [{ name: 'shot.png' }] },
  };
}

test('viewOf reads the sweep fields off an incoming message', () => {
  const v = viewOf(msg(4, true, 'hi there'));
  assert.equal(v.id, 4);
  assert.equal(v.mentioned, true);
  assert.equal(v.topic, 'topic-a');
  assert.equal(v.authorName, 'Ann');
  assert.equal(v.text, 'hi there', 'the attachment note is not part of the text');
  assert.deepEqual(v.attachmentNames, ['shot.png']);
  // A DM is addressed even without a mention flag.
  assert.equal(viewOf({ ...msg(5, false), metadata: { isDM: true } }).mentioned, true);
});

test('selectMissed keeps every message for a channel the host had open, else mentions with vicinity', () => {
  const views: MissedView[] = Array.from({ length: 30 }, (_, i) => viewOf(msg(i + 1, i + 1 === 10 || i + 1 === 25)));
  assert.equal(selectMissed(views, { keepAll: true, vicinity: 2 }).length, 30);

  const kept = selectMissed(views, { keepAll: false, vicinity: 2 });
  assert.deepEqual(kept.map((v) => v.id), [8, 9, 10, 11, 12, 23, 24, 25, 26, 27]);

  // Windows overlap and clamp at the edges without duplicating.
  const edge = selectMissed(views.slice(0, 3).map((v) => ({ ...v, mentioned: v.id === 1 })), { keepAll: false, vicinity: 5 });
  assert.deepEqual(edge.map((v) => v.id), [1, 2, 3]);

  assert.deepEqual(selectMissed(views.map((v) => ({ ...v, mentioned: false })), { keepAll: false, vicinity: 7 }), []);
});

test('renderMissedBlock leads with ids, flags mentions, and reports counts by reason', () => {
  const views = [viewOf(msg(1, false, 'context')), viewOf(msg(2, true, 'ping')), viewOf(msg(3, false, 'after'))];
  const block = renderMissedBlock(views, {
    streamName: 'general',
    channelId: 'zulip:general',
    reason: 'mention',
    count: 1,
    formatTime: () => '12:00',
  });
  const lines = block.split('\n');
  assert.equal(lines[0], '<missed stream="#general" channelId="zulip:general" count="1" lines="3" reason="mention">');
  assert.equal(lines[1], '[12:00 id=1] [topic-a] Ann: context');
  assert.equal(lines[2], '[12:00 id=2] [topic-a] Ann (mention): ping [attachments: shot.png]');
  assert.equal(lines[4], '</missed>');

  const backscroll = renderMissedBlock(views, {
    streamName: 'general',
    channelId: 'zulip:general',
    reason: 'backscroll',
    count: 3,
    formatTime: () => '',
  });
  assert.match(backscroll, /^<missed stream="#general" channelId="zulip:general" count="3" reason="backscroll">/);
  assert.match(backscroll, /\n\[id=1\] /, 'an empty timestamp leaves the id alone');
});
