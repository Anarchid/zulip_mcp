/**
 * The tool runtime against a fake zulip-js client — DM conversations in
 * fetch_history, reaction suppression on the legacy raw format, and API
 * errors that surface as errors.
 *
 * Run: node --import tsx --test test/toolRuntime.test.ts
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ZulipToolRuntime, stripSuppressedReactions } from '../src/tool-runtime.ts';
import type { ZulipSession } from '../src/zulip-client.ts';

function runtime(client: Record<string, unknown>): { tools: ZulipToolRuntime; dir: string } {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-tools-'));
  const session: ZulipSession = { client, selfUserId: 790, realm: 'https://z.example.com', authHeader: '', sessionId: 't' };
  const original = console.error;
  console.error = () => {};
  try {
    return { tools: new ZulipToolRuntime(session, dir), dir };
  } finally {
    console.error = original;
  }
}

test('fetch_history reads a DM conversation by its channel id, the form every catch-up note quotes', async () => {
  const calls: Record<string, unknown>[] = [];
  const client = {
    messages: {
      async retrieve(params: Record<string, unknown>) {
        calls.push(params);
        return { result: 'success', messages: [], found_newest: true, found_oldest: true };
      },
    },
  };
  const { tools, dir } = runtime(client);
  try {
    const dm = await tools.handleToolCall('fetch_history', { channel: 'zulip:dm:7+42', after: 100 });
    assert.deepEqual(calls[0].narrow, [{ operator: 'dm', operand: [7, 42] }]);
    assert.equal(dm.channelId, 'zulip:dm:7+42');
    const stream = await tools.handleToolCall('fetch_history', { channel: '#general', topic: 'deploys' });
    assert.deepEqual(calls[1].narrow, [['stream', 'general'], ['topic', 'deploys']]);
    assert.equal(stream.channelId, 'zulip:general');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('suppressed reactions are withheld from the raw format too', () => {
  const policy = { suppressed: (name: string, code?: string, type?: string) => name === 'biohazard' || (type === 'unicode_emoji' && code === '1f6d1') };
  const stripped = stripSuppressedReactions([
    { id: 1, reactions: [
      { emoji_name: 'biohazard', emoji_code: '2623', reaction_type: 'unicode_emoji', user_id: 1 },
      { emoji_name: 'octagonal_sign', emoji_code: '1f6d1', reaction_type: 'unicode_emoji', user_id: 1 },
      { emoji_name: 'eyes', emoji_code: '1f440', reaction_type: 'unicode_emoji', user_id: 2 },
    ] },
    { id: 2 },
  ], policy);
  assert.deepEqual(stripped[0].reactions, [{ emoji_name: 'eyes', emoji_code: '1f440', reaction_type: 'unicode_emoji', user_id: 2 }]);
  assert.deepEqual(stripped[1], { id: 2 });
});

test('get_channel_history hands the model a raw payload with suppressed reactions removed', async () => {
  const client = {
    messages: {
      async retrieve() {
        return { result: 'success', messages: [{ id: 5, subject: 'x', sender_full_name: 'Ann', content: 'hi', timestamp: 1_700_000_000, reactions: [
          { emoji_name: 'biohazard', emoji_code: '2623', reaction_type: 'unicode_emoji', user_id: 1 },
          { emoji_name: 'eyes', emoji_code: '1f440', reaction_type: 'unicode_emoji', user_id: 2 },
        ] }] };
      },
    },
  };
  const { tools, dir } = runtime(client);
  try {
    tools.setReactionPolicy({ suppressed: (name) => name === 'biohazard' });
    const out = await tools.handleToolCall('get_channel_history', { channel: 'general', format: 'raw', start_date: '2000-01-01', auto_monitor: false });
    assert.doesNotMatch(out.formatted_history, /biohazard/);
    assert.match(out.formatted_history, /eyes/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('find_user and list_emojis report a Zulip API error as an error, never as an empty result', async () => {
  const client = {
    users: { async retrieve() { return { result: 'error', msg: 'Invalid API key' }; } },
    emojis: { async retrieve() { return { result: 'error', msg: 'Invalid API key' }; } },
  };
  const { tools, dir } = runtime(client);
  try {
    await assert.rejects(tools.handleToolCall('find_user', { query: 'ann' }), /Zulip refused listing users: Invalid API key/);
    await assert.rejects(tools.handleToolCall('list_emojis', {}), /Zulip refused listing realm emoji: Invalid API key/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
