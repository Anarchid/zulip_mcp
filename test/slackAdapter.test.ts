/**
 * Tests for SlackAdapter incoming-message handling (mention flag, filtering).
 *
 * Run: node --import tsx --test test/slackAdapter.test.ts
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { SlackAdapter } from '../src/platforms/slack.ts';
import type { ChannelIncomingMessage } from '../src/mcpl/types.ts';

function makeAdapter() {
  let handler: ((args: { event: any; ack: () => Promise<void> }) => Promise<void>) | undefined;
  const socket = {
    on(_event: string, h: any) { handler = h; },
    async start() {},
    async disconnect() {},
  } as any;
  const web = {
    users: {
      info: async ({ user }: { user: string }) => ({ user: { name: user.toLowerCase() } }),
    },
  } as any;
  const adapter = new SlackAdapter(web, socket, 'UBOT', 'acme');
  const received: ChannelIncomingMessage[] = [];
  adapter.startEvents(msg => received.push(msg));
  const emit = (event: any) => handler!({ event, ack: async () => {} });
  return { emit, received };
}

test('incoming message sets mentioned=true when the bot is mentioned', async () => {
  const { emit, received } = makeAdapter();
  await emit({
    type: 'message', channel: 'C1', user: 'U1',
    ts: '1718000000.000100', text: 'hey <@UBOT>, look at this',
  });
  assert.equal(received.length, 1);
  assert.equal(received[0].metadata?.mentioned, true);
  assert.deepEqual(received[0].metadata?.mentionIds, ['UBOT']);
});

test('incoming message sets mentioned=false for other mentions and broadcasts', async () => {
  const { emit, received } = makeAdapter();
  await emit({
    type: 'message', channel: 'C1', user: 'U1',
    ts: '1718000000.000200', text: '<!here> <@U2> can you take this?',
  });
  assert.equal(received.length, 1);
  assert.equal(received[0].metadata?.mentioned, false);
});

test('own and bot messages are filtered out', async () => {
  const { emit, received } = makeAdapter();
  await emit({ type: 'message', channel: 'C1', user: 'UBOT', ts: '1.0', text: 'self' });
  await emit({ type: 'message', channel: 'C1', user: 'U1', bot_id: 'B1', ts: '2.0', text: 'bot' });
  assert.equal(received.length, 0);
});
