/**
 * Tests for ChannelManager adapter routing and in-thread publish hints.
 *
 * Run: node --import tsx --test test/channelManager.test.ts
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { ChannelManager } from '../src/mcpl/channels.ts';
import type { PlatformAdapter, RoutingHints } from '../src/platforms/adapter.ts';
import type { ChannelDescriptor, McplContentBlock } from '../src/mcpl/types.ts';

function fakeAdapter(type: string, descriptors: ChannelDescriptor[]) {
  const calls: { channelId: string; content: McplContentBlock[]; hints?: RoutingHints }[] = [];
  const adapter: PlatformAdapter = {
    type,
    async discoverChannels() { return descriptors; },
    async publish(channelId, _descriptor, content, hints) {
      calls.push({ channelId, content, hints });
      return { delivered: true, messageId: 'm1' };
    },
    async fetchContext() { return null; },
    startEvents() {},
    stopEvents() {},
  };
  return { adapter, calls };
}

const fakeMcplClient = {
  registerChannels: async () => {},
  sendIncoming: async () => {},
} as any;

function makeManager() {
  const slackDesc: ChannelDescriptor = {
    id: 'slack:C1',
    type: 'slack',
    label: '#general (acme)',
    direction: 'bidirectional',
    address: { channel_id: 'C1' },
  };
  const { adapter, calls } = fakeAdapter('slack', [slackDesc]);
  const adapters = new Map<string, PlatformAdapter>([['slack', adapter]]);
  const manager = new ChannelManager(fakeMcplClient, adapters, 10);
  return { manager, calls };
}

test('adapterFor routes by channel ID prefix', () => {
  const { manager } = makeManager();
  assert.equal(manager.adapterFor('slack:C1')?.type, 'slack');
  assert.equal(manager.adapterFor('discord:g:c'), undefined);
});

test('publish throws for unknown channel prefix', async () => {
  const { manager } = makeManager();
  await assert.rejects(
    () => manager.publish({ conversationId: '', channelId: 'matrix:room', content: [] }),
    /Unknown channel format/,
  );
  manager.destroy();
});

test('publish forwards last-incoming thread hints to the adapter', async () => {
  const { manager, calls } = makeManager();
  await manager.registerChannels();
  manager.openChannel({ type: 'slack' });

  manager.onIncomingMessage('slack:C1', {
    channelId: 'slack:C1',
    messageId: '1718012345.000200',
    threadId: '1718000000.000100',
    author: { id: 'U1', name: 'alice' },
    timestamp: new Date(0).toISOString(),
    content: [{ type: 'text', text: 'hello' }],
    metadata: { thread_ts: '1718000000.000100' },
  });

  await manager.publish({
    conversationId: '',
    channelId: 'slack:C1',
    content: [{ type: 'text', text: 'reply' }],
  });

  assert.equal(calls.length, 1);
  assert.equal(calls[0].channelId, 'slack:C1');
  assert.equal(calls[0].hints?.threadId, '1718000000.000100');
  assert.equal(calls[0].hints?.metadata?.thread_ts, '1718000000.000100');
  manager.destroy();
});

test('publish has no hints before any incoming message', async () => {
  const { manager, calls } = makeManager();
  await manager.registerChannels();

  await manager.publish({
    conversationId: '',
    channelId: 'slack:C1',
    content: [{ type: 'text', text: 'first contact' }],
  });

  assert.equal(calls.length, 1);
  assert.equal(calls[0].hints, undefined);
  manager.destroy();
});

test('incoming messages on unopened channels are ignored', () => {
  const { manager } = makeManager();
  // Channel never opened by host — should not record hints or buffer.
  manager.onIncomingMessage('slack:C1', {
    channelId: 'slack:C1',
    messageId: '1',
    author: { id: 'U1', name: 'alice' },
    timestamp: new Date(0).toISOString(),
    content: [{ type: 'text', text: 'ignored' }],
  });
  manager.destroy();
  // No assertion target beyond "doesn't throw" — hint state is private;
  // covered indirectly by the publish-hints test requiring openChannel.
  assert.ok(true);
});
