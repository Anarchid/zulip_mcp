/**
 * Tests for ChannelManager adapter routing and in-thread publish hints.
 *
 * Run: node --import tsx --test test/channelManager.test.ts
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { ChannelManager } from '../src/channels.ts';
import { CapabilityGrant } from '../src/grant.ts';
import type { PlatformAdapter, RoutingHints } from '../src/platforms/adapter.ts';
import type { ChannelDescriptor, ContentBlock } from '@animalabs/mcpl-core';

function fakeAdapter(type: string, descriptors: ChannelDescriptor[]) {
  const calls: { channelId: string; content: ContentBlock[]; hints?: RoutingHints }[] = [];
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
  registerChannels: async () => ({}),
  sendIncoming: async () => {},
} as any;

/**
 * A grant with the channel capabilities these tests exercise. There is no
 * ungated ChannelManager: SPEC 0.5 §5.4 makes absence the denial, so a test
 * has to say what the host granted.
 */
function grantedChannels(): CapabilityGrant {
  const grant = new CapabilityGrant();
  grant.apply({
    effectiveCapabilities: [
      'channels.register',
      'channels.lifecycle',
      'channels.publish',
      'channels.incoming',
    ],
  });
  return grant;
}

function makeManager() {
  const zulipDesc: ChannelDescriptor = {
    id: 'zulip:C1',
    type: 'zulip',
    label: '#general',
    direction: 'bidirectional',
    address: { channel_id: 'C1' },
  };
  const { adapter, calls } = fakeAdapter('zulip', [zulipDesc]);
  const adapters = new Map<string, PlatformAdapter>([['zulip', adapter]]);
  const manager = new ChannelManager(fakeMcplClient, adapters, grantedChannels(), 10);
  return { manager, calls };
}

test('adapterFor routes by channel ID prefix', () => {
  const { manager } = makeManager();
  assert.equal(manager.adapterFor('zulip:C1')?.type, 'zulip');
  assert.equal(manager.adapterFor('other:g:c'), undefined);
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
  manager.openChannel({ type: 'zulip' });

  manager.onIncomingMessage('zulip:C1', {
    channelId: 'zulip:C1',
    messageId: '1718012345.000200',
    threadId: '1718000000.000100',
    author: { id: 'U1', name: 'alice' },
    timestamp: new Date(0).toISOString(),
    content: [{ type: 'text', text: 'hello' }],
    metadata: { thread_ts: '1718000000.000100' },
  });

  await manager.publish({
    conversationId: '',
    channelId: 'zulip:C1',
    content: [{ type: 'text', text: 'reply' }],
  });

  assert.equal(calls.length, 1);
  assert.equal(calls[0].channelId, 'zulip:C1');
  assert.equal(calls[0].hints?.threadId, '1718000000.000100');
  assert.equal(calls[0].hints?.metadata?.thread_ts, '1718000000.000100');
  manager.destroy();
});

test('publish has no hints before any incoming message', async () => {
  const { manager, calls } = makeManager();
  await manager.registerChannels();

  await manager.publish({
    conversationId: '',
    channelId: 'zulip:C1',
    content: [{ type: 'text', text: 'first contact' }],
  });

  assert.equal(calls.length, 1);
  assert.equal(calls[0].hints, undefined);
  manager.destroy();
});

test('broadcastSystemEvent reaches open channels of the platform without clobbering thread hints', async () => {
  const sent: any[][] = [];
  const client = {
    registerChannels: async () => ({}),
    sendIncoming: async (messages: any[]) => { sent.push(messages); },
  } as any;

  const desc: ChannelDescriptor = {
    id: 'zulip:general',
    type: 'zulip',
    label: '#general',
    direction: 'bidirectional',
  };
  const { adapter, calls } = fakeAdapter('zulip', [desc]);
  const manager = new ChannelManager(client, new Map([['zulip', adapter]]), grantedChannels(), 10);
  await manager.registerChannels();
  manager.openChannel({ type: 'zulip' });

  // Real conversation establishes thread routing.
  manager.onIncomingMessage('zulip:general', {
    channelId: 'zulip:general',
    messageId: '7',
    threadId: 'deploys',
    author: { id: 'U1', name: 'alice' },
    timestamp: new Date(0).toISOString(),
    content: [{ type: 'text', text: 'hello' }],
    metadata: { topic: 'deploys' },
  });

  manager.broadcastSystemEvent('zulip', {
    kind: 'gap',
    text: 'queue expired; messages may have been missed',
    metadata: { expiredQueueId: 'q1', lastEventId: 42 },
  });

  await new Promise(resolve => setTimeout(resolve, 30)); // let the 10ms batch flush

  const flushed = sent.flat();
  const marker = flushed.find(m => m.metadata?.system === true);
  assert.ok(marker, 'gap marker delivered to host');
  assert.equal(marker.channelId, 'zulip:general');
  assert.equal(marker.author.id, 'system');
  assert.equal(marker.metadata.kind, 'gap');
  assert.equal(marker.metadata.lastEventId, 42);
  assert.match(marker.content[0].text, /missed/);

  // The system marker must not steal publish thread routing.
  await manager.publish({
    conversationId: '',
    channelId: 'zulip:general',
    content: [{ type: 'text', text: 'reply' }],
  });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].hints?.threadId, 'deploys');
  manager.destroy();
});

test('broadcastSystemEvent with no open channels does not throw', () => {
  const { manager } = makeManager();
  manager.broadcastSystemEvent('zulip', { kind: 'degraded', text: 'polling failing' });
  manager.destroy();
  assert.ok(true);
});

// --- Capability gating (SPEC 0.5 §5.4, §14.1, §14.5) ------------------------

function ungatedManager(client: any = fakeMcplClient) {
  const desc: ChannelDescriptor = {
    id: 'zulip:C1',
    type: 'zulip',
    label: '#general',
    direction: 'bidirectional',
    address: { channel_id: 'C1' },
  };
  const { adapter, calls } = fakeAdapter('zulip', [desc]);
  // A grant that names nothing: §5.4 makes absence the denial.
  const grant = new CapabilityGrant();
  grant.apply({ effectiveCapabilities: [] });
  const manager = new ChannelManager(client, new Map([['zulip', adapter]]), grant, 10);
  return { manager, calls };
}

test('a channel method whose capability is denied answers -32002, not silence (§6.6, §14.6)', async () => {
  const { manager } = ungatedManager();
  for (const [capability, invoke] of [
    ['channels.lifecycle', () => manager.openChannel({ type: 'zulip' })],
    ['channels.lifecycle', () => manager.closeChannel({ channelId: 'zulip:C1' })],
    ['channels.register', () => manager.listChannels()],
    ['channels.publish', () => manager.publish({ conversationId: '', channelId: 'zulip:C1', content: [] })],
    ['channels.typing', () => manager.sendTyping('zulip:C1')],
  ] as [string, () => unknown][]) {
    let thrown: any;
    try {
      await invoke();
    } catch (error) {
      thrown = error;
    }
    assert.ok(thrown, `expected a denial for ${capability}`);
    assert.equal(thrown.code, -32002);
    assert.deepEqual(thrown.data, { capability });
  }
  manager.destroy();
});

test('registration is skipped without channels.register, and nothing is recorded as registered', async () => {
  let registered = false;
  const client = {
    registerChannels: async () => { registered = true; return {}; },
    sendIncoming: async () => {},
  } as any;
  const { manager } = ungatedManager(client);
  await manager.registerChannels();
  assert.equal(registered, false);
  manager.destroy();
});

test('a descriptor the host rejects is not recorded as registered (§14.5)', async () => {
  const client = {
    registerChannels: async () => ({
      results: [{ id: 'zulip:C1', accepted: false, reason: 'capability_denied' }],
    }),
    sendIncoming: async () => {},
  } as any;
  const { manager } = makeManagerWith(client);
  await manager.registerChannels();
  assert.deepEqual(manager.listChannels().channels, []);
  // ...and a channel that was never registered cannot then be opened.
  assert.throws(() => manager.openChannel({ type: 'zulip' }), /No channel found/);
  manager.destroy();
});

test('an itemized accept records exactly the accepted descriptors (§14.5)', async () => {
  const client = {
    registerChannels: async () => ({ results: [{ id: 'zulip:C1', accepted: true }] }),
    sendIncoming: async () => {},
  } as any;
  const { manager } = makeManagerWith(client);
  await manager.registerChannels();
  assert.deepEqual(manager.listChannels().channels.map((c) => c.id), ['zulip:C1']);
  manager.destroy();
});

test('inbound batches are dropped, not queued, without channels.incoming (§14.1)', async () => {
  const sent: any[][] = [];
  const client = {
    registerChannels: async () => ({ results: [{ id: 'zulip:C1', accepted: true }] }),
    sendIncoming: async (messages: any[]) => { sent.push(messages); },
  } as any;
  const desc: ChannelDescriptor = {
    id: 'zulip:C1', type: 'zulip', label: '#general', direction: 'bidirectional',
  };
  const { adapter } = fakeAdapter('zulip', [desc]);
  const grant = new CapabilityGrant();
  grant.apply({ effectiveCapabilities: ['channels.register', 'channels.lifecycle'] });
  const manager = new ChannelManager(client, new Map([['zulip', adapter]]), grant, 10);

  await manager.registerChannels();
  manager.openChannel({ type: 'zulip' });
  manager.onIncomingMessage('zulip:C1', {
    channelId: 'zulip:C1',
    messageId: '1',
    author: { id: 'U1', name: 'alice' },
    timestamp: new Date(0).toISOString(),
    content: [{ type: 'text', text: 'hello' }],
  });

  await new Promise((resolve) => setTimeout(resolve, 30));
  assert.deepEqual(sent, []);
  manager.destroy();
});

function makeManagerWith(client: any) {
  const desc: ChannelDescriptor = {
    id: 'zulip:C1',
    type: 'zulip',
    label: '#general',
    direction: 'bidirectional',
    address: { channel_id: 'C1' },
  };
  const { adapter, calls } = fakeAdapter('zulip', [desc]);
  const manager = new ChannelManager(client, new Map([['zulip', adapter]]), grantedChannels(), 10);
  return { manager, calls };
}

test('incoming messages on unopened channels are ignored', () => {
  const { manager } = makeManager();
  // Channel never opened by host — should not record hints or buffer.
  manager.onIncomingMessage('zulip:C1', {
    channelId: 'zulip:C1',
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
