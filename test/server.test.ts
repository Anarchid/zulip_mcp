/**
 * ZulipMcplServer — the wire. A host talks to the server over an in-memory
 * stream pair through the same `McplConnection` both ends use in
 * production, so these cases exercise the actual JSON-RPC framing, the
 * initialize handshake, the §5.3 policy exchange, channel registration,
 * incoming delivery with tags, publish routing, and the tool surface in both
 * plain-MCP and MCPL modes.
 *
 * Run: node --import tsx --test test/server.test.ts
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { PassThrough } from 'node:stream';
import {
  McplConnection,
  method,
  type ChannelDescriptor,
  type ChannelsIncomingParams,
  type ChannelsRegisterParams,
  type ContentBlock,
  type ContextInjection,
  type IncomingChannelMessage,
  type JsonRpcRequest,
} from '@animalabs/mcpl-core';
import type { OnIncomingMessage, OnSystemEvent, PlatformAdapter, RoutingHints } from '../src/platforms/adapter.ts';
import { ZulipMcplServer } from '../src/server.ts';
import type { ZulipToolRuntime } from '../src/tool-runtime.ts';

const DESCRIPTOR: ChannelDescriptor = {
  id: 'zulip:general',
  type: 'zulip',
  label: '#general',
  direction: 'bidirectional',
  address: { stream_name: 'general', stream_id: 7 },
};

interface FakeAdapter extends PlatformAdapter {
  published: { channelId: string; content: ContentBlock[]; hints?: RoutingHints }[];
  typing: { channelId: string; op: string }[];
  emit: OnIncomingMessage | null;
  systemEvent: OnSystemEvent | null;
}

function fakeAdapter(withTyping = true): FakeAdapter {
  const adapter: FakeAdapter = {
    type: 'zulip',
    published: [],
    typing: [],
    emit: null,
    systemEvent: null,
    async discoverChannels() { return [DESCRIPTOR]; },
    async publish(channelId, _descriptor, content, hints) {
      adapter.published.push({ channelId, content, hints });
      return { delivered: true, messageId: '42' };
    },
    async fetchContext(channelId): Promise<ContextInjection> {
      return { namespace: channelId, position: 'beforeUser', content: 'recent history' };
    },
    startEvents(onMessage, onSystemEvent) {
      adapter.emit = onMessage;
      adapter.systemEvent = onSystemEvent ?? null;
    },
    stopEvents() { adapter.emit = null; },
  };
  if (withTyping) {
    adapter.sendTyping = async (channelId, _d, _m, op) => { adapter.typing.push({ channelId, op }); };
  }
  return adapter;
}

const fakeTools = {
  calls: [] as { name: string; args: Record<string, unknown> }[],
  async handleToolCall(name: string, args: Record<string, unknown>) {
    fakeTools.calls.push({ name, args });
    if (name === 'explode') throw new Error('boom');
    return { ok: true, name };
  },
  listResources() {
    return [{ uri: 'zulip://monitoring/status', name: 'status', description: '', mimeType: 'application/json' }];
  },
  async readResource(uri: string) {
    if (uri !== 'zulip://monitoring/status') throw new Error(`Unknown resource: ${uri}`);
    return { contents: [{ uri, mimeType: 'application/json', text: '{}' }] };
  },
};

interface Harness {
  server: ZulipMcplServer;
  adapter: FakeAdapter;
  host: McplConnection;
  /** Server→host requests the host reactor answered, in order. */
  hostSaw: JsonRpcRequest[];
  incoming: IncomingChannelMessage[];
  served: Promise<void>;
  close(): Promise<void>;
}

function harness(opts: { mcpl?: boolean; typing?: boolean } = {}): Harness {
  const toServer = new PassThrough();
  const toHost = new PassThrough();
  const serverConn = McplConnection.fromStreams(toServer, toHost);
  const host = McplConnection.fromStreams(toHost, toServer);

  const adapter = fakeAdapter(opts.typing ?? true);
  const server = new ZulipMcplServer(adapter, fakeTools as unknown as ZulipToolRuntime, {
    serverInfo: { name: 'zulip-mcp-test', version: '0.0.0' },
    mcplEnabled: opts.mcpl ?? true,
    batchWindowMs: 5,
    contextHistorySize: 3,
  });

  const hostSaw: JsonRpcRequest[] = [];
  const incoming: IncomingChannelMessage[] = [];
  // The host reactor: answer every server→host Request the way conhost does.
  host.on('request', (req) => {
    hostSaw.push(req);
    if (req.method === method.CHANNELS_REGISTER) {
      const p = req.params as ChannelsRegisterParams;
      host.sendResponse(req.id, { results: p.channels.map((c) => ({ id: c.id, accepted: true })) });
    } else if (req.method === method.CHANNELS_INCOMING) {
      const p = req.params as ChannelsIncomingParams;
      incoming.push(...p.messages);
      host.sendResponse(req.id, { results: p.messages.map((m) => ({ messageId: m.messageId, accepted: true })) });
    } else {
      host.sendError(req.id, -32601, `unexpected ${req.method}`);
    }
  });

  const served = server.serve(serverConn);
  return {
    server,
    adapter,
    host,
    hostSaw,
    incoming,
    served,
    async close() {
      // EOF on the server's stdin analog: readline closes on 'end', not on
      // destroy, so ending the stream is what actually returns serve().
      toServer.end();
      await served;
      server.shutdown();
      host.close();
    },
  };
}

async function initialize(h: Harness, mcpl: boolean) {
  const result = (await h.host.sendRequest(method.INITIALIZE, {
    protocolVersion: '2025-03-26',
    capabilities: mcpl ? { experimental: { mcpl: { version: '0.5', channels: true } } } : {},
    clientInfo: { name: 'test-host', version: '0' },
  })) as { protocolVersion: string; capabilities: Record<string, unknown>; serverInfo: { name: string } };
  h.host.sendNotification('notifications/initialized');
  return result;
}

const FULL_GRANT = [
  'tools',
  'channels.register',
  'channels.lifecycle',
  'channels.publish',
  'channels.incoming',
  'channels.typing',
  'contextHooks.beforeInference.inject.beforeUser',
];

/** Wait until `predicate` holds, polling — the server's async work is real. */
async function until(predicate: () => boolean, what: string, ms = 2000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 5));
  }
}

/** Registration completes only once the host's answer reaches the server;
 *  channels/list is the observable that it did. */
async function awaitRegistered(h: Harness): Promise<void> {
  await until(() => h.adapter.emit !== null, 'event delivery to start');
  const deadline = Date.now() + 2000;
  while (true) {
    const listed = (await h.host.sendRequest(method.CHANNELS_LIST)) as { channels: ChannelDescriptor[] };
    if (listed.channels.length > 0) return;
    if (Date.now() > deadline) throw new Error('timed out waiting for registration');
    await new Promise((r) => setTimeout(r, 5));
  }
}

// --- plain MCP ---------------------------------------------------------------

test('a plain-MCP client gets tools and resources, and no MCPL manifest', async () => {
  const h = harness();
  const init = await initialize(h, false);
  assert.equal(init.protocolVersion, '2025-03-26');
  assert.equal(init.serverInfo.name, 'zulip-mcp-test');
  assert.deepEqual(Object.keys(init.capabilities).sort(), ['resources', 'tools']);
  assert.equal(h.server.mcplMode, false);

  assert.deepEqual(await h.host.sendRequest('ping'), {});

  const tools = (await h.host.sendRequest('tools/list')) as { tools: { name: string }[] };
  assert.ok(tools.tools.some((t) => t.name === 'send_message'));
  assert.ok(tools.tools.some((t) => t.name === 'fetch_attachment'));

  const called = (await h.host.sendRequest('tools/call', { name: 'list_streams', arguments: { verbose: false } })) as {
    content: { type: string; text: string }[];
  };
  assert.deepEqual(JSON.parse(called.content[0].text), { ok: true, name: 'list_streams' });

  const failed = (await h.host.sendRequest('tools/call', { name: 'explode', arguments: {} })) as {
    isError?: boolean;
    content: { text: string }[];
  };
  assert.equal(failed.isError, true);
  assert.match(failed.content[0].text, /boom/);

  const resources = (await h.host.sendRequest('resources/list')) as { resources: { uri: string }[] };
  assert.equal(resources.resources[0].uri, 'zulip://monitoring/status');
  const read = (await h.host.sendRequest('resources/read', { uri: 'zulip://monitoring/status' })) as {
    contents: { text: string }[];
  };
  assert.equal(read.contents[0].text, '{}');

  // MCPL methods are not on offer to a client that did not negotiate MCPL.
  await assert.rejects(h.host.sendRequest(method.CHANNELS_LIST), /-32601/);
  await assert.rejects(h.host.sendRequest('no/such/method'), /-32601/);

  // Events are never started for a plain-MCP client.
  assert.equal(h.adapter.emit, null);
  await h.close();
});

test('an unknown MCP protocol revision is answered with the fallback', async () => {
  const h = harness();
  const result = (await h.host.sendRequest(method.INITIALIZE, {
    protocolVersion: '1999-01-01',
    capabilities: {},
    clientInfo: { name: 'old', version: '0' },
  })) as { protocolVersion: string };
  assert.equal(result.protocolVersion, '2024-11-05');
  await h.close();
});

// --- MCPL --------------------------------------------------------------------

test('initialize carries the 0.5 manifest, and mcpl/manifest answers with the same snapshot (§5.1, §17.4)', async () => {
  const h = harness();
  const init = await initialize(h, true);
  const manifest = (init.capabilities.experimental as { mcpl: Record<string, unknown> }).mcpl;
  assert.equal(manifest.version, '0.5');
  assert.ok(String(manifest.revision).startsWith('sha256:'));
  const featureSets = manifest.featureSets as Record<string, { uses: string[]; tagOntology?: unknown }>;
  assert.ok(featureSets['zulip.messaging'].uses.includes('channels.typing'));
  assert.ok(featureSets['zulip.messaging'].tagOntology);
  assert.ok(featureSets['zulip.context']);

  const answered = await h.host.sendRequest(method.MCPL_MANIFEST);
  assert.deepEqual(answered, manifest);
  await h.close();
});

test('nothing is available before the policy exchange; the Request form settles it and registration follows (§5.3, §6.7, §14.3)', async () => {
  const h = harness();
  await initialize(h, true);

  // Absence is denial: channel methods are refused with the capability named.
  await assert.rejects(h.host.sendRequest(method.CHANNELS_LIST), (err: Error & { code?: number; data?: unknown }) => {
    assert.equal(err.code, -32002);
    assert.deepEqual(err.data, { capability: 'channels.register' });
    return true;
  });
  // ...and so is the tool surface.
  await assert.rejects(
    h.host.sendRequest('tools/call', { name: 'list_streams', arguments: {} }),
    (err: Error & { code?: number }) => err.code === -32002,
  );
  assert.equal(h.hostSaw.length, 0, 'no channels/register before policy');

  const receipt = (await h.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT })) as {
    accepted: boolean;
    mode: string;
    unavailableFeatures: unknown[];
  };
  assert.equal(receipt.accepted, true);
  assert.equal(receipt.mode, 'full');
  assert.deepEqual(receipt.unavailableFeatures, []);

  await until(() => h.hostSaw.some((r) => r.method === method.CHANNELS_REGISTER), 'channels/register');
  const registered = h.hostSaw.find((r) => r.method === method.CHANNELS_REGISTER)!.params as ChannelsRegisterParams;
  assert.deepEqual(registered.channels.map((c) => c.id), ['zulip:general']);

  await until(() => h.adapter.emit !== null, 'event delivery to start');

  const listed = (await h.host.sendRequest(method.CHANNELS_LIST)) as { channels: ChannelDescriptor[] };
  assert.deepEqual(listed.channels.map((c) => c.id), ['zulip:general']);
  await h.close();
});

test('a degraded grant is reported as such and disables the tools of the feature set (§6.4, §6.7)', async () => {
  const h = harness();
  await initialize(h, true);
  const receipt = (await h.host.sendRequest(method.FEATURE_SETS_UPDATE, {
    effectiveCapabilities: ['tools', 'channels.register'],
  })) as { mode: string; unavailableFeatures: { featureSet: string; missingCapabilities: string[] }[] };
  assert.equal(receipt.mode, 'degraded');
  assert.ok(receipt.unavailableFeatures.some((f) => f.featureSet === 'zulip.messaging'));

  // A messaging tool is unavailable with its feature set; a plain lookup is not.
  const send = (await h.host.sendRequest('tools/call', { name: 'send_message', arguments: {} })) as { isError?: boolean };
  assert.equal(send.isError, true);
  const list = (await h.host.sendRequest('tools/call', { name: 'list_streams', arguments: {} })) as { isError?: boolean };
  assert.notEqual(list.isError, true);
  await h.close();
});

test('a malformed policy is rejected and fails closed (§5.4)', async () => {
  const h = harness();
  await initialize(h, true);
  await h.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
  await assert.rejects(
    h.host.sendRequest(method.FEATURE_SETS_UPDATE, {
      effectiveCapabilities: ['tools'],
      deniedCapabilities: ['tools'],
    }),
    (err: Error & { code?: number }) => err.code === -32602,
  );
  await assert.rejects(
    h.host.sendRequest('tools/call', { name: 'list_streams', arguments: {} }),
    (err: Error & { code?: number }) => err.code === -32002,
  );
  await h.close();
});

test('open → incoming with tags → publish routes to the topic of the conversation (§14)', async () => {
  const h = harness();
  await initialize(h, true);
  await h.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
  await awaitRegistered(h);

  // Not open yet: an incoming message on the channel goes nowhere.
  h.adapter.emit!({
    channelId: 'zulip:general',
    messageId: '1',
    author: { id: '9', name: 'Ann' },
    timestamp: new Date().toISOString(),
    content: [{ type: 'text', text: 'before open' }],
    tags: ['chat:ambient'],
  });
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(h.incoming.length, 0);

  const opened = (await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} })) as {
    channel: ChannelDescriptor;
  };
  assert.equal(opened.channel.id, 'zulip:general');

  h.adapter.emit!({
    channelId: 'zulip:general',
    messageId: '2',
    threadId: 'deploys',
    author: { id: '9', name: 'Ann' },
    timestamp: new Date().toISOString(),
    content: [{ type: 'text', text: 'ship it?' }],
    tags: ['chat:mention', 'chat:from-human'],
    metadata: { topic: 'deploys', mentioned: true },
  });
  await until(() => h.incoming.length === 1, 'channels/incoming');
  assert.equal(h.incoming[0].messageId, '2');
  assert.deepEqual(h.incoming[0].tags, ['chat:mention', 'chat:from-human']);

  const published = (await h.host.sendRequest(method.CHANNELS_PUBLISH, {
    conversationId: 'c1',
    channelId: 'zulip:general',
    content: [{ type: 'text', text: 'shipping' }],
  })) as { delivered: boolean; messageId?: string };
  assert.deepEqual(published, { delivered: true, messageId: '42' });
  assert.equal(h.adapter.published.length, 1);
  assert.equal(h.adapter.published[0].hints?.threadId, 'deploys');
  assert.equal((h.adapter.published[0].hints?.metadata as { topic: string }).topic, 'deploys');

  // Typing, both carriers.
  await h.host.sendRequest(method.CHANNELS_TYPING, { channelId: 'zulip:general', op: 'start' });
  h.host.sendNotification(method.CHANNELS_TYPING, { channelId: 'zulip:general', op: 'stop' });
  await until(() => h.adapter.typing.length === 2, 'typing');
  assert.deepEqual(h.adapter.typing.map((t) => t.op), ['start', 'stop']);

  // A platform system event reaches the open channel as a system message.
  h.adapter.systemEvent!({ kind: 'gap', text: 'queue expired', metadata: { platform: 'zulip' } });
  await until(() => h.incoming.length === 2, 'system event');
  assert.equal((h.incoming[1].metadata as { system: boolean; kind: string }).system, true);
  assert.equal(h.incoming[1].author.id, 'system');

  const closed = (await h.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' })) as { closed: boolean };
  assert.equal(closed.closed, true);
  await h.close();
});

test('context/beforeInference injects history for open channels under zulip.context (§10.1, §6.5)', async () => {
  const h = harness();
  await initialize(h, true);
  await h.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
  await awaitRegistered(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });

  const params = {
    inferenceId: 'i1',
    conversationId: 'c1',
    turnIndex: 0,
    userMessage: null,
    model: { id: 'm', vendor: 'v', contextWindow: 1, capabilities: [] },
  };
  const result = (await h.host.sendRequest(method.CONTEXT_BEFORE_INFERENCE, params)) as {
    featureSet: string;
    contextInjections: ContextInjection[];
  };
  assert.equal(result.featureSet, 'zulip.context');
  assert.equal(result.contextInjections.length, 1);
  assert.equal(result.contextInjections[0].position, 'beforeUser');

  // Disabling the feature set by Notification is a reduction, honoured at once.
  h.host.sendNotification(method.FEATURE_SETS_UPDATE, { disabled: ['zulip.context'] });
  await new Promise((r) => setTimeout(r, 10));
  const reduced = (await h.host.sendRequest(method.CONTEXT_BEFORE_INFERENCE, params)) as { contextInjections: unknown[] };
  assert.deepEqual(reduced.contextInjections, []);
  await h.close();
});

test('MCPL_ENABLED=false keeps an MCPL host on the plain-MCP surface', async () => {
  const h = harness({ mcpl: false });
  const init = await initialize(h, true);
  assert.equal(init.capabilities.experimental, undefined);
  assert.equal(h.server.mcplMode, false);
  await h.close();
});

test('the manifest omits channels.typing when the adapter cannot type', async () => {
  const h = harness({ typing: false });
  const init = await initialize(h, true);
  const manifest = (init.capabilities.experimental as { mcpl: { channels: { typing: boolean } } }).mcpl;
  assert.equal(manifest.channels.typing, false);
  await h.close();
});
