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
  type PushEventParams,
} from '@animalabs/mcpl-core';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { ChannelHistoryQuery, OnIncomingMessage, OnReaction, OnSystemEvent, PlatformAdapter, RoutingHints } from '../src/platforms/adapter.ts';
import { ZulipMcplServer, type ZulipMcplServerOptions } from '../src/server.ts';
import { FiltersPlane } from '../src/filters.ts';
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
  react: OnReaction | null;
  /** The stream's messages, oldest first; fetchHistory pages over them by id. */
  history: IncomingChannelMessage[];
  historyCalls: { channelId: string; query: ChannelHistoryQuery }[];
  subscribed: string[];
  acked: string[];
  deleted: string[];
  /** Streams whose subscription fails, with the reason. */
  subscribeError: Map<string, string>;
  /** The platform's page ceiling (Zulip: 5000); smaller than the ask forces paging. */
  pageCap: number;
}

function fakeAdapter(withTyping = true): FakeAdapter {
  const adapter: FakeAdapter = {
    type: 'zulip',
    published: [],
    typing: [],
    emit: null,
    systemEvent: null,
    react: null,
    history: [],
    historyCalls: [],
    subscribed: [],
    acked: [],
    deleted: [],
    subscribeError: new Map(),
    pageCap: Infinity,
    async discoverChannels() { return [DESCRIPTOR]; },
    async fetchHistory(channelId, query) {
      adapter.historyCalls.push({ channelId, query });
      let rows = adapter.history.filter((m) => m.channelId === channelId);
      if (query.afterMessageId !== undefined) rows = rows.filter((m) => Number(m.messageId) > Number(query.afterMessageId));
      if (query.beforeMessageId !== undefined) rows = rows.filter((m) => Number(m.messageId) < Number(query.beforeMessageId));
      const limit = Math.min(query.limit, adapter.pageCap);
      rows = query.afterMessageId !== undefined ? rows.slice(0, limit) : rows.slice(-limit);
      return rows.map((m) => ({ ...m, metadata: { ...(m.metadata as object), backscroll: true } }));
    },
    async ensureSubscribed(channelId) {
      const reason = adapter.subscribeError.get(channelId);
      if (reason) throw new Error(reason);
      adapter.subscribed.push(channelId);
    },
    async acknowledge(_channelId, messageId, value) { adapter.acked.push(`${messageId}:${value ?? ''}`); return `:${value ?? 'eyes'}:`; },
    async deleteMessage(_channelId, messageId) { adapter.deleted.push(messageId); },
    async publish(channelId, _descriptor, content, hints) {
      adapter.published.push({ channelId, content, hints });
      const n = adapter.published.length;
      return { delivered: true, messageId: String(40 + n), messageIds: [String(40 + n)] };
    },
    async fetchContext(channelId): Promise<ContextInjection> {
      return { namespace: channelId, position: 'beforeUser', content: 'recent history' };
    },
    startEvents(onMessage, onSystemEvent, onReaction) {
      adapter.emit = onMessage;
      adapter.systemEvent = onSystemEvent ?? null;
      adapter.react = onReaction ?? null;
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
  /** Set by the server; the real runtime reports every message a tool sends. */
  onSent: null as null | ((sent: { messageId: string; channelId: string; content: string }) => void),
  async handleToolCall(name: string, args: Record<string, unknown>) {
    fakeTools.calls.push({ name, args });
    if (name === 'explode') throw new Error('boom');
    if (name === 'send_message') fakeTools.onSent?.({ messageId: '99', channelId: 'zulip:general', content: String(args.content ?? '') });
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
  pushed: PushEventParams[];
  served: Promise<void>;
  /** How the host answers channels/incoming: per-message rejections, and
   *  whole batches that error out (counted down per batch). */
  policy: { rejectIds: Set<string>; failIncomingBatches: number };
  close(): Promise<void>;
}

function harness(opts: { mcpl?: boolean; typing?: boolean; stateDir?: string; sessionId?: string; history?: IncomingChannelMessage[]; filters?: FiltersPlane; attachments?: ZulipMcplServerOptions['attachments'] } = {}): Harness {
  const toServer = new PassThrough();
  const toHost = new PassThrough();
  const serverConn = McplConnection.fromStreams(toServer, toHost);
  const host = McplConnection.fromStreams(toHost, toServer);

  const adapter = fakeAdapter(opts.typing ?? true);
  if (opts.history) adapter.history = opts.history;
  const server = new ZulipMcplServer(adapter, fakeTools as unknown as ZulipToolRuntime, {
    serverInfo: { name: 'zulip-mcp-test', version: '0.0.0' },
    mcplEnabled: opts.mcpl ?? true,
    batchWindowMs: 5,
    contextHistorySize: 3,
    stateDir: opts.stateDir ?? null,
    sessionId: opts.sessionId ?? 'test',
    catchupLimit: 100,
    formatTime: () => 'T',
    filters: opts.filters,
    attachments: opts.attachments,
  });

  const hostSaw: JsonRpcRequest[] = [];
  const incoming: IncomingChannelMessage[] = [];
  const pushed: PushEventParams[] = [];
  const policy = { rejectIds: new Set<string>(), failIncomingBatches: 0 };
  // The host reactor: answer every server→host Request the way conhost does.
  host.on('request', (req) => {
    hostSaw.push(req);
    if (req.method === method.CHANNELS_REGISTER) {
      const p = req.params as ChannelsRegisterParams;
      host.sendResponse(req.id, { results: p.channels.map((c) => ({ id: c.id, accepted: true })) });
    } else if (req.method === method.CHANNELS_INCOMING) {
      const p = req.params as ChannelsIncomingParams;
      if (policy.failIncomingBatches > 0) {
        policy.failIncomingBatches--;
        host.sendError(req.id, -32603, 'host hiccup');
        return;
      }
      incoming.push(...p.messages.filter((m) => !policy.rejectIds.has(m.messageId)));
      host.sendResponse(req.id, { results: p.messages.map((m) => ({ messageId: m.messageId, accepted: !policy.rejectIds.has(m.messageId) })) });
    } else if (req.method === method.PUSH_EVENT) {
      pushed.push(req.params as PushEventParams);
      host.sendResponse(req.id, { accepted: true });
    } else if (req.method === method.CHANNELS_CHANGED) {
      const p = req.params as { added?: ChannelDescriptor[] };
      host.sendResponse(req.id, { results: (p.added ?? []).map((c) => ({ id: c.id, accepted: true })) });
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
    pushed,
    served,
    policy,
    async close() {
      // EOF on the server's stdin analog: readline closes on 'end', not on
      // destroy, so ending the stream is what actually returns serve().
      toServer.end();
      await served;
      await server.shutdown();
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
  'pushEvents',
  'channels.acknowledge',
  'channels.streaming',
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
 *  channels/list is the observable that it did. Live delivery follows the
 *  catch-up sweep, which is what most cases go on to exercise. */
async function awaitRegistered(h: Harness, andLive = true): Promise<void> {
  await until(() => h.adapter.emit !== null, 'event delivery to start');
  const deadline = Date.now() + 2000;
  while (true) {
    const listed = (await h.host.sendRequest(method.CHANNELS_LIST)) as { channels: ChannelDescriptor[] };
    if (listed.channels.length > 0) break;
    if (Date.now() > deadline) throw new Error('timed out waiting for registration');
    await new Promise((r) => setTimeout(r, 5));
  }
  if (andLive) await until(() => h.server.isLive, 'live delivery');
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
  assert.deepEqual(published, { delivered: true, messageId: '41' });
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

// --- delivery model ---------------------------------------------------------

function streamMsg(id: number, over: Partial<IncomingChannelMessage> & { mentioned?: boolean; text?: string } = {}): IncomingChannelMessage {
  const { mentioned = false, text = `msg ${id}`, ...rest } = over;
  return {
    channelId: 'zulip:general',
    messageId: String(id),
    threadId: 'deploys',
    author: { id: '9', name: 'Ann' },
    timestamp: new Date(1_700_000_000_000 + id * 1000).toISOString(),
    content: [{ type: 'text', text }],
    tags: [mentioned ? 'chat:mention' : 'chat:ambient', 'chat:from-human'],
    metadata: { topic: 'deploys', mentioned, isDM: false },
    ...rest,
  };
}

async function settled(h: Harness): Promise<void> {
  await h.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
  await awaitRegistered(h);
}

test('a mention on a closed channel is pushed, ambient is tallied, and channel_missed reports it', async () => {
  const h = harness();
  await initialize(h, true);
  await settled(h);

  // Open then close: closing is what starts the tally.
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.adapter.emit!(streamMsg(1));
  await until(() => h.incoming.length === 1, 'open-channel delivery');
  assert.equal(h.server.delivery.watermark('zulip:general'), 1);
  await h.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });

  h.adapter.emit!(streamMsg(2, { text: 'chatter' }));
  h.adapter.emit!(streamMsg(3, { text: 'more chatter' }));
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(h.incoming.length, 1, 'ambient on a closed channel is not delivered');
  assert.equal(h.pushed.length, 0);
  assert.equal(h.server.delivery.watermark('zulip:general'), 1, 'a dropped message does not advance the watermark');

  h.adapter.emit!(streamMsg(4, { mentioned: true, text: '@bot ping' }));
  await until(() => h.pushed.length === 1, 'push/event for the mention');
  const push = h.pushed[0];
  assert.equal(push.featureSet, 'zulip.messaging');
  assert.equal(push.eventId, 'zulip_msg_4');
  assert.deepEqual(push.tags, ['chat:mention', 'chat:from-human']);
  const origin = push.origin as Record<string, unknown>;
  assert.equal(origin.mcplChannelId, 'zulip:general');
  assert.equal(origin.stream, 'general');
  assert.equal(origin.isMention, true);
  assert.equal(origin.missedMessages, 2);
  assert.equal(origin.missedCharacters, 'chatter'.length + 'more chatter'.length);
  // The watermark moves once the host has acknowledged the push, not when
  // the request leaves — an unacknowledged forward is not a forward.
  await until(() => h.server.delivery.watermark('zulip:general') === 4, 'watermark after acknowledged push');

  const missed = (await h.host.sendRequest('tools/call', { name: 'channel_missed', arguments: { channel: '#general' } })) as {
    content: { text: string }[];
  };
  const report = JSON.parse(missed.content[0].text);
  assert.equal(report.tracked, true);
  assert.equal(report.missedMessages, 2);
  assert.equal(report.sinceMessageId, 1);

  // Reopening ends the tally.
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  const cleared = JSON.parse(
    ((await h.host.sendRequest('tools/call', { name: 'channel_missed', arguments: { channel: 'general' } })) as { content: { text: string }[] }).content[0].text,
  );
  assert.equal(cleared.tracked, false);
  assert.equal(cleared.open, true);
  await h.close();
});

test('channels/open returns capped history before the lifecycle commits, and subscribes the bot (§14.4)', async () => {
  const h = harness({ history: Array.from({ length: 12 }, (_, i) => streamMsg(i + 1)) });
  await initialize(h, true);
  await settled(h);

  const opened = (await h.host.sendRequest(method.CHANNELS_OPEN, {
    channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 5 },
  })) as { channel: ChannelDescriptor; history?: IncomingChannelMessage[]; historyTruncated?: boolean };
  assert.deepEqual(opened.history!.map((m) => m.messageId), ['8', '9', '10', '11', '12']);
  assert.equal(opened.historyTruncated, false);
  assert.equal((opened.history![0].metadata as { backscroll: boolean }).backscroll, true);
  assert.deepEqual(h.adapter.subscribed, ['zulip:general']);
  assert.equal(h.server.delivery.watermark('zulip:general'), 12, 'returned history counts as forwarded');

  // A request past the descriptor's cap is truncated and says so.
  const capped = (await h.host.sendRequest(method.CHANNELS_OPEN, {
    channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 9999 },
  })) as { historyTruncated?: boolean; history?: unknown[] };
  assert.equal(capped.historyTruncated, true);

  // sinceLastSeen pages from the watermark.
  h.adapter.history.push(streamMsg(13), streamMsg(14));
  const since = (await h.host.sendRequest(method.CHANNELS_OPEN, {
    channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 50, sinceLastSeen: true },
  })) as { history?: IncomingChannelMessage[] };
  assert.deepEqual(since.history!.map((m) => m.messageId), ['13', '14']);
  const lastCall = h.adapter.historyCalls[h.adapter.historyCalls.length - 1];
  assert.equal(lastCall.query.afterMessageId, '12');
  await h.close();
});

test('the reconnect sweep delivers what arrived while offline, by what the host had open', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-sweep-'));
  try {
    // Session 1: general open, watermark at 3; dev only watermarked (closed).
    const first = harness({ stateDir: dir, sessionId: 'sw' });
    await initialize(first, true);
    await settled(first);
    await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    first.adapter.emit!(streamMsg(3));
    await until(() => first.incoming.length === 1, 'delivery');
    first.server.delivery.advance('zulip:dev', 50);
    first.server.delivery.save();
    await first.close();

    // Session 2: new messages exist beyond both watermarks.
    const later = [
      streamMsg(4, { text: 'while you were away' }),
      streamMsg(5, { mentioned: true, text: '@bot are you back?' }),
      ...Array.from({ length: 20 }, (_, i) => streamMsg(51 + i, { channelId: 'zulip:dev', text: `dev ${51 + i}`, mentioned: 51 + i === 60 })),
    ];
    const second = harness({ stateDir: dir, sessionId: 'sw', history: later });
    // dev must be a registered channel for the sweep to consider it.
    second.adapter.discoverChannels = async () => [DESCRIPTOR, { ...DESCRIPTOR, id: 'zulip:dev', label: '#dev', address: { stream_name: 'dev', stream_id: 8 } }];
    await initialize(second, true);
    await settled(second);
    await until(() => second.pushed.length === 2, 'two catch-up pushes');

    const general = second.pushed.find((p) => (p.origin as { mcplChannelId: string }).mcplChannelId === 'zulip:general')!;
    const generalText = (general.payload.content[0] as { text: string }).text;
    assert.match(generalText, /^<missed stream="#general" channelId="zulip:general" count="2" reason="backscroll">/);
    assert.match(generalText, /\[T id=4\] \[deploys\] Ann: while you were away/);
    assert.match(generalText, /\[T id=5\] \[deploys\] Ann \(mention\): @bot are you back\?/);
    assert.ok(general.tags!.includes('zulip:missed'));
    assert.ok(general.tags!.includes('chat:mention'));

    const dev = second.pushed.find((p) => (p.origin as { mcplChannelId: string }).mcplChannelId === 'zulip:dev')!;
    const devText = (dev.payload.content[0] as { text: string }).text;
    assert.match(devText, /count="1" lines="15" reason="mention"/, 'closed channel: the mention plus ±7 vicinity');
    assert.doesNotMatch(devText, /dev 51\b/, 'far ambient is not replayed');

    assert.equal(second.server.delivery.watermark('zulip:general'), 5);
    assert.equal(second.server.delivery.watermark('zulip:dev'), 70, 'advanced past everything scanned');
    await second.close();

    // Session 3: nothing new → nothing pushed, and the sweep runs once.
    const third = harness({ stateDir: dir, sessionId: 'sw', history: later });
    await initialize(third, true);
    await settled(third);
    await new Promise((r) => setTimeout(r, 30));
    assert.equal(third.pushed.length, 0);
    await third.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a queue-expiry gap is healed from history for open channels before the marker is delivered', async () => {
  const h = harness({ history: [streamMsg(1), streamMsg(2), streamMsg(3)] });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.adapter.emit!(streamMsg(1));
  await until(() => h.incoming.length === 1, 'delivery');

  h.adapter.systemEvent!({ kind: 'gap', text: 'Queue expired.', metadata: { platform: 'zulip' } });
  await until(() => h.incoming.length === 4, 'recovered messages + marker');
  assert.deepEqual(h.incoming.slice(1, 3).map((m) => m.messageId), ['2', '3']);
  assert.ok(h.incoming[1].tags!.includes('zulip:missed'));
  assert.equal((h.incoming[1].metadata as { recovered: boolean }).recovered, true);
  const marker = h.incoming[3];
  assert.equal((marker.metadata as { kind: string; recoveredMessages: number }).kind, 'gap');
  assert.equal((marker.metadata as { recoveredMessages: number }).recoveredMessages, 2);
  assert.match((marker.content[0] as { text: string }).text, /2 message\(s\) on open channels were recovered/);
  assert.equal(h.server.delivery.watermark('zulip:general'), 3);
  await h.close();
});

test('a DM from a new conversation registers its channel, is pushed with a reply affordance, and can be answered', async () => {
  const h = harness();
  await initialize(h, true);
  await settled(h);

  const dmDesc: ChannelDescriptor = {
    id: 'zulip:dm:42',
    type: 'zulip',
    label: 'DM: Bo',
    direction: 'bidirectional',
    address: { dm: true, user_ids: [42], emails: ['bo@example.com'] },
    metadata: { channelType: 'dm', recipientName: 'Bo', recipientId: '42' },
  };
  const dm: IncomingChannelMessage = {
    channelId: 'zulip:dm:42',
    messageId: '500',
    author: { id: '42', name: 'Bo' },
    timestamp: new Date().toISOString(),
    content: [{ type: 'text', text: 'hey, got a minute?' }],
    tags: ['chat:dm', 'chat:private', 'chat:from-human'],
    metadata: { isDM: true, mentioned: false },
  };
  h.adapter.emit!(dm, dmDesc);

  await until(() => h.pushed.length === 1, 'push for the DM');
  assert.ok(h.hostSaw.some((r) => r.method === method.CHANNELS_CHANGED), 'the new conversation was announced first');
  const listed = (await h.host.sendRequest(method.CHANNELS_LIST)) as { channels: ChannelDescriptor[] };
  assert.ok(listed.channels.some((c) => c.id === 'zulip:dm:42'));

  const push = h.pushed[0];
  assert.equal((push.origin as { isDM: boolean }).isDM, true);
  assert.equal((push.origin as { stream?: string }).stream, undefined);
  const first = (push.payload.content[0] as { text: string }).text;
  assert.match(first, /^<system>Direct message from Bo \(user id 42\)\. To reply, use send_dm\(\["42"\]\) or publish to channel zulip:dm:42/);
  assert.equal((push.payload.content[1] as { text: string }).text, 'hey, got a minute?');

  // The second message from the same conversation carries no affordance.
  await until(() => h.server.delivery.watermark('zulip:dm:42') === 500, 'watermark');
  h.adapter.emit!({ ...dm, messageId: '501', content: [{ type: 'text', text: 'still there?' }] });
  await until(() => h.pushed.length === 2, 'second push');
  assert.equal((h.pushed[1].payload.content[0] as { text: string }).text, 'still there?');

  // Opening the DM channel routes the conversation through channels/incoming,
  // and a publish reaches the adapter with the DM channel id.
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:dm:42', type: 'zulip', address: {} });
  h.adapter.emit!({ ...dm, messageId: '502', content: [{ type: 'text', text: 'ok' }] });
  await until(() => h.incoming.length === 1, 'incoming on the open DM');
  await h.host.sendRequest(method.CHANNELS_PUBLISH, {
    conversationId: 'c1', channelId: 'zulip:dm:42', content: [{ type: 'text', text: 'here now' }],
  });
  assert.equal(h.adapter.published[0].channelId, 'zulip:dm:42');
  await h.close();
});

test('a muted stream delivers nothing, and the filters tools read and write the plane', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-filters-wire-'));
  const original = console.error;
  console.error = () => {};
  try {
    const plane = new FiltersPlane(join(dir, 'filters.json'), {}, { pollMs: 60_000 });
    plane.start();
    const h = harness({ filters: plane });
    await initialize(h, true);
    await settled(h);
    await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });

    const call = async (name: string, args: Record<string, unknown>) =>
      (await h.host.sendRequest('tools/call', { name, arguments: args })) as { isError?: boolean; content: { text: string }[] };
    const json = (r: { content: { text: string }[] }) => JSON.parse(r.content[0].text);

    const mute = json(await call('mute_channel', { channel: '#general' }));
    assert.equal(mute.muted, true);
    assert.deepEqual(plane.current().mutedStreams, ['general']);

    h.adapter.emit!(streamMsg(1, { mentioned: true, text: '@bot?' }));
    h.adapter.emit!(streamMsg(2));
    await new Promise((r) => setTimeout(r, 25));
    assert.equal(h.incoming.length, 0, 'muted: not even a mention on an open channel gets through');
    assert.equal(h.pushed.length, 0);

    json(await call('unmute_channel', { channel: 'zulip:general' }));
    h.adapter.emit!(streamMsg(3));
    await until(() => h.incoming.length === 1, 'delivery after unmute');

    const got = json(await call('filters_get', {}));
    assert.equal(got.streams, null);
    assert.equal(got.dmUsers, null);
    assert.deepEqual(got.mutedStreams, []);
    assert.equal(got.plane.status, 'live');
    assert.equal(got.reactionSuppression.status, 'not-configured');

    // Removing the only allowed stream would empty the list — and an empty
    // allowlist is unrestricted, so the edit is refused rather than
    // silently re-opening everything.
    const emptied = await call('filters_update', { removeStreams: ['general'] });
    assert.equal(emptied.isError, true);
    assert.match(emptied.content[0].text, /last allowed stream/);
    assert.equal(plane.current().streams, undefined, 'nothing was written');

    // Removing from an unrestricted allowlist materializes it first; adding
    // a stream re-discovers and announces channels the host lacks.
    h.adapter.discoverChannels = async () => [DESCRIPTOR, { ...DESCRIPTOR, id: 'zulip:dev', label: '#dev', address: { stream_name: 'dev', stream_id: 8 } }];
    const upd = json(await call('filters_update', { addStreams: ['dev'], removeStreams: ['general'], setDmUsers: ['42'] }));
    assert.deepEqual(upd.streams, ['dev']);
    assert.deepEqual(upd.dmUsers, ['42']);
    assert.match(upd.note, /materialized/);
    assert.deepEqual(upd.registered, ['zulip:dev']);
    assert.ok(h.hostSaw.some((r) => r.method === method.CHANNELS_CHANGED));
    assert.equal(plane.streamAllowed('general'), false);
    assert.equal(plane.streamAllowed('dev'), true);
    assert.equal(plane.dmAllowed({ id: 42, email: 'x' }), true);
    assert.equal(plane.dmAllowed({ id: 7, email: 'x' }), false);

    // Clearing the DM allowlist is allowed, and says what it means.
    const anyone = json(await call('filters_update', { setDmUsers: [] }));
    assert.equal(anyone.dmUsers, null);
    assert.match(anyone.note, /UNRESTRICTED/);

    // A DM cannot be muted; the error names the right lever.
    const bad = await call('mute_channel', { channel: 'zulip:dm:42' });
    assert.equal(bad.isError, true);
    assert.match(bad.content[0].text, /dmUsers/);
    plane.stop();
    await h.close();
  } finally {
    console.error = original;
    rmSync(dir, { recursive: true, force: true });
  }
});

test('attachments on live delivery are inlined from the configured source', async () => {
  const fetched: string[] = [];
  const h = harness({
    attachments: {
      source: {
        async fetch(path) {
          fetched.push(path);
          if (path.endsWith('.png')) {
            // A 1x1 PNG.
            const buf = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==', 'base64');
            return { buf, mimeType: 'image/png', overflow: false };
          }
          return { buf: Buffer.from('log line'), mimeType: 'text/plain', overflow: false };
        },
      },
      inline: { inlineImages: true, inlineTextMaxBytes: 5120, maxImages: 4 },
    },
  });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });

  h.adapter.emit!({
    ...streamMsg(1, { text: 'see attached' }),
    content: [{ type: 'text', text: 'see attached' }, { type: 'text', text: '[attachments: 2]\n- shot.png\n- run.log' }],
    metadata: {
      topic: 'deploys', mentioned: false, isDM: false,
      attachments: [
        { path: '/user_uploads/1/a/shot.png', name: 'shot.png', mimeType: 'image/png', isImage: true },
        { path: '/user_uploads/1/a/run.log', name: 'run.log', mimeType: 'text/plain', isImage: false },
      ],
    },
  });
  await until(() => h.incoming.length === 1, 'delivery');
  const content = h.incoming[0].content;
  assert.deepEqual(fetched, ['/user_uploads/1/a/shot.png', '/user_uploads/1/a/run.log']);
  assert.equal(content[0].type, 'text');
  assert.match((content[1] as { text: string }).text, /^\[attachments: 2\]/, 'the reference note stays');
  assert.equal(content[2].type, 'image');
  assert.equal((content[2] as { mimeType: string }).mimeType, 'image/png');
  assert.equal((content[3] as { text: string }).text, '[image attachment: shot.png]');
  assert.equal((content[4] as { text: string }).text, '[attachment: run.log (8B)]\nlog line');
  await h.close();
});

test('reactions surface only on channels opted in, never wake, and honour suppression', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-reactions-wire-'));
  const original = console.error;
  console.error = () => {};
  try {
    const plane = new FiltersPlane(join(dir, 'filters.json'), { ZULIP_SUPPRESSED_REACTIONS_BASELINE: 'biohazard' }, { pollMs: 60_000 });
    plane.start();
    const h = harness({ filters: plane });
    await initialize(h, true);
    await settled(h);
    await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });

    const reaction = (over: Partial<Parameters<OnReaction>[0]> = {}) => h.adapter.react!({
      action: 'add', channelId: 'zulip:general', messageId: '77', emoji: 'thumbs_up',
      reactorId: '9', reactorName: 'Ann', onOwnMessage: true, messageSnippet: 'ship it', timestamp: new Date(1_700_000_000_000),
      ...over,
    });

    // Default off: nothing surfaces.
    reaction();
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(h.incoming.length, 0);

    const on = JSON.parse(((await h.host.sendRequest('tools/call', { name: 'set_reaction_visibility', arguments: { channel: 'general', visible: true } })) as { content: { text: string }[] }).content[0].text);
    assert.equal(on.visible, true);
    assert.deepEqual(plane.current().reactionChannels, ['zulip:general']);

    reaction();
    await until(() => h.incoming.length === 1, 'reaction on the open channel');
    const r = h.incoming[0];
    assert.deepEqual(r.tags, ['chat:reaction']);
    assert.equal((r.content[0] as { text: string }).text, '[reaction] Ann reacted :thumbs_up: on your message — "ship it"');
    assert.equal((r.metadata as { reaction: boolean; targetMessageId: string }).targetMessageId, '77');
    assert.equal(h.server.delivery.watermark('zulip:general'), undefined, 'a reaction never advances the watermark');

    reaction({ action: 'remove', onOwnMessage: false, messageSnippet: null });
    await until(() => h.incoming.length === 2, 'reaction removal');
    assert.deepEqual(h.incoming[1].tags, ['chat:reaction-remove']);
    assert.equal((h.incoming[1].content[0] as { text: string }).text, '[reaction] Ann removed a reaction :thumbs_up: on message 77');

    // Suppressed emoji: no glyph, no event, nowhere.
    reaction({ emoji: 'biohazard' });
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(h.incoming.length, 2);

    // Closed channel with visibility on → push/event.
    await h.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });
    reaction({ emoji: 'eyes' });
    await until(() => h.pushed.length === 1, 'reaction push on a closed channel');
    assert.deepEqual(h.pushed[0].tags, ['chat:reaction']);
    assert.equal((h.pushed[0].origin as { reaction: boolean }).reaction, true);

    plane.stop();
    await h.close();
  } finally {
    console.error = original;
    rmSync(dir, { recursive: true, force: true });
  }
});

test('messaging tools mint checkpoints the host can roll back to; publishes and tool sends after it are deleted; acknowledge reacts; streaming terminators are inert', async () => {
  const h = harness();
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });

  const publish = (text: string) => h.host.sendRequest(method.CHANNELS_PUBLISH, { conversationId: 'c', channelId: 'zulip:general', content: [{ type: 'text', text }] });
  const call = async (name: string, args: Record<string, unknown>) =>
    (await h.host.sendRequest('tools/call', { name, arguments: args })) as { isError?: boolean; state?: { featureSet: string; checkpoint: string; parent: string | null } };

  await publish('one');
  // The only way a host learns a checkpoint is a tool result's `state` (§8):
  // a messaging tool mints one after its own send is on the record.
  const sent = await call('send_message', { content: 'via tool' });
  assert.equal(sent.state?.featureSet, 'zulip.messaging');
  assert.match(sent.state!.checkpoint, /^chk_/);
  assert.equal(sent.state!.parent, null);
  const afterOne = sent.state!.checkpoint;
  // A read-only tool carries no checkpoint; a later messaging tool chains.
  assert.equal((await call('list_streams', {})).state, undefined);
  const next = await call('add_reaction', { message_id: 1, emoji_name: 'eyes' });
  assert.equal(next.state!.parent, afterOne);
  await publish('two');
  await publish('three');

  const unknown = (await h.host.sendRequest(method.STATE_ROLLBACK, { featureSet: 'zulip.messaging', checkpoint: 'chk_nope' })) as { success: boolean; reason?: string };
  assert.equal(unknown.success, false);
  assert.match(unknown.reason!, /not found/);
  const wrongSet = (await h.host.sendRequest(method.STATE_ROLLBACK, { featureSet: 'zulip.context', checkpoint: afterOne })) as { success: boolean };
  assert.equal(wrongSet.success, false);

  const rolled = (await h.host.sendRequest(method.STATE_ROLLBACK, { featureSet: 'zulip.messaging', checkpoint: afterOne })) as { success: boolean; reason?: string };
  assert.equal(rolled.success, true);
  assert.equal(rolled.reason, undefined);
  assert.deepEqual(h.adapter.deleted, ['42', '43'], 'everything sent after the checkpoint (publish 41 and tool send 99 came before)');

  // Rolling back to the later checkpoint now undoes the tool's own send.
  const rolledFurther = (await h.host.sendRequest(method.STATE_ROLLBACK, { featureSet: 'zulip.messaging', checkpoint: afterOne })) as { success: boolean };
  assert.equal(rolledFurther.success, true);
  assert.deepEqual(h.adapter.deleted, ['42', '43'], 'nothing left to undo after the checkpoint');

  const ack = (await h.host.sendRequest(method.CHANNELS_ACKNOWLEDGE, { channelId: 'zulip:general', messageId: '7', intent: 'seen-not-opening', value: 'eyes' })) as { acknowledged: boolean; representation?: string };
  assert.deepEqual(ack, { acknowledged: true, representation: ':eyes:' });
  assert.deepEqual(h.adapter.acked, ['7:eyes']);

  // Streaming notifications are accepted and deliver nothing.
  h.host.sendNotification(method.CHANNELS_OUTGOING_CHUNK, { inferenceId: 'i', conversationId: 'c', channelId: 'zulip:general', index: 0, delta: 'never sent' });
  h.host.sendNotification(method.CHANNELS_OUTGOING_COMPLETE, { inferenceId: 'i', conversationId: 'c', channelId: 'zulip:general', content: [{ type: 'text', text: 'never sent' }] });
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(h.adapter.published.length, 3);
  await h.close();
});

// --- review fix round ---------------------------------------------------------

test('an open channel\'s watermark moves only on the host\'s acceptance; rejections hold it, failed batches retry once, shutdown flushes', async () => {
  const original = console.error;
  console.error = () => {};
  try {
    const h = harness();
    await initialize(h, true);
    await settled(h);
    await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });

    // The host itemizes: 2 is refused. The watermark stops short of it so
    // the next sweep can still offer it; 3 is delivered but not "forwarded".
    h.policy.rejectIds.add('2');
    h.adapter.emit!(streamMsg(1));
    h.adapter.emit!(streamMsg(2));
    h.adapter.emit!(streamMsg(3));
    await until(() => h.incoming.length === 2, 'the accepted pair');
    assert.deepEqual(h.incoming.map((m) => m.messageId), ['1', '3']);
    assert.equal(h.server.delivery.watermark('zulip:general'), 1, 'held below the rejected id');
    h.policy.rejectIds.clear();

    // A batch the host errors on is retried once and then delivered.
    h.policy.failIncomingBatches = 1;
    h.adapter.emit!(streamMsg(4));
    await until(() => h.incoming.length === 3, 'delivery after one retry');
    assert.equal(h.server.delivery.watermark('zulip:general'), 4);
    assert.equal(h.hostSaw.filter((r) => r.method === method.CHANNELS_INCOMING).length, 3, 'two batches for the first pair, two attempts for the retry');

    // Persistent failure: given up, watermark untouched, nothing pretends.
    h.policy.failIncomingBatches = 5;
    h.adapter.emit!(streamMsg(5));
    await new Promise((r) => setTimeout(r, 60));
    assert.equal(h.incoming.length, 3);
    assert.equal(h.server.delivery.watermark('zulip:general'), 4, 'a message the host never accepted is not forwarded');
    assert.equal(h.server.channelManager.pendingCount(), 0, 'and not queued forever');
    h.policy.failIncomingBatches = 0;

    // Shutdown on a live connection pushes the last window out instead of
    // cancelling it.
    h.adapter.emit!(streamMsg(6));
    assert.equal(h.server.channelManager.pendingCount(), 1);
    await h.server.shutdown();
    assert.equal(h.incoming[h.incoming.length - 1].messageId, '6');
    assert.equal(h.server.delivery.watermark('zulip:general'), 6);
    await h.close();
  } finally {
    console.error = original;
  }
});

test('a muted stream is silent on every surface: open-history, context injection, reactions, gap recovery, and the reconnect sweep', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-mute-everywhere-'));
  const original = console.error;
  console.error = () => {};
  try {
    const plane = new FiltersPlane(join(dir, 'filters.json'), { ZULIP_MUTED_STREAMS: 'general' }, { pollMs: 60_000 });
    plane.start();
    const h = harness({ filters: plane, history: [streamMsg(1), streamMsg(2, { mentioned: true }), streamMsg(3)] });
    await initialize(h, true);
    await settled(h);

    const opened = (await h.host.sendRequest(method.CHANNELS_OPEN, {
      channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 10 },
    })) as { history?: unknown[] };
    assert.deepEqual(opened.history, [], 'no backscroll from a muted stream');
    assert.equal(h.adapter.historyCalls.length, 0);

    const ctx = (await h.host.sendRequest(method.CONTEXT_BEFORE_INFERENCE, {
      inferenceId: 'i', conversationId: 'c', turnIndex: 0, userMessage: null,
      model: { id: 'm', vendor: 'v', contextWindow: 1, capabilities: [] },
    })) as { contextInjections: unknown[] };
    assert.deepEqual(ctx.contextInjections, [], 'no context injection for a muted open channel');

    await h.host.sendRequest('tools/call', { name: 'set_reaction_visibility', arguments: { channel: 'general', visible: true } });
    h.adapter.react!({ action: 'add', channelId: 'zulip:general', messageId: '1', emoji: 'eyes', reactorId: '9', reactorName: 'Ann', onOwnMessage: true, messageSnippet: null, timestamp: new Date() });
    h.adapter.systemEvent!({ kind: 'gap', text: 'Queue expired.', metadata: { platform: 'zulip' } });
    await until(() => h.incoming.length === 1, 'the gap marker itself');
    const marker = h.incoming[0];
    assert.equal((marker.metadata as { kind: string }).kind, 'gap');
    assert.equal((marker.metadata as { recoveredMessages: number }).recoveredMessages, 0, 'nothing recovered from the muted stream');
    assert.equal(h.adapter.historyCalls.length, 0);
    await h.close();

    // The reconnect sweep: a watermarked muted stream with new mentions pushes nothing.
    const first = harness({ stateDir: dir, sessionId: 'mute' });
    await initialize(first, true);
    await settled(first);
    first.server.delivery.advance('zulip:general', 1);
    first.server.delivery.save();
    await first.close();
    const second = harness({ stateDir: dir, sessionId: 'mute', filters: plane, history: [streamMsg(2, { mentioned: true }), streamMsg(3)] });
    await initialize(second, true);
    await settled(second);
    assert.equal(second.pushed.length, 0);
    assert.equal(second.adapter.historyCalls.length, 0, 'not even fetched');
    plane.stop();
    await second.close();
  } finally {
    console.error = original;
    rmSync(dir, { recursive: true, force: true });
  }
});

test('channels/open fails — and commits nothing — when the bot cannot subscribe to the stream', async () => {
  const h = harness({ history: [streamMsg(1)] });
  await initialize(h, true);
  await settled(h);
  h.adapter.subscribeError.set('zulip:general', 'not authorized to subscribe to #general (private stream; the bot must be invited)');

  await assert.rejects(
    h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 5 } }),
    (err: Error & { code?: number; data?: unknown }) => {
      assert.equal(err.code, -32024, 'ERR_CHANNEL_OPEN_FAILED');
      assert.match(err.message, /not authorized to subscribe to #general/);
      assert.deepEqual(err.data, { channelId: 'zulip:general' });
      return true;
    },
  );
  assert.equal(h.adapter.historyCalls.length, 0, 'history is not fetched for an open that failed');
  assert.equal(h.server.channelManager.isOpen('zulip:general'), false);
  assert.equal(h.server.delivery.wasOpen('zulip:general'), false);
  // Still closed: ambient goes nowhere, and a mention is pushed as usual.
  h.adapter.emit!(streamMsg(2));
  h.adapter.emit!(streamMsg(3, { mentioned: true }));
  await until(() => h.pushed.length === 1, 'mention on the still-closed channel');
  assert.equal(h.incoming.length, 0);

  // Once the subscription can succeed, the open goes through.
  h.adapter.subscribeError.clear();
  const opened = (await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} })) as { channel: ChannelDescriptor };
  assert.equal(opened.channel.id, 'zulip:general');
  assert.deepEqual(h.adapter.subscribed, ['zulip:general']);
  await h.close();
});

test('live events received before the catch-up sweep are held, so a live delivery cannot jump the watermark over the offline gap', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-prelive-'));
  try {
    const first = harness({ stateDir: dir, sessionId: 'pl' });
    await initialize(first, true);
    await settled(first);
    await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    first.adapter.emit!(streamMsg(3));
    await until(() => first.incoming.length === 1, 'delivery');
    await first.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });
    await first.close();

    // Session 2: the sweep's history fetch is slow; a live mention (6) and a
    // duplicate of something the sweep will deliver (5) arrive meanwhile.
    const second = harness({ stateDir: dir, sessionId: 'pl', history: [streamMsg(4), streamMsg(5, { mentioned: true })] });
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    const realFetch = second.adapter.fetchHistory!.bind(second.adapter);
    second.adapter.fetchHistory = async (channelId, query) => { await gate; return realFetch(channelId, query); };
    await initialize(second, true);
    await second.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
    await awaitRegistered(second, false);
    assert.equal(second.server.isLive, false);
    second.adapter.emit!(streamMsg(6, { mentioned: true, text: 'live @bot' }));
    second.adapter.emit!(streamMsg(5, { mentioned: true }));
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(second.pushed.length, 0, 'held until the sweep has run');
    release();
    await until(() => second.pushed.length === 2, 'the sweep block, then the live mention');
    const block = (second.pushed[0].payload.content[0] as { text: string }).text;
    assert.match(block, /^<missed /);
    assert.match(block, /id=4\]/);
    assert.match(block, /id=5\]/);
    assert.equal(second.pushed[1].eventId, 'zulip_msg_6');
    assert.equal(second.server.delivery.watermark('zulip:general'), 6);
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(second.pushed.length, 2, 'the duplicate of 5 was dropped, not pushed twice');
    await second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('gap recovery also catches up closed channels, paging through history up to the ceiling', async () => {
  const h = harness({ history: Array.from({ length: 250 }, (_, i) => streamMsg(i + 2, { mentioned: i + 2 === 50 })) });
  await initialize(h, true);
  await settled(h);
  h.adapter.pageCap = 30;
  h.server.delivery.advance('zulip:general', 1);

  h.adapter.systemEvent!({ kind: 'gap', text: 'Queue expired.', metadata: { platform: 'zulip' } });
  await until(() => h.pushed.length === 1, 'catch-up push for the closed channel');
  const calls = h.adapter.historyCalls.filter((c) => c.channelId === 'zulip:general');
  assert.deepEqual(calls.map((c) => c.query.afterMessageId), ['1', '31', '61', '91'], 'paged from the watermark, page by page');
  assert.deepEqual(calls.map((c) => c.query.limit), [100, 70, 40, 10], 'never over the ceiling in total');
  const block = (h.pushed[0].payload.content[0] as { text: string }).text;
  assert.match(block, /count="1" lines="15" reason="mention" truncated="true"/);
  assert.match(block, /catch-up ceiling was reached.*after=101/);
  assert.equal(h.server.delivery.watermark('zulip:general'), 101, 'advanced through everything scanned');
  assert.equal((h.pushed[0].origin as { truncated?: boolean }).truncated, true);
  await h.close();
});

test('disabling zulip.messaging stops incoming delivery and its tools until re-enabled', async () => {
  const h = harness();
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });

  h.host.sendNotification(method.FEATURE_SETS_UPDATE, { disabled: ['zulip.messaging'] });
  await new Promise((r) => setTimeout(r, 10));
  h.adapter.emit!(streamMsg(1));
  h.adapter.emit!(streamMsg(2, { mentioned: true }));
  await new Promise((r) => setTimeout(r, 25));
  assert.equal(h.incoming.length, 0, 'no channels/incoming under a disabled feature set');
  assert.equal(h.pushed.length, 0);
  assert.equal(h.server.delivery.watermark('zulip:general'), undefined, 'and nothing is pretended forwarded');
  const refresh = (await h.host.sendRequest('tools/call', { name: 'refresh_channels', arguments: {} })) as { isError?: boolean; content: { text: string }[] };
  assert.equal(refresh.isError, true);
  assert.match(refresh.content[0].text, /not enabled/);
  await h.close();
});
