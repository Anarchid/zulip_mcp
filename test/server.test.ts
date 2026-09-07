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
import type { ChannelHistoryPage, ChannelHistoryQuery, OnIncomingMessage, OnReaction, OnSystemEvent, PlatformAdapter, RoutingHints } from '../src/platforms/adapter.ts';
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
    async fetchHistory(channelId, query): Promise<ChannelHistoryPage> {
      adapter.historyCalls.push({ channelId, query });
      let rows = adapter.history.filter((m) => m.channelId === channelId);
      if (query.afterMessageId !== undefined) rows = rows.filter((m) => Number(m.messageId) > Number(query.afterMessageId));
      if (query.beforeMessageId !== undefined) rows = rows.filter((m) => Number(m.messageId) < Number(query.beforeMessageId));
      const limit = Math.min(query.limit, adapter.pageCap);
      const page = query.afterMessageId !== undefined ? rows.slice(0, limit) : rows.slice(-limit);
      const newest = page[page.length - 1];
      return {
        messages: page.map((m) => ({ ...m, metadata: { ...(m.metadata as object), backscroll: true } })),
        scannedThrough: newest ? Number(newest.messageId) : null,
        reachedNewest: !newest || newest === rows[rows.length - 1],
      };
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
  /** How the host answers channels/incoming: per-message rejections (for
   *  good, or once), and whole batches that error out (counted down per batch). */
  policy: { rejectIds: Set<string>; rejectOnce: Set<string>; failIncomingBatches: number };
  close(): Promise<void>;
}

interface HarnessOptions {
  mcpl?: boolean;
  typing?: boolean;
  stateDir?: string;
  sessionId?: string;
  history?: IncomingChannelMessage[];
  filters?: FiltersPlane;
  attachments?: ZulipMcplServerOptions['attachments'];
  batchWindowMs?: number;
  /** A second connection to a server that already served one (the TCP case). */
  reuse?: { server: ZulipMcplServer; adapter: FakeAdapter };
}

function harness(opts: HarnessOptions = {}): Harness {
  const toServer = new PassThrough();
  const toHost = new PassThrough();
  const serverConn = McplConnection.fromStreams(toServer, toHost);
  const host = McplConnection.fromStreams(toHost, toServer);

  const adapter = opts.reuse?.adapter ?? fakeAdapter(opts.typing ?? true);
  if (opts.history) adapter.history = opts.history;
  const server = opts.reuse?.server ?? new ZulipMcplServer(adapter, fakeTools as unknown as ZulipToolRuntime, {
    serverInfo: { name: 'zulip-mcp-test', version: '0.0.0' },
    mcplEnabled: opts.mcpl ?? true,
    batchWindowMs: opts.batchWindowMs ?? 5,
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
  const policy = { rejectIds: new Set<string>(), rejectOnce: new Set<string>(), failIncomingBatches: 0 };
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
      const refused = new Set(p.messages.filter((m) => policy.rejectIds.has(m.messageId) || policy.rejectOnce.delete(m.messageId)).map((m) => m.messageId));
      incoming.push(...p.messages.filter((m) => !refused.has(m.messageId)));
      host.sendResponse(req.id, { results: p.messages.map((m) => ({ messageId: m.messageId, accepted: !refused.has(m.messageId) })) });
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
    // One name-shaped entry and one glyph, as the host's baseline carries them.
    const plane = new FiltersPlane(join(dir, 'filters.json'), { ZULIP_SUPPRESSED_REACTIONS_BASELINE: 'biohazard,🛑' }, { pollMs: 60_000 });
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

    // Suppressed emoji: no glyph, no event, nowhere — by name, or by the
    // codepoints a glyph-shaped baseline entry is matched on.
    reaction({ emoji: 'biohazard' });
    reaction({ emoji: 'octagonal_sign', emojiCode: '1f6d1', emojiType: 'unicode_emoji' });
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

test('the watermark never passes an undelivered message: a refusal or a lost batch holds it, the next answer replays them, shutdown flushes', async () => {
  const original = console.error;
  console.error = () => {};
  try {
    const h = harness({ history: Array.from({ length: 9 }, (_, i) => streamMsg(i + 1)) });
    await initialize(h, true);
    await settled(h);
    await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });

    // The host itemizes: 2 is refused (once). The watermark stops short of
    // it although 3, in the same batch, was accepted.
    h.policy.rejectOnce.add('2');
    h.adapter.emit!(streamMsg(1));
    h.adapter.emit!(streamMsg(2));
    h.adapter.emit!(streamMsg(3));
    await until(() => h.incoming.length === 2, 'the accepted pair');
    assert.deepEqual(h.incoming.map((m) => m.messageId), ['1', '3']);
    assert.equal(h.server.delivery.watermark('zulip:general'), 1, 'held below the refused id');
    // The host is answering, so the refused message is fetched back from
    // history and offered again; accepted now, the watermark catches up.
    await until(() => h.incoming.length === 3, 'the replay');
    assert.equal(h.incoming[2].messageId, '2');
    assert.ok(h.incoming[2].tags!.includes('zulip:missed'));
    assert.equal((h.incoming[2].metadata as { recovered: boolean }).recovered, true);
    await until(() => h.server.delivery.watermark('zulip:general') === 3, 'watermark through everything accepted');
    h.adapter.emit!(streamMsg(4));
    await until(() => h.server.delivery.watermark('zulip:general') === 4, 'watermark 4');

    // Two transport failures in a row — the attempt and its retry — then
    // the host recovers and accepts the next message: the lost one is not
    // buried under that acceptance.
    h.policy.failIncomingBatches = 2;
    h.adapter.emit!(streamMsg(5));
    await until(() => h.policy.failIncomingBatches === 0 && h.server.channelManager.pendingCount() === 0, 'given up after the retry');
    assert.equal(h.server.delivery.watermark('zulip:general'), 4, 'a message the host never accepted is not forwarded');
    assert.equal(h.server.delivery.floor('zulip:general'), 5, 'and holds the watermark below it');
    h.adapter.emit!(streamMsg(6));
    await until(() => h.incoming.some((m) => m.messageId === '6'), '6 accepted');
    assert.equal(h.server.delivery.watermark('zulip:general'), 4, 'still not past the lost message');
    await until(() => h.incoming.some((m) => m.messageId === '5'), '5 fetched back from history and accepted');
    await until(() => h.server.delivery.watermark('zulip:general') === 6, 'watermark 6 once 5 is in');
    assert.equal(h.server.delivery.floor('zulip:general'), undefined);

    // Shutdown on a live connection pushes the last window out instead of
    // cancelling it.
    h.adapter.emit!(streamMsg(7));
    assert.equal(h.server.channelManager.pendingCount(), 1);
    await h.server.shutdown();
    assert.equal(h.incoming[h.incoming.length - 1].messageId, '7');
    assert.equal(h.server.delivery.watermark('zulip:general'), 7);
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

test('a live message landing mid-drain queues behind the held ones instead of burying them', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-drain-'));
  try {
    const first = harness({ stateDir: dir, sessionId: 'dr' });
    await initialize(first, true);
    await settled(first);
    await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    first.adapter.emit!(streamMsg(3));
    await until(() => first.incoming.length === 1, 'delivery');
    await first.close();

    // Session 2: the sweep is slow, and the first held message carries an
    // attachment whose fetch stalls once the drain reaches it.
    let releaseSweep!: () => void;
    const sweepGate = new Promise<void>((r) => { releaseSweep = r; });
    let releaseFetch!: () => void;
    const fetchGate = new Promise<void>((r) => { releaseFetch = r; });
    const second = harness({
      stateDir: dir, sessionId: 'dr',
      attachments: {
        source: { async fetch() { await fetchGate; return { buf: Buffer.from('log line'), mimeType: 'text/plain', overflow: false }; } },
        inline: { inlineImages: true, inlineTextMaxBytes: 5120, maxImages: 4 },
      },
    });
    const realFetch = second.adapter.fetchHistory!.bind(second.adapter);
    second.adapter.fetchHistory = async (channelId, query) => { await sweepGate; return realFetch(channelId, query); };
    await initialize(second, true);
    await second.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
    await awaitRegistered(second, false);
    // The host reopens the channel at boot (without history, as AF does).
    await second.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    second.adapter.emit!({
      ...streamMsg(20, { text: 'see the log' }),
      metadata: { topic: 'deploys', mentioned: false, isDM: false, attachments: [{ path: '/user_uploads/1/a/run.log', name: 'run.log', mimeType: 'text/plain', isImage: false }] },
    });
    second.adapter.emit!(streamMsg(21));
    assert.equal(second.server.isLive, false);
    releaseSweep();
    // The drain is now stalled on 20's attachment fetch when a live message lands.
    await new Promise((r) => setTimeout(r, 25));
    assert.equal(second.server.isLive, false, 'delivery stays gated until the buffer is empty');
    second.adapter.emit!(streamMsg(22));
    await new Promise((r) => setTimeout(r, 25));
    assert.equal(second.incoming.length, 0, 'nothing overtakes the held messages');
    releaseFetch();
    await until(() => second.incoming.length === 3, 'all three, in order');
    assert.deepEqual(second.incoming.map((m) => m.messageId), ['20', '21', '22']);
    assert.ok(second.incoming[0].content.some((c) => c.type === 'text' && /run\.log/.test(c.text)), 'the stalled attachment was inlined');
    await until(() => second.server.delivery.watermark('zulip:general') === 22, 'watermark 22');
    assert.equal(second.server.isLive, true);
    await second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a narrowed allowlist is enforced on every surface: live, push, backscroll, context, gap markers, the sweep, and DMs by sender', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-allowlist-'));
  const original = console.error;
  console.error = () => {};
  try {
    const plane = new FiltersPlane(join(dir, 'filters.json'), {}, { pollMs: 60_000 });
    plane.start();
    const both = [DESCRIPTOR, { ...DESCRIPTOR, id: 'zulip:dev', label: '#dev', address: { stream_name: 'dev', stream_id: 8 } }];
    const h = harness({ filters: plane, stateDir: dir, sessionId: 'al' });
    h.adapter.discoverChannels = async () => both;
    await initialize(h, true);
    await settled(h);
    await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    h.adapter.emit!(streamMsg(1));
    await until(() => h.incoming.length === 1, 'delivery while allowed');
    const call = async (name: string, args: Record<string, unknown>) =>
      JSON.parse(((await h.host.sendRequest('tools/call', { name, arguments: args })) as { content: { text: string }[] }).content[0].text);

    // Narrow to #dev. The host still has #general registered and open.
    await call('filters_update', { addStreams: ['dev'], removeStreams: ['general'] });
    assert.equal(plane.streamAllowed('general'), false);
    h.adapter.history = [streamMsg(1), streamMsg(2, { mentioned: true }), streamMsg(3)];

    // Live: dropped; a mention: not pushed; nothing tallied or watermarked.
    h.adapter.emit!(streamMsg(2, { mentioned: true }));
    h.adapter.emit!(streamMsg(3));
    await new Promise((r) => setTimeout(r, 25));
    assert.equal(h.incoming.length, 1);
    assert.equal(h.pushed.length, 0);
    assert.equal(h.server.delivery.watermark('zulip:general'), 1);

    // Backscroll on open and context injection: nothing, and no fetch either.
    const reopened = (await h.host.sendRequest(method.CHANNELS_OPEN, {
      channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 10 },
    })) as { history?: unknown[] };
    assert.deepEqual(reopened.history, []);
    const ctx = (await h.host.sendRequest(method.CONTEXT_BEFORE_INFERENCE, {
      inferenceId: 'i', conversationId: 'c', turnIndex: 0, userMessage: null,
      model: { id: 'm', vendor: 'v', contextWindow: 1, capabilities: [] },
    })) as { contextInjections: unknown[] };
    assert.deepEqual(ctx.contextInjections, []);
    // Gap recovery: no replay from the stream. The marker itself is about the
    // connection, not the stream, and still rides the open channel.
    h.adapter.systemEvent!({ kind: 'gap', text: 'Queue expired.', metadata: { platform: 'zulip' } });
    await until(() => h.incoming.length === 2, 'the gap marker');
    assert.equal((h.incoming[1].metadata as { kind: string; recoveredMessages: number }).kind, 'gap');
    assert.equal((h.incoming[1].metadata as { recoveredMessages: number }).recoveredMessages, 0);
    assert.equal(h.adapter.historyCalls.length, 0, 'never fetched');
    h.server.delivery.advance('zulip:dev', 50);
    h.server.delivery.save();
    await h.close();

    // The reconnect sweep: #general is watermarked with new mentions beyond
    // the watermark, but outside the allowlist — not fetched, not pushed.
    // #dev, inside it, is caught up.
    const second = harness({
      filters: plane, stateDir: dir, sessionId: 'al',
      history: [streamMsg(2, { mentioned: true }), streamMsg(3), streamMsg(60, { channelId: 'zulip:dev', mentioned: true, text: 'dev mention' })],
    });
    second.adapter.discoverChannels = async () => both;
    await initialize(second, true);
    await settled(second);
    await until(() => second.pushed.length === 1, 'the dev catch-up');
    assert.equal((second.pushed[0].origin as { mcplChannelId: string }).mcplChannelId, 'zulip:dev');
    assert.ok(second.adapter.historyCalls.every((c) => c.channelId !== 'zulip:general'), 'the excluded stream is never fetched');

    // DMs are judged by sender: only user 42 may DM the bot now.
    const call2 = async (name: string, args: Record<string, unknown>) =>
      JSON.parse(((await second.host.sendRequest('tools/call', { name, arguments: args })) as { content: { text: string }[] }).content[0].text);
    await call2('filters_update', { setDmUsers: ['42'] });
    const dmFrom = (id: number): [IncomingChannelMessage, ChannelDescriptor] => [
      {
        channelId: `zulip:dm:${id}`, messageId: String(500 + id), author: { id: String(id), name: `User ${id}` },
        timestamp: new Date().toISOString(), content: [{ type: 'text', text: 'hi' }], tags: ['chat:dm', 'chat:private', 'chat:from-human'],
        metadata: { isDM: true, mentioned: false, senderEmail: `u${id}@example.com` },
      },
      {
        id: `zulip:dm:${id}`, type: 'zulip', label: `DM: User ${id}`, direction: 'bidirectional',
        address: { dm: true, user_ids: [id], emails: [`u${id}@example.com`] },
        metadata: { channelType: 'dm', participants: [{ id, name: `User ${id}`, email: `u${id}@example.com` }] },
      },
    ];
    second.adapter.emit!(...dmFrom(7));
    await new Promise((r) => setTimeout(r, 25));
    assert.equal(second.pushed.length, 1, 'a DM from an excluded sender goes nowhere');
    assert.ok(!second.hostSaw.some((r) => r.method === method.CHANNELS_CHANGED && JSON.stringify(r.params).includes('zulip:dm:7')), 'and is not even announced');
    second.adapter.emit!(...dmFrom(42));
    await until(() => second.pushed.length === 2, 'a DM from the allowed sender');
    plane.stop();
    await second.close();
  } finally {
    console.error = original;
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a feature set disabled while a message waits in the batch window withholds it; enabling it again replays it', async () => {
  const original = console.error;
  console.error = () => {};
  try {
    const h = harness({ batchWindowMs: 120, history: [streamMsg(1)] });
    await initialize(h, true);
    await settled(h);
    await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });

    h.adapter.emit!(streamMsg(1));
    h.host.sendNotification(method.FEATURE_SETS_UPDATE, { disabled: ['zulip.messaging'] });
    await new Promise((r) => setTimeout(r, 200));
    assert.equal(h.incoming.length, 0, 'withheld at send time, not only at enqueue');
    assert.equal(h.server.delivery.watermark('zulip:general'), undefined);
    assert.equal(h.server.delivery.floor('zulip:general'), 1, 'held, not forgotten: not heard is not the same as heard');

    await h.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
    await until(() => h.incoming.length === 1, 'replayed once the feature set is back');
    assert.equal(h.incoming[0].messageId, '1');
    assert.equal((h.incoming[0].metadata as { recovered: boolean }).recovered, true);
    await until(() => h.server.delivery.watermark('zulip:general') === 1, 'watermark 1');
    await h.close();
  } finally {
    console.error = original;
  }
});

test('a channel_open with backscroll during the sweep window does not hide the offline gap from the sweep', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-sweep-anchor-'));
  try {
    const first = harness({ stateDir: dir, sessionId: 'sa' });
    await initialize(first, true);
    await settled(first);
    await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    first.adapter.emit!(streamMsg(3));
    await until(() => first.incoming.length === 1, 'delivery');
    await first.close();

    // Session 2: the sweep's fetch (the first one) is slow; meanwhile the
    // agent opens the channel asking for the two newest messages.
    const second = harness({ stateDir: dir, sessionId: 'sa', history: [4, 5, 6, 7, 8].map((id) => streamMsg(id)) });
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    let calls = 0;
    const realFetch = second.adapter.fetchHistory!.bind(second.adapter);
    second.adapter.fetchHistory = async (channelId, query) => { if (calls++ === 0) await gate; return realFetch(channelId, query); };
    await initialize(second, true);
    await second.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
    await awaitRegistered(second, false);
    await until(() => calls === 1, 'the sweep to start fetching');
    const opened = (await second.host.sendRequest(method.CHANNELS_OPEN, {
      channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 2 },
    })) as { history?: IncomingChannelMessage[] };
    assert.deepEqual(opened.history!.map((m) => m.messageId), ['7', '8']);
    assert.equal(second.server.delivery.watermark('zulip:general'), 8, 'the backscroll moved the live watermark');
    release();
    await until(() => second.pushed.length === 1, 'the sweep still delivers from where the connection began');
    const block = (second.pushed[0].payload.content[0] as { text: string }).text;
    for (const id of [4, 5, 6]) assert.match(block, new RegExp(`id=${id}\\]`));
    await second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a second connection to the same server starts from a fresh grant, registration and sweep', async () => {
  const one = harness();
  await initialize(one, true);
  await settled(one);
  await one.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  one.adapter.emit!(streamMsg(1));
  await until(() => one.incoming.length === 1, 'delivery to the first peer');
  const toServer = one as unknown as { served: Promise<void> };
  await one.close();
  await toServer.served;

  const two = harness({ reuse: { server: one.server, adapter: one.adapter } });
  await initialize(two, true);
  // Nothing of the first peer's grant carries over: absence is denial again.
  await assert.rejects(two.host.sendRequest(method.CHANNELS_LIST), (err: Error & { code?: number }) => err.code === -32002);
  assert.equal(two.server.channelManager.isOpen('zulip:general'), false, 'nor what it had open');
  await settled(two);
  assert.ok(two.hostSaw.some((r) => r.method === method.CHANNELS_REGISTER), 'registered afresh');
  await two.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  two.adapter.emit!(streamMsg(2));
  await until(() => two.incoming.length === 1, 'delivery to the second peer');
  assert.equal(one.incoming.length, 1, 'and none of it went to the first');
  await two.close();
});

test('shutdown waits for a delivery still fetching its attachment, then flushes it', async () => {
  let release!: () => void;
  const gate = new Promise<void>((r) => { release = r; });
  const h = harness({
    attachments: {
      source: { async fetch() { await gate; return { buf: Buffer.from('log line'), mimeType: 'text/plain', overflow: false }; } },
      inline: { inlineImages: true, inlineTextMaxBytes: 5120, maxImages: 4 },
    },
  });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.adapter.emit!({
    ...streamMsg(1, { text: 'see the log' }),
    metadata: { topic: 'deploys', mentioned: false, isDM: false, attachments: [{ path: '/user_uploads/1/a/run.log', name: 'run.log', mimeType: 'text/plain', isImage: false }] },
  });
  await new Promise((r) => setTimeout(r, 10));
  assert.equal(h.server.channelManager.pendingCount(), 0, 'still building its content');
  const done = h.server.shutdown();
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(h.incoming.length, 0);
  release();
  await done;
  assert.equal(h.incoming.length, 1);
  assert.equal(h.server.delivery.watermark('zulip:general'), 1);
  await h.close();
});

test('catch-up pages past a page holding nothing but the bot\'s own messages', async () => {
  const h = harness({ history: [streamMsg(101), streamMsg(102), streamMsg(103, { mentioned: true })] });
  // The adapter withholds the bot's own messages after fetching; 101 and
  // 102 are its own, and the page size is two.
  const realFetch = h.adapter.fetchHistory!.bind(h.adapter);
  h.adapter.fetchHistory = async (channelId, query) => {
    const page = await realFetch(channelId, query);
    return { ...page, messages: page.messages.filter((m) => Number(m.messageId) > 102) };
  };
  h.adapter.pageCap = 2;
  await initialize(h, true);
  await settled(h);
  h.server.delivery.advance('zulip:general', 100);
  h.adapter.systemEvent!({ kind: 'gap', text: 'Queue expired.', metadata: { platform: 'zulip' } });
  await until(() => h.pushed.length === 1, 'the mention beyond the self-only page');
  assert.match((h.pushed[0].payload.content[0] as { text: string }).text, /id=103\]/);
  assert.deepEqual(h.adapter.historyCalls.map((c) => c.query.afterMessageId), ['100', '102'], 'the cursor advanced on what was scanned');
  assert.equal((h.pushed[0].origin as { truncated?: boolean }).truncated, undefined);
  assert.equal(h.server.delivery.watermark('zulip:general'), 103);
  await h.close();
});
