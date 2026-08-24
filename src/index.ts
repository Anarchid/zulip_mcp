#!/usr/bin/env node
/**
 * Zulip MCP/MCPL server — CLI entry point.
 *
 * Usage:
 *   zulip-mcp-server                 # stdio transport (default; MCP-compatible)
 *   zulip-mcp-server --stdio
 *   zulip-mcp-server --tcp <port>    # TCP transport for MCPL hosts
 *
 * Environment:
 *   ZULIP_REALM / ZULIP_EMAIL / ZULIP_API_KEY   - bot credentials
 *   ZULIP_RC_PATH                               - alternative: a zuliprc file
 *   ZULIP_SESSION_ID                            - persistent monitoring state id
 *   ZULIP_SUBSCRIBE                             - comma-separated streams to join on startup
 *   MCPL_ENABLED                                - "false" forces plain-MCP mode
 *   MCPL_BATCH_WINDOW_MS                        - channels/incoming batching window (500)
 *   MCPL_CONTEXT_HISTORY_SIZE                   - messages injected per open channel (20)
 */

import * as net from 'node:net';
import { McplConnection } from '@animalabs/mcpl-core';
import { isMainModule } from './content.js';
import { ZulipAdapter } from './platforms/zulip.js';
import { ZulipMcplServer } from './server.js';
import { ZulipToolRuntime } from './tool-runtime.js';
import { initializeZulipClient } from './zulip-client.js';

export { fetchAttachmentBytes, extractZulipAttachments, cleanContent } from './content.js';
export { formatMessages } from './tool-runtime.js';

const SERVER_INFO = { name: 'zulip-mcp-server', version: '3.0.0' };

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const tcpIdx = args.indexOf('--tcp');
  const tcpPort = tcpIdx >= 0 ? parseInt(args[tcpIdx + 1], 10) : undefined;
  if (tcpIdx >= 0 && !(tcpPort! > 0)) {
    console.error('Usage: zulip-mcp-server [--stdio | --tcp <port>]');
    process.exit(1);
  }

  const session = await initializeZulipClient();
  const adapter = new ZulipAdapter(session.client, session.selfUserId, session.sessionId);
  const tools = new ZulipToolRuntime(session);
  const server = new ZulipMcplServer(adapter, tools, {
    serverInfo: SERVER_INFO,
    mcplEnabled: process.env.MCPL_ENABLED !== 'false',
    batchWindowMs: parseInt(process.env.MCPL_BATCH_WINDOW_MS || '500', 10),
    contextHistorySize: parseInt(process.env.MCPL_CONTEXT_HISTORY_SIZE || '20', 10),
  });

  if (tcpPort) {
    console.error(`[zulip-mcp] Listening on TCP port ${tcpPort}`);
    const tcpServer = net.createServer();
    tcpServer.listen(tcpPort, '127.0.0.1');
    await new Promise<void>((resolve) => tcpServer.once('listening', resolve));

    // One connection at a time.
    while (true) {
      const conn = await McplConnection.acceptTcp(tcpServer);
      console.error('[zulip-mcp] Client connected');
      await server.serve(conn);
      console.error('[zulip-mcp] Client disconnected, waiting for next...');
    }
  }

  // Stdio: stdout is the protocol channel, so everything else logs to stderr.
  const conn = McplConnection.fromStreams(process.stdin, process.stdout);
  await server.serve(conn);
  server.shutdown();
  process.exit(0);
}

// Only auto-start when run as a CLI. Importing this module (e.g. from tests)
// should not boot the server or require Zulip credentials. isMainModule
// realpaths argv[1] so the guard also passes when launched through an npm
// bin symlink (npx zulip-mcp-server).
if (isMainModule(import.meta.url, process.argv[1])) {
  main().catch((error) => {
    console.error('Failed to start server:', error);
    process.exit(1);
  });
}
