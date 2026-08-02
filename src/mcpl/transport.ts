/**
 * McplTransport — Custom Transport that intercepts stdin, routes MCPL vs MCP.
 *
 * Implements the MCP SDK Transport interface. Replaces StdioServerTransport.
 *
 * Routing logic for each line from stdin:
 * - Has "method" AND method is MCPL → dispatch to McplDispatcher, write response to stdout
 * - Has "method" AND method is "initialize" → extract host MCPL caps, then forward to onmessage
 * - Has "method" AND method is MCP → forward to onmessage
 * - Has "id" but no "method" (response) → route to McplClient.handleResponse()
 */

import type { Transport } from '@modelcontextprotocol/sdk/shared/transport.js';
import type { JSONRPCMessage } from '@modelcontextprotocol/sdk/types.js';
import { createInterface, type Interface as ReadlineInterface } from 'readline';
import type { McplDispatcher } from './dispatcher.js';
import type { McplClient } from './client.js';
import type { JsonRpcResponse } from './types.js';
import { MCPL_METHODS } from './types.js';

/**
 * The host's advertised MCPL support (§5.2), mirroring the server shape.
 *
 * This is advertisement, not authorization: what the server may actually do is
 * the effective grant delivered by `featureSets/update` (§5.3, §5.4). Nothing
 * in this object widens anything.
 */
export interface McplHostCapabilities {
  version: string;
  pushEvents?: boolean;
  contextHooks?: {
    beforeInference?: boolean | Record<string, unknown>;
  };
  inferenceLifecycle?: boolean;
  featureSets?: boolean;
  channels?: {
    register?: boolean;
    lifecycle?: boolean;
    publish?: boolean;
    incoming?: boolean;
    streaming?: boolean;
    acknowledge?: boolean;
    typing?: boolean;
  };
}

export class McplTransport implements Transport {
  onmessage?: (msg: JSONRPCMessage) => void;
  onclose?: () => void;
  onerror?: (err: Error) => void;
  sessionId?: string;

  private rl: ReadlineInterface | null = null;
  private hostCapabilities: McplHostCapabilities | null = null;

  constructor(
    private dispatcher: McplDispatcher,
    private client: McplClient,
  ) {}

  /**
   * The host's MCPL capabilities, extracted from the initialize request.
   */
  getHostCapabilities(): McplHostCapabilities | null {
    return this.hostCapabilities;
  }

  async start(): Promise<void> {
    this.rl = createInterface({ input: process.stdin, terminal: false });

    this.rl.on('line', (line: string) => {
      if (!line.trim()) return;

      let parsed: Record<string, unknown>;
      try {
        parsed = JSON.parse(line);
      } catch {
        this.onerror?.(new Error(`Invalid JSON on stdin: ${line.substring(0, 100)}`));
        return;
      }

      this.routeMessage(parsed);
    });

    this.rl.on('close', () => {
      this.onclose?.();
    });

    process.stdin.on('error', (err: Error) => {
      this.onerror?.(err);
    });
  }

  async send(message: JSONRPCMessage): Promise<void> {
    return new Promise<void>((resolve) => {
      const json = JSON.stringify(message) + '\n';
      if (process.stdout.write(json)) {
        resolve();
      } else {
        process.stdout.once('drain', resolve);
      }
    });
  }

  async close(): Promise<void> {
    if (this.rl) {
      this.rl.close();
      this.rl = null;
    }
    this.onclose?.();
  }

  private routeMessage(parsed: Record<string, unknown>): void {
    const method = parsed['method'] as string | undefined;
    const id = parsed['id'] as string | number | undefined;

    if (method) {
      // It's a request or notification

      if (method === 'initialize') {
        // Extract MCPL capabilities from the host's initialize request
        this.extractHostCaps(parsed);
        // Forward to MCP Server for normal handling
        this.onmessage?.(parsed as unknown as JSONRPCMessage);
        return;
      }

      if (MCPL_METHODS.has(method) || this.dispatcher.handles(method)) {
        // MCPL method — dispatch and write response to stdout
        this.dispatchMcpl(parsed);
        return;
      }

      // Standard MCP method — forward to Server
      this.onmessage?.(parsed as unknown as JSONRPCMessage);
      return;
    }

    if (id !== undefined) {
      // It's a response (has id but no method) — route to McplClient
      this.client.handleResponse(parsed as unknown as JsonRpcResponse);
      return;
    }

    // Unknown message shape, forward to MCP Server
    this.onmessage?.(parsed as unknown as JSONRPCMessage);
  }

  private extractHostCaps(parsed: Record<string, unknown>): void {
    try {
      const params = parsed['params'] as Record<string, unknown> | undefined;
      const capabilities = params?.['capabilities'] as Record<string, unknown> | undefined;
      const experimental = capabilities?.['experimental'] as Record<string, unknown> | undefined;
      const mcpl = experimental?.['mcpl'] as McplHostCapabilities | undefined;
      if (mcpl) {
        this.hostCapabilities = mcpl;
        console.error(`Host MCPL capabilities: ${JSON.stringify(mcpl)}`);
      }
    } catch {
      // Non-fatal: host may not support MCPL
    }
  }

  private async dispatchMcpl(parsed: Record<string, unknown>): Promise<void> {
    try {
      const response = await this.dispatcher.dispatch(parsed as any);
      if (response) {
        process.stdout.write(JSON.stringify(response) + '\n');
      }
    } catch (error) {
      const id = parsed['id'] as string | number | undefined;
      if (id !== undefined) {
        const errResponse: JsonRpcResponse = {
          jsonrpc: '2.0',
          id,
          error: {
            code: -32000,
            message: error instanceof Error ? error.message : String(error),
          },
        };
        process.stdout.write(JSON.stringify(errResponse) + '\n');
      }
    }
  }
}
