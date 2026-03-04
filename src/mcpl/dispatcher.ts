/**
 * MCPL Dispatcher — Routes MCPL JSON-RPC methods to handlers.
 *
 * Same pattern as discord-mcpl Java implementation:
 * register(method, handler), handles(method), dispatch(request).
 */

import type { JsonRpcRequest, JsonRpcResponse } from './types.js';

export type McplHandler = (params: Record<string, unknown>) => Promise<unknown> | unknown;

export class McplDispatcher {
  private handlers = new Map<string, McplHandler>();

  /**
   * Register a handler for an MCPL method.
   */
  register(method: string, handler: McplHandler): void {
    this.handlers.set(method, handler);
  }

  /**
   * Check if a method is handled by this dispatcher.
   */
  handles(method: string): boolean {
    return this.handlers.has(method);
  }

  /**
   * Dispatch a JSON-RPC request to the appropriate handler.
   * Returns a JsonRpcResponse for requests (with id), null for notifications.
   */
  async dispatch(request: JsonRpcRequest): Promise<JsonRpcResponse | null> {
    const handler = this.handlers.get(request.method);
    if (!handler) {
      // Method not found
      if (request.id !== undefined) {
        return {
          jsonrpc: '2.0',
          id: request.id,
          error: { code: -32601, message: `Unknown MCPL method: ${request.method}` },
        };
      }
      return null;
    }

    try {
      const result = await handler(request.params ?? {});

      // If it's a notification (no id), don't send a response
      if (request.id === undefined) return null;

      return {
        jsonrpc: '2.0',
        id: request.id,
        result: result ?? {},
      };
    } catch (error) {
      if (request.id === undefined) return null;

      return {
        jsonrpc: '2.0',
        id: request.id,
        error: {
          code: -32000,
          message: error instanceof Error ? error.message : String(error),
        },
      };
    }
  }
}
