/**
 * MCPL Dispatcher — Routes MCPL JSON-RPC methods to handlers.
 *
 * Same pattern as discord-mcpl Java implementation:
 * register(method, handler), handles(method), dispatch(request).
 */

import type { JsonRpcRequest, JsonRpcResponse } from './types.js';
import { McplRpcError } from './errors.js';

/**
 * How the message arrived. SPEC 0.5 §6.7 distinguishes the two forms for
 * `featureSets/update`: a Notification "cannot establish a ready state", so a
 * handler that changes what this server believes it may do has to know which
 * form it was told in. Every other handler ignores it.
 */
export interface McplDispatchContext {
  /** True when the message carried an `id` and a response is expected. */
  isRequest: boolean;
}

export type McplHandler = (
  params: Record<string, unknown>,
  context: McplDispatchContext,
) => Promise<unknown> | unknown;

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
      const result = await handler(request.params ?? {}, { isRequest: request.id !== undefined });

      // If it's a notification (no id), don't send a response
      if (request.id === undefined) return null;

      return {
        jsonrpc: '2.0',
        id: request.id,
        result: result ?? {},
      };
    } catch (error) {
      if (request.id === undefined) return null;

      // §6.6: rejection is diagnostics. Carry the documented code when the
      // handler supplied one instead of flattening everything to -32000.
      if (error instanceof McplRpcError) {
        return {
          jsonrpc: '2.0',
          id: request.id,
          error: { code: error.code, message: error.message, data: error.data },
        };
      }

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
