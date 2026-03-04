/**
 * MCPL Client — Outbound messaging (server → host).
 *
 * Writes JSON-RPC 2.0 messages to stdout for the host to receive.
 * Tracks pending requests by ID for request/response correlation.
 */

import type {
  JsonRpcRequest,
  JsonRpcResponse,
  ChannelDescriptor,
  ChannelIncomingMessage,
  ChannelsRegisterParams,
  ChannelsIncomingParams,
} from './types.js';
import { McplMethod } from './types.js';

export class McplClient {
  private nextId = 1;
  private pending = new Map<string | number, {
    resolve: (result: unknown) => void;
    reject: (error: Error) => void;
  }>();

  /**
   * Send a JSON-RPC notification (fire-and-forget, no id).
   */
  notify(method: string, params?: Record<string, unknown>): void {
    const msg: JsonRpcRequest = { jsonrpc: '2.0', method };
    if (params) msg.params = params;
    this.write(msg);
  }

  /**
   * Send a JSON-RPC request and return a Promise for the result.
   */
  request<T = unknown>(method: string, params?: Record<string, unknown>): Promise<T> {
    const id = this.nextId++;
    const msg: JsonRpcRequest = { jsonrpc: '2.0', method, id };
    if (params) msg.params = params;

    return new Promise<T>((resolve, reject) => {
      this.pending.set(id, {
        resolve: resolve as (result: unknown) => void,
        reject,
      });
      this.write(msg);
    });
  }

  /**
   * Handle a JSON-RPC response from the host. Resolves/rejects the pending promise.
   */
  handleResponse(response: JsonRpcResponse): void {
    const entry = this.pending.get(response.id);
    if (!entry) return; // orphan response, ignore
    this.pending.delete(response.id);

    if (response.error) {
      entry.reject(new Error(`MCPL error ${response.error.code}: ${response.error.message}`));
    } else {
      entry.resolve(response.result);
    }
  }

  // -- Convenience methods --

  /**
   * Register channels with the host.
   */
  registerChannels(channels: ChannelDescriptor[]): Promise<{ registered: string[] }> {
    const params: ChannelsRegisterParams = { channels };
    return this.request(McplMethod.ChannelsRegister, params as unknown as Record<string, unknown>);
  }

  /**
   * Send incoming messages to the host (batched).
   */
  sendIncoming(messages: ChannelIncomingMessage[]): Promise<unknown> {
    const params: ChannelsIncomingParams = { messages };
    return this.request(McplMethod.ChannelsIncoming, params as unknown as Record<string, unknown>);
  }

  /**
   * Notify host that available channels have changed.
   */
  sendChannelsChanged(added?: ChannelDescriptor[], removed?: string[]): void {
    this.notify(McplMethod.ChannelsChanged, {
      ...(added && { added }),
      ...(removed && { removed }),
    });
  }

  // -- Internal --

  private write(msg: JsonRpcRequest): void {
    process.stdout.write(JSON.stringify(msg) + '\n');
  }
}
