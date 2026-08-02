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
  ChannelsRegisterResult,
  ChannelsChangedParams,
  ChannelsIncomingParams,
  ManifestChangedParams,
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
   * Register channels with the host (§14.3).
   *
   * The host authorizes each descriptor independently (§14.5) and the Request
   * form answers with one entry per submitted descriptor, so the result is
   * itemized rather than whole-request.
   */
  registerChannels(channels: ChannelDescriptor[]): Promise<ChannelsRegisterResult> {
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
   * Tell the host that the set of available channels changed (§14.3).
   *
   * Sent as a **Request**, not a Notification. §14.5 makes `channels/changed`
   * dual-mode: a Notification cannot carry a result, so a host whose policy
   * rejects some descriptors has no way to say which. The Request form gets an
   * itemized answer, and a rejected descriptor stays unregistered here.
   */
  sendChannelsChanged(params: ChannelsChangedParams): Promise<ChannelsRegisterResult> {
    return this.request(McplMethod.ChannelsChanged, params as unknown as Record<string, unknown>);
  }

  /**
   * Announce that this server's manifest changed (§17.3).
   *
   * A Notification carrying only an opaque revision and the changed domains.
   * No diff, no payload, no conclusion — the host re-fetches `mcpl/manifest`
   * and derives everything itself. No capability path gates this (§17.3).
   */
  sendManifestChanged(params: ManifestChangedParams): void {
    this.notify(McplMethod.ManifestChanged, params as unknown as Record<string, unknown>);
  }

  // -- Internal --

  private write(msg: JsonRpcRequest): void {
    process.stdout.write(JSON.stringify(msg) + '\n');
  }
}
