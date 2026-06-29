/**
 * Context Provider — Handles context/beforeInference.
 *
 * Injects recent message history from open channels into the inference
 * context. Platform-agnostic: history fetching/formatting is delegated to
 * the owning PlatformAdapter per channel.
 */

import type {
  BeforeInferenceParams,
  BeforeInferenceResult,
  McplContextInjection,
} from './types.js';
import type { ChannelManager } from './channels.js';

const DEFAULT_HISTORY_SIZE = 20;

export class ContextProvider {
  private historySize: number;

  constructor(
    private channelManager: ChannelManager,
    historySize?: number,
  ) {
    this.historySize = historySize ?? DEFAULT_HISTORY_SIZE;
  }

  /**
   * Handle context/beforeInference — return context injections for open channels.
   */
  async handleBeforeInference(_params: BeforeInferenceParams): Promise<BeforeInferenceResult> {
    const injections: McplContextInjection[] = [];
    const contributingTypes = new Set<string>();
    const openChannels = this.channelManager.getOpenChannels();

    for (const channelId of openChannels) {
      const adapter = this.channelManager.adapterFor(channelId);
      if (!adapter) continue;
      try {
        const injection = await adapter.fetchContext(
          channelId,
          this.channelManager.getChannel(channelId),
          this.historySize,
        );
        if (injection) {
          injections.push(injection);
          contributingTypes.add(adapter.type);
        }
      } catch (error) {
        console.error(`Failed to get context for channel ${channelId}:`, error);
      }
    }

    // Tag with the contributing platform's context feature set. When multiple
    // (or no) platforms contributed, fall back to the first registered one.
    const type = contributingTypes.size === 1
      ? contributingTypes.values().next().value
      : this.channelManager.firstAdapterType();

    return {
      featureSet: `${type}.context`,
      contextInjections: injections,
    };
  }
}
