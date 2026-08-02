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
import type { CapabilityGrant } from './grant.js';

const DEFAULT_HISTORY_SIZE = 20;

/** SPEC §6.2 — the capability path for each injection position. */
const POSITION_CAPABILITY: Record<McplContextInjection['position'], string> = {
  system: 'contextHooks.beforeInference.inject.system',
  beforeUser: 'contextHooks.beforeInference.inject.beforeUser',
  afterUser: 'contextHooks.beforeInference.inject.afterUser',
};

export class ContextProvider {
  private historySize: number;

  constructor(
    private channelManager: ChannelManager,
    private grant: CapabilityGrant,
    historySize?: number,
  ) {
    this.historySize = historySize ?? DEFAULT_HISTORY_SIZE;
  }

  /**
   * Handle context/beforeInference — return context injections for open channels.
   *
   * `params` is deliberately unread. This server injects history and never
   * needs the user's text, so it does not declare
   * `contextHooks.beforeInference.observe` and a conforming host sends
   * `userMessage: null` regardless (§10.1). Injections are filtered by
   * position against the grant before returning: the host authorizes each one
   * independently at response-receipt (§5.4, §10.8), and a server that must
   * respect a reduction immediately (§6.7) should not be offering them.
   */
  async handleBeforeInference(_params: BeforeInferenceParams): Promise<BeforeInferenceResult> {
    const injections: McplContextInjection[] = [];
    const contributingTypes = new Set<string>();
    const openChannels = this.channelManager.getOpenChannels();

    for (const channelId of openChannels) {
      const adapter = this.channelManager.adapterFor(channelId);
      if (!adapter) continue;
      // §6.7: a server must immediately respect a reduction. The response
      // claims `{type}.context` (§6.5); if the host disabled it, or the grant
      // no longer covers what it declares, this server does not contribute
      // under it. That is derivation, not authorization — the host authorizes
      // each injection again at response-receipt (§5.4).
      if (!this.grant.isFeatureSetActive(`${adapter.type}.context`)) continue;
      try {
        const injection = await adapter.fetchContext(
          channelId,
          this.channelManager.getChannel(channelId),
          this.historySize,
        );
        if (injection) {
          if (!this.grant.has(POSITION_CAPABILITY[injection.position])) {
            console.error(
              `Dropping ${injection.position} injection for ${channelId}: ` +
                `${POSITION_CAPABILITY[injection.position]} not granted`,
            );
            continue;
          }
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
