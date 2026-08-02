/**
 * Feature Set Declarations — MCPL 0.5 (SPEC §5.1, §6.1, §6.2).
 *
 * Declares MCPL capabilities based on which platforms are enabled, and builds
 * the manifest (`experimental.mcpl`) object for the MCP Server constructor.
 *
 * Each platform contributes two feature sets:
 *   {type}.messaging — real-time delivery + channel management
 *   {type}.context   — history injection before inference
 *
 * `uses` is a closed vocabulary in 0.5 (§6.2) and derivation is fail-closed
 * (§6.4): an inaccurate declaration disables the feature set. Every path below
 * is justified by a call site, not by aspiration:
 *
 *   channels.register  — ChannelManager.registerChannels sends channels/register
 *   channels.lifecycle — index.ts handles channels/open and channels/close
 *   channels.publish   — index.ts handles channels/publish
 *   channels.incoming  — ChannelManager.flushBatch sends channels/incoming
 *   channels.typing    — index.ts handles channels/typing (adapters that support it)
 *   tools              — the MCP tool surface of this server
 *   contextHooks.beforeInference.inject.beforeUser
 *                      — ContextProvider returns injections, all at
 *                        position 'beforeUser' (platforms/*.ts fetchContext)
 *
 * Deliberately NOT declared:
 *   pushEvents  — nothing in this server ever sends `push/event`; platform
 *                 system events are delivered as channels/incoming messages
 *                 (ChannelManager.broadcastSystemEvent → enqueue → flushBatch).
 *   contextHooks.beforeInference.observe
 *               — ContextProvider.handleBeforeInference ignores its params
 *                 entirely, so it never reads `userMessage` (§10.1).
 *   inferenceLifecycle — this server has no use for turn boundaries; absence
 *                 of a capability is denial, and advertising one we do not
 *                 consume would invite a grant we cannot justify.
 */

import type {
  FeatureSetDeclaration,
  FeatureSetUse,
  McplServerCapabilities,
} from './types.js';

function displayName(type: string): string {
  return type.charAt(0).toUpperCase() + type.slice(1);
}

export interface FeatureSetOptions {
  /**
   * Platform types whose adapter implements `sendTyping`. Derived from the
   * adapter classes at the call site rather than restated here, so the
   * declaration cannot drift from the implementation.
   */
  typingCapable?: ReadonlySet<string>;
}

export function buildFeatureSets(
  platforms: string[],
  options: FeatureSetOptions = {},
): Record<string, FeatureSetDeclaration> {
  const featureSets: Record<string, FeatureSetDeclaration> = {};
  const typingCapable = options.typingCapable ?? new Set<string>();

  for (const type of platforms) {
    const name = displayName(type);

    const messagingUses: FeatureSetUse[] = [
      'channels.register',
      'channels.lifecycle',
      'channels.publish',
      'channels.incoming',
      'tools',
    ];
    if (typingCapable.has(type)) messagingUses.push('channels.typing');

    featureSets[`${type}.messaging`] = {
      description: `Real-time ${name} message delivery and channel management`,
      uses: messagingUses,
    };
    featureSets[`${type}.context`] = {
      description: `${name} message history injection before inference`,
      uses: ['contextHooks.beforeInference.inject.beforeUser'],
    };
  }

  return featureSets;
}

export function buildServerCapabilities(
  platforms: string[],
  options: FeatureSetOptions = {},
): McplServerCapabilities {
  const featureSets = buildFeatureSets(platforms, options);
  const hasAny = Object.keys(featureSets).length > 0;
  const typingCapable = options.typingCapable ?? new Set<string>();
  const anyTyping = platforms.some((p) => typingCapable.has(p));

  return {
    version: '0.5',
    contextHooks: {
      beforeInference: {
        // Injection without observation — the write-without-read shape of
        // §10.1. This server never reads `userMessage`.
        observe: false,
        inject: { system: false, beforeUser: hasAny, afterUser: false },
      },
    },
    featureSets,
    channels: {
      register: hasAny,
      lifecycle: hasAny,
      publish: hasAny,
      incoming: hasAny,
      typing: anyTyping,
    },
  };
}
