/**
 * Feature Set Declarations — MCPL 0.5 (SPEC §5.1, §6.1, §6.2).
 *
 * Declares this server's feature sets and builds the manifest
 * (`experimental.mcpl`) presented at `initialize` and returned by
 * `mcpl/manifest`.
 *
 *   zulip.messaging — real-time delivery + channel management
 *   zulip.history   — reading back through streams (tools)
 *   zulip.context   — history injection before inference
 *
 * `uses` is a closed vocabulary in 0.5 (§6.2) and derivation is fail-closed
 * (§6.4): an inaccurate declaration disables the feature set. Every path below
 * is justified by a call site, not by aspiration:
 *
 *   channels.register  — ChannelManager.registerChannels sends channels/register
 *   channels.lifecycle — server.ts handles channels/open and channels/close
 *   channels.publish   — server.ts handles channels/publish
 *   channels.incoming  — ChannelManager.flushBatch sends channels/incoming
 *   channels.typing    — server.ts handles channels/typing (adapter implements it)
 *   pushEvents         — server.ts sends push/event for addressed messages on
 *                        channels the host has not opened, and for the
 *                        reconnect catch-up sweep
 *   tools              — the MCP tool surface of this server
 *   contextHooks.beforeInference.inject.beforeUser
 *                      — ContextProvider returns injections, all at
 *                        position 'beforeUser' (platforms/zulip.ts fetchContext)
 *
 * Deliberately NOT declared:
 *   contextHooks.beforeInference.observe
 *               — ContextProvider.handleBeforeInference ignores its params
 *                 entirely, so it never reads `userMessage` (§10.1).
 *   inferenceLifecycle — this server has no use for turn boundaries; absence
 *                 of a capability is denial, and advertising one we do not
 *                 consume would invite a grant we cannot justify.
 */

import type {
  CapabilityPath,
  FeatureSetDeclaration,
  McplManifest,
  TagOntology,
} from '@animalabs/mcpl-core';

export const MESSAGING_FEATURE_SET = 'zulip.messaging';
export const HISTORY_FEATURE_SET = 'zulip.history';
export const CONTEXT_FEATURE_SET = 'zulip.context';

export interface FeatureSetOptions {
  /**
   * Whether the adapter implements `sendTyping`. Derived from the adapter
   * class at the call site rather than restated here, so the declaration
   * cannot drift from the implementation.
   */
  typing: boolean;
}

/**
 * MCPL RFC-001 — tags carried on Zulip message events. The `chat:*` core is
 * the reserved cross-platform vocabulary; the adapter emits the most specific
 * tag and hosts expand umbrellas (`chat:mention` ⇒ `chat:addressed`).
 */
export const ZULIP_TAG_ONTOLOGY: TagOntology = {
  coreTags: [
    'chat:addressed', 'chat:mention', 'chat:dm', 'chat:private', 'chat:ambient',
    'chat:from-human', 'chat:from-bot',
    'chat:reaction', 'chat:reaction-remove',
    'chat:has-image', 'chat:has-file',
  ],
  tags: {
    'zulip:wildcard-mention': {
      desc: 'An @all / @everyone / @stream wildcard reached the bot; never counts as addressed.',
      facet: 'addressing',
    },
    'zulip:missed': {
      desc: 'Catch-up delivery of messages that arrived while the server was offline.',
      facet: 'lifecycle',
    },
  },
  // Zulip-specific extensions may be emitted; consumers should tolerate
  // undeclared tags.
  open: true,
};

export function buildFeatureSets(options: FeatureSetOptions): Record<string, FeatureSetDeclaration> {
  const messagingUses: CapabilityPath[] = [
    'channels.register',
    'channels.lifecycle',
    'channels.publish',
    'channels.incoming',
    'pushEvents',
    'tools',
  ];
  if (options.typing) messagingUses.push('channels.typing');

  return {
    [MESSAGING_FEATURE_SET]: {
      description: 'Real-time Zulip message delivery and channel management',
      uses: messagingUses,
      tagOntology: ZULIP_TAG_ONTOLOGY,
    },
    [HISTORY_FEATURE_SET]: {
      description: 'Read back through Zulip stream history',
      uses: ['tools'],
    },
    [CONTEXT_FEATURE_SET]: {
      description: 'Zulip message history injection before inference',
      uses: ['contextHooks.beforeInference.inject.beforeUser'],
    },
  };
}

export function buildServerCapabilities(options: FeatureSetOptions): McplManifest {
  return {
    version: '0.5',
    pushEvents: true,
    contextHooks: {
      beforeInference: {
        // Injection without observation — the write-without-read shape of
        // §10.1. This server never reads `userMessage`.
        observe: false,
        inject: { system: false, beforeUser: true, afterUser: false },
      },
    },
    featureSets: buildFeatureSets(options),
    channels: {
      register: true,
      lifecycle: true,
      publish: true,
      incoming: true,
      typing: options.typing,
    },
  };
}

/**
 * The feature set that owns a tool, for §6.7 selection: a host that disables
 * `zulip.messaging` disables its tools with it. Read-only lookups return
 * undefined and stay available under the plain `tools` capability.
 */
export function featureSetForTool(toolName: string): string | undefined {
  switch (toolName) {
    case 'send_message':
    case 'send_dm':
    case 'edit_message':
    case 'delete_message':
    case 'add_reaction':
    case 'remove_reaction':
    case 'set_reaction_visibility':
    case 'listen':
    case 'unlisten':
    case 'start_monitoring':
    case 'stop_monitoring':
    case 'channel_missed':
    case 'mute_channel':
    case 'unmute_channel':
      return MESSAGING_FEATURE_SET;
    case 'fetch_history':
    case 'fetch_around':
    case 'get_channel_history':
    case 'get_unread_messages':
      return HISTORY_FEATURE_SET;
    default:
      return undefined;
  }
}
