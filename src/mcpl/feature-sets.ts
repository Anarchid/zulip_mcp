/**
 * Feature Set Declarations
 *
 * Declares MCPL capabilities based on which services are enabled.
 * Builds the McplServerCapabilities object for the MCP Server constructor.
 */

import type { FeatureSetDeclaration, McplServerCapabilities } from './types.js';

export function buildFeatureSets(enableZulip: boolean, enableDiscord: boolean): Record<string, FeatureSetDeclaration> {
  const featureSets: Record<string, FeatureSetDeclaration> = {};

  if (enableZulip) {
    featureSets['zulip.messaging'] = {
      description: 'Real-time Zulip message delivery and channel management',
      uses: ['channels.publish', 'channels.observe', 'pushEvents', 'tools'],
    };
    featureSets['zulip.context'] = {
      description: 'Zulip message history injection before inference',
      uses: ['contextHooks.beforeInference'],
    };
  }

  if (enableDiscord) {
    featureSets['discord.messaging'] = {
      description: 'Real-time Discord message delivery and channel management',
      uses: ['channels.publish', 'channels.observe', 'pushEvents', 'tools'],
    };
    featureSets['discord.context'] = {
      description: 'Discord message history injection before inference',
      uses: ['contextHooks.beforeInference'],
    };
  }

  return featureSets;
}

export function buildServerCapabilities(enableZulip: boolean, enableDiscord: boolean): McplServerCapabilities {
  const featureSets = buildFeatureSets(enableZulip, enableDiscord);
  const hasAny = Object.keys(featureSets).length > 0;

  return {
    version: '0.4',
    pushEvents: hasAny,
    contextHooks: {
      beforeInference: hasAny,
      afterInference: false,
    },
    featureSets,
    channels: {
      register: hasAny,
      publish: hasAny,
      lifecycle: hasAny,
    },
  };
}
