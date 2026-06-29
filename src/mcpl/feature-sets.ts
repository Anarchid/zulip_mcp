/**
 * Feature Set Declarations
 *
 * Declares MCPL capabilities based on which platforms are enabled.
 * Builds the McplServerCapabilities object for the MCP Server constructor.
 *
 * Each platform contributes two feature sets:
 *   {type}.messaging — real-time delivery + channel management
 *   {type}.context   — history injection before inference
 */

import type { FeatureSetDeclaration, McplServerCapabilities } from './types.js';

function displayName(type: string): string {
  return type.charAt(0).toUpperCase() + type.slice(1);
}

export function buildFeatureSets(platforms: string[]): Record<string, FeatureSetDeclaration> {
  const featureSets: Record<string, FeatureSetDeclaration> = {};

  for (const type of platforms) {
    const name = displayName(type);
    featureSets[`${type}.messaging`] = {
      description: `Real-time ${name} message delivery and channel management`,
      uses: ['channels.publish', 'channels.observe', 'pushEvents', 'tools'],
    };
    featureSets[`${type}.context`] = {
      description: `${name} message history injection before inference`,
      uses: ['contextHooks.beforeInference'],
    };
  }

  return featureSets;
}

export function buildServerCapabilities(platforms: string[]): McplServerCapabilities {
  const featureSets = buildFeatureSets(platforms);
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
