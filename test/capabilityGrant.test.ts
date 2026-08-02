/**
 * Capability grant and feature-set declaration conformance (SPEC 0.5 §5.3,
 * §5.4, §6.2, §6.4, §6.7).
 *
 * The theme of every case here is that absence is denial. Nothing the server
 * says — not its advertisement, not its degradation receipt — may widen what
 * the host granted.
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { CapabilityGrant } from '../src/mcpl/grant.ts';
import { McplRpcError } from '../src/mcpl/errors.ts';
import { buildFeatureSets, buildServerCapabilities } from '../src/mcpl/feature-sets.ts';
import { CAPABILITY_PATH_SET } from '../src/mcpl/types.ts';

const PLATFORMS = ['zulip', 'discord', 'slack'];

// --- §6.2: `uses` is a closed vocabulary ------------------------------------

test('every declared `uses` value is in the SPEC §6.2 capability-path vocabulary', () => {
  const sets = buildFeatureSets(PLATFORMS, { typingCapable: new Set(['zulip']) });
  assert.ok(Object.keys(sets).length > 0);
  for (const [name, decl] of Object.entries(sets)) {
    assert.ok(Array.isArray(decl.uses) && decl.uses.length > 0, `${name}: uses must be non-empty (§6.4)`);
    for (const use of decl.uses) {
      assert.ok(CAPABILITY_PATH_SET.has(use), `${name}: '${use}' is not a §6.2 capability path`);
    }
  }
});

test('struck and removed vocabulary is not declared anywhere', () => {
  const serialized = JSON.stringify(buildServerCapabilities(PLATFORMS, { typingCapable: new Set(['zulip']) }));
  // `channels.observe` was struck in 0.5.0 (inbound content is
  // `channels.incoming`); `afterInference` was removed with §10.5.
  assert.ok(!serialized.includes('channels.observe'), 'channels.observe was struck in 0.5.0');
  assert.ok(!serialized.includes('afterInference'), 'context/afterInference was removed in 0.5.0');
  // The un-split `contextHooks.beforeInference` is no longer a capability path.
  assert.ok(!serialized.includes('"contextHooks.beforeInference"'));
});

test('channels.typing is advertised only for a platform whose adapter implements it', () => {
  const withTyping = buildServerCapabilities(PLATFORMS, { typingCapable: new Set(['zulip']) });
  assert.equal(withTyping.channels?.typing, true);
  assert.ok(withTyping.featureSets!['zulip.messaging'].uses.includes('channels.typing'));
  assert.ok(!withTyping.featureSets!['slack.messaging'].uses.includes('channels.typing'));

  const withoutTyping = buildServerCapabilities(['slack'], { typingCapable: new Set() });
  assert.equal(withoutTyping.channels?.typing, false);
});

test('the manifest advertises injection without observation (§10.1 write-without-read)', () => {
  const caps = buildServerCapabilities(['zulip'], { typingCapable: new Set(['zulip']) });
  assert.equal(caps.contextHooks?.beforeInference?.observe, false);
  assert.deepEqual(caps.contextHooks?.beforeInference?.inject, {
    system: false,
    beforeUser: true,
    afterUser: false,
  });
  assert.equal(caps.version, '0.5');
});

// --- §5.3 / §5.4: absence is denial -----------------------------------------

test('nothing is granted before the initial policy exchange (§5.3)', () => {
  const grant = new CapabilityGrant({
    'zulip.messaging': { description: 'x', uses: ['channels.publish'] },
  });
  assert.equal(grant.isReady(), false);
  assert.equal(grant.has('channels.publish'), false);
  assert.equal(grant.has('tools'), false);
  assert.equal(grant.isFeatureSetActive('zulip.messaging'), false);
});

test('a path the host did not name is denied, and an interior node grants no leaf', () => {
  const grant = new CapabilityGrant();
  grant.apply({ effectiveCapabilities: ['channels', 'channels.publish'] });
  assert.equal(grant.has('channels.publish'), true);
  assert.equal(grant.has('channels.incoming'), false);
  assert.equal(grant.has('channels.register'), false);
});

test('a trailing wildcard covers the paths beneath it (§5.4 recursive walk)', () => {
  const grant = new CapabilityGrant();
  grant.apply({ effectiveCapabilities: ['channels.*', 'contextHooks.beforeInference.inject.*'] });
  assert.equal(grant.has('channels.publish'), true);
  assert.equal(grant.has('channels.acknowledge'), true);
  assert.equal(grant.has('contextHooks.beforeInference.inject.system'), true);
  assert.equal(grant.has('contextHooks.beforeInference.observe'), false);
  assert.equal(grant.has('pushEvents'), false);
});

test('an update with no effectiveCapabilities empties the grant rather than leaving it standing', () => {
  const grant = new CapabilityGrant();
  grant.apply({ effectiveCapabilities: ['channels.publish'] });
  assert.equal(grant.has('channels.publish'), true);

  const receipt = grant.apply({ enabled: ['zulip.messaging'] });
  assert.equal(grant.has('channels.publish'), false);
  assert.ok(receipt.notes.some((n) => n.includes('empty')));
});

test('a path in both effectiveCapabilities and deniedCapabilities is rejected as malformed (§5.4)', () => {
  const grant = new CapabilityGrant();
  grant.apply({ effectiveCapabilities: ['channels.publish'] });

  let thrown: unknown;
  try {
    grant.apply({
      effectiveCapabilities: ['channels.publish', 'channels.incoming'],
      deniedCapabilities: ['channels.incoming'],
    });
  } catch (error) {
    thrown = error;
  }
  assert.ok(thrown instanceof McplRpcError, 'expected a typed JSON-RPC error');
  assert.equal((thrown as McplRpcError).code, -32602);
  // The previous grant stands: a malformed message widens nothing and
  // silently narrows nothing.
  assert.equal(grant.has('channels.publish'), true);
  assert.equal(grant.has('channels.incoming'), false);
});

test('deniedCapabilities never participates in an authorization decision (§5.4)', () => {
  const grant = new CapabilityGrant();
  // A host that lists a path only under `denied` changes nothing: the path was
  // already denied by not appearing in the allowlist.
  grant.apply({
    effectiveCapabilities: ['channels.publish'],
    deniedCapabilities: ['contextHooks.beforeInference.inject.system'],
  });
  assert.equal(grant.has('contextHooks.beforeInference.inject.system'), false);
  assert.equal(grant.has('channels.publish'), true);
});

// --- §6.4 / §6.7: derivation and the degradation receipt ---------------------

const DECLARATIONS = {
  'zulip.messaging': {
    description: 'x',
    uses: ['channels.register', 'channels.publish', 'channels.incoming'] as const,
  },
  'zulip.context': {
    description: 'y',
    uses: ['contextHooks.beforeInference.inject.beforeUser'] as const,
  },
};

function declarations() {
  return JSON.parse(JSON.stringify(DECLARATIONS));
}

test('a denied capability disables every feature set whose uses requires it (§6.4)', () => {
  const grant = new CapabilityGrant(declarations());
  const receipt = grant.apply({
    effectiveCapabilities: ['channels.register', 'channels.publish'],
  });

  assert.equal(receipt.accepted, true);
  assert.equal(receipt.mode, 'degraded');
  assert.deepEqual(
    receipt.unavailableFeatures.map((f) => f.featureSet).sort(),
    ['zulip.context', 'zulip.messaging'],
  );
  const messaging = receipt.unavailableFeatures.find((f) => f.featureSet === 'zulip.messaging')!;
  assert.deepEqual(messaging.missingCapabilities, ['channels.incoming']);
  assert.equal(messaging.effect, 'disabled');
  assert.equal(grant.isFeatureSetActive('zulip.messaging'), false);
});

test('a fully covered grant reports mode "full" and lists nothing unavailable', () => {
  const grant = new CapabilityGrant(declarations());
  const receipt = grant.apply({
    effectiveCapabilities: [
      'channels.register',
      'channels.publish',
      'channels.incoming',
      'contextHooks.beforeInference.inject.beforeUser',
    ],
  });
  assert.equal(receipt.mode, 'full');
  assert.deepEqual(receipt.unavailableFeatures, []);
  assert.equal(grant.isFeatureSetActive('zulip.messaging'), true);
  assert.equal(grant.isFeatureSetActive('zulip.context'), true);
});

test('the receipt asserts no entitlement — it names only what is missing and what breaks', () => {
  const grant = new CapabilityGrant(declarations());
  const receipt = grant.apply({ effectiveCapabilities: [] });
  const serialized = JSON.stringify(receipt);
  for (const forbidden of ['require', 'request', 'grant', 'entitle', 'must ', 'please']) {
    assert.ok(
      !serialized.toLowerCase().includes(forbidden),
      `receipt must not ask for anything; found '${forbidden}' in ${serialized}`,
    );
  }
  assert.equal(receipt.accepted, true);
  for (const feature of receipt.unavailableFeatures) assert.equal(feature.effect, 'disabled');
});

test('an explicitly disabled feature set is inactive even when fully covered (§6.7)', () => {
  const grant = new CapabilityGrant(declarations());
  grant.apply({
    effectiveCapabilities: [
      'channels.register',
      'channels.publish',
      'channels.incoming',
      'contextHooks.beforeInference.inject.beforeUser',
    ],
    disabled: ['zulip.context'],
  });
  assert.equal(grant.isFeatureSetActive('zulip.messaging'), true);
  assert.equal(grant.isFeatureSetActive('zulip.context'), false);
});

test('an `enabled` selection excludes what it does not name', () => {
  const grant = new CapabilityGrant(declarations());
  grant.apply({
    effectiveCapabilities: [
      'channels.register',
      'channels.publish',
      'channels.incoming',
      'contextHooks.beforeInference.inject.beforeUser',
    ],
    enabled: ['zulip.messaging'],
  });
  assert.equal(grant.isFeatureSetActive('zulip.messaging'), true);
  assert.equal(grant.isFeatureSetActive('zulip.context'), false);
});

// --- §6.7: a Notification cannot establish a ready state --------------------

test('a featureSets/update Notification establishes nothing before the initial exchange (§6.7)', () => {
  const grant = new CapabilityGrant(declarations());
  const receipt = grant.apply({ effectiveCapabilities: ['channels.publish'] }, 'notification');
  assert.equal(grant.isReady(), false);
  assert.equal(grant.has('channels.publish'), false);
  assert.ok(receipt.notes.some((n) => n.includes('ready state')));
});

test('a Notification may narrow the grant but never widen it (§6.7)', () => {
  const grant = new CapabilityGrant(declarations());
  grant.apply({ effectiveCapabilities: ['channels.register', 'channels.publish'] });

  grant.apply(
    { effectiveCapabilities: ['channels.register', 'channels.publish', 'channels.incoming'] },
    'notification',
  );
  assert.equal(grant.has('channels.incoming'), false, 'a Notification must not widen');
  assert.equal(grant.has('channels.publish'), true);

  grant.apply({ effectiveCapabilities: ['channels.register'] }, 'notification');
  assert.equal(grant.has('channels.publish'), false, 'a reduction is respected immediately');
});

test('the Request form is what expands a grant (§6.7 tell → receipt → activate)', () => {
  const grant = new CapabilityGrant(declarations());
  grant.apply({ effectiveCapabilities: ['channels.register'] });
  assert.equal(grant.has('channels.publish'), false);
  grant.apply({ effectiveCapabilities: ['channels.register', 'channels.publish'] }, 'request');
  assert.equal(grant.has('channels.publish'), true);
});

test('setDeclarations re-derives degradation without touching the grant (§17.5)', () => {
  const grant = new CapabilityGrant(declarations());
  grant.apply({ effectiveCapabilities: ['channels.register', 'channels.publish'] });
  assert.equal(grant.has('channels.publish'), true);

  grant.setDeclarations({
    'zulip.messaging': { description: 'x', uses: ['channels.register', 'channels.publish'] },
  });
  assert.equal(grant.isFeatureSetActive('zulip.messaging'), true);
  // Unchanged: declarations are not authority.
  assert.equal(grant.has('channels.incoming'), false);
});
