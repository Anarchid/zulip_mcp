/**
 * Capability grant and feature-set declaration conformance (SPEC 0.5 §5.3,
 * §5.4, §6.2, §6.4, §6.7).
 *
 * The theme of every case here is that absence is denial. Nothing the server
 * says — not its advertisement, not its degradation receipt — may widen what
 * the host granted. The message-to-grant step itself is @animalabs/mcpl-core's;
 * these cases pin how this server's wrapper composes it over a connection's
 * lifetime.
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { isCapabilityPath } from '@animalabs/mcpl-core';
import { CapabilityGrant } from '../src/grant.ts';
import { McplRpcError } from '../src/errors.ts';
import { buildFeatureSets, buildServerCapabilities } from '../src/feature-sets.ts';

// --- §6.2: `uses` is a closed vocabulary ------------------------------------

test('every declared `uses` value is in the SPEC §6.2 capability-path vocabulary', () => {
  const sets = buildFeatureSets({ typing: true });
  assert.ok(Object.keys(sets).length > 0);
  for (const [name, decl] of Object.entries(sets)) {
    assert.ok(Array.isArray(decl.uses) && decl.uses.length > 0, `${name}: uses must be non-empty (§6.4)`);
    for (const use of decl.uses) {
      assert.ok(isCapabilityPath(use), `${name}: '${use}' is not a §6.2 capability path`);
    }
  }
});

test('struck and removed vocabulary is not declared anywhere', () => {
  const serialized = JSON.stringify(buildServerCapabilities({ typing: true }));
  // `channels.observe` was struck in 0.5.0 (inbound content is
  // `channels.incoming`); `afterInference` was removed with §10.5.
  assert.ok(!serialized.includes('channels.observe'), 'channels.observe was struck in 0.5.0');
  assert.ok(!serialized.includes('afterInference'), 'context/afterInference was removed in 0.5.0');
  // The un-split `contextHooks.beforeInference` is no longer a capability path.
  assert.ok(!serialized.includes('"contextHooks.beforeInference"'));
});

test('channels.typing is advertised only when the adapter implements it', () => {
  const withTyping = buildServerCapabilities({ typing: true });
  assert.equal((withTyping.channels as { typing?: boolean }).typing, true);
  assert.ok(
    (withTyping.featureSets as Record<string, { uses: string[] }>)['zulip.messaging'].uses.includes('channels.typing'),
  );

  const withoutTyping = buildServerCapabilities({ typing: false });
  assert.equal((withoutTyping.channels as { typing?: boolean }).typing, false);
  assert.ok(
    !(withoutTyping.featureSets as Record<string, { uses: string[] }>)['zulip.messaging'].uses.includes('channels.typing'),
  );
});

test('the manifest advertises injection without observation (§10.1 write-without-read)', () => {
  const caps = buildServerCapabilities({ typing: true });
  const hooks = caps.contextHooks as { beforeInference: { observe: boolean; inject: unknown } };
  assert.equal(hooks.beforeInference.observe, false);
  assert.deepEqual(hooks.beforeInference.inject, {
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

test('a `*` wildcard matches exactly one segment (§5.4, pinned 2026-08-02)', () => {
  const grant = new CapabilityGrant();
  grant.apply({ effectiveCapabilities: ['channels.*', 'contextHooks.beforeInference.inject.*'] });
  assert.equal(grant.has('channels.publish'), true);
  assert.equal(grant.has('channels.acknowledge'), true);
  assert.equal(grant.has('contextHooks.beforeInference.inject.system'), true);
  assert.equal(grant.has('contextHooks.beforeInference.observe'), false);
  assert.equal(grant.has('pushEvents'), false);

  // A trailing `*` is NOT a subtree match: `contextHooks.*` reaches none of
  // the depth-4 injection leaves. A mistaken narrow pattern can only
  // under-grant, which the host observes and corrects.
  const shallow = new CapabilityGrant();
  shallow.apply({ effectiveCapabilities: ['contextHooks.*'] });
  assert.equal(shallow.has('contextHooks.beforeInference.inject.beforeUser'), false);
});

test('an update with no effectiveCapabilities empties the grant rather than leaving it standing', () => {
  const grant = new CapabilityGrant();
  grant.apply({ effectiveCapabilities: ['channels.publish'] });
  assert.equal(grant.has('channels.publish'), true);

  const receipt = grant.apply({ enabled: ['zulip.messaging'] });
  assert.equal(grant.has('channels.publish'), false);
  assert.ok(receipt.notes.some((n) => n.includes('empty')));
});

test('a path in both effectiveCapabilities and deniedCapabilities is rejected as malformed and fails closed (§5.4)', () => {
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
  // Fail closed means closed: a malformed policy leaves nothing granted and
  // no ready state. The previous, wider grant does not survive it.
  assert.equal(grant.isReady(), false);
  assert.equal(grant.has('channels.publish'), false);
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

test('a Notification neither widens nor rewrites the grant; only `disabled` reductions apply (§6.7)', () => {
  const grant = new CapabilityGrant(declarations());
  grant.apply({ effectiveCapabilities: ['channels.register', 'channels.publish', 'channels.incoming'] });
  assert.equal(grant.isFeatureSetActive('zulip.messaging'), true);

  const widening = grant.apply(
    { effectiveCapabilities: ['channels.register', 'channels.publish', 'channels.incoming', 'tools'] },
    'notification',
  );
  assert.equal(grant.has('tools'), false, 'a Notification must not widen');
  assert.ok(widening.notes.some((n) => n.includes('effectiveCapabilities')), 'the discarded field is named');

  // A grant carried by an unacknowledgeable message is discarded whole — it
  // is not read as a narrowing either. Reductions travel as `disabled`.
  grant.apply({ effectiveCapabilities: ['channels.register'] }, 'notification');
  assert.equal(grant.has('channels.publish'), true);

  grant.apply({ disabled: ['zulip.messaging'] }, 'notification');
  assert.equal(grant.isFeatureSetActive('zulip.messaging'), false, 'a reduction is respected immediately');
  assert.equal(grant.has('channels.publish'), true, 'the capability grant itself is untouched');
});

test('the Request form is what expands a grant (§6.7 tell → receipt → activate)', () => {
  const grant = new CapabilityGrant(declarations());
  grant.apply({ effectiveCapabilities: ['channels.register'] });
  assert.equal(grant.has('channels.publish'), false);
  grant.apply({ effectiveCapabilities: ['channels.register', 'channels.publish'] }, 'request');
  assert.equal(grant.has('channels.publish'), true);
});

test('whenReady resolves on the first grant-bearing Request and never on a Notification', async () => {
  const grant = new CapabilityGrant(declarations());
  let ready = false;
  const waiting = grant.whenReady().then(() => { ready = true; });
  grant.apply({ effectiveCapabilities: ['channels.register'] }, 'notification');
  await Promise.resolve();
  assert.equal(ready, false);
  grant.apply({ effectiveCapabilities: ['channels.register'] }, 'request');
  await waiting;
  assert.equal(ready, true);
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
