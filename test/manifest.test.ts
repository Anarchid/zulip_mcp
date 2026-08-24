/**
 * The server manifest — SPEC 0.5 §17.1, §17.3, §17.4, §17.10.
 *
 * Canonicalization, the content digest, and the change-domain diff are
 * @animalabs/mcpl-core's (conformance-tested there against the RFC-003
 * vectors). These cases pin how this server's manifest rides on them: the
 * digest is content-derived and stable, a change is announced once with the
 * domains it touched, and `mcpl/manifest` answers with a snapshot.
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import {
  ManifestTracker,
  manifestDigest,
  type ManifestChangedParams,
  type McplManifest,
} from '@animalabs/mcpl-core';
import { buildServerCapabilities } from '../src/feature-sets.ts';

function collector() {
  const announcements: ManifestChangedParams[] = [];
  const notifier = {
    sendNotification: (_method: string, params?: unknown) => {
      announcements.push(params as ManifestChangedParams);
    },
  };
  return { announcements, notifier };
}

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value));
}

test('the manifest carries its own content digest (§17.1)', () => {
  const tracker = new ManifestTracker(buildServerCapabilities({ typing: true }));
  const { revision, ...rest } = tracker.snapshot();
  assert.ok(revision?.startsWith('sha256:'));
  assert.equal(revision, manifestDigest(rest));
});

test('the digest is stable across restarts for the same configuration (§17.1)', () => {
  const a = new ManifestTracker(buildServerCapabilities({ typing: true }));
  const b = new ManifestTracker(buildServerCapabilities({ typing: true }));
  assert.equal(a.revision, b.revision);
});

test('a different configuration is a different revision', () => {
  const a = new ManifestTracker(buildServerCapabilities({ typing: true }));
  const b = new ManifestTracker(buildServerCapabilities({ typing: false }));
  assert.notEqual(a.revision, b.revision);
});

test('reinstalling the same manifest is silent (§17.10)', () => {
  const tracker = new ManifestTracker(buildServerCapabilities({ typing: true }));
  const { announcements, notifier } = collector();
  tracker.attach(notifier);

  const change = tracker.setManifest(buildServerCapabilities({ typing: true }));
  assert.equal(change.changed, false);
  assert.deepEqual(change.domains, []);
  assert.deepEqual(announcements, []);
});

test('a real change announces exactly one notification, with domains and nothing else (§17.3)', () => {
  const tracker = new ManifestTracker(buildServerCapabilities({ typing: true }));
  const { announcements, notifier } = collector();
  tracker.attach(notifier);

  // Typing off touches the channel advertisement AND the messaging `uses`.
  const change = tracker.setManifest(buildServerCapabilities({ typing: false }));

  assert.equal(change.changed, true);
  assert.deepEqual(change.domains, ['capabilities', 'featureSets']);
  assert.equal(announcements.length, 1);
  assert.deepEqual(Object.keys(announcements[0]).sort(), ['domains', 'revision']);
  assert.equal(announcements[0].revision, tracker.revision);
  assert.notEqual(announcements[0].revision, manifestDigest(buildServerCapabilities({ typing: true })));
});

test('a capability-only change reports the capabilities domain (§17.1)', () => {
  const base = buildServerCapabilities({ typing: true });
  const tracker = new ManifestTracker(base);
  const { announcements, notifier } = collector();
  tracker.attach(notifier);

  const next = clone(base) as McplManifest;
  (next.channels as { typing?: boolean }).typing = false;
  const change = tracker.setManifest(next);
  assert.deepEqual(change.domains, ['capabilities']);
  assert.equal(announcements.length, 1);
});

test('a tagOntology change is its own domain and does not move featureSets (§17.1)', () => {
  const base = clone(buildServerCapabilities({ typing: true })) as McplManifest;
  const tracker = new ManifestTracker(base);
  const { announcements, notifier } = collector();
  tracker.attach(notifier);

  const next = clone(base) as McplManifest;
  (next.featureSets as Record<string, { tagOntology?: unknown }>)['zulip.messaging'].tagOntology = {
    coreTags: ['chat:mention', 'chat:addressed'],
    open: true,
  };
  const change = tracker.setManifest(next);
  assert.deepEqual(change.domains, ['tagOntology']);
  assert.equal(announcements.length, 1);
});

test('mcpl/manifest returns the complete manifest, never a delta, and never the live object (§17.4)', () => {
  const tracker = new ManifestTracker(buildServerCapabilities({ typing: true }));
  const answered = tracker.handleManifestRequest();

  assert.deepEqual(answered, tracker.snapshot());
  assert.notEqual(answered, tracker.snapshot(), 'the snapshot must not be handed out by reference');
  const featureSets = answered.featureSets as Record<string, unknown>;
  assert.ok(featureSets['zulip.messaging']);
  assert.ok(featureSets['zulip.context']);
  assert.equal(answered.version, '0.5');

  featureSets['injected'] = { description: 'x', uses: [] };
  assert.equal((tracker.snapshot().featureSets as Record<string, unknown>)['injected'], undefined);
});

test('a stale revision supplied by a caller is recomputed, not trusted (§17.1)', () => {
  const caps = buildServerCapabilities({ typing: true });
  const tracker = new ManifestTracker({ ...caps, revision: 'sha256:not-a-real-digest' });
  assert.notEqual(tracker.revision, 'sha256:not-a-real-digest');
  assert.equal(tracker.revision, manifestDigest(caps));
});
