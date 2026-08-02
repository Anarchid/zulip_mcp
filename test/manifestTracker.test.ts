/**
 * ManifestTracker — SPEC 0.5 §17.1, §17.3, §17.4, §17.10.
 *
 * The tracker's job is narrow on purpose: hold the current manifest, answer
 * `mcpl/manifest` from it, and announce an opaque revision plus the changed
 * domains. It never authors a diff, a payload, or a conclusion — that is the
 * self-attestation defect §5.4 removes, and §17.10 says the impact vocabulary
 * is host-derived precisely so the party making a change does not narrate it.
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { ManifestTracker, manifestRevision } from '../src/mcpl/manifest.ts';
import { buildServerCapabilities } from '../src/mcpl/feature-sets.ts';
import type { ManifestChangedParams, McplServerCapabilities } from '../src/mcpl/types.ts';

const TYPING = { typingCapable: new Set(['zulip']) };

function collector() {
  const announcements: ManifestChangedParams[] = [];
  return { announcements, emit: (p: ManifestChangedParams) => announcements.push(p) };
}

test('the manifest carries its own content digest (§17.1)', () => {
  const caps = buildServerCapabilities(['zulip'], TYPING);
  const tracker = new ManifestTracker(caps);
  const { revision, ...rest } = tracker.manifest;
  assert.ok(revision?.startsWith('sha256:'));
  assert.equal(revision, manifestRevision(rest));
});

test('the digest is stable across restarts for the same configuration (§17.1)', () => {
  const a = new ManifestTracker(buildServerCapabilities(['zulip', 'slack'], TYPING));
  const b = new ManifestTracker(buildServerCapabilities(['zulip', 'slack'], TYPING));
  assert.equal(a.revision, b.revision);
});

test('a different configuration is a different revision', () => {
  const a = new ManifestTracker(buildServerCapabilities(['zulip'], TYPING));
  const b = new ManifestTracker(buildServerCapabilities(['zulip', 'slack'], TYPING));
  assert.notEqual(a.revision, b.revision);
});

test('the last-announced revision is seeded from initialize, so reinstalling it is silent (§17.10)', () => {
  const caps = buildServerCapabilities(['zulip'], TYPING);
  const tracker = new ManifestTracker(caps);
  const { announcements, emit } = collector();

  const domains = tracker.setManifest(buildServerCapabilities(['zulip'], TYPING), emit);
  assert.deepEqual(domains, []);
  assert.deepEqual(announcements, []);
});

test('a real change announces exactly one notification, with domains and nothing else (§17.3)', () => {
  const tracker = new ManifestTracker(buildServerCapabilities(['zulip', 'slack'], TYPING));
  const { announcements, emit } = collector();

  const domains = tracker.setManifest(buildServerCapabilities(['zulip'], TYPING), emit);

  assert.deepEqual(domains, ['featureSets']);
  assert.equal(announcements.length, 1);
  assert.deepEqual(Object.keys(announcements[0]).sort(), ['domains', 'revision']);
  assert.equal(announcements[0].revision, tracker.revision);
  assert.notEqual(announcements[0].revision, manifestRevision(buildServerCapabilities(['zulip', 'slack'], TYPING)));
});

test('a capability-only change reports the capabilities domain (§17.1)', () => {
  const base = buildServerCapabilities(['zulip'], TYPING);
  const tracker = new ManifestTracker(base);
  const { announcements, emit } = collector();

  const next: McplServerCapabilities = {
    ...JSON.parse(JSON.stringify(base)),
    channels: { ...base.channels, typing: false },
  };
  const domains = tracker.setManifest(next, emit);
  assert.deepEqual(domains, ['capabilities']);
  assert.equal(announcements.length, 1);
});

test('a tagOntology change is its own domain and does not move featureSets (§17.1)', () => {
  const base = JSON.parse(JSON.stringify(buildServerCapabilities(['zulip'], TYPING)));
  const tracker = new ManifestTracker(base);
  const { announcements, emit } = collector();

  const next = JSON.parse(JSON.stringify(base));
  next.featureSets['zulip.messaging'].tagOntology = {
    coreTags: ['chat:mention', 'chat:addressed'],
    open: true,
  };
  const domains = tracker.setManifest(next, emit);
  assert.deepEqual(domains, ['tagOntology']);
  assert.equal(announcements.length, 1);
});

test('mcpl/manifest returns the complete manifest, never a delta, and never the live object (§17.4)', () => {
  const tracker = new ManifestTracker(buildServerCapabilities(['zulip', 'discord'], TYPING));
  const answered = tracker.handleManifestRequest();

  assert.deepEqual(answered, tracker.manifest);
  assert.notEqual(answered, tracker.manifest, 'the snapshot must not be handed out by reference');
  assert.ok(answered.featureSets?.['zulip.messaging']);
  assert.ok(answered.featureSets?.['discord.messaging']);
  assert.equal(answered.version, '0.5');

  (answered.featureSets as Record<string, unknown>)['injected'] = { description: 'x', uses: [] };
  assert.equal(tracker.manifest.featureSets?.['injected'], undefined);
});

test('a stale revision supplied by a caller is recomputed, not trusted (§17.1)', () => {
  const caps = buildServerCapabilities(['zulip'], TYPING);
  const tracker = new ManifestTracker({ ...caps, revision: 'sha256:not-a-real-digest' });
  assert.notEqual(tracker.revision, 'sha256:not-a-real-digest');
  assert.equal(tracker.revision, manifestRevision(caps));
});
