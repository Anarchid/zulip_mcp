/**
 * The filters plane — normalization, the env seed, file load/save, the
 * poll tracker, and the plane's desired/effective/status lifecycle.
 *
 * Run: node --import tsx --test test/filters.test.ts
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, unlinkSync, writeFileSync, utimesSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  FiltersFilePollTracker,
  FiltersPlane,
  loadFiltersFile,
  normalizeFilters,
  normalizeReactionEmoji,
  parseBaselineFromEnv,
  parseFiltersFromEnv,
  saveFiltersFile,
} from '../src/filters.ts';

function tmp(): string {
  return mkdtempSync(join(tmpdir(), 'zulip-filters-'));
}

function quiet<T>(fn: () => T): T {
  const original = console.error;
  console.error = () => {};
  try {
    return fn();
  } finally {
    console.error = original;
  }
}

test('normalizeFilters: empty means unset, duplicates collapse, names lose their #, emails lower-case', () => {
  assert.deepEqual(normalizeFilters({ streams: [], dmUsers: [], mutedStreams: [] }), {});
  assert.deepEqual(
    normalizeFilters({ streams: ['#general', 'general', ' dev '], dmUsers: ['42', 'Ann@Example.com'], mutedStreams: ['#random'] }),
    { streams: ['general', 'dev'], dmUsers: ['42', 'ann@example.com'], mutedStreams: ['random'] },
  );
  // An explicit empty suppression list is a deliberate choice and survives.
  assert.deepEqual(normalizeFilters({ suppressedReactionEmojis: [] }), { suppressedReactionEmojis: [] });
  assert.deepEqual(normalizeFilters({ suppressedReactionEmojis: [':Biohazard:', 'biohazard️'] }), { suppressedReactionEmojis: ['biohazard'] });
  assert.equal(normalizeReactionEmoji(':Thumbs_Up:'), 'thumbs_up');
});

test('parseFiltersFromEnv reads the seed variables; the baseline is host-owned and read separately', () => {
  assert.deepEqual(parseFiltersFromEnv({}), {});
  assert.deepEqual(
    parseFiltersFromEnv({ ZULIP_STREAMS: 'general, dev', ZULIP_DM_USERS: '42', ZULIP_MUTED_STREAMS: 'random', ZULIP_SUPPRESSED_REACTIONS_BASELINE: 'biohazard' }),
    { streams: ['general', 'dev'], dmUsers: ['42'], mutedStreams: ['random'] },
    'the baseline never enters the file seed',
  );
  assert.deepEqual(parseBaselineFromEnv({}), []);
  assert.deepEqual(parseBaselineFromEnv({ ZULIP_SUPPRESSED_REACTIONS_BASELINE: ':Biohazard:, radioactive' }), ['biohazard', 'radioactive']);
  // The host injects one name into every MCPL child, whatever the platform.
  assert.deepEqual(parseBaselineFromEnv({ DISCORD_SUPPRESSED_REACTIONS_BASELINE: 'biohazard' }), ['biohazard']);
  assert.deepEqual(
    parseBaselineFromEnv({ ZULIP_SUPPRESSED_REACTIONS_BASELINE: 'x', DISCORD_SUPPRESSED_REACTIONS_BASELINE: 'y' }),
    ['x'],
    'the Zulip-specific name wins when both are set',
  );
});

test('loadFiltersFile refuses any wrong-typed key: every list is an authorization list', () => {
  const dir = tmp();
  try {
    const path = join(dir, 'f.json');
    writeFileSync(path, JSON.stringify({ dmUsers: [42], extra: true }));
    assert.deepEqual(loadFiltersFile(path), { dmUsers: ['42'] }, 'numbers are accepted as ids; unknown keys are ignored');
    writeFileSync(path, JSON.stringify({ streams: 'private', dmUsers: [42] }));
    assert.equal(loadFiltersFile(path), null, 'a string where a list belongs is not "unrestricted"');
    writeFileSync(path, JSON.stringify({ streams: [{ name: 'x' }] }));
    assert.equal(loadFiltersFile(path), null);
    writeFileSync(path, JSON.stringify({ suppressedReactionEmojis: 'nope' }));
    assert.equal(loadFiltersFile(path), null);
    writeFileSync(path, '[1,2]');
    assert.equal(loadFiltersFile(path), null);
    writeFileSync(path, '{not json');
    assert.equal(loadFiltersFile(path), null);
    assert.equal(loadFiltersFile(join(dir, 'missing.json')), null);

    saveFiltersFile(path, { streams: ['a', 'a'], suppressedReactionEmojis: [] });
    assert.deepEqual(JSON.parse(readFileSync(path, 'utf-8')), { streams: ['a'], suppressedReactionEmojis: [] });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the poll tracker gives one poll of grace, then reports missing, and reloads on reappearance', () => {
  const t = new FiltersFilePollTracker(100);
  assert.equal(t.observe(100), 'none');
  assert.equal(t.observe(101), 'reload');
  assert.equal(t.observe(null), 'none', 'first miss is grace');
  assert.equal(t.observe(null), 'missing');
  assert.equal(t.observe(101), 'reload', 'reappearing with the same mtime still reloads');
});

test('the plane seeds from env, hot-reloads edits, and refuses updates while broken', () => {
  const dir = tmp();
  try {
    const path = join(dir, 'filters.json');
    const plane = new FiltersPlane(path, { ZULIP_STREAMS: 'general', ZULIP_DM_USERS: '42' }, { pollMs: 60_000 });
    quiet(() => plane.start());
    assert.deepEqual(JSON.parse(readFileSync(path, 'utf-8')), { streams: ['general'], dmUsers: ['42'] });
    assert.equal(plane.streamAllowed('general'), true);
    assert.equal(plane.streamAllowed('dev'), false);
    assert.equal(plane.dmAllowed({ id: 42, email: 'x' }), true);
    assert.equal(plane.dmAllowed({ id: 7, email: 'x' }), false);
    assert.equal(plane.planeStatus().status, 'live');
    assert.equal(plane.suppressionStatus().status, 'not-configured');

    // An agent-side update writes through and notifies.
    const changes: string[] = [];
    plane.onChange((next) => changes.push(JSON.stringify(next)));
    const updated = plane.update((f) => ({ ...f, mutedStreams: ['random'], streams: [...(f.streams ?? []), 'dev'] }));
    assert.equal(updated.ok, true);
    assert.equal(plane.streamAllowed('dev'), true);
    assert.equal(plane.streamMuted('random'), true);
    assert.equal(changes.length, 1);
    assert.deepEqual(JSON.parse(readFileSync(path, 'utf-8')).mutedStreams, ['random']);

    // A hand edit is picked up by the poll; touch the mtime forward so the
    // change is observable regardless of filesystem timestamp granularity.
    writeFileSync(path, JSON.stringify({ streams: ['dev'], suppressedReactionEmojis: ['biohazard'] }));
    utimesSync(path, new Date(Date.now() + 5000), new Date(Date.now() + 5000));
    quiet(() => plane.tick());
    assert.equal(plane.streamAllowed('general'), false);
    assert.equal(plane.reactionSuppressed(':Biohazard:'), true);
    assert.equal(plane.suppressionStatus().status, 'active');
    assert.equal(plane.suppressionStatus().effectiveCount, 1);
    assert.equal(changes.length, 2);

    // A corrupt rewrite keeps the last-known-good filters and marks the plane stale.
    writeFileSync(path, '{broken');
    utimesSync(path, new Date(Date.now() + 10_000), new Date(Date.now() + 10_000));
    quiet(() => plane.tick());
    assert.equal(plane.streamAllowed('dev'), true, 'last-known-good stays in force');
    assert.equal(plane.planeStatus().status, 'stale');
    assert.equal(plane.planeStatus().desiredState, 'invalid');
    assert.equal(plane.suppressionStatus().status, 'stale');
    const refused = plane.update((f) => f);
    assert.equal(refused.ok, false);
    assert.match((refused as { reason: string }).reason, /invalid on disk/);

    // Deleted: one poll of grace, then missing.
    unlinkSync(path);
    quiet(() => plane.tick());
    assert.equal(plane.planeStatus().desiredState, 'invalid', 'grace poll');
    quiet(() => plane.tick());
    assert.equal(plane.planeStatus().desiredState, 'missing');

    // Repaired: live again, and the update goes through.
    writeFileSync(path, JSON.stringify({ streams: ['ops'] }));
    quiet(() => plane.tick());
    assert.equal(plane.planeStatus().status, 'live');
    assert.equal(plane.streamAllowed('ops'), true);
    assert.equal(plane.update((f) => f).ok, true);
    plane.stop();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the host baseline is enforced every start, is never written to the file, and merges with file entries', () => {
  const dir = tmp();
  try {
    const path = join(dir, 'filters.json');
    const first = new FiltersPlane(path, { DISCORD_SUPPRESSED_REACTIONS_BASELINE: 'biohazard' }, { pollMs: 60_000 });
    quiet(() => first.start());
    assert.deepEqual(JSON.parse(readFileSync(path, 'utf-8')), {}, 'the seed carries no baseline');
    assert.equal(first.reactionSuppressed('biohazard'), true);
    assert.equal(first.reactionSuppressed('eyes'), false);
    assert.deepEqual(
      { ...first.suppressionStatus(), effectiveDigest: undefined },
      { status: 'active', protectionActive: true, effectiveCount: 1, baselineCount: 1, source: 'baseline', effectiveDigest: undefined },
    );
    first.stop();

    // A later deployment changes the host's markers: the new set applies,
    // the old one is gone — nothing was frozen into the file.
    const second = new FiltersPlane(path, { DISCORD_SUPPRESSED_REACTIONS_BASELINE: 'radioactive' }, { pollMs: 60_000 });
    quiet(() => second.start());
    assert.equal(second.reactionSuppressed('biohazard'), false);
    assert.equal(second.reactionSuppressed('radioactive'), true);

    // Operator entries in the file add to the baseline; neither removes the other.
    writeFileSync(path, JSON.stringify({ suppressedReactionEmojis: ['skull'] }));
    utimesSync(path, new Date(Date.now() + 5000), new Date(Date.now() + 5000));
    quiet(() => second.tick());
    assert.equal(second.reactionSuppressed('skull'), true);
    assert.equal(second.reactionSuppressed('radioactive'), true);
    assert.equal(second.suppressionStatus().effectiveCount, 2);
    assert.equal(second.suppressionStatus().source, 'file+baseline');
    // An agent-side update round-trips the file without the baseline leaking in.
    second.update((f) => ({ ...f, mutedStreams: ['random'] }));
    assert.deepEqual(JSON.parse(readFileSync(path, 'utf-8')).suppressedReactionEmojis, ['skull']);
    second.stop();

    // Without a baseline and without a file key: honestly unprotected.
    const bare = new FiltersPlane(join(dir, 'bare.json'), {}, { pollMs: 60_000 });
    quiet(() => bare.start());
    assert.equal(bare.suppressionStatus().status, 'not-configured');
    bare.stop();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a filters file that cannot be created fails the start, not silently', () => {
  const dir = tmp();
  try {
    // A regular file where the parent directory should be.
    writeFileSync(join(dir, 'blocker'), '');
    const plane = new FiltersPlane(join(dir, 'blocker', 'filters.json'), {}, { pollMs: 60_000 });
    assert.throws(() => quiet(() => plane.start()), /cannot create the filters file/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a broken file at startup runs the env seed and withholds all reactions until repaired', () => {
  const dir = tmp();
  try {
    const path = join(dir, 'filters.json');
    writeFileSync(path, '{nope');
    const plane = new FiltersPlane(path, { ZULIP_STREAMS: 'general' }, { pollMs: 60_000 });
    quiet(() => plane.start());
    assert.equal(readFileSync(path, 'utf-8'), '{nope', 'the operator file is never overwritten');
    assert.equal(plane.streamAllowed('general'), true);
    assert.equal(plane.planeStatus().status, 'unavailable');
    assert.equal(plane.suppressAllReactions(), true);
    assert.equal(plane.suppressionStatus().suppressingAllReactions, true);
    assert.equal(plane.update((f) => f).ok, false);

    writeFileSync(path, JSON.stringify({ streams: ['general'], suppressedReactionEmojis: [] }));
    utimesSync(path, new Date(Date.now() + 5000), new Date(Date.now() + 5000));
    quiet(() => plane.tick());
    assert.equal(plane.planeStatus().status, 'live');
    assert.equal(plane.suppressAllReactions(), false);
    assert.equal(plane.suppressionStatus().status, 'configured-empty');
    plane.stop();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
