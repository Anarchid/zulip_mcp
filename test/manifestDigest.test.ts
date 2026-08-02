/**
 * MCPL manifest canonical-digest conformance (SPEC 0.5 §17.2 / RFC-003 §3.1).
 *
 * These assertions come from the frozen conformance vector file, not from a
 * second reading of the prose: `test/vectors/manifest-digest-vectors.json`
 * carries RFC-003 §3.1's published vector byte for byte plus the derived cases
 * (set ordering, revision stripping, boolean shorthand, number and string
 * canonicalization, and the negative charset cases).
 *
 * Provenance: vendored verbatim, because the shared location RFC-003 points at
 * (`mcpl/conformance/manifest-digest-vectors.json`) does not exist yet. Point
 * MCPL_DIGEST_VECTORS at it once it does — the loader accepts this shape or a
 * bare array of vectors:
 *
 *     MCPL_DIGEST_VECTORS=../mcpl/conformance/manifest-digest-vectors.json npm test
 *
 * §17.2: "two implementations that agree here agree on canonicalization, set
 * ordering, hashing, and encoding."
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import {
  ManifestDigestError,
  canonicalManifestBytes,
  compareUtf8,
  manifestRevision,
} from '../src/mcpl/manifest.ts';

/** `assert.throws` does not hand back the error, and the code is the assertion. */
function refusal(fn: () => unknown): ManifestDigestError {
  try {
    fn();
  } catch (error) {
    assert.ok(error instanceof ManifestDigestError, `expected ManifestDigestError, got ${error}`);
    return error;
  }
  assert.fail('expected the manifest to be refused, but a revision was produced');
}

interface Vector {
  name: string;
  input: unknown;
  canonicalJson?: string;
  sha256Hex?: string;
  digest?: string;
  expectError?: string;
  errorDetail?: string;
  sameDigestAs?: string;
  differentDigestFrom?: string;
}

interface SortVector {
  name: string;
  input: string[];
  sorted: string[];
}

const vectorsPath =
  process.env.MCPL_DIGEST_VECTORS ??
  fileURLToPath(new URL('./vectors/manifest-digest-vectors.json', import.meta.url));

const file = JSON.parse(readFileSync(vectorsPath, 'utf8')) as {
  vectors: Vector[];
  sortVectors?: SortVector[];
};

const vectors: Vector[] = Array.isArray(file) ? file : file.vectors;
const byName = new Map(vectors.map((v) => [v.name, v]));

test('the vector file actually loaded', () => {
  assert.ok(vectors.length > 0, `no vectors in ${vectorsPath}`);
  assert.ok(
    vectors.some((v) => v.digest === 'sha256:_YZTS0h1tqTAMZI6eElCszSQE2WNx3xhAhmgUvNI9H4'),
    'vector file does not contain the RFC-003 §3.1 published digest',
  );
});

for (const vector of vectors) {
  test(`digest vector: ${vector.name}`, () => {
    if (vector.expectError) {
      const err = refusal(() => manifestRevision(vector.input));
      assert.equal(err.code, vector.expectError);
      if (vector.errorDetail) assert.equal(err.detail, vector.errorDetail);
      return;
    }

    const canonical = canonicalManifestBytes(vector.input);
    if (vector.canonicalJson !== undefined) {
      assert.equal(canonical, vector.canonicalJson);
    }
    if (vector.sha256Hex !== undefined) {
      assert.equal(createHash('sha256').update(canonical, 'utf8').digest('hex'), vector.sha256Hex);
    }
    if (vector.digest !== undefined) {
      assert.equal(manifestRevision(vector.input), vector.digest);
    }

    if (vector.sameDigestAs) {
      const other = byName.get(vector.sameDigestAs);
      assert.ok(other, `unknown cross-reference ${vector.sameDigestAs}`);
      assert.equal(manifestRevision(vector.input), manifestRevision(other.input));
    }
    if (vector.differentDigestFrom) {
      const other = byName.get(vector.differentDigestFrom);
      assert.ok(other, `unknown cross-reference ${vector.differentDigestFrom}`);
      assert.notEqual(manifestRevision(vector.input), manifestRevision(other.input));
    }
  });
}

/**
 * The set-array comparator in isolation. §17.2 requires UTF-8 byte order, not
 * JavaScript's default UTF-16 code-unit comparison — the two disagree above
 * U+FFFF. No set-valued array in the 0.5 manifest shape can legally hold a
 * non-ASCII string, so the divergent cases are unreachable through a conforming
 * manifest and the comparator is exercised directly.
 */
for (const sortVector of file.sortVectors ?? []) {
  test(`set ordering: ${sortVector.name}`, () => {
    assert.deepEqual([...new Set(sortVector.input)].sort(compareUtf8), sortVector.sorted);
  });
}

test('set ordering runs through the manifest path, not only the comparator', () => {
  const canonical = canonicalManifestBytes({
    version: '0.5',
    featureSets: {
      'demo.messaging': { description: 'x', uses: ['tools', 'channels.publish', 'tools', 'pushEvents'] },
    },
  });
  assert.match(canonical, /"uses":\["channels\.publish","pushEvents","tools"\]/);
});

test('a set-valued array holding a non-string is refused, not coerced', () => {
  const err = refusal(() =>
    manifestRevision({
      version: '0.5',
      featureSets: { 'demo.messaging': { description: 'x', uses: ['tools', 7] } },
    }),
  );
  assert.equal(err.code, 'set_member_not_string');
});

test('a non-object manifest is refused', () => {
  const err = refusal(() => manifestRevision(['not', 'a', 'manifest']));
  assert.equal(err.code, 'manifest_not_object');
});
