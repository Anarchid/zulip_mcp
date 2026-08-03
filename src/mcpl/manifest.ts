/**
 * Server manifest changes — SPEC 0.5 §17 (RFC-003).
 *
 * Two things live here:
 *
 * 1. {@link manifestRevision} — the canonical content digest of §17.2:
 *
 *      revision = "sha256:" + base64url_unpadded( SHA-256( JCS( manifest_without_revision ) ) )
 *
 *    JCS is RFC 8785. The root `revision` member is excluded so the digest never
 *    covers itself; nothing else is stripped (`version` is included, a nested
 *    member that happens to be named `revision` is ordinary content). The digest
 *    is content-derived precisely so it cannot be hand-maintained out of sync.
 *
 *    The rule is not reimplemented from prose: it is checked against the frozen
 *    conformance vectors in `test/vectors/manifest-digest-vectors.json`
 *    (test/manifestDigest.test.ts), which carry RFC-003 §3.1's published vector
 *    verbatim plus the derived cases.
 *
 * 2. {@link ManifestTracker} — holds the current manifest, derives the changed
 *    domains by diffing old against new, and emits `mcpl/manifestChanged`
 *    (§17.3) when the revision this connection last announced differs.
 *
 * What is deliberately absent: the notification carries no diff, no list of
 * additions or removals, and no policy conclusion (§17.3, App. B.3
 * `additionalProperties: false`). Everything a server might assert about its
 * own change is something the host has to re-derive anyway, and asserting it
 * is the self-attestation defect §5.4 exists to remove. The host fetches
 * `mcpl/manifest` and diffs; this module never tells it what to conclude, and
 * never generates resident-facing prose (§17.10).
 */

import { createHash } from 'node:crypto';
import type {
  ManifestChangeDomain,
  ManifestChangedParams,
  McplServerCapabilities,
} from './types.js';

type Json = null | boolean | number | string | Json[] | { [k: string]: Json };

/**
 * §17.2 / RFC-003 §3.1: "capability paths and tag identifiers MUST be ASCII —
 * `[A-Za-z0-9._:*-]`". Non-empty: the empty string is not a capability path.
 */
const IDENTIFIER = /^[A-Za-z0-9._:*-]+$/;

export type ManifestDigestErrorCode =
  | 'identifier_charset'
  | 'set_member_not_string'
  | 'manifest_not_object';

/**
 * Refusal to digest a manifest, rather than silently hashing one whose ordering
 * is not interoperable.
 *
 * §17.2 states the ASCII restriction as a MUST but does not say who enforces
 * it. Refusing at digest time is the fail-closed reading: the UTF-8-vs-UTF-16
 * ordering divergence the restriction exists to prevent becomes reachable the
 * moment a non-ASCII string enters a set-valued array, and a server that cannot
 * produce an interoperable revision should not publish one.
 */
export class ManifestDigestError extends Error {
  constructor(
    readonly code: ManifestDigestErrorCode,
    readonly detail: string,
  ) {
    super(`${code}: ${detail}`);
    this.name = 'ManifestDigestError';
  }
}

function isPlainObject(value: unknown): value is { [k: string]: Json } {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function checkIdentifier(value: unknown, path: string): void {
  if (typeof value !== 'string' || !IDENTIFIER.test(value)) {
    throw new ManifestDigestError('identifier_charset', `${path} = '${String(value)}'`);
  }
}

/**
 * §17.2 set ordering: by UTF-8 byte sequence, ascending. Not the platform
 * default — JavaScript's `Array.prototype.sort` compares UTF-16 code units,
 * which disagrees with UTF-8 byte order above U+FFFF. Capability paths and tag
 * identifiers are ASCII, where the two coincide; this comparator governs
 * anything else.
 */
export function compareUtf8(a: string, b: string): number {
  return Buffer.compare(Buffer.from(a, 'utf8'), Buffer.from(b, 'utf8'));
}

// ---------------------------------------------------------------------------
// RFC 8785 primitives
// ---------------------------------------------------------------------------

function scalar(value: Json): string {
  if (value === null) return 'null';
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new Error('Manifest contains a non-finite number; RFC 8785 has no serialization for it');
    }
    // RFC 8785 §3.2.2.3 uses the ECMAScript number-to-string algorithm, which
    // is exactly what JSON.stringify emits for finite numbers.
    return JSON.stringify(value);
  }
  // RFC 8785 §3.2.2.2 string serialization is the ECMAScript JSON.stringify
  // escaping: only U+0022, U+005C and the C0 controls are escaped.
  return JSON.stringify(value as string);
}

/** RFC 8785 §3.2.3 sorts object members by the UTF-16 code units of the member
 *  name, which is JavaScript's default string comparison. */
function memberNames(obj: { [k: string]: Json }): string[] {
  return Object.keys(obj).filter((k) => obj[k] !== undefined).sort();
}

/** A value with no MCPL-defined structure: every array is a list, no member
 *  name is an identifier position. */
function generic(value: Json): string {
  if (Array.isArray(value)) return '[' + value.map(generic).join(',') + ']';
  if (isPlainObject(value)) {
    return '{' + memberNames(value).map((k) => JSON.stringify(k) + ':' + generic(value[k])).join(',') + '}';
  }
  return scalar(value);
}

/**
 * A set-valued array (§17.2): de-duplicated, sorted by UTF-8 byte order.
 * Members must be strings, and every set-valued array in the 0.5 manifest shape
 * holds identifiers, so each member is charset-checked.
 */
function setArray(value: Json, path: string): string {
  if (!Array.isArray(value)) return generic(value);
  const unique: string[] = [];
  const seen = new Set<string>();
  for (const item of value) {
    if (typeof item !== 'string') {
      throw new ManifestDigestError(
        'set_member_not_string',
        `${path}[] = ${JSON.stringify(item)}`,
      );
    }
    checkIdentifier(item, `${path}[]`);
    if (seen.has(item)) continue;
    seen.add(item);
    unique.push(item);
  }
  unique.sort(compareUtf8);
  return '[' + unique.map((s) => JSON.stringify(s)).join(',') + ']';
}

/** A list-valued array whose members are nonetheless identifiers
 *  (`keyed.*.values`, `suggestedTreatment.*.tags{Any,All,None}`): order is
 *  preserved verbatim, duplicates kept, but the charset rule still applies. */
function identifierList(value: Json, path: string): string {
  if (!Array.isArray(value)) return generic(value);
  for (const item of value) checkIdentifier(item, `${path}[]`);
  return '[' + value.map((s) => JSON.stringify(s as string)).join(',') + ']';
}

// ---------------------------------------------------------------------------
// Manifest-shaped serialization
// ---------------------------------------------------------------------------

/**
 * A capability subtree. §5.1 says advertisement mirrors the capability paths, so
 * every nested member name is a path segment and is charset-checked. Boolean
 * shorthand at an interior node is NOT expanded: §17.2 defines no expansion,
 * and expanding would make the digest depend on a vocabulary §5.4 says will
 * grow.
 */
function capability(value: Json, path: string): string {
  if (isPlainObject(value)) {
    const keys = memberNames(value);
    for (const k of keys) checkIdentifier(k, `${path}.${k}`);
    return '{' + keys.map((k) => JSON.stringify(k) + ':' + capability(value[k], `${path}.${k}`)).join(',') + '}';
  }
  if (Array.isArray(value)) return generic(value);
  return scalar(value);
}

function tagOntology(value: Json, path: string): string {
  if (!isPlainObject(value)) return generic(value);
  const parts: string[] = [];
  for (const k of memberNames(value)) {
    const v = value[k];
    let serialized: string;
    switch (k) {
      case 'coreTags':
        serialized = setArray(v, `${path}.coreTags`);
        break;
      case 'tags':
        serialized = tagMap(v, `${path}.tags`);
        break;
      case 'keyed':
        serialized = keyedMap(v, `${path}.keyed`);
        break;
      case 'suggestedTreatment':
        serialized = treatmentRules(v, `${path}.suggestedTreatment`);
        break;
      default:
        serialized = generic(v);
    }
    parts.push(JSON.stringify(k) + ':' + serialized);
  }
  return '{' + parts.join(',') + '}';
}

function tagMap(value: Json, path: string): string {
  if (!isPlainObject(value)) return generic(value);
  const parts: string[] = [];
  for (const tag of memberNames(value)) {
    checkIdentifier(tag, `${path}.${tag}`);
    const decl = value[tag];
    if (!isPlainObject(decl)) {
      parts.push(JSON.stringify(tag) + ':' + generic(decl));
      continue;
    }
    const inner: string[] = [];
    for (const k of memberNames(decl)) {
      let serialized: string;
      if (k === 'implies') {
        serialized = setArray(decl[k], `${path}.${tag}.implies`);
      } else if (k === 'facet') {
        checkIdentifier(decl[k], `${path}.${tag}.facet`);
        serialized = scalar(decl[k]);
      } else {
        serialized = generic(decl[k]);
      }
      inner.push(JSON.stringify(k) + ':' + serialized);
    }
    parts.push(JSON.stringify(tag) + ':{' + inner.join(',') + '}');
  }
  return '{' + parts.join(',') + '}';
}

function keyedMap(value: Json, path: string): string {
  if (!isPlainObject(value)) return generic(value);
  const parts: string[] = [];
  for (const family of memberNames(value)) {
    checkIdentifier(family, `${path}.${family}`);
    const decl = value[family];
    if (!isPlainObject(decl)) {
      parts.push(JSON.stringify(family) + ':' + generic(decl));
      continue;
    }
    const inner = memberNames(decl).map((k) =>
      JSON.stringify(k) +
      ':' +
      // §17.2: `keyed.*.values` is a LIST — order is meaningful and preserved.
      (k === 'values' ? identifierList(decl[k], `${path}.${family}.values`) : generic(decl[k])),
    );
    parts.push(JSON.stringify(family) + ':{' + inner.join(',') + '}');
  }
  return '{' + parts.join(',') + '}';
}

function treatmentRules(value: Json, path: string): string {
  if (!Array.isArray(value)) return generic(value);
  return (
    '[' +
    value
      .map((rule) => {
        if (!isPlainObject(rule)) return generic(rule);
        const inner = memberNames(rule).map((k) =>
          JSON.stringify(k) +
          ':' +
          // Semantically sets, but §17.2 does not name them, so they are lists.
          (k === 'tagsAny' || k === 'tagsAll' || k === 'tagsNone'
            ? identifierList(rule[k], `${path}[].${k}`)
            : generic(rule[k])),
        );
        return '{' + inner.join(',') + '}';
      })
      .join(',') +
    ']'
  );
}

function featureSetDeclaration(value: Json, path: string): string {
  if (!isPlainObject(value)) return generic(value);
  const parts: string[] = [];
  for (const k of memberNames(value)) {
    let serialized: string;
    if (k === 'uses') serialized = setArray(value[k], `${path}.uses`);
    else if (k === 'tagOntology') serialized = tagOntology(value[k], `${path}.tagOntology`);
    else serialized = generic(value[k]);
    parts.push(JSON.stringify(k) + ':' + serialized);
  }
  return '{' + parts.join(',') + '}';
}

function featureSets(value: Json, path: string): string {
  if (!isPlainObject(value)) return generic(value);
  const parts: string[] = [];
  for (const name of memberNames(value)) {
    checkIdentifier(name, `${path}.${name}`);
    parts.push(JSON.stringify(name) + ':' + featureSetDeclaration(value[name], `${path}.${name}`));
  }
  return '{' + parts.join(',') + '}';
}

/**
 * The canonical bytes hashed by {@link manifestRevision}. Exported so the §17.2
 * conformance vectors can assert on the intermediate form: two implementations
 * that agree here agree on canonicalization and set ordering.
 */
export function canonicalManifestBytes(manifest: unknown): string {
  if (!isPlainObject(manifest)) {
    throw new ManifestDigestError('manifest_not_object', typeof manifest);
  }
  const parts: string[] = [];
  for (const k of memberNames(manifest)) {
    // §17.2: exactly one thing is stripped, and only at the root.
    if (k === 'revision') continue;
    let serialized: string;
    if (k === 'featureSets') {
      serialized = featureSets(manifest[k], 'featureSets');
    } else if (k === 'version') {
      // Protocol identity, not a capability path (§17.1).
      serialized = generic(manifest[k]);
    } else {
      checkIdentifier(k, k);
      serialized = capability(manifest[k], k);
    }
    parts.push(JSON.stringify(k) + ':' + serialized);
  }
  return '{' + parts.join(',') + '}';
}

/** §17.2 — `"sha256:" + base64url_unpadded(SHA-256(JCS(manifest_without_revision)))`. */
export function manifestRevision(manifest: unknown): string {
  const bytes = canonicalManifestBytes(manifest);
  // Node's 'base64url' encoding is RFC 4648 §5 and is already unpadded.
  return 'sha256:' + createHash('sha256').update(bytes, 'utf8').digest('base64url');
}

// ---------------------------------------------------------------------------
// Change domains (§17.1)
// ---------------------------------------------------------------------------

function stripTagOntology(sets: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [name, decl] of Object.entries(sets)) {
    if (isPlainObject(decl)) {
      const { tagOntology: _omit, ...rest } = decl as Record<string, unknown>;
      out[name] = rest;
    } else {
      out[name] = decl;
    }
  }
  return out;
}

function tagOntologies(sets: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [name, decl] of Object.entries(sets)) {
    if (isPlainObject(decl)) {
      const ontology = (decl as Record<string, unknown>).tagOntology;
      if (ontology !== undefined) out[name] = ontology;
    }
  }
  return out;
}

/**
 * §17.1 — the three domains partition the manifest:
 *   capabilities — every member other than `version`, `revision`, `featureSets`
 *   featureSets  — the `featureSets` member, excluding any `tagOntology` in it
 *   tagOntology  — the `tagOntology` of any feature set
 */
export function changedDomains(
  previous: Record<string, unknown>,
  next: Record<string, unknown>,
): ManifestChangeDomain[] {
  const domains = new Set<ManifestChangeDomain>();

  const members = new Set([...Object.keys(previous), ...Object.keys(next)]);
  for (const member of members) {
    if (member === 'featureSets' || member === 'version' || member === 'revision') continue;
    const a = capability((previous[member] ?? null) as Json, member);
    const b = capability((next[member] ?? null) as Json, member);
    if (a !== b) domains.add('capabilities');
  }

  const prevFs = (previous.featureSets ?? {}) as Record<string, unknown>;
  const nextFs = (next.featureSets ?? {}) as Record<string, unknown>;
  if (
    featureSets(stripTagOntology(prevFs) as Json, 'featureSets') !==
    featureSets(stripTagOntology(nextFs) as Json, 'featureSets')
  ) {
    domains.add('featureSets');
  }
  if (
    generic(tagOntologies(prevFs) as Json) !== generic(tagOntologies(nextFs) as Json)
  ) {
    domains.add('tagOntology');
  }

  // App. B.3 requires `domains` to have at least one entry, so a caller that
  // reaches the notification path with an empty set must not send one; the
  // tracker checks for that.
  return Array.from(domains).sort();
}

export type ManifestChangedEmitter = (params: ManifestChangedParams) => void;

/**
 * Holds the current manifest and answers `mcpl/manifest` (§17.4) from the same
 * canonical snapshot the digest was computed over.
 *
 * The connection's last-announced revision is seeded from the manifest
 * presented at `initialize`, so a fresh connection does not fire a redundant
 * announcement for a manifest the handshake already carried (§17.10).
 */
export class ManifestTracker {
  private current: McplServerCapabilities;
  private lastAnnounced: string;

  constructor(manifest: McplServerCapabilities) {
    this.current = ManifestTracker.withRevision(manifest);
    // Seeded from initialize: this is what the host already has.
    this.lastAnnounced = this.current.revision!;
  }

  private static withRevision(manifest: McplServerCapabilities): McplServerCapabilities {
    const { revision: _drop, ...rest } = manifest;
    return { ...rest, revision: manifestRevision(rest as Record<string, unknown>) };
  }

  /** The complete current manifest — the `experimental.mcpl` object itself. */
  get manifest(): McplServerCapabilities {
    return this.current;
  }

  get revision(): string {
    return this.current.revision!;
  }

  /**
   * `mcpl/manifest` (§17.4): the current, complete manifest, never a delta.
   * Returned as a defensive copy so a caller cannot mutate the snapshot the
   * digest was computed over.
   */
  handleManifestRequest(): McplServerCapabilities {
    return JSON.parse(JSON.stringify(this.current)) as McplServerCapabilities;
  }

  /**
   * Install a new manifest atomically and, if the content actually changed,
   * emit one `mcpl/manifestChanged`. Servers call this rather than
   * hand-authoring an announcement (§17.10) — the digest moves on its own, so a
   * change cannot be installed without being announced.
   *
   * Returns the domains announced, or an empty array when nothing changed.
   */
  setManifest(next: McplServerCapabilities, emit?: ManifestChangedEmitter): ManifestChangeDomain[] {
    const previous = this.current;
    const installed = ManifestTracker.withRevision(next);

    if (installed.revision === this.lastAnnounced) {
      this.current = installed;
      return [];
    }

    const domains = changedDomains(
      previous as unknown as Record<string, unknown>,
      installed as unknown as Record<string, unknown>,
    );

    this.current = installed;
    this.lastAnnounced = installed.revision!;

    if (domains.length === 0) {
      // The digest moved but no domain did — only `version` can do that, and
      // §17.1 says version is not a domain. Nothing to announce.
      return [];
    }

    emit?.({ revision: installed.revision!, domains });
    return domains;
  }
}
