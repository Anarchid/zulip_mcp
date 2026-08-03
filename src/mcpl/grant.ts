/**
 * Capability grant state (SPEC 0.5 §5.3, §5.4, §6.4, §6.7).
 *
 * The grant is the security boundary. This class holds what the *host* told
 * this connection it may do, and nothing else:
 *
 * - `effectiveCapabilities` is the sole normative allowlist. Every path not
 *   present is denied; absence is the denial, there is no unspecified state
 *   (§5.4).
 * - `deniedCapabilities` is derived diagnostic data. It is read only to detect
 *   a malformed policy message and never participates in an authorization
 *   decision (§5.4).
 * - Until the initial policy exchange completes, every capability-dependent
 *   behavior is unavailable (§5.3). `has()` therefore returns false before the
 *   first `featureSets/update`.
 *
 * Nothing here widens anything. The degradation receipt this produces is
 * testimony about consequences, never an assertion of entitlement (§6.7).
 */

import { McplRpcError } from './errors.js';
import type {
  DegradationReceipt,
  FeatureSetDeclaration,
  FeatureSetsUpdateParams,
  UnavailableFeature,
} from './types.js';
import { CAPABILITY_PATH_SET } from './types.js';

/**
 * Match a capability path against a grant entry. §5.4 requires matching over
 * full paths with `*` wildcards and a generic recursive walk — a granted
 * interior node does NOT imply its leaves, so `channels` alone grants nothing;
 * a host that means every leaf sends `channels.*`.
 */
function matches(entry: string, path: string): boolean {
  const e = entry.split('.');
  const p = path.split('.');
  if (e.length !== p.length) {
    // A trailing `*` is the only length-flexible form: `channels.*` covers
    // every path beneath `channels`, at any depth.
    if (e[e.length - 1] !== '*' || p.length < e.length) return false;
    for (let i = 0; i < e.length - 1; i++) {
      if (e[i] !== '*' && e[i] !== p[i]) return false;
    }
    return true;
  }
  for (let i = 0; i < e.length; i++) {
    if (e[i] !== '*' && e[i] !== p[i]) return false;
  }
  return true;
}

export class CapabilityGrant {
  /** null until the initial policy exchange completes (§5.3). */
  private effective: string[] | null = null;
  private explicitlyEnabled: Set<string> | null = null;
  private explicitlyDisabled = new Set<string>();
  private readyWaiters: (() => void)[] = [];

  constructor(private declarations: Record<string, FeatureSetDeclaration> = {}) {}

  /**
   * Point the grant at the feature-set declarations of a newly installed
   * manifest (§17.5). Declarations are what degradation is derived from
   * (§6.4); they are not authority, so this widens nothing — the effective
   * grant is untouched and stays whatever the host last sent.
   */
  setDeclarations(declarations: Record<string, FeatureSetDeclaration>): void {
    this.declarations = declarations;
  }

  /** True once a `featureSets/update` has been accepted. */
  isReady(): boolean {
    return this.effective !== null;
  }

  /** Resolves the first time a policy is accepted. */
  whenReady(): Promise<void> {
    if (this.isReady()) return Promise.resolve();
    return new Promise<void>((resolve) => this.readyWaiters.push(resolve));
  }

  /**
   * Is `path` in the effective grant? False before the initial policy
   * exchange, and false for any path the host did not name.
   */
  has(path: string): boolean {
    if (this.effective === null) return false;
    return this.effective.some((entry) => matches(entry, path));
  }

  /** Is a declared feature set currently active? */
  isFeatureSetActive(name: string): boolean {
    if (this.effective === null) return false;
    if (this.explicitlyDisabled.has(name)) return false;
    if (this.explicitlyEnabled !== null && !this.explicitlyEnabled.has(name)) return false;
    return this.missingFor(name).length === 0;
  }

  /** Capability paths a declared feature set needs but has not been granted. */
  missingFor(name: string): string[] {
    const decl = this.declarations[name];
    if (!decl) return [];
    return decl.uses.filter((use) => !this.has(use));
  }

  /**
   * Apply a `featureSets/update` and return the degradation receipt (§6.7).
   *
   * Throws {@link McplRpcError} when the message is malformed — §5.4 requires
   * the receiving side to fail closed and reject a policy naming a path in
   * both `effectiveCapabilities` and `deniedCapabilities`. The previous grant
   * is left untouched in that case, so a malformed message cannot widen or
   * silently narrow anything.
   *
   * `form` is how the message arrived. §6.7 requires the **Request** form for
   * any change to the effective grant and says a Notification "cannot
   * establish a ready state". A Notification is therefore never allowed to
   * widen: before the initial policy exchange it establishes nothing, and
   * afterwards it can only intersect. A host that narrows by Notification is
   * outside §6.7, but a reduction MUST be respected immediately, so the
   * narrowing direction is still honoured — the direction that could hand this
   * server authority is the one that is refused.
   */
  apply(
    params: FeatureSetsUpdateParams,
    form: 'request' | 'notification' = 'request',
  ): DegradationReceipt {
    const effective = Array.isArray(params.effectiveCapabilities)
      ? params.effectiveCapabilities.filter((p) => typeof p === 'string')
      : [];
    const denied = Array.isArray(params.deniedCapabilities)
      ? params.deniedCapabilities.filter((p) => typeof p === 'string')
      : [];

    const contradictory = effective.filter((p) => denied.includes(p));
    if (contradictory.length > 0) {
      throw new McplRpcError(
        -32602,
        'Malformed policy: capability appears in both effectiveCapabilities and deniedCapabilities',
        { capabilities: contradictory },
      );
    }

    const notes: string[] = [];
    if (params.effectiveCapabilities === undefined) {
      // §5.4: effectiveCapabilities is the sole allowlist. Absent means the
      // empty allowlist, not "unchanged" and not "everything".
      notes.push(
        'No effectiveCapabilities in featureSets/update; treating the grant as empty (SPEC 0.5 §5.4).',
      );
    }

    const unrecognized = effective.filter(
      (p) => !p.includes('*') && !CAPABILITY_PATH_SET.has(p),
    );
    if (unrecognized.length > 0) {
      notes.push(`Ignoring capability paths outside the §6.2 vocabulary: ${unrecognized.join(', ')}`);
    }

    if (form === 'notification') {
      if (this.effective === null) {
        notes.push(
          'featureSets/update arrived as a Notification; a Notification cannot establish a ready state (SPEC 0.5 §6.7), so no capability is in force.',
        );
      } else {
        const previous = this.effective;
        const narrowed = previous.filter((entry) => effective.includes(entry));
        if (narrowed.length !== previous.length || effective.length !== previous.length) {
          notes.push(
            'featureSets/update arrived as a Notification; applied as a narrowing only — a Notification may not widen the grant (SPEC 0.5 §6.7).',
          );
        }
        this.effective = narrowed;
      }
    } else {
      this.effective = effective;
    }
    this.explicitlyDisabled = new Set(params.disabled ?? []);
    // An `enabled` list constrains only when it names something; an empty list
    // is read as "no selection", not as "disable everything".
    this.explicitlyEnabled =
      Array.isArray(params.enabled) && params.enabled.length > 0
        ? new Set(params.enabled)
        : null;

    const unavailableFeatures: UnavailableFeature[] = [];
    for (const name of Object.keys(this.declarations)) {
      const missing = this.missingFor(name);
      if (missing.length > 0) {
        unavailableFeatures.push({
          featureSet: name,
          missingCapabilities: missing,
          effect: 'disabled',
        });
      }
    }

    // §6.7: only the Request form can establish a ready state, and
    // `this.effective` is still null when a Notification arrived first.
    if (this.effective !== null) {
      const waiters = this.readyWaiters;
      this.readyWaiters = [];
      for (const resolve of waiters) resolve();
    }

    return {
      accepted: true,
      mode: unavailableFeatures.length > 0 ? 'degraded' : 'full',
      unavailableFeatures,
      notes,
    };
  }
}
