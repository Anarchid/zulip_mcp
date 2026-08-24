/**
 * The filters plane — which streams and people can reach the agent, which
 * streams are muted, and (for the reactions surface) which channels show
 * live reactions and which reaction emojis are withheld from the model.
 *
 * One JSON file is the desired state. It always exists once the server has
 * started: `ZULIP_FILTERS_FILE` when set, else `<state dir>/<session>.filters.json`,
 * seeded from the environment on first materialization. From then on edits
 * — by hand, by ops tooling, or by the agent through `filters_update` /
 * `mute_channel` — are hot-applied within seconds; nothing here needs a
 * restart, so an allowlist change can never require one (discord-mcpl #26).
 *
 * Semantics:
 *   streams unset/empty      → every stream the bot can see
 *   dmUsers unset/empty      → anyone may DM the bot
 *   mutedStreams             → nothing from these streams reaches the agent,
 *                              mentions included, and nothing is tallied
 *   reactionChannels         → channels opted into live reaction visibility
 *   suppressedReactionEmojis → reaction markers withheld from every
 *                              model-visible surface (operator-owned; the
 *                              agent's tools cannot carry this key)
 *
 * File schema:
 *   {
 *     "streams": ["general", "dev"],
 *     "dmUsers": ["42", "ann@example.com"],
 *     "mutedStreams": ["random"],
 *     "reactionChannels": ["zulip:general"],
 *     "suppressedReactionEmojis": ["biohazard"]
 *   }
 *
 * Failure posture: an unparseable or vanished file keeps the last-known-good
 * filters in force (never fail-open) and marks the plane stale; a broken
 * file at startup runs the env seed for the allowlists and withholds ALL
 * reactions until the file is repaired. `filters_get` reports the plane's
 * desired-vs-effective state so disk ≠ process is always witnessed.
 */

import { existsSync, mkdirSync, readFileSync, renameSync, statSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import { createHash } from 'node:crypto';

export interface ZulipFilters {
  /** Allowed stream names. Unset = every visible stream. */
  streams?: string[];
  /** Allowed DM senders, as user ids or (lower-cased) emails. Unset = anyone. */
  dmUsers?: string[];
  /** Streams dropped entirely. */
  mutedStreams?: string[];
  /** Channel ids opted into live reaction delivery. */
  reactionChannels?: string[];
  /** Reaction emoji names withheld from the model. An explicit `[]` is preserved. */
  suppressedReactionEmojis?: string[];
}

function uniqueStrings(values: string[] | undefined, map: (s: string) => string = (s) => s): string[] {
  return [...new Set((values ?? []).map((v) => map(String(v).trim())).filter(Boolean))];
}

function normalizeEmail(s: string): string {
  return s.includes('@') ? s.toLowerCase() : s;
}

function stripHash(s: string): string {
  return s.replace(/^#/, '');
}

/** Normalize a reaction emoji name for matching: strip VS-16 and colons. */
export function normalizeReactionEmoji(emoji: string): string {
  return emoji.replace(/️/g, '').trim().replace(/^:|:$/g, '').toLowerCase();
}

/** Drop empty lists so "unset" and "empty" stay one state (= unrestricted),
 *  except suppressedReactionEmojis, where an explicit empty list is a
 *  deliberate operator choice and must survive. */
export function normalizeFilters(f: ZulipFilters): ZulipFilters {
  const out: ZulipFilters = {};
  const streams = uniqueStrings(f.streams, stripHash);
  if (streams.length) out.streams = streams;
  const dmUsers = uniqueStrings(f.dmUsers, normalizeEmail);
  if (dmUsers.length) out.dmUsers = dmUsers;
  const muted = uniqueStrings(f.mutedStreams, stripHash);
  if (muted.length) out.mutedStreams = muted;
  const reactionChannels = uniqueStrings(f.reactionChannels);
  if (reactionChannels.length) out.reactionChannels = reactionChannels;
  if (f.suppressedReactionEmojis !== undefined) {
    out.suppressedReactionEmojis = uniqueStrings(f.suppressedReactionEmojis, normalizeReactionEmoji);
  }
  return out;
}

function splitList(raw: string | undefined): string[] {
  return (raw ?? '').split(',').map((s) => s.trim()).filter(Boolean);
}

/** The environment seed: ZULIP_STREAMS, ZULIP_DM_USERS, ZULIP_MUTED_STREAMS,
 *  ZULIP_SUPPRESSED_REACTIONS_BASELINE (host-injected; written durably on
 *  first materialization so the file becomes the operator-ownable record). */
export function parseFiltersFromEnv(env: NodeJS.ProcessEnv = process.env): ZulipFilters {
  const f: ZulipFilters = {
    streams: splitList(env.ZULIP_STREAMS),
    dmUsers: splitList(env.ZULIP_DM_USERS),
    mutedStreams: splitList(env.ZULIP_MUTED_STREAMS),
  };
  const baseline = splitList(env.ZULIP_SUPPRESSED_REACTIONS_BASELINE);
  if (baseline.length) f.suppressedReactionEmojis = baseline;
  return normalizeFilters(f);
}

/** Load + validate the filters file. Returns null when the file is missing,
 *  unparseable, or carries a wrong-typed safety field — callers keep the
 *  previous filters (fail-safe, never fail-open). The allowlist keys
 *  tolerate loose shapes (a wrong-typed one fails toward "unrestricted",
 *  visible in filters_get); suppressedReactionEmojis does not, because a
 *  wrong-typed value degrading to "absent" would silently drop protection. */
export function loadFiltersFile(path: string): ZulipFilters | null {
  try {
    const raw: unknown = JSON.parse(readFileSync(path, 'utf-8'));
    if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) return null;
    const r = raw as Record<string, unknown>;
    const out: ZulipFilters = {};
    for (const key of ['streams', 'dmUsers', 'mutedStreams', 'reactionChannels'] as const) {
      if (Array.isArray(r[key])) out[key] = (r[key] as unknown[]).map(String);
    }
    if ('suppressedReactionEmojis' in r) {
      const v = r.suppressedReactionEmojis;
      if (!Array.isArray(v) || v.some((x) => typeof x !== 'string')) return null;
      out.suppressedReactionEmojis = v as string[];
    }
    return normalizeFilters(out);
  } catch {
    return null;
  }
}

/** Atomic write (tmp + rename) so the poller never reads a half-written file. */
export function saveFiltersFile(path: string, filters: ZulipFilters): void {
  mkdirSync(dirname(path), { recursive: true });
  const tmp = `${path}.tmp`;
  writeFileSync(tmp, JSON.stringify(normalizeFilters(filters), null, 2) + '\n');
  renameSync(tmp, path);
}

export function filtersFileMtime(path: string): number | null {
  try {
    return statSync(path).mtimeMs;
  } catch {
    return null;
  }
}

/** Poll-cycle interpreter for the file's presence/mtime, so the poller can
 *  tell an atomic-rename blink from a real deletion and force a reload when
 *  a vanished file reappears with a preserved mtime. One missing observation
 *  is grace (a non-atomic editor mid-replace); persistent absence is a real
 *  state the plane must witness. */
export class FiltersFilePollTracker {
  private lastMtime: number | null;
  private missedPolls = 0;

  constructor(initialMtime: number | null) {
    this.lastMtime = initialMtime;
  }

  observe(mtime: number | null): 'none' | 'missing' | 'reload' {
    if (mtime === null) {
      this.missedPolls++;
      return this.missedPolls >= 2 ? 'missing' : 'none';
    }
    const reappeared = this.missedPolls > 0;
    this.missedPolls = 0;
    if (!reappeared && mtime === this.lastMtime) return 'none';
    this.lastMtime = mtime;
    return 'reload';
  }
}

function stableStringify(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
  if (value && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([k, v]) => `${JSON.stringify(k)}:${stableStringify(v)}`);
    return `{${entries.join(',')}}`;
  }
  return JSON.stringify(value);
}

function sha256(s: string): string {
  return 'sha256:' + createHash('sha256').update(s).digest('hex');
}

export interface FiltersPlaneStatus {
  /** live: the file parsed and is applied. stale: the file is unreadable or
   *  gone and the last-known-good filters are enforced (process-lifetime).
   *  unavailable: broken with no good parse ever this process — allowlists
   *  run on the env seed, reactions are withheld. */
  status: 'live' | 'stale' | 'unavailable';
  desiredState: 'ok' | 'invalid' | 'missing';
  path: string;
  /** Digest of the normalized effective filters — the whole plane. */
  effectiveDigest: string | null;
  loadedAt?: string;
  staleSince?: string;
}

export interface ReactionSuppressionStatus {
  status: 'not-configured' | 'configured-empty' | 'active' | 'stale' | 'unavailable';
  protectionActive: boolean;
  effectiveCount: number;
  /** Redacted by construction — a digest, never the entries. */
  effectiveDigest: string | null;
  suppressingAllReactions?: true;
  source: 'file' | 'none';
}

/**
 * The plane: desired state on disk, effective state in process, the
 * poller that keeps them aligned, and the status that says when they are
 * not. Consumers read `current()` (the effective filters) and subscribe to
 * `onChange` to react to reloads.
 */
export class FiltersPlane {
  private effective: ZulipFilters;
  private digest: string | null = null;
  private loadedAt: string | null = null;
  private broken: { since: string; desired: 'invalid' | 'missing' } | null = null;
  private everParsed = false;
  private listeners: ((next: ZulipFilters, prev: ZulipFilters) => void)[] = [];
  private poll: ReturnType<typeof setInterval> | null = null;
  private tracker: FiltersFilePollTracker | null = null;

  constructor(
    readonly path: string,
    private readonly env: NodeJS.ProcessEnv = process.env,
    private readonly options: { pollMs?: number } = {},
  ) {
    this.effective = parseFiltersFromEnv(env);
  }

  /** Seed or load the file, then start watching it. Idempotent. */
  start(): void {
    if (this.poll) return;
    if (existsSync(this.path)) {
      const fromFile = loadFiltersFile(this.path);
      if (fromFile) {
        this.apply(fromFile, true);
        console.error(`[zulip-mcp] filters loaded from ${this.path}`);
      } else {
        this.broken = { since: new Date().toISOString(), desired: 'invalid' };
        console.error(
          `[zulip-mcp] filters file ${this.path} exists but cannot be parsed — NOT overwriting it. ` +
            'Running on the env seed; reaction suppression fails closed until the file is repaired.',
        );
      }
    } else {
      try {
        saveFiltersFile(this.path, this.effective);
        this.apply(this.effective, true);
        console.error(`[zulip-mcp] filters file seeded from env -> ${this.path}`);
      } catch (err) {
        this.broken = { since: new Date().toISOString(), desired: 'missing' };
        console.error(`[zulip-mcp] could not seed filters file ${this.path}:`, (err as Error).message);
      }
    }
    this.tracker = new FiltersFilePollTracker(filtersFileMtime(this.path));
    this.poll = setInterval(() => this.tick(), this.options.pollMs ?? 3000);
    (this.poll as { unref?: () => void }).unref?.();
  }

  stop(): void {
    if (this.poll) clearInterval(this.poll);
    this.poll = null;
  }

  /** One poll cycle. Exposed for tests; the interval calls it. */
  tick(): void {
    if (!this.tracker) return;
    const action = this.tracker.observe(filtersFileMtime(this.path));
    if (action === 'none') return;
    if (action === 'missing') {
      if (this.markBroken('missing')) {
        console.error(`[zulip-mcp] filters file ${this.path} is MISSING — last-known-good filters stay in force until it reappears`);
      }
      return;
    }
    const next = loadFiltersFile(this.path);
    if (!next) {
      if (this.markBroken('invalid')) {
        console.error(`[zulip-mcp] filters file changed but is unparseable — keeping previous filters (${this.path})`);
      }
      return;
    }
    if (this.apply(next, false)) console.error(`[zulip-mcp] filters hot-reloaded from ${this.path}`);
  }

  /** The effective filters (normalized). */
  current(): ZulipFilters {
    return this.effective;
  }

  onChange(listener: (next: ZulipFilters, prev: ZulipFilters) => void): () => void {
    this.listeners.push(listener);
    return () => {
      this.listeners = this.listeners.filter((l) => l !== listener);
    };
  }

  /**
   * Change the desired state: write the file atomically, apply, notify.
   * Refused while the plane is broken — an operator repairs the file (hot
   * reload applies it within seconds) and the update can be retried; the
   * agent must never overwrite a file it could not read.
   */
  update(edit: (current: ZulipFilters) => ZulipFilters): { ok: true; filters: ZulipFilters } | { ok: false; reason: string } {
    if (this.broken) {
      return {
        ok: false,
        reason: `the filters file ${this.path} is ${this.broken.desired} on disk; repair it (hot reload applies within seconds) and retry`,
      };
    }
    const next = normalizeFilters(edit(structuredClone(this.effective)));
    try {
      saveFiltersFile(this.path, next);
    } catch (err) {
      return { ok: false, reason: `could not write ${this.path}: ${(err as Error).message}` };
    }
    // Reset the tracker to our own write so the poller does not re-apply it.
    this.tracker = new FiltersFilePollTracker(filtersFileMtime(this.path));
    this.apply(next, false);
    return { ok: true, filters: this.effective };
  }

  // ── queries ──

  streamAllowed(streamName: string): boolean {
    const allow = this.effective.streams;
    return !allow || allow.includes(streamName);
  }

  streamMuted(streamName: string): boolean {
    return this.effective.mutedStreams?.includes(streamName) ?? false;
  }

  dmAllowed(sender: { id: number; email: string }): boolean {
    const allow = this.effective.dmUsers;
    return !allow || allow.includes(String(sender.id)) || allow.includes(sender.email.toLowerCase());
  }

  reactionsVisible(channelId: string): boolean {
    return this.effective.reactionChannels?.includes(channelId) ?? false;
  }

  /** The withhold-everything posture: broken with no usable set ever. */
  suppressAllReactions(): boolean {
    if (!this.broken) return false;
    if (!this.everParsed) return true;
    const list = this.effective.suppressedReactionEmojis;
    return list !== undefined && list.length === 0;
  }

  reactionSuppressed(emojiName: string): boolean {
    const list = this.effective.suppressedReactionEmojis;
    if (!list || list.length === 0) return false;
    return list.includes(normalizeReactionEmoji(emojiName));
  }

  planeStatus(): FiltersPlaneStatus {
    const loaded = this.loadedAt ? { loadedAt: this.loadedAt } : {};
    if (this.broken) {
      return {
        status: this.everParsed ? 'stale' : 'unavailable',
        desiredState: this.broken.desired,
        path: this.path,
        effectiveDigest: this.digest,
        staleSince: this.broken.since,
        ...loaded,
      };
    }
    return { status: 'live', desiredState: 'ok', path: this.path, effectiveDigest: this.digest, ...loaded };
  }

  suppressionStatus(): ReactionSuppressionStatus {
    if (this.suppressAllReactions()) {
      return { status: 'unavailable', protectionActive: true, effectiveCount: 0, effectiveDigest: null, suppressingAllReactions: true, source: this.everParsed ? 'file' : 'none' };
    }
    const list = this.effective.suppressedReactionEmojis;
    if (list === undefined) {
      return { status: 'not-configured', protectionActive: false, effectiveCount: 0, effectiveDigest: null, source: 'none' };
    }
    const n = list.length;
    const digest = n > 0 ? sha256(JSON.stringify([...list].sort())) : null;
    if (this.broken) {
      return { status: 'stale', protectionActive: true, effectiveCount: n, effectiveDigest: digest, source: 'file' };
    }
    return { status: n > 0 ? 'active' : 'configured-empty', protectionActive: n > 0, effectiveCount: n, effectiveDigest: digest, source: 'file' };
  }

  // ── internals ──

  private apply(filters: ZulipFilters, initial: boolean): boolean {
    const prev = this.effective;
    const normalized = normalizeFilters(filters);
    const digest = sha256(stableStringify(normalized));
    const changed = digest !== this.digest;
    this.broken = null;
    this.everParsed = true;
    if (changed) this.loadedAt = new Date().toISOString();
    this.digest = digest;
    this.effective = normalized;
    if (changed && !initial) {
      for (const l of this.listeners) {
        try {
          l(normalized, prev);
        } catch (err) {
          console.error('[zulip-mcp] filters listener failed:', (err as Error).message);
        }
      }
    }
    return changed;
  }

  private markBroken(desired: 'invalid' | 'missing'): boolean {
    if (this.broken) {
      this.broken.desired = desired;
      return false;
    }
    this.broken = { since: new Date().toISOString(), desired };
    return true;
  }
}
