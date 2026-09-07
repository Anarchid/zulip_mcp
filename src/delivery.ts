/**
 * Delivery bookkeeping — what has already been forwarded, what the agent is
 * missing, and which channels it had open. Persisted so a restart can catch
 * up instead of starting from "now".
 *
 *   watermarks   channelId → highest message id forwarded to the host. The
 *                "since when" anchor for the reconnect sweep, for gap
 *                recovery after a Zulip event-queue expiry, and for
 *                `sinceLastSeen` history on channels/open. It never passes
 *                an id the host was offered but has not accepted (the
 *                undelivered floor, below), so a later acceptance cannot
 *                bury an earlier loss.
 *   missed       channelId → tally of ambient messages dropped because the
 *                host had the channel closed. Reported by `channel_missed`
 *                and carried on closed-channel push events so "reply without
 *                opening" is an informed choice.
 *   lastOpen     channels the host had open at last sight. The reconnect
 *                sweep delivers their full missed backscroll (the host will
 *                reopen them); every other watermarked channel gets only its
 *                mentions, with vicinity.
 *
 * File: `<stateDir>/<session>.delivery.json`, alongside the tool layer's
 * monitoring state. Best-effort: a failed save is logged, never fatal.
 */

import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import type { IncomingChannelMessage, TextContent } from '@animalabs/mcpl-core';

export interface MissedTally {
  /** Watermark at the moment the channel was closed (0 = none known). */
  anchorId: number;
  /** Highest id counted so far; the reconnect backfill resumes from here. */
  talliedThrough: number;
  messages: number;
  characters: number;
}

interface DeliveryFile {
  watermarks?: Record<string, number>;
  missed?: Record<string, Partial<MissedTally>>;
  lastOpen?: string[];
}

/** Why an offered message is not (yet) accepted. `queued` is on its way;
 *  `failed` outlived the transport retry; `rejected` the host refused. */
export type HeldStatus = 'queued' | 'failed' | 'rejected';

interface Held {
  status: HeldStatus;
  /** A live replay from history has been attempted once already. */
  replayed: boolean;
}

/** Ids held per channel before the newest ones stop being tracked. Far
 *  beyond any real outage; the floor (the lowest id) is what matters and
 *  the lowest ids are the ones kept. */
const HELD_CAP_PER_CHANNEL = 50_000;

export class DeliveryState {
  private watermarks = new Map<string, number>();
  private missed = new Map<string, MissedTally>();
  private lastOpen = new Set<string>();
  /**
   * The undelivered floor (in-memory). Every message id offered to the host
   * on a channel — queued for `channels/incoming`, or sent as `push/event` —
   * is held here until the host accepts it. The watermark never advances
   * past the lowest held id, whatever is accepted after it: a message the
   * host refused or a batch lost to a transport failure stays below the
   * watermark, so the next sweep, a gap recovery, or a live replay can offer
   * it again instead of a later acceptance burying it. Not persisted: the
   * persisted watermark is already below every held id, which is all a
   * restart needs.
   */
  private held = new Map<string, Map<number, Held>>();
  /** The highest id acceptance has reached per channel; the watermark is
   *  this, capped by the floor. Recomputed when a hold is released. */
  private reached = new Map<string, number>();

  constructor(
    private readonly stateDir: string | null,
    private readonly sessionId: string,
  ) {
    this.load();
  }

  private file(): string | null {
    return this.stateDir ? join(this.stateDir, `${this.sessionId}.delivery.json`) : null;
  }

  private load(): void {
    const path = this.file();
    if (!path || !existsSync(path)) return;
    try {
      const parsed = JSON.parse(readFileSync(path, 'utf-8')) as DeliveryFile;
      for (const [chan, id] of Object.entries(parsed.watermarks ?? {})) {
        if (typeof id === 'number' && Number.isFinite(id) && id > 0) this.watermarks.set(chan, id);
      }
      for (const [chan, t] of Object.entries(parsed.missed ?? {})) {
        this.missed.set(chan, {
          anchorId: num(t.anchorId),
          talliedThrough: num(t.talliedThrough ?? t.anchorId),
          messages: num(t.messages),
          characters: num(t.characters),
        });
      }
      for (const id of parsed.lastOpen ?? []) if (typeof id === 'string') this.lastOpen.add(id);
    } catch (err) {
      console.error('[zulip-mcp] Failed to load delivery state:', (err as Error).message);
    }
  }

  save(): void {
    const path = this.file();
    if (!path) return;
    try {
      mkdirSync(this.stateDir!, { recursive: true });
      const out: DeliveryFile = {
        watermarks: Object.fromEntries([...this.watermarks].sort((a, b) => a[0].localeCompare(b[0]))),
        missed: Object.fromEntries([...this.missed].sort((a, b) => a[0].localeCompare(b[0]))),
        lastOpen: [...this.lastOpen].sort(),
      };
      // tmp + rename: a crash mid-write must not leave a truncated file that
      // the next start reads as "no watermarks" and sweeps from nowhere.
      const tmp = `${path}.${process.pid}.tmp`;
      writeFileSync(tmp, JSON.stringify(out, null, 2) + '\n');
      renameSync(tmp, path);
    } catch (err) {
      console.error('[zulip-mcp] Failed to save delivery state:', (err as Error).message);
    }
  }

  // ── watermarks ──

  watermark(channelId: string): number | undefined {
    return this.watermarks.get(channelId);
  }

  /**
   * Advance (never retreat) the forwarded watermark towards `messageId`,
   * stopping below the undelivered floor. Returns true if it moved.
   */
  advance(channelId: string, messageId: number): boolean {
    const reached = Math.max(this.reached.get(channelId) ?? 0, this.watermarks.get(channelId) ?? 0, messageId);
    this.reached.set(channelId, reached);
    return this.settle(channelId);
  }

  /**
   * Everything at or below `messageId` has been delivered (a catch-up block
   * scanned and pushed the whole range): release the holds in it and
   * advance. Returns true if the watermark moved.
   */
  advanceThrough(channelId: string, messageId: number): boolean {
    const held = this.held.get(channelId);
    if (held) {
      for (const id of held.keys()) if (id <= messageId) held.delete(id);
      if (held.size === 0) this.held.delete(channelId);
    }
    return this.advance(channelId, messageId);
  }

  /** Apply the floor: watermark = highest reached, capped below the lowest held id. */
  private settle(channelId: string): boolean {
    const reached = this.reached.get(channelId);
    if (reached === undefined) return false;
    const floor = this.floor(channelId);
    const target = floor === undefined ? reached : Math.min(reached, floor - 1);
    const current = this.watermarks.get(channelId) ?? 0;
    if (target <= current) return false;
    this.watermarks.set(channelId, target);
    return true;
  }

  watermarkedChannels(): string[] {
    return [...this.watermarks.keys()];
  }

  // ── undelivered floor ──

  /** The message was offered to the host and is not accepted yet. An id at
   *  or below the watermark is already forwarded and is not held. */
  hold(channelId: string, messageId: number, status: HeldStatus = 'queued'): void {
    if (!Number.isFinite(messageId) || messageId <= 0) return;
    if (messageId <= (this.watermarks.get(channelId) ?? 0)) return;
    let held = this.held.get(channelId);
    if (!held) {
      held = new Map();
      this.held.set(channelId, held);
    }
    const existing = held.get(messageId);
    if (existing) {
      existing.status = status;
      return;
    }
    if (held.size >= HELD_CAP_PER_CHANNEL) {
      // Keep the lowest ids: the floor protects everything above it anyway.
      const highest = Math.max(...held.keys());
      if (messageId >= highest) {
        console.error(`[zulip-mcp] ${channelId}: undelivered floor holds ${held.size} ids; not tracking ${messageId}`);
        return;
      }
      held.delete(highest);
    }
    held.set(messageId, { status, replayed: false });
  }

  /** The host accepted the message. Returns true if the watermark moved. */
  release(channelId: string, messageId: number): boolean {
    const held = this.held.get(channelId);
    if (!held?.delete(messageId)) return false;
    if (held.size === 0) this.held.delete(channelId);
    return this.settle(channelId);
  }

  /** Drop every hold on a channel (nothing from it may reach the agent any more). */
  releaseAll(channelId: string): boolean {
    if (!this.held.delete(channelId)) return false;
    return this.settle(channelId);
  }

  /** The lowest id offered but not accepted, if any. */
  floor(channelId: string): number | undefined {
    const held = this.held.get(channelId);
    if (!held || held.size === 0) return undefined;
    return Math.min(...held.keys());
  }

  heldChannels(): string[] {
    return [...this.held.keys()];
  }

  /** Held ids on a channel, ascending; optionally only those a live replay may still try. */
  heldIds(channelId: string, opts: { replayable?: boolean } = {}): number[] {
    const held = this.held.get(channelId);
    if (!held) return [];
    return [...held.entries()]
      .filter(([, h]) => !opts.replayable || (h.status !== 'queued' && !h.replayed))
      .map(([id]) => id)
      .sort((a, b) => a - b);
  }

  /** A live replay of these ids has been attempted; only a sweep or gap recovery re-offers them after this. */
  markReplayed(channelId: string, ids: number[]): void {
    const held = this.held.get(channelId);
    if (!held) return;
    for (const id of ids) {
      const h = held.get(id);
      if (h) h.replayed = true;
    }
  }

  // ── open mirror ──

  wasOpen(channelId: string): boolean {
    return this.lastOpen.has(channelId);
  }

  lastOpenChannels(): string[] {
    return [...this.lastOpen];
  }

  /** Host opened the channel: it receives everything, so no tally applies. */
  markOpen(channelId: string): void {
    this.lastOpen.add(channelId);
    this.missed.delete(channelId);
  }

  /** Host closed the channel: start counting what it no longer sees. */
  markClosed(channelId: string): void {
    if (!this.lastOpen.delete(channelId)) return;
    const anchor = this.watermarks.get(channelId) ?? 0;
    this.missed.set(channelId, { anchorId: anchor, talliedThrough: anchor, messages: 0, characters: 0 });
  }

  // ── missed tallies ──

  tally(channelId: string): MissedTally | undefined {
    return this.missed.get(channelId);
  }

  talliedChannels(): string[] {
    return [...this.missed.keys()];
  }

  /** Count one dropped ambient message. No-op unless the channel is tracked. */
  countMissed(channelId: string, m: { id: number; text: string }): boolean {
    const t = this.missed.get(channelId);
    if (!t) return false;
    t.messages += 1;
    t.characters += m.text.length;
    if (m.id > t.talliedThrough) t.talliedThrough = m.id;
    return true;
  }

  /** Fold a batch of fetched messages into the tally (reconnect backfill). */
  backfillMissed(channelId: string, ambient: { id: number; text: string }[], scannedThrough: number): void {
    const t = this.missed.get(channelId);
    if (!t) return;
    t.messages += ambient.length;
    t.characters += ambient.reduce((n, m) => n + m.text.length, 0);
    if (scannedThrough > t.talliedThrough) t.talliedThrough = scannedThrough;
  }
}

function num(v: unknown): number {
  return typeof v === 'number' && Number.isFinite(v) ? v : 0;
}

// ── Pure selection/rendering helpers for the reconnect sweep ──

/** The fields of an incoming-shaped message the sweep reads. */
export interface MissedView {
  id: number;
  mentioned: boolean;
  topic: string;
  authorName: string;
  text: string;
  timestamp: Date;
  attachmentNames: string[];
}

export function viewOf(m: IncomingChannelMessage): MissedView {
  const meta = (typeof m.metadata === 'object' && m.metadata !== null ? m.metadata : {}) as Record<string, unknown>;
  const attachments = Array.isArray(meta.attachments) ? (meta.attachments as { name?: string }[]) : [];
  const text = m.content
    .filter((c): c is TextContent => c.type === 'text')
    .map((c) => c.text)
    .filter((t) => !t.startsWith('[attachments:'))
    .join('\n');
  return {
    id: Number(m.messageId),
    mentioned: meta.mentioned === true || meta.isDM === true,
    topic: typeof meta.topic === 'string' ? meta.topic : (m.threadId ?? ''),
    authorName: m.author.name,
    text,
    timestamp: new Date(m.timestamp),
    attachmentNames: attachments.map((a) => a.name ?? 'attachment'),
  };
}

/** Messages around each mention, ±`vicinity` by count (robust to channel pace). */
export function selectMissed<T extends { mentioned: boolean }>(
  msgs: T[],
  opts: { keepAll: boolean; vicinity: number },
): T[] {
  if (opts.keepAll) return msgs;
  const keep = new Set<number>();
  for (let i = 0; i < msgs.length; i++) {
    if (!msgs[i].mentioned) continue;
    for (let j = Math.max(0, i - opts.vicinity); j <= Math.min(msgs.length - 1, i + opts.vicinity); j++) keep.add(j);
  }
  return [...keep].sort((a, b) => a - b).map((i) => msgs[i]);
}

/** Default size cap on one `<missed>` block, in characters (~10k tokens). */
export const DEFAULT_MISSED_BLOCK_MAX_CHARS = 40_000;

export interface MissedBlockOptions {
  streamName: string;
  channelId: string;
  reason: 'backscroll' | 'mention';
  /** Number of actual mentions (for the mention reason); total lines otherwise. */
  count: number;
  formatTime: (d: Date) => string;
  /** Character budget for the block; the OLDEST lines are elided first. */
  maxChars?: number;
  /** The fetch hit its ceiling: there is more beyond the newest line shown. */
  moreBeyond?: boolean;
  /** Newest id the fetch scanned (may be past the newest line kept). */
  newestScannedId?: number;
}

/** The channel a `<missed>` note points the agent at — `fetch_history`
 *  reads DM conversations by their channel id, so the id is quoted as is. */
function historyPointer(channelId: string, cursor: 'before' | 'after', id: number | undefined): string {
  return `fetch_history(channel="${channelId}", ${cursor}=${id})`;
}

/**
 * The `<missed>` transcript block delivered after downtime. Each line leads
 * with the message id so the agent can fetch_around(id) for more context,
 * and mention lines are flagged so they stand out from vicinity.
 *
 * `maxChars` is a hard cap on the whole serialized block, header and notes
 * included: the oldest lines go first, replaced by one line naming the
 * elided id range so fetch_history can page into it; when the single
 * newest line is itself over budget it is cut and says so.
 */
export function renderMissedBlock(msgs: MissedView[], opts: MissedBlockOptions): string {
  const render = (m: MissedView): string => {
    const ts = opts.formatTime(m.timestamp);
    const att = m.attachmentNames.length > 0 ? ` [attachments: ${m.attachmentNames.join(', ')}]` : '';
    const mark = m.mentioned ? ' (mention)' : '';
    return `[${ts ? `${ts} ` : ''}id=${m.id}] [${m.topic}] ${m.authorName}${mark}: ${m.text}${att}`;
  };
  const lines = msgs.map(render);
  const budget = opts.maxChars ?? Infinity;
  const newestScanned = opts.newestScannedId ?? msgs[msgs.length - 1]?.id;

  const assemble = (elided: number, cut: string | null): string => {
    const attrs = [`stream="#${opts.streamName}"`, `channelId="${opts.channelId}"`, `count="${opts.count}"`];
    if (opts.reason === 'mention') attrs.push(`lines="${msgs.length}"`);
    attrs.push(`reason="${opts.reason}"`);
    const head: string[] = [];
    const tail: string[] = [];
    if (elided > 0) {
      head.push(
        `[${elided} earlier line(s) elided (ids ${msgs[0].id}–${msgs[elided - 1].id}) to fit the catch-up budget — ` +
          `${historyPointer(opts.channelId, 'before', msgs[elided].id)} pages into them]`,
      );
      attrs.push(`elided="${elided}"`);
    }
    if (opts.moreBeyond) {
      tail.push(`[the catch-up ceiling was reached; newer messages exist — ${historyPointer(opts.channelId, 'after', newestScanned)} continues]`);
      attrs.push('truncated="true"');
    }
    const body = cut !== null ? [cut] : lines.slice(elided);
    return [`<missed ${attrs.join(' ')}>`, ...head, ...body, ...tail, '</missed>'].join('\n');
  };

  // Elide the oldest lines until the whole block fits; always keep the newest.
  let elided = 0;
  let block = assemble(elided, null);
  while (block.length > budget && elided < lines.length - 1) {
    elided++;
    block = assemble(elided, null);
  }
  if (block.length > budget && lines.length > 0) {
    // One line is over budget by itself: cut it to what fits, and say so.
    const last = msgs[msgs.length - 1];
    const marker = ` … [line cut to fit the catch-up budget — fetch_around(${last.id}) has the whole message]`;
    const room = budget - (assemble(elided, '').length + marker.length);
    const full = lines[lines.length - 1];
    block = assemble(elided, `${full.slice(0, Math.max(0, room))}${marker}`);
  }
  return block;
}
