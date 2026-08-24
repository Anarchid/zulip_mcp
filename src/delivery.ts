/**
 * Delivery bookkeeping — what has already been forwarded, what the agent is
 * missing, and which channels it had open. Persisted so a restart can catch
 * up instead of starting from "now".
 *
 *   watermarks   channelId → highest message id forwarded to the host. The
 *                "since when" anchor for the reconnect sweep, for gap
 *                recovery after a Zulip event-queue expiry, and for
 *                `sinceLastSeen` history on channels/open.
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

import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
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

export class DeliveryState {
  private watermarks = new Map<string, number>();
  private missed = new Map<string, MissedTally>();
  private lastOpen = new Set<string>();

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
      writeFileSync(path, JSON.stringify(out, null, 2) + '\n');
    } catch (err) {
      console.error('[zulip-mcp] Failed to save delivery state:', (err as Error).message);
    }
  }

  // ── watermarks ──

  watermark(channelId: string): number | undefined {
    return this.watermarks.get(channelId);
  }

  /** Advance (never retreat) the forwarded watermark. Returns true if it moved. */
  advance(channelId: string, messageId: number): boolean {
    const current = this.watermarks.get(channelId) ?? 0;
    if (messageId <= current) return false;
    this.watermarks.set(channelId, messageId);
    return true;
  }

  watermarkedChannels(): string[] {
    return [...this.watermarks.keys()];
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

export interface MissedBlockOptions {
  streamName: string;
  channelId: string;
  reason: 'backscroll' | 'mention';
  /** Number of actual mentions (for the mention reason); total lines otherwise. */
  count: number;
  formatTime: (d: Date) => string;
}

/**
 * The `<missed>` transcript block delivered after downtime. Each line leads
 * with the message id so the agent can fetch_around(id) for more context,
 * and mention lines are flagged so they stand out from vicinity.
 */
export function renderMissedBlock(msgs: MissedView[], opts: MissedBlockOptions): string {
  const attrs = [
    `stream="#${opts.streamName}"`,
    `channelId="${opts.channelId}"`,
    `count="${opts.count}"`,
  ];
  if (opts.reason === 'mention') attrs.push(`lines="${msgs.length}"`);
  attrs.push(`reason="${opts.reason}"`);
  const lines = msgs.map((m) => {
    const ts = opts.formatTime(m.timestamp);
    const att = m.attachmentNames.length > 0 ? ` [attachments: ${m.attachmentNames.join(', ')}]` : '';
    const mark = m.mentioned ? ' (mention)' : '';
    return `[${ts ? `${ts} ` : ''}id=${m.id}] [${m.topic}] ${m.authorName}${mark}: ${m.text}${att}`;
  });
  return [`<missed ${attrs.join(' ')}>`, ...lines, '</missed>'].join('\n');
}
