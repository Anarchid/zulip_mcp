/**
 * Zulip message history — one fetch shape for every reader (context
 * injection, channel-open backscroll, the reconnect sweep, and the
 * fetch_history / fetch_around tools), and one conversion from a Zulip
 * message to the MCPL incoming shape so history and live delivery render
 * identically.
 *
 * Zulip message ids are realm-global and monotonic, which is what makes them
 * usable as cursors: `after`/`before` are exclusive id bounds, and a
 * watermark is just the highest id already forwarded.
 */

import type { ChannelDescriptor, IncomingChannelMessage, TextContent } from '@animalabs/mcpl-core';
import { CHAT_TAGS } from '@animalabs/mcpl-core';
import { cleanContent, extractZulipAttachments, type AttachmentRef } from './content.js';

/** A Zulip message as `GET /messages` and the event queue both deliver it. */
export interface ZulipRawMessage {
  id: number;
  sender_id: number;
  sender_full_name: string;
  sender_email: string;
  /** Stream name for stream messages; recipient list for DMs. */
  display_recipient: string | { email: string; full_name: string; id: number }[];
  subject: string;
  content: string;
  timestamp: number;
  type: string;
  stream_id?: number;
  /** The requesting user's flags — `mentioned` is Zulip's server-side verdict. */
  flags?: string[];
  reactions?: { emoji_name: string; emoji_code?: string; reaction_type?: string; user_id: number }[];
}

/** One emoji reaction bucket on a message. */
export interface ReactionSummary {
  /** Zulip emoji name, e.g. 'thumbs_up' — the `:name:` form is what add_reaction takes. */
  name: string;
  count: number;
  /** Who reacted. */
  userIds: number[];
}

export function summarizeReactions(raw: ZulipRawMessage['reactions']): ReactionSummary[] {
  const buckets = new Map<string, ReactionSummary>();
  for (const r of raw ?? []) {
    if (!r || typeof r.emoji_name !== 'string') continue;
    let b = buckets.get(r.emoji_name);
    if (!b) {
      b = { name: r.emoji_name, count: 0, userIds: [] };
      buckets.set(r.emoji_name, b);
    }
    b.count += 1;
    b.userIds.push(r.user_id);
  }
  return [...buckets.values()];
}

/** ` [reactions: :thumbs_up: x2 (incl. me), :eyes: x1]`, or '' when none. */
export function renderReactions(reactions: ReactionSummary[], selfUserId: number | null): string {
  if (reactions.length === 0) return '';
  const parts = reactions.map((r) => `:${r.name}: x${r.count}${selfUserId !== null && r.userIds.includes(selfUserId) ? ' (incl. me)' : ''}`);
  return ` [reactions: ${parts.join(', ')}]`;
}

export interface ZulipRecipient {
  id: number;
  full_name: string;
  email: string;
}

/** The normalized, model-facing view of one message. */
export interface ZulipMessage {
  id: number;
  streamName: string | null;
  topic: string;
  isDm: boolean;
  /** Every party to a DM (the bot included); empty for stream messages. */
  recipients: ZulipRecipient[];
  authorId: number;
  authorName: string;
  authorEmail: string;
  timestamp: Date;
  /** Raw markdown (history is fetched with apply_markdown=false). */
  rawContent: string;
  cleanContent: string;
  mentioned: boolean;
  wildcardMentioned: boolean;
  attachments: AttachmentRef[];
  /** Reactions currently on the message (history fetches carry them; events do not). */
  reactions: ReactionSummary[];
}

export interface HistoryQuery {
  /** Stream to read; omit for a cross-stream window (fetch_around). */
  streamName?: string;
  topic?: string;
  /** The other parties of a DM conversation (user ids); reads that conversation instead of a stream. */
  dmUserIds?: number[];
  limit: number;
  /** Exclusive upper id bound — page backwards from here. */
  before?: number;
  /** Exclusive lower id bound — everything newer than here. */
  after?: number;
}

export interface HistoryPage {
  /** Oldest first. */
  messages: ZulipMessage[];
  /** Zulip's own bound reports: whether the fetch reached the end of history in each direction. */
  foundNewest: boolean;
  foundOldest: boolean;
}

/** Zulip caps a single GET /messages at 5000 rows. */
export const ZULIP_MAX_PAGE = 5000;

/**
 * zulip-js resolves API errors as values (`{ result: 'error', code, msg }`)
 * rather than rejecting — an unknown stream would otherwise read as "no
 * messages". Turn them into thrown errors with Zulip's own wording.
 */
export function assertApiSuccess(result: unknown, what: string): void {
  const r = result as { result?: string; msg?: string; code?: string } | null | undefined;
  if (r && r.result === 'error') {
    throw new Error(`Zulip refused ${what}: ${r.msg ?? r.code ?? 'unknown error'}`);
  }
}

function narrowFor(q: Pick<HistoryQuery, 'streamName' | 'topic' | 'dmUserIds'>): unknown[] {
  const narrow: unknown[] = [];
  if (q.dmUserIds && q.dmUserIds.length > 0) {
    narrow.push(['dm', q.dmUserIds]);
    return narrow;
  }
  if (q.streamName) narrow.push(['stream', q.streamName]);
  if (q.topic) narrow.push(['topic', q.topic]);
  return narrow;
}

// ── Direct-message channels ──
//
// A DM conversation is identified by its participants other than the bot,
// as sorted user ids: `zulip:dm:42` for a 1:1, `zulip:dm:7+42` for a group.
// Ids, not names, because names change and ids are what the send API takes.

export const DM_CHANNEL_PREFIX = 'zulip:dm:';

/** The other parties of a DM (the bot excluded), sorted by id. */
export function dmCounterparts(recipients: ZulipRecipient[], selfUserId: number | null): ZulipRecipient[] {
  const others = recipients.filter((r) => selfUserId === null || r.id !== selfUserId);
  // A DM to oneself has no counterpart; it is its own conversation.
  const parties = others.length > 0 ? others : recipients;
  return [...parties].sort((a, b) => a.id - b.id);
}

export function dmChannelIdFor(userIds: number[]): string {
  return DM_CHANNEL_PREFIX + [...userIds].sort((a, b) => a - b).join('+');
}

/** `zulip:dm:7+42` → [7, 42]; null for anything else. */
export function parseDmChannelId(channelId: string): number[] | null {
  if (!channelId.startsWith(DM_CHANNEL_PREFIX)) return null;
  const ids = channelId.slice(DM_CHANNEL_PREFIX.length).split('+').map((s) => Number(s));
  if (ids.length === 0 || ids.some((n) => !Number.isInteger(n) || n <= 0)) return null;
  return ids;
}

export function isDmChannelId(channelId: string): boolean {
  return parseDmChannelId(channelId) !== null;
}

/** The channel a message belongs to: its stream, or its DM conversation. */
export function channelIdOf(m: ZulipMessage, selfUserId: number | null): string {
  if (m.isDm) return dmChannelIdFor(dmCounterparts(m.recipients, selfUserId).map((r) => r.id));
  return `zulip:${m.streamName ?? ''}`;
}

/** Bot accounts in Zulip carry a `-bot@` local part; the event envelope has no is_bot. */
export function looksLikeBotEmail(email: string): boolean {
  return /-bot@/.test(email);
}

export function normalizeMessage(raw: ZulipRawMessage): ZulipMessage {
  const flags = raw.flags ?? [];
  const isDm = raw.type === 'private';
  const recipients: ZulipRecipient[] = isDm && Array.isArray(raw.display_recipient)
    ? raw.display_recipient.map((r) => ({ id: r.id, full_name: r.full_name, email: r.email }))
    : [];
  return {
    id: raw.id,
    streamName: !isDm && typeof raw.display_recipient === 'string' ? raw.display_recipient : null,
    topic: raw.subject ?? '',
    isDm,
    recipients,
    authorId: raw.sender_id,
    authorName: raw.sender_full_name,
    authorEmail: raw.sender_email,
    timestamp: new Date(raw.timestamp * 1000),
    rawContent: raw.content,
    cleanContent: cleanContent(raw.content),
    mentioned: flags.includes('mentioned'),
    wildcardMentioned: flags.includes('wildcard_mentioned'),
    attachments: extractZulipAttachments(raw.content),
    reactions: summarizeReactions(raw.reactions),
  };
}

/**
 * Fetch one page of history, oldest first. `before` and `after` are
 * exclusive; with neither the page is the newest `limit` messages.
 */
export async function fetchHistory(zulipClient: any, q: HistoryQuery): Promise<HistoryPage> {
  const limit = Math.max(0, Math.min(ZULIP_MAX_PAGE, Math.floor(q.limit)));
  if (limit === 0) return { messages: [], foundNewest: true, foundOldest: false };

  let anchor: number | string;
  let numBefore: number;
  let numAfter: number;
  if (q.after !== undefined) {
    anchor = q.after;
    numBefore = 0;
    numAfter = limit;
  } else if (q.before !== undefined) {
    anchor = q.before;
    numBefore = limit;
    numAfter = 0;
  } else {
    anchor = 'newest';
    numBefore = limit;
    numAfter = 0;
  }

  const result = await zulipClient.messages.retrieve({
    anchor,
    num_before: numBefore,
    num_after: numAfter,
    narrow: narrowFor(q),
    // Raw markdown so attachment refs stay textual, matching the event path.
    apply_markdown: false,
    // Exclusive cursors; 'newest' already excludes nothing real.
    include_anchor: q.after === undefined && q.before === undefined,
  });
  assertApiSuccess(result, `history of ${q.streamName ?? 'all'}`);

  const messages: ZulipMessage[] = ((result?.messages ?? []) as ZulipRawMessage[])
    .map(normalizeMessage)
    .sort((a, b) => a.id - b.id);
  return {
    messages,
    foundNewest: result?.found_newest === true,
    foundOldest: result?.found_oldest === true,
  };
}

/**
 * A window centred on one message: the message itself plus roughly half the
 * window on either side, WITHIN the anchor's own conversation (its stream
 * and topic, or its DM). Message ids are realm-global, so without a narrow
 * Zulip would answer with the realm-wide timeline — neighbours from
 * unrelated streams, which is not "surrounding context". The anchor is
 * looked up first to learn where it lives; a message that cannot be read
 * fails loudly rather than silently widening to everything.
 */
export async function fetchAround(zulipClient: any, messageId: number, limit: number): Promise<HistoryPage> {
  const half = Math.max(0, Math.floor(Math.min(ZULIP_MAX_PAGE, limit) / 2));
  const single = await zulipClient.messages.getById({ message_id: messageId, apply_markdown: false });
  assertApiSuccess(single, `message ${messageId}`);
  const anchorMsg = single?.message ? normalizeMessage(single.message as ZulipRawMessage) : null;
  if (!anchorMsg) throw new Error(`message ${messageId} is not readable by this bot`);
  const narrow = anchorMsg.isDm
    ? narrowFor({ dmUserIds: anchorMsg.recipients.map((r) => r.id) })
    : narrowFor({ streamName: anchorMsg.streamName ?? undefined, topic: anchorMsg.topic || undefined });
  const result = await zulipClient.messages.retrieve({
    anchor: messageId,
    num_before: half,
    num_after: half,
    narrow,
    apply_markdown: false,
    include_anchor: true,
  });
  assertApiSuccess(result, `messages around ${messageId}`);
  const messages: ZulipMessage[] = ((result?.messages ?? []) as ZulipRawMessage[])
    .map(normalizeMessage)
    .sort((a, b) => a.id - b.id);
  return { messages, foundNewest: result?.found_newest === true, foundOldest: result?.found_oldest === true };
}

/** What the adapter needs to know about itself to label its own output. */
export interface ZulipIdentity {
  selfUserId: number | null;
  sessionId: string;
}

/** RFC-001 tags for a message. The most specific addressing tag; hosts expand umbrellas. */
export function tagsFor(m: ZulipMessage): string[] {
  const tags: string[] = [];
  if (m.isDm) tags.push(CHAT_TAGS.dm, CHAT_TAGS.private);
  else if (m.mentioned) tags.push(CHAT_TAGS.mention);
  else tags.push(CHAT_TAGS.ambient);
  if (m.wildcardMentioned) tags.push('zulip:wildcard-mention');
  tags.push(looksLikeBotEmail(m.authorEmail) ? CHAT_TAGS.fromBot : CHAT_TAGS.fromHuman);
  if (m.attachments.some((a) => a.isImage)) tags.push(CHAT_TAGS.hasImage);
  if (m.attachments.some((a) => !a.isImage)) tags.push(CHAT_TAGS.hasFile);
  return tags;
}

/** The attachment note appended to a message's content blocks. */
export function attachmentNote(attachments: AttachmentRef[]): TextContent | null {
  if (attachments.length === 0) return null;
  // Reference-only by default: agent reads the note, then decides whether
  // to call fetch_attachment to pull bytes into context.
  const lines = attachments.map(
    (a) => `- ${a.name} (${a.mimeType})${a.isImage ? ' — image, fetchable via fetch_attachment' : ''}: ${a.path}`,
  );
  return { type: 'text', text: `[attachments: ${attachments.length}]\n${lines.join('\n')}` };
}

/**
 * The MCPL incoming shape for a message on `channelId`. Used for live
 * delivery and for history handed back on channels/open (`backscroll: true`
 * in metadata marks the latter).
 */
export function toIncoming(
  channelId: string,
  m: ZulipMessage,
  identity: ZulipIdentity,
  extra: Record<string, unknown> = {},
): IncomingChannelMessage {
  const content: TextContent[] = [{ type: 'text', text: m.cleanContent }];
  const note = attachmentNote(m.attachments);
  if (note) content.push(note);
  return {
    channelId,
    messageId: String(m.id),
    threadId: m.topic || undefined,
    author: { id: String(m.authorId), name: m.authorName },
    timestamp: m.timestamp.toISOString(),
    content,
    tags: tagsFor(m),
    metadata: {
      senderEmail: m.authorEmail,
      topic: m.topic,
      // Zulip's server-computed flag: personal or user-group mention of the
      // bot. Wildcards (@all/@everyone) deliberately don't count.
      mentioned: m.mentioned,
      isDM: m.isDm,
      botUserId: identity.selfUserId !== null ? String(identity.selfUserId) : identity.sessionId,
      ...(m.attachments.length > 0 ? { attachments: m.attachments } : {}),
      ...(m.reactions.length > 0 ? { reactions: m.reactions } : {}),
      ...extra,
    },
  };
}

/**
 * The descriptor for a DM conversation. `recipientName` / `recipientId`
 * make DM addressing people-first for hosts that resolve mention tokens
 * against them; the `address` is what the send API takes.
 */
export function dmDescriptor(
  counterparts: ZulipRecipient[],
  historyCap: number,
): ChannelDescriptor {
  const ids = counterparts.map((r) => r.id);
  const names = counterparts.map((r) => r.full_name);
  const single = counterparts.length === 1 ? counterparts[0] : null;
  return {
    id: dmChannelIdFor(ids),
    type: 'zulip',
    label: single ? `DM: ${single.full_name}` : `Group DM: ${names.join(', ')}`,
    direction: 'bidirectional',
    address: { dm: true, user_ids: ids, emails: counterparts.map((r) => r.email) },
    metadata: {
      channelType: 'dm',
      recipientName: single ? single.full_name : names.join(', '),
      ...(single ? { recipientId: String(single.id), recipientEmail: single.email } : {}),
      participants: counterparts.map((r) => ({ id: r.id, name: r.full_name, email: r.email })),
    },
    capabilities: {
      history: { maxMessages: historyCap, supportsBeforeMessage: true, supportsSinceLastSeen: true },
    },
  };
}
