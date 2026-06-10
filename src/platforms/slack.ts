/**
 * SlackAdapter — Slack implementation of PlatformAdapter.
 *
 * Channel ID format: slack:{conversationId}  (C… public, G… private,
 * D… DM, immutable Slack IDs — names are mutable and never used for routing).
 *
 * Connectivity: Web API (xoxb- bot token) for calls, Socket Mode (xapp-
 * app-level token) for real-time events — no public webhook URL needed,
 * matching the stdio-spawned server model.
 *
 * Threads map to thread_ts: incoming messages carry their thread_ts as
 * threadId, and outgoing publishes reply in the thread of the most recent
 * incoming message on the channel (top-level when the conversation isn't
 * threaded). DMs and MPIMs are first-class channels so users can ping the
 * agent privately.
 *
 * No typing indicator: Slack's Web API exposes none for bots (RTM-only,
 * deprecated), so sendTyping is omitted.
 */

import type { WebClient } from '@slack/web-api';
import type { SocketModeClient } from '@slack/socket-mode';
import type {
  ChannelDescriptor,
  ChannelIncomingMessage,
  McplContentBlock,
  McplContextInjection,
  McplTextContent,
} from '../mcpl/types.js';
import type { PlatformAdapter, PublishResult, RoutingHints, OnIncomingMessage } from './adapter.js';
import { formatSlackText, extractSlackUserIds, resolveSlackUserNames, type AttachmentRef } from '../content.js';

/** Message subtypes that represent real user content. Everything else
 * (message_changed, message_deleted, channel_join, bot_message, …) is noise
 * for the inference loop. */
const CONTENT_SUBTYPES = new Set([undefined, 'file_share', 'thread_broadcast', 'me_message']);

/** The fields of a Socket Mode message event this adapter dereferences.
 * Payloads come off the wire — everything is optional until checked. */
interface SlackMessageEvent {
  type?: string;
  subtype?: string;
  channel?: string;
  user?: string;
  text?: string;
  ts?: string;
  thread_ts?: string;
  bot_id?: string;
  channel_type?: string;
  team?: string;
  files?: Array<{ url_private?: string; name?: string; mimetype?: string }>;
}

/** The fields of a conversations.list entry this adapter dereferences —
 * structurally satisfied by @slack/web-api's Channel type. */
interface SlackConversation {
  id?: string;
  name?: string;
  user?: string;
  is_im?: boolean;
  is_mpim?: boolean;
  is_private?: boolean;
  is_member?: boolean;
  num_members?: number;
  topic?: { value?: string };
}

export class SlackAdapter implements PlatformAdapter {
  readonly type = 'slack';

  private userNameCache = new Map<string, string>();
  private socketStarted = false;

  constructor(
    private web: WebClient,
    private socket: SocketModeClient,
    private selfUserId: string | null,
    private teamName: string,
  ) {}

  async discoverChannels(): Promise<ChannelDescriptor[]> {
    const channels: ChannelDescriptor[] = [];
    try {
      let cursor: string | undefined;
      do {
        const result = await this.web.conversations.list({
          types: 'public_channel,private_channel,im,mpim',
          exclude_archived: true,
          limit: 200,
          cursor,
        });
        for (const conv of result.channels ?? []) {
          const descriptor = await this.describeConversation(conv);
          if (descriptor) channels.push(descriptor);
        }
        cursor = result.response_metadata?.next_cursor || undefined;
      } while (cursor);
    } catch (error) {
      console.error('Failed to discover Slack conversations:', error);
    }
    return channels;
  }

  async publish(
    channelId: string,
    _descriptor: ChannelDescriptor | undefined,
    content: McplContentBlock[],
    hints?: RoutingHints,
  ): Promise<PublishResult> {
    const textContent = content
      .filter((c): c is McplTextContent => c.type === 'text')
      .map(c => c.text)
      .join('\n');
    if (!textContent) return { delivered: false };

    // channelId format: slack:{conversationId}
    const conversationId = channelId.slice('slack:'.length);

    // Reply in the thread of the most recent incoming message; top-level
    // when the conversation isn't threaded.
    const threadTs =
      (typeof hints?.metadata?.thread_ts === 'string' ? hints.metadata.thread_ts : undefined) ??
      hints?.threadId;

    const result = await this.web.chat.postMessage({
      channel: conversationId,
      text: textContent,
      ...(threadTs ? { thread_ts: threadTs } : {}),
    });

    return { delivered: true, messageId: result.ts ? String(result.ts) : undefined };
  }

  async fetchContext(
    channelId: string,
    descriptor: ChannelDescriptor | undefined,
    historySize: number,
  ): Promise<McplContextInjection | null> {
    const conversationId = channelId.slice('slack:'.length);

    const result = await this.web.conversations.history({
      channel: conversationId,
      limit: historySize,
    });

    // Newest-first from the API; oldest-first for reading.
    const messages = (result.messages ?? []).slice().reverse();
    if (messages.length === 0) return null;

    // Pre-resolve author + mentioned user names in one pass.
    const ids = new Set<string>();
    for (const msg of messages) {
      if (msg.user) ids.add(msg.user);
      for (const id of extractSlackUserIds(msg.text ?? '')) ids.add(id);
    }
    await this.resolveUserNames(Array.from(ids));

    const formatted = messages.map(msg => {
      const time = new Date(parseFloat(msg.ts ?? '0') * 1000).toLocaleTimeString('en-US', {
        hour: '2-digit', minute: '2-digit',
      });
      const author = msg.user ? (this.userNameCache.get(msg.user) ?? msg.user) : (msg.username ?? 'bot');
      const threadPrefix = msg.thread_ts && msg.thread_ts !== msg.ts ? '(thread reply) ' : '';
      const text = formatSlackText(msg.text ?? '', this.userNameCache);
      return `[${time}] ${threadPrefix}${author}: ${text}`;
    }).join('\n');

    const label = descriptor?.label ?? `slack:${conversationId}`;

    return {
      namespace: `slack:${conversationId}`,
      position: 'beforeUser',
      content: `Recent messages from Slack ${label}:\n${formatted}`,
    };
  }

  startEvents(onMessage: OnIncomingMessage): void {
    this.socket.on('message', async ({ event, ack }: { event: SlackMessageEvent; ack: () => Promise<void> }) => {
      // Always ack first — Slack redelivers unacked envelopes.
      try { await ack(); } catch { /* ignore */ }
      try {
        await this.handleMessageEvent(event, onMessage);
      } catch (error) {
        console.error('Failed to handle Slack message event:', error);
      }
    });

    this.socket.start().then(() => {
      this.socketStarted = true;
      console.error('Slack Socket Mode connected');
    }).catch(error => {
      console.error('Slack Socket Mode failed to start:', error);
    });
  }

  stopEvents(): void {
    if (this.socketStarted) {
      this.socket.disconnect().catch(() => {});
      this.socketStarted = false;
    }
  }

  // -- Private --

  private async handleMessageEvent(event: SlackMessageEvent, onMessage: OnIncomingMessage): Promise<void> {
    if (!event || event.type !== 'message') return;
    if (!CONTENT_SUBTYPES.has(event.subtype)) return;
    // Self-filter: skip our own messages and other bots' (bot_id covers
    // bot_message-without-subtype edge cases like app-posted file shares).
    if (event.bot_id) return;
    if (this.selfUserId !== null && event.user === this.selfUserId) return;
    // A real user content message always carries channel, user, and ts;
    // a malformed payload missing any of them is dropped, not crashed on
    // (parseFloat(undefined) → NaN → toISOString() throws).
    if (!event.channel || !event.user || !event.ts) return;

    const channelId = `slack:${event.channel}`;

    // Resolve author + mentioned users before formatting.
    const mentionIds = extractSlackUserIds(event.text ?? '');
    await this.resolveUserNames([event.user, ...mentionIds].filter(Boolean));
    const authorName = this.userNameCache.get(event.user) ?? event.user ?? 'unknown';

    const cleaned = formatSlackText(event.text ?? '', this.userNameCache);

    const attachments: AttachmentRef[] = (event.files ?? [])
      .filter((f): f is { url_private: string; name?: string; mimetype?: string } => !!f.url_private)
      .map((f) => {
        const mime = f.mimetype || 'application/octet-stream';
        return {
          path: f.url_private,    // needs Bearer bot-token auth to fetch
          name: f.name ?? 'attachment',
          mimeType: mime,
          isImage: mime.startsWith('image/'),
        };
      });

    const content: McplTextContent[] = [{ type: 'text', text: cleaned }];
    if (attachments.length > 0) {
      const lines = attachments.map(a =>
        `- ${a.name} (${a.mimeType})${a.isImage ? ' — image, fetchable via slack_fetch_attachment' : ''}: ${a.path}`,
      );
      content.push({
        type: 'text',
        text: `[attachments: ${attachments.length}]\n${lines.join('\n')}`,
      });
    }

    const incoming: ChannelIncomingMessage = {
      channelId,
      messageId: String(event.ts),
      threadId: event.thread_ts || undefined,
      author: { id: String(event.user), name: authorName },
      timestamp: new Date(parseFloat(event.ts) * 1000).toISOString(),
      content,
      metadata: {
        mentionIds,
        // Personal mention of the bot — hosts use this to gate spawn/inference
        // policy (e.g. "respond on-mention in channels"). @here/@channel
        // broadcasts deliberately don't count.
        mentioned: this.selfUserId !== null && mentionIds.includes(this.selfUserId),
        thread_ts: event.thread_ts,
        channel_type: event.channel_type,
        team: event.team,
        botUserId: this.selfUserId ?? undefined,
        ...(attachments.length > 0 ? { attachments } : {}),
      },
    };
    onMessage(incoming);
  }

  private async describeConversation(conv: SlackConversation): Promise<ChannelDescriptor | null> {
    if (!conv.id) return null;

    if (conv.is_im) {
      // DM: label with the human's name so the host can scope/whitelist it.
      await this.resolveUserNames([conv.user]);
      const userName = (conv.user ? this.userNameCache.get(conv.user) : undefined) ?? conv.user ?? conv.id;
      return {
        id: `slack:${conv.id}`,
        type: 'slack',
        label: `DM: @${userName} (${this.teamName})`,
        direction: 'bidirectional',
        address: { channel_id: conv.id, user_id: conv.user, user_name: userName },
        metadata: { is_im: true },
      };
    }

    if (conv.is_mpim) {
      return {
        id: `slack:${conv.id}`,
        type: 'slack',
        label: `Group DM: ${conv.name ?? conv.id} (${this.teamName})`,
        direction: 'bidirectional',
        address: { channel_id: conv.id, channel_name: conv.name },
        metadata: { is_mpim: true },
      };
    }

    return {
      id: `slack:${conv.id}`,
      type: 'slack',
      label: `#${conv.name} (${this.teamName})`,
      direction: 'bidirectional',
      address: { channel_id: conv.id, channel_name: conv.name },
      metadata: {
        topic: conv.topic?.value || undefined,
        is_private: !!conv.is_private,
        is_member: !!conv.is_member,
        num_members: conv.num_members,
      },
    };
  }

  /** Resolve user IDs into this adapter's cache (shared logic in content.ts). */
  private async resolveUserNames(userIds: Array<string | undefined>): Promise<void> {
    await resolveSlackUserNames(
      this.web,
      this.userNameCache,
      userIds.filter((id): id is string => !!id),
    );
  }
}
