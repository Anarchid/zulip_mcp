/**
 * DiscordAdapter — Discord implementation of PlatformAdapter.
 *
 * Channel ID format: discord:{guildId}:{channelId}
 * Guild text channels only (DMs are ignored, matching prior behavior).
 * No typing indicator wired (sendTyping omitted).
 */

import type { Client as DiscordClient, TextChannel, Message } from 'discord.js';
import type {
  ChannelDescriptor,
  ChannelIncomingMessage,
  McplContentBlock,
  McplContextInjection,
  McplTextContent,
} from '../mcpl/types.js';
import type { PlatformAdapter, PublishResult, OnIncomingMessage } from './adapter.js';
import { formatDiscordContent, classifyExtension, type AttachmentRef } from '../content.js';

export class DiscordAdapter implements PlatformAdapter {
  readonly type = 'discord';

  private messageListener: ((msg: Message) => void) | null = null;

  constructor(private discordClient: DiscordClient) {}

  async discoverChannels(): Promise<ChannelDescriptor[]> {
    const channels: ChannelDescriptor[] = [];
    for (const guild of this.discordClient.guilds.cache.values()) {
      for (const channel of guild.channels.cache.values()) {
        if (channel.isTextBased() && 'name' in channel) {
          const textChannel = channel as TextChannel;
          channels.push({
            id: `discord:${guild.id}:${textChannel.id}`,
            type: 'discord',
            label: `#${textChannel.name} (${guild.name})`,
            direction: 'bidirectional',
            address: { guild_id: guild.id, guild_name: guild.name, channel_id: textChannel.id },
            metadata: { topic: textChannel.topic || undefined },
          });
        }
      }
    }
    return channels;
  }

  async publish(
    channelId: string,
    _descriptor: ChannelDescriptor | undefined,
    content: McplContentBlock[],
  ): Promise<PublishResult> {
    const textContent = content
      .filter((c): c is McplTextContent => c.type === 'text')
      .map(c => c.text)
      .join('\n');
    if (!textContent) return { delivered: false };

    // channelId format: discord:{guildId}:{channelId}
    const parts = channelId.split(':');
    const discordChannelId = parts[2];

    const channel = await this.discordClient.channels.fetch(discordChannelId);
    if (!channel || !('send' in channel)) {
      throw new Error(`Discord channel ${discordChannelId} not found or not a text channel`);
    }

    const sent = await (channel as TextChannel).send(textContent);
    return { delivered: true, messageId: sent.id };
  }

  async fetchContext(
    channelId: string,
    _descriptor: ChannelDescriptor | undefined,
    historySize: number,
  ): Promise<McplContextInjection | null> {
    // channelId format: discord:{guildId}:{channelId}
    const parts = channelId.split(':');
    const discordChannelId = parts[2];

    const channel = await this.discordClient.channels.fetch(discordChannelId);
    if (!channel || !('messages' in channel)) return null;

    const textChannel = channel as TextChannel;
    const fetched = await textChannel.messages.fetch({ limit: historySize });
    const messages = Array.from(fetched.values()).reverse();

    if (messages.length === 0) return null;

    const formatted = messages.map(msg => {
      const time = msg.createdAt.toLocaleTimeString('en-US', {
        hour: '2-digit', minute: '2-digit',
      });
      const replyPrefix = msg.reference?.messageId ? '(reply) ' : '';
      return `[${time}] ${replyPrefix}${msg.author.tag}: ${msg.content}`;
    }).join('\n');

    const channelName = textChannel.name;
    const guildName = textChannel.guild.name;

    return {
      namespace: `discord:${channelName}`,
      position: 'beforeUser',
      content: `Recent messages from Discord #${channelName} (${guildName}):\n${formatted}`,
    };
  }

  startEvents(onMessage: OnIncomingMessage): void {
    this.messageListener = (msg: Message) => {
      // Ignore bot's own messages
      if (msg.author.id === this.discordClient.user?.id) return;
      if (!msg.guild) return; // Ignore DMs

      const channelId = `discord:${msg.guild.id}:${msg.channelId}`;
      const cleaned = formatDiscordContent(msg.content, msg.mentions);
      const attachments: AttachmentRef[] = msg.attachments.map((a) => {
        const name = a.name ?? 'attachment';
        const { mimeType: extMime } = classifyExtension(name);
        const mime = a.contentType ?? extMime;
        return {
          path: a.url,            // Discord CDN URL, no auth needed
          name,
          mimeType: mime,
          isImage: mime.startsWith('image/'),
        };
      });
      const content: McplTextContent[] = [{ type: 'text', text: cleaned }];
      if (attachments.length > 0) {
        const lines = attachments.map(a =>
          `- ${a.name} (${a.mimeType})${a.isImage ? ' — image, fetchable via discord_fetch_attachment' : ''}: ${a.path}`,
        );
        content.push({
          type: 'text',
          text: `[attachments: ${attachments.length}]\n${lines.join('\n')}`,
        });
      }
      const incoming: ChannelIncomingMessage = {
        channelId,
        messageId: msg.id,
        author: { id: msg.author.id, name: msg.author.tag },
        timestamp: msg.createdAt.toISOString(),
        content,
        metadata: {
          mentionIds: Array.from(msg.mentions.users.keys()),
          replyToAuthorId: msg.reference?.messageId ? msg.author.id : undefined,
          botUserId: this.discordClient.user?.id,
          ...(attachments.length > 0 ? { attachments } : {}),
        },
      };
      onMessage(incoming);
    };
    this.discordClient.on('messageCreate', this.messageListener);
  }

  stopEvents(): void {
    if (this.messageListener) {
      this.discordClient.off('messageCreate', this.messageListener);
      this.messageListener = null;
    }
  }
}
