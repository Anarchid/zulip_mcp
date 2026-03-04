/**
 * Channel Manager — Maps platform channels to MCPL channels.
 *
 * Channel ID formats:
 *   Zulip:   zulip:{stream_name}
 *   Discord: discord:{guildId}:{channelId}
 *
 * Handles registration, open/close lifecycle, incoming message batching,
 * publish routing, and channel listing.
 */

import type { Client as DiscordClient, TextChannel } from 'discord.js';
import type {
  ChannelDescriptor,
  ChannelIncomingMessage,
  McplTextContent,
  ChannelsPublishParams,
  ChannelsOpenParams,
  ChannelsCloseParams,
  ChannelsListResult,
} from './types.js';
import type { McplClient } from './client.js';

const DEFAULT_BATCH_WINDOW_MS = 500;

export class ChannelManager {
  private allChannels = new Map<string, ChannelDescriptor>();
  private openChannels = new Set<string>();
  private batchBuffer = new Map<string, ChannelIncomingMessage[]>();
  private batchTimer: ReturnType<typeof setTimeout> | null = null;
  private batchWindowMs: number;

  constructor(
    private mcplClient: McplClient,
    private zulipClient: any | null,
    private discordClient: DiscordClient | null,
    batchWindowMs?: number,
  ) {
    this.batchWindowMs = batchWindowMs ?? DEFAULT_BATCH_WINDOW_MS;
  }

  /**
   * Discover all available channels and register them with the host.
   */
  async registerChannels(): Promise<void> {
    const channels: ChannelDescriptor[] = [];

    // Discover Zulip streams
    if (this.zulipClient) {
      try {
        const result = await this.zulipClient.streams.retrieve({
          include_public: true,
          include_subscribed: true,
        });
        const streams = result.streams || [];
        for (const stream of streams) {
          const descriptor: ChannelDescriptor = {
            id: `zulip:${stream.name}`,
            type: 'zulip',
            label: `#${stream.name}`,
            direction: 'bidirectional',
            address: { stream_name: stream.name, stream_id: stream.stream_id },
            metadata: {
              subscriber_count: stream.subscriber_count,
              is_public: !stream.invite_only,
            },
          };
          channels.push(descriptor);
          this.allChannels.set(descriptor.id, descriptor);
        }
      } catch (error) {
        console.error('Failed to discover Zulip streams:', error);
      }
    }

    // Discover Discord text channels
    if (this.discordClient) {
      for (const guild of this.discordClient.guilds.cache.values()) {
        for (const channel of guild.channels.cache.values()) {
          if (channel.isTextBased() && 'name' in channel) {
            const textChannel = channel as TextChannel;
            const descriptor: ChannelDescriptor = {
              id: `discord:${guild.id}:${textChannel.id}`,
              type: 'discord',
              label: `#${textChannel.name} (${guild.name})`,
              direction: 'bidirectional',
              address: { guild_id: guild.id, guild_name: guild.name, channel_id: textChannel.id },
              metadata: { topic: textChannel.topic || undefined },
            };
            channels.push(descriptor);
            this.allChannels.set(descriptor.id, descriptor);
          }
        }
      }
    }

    if (channels.length > 0) {
      try {
        await this.mcplClient.registerChannels(channels);
        console.error(`Registered ${channels.length} channels with host`);
      } catch (error) {
        console.error('Failed to register channels:', error);
      }
    }
  }

  /**
   * Handle channels/open from the host.
   */
  openChannel(params: ChannelsOpenParams): { channel: ChannelDescriptor } {
    // Find channel by type and address
    for (const [id, descriptor] of this.allChannels) {
      if (descriptor.type === params.type) {
        const matchesAddress = !params.address ||
          Object.entries(params.address).every(([k, v]) => descriptor.address?.[k] === v);
        if (matchesAddress) {
          this.openChannels.add(id);
          return { channel: descriptor };
        }
      }
    }
    throw new Error(`No channel found matching type=${params.type}`);
  }

  /**
   * Handle channels/close from the host.
   */
  closeChannel(params: ChannelsCloseParams): { closed: boolean } {
    const existed = this.openChannels.delete(params.channelId);
    return { closed: existed };
  }

  /**
   * Handle channels/list from the host.
   */
  listChannels(): ChannelsListResult {
    return { channels: Array.from(this.allChannels.values()) };
  }

  /**
   * Called when a new message arrives from a platform.
   * Buffers messages and flushes in batches.
   */
  onIncomingMessage(channelId: string, message: ChannelIncomingMessage): void {
    if (!this.openChannels.has(channelId)) return; // channel not opened by host

    let buffer = this.batchBuffer.get(channelId);
    if (!buffer) {
      buffer = [];
      this.batchBuffer.set(channelId, buffer);
    }
    buffer.push(message);

    this.scheduleBatchFlush();
  }

  /**
   * Handle channels/publish from the host — send a message to Zulip or Discord.
   */
  async publish(params: ChannelsPublishParams): Promise<{ delivered: boolean; messageId?: string }> {
    const channelId = params.channelId;
    const textContent = params.content
      .filter((c): c is McplTextContent => c.type === 'text')
      .map(c => c.text)
      .join('\n');

    if (!textContent) {
      return { delivered: false };
    }

    if (channelId.startsWith('zulip:')) {
      return this.publishToZulip(channelId, textContent);
    }

    if (channelId.startsWith('discord:')) {
      return this.publishToDiscord(channelId, textContent);
    }

    throw new Error(`Unknown channel format: ${channelId}`);
  }

  /**
   * Get the set of currently open channel IDs.
   */
  getOpenChannels(): Set<string> {
    return this.openChannels;
  }

  /**
   * Get a channel descriptor by ID.
   */
  getChannel(id: string): ChannelDescriptor | undefined {
    return this.allChannels.get(id);
  }

  /**
   * Cleanup timers.
   */
  destroy(): void {
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }
  }

  // -- Private --

  private scheduleBatchFlush(): void {
    if (this.batchTimer) return; // already scheduled
    this.batchTimer = setTimeout(() => {
      this.batchTimer = null;
      this.flushBatch();
    }, this.batchWindowMs);
  }

  private async flushBatch(): Promise<void> {
    const allMessages: ChannelIncomingMessage[] = [];

    for (const [, messages] of this.batchBuffer) {
      allMessages.push(...messages);
    }
    this.batchBuffer.clear();

    if (allMessages.length === 0) return;

    try {
      await this.mcplClient.sendIncoming(allMessages);
    } catch (error) {
      console.error('Failed to send incoming messages to host:', error);
    }
  }

  private async publishToZulip(channelId: string, content: string): Promise<{ delivered: boolean; messageId?: string }> {
    if (!this.zulipClient) throw new Error('Zulip client not initialized');

    // channelId format: zulip:{stream_name}
    const streamName = channelId.slice('zulip:'.length);

    // Default topic — the host can override via content conventions
    const result = await this.zulipClient.messages.send({
      type: 'stream',
      to: streamName,
      topic: 'mcpl',
      content,
    });

    return { delivered: true, messageId: String(result.id) };
  }

  private async publishToDiscord(channelId: string, content: string): Promise<{ delivered: boolean; messageId?: string }> {
    if (!this.discordClient) throw new Error('Discord client not initialized');

    // channelId format: discord:{guildId}:{channelId}
    const parts = channelId.split(':');
    const discordChannelId = parts[2];

    const channel = await this.discordClient.channels.fetch(discordChannelId);
    if (!channel || !('send' in channel)) {
      throw new Error(`Discord channel ${discordChannelId} not found or not a text channel`);
    }

    const sent = await (channel as TextChannel).send(content);
    return { delivered: true, messageId: sent.id };
  }
}
