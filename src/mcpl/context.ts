/**
 * Context Provider — Handles context/beforeInference.
 *
 * Injects recent message history from open channels into the inference context.
 * Fetches last N messages from each open Zulip stream and Discord channel.
 */

import type { TextChannel, Client as DiscordClient } from 'discord.js';
import type {
  BeforeInferenceParams,
  BeforeInferenceResult,
  McplContextInjection,
} from './types.js';
import type { ChannelManager } from './channels.js';

const DEFAULT_HISTORY_SIZE = 20;

export class ContextProvider {
  private historySize: number;

  constructor(
    private channelManager: ChannelManager,
    private zulipClient: any | null,
    private discordClient: DiscordClient | null,
    private cleanContent: (html: string) => string,
    historySize?: number,
  ) {
    this.historySize = historySize ?? DEFAULT_HISTORY_SIZE;
  }

  /**
   * Handle context/beforeInference — return context injections for open channels.
   */
  async handleBeforeInference(_params: BeforeInferenceParams): Promise<BeforeInferenceResult> {
    const injections: McplContextInjection[] = [];
    const openChannels = this.channelManager.getOpenChannels();

    for (const channelId of openChannels) {
      try {
        const injection = await this.getChannelContext(channelId);
        if (injection) {
          injections.push(injection);
        }
      } catch (error) {
        console.error(`Failed to get context for channel ${channelId}:`, error);
      }
    }

    // Determine feature set based on which channels contributed
    const hasZulip = injections.some(i => i.namespace.startsWith('zulip'));
    const hasDiscord = injections.some(i => i.namespace.startsWith('discord'));
    const featureSet = hasZulip && hasDiscord
      ? 'zulip.context'
      : hasDiscord ? 'discord.context' : 'zulip.context';

    return {
      featureSet,
      contextInjections: injections,
    };
  }

  private async getChannelContext(channelId: string): Promise<McplContextInjection | null> {
    if (channelId.startsWith('zulip:')) {
      return this.getZulipContext(channelId);
    }
    if (channelId.startsWith('discord:')) {
      return this.getDiscordContext(channelId);
    }
    return null;
  }

  private async getZulipContext(channelId: string): Promise<McplContextInjection | null> {
    if (!this.zulipClient) return null;

    const streamName = channelId.slice('zulip:'.length);

    const result = await this.zulipClient.messages.retrieve({
      anchor: 'newest',
      num_before: this.historySize,
      num_after: 0,
      narrow: [['stream', streamName]],
    });

    const messages = result.messages || [];
    if (messages.length === 0) return null;

    const formatted = messages.map((msg: any) => {
      const time = new Date(msg.timestamp * 1000).toLocaleTimeString('en-US', {
        hour: '2-digit', minute: '2-digit',
      });
      const content = this.cleanContent(msg.content);
      return `[${time}] [${msg.subject}] ${msg.sender_full_name}: ${content}`;
    }).join('\n');

    return {
      namespace: `zulip:${streamName}`,
      position: 'beforeUser',
      content: `Recent messages from Zulip #${streamName}:\n${formatted}`,
    };
  }

  private async getDiscordContext(channelId: string): Promise<McplContextInjection | null> {
    if (!this.discordClient) return null;

    // channelId format: discord:{guildId}:{channelId}
    const parts = channelId.split(':');
    const discordChannelId = parts[2];

    const channel = await this.discordClient.channels.fetch(discordChannelId);
    if (!channel || !('messages' in channel)) return null;

    const textChannel = channel as TextChannel;
    const fetched = await textChannel.messages.fetch({ limit: this.historySize });
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
}
