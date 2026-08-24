/**
 * Tool handlers and MCP resources — the stateful tool layer of this server.
 *
 * Owns the persistent monitoring state (`~/.zulip_mcp_state/<session>.json`):
 * which streams are monitored and the last-read message id per stream. The
 * MCPL layer (channels/context) is independent of this; tools and resources
 * are the plain-MCP surface every client gets.
 */

import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";
import type { ContentBlock } from "@animalabs/mcpl-core";
import {
  cleanContent,
  fetchAttachmentBytes,
  parseZulipAttachmentUrl,
  toFetchResult,
} from "./content.js";
import type { ZulipSession } from "./zulip-client.js";

export interface ChannelState {
  channelName: string;
  lastReadMessageId: number | string;
  subscribed: boolean;
}

interface SessionState {
  sessionId: string;
  userId?: string;
  monitoredChannels: Record<string, ChannelState>;
}

export interface ResourceDescriptor {
  uri: string;
  name: string;
  description: string;
  mimeType: string;
}

export interface ResourceContents {
  contents: { uri: string; mimeType: string; text: string }[];
}

/** MCP `tools/call` result shape. */
export interface ToolCallResult {
  content: ContentBlock[];
  isError?: boolean;
}

const STATE_DIR = join(homedir(), ".zulip_mcp_state");

// Helper function to parse date strings
export function parseDate(dateStr: string | undefined, defaultDate: Date): Date {
  if (!dateStr) return defaultDate;

  const lower = dateStr.toLowerCase();
  const now = new Date();

  if (lower === 'now') return now;
  if (lower === 'today') {
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    return today;
  }
  if (lower === 'yesterday') {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    yesterday.setHours(0, 0, 0, 0);
    return yesterday;
  }

  return new Date(dateStr);
}

// Helper function to format messages
export function formatMessages(messages: any[], format: string): string {
  if (format === 'raw') {
    return JSON.stringify(messages, null, 2);
  }

  if (format === 'summary') {
    const summary = messages.map(msg => {
      const time = new Date(msg.timestamp * 1000).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
      const content = cleanContent(msg.content).substring(0, 80);
      return `[${time}] [${msg.subject}] ${msg.sender_full_name}: ${content}...`;
    }).join('\n');
    return `📊 ${messages.length} messages\n\n${summary}`;
  }

  // Detailed format
  const formatted = messages.map(msg => {
    const time = new Date(msg.timestamp * 1000).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
    const date = new Date(msg.timestamp * 1000).toLocaleDateString('en-US');
    const content = cleanContent(msg.content);

    return `[${date} ${time}] 📝 Topic: ${msg.subject}\n👤 ${msg.sender_full_name}\n💬 ${content}\n`;
  }).join('\n' + '─'.repeat(80) + '\n\n');

  return `📊 Retrieved ${messages.length} messages\n${'='.repeat(80)}\n\n${formatted}`;
}

/**
 * Shape a handler's return value into an MCP `tools/call` result. Handlers
 * that need to return image/non-text content set `_content` directly.
 */
export function toToolCallResult(result: unknown): ToolCallResult {
  if (result && typeof result === "object" && Array.isArray((result as any)._content)) {
    return { content: (result as any)._content };
  }
  return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
}

export class ZulipToolRuntime {
  private readonly zulipClient: any;
  private readonly sessionId: string;
  private readonly monitoredChannels = new Map<string, ChannelState>();

  constructor(
    private readonly session: ZulipSession,
    private readonly stateDir: string = STATE_DIR,
  ) {
    this.zulipClient = session.client;
    this.sessionId = session.sessionId;
    this.loadState();
  }

  // ── Persistent monitoring state ──

  private stateFile(): string {
    return join(this.stateDir, `${this.sessionId}.json`);
  }

  private loadState(): void {
    const stateFile = this.stateFile();
    if (existsSync(stateFile)) {
      try {
        const data = JSON.parse(readFileSync(stateFile, "utf-8")) as SessionState;
        this.monitoredChannels.clear();

        if (data.monitoredChannels) {
          Object.values(data.monitoredChannels).forEach(channel => {
            this.monitoredChannels.set(channel.channelName, channel);
          });
        }

        console.error(`Loaded state for session ${this.sessionId}: ${this.monitoredChannels.size} Zulip channels`);
      } catch (error) {
        console.error(`Failed to load state: ${error}`);
      }
    }
  }

  private saveState(): void {
    if (!this.sessionId) return;

    try {
      if (!existsSync(this.stateDir)) {
        mkdirSync(this.stateDir, { recursive: true });
      }

      const state: SessionState = {
        sessionId: this.sessionId,
        monitoredChannels: Object.fromEntries(this.monitoredChannels),
      };

      writeFileSync(this.stateFile(), JSON.stringify(state, null, 2));
    } catch (error) {
      console.error(`Failed to save state: ${error}`);
    }
  }

  // ── Tools ──

  /** Execute a tool. Returns the raw handler result; see toToolCallResult. */
  async handleToolCall(name: string, args: any): Promise<any> {
    const zulipClient = this.zulipClient;
    const monitoredChannels = this.monitoredChannels;
    args = args ?? {};

    switch (name) {
      case "start_monitoring": {
        const channels: string[] = args.channels;
        const results: any[] = [];

        for (const channelName of channels) {
          try {
            // Get latest message to establish baseline
            const messages = await zulipClient.messages.retrieve({
              anchor: "newest",
              num_before: 1,
              num_after: 0,
              narrow: [["stream", channelName]],
            });

            const lastMessageId = messages.messages.length > 0
              ? messages.messages[0].id
              : 0;

            monitoredChannels.set(channelName, {
              channelName,
              lastReadMessageId: lastMessageId,
              subscribed: true,
            });

            results.push({
              channel: channelName,
              status: "monitoring",
              last_message_id: lastMessageId,
            });
          } catch (error) {
            results.push({
              channel: channelName,
              status: "error",
              error: error instanceof Error ? error.message : String(error),
            });
          }
        }

        this.saveState();

        return {
          session_id: this.sessionId,
          monitored_count: monitoredChannels.size,
          channels: results,
        };
      }

      case "stop_monitoring": {
        const channels: string[] = args.channels;

        if (!channels || channels.length === 0) {
          // Stop all monitoring
          const stopped = Array.from(monitoredChannels.keys());
          monitoredChannels.clear();
          this.saveState();
          return {
            message: "Stopped monitoring all channels",
            stopped_channels: stopped,
          };
        }

        const stopped: string[] = [];
        for (const channel of channels) {
          if (monitoredChannels.has(channel)) {
            monitoredChannels.delete(channel);
            stopped.push(channel);
          }
        }

        this.saveState();

        return {
          stopped_channels: stopped,
          still_monitoring: Array.from(monitoredChannels.keys()),
        };
      }

      case "listen": {
        const channels: string[] = args.channels;
        if (!Array.isArray(channels) || channels.length === 0) {
          return { error: "channels array is required and must be non-empty" };
        }
        try {
          const result = await zulipClient.users.me.subscriptions.add({
            subscriptions: channels.map(name => ({ name })),
          });
          return {
            result: result?.result,
            subscribed: result?.subscribed ?? {},
            already_subscribed: result?.already_subscribed ?? {},
            unauthorized: result?.unauthorized ?? [],
            msg: result?.msg,
          };
        } catch (error) {
          return {
            error: error instanceof Error ? error.message : String(error),
          };
        }
      }

      case "unlisten": {
        const channels: string[] = args.channels;
        if (!Array.isArray(channels) || channels.length === 0) {
          return { error: "channels array is required and must be non-empty" };
        }
        try {
          const result = await zulipClient.users.me.subscriptions.remove({
            subscriptions: JSON.stringify(channels),
          });
          return {
            result: result?.result,
            removed: result?.removed ?? [],
            not_removed: result?.not_removed ?? [],
            msg: result?.msg,
          };
        } catch (error) {
          return {
            error: error instanceof Error ? error.message : String(error),
          };
        }
      }

      case "get_monitored_channels": {
        const channels = Array.from(monitoredChannels.values());
        return {
          monitored_count: channels.length,
          channels: channels.map(c => ({
            name: c.channelName,
            last_read_message_id: c.lastReadMessageId,
            subscribed: c.subscribed,
          })),
        };
      }

      case "get_channel_history": {
        // Parse dates
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        const now = new Date();

        const startDate = parseDate(args.start_date, today);
        const endDate = parseDate(args.end_date, now);
        const startTimestamp = Math.floor(startDate.getTime() / 1000);
        const endTimestamp = Math.floor(endDate.getTime() / 1000);

        // Build narrow filters
        const narrow: any[] = [["stream", args.channel]];
        if (args.topic) {
          narrow.push(["topic", args.topic]);
        }

        // Retrieve messages
        const maxMessages = args.max_messages || 500;
        const result = await zulipClient.messages.retrieve({
          anchor: "newest",
          num_before: maxMessages,
          num_after: 0,
          narrow: narrow,
        });

        // Filter by date range
        const filteredMessages = result.messages.filter((msg: any) => {
          return msg.timestamp >= startTimestamp && msg.timestamp <= endTimestamp;
        });

        // Auto-monitor: Start monitoring this channel if requested (default: true)
        const autoMonitor = args.auto_monitor !== false;
        let monitoringStatus = "not_monitored";

        if (autoMonitor && result.messages.length > 0) {
          const latestMessageId = Math.max(...result.messages.map((m: any) => m.id));

          if (!monitoredChannels.has(args.channel)) {
            monitoredChannels.set(args.channel, {
              channelName: args.channel,
              lastReadMessageId: latestMessageId,
              subscribed: true,
            });
            monitoringStatus = "started_monitoring";
            this.saveState();
          } else {
            // Update last read
            const state = monitoredChannels.get(args.channel)!;
            state.lastReadMessageId = latestMessageId;
            monitoringStatus = "updated_read_position";
            this.saveState();
          }
        } else if (monitoredChannels.has(args.channel)) {
          monitoringStatus = "already_monitored";
        }

        // Format output
        const format = args.format || "detailed";
        const formattedOutput = formatMessages(filteredMessages, format);

        return {
          channel: args.channel,
          topic: args.topic,
          start_date: startDate.toISOString(),
          end_date: endDate.toISOString(),
          message_count: filteredMessages.length,
          total_retrieved: result.messages.length,
          monitoring_status: monitoringStatus,
          formatted_history: formattedOutput,
        };
      }

      case "get_unread_messages": {
        const channelsToCheck = args.channels || Array.from(monitoredChannels.keys());

        if (channelsToCheck.length === 0) {
          return {
            message: "No channels being monitored. Use start_monitoring first.",
            unread_messages: [],
          };
        }

        const allUnreadMessages: any[] = [];
        const channelResults: any[] = [];

        for (const channelName of channelsToCheck) {
          const state = monitoredChannels.get(channelName);

          if (!state) {
            channelResults.push({
              channel: channelName,
              status: "not_monitored",
              unread_count: 0,
            });
            continue;
          }

          try {
            // Get messages since last read
            const result = await zulipClient.messages.retrieve({
              anchor: "newest",
              num_before: 200,
              num_after: 0,
              narrow: [["stream", channelName]],
            });

            const unreadMessages = result.messages.filter((msg: any) =>
              msg.id > state.lastReadMessageId
            );

            allUnreadMessages.push(...unreadMessages.map((msg: any) => ({
              ...msg,
              channel: channelName,
            })));

            // Update last read if mark_as_read is true
            if (args.mark_as_read !== false && unreadMessages.length > 0) {
              state.lastReadMessageId = Math.max(...unreadMessages.map((m: any) => m.id));
            }

            channelResults.push({
              channel: channelName,
              status: "checked",
              unread_count: unreadMessages.length,
              latest_message_id: unreadMessages.length > 0 ? unreadMessages[0].id : state.lastReadMessageId,
            });
          } catch (error) {
            channelResults.push({
              channel: channelName,
              status: "error",
              error: error instanceof Error ? error.message : String(error),
            });
          }
        }

        // Sort by timestamp
        allUnreadMessages.sort((a, b) => a.timestamp - b.timestamp);

        // Save state if we marked messages as read
        if (args.mark_as_read !== false && allUnreadMessages.length > 0) {
          this.saveState();
        }

        // Format output
        const format = args.format || "detailed";
        const formattedOutput = formatMessages(allUnreadMessages, format);

        return {
          total_unread: allUnreadMessages.length,
          channels_checked: channelResults,
          formatted_messages: formattedOutput,
        };
      }

      case "send_message":
        return await zulipClient.messages.send({
          type: args.type,
          to: args.to,
          topic: args.topic,
          content: args.content,
        });

      case "edit_message":
        return await zulipClient.messages.update({
          message_id: args.message_id,
          content: args.content,
        });

      case "delete_message":
        return await zulipClient.messages.deleteById({
          message_id: args.message_id,
        });

      case "list_streams": {
        const result = await zulipClient.streams.retrieve({
          include_public: args.include_public ?? true,
          include_subscribed: args.include_subscribed ?? true,
        });

        // Format streams nicely
        const streams = result.streams || [];
        const formatted = streams
          .sort((a: any, b: any) => b.stream_weekly_traffic - a.stream_weekly_traffic)
          .map((s: any) => {
            const activity = s.is_recently_active ? "🟢" : "⚪";
            const privacy = s.invite_only ? "🔒" : "🌐";
            const traffic = s.stream_weekly_traffic > 0 ? `${s.stream_weekly_traffic} msgs/wk` : "inactive";
            return `${activity} ${privacy} **${s.name}** (${s.subscriber_count} subs, ${traffic})${s.description ? `\n   └─ ${s.description}` : ""}`;
          })
          .join("\n");

        return {
          total_streams: streams.length,
          formatted_list: `📋 **${streams.length} Streams**\n\n${formatted}`,
          stream_ids: Object.fromEntries(streams.map((s: any) => [s.name, s.stream_id])),
          ...(args.verbose && { raw_data: result }),
        };
      }

      case "get_stream_topics": {
        const result = await zulipClient.streams.topics.retrieve({
          stream_id: args.stream_id,
        });

        const topics = result.topics || [];
        const formatted = topics
          .map((t: any, idx: number) => `${idx + 1}. **${t.name}** (latest msg: ${t.max_id})`)
          .join("\n");

        return {
          topic_count: topics.length,
          formatted_list: `📑 **${topics.length} Topics**\n\n${formatted}`,
          ...(args.verbose && { raw_data: result }),
        };
      }

      case "list_users": {
        const result = await zulipClient.users.retrieve({
          client_gravatar: args.client_gravatar || false,
        });

        const users = result.members || [];
        const formatted = users
          .filter((u: any) => u.is_active)
          .map((u: any) => {
            const status = u.is_bot ? "🤖" : "👤";
            const role = u.is_admin ? " (admin)" : u.is_owner ? " (owner)" : "";
            return `${status} **${u.full_name}** <${u.email}>${role}`;
          })
          .join("\n");

        return {
          total_users: users.length,
          active_users: users.filter((u: any) => u.is_active).length,
          formatted_list: `👥 **${users.length} Users** (${users.filter((u: any) => u.is_active).length} active)\n\n${formatted}`,
          ...(args.verbose && { raw_data: result }),
        };
      }

      case "get_user_profile":
        return await zulipClient.users.me.getProfile();

      case "add_reaction":
        return await zulipClient.reactions.add({
          message_id: args.message_id,
          emoji_name: args.emoji_name,
          reaction_type: "unicode_emoji",
        });

      case "find_user": {
        const usersResult = await zulipClient.users.retrieve();
        const members = usersResult.members || [];
        const query = args.query.toLowerCase();

        const matches = members.filter((user: any) =>
          user.full_name.toLowerCase().includes(query) ||
          user.email.toLowerCase().includes(query)
        );

        if (matches.length === 0) {
          return {
            found: false,
            message: `No users found matching "${args.query}"`,
          };
        }

        const formatted = matches.map((user: any) => {
          const status = user.is_bot ? "🤖" : "👤";
          return `${status} **${user.full_name}** <${user.email}>\n   └─ User ID: ${user.user_id}\n   └─ Mention format: @**${user.full_name}**`;
        }).join("\n\n");

        return {
          found: true,
          match_count: matches.length,
          formatted_list: `👥 Found ${matches.length} user${matches.length !== 1 ? 's' : ''}:\n\n${formatted}`,
          users: matches.map((u: any) => ({
            user_id: u.user_id,
            full_name: u.full_name,
            email: u.email,
            is_bot: u.is_bot,
            mention_syntax: `@**${u.full_name}**`,
          })),
        };
      }

      case "fetch_attachment": {
        // Validation (realm host + /user_uploads/ prefix, with the URL
        // dot-segment normalization invariant) lives in parseZulipAttachmentUrl.
        const url = parseZulipAttachmentUrl(String(args.path || ""), this.session.realm);

        const headers: Record<string, string> = {};
        if (this.session.authHeader) headers["Authorization"] = this.session.authHeader;
        const name = decodeURIComponent(url.pathname.split("/").pop() || "attachment");
        return toFetchResult(await fetchAttachmentBytes(url.toString(), name, { headers }));
      }

      default:
        throw new Error(`Unknown tool: ${name}`);
    }
  }

  // ── Resources ──

  listResources(): ResourceDescriptor[] {
    const resources: ResourceDescriptor[] = [
      {
        uri: "zulip://unread/summary",
        name: "Zulip - Unread Messages Summary",
        description: "Count of unread messages across all monitored Zulip channels",
        mimeType: "text/plain",
      },
      {
        uri: "zulip://monitoring/status",
        name: "Zulip - Monitoring Status",
        description: "Current Zulip monitoring state and channel list",
        mimeType: "application/json",
      },
    ];

    // Add a resource for each monitored Zulip channel
    for (const channel of this.monitoredChannels.keys()) {
      resources.push({
        uri: `zulip://channel/${channel}/unread`,
        name: `Zulip #${channel} - Unread Messages`,
        description: `Unread message count for Zulip #${channel}`,
        mimeType: "text/plain",
      });
    }

    return resources;
  }

  async readResource(uri: string): Promise<ResourceContents> {
    const zulipClient = this.zulipClient;
    const monitoredChannels = this.monitoredChannels;

    // Handle unread summary
    if (uri === "zulip://unread/summary") {
      let totalUnread = 0;
      const channelSummaries: string[] = [];

      for (const [channelName, state] of monitoredChannels) {
        try {
          const result = await zulipClient.messages.retrieve({
            anchor: "newest",
            num_before: 50,
            num_after: 0,
            narrow: [["stream", channelName]],
          });

          const unread = result.messages.filter((msg: any) => msg.id > state.lastReadMessageId);
          totalUnread += unread.length;

          if (unread.length > 0) {
            channelSummaries.push(`📬 #${channelName}: ${unread.length} unread`);
          }
        } catch (error) {
          // Skip channels with errors
        }
      }

      const summary = totalUnread > 0
        ? `🔔 ${totalUnread} unread message${totalUnread !== 1 ? 's' : ''}\n\n${channelSummaries.join('\n')}`
        : "✅ No unread messages";

      return { contents: [{ uri, mimeType: "text/plain", text: summary }] };
    }

    // Handle monitoring status
    if (uri === "zulip://monitoring/status") {
      const status = {
        session_id: this.sessionId,
        monitored_count: monitoredChannels.size,
        channels: Array.from(monitoredChannels.values()).map(c => ({
          name: c.channelName,
          last_read_message_id: c.lastReadMessageId,
        })),
      };

      return { contents: [{ uri, mimeType: "application/json", text: JSON.stringify(status, null, 2) }] };
    }

    // Handle individual channel unread count
    const channelMatch = uri.match(/^zulip:\/\/channel\/([^/]+)\/unread$/);
    if (channelMatch) {
      const channelName = channelMatch[1];
      const state = monitoredChannels.get(channelName);

      if (!state) {
        return { contents: [{ uri, mimeType: "text/plain", text: `Channel "${channelName}" is not being monitored` }] };
      }

      try {
        const result = await zulipClient.messages.retrieve({
          anchor: "newest",
          num_before: 50,
          num_after: 0,
          narrow: [["stream", channelName]],
        });

        const unread = result.messages.filter((msg: any) => msg.id > state.lastReadMessageId);
        const count = unread.length;

        const text = count > 0
          ? `📬 ${count} unread message${count !== 1 ? 's' : ''} in #${channelName}`
          : `✅ No unread messages in #${channelName}`;

        return { contents: [{ uri, mimeType: "text/plain", text }] };
      } catch (error) {
        return { contents: [{ uri, mimeType: "text/plain", text: `Error checking #${channelName}: ${error}` }] };
      }
    }

    throw new Error(`Unknown resource: ${uri}`);
  }
}
