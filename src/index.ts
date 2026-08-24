#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  ListResourcesRequestSchema,
  ReadResourceRequestSchema,
  Tool,
} from "@modelcontextprotocol/sdk/types.js";
import zulipInit from "zulip-js";
import { Client as DiscordClient, GatewayIntentBits, TextChannel } from "discord.js";

// MCPL imports
import { McplClient } from './mcpl/client.js';
import { McplDispatcher } from './mcpl/dispatcher.js';
import { McplTransport } from './mcpl/transport.js';
import { ChannelManager } from './mcpl/channels.js';
import { ContextProvider } from './mcpl/context.js';
import { buildServerCapabilities } from './mcpl/feature-sets.js';
import { CapabilityGrant } from './mcpl/grant.js';
import { McplRpcError } from './mcpl/errors.js';
import { ManifestTracker } from './mcpl/manifest.js';
import { McplMethod } from './mcpl/types.js';
import type { ChannelsPublishParams, ChannelsOpenParams, ChannelsCloseParams, BeforeInferenceParams, FeatureSetsUpdateParams } from './mcpl/types.js';

// Platform adapters
import type { PlatformAdapter } from './platforms/adapter.js';
import { ZulipAdapter } from './platforms/zulip.js';
import { DiscordAdapter } from './platforms/discord.js';
import { SlackAdapter, type SlackConversation } from './platforms/slack.js';
import { WebClient as SlackWebClient } from '@slack/web-api';
import { SocketModeClient as SlackSocketModeClient } from '@slack/socket-mode';

// Content helpers (re-exported for tests and downstream importers)
import {
  fetchAttachmentBytes,
  toFetchResult,
  cleanContent,
  formatDiscordContent,
  formatSlackText,
  extractSlackUserIds,
  resolveSlackUserNames,
  parseDiscordAttachmentUrl,
  parseSlackAttachmentUrl,
  parseZulipAttachmentUrl,
  fetchSlackHistory,
  type SlackHistoryMessage,
  isMainModule,
} from './content.js';
export { fetchAttachmentBytes, extractZulipAttachments, cleanContent, formatDiscordContent } from './content.js';

// Startup flags
const ENABLE_ZULIP = process.env.ENABLE_ZULIP !== "false";
const ENABLE_DISCORD = process.env.ENABLE_DISCORD === "true";
const ENABLE_SLACK = process.env.ENABLE_SLACK === "true";
const MCPL_ENABLED = process.env.MCPL_ENABLED !== "false";
const MCPL_BATCH_WINDOW_MS = parseInt(process.env.MCPL_BATCH_WINDOW_MS || "500", 10);
const MCPL_CONTEXT_HISTORY_SIZE = parseInt(process.env.MCPL_CONTEXT_HISTORY_SIZE || "20", 10);

// Initialize clients
let zulipClient: any = null;
let zulipSelfUserId: number | null = null;
let zulipRealm: string = "";
let zulipAuthHeader: string = "";
let discordClient: DiscordClient | null = null;
let slackWebClient: SlackWebClient | null = null;
let slackSocketClient: SlackSocketModeClient | null = null;
let slackSelfUserId: string | null = null;
let slackTeamName: string = "";

// Session and state management
interface ChannelState {
  channelName: string;
  lastReadMessageId: number | string;
  subscribed: boolean;
}

interface DiscordChannelState {
  channelId: string;
  channelName: string;
  guildId: string;
  lastReadMessageId: string;
}

interface SlackChannelState {
  channelId: string;
  channelName: string;
  lastReadTs: string;
}

interface SessionState {
  sessionId: string;
  userId?: string;
  monitoredChannels: Record<string, ChannelState>;
  monitoredDiscordChannels: Record<string, DiscordChannelState>;
  monitoredSlackChannels?: Record<string, SlackChannelState>;
}

let sessionId: string = "";
const monitoredChannels: Map<string, ChannelState> = new Map();
const monitoredDiscordChannels: Map<string, DiscordChannelState> = new Map();
const monitoredSlackChannels: Map<string, SlackChannelState> = new Map();

// File system for persistent state
import { readFileSync, writeFileSync, existsSync, mkdirSync } from "fs";
import { join } from "path";
import { homedir } from "os";

const STATE_DIR = join(homedir(), ".zulip_mcp_state");
const getStateFile = (sessionId: string) => join(STATE_DIR, `${sessionId}.json`);

function loadState(sessionId: string): void {
  const stateFile = getStateFile(sessionId);
  if (existsSync(stateFile)) {
    try {
      const data = JSON.parse(readFileSync(stateFile, "utf-8")) as SessionState;
      monitoredChannels.clear();
      monitoredDiscordChannels.clear();
      monitoredSlackChannels.clear();

      if (data.monitoredChannels) {
        Object.values(data.monitoredChannels).forEach(channel => {
          monitoredChannels.set(channel.channelName, channel);
        });
      }

      if (data.monitoredDiscordChannels) {
        Object.values(data.monitoredDiscordChannels).forEach(channel => {
          monitoredDiscordChannels.set(channel.channelId, channel);
        });
      }

      if (data.monitoredSlackChannels) {
        Object.values(data.monitoredSlackChannels).forEach(channel => {
          monitoredSlackChannels.set(channel.channelId, channel);
        });
      }

      console.error(`Loaded state for session ${sessionId}: ${monitoredChannels.size} Zulip, ${monitoredDiscordChannels.size} Discord, ${monitoredSlackChannels.size} Slack channels`);
    } catch (error) {
      console.error(`Failed to load state: ${error}`);
    }
  }
}

function saveState(): void {
  if (!sessionId) return;
  
  try {
    if (!existsSync(STATE_DIR)) {
      mkdirSync(STATE_DIR, { recursive: true });
    }
    
    const state: SessionState = {
      sessionId,
      monitoredChannels: Object.fromEntries(monitoredChannels),
      monitoredDiscordChannels: Object.fromEntries(monitoredDiscordChannels),
      monitoredSlackChannels: Object.fromEntries(monitoredSlackChannels),
    };
    
    writeFileSync(getStateFile(sessionId), JSON.stringify(state, null, 2));
  } catch (error) {
    console.error(`Failed to save state: ${error}`);
  }
}

async function initializeZulipClient(): Promise<void> {
  const config: any = {
    realm: process.env.ZULIP_REALM || "",
  };

  if (process.env.ZULIP_RC_PATH) {
    config.zuliprc = process.env.ZULIP_RC_PATH;
  } else {
    config.username = process.env.ZULIP_USERNAME || process.env.ZULIP_EMAIL;
    config.apiKey = process.env.ZULIP_API_KEY;
    config.password = process.env.ZULIP_PASSWORD;
  }

  if (!config.realm && !config.zuliprc) {
    throw new Error(
      "ZULIP_REALM must be set (or provide ZULIP_RC_PATH for zuliprc file)"
    );
  }

  if (!config.zuliprc && !config.username) {
    throw new Error(
      "ZULIP_USERNAME/ZULIP_EMAIL must be set (or provide ZULIP_RC_PATH)"
    );
  }

  if (!config.zuliprc && !config.apiKey && !config.password) {
    throw new Error(
      "Either ZULIP_API_KEY or ZULIP_PASSWORD must be set (or provide ZULIP_RC_PATH)"
    );
  }

  zulipClient = await zulipInit(config);

  // Capture realm + auth for direct fetches (zulip-js doesn't expose user_uploads).
  // Preference order: resolved client config, input config, env vars, zuliprc file.
  const resolved = (zulipClient && zulipClient.config) || config;
  let realm = (resolved.realm || config.realm || "").replace(/\/+$/, "");
  let email = resolved.username || config.username || process.env.ZULIP_EMAIL || process.env.ZULIP_USERNAME || "";
  let apiKey = resolved.apiKey || config.apiKey || process.env.ZULIP_API_KEY || "";

  // Final fallback: parse the zuliprc file directly if any field is still missing.
  if ((!realm || !email || !apiKey) && config.zuliprc) {
    try {
      const raw = readFileSync(config.zuliprc, "utf-8");
      const parsed: Record<string, string> = {};
      for (const line of raw.split(/\r?\n/)) {
        const m = line.match(/^\s*(email|key|site)\s*=\s*(.+?)\s*$/);
        if (m) parsed[m[1]] = m[2];
      }
      if (!realm && parsed.site) realm = parsed.site.replace(/\/+$/, "");
      if (!email && parsed.email) email = parsed.email;
      if (!apiKey && parsed.key) apiKey = parsed.key;
    } catch (err) {
      console.error("Failed to parse zuliprc for direct-HTTP credentials:", err);
    }
  }

  zulipRealm = realm;
  if (email && apiKey) {
    zulipAuthHeader = "Basic " + Buffer.from(`${email}:${apiKey}`).toString("base64");
  }

  // Set up session ID and load persistent state
  sessionId = process.env.ZULIP_SESSION_ID || process.env.ZULIP_EMAIL || process.env.ZULIP_USERNAME || "default";
  loadState(sessionId);

  // Fail-open: if profile fetch fails, leave zulipSelfUserId null (no self-filter).
  try {
    const profile = await zulipClient.users.me.getProfile();
    if (profile && typeof profile.user_id === "number") {
      zulipSelfUserId = profile.user_id;
      console.error(`Zulip MCP bot user_id: ${zulipSelfUserId}`);
    }
  } catch (err) {
    console.error("Failed to fetch bot profile for self-filter:", err);
  }

  // Auto-subscribe to streams named in ZULIP_SUBSCRIBE (comma-separated).
  // Needed because Zulip's event queue only delivers message events for streams
  // the bot is subscribed to, even with all_public_streams: true on the queue.
  if (process.env.ZULIP_SUBSCRIBE) {
    const streams = process.env.ZULIP_SUBSCRIBE.split(",").map(s => s.trim()).filter(Boolean);
    if (streams.length > 0) {
      try {
        const result = await zulipClient.users.me.subscriptions.add({
          subscriptions: streams.map(name => ({ name })),
        });
        const subscribed = result?.subscribed ?? {};
        const already = result?.already_subscribed ?? {};
        console.error(`Zulip MCP auto-subscribed: new=${JSON.stringify(subscribed)} already=${JSON.stringify(already)}`);
      } catch (err) {
        console.error(`Zulip MCP auto-subscribe failed for [${streams.join(", ")}]:`, err);
      }
    }
  }

  console.error(`Zulip MCP initialized with session: ${sessionId}`);
}

async function initializeSlackClient(): Promise<void> {
  const botToken = process.env.SLACK_BOT_TOKEN;
  const appToken = process.env.SLACK_APP_TOKEN;
  if (!botToken) {
    throw new Error("SLACK_BOT_TOKEN must be set (bot token, xoxb-...)");
  }
  if (!appToken) {
    throw new Error("SLACK_APP_TOKEN must be set (Socket Mode app-level token, xapp-...)");
  }

  slackWebClient = new SlackWebClient(botToken);

  // Resolve bot identity for self-filtering and workspace labeling.
  const auth = await slackWebClient.auth.test();
  slackSelfUserId = (auth.user_id as string) ?? null;
  slackTeamName = (auth.team as string) ?? "";

  // Socket Mode client — started later by the adapter (MCPL mode only);
  // plain MCP mode uses the Web API exclusively.
  slackSocketClient = new SlackSocketModeClient({ appToken });

  // Set up session ID and load persistent state (Zulip init may have done
  // this already; only fill in when Slack is the first/only platform).
  if (!sessionId) {
    sessionId = process.env.SLACK_SESSION_ID || slackTeamName || "default";
    loadState(sessionId);
  }

  console.error(`Slack MCP initialized: bot ${auth.user} (${slackSelfUserId}) in ${slackTeamName}`);
}

async function initializeDiscordClient(): Promise<void> {
  if (!process.env.DISCORD_TOKEN) {
    throw new Error("DISCORD_TOKEN must be set");
  }
  
  discordClient = new DiscordClient({
    intents: [
      GatewayIntentBits.Guilds,
      GatewayIntentBits.GuildMessages,
      GatewayIntentBits.MessageContent,
      GatewayIntentBits.GuildMembers, // Required for user search
    ],
  });
  
  await discordClient.login(process.env.DISCORD_TOKEN);
  
  // Wait for ready
  await new Promise<void>((resolve) => {
    discordClient!.once('ready', () => {
      console.error(`Discord connected as ${discordClient!.user?.tag}`);
      resolve();
    });
  });
}

// Helper function to parse date strings
function parseDate(dateStr: string | undefined, defaultDate: Date): Date {
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

// Helper function to format Discord messages
export function formatDiscordMessages(messages: any[], format: string): string {
  if (format === 'raw') {
    return JSON.stringify(messages, null, 2);
  }
  
  if (format === 'summary') {
    const summary = messages.map(msg => {
      const time = new Date(msg.timestamp * 1000).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
      const content = msg.content.substring(0, 80);
      const replyIndicator = msg.reply_to ? '↩️ ' : '';
      return `[${time}] ${replyIndicator}${msg.author}: ${content}...`;
    }).join('\n');
    return `📊 ${messages.length} messages\n\n${summary}`;
  }
  
  // Detailed format
  const formatted = messages.map(msg => {
    const time = new Date(msg.timestamp * 1000).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
    const date = new Date(msg.timestamp * 1000).toLocaleDateString('en-US');
    const replyInfo = msg.reply_to ? `\n↩️ Replying to message ID: ${msg.reply_to}` : '';
    const attachmentInfo = msg.attachments > 0 ? `\n📎 ${msg.attachments} attachment(s)` : '';
    
    return `[${date} ${time}] 💬 ${msg.channel ? `#${msg.channel}` : ''}${replyInfo}\n👤 ${msg.author}\n💬 ${msg.content}${attachmentInfo}\n`;
  }).join('\n' + '─'.repeat(80) + '\n\n');
  
  return `📊 Retrieved ${messages.length} messages\n${'='.repeat(80)}\n\n${formatted}`;
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

// Slack user-name cache for the tool layer (the MCPL adapter keeps its own
// cache; the resolution logic is shared via content.ts).
const slackUserNames = new Map<string, string>();

async function resolveSlackUsers(userIds: string[]): Promise<void> {
  if (!slackWebClient) return;
  await resolveSlackUserNames(slackWebClient, slackUserNames, userIds);
}

interface ShapedSlackAttachment {
  name: string;
  mimetype?: string;
  url_private?: string;
}

interface ShapedSlackMessage {
  ts?: string;
  author: string;
  content: string;
  timestamp: number;
  thread_ts?: string;
  attachments: ShapedSlackAttachment[];
  channel?: string;
}

/** Resolve author + mention names for raw Slack history messages and shape
 * them for formatSlackMessages. */
async function shapeSlackMessages(messages: SlackHistoryMessage[], channelName?: string): Promise<ShapedSlackMessage[]> {
  const ids = new Set<string>();
  for (const msg of messages) {
    if (msg.user) ids.add(msg.user);
    for (const id of extractSlackUserIds(msg.text ?? '')) ids.add(id);
  }
  await resolveSlackUsers(Array.from(ids));

  return messages.map(msg => ({
    ts: msg.ts,
    author: msg.user ? (slackUserNames.get(msg.user) ?? msg.user) : (msg.username ?? 'bot'),
    content: formatSlackText(msg.text ?? '', slackUserNames),
    timestamp: Math.floor(parseFloat(msg.ts ?? '0')),
    thread_ts: msg.thread_ts && msg.thread_ts !== msg.ts ? msg.thread_ts : undefined,
    attachments: (msg.files ?? []).map(f => ({
      name: f.name ?? 'attachment',
      mimetype: f.mimetype,
      url_private: f.url_private,
    })),
    channel: channelName,
  }));
}

// Helper function to format Slack messages (shaped by shapeSlackMessages)
function formatSlackMessages(messages: ShapedSlackMessage[], format: string): string {
  if (format === 'raw') {
    return JSON.stringify(messages, null, 2);
  }

  if (format === 'summary') {
    const summary = messages.map(msg => {
      const time = new Date(msg.timestamp * 1000).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
      const content = msg.content.length > 80 ? `${msg.content.substring(0, 80)}…` : msg.content;
      const threadIndicator = msg.thread_ts ? '🧵 ' : '';
      return `[${time}] ${threadIndicator}${msg.author}: ${content}`;
    }).join('\n');
    return `📊 ${messages.length} messages\n\n${summary}`;
  }

  // Detailed format
  const formatted = messages.map(msg => {
    const time = new Date(msg.timestamp * 1000).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
    const date = new Date(msg.timestamp * 1000).toLocaleDateString('en-US');
    const threadInfo = msg.thread_ts ? `\n🧵 In thread: ${msg.thread_ts}` : '';
    const attachmentInfo = msg.attachments.length > 0
      ? '\n' + msg.attachments.map(a =>
          `📎 ${a.name}${a.mimetype ? ` (${a.mimetype})` : ''}${a.url_private ? ` — fetchable via slack_fetch_attachment: ${a.url_private}` : ''}`,
        ).join('\n')
      : '';

    return `[${date} ${time}] 💬 ${msg.channel ? `#${msg.channel}` : ''} (ts: ${msg.ts})${threadInfo}\n👤 ${msg.author}\n💬 ${msg.content}${attachmentInfo}\n`;
  }).join('\n' + '─'.repeat(80) + '\n\n');

  return `📊 Retrieved ${messages.length} messages\n${'='.repeat(80)}\n\n${formatted}`;
}

// Define available tools (dynamically based on enabled services)
function getTools(): Tool[] {
  const tools: Tool[] = [];
  
  // Zulip tools
  if (ENABLE_ZULIP) {
    tools.push({
      name: "start_monitoring",
    description:
      "Start monitoring one or more channels. This enables tracking of read/unread messages and allows efficient message retrieval.",
    inputSchema: {
      type: "object",
      properties: {
        channels: {
          type: "array",
          items: { type: "string" },
          description: "Channel names to start monitoring (e.g., ['analysts', 'general'])",
        },
      },
      required: ["channels"],
    },
  },
  {
    name: "stop_monitoring",
    description:
      "Stop monitoring one or more channels.",
    inputSchema: {
      type: "object",
      properties: {
        channels: {
          type: "array",
          items: { type: "string" },
          description: "Channel names to stop monitoring. If not provided, stops all.",
        },
      },
    },
  },
  {
    name: "listen",
    description:
      "Subscribe the bot to one or more Zulip streams. Required before the bot can receive real-time message events from a stream — event queues with all_public_streams only deliver events for streams the bot is subscribed to. Subscription persists server-side across session restarts.",
    inputSchema: {
      type: "object",
      properties: {
        channels: {
          type: "array",
          items: { type: "string" },
          description: "Stream names to subscribe to (e.g., ['tracker-miner-f', 'infra']).",
        },
      },
      required: ["channels"],
    },
  },
  {
    name: "unlisten",
    description:
      "Unsubscribe the bot from one or more Zulip streams. After this, the bot stops receiving real-time events for those streams until listen is called again. Unsubscription persists server-side.",
    inputSchema: {
      type: "object",
      properties: {
        channels: {
          type: "array",
          items: { type: "string" },
          description: "Stream names to unsubscribe from.",
        },
      },
      required: ["channels"],
    },
  },
  {
    name: "get_monitored_channels",
    description:
      "List all currently monitored channels and their state.",
    inputSchema: {
      type: "object",
      properties: {},
    },
  },
  {
    name: "get_channel_history",
    description:
      "Get message history from a channel with convenient date/time filtering. Returns formatted, readable messages from the specified time range.",
    inputSchema: {
      type: "object",
      properties: {
        channel: {
          type: "string",
          description: "Channel/stream name (e.g., 'analysts', 'general')",
        },
        topic: {
          type: "string",
          description: "Optional: filter to specific topic within the channel",
        },
        start_date: {
          type: "string",
          description: "Start date/time in ISO format or 'today', 'yesterday' (e.g., '2025-11-03', '2025-11-03T09:00:00'). Defaults to start of today.",
        },
        end_date: {
          type: "string",
          description: "End date/time in ISO format or 'now' (e.g., '2025-11-03T17:00:00'). Defaults to now.",
        },
        max_messages: {
          type: "number",
          description: "Maximum number of messages to retrieve (default: 500)",
          default: 500,
        },
        format: {
          type: "string",
          enum: ["detailed", "summary", "raw"],
          description: "Output format: 'detailed' (full formatted), 'summary' (brief), 'raw' (JSON)",
          default: "detailed",
        },
        auto_monitor: {
          type: "boolean",
          description: "Automatically start monitoring this channel and mark messages as read (default: true)",
          default: true,
        },
      },
      required: ["channel"],
    },
  },
  {
    name: "get_unread_messages",
    description:
      "Get unread messages from monitored channels. Only works for channels you're actively monitoring.",
    inputSchema: {
      type: "object",
      properties: {
        channels: {
          type: "array",
          items: { type: "string" },
          description: "Optional: specific channels to check. If not provided, checks all monitored channels.",
        },
        format: {
          type: "string",
          enum: ["detailed", "summary", "raw"],
          description: "Output format: 'detailed', 'summary', or 'raw'",
          default: "detailed",
        },
        mark_as_read: {
          type: "boolean",
          description: "Mark messages as read after retrieving (default: true)",
          default: true,
        },
      },
    },
  },
  {
    name: "send_message",
    description:
      "Send a message to a stream or as a direct message. For streams, provide 'stream' and 'topic'. For DMs, provide 'to' as user email(s).",
    inputSchema: {
      type: "object",
      properties: {
        type: {
          type: "string",
          enum: ["stream", "private"],
          description: "Type of message: 'stream' for stream messages, 'private' for direct messages",
        },
        to: {
          type: "string",
          description: "For stream: stream name. For private: comma-separated email addresses",
        },
        topic: {
          type: "string",
          description: "Topic for stream messages (required for type='stream')",
        },
        content: {
          type: "string",
          description: "The message content (supports Markdown)",
        },
      },
      required: ["type", "to", "content"],
    },
  },
  {
    name: "edit_message",
    description: "Edit the content of an existing Zulip message by ID. You can edit your own messages (subject to the realm's message-edit time limit; an expired window returns an error). Useful for maintaining a live status message: post once with send_message, then update it in place instead of flooding the topic.",
    inputSchema: {
      type: "object",
      properties: {
        message_id: {
          type: "number",
          description: "ID of the message to edit",
        },
        content: {
          type: "string",
          description: "The new message content (supports Markdown, replaces the old content entirely)",
        },
      },
      required: ["message_id", "content"],
    },
  },
  {
    name: "delete_message",
    description: "Delete a Zulip message by ID. You can delete your own messages, and if you have permissions, others' messages too.",
    inputSchema: {
      type: "object",
      properties: {
        message_id: {
          type: "number",
          description: "ID of the message to delete",
        },
      },
      required: ["message_id"],
    },
  },
  {
    name: "list_streams",
    description: "Get all streams in the Zulip organization",
    inputSchema: {
      type: "object",
      properties: {
        include_public: {
          type: "boolean",
          description: "Include public streams",
          default: true,
        },
        include_subscribed: {
          type: "boolean",
          description: "Include subscribed streams",
          default: true,
        },
        verbose: {
          type: "boolean",
          description: "Include full raw API response (warning: very large)",
          default: false,
        },
      },
    },
  },
  {
    name: "get_stream_topics",
    description: "Get all topics in a specific stream",
    inputSchema: {
      type: "object",
      properties: {
        stream_id: {
          type: "number",
          description: "ID of the stream",
        },
        verbose: {
          type: "boolean",
          description: "Include full raw API response (warning: very large)",
          default: false,
        },
      },
      required: ["stream_id"],
    },
  },
  {
    name: "list_users",
    description: "Get all users in the Zulip organization",
    inputSchema: {
      type: "object",
      properties: {
        client_gravatar: {
          type: "boolean",
          description: "Whether to include gravatar URLs",
          default: false,
        },
        verbose: {
          type: "boolean",
          description: "Include full raw API response (warning: very large)",
          default: false,
        },
      },
    },
  },
  {
    name: "get_user_profile",
    description: "Get the profile of the authenticated user/bot",
    inputSchema: {
      type: "object",
      properties: {},
    },
  },
  {
    name: "add_reaction",
    description: "Add an emoji reaction to a message",
    inputSchema: {
      type: "object",
      properties: {
        message_id: {
          type: "number",
          description: "ID of the message",
        },
        emoji_name: {
          type: "string",
          description: "Name of the emoji (e.g., 'thumbs_up', 'heart', 'rocket')",
        },
      },
      required: ["message_id", "emoji_name"],
    },
  },
  {
    name: "find_user",
    description: "Find a Zulip user by name or email to get their ID for mentions. Use @**username** format in messages to mention.",
    inputSchema: {
      type: "object",
      properties: {
        query: {
          type: "string",
          description: "Name or email to search for",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "fetch_attachment",
    description:
      "Fetch a Zulip user-upload attachment by path and return its bytes inline. " +
      "Images (png/jpg/jpeg/gif/webp) return as an image content block usable by vision models. " +
      "Text-ish MIME types (text/*, JSON, CSV, YAML) return as `content_text` so the model can read directly. " +
      "Other binaries return as `base64`. Paths look like '/user_uploads/X/Yy/Zz/name.ext' and appear in " +
      "incoming message attachment refs. Only paths under /user_uploads/ on the configured realm are allowed.",
    inputSchema: {
      type: "object",
      properties: {
        path: {
          type: "string",
          description: "Attachment path (e.g. '/user_uploads/2/Ab/cd/screenshot.png') or full Zulip URL.",
        },
      },
      required: ["path"],
    },
    });
  }
  
  // Discord tools
  if (ENABLE_DISCORD) {
    tools.push({
      name: "discord_find_user",
    description: "Find a Discord user by username in guilds. Returns user ID for mentions. Use <@user_id> format in messages.",
    inputSchema: {
      type: "object",
      properties: {
        username: {
          type: "string",
          description: "Username to search for (can include discriminator like 'user#1234')",
        },
        guild_id: {
          type: "string",
          description: "Optional: limit search to specific guild",
        },
      },
      required: ["username"],
    },
  },
  {
    name: "discord_start_monitoring",
    description:
      "Start monitoring Discord channels. Enables tracking of read/unread messages.",
    inputSchema: {
      type: "object",
      properties: {
        channel_ids: {
          type: "array",
          items: { type: "string" },
          description: "Discord channel IDs to monitor (e.g., ['1234567890', '0987654321'])",
        },
      },
      required: ["channel_ids"],
    },
  },
  {
    name: "discord_get_channel_history",
    description:
      "Get Discord channel message history with date/time filtering. Auto-monitors by default.",
    inputSchema: {
      type: "object",
      properties: {
        channel_id: {
          type: "string",
          description: "Discord channel ID",
        },
        start_date: {
          type: "string",
          description: "Start date/time ('today', 'yesterday', or ISO format). Defaults to today.",
        },
        end_date: {
          type: "string",
          description: "End date/time ('now' or ISO format). Defaults to now.",
        },
        max_messages: {
          type: "number",
          description: "Maximum messages to retrieve (default: 100, max: 100)",
          default: 100,
        },
        format: {
          type: "string",
          enum: ["detailed", "summary", "raw"],
          description: "Output format",
          default: "detailed",
        },
        auto_monitor: {
          type: "boolean",
          description: "Auto-start monitoring and mark as read (default: true)",
          default: true,
        },
      },
      required: ["channel_id"],
    },
  },
  {
    name: "discord_get_unread_messages",
    description:
      "Get unread Discord messages from monitored channels.",
    inputSchema: {
      type: "object",
      properties: {
        channel_ids: {
          type: "array",
          items: { type: "string" },
          description: "Specific channels to check (optional, defaults to all monitored)",
        },
        format: {
          type: "string",
          enum: ["detailed", "summary", "raw"],
          description: "Output format",
          default: "detailed",
        },
        mark_as_read: {
          type: "boolean",
          description: "Mark as read after retrieving (default: true)",
          default: true,
        },
      },
    },
  },
  {
    name: "discord_send_message",
    description: "Send a message to a Discord channel. Optionally reply to another message.",
    inputSchema: {
      type: "object",
      properties: {
        channel_id: {
          type: "string",
          description: "Discord channel ID",
        },
        content: {
          type: "string",
          description: "Message content (supports Discord markdown)",
        },
        reply_to: {
          type: "string",
          description: "Optional: Message ID to reply to",
        },
      },
      required: ["channel_id", "content"],
    },
  },
  {
    name: "discord_delete_message",
    description: "Delete a Discord message by ID. Requires appropriate permissions.",
    inputSchema: {
      type: "object",
      properties: {
        channel_id: {
          type: "string",
          description: "Discord channel ID",
        },
        message_id: {
          type: "string",
          description: "Message ID to delete",
        },
      },
      required: ["channel_id", "message_id"],
    },
  },
  {
    name: "discord_list_channels",
    description: "List all Discord channels (optionally filtered by guild)",
    inputSchema: {
      type: "object",
      properties: {
        guild_id: {
          type: "string",
          description: "Optional: filter to specific guild/server",
        },
      },
    },
  },
  {
    name: "discord_get_monitored_channels",
    description: "List all monitored Discord channels and their state",
    inputSchema: {
      type: "object",
      properties: {},
    },
  },
  {
    name: "discord_stop_monitoring",
    description: "Stop monitoring Discord channels",
    inputSchema: {
      type: "object",
      properties: {
        channel_ids: {
          type: "array",
          items: { type: "string" },
          description: "Channel IDs to stop monitoring (optional, stops all if not provided)",
        },
      },
    },
  },
  {
    name: "discord_fetch_attachment",
    description:
      "Fetch a Discord attachment by URL and return its bytes inline. " +
      "Images return as an image content block usable by vision models. " +
      "Text-ish MIME types return as `content_text`; other binaries return as `base64`. " +
      "URLs come from incoming Discord message attachment refs and must be on the Discord CDN " +
      "(cdn.discordapp.com / media.discordapp.net); other hosts are refused.",
    inputSchema: {
      type: "object",
      properties: {
        url: {
          type: "string",
          description: "Full Discord CDN URL of the attachment.",
        },
      },
      required: ["url"],
    },
    });
  }

  // Slack tools
  if (ENABLE_SLACK) {
    tools.push({
      name: "slack_list_channels",
      description: "List Slack conversations: channels, private channels, DMs, and group DMs. Returns conversation IDs used by other slack_ tools.",
      inputSchema: {
        type: "object",
        properties: {
          types: {
            type: "string",
            description: "Comma-separated conversation types (default: 'public_channel,private_channel,im,mpim')",
          },
        },
      },
    },
    {
      name: "slack_start_monitoring",
      description: "Start monitoring Slack conversations. Enables tracking of read/unread messages.",
      inputSchema: {
        type: "object",
        properties: {
          channel_ids: {
            type: "array",
            items: { type: "string" },
            description: "Slack conversation IDs to monitor (e.g., ['C0123ABC', 'D0456DEF'])",
          },
        },
        required: ["channel_ids"],
      },
    },
    {
      name: "slack_get_channel_history",
      description: "Get Slack conversation history with date/time filtering. Auto-monitors by default. Note: covers channel-level messages only — replies inside threads are not returned (Slack requires conversations.replies per thread), so an active thread can look quiet here.",
      inputSchema: {
        type: "object",
        properties: {
          channel_id: {
            type: "string",
            description: "Slack conversation ID (C… channel, D… DM, G… group)",
          },
          start_date: {
            type: "string",
            description: "Start date/time ('today', 'yesterday', or ISO format). Defaults to today.",
          },
          end_date: {
            type: "string",
            description: "End date/time ('now' or ISO format). Defaults to now.",
          },
          max_messages: {
            type: "number",
            description: "Maximum messages to retrieve (default: 100, max: 200)",
            default: 100,
          },
          format: {
            type: "string",
            enum: ["detailed", "summary", "raw"],
            description: "Output format",
            default: "detailed",
          },
          auto_monitor: {
            type: "boolean",
            description: "Auto-start monitoring and mark as read (default: true)",
            default: true,
          },
        },
        required: ["channel_id"],
      },
    },
    {
      name: "slack_get_unread_messages",
      description: "Get unread Slack messages from monitored conversations. Note: covers channel-level messages only — replies inside threads are not returned (Slack requires conversations.replies per thread), so an active thread can look quiet here.",
      inputSchema: {
        type: "object",
        properties: {
          channel_ids: {
            type: "array",
            items: { type: "string" },
            description: "Specific conversations to check (optional, defaults to all monitored)",
          },
          format: {
            type: "string",
            enum: ["detailed", "summary", "raw"],
            description: "Output format",
            default: "detailed",
          },
          mark_as_read: {
            type: "boolean",
            description: "Mark as read after retrieving (default: true)",
            default: true,
          },
        },
      },
    },
    {
      name: "slack_send_message",
      description: "Send a message to a Slack conversation (channel or DM). Use <@user_id> for mentions. Optionally reply in a thread via thread_ts.",
      inputSchema: {
        type: "object",
        properties: {
          channel_id: {
            type: "string",
            description: "Slack conversation ID",
          },
          content: {
            type: "string",
            description: "Message text (supports Slack mrkdwn)",
          },
          thread_ts: {
            type: "string",
            description: "Optional: thread timestamp to reply in (the ts of the thread's parent message)",
          },
        },
        required: ["channel_id", "content"],
      },
    },
    {
      name: "slack_delete_message",
      description: "Delete a Slack message by channel and ts. Requires appropriate permissions.",
      inputSchema: {
        type: "object",
        properties: {
          channel_id: {
            type: "string",
            description: "Slack conversation ID",
          },
          message_ts: {
            type: "string",
            description: "Timestamp (ts) of the message to delete",
          },
        },
        required: ["channel_id", "message_ts"],
      },
    },
    {
      name: "slack_add_reaction",
      description: "Add an emoji reaction to a Slack message.",
      inputSchema: {
        type: "object",
        properties: {
          channel_id: {
            type: "string",
            description: "Slack conversation ID",
          },
          message_ts: {
            type: "string",
            description: "Timestamp (ts) of the message to react to",
          },
          emoji_name: {
            type: "string",
            description: "Emoji name without colons (e.g., 'thumbsup')",
          },
        },
        required: ["channel_id", "message_ts", "emoji_name"],
      },
    },
    {
      name: "slack_find_user",
      description: "Find a Slack user by name. Returns user ID for mentions. Use <@user_id> format in messages.",
      inputSchema: {
        type: "object",
        properties: {
          query: {
            type: "string",
            description: "Name to search for (matches username, real name, or display name)",
          },
        },
        required: ["query"],
      },
    },
    {
      name: "slack_get_monitored_channels",
      description: "List all monitored Slack conversations and their state",
      inputSchema: {
        type: "object",
        properties: {},
      },
    },
    {
      name: "slack_stop_monitoring",
      description: "Stop monitoring Slack conversations",
      inputSchema: {
        type: "object",
        properties: {
          channel_ids: {
            type: "array",
            items: { type: "string" },
            description: "Conversation IDs to stop monitoring (optional, stops all if not provided)",
          },
        },
      },
    },
    {
      name: "slack_fetch_attachment",
      description:
        "Fetch a Slack file attachment by URL and return its bytes inline. " +
        "Images return as an image content block usable by vision models. " +
        "Text-ish MIME types return as `content_text`; other binaries return as `base64`. " +
        "URLs come from incoming Slack message attachment refs (url_private, fetched with bot auth).",
      inputSchema: {
        type: "object",
        properties: {
          url: {
            type: "string",
            description: "Full url_private of the Slack file.",
          },
        },
        required: ["url"],
      },
    });
  }

  return tools;
}

// Handle tool execution
async function handleToolCall(name: string, args: any): Promise<any> {
  // Check if the required client is initialized
  const isDiscordTool = name.startsWith("discord_");
  const isSlackTool = name.startsWith("slack_");
  const isZulipTool = !isDiscordTool && !isSlackTool;

  if (isZulipTool && !zulipClient) {
    throw new Error("Zulip client not initialized. Set ENABLE_ZULIP=true");
  }

  if (isDiscordTool && !discordClient) {
    throw new Error("Discord client not initialized. Set ENABLE_DISCORD=true");
  }

  if (isSlackTool && !slackWebClient) {
    throw new Error("Slack client not initialized. Set ENABLE_SLACK=true");
  }

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
      
      saveState();
      
      return {
        session_id: sessionId,
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
        saveState();
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
      
      saveState();
      
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
          saveState();
        } else {
          // Update last read
          const state = monitoredChannels.get(args.channel)!;
          state.lastReadMessageId = latestMessageId;
          monitoringStatus = "updated_read_position";
          saveState();
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
        saveState();
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
      const url = parseZulipAttachmentUrl(String(args.path || ""), zulipRealm);

      const headers: Record<string, string> = {};
      if (zulipAuthHeader) headers["Authorization"] = zulipAuthHeader;
      const name = decodeURIComponent(url.pathname.split("/").pop() || "attachment");
      return toFetchResult(await fetchAttachmentBytes(url.toString(), name, { headers }));
    }

    case "discord_fetch_attachment": {
      // No credentials attached, but the URL comes from untrusted message
      // content — host allowlisting lives in parseDiscordAttachmentUrl.
      const url = parseDiscordAttachmentUrl(String(args.url || ""));
      const name = decodeURIComponent(url.pathname.split("/").pop() || "attachment");
      return toFetchResult(await fetchAttachmentBytes(url.toString(), name));
    }

    // Discord handlers
    case "discord_find_user": {
      if (!discordClient) {
        throw new Error("Discord is not enabled");
      }
      
      const username = args.username.toLowerCase();
      const guildFilter = args.guild_id;
      const matches: any[] = [];
      const seenUserIds = new Set<string>();
      
      for (const guild of discordClient.guilds.cache.values()) {
        if (guildFilter && guild.id !== guildFilter) continue;
        
        try {
          // Use Discord's search API with the query parameter (efficient)
          const searchResults = await guild.members.fetch({ 
            query: args.username, 
            limit: 25 
          });
          
          for (const member of searchResults.values()) {
            const user = member.user;
            if (seenUserIds.has(user.id)) continue;
            
            matches.push({
              user_id: user.id,
              username: user.username,
              tag: user.tag,
              display_name: member.displayName,
              guild_id: guild.id,
              guild_name: guild.name,
              mention_syntax: `<@${user.id}>`,
            });
            seenUserIds.add(user.id);
          }
        } catch (error) {
          // If search fails, check cache as fallback
          for (const member of guild.members.cache.values()) {
            const user = member.user;
            if (seenUserIds.has(user.id)) continue;
            
            if (
              user.username.toLowerCase().includes(username) ||
              user.tag.toLowerCase().includes(username) ||
              (member.displayName && member.displayName.toLowerCase().includes(username))
            ) {
              matches.push({
                user_id: user.id,
                username: user.username,
                tag: user.tag,
                display_name: member.displayName,
                guild_id: guild.id,
                guild_name: guild.name,
                mention_syntax: `<@${user.id}>`,
              });
              seenUserIds.add(user.id);
            }
          }
        }
      }
      
      if (matches.length === 0) {
        return {
          found: false,
          message: `No users found matching "${args.username}".`,
        };
      }
      
      const formatted = matches.map(user => 
        `👤 **${user.tag}** (${user.guild_name})\n   └─ User ID: ${user.user_id}\n   └─ Mention format: <@${user.user_id}>`
      ).join("\n\n");
      
      return {
        found: true,
        match_count: matches.length,
        formatted_list: `👥 Found ${matches.length} user${matches.length !== 1 ? 's' : ''}:\n\n${formatted}`,
        users: matches,
      };
    }
    case "discord_start_monitoring": {
      if (!discordClient) {
        throw new Error("Discord is not enabled. Set ENABLE_DISCORD=true");
      }
      
      const channelIds: string[] = args.channel_ids;
      const results: any[] = [];
      
      for (const channelId of channelIds) {
        try {
          const channel = await discordClient.channels.fetch(channelId);
          if (!channel || !(channel instanceof TextChannel)) {
            results.push({
              channel_id: channelId,
              status: "error",
              error: "Not a text channel or not found",
            });
            continue;
          }
          
          // Get latest message
          const messages = await channel.messages.fetch({ limit: 1 });
          const lastMessageId = messages.size > 0 ? messages.first()!.id : "0";
          
          monitoredDiscordChannels.set(channelId, {
            channelId,
            channelName: channel.name,
            guildId: channel.guildId,
            lastReadMessageId: lastMessageId,
          });
          
          results.push({
            channel_id: channelId,
            channel_name: channel.name,
            guild_name: channel.guild.name,
            status: "monitoring",
            last_message_id: lastMessageId,
          });
        } catch (error) {
          results.push({
            channel_id: channelId,
            status: "error",
            error: error instanceof Error ? error.message : String(error),
          });
        }
      }
      
      saveState();
      
      return {
        session_id: sessionId,
        monitored_count: monitoredDiscordChannels.size,
        channels: results,
      };
    }

    case "discord_get_channel_history": {
      if (!discordClient) {
        throw new Error("Discord is not enabled. Set ENABLE_DISCORD=true");
      }
      
      const channelId = args.channel_id;
      const channel = await discordClient.channels.fetch(channelId);
      
      if (!channel || !(channel instanceof TextChannel)) {
        throw new Error("Channel not found or not a text channel");
      }
      
      // Parse dates
      const today = new Date();
      today.setHours(0, 0, 0, 0);
      const now = new Date();
      
      const startDate = parseDate(args.start_date, today);
      const endDate = parseDate(args.end_date, now);
      
      // Fetch messages (Discord limit is 100 per request)
      const maxMessages = Math.min(args.max_messages || 100, 100);
      const fetchedMessages = await channel.messages.fetch({ limit: maxMessages });
      
      // Filter by date range
      const filteredMessages = Array.from(fetchedMessages.values())
        .filter(msg => {
          const msgTime = msg.createdAt;
          return msgTime >= startDate && msgTime <= endDate;
        })
        .reverse(); // Oldest first
      
      // Auto-monitor
      const autoMonitor = args.auto_monitor !== false;
      let monitoringStatus = "not_monitored";
      
      if (autoMonitor && filteredMessages.length > 0) {
        const latestMessageId = filteredMessages[filteredMessages.length - 1].id;
        
        if (!monitoredDiscordChannels.has(channelId)) {
          monitoredDiscordChannels.set(channelId, {
            channelId,
            channelName: channel.name,
            guildId: channel.guildId,
            lastReadMessageId: latestMessageId,
          });
          monitoringStatus = "started_monitoring";
          saveState();
        } else {
          const state = monitoredDiscordChannels.get(channelId)!;
          state.lastReadMessageId = latestMessageId;
          monitoringStatus = "updated_read_position";
          saveState();
        }
      }
      
      // Format messages
      const format = args.format || "detailed";
      const formattedMessages = filteredMessages.map(msg => ({
        id: msg.id,
        author: msg.author.tag,
        content: formatDiscordContent(msg.content, msg.mentions),
        timestamp: Math.floor(msg.createdTimestamp / 1000),
        attachments: msg.attachments.size,
        reply_to: msg.reference?.messageId,
      }));
      
      const formattedOutput = formatDiscordMessages(formattedMessages, format);
      
      return {
        channel_id: channelId,
        channel_name: channel.name,
        guild_name: channel.guild.name,
        start_date: startDate.toISOString(),
        end_date: endDate.toISOString(),
        message_count: filteredMessages.length,
        monitoring_status: monitoringStatus,
        formatted_history: formattedOutput,
      };
    }

    case "discord_get_unread_messages": {
      if (!discordClient) {
        throw new Error("Discord is not enabled. Set ENABLE_DISCORD=true");
      }
      
      const channelIdsToCheck = args.channel_ids || Array.from(monitoredDiscordChannels.keys());
      
      if (channelIdsToCheck.length === 0) {
        return {
          message: "No Discord channels being monitored",
          total_unread: 0,
          formatted_messages: "📊 0 messages\n\n",
        };
      }
      
      const allUnreadMessages: any[] = [];
      const channelResults: any[] = [];
      
      for (const channelId of channelIdsToCheck) {
        const state = monitoredDiscordChannels.get(channelId);
        
        if (!state) {
          channelResults.push({
            channel_id: channelId,
            status: "not_monitored",
            unread_count: 0,
          });
          continue;
        }
        
        try {
          const channel = await discordClient.channels.fetch(channelId);
          if (!channel || !(channel instanceof TextChannel)) {
            channelResults.push({
              channel_id: channelId,
              status: "error",
              error: "Not a text channel",
            });
            continue;
          }
          
          // Fetch recent messages
          const messages = await channel.messages.fetch({ limit: 100 });
          const unreadMessages = Array.from(messages.values())
            .filter(msg => msg.id > state.lastReadMessageId)
            .reverse();
          
          allUnreadMessages.push(...unreadMessages.map(msg => ({
            id: msg.id,
            author: msg.author.tag,
            content: formatDiscordContent(msg.content, msg.mentions),
            timestamp: Math.floor(msg.createdTimestamp / 1000),
            channel: state.channelName,
            guild: channel.guild.name,
            reply_to: msg.reference?.messageId,
          })));
          
          // Update state if mark_as_read
          if (args.mark_as_read !== false && unreadMessages.length > 0) {
            state.lastReadMessageId = unreadMessages[unreadMessages.length - 1].id;
            saveState();
          }
          
          channelResults.push({
            channel_id: channelId,
            channel_name: state.channelName,
            status: "checked",
            unread_count: unreadMessages.length,
          });
        } catch (error) {
          channelResults.push({
            channel_id: channelId,
            status: "error",
            error: error instanceof Error ? error.message : String(error),
          });
        }
      }
      
      const format = args.format || "detailed";
      const formattedOutput = formatDiscordMessages(allUnreadMessages, format);
      
      return {
        total_unread: allUnreadMessages.length,
        channels_checked: channelResults,
        formatted_messages: formattedOutput,
      };
    }

    case "discord_send_message": {
      if (!discordClient) {
        throw new Error("Discord is not enabled. Set ENABLE_DISCORD=true");
      }
      
      const channel = await discordClient.channels.fetch(args.channel_id);
      if (!channel || !(channel instanceof TextChannel)) {
        throw new Error("Channel not found or not a text channel");
      }
      
      const messageOptions: any = {
        content: args.content,
      };
      
      // Add reply reference if provided
      if (args.reply_to) {
        messageOptions.reply = {
          messageReference: args.reply_to,
        };
      }
      
      const sentMessage = await channel.send(messageOptions);
      
      return {
        success: true,
        message_id: sentMessage.id,
        channel_name: channel.name,
        guild_name: channel.guild.name,
        reply_to: args.reply_to,
      };
    }

    case "discord_delete_message": {
      if (!discordClient) {
        throw new Error("Discord is not enabled. Set ENABLE_DISCORD=true");
      }
      
      const channel = await discordClient.channels.fetch(args.channel_id);
      if (!channel || !(channel instanceof TextChannel)) {
        throw new Error("Channel not found or not a text channel");
      }
      
      const message = await channel.messages.fetch(args.message_id);
      await message.delete();
      
      return {
        success: true,
        message_id: args.message_id,
        channel_name: channel.name,
        deleted: true,
      };
    }

    case "discord_list_channels": {
      if (!discordClient) {
        throw new Error("Discord is not enabled. Set ENABLE_DISCORD=true");
      }
      
      const guildFilter = args.guild_id;
      const allChannels: any[] = [];
      
      for (const guild of discordClient.guilds.cache.values()) {
        if (guildFilter && guild.id !== guildFilter) continue;
        
        const textChannels = guild.channels.cache
          .filter(ch => ch instanceof TextChannel)
          .map(ch => ({
            id: ch.id,
            name: ch.name,
            guild_id: guild.id,
            guild_name: guild.name,
            topic: (ch as TextChannel).topic || "",
          }));
        
        allChannels.push(...textChannels);
      }
      
      // Format nicely
      const formatted = allChannels
        .map(ch => `💬 **#${ch.name}** (${ch.guild_name})\n   └─ ID: ${ch.id}${ch.topic ? `\n   └─ ${ch.topic}` : ""}`)
        .join("\n\n");
      
      return {
        total_channels: allChannels.length,
        formatted_list: `📋 **${allChannels.length} Discord Channels**\n\n${formatted}`,
        raw_data: allChannels,
      };
    }

    case "discord_get_monitored_channels": {
      const channels = Array.from(monitoredDiscordChannels.values());
      return {
        monitored_count: channels.length,
        channels: channels.map(c => ({
          channel_id: c.channelId,
          channel_name: c.channelName,
          guild_id: c.guildId,
          last_read_message_id: c.lastReadMessageId,
        })),
      };
    }

    case "discord_stop_monitoring": {
      const channelIds: string[] = args.channel_ids;
      
      if (!channelIds || channelIds.length === 0) {
        const stopped = Array.from(monitoredDiscordChannels.keys());
        monitoredDiscordChannels.clear();
        saveState();
        return {
          message: "Stopped monitoring all Discord channels",
          stopped_channels: stopped,
        };
      }
      
      const stopped: string[] = [];
      for (const channelId of channelIds) {
        if (monitoredDiscordChannels.has(channelId)) {
          monitoredDiscordChannels.delete(channelId);
          stopped.push(channelId);
        }
      }
      
      saveState();
      
      return {
        stopped_channels: stopped,
        still_monitoring: Array.from(monitoredDiscordChannels.keys()),
      };
    }

    // Slack handlers
    case "slack_list_channels": {
      const types = typeof args?.types === "string" && args.types
        ? args.types
        : "public_channel,private_channel,im,mpim";

      const allChannels: any[] = [];
      let cursor: string | undefined;
      do {
        const result = await slackWebClient!.conversations.list({
          types,
          exclude_archived: true,
          limit: 200,
          cursor,
        });
        for (const conv of (result.channels ?? []) as SlackConversation[]) {
          if (conv.is_im) {
            await resolveSlackUsers(conv.user ? [conv.user] : []);
            allChannels.push({
              id: conv.id,
              kind: "dm",
              name: conv.user ? (slackUserNames.get(conv.user) ?? conv.user) : (conv.id ?? "unknown"),
              user_id: conv.user,
            });
          } else if (conv.is_mpim) {
            allChannels.push({ id: conv.id, kind: "group_dm", name: conv.name });
          } else {
            allChannels.push({
              id: conv.id,
              kind: conv.is_private ? "private_channel" : "channel",
              name: conv.name,
              topic: conv.topic?.value || "",
              is_member: !!conv.is_member,
            });
          }
        }
        cursor = (result.response_metadata?.next_cursor as string) || undefined;
      } while (cursor);

      const formatted = allChannels
        .map(ch => {
          const icon = ch.kind === "dm" ? "👤" : ch.kind === "group_dm" ? "👥" : "💬";
          const label = ch.kind === "dm" ? `DM: @${ch.name}` : `#${ch.name}`;
          const member = ch.kind === "channel" || ch.kind === "private_channel"
            ? (ch.is_member ? "" : " (not a member)")
            : "";
          return `${icon} **${label}**${member}\n   └─ ID: ${ch.id}${ch.topic ? `\n   └─ ${ch.topic}` : ""}`;
        })
        .join("\n\n");

      return {
        total_channels: allChannels.length,
        formatted_list: `📋 **${allChannels.length} Slack Conversations**\n\n${formatted}`,
        raw_data: allChannels,
      };
    }

    case "slack_start_monitoring": {
      const channelIds: string[] = args.channel_ids;
      const results: any[] = [];

      for (const channelId of channelIds) {
        try {
          const info = await slackWebClient!.conversations.info({ channel: channelId });
          const conv = (info.channel ?? {}) as SlackConversation;
          const channelName = conv.is_im
            ? `DM:${conv.user ?? channelId}`
            : (conv.name ?? channelId);

          // Get latest message ts as the read cursor
          const history = await slackWebClient!.conversations.history({ channel: channelId, limit: 1 });
          const lastTs = history.messages?.[0]?.ts ?? "0";

          monitoredSlackChannels.set(channelId, {
            channelId,
            channelName,
            lastReadTs: String(lastTs),
          });

          results.push({
            channel_id: channelId,
            channel_name: channelName,
            status: "monitoring",
            last_read_ts: lastTs,
          });
        } catch (error) {
          results.push({
            channel_id: channelId,
            status: "error",
            error: error instanceof Error ? error.message : String(error),
          });
        }
      }

      saveState();

      return {
        session_id: sessionId,
        monitored_count: monitoredSlackChannels.size,
        channels: results,
      };
    }

    case "slack_get_channel_history": {
      const channelId = args.channel_id;

      const info = await slackWebClient!.conversations.info({ channel: channelId });
      const conv = (info.channel ?? {}) as SlackConversation;
      const channelName = conv.is_im ? `DM:${conv.user ?? channelId}` : (conv.name ?? channelId);

      // Parse dates
      const today = new Date();
      today.setHours(0, 0, 0, 0);
      const now = new Date();

      const startDate = parseDate(args.start_date, today);
      const endDate = parseDate(args.end_date, now);

      const maxMessages = Math.min(args.max_messages || 100, 200);
      // Cursor-drained and oldest-first; `truncated` means the range holds
      // more than maxMessages and the OLDEST messages were dropped.
      const { messages: rawMessages, truncated } = await fetchSlackHistory(slackWebClient!, {
        channel: channelId,
        oldest: String(startDate.getTime() / 1000),
        latest: String(endDate.getTime() / 1000),
        inclusive: true,
        maxMessages,
      });

      // Auto-monitor
      const autoMonitor = args.auto_monitor !== false;
      let monitoringStatus = "not_monitored";

      if (autoMonitor && rawMessages.length > 0) {
        const latestTs = String(rawMessages[rawMessages.length - 1].ts);

        if (!monitoredSlackChannels.has(channelId)) {
          monitoredSlackChannels.set(channelId, {
            channelId,
            channelName,
            lastReadTs: latestTs,
          });
          monitoringStatus = "started_monitoring";
          saveState();
        } else {
          const state = monitoredSlackChannels.get(channelId)!;
          state.lastReadTs = latestTs;
          monitoringStatus = "updated_read_position";
          saveState();
        }
      }

      const format = args.format || "detailed";
      const shaped = await shapeSlackMessages(rawMessages, channelName);
      const formattedOutput = formatSlackMessages(shaped, format);

      return {
        channel_id: channelId,
        channel_name: channelName,
        start_date: startDate.toISOString(),
        end_date: endDate.toISOString(),
        message_count: rawMessages.length,
        has_more: truncated,
        ...(truncated ? {
          note: `The range contains more than ${maxMessages} messages; the oldest were omitted. Narrow the date range to see them.`,
        } : {}),
        monitoring_status: monitoringStatus,
        formatted_history: formattedOutput,
      };
    }

    case "slack_get_unread_messages": {
      const channelIdsToCheck = args.channel_ids || Array.from(monitoredSlackChannels.keys());

      if (channelIdsToCheck.length === 0) {
        return {
          message: "No Slack conversations being monitored",
          total_unread: 0,
          formatted_messages: "📊 0 messages\n\n",
        };
      }

      // Backstop, not a page size — the cursor is drained up to this many.
      const MAX_UNREAD_PER_CHANNEL = 1000;

      const allUnreadMessages: ShapedSlackMessage[] = [];
      const channelResults: any[] = [];
      let stateDirty = false;

      for (const channelId of channelIdsToCheck) {
        const state = monitoredSlackChannels.get(channelId);

        if (!state) {
          channelResults.push({
            channel_id: channelId,
            status: "not_monitored",
            unread_count: 0,
          });
          continue;
        }

        try {
          // oldest is exclusive by default — returns strictly newer messages.
          // Cursor-drained and oldest-first.
          const { messages: unread, truncated } = await fetchSlackHistory(slackWebClient!, {
            channel: channelId,
            oldest: state.lastReadTs,
            maxMessages: MAX_UNREAD_PER_CHANNEL,
          });

          const shaped = await shapeSlackMessages(unread, state.channelName);
          allUnreadMessages.push(...shaped);

          if (args.mark_as_read !== false && unread.length > 0 && !truncated) {
            // When truncated, the OLDEST unread were dropped — advancing the
            // read cursor would skip them forever, so it stays put and the
            // caller sees has_more instead.
            state.lastReadTs = String(unread[unread.length - 1].ts);
            stateDirty = true;
          }

          channelResults.push({
            channel_id: channelId,
            channel_name: state.channelName,
            status: "checked",
            unread_count: unread.length,
            has_more: truncated,
            ...(truncated ? {
              note: `Unread backlog exceeds ${MAX_UNREAD_PER_CHANNEL}; read position NOT advanced. Use slack_get_channel_history with explicit dates to page through it.`,
            } : {}),
          });
        } catch (error) {
          channelResults.push({
            channel_id: channelId,
            status: "error",
            error: error instanceof Error ? error.message : String(error),
          });
        }
      }

      if (stateDirty) saveState();

      const format = args.format || "detailed";
      const formattedOutput = formatSlackMessages(allUnreadMessages, format);

      return {
        total_unread: allUnreadMessages.length,
        has_more: channelResults.some(r => r.has_more === true),
        channels_checked: channelResults,
        formatted_messages: formattedOutput,
      };
    }

    case "slack_send_message": {
      const result = await slackWebClient!.chat.postMessage({
        channel: args.channel_id,
        text: args.content,
        ...(args.thread_ts ? { thread_ts: args.thread_ts } : {}),
      });

      return {
        success: true,
        message_ts: result.ts,
        channel_id: args.channel_id,
        thread_ts: args.thread_ts,
      };
    }

    case "slack_delete_message": {
      await slackWebClient!.chat.delete({
        channel: args.channel_id,
        ts: args.message_ts,
      });

      return {
        success: true,
        message_ts: args.message_ts,
        channel_id: args.channel_id,
        deleted: true,
      };
    }

    case "slack_add_reaction": {
      await slackWebClient!.reactions.add({
        channel: args.channel_id,
        timestamp: args.message_ts,
        name: String(args.emoji_name).replace(/:/g, ""),
      });

      return {
        success: true,
        message_ts: args.message_ts,
        emoji: args.emoji_name,
      };
    }

    case "slack_find_user": {
      const query = String(args.query || "").toLowerCase();
      if (!query) throw new Error("query is required");

      const matches: any[] = [];
      let cursor: string | undefined;
      do {
        const result = await slackWebClient!.users.list({ limit: 200, cursor });
        for (const user of (result.members ?? []) as any[]) {
          if (user.deleted || user.is_bot) continue;
          const name = user.name?.toLowerCase() ?? "";
          const realName = user.real_name?.toLowerCase() ?? "";
          const displayName = user.profile?.display_name?.toLowerCase() ?? "";
          if (name.includes(query) || realName.includes(query) || displayName.includes(query)) {
            matches.push({
              user_id: user.id,
              username: user.name,
              real_name: user.real_name,
              display_name: user.profile?.display_name,
              mention_syntax: `<@${user.id}>`,
            });
            if (matches.length >= 25) break;
          }
        }
        cursor = matches.length >= 25
          ? undefined
          : ((result.response_metadata?.next_cursor as string) || undefined);
      } while (cursor);

      if (matches.length === 0) {
        return {
          found: false,
          message: `No users found matching "${args.query}".`,
        };
      }

      const formatted = matches.map(user =>
        `👤 **${user.real_name || user.username}**\n   └─ User ID: ${user.user_id}\n   └─ Mention format: <@${user.user_id}>`
      ).join("\n\n");

      return {
        found: true,
        match_count: matches.length,
        formatted_list: `👥 Found ${matches.length} user${matches.length !== 1 ? 's' : ''}:\n\n${formatted}`,
        users: matches,
      };
    }

    case "slack_get_monitored_channels": {
      const channels = Array.from(monitoredSlackChannels.values());
      return {
        monitored_count: channels.length,
        channels: channels.map(c => ({
          channel_id: c.channelId,
          channel_name: c.channelName,
          last_read_ts: c.lastReadTs,
        })),
      };
    }

    case "slack_stop_monitoring": {
      const channelIds: string[] = args.channel_ids;

      if (!channelIds || channelIds.length === 0) {
        const stopped = Array.from(monitoredSlackChannels.keys());
        monitoredSlackChannels.clear();
        saveState();
        return {
          message: "Stopped monitoring all Slack conversations",
          stopped_channels: stopped,
        };
      }

      const stopped: string[] = [];
      for (const channelId of channelIds) {
        if (monitoredSlackChannels.has(channelId)) {
          monitoredSlackChannels.delete(channelId);
          stopped.push(channelId);
        }
      }

      saveState();

      return {
        stopped_channels: stopped,
        still_monitoring: Array.from(monitoredSlackChannels.keys()),
      };
    }

    case "slack_fetch_attachment": {
      // The bot token must only be sent to files.slack.com — never to
      // workspace hosts. Enforcement lives in parseSlackAttachmentUrl.
      const url = parseSlackAttachmentUrl(String(args.url || ""));
      const botToken = process.env.SLACK_BOT_TOKEN || "";
      const headers: Record<string, string> = botToken
        ? { Authorization: `Bearer ${botToken}` }
        : {};
      const fileName = decodeURIComponent(url.pathname.split("/").pop() || "attachment");
      return toFetchResult(await fetchAttachmentBytes(url.toString(), fileName, { headers }));
    }

    default:
      throw new Error(`Unknown tool: ${name}`);
  }
}

// Build MCPL capabilities (used in server constructor when MCPL is enabled)
const ENABLED_PLATFORMS = [
  ...(ENABLE_ZULIP ? ['zulip'] : []),
  ...(ENABLE_DISCORD ? ['discord'] : []),
  ...(ENABLE_SLACK ? ['slack'] : []),
];

// Which platform types can actually send a typing indicator. Derived from the
// adapter classes rather than restated, so `channels.typing` cannot be
// advertised for a platform whose adapter does not implement it (§6.4).
const TYPING_CAPABLE_PLATFORMS = new Set<string>(
  ([
    ['zulip', ZulipAdapter],
    ['discord', DiscordAdapter],
    ['slack', SlackAdapter],
  ] as [string, { prototype: PlatformAdapter }][])
    .filter(([, cls]) => typeof cls.prototype.sendTyping === 'function')
    .map(([type]) => type),
);

const mcplServerCaps = MCPL_ENABLED
  ? buildServerCapabilities(ENABLED_PLATFORMS, { typingCapable: TYPING_CAPABLE_PLATFORMS })
  : null;

// The manifest is what `initialize` presents (§5.1) and what `mcpl/manifest`
// returns (§17.4). Building it through the tracker stamps the canonical
// content digest (§17.2) onto the same object both paths serve, and seeds this
// connection's last-announced revision from the handshake so a fresh
// connection does not fire a redundant `mcpl/manifestChanged` (§17.10).
const manifestTracker = mcplServerCaps ? new ManifestTracker(mcplServerCaps) : null;

// Create and configure the server
const server = new Server(
  {
    name: "zulip-mcp-server",
    version: "2.2.0",
  },
  {
    capabilities: {
      tools: {},
      resources: {},
      ...(manifestTracker ? { experimental: { mcpl: manifestTracker.manifest } } : {}),
    },
  }
);

// Register handlers
server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: getTools(),
}));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  try {
    const result = await handleToolCall(request.params.name, request.params.arguments);
    // Tools that need to return image/non-text content set `_content` directly.
    if (result && typeof result === "object" && Array.isArray((result as any)._content)) {
      return { content: (result as any)._content };
    }
    return {
      content: [
        {
          type: "text",
          text: JSON.stringify(result, null, 2),
        },
      ],
    };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    return {
      content: [
        {
          type: "text",
          text: `Error: ${errorMessage}`,
        },
      ],
      isError: true,
    };
  }
});

// Register resource handlers
server.setRequestHandler(ListResourcesRequestSchema, async () => {
  const resources: any[] = [];
  
  // Zulip resources
  if (ENABLE_ZULIP && zulipClient) {
    resources.push(
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
      }
    );
    
    // Add a resource for each monitored Zulip channel
    for (const channel of monitoredChannels.keys()) {
      resources.push({
        uri: `zulip://channel/${channel}/unread`,
        name: `Zulip #${channel} - Unread Messages`,
        description: `Unread message count for Zulip #${channel}`,
        mimeType: "text/plain",
      });
    }
  }
  
  // Discord resources
  if (ENABLE_DISCORD && discordClient) {
    resources.push(
      {
        uri: "discord://unread/summary",
        name: "Discord - Unread Messages Summary",
        description: "Count of unread messages across all monitored Discord channels",
        mimeType: "text/plain",
      },
      {
        uri: "discord://monitoring/status",
        name: "Discord - Monitoring Status",
        description: "Current Discord monitoring state and channel list",
        mimeType: "application/json",
      }
    );
    
    // Add a resource for each monitored Discord channel
    for (const [channelId, state] of monitoredDiscordChannels) {
      resources.push({
        uri: `discord://channel/${channelId}/unread`,
        name: `Discord #${state.channelName} - Unread Messages`,
        description: `Unread message count for Discord #${state.channelName}`,
        mimeType: "text/plain",
      });
    }
  }

  // Slack resources
  if (ENABLE_SLACK && slackWebClient) {
    resources.push(
      {
        uri: "slack://unread/summary",
        name: "Slack - Unread Messages Summary",
        description: "Count of unread messages across all monitored Slack conversations",
        mimeType: "text/plain",
      },
      {
        uri: "slack://monitoring/status",
        name: "Slack - Monitoring Status",
        description: "Current Slack monitoring state and conversation list",
        mimeType: "application/json",
      }
    );

    // Add a resource for each monitored Slack conversation
    for (const [channelId, state] of monitoredSlackChannels) {
      resources.push({
        uri: `slack://channel/${channelId}/unread`,
        name: `Slack #${state.channelName} - Unread Messages`,
        description: `Unread message count for Slack #${state.channelName}`,
        mimeType: "text/plain",
      });
    }
  }

  return { resources };
});

server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
  const uri = request.params.uri;
  
  try {
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
      
      return {
        contents: [
          {
            uri,
            mimeType: "text/plain",
            text: summary,
          },
        ],
      };
    }
    
    // Handle monitoring status
    if (uri === "zulip://monitoring/status") {
      const status = {
        session_id: sessionId,
        monitored_count: monitoredChannels.size,
        channels: Array.from(monitoredChannels.values()).map(c => ({
          name: c.channelName,
          last_read_message_id: c.lastReadMessageId,
        })),
      };
      
      return {
        contents: [
          {
            uri,
            mimeType: "application/json",
            text: JSON.stringify(status, null, 2),
          },
        ],
      };
    }
    
    // Handle individual channel unread count
    const channelMatch = uri.match(/^zulip:\/\/channel\/([^/]+)\/unread$/);
    if (channelMatch) {
      const channelName = channelMatch[1];
      const state = monitoredChannels.get(channelName);
      
      if (!state) {
        return {
          contents: [
            {
              uri,
              mimeType: "text/plain",
              text: `Channel "${channelName}" is not being monitored`,
            },
          ],
        };
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
        
        return {
          contents: [
            {
              uri,
              mimeType: "text/plain",
              text,
            },
          ],
        };
      } catch (error) {
        return {
          contents: [
            {
              uri,
              mimeType: "text/plain",
              text: `Error checking #${channelName}: ${error}`,
            },
          ],
        };
      }
    }
    
    // Discord resource handlers
    if (uri === "discord://unread/summary") {
      if (!discordClient) {
        return {
          contents: [{
            uri,
            mimeType: "text/plain",
            text: "Discord is not enabled",
          }],
        };
      }
      
      let totalUnread = 0;
      const channelSummaries: string[] = [];
      
      for (const [channelId, state] of monitoredDiscordChannels) {
        try {
          const channel = await discordClient.channels.fetch(channelId);
          if (!channel || !(channel instanceof TextChannel)) continue;
          
          const messages = await channel.messages.fetch({ limit: 50 });
          const unread = Array.from(messages.values()).filter(msg => msg.id > state.lastReadMessageId);
          totalUnread += unread.length;
          
          if (unread.length > 0) {
            channelSummaries.push(`📬 #${state.channelName}: ${unread.length} unread`);
          }
        } catch (error) {
          // Skip channels with errors
        }
      }
      
      const summary = totalUnread > 0
        ? `🔔 ${totalUnread} unread Discord message${totalUnread !== 1 ? 's' : ''}\n\n${channelSummaries.join('\n')}`
        : "✅ No unread Discord messages";
      
      return {
        contents: [{
          uri,
          mimeType: "text/plain",
          text: summary,
        }],
      };
    }
    
    if (uri === "discord://monitoring/status") {
      const status = {
        session_id: sessionId,
        monitored_count: monitoredDiscordChannels.size,
        channels: Array.from(monitoredDiscordChannels.values()).map(c => ({
          channel_id: c.channelId,
          channel_name: c.channelName,
          guild_id: c.guildId,
          last_read_message_id: c.lastReadMessageId,
        })),
      };
      
      return {
        contents: [{
          uri,
          mimeType: "application/json",
          text: JSON.stringify(status, null, 2),
        }],
      };
    }
    
    const discordChannelMatch = uri.match(/^discord:\/\/channel\/([^/]+)\/unread$/);
    if (discordChannelMatch) {
      const channelId = discordChannelMatch[1];
      const state = monitoredDiscordChannels.get(channelId);
      
      if (!state || !discordClient) {
        return {
          contents: [{
            uri,
            mimeType: "text/plain",
            text: `Discord channel "${channelId}" is not being monitored`,
          }],
        };
      }
      
      try {
        const channel = await discordClient.channels.fetch(channelId);
        if (!channel || !(channel instanceof TextChannel)) {
          return {
            contents: [{
              uri,
              mimeType: "text/plain",
              text: "Not a text channel",
            }],
          };
        }
        
        const messages = await channel.messages.fetch({ limit: 50 });
        const unread = Array.from(messages.values()).filter(msg => msg.id > state.lastReadMessageId);
        const count = unread.length;
        
        const text = count > 0
          ? `📬 ${count} unread message${count !== 1 ? 's' : ''} in #${state.channelName}`
          : `✅ No unread messages in #${state.channelName}`;
        
        return {
          contents: [{
            uri,
            mimeType: "text/plain",
            text,
          }],
        };
      } catch (error) {
        return {
          contents: [{
            uri,
            mimeType: "text/plain",
            text: `Error checking #${state.channelName}: ${error}`,
          }],
        };
      }
    }
    
    if (uri === "slack://unread/summary") {
      if (!slackWebClient) {
        return {
          contents: [{
            uri,
            mimeType: "text/plain",
            text: "Slack is not enabled",
          }],
        };
      }

      let totalUnread = 0;
      let anyTruncated = false;
      const channelSummaries: string[] = [];

      for (const [channelId, state] of monitoredSlackChannels) {
        try {
          const { messages: unread, truncated } = await fetchSlackHistory(slackWebClient!, {
            channel: channelId,
            oldest: state.lastReadTs,
            maxMessages: 200,
          });
          totalUnread += unread.length;
          if (truncated) anyTruncated = true;

          if (unread.length > 0) {
            channelSummaries.push(`📬 #${state.channelName}: ${unread.length}${truncated ? '+' : ''} unread`);
          }
        } catch (error) {
          // Skip the channel but leave a trace — a silently swallowed 403
          // makes the summary undercount forever with no way to notice.
          console.error(`Failed to read unread count for Slack #${state.channelName} (${channelId}):`, error);
        }
      }

      const summary = totalUnread > 0
        ? `🔔 ${totalUnread}${anyTruncated ? '+' : ''} unread Slack message${totalUnread !== 1 ? 's' : ''}\n\n${channelSummaries.join('\n')}`
        : "✅ No unread Slack messages";

      return {
        contents: [{
          uri,
          mimeType: "text/plain",
          text: summary,
        }],
      };
    }

    if (uri === "slack://monitoring/status") {
      const status = {
        session_id: sessionId,
        monitored_count: monitoredSlackChannels.size,
        channels: Array.from(monitoredSlackChannels.values()).map(c => ({
          channel_id: c.channelId,
          channel_name: c.channelName,
          last_read_ts: c.lastReadTs,
        })),
      };

      return {
        contents: [{
          uri,
          mimeType: "application/json",
          text: JSON.stringify(status, null, 2),
        }],
      };
    }

    const slackChannelMatch = uri.match(/^slack:\/\/channel\/([^/]+)\/unread$/);
    if (slackChannelMatch) {
      const channelId = slackChannelMatch[1];
      const state = monitoredSlackChannels.get(channelId);

      if (!state || !slackWebClient) {
        return {
          contents: [{
            uri,
            mimeType: "text/plain",
            text: `Slack conversation "${channelId}" is not being monitored`,
          }],
        };
      }

      try {
        const { messages, truncated } = await fetchSlackHistory(slackWebClient!, {
          channel: channelId,
          oldest: state.lastReadTs,
          maxMessages: 200,
        });
        const count = messages.length;

        const text = count > 0
          ? `📬 ${count}${truncated ? '+' : ''} unread message${count !== 1 ? 's' : ''} in #${state.channelName}`
          : `✅ No unread messages in #${state.channelName}`;

        return {
          contents: [{
            uri,
            mimeType: "text/plain",
            text,
          }],
        };
      } catch (error) {
        return {
          contents: [{
            uri,
            mimeType: "text/plain",
            text: `Error checking #${state.channelName}: ${error}`,
          }],
        };
      }
    }

    throw new Error(`Unknown resource: ${uri}`);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    throw new Error(`Failed to read resource: ${errorMessage}`);
  }
});

// Start the server
async function main() {
  try {
    const enabledServices: string[] = [];
    let firstInitError: unknown = null;

    // Initialize Zulip if enabled
    if (ENABLE_ZULIP) {
      try {
        await initializeZulipClient();
        enabledServices.push("Zulip");
      } catch (error) {
        console.error("Failed to initialize Zulip:", error);
        firstInitError ??= error;
      }
    }

    // Initialize Discord if enabled
    if (ENABLE_DISCORD) {
      try {
        await initializeDiscordClient();
        enabledServices.push("Discord");
      } catch (error) {
        console.error("Failed to initialize Discord:", error);
        firstInitError ??= error;
      }
    }

    // Initialize Slack if enabled
    if (ENABLE_SLACK) {
      try {
        await initializeSlackClient();
        enabledServices.push("Slack");
      } catch (error) {
        console.error("Failed to initialize Slack:", error);
        firstInitError ??= error;
      }
    }

    if (enabledServices.length === 0) {
      throw firstInitError ?? new Error(
        "No services enabled. Set ENABLE_ZULIP=true, ENABLE_DISCORD=true, or ENABLE_SLACK=true"
      );
    }

    if (MCPL_ENABLED) {
      // -- MCPL mode --
      const client = new McplClient();
      const dispatcher = new McplDispatcher();

      // Build platform adapters for every initialized client
      const adapters = new Map<string, PlatformAdapter>();
      if (zulipClient) {
        adapters.set('zulip', new ZulipAdapter(zulipClient, zulipSelfUserId, sessionId));
      }
      if (discordClient) {
        adapters.set('discord', new DiscordAdapter(discordClient));
      }
      if (slackWebClient && slackSocketClient) {
        adapters.set('slack', new SlackAdapter(slackWebClient, slackSocketClient, slackSelfUserId, slackTeamName));
      }

      // The effective capability grant for this connection (§5.4). It starts
      // empty: until the initial policy exchange completes, every
      // capability-dependent behavior is unavailable (§5.3).
      const grant = new CapabilityGrant(mcplServerCaps?.featureSets ?? {});

      const channelManager = new ChannelManager(client, adapters, grant, MCPL_BATCH_WINDOW_MS);
      const contextProvider = new ContextProvider(channelManager, grant, MCPL_CONTEXT_HISTORY_SIZE);

      // Register dispatcher handlers
      dispatcher.register(McplMethod.BeforeInference, (params) =>
        contextProvider.handleBeforeInference(params as unknown as BeforeInferenceParams),
      );
      // §6.7: featureSets/update is a Request carrying the effective grant, and
      // its response is a degradation receipt — what this server WILL DO under
      // the grant it was given. It is testimony about consequences, never a
      // claim of entitlement, and it asks for nothing. The dual-mode form
      // matters here: a Notification cannot establish a ready state, so it is
      // never allowed to widen.
      dispatcher.register(McplMethod.FeatureSetsUpdate, (params, ctx) =>
        grant.apply(
          params as unknown as FeatureSetsUpdateParams,
          ctx.isRequest ? 'request' : 'notification',
        ),
      );
      // §17.4: the complete current manifest, never a delta, in the same shape
      // initialize carries. Not gated on any capability path.
      dispatcher.register(McplMethod.Manifest, () => {
        if (!manifestTracker) throw new McplRpcError(-32601, 'MCPL manifest unavailable');
        return manifestTracker.handleManifestRequest();
      });
      dispatcher.register(McplMethod.ChannelsOpen, (params) =>
        channelManager.openChannel(params as unknown as ChannelsOpenParams),
      );
      dispatcher.register(McplMethod.ChannelsClose, (params) =>
        channelManager.closeChannel(params as unknown as ChannelsCloseParams),
      );
      dispatcher.register(McplMethod.ChannelsList, () =>
        channelManager.listChannels(),
      );
      dispatcher.register(McplMethod.ChannelsPublish, (params) =>
        channelManager.publish(params as unknown as ChannelsPublishParams),
      );
      dispatcher.register(McplMethod.ChannelsTyping, async (params) => {
        const p = params as {
          channelId: string;
          metadata?: Record<string, unknown>;
          op?: 'start' | 'stop';
        };
        await channelManager.sendTyping(p.channelId, p.metadata, p.op);
        return {};
      });

      // Create MCPL transport and connect
      const transport = new McplTransport(dispatcher, client);
      await server.connect(transport);

      // §5.3: registration waits for the initial policy exchange, not for a
      // timer. Until `featureSets/update` arrives the grant is empty and
      // `channels.register` is denied, so registering earlier would be acting
      // on a capability nobody has granted yet. A host that never sends it
      // leaves this server inert by design — absence is denial.
      void grant.whenReady().then(async () => {
        // §17.3: the manifest presented at `initialize` was built from the
        // ENABLE_* environment, before any platform client had been contacted.
        // A platform that failed to initialize leaves feature sets advertised
        // that nothing can serve, and §6.4 makes an inaccurate declaration
        // consequential. Reinstall the manifest describing the adapters that
        // actually came up; the digest moves on its own, so the correction
        // cannot be installed without being announced, and when every platform
        // came up the digest is unchanged and nothing is sent.
        if (manifestTracker) {
          const actual = buildServerCapabilities(Array.from(adapters.keys()), {
            typingCapable: TYPING_CAPABLE_PLATFORMS,
          });
          const domains = manifestTracker.setManifest(actual, (params) =>
            client.sendManifestChanged(params),
          );
          if (domains.length > 0) {
            // Declarations feed degradation derivation (§6.4), not authority.
            // The grant itself is untouched — only the host widens a grant.
            grant.setDeclarations(manifestTracker.manifest.featureSets ?? {});
            console.error(
              `Announced mcpl/manifestChanged (${domains.join(', ')}): advertised ` +
                `[${ENABLED_PLATFORMS.join(', ')}], initialized [${Array.from(adapters.keys()).join(', ')}]`,
            );
          }
        }

        try {
          await channelManager.registerChannels();
        } catch (error) {
          console.error('Failed to register channels after initial policy:', error);
        }
      });

      // Start real-time event delivery for every adapter
      for (const adapter of adapters.values()) {
        adapter.startEvents(
          (message) => {
            channelManager.onIncomingMessage(message.channelId, message);
          },
          (event) => {
            // Delivery gaps / degraded polling: surface to the agent as a
            // synthetic system message on the platform's open channels.
            channelManager.broadcastSystemEvent(adapter.type, event);
          },
        );
      }

      console.error(`MCPL server running with: ${enabledServices.join(", ")}`);
    } else {
      // -- Plain MCP mode --
      const transport = new StdioServerTransport();
      await server.connect(transport);
      console.error(`MCP server running with: ${enabledServices.join(", ")}`);
    }
  } catch (error) {
    console.error("Failed to start server:", error);
    process.exit(1);
  }
}

// Only auto-start when run as a CLI. Importing this module (e.g. from tests)
// should not boot the MCP server or require Zulip credentials. isMainModule
// realpaths argv[1] so the guard also passes when launched through an npm
// bin symlink (npx zulip-mcp-server).
if (isMainModule(import.meta.url, process.argv[1])) {
  main();
}
