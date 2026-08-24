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

// Content helpers (re-exported for tests and downstream importers)
// Content helpers (re-exported for tests and downstream importers)
import {
  fetchAttachmentBytes,
  toFetchResult,
  cleanContent,
  parseZulipAttachmentUrl,
  isMainModule,
} from './content.js';
export { fetchAttachmentBytes, extractZulipAttachments, cleanContent } from './content.js';

// Startup flags
const MCPL_ENABLED = process.env.MCPL_ENABLED !== "false";
const MCPL_BATCH_WINDOW_MS = parseInt(process.env.MCPL_BATCH_WINDOW_MS || "500", 10);
const MCPL_CONTEXT_HISTORY_SIZE = parseInt(process.env.MCPL_CONTEXT_HISTORY_SIZE || "20", 10);

// Initialize clients
let zulipClient: any = null;
let zulipSelfUserId: number | null = null;
let zulipRealm: string = "";
let zulipAuthHeader: string = "";

// Session and state management
interface ChannelState {
  channelName: string;
  lastReadMessageId: number | string;
  subscribed: boolean;
}

interface SessionState {
  sessionId: string;
  userId?: string;
  monitoredChannels: Record<string, ChannelState>;
}

let sessionId: string = "";
const monitoredChannels: Map<string, ChannelState> = new Map();

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

      if (data.monitoredChannels) {
        Object.values(data.monitoredChannels).forEach(channel => {
          monitoredChannels.set(channel.channelName, channel);
        });
      }

      console.error(`Loaded state for session ${sessionId}: ${monitoredChannels.size} Zulip channels`);
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

// Define available tools (dynamically based on enabled services)
function getTools(): Tool[] {
  const tools: Tool[] = [];
  
  // Zulip tools
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

  return tools;
}

// Handle tool execution
async function handleToolCall(name: string, args: any): Promise<any> {
  if (!zulipClient) {
    throw new Error("Zulip client not initialized");
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

    default:
      throw new Error(`Unknown tool: ${name}`);
  }
}

// Build MCPL capabilities (used in server constructor when MCPL is enabled)
const ENABLED_PLATFORMS = ['zulip'];

// Derived from the adapter class rather than restated, so `channels.typing`
// cannot be advertised when the adapter does not implement it (§6.4).
const TYPING_CAPABLE_PLATFORMS = new Set<string>(
  typeof ZulipAdapter.prototype.sendTyping === 'function' ? ['zulip'] : [],
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
  if (zulipClient) {
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
    
    throw new Error(`Unknown resource: ${uri}`);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    throw new Error(`Failed to read resource: ${errorMessage}`);
  }
});

// Start the server
async function main() {
  try {
    await initializeZulipClient();

    if (MCPL_ENABLED) {
      // -- MCPL mode --
      const client = new McplClient();
      const dispatcher = new McplDispatcher();

      // Build platform adapters for every initialized client
      const adapters = new Map<string, PlatformAdapter>();
      if (zulipClient) {
        adapters.set('zulip', new ZulipAdapter(zulipClient, zulipSelfUserId, sessionId));
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
        // environment, before the Zulip client had been contacted.
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

      console.error("MCPL server running (Zulip)");
    } else {
      // -- Plain MCP mode --
      const transport = new StdioServerTransport();
      await server.connect(transport);
      console.error("MCP server running (Zulip)");
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
