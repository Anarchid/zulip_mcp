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

// Initialize Zulip client
let zulipClient: any = null;

// Session and state management
interface ChannelState {
  channelName: string;
  lastReadMessageId: number;
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
      Object.values(data.monitoredChannels).forEach(channel => {
        monitoredChannels.set(channel.channelName, channel);
      });
      console.error(`Loaded state for session ${sessionId}: ${monitoredChannels.size} channels`);
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
  
  // Set up session ID and load persistent state
  sessionId = process.env.ZULIP_SESSION_ID || process.env.ZULIP_EMAIL || process.env.ZULIP_USERNAME || "default";
  loadState(sessionId);
  
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

// Helper function to strip HTML and format content
function cleanContent(html: string): string {
  return html
    .replace(/<p>/gi, '\n')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<[^>]*>/g, '')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .trim();
}

// Helper function to format messages
function formatMessages(messages: any[], format: string): string {
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

// Define available tools
const tools: Tool[] = [
  {
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
];

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
        raw_data: result,
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
        raw_data: result,
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
        raw_data: result,
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

    default:
      throw new Error(`Unknown tool: ${name}`);
  }
}

// Create and configure the server
const server = new Server(
  {
    name: "zulip-mcp-server",
    version: "2.0.0",
  },
  {
    capabilities: {
      tools: {},
      resources: {},
    },
  }
);

// Register handlers
server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools,
}));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  try {
    const result = await handleToolCall(request.params.name, request.params.arguments);
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
  const resources: any[] = [
    {
      uri: "zulip://unread/summary",
      name: "Unread Messages Summary",
      description: "Count of unread messages across all monitored channels",
      mimeType: "text/plain",
    },
    {
      uri: "zulip://monitoring/status",
      name: "Monitoring Status",
      description: "Current monitoring state and channel list",
      mimeType: "application/json",
    },
  ];
  
  // Add a resource for each monitored channel
  for (const channel of monitoredChannels.keys()) {
    resources.push({
      uri: `zulip://channel/${channel}/unread`,
      name: `${channel} - Unread Messages`,
      description: `Unread message count for #${channel}`,
      mimeType: "text/plain",
    });
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
    
    const transport = new StdioServerTransport();
    await server.connect(transport);
    
    console.error("Zulip MCP server running on stdio");
  } catch (error) {
    console.error("Failed to start server:", error);
    process.exit(1);
  }
}

main();
