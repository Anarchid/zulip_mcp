# Zulip, Discord & Slack MCP Server

[![npm version](https://img.shields.io/npm/v/zulip-mcp-server.svg)](https://www.npmjs.com/package/zulip-mcp-server)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Model Context Protocol (MCP) server that provides **stateful, ergonomic** integration with **Zulip and Discord**. This server allows AI assistants and other MCP clients to monitor channels, track read/unread messages, and retrieve history with natural date-based queries.

**Install:** `npm install -g zulip-mcp-server`

**Features:** Zulip ✅ | Discord ✅ | Stateful ✅ | Persistent ✅ | Ambient Awareness ✅

## 🎯 Design Philosophy

**Fewer, Better Tools** - Instead of exposing 20+ low-level API endpoints, this server provides a small set of high-level, stateful tools that are intuitive and powerful.

**Stateful Monitoring** - The server maintains state about which channels you're monitoring and tracks read/unread messages automatically.

**Ambient Awareness via Resources** - MCP Resources expose unread message counts that the agent can see when activated for any reason, creating passive awareness without explicit queries.

**Ergonomic Date Handling** - Use natural keywords like "today" and "yesterday" instead of timestamps and anchors.

## 🚀 Core Features

### 🔔 **Ambient Awareness (MCP Resources)**
The agent can passively see unread message notifications when activated for any reason:

**Zulip:**
- `zulip://unread/summary` - Total unread count across monitored channels
- `zulip://monitoring/status` - Current monitoring state
- `zulip://channel/{name}/unread` - Per-channel unread count

**Discord:**
- `discord://unread/summary` - Total unread count across monitored channels  
- `discord://monitoring/status` - Current monitoring state
- `discord://channel/{id}/unread` - Per-channel unread count

### 📺 **Zulip Tools**
- `start_monitoring` - Monitor Zulip channels
- `get_channel_history` - Get history with natural dates (mentions formatted as `@username (uid:123)`)
- `get_unread_messages` - Get unread from monitored channels
- `send_message` - Send to streams or DMs (use `@**username**` for mentions)
- `delete_message` - Delete any message by ID (with permissions)
- `add_reaction` - Add emoji reactions
- `find_user` - Search users to get mention format
- `list_streams` - Browse channels
- `get_stream_topics` - See topics
- `list_users` - View users
- `get_user_profile` - Get your info
- `get_monitored_channels` - View monitoring state
- `stop_monitoring` - Stop tracking

### 💬 **Discord Tools**
- `discord_start_monitoring` - Monitor Discord channels
- `discord_get_channel_history` - Get history with natural dates (mentions formatted as `@username (uid:123...)`, shows replies)
- `discord_get_unread_messages` - Get unread from monitored channels
- `discord_send_message` - Send to Discord channels (supports `reply_to` parameter, use `<@user_id>` for mentions)
- `discord_delete_message` - Delete any message by ID (with permissions)
- `discord_find_user` - Search users to get user ID for mentions
- `discord_list_channels` - Browse Discord channels
- `discord_get_monitored_channels` - View monitoring state
- `discord_stop_monitoring` - Stop tracking

### 💼 **Slack Tools**
- `slack_list_channels` - Browse Slack conversations (channels, private channels, **DMs, group DMs**)
- `slack_start_monitoring` - Monitor Slack conversations
- `slack_get_channel_history` - Get history with natural dates (mentions formatted as `@username (uid:U123...)`, shows threads)
- `slack_get_unread_messages` - Get unread from monitored conversations
- `slack_send_message` - Send to channels or DMs (supports `thread_ts` for in-thread replies, use `<@user_id>` for mentions)
- `slack_delete_message` - Delete a message by ts (with permissions)
- `slack_add_reaction` - Add emoji reactions
- `slack_find_user` - Search users to get user ID for mentions
- `slack_fetch_attachment` - Fetch file bytes inline (auth-locked to `files.slack.com`)
- `slack_get_monitored_channels` - View monitoring state
- `slack_stop_monitoring` - Stop tracking

Slack resources for ambient awareness: `slack://unread/summary`, `slack://monitoring/status`, `slack://channel/{id}/unread`.

## Installation

**No installation needed!** Just use `npx` to run directly from npm:

```bash
npx zulip-mcp-server
```

The package is published on npm: https://www.npmjs.com/package/zulip-mcp-server

**Optional:** Install globally if you prefer:

```bash
npm install -g zulip-mcp-server
```

## Configuration

### Service Selection

Enable the services you need via environment variables:

```bash
# Enable Zulip (enabled by default)
export ENABLE_ZULIP=true

# Enable Discord (disabled by default)  
export ENABLE_DISCORD=true

# Enable Slack (disabled by default)
export ENABLE_SLACK=true
```

You can enable any combination:
- **Just Zulip** (default): Don't set `ENABLE_DISCORD`/`ENABLE_SLACK` - Only Zulip tools will be available
- **Just Discord**: Set `ENABLE_DISCORD=true` and `ENABLE_ZULIP=false`
- **Just Slack**: Set `ENABLE_SLACK=true` and `ENABLE_ZULIP=false`
- **Any mix**: Enable multiple platforms - each contributes its own tool set, channels, and feature sets

**Note:** Tools are dynamically loaded based on enabled services. If a service isn't configured, its tools won't appear in the tool list.

### Authentication

#### Zulip Authentication

The server supports three authentication methods:

### Option 1: Environment Variables with API Key (Recommended)

```bash
export ZULIP_REALM="https://your-org.zulipchat.com"
export ZULIP_EMAIL="your-bot@example.com"
export ZULIP_API_KEY="your-api-key"
export ZULIP_SESSION_ID="agent_name"  # Optional: for persistent state across restarts
```

**Note:** `ZULIP_SESSION_ID` is optional but recommended. It allows:
- Multiple agents to have separate monitoring state
- State to persist across server restarts
- Each agent to have their own read/unread tracking

If not set, defaults to your email/username.

### Option 2: Environment Variables with Password

```bash
export ZULIP_REALM="https://your-org.zulipchat.com"
export ZULIP_USERNAME="your-bot@example.com"
export ZULIP_PASSWORD="your-password"
```

### Option 3: Using zuliprc File

Create a `zuliprc` file (see `zuliprc.example`):

```ini
[api]
email=your-bot@example.com
key=your-api-key
site=https://your-org.zulipchat.com
```

Then set the path:

```bash
export ZULIP_RC_PATH="/path/to/zuliprc"
```

**Important:** Add `zuliprc` to your `.gitignore` to avoid committing credentials!

#### Discord Authentication

```bash
export DISCORD_TOKEN="your-bot-token"
```

#### Slack Authentication

Slack uses two tokens — a bot token for Web API calls and an app-level token for Socket Mode (real-time events without a public webhook URL):

```bash
export SLACK_BOT_TOKEN="xoxb-..."   # Bot User OAuth Token
export SLACK_APP_TOKEN="xapp-..."   # App-level token with connections:write
export SLACK_SESSION_ID="agent_name"  # Optional: persistent state (defaults to workspace name)
```

## Getting Your API Keys

### Zulip API Key

1. Log in to your Zulip organization
2. Go to Settings (gear icon) → Account & Privacy
3. Under "API key", click "Show/change your API key"
4. Copy the key or create a bot for API access

For bots:
1. Go to Settings → Your bots
2. Add a new bot
3. Copy the bot's email and API key

### Discord Bot Token

1. Go to https://discord.com/developers/applications
2. Create a New Application
3. Go to "Bot" section and click "Add Bot"
4. Under "Token", click "Reset Token" and copy it
5. **Enable Required Privileged Gateway Intents:**
   - ✅ **Message Content Intent** (required for reading messages)
   - ✅ **Server Members Intent** (required for user search)
6. Invite bot to your server with these permissions:
   - Read Messages/View Channels
   - Send Messages
   - Read Message History

### Slack App Tokens

1. Go to https://api.slack.com/apps → **Create New App** → *From a manifest*, and paste:

```yaml
display_information:
  name: Connectome Agent
features:
  bot_user:
    display_name: connectome-agent
    always_online: true
oauth_config:
  scopes:
    bot:
      - channels:history
      - channels:read
      - groups:history
      - groups:read
      - im:history
      - im:read
      - im:write
      - mpim:history
      - mpim:read
      - chat:write
      - users:read
      - reactions:write
      - files:read
settings:
  event_subscriptions:
    bot_events:
      - message.channels
      - message.groups
      - message.im
      - message.mpim
  socket_mode_enabled: true
```

2. **Install to Workspace** → copy the **Bot User OAuth Token** (`xoxb-...`) → `SLACK_BOT_TOKEN`
3. Under **Basic Information → App-Level Tokens**, generate a token with the `connections:write` scope (`xapp-...`) → `SLACK_APP_TOKEN`
4. Invite the bot to channels you want it to see: `/invite @connectome-agent` (DMs work without invites — users can message the bot directly)

## Usage with Cursor

Add this to your Cursor MCP configuration (`~/.cursor/mcp.json`):

### Zulip Only (Default)

```json
{
  "mcpServers": {
    "zulip": {
      "command": "npx",
      "args": ["-y", "zulip-mcp-server"],
      "env": {
        "ZULIP_REALM": "https://your-org.zulipchat.com",
        "ZULIP_EMAIL": "your-bot@example.com",
        "ZULIP_API_KEY": "your-api-key",
        "ZULIP_SESSION_ID": "my_agent"
      }
    }
  }
}
```

### Discord Only

```json
{
  "mcpServers": {
    "discord": {
      "command": "npx",
      "args": ["-y", "zulip-mcp-server"],
      "env": {
        "ENABLE_ZULIP": "false",
        "ENABLE_DISCORD": "true",
        "DISCORD_TOKEN": "your-bot-token",
        "ZULIP_SESSION_ID": "my_agent"
      }
    }
  }
}
```

### Both Zulip and Discord

```json
{
  "mcpServers": {
    "chat": {
      "command": "npx",
      "args": ["-y", "zulip-mcp-server"],
      "env": {
        "ENABLE_DISCORD": "true",
        "ZULIP_REALM": "https://your-org.zulipchat.com",
        "ZULIP_EMAIL": "your-bot@example.com",
        "ZULIP_API_KEY": "your-api-key",
        "DISCORD_TOKEN": "your-bot-token",
        "ZULIP_SESSION_ID": "my_agent"
      }
    }
  }
}
```

**Alternative (if installed globally):**

```json
{
  "mcpServers": {
    "zulip": {
      "command": "zulip-mcp-server",
      "env": {
        "ZULIP_REALM": "https://your-org.zulipchat.com",
        "ZULIP_EMAIL": "your-bot@example.com",
        "ZULIP_API_KEY": "your-api-key",
        "ZULIP_SESSION_ID": "my_agent"
      }
    }
  }
}
```

**Multiple Agents:** To run multiple agents with separate state, use different session IDs:

```json
{
  "mcpServers": {
    "zulip-agent1": {
      "command": "node",
      "args": ["/path/to/zulip_mcp/build/index.js"],
      "env": {
        "ZULIP_REALM": "https://your-org.zulipchat.com",
        "ZULIP_EMAIL": "bot1@example.com",
        "ZULIP_API_KEY": "key1",
        "ZULIP_SESSION_ID": "agent1"
      }
    },
    "zulip-agent2": {
      "command": "node",
      "args": ["/path/to/zulip_mcp/build/index.js"],
      "env": {
        "ZULIP_REALM": "https://your-org.zulipchat.com",
        "ZULIP_EMAIL": "bot2@example.com",
        "ZULIP_API_KEY": "key2",
        "ZULIP_SESSION_ID": "agent2"
      }
    }
  }
}
```

After updating the configuration, **restart Cursor**.

## 📖 Usage Examples

### Stateful Workflow

```typescript
// Simple! Just ask for history - monitoring starts automatically
get_channel_history({ 
  channel: "analysts", 
  start_date: "today",
  format: "detailed"
  // auto_monitor: true by default - starts monitoring automatically!
})

// Check for unread messages (only new ones since last check!)
get_unread_messages({ 
  format: "summary",
  mark_as_read: true 
})

// Get messages from a specific date range and topic
get_channel_history({
  channel: "engineering",
  topic: "Sprint Planning",
  start_date: "2025-11-01",
  end_date: "2025-11-03T17:00:00",
  format: "detailed"
  // This also updates monitoring state!
})

// Optional: Explicitly manage monitoring
start_monitoring({ channels: ["qa", "support"] })
stop_monitoring({ channels: ["analysts"] })
get_monitored_channels()
```

### Natural Language Examples

Once configured in Cursor, you can simply ask:

**Zulip:**
- **"Get today's messages from #analysts"**
- **"Show me unread Zulip messages"**
- **"Get yesterday's history from #general"**
- **"Send a message to #team-updates about the deployment"**

**Discord:**
- **"Get today's Discord messages from channel 123456789"**
- **"Show me unread Discord messages"**
- **"List all Discord channels"**
- **"Send a message to Discord channel 123456789"**

**Both:**
- **"Check all my unread messages"** (if both enabled, check both!)
- The agent will see unread counts from both services via Resources

### Date/Time Formats

The `get_channel_history` tool supports flexible date inputs:

**Keywords:**
- `"today"` - Start of today (00:00)
- `"yesterday"` - Start of yesterday (00:00)
- `"now"` - Current time

**ISO Dates:**
- `"2025-11-03"` - Specific date (00:00)
- `"2025-11-03T14:30:00"` - Specific date and time

**Defaults:**
- `start_date` defaults to start of today
- `end_date` defaults to current time

### Output Formats

**Detailed** (default) - Full formatted messages:
```
[11/3/2025 08:43:21 AM] 📝 Topic: Kraków
👤 Lena C
💬 Насколько она использует для этого ллмки?
```

**Summary** - Quick overview:
```
[08:43] [Kraków] Lena C: Насколько она использует для этого ллмки?...
```

**Raw** - Complete JSON for programmatic processing

### Working with Mentions

#### Zulip Mentions

**Inbound (Reading):**
Mentions in retrieved messages are automatically formatted as:
```
@Daria Kroshka (uid:667)
```

**Outbound (Sending):**
Use the Zulip mention syntax in your messages:
```
@**Daria Kroshka**
```

**Finding Users:**
```typescript
find_user({ query: "daria" })
// Returns: mention_syntax: "@**Daria Kroshka**"
```

#### Discord Mentions

**Inbound (Reading):**
Mentions in retrieved messages are automatically formatted as:
```
@username#1234 (uid:123456789012345678)
```

**Outbound (Sending):**
Use Discord's mention format:
```
<@123456789012345678>
```

**Finding Users:**
```typescript
discord_find_user({ username: "daria" })
// Returns: mention_syntax: "<@123456789012345678>"
```

## API Reference

### start_monitoring

Start monitoring channels to track read/unread state.

```json
{
  "channels": ["analysts", "engineering"]
}
```

Returns: Status of each channel and last message IDs

### get_channel_history

Retrieve channel messages with date filtering.

```json
{
  "channel": "analysts",           // Required
  "topic": "Sprint Planning",      // Optional
  "start_date": "today",          // Optional (default: today 00:00)
  "end_date": "now",              // Optional (default: now)
  "max_messages": 500,            // Optional (default: 500)
  "format": "detailed"            // Optional: detailed|summary|raw
}
```

Returns:
- `message_count` - Number of messages in date range
- `formatted_history` - Beautifully formatted messages
- Date range info and metadata

### get_unread_messages

Get unread messages from monitored channels.

```json
{
  "channels": ["analysts"],       // Optional: specific channels, or all monitored
  "format": "detailed",          // Optional: detailed|summary|raw
  "mark_as_read": true          // Optional: update read state (default: true)
}
```

Returns:
- `total_unread` - Count of unread messages
- `channels_checked` - Status per channel
- `formatted_messages` - Formatted unread messages

### send_message

Send messages to streams or DMs.

```json
{
  "type": "stream",              // stream|private
  "to": "general",              // stream name or email(s)
  "topic": "Announcements",     // Required for streams
  "content": "Hello team!"      // Markdown supported
}
```

### Other Tools

- **get_monitored_channels** - List monitoring state
- **stop_monitoring** - Stop tracking channels
- **list_streams** - Browse all channels
- **get_stream_topics** - See topics in a channel
- **list_users** - View organization users
- **get_user_profile** - Get bot/user info
- **add_reaction** - Add emoji reactions to messages

## Development

```bash
# Install dependencies
npm install

# Build the project
npm run build

# Watch mode for development
npm run watch
```

## Workflow Example

Here's a typical workflow with the stateful server:

1. **Morning - Just Ask for History**
   ```
   "Get today's messages from #analysts"
   ```
   → Auto-starts monitoring and marks as read!

2. **Throughout the Day - Check Unreads**
   ```
   "What are the unread messages?"
   ```
   → Returns only NEW messages since last check

3. **Deep Dive - Specific Topics**
   ```
   "Show me messages from #engineering about API design from yesterday"
   ```
   → Automatically starts monitoring #engineering too

4. **Participate**
   ```
   "Send a message to #engineering topic 'API Review': Great points!"
   "Add a rocket reaction to message 14573574"
   ```

5. **State Persists**
   → Monitoring state saved to `~/.zulip_mcp_state/{session_id}.json`
   → Survives server restarts!
   → Each agent has separate state

## Why This Design?

### Traditional Approach
❌ Complex low-level APIs for each service  
❌ Need to manage anchors and message IDs manually  
❌ No state tracking  
❌ State lost on restart  
❌ Cognitive overhead  
❌ Raw data output  
❌ Separate tools for each service with no consistency

### Our Approach
✅ Unified ergonomic design across Zulip & Discord  
✅ Automatic state management  
✅ **Persistent state** across restarts  
✅ **Auto-monitoring** on history retrieval  
✅ **Multi-agent support** via session IDs  
✅ **Ambient awareness** via MCP Resources  
✅ Natural date/time handling  
✅ **Beautiful formatted output** everywhere  
✅ **Enable only what you need** via flags  
✅ "Just works" experience  

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## Troubleshooting

### Authentication Errors

- Verify your API key is correct
- Ensure your realm URL is complete (including `https://`)
- Check that your bot has the necessary permissions

### Monitoring Issues

- Make sure to call `start_monitoring` before using `get_unread_messages`
- The server maintains state per session (state resets on server restart)
- Use `get_monitored_channels` to check current monitoring state

### Date Parsing

- Dates are parsed in UTC by default
- Use ISO 8601 format for precise timestamps
- Keywords ("today", "yesterday") use local time

## Links

- [Zulip API Documentation](https://zulip.com/api/)
- [zulip-js Library](https://github.com/zulip/zulip-js)
- [Model Context Protocol](https://modelcontextprotocol.io/)
