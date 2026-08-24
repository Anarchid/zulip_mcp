# Zulip MCP Server

A Zulip server for AI agents, speaking plain **MCP** (Model Context Protocol) to
any client and **MCPL** (MCP Live) to hosts that support it — live delivery of
stream messages and DMs, host-managed channels, catch-up after downtime, a
hot-reloadable filters plane, and a stateful tool surface for reading and
writing Zulip.

Built on [`@animalabs/mcpl-core`](https://github.com/anima-research/mcpl-core-ts),
the same substrate as [discord-mcpl](https://github.com/anima-research/discord-mcpl)
and [slack-mcpl](https://github.com/anima-research/slack-mcpl). Zulip only — the
Discord and Slack adapters that once lived here moved to those servers.

## What you get

**Plain MCP (Claude Code, Cursor, any MCP client)**

- 29 tools: stream/topic history with natural dates or id cursors,
  `fetch_around`, sending to streams and DMs, editing, deleting, reactions,
  user lookup, attachments, and a persistent read/unread monitor.
- Resources: `zulip://unread/summary`, `zulip://monitoring/status`,
  `zulip://channel/{stream}/unread`.

**MCPL hosts (connectome-host and friends)**

- Every stream and DM conversation the bot can see is a channel the host can
  open and close. Opening a channel subscribes the bot to the stream (Zulip
  only delivers events to subscribers) and can return backscroll atomically.
- Delivery model: messages on **open** channels arrive as `channels/incoming`;
  **mentions and DMs on closed channels** arrive as `push/event` so they always
  reach the agent; ambient traffic on closed channels is dropped and counted
  (`channel_missed`).
- Catch-up: every forward advances a persisted watermark. On the next
  connection a `<missed>` block per channel delivers what arrived meanwhile
  (full backscroll for channels the host had open, mention ± 7 messages for
  the rest). A Zulip event-queue expiry is healed from history, not merely
  reported.
- RFC-001 tags on every message (`chat:mention`, `chat:dm`, `chat:ambient`,
  `chat:from-bot`, `chat:has-image`, `chat:reaction`, …) for the host's wake
  policy. Images are inlined on live delivery, downsampled to model-max.
- Reactions, opt-in per channel, never wake the agent; operator-owned
  suppression of reaction markers; rollback checkpoints; acknowledge by
  reaction; typing indicators routed to the active topic.

## Installation

```bash
npm install
npm run build
```

Requires Node 20+. `npm test` runs the suite (`node --test`, no network).

## Configuration

Credentials, via environment or a zuliprc file:

```bash
export ZULIP_REALM=https://your-org.zulipchat.com
export ZULIP_EMAIL=your-bot@your-org.zulipchat.com
export ZULIP_API_KEY=your-api-key
# or
export ZULIP_RC_PATH=/path/to/zuliprc
```

Everything else is optional. `.env.example` lists every variable; the ones you
are likely to touch:

| Variable | Default | Meaning |
|---|---|---|
| `ZULIP_SESSION_ID` | bot email | Keys the persistent state files (monitoring, delivery, filters) |
| `ZULIP_STATE_DIR` | `~/.zulip_mcp_state` | Where those files live |
| `ZULIP_SUBSCRIBE` | — | Streams to subscribe the bot to on startup |
| `ZULIP_FILTERS_FILE` | `<state dir>/<session>.filters.json` | The filters plane file (hot-reloaded) |
| `ZULIP_STREAMS`, `ZULIP_DM_USERS`, `ZULIP_MUTED_STREAMS` | — | Seed for the filters file on first materialization |
| `ZULIP_CATCHUP_LIMIT` | 3000 | Per-channel ceiling for catch-up and gap recovery |
| `ZULIP_BACKSCROLL_DEFAULT`, `ZULIP_BACKSCROLL_CHANNELS` | 500 | History cap on `channels/open`, per stream as `general:50,dev:200` |
| `ZULIP_INLINE_IMAGES`, `ZULIP_INLINE_IMAGES_MAX`, `ZULIP_ATTACHMENT_INLINE_MAX_BYTES` | true, 4, 5120 | Attachment inlining on live delivery |
| `AGENT_TIMEZONE`, `AGENT_TIMESTAMP_STYLE` | system, `full` | Agent-visible timestamps in catch-up blocks |
| `MCPL_ENABLED` | true | `false` forces plain-MCP mode even for MCPL hosts |

### Plain MCP client (Claude Code, Cursor)

```json
{
  "mcpServers": {
    "zulip": {
      "command": "node",
      "args": ["/path/to/zulip-mcp/build/index.js"],
      "env": {
        "ZULIP_RC_PATH": "/path/to/zuliprc",
        "ZULIP_SESSION_ID": "my-agent"
      }
    }
  }
}
```

### MCPL host

The server negotiates MCPL when the host advertises `experimental.mcpl` in
`initialize`. It stays inert until the host's `featureSets/update` Request
establishes the capability grant (SPEC 0.5 §5.3 — absence is denial), then
registers channels and runs the catch-up sweep. Stdio is the default
transport; `--tcp <port>` serves one connection at a time on localhost.

Feature sets: `zulip.messaging` (channels, push events, tools, rollback),
`zulip.history` (the read tools), `zulip.context` (recent history injected
before inference for open channels).

## Channels

| Channel id | What it is |
|---|---|
| `zulip:<stream>` | A stream. Topics are threads: incoming messages carry the topic as `threadId`; publishes go to the topic of the most recent incoming message, else `mcpl`. |
| `zulip:dm:<ids>` | A DM conversation, keyed by the other parties' sorted user ids (`zulip:dm:42`, `zulip:dm:7+42`). Discovered from recent DM history and announced on the fly (`channels/changed`) when someone new writes. |

Descriptors carry `capabilities.history` (`maxMessages`, `supportsBeforeMessage`,
`supportsSinceLastSeen`); `channels/open` may ask for history and gets it
before the lifecycle commits.

## Filters plane

One JSON file is the desired state for what reaches the agent. It always exists
once the server has started (seeded from the environment), is authoritative
from then on, and is hot-reloaded within seconds — no change here ever needs a
restart.

```json
{
  "streams": ["general", "dev"],
  "dmUsers": ["42", "ann@example.com"],
  "mutedStreams": ["random"],
  "reactionChannels": ["zulip:general"],
  "suppressedReactionEmojis": ["biohazard"]
}
```

- `streams` — allowlist (absent = every stream the bot can see). Gates
  discovery and delivery.
- `dmUsers` — who may DM the bot (absent = anyone). Empty means unrestricted,
  deliberately: unsetting a variable must not silently lose every DM.
- `mutedStreams` — nothing from these reaches the agent, mentions included.
- `reactionChannels` — channels showing live reactions.
- `suppressedReactionEmojis` — reaction markers withheld from every
  model-visible surface. Operator-owned: the agent's tools cannot carry this
  key, and `filters_get` reports it only as a count and digest.

An unparseable or vanished file keeps the last-known-good filters in force and
marks the plane stale; updates from the tools are refused until it is repaired.

## Tools

**Reading**
`fetch_history` (stream/topic, `before`/`after` id cursors, ids on every line),
`fetch_around` (window centred on a message, within its conversation),
`get_channel_history` (natural dates), `get_unread_messages`,
`list_streams`, `get_stream_topics`, `list_users`, `find_user`,
`get_user_profile`, `fetch_attachment`, `list_emojis`.

**Writing**
`send_message`, `send_dm` (by name, email, or id), `edit_message`,
`delete_message`, `add_reaction`, `remove_reaction`.

**Attention**
`listen` / `unlisten` (Zulip stream subscription), `start_monitoring` /
`stop_monitoring` / `get_monitored_channels` (read cursors for the plain-MCP
unread tools), `channel_missed`, `mute_channel` / `unmute_channel`,
`set_reaction_visibility`, `filters_get` / `filters_update`, `refresh_channels`.

Message ids are realm-global and monotonic, which makes them cursors: every
history line, `<missed>` block, and incoming message leads with one so the
agent can `fetch_around` it.

## State on disk

Under `ZULIP_STATE_DIR`, keyed by session:

- `<session>.json` — the plain-MCP monitor (streams, last-read ids)
- `<session>.delivery.json` — watermarks, missed tallies, last-open channels
- `<session>.filters.json` — the filters plane (unless `ZULIP_FILTERS_FILE`)

## Notes for operators

- **Subscription is not optional.** Zulip delivers stream events only to
  subscribers, even with `all_public_streams` on the event queue. Opening a
  channel subscribes the bot; `ZULIP_SUBSCRIBE` and `listen` do it explicitly.
- **zulip-js quirks** (in `platforms/zulip-events.ts`): booleans in POST bodies
  must be strings, arrays must be raw arrays; API errors come back as values
  (`result: 'error'`), which this server turns into thrown errors.
- **Debugging delivery:** run the server standalone with the env of the recipe
  and watch stderr — hosts do not always capture MCPL child stderr.

## Development

```bash
npm run build      # tsc → build/
npm test           # node --test test/*.test.ts (via tsx)
npm run watch
```

`test/server.test.ts` drives the real `McplConnection` over an in-memory stream
pair through the handshake, the policy exchange, registration, delivery,
catch-up, and the tools — the fastest way to see the wire behaviour.

## License

MIT
