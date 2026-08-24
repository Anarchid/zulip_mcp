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
  open and close. Opening a channel subscribes the bot to the stream first
  (Zulip only delivers events to subscribers) — a stream the bot cannot
  subscribe to fails the open rather than opening onto silence — and can
  return backscroll atomically.
- Delivery model: messages on **open** channels arrive as `channels/incoming`;
  **mentions and DMs on closed channels** arrive as `push/event` so they always
  reach the agent; ambient traffic on closed channels is dropped and counted
  (`channel_missed`).
- Catch-up: a persisted per-channel watermark advances only when the host
  has accepted a message (itemized `channels/incoming` results, acknowledged
  `push/event`). On the next connection a `<missed>` block per channel
  delivers what arrived meanwhile (full backscroll for channels the host had
  open, mention ± 7 messages for the rest), paged up to `ZULIP_CATCHUP_LIMIT`
  and capped in size. A Zulip event-queue expiry is healed the same way —
  open channels replayed, closed channels' mentions pushed — not merely
  reported. Live events that arrive before the sweep has run are held, so a
  live delivery can never jump the watermark over the offline gap.
- RFC-001 tags on every message (`chat:mention`, `chat:dm`, `chat:ambient`,
  `chat:from-bot`, `chat:has-image`, `chat:reaction`, …) for the host's wake
  policy. Images are inlined on live delivery, downsampled to model-max.
- Reactions, opt-in per channel, carry only reaction tags so a tag-keyed
  wake policy ignores them; operator-owned suppression of reaction markers
  plus the host-injected baseline; rollback checkpoints minted by every
  messaging tool; acknowledge by reaction; typing indicators routed to the
  active topic.

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
| `ZULIP_SESSION_ID` | bot email (from env or zuliprc) | Keys the persistent state files (monitoring, delivery, filters); set it when two sessions of one bot share a state dir |
| `ZULIP_STATE_DIR` | `~/.zulip_mcp_state` | Where those files live |
| `ZULIP_SUBSCRIBE` | — | Streams to subscribe the bot to on startup |
| `ZULIP_FILTERS_FILE` | `<state dir>/<session>.filters.json` | The filters plane file (hot-reloaded) |
| `ZULIP_STREAMS`, `ZULIP_DM_USERS`, `ZULIP_MUTED_STREAMS` | — | Seed for the filters file on first materialization |
| `ZULIP_SUPPRESSED_REACTIONS_BASELINE` | `DISCORD_SUPPRESSED_REACTIONS_BASELINE` | Host-owned reaction markers withheld from the model; re-read every start, never persisted (connectome-host injects the `DISCORD_` name into every MCPL child) |
| `ZULIP_CATCHUP_LIMIT` | 3000 | Per-channel ceiling for catch-up and gap recovery (max 10000). For an always-open desk channel a few hundred is plenty |
| `ZULIP_MISSED_BLOCK_MAX_CHARS` | 40000 | Size cap on one `<missed>` block; the oldest lines are elided with a `fetch_history` pointer |
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

Every `zulip.messaging` tool result carries `state.checkpoint` (SPEC §8);
`state/rollback` to a checkpoint deletes what the bot sent after it —
tool sends and `channels/publish` alike. Disabling `zulip.messaging` stops
its delivery (incoming and push) and its tools at once.

**Wake policy.** Closed-channel mentions and DMs, new-DM announcements and
`<missed>` catch-up blocks arrive as `push/event`, not `channels/incoming`.
A host whose gate defaults to skip needs a policy on the `mcpl:push-event`
scope (e.g. `tags: ['chat:addressed']` → `always`) or they land in context
without a turn. Conversely, reactions on an open channel are ordinary
`channels/incoming` messages tagged `chat:reaction` — an unconditional
"always wake on this channel" policy wakes on them too; key on tags.

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
- `mutedStreams` — nothing from these reaches the agent on any surface:
  live delivery, mentions, backscroll on open, context injection, reactions,
  catch-up and gap recovery. The pull tools (`fetch_history`, …) still work.
- `reactionChannels` — channels showing live reactions.
- `suppressedReactionEmojis` — reaction markers withheld from every
  model-visible surface. Operator-owned: the agent's tools cannot carry this
  key, and `filters_get` reports it only as a count and digest. The host's
  baseline (`*_SUPPRESSED_REACTIONS_BASELINE`) is added on top at every
  start and is never written into the file.

Every key is an authorization list: a wrong-typed value makes the file
invalid (last-known-good stays in force) rather than reading as
"unrestricted". An unparseable or vanished file keeps the last-known-good
filters in force and marks the plane stale; updates from the tools are
refused until it is repaired. A file that cannot be created at start is a
startup failure, not a degraded mode.

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
  channel subscribes the bot (and fails if it cannot — a private stream the
  bot was not invited to answers `success` with the stream under
  `unauthorized`, which this server treats as a refusal); `ZULIP_SUBSCRIBE`
  and `listen` do it explicitly. `listen` alone leaves the channel *closed*:
  ambient is tallied and mentions become push events — the host must open
  the channel to receive its traffic.
- **State from 2.x.** The plain-MCP monitor cursors (`<session>.json`) are
  read as before but are not migrated into delivery watermarks: the first
  3.x start anchors catch-up at "now".
- **No per-call timeouts** on the Zulip API yet: the serve loop handles one
  host request at a time, so a hung Zulip call stalls the requests behind it
  until the host's own timeout.
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
