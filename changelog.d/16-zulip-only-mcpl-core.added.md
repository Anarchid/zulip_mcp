- Closed-channel delivery (#16): mentions and DMs on channels the host has
  not opened arrive as `push/event` with the missed-ambient tally in the
  origin; ambient on closed channels is counted (`channel_missed`).
- Catch-up: persisted per-channel watermarks that advance only on the host's
  acceptance; a `<missed>` block per channel on reconnect (full backscroll
  for channels the host had open, mention ±7 otherwise), paged up to
  `ZULIP_CATCHUP_LIMIT` and capped by `ZULIP_MISSED_BLOCK_MAX_CHARS`; a
  Zulip event-queue expiry is healed from history for open and closed
  channels alike.
- DM conversations are channels (`zulip:dm:<ids>`), discovered from history
  and announced on the fly; `send_dm` by name, email or id (closes #5).
- History on `channels/open` (`limit`, `beforeMessageId`, `sinceLastSeen`),
  `fetch_history` with id cursors and `fetch_around` within the anchor's
  conversation.
- Filters plane: one hot-reloaded JSON file (stream/DM allowlists, muted
  streams, reaction visibility, operator-owned reaction suppression) with
  `filters_get` / `filters_update` / `mute_channel` / `unmute_channel` /
  `refresh_channels`. The host's reaction-suppression baseline
  (`ZULIP_` or `DISCORD_SUPPRESSED_REACTIONS_BASELINE`) applies at every
  start and is never persisted.
- Images inlined on live delivery, downsampled to model-max; small text
  attachments inlined (closes #6).
- Live reactions per channel (`set_reaction_visibility`), reactions on
  history lines, `remove_reaction`, `list_emojis`, realm-emoji resolution.
- Rollback checkpoints on every `zulip.messaging` tool result
  (`state.checkpoint`), `channels/acknowledge` by reaction, sends split at
  the realm's message length, agent-visible timestamps (`AGENT_TIMEZONE`,
  `AGENT_TIMESTAMP_STYLE`).
