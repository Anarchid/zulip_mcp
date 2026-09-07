- `fetch_around` stays inside the anchor's conversation; without a narrow
  Zulip answered with the realm-wide timeline (#16).
- `fetch_attachment` refuses a protocol downgrade from the realm's scheme
  (an `http` URL on an `https` realm), which would have sent the bot's
  credentials in clear.
- Live events received before the catch-up sweep has run are held and
  released in order afterwards, so a live delivery cannot advance the
  watermark over the offline gap the sweep is about to fetch; delivery
  opens only once the buffer is empty, so a live message arriving during
  the release cannot overtake the held ones and bury them. The sweep
  catches up from the watermarks as they stood when the connection began,
  so a `channel_open` with backscroll answered meanwhile cannot hide the
  gap from it.
- Catch-up pages past a page holding nothing but the bot's own messages
  instead of reading it as the end of history.
- `ZULIP_MISSED_BLOCK_MAX_CHARS` is a hard cap on the whole `<missed>`
  block, header and notes included; a single over-long line is cut and
  says so. Catch-up notes point at DM conversations by channel id, which
  `fetch_history` now reads.
- Suppressed reaction markers are withheld from the legacy `raw` history
  format too.
- Shutdown (EOF, `SIGTERM`, `SIGINT`) waits for deliveries still fetching
  their attachments, flushes the last delivery window and writes state
  atomically.
