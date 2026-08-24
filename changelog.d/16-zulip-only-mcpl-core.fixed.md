- `fetch_around` stays inside the anchor's conversation; without a narrow
  Zulip answered with the realm-wide timeline (#16).
- `fetch_attachment` refuses the realm host over plain `http`, which would
  have sent the bot's credentials in clear.
- Live events received before the catch-up sweep has run are held and
  released afterwards, so a live delivery cannot advance the watermark over
  the offline gap the sweep is about to fetch.
- Shutdown (EOF, `SIGTERM`, `SIGINT`) flushes the last delivery window and
  writes state atomically.
