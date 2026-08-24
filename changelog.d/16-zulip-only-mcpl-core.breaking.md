- **Host configs:** the Discord and Slack platforms are gone (#16); this is
  a Zulip-only server. Discord runs on
  [discord-mcpl](https://github.com/anima-research/discord-mcpl), Slack on
  [slack-mcpl](https://github.com/anima-research/slack-mcpl). The `ENABLE_*`
  switches, `DISCORD_*`/`SLACK_*` variables and the `discord.js`/`@slack/*`
  dependencies are removed; `ENABLE_ZULIP` is ignored.
- **Hosts:** the hand-rolled MCPL layer is replaced by `@animalabs/mcpl-core`
  0.3.0. Feature sets are `zulip.messaging` / `zulip.history` /
  `zulip.context`; grant semantics follow the core (`*` matches one segment,
  a malformed policy fails closed, a `featureSets/update` Notification never
  rewrites the grant, `enabled: []` is empty). `pushEvents` is declared and
  used. Requires Node 20+.
- **Operators:** the persistent-state session id defaults to the bot email
  whichever way the credentials arrived (zuliprc-only deployments were keyed
  `default`); set `ZULIP_SESSION_ID` to keep existing state-file names.
