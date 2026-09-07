- `channels/open` subscribes the bot to the stream first and fails
  (`ERR_CHANNEL_OPEN_FAILED`) when it cannot — a private stream the bot was
  not invited to no longer opens onto silence (#16).
- A muted stream is silent on every surface: live delivery, mentions,
  backscroll on open, context injection, reactions, catch-up and gap
  recovery. So is a stream outside the `streams` allowlist, and a DM
  sender outside `dmUsers` — the allowlists are enforced on every surface,
  not only on live events.
- `filters_update` refuses to remove the last allowed stream (an empty
  allowlist is unrestricted); a wrong-typed key in the filters file makes
  the file invalid rather than reading as unrestricted; a filters file that
  cannot be created — or exists but cannot be parsed — is a startup
  failure, never a run on the env seed.
- Disabling `zulip.messaging` stops its delivery (incoming and push) and
  its tools at once, queued and recovered traffic included; what was
  withheld is replayed when it is enabled again. `filters_update` and
  `refresh_channels` belong to it.
- `edit_message`, `delete_message`, `list_streams`, `get_stream_topics`,
  `list_users`, `get_user_profile`, `find_user` and `list_emojis` report a
  Zulip API error as an error instead of returning it as a result.
- `channels/open` asks Zulip about the subscription every time rather than
  remembering one, so a subscription dropped through `unlisten` cannot let
  a later open succeed onto silence.
- Over TCP, every connection starts from a fresh grant, registration and
  catch-up sweep; nothing of a previous peer's session carries over.
