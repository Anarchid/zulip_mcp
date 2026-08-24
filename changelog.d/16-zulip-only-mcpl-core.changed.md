- `channels/open` subscribes the bot to the stream first and fails
  (`ERR_CHANNEL_OPEN_FAILED`) when it cannot — a private stream the bot was
  not invited to no longer opens onto silence (#16).
- A muted stream is silent on every surface: live delivery, mentions,
  backscroll on open, context injection, reactions, catch-up and gap
  recovery.
- `filters_update` refuses to remove the last allowed stream (an empty
  allowlist is unrestricted); a wrong-typed key in the filters file makes
  the file invalid rather than reading as unrestricted; a filters file that
  cannot be created is a startup failure.
- Disabling `zulip.messaging` stops its delivery (incoming and push) and
  its tools at once; `filters_update` and `refresh_channels` belong to it.
- `edit_message`, `delete_message`, `list_streams`, `get_stream_topics`,
  `list_users` and `get_user_profile` report a Zulip API error as an error
  instead of returning it as a result.
