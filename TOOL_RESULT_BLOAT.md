# Tool Result Bloat: `raw_data` in listing tools

## Problem

The listing tools (`list_streams`, `get_stream_topics`, `list_users`) return both a compact `formatted_list` and a full `raw_data` dump of the Zulip API response. The `raw_data` is 10-20x larger than the formatted summary and contains fields the agent never uses.

Measured from a real run (114 streams):

| Field | Size | % of total | Useful? |
|-------|------|-----------|---------|
| `formatted_list` | 6 KB | 4% | Yes — channel names, subscriber counts, message rates, descriptions |
| `raw_data` | 93 KB | 64% | No — 29 fields per stream including `can_add_subscribers_group`, `can_delete_any_message_group`, `can_move_messages_within_channel_group`, `rendered_description`, etc. |
| JSON overhead | 46 KB | 32% | Structural |
| **Total** | **145 KB** | | |

The same pattern applies to `get_stream_topics` (~54K with raw_data) and `list_users`.

## Impact

This bloat propagates through the system:
- Each tool result is stored verbatim in the agent's context
- When forking, the parent's context (including these results) is copied to every child
- 3 forks × 145K = 435K chars of raw Zulip API data carried into agents that only need channel names and IDs
- The `maxMessageTokens` truncation in AutobiographicalStrategy mitigates this at compile time, but the truncation is positional (chops from the end) — if `raw_data` appears before `formatted_list` in the JSON, the useful summary gets cut instead

## Fix

In `src/index.ts`, for each listing tool, replace the `raw_data` field with a minimal lookup map (stream_id → name) so the agent can make follow-up calls. The `formatted_list` already contains everything needed for decision-making.

### `list_streams` (line ~1035-1039)

Before:
```js
return {
  total_streams: streams.length,
  formatted_list: `📋 **${streams.length} Streams**\n\n${formatted}`,
  raw_data: result,
};
```

After:
```js
return {
  total_streams: streams.length,
  formatted_list: `📋 **${streams.length} Streams**\n\n${formatted}`,
  stream_ids: Object.fromEntries(streams.map((s) => [s.name, s.stream_id])),
};
```

### `get_stream_topics` (line ~1052-1056)

Before:
```js
return {
  topic_count: topics.length,
  formatted_list: `📑 **${topics.length} Topics**\n\n${formatted}`,
  raw_data: result,
};
```

After:
```js
return {
  topic_count: topics.length,
  formatted_list: `📑 **${topics.length} Topics**\n\n${formatted}`,
};
```

### `list_users` (line ~1074-1079)

Before:
```js
return {
  total_users: users.length,
  active_users: users.filter((u) => u.is_active).length,
  formatted_list: `👥 **${users.length} Users**...\n\n${formatted}`,
  raw_data: result,
};
```

After:
```js
return {
  total_users: users.length,
  active_users: users.filter((u) => u.is_active).length,
  formatted_list: `👥 **${users.length} Users**...\n\n${formatted}`,
};
```

Expected result size for `list_streams` after fix: ~8 KB (from 145 KB) — an 18x reduction.
