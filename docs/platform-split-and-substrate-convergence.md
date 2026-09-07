# Design: Platform Split & MCPL Substrate Convergence

> **Status (September 2026):** landed as PR #16. Two details below are
> historical: the shared substrate ships as `@animalabs/mcpl-core` (the
> `@connectome/mcpl-core` name predates the org move), and this server
> declares its feature sets in the core's record shape
> (`buildServerCapabilities` in `src/feature-sets.ts`), not the array form
> the C.2 table anticipated. The sequencing, acceptance criteria and recipe
> coordination sections are as executed.


**Status:** Implemented (Aug 24 2026) — see the end-state checklist
**Date:** 2026-06-30 (proposal); 2026-08-24 (landed)
**Scope:** `zulip-mcp/` (this repo), `discord-mcpl/`, new `slack-mcpl/`, `mcpl-core-ts/`, and the recipes/host that consume them.
**Decisions baked in:** *full* substrate convergence; in-house ownership (we now hold write privileges to `zulip_mcp`, so the upstream-merge constraint is dissolved).

---

## 1. Problem

The Zulip MCPL server grew a multi-platform empire. A stacked-PR fork chain on `Anarchid/zulip_mcp` added Discord and Slack alongside Zulip:

- `feat/attachments-images` (PR #7) — attachment/image surfacing. *Currently checked out on disk.*
- `pr8-slack` / `Tengro:slack-integration` (PR #8, stacked on #7) — a `PlatformAdapter` refactor that bolted **Discord** and **Slack** into the single server binary.

The result is one server with `discord.js` + `@slack/web-api` + `@slack/socket-mode` dependencies, three platform adapters, and triple the tool surface — gated behind `ENABLE_ZULIP/DISCORD/SLACK` flags. In every deployed config it runs Zulip-only:

- `connectome-host/mcpl-servers.json`: `ENABLE_ZULIP=true`, `ENABLE_DISCORD=false`, no Slack — and wires Discord *separately* to the standalone `discord-mcpl/`.
- Lynx `clerk.json` / `knowledge-miner.json`: same, Zulip-only.

So the non-Zulip code is **dead weight as shipped**, and there are now **two divergent MCPL substrates** in the codebase (see §3). This document specifies decomposing the empire back into three single-purpose servers on one shared substrate.

## 2. Current state (what we're starting from)

The `pr8-slack` branch is, fortunately, already well-factored:

```
zulip-mcp/ (fork @ pr8-slack)
├── src/mcpl/                 ← HAND-ROLLED MCPL substrate (the problem)
│   ├── transport.ts          (175)  stdio JSON-RPC framing
│   ├── client.ts             (99)   host-call client
│   ├── dispatcher.ts         (71)   request routing
│   ├── types.ts              (201)  McplServerCapabilities v0.4, FeatureSetDeclaration (Record-shaped)
│   ├── channels.ts           (315)  ChannelManager — platform-agnostic, routes on channel-ID prefix
│   ├── context.ts            (135)  beforeInference context provider
│   └── feature-sets.ts       (56)   buildFeatureSets(platforms[]) → Record<string, FeatureSetDeclaration>
├── src/platforms/            ← clean adapter seam (the asset)
│   ├── adapter.ts            (92)   PlatformAdapter interface
│   ├── zulip.ts              (217)
│   ├── zulip-events.ts       (111)
│   ├── discord.ts            (163)  live gateway push via messageCreate
│   └── slack.ts              (324)  live Socket Mode push
├── src/content.ts            (511)  formatDiscordContent / formatSlackText / attachment refs
└── src/index.ts              (2547) client init + ALL tool defs + adapter wiring + assembly
```

Key properties already true:

- **`PlatformAdapter` is a real seam.** Each adapter owns one connection; the MCPL layer routes purely on the `zulip:` / `discord:` / `slack:` channel-ID prefix and never inspects platform internals.
- **Both non-Zulip adapters already implement MCPL push.** `discord.ts` hooks `discordClient.on('messageCreate')`; `slack.ts` runs `@slack/socket-mode` and emits via `startEvents(onMessage)`. The "L" (ambient server-push) is *done* for all three platforms inside this fork.
- **Capabilities are platform-derived.** `feature-sets.ts` builds `{type}.messaging` + `{type}.context` for each enabled platform; `index.ts` assembles the active platform list from the `ENABLE_*` flags.

The monolith is concentrated in `index.ts`: per-platform client init, per-platform tool definitions, and the final adapter-map wiring all live there and branch on the flags.

## 3. The substrate divergence (the deeper untidiness)

Two MCPL implementations coexist:

| | `zulip-mcp` (fork) | `discord-mcpl/` (standalone) |
|---|---|---|
| Substrate | **hand-rolled** `src/mcpl/` | **shared** `@connectome/mcpl-core` (mcpl-core-ts) |
| Capabilities | `McplServerCapabilities` v0.4, local type | from mcpl-core |
| Feature sets | `Record<string, FeatureSetDeclaration>` | `FeatureSetDeclaration[]` (array) |
| Transport/dispatch | own `transport.ts` + `client.ts` + `dispatcher.ts` | `McplConnection` from mcpl-core |
| Tag ontology | none | RFC-001 (`chat:addressed`, `chat:mention`, …) |
| Rollback / checkpoints | none | yes (`state.ts`) |

`mcpl-core-ts` exports the canonical surface (`McplConnection`, capability builders, feature-set & tag types, methods, errors) and is already the substrate for `discord-mcpl`. The fork predates/ignores it. **Full convergence (this design) makes `mcpl-core-ts` the single source of truth for all three servers.**

## 4. Cross-literature check: build vs. adopt

Before committing to in-house servers, we surveyed the external Slack/Discord MCP ecosystem. Verdict: **"the L" is a genuine gap — adopt nothing, harvest our own code.**

### Discord — we already have the best server, in-house
`discord-mcpl/` is *superior* to every community option: shared substrate, RFC-001 tags, rollback, subscriptions, DMs, reactions, `fetch_around`, `channel_missed`. The community field is pull-only:

| Server | ★ | Push? | Live gateway held? |
|---|---|---|---|
| SaseQ/discord-mcp | ~380 | pull | yes (JDA) |
| v-3/discordmcp | ~216 | pull | yes |
| hanweg/mcp-discord | ~161 | pull | yes |
| barryyip0625/mcp-discord | ~95 | pull | yes (streamable-http) |
| tolgasumer/discord-mcp | ~4 | **push** (custom `discord/*` ns) | yes |

Adopting any of these would *regress* below `discord-mcpl`. **Don't.**

### Slack — no viable base exists
The leaders are **stateless REST wrappers** with no live connection; uplift = standing up Socket Mode from scratch ≈ build cost:

| Server | ★ | Push? | Live socket held? |
|---|---|---|---|
| korotovsky/slack-mcp-server | ~1693 | pull | no (REST) |
| `@modelcontextprotocol/server-slack` | — | pull | no — **ARCHIVED** |
| zencoderai/slack-mcp-server | ~69 | pull | no |
| slackapi/slack-mcp-plugin (official) | ~71 | pull | no — real-time is open req #22 |
| trtd56/AskOnSlackMCP | ~6 | blocking call | yes (Socket Mode) |
| bahakizil/slack_mcp | ~2 | internal only | yes (Socket Mode) |

Only ≤6★ toys touch Socket Mode, and none emit unsolicited `notifications/*`. Our fork's `slack.ts` (324 lines, working Socket Mode push, self-filtered) is the **single most valuable Slack-MCPL asset in the ecosystem for our purposes.** Harvest it.

**Conclusion:** the literature confirms in-house. We extract our own adapters onto the shared substrate; we do not import external servers.

## 5. Target architecture

Three single-purpose servers, all on `mcpl-core-ts`, each enabled à la carte in recipes:

```
mcpl-core-ts/         @connectome/mcpl-core   — single MCPL substrate (source of truth)
zulip-mcp/            Zulip only, migrated onto mcpl-core
discord-mcpl/         Discord only (already exists, already converged)
slack-mcpl/  (NEW)    Slack only, built on mcpl-core, seeded from fork's slack.ts
```

No multi-platform binary. No `ENABLE_*` flags. No dead dependencies. Adding a fourth platform later = copy the `discord-mcpl` 3-file template (`server.ts` + `feature-sets.ts` + `tools.ts`) + one adapter.

## 6. Workstreams

Three independent streams, ordered by value-to-effort. A and B can proceed in parallel; C (the substrate migration) is the largest and gates the "full convergence" goal.

### Workstream A — Discord: delete, don't split *(lowest effort, do first)*

The fork's Discord adapter is pure redundancy against the superior `discord-mcpl/`.

1. Branch `refactor/zulip-only` off the post-merge tip.
2. Remove `src/platforms/discord.ts`; drop `discord.js` from `package.json` + lockfile.
3. Strip all `ENABLE_DISCORD` branches, Discord client init, and Discord tool definitions from `index.ts`.
4. Remove `formatDiscordContent` / Discord paths from `content.ts`.
5. Audit consumers point Discord at `discord-mcpl/` (host already does; Lynx recipes have Discord off — no-op). 
6. **Acceptance:** build green, no `discord` symbol remains, Zulip tests pass.

*Result: ~160 LOC + `discord.js` gone, nothing lost.*

### Workstream B — Slack: extract to new `slack-mcpl/` repo

Mirror the `discord-mcpl/` template so Slack lands on the shared substrate, not the hand-rolled one.

1. New repo `slack-mcpl/` at the monorepo root; package `@connectome/slack-mcpl`; dependency `"@connectome/mcpl-core": "file:../mcpl-core-ts"` (matching discord-mcpl).
2. Copy the `discord-mcpl` scaffold (`server.ts`, `feature-sets.ts`, `tools.ts`, `channels.ts`, `state.ts`, `index.ts`) as the structural template.
3. Port the adapter: fork `slack.ts` → `slack-adapter.ts`, retaining `@slack/web-api` + `@slack/socket-mode` and the **Socket Mode push** path. Carry the self-filter (`slackSelfUserId`).
4. Port helpers `formatSlackText` / `extractSlackUserIds` / `resolveSlackUserNames` from `content.ts`.
5. Declare `slack.messaging` / `slack.channels` / `slack.history` feature sets in the array shape, with the RFC-001 tag ontology for parity with discord-mcpl (`chat:addressed`, `chat:mention`, `chat:dm`, `chat:ambient`, …).
6. Harvest tests from `pr8-slack`: `formatSlackText.test.ts`, `slackAdapter.test.ts`, `slackHistory.test.ts`.
7. Wire into `mcpl-servers.json` as a sibling `slack` server, enabled per-recipe with Slack creds (`SLACK_BOT_TOKEN`, `SLACK_APP_TOKEN`).
8. **Acceptance:** standalone server boots, Socket Mode connects, a posted Slack message produces an `mcpl:channel-incoming` event; tool round-trips for send/history work.

*Note: no typing indicator — Slack's Web API exposes none for bots; the adapter omits `sendTyping` (already the case in `slack.ts`).*

### Workstream C — Zulip: slim back AND migrate onto `mcpl-core-ts` *(full convergence)*

#### C.1 Slim to Zulip-only
- Keep `src/platforms/zulip.ts` + `zulip-events.ts`. Keep the `PlatformAdapter` interface — one implementer, but it's the documented seam and costs nothing.
- Remove residual multi-platform assembly from `index.ts` (single adapter, no flag matrix).

#### C.2 Replace hand-rolled `src/mcpl/` with `@connectome/mcpl-core`
Add `"@connectome/mcpl-core": "file:../mcpl-core-ts"`; then map:

| Delete (fork) | Replace with (mcpl-core / discord-mcpl pattern) |
|---|---|
| `src/mcpl/transport.ts` | `McplConnection` (handles stdio JSON-RPC framing) |
| `src/mcpl/client.ts` | `McplConnection` host-call surface |
| `src/mcpl/dispatcher.ts` | `McplConnection` request routing + handler registration |
| `src/mcpl/types.ts` (`McplServerCapabilities` v0.4, local `FeatureSetDeclaration`) | types from `@connectome/mcpl-core` |
| `src/mcpl/feature-sets.ts` — `Record<string, FeatureSetDeclaration>` | **array** `FeatureSetDeclaration[]`, `zulip.messaging` / `zulip.channels` / `zulip.history`, RFC-001 tag ontology |
| `src/mcpl/channels.ts` | keep logic, retype against mcpl-core `ChannelDescriptor` etc.; model on `discord-mcpl/src/channels.ts` |
| `src/mcpl/context.ts` | keep, retype against mcpl-core context-injection types |

Add `state.ts` (rollback checkpoints) if Zulip messaging should declare `rollback: true` for parity — Zulip supports message edit/delete, so this is feasible; flag as optional in review.

**Feature-set shape is the main breaking change:** capabilities go from the fork's `version:'0.4'` Record object to mcpl-core's array form consumed by `McplConnection`. Validate against the host's MCPL handshake (conhost `recipe.ts` / framework `mcpl-first-class`) before merge — this is the highest-risk step.

#### C.3 Acceptance
- `zulip-mcp` boots on mcpl-core, declares `zulip.*` feature sets, completes the MCPL init handshake with conhost.
- Subscribe → post → `mcpl:channel-incoming` wakes the agent (the clerk hot path).
- `listen`/`unlisten`, `channel_publish`, attachment/`fetch_attachment` (from PR #7) all round-trip.
- Self-filter holds (`zulipSelfUserId` numeric guard — see project memory).

## 7. Sequencing & risk

```
A (Discord delete) ──┐
                     ├─► both low-risk, parallel, independent
B (Slack extract) ───┘
C.1 (Zulip slim) ──► C.2 (substrate migrate) ──► C.3 (validate handshake)   ← highest risk
```

- **Land A first** (pure deletion, instant tidiness, de-risks the rest).
- **B and C.2 both consume `mcpl-core-ts`** — doing B first surfaces any gaps in mcpl-core's coverage of the messaging/context/history feature sets *before* touching the load-bearing Zulip server.
- **C.2 is the only step that can break production.** The clerk/miner pipeline depends on the Zulip handshake and the `mcpl:channel-incoming` wake. Gate the merge on a full clerk smoke test (subscribe → DM/post → wake → reply).
- **Recipe coordination:** cook errors if the same source URL has conflicting refs across recipes. Once `zulip-mcp` is in-house Zulip-only, update `clerk.json` + `knowledge-miner.json` together (drop the `Tengro/zulip_mcp` `ref: slack-integration` pin; point at the new canonical Zulip-only repo/build). Add the new `slack` server only where Slack is actually used.
- **Known gotchas to preserve** (from project memory): zulip-js FormData quirks (string booleans, raw arrays in `queues.register`); `all_public_streams:true` ≠ auto-subscribe; capture MCPL-child stderr when debugging the handshake.

## 8. Out of scope / deferred
- Migrating `discord-mcpl` further (already converged).
- Replacing any server with an external community MCP server (rejected in §4).
- Distributed/multi-host MCPL sync.
- Uplifting an external Slack server to push (rejected — build cost ≈ uplift cost, and we already have the adapter).

## 9. End state checklist
- [x] `zulip-mcp` = Zulip only, on `@animalabs/mcpl-core` 0.3.0, no `ENABLE_*`, no `discord.js`/`@slack/*` (Aug 24 2026).
- [x] `discord-mcpl` unchanged (already converged).
- [x] `slack-mcpl` exists, on `@animalabs/mcpl-core`, Socket Mode push, RFC-001 tags (Jul 7 2026; not yet deployed).
- [x] `mcpl-core-ts` is the sole MCPL substrate across all three.
- [ ] Recipes updated (drop the `Tengro/zulip_mcp` `ref: slack-integration` pin); clerk smoke test green; no conflicting source refs.

## 10. What landed beyond the plan (Aug 24 2026)

The Zulip rebuild also adopted discord-mcpl's behaviour families, so the three
servers now share more than a substrate: the closed-channel delivery model
(mentions/DMs as push events, ambient tallied), persisted watermarks with a
reconnect catch-up sweep and event-queue gap recovery, history on
`channels/open`, DM conversations as channels, a hot-reloadable filters plane
(allowlists, mutes, reaction policy), image inlining on ingest, live reaction
visibility with suppression, rollback checkpoints, acknowledge-by-reaction,
chunked sends, and agent-visible timestamps. §3's substrate-divergence table is
historical: upstream had already reimplemented MCPL 0.5 by hand (PR #14) before
the swap; the swap replaced that with the library.
