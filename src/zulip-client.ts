/**
 * Zulip client bootstrap — credentials, realm/auth capture for direct HTTP
 * (attachments), bot identity for the self-filter, and startup subscriptions.
 */

import { readFileSync } from 'node:fs';
import zulipInit from 'zulip-js';

export interface ZulipSession {
  /** The zulip-js client. Untyped upstream. */
  client: any;
  /** Numeric bot user id when the profile fetch succeeded; null = no self-filter. */
  selfUserId: number | null;
  /** Realm URL without trailing slash, for direct `/user_uploads/` fetches. */
  realm: string;
  /** `Basic …` header for direct fetches; empty when credentials are unknown. */
  authHeader: string;
  /** Persistent-state id (ZULIP_SESSION_ID, else the bot email, else 'default'). */
  sessionId: string;
}

export async function initializeZulipClient(env: NodeJS.ProcessEnv = process.env): Promise<ZulipSession> {
  const config: any = {
    realm: env.ZULIP_REALM || "",
  };

  if (env.ZULIP_RC_PATH) {
    config.zuliprc = env.ZULIP_RC_PATH;
  } else {
    config.username = env.ZULIP_USERNAME || env.ZULIP_EMAIL;
    config.apiKey = env.ZULIP_API_KEY;
    config.password = env.ZULIP_PASSWORD;
  }

  if (!config.realm && !config.zuliprc) {
    throw new Error(
      "ZULIP_REALM must be set (or provide ZULIP_RC_PATH for zuliprc file)"
    );
  }

  if (!config.zuliprc && !config.username) {
    throw new Error(
      "ZULIP_USERNAME/ZULIP_EMAIL must be set (or provide ZULIP_RC_PATH)"
    );
  }

  if (!config.zuliprc && !config.apiKey && !config.password) {
    throw new Error(
      "Either ZULIP_API_KEY or ZULIP_PASSWORD must be set (or provide ZULIP_RC_PATH)"
    );
  }

  // zulip-js's own typings cover a fraction of the surface this server uses.
  const client: any = await zulipInit(config);

  // Capture realm + auth for direct fetches (zulip-js doesn't expose user_uploads).
  // Preference order: resolved client config, input config, env vars, zuliprc file.
  const resolved = (client && client.config) || config;
  let realm = (resolved.realm || config.realm || "").replace(/\/+$/, "");
  let email = resolved.username || config.username || env.ZULIP_EMAIL || env.ZULIP_USERNAME || "";
  let apiKey = resolved.apiKey || config.apiKey || env.ZULIP_API_KEY || "";

  // Final fallback: parse the zuliprc file directly if any field is still missing.
  if ((!realm || !email || !apiKey) && config.zuliprc) {
    try {
      const raw = readFileSync(config.zuliprc, "utf-8");
      const parsed: Record<string, string> = {};
      for (const line of raw.split(/\r?\n/)) {
        const m = line.match(/^\s*(email|key|site)\s*=\s*(.+?)\s*$/);
        if (m) parsed[m[1]] = m[2];
      }
      if (!realm && parsed.site) realm = parsed.site.replace(/\/+$/, "");
      if (!email && parsed.email) email = parsed.email;
      if (!apiKey && parsed.key) apiKey = parsed.key;
    } catch (err) {
      console.error("Failed to parse zuliprc for direct-HTTP credentials:", err);
    }
  }

  const authHeader = email && apiKey
    ? "Basic " + Buffer.from(`${email}:${apiKey}`).toString("base64")
    : "";

  // Keys the persistent state files. The bot's identity is the natural
  // default whichever way the credentials arrived (env or zuliprc); two
  // sessions of one bot sharing a state dir must set ZULIP_SESSION_ID.
  const sessionId = env.ZULIP_SESSION_ID || email || env.ZULIP_USERNAME || "default";

  // Fail-open: if profile fetch fails, leave selfUserId null (no self-filter).
  let selfUserId: number | null = null;
  try {
    const profile = await client.users.me.getProfile();
    if (profile && typeof profile.user_id === "number") {
      selfUserId = profile.user_id;
      console.error(`Zulip MCP bot user_id: ${selfUserId}`);
    }
  } catch (err) {
    console.error("Failed to fetch bot profile for self-filter:", err);
  }

  // Auto-subscribe to streams named in ZULIP_SUBSCRIBE (comma-separated).
  // Needed because Zulip's event queue only delivers message events for streams
  // the bot is subscribed to, even with all_public_streams: true on the queue.
  if (env.ZULIP_SUBSCRIBE) {
    const streams = env.ZULIP_SUBSCRIBE.split(",").map(s => s.trim()).filter(Boolean);
    if (streams.length > 0) {
      try {
        const result = await client.users.me.subscriptions.add({
          subscriptions: streams.map(name => ({ name })),
        });
        const subscribed = result?.subscribed ?? {};
        const already = result?.already_subscribed ?? {};
        console.error(`Zulip MCP auto-subscribed: new=${JSON.stringify(subscribed)} already=${JSON.stringify(already)}`);
      } catch (err) {
        console.error(`Zulip MCP auto-subscribe failed for [${streams.join(", ")}]:`, err);
      }
    }
  }

  console.error(`Zulip MCP initialized with session: ${sessionId}`);
  return { client, selfUserId, realm, authHeader, sessionId };
}
