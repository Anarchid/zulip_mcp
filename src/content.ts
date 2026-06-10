/**
 * Content helpers — platform message formatting and attachment handling.
 *
 * Pure functions shared by the MCP tool layer (index.ts) and the platform
 * adapters (src/platforms/*). Kept free of client/SDK dependencies so both
 * sides can import without cycles.
 */

// Anthropic refuses images larger than this; mirror their cap server-side
// so an over-eager fetch can't poison the agent's next turn.
export const MAX_ATTACHMENT_BYTES = 5 * 1024 * 1024;

// MIME types we decode and return as `content_text` instead of base64,
// so the agent can read them directly (CSVs, JSON, plain text, etc).
const TEXT_MIME_RE = /^(text\/|application\/(json|xml|x-yaml|yaml|csv|x-www-form-urlencoded)\b)/i;

export interface FetchedAttachment {
  buf: Buffer;
  mimeType: string;
  name: string;
}

/**
 * Authenticated GET with an enforced size cap. Checks Content-Length when the
 * server provides one, and streams with a running byte budget as defense in
 * depth so a missing or lying header can't OOM the process.
 */
export async function fetchAttachmentBytes(
  url: string,
  fallbackName: string,
  opts: { headers?: Record<string, string> } = {},
): Promise<FetchedAttachment> {
  const resp = await fetch(url, { headers: opts.headers ?? {}, redirect: "follow" });
  if (!resp.ok) throw new Error(`fetch failed: ${resp.status} ${resp.statusText}`);

  // Pre-check Content-Length so an oversized declared body never starts buffering.
  const declared = resp.headers.get("content-length");
  if (declared !== null) {
    const n = Number(declared);
    if (Number.isFinite(n) && n > MAX_ATTACHMENT_BYTES) {
      throw new Error(`attachment too large: ${n} bytes (max ${MAX_ATTACHMENT_BYTES})`);
    }
  }

  const reader = resp.body?.getReader();
  let buf: Buffer;
  if (!reader) {
    // No streaming body: fall back to arrayBuffer with a post-read check.
    buf = Buffer.from(await resp.arrayBuffer());
    if (buf.byteLength > MAX_ATTACHMENT_BYTES) {
      throw new Error(`attachment too large: ${buf.byteLength} bytes (max ${MAX_ATTACHMENT_BYTES})`);
    }
  } else {
    const chunks: Buffer[] = [];
    let total = 0;
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      if (!value) continue;
      total += value.byteLength;
      if (total > MAX_ATTACHMENT_BYTES) {
        reader.cancel().catch(() => {});
        throw new Error(`attachment too large: streamed past ${MAX_ATTACHMENT_BYTES} bytes`);
      }
      chunks.push(Buffer.from(value.buffer, value.byteOffset, value.byteLength));
    }
    buf = Buffer.concat(chunks, total);
  }

  const headerMime = resp.headers.get("content-type")?.split(";")[0].trim();
  const classified = classifyExtension(fallbackName);
  const mimeType = headerMime || classified.mimeType;
  return { buf, mimeType, name: fallbackName };
}

/**
 * Shape a successful fetch into the tool's response. Images return as native
 * MCP content blocks (via `_content`), text-ish MIME types return decoded
 * `content_text`, other binary returns `base64`.
 */
export function toFetchResult({ buf, mimeType, name }: FetchedAttachment): Record<string, unknown> {
  if (mimeType.startsWith("image/")) {
    return {
      _content: [
        { type: "text", text: `Fetched ${name} (${mimeType}, ${buf.byteLength} bytes):` },
        { type: "image", data: buf.toString("base64"), mimeType },
      ],
    };
  }
  if (TEXT_MIME_RE.test(mimeType)) {
    return {
      name,
      mimeType,
      size: buf.byteLength,
      content_text: buf.toString("utf-8"),
    };
  }
  return {
    name,
    mimeType,
    size: buf.byteLength,
    base64: buf.toString("base64"),
    note: "Non-image, non-text attachment returned as base64. Decode externally as needed.",
  };
}

export interface AttachmentRef {
  path: string;       // e.g. "/user_uploads/2/Ab/cd/screenshot.png"
  name: string;       // basename for human display
  mimeType: string;   // best-effort from extension
  isImage: boolean;
}

const IMAGE_EXT_MIME: Record<string, string> = {
  png: 'image/png',
  jpg: 'image/jpeg',
  jpeg: 'image/jpeg',
  gif: 'image/gif',
  webp: 'image/webp',
};

const NONIMAGE_EXT_MIME: Record<string, string> = {
  pdf: 'application/pdf',
  txt: 'text/plain',
  md: 'text/markdown',
  json: 'application/json',
  csv: 'text/csv',
};

export function classifyExtension(name: string): { mimeType: string; isImage: boolean } {
  const ext = name.split('.').pop()?.toLowerCase() ?? '';
  if (IMAGE_EXT_MIME[ext]) return { mimeType: IMAGE_EXT_MIME[ext], isImage: true };
  if (NONIMAGE_EXT_MIME[ext]) return { mimeType: NONIMAGE_EXT_MIME[ext], isImage: false };
  return { mimeType: 'application/octet-stream', isImage: false };
}

// Zulip uploads appear in markdown as `[name](/user_uploads/X/Yy/Zz/name.ext)`
// or inline-image syntax `![name](/user_uploads/...)`. We pull paths out so the
// agent can request the bytes on demand via fetch_attachment.
export function extractZulipAttachments(rawContent: string): AttachmentRef[] {
  const refs: AttachmentRef[] = [];
  const seen = new Set<string>();
  const re = /\/user_uploads\/[^\s)>\]"']+/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(rawContent)) !== null) {
    const path = m[0];
    if (seen.has(path)) continue;
    seen.add(path);
    const name = decodeURIComponent(path.split('/').pop() ?? 'attachment');
    const { mimeType, isImage } = classifyExtension(name);
    refs.push({ path, name, mimeType, isImage });
  }
  return refs;
}

// Helper function to strip HTML and format content with mention handling.
// Used on paths where Zulip returns rendered HTML (get_channel_history,
// context/beforeInference message history). The push-event path receives raw
// markdown (apply_markdown:false) where /user_uploads/ paths are already
// textual, so attachment refs there are extracted by extractZulipAttachments.
export function cleanContent(html: string): string {
  let content = html;

  // Extract Zulip mentions first
  content = content.replace(
    /<span class="user-mention"[^>]*data-user-id="(\d+)"[^>]*>@([^<]+)<\/span>/g,
    '@$2 (uid:$1)'
  );

  // Handle silent mentions
  content = content.replace(
    /<span class="user-mention silent"[^>]*data-user-id="(\d+)"[^>]*>([^<]+)<\/span>/g,
    '$2 (uid:$1)'
  );

  // Preserve attachment / inline-image URLs before the generic tag strip below
  // eats them. Zulip renders uploads as `<a href="/user_uploads/...">name</a>`
  // and inline images as the same anchor wrapping an `<img>`. Without this,
  // history fetches lose the URL and the agent can't call fetch_attachment.
  content = content.replace(
    /<a [^>]*href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/g,
    (_match, href: string, inner: string) => {
      const hasImg = /<img\b/i.test(inner);
      const text = inner.replace(/<[^>]*>/g, '').trim();
      if (href.startsWith('/user_uploads/')) {
        if (hasImg) return `[image: ${href}]`;
        return text ? `[attachment: ${text} — ${href}]` : `[attachment: ${href}]`;
      }
      // External anchors with an inline image preview: keep `[image: ...]` so
      // the agent can choose to fetch. Plain external links keep the prior
      // text-only behaviour (no scope creep on non-attachment links).
      if (hasImg) return `[image: ${href}]`;
      return text || href;
    }
  );

  // Bare `<img>` tags (rare in Zulip, but possible via external image previews).
  content = content.replace(
    /<img [^>]*src="([^"]+)"[^>]*>/g,
    '[image: $1]'
  );

  // Clean up HTML
  content = content
    .replace(/<p>/gi, '\n')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<[^>]*>/g, '')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .trim();

  return content;
}

// Helper to format Discord mentions
export function formatDiscordContent(content: string, mentions: any): string {
  let formatted = content;

  // Replace user mentions with readable format
  if (mentions && mentions.users) {
    for (const [userId, user] of mentions.users) {
      formatted = formatted.replace(
        new RegExp(`<@${userId}>`, 'g'),
        `@${user.username} (uid:${userId})`
      );
      formatted = formatted.replace(
        new RegExp(`<@!${userId}>`, 'g'),
        `@${user.username} (uid:${userId})`
      );
    }
  }

  // Replace channel mentions
  if (mentions && mentions.channels) {
    for (const [channelId, channel] of mentions.channels) {
      formatted = formatted.replace(
        new RegExp(`<#${channelId}>`, 'g'),
        `#${channel.name}`
      );
    }
  }

  // Replace role mentions
  if (mentions && mentions.roles) {
    for (const [roleId, role] of mentions.roles) {
      formatted = formatted.replace(
        new RegExp(`<@&${roleId}>`, 'g'),
        `@${role.name} (role)`
      );
    }
  }

  return formatted;
}
