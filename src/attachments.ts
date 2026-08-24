/**
 * Attachment inlining on live delivery — images downsampled to model-max,
 * small text files as text — so the agent sees what was shared instead of
 * a path it would have to fetch. Everything else stays a reference the
 * agent can pull with fetch_attachment.
 *
 * Applies to live delivery only (channels/incoming and push/event). History
 * on channels/open, the reconnect sweep, and the fetch tools keep the
 * reference form: replaying a backlog is not the moment to pull every
 * image that was ever posted.
 */

import sharp from 'sharp';
import type { ContentBlock, TextContent } from '@animalabs/mcpl-core';
import type { AttachmentRef } from './content.js';

// ── Image normalization (downsample-on-ingest) ──

/** Longest edge (px) kept for inlined images. Matches the ~1568px ceiling
 *  every major vision model downscales to server-side, so resizing to this is
 *  perceptually lossless — the model discards anything finer regardless. */
export const IMAGE_LONG_EDGE_MAX = 1568;
const IMAGE_JPEG_QUALITY = 85;
/** Cap on the *encoded* bytes inlined (raw, pre-base64). Anthropic accepts
 *  ~5MB/image of base64; staying under ~3.5MB raw keeps us inside. */
export const IMAGE_OUTPUT_RAW_CAP = 3.5 * 1024 * 1024;
/** Refuse to even download sources larger than this (OOM guard). sharp's own
 *  pixel limit guards the decoded bitmap against decompression bombs. */
export const IMAGE_FETCH_CEILING = 25 * 1024 * 1024;
/** Absolute ceiling on inlined text-attachment bytes: however high the knob
 *  is set, a text attachment can never put more than this into context. */
export const MAX_TEXT_BYTES = 256 * 1024;
export const DEFAULT_ATTACHMENT_INLINE_MAX_BYTES = 5120;
export const DEFAULT_MAX_INLINE_IMAGES = 4;

/** The pass-through fast paths may ONLY emit formats the model API accepts.
 *  sharp happily reads svg/tiff/avif/heif too — an SVG small enough to skip
 *  re-encoding would otherwise sail through as `image/svg` and poison the
 *  agent's history with a permanently-rejected block. Non-API formats fall
 *  through to the re-encode pipeline, which rasterizes them to PNG/JPEG. */
const API_SAFE_FORMATS = new Set(['jpeg', 'png', 'gif', 'webp']);

/** Decoded-bitmap ceiling handed to sharp (default is 268 Mpx, ~1 GiB RGBA).
 *  Message attachments over 40 Mpx are not something a model will see at
 *  1568 px anyway; refusing them bounds memory per decode. */
const IMAGE_MAX_INPUT_PIXELS = 40_000_000;
const sharpOpts = { limitInputPixels: IMAGE_MAX_INPUT_PIXELS };

/** At most this many image decodes run at once across all messages. */
const IMAGE_DECODE_CONCURRENCY = 2;
let decodesInFlight = 0;
const decodeWaiters: (() => void)[] = [];
async function withDecodeSlot<T>(fn: () => Promise<T>): Promise<T> {
  if (decodesInFlight >= IMAGE_DECODE_CONCURRENCY) {
    await new Promise<void>((resolve) => decodeWaiters.push(resolve));
  }
  decodesInFlight++;
  try {
    return await fn();
  } finally {
    decodesInFlight--;
    decodeWaiters.shift()?.();
  }
}

export interface NormalizedImage {
  data: string; // base64
  mimeType: string;
}

/** Downsample an image to model-max on ingest: resize so the longest edge is
 *  <= IMAGE_LONG_EDGE_MAX (never upscales), re-encoding to stay under the inline
 *  byte cap. Opaque images become JPEG; images with alpha stay PNG (flattened to
 *  JPEG only as a last resort to fit the cap). Already-small images pass through
 *  untouched. Animated GIFs are left as-is (frame resizing is out of scope) and
 *  inlined only when already under cap. Returns null when nothing inlinable can
 *  be produced, letting the caller degrade to a text note. */
export function normalizeImageForInference(buf: Buffer): Promise<NormalizedImage | null> {
  return withDecodeSlot(() => normalizeImageUnbounded(buf));
}

async function normalizeImageUnbounded(buf: Buffer): Promise<NormalizedImage | null> {
  try {
    const meta = await sharp(buf, { ...sharpOpts, animated: true }).metadata();
    const longest = Math.max(meta.width ?? 0, meta.height ?? 0);
    const isAnimated = (meta.pages ?? 1) > 1;
    const apiSafe = API_SAFE_FORMATS.has(meta.format ?? '');

    // Animated: don't resize frames here. Inline as-is if small enough.
    if (isAnimated) {
      return apiSafe && buf.length <= IMAGE_OUTPUT_RAW_CAP
        ? { data: buf.toString('base64'), mimeType: `image/${meta.format}` }
        : null;
    }

    // Already within bounds and under cap → inline original bytes unchanged.
    if (apiSafe && longest > 0 && longest <= IMAGE_LONG_EDGE_MAX && buf.length <= IMAGE_OUTPUT_RAW_CAP) {
      return { data: buf.toString('base64'), mimeType: `image/${meta.format}` };
    }

    // Fresh pipeline per encode (sharp instances aren't safely reusable across
    // multiple toBuffer() calls). resize() with withoutEnlargement is a no-op
    // when the image is already within bounds but over the byte cap.
    const resizeOpts = { width: IMAGE_LONG_EDGE_MAX, height: IMAGE_LONG_EDGE_MAX, fit: 'inside' as const, withoutEnlargement: true };
    const base = () => sharp(buf, sharpOpts).resize(resizeOpts);

    let out: Buffer;
    let mimeType: string;
    if (meta.hasAlpha) {
      out = await base().png({ compressionLevel: 9 }).toBuffer();
      mimeType = 'image/png';
    } else {
      out = await base().jpeg({ quality: IMAGE_JPEG_QUALITY }).toBuffer();
      mimeType = 'image/jpeg';
    }

    // Still over cap (large PNG / high-detail photo) → flatten + shrink harder.
    if (out.length > IMAGE_OUTPUT_RAW_CAP) {
      out = await sharp(buf, sharpOpts)
        .resize({ width: 1024, height: 1024, fit: 'inside', withoutEnlargement: true })
        .flatten({ background: '#ffffff' })
        .jpeg({ quality: 70 })
        .toBuffer();
      mimeType = 'image/jpeg';
      if (out.length > IMAGE_OUTPUT_RAW_CAP) return null;
    }

    return { data: out.toString('base64'), mimeType };
  } catch {
    return null;
  }
}

// ── Fetching ──

export interface FetchedBytes {
  buf: Buffer;
  mimeType: string | null;
  /** The body exceeded `maxBytes`; `buf` is then partial and unused. */
  overflow: boolean;
}

/** GET with an enforced byte ceiling: the read is cancelled the moment the
 *  body passes `maxBytes`, so a lying or absent Content-Length cannot OOM
 *  the process. */
export async function fetchCapped(
  url: string,
  headers: Record<string, string>,
  maxBytes: number,
  timeoutMs = 15_000,
): Promise<FetchedBytes> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, { headers, redirect: 'follow', signal: ctrl.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const mimeType = res.headers.get('content-type')?.split(';')[0].trim() ?? null;
    const declared = Number(res.headers.get('content-length') ?? '');
    if (Number.isFinite(declared) && declared > maxBytes) {
      return { buf: Buffer.alloc(0), mimeType, overflow: true };
    }
    if (!res.body) return { buf: Buffer.alloc(0), mimeType, overflow: true };
    const reader = res.body.getReader();
    const chunks: Uint8Array[] = [];
    let total = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.length;
      if (total > maxBytes) {
        void reader.cancel().catch(() => {});
        return { buf: Buffer.alloc(0), mimeType, overflow: true };
      }
      chunks.push(value);
    }
    return { buf: Buffer.concat(chunks), mimeType, overflow: false };
  } finally {
    clearTimeout(timer);
  }
}

// ── Blocks ──

const TEXT_EXT =
  /\.(txt|md|markdown|json|jsonl|csv|tsv|log|ya?ml|xml|html?|css|js|mjs|cjs|ts|tsx|jsx|py|rb|go|rs|java|kt|c|h|cpp|hpp|sh|bash|zsh|toml|ini|cfg|conf|sql|diff|patch|env)$/i;

export function isTextAttachment(ref: AttachmentRef): boolean {
  return ref.mimeType.startsWith('text/') || TEXT_EXT.test(ref.name);
}

export interface AttachmentSource {
  /** Fetch an upload by its `/user_uploads/...` path with the bot's credentials. */
  fetch(path: string, maxBytes: number): Promise<FetchedBytes>;
}

export interface InlineOptions {
  inlineImages: boolean;
  /** Text attachments at or under this many bytes are inlined (clamped to MAX_TEXT_BYTES). */
  inlineTextMaxBytes: number;
  /** Images inlined per message; the rest stay references. */
  maxImages: number;
}

export function resolveInlineOptions(env: NodeJS.ProcessEnv = process.env): InlineOptions {
  const knob = Number(env.ZULIP_ATTACHMENT_INLINE_MAX_BYTES ?? '');
  const maxImages = Number(env.ZULIP_INLINE_IMAGES_MAX ?? '');
  return {
    inlineImages: env.ZULIP_INLINE_IMAGES !== 'false',
    inlineTextMaxBytes: Math.min(
      MAX_TEXT_BYTES,
      Number.isFinite(knob) && knob >= 0 && env.ZULIP_ATTACHMENT_INLINE_MAX_BYTES !== undefined ? Math.floor(knob) : DEFAULT_ATTACHMENT_INLINE_MAX_BYTES,
    ),
    maxImages: Number.isFinite(maxImages) && maxImages >= 0 && env.ZULIP_INLINE_IMAGES_MAX !== undefined ? Math.floor(maxImages) : DEFAULT_MAX_INLINE_IMAGES,
  };
}

function fmt(n: number): string {
  return n >= 1048576 ? `${(n / 1048576).toFixed(1)}MB` : n >= 1024 ? `${Math.round(n / 1024)}KB` : `${n}B`;
}

function text(t: string): TextContent {
  return { type: 'text', text: t };
}

/**
 * Content blocks for a message's attachments: inlined images (after
 * normalization) and small text files, notes for the rest. Never throws —
 * a failed fetch becomes a note naming the path so the agent can still
 * fetch_attachment it.
 */
export async function buildAttachmentBlocks(
  refs: AttachmentRef[],
  source: AttachmentSource,
  options: InlineOptions,
): Promise<ContentBlock[]> {
  const blocks: ContentBlock[] = [];
  let imagesLeft = options.inlineImages ? options.maxImages : 0;
  for (const ref of refs) {
    try {
      if (ref.isImage) {
        if (imagesLeft <= 0) {
          blocks.push(text(`[image attachment "${ref.name}" not inlined (${options.inlineImages ? 'per-message image limit reached' : 'image inlining is off'}) — fetch_attachment ${ref.path}]`));
          continue;
        }
        const got = await source.fetch(ref.path, IMAGE_FETCH_CEILING);
        if (got.overflow) {
          blocks.push(text(`[image attachment "${ref.name}" too large to fetch (over ${fmt(IMAGE_FETCH_CEILING)}) — ${ref.path}]`));
          continue;
        }
        const norm = await normalizeImageForInference(got.buf);
        if (!norm) {
          blocks.push(text(`[image attachment "${ref.name}" (${fmt(got.buf.length)}) could not be inlined — fetch_attachment ${ref.path}]`));
          continue;
        }
        imagesLeft--;
        blocks.push({ type: 'image', data: norm.data, mimeType: norm.mimeType });
        blocks.push(text(`[image attachment: ${ref.name}]`));
      } else if (isTextAttachment(ref) && options.inlineTextMaxBytes > 0) {
        const got = await source.fetch(ref.path, options.inlineTextMaxBytes);
        if (got.overflow) {
          blocks.push(text(`[attachment: ${ref.name} over the ${fmt(options.inlineTextMaxBytes)} inline cap — not inlined: fetch_attachment ${ref.path}]`));
          continue;
        }
        blocks.push(text(`[attachment: ${ref.name} (${fmt(got.buf.length)})]\n${got.buf.toString('utf8')}`));
      }
      // Other binaries stay as the reference note already on the message.
    } catch (err) {
      blocks.push(text(`[attachment: ${ref.name} — could not fetch (${(err as Error).message}); fetch_attachment ${ref.path}]`));
    }
  }
  return blocks;
}
