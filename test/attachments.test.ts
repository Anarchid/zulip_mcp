/**
 * Attachment inlining — image normalization against real encoders, the
 * capped fetch against a local HTTP server, and block building.
 *
 * Run: node --import tsx --test test/attachments.test.ts
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, type Server } from 'node:http';
import { once } from 'node:events';
import sharp from 'sharp';
import {
  IMAGE_LONG_EDGE_MAX,
  MAX_TEXT_BYTES,
  buildAttachmentBlocks,
  fetchCapped,
  normalizeImageForInference,
  resolveInlineOptions,
  type AttachmentSource,
} from '../src/attachments.ts';
import type { AttachmentRef } from '../src/content.ts';

async function png(width: number, height: number, alpha = false): Promise<Buffer> {
  return sharp({ create: { width, height, channels: alpha ? 4 : 3, background: alpha ? { r: 200, g: 20, b: 20, alpha: 0.5 } : '#3366cc' } })
    .png()
    .toBuffer();
}

test('small API-safe images pass through unchanged', async () => {
  const src = await png(200, 100);
  const norm = await normalizeImageForInference(src);
  assert.ok(norm);
  assert.equal(norm.mimeType, 'image/png');
  assert.equal(Buffer.from(norm.data, 'base64').equals(src), true);
});

test('oversized images are downsampled to the model ceiling; opaque → jpeg, alpha → png', async () => {
  const opaque = await normalizeImageForInference(await png(4000, 1000));
  assert.ok(opaque);
  assert.equal(opaque.mimeType, 'image/jpeg');
  const meta = await sharp(Buffer.from(opaque.data, 'base64')).metadata();
  assert.equal(meta.width, IMAGE_LONG_EDGE_MAX);
  assert.equal(meta.height, Math.round((1000 * IMAGE_LONG_EDGE_MAX) / 4000));

  const withAlpha = await normalizeImageForInference(await png(2000, 2000, true));
  assert.ok(withAlpha);
  assert.equal(withAlpha.mimeType, 'image/png');
  assert.equal((await sharp(Buffer.from(withAlpha.data, 'base64')).metadata()).hasAlpha, true);
});

test('non-API formats are rasterized rather than emitted as-is, and garbage yields null', async () => {
  const svg = Buffer.from('<svg xmlns="http://www.w3.org/2000/svg" width="40" height="20"><rect width="40" height="20" fill="red"/></svg>');
  const norm = await normalizeImageForInference(svg);
  assert.ok(norm);
  assert.notEqual(norm.mimeType, 'image/svg');
  assert.ok(['image/jpeg', 'image/png'].includes(norm.mimeType));
  assert.equal(await normalizeImageForInference(Buffer.from('not an image')), null);
});

test('resolveInlineOptions clamps the text cap and reads the knobs', () => {
  assert.deepEqual(resolveInlineOptions({}), { inlineImages: true, inlineTextMaxBytes: 5120, maxImages: 4 });
  const opts = resolveInlineOptions({ ZULIP_INLINE_IMAGES: 'false', ZULIP_ATTACHMENT_INLINE_MAX_BYTES: '99999999', ZULIP_INLINE_IMAGES_MAX: '1' });
  assert.equal(opts.inlineImages, false);
  assert.equal(opts.inlineTextMaxBytes, MAX_TEXT_BYTES);
  assert.equal(opts.maxImages, 1);
  assert.equal(resolveInlineOptions({ ZULIP_ATTACHMENT_INLINE_MAX_BYTES: 'junk' }).inlineTextMaxBytes, 5120);
});

/** A local upload host that checks the Authorization header. */
async function uploadHost(files: Record<string, { body: Buffer; type: string; chunked?: boolean }>): Promise<{ server: Server; origin: string; hits: string[] }> {
  const hits: string[] = [];
  const server = createServer((req, res) => {
    hits.push(`${req.headers.authorization ?? '-'} ${req.url}`);
    const f = files[req.url ?? ''];
    if (!f || req.headers.authorization !== 'Basic dGVzdA==') {
      res.writeHead(f ? 401 : 404).end();
      return;
    }
    if (f.chunked) {
      // No Content-Length at all: the wire has to decide.
      res.writeHead(200, { 'content-type': f.type, 'transfer-encoding': 'chunked' });
      for (let i = 0; i < f.body.length; i += 4096) res.write(f.body.subarray(i, i + 4096));
      res.end();
      return;
    }
    res.writeHead(200, { 'content-type': f.type });
    res.end(f.body);
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address() as { port: number };
  return { server, origin: `http://127.0.0.1:${address.port}`, hits };
}

test('fetchCapped sends the credentials, honours the ceiling, and reports overflow without buffering', async () => {
  const big = Buffer.alloc(50_000, 0x41);
  const { server, origin, hits } = await uploadHost({
    '/user_uploads/1/a/small.txt': { body: Buffer.from('hello'), type: 'text/plain' },
    '/user_uploads/1/a/big.txt': { body: big, type: 'text/plain' },
    '/user_uploads/1/a/nolength.txt': { body: big, type: 'text/plain', chunked: true },
  });
  try {
    const headers = { Authorization: 'Basic dGVzdA==' };
    const small = await fetchCapped(`${origin}/user_uploads/1/a/small.txt`, headers, 100);
    assert.equal(small.overflow, false);
    assert.equal(small.buf.toString(), 'hello');
    assert.equal(small.mimeType, 'text/plain');

    const over = await fetchCapped(`${origin}/user_uploads/1/a/big.txt`, headers, 1000);
    assert.equal(over.overflow, true);
    assert.equal(over.buf.length, 0);

    // No Content-Length: the streamed read stops at the cap.
    const nolength = await fetchCapped(`${origin}/user_uploads/1/a/nolength.txt`, headers, 1000);
    assert.equal(nolength.overflow, true);
    assert.equal(nolength.buf.length, 0);

    await assert.rejects(fetchCapped(`${origin}/user_uploads/1/a/small.txt`, {}, 100), /HTTP 401/);
    await assert.rejects(fetchCapped(`${origin}/nope`, headers, 100), /HTTP 404/);
    assert.ok(hits.every((h) => h.startsWith('Basic dGVzdA== ') || h.startsWith('- ')));
  } finally {
    server.close();
  }
});

function ref(path: string, mimeType: string, isImage: boolean): AttachmentRef {
  return { path, name: path.split('/').pop()!, mimeType, isImage };
}

test('buildAttachmentBlocks inlines images and small text, and degrades everything else to notes', async () => {
  const image = await png(3000, 1500);
  const { server, origin } = await uploadHost({
    '/user_uploads/1/a/shot.png': { body: image, type: 'image/png' },
    '/user_uploads/1/a/notes.md': { body: Buffer.from('# notes\nhi'), type: 'text/markdown' },
    '/user_uploads/1/a/huge.log': { body: Buffer.alloc(20_000, 0x42), type: 'text/plain' },
    '/user_uploads/1/a/second.png': { body: image, type: 'image/png' },
  });
  const source: AttachmentSource = {
    fetch: (path, maxBytes) => fetchCapped(`${origin}${path}`, { Authorization: 'Basic dGVzdA==' }, maxBytes),
  };
  try {
    const refs = [
      ref('/user_uploads/1/a/missing.png', 'image/png', true),
      ref('/user_uploads/1/a/shot.png', 'image/png', true),
      ref('/user_uploads/1/a/notes.md', 'text/markdown', false),
      ref('/user_uploads/1/a/huge.log', 'text/plain', false),
      ref('/user_uploads/1/a/deck.pdf', 'application/pdf', false),
      ref('/user_uploads/1/a/second.png', 'image/png', true),
    ];
    const blocks = await buildAttachmentBlocks(refs, source, { inlineImages: true, inlineTextMaxBytes: 5120, maxImages: 1 });
    // A failed fetch does not spend the image budget.
    assert.match((blocks[0] as { text: string }).text, /missing\.png — could not fetch \(HTTP 404\)/);
    assert.equal(blocks[1].type, 'image');
    assert.equal((blocks[1] as { mimeType: string }).mimeType, 'image/jpeg');
    assert.equal((blocks[2] as { text: string }).text, '[image attachment: shot.png]');
    assert.equal((blocks[3] as { text: string }).text, '[attachment: notes.md (10B)]\n# notes\nhi');
    assert.match((blocks[4] as { text: string }).text, /^\[attachment: huge\.log over the 5KB inline cap — not inlined: fetch_attachment \/user_uploads\/1\/a\/huge\.log\]$/);
    // The pdf produces no block: its reference note is already on the message.
    assert.match((blocks[5] as { text: string }).text, /second\.png" not inlined \(per-message image limit reached\)/);
    assert.equal(blocks.length, 6);

    const off = await buildAttachmentBlocks(refs.slice(0, 1), source, { inlineImages: false, inlineTextMaxBytes: 0, maxImages: 4 });
    assert.match((off[0] as { text: string }).text, /image inlining is off/);
  } finally {
    server.close();
  }
});
