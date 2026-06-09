/**
 * Tests for cleanContent + extractZulipAttachments — the two regex-heavy
 * parsers most likely to silently regress on a future Zulip render-format
 * change.
 *
 * Run: node --import tsx --test test/cleanContent.test.ts
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { cleanContent, extractZulipAttachments } from '../src/index.ts';

test('cleanContent: attachment anchor preserves filename + URL', () => {
  const html = '<p><a href="/user_uploads/2/foo.csv">foo.csv</a></p>';
  assert.equal(cleanContent(html), '[attachment: foo.csv — /user_uploads/2/foo.csv]');
});

test('cleanContent: inline image (anchor wrapping img) preserves URL as [image: ...]', () => {
  const html = '<div class="message_inline_image">'
    + '<a href="/user_uploads/2/img.png" title="img.png">'
    + '<img src="/user_uploads/thumb/img.png">'
    + '</a></div>';
  assert.equal(cleanContent(html), '[image: /user_uploads/2/img.png]');
});

test('cleanContent: plain external link keeps prior text-only behaviour (no scope creep)', () => {
  const html = '<p>see <a href="https://example.com">docs</a> for more</p>';
  assert.equal(cleanContent(html), 'see docs for more');
});

test('cleanContent: external anchor with image preview keeps URL as [image: ...]', () => {
  const html = '<div class="message_inline_image">'
    + '<a href="https://example.com/external.png">'
    + '<img src="https://uploads.zulipusercontent.net/external/abc">'
    + '</a></div>';
  assert.equal(cleanContent(html), '[image: https://example.com/external.png]');
});

test('cleanContent: bare <img> tag becomes [image: src]', () => {
  const html = '<p>look: <img src="https://example.com/x.png"></p>';
  assert.equal(cleanContent(html), 'look: [image: https://example.com/x.png]');
});

test('cleanContent: mention + attachment in same message', () => {
  const html = '<p><span class="user-mention" data-user-id="42">@Alice</span>'
    + ' see <a href="/user_uploads/x/data.json">data.json</a></p>';
  assert.equal(
    cleanContent(html),
    '@Alice (uid:42) see [attachment: data.json — /user_uploads/x/data.json]',
  );
});

test('cleanContent: HTML entity decoding still works', () => {
  assert.equal(cleanContent('<p>2 &amp; 3 &lt; 5</p>'), '2 & 3 < 5');
});

test('cleanContent: attachment anchor without filename label still keeps URL', () => {
  const html = '<a href="/user_uploads/2/foo.csv"></a>';
  assert.equal(cleanContent(html), '[attachment: /user_uploads/2/foo.csv]');
});

test('extractZulipAttachments: dedupes repeat refs to the same path', () => {
  const raw = '[a.png](/user_uploads/1/a.png) and again /user_uploads/1/a.png';
  const refs = extractZulipAttachments(raw);
  assert.equal(refs.length, 1);
  assert.equal(refs[0].path, '/user_uploads/1/a.png');
  assert.equal(refs[0].name, 'a.png');
  assert.equal(refs[0].isImage, true);
  assert.equal(refs[0].mimeType, 'image/png');
});

test('extractZulipAttachments: classifies images vs non-image binaries', () => {
  const raw = '[shot.png](/user_uploads/1/shot.png) [data.csv](/user_uploads/2/data.csv) [doc.pdf](/user_uploads/3/doc.pdf)';
  const refs = extractZulipAttachments(raw);
  assert.equal(refs.length, 3);
  assert.deepEqual(refs.map(r => [r.name, r.mimeType, r.isImage]), [
    ['shot.png', 'image/png', true],
    ['data.csv', 'text/csv', false],
    ['doc.pdf', 'application/pdf', false],
  ]);
});

test('extractZulipAttachments: returns empty list when no refs present', () => {
  assert.deepEqual(extractZulipAttachments('just a plain message'), []);
});

test('extractZulipAttachments: trailing punctuation in markdown links is not consumed', () => {
  // `[name](/user_uploads/.../name.ext)` — the closing paren must not be in the path.
  const raw = '[a.png](/user_uploads/1/a.png)';
  const refs = extractZulipAttachments(raw);
  assert.equal(refs.length, 1);
  assert.equal(refs[0].path, '/user_uploads/1/a.png');
});
