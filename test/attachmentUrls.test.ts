/**
 * Regression tests for the attachment-fetch URL validators and the
 * run-as-main guard. These guard credential boundaries: the bot token /
 * realm auth header must never be attachable to a host or path outside
 * the allowlist, no matter what an untrusted message author puts in a URL.
 *
 * Run: node --import tsx --test test/attachmentUrls.test.ts
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, symlinkSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import {
  parseZulipAttachmentUrl,
  isMainModule,
} from '../src/content.ts';

// --- fetch_attachment (Zulip) path allowlist --------------------------------

const REALM = 'https://example.zulipchat.com';

test('parseZulipAttachmentUrl accepts /user_uploads/ paths and realm URLs', () => {
  assert.equal(
    parseZulipAttachmentUrl('/user_uploads/2/ab/cd/shot.png', REALM).pathname,
    '/user_uploads/2/ab/cd/shot.png',
  );
  // Without leading slash, and as a full realm URL.
  assert.ok(parseZulipAttachmentUrl('user_uploads/2/ab/cd/shot.png', REALM));
  assert.ok(parseZulipAttachmentUrl(`${REALM}/user_uploads/2/ab/cd/shot.png`, REALM));
});

test('parseZulipAttachmentUrl rejects dot-segment traversal out of /user_uploads/', () => {
  // Relies on URL normalizing dot segments before pathname is inspected.
  assert.throws(() => parseZulipAttachmentUrl('/user_uploads/../api/v1/users/me', REALM), /user_uploads/);
  assert.throws(() => parseZulipAttachmentUrl('/user_uploads/%2e%2e/api/v1/users/me', REALM), /user_uploads/);
  assert.throws(() => parseZulipAttachmentUrl(`${REALM}/user_uploads/../api/v1/users/me`, REALM), /user_uploads/);
});

test('parseZulipAttachmentUrl rejects foreign hosts and non-upload paths', () => {
  assert.throws(() => parseZulipAttachmentUrl('https://evil.example/user_uploads/x', REALM), /foreign host/);
  // Same host over plain http would send the Basic credentials in clear.
  assert.throws(() => parseZulipAttachmentUrl('http://example.zulipchat.com/user_uploads/x', REALM), /refusing to fetch over http/);
  assert.throws(() => parseZulipAttachmentUrl('/api/v1/users/me', REALM), /user_uploads/);
  assert.throws(() => parseZulipAttachmentUrl('', REALM), /required/);
  assert.throws(() => parseZulipAttachmentUrl('/user_uploads/x', ''), /realm/);
});

// --- run-as-main guard (npm bin symlink regression) -------------------------

test('isMainModule matches when argv[1] is a symlink to the entry module', () => {
  const dir = mkdtempSync(join(tmpdir(), 'mainguard-'));
  try {
    const real = join(dir, 'real.mjs');
    const link = join(dir, 'link.mjs');
    writeFileSync(real, '// entry');
    symlinkSync(real, link);

    const entryUrl = pathToFileURL(real).href;
    // npm bin shim: argv[1] is the symlink, import.meta.url is the realpath.
    assert.equal(isMainModule(entryUrl, link), true);
    assert.equal(isMainModule(entryUrl, real), true);
    // Imported as a library: argv[1] points elsewhere (or nowhere).
    assert.equal(isMainModule(entryUrl, join(dir, 'other.mjs')), false);
    assert.equal(isMainModule(entryUrl, undefined), false);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
