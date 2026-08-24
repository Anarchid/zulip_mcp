import test from 'node:test';
import assert from 'node:assert/strict';
import { StateTracker } from '../src/state.ts';
import { chunkMessage, ZULIP_MAX_MESSAGE_LENGTH } from '../src/content.ts';

test('rollback hands back what was sent after the checkpoint and truncates the record', () => {
  const t = new StateTracker();
  assert.equal(t.current, null);
  assert.equal(t.getCheckpointState(), null);
  t.recordSent('1', 'zulip:general', 'a');
  const c1 = t.createCheckpoint();
  assert.deepEqual(t.getCheckpointState(), { checkpoint: c1, parent: null });
  t.recordSent('2', 'zulip:general', 'b');
  t.recordSent('3', 'zulip:dm:42', 'c');
  const c2 = t.createCheckpoint();
  assert.deepEqual(t.getCheckpointState(), { checkpoint: c2, parent: c1 });
  t.recordSent('4', 'zulip:general', 'd');

  assert.equal(t.rollback('chk_nope'), null);
  assert.deepEqual(t.rollback(c1)!.map((m) => m.messageId), ['2', '3', '4']);
  assert.equal(t.current, c1);
  // A second rollback to the same checkpoint has nothing left to undo.
  assert.deepEqual(t.rollback(c1), []);
  // Old checkpoints survive: c2 is still addressable (now empty).
  assert.deepEqual(t.rollback(c2), []);
});

test('chunkMessage splits long text at paragraph, then line, then hard boundaries', () => {
  assert.deepEqual(chunkMessage('short', 100), ['short']);
  assert.deepEqual(chunkMessage('', 100), []);
  const paras = ['a'.repeat(60), 'b'.repeat(60), 'c'.repeat(60)].join('\n\n');
  assert.deepEqual(chunkMessage(paras, 130).map((c) => c.length), [122, 60]);
  const lines = ['x'.repeat(50), 'y'.repeat(50), 'z'.repeat(50)].join('\n');
  assert.deepEqual(chunkMessage(lines, 110).map((c) => c.length), [101, 50]);
  const wall = 'w'.repeat(250);
  assert.deepEqual(chunkMessage(wall, 100).map((c) => c.length), [100, 100, 50]);
  // Every chunk respects the limit and the concatenation loses no characters.
  const mixed = `${'p'.repeat(9000)}\n\n${'q'.repeat(9000)}\n${'r'.repeat(3000)}`;
  const chunks = chunkMessage(mixed);
  assert.ok(chunks.every((c) => c.length <= ZULIP_MAX_MESSAGE_LENGTH));
  assert.equal(chunks.join('').replace(/\s/g, '').length, mixed.replace(/\s/g, '').length);
});
