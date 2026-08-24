import test from 'node:test';
import assert from 'node:assert/strict';
import { formatAgentDateTime, isValidTimeZone, resolveAgentTimeZone, resolveTimestampStyle } from '../src/timezone.ts';

function capturing(fn: () => void): string[] {
  const warnings: string[] = [];
  const original = console.error;
  console.error = (...args: unknown[]) => { warnings.push(args.map(String).join(' ')); };
  try {
    fn();
  } finally {
    console.error = original;
  }
  return warnings;
}

test('formats catch-up times in the configured zone', () => {
  assert.equal(
    formatAgentDateTime(new Date('2026-01-15T12:34:56Z'), 'America/Los_Angeles'),
    '2026-01-15T04:34:56-08:00 [America/Los_Angeles]',
  );
});

test('accepts a valid configured zone without warning', () => {
  const warnings = capturing(() => {
    assert.equal(resolveAgentTimeZone('America/Los_Angeles'), 'America/Los_Angeles');
  });
  assert.equal(warnings.length, 0);
});

test('invalid AGENT_TIMEZONE falls back loudly instead of throwing at import', () => {
  const systemDefault = Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';
  const warnings = capturing(() => {
    // Must not throw — a throw here kills the stdio child before the MCPL
    // handshake, and a dead subprocess is never respawned by reconnect.
    assert.equal(resolveAgentTimeZone('Amrica/New_York'), systemDefault);
  });
  assert.equal(warnings.length, 1);
  assert.match(warnings[0], /\[timezone\] Invalid AGENT_TIMEZONE "Amrica\/New_York"/);
  assert.match(warnings[0], /falling back to system time zone/);
});

test('isValidTimeZone stays exported for hosts that want strictness', () => {
  assert.equal(isValidTimeZone('America/Los_Angeles'), true);
  assert.equal(isValidTimeZone('Not/A_Real_Zone'), false);
});

test('timestamp styles: compact and time render terse zone-converted forms', () => {
  const d = new Date('2026-01-15T12:34:56Z');
  assert.equal(formatAgentDateTime(d, 'America/Los_Angeles', 'compact'), '2026-01-15 04:34');
  assert.equal(formatAgentDateTime(d, 'America/Los_Angeles', 'time'), '04:34');
  assert.equal(formatAgentDateTime(d, 'America/Los_Angeles', 'none'), '');
  assert.equal(formatAgentDateTime(d, 'UTC'), '2026-01-15T12:34:56+00:00 [UTC]');
});

test('resolveTimestampStyle accepts valid styles case-insensitively and falls back on junk', () => {
  assert.equal(resolveTimestampStyle('compact'), 'compact');
  assert.equal(resolveTimestampStyle(' TIME '), 'time');
  assert.equal(resolveTimestampStyle('none'), 'none');
  assert.equal(resolveTimestampStyle(undefined), 'full');
  assert.equal(resolveTimestampStyle(''), 'full');
  const warnings = capturing(() => {
    assert.equal(resolveTimestampStyle('iso8601-nano'), 'full');
  });
  assert.equal(warnings.length, 1);
  assert.ok(warnings[0].includes('AGENT_TIMESTAMP_STYLE'));
});
