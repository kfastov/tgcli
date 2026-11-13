// `tgcli store compact` must have the store to itself: it rewrites every row and
// VACUUMs, which holds SQLite's write lock for minutes.

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const store = vi.hoisted(() => ({ dir: '/tmp/tgcli-cli-store-compact-unset' }));
const control = vi.hoisted(() => ({
  pingServer: vi.fn(async () => null),
  syncServerVersion: vi.fn(async () => 'none'),
}));

vi.mock('../core/store.js', async (importOriginal) => ({
  ...(await importOriginal()),
  resolveStoreDir: vi.fn(() => store.dir),
}));

vi.mock('../core/control-client.js', async (importOriginal) => ({
  ...(await importOriginal()),
  ...control,
}));

const { buildProgram } = await import('../cli.js');
const { saveConfig } = await import('../core/config.js');

let stderrSpy;
let logSpy;

async function run(argv) {
  const program = buildProgram();
  program.exitOverride();
  await program.parseAsync(['node', 'cli.js', ...argv]);
}

function stderr() {
  return stderrSpy.mock.calls.map((call) => String(call[0])).join('');
}

beforeEach(() => {
  store.dir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-cli-store-compact-'));
  saveConfig(store.dir, { apiId: 1, apiHash: 'hash', phoneNumber: '+10000000000' });
  stderrSpy = vi.spyOn(process.stderr, 'write').mockImplementation(() => true);
  logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
  vi.spyOn(console, 'error').mockImplementation(() => {});
  control.pingServer.mockReset().mockResolvedValue(null);
  control.syncServerVersion.mockClear();
});

afterEach(() => {
  fs.rmSync(store.dir, { recursive: true, force: true });
  vi.restoreAllMocks();
  process.exitCode = 0;
});

describe('tgcli store compact', () => {
  it('refuses while a server is running on the store, and says how to stop it', async () => {
    control.pingServer.mockResolvedValue({ ok: true, pid: 4242, version: '2.3.0' });

    await run(['store', 'compact']);

    expect(process.exitCode).toBe(1);
    expect(stderr()).toMatch(/server .*pid 4242.* is running/);
    expect(stderr()).toContain('tgcli service stop');
    expect(stderr()).toContain('tgcli server stop');
    expect(fs.existsSync(path.join(store.dir, 'messages.db'))).toBe(false);
  });

  it('does not start or replace a server before compacting', async () => {
    await run(['store', 'compact']);

    expect(control.syncServerVersion).not.toHaveBeenCalled();
    expect(process.exitCode).not.toBe(1);
    expect(logSpy.mock.calls.flat().join(' ')).toContain('Rows scanned: 0');
  });

  it('holds the store lock while it runs', async () => {
    fs.writeFileSync(path.join(store.dir, 'LOCK'), JSON.stringify({ pid: process.ppid, startedAt: 's' }));

    await run(['store', 'compact']);

    expect(process.exitCode).toBe(1);
    expect(stderr()).toMatch(/locked/i);
  });
});
