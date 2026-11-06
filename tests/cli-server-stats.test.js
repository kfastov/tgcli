// `tgcli server stats` sits under `tgcli server`, which has its own action: the CLI
// spawns `server --idle-exit 60s --auto-started`, and that must keep starting a
// server while `server stats` reads counters and a typo starts nothing.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const control = vi.hoisted(() => ({ fetchServerStats: null }));
// cli.js resolves the store once on import; each test points it at an empty one.
const store = vi.hoisted(() => ({ dir: `${process.env.TMPDIR ?? '/tmp'}/tgcli-cli-server-stats-import` }));

vi.mock('../core/control-client.js', () => ({
  ensureServer: vi.fn(),
  invoke: vi.fn(),
  pingServer: vi.fn(),
  enqueueBackfill: vi.fn(),
  enqueueRepair: vi.fn(),
  cancelBackfill: vi.fn(),
  retryBackfill: vi.fn(),
  fetchServerStats: (...args) => control.fetchServerStats(...args),
  ServerUnavailableError: class ServerUnavailableError extends Error {},
}));

vi.mock('../core/store.js', async (importOriginal) => ({
  ...await importOriginal(),
  resolveStoreDir: vi.fn(() => store.dir),
}));

const { buildProgram } = await import('../cli.js');
const { ServerStats } = await import('../core/server-stats.js');

async function runProgram(argv) {
  const program = buildProgram();
  program.exitOverride();
  program.configureOutput({ writeOut: () => {}, writeErr: () => {} });
  for (const command of program.commands) {
    command.exitOverride();
    command.configureOutput({ writeOut: () => {}, writeErr: () => {} });
  }
  return program.parseAsync(['node', 'cli.js', ...argv]);
}

describe('server stats on the command line', () => {
  let stdout;
  let stderr;

  beforeEach(() => {
    store.dir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-cli-server-stats-'));
    stdout = vi.spyOn(process.stdout, 'write').mockImplementation(() => true);
    stderr = vi.spyOn(process.stderr, 'write').mockImplementation(() => true);
    vi.spyOn(console, 'log').mockImplementation((line) => stdout(`${line}\n`));
    const payload = { pid: 7, version: '9.9.9', startedAt: new Date(0).toISOString(), ...new ServerStats().snapshot() };
    control.fetchServerStats = vi.fn().mockResolvedValue(payload);
  });

  afterEach(() => {
    fs.rmSync(store.dir, { recursive: true, force: true });
    process.exitCode = undefined;
    vi.restoreAllMocks();
  });

  const written = (spy) => spy.mock.calls.map(([chunk]) => String(chunk)).join('');

  it('reads the counters of the server in the store', async () => {
    await runProgram(['server', 'stats']);

    expect(control.fetchServerStats).toHaveBeenCalledWith(store.dir);
    expect(written(stdout)).toMatch(/^tgcli server 9\.9\.9, pid 7, up /);
    expect(written(stdout)).toContain('Requests to Telegram');
  });

  it.each([
    [['server', 'stats', '--json']],
    [['--json', 'server', 'stats']],
  ])('prints the answer as JSON: %j', async (argv) => {
    await runProgram(argv);

    expect(JSON.parse(written(stdout))).toMatchObject({ pid: 7, sinceStart: { requests: { total: 0 } } });
  });

  it('still runs the server for the arguments the CLI spawns it with', async () => {
    await runProgram(['server', '--idle-exit', '60s', '--auto-started']);

    // The empty store stops it before anything is spawned.
    expect(written(stderr)).toContain('Not authenticated');
    expect(control.fetchServerStats).not.toHaveBeenCalled();
  });

  it.each(['stat', 'status'])('starts no server for `server %s`', async (word) => {
    await expect(runProgram(['server', word])).rejects.toMatchObject({ code: 'commander.excessArguments' });

    expect(written(stderr)).not.toContain('Not authenticated');
  });
});
