// `tgcli server stop`, `restart` and `logs` as the CLI prints them. The control
// client is stubbed; the log is a real file in a temporary store.

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const store = vi.hoisted(() => ({ dir: '/tmp/tgcli-cli-server-unset' }));
const control = vi.hoisted(() => ({
  stopServer: vi.fn(),
  restartServer: vi.fn(),
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
const { readVersion } = await import('../core/version.js');

const CLI_VERSION = readVersion();

let stdoutSpy;
let stderrSpy;
let logSpy;

async function run(argv) {
  const program = buildProgram();
  program.exitOverride();
  await program.parseAsync(['node', 'cli.js', ...argv]);
}

function stdout() {
  return [...stdoutSpy.mock.calls.map((call) => String(call[0])), ...logSpy.mock.calls.map((call) => `${call.join(' ')}\n`)].join('');
}

function stderr() {
  return stderrSpy.mock.calls.map((call) => String(call[0])).join('');
}

function server(fields = {}) {
  return { running: true, stopped: true, pid: 4242, version: '2.2.5', autoStarted: true, manager: 'manual', method: 'control', ...fields };
}

beforeEach(() => {
  store.dir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-cli-server-'));
  stdoutSpy = vi.spyOn(process.stdout, 'write').mockImplementation(() => true);
  stderrSpy = vi.spyOn(process.stderr, 'write').mockImplementation(() => true);
  logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
  control.stopServer.mockReset();
  control.restartServer.mockReset();
  control.syncServerVersion.mockClear();
});

afterEach(() => {
  fs.rmSync(store.dir, { recursive: true, force: true });
  vi.restoreAllMocks();
  process.exitCode = 0;
});

describe('tgcli server stop', () => {
  it('reports the server it stopped, after saying it is waiting for it', async () => {
    control.stopServer.mockImplementation(async (_storeDir, { onStopping }) => {
      onStopping(server());
      return server();
    });
    await run(['server', 'stop']);
    expect(control.stopServer).toHaveBeenCalledWith(store.dir, expect.objectContaining({ force: false }));
    expect(stderr()).toContain('Stopping tgcli server 2.2.5 (pid 4242)...');
    expect(stdout()).toContain('Stopped tgcli server 2.2.5 (pid 4242).');
    expect(control.syncServerVersion).not.toHaveBeenCalled();
  });

  it('says when nothing was running', async () => {
    control.stopServer.mockResolvedValue({ running: false, stopped: false, pid: null, version: null });
    await run(['server', 'stop']);
    expect(stdout()).toContain('No tgcli server is running.');
    expect(process.exitCode).not.toBe(1);
  });

  it('prints JSON', async () => {
    control.stopServer.mockResolvedValue(server({ method: 'signal' }));
    await run(['--json', 'server', 'stop']);
    expect(JSON.parse(stdout())).toEqual({
      running: true,
      stopped: true,
      server: { pid: 4242, version: '2.2.5', autoStarted: true, manager: 'manual', method: 'signal' },
    });
  });

  it('fails and points at --force when the server does not exit', async () => {
    control.stopServer.mockResolvedValue(server({ stopped: false }));
    await run(['server', 'stop']);
    expect(process.exitCode).toBe(1);
    expect(stderr()).toContain('has not exited a minute after it was asked to stop; `tgcli server stop --force` kills it.');
  });

  it('passes --force on', async () => {
    control.stopServer.mockResolvedValue(server({ method: 'kill' }));
    await run(['server', 'stop', '--force']);
    expect(control.stopServer).toHaveBeenCalledWith(store.dir, expect.objectContaining({ force: true }));
  });

  it('warns that a service manager may start its server again', async () => {
    control.stopServer.mockResolvedValue(server({ autoStarted: false, manager: 'launchd' }));
    await run(['server', 'stop']);
    expect(stdout()).toContain('launchd runs it as a service and may start it again; `tgcli service stop` keeps it stopped.');
  });
});

describe('tgcli server restart', () => {
  beforeEach(() => {
    saveConfig(store.dir, { apiId: 1, apiHash: 'hash', phoneNumber: '+10000000000' });
  });

  it('reports both servers', async () => {
    control.restartServer.mockResolvedValue({
      previous: server(),
      current: { ok: true, pid: 4343, version: CLI_VERSION, startedAt: 'now' },
    });
    await run(['server', 'restart']);
    expect(stdout()).toContain(`Restarted tgcli server: 2.2.5 (pid 4242) -> ${CLI_VERSION} (pid 4343).`);
  });

  it('reports a fresh start and prints JSON', async () => {
    control.restartServer.mockResolvedValue({
      previous: { running: false, stopped: false },
      current: { ok: true, pid: 4343, version: CLI_VERSION, startedAt: 'now' },
    });
    await run(['--json', 'server', 'restart']);
    expect(JSON.parse(stdout())).toEqual({
      previous: null,
      current: { pid: 4343, version: CLI_VERSION, startedAt: 'now' },
    });
  });

  it('says when another version came up instead', async () => {
    control.restartServer.mockResolvedValue({
      previous: server({ autoStarted: false, manager: 'brew' }),
      current: { ok: true, pid: 4343, version: '2.2.5' },
    });
    await run(['server', 'restart']);
    expect(stdout()).toContain(`It runs 2.2.5, not this CLI's ${CLI_VERSION}: brew started it.`);
  });

  it('fails when the old server does not exit', async () => {
    control.restartServer.mockResolvedValue({ previous: server({ stopped: false }), current: null });
    await run(['server', 'restart']);
    expect(process.exitCode).toBe(1);
    expect(stderr()).toContain('has not exited');
  });

  it('needs a configured account before it stops anything', async () => {
    fs.rmSync(path.join(store.dir, 'config.json'), { force: true });
    await run(['server', 'restart']);
    expect(process.exitCode).toBe(1);
    expect(stderr()).toContain('Not authenticated');
    expect(control.restartServer).not.toHaveBeenCalled();
  });
});

describe('tgcli server logs', () => {
  function writeLog(name, lines) {
    fs.writeFileSync(path.join(store.dir, name), lines.map((line) => `${line}\n`).join(''));
  }

  it('prints the last lines, 100 by default', async () => {
    writeLog('server.log', Array.from({ length: 150 }, (_, index) => `line ${index}`));
    await run(['server', 'logs']);
    const lines = stdout().trim().split('\n');
    expect(lines).toHaveLength(100);
    expect(lines[0]).toBe('line 50');
    expect(lines[99]).toBe('line 149');
  });

  it('takes -n and reaches into the rotated log', async () => {
    writeLog('server.log.1', ['old 1', 'old 2']);
    writeLog('server.log', ['new 1']);
    await run(['server', 'logs', '-n', '2']);
    expect(stdout()).toBe('old 2\nnew 1\n');
  });

  it('says where the log will be when there is none', async () => {
    await run(['server', 'logs']);
    expect(stdout()).toContain(`No server log yet (${path.join(store.dir, 'server.log')}).`);
  });

  it('prints JSON', async () => {
    writeLog('server.log', ['a', 'b']);
    await run(['--json', 'server', 'logs']);
    expect(JSON.parse(stdout())).toEqual({ path: path.join(store.dir, 'server.log'), exists: true, lines: ['a', 'b'] });
  });

  it('follows new lines until --timeout', async () => {
    writeLog('server.log', ['before']);
    const following = run(['--timeout', '1500ms', 'server', 'logs', '--follow']);
    setTimeout(() => fs.appendFileSync(path.join(store.dir, 'server.log'), 'after\n'), 200);
    await following;
    expect(stdout()).toBe('before\nafter\n');
    expect(process.exitCode).not.toBe(1);
  });
});
