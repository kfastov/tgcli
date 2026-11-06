// `tgcli server stop` and the version takeover against real server processes: a
// small stand-in serves the real control API handler from its own process, so
// the shutdown request, SIGTERM and SIGKILL fallbacks and the wait for the
// process to exit all run for real. Nothing here touches Telegram.

import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { invoke, ServerUnavailableError, stopServer, syncServerVersion } from '../core/control-client.js';
import { isPidAlive } from '../store-lock.js';

const CONTROL_SERVER_MODULE = fileURLToPath(new URL('../core/control-server.js', import.meta.url));

// Modes: 'current' answers /control/shutdown; 'legacy' predates it and only
// stops on SIGTERM, like 2.2.5; 'stubborn' ignores both.
const FAKE_SERVER = `
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { createControlRequestHandler, removeControlFile, writeControlFile } from ${JSON.stringify(CONTROL_SERVER_MODULE)};

const [storeDir, mode, version, idleMs = '0'] = process.argv.slice(2);
let server;
const stop = (how) => {
  fs.writeFileSync(path.join(storeDir, 'stopped-by'), how);
  server.close();
  removeControlFile(storeDir);
  setTimeout(() => process.exit(0), 100);
};
let firstClockRead = true;
const handler = createControlRequestHandler({
  service: { getJobCounts: () => ({ pending: 0, inProgress: 0 }) },
  token: 'tok',
  pid: process.pid,
  version,
  startedAt: new Date().toISOString(),
  onShutdown: mode === 'current' ? () => stop('control') : mode === 'stubborn' ? () => {} : undefined,
  // Pretends no CLI of its own version has called for idleMs.
  now: () => {
    const now = firstClockRead ? Date.now() - Number(idleMs) : Date.now();
    firstClockRead = false;
    return now;
  },
});
process.on('SIGTERM', mode === 'stubborn' ? () => {} : () => stop('signal'));
server = http.createServer((req, res) => void handler(req, res));
server.listen(0, '127.0.0.1', () => {
  writeControlFile(storeDir, {
    pid: process.pid,
    port: server.address().port,
    token: 'tok',
    startedAt: 's',
    version,
    autoStarted: true,
    manager: 'manual',
  });
});
`;

let storeDir;
let scriptPath;
let children;

async function startFakeServer({ mode = 'current', version = '0.0.1', idleMs = 0 } = {}) {
  const child = spawn(process.execPath, [scriptPath, storeDir, mode, version, String(idleMs)], { stdio: 'ignore' });
  children.push(child);
  await vi.waitFor(() => {
    expect(fs.existsSync(path.join(storeDir, 'control.json'))).toBe(true);
  }, { timeout: 5000 });
  return child.pid;
}

beforeEach(() => {
  storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-lifecycle-'));
  // Named like the real entry point, which is how a hung server is recognised.
  scriptPath = path.join(storeDir, 'mcp-server.js');
  fs.writeFileSync(scriptPath, FAKE_SERVER);
  children = [];
});

afterEach(() => {
  for (const child of children) {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill('SIGKILL');
    }
  }
  fs.rmSync(storeDir, { recursive: true, force: true });
  vi.restoreAllMocks();
});

describe('stopServer', () => {
  it('says so when no server runs', async () => {
    await expect(stopServer(storeDir)).resolves.toMatchObject({ running: false, stopped: false });
  });

  it('ignores a control.json left by a server that is gone', async () => {
    fs.writeFileSync(path.join(storeDir, 'control.json'), JSON.stringify({ pid: 2 ** 22 + 1, port: 1, token: 't', version: '1.0.0' }));
    await expect(stopServer(storeDir)).resolves.toMatchObject({ running: false });
  });

  it('stops a server over the control API and returns once its process is gone', async () => {
    const pid = await startFakeServer({ version: '2.2.6' });
    const onStopping = vi.fn();
    const result = await stopServer(storeDir, { onStopping });
    expect(result).toMatchObject({ running: true, stopped: true, method: 'control', pid, version: '2.2.6', autoStarted: true });
    expect(fs.readFileSync(path.join(storeDir, 'stopped-by'), 'utf8')).toBe('control');
    expect(onStopping).toHaveBeenCalledWith(expect.objectContaining({ pid, version: '2.2.6' }));
    expect(isPidAlive(pid)).toBe(false);
    expect(fs.existsSync(path.join(storeDir, 'control.json'))).toBe(false);
  });

  it('falls back to SIGTERM for a server that predates /control/shutdown', async () => {
    const pid = await startFakeServer({ mode: 'legacy', version: '2.2.5' });
    const result = await stopServer(storeDir);
    expect(result).toMatchObject({ stopped: true, method: 'signal', pid });
    expect(fs.readFileSync(path.join(storeDir, 'stopped-by'), 'utf8')).toBe('signal');
    expect(isPidAlive(pid)).toBe(false);
  });

  it('reports a server that does not exit, and kills it with force', async () => {
    const pid = await startFakeServer({ mode: 'stubborn' });
    await expect(stopServer(storeDir, { timeoutMs: 600 })).resolves.toMatchObject({ running: true, stopped: false, pid });
    expect(isPidAlive(pid)).toBe(true);

    const forced = await stopServer(storeDir, { timeoutMs: 600, force: true });
    expect(forced).toMatchObject({ stopped: true, method: 'kill', pid });
    expect(isPidAlive(pid)).toBe(false);
    expect(fs.existsSync(path.join(storeDir, 'control.json'))).toBe(false);
  });
});

describe('a CLI meeting a server on another version', () => {
  it('stops an older auto-started server over the control API', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const pid = await startFakeServer({ version: '0.0.1' });
    const error = await invoke(storeDir, { op: 'listChannels', args: {} }).catch((e) => e);
    expect(error).toBeInstanceOf(ServerUnavailableError);
    expect(isPidAlive(pid)).toBe(false);
    expect(fs.readFileSync(path.join(storeDir, 'stopped-by'), 'utf8')).toBe('control');
    expect(console.error).toHaveBeenCalledWith(expect.stringContaining('Restarting tgcli server (0.0.1 -> '));
  });

  it('leaves a dev build that its own CLI used just now alone', async () => {
    const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const pid = await startFakeServer({ version: '0.0.2-dev+0000000' });
    await expect(syncServerVersion(storeDir)).resolves.toBe('kept');
    expect(isPidAlive(pid)).toBe(true);
    expect(errSpy.mock.calls.flat().join('\n')).toContain('is a dev build in use');
  });

  it('takes over a dev build nobody has used for ten minutes, even a newer one', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const pid = await startFakeServer({ version: '999.0.0-dev+0000000', idleMs: 11 * 60 * 1000 });
    const error = await invoke(storeDir, { op: 'listChannels', args: {} }).catch((e) => e);
    expect(error).toBeInstanceOf(ServerUnavailableError);
    expect(isPidAlive(pid)).toBe(false);
  });

  it('leaves a newer release alone', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const pid = await startFakeServer({ version: '999.0.0' });
    await expect(syncServerVersion(storeDir, { quiet: true })).resolves.toBe('kept');
    expect(isPidAlive(pid)).toBe(true);
    expect(console.error).not.toHaveBeenCalled();
  });
});
