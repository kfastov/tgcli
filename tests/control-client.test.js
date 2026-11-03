// Tests for the control client (issue #30/#26): the thin HTTP layer the one-shot
// CLI uses to drive the always-on control server. Global `fetch` and
// child_process.spawn are mocked — no real network, no real process.

import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const spawnMock = vi.hoisted(() => vi.fn());
const spawnSyncMock = vi.hoisted(() => vi.fn());
vi.mock('node:child_process', () => ({ spawn: spawnMock, spawnSync: spawnSyncMock }));
vi.mock('child_process', () => ({ spawn: spawnMock, spawnSync: spawnSyncMock }));

import {
  cancelBackfill,
  enqueueBackfill,
  ensureServer,
  invoke,
  pingServer,
  readControlFile,
  retryBackfill,
  ServerUnavailableError,
} from '../core/control-client.js';
import { CONTROL_TOKEN_HEADER } from '../core/control-server.js';
import { SendCommandError } from '../core/send-utils.js';

const realFetch = global.fetch;
const CLI_VERSION = JSON.parse(fs.readFileSync(new URL('../package.json', import.meta.url), 'utf8')).version;

let storeDir;

// A control file describes a server on this CLI's version unless a test says otherwise.
function writeControl(descriptor) {
  fs.writeFileSync(path.join(storeDir, 'control.json'), JSON.stringify({ version: CLI_VERSION, ...descriptor }));
}

function jsonResponse(status, body) {
  return {
    status,
    json: () => Promise.resolve(body),
  };
}

beforeEach(() => {
  storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-control-client-'));
  spawnMock.mockReset();
  spawnMock.mockReturnValue({ unref: vi.fn() });
  spawnSyncMock.mockReset();
  spawnSyncMock.mockReturnValue({ status: 1, stdout: '' });
  global.fetch = vi.fn();
});

afterEach(() => {
  fs.rmSync(storeDir, { recursive: true, force: true });
  vi.restoreAllMocks();
});

describe('readControlFile', () => {
  it('returns null when control.json is absent', () => {
    expect(readControlFile(storeDir)).toBeNull();
  });

  it('parses a present control.json', () => {
    writeControl({ pid: 1, port: 8765, token: 'tok', startedAt: 'now', version: CLI_VERSION });
    expect(readControlFile(storeDir)).toMatchObject({ port: 8765, token: 'tok' });
  });

  it('returns null on malformed JSON', () => {
    fs.writeFileSync(path.join(storeDir, 'control.json'), '{not json');
    expect(readControlFile(storeDir)).toBeNull();
  });
});

describe('pingServer', () => {
  it('returns null with no control file (no fetch attempted)', async () => {
    const result = await pingServer(storeDir);
    expect(result).toBeNull();
    expect(global.fetch).not.toHaveBeenCalled();
  });

  it('returns the body and sends the token header when reachable', async () => {
    writeControl({ pid: 9, port: 8765, token: 'secret', startedAt: 's' });
    global.fetch.mockResolvedValue(jsonResponse(200, { ok: true, pid: 9, version: CLI_VERSION }));
    const result = await pingServer(storeDir);
    expect(result).toMatchObject({ ok: true, pid: 9 });
    const [url, init] = global.fetch.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/control/ping');
    expect(init.headers[CONTROL_TOKEN_HEADER]).toBe('secret');
  });

  it('returns null when the server is unreachable', async () => {
    writeControl({ pid: 9, port: 8765, token: 'secret', startedAt: 's' });
    global.fetch.mockRejectedValue(new Error('ECONNREFUSED'));
    expect(await pingServer(storeDir)).toBeNull();
  });

  it('returns null on a non-200 response', async () => {
    writeControl({ pid: 9, port: 8765, token: 'secret', startedAt: 's' });
    global.fetch.mockResolvedValue(jsonResponse(401, { error: 'Unauthorized' }));
    expect(await pingServer(storeDir)).toBeNull();
  });
});

describe('enqueueBackfill / cancelBackfill', () => {
  beforeEach(() => {
    writeControl({ pid: 9, port: 8765, token: 'secret', startedAt: 's' });
  });

  it('POSTs to /control/backfill and returns the job descriptor', async () => {
    global.fetch.mockResolvedValue(jsonResponse(200, { jobId: 7, channelId: '-100', status: 'pending' }));
    const result = await enqueueBackfill(storeDir, { chatId: '@c', depth: 50, minDate: '2026-01-01' });
    expect(result).toEqual({ jobId: 7, channelId: '-100', status: 'pending' });
    const [url, init] = global.fetch.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/control/backfill');
    expect(init.method).toBe('POST');
    expect(JSON.parse(init.body)).toEqual({ chatId: '@c', depth: 50, minDate: '2026-01-01' });
  });

  it('throws the server error message on a non-200 backfill response', async () => {
    global.fetch.mockResolvedValue(jsonResponse(400, { error: 'chatId is required' }));
    await expect(enqueueBackfill(storeDir, {})).rejects.toThrow('chatId is required');
  });

  it('POSTs to /control/cancel', async () => {
    global.fetch.mockResolvedValue(jsonResponse(200, { canceled: 2, jobIds: [1, 2] }));
    const result = await cancelBackfill(storeDir, { chatId: '@c' });
    expect(result).toEqual({ canceled: 2, jobIds: [1, 2] });
    const [url, init] = global.fetch.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/control/cancel');
    expect(JSON.parse(init.body)).toEqual({ chatId: '@c', jobId: undefined });
  });

  it('POSTs to /control/retry and returns the reset result', async () => {
    global.fetch.mockResolvedValue(jsonResponse(200, { updated: 3, jobIds: [1, 2, 3] }));
    const result = await retryBackfill(storeDir, { allErrors: true });
    expect(result).toEqual({ updated: 3, jobIds: [1, 2, 3] });
    const [url, init] = global.fetch.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/control/retry');
    expect(init.method).toBe('POST');
    expect(JSON.parse(init.body)).toEqual({ jobId: undefined, channelId: undefined, allErrors: true });
  });

  it('throws the server error message on a non-200 retry response', async () => {
    global.fetch.mockResolvedValue(jsonResponse(400, { error: 'Provide jobId, channelId, or allErrors' }));
    await expect(retryBackfill(storeDir, {})).rejects.toThrow('Provide jobId, channelId, or allErrors');
  });
});

describe('invoke', () => {
  beforeEach(() => {
    writeControl({ pid: 9, port: 8765, token: 'secret', startedAt: 's' });
  });

  it('POSTs { op, args } to /control/invoke and returns result', async () => {
    global.fetch.mockResolvedValue(jsonResponse(200, { result: [{ id: '1' }] }));
    const result = await invoke(storeDir, { op: 'listChannels', args: { limit: 5 } });
    expect(result).toEqual([{ id: '1' }]);
    const [url, init] = global.fetch.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/control/invoke');
    expect(init.method).toBe('POST');
    expect(init.headers[CONTROL_TOKEN_HEADER]).toBe('secret');
    expect(JSON.parse(init.body)).toEqual({ op: 'listChannels', args: { limit: 5 } });
  });

  it('throws the server error message on a non-200 response', async () => {
    global.fetch.mockResolvedValue(jsonResponse(400, { error: 'Unknown operation: nope' }));
    const error = await invoke(storeDir, { op: 'nope', args: {} }).catch((e) => e);
    expect(error.message).toBe('Unknown operation: nope');
    // An op that ran and failed is NOT a connection failure.
    expect(error).not.toBeInstanceOf(ServerUnavailableError);
  });

  it('throws ServerUnavailableError when no control file is present', async () => {
    fs.rmSync(path.join(storeDir, 'control.json'));
    const error = await invoke(storeDir, { op: 'listChannels', args: {} }).catch((e) => e);
    expect(error).toBeInstanceOf(ServerUnavailableError);
    expect(global.fetch).not.toHaveBeenCalled();
  });

  it('throws ServerUnavailableError on a refused loopback connection', async () => {
    const refused = new TypeError('fetch failed');
    refused.cause = Object.assign(new Error('connect ECONNREFUSED'), { code: 'ECONNREFUSED' });
    global.fetch.mockRejectedValue(refused);
    const error = await invoke(storeDir, { op: 'listChannels', args: {} }).catch((e) => e);
    expect(error).toBeInstanceOf(ServerUnavailableError);
    expect(error.cause).toBe(refused);
  });

  it('re-throws a request timeout (not a connection failure, so no retry/spawn)', async () => {
    const timeout = new Error('The operation was aborted due to timeout');
    timeout.name = 'TimeoutError';
    global.fetch.mockRejectedValue(timeout);
    const error = await invoke(storeDir, { op: 'sendText', args: {} }).catch((e) => e);
    expect(error).toBe(timeout);
    expect(error).not.toBeInstanceOf(ServerUnavailableError);
  });

  it('reconstructs a SendCommandError from a sendError payload', async () => {
    global.fetch.mockResolvedValue(jsonResponse(500, {
      error: 'FLOOD_WAIT_120',
      sendError: { type: 'rate_limit', method: 'sendText', message: 'FLOOD_WAIT_120', waitSeconds: 120 },
    }));
    const error = await invoke(storeDir, { op: 'sendText', args: {} }).catch((e) => e);
    expect(error).toBeInstanceOf(SendCommandError);
    expect(error.details).toMatchObject({ type: 'rate_limit', method: 'sendText', waitSeconds: 120 });
  });

  it('passes timeoutMs through as the request budget (0 disables it)', async () => {
    global.fetch.mockResolvedValue(jsonResponse(200, { result: { ok: true } }));
    await invoke(storeDir, { op: 'sendText', args: {}, timeoutMs: 0 });
    const [, init] = global.fetch.mock.calls[0];
    expect(init.signal).toBeUndefined();
  });
});

describe('ensureServer', () => {
  it('uses an existing server when ping succeeds (no spawn)', async () => {
    writeControl({ pid: 9, port: 8765, token: 'secret', startedAt: 's' });
    global.fetch.mockResolvedValue(jsonResponse(200, { ok: true, pid: 9 }));
    const result = await ensureServer(storeDir, { idleExit: '60s' });
    expect(result).toMatchObject({ ok: true, pid: 9 });
    expect(spawnMock).not.toHaveBeenCalled();
  });

  it('spawns a detached server and polls until it becomes reachable', async () => {
    // First ping (pre-spawn) fails: no control file yet. After "spawn" we write
    // the control file and make subsequent pings succeed.
    let started = false;
    spawnMock.mockImplementation(() => {
      started = true;
      writeControl({ pid: 42, port: 8765, token: 'secret', startedAt: 's' });
      return { unref: vi.fn() };
    });
    global.fetch.mockImplementation(() => {
      if (!started) {
        return Promise.reject(new Error('ECONNREFUSED'));
      }
      return Promise.resolve(jsonResponse(200, { ok: true, pid: 42 }));
    });

    const result = await ensureServer(storeDir, { idleExit: '60s' });
    expect(result).toMatchObject({ ok: true, pid: 42 });
    expect(spawnMock).toHaveBeenCalledTimes(1);
    const [, args, opts] = spawnMock.mock.calls[0];
    expect(args).toContain('server');
    expect(args).toContain('--idle-exit');
    expect(args).toContain('60s');
    expect(opts).toMatchObject({ detached: true, stdio: 'ignore' });
  });

  it('handles the race where another process started the server first', async () => {
    // No control file initially; "spawn" does nothing, but a concurrent process
    // wrote control.json before our first poll tick.
    spawnMock.mockImplementation(() => {
      writeControl({ pid: 99, port: 8765, token: 'secret', startedAt: 's' });
      return { unref: vi.fn() };
    });
    global.fetch.mockResolvedValue(jsonResponse(200, { ok: true, pid: 99 }));
    const result = await ensureServer(storeDir, {});
    expect(result).toMatchObject({ ok: true, pid: 99 });
  });
});

describe('server older or newer than this CLI', () => {
  const OLD_VERSION = '0.0.1';
  const NEW_VERSION = '999.0.0';
  let pid = 1000;
  let errSpy;
  let killSpy;
  let serverAlive;

  function runningServer({ version = OLD_VERSION, jobs = { pending: 0, inProgress: 0 }, args = '--idle-exit 60000' } = {}) {
    pid += 1;
    writeControl({ pid, port: 8765, token: 'secret', startedAt: 's', version });
    spawnSyncMock.mockReturnValue({
      status: 0,
      stdout: `node /x/mcp-server.js ${args}\n`,
    });
    global.fetch.mockImplementation((url) => Promise.resolve(url.endsWith('/control/ping')
      ? jsonResponse(200, { ok: true, pid, version, startedAt: 's', jobs })
      : jsonResponse(200, { result: 'done' })));
    return pid;
  }

  function invokeCalls() {
    return global.fetch.mock.calls.filter(([url]) => url.endsWith('/control/invoke'));
  }

  function stderr() {
    return errSpy.mock.calls.flat().join('\n');
  }

  beforeEach(() => {
    serverAlive = true;
    errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    // Stands in for the real server: SIGTERM removes control.json and ends it.
    killSpy = vi.spyOn(process, 'kill').mockImplementation((_pid, signal) => {
      if (signal === 'SIGTERM') {
        fs.rmSync(path.join(storeDir, 'control.json'), { force: true });
        serverAlive = false;
        return true;
      }
      if (!serverAlive) {
        throw Object.assign(new Error('kill ESRCH'), { code: 'ESRCH' });
      }
      return true;
    });
  });

  it('leaves a server on the same version alone, at the cost of one round-trip', async () => {
    const serverPid = runningServer({ version: CLI_VERSION });
    await expect(invoke(storeDir, { op: 'listChannels', args: {} })).resolves.toBe('done');
    expect(global.fetch).toHaveBeenCalledTimes(1);
    expect(killSpy).not.toHaveBeenCalledWith(serverPid, 'SIGTERM');
    expect(errSpy).not.toHaveBeenCalled();
  });

  it('invoke stops an idle auto-started older server and reports it as unavailable', async () => {
    const serverPid = runningServer();
    const error = await invoke(storeDir, { op: 'listChannels', args: {} }).catch((e) => e);
    expect(killSpy).toHaveBeenCalledWith(serverPid, 'SIGTERM');
    // The caller's existing cold-start path starts the new version from here.
    expect(error).toBeInstanceOf(ServerUnavailableError);
    expect(invokeCalls()).toHaveLength(0);
    expect(stderr()).toContain(`${OLD_VERSION} -> ${CLI_VERSION}`);
  });

  it('ensureServer replaces an idle auto-started older server with a new one', async () => {
    const serverPid = runningServer();
    spawnMock.mockImplementation(() => {
      writeControl({ pid: serverPid + 1, port: 8765, token: 'secret', startedAt: 's', version: CLI_VERSION });
      return { unref: vi.fn() };
    });
    const result = await ensureServer(storeDir, { idleExit: '60s' });
    expect(killSpy).toHaveBeenCalledWith(serverPid, 'SIGTERM');
    expect(spawnMock).toHaveBeenCalledTimes(1);
    expect(result).toMatchObject({ ok: true });
  });

  it.each([
    ['without --idle-exit', ''],
    ['with its own --idle-exit window', '--idle-exit 300000'],
  ])('warns on stderr only and keeps a server the user started %s', async (_label, args) => {
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    const serverPid = runningServer({ args });
    await expect(invoke(storeDir, { op: 'listChannels', args: {} })).resolves.toBe('done');
    expect(killSpy).not.toHaveBeenCalledWith(serverPid, 'SIGTERM');
    expect(stderr()).toContain(`pid ${serverPid}`);
    expect(stderr()).toContain(OLD_VERSION);
    expect(stderr()).toContain('Restart it');
    expect(logSpy).not.toHaveBeenCalled();
  });

  it('warns and keeps the server when it cannot tell who started it', async () => {
    const serverPid = runningServer();
    spawnSyncMock.mockReturnValue({ status: null, stdout: null, error: new Error('spawnSync ps ENOENT') });
    await expect(invoke(storeDir, { op: 'listChannels', args: {} })).resolves.toBe('done');
    expect(killSpy).not.toHaveBeenCalledWith(serverPid, 'SIGTERM');
    expect(stderr()).toContain('Restart it');
  });

  it.each([
    ['pending', { pending: 1, inProgress: 0 }],
    ['in-progress', { pending: 0, inProgress: 1 }],
  ])('defers the restart while a backfill job is %s', async (_label, jobs) => {
    const serverPid = runningServer({ jobs });
    await expect(invoke(storeDir, { op: 'listChannels', args: {} })).resolves.toBe('done');
    expect(killSpy).not.toHaveBeenCalledWith(serverPid, 'SIGTERM');
    expect(stderr()).toContain('once its backfill queue is empty');
  });

  it('warns instead of downgrading a server newer than this CLI', async () => {
    const serverPid = runningServer({ version: NEW_VERSION });
    await expect(invoke(storeDir, { op: 'listChannels', args: {} })).resolves.toBe('done');
    expect(killSpy).not.toHaveBeenCalledWith(serverPid, 'SIGTERM');
    expect(stderr()).toContain(NEW_VERSION);
  });

  it('compares versions numerically', async () => {
    const [major, minor, patch] = CLI_VERSION.split('.').map(Number);
    const serverPid = runningServer({ version: `${major}.${minor}.${patch + 10}` });
    await expect(invoke(storeDir, { op: 'listChannels', args: {} })).resolves.toBe('done');
    expect(killSpy).not.toHaveBeenCalledWith(serverPid, 'SIGTERM');
  });

  it('--quiet suppresses the warning and the restart notice but not the restart', async () => {
    runningServer({ args: '' });
    await invoke(storeDir, { op: 'listChannels', args: {}, quiet: true });
    expect(errSpy).not.toHaveBeenCalled();

    const serverPid = runningServer();
    await invoke(storeDir, { op: 'listChannels', args: {}, quiet: true }).catch(() => {});
    expect(killSpy).toHaveBeenCalledWith(serverPid, 'SIGTERM');
    expect(errSpy).not.toHaveBeenCalled();
  });

  it('ignores a stale control.json whose server no longer answers', async () => {
    const serverPid = runningServer();
    global.fetch.mockRejectedValue(new TypeError('fetch failed'));
    const error = await invoke(storeDir, { op: 'listChannels', args: {} }).catch((e) => e);
    expect(error).toBeInstanceOf(ServerUnavailableError);
    expect(killSpy).not.toHaveBeenCalledWith(serverPid, 'SIGTERM');
    expect(errSpy).not.toHaveBeenCalled();
  });

  it('warns and carries on when the old server does not exit in time', async () => {
    vi.useFakeTimers();
    try {
      runningServer();
      killSpy.mockImplementation(() => true);
      const done = invoke(storeDir, { op: 'listChannels', args: {} });
      await vi.advanceTimersByTimeAsync(60000);
      await expect(done).resolves.toBe('done');
      expect(stderr()).toContain('has not exited');
    } finally {
      vi.useRealTimers();
    }
  });

  it('waits for a server that is shutting down before starting its replacement', async () => {
    const serverPid = runningServer({ version: CLI_VERSION });
    global.fetch.mockRejectedValue(new TypeError('fetch failed'));
    setTimeout(() => {
      serverAlive = false;
    }, 50);
    let aliveAtSpawn;
    spawnMock.mockImplementation(() => {
      aliveAtSpawn = serverAlive;
      writeControl({ pid: serverPid + 1, port: 8765, token: 'secret', startedAt: 's' });
      global.fetch.mockResolvedValue(jsonResponse(200, { ok: true, pid: serverPid + 1 }));
      return { unref: vi.fn() };
    });
    await ensureServer(storeDir, {});
    expect(aliveAtSpawn).toBe(false);
  });

  it('does not wait on a stale control.json whose pid now belongs to something else', async () => {
    const serverPid = runningServer({ version: CLI_VERSION });
    spawnSyncMock.mockReturnValue({ status: 0, stdout: 'vim notes.txt\n' });
    global.fetch.mockRejectedValue(new TypeError('fetch failed'));
    spawnMock.mockImplementation(() => {
      writeControl({ pid: serverPid + 1, port: 8765, token: 'secret', startedAt: 's' });
      global.fetch.mockResolvedValue(jsonResponse(200, { ok: true, pid: serverPid + 1 }));
      return { unref: vi.fn() };
    });
    await ensureServer(storeDir, {});
    expect(killSpy).not.toHaveBeenCalled();
    expect(spawnMock).toHaveBeenCalledTimes(1);
  });
});

describe('invoke over a real socket', () => {
  let server;

  afterEach(async () => {
    if (server) {
      server.closeAllConnections();
      await new Promise((resolve) => server.close(resolve));
      server = null;
    }
  });

  async function listen(handler) {
    server = http.createServer(handler);
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    return server.address().port;
  }

  it('does not treat a request lost midway as an unreachable server, so it is not retried', async () => {
    global.fetch = realFetch;
    const port = await listen((req) => req.socket.destroy());
    writeControl({ pid: 9, port, token: 'secret', startedAt: 's' });
    const error = await invoke(storeDir, { op: 'sendText', args: {} }).catch((e) => e);
    expect(error).not.toBeInstanceOf(ServerUnavailableError);
    expect(error.message).toContain('sendText');
    expect(error.message).toContain('may or may not have completed');
  });

  it('still treats a refused connection as an unreachable server', async () => {
    global.fetch = realFetch;
    const port = await listen(() => {});
    await new Promise((resolve) => server.close(resolve));
    server = null;
    writeControl({ pid: 9, port, token: 'secret', startedAt: 's' });
    const error = await invoke(storeDir, { op: 'sendText', args: {} }).catch((e) => e);
    expect(error).toBeInstanceOf(ServerUnavailableError);
  });
});
