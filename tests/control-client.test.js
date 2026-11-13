// Tests for the control client (issue #30/#26): the thin HTTP layer the one-shot
// CLI uses to drive the always-on control server. Global `fetch` and
// child_process.spawn are mocked — no real network, no real process.

import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const spawnMock = vi.hoisted(() => vi.fn());
const spawnSyncMock = vi.hoisted(() => vi.fn());
vi.mock('node:child_process', () => ({ spawn: spawnMock, spawnSync: spawnSyncMock }));
vi.mock('child_process', () => ({ spawn: spawnMock, spawnSync: spawnSyncMock }));

import {
  cancelBackfill,
  DEV_LEASE_MS,
  enqueueBackfill,
  enqueueRepair,
  ensureServer,
  fetchServerStats,
  invoke,
  pingServer,
  readControlFile,
  restartServer,
  retryBackfill,
  ServerUnavailableError,
  syncServerVersion,
  versionTakeover,
} from '../core/control-client.js';
import { CONTROL_CLIENT_VERSION_HEADER, CONTROL_TOKEN_HEADER } from '../core/control-server.js';
import { SendCommandError } from '../core/send-utils.js';
import { parseVersion, readVersion } from '../core/version.js';

const realFetch = global.fetch;
const CLI_VERSION = readVersion();

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
    expect(init.headers[CONTROL_CLIENT_VERSION_HEADER]).toBe(CLI_VERSION);
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

describe('fetchServerStats', () => {
  it('GETs /control/stats with the token and returns the counters', async () => {
    writeControl({ pid: 9, port: 8765, token: 'secret', startedAt: 's' });
    global.fetch.mockResolvedValue(jsonResponse(200, { pid: 9, sinceStart: {}, lastHour: {} }));

    await expect(fetchServerStats(storeDir)).resolves.toEqual({ pid: 9, sinceStart: {}, lastHour: {} });
    const [url, init] = global.fetch.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/control/stats');
    expect(init.method).toBe('GET');
    expect(init.headers[CONTROL_TOKEN_HEADER]).toBe('secret');
  });

  it('says no server is running when there is no control file, and starts none', async () => {
    await expect(fetchServerStats(storeDir)).rejects.toThrow(ServerUnavailableError);
    expect(global.fetch).not.toHaveBeenCalled();
    expect(spawnMock).not.toHaveBeenCalled();
  });

  it('says no server is running when the control file outlived it', async () => {
    writeControl({ pid: 9, port: 8765, token: 'secret', startedAt: 's' });
    global.fetch.mockRejectedValue(Object.assign(new TypeError('fetch failed'), { cause: { code: 'ECONNREFUSED' } }));

    await expect(fetchServerStats(storeDir)).rejects.toThrow('No tgcli server is running.');
    expect(spawnMock).not.toHaveBeenCalled();
  });

  it('leaves a server on another version running', async () => {
    writeControl({ pid: 9, port: 8765, token: 'secret', startedAt: 's', version: '0.0.1', autoStarted: true });
    global.fetch.mockResolvedValue(jsonResponse(200, { pid: 9 }));
    const kill = vi.spyOn(process, 'kill').mockImplementation(() => true);

    await fetchServerStats(storeDir);

    expect(kill).not.toHaveBeenCalled();
    expect(global.fetch).toHaveBeenCalledTimes(1);
  });

  it('says to restart a server too old to know /control/stats', async () => {
    writeControl({ pid: 9, port: 8765, token: 'secret', startedAt: 's', version: '2.2.5' });
    global.fetch.mockResolvedValue(jsonResponse(404, { error: 'Not found' }));

    await expect(fetchServerStats(storeDir)).rejects.toThrow(/server \(2\.2\.5, pid 9\) predates `server stats`; restart it/);
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

  it('POSTs to /control/repair and returns the queued chats', async () => {
    global.fetch.mockResolvedValue(jsonResponse(200, { channelIds: ['-100'] }));
    const result = await enqueueRepair(storeDir, { chats: ['@c'], all: false, afterId: 0, sinceDate: 1700000000 });
    expect(result).toEqual({ channelIds: ['-100'] });
    const [url, init] = global.fetch.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/control/repair');
    expect(JSON.parse(init.body)).toEqual({ chats: ['@c'], all: false, afterId: 0, sinceDate: 1700000000 });
  });

  it('sends a dry run to a path of its own, and says when the server is too old for it', async () => {
    global.fetch.mockResolvedValue(jsonResponse(200, { channelIds: ['-100'] }));
    await enqueueRepair(storeDir, { all: true, dryRun: true });
    expect(global.fetch.mock.calls[0][0]).toBe('http://127.0.0.1:8765/control/repair/dry-run');

    writeControl({ pid: 9, port: 8765, token: 'secret', startedAt: 's', version: '2.2.5' });
    global.fetch.mockResolvedValue(jsonResponse(404, { error: 'Not found' }));
    await expect(enqueueRepair(storeDir, { all: true, dryRun: true })).rejects.toThrow(/predates `backfill repair --dry-run`/);
  });

  it('says to restart a server too old to know /control/repair', async () => {
    writeControl({ pid: 9, port: 8765, token: 'secret', startedAt: 's', version: '2.2.4' });
    global.fetch.mockResolvedValue(jsonResponse(404, { error: 'Not found' }));
    await expect(enqueueRepair(storeDir, { all: true })).rejects.toThrow(/server \(2\.2\.4, pid 9\) predates `backfill repair`; restart it/);
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
    const result = await ensureServer(storeDir);
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

    const result = await ensureServer(storeDir);
    expect(result).toMatchObject({ ok: true, pid: 42 });
    expect(spawnMock).toHaveBeenCalledTimes(1);
    const [, args, opts] = spawnMock.mock.calls[0];
    expect(args).toContain('server');
    expect(args).toContain('--idle-exit');
    expect(args).toContain('60s');
    expect(args).toContain('--auto-started');
    expect(opts).toMatchObject({ detached: true, stdio: 'ignore' });
  });

  it('refuses to start a server while another live process holds the store lock', async () => {
    // `tgcli store compact` (or a login) holds the write lock; a server started
    // under it would write to the archive during the rewrite and VACUUM.
    fs.writeFileSync(path.join(storeDir, 'LOCK'), JSON.stringify({ pid: process.ppid, startedAt: 's' }));
    global.fetch.mockRejectedValue(new Error('ECONNREFUSED'));

    await expect(ensureServer(storeDir)).rejects.toThrow(/locked by another tgcli process/);
    expect(spawnMock).not.toHaveBeenCalled();
  });

  it('ignores a stale store lock left by a dead process', async () => {
    fs.writeFileSync(path.join(storeDir, 'LOCK'), JSON.stringify({ pid: 2 ** 22 + 12345, startedAt: 's' }));
    let started = false;
    spawnMock.mockImplementation(() => {
      started = true;
      writeControl({ pid: 42, port: 8765, token: 'secret', startedAt: 's' });
      return { unref: vi.fn() };
    });
    global.fetch.mockImplementation(() => (started
      ? Promise.resolve(jsonResponse(200, { ok: true, pid: 42 }))
      : Promise.reject(new Error('ECONNREFUSED'))));

    await expect(ensureServer(storeDir)).resolves.toMatchObject({ pid: 42 });
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

  function runningServer({
    version = OLD_VERSION,
    jobs = { pending: 0, inProgress: 0 },
    args = '--idle-exit 60000',
    tty = '??',
    owner = {},
    ping = {},
  } = {}) {
    pid += 1;
    writeControl({ pid, port: 8765, token: 'secret', startedAt: 's', version, ...owner });
    spawnSyncMock.mockImplementation((_command, psArgs) => ({
      status: 0,
      stdout: psArgs.includes('tty=') ? `${tty}\n` : `node /x/mcp-server.js ${args}\n`,
    }));
    global.fetch.mockImplementation((url) => Promise.resolve(url.endsWith('/control/ping')
      ? jsonResponse(200, { ok: true, pid, version, startedAt: 's', jobs, ...ping })
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

  it('replaces a server that says it was auto-started, whatever its window', async () => {
    const serverPid = runningServer({ args: '--idle-exit 120000', owner: { autoStarted: true } });
    await invoke(storeDir, { op: 'listChannels', args: {} }).catch(() => {});
    expect(killSpy).toHaveBeenCalledWith(serverPid, 'SIGTERM');
  });

  it('leaves a server started by hand alone, even with the auto-started window', async () => {
    const serverPid = runningServer({ tty: '??', owner: { autoStarted: false } });
    await expect(invoke(storeDir, { op: 'listChannels', args: {} })).resolves.toBe('done');
    expect(killSpy).not.toHaveBeenCalledWith(serverPid, 'SIGTERM');
    expect(stderr()).toContain('Restart it to use this version');
  });

  it('leaves an older server alone when it has a terminal, whatever its window', async () => {
    const serverPid = runningServer({ tty: 'ttys003' });
    await expect(invoke(storeDir, { op: 'listChannels', args: {} })).resolves.toBe('done');
    expect(killSpy).not.toHaveBeenCalledWith(serverPid, 'SIGTERM');
    expect(stderr()).toContain('Restart it to use this version');
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
    const result = await ensureServer(storeDir);
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

  it('defers the restart while the server answers another CLI', async () => {
    const serverPid = runningServer({ ping: { inFlight: 1 } });
    await expect(invoke(storeDir, { op: 'listChannels', args: {} })).resolves.toBe('done');
    expect(killSpy).not.toHaveBeenCalledWith(serverPid, 'SIGTERM');
    expect(stderr()).toContain('no request is running');
  });

  it('replaces a newer server that runs from its own directory', async () => {
    const root = fileURLToPath(new URL('..', import.meta.url));
    const serverPid = runningServer({ version: NEW_VERSION, ping: { root } });
    await invoke(storeDir, { op: 'listChannels', args: {} }).catch(() => {});
    expect(killSpy).toHaveBeenCalledWith(serverPid, 'SIGTERM');
  });

  it('warns instead of downgrading a server newer than this CLI', async () => {
    const serverPid = runningServer({ version: NEW_VERSION });
    await expect(invoke(storeDir, { op: 'listChannels', args: {} })).resolves.toBe('done');
    expect(killSpy).not.toHaveBeenCalledWith(serverPid, 'SIGTERM');
    expect(stderr()).toContain(NEW_VERSION);
  });

  it('compares versions numerically', async () => {
    const { major, minor, patch } = parseVersion(CLI_VERSION);
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

describe('versionTakeover', () => {
  const recent = DEV_LEASE_MS - 1;
  const stale = DEV_LEASE_MS;

  it.each([
    ['the same version', '2.2.5', '2.2.5', undefined, 'same'],
    ['an older release', '2.2.4', '2.2.5', undefined, 'take-over'],
    ['a newer release', '2.2.6', '2.2.5', undefined, 'newer'],
    ['the release a dev build grew from', '2.2.5', '2.2.6-dev+abc1234', undefined, 'take-over'],
    ['a release newer than a dev build', '2.3.0', '2.2.6-dev+abc1234', undefined, 'newer'],
    ['a dev build used recently, from a release', '2.2.6-dev+abc1234', '2.2.5', recent, 'dev-in-use'],
    ['a dev build used recently, from another dev build', '2.2.6-dev+abc1234', '2.2.6-dev+def5678', recent, 'dev-in-use'],
    ['a dev build used recently, from a newer release', '2.2.6-dev+abc1234', '2.3.0', recent, 'dev-in-use'],
    ['a dev build left alone, from an older release', '2.2.6-dev+abc1234', '2.2.5', stale, 'take-over'],
    ['a dev build that cannot say', '2.2.6-dev+abc1234', '2.2.5', undefined, 'take-over'],
  ])('%s', (_label, serverVersion, cliVersion, ownClientIdleMs, expected) => {
    expect(versionTakeover({ serverVersion, cliVersion, ownClientIdleMs })).toBe(expected);
  });

  it('takes over a server running from its own directory, dev build in use or not', () => {
    expect(versionTakeover({ serverVersion: '2.2.6-dev+abc1234', cliVersion: '2.2.6-dev+def5678', ownClientIdleMs: 0, sameCheckout: true })).toBe('take-over');
    expect(versionTakeover({ serverVersion: '2.2.6', cliVersion: '2.2.5', sameCheckout: true })).toBe('take-over');
    expect(versionTakeover({ serverVersion: '2.2.5', cliVersion: '2.2.5', sameCheckout: true })).toBe('same');
  });

  it('never lets two CLIs in use take the server from each other in turn', () => {
    const versions = ['2.2.4', '2.2.5', '2.2.6-dev+abc1234', '2.2.6-dev+def5678', '2.3.0'];
    for (const a of versions) {
      for (const b of versions) {
        // Both run all the time, so whichever server is up was just used by its CLI.
        const aTakesFromB = versionTakeover({ serverVersion: b, cliVersion: a, ownClientIdleMs: 0 }) === 'take-over';
        const bTakesFromA = versionTakeover({ serverVersion: a, cliVersion: b, ownClientIdleMs: 0 }) === 'take-over';
        expect(aTakesFromB && bTakesFromA, `${a} and ${b}`).toBe(false);
      }
    }
  });
});

describe('syncServerVersion', () => {
  let killSpy;

  beforeEach(() => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    killSpy = vi.spyOn(process, 'kill').mockImplementation(() => {
      throw Object.assign(new Error('kill ESRCH'), { code: 'ESRCH' });
    });
  });

  it('starts this version in place of the server it replaced', async () => {
    writeControl({ pid: 50, port: 8765, token: 'secret', startedAt: 's', version: '0.0.1', autoStarted: true });
    global.fetch.mockImplementation((url) => {
      if (url.endsWith('/control/shutdown')) {
        fs.rmSync(path.join(storeDir, 'control.json'), { force: true });
        return Promise.resolve(jsonResponse(202, { ok: true, pid: 50 }));
      }
      if (!fs.existsSync(path.join(storeDir, 'control.json'))) {
        return Promise.reject(new TypeError('fetch failed'));
      }
      const { pid, version } = readControlFile(storeDir);
      return Promise.resolve(jsonResponse(200, { ok: true, pid, version, jobs: {} }));
    });
    killSpy.mockImplementation((pid) => {
      if (pid === 50) {
        throw Object.assign(new Error('kill ESRCH'), { code: 'ESRCH' });
      }
      return true;
    });
    spawnMock.mockImplementation(() => {
      writeControl({ pid: 51, port: 8765, token: 'secret', startedAt: 's', version: CLI_VERSION });
      return { unref: vi.fn() };
    });

    await expect(syncServerVersion(storeDir)).resolves.toBe('replaced');
    expect(spawnMock).toHaveBeenCalledTimes(1);
    expect(readControlFile(storeDir)).toMatchObject({ pid: 51, version: CLI_VERSION });
  });

  it('does nothing without a server', async () => {
    await expect(syncServerVersion(storeDir)).resolves.toBe('none');
    expect(global.fetch).not.toHaveBeenCalled();
    expect(spawnMock).not.toHaveBeenCalled();
  });

  it('renews the lease of a dev server on this version with a ping', async () => {
    writeControl({ pid: 60, port: 8765, token: 'secret', startedAt: 's', version: CLI_VERSION });
    global.fetch.mockResolvedValue(jsonResponse(200, { ok: true, pid: 60, version: CLI_VERSION }));
    await expect(syncServerVersion(storeDir)).resolves.toBe('current');
    const [url, init] = global.fetch.mock.calls[0];
    expect(url).toBe('http://127.0.0.1:8765/control/ping');
    expect(init.headers[CONTROL_CLIENT_VERSION_HEADER]).toBe(CLI_VERSION);
  });

  it('warns once per server when a command checks twice', async () => {
    writeControl({ pid: 70, port: 8765, token: 'secret', startedAt: 's', version: '999.0.0', autoStarted: true });
    global.fetch.mockImplementation((url) => Promise.resolve(url.endsWith('/control/ping')
      ? jsonResponse(200, { ok: true, pid: 70, version: '999.0.0', jobs: {} })
      : jsonResponse(200, { result: 'done' })));
    await syncServerVersion(storeDir);
    await expect(invoke(storeDir, { op: 'listChannels', args: {} })).resolves.toBe('done');
    expect(console.error).toHaveBeenCalledTimes(1);
    expect(global.fetch.mock.calls.filter(([url]) => url.endsWith('/control/ping'))).toHaveLength(1);
  });
});

describe('restartServer', () => {
  let alive;

  beforeEach(() => {
    alive = new Set();
    vi.spyOn(process, 'kill').mockImplementation((pid) => {
      if (!alive.has(pid)) {
        throw Object.assign(new Error('kill ESRCH'), { code: 'ESRCH' });
      }
      return true;
    });
  });

  function serveFromControlFile({ onShutdown } = {}) {
    global.fetch.mockImplementation((url) => {
      const control = readControlFile(storeDir);
      if (!control) {
        return Promise.reject(new TypeError('fetch failed'));
      }
      if (url.endsWith('/control/shutdown')) {
        onShutdown?.(control.pid);
        return Promise.resolve(jsonResponse(202, { ok: true, pid: control.pid }));
      }
      return Promise.resolve(jsonResponse(200, { ok: true, pid: control.pid, version: control.version, startedAt: 'now', jobs: {} }));
    });
  }

  function stopsCleanly(pid) {
    fs.rmSync(path.join(storeDir, 'control.json'), { force: true });
    alive.delete(pid);
  }

  it('stops the old server, then starts this version in its place', async () => {
    alive.add(80);
    writeControl({ pid: 80, port: 8765, token: 'secret', startedAt: 's', version: '2.2.5', autoStarted: true });
    serveFromControlFile({ onShutdown: stopsCleanly });
    spawnMock.mockImplementation(() => {
      expect(alive.has(80)).toBe(false);
      writeControl({ pid: 81, port: 8765, token: 'secret', startedAt: 's', version: CLI_VERSION });
      return { unref: vi.fn() };
    });

    const { previous, current } = await restartServer(storeDir);
    expect(previous).toMatchObject({ running: true, stopped: true, pid: 80, version: '2.2.5', method: 'control' });
    expect(current).toMatchObject({ pid: 81, version: CLI_VERSION });
    expect(spawnMock).toHaveBeenCalledTimes(1);
  });

  it('starts a server when none runs', async () => {
    serveFromControlFile();
    spawnMock.mockImplementation(() => {
      writeControl({ pid: 90, port: 8765, token: 'secret', startedAt: 's', version: CLI_VERSION });
      return { unref: vi.fn() };
    });
    const { previous, current } = await restartServer(storeDir);
    expect(previous.running).toBe(false);
    expect(current).toMatchObject({ pid: 90 });
  });

  it('lets a service manager bring its server back instead of starting a second one', async () => {
    alive.add(100);
    writeControl({ pid: 100, port: 8765, token: 'secret', startedAt: 's', version: '2.2.5', autoStarted: false, manager: 'launchd' });
    serveFromControlFile({
      onShutdown: (pid) => {
        stopsCleanly(pid);
        setTimeout(() => writeControl({ pid: 101, port: 8765, token: 'secret', startedAt: 's', version: '2.2.5', autoStarted: false, manager: 'launchd' }), 300);
      },
    });
    const { previous, current } = await restartServer(storeDir);
    expect(previous).toMatchObject({ pid: 100, manager: 'launchd' });
    expect(current).toMatchObject({ pid: 101 });
    expect(spawnMock).not.toHaveBeenCalled();
  });

  it('starts no second server when the service manager does not bring its own back', async () => {
    vi.useFakeTimers();
    try {
      alive.add(105);
      writeControl({ pid: 105, port: 8765, token: 'secret', startedAt: 's', version: '2.2.5', autoStarted: false, manager: 'systemd' });
      serveFromControlFile({ onShutdown: stopsCleanly });
      const done = restartServer(storeDir);
      await vi.advanceTimersByTimeAsync(15000);
      const { previous, current } = await done;
      expect(previous).toMatchObject({ stopped: true, manager: 'systemd' });
      expect(current).toBeNull();
      expect(spawnMock).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });

  it('starts nothing when the old server does not stop', async () => {
    vi.useFakeTimers();
    try {
      alive.add(110);
      writeControl({ pid: 110, port: 8765, token: 'secret', startedAt: 's', version: '2.2.5', autoStarted: true });
      serveFromControlFile();
      const done = restartServer(storeDir, { timeoutMs: 1000 });
      await vi.advanceTimersByTimeAsync(5000);
      const { previous, current } = await done;
      expect(previous).toMatchObject({ running: true, stopped: false, pid: 110 });
      expect(current).toBeNull();
      expect(spawnMock).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });
});
