import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  createControlRequestHandler,
  CONTROL_CLIENT_VERSION_HEADER,
  CONTROL_TOKEN_HEADER,
  removeControlFile,
  writeControlFile,
} from '../core/control-server.js';

const TOKEN = 'test-token-abc123';

// '@chan' is the one username Telegram knows here.
async function resolveChatKey(chat) {
  if (/^-?\d+$/.test(String(chat))) {
    return String(chat);
  }
  if (chat === '@chan') {
    return '9001';
  }
  const error = new Error(`Telegram has no chat ${chat}.`);
  error.code = 'CHAT_NOT_FOUND';
  throw error;
}

function makeService(overrides = {}) {
  return {
    // Nothing local knows a username unless a test says so.
    findChatKeyLocally: vi.fn(async () => null),
    resolveChatKey: vi.fn(resolveChatKey),
    getJobCounts: vi.fn(() => ({ pending: 1, inProgress: 2 })),
    addJob: vi.fn((chatId, opts) => ({
      id: 42,
      channel_id: String(chatId),
      status: 'pending',
      ...opts,
    })),
    processQueue: vi.fn(() => Promise.resolve()),
    cancelJobs: vi.fn(() => ({ canceled: 1, jobIds: [42] })),
    retryJobs: vi.fn(() => ({ updated: 1, jobIds: [42] })),
    addRepairs: vi.fn(({ chats, all }) => (all ? ['-100', '-200'] : chats)),
    ...overrides,
  };
}

function startServer(handler) {
  const server = http.createServer((req, res) => {
    void handler(req, res);
  });
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      resolve({ server, port });
    });
  });
}

async function request(port, method, pathname, { token, body } = {}) {
  const headers = { 'Content-Type': 'application/json' };
  if (token !== undefined) {
    headers[CONTROL_TOKEN_HEADER] = token;
  }
  const res = await fetch(`http://127.0.0.1:${port}${pathname}`, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  let json = null;
  try {
    json = await res.json();
  } catch {
    json = null;
  }
  return { status: res.status, json };
}

describe('control API request handler', () => {
  let service;
  let onActivity;
  let stats;
  let server;
  let port;

  beforeEach(async () => {
    service = makeService();
    onActivity = vi.fn();
    stats = { snapshot: vi.fn(() => ({ at: 'now', sinceStart: { requests: { total: 3 } }, lastHour: { requests: { total: 1 } } })) };
    const handler = createControlRequestHandler({
      service,
      token: TOKEN,
      pid: 12345,
      version: '9.9.9',
      startedAt: '2026-06-02T00:00:00.000Z',
      onActivity,
      ensureLogin: vi.fn(() => Promise.resolve()),
      stats,
    });
    ({ server, port } = await startServer(handler));
  });

  afterEach(() => {
    server.close();
  });

  it('GET /control/ping returns ok with job counts', async () => {
    const { status, json } = await request(port, 'GET', '/control/ping', { token: TOKEN });
    expect(status).toBe(200);
    expect(json).toMatchObject({
      ok: true,
      pid: 12345,
      version: '9.9.9',
      startedAt: '2026-06-02T00:00:00.000Z',
      jobs: { pending: 1, inProgress: 2 },
    });
    expect(onActivity).toHaveBeenCalledTimes(1);
  });

  it('GET /control/stats returns the counters with who is counting', async () => {
    const { status, json } = await request(port, 'GET', '/control/stats', { token: TOKEN });
    expect(status).toBe(200);
    expect(json).toEqual({
      pid: 12345,
      version: '9.9.9',
      startedAt: '2026-06-02T00:00:00.000Z',
      at: 'now',
      sinceStart: { requests: { total: 3 } },
      lastHour: { requests: { total: 1 } },
    });
  });

  it('GET /control/stats requires the token', async () => {
    const { status } = await request(port, 'GET', '/control/stats', { token: 'wrong' });
    expect(status).toBe(401);
    expect(stats.snapshot).not.toHaveBeenCalled();
  });

  it('GET /control/stats does not keep an idle server up', async () => {
    await request(port, 'GET', '/control/stats', { token: TOKEN });
    expect(onActivity).not.toHaveBeenCalled();
  });

  it('GET /control/stats answers 500 when the counters fail, and the server keeps serving', async () => {
    stats.snapshot.mockImplementationOnce(() => {
      throw new Error('broken');
    });
    const failed = await request(port, 'GET', '/control/stats', { token: TOKEN });
    expect(failed).toEqual({ status: 500, json: { error: 'broken' } });
    const { status } = await request(port, 'GET', '/control/stats', { token: TOKEN });
    expect(status).toBe(200);
  });

  it('GET /control/stats is not found on a server that keeps no counters', async () => {
    const handler = createControlRequestHandler({ service, token: TOKEN, pid: 1, version: '9.9.9', startedAt: 's' });
    const bare = await startServer(handler);
    try {
      const { status } = await request(bare.port, 'GET', '/control/stats', { token: TOKEN });
      expect(status).toBe(404);
    } finally {
      bare.server.close();
    }
  });

  it('POST /control/backfill enqueues via addJob and kicks the queue', async () => {
    const { status, json } = await request(port, 'POST', '/control/backfill', {
      token: TOKEN,
      body: { chatId: '777', depth: 500, minDate: '2026-01-01T00:00:00.000Z' },
    });
    expect(status).toBe(200);
    expect(service.addJob).toHaveBeenCalledWith('777', {
      depth: 500,
      minDate: '2026-01-01T00:00:00.000Z',
    });
    expect(service.processQueue).toHaveBeenCalledTimes(1);
    expect(json).toEqual({ jobId: 42, channelId: '777', status: 'pending' });
  });

  it('POST /control/backfill queues a chat given by username under its Telegram id', async () => {
    const { status, json } = await request(port, 'POST', '/control/backfill', {
      token: TOKEN,
      body: { chatId: '@chan', depth: 50 },
    });
    expect(status).toBe(200);
    expect(service.addJob).toHaveBeenCalledWith('9001', { depth: 50, minDate: undefined });
    expect(json).toEqual({ jobId: 42, channelId: '9001', status: 'pending' });
  });

  it('POST /control/backfill answers 400 for a username Telegram does not know, and queues nothing', async () => {
    const { status, json } = await request(port, 'POST', '/control/backfill', {
      token: TOKEN,
      body: { chatId: '@nobody' },
    });
    expect(status).toBe(400);
    expect(json).toEqual({ error: 'Telegram has no chat @nobody.' });
    expect(service.addJob).not.toHaveBeenCalled();
    expect(service.processQueue).not.toHaveBeenCalled();
  });

  it('POST /control/cancel and /control/retry find a chat given by username under its id', async () => {
    await request(port, 'POST', '/control/cancel', { token: TOKEN, body: { chatId: '@chan' } });
    expect(service.cancelJobs).toHaveBeenCalledWith({ channelId: '9001', jobId: undefined });
    await request(port, 'POST', '/control/retry', { token: TOKEN, body: { channelId: '@chan' } });
    expect(service.retryJobs).toHaveBeenCalledWith({ jobId: undefined, channelId: '9001', allErrors: false });
  });

  it('POST /control/cancel finds a username the archive knows without asking Telegram', async () => {
    service.findChatKeyLocally.mockResolvedValueOnce('9002');

    await request(port, 'POST', '/control/cancel', { token: TOKEN, body: { chatId: '@known' } });

    expect(service.cancelJobs).toHaveBeenCalledWith({ channelId: '9002', jobId: undefined });
    expect(service.resolveChatKey).not.toHaveBeenCalled();
  });

  // What is left under a username Telegram no longer has can still be canceled.
  it('POST /control/cancel falls back to the username itself when Telegram does not know it', async () => {
    const { status } = await request(port, 'POST', '/control/cancel', { token: TOKEN, body: { chatId: '@nobody' } });
    expect(status).toBe(200);
    expect(service.cancelJobs).toHaveBeenCalledWith({ channelId: '@nobody', jobId: undefined });
  });

  it('POST /control/repair looks a chat given by username up under its id', async () => {
    const { status } = await request(port, 'POST', '/control/repair', {
      token: TOKEN,
      body: { chats: ['@chan', '@nobody'] },
    });
    expect(status).toBe(200);
    expect(service.addRepairs).toHaveBeenCalledWith(expect.objectContaining({ chats: ['9001', '@nobody'] }));
  });

  it('POST /control/cancel looks the username up as it is when Telegram does not answer', async () => {
    service.resolveChatKey.mockRejectedValueOnce(new Error('Connection closed'));
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
    const { status } = await request(port, 'POST', '/control/cancel', { token: TOKEN, body: { chatId: '@chan' } });
    logged.mockRestore();
    expect(status).toBe(200);
    expect(service.cancelJobs).toHaveBeenCalledWith({ channelId: '@chan', jobId: undefined });
  });

  it('POST /control/backfill requires chatId', async () => {
    const { status } = await request(port, 'POST', '/control/backfill', {
      token: TOKEN,
      body: {},
    });
    expect(status).toBe(400);
    expect(service.addJob).not.toHaveBeenCalled();
  });

  it('POST /control/repair queues repairs and kicks the queue', async () => {
    const { status, json } = await request(port, 'POST', '/control/repair', {
      token: TOKEN,
      body: { chats: ['-100'], afterId: 0, sinceDate: 1700000000 },
    });
    expect(status).toBe(200);
    expect(service.addRepairs).toHaveBeenCalledWith({ chats: ['-100'], all: false, afterId: 0, sinceDate: 1700000000, dryRun: false });
    expect(service.processQueue).toHaveBeenCalledTimes(1);
    expect(json).toEqual({ channelIds: ['-100'] });
  });

  it('POST /control/repair takes all chats with sync on', async () => {
    const { status, json } = await request(port, 'POST', '/control/repair', {
      token: TOKEN,
      body: { all: true, sinceDate: 1700000000 },
    });
    expect(status).toBe(200);
    expect(service.addRepairs).toHaveBeenCalledWith({ chats: [], all: true, afterId: undefined, sinceDate: 1700000000, dryRun: false });
    expect(json).toEqual({ channelIds: ['-100', '-200'] });
  });

  // An older server answers 404 on this path instead of ignoring a flag and writing.
  it('POST /control/repair/dry-run queues a dry run', async () => {
    const { status } = await request(port, 'POST', '/control/repair/dry-run', {
      token: TOKEN,
      body: { all: true, sinceDate: 1700000000 },
    });
    expect(status).toBe(200);
    expect(service.addRepairs).toHaveBeenCalledWith({ chats: [], all: true, afterId: undefined, sinceDate: 1700000000, dryRun: true });
  });

  it('POST /control/repair requires chats or all', async () => {
    const { status } = await request(port, 'POST', '/control/repair', { token: TOKEN, body: { chats: [' '] } });
    expect(status).toBe(400);
    expect(service.addRepairs).not.toHaveBeenCalled();
  });

  it.each([
    [{ chats: ['-100'], sinceDate: '2026-01-01' }],
    [{ chats: ['-100'], afterId: -1 }],
    [{ all: true, sinceDate: 1.5 }],
  ])('POST /control/repair refuses bounds that are not whole numbers: %j', async (body) => {
    const { status } = await request(port, 'POST', '/control/repair', { token: TOKEN, body });
    expect(status).toBe(400);
    expect(service.addRepairs).not.toHaveBeenCalled();
  });

  it('POST /control/repair answers 400 with the reason a chat cannot be repaired', async () => {
    service.addRepairs.mockImplementation(() => {
      throw new Error('Chat -999 is not in the archive.');
    });
    const { status, json } = await request(port, 'POST', '/control/repair', { token: TOKEN, body: { chats: ['-999'] } });
    expect(status).toBe(400);
    expect(json).toEqual({ error: 'Chat -999 is not in the archive.' });
    expect(service.processQueue).not.toHaveBeenCalled();
  });

  it('POST /control/cancel cancels via cancelJobs', async () => {
    const { status, json } = await request(port, 'POST', '/control/cancel', {
      token: TOKEN,
      body: { chatId: '777' },
    });
    expect(status).toBe(200);
    expect(service.cancelJobs).toHaveBeenCalledWith({ channelId: '777', jobId: undefined });
    expect(json).toEqual({ canceled: 1, jobIds: [42] });
  });

  it('POST /control/cancel accepts jobId', async () => {
    const { status } = await request(port, 'POST', '/control/cancel', {
      token: TOKEN,
      body: { jobId: 42 },
    });
    expect(status).toBe(200);
    expect(service.cancelJobs).toHaveBeenCalledWith({ channelId: undefined, jobId: 42 });
  });

  it('POST /control/cancel requires chatId or jobId', async () => {
    const { status } = await request(port, 'POST', '/control/cancel', {
      token: TOKEN,
      body: {},
    });
    expect(status).toBe(400);
    expect(service.cancelJobs).not.toHaveBeenCalled();
  });

  it('POST /control/retry resets via retryJobs and kicks the queue', async () => {
    const { status, json } = await request(port, 'POST', '/control/retry', {
      token: TOKEN,
      body: { channelId: '777' },
    });
    expect(status).toBe(200);
    expect(service.retryJobs).toHaveBeenCalledWith({
      jobId: undefined,
      channelId: '777',
      allErrors: false,
    });
    expect(service.processQueue).toHaveBeenCalledTimes(1);
    expect(json).toEqual({ updated: 1, jobIds: [42] });
  });

  it('POST /control/retry accepts allErrors', async () => {
    const { status } = await request(port, 'POST', '/control/retry', {
      token: TOKEN,
      body: { allErrors: true },
    });
    expect(status).toBe(200);
    expect(service.retryJobs).toHaveBeenCalledWith({
      jobId: undefined,
      channelId: undefined,
      allErrors: true,
    });
  });

  it('POST /control/retry does not kick the queue when nothing was reset', async () => {
    service.retryJobs.mockReturnValueOnce({ updated: 0, jobIds: [] });
    const { status } = await request(port, 'POST', '/control/retry', {
      token: TOKEN,
      body: { jobId: 99 },
    });
    expect(status).toBe(200);
    expect(service.processQueue).not.toHaveBeenCalled();
  });

  it('POST /control/retry requires jobId, channelId, or allErrors', async () => {
    const { status } = await request(port, 'POST', '/control/retry', {
      token: TOKEN,
      body: {},
    });
    expect(status).toBe(400);
    expect(service.retryJobs).not.toHaveBeenCalled();
  });

  it('rejects requests with a missing token', async () => {
    const { status, json } = await request(port, 'GET', '/control/ping');
    expect(status).toBe(401);
    expect(json).toEqual({ error: 'Unauthorized' });
    expect(service.getJobCounts).not.toHaveBeenCalled();
    expect(onActivity).not.toHaveBeenCalled();
  });

  it('rejects requests with a wrong token', async () => {
    const { status } = await request(port, 'GET', '/control/ping', { token: 'nope' });
    expect(status).toBe(401);
    expect(service.getJobCounts).not.toHaveBeenCalled();
  });

  it('returns 404 for unknown control paths', async () => {
    const { status } = await request(port, 'GET', '/control/unknown', { token: TOKEN });
    expect(status).toBe(404);
  });

  it('returns 404 for non-control paths', async () => {
    const { status } = await request(port, 'GET', '/something-else', { token: TOKEN });
    expect(status).toBe(404);
  });
});

describe('POST /control/run', () => {
  let warmServices;
  let ensureLogin;
  let server;
  let port;

  beforeEach(async () => {
    warmServices = {
      telegramClient: {
        listDialogs: vi.fn(() => Promise.resolve([{ id: '1', title: 'Warm' }])),
        searchDialogs: vi.fn(() => Promise.resolve([])),
        getGroupInviteLink: vi.fn(() => Promise.resolve({ link: 'https://t.me/+warm' })),
      },
      messageSyncService: {
        resolveChatKey: vi.fn(async () => '1001'),
        setChannelSync: vi.fn(() => ({ channel_id: '1001', sync_enabled: 1 })),
        queueResyncJob: vi.fn(() => ({ job: { id: 5, status: 'pending' }, queued: true })),
      },
    };
    ensureLogin = vi.fn(() => Promise.resolve());
    const handler = createControlRequestHandler({
      service: makeService(),
      warmServices,
      token: TOKEN,
      pid: 1,
      version: '1',
      startedAt: 's',
      ensureLogin,
    });
    ({ server, port } = await startServer(handler));
  });

  afterEach(() => {
    server.close();
    vi.restoreAllMocks();
  });

  it('runs a known op against the warm services and returns its result', async () => {
    const { status, json } = await request(port, 'POST', '/control/run', {
      token: TOKEN,
      body: { op: 'listChannels', args: { limit: 10 } },
    });
    expect(status).toBe(200);
    expect(warmServices.telegramClient.listDialogs).toHaveBeenCalledWith(10);
    expect(json).toEqual({ result: [{ id: '1', title: 'Warm' }] });
    expect(ensureLogin).toHaveBeenCalledTimes(1);
  });

  it('refuses the operations of a CLI older than this server, running nothing', async () => {
    const { status, json } = await request(port, 'POST', '/control/invoke', {
      token: TOKEN,
      body: { op: 'foldersEdit', args: { folder: '1', modification: { title: 'X' } } },
    });
    expect(status).toBe(400);
    expect(json.error).toMatch(/newer than this CLI; update tgcli, or run `tgcli server restart`/);
    expect(ensureLogin).not.toHaveBeenCalled();
  });

  it('applies the operation defaults to the args the caller left out', async () => {
    const { status } = await request(port, 'POST', '/control/run', {
      token: TOKEN,
      body: { op: 'listChannels', args: {} },
    });
    expect(status).toBe(200);
    expect(warmServices.telegramClient.listDialogs).toHaveBeenCalledWith(50);
  });

  it('returns 400 naming the arg when the args do not fit the operation (no handler invoked)', async () => {
    const { status, json } = await request(port, 'POST', '/control/run', {
      token: TOKEN,
      body: { op: 'listChannels', args: { limit: 'ten' } },
    });
    expect(status).toBe(400);
    expect(json.error).toBe('limit must be a positive integer');
    expect(warmServices.telegramClient.listDialogs).not.toHaveBeenCalled();
    expect(ensureLogin).not.toHaveBeenCalled();
  });

  it('dispatches a write op (channelsWatch) against the warm services', async () => {
    const { status, json } = await request(port, 'POST', '/control/run', {
      token: TOKEN,
      body: { op: 'channelsWatch', args: { channelId: '@g' } },
    });
    expect(status).toBe(200);
    expect(warmServices.messageSyncService.resolveChatKey).toHaveBeenCalledWith('@g');
    expect(warmServices.messageSyncService.setChannelSync).toHaveBeenCalledWith('1001', true);
    expect(warmServices.messageSyncService.queueResyncJob).toHaveBeenCalledWith('1001');
    expect(json.result).toMatchObject({ channelId: '1001', syncEnabled: true, jobId: 5, jobQueued: true });
    expect(ensureLogin).toHaveBeenCalledTimes(1);
  });

  it('returns 500 with a sendError payload when a send op fails', async () => {
    warmServices.telegramClient.sendTextMessage = vi.fn(() => Promise.reject(new Error('FLOOD_WAIT_120')));
    const { status, json } = await request(port, 'POST', '/control/run', {
      token: TOKEN,
      body: { op: 'messagesSend', args: { channelId: '@g', text: 'hi', retries: 0, timeoutMs: 30000 } },
    });
    expect(status).toBe(500);
    expect(json.sendError).toMatchObject({ type: 'rate_limit', method: 'sendText', waitSeconds: 120 });
  });

  it('returns 400 for an unknown op (no handler invoked)', async () => {
    const { status, json } = await request(port, 'POST', '/control/run', {
      token: TOKEN,
      body: { op: 'doesNotExist', args: {} },
    });
    expect(status).toBe(400);
    expect(json.error).toMatch(/Unknown operation/);
    expect(ensureLogin).not.toHaveBeenCalled();
  });

  it('rejects an invoke without a token', async () => {
    const { status } = await request(port, 'POST', '/control/run', {
      body: { op: 'listChannels', args: {} },
    });
    expect(status).toBe(401);
    expect(warmServices.telegramClient.listDialogs).not.toHaveBeenCalled();
  });
});

describe('control API shutdown and client versions', () => {
  let server;
  let port;
  let clock;
  let onShutdown;

  async function start(overrides = {}) {
    clock = 1_000_000;
    onShutdown = 'onShutdown' in overrides ? overrides.onShutdown : vi.fn();
    const handler = createControlRequestHandler({
      service: makeService(),
      token: TOKEN,
      pid: 12345,
      version: '2.2.6-dev+abc1234',
      startedAt: 's',
      now: () => clock,
      ...overrides,
      onShutdown,
    });
    ({ server, port } = await startServer(handler));
  }

  afterEach(() => {
    server.close();
  });

  function ping(clientVersion) {
    return fetch(`http://127.0.0.1:${port}/control/ping`, {
      headers: {
        [CONTROL_TOKEN_HEADER]: TOKEN,
        ...(clientVersion ? { [CONTROL_CLIENT_VERSION_HEADER]: clientVersion } : {}),
      },
    }).then((res) => res.json());
  }

  it('POST /control/shutdown replies before it starts the shutdown', async () => {
    // Like the real one, the shutdown drops every open connection.
    await start({ onShutdown: vi.fn(() => server.closeAllConnections()) });
    const { status, json } = await request(port, 'POST', '/control/shutdown', { token: TOKEN });
    expect(status).toBe(202);
    expect(json).toEqual({ ok: true, pid: 12345 });
    await vi.waitFor(() => expect(onShutdown).toHaveBeenCalledTimes(1));
  });

  it('POST /control/shutdown needs the token', async () => {
    await start();
    const { status } = await request(port, 'POST', '/control/shutdown');
    expect(status).toBe(401);
    expect(onShutdown).not.toHaveBeenCalled();
  });

  it('answers 501 when the server has no shutdown hook', async () => {
    await start({ onShutdown: undefined });
    const { status } = await request(port, 'POST', '/control/shutdown', { token: TOKEN });
    expect(status).toBe(501);
  });

  it('times how long no CLI of its own version has called, from the start', async () => {
    await start();
    clock += 5000;
    expect((await ping('2.2.5')).ownClientIdleMs).toBe(5000);
    expect((await ping()).ownClientIdleMs).toBe(5000);
    expect((await ping('2.2.6-dev+abc1234')).ownClientIdleMs).toBe(0);
    clock += 700;
    expect((await ping('2.2.5')).ownClientIdleMs).toBe(700);
  });

  it('counts a stats read by a CLI of its own version as that CLI being around, not as activity', async () => {
    const onActivity = vi.fn();
    await start({ onActivity, stats: { snapshot: () => ({}) } });
    clock += 5000;
    const res = await fetch(`http://127.0.0.1:${port}/control/stats`, {
      headers: { [CONTROL_TOKEN_HEADER]: TOKEN, [CONTROL_CLIENT_VERSION_HEADER]: '2.2.6-dev+abc1234' },
    });
    expect(res.status).toBe(200);
    expect(onActivity).not.toHaveBeenCalled();
    expect((await ping('2.2.5')).ownClientIdleMs).toBe(0);
  });
});

describe('control API requests in flight', () => {
  it('counts the requests it is still answering in the ping, and reports where it runs from', async () => {
    let finish;
    const warmServices = {
      telegramClient: { listDialogs: vi.fn(() => new Promise((resolve) => { finish = () => resolve([]); })) },
    };
    const handler = createControlRequestHandler({
      service: makeService(),
      warmServices,
      token: TOKEN,
      pid: 1,
      version: '1.0.0',
      startedAt: 's',
      root: '/opt/tgcli',
    });
    const { server, port } = await startServer(handler);
    try {
      const slow = request(port, 'POST', '/control/run', { token: TOKEN, body: { op: 'listChannels', args: {} } });
      await vi.waitFor(() => expect(handler.inFlight()).toBe(1));
      const { json } = await request(port, 'GET', '/control/ping', { token: TOKEN });
      expect(json).toMatchObject({ inFlight: 1, root: '/opt/tgcli' });
      finish();
      await slow;
      expect(handler.inFlight()).toBe(0);
      expect((await request(port, 'GET', '/control/ping', { token: TOKEN })).json.inFlight).toBe(0);
    } finally {
      server.close();
    }
  });
});

describe('removeControlFile', () => {
  let storeDir;

  beforeEach(() => {
    storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-control-file-'));
  });

  afterEach(() => {
    fs.rmSync(storeDir, { recursive: true, force: true });
  });

  it("leaves the file of the server that replaced this one", () => {
    writeControlFile(storeDir, { pid: 2, token: 'new' });
    removeControlFile(storeDir, { pid: 1, token: 'old' });
    expect(JSON.parse(fs.readFileSync(path.join(storeDir, 'control.json'), 'utf8'))).toMatchObject({ pid: 2 });
  });

  it('removes its own file, and copes with none', () => {
    writeControlFile(storeDir, { pid: 1, token: 'old' });
    removeControlFile(storeDir, { pid: 1, token: 'old' });
    expect(fs.existsSync(path.join(storeDir, 'control.json'))).toBe(false);
    expect(() => removeControlFile(storeDir, { pid: 1, token: 'old' })).not.toThrow();
  });
});
