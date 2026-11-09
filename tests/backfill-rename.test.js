// Tests for issue #30 (PR-1): naming/aliasing only.
//
// `backfill` is the canonical name for the whole `sync` command tree, with
// `sync` kept as a silent alias. `channels watch`/`channels unwatch` are the
// canonical per-chat archive subscription commands, delegating to the same
// handler as the legacy (now hidden) `channels sync --enable/--disable`.
//
// These tests drive the real commander surface from buildProgram() with all
// service/lock/store seams stubbed, so there is no network, filesystem, or auth
// side effect. They assert that the new names resolve to the SAME handlers and
// behavior as the old names.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const services = vi.hoisted(() => ({ current: null }));
const control = vi.hoisted(() => ({ ensureServer: null, invoke: null, enqueueBackfill: null }));

vi.mock('../core/services.js', () => ({
  createServices: vi.fn(() => services.current),
  // withCommand builds only the needed half via these granular factories.
  createTelegramClient: vi.fn(() => ({ telegramClient: services.current.telegramClient })),
  createMessageSyncService: vi.fn(() => ({ messageSyncService: services.current.messageSyncService })),
  // Archive-only commands validate config via withCommand; stub it as a no-op.
  resolveValidatedConfig: vi.fn(() => ({})),
}));

// Commands routed through the warm server reach it via ensureServer + invoke;
// stub both so `channels watch/unwatch` resolve to a server invoke without any
// real process or network.
class ServerUnavailableError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ServerUnavailableError';
  }
}

vi.mock('../core/control-client.js', () => ({
  ensureServer: (...args) => control.ensureServer(...args),
  invoke: (...args) => control.invoke(...args),
  pingServer: vi.fn(),
  syncServerVersion: vi.fn(async () => 'current'),
  enqueueBackfill: (...args) => control.enqueueBackfill(...args),
  cancelBackfill: vi.fn(),
  retryBackfill: vi.fn(),
  ServerUnavailableError,
}));

vi.mock('../store-lock.js', () => ({
  acquireStoreLock: vi.fn(() => () => {}),
  acquireReadLock: vi.fn(() => () => {}),
  readStoreLock: vi.fn(() => ({ exists: false })),
}));

vi.mock('../core/store.js', () => ({
  resolveStoreDir: vi.fn(() => '/tmp/tgcli-test-store'),
}));

const { buildProgram } = await import('../cli.js');

function makeFakeServices(overrides = {}) {
  const telegramClient = {
    isAuthorized: vi.fn().mockResolvedValue(true),
    destroy: vi.fn().mockResolvedValue(undefined),
  };
  const messageSyncService = {
    getQueueStats: vi.fn(() => ({
      pending: 0,
      in_progress: 0,
      idle: 0,
      error: 0,
      processing: false,
    })),
    listJobs: vi.fn(() => []),
    listRepairs: vi.fn(() => []),
    addJob: vi.fn((chat) => ({ id: 1, channel_id: chat, status: 'pending' })),
    setChannelSync: vi.fn((chat, enabled) => ({
      channel_id: chat,
      sync_enabled: enabled ? 1 : 0,
    })),
    processQueue: vi.fn(),
    shutdown: vi.fn().mockResolvedValue(undefined),
    close: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  };
  return { telegramClient, messageSyncService };
}

async function runProgram(argv) {
  const program = buildProgram();
  program.exitOverride();
  return program.parseAsync(['node', 'cli.js', ...argv]);
}

describe('backfill is a canonical alias of sync', () => {
  let logSpy;

  beforeEach(() => {
    logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    control.ensureServer = vi.fn().mockResolvedValue({ ok: true });
    control.enqueueBackfill = vi.fn(async (_storeDir, { chatId }) => ({
      jobId: 1,
      channelId: chatId,
      status: 'pending',
    }));
  });

  afterEach(() => {
    logSpy.mockRestore();
    services.current = null;
    vi.clearAllMocks();
  });

  it('`backfill status` invokes the same handler as `sync status`', async () => {
    services.current = makeFakeServices();
    await runProgram(['backfill', 'status']);
    expect(services.current.messageSyncService.getQueueStats).toHaveBeenCalledTimes(1);

    // Same behavior under the `sync` alias.
    services.current = makeFakeServices();
    await runProgram(['sync', 'status']);
    expect(services.current.messageSyncService.getQueueStats).toHaveBeenCalledTimes(1);
  });

  it('`backfill status` and `sync status` produce identical output', async () => {
    services.current = makeFakeServices();
    await runProgram(['backfill', 'status']);
    const backfillOut = logSpy.mock.calls.map((c) => c.join(' '));

    logSpy.mockClear();
    services.current = makeFakeServices();
    await runProgram(['sync', 'status']);
    const syncOut = logSpy.mock.calls.map((c) => c.join(' '));

    expect(backfillOut).toEqual(syncOut);
    expect(backfillOut.join('\n')).toContain('QUEUE:');
  });

  it('`backfill jobs add` resolves the nested subcommand and queues a job on the server (like `sync jobs add`)', async () => {
    services.current = makeFakeServices();
    await runProgram(['backfill', 'jobs', 'add', '--chat', '@chan']);
    expect(control.ensureServer).toHaveBeenCalledWith('/tmp/tgcli-test-store', { quiet: false });
    expect(control.enqueueBackfill).toHaveBeenCalledWith('/tmp/tgcli-test-store', {
      chatId: '@chan',
      depth: null,
      minDate: null,
    });
    // No in-process job/queue work.
    expect(services.current.messageSyncService.addJob).not.toHaveBeenCalled();
    expect(services.current.messageSyncService.processQueue).not.toHaveBeenCalled();

    control.enqueueBackfill.mockClear();
    services.current = makeFakeServices();
    await runProgram(['sync', 'jobs', 'add', '--chat', '@chan']);
    expect(control.enqueueBackfill).toHaveBeenCalledWith('/tmp/tgcli-test-store', {
      chatId: '@chan',
      depth: null,
      minDate: null,
    });
  });
});

describe('channels watch / unwatch', () => {
  let logSpy;

  beforeEach(() => {
    logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    control.ensureServer = vi.fn().mockResolvedValue({ ok: true });
    // The server runs channelsWatch / channelsUnwatch / channelsSync; model their
    // result shape here.
    control.invoke = vi.fn(async (_storeDir, { op, args }) => {
      const enable = op === 'channelsWatch' || (op === 'channelsSync' && Boolean(args.enable));
      return {
        channelId: args.channelId,
        syncEnabled: enable,
        jobId: enable ? 1 : null,
        jobStatus: enable ? 'pending' : null,
        jobQueued: enable,
      };
    });
  });

  afterEach(() => {
    logSpy.mockRestore();
    services.current = null;
    vi.clearAllMocks();
  });

  it('`channels watch --chat X` routes channelsWatch to the server', async () => {
    await runProgram(['channels', 'watch', '--chat', '@chan']);
    // The server is reachable, so the op invokes directly without a spawn.
    expect(control.ensureServer).not.toHaveBeenCalled();
    expect(control.invoke).toHaveBeenCalledWith('/tmp/tgcli-test-store', {
      op: 'channelsWatch',
      args: { channelId: '@chan' },
      timeoutMs: undefined,
      quiet: false,
    });
  });

  it('`channels unwatch --chat X` routes channelsUnwatch to the server', async () => {
    await runProgram(['channels', 'unwatch', '--chat', '@chan']);
    expect(control.invoke).toHaveBeenCalledWith('/tmp/tgcli-test-store', {
      op: 'channelsUnwatch',
      args: { channelId: '@chan' },
      timeoutMs: undefined,
      quiet: false,
    });
  });

  it('`channels unwatch --missing` routes channelsUnwatch with missing to the server', async () => {
    control.invoke.mockResolvedValueOnce({ channelIds: ['-1000000000820'] });
    await runProgram(['channels', 'unwatch', '--missing']);
    expect(control.invoke).toHaveBeenCalledWith('/tmp/tgcli-test-store', {
      op: 'channelsUnwatch',
      args: { missing: true },
      timeoutMs: undefined,
      quiet: false,
    });
  });

  it('`channels watch` prints what legacy `channels sync --enable` prints', async () => {
    await runProgram(['channels', 'watch', '--chat', '@chan']);
    const watchOut = logSpy.mock.calls.map((c) => c.join(' '));

    logSpy.mockClear();
    await runProgram(['channels', 'sync', '--chat', '@chan', '--enable']);
    const enableOut = logSpy.mock.calls.map((c) => c.join(' '));

    expect(watchOut).toEqual(enableOut);
    expect(watchOut.join('\n')).toContain('Sync enabled for @chan');
  });

  it('legacy hidden `channels sync --enable/--disable` still routes to the server', async () => {
    await runProgram(['channels', 'sync', '--chat', '@chan', '--enable']);
    expect(control.invoke).toHaveBeenCalledWith('/tmp/tgcli-test-store', {
      op: 'channelsSync',
      args: { channelId: '@chan', enable: true },
      timeoutMs: undefined,
      quiet: false,
    });

    control.invoke.mockClear();
    await runProgram(['channels', 'sync', '--chat', '@chan', '--disable']);
    expect(control.invoke).toHaveBeenCalledWith('/tmp/tgcli-test-store', {
      op: 'channelsSync',
      args: { channelId: '@chan', disable: true },
      timeoutMs: undefined,
      quiet: false,
    });
  });
});
