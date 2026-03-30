import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const {
  acquireStoreLockMock,
  createTelegramClientMock,
  createMessageSyncServiceMock,
  loadConfigMock,
  normalizeConfigMock,
  resolveStoreDirMock,
  validateConfigMock,
} = vi.hoisted(() => ({
  acquireStoreLockMock: vi.fn(),
  createTelegramClientMock: vi.fn(),
  createMessageSyncServiceMock: vi.fn(),
  loadConfigMock: vi.fn(),
  normalizeConfigMock: vi.fn(),
  resolveStoreDirMock: vi.fn(),
  validateConfigMock: vi.fn(),
}));

vi.mock('../store-lock.js', () => ({
  acquireStoreLock: acquireStoreLockMock,
  acquireReadLock: vi.fn(),
  readStoreLock: vi.fn(),
}));

vi.mock('../core/config.js', () => ({
  loadConfig: loadConfigMock,
  normalizeConfig: normalizeConfigMock,
  saveConfig: vi.fn(),
  validateConfig: validateConfigMock,
}));

vi.mock('../core/services.js', () => ({
  createMessageSyncService: createMessageSyncServiceMock,
  createServices: vi.fn(),
  createTelegramClient: createTelegramClientMock,
}));

vi.mock('../core/store.js', () => ({
  resolveStoreDir: resolveStoreDirMock,
}));

import { isCliEntrypoint, runAuthLogin } from '../cli.js';

describe('cli auth command', () => {
  let logSpy;

  beforeEach(() => {
    logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    resolveStoreDirMock.mockReturnValue('/tmp/tgcli-store');
    loadConfigMock.mockReturnValue({
      config: {
        apiId: '12345',
        apiHash: 'hash',
        phoneNumber: '+1234567890',
      },
    });
    normalizeConfigMock.mockImplementation((config) => config);
    validateConfigMock.mockReturnValue([]);
    acquireStoreLockMock.mockReturnValue(vi.fn());
    createMessageSyncServiceMock.mockReset();
    createTelegramClientMock.mockReset();
  });

  afterEach(() => {
    logSpy.mockRestore();
  });

  it('completes auth without bootstrapping archive sync by default', async () => {
    const destroy = vi.fn().mockResolvedValue(undefined);
    const login = vi.fn().mockResolvedValue(true);
    createTelegramClientMock.mockReturnValue({
      telegramClient: {
        destroy,
        login,
      },
    });

    await runAuthLogin({ json: false, timeoutMs: null }, {});

    expect(login).toHaveBeenCalledTimes(1);
    expect(createMessageSyncServiceMock).not.toHaveBeenCalled();
    expect(logSpy).toHaveBeenCalledWith(
      'Authenticated. Run `tgcli sync --once` or `tgcli sync --follow` when you need archive data.',
    );
    expect(destroy).toHaveBeenCalledTimes(1);
  });

  it('treats symlinked tgcli binaries as the cli entrypoint', () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-cli-entrypoint-'));
    const symlinkPath = path.join(tmpDir, 'tgcli');

    try {
      fs.symlinkSync(path.join(process.cwd(), 'cli.js'), symlinkPath);
      expect(isCliEntrypoint(symlinkPath)).toBe(true);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
