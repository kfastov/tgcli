// `tgcli doctor` reports the cached update state: an UPDATE line in text output and
// an `update` object in --json. Telegram, the archive and the install path are
// stubbed; the store is a temporary directory holding only the update cache.

import fs from 'fs';
import os from 'os';
import path from 'path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const install = vi.hoisted(() => ({ method: 'npm' }));

vi.mock('../core/services.js', () => ({
  createServices: vi.fn(),
  resolveValidatedConfig: vi.fn(),
  createTelegramClient: vi.fn(() => ({
    telegramClient: {
      isAuthorized: vi.fn().mockResolvedValue(false),
      destroy: vi.fn().mockResolvedValue(undefined),
    },
  })),
  createMessageSyncService: vi.fn(() => ({
    messageSyncService: {
      getSearchStatus: () => ({ enabled: true, version: 5 }),
      getQueueStats: () => ({ pending: 0, in_progress: 0, idle: 0, error: 0, gaps: 0 }),
      isRepairSuggested: () => false,
      getLeftChatCounts: () => ({ syncOff: 0, stillJoined: 0 }),
      listUsernameKeyedChats: () => [],
      close: vi.fn().mockResolvedValue(undefined),
    },
  })),
}));

vi.mock('../core/version.js', () => ({ readVersion: () => '2.2.0' }));

vi.mock('../core/update-check.js', async (importOriginal) => ({
  ...(await importOriginal()),
  detectInstallMethod: () => install.method,
}));

const { buildProgram } = await import('../cli.js');

let storeDir;
let stdout;

beforeEach(() => {
  storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-doctor-'));
  // doctor reads the switches from process.env; CI runners set CI=true.
  vi.stubEnv('TGCLI_STORE', storeDir);
  vi.stubEnv('CI', undefined);
  vi.stubEnv('NO_UPDATE_NOTIFIER', undefined);
  vi.stubEnv('TGCLI_NO_UPDATE_CHECK', undefined);
  install.method = 'npm';
  stdout = [];
  vi.spyOn(console, 'log').mockImplementation((...args) => { stdout.push(args.join(' ')); });
  vi.spyOn(process.stdout, 'write').mockImplementation((chunk) => { stdout.push(String(chunk)); return true; });
  vi.spyOn(process.stderr, 'write').mockImplementation(() => true);
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  process.exitCode = undefined;
  fs.rmSync(storeDir, { recursive: true, force: true });
});

async function doctor(...flags) {
  stdout.length = 0;
  const program = buildProgram();
  program.exitOverride();
  await program.parseAsync(['node', 'cli.js', ...flags, 'doctor']);
  expect(process.exitCode).toBeUndefined();
  return stdout.join('\n');
}

function writeCache(data) {
  fs.writeFileSync(path.join(storeDir, 'update-check.json'), JSON.stringify(data));
}

describe('doctor update report', () => {
  it('prints the UPDATE line with the upgrade command', async () => {
    writeCache({ checkedAt: Date.now(), latest: '2.2.5', latestAt: Date.now() });
    const out = await doctor();
    expect(out).toContain('UPDATE: 2.2.5 available (installed 2.2.0); run `npm i -g @kfastov/tgcli`');
  });

  it('puts the same facts under `update` in --json', async () => {
    const latestAt = Date.parse('2026-11-06T12:00:00Z');
    writeCache({ checkedAt: latestAt, latest: '2.2.5', latestAt });
    const payload = JSON.parse(await doctor('--json'));
    expect(payload.update).toEqual({
      current: '2.2.0',
      latest: '2.2.5',
      available: true,
      command: 'npm i -g @kfastov/tgcli',
      installMethod: 'npm',
      latestCheckedAt: '2026-11-06T12:00:00.000Z',
      lastError: null,
      disabledReason: null,
    });
  });

  it('says why there is no check', async () => {
    install.method = null;
    expect(await doctor()).toContain('UPDATE: no check (not installed with Homebrew or npm)');
    install.method = 'brew';
    vi.stubEnv('TGCLI_NO_UPDATE_CHECK', '1');
    expect(await doctor()).toContain('UPDATE: no check (TGCLI_NO_UPDATE_CHECK is set)');
    vi.stubEnv('TGCLI_NO_UPDATE_CHECK', undefined);
    vi.stubEnv('CI', 'true');
    expect(await doctor()).toContain('UPDATE: no check (CI)');
  });
});
