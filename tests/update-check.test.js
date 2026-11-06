import fs from 'fs';
import http from 'http';
import os from 'os';
import path from 'path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  UPDATE_CACHE_FILE,
  UPDATE_INTERVAL_MS,
  classifyInstallPath,
  detectInstallMethod,
  formatDoctorUpdateLine,
  getUpdateCheckDisabledReason,
  getUpdateStatus,
  isNewerVersion,
  maybePrintUpdateNotice,
  readUpdateCache,
  refreshUpdateCache,
  scheduleUpdateCheck,
} from '../core/update-check.js';
import { buildProgram, installUpdateNotice } from '../cli.js';

const HOUR = 60 * 60 * 1000;
const T0 = Date.parse('2026-11-06T12:00:00Z');
const NO_ENV = {};

let storeDir;

function writeCache(data) {
  fs.writeFileSync(path.join(storeDir, UPDATE_CACHE_FILE), JSON.stringify(data));
}

function fakeTty() {
  const lines = [];
  return { isTTY: true, write: vi.fn((chunk) => { lines.push(String(chunk)); return true; }), lines };
}

function fakeSpawn() {
  const child = { on: vi.fn(), unref: vi.fn() };
  const fn = vi.fn(() => child);
  fn.child = child;
  return fn;
}

function jsonResponse(body, status = 200) {
  return { ok: status >= 200 && status < 300, status, json: async () => body };
}

beforeEach(() => {
  storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-update-'));
});

afterEach(() => {
  fs.rmSync(storeDir, { recursive: true, force: true });
});

describe('isNewerVersion', () => {
  it('compares release versions numerically', () => {
    expect(isNewerVersion('2.3.0', '2.2.5')).toBe(true);
    expect(isNewerVersion('2.2.10', '2.2.9')).toBe(true);
    expect(isNewerVersion('10.0.0', '9.9.9')).toBe(true);
    expect(isNewerVersion('2.2.5', '2.2.5')).toBe(false);
    expect(isNewerVersion('2.2.4', '2.2.5')).toBe(false);
  });

  it('never offers a prerelease and never offers anything to one', () => {
    expect(isNewerVersion('2.3.0-beta.1', '2.2.5')).toBe(false);
    expect(isNewerVersion('2.3.0', '2.3.0-dev')).toBe(false);
    expect(isNewerVersion('garbage', '2.2.5')).toBe(false);
  });
});

describe('install method', () => {
  it('recognises a Homebrew keg', () => {
    expect(classifyInstallPath('/opt/homebrew/Cellar/tgcli/2.2.5/libexec/lib/node_modules/@kfastov/tgcli')).toBe('brew');
    expect(classifyInstallPath('/home/linuxbrew/.linuxbrew/Cellar/tgcli/2.2.5/libexec/lib/node_modules/@kfastov/tgcli')).toBe('brew');
  });

  it('recognises a global npm install', () => {
    expect(classifyInstallPath('/usr/local/lib/node_modules/@kfastov/tgcli')).toBe('npm');
    expect(classifyInstallPath('/Users/me/.nvm/versions/node/v22.0.0/lib/node_modules/@kfastov/tgcli/')).toBe('npm');
    expect(classifyInstallPath('C:\\Users\\me\\AppData\\Roaming\\npm\\node_modules\\@kfastov\\tgcli')).toBe('npm');
  });

  it('gives no method for an npx cache, a project dependency or the Nix store', () => {
    expect(classifyInstallPath('/Users/me/.npm/_npx/1a2b3c/node_modules/@kfastov/tgcli')).toBeNull();
    expect(classifyInstallPath('/Users/me/work/app/node_modules/@kfastov/tgcli')).toBeNull();
    expect(classifyInstallPath('/nix/store/abc-tgcli-2.2.5/lib/node_modules/@kfastov/tgcli')).toBeNull();
  });

  it('resolves symlinks, so a clone stays a clone even behind an npm-shaped link', () => {
    const clone = path.resolve(import.meta.dirname, '..');
    expect(detectInstallMethod(clone)).toBeNull();
    const linkDir = path.join(storeDir, 'lib', 'node_modules', '@kfastov');
    fs.mkdirSync(linkDir, { recursive: true });
    fs.symlinkSync(clone, path.join(linkDir, 'tgcli'));
    expect(detectInstallMethod(path.join(linkDir, 'tgcli'))).toBeNull();
  });
});

describe('getUpdateCheckDisabledReason', () => {
  const base = { version: '2.2.5', installMethod: 'brew' };

  it('is on for a released brew or npm install', () => {
    expect(getUpdateCheckDisabledReason({ ...base, env: NO_ENV })).toBeNull();
    expect(getUpdateCheckDisabledReason({ ...base, env: { TGCLI_NO_UPDATE_CHECK: '0', CI: 'false' } })).toBeNull();
  });

  it('is off with TGCLI_NO_UPDATE_CHECK, NO_UPDATE_NOTIFIER or in CI', () => {
    expect(getUpdateCheckDisabledReason({ ...base, env: { TGCLI_NO_UPDATE_CHECK: '1' } })).toMatch(/TGCLI_NO_UPDATE_CHECK/);
    expect(getUpdateCheckDisabledReason({ ...base, env: { NO_UPDATE_NOTIFIER: '1' } })).toMatch(/NO_UPDATE_NOTIFIER/);
    expect(getUpdateCheckDisabledReason({ ...base, env: { CI: 'true' } })).toBe('CI');
    expect(getUpdateCheckDisabledReason({ ...base, env: { TGCLI_NO_UPDATE_CHECK: '' } })).toBeNull();
  });

  it('is off for dev builds and copies with no known install method', () => {
    expect(getUpdateCheckDisabledReason({ ...base, env: NO_ENV, version: '2.3.0-dev' })).toBe('development build');
    expect(getUpdateCheckDisabledReason({ ...base, env: NO_ENV, version: '0.0.0' })).toBe('development build');
    expect(getUpdateCheckDisabledReason({ ...base, env: NO_ENV, installMethod: null })).toMatch(/not installed/);
  });
});

describe('scheduleUpdateCheck', () => {
  it('starts one detached check a day and records the attempt first', () => {
    const spawnFn = fakeSpawn();
    expect(scheduleUpdateCheck({ storeDir, now: T0, spawnFn })).toBe(true);
    expect(spawnFn).toHaveBeenCalledTimes(1);
    const [, args, options] = spawnFn.mock.calls[0];
    expect(args[0]).toMatch(/update-check-worker\.js$/);
    expect(args[1]).toBe(storeDir);
    expect(options).toMatchObject({ detached: true, stdio: 'ignore' });
    expect(spawnFn.child.unref).toHaveBeenCalled();
    expect(spawnFn.child.on).toHaveBeenCalledWith('error', expect.any(Function));
    expect(readUpdateCache(storeDir).checkedAt).toBe(T0);

    expect(scheduleUpdateCheck({ storeDir, now: T0 + 23 * HOUR, spawnFn })).toBe(false);
    expect(spawnFn).toHaveBeenCalledTimes(1);

    expect(scheduleUpdateCheck({ storeDir, now: T0 + UPDATE_INTERVAL_MS, spawnFn })).toBe(true);
    expect(spawnFn).toHaveBeenCalledTimes(2);
  });

  it('checks again when the last check is stamped in the future', () => {
    writeCache({ checkedAt: T0 + 48 * HOUR, latest: '2.2.5' });
    const spawnFn = fakeSpawn();
    expect(scheduleUpdateCheck({ storeDir, now: T0, spawnFn })).toBe(true);
  });

  it('does nothing before the store exists and never throws', () => {
    const spawnFn = fakeSpawn();
    expect(scheduleUpdateCheck({ storeDir: path.join(storeDir, 'missing'), now: T0, spawnFn })).toBe(false);
    const throwing = vi.fn(() => { throw new Error('spawn failed'); });
    expect(scheduleUpdateCheck({ storeDir, now: T0, spawnFn: throwing })).toBe(false);
    expect(spawnFn).not.toHaveBeenCalled();
  });

  it('runs the real worker in the background against a registry', async () => {
    let release;
    const answered = new Promise((resolve) => { release = resolve; });
    const server = http.createServer(async (req, res) => {
      await answered;
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ latest: '9.9.9' }));
    });
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    const url = `http://127.0.0.1:${server.address().port}/dist-tags`;
    try {
      expect(scheduleUpdateCheck({ storeDir, now: T0, url })).toBe(true);
      // The call returned while the registry still holds its answer.
      expect(readUpdateCache(storeDir)).toEqual({ checkedAt: T0 });
      release();
      const deadline = Date.now() + 10_000;
      while (!readUpdateCache(storeDir)?.latest && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      expect(readUpdateCache(storeDir)).toMatchObject({ latest: '9.9.9', error: null });
    } finally {
      server.closeAllConnections?.();
      await new Promise((resolve) => server.close(resolve));
    }
  }, 15_000);
});

describe('refreshUpdateCache', () => {
  it('stores the latest dist-tag', async () => {
    const fetchFn = vi.fn(async () => jsonResponse({ latest: '2.3.0', next: '2.4.0-rc.1' }));
    const result = await refreshUpdateCache({ storeDir, fetchFn, now: () => T0, url: 'https://registry.test/x' });
    expect(result).toEqual({ latest: '2.3.0', error: null });
    expect(fetchFn).toHaveBeenCalledWith('https://registry.test/x', expect.any(Object));
    expect(readUpdateCache(storeDir)).toMatchObject({ checkedAt: T0, latest: '2.3.0', latestAt: T0, error: null });
  });

  it('keeps the last known version when the registry fails', async () => {
    writeCache({ checkedAt: T0 - 25 * HOUR, latest: '2.3.0', latestAt: T0 - 25 * HOUR, notifiedVersion: '2.3.0' });
    await refreshUpdateCache({ storeDir, fetchFn: async () => jsonResponse({}, 503), now: () => T0 });
    expect(readUpdateCache(storeDir)).toMatchObject({
      checkedAt: T0,
      latest: '2.3.0',
      notifiedVersion: '2.3.0',
      error: 'registry answered HTTP 503',
    });

    await refreshUpdateCache({ storeDir, fetchFn: async () => jsonResponse({ latest: 'nope' }), now: () => T0 });
    expect(readUpdateCache(storeDir)).toMatchObject({ latest: '2.3.0', error: expect.stringMatching(/no valid latest/) });
  });

  it('gives up on a registry that does not answer', async () => {
    const hanging = (url, { signal }) => new Promise((resolve, reject) => {
      signal.addEventListener('abort', () => reject(signal.reason));
    });
    const result = await refreshUpdateCache({ storeDir, fetchFn: hanging, now: () => T0, timeoutMs: 20 });
    expect(result.latest).toBeNull();
    expect(readUpdateCache(storeDir).error).toBeTruthy();
  });
});

describe('maybePrintUpdateNotice', () => {
  const brew = { version: '2.2.5', installMethod: 'brew', env: NO_ENV };

  it('prints one stderr line with the brew command, once a day per version', () => {
    writeCache({ checkedAt: T0, latest: '2.3.0' });
    const stderr = fakeTty();
    expect(maybePrintUpdateNotice({ ...brew, storeDir, stderr, now: T0 })).toBe(true);
    expect(stderr.lines).toEqual([
      'Update available: tgcli 2.2.5 -> 2.3.0. Run `brew update && brew upgrade tgcli` (TGCLI_NO_UPDATE_CHECK=1 turns this notice off).\n',
    ]);

    expect(maybePrintUpdateNotice({ ...brew, storeDir, stderr, now: T0 + HOUR })).toBe(false);
    expect(maybePrintUpdateNotice({ ...brew, storeDir, stderr, now: T0 + 23 * HOUR })).toBe(false);
    expect(stderr.lines).toHaveLength(1);

    expect(maybePrintUpdateNotice({ ...brew, storeDir, stderr, now: T0 + 24 * HOUR })).toBe(true);
    expect(stderr.lines).toHaveLength(2);
  });

  it('prints a newer release at once even inside the day', () => {
    writeCache({ checkedAt: T0, latest: '2.3.0', notifiedVersion: '2.3.0', notifiedAt: T0 });
    writeCache({ ...readUpdateCache(storeDir), latest: '2.3.1' });
    const stderr = fakeTty();
    expect(maybePrintUpdateNotice({ ...brew, storeDir, stderr, now: T0 + HOUR })).toBe(true);
    expect(stderr.lines[0]).toContain('2.2.5 -> 2.3.1');
  });

  it('names the npm command for npm installs', () => {
    writeCache({ checkedAt: T0, latest: '2.3.0' });
    const stderr = fakeTty();
    maybePrintUpdateNotice({ ...brew, installMethod: 'npm', storeDir, stderr, now: T0 });
    expect(stderr.lines[0]).toContain('Run `npm i -g @kfastov/tgcli`');
  });

  it('stays silent with --json, --quiet, a non-TTY stderr, no newer version, or the switch off', () => {
    writeCache({ checkedAt: T0, latest: '2.3.0' });
    const stderr = fakeTty();
    expect(maybePrintUpdateNotice({ ...brew, storeDir, stderr, now: T0, json: true })).toBe(false);
    expect(maybePrintUpdateNotice({ ...brew, storeDir, stderr, now: T0, quiet: true })).toBe(false);
    expect(maybePrintUpdateNotice({ ...brew, storeDir, stderr: { ...stderr, isTTY: false }, now: T0 })).toBe(false);
    expect(maybePrintUpdateNotice({ ...brew, storeDir, stderr, now: T0, env: { TGCLI_NO_UPDATE_CHECK: '1' } })).toBe(false);
    expect(maybePrintUpdateNotice({ ...brew, storeDir, stderr, now: T0, env: { CI: '1' } })).toBe(false);
    expect(maybePrintUpdateNotice({ ...brew, storeDir, stderr, now: T0, version: '2.3.0' })).toBe(false);
    expect(maybePrintUpdateNotice({ ...brew, storeDir, stderr, now: T0, version: '2.4.0-dev' })).toBe(false);
    expect(maybePrintUpdateNotice({ ...brew, storeDir, stderr, now: T0, installMethod: null })).toBe(false);
    expect(stderr.write).not.toHaveBeenCalled();
    // Nothing printed, so nothing recorded as shown.
    expect(readUpdateCache(storeDir).notifiedAt).toBeUndefined();
  });

  it('stays silent with no cache or a broken one', () => {
    const stderr = fakeTty();
    expect(maybePrintUpdateNotice({ ...brew, storeDir, stderr, now: T0 })).toBe(false);
    fs.writeFileSync(path.join(storeDir, UPDATE_CACHE_FILE), '{not json');
    expect(maybePrintUpdateNotice({ ...brew, storeDir, stderr, now: T0 })).toBe(false);
    expect(stderr.write).not.toHaveBeenCalled();
  });
});

describe('doctor UPDATE line', () => {
  const brew = { version: '2.2.5', installMethod: 'brew', env: NO_ENV };

  it('reports each state', () => {
    expect(formatDoctorUpdateLine(getUpdateStatus({ ...brew, storeDir }))).toBe('UPDATE: not checked yet');

    writeCache({ checkedAt: T0, latest: '2.3.0', latestAt: T0 });
    const available = getUpdateStatus({ ...brew, storeDir });
    expect(available).toMatchObject({
      current: '2.2.5',
      latest: '2.3.0',
      available: true,
      command: 'brew update && brew upgrade tgcli',
      latestCheckedAt: '2026-11-06T12:00:00.000Z',
    });
    expect(formatDoctorUpdateLine(available))
      .toBe('UPDATE: 2.3.0 available (installed 2.2.5); run `brew update && brew upgrade tgcli`');

    // Offline for a week: the date is when 2.2.5 was last confirmed, not the last attempt.
    writeCache({ checkedAt: T0, latest: '2.2.5', latestAt: T0 - 7 * 24 * HOUR, error: 'registry answered HTTP 503' });
    expect(formatDoctorUpdateLine(getUpdateStatus({ ...brew, storeDir })))
      .toBe('UPDATE: up to date (2.2.5, checked 2026-10-30T12:00:00.000Z); last check failed: registry answered HTTP 503');

    writeCache({ checkedAt: T0, error: 'fetch failed' });
    expect(formatDoctorUpdateLine(getUpdateStatus({ ...brew, storeDir })))
      .toBe('UPDATE: not checked yet; last check failed: fetch failed');

    expect(formatDoctorUpdateLine(getUpdateStatus({ ...brew, storeDir, installMethod: null })))
      .toBe('UPDATE: no check (not installed with Homebrew or npm)');
  });

  it('survives a cache with nonsense in it', () => {
    writeCache({ checkedAt: 1e20, latest: '2.2.5', latestAt: 1e20, error: { nested: true } });
    const status = getUpdateStatus({ ...brew, storeDir });
    expect(status).toMatchObject({ latest: '2.2.5', latestCheckedAt: null, lastError: null });
    expect(formatDoctorUpdateLine(status)).toBe('UPDATE: up to date (2.2.5)');
  });
});

describe('CLI wiring', () => {
  let stdoutSpy;
  let logSpy;

  beforeEach(() => {
    vi.stubEnv('TGCLI_STORE', storeDir);
    stdoutSpy = vi.spyOn(process.stdout, 'write').mockImplementation(() => true);
    logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    vi.unstubAllEnvs();
    process.exitCode = undefined;
    vi.restoreAllMocks();
  });

  // Runs the real program; `stub` swaps the action of a top-level command for a
  // no-op, for commands (server, doctor) that would otherwise touch Telegram.
  async function run(argv, deps, stub = null) {
    const program = buildProgram();
    if (stub) {
      program.commands.find((command) => command.name() === stub).action(() => {});
    }
    installUpdateNotice(program, deps);
    program.exitOverride();
    await program.parseAsync(['node', 'cli.js', ...argv]);
  }

  function due(deps) {
    writeCache({ checkedAt: T0 - 25 * HOUR, latest: '2.3.0' });
    return { version: '2.2.5', installMethod: 'brew', env: NO_ENV, now: () => T0, ...deps };
  }

  function printedToStdout() {
    return [
      ...stdoutSpy.mock.calls.map((call) => String(call[0])),
      ...logSpy.mock.calls.map((call) => call.join(' ')),
    ].join('');
  }

  it('starts the check and prints the cached notice on stderr only', async () => {
    writeCache({ checkedAt: T0 - 25 * HOUR, latest: '2.3.0' });
    const stderr = fakeTty();
    const spawnFn = fakeSpawn();
    const deps = { version: '2.2.5', installMethod: 'brew', env: NO_ENV, stderr, spawnFn, now: () => T0 };

    await run(['config', 'list'], deps);
    expect(spawnFn).toHaveBeenCalledTimes(1);
    expect(stderr.lines).toHaveLength(1);
    expect(stderr.lines[0]).toContain('brew upgrade tgcli');
    expect(printedToStdout()).not.toContain('Update available');

    // Same day: no second check, no second notice.
    await run(['config', 'list'], deps);
    expect(spawnFn).toHaveBeenCalledTimes(1);
    expect(stderr.lines).toHaveLength(1);
  });

  it('keeps --json output clean', async () => {
    writeCache({ checkedAt: T0, latest: '2.3.0' });
    const stderr = fakeTty();
    await run(['--json', 'config', 'list'], { version: '2.2.5', installMethod: 'brew', env: NO_ENV, stderr, spawnFn: fakeSpawn(), now: () => T0 });
    expect(stderr.write).not.toHaveBeenCalled();
    const out = printedToStdout();
    expect(() => JSON.parse(out)).not.toThrow();
    expect(out).not.toContain('Update available');
  });

  it('skips server entirely, and lets doctor check without printing the notice', async () => {
    const stderr = fakeTty();
    const spawnFn = fakeSpawn();
    await run(['server'], due({ stderr, spawnFn }), 'server');
    expect(spawnFn).not.toHaveBeenCalled();
    expect(stderr.write).not.toHaveBeenCalled();

    await run(['doctor'], due({ stderr, spawnFn }), 'doctor');
    expect(spawnFn).toHaveBeenCalledTimes(1);
    expect(stderr.write).not.toHaveBeenCalled();
  });

  it('prints nothing after a failed command or without a terminal', async () => {
    const stderr = fakeTty();
    const errSpy = vi.spyOn(process.stderr, 'write').mockImplementation(() => true);
    await run(['config', 'get', 'no.such.key'], due({ stderr, spawnFn: fakeSpawn() }));
    expect(process.exitCode).toBe(1);
    expect(errSpy).toHaveBeenCalled();
    expect(stderr.write).not.toHaveBeenCalled();
    process.exitCode = undefined;

    const pipe = { isTTY: false, write: vi.fn() };
    const spawnFn = fakeSpawn();
    await run(['config', 'list'], due({ stderr: pipe, spawnFn }));
    expect(pipe.write).not.toHaveBeenCalled();
    // The check still runs, so the cache is fresh for the next person at a terminal.
    expect(spawnFn).toHaveBeenCalledTimes(1);
  });

  it('does nothing when switched off or run from a clone', async () => {
    writeCache({ checkedAt: T0 - 25 * HOUR, latest: '2.3.0' });
    const stderr = fakeTty();
    const spawnFn = fakeSpawn();
    const base = { version: '2.2.5', installMethod: 'brew', stderr, spawnFn, now: () => T0 };
    await run(['config', 'list'], { ...base, env: { TGCLI_NO_UPDATE_CHECK: '1' } });
    await run(['config', 'list'], { ...base, env: NO_ENV, installMethod: null });
    await run(['config', 'list'], { ...base, env: NO_ENV, version: '2.3.0-dev' });
    expect(spawnFn).not.toHaveBeenCalled();
    expect(stderr.write).not.toHaveBeenCalled();
  });
});
