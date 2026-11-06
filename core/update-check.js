import fs from 'fs';
import path from 'path';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';

// New-version notice. Once a day a detached child asks the npm registry for the
// latest published version and writes it to a small cache file in the store; the
// CLI itself never waits on the network and decides from that cache alone whether
// to print one line on stderr.

export const UPDATE_CHECK_DISABLE_ENV = 'TGCLI_NO_UPDATE_CHECK';
export const UPDATE_CACHE_FILE = 'update-check.json';
// How often the registry is asked, and how often the same notice is repeated.
export const UPDATE_INTERVAL_MS = 24 * 60 * 60 * 1000;
export const REGISTRY_DIST_TAGS_URL = 'https://registry.npmjs.org/-/package/@kfastov/tgcli/dist-tags';
// Homebrew refreshes taps only once a day before `brew upgrade` (HOMEBREW_AUTO_UPDATE_SECS
// defaults to 86400), so a bare `brew upgrade tgcli` can miss a release made today.
export const UPGRADE_COMMANDS = {
  brew: 'brew update && brew upgrade tgcli',
  npm: 'npm i -g @kfastov/tgcli',
};

const FETCH_TIMEOUT_MS = 10_000;
const PACKAGE_ROOT = fileURLToPath(new URL('..', import.meta.url));
const WORKER_PATH = fileURLToPath(new URL('./update-check-worker.js', import.meta.url));
const VERSION_PATTERN = /^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?(?:\+[0-9A-Za-z.-]+)?$/;
const FALSY_ENV_VALUES = new Set(['', '0', 'false', 'no', 'off']);

export function parseVersion(value) {
  const match = typeof value === 'string' ? VERSION_PATTERN.exec(value.trim()) : null;
  if (!match) return null;
  return {
    major: Number(match[1]),
    minor: Number(match[2]),
    patch: Number(match[3]),
    prerelease: match[4] ?? null,
  };
}

// True when `latest` is a release strictly newer than `current`. Prereleases on
// either side never count: a dev build is not offered an update, and a prerelease
// that reaches the `latest` tag by mistake is not advertised.
export function isNewerVersion(latest, current) {
  const a = parseVersion(latest);
  const b = parseVersion(current);
  if (!a || !b || a.prerelease || b.prerelease) return false;
  if (a.major !== b.major) return a.major > b.major;
  if (a.minor !== b.minor) return a.minor > b.minor;
  return a.patch > b.patch;
}

// How a package root at this (real) path was installed:
//   'brew' -> <prefix>/Cellar/tgcli/<version>/libexec/lib/node_modules/@kfastov/tgcli
//   'npm'  -> <prefix>/lib/node_modules/@kfastov/tgcli (nvm, volta and system Node
//             alike) or %AppData%\npm\node_modules\@kfastov\tgcli on Windows
//   null   -> anything else: a git clone, an npx cache, a project dependency, pnpm,
//             the Nix store.
// Only the first two know a command that updates them.
export function classifyInstallPath(packageRoot) {
  const normalized = String(packageRoot).replace(/\\/g, '/').replace(/\/+$/, '');
  if (/\/Cellar\/tgcli\/[^/]+\//.test(normalized)) return 'brew';
  if (normalized.startsWith('/nix/store/')) return null;
  if (/\/lib\/node_modules\/@kfastov\/tgcli$/.test(normalized)) return 'npm';
  if (/\/npm\/node_modules\/@kfastov\/tgcli$/i.test(normalized)) return 'npm';
  return null;
}

// The install method of this copy. Symlinks are resolved first, so `npm link` and
// Homebrew's opt/ and bin/ links classify by where the files really are.
export function detectInstallMethod(packageRoot = PACKAGE_ROOT) {
  let resolved;
  try {
    resolved = fs.realpathSync(packageRoot);
  } catch {
    resolved = path.resolve(packageRoot);
  }
  return classifyInstallPath(resolved);
}

function isTruthyEnv(value) {
  return value !== undefined && !FALSY_ENV_VALUES.has(String(value).trim().toLowerCase());
}

// Why there is no update check for this run, or null when there is one.
export function getUpdateCheckDisabledReason({ env = process.env, version, installMethod } = {}) {
  if (isTruthyEnv(env[UPDATE_CHECK_DISABLE_ENV])) return `${UPDATE_CHECK_DISABLE_ENV} is set`;
  if (isTruthyEnv(env.NO_UPDATE_NOTIFIER)) return 'NO_UPDATE_NOTIFIER is set';
  if (isTruthyEnv(env.CI)) return 'CI';
  if (!installMethod) return 'not installed with Homebrew or npm';
  const parsed = parseVersion(version);
  if (!parsed || parsed.prerelease || version === '0.0.0') return 'development build';
  return null;
}

function cachePath(storeDir) {
  return path.join(storeDir, UPDATE_CACHE_FILE);
}

export function readUpdateCache(storeDir) {
  try {
    const data = JSON.parse(fs.readFileSync(cachePath(storeDir), 'utf8'));
    return data && typeof data === 'object' && !Array.isArray(data) ? data : null;
  } catch {
    return null;
  }
}

// Write through a temporary file so a reader never sees half a file. Concurrent
// writers (the worker, a notice, another command) are not merged: the last one
// wins, which at worst costs one extra check or one repeated notice.
function writeUpdateCache(storeDir, data) {
  const target = cachePath(storeDir);
  const temp = `${target}.${process.pid}.tmp`;
  try {
    fs.writeFileSync(temp, `${JSON.stringify(data, null, 2)}\n`);
    fs.renameSync(temp, target);
  } catch (error) {
    fs.rmSync(temp, { force: true });
    throw error;
  }
}

function isDue(timestamp, now, interval) {
  if (!Number.isFinite(timestamp)) return true;
  const age = now - timestamp;
  return age < 0 || age >= interval;
}

// Start a background registry check when the last one is a day old. Records the
// attempt before starting it, so the commands that follow do not start another
// while it runs. Never throws and never waits on the network. Returns true when
// it started one.
export function scheduleUpdateCheck({
  storeDir,
  now = Date.now(),
  url = REGISTRY_DIST_TAGS_URL,
  spawnFn = spawn,
  execPath = process.execPath,
} = {}) {
  try {
    if (!storeDir || !fs.existsSync(storeDir)) return false;
    const cache = readUpdateCache(storeDir) ?? {};
    if (!isDue(cache.checkedAt, now, UPDATE_INTERVAL_MS)) return false;
    writeUpdateCache(storeDir, { ...cache, checkedAt: now });
    const child = spawnFn(execPath, [WORKER_PATH, storeDir, url], {
      detached: true,
      stdio: 'ignore',
      windowsHide: true,
    });
    child.on?.('error', () => {});
    child.unref?.();
    return true;
  } catch {
    return false;
  }
}

// Body of the background check: fetch the `latest` dist-tag and store it. A failed
// fetch keeps the last known version and records the error. Never throws.
export async function refreshUpdateCache({
  storeDir,
  url = REGISTRY_DIST_TAGS_URL,
  fetchFn = globalThis.fetch,
  now = () => Date.now(),
  timeoutMs = FETCH_TIMEOUT_MS,
} = {}) {
  let latest = null;
  let error = null;
  try {
    const response = await fetchFn(url, {
      headers: { accept: 'application/json' },
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (!response.ok) {
      throw new Error(`registry answered HTTP ${response.status}`);
    }
    const body = await response.json();
    if (!parseVersion(body?.latest)) {
      throw new Error('registry answer has no valid latest version');
    }
    latest = body.latest;
  } catch (err) {
    error = err?.message ?? String(err);
  }
  try {
    const cache = readUpdateCache(storeDir) ?? {};
    const checkedAt = now();
    const next = error
      ? { ...cache, checkedAt, error }
      : { ...cache, checkedAt, latest, latestAt: checkedAt, error: null };
    writeUpdateCache(storeDir, next);
  } catch {
    // The store went away or is read-only; the next run tries again tomorrow.
  }
  return { latest, error };
}

function toIsoOrNull(timestamp) {
  const date = new Date(Number.isFinite(timestamp) ? timestamp : NaN);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

function statusFromCache(cache, { version, installMethod, disabledReason }) {
  const latest = parseVersion(cache?.latest) ? cache.latest : null;
  return {
    current: version,
    latest,
    available: Boolean(latest) && isNewerVersion(latest, version),
    command: installMethod ? UPGRADE_COMMANDS[installMethod] : null,
    installMethod: installMethod ?? null,
    // When `latest` was learned, not when a check was last tried: a week offline
    // shows a week-old date here, with the failure in lastError.
    latestCheckedAt: latest ? toIsoOrNull(cache.latestAt) : null,
    lastError: (typeof cache?.error === 'string' && cache.error) || null,
    disabledReason,
  };
}

// What the cache says about updates for this copy. Pure read; no network.
export function getUpdateStatus({ storeDir, version, installMethod, env = process.env } = {}) {
  const disabledReason = getUpdateCheckDisabledReason({ env, version, installMethod });
  const cache = disabledReason ? null : readUpdateCache(storeDir);
  return statusFromCache(cache, { version, installMethod, disabledReason });
}

export function formatUpdateNotice(status) {
  return `Update available: tgcli ${status.current} -> ${status.latest}. `
    + `Run \`${status.command}\` (${UPDATE_CHECK_DISABLE_ENV}=1 turns this notice off).`;
}

// Print the notice line on stderr when the cache knows a newer version, at most
// once a day per version. Only for a person at a terminal: never with --json or
// --quiet, and never when stderr is not a TTY (agents, pipes, cron). Never throws.
export function maybePrintUpdateNotice({
  storeDir,
  version,
  installMethod,
  json = false,
  quiet = false,
  env = process.env,
  stderr = process.stderr,
  now = Date.now(),
} = {}) {
  try {
    if (json || quiet || !stderr?.isTTY) return false;
    const disabledReason = getUpdateCheckDisabledReason({ env, version, installMethod });
    if (disabledReason) return false;
    const cache = readUpdateCache(storeDir) ?? {};
    const status = statusFromCache(cache, { version, installMethod, disabledReason });
    if (!status.available || !status.command) return false;
    if (cache.notifiedVersion === status.latest
      && !isDue(cache.notifiedAt, now, UPDATE_INTERVAL_MS)) {
      return false;
    }
    stderr.write(`${formatUpdateNotice(status)}\n`);
    writeUpdateCache(storeDir, { ...cache, notifiedAt: now, notifiedVersion: status.latest });
    return true;
  } catch {
    return false;
  }
}

// The `UPDATE:` line of `tgcli doctor`.
export function formatDoctorUpdateLine(status) {
  if (status.disabledReason) {
    return `UPDATE: no check (${status.disabledReason})`;
  }
  const failure = status.lastError ? `; last check failed: ${status.lastError}` : '';
  if (!status.latest) {
    return `UPDATE: not checked yet${failure}`;
  }
  if (status.available) {
    return `UPDATE: ${status.latest} available (installed ${status.current}); run \`${status.command}\`${failure}`;
  }
  const checked = status.latestCheckedAt ? `, checked ${status.latestCheckedAt}` : '';
  return `UPDATE: up to date (${status.current}${checked})${failure}`;
}
