// Thin client over the always-on loopback control API (see core/control-server.js).
// The CLI is a one-shot process: it asks a long-lived server to execute backfill
// work rather than doing MTProto/SQLite writes itself. Every call here is a plain
// HTTP request to 127.0.0.1, authed with the token from <store>/control.json.
//
// We deliberately use Node's built-in global `fetch` (Node 18+) — no http.request
// hand-rolling — and reuse the server's existing control endpoints, so enqueue and
// cancel logic is never reimplemented client-side.
//
// Because a server outlives upgrades, every command keeps it on this CLI's version
// (see versionTakeover and syncServerVersion), and `tgcli server stop`/`restart`
// stop it the same graceful way.

import fs from 'fs';
import path from 'path';
import { spawn, spawnSync } from 'child_process';
import { fileURLToPath } from 'url';

import { isPidAlive } from '../store-lock.js';
import {
  CONTROL_CLIENT_VERSION_HEADER,
  CONTROL_FILE,
  CONTROL_TOKEN_HEADER,
  hasQueuedJobs,
  removeControlFile,
} from './control-server.js';
import { SendCommandError } from './send-utils.js';
import { compareVersions, isDevVersion, readVersion } from './version.js';

// Path to this package's CLI, used to (re)spawn `tgcli server`.
const CLI_ENTRYPOINT = fileURLToPath(new URL('../cli.js', import.meta.url));

const PING_TIMEOUT_MS = 1500;
const ENQUEUE_TIMEOUT_MS = 30000;
const SERVER_START_TIMEOUT_MS = 10000;
const SERVER_STOP_TIMEOUT_MS = 15000;
// `tgcli server stop` lets the queue worker finish the batch it is on.
const EXPLICIT_STOP_TIMEOUT_MS = 60000;
const KILL_TIMEOUT_MS = 5000;
const SERVER_POLL_INTERVAL_MS = 250;
// A dev server that a CLI of its own version used this recently is being tested;
// other versions leave it alone until then (see versionTakeover).
export const DEV_LEASE_MS = 10 * 60 * 1000;

export const CLI_VERSION = readVersion();
const AUTO_STARTED_IDLE_EXIT = '60s';
// Servers older than control.json's autoStarted field do not say who started them.
// ensureServer's ran with this window and, being detached, with no terminal.
const LEGACY_AUTO_STARTED_ARGS = /--idle-exit 60000(\s|$)/;
const NO_TERMINAL = /^\?+$/;
const REQUEST_LOST_CODES = new Set(['UND_ERR_SOCKET', 'ECONNRESET', 'EPIPE']);

// Raised when the control server cannot be reached at all: no control.json, a
// refused/failed loopback connection, etc. This is distinct from an operation
// that ran on a live server and failed (a 4xx/5xx). Callers use it to decide
// whether to start a server and retry, versus surfacing the operation's error.
export class ServerUnavailableError extends Error {
  constructor(message, { cause } = {}) {
    super(message);
    this.name = 'ServerUnavailableError';
    if (cause !== undefined) {
      this.cause = cause;
    }
  }
}

// True for a network-level fetch failure (e.g. ECONNREFUSED) where the server
// never answered. An AbortSignal.timeout fires a TimeoutError and an explicit
// abort fires an AbortError; those mean the server was reached but did not reply
// in time, so they are NOT connection failures.
function isFetchConnectionFailure(error) {
  if (!error) {
    return false;
  }
  if (error.name === 'TimeoutError' || error.name === 'AbortError') {
    return false;
  }
  return error instanceof TypeError || typeof error.code === 'string';
}

// The socket died after the request went out, so the server may already have run
// the operation: a shutdown closes connections mid-request. Retrying could send a
// message twice, so this must not look like an unreachable server.
function isRequestLostMidway(error) {
  return REQUEST_LOST_CODES.has(error?.cause?.code);
}

// Parse <store>/control.json, returning the descriptor or null when absent.
// Shape: { pid, port, token, startedAt, version }.
export function readControlFile(storeDir) {
  const filePath = path.join(storeDir, CONTROL_FILE);
  let raw;
  try {
    raw = fs.readFileSync(filePath, 'utf8');
  } catch (error) {
    if (error?.code === 'ENOENT') {
      return null;
    }
    throw error;
  }
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function controlUrl(control, pathname) {
  return `http://127.0.0.1:${control.port}${pathname}`;
}

async function controlFetch(control, pathname, { method = 'GET', body, timeoutMs } = {}) {
  const headers = {
    [CONTROL_TOKEN_HEADER]: control.token,
    [CONTROL_CLIENT_VERSION_HEADER]: CLI_VERSION,
  };
  if (body !== undefined) {
    headers['Content-Type'] = 'application/json';
  }
  const signal = Number.isFinite(timeoutMs) ? AbortSignal.timeout(timeoutMs) : undefined;
  const res = await fetch(controlUrl(control, pathname), {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
    signal,
  });
  let json = null;
  try {
    json = await res.json();
  } catch {
    json = null;
  }
  return { status: res.status, json };
}

// GET /control/ping. Returns the parsed body ({ ok, pid, version, startedAt, jobs,
// ownClientIdleMs })
// or null when no server is reachable (no control.json, refused connection, timeout,
// or a non-2xx response).
export async function pingServer(storeDir) {
  const control = readControlFile(storeDir);
  if (!control || !control.port || !control.token) {
    return null;
  }
  try {
    const { status, json } = await controlFetch(control, '/control/ping', {
      timeoutMs: PING_TIMEOUT_MS,
    });
    if (status === 200 && json?.ok) {
      return json;
    }
    return null;
  } catch {
    return null;
  }
}

// POST /control/backfill → { jobId, channelId, status }. Throws on a non-2xx
// response so the caller surfaces the server's error (e.g. unknown chat).
export async function enqueueBackfill(storeDir, { chatId, depth, minDate } = {}) {
  const control = readControlFile(storeDir);
  if (!control) {
    throw new Error('No control server is running. Start one with `tgcli server`.');
  }
  const { status, json } = await controlFetch(control, '/control/backfill', {
    method: 'POST',
    body: { chatId, depth, minDate },
    timeoutMs: ENQUEUE_TIMEOUT_MS,
  });
  if (status !== 200) {
    throw new Error(json?.error ?? `Backfill request failed (HTTP ${status})`);
  }
  return json;
}

// POST /control/repair → { channelIds }. Queues a repair of each chat (or of every
// chat with sync on). Throws on a non-2xx response, e.g. a chat not in the archive.
export async function enqueueRepair(storeDir, { chats, all, afterId, sinceDate } = {}) {
  const control = readControlFile(storeDir);
  if (!control) {
    throw new Error('No control server is running. Start one with `tgcli server`.');
  }
  const { status, json } = await controlFetch(control, '/control/repair', {
    method: 'POST',
    body: { chats, all, afterId, sinceDate },
    timeoutMs: ENQUEUE_TIMEOUT_MS,
  });
  if (status === 404) {
    throw new Error(`The running tgcli server (${control.version ?? 'unknown version'}, pid ${control.pid ?? '?'}) predates \`backfill repair\`; restart it and try again.`);
  }
  if (status !== 200) {
    throw new Error(json?.error ?? `Repair request failed (HTTP ${status})`);
  }
  return json;
}

// POST /control/cancel → { canceled, jobIds }. Throws on a non-2xx response.
export async function cancelBackfill(storeDir, { chatId, jobId } = {}) {
  const control = readControlFile(storeDir);
  if (!control) {
    throw new Error('No control server is running.');
  }
  const { status, json } = await controlFetch(control, '/control/cancel', {
    method: 'POST',
    body: { chatId, jobId },
    timeoutMs: ENQUEUE_TIMEOUT_MS,
  });
  if (status !== 200) {
    throw new Error(json?.error ?? `Cancel request failed (HTTP ${status})`);
  }
  return json;
}

// POST /control/retry → { updated, jobIds }. Resets the matching errored job(s)
// to pending on the server so its queue reprocesses them. Throws on a non-2xx
// response.
export async function retryBackfill(storeDir, { jobId, channelId, allErrors } = {}) {
  const control = readControlFile(storeDir);
  if (!control) {
    throw new Error('No control server is running. Start one with `tgcli server`.');
  }
  const { status, json } = await controlFetch(control, '/control/retry', {
    method: 'POST',
    body: { jobId, channelId, allErrors },
    timeoutMs: ENQUEUE_TIMEOUT_MS,
  });
  if (status !== 200) {
    throw new Error(json?.error ?? `Retry request failed (HTTP ${status})`);
  }
  return json;
}

// POST /control/invoke { op, args } → the operation's result. Runs the shared
// operation handler against the server's warm services and returns `result`.
// timeoutMs bounds the request wait (defaults to the enqueue budget); a value of
// 0 disables it. A send op that fails reports structured details under
// `sendError`, which we re-throw as a SendCommandError so the CLI renders the
// same human/JSON error as a local send would. Other non-2xx responses surface
// the server's error message.
export async function invoke(storeDir, { op, args, timeoutMs, quiet } = {}) {
  await reconcileServerVersion(storeDir, { quiet });
  const control = readControlFile(storeDir);
  if (!control) {
    throw new ServerUnavailableError('No control server is running.');
  }
  const effectiveTimeoutMs = timeoutMs === undefined ? ENQUEUE_TIMEOUT_MS : timeoutMs;
  let response;
  try {
    response = await controlFetch(control, '/control/invoke', {
      method: 'POST',
      body: { op, args },
      timeoutMs: effectiveTimeoutMs > 0 ? effectiveTimeoutMs : undefined,
    });
  } catch (error) {
    // A refused/failed loopback connection means the server is not actually up;
    // surface it as unavailable so the caller can start one and retry. A request
    // timeout (TimeoutError/AbortError) reaches a live server and is re-thrown.
    if (isRequestLostMidway(error)) {
      throw new Error(`Lost the connection to tgcli server during ${op}; it may or may not have completed, so it was not retried.`, { cause: error });
    }
    if (isFetchConnectionFailure(error)) {
      throw new ServerUnavailableError('Could not reach the control server.', { cause: error });
    }
    throw error;
  }
  const { status, json } = response;
  if (status !== 200) {
    if (json?.sendError) {
      throw new SendCommandError(json.sendError);
    }
    throw new Error(json?.error ?? `Operation failed (HTTP ${status})`);
  }
  return json?.result;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function readProcessField(pid, field) {
  const result = spawnSync('ps', ['-o', `${field}=`, '-p', String(pid)], { encoding: 'utf8' });
  return result?.status === 0 ? result.stdout.trim() : '';
}

function isServerProcess(pid) {
  return readProcessField(pid, 'command').includes('mcp-server.js');
}

function isAutoStarted(control, pid) {
  if (typeof control.autoStarted === 'boolean') {
    return control.autoStarted;
  }
  return LEGACY_AUTO_STARTED_ARGS.test(readProcessField(pid, 'command'))
    && NO_TERMINAL.test(readProcessField(pid, 'tty'));
}

const SERVICE_MANAGERS = new Set(['brew', 'launchd', 'systemd']);

// The service manager that runs the server, if any: control.json says so since
// 2.2.6; older servers only wrote it to service-state.json.
function serviceManager(storeDir, control, pid) {
  if (control?.manager) {
    return control.manager;
  }
  try {
    const state = JSON.parse(fs.readFileSync(path.join(storeDir, 'service-state.json'), 'utf8'));
    return state?.pid === pid ? state.manager ?? null : null;
  } catch {
    return null;
  }
}

async function waitForExit(pid, timeoutMs = SERVER_STOP_TIMEOUT_MS) {
  const deadline = Date.now() + timeoutMs;
  while (isPidAlive(pid)) {
    if (Date.now() >= deadline) {
      return false;
    }
    await sleep(SERVER_POLL_INTERVAL_MS);
  }
  return true;
}

// Polls until a server that satisfies accept answers, or the start timeout ends.
async function waitForServer(storeDir, accept = () => true) {
  const deadline = Date.now() + SERVER_START_TIMEOUT_MS;
  while (Date.now() < deadline) {
    await sleep(SERVER_POLL_INTERVAL_MS);
    const ping = await pingServer(storeDir);
    if (ping && accept(ping)) {
      return ping;
    }
  }
  return null;
}

// Asks the server to run its graceful shutdown: over the control API, or with
// SIGTERM when it predates /control/shutdown or does not answer. Resolves to how
// it was asked ('control' or 'signal'), or null when the process is gone already.
async function requestShutdown(control, pid) {
  if (control?.port && control?.token && control.pid === pid) {
    try {
      const { status } = await controlFetch(control, '/control/shutdown', {
        method: 'POST',
        timeoutMs: PING_TIMEOUT_MS,
      });
      if (status === 202) {
        return 'control';
      }
    } catch {
      // The signal below runs the same shutdown.
    }
  }
  try {
    process.kill(pid, 'SIGTERM');
    return 'signal';
  } catch (error) {
    if (error.code === 'ESRCH') {
      return null;
    }
    throw error;
  }
}

// Whether a CLI on cliVersion replaces a server on serverVersion by itself:
//   'same'       nothing to do;
//   'take-over'  this CLI is newer, or the server is a dev build nobody uses now;
//   'dev-in-use' the server is a dev build a CLI of its own version used within
//                devLeaseMs, so it is being tested and every other version
//                leaves it alone until it has been unused that long;
//   'newer'      the server is newer, and this CLI does not downgrade it.
// So two installed versions settle on the newer one instead of restarting each
// other on every command, a CLI from a clone (a dev version above the release it
// grew from) takes over from the installed one, and the installed CLI takes its
// server back once the dev build has been left alone. `tgcli server restart`
// switches to the caller's version in any direction.
export function versionTakeover({ serverVersion, cliVersion, ownClientIdleMs, devLeaseMs = DEV_LEASE_MS }) {
  if (!serverVersion || serverVersion === cliVersion) {
    return 'same';
  }
  if (isDevVersion(serverVersion)) {
    return Number.isFinite(ownClientIdleMs) && ownClientIdleMs < devLeaseMs ? 'dev-in-use' : 'take-over';
  }
  return compareVersions(cliVersion, serverVersion) > 0 ? 'take-over' : 'newer';
}

// Servers this process already decided to keep, so a command that checks twice
// (before it runs, then in invoke) warns once and pings once.
const keptServers = new Set();

function keepServerHint({ takeover, ours, busy, serverVersion }) {
  if (takeover === 'take-over' && ours && busy) {
    return 'It will be replaced once its backfill queue is empty.';
  }
  if (takeover === 'dev-in-use') {
    return `${serverVersion} is a dev build in use; it goes back to this version once unused for ${Math.round(DEV_LEASE_MS / 60000)} minutes, or now with \`tgcli server restart\`.`;
  }
  if (takeover === 'newer') {
    return '`tgcli server restart` switches it to this version.';
  }
  return 'Restart it to use this version: `tgcli server restart`.';
}

// A server outlives upgrades: it never idles out while channels are watched, and
// every command runs on it, so without this check an upgraded CLI keeps executing
// the old code. An idle auto-started server that versionTakeover hands to this CLI
// is stopped here. Anything else keeps serving, with a warning: a user-owned
// server is not ours to stop, and stopping a busy one interrupts its backfill.
//
// Resolves to 'replaced' when it stopped the server (callers start this version
// in its place), 'current' when the server runs this version, 'kept' when another
// version keeps serving, and 'none' when no server answers.
async function reconcileServerVersion(storeDir, { quiet = false } = {}) {
  const control = readControlFile(storeDir);
  if (!control?.version) {
    return 'none';
  }
  if (control.version === CLI_VERSION) {
    return 'current';
  }
  const key = `${storeDir}\0${control.pid}`;
  if (keptServers.has(key)) {
    return 'kept';
  }
  const ping = await pingServer(storeDir);
  if (!ping) {
    return 'none';
  }

  const { pid } = ping;
  const serverVersion = ping.version ?? control.version;
  const takeover = versionTakeover({
    serverVersion,
    cliVersion: CLI_VERSION,
    ownClientIdleMs: ping.ownClientIdleMs,
  });
  if (takeover === 'same') {
    return 'current';
  }
  const ours = isAutoStarted(control, pid);
  const busy = hasQueuedJobs(ping.jobs);
  if (takeover === 'take-over' && ours && !busy) {
    const method = await requestShutdown(control, pid);
    const stopped = method === null || await waitForExit(pid);
    if (!quiet) {
      console.error(stopped
        ? `Restarting tgcli server (${serverVersion} -> ${CLI_VERSION}).`
        : `Warning: tgcli server (pid ${pid}) was asked to stop to move from ${serverVersion} to ${CLI_VERSION} but has not exited.`);
    }
    if (stopped) {
      return 'replaced';
    }
    keptServers.add(key);
    return 'kept';
  }

  keptServers.add(key);
  if (!quiet) {
    console.error(`Warning: tgcli server (pid ${pid}) runs ${serverVersion}, this CLI is ${CLI_VERSION}; commands use the server's code. ${keepServerHint({ takeover, ours, busy, serverVersion })}`);
  }
  return 'kept';
}

// Runs before every command, so a server on another version is replaced as soon
// as any command sees it, not at the first one that needs the server: after
// `brew upgrade`, `tgcli backfill status` already brings up the new version. A dev
// build also renews its lease here (see versionTakeover) on commands that would
// not talk to the server otherwise.
export async function syncServerVersion(storeDir, { quiet } = {}) {
  const outcome = await reconcileServerVersion(storeDir, { quiet });
  if (outcome === 'replaced') {
    await ensureServer(storeDir, { quiet });
  } else if (outcome === 'current' && isDevVersion(CLI_VERSION)) {
    await pingServer(storeDir);
  }
  return outcome;
}

function describeServer(storeDir, control, ping, pid) {
  return {
    pid,
    version: ping?.version ?? control?.version ?? null,
    autoStarted: control ? isAutoStarted(control, pid) : null,
    manager: serviceManager(storeDir, control, pid),
  };
}

// `tgcli server stop`: asks the running server to shut down and waits until its
// process is gone, so whatever comes next does not start beside it. The worker
// requeues the job it was on, so a stopped backfill resumes on the next start.
// With force, a server still running after timeoutMs is killed. onStopping hears
// about the server before the wait starts.
//
// Resolves to { running, stopped, pid, version, autoStarted, manager, method }:
// running is false when no server was up; method is 'control', 'signal' or 'kill'.
export async function stopServer(storeDir, { timeoutMs = EXPLICIT_STOP_TIMEOUT_MS, force = false, onStopping } = {}) {
  const control = readControlFile(storeDir);
  const ping = await pingServer(storeDir);
  const pid = ping?.pid ?? control?.pid ?? null;
  // A server that keeps its control.json but no longer answers is shutting down
  // or hung; it is still the one to stop.
  if (!ping && !(pid && isPidAlive(pid) && isServerProcess(pid))) {
    return { running: false, stopped: false, pid: null, version: null, autoStarted: null, manager: null, method: null };
  }
  const server = { running: true, ...describeServer(storeDir, control, ping, pid) };
  onStopping?.(server);
  const method = await requestShutdown(control, pid);
  if (method === null || await waitForExit(pid, timeoutMs)) {
    return { ...server, stopped: true, method };
  }
  if (!force) {
    return { ...server, stopped: false, method };
  }
  try {
    process.kill(pid, 'SIGKILL');
  } catch (error) {
    if (error.code !== 'ESRCH') {
      throw error;
    }
  }
  const killed = await waitForExit(pid, KILL_TIMEOUT_MS);
  // A killed server leaves its control.json behind.
  if (killed && readControlFile(storeDir)?.pid === pid) {
    removeControlFile(storeDir);
  }
  return { ...server, stopped: killed, method: 'kill' };
}

export function isManagedServer(server) {
  return Boolean(server?.running) && server.autoStarted === false && SERVICE_MANAGERS.has(server.manager);
}

// `tgcli server restart`: stops the running server, if any, and starts this CLI's
// version in its place. A server run by a service manager comes back through it,
// on the version the service is set up with. Resolves to { previous, current }:
// previous is stopServer's result, current the new server's ping (null when the
// old one did not stop).
export async function restartServer(storeDir, { timeoutMs, force = false, quiet = false, onStopping } = {}) {
  const previous = await stopServer(storeDir, { timeoutMs, force, onStopping });
  if (previous.running && !previous.stopped) {
    return { previous, current: null };
  }
  let current = null;
  if (isManagedServer(previous)) {
    current = await waitForServer(storeDir, (ping) => ping.pid !== previous.pid);
  }
  current ??= await ensureServer(storeDir, { quiet });
  return { previous, current };
}

// Spawn `tgcli server --idle-exit 60s --auto-started` fully detached so it outlives
// this one-shot CLI process. We ignore its stdio and unref the child: the server
// writes its output to <store>/server.log and its own control.json once it is
// listening.
function spawnDetachedServer() {
  const child = spawn(process.execPath, [CLI_ENTRYPOINT, 'server', '--idle-exit', AUTO_STARTED_IDLE_EXIT, '--auto-started'], {
    detached: true,
    stdio: 'ignore',
    env: process.env,
  });
  child.unref();
  return child;
}

// Ensure a control server is reachable, returning its ping body.
//   - If one is already up, use it (covers the race where another CLI invocation,
//     or the user's own `tgcli server`, started it first), unless it is outdated
//     and reconcileServerVersion stopped it.
//   - A server that is shutting down no longer answers but keeps its control.json
//     and the DB until it exits, so let it finish rather than start beside it.
//   - Otherwise spawn a detached server and poll pingServer for up to ~10s. The
//     idle-exit window lets it shut itself down once the queue drains and nothing
//     is being watched.
export async function ensureServer(storeDir, { quiet } = {}) {
  await reconcileServerVersion(storeDir, { quiet });
  const existing = await pingServer(storeDir);
  if (existing) {
    return existing;
  }

  const previousPid = readControlFile(storeDir)?.pid;
  if (previousPid && isServerProcess(previousPid)) {
    await waitForExit(previousPid);
  }

  spawnDetachedServer();

  const ping = await waitForServer(storeDir);
  if (ping) {
    return ping;
  }
  throw new Error('Timed out waiting for the control server to start; see `tgcli server logs`.');
}
