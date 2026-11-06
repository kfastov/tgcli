// Thin client over the always-on loopback control API (see core/control-server.js).
// The CLI is a one-shot process: it asks a long-lived server to execute backfill
// work rather than doing MTProto/SQLite writes itself. Every call here is a plain
// HTTP request to 127.0.0.1, authed with the token from <store>/control.json.
//
// We deliberately use Node's built-in global `fetch` (Node 18+) — no http.request
// hand-rolling — and reuse the server's existing control endpoints, so enqueue and
// cancel logic is never reimplemented client-side.
//
// Because a server outlives upgrades, starting and calling one also keeps it on
// this CLI's version; see reconcileServerVersion.

import fs from 'fs';
import path from 'path';
import { spawn, spawnSync } from 'child_process';
import { fileURLToPath } from 'url';

import { isPidAlive } from '../store-lock.js';
import { CONTROL_FILE, CONTROL_TOKEN_HEADER, hasQueuedJobs } from './control-server.js';
import { SendCommandError } from './send-utils.js';
import { readVersion } from './version.js';

// Path to this package's CLI, used to (re)spawn `tgcli server`.
const CLI_ENTRYPOINT = fileURLToPath(new URL('../cli.js', import.meta.url));

const PING_TIMEOUT_MS = 1500;
const ENQUEUE_TIMEOUT_MS = 30000;
const SERVER_START_TIMEOUT_MS = 10000;
const SERVER_STOP_TIMEOUT_MS = 15000;
const SERVER_POLL_INTERVAL_MS = 250;

const CLI_VERSION = readVersion();
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
  const headers = { [CONTROL_TOKEN_HEADER]: control.token };
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

// GET /control/ping. Returns the parsed body ({ ok, pid, version, startedAt, jobs })
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

function isOlderVersion(version, than) {
  return version.localeCompare(than, undefined, { numeric: true }) < 0;
}

function readProcessField(pid, field) {
  const result = spawnSync('ps', ['-o', `${field}=`, '-p', String(pid)], { encoding: 'utf8' });
  return result.status === 0 ? result.stdout.trim() : '';
}

function isAutoStarted(control, pid) {
  if (typeof control.autoStarted === 'boolean') {
    return control.autoStarted;
  }
  return LEGACY_AUTO_STARTED_ARGS.test(readProcessField(pid, 'command'))
    && NO_TERMINAL.test(readProcessField(pid, 'tty'));
}

async function waitForExit(pid) {
  const deadline = Date.now() + SERVER_STOP_TIMEOUT_MS;
  while (isPidAlive(pid)) {
    if (Date.now() >= deadline) {
      return false;
    }
    await sleep(SERVER_POLL_INTERVAL_MS);
  }
  return true;
}

// SIGTERM runs the server's graceful shutdown. Resolves true once the process is
// gone, so the replacement does not start beside it.
async function stopServer(pid) {
  try {
    process.kill(pid, 'SIGTERM');
  } catch (error) {
    return error.code === 'ESRCH';
  }
  return waitForExit(pid);
}

// A server outlives upgrades: it never idles out while channels are watched, and
// every command runs on it, so without this check an upgraded CLI keeps executing
// the old code. An idle auto-started server older than this CLI is stopped, and the
// callers' cold-start path brings up the new version. Anything else keeps serving,
// with a warning: a user-owned server is not ours to stop, stopping a busy one
// interrupts its backfill, and replacing a newer one would make two installed
// versions keep restarting each other.
async function reconcileServerVersion(storeDir, { quiet = false } = {}) {
  const control = readControlFile(storeDir);
  if (!control?.version || control.version === CLI_VERSION) {
    return;
  }
  const ping = await pingServer(storeDir);
  if (!ping) {
    return;
  }

  const { pid } = ping;
  const replaceable = isOlderVersion(control.version, CLI_VERSION) && isAutoStarted(control, pid);
  if (replaceable && !hasQueuedJobs(ping.jobs)) {
    const stopped = await stopServer(pid);
    if (!quiet) {
      console.error(stopped
        ? `Restarting tgcli server (${control.version} -> ${CLI_VERSION}).`
        : `Warning: tgcli server (pid ${pid}) was sent SIGTERM to move from ${control.version} to ${CLI_VERSION} but has not exited.`);
    }
    return;
  }

  if (!quiet) {
    const nextStep = replaceable
      ? 'It will be replaced once its backfill queue is empty.'
      : 'Restart it to use this version.';
    console.error(`Warning: tgcli server (pid ${pid}) runs ${control.version}, this CLI is ${CLI_VERSION}; commands use the server's code. ${nextStep}`);
  }
}

// Spawn `tgcli server --idle-exit 60s --auto-started` fully detached so it outlives
// this one-shot CLI process. We ignore its stdio and unref the child; the server
// writes its own control.json once it is listening.
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
  if (previousPid && readProcessField(previousPid, 'command').includes('mcp-server.js')) {
    await waitForExit(previousPid);
  }

  spawnDetachedServer();

  const deadline = Date.now() + SERVER_START_TIMEOUT_MS;
  while (Date.now() < deadline) {
    await sleep(SERVER_POLL_INTERVAL_MS);
    const ping = await pingServer(storeDir);
    if (ping) {
      return ping;
    }
  }
  throw new Error('Timed out waiting for the control server to start.');
}
