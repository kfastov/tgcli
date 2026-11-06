import fs from 'fs';
import path from 'path';
import crypto from 'crypto';

import { readJsonBody, sendJson } from './http-util.js';
import { OPERATIONS } from './operations.js';
import { SendCommandError } from './send-utils.js';
import { isChatId } from './chat-key.js';

export const CONTROL_FILE = 'control.json';
export const CONTROL_TOKEN_HEADER = 'x-tgcli-control-token';
// Each CLI request names its version, so a server knows when a CLI of its own
// version last used it (see ownClientIdleMs in /control/ping).
export const CONTROL_CLIENT_VERSION_HEADER = 'x-tgcli-client-version';

// Generate a fresh control secret. Loopback binding plus this token are the two
// layers guarding the always-on control API.
export function generateControlToken() {
  return crypto.randomBytes(24).toString('hex');
}

// Write <store>/control.json with mode 0600 so only the owner can read the token.
// Returns the descriptor written so callers can hold onto it.
export function writeControlFile(storeDir, descriptor) {
  fs.mkdirSync(storeDir, { recursive: true });
  const filePath = path.join(storeDir, CONTROL_FILE);
  fs.writeFileSync(filePath, `${JSON.stringify(descriptor, null, 2)}\n`, { mode: 0o600 });
  // writeFileSync only applies mode on create; chmod guards the overwrite case.
  fs.chmodSync(filePath, 0o600);
  return descriptor;
}

// With owner ({ pid, token }), removes the file only while it still describes
// that server: one that took long to shut down must not delete the control.json
// of the server that started in its place.
export function removeControlFile(storeDir, owner = null) {
  const filePath = path.join(storeDir, CONTROL_FILE);
  try {
    if (owner) {
      let current = null;
      try {
        current = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      } catch (error) {
        if (error?.code === 'ENOENT') {
          return;
        }
      }
      if (current && (current.pid !== owner.pid || current.token !== owner.token)) {
        return;
      }
    }
    fs.unlinkSync(filePath);
  } catch (error) {
    if (error?.code !== 'ENOENT') {
      throw error;
    }
  }
}

export function hasQueuedJobs(jobCounts) {
  return (jobCounts?.pending ?? 0) > 0 || (jobCounts?.inProgress ?? 0) > 0;
}

// Pure idle predicate, shared by the server idle monitor and its unit tests.
// Idle = no queued/in-flight jobs AND no watched channels AND control API has
// been quiet longer than the configured window.
export function isIdle({ jobCounts, watchedCount, lastActivityAt, now, idleExitMs }) {
  if (!Number.isFinite(idleExitMs) || idleExitMs <= 0) {
    return false;
  }
  if (hasQueuedJobs(jobCounts)) {
    return false;
  }
  if ((watchedCount ?? 0) > 0) {
    return false;
  }
  return now - lastActivityAt > idleExitMs;
}

/**
 * Builds the request handler for the loopback control API. Every endpoint
 * delegates to the same MessageSyncService methods the MCP tools use, so there
 * is no duplicated job logic. The returned handler is meant to back a dedicated
 * http.Server bound to loopback.
 *
 * @param {object} deps
 * @param {object} deps.service       MessageSyncService (or compatible stub).
 * @param {object} [deps.warmServices] Warm { telegramClient, messageSyncService }
 *                                     the shared operation handlers run against
 *                                     for /control/invoke.
 * @param {string} deps.token         Secret required in the control token header.
 * @param {number} deps.pid           Server process id.
 * @param {string} deps.version       Server version string.
 * @param {string} deps.startedAt     ISO timestamp the server started.
 * @param {Function} [deps.onActivity] Called on every authed request (idle reset).
 * @param {Function} [deps.ensureLogin] Optional async hook run before enqueue.
 * @param {Function} [deps.onShutdown] Runs the server's graceful shutdown; called
 *                                     once the /control/shutdown reply is sent.
 * @param {Function} [deps.now]        Clock in ms, for tests.
 * @param {string} [deps.root]         Directory the server's code runs from.
 * @param {object} [deps.stats]       ServerStats the server counts into.
 *
 * The handler's inFlight() counts the requests it is still answering, other
 * than pings and shutdowns, so a shutdown can let them finish.
 */
export function createControlRequestHandler({
  service,
  warmServices,
  token,
  pid,
  version,
  startedAt,
  onActivity,
  ensureLogin,
  onShutdown,
  now = Date.now,
  root = null,
  stats,
}) {
  // The CLI that started this server ran its version, so the clock starts now.
  let ownClientAt = now();
  let inFlight = 0;

  // A chat given by username finds the jobs and chats under its id. These requests
  // were answered from the archive alone before, so the archive and the peer cache are
  // asked first, and when Telegram does not know the username, or does not answer, the
  // username itself is looked up as it is.
  async function chatKeyForLookup(chat) {
    if (chat === undefined || chat === null || isChatId(chat)) {
      return chat;
    }
    const local = await service.findChatKeyLocally(chat);
    if (local) {
      return local;
    }
    try {
      if (typeof ensureLogin === 'function') {
        await ensureLogin();
      }
      return await service.resolveChatKey(chat);
    } catch (error) {
      if (error?.code !== 'CHAT_NOT_FOUND') {
        console.error(`[control] Could not ask Telegram for ${chat}, looking it up in the archive: ${error?.message ?? error}`);
      }
      return chat;
    }
  }

  async function handleControlRequest(req, res) {
    let url;
    try {
      url = new URL(req.url ?? '', 'http://127.0.0.1');
    } catch (error) {
      sendJson(res, 400, { error: 'Bad request' });
      return;
    }

    if (!url.pathname.startsWith('/control/')) {
      sendJson(res, 404, { error: 'Not found' });
      return;
    }

    const provided = req.headers[CONTROL_TOKEN_HEADER];
    if (!provided || provided !== token) {
      sendJson(res, 401, { error: 'Unauthorized' });
      return;
    }

    if (req.headers[CONTROL_CLIENT_VERSION_HEADER] === version) {
      ownClientAt = now();
    }

    // Reading counters is no reason for an idle server to stay up, so this comes
    // before the activity hook.
    if (req.method === 'GET' && url.pathname === '/control/stats' && stats) {
      try {
        sendJson(res, 200, { pid, version, startedAt, ...stats.snapshot() });
      } catch (error) {
        sendJson(res, 500, { error: error?.message ?? 'Internal error' });
      }
      return;
    }

    if (typeof onActivity === 'function') {
      onActivity();
    }

    if (req.method === 'GET' && url.pathname === '/control/ping') {
      try {
        sendJson(res, 200, {
          ok: true,
          pid,
          version,
          startedAt,
          jobs: service.getJobCounts(),
          ownClientIdleMs: Math.max(0, now() - ownClientAt),
          inFlight,
          root,
        });
      } catch (error) {
        sendJson(res, 500, { error: error?.message ?? 'Internal error' });
      }
      return;
    }

    const counted = url.pathname !== '/control/shutdown';
    if (counted) {
      inFlight += 1;
    }
    try {
      await routeRequest(req, res, url);
    } finally {
      if (counted) {
        inFlight -= 1;
      }
    }
  }

  async function routeRequest(req, res, url) {
    try {
      if (req.method === 'POST' && url.pathname === '/control/shutdown') {
        if (typeof onShutdown !== 'function') {
          sendJson(res, 501, { error: 'Shutdown is not available on this server' });
          return;
        }
        // Reply first: the shutdown closes every control connection.
        res.once('finish', () => onShutdown());
        sendJson(res, 202, { ok: true, pid });
        return;
      }

      if (req.method === 'POST' && url.pathname === '/control/backfill') {
        const body = await readJsonBody(req);
        const chatId = body?.chatId;
        if (chatId === undefined || chatId === null || String(chatId).trim() === '') {
          sendJson(res, 400, { error: 'chatId is required' });
          return;
        }
        if (typeof ensureLogin === 'function') {
          await ensureLogin();
        }
        let channelId;
        try {
          channelId = await service.resolveChatKey(chatId);
        } catch (error) {
          if (error?.code === 'CHAT_NOT_FOUND') {
            sendJson(res, 400, { error: error.message });
            return;
          }
          throw error;
        }
        // Same path as the scheduleMessageSync MCP tool: enqueue then kick the
        // queue so processing starts without waiting for the next trigger.
        const job = service.addJob(channelId, { depth: body?.depth, minDate: body?.minDate });
        void service.processQueue();
        sendJson(res, 200, {
          jobId: job?.id ?? null,
          channelId: job?.channel_id ?? null,
          status: job?.status ?? null,
        });
        return;
      }

      // A dry run has a path of its own: a server that predates it answers 404 instead
      // of ignoring the flag and writing.
      if (req.method === 'POST' && (url.pathname === '/control/repair' || url.pathname === '/control/repair/dry-run')) {
        const dryRun = url.pathname === '/control/repair/dry-run';
        const body = await readJsonBody(req);
        const chats = Array.isArray(body?.chats) ? body.chats.filter((chat) => String(chat ?? '').trim() !== '') : [];
        if (!body?.all && !chats.length) {
          sendJson(res, 400, { error: 'Provide chats or all' });
          return;
        }
        const isBound = (value) => value === undefined || value === null || (Number.isInteger(value) && value >= 0);
        if (!isBound(body?.afterId) || !isBound(body?.sinceDate)) {
          sendJson(res, 400, { error: 'afterId and sinceDate must be non-negative integers' });
          return;
        }
        const chatKeys = [];
        for (const chat of chats) {
          chatKeys.push(await chatKeyForLookup(chat));
        }
        let channelIds;
        try {
          channelIds = service.addRepairs({
            chats: chatKeys,
            all: Boolean(body?.all),
            afterId: body?.afterId,
            sinceDate: body?.sinceDate,
            dryRun,
          });
        } catch (error) {
          sendJson(res, 400, { error: error?.message ?? 'Repair request failed' });
          return;
        }
        if (typeof ensureLogin === 'function') {
          await ensureLogin();
        }
        void service.processQueue();
        sendJson(res, 200, { channelIds });
        return;
      }

      if (req.method === 'POST' && url.pathname === '/control/invoke') {
        const body = await readJsonBody(req);
        const op = body?.op;
        const handler = typeof op === 'string' ? OPERATIONS[op] : undefined;
        if (!handler) {
          sendJson(res, 400, { error: `Unknown operation: ${op ?? ''}` });
          return;
        }
        // The warm client serves these operations, so make sure it is logged in
        // before the handler touches MTProto.
        if (typeof ensureLogin === 'function') {
          await ensureLogin();
        }
        try {
          const result = await handler(warmServices, body?.args ?? {});
          sendJson(res, 200, { result });
        } catch (error) {
          // A send op carries structured failure details; relay them so the CLI
          // renders the same human/JSON error a local send would.
          if (error instanceof SendCommandError) {
            sendJson(res, 500, { error: error.message, sendError: error.details });
          } else {
            sendJson(res, 500, { error: error?.message ?? 'Operation failed' });
          }
        }
        return;
      }

      if (req.method === 'POST' && url.pathname === '/control/retry') {
        const body = await readJsonBody(req);
        const jobId = body?.jobId;
        const channelId = body?.channelId;
        const allErrors = Boolean(body?.allErrors);
        if (
          !allErrors &&
          (jobId === undefined || jobId === null) &&
          (channelId === undefined || channelId === null || String(channelId).trim() === '')
        ) {
          sendJson(res, 400, { error: 'Provide jobId, channelId, or allErrors' });
          return;
        }
        // Same logic as `sync jobs retry`: reset the errored job(s) to pending,
        // then kick the queue so the server's worker picks them back up.
        const result = service.retryJobs({ jobId, channelId: await chatKeyForLookup(channelId), allErrors });
        if (result.updated > 0) {
          void service.processQueue();
        }
        sendJson(res, 200, {
          updated: result.updated,
          jobIds: result.jobIds,
        });
        return;
      }

      if (req.method === 'POST' && url.pathname === '/control/cancel') {
        const body = await readJsonBody(req);
        const chatId = body?.chatId;
        const jobId = body?.jobId;
        if (
          (chatId === undefined || chatId === null || String(chatId).trim() === '') &&
          (jobId === undefined || jobId === null)
        ) {
          sendJson(res, 400, { error: 'Provide chatId or jobId' });
          return;
        }
        // Same logic as `sync jobs cancel`.
        const result = service.cancelJobs({ channelId: await chatKeyForLookup(chatId), jobId });
        sendJson(res, 200, {
          canceled: result.canceled,
          jobIds: result.jobIds,
        });
        return;
      }

      sendJson(res, 404, { error: 'Not found' });
    } catch (error) {
      if (!res.headersSent) {
        sendJson(res, 500, { error: error?.message ?? 'Internal error' });
      }
    }
  }

  handleControlRequest.inFlight = () => inFlight;
  return handleControlRequest;
}
