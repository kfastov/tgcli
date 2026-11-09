import http from "http";
import fs from "fs";
import path from "path";
import { randomUUID } from "crypto";
import { fileURLToPath } from "url";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { isInitializeRequest } from "@modelcontextprotocol/sdk/types.js";

import { loadConfig, validateConfig } from "./core/config.js";
import { createServices } from "./core/services.js";
import { createTelegramConnector } from "./core/telegram-connection.js";
import { resolveStoreDir } from "./core/store.js";
import {
  createControlRequestHandler,
  generateControlToken,
  writeControlFile,
  removeControlFile,
  isIdle,
} from "./core/control-server.js";
import { ServerStats } from "./core/server-stats.js";
import { readVersion } from "./core/version.js";
import { captureServerOutput } from "./core/server-log.js";
import { readJsonBody } from "./core/http-util.js";
import { parseDuration } from "./core/duration.js";
import { OPERATION_LIST } from "./core/ops/index.js";
import { executeOperation } from "./core/ops/execute.js";
import { registerOperationTools } from "./core/ops/mcp.js";

const SERVICE_STATE_FILE = "service-state.json";

const storeDir = resolveStoreDir();
// `tgcli server` pipes this process's output into <store>/server.log itself, so
// that even a failure to load this module is logged. Run directly (`npm start`),
// the server writes the log on its own, from here on.
if (process.env.TGCLI_SERVER_LOG === "wrapper") {
  // If the wrapper dies, its pipes close; losing output must not kill the server.
  process.stdout.on("error", () => {});
  process.stderr.on("error", () => {});
} else {
  captureServerOutput(storeDir);
}
const { config, path: configPath } = loadConfig(storeDir);
const missingConfig = validateConfig(config ?? {});
if (missingConfig.length > 0) {
  console.error(`[startup] Missing tgcli configuration at ${configPath}. Run "tgcli auth".`);
  process.exit(1);
}
const mcpConfig = config?.mcp ?? {};
const mcpEnabled = Boolean(mcpConfig.enabled);
const resolvedHost = mcpConfig.host ?? process.env.MCP_HOST ?? process.env.FASTMCP_HOST ?? "127.0.0.1";
const resolvedPort = Number(mcpConfig.port ?? process.env.MCP_PORT ?? process.env.FASTMCP_PORT ?? "8080");
const HOST = resolvedHost;
const PORT = Number.isFinite(resolvedPort) && resolvedPort > 0 ? resolvedPort : 8080;

// Always-on loopback control API. Separate listener from MCP and not gated by
// mcp.enabled; bound to loopback only.
const controlConfig = config?.control ?? {};
const controlEnabled = controlConfig.enabled !== false;
const CONTROL_HOST = controlConfig.host ?? "127.0.0.1";
const controlPortRaw = Number(controlConfig.port ?? 8765);
const CONTROL_PORT = Number.isFinite(controlPortRaw) && controlPortRaw > 0 ? controlPortRaw : 8765;

// Idle-exit window (ms). Plumbed from the CLI `server --idle-exit <duration>`
// via argv or the TGCLI_IDLE_EXIT env var. When unset/zero the server stays up
// forever. See resolveIdleExitMs for the ms-vs-duration-string handling.
const IDLE_EXIT_MS = resolveIdleExitMs(readIdleExitArg());
// Set by ensureServer on the servers it starts, which a newer CLI may then replace.
// Any other server belongs to the user.
const AUTO_STARTED = process.argv.includes("--auto-started");
const IDLE_CHECK_INTERVAL_MS = 5000;
// How long a shutdown lets control requests already being answered finish.
const CONTROL_DRAIN_MS = 10000;
console.log(`[startup] tgcli server ${readVersion()} (pid ${process.pid}, ${AUTO_STARTED ? "auto-started" : "started by hand"}${IDLE_EXIT_MS > 0 ? `, idle exit after ${IDLE_EXIT_MS}ms` : ""})`);

const stats = new ServerStats();
const { telegramClient, messageSyncService } = createServices({ storeDir, config, stats });

const ensureTelegramConnected = createTelegramConnector({ telegramClient, messageSyncService });

let serviceState = null;
let controlToken = null;
let controlServer = null;
let controlHandler = null;
let controlFileWritten = false;
let idleTimer = null;
let lastControlActivityAt = Date.now();

function readIdleExitArg() {
  const args = process.argv.slice(2);
  const flagIndex = args.indexOf("--idle-exit");
  if (flagIndex !== -1 && args[flagIndex + 1]) {
    return args[flagIndex + 1];
  }
  const inline = args.find((arg) => arg.startsWith("--idle-exit="));
  if (inline) {
    return inline.slice("--idle-exit=".length);
  }
  return process.env.TGCLI_IDLE_EXIT ?? null;
}

// Resolve the idle-exit window to milliseconds. The CLI forwards `--idle-exit`
// as a plain integer of milliseconds (it has already parsed the user's duration
// via parseDuration), so a bare integer here is treated AS-IS as ms. A value
// carrying a unit suffix (e.g. "60s", "5m" from TGCLI_IDLE_EXIT) is parsed with
// the shared parseDuration (where a bare number would mean seconds, but that
// branch never applies here because bare integers are short-circuited above).
// This explicit split avoids the previous accidental divergence where the same
// bare number meant seconds in the CLI but milliseconds in the server.
function resolveIdleExitMs(value) {
  if (value === null || value === undefined || value === "") {
    return 0;
  }
  const raw = String(value).trim();
  if (/^\d+$/.test(raw)) {
    return Number(raw);
  }
  try {
    return parseDuration(raw) ?? 0;
  } catch (error) {
    return 0;
  }
}

function writeServiceState(nextState) {
  if (!nextState) {
    return;
  }
  try {
    fs.mkdirSync(storeDir, { recursive: true });
    fs.writeFileSync(
      path.join(storeDir, SERVICE_STATE_FILE),
      `${JSON.stringify(nextState, null, 2)}\n`,
      "utf8",
    );
  } catch (error) {
    console.error(`[startup] Failed to write service state: ${error?.message ?? error}`);
  }
}

function updateServiceState(patch) {
  if (!serviceState) {
    return;
  }
  serviceState = {
    ...serviceState,
    ...patch,
    updatedAt: new Date().toISOString(),
  };
  writeServiceState(serviceState);
}

/**
 * Represents an active MCP session – a transport plus its server instance.
 */
const sessions = new Map();
let shuttingDown = false;

function closeSessionRecord(record, context) {
  if (!record || record.closing) {
    return null;
  }
  record.closing = true;
  if (record.sessionId) {
    sessions.delete(record.sessionId);
  }
  if (record.transport?.close) {
    return record.transport.close().catch((error) => {
      console.error(`[server] error closing ${context}: ${error.message}`);
    });
  }
  return null;
}

function createServerInstance() {
  const server = new McpServer({
    name: "tgcli",
    version: readVersion(),
  });
  // The tools run the same operations, through the same entry, as the control API
  // the CLI uses, so a tool and its CLI command take the same args and return the
  // same payload.
  registerOperationTools(server, OPERATION_LIST, (op, args) => executeOperation(op, args, {
    services: { telegramClient, messageSyncService },
    ensureLogin: ensureTelegramConnected,
  }));
  return server;
}

async function ensureSession(req, res, body) {
  if (shuttingDown) {
    res.writeHead(503, { "Content-Type": "application/json" }).end(
      JSON.stringify({
        jsonrpc: "2.0",
        error: {
          code: -32000,
          message: "Server is shutting down",
        },
        id: null,
      }),
    );
    return null;
  }

  const sessionId = req.headers["mcp-session-id"];

  if (sessionId && typeof sessionId === "string") {
    const existing = sessions.get(sessionId);
    if (existing) {
      return existing;
    }

    res.writeHead(404, { "Content-Type": "application/json" }).end(
      JSON.stringify({
        jsonrpc: "2.0",
        error: {
          code: -32001,
          message: "Session not found",
        },
        id: null,
      }),
    );
    return null;
  }

  if (!isInitializeRequest(body)) {
    res.writeHead(400, { "Content-Type": "application/json" }).end(
      JSON.stringify({
        jsonrpc: "2.0",
        error: {
          code: -32000,
          message: "Bad Request: No valid session ID provided",
        },
        id: null,
      }),
    );
    return null;
  }

  const record = { server: null, transport: null, sessionId: null };

  const transport = new StreamableHTTPServerTransport({
    sessionIdGenerator: () => randomUUID(),
    onsessioninitialized: (sessionId) => {
      record.sessionId = sessionId;
      sessions.set(sessionId, record);
    },
    onsessionclosed: (sessionId) => {
      const existing = sessions.get(sessionId);
      if (existing) {
        existing.closing = true;
        sessions.delete(sessionId);
      }
    },
  });

  record.transport = transport;

  transport.onerror = (error) => {
    console.error(`[transport] error: ${error.message}`);
  };

  transport.onclose = () => {
    if (record.sessionId) {
      sessions.delete(record.sessionId);
    }
  };

  const serverInstance = createServerInstance();
  record.server = serverInstance;

  await serverInstance.connect(transport);

  return record;
}

async function handlePost(req, res) {
  const body = await readJsonBody(req);
  const sessionRecord = await ensureSession(req, res, body);
  if (!sessionRecord) {
    return;
  }

  try {
    await sessionRecord.transport.handleRequest(req, res, body);
  } catch (error) {
    console.error(`[http] POST handling failed: ${error?.message ?? error}`);
    if (!res.headersSent) {
      res.writeHead(500, { "Content-Type": "application/json" }).end(
        JSON.stringify({
          jsonrpc: "2.0",
          error: {
            code: -32603,
            message: "Internal server error",
          },
          id: null,
        }),
      );
    }
  }
}

async function handleSessionRequest(req, res) {
  if (shuttingDown) {
    res.writeHead(503, { "Content-Type": "application/json" }).end(
      JSON.stringify({
        jsonrpc: "2.0",
        error: {
          code: -32000,
          message: "Server is shutting down",
        },
        id: null,
      }),
    );
    return;
  }

  const sessionIdHeader = req.headers["mcp-session-id"];
  if (!sessionIdHeader || typeof sessionIdHeader !== "string") {
    res.writeHead(400, { "Content-Type": "application/json" }).end(
      JSON.stringify({
        jsonrpc: "2.0",
        error: {
          code: -32000,
          message: "Invalid or missing session ID",
        },
        id: null,
      }),
    );
    return;
  }

  const record = sessions.get(sessionIdHeader);
  if (!record) {
    res.writeHead(404, { "Content-Type": "application/json" }).end(
      JSON.stringify({
        jsonrpc: "2.0",
        error: {
          code: -32001,
          message: "Session not found",
        },
        id: null,
      }),
    );
    return;
  }

  await record.transport.handleRequest(req, res);
}

// TODO: MCP server should participate in the store locking protocol.
// Currently it opens the SQLite DB and Telegram session without any lock,
// which can cause conflicts with concurrent CLI commands.
//
// The listeners below start first so /control/ping succeeds and control.json is
// written within ~1s of spawn, independent of the Telegram connection. The
// connect runs in the background; each control handler's ensureLogin hook awaits
// the shared connect promise before touching MTProto, so the first operation —
// not the readiness probe — pays the connect cost.

serviceState = {
  pid: process.pid,
  version: readVersion(),
  manager: process.env.TGCLI_SERVICE_MANAGER ?? "manual",
  startedAt: new Date().toISOString(),
  mcpEnabled,
  mcpHost: mcpEnabled ? HOST : null,
  mcpPort: mcpEnabled ? PORT : null,
};
writeServiceState(serviceState);

let httpServer = null;
if (mcpEnabled) {
  httpServer = http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url ?? "", `http://${req.headers.host ?? `${HOST}:${PORT}`}`);

      if (req.method === "OPTIONS") {
        res.writeHead(204).end();
        return;
      }

      if (req.method === "GET" && url.pathname === "/health") {
        res.writeHead(200, { "Content-Type": "application/json" }).end(
          JSON.stringify({ status: "ok" }),
        );
        return;
      }

      if (req.method === "POST" && url.pathname === "/mcp") {
        await handlePost(req, res);
        return;
      }

      if ((req.method === "GET" || req.method === "DELETE") && url.pathname === "/mcp") {
        await handleSessionRequest(req, res);
        return;
      }

      if (req.method === "POST") {
        res.writeHead(404, { "Content-Type": "application/json" }).end(
          JSON.stringify({
            jsonrpc: "2.0",
            error: {
              code: -32601,
              message: "Endpoint not found",
            },
            id: null,
          }),
        );
        return;
      }

      res.writeHead(405, { Allow: "GET, POST, DELETE" }).end();
    } catch (error) {
      console.error(`[http] unexpected error: ${error?.message ?? error}`);
      if (!res.headersSent) {
        res.writeHead(500, { "Content-Type": "application/json" }).end(
          JSON.stringify({
            jsonrpc: "2.0",
            error: {
              code: -32603,
              message: "Internal server error",
            },
            id: null,
          }),
        );
      }
    }
  });

  httpServer.listen(PORT, HOST, () => {
    console.log(`[startup] MCP HTTP server listening on http://${HOST}:${PORT}/mcp`);
  });

  httpServer.on("error", (error) => {
    console.error(`[http] server error: ${error.message}`);
  });
} else {
  console.log("[startup] MCP disabled; running sync-only service.");
}

// --- Always-on loopback control API (independent of mcp.enabled) ---
if (controlEnabled) {
  controlToken = generateControlToken();
  const startedAt = serviceState.startedAt;
  const handleControlRequest = createControlRequestHandler({
    service: messageSyncService,
    warmServices: { telegramClient, messageSyncService },
    token: controlToken,
    pid: process.pid,
    version: serviceState.version,
    startedAt,
    onActivity: () => {
      lastControlActivityAt = Date.now();
    },
    // Await the shared connect before any op touches MTProto; the connect is
    // kicked off in the background once the listeners are up.
    ensureLogin: () => ensureTelegramConnected(),
    // `tgcli server stop` / `restart`.
    onShutdown: () => {
      void shutdown("stop requested over the control API").finally(() => process.exit(0));
    },
    root: path.dirname(fileURLToPath(import.meta.url)),
    stats,
  });
  controlHandler = handleControlRequest;

  controlServer = http.createServer((req, res) => {
    if (req.method === "OPTIONS") {
      res.writeHead(204).end();
      return;
    }
    void handleControlRequest(req, res);
  });

  controlServer.on("error", (error) => {
    console.error(`[control] server error: ${error.message}`);
    // Two CLIs raced to auto-start a server and the other one won. Staying up here
    // would leave a second writer that no CLI can reach or replace. A server started
    // by hand keeps running, since its other work does not need the control port.
    if (error.code === "EADDRINUSE" && AUTO_STARTED) {
      process.exit(1);
    }
  });

  controlServer.listen(CONTROL_PORT, CONTROL_HOST, () => {
    const address = controlServer.address();
    const boundPort = typeof address === "object" && address ? address.port : CONTROL_PORT;
    try {
      writeControlFile(storeDir, {
        pid: process.pid,
        port: boundPort,
        token: controlToken,
        startedAt,
        version: serviceState.version,
        autoStarted: AUTO_STARTED,
        manager: serviceState.manager,
      });
      controlFileWritten = true;
    } catch (error) {
      console.error(`[control] failed to write control.json: ${error?.message ?? error}`);
    }
    console.log(`[startup] control API listening on http://${CONTROL_HOST}:${boundPort}/control`);
  });

  updateServiceState({
    controlEnabled: true,
    controlHost: CONTROL_HOST,
    controlPort: CONTROL_PORT,
  });
}

// Warm the Telegram connection in the background now that the listeners are up
// and reachable. Operations await this same promise via ensureTelegramConnected,
// so a connect that is still in flight when the first op arrives is shared, not
// restarted. A failure here is logged; the next operation retries the connect.
void ensureTelegramConnected().catch((error) => {
  console.error(`[startup] Telegram connect failed: ${error?.message ?? error}`);
});

// --- Idle-exit monitor (active only when --idle-exit is configured) ---
if (IDLE_EXIT_MS > 0) {
  idleTimer = setInterval(() => {
    if (shuttingDown) {
      return;
    }
    const idle = isIdle({
      jobCounts: messageSyncService.getJobCounts(),
      watchedCount: messageSyncService.getWatchedChannelCount(),
      lastActivityAt: lastControlActivityAt,
      now: Date.now(),
      idleExitMs: IDLE_EXIT_MS,
    });
    if (idle) {
      void shutdown("idle window elapsed with no work").finally(() => process.exit(0));
    }
  }, IDLE_CHECK_INTERVAL_MS);
  if (typeof idleTimer.unref === "function") {
    idleTimer.unref();
  }
}

// Every caller gets the one shutdown in flight, so a second request (a signal
// after a control API stop, say) waits for it instead of exiting halfway.
let shutdownPromise = null;
function shutdown(reason) {
  shutdownPromise ??= runShutdown(reason);
  return shutdownPromise;
}

async function runShutdown(reason) {
  shuttingDown = true;
  console.log(`[shutdown] ${reason}; closing resources...`);
  const closeTasks = [];
  for (const record of sessions.values()) {
    const task = closeSessionRecord(record, "shutdown");
    if (task) {
      closeTasks.push(task);
    }
  }
  if (closeTasks.length) {
    await Promise.allSettled(closeTasks);
  }
  if (httpServer) {
    httpServer.closeAllConnections?.();
    httpServer.close(() => {
      console.log("[shutdown] HTTP server closed");
    });
  }

  if (idleTimer) {
    clearInterval(idleTimer);
    idleTimer = null;
  }

  if (controlServer) {
    // Take no new connections, let the requests other CLIs are waiting on finish,
    // then drop what is left.
    const closingServer = controlServer;
    controlServer = null;
    closingServer.close(() => {
      console.log("[shutdown] control server closed");
    });
    const drainDeadline = Date.now() + CONTROL_DRAIN_MS;
    while ((controlHandler?.inFlight() ?? 0) > 0 && Date.now() < drainDeadline) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    if ((controlHandler?.inFlight() ?? 0) > 0) {
      console.log(`[shutdown] dropping ${controlHandler.inFlight()} control request(s) still running`);
    }
    closingServer.closeAllConnections?.();
  }

  try {
    await messageSyncService.shutdown();
  } catch (error) {
    console.error(`[shutdown] error while stopping message sync: ${error?.message ?? error}`);
  }

  try {
    await telegramClient.destroy();
  } catch (error) {
    console.error(`[shutdown] error while closing Telegram client: ${error?.message ?? error}`);
  }

  // Last, so a CLI that arrives mid-shutdown can see this process and wait for it.
  if (controlFileWritten) {
    try {
      removeControlFile(storeDir, { pid: process.pid, token: controlToken });
    } catch (error) {
      console.error(`[shutdown] failed to remove control.json: ${error?.message ?? error}`);
    }
  }

  updateServiceState({
    stoppedAt: new Date().toISOString(),
    pid: null,
  });
  console.log("[shutdown] done");
}

const handleShutdownSignal = (signal) => {
  void shutdown(`received ${signal}`).finally(() => process.exit(0));
};

process.prependListener("SIGINT", handleShutdownSignal);
process.prependListener("SIGTERM", handleShutdownSignal);
