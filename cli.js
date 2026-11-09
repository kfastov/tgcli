#!/usr/bin/env node
import fs from 'fs';
import os from 'os';
import path from 'path';
import { spawn, spawnSync } from 'child_process';
import { setTimeout as delay } from 'timers/promises';
import { fileURLToPath, pathToFileURL } from 'url';
import readline from 'readline';
import { Command, Option } from 'commander';

import { acquireStoreLock, acquireReadLock, readStoreLock } from './store-lock.js';
import { loadConfig, normalizeConfig, saveConfig, validateConfig } from './core/config.js';
import { createServices, createTelegramClient } from './core/services.js';
import { withCommand, runWithTimeout, runOperation } from './core/command-context.js';
import { readVersion } from './core/version.js';
import {
  detectInstallMethod,
  formatDoctorUpdateLine,
  getUpdateCheckDisabledReason,
  getUpdateStatus,
  maybePrintUpdateNotice,
  scheduleUpdateCheck,
} from './core/update-check.js';
import { resolveStoreDir } from './core/store.js';
import { parseDuration } from './core/duration.js';
import { formatServerStats } from './core/server-stats.js';
import {
  cancelBackfill,
  enqueueBackfill,
  enqueueRepair,
  ensureServer,
  fetchServerStats,
  isManagedServer,
  pingServer,
  restartServer,
  retryBackfill,
  stopServer,
  syncServerVersion,
} from './core/control-client.js';
import { appendServerLog, followServerLog, openServerLog, readServerLogTail } from './core/server-log.js';
import { getGlobalFlags, withGlobalOptions, writeJson } from './core/cli-io.js';
import { addOperationCommands } from './core/ops/cli.js';
import { COMMAND_GROUPS, OPERATION_LIST } from './core/ops/index.js';

const CLI_PATH = fileURLToPath(import.meta.url);
const SERVICE_STATE_FILE = 'service-state.json';
const LAUNCHD_LABEL = 'com.kfastov.tgcli';
const SYSTEMD_SERVICE_NAME = 'tgcli';
// How far back `backfill repair` reads when given no bound.
const DEFAULT_REPAIR_SINCE = '30d';
const REPAIR_PROGRESS_INTERVAL_MS = 5000;
const AUTH_SYNC_HINT = 'Run `tgcli backfill --once` or `tgcli backfill --follow` when you need archive data.';
const CONFIG_SPECS = [
  { key: 'apiId', path: ['apiId'], type: 'number' },
  { key: 'apiHash', path: ['apiHash'], type: 'string', secret: true },
  { key: 'phoneNumber', path: ['phoneNumber'], type: 'string' },
  { key: 'feedback.chatId', path: ['feedback', 'chatId'], type: 'string' },
  { key: 'mcp.enabled', path: ['mcp', 'enabled'], type: 'boolean' },
  { key: 'mcp.host', path: ['mcp', 'host'], type: 'string' },
  { key: 'mcp.port', path: ['mcp', 'port'], type: 'number' },
  { key: 'control.enabled', path: ['control', 'enabled'], type: 'boolean' },
  { key: 'control.host', path: ['control', 'host'], type: 'string' },
  { key: 'control.port', path: ['control', 'port'], type: 'number' },
];

const FEEDBACK_LAST_FILE = 'feedback-last.json';
const FEEDBACK_COOLDOWN_MS = 60 * 1000;
const FEEDBACK_DEFAULT_CHAT_ID = '@kfastov';
const DEFAULT_SERVER_LOG_LINES = 100;

const CLI_PROGRAM = buildProgram();

function buildProgram() {
  const program = new Command();
  program
    .name('tgcli')
    .description('Telegram CLI + MCP server')
    .usage('[options] <command>')
    .option('--json', 'Machine-readable output')
    .option('--timeout <duration>', 'Wall-clock timeout (e.g. 30s, 5m)')
    .option('--quiet', 'Suppress progress/status output on stderr')
    .version(readVersion(), '--version', 'Print version and exit')
    .showHelpAfterError(true);

  const auth = program.command('auth').description('Authentication and session setup');
  auth
    .option('--force-sms', 'Force SMS code delivery instead of in-app')
    .option('--qr', 'Use QR-code login flow instead of confirmation code')
    .option('--qr-file <path>', 'Save QR code as PNG image file')
    .action(withGlobalOptions((globalFlags, options) =>
      runAuthLogin(globalFlags, options),
    ));
  auth
    .command('status')
    .description('Show auth status')
    .action(withGlobalOptions((globalFlags) => runAuthStatus(globalFlags)));
  auth
    .command('logout')
    .description('Log out of Telegram')
    .action(withGlobalOptions((globalFlags) => runAuthLogout(globalFlags)));

  const config = program.command('config').description('View and edit config');
  config
    .command('list')
    .description('List config values')
    .action(withGlobalOptions((globalFlags) => runConfigList(globalFlags)));
  config
    .command('get')
    .description('Get a config value')
    .argument('<key>', 'Config key')
    .action(withGlobalOptions((globalFlags, key) => runConfigGet(globalFlags, key)));
  config
    .command('set')
    .description('Set a config value')
    .argument('<key>', 'Config key')
    .argument('<value>', 'Config value')
    .action(withGlobalOptions((globalFlags, key, value) => runConfigSet(globalFlags, key, value)));
  config
    .command('unset')
    .description('Unset a config value')
    .argument('<key>', 'Config key')
    .action(withGlobalOptions((globalFlags, key) => runConfigUnset(globalFlags, key)));

  program
    .command('feedback')
    .description('Send feedback to the maintainer')
    .argument('<message...>', 'Feedback message')

    .action(withGlobalOptions((globalFlags, messageParts, options) =>
      runFeedback(globalFlags, messageParts, options),
    ));

  // `backfill` is the canonical name; `sync` stays as a silent alias so existing
  // invocations (and the subcommands below) keep working unchanged. Commander's
  // `.alias()` makes the alias resolve to the same command tree, so every nested
  // subcommand (`backfill jobs add`, `backfill status`, ...) is reachable under
  // both `backfill <...>` and `sync <...>`.
  const sync = program
    .command('backfill')
    .alias('sync')
    .description('Archive backfill and realtime sync');
  sync
    // `--chat` enqueues a single-chat backfill on the always-on server
    // (auto-starting it) and follows progress.
    .option('--chat <id|username>', 'Backfill a single chat via the control server')
    .option('--depth <n>', 'Maximum messages to backfill (with --chat)')
    .option('--min-date <iso>', 'Earliest date to backfill (with --chat)')
    .option('--background', 'Enqueue and return immediately (with --chat)')
    // Without --chat: drain the server's queue. --follow tracks progress to the
    // foreground; --once is a deprecated alias of `backfill wait`.
    .option('--follow', 'Track the backfill queue until it drains, then exit')
    .addOption(new Option('--once', 'Deprecated alias of `backfill wait`').hideHelp())
    .action(withGlobalOptions((globalFlags, options) => runBackfill(globalFlags, options)));
  sync
    .command('status')
    .description('Show backfill progress and server status')
    .action(withGlobalOptions((globalFlags) => runSyncStatus(globalFlags)));
  sync
    .command('count')
    .description('Print the number of in-progress backfills')
    .action(withGlobalOptions((globalFlags) => runBackfillCount(globalFlags)));
  sync
    .command('wait')
    .description('Start the server if needed and wait for backfills to drain')
    .action(withGlobalOptions((globalFlags) => runBackfillWait(globalFlags)));
  sync
    .command('cancel')
    .description('Cancel backfills for a chat')
    .option('--chat <id|username>', 'Channel identifier')
    // --chat is also declared on the parent `backfill`; commander binds the
    // duplicated flag to the parent, so merge via optsWithGlobals (and validate
    // presence inside the handler rather than with requiredOption).
    .action(withGlobalOptions((globalFlags, options, command) =>
      runBackfillCancel(globalFlags, command.optsWithGlobals()),
    ));
  sync
    .command('repair')
    .description('Fill holes in the archive from Telegram history (cursors stay put)')
    // --chat and --background are also declared on the parent `backfill`; read them
    // through optsWithGlobals, as `backfill cancel` does.
    .option('--chat <id|username>', 'Chat to repair')
    .option('--all', 'Repair every chat with sync enabled')
    .option('--since <date|duration>', `Lower bound: ISO date or duration such as 7d (default ${DEFAULT_REPAIR_SINCE})`)
    .option('--after-id <id>', 'Lower bound: only messages above this id (with --chat; no default date bound)')
    .option('--dry-run', 'Count what is missing without writing anything')
    .option('--background', 'Enqueue and return immediately')
    .action(withGlobalOptions((globalFlags, options, command) =>
      runBackfillRepair(globalFlags, command.optsWithGlobals()),
    ));
  const syncJobs = sync.command('jobs').description('Manage sync jobs');
  syncJobs
    .command('list')
    .description('List sync jobs')
    .option('--status <status>', 'Filter by status (pending|in_progress|idle|error)')
    .option('--limit <n>', 'Limit results')
    .option('--channel <id|username>', 'Filter by channel')
    .action(withGlobalOptions((globalFlags, options) => runSyncJobsList(globalFlags, options)));
  syncJobs
    .command('add')
    .description('Add a sync job')
    .option('--chat <id|username>', 'Channel identifier')
    .option('--depth <n>', 'Maximum messages to backfill')
    .option('--min-date <iso>', 'Earliest date to backfill')
    // optsWithGlobals merges in the parent `backfill` options. The parent now
    // declares --chat/--depth/--min-date too (for `backfill --chat`), and
    // commander binds those duplicated flags to the parent scope; merging here
    // keeps `backfill jobs add --chat X` working.
    .action(withGlobalOptions((globalFlags, options, command) =>
      runSyncJobsAdd(globalFlags, command.optsWithGlobals()),
    ));
  syncJobs
    .command('retry')
    .description('Retry failed jobs')
    .option('--job-id <n>', 'Retry by job id')
    .option('--channel <id|username>', 'Retry by channel')
    .option('--all-errors', 'Retry all error jobs')
    .action(withGlobalOptions((globalFlags, options) => runSyncJobsRetry(globalFlags, options)));
  syncJobs
    .command('cancel')
    .description('Cancel jobs')
    .option('--job-id <n>', 'Cancel by job id')
    .option('--channel <id|username>', 'Cancel by channel')
    .action(withGlobalOptions((globalFlags, options) => runSyncJobsCancel(globalFlags, options)));

  const server = program
    .command('server')
    .description('Run background sync service (MCP optional)')
    .option('--idle-exit <duration>', 'Exit after this idle period with no jobs or watched channels')
    .addOption(new Option('--auto-started').hideHelp())
    // `server stat` must not start a second server.
    .allowExcessArguments(false)
    .action(withGlobalOptions((globalFlags, options) => runServer(globalFlags, options)));
  server
    .command('stop')
    .description('Stop the running server and wait until it has exited')
    .option('--force', 'Kill the server if it has not exited after a minute')
    .action(withGlobalOptions((globalFlags, options) => runServerStop(globalFlags, options)));
  server
    .command('restart')
    .description("Restart the server on this CLI's version")
    .option('--force', 'Kill the old server if it has not exited after a minute')
    .action(withGlobalOptions((globalFlags, options) => runServerRestart(globalFlags, options)));
  server
    .command('logs')
    .description('Show the end of the server log')
    .option('-n, --lines <n>', `Number of lines to show (default ${DEFAULT_SERVER_LOG_LINES})`)
    .option('--follow', 'Keep printing lines as the server writes them')
    .action(withGlobalOptions((globalFlags, options) => runServerLogs(globalFlags, options)));
  server
    .command('stats')
    .description('Show what the running server asked Telegram and did, since start and over the last hour')
    .action(withGlobalOptions((globalFlags) => runServerStats(globalFlags)));

  const service = program.command('service').description('Manage background service');
  service
    .command('install')
    .description('Install service definition')
    .action(withGlobalOptions((globalFlags) => runServiceInstall(globalFlags)));
  service
    .command('start')
    .description('Start service')
    .action(withGlobalOptions((globalFlags) => runServiceStart(globalFlags)));
  service
    .command('stop')
    .description('Stop service')
    .action(withGlobalOptions((globalFlags) => runServiceStop(globalFlags)));
  service
    .command('status')
    .description('Show service status')
    .action(withGlobalOptions((globalFlags) => runServiceStatus(globalFlags)));
  service
    .command('logs')
    .description('Show service logs')
    .action(withGlobalOptions((globalFlags) => runServiceLogs(globalFlags)));

  program
    .command('doctor')
    .description('Diagnostics and sanity checks')
    .option('--connect', 'Connect to Telegram for live checks')
    .action(withGlobalOptions((globalFlags, options) => runDoctor(globalFlags, options)));

  addOperationCommands(program, { operations: OPERATION_LIST, groups: COMMAND_GROUPS, invoke: runOperation });

  disableHelpCommand(program);
  program.addHelpText('after', '\nUse "tgcli [command] --help" for more information about a command.');
  program.action(() => {
    program.help();
  });
  program.hook('preAction', (_program, actionCommand) => keepServerOnThisVersion(actionCommand));
  return program;
}

// `server` and `service` commands start, stop and replace servers themselves.
function managesServerItself(command) {
  for (let current = command; current?.parent; current = current.parent) {
    if (current.name() === 'server' || current.name() === 'service') {
      return true;
    }
  }
  return !command?.parent;
}

// Before any command runs, a server on another version is replaced by this one
// (see syncServerVersion). A failed check never blocks the command itself, and
// --timeout bounds it like the command.
async function keepServerOnThisVersion(actionCommand) {
  if (managesServerItself(actionCommand)) {
    return;
  }
  let globalFlags;
  try {
    globalFlags = getGlobalFlags(actionCommand);
  } catch {
    // The command reports its own bad flags.
    return;
  }
  const { quiet } = globalFlags;
  try {
    await runWithTimeout(
      () => syncServerVersion(resolveStoreDir(), { quiet }),
      globalFlags.timeoutMs,
      undefined,
      `timed out after ${globalFlags.timeout}`,
    );
  } catch (error) {
    if (!quiet) {
      process.stderr.write(`Warning: could not bring tgcli server to this version: ${error?.message ?? error}\n`);
    }
  }
}

function disableHelpCommand(command) {
  command.addHelpCommand(false);
  for (const subcommand of command.commands) {
    disableHelpCommand(subcommand);
  }
}

function resolveConfigSpec(key) {
  if (typeof key !== 'string' || !key.trim()) {
    throw new Error('Config key is required.');
  }
  const normalized = key.trim().toLowerCase();
  const spec = CONFIG_SPECS.find((entry) => entry.key.toLowerCase() === normalized);
  if (!spec) {
    const allowed = CONFIG_SPECS.map((entry) => entry.key).join(', ');
    throw new Error(`Unknown config key "${key}". Supported keys: ${allowed}.`);
  }
  return spec;
}

function normalizeOutputValue(value) {
  if (value === undefined || value === null) {
    return null;
  }
  if (typeof value === 'string' && !value.trim()) {
    return null;
  }
  return value;
}

function maskSecret(value) {
  if (value === null || value === undefined) {
    return null;
  }
  const str = String(value);
  if (str.length <= 4) {
    return '****';
  }
  return `${'*'.repeat(str.length - 4)}${str.slice(-4)}`;
}

function formatConfigValue(value) {
  if (value === null || value === undefined) {
    return 'unset';
  }
  if (typeof value === 'boolean') {
    return value ? 'true' : 'false';
  }
  return String(value);
}

function getValueAtPath(target, pathParts) {
  let current = target;
  for (const part of pathParts) {
    if (!current || typeof current !== 'object' || !(part in current)) {
      return undefined;
    }
    current = current[part];
  }
  return current;
}

function setValueAtPath(target, pathParts, value) {
  let current = target;
  for (let index = 0; index < pathParts.length - 1; index += 1) {
    const part = pathParts[index];
    if (!current[part] || typeof current[part] !== 'object') {
      current[part] = {};
    }
    current = current[part];
  }
  current[pathParts[pathParts.length - 1]] = value;
}

function deleteValueAtPath(target, pathParts) {
  let current = target;
  for (let index = 0; index < pathParts.length - 1; index += 1) {
    const part = pathParts[index];
    if (!current || typeof current !== 'object') {
      return;
    }
    current = current[part];
  }
  if (current && typeof current === 'object') {
    delete current[pathParts[pathParts.length - 1]];
  }
}

function parseBooleanValue(value) {
  if (typeof value === 'boolean') {
    return value;
  }
  if (typeof value === 'number') {
    return value !== 0;
  }
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (['true', '1', 'yes', 'y', 'on'].includes(normalized)) {
      return true;
    }
    if (['false', '0', 'no', 'n', 'off'].includes(normalized)) {
      return false;
    }
  }
  throw new Error('Value must be boolean (true/false).');
}

function parseNumberValue(value, label) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${label} must be a positive number.`);
  }
  return parsed;
}

function parseStringValue(value, label) {
  if (value === undefined || value === null) {
    throw new Error(`${label} is required.`);
  }
  const trimmed = String(value).trim();
  if (!trimmed) {
    throw new Error(`${label} must not be empty.`);
  }
  return trimmed;
}

function parseConfigValue(spec, rawValue) {
  if (spec.type === 'boolean') {
    return parseBooleanValue(rawValue);
  }
  if (spec.type === 'number') {
    return parseNumberValue(rawValue, spec.key);
  }
  return parseStringValue(rawValue, spec.key);
}



function getFeedbackRateLimitPath(storeDir) {
  return path.join(storeDir, FEEDBACK_LAST_FILE);
}

function readFeedbackLastSentAt(storeDir) {
  try {
    const raw = JSON.parse(fs.readFileSync(getFeedbackRateLimitPath(storeDir), 'utf8'));
    const timestamp = Number(raw?.lastFeedbackAt);
    return Number.isFinite(timestamp) && timestamp > 0 ? timestamp : null;
  } catch (error) {
    if (error?.code === 'ENOENT' || error instanceof SyntaxError) {
      return null;
    }
    throw error;
  }
}

function getFeedbackCooldownRemainingSeconds(storeDir, now = Date.now()) {
  const lastFeedbackAt = readFeedbackLastSentAt(storeDir);
  if (!lastFeedbackAt) {
    return 0;
  }
  const remainingMs = lastFeedbackAt + FEEDBACK_COOLDOWN_MS - now;
  return remainingMs > 0 ? Math.ceil(remainingMs / 1000) : 0;
}

function writeFeedbackLastSentAt(storeDir, timestamp = Date.now()) {
  const payload = { lastFeedbackAt: timestamp };
  fs.mkdirSync(storeDir, { recursive: true });
  fs.writeFileSync(
    getFeedbackRateLimitPath(storeDir),
    `${JSON.stringify(payload, null, 2)}\n`,
    'utf8',
  );
  return timestamp;
}

function formatFeedbackMessage(messageText, metadata = {}) {
  const version = metadata.version ?? readVersion();
  const platform = metadata.platform ?? process.platform;
  const nodeVersion = metadata.nodeVersion ?? process.version;
  const normalizedMessage = parseStringValue(messageText, 'Feedback message');
  return `💬 tgcli feedback\n\n${normalizedMessage}\n\n---\ntgcli v${version} | ${platform} | ${nodeVersion}`;
}

function runCommand(command, args, options = {}) {
  const result = spawnSync(command, args, {
    encoding: 'utf8',
    ...options,
  });
  return {
    status: result.status ?? 1,
    stdout: result.stdout ?? '',
    stderr: result.stderr ?? '',
    error: result.error ?? null,
  };
}

function getServiceStatePath(storeDir) {
  return path.join(storeDir, SERVICE_STATE_FILE);
}

function readServiceState(storeDir) {
  try {
    const raw = fs.readFileSync(getServiceStatePath(storeDir), 'utf8');
    return JSON.parse(raw);
  } catch (error) {
    return null;
  }
}

function getLaunchdPaths() {
  const baseDir = path.join(os.homedir(), 'Library', 'LaunchAgents');
  return {
    plistPath: path.join(baseDir, `${LAUNCHD_LABEL}.plist`),
    logPath: path.join(os.homedir(), 'Library', 'Logs', 'tgcli.log'),
    errorLogPath: path.join(os.homedir(), 'Library', 'Logs', 'tgcli.error.log'),
  };
}

function getSystemdPath() {
  return path.join(os.homedir(), '.config', 'systemd', 'user', `${SYSTEMD_SERVICE_NAME}.service`);
}

function xmlEscape(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function buildLaunchdPlist({ nodePath, cliPath, envVars, logPath, errorLogPath }) {
  const envEntries = Object.entries(envVars || {})
    .map(([key, value]) => `    <key>${xmlEscape(key)}</key>\n    <string>${xmlEscape(value)}</string>`)
    .join('\n');
  const envBlock = envEntries
    ? `  <key>EnvironmentVariables</key>\n  <dict>\n${envEntries}\n  </dict>\n`
    : '';

  return [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">',
    '<plist version="1.0">',
    '<dict>',
    `  <key>Label</key>`,
    `  <string>${LAUNCHD_LABEL}</string>`,
    '  <key>ProgramArguments</key>',
    '  <array>',
    `    <string>${xmlEscape(nodePath)}</string>`,
    `    <string>${xmlEscape(cliPath)}</string>`,
    '    <string>server</string>',
    '  </array>',
    envBlock.trimEnd(),
    '  <key>RunAtLoad</key>',
    '  <true/>',
    '  <key>KeepAlive</key>',
    '  <true/>',
    `  <key>StandardOutPath</key>`,
    `  <string>${xmlEscape(logPath)}</string>`,
    `  <key>StandardErrorPath</key>`,
    `  <string>${xmlEscape(errorLogPath)}</string>`,
    '</dict>',
    '</plist>',
    '',
  ]
    .filter((line) => line !== '')
    .join('\n');
}

function buildSystemdService({ nodePath, cliPath, envVars }) {
  const envLines = Object.entries(envVars || {}).map(
    ([key, value]) => `Environment=${key}=${JSON.stringify(String(value))}`,
  );
  return [
    '[Unit]',
    'Description=tgcli background service',
    'After=network-online.target',
    '',
    '[Service]',
    `ExecStart=${nodePath} ${cliPath} server`,
    'Restart=on-failure',
    ...envLines,
    '',
    '[Install]',
    'WantedBy=default.target',
    '',
  ].join('\n');
}

function parseBrewServicesList(output) {
  const lines = output.split('\n').slice(1);
  for (const line of lines) {
    if (!line.trim()) continue;
    const [name, status] = line.trim().split(/\s+/);
    if (name === 'tgcli') {
      return { status };
    }
  }
  return null;
}

function detectBrewService() {
  const brewCheck = runCommand('brew', ['--version']);
  if (brewCheck.status !== 0) {
    return { available: false };
  }
  const list = runCommand('brew', ['list', '--formula', 'tgcli']);
  if (list.status !== 0) {
    return { available: true, installed: false };
  }
  const prefixResult = runCommand('brew', ['--prefix', 'tgcli']);
  const brewPrefix = prefixResult.status === 0 ? prefixResult.stdout.trim() : null;
  const cliPath = fs.realpathSync(CLI_PATH);
  const brewCliMatch = brewPrefix ? cliPath.startsWith(path.join(brewPrefix, 'libexec')) : false;

  const servicesResult = runCommand('brew', ['services', 'list']);
  const serviceEntry = servicesResult.status === 0 ? parseBrewServicesList(servicesResult.stdout) : null;
  const serviceAvailable = Boolean(serviceEntry);

  return {
    available: true,
    installed: true,
    brewPrefix,
    brewCliMatch,
    serviceAvailable,
    serviceStatus: serviceEntry?.status ?? null,
  };
}

function resolveServiceManager() {
  const brewInfo = detectBrewService();
  if (brewInfo.available && brewInfo.installed && brewInfo.serviceAvailable) {
    return { manager: 'brew', brewInfo };
  }
  if (process.platform === 'darwin') {
    return { manager: 'launchd', brewInfo };
  }
  if (process.platform === 'linux') {
    const systemctlCheck = runCommand('systemctl', ['--user', '--version']);
    if (systemctlCheck.status !== 0) {
      return { manager: 'unsupported', brewInfo };
    }
    return { manager: 'systemd', brewInfo };
  }
  return { manager: 'unsupported', brewInfo };
}

function promptInput(question) {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });
  return new Promise(resolve => {
    rl.question(question, answer => {
      rl.close();
      resolve(answer.trim());
    });
  });
}

function getStoreConfig(storeDir) {
  const { config } = loadConfig(storeDir);
  const normalized = normalizeConfig(config ?? {});
  const missing = validateConfig(normalized);
  return { config: normalized, missing };
}

async function ensureStoreConfig(storeDir) {
  const { config, missing } = getStoreConfig(storeDir);
  if (missing.length === 0) {
    return config;
  }

  const updated = { ...config };
  if (!updated.apiId) {
    updated.apiId = await promptInput('Telegram API ID: ');
  }
  if (!updated.apiHash) {
    updated.apiHash = await promptInput('Telegram API hash: ');
  }
  if (!updated.phoneNumber) {
    updated.phoneNumber = await promptInput('Telegram phone number (+...): ');
  }

  const normalized = normalizeConfig(updated);
  const remaining = validateConfig(normalized);
  if (remaining.length > 0) {
    throw new Error('Missing tgcli configuration. Run "tgcli auth" to set credentials.');
  }
  saveConfig(storeDir, normalized);
  return normalized;
}

function parsePositiveInt(value, label) {
  if (value === undefined || value === null || value === '') {
    return null;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${label} must be a positive number`);
  }
  return parsed;
}

async function runAuthStatus(globalFlags) {
  const timeoutMs = globalFlags.timeoutMs;
  return runWithTimeout(async () => {
    const storeDir = resolveStoreDir();
    const { config, missing } = getStoreConfig(storeDir);
    if (missing.length > 0) {
      if (globalFlags.json) {
        writeJson({ authenticated: false, configured: false });
      } else {
        console.log('Not authenticated. Run `tgcli auth`.');
      }
      return;
    }
    const { telegramClient, messageSyncService } = createServices({ storeDir, config });
    try {
      const me = await telegramClient.getCurrentUser();
      const authenticated = Boolean(me);
      const search = messageSyncService.getSearchStatus();
      const username = me?.username ? `@${me.username}` : null;
      const payload = {
        authenticated,
        configured: true,
        phoneNumber: config.phoneNumber || null,
        username: me?.username ?? null,
        ftsEnabled: search.enabled,
      };
      if (globalFlags.json) {
        writeJson(payload);
      } else {
        if (!authenticated) {
          console.log('Not authenticated. Run `tgcli auth`.');
        } else if (username) {
          console.log(`Authenticated as ${config.phoneNumber} (${username}).`);
        } else {
          console.log(`Authenticated as ${config.phoneNumber}.`);
        }
      }
    } finally {
      await messageSyncService.close();
      await telegramClient.destroy();
    }
  }, timeoutMs);
}

async function runAuthLogout(globalFlags) {
  const timeoutMs = globalFlags.timeoutMs;
  return runWithTimeout(async () => {
    const storeDir = resolveStoreDir();
    const { missing } = getStoreConfig(storeDir);
    if (missing.length > 0) {
      if (globalFlags.json) {
        writeJson({ loggedOut: false, configured: false });
      } else {
        console.log('Not authenticated.');
      }
      return;
    }
    const release = acquireStoreLock(storeDir);
    const config = await ensureStoreConfig(storeDir);
    const { telegramClient, messageSyncService } = createServices({ storeDir, config });
    try {
      await telegramClient.login();
      await telegramClient.client.logout();
      if (globalFlags.json) {
        writeJson({ loggedOut: true });
      } else {
        console.log('Logged out.');
      }
    } finally {
      await messageSyncService.close();
      await telegramClient.destroy();
      release();
    }
  }, timeoutMs);
}

// Interactive login + session bootstrap only: write the session and exit. Archive
// sync and the queue run in the always-on server, which auto-starts on the next
// command that needs it (or via `tgcli server`).
export async function runAuthLogin(globalFlags, options = {}) {
  const timeoutMs = globalFlags.timeoutMs;
  let release = null;
  let telegramClient = null;
  const cleanup = async () => {
    if (telegramClient) {
      const currentClient = telegramClient;
      telegramClient = null;
      await currentClient.destroy();
    }
    if (release) {
      const currentRelease = release;
      release = null;
      currentRelease();
    }
  };
  return runWithTimeout(async () => {
    const storeDir = resolveStoreDir();
    const config = await ensureStoreConfig(storeDir);
    release = acquireStoreLock(storeDir);
    try {
      ({ telegramClient } = createTelegramClient({
        storeDir,
        config,
        forceSms: options.forceSms,
        useQr: options.qr,
        qrFilePath: options.qrFile,
        json: globalFlags.json,
        // QR login is completed by mtcute after it receives updateLoginToken.
        // Keep updates disabled for the regular code flow, but enable them for QR.
        disableUpdates: !options.qr,
      }));
      const loginSuccess = await telegramClient.login();
      if (!loginSuccess) {
        throw new Error('Failed to login to Telegram.');
      }

      if (globalFlags.json) {
        writeJson({ authenticated: true });
      } else {
        console.log(`Authenticated. ${AUTH_SYNC_HINT}`);
      }
    } finally {
      await cleanup();
    }
  }, timeoutMs, cleanup);
}

async function runConfigList(globalFlags) {
  const storeDir = resolveStoreDir();
  const { config } = loadConfig(storeDir);
  const normalized = normalizeConfig(config ?? {});
  const payload = {};
  for (const spec of CONFIG_SPECS) {
    let value = normalizeOutputValue(getValueAtPath(normalized, spec.path));
    if (spec.secret) {
      value = maskSecret(value);
    }
    payload[spec.key] = value;
  }
  if (globalFlags.json) {
    writeJson(payload);
    return;
  }
  for (const [key, value] of Object.entries(payload)) {
    console.log(`${key}: ${formatConfigValue(value)}`);
  }
}

async function runConfigGet(globalFlags, key) {
  const storeDir = resolveStoreDir();
  const spec = resolveConfigSpec(key);
  const { config } = loadConfig(storeDir);
  const normalized = normalizeConfig(config ?? {});
  const value = normalizeOutputValue(getValueAtPath(normalized, spec.path));
  if (globalFlags.json) {
    writeJson({ key: spec.key, value });
    return;
  }
  console.log(`${spec.key}: ${formatConfigValue(value)}`);
}

async function runConfigSet(globalFlags, key, value) {
  const storeDir = resolveStoreDir();
  const spec = resolveConfigSpec(key);
  const parsedValue = parseConfigValue(spec, value);
  const { config } = loadConfig(storeDir);
  const next = normalizeConfig(config ?? {});
  setValueAtPath(next, spec.path, parsedValue);
  const { config: saved } = saveConfig(storeDir, next);
  const storedValue = normalizeOutputValue(getValueAtPath(saved, spec.path));
  if (globalFlags.json) {
    writeJson({ ok: true, key: spec.key, value: storedValue });
    return;
  }
  console.log(`Updated ${spec.key}: ${formatConfigValue(storedValue)}`);
}

async function runConfigUnset(globalFlags, key) {
  const storeDir = resolveStoreDir();
  const spec = resolveConfigSpec(key);
  const { config } = loadConfig(storeDir);
  if (!config) {
    if (globalFlags.json) {
      writeJson({ ok: true, key: spec.key, value: null });
    } else {
      console.log(`${spec.key} already unset.`);
    }
    return;
  }
  const next = normalizeConfig(config ?? {});
  deleteValueAtPath(next, spec.path);
  const { config: saved } = saveConfig(storeDir, next);
  const storedValue = normalizeOutputValue(getValueAtPath(saved, spec.path));
  if (globalFlags.json) {
    writeJson({ ok: true, key: spec.key, value: storedValue });
    return;
  }
  console.log(`Cleared ${spec.key}.`);
}

async function runFeedback(globalFlags, messageParts, options = {}) {
  const timeoutMs = globalFlags.timeoutMs;
  return runWithTimeout(async () => {
    const storeDir = resolveStoreDir();
    const release = acquireStoreLock(storeDir);
    let telegramClient = null;
    let messageSyncService = null;

    try {
      const { config } = loadConfig(storeDir);
      const normalizedConfig = normalizeConfig(config ?? {});
      const chatId = normalizedConfig.feedback?.chatId || FEEDBACK_DEFAULT_CHAT_ID;

            const remainingSeconds = getFeedbackCooldownRemainingSeconds(storeDir);
      if (remainingSeconds > 0) {
        throw new Error(`Please wait ${remainingSeconds} seconds before sending another feedback.`);
      }

      ({ telegramClient, messageSyncService } = createServices({
        storeDir,
        config: normalizedConfig,
      }));

      if (!(await telegramClient.isAuthorized().catch(() => false))) {
        throw new Error('Not authenticated. Run `tgcli auth` first.');
      }

      const feedbackText = formatFeedbackMessage(
        Array.isArray(messageParts) ? messageParts.join(' ') : String(messageParts ?? ''),
      );
      const result = await telegramClient.sendTextMessage(chatId, feedbackText, {
        parseMode: 'none',
      });
      writeFeedbackLastSentAt(storeDir);

      if (globalFlags.json) {
        writeJson({
          ok: true,
          messageId: result?.messageId ?? null,
        });
      } else {
        console.log(`Feedback sent (${result?.messageId ?? 'unknown'}).`);
      }
    } finally {
      if (messageSyncService) {
        await messageSyncService.close();
      }
      if (telegramClient) {
        await telegramClient.destroy();
      }
      release();
    }
  }, timeoutMs);
}

// A backfill job is terminal once the worker stops touching it: `idle` means the
// backfill reached its target/exhausted history; `error` means it failed.
const TERMINAL_JOB_STATUSES = new Set(['idle', 'error']);
// A skipped repair is one Telegram refuses for good, such as in a chat the account left.
const TERMINAL_REPAIR_STATUSES = new Set([...TERMINAL_JOB_STATUSES, 'skipped']);

// Briefly take a read lock, read job state, then release — never held across a
// poll loop. Holding it for the whole wait would block CLI writers like `send`,
// which need the write lock. Returns whatever the reader produces.
//
// We deliberately close the DB handle directly rather than calling
// service.shutdown(): shutdown() rewrites in_progress jobs back to pending, which
// would corrupt the job state owned by the running server we are merely observing.
function withReadSnapshot(storeDir, reader) {
  const release = acquireReadLock(storeDir);
  let service;
  let telegramClient;
  try {
    ({ telegramClient, messageSyncService: service } = createServices({ storeDir }));
    return reader(service);
  } finally {
    if (service?.db?.open) {
      service.db.close();
    }
    if (telegramClient) {
      void telegramClient.destroy?.();
    }
    release();
  }
}

function jobProgressLine(job) {
  const label = job.peer_title || job.channel_id;
  const done = job.message_count ?? 0;
  const target = job.target_message_count ?? 0;
  const pct = target > 0 ? Math.min(100, Math.round((done / target) * 100)) : 0;
  return `Backfilling ${label}: ${done}/${target} (${pct}%)`;
}

function writeProgress(globalFlags, line) {
  if (!globalFlags.quiet) {
    process.stderr.write(`${line}\n`);
  }
}

function ensureAutoStartedServer(globalFlags, storeDir) {
  return ensureServer(storeDir, { quiet: globalFlags.quiet });
}

// With `--chat`, enqueue a single-chat backfill on the always-on control server
// (auto-starting it) and either return the job id (`--background`) or follow
// progress in the foreground. Without `--chat`, delegate to the queue-draining
// path (`--follow`/`--once`/bare usage).
async function runBackfill(globalFlags, options = {}) {
  if (!options.chat) {
    return runSync(globalFlags, options);
  }

  // Long-running command: do NOT apply the send default timeout. A user-supplied
  // --timeout is still honored via runWithTimeout below.
  return runWithTimeout(async () => {
    const storeDir = resolveStoreDir();
    const depth = parsePositiveInt(options.depth, '--depth');
    const minDate = options.minDate ?? null;

    await ensureAutoStartedServer(globalFlags, storeDir);
    const enqueued = await enqueueBackfill(storeDir, {
      chatId: options.chat,
      depth,
      minDate,
    });
    const channelId = enqueued?.channelId ?? null;
    const jobId = enqueued?.jobId ?? null;

    if (options.background) {
      if (globalFlags.json) {
        writeJson({ jobId, channelId, status: enqueued?.status ?? null });
      } else {
        console.log(`Queued backfill for ${channelId} (job #${jobId}).`);
      }
      return;
    }

    // Ctrl-C detaches instead of cancelling: the server keeps draining the queue.
    let detached = false;
    const onSigint = () => {
      detached = true;
      writeProgress(globalFlags, 'Still running in the background — check `tgcli backfill status`');
      process.exit(0);
    };
    process.on('SIGINT', onSigint);

    try {
      let job = null;
      while (!detached) {
        job = withReadSnapshot(storeDir, (service) => {
          const jobs = service.listJobs({ channelId, limit: 1 });
          return jobs[0] ?? null;
        });
        if (!job) {
          // Job already gone (cancelled or never persisted) — nothing to follow.
          break;
        }
        writeProgress(globalFlags, jobProgressLine(job));
        if (TERMINAL_JOB_STATUSES.has(job.status)) {
          break;
        }
        await delay(1000);
      }

      if (detached) {
        return;
      }

      if (job && job.status === 'error') {
        if (globalFlags.json) {
          writeJson({ ok: false, jobId: job.id, channelId, status: job.status, error: job.error ?? null });
        } else {
          console.error(`Backfill failed for ${channelId}: ${job.error ?? 'unknown error'}`);
        }
        process.exitCode = 1;
        return;
      }

      if (globalFlags.json) {
        writeJson({
          ok: true,
          jobId: job?.id ?? jobId,
          channelId,
          status: job?.status ?? null,
          messageCount: job?.message_count ?? 0,
          targetMessageCount: job?.target_message_count ?? 0,
        });
      } else if (job) {
        console.log(`Backfill complete for ${channelId}: ${job.message_count ?? 0} message(s) archived.`);
      } else {
        console.log(`Backfill for ${channelId} is no longer queued.`);
      }
    } finally {
      process.off('SIGINT', onSigint);
    }
  }, globalFlags.timeoutMs);
}

// Snapshot of in-progress + pending backfills plus whether the server is up.
async function runBackfillCount(globalFlags) {
  return runWithTimeout(async () => {
    const storeDir = resolveStoreDir();
    const count = withReadSnapshot(storeDir, (service) => service.getJobCounts().inProgress);
    if (globalFlags.json) {
      writeJson({ inProgress: count });
    } else {
      console.log(String(count));
    }
  }, globalFlags.timeoutMs);
}

// Start the server if needed (so the queue actually drains) and poll under brief
// read snapshots until no pending/in-progress backfills remain, writing per-chat
// progress to stderr along the way. The server keeps running on its own (realtime
// continues while it is up / has watched chats); this only tracks the queue.
async function drainBackfillQueue(globalFlags, storeDir) {
  await ensureAutoStartedServer(globalFlags, storeDir);
  while (true) {
    const { active, jobs } = withReadSnapshot(storeDir, (service) => {
      const counts = service.getJobCounts();
      const inFlight = service.listJobs({ status: 'in_progress', limit: 50 });
      return { active: counts.pending + counts.inProgress, jobs: inFlight };
    });
    for (const job of jobs) {
      writeProgress(globalFlags, jobProgressLine(job));
    }
    if (active === 0) {
      break;
    }
    await delay(1000);
  }
}

// `backfill wait`: start the server if needed and track the queue to completion.
async function runBackfillWait(globalFlags) {
  return runWithTimeout(async () => {
    const storeDir = resolveStoreDir();
    await drainBackfillQueue(globalFlags, storeDir);
    if (globalFlags.json) {
      writeJson({ ok: true, drained: true });
    } else if (!globalFlags.quiet) {
      console.log('Backfill queue drained.');
    }
  }, globalFlags.timeoutMs);
}

// Cancel backfills for a chat through the control server, auto-starting it when
// needed so all queue writes go through the sole writer.
async function runBackfillCancel(globalFlags, options = {}) {
  return runWithTimeout(async () => {
    if (!options.chat) {
      throw new Error('--chat is required');
    }
    const storeDir = resolveStoreDir();
    await ensureAutoStartedServer(globalFlags, storeDir);
    const result = await cancelBackfill(storeDir, { chatId: options.chat });
    if (globalFlags.json) {
      writeJson(result);
    } else {
      console.log(`Canceled ${result.canceled} job(s).`);
    }
  }, globalFlags.timeoutMs);
}

// An ISO date, or a duration back from now. Returns unix seconds.
function parseRepairSince(value, now = Date.now()) {
  const raw = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(raw)) {
    const ms = Date.parse(raw);
    if (!Number.isFinite(ms)) {
      throw new Error(`--since: invalid date ${raw}`);
    }
    return Math.floor(ms / 1000);
  }
  let ms;
  try {
    // A bare number would read as seconds; nobody means that here.
    ms = /^\d+$/.test(raw) ? null : parseDuration(raw);
  } catch {
    ms = null;
  }
  if (ms === null) {
    throw new Error(`--since must be an ISO date or a duration such as 7d, got ${raw}`);
  }
  return Math.floor((now - ms) / 1000);
}

// A dry run counts in added_count what it would have added.
function addedWord(repair) {
  return repair.dry_run ? 'missing' : 'added';
}

function repairLabel(repair) {
  return repair.peer_title || repair.channel_id;
}

const REPAIR_STATUS_ORDER = ['idle', 'in_progress', 'pending', 'skipped', 'error'];
const REPAIR_STATUS_LINES = 10;
const DOCTOR_CHAT_LINES = 10;

function summarizeRepairs(repairs) {
  const byStatus = {};
  let checked = 0;
  let added = 0;
  let missing = 0;
  for (const repair of repairs) {
    byStatus[repair.status] = (byStatus[repair.status] ?? 0) + 1;
    checked += repair.checked_count ?? 0;
    if (repair.dry_run) {
      missing += repair.added_count ?? 0;
    } else {
      added += repair.added_count ?? 0;
    }
  }
  return { chats: repairs.length, byStatus, checked, added, missing };
}

function repairSummary(repair) {
  return {
    channelId: repair.channel_id,
    title: repair.peer_title ?? null,
    status: repair.status,
    checked: repair.checked_count ?? 0,
    added: repair.dry_run ? 0 : repair.added_count ?? 0,
    missing: repair.dry_run ? repair.added_count ?? 0 : null,
    dryRun: Boolean(repair.dry_run),
    cursorMessageId: repair.cursor_message_id ?? null,
    error: repair.error ?? null,
  };
}

// Queue a repair on the always-on server (auto-starting it) and follow it, or return
// at once with --background. Like `backfill --chat`, Ctrl-C detaches and the server
// carries on.
async function runBackfillRepair(globalFlags, options = {}) {
  return runWithTimeout(async () => {
    if (Boolean(options.chat) === Boolean(options.all)) {
      throw new Error('Pass either --chat <id|username> or --all');
    }
    if (options.afterId !== undefined && !options.chat) {
      throw new Error('--after-id works with --chat only');
    }
    const afterId = options.afterId !== undefined ? Number(options.afterId) : 0;
    if (!Number.isInteger(afterId) || afterId < 0) {
      throw new Error('--after-id must be a message id (0 or more)');
    }
    const since = options.since ?? (options.afterId !== undefined ? null : DEFAULT_REPAIR_SINCE);
    const sinceDate = since ? parseRepairSince(since) : null;

    const storeDir = resolveStoreDir();
    await ensureAutoStartedServer(globalFlags, storeDir);
    const { channelIds } = await enqueueRepair(storeDir, {
      chats: options.chat ? [options.chat] : [],
      all: Boolean(options.all),
      afterId,
      sinceDate,
      dryRun: Boolean(options.dryRun),
    });
    const kind = options.dryRun ? 'dry run of a repair' : 'repair';

    if (options.background) {
      if (globalFlags.json) {
        writeJson({ ok: true, channelIds });
      } else {
        console.log(`Queued a ${kind} of ${channelIds.length} chat(s). Follow it with \`tgcli backfill status\`.`);
      }
      return;
    }

    let detached = false;
    const onSigint = () => {
      detached = true;
      writeProgress(globalFlags, 'Still running in the background — check `tgcli backfill status`');
      process.exit(0);
    };
    process.on('SIGINT', onSigint);

    try {
      let repairs = [];
      let lastLine = null;
      let lastPrintedAt = 0;
      while (!detached) {
        repairs = withReadSnapshot(storeDir, (service) => service.listRepairs({ channelIds }));
        const active = repairs.find((repair) => !TERMINAL_REPAIR_STATUSES.has(repair.status));
        if (!active) {
          break;
        }
        const finished = repairs.filter((repair) => TERMINAL_REPAIR_STATUSES.has(repair.status)).length;
        const line = `${active.dry_run ? 'Checking' : 'Repairing'} ${repairLabel(active)}: checked ${active.checked_count}, ${addedWord(active)} ${active.added_count}`
          + (repairs.length > 1 ? ` (${finished}/${repairs.length} chats done)` : '');
        if (line !== lastLine && Date.now() - lastPrintedAt >= REPAIR_PROGRESS_INTERVAL_MS) {
          writeProgress(globalFlags, line);
          lastLine = line;
          lastPrintedAt = Date.now();
        }
        await delay(1000);
      }

      const failed = repairs.filter((repair) => repair.status === 'error');
      if (failed.length) {
        process.exitCode = 1;
      }
      if (globalFlags.json) {
        writeJson({ ok: failed.length === 0, repairs: repairs.map(repairSummary) });
        return;
      }
      const skipped = repairs.filter((repair) => repair.status === 'skipped');
      for (const repair of repairs) {
        if (repair.status === 'error') {
          console.error(`Repair of ${repairLabel(repair)} failed: ${repair.error ?? 'unknown error'}`);
        } else if (repair.status === 'skipped') {
          console.log(`Skipped ${repairLabel(repair)}: ${repair.error ?? 'Telegram refuses to read it'}`);
        } else if (repair.added_count > 0 || repairs.length === 1) {
          console.log(`${repairLabel(repair)}: checked ${repair.checked_count}, ${addedWord(repair)} ${repair.added_count}.`);
        }
      }
      const added = repairs.reduce((sum, repair) => sum + (repair.added_count ?? 0), 0);
      if (repairs.length > 1) {
        const repaired = repairs.length - failed.length - skipped.length;
        const skippedNote = skipped.length ? `, skipped ${skipped.length}` : '';
        const verb = options.dryRun ? 'Checked' : 'Repaired';
        console.log(`${verb} ${repaired}/${repairs.length} chat(s)${skippedNote}, ${options.dryRun ? 'missing' : 'added'} ${added} message(s).`);
      }
      if (options.dryRun) {
        console.log('Dry run: nothing written. Run it again without --dry-run to add them.');
      }
    } finally {
      process.off('SIGINT', onSigint);
    }
  }, globalFlags.timeoutMs);
}

// Bare `backfill`/`sync` (no --chat). Archiving runs in the always-on server; this
// only tracks its queue. `--follow` and `--once` both drain the queue and exit —
// they never run a worker or hold the store write lock. With no flag, there is
// nothing to track, so show usage.
async function runSync(globalFlags, options = {}) {
  if (!options.follow && !options.once) {
    const backfill = CLI_PROGRAM.commands.find((command) => command.name() === 'backfill');
    if (backfill) {
      process.stderr.write(backfill.helpInformation());
    }
    return;
  }
  // `--once` is a deprecated alias of `backfill wait`: drain quietly and exit.
  return runBackfillWait(globalFlags);
}

async function runServerStats(globalFlags) {
  const stats = await fetchServerStats(resolveStoreDir());
  if (globalFlags.json) {
    writeJson(stats);
    return;
  }
  console.log(formatServerStats(stats));
}

async function runServer(globalFlags, options = {}) {
  const timeoutMs = globalFlags.timeoutMs;
  // When no --idle-exit is passed the server stays up forever (manual server is
  // always-on). Validate the duration here so a bad value fails fast in the CLI.
  const idleExitMs = options.idleExit ? parseDuration(options.idleExit) : null;
  let child = null;

  const runChild = () => new Promise((resolve, reject) => {
    const serverPath = fileURLToPath(new URL('./mcp-server.js', import.meta.url));
    const handleSignal = (signal) => {
      if (child && !child.killed) {
        child.kill(signal);
      }
    };
    const cleanup = () => {
      process.off('SIGINT', handleSignal);
      process.off('SIGTERM', handleSignal);
    };

    process.on('SIGINT', handleSignal);
    process.on('SIGTERM', handleSignal);

    const childArgs = [serverPath];
    if (Number.isFinite(idleExitMs) && idleExitMs > 0) {
      childArgs.push('--idle-exit', String(idleExitMs));
    }
    if (options.autoStarted) {
      childArgs.push('--auto-started');
    }

    // This process writes the server's log, so the log also has what the server
    // prints before it could log anything itself (a module that fails to load)
    // and how it ended (a signal). The output still goes where it went before:
    // the terminal, a service manager's log, or nowhere for an auto-started one.
    const log = openServerLog(storeDir);
    child = spawn(process.execPath, childArgs, {
      stdio: ['inherit', 'pipe', 'pipe'],
      env: { ...process.env, TGCLI_SERVER_LOG: 'wrapper' },
    });
    for (const [source, from, to] of [['stdout', child.stdout, process.stdout], ['stderr', child.stderr, process.stderr]]) {
      to.on('error', () => {});
      from.on('data', (chunk) => {
        log.write(chunk, source);
        to.write(chunk);
      });
    }

    child.on('error', (error) => {
      cleanup();
      log.close();
      reject(error);
    });

    // 'close' comes after the child's output has all been read.
    child.on('close', (code, signal) => {
      cleanup();
      if (code === 0 || signal === 'SIGINT' || signal === 'SIGTERM') {
        log.close();
        resolve();
        return;
      }
      const message = `Server exited with code ${code ?? 'null'}${signal ? ` (${signal})` : ''}`;
      log.write(`[server] ${message}\n`, 'server');
      log.close();
      reject(new Error(message));
    });
  });

  const storeDir = resolveStoreDir();
  const { missing } = getStoreConfig(storeDir);
  if (missing.length > 0) {
    const message = 'Not authenticated. Run "tgcli auth" to configure credentials.';
    if (options.autoStarted) {
      appendServerLog(storeDir, `[startup] ${message}`);
    }
    throw new Error(message);
  }

  return runWithTimeout(runChild, timeoutMs, () => {
    if (child && !child.killed) {
      child.kill('SIGTERM');
    }
  });
}

function serverLabel(server) {
  return `tgcli server ${server.version ?? '(unknown version)'} (pid ${server.pid})`;
}

function reportStopping(globalFlags) {
  return (server) => writeProgress(globalFlags, `Stopping ${serverLabel(server)}...`);
}

function notStoppedError(server, force) {
  return new Error(force
    ? `${serverLabel(server)} could not be killed.`
    : `${serverLabel(server)} has not exited a minute after it was asked to stop; \`tgcli server stop --force\` kills it.`);
}

function stoppedServerPayload(server) {
  return {
    pid: server.pid,
    version: server.version,
    autoStarted: server.autoStarted,
    manager: server.manager,
    method: server.method,
  };
}

async function runServerStop(globalFlags, options = {}) {
  return runWithTimeout(async () => {
    const force = Boolean(options.force);
    const result = await stopServer(resolveStoreDir(), { force, onStopping: reportStopping(globalFlags) });
    if (result.running && !result.stopped) {
      throw notStoppedError(result, force);
    }
    if (globalFlags.json) {
      writeJson({ running: result.running, stopped: result.stopped, server: result.running ? stoppedServerPayload(result) : null });
      return;
    }
    if (!result.running) {
      console.log('No tgcli server is running.');
      return;
    }
    console.log(`Stopped ${serverLabel(result)}.`);
    if (isManagedServer(result)) {
      console.log(`${result.manager} runs it as a service and may start it again; \`tgcli service stop\` keeps it stopped.`);
    }
  }, globalFlags.timeoutMs);
}

async function runServerRestart(globalFlags, options = {}) {
  return runWithTimeout(async () => {
    const storeDir = resolveStoreDir();
    if (getStoreConfig(storeDir).missing.length > 0) {
      throw new Error('Not authenticated. Run "tgcli auth" to configure credentials.');
    }
    const force = Boolean(options.force);
    const { previous, current } = await restartServer(storeDir, {
      force,
      quiet: globalFlags.quiet,
      onStopping: reportStopping(globalFlags),
    });
    if (previous.running && !previous.stopped) {
      throw notStoppedError(previous, force);
    }
    if (!current) {
      throw new Error(`Stopped ${serverLabel(previous)}, but ${previous.manager} has not started it again; \`tgcli service start\` does.`);
    }
    const currentPayload = { pid: current.pid, version: current.version ?? null, startedAt: current.startedAt ?? null };
    if (globalFlags.json) {
      writeJson({ previous: previous.running ? stoppedServerPayload(previous) : null, current: currentPayload });
      return;
    }
    console.log(previous.running
      ? `Restarted tgcli server: ${previous.version ?? '(unknown version)'} (pid ${previous.pid}) -> ${current.version} (pid ${current.pid}).`
      : `Started ${serverLabel(current)}.`);
    if (current.version !== readVersion()) {
      console.log(`It runs ${current.version}, not this CLI's ${readVersion()}: ${isManagedServer(previous) ? `${previous.manager} started it` : 'another CLI started it first'}.`);
    }
  }, globalFlags.timeoutMs);
}

async function runServerLogs(globalFlags, options = {}) {
  const count = parsePositiveInt(options.lines, '--lines') ?? DEFAULT_SERVER_LOG_LINES;
  const storeDir = resolveStoreDir();
  const tail = readServerLogTail(storeDir, count);
  if (globalFlags.json && !options.follow) {
    writeJson({ path: tail.path, exists: tail.exists, lines: tail.lines });
    return;
  }
  // With --follow, --json prints one JSON object per line as it arrives.
  const emit = globalFlags.json
    ? (line) => process.stdout.write(`${JSON.stringify({ line })}\n`)
    : (line) => process.stdout.write(`${line}\n`);
  if (!tail.exists) {
    if (!options.follow) {
      console.log(`No server log yet (${tail.path}).`);
      return;
    }
    writeProgress(globalFlags, `Waiting for ${tail.path}...`);
  }
  tail.lines.forEach(emit);
  if (!options.follow) {
    return;
  }

  const controller = new AbortController();
  const stop = () => controller.abort();
  process.once('SIGINT', stop);
  const timer = globalFlags.timeoutMs ? setTimeout(stop, globalFlags.timeoutMs) : null;
  try {
    await followServerLog(storeDir, (lines) => lines.forEach(emit), {
      signal: controller.signal,
      from: tail.position ?? undefined,
    });
  } finally {
    process.off('SIGINT', stop);
    if (timer) {
      clearTimeout(timer);
    }
  }
}

async function runServiceInstall(globalFlags) {
  const timeoutMs = globalFlags.timeoutMs;
  return runWithTimeout(async () => {
    const { manager, brewInfo } = resolveServiceManager();
    const envVars = {
      TGCLI_SERVICE_MANAGER: manager,
    };
    if (process.env.TGCLI_STORE) {
      envVars.TGCLI_STORE = process.env.TGCLI_STORE;
    }

    if (manager === 'brew') {
      const note = brewInfo?.brewCliMatch
        ? 'Brew service available. Use `tgcli service start`.'
        : 'Brew service available, but current tgcli is not from brew.';
      if (globalFlags.json) {
        writeJson({ manager, installed: true, note });
      } else {
        console.log(note);
      }
      return;
    }

    if (manager === 'unsupported') {
      throw new Error('Service install is supported only on macOS and Linux.');
    }

    if (manager === 'launchd') {
      const { plistPath, logPath, errorLogPath } = getLaunchdPaths();
      fs.mkdirSync(path.dirname(plistPath), { recursive: true });
      const content = buildLaunchdPlist({
        nodePath: process.execPath,
        cliPath: CLI_PATH,
        envVars,
        logPath,
        errorLogPath,
      });
      fs.writeFileSync(plistPath, content, 'utf8');
      if (globalFlags.json) {
        writeJson({ manager, installed: true, path: plistPath });
      } else {
        console.log(`Service installed at ${plistPath}. Run \`tgcli service start\` to launch.`);
      }
      return;
    }

    if (manager === 'systemd') {
      const servicePath = getSystemdPath();
      fs.mkdirSync(path.dirname(servicePath), { recursive: true });
      const content = buildSystemdService({
        nodePath: process.execPath,
        cliPath: CLI_PATH,
        envVars,
      });
      fs.writeFileSync(servicePath, content, 'utf8');
      runCommand('systemctl', ['--user', 'daemon-reload']);
      if (globalFlags.json) {
        writeJson({ manager, installed: true, path: servicePath });
      } else {
        console.log(`Service installed at ${servicePath}. Run \`tgcli service start\` to launch.`);
      }
      return;
    }
  }, timeoutMs);
}

async function runServiceStart(globalFlags) {
  const timeoutMs = globalFlags.timeoutMs;
  return runWithTimeout(async () => {
    const { manager, brewInfo } = resolveServiceManager();

    if (manager === 'brew') {
      const result = runCommand('brew', ['services', 'start', 'tgcli'], { stdio: 'inherit' });
      if (result.status !== 0) {
        throw new Error(result.stderr || 'Failed to start brew service.');
      }
      if (globalFlags.json) {
        writeJson({ manager, started: true });
      }
      if (!brewInfo?.brewCliMatch && !globalFlags.json) {
        console.log('Warning: brew-managed service may not match this tgcli binary.');
      }
      return;
    }

    if (manager === 'unsupported') {
      throw new Error('Service start is supported only on macOS and Linux.');
    }

    if (manager === 'launchd') {
      const { plistPath } = getLaunchdPaths();
      if (!fs.existsSync(plistPath)) {
        throw new Error(`Service not installed. Run \`tgcli service install\` first.`);
      }
      const domain = `gui/${process.getuid()}`;
      const result = runCommand('launchctl', ['bootstrap', domain, plistPath]);
      if (result.status !== 0) {
        throw new Error(result.stderr || 'Failed to start launchd service.');
      }
      if (globalFlags.json) {
        writeJson({ manager, started: true });
      } else {
        console.log('Service started.');
      }
      return;
    }

    if (manager === 'systemd') {
      const servicePath = getSystemdPath();
      if (!fs.existsSync(servicePath)) {
        throw new Error(`Service not installed. Run \`tgcli service install\` first.`);
      }
      const result = runCommand('systemctl', ['--user', 'enable', '--now', SYSTEMD_SERVICE_NAME]);
      if (result.status !== 0) {
        throw new Error(result.stderr || 'Failed to start systemd service.');
      }
      if (globalFlags.json) {
        writeJson({ manager, started: true });
      } else {
        console.log('Service started.');
      }
    }
  }, timeoutMs);
}

async function runServiceStop(globalFlags) {
  const timeoutMs = globalFlags.timeoutMs;
  return runWithTimeout(async () => {
    const { manager } = resolveServiceManager();

    if (manager === 'brew') {
      const result = runCommand('brew', ['services', 'stop', 'tgcli'], { stdio: 'inherit' });
      if (result.status !== 0) {
        throw new Error(result.stderr || 'Failed to stop brew service.');
      }
      if (globalFlags.json) {
        writeJson({ manager, stopped: true });
      }
      return;
    }

    if (manager === 'unsupported') {
      throw new Error('Service stop is supported only on macOS and Linux.');
    }

    if (manager === 'launchd') {
      const { plistPath } = getLaunchdPaths();
      if (!fs.existsSync(plistPath)) {
        throw new Error(`Service not installed. Run \`tgcli service install\` first.`);
      }
      const domain = `gui/${process.getuid()}`;
      const result = runCommand('launchctl', ['bootout', domain, plistPath]);
      if (result.status !== 0 && result.stderr.trim()) {
        throw new Error(result.stderr || 'Failed to stop launchd service.');
      }
      if (globalFlags.json) {
        writeJson({ manager, stopped: true });
      } else {
        console.log('Service stopped.');
      }
      return;
    }

    if (manager === 'systemd') {
      const result = runCommand('systemctl', ['--user', 'stop', SYSTEMD_SERVICE_NAME]);
      if (result.status !== 0) {
        throw new Error(result.stderr || 'Failed to stop systemd service.');
      }
      if (globalFlags.json) {
        writeJson({ manager, stopped: true });
      } else {
        console.log('Service stopped.');
      }
    }
  }, timeoutMs);
}

async function runServiceStatus(globalFlags) {
  const timeoutMs = globalFlags.timeoutMs;
  return runWithTimeout(async () => {
    const { manager, brewInfo } = resolveServiceManager();
    const storeDir = resolveStoreDir();
    const serviceState = readServiceState(storeDir);
    const cliVersion = readVersion();

    let installed = false;
    let running = false;
    let pid = null;
    let statusLabel = null;

    if (manager === 'brew') {
      installed = Boolean(brewInfo?.installed && brewInfo.serviceAvailable);
      if (brewInfo?.serviceStatus) {
        statusLabel = brewInfo.serviceStatus;
        running = brewInfo.serviceStatus === 'started';
      }
    } else if (manager === 'launchd') {
      const { plistPath } = getLaunchdPaths();
      installed = fs.existsSync(plistPath);
      const list = runCommand('launchctl', ['list']);
      if (list.status === 0) {
        const lines = list.stdout.split('\n');
        for (const line of lines) {
          if (!line.includes(LAUNCHD_LABEL)) continue;
          const parts = line.trim().split(/\s+/);
          const pidValue = parts[0];
          pid = pidValue && pidValue !== '-' ? Number(pidValue) : null;
          running = Boolean(pid);
          statusLabel = running ? 'started' : 'stopped';
          break;
        }
      }
    } else if (manager === 'systemd') {
      const servicePath = getSystemdPath();
      installed = fs.existsSync(servicePath);
      const active = runCommand('systemctl', ['--user', 'is-active', SYSTEMD_SERVICE_NAME]);
      running = active.status === 0 && active.stdout.trim() === 'active';
      statusLabel = active.stdout.trim();
    } else {
      statusLabel = 'unsupported';
    }

    const serviceVersion = serviceState?.version ?? null;
    const versionMismatch = serviceVersion && cliVersion && serviceVersion !== cliVersion;
    const resolvedPid = running ? (pid ?? serviceState?.pid ?? null) : null;
    const output = {
      manager,
      installed,
      running,
      status: statusLabel,
      pid: resolvedPid,
      serviceVersion,
      cliVersion,
      storeDir,
      mcpEnabled: serviceState?.mcpEnabled ?? null,
    };

    if (globalFlags.json) {
      writeJson({
        ...output,
        versionMismatch,
        brewCliMatch: brewInfo?.brewCliMatch ?? null,
      });
      return;
    }

    console.log(`Manager: ${manager}`);
    console.log(`Installed: ${installed ? 'yes' : 'no'}`);
    console.log(`Running: ${running ? 'yes' : 'no'}`);
    if (statusLabel) {
      console.log(`Status: ${statusLabel}`);
    }
    if (output.pid) {
      console.log(`PID: ${output.pid}`);
    }
    if (serviceVersion) {
      console.log(`Service version: ${serviceVersion}`);
    }
    console.log(`CLI version: ${cliVersion}`);
    console.log(`Store: ${storeDir}`);
    if (serviceState && serviceState.mcpEnabled !== null && serviceState.mcpEnabled !== undefined) {
      console.log(`MCP enabled: ${serviceState.mcpEnabled ? 'yes' : 'no'}`);
    }
    if (versionMismatch) {
      console.log('Warning: service version differs from CLI. Run `tgcli doctor`.');
    }
    if (manager === 'brew' && brewInfo && brewInfo.brewCliMatch === false) {
      console.log('Warning: brew-managed service may not match this tgcli binary.');
    }
  }, timeoutMs);
}

async function runServiceLogs(globalFlags) {
  const timeoutMs = globalFlags.timeoutMs;
  return runWithTimeout(async () => {
    const { manager } = resolveServiceManager();

    if (manager === 'brew') {
      const info = runCommand('brew', ['services', 'info', 'tgcli']);
      if (info.status !== 0) {
        throw new Error(info.stderr || 'Failed to read brew service info.');
      }
      const match = info.stdout.match(/Log:\s+(.+)/i);
      const logPath = match ? match[1].trim() : null;
      if (globalFlags.json) {
        writeJson({ manager, logPath });
        return;
      }
      if (logPath && fs.existsSync(logPath)) {
        runCommand('tail', ['-n', '200', logPath], { stdio: 'inherit' });
      } else {
        process.stdout.write(info.stdout);
      }
      return;
    }

    if (manager === 'launchd') {
      const { logPath, errorLogPath } = getLaunchdPaths();
      if (globalFlags.json) {
        writeJson({ manager, logPath, errorLogPath });
        return;
      }
      if (fs.existsSync(logPath)) {
        runCommand('tail', ['-n', '200', logPath], { stdio: 'inherit' });
      } else if (fs.existsSync(errorLogPath)) {
        runCommand('tail', ['-n', '200', errorLogPath], { stdio: 'inherit' });
      } else {
        console.log('No log file found.');
      }
      return;
    }

    if (manager === 'systemd') {
      if (globalFlags.json) {
        writeJson({ manager, journal: true });
        return;
      }
      runCommand('journalctl', ['--user', '-u', SYSTEMD_SERVICE_NAME, '-n', '200', '--no-pager'], {
        stdio: 'inherit',
      });
      return;
    }

    throw new Error('Service logs are supported only on macOS and Linux.');
  }, timeoutMs);
}

async function runSyncStatus(globalFlags) {
  const timeoutMs = globalFlags.timeoutMs;
  return runWithTimeout(async () => {
    const storeDir = resolveStoreDir();
    // Snapshot the queue + active backfills under a brief read lock so a running
    // server keeps its write lock free.
    const { queue, jobs, repairs } = withReadSnapshot(storeDir, (service) => ({
      queue: service.getQueueStats(),
      jobs: service.listJobs({ limit: 100 }),
      repairs: service.listRepairs(),
    }));
    const repairTotals = summarizeRepairs(repairs);
    // in_progress first, then pending — the chats actively/imminently backfilling.
    const active = jobs.filter((job) => job.status === 'in_progress' || job.status === 'pending');
    const ping = await pingServer(storeDir);

    if (globalFlags.json) {
      writeJson({
        server: ping ? { up: true, pid: ping.pid, version: ping.version, startedAt: ping.startedAt } : { up: false },
        queue,
        backfills: active.map((job) => ({
          jobId: job.id,
          channelId: job.channel_id,
          title: job.peer_title ?? null,
          status: job.status,
          messageCount: job.message_count ?? 0,
          targetMessageCount: job.target_message_count ?? 0,
          cursorDate: job.cursor_message_date ?? null,
          updatedAt: job.updated_at ?? null,
        })),
        repairTotals,
        repairs: repairs.map(repairSummary),
      });
      return;
    }

    console.log(`QUEUE: pending=${queue.pending} in_progress=${queue.in_progress} idle=${queue.idle} error=${queue.error} gaps=${queue.gaps}`);
    console.log(`SERVER: ${ping ? `up (pid ${ping.pid})` : 'down'}`);
    if (active.length === 0) {
      console.log('No active backfills.');
    } else {
      for (const job of active) {
        const label = job.peer_title || job.channel_id;
        const done = job.message_count ?? 0;
        const target = job.target_message_count ?? 0;
        const pct = target > 0 ? Math.min(100, Math.round((done / target) * 100)) : 0;
        const cursor = job.cursor_message_date ? ` cursor=${job.cursor_message_date}` : '';
        const updated = job.updated_at ? ` updated=${job.updated_at}` : '';
        console.log(`  ${label} [${job.status}] ${done}/${target} (${pct}%)${cursor}${updated}`);
      }
    }
    if (repairs.length) {
      // `--all` queues every synced chat, so name only the one under way and the failures.
      const counts = REPAIR_STATUS_ORDER.map((status) => `${status}=${repairTotals.byStatus[status] ?? 0}`).join(' ');
      const missing = repairs.some((repair) => repair.dry_run) ? ` missing=${repairTotals.missing}` : '';
      console.log(`REPAIRS: ${repairTotals.chats} chat(s) ${counts} checked=${repairTotals.checked} added=${repairTotals.added}${missing}`);
      const named = [
        ...repairs.filter((repair) => repair.status === 'in_progress'),
        ...repairs.filter((repair) => repair.status === 'error'),
      ];
      for (const repair of named.slice(0, REPAIR_STATUS_LINES)) {
        const error = repair.error ? ` error=${repair.error}` : '';
        console.log(`  repair ${repairLabel(repair)} [${repair.status}] checked=${repair.checked_count} ${addedWord(repair)}=${repair.added_count}${error}`);
      }
      if (named.length > REPAIR_STATUS_LINES) {
        console.log(`  … and ${named.length - REPAIR_STATUS_LINES} more; \`tgcli --json backfill status\` lists them all.`);
      }
    }
  }, timeoutMs);
}

async function runSyncJobsList(globalFlags, options = {}) {
  const status = options.status ? String(options.status) : null;
  if (status && !['pending', 'in_progress', 'idle', 'error'].includes(status)) {
    throw new Error(`Unknown status: ${status}`);
  }
  const limit = parsePositiveInt(options.limit, '--limit') ?? 100;
  return withCommand(globalFlags, { need: 'archive', lock: null }, async ({ messageSyncService }) => {
    const jobs = messageSyncService.listJobs({
      status,
      channelId: options.channel ?? null,
      limit,
    });
    if (globalFlags.json) {
      writeJson(jobs);
    } else {
      for (const job of jobs) {
        const label = job.peer_title || job.channel_id;
        console.log(`#${job.id} ${label} [${job.status}] ${job.message_count}/${job.target_message_count}`);
      }
    }
  });
}

// Enqueue a job on the always-on server (auto-starting it); the server's queue
// processes it. Returns immediately after enqueue.
async function runSyncJobsAdd(globalFlags, options = {}) {
  if (!options.chat) {
    throw new Error('--chat is required');
  }
  return runWithTimeout(async () => {
    const storeDir = resolveStoreDir();
    const depth = parsePositiveInt(options.depth, '--depth');
    await ensureAutoStartedServer(globalFlags, storeDir);
    const job = await enqueueBackfill(storeDir, {
      chatId: options.chat,
      depth,
      minDate: options.minDate ?? null,
    });
    if (globalFlags.json) {
      writeJson(job);
    } else {
      console.log(`Job scheduled for ${job.channelId} (#${job.jobId}).`);
    }
  }, globalFlags.timeoutMs);
}

// Reset errored job(s) to pending on the always-on server (auto-starting it); the
// server's queue reprocesses them.
async function runSyncJobsRetry(globalFlags, options = {}) {
  const jobId = parsePositiveInt(options.jobId, '--job-id');
  const channelId = options.channel ?? null;
  const allErrors = Boolean(options.allErrors);
  if (!jobId && !channelId && !allErrors) {
    throw new Error('--job-id, --channel, or --all-errors is required');
  }
  if (allErrors && (jobId || channelId)) {
    throw new Error('Use --all-errors without --job-id/--channel');
  }
  return runWithTimeout(async () => {
    const storeDir = resolveStoreDir();
    await ensureAutoStartedServer(globalFlags, storeDir);
    const result = await retryBackfill(storeDir, { jobId, channelId, allErrors });
    if (globalFlags.json) {
      writeJson(result);
    } else {
      console.log(`Re-queued ${result.updated} job(s).`);
    }
  }, globalFlags.timeoutMs);
}

async function runSyncJobsCancel(globalFlags, options = {}) {
  const jobId = parsePositiveInt(options.jobId, '--job-id');
  const channelId = options.channel ?? null;
  if (!jobId && !channelId) {
    throw new Error('--job-id or --channel is required');
  }
  if (jobId && channelId) {
    throw new Error('Use --job-id or --channel, not both');
  }
  return runWithTimeout(async () => {
    const storeDir = resolveStoreDir();
    await ensureAutoStartedServer(globalFlags, storeDir);
    const result = await cancelBackfill(storeDir, {
      jobId,
      chatId: channelId,
    });
    if (globalFlags.json) {
      writeJson(result);
    } else {
      console.log(`Canceled ${result.canceled} job(s).`);
    }
  }, globalFlags.timeoutMs);
}

// Chats that older versions stored under a username, and what becomes of each.
function formatUsernameKeyLines(chats) {
  if (!chats.length) {
    return [];
  }
  const lines = [`USERNAME KEYS: ${chats.length} chat(s) stored under a username instead of a Telegram id. The server moves each to its id after it logs in to Telegram; messages already under the id stay, duplicates are dropped.`];
  for (const chat of chats) {
    const messages = `${chat.messages} message(s)`;
    if (chat.lastCheck) {
      const { outcome, detail, checkedAt } = chat.lastCheck;
      const next = outcome === 'failed' ? 'tried again when the server next starts' : 'asked again a day after that';
      lines.push(`  ${chat.key}: ${messages}, stays as it is: ${detail} (${checkedAt}); ${next}.`);
    } else if (chat.archivedAs) {
      lines.push(`  ${chat.key}: ${messages}, to be merged with its Telegram id; the archive also has ${chat.archivedAs} under this username.`);
    } else {
      lines.push(`  ${chat.key}: ${messages}, no Telegram id known for it yet; the server asks Telegram for one.`);
    }
  }
  return lines;
}

async function runDoctor(globalFlags, options = {}) {
  return withCommand(globalFlags, { need: 'full', lock: null }, async ({ storeDir, telegramClient, messageSyncService }) => {
    const lock = readStoreLock(storeDir);
    let authenticated = false;
    let connected = false;
    try {
      authenticated = await telegramClient.isAuthorized();
      if (options.connect && authenticated) {
        await telegramClient.startUpdates();
        connected = true;
      }
    } catch (error) {
      authenticated = false;
    }

    const search = messageSyncService.getSearchStatus();
    const queue = messageSyncService.getQueueStats();
    const repairSuggested = messageSyncService.isRepairSuggested();
    const leftChats = messageSyncService.getLeftChatCounts();
    const unansweredChats = messageSyncService.listUnansweredChats().map((chat) => ({
      channelId: chat.channel_id,
      title: chat.peer_title ?? null,
      checkedAt: chat.membership_checked_at ? new Date(chat.membership_checked_at).toISOString() : null,
    }));
    const usernameKeys = messageSyncService.listUsernameKeyedChats();
    const update = getUpdateStatus({
      storeDir,
      version: readVersion(),
      installMethod: detectInstallMethod(),
    });

    const payload = {
      storeDir,
      lockHeld: lock.exists,
      lockInfo: lock.info,
      authenticated,
      connected,
      ftsEnabled: search.enabled,
      ftsVersion: search.version,
      queue,
      repairSuggested,
      leftChats,
      unansweredChats,
      usernameKeys,
      update,
    };

    if (globalFlags.json) {
      writeJson(payload);
      return;
    }

    console.log(`STORE: ${payload.storeDir}`);
    console.log(`LOCKED: ${payload.lockHeld}${payload.lockInfo ? ` (${payload.lockInfo})` : ''}`);
    console.log(`AUTHENTICATED: ${payload.authenticated}`);
    console.log(`CONNECTED: ${payload.connected}`);
    console.log(`FTS: ${payload.ftsEnabled}${payload.ftsVersion ? ` (v${payload.ftsVersion})` : ''}`);
    console.log(`QUEUE: pending=${queue.pending} in_progress=${queue.in_progress} idle=${queue.idle} error=${queue.error} gaps=${queue.gaps}`);
    if (leftChats.syncOff) {
      console.log(`LEFT: sync is off for ${leftChats.syncOff} chat(s) you are not in (left, removed or never joined); it comes back on if you rejoin, or for good with \`tgcli channels watch --chat <id>\`.`);
    }
    if (leftChats.stillJoined) {
      console.log(`DIALOGS: ${leftChats.stillJoined} synced chat(s) are missing from your dialog list, though Telegram says you are still in them; sync stays on.`);
    }
    if (unansweredChats.length) {
      console.log(`UNANSWERED: ${unansweredChats.length} synced chat(s) are missing from your dialog list, and Telegram gives no answer about them (most often a private chat you were removed from), so nothing arrives from them. Turn their sync off with \`tgcli channels unwatch --missing\`.`);
      for (const chat of unansweredChats.slice(0, DOCTOR_CHAT_LINES)) {
        console.log(`  ${chat.channelId}${chat.title ? ` ${chat.title}` : ''}`);
      }
      if (unansweredChats.length > DOCTOR_CHAT_LINES) {
        console.log(`  … and ${unansweredChats.length - DOCTOR_CHAT_LINES} more; \`tgcli --json doctor\` lists them all.`);
      }
    }
    if (repairSuggested) {
      console.log(`REPAIR: versions before 2.2.5 could lose messages; fill the holes with \`tgcli backfill repair --all\` (the last ${DEFAULT_REPAIR_SINCE} of every synced chat).`);
    }
    for (const line of formatUsernameKeyLines(usernameKeys)) {
      console.log(line);
    }
    console.log(formatDoctorUpdateLine(update));
  });
}

function isCliEntrypoint(argvPath = process.argv[1]) {
  if (!argvPath) return false;
  try {
    return pathToFileURL(fs.realpathSync(argvPath)).href === import.meta.url;
  } catch (error) {
    return pathToFileURL(path.resolve(argvPath)).href === import.meta.url;
  }
}

function shouldRunMain(entryPath = process.argv[1]) {
  if (!entryPath) return false;
  try {
    return fs.realpathSync(entryPath) === CLI_PATH;
  } catch (error) {
    return path.resolve(entryPath) === CLI_PATH;
  }
}

// `server` runs unattended with its output in a log, so it neither checks nor
// prints; `doctor` checks but prints its own UPDATE line instead of the notice.
const UPDATE_CHECK_SKIPPED_COMMANDS = new Set(['server']);
const UPDATE_NOTICE_SKIPPED_COMMANDS = new Set(['server', 'doctor']);

function commandPath(command) {
  const names = [];
  for (let current = command; current?.parent; current = current.parent) {
    names.unshift(current.name());
  }
  return names.join(' ');
}

// Hook the new-version notice into a program: before the action, start the daily
// background registry check if it is due; after a successful action, print the
// cached notice.
function installUpdateNotice(program, deps = {}) {
  const {
    version = readVersion(),
    installMethod = detectInstallMethod(),
    env = process.env,
    stderr = process.stderr,
    now = () => Date.now(),
    spawnFn,
  } = deps;
  const enabled = () => !getUpdateCheckDisabledReason({ env, version, installMethod });
  program.hook('preAction', (_program, actionCommand) => {
    if (UPDATE_CHECK_SKIPPED_COMMANDS.has(commandPath(actionCommand)) || !enabled()) return;
    scheduleUpdateCheck({ storeDir: resolveStoreDir(), now: now(), spawnFn });
  });
  program.hook('postAction', (_program, actionCommand) => {
    if (UPDATE_NOTICE_SKIPPED_COMMANDS.has(commandPath(actionCommand)) || !enabled()) return;
    // A failed command ends on its error; the notice waits for the next run.
    if (process.exitCode) return;
    const options = actionCommand.optsWithGlobals();
    maybePrintUpdateNotice({
      storeDir: resolveStoreDir(),
      version,
      installMethod,
      json: Boolean(options.json),
      quiet: Boolean(options.quiet),
      env,
      stderr,
      now: now(),
    });
  });
  return program;
}

async function main() {
  installUpdateNotice(CLI_PROGRAM);
  await CLI_PROGRAM.parseAsync(process.argv);
}

export {
  buildProgram,
  installUpdateNotice,
  formatFeedbackMessage,
  getFeedbackCooldownRemainingSeconds,
  isCliEntrypoint,
  main,
  runFeedback,
  shouldRunMain,
  writeFeedbackLastSentAt,
};

if (isCliEntrypoint()) {
  await main();
}
