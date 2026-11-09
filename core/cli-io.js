// What every CLI command shares: the global flags, JSON and error output, and the
// wrapper that turns a thrown error into exit code 1.

import { parseDuration } from './duration.js';
import { buildSendErrorPayload, formatSendErrorMessage, SendCommandError } from './send-utils.js';

export function getGlobalFlags(command) {
  const options = command.optsWithGlobals();
  const timeoutMs = options.timeout ? parseDuration(options.timeout) : null;
  return {
    json: Boolean(options.json),
    quiet: Boolean(options.quiet),
    timeout: options.timeout ?? null,
    timeoutMs,
  };
}

// Wraps a commander action: handler(globalFlags, ...actionArgs).
export function withGlobalOptions(handler) {
  return async (...args) => {
    let globalFlags;
    try {
      const command = args[args.length - 1];
      globalFlags = getGlobalFlags(command);
      await handler(globalFlags, ...args);
    } catch (error) {
      writeError(error, globalFlags?.json ?? process.argv.includes('--json'));
      process.exitCode = 1;
    }
  };
}

export function writeJson(payload) {
  process.stdout.write(`${JSON.stringify(payload, null, 2)}\n`);
}

export function writeError(error, asJson) {
  if (error instanceof SendCommandError) {
    if (asJson) {
      process.stderr.write(`${JSON.stringify(buildSendErrorPayload(error.details), null, 2)}\n`);
    } else {
      process.stderr.write(`${formatSendErrorMessage(error.details)}\n`);
    }
    return;
  }
  const message = error?.message ?? String(error);
  if (asJson) {
    process.stderr.write(`${JSON.stringify({ ok: false, error: message })}\n`);
  } else {
    process.stderr.write(`${message}\n`);
  }
}
