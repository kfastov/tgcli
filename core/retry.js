import { setTimeout as delay } from 'timers/promises';

/**
 * Extract a human-readable message from any error shape.
 */
export function formatErrorMessage(error) {
  if (error instanceof Error && error.message) {
    return error.message;
  }
  if (typeof error === 'string') {
    return error;
  }
  return String(error);
}

/**
 * Parse Telegram FLOOD_WAIT / "wait of N seconds" errors.
 * Returns the required wait in seconds, or null if not a rate-limit error.
 */
export function parseRequiredWaitSeconds(error) {
  const text = formatErrorMessage(error);
  const waitMatch = /wait of (\d+) seconds is required/i.exec(text);
  if (waitMatch) {
    return Number(waitMatch[1]);
  }
  const floodWaitMatch = /FLOOD_WAIT_(\d+)/i.exec(text);
  if (floodWaitMatch) {
    return Number(floodWaitMatch[1]);
  }
  return null;
}

/**
 * Classify an error into: rate_limit, network, or api.
 */
export function classifyError(error) {
  const message = formatErrorMessage(error);
  const code = error?.code ?? null;

  if (parseRequiredWaitSeconds(error) !== null || /FLOOD_WAIT/i.test(message)) {
    return { type: 'rate_limit', message, code };
  }

  if (/ECONNRESET|ETIMEDOUT|ENETUNREACH/i.test(message)
    || /ECONNRESET|ETIMEDOUT|ENETUNREACH/.test(code ?? '')) {
    return { type: 'network', message, code };
  }

  return { type: 'api', message, code };
}

/**
 * Compute how long to wait before retrying.
 * For rate-limit errors, use the server-specified wait time.
 * For others, use exponential backoff: 1s, 2s, 4s, ...
 */
export function computeRetryWaitSeconds(error, attempt) {
  const rateLimitWait = parseRequiredWaitSeconds(error);
  if (rateLimitWait !== null) {
    return rateLimitWait;
  }
  return Math.pow(2, attempt - 1);
}

/**
 * Execute a function with automatic retries on transient errors.
 *
 * Options:
 *   retries        — max retry count (default 0 = no retries)
 *   json           — write JSON retry events to stderr (default false)
 *   maxWaitSeconds — skip retry if wait exceeds this (default 300)
 *
 * Returns { result, retryLog, attempts } on success.
 * Throws the original error (with .retryLog and .attempts attached) on failure.
 */
export async function withSendRetry(fn, options = {}) {
  const maxRetries = options.retries ?? 0;
  const json = options.json ?? false;
  const maxWaitSeconds = options.maxWaitSeconds ?? 300;
  const retryLog = [];
  const maxAttempts = maxRetries + 1;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const result = await fn();
      return { result, retryLog, attempts: attempt };
    } catch (error) {
      const classified = classifyError(error);

      // Don't retry API errors or if we've exhausted attempts
      if (attempt >= maxAttempts || classified.type === 'api') {
        error.retryLog = retryLog;
        error.attempts = attempt;
        throw error;
      }

      const waitSeconds = computeRetryWaitSeconds(error, attempt);

      // Don't wait longer than the configured maximum
      if (waitSeconds > maxWaitSeconds) {
        error.retryLog = retryLog;
        error.attempts = attempt;
        throw error;
      }

      retryLog.push({ attempt, error: classified, waitSeconds });

      if (json) {
        process.stderr.write(`${JSON.stringify({
          event: 'retry',
          attempt,
          maxAttempts,
          error: classified,
          waitSeconds,
        })}\n`);
      } else {
        process.stderr.write(
          `Retry ${attempt}/${maxRetries}: ${classified.type.toUpperCase()} — ${classified.message}. Waiting ${waitSeconds}s...\n`,
        );
      }

      await delay(waitSeconds * 1000);
    }
  }
}
