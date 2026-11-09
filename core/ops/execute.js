// The server's single entry to an operation: the control API's /control/run and
// every MCP tool run operations through here. The args are validated again on this
// side of the seam, so a caller that skipped the CLI's checks gets the same errors.

import { needsTelegram, parseOperationArgs } from './operation.js';

// services: { telegramClient, messageSyncService }; ensureLogin: awaited before an
// operation that needs Telegram.
export async function executeOperation(op, rawArgs, { services, ensureLogin } = {}) {
  const args = parseOperationArgs(op, rawArgs, 'wire');
  if (typeof ensureLogin === 'function' && needsTelegram(op, args)) {
    await ensureLogin();
  }
  return op.run(services, args);
}
