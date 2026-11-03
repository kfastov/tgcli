#!/usr/bin/env node
// Fills gaps in the local archive by walking one chat's history from new to old,
// down to a given message. Rows already in the archive and the sync cursors stay as
// they are.
// See docs/archive-recovery.md before running this on a real archive.
import fs from 'node:fs';
import { pathToFileURL } from 'node:url';

import { pingServer } from '../core/control-client.js';
import { createServices } from '../core/services.js';
import { resolveStoreDir } from '../core/store.js';
import { acquireStoreLock } from '../store-lock.js';

const PAGE_SIZE = 100;
const PAGE_DELAY_MS = 1000;

export async function recoverArchiveGaps(
  { telegramClient, messageSyncService },
  { chat, afterId, delayMs = PAGE_DELAY_MS },
) {
  const channel = messageSyncService.getChannel(chat);
  if (!channel) {
    throw new Error(`Chat ${chat} is not in the archive.`);
  }
  if (!channel.syncEnabled) {
    throw new Error(`Chat ${chat} has sync disabled; enable it first if it belongs in the archive.`);
  }

  let offsetId = 0;
  let added = 0;
  for (;;) {
    const { messages } = await telegramClient.getMessagesByChannelId(chat, PAGE_SIZE, { offsetId });
    const inRange = messages.filter((message) => message.id > afterId);
    if (inRange.length) {
      added += messageSyncService.archiveHistoryMessages(channel.channelId, inRange);
    }
    if (!messages.length || inRange.length < messages.length) {
      return { added };
    }
    offsetId = Math.min(...messages.map((message) => message.id));
    console.log(`Chat ${chat}: reached message ${offsetId}, added ${added}.`);
    await new Promise((resolve) => setTimeout(resolve, delayMs));
  }
}

async function main() {
  const [chat, afterArg] = process.argv.slice(2);
  const afterId = Number(afterArg);
  if (!chat || !Number.isInteger(afterId) || afterId < 0) {
    console.error('Usage: node scripts/recover-archive-gaps.js <chatId> <afterMessageId>');
    process.exitCode = 1;
    return;
  }

  const storeDir = resolveStoreDir();
  // The server takes no store lock, so the lock below would not keep the two apart.
  const server = await pingServer(storeDir);
  if (server) {
    console.error(`A tgcli server (pid ${server.pid}) is running on this store. Stop it first: tgcli service stop, or kill ${server.pid}.`);
    process.exitCode = 1;
    return;
  }

  const release = acquireStoreLock(storeDir);
  let services = null;
  try {
    // Without updates: the catch-up belongs to the server, not to this script.
    services = createServices({ storeDir, disableUpdates: true });
    const { added } = await recoverArchiveGaps(services, { chat, afterId });
    console.log(`Chat ${chat}: done, added ${added}.`);
  } finally {
    if (services) {
      await services.messageSyncService.close();
      await services.telegramClient.destroy();
    }
    release();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(fs.realpathSync(process.argv[1])).href) {
  main().catch((error) => {
    console.error(error?.message ?? error);
    process.exitCode = 1;
  });
}
