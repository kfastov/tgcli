// The archive's own state as MCP tools: the chats it tracks, its backfill jobs and
// its message counts. The CLI shows the same through `backfill` and `doctor`.

import { defineOperation, field } from './operation.js';
import { archiveKey, telegramForUsername } from './chat-keys.js';

export const listActiveChannels = defineOperation({
  name: 'listActiveChannels',
  summary: 'Lists dialogs tracked in the local archive registry.',
  annotations: { readOnly: true },
  telegram: false,
  run: (ctx) => ctx.messageSyncService.listActiveChannels(),
});

export const scheduleMessageSync = defineOperation({
  name: 'scheduleMessageSync',
  summary: 'Schedules a background job to archive channel messages locally.',
  input: {
    channelId: field.chat('Numeric channel ID or username'),
    depth: field.int('Maximum messages to retain per channel (default 1000)', { max: 50000 }).optional(),
    minDate: field.date('Earliest ISO-8601 timestamp to backfill').optional(),
  },
  async run(ctx, args) {
    const chatId = await ctx.messageSyncService.resolveChatKey(args.channelId);
    const job = ctx.messageSyncService.addJob(chatId, { depth: args.depth, minDate: args.minDate });
    void ctx.messageSyncService.processQueue();
    return job;
  },
});

export const getSyncedMessageStats = defineOperation({
  name: 'getSyncedMessageStats',
  summary: 'Returns summary statistics for stored messages in a channel.',
  annotations: { readOnly: true },
  telegram: telegramForUsername,
  input: { channelId: field.chat('Numeric channel ID or username') },
  run: async (ctx, args) => ctx.messageSyncService.getMessageStats(await archiveKey(ctx, args.channelId)),
});

export const listMessageSyncJobs = defineOperation({
  name: 'listMessageSyncJobs',
  summary: 'Lists tracked message sync jobs and their current status.',
  annotations: { readOnly: true },
  telegram: false,
  run: (ctx) => ctx.messageSyncService.listJobs(),
});

export const ARCHIVE_OPERATIONS = [listActiveChannels, scheduleMessageSync, getSyncedMessageStats, listMessageSyncJobs];
