// Chats: finding them, their archive record, which of them the archive follows, and
// marking them read.

import { defineOperation, field } from './operation.js';
import { archiveKey, chatOption } from './chat-keys.js';

function renderDialogs(dialogs, { print }) {
  for (const dialog of dialogs) {
    const label = dialog.title || dialog.username || dialog.id;
    print(`${label} (${dialog.id})`);
  }
}

export const listChannels = defineOperation({
  name: 'listChannels',
  command: 'channels list',
  summary: 'List channels',
  description: 'Lists Telegram dialogs for the account, including unread counts; with a query, searches them by title or username.',
  annotations: { readOnly: true },
  input: {
    query: field.textOrEmpty('Search by title or username').optional().flag('--query <text>'),
    limit: field.int('Maximum number of dialogs to return').default(50).flag('--limit <n>', { help: 'Limit results' }),
  },
  run(ctx, args) {
    return args.query
      ? ctx.telegramClient.searchDialogs(args.query, args.limit)
      : ctx.telegramClient.listDialogs(args.limit);
  },
  render: renderDialogs,
});

// Dialog search as an MCP tool of its own; `listChannels` with a query does the same.
export const searchChannels = defineOperation({
  name: 'searchChannels',
  command: null,
  summary: 'Search dialogs by title or username',
  annotations: { readOnly: true },
  input: {
    keywords: field.text('Keywords to search for in channel titles or usernames'),
    limit: field.int('Maximum number of results to return').default(100),
  },
  run: (ctx, args) => ctx.telegramClient.searchDialogs(args.keywords, args.limit),
});

export const channelsShow = defineOperation({
  name: 'channelsShow',
  command: 'channels show',
  summary: 'Show channel info',
  description: "Returns a chat's archive record (sync state, cursors, metadata), or what Telegram says about a chat the archive does not track.",
  annotations: { readOnly: true },
  input: { channelId: chatOption() },
  async run(ctx, args) {
    const channel = ctx.messageSyncService.getChannel(await archiveKey(ctx, args.channelId));
    if (channel) {
      return channel;
    }
    const meta = await ctx.telegramClient.getPeerMetadata(args.channelId);
    return {
      channelId: String(args.channelId),
      peerTitle: meta?.peerTitle ?? null,
      peerType: meta?.peerType ?? null,
      chatType: meta?.chatType ?? null,
      isForum: meta?.isForum ?? null,
      username: meta?.username ?? null,
      syncEnabled: null,
      lastMessageId: null,
      lastMessageDate: null,
      oldestMessageId: null,
      oldestMessageDate: null,
      about: meta?.about ?? null,
      metadataUpdatedAt: null,
      createdAt: null,
      updatedAt: null,
      source: 'live',
    };
  },
});

// Turns archiving of a chat on or off; turning it on also makes sure a job fetches
// what arrived while it was off.
async function setChannelSync(ctx, chat, enable) {
  const chatId = await ctx.messageSyncService.resolveChatKey(chat);
  const result = ctx.messageSyncService.setChannelSync(chatId, enable);
  const { job, queued } = enable
    ? ctx.messageSyncService.queueResyncJob(chatId)
    : { job: null, queued: false };
  return {
    channelId: result.channel_id,
    syncEnabled: Boolean(result.sync_enabled),
    jobId: job?.id ?? null,
    jobStatus: job?.status ?? null,
    jobQueued: queued,
  };
}

function renderSyncState(result, { print }) {
  if (result.syncEnabled) {
    const jobMessage = result.jobId
      ? result.jobQueued
        ? ` Backfill job queued (#${result.jobId}).`
        : ` Backfill job exists (#${result.jobId}, ${result.jobStatus}).`
      : '';
    print(
      `Sync enabled for ${result.channelId}.${jobMessage} ` +
        'Run `tgcli backfill --once` (or `tgcli backfill --follow`/`tgcli server`) to process.',
    );
  } else {
    print(`Sync disabled for ${result.channelId}`);
  }
}

export const channelsWatch = defineOperation({
  name: 'channelsWatch',
  command: 'channels watch',
  summary: 'Subscribe a chat for archiving (enable sync, queue a backfill)',
  annotations: { idempotent: true },
  input: { channelId: chatOption() },
  run: (ctx, args) => setChannelSync(ctx, args.channelId, true),
  render: renderSyncState,
});

export const channelsUnwatch = defineOperation({
  name: 'channelsUnwatch',
  command: 'channels unwatch',
  summary: 'Unsubscribe a chat from archiving (disable sync)',
  description: 'Stops archiving a chat, or with missing, every chat doctor lists as missing from the dialog list with no answer from Telegram.',
  annotations: { idempotent: true, destructive: true },
  input: {
    channelId: chatOption().optional(),
    missing: field.bool('Every chat doctor lists as missing from your dialogs with no answer from Telegram').optional()
      .flag('--missing'),
  },
  refine(args, fail) {
    if (args.channelId !== undefined && args.missing) {
      fail('Use either {channelId} or {missing}');
    } else if (args.channelId === undefined && !args.missing) {
      fail('{channelId} is required');
    }
  },
  async run(ctx, args) {
    if (args.missing) {
      return { ok: true, channelIds: ctx.messageSyncService.unwatchUnansweredChats() };
    }
    return setChannelSync(ctx, args.channelId, false);
  },
  render(result, out) {
    if (!result.channelIds) {
      renderSyncState(result, out);
    } else if (result.channelIds.length) {
      out.print(`Sync disabled for ${result.channelIds.length} chat(s): ${result.channelIds.join(', ')}.`);
    } else {
      out.print('No chat is missing from your dialogs without an answer from Telegram.');
    }
  },
});

// `channels watch`/`unwatch` as one hidden command with --enable/--disable.
export const channelsSync = defineOperation({
  name: 'channelsSync',
  command: 'channels sync',
  hidden: true,
  mcp: false,
  summary: 'Enable or disable sync (alias of watch/unwatch)',
  input: {
    channelId: chatOption(),
    enable: field.bool('Enable sync').optional().flag('--enable'),
    disable: field.bool('Disable sync').optional().flag('--disable'),
  },
  refine(args, fail) {
    if (args.enable && args.disable) {
      fail('Use either {enable} or {disable}');
    } else if (!args.enable && !args.disable) {
      fail('{enable} or {disable} is required');
    }
  },
  run: (ctx, args) => setChannelSync(ctx, args.channelId, Boolean(args.enable)),
  render: renderSyncState,
});

export const markChannelRead = defineOperation({
  name: 'markChannelRead',
  command: 'channels mark-read',
  summary: 'Mark channel as read up to a message',
  description: 'Marks a Telegram chat as read up to the given message ID (inclusive).',
  annotations: { idempotent: true },
  input: {
    channelId: chatOption(),
    messageId: field.int('Mark as read up to this message ID (inclusive)')
      .flag('--message-id <n>', { help: 'Mark as read up to this message ID' }),
  },
  run: (ctx, args) => ctx.telegramClient.markChannelRead(args.channelId, args.messageId),
  render(result, { print }) {
    print(`Marked channel ${result.channelId} as read up to message ${result.messageId}.`);
  },
});

export const CHANNEL_OPERATIONS = [
  listChannels,
  searchChannels,
  channelsShow,
  channelsWatch,
  channelsUnwatch,
  channelsSync,
  markChannelRead,
];
