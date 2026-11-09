// The archive's cache of chat metadata (title, username, about).

import { defineOperation, field } from './operation.js';
import { archiveKey, archiveKeys, chatOption, toChatList } from './chat-keys.js';

export const getChannelMetadata = defineOperation({
  name: 'getChannelMetadata',
  command: 'metadata get',
  summary: 'Show cached metadata',
  description: 'Returns cached metadata for a chat, or what Telegram says when the archive has none.',
  annotations: { readOnly: true },
  input: {
    channelId: chatOption(),
  },
  async run(ctx, args) {
    const metadata = ctx.messageSyncService.getChannelMetadata(await archiveKey(ctx, args.channelId));
    if (metadata) {
      return metadata;
    }
    const live = await ctx.telegramClient.getPeerMetadata(args.channelId);
    return {
      channelId: String(args.channelId),
      peerTitle: live?.peerTitle ?? null,
      peerType: live?.peerType ?? null,
      chatType: live?.chatType ?? null,
      isForum: live?.isForum ?? null,
      username: live?.username ?? null,
      about: live?.about ?? null,
      metadataUpdatedAt: null,
      source: 'live',
    };
  },
});

export const refreshChannelMetadata = defineOperation({
  name: 'refreshChannelMetadata',
  command: 'metadata refresh',
  summary: 'Refresh cached metadata',
  description: 'Fetches and caches extended metadata for chats.',
  annotations: { idempotent: true },
  input: {
    channelIds: field.chats('Chats to refresh (default: the archive picks)').optional()
      .flag('--chat <id|username>', { help: 'Channel identifier' }),
    limit: field.int('Maximum number of chats to refresh').default(20).flag('--limit <n>', { help: 'Limit channels' }),
    force: field.bool('Refresh even if cached metadata is fresh').optional().flag('--force', { help: 'Force refresh' }),
    onlyMissing: field.bool('Refresh only chats without cached metadata').optional()
      .flag('--only-missing', { help: 'Only refresh missing metadata' }),
  },
  async run(ctx, args) {
    const chats = toChatList(args.channelIds);
    return ctx.messageSyncService.refreshChannelMetadata({
      channelIds: chats.length ? await archiveKeys(ctx, chats, { telegram: true }) : null,
      limit: args.limit,
      force: Boolean(args.force),
      onlyMissing: Boolean(args.onlyMissing),
    });
  },
});

export const METADATA_OPERATIONS = [getChannelMetadata, refreshChannelMetadata];
