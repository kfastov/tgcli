// Tags on chats, set by hand or by the auto-tagger, and finding chats by tag.

import { defineOperation, field } from './operation.js';
import { archiveKey, archiveKeys, chatOption, telegramForUsername, toChatList } from './chat-keys.js';

const sourceInput = (help) => field.text(help).optional().flag('--source <source>', { help: 'Tag source' });

export const setChannelTags = defineOperation({
  name: 'setChannelTags',
  command: 'tags set',
  summary: 'Set channel tags',
  description: "Replaces a chat's tags from one source, for later cross-chat search.",
  annotations: { idempotent: true, destructive: true },
  telegram: telegramForUsername,
  input: {
    channelId: chatOption(),
    tags: field.strings('Tags to attach to the chat; an empty list clears them')
      .flag('--tags <tags>', { help: 'Comma-separated tags', aliases: [{ flag: '--tag <tag>', help: 'Tag' }] }),
    source: sourceInput('Tag source label (default: manual)'),
  },
  async run(ctx, args) {
    const chatId = await ctx.messageSyncService.resolveChatKey(args.channelId);
    const tags = ctx.messageSyncService.setChannelTags(chatId, args.tags, { source: args.source });
    return { channelId: chatId, tags };
  },
  render(result, { args, print }) {
    print(`Tags set for ${args.channelId}: ${result.tags.join(', ')}`);
  },
});

export const listChannelTags = defineOperation({
  name: 'listChannelTags',
  command: 'tags list',
  summary: 'List channel tags',
  description: 'Lists the tags attached to a chat.',
  annotations: { readOnly: true },
  telegram: telegramForUsername,
  input: {
    channelId: chatOption(),
    source: sourceInput('Only tags from this source'),
  },
  run: async (ctx, args) => ctx.messageSyncService.listChannelTags(await archiveKey(ctx, args.channelId), { source: args.source }),
  render(tags, { print }) {
    print(tags.map((tag) => tag.tag).join(', '));
  },
});

export const listTaggedChannels = defineOperation({
  name: 'listTaggedChannels',
  command: 'tags search',
  summary: 'Search channels by tag',
  description: 'Lists the chats that carry a tag.',
  annotations: { readOnly: true },
  telegram: false,
  input: {
    tag: field.text('Tag to look up').flag('--tag <tag>', { help: 'Tag to search' }),
    source: sourceInput('Only tags from this source'),
    limit: field.int('Maximum number of chats to return').default(100).flag('--limit <n>', { help: 'Limit results' }),
  },
  run: (ctx, args) => ctx.messageSyncService.listTaggedChannels(args.tag, { source: args.source, limit: args.limit }),
  render(channels, { print }) {
    for (const channel of channels) {
      const label = channel.peerTitle || channel.username || channel.channelId;
      print(`${label} (${channel.channelId})`);
    }
  },
});

export const autoTagChannels = defineOperation({
  name: 'autoTagChannels',
  command: 'tags auto',
  summary: 'Auto-tag channels',
  description: 'Tags chats from their title, username and cached metadata, replacing the tags it gave them before.',
  annotations: { destructive: true, idempotent: true },
  input: {
    channelIds: field.chats('Chats to tag (default: the archive picks)').optional()
      .flag('--chat <id|username>', { help: 'Channel identifier' }),
    limit: field.int('Maximum number of chats to tag').default(50).flag('--limit <n>', { help: 'Limit channels' }),
    source: sourceInput('Tag source label (default: auto)'),
    refreshMetadata: field.bool('Refresh cached metadata before tagging').default(true)
      .flag('--no-refresh-metadata', { presence: false, help: 'Skip metadata refresh' }),
  },
  async run(ctx, args) {
    const chats = toChatList(args.channelIds);
    return ctx.messageSyncService.autoTagChannels({
      channelIds: chats.length ? await archiveKeys(ctx, chats, { telegram: true }) : null,
      limit: args.limit,
      source: args.source,
      refreshMetadata: args.refreshMetadata,
    });
  },
  render(results, { print }) {
    for (const entry of results) {
      print(`${entry.channelId}: ${entry.tags.map((tag) => tag.tag).join(', ')}`);
    }
  },
});

export const TAG_OPERATIONS = [setChannelTags, listChannelTags, listTaggedChannels, autoTagChannels];
