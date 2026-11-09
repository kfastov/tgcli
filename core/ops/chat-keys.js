// How operations turn the chats a caller names into the keys the archive keeps them
// under.

import { isChatId } from '../chat-key.js';
import { field } from './operation.js';

// The `--chat` option most operations take, under the key `channelId`.
export function chatOption(help = 'Chat ID or username', cliHelp = 'Channel identifier') {
  return field.chat(help).flag('--chat <id|username>', { help: cliHelp });
}

// Archive reads by username look under the chat's id, where writes put it, found without
// Telegram when the archive or the peer cache knows the username. Reads that ask Telegram
// anyway, and writes, take Telegram's word (telegram), which a stored username can lag
// behind. A username Telegram does not know is looked up as given.
export async function archiveKey(ctx, chat, { telegram = false } = {}) {
  if (isChatId(chat)) {
    return chat;
  }
  const service = ctx.messageSyncService;
  try {
    return await (telegram ? service.resolveChatKey(chat) : service.resolveChatKeyForRead(chat));
  } catch (error) {
    if (error?.code === 'CHAT_NOT_FOUND') {
      return String(chat).trim();
    }
    throw error;
  }
}

export async function archiveKeys(ctx, chats, options = {}) {
  const keys = [];
  for (const chat of chats) {
    keys.push(await archiveKey(ctx, chat, options));
  }
  return keys;
}

// For an operation that reads the archive by `channelId`: an id is the archive key
// as it is, while a username needs Telegram to find the id.
export function telegramForUsername(args) {
  return !isChatId(args.channelId);
}

// One chat or a list of them, as a list without blanks.
export function toChatList(value) {
  if (Array.isArray(value)) {
    return value.filter((id) => id !== null && id !== undefined && String(id).trim() !== '');
  }
  if (value === null || value === undefined || String(value).trim() === '') {
    return [];
  }
  return [value];
}
