// The archive keys a chat by its Telegram id. A username, or 'me', names a chat only
// for as long as the chat keeps it, and rows under it would sit apart from the ones
// updates write by id, so it is resolved to the id before anything is written.

// Marked ids of channels and supergroups lie at and below this one. Only their messages
// have ids of their own; private chats and basic groups share one sequence per account.
export const MAX_MARKED_CHANNEL_ID = -1000000000000;

export function isChatId(key) {
  const text = String(key ?? '').trim();
  return /^-?\d+$/.test(text) && String(Number(text)) === text;
}

// isChatId for a key column in SQL.
export function chatIdSql(column = 'channel_id') {
  return `CAST(CAST(${column} AS INTEGER) AS TEXT) = ${column}`;
}

// A chat as a caller names it, made comparable with archive keys: an id in its canonical
// digits, or a username as typed, which is no archive key until resolveChatKey resolves it.
// Throws for what names no chat at all.
export function chatKey(chat) {
  if (typeof chat === 'number' || typeof chat === 'bigint') {
    return String(Number(chat));
  }
  if (typeof chat !== 'string') {
    throw new Error('Invalid channel ID provided');
  }
  const trimmed = chat.trim();
  return /^-?\d+$/.test(trimmed) ? String(Number(trimmed)) : trimmed;
}

// A username gets to a write only through resolveChatKey, which turns it into the id.
export function chatIdForWrite(chat) {
  const key = chatKey(chat);
  if (!isChatId(key)) {
    throw new Error(`Chat ${key} has to be resolved to its Telegram id before it is archived.`);
  }
  return key;
}

// The form a channel tag is kept and compared in.
export function normalizeTag(tag) {
  if (!tag) return null;
  const normalized = String(tag).trim().toLowerCase();
  return normalized.replace(/\s+/g, ' ');
}
