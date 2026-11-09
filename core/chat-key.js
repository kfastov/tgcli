// The archive keys a chat by its Telegram id. A username, or 'me', names a chat only
// for as long as the chat keeps it, and rows under it would sit apart from the ones
// updates write by id, so it is resolved to the id before anything is written.

export function isChatId(key) {
  const text = String(key ?? '').trim();
  return /^-?\d+$/.test(text) && String(Number(text)) === text;
}

// isChatId for a key column in SQL.
export function chatIdSql(column = 'channel_id') {
  return `CAST(CAST(${column} AS INTEGER) AS TEXT) = ${column}`;
}

// The archive key of a chat as a caller gives it: an id in its canonical digits, or a
// username as typed. Throws for what names no chat at all.
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
