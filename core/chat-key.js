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
