// The message archive: the tables that hold messages (`messages`, `message_links`,
// `message_media` and the FTS index `message_search`), one write path into them and one
// read projection out of them.
//
// A row keeps the stored message in `raw_json` (see core/message-codec.js) and a few
// columns that SQL filters on or indexes. Reads decode `raw_json` every time, so what a
// reader sees is whatever the codec makes of the stored TL today, the same message view
// a live read of the message returns.

import { MAX_MARKED_CHANNEL_ID, chatIdForWrite, chatKey, normalizeTag } from './chat-key.js';
import { parseIsoDate } from './duration.js';
import { decodeMessage, parseStoredMessage, storedSender } from './message-codec.js';
import { isoDate, toMessageView } from './message-view.js';

const SEARCH_INDEX_VERSION = 2;
const MEDIA_INDEX_VERSION = 1;
const URL_PATTERN = /https?:\/\/[^\s<>"')]+/giu;
const FILE_NAME_PATTERN = /\b[\w.\-]+\.[a-z0-9]{2,7}\b/iu;
const MAX_FILENAME_SCAN_DEPTH = 5;
const BACKFILL_BATCH_SIZE = 500;
// What SQLite says of a MATCH it cannot parse as FTS5 query syntax.
const FTS_SYNTAX_ERROR = /fts5|syntax error|no such column|unterminated string|unknown special query/i;

const VIEW_COLUMNS = `
  messages.channel_id,
  messages.message_id,
  messages.date,
  messages.from_id,
  messages.text,
  messages.topic_id,
  messages.raw_json,
  channels.peer_title,
  channels.username,
  users.username AS from_username,
  users.display_name AS from_display_name,
  users.peer_type AS from_peer_type,
  users.is_bot AS from_is_bot
`;
const VIEW_JOINS = `
  LEFT JOIN channels ON channels.channel_id = messages.channel_id
  LEFT JOIN users ON users.user_id = messages.from_id
`;

// --- search fields ---

function extractLinksFromText(text) {
  if (!text || typeof text !== 'string') {
    return [];
  }
  const results = new Set();
  for (const raw of text.match(URL_PATTERN) ?? []) {
    const cleaned = raw.replace(/[),.!?;:]+$/g, '');
    if (cleaned) {
      results.add(cleaned);
    }
  }
  return [...results];
}

function collectFileNames(value, results, depth = 0) {
  if (!value || depth > MAX_FILENAME_SCAN_DEPTH) {
    return;
  }
  if (Array.isArray(value)) {
    for (const entry of value) {
      collectFileNames(entry, results, depth + 1);
    }
    return;
  }
  if (typeof value !== 'object' || value instanceof Uint8Array) {
    return;
  }
  for (const [key, entry] of Object.entries(value)) {
    if ((key === 'fileName' || key === 'file_name') && typeof entry === 'string') {
      results.add(entry);
    } else if (key === 'name' && typeof entry === 'string' && FILE_NAME_PATTERN.test(entry)) {
      results.add(entry);
    } else {
      collectFileNames(entry, results, depth + 1);
    }
  }
}

function extractFileNames(stored) {
  const results = new Set();
  const text = stored?.text ?? stored?.message ?? null;
  if (typeof text === 'string') {
    for (const match of text.match(new RegExp(FILE_NAME_PATTERN.source, 'giu')) ?? []) {
      results.add(match);
    }
  }
  if (stored?.raw) {
    collectFileNames(stored.raw, results);
  }
  return [...results];
}

function buildSenderText(stored) {
  const parts = [stored?.from_username, stored?.from_display_name, stored?.from_id]
    .filter(Boolean)
    .map(String);
  return parts.length ? parts.join(' ') : null;
}

function buildTopicText(stored) {
  if (stored?.topic_id !== null && stored?.topic_id !== undefined) {
    return String(stored.topic_id);
  }
  return null;
}

function buildLinkEntries(links) {
  return links.map((url) => {
    let domain = null;
    try {
      domain = new URL(url).hostname || null;
    } catch {
      domain = null;
    }
    return { url, domain };
  });
}

// The columns the FTS index covers besides the text, and the rows of message_links.
function buildSearchFields(stored) {
  const links = extractLinksFromText(stored?.text ?? stored?.message ?? null);
  const files = extractFileNames(stored);
  return {
    linksText: links.length ? links.join(' ') : null,
    filesText: files.length ? files.join(' ') : null,
    senderText: buildSenderText(stored),
    topicText: buildTopicText(stored),
    linkEntries: buildLinkEntries(links),
  };
}

function buildMediaRow(summary) {
  if (!summary?.type) {
    return null;
  }
  let extraJson = null;
  if (summary.extras && typeof summary.extras === 'object') {
    try {
      extraJson = JSON.stringify(summary.extras);
    } catch {
      extraJson = null;
    }
  }
  return {
    media_type: summary.type,
    file_id: summary.fileId ?? null,
    unique_file_id: summary.uniqueFileId ?? null,
    file_name: summary.fileName ?? null,
    mime_type: summary.mimeType ?? null,
    file_size: summary.fileSize ?? null,
    width: summary.width ?? null,
    height: summary.height ?? null,
    duration: summary.duration ?? null,
    extra_json: extraJson,
  };
}

// --- users ---

function userRow(peer) {
  const flag = (value) => (typeof value === 'boolean' ? (value ? 1 : 0) : null);
  return {
    user_id: String(peer.id),
    peer_type: peer.peerType ?? null,
    username: peer.username ?? null,
    display_name: peer.displayName ?? null,
    phone: peer.phone ?? null,
    is_contact: flag(peer.isContact),
    is_bot: flag(peer.isBot),
  };
}

// --- reads ---

const FTS_OPERATORS = new Set(['AND', 'OR', 'NOT']);

function quoteFtsTerm(term) {
  const prefix = term.length > 1 && term.endsWith('*');
  const word = prefix ? term.slice(0, -1) : term;
  return `"${word.replace(/"/g, '""')}"${prefix ? '*' : ''}`;
}

// Each word quoted as a phrase, so punctuation inside it is no syntax. With keepOperators,
// AND, OR and NOT between two words and a trailing * stay what they mean in FTS5.
function quoteFtsQuery(query, { keepOperators = false } = {}) {
  const terms = query.split(/\s+/).filter(Boolean);
  return terms
    .map((term, index) => {
      const between = index > 0 && index < terms.length - 1;
      if (keepOperators && between && FTS_OPERATORS.has(term)) {
        return term;
      }
      return keepOperators ? quoteFtsTerm(term) : `"${term.replace(/"/g, '""')}"`;
    })
    .join(' ');
}

function senderOfRow(row, message) {
  const snapshot = message.sender ?? {};
  const sender = {
    username: row.from_username ?? snapshot.username ?? null,
    displayName: row.from_display_name ?? snapshot.displayName ?? null,
    peerType: row.from_peer_type ?? snapshot.peerType ?? null,
    isBot: typeof row.from_is_bot === 'number' ? Boolean(row.from_is_bot) : snapshot.isBot ?? null,
  };
  return sender;
}

// The columns are what SQL filters on, and the view agrees with them.
function viewOfRow(row) {
  const parsed = parseStoredMessage(row.raw_json) ?? {};
  const stored = parsed.raw
    ? parsed
    : { ...parsed, id: row.message_id, date: row.date, text: row.text ?? '', from_id: row.from_id, topic_id: row.topic_id };
  const message = decodeMessage(stored);
  return toMessageView(
    {
      ...message,
      id: row.message_id,
      date: row.date ?? message.date,
      fromId: row.from_id && row.from_id !== 'unknown' ? row.from_id : message.fromId,
      text: row.text ?? message.text,
      topicId: row.topic_id ?? null,
    },
    {
      chat: { channelId: row.channel_id, peerTitle: row.peer_title ?? null, username: row.username ?? null },
      sender: senderOfRow(row, message),
    },
  );
}

function filterClauses(filters) {
  const clauses = [];
  const params = [];
  const chats = (filters.channelIds ?? []).map((id) => chatKey(id)).filter(Boolean);
  if (chats.length) {
    clauses.push(`messages.channel_id IN (${chats.map(() => '?').join(', ')})`);
    params.push(...chats);
  }
  if (typeof filters.topicId === 'number') {
    clauses.push('messages.topic_id = ?');
    params.push(filters.topicId);
  }
  if (filters.fromDate) {
    clauses.push('messages.date >= ?');
    params.push(parseIsoDate(filters.fromDate, 'fromDate'));
  }
  if (filters.toDate) {
    clauses.push('messages.date <= ?');
    params.push(parseIsoDate(filters.toDate, 'toDate'));
  }
  if (Number.isFinite(filters.beforeId) && filters.beforeId > 0) {
    clauses.push('messages.message_id < ?');
    params.push(Number(filters.beforeId));
  }
  if (Number.isFinite(filters.afterId) && filters.afterId > 0) {
    clauses.push('messages.message_id > ?');
    params.push(Number(filters.afterId));
  }
  return { clauses, params };
}

function asList(value) {
  if (Array.isArray(value)) {
    return value;
  }
  return value ? [value] : [];
}

export default class MessageArchive {
  constructor(db) {
    this.db = db;
  }

  // Creates the message tables, brings the FTS index up to date and prepares the
  // statements. The users and channels tables, which reads join, are the caller's to
  // create; the archive keeps the senders of what it writes in users.
  init() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_id TEXT NOT NULL,
        message_id INTEGER NOT NULL,
        topic_id INTEGER,
        date INTEGER,
        from_id TEXT,
        text TEXT,
        links TEXT,
        files TEXT,
        sender TEXT,
        topic TEXT,
        raw_json TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(channel_id, message_id)
      );

      CREATE TABLE IF NOT EXISTS message_links (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_id TEXT NOT NULL,
        message_id INTEGER NOT NULL,
        url TEXT NOT NULL,
        domain TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(channel_id, message_id, url)
      );
      CREATE INDEX IF NOT EXISTS message_links_url_idx ON message_links (url);
      CREATE INDEX IF NOT EXISTS message_links_domain_idx ON message_links (domain);

      CREATE TABLE IF NOT EXISTS message_media (
        channel_id TEXT NOT NULL,
        message_id INTEGER NOT NULL,
        media_type TEXT,
        file_id TEXT,
        unique_file_id TEXT,
        file_name TEXT,
        mime_type TEXT,
        file_size INTEGER,
        width INTEGER,
        height INTEGER,
        duration INTEGER,
        extra_json TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (channel_id, message_id)
      );
      CREATE INDEX IF NOT EXISTS message_media_type_idx ON message_media (media_type);
      CREATE INDEX IF NOT EXISTS message_media_mime_idx ON message_media (mime_type);
      CREATE INDEX IF NOT EXISTS message_media_name_idx ON message_media (file_name);

      CREATE TABLE IF NOT EXISTS search_meta (
        key TEXT PRIMARY KEY,
        value TEXT
      );
    `);
    for (const column of ['topic_id INTEGER', 'links TEXT', 'files TEXT', 'sender TEXT', 'topic TEXT']) {
      this._ensureMessageColumn(...column.split(' '));
    }

    this._prepareStatements();
    this._initSearchIndex();
  }

  _ensureMessageColumn(column, definition) {
    const existing = this.db.prepare('PRAGMA table_info(messages)').all();
    if (!existing.some((col) => col.name === column)) {
      this.db.exec(`ALTER TABLE messages ADD COLUMN ${column} ${definition}`);
    }
  }

  _metaVersion(key) {
    return Number(this.db.prepare('SELECT value FROM search_meta WHERE key = ?').get(key)?.value ?? 0);
  }

  _setMetaVersion(key, version) {
    this.db.prepare(`
      INSERT INTO search_meta (key, value) VALUES (?, ?)
      ON CONFLICT(key) DO UPDATE SET value = excluded.value
    `).run(key, String(version));
  }

  _initSearchIndex() {
    const searchSchema = this.db.prepare(`
      SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'message_search'
    `).get();
    const needsVersionRebuild = this._metaVersion('search_index_version') !== SEARCH_INDEX_VERSION;
    const needsMediaRebuild = this._metaVersion('media_index_version') !== MEDIA_INDEX_VERSION;
    const needsSearchRecreate = !searchSchema?.sql
      || !searchSchema.sql.includes("tokenize='unicode61'")
      || !searchSchema.sql.includes('links')
      || needsVersionRebuild;

    if (needsSearchRecreate) {
      this.db.exec(`
        DROP TRIGGER IF EXISTS messages_ai;
        DROP TRIGGER IF EXISTS messages_ad;
        DROP TRIGGER IF EXISTS messages_au;
        DROP TABLE IF EXISTS message_search;
      `);
    }
    if (needsVersionRebuild || needsMediaRebuild) {
      this._backfillSearchFields({ rebuildMedia: needsMediaRebuild });
    }

    this.db.exec(`
      CREATE VIRTUAL TABLE IF NOT EXISTS message_search USING fts5(
        text, links, files, sender, topic,
        content='messages',
        content_rowid='id',
        tokenize='unicode61'
      );

      CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
        INSERT INTO message_search(rowid, text, links, files, sender, topic)
        VALUES (new.id, COALESCE(new.text, ''), COALESCE(new.links, ''), COALESCE(new.files, ''),
                COALESCE(new.sender, ''), COALESCE(new.topic, ''));
      END;

      CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
        INSERT INTO message_search(message_search, rowid, text, links, files, sender, topic)
        VALUES ('delete', old.id, COALESCE(old.text, ''), COALESCE(old.links, ''), COALESCE(old.files, ''),
                COALESCE(old.sender, ''), COALESCE(old.topic, ''));
      END;

      CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE ON messages BEGIN
        INSERT INTO message_search(message_search, rowid, text, links, files, sender, topic)
        VALUES ('delete', old.id, COALESCE(old.text, ''), COALESCE(old.links, ''), COALESCE(old.files, ''),
                COALESCE(old.sender, ''), COALESCE(old.topic, ''));
        INSERT INTO message_search(rowid, text, links, files, sender, topic)
        VALUES (new.id, COALESCE(new.text, ''), COALESCE(new.links, ''), COALESCE(new.files, ''),
                COALESCE(new.sender, ''), COALESCE(new.topic, ''));
      END;

      CREATE INDEX IF NOT EXISTS messages_channel_topic_idx ON messages (channel_id, topic_id, message_id);
    `);

    if (needsSearchRecreate) {
      this.db.prepare("INSERT INTO message_search(message_search) VALUES ('rebuild')").run();
      this._setMetaVersion('search_index_version', SEARCH_INDEX_VERSION);
    }
    if (needsMediaRebuild) {
      this._setMetaVersion('media_index_version', MEDIA_INDEX_VERSION);
    }
  }

  _prepareStatements() {
    const columns = 'channel_id, message_id, topic_id, date, from_id, text, links, files, sender, topic, raw_json';
    const values = '@channel_id, @message_id, @topic_id, @date, @from_id, @text, @links, @files, @sender, @topic, @raw_json';
    this.insertMessageStmt = this.db.prepare(`
      INSERT OR IGNORE INTO messages (${columns}) VALUES (${values})
    `);
    this.upsertMessageStmt = this.db.prepare(`
      INSERT INTO messages (${columns}) VALUES (${values})
      ON CONFLICT(channel_id, message_id) DO UPDATE SET
        topic_id = excluded.topic_id,
        date = excluded.date,
        from_id = excluded.from_id,
        text = excluded.text,
        links = excluded.links,
        files = excluded.files,
        sender = excluded.sender,
        topic = excluded.topic,
        raw_json = excluded.raw_json
    `);
    this.deleteLinksStmt = this.db.prepare('DELETE FROM message_links WHERE channel_id = ? AND message_id = ?');
    this.insertLinkStmt = this.db.prepare(`
      INSERT OR IGNORE INTO message_links (channel_id, message_id, url, domain)
      VALUES (@channel_id, @message_id, @url, @domain)
    `);
    this.deleteMediaStmt = this.db.prepare('DELETE FROM message_media WHERE channel_id = ? AND message_id = ?');
    this.upsertMediaStmt = this.db.prepare(`
      INSERT INTO message_media (
        channel_id, message_id, media_type, file_id, unique_file_id, file_name, mime_type,
        file_size, width, height, duration, extra_json, updated_at
      )
      VALUES (
        @channel_id, @message_id, @media_type, @file_id, @unique_file_id, @file_name, @mime_type,
        @file_size, @width, @height, @duration, @extra_json, CURRENT_TIMESTAMP
      )
      ON CONFLICT(channel_id, message_id) DO UPDATE SET
        media_type = excluded.media_type,
        file_id = excluded.file_id,
        unique_file_id = excluded.unique_file_id,
        file_name = excluded.file_name,
        mime_type = excluded.mime_type,
        file_size = excluded.file_size,
        width = excluded.width,
        height = excluded.height,
        duration = excluded.duration,
        extra_json = excluded.extra_json,
        updated_at = CURRENT_TIMESTAMP
    `);
    this.upsertUserStmt = this.db.prepare(`
      INSERT INTO users (user_id, peer_type, username, display_name, phone, is_contact, is_bot, updated_at)
      VALUES (@user_id, @peer_type, @username, @display_name, @phone, @is_contact, @is_bot, CURRENT_TIMESTAMP)
      ON CONFLICT(user_id) DO UPDATE SET
        peer_type = COALESCE(excluded.peer_type, users.peer_type),
        username = COALESCE(excluded.username, users.username),
        display_name = COALESCE(excluded.display_name, users.display_name),
        phone = COALESCE(excluded.phone, users.phone),
        is_contact = COALESCE(excluded.is_contact, users.is_contact),
        is_bot = COALESCE(excluded.is_bot, users.is_bot),
        updated_at = CURRENT_TIMESTAMP
    `);
    this.updateTopicTextStmt = this.db.prepare('UPDATE messages SET topic = ? WHERE channel_id = ? AND topic_id = ?');

    this.writeTx = this.db.transaction((records, users, replace) => {
      this.upsertUsersTx(users);
      const statement = replace ? this.upsertMessageStmt : this.insertMessageStmt;
      let written = 0;
      for (const { row, linkEntries, media } of records) {
        if (statement.run(row).changes === 0) {
          continue;
        }
        written += 1;
        this._replaceLinks(row, linkEntries);
        this._replaceMedia(row, media);
      }
      return written;
    });
    this.upsertUsersTx = this.db.transaction((users) => {
      for (const user of users) {
        this.upsertUserStmt.run(userRow(user));
      }
    });
  }

  // --- writes ---

  _record(chatId, stored) {
    const fields = buildSearchFields(stored);
    return {
      row: {
        channel_id: chatId,
        message_id: stored.id,
        topic_id: stored.topic_id ?? null,
        date: stored.date ?? null,
        from_id: stored.from_id ?? null,
        text: stored.text ?? null,
        links: fields.linksText,
        files: fields.filesText,
        sender: fields.senderText,
        topic: fields.topicText,
        raw_json: JSON.stringify(stored),
      },
      linkEntries: fields.linkEntries,
      media: buildMediaRow(stored.media ?? (stored.raw?.media ? decodeMessage(stored)?.media : null)),
    };
  }

  _replaceLinks(row, linkEntries) {
    this.deleteLinksStmt.run(row.channel_id, row.message_id);
    for (const entry of linkEntries) {
      this.insertLinkStmt.run({
        channel_id: row.channel_id,
        message_id: row.message_id,
        url: entry.url,
        domain: entry.domain ?? null,
      });
    }
  }

  _replaceMedia(row, media) {
    if (!media) {
      this.deleteMediaStmt.run(row.channel_id, row.message_id);
      return;
    }
    this.upsertMediaStmt.run({ channel_id: row.channel_id, message_id: row.message_id, ...media });
  }

  // Writes stored messages of one chat in one transaction, with their senders. A message
  // already archived is kept unless `replace` says the new copy wins, as an edit does.
  // `senders` are peer descriptions (describePeer) that know more of a sender than the
  // message's own snapshot, such as a phone number. Returns how many rows it wrote.
  write(chatId, messages, { replace = false, senders = [] } = {}) {
    const key = chatIdForWrite(chatId);
    const users = new Map();
    for (const stored of messages) {
      const sender = storedSender(stored);
      if (sender) {
        users.set(sender.id, sender);
      }
    }
    for (const sender of senders) {
      if (sender?.id) {
        users.set(String(sender.id), { ...users.get(String(sender.id)), ...sender, id: String(sender.id) });
      }
    }
    const records = messages.map((stored) => this._record(key, stored));
    return this.writeTx(records, [...users.values()], replace);
  }

  upsertUsers(peers) {
    const users = peers.filter((peer) => peer?.id);
    if (users.length) {
      this.upsertUsersTx(users);
    }
    return users.length;
  }

  setTopicTitle(chatId, topicId, title) {
    this.updateTopicTextStmt.run(title, chatKey(chatId), topicId);
  }

  // Deletes messages of a channel or supergroup, whose ids are its own.
  deleteChannelMessages(chatId, messageIds) {
    this._deleteWhere('channel_id = ?', [chatKey(chatId)], messageIds);
  }

  // Deletes messages of private chats and basic groups, which share one id sequence
  // across the account, so Telegram names the message without its chat.
  deleteSharedSequenceMessages(messageIds) {
    this._deleteWhere(`channel_id IN (
      SELECT channel_id FROM channels
      WHERE peer_type IN ('chat', 'user') AND CAST(channel_id AS INTEGER) > ${MAX_MARKED_CHANNEL_ID}
    )`, [], messageIds);
  }

  _deleteWhere(chatClause, chatParams, messageIds) {
    if (!messageIds.length) {
      return;
    }
    const placeholders = messageIds.map(() => '?').join(', ');
    for (const table of ['messages', 'message_links', 'message_media']) {
      this.db.prepare(`
        DELETE FROM ${table} WHERE ${chatClause} AND message_id IN (${placeholders})
      `).run(...chatParams, ...messageIds);
    }
  }

  _backfillSearchFields({ rebuildMedia = false } = {}) {
    const selectStmt = this.db.prepare(`
      SELECT id, channel_id, message_id, raw_json, text, topic_id, from_id
      FROM messages WHERE id > ? ORDER BY id ASC LIMIT ?
    `);
    const updateStmt = this.db.prepare(`
      UPDATE messages SET links = ?, files = ?, sender = ?, topic = ? WHERE id = ?
    `);
    const applyBatch = this.db.transaction((rows) => {
      for (const row of rows) {
        const parsed = parseStoredMessage(row.raw_json) ?? {};
        const stored = {
          ...parsed,
          text: parsed.text ?? row.text ?? null,
          topic_id: parsed.topic_id ?? row.topic_id ?? null,
          from_id: parsed.from_id ?? row.from_id ?? null,
        };
        const fields = buildSearchFields(stored);
        updateStmt.run(fields.linksText, fields.filesText, fields.senderText, fields.topicText, row.id);
        this._replaceLinks(row, fields.linkEntries);
        if (rebuildMedia) {
          this._replaceMedia(row, buildMediaRow(decodeMessage(stored)?.media));
        }
      }
    });

    let lastId = 0;
    while (true) {
      const rows = selectStmt.all(lastId, BACKFILL_BATCH_SIZE);
      if (!rows.length) {
        break;
      }
      applyBatch(rows);
      lastId = rows[rows.length - 1].id;
    }
  }

  // --- reads ---

  list(filters = {}) {
    const { clauses, params } = filterClauses({ ...filters, channelIds: asList(filters.channelIds) });
    const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
    const limit = filters.limit && filters.limit > 0 ? Number(filters.limit) : 50;
    const rows = this.db.prepare(`
      SELECT ${VIEW_COLUMNS} FROM messages ${VIEW_JOINS}
      ${where}
      ORDER BY messages.date DESC
      LIMIT ?
    `).all(...params, limit);
    return rows.map(viewOfRow);
  }

  get(chatId, messageId) {
    const row = this.db.prepare(`
      SELECT ${VIEW_COLUMNS} FROM messages ${VIEW_JOINS}
      WHERE messages.channel_id = ? AND messages.message_id = ?
    `).get(chatKey(chatId), Number(messageId));
    return row ? viewOfRow(row) : null;
  }

  context(chatId, messageId, { before = 20, after = 20 } = {}) {
    const key = chatKey(chatId);
    const target = this.get(key, messageId);
    if (!target) {
      return { target: null, before: [], after: [] };
    }
    const around = (comparison, order, count) => (count > 0
      ? this.db.prepare(`
          SELECT ${VIEW_COLUMNS} FROM messages ${VIEW_JOINS}
          WHERE messages.channel_id = ? AND messages.message_id ${comparison} ?
          ORDER BY messages.message_id ${order}
          LIMIT ?
        `).all(key, Number(messageId), Number(count)).map(viewOfRow)
      : []);
    return {
      target,
      before: around('<', 'DESC', before).reverse(),
      after: around('>', 'ASC', after),
    };
  }

  // Full-text search, by FTS5 query syntax when the query parses as one. One that does
  // not, as "UTF-8 OR utf8" or "C++" would not, is searched by its words as phrases,
  // keeping AND, OR, NOT and a trailing * if that parses, and without them if not.
  search(options = {}) {
    const query = typeof options.query === 'string' ? options.query.trim() : '';
    const regexText = typeof options.regex === 'string' ? options.regex.trim() : '';
    const tags = asList(options.tags ?? options.tag).map(normalizeTag).filter(Boolean);
    const limit = options.limit && options.limit > 0 ? Number(options.limit) : 100;
    const caseInsensitive = options.caseInsensitive !== false;

    let regex = null;
    if (regexText) {
      try {
        regex = new RegExp(regexText, caseInsensitive ? 'i' : '');
      } catch (error) {
        throw new Error(`Invalid regex: ${error.message}`);
      }
    }

    const { clauses, params } = filterClauses({ ...options, channelIds: asList(options.channelIds) });
    if (tags.length) {
      clauses.unshift(`EXISTS (
        SELECT 1 FROM channel_tags
        WHERE channel_tags.channel_id = messages.channel_id
          AND channel_tags.tag IN (${tags.map(() => '?').join(', ')})
      )`);
      params.unshift(...tags);
    }
    const preLimit = regex ? Math.min(limit * 5, 1000) : limit;

    const run = (match) => {
      const matchClauses = match ? ['message_search MATCH ?', ...clauses] : clauses;
      const where = matchClauses.length ? `WHERE ${matchClauses.join(' AND ')}` : '';
      const from = match
        ? 'FROM message_search JOIN messages ON messages.id = message_search.rowid'
        : 'FROM messages';
      return this.db.prepare(`
        SELECT ${VIEW_COLUMNS} ${from} ${VIEW_JOINS}
        ${where}
        ORDER BY messages.date DESC
        LIMIT ?
      `).all(...(match ? [match] : []), ...params, preLimit);
    };

    // Each try falls to the next only on a query FTS5 cannot parse.
    const tries = query
      ? [query, quoteFtsQuery(query, { keepOperators: true }), quoteFtsQuery(query)]
      : [null];
    let rows;
    for (const [index, match] of tries.entries()) {
      try {
        rows = run(match);
        break;
      } catch (error) {
        if (index === tries.length - 1 || !FTS_SYNTAX_ERROR.test(error?.message ?? '')) {
          throw error;
        }
      }
    }

    let results = rows.map(viewOfRow);
    if (regex) {
      results = results.filter((message) => regex.test(message.text || ''));
    }
    return results.slice(0, limit);
  }

  count(chatId) {
    return this.db.prepare('SELECT COUNT(*) AS n FROM messages WHERE channel_id = ?').get(chatKey(chatId)).n;
  }

  // How many messages under a key name each chat in their TL: rows of { type, id, n },
  // type and id null for a row whose raw_json says nothing. Moving a chat off a username
  // key goes by it.
  countByPeer(key) {
    return this.db.prepare(`
      SELECT type, id, COUNT(*) AS n FROM (
        SELECT
          CASE WHEN json_valid(raw_json) THEN json_extract(raw_json, '$.raw.peerId._') END AS type,
          CASE WHEN json_valid(raw_json) THEN COALESCE(
            json_extract(raw_json, '$.raw.peerId.userId'),
            json_extract(raw_json, '$.raw.peerId.chatId'),
            json_extract(raw_json, '$.raw.peerId.channelId')
          ) END AS id
        FROM messages
        WHERE channel_id = ?
      )
      GROUP BY type, id
    `).all(key);
  }

  // How many of these ids the archive holds for the chat.
  countPresent(chatId, messageIds) {
    return this.db.prepare(`
      SELECT COUNT(*) AS present FROM messages
      WHERE channel_id = ? AND message_id IN (SELECT value FROM json_each(?))
    `).get(chatKey(chatId), JSON.stringify(messageIds)).present;
  }

  // The archived ids of the chat strictly between two ids.
  idsBetween(chatId, afterId, beforeId) {
    return new Set(this.db.prepare(`
      SELECT message_id FROM messages WHERE channel_id = ? AND message_id > ? AND message_id < ?
    `).all(chatKey(chatId), afterId, beforeId).map((row) => row.message_id));
  }

  stats(chatId) {
    const summary = this.db.prepare(`
      SELECT COUNT(*) AS total, MIN(message_id) AS oldestMessageId, MAX(message_id) AS newestMessageId,
             MIN(date) AS oldestDate, MAX(date) AS newestDate
      FROM messages WHERE channel_id = ?
    `).get(chatKey(chatId));
    return {
      total: summary.total || 0,
      oldestMessageId: summary.oldestMessageId || null,
      newestMessageId: summary.newestMessageId || null,
      oldestDate: isoDate(summary.oldestDate),
      newestDate: isoDate(summary.newestDate),
    };
  }

  searchStatus() {
    const row = this.db.prepare(`
      SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'message_search'
    `).get();
    const version = this._metaVersion('search_index_version');
    return { enabled: Boolean(row?.name), version: version || null };
  }
}
