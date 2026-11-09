// Chats are archived under their Telegram id. Older versions stored a chat
// given by username (backfill --chat @name, channels watch, tags, topics) under that
// username, next to the rows updates write by id. Every way in now resolves the
// username first, and the server moves what is left under one to the chat's id.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { MtPeerNotFoundError } from '@mtcute/core';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import MessageSyncService from '../message-sync-service.js';
import { OPERATIONS } from '../core/operations.js';
import { createTelegramConnector } from '../core/telegram-connection.js';

const BOT_ID = '700000002';
const SELF_ID = '700000001';
const DAY_MS = 24 * 60 * 60 * 1000;

function peer(id, username, type = 'user') {
  return { id: Number(id), type, displayName: `Peer ${id}`, username, usernames: [{ username }] };
}

describe('chats given by username', () => {
  let storeDir;
  let services;
  let peers;
  let getPeer;
  let resolvePeer;

  // Telegram as far as these tests need it: peers by username, in mtcute's shapes.
  const makeService = () => {
    getPeer = vi.fn(async (ref) => {
      const found = typeof ref === 'object' ? peers.get(`#${ref.id}`) : peers.get(String(ref).replace(/^@/, '').toLowerCase());
      if (!found) {
        throw new MtPeerNotFoundError(`Peer with username ${ref} was not found`);
      }
      return found;
    });
    resolvePeer = vi.fn(async (ref) => {
      const found = peers.get(`fresh:${String(ref).replace(/^@/, '').toLowerCase()}`);
      if (!found) {
        throw new MtPeerNotFoundError(`Peer with username ${ref} was not found`);
      }
      return { id: found.id };
    });
    const service = new MessageSyncService({ client: { getPeer, resolvePeer } }, {
      dbPath: path.join(storeDir, 'messages.db'),
    });
    service.processQueue = vi.fn(async () => {});
    services.push(service);
    return service;
  };

  // The rows an older version left: a chat row typed 'chat', a job, a repair, and
  // messages whose raw copy names the chat they came from.
  const seedUsernameChat = (service, key, { messageIds = [], peerId = BOT_ID, job = 'idle', repair = 'idle', target = 1000 } = {}) => {
    const db = service.db;
    db.prepare(`
      INSERT INTO channels (channel_id, peer_type, sync_enabled, last_message_id, oldest_message_id, created_at)
      VALUES (?, 'chat', 1, ?, ?, '2026-08-08 10:00:00')
    `).run(key, Math.max(0, ...messageIds), messageIds.length ? Math.min(...messageIds) : null);
    if (job) {
      db.prepare('INSERT INTO jobs (channel_id, status, target_message_count) VALUES (?, ?, ?)').run(key, job, target);
    }
    if (repair) {
      db.prepare('INSERT INTO archive_repairs (channel_id, status) VALUES (?, ?)').run(key, repair);
    }
    insertMessages(service, key, messageIds, { peerId, text: 'from the username' });
  };

  const insertMessages = (service, key, ids, { peerId = BOT_ID, text = 'from the id' } = {}) => {
    const insert = service.db.prepare(`
      INSERT INTO messages (channel_id, message_id, date, text, raw_json) VALUES (?, ?, ?, ?, ?)
    `);
    for (const id of ids) {
      const raw = peerId === null
        ? { id, raw: { _: 'message', id } }
        : { id, raw: { _: 'message', id, peerId: { _: 'peerUser', userId: Number(typeof peerId === 'function' ? peerId(id) : peerId) } } };
      insert.run(key, id, 1700000000 + id, `${text} ${id}`, JSON.stringify(raw));
      service.db.prepare('INSERT INTO message_links (channel_id, message_id, url, domain) VALUES (?, ?, ?, ?)')
        .run(key, id, `https://example.com/${text.replace(/ /g, '-')}/${id}`, 'example.com');
      service.db.prepare('INSERT INTO message_media (channel_id, message_id, media_type) VALUES (?, ?, ?)')
        .run(key, id, text === 'from the id' ? 'photo' : 'document');
    }
  };

  const seedIdChat = (service, id, { username = 'shop_bot', messageIds = [], job = 'idle', repair = 'idle' } = {}) => {
    service.db.prepare(`
      INSERT INTO channels (channel_id, peer_type, username, sync_enabled, last_message_id)
      VALUES (?, 'user', ?, 1, ?)
    `).run(id, username, Math.max(0, ...messageIds));
    if (job) {
      service.db.prepare('INSERT INTO jobs (channel_id, status, target_message_count) VALUES (?, ?, 1000)').run(id, job);
    }
    if (repair) {
      service.db.prepare('INSERT INTO archive_repairs (channel_id, status) VALUES (?, ?)').run(id, repair);
    }
    insertMessages(service, id, messageIds);
  };

  const keysIn = (service, table) => service.db.prepare(`SELECT DISTINCT channel_id FROM ${table} ORDER BY channel_id`)
    .all().map((row) => row.channel_id);

  const allKeys = (service) => {
    const keys = new Set();
    for (const table of service._chatKeyedTables()) {
      for (const key of keysIn(service, table)) {
        keys.add(key);
      }
    }
    return [...keys].sort();
  };

  beforeEach(() => {
    storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-username-keys-'));
    services = [];
    peers = new Map([['shop_bot', peer(BOT_ID, 'shop_bot')]]);
    vi.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    for (const service of services) {
      service.close();
    }
    fs.rmSync(storeDir, { recursive: true, force: true });
    vi.restoreAllMocks();
  });

  describe('resolveChatKey', () => {
    it('returns an id as it is without asking Telegram', async () => {
      const service = makeService();
      await expect(service.resolveChatKey('-1001234567890')).resolves.toBe('-1001234567890');
      await expect(service.resolveChatKey(BOT_ID)).resolves.toBe(BOT_ID);
      await expect(service.resolveChatKey(42)).resolves.toBe('42');
      expect(getPeer).not.toHaveBeenCalled();
    });

    it('turns a username into the Telegram id of the chat that has it', async () => {
      const service = makeService();
      await expect(service.resolveChatKey('@shop_bot')).resolves.toBe(BOT_ID);
      await expect(service.resolveChatKey(' Shop_Bot ')).resolves.toBe(BOT_ID);
    });

    // mtcute answers a username from its cache first, which can name the previous owner.
    it('asks Telegram again when the cached answer no longer has the username', async () => {
      peers.set('moved', peer('111', 'renamed'));
      peers.set('fresh:moved', peer('222', 'moved'));
      peers.set('#222', peer('222', 'moved'));
      const service = makeService();
      await expect(service.resolveChatKey('@moved')).resolves.toBe('222');
      expect(resolvePeer).toHaveBeenCalledWith('@moved', true);
    });

    it('takes me for the account itself', async () => {
      peers.set('me', { id: Number(SELF_ID), type: 'user', username: null, usernames: null });
      const service = makeService();
      await expect(service.resolveChatKey('me')).resolves.toBe(SELF_ID);
    });

    it('says plainly that Telegram has no such chat', async () => {
      const service = makeService();
      await expect(service.resolveChatKey('@nobody')).rejects.toMatchObject({
        code: 'CHAT_NOT_FOUND',
        message: 'Telegram has no chat @nobody.',
      });
    });

    it('lets other failures through as they are', async () => {
      const service = makeService();
      getPeer.mockRejectedValueOnce(Object.assign(new Error('FLOOD_WAIT_400'), { text: 'FLOOD_WAIT_X' }));
      await expect(service.resolveChatKey('@shop_bot')).rejects.toThrow('FLOOD_WAIT_400');
    });
  });

  // Whatever forgets to resolve, the archive refuses to write under a username.
  describe('writes under a username', () => {
    it.each([
      ['addJob', (service) => service.addJob('@shop_bot')],
      ['setChannelSync', (service) => service.setChannelSync('@shop_bot', true)],
      ['setChannelTags', (service) => service.setChannelTags('@shop_bot', ['news'])],
      ['upsertTopics', (service) => service.upsertTopics('@shop_bot', [{ id: 1, title: 'General' }])],
      ['archiveHistoryMessages', (service) => service.archiveHistoryMessages('@shop_bot', [{ id: 1, date: 1700000001 }])],
    ])('%s throws and writes nothing', (name, write) => {
      const service = makeService();
      expect(() => write(service)).toThrow('Chat @shop_bot has to be resolved to its Telegram id before it is archived.');
      expect(allKeys(service)).toEqual([]);
    });
  });

  describe('ways in', () => {
    it('channels watch archives the chat under its id', async () => {
      const service = makeService();
      const result = await OPERATIONS.channelSetSync({ messageSyncService: service }, { chat: '@shop_bot', enable: true });
      expect(result).toMatchObject({ channelId: BOT_ID, syncEnabled: true, jobQueued: true });
      expect(allKeys(service)).toEqual([BOT_ID]);
    });

    it('tags set tags the chat under its id', async () => {
      const service = makeService();
      const result = await OPERATIONS.tagsSet({ messageSyncService: service }, { chat: '@shop_bot', tags: ['jobs'] });
      expect(result).toEqual({ channelId: BOT_ID, tags: ['jobs'] });
      expect(allKeys(service)).toEqual([BOT_ID]);
    });

    it('topics list caches the topics under the id', async () => {
      const service = makeService();
      const listForumTopics = vi.fn(async () => [{ id: 1, title: 'General' }]);
      await OPERATIONS.topicsList({ messageSyncService: service, telegramClient: { listForumTopics } }, { channelId: '@shop_bot' });
      expect(keysIn(service, 'topics')).toEqual([BOT_ID]);
    });

    it('a username Telegram does not know leaves no row behind', async () => {
      const service = makeService();
      await expect(OPERATIONS.channelSetSync({ messageSyncService: service }, { chat: '@nobody', enable: true }))
        .rejects.toMatchObject({ code: 'CHAT_NOT_FOUND' });
      await expect(OPERATIONS.tagsSet({ messageSyncService: service }, { chat: '@nobody', tags: ['x'] }))
        .rejects.toMatchObject({ code: 'CHAT_NOT_FOUND' });
      expect(allKeys(service)).toEqual([]);
    });

    // repair --all once walked these and wrote what it found under the username.
    it('backfill repair leaves chats under a username alone', () => {
      const service = makeService();
      seedUsernameChat(service, '@shop_bot', { repair: null });
      seedIdChat(service, BOT_ID, { repair: null });
      expect(service.addRepairs({ all: true })).toEqual([BOT_ID]);
      expect(service.addRepairs({ chats: ['@shop_bot'] })).toEqual([BOT_ID]);
      expect(keysIn(service, 'archive_repairs')).toEqual([BOT_ID]);
    });

    it('reads by username find the chat under its id, and what is left under a username Telegram lost', async () => {
      const service = makeService();
      const ctx = { messageSyncService: service, telegramClient: { getPeerMetadata: vi.fn(async () => ({})) } };
      await OPERATIONS.channelSetSync(ctx, { chat: '@shop_bot', enable: true });
      await OPERATIONS.tagsSet(ctx, { chat: '@shop_bot', tags: ['jobs'] });
      insertMessages(service, BOT_ID, [1, 2]);
      seedUsernameChat(service, '@lost', { messageIds: [9], job: null, repair: null });

      expect((await OPERATIONS.tagsList(ctx, { chat: '@shop_bot' })).map((tag) => tag.tag)).toEqual(['jobs']);
      expect(await OPERATIONS.channelShow(ctx, { chat: '@shop_bot' })).toMatchObject({ channelId: BOT_ID, syncEnabled: true });
      expect(await OPERATIONS.metadataGet(ctx, { chat: '@shop_bot' })).toMatchObject({ channelId: BOT_ID });
      const listed = await OPERATIONS.messagesList(ctx, { channelIds: ['@shop_bot'], source: 'archive' });
      expect(listed.messages.map((message) => [message.channelId, message.messageId])).toEqual([[BOT_ID, 2], [BOT_ID, 1]]);
      const searched = await OPERATIONS.messagesSearch(ctx, { channelIds: ['@shop_bot'], query: 'id', source: 'archive' });
      expect(searched.messages.map((message) => message.messageId).sort()).toEqual([1, 2]);
      expect((await OPERATIONS.messagesGet(ctx, { channelId: '@shop_bot', messageId: 2, source: 'archive' })).message)
        .toMatchObject({ channelId: BOT_ID, messageId: 2 });
      expect((await OPERATIONS.messagesList(ctx, { channelIds: ['@lost'], source: 'archive' })).messages.map((m) => m.messageId))
        .toEqual([9]);
      // `backfill jobs list` reads without Telegram, by the username the watch stored.
      expect(service.listJobs({ channelId: '@Shop_bot' }).map((job) => job.channel_id)).toEqual([BOT_ID]);
    });

    it('messages list --source both shows a message once, under the id', async () => {
      const service = makeService();
      insertMessages(service, BOT_ID, [1, 2]);
      const ctx = {
        messageSyncService: service,
        telegramClient: {
          getMessagesByChannelId: vi.fn(async () => ({ messages: [{ id: 2, date: 1700000002, text: 'live 2' }], peerTitle: 'Bot' })),
          getPeerMetadata: vi.fn(async () => ({})),
        },
      };
      const listed = await OPERATIONS.messagesList(ctx, { channelIds: ['@shop_bot'], source: 'both' });
      expect(listed.messages.map((message) => [message.channelId, message.messageId, message.source]))
        .toEqual([[BOT_ID, 2, 'live'], [BOT_ID, 1, 'archive']]);
    });

    it('a backfill by username stores what Telegram says of the chat', async () => {
      const service = makeService();
      service.addJob(await service.resolveChatKey('@shop_bot'));
      expect(service.db.prepare('SELECT peer_type, username, peer_title FROM channels WHERE channel_id = ?').get(BOT_ID))
        .toEqual({ peer_type: 'user', username: 'shop_bot', peer_title: `Peer ${BOT_ID}` });
    });

    it('metadata refresh and auto-tagging leave chats under a username alone', async () => {
      const service = makeService();
      service.telegramClient.getPeerMetadata = vi.fn(async () => ({ peerTitle: 'T', about: 'sport news' }));
      seedUsernameChat(service, '@shop_bot', { job: null, repair: null });
      await service.refreshChannelMetadata({ force: true });
      await service.refreshChannelMetadata({ channelIds: ['@shop_bot'], force: true });
      await service.autoTagChannels({});
      await service.autoTagChannels({ channelIds: ['@shop_bot'] });
      expect(service.telegramClient.getPeerMetadata).not.toHaveBeenCalled();
      expect(keysIn(service, 'channel_metadata')).toEqual([]);
      expect(keysIn(service, 'channel_tags')).toEqual([]);
    });

    it('backfill repair says why a chat left under its username cannot be repaired', () => {
      const service = makeService();
      seedUsernameChat(service, '@lost', { repair: null });
      expect(() => service.addRepairs({ chats: ['@lost'] })).toThrow('still stored under its username');
    });

    it('the queue never runs or retries work under a username', () => {
      const service = makeService();
      seedUsernameChat(service, '@shop_bot', { job: 'pending', repair: 'pending' });
      service.db.prepare("INSERT INTO jobs (channel_id, status) VALUES ('@lost', 'error')").run();
      service.db.prepare("INSERT INTO channels (channel_id, sync_enabled) VALUES ('@lost', 1)").run();
      expect(service._getNextJob()).toBeUndefined();
      expect(service._getNextRepair()).toBeUndefined();
      expect(service.retryJobs({ allErrors: true })).toEqual({ updated: 0, jobIds: [] });
      expect(service.retryJobs({ channelId: '@lost' })).toEqual({ updated: 0, jobIds: [] });
      service._resetErroredJobs();
      expect(service.db.prepare("SELECT status FROM jobs WHERE channel_id = '@lost'").get().status).toBe('error');
    });
  });

  describe('migrateUsernameKeys', () => {
    it('merges a chat into the one under its id, keeping what was there and dropping duplicates', async () => {
      const service = makeService();
      seedIdChat(service, BOT_ID, { messageIds: [1, 2, 3] });
      seedUsernameChat(service, '@shop_bot', { messageIds: [2, 3, 4, 5] });

      const report = await service.migrateUsernameKeys();

      expect(report).toEqual({ moved: [{ key: '@shop_bot', channelId: BOT_ID, messages: 2, duplicates: 2 }], left: [], dropped: [] });
      expect(allKeys(service)).toEqual([BOT_ID]);
      const rows = service.db.prepare('SELECT message_id, text FROM messages WHERE channel_id = ? ORDER BY message_id').all(BOT_ID);
      expect(rows).toEqual([
        { message_id: 1, text: 'from the id 1' },
        { message_id: 2, text: 'from the id 2' },
        { message_id: 3, text: 'from the id 3' },
        { message_id: 4, text: 'from the username 4' },
        { message_id: 5, text: 'from the username 5' },
      ]);
      // A duplicate keeps the links and media of the copy under the id.
      const media = service.db.prepare('SELECT message_id, media_type FROM message_media WHERE channel_id = ? ORDER BY message_id').all(BOT_ID);
      expect(media.map((row) => row.media_type)).toEqual(['photo', 'photo', 'photo', 'document', 'document']);
      const links = service.db.prepare('SELECT message_id, url FROM message_links WHERE channel_id = ? ORDER BY message_id').all(BOT_ID);
      expect(links.map((row) => row.message_id)).toEqual([1, 2, 3, 4, 5]);
      expect(links[1].url).toContain('from-the-id');
      // Search finds the moved messages under the id, and nothing under the username.
      const found = service.archive.search({ query: 'username' });
      expect(found.map((message) => [message.channelId, message.messageId])).toEqual(expect.arrayContaining([[BOT_ID, 4], [BOT_ID, 5]]));
      expect(found.every((message) => message.channelId === BOT_ID)).toBe(true);
      // The chat under the id keeps its own row.
      expect(service.db.prepare('SELECT peer_type, last_message_id FROM channels WHERE channel_id = ?').get(BOT_ID))
        .toEqual({ peer_type: 'user', last_message_id: 3 });
      expect(console.log).toHaveBeenCalledWith(`[archive] Chat @shop_bot is now stored as ${BOT_ID}: 2 message(s) moved, 2 duplicate(s) dropped.`);
    });

    it('moves a chat with nothing under its id over whole, typed by Telegram', async () => {
      const service = makeService();
      seedUsernameChat(service, '@shop_bot', { messageIds: [7, 8], job: 'error', repair: 'pending' });
      service.db.prepare("INSERT INTO channel_tags (channel_id, tag) VALUES ('@shop_bot', 'jobs')").run();
      service.db.prepare("INSERT INTO topics (channel_id, topic_id, title) VALUES ('@shop_bot', 1, 'General')").run();

      const report = await service.migrateUsernameKeys();

      expect(report.moved).toEqual([{ key: '@shop_bot', channelId: BOT_ID, messages: 2, duplicates: 0 }]);
      expect(allKeys(service)).toEqual([BOT_ID]);
      expect(service.db.prepare(`
        SELECT peer_type, username, peer_title, sync_enabled, last_message_id, oldest_message_id, created_at
        FROM channels WHERE channel_id = ?
      `).get(BOT_ID)).toEqual({
        peer_type: 'user',
        username: 'shop_bot',
        peer_title: `Peer ${BOT_ID}`,
        sync_enabled: 1,
        last_message_id: 8,
        oldest_message_id: 7,
        created_at: '2026-08-08 10:00:00',
      });
      expect(service.db.prepare('SELECT status FROM jobs WHERE channel_id = ?').get(BOT_ID).status).toBe('error');
      expect(service.db.prepare('SELECT status FROM archive_repairs WHERE channel_id = ?').get(BOT_ID).status).toBe('pending');
      expect(service.listChannelTags(BOT_ID).map((tag) => tag.tag)).toEqual(['jobs']);
    });

    it('hands the work still due under the username to the job and repair under the id', async () => {
      const service = makeService();
      seedIdChat(service, BOT_ID, { job: 'idle', repair: 'idle' });
      seedUsernameChat(service, '@shop_bot', { job: 'pending', repair: 'in_progress', target: 5000 });

      await service.migrateUsernameKeys();

      expect(service.db.prepare('SELECT status, target_message_count FROM jobs').all())
        .toEqual([{ status: 'pending', target_message_count: 5000 }]);
      expect(service.db.prepare('SELECT channel_id, status FROM archive_repairs').all())
        .toEqual([{ channel_id: BOT_ID, status: 'in_progress' }]);
      expect(service.db.prepare('SELECT newer_sync_pending FROM channels WHERE channel_id = ?').get(BOT_ID).newer_sync_pending).toBe(1);
    });

    it('drops finished work under the username when the id has its own', async () => {
      const service = makeService();
      seedIdChat(service, BOT_ID, { job: 'idle', repair: 'idle' });
      service.db.prepare("UPDATE jobs SET target_message_count = 300 WHERE channel_id = ?").run(BOT_ID);
      seedUsernameChat(service, '@shop_bot', { job: 'idle', repair: 'idle', target: 5000 });

      await service.migrateUsernameKeys();

      expect(service.db.prepare('SELECT channel_id, status, target_message_count FROM jobs').all())
        .toEqual([{ channel_id: BOT_ID, status: 'idle', target_message_count: 300 }]);
      expect(keysIn(service, 'archive_repairs')).toEqual([BOT_ID]);
    });

    // Without a peer in its messages nothing says which chat they came from.
    it('leaves a chat Telegram does not find and its messages do not name, parks its work, and asks again a day later', async () => {
      const service = makeService();
      seedUsernameChat(service, '@lost', { messageIds: [1], peerId: null, job: 'pending', repair: 'pending' });
      const now = Date.parse('2026-11-06T12:00:00Z');
      const detail = 'Telegram did not find it, and its messages do not all name one chat';

      const report = await service.migrateUsernameKeys({ now });

      expect(report).toEqual({ moved: [], left: [{ key: '@lost', outcome: 'not_found', detail }], dropped: [] });
      expect(allKeys(service)).toEqual(['@lost']);
      expect(service.db.prepare("SELECT status, error FROM jobs WHERE channel_id = '@lost'").get()).toEqual({
        status: 'error',
        error: `Stored under the username @lost: ${detail}.`,
      });
      expect(service.getQueueStats()).toMatchObject({ pending: 0, in_progress: 0 });
      expect(service.listUsernameKeyedChats()).toEqual([{
        key: '@lost',
        messages: 1,
        archivedAs: null,
        lastCheck: { outcome: 'not_found', detail, checkedAt: '2026-11-06T12:00:00.000Z' },
      }]);

      getPeer.mockClear();
      await service.migrateUsernameKeys({ now: now + DAY_MS - 1 });
      expect(getPeer).not.toHaveBeenCalled();

      peers.set('lost', peer(BOT_ID, 'lost'));
      const later = await service.migrateUsernameKeys({ now: now + DAY_MS });
      expect(later.moved).toEqual([{ key: '@lost', channelId: BOT_ID, messages: 1, duplicates: 0 }]);
      expect(service.listUsernameKeyedChats()).toEqual([]);
      expect(service.db.prepare('SELECT COUNT(*) AS n FROM username_key_checks').get().n).toBe(0);
    });

    it('tries again at the next start after Telegram failed to answer', async () => {
      const service = makeService();
      seedUsernameChat(service, '@shop_bot', { messageIds: [1] });
      getPeer.mockRejectedValueOnce(new Error('Connection closed'));

      const report = await service.migrateUsernameKeys();
      expect(report.left).toEqual([{ key: '@shop_bot', outcome: 'failed', detail: 'Telegram did not answer: Connection closed' }]);
      expect(allKeys(service)).toEqual(['@shop_bot']);

      const next = await service.migrateUsernameKeys();
      expect(next.moved).toHaveLength(1);
      expect(allKeys(service)).toEqual([BOT_ID]);
    });

    // A username that changed hands would carry its messages into someone else's chat.
    it('leaves a chat whose messages name another chat than the one Telegram gives', async () => {
      const service = makeService();
      seedUsernameChat(service, '@shop_bot', { messageIds: [1, 2], peerId: '555' });

      const report = await service.migrateUsernameKeys();

      expect(report.left).toEqual([{
        key: '@shop_bot',
        outcome: 'conflict',
        detail: `Telegram gives ${BOT_ID} for it, but its messages are from 555`,
      }]);
      expect(allKeys(service)).toEqual(['@shop_bot']);
    });

    it('does nothing, and asks Telegram nothing, once every chat is under its id', async () => {
      const service = makeService();
      seedIdChat(service, BOT_ID, { messageIds: [1] });
      seedUsernameChat(service, '@shop_bot', { messageIds: [1, 2] });
      await service.migrateUsernameKeys();
      const before = service.db.prepare('SELECT * FROM messages ORDER BY id').all();
      getPeer.mockClear();

      expect(await service.migrateUsernameKeys()).toEqual({ moved: [], left: [], dropped: [] });
      expect(getPeer).not.toHaveBeenCalled();
      expect(service.db.prepare('SELECT * FROM messages ORDER BY id').all()).toEqual(before);
    });

    // One transaction per chat: a stop or a crash halfway leaves the chat whole.
    it('leaves a chat whole under its username when the merge stops halfway, and finishes it next time', async () => {
      const service = makeService();
      seedIdChat(service, BOT_ID, { messageIds: [1] });
      seedUsernameChat(service, '@shop_bot', { messageIds: [1, 2, 3], job: 'pending' });
      const mergeRepair = service._mergeChatRepair;
      service._mergeChatRepair = () => {
        throw new Error('killed');
      };
      vi.spyOn(console, 'error').mockImplementation(() => {});

      const failed = await service.migrateUsernameKeys();
      expect(failed.left).toEqual([{ key: '@shop_bot', outcome: 'failed', detail: `moving it to ${BOT_ID} failed: killed` }]);
      expect(keysIn(service, 'messages')).toEqual([BOT_ID, '@shop_bot']);
      expect(service.db.prepare("SELECT COUNT(*) AS n FROM messages WHERE channel_id = '@shop_bot'").get().n).toBe(3);
      expect(service.db.prepare('SELECT COUNT(*) AS n FROM messages WHERE channel_id = ?').get(BOT_ID).n).toBe(1);
      expect(keysIn(service, 'jobs')).toEqual([BOT_ID, '@shop_bot']);

      service._mergeChatRepair = mergeRepair;
      const report = await service.migrateUsernameKeys();
      expect(report.moved).toEqual([{ key: '@shop_bot', channelId: BOT_ID, messages: 2, duplicates: 1 }]);
      expect(allKeys(service)).toEqual([BOT_ID]);
    });

    it('goes on with the other chats when one cannot be moved', async () => {
      const service = makeService();
      peers.set('other', peer('333', 'other'));
      seedUsernameChat(service, '@shop_bot', { messageIds: [1], job: 'pending' });
      seedUsernameChat(service, '@other', { messageIds: [5], peerId: '333' });
      const merge = service.mergeChatKey.bind(service);
      vi.spyOn(service, 'mergeChatKey').mockImplementation((key, ...rest) => {
        if (key === '@shop_bot') {
          throw new Error('database is locked');
        }
        return merge(key, ...rest);
      });
      vi.spyOn(console, 'error').mockImplementation(() => {});

      const report = await service.migrateUsernameKeys();

      expect(report.moved.map((entry) => entry.key)).toEqual(['@other']);
      expect(report.left).toEqual([{ key: '@shop_bot', outcome: 'failed', detail: `moving it to ${BOT_ID} failed: database is locked` }]);
      // Its job waits as an error instead of holding up idle exit and version takeover.
      expect(service.getQueueStats()).toMatchObject({ pending: 0, in_progress: 0, error: 1 });
    });

    it('takes a chat mtcute found but cannot reach yet for a failure, not for a missing chat', async () => {
      const service = makeService();
      seedUsernameChat(service, '@shop_bot', { messageIds: [1] });
      getPeer.mockRejectedValueOnce(new MtPeerNotFoundError('Peer (user) with username shop_bot was found, but it has no access hash'));
      const report = await service.migrateUsernameKeys();
      expect(report.left[0].outcome).toBe('failed');
      expect((await service.migrateUsernameKeys()).moved).toHaveLength(1);
    });

    it('keeps sync off when it was turned off under the username', async () => {
      const service = makeService();
      seedIdChat(service, BOT_ID);
      seedUsernameChat(service, '@shop_bot');
      service.db.prepare("UPDATE channels SET sync_enabled = 0 WHERE channel_id = '@shop_bot'").run();
      await service.migrateUsernameKeys();
      expect(service.db.prepare('SELECT sync_enabled, sync_by_user FROM channels WHERE channel_id = ?').get(BOT_ID))
        .toEqual({ sync_enabled: 0, sync_by_user: 1 });
    });

    // A backfill asked for under the username is the user's word, as addJob takes it.
    it('turns sync back on for a chat the server turned off as left when the username had work due', async () => {
      const service = makeService();
      const CHANNEL = '-1000000000500';
      peers.set('news', peer(CHANNEL, 'news', 'channel'));
      service.db.prepare(`
        INSERT INTO channels (channel_id, username, sync_enabled, sync_off_reason, in_dialogs) VALUES (?, 'news', 0, 'left', 0)
      `).run(CHANNEL);
      service.db.prepare('INSERT INTO jobs (channel_id, status) VALUES (?, ?)').run(CHANNEL, 'idle');
      seedUsernameChat(service, '@news', { job: 'pending', repair: null });
      await service.migrateUsernameKeys();
      expect(service.db.prepare('SELECT sync_enabled, sync_by_user, sync_off_reason FROM channels WHERE channel_id = ?').get(CHANNEL))
        .toEqual({ sync_enabled: 1, sync_by_user: 1, sync_off_reason: null });
    });

    // An update during login can start the queue before the migration does.
    it('waits for a queue step already under way before merging into its job', async () => {
      const service = makeService();
      delete service.processQueue;
      service.interJobDelayMs = 1;
      seedIdChat(service, BOT_ID, { job: 'pending' });
      seedUsernameChat(service, '@shop_bot', { messageIds: [1], job: 'pending', target: 5000 });
      let finishStep;
      vi.spyOn(service, '_processJob').mockImplementationOnce(async (job) => {
        service._updateJobStatus(job.id, 'in_progress');
        await new Promise((resolve) => {
          finishStep = resolve;
        });
        service._updateJobStatus(job.id, 'idle');
      }).mockImplementation(async (job) => {
        service._updateJobStatus(job.id, 'idle');
      });
      vi.spyOn(service, '_scheduleNextGapRetry').mockImplementation(() => {});
      const queue = service.processQueue();
      await vi.waitFor(() => expect(finishStep).toBeTypeOf('function'));

      const migration = service.migrateUsernameKeys();
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(keysIn(service, 'messages')).toEqual(['@shop_bot']);

      finishStep();
      await migration;
      expect(keysIn(service, 'messages')).toEqual([BOT_ID]);
      // The merged request is still due, and the queue runs it after the migration.
      expect(service.db.prepare('SELECT status, target_message_count FROM jobs').get())
        .toEqual({ status: 'pending', target_message_count: 5000 });
      await vi.waitFor(() => expect(service._processJob).toHaveBeenCalledTimes(2));
      service.stopRequested = true;
      await queue;
    });

    it('does not wait for the queue when it has nothing to merge', async () => {
      const service = makeService();
      seedUsernameChat(service, '@lost', { messageIds: [1], peerId: null, job: null, repair: null });
      seedUsernameChat(service, '@empty', { job: null, repair: null });
      const now = Date.now();
      service.db.prepare("INSERT INTO username_key_checks VALUES ('@lost', 'not_found', 'Telegram did not find it', ?)").run(now);
      service.queueStep = new Promise(() => {});
      await expect(Promise.race([service.migrateUsernameKeys({ now }), new Promise((resolve) => setTimeout(() => resolve('waited'), 500))]))
        .resolves.toMatchObject({ moved: [], left: [{ key: '@lost', outcome: 'not_found' }], dropped: [{ key: '@empty' }] });
    });

    it('lets the server stop while Telegram has not answered', async () => {
      const service = makeService();
      delete service.processQueue;
      seedUsernameChat(service, '@shop_bot', { messageIds: [1] });
      getPeer.mockImplementationOnce(() => new Promise(() => {}));
      vi.spyOn(service, '_scheduleNextGapRetry').mockImplementation(() => {});
      void service.migrateUsernameKeys();
      const queue = service.processQueue();
      const stopped = service.shutdown();
      await expect(Promise.race([stopped.then(() => 'stopped'), new Promise((resolve) => setTimeout(() => resolve('stuck'), 3000))]))
        .resolves.toBe('stopped');
      await queue;
      services.splice(services.indexOf(service), 1);
    });

    it('survives a restart: a new server over the same archive finds nothing left to move', async () => {
      const first = makeService();
      seedUsernameChat(first, '@shop_bot', { messageIds: [1] });
      seedUsernameChat(first, '@lost', { messageIds: [2], peerId: null });
      await first.migrateUsernameKeys();
      first.close();

      const second = makeService();
      expect(second.listUsernameKeyedChats().map((chat) => chat.key)).toEqual(['@lost']);
      expect(keysIn(second, 'messages')).toEqual([BOT_ID, '@lost']);
      expect((await second.migrateUsernameKeys()).moved).toEqual([]);
    });

    describe('a username Telegram does not find', () => {
      it('moves to the one chat all its messages came from', async () => {
        const service = makeService();
        seedIdChat(service, BOT_ID, { username: 'someone_else', messageIds: [1] });
        seedUsernameChat(service, '@gone', { messageIds: [1, 2, 3], job: 'error' });

        const report = await service.migrateUsernameKeys();

        expect(report).toEqual({ moved: [{ key: '@gone', channelId: BOT_ID, messages: 2, duplicates: 1 }], left: [], dropped: [] });
        expect(allKeys(service)).toEqual([BOT_ID]);
        expect(console.log).toHaveBeenCalledWith(`[archive] Chat @gone is now stored as ${BOT_ID}, found by its messages as Telegram did not find the username: 2 message(s) moved, 1 duplicate(s) dropped.`);
      });

      it('takes the type of a new chat from its messages', async () => {
        const service = makeService();
        seedUsernameChat(service, '@gone', { messageIds: [4] });
        await service.migrateUsernameKeys();
        expect(service.db.prepare('SELECT peer_type FROM channels WHERE channel_id = ?').get(BOT_ID).peer_type).toBe('user');
      });

      it('stays when its messages came from different chats', async () => {
        const service = makeService();
        seedUsernameChat(service, '@gone', { messageIds: [1, 2], peerId: (id) => (id === 1 ? BOT_ID : '555') });
        const report = await service.migrateUsernameKeys();
        expect(report.left).toEqual([{ key: '@gone', outcome: 'not_found', detail: 'Telegram did not find it, and its messages do not all name one chat' }]);
        expect(allKeys(service)).toEqual(['@gone']);
      });

      it('stays when only some of its messages say where they came from', async () => {
        const service = makeService();
        seedUsernameChat(service, '@gone', { messageIds: [1] });
        insertMessages(service, '@gone', [2], { peerId: null });
        expect((await service.migrateUsernameKeys()).left).toHaveLength(1);
        expect(allKeys(service)).toEqual(['@gone']);
      });

      it('is dropped with its jobs and repairs when no message is under it', async () => {
        const service = makeService();
        seedIdChat(service, BOT_ID, { username: 'gone', messageIds: [1] });
        seedUsernameChat(service, '@gone', { job: 'error', repair: 'idle' });
        service.db.prepare("INSERT INTO channel_tags (channel_id, tag) VALUES ('@gone', 'x')").run();

        const report = await service.migrateUsernameKeys();

        expect(report).toEqual({ moved: [], left: [], dropped: [{ key: '@gone', jobs: 1, repairs: 1, rows: 4 }] });
        // A chat under an id with the same username is no evidence: usernames change hands.
        expect(allKeys(service)).toEqual([BOT_ID]);
        expect(service.db.prepare('SELECT COUNT(*) AS n FROM jobs WHERE channel_id = ?').get(BOT_ID).n).toBe(1);
        expect(service.listUsernameKeyedChats()).toEqual([]);
        expect(console.log).toHaveBeenCalledWith('[archive] Chat @gone: Telegram did not find it and no message is archived under it, so its rows are dropped: 1 job(s), 1 repair(s), 4 row(s) in all.');
      });

      // A username Telegram did not find earlier the same day is not asked about again.
      it('is settled from the archive alone when Telegram said so within the day', async () => {
        const service = makeService();
        seedUsernameChat(service, '@gone', { job: 'error' });
        const now = Date.now();
        service.db.prepare("INSERT INTO username_key_checks VALUES ('@gone', 'not_found', 'Telegram did not find it', ?)").run(now - 1000);
        const report = await service.migrateUsernameKeys({ now });
        expect(getPeer).not.toHaveBeenCalled();
        expect(report.dropped.map((entry) => entry.key)).toEqual(['@gone']);
        expect(allKeys(service)).toEqual([]);
      });
    });

    it('finds a username left in any table, even without a chat row', async () => {
      const service = makeService();
      service.db.prepare("INSERT INTO topics (channel_id, topic_id, title) VALUES ('@shop_bot', 3, 'Jobs')").run();
      expect(service.listUsernameKeyedChats().map((chat) => chat.key)).toEqual(['@shop_bot']);
      await service.migrateUsernameKeys();
      expect(keysIn(service, 'topics')).toEqual([BOT_ID]);
    });

    it('tells which chat under an id has the same username', () => {
      const service = makeService();
      seedIdChat(service, BOT_ID, { username: 'Shop_Bot' });
      seedUsernameChat(service, '@shop_bot', { messageIds: [1, 2] });
      expect(service.listUsernameKeyedChats()).toEqual([{ key: '@shop_bot', messages: 2, archivedAs: BOT_ID, lastCheck: null }]);
    });

    it('holds the queue until it is done', async () => {
      const service = makeService();
      delete service.processQueue;
      seedUsernameChat(service, '@shop_bot', { messageIds: [1] });
      let release;
      getPeer.mockImplementationOnce(() => new Promise((resolve) => {
        release = () => resolve(peers.get('shop_bot'));
      }));
      const nextJob = vi.spyOn(service, '_getNextJob').mockReturnValue(undefined);
      vi.spyOn(service, '_scheduleNextGapRetry').mockImplementation(() => {});
      const migration = service.migrateUsernameKeys();
      const queue = service.processQueue();
      await new Promise((resolve) => setTimeout(resolve, 10));
      expect(nextJob).not.toHaveBeenCalled();
      release();
      await migration;
      await queue;
      expect(nextJob).toHaveBeenCalled();
    });
  });

  it('the server moves them after login, before the queue resumes', async () => {
    const order = [];
    const connect = createTelegramConnector({
      telegramClient: { initializeDialogCache: vi.fn(async () => true) },
      messageSyncService: {
        startRealtimeSync: vi.fn(),
        migrateUsernameKeys: vi.fn(async () => order.push('migrate')),
        reconcileWithDialogs: vi.fn(async () => {
          order.push('reconcile');
          return 3;
        }),
        resumePendingJobs: vi.fn(() => order.push('resume')),
      },
    });
    await connect();
    await vi.waitFor(() => expect(order).toEqual(['migrate', 'reconcile', 'resume']));
  });
});
