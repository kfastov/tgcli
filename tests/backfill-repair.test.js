// `backfill repair` on the server side: a repair walks a chat's history one page per
// queue step and writes back what the archive lacks, leaving the sync cursors alone.
// History comes from real mtcute iterHistory over a stubbed messages.getHistory.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { Long, MtPeerNotFoundError, tl } from '@mtcute/core';
import { iterHistory } from '@mtcute/core/methods.js';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import MessageSyncService from '../message-sync-service.js';
import TelegramClient from '../telegram-client.js';

const CHANNEL_ID = '-1000000000500';
const OTHER_ID = '-1000000000600';
const OFF_ID = '-1000000000700';
const ACCESS_HASH = Long.fromNumber(1);
const INPUT_PEER = { _: 'inputPeerChannel', channelId: 500, accessHash: ACCESS_HASH };
const chats = [{ _: 'channel', id: 500, title: 'Group', accessHash: ACCESS_HASH, megagroup: true }];
const users = [{ _: 'user', id: 2, firstName: 'Friend', username: 'friend', accessHash: ACCESS_HASH }];
const BASE_DATE = 1700000000;

function rawMessage(id) {
  return {
    _: 'message',
    id,
    date: BASE_DATE + id,
    peerId: { _: 'peerChannel', channelId: 500 },
    fromId: { _: 'peerUser', userId: 2 },
    message: `message ${id}`,
  };
}

// messages.getHistory as Telegram serves it for a plain walk: newest first, below offset_id.
function historyOf(ids) {
  return ({ offsetId, limit }) => {
    const page = ids.filter((id) => !offsetId || id < offsetId).sort((a, b) => b - a).slice(0, limit);
    return { _: 'messages.messages', messages: page.map(rawMessage), users, chats, count: ids.length };
  };
}

const range = (from, to) => Array.from({ length: to - from + 1 }, (_, index) => from + index);

describe('backfill repair', () => {
  let storeDir;
  let services;
  let historyRequests;
  let answer;

  const makeService = () => {
    const telegramClient = Object.create(TelegramClient.prototype);
    telegramClient.ensureLogin = vi.fn().mockResolvedValue(undefined);
    telegramClient.client = {
      resolvePeer: async () => INPUT_PEER,
      iterHistory: (peer, options) => iterHistory({
        call: async (request) => {
          historyRequests.push(request);
          return answer(request);
        },
      }, peer, options),
    };
    const service = new MessageSyncService(telegramClient, {
      dbPath: path.join(storeDir, 'messages.db'),
      batchSize: 100,
      interJobDelayMs: 1,
      repairPageDelayMs: 0,
    });
    services.push(service);
    return service;
  };

  // enteredAt is when the chat entered the archive; by default before all its history.
  const setUp = (historyIds, archivedIds, {
    syncEnabled = 1,
    username = 'group',
    oldestCursor = archivedIds.length ? Math.min(...archivedIds) : null,
    enteredAt = BASE_DATE,
  } = {}) => {
    answer = historyOf(historyIds);
    const service = makeService();
    service.db.prepare(`
      INSERT INTO channels (channel_id, peer_title, username, sync_enabled, last_message_id, oldest_message_id, created_at)
      VALUES (?, 'Group', ?, ?, ?, ?, datetime(?, 'unixepoch'))
    `).run(CHANNEL_ID, username, syncEnabled, Math.max(...historyIds), oldestCursor, enteredAt);
    service.archive.write(CHANNEL_ID, archivedIds.map((id) => ({
      id,
      date: BASE_DATE + id,
      text: `archived ${id}`,
    })));
    return service;
  };

  const archivedIds = (service) => service.db.prepare(`
    SELECT message_id FROM messages WHERE channel_id = ? ORDER BY message_id
  `).all(CHANNEL_ID).map((row) => row.message_id);

  const repairRow = (service, channelId = CHANNEL_ID) => service.db.prepare(`
    SELECT * FROM archive_repairs WHERE channel_id = ?
  `).get(channelId);

  beforeEach(() => {
    storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-repair-test-'));
    services = [];
    historyRequests = [];
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    for (const service of services) {
      if (service.db?.open) {
        service.close();
      }
    }
    fs.rmSync(storeDir, { recursive: true, force: true });
    vi.restoreAllMocks();
  });

  it('fills holes at any distance across pages, and keeps existing rows and the cursors', async () => {
    const historyIds = range(10, 259);
    const service = setUp(historyIds, historyIds.filter((id) => id !== 11 && id !== 137));

    service.addRepairs({ chats: [CHANNEL_ID] });
    await service.processQueue();

    expect(archivedIds(service)).toEqual(historyIds);
    const kept = service.db.prepare('SELECT text FROM messages WHERE channel_id = ? AND message_id = 12').get(CHANNEL_ID);
    expect(kept.text).toBe('archived 12');
    expect(service.db.prepare('SELECT last_message_id, oldest_message_id FROM channels WHERE channel_id = ?').get(CHANNEL_ID))
      .toEqual({ last_message_id: 259, oldest_message_id: 10 });
    expect(repairRow(service)).toMatchObject({ status: 'idle', checked_count: 250, added_count: 2, error: null });
  });

  // Telegram's handling of min_id combined with a negative add_offset is not something
  // to depend on: a server that windows first would answer empty and read as done.
  it('uses only the plain newest-first walk', async () => {
    const service = setUp(range(10, 159), [10]);

    service.addRepairs({ chats: [CHANNEL_ID] });
    await service.processQueue();

    expect(historyRequests.length).toBeGreaterThan(1);
    for (const request of historyRequests) {
      expect(request).toMatchObject({ addOffset: 0, minId: 0, maxId: 0 });
    }
  });

  // Holes come from following the chat, so they lie after it entered the archive. History
  // from before came from a backfill, which pages through it without gaps.
  it('walks below its oldest row, back to when the chat entered the archive', async () => {
    const service = setUp(range(100, 200), range(150, 200), { enteredAt: BASE_DATE + 120 });

    service.addRepairs({ chats: [CHANNEL_ID] });
    await service.processQueue();

    expect(archivedIds(service)).toEqual(range(120, 200));
    expect(historyRequests).toHaveLength(1);
  });

  // An edit or a reply can bring a message from long before into the archive; that row
  // says nothing about what the archive followed.
  it('takes no floor from a stray old row', async () => {
    const service = setUp(range(100, 200), [105, ...range(150, 200).filter((id) => id !== 170)], {
      enteredAt: BASE_DATE + 150,
    });

    service.addRepairs({ chats: [CHANNEL_ID] });
    await service.processQueue();

    expect(archivedIds(service)).toEqual([105, ...range(150, 200)]);
    expect(repairRow(service)).toMatchObject({ status: 'idle', added_count: 1 });
  });

  it('fills a chat with no rows from when it entered the archive', async () => {
    const service = setUp(range(100, 200), [], { enteredAt: BASE_DATE + 190 });

    service.addRepairs({ chats: [CHANNEL_ID], afterId: 0 });
    await service.processQueue();

    expect(archivedIds(service)).toEqual(range(190, 200));
    expect(repairRow(service)).toMatchObject({ status: 'idle', checked_count: 11, added_count: 11 });
  });

  it('keeps to an asked bound older than the chat in the archive', async () => {
    const service = setUp(range(100, 200), [200], { enteredAt: BASE_DATE + 150 });

    service.addRepairs({ chats: [CHANNEL_ID], sinceDate: BASE_DATE + 180 });
    await service.processQueue();

    expect(archivedIds(service)).toEqual(range(180, 200));
  });

  it('counts in a dry run what is missing, and writes nothing', async () => {
    const historyIds = range(10, 259);
    const archived = historyIds.filter((id) => id !== 11 && id !== 137);
    const service = setUp(historyIds, archived);

    service.addRepairs({ chats: [CHANNEL_ID], dryRun: true });
    await service.processQueue();

    expect(archivedIds(service)).toEqual(archived);
    expect(repairRow(service)).toMatchObject({ status: 'idle', dry_run: 1, checked_count: 250, added_count: 2 });

    service.addRepairs({ chats: [CHANNEL_ID] });
    await service.processQueue();
    expect(archivedIds(service)).toEqual(historyIds);
    expect(repairRow(service)).toMatchObject({ dry_run: 0, added_count: 2 });
  });

  // A dry run takes the chat's row: over a repair under way it would drop the rest of it.
  it('refuses a dry run over a repair still under way, and takes one after it', async () => {
    const service = setUp(range(10, 259), [10]);
    service.addRepairs({ chats: [CHANNEL_ID] });
    await service._repairNextPage(service._getNextRepair());

    expect(() => service.addRepairs({ chats: [CHANNEL_ID], dryRun: true })).toThrow(/repair is under way in 1 of these chat/);
    expect(() => service.addRepairs({ all: true, dryRun: true })).toThrow(/under way/);
    expect(repairRow(service)).toMatchObject({ status: 'in_progress', dry_run: 0, cursor_message_id: 160 });

    await service.processQueue();
    expect(service.addRepairs({ chats: [CHANNEL_ID], dryRun: true })).toEqual([CHANNEL_ID]);
  });

  it('counts in a dry run only this chat\'s rows as present', async () => {
    const service = setUp(range(10, 20), [10]);
    service.db.prepare('INSERT INTO channels (channel_id, sync_enabled) VALUES (?, 1)').run(OTHER_ID);
    service.archive.write(OTHER_ID, range(11, 20).map((id) => ({ id, date: BASE_DATE + id })));

    service.addRepairs({ chats: [CHANNEL_ID], dryRun: true });
    await service.processQueue();

    expect(repairRow(service)).toMatchObject({ dry_run: 1, added_count: 10 });
  });

  it('adds what it needs to an archive made before dry runs and dialog membership', async () => {
    const first = setUp([10, 11], [10]);
    const dbPath = first.db.name;
    first.db.exec('ALTER TABLE archive_repairs DROP COLUMN dry_run');
    first.db.exec('ALTER TABLE channels DROP COLUMN in_dialogs');
    first.close();

    const service = makeService();
    const columns = (table) => service.db.prepare(`PRAGMA table_info(${table})`).all().map((col) => col.name);
    expect(service.db.name).toBe(dbPath);
    expect(columns('archive_repairs')).toContain('dry_run');
    expect(columns('channels')).toContain('in_dialogs');

    service.addRepairs({ chats: [CHANNEL_ID], dryRun: true });
    await service.processQueue();
    expect(repairRow(service)).toMatchObject({ status: 'idle', dry_run: 1, added_count: 1 });
  });

  it('stops at the date bound', async () => {
    const service = setUp(range(100, 200), [100, 101, 200]);

    service.addRepairs({ chats: [CHANNEL_ID], sinceDate: BASE_DATE + 150 });
    await service.processQueue();

    expect(archivedIds(service)).toEqual([100, 101, ...range(150, 200)]);
    expect(repairRow(service)).toMatchObject({ status: 'idle', added_count: 50 });
    expect(historyRequests).toHaveLength(1);
  });

  it('stops above the message id bound', async () => {
    const service = setUp(range(100, 200), [100, 200]);

    service.addRepairs({ chats: [CHANNEL_ID], afterId: 180 });
    await service.processQueue();

    expect(archivedIds(service)).toEqual([100, ...range(181, 200)]);
    expect(historyRequests).toHaveLength(1);
  });

  it('records the senders of the messages it adds', async () => {
    const service = setUp([10, 11], [10]);

    service.addRepairs({ chats: [CHANNEL_ID] });
    await service.processQueue();

    expect(service.db.prepare('SELECT username FROM users WHERE user_id = ?').get('2').username).toBe('friend');
  });

  it('carries on from its cursor after a restart', async () => {
    const historyIds = range(10, 259);
    const first = setUp(historyIds, [10]);
    first.addRepairs({ chats: [CHANNEL_ID] });
    await first._repairNextPage(first._getNextRepair());
    expect(repairRow(first)).toMatchObject({ status: 'in_progress', cursor_message_id: 160 });
    first.close();

    historyRequests = [];
    const second = makeService();
    await second.processQueue();

    expect(historyRequests[0]).toMatchObject({ offsetId: 160 });
    expect(archivedIds(second)).toEqual(historyIds);
  });

  it('waits out a rate limit and goes on without counting it as a failure', async () => {
    const service = setUp(range(10, 30), [10]);
    const history = answer;
    let limited = false;
    answer = (request) => {
      if (!limited) {
        limited = true;
        throw new Error('FLOOD_WAIT_0');
      }
      return history(request);
    };

    service.addRepairs({ chats: [CHANNEL_ID] });
    await service.processQueue();

    expect(limited).toBe(true);
    expect(repairRow(service)).toMatchObject({ status: 'idle', added_count: 20, error: null });
  });

  it('keeps its cursor through a failure, and the hourly reset of failed jobs picks it up there', async () => {
    const historyIds = range(10, 259);
    const service = setUp(historyIds, [10]);
    const history = answer;
    let calls = 0;
    answer = (request) => {
      calls += 1;
      if (calls === 2) {
        throw new Error('Connection lost');
      }
      return history(request);
    };

    service.addRepairs({ chats: [CHANNEL_ID] });
    await service.processQueue();
    expect(repairRow(service)).toMatchObject({ status: 'error', cursor_message_id: 160 });

    historyRequests = [];
    service._resetErroredJobs();
    await service.processQueue();
    expect(historyRequests[0]).toMatchObject({ offsetId: 160 });
    expect(archivedIds(service)).toEqual(historyIds);
  });

  it('records a failure while writing the page instead of letting it take the queue down', async () => {
    const service = setUp(range(10, 30), [10]);
    vi.spyOn(service, 'archiveHistoryMessages').mockImplementation(() => {
      throw new Error('SQLITE_FULL: database or disk is full');
    });

    service.addRepairs({ chats: [CHANNEL_ID] });
    await expect(service.processQueue()).resolves.toBeUndefined();

    expect(repairRow(service)).toMatchObject({ status: 'error', error: expect.stringContaining('SQLITE_FULL'), cursor_message_id: null });
  });

  it('starts over when asked again while a page is still loading', async () => {
    const service = setUp(range(10, 259), [10]);
    const history = answer;
    let release;
    const held = new Promise((resolve) => {
      release = resolve;
    });
    answer = async (request) => {
      await held;
      return history(request);
    };

    service.addRepairs({ chats: [CHANNEL_ID], sinceDate: BASE_DATE + 200 });
    const page = service._repairNextPage(service._getNextRepair());
    while (historyRequests.length === 0) {
      await new Promise((resolve) => setImmediate(resolve));
    }
    service.addRepairs({ chats: [CHANNEL_ID], afterId: 0 });
    release();
    await page;

    expect(repairRow(service)).toMatchObject({
      status: 'pending',
      after_message_id: 0,
      since_date: null,
      cursor_message_id: null,
      checked_count: 0,
      added_count: 0,
    });
  });

  it('stops on a failure, and the hourly reset of failed jobs picks it up again', async () => {
    const service = setUp(range(10, 30), [10]);
    const history = answer;
    answer = () => {
      throw new Error('Connection lost');
    };

    service.addRepairs({ chats: [CHANNEL_ID] });
    await service.processQueue();
    expect(repairRow(service)).toMatchObject({ status: 'error', error: expect.stringContaining('Connection lost') });

    answer = history;
    service._resetErroredJobs();
    await service.processQueue();
    expect(repairRow(service)).toMatchObject({ status: 'idle', added_count: 20, error: null });
  });

  it('skips a chat Telegram refuses for good, and the hourly reset leaves it alone', async () => {
    const service = setUp(range(10, 30), [10]);
    answer = () => {
      throw tl.RpcError.fromTl({ _: 'rpc_error', errorCode: 400, errorMessage: 'CHANNEL_PRIVATE' });
    };

    service.addRepairs({ chats: [CHANNEL_ID] });
    await service.processQueue();
    expect(repairRow(service)).toMatchObject({ status: 'skipped', error: expect.stringContaining("haven't joined") });

    historyRequests = [];
    service._resetErroredJobs();
    await service.processQueue();
    expect(historyRequests).toHaveLength(0);
    expect(repairRow(service).status).toBe('skipped');
  });

  it.each([
    ['CHANNEL_INVALID', () => tl.RpcError.fromTl({ _: 'rpc_error', errorCode: 400, errorMessage: 'CHANNEL_INVALID' })],
    ['a peer missing from the cache', () => new MtPeerNotFoundError('Peer -1000000000500 is not found in local cache')],
  ])('skips a chat on %s', async (_, makeError) => {
    const service = setUp(range(10, 30), [10]);
    answer = () => {
      throw makeError();
    };

    service.addRepairs({ chats: [CHANNEL_ID] });
    await service.processQueue();

    expect(repairRow(service).status).toBe('skipped');
  });

  // These break every request, not this chat: skipping would give up on each chat in turn.
  it.each([
    [406, 'AUTH_KEY_DUPLICATED'],
    [420, 'FROZEN_METHOD_INVALID'],
  ])('keeps %i %s a failure that is retried', async (errorCode, errorMessage) => {
    const service = setUp(range(10, 30), [10]);
    answer = () => {
      throw tl.RpcError.fromTl({ _: 'rpc_error', errorCode, errorMessage });
    };

    service.addRepairs({ chats: [CHANNEL_ID] });
    await service.processQueue();

    expect(repairRow(service).status).toBe('error');
  });

  it('stops for a chat whose sync was turned off, and stays stopped while it is off', async () => {
    const service = setUp(range(10, 30), [10]);
    service.addRepairs({ chats: [CHANNEL_ID] });
    service.db.prepare('UPDATE channels SET sync_enabled = 0 WHERE channel_id = ?').run(CHANNEL_ID);

    await service.processQueue();
    service._resetErroredJobs();

    expect(historyRequests).toHaveLength(0);
    expect(repairRow(service)).toMatchObject({ status: 'error', error: 'Sync is disabled for this chat.' });
  });

  it('lets jobs and gap fills go before each page', async () => {
    const service = setUp(range(10, 259), [10]);
    service.addRepairs({ chats: [CHANNEL_ID] });
    const order = [];
    const repairPage = service._repairNextPage.bind(service);
    vi.spyOn(service, '_repairNextPage').mockImplementation(async (repair) => {
      order.push('repair');
      if (order.length === 1) {
        service.addJob(OTHER_ID);
      }
      return repairPage(repair);
    });
    vi.spyOn(service, '_processJob').mockImplementation(async (job) => {
      order.push('job');
      service._updateJobStatus(job.id, 'idle');
    });
    let gapsLeft = 1;
    vi.spyOn(service, '_getNextGapChannelId').mockImplementation(() => (order.includes('job') && gapsLeft > 0 ? OTHER_ID : null));
    vi.spyOn(service, '_fillChannelGap').mockImplementation(async () => {
      order.push('gap');
      gapsLeft -= 1;
    });

    await service.processQueue();

    // Four pages: three of history and the empty one that ends the walk.
    expect(order).toEqual(['repair', 'job', 'gap', 'repair', 'repair', 'repair']);
  });

  it('finds a chat by its username, with or without @', () => {
    const service = setUp([10], [10]);

    expect(service.addRepairs({ chats: ['@Group'] })).toEqual([CHANNEL_ID]);
    expect(service.addRepairs({ chats: ['group'] })).toEqual([CHANNEL_ID]);
  });

  it('refuses a chat that is not in the archive, or has sync off', () => {
    const service = setUp([10], [10], { syncEnabled: 0 });

    expect(() => service.addRepairs({ chats: ['-1000000000999'] })).toThrow(/not in the archive/);
    expect(() => service.addRepairs({ chats: [CHANNEL_ID] })).toThrow(/sync disabled/);
    expect(service.listRepairs()).toEqual([]);
  });

  it('repairs every chat with sync on when asked for all', () => {
    const service = setUp([10], [10]);
    const insert = service.db.prepare('INSERT INTO channels (channel_id, sync_enabled) VALUES (?, ?)');
    insert.run(OTHER_ID, 1);
    insert.run(OFF_ID, 0);

    expect(service.addRepairs({ all: true }).sort()).toEqual([CHANNEL_ID, OTHER_ID].sort());
  });

  // Telegram sends nothing from a chat the account has left, so what the archive lacks
  // there is no loss.
  it('leaves out of --all the chats the last dialog list did not have, but repairs one by name', async () => {
    const service = setUp([10], [10]);
    service.db.prepare('INSERT INTO channels (channel_id, sync_enabled) VALUES (?, 1)').run(OTHER_ID);
    service.telegramClient.listDialogs = vi.fn().mockResolvedValue([{ id: Number(OTHER_ID), title: 'Other' }]);

    await service.refreshChannelsFromDialogs();

    expect(service.addRepairs({ all: true })).toEqual([OTHER_ID]);
    expect(service.addRepairs({ chats: [CHANNEL_ID] })).toEqual([CHANNEL_ID]);
  });

  it('marks who is in the dialog list only from a list with something in it', async () => {
    const service = setUp([10], [10]);
    service.db.prepare('INSERT INTO channels (channel_id, sync_enabled) VALUES (?, 1)').run(OTHER_ID);
    const inDialogs = () => Object.fromEntries(service.db.prepare('SELECT channel_id, in_dialogs FROM channels').all()
      .map((row) => [row.channel_id, row.in_dialogs]));
    expect(inDialogs()).toEqual({ [CHANNEL_ID]: null, [OTHER_ID]: null });

    service.telegramClient.listDialogs = vi.fn().mockResolvedValue([{ id: Number(CHANNEL_ID), title: 'Group' }]);
    await service.refreshChannelsFromDialogs();
    expect(inDialogs()).toEqual({ [CHANNEL_ID]: 1, [OTHER_ID]: 0 });

    service.telegramClient.listDialogs = vi.fn().mockResolvedValue([]);
    await service.refreshChannelsFromDialogs();
    expect(inDialogs()).toEqual({ [CHANNEL_ID]: 1, [OTHER_ID]: 0 });
  });

  it('starts a repair asked for again from the top, with the new bounds', async () => {
    const service = setUp(range(10, 259), [10]);
    service.addRepairs({ chats: [CHANNEL_ID] });
    await service._repairNextPage(service._getNextRepair());

    service.addRepairs({ chats: [CHANNEL_ID], afterId: 250 });

    expect(repairRow(service)).toMatchObject({
      status: 'pending',
      after_message_id: 250,
      cursor_message_id: null,
      checked_count: 0,
      added_count: 0,
    });
  });

  it('keeps suggesting a repair after a dry run of every chat', () => {
    const service = setUp([10], [10]);
    service._setRepairNotNeeded = vi.fn(service._setRepairNotNeeded.bind(service));
    service.db.prepare("DELETE FROM search_meta WHERE key = 'archive_repair'").run();

    service.addRepairs({ all: true, dryRun: true });

    expect(service._setRepairNotNeeded).not.toHaveBeenCalled();
    expect(service.isRepairSuggested()).toBe(true);
  });

  it('never suggests a repair for an archive started by this version', () => {
    const service = setUp([10], [10]);

    expect(service.isRepairSuggested()).toBe(false);
  });

  it('suggests a repair of an older archive until one of every chat has run', () => {
    const service = setUp([10], [10]);
    service.db.prepare("DELETE FROM search_meta WHERE key = 'archive_repair'").run();
    expect(service.isRepairSuggested()).toBe(true);

    service.addRepairs({ chats: [CHANNEL_ID] });
    expect(service.isRepairSuggested()).toBe(true);

    service.addRepairs({ all: true });
    expect(service.isRepairSuggested()).toBe(false);
  });

  it('does not suggest a repair of an older archive with no synced messages', () => {
    const service = setUp([10], []);
    service.db.prepare("DELETE FROM search_meta WHERE key = 'archive_repair'").run();

    expect(service.isRepairSuggested()).toBe(false);
  });
});
