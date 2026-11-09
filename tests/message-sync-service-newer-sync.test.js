// Runs the catch-up through real mtcute and a real SQLite archive, stubbing only
// messages.getHistory: what goes wrong here depends on how mtcute turns paging
// options into offsets and how Telegram applies them, which a mocked
// getMessagesByChannelId could not show.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { Long, Message, PeersIndex } from '@mtcute/core';
import { iterHistory, resolvePeer } from '@mtcute/core/methods.js';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { OPERATIONS } from '../core/ops/index.js';
import { executeOperation } from '../core/ops/execute.js';
import MessageSyncService from '../message-sync-service.js';
import TelegramClient from '../telegram-client.js';

const BARE_CHANNEL_ID = 500;
const CHANNEL_ID = '-1000000000500';
const ACCESS_HASH = Long.fromNumber(1);
const chats = [
  { _: 'channel', id: BARE_CHANNEL_ID, title: 'Group', accessHash: ACCESS_HASH, megagroup: true },
];

function rawMessage(id) {
  return {
    _: 'message',
    id,
    date: 1700000000 + id,
    peerId: { _: 'peerChannel', channelId: BARE_CHANNEL_ID },
    message: `message ${id}`,
  };
}

function parsed(id) {
  return new Message(rawMessage(id), PeersIndex.from({ users: [], chats }));
}

function range(from, to) {
  return Array.from({ length: to - from + 1 }, (_, index) => from + index);
}

function sortedIds(messages) {
  return messages.map((message) => message.id).sort((a, b) => a - b);
}

// messages.getHistory as core.telegram.org/api/offsets documents it: a newest-first
// list is sliced by offset_id + add_offset and limit, and only that slice is then
// filtered by min_id and max_id.
function getHistoryResponse(ids, { offsetId, addOffset, limit, minId, maxId }) {
  const newestFirst = [...ids].sort((a, b) => b - a);
  const offsetFromId = offsetId ? newestFirst.filter((id) => id >= offsetId).length : 0;
  const start = offsetFromId + addOffset;
  const slice = newestFirst
    .slice(Math.max(start, 0), Math.max(start + limit, 0))
    .filter((id) => (!minId || id > minId) && (!maxId || id < maxId));
  return {
    _: 'messages.channelMessages',
    messages: slice.map(rawMessage),
    users: [],
    chats,
    topics: [],
    count: ids.length,
    pts: 0,
  };
}

describe('catching up on messages newer than the cursor', () => {
  let storeDir;
  let service;
  let telegramClient;
  let historyIds;
  let requests;
  let onRequest;

  beforeEach(() => {
    storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-sync-newer-test-'));
    historyIds = [];
    requests = [];
    onRequest = null;

    const raw = {
      call: vi.fn(async (request) => {
        requests.push(request);
        onRequest?.(requests.length);
        return getHistoryResponse(historyIds, request);
      }),
      storage: {
        self: { getCached: () => ({ userId: 1 }) },
        peers: {
          getById: async () => ({
            _: 'inputPeerChannel',
            channelId: BARE_CHANNEL_ID,
            accessHash: ACCESS_HASH,
          }),
        },
      },
    };
    telegramClient = Object.create(TelegramClient.prototype);
    telegramClient.ensureLogin = vi.fn().mockResolvedValue(undefined);
    telegramClient.client = {
      resolvePeer: (peer) => resolvePeer(raw, peer),
      iterHistory: (peer, params) => iterHistory(raw, peer, params),
    };
    service = new MessageSyncService(telegramClient, {
      dbPath: path.join(storeDir, 'messages.db'),
      batchSize: 2,
      interJobDelayMs: 0,
      interBatchDelayMs: 1,
    });
  });

  afterEach(() => {
    service?.close();
    fs.rmSync(storeDir, { recursive: true, force: true });
  });

  const seedChat = ({ lastMessageId = 10 } = {}) => {
    service.db.prepare(`
      INSERT INTO channels (channel_id, peer_title, sync_enabled, last_message_id)
      VALUES (?, 'Group', 1, ?)
    `).run(CHANNEL_ID, lastMessageId);
  };

  const archivedIds = () => service.db.prepare(`
    SELECT message_id FROM messages WHERE channel_id = ? ORDER BY message_id
  `).all(CHANNEL_ID).map((row) => row.message_id);

  const cursor = () => service.db.prepare(`
    SELECT last_message_id FROM channels WHERE channel_id = ?
  `).get(CHANNEL_ID).last_message_id;

  const jobStatus = () => service.db.prepare(`
    SELECT status FROM jobs WHERE channel_id = ?
  `).get(CHANNEL_ID).status;

  const oldestCursor = () => service.db.prepare(`
    SELECT oldest_message_id FROM channels WHERE channel_id = ?
  `).get(CHANNEL_ID).oldest_message_id;

  const addJob = () => service.addJob(CHANNEL_ID, { depth: 1 });

  const stopOnPage = (stopPage) => {
    onRequest = (page) => {
      if (page === stopPage) service.stopRequested = true;
    };
  };

  const failOnPage = (failPage) => {
    onRequest = (page) => {
      if (page === failPage) throw new Error('network down');
    };
  };

  const resume = () => {
    service.stopRequested = false;
    onRequest = null;
    requests = [];
  };

  it('archives every message when more than a batch arrived since the cursor', async () => {
    seedChat();
    historyIds = range(1, 15);

    await service._syncNewerMessages(CHANNEL_ID);

    expect(archivedIds()).toEqual(range(11, 15));
    expect(cursor()).toBe(15);
  });

  it('pages across ids that no longer exist in the chat', async () => {
    seedChat();
    historyIds = [...range(1, 10), 12, 13, 17, 21, 22, 30, 41];

    await service._syncNewerMessages(CHANNEL_ID);

    expect(archivedIds()).toEqual([12, 13, 17, 21, 22, 30, 41]);
    expect(cursor()).toBe(41);
  });

  it('finds nothing to do when the cursor is at the newest message', async () => {
    seedChat({ lastMessageId: 15 });
    historyIds = range(1, 15);

    const result = await service._syncNewerMessages(CHANNEL_ID);

    expect(result.hasMoreNewer).toBe(false);
    expect(archivedIds()).toEqual([]);
    expect(cursor()).toBe(15);
  });

  it('does not add or change rows when run again', async () => {
    seedChat();
    historyIds = range(1, 15);

    await service._syncNewerMessages(CHANNEL_ID);
    await service._syncNewerMessages(CHANNEL_ID);
    service.db.prepare('UPDATE channels SET last_message_id = 10 WHERE channel_id = ?').run(CHANNEL_ID);
    await service._syncNewerMessages(CHANNEL_ID);

    expect(archivedIds()).toEqual(range(11, 15));
    expect(cursor()).toBe(15);
  });

  it('takes only the newest batch for a chat with no cursor yet', async () => {
    seedChat({ lastMessageId: 0 });
    historyIds = range(1, 15);

    await service._syncNewerMessages(CHANNEL_ID);

    expect(archivedIds()).toEqual([14, 15]);
    expect(cursor()).toBe(15);
  });

  it('keeps the cursor on what is archived when stopped, and the next run finishes', async () => {
    seedChat();
    historyIds = range(1, 15);
    stopOnPage(2);

    const stopped = await service._syncNewerMessages(CHANNEL_ID);

    expect(stopped.stoppedEarly).toBe(true);
    expect(archivedIds()).toEqual([11, 12]);
    expect(cursor()).toBe(12);

    resume();
    await service._syncNewerMessages(CHANNEL_ID);

    expect(archivedIds()).toEqual(range(11, 15));
    expect(cursor()).toBe(15);
  });

  it('keeps earlier pages when a request fails, and the next run continues from them', async () => {
    seedChat();
    historyIds = range(1, 15);
    failOnPage(2);

    await expect(service._syncNewerMessages(CHANNEL_ID)).rejects.toThrow('network down');

    expect(archivedIds()).toEqual([11, 12]);
    expect(cursor()).toBe(12);

    resume();
    await service._syncNewerMessages(CHANNEL_ID);

    expect(requests[0].offsetId).toBe(13);
    expect(archivedIds()).toEqual(range(11, 15));
    expect(cursor()).toBe(15);
  });

  describe('updates arriving around an unfinished job', () => {
    const interruptions = {
      stop: { arrange: () => stopOnPage(2), status: 'pending' },
      failure: { arrange: () => failOnPage(2), status: 'error' },
    };

    it.each(Object.keys(interruptions))(
      'a new message does not lift the cursor over what the job has not fetched (%s)',
      async (name) => {
        const { arrange, status } = interruptions[name];
        seedChat();
        historyIds = range(1, 15);
        const job = addJob();
        arrange();

        await service._processJob(job);
        expect(jobStatus()).toBe(status);
        expect(cursor()).toBe(12);

        resume();
        historyIds = range(1, 16);
        service._handleIncomingMessage(parsed(16), { isEdit: false });

        expect(archivedIds()).toEqual([11, 12, 16]);
        expect(cursor()).toBe(12);

        await service._processJob(addJob());

        expect(archivedIds()).toEqual(range(11, 16));
        expect(cursor()).toBe(16);
        expect(jobStatus()).toBe('idle');
      },
    );

    it('a channel-too-long diff does not lift the cursor over what the job has not fetched', async () => {
      seedChat();
      historyIds = range(1, 15);
      const job = addJob();
      stopOnPage(2);

      await service._processJob(job);
      expect(cursor()).toBe(12);

      service._handleChannelTooLong({
        channelId: CHANNEL_ID,
        diff: { messages: [rawMessage(14), rawMessage(15)], users: [], chats },
      });

      expect(archivedIds()).toEqual([11, 12, 14, 15]);
      expect(cursor()).toBe(12);

      resume();
      await service._processJob(addJob());

      expect(archivedIds()).toEqual(range(11, 15));
      expect(cursor()).toBe(15);
    });

    it('a new message does not lift the cursor after the unfinished job is canceled', async () => {
      seedChat();
      historyIds = range(1, 15);
      const job = addJob();
      stopOnPage(2);

      await service._processJob(job);
      expect(cursor()).toBe(12);

      resume();
      service.cancelJobs({ channelId: CHANNEL_ID });
      historyIds = range(1, 16);
      service._handleIncomingMessage(parsed(16), { isEdit: false });

      expect(cursor()).toBe(12);

      await service._processJob(addJob());

      expect(archivedIds()).toEqual(range(11, 16));
      expect(cursor()).toBe(16);
    });

    it('a new message does not lift the cursor after a stopped chat is turned off and on', async () => {
      seedChat();
      historyIds = range(1, 15);
      const job = addJob();
      stopOnPage(2);

      await service._processJob(job);
      expect(cursor()).toBe(12);

      resume();
      service.setChannelSync(CHANNEL_ID, false);
      await service._processJob(service._getNextJob());
      expect(jobStatus()).toBe('idle');

      historyIds = range(1, 16);
      const result = await executeOperation(
        OPERATIONS.channelsWatch,
        { channelId: CHANNEL_ID },
        { services: { messageSyncService: service } },
      );
      expect(result.jobQueued).toBe(true);
      service._handleIncomingMessage(parsed(16), { isEdit: false });

      expect(cursor()).toBe(12);

      await vi.waitFor(() => expect(jobStatus()).toBe('idle'));
      expect(archivedIds()).toEqual(range(11, 16));
      expect(cursor()).toBe(16);
    });

    it('turning a chat back on fetches what arrived while it was off and keeps the depth', async () => {
      seedChat();
      historyIds = range(1, 15);
      await service._processJob(addJob());
      expect(cursor()).toBe(15);

      service.setChannelSync(CHANNEL_ID, false);
      historyIds = range(1, 18);
      await executeOperation(
        OPERATIONS.channelsWatch,
        { channelId: CHANNEL_ID },
        { services: { messageSyncService: service } },
      );

      await vi.waitFor(() => expect(jobStatus()).toBe('idle'));
      expect(archivedIds()).toEqual(range(11, 18));
      expect(cursor()).toBe(18);
    });

    it('a new message moves the cursor once only backfill is left', async () => {
      seedChat();
      historyIds = range(1, 15);
      const job = service.addJob(CHANNEL_ID, { depth: 1000 });
      // The third page of catch-up takes two requests: its short answer, then an empty one.
      stopOnPage(5);

      await service._processJob(job);
      expect(jobStatus()).toBe('pending');
      expect(cursor()).toBe(15);

      service._handleIncomingMessage(parsed(16), { isEdit: false });

      expect(cursor()).toBe(16);
    });

    it('holds the cursor for a job queued before the archive had the pending flag', () => {
      seedChat();
      addJob();
      const dbPath = service.db.name;
      service.db.exec('ALTER TABLE channels DROP COLUMN newer_sync_pending');
      service.db.close();
      service = new MessageSyncService(telegramClient, {
        dbPath,
        batchSize: 2,
        interJobDelayMs: 0,
        interBatchDelayMs: 1,
      });

      service._handleIncomingMessage(parsed(16), { isEdit: false });

      expect(cursor()).toBe(10);
    });

    it('a new message moves the cursor once the job is finished', async () => {
      seedChat();
      historyIds = range(1, 15);

      await service._processJob(addJob());
      expect(jobStatus()).toBe('idle');

      service._handleIncomingMessage(parsed(16), { isEdit: false });

      expect(cursor()).toBe(16);
    });

    it('a new message moves the cursor in a chat with no job', () => {
      seedChat();

      service._handleIncomingMessage(parsed(16), { isEdit: false });

      expect(cursor()).toBe(16);
    });

    it('an update after a long pause holds the cursor, without waiting for the periodic check', () => {
      seedChat();
      const reconcile = vi.spyOn(service, 'reconcileWithDialogs').mockResolvedValue(null);
      service.realtimeActive = true;
      service.lastCheckAt = Date.now() - 6 * 60 * 1000;

      service._handleIncomingMessage(parsed(16), { isEdit: false });

      expect(cursor()).toBe(10);
      expect(reconcile).toHaveBeenCalledTimes(1);
    });

    it('runs a failed job again within the hour, once per hour', () => {
      seedChat();
      addJob();
      service.db.prepare('UPDATE jobs SET status = ? WHERE channel_id = ?').run('error', CHANNEL_ID);
      const processQueue = vi.spyOn(service, 'processQueue').mockResolvedValue();

      service._retryErroredJobsWhenDue();
      expect(jobStatus()).toBe('error');

      service.lastErroredJobsRetryAt = Date.now() - 60 * 60 * 1000;
      service._retryErroredJobsWhenDue();
      expect(jobStatus()).toBe('pending');
      expect(processQueue).toHaveBeenCalledTimes(1);

      service.db.prepare('UPDATE jobs SET status = ? WHERE channel_id = ?').run('error', CHANNEL_ID);
      service._retryErroredJobsWhenDue();
      expect(jobStatus()).toBe('error');
    });

    it('leaves a failed job alone while its chat is turned off, and runs it once turned on', () => {
      seedChat();
      addJob();
      service.db.prepare('UPDATE jobs SET status = ? WHERE channel_id = ?').run('error', CHANNEL_ID);
      service.setChannelSync(CHANNEL_ID, false);
      vi.spyOn(service, 'processQueue').mockResolvedValue();

      service._resetErroredJobs();
      expect(jobStatus()).toBe('error');

      service.setChannelSync(CHANNEL_ID, true);
      service.queueResyncJob(CHANNEL_ID);
      expect(jobStatus()).toBe('pending');
    });

    it('does not lose an outage that ends just as a comparison finishes', async () => {
      const refresh = vi.spyOn(service, 'refreshChannelsFromDialogs').mockResolvedValue(1);
      vi.spyOn(service, 'resumePendingJobs').mockImplementation(() => {});
      const finishComparison = service._reconcileUntilDone.bind(service);
      vi.spyOn(service, '_reconcileUntilDone').mockImplementationOnce(() => finishComparison().then((count) => {
        service._reconcileAfterOutage();
        return count;
      }));

      await service.reconcileWithDialogs();

      await vi.waitFor(() => expect(refresh).toHaveBeenCalledTimes(2));
      await vi.waitFor(() => expect(service.reconcileNeeded).toBe(false));
    });

    it('a job canceled before its first sync does not stop updates from setting a cursor', () => {
      seedChat({ lastMessageId: 0 });
      addJob();
      service.cancelJobs({ channelId: CHANNEL_ID });

      service._handleIncomingMessage(parsed(16), { isEdit: false });

      expect(cursor()).toBe(16);
    });

    it('a job canceled with a cursor in place still holds it for the next sync', () => {
      seedChat();
      addJob();
      service.cancelJobs({ channelId: CHANNEL_ID });

      service._handleIncomingMessage(parsed(16), { isEdit: false });

      expect(cursor()).toBe(10);
    });
  });

  it('an edit of an old message leaves the cursors, so backfill does not skip below them', async () => {
    seedChat({ lastMessageId: 30 });
    service.db.prepare('UPDATE channels SET oldest_message_id = 21 WHERE channel_id = ?').run(CHANNEL_ID);
    historyIds = range(1, 30);

    service._handleIncomingMessage(parsed(5), { isEdit: true });

    expect(oldestCursor()).toBe(21);
    expect(cursor()).toBe(30);

    await service._processJob(service.addJob(CHANNEL_ID, { depth: 1000 }));

    expect(archivedIds()).toEqual(range(1, 20));
  });

  describe('an id that jumps over the cursor', () => {
    const recordedGap = () => service._getChannel(CHANNEL_ID).gap_after_message_id;

    beforeEach(() => {
      vi.spyOn(service, 'processQueue').mockResolvedValue();
    });

    it('records a gap from the cursor and wakes the queue', () => {
      seedChat();

      service._handleIncomingMessage(parsed(16), { isEdit: false });

      expect(recordedGap()).toBe(10);
      expect(cursor()).toBe(16);
      expect(service.processQueue).toHaveBeenCalledTimes(1);
    });

    it('is not what the next id in sequence does', () => {
      seedChat();

      service._handleIncomingMessage(parsed(11), { isEdit: false });

      expect(recordedGap()).toBeNull();
      expect(cursor()).toBe(11);
      expect(service.processQueue).not.toHaveBeenCalled();
    });

    it('is not what a message below the cursor does', () => {
      seedChat();

      service._handleIncomingMessage(parsed(5), { isEdit: false });

      expect(recordedGap()).toBeNull();
      expect(cursor()).toBe(10);
    });

    it('is not what an edit does', () => {
      seedChat();

      service._handleIncomingMessage(parsed(16), { isEdit: true });

      expect(recordedGap()).toBeNull();
      expect(cursor()).toBe(10);
    });

    it('means nothing in a chat with no cursor', () => {
      seedChat({ lastMessageId: 0 });

      service._handleIncomingMessage(parsed(16), { isEdit: false });

      expect(recordedGap()).toBeNull();
      expect(cursor()).toBe(16);
    });

    it('is left to the comparison with the dialog list while cursors are held', () => {
      seedChat();
      service.reconcileNeeded = true;

      service._handleIncomingMessage(parsed(16), { isEdit: false });

      expect(recordedGap()).toBeNull();
      expect(cursor()).toBe(10);
    });

    it('is left to the job while a newer sync is pending', () => {
      seedChat();
      addJob();

      service._handleIncomingMessage(parsed(16), { isEdit: false });

      expect(recordedGap()).toBeNull();
      expect(cursor()).toBe(10);
    });

    it('keeps the start of a gap already recorded', () => {
      seedChat();
      service._setChannelGapStart(CHANNEL_ID, 5);

      service._handleIncomingMessage(parsed(16), { isEdit: false });

      expect(recordedGap()).toBe(5);
      expect(cursor()).toBe(16);
      expect(service.processQueue).not.toHaveBeenCalled();
    });

    it('closes a gap under a cursor that history no longer reaches', async () => {
      seedChat({ lastMessageId: 20 });
      service._setChannelGapStart(CHANNEL_ID, 10);
      historyIds = range(1, 19);

      await service._fillChannelGap(CHANNEL_ID);

      expect(archivedIds()).toEqual(range(11, 19));
      expect(recordedGap()).toBeNull();
      expect(cursor()).toBe(20);
    });

    it('costs one page when the skipped ids were deleted, and closes the gap', async () => {
      seedChat();
      historyIds = [...range(1, 10), 16];
      service._handleIncomingMessage(parsed(16), { isEdit: false });

      await service._fillChannelGap(CHANNEL_ID);

      expect(archivedIds()).toEqual([16]);
      expect(recordedGap()).toBeNull();
      expect(cursor()).toBe(16);
    });

    it('keeps a gap open when an update passes what its last page could see', async () => {
      seedChat();
      service._markChannelGap(CHANNEL_ID);
      historyIds = range(1, 11);
      const fetchPage = telegramClient.getMessagesByChannelId.bind(telegramClient);
      vi.spyOn(telegramClient, 'getMessagesByChannelId').mockImplementationOnce(async (...args) => {
        const page = await fetchPage(...args);
        // The chat moves on between Telegram's answer and its arrival here.
        historyIds = range(1, 16);
        service._handleIncomingMessage(parsed(16), { isEdit: false });
        return page;
      });

      await service._fillChannelGap(CHANNEL_ID);

      expect(recordedGap()).toBe(11);
      expect(cursor()).toBe(16);

      await service._fillChannelGap(CHANNEL_ID);

      expect(archivedIds()).toEqual(range(11, 16));
      expect(recordedGap()).toBeNull();
    });
  });

  describe('what a history fetch says about the chat', () => {
    const channelRow = () => service._getChannel(CHANNEL_ID);

    it('gives its real title and type', async () => {
      seedChat();
      service._markChannelGap(CHANNEL_ID);
      service.db.prepare(`UPDATE channels SET peer_title = 'Unknown', peer_type = 'chat' WHERE channel_id = ?`).run(CHANNEL_ID);
      historyIds = range(1, 12);

      await service._fillChannelGap(CHANNEL_ID);

      expect(channelRow().peer_title).toBe('Group');
      expect(channelRow().peer_type).toBe('channel');
    });

    it('changes nothing when the page is empty', async () => {
      seedChat();
      service._markChannelGap(CHANNEL_ID);
      service.db.prepare(`UPDATE channels SET peer_type = 'channel' WHERE channel_id = ?`).run(CHANNEL_ID);
      historyIds = range(1, 10);

      await service._fillChannelGap(CHANNEL_ID);

      expect(channelRow().peer_title).toBe('Group');
      expect(channelRow().peer_type).toBe('channel');
    });
  });

  describe('a delete that names no channel', () => {
    function seedPrivateRow(id) {
      service.db.prepare(`INSERT INTO channels (channel_id, peer_type, sync_enabled) VALUES ('2', 'user', 1)`).run();
      service.db.prepare(`INSERT INTO messages (channel_id, message_id, date, text) VALUES ('2', ?, 1, 'hi')`).run(id);
    }
    const privateRows = () => service.db.prepare(`SELECT message_id FROM messages WHERE channel_id = '2'`).all();

    it('removes the row of a private chat, and never one of a channel, whatever its label', () => {
      seedChat();
      service._handleIncomingMessage(parsed(11), { isEdit: false });
      service.db.prepare(`UPDATE channels SET peer_type = 'chat' WHERE channel_id = ?`).run(CHANNEL_ID);
      seedPrivateRow(11);

      service._handleDeleteMessage({ messageIds: [11] });

      expect(privateRows()).toEqual([]);
      expect(archivedIds()).toEqual([11]);
    });

    it('still reaches a basic group whose marked id looks like a channel prefix', () => {
      service.db.prepare(`INSERT INTO channels (channel_id, peer_type, sync_enabled) VALUES ('-1001234567', 'chat', 1)`).run();
      service.db.prepare(`INSERT INTO messages (channel_id, message_id, date, text) VALUES ('-1001234567', 11, 1, 'hi')`).run();

      service._handleDeleteMessage({ messageIds: [11] });

      expect(service.db.prepare(`SELECT message_id FROM messages WHERE channel_id = '-1001234567'`).all()).toEqual([]);
    });
  });

  describe('the hourly comparison with the dialog list', () => {
    it('stays out of the way of a comparison that is already due', () => {
      const compare = vi.spyOn(service, 'reconcileWithDialogs').mockResolvedValue(1);
      service.lastReconcileAt = 0;
      service.reconcileNeeded = true;

      service._reconcileWhenDue();

      expect(compare).not.toHaveBeenCalled();
    });

    it('counts any finished comparison, so an outage is not followed by another', async () => {
      vi.spyOn(service, 'refreshChannelsFromDialogs').mockResolvedValue(1);
      service.lastReconcileAt = 0;

      await service.reconcileWithDialogs();

      expect(service.lastReconcileAt).toBeGreaterThan(Date.now() - 1000);
    });

    it('runs when due, then waits another hour, and fills what it finds', async () => {
      const compare = vi.spyOn(service, 'reconcileWithDialogs').mockResolvedValue(1);
      const processQueue = vi.spyOn(service, 'processQueue').mockResolvedValue();

      service._reconcileWhenDue();
      expect(compare).not.toHaveBeenCalled();

      service.lastReconcileAt = Date.now() - 60 * 60 * 1000;
      service._reconcileWhenDue();
      service._reconcileWhenDue();

      expect(compare).toHaveBeenCalledTimes(1);
      await vi.waitFor(() => expect(processQueue).toHaveBeenCalledTimes(1));
    });

    it('does not start the queue when it was stopped', async () => {
      vi.spyOn(service, 'reconcileWithDialogs').mockResolvedValue(null);
      const processQueue = vi.spyOn(service, 'processQueue').mockResolvedValue();
      service.lastReconcileAt = 0;

      service._reconcileWhenDue();
      await Promise.resolve();
      await Promise.resolve();

      expect(processQueue).not.toHaveBeenCalled();
    });
  });

  describe('ids a page of history skipped', () => {
    function queued() {
      return service.db
        .prepare('SELECT message_id FROM messages_to_recheck ORDER BY message_id')
        .all()
        .map((row) => row.message_id);
    }

    function seedAt(channelId, id) {
      service.upsertChannels([{ id: channelId, title: 'Group', type: 'channel' }]);
      service._updateChannelCursors(channelId, { lastMessageId: id, oldestMessageId: id });
    }

    function at(messageId, secondsLater = 0) {
      vi.spyOn(Date, 'now').mockReturnValue((1700000000 + messageId + secondsLater) * 1000);
    }

    afterEach(() => {
      vi.restoreAllMocks();
    });

    it('remembers those next to fresh messages', async () => {
      seedAt(CHANNEL_ID, 10);
      historyIds = [11, 14, 15, 18];
      at(18);

      await service._syncNewerMessages(CHANNEL_ID);

      expect(queued()).toEqual([12, 13, 16, 17]);
    });

    it('leaves out those next to messages older than ten minutes', async () => {
      seedAt(CHANNEL_ID, 10);
      historyIds = [13, 16];
      at(16, 10 * 60 - 2);

      await service._syncNewerMessages(CHANNEL_ID);

      expect(queued()).toEqual([14, 15]);
    });

    it('has nothing to remember when ids run in sequence', async () => {
      seedAt(CHANNEL_ID, 10);
      historyIds = [11, 12, 13];
      at(13);

      await service._syncNewerMessages(CHANNEL_ID);

      expect(queued()).toEqual([]);
    });

    it('leaves out a jump of more than a hundred ids', async () => {
      seedAt(CHANNEL_ID, 10);
      historyIds = [110, 212];
      at(212);

      await service._syncNewerMessages(CHANNEL_ID);

      expect(queued()).toEqual(Array.from({ length: 99 }, (_, index) => 11 + index));
    });

    it('leaves out everything below the first message of a chat with no cursor', async () => {
      service.upsertChannels([{ id: CHANNEL_ID, title: 'Group', type: 'channel' }]);
      historyIds = [50, 53];
      at(53);

      await service._syncNewerMessages(CHANNEL_ID);

      expect(queued()).toEqual([51, 52]);
    });

    describe('when a fill ends below the cursor', () => {
      function openGap(channelId, gapAfter, cursorId) {
        seedAt(channelId, cursorId);
        service._setChannelGapStart(channelId, gapAfter);
      }

      function archive(channelId, id) {
        service.archiveHistoryMessages(channelId, [{ id, date: 1700000000 + id, text: 'kept' }]);
      }

      it('remembers what history did not show up to the cursor', async () => {
        openGap(CHANNEL_ID, 12, 16);
        archive(CHANNEL_ID, 16);
        historyIds = [13];
        at(16);

        await service._syncNewerMessages(CHANNEL_ID);

        expect(queued()).toEqual([14, 15]);
        expect(service._getChannel(CHANNEL_ID).gap_after_message_id).toBeNull();
      });

      it('remembers them when history shows nothing at all', async () => {
        openGap(CHANNEL_ID, 12, 15);
        archive(CHANNEL_ID, 14);
        historyIds = [];
        at(15);

        await service._syncNewerMessages(CHANNEL_ID);

        expect(queued()).toEqual([13]);
      });

      it('leaves out a cursor more than a hundred ids away', async () => {
        openGap(CHANNEL_ID, 12, 113);
        historyIds = [];
        at(113);

        await service._syncNewerMessages(CHANNEL_ID);

        expect(queued()).toEqual([]);
      });

      it('leaves out chats that share ids across the account', async () => {
        openGap('42', 12, 15);
        historyIds = [];
        at(15);

        await service._syncNewerMessages('42');

        expect(queued()).toEqual([]);
      });
    });

    it('leaves out chats that share ids across the account', async () => {
      const privateChatId = '42';
      seedAt(privateChatId, 10);
      historyIds = [11, 14];
      at(14);

      await service._syncNewerMessages(privateChatId);

      expect(queued()).toEqual([]);
    });
  });

  describe('queueResyncJob', () => {
    beforeEach(() => {
      vi.spyOn(service, 'processQueue').mockResolvedValue();
    });

    it('queues a job for a chat without one', () => {
      seedChat();

      const { job, queued } = service.queueResyncJob(CHANNEL_ID);

      expect(queued).toBe(true);
      expect(job.status).toBe('pending');
      expect(service.processQueue).toHaveBeenCalledTimes(1);
    });

    it('leaves a job alone that is already waiting', () => {
      seedChat();
      const existing = addJob();

      const { job, queued } = service.queueResyncJob(CHANNEL_ID);

      expect(queued).toBe(false);
      expect(job.id).toBe(existing.id);
    });

    it.each(['idle', 'error'])('runs a job again that is %s, keeping its depth', (status) => {
      seedChat();
      service.addJob(CHANNEL_ID, { depth: 77 });
      service.db.prepare('UPDATE jobs SET status = ? WHERE channel_id = ?').run(status, CHANNEL_ID);

      const { job, queued } = service.queueResyncJob(CHANNEL_ID);

      expect(queued).toBe(true);
      expect(job.status).toBe('pending');
      expect(job.target_message_count).toBe(77);
    });
  });

  describe('getMessagesByChannelId afterId', () => {
    it('returns the oldest messages above afterId, far from both ends of the chat', async () => {
      historyIds = range(1, 500);

      const { messages } = await telegramClient.getMessagesByChannelId(CHANNEL_ID, 3, { afterId: 300 });

      expect(sortedIds(messages)).toEqual([301, 302, 303]);
    });

    it('stops at beforeId', async () => {
      historyIds = range(1, 500);

      const { messages } = await telegramClient.getMessagesByChannelId(CHANNEL_ID, 5, {
        afterId: 300,
        beforeId: 303,
      });

      expect(sortedIds(messages)).toEqual([301, 302]);
    });

    it('returns what is left when fewer than limit messages are above afterId', async () => {
      historyIds = range(1, 500);

      const { messages } = await telegramClient.getMessagesByChannelId(CHANNEL_ID, 5, { afterId: 498 });

      expect(sortedIds(messages)).toEqual([499, 500]);
    });

    it('returns nothing above the newest message', async () => {
      historyIds = range(1, 500);

      const { messages } = await telegramClient.getMessagesByChannelId(CHANNEL_ID, 5, { afterId: 500 });

      expect(messages).toEqual([]);
    });
  });
});
