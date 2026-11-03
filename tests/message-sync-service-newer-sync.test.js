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

import { OPERATIONS } from '../core/operations.js';
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
    if (service?.db?.open) {
      service.db.close();
    }
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
      const result = await OPERATIONS.channelSetSync(
        { messageSyncService: service },
        { chat: CHANNEL_ID, enable: true },
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
      await OPERATIONS.channelSetSync(
        { messageSyncService: service },
        { chat: CHANNEL_ID, enable: true },
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
