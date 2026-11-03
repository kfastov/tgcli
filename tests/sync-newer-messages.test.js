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
