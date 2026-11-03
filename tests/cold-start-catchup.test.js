// Runs the server's real connect sequence through real mtcute, a real session file
// and a real SQLite archive, stubbing only the network. What goes wrong on a cold
// start depends on when mtcute dispatches missed updates relative to the moment
// tgcli subscribes, which a mocked client could not show.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { Long } from '@mtcute/core';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { createTelegramConnector } from '../core/telegram-connection.js';
import MessageSyncService from '../message-sync-service.js';
import TelegramClient from '../telegram-client.js';

const SELF_ID = 1;
const FRIEND_ID = 2;
const BARE_CHANNEL_ID = 500;
const CHANNEL_ID = '-1000000000500';
const FRIEND_CHAT_ID = String(FRIEND_ID);

const ACCESS_HASH = Long.fromNumber(1);
const SELF = { _: 'user', id: SELF_ID, firstName: 'Me', accessHash: ACCESS_HASH, self: true };
const FRIEND = { _: 'user', id: FRIEND_ID, firstName: 'Friend', accessHash: ACCESS_HASH };
const CHANNEL = {
  _: 'channel',
  id: BARE_CHANNEL_ID,
  title: 'Group',
  accessHash: ACCESS_HASH,
  megagroup: true,
  photo: { _: 'chatPhotoEmpty' },
  date: 1700000000,
};
const PEERS = { users: [SELF, FRIEND], chats: [CHANNEL] };

const STORED_PTS = 100;

function friendMessage(id) {
  return {
    _: 'message',
    id,
    date: 1700000000 + id,
    peerId: { _: 'peerUser', userId: FRIEND_ID },
    fromId: { _: 'peerUser', userId: FRIEND_ID },
    message: `friend ${id}`,
  };
}

function channelMessage(id) {
  return {
    _: 'message',
    id,
    date: 1700000000 + id,
    peerId: { _: 'peerChannel', channelId: BARE_CHANNEL_ID },
    fromId: { _: 'peerUser', userId: FRIEND_ID },
    message: `channel ${id}`,
  };
}

function deferred() {
  let resolve;
  const promise = new Promise((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

// The account as Telegram sees it. Answers the RPCs a cold start makes and keeps
// what arrived while the server was off, so a catch-up that was consumed once stays
// consumed. Anything unexpected fails loudly instead of returning a plausible empty.
class FakeTelegram {
  constructor() {
    this.pts = STORED_PTS;
    this.missed = [];
    this.channelDifference = null;
    this.history = new Map();
    this.dialogsGate = null;
    this.failNext = new Map();
    this.historyPages = 0;
    this.onHistoryPage = null;
  }

  arriveWhileOffline(message) {
    this.pts += 1;
    this.missed.push({ pts: this.pts, message });
  }

  async respond(request) {
    const failure = this.failNext.get(request._);
    if (failure) {
      this.failNext.delete(request._);
      throw failure;
    }
    switch (request._) {
      case 'users.getUsers':
        return [SELF];
      case 'updates.getState':
        return { _: 'updates.state', pts: this.pts, qts: 0, date: 1700000000, seq: 0, unreadCount: 0 };
      case 'updates.getDifference':
        return this._difference(request);
      case 'updates.getChannelDifference':
        return this.channelDifference ?? {
          _: 'updates.channelDifferenceEmpty', final: true, pts: request.pts,
        };
      case 'messages.getPinnedDialogs':
        await this.dialogsGate?.promise;
        return { _: 'messages.peerDialogs', dialogs: [], messages: [], ...PEERS, state: {} };
      case 'messages.getDialogs':
        await this.dialogsGate?.promise;
        return { _: 'messages.dialogs', dialogs: [], messages: [], ...PEERS };
      case 'messages.getHistory':
        return this._historySlice(request);
      default:
        throw new Error(`FakeTelegram has no answer for ${request._}`);
    }
  }

  _difference(request) {
    const pending = this.missed.filter((entry) => entry.pts > request.pts);
    const state = { _: 'updates.state', pts: this.pts, qts: 0, date: 1700000000, seq: 0, unreadCount: 0 };
    if (!pending.length && !this.channelTooLong) {
      return { _: 'updates.differenceEmpty', date: 1700000000, seq: 0 };
    }
    const otherUpdates = this.channelTooLong
      ? [{ _: 'updateChannelTooLong', channelId: BARE_CHANNEL_ID, pts: this.channelTooLong }]
      : [];
    this.channelTooLong = null;
    return {
      _: 'updates.difference',
      newMessages: pending.map((entry) => entry.message),
      newEncryptedMessages: [],
      otherUpdates,
      ...PEERS,
      state,
    };
  }

  // messages.getHistory for a known chat: the slice starts at offsetId, addOffset
  // moves it towards newer messages when negative, and minId bounds it from below.
  _historySlice({ peer, offsetId, addOffset, limit, minId }) {
    this.historyPages += 1;
    this.onHistoryPage?.(this.historyPages);
    const messages = this.history.get(peer.channelId ?? peer.userId) ?? [];
    const descending = [...messages].sort((a, b) => b.id - a.id);
    const start = offsetId
      ? descending.findIndex((message) => message.id < offsetId)
      : 0;
    const anchor = start === -1 ? descending.length : start;
    const from = Math.max(0, anchor + addOffset);
    const slice = descending
      .slice(from, anchor + addOffset + limit)
      .filter((message) => message.id > minId);
    return { _: 'messages.channelMessages', messages: slice, count: messages.length, ...PEERS };
  }
}

// The repo's real client wrapper, with only the mtcute network layer replaced. Set
// through _createClient so a client recreated during login recovery is offline too.
function createOfflineClient(telegram, sessionPath) {
  class OfflineTelegramClient extends TelegramClient {
    _createClient() {
      const client = super._createClient();
      const { mt } = client._client;
      mt.connect = async () => {};
      mt.call = (request) => telegram.respond(request);
      return client;
    }
  }
  return new OfflineTelegramClient(12345, 'hash', '+10000000000', sessionPath, {});
}

describe('cold start catch-up', () => {
  let storeDir;
  let telegram;
  let sessionPath;
  let running;

  async function storeUpdatesState(pts) {
    const client = createOfflineClient(telegram, sessionPath);
    await client.client.prepare();
    const { updates } = client.client.storage;
    await updates.setPts(pts);
    await updates.setQts(0);
    await updates.setDate(1700000000);
    await updates.setSeq(0);
    await client.client._client.mt.storage.save();
    await client.client.destroy();
  }

  async function storedPts() {
    const state = await running.telegramClient.client.storage.updates.getState();
    return state?.[0];
  }

  function startServer() {
    const telegramClient = createOfflineClient(telegram, sessionPath);
    const messageSyncService = new MessageSyncService(telegramClient, {
      dbPath: path.join(storeDir, 'messages.db'),
      batchSize: 2,
      interJobDelayMs: 1,
      interBatchDelayMs: 1,
    });
    const ensureConnected = createTelegramConnector({ telegramClient, messageSyncService });
    running = { telegramClient, messageSyncService, ensureConnected };
    return running;
  }

  async function stopServer() {
    if (!running) return;
    const { telegramClient, messageSyncService } = running;
    running = null;
    await messageSyncService.shutdown();
    await telegramClient.destroy();
  }

  function archivedIds(channelId) {
    return running.messageSyncService.db
      .prepare('SELECT message_id FROM messages WHERE channel_id = ? ORDER BY message_id')
      .all(channelId)
      .map((row) => row.message_id);
  }

  function cursor(channelId) {
    return running.messageSyncService.db
      .prepare('SELECT last_message_id FROM channels WHERE channel_id = ?')
      .get(channelId)?.last_message_id;
  }

  // Delivered while the server runs, the way the connection hands updates to mtcute.
  function arriveLive(message, pts) {
    telegram.pts = pts;
    running.telegramClient.client._client.onServerUpdate.emit({
      _: 'updates',
      updates: [{ _: 'updateNewMessage', message, pts, ptsCount: 1 }],
      ...PEERS,
      date: 1700000000,
      seq: 0,
    });
  }

  beforeEach(async () => {
    vi.spyOn(console, 'log').mockImplementation(() => {});
    storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-cold-start-catchup-'));
    sessionPath = path.join(storeDir, 'session.db');
    telegram = new FakeTelegram();
    await storeUpdatesState(STORED_PTS);
  });

  afterEach(async () => {
    await stopServer();
    fs.rmSync(storeDir, { recursive: true, force: true });
    vi.restoreAllMocks();
  });

  it('archives messages missed while off, even while the dialog refresh is still running', async () => {
    telegram.arriveWhileOffline(friendMessage(11));
    telegram.arriveWhileOffline(friendMessage(12));
    // A large account: the dialog refresh is still paging after catch-up is done.
    telegram.dialogsGate = deferred();

    const { ensureConnected } = startServer();
    await ensureConnected();

    // mtcute persists the new pts once it has dispatched the batch, and from then
    // on it will never ask Telegram for these updates again.
    await vi.waitFor(async () => expect(await storedPts()).toBe(telegram.pts));

    expect(archivedIds(FRIEND_CHAT_ID)).toEqual([11, 12]);
    expect(cursor(FRIEND_CHAT_ID)).toBe(12);

    telegram.dialogsGate.resolve();
  });

  it('archives channel messages that catch-up fetches as a channel difference', async () => {
    telegram.channelTooLong = 50;
    telegram.channelDifference = {
      _: 'updates.channelDifference',
      final: true,
      pts: 52,
      newMessages: [channelMessage(21), channelMessage(22)],
      otherUpdates: [],
      ...PEERS,
    };
    telegram.dialogsGate = deferred();

    const { ensureConnected } = startServer();
    await ensureConnected();

    await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual([21, 22]));
    expect(cursor(CHANNEL_ID)).toBe(22);

    telegram.dialogsGate.resolve();
  });

  it('leaves no gap before the next incoming message', async () => {
    telegram.arriveWhileOffline(friendMessage(11));
    telegram.arriveWhileOffline(friendMessage(12));

    const { ensureConnected } = startServer();
    await ensureConnected();
    await vi.waitFor(async () => expect(await storedPts()).toBe(telegram.pts));

    arriveLive(friendMessage(13), telegram.pts + 1);

    await vi.waitFor(() => expect(archivedIds(FRIEND_CHAT_ID)).toEqual([11, 12, 13]));
    expect(cursor(FRIEND_CHAT_ID)).toBe(13);
  });

  it('does not duplicate messages when catch-up delivers them again', async () => {
    telegram.arriveWhileOffline(friendMessage(11));
    telegram.arriveWhileOffline(friendMessage(12));

    let server = startServer();
    await server.ensureConnected();
    await vi.waitFor(async () => expect(await storedPts()).toBe(telegram.pts));
    expect(archivedIds(FRIEND_CHAT_ID)).toEqual([11, 12]);
    await stopServer();

    // An unclean exit leaves an older pts behind, so mtcute replays the same batch.
    await storeUpdatesState(STORED_PTS);
    telegram.arriveWhileOffline(friendMessage(13));

    server = startServer();
    await server.ensureConnected();
    await vi.waitFor(async () => expect(await storedPts()).toBe(telegram.pts));

    expect(archivedIds(FRIEND_CHAT_ID)).toEqual([11, 12, 13]);
    expect(cursor(FRIEND_CHAT_ID)).toBe(13);
  });

  it('keeps archiving when login recovery replaces the mtcute client', async () => {
    telegram.arriveWhileOffline(friendMessage(11));
    telegram.failNext.set('users.getUsers', new Error('Session is reset'));

    const { ensureConnected, telegramClient } = startServer();
    const firstClient = telegramClient.client;
    await ensureConnected();

    expect(telegramClient.client).not.toBe(firstClient);
    await vi.waitFor(() => expect(archivedIds(FRIEND_CHAT_ID)).toEqual([11]));
  });

  it('keeps archiving when the dialog refresh replaces the mtcute client', async () => {
    telegram.failNext.set('messages.getDialogs', new Error('Session is reset'));

    const { ensureConnected, messageSyncService, telegramClient } = startServer();
    const firstClient = telegramClient.client;
    const seeded = vi.spyOn(messageSyncService, 'resumePendingJobs');
    await ensureConnected();
    await vi.waitFor(() => expect(seeded).toHaveBeenCalled());
    expect(telegramClient.client).not.toBe(firstClient);

    arriveLive(friendMessage(11), telegram.pts + 1);

    await vi.waitFor(() => expect(archivedIds(FRIEND_CHAT_ID)).toEqual([11]));
  });

  it('keeps archiving when the dialog refresh fails', async () => {
    const errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});
    telegram.arriveWhileOffline(friendMessage(11));
    telegram.failNext.set('messages.getDialogs', new Error('dialogs unavailable'));

    const { ensureConnected } = startServer();
    await ensureConnected();

    await vi.waitFor(() => expect(errorLog).toHaveBeenCalled());
    await vi.waitFor(() => expect(archivedIds(FRIEND_CHAT_ID)).toEqual([11]));
  });

  it('subscribes once when a failed connect is retried', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    telegram.arriveWhileOffline(friendMessage(11));
    telegram.failNext.set('users.getUsers', new Error('network down'));

    const { ensureConnected, messageSyncService, telegramClient } = startServer();
    await expect(ensureConnected()).rejects.toThrow();
    const handle = vi.spyOn(messageSyncService, '_handleIncomingMessage');
    await ensureConnected();

    await vi.waitFor(() => expect(archivedIds(FRIEND_CHAT_ID)).toEqual([11]));
    expect(handle).toHaveBeenCalledTimes(1);
    expect(telegramClient.client.onNewMessage.length).toBe(1);
  });

  it('removes its handlers from the current client on close', async () => {
    telegram.failNext.set('users.getUsers', new Error('Session is reset'));

    const { ensureConnected, messageSyncService, telegramClient } = startServer();
    await ensureConnected();
    messageSyncService.close();

    expect(telegramClient.client.onNewMessage.length).toBe(0);
    expect(telegramClient.client.onEditMessage.length).toBe(0);
    expect(telegramClient.client.onDeleteMessage.length).toBe(0);
    expect(telegramClient.updateEmitter.listenerCount('clientReplaced')).toBe(0);
  });

  describe('when a channel missed more than catch-up can carry', () => {
    const channelHistory = (from, to) => {
      const messages = [];
      for (let id = from; id <= to; id += 1) {
        messages.push(channelMessage(id));
      }
      return messages;
    };

    function channelTooLong(newest) {
      telegram.channelTooLong = 50;
      telegram.channelDifference = {
        _: 'updates.channelDifferenceTooLong',
        final: true,
        dialog: {
          _: 'dialog',
          peer: { _: 'peerChannel', channelId: BARE_CHANNEL_ID },
          topMessage: newest[newest.length - 1].id,
          readInboxMaxId: 0,
          readOutboxMaxId: 0,
          unreadCount: 0,
          unreadMentionsCount: 0,
          unreadReactionsCount: 0,
          notifySettings: { _: 'peerNotifySettings' },
          pts: 500,
        },
        messages: newest,
        ...PEERS,
      };
    }

    function seedCursor(service, id) {
      service.upsertChannels([{ id: CHANNEL_ID, title: 'Group', type: 'channel' }]);
      service._updateChannelCursors(CHANNEL_ID, { lastMessageId: id, oldestMessageId: id });
    }

    it('fills the gap between the cursor and the messages the diff carried', async () => {
      telegram.history.set(BARE_CHANNEL_ID, channelHistory(1, 20));
      channelTooLong(channelHistory(19, 20));

      const { ensureConnected, messageSyncService } = startServer();
      seedCursor(messageSyncService, 10);
      await ensureConnected();

      await vi.waitFor(() => expect(cursor(CHANNEL_ID)).toBe(20));
      expect(archivedIds(CHANNEL_ID)).toEqual([11, 12, 13, 14, 15, 16, 17, 18, 19, 20]);
    });

    it('keeps the cursor before the gap when fetching it fails, so a later sync can retry', async () => {
      const errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});
      telegram.history.set(BARE_CHANNEL_ID, channelHistory(1, 20));
      channelTooLong(channelHistory(19, 20));
      telegram.failNext.set('messages.getHistory', new Error('history unavailable'));

      const { ensureConnected, messageSyncService } = startServer();
      seedCursor(messageSyncService, 10);
      await ensureConnected();
      await vi.waitFor(() => expect(errorLog).toHaveBeenCalled());

      expect(archivedIds(CHANNEL_ID)).toEqual([19, 20]);
      expect(cursor(CHANNEL_ID)).toBe(10);

      await messageSyncService._syncNewerMessages(CHANNEL_ID);

      expect(archivedIds(CHANNEL_ID)).toEqual([11, 12, 13, 14, 15, 16, 17, 18, 19, 20]);
      expect(cursor(CHANNEL_ID)).toBe(20);
    });

    it('does not let a live message hide a gap that is waiting for its turn', async () => {
      telegram.history.set(BARE_CHANNEL_ID, channelHistory(1, 20));
      channelTooLong(channelHistory(19, 20));

      const { ensureConnected, messageSyncService, telegramClient } = startServer();
      seedCursor(messageSyncService, 10);
      const earlierGap = deferred();
      messageSyncService.channelGapSyncs = earlierGap.promise;
      await ensureConnected();
      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual([19, 20]));

      const update = channelHistory(21, 21)[0];
      telegram.history.get(BARE_CHANNEL_ID).push(update);
      telegramClient.client._client.onServerUpdate.emit({
        _: 'updates',
        updates: [{ _: 'updateNewChannelMessage', message: update, pts: 501, ptsCount: 1 }],
        ...PEERS,
        date: 1700000000,
        seq: 0,
      });
      await vi.waitFor(() => expect(cursor(CHANNEL_ID)).toBe(21));

      earlierGap.resolve();

      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual(
        [11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21],
      ));
      expect(cursor(CHANNEL_ID)).toBe(21);
    });

    it('resumes a stopped sync from where the archive is continuous', async () => {
      telegram.history.set(BARE_CHANNEL_ID, channelHistory(1, 16));

      const { ensureConnected, messageSyncService } = startServer();
      seedCursor(messageSyncService, 10);
      await ensureConnected();
      await messageSyncService.refreshChannelsFromDialogs();
      telegram.onHistoryPage = (page) => {
        if (page === 2) messageSyncService.stopRequested = true;
      };

      const stopped = await messageSyncService._syncNewerMessages(CHANNEL_ID);

      expect(stopped.stoppedEarly).toBe(true);
      expect(cursor(CHANNEL_ID)).toBe(12);

      messageSyncService.stopRequested = false;
      telegram.onHistoryPage = null;
      await messageSyncService._syncNewerMessages(CHANNEL_ID);

      expect(archivedIds(CHANNEL_ID)).toEqual([11, 12, 13, 14, 15, 16]);
      expect(cursor(CHANNEL_ID)).toBe(16);
    });

    it('takes only the newest batch for a chat with nothing archived yet', async () => {
      telegram.history.set(BARE_CHANNEL_ID, channelHistory(1, 20));

      const { ensureConnected, messageSyncService } = startServer();
      messageSyncService.upsertChannels([{ id: CHANNEL_ID, title: 'Group', type: 'channel' }]);
      await ensureConnected();
      await messageSyncService.refreshChannelsFromDialogs();

      await messageSyncService._syncNewerMessages(CHANNEL_ID);

      expect(archivedIds(CHANNEL_ID)).toEqual([19, 20]);
      expect(cursor(CHANNEL_ID)).toBe(20);
    });
  });
});
