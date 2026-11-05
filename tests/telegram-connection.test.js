// Runs the server's real connect sequence through real mtcute, a real session file
// and a real SQLite archive, stubbing only the network. What goes wrong on a cold
// start depends on when mtcute dispatches missed updates relative to the moment
// tgcli subscribes, which a mocked client could not show.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { Long, MtPeerNotFoundError, tl } from '@mtcute/core';
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

function dialogWith(peer, topMessage) {
  return {
    _: 'dialog',
    peer,
    topMessage,
    readInboxMaxId: 0,
    readOutboxMaxId: 0,
    unreadCount: 0,
    unreadMentionsCount: 0,
    unreadReactionsCount: 0,
    notifySettings: { _: 'peerNotifySettings' },
    pts: 500,
  };
}

const FRIEND_PEER = { _: 'peerUser', userId: FRIEND_ID };
const CHANNEL_PEER = { _: 'peerChannel', channelId: BARE_CHANNEL_ID };

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
    this.commonDifferenceTooLong = false;
    this.channelTooLongPts = null;
    this.channelDifference = null;
    this.history = new Map();
    this.historyGate = null;
    this.historyPages = 0;
    this.maxHistoryPage = Infinity;
    this.onHistoryPage = null;
    this.dialogs = [];
    this.dialogsGate = null;
    this.failNext = new Map();
    this.archivedDialogs = [];
    this.messageReads = 0;
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
        return this._state();
      case 'updates.getDifference':
        return this._difference(request);
      case 'updates.getChannelDifference':
        if (request.channel._ !== 'inputChannel' || !request.channel.accessHash.eq(ACCESS_HASH)) {
          throw tl.RpcError.fromTl({ _: 'rpc_error', errorCode: 400, errorMessage: 'CHANNEL_INVALID' });
        }
        return this.channelDifference ?? {
          _: 'updates.channelDifferenceEmpty', final: true, pts: request.pts,
        };
      case 'messages.getDialogs':
        await this.dialogsGate?.promise;
        return {
          _: 'messages.dialogs',
          dialogs: request.offsetPeer._ === 'inputPeerEmpty' ? this._dialogsIn(request.folderId) : [],
          messages: [],
          ...PEERS,
        };
      case 'messages.getHistory':
        await this.historyGate?.promise;
        return this._historySlice(request);
      case 'channels.getMessages': {
        this.messageReads += request.id.length;
        const known = this.history.get(request.channel.channelId) ?? [];
        return {
          _: 'messages.channelMessages',
          messages: request.id.map(({ id }) => (
            known.find((message) => message.id === id) ?? { _: 'messageEmpty', id }
          )),
          count: known.length,
          ...PEERS,
        };
      }
      default:
        throw new Error(`FakeTelegram has no answer for ${request._}`);
    }
  }

  _dialogsIn(folderId) {
    return folderId === 0 ? this.dialogs : [...this.dialogs, ...this.archivedDialogs];
  }

  _state() {
    return { _: 'updates.state', pts: this.pts, qts: 0, date: 1700000000, seq: 0, unreadCount: 0 };
  }

  _difference(request) {
    if (this.commonDifferenceTooLong) {
      this.commonDifferenceTooLong = false;
      return { _: 'updates.differenceTooLong', pts: this.pts };
    }
    const pending = this.missed.filter((entry) => entry.pts > request.pts);
    if (!pending.length && !this.channelTooLongPts) {
      return { _: 'updates.differenceEmpty', date: 1700000000, seq: 0 };
    }
    const otherUpdates = this.channelTooLongPts
      ? [{ _: 'updateChannelTooLong', channelId: BARE_CHANNEL_ID, pts: this.channelTooLongPts }]
      : [];
    this.channelTooLongPts = null;
    return {
      _: 'updates.difference',
      newMessages: pending.map((entry) => entry.message),
      newEncryptedMessages: [],
      otherUpdates,
      ...PEERS,
      state: this._state(),
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
    // A page nearest the offset, which is its tail when walking towards newer messages.
    const slice = descending
      .slice(from, anchor + addOffset + limit)
      .filter((message) => message.id > minId)
      .slice(-this.maxHistoryPage);
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
  function deliverLive(update) {
    running.telegramClient.client._client.onServerUpdate.emit({
      _: 'updates',
      updates: [update],
      ...PEERS,
      date: 1700000000,
      seq: 0,
    });
  }

  function arriveLive(message) {
    telegram.pts += 1;
    deliverLive({ _: 'updateNewMessage', message, pts: telegram.pts, ptsCount: 1 });
  }

  function seedCursor(service, channelId, id) {
    service.upsertChannels([{ id: channelId, title: 'Seeded', type: 'channel' }]);
    service._updateChannelCursors(channelId, { lastMessageId: id, oldestMessageId: id });
  }

  function recordedGap(channelId) {
    return running.messageSyncService._getChannel(channelId)?.gap_after_message_id;
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

    telegram.dialogsGate.resolve();
  });

  it('archives catch-up that mtcute dispatches before login returns', async () => {
    telegram.arriveWhileOffline(friendMessage(11));

    // mtcute starts the updates loop inside client.start(), so nothing says the batch
    // waits for the connect to finish. This pins the order instead of a timing.
    const { ensureConnected, telegramClient } = startServer();
    const login = telegramClient.login.bind(telegramClient);
    let archivedWhenLoginReturned;
    telegramClient.login = async (...args) => {
      const loggedIn = await login(...args);
      await vi.waitFor(async () => expect(await storedPts()).toBe(telegram.pts));
      archivedWhenLoginReturned = archivedIds(FRIEND_CHAT_ID);
      return loggedIn;
    };
    await ensureConnected();

    expect(archivedWhenLoginReturned).toEqual([11]);
  });

  it('archives channel messages that catch-up fetches as a channel difference', async () => {
    telegram.channelTooLongPts = 50;
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

    telegram.dialogsGate.resolve();
  });

  it('leaves no gap before the next incoming message', async () => {
    telegram.arriveWhileOffline(friendMessage(11));
    telegram.arriveWhileOffline(friendMessage(12));

    const { ensureConnected, messageSyncService } = startServer();
    const reconciled = vi.spyOn(messageSyncService, 'resumePendingJobs');
    await ensureConnected();
    await vi.waitFor(() => expect(reconciled).toHaveBeenCalled());
    await vi.waitFor(async () => expect(await storedPts()).toBe(telegram.pts));

    arriveLive(friendMessage(13));

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
    const jobsResumed = vi.spyOn(messageSyncService, 'resumePendingJobs');
    await ensureConnected();
    await vi.waitFor(() => expect(jobsResumed).toHaveBeenCalled());
    expect(telegramClient.client).not.toBe(firstClient);

    arriveLive(friendMessage(11));

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

  it('adds the gap column to an archive created before it existed', async () => {
    const { messageSyncService } = startServer();
    messageSyncService.db.exec('ALTER TABLE channels DROP COLUMN gap_after_message_id');
    await stopServer();

    const reopened = startServer();
    seedCursor(reopened.messageSyncService, CHANNEL_ID, 10);
    reopened.messageSyncService._markChannelGap(CHANNEL_ID);

    expect(recordedGap(CHANNEL_ID)).toBe(10);
  });

  it('does not move the cursors on an edit, whatever the age of the message', async () => {
    const { ensureConnected, messageSyncService } = startServer();
    seedCursor(messageSyncService, FRIEND_CHAT_ID, 20);
    await ensureConnected();
    await vi.waitFor(async () => expect(await storedPts()).toBe(telegram.pts));

    telegram.pts += 1;
    deliverLive({ _: 'updateEditMessage', message: friendMessage(5), pts: telegram.pts, ptsCount: 1 });
    telegram.pts += 1;
    deliverLive({ _: 'updateEditMessage', message: friendMessage(30), pts: telegram.pts, ptsCount: 1 });

    await vi.waitFor(() => expect(archivedIds(FRIEND_CHAT_ID)).toEqual([5, 30]));
    const channel = messageSyncService._getChannel(FRIEND_CHAT_ID);
    expect(channel.last_message_id).toBe(20);
    expect(channel.oldest_message_id).toBe(20);
  });

  describe('when a channel missed more than catch-up can carry', () => {
    const channelHistory = (from, to) => {
      const messages = [];
      for (let id = from; id <= to; id += 1) {
        messages.push(channelMessage(id));
      }
      return messages;
    };

    function channelDifferenceTooLong(newest) {
      telegram.channelTooLongPts = 50;
      telegram.channelDifference = {
        _: 'updates.channelDifferenceTooLong',
        final: true,
        dialog: dialogWith(CHANNEL_PEER, newest[newest.length - 1].id),
        messages: newest,
        ...PEERS,
      };
    }

    function liveChannelMessage(id, pts) {
      const message = channelMessage(id);
      telegram.history.get(BARE_CHANNEL_ID).push(message);
      deliverLive({ _: 'updateNewChannelMessage', message, pts, ptsCount: 1 });
    }

    // 19 and 20 arrive in the diff, and 11..18 are only reachable through history.
    function startWithGapAfter10() {
      telegram.history.set(BARE_CHANNEL_ID, channelHistory(1, 20));
      channelDifferenceTooLong(channelHistory(19, 20));
      const server = startServer();
      seedCursor(server.messageSyncService, CHANNEL_ID, 10);
      return server;
    }

    const ID_11_TO_20 = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20];

    it('fills the gap between the cursor and the messages the diff carried', async () => {
      const { ensureConnected } = startWithGapAfter10();
      await ensureConnected();

      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual(ID_11_TO_20));
      await vi.waitFor(() => expect(recordedGap(CHANNEL_ID)).toBeNull());
      expect(cursor(CHANNEL_ID)).toBe(20);
    });

    it('does not let a live message hide a gap', async () => {
      telegram.historyGate = deferred();
      const { ensureConnected } = startWithGapAfter10();
      await ensureConnected();
      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual([19, 20]));

      liveChannelMessage(21, 501);
      await vi.waitFor(() => expect(cursor(CHANNEL_ID)).toBe(21));
      telegram.historyGate.resolve();

      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual([...ID_11_TO_20, 21]));
      expect(cursor(CHANNEL_ID)).toBe(21);
    });

    it('keeps the gap across a shutdown, and a live message before the refill does not hide it', async () => {
      const errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});
      const { ensureConnected } = startWithGapAfter10();
      let stopping;
      telegram.onHistoryPage = (page) => {
        if (page === 2) stopping = stopServer();
      };
      await ensureConnected();
      await vi.waitFor(() => expect(stopping).toBeDefined());
      await stopping;
      expect(errorLog).not.toHaveBeenCalled();

      // mtcute has moved past this channel, so the restart gets no second report of it.
      telegram.onHistoryPage = null;
      telegram.dialogsGate = deferred();
      const server = startServer();
      await server.ensureConnected();
      expect(recordedGap(CHANNEL_ID)).toBe(12);
      liveChannelMessage(21, 501);
      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toContain(21));
      telegram.dialogsGate.resolve();

      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual([...ID_11_TO_20, 21]));
      await vi.waitFor(() => expect(recordedGap(CHANNEL_ID)).toBeNull());
    });

    async function failFirstGapFill() {
      const errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});
      telegram.history.set(BARE_CHANNEL_ID, channelHistory(1, 20));
      telegram.dialogs = [dialogWith(CHANNEL_PEER, 20)];
      telegram.failNext.set('messages.getHistory', new Error('history unavailable'));
      const server = startServer();
      seedCursor(server.messageSyncService, CHANNEL_ID, 10);
      await server.ensureConnected();
      await vi.waitFor(() => expect(errorLog).toHaveBeenCalledTimes(1));
      await vi.waitFor(() => expect(server.messageSyncService.processing).toBe(false));
      return { ...server, errorLog };
    }

    it('does not spin on a failed gap, and says when it will try again', async () => {
      const { messageSyncService, errorLog } = await failFirstGapFill();
      expect(recordedGap(CHANNEL_ID)).toBe(10);
      expect(errorLog.mock.calls[0][0]).toContain('retrying in 60s');
      expect(messageSyncService.getQueueStats().gaps).toBe(1);
      expect(messageSyncService.gapRetryTimer).not.toBeNull();
      const pagesAfterFailure = telegram.historyPages;

      await messageSyncService.processQueue();

      expect(telegram.historyPages).toBe(pagesAfterFailure);
    });

    it('retries a failed gap while the server keeps running', async () => {
      const { messageSyncService } = await failFirstGapFill();
      messageSyncService.gapRetries.get(CHANNEL_ID).retryAt = Date.now();
      clearTimeout(messageSyncService.gapRetryTimer);
      await messageSyncService.processQueue();

      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual(ID_11_TO_20));
      await vi.waitFor(() => expect(recordedGap(CHANNEL_ID)).toBeNull());
      expect(messageSyncService.gapRetries.size).toBe(0);
      expect(messageSyncService.getQueueStats().gaps).toBe(0);
    });

    it('wakes the queue by itself when the retry is due, waiting longer after each failure', async () => {
      const { messageSyncService, errorLog } = await failFirstGapFill();
      telegram.failNext.set('messages.getHistory', new Error('still unavailable'));

      messageSyncService.gapRetries.get(CHANNEL_ID).retryAt = Date.now() + 5;
      messageSyncService._scheduleNextGapRetry();

      await vi.waitFor(() => expect(errorLog).toHaveBeenCalledTimes(2));
      expect(errorLog.mock.calls[1][0]).toContain('retrying in 120s');
      expect(messageSyncService.gapRetries.get(CHANNEL_ID).attempts).toBe(2);
    });

    it('retries a failed gap on the next start', async () => {
      await failFirstGapFill();

      await stopServer();
      const restarted = startServer();
      await restarted.ensureConnected();

      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual(ID_11_TO_20));
      await vi.waitFor(() => expect(recordedGap(CHANNEL_ID)).toBeNull());
    });

    it('retries a gap that failed before the dialog refresh once the refresh is done', async () => {
      vi.spyOn(console, 'error').mockImplementation(() => {});
      telegram.failNext.set('messages.getHistory', new Error('client is being replaced'));
      telegram.dialogsGate = deferred();
      const { ensureConnected, messageSyncService } = startWithGapAfter10();
      await ensureConnected();
      await vi.waitFor(() => expect(messageSyncService.gapRetries.size).toBe(1));

      telegram.dialogsGate.resolve();

      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual(ID_11_TO_20));
    });

    it('keeps the lower start when a second gap opens before the first is filled', async () => {
      const { messageSyncService } = startServer();
      seedCursor(messageSyncService, CHANNEL_ID, 10);

      messageSyncService._markChannelGap(CHANNEL_ID);
      messageSyncService._updateChannelCursors(CHANNEL_ID, { lastMessageId: 20 });
      messageSyncService._markChannelGap(CHANNEL_ID);

      expect(recordedGap(CHANNEL_ID)).toBe(10);
    });

    it('waits out a flood wait and carries on', async () => {
      const floodWait = tl.RpcError.fromTl({ _: 'rpc_error', errorCode: 420, errorMessage: 'FLOOD_WAIT_1' });
      telegram.failNext.set('messages.getHistory', floodWait);
      const { ensureConnected } = startWithGapAfter10();
      await ensureConnected();

      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual(ID_11_TO_20), { timeout: 3000 });
    });

    it('does not hold up a shutdown for a long flood wait', async () => {
      const floodWait = tl.RpcError.fromTl({ _: 'rpc_error', errorCode: 420, errorMessage: 'FLOOD_WAIT_3600' });
      telegram.failNext.set('messages.getHistory', floodWait);
      const { ensureConnected, messageSyncService } = startWithGapAfter10();
      const waited = vi.spyOn(messageSyncService, '_delayUnlessStopped');
      await ensureConnected();
      await vi.waitFor(() => expect(waited).toHaveBeenCalledWith(3600000));

      await stopServer();
    }, 3000);

    it('does not take a short page for the end of a gap', async () => {
      telegram.maxHistoryPage = 1;
      const { ensureConnected } = startWithGapAfter10();
      await ensureConnected();

      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual(ID_11_TO_20));
    });

    it('records no gap and fetches nothing for a chat with no cursor yet', async () => {
      telegram.history.set(BARE_CHANNEL_ID, channelHistory(1, 20));
      channelDifferenceTooLong(channelHistory(19, 20));
      const { ensureConnected, messageSyncService } = startServer();
      await ensureConnected();

      await vi.waitFor(() => expect(cursor(CHANNEL_ID)).toBe(20));
      await vi.waitFor(() => expect(messageSyncService.processing).toBe(false));
      expect(archivedIds(CHANNEL_ID)).toEqual([19, 20]);
      expect(recordedGap(CHANNEL_ID)).toBeNull();
      expect(telegram.historyPages).toBe(0);
    });

    it('resumes a stopped sync from where the archive is continuous', async () => {
      telegram.history.set(BARE_CHANNEL_ID, channelHistory(1, 16));

      const { ensureConnected, messageSyncService } = startServer();
      seedCursor(messageSyncService, CHANNEL_ID, 10);
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

  describe('when an update carries a min copy of a chat', () => {
    const MIN_HASH = Long.fromNumber(999);
    const MIN_CHANNEL = { ...CHANNEL, min: true, accessHash: MIN_HASH };
    const OTHER_MIN_CHANNEL = { ...MIN_CHANNEL, id: 600 };

    function emit(update, chats) {
      running.telegramClient.client._client.onServerUpdate.emit({
        _: 'updates',
        updates: [update],
        users: [SELF, FRIEND],
        chats,
        date: 1700000000,
        seq: 0,
      });
    }

    // The way a forward from that chat comes in: the chat among the peers of an update elsewhere.
    function deliverCopies(chats) {
      telegram.pts += 1;
      emit({ _: 'updateNewMessage', message: friendMessage(11), pts: telegram.pts, ptsCount: 1 }, chats);
    }

    async function startKnowingTheChannel() {
      telegram.dialogs = [dialogWith(CHANNEL_PEER, 10)];
      const server = startServer();
      const resumed = vi.spyOn(server.messageSyncService, 'resumePendingJobs');
      seedCursor(server.messageSyncService, CHANNEL_ID, 10);
      await server.ensureConnected();
      await vi.waitFor(() => expect(resumed).toHaveBeenCalled());
      await vi.waitFor(async () => expect(await storedPts()).toBe(telegram.pts));
      return server;
    }

    it('keeps the access hash it has, in memory and on disk', async () => {
      const { telegramClient } = await startKnowingTheChannel();

      deliverCopies([MIN_CHANNEL]);
      await vi.waitFor(() => expect(archivedIds(FRIEND_CHAT_ID)).toEqual([11]));
      const inMemory = await telegramClient.client.storage.peers.getById(Number(CHANNEL_ID));
      expect(inMemory.accessHash.eq(ACCESS_HASH)).toBe(true);

      await stopServer();
      const restarted = startServer();
      await restarted.telegramClient.client.prepare();
      const onDisk = await restarted.telegramClient.client.storage.peers.getById(Number(CHANNEL_ID));
      expect(onDisk?.accessHash.eq(ACCESS_HASH)).toBe(true);
    });

    it('keeps it after a restart too, before the chat has been seen again', async () => {
      await startKnowingTheChannel();
      await stopServer();

      const restarted = startServer();
      await restarted.telegramClient.client.prepare();
      const { peers } = restarted.telegramClient.client.storage;
      await peers.updatePeersFrom({ users: [], chats: [MIN_CHANNEL] });

      const peer = await peers.getById(Number(CHANNEL_ID));
      expect(peer?.accessHash.eq(ACCESS_HASH)).toBe(true);
    });

    it('is not undone by a delayed write of an earlier min copy', async () => {
      const { telegramClient } = startServer();
      await telegramClient.client.prepare();
      const { peers } = telegramClient.client.storage;

      await peers.updatePeersFrom({ users: [], chats: [MIN_CHANNEL] });
      await peers.updatePeersFrom({ users: [], chats: [MIN_CHANNEL] });
      await peers.updatePeersFrom({ users: [], chats: [CHANNEL] });
      await stopServer();

      const restarted = startServer();
      await restarted.telegramClient.client.prepare();
      const peer = await restarted.telegramClient.client.storage.peers.getById(Number(CHANNEL_ID));
      expect(peer?.accessHash.eq(ACCESS_HASH)).toBe(true);
    });

    it('stops answering with the hash of a min row once it has the real one', async () => {
      const { telegramClient } = startServer();
      await telegramClient.client.prepare();
      const { peers } = telegramClient.client.storage;
      await peers.updatePeersFrom({ users: [], chats: [MIN_CHANNEL] });
      // resolvePeer does this on its way to asking Telegram, and the result stays cached.
      await peers.getCompleteById(Number(CHANNEL_ID), true);

      await peers.updatePeersFrom({ users: [], chats: [CHANNEL] });
      expect((await peers.getById(Number(CHANNEL_ID))).accessHash.eq(ACCESS_HASH)).toBe(true);

      await peers.updatePeersFrom({ users: [], chats: [MIN_CHANNEL] });
      expect((await peers.getById(Number(CHANNEL_ID))).accessHash.eq(ACCESS_HASH)).toBe(true);
    });

    it('overwrites the cached min entry, since deleting one damages the LRU map', async () => {
      const { telegramClient } = startServer();
      await telegramClient.client.prepare();
      const { peers } = telegramClient.client.storage;
      await peers.updatePeersFrom({ users: [], chats: [MIN_CHANNEL] });
      await peers.getCompleteById(Number(CHANNEL_ID), true);

      await peers.updatePeersFrom({ users: [], chats: [CHANNEL] });

      expect(peers._cache.has(Number(CHANNEL_ID))).toBe(true);
      expect(peers._cache.get(Number(CHANNEL_ID)).peer.accessHash.eq(ACCESS_HASH)).toBe(true);
    });

    it('drops the message reference of a chat whose complete row it keeps', async () => {
      const { telegramClient } = await startKnowingTheChannel();
      const { peers, refMsgs } = telegramClient.client.storage;
      await refMsgs.store(Number(CHANNEL_ID), Number(CHANNEL_ID), 1);

      await peers.updatePeersFrom({ users: [], chats: [MIN_CHANNEL] });

      await expect(refMsgs.getForPeer(Number(CHANNEL_ID))).resolves.toBeNull();
    });

    it('still stores a min copy of a chat it knows nothing else about', async () => {
      const { telegramClient } = await startKnowingTheChannel();

      deliverCopies([OTHER_MIN_CHANNEL]);
      await vi.waitFor(() => expect(archivedIds(FRIEND_CHAT_ID)).toEqual([11]));

      const hash = await telegramClient.client.storage.peers.getMinAccessHash(-1000000000600);
      expect(hash.eq(MIN_HASH)).toBe(true);
    });

    it('can still fetch what that chat has missed', async () => {
      await startKnowingTheChannel();
      deliverCopies([MIN_CHANNEL]);
      await vi.waitFor(() => expect(archivedIds(FRIEND_CHAT_ID)).toEqual([11]));

      telegram.channelDifference = {
        _: 'updates.channelDifference',
        final: true,
        pts: 505,
        newMessages: [channelMessage(13), channelMessage(14)],
        otherUpdates: [],
        ...PEERS,
      };
      // Names the chat without carrying a copy of it, so mtcute has to look it up.
      emit({ _: 'updateChannelTooLong', channelId: BARE_CHANNEL_ID, pts: 500 }, []);

      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual([13, 14]));
    });
  });

  describe('when a channel id jumps over the cursor while running', () => {
    async function startAtCursor10() {
      telegram.dialogs = [];
      const server = startServer();
      const resumed = vi.spyOn(server.messageSyncService, 'resumePendingJobs');
      seedCursor(server.messageSyncService, CHANNEL_ID, 10);
      await server.ensureConnected();
      await vi.waitFor(() => expect(resumed).toHaveBeenCalled());
      await vi.waitFor(async () => expect(await storedPts()).toBe(telegram.pts));
      return server;
    }

    function history(from, to) {
      const messages = [];
      for (let id = from; id <= to; id += 1) {
        messages.push(channelMessage(id));
      }
      return messages;
    }

    function arriveInChannel(id, pts) {
      deliverLive({ _: 'updateNewChannelMessage', message: channelMessage(id), pts, ptsCount: 1 });
    }

    it('fetches what mtcute never handed over', async () => {
      telegram.history.set(BARE_CHANNEL_ID, history(1, 16));
      await startAtCursor10();

      arriveInChannel(16, 500);

      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual([11, 12, 13, 14, 15, 16]));
      await vi.waitFor(() => expect(recordedGap(CHANNEL_ID)).toBeNull());
      expect(cursor(CHANNEL_ID)).toBe(16);
    });

    it('asks Telegram nothing while ids run in sequence', async () => {
      telegram.history.set(BARE_CHANNEL_ID, history(1, 12));
      await startAtCursor10();

      arriveInChannel(11, 500);
      arriveInChannel(12, 501);

      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual([11, 12]));
      expect(telegram.historyPages).toBe(0);
      expect(cursor(CHANNEL_ID)).toBe(12);
    });

    it('leaves private chats alone, where ids are shared across the account', async () => {
      const { messageSyncService } = await startAtCursor10();
      seedCursor(messageSyncService, FRIEND_CHAT_ID, 10);

      arriveLive(friendMessage(50));

      await vi.waitFor(() => expect(archivedIds(FRIEND_CHAT_ID)).toEqual([50]));
      expect(recordedGap(FRIEND_CHAT_ID)).toBeNull();
      expect(cursor(FRIEND_CHAT_ID)).toBe(50);
      expect(telegram.historyPages).toBe(0);
    });
  });

  // As seen on a busy forum: updates arrive out of order, so mtcute asks for a channel
  // difference. Telegram answers that some of those messages were deleted and hides
  // them from history as well, and mtcute drops the copies it already received. They
  // show up in history about half a minute later, with nothing to announce them.
  describe('when a channel difference names real messages as deleted', () => {
    const RECHECK_TIMES = [30, 120, 600, 1800, 7200].map((seconds) => seconds * 1000);
    let queuedAt;

    function history(...ids) {
      return ids.map(channelMessage);
    }

    function arriveInChannel(id, pts) {
      deliverLive({ _: 'updateNewChannelMessage', message: channelMessage(id), pts, ptsCount: 1 });
    }

    async function startAtCursor(id) {
      telegram.dialogs = [];
      const server = startServer();
      const resumed = vi.spyOn(server.messageSyncService, 'resumePendingJobs');
      seedCursor(server.messageSyncService, CHANNEL_ID, id);
      await server.ensureConnected();
      await vi.waitFor(() => expect(resumed).toHaveBeenCalled());
      await vi.waitFor(async () => expect(await storedPts()).toBe(telegram.pts));
      return server;
    }

    // 13 and 14 reach mtcute, then the difference calls them deleted and history has
    // not got them, so the fill started by the jump to 15 finds nothing between.
    async function lose13And14() {
      telegram.history.set(BARE_CHANNEL_ID, history(11, 12, 15));
      const server = await startAtCursor(10);
      arriveInChannel(11, 500);
      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual([11]));

      telegram.channelDifference = {
        _: 'updates.channelDifference',
        final: true,
        pts: 504,
        newMessages: history(12, 15),
        otherUpdates: [
          { _: 'updateDeleteChannelMessages', channelId: BARE_CHANNEL_ID, messages: [13, 14], pts: 504, ptsCount: 0 },
        ],
        ...PEERS,
      };
      arriveInChannel(13, 502);
      arriveInChannel(14, 503);
      arriveInChannel(15, 504);

      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual([11, 12, 15]));
      await vi.waitFor(() => expect(recordedGap(CHANNEL_ID)).toBeNull());
      queuedAt = Date.now();
      return server;
    }

    async function recheckAt(elapsedMs) {
      const clock = vi.spyOn(Date, 'now').mockReturnValue(queuedAt + elapsedMs);
      try {
        await running.messageSyncService._recheckDueMessages();
      } finally {
        clock.mockRestore();
      }
    }

    function queuedIds() {
      return running.messageSyncService.db
        .prepare('SELECT message_id FROM messages_to_recheck ORDER BY message_id')
        .all()
        .map((row) => row.message_id);
    }

    it('archives them once Telegram shows them again, leaving the cursor alone', async () => {
      await lose13And14();
      expect(queuedIds()).toEqual([13, 14]);

      telegram.history.set(BARE_CHANNEL_ID, history(11, 12, 13, 14, 15));
      await recheckAt(RECHECK_TIMES[0]);

      expect(archivedIds(CHANNEL_ID)).toEqual([11, 12, 13, 14, 15]);
      expect(queuedIds()).toEqual([]);
      expect(cursor(CHANNEL_ID)).toBe(15);
    });

    it('keeps looking while Telegram still hides them', async () => {
      await lose13And14();

      await recheckAt(RECHECK_TIMES[0]);
      await recheckAt(RECHECK_TIMES[1]);
      expect(archivedIds(CHANNEL_ID)).toEqual([11, 12, 15]);

      telegram.history.set(BARE_CHANNEL_ID, history(11, 12, 13, 14, 15));
      await recheckAt(RECHECK_TIMES[2]);

      expect(archivedIds(CHANNEL_ID)).toEqual([11, 12, 13, 14, 15]);
    });

    it('reads only when a check is due, then lets go of a message that stays deleted', async () => {
      await lose13And14();

      await recheckAt(RECHECK_TIMES[0] - 1000);
      expect(telegram.messageReads).toBe(0);

      for (const [index, elapsed] of RECHECK_TIMES.entries()) {
        await recheckAt(elapsed);
        expect(telegram.messageReads).toBe(2 * (index + 1));
        await recheckAt(elapsed + 1000);
        expect(telegram.messageReads).toBe(2 * (index + 1));
      }

      expect(queuedIds()).toEqual([]);
      expect(archivedIds(CHANNEL_ID)).toEqual([11, 12, 15]);
      await recheckAt(RECHECK_TIMES.at(-1) * 10);
      expect(telegram.messageReads).toBe(2 * RECHECK_TIMES.length);
    });

    it('reads once for a schedule that passed during downtime, then waits for what is left', async () => {
      await lose13And14();

      await recheckAt(RECHECK_TIMES[2] + 1000);
      expect(telegram.messageReads).toBe(2);

      await recheckAt(RECHECK_TIMES[2] + 2000);
      expect(telegram.messageReads).toBe(2);

      await recheckAt(RECHECK_TIMES[3]);
      expect(telegram.messageReads).toBe(4);
    });

    it('lets go after one read when the whole schedule passed during downtime', async () => {
      await lose13And14();

      await recheckAt(RECHECK_TIMES.at(-1) + 1000);

      expect(telegram.messageReads).toBe(2);
      expect(queuedIds()).toEqual([]);
    });

    it('survives a restart', async () => {
      await lose13And14();
      await stopServer();

      telegram.history.set(BARE_CHANNEL_ID, history(11, 12, 13, 14, 15));
      startServer();
      await running.ensureConnected();
      await recheckAt(RECHECK_TIMES[0]);

      expect(archivedIds(CHANNEL_ID)).toEqual([11, 12, 13, 14, 15]);
    });

    it('restores a row that a false delete removed', async () => {
      telegram.history.set(BARE_CHANNEL_ID, history(11));
      await startAtCursor(10);
      arriveInChannel(11, 500);
      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual([11]));

      deliverLive({ _: 'updateDeleteChannelMessages', channelId: BARE_CHANNEL_ID, messages: [11], pts: 501, ptsCount: 1 });
      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual([]));
      queuedAt = Date.now();
      await recheckAt(RECHECK_TIMES[0]);

      expect(archivedIds(CHANNEL_ID)).toEqual([11]);
    });

    it('keeps a really deleted message out of the archive', async () => {
      telegram.history.set(BARE_CHANNEL_ID, []);
      await startAtCursor(10);
      arriveInChannel(11, 500);
      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual([11]));

      deliverLive({ _: 'updateDeleteChannelMessages', channelId: BARE_CHANNEL_ID, messages: [11], pts: 501, ptsCount: 1 });
      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual([]));
      queuedAt = Date.now();
      for (const elapsed of RECHECK_TIMES) {
        await recheckAt(elapsed);
      }

      expect(archivedIds(CHANNEL_ID)).toEqual([]);
      expect(queuedIds()).toEqual([]);
    });

    it('does not check deletes of messages far below the cursor', async () => {
      await startAtCursor(500);

      deliverLive({
        _: 'updateDeleteChannelMessages',
        channelId: BARE_CHANNEL_ID,
        messages: [50, 400, 401],
        pts: 500,
        ptsCount: 1,
      });

      await vi.waitFor(() => expect(queuedIds()).toEqual([401]));
    });

    it('does not check deletes in a chat with no cursor yet', async () => {
      const { messageSyncService } = await startAtCursor(10);
      messageSyncService.db
        .prepare('UPDATE channels SET last_message_id = NULL WHERE channel_id = ?')
        .run(CHANNEL_ID);

      messageSyncService._handleDeleteMessage({ channelId: CHANNEL_ID, messageIds: [5] });

      expect(queuedIds()).toEqual([]);
    });

    it('does not check deletes in a chat with sync off, and forgets one turned off later', async () => {
      await lose13And14();
      running.messageSyncService.db
        .prepare('UPDATE channels SET sync_enabled = 0 WHERE channel_id = ?')
        .run(CHANNEL_ID);

      telegram.history.set(BARE_CHANNEL_ID, history(11, 12, 13, 14, 15));
      await recheckAt(RECHECK_TIMES[0]);

      expect(telegram.messageReads).toBe(0);
      expect(queuedIds()).toEqual([]);
      expect(archivedIds(CHANNEL_ID)).toEqual([11, 12, 15]);

      deliverLive({ _: 'updateDeleteChannelMessages', channelId: BARE_CHANNEL_ID, messages: [12], pts: 505, ptsCount: 1 });
      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual([11, 15]));
      expect(queuedIds()).toEqual([]);
    });

    it('spends no attempt while Telegram is out of reach', async () => {
      await lose13And14();
      running.messageSyncService.disconnectedAt = Date.now();

      await recheckAt(RECHECK_TIMES[0]);
      expect(telegram.messageReads).toBe(0);

      running.messageSyncService.disconnectedAt = null;
      telegram.history.set(BARE_CHANNEL_ID, history(11, 12, 13, 14, 15));
      await recheckAt(RECHECK_TIMES[0]);

      expect(archivedIds(CHANNEL_ID)).toEqual([11, 12, 13, 14, 15]);
    });

    it('takes back nothing deleted during a read, and starts the schedule again', async () => {
      await lose13And14();
      telegram.history.set(BARE_CHANNEL_ID, history(11, 12, 13, 14, 15));
      const { messageSyncService, telegramClient } = running;
      const read = telegramClient.getMessageById.bind(telegramClient);
      vi.spyOn(telegramClient, 'getMessageById').mockImplementation(async (channelId, messageId) => {
        const message = await read(channelId, messageId);
        if (messageId === 13) {
          messageSyncService._handleDeleteMessage({ channelId: CHANNEL_ID, messageIds: [13] });
        }
        return message;
      });

      await recheckAt(RECHECK_TIMES[0]);

      expect(archivedIds(CHANNEL_ID)).toEqual([11, 12, 14, 15]);
      expect(queuedIds()).toEqual([13]);
      expect(messageSyncService.db.prepare('SELECT attempts FROM messages_to_recheck').get().attempts).toBe(0);
    });

    it('takes back nothing deleted during a read after the cursor moved far away', async () => {
      await lose13And14();
      telegram.history.set(BARE_CHANNEL_ID, history(11, 12, 13, 14, 15));
      const { messageSyncService, telegramClient } = running;
      messageSyncService._updateChannelCursors(CHANNEL_ID, { lastMessageId: 500 });
      const read = telegramClient.getMessageById.bind(telegramClient);
      vi.spyOn(telegramClient, 'getMessageById').mockImplementation(async (channelId, messageId) => {
        const message = await read(channelId, messageId);
        if (messageId === 13) {
          messageSyncService._handleDeleteMessage({ channelId: CHANNEL_ID, messageIds: [13] });
        }
        return message;
      });

      await recheckAt(RECHECK_TIMES[0]);

      expect(archivedIds(CHANNEL_ID)).toEqual([11, 12, 14, 15]);
      expect(queuedIds()).toEqual([13]);
    });

    it('gives a message named again the full schedule', async () => {
      await lose13And14();
      await recheckAt(RECHECK_TIMES[0]);
      await recheckAt(RECHECK_TIMES[1]);
      running.messageSyncService._handleDeleteMessage({ channelId: CHANNEL_ID, messageIds: [13, 14] });
      queuedAt = Date.now();
      telegram.messageReads = 0;

      for (const elapsed of RECHECK_TIMES) {
        await recheckAt(elapsed);
      }

      expect(telegram.messageReads).toBe(2 * RECHECK_TIMES.length);
    });

    it('spends no attempt on a rate limit, and stops reading until the next check', async () => {
      vi.spyOn(console, 'error').mockImplementation(() => {});
      await lose13And14();
      telegram.failNext.set(
        'channels.getMessages',
        tl.RpcError.fromTl({ _: 'rpc_error', errorCode: 420, errorMessage: 'FLOOD_WAIT_100' }),
      );

      await recheckAt(RECHECK_TIMES[0]);
      expect(telegram.messageReads).toBe(0);
      expect(queuedIds()).toEqual([13, 14]);

      telegram.history.set(BARE_CHANNEL_ID, history(11, 12, 13, 14, 15));
      await recheckAt(RECHECK_TIMES[0]);

      expect(archivedIds(CHANNEL_ID)).toEqual([11, 12, 13, 14, 15]);
    });

    it.each([
      ['an expired session', new Error('Not logged in')],
      ['a 401', tl.RpcError.fromTl({ _: 'rpc_error', errorCode: 401, errorMessage: 'AUTH_KEY_UNREGISTERED' })],
      ['a failure on the side of Telegram', tl.RpcError.fromTl({ _: 'rpc_error', errorCode: 500, errorMessage: 'INTERNAL' })],
    ])('spends no attempt on %s', async (_, failure) => {
      vi.spyOn(console, 'error').mockImplementation(() => {});
      await lose13And14();
      const read = vi.spyOn(running.telegramClient, 'getMessageById').mockRejectedValue(failure);

      for (const elapsed of RECHECK_TIMES) {
        await recheckAt(elapsed);
      }

      expect(queuedIds()).toEqual([13, 14]);
      read.mockRestore();
      telegram.history.set(BARE_CHANNEL_ID, history(11, 12, 13, 14, 15));
      await recheckAt(RECHECK_TIMES[0]);

      expect(archivedIds(CHANNEL_ID)).toEqual([11, 12, 13, 14, 15]);
    });

    it('does not let a read that keeps failing hold up the next message', async () => {
      vi.spyOn(console, 'error').mockImplementation(() => {});
      await lose13And14();
      telegram.history.set(BARE_CHANNEL_ID, history(11, 12, 13, 14, 15));
      const { telegramClient } = running;
      const read = telegramClient.getMessageById.bind(telegramClient);
      vi.spyOn(telegramClient, 'getMessageById').mockImplementation(async (channelId, messageId) => {
        if (messageId === 13) {
          throw new TypeError('broken');
        }
        return read(channelId, messageId);
      });

      await recheckAt(RECHECK_TIMES[0]);

      expect(archivedIds(CHANNEL_ID)).toEqual([11, 12, 14, 15]);
      expect(queuedIds()).toEqual([13]);
    });

    it('gives up on a read that keeps failing for a day of running', async () => {
      vi.spyOn(console, 'error').mockImplementation(() => {});
      await lose13And14();
      vi.spyOn(running.telegramClient, 'getMessageById').mockRejectedValue(new Error('broken'));
      const day = 24 * 60 * 60 * 1000;

      await recheckAt(day - 60 * 1000);
      expect(queuedIds()).toEqual([13, 14]);

      await recheckAt(day);
      expect(queuedIds()).toEqual([]);
    });

    it('counts that day from the start of the server, not from the queueing', async () => {
      vi.spyOn(console, 'error').mockImplementation(() => {});
      await lose13And14();
      const day = 24 * 60 * 60 * 1000;
      queuedAt -= 10 * day;
      running.messageSyncService.db.prepare('UPDATE messages_to_recheck SET queued_at = ?').run(queuedAt);
      vi.spyOn(running.telegramClient, 'getMessageById').mockRejectedValue(new Error('broken'));

      await recheckAt(10 * day);

      expect(queuedIds()).toEqual([13, 14]);
    });

    it.each([
      [400, 'CHANNEL_PRIVATE'],
      [403, 'CHAT_ADMIN_REQUIRED'],
      [406, 'CHANNEL_PRIVATE'],
    ])('spends one attempt when Telegram refuses the chat with %i, and goes on to the next message', async (errorCode, errorMessage) => {
      vi.spyOn(console, 'error').mockImplementation(() => {});
      await lose13And14();
      telegram.failNext.set(
        'channels.getMessages',
        tl.RpcError.fromTl({ _: 'rpc_error', errorCode, errorMessage }),
      );
      telegram.history.set(BARE_CHANNEL_ID, history(11, 12, 13, 14, 15));

      await recheckAt(RECHECK_TIMES[0]);

      expect(archivedIds(CHANNEL_ID)).toEqual([11, 12, 14, 15]);
      expect(queuedIds()).toEqual([13]);

      await recheckAt(RECHECK_TIMES[0]);
      expect(queuedIds()).toEqual([13]);

      await recheckAt(RECHECK_TIMES[1]);
      expect(archivedIds(CHANNEL_ID)).toEqual([11, 12, 13, 14, 15]);
    });

    it('lets go of a message in a chat it can no longer resolve', async () => {
      vi.spyOn(console, 'error').mockImplementation(() => {});
      await lose13And14();
      const { telegramClient } = running;
      vi.spyOn(telegramClient, 'getMessageById').mockRejectedValue(new MtPeerNotFoundError('not cached'));

      for (const elapsed of RECHECK_TIMES) {
        await recheckAt(elapsed);
      }

      expect(queuedIds()).toEqual([]);
    });

    it('writes nothing when the server stops during a read', async () => {
      await lose13And14();
      telegram.history.set(BARE_CHANNEL_ID, history(11, 12, 13, 14, 15));
      const { messageSyncService, telegramClient } = running;
      const read = telegramClient.getMessageById.bind(telegramClient);
      vi.spyOn(telegramClient, 'getMessageById').mockImplementation(async (...args) => {
        const message = await read(...args);
        messageSyncService.stopRequested = true;
        return message;
      });

      await recheckAt(RECHECK_TIMES[0]);

      expect(archivedIds(CHANNEL_ID)).toEqual([11, 12, 15]);
      expect(queuedIds()).toEqual([13, 14]);
      messageSyncService.stopRequested = false;
    });

    it('is part of the periodic checks', async () => {
      const { messageSyncService } = await startAtCursor(10);
      const recheck = vi.spyOn(messageSyncService, '_recheckDueMessages').mockResolvedValue();
      clearInterval(messageSyncService.periodicCheckTimer);
      vi.useFakeTimers();
      try {
        messageSyncService._startPeriodicChecks();
        await vi.advanceTimersByTimeAsync(30 * 1000);
        expect(recheck).toHaveBeenCalledTimes(1);
      } finally {
        clearInterval(messageSyncService.periodicCheckTimer);
        vi.useRealTimers();
      }
    });

    it('never runs two passes at once', async () => {
      await lose13And14();
      const clock = vi.spyOn(Date, 'now').mockReturnValue(queuedAt + RECHECK_TIMES[0]);
      await Promise.all([
        running.messageSyncService._recheckDueMessages(),
        running.messageSyncService._recheckDueMessages(),
      ]);
      clock.mockRestore();

      expect(telegram.messageReads).toBe(2);
    });
  });

  describe('when catch-up reports nothing about what a chat missed', () => {
    function friendHistory(from, to) {
      const messages = [];
      for (let id = from; id <= to; id += 1) {
        messages.push(friendMessage(id));
      }
      return messages;
    }

    it('finds a gap left by updates.differenceTooLong through the dialog list', async () => {
      // mtcute moves the pts past these and hands over nothing.
      telegram.pts += 5;
      telegram.commonDifferenceTooLong = true;
      telegram.history.set(FRIEND_ID, friendHistory(1, 15));
      telegram.dialogs = [dialogWith(FRIEND_PEER, 15)];

      const { ensureConnected, messageSyncService } = startServer();
      seedCursor(messageSyncService, FRIEND_CHAT_ID, 10);
      await ensureConnected();

      await vi.waitFor(() => expect(archivedIds(FRIEND_CHAT_ID)).toEqual([11, 12, 13, 14, 15]));
      await vi.waitFor(() => expect(recordedGap(FRIEND_CHAT_ID)).toBeNull());
      expect(cursor(FRIEND_CHAT_ID)).toBe(15);
    });

    it('is not fooled by a message that arrives before the dialog list does', async () => {
      telegram.pts += 5;
      telegram.commonDifferenceTooLong = true;
      telegram.history.set(FRIEND_ID, friendHistory(1, 16));
      telegram.dialogs = [dialogWith(FRIEND_PEER, 16)];
      telegram.dialogsGate = deferred();

      const { ensureConnected, messageSyncService } = startServer();
      seedCursor(messageSyncService, FRIEND_CHAT_ID, 10);
      await ensureConnected();
      await vi.waitFor(async () => expect(await storedPts()).toBe(telegram.pts));

      arriveLive(friendMessage(16));
      await vi.waitFor(() => expect(archivedIds(FRIEND_CHAT_ID)).toEqual([16]));
      expect(cursor(FRIEND_CHAT_ID)).toBe(10);
      telegram.dialogsGate.resolve();

      await vi.waitFor(() => expect(archivedIds(FRIEND_CHAT_ID)).toEqual([11, 12, 13, 14, 15, 16]));
      await vi.waitFor(() => expect(recordedGap(FRIEND_CHAT_ID)).toBeNull());
      expect(cursor(FRIEND_CHAT_ID)).toBe(16);
    });

    it('keeps trying when the dialog list fails, and starts the queue after that', async () => {
      const errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});
      telegram.pts += 5;
      telegram.commonDifferenceTooLong = true;
      telegram.history.set(FRIEND_ID, friendHistory(1, 15));
      telegram.dialogs = [dialogWith(FRIEND_PEER, 15)];
      telegram.failNext.set('messages.getDialogs', new Error('dialogs unavailable'));

      const { ensureConnected, messageSyncService } = startServer();
      vi.spyOn(messageSyncService, '_delayUnlessStopped').mockResolvedValue();
      seedCursor(messageSyncService, FRIEND_CHAT_ID, 10);
      await ensureConnected();

      await vi.waitFor(() => expect(archivedIds(FRIEND_CHAT_ID)).toEqual([11, 12, 13, 14, 15]));
      await vi.waitFor(() => expect(recordedGap(FRIEND_CHAT_ID)).toBeNull());
      expect(errorLog).toHaveBeenCalledTimes(1);
    });

    it('looks again after a long disconnect, not after a short one', async () => {
      telegram.history.set(FRIEND_ID, friendHistory(1, 10));
      telegram.dialogs = [dialogWith(FRIEND_PEER, 10)];
      const { ensureConnected, messageSyncService, telegramClient } = startServer();
      const resumed = vi.spyOn(messageSyncService, 'resumePendingJobs');
      seedCursor(messageSyncService, FRIEND_CHAT_ID, 10);
      await ensureConnected();
      await vi.waitFor(() => expect(resumed).toHaveBeenCalled());
      await vi.waitFor(async () => expect(await storedPts()).toBe(telegram.pts));
      const listDialogs = vi.spyOn(telegramClient, 'listDialogs');

      messageSyncService._handleConnectionState('connecting');
      messageSyncService._handleConnectionState('connected');
      await vi.waitFor(() => expect(messageSyncService.disconnectedAt).toBeNull());
      expect(listDialogs).not.toHaveBeenCalled();

      messageSyncService._handleConnectionState('offline');
      messageSyncService.disconnectedAt -= 6 * 60 * 1000;
      telegram.history.set(FRIEND_ID, friendHistory(1, 16));
      telegram.dialogs = [dialogWith(FRIEND_PEER, 16)];
      messageSyncService._handleConnectionState('updating');
      arriveLive(friendMessage(16));

      await vi.waitFor(() => expect(archivedIds(FRIEND_CHAT_ID)).toEqual([11, 12, 13, 14, 15, 16]));
      await vi.waitFor(() => expect(recordedGap(FRIEND_CHAT_ID)).toBeNull());
      expect(listDialogs).toHaveBeenCalledTimes(1);
      expect(cursor(FRIEND_CHAT_ID)).toBe(16);
      expect(resumed).toHaveBeenCalledTimes(2);
    });

    it('does not take an open socket for the end of an outage', async () => {
      telegram.history.set(FRIEND_ID, friendHistory(1, 10));
      telegram.dialogs = [dialogWith(FRIEND_PEER, 10)];
      const { ensureConnected, messageSyncService } = startServer();
      const resumed = vi.spyOn(messageSyncService, 'resumePendingJobs');
      seedCursor(messageSyncService, FRIEND_CHAT_ID, 10);
      await ensureConnected();
      await vi.waitFor(() => expect(resumed).toHaveBeenCalled());
      await vi.waitFor(async () => expect(await storedPts()).toBe(telegram.pts));

      // A proxy accepts the connection every couple of minutes, and Telegram stays silent.
      const confirmed = vi.spyOn(messageSyncService, '_confirmConnection');
      telegram.failNext.set('updates.getState', new Error('no answer'));
      messageSyncService._handleConnectionState('connecting');
      const outageStart = messageSyncService.disconnectedAt;
      messageSyncService._handleConnectionState('connected');
      await confirmed.mock.results[0].value;
      expect(messageSyncService.disconnectedAt).toBe(outageStart);

      messageSyncService._handleConnectionState('connecting');
      expect(messageSyncService.disconnectedAt).toBe(outageStart);
      arriveLive(friendMessage(16));
      await vi.waitFor(() => expect(archivedIds(FRIEND_CHAT_ID)).toEqual([16]));
      expect(cursor(FRIEND_CHAT_ID)).toBe(10);

      messageSyncService.disconnectedAt -= 6 * 60 * 1000;
      telegram.history.set(FRIEND_ID, friendHistory(1, 16));
      telegram.dialogs = [dialogWith(FRIEND_PEER, 16)];
      messageSyncService._handleConnectionState('connected');

      await vi.waitFor(() => expect(archivedIds(FRIEND_CHAT_ID)).toEqual([11, 12, 13, 14, 15, 16]));
      await vi.waitFor(() => expect(recordedGap(FRIEND_CHAT_ID)).toBeNull());
      expect(cursor(FRIEND_CHAT_ID)).toBe(16);
    });

    it('asks Telegram nothing before the first comparison, which may be before login', () => {
      const { messageSyncService, telegramClient } = startServer();
      const call = vi.spyOn(telegramClient.client, 'call');
      messageSyncService.startRealtimeSync();

      messageSyncService._handleConnectionState('connecting');
      messageSyncService._handleConnectionState('connected');

      expect(call).not.toHaveBeenCalled();
      expect(messageSyncService.disconnectedAt).toBeNull();
      messageSyncService.close();
    });

    it('does not end an outage on an answer to a connection that has dropped since', async () => {
      const { ensureConnected, messageSyncService } = startServer();
      const resumed = vi.spyOn(messageSyncService, 'resumePendingJobs');
      await ensureConnected();
      await vi.waitFor(() => expect(resumed).toHaveBeenCalled());
      messageSyncService._handleConnectionState('connecting');
      const outageStart = messageSyncService.disconnectedAt;

      const confirming = messageSyncService._confirmConnection();
      messageSyncService._handleConnectionState('offline');
      await confirming;

      expect(messageSyncService.disconnectedAt).toBe(outageStart);
    });

    it('finds a gap in an archived chat, which the dialog list leaves out by default', async () => {
      telegram.pts += 5;
      telegram.commonDifferenceTooLong = true;
      telegram.history.set(FRIEND_ID, friendHistory(1, 15));
      telegram.archivedDialogs = [dialogWith(FRIEND_PEER, 15)];

      const { ensureConnected, messageSyncService, telegramClient } = startServer();
      seedCursor(messageSyncService, FRIEND_CHAT_ID, 10);
      await ensureConnected();

      await vi.waitFor(() => expect(archivedIds(FRIEND_CHAT_ID)).toEqual([11, 12, 13, 14, 15]));
      await vi.waitFor(() => expect(recordedGap(FRIEND_CHAT_ID)).toBeNull());
      await expect(telegramClient.listDialogs(0)).resolves.toEqual([]);
    });

    it('treats an overdue timer as an outage, since a sleeping machine reports none', () => {
      vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval', 'Date'] });
      try {
        const { messageSyncService } = startServer();
        const reconcile = vi.spyOn(messageSyncService, '_reconcileAfterOutage').mockImplementation(() => {});
        const confirm = vi.spyOn(messageSyncService, '_confirmConnection').mockResolvedValue();
        const retryJobs = vi.spyOn(messageSyncService, '_retryErroredJobsWhenDue').mockImplementation(() => {});
        const compare = vi.spyOn(messageSyncService, '_reconcileWhenDue').mockImplementation(() => {});
        messageSyncService.startRealtimeSync();

        vi.advanceTimersByTime(30 * 1000);
        expect(reconcile).not.toHaveBeenCalled();
        expect(confirm).toHaveBeenCalledTimes(1);
        expect(retryJobs).toHaveBeenCalledTimes(1);
        expect(compare).toHaveBeenCalledTimes(1);

        vi.setSystemTime(Date.now() + 6 * 60 * 1000);
        vi.advanceTimersByTime(30 * 1000);
        vi.advanceTimersByTime(30 * 1000);
        expect(reconcile).toHaveBeenCalledTimes(1);
        messageSyncService.close();
      } finally {
        vi.useRealTimers();
      }
    });

    it('logs the chats it found behind, leaving out those that are not synced', () => {
      const log = vi.spyOn(console, 'log').mockImplementation(() => {});
      const { messageSyncService } = startServer();
      seedCursor(messageSyncService, CHANNEL_ID, 10);
      seedCursor(messageSyncService, FRIEND_CHAT_ID, 10);
      messageSyncService.setChannelSync(CHANNEL_ID, false);

      messageSyncService._markGapsBehindDialogs([
        { id: FRIEND_CHAT_ID, topMessageId: 15 },
        { id: CHANNEL_ID, topMessageId: 15 },
      ]);

      expect(log).toHaveBeenCalledWith(`[sync] 1 of 2 dialogs are ahead of the archive: ${FRIEND_CHAT_ID}.`);
    });

    it('finds a channel Telegram never named through the dialog list', async () => {
      telegram.history.set(BARE_CHANNEL_ID, [channelMessage(11), channelMessage(12)]);
      telegram.dialogs = [dialogWith(CHANNEL_PEER, 12)];

      const { ensureConnected, messageSyncService } = startServer();
      seedCursor(messageSyncService, CHANNEL_ID, 10);
      await ensureConnected();

      await vi.waitFor(() => expect(archivedIds(CHANNEL_ID)).toEqual([11, 12]));
    });

    it('fetches nothing for chats that are current, have no cursor, or are not synced', async () => {
      telegram.history.set(FRIEND_ID, friendHistory(1, 15));
      telegram.history.set(BARE_CHANNEL_ID, [channelMessage(11), channelMessage(12)]);
      telegram.dialogs = [dialogWith(FRIEND_PEER, 15), dialogWith(CHANNEL_PEER, 12)];

      const first = startServer();
      seedCursor(first.messageSyncService, FRIEND_CHAT_ID, 15);
      first.messageSyncService.upsertChannels([{ id: CHANNEL_ID, title: 'Group', type: 'channel' }]);
      const jobsResumed = vi.spyOn(first.messageSyncService, 'resumePendingJobs');
      await first.ensureConnected();
      await vi.waitFor(() => expect(jobsResumed).toHaveBeenCalled());
      await vi.waitFor(() => expect(first.messageSyncService.processing).toBe(false));
      expect(telegram.historyPages).toBe(0);
      await stopServer();

      const second = startServer();
      seedCursor(second.messageSyncService, CHANNEL_ID, 10);
      second.messageSyncService.setChannelSync(CHANNEL_ID, false);
      const resumed = vi.spyOn(second.messageSyncService, 'resumePendingJobs');
      await second.ensureConnected();
      await vi.waitFor(() => expect(resumed).toHaveBeenCalled());
      await vi.waitFor(() => expect(second.messageSyncService.processing).toBe(false));

      expect(telegram.historyPages).toBe(0);
      expect(recordedGap(CHANNEL_ID)).toBeNull();
    });
  });
});
