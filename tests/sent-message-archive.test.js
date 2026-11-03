// Runs the send operations through real mtcute, a real updates manager and a real
// SQLite archive, stubbing only the network: what goes wrong here depends on how
// mtcute reports a send back to the client, which a mocked client.sendText could
// not show.
import { EventEmitter } from 'node:events';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { InputMedia, Long, Message, PeersIndex, UpdatesManager } from '@mtcute/core';
import { resolvePeer, sendMedia, sendText } from '@mtcute/core/methods.js';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { printSendOutcome } from '../cli.js';
import { OPERATIONS } from '../core/operations.js';
import MessageSyncService from '../message-sync-service.js';
import TelegramClient from '../telegram-client.js';

const EXISTING_FILE = fileURLToPath(new URL('../package.json', import.meta.url));

const SELF_ID = 1;
const FRIEND_ID = 2;
const BARE_CHANNEL_ID = 500;
const CHANNEL_ID = '-1000000000500';
const FRIEND_CHAT_ID = String(FRIEND_ID);
const START_PTS = 100;

const ACCESS_HASH = Long.fromNumber(1);
const users = [
  { _: 'user', id: SELF_ID, firstName: 'Me', accessHash: ACCESS_HASH, self: true },
  { _: 'user', id: FRIEND_ID, firstName: 'Friend', accessHash: ACCESS_HASH },
];
const chats = [
  { _: 'channel', id: BARE_CHANNEL_ID, title: 'Group', accessHash: ACCESS_HASH, megagroup: true, forum: true },
];

const PHOTO_MEDIA = {
  _: 'messageMediaPhoto',
  photo: {
    _: 'photo',
    id: Long.fromNumber(5),
    accessHash: ACCESS_HASH,
    fileReference: new Uint8Array(0),
    date: 1,
    sizes: [{ _: 'photoSize', type: 'x', w: 800, h: 600, size: 1000 }],
    dcId: 2,
  },
};

function rawChannelMessage(id, overrides = {}) {
  return {
    _: 'message',
    id,
    date: 1700000000 + id,
    peerId: { _: 'peerChannel', channelId: BARE_CHANNEL_ID },
    fromId: { _: 'peerUser', userId: FRIEND_ID },
    message: `message ${id}`,
    ...overrides,
  };
}

function ownChannelMessage(id, overrides = {}) {
  return rawChannelMessage(id, { out: true, fromId: { _: 'peerUser', userId: SELF_ID }, ...overrides });
}

function rawFriendMessage(id, overrides = {}) {
  return rawChannelMessage(id, { peerId: { _: 'peerUser', userId: FRIEND_ID }, ...overrides });
}

function ownFriendMessage(id, overrides = {}) {
  return rawFriendMessage(id, { out: true, fromId: { _: 'peerUser', userId: SELF_ID }, ...overrides });
}

function parsed(raw) {
  return new Message(raw, PeersIndex.from({ users, chats }));
}

function updatesWith(update) {
  return { _: 'updates', updates: [update], users, chats, date: 0, seq: 0 };
}

function sentToChannel(raw) {
  return updatesWith({ _: 'updateNewChannelMessage', message: raw, pts: 1, ptsCount: 1 });
}

function sentToFriend(raw, pts = START_PTS + 1) {
  return updatesWith({ _: 'updateNewMessage', message: raw, pts, ptsCount: 1 });
}

function scheduledIn(raw) {
  return updatesWith({ _: 'updateNewScheduledMessage', message: raw });
}

function createEvent() {
  const handlers = new Set();
  return {
    add: (handler) => handlers.add(handler),
    remove: (handler) => handlers.delete(handler),
    emit: (value) => handlers.forEach((handler) => handler(value)),
  };
}

async function createHarness({ respond, dbPath }) {
  const call = vi.fn(async (request) => {
    if (request._ === 'updates.getState') {
      return { _: 'updates.state', pts: START_PTS, qts: 0, date: 0, seq: 0, unreadCount: 0 };
    }
    if (request._ === 'updates.getDifference') {
      return { _: 'updates.differenceEmpty', date: 0, seq: 0 };
    }
    return respond(request);
  });
  const logger = { warn: vi.fn(), debug: vi.fn(), verbose: vi.fn(), error: vi.fn() };
  logger.create = () => logger;
  const raw = {
    params: {},
    call,
    log: logger,
    onRawUpdate: createEvent(),
    onError: createEvent(),
    isConnected: true,
    destroyed: false,
    timers: { cancel: vi.fn() },
    getMtprotoMessageId: vi.fn(),
    mt: {
      storage: { save: async () => {} },
      network: { getPoolSize: () => 1, config: { getCached: () => null, setData: () => {} } },
    },
    storage: {
      self: { getCached: () => ({ userId: SELF_ID }) },
      peers: {
        getById: async (id) => (id === FRIEND_ID
          ? { _: 'inputPeerUser', userId: FRIEND_ID, accessHash: ACCESS_HASH }
          : { _: 'inputPeerChannel', channelId: BARE_CHANNEL_ID, accessHash: ACCESS_HASH }),
        getCompleteById: async (id) => users.find((user) => user.id === id) ?? null,
        updatePeersFrom: async () => {},
      },
      updates: {
        getState: async () => [START_PTS, 0, 0, 0],
        setPts: async () => {},
        setQts: async () => {},
        setDate: async () => {},
        setSeq: async () => {},
        getChannelPts: async () => null,
        setManyChannelPts: async () => {},
      },
      refMsgs: { store: async () => {}, delete: async () => {} },
    },
  };
  const manager = new UpdatesManager(raw, {});
  raw.handleClientUpdate = (update, noDispatch) => manager.handleClientUpdate(update, noDispatch);

  const onNewMessage = createEvent();
  raw.onRawUpdate.add(({ update, peers }) => {
    if (update._ === 'updateNewMessage' || update._ === 'updateNewChannelMessage') {
      onNewMessage.emit(new Message(update.message, peers));
    }
  });

  const telegramClient = Object.create(TelegramClient.prototype);
  telegramClient.ensureLogin = vi.fn().mockResolvedValue(undefined);
  telegramClient.updateEmitter = new EventEmitter();
  telegramClient.client = {
    sendText: (peer, text, params) => sendText(raw, peer, text, params),
    // Uploading is out of scope: a dice needs no upload and takes the same params.
    sendMedia: (peer, _media, params) => sendMedia(raw, peer, InputMedia.dice('🎲', {}), params),
    resolvePeer: (peer) => resolvePeer(raw, peer),
    _normalizeInputMedia: async () => ({ _: 'inputMediaDice', emoticon: '🎲' }),
    call,
    handleClientUpdate: raw.handleClientUpdate,
    getMessages: vi.fn().mockRejectedValue(new Error('getMessages is not available')),
    getScheduledMessages: vi.fn().mockRejectedValue(new Error('getScheduledMessages is not available')),
    onNewMessage,
    onEditMessage: createEvent(),
    onDeleteMessage: createEvent(),
  };
  const messageSyncService = new MessageSyncService(telegramClient, {
    dbPath,
    batchSize: 100,
    interJobDelayMs: 0,
    interBatchDelayMs: 0,
  });
  messageSyncService.startRealtimeSync();

  await manager.prepare();
  await manager.startLoop();

  return {
    call,
    manager,
    telegramClient,
    messageSyncService,
    services: { telegramClient, messageSyncService },
    sendRequests: () => call.mock.calls.map(([request]) => request).filter((request) => request._.startsWith('messages.send')),
  };
}

describe('archiving messages sent by tgcli', () => {
  let storeDir;
  let harness;

  const start = async (respond) => {
    harness = await createHarness({ respond, dbPath: path.join(storeDir, 'messages.db') });
    return harness;
  };

  const seedChat = (channelId, { syncEnabled = 1, lastMessageId = 10 } = {}) => {
    harness.messageSyncService.db.prepare(`
      INSERT INTO channels (channel_id, peer_title, sync_enabled, last_message_id)
      VALUES (?, 'Seeded', ?, ?)
    `).run(channelId, syncEnabled, lastMessageId);
  };

  const archived = (channelId) => harness.messageSyncService.db.prepare(`
    SELECT message_id, topic_id, text FROM messages WHERE channel_id = ? ORDER BY message_id
  `).all(channelId);

  const archivedIds = (channelId) => archived(channelId).map((row) => row.message_id);

  const archivedRaw = (channelId, messageId) => JSON.parse(harness.messageSyncService.db.prepare(`
    SELECT raw_json FROM messages WHERE channel_id = ? AND message_id = ?
  `).get(channelId, messageId).raw_json).raw;

  const mediaType = (channelId, messageId) => harness.messageSyncService.db.prepare(`
    SELECT media_type FROM message_media WHERE channel_id = ? AND message_id = ?
  `).get(channelId, messageId)?.media_type;

  const linkUrls = (channelId, messageId) => harness.messageSyncService.db.prepare(`
    SELECT url FROM message_links WHERE channel_id = ? AND message_id = ?
  `).all(channelId, messageId).map((row) => row.url);

  const refetchQueueSize = () => harness.messageSyncService.db.prepare(`
    SELECT COUNT(*) AS count FROM sent_messages_to_refetch
  `).get().count;

  const cursor = (channelId) => harness.messageSyncService.db.prepare(`
    SELECT last_message_id FROM channels WHERE channel_id = ?
  `).get(channelId).last_message_id;

  const syncEnabled = (channelId) => harness.messageSyncService.db.prepare(`
    SELECT sync_enabled FROM channels WHERE channel_id = ?
  `).get(channelId).sync_enabled;

  const liveHistory = (...messages) => {
    harness.telegramClient.getMessagesByChannelId = vi.fn(async (_id, _limit, { afterId: minId }) => ({
      peerTitle: 'Group',
      peerType: 'chat',
      messages: messages
        .filter((message) => message.id > minId)
        .map((message) => harness.telegramClient._serializeMessage(message, message.chat)),
    }));
  };

  beforeEach(() => {
    storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-sent-archive-test-'));
    vi.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    harness?.manager?.stopLoop();
    if (harness?.messageSyncService?.db?.open) {
      harness.messageSyncService.db.close();
    }
    fs.rmSync(storeDir, { recursive: true, force: true });
    vi.restoreAllMocks();
  });

  it('keeps a sent text message after the next incoming message moves the cursor', async () => {
    const { services, telegramClient, messageSyncService } = await start(() => sentToChannel(ownChannelMessage(11, { message: 'mine' })));
    seedChat(CHANNEL_ID);

    const { result } = await OPERATIONS.sendText(services, { chat: CHANNEL_ID, text: 'mine' });
    expect(result).toEqual({ messageId: 11 });
    expect(telegramClient.client.getMessages).not.toHaveBeenCalled();

    messageSyncService._handleIncomingMessage(parsed(rawChannelMessage(12)), { isEdit: false });
    liveHistory(parsed(ownChannelMessage(11)), parsed(rawChannelMessage(12)));
    await messageSyncService._syncNewerMessages(CHANNEL_ID);

    expect(cursor(CHANNEL_ID)).toBe(12);
    expect(archivedIds(CHANNEL_ID)).toEqual([11, 12]);
    expect(archived(CHANNEL_ID)[0].text).toBe('mine');
  });

  describe.each([
    ['text', (services) => OPERATIONS.sendText(services, { chat: FRIEND_CHAT_ID, text: 'mine' })],
    ['file', (services) => OPERATIONS.sendFile(services, { chat: FRIEND_CHAT_ID, file: EXISTING_FILE })],
    ['photo', (services) => OPERATIONS.sendPhoto(services, { chat: FRIEND_CHAT_ID, photo: EXISTING_FILE })],
  ])('a sent %s', (kind, send) => {
    it('is kept out of onNewMessage by noDispatch, while a later incoming message still arrives', async () => {
      const media = kind === 'text' ? undefined : PHOTO_MEDIA;
      const { services, manager, messageSyncService } = await start(() => sentToFriend(ownFriendMessage(101, { media })));
      seedChat(FRIEND_CHAT_ID);
      vi.spyOn(console, 'error').mockImplementation(() => {});
      const realtime = vi.spyOn(messageSyncService, '_handleIncomingMessage');

      await send(services);
      await vi.waitFor(() => expect(manager.pts).toBe(START_PTS + 1));

      expect(realtime).not.toHaveBeenCalled();
      expect(archivedIds(FRIEND_CHAT_ID)).toEqual([101]);

      manager.handleUpdate(sentToFriend(rawFriendMessage(102), START_PTS + 2));
      await vi.waitFor(() => expect(archivedIds(FRIEND_CHAT_ID)).toEqual([101, 102]));

      expect(realtime).toHaveBeenCalledTimes(1);
    });
  });

  it('archives a message sent to Saved Messages', async () => {
    const { services } = await start(() => updatesWith({
      _: 'updateNewMessage',
      message: {
        _: 'message',
        id: 25,
        date: 1700000025,
        out: true,
        peerId: { _: 'peerUser', userId: SELF_ID },
        fromId: { _: 'peerUser', userId: SELF_ID },
        message: 'note to self',
      },
      pts: START_PTS + 1,
      ptsCount: 1,
    }));
    seedChat(String(SELF_ID));

    await OPERATIONS.sendText(services, { chat: 'me', text: 'note to self' });

    expect(archived(String(SELF_ID))).toEqual([expect.objectContaining({ message_id: 25, text: 'note to self' })]);
  });

  describe('a text message answered with updateShortSentMessage', () => {
    const shortResponse = (id = 21, pts = START_PTS + 1) => ({
      _: 'updateShortSentMessage',
      id,
      pts,
      ptsCount: 1,
      date: 1700000000 + id,
      out: true,
    });
    const fullCopy = () => parsed(ownFriendMessage(21, {
      message: 'see https://example.com',
      entities: [{ _: 'messageEntityUrl', offset: 4, length: 19 }],
      replyTo: { _: 'messageReplyHeader', replyToMsgId: 20 },
      media: PHOTO_MEDIA,
    }));

    it('is archived at once and then completed from Telegram', async () => {
      const { services, telegramClient } = await start(() => shortResponse());
      seedChat(FRIEND_CHAT_ID);
      telegramClient.client.getMessages = vi.fn().mockResolvedValue([fullCopy()]);

      const { result } = await OPERATIONS.sendText(services, {
        chat: FRIEND_CHAT_ID,
        text: 'see https://example.com',
        replyToMessageId: 20,
      });

      expect(result).toEqual({ messageId: 21 });
      await vi.waitFor(() => expect(refetchQueueSize()).toBe(0));
      expect(telegramClient.client.getMessages).toHaveBeenCalledWith(FRIEND_ID, 21);
      expect(archivedIds(FRIEND_CHAT_ID)).toEqual([21]);
      expect(archivedRaw(FRIEND_CHAT_ID, 21).replyTo.replyToMsgId).toBe(20);
      expect(mediaType(FRIEND_CHAT_ID, 21)).toBe('photo');
      expect(linkUrls(FRIEND_CHAT_ID, 21)).toEqual(['https://example.com']);
    });

    it('stays queued when completing it fails, and is completed on the next server start', async () => {
      const { services, telegramClient, messageSyncService } = await start(() => shortResponse());
      seedChat(FRIEND_CHAT_ID);
      const errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});
      telegramClient.client.getMessages = vi.fn().mockRejectedValueOnce(new Error('network down'));

      const { result } = await OPERATIONS.sendText(services, { chat: FRIEND_CHAT_ID, text: 'hello' });

      expect(result).toEqual({ messageId: 21 });
      await vi.waitFor(() => expect(errorLog).toHaveBeenCalledWith(expect.stringContaining('network down')));
      expect(archived(FRIEND_CHAT_ID)).toEqual([expect.objectContaining({ message_id: 21, text: 'hello' })]);
      expect(archivedRaw(FRIEND_CHAT_ID, 21).replyTo).toBeUndefined();
      expect(refetchQueueSize()).toBe(1);

      telegramClient.client.getMessages = vi.fn().mockResolvedValue([fullCopy()]);
      messageSyncService.resumePendingJobs();

      await vi.waitFor(() => expect(refetchQueueSize()).toBe(0));
      expect(archivedRaw(FRIEND_CHAT_ID, 21).replyTo.replyToMsgId).toBe(20);
    });

    it('adds no requests to later sends while it stays queued', async () => {
      const responses = [shortResponse(21, START_PTS + 1), shortResponse(22, START_PTS + 2)];
      const { services, telegramClient } = await start(() => responses.shift());
      seedChat(FRIEND_CHAT_ID);
      vi.spyOn(console, 'error').mockImplementation(() => {});
      telegramClient.client.getMessages = vi.fn().mockRejectedValue(new Error('network down'));

      await OPERATIONS.sendText(services, { chat: FRIEND_CHAT_ID, text: 'first' });
      await vi.waitFor(() => expect(telegramClient.client.getMessages).toHaveBeenCalledTimes(1));
      await OPERATIONS.sendText(services, { chat: FRIEND_CHAT_ID, text: 'second' });
      await vi.waitFor(() => expect(telegramClient.client.getMessages).toHaveBeenCalledTimes(2));

      expect(telegramClient.client.getMessages.mock.calls).toEqual([[FRIEND_ID, 21], [FRIEND_ID, 22]]);
      expect(refetchQueueSize()).toBe(2);
    });

    it('survives a closed database when the queue is retried', async () => {
      const { messageSyncService } = await start(() => shortResponse());
      const errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});
      messageSyncService.db.close();

      await messageSyncService.refetchQueuedSentMessages();

      expect(errorLog).toHaveBeenCalled();
    });

    it('is no longer queued once Telegram says it is gone', async () => {
      const { services, telegramClient } = await start(() => shortResponse());
      seedChat(FRIEND_CHAT_ID);
      telegramClient.client.getMessages = vi.fn().mockResolvedValue([null]);

      await OPERATIONS.sendText(services, { chat: FRIEND_CHAT_ID, text: 'hello' });

      await vi.waitFor(() => expect(refetchQueueSize()).toBe(0));
      expect(archivedIds(FRIEND_CHAT_ID)).toEqual([21]);
    });
  });

  // A text response in full to a user cannot be told apart from a short one, so that
  // case does get one extra request.
  it('does not ask Telegram again for a message with media', async () => {
    const { services, telegramClient } = await start(() => sentToFriend(ownFriendMessage(26, { media: PHOTO_MEDIA })));
    seedChat(FRIEND_CHAT_ID);

    await OPERATIONS.sendFile(services, { chat: FRIEND_CHAT_ID, file: EXISTING_FILE });

    expect(telegramClient.client.getMessages).not.toHaveBeenCalled();
    expect(refetchQueueSize()).toBe(0);
  });

  it('archives a sent file with its media and links', async () => {
    const { services, telegramClient } = await start(() => sentToChannel(ownChannelMessage(31, {
      message: 'see https://example.com',
      entities: [{ _: 'messageEntityUrl', offset: 4, length: 19 }],
      media: PHOTO_MEDIA,
    })));
    seedChat(CHANNEL_ID);

    await OPERATIONS.sendFile(services, { chat: CHANNEL_ID, file: EXISTING_FILE, caption: 'see https://example.com' });

    expect(archived(CHANNEL_ID)).toEqual([expect.objectContaining({ message_id: 31, text: 'see https://example.com' })]);
    expect(mediaType(CHANNEL_ID, 31)).toBe('photo');
    expect(linkUrls(CHANNEL_ID, 31)).toEqual(['https://example.com']);
    expect(telegramClient.client.getMessages).not.toHaveBeenCalled();
  });

  it('archives a sent photo from the send response, even if the follow-up lookup fails', async () => {
    const { services, sendRequests } = await start(() => sentToChannel(ownChannelMessage(41, {
      message: 'photo caption',
      media: PHOTO_MEDIA,
    })));
    seedChat(CHANNEL_ID);
    vi.spyOn(console, 'error').mockImplementation(() => {});

    const { result } = await OPERATIONS.sendPhoto(services, { chat: CHANNEL_ID, photo: EXISTING_FILE, caption: 'photo caption' });

    expect(sendRequests()).toHaveLength(1);
    expect(result.warning).toMatch(/enrichment failed/i);
    expect(result.warning).not.toMatch(/archive/i);
    expect(archived(CHANNEL_ID)).toEqual([expect.objectContaining({ message_id: 41, text: 'photo caption' })]);
    expect(mediaType(CHANNEL_ID, 41)).toBe('photo');
  });

  it('records the forum topic of a sent message', async () => {
    const { services, sendRequests } = await start(() => sentToChannel(ownChannelMessage(51, {
      replyTo: { _: 'messageReplyHeader', forumTopic: true, replyToMsgId: 77 },
    })));
    seedChat(CHANNEL_ID);

    await OPERATIONS.sendText(services, { chat: CHANNEL_ID, text: 'in topic', topicId: 77 });

    expect(sendRequests()[0].replyTo.replyToMsgId).toBe(77);
    expect(archived(CHANNEL_ID)).toEqual([expect.objectContaining({ message_id: 51, topic_id: 77 })]);
  });

  it('does not archive into a chat with sync disabled, and leaves it disabled', async () => {
    const { services } = await start(() => sentToChannel(ownChannelMessage(61)));
    seedChat(CHANNEL_ID, { syncEnabled: 0 });

    const { result } = await OPERATIONS.sendText(services, { chat: CHANNEL_ID, text: 'hi' });

    expect(result).toEqual({ messageId: 61 });
    expect(archivedIds(CHANNEL_ID)).toEqual([]);
    expect(syncEnabled(CHANNEL_ID)).toBe(0);
  });

  it('does not duplicate a sent message that sync or realtime see again', async () => {
    const { services, messageSyncService } = await start(() => sentToChannel(ownChannelMessage(71)));
    seedChat(CHANNEL_ID);

    await OPERATIONS.sendText(services, { chat: CHANNEL_ID, text: 'once' });

    const sameMessage = parsed(ownChannelMessage(71));
    messageSyncService._handleIncomingMessage(sameMessage, { isEdit: false });
    liveHistory(sameMessage);
    await messageSyncService._syncNewerMessages(CHANNEL_ID);

    expect(archivedIds(CHANNEL_ID)).toEqual([71]);
  });

  it('leaves the cursor alone, so messages received while the server was down still sync', async () => {
    const { services, messageSyncService } = await start(() => sentToChannel(ownChannelMessage(82)));
    seedChat(CHANNEL_ID, { lastMessageId: 80 });

    await OPERATIONS.sendText(services, { chat: CHANNEL_ID, text: 'mine' });
    expect(cursor(CHANNEL_ID)).toBe(80);

    liveHistory(parsed(rawChannelMessage(81)), parsed(ownChannelMessage(82)));
    await messageSyncService._syncNewerMessages(CHANNEL_ID);

    expect(archivedIds(CHANNEL_ID)).toEqual([81, 82]);
    expect(cursor(CHANNEL_ID)).toBe(82);
  });

  describe('scheduled messages', () => {
    const SCHEDULED_ID = 900;
    const inOneHour = () => Math.floor(Date.now() / 1000) + 3600;

    const sends = {
      text: (services, scheduleDate) => OPERATIONS.sendText(services, { chat: CHANNEL_ID, text: 'later', scheduleDate }),
      file: (services, scheduleDate) => OPERATIONS.sendFile(services, { chat: CHANNEL_ID, file: EXISTING_FILE, scheduleDate }),
      photo: (services, scheduleDate) => OPERATIONS.sendPhoto(services, { chat: CHANNEL_ID, photo: EXISTING_FILE, scheduleDate }),
    };

    // Telegram schedules only what the request asks to schedule, so a send that lost
    // its date on the way to the network comes back as a normal message here.
    const respond = (request) => (request.scheduleDate
      ? scheduledIn(ownChannelMessage(SCHEDULED_ID, { media: PHOTO_MEDIA }))
      : sentToChannel(ownChannelMessage(11, { media: PHOTO_MEDIA })));

    it.each(Object.keys(sends))('%s is archived only once it is delivered', async (kind) => {
      const { services, telegramClient, messageSyncService, sendRequests } = await start(respond);
      seedChat(CHANNEL_ID);
      telegramClient.client.getScheduledMessages = vi.fn().mockResolvedValue([parsed(ownChannelMessage(SCHEDULED_ID))]);
      const scheduleDate = inOneHour();

      const { result } = await sends[kind](services, scheduleDate);

      expect(sendRequests()[0].scheduleDate).toBe(scheduleDate);
      expect(result.scheduled).toBe(true);
      expect(result.warning).toBeUndefined();
      expect(archivedIds(CHANNEL_ID)).toEqual([]);
      expect(cursor(CHANNEL_ID)).toBe(10);
      expect(refetchQueueSize()).toBe(0);

      messageSyncService._handleIncomingMessage(parsed(ownChannelMessage(11, { message: 'later' })), { isEdit: false });

      expect(archivedIds(CHANNEL_ID)).toEqual([11]);
    });
  });

  describe('a delivered send that cannot be archived', () => {
    it('is neither failed nor repeated when the archive write fails, and says so', async () => {
      const { services, messageSyncService, sendRequests } = await start(() => sentToChannel(ownChannelMessage(91)));
      seedChat(CHANNEL_ID);
      const errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});
      messageSyncService.db.close();

      const { result, attempts } = await OPERATIONS.sendText(services, { chat: CHANNEL_ID, text: 'delivered', retries: 3 });

      expect(result.messageId).toBe(91);
      expect(result.warning).toMatch(/not archived/i);
      expect(attempts).toBe(1);
      expect(sendRequests()).toHaveLength(1);
      expect(errorLog).toHaveBeenCalledWith(expect.stringContaining('91'));
    });

    it('is neither failed nor repeated when the peer of the message is unknown', async () => {
      const { services, sendRequests } = await start(() => ({
        ...sentToChannel(ownChannelMessage(92)),
        users: [],
        chats: [],
      }));
      vi.spyOn(console, 'error').mockImplementation(() => {});

      const { result, attempts } = await OPERATIONS.sendText(services, { chat: CHANNEL_ID, text: 'delivered', retries: 3 });

      expect(result.messageId).toBe(92);
      expect(result.warning).toMatch(/not archived/i);
      expect(attempts).toBe(1);
      expect(sendRequests()).toHaveLength(1);
    });

    it('is neither failed nor repeated when a photo response cannot be parsed', async () => {
      const { services, telegramClient, sendRequests } = await start(() => updatesWith({
        _: 'updateNewChannelMessage',
        message: { _: 'messageEmpty', id: 93 },
        pts: 1,
        ptsCount: 1,
      }));
      vi.spyOn(console, 'error').mockImplementation(() => {});
      telegramClient.client.getMessages = vi.fn().mockResolvedValue([parsed(ownChannelMessage(93, { media: PHOTO_MEDIA }))]);

      const { result, attempts } = await OPERATIONS.sendPhoto(services, { chat: CHANNEL_ID, photo: EXISTING_FILE, retries: 3 });

      expect(result.messageId).toBe(93);
      expect(result.warning).toMatch(/not archived/i);
      expect(attempts).toBe(1);
      expect(sendRequests()).toHaveLength(1);
    });

    it('says so when a send operation does not report its message', async () => {
      const { messageSyncService } = await start(() => sentToChannel(ownChannelMessage(94)));
      const telegramClient = { sendTextMessage: vi.fn().mockResolvedValue({ messageId: 94 }) };

      const { result } = await OPERATIONS.sendText({ telegramClient, messageSyncService }, { chat: CHANNEL_ID, text: 'hi' });

      expect(result.warning).toMatch(/not archived/i);
    });

    it('keeps an existing warning next to the archive one', async () => {
      const { messageSyncService } = await start(() => sentToChannel(ownChannelMessage(95)));
      const telegramClient = { sendTextMessage: vi.fn().mockResolvedValue({ messageId: 95, warning: 'first' }) };

      const { result } = await OPERATIONS.sendText({ telegramClient, messageSyncService }, { chat: CHANNEL_ID, text: 'hi' });

      expect(result.warning).toMatch(/^first; .*not archived/i);
    });
  });

  describe('printSendOutcome', () => {
    it('prints the warning next to the outcome in text mode', () => {
      const log = vi.spyOn(console, 'log').mockImplementation(() => {});
      const error = vi.spyOn(console, 'error').mockImplementation(() => {});

      printSendOutcome('Message', { messageId: 7, warning: 'Message delivered but not archived.' });

      expect(log).toHaveBeenCalledWith('Message sent (7).');
      expect(error).toHaveBeenCalledWith('Warning: Message delivered but not archived.');
    });

    it('prints no warning line when there is none', () => {
      vi.spyOn(console, 'log').mockImplementation(() => {});
      const error = vi.spyOn(console, 'error').mockImplementation(() => {});

      printSendOutcome('Photo', { messageId: 8, scheduled: true });

      expect(error).not.toHaveBeenCalled();
    });
  });
});
