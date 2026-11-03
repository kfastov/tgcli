// Runs the send operations through real mtcute and a real SQLite archive, stubbing
// only the network call: what goes wrong here depends on how mtcute reports a send
// back to the client, which a mocked client.sendText could not show.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { InputMedia, Long, Message, PeersIndex } from '@mtcute/core';
import { resolvePeer, sendMedia, sendText } from '@mtcute/core/methods.js';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { OPERATIONS } from '../core/operations.js';
import MessageSyncService from '../message-sync-service.js';
import TelegramClient from '../telegram-client.js';

const EXISTING_FILE = fileURLToPath(new URL('../package.json', import.meta.url));

const SELF_ID = 1;
const FRIEND_ID = 2;
const BARE_CHANNEL_ID = 500;
const CHANNEL_ID = '-1000000000500';
const FRIEND_CHAT_ID = String(FRIEND_ID);

const ACCESS_HASH = Long.fromNumber(1);
const users = [
  { _: 'user', id: SELF_ID, firstName: 'Me', accessHash: ACCESS_HASH, self: true },
  { _: 'user', id: FRIEND_ID, firstName: 'Friend', accessHash: ACCESS_HASH },
];
const chats = [
  { _: 'channel', id: BARE_CHANNEL_ID, title: 'Group', accessHash: ACCESS_HASH, megagroup: true, forum: true },
];

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

function parsed(raw) {
  return new Message(raw, PeersIndex.from({ users, chats }));
}

function updatesWith(update) {
  return { _: 'updates', updates: [update], users, chats, date: 0, seq: 0 };
}

function sentToChannel(raw) {
  return updatesWith({ _: 'updateNewChannelMessage', message: raw, pts: 1, ptsCount: 1 });
}

function scheduledIn(raw) {
  return updatesWith({ _: 'updateNewScheduledMessage', message: raw });
}

function createHarness({ respond, dbPath }) {
  const call = vi.fn(async (request) => respond(request));
  const raw = {
    call,
    handleClientUpdate: vi.fn(),
    timers: { cancel: vi.fn() },
    log: { warn: vi.fn(), debug: vi.fn() },
    getMtprotoMessageId: vi.fn(),
    storage: {
      self: { getCached: () => ({ userId: SELF_ID }) },
      peers: {
        getById: async (id) => (id === FRIEND_ID
          ? { _: 'inputPeerUser', userId: FRIEND_ID, accessHash: ACCESS_HASH }
          : { _: 'inputPeerChannel', channelId: BARE_CHANNEL_ID, accessHash: ACCESS_HASH }),
        getCompleteById: async (id) => users.find((user) => user.id === id) ?? null,
      },
    },
  };
  const telegramClient = Object.create(TelegramClient.prototype);
  telegramClient.ensureLogin = vi.fn().mockResolvedValue(undefined);
  telegramClient.client = {
    sendText: (peer, text, params) => sendText(raw, peer, text, params),
    // Uploading is out of scope: a dice needs no upload and takes the same params.
    sendMedia: (peer, _media, params) => sendMedia(raw, peer, InputMedia.dice('🎲', {}), params),
    resolvePeer: (peer) => resolvePeer(raw, peer),
    _normalizeInputMedia: async () => ({ _: 'inputMediaDice', emoticon: '🎲' }),
    call,
    handleClientUpdate: raw.handleClientUpdate,
    getMessages: vi.fn().mockRejectedValue(new Error('getMessages is not available')),
  };
  const messageSyncService = new MessageSyncService(telegramClient, {
    dbPath,
    batchSize: 100,
    interJobDelayMs: 0,
    interBatchDelayMs: 0,
  });
  return { call, telegramClient, messageSyncService };
}

describe('archiving messages sent by tgcli', () => {
  let storeDir;
  let harness;

  const start = (respond) => {
    harness = createHarness({ respond, dbPath: path.join(storeDir, 'messages.db') });
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

  const cursor = (channelId) => harness.messageSyncService.db.prepare(`
    SELECT last_message_id FROM channels WHERE channel_id = ?
  `).get(channelId).last_message_id;

  const syncEnabled = (channelId) => harness.messageSyncService.db.prepare(`
    SELECT sync_enabled FROM channels WHERE channel_id = ?
  `).get(channelId).sync_enabled;

  const liveHistory = (...messages) => {
    harness.telegramClient.getMessagesByChannelId = vi.fn(async (_id, _limit, { minId }) => ({
      peerTitle: 'Group',
      peerType: 'chat',
      messages: messages
        .filter((message) => message.id > minId)
        .map((message) => harness.telegramClient._serializeMessage(message, message.chat)),
    }));
  };

  beforeEach(() => {
    storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-sent-archive-test-'));
  });

  afterEach(() => {
    if (harness?.messageSyncService?.db?.open) {
      harness.messageSyncService.db.close();
    }
    fs.rmSync(storeDir, { recursive: true, force: true });
    vi.restoreAllMocks();
  });

  it('keeps a sent text message after the next incoming message moves the cursor', async () => {
    const { telegramClient, messageSyncService } = start(() => sentToChannel(ownChannelMessage(11, { message: 'mine' })));
    seedChat(CHANNEL_ID);

    const { result } = await OPERATIONS.sendText({ telegramClient, messageSyncService }, { chat: CHANNEL_ID, text: 'mine' });
    expect(result.messageId).toBe(11);

    messageSyncService._handleIncomingMessage(parsed(rawChannelMessage(12)), { isEdit: false });
    liveHistory(parsed(ownChannelMessage(11)), parsed(rawChannelMessage(12)));
    await messageSyncService._syncNewerMessages(CHANNEL_ID);

    expect(cursor(CHANNEL_ID)).toBe(12);
    expect(archivedIds(CHANNEL_ID)).toEqual([11, 12]);
    expect(archived(CHANNEL_ID)[0].text).toBe('mine');
  });

  it('archives a text message that Telegram answers with updateShortSentMessage', async () => {
    const { telegramClient, messageSyncService } = start(() => ({
      _: 'updateShortSentMessage',
      id: 21,
      pts: 5,
      ptsCount: 1,
      date: 1700000021,
      out: true,
    }));
    seedChat(FRIEND_CHAT_ID);

    await OPERATIONS.sendText({ telegramClient, messageSyncService }, { chat: FRIEND_CHAT_ID, text: 'hello' });

    expect(archived(FRIEND_CHAT_ID)).toEqual([expect.objectContaining({ message_id: 21, text: 'hello' })]);
  });

  it('archives a message sent to Saved Messages', async () => {
    const { telegramClient, messageSyncService } = start(() => updatesWith({
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
      pts: 6,
      ptsCount: 1,
    }));
    seedChat(String(SELF_ID));

    await OPERATIONS.sendText({ telegramClient, messageSyncService }, { chat: 'me', text: 'note to self' });

    expect(archived(String(SELF_ID))).toEqual([expect.objectContaining({ message_id: 25, text: 'note to self' })]);
  });

  it('archives a sent file', async () => {
    const { telegramClient, messageSyncService } = start(() => sentToChannel(ownChannelMessage(31, {
      message: 'caption',
      media: { _: 'messageMediaDice', value: 1, emoticon: '🎲' },
    })));
    seedChat(CHANNEL_ID);

    await OPERATIONS.sendFile({ telegramClient, messageSyncService }, { chat: CHANNEL_ID, file: EXISTING_FILE, caption: 'caption' });

    expect(archived(CHANNEL_ID)).toEqual([expect.objectContaining({ message_id: 31, text: 'caption' })]);
  });

  it('archives a sent photo from the send response, even if the follow-up lookup fails', async () => {
    const { telegramClient, messageSyncService, call } = start(() => sentToChannel(ownChannelMessage(41, {
      message: 'photo caption',
      media: { _: 'messageMediaDice', value: 1, emoticon: '🎲' },
    })));
    seedChat(CHANNEL_ID);
    vi.spyOn(console, 'error').mockImplementation(() => {});

    const { result } = await OPERATIONS.sendPhoto(
      { telegramClient, messageSyncService },
      { chat: CHANNEL_ID, photo: EXISTING_FILE, caption: 'photo caption' },
    );

    expect(call).toHaveBeenCalledTimes(1);
    expect(result.warning).toMatch(/enrichment failed/i);
    expect(archived(CHANNEL_ID)).toEqual([expect.objectContaining({ message_id: 41, text: 'photo caption' })]);
  });

  it('records the forum topic of a sent message', async () => {
    const { telegramClient, messageSyncService, call } = start(() => sentToChannel(ownChannelMessage(51, {
      replyTo: { _: 'messageReplyHeader', forumTopic: true, replyToMsgId: 77 },
    })));
    seedChat(CHANNEL_ID);

    await OPERATIONS.sendText({ telegramClient, messageSyncService }, { chat: CHANNEL_ID, text: 'in topic', topicId: 77 });

    expect(call.mock.calls[0][0].replyTo.replyToMsgId).toBe(77);
    expect(archived(CHANNEL_ID)).toEqual([expect.objectContaining({ message_id: 51, topic_id: 77 })]);
  });

  it('does not archive into a chat with sync disabled, and leaves it disabled', async () => {
    const { telegramClient, messageSyncService } = start(() => sentToChannel(ownChannelMessage(61)));
    seedChat(CHANNEL_ID, { syncEnabled: 0 });

    const { result } = await OPERATIONS.sendText({ telegramClient, messageSyncService }, { chat: CHANNEL_ID, text: 'hi' });

    expect(result.messageId).toBe(61);
    expect(archivedIds(CHANNEL_ID)).toEqual([]);
    expect(syncEnabled(CHANNEL_ID)).toBe(0);
  });

  it('does not duplicate a sent message that sync or realtime see again', async () => {
    const { telegramClient, messageSyncService } = start(() => sentToChannel(ownChannelMessage(71)));
    seedChat(CHANNEL_ID);

    await OPERATIONS.sendText({ telegramClient, messageSyncService }, { chat: CHANNEL_ID, text: 'once' });

    const sameMessage = parsed(ownChannelMessage(71));
    messageSyncService._handleIncomingMessage(sameMessage, { isEdit: false });
    liveHistory(sameMessage);
    await messageSyncService._syncNewerMessages(CHANNEL_ID);

    expect(archivedIds(CHANNEL_ID)).toEqual([71]);
  });

  it('leaves the cursor alone, so messages received while the server was down still sync', async () => {
    const { telegramClient, messageSyncService } = start(() => sentToChannel(ownChannelMessage(82)));
    seedChat(CHANNEL_ID, { lastMessageId: 80 });

    await OPERATIONS.sendText({ telegramClient, messageSyncService }, { chat: CHANNEL_ID, text: 'mine' });
    expect(cursor(CHANNEL_ID)).toBe(80);

    liveHistory(parsed(rawChannelMessage(81)), parsed(ownChannelMessage(82)));
    await messageSyncService._syncNewerMessages(CHANNEL_ID);

    expect(archivedIds(CHANNEL_ID)).toEqual([81, 82]);
    expect(cursor(CHANNEL_ID)).toBe(82);
  });

  describe('scheduled messages', () => {
    const SCHEDULED_ID = 900;
    const scheduleDate = () => Math.floor(Date.now() / 1000) + 3600;
    const media = { _: 'messageMediaDice', value: 1, emoticon: '🎲' };

    const sends = {
      text: (services) => OPERATIONS.sendText(services, { chat: CHANNEL_ID, text: 'later', scheduleDate: scheduleDate() }),
      file: (services) => OPERATIONS.sendFile(services, { chat: CHANNEL_ID, file: EXISTING_FILE, scheduleDate: scheduleDate() }),
      photo: (services) => OPERATIONS.sendPhoto(services, { chat: CHANNEL_ID, photo: EXISTING_FILE, scheduleDate: scheduleDate() }),
    };

    it.each(Object.keys(sends))('%s is archived only once it is delivered', async (kind) => {
      const { telegramClient, messageSyncService } = start(() => scheduledIn(ownChannelMessage(SCHEDULED_ID, { media })));
      seedChat(CHANNEL_ID);
      // A scheduled id is from another sequence: looking it up can hit an unrelated message.
      telegramClient.client.getMessages = vi.fn().mockResolvedValue([parsed(rawChannelMessage(SCHEDULED_ID))]);

      await sends[kind]({ telegramClient, messageSyncService });

      expect(archivedIds(CHANNEL_ID)).toEqual([]);
      expect(cursor(CHANNEL_ID)).toBe(10);

      messageSyncService._handleIncomingMessage(parsed(ownChannelMessage(11, { message: 'later' })), { isEdit: false });

      expect(archivedIds(CHANNEL_ID)).toEqual([11]);
    });
  });

  it('does not fail or repeat a delivered send when archiving it fails', async () => {
    const { telegramClient, messageSyncService, call } = start(() => sentToChannel(ownChannelMessage(91)));
    seedChat(CHANNEL_ID);
    const errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});
    messageSyncService.db.close();

    const { result, attempts } = await OPERATIONS.sendText(
      { telegramClient, messageSyncService },
      { chat: CHANNEL_ID, text: 'delivered', retries: 3 },
    );

    expect(result.messageId).toBe(91);
    expect(attempts).toBe(1);
    expect(call).toHaveBeenCalledTimes(1);
    expect(errorLog).toHaveBeenCalledWith(expect.stringContaining('91'));
  });
});
