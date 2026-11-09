// Reads by username find the chat without Telegram, as the archive did before #73: by the
// username the archive keeps, then by mtcute's peer cache. Only a username neither knows
// goes to Telegram; writes still ask Telegram, since a username changes hands.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { Long } from '@mtcute/core';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import MessageSyncService from '../message-sync-service.js';
import { OPERATIONS } from '../core/ops/index.js';
import { executeOperation } from '../core/ops/execute.js';

// Runs an operation the way the server does: args validated, defaults applied.
function run(name, services, args) {
  return executeOperation(OPERATIONS[name], args, { services });
}

const CHANNEL = '-1000000000701';
const OTHER = '-1000000000702';
const USER = '700000003';
const hash = Long.fromNumber(1);

describe('chats given by username, for reads', () => {
  let storeDir;
  let service;
  let telegram;
  let cache;

  beforeEach(() => {
    storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-username-reads-'));
    cache = new Map();
    telegram = {
      getPeer: vi.fn(async () => {
        throw new Error('asked Telegram');
      }),
      resolvePeer: vi.fn(async () => {
        throw new Error('asked Telegram');
      }),
      call: vi.fn(async () => {
        throw new Error('asked Telegram');
      }),
      storage: {
        // mtcute keeps usernames lowercased.
        peers: { getByUsername: vi.fn(async (name) => cache.get(name.toLowerCase()) ?? null) },
        self: { fetch: vi.fn(async () => ({ userId: Number(USER), isBot: false })) },
      },
    };
    service = new MessageSyncService({ client: telegram }, { dbPath: path.join(storeDir, 'messages.db') });
    const insert = service.db.prepare('INSERT INTO channels (channel_id, username, in_dialogs) VALUES (?, ?, ?)');
    insert.run(CHANNEL, 'News_Room', 1);
    insert.run(USER, 'friend', 1);
  });

  afterEach(() => {
    service?.close();
    fs.rmSync(storeDir, { recursive: true, force: true });
  });

  it('finds a chat by the username the archive keeps, in any case, with or without @', async () => {
    for (const chat of ['@News_Room', 'news_room', ' @NEWS_ROOM ']) {
      expect(await service.resolveChatKeyForRead(chat)).toBe(CHANNEL);
    }
    expect(await service.resolveChatKeyForRead('@friend')).toBe(USER);
    expect(await service.resolveChatKeyForRead(CHANNEL)).toBe(CHANNEL);
    expect(telegram.getPeer).not.toHaveBeenCalled();
    expect(telegram.call).not.toHaveBeenCalled();
  });

  it('takes the peer cache for a username the archive does not keep', async () => {
    cache.set('elsewhere', { _: 'inputPeerChannel', channelId: 702, accessHash: hash });

    expect(await service.resolveChatKeyForRead('@elsewhere')).toBe(OTHER);
    expect(telegram.getPeer).not.toHaveBeenCalled();
  });

  it('answers me from the stored session', async () => {
    expect(await service.resolveChatKeyForRead('me')).toBe(USER);
    expect(telegram.getPeer).not.toHaveBeenCalled();
  });

  it('takes the one in the dialog list when two archived chats have the username', async () => {
    service.db.prepare('INSERT INTO channels (channel_id, username, in_dialogs) VALUES (?, ?, 0)').run(OTHER, 'news_room');

    expect(await service.findChatKeyLocally('@news_room')).toBe(CHANNEL);
    expect(telegram.getPeer).not.toHaveBeenCalled();
  });

  it('forgets a username the dialog list no longer shows', async () => {
    service.upsertChannels([{ id: CHANNEL, title: 'News', username: null }, { id: USER, title: 'Friend', username: 'friend' }]);

    expect(service.db.prepare('SELECT username FROM channels WHERE channel_id = ?').get(CHANNEL).username).toBeNull();
    expect(service.db.prepare('SELECT username FROM channels WHERE channel_id = ?').get(USER).username).toBe('friend');
  });

  it('asks Telegram only for a username nothing here knows', async () => {
    telegram.getPeer.mockResolvedValueOnce({ id: 700000009, username: 'stranger', usernames: [] });

    expect(await service.resolveChatKeyForRead('@stranger')).toBe('700000009');
    expect(telegram.getPeer).toHaveBeenCalledTimes(1);
  });

  it('keeps asking Telegram for writes, where a stale username would pick the wrong chat', async () => {
    telegram.getPeer.mockResolvedValueOnce({ id: Number(CHANNEL), username: 'News_Room', usernames: [] });

    expect(await service.resolveChatKey('@News_Room')).toBe(CHANNEL);
    expect(telegram.getPeer).toHaveBeenCalledTimes(1);
  });

  // These ask Telegram anyway, so they take its word over a username stored here.
  it('asks Telegram which chat a username is for live reads and for the live fallback', async () => {
    const live = { getMessagesByChannelId: vi.fn(async () => ({ messages: [] })), getPeerMetadata: vi.fn(async () => ({})) };
    telegram.getPeer.mockResolvedValue({ id: Number(OTHER), username: 'news_room', usernames: [] });

    await run('messagesList', { messageSyncService: service, telegramClient: live }, { channelIds: '@news_room', source: 'live', limit: 5 });
    expect(live.getMessagesByChannelId).toHaveBeenLastCalledWith(OTHER, 5, {});

    await run('messagesList', { messageSyncService: service, telegramClient: live }, { channelIds: '@news_room', source: 'archive', limit: 5 });
    expect(live.getMessagesByChannelId).toHaveBeenLastCalledWith(OTHER, 5, {});
  });

  it('lists archived messages by username without Telegram', async () => {
    service.insertMessagesTx([service._buildMessageRecord(CHANNEL, { id: 1, date: 1700000001, text: 'one' })]);

    const payload = await run('messagesList', { messageSyncService: service, telegramClient: {} }, {
      channelIds: '@news_room',
      source: 'archive',
      limit: 5,
    });

    expect(payload.messages.map((message) => [message.channelId, message.messageId ?? message.id])).toEqual([[CHANNEL, 1]]);
    expect(telegram.getPeer).not.toHaveBeenCalled();
  });
});
