// Reads by username find the chat without Telegram, as the archive did before #73: by the
// username the archive keeps, then by mtcute's peer cache. Only a username neither knows
// goes to Telegram; writes still ask Telegram, since a username changes hands.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { Long } from '@mtcute/core';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import MessageSyncService from '../message-sync-service.js';
import { OPERATIONS } from '../core/operations.js';

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
        peers: { getByUsername: vi.fn(async (name) => cache.get(name) ?? null) },
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

  it('lets the peer cache, then the dialog list, choose between two chats with one username', async () => {
    service.db.prepare('INSERT INTO channels (channel_id, username, in_dialogs) VALUES (?, ?, 0)').run(OTHER, 'news_room');

    expect(await service.findChatKeyLocally('@news_room')).toBe(CHANNEL);

    cache.set('news_room', { _: 'inputPeerChannel', channelId: 702, accessHash: hash });
    expect(await service.findChatKeyLocally('@news_room')).toBe(OTHER);

    service.db.prepare('UPDATE channels SET in_dialogs = 1 WHERE channel_id = ?').run(OTHER);
    cache.clear();
    expect(await service.findChatKeyLocally('@news_room')).toBeNull();
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

  it('lists archived messages by username without Telegram', async () => {
    service.insertMessagesTx([service._buildMessageRecord(CHANNEL, { id: 1, date: 1700000001, text: 'one' })]);

    const payload = await OPERATIONS.messagesList({ messageSyncService: service, telegramClient: {} }, {
      channelId: '@news_room',
      source: 'archive',
      limit: 5,
    });

    expect(payload.messages.map((message) => [message.channelId, message.messageId ?? message.id])).toEqual([[CHANNEL, 1]]);
    expect(telegram.getPeer).not.toHaveBeenCalled();
  });
});
