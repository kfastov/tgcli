// The message archive on a real SQLite file: one write path, one read projection, and
// the same JSON for a message whether a read takes it from Telegram or from the archive.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { Long, Message, PeersIndex } from '@mtcute/core';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { encodeMessage } from '../core/message-codec.js';
import { OPERATIONS } from '../core/operations.js';
import MessageSyncService from '../message-sync-service.js';

const FRIEND_ID = 700000002;
const BARE_CHANNEL_ID = 700000600;
const CHANNEL_ID = '-1000700000600';
const DATE = 1700000000;
const ACCESS_HASH = Long.fromNumber(1);

const peers = PeersIndex.from({
  users: [{ _: 'user', id: FRIEND_ID, firstName: 'Friend', username: 'friend_one', accessHash: ACCESS_HASH }],
  chats: [{ _: 'channel', id: BARE_CHANNEL_ID, title: 'Group', accessHash: ACCESS_HASH, megagroup: true }],
});

function message(id, overrides = {}) {
  return new Message({
    _: 'message',
    id,
    date: DATE + id,
    peerId: { _: 'peerChannel', channelId: BARE_CHANNEL_ID },
    fromId: { _: 'peerUser', userId: FRIEND_ID },
    message: `message ${id}`,
    ...overrides,
  }, peers);
}

const history = [
  message(1, { message: 'encoding is UTF-8 here' }),
  message(2, {
    message: 'photo with a link https://example.org',
    media: {
      _: 'messageMediaPhoto',
      photo: {
        _: 'photo',
        id: Long.fromNumber(5),
        accessHash: ACCESS_HASH,
        fileReference: new Uint8Array([9, 9]),
        date: DATE,
        sizes: [{ _: 'photoSize', type: 'x', w: 640, h: 480, size: 100 }],
        dcId: 2,
      },
    },
    entities: [{ _: 'messageEntityUrl', offset: 18, length: 19 }],
    groupedId: Long.fromNumber(42),
  }),
  message(3, { replyTo: { _: 'messageReplyHeader', replyToMsgId: 1 }, editDate: DATE + 100 }),
  new Message({
    _: 'messageService',
    id: 4,
    date: DATE + 4,
    peerId: { _: 'peerChannel', channelId: BARE_CHANNEL_ID },
    fromId: { _: 'peerUser', userId: FRIEND_ID },
    action: { _: 'messageActionChatJoinedByLink', inviterId: FRIEND_ID },
  }, peers),
];
const stored = history.map(encodeMessage);

describe('message archive', () => {
  let storeDir;
  let service;
  let archive;

  beforeEach(() => {
    storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-message-archive-'));
    service = new MessageSyncService({}, { dbPath: path.join(storeDir, 'messages.db') });
    archive = service.archive;
    service.db.prepare(`
      INSERT INTO channels (channel_id, peer_title, peer_type, username, sync_enabled)
      VALUES (?, 'Group', 'channel', 'group_chat', 1)
    `).run(CHANNEL_ID);
  });

  afterEach(() => {
    service?.close();
    fs.rmSync(storeDir, { recursive: true, force: true });
  });

  function liveContext() {
    const byId = new Map(stored.map((entry) => [entry.id, entry]));
    return {
      messageSyncService: service,
      telegramClient: {
        getMessageById: vi.fn(async (_chat, id) => byId.get(Number(id)) ?? null),
        getMessagesByChannelId: vi.fn(async () => ({ peerTitle: 'Group', messages: [...stored].reverse() })),
        getMessageContext: vi.fn(async (_chat, id) => ({
          target: byId.get(Number(id)),
          before: stored.filter((entry) => entry.id < id),
          after: stored.filter((entry) => entry.id > id),
        })),
        getPeerMetadata: vi.fn(async () => ({ peerTitle: 'Group', username: 'group_chat' })),
      },
    };
  }

  const withoutSource = ({ source, ...view }) => view;

  it('returns the same message from Telegram and from the archive', async () => {
    archive.write(CHANNEL_ID, stored);
    const ctx = liveContext();

    const live = await OPERATIONS.messagesList(ctx, { channelId: CHANNEL_ID, source: 'live' });
    const archived = await OPERATIONS.messagesList(ctx, { channelId: CHANNEL_ID, source: 'archive' });
    expect(archived.messages.map(withoutSource)).toEqual(live.messages.map(withoutSource));
    expect(archived.messages).toHaveLength(4);

    for (const id of [1, 2, 3, 4]) {
      const fromTelegram = await OPERATIONS.messagesGet(ctx, { channelId: CHANNEL_ID, messageId: id, source: 'live' });
      const fromArchive = await OPERATIONS.messagesGet(ctx, { channelId: CHANNEL_ID, messageId: id, source: 'archive' });
      expect(withoutSource(fromArchive.message)).toEqual(withoutSource(fromTelegram.message));
    }

    const liveContextResult = await OPERATIONS.messagesContext(ctx, { channelId: CHANNEL_ID, messageId: 2, source: 'live' });
    const archiveContextResult = await OPERATIONS.messagesContext(ctx, { channelId: CHANNEL_ID, messageId: 2, source: 'archive' });
    for (const part of ['before', 'after']) {
      expect(archiveContextResult[part].map(withoutSource)).toEqual(liveContextResult[part].map(withoutSource));
    }
  });

  it('gives a message its reply, album, edit, links and action', () => {
    archive.write(CHANNEL_ID, stored);
    expect(archive.get(CHANNEL_ID, 2)).toMatchObject({
      channelId: CHANNEL_ID,
      peerTitle: 'Group',
      username: 'group_chat',
      fromUsername: 'friend_one',
      urls: ['https://example.org'],
      groupedId: '42',
      media: { type: 'photo', width: 640, height: 480 },
    });
    expect(archive.get(CHANNEL_ID, 3)).toMatchObject({
      replyTo: { messageId: 1, topId: null },
      editDate: new Date((DATE + 100) * 1000).toISOString(),
    });
    expect(archive.get(CHANNEL_ID, 4)).toMatchObject({
      text: '',
      action: { type: 'chatJoinedByLink', inviterId: String(FRIEND_ID) },
    });
  });

  it('keeps an archived message unless the new copy replaces it', () => {
    expect(archive.write(CHANNEL_ID, stored)).toBe(4);
    const edited = encodeMessage(message(1, { message: 'edited text', editDate: DATE + 500 }));

    expect(archive.write(CHANNEL_ID, [edited])).toBe(0);
    expect(archive.get(CHANNEL_ID, 1).text).toBe('encoding is UTF-8 here');

    expect(archive.write(CHANNEL_ID, [edited], { replace: true })).toBe(1);
    expect(archive.get(CHANNEL_ID, 1)).toMatchObject({ text: 'edited text', editDate: new Date((DATE + 500) * 1000).toISOString() });
  });

  it('drops the links and media of a message replaced by a copy without them', () => {
    archive.write(CHANNEL_ID, stored);
    const count = (table) => service.db.prepare(`SELECT COUNT(*) AS n FROM ${table} WHERE message_id = 2`).get().n;
    expect([count('message_links'), count('message_media')]).toEqual([1, 1]);

    archive.write(CHANNEL_ID, [encodeMessage(message(2, { message: 'no more photo' }))], { replace: true });
    expect([count('message_links'), count('message_media')]).toEqual([0, 0]);
    expect(archive.get(CHANNEL_ID, 2).media).toBeNull();
  });

  it('refuses a chat that is not resolved to its id', () => {
    expect(() => archive.write('@group_chat', stored)).toThrow(/resolved to its Telegram id/);
  });

  it('keeps what a fuller sender description adds to the users table', () => {
    archive.write(CHANNEL_ID, [stored[0]], {
      senders: [{ id: String(FRIEND_ID), peerType: 'user', phone: '15550000000', isContact: true, isBot: false }],
    });
    archive.write(CHANNEL_ID, [stored[1]]);
    expect(service.db.prepare('SELECT username, phone, is_contact FROM users WHERE user_id = ?').get(String(FRIEND_ID)))
      .toEqual({ username: 'friend_one', phone: '15550000000', is_contact: 1 });
  });

  it('searches words that are not FTS5 syntax as plain text', () => {
    archive.write(CHANNEL_ID, stored);
    expect(archive.search({ query: 'UTF-8' }).map((view) => view.messageId)).toEqual([1]);
    expect(archive.search({ query: 'example.org' }).map((view) => view.messageId)).toEqual([2]);
  });

  it('still takes FTS5 syntax that parses', () => {
    archive.write(CHANNEL_ID, stored);
    expect(archive.search({ query: 'encoding OR photo' }).map((view) => view.messageId).sort()).toEqual([1, 2]);
    expect(archive.search({ query: 'phot*' }).map((view) => view.messageId)).toEqual([2]);
  });

  it('lists a message of a chat with several matching tags once', () => {
    archive.write(CHANNEL_ID, stored);
    service.setChannelTags(CHANNEL_ID, ['news', 'tech']);
    const found = archive.search({ query: 'photo', tags: ['news', 'tech'] });
    expect(found.map((view) => view.messageId)).toEqual([2]);
  });

  it('deletes a message with its links and media', () => {
    archive.write(CHANNEL_ID, stored);
    archive.deleteChannelMessages(CHANNEL_ID, [2]);
    expect(archive.get(CHANNEL_ID, 2)).toBeNull();
    expect(service.db.prepare('SELECT COUNT(*) AS n FROM message_media').get().n).toBe(0);
    expect(archive.count(CHANNEL_ID)).toBe(3);
  });
});
