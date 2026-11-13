import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import MessageSyncService from '../message-sync-service.js';

/**
 * mtcute builds `Message.chat` lazily and throws when it cannot: a message with no
 * peerId, or a peer missing from the diff's PeersIndex ("Given peer is not available
 * in this index"). `?.` does not catch a throwing getter, so one such message used to
 * throw out of the realtime callback and drop the whole diff: no message archived,
 * no gap recorded, and everything between the cursor and the diff lost for good.
 *
 * mtcute reports CHANNEL_TOO_LONG with the BARE channel id, while the archive keys
 * channels by the marked id.
 */
const BARE_ID = 3713035210;
const MARKED_ID = '-1003713035210';
const CHANNEL = {
  _: 'channel',
  id: BARE_ID,
  title: 'Test Chat',
  accessHash: { low: 1, high: 0, unsigned: false },
  megagroup: true,
  photo: { _: 'chatPhotoEmpty' },
  date: 1,
};

let service;
let tmpDir;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-toolong-'));
  const telegramClient = {
    _serializeMessage: vi.fn((message) => ({
      id: message.id,
      date: Math.floor(message.date.getTime() / 1000),
      text: message.text,
    })),
  };
  service = new MessageSyncService(telegramClient, { dbPath: path.join(tmpDir, 'messages.db') });
  vi.spyOn(service, 'processQueue').mockResolvedValue(undefined);
  vi.spyOn(console, 'log').mockImplementation(() => {});
  vi.spyOn(console, 'warn').mockImplementation(() => {});
  service.db.prepare(`
    INSERT INTO channels (channel_id, peer_title, peer_type, sync_enabled, last_message_id)
    VALUES (?, 'Test Chat', 'channel', 1, 100)
  `).run(MARKED_ID);
});

afterEach(() => {
  service?.close();
  fs.rmSync(tmpDir, { recursive: true, force: true });
  vi.restoreAllMocks();
});

function rawMessage(id, { withPeer = true } = {}) {
  return {
    _: 'message',
    id,
    date: 1757000000 + id,
    message: `msg ${id}`,
    ...(withPeer ? { peerId: { _: 'peerChannel', channelId: BARE_ID } } : {}),
  };
}

function recordedGap() {
  return service.db.prepare('SELECT gap_after_message_id AS gap FROM channels WHERE channel_id = ?').get(MARKED_ID).gap;
}

function archivedIds() {
  return service.db.prepare('SELECT message_id FROM messages WHERE channel_id = ? ORDER BY message_id')
    .all(MARKED_ID).map((row) => row.message_id);
}

describe('_handleChannelTooLong with messages mtcute cannot resolve a chat for', () => {
  it('records the gap instead of throwing when the channel is missing from the diff peers', () => {
    const diff = { messages: [rawMessage(499), rawMessage(500)], chats: [], users: [] };

    expect(() => service._handleChannelTooLong({ channelId: BARE_ID, diff })).not.toThrow();

    expect(recordedGap()).toBe(100);
    expect(service.processQueue).toHaveBeenCalled();
  });

  it('archives the messages it can resolve and records the gap when one has no peer', () => {
    const diff = { messages: [rawMessage(499, { withPeer: false }), rawMessage(500)], chats: [CHANNEL], users: [] };

    expect(() => service._handleChannelTooLong({ channelId: BARE_ID, diff })).not.toThrow();

    expect(archivedIds()).toEqual([500]);
    expect(recordedGap()).toBe(100);
  });
});
