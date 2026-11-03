import { execFile } from 'node:child_process';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { Long } from '@mtcute/core';
import { iterHistory } from '@mtcute/core/methods.js';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import MessageSyncService from '../message-sync-service.js';
import { recoverArchiveGaps } from '../scripts/recover-archive-gaps.js';
import TelegramClient from '../telegram-client.js';

const SCRIPT = fileURLToPath(new URL('../scripts/recover-archive-gaps.js', import.meta.url));

const CHANNEL_ID = '-1000000000500';
const ACCESS_HASH = Long.fromNumber(1);
const INPUT_PEER = { _: 'inputPeerChannel', channelId: 500, accessHash: ACCESS_HASH };
const chats = [{ _: 'channel', id: 500, title: 'Group', accessHash: ACCESS_HASH, megagroup: true }];
const users = [{ _: 'user', id: 2, firstName: 'Friend', username: 'friend', accessHash: ACCESS_HASH }];

function rawMessage(id) {
  return {
    _: 'message',
    id,
    date: 1700000000 + id,
    peerId: { _: 'peerChannel', channelId: 500 },
    fromId: { _: 'peerUser', userId: 2 },
    message: `message ${id}`,
  };
}

// messages.getHistory as Telegram serves it for a plain walk: newest first, below offset_id.
function historyOf(ids) {
  return ({ offsetId, limit }) => {
    const page = ids.filter((id) => !offsetId || id < offsetId).sort((a, b) => b - a).slice(0, limit);
    return { _: 'messages.messages', messages: page.map(rawMessage), users, chats, count: ids.length };
  };
}

describe('recoverArchiveGaps', () => {
  let storeDir;
  let service;
  let historyRequests;

  const setUp = (historyIds, archivedIds, { syncEnabled = 1 } = {}) => {
    const history = historyOf(historyIds);
    historyRequests = [];
    const telegramClient = Object.create(TelegramClient.prototype);
    telegramClient.ensureLogin = vi.fn().mockResolvedValue(undefined);
    telegramClient.client = {
      resolvePeer: async () => INPUT_PEER,
      iterHistory: (peer, options) => iterHistory({
        call: async (request) => {
          historyRequests.push(request);
          return history(request);
        },
      }, peer, options),
    };
    service = new MessageSyncService(telegramClient, { dbPath: path.join(storeDir, 'messages.db'), batchSize: 100 });
    service.db.prepare(`
      INSERT INTO channels (channel_id, peer_title, sync_enabled, last_message_id) VALUES (?, 'Group', ?, ?)
    `).run(CHANNEL_ID, syncEnabled, Math.max(...historyIds));
    service.insertMessagesTx(archivedIds.map((id) => service._buildMessageRecord(CHANNEL_ID, {
      id,
      date: 1700000000 + id,
      text: `archived ${id}`,
    })));
    return { telegramClient, messageSyncService: service };
  };

  const archived = () => service.db.prepare(`
    SELECT message_id, text FROM messages WHERE channel_id = ? ORDER BY message_id
  `).all(CHANNEL_ID);

  const cursor = () => service.db.prepare(`
    SELECT last_message_id FROM channels WHERE channel_id = ?
  `).get(CHANNEL_ID).last_message_id;

  beforeEach(() => {
    storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-recover-test-'));
    vi.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    if (service?.db?.open) {
      service.db.close();
    }
    fs.rmSync(storeDir, { recursive: true, force: true });
    vi.restoreAllMocks();
  });

  it('fills gaps at any distance across pages, and keeps existing rows and the cursor', async () => {
    const historyIds = Array.from({ length: 250 }, (_, index) => 10 + index);
    const services = setUp(historyIds, historyIds.filter((id) => id !== 11 && id !== 137));

    const outcome = await recoverArchiveGaps(services, { chat: CHANNEL_ID, afterId: 10, delayMs: 0 });

    expect(outcome).toEqual({ added: 2 });
    const rows = archived();
    expect(rows).toHaveLength(250);
    expect(rows.find((row) => row.message_id === 11).text).toBe('message 11');
    expect(rows.find((row) => row.message_id === 137).text).toBe('message 137');
    expect(rows.find((row) => row.message_id === 12).text).toBe('archived 12');
    expect(cursor()).toBe(259);
  });

  it('walks the whole chat from 0, and stops below the given message otherwise', async () => {
    const services = setUp([10, 11, 12, 13], [12, 13]);

    expect(await recoverArchiveGaps(services, { chat: CHANNEL_ID, afterId: 11, delayMs: 0 })).toEqual({ added: 0 });
    expect(archived().map((row) => row.message_id)).toEqual([12, 13]);

    expect(await recoverArchiveGaps(services, { chat: CHANNEL_ID, afterId: 0, delayMs: 0 })).toEqual({ added: 2 });
    expect(archived().map((row) => row.message_id)).toEqual([10, 11, 12, 13]);
  });

  // Telegram's handling of min_id combined with a negative add_offset is not something
  // to depend on: a server that windows first would answer empty and read as done.
  it('uses only the plain newest-first walk', async () => {
    const historyIds = Array.from({ length: 150 }, (_, index) => 10 + index);
    const services = setUp(historyIds, []);

    await recoverArchiveGaps(services, { chat: CHANNEL_ID, afterId: 5, delayMs: 0 });

    expect(historyRequests.length).toBeGreaterThan(1);
    for (const request of historyRequests) {
      expect(request).toMatchObject({ addOffset: 0, minId: 0, maxId: 0 });
    }
  });

  it('records the senders of the messages it adds', async () => {
    const services = setUp([10, 11], [10]);

    await recoverArchiveGaps(services, { chat: CHANNEL_ID, afterId: 10, delayMs: 0 });

    const sender = service.db.prepare('SELECT username FROM users WHERE user_id = ?').get('2');
    expect(sender.username).toBe('friend');
  });

  it('refuses a chat with sync disabled', async () => {
    const services = setUp([10, 11], [10], { syncEnabled: 0 });

    await expect(recoverArchiveGaps(services, { chat: CHANNEL_ID, afterId: 10, delayMs: 0 }))
      .rejects.toThrow(/sync disabled/);
    expect(archived()).toHaveLength(1);
  });

  it('refuses a chat that is not in the archive', async () => {
    const services = setUp([10], [10]);

    await expect(recoverArchiveGaps(services, { chat: '-1000000000999', afterId: 0, delayMs: 0 }))
      .rejects.toThrow(/not in the archive/);
  });
});

describe('scripts/recover-archive-gaps.js', () => {
  let storeDir;

  const runScript = (args) => new Promise((resolve) => {
    execFile(process.execPath, [SCRIPT, ...args], { env: { ...process.env, TGCLI_STORE: storeDir } }, (error, stdout, stderr) => {
      resolve({ code: error?.code ?? 0, stderr });
    });
  });

  beforeEach(() => {
    storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-recover-script-test-'));
  });

  afterEach(() => {
    fs.rmSync(storeDir, { recursive: true, force: true });
  });

  it('prints usage on bad arguments', async () => {
    const { code, stderr } = await runScript(['123', 'not-a-number']);

    expect(code).toBe(1);
    expect(stderr).toMatch(/^Usage:/);
  });

  // The server takes no store lock, so the lock alone would not stop the script
  // from writing next to it.
  it('refuses to run while a server answers on the store', async () => {
    const server = http.createServer((_req, res) => {
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ ok: true, pid: process.pid }));
    });
    await new Promise((resolve) => {
      server.listen(0, '127.0.0.1', resolve);
    });
    try {
      fs.writeFileSync(path.join(storeDir, 'control.json'), JSON.stringify({
        port: server.address().port,
        token: 'token',
        pid: process.pid,
      }));

      const { code, stderr } = await runScript(['123', '0']);

      expect(code).toBe(1);
      expect(stderr).toContain(`pid ${process.pid}`);
    } finally {
      server.close();
    }
  });
});
