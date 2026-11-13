import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { _messageMediaFromTl } from '@mtcute/core';

import { compactRawJson, parseRawJson, serializeRawJson } from '../core/raw-json.js';
import MessageSyncService from '../message-sync-service.js';
import { summarizeMedia } from '../telegram-client.js';

/**
 * `messages.raw_json` used to be a plain JSON.stringify of the mtcute message:
 * every Uint8Array (photo thumbnails, fileReference) became {"0":1,"1":24,...}
 * (~9 chars per byte) and every TL flag was spelled out as `false`. That made
 * raw_json ~2.6x bigger than its information content, and — because the index
 * maps are no longer bytes — media rebuilt from it got garbage file ids.
 */

function bytes(length, seed) {
  return Uint8Array.from({ length }, (_, i) => (i * 31 + seed) % 256);
}

// A stripped JPEG thumbnail must start with 0x01 or mtcute rejects it.
function strippedThumb(length) {
  const data = bytes(length, 3);
  data[0] = 1;
  return data;
}

// Mirrors the shape mtcute hands us for a photo message: Longs as
// {low, high, unsigned}, binary fields as Uint8Array, explicit false flags.
function rawPhotoMessage() {
  return {
    _: 'message',
    out: false,
    mentioned: false,
    silent: false,
    pinned: false,
    id: 104493,
    peerId: { _: 'peerChannel', channelId: 4401635916 },
    date: 1794054490,
    message: '',
    media: {
      _: 'messageMediaPhoto',
      spoiler: false,
      photo: {
        _: 'photo',
        hasStickers: false,
        id: { low: 460004034, high: 1265719826, unsigned: false },
        accessHash: { low: -2023899295, high: -864648764, unsigned: false },
        fileReference: bytes(29, 7),
        date: 1793954972,
        sizes: [
          { _: 'photoStrippedSize', type: 'i', bytes: strippedThumb(133) },
          { _: 'photoSize', type: 'm', w: 320, h: 218, size: 21327 },
          { _: 'photoSizeProgressive', type: 'y', w: 1280, h: 872, sizes: [17799, 48631, 82440] },
        ],
        dcId: 2,
      },
    },
  };
}

function archivedMessage(raw) {
  return {
    id: raw.id,
    date: raw.date,
    edit_date: null,
    message: raw.message,
    text: raw.message,
    urls: null,
    from_id: null,
    from_is_bot: false,
    topic_id: null,
    media: null,
    raw,
  };
}

function fileIdFromRaw(raw) {
  return summarizeMedia(_messageMediaFromTl(null, raw.media))?.fileId ?? null;
}

describe('serializeRawJson', () => {
  it('stores binary fields as tagged base64 instead of index maps', () => {
    const json = serializeRawJson({ data: Uint8Array.from([1, 24, 255]) });

    expect(JSON.parse(json)).toEqual({ data: { $b64: 'ARj/' } });
  });

  it('stores Node Buffers as tagged base64 too', () => {
    const json = serializeRawJson({ data: Buffer.from([1, 24, 255]) });

    expect(JSON.parse(json)).toEqual({ data: { $b64: 'ARj/' } });
  });

  it('omits null object properties', () => {
    const json = serializeRawJson({ b: null, c: 0, d: '', e: true });

    expect(JSON.parse(json)).toEqual({ c: 0, d: '', e: true });
  });

  it('keeps false, because TL optional Bool fields differ from absent ones', () => {
    // messageActionTopicEdit.closed: false means "reopened", absent means "not about closing".
    const json = serializeRawJson({ _: 'messageActionTopicEdit', closed: false });

    expect(JSON.parse(json)).toEqual({ _: 'messageActionTopicEdit', closed: false });
  });

  it('keeps false and null inside arrays so positions stay intact', () => {
    const json = serializeRawJson({ list: [false, null, 1] });

    expect(JSON.parse(json)).toEqual({ list: [false, null, 1] });
  });

  it('is much smaller than plain JSON.stringify for a photo message', () => {
    const message = archivedMessage(rawPhotoMessage());

    const compact = serializeRawJson(message).length;
    const legacy = JSON.stringify(message).length;

    expect(compact * 2).toBeLessThan(legacy);
  });
});

describe('parseRawJson', () => {
  it('turns tagged base64 back into bytes', () => {
    const parsed = parseRawJson('{"data":{"$b64":"ARj/"}}');

    expect(parsed.data).toBeInstanceOf(Uint8Array);
    expect([...parsed.data]).toEqual([1, 24, 255]);
  });

  it('turns legacy index maps back into bytes', () => {
    const parsed = parseRawJson('{"data":{"0":1,"1":24,"2":255}}');

    expect(parsed.data).toBeInstanceOf(Uint8Array);
    expect([...parsed.data]).toEqual([1, 24, 255]);
  });

  it('leaves ordinary objects with numeric-looking values alone', () => {
    const parsed = parseRawJson('{"peer":{"_":"peerChannel","channelId":5},"pair":{"0":1,"2":3}}');

    expect(parsed.peer).toEqual({ _: 'peerChannel', channelId: 5 });
    expect(parsed.pair).toEqual({ 0: 1, 2: 3 });
  });

  it('returns null for empty or malformed input', () => {
    expect(parseRawJson(null)).toBeNull();
    expect(parseRawJson('')).toBeNull();
    expect(parseRawJson('{not json')).toBeNull();
  });

  it('round-trips a photo message to the same media file id', () => {
    const raw = rawPhotoMessage();
    const expected = fileIdFromRaw(raw);

    const parsed = parseRawJson(serializeRawJson(archivedMessage(raw)));

    expect(expected).toMatch(/^AgAC/);
    expect(fileIdFromRaw(parsed.raw)).toBe(expected);
  });

  it('recovers the media file id from a legacy row', () => {
    const raw = rawPhotoMessage();
    const expected = fileIdFromRaw(raw);

    const parsed = parseRawJson(JSON.stringify(archivedMessage(raw)));

    expect(expected).toMatch(/^AgAC/);
    expect(fileIdFromRaw(parsed.raw)).toBe(expected);
  });
});

describe('compactRawJson', () => {
  it('rewrites a legacy row into the compact format', () => {
    const legacy = JSON.stringify(archivedMessage(rawPhotoMessage()));

    const compact = compactRawJson(legacy);

    expect(compact).toBe(serializeRawJson(parseRawJson(legacy)));
    expect(compact).not.toContain('"0":');
  });

  it('returns null when the input cannot be parsed, so callers leave the row alone', () => {
    expect(compactRawJson('{not json')).toBeNull();
  });
});

describe('MessageSyncService raw_json storage', () => {
  let service;
  let tmpDir;
  let dbPath;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-raw-json-'));
    dbPath = path.join(tmpDir, 'messages.db');
    service = new MessageSyncService(null, { dbPath });
  });

  afterEach(async () => {
    await service?.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  function insertLegacyRow(messageId, text) {
    const raw = { ...rawPhotoMessage(), id: messageId, message: text };
    const message = { ...archivedMessage(raw), text, message: text };
    const record = service._buildMessageRecord('-100123', message);
    service.db.prepare(`
      INSERT INTO messages (channel_id, message_id, date, text, links, files, sender, topic, raw_json)
      VALUES (@channel_id, @message_id, @date, @text, @links, @files, @sender, @topic, @raw_json)
    `).run({ ...record, raw_json: JSON.stringify(message) });
    return raw;
  }

  it('archives new messages in the compact format', () => {
    const record = service._buildMessageRecord('-100123', archivedMessage(rawPhotoMessage()));

    expect(record.raw_json).toContain('"$b64"');
    expect(record.raw_json).not.toContain('"fileReference":{"0"');
  });

  it('rebuilds correct media file ids from legacy rows', () => {
    const raw = insertLegacyRow(1, 'photo');

    service._backfillSearchFields({ rebuildMedia: true });

    const row = service.db.prepare('SELECT file_id FROM message_media WHERE message_id = 1').get();
    expect(fileIdFromRaw(raw)).toMatch(/^AgAC/);
    expect(row?.file_id).toBe(fileIdFromRaw(raw));
  });

  it('does not touch the search index when only raw_json changes', () => {
    insertLegacyRow(1, 'apartment');
    const trigger = service.db.prepare(
      "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = 'messages_au'",
    ).get();

    expect(trigger.sql).toMatch(/AFTER UPDATE OF text, links, files, sender, topic ON messages/);
  });

  it('upgrades the old catch-all update trigger on an existing store', async () => {
    service.db.exec(`
      DROP TRIGGER messages_au;
      CREATE TRIGGER messages_au AFTER UPDATE ON messages BEGIN SELECT 1; END;
    `);
    await service.close();

    service = new MessageSyncService(null, { dbPath });

    const trigger = service.db.prepare(
      "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = 'messages_au'",
    ).get();
    expect(trigger.sql).toMatch(/AFTER UPDATE OF text, links, files, sender, topic ON messages/);
  });

  it('compactRawJson rewrites legacy rows and keeps search working', () => {
    for (let i = 1; i <= 50; i += 1) {
      insertLegacyRow(i, `apartment ${i}`);
    }

    const stats = service.compactRawJson({ batchSize: 7 });

    expect(stats.rowsScanned).toBe(50);
    expect(stats.rowsRewritten).toBe(50);
    expect(stats.bytesAfter * 2).toBeLessThan(stats.bytesBefore);
    const legacyLeft = service.db.prepare(
      `SELECT COUNT(*) AS n FROM messages WHERE raw_json LIKE '%"0":%'`,
    ).get().n;
    expect(legacyLeft).toBe(0);
    const hits = service.db.prepare(
      `SELECT COUNT(*) AS n FROM message_search WHERE message_search MATCH 'apartment'`,
    ).get().n;
    expect(hits).toBe(50);
  });

  it('compactRawJson is idempotent', () => {
    insertLegacyRow(1, 'photo');
    service.compactRawJson();

    const second = service.compactRawJson();

    expect(second.rowsScanned).toBe(1);
    expect(second.rowsRewritten).toBe(0);
  });

  it('keeps the rewrite stats when VACUUM cannot run', () => {
    insertLegacyRow(1, 'photo');
    service._assertFreeSpaceForVacuum = () => {
      throw new Error('VACUUM needs ~9 GB free');
    };

    const stats = service.compactRawJson({ vacuum: true });

    expect(stats.rowsRewritten).toBe(1);
    expect(stats.vacuumed).toBe(false);
    expect(stats.vacuumError).toMatch(/VACUUM needs/);
  });

  it('compactRawJson with vacuum shrinks the database file', () => {
    for (let i = 1; i <= 400; i += 1) {
      insertLegacyRow(i, `apartment ${i}`);
    }
    service.db.pragma('wal_checkpoint(TRUNCATE)');
    const before = fs.statSync(dbPath).size;

    const stats = service.compactRawJson({ vacuum: true });

    expect(stats.vacuumed).toBe(true);
    expect(fs.statSync(dbPath).size).toBeLessThan(before / 2);
  });
});
