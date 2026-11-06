// `tgcli server stats`: the counters a running server keeps. Requests are counted in
// one place, an mtcute network middleware, so the tests below drive the real mtcute
// network manager with only its final send stubbed; archive events are counted at
// fixed points in MessageSyncService, driven here with a stubbed client.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { MtTimeoutError } from '@mtcute/core';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { formatServerStats, ServerStats, STAT_EVENTS } from '../core/server-stats.js';
import MessageSyncService from '../message-sync-service.js';
import TelegramClient from '../telegram-client.js';

const MINUTE_MS = 60 * 1000;
const START = Date.UTC(2026, 10, 6, 12, 0, 30);
const CHANNEL_ID = '-1000000000500';
const BASE_DATE = 1700000000;

function rpcError(errorCode, errorMessage) {
  return { _: 'mt_rpc_error', errorCode, errorMessage };
}

// The stats middlewares around a send that answers `answer`, the way mtcute composes them.
function callThrough(stats, method, answer) {
  const [outer, inner] = stats.networkMiddlewares([]);
  return outer({ request: { _: method } }, (ctx) => inner(ctx, async () => answer()));
}

function makeClock(start = START) {
  let now = start;
  return {
    now: () => now,
    advance: (ms) => {
      now += ms;
    },
  };
}

describe('ServerStats', () => {
  it('starts with every counter at zero', () => {
    const report = new ServerStats({ now: () => START }).snapshot();

    expect(report.sinceStart.requests).toEqual({ total: 0, byMethod: {} });
    expect(report.sinceStart.errors).toEqual({
      total: 0,
      floodWait: { count: 0, seconds: 0 },
      byError: {},
      byMethod: {},
      heldBack: { total: 0, byMethod: {} },
    });
    for (const [group, names] of Object.entries(STAT_EVENTS)) {
      expect(report.sinceStart[group]).toEqual(Object.fromEntries(names.map((name) => [name, 0])));
      expect(report.lastHour[group]).toEqual(report.sinceStart[group]);
    }
  });

  it('counts requests and errors by method, flood waits by name and seconds', async () => {
    const stats = new ServerStats({ now: () => START });
    const answers = [
      { _: 'messages.messages' },
      rpcError(420, 'FLOOD_WAIT_23'),
      rpcError(420, 'FLOOD_PREMIUM_WAIT_5'),
      rpcError(400, 'CHANNEL_INVALID'),
    ];
    for (const answer of answers) {
      await callThrough(stats, 'messages.getHistory', () => answer);
    }
    await callThrough(stats, 'updates.getChannelDifference', () => ({ _: 'updates.channelDifferenceEmpty' }));

    const { sinceStart } = stats.snapshot();
    expect(sinceStart.requests).toEqual({
      total: 5,
      byMethod: { 'messages.getHistory': 4, 'updates.getChannelDifference': 1 },
    });
    expect(sinceStart.errors).toEqual({
      total: 3,
      floodWait: { count: 2, seconds: 28 },
      byError: { CHANNEL_INVALID: 1, FLOOD_PREMIUM_WAIT: 1, FLOOD_WAIT: 1 },
      byMethod: { 'messages.getHistory': { CHANNEL_INVALID: 1, FLOOD_PREMIUM_WAIT: 1, FLOOD_WAIT: 1 } },
      heldBack: { total: 0, byMethod: {} },
    });
  });

  it('names a thrown error by its name, or by its class when the name says only Error', async () => {
    const stats = new ServerStats({ now: () => START });
    const abort = new DOMException('aborted', 'AbortError');
    for (const error of [new MtTimeoutError(15000), abort, new Error('plain')]) {
      await expect(callThrough(stats, 'help.getConfig', () => {
        throw error;
      })).rejects.toBe(error);
    }

    const { sinceStart } = stats.snapshot();
    expect(sinceStart.requests.total).toBe(3);
    expect(sinceStart.errors.byMethod).toEqual({ 'help.getConfig': { AbortError: 1, Error: 1, MtTimeoutError: 1 } });
    expect(sinceStart.errors.heldBack.total).toBe(0);
  });

  it('keeps the last hour per minute and everything since start', () => {
    const clock = makeClock();
    const stats = new ServerStats({ now: clock.now });

    stats.count('rechecks.queued', 3);
    clock.advance(30 * MINUTE_MS);
    stats.count('rechecks.queued', 2);
    clock.advance(31 * MINUTE_MS);
    stats.count('rechecks.queued');

    const report = stats.snapshot();
    expect(report.sinceStart.rechecks.queued).toBe(6);
    expect(report.lastHour.rechecks.queued).toBe(3);
    expect(report.sinceStart.since).toBe(new Date(START).toISOString());
    // The window starts on the whole minute 59 minutes before the current one.
    expect(report.lastHour.since).toBe(new Date(Date.UTC(2026, 10, 6, 12, 2, 0)).toISOString());
    expect(report.at).toBe(new Date(START + 61 * MINUTE_MS).toISOString());
  });

  it('starts the last hour at the server start while it is younger than an hour', () => {
    const clock = makeClock();
    const stats = new ServerStats({ now: clock.now });
    clock.advance(5 * MINUTE_MS);

    expect(stats.snapshot().lastHour.since).toBe(new Date(START).toISOString());
  });

  it('empties the last hour after an hour of nothing, without losing the totals', () => {
    const clock = makeClock();
    const stats = new ServerStats({ now: clock.now });
    stats.recordRequest('messages.getHistory');
    stats.recordError('messages.getHistory', rpcError(420, 'FLOOD_WAIT_7'));
    clock.advance(60 * MINUTE_MS);

    const report = stats.snapshot();
    expect(report.lastHour.requests.total).toBe(0);
    expect(report.lastHour.errors.floodWait).toEqual({ count: 0, seconds: 0 });
    expect(report.sinceStart.requests.total).toBe(1);
    expect(report.sinceStart.errors.floodWait).toEqual({ count: 1, seconds: 7 });
  });

  it('ignores counts of zero and events no report reads', () => {
    const stats = new ServerStats({ now: () => START });
    stats.count('repair.added', 0);
    stats.count('recheck.queued');

    expect(stats.minutes).toEqual([]);
    expect(stats.snapshot().sinceStart.repair.added).toBe(0);
  });

  it('knows every event MessageSyncService counts', () => {
    const source = fs.readFileSync(new URL('../message-sync-service.js', import.meta.url), 'utf8');
    const counted = source.split('\n').filter((line) => line.includes('stats?.count('))
      .flatMap((line) => [...line.matchAll(/'([a-zA-Z]+\.[a-zA-Z]+)'/g)].map((match) => match[1]));
    const known = Object.entries(STAT_EVENTS).flatMap(([group, names]) => names.map((name) => `${group}.${name}`));

    expect(counted).toEqual(expect.arrayContaining(['repair.wouldAdd', 'repair.added', 'rechecks.queued']));
    expect(counted.filter((event) => !known.includes(event))).toEqual([]);
  });
});

describe('TelegramClient with stats', () => {
  let storeDir;
  let telegramClient;

  beforeEach(() => {
    storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-server-stats-client-'));
  });

  afterEach(async () => {
    vi.useRealTimers();
    await telegramClient?.destroy().catch(() => {});
    telegramClient = null;
    fs.rmSync(storeDir, { recursive: true, force: true });
  });

  // The network manager with its final send stubbed: everything above it, mtcute's
  // flood waiter and internal-error retries included, runs as on a real server.
  function offlineNetwork(stats, send) {
    telegramClient = new TelegramClient(12345, 'hash', '+10000000000', path.join(storeDir, 'session.db'), {
      disableUpdates: true,
      stats,
    });
    const { network } = telegramClient.client._client.mt;
    network._call = send;
    return network;
  }

  it('counts each request that goes out, the flood-wait retry included', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    const stats = new ServerStats();
    const answers = [rpcError(420, 'FLOOD_WAIT_1'), { _: 'messages.messages', messages: [] }];
    const network = offlineNetwork(stats, async () => answers.shift());

    const call = network.call({ _: 'messages.getHistory' });
    await vi.advanceTimersByTimeAsync(1000);
    await expect(call).resolves.toMatchObject({ _: 'messages.messages' });

    const { sinceStart } = stats.snapshot();
    expect(sinceStart.requests.byMethod).toEqual({ 'messages.getHistory': 2 });
    expect(sinceStart.errors.byMethod).toEqual({ 'messages.getHistory': { FLOOD_WAIT: 1 } });
    expect(sinceStart.errors.floodWait).toEqual({ count: 1, seconds: 1 });
  });

  it('counts a timeout under its class, through the real network manager', async () => {
    const stats = new ServerStats();
    const network = offlineNetwork(stats, async () => {
      throw new MtTimeoutError(15000);
    });

    await expect(network.call({ _: 'channels.getMessages' }, { maxRetryCount: 0 })).rejects.toBeInstanceOf(MtTimeoutError);

    expect(stats.snapshot().sinceStart.errors.byMethod).toEqual({ 'channels.getMessages': { MtTimeoutError: 1 } });
  });

  it('counts what mtcute answers from a remembered flood wait as held back, not as a request', async () => {
    const stats = new ServerStats();
    const send = vi.fn(async () => rpcError(420, 'FLOOD_WAIT_30'));
    const network = offlineNetwork(stats, send);

    for (let i = 0; i < 3; i += 1) {
      await expect(network.call({ _: 'messages.getHistory' })).resolves.toMatchObject({ errorCode: 420 });
    }

    expect(send).toHaveBeenCalledTimes(1);
    const { sinceStart } = stats.snapshot();
    expect(sinceStart.requests.byMethod).toEqual({ 'messages.getHistory': 1 });
    expect(sinceStart.errors.byMethod).toEqual({ 'messages.getHistory': { FLOOD_WAIT: 1 } });
    expect(sinceStart.errors.heldBack).toEqual({ total: 2, byMethod: { 'messages.getHistory': { FLOOD_WAIT: 2 } } });
  });

  it('counts an error Telegram answers with before mtcute turns it into an exception', async () => {
    const stats = new ServerStats();
    const network = offlineNetwork(stats, async () => rpcError(400, 'MESSAGE_ID_INVALID'));

    await expect(network.call({ _: 'channels.getMessages' })).resolves.toMatchObject({ errorMessage: 'MESSAGE_ID_INVALID' });

    expect(stats.snapshot().sinceStart.errors.byMethod).toEqual({ 'channels.getMessages': { MESSAGE_ID_INVALID: 1 } });
  });

  it('keeps counting into the same stats after the client is recreated', async () => {
    const stats = new ServerStats();
    offlineNetwork(stats, async () => ({ _: 'boolTrue' }));
    await telegramClient._recreateClient();
    const { network } = telegramClient.client._client.mt;
    network._call = async () => ({ _: 'boolTrue' });

    await network.call({ _: 'account.updateStatus' });

    expect(stats.snapshot().sinceStart.requests.byMethod).toEqual({ 'account.updateStatus': 1 });
  });

  it('adds no middleware when nothing counts', () => {
    telegramClient = new TelegramClient(12345, 'hash', '+10000000000', path.join(storeDir, 'session.db'), {
      disableUpdates: true,
    });

    expect(telegramClient.client._client.mt.network.params.middlewares).toBeUndefined();
  });
});

describe('MessageSyncService counting points', () => {
  let storeDir;
  let service;
  let stats;
  let telegramClient;

  function message(id) {
    return { id, date: BASE_DATE + id, text: `message ${id}` };
  }

  // In the archive since before the messages below, so a repair reaches them.
  function seedChannel({ lastMessageId, oldestMessageId = null, gapAfter = null } = {}) {
    service.db.prepare(`
      INSERT INTO channels (channel_id, peer_title, sync_enabled, last_message_id, oldest_message_id, gap_after_message_id, created_at)
      VALUES (?, 'Group', 1, ?, ?, ?, '2023-01-01 00:00:00')
    `).run(CHANNEL_ID, lastMessageId, oldestMessageId, gapAfter);
  }

  async function recheckAfter(ms) {
    const realNow = Date.now();
    const clock = vi.spyOn(Date, 'now').mockReturnValue(realNow + ms);
    try {
      await service._recheckDueMessages();
    } finally {
      clock.mockRestore();
    }
  }

  beforeEach(() => {
    storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-server-stats-sync-'));
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
    stats = new ServerStats();
    telegramClient = Object.create(TelegramClient.prototype);
    telegramClient.getMessageById = vi.fn();
    telegramClient.getMessagesByChannelId = vi.fn();
    service = new MessageSyncService(telegramClient, {
      dbPath: path.join(storeDir, 'messages.db'),
      batchSize: 100,
      interJobDelayMs: 1,
      interBatchDelayMs: 1,
      repairPageDelayMs: 0,
      stats,
    });
  });

  afterEach(() => {
    if (service.db?.open) {
      service.close();
    }
    fs.rmSync(storeDir, { recursive: true, force: true });
    vi.restoreAllMocks();
  });

  it('counts deleted messages queued, read, found alive, put back and given up', async () => {
    seedChannel({ lastMessageId: 20 });
    service._handleDeleteMessage({ channelId: CHANNEL_ID, messageIds: [13, 14], raw: { ptsCount: 0 } });
    expect(stats.snapshot().sinceStart.rechecks.queued).toBe(2);

    telegramClient.getMessageById.mockImplementation(async (_, id) => (id === 13 ? message(13) : null));
    // A read due after the whole schedule passed lets go of what is still missing.
    await recheckAfter(2 * 60 * 60 * 1000);

    expect(stats.snapshot().sinceStart.rechecks).toEqual({
      queued: 2,
      checked: 2,
      found: 1,
      restored: 1,
      failed: 0,
      gaveUp: 1,
    });
  });

  it('counts a pushed delete as nothing to read again', () => {
    seedChannel({ lastMessageId: 20 });
    service._handleDeleteMessage({ channelId: CHANNEL_ID, messageIds: [13], raw: { ptsCount: 1 } });

    expect(stats.snapshot().sinceStart.rechecks.queued).toBe(0);
  });

  it('counts a read that fails', async () => {
    seedChannel({ lastMessageId: 20 });
    service._handleDeleteMessage({ channelId: CHANNEL_ID, messageIds: [13], raw: { ptsCount: 0 } });
    telegramClient.getMessageById.mockRejectedValue(new Error('network down'));

    await recheckAfter(60 * 1000);

    expect(stats.snapshot().sinceStart.rechecks).toMatchObject({ checked: 0, failed: 1, gaveUp: 0 });
  });

  it('counts repair pages, the messages they checked and added, and finished repairs', async () => {
    seedChannel({ lastMessageId: 12, oldestMessageId: 10 });
    service.insertMessagesTx([10, 12].map((id) => service._buildMessageRecord(CHANNEL_ID, message(id))));
    telegramClient.getMessagesByChannelId.mockImplementation(async (_, __, { offsetId }) => ({
      messages: [12, 11, 10, 9].filter((id) => !offsetId || id < offsetId).slice(0, 2).map(message),
    }));

    service.addRepairs({ chats: [CHANNEL_ID] });
    await service.processQueue();

    // Pages [12, 11], [10, 9] and an empty one; 11 and 9 were missing.
    expect(stats.snapshot().sinceStart.repair).toEqual({
      pages: 3,
      checked: 4,
      added: 2,
      wouldAdd: 0,
      completed: 1,
      skipped: 0,
      failed: 0,
    });
  });

  it('counts a repair that fails', async () => {
    seedChannel({ lastMessageId: 12, oldestMessageId: 10 });
    service.insertMessagesTx([service._buildMessageRecord(CHANNEL_ID, message(10))]);
    telegramClient.getMessagesByChannelId.mockRejectedValue(new Error('CHANNEL_PRIVATE'));

    service.addRepairs({ chats: [CHANNEL_ID] });
    await service.processQueue();

    expect(stats.snapshot().sinceStart.repair).toMatchObject({ pages: 0, failed: 1 });
  });

  it('counts a repair of a chat no longer synced as failed, without reading', async () => {
    seedChannel({ lastMessageId: 12 });
    service.addRepairs({ chats: [CHANNEL_ID] });
    service.db.prepare('UPDATE channels SET sync_enabled = 0 WHERE channel_id = ?').run(CHANNEL_ID);
    await service.processQueue();

    expect(stats.snapshot().sinceStart.repair).toMatchObject({ pages: 0, completed: 0, failed: 1 });
    expect(telegramClient.getMessagesByChannelId).not.toHaveBeenCalled();
  });

  it('counts a repair of a chat that is gone as skipped', async () => {
    seedChannel({ lastMessageId: 12 });
    telegramClient.getMessagesByChannelId.mockRejectedValue(Object.assign(new Error('CHANNEL_PRIVATE'), { code: 400, text: 'CHANNEL_PRIVATE' }));

    service.addRepairs({ chats: [CHANNEL_ID] });
    await service.processQueue();

    expect(stats.snapshot().sinceStart.repair).toMatchObject({ pages: 0, skipped: 1, failed: 0 });
  });

  it('counts what a dry run would add apart from what a repair added', async () => {
    seedChannel({ lastMessageId: 12, oldestMessageId: 10 });
    service.insertMessagesTx([10, 12].map((id) => service._buildMessageRecord(CHANNEL_ID, message(id))));
    telegramClient.getMessagesByChannelId.mockImplementation(async (_, __, { offsetId }) => ({
      messages: [12, 11, 10].filter((id) => !offsetId || id < offsetId).map(message),
    }));

    service.addRepairs({ chats: [CHANNEL_ID], dryRun: true });
    await service.processQueue();

    expect(stats.snapshot().sinceStart.repair).toMatchObject({ pages: 2, checked: 3, added: 0, wouldAdd: 1, completed: 1 });
  });

  it('counts the pages of a gap fill, what they added, and the gap closing', async () => {
    seedChannel({ lastMessageId: 15, gapAfter: 10 });
    service.insertMessagesTx([10, 15].map((id) => service._buildMessageRecord(CHANNEL_ID, message(id))));
    telegramClient.getMessagesByChannelId.mockImplementation(async (_, __, { afterId }) => ({
      messages: [11, 12, 15].filter((id) => id > afterId).map(message),
    }));

    await service._fillChannelGap(CHANNEL_ID);

    expect(stats.snapshot().sinceStart.gaps).toEqual({ pages: 1, added: 2, closed: 1, failed: 0 });
  });

  it('counts a gap fill that fails', async () => {
    seedChannel({ lastMessageId: 15, gapAfter: 10 });
    telegramClient.getMessagesByChannelId.mockRejectedValue(new Error('network down'));

    await service._fillChannelGap(CHANNEL_ID);

    expect(stats.snapshot().sinceStart.gaps).toMatchObject({ pages: 0, closed: 0, failed: 1 });
  });

  it('counts job passes, finished jobs and failed ones', async () => {
    seedChannel({ lastMessageId: 15 });
    vi.spyOn(service, '_syncNewerMessages').mockResolvedValue({ hasMoreNewer: false, stoppedEarly: false });
    const backfill = vi.spyOn(service, '_backfillHistory')
      .mockResolvedValueOnce({ hasMoreOlder: true, finalCount: 1, stoppedEarly: false })
      .mockResolvedValueOnce({ hasMoreOlder: false, finalCount: 2, stoppedEarly: false });

    service.addJob(CHANNEL_ID);
    await service.processQueue();
    expect(stats.snapshot().sinceStart.jobs).toEqual({ runs: 2, completed: 1, failed: 0 });

    backfill.mockRejectedValueOnce(new Error('CHANNEL_PRIVATE'));
    service.addJob(CHANNEL_ID);
    await service.processQueue();
    expect(stats.snapshot().sinceStart.jobs).toEqual({ runs: 3, completed: 1, failed: 1 });
  });
});

describe('formatServerStats', () => {
  it('prints each counter since start and over the last hour', () => {
    const clock = makeClock();
    const stats = new ServerStats({ now: clock.now });
    stats.recordRequest('messages.getHistory');
    stats.recordError('messages.getHistory', rpcError(420, 'FLOOD_WAIT_12'));
    clock.advance(61 * MINUTE_MS);
    stats.recordRequest('messages.getHistory');
    stats.recordRequest('channels.getMessages');
    stats.count('rechecks.queued', 4);

    const text = formatServerStats({
      pid: 4321,
      version: '9.9.9',
      startedAt: new Date(START).toISOString(),
      ...stats.snapshot(),
    });
    const row = (label) => text.split('\n').find((line) => line.trimStart().startsWith(label))?.trim().split(/\s{2,}/);

    expect(text).toMatch(/^tgcli server 9\.9\.9, pid 4321, up 1h 1m$/m);
    expect(text).toMatch(/since start\s+last hour$/m);
    expect(row('Requests to Telegram')).toEqual(['Requests to Telegram', '3', '2']);
    expect(row('messages.getHistory')).toEqual(['messages.getHistory', '2', '1']);
    expect(row('channels.getMessages')).toEqual(['channels.getMessages', '1', '1']);
    expect(row('Request errors')).toEqual(['Request errors', '1', '0']);
    expect(row('FLOOD_WAIT messages.getHistory')).toEqual(['FLOOD_WAIT messages.getHistory', '1', '0']);
    expect(row('flood waits, seconds asked')).toEqual(['flood waits, seconds asked', '12', '0']);
    expect(text).not.toContain('Held back');
    expect(row('queued')).toEqual(['queued', '4', '4']);
    expect(text).toContain('Deleted messages read again');
    expect(text).toContain('backfill repair');
  });

  it('lists what mtcute held back, once there is some', async () => {
    const stats = new ServerStats({ now: () => START });
    const [outer] = stats.networkMiddlewares([]);
    await outer({ request: { _: 'messages.getHistory' } }, async () => rpcError(420, 'FLOOD_WAIT_9'));

    const text = formatServerStats({ pid: 1, version: '9.9.9', ...stats.snapshot() });
    const lines = text.split('\n').map((line) => line.trim().split(/\s{2,}/));

    expect(lines).toContainEqual(['Held back by mtcute (remembered flood wait)', '1', '1']);
    expect(lines).toContainEqual(['FLOOD_WAIT messages.getHistory', '1', '1']);
    expect(lines).toContainEqual(['Request errors', '0', '0']);
  });
});
