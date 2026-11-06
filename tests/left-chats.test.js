// Chats the account has left: Telegram sends nothing from them, so the server asks
// Telegram about synced channels and groups missing from the dialog list and turns their
// sync off only on its word, and back on when the chat returns to the dialog list.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { Long, MtPeerNotFoundError, tl } from '@mtcute/core';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import MessageSyncService from '../message-sync-service.js';
import TelegramClient from '../telegram-client.js';

const rpcError = (errorCode, errorMessage) => tl.RpcError.fromTl({ _: 'rpc_error', errorCode, errorMessage });
const hash = Long.fromNumber(1);

describe('membership check in the Telegram client', () => {
  let client;
  let calls;
  let answer;

  beforeEach(() => {
    calls = [];
    client = Object.create(TelegramClient.prototype);
    client.ensureLogin = vi.fn().mockResolvedValue(undefined);
    client.client = {
      resolvePeer: async (id) => {
        if (id === -1000000000999) {
          throw new MtPeerNotFoundError(`Peer ${id} is not found in local cache`);
        }
        return id <= -1000000000000
          ? { _: 'inputPeerChannel', channelId: -1000000000000 - id, accessHash: hash }
          : { _: 'inputPeerChat', chatId: -id };
      },
      call: async (request) => {
        calls.push(request);
        return answer(request);
      },
    };
  });

  it('reads joined, left, removed and gone channels and groups, and names what is not cached', async () => {
    answer = (request) => (request._ === 'channels.getChannels'
      ? {
        chats: [
          { _: 'channel', id: 1, left: false },
          { _: 'channel', id: 2, left: true },
          { _: 'channelForbidden', id: 3 },
        ],
      }
      : {
        chats: [
          { _: 'chat', id: 10, left: false },
          { _: 'chat', id: 11, deactivated: true },
          { _: 'chat', id: 12, migratedTo: { _: 'inputChannel', channelId: 5, accessHash: hash } },
          { _: 'chatForbidden', id: 13 },
        ],
      });

    const membership = await client.getChatMembership([
      '-1000000000001', '-1000000000002', '-1000000000003', '-1000000000004', '-1000000000999',
      '-10', '-11', '-12', '-13',
    ]);

    expect(Object.fromEntries(membership)).toEqual({
      '-1000000000001': 'member',
      '-1000000000002': 'left',
      '-1000000000003': 'gone',
      // Asked about, but not in the answer: nothing to go on.
      '-1000000000004': 'unknown',
      '-1000000000999': 'unknown',
      '-10': 'member',
      '-11': 'gone',
      '-12': 'gone',
      '-13': 'gone',
    });
    expect(calls.map((request) => request._)).toEqual(['channels.getChannels', 'messages.getChats']);
  });

  it('asks about 100 chats a request', async () => {
    const ids = Array.from({ length: 150 }, (_, index) => String(-1000000000001 - index));
    answer = (request) => ({ chats: request.id.map((input) => ({ _: 'channel', id: input.channelId, left: false })) });

    const membership = await client.getChatMembership(ids);

    expect(calls.map((request) => request.id.length)).toEqual([100, 50]);
    expect([...membership.values()].every((state) => state === 'member')).toBe(true);
  });

  it('asks one by one when one refused chat fails the batch', async () => {
    answer = (request) => {
      if (request.id.some((input) => input.channelId === 2)) {
        throw rpcError(400, 'CHANNEL_PRIVATE');
      }
      return { chats: request.id.map((input) => ({ _: 'channel', id: input.channelId, left: false })) };
    };

    const membership = await client.getChatMembership(['-1000000000001', '-1000000000002']);

    // A refusal of one chat may be a stale access hash, not a left chat: no answer.
    expect(Object.fromEntries(membership)).toEqual({ '-1000000000001': 'member', '-1000000000002': 'unknown' });
  });

  it('takes an id mtcute cannot resolve as unknown, and asks about the rest', async () => {
    const resolvePeer = client.client.resolvePeer;
    client.client.resolvePeer = async (id) => {
      if (Number.isNaN(id)) {
        throw new Error('Invalid marked peer id');
      }
      return resolvePeer(id);
    };
    answer = () => ({ chats: [{ _: 'channel', id: 1, left: false }] });

    const membership = await client.getChatMembership(['-100123abc', '-1000000000001']);

    expect(Object.fromEntries(membership)).toEqual({ '-100123abc': 'unknown', '-1000000000001': 'member' });
  });

  it('fails on an error that is not about a chat, so nothing is decided on it', async () => {
    answer = () => {
      throw rpcError(406, 'AUTH_KEY_DUPLICATED');
    };

    await expect(client.getChatMembership(['-1000000000001'])).rejects.toThrow();
  });
});

describe('sync of chats missing from the dialog list', () => {
  const JOINED = '-1000000000001';
  const LEFT = '-1000000000002';
  const GONE = '-1000000000003';
  const UNKNOWN = '-1000000000004';
  const PRIVATE = '42';
  const IN_DIALOGS = '-1000000000005';

  let storeDir;
  let service;
  let telegramClient;
  let membership;

  const channel = (channelId) => service.db.prepare(`
    SELECT sync_enabled, sync_off_reason, membership, in_dialogs, gap_after_message_id FROM channels WHERE channel_id = ?
  `).get(channelId);

  const dialogs = (...ids) => {
    telegramClient.listDialogs.mockResolvedValue(ids.map(({ id, top = 0 }) => ({ id, title: id, topMessageId: top })));
  };

  beforeEach(() => {
    storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-left-chats-'));
    membership = new Map([[JOINED, 'member'], [LEFT, 'left'], [GONE, 'gone'], [UNKNOWN, 'unknown']]);
    telegramClient = {
      listDialogs: vi.fn(),
      getChatMembership: vi.fn(async (ids) => new Map(ids.map((id) => [id, membership.get(id) ?? 'unknown']))),
    };
    service = new MessageSyncService(telegramClient, { dbPath: path.join(storeDir, 'messages.db') });
    const insert = service.db.prepare('INSERT INTO channels (channel_id, sync_enabled, last_message_id) VALUES (?, 1, 10)');
    for (const id of [JOINED, LEFT, GONE, UNKNOWN, PRIVATE, IN_DIALOGS]) {
      insert.run(id);
    }
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'warn').mockImplementation(() => {});
  });

  afterEach(() => {
    service?.close();
    fs.rmSync(storeDir, { recursive: true, force: true });
    vi.restoreAllMocks();
  });

  it('turns sync off only where Telegram says the account left or the chat is gone', async () => {
    dialogs({ id: IN_DIALOGS });
    await service.refreshChannelsFromDialogs();

    const counts = await service.checkChatsMissingFromDialogs();

    expect(counts).toEqual({ off: 2, member: 1, unknown: 1 });
    expect(telegramClient.getChatMembership).toHaveBeenCalledWith([JOINED, LEFT, GONE, UNKNOWN]);
    expect(channel(LEFT)).toMatchObject({ sync_enabled: 0, sync_off_reason: 'left', membership: 'left' });
    expect(channel(GONE)).toMatchObject({ sync_enabled: 0, sync_off_reason: 'left', membership: 'gone' });
    expect(channel(JOINED)).toMatchObject({ sync_enabled: 1, sync_off_reason: null, membership: 'member' });
    expect(channel(UNKNOWN)).toMatchObject({ sync_enabled: 1, sync_off_reason: null, membership: 'unknown' });
    // Private chats get their messages without a dialog; nobody asks about them.
    expect(channel(PRIVATE)).toMatchObject({ sync_enabled: 1, membership: null });
    expect(console.warn.mock.calls.flat().join(' ')).toContain(JOINED);
    expect(service.getLeftChatCounts()).toEqual({ syncOff: 2, stillJoined: 1 });
  });

  it('asks again about a chat still joined a day later, not on every comparison', async () => {
    const now = vi.spyOn(Date, 'now').mockReturnValue(1_800_000_000_000);
    dialogs({ id: IN_DIALOGS });
    await service.refreshChannelsFromDialogs();
    await service.checkChatsMissingFromDialogs();
    telegramClient.getChatMembership.mockClear();

    now.mockReturnValue(1_800_000_000_000 + 60 * 60 * 1000);
    expect(await service.checkChatsMissingFromDialogs()).toBeNull();
    expect(telegramClient.getChatMembership).not.toHaveBeenCalled();

    now.mockReturnValue(1_800_000_000_000 + 25 * 60 * 60 * 1000);
    await service.checkChatsMissingFromDialogs();
    expect(telegramClient.getChatMembership).toHaveBeenCalledWith([JOINED, UNKNOWN]);
  });

  it('turns sync back on when a left chat returns to the dialog list, and fills what it missed', async () => {
    dialogs({ id: IN_DIALOGS });
    await service.refreshChannelsFromDialogs();
    await service.checkChatsMissingFromDialogs();

    dialogs({ id: IN_DIALOGS }, { id: LEFT, top: 25 });
    await service.refreshChannelsFromDialogs();

    expect(channel(LEFT)).toMatchObject({ sync_enabled: 1, sync_off_reason: null, membership: null, gap_after_message_id: 10 });
  });

  it('leaves alone a chat the user turned off', async () => {
    service.setChannelSync(LEFT, false);
    dialogs({ id: IN_DIALOGS }, { id: LEFT, top: 25 });

    await service.refreshChannelsFromDialogs();
    await service.checkChatsMissingFromDialogs();

    expect(channel(LEFT)).toMatchObject({ sync_enabled: 0, sync_off_reason: null });
  });

  it('forgets the reason when the user turns sync on or off by hand', async () => {
    dialogs({ id: IN_DIALOGS });
    await service.refreshChannelsFromDialogs();
    await service.checkChatsMissingFromDialogs();

    service.setChannelSync(LEFT, true);

    expect(channel(LEFT)).toMatchObject({ sync_enabled: 1, sync_off_reason: null, membership: null });
  });

  // Telegram calls a public channel archived without joining left, and the user asked for it.
  it('never turns off a chat the user asked for, by watch or by backfill', async () => {
    service.setChannelSync(LEFT, true);
    service.addJob(GONE);
    dialogs({ id: IN_DIALOGS });

    await service.refreshChannelsFromDialogs();
    await service.checkChatsMissingFromDialogs();

    expect(telegramClient.getChatMembership).toHaveBeenCalledWith([JOINED, UNKNOWN]);
    expect(channel(LEFT)).toMatchObject({ sync_enabled: 1, sync_off_reason: null });
    expect(channel(GONE)).toMatchObject({ sync_enabled: 1, sync_off_reason: null });
  });

  it('turns sync back on when the user asks to backfill a chat the server turned off', async () => {
    dialogs({ id: IN_DIALOGS });
    await service.refreshChannelsFromDialogs();
    await service.checkChatsMissingFromDialogs();

    service.addJob(LEFT);

    const row = service.db.prepare('SELECT sync_enabled, sync_off_reason, sync_by_user FROM channels WHERE channel_id = ?').get(LEFT);
    expect(row).toEqual({ sync_enabled: 1, sync_off_reason: null, sync_by_user: 1 });
  });

  it('forgets that a chat was still joined once it is back in the dialog list', async () => {
    dialogs({ id: IN_DIALOGS });
    await service.refreshChannelsFromDialogs();
    await service.checkChatsMissingFromDialogs();
    expect(channel(JOINED).membership).toBe('member');

    dialogs({ id: IN_DIALOGS }, { id: JOINED });
    await service.refreshChannelsFromDialogs();
    expect(channel(JOINED).membership).toBeNull();

    membership.set(JOINED, 'left');
    dialogs({ id: IN_DIALOGS });
    await service.refreshChannelsFromDialogs();
    await service.checkChatsMissingFromDialogs();
    expect(channel(JOINED)).toMatchObject({ sync_enabled: 0, sync_off_reason: 'left' });
  });

  it('asks once when two comparisons come close together', async () => {
    dialogs({ id: IN_DIALOGS });
    await service.refreshChannelsFromDialogs();

    await Promise.all([service.checkChatsMissingFromDialogs(), service.checkChatsMissingFromDialogs()]);

    expect(telegramClient.getChatMembership).toHaveBeenCalledTimes(1);
  });

  it('does not ask about ids that are not a chat id', async () => {
    service.db.prepare('INSERT INTO channels (channel_id, sync_enabled) VALUES (?, 1)').run('-100123abc');
    dialogs({ id: IN_DIALOGS });
    await service.refreshChannelsFromDialogs();

    await service.checkChatsMissingFromDialogs();

    expect(telegramClient.getChatMembership.mock.calls[0][0]).not.toContain('-100123abc');
  });

  it('asks after the comparison with the dialog list, and a failed answer changes nothing', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    telegramClient.getChatMembership.mockRejectedValueOnce(rpcError(500, 'INTERNAL'));
    dialogs({ id: IN_DIALOGS });

    await service.reconcileWithDialogs();
    await vi.waitFor(() => expect(console.error).toHaveBeenCalled());

    expect(channel(LEFT)).toMatchObject({ sync_enabled: 1, membership: null });
    await service.checkChatsMissingFromDialogs();
    expect(channel(LEFT)).toMatchObject({ sync_enabled: 0, sync_off_reason: 'left' });
  });
});
