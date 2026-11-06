// ensureLogin runs before every page of history and every recheck. Asking Telegram
// (getMe) each time doubled the requests of a backfill or a repair, so a confirmed
// session stands for five minutes.
import { tl } from '@mtcute/core';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import TelegramClient from '../telegram-client.js';

const MINUTE = 60 * 1000;

describe('login check', () => {
  let client;
  let getMe;
  let now;

  beforeEach(() => {
    now = vi.spyOn(Date, 'now').mockReturnValue(1_800_000_000_000);
    getMe = vi.fn().mockResolvedValue({ id: 1 });
    client = Object.create(TelegramClient.prototype);
    client.client = { getMe };
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('asks Telegram once in five minutes, however often it is called', async () => {
    for (let index = 0; index < 50; index += 1) {
      await client.ensureLogin();
    }
    expect(getMe).toHaveBeenCalledTimes(1);

    now.mockReturnValue(1_800_000_000_000 + 5 * MINUTE + 1);
    await client.ensureLogin();
    expect(getMe).toHaveBeenCalledTimes(2);
  });

  it('asks again right after a session Telegram refused', async () => {
    getMe.mockRejectedValueOnce(tl.RpcError.fromTl({ _: 'rpc_error', errorCode: 401, errorMessage: 'AUTH_KEY_UNREGISTERED' }));

    await expect(client.ensureLogin()).rejects.toThrow('Not logged in to Telegram');
    await client.ensureLogin();

    expect(getMe).toHaveBeenCalledTimes(2);
  });

  it('asks again after a failed check that was not about the session', async () => {
    getMe.mockRejectedValueOnce(new Error('Connection lost'));

    await expect(client.ensureLogin()).rejects.toThrow('Connection lost');
    await client.ensureLogin();

    expect(getMe).toHaveBeenCalledTimes(2);
  });

  it('always asks for commands that report the state', async () => {
    await client.ensureLogin();
    await client.isAuthorized();
    await client.isAuthorized();

    expect(getMe).toHaveBeenCalledTimes(3);
  });

  it('forgets the answer when the client is replaced', async () => {
    client._createClient = () => ({ getMe });
    client.client.destroy = vi.fn().mockResolvedValue(undefined);
    client.updateEmitter = { emit: vi.fn() };
    await client.ensureLogin();

    await client._recreateClient();
    await client.ensureLogin();

    expect(getMe).toHaveBeenCalledTimes(2);
  });
});
