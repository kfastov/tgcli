// Runs the real mtcute send path and stubs only the network call: the other send
// tests mock client.sendText, so they could not notice mtcute ignoring a param name.
import { fileURLToPath } from 'node:url';

import { InputMedia } from '@mtcute/core';
import { sendMedia, sendText } from '@mtcute/core/methods.js';
import { describe, expect, it, vi } from 'vitest';

import TelegramClient from '../telegram-client.js';

const EXISTING_FILE = fileURLToPath(new URL('../package.json', import.meta.url));

function createClient() {
  const call = vi.fn(async (request) => {
    const message = { _: 'message', id: 7, date: request.scheduleDate ?? 0, peerId: { _: 'peerUser', userId: 1 }, message: 'hello' };
    return {
      _: 'updates',
      updates: [{ _: request.scheduleDate ? 'updateNewScheduledMessage' : 'updateNewMessage', message, pts: 1, ptsCount: 1 }],
      users: [],
      chats: [],
      date: 0,
      seq: 0,
    };
  });
  const raw = {
    call,
    handleClientUpdate: vi.fn(),
    timers: { cancel: vi.fn() },
    log: { warn: vi.fn(), debug: vi.fn() },
    getMtprotoMessageId: vi.fn(),
    storage: { self: { getCached: () => ({ userId: 1 }) } },
  };
  const tc = Object.create(TelegramClient.prototype);
  tc.ensureLogin = vi.fn().mockResolvedValue(undefined);
  tc.client = {
    sendText: (peer, text, params) => sendText(raw, peer, text, params),
    // Uploading is out of scope: a dice needs no upload and takes the same params.
    sendMedia: (peer, _media, params) => sendMedia(raw, peer, InputMedia.dice('🎲', {}), params),
  };
  return { tc, call };
}

const inOneHour = () => Math.floor(Date.now() / 1000) + 3600;

describe('--schedule through real mtcute', () => {
  it('sendTextMessage schedules with unix seconds and reports it', async () => {
    const { tc, call } = createClient();
    const scheduleDate = inOneHour();
    const result = await tc.sendTextMessage('me', 'hello', { scheduleDate });
    expect(call.mock.calls[0][0]).toMatchObject({ _: 'messages.sendMessage', scheduleDate });
    expect(result).toEqual({ messageId: 7, scheduled: true });
  });

  it('sendTextMessage without a schedule is not reported as scheduled', async () => {
    const { tc, call } = createClient();
    const result = await tc.sendTextMessage('me', 'hello');
    expect(call.mock.calls[0][0].scheduleDate).toBeUndefined();
    expect(result).toEqual({ messageId: 7 });
  });

  it('sendFileMessage schedules with unix seconds and reports it', async () => {
    const { tc, call } = createClient();
    const scheduleDate = inOneHour();
    const result = await tc.sendFileMessage('me', EXISTING_FILE, { scheduleDate });
    expect(call.mock.calls[0][0]).toMatchObject({ _: 'messages.sendMedia', scheduleDate });
    expect(result.scheduled).toBe(true);
  });

  it('sendFileMessage without a schedule is not reported as scheduled', async () => {
    const { tc } = createClient();
    const result = await tc.sendFileMessage('me', EXISTING_FILE);
    expect(result).not.toHaveProperty('scheduled');
  });
});
