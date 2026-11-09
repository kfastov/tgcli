// Tests for the send operations (sendText/sendPhoto/sendFile) and how the control
// server relays their structured failures. The send ops wrap the existing
// send-utils retry/flood/timeout logic against the warm client, so retry and
// rate-limit behavior runs server-side; on failure the control server serializes
// the SendCommandError details so the CLI can render the same error.

import http from 'node:http';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { OPERATIONS } from '../core/ops/index.js';
import { executeOperation } from '../core/ops/execute.js';
import { SendCommandError } from '../core/send-utils.js';
import { createControlRequestHandler, CONTROL_TOKEN_HEADER } from '../core/control-server.js';

const TOKEN = 'send-token';

function startServer(handler) {
  const server = http.createServer((req, res) => void handler(req, res));
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => resolve({ server, port: server.address().port }));
  });
}

async function request(port, body) {
  const res = await fetch(`http://127.0.0.1:${port}/control/run`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', [CONTROL_TOKEN_HEADER]: TOKEN },
    body: JSON.stringify(body),
  });
  let json = null;
  try {
    json = await res.json();
  } catch {
    json = null;
  }
  return { status: res.status, json };
}

// Runs a send operation the way the server does: args validated, defaults applied.
function run(name, services, args) {
  return executeOperation(OPERATIONS[name], args, { services });
}

// A client send that reports its message through onSent, as the real client does.
function reportingSend(result) {
  return vi.fn(async (...args) => {
    args.at(-1).onSent({ id: result.messageId });
    return result;
  });
}

describe('send operations against the warm client', () => {
  const messageSyncService = { archiveSentMessage: vi.fn() };

  it('messagesSend returns the client result as its payload', async () => {
    const telegramClient = {
      sendTextMessage: reportingSend({ messageId: 55 }),
    };
    const result = await run('messagesSend', { telegramClient, messageSyncService }, {
      channelId: '@c',
      text: 'hi',
      topicId: 3,
      parseMode: 'markdown',
      silent: true,
    });
    expect(telegramClient.sendTextMessage).toHaveBeenCalledWith('@c', 'hi', expect.objectContaining({
      topicId: 3,
      parseMode: 'markdown',
      silent: true,
    }));
    expect(messageSyncService.archiveSentMessage).toHaveBeenCalledWith({ id: 55 });
    // One attempt is not worth reporting.
    expect(result).toEqual({ channelId: '@c', messageId: 55 });
  });

  it('retries a transient failure server-side and reports the attempt count', async () => {
    const send = reportingSend({ messageId: 88 });
    const telegramClient = {
      sendTextMessage: vi.fn()
        .mockRejectedValueOnce(new Error('A wait of 1 seconds is required'))
        .mockImplementationOnce(send),
    };
    const result = await run('messagesSend', { telegramClient, messageSyncService }, {
      channelId: '@c',
      text: 'hi',
      retries: 2,
      // Constant 0ms backoff keeps the retry instant for the test.
      retryBackoff: '0',
    });
    expect(telegramClient.sendTextMessage).toHaveBeenCalledTimes(2);
    expect(result).toEqual({ channelId: '@c', messageId: 88, attempts: 2 });
  });

  it('throws a SendCommandError when a FLOOD_WAIT exceeds the timeout budget', async () => {
    const telegramClient = {
      sendFileMessage: vi.fn().mockRejectedValue(new Error('FLOOD_WAIT_120')),
    };
    const error = await run('messagesSendFile', { telegramClient }, {
      channelId: '@c',
      filePath: '/tmp/x',
      retries: 2,
      timeoutMs: 30000,
    }).catch((e) => e);
    expect(error).toBeInstanceOf(SendCommandError);
    expect(error.details).toMatchObject({ type: 'rate_limit', method: 'sendFile', waitSeconds: 120 });
    expect(error.details.message).toContain('FLOOD_WAIT 120s');
  });

  it('sendPhoto prepares then sends the prepared media', async () => {
    const prepared = { method: 'sendPhoto' };
    const telegramClient = {
      preparePhotoMessage: vi.fn().mockResolvedValue(prepared),
      sendPreparedPhotoMessage: reportingSend({ chatId: '1', messageId: 9, method: 'sendPhoto' }),
    };
    const result = await run('messagesSendPhoto', { telegramClient, messageSyncService }, {
      channelId: '@c',
      photo: '/tmp/p.png',
      caption: 'hi',
    });
    expect(telegramClient.preparePhotoMessage).toHaveBeenCalledWith('@c', '/tmp/p.png', expect.objectContaining({ caption: 'hi' }));
    expect(telegramClient.sendPreparedPhotoMessage).toHaveBeenCalledWith(prepared, { onSent: expect.any(Function) });
    expect(result).toMatchObject({ ok: true, method: 'sendPhoto', chat_id: '1', message_id: 9, attempts: 1 });
  });
});

describe('/control/run send error relay', () => {
  let server;
  let port;
  let telegramClient;

  beforeEach(async () => {
    telegramClient = {
      sendTextMessage: reportingSend({ messageId: 1 }),
    };
    const handler = createControlRequestHandler({
      service: { getJobCounts: vi.fn(() => ({})) },
      warmServices: { telegramClient, messageSyncService: { archiveSentMessage: vi.fn() } },
      token: TOKEN,
      pid: 1,
      version: '1',
      startedAt: 's',
      ensureLogin: vi.fn(() => Promise.resolve()),
    });
    ({ server, port } = await startServer(handler));
  });

  afterEach(() => {
    server.close();
    vi.restoreAllMocks();
  });

  it('runs a send op and returns its result', async () => {
    const { status, json } = await request(port, {
      op: 'messagesSend',
      args: { channelId: '@c', text: 'hi' },
    });
    expect(status).toBe(200);
    expect(json).toEqual({ result: { channelId: '@c', messageId: 1 } });
  });

  it('relays a SendCommandError as a sendError payload on failure', async () => {
    telegramClient.sendTextMessage = vi.fn().mockRejectedValue(new Error('FLOOD_WAIT_120'));
    const { status, json } = await request(port, {
      op: 'messagesSend',
      args: { channelId: '@c', text: 'hi', retries: 0, timeoutMs: 30000 },
    });
    expect(status).toBe(500);
    expect(json.sendError).toMatchObject({ type: 'rate_limit', method: 'sendText', waitSeconds: 120 });
    expect(json.error).toContain('FLOOD_WAIT_120');
  });
});
