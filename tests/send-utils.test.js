import { describe, expect, it, vi } from 'vitest';

import {
  buildSendErrorPayload,
  buildSendSuccessPayload,
  classifySendError,
  executeSendWithRetries,
  formatSendErrorMessage,
  getRetryDelayMs,
  parseRetryBackoff,
  SendCommandError,
} from '../core/send-utils.js';

describe('executeSendWithRetries', () => {
  it('retries once and succeeds on transient network error', async () => {
    const sendFn = vi.fn()
      .mockRejectedValueOnce(Object.assign(new Error('ECONNRESET'), { code: 'ECONNRESET' }))
      .mockResolvedValueOnce({ messageId: 456, media: { type: 'photo', fileId: 'file-123' } });
    const sleep = vi.fn().mockResolvedValue(undefined);

    const result = await executeSendWithRetries(sendFn, {
      method: 'sendPhoto',
      retries: 2,
      retryBackoff: parseRetryBackoff('25'),
      sleep,
    });

    expect(sendFn).toHaveBeenCalledTimes(2);
    expect(sleep).toHaveBeenCalledWith(25);
    expect(result).toEqual({
      result: { messageId: 456, media: { type: 'photo', fileId: 'file-123' } },
      attempts: 2,
    });
  });

  it('retries FLOOD_WAIT using the server-specified wait time', async () => {
    const sendFn = vi.fn()
      .mockRejectedValueOnce(new Error('A wait of 3 seconds is required'))
      .mockResolvedValueOnce({ messageId: 123 });
    const sleep = vi.fn().mockResolvedValue(undefined);

    const result = await executeSendWithRetries(sendFn, {
      method: 'sendPhoto',
      retries: 2,
      retryBackoff: parseRetryBackoff('10'),
      sleep,
    });

    expect(sendFn).toHaveBeenCalledTimes(2);
    expect(sleep).toHaveBeenCalledWith(3000);
    expect(result).toEqual({
      result: { messageId: 123 },
      attempts: 2,
    });
  });

  it('does not retry validation errors', async () => {
    const sendFn = vi.fn().mockRejectedValue(new Error('File not found: /tmp/missing.png'));

    await expect(
      executeSendWithRetries(sendFn, {
        method: 'sendPhoto',
        retries: 3,
        retryBackoff: parseRetryBackoff('constant'),
      }),
    ).rejects.toMatchObject({
      name: 'SendCommandError',
      details: expect.objectContaining({
        type: 'validation',
        method: 'sendPhoto',
        attempt: 1,
        retries: 3,
      }),
    });

    expect(sendFn).toHaveBeenCalledTimes(1);
  });

  it('does not retry non-transient telegram errors', async () => {
    const sendFn = vi.fn().mockRejectedValue(
      Object.assign(new Error('CHAT_WRITE_FORBIDDEN'), { code: 403 }),
    );

    await expect(
      executeSendWithRetries(sendFn, {
        method: 'sendPhoto',
        retries: 2,
        retryBackoff: parseRetryBackoff('linear'),
      }),
    ).rejects.toMatchObject({
      name: 'SendCommandError',
      details: expect.objectContaining({
        type: 'telegram',
        method: 'sendPhoto',
        attempt: 1,
        retries: 2,
        code: 403,
      }),
    });

    expect(sendFn).toHaveBeenCalledTimes(1);
  });

  it('retries mtcute transport errors even when they use numeric codes', async () => {
    const sendFn = vi.fn()
      .mockRejectedValueOnce(Object.assign(new Error('Transport error: 404'), { name: 'TransportError', code: 404 }))
      .mockResolvedValueOnce({ messageId: 789, media: { type: 'photo' } });
    const sleep = vi.fn().mockResolvedValue(undefined);

    const result = await executeSendWithRetries(sendFn, {
      method: 'sendPhoto',
      retries: 2,
      retryBackoff: parseRetryBackoff('10'),
      sleep,
    });

    expect(sendFn).toHaveBeenCalledTimes(2);
    expect(sleep).toHaveBeenCalledWith(10);
    expect(result).toEqual({
      result: { messageId: 789, media: { type: 'photo' } },
      attempts: 2,
    });
  });

  it('continues retrying when onRetry callback throws', async () => {
    const sendFn = vi.fn()
      .mockRejectedValueOnce(Object.assign(new Error('ECONNRESET'), { code: 'ECONNRESET' }))
      .mockResolvedValueOnce({ messageId: 100 });
    const sleep = vi.fn().mockResolvedValue(undefined);
    const onRetry = vi.fn(() => { throw new Error('callback crash'); });

    const result = await executeSendWithRetries(sendFn, {
      method: 'sendPhoto',
      retries: 2,
      retryBackoff: parseRetryBackoff('10'),
      sleep,
      onRetry,
    });

    expect(sendFn).toHaveBeenCalledTimes(2);
    expect(onRetry).toHaveBeenCalledTimes(1);
    expect(result).toEqual({ result: { messageId: 100 }, attempts: 2 });
  });

  it('logs onRetry callback errors to stderr instead of swallowing them', async () => {
    const sendFn = vi.fn()
      .mockRejectedValueOnce(Object.assign(new Error('ECONNRESET'), { code: 'ECONNRESET' }))
      .mockResolvedValueOnce({ messageId: 100 });
    const sleep = vi.fn().mockResolvedValue(undefined);
    const callbackError = new Error('bad callback');
    const onRetry = vi.fn(() => { throw callbackError; });
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    await executeSendWithRetries(sendFn, {
      method: 'sendPhoto',
      retries: 2,
      retryBackoff: parseRetryBackoff('10'),
      sleep,
      onRetry,
    });

    expect(errorSpy).toHaveBeenCalledWith(
      expect.stringContaining('onRetry'),
      callbackError,
    );
    errorSpy.mockRestore();
  });

  it('re-throws TypeError from onRetry callback instead of swallowing it', async () => {
    const sendFn = vi.fn()
      .mockRejectedValueOnce(Object.assign(new Error('ECONNRESET'), { code: 'ECONNRESET' }))
      .mockResolvedValueOnce({ messageId: 100 });
    const sleep = vi.fn().mockResolvedValue(undefined);
    const onRetry = vi.fn(() => { throw new TypeError('x is not a function'); });

    await expect(
      executeSendWithRetries(sendFn, {
        method: 'sendPhoto',
        retries: 2,
        retryBackoff: parseRetryBackoff('10'),
        sleep,
        onRetry,
      }),
    ).rejects.toBeInstanceOf(TypeError);
  });

  it('throws timeout before first attempt when budget already expired', async () => {
    let currentTime = 100;
    const sendFn = vi.fn();
    const sleep = vi.fn();

    await expect(
      executeSendWithRetries(sendFn, {
        method: 'sendPhoto',
        retries: 2,
        timeoutMs: 1,
        now: () => { currentTime += 10; return currentTime; },
        sleep,
      }),
    ).rejects.toMatchObject({
      name: 'SendCommandError',
      details: expect.objectContaining({
        type: 'timeout',
        attempt: 1,
        retries: 2,
      }),
    });

    expect(sendFn).not.toHaveBeenCalled();
  });

  it('throws timeout when budget expires before second attempt', async () => {
    let currentTime = 0;
    const now = vi.fn(() => currentTime);
    const sleep = vi.fn(async (ms) => { currentTime += ms; });
    const sendFn = vi.fn(async () => {
      currentTime += 200;
      throw Object.assign(new Error('ECONNRESET'), { code: 'ECONNRESET' });
    });

    await expect(
      executeSendWithRetries(sendFn, {
        method: 'sendPhoto',
        retries: 2,
        retryBackoff: parseRetryBackoff('50'),
        timeoutMs: 200,
        sleep,
        now,
      }),
    ).rejects.toMatchObject({
      name: 'SendCommandError',
      details: expect.objectContaining({
        type: 'timeout',
        attempt: 1,
        retries: 2,
      }),
    });

    expect(sendFn).toHaveBeenCalledTimes(1);
  });

  it('fails immediately with retries: 0 and does not sleep', async () => {
    const sendFn = vi.fn().mockRejectedValue(Object.assign(new Error('ECONNRESET'), { code: 'ECONNRESET' }));
    const sleep = vi.fn().mockResolvedValue(undefined);

    await expect(
      executeSendWithRetries(sendFn, {
        method: 'sendPhoto',
        retries: 0,
        retryBackoff: parseRetryBackoff('10'),
        sleep,
      }),
    ).rejects.toMatchObject({
      name: 'SendCommandError',
      details: expect.objectContaining({
        type: 'network',
        attempt: 1,
        retries: 0,
      }),
    });

    expect(sendFn).toHaveBeenCalledTimes(1);
    expect(sleep).not.toHaveBeenCalled();
  });

  it('does not retry FLOOD_WAIT when maxWaitSeconds is exceeded', async () => {
    const sendFn = vi.fn().mockRejectedValue(new Error('FLOOD_WAIT_301'));
    const sleep = vi.fn().mockResolvedValue(undefined);

    await expect(
      executeSendWithRetries(sendFn, {
        method: 'sendPhoto',
        retries: 2,
        maxWaitSeconds: 300,
        sleep,
      }),
    ).rejects.toMatchObject({
      name: 'SendCommandError',
      details: expect.objectContaining({
        type: 'rate_limit',
        attempt: 1,
        retries: 2,
        retryable: true,
        waitSeconds: 301,
      }),
    });

    expect(sendFn).toHaveBeenCalledTimes(1);
    expect(sleep).not.toHaveBeenCalled();
  });

  it('coerces invalid retries (negative, fractional) to 0', async () => {
    const sendFn = vi.fn().mockRejectedValue(Object.assign(new Error('ECONNRESET'), { code: 'ECONNRESET' }));
    const sleep = vi.fn().mockResolvedValue(undefined);

    for (const retries of [-1, 1.5, NaN]) {
      sendFn.mockClear();
      sleep.mockClear();

      await expect(
        executeSendWithRetries(sendFn, { method: 'sendPhoto', retries, sleep }),
      ).rejects.toMatchObject({ name: 'SendCommandError' });

      expect(sendFn).toHaveBeenCalledTimes(1);
      expect(sleep).not.toHaveBeenCalled();
    }
  });

  it('throws SendCommandError after exhausting all retries', async () => {
    const sendFn = vi.fn().mockRejectedValue(Object.assign(new Error('ECONNRESET'), { code: 'ECONNRESET' }));
    const sleep = vi.fn().mockResolvedValue(undefined);

    await expect(
      executeSendWithRetries(sendFn, {
        method: 'sendPhoto',
        retries: 2,
        retryBackoff: parseRetryBackoff('10'),
        sleep,
      }),
    ).rejects.toMatchObject({
      name: 'SendCommandError',
      details: expect.objectContaining({
        type: 'network',
        method: 'sendPhoto',
        attempt: 3,
        retries: 2,
        retryable: true,
      }),
    });

    expect(sendFn).toHaveBeenCalledTimes(3);
    expect(sleep).toHaveBeenCalledTimes(2);
  });

  it('stops retrying when the timeout budget is exhausted during backoff', async () => {
    let currentTime = 0;
    const now = vi.fn(() => currentTime);
    const sleep = vi.fn(async (ms) => {
      currentTime += ms;
    });
    const sendFn = vi.fn().mockRejectedValue(Object.assign(new Error('ECONNRESET'), { code: 'ECONNRESET' }));

    await expect(
      executeSendWithRetries(sendFn, {
        method: 'sendPhoto',
        retries: 2,
        retryBackoff: parseRetryBackoff('100'),
        timeoutMs: 100,
        sleep,
        now,
      }),
    ).rejects.toMatchObject({
      name: 'SendCommandError',
      details: expect.objectContaining({
        type: 'timeout',
        method: 'sendPhoto',
        attempt: 1,
        retries: 2,
      }),
    });

    expect(sendFn).toHaveBeenCalledTimes(1);
    expect(sleep).toHaveBeenCalledWith(100);
  });

  it('clamps sleep duration to remaining timeout budget', async () => {
    let currentTime = 0;
    const now = vi.fn(() => currentTime);
    const sleep = vi.fn(async (ms) => { currentTime += ms - 1; });
    const sendFn = vi.fn()
      .mockImplementationOnce(async () => { currentTime += 80; throw Object.assign(new Error('ECONNRESET'), { code: 'ECONNRESET' }); })
      .mockResolvedValueOnce({ messageId: 1 });

    await executeSendWithRetries(sendFn, {
      method: 'sendPhoto',
      retries: 2,
      retryBackoff: parseRetryBackoff('500'),
      timeoutMs: 200,
      sleep,
      now,
    });

    expect(sleep).toHaveBeenCalledWith(120);
  });
});

describe('send payload builders', () => {
  it('creates structured JSON success payload', () => {
    expect(
      buildSendSuccessPayload({
        method: 'sendPhoto',
        chatId: 123,
        messageId: 456,
        media: { type: 'photo', fileId: 'file-123' },
        attempts: 2,
      }),
    ).toEqual({
      ok: true,
      method: 'sendPhoto',
      chat_id: 123,
      message_id: 456,
      media: { type: 'photo', file_id: 'file-123' },
      attempts: 2,
    });
  });

  it('includes warning in success payload when provided', () => {
    const result = buildSendSuccessPayload({
      method: 'sendPhoto',
      chatId: 123,
      messageId: 456,
      media: { type: 'photo' },
      attempts: 1,
      warning: 'Media enrichment failed; file_id unavailable',
    });
    expect(result.warning).toBe('Media enrichment failed; file_id unavailable');
  });

  it('omits warning from success payload when not provided', () => {
    const result = buildSendSuccessPayload({
      method: 'sendPhoto',
      chatId: 123,
      messageId: 456,
      attempts: 1,
    });
    expect(result).not.toHaveProperty('warning');
  });

  it('includes media with only type when fileId is absent', () => {
    const result = buildSendSuccessPayload({
      method: 'sendPhoto',
      chatId: 123,
      messageId: 456,
      media: { type: 'photo' },
      attempts: 1,
    });
    expect(result.media).toEqual({ type: 'photo' });
    expect(result.media).not.toHaveProperty('file_id');
  });

  it('omits media field when media is absent or empty', () => {
    const withoutMedia = buildSendSuccessPayload({
      method: 'sendPhoto',
      chatId: 123,
      messageId: 456,
      attempts: 1,
    });
    expect(withoutMedia).not.toHaveProperty('media');

    const withEmptyMedia = buildSendSuccessPayload({
      method: 'sendPhoto',
      chatId: 123,
      messageId: 456,
      media: {},
      attempts: 1,
    });
    expect(withEmptyMedia).not.toHaveProperty('media');
  });

  it('includes code:0 in error payload (falsy but valid)', () => {
    const result = buildSendErrorPayload({
      type: 'telegram',
      method: 'sendPhoto',
      message: 'Some error',
      code: 0,
      attempt: 1,
      retries: 0,
    });
    expect(result.error.code).toBe(0);
  });

  it('creates structured JSON error payload', () => {
    const error = new SendCommandError({
      type: 'network',
      method: 'sendPhoto',
      message: 'ECONNRESET',
      code: 'ECONNRESET',
      attempt: 2,
      retries: 3,
    });

    expect(buildSendErrorPayload(error.details)).toEqual({
      ok: false,
      error: {
        type: 'network',
        method: 'sendPhoto',
        message: 'ECONNRESET',
        code: 'ECONNRESET',
        attempt: 2,
        retries: 3,
      },
    });
  });
});

describe('classifySendError', () => {
  it('returns existing details for SendCommandError', () => {
    const details = { type: 'validation', method: 'sendPhoto', message: 'bad', attempt: 1, retries: 0 };
    const result = classifySendError(new SendCommandError(details));
    expect(result).toBe(details);
  });

  it('classifies ENOENT as validation error', () => {
    const error = Object.assign(new Error('ENOENT: no such file'), { code: 'ENOENT' });
    const result = classifySendError(error, { method: 'sendPhoto' });
    expect(result).toMatchObject({ type: 'validation', retryable: false });
  });

  it('classifies "File not found:" message as validation error', () => {
    const result = classifySendError(new Error('File not found: /tmp/x.png'), { method: 'sendPhoto' });
    expect(result).toMatchObject({ type: 'validation', retryable: false });
  });

  it('classifies ETIMEDOUT as retryable timeout', () => {
    const error = Object.assign(new Error('connect ETIMEDOUT'), { code: 'ETIMEDOUT' });
    const result = classifySendError(error, { method: 'sendPhoto' });
    expect(result).toMatchObject({ type: 'timeout', retryable: true, code: 'ETIMEDOUT' });
  });

  it('classifies ETIMEDOUT code with bare "Timeout" message as retryable (code takes precedence)', () => {
    const error = Object.assign(new Error('Timeout'), { code: 'ETIMEDOUT' });
    const result = classifySendError(error, { method: 'sendPhoto' });
    expect(result).toMatchObject({ type: 'timeout', retryable: true, code: 'ETIMEDOUT' });
  });

  it('classifies bare "timeout" message as non-retryable timeout', () => {
    const result = classifySendError(new Error('Timeout'), { method: 'sendPhoto' });
    expect(result).toMatchObject({ type: 'timeout', retryable: false });
  });

  it('classifies "timed out" message without code as retryable timeout', () => {
    const result = classifySendError(new Error('connection timed out'), { method: 'sendPhoto' });
    expect(result).toMatchObject({ type: 'timeout', retryable: true });
  });

  it('classifies "Request timeout" substring as retryable timeout', () => {
    const result = classifySendError(new Error('Request timeout exceeded'), { method: 'sendPhoto' });
    expect(result).toMatchObject({ type: 'timeout', retryable: true });
  });

  it('classifies MtRpcError by name as telegram error', () => {
    const error = Object.assign(new Error('PEER_ID_INVALID'), { name: 'MtRpcError' });
    const result = classifySendError(error, { method: 'sendPhoto' });
    expect(result).toMatchObject({ type: 'telegram', retryable: false });
  });

  it('classifies ECONNRESET as retryable network error', () => {
    const error = Object.assign(new Error('ECONNRESET'), { code: 'ECONNRESET' });
    const result = classifySendError(error, { method: 'sendPhoto' });
    expect(result).toMatchObject({ type: 'network', retryable: true });
  });

  it('classifies TransportError with numeric code as retryable network', () => {
    const error = Object.assign(new Error('Transport error: 404'), { name: 'TransportError', code: 404 });
    const result = classifySendError(error, { method: 'sendPhoto' });
    expect(result).toMatchObject({ type: 'network', retryable: true });
  });

  it('classifies numeric code (non-transport) as non-retryable telegram error', () => {
    const error = Object.assign(new Error('CHAT_WRITE_FORBIDDEN'), { code: 403 });
    const result = classifySendError(error, { method: 'sendPhoto' });
    expect(result).toMatchObject({ type: 'telegram', retryable: false, code: 403 });
  });

  it('classifies FLOOD_WAIT as retryable rate_limit error', () => {
    const result = classifySendError(new Error('FLOOD_WAIT_30'), { method: 'sendPhoto' });
    expect(result).toMatchObject({ type: 'rate_limit', retryable: true, waitSeconds: 30 });
  });

  it('classifies RpcError by name as telegram error', () => {
    const error = Object.assign(new Error('PEER_ID_INVALID'), { name: 'RpcError' });
    const result = classifySendError(error, { method: 'sendPhoto' });
    expect(result).toMatchObject({ type: 'telegram', retryable: false });
  });

  it('classifies unknown errors as non-retryable unknown fallback', () => {
    const result = classifySendError(new Error('something weird'), { method: 'sendPhoto' });
    expect(result).toMatchObject({ type: 'unknown', retryable: false });
  });

  it('handles null and primitive error arguments', () => {
    const nullResult = classifySendError(null, { method: 'sendPhoto' });
    expect(nullResult).toMatchObject({ type: 'unknown', message: 'Unknown error' });

    const stringResult = classifySendError('raw string error', { method: 'sendPhoto' });
    expect(stringResult).toMatchObject({ type: 'unknown', message: 'raw string error' });
  });

  it('passes method, attempt, and retries through', () => {
    const result = classifySendError(new Error('ECONNRESET'), { method: 'sendPhoto', attempt: 3, retries: 5 });
    expect(result.method).toBe('sendPhoto');
    expect(result.attempt).toBe(3);
    expect(result.retries).toBe(5);
  });
});

describe('getRetryDelayMs', () => {
  it('returns constant baseMs regardless of attempt', () => {
    const backoff = parseRetryBackoff('500');
    expect(getRetryDelayMs(backoff, 1)).toBe(500);
    expect(getRetryDelayMs(backoff, 3)).toBe(500);
  });

  it('returns linear baseMs * attempt', () => {
    const backoff = parseRetryBackoff('linear');
    expect(getRetryDelayMs(backoff, 1)).toBe(1000);
    expect(getRetryDelayMs(backoff, 3)).toBe(3000);
  });

  it('returns exponential baseMs * 2^(attempt-1)', () => {
    const backoff = parseRetryBackoff('exponential');
    expect(getRetryDelayMs(backoff, 1)).toBe(1000);
    expect(getRetryDelayMs(backoff, 2)).toBe(2000);
    expect(getRetryDelayMs(backoff, 3)).toBe(4000);
  });

  it('falls back to constant for undefined backoff', () => {
    expect(getRetryDelayMs(undefined, 2)).toBe(1000);
  });

  it('works with explicit object not produced by parseRetryBackoff', () => {
    expect(getRetryDelayMs({ kind: 'exponential', baseMs: 500 }, 3)).toBe(2000);
    expect(getRetryDelayMs({ kind: 'linear', baseMs: 200 }, 4)).toBe(800);
    expect(getRetryDelayMs({ kind: 'constant', baseMs: 300 }, 5)).toBe(300);
  });
});

describe('parseRetryBackoff validation', () => {
  it('throws on invalid string input', () => {
    expect(() => parseRetryBackoff('invalid')).toThrow(
      '--retry-backoff must be a non-negative integer or one of: constant, linear, exponential',
    );
  });

  it('throws on negative numeric string', () => {
    expect(() => parseRetryBackoff('-5')).toThrow(
      '--retry-backoff must be a non-negative integer or one of: constant, linear, exponential',
    );
  });
});

describe('formatSendErrorMessage', () => {
  it('formats error details into human-readable string', () => {
    const msg = formatSendErrorMessage({
      type: 'network',
      method: 'sendPhoto',
      message: 'ECONNRESET',
      code: 'ECONNRESET',
      attempt: 2,
      retries: 3,
    });
    expect(msg).toBe('sendPhoto failed [network]: ECONNRESET (attempt 2/4, code ECONNRESET)');
  });

  it('omits code suffix when code is absent', () => {
    const msg = formatSendErrorMessage({
      type: 'timeout',
      method: 'sendPhoto',
      message: 'Timeout',
      attempt: 1,
      retries: 0,
    });
    expect(msg).toBe('sendPhoto failed [timeout]: Timeout (attempt 1/1)');
  });

  it('uses defaults for missing fields', () => {
    const msg = formatSendErrorMessage({});
    expect(msg).toBe('sendMedia failed [unknown]: Unknown error (attempt 1/1)');
  });
});
