import { describe, expect, it, vi } from 'vitest';

import {
  classifyError,
  computeRetryWaitSeconds,
  executeSendWithRetries,
  parseRetryBackoff,
} from '../core/send-utils.js';

describe('classifyError compatibility export', () => {
  it('classifies FLOOD_WAIT as retryable rate_limit', () => {
    const error = new Error('FLOOD_WAIT_30');
    const result = classifyError(error, { method: 'sendText' });

    expect(result).toMatchObject({
      type: 'rate_limit',
      method: 'sendText',
      message: 'FLOOD_WAIT_30',
      retryable: true,
      waitSeconds: 30,
    });
  });

  it('classifies "wait of N seconds" as retryable rate_limit', () => {
    const error = new Error('A wait of 60 seconds is required');
    const result = classifyError(error, { method: 'sendText' });

    expect(result).toMatchObject({
      type: 'rate_limit',
      retryable: true,
      waitSeconds: 60,
    });
  });
});

describe('computeRetryWaitSeconds compatibility export', () => {
  it('returns parsed seconds for FLOOD_WAIT errors', () => {
    expect(computeRetryWaitSeconds(new Error('FLOOD_WAIT_30'), 1)).toBe(30);
    expect(computeRetryWaitSeconds(new Error('A wait of 60 seconds is required'), 1)).toBe(60);
  });

  it('uses exponential backoff seconds for non-rate-limit errors', () => {
    const error = new Error('ECONNRESET');

    expect(computeRetryWaitSeconds(error, 1)).toBe(1);
    expect(computeRetryWaitSeconds(error, 2)).toBe(2);
    expect(computeRetryWaitSeconds(error, 3)).toBe(4);
  });
});

describe('executeSendWithRetries unified retry behavior', () => {
  it('retries FLOOD_WAIT using the server-specified delay', async () => {
    const sendFn = vi.fn()
      .mockRejectedValueOnce(new Error('FLOOD_WAIT_5'))
      .mockResolvedValueOnce('ok');
    const sleep = vi.fn().mockResolvedValue(undefined);
    const onRetry = vi.fn();

    const result = await executeSendWithRetries(sendFn, {
      method: 'sendText',
      retries: 1,
      retryBackoff: parseRetryBackoff('exponential'),
      sleep,
      onRetry,
    });

    expect(result).toEqual({ result: 'ok', attempts: 2 });
    expect(sendFn).toHaveBeenCalledTimes(2);
    expect(sleep).toHaveBeenCalledWith(5000);
    expect(onRetry).toHaveBeenCalledWith(expect.objectContaining({
      type: 'rate_limit',
      retryable: true,
      waitSeconds: 5,
      attempt: 1,
      retries: 1,
    }));
  });

  it('uses exponential backoff for retryable non-rate-limit errors', async () => {
    const sendFn = vi.fn()
      .mockRejectedValueOnce(Object.assign(new Error('ECONNRESET'), { code: 'ECONNRESET' }))
      .mockRejectedValueOnce(Object.assign(new Error('ECONNRESET'), { code: 'ECONNRESET' }))
      .mockResolvedValueOnce('ok');
    const sleep = vi.fn().mockResolvedValue(undefined);

    const result = await executeSendWithRetries(sendFn, {
      method: 'sendPhoto',
      retries: 2,
      retryBackoff: parseRetryBackoff('exponential'),
      sleep,
    });

    expect(result).toEqual({ result: 'ok', attempts: 3 });
    expect(sendFn).toHaveBeenCalledTimes(3);
    expect(sleep).toHaveBeenNthCalledWith(1, 1000);
    expect(sleep).toHaveBeenNthCalledWith(2, 2000);
  });

  it('does not retry when FLOOD_WAIT exceeds maxWaitSeconds', async () => {
    const sendFn = vi.fn().mockRejectedValue(new Error('FLOOD_WAIT_3600'));
    const sleep = vi.fn().mockResolvedValue(undefined);

    await expect(
      executeSendWithRetries(sendFn, {
        method: 'sendText',
        retries: 3,
        maxWaitSeconds: 60,
        sleep,
      }),
    ).rejects.toMatchObject({
      name: 'SendCommandError',
      details: expect.objectContaining({
        type: 'rate_limit',
        attempt: 1,
        retries: 3,
        retryable: true,
        waitSeconds: 3600,
      }),
    });

    expect(sendFn).toHaveBeenCalledTimes(1);
    expect(sleep).not.toHaveBeenCalled();
  });
});
