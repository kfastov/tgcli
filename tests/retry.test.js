vi.mock('timers/promises', () => ({
  setTimeout: vi.fn().mockResolvedValue(undefined),
}));

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { classifyError, computeRetryWaitSeconds, withSendRetry } from '../core/retry.js';

describe('classifyError', () => {
  it('classifies FLOOD_WAIT_30 as rate_limit', () => {
    const error = new Error('FLOOD_WAIT_30');
    const result = classifyError(error);
    expect(result.type).toBe('rate_limit');
    expect(result.message).toBe('FLOOD_WAIT_30');
    expect(result.code).toBeNull();
  });

  it('classifies "wait of 60 seconds is required" as rate_limit', () => {
    const error = new Error('A wait of 60 seconds is required');
    const result = classifyError(error);
    expect(result.type).toBe('rate_limit');
  });

  it('classifies error with code ECONNRESET as network', () => {
    const error = new Error('connection reset');
    error.code = 'ECONNRESET';
    const result = classifyError(error);
    expect(result.type).toBe('network');
    expect(result.code).toBe('ECONNRESET');
  });

  it('classifies error with ETIMEDOUT in message as network', () => {
    const error = new Error('connect ETIMEDOUT 1.2.3.4:443');
    const result = classifyError(error);
    expect(result.type).toBe('network');
  });

  it('classifies generic error as api', () => {
    const error = new Error('Something went wrong');
    const result = classifyError(error);
    expect(result.type).toBe('api');
    expect(result.message).toBe('Something went wrong');
    expect(result.code).toBeNull();
  });

  it('handles string errors', () => {
    const result = classifyError('FLOOD_WAIT_10');
    expect(result.type).toBe('rate_limit');
  });

  it('handles non-Error objects', () => {
    const result = classifyError({ code: 'ECONNRESET' });
    expect(result.type).toBe('network');
    expect(result.code).toBe('ECONNRESET');
  });
});

describe('computeRetryWaitSeconds', () => {
  it('returns parsed seconds for FLOOD_WAIT_30', () => {
    const error = new Error('FLOOD_WAIT_30');
    expect(computeRetryWaitSeconds(error, 1)).toBe(30);
  });

  it('returns parsed seconds for "wait of 60 seconds" error', () => {
    const error = new Error('A wait of 60 seconds is required');
    expect(computeRetryWaitSeconds(error, 1)).toBe(60);
  });

  it('returns 1 for non-rate-limit error, attempt 1', () => {
    const error = new Error('generic error');
    expect(computeRetryWaitSeconds(error, 1)).toBe(1);
  });

  it('returns 2 for non-rate-limit error, attempt 2', () => {
    const error = new Error('generic error');
    expect(computeRetryWaitSeconds(error, 2)).toBe(2);
  });

  it('returns 4 for non-rate-limit error, attempt 3', () => {
    const error = new Error('generic error');
    expect(computeRetryWaitSeconds(error, 3)).toBe(4);
  });
});

describe('withSendRetry', () => {
  let stderrSpy;

  beforeEach(() => {
    stderrSpy = vi.spyOn(process.stderr, 'write').mockImplementation(() => true);
  });

  afterEach(() => {
    stderrSpy.mockRestore();
  });

  it('returns result on first attempt success', async () => {
    const fn = vi.fn().mockResolvedValue('ok');
    const result = await withSendRetry(fn, { retries: 3 });
    expect(result).toEqual({ result: 'ok', retryLog: [], attempts: 1 });
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('returns result after 1 retry on network error', async () => {
    const networkError = new Error('connect ETIMEDOUT 1.2.3.4:443');
    const fn = vi.fn()
      .mockRejectedValueOnce(networkError)
      .mockResolvedValueOnce('ok');
    const result = await withSendRetry(fn, { retries: 2 });
    expect(result.result).toBe('ok');
    expect(result.attempts).toBe(2);
    expect(result.retryLog).toHaveLength(1);
    expect(result.retryLog[0].attempt).toBe(1);
    expect(result.retryLog[0].error.type).toBe('network');
    expect(result.retryLog[0].waitSeconds).toBe(1);
  });

  it('throws immediately on non-retryable api error', async () => {
    const fn = vi.fn().mockRejectedValue(new Error('CHAT_WRITE_FORBIDDEN'));
    try {
      await withSendRetry(fn, { retries: 3 });
      expect.unreachable('should have thrown');
    } catch (error) {
      expect(error.message).toBe('CHAT_WRITE_FORBIDDEN');
      expect(error.retryLog).toEqual([]);
      expect(error.attempts).toBe(1);
      expect(fn).toHaveBeenCalledTimes(1);
    }
  });

  it('throws after max retries exhausted on network errors', async () => {
    const fn = vi.fn().mockRejectedValue(new Error('connect ECONNRESET'));
    try {
      await withSendRetry(fn, { retries: 2 });
      expect.unreachable('should have thrown');
    } catch (error) {
      expect(error.message).toBe('connect ECONNRESET');
      expect(error.retryLog).toHaveLength(2);
      expect(error.attempts).toBe(3);
    }
  });

  it('throws immediately when retries is 0', async () => {
    const fn = vi.fn().mockRejectedValue(new Error('fail'));
    try {
      await withSendRetry(fn, { retries: 0 });
      expect.unreachable('should have thrown');
    } catch (error) {
      expect(error.message).toBe('fail');
      expect(error.retryLog).toEqual([]);
      expect(error.attempts).toBe(1);
      expect(fn).toHaveBeenCalledTimes(1);
    }
  });

  it('throws when FLOOD_WAIT exceeds maxWaitSeconds', async () => {
    const fn = vi.fn().mockRejectedValue(new Error('FLOOD_WAIT_3600'));
    try {
      await withSendRetry(fn, { retries: 3, maxWaitSeconds: 60 });
      expect.unreachable('should have thrown');
    } catch (error) {
      expect(error.message).toBe('FLOOD_WAIT_3600');
      expect(error.attempts).toBe(1);
      expect(fn).toHaveBeenCalledTimes(1);
    }
  });

  it('writes JSON retry event to stderr in json mode', async () => {
    const fn = vi.fn()
      .mockRejectedValueOnce(new Error('FLOOD_WAIT_5'))
      .mockResolvedValueOnce('ok');
    await withSendRetry(fn, { retries: 1, json: true });
    expect(stderrSpy).toHaveBeenCalledTimes(1);
    const written = stderrSpy.mock.calls[0][0];
    const parsed = JSON.parse(written.trim());
    expect(parsed.event).toBe('retry');
    expect(parsed.attempt).toBe(1);
    expect(parsed.maxAttempts).toBe(2);
    expect(parsed.error.type).toBe('rate_limit');
    expect(parsed.waitSeconds).toBe(5);
  });

  it('writes human-readable retry message to stderr in non-json mode', async () => {
    const fn = vi.fn()
      .mockRejectedValueOnce(new Error('connect ETIMEDOUT 1.2.3.4:443'))
      .mockResolvedValueOnce('ok');
    await withSendRetry(fn, { retries: 1, json: false });
    expect(stderrSpy).toHaveBeenCalledTimes(1);
    const written = stderrSpy.mock.calls[0][0];
    expect(written).toContain('Retry 1/1');
    expect(written).toContain('NETWORK');
    expect(written).toContain('ETIMEDOUT');
    expect(written).toContain('Waiting 1s');
  });

  it('accumulates multiple retries in retryLog', async () => {
    const fn = vi.fn()
      .mockRejectedValueOnce(new Error('connect ECONNRESET'))
      .mockRejectedValueOnce(new Error('connect ETIMEDOUT'))
      .mockResolvedValueOnce('ok');
    const result = await withSendRetry(fn, { retries: 3 });
    expect(result.attempts).toBe(3);
    expect(result.retryLog).toHaveLength(2);
    expect(result.retryLog[0].attempt).toBe(1);
    expect(result.retryLog[0].waitSeconds).toBe(1);
    expect(result.retryLog[1].attempt).toBe(2);
    expect(result.retryLog[1].waitSeconds).toBe(2);
  });
});
