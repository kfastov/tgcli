import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../core/services.js', () => ({
  createServices: vi.fn(),
}));

vi.mock('../store-lock.js', () => ({
  acquireStoreLock: vi.fn(() => () => {}),
  acquireReadLock: vi.fn(),
  readStoreLock: vi.fn(),
}));

import { createServices } from '../core/services.js';
import { saveConfig } from '../core/config.js';
import {
  formatFeedbackMessage,
  getFeedbackCooldownRemainingSeconds,
  resolveFeedbackCategory,
  runFeedback,
  writeFeedbackLastSentAt,
} from '../cli.js';

const PACKAGE_VERSION = JSON.parse(fs.readFileSync(path.resolve('package.json'), 'utf8')).version;

function createTempStore() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-feedback-test-'));
}

function createMockServices(messageId = 321) {
  const telegramClient = {
    isAuthorized: vi.fn().mockResolvedValue(true),
    sendTextMessage: vi.fn().mockResolvedValue({ messageId }),
    destroy: vi.fn().mockResolvedValue(undefined),
  };
  const messageSyncService = {
    shutdown: vi.fn().mockResolvedValue(undefined),
  };
  createServices.mockReturnValue({ telegramClient, messageSyncService });
  return { telegramClient, messageSyncService };
}

describe('tgcli feedback helpers', () => {
  let storeDir;
  let originalStoreDir;

  beforeEach(() => {
    storeDir = createTempStore();
    originalStoreDir = process.env.TGCLI_STORE;
    process.env.TGCLI_STORE = storeDir;
    createServices.mockReset();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    if (originalStoreDir === undefined) {
      delete process.env.TGCLI_STORE;
    } else {
      process.env.TGCLI_STORE = originalStoreDir;
    }
    fs.rmSync(storeDir, { recursive: true, force: true });
  });

  it('formats feedback messages with category header and metadata footer', () => {
    expect(
      formatFeedbackMessage('global search should be supported', 'bug', {
        version: '2.0.8',
        platform: 'linux',
        nodeVersion: 'v24.0.0',
      }),
    ).toBe(`📋 tgcli feedback [bug]

global search should be supported

---
tgcli v2.0.8 | linux | v24.0.0`);
  });

  it('detects category flags and defaults to general', () => {
    expect(resolveFeedbackCategory({ bug: true })).toBe('bug');
    expect(resolveFeedbackCategory({ suggestion: true })).toBe('suggestion');
    expect(resolveFeedbackCategory({ praise: true })).toBe('praise');
    expect(resolveFeedbackCategory({})).toBe('general');
    expect(() => resolveFeedbackCategory({ bug: true, suggestion: true })).toThrow(
      'Choose only one of --bug, --suggestion, or --praise.',
    );
  });

  it('computes cooldown remaining seconds from the last feedback timestamp', () => {
    writeFeedbackLastSentAt(storeDir, 1_000_000);
    expect(getFeedbackCooldownRemainingSeconds(storeDir, 1_010_001)).toBe(50);
    expect(getFeedbackCooldownRemainingSeconds(storeDir, 1_060_000)).toBe(0);
  });

  it('rejects feedback when feedback.chatId is missing', async () => {
    saveConfig(storeDir, {
      apiId: '12345',
      apiHash: 'hash',
      phoneNumber: '+10000000000',
    });

    await expect(
      runFeedback({ json: false, timeoutMs: null }, ['missing', 'recipient'], {}),
    ).rejects.toThrow(`Feedback recipient not configured. Run:
  tgcli config set feedback.chatId <username-or-id>`);
    expect(createServices).not.toHaveBeenCalled();
  });

  it('sends plain-text feedback and returns JSON output when requested', async () => {
    saveConfig(storeDir, {
      apiId: '12345',
      apiHash: 'hash',
      phoneNumber: '+10000000000',
      feedback: { chatId: '@maintainer' },
    });
    const { telegramClient, messageSyncService } = createMockServices(654);
    const stdoutSpy = vi.spyOn(process.stdout, 'write').mockImplementation(() => true);

    await runFeedback(
      { json: true, timeoutMs: null },
      ['rename', 'sync', 'to', 'backfill'],
      { suggestion: true },
    );

    expect(telegramClient.sendTextMessage).toHaveBeenCalledWith(
      '@maintainer',
      formatFeedbackMessage('rename sync to backfill', 'suggestion', {
        version: PACKAGE_VERSION,
        platform: process.platform,
        nodeVersion: process.version,
      }),
      { parseMode: 'none' },
    );
    expect(JSON.parse(stdoutSpy.mock.calls[0][0])).toEqual({
      ok: true,
      messageId: 654,
      category: 'suggestion',
    });
    expect(messageSyncService.shutdown).toHaveBeenCalledTimes(1);
    expect(telegramClient.destroy).toHaveBeenCalledTimes(1);
  });

  it('blocks repeated feedback during the cooldown window', async () => {
    saveConfig(storeDir, {
      apiId: '12345',
      apiHash: 'hash',
      phoneNumber: '+10000000000',
      feedback: { chatId: '@maintainer' },
    });
    const { telegramClient } = createMockServices(700);

    await runFeedback({ json: false, timeoutMs: null }, ['first', 'feedback'], { bug: true });

    await expect(
      runFeedback({ json: false, timeoutMs: null }, ['second', 'feedback'], { bug: true }),
    ).rejects.toThrow('Please wait 60 seconds before sending another feedback.');
    expect(telegramClient.sendTextMessage).toHaveBeenCalledTimes(1);
    expect(createServices).toHaveBeenCalledTimes(1);
  });
});
