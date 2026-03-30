import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const { telegramClientCtor, messageSyncServiceCtor } = vi.hoisted(() => ({
  telegramClientCtor: vi.fn(function (...args) {
    return { kind: 'telegram', args };
  }),
  messageSyncServiceCtor: vi.fn(function (...args) {
    return { kind: 'sync', args };
  }),
}));

vi.mock('../telegram-client.js', () => ({
  default: telegramClientCtor,
}));

vi.mock('../message-sync-service.js', () => ({
  default: messageSyncServiceCtor,
}));

import {
  createMessageSyncService,
  createServices,
  createTelegramClient,
} from '../core/services.js';

describe('core services helpers', () => {
  let storeDir;

  beforeEach(() => {
    storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-services-test-'));
    fs.writeFileSync(path.join(storeDir, 'config.json'), JSON.stringify({
      apiId: '12345',
      apiHash: 'hash-value',
      phoneNumber: '+1234567890',
    }));
    telegramClientCtor.mockClear();
    messageSyncServiceCtor.mockClear();
  });

  afterEach(() => {
    fs.rmSync(storeDir, { recursive: true, force: true });
  });

  it('creates a telegram client without bootstrapping the archive service', () => {
    const result = createTelegramClient({
      storeDir,
      forceSms: true,
      useQr: true,
      disableUpdates: true,
    });

    expect(telegramClientCtor).toHaveBeenCalledWith(
      '12345',
      'hash-value',
      '+1234567890',
      path.join(storeDir, 'session.json'),
      { forceSms: true, useQr: true, disableUpdates: true },
    );
    expect(messageSyncServiceCtor).not.toHaveBeenCalled();
    expect(result.sessionPath).toBe(path.join(storeDir, 'session.json'));
  });

  it('creates a message sync service on demand for an existing telegram client', () => {
    const fakeClient = { kind: 'telegram' };
    const result = createMessageSyncService(fakeClient, {
      storeDir,
      batchSize: 7,
      interJobDelayMs: 11,
      interBatchDelayMs: 13,
    });

    expect(messageSyncServiceCtor).toHaveBeenCalledWith(fakeClient, {
      dbPath: path.join(storeDir, 'messages.db'),
      batchSize: 7,
      interJobDelayMs: 11,
      interBatchDelayMs: 13,
    });
    expect(result.dbPath).toBe(path.join(storeDir, 'messages.db'));
  });

  it('keeps the legacy createServices composition intact', () => {
    const result = createServices({ storeDir });

    expect(telegramClientCtor).toHaveBeenCalledTimes(1);
    expect(messageSyncServiceCtor).toHaveBeenCalledTimes(1);
    expect(result.sessionPath).toBe(path.join(storeDir, 'session.json'));
    expect(result.dbPath).toBe(path.join(storeDir, 'messages.db'));
  });
});
