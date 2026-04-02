import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { acquireStoreLock } from '../store-lock.js';

const originalUnlinkSync = fs.unlinkSync;

describe('store lock recovery', () => {
  let storeDir;
  let lockPath;
  let unlinkSpy;
  let killSpy;

  beforeEach(() => {
    storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-lock-test-'));
    lockPath = path.join(storeDir, 'LOCK');
    fs.writeFileSync(lockPath, JSON.stringify({ pid: 424242, startedAt: '2026-03-12T00:00:00.000Z' }));
  });

  afterEach(() => {
    unlinkSpy?.mockRestore();
    killSpy?.mockRestore();
    fs.rmSync(storeDir, { recursive: true, force: true });
  });

  it('surfaces stale lock cleanup failures instead of reporting a live locker', () => {
    killSpy = vi.spyOn(process, 'kill').mockImplementation(() => {
      const error = new Error('no such process');
      error.code = 'ESRCH';
      throw error;
    });
    unlinkSpy = vi.spyOn(fs, 'unlinkSync').mockImplementation((target) => {
      if (target === lockPath) {
        const error = new Error('operation not permitted');
        error.code = 'EPERM';
        throw error;
      }
      return originalUnlinkSync(target);
    });

    expect(() => acquireStoreLock(storeDir)).toThrow(
      `Found stale store lock for dead pid 424242, but could not remove ${lockPath}: operation not permitted`,
    );
  });
});
