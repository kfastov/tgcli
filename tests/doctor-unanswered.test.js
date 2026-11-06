// `tgcli doctor` lists synced chats missing from the dialog list that Telegram gave no
// answer about. The archive is real, in a temporary store; Telegram is stubbed out.

import fs from 'fs';
import os from 'os';
import path from 'path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import MessageSyncService from '../message-sync-service.js';

vi.mock('../core/services.js', () => ({
  createTelegramClient: vi.fn(() => ({
    telegramClient: {
      isAuthorized: vi.fn().mockResolvedValue(true),
      destroy: vi.fn().mockResolvedValue(undefined),
    },
  })),
  createMessageSyncService: vi.fn((telegramClient, { storeDir }) => ({
    messageSyncService: new MessageSyncService(telegramClient, { dbPath: path.join(storeDir, 'messages.db') }),
  })),
  resolveValidatedConfig: vi.fn(),
}));

vi.mock('../core/update-check.js', async (importOriginal) => ({
  ...(await importOriginal()),
  getUpdateStatus: () => ({ disabledReason: 'CI' }),
}));

const { buildProgram } = await import('../cli.js');

let storeDir;
let stdout;

beforeEach(() => {
  storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-doctor-unanswered-'));
  vi.stubEnv('TGCLI_STORE', storeDir);
  stdout = [];
  vi.spyOn(console, 'log').mockImplementation((...args) => { stdout.push(args.join(' ')); });
  vi.spyOn(process.stdout, 'write').mockImplementation((chunk) => { stdout.push(String(chunk)); return true; });
  vi.spyOn(process.stderr, 'write').mockImplementation(() => true);
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  process.exitCode = undefined;
  fs.rmSync(storeDir, { recursive: true, force: true });
});

function seed(statements) {
  const service = new MessageSyncService(null, { dbPath: path.join(storeDir, 'messages.db') });
  for (const [sql, ...params] of statements) {
    service.db.prepare(sql).run(...params);
  }
  service.close();
}

async function doctor(...flags) {
  stdout.length = 0;
  const program = buildProgram();
  program.exitOverride();
  await program.parseAsync(['node', 'cli.js', ...flags, 'doctor']);
  expect(process.exitCode).toBeUndefined();
  return stdout.join('\n');
}

const chat = (id, { membership = 'unknown', inDialogs = 0, sync = 1, title = null } = {}) => [
  'INSERT INTO channels (channel_id, peer_title, sync_enabled, in_dialogs, membership, membership_checked_at) VALUES (?, ?, ?, ?, ?, ?)',
  id, title, sync, inDialogs, membership, Date.parse('2026-11-06T12:00:00Z'),
];

describe('doctor unanswered chats', () => {
  it('says nothing when Telegram answered about every missing chat', async () => {
    seed([chat('-1000000000801', { membership: 'member' }), chat('-1000000000802', { membership: 'left', sync: 0 })]);

    expect(await doctor()).not.toContain('UNANSWERED');
    expect(JSON.parse(await doctor('--json')).unansweredChats).toEqual([]);
  });

  it('lists them, ten at most, with the command that turns them off', async () => {
    seed([
      ...Array.from({ length: 12 }, (_, index) => chat(String(-1000000000810 - index), { title: index === 0 ? 'Closed group' : null })),
      chat('-1000000000899', { inDialogs: 1, membership: null }),
      chat('-1000000000898', { sync: 0 }),
    ]);

    const lines = (await doctor()).split('\n');

    expect(lines.find((line) => line.startsWith('UNANSWERED:'))).toContain('UNANSWERED: 12 synced chat(s) are missing from your dialog list');
    expect(lines.find((line) => line.startsWith('UNANSWERED:'))).toContain('`tgcli channels unwatch --missing`');
    expect(lines.filter((line) => /^  -100/.test(line))).toHaveLength(10);
    expect(lines).toContain('  -1000000000810 Closed group');
    expect(lines).toContain('  … and 2 more; `tgcli --json doctor` lists them all.');

    const { unansweredChats } = JSON.parse(await doctor('--json'));
    expect(unansweredChats).toHaveLength(12);
    expect(unansweredChats[0]).toEqual({ channelId: '-1000000000810', title: 'Closed group', checkedAt: '2026-11-06T12:00:00.000Z' });
  });
});

describe('turning off the unanswered chats', () => {
  it('turns off only those, as if the user had, and leaves them off when they come back', async () => {
    const service = new MessageSyncService(null, { dbPath: path.join(storeDir, 'messages.db') });
    for (const [sql, ...params] of [
      chat('-1000000000820'),
      chat('-1000000000821'),
      chat('-1000000000822', { membership: 'member' }),
      chat('-1000000000823', { inDialogs: 1, membership: null }),
    ]) {
      service.db.prepare(sql).run(...params);
    }

    expect(service.unwatchUnansweredChats()).toEqual(['-1000000000820', '-1000000000821']);
    const rows = Object.fromEntries(service.db.prepare('SELECT channel_id, sync_enabled, sync_off_reason FROM channels').all()
      .map((row) => [row.channel_id, [row.sync_enabled, row.sync_off_reason]]));
    expect(rows).toEqual({
      '-1000000000820': [0, null],
      '-1000000000821': [0, null],
      '-1000000000822': [1, null],
      '-1000000000823': [1, null],
    });
    expect(service.listUnansweredChats()).toEqual([]);
    service.close();
  });
});
