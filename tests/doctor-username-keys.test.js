// `tgcli doctor` names the chats still stored under a username and says what becomes
// of each. The archive is real, in a temporary store; Telegram is stubbed out.

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
  storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-doctor-keys-'));
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

describe('doctor username keys', () => {
  it('says nothing about them in an archive without any', async () => {
    seed([["INSERT INTO channels (channel_id, username) VALUES ('700000002', 'shop_bot')"]]);
    expect(await doctor()).not.toContain('USERNAME KEYS');
    expect(JSON.parse(await doctor('--json')).usernameKeys).toEqual([]);
  });

  it('counts them, names the ones with no Telegram id, and says what happens to each', async () => {
    seed([
      ["INSERT INTO channels (channel_id, username) VALUES ('700000002', 'shop_bot')"],
      ["INSERT INTO channels (channel_id, peer_type) VALUES ('@shop_bot', 'chat')"],
      ["INSERT INTO messages (channel_id, message_id) VALUES ('@shop_bot', 1), ('@shop_bot', 2)"],
      ["INSERT INTO channels (channel_id, peer_type) VALUES ('@fresh', 'chat')"],
      ["INSERT INTO channels (channel_id, peer_type) VALUES ('@lost', 'chat')"],
      ["INSERT INTO username_key_checks (chat_key, outcome, detail, checked_at) VALUES ('@lost', 'not_found', 'Telegram did not find it', ?)", Date.parse('2026-11-06T12:00:00Z')],
    ]);

    const out = await doctor();

    expect(out).toContain('USERNAME KEYS: 3 chat(s) stored under a username instead of a Telegram id. The server moves each to its id after it logs in to Telegram; messages already under the id stay, duplicates are dropped.');
    expect(out).toContain('  @shop_bot: 2 message(s), to be merged with its Telegram id; the archive also has 700000002 under this username.');
    expect(out).toContain('  @fresh: 0 message(s), no Telegram id known for it yet; the server asks Telegram for one.');
    expect(out).toContain('  @lost: 0 message(s), stays as it is: Telegram did not find it (2026-11-06T12:00:00.000Z); asked again a day after that.');

    const { usernameKeys } = JSON.parse(await doctor('--json'));
    expect(usernameKeys.map((chat) => [chat.key, chat.archivedAs, chat.lastCheck?.outcome ?? null])).toEqual([
      ['@fresh', null, null],
      ['@lost', null, 'not_found'],
      ['@shop_bot', '700000002', null],
    ]);
  });
});
