// `tgcli server` writes the server's log itself, so a server that dies before it
// could log anything still leaves its error in <store>/server.log. The server is
// made to fail while it loads, before it reads the store or opens a connection.

import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

const CLI = fileURLToPath(new URL('../cli.js', import.meta.url));

let storeDir;
let preload;

beforeEach(() => {
  storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-wrapper-log-'));
  fs.writeFileSync(path.join(storeDir, 'config.json'), JSON.stringify({ apiId: 1, apiHash: 'hash', phoneNumber: '+10000000000' }));
  preload = path.join(storeDir, 'fail-server.mjs');
  fs.writeFileSync(preload, `
    if (process.argv[1]?.endsWith('mcp-server.js')) {
      throw new Error('server failed to load');
    }
  `);
});

afterEach(() => {
  fs.rmSync(storeDir, { recursive: true, force: true });
});

describe('tgcli server', () => {
  it('logs a server that dies before it can log anything itself', () => {
    const result = spawnSync(process.execPath, [CLI, 'server', '--auto-started'], {
      encoding: 'utf8',
      timeout: 30000,
      env: {
        ...process.env,
        TGCLI_STORE: storeDir,
        NODE_OPTIONS: `--import=${pathToFileURL(preload).href}`,
      },
    });
    expect(result.status).toBe(1);
    // Still printed where it went before.
    expect(result.stderr).toContain('server failed to load');
    const log = fs.readFileSync(path.join(storeDir, 'server.log'), 'utf8');
    expect(log).toMatch(/Z .*Error: server failed to load/);
    expect(log).toMatch(/Z \[server\] Server exited with code 1\n$/);
  });
});
