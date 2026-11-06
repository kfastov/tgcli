import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import {
  appendServerLog,
  followServerLog,
  openServerLog,
  readServerLogTail,
  serverLogPaths,
} from '../core/server-log.js';

const SERVER_LOG_MODULE = fileURLToPath(new URL('../core/server-log.js', import.meta.url));
const FIXED_TIME = new Date('2026-11-06T12:00:00.000Z');

let storeDir;

function readLog(name = 'server.log') {
  return fs.readFileSync(path.join(storeDir, name), 'utf8');
}

beforeEach(() => {
  storeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-server-log-'));
});

afterEach(() => {
  fs.rmSync(storeDir, { recursive: true, force: true });
});

describe('openServerLog', () => {
  it('stamps every line once, also when a line arrives in pieces', () => {
    const log = openServerLog(storeDir, { now: () => FIXED_TIME });
    log.write('[startup] one\n[startup] two');
    log.write(' continued\n');
    log.write(Buffer.from('[sync] three\n'));
    log.close();
    expect(readLog()).toBe([
      '2026-11-06T12:00:00.000Z [startup] one',
      '2026-11-06T12:00:00.000Z [startup] two continued',
      '2026-11-06T12:00:00.000Z [sync] three',
      '',
    ].join('\n'));
  });

  it('leaves a line that has its own timestamp as it is', () => {
    const log = openServerLog(storeDir, { now: () => FIXED_TIME });
    log.write('2026-11-06T11:59:59.000Z [ERR] [client] connect failed\n');
    log.close();
    expect(readLog()).toBe('2026-11-06T11:59:59.000Z [ERR] [client] connect failed\n');
  });

  it('drops terminal colours', () => {
    const log = openServerLog(storeDir, { now: () => FIXED_TIME });
    log.write('\x1b[33mwarn\x1b[0m done\n');
    log.close();
    expect(readLog()).toBe('2026-11-06T12:00:00.000Z warn done\n');
  });

  it('appends to the log a previous server left', () => {
    fs.writeFileSync(path.join(storeDir, 'server.log'), 'old line\n');
    const log = openServerLog(storeDir, { now: () => FIXED_TIME });
    log.write('new line\n');
    log.close();
    expect(readLog()).toBe('old line\n2026-11-06T12:00:00.000Z new line\n');
  });

  it('moves a full log to server.log.1 and keeps no more than that one', () => {
    const log = openServerLog(storeDir, { maxBytes: 100, now: () => FIXED_TIME });
    for (let index = 0; index < 20; index += 1) {
      log.write(`line ${index}\n`);
    }
    log.close();
    const current = readLog();
    const rotated = readLog('server.log.1');
    expect(Buffer.byteLength(rotated)).toBeGreaterThanOrEqual(100);
    expect(Buffer.byteLength(rotated)).toBeLessThan(140);
    expect(Buffer.byteLength(current)).toBeLessThan(100);
    expect(`${rotated}${current}`).toMatch(/line 18\n.*line 19\n$/);
    expect(fs.readdirSync(storeDir).sort()).toEqual(['server.log', 'server.log.1']);
  });

  it('stamps each source at its own line starts', () => {
    const log = openServerLog(storeDir, { now: () => FIXED_TIME });
    log.write('out ', 'stdout');
    log.write('err line\n', 'stderr');
    log.write('line\n', 'stdout');
    log.close();
    expect(readLog()).toBe('2026-11-06T12:00:00.000Z out 2026-11-06T12:00:00.000Z err line\nline\n');
  });

  it('shares the file with another writer without rotating over its lines', () => {
    const first = openServerLog(storeDir, { maxBytes: 200, now: () => FIXED_TIME });
    const second = openServerLog(storeDir, { maxBytes: 200, now: () => FIXED_TIME });
    for (let index = 0; index < 30; index += 1) {
      (index % 2 ? second : first).write(`line ${String(index).padStart(2, '0')}\n`);
    }
    first.close();
    second.close();
    const all = `${readLog('server.log.1')}${readLog()}`;
    // Only server.log.1 and server.log remain, so the oldest lines may be gone,
    // but nothing after them is lost or out of order.
    const numbers = [...all.matchAll(/line (\d\d)/g)].map((match) => Number(match[1]));
    expect(numbers.at(-1)).toBe(29);
    expect(numbers).toEqual(numbers.map((_, index) => numbers[0] + index));
  });

  it('keeps the file private to its owner', () => {
    openServerLog(storeDir).close();
    expect(fs.statSync(path.join(storeDir, 'server.log')).mode & 0o777).toBe(0o600);
  });
});

describe('appendServerLog', () => {
  it('adds one stamped line', () => {
    appendServerLog(storeDir, '[server] Server exited with code null (SIGKILL)');
    expect(readLog()).toMatch(/^\d{4}-\d\d-\d\dT[\d:.]+Z \[server\] Server exited with code null \(SIGKILL\)\n$/);
  });
});

describe('readServerLogTail', () => {
  it('says there is no log yet', () => {
    expect(readServerLogTail(storeDir, 10)).toMatchObject({ exists: false, lines: [], position: null });
  });

  it('leaves out a line still being written', () => {
    fs.writeFileSync(path.join(storeDir, 'server.log'), 'done\nhalf of a ');
    const tail = readServerLogTail(storeDir, 10);
    expect(tail.lines).toEqual(['done']);
    expect(tail.position.offset).toBe(5);
  });

  it('returns the last lines, reaching into the rotated file', () => {
    fs.writeFileSync(path.join(storeDir, 'server.log.1'), 'a\nb\nc\n');
    fs.writeFileSync(path.join(storeDir, 'server.log'), 'd\ne\n');
    expect(readServerLogTail(storeDir, 3).lines).toEqual(['c', 'd', 'e']);
    expect(readServerLogTail(storeDir, 2).lines).toEqual(['d', 'e']);
    expect(readServerLogTail(storeDir, 2).position.offset).toBe(4);
  });
});

describe('followServerLog', () => {
  it('prints lines written after the tail, also across a rotation', async () => {
    const { current, rotated } = serverLogPaths(storeDir);
    fs.writeFileSync(current, 'before\n');
    const { position } = readServerLogTail(storeDir, 10);
    const seen = [];
    const controller = new AbortController();
    const following = followServerLog(storeDir, (lines) => seen.push(...lines), {
      signal: controller.signal,
      from: position,
      intervalMs: 10,
    });

    fs.appendFileSync(current, 'one\ntw');
    await expect.poll(() => seen).toEqual(['one']);
    fs.appendFileSync(current, 'o\n');
    await expect.poll(() => seen).toEqual(['one', 'two']);
    fs.renameSync(current, rotated);
    fs.writeFileSync(current, 'three\n');
    await expect.poll(() => seen).toEqual(['one', 'two', 'three']);

    controller.abort();
    await following;
  });

  it('prints what was written just before a rotation, and a line split across it', async () => {
    const { current, rotated } = serverLogPaths(storeDir);
    fs.writeFileSync(current, 'before\nhalf of a ');
    const { position, lines } = readServerLogTail(storeDir, 10);
    expect(lines).toEqual(['before']);
    const seen = [];
    const controller = new AbortController();
    const following = followServerLog(storeDir, (batch) => seen.push(...batch), {
      signal: controller.signal,
      from: position,
      intervalMs: 50,
    });
    // All within one poll: more lines, then the rotation, then the new file.
    fs.appendFileSync(current, 'line\nA\nB\nsplit ');
    fs.renameSync(current, rotated);
    fs.writeFileSync(current, 'here\nC\n');
    await expect.poll(() => seen).toEqual(['half of a line', 'A', 'B', 'split here', 'C']);
    controller.abort();
    await following;
  });

  it('waits for a log that does not exist yet', async () => {
    const seen = [];
    const controller = new AbortController();
    const following = followServerLog(storeDir, (lines) => seen.push(...lines), {
      signal: controller.signal,
      intervalMs: 10,
    });
    fs.writeFileSync(path.join(storeDir, 'server.log'), 'first\n');
    await expect.poll(() => seen).toEqual(['first']);
    controller.abort();
    await following;
  });
});

describe('captureServerOutput', () => {
  it('copies what the process prints, and the error that kills it, into the log', () => {
    const script = `
      import { captureServerOutput } from ${JSON.stringify(SERVER_LOG_MODULE)};
      captureServerOutput(process.argv[1]);
      console.log('[startup] listening');
      console.error('[control] warning');
      process.stdout.write('partial ');
      process.stdout.write('line\\n');
      throw new Error('boom');
    `;
    const result = spawnSync(process.execPath, ['--input-type=module', '-e', script, storeDir], { encoding: 'utf8' });
    expect(result.status).not.toBe(0);
    // Still printed where it would have gone without the log.
    expect(result.stdout).toContain('[startup] listening');
    expect(result.stderr).toContain('[control] warning');
    const lines = readLog().split('\n');
    expect(lines[0]).toMatch(/Z \[startup\] listening$/);
    expect(lines[1]).toMatch(/Z \[control\] warning$/);
    expect(lines[2]).toMatch(/Z partial line$/);
    // A module's top-level throw reaches the process as a rejection.
    expect(lines[3]).toMatch(/Z \[fatal\] (uncaughtException|unhandledRejection): Error: boom$/);
    expect(lines[4]).toMatch(/Z {5}at /);
  });
});
