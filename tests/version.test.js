import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { compareVersions, devVersion, isDevVersion, parseVersion, readVersion } from '../core/version.js';

const COMMIT = 'abcdef0123456789abcdef0123456789abcdef01';
const OTHER_COMMIT = '0123456789abcdef0123456789abcdef01234567';

let root;

function write(relativePath, content) {
  const filePath = path.join(root, relativePath);
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, content);
}

beforeEach(() => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-version-'));
  write('package.json', JSON.stringify({ version: '2.2.5' }));
});

afterEach(() => {
  fs.rmSync(root, { recursive: true, force: true });
});

describe('readVersion', () => {
  it("reports package.json's version for an installed package", () => {
    expect(readVersion({ root })).toBe('2.2.5');
  });

  it('reports the next patch as a dev build with the commit in a clone', () => {
    write('.git/HEAD', 'ref: refs/heads/main\n');
    write('.git/refs/heads/main', `${COMMIT}\n`);
    expect(readVersion({ root })).toBe('2.2.6-dev+abcdef0');
  });

  it('finds a branch that only packed-refs has', () => {
    write('.git/HEAD', 'ref: refs/heads/main\n');
    write('.git/packed-refs', `# pack-refs with: peeled fully-peeled sorted\n${OTHER_COMMIT} refs/heads/other\n${COMMIT} refs/heads/main\n`);
    expect(readVersion({ root })).toBe('2.2.6-dev+abcdef0');
  });

  it('reads a detached HEAD', () => {
    write('.git/HEAD', `${COMMIT}\n`);
    expect(readVersion({ root })).toBe('2.2.6-dev+abcdef0');
  });

  it("follows a worktree's .git file to the main clone's refs", () => {
    const main = fs.mkdtempSync(path.join(os.tmpdir(), 'tgcli-version-main-'));
    try {
      const gitDir = path.join(main, '.git', 'worktrees', 'wt');
      fs.mkdirSync(gitDir, { recursive: true });
      fs.writeFileSync(path.join(gitDir, 'HEAD'), 'ref: refs/heads/feature\n');
      fs.writeFileSync(path.join(gitDir, 'commondir'), '../..\n');
      fs.mkdirSync(path.join(main, '.git', 'refs', 'heads'), { recursive: true });
      fs.writeFileSync(path.join(main, '.git', 'refs', 'heads', 'feature'), `${COMMIT}\n`);
      write('.git', `gitdir: ${gitDir}\n`);
      expect(readVersion({ root })).toBe('2.2.6-dev+abcdef0');
    } finally {
      fs.rmSync(main, { recursive: true, force: true });
    }
  });

  it('still reports a dev build when the commit cannot be read', () => {
    write('.git/HEAD', 'ref: refs/heads/unborn\n');
    expect(readVersion({ root })).toBe('2.2.6-dev');
  });

  it('is the dev version of this very clone', () => {
    expect(isDevVersion(readVersion())).toBe(true);
  });
});

describe('devVersion', () => {
  it('bumps the patch and keeps seven characters of the commit', () => {
    expect(devVersion('2.9.19', COMMIT)).toBe('2.9.20-dev+abcdef0');
  });
});

describe('compareVersions', () => {
  it.each([
    ['2.2.5', '2.2.6', -1],
    ['2.10.0', '2.9.9', 1],
    ['2.2.6-dev+abc1234', '2.2.5', 1],
    ['2.2.6-dev+abc1234', '2.2.6', -1],
    ['2.2.6-dev+abc1234', '2.3.0', -1],
    ['2.2.6-dev+abc1234', '2.2.6-dev+def5678', 0],
    ['1.0.0-alpha', '1.0.0-alpha.1', -1],
    ['1.0.0-alpha.2', '1.0.0-alpha.10', -1],
    ['1.0.0-alpha.1', '1.0.0-beta', -1],
    ['1.0.0-1', '1.0.0-alpha', -1],
    ['2.2.5', '2.2.5', 0],
  ])('%s vs %s is %i', (a, b, expected) => {
    expect(compareVersions(a, b)).toBe(expected);
    expect(compareVersions(b, a)).toBe(-expected || 0);
  });
});

describe('isDevVersion / parseVersion', () => {
  it('tells dev builds from releases and other prereleases', () => {
    expect(isDevVersion('2.2.6-dev+abc1234')).toBe(true);
    expect(isDevVersion('2.2.6-dev')).toBe(true);
    expect(isDevVersion('2.2.6')).toBe(false);
    expect(isDevVersion('2.2.6-rc.1')).toBe(false);
    expect(isDevVersion(undefined)).toBe(false);
  });

  it('returns null for something that is not a version', () => {
    expect(parseVersion('nightly')).toBeNull();
  });
});
