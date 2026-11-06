import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const PACKAGE_ROOT = fileURLToPath(new URL('..', import.meta.url));
const DEV_TAG = 'dev';
const SEMVER = /^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?(?:\+([0-9A-Za-z.-]+))?$/;
const COMMIT = /^[0-9a-f]{40,64}$/;

function readFileTrimmed(filePath) {
  try {
    return fs.readFileSync(filePath, 'utf8').trim();
  } catch {
    return null;
  }
}

function readPackageVersion(root) {
  try {
    return JSON.parse(fs.readFileSync(path.join(root, 'package.json'), 'utf8')).version || '0.0.0';
  } catch {
    return '0.0.0';
  }
}

// The git directory of a clone or worktree rooted at root, or null when root is
// not one. A worktree's .git is a file pointing at its directory under the main
// clone's .git/worktrees.
function findGitDir(root) {
  const dotGit = path.join(root, '.git');
  let stat;
  try {
    stat = fs.statSync(dotGit);
  } catch {
    return null;
  }
  if (stat.isDirectory()) {
    return dotGit;
  }
  const pointer = readFileTrimmed(dotGit)?.match(/^gitdir:\s*(.+)$/);
  return pointer ? path.resolve(root, pointer[1]) : null;
}

// HEAD's commit, read from the git files so a CLI run spawns no git process.
function readHeadCommit(gitDir) {
  const head = readFileTrimmed(path.join(gitDir, 'HEAD'));
  const ref = head?.match(/^ref:\s*(.+)$/)?.[1];
  if (!ref) {
    return COMMIT.test(head ?? '') ? head : null;
  }
  const commonDirPointer = readFileTrimmed(path.join(gitDir, 'commondir'));
  const commonDir = commonDirPointer ? path.resolve(gitDir, commonDirPointer) : gitDir;
  for (const dir of new Set([gitDir, commonDir])) {
    const loose = readFileTrimmed(path.join(dir, ref));
    if (COMMIT.test(loose ?? '')) {
      return loose;
    }
  }
  for (const line of readFileTrimmed(path.join(commonDir, 'packed-refs'))?.split('\n') ?? []) {
    const [commit, name] = line.trim().split(' ');
    if (name === ref && COMMIT.test(commit)) {
      return commit;
    }
  }
  return null;
}

// A build run from a git clone says so. package.json holds the last release and
// the clone is ahead of it, so it reports the next patch as a dev prerelease with
// the commit as build metadata: 2.2.5 in package.json -> 2.2.6-dev+abc1234. That
// ranks above the release it grew from and below the next one.
export function devVersion(releaseVersion, commit) {
  const parsed = parseVersion(releaseVersion);
  const base = parsed
    ? `${parsed.major}.${parsed.minor}.${parsed.patch + 1}-${DEV_TAG}`
    : `${releaseVersion}-${DEV_TAG}`;
  return commit ? `${base}+${commit.slice(0, 7)}` : base;
}

// This package's version: package.json's for an installed package, a dev version
// for a git clone. '0.0.0' if package.json cannot be read.
export function readVersion({ root = PACKAGE_ROOT } = {}) {
  const release = readPackageVersion(root);
  const gitDir = findGitDir(root);
  return gitDir ? devVersion(release, readHeadCommit(gitDir)) : release;
}

export function parseVersion(version) {
  const match = SEMVER.exec(String(version ?? '').trim());
  if (!match) {
    return null;
  }
  return {
    major: Number(match[1]),
    minor: Number(match[2]),
    patch: Number(match[3]),
    prerelease: match[4] ? match[4].split('.') : [],
    build: match[5] ?? null,
  };
}

export function isDevVersion(version) {
  return parseVersion(version)?.prerelease[0] === DEV_TAG;
}

function compareIdentifiers(a, b) {
  const aNumeric = /^\d+$/.test(a);
  const bNumeric = /^\d+$/.test(b);
  if (aNumeric && bNumeric) {
    return Math.sign(Number(a) - Number(b));
  }
  if (aNumeric !== bNumeric) {
    return aNumeric ? -1 : 1;
  }
  return a < b ? -1 : a > b ? 1 : 0;
}

// Semver precedence as -1, 0 or 1. Build metadata (a dev build's commit) does not
// count, so two dev builds of one base rank equal. Anything that is not semver
// compares as text with numeric runs.
export function compareVersions(a, b) {
  const left = parseVersion(a);
  const right = parseVersion(b);
  if (!left || !right) {
    return Math.sign(String(a).localeCompare(String(b), undefined, { numeric: true }));
  }
  for (const key of ['major', 'minor', 'patch']) {
    if (left[key] !== right[key]) {
      return Math.sign(left[key] - right[key]);
    }
  }
  // A release outranks its prereleases.
  if (!left.prerelease.length || !right.prerelease.length) {
    return Math.sign(right.prerelease.length - left.prerelease.length);
  }
  const length = Math.max(left.prerelease.length, right.prerelease.length);
  for (let index = 0; index < length; index += 1) {
    if (left.prerelease[index] === undefined) return -1;
    if (right.prerelease[index] === undefined) return 1;
    const order = compareIdentifiers(left.prerelease[index], right.prerelease[index]);
    if (order !== 0) {
      return order;
    }
  }
  return 0;
}
