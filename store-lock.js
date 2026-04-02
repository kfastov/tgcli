import fs from 'fs';
import path from 'path';

function lockPayload() {
  return JSON.stringify({
    pid: process.pid,
    startedAt: new Date().toISOString(),
  });
}

export function readStoreLock(storeDir) {
  const lockPath = path.join(storeDir, 'LOCK');
  try {
    const raw = fs.readFileSync(lockPath, 'utf8');
    return {
      exists: true,
      path: lockPath,
      info: raw.trim(),
    };
  } catch (error) {
    if (error.code === 'ENOENT') {
      return { exists: false, path: lockPath, info: null };
    }
    throw error;
  }
}

function isPidAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function parseLockPid(raw) {
  try {
    const parsed = JSON.parse(raw);
    return typeof parsed?.pid === 'number' ? parsed.pid : null;
  } catch {
    return null;
  }
}

function removeStaleLockFile(lockPath, pid, label) {
  try {
    fs.unlinkSync(lockPath);
  } catch (error) {
    if (error?.code === 'ENOENT') {
      return;
    }
    const details = error?.message ? `: ${error.message}` : '';
    throw new Error(`Found stale ${label} for dead pid ${pid}, but could not remove ${lockPath}${details}`);
  }
}

function getAliveReadLocks(storeDir) {
  let entries;
  try {
    entries = fs.readdirSync(storeDir);
  } catch {
    return [];
  }
  const alive = [];
  for (const name of entries) {
    if (!name.startsWith('LOCK.read.')) continue;
    const filePath = path.join(storeDir, name);
    let raw;
    try { raw = fs.readFileSync(filePath, 'utf8').trim(); } catch { continue; }
    const pid = parseLockPid(raw);
    if (!pid) continue;
    if (isPidAlive(pid)) {
      alive.push({ name, pid });
    } else {
      removeStaleLockFile(filePath, pid, 'read lock');
    }
  }
  return alive;
}

export function acquireStoreLock(storeDir, _retried = false) {
  const lockPath = path.join(storeDir, 'LOCK');
  fs.mkdirSync(storeDir, { recursive: true });

  // Check for alive read locks before acquiring write lock
  const aliveReaders = getAliveReadLocks(storeDir);
  if (aliveReaders.length > 0) {
    const pids = aliveReaders.map(r => r.pid).join(', ');
    throw new Error(`Store has active readers (pids: ${pids}), cannot acquire write lock`);
  }

  try {
    const fd = fs.openSync(lockPath, 'wx');
    fs.writeFileSync(fd, lockPayload());
    fs.closeSync(fd);
  } catch (error) {
    if (error.code === 'EEXIST') {
      const info = readStoreLock(storeDir);
      const pid = parseLockPid(info.info);
      if (pid && !isPidAlive(pid) && !_retried) {
        removeStaleLockFile(lockPath, pid, 'store lock');
        return acquireStoreLock(storeDir, true);
      }
      const details = info.info ? ` (${info.info})` : '';
      throw new Error(`Store is locked by another process${details}`);
    }
    throw error;
  }

  let released = false;
  return () => {
    if (released) {
      return;
    }
    released = true;
    try {
      fs.unlinkSync(lockPath);
    } catch (error) {
      if (error.code !== 'ENOENT') {
        throw error;
      }
    }
  };
}

export function acquireReadLock(storeDir) {
  fs.mkdirSync(storeDir, { recursive: true });

  // Check for alive write lock
  const writeLock = readStoreLock(storeDir);
  if (writeLock.exists) {
    const pid = parseLockPid(writeLock.info);
    if (pid && !isPidAlive(pid)) {
      removeStaleLockFile(writeLock.path, pid, 'store lock');
    } else {
      const details = writeLock.info ? ` (${writeLock.info})` : '';
      throw new Error(`Store is locked by a writer${details}`);
    }
  }

  const readLockPath = path.join(storeDir, `LOCK.read.${process.pid}`);
  fs.writeFileSync(readLockPath, lockPayload());

  let released = false;
  return () => {
    if (released) {
      return;
    }
    released = true;
    try {
      fs.unlinkSync(readLockPath);
    } catch (error) {
      if (error.code !== 'ENOENT') {
        throw error;
      }
    }
  };
}
