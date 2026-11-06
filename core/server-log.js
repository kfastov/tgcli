// The server's log in the store. A server started in the background has no
// terminal, so everything it prints also goes here, one timestamp per line; when
// the file passes its size limit it moves to server.log.1 (replacing the previous
// one) and a fresh file starts, so the log never takes more than twice the limit.

import fs from 'fs';
import path from 'path';

export const SERVER_LOG_FILE = 'server.log';
export const SERVER_LOG_MAX_BYTES = 5 * 1024 * 1024;

const ANSI_ESCAPE = /\x1b\[[0-9;?]*[A-Za-z]/g;
// mtcute stamps its own lines.
const STAMPED = /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d/;

export function serverLogPaths(storeDir) {
  const current = path.join(storeDir, SERVER_LOG_FILE);
  return { current, rotated: `${current}.1` };
}

// An append-only log file that rotates itself. Writes are synchronous, so a line
// written just before the process dies is on disk. Each source (stdout, stderr)
// keeps its own line state, so their timestamps do not land mid-line.
//
// Another process may write the same file (two servers racing to start), so the
// size is the file's, and a file that another writer rotated away is reopened
// rather than rotated again over its replacement.
export function openServerLog(storeDir, { maxBytes = SERVER_LOG_MAX_BYTES, now = () => new Date() } = {}) {
  const paths = serverLogPaths(storeDir);
  fs.mkdirSync(storeDir, { recursive: true });
  let fd = null;
  const atLineStart = new Map();

  const open = () => {
    try {
      fd = fs.openSync(paths.current, 'a', 0o600);
    } catch {
      fd = null;
    }
  };

  const pathInode = () => {
    try {
      return fs.statSync(paths.current).ino;
    } catch {
      return null;
    }
  };

  const reopen = () => {
    try {
      fs.closeSync(fd);
    } catch {
      // Already gone.
    }
    open();
  };

  const stamp = (text, source) => {
    let out = '';
    for (const piece of text.split(/(?<=\n)/)) {
      if ((atLineStart.get(source) ?? true) && !STAMPED.test(piece)) {
        out += `${now().toISOString()} `;
      }
      out += piece;
      atLineStart.set(source, piece.endsWith('\n'));
    }
    return out;
  };

  open();

  return {
    path: paths.current,
    write(chunk, source = 'default') {
      const text = (typeof chunk === 'string' ? chunk : Buffer.from(chunk).toString('utf8')).replace(ANSI_ESCAPE, '');
      if (!text) {
        return;
      }
      const bytes = Buffer.from(stamp(text, source), 'utf8');
      try {
        if (fd === null) {
          open();
        } else if (fs.fstatSync(fd).ino !== pathInode()) {
          reopen();
        }
        if (fd === null) {
          return;
        }
        fs.writeSync(fd, bytes);
        if (fs.fstatSync(fd).size >= maxBytes) {
          // Rotate only the file the path still names.
          if (fs.fstatSync(fd).ino === pathInode()) {
            fs.renameSync(paths.current, paths.rotated);
          }
          reopen();
        }
      } catch {
        // A full disk must not take the server down with it.
      }
    },
    close() {
      if (fd !== null) {
        fs.closeSync(fd);
        fd = null;
      }
    },
  };
}

// Copy everything this process prints into the store log as well, including the
// error that kills it.
export function captureServerOutput(storeDir, options = {}) {
  const log = openServerLog(storeDir, options);
  for (const [source, stream] of [['stdout', process.stdout], ['stderr', process.stderr]]) {
    const original = stream.write.bind(stream);
    stream.write = (chunk, encoding, callback) => {
      log.write(typeof chunk === 'string' && typeof encoding === 'string'
        ? Buffer.from(chunk, encoding)
        : chunk, source);
      return original(chunk, encoding, callback);
    };
  }
  process.on('uncaughtExceptionMonitor', (error, origin) => {
    log.write(`[fatal] ${origin}: ${error?.stack ?? error}\n`, 'fatal');
  });
  return log;
}

// One line appended outside a running server, e.g. by the process that started
// it when it exits abnormally.
export function appendServerLog(storeDir, line, options = {}) {
  try {
    const log = openServerLog(storeDir, options);
    log.write(line.endsWith('\n') ? line : `${line}\n`);
    log.close();
  } catch {
    // Logging is best effort.
  }
}

function splitLines(text) {
  const lines = text.split('\n');
  if (lines[lines.length - 1] === '') {
    lines.pop();
  }
  return lines;
}

// The whole file with its inode and size at the time of reading, or null.
function readWithPosition(filePath) {
  let fd;
  try {
    fd = fs.openSync(filePath, 'r');
  } catch (error) {
    if (error?.code === 'ENOENT') {
      return null;
    }
    throw error;
  }
  try {
    const { ino, size } = fs.fstatSync(fd);
    const buffer = Buffer.alloc(size);
    const read = fs.readSync(fd, buffer, 0, size, 0);
    return { text: buffer.subarray(0, read).toString('utf8'), position: { ino, offset: read } };
  } finally {
    fs.closeSync(fd);
  }
}

// The last `count` complete lines, reaching into server.log.1 when the current
// file is shorter. exists is false when there has never been a log; position
// marks where followServerLog picks up, which is before a line still being
// written, so it comes out once, whole.
export function readServerLogTail(storeDir, count) {
  const paths = serverLogPaths(storeDir);
  const current = readWithPosition(paths.current);
  if (current) {
    const partial = current.text.length - (current.text.lastIndexOf('\n') + 1);
    if (partial > 0) {
      current.position.offset -= Buffer.byteLength(current.text.slice(-partial), 'utf8');
      current.text = current.text.slice(0, -partial);
    }
  }
  let lines = current ? splitLines(current.text) : [];
  let exists = current !== null;
  if (lines.length < count) {
    const rotated = readWithPosition(paths.rotated);
    if (rotated) {
      exists = true;
      lines = splitLines(rotated.text).concat(lines);
    }
  }
  return {
    path: paths.current,
    exists,
    lines: count > 0 ? lines.slice(-count) : [],
    position: current?.position ?? null,
  };
}

function waitOrAbort(ms, signal) {
  return new Promise((resolve) => {
    const timer = setTimeout(done, ms);
    function done() {
      clearTimeout(timer);
      signal?.removeEventListener('abort', done);
      resolve();
    }
    signal?.addEventListener('abort', done, { once: true });
  });
}

// Calls onLines with each batch of complete lines appended to server.log after
// `from` (a readServerLogTail position; the current end when omitted), following
// the log across rotations, until signal aborts.
export async function followServerLog(storeDir, onLines, { signal, from, intervalMs = 500 } = {}) {
  const { current, rotated } = serverLogPaths(storeDir);
  const statOrNull = (filePath = current) => {
    try {
      return fs.statSync(filePath);
    } catch {
      return null;
    }
  };
  const readRange = (filePath, start, end) => {
    const fd = fs.openSync(filePath, 'r');
    try {
      const buffer = Buffer.alloc(Math.max(0, end - start));
      const read = fs.readSync(fd, buffer, 0, buffer.length, start);
      return buffer.subarray(0, read).toString('utf8');
    } finally {
      fs.closeSync(fd);
    }
  };
  let position = from;
  if (!position) {
    const stat = statOrNull();
    position = stat ? { ino: stat.ino, offset: stat.size } : { ino: null, offset: 0 };
  }
  let partial = '';

  while (!signal?.aborted) {
    await waitOrAbort(intervalMs, signal);
    if (signal?.aborted) {
      break;
    }
    const stat = statOrNull();
    if (!stat) {
      continue;
    }
    let text = '';
    try {
      if (stat.ino !== position.ino || stat.size < position.offset) {
        // Rotated: first the end of the old file, now server.log.1, then the new
        // file from its start.
        const old = statOrNull(rotated);
        if (old && old.ino === position.ino) {
          text += readRange(rotated, position.offset, old.size);
        } else {
          // The old file is gone too, and with it the rest of any partial line.
          partial = '';
        }
        position = { ino: stat.ino, offset: 0 };
      }
      if (stat.size > position.offset) {
        text += readRange(current, position.offset, stat.size);
        position = { ino: stat.ino, offset: stat.size };
      }
    } catch {
      continue;
    }
    if (!text) {
      continue;
    }
    const lines = (partial + text).split('\n');
    partial = lines.pop();
    if (lines.length) {
      onLines(lines);
    }
  }
}
