// Counters of a running server: the requests it sent Telegram, the errors they got,
// and the archive work it did. They live in memory only, as totals since the server
// started and per minute for the last hour, so `tgcli server stats` can answer
// "how much did it ask Telegram" without debug edits. Only method names, error
// names and counts are kept, never message content.

const MINUTE_MS = 60 * 1000;
const WINDOW_MINUTES = 60;

// Every archive event the server counts, with the labels `server stats` prints, so a
// report lists each one even before it first happens. Counted as 'group.name'.
const STAT_GROUPS = {
  // Messages a difference called deleted, or a page of history skipped, read again later.
  rechecks: ['Deleted messages read again', {
    queued: 'queued',
    checked: 'read',
    found: 'found alive',
    restored: 'put back in the archive',
    failed: 'reads failed',
    gaveUp: 'given up',
  }],
  // `backfill repair` walks.
  repair: ['backfill repair', {
    pages: 'pages read',
    checked: 'messages checked',
    added: 'messages added',
    wouldAdd: 'messages a dry run would add',
    completed: 'repairs finished',
    skipped: 'repairs skipped (chat gone)',
    failed: 'repairs failed',
  }],
  // Fetches of what a too-long channel difference skipped.
  gaps: ['Gaps after a too-long difference', {
    pages: 'pages with new messages',
    added: 'messages added',
    closed: 'gaps closed',
    failed: 'fills failed',
  }],
  jobs: ['Backfill jobs', {
    runs: 'passes',
    completed: 'finished',
    failed: 'failed',
  }],
};

export const STAT_EVENTS = Object.fromEntries(Object.entries(STAT_GROUPS)
  .map(([group, [, labels]]) => [group, Object.keys(labels)]));

const KNOWN_EVENTS = new Set(Object.entries(STAT_EVENTS)
  .flatMap(([group, names]) => names.map((name) => `${group}.${name}`)));

const FLOOD_ERROR_CODE = 420;
// How many times a call reached Telegram, kept on mtcute's per-call context.
const SENT = Symbol('tgcli.requestsSent');

// FLOOD_WAIT_23 and PHONE_MIGRATE_2 carry a number; the name without it is what to count.
function rpcErrorName(message) {
  return String(message || 'UNKNOWN').replace(/_\d+$/, '');
}

function rpcErrorSeconds(message) {
  const match = /_(\d+)$/.exec(String(message ?? ''));
  return match ? Number(match[1]) : 0;
}

// mtcute's error classes keep the name 'Error'; their class says what happened.
function thrownErrorName(error) {
  if (error?.name && error.name !== 'Error') {
    return error.name;
  }
  return error?.constructor?.name || 'Error';
}

function addTo(map, key, by) {
  map.set(key, (map.get(key) ?? 0) + by);
}

function sortedObject(entries) {
  return Object.fromEntries([...entries].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])));
}

// Shapes one set of raw counters into the report `server stats` prints.
function buildReport(counts) {
  const byMethod = new Map();
  const byError = new Map();
  const errorsByMethod = new Map();
  const heldBack = new Map();
  const events = new Map();
  let floodWaits = 0;
  let floodWaitSeconds = 0;

  for (const [key, value] of counts) {
    const [kind, first, second] = key.split('|');
    if (kind === 'request') {
      addTo(byMethod, first, value);
    } else if (kind === 'error') {
      addTo(byError, first, value);
      if (!errorsByMethod.has(second)) {
        errorsByMethod.set(second, new Map());
      }
      addTo(errorsByMethod.get(second), first, value);
    } else if (kind === 'held') {
      if (!heldBack.has(second)) {
        heldBack.set(second, new Map());
      }
      addTo(heldBack.get(second), first, value);
    } else if (kind === 'flood') {
      floodWaits += value;
    } else if (kind === 'floodSeconds') {
      floodWaitSeconds += value;
    } else if (kind === 'event') {
      addTo(events, first, value);
    }
  }

  const sum = (map) => [...map.values()].reduce((total, value) => total + value, 0);
  const byMethodOf = (nested) => Object.fromEntries([...nested.keys()].sort().map((method) => [
    method,
    sortedObject(nested.get(method)),
  ]));
  const report = {
    requests: {
      total: sum(byMethod),
      byMethod: sortedObject(byMethod),
    },
    errors: {
      total: sum(byError),
      floodWait: { count: floodWaits, seconds: floodWaitSeconds },
      byError: sortedObject(byError),
      byMethod: byMethodOf(errorsByMethod),
      // Calls mtcute answered itself from a flood wait it remembers, without asking Telegram.
      heldBack: {
        total: [...heldBack.values()].reduce((total, errors) => total + sum(errors), 0),
        byMethod: byMethodOf(heldBack),
      },
    },
  };
  for (const [group, names] of Object.entries(STAT_EVENTS)) {
    report[group] = Object.fromEntries(names.map((name) => [name, events.get(`${group}.${name}`) ?? 0]));
  }
  return report;
}

export class ServerStats {
  constructor({ now = Date.now } = {}) {
    this.now = now;
    this.startedAt = now();
    this.total = new Map();
    // { minute, counts } per minute that saw something, oldest first.
    this.minutes = [];
  }

  _add(key, by) {
    addTo(this.total, key, by);
    const minute = Math.floor(this.now() / MINUTE_MS);
    let bucket = this.minutes[this.minutes.length - 1];
    // A clock that steps back keeps adding to the newest minute.
    if (!bucket || bucket.minute < minute) {
      bucket = { minute, counts: new Map() };
      this.minutes.push(bucket);
      this._dropOlderThan(minute);
    }
    addTo(bucket.counts, key, by);
  }

  _dropOlderThan(minute) {
    const oldest = minute - WINDOW_MINUTES + 1;
    while (this.minutes.length && this.minutes[0].minute < oldest) {
      this.minutes.shift();
    }
  }

  // An archive event from STAT_EVENTS, as 'group.name'. A name no report reads is
  // dropped; a test keeps the counting points in step with the list.
  count(event, by = 1) {
    if (by > 0 && KNOWN_EVENTS.has(event)) {
      this._add(`event|${event}`, by);
    }
  }

  recordRequest(method) {
    this._add(`request|${method}`, 1);
  }

  // `error` is the mt_rpc_error Telegram answered with, or what the call threw.
  recordError(method, error) {
    if (error?._ === 'mt_rpc_error') {
      this._add(`error|${rpcErrorName(error.errorMessage)}|${method}`, 1);
      if (error.errorCode === FLOOD_ERROR_CODE) {
        this._add('flood', 1);
        const seconds = rpcErrorSeconds(error.errorMessage);
        if (seconds > 0) {
          this._add('floodSeconds', seconds);
        }
      }
      return;
    }
    this._add(`error|${thrownErrorName(error)}|${method}`, 1);
  }

  // The mtcute network middlewares to use in place of `inner` (mtcute's flood waiter
  // and internal-error retries). The counter goes right above the send, so it sees
  // every request handed to the connection, retries included, and every error
  // Telegram answers with. A thin one outside catches the calls the flood waiter
  // answers itself, from a flood wait it remembers, without sending anything.
  networkMiddlewares(inner) {
    return [this._heldBackMiddleware(), ...inner, this._requestMiddleware()];
  }

  _heldBackMiddleware() {
    return async (ctx, next) => {
      ctx[SENT] = 0;
      const result = await next(ctx);
      if (ctx[SENT] === 0 && result?._ === 'mt_rpc_error') {
        this._add(`held|${rpcErrorName(result.errorMessage)}|${ctx.request?._ ?? 'unknown'}`, 1);
      }
      return result;
    };
  }

  _requestMiddleware() {
    return async (ctx, next) => {
      const method = ctx.request?._ ?? 'unknown';
      ctx[SENT] = (ctx[SENT] ?? 0) + 1;
      this.recordRequest(method);
      let result;
      try {
        result = await next(ctx);
      } catch (error) {
        this.recordError(method, error);
        throw error;
      }
      if (result?._ === 'mt_rpc_error') {
        this.recordError(method, result);
      }
      return result;
    };
  }

  snapshot() {
    const now = this.now();
    const minute = Math.floor(now / MINUTE_MS);
    this._dropOlderThan(minute);
    const lastHour = new Map();
    for (const bucket of this.minutes) {
      for (const [key, value] of bucket.counts) {
        addTo(lastHour, key, value);
      }
    }
    // The window starts on a whole minute, up to an hour back.
    const since = Math.max(this.startedAt, (minute - WINDOW_MINUTES + 1) * MINUTE_MS);
    return {
      at: new Date(now).toISOString(),
      sinceStart: { since: new Date(this.startedAt).toISOString(), ...buildReport(this.total) },
      lastHour: { since: new Date(since).toISOString(), ...buildReport(lastHour) },
    };
  }
}

function formatUptime(ms) {
  const minutes = Math.max(0, Math.floor(ms / MINUTE_MS));
  const days = Math.floor(minutes / (24 * 60));
  const hours = Math.floor(minutes / 60) % 24;
  if (days) {
    return `${days}d ${hours}h`;
  }
  if (hours) {
    return `${hours}h ${minutes % 60}m`;
  }
  return `${minutes}m`;
}

function formatLocalTime(iso) {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) {
    return String(iso);
  }
  const pad = (value) => String(value).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} `
    + `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

// One row per error and method, most frequent first.
function errorRows(totalByMethod, hourByMethod) {
  const rows = [];
  for (const [method, errors] of Object.entries(totalByMethod)) {
    for (const [name, count] of Object.entries(errors)) {
      rows.push([`  ${name} ${method}`, count, hourByMethod[method]?.[name] ?? 0]);
    }
  }
  return rows.sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
}

// The human form of a /control/stats answer: one row per counter, since the server
// started and over the last hour.
export function formatServerStats(payload) {
  const total = payload.sinceStart;
  const hour = payload.lastHour;
  const rows = [];

  rows.push(['Requests to Telegram', total.requests.total, hour.requests.total]);
  for (const [method, count] of Object.entries(total.requests.byMethod)) {
    rows.push([`  ${method}`, count, hour.requests.byMethod[method] ?? 0]);
  }

  rows.push(['Request errors', total.errors.total, hour.errors.total]);
  rows.push(...errorRows(total.errors.byMethod, hour.errors.byMethod));
  if (total.errors.floodWait.count) {
    rows.push(['  flood waits, seconds asked', total.errors.floodWait.seconds, hour.errors.floodWait.seconds]);
  }
  if (total.errors.heldBack.total) {
    rows.push(['Held back by mtcute (remembered flood wait)', total.errors.heldBack.total, hour.errors.heldBack.total]);
    rows.push(...errorRows(total.errors.heldBack.byMethod, hour.errors.heldBack.byMethod));
  }

  for (const [group, [title, labels]] of Object.entries(STAT_GROUPS)) {
    rows.push([title]);
    for (const [name, label] of Object.entries(labels)) {
      rows.push([`  ${label}`, total[group]?.[name] ?? 0, hour[group]?.[name] ?? 0]);
    }
  }

  const headers = ['since start', 'last hour'];
  const labelWidth = Math.max(...rows.map((row) => row[0].length));
  const numberWidth = Math.max(...headers.map((header) => header.length),
    ...rows.flatMap((row) => row.slice(1).map((value) => String(value).length)));
  const line = (label, values) => [label.padEnd(labelWidth), ...values.map((value) => String(value).padStart(numberWidth))]
    .join('  ').trimEnd();

  const uptime = formatUptime(Date.parse(payload.at) - Date.parse(payload.startedAt ?? total.since));
  return [
    `tgcli server ${payload.version ?? '?'}, pid ${payload.pid ?? '?'}, up ${uptime}`,
    `Since start: ${formatLocalTime(total.since)}. Last hour: since ${formatLocalTime(hour.since)}.`,
    '',
    line('', headers),
    ...rows.map(([label, ...values]) => line(label, values)),
  ].join('\n');
}
