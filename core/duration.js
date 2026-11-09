// Shared duration parser used by the CLI (cli.js) and the background server
// (mcp-server.js). Returns the duration in milliseconds.
//
// Accepts a string with an optional unit suffix: ms | s | m | h | d. A bare number
// is interpreted as SECONDS (e.g. "30" -> 30000), matching long-standing CLI
// behavior. Returns null for empty/non-string input; throws on an unparseable
// value.
export function parseDuration(value) {
  if (typeof value !== 'string' || !value.trim()) {
    return null;
  }
  const raw = value.trim();
  const match = raw.match(/^(\d+)(ms|s|m|h|d)?$/i);
  if (!match) {
    throw new Error(`Invalid duration: ${value}`);
  }
  const amount = Number(match[1]);
  const unit = (match[2] || 's').toLowerCase();
  if (unit === 'ms') return amount;
  if (unit === 's') return amount * 1000;
  if (unit === 'm') return amount * 60 * 1000;
  if (unit === 'h') return amount * 60 * 60 * 1000;
  if (unit === 'd') return amount * 24 * 60 * 60 * 1000;
  return amount * 1000;
}

// Unix seconds of an ISO-8601 date (or a Date), or null for none. Throws naming the
// option the value came in, as `label`.
export function parseIsoDate(value, label = 'date') {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  const ts = date.getTime();
  if (Number.isNaN(ts)) {
    throw new Error(`${label} must be a valid ISO-8601 string`);
  }
  return Math.floor(ts / 1000);
}
