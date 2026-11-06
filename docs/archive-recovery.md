# Recovering archive gaps

Versions before the sent-message fix did not archive messages sent through tgcli
(`send text`, `send file`, `send photo`), and the next incoming message moved the
sync cursor past them for good. Versions before the realtime fix also lost single
messages in channels and supergroups while the server ran, with no sign in the
archive, because a min copy of a chat overwrote its access hash in the session.
Versions before the recheck fix (2.2.5) lost fresh messages in busy channels too:
Telegram can name real messages as deleted in a channel difference and hide them from
history for a while, and nothing announces them when they show up.

`tgcli backfill repair` writes back whatever a chat's history has and the archive does
not. The always-on server runs it between its other work, so nothing has to be stopped.
`tgcli doctor` reminds you of it until `--all` has run; an archive started by 2.2.5 or later
needs no repair and gets no reminder.

## Steps

1. Install 2.2.5 or later, or new gaps keep appearing.
2. Optionally back up the archive. A repair only adds rows, but the archive is
   `messages.db` in the store directory (`TGCLI_STORE`, or the OS app-data directory):
   ```bash
   sqlite3 <store>/messages.db ".backup <store>/messages.backup.db"
   ```
3. Repair every chat with sync on, over the last 30 days:
   ```bash
   tgcli backfill repair --all
   ```
   Or one chat, with a date or a message id as the lower bound:
   ```bash
   tgcli backfill repair --chat @channel --since 2026-01-01
   tgcli backfill repair --chat @channel --after-id 123456
   ```
   `--since` also takes a duration with a unit, such as `7d`. `--after-id` alone sets no
   date bound, and `--after-id 0` walks everything the archive covers. The command follows progress
   on stderr and prints what it added; Ctrl-C detaches and the server carries on.
   `--background` returns at once, and `tgcli backfill status` shows repairs under way.
4. Check a message you know was missing. Ask SQLite rather than `tgcli messages show`,
   which falls back to Telegram when the archive has no such row:
   ```bash
   sqlite3 <store>/messages.db "SELECT message_id, text FROM messages WHERE channel_id = '<chatId>' AND message_id = <messageId>;"
   ```

## What it does

- Walks the history from new to old, one page of 100 messages per step with a second
  between pages, and writes through `INSERT OR IGNORE`: existing rows do not change and
  nothing is duplicated.
- Leaves the sync cursors alone, and goes no lower than what the archive covers: the
  older of the chat's oldest archived message and its backfill cursor. Older history is
  what `backfill --depth` and `--min-date` are for, and a chat with nothing archived is
  skipped.
- Adds everything missing in the range, not only your own sends.
- Lets backfills and gap fills go first, a page at a time. A restarted server carries on
  where it stopped, a rate limit pauses it, and a failed repair is retried with failed
  backfills, hourly, unless the chat's sync is off.
- Asking again for a chat starts its repair over with the new bounds.
