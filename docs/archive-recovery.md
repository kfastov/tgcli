# Recovering archive gaps

Versions before the sent-message fix did not archive messages sent through tgcli
(`send text`, `send file`, `send photo`), and the next incoming message moved the
sync cursor past them for good. Versions before the realtime fix also lost single
messages in channels and supergroups while the server ran, with no sign in the
archive, because a min copy of a chat overwrote its access hash in the session.
Versions before the recheck fix lost fresh messages in busy channels too: Telegram can
name real messages as deleted in a channel difference and hide them from history for a
while, and nothing announces them when they show up.
`scripts/recover-archive-gaps.js` writes back whatever a chat's history has and the
archive does not.

## Steps

1. Install a version with the fixes, or new gaps keep appearing.
2. Back up the archive. It is `messages.db` in the store directory (`TGCLI_STORE`, or
   the OS app-data directory):

   ```bash
   sqlite3 <store>/messages.db ".backup <store>/messages.backup.db"
   ```

3. List the chats to repair: those you sent to through tgcli, whose sends the archive
   does not mark, and active channels and supergroups. The script refuses chats with
   sync disabled.
4. For each chat, pick `afterMessageId`: the last message before the first possible
   gap. `0` walks the whole chat. The walk starts at the newest message and costs one
   page of 100 messages per second.
5. Stop the server: `tgcli service stop` if it runs as a service, otherwise kill it. It
   does not exit by itself while any chat is synced, and the script refuses to run
   while one answers, printing its pid. Any other tgcli command starts a new server,
   so run none until you are done.
6. Run the script from a repository checkout, with the Node version `better-sqlite3`
   is built for. So far it has only run against a stubbed Telegram, so start with one
   chat where you know a message is missing, and check it as in step 7 before going on:

   ```bash
   node scripts/recover-archive-gaps.js <chatId> <afterMessageId>
   ```

7. Check a message you know was missing. Ask SQLite rather than `tgcli messages show`,
   which falls back to Telegram when the archive has no such row:

   ```bash
   sqlite3 <store>/messages.db "SELECT message_id, text FROM messages WHERE channel_id = '<chatId>' AND message_id = <messageId>;"
   ```

## What it does

- Walks the history from new to old and writes through `INSERT OR IGNORE`: existing
  rows do not change and nothing is duplicated, so an interrupted run can be repeated.
- Leaves the sync cursors alone.
- Adds everything missing in the range, not only your own sends.
- Runs with updates disabled, so it does not take the catch-up from the server.
