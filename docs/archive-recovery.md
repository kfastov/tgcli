# Recovering archive gaps

Versions before the sent-message fix did not archive messages sent through tgcli
(`send text`, `send file`, `send photo`), and the next incoming message moved the
sync cursor past them for good. `scripts/recover-archive-gaps.js` writes back
whatever a chat's history has and the archive does not.

## Steps

1. Install a version with the fix, or new sends keep leaving gaps.
2. Back up the archive. It is `messages.db` in the store directory (`TGCLI_STORE`, or
   the OS app-data directory):

   ```bash
   sqlite3 <store>/messages.db ".backup <store>/messages.backup.db"
   ```

3. List the chats you sent to through tgcli. The archive does not record which sends
   came from tgcli, so this list comes from you. The script refuses chats with sync
   disabled.
4. For each chat, pick `afterMessageId`: the last message before the first possible
   gap. If you do not know it, the chat's `oldest_message_id` works, at a cost of one
   page of 100 messages per second.
5. Stop the server. An auto-started one exits by itself after 60 seconds idle. The
   script takes the store lock and fails while a server holds it.
6. Run the script from a repository checkout, with the Node version `better-sqlite3`
   is built for:

   ```bash
   node scripts/recover-archive-gaps.js <chatId> <afterMessageId>
   ```

7. Check a message you know was missing:

   ```bash
   tgcli messages show --chat <chatId> --id <messageId> --source archive
   ```

## What it does

- Walks the history from old to new and writes through `INSERT OR IGNORE`: existing
  rows do not change and nothing is duplicated.
- Leaves the sync cursors alone.
- Adds everything missing in the range, not only your own sends.
- Runs with updates disabled, so it does not take the catch-up from the server.

## Why not rewind the cursor

Rewinding `last_message_id` and running a sync job fixes a gap only if at most one
batch (100 messages) came after it. `_syncNewerMessages` takes the newest batch, so an
older gap stays and the cursor returns to the top with no error.
