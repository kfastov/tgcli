# tgcli CLI Commands

CLI goal: human-readable output by default with --json for scripting.

## Execution model
Every Telegram and archive command (channels, messages, send, media, topics,
tags, metadata, contacts, groups, folders) runs through the always-on control
server, not in the CLI process. The CLI is a thin one-shot client: it auto-starts
`tgcli server --idle-exit 60s` in the background when one isn't already running,
asks it to execute the operation against its warm (already-authed) MTProto
connection and open database over the loopback control API, then renders the
result. The server is the single writer; the auto-started server shuts itself
down once the queue drains and nothing is watched.

Commands that do not run on the server: `config`, `service`, `doctor`, and `auth`
(interactive login bootstraps the session the server then uses).

Before any command other than `server` and `service` runs, the CLI checks the
version of a running server and replaces an outdated one it started itself, also
for the commands above (see `server` below).

## Global flags
- --json
- --timeout DURATION
  - No default for most commands (long-running ones like `backfill`, `--follow`, and `server` stay unbounded).
  - `send` commands default to `30s` so agents/scripts never hang on a stuck connection. Override with `--timeout 5m`, or `--timeout 0` to disable.
- --quiet
  - Suppress informational progress/status output on stderr (e.g. backfill progress lines). Errors and primary stdout/`--json` output are unaffected.
- --version

Store location: OS app data dir (override with TGCLI_STORE).
MCP: disabled by default (set `mcp.enabled` in config.json to true to serve MCP).

## auth
- auth [--qr] [--qr-file <path>] [--force-sms]
  - Interactive login + session bootstrap only: writes the session and exits. It
    does not sync or run a worker. Archiving runs in the always-on server, which
    auto-starts on the next command that needs it (or via `tgcli server`).
  - --qr for QR code login (scan in Telegram app).
  - --qr-file saves the QR code as a PNG image (useful for agents).
  - --force-sms forces code delivery via SMS instead of in-app notification.
- auth status
- auth logout

## backfill
A chat given by username is archived under its Telegram id, which the server asks
Telegram for; a username Telegram does not know is an error and queues nothing. The
same goes for `channels watch`, `tags set` and `topics`.

Backfill work is executed by the always-on control server, not in the CLI
process. Commands that need the server (`backfill --chat`, `backfill wait`)
auto-start `tgcli server --idle-exit 60s` in the background if one isn't already
running, talk to it over the loopback control API, and the server shuts itself
down once the queue drains and nothing is watched. The CLI stays a thin,
one-shot client.

- backfill --chat <id|username> [--depth N] [--min-date ISO] [--background]
  - Enqueue a single-chat backfill on the server (auto-starting it).
  - Foreground (default): follows progress to terminal, printing
    `Backfilling <title>: <done>/<target> (NN%)` to stderr (suppressed by
    `--quiet`) and a final summary on stdout. Exits non-zero if the job errors.
  - **Ctrl-C detaches** instead of cancelling: it prints a note and exits 0; the
    server keeps draining the job. Use `backfill cancel --chat <id>` to stop it.
  - `--background`: enqueue and return immediately, printing `{ jobId, channelId,
    status }` (one human line, or JSON with `--json`).
  - Long-running: no default timeout (a user-supplied `--timeout` is honored).
- backfill status [--json]
  - Snapshot: queue counts, whether the server is up, and each in-progress /
    pending backfill (title, message_count/target, %, cursor date, updated_at).
- backfill count [--json]
  - Print the number of in-progress backfills (cheap; brief read lock).
- backfill wait [--json]
  - Auto-start the server if needed, then block until no pending/in-progress
    backfills remain, showing progress (respects `--quiet`).
- backfill cancel --chat <id|username>
  - Cancel a chat's backfills via the server when one is up; falls back to a
    direct cancel (same as `backfill jobs cancel --channel`) when none is.
- backfill repair --chat <id|username>|--all [--since ISO|7d] [--after-id N] [--dry-run] [--background]
  - Fill holes that older versions left in the archive: walk the chat's history from
    the newest message down to the bound and write back what the archive lacks.
    Cursors stay put, and it goes no further back than when the chat entered the
    archive: older history is what `backfill --depth` is for. Without a bound it reads
    the last 30 days; `--since` takes an ISO date or a duration with a unit (`7d`), and
    `--after-id` alone sets no date bound. `--all` leaves out chats the last dialog list
    did not have (ones the account left); `--chat` still takes them. Runs on the server
    between its other jobs, one page a second; follows progress like `backfill --chat`,
    and Ctrl-C detaches. A chat Telegram refuses to read is skipped; exits non-zero if a
    repair fails. `--dry-run` reads the same pages and only counts what is missing; it
    costs as many requests as the repair itself. See [archive-recovery.md](archive-recovery.md).
- backfill (no --chat): track the server's queue (alias: `sync`)
  - `--follow`: auto-start the server if needed, track the queue with live
    progress, and exit once it drains. The server keeps running on its own
    (realtime continues while it is up / has watched chats).
  - `--once`: deprecated alias of `backfill wait` — drain quietly and exit.
  - With no flag: prints usage (there is no queue to track).
- backfill jobs list [--status] [--limit] [--channel]
- backfill jobs add --chat <id|username> [--min-date ISO] [--depth N]
  - Enqueue a job on the server (auto-starting it); the server's queue processes it.
- backfill jobs retry [--job-id] [--channel] [--all-errors]
  - Reset errored job(s) to pending on the server; the server's queue reprocesses them.
- backfill jobs cancel --job-id|--channel
- `sync` remains a silent alias of `backfill` (and every subcommand), so existing `sync …` invocations keep working unchanged.

## server
- server [--idle-exit DURATION]
  - Start the background sync service (MCP HTTP server runs only when enabled in config).
  - Writes everything the server prints to `server.log` in the store, also when it
    runs in the background and including a failure to start; at 5 MB the file moves
    to `server.log.1` and a new one starts.
- server stop [--force]
  - Ask the running server to shut down (over the control API; SIGTERM for an older
    server) and wait until its process has exited. Requests other CLIs are waiting on
    get up to 10 seconds to finish, and the job it was backfilling goes back to the
    queue. `--force` kills a server that has not exited after a minute. Prints
    `No tgcli server is running.` and exits 0 when there is none.
  - `--json`: `{ running, stopped, server: { pid, version, autoStarted, manager, method } }`.
- server restart [--force]
  - Stop the running server, if any, and start this CLI's version in its place. A
    server run by `tgcli service` is left for its service manager to start again;
    if it has not within 10 seconds, the command fails and says to run
    `tgcli service start`.
  - `--json`: `{ previous: { pid, version, ... } | null, current: { pid, version, startedAt } }`.
- server logs [-n N] [--follow]
  - Print the last N lines of the server log (default 100). `--follow` keeps printing
    new lines, across rotations, until Ctrl-C or `--timeout`.
  - `--json`: `{ path, exists, lines }`; with `--follow`, one `{ "line": ... }` object
    per line.

- server stats [--json]
  - Counters of the running server, since it started and over the last hour (whole
    minutes, so the window is 59–60 minutes and its start is printed). Reads only: it
    neither starts a server nor replaces an outdated one, and exits non-zero when none
    is running. A server too old to keep counters says to restart it. Asking does not
    count as activity, so it keeps no idle server up.
  - Requests to Telegram, by MTProto method. Counted once, right below mtcute's
    flood-wait and internal-error retries, so each retry counts too. A resend mtcute
    makes inside one send (a DC migration) counts once, and a call that fails before
    it is sent (no connection) still counts, with its error.
  - Request errors by name and method (`FLOOD_WAIT_23` counts as `FLOOD_WAIT`; a
    request that throws, such as a timeout, counts under the exception's class, e.g.
    `MtTimeoutError`), and the flood waits with the seconds Telegram asked for.
  - Held back by mtcute: after a flood wait longer than it sleeps through, mtcute
    answers further calls of that method with the remembered `FLOOD_WAIT` itself,
    without asking Telegram. Those calls are neither requests nor request errors and
    are listed here instead (only when there are any).
  - Deleted messages read again: ids queued (a difference called them deleted, or a page
    of history skipped them), reads done, found alive, put back in the archive, reads
    that failed, and ids given up on.
  - `backfill repair`: pages read, messages checked and added (a `--dry-run` counts what
    it would add separately), repairs finished, skipped (the chat is gone) and failed.
  - Gaps after a too-long channel difference: pages that brought new messages, messages
    added, gaps closed, fills that failed.
  - Backfill jobs: passes, jobs finished, passes that failed.
  - `--json` prints `{ pid, version, startedAt, at, sinceStart, lastHour }`; each of the
    last two has `since`, `requests { total, byMethod }`,
    `errors { total, floodWait { count, seconds }, byError, byMethod, heldBack { total, byMethod } }`,
    `rechecks`, `repair`, `gaps` and `jobs`.
  - Counters live in memory and start over with the server. Only method names, error
    names and counts are kept.

Every command except `server` and `service` checks the running server's version
before it runs. A server that an older CLI started on its own is replaced by this
version right away, unless it has backfill jobs queued or is answering another
CLI's request; a newer server is left alone, and a server running from the CLI's
own directory on another version is always replaced. A dev build run from a clone
(`2.2.6-dev+<commit>`) counts as newer than the release it grew from, and while a CLI
of that build has used its server in the last 10 minutes no other version replaces
it. `server restart` switches to the caller's version in any direction. See
`docs/development.md`.

## service
- service install
- service start
- service stop
- service status
- service logs

## doctor
- doctor [--connect]
  - Checks auth, lock, FTS, last sync, queue state. For an archive started before 2.2.5,
    suggests `backfill repair --all` until it has run.
  - `USERNAME KEYS:` counts chats an older version stored under a username (a chat
    given as `--chat @name`) instead of its Telegram id, next to the rows updates write
    by id. The server moves each to its id after it logs in to Telegram, with nothing
    to run: messages already under the id stay, duplicates are dropped, jobs and
    repairs carry over. When Telegram no longer knows the username, the messages under
    it decide: if they all come from one chat (each archived message records its chat),
    they move there; with no message under it at all, its rows are dropped along with
    its jobs and repairs, which could never run. A chat whose messages come from several
    chats, or from another one than Telegram gives for the username, stays as it is.
    One line per chat says what becomes of it, naming the reason for a chat that stays.
    With `--json` the list is under `usernameKeys`.
  - The `UPDATE:` line reports what the last update check found: a newer version
    and the command that installs it, `up to date` with the time that version was
    last confirmed, `not checked yet`, or why there is no check, plus the error of a
    failed last check. With `--json` the same facts are under `update`.
  - `LEFT:` counts chats whose sync the server turned off because you left them (see
    `channels unwatch`), and `DIALOGS:` the synced chats missing from your dialog list
    that Telegram says you are still in. With `--json`: `leftChats`.
  - `UNANSWERED:` lists synced chats missing from your dialog list that Telegram gives
    no answer about, most often a private chat you were removed from (Telegram refuses
    it the same way it refuses a stale access hash, so the server does not act on it).
    Nothing arrives from them; `channels unwatch --missing` turns them all off. With
    `--json`: `unansweredChats`.

## Update notice
Once a day tgcli asks the npm registry for the latest `@kfastov/tgcli` and keeps the
answer in `update-check.json` in the store. The request runs in a detached background
process, so no command waits for it; the notice is decided from the cache alone, which
means a new release shows up on the first command after the check that found it.

When the cache knows a newer version, one line goes to stderr after a command
succeeds, with the command for how this copy was installed:

```
Update available: tgcli 2.2.5 -> 2.3.0. Run `brew update && brew upgrade tgcli` (TGCLI_NO_UPDATE_CHECK=1 turns this notice off).
```

- Install method comes from where the package sits: under Homebrew's `Cellar/tgcli`
  the command is `brew update && brew upgrade tgcli` (Homebrew refreshes taps only
  once a day on its own, so a bare `brew upgrade` can miss a release made today);
  under a global npm `lib/node_modules` (or `%AppData%\npm\node_modules` on
  Windows) it is `npm i -g @kfastov/tgcli`. Anything else (a git clone, an npx
  cache, a project dependency, pnpm, the Nix store) gets no check and no notice.
  So does a version with a prerelease suffix such as `2.3.0-dev`.
- Never on stdout. Never with `--json` or `--quiet`, never after a failed command,
  and only when stderr is a terminal: pipes, redirects, cron and agents that capture
  output see nothing. An agent that drives a real terminal sees the line like a
  person would, at most once a day; `--json` or the switch below keeps it out.
  The background check still runs without a terminal, so the notice is ready when a
  person next runs tgcli.
- Shown at most once a day for the same version; a newer release shows at once.
- Not in `tgcli server`, which neither checks nor prints. `doctor` checks but prints
  its `UPDATE:` line instead of the notice.
- Off entirely, check included, when `TGCLI_NO_UPDATE_CHECK`, `NO_UPDATE_NOTIFIER` or
  `CI` is set to a value other than empty, `0`, `false`, `no` or `off`.
- The release workflow updates the Homebrew formula right after npm. If that step
  fails, Homebrew users see the notice before `brew upgrade` can install the release.

## channels
- channels list [--limit] [--query]
- channels show --chat <id|username>
- channels watch --chat <id|username>
  - Subscribe a chat for archiving: enables sync and queues a backfill job; run `tgcli backfill --once` or `tgcli backfill --follow` to process it.
- channels unwatch --chat <id|username> | --missing
  - Unsubscribe a chat from archiving (disables sync). `--missing` does it for every
    chat `doctor` lists under `UNANSWERED:`.
  - The server does this by itself for a channel or group you left: Telegram sends
    nothing from it, so the archive could not keep up. A synced chat missing from the
    dialog list is only a reason to ask Telegram, once a day; sync goes off when Telegram
    says you left or were removed, or the chat is gone, and comes back on, with what it
    missed, when the chat returns to your dialog list. Chats you asked for yourself
    (`channels watch`, `backfill --chat`) are never turned off this way, so a public
    channel archived without joining keeps its sync; chats you turned off stay off, and
    private chats are never asked about.
  - `channels sync --chat <id|username> --enable|--disable` remains as a hidden alias of watch/unwatch.

## topics (forum supergroups)
- topics list --chat <id|username> [--limit]
- topics search --chat <id|username> --query <text> [--limit]

## messages
- messages list [--chat] [--topic] [--source archive|live|both] [--after ISO] [--before ISO] [--before-id N] [--after-id N] [--limit]
- messages search <query> [--chat] [--topic] [--source] [--after] [--before] [--before-id N] [--after-id N] [--tag] [--regex] [--limit]
- messages show --chat <id> --id <msgId> [--source]
- messages context --chat <id> --id <msgId> [--before N] [--after N] [--source]

### Message fields

`list`, `search`, `show` and `context` return each message in the same shape, from
Telegram or from the archive alike (only `source` tells them apart):

- `channelId`, `peerTitle`, `username` — the chat.
- `messageId`, `date`, `editDate` (ISO, null when never edited), `out` (sent by you).
- `fromId`, `fromUsername`, `fromDisplayName`, `fromPeerType`, `fromIsBot` — the sender.
- `text`, `urls` (from the message's links), `media` (type, file id, name, size…).
- `topicId` — the forum topic, or the thread of a channel post a comment belongs to.
- `replyTo` — `{ messageId, topId, chatId, quote }` of the message this one answers, or null.
  In a forum a message that only sits in a topic answers nothing.
- `forward` — `{ date, fromId, fromName, messageId, postAuthor, imported }`. For a message
  imported from another app, `imported` is true and `date` is when it was first sent;
  the message's own `date` is the day of the import.
- `action` — what a service message did, named after Telegram's action without its
  `messageAction` prefix: `{ type: "chatAddUser", users: ["…"] }`, `{ type: "pinMessage" }`…
- `todo` — `{ title, othersCanAppend, othersCanComplete, items: [{ id, text, completedBy, completedAt }] }`.
- `poll` — `{ question, closed, quiz, multipleChoice, publicVoters, totalVoters, answers: [{ text, voters, chosen, correct }] }`.
- `groupedId` (the album a photo or file belongs to), `viaBotId`, `postAuthor`.

Archived messages get these fields from the Telegram message the archive keeps, so
messages archived by older versions have them too.

`search` takes FTS5 query syntax (`a OR b`, `pre*`); a query that is not valid syntax,
such as `UTF-8` or `example.org`, is searched as plain words.

### Pagination

Use `--before-id` and `--after-id` for cursor-based pagination by message ID:
- `--before-id N` — only messages older than message ID N (backward pagination)
- `--after-id N` — only messages newer than message ID N (forward pagination)

With `--json`, the response includes pagination metadata:
```json
{
  "source": "live",
  "returned": 50,
  "hasMore": true,
  "nextBeforeId": 429100,
  "messages": [...]
}
```
- `hasMore` — true when the number of returned messages equals the requested limit
- `nextBeforeId` — the ID of the oldest message in the batch; pass it as `--before-id` to get the next page

Example: paginating through history:
```bash
# Page 1
tgcli messages list --chat <id> --limit 50 --source live --json
# → hasMore: true, nextBeforeId: 12345

# Page 2
tgcli messages list --chat <id> --limit 50 --source live --before-id 12345 --json
# → hasMore: true, nextBeforeId: 11000

# Continue until hasMore: false
```

Legacy `--offset-id` is accepted as a hidden alias for `--before-id`.

## feedback
- feedback <message>
  - Sends feedback directly to the tgcli maintainer (@kfastov) via Telegram.
  - Override recipient: `tgcli config set feedback.chatId <username-or-id>`
  - Rate limited: 1 message per 60 seconds.
  - **What is sent:** your message text, plus a metadata footer containing: tgcli version, OS name (`process.platform`), and Node.js version. No other data (no username, chat history, file paths, or system info) is included. The message is sent from your authenticated Telegram account, so the recipient will see your Telegram profile.

## send
- send text --to <id|username> --message "..." [--topic <id>] [--parse-mode markdown|html|none] [--reply-to <id>] [--schedule <iso>] [--silent] [--no-preview] [--no-forwards] [--retries <n>] [--retry-backoff constant|linear|exponential|<ms>]
- send photo --to <id|username> --photo PATH [--caption "..."] [--topic <id>] [--parse-mode markdown|html|none] [--reply-to <id>] [--schedule <iso>] [--silent] [--no-forwards] [--spoiler] [--caption-above] [--retries <n>] [--retry-backoff constant|linear|exponential|<ms>]
- send file --to <id|username> --file PATH [--caption "..."] [--filename NAME] [--topic <id>] [--parse-mode markdown|html|none] [--reply-to <id>] [--schedule <iso>] [--silent] [--no-forwards] [--spoiler] [--caption-above] [--force-document] [--retries <n>] [--retry-backoff constant|linear|exponential|<ms>]
  - Sends are executed by the warm server (the single writer): the CLI routes the send through the control server, which runs the retry/`FLOOD_WAIT` logic against its live connection.
  - `--retries` defaults to `2` for all send commands. Retries and `FLOOD_WAIT` waits happen server-side.
  - Send commands default to a `30s` wall-clock timeout, which bounds the CLI's wait on the server. A stuck send fails fast instead of hanging. Override with `--timeout <duration>` (e.g. `--timeout 5m`) or disable with `--timeout 0`. On timeout the command exits non-zero with: `Send timed out after 30s (no response from Telegram).`
  - If Telegram returns a `FLOOD_WAIT` whose required wait exceeds the timeout budget, the command reports the rate limit (e.g. `Telegram rate-limited this send (FLOOD_WAIT 120s), which exceeds the 30s timeout.`) rather than silently timing out. Retry later or pass `--timeout 0`.

## media
- media download --chat <id|username> --id <msgId> [--output PATH]

## tags
- tags set --chat <id|username> --tags ai,news [--source]
- tags list --chat <id|username> [--source]
- tags search --tag ai [--source] [--limit]
- tags auto [--chat ...] [--limit] [--refresh-metadata]

## metadata
- metadata get --chat <id|username>
- metadata refresh [--chat ...] [--limit] [--force] [--only-missing]

## contacts (users)
- contacts search <query> [--limit]
- contacts show --user <id>
- contacts alias set --user <id> --alias "Name"
- contacts alias rm --user <id>
- contacts tags add --user <id> --tag <tag> [--tag ...]
- contacts tags rm --user <id> --tag <tag> [--tag ...]
- contacts notes set --user <id> --notes "..."

## groups (optional, permission-based)
- groups list [--query]
- groups info --chat <id>
- groups rename --chat <id> --name "New Name"
- groups members add --chat <id> --user <id> [--user ...]
- groups members remove --chat <id> --user <id> [--user ...]
- groups invite get --chat <id>
- groups invite revoke --chat <id>
- groups join --code <invite-code>
- groups leave --chat <id>

## folders
- folders list
- folders show <folder> [--resolve]
- folders create --title <name> [--emoji] [--include-contacts] [--include-non-contacts] [--include-groups] [--include-channels] [--include-bots] [--exclude-muted] [--exclude-read] [--exclude-archived] [--chat <id>...] [--exclude-chat <id>...] [--pin-chat <id>...]
- folders edit <folder> [--title] [--emoji] [flags...] [--chat <id>...] [--exclude-chat <id>...] [--pin-chat <id>...]
- folders delete <folder>
- folders reorder --ids <id1,id2,...>
- folders chats add <folder> --chat <id>
- folders chats remove <folder> --chat <id>
- folders join <link>

`<folder>` can be a numeric folder ID or folder title.

## Recovering archive gaps
`tgcli backfill repair` fills gaps that older versions left in the archive.
See [archive-recovery.md](archive-recovery.md).
