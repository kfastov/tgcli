# Changelog

## [2.3.0](https://github.com/kfastov/tgcli/compare/v2.2.5...v2.3.0) (2026-11-06)

### New

- `tgcli server stop`, `tgcli server restart` and `tgcli server logs` (with `--follow`). The server writes everything it prints to `server.log` in the store, rotated at 5 MB, also when it was started in the background.
- After an upgrade, the first command replaces a server still running the old version. A build run from a clone replaces the installed one the same way; see `docs/development.md`.
- `tgcli server stats` counts the server's requests to Telegram by method, rate limits and failures, rechecks of deleted messages, repair pages and gap fills, since start and over the last hour.
- Once a day tgcli asks npm for a newer version and, when there is one, prints a line on stderr with the command that installs it. `tgcli doctor` shows what it found; `TGCLI_NO_UPDATE_CHECK=1` turns the check off.
- The server turns sync off for a channel or group you have left, once Telegram confirms it, and back on if you rejoin. A chat missing from the dialog list is only a reason to ask, and chats you added yourself are left alone. `tgcli doctor` counts them under `LEFT:` and lists under `UNANSWERED:` the ones Telegram gives no answer about; `tgcli channels unwatch --missing` turns those off.
- `tgcli backfill repair --dry-run` counts what a repair would add without writing anything.

### Fixes

- `backfill repair` goes no further back than when a chat entered the archive, `--all` leaves out chats you have left, and a chat Telegram refuses to read is skipped instead of retried every hour. Before, a repair could pull in weeks of history from before you joined a chat, or from channels you had left.
- `tgcli backfill status` sums up repairs instead of printing a line for each chat.
- A chat given by username (`backfill --chat @name`, `channels watch`, the `scheduleMessageSync` MCP tool) is archived under its Telegram id. Older versions stored it under the username, which split and duplicated its messages; the server now merges such chats into their id after it logs in, and `tgcli doctor` lists any it could not.
- Archive reads by username (`messages list --chat @name` and the like) find the chat in the archive without asking Telegram.
- Paging through history takes half the requests: the server no longer asks Telegram whether it is logged in before every page.

## [2.2.5](https://github.com/kfastov/tgcli/compare/v2.2.4...v2.2.5) (2026-11-06)

### Fixes

- A running server no longer loses fresh messages in busy channels. In a burst of updates Telegram can name real messages as deleted in a channel difference and hide them from history for a while. The server now reads such messages again 1, 10 and 60 minutes later and archives whatever is there, with the cursors left alone. Deletes Telegram pushes in real time are taken as they are; in a two-hour measurement on a busy account that came to about 140 extra reads an hour.

### New

- `tgcli backfill repair` fills holes that older versions left in the archive. The server runs it between its other work, one page of history a second, and leaves the cursors alone. `tgcli backfill repair --all` covers the last 30 days of every synced chat; `--chat` with `--since` (a date or a duration such as `7d`) or `--after-id` sets other bounds. `tgcli doctor` suggests it until a repair has run. It replaces `scripts/recover-archive-gaps.js`; see `docs/archive-recovery.md`.
- Durations accept days, so `--timeout` and `--idle-exit` take `1d` too.

## [2.2.4](https://github.com/kfastov/tgcli/compare/v2.2.3...v2.2.4) (2026-11-04)

### Fixes

- `send --schedule` now schedules the message instead of sending it right away, and prints `Message scheduled`.
- After an upgrade, the CLI replaces an auto-started server of an older version instead of working through it.
- A running server no longer loses channel messages when min copies of a chat overwrite its access hash. It also fetches history when a channel's ids jump over the archive cursor, and compares the archive with the dialog list every hour.
- Messages missed while the server was off or the machine was asleep are caught up on the next start or wake-up, including chats in the archived folder.
- Messages sent through tgcli are archived, and an edit of an old message no longer moves the channel cursors.

### Holes already in an archive

This release stops new losses but does not repair old ones. To fill them, run `scripts/recover-archive-gaps.js` from a repository checkout after upgrading; see `docs/archive-recovery.md`.

Earlier versions: see [GitHub releases](https://github.com/kfastov/tgcli/releases).
