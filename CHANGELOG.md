# Changelog

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
