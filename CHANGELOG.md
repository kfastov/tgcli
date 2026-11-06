# Changelog

## [2.3.0](https://github.com/kfastov/tgcli/compare/v2.2.5...v2.3.0) (2026-11-06)


### Features

* add backfill repair --dry-run ([2ea92fd](https://github.com/kfastov/tgcli/commit/2ea92fdaf8455d438016526400be149561309acb))
* add server stats with request and archive counters ([df8587b](https://github.com/kfastov/tgcli/commit/df8587b7b72ef086716f3a7dc5c10ea0eb95ec59))
* list in doctor the chats Telegram gives no answer about ([2acc15d](https://github.com/kfastov/tgcli/commit/2acc15dc2b81f1197a8d200881141b533665b047))
* **server:** stop, restart and log the server, and keep it on the CLI's version ([26197e1](https://github.com/kfastov/tgcli/commit/26197e182ca059ffd0a233b4ce4267e767e41b6b))
* tell the user about a new tgcli release ([1f31a51](https://github.com/kfastov/tgcli/commit/1f31a51116f7ea89cdf230b06a8f31b702221754))
* turn sync off for chats the account left ([fae3d42](https://github.com/kfastov/tgcli/commit/fae3d4271eb89d990063e8aa326c754c059e1b3c))


### Bug Fixes

* address review of repair bounds and dry runs ([387cc17](https://github.com/kfastov/tgcli/commit/387cc17e4ae3cfe637f2aa44b80d036e38840b9c))
* address review of turning sync off for left chats ([bdb244e](https://github.com/kfastov/tgcli/commit/bdb244edd21637afdc7d2649e584a14721cc550d))
* archive chats given by username under their Telegram id ([ec257f2](https://github.com/kfastov/tgcli/commit/ec257f23747361446a675ddd4a21a8ae98c32033))
* find chats given by username for reads without Telegram ([758d18e](https://github.com/kfastov/tgcli/commit/758d18e30a42eba05b0d34167bfd6abb0f6d985f))
* keep backfill repair to what the archive followed ([5e72e45](https://github.com/kfastov/tgcli/commit/5e72e45a87234f90342e3753a403f6da5e808ffa))
* keep chats the user asked for out of the unanswered list ([e383012](https://github.com/kfastov/tgcli/commit/e383012cdaa71bc453f7f4baa0849ce5a63188ea))
* let Telegram decide when a stored username may be stale ([9619c21](https://github.com/kfastov/tgcli/commit/9619c210715e21dd40ed1db2a9131d4c95f75066))
* **server:** keep other servers' control.json, requests and log lines intact ([737771c](https://github.com/kfastov/tgcli/commit/737771c0e7189f27ef7ca777de4f1f024df8f0f2))
* stop asking Telegram whether the session is logged in before every page ([47f4d18](https://github.com/kfastov/tgcli/commit/47f4d184106e8e16fefc7497999cfbda90478f8b))
* stop the queue loop when the archive closes ([8dfcc89](https://github.com/kfastov/tgcli/commit/8dfcc8909f747a285f7ca652fc5d4f6a8b790881))
* sum up repairs in backfill status ([49f5e07](https://github.com/kfastov/tgcli/commit/49f5e0726ecdc51f77276d7637ab2853f34abfd8))

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
