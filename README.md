# tgcli

Telegram CLI with background sync and an optional MCP server for your personal account (MTProto, not bot API).

## Installation

```bash
npm install -g @kfastov/tgcli
```

```bash
brew install kfastov/tap/tgcli
```

## Authentication

Get Telegram API credentials:
1. Go to https://my.telegram.org/apps
2. Log in with your phone number
3. Create a new application
4. Copy `api_id` and `api_hash`

Then authenticate:

```bash
tgcli auth
```

## Quick start

```bash
tgcli auth
tgcli sync --follow
tgcli messages list --chat @username --limit 20
tgcli messages search "course" --chat @channel --source archive
tgcli send text --to @username --message "hello"
tgcli server
```

## Commands

```bash
tgcli auth           Authentication and session setup
tgcli config         View and edit config
tgcli sync           Archive backfill and realtime sync
tgcli server         Run background sync service (MCP optional)
tgcli service        Install/start/stop/status/logs for background service
tgcli channels       List/search channels
tgcli messages       List/search messages
tgcli send           Send text or files
tgcli media          Download media
tgcli topics         Forum topics
tgcli tags           Channel tags
tgcli metadata       Channel metadata cache
tgcli contacts       Contacts and people
tgcli groups         Group management
tgcli doctor         Diagnostics and sanity checks
```

Commands run through `tgcli server`, which the CLI starts on demand. After an upgrade, the CLI replaces a server it started itself as soon as its backfill queue is empty. A server you started yourself with `tgcli server` or `tgcli service` keeps running the old code until you restart it; the CLI warns about this on stderr.

Use `tgcli [command] --help` for details. Add `--json` for machine-readable output.

Once a day tgcli checks npm for a newer release in the background and, at a terminal, prints one line on stderr with the upgrade command (`brew update && brew upgrade tgcli` or `npm i -g @kfastov/tgcli`). Set `TGCLI_NO_UPDATE_CHECK=1` to turn it off; see [docs/cli.md](docs/cli.md#update-notice).

## MCP (optional)

Enable it via config:

```bash
tgcli config set mcp.enabled true
```

By default the server binds to `http://127.0.0.1:8080/mcp`. To change it:

```bash
tgcli config set mcp.host 127.0.0.1
tgcli config set mcp.port 8080
```

Then run `tgcli server` and point your client at the configured address.

## Configuration & Store

The tgcli store lives in the OS app-data directory and contains `config.json`, sessions, and `messages.db`.
Override the location with `TGCLI_STORE`.

Legacy version: see `MIGRATION.md`.
