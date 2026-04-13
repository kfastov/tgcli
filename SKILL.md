---
name: tgcli
description: Telegram CLI for reading/searching messages, syncing archives, and sending or downloading files. Use when the user asks about Telegram chats, messages, contacts, groups, or files.
---

# tgcli

Telegram CLI with background sync.

## When to Use

Use this skill when the user:
- Wants to read or search Telegram messages
- Needs recent updates or an inbox-style view of chats
- Asks to send a Telegram message or file
- Wants to download media or files from Telegram
- Wants to look up channels, groups, or contacts
- Needs archive/backfill sync for a chat

## Install

```bash
npm install -g @kfastov/tgcli
```

Or:
```bash
brew install kfastov/tap/tgcli
```

## First-Time Setup

```bash
# 1. Authenticate (needs API credentials from https://my.telegram.org/apps)
tgcli auth

# 2. Verify authentication succeeded
tgcli channels list --limit 1

# 3. Install and start background sync service
tgcli service install
tgcli service start

# 4. Begin syncing messages
tgcli sync --follow
```

If auth fails or sync doesn't start, run `tgcli doctor` to diagnose. Verify API credentials are correct at https://my.telegram.org/apps.

## Common Commands

### Reading
```bash
tgcli channels list --limit 20
tgcli messages list --chat @username --limit 50
tgcli messages search "query" --chat @channel --source archive
tgcli topics list --chat @channel --limit 20
```

### Files & Media
```bash
tgcli media download --chat @channel --id 12345
tgcli send file --to @channel --file ./report.pdf --caption "FYI"
```

### Writing
```bash
tgcli send text --to @username --message "Hello"
```

### Sync & Service
```bash
tgcli sync --follow
tgcli sync jobs add --chat @channel --min-date 2024-01-01T00:00:00Z
tgcli service install
tgcli service start
```

### Contacts & Groups
```bash
tgcli contacts search "alex"
tgcli groups list --query "Nha Trang"
```

## Output Formats

All commands support `--json` for structured output:

```bash
tgcli messages list --chat @username --limit 5 --json
tgcli channels list --limit 10 --json
```

## Notes

- Use `--source live|archive|both` when listing or searching messages.
- `--json` is best for AI/tooling pipelines.
- Run `tgcli [command] --help` for full option details on any command.
- See `tgcli doctor` for diagnostics if something isn't working.
