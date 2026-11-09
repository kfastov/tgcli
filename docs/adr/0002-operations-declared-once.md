# ADR 0002: Every operation is declared once

## Status

Accepted.

## Context

Every Telegram and archive operation the warm server runs had three copies of its
interface. `cli.js` declared the commander options, checked them by hand (`--chat is
required`, `parsePositiveInt`), mapped them to an args object and rendered the result;
`mcp-server.js` declared a second zod schema, mapped its params to the same args
object, and shaped the result again; `core/operations.js` documented the args in a
comment and normalized them a third time. One command cost about seven edits in four
files, and the copies had drifted:

- MCP had no `beforeId`/`afterId`, folders, `sendPhoto`, `watch`, or channel details;
  six MCP tools bypassed the operations entirely.
- The two surfaces returned different JSON for the same operation (topics, invite
  links, joined groups, send results, cached metadata).
- The control API accepted any args object; validation lived only in the CLI.
- Paths crossed the seam unresolved, so a relative `--output` or `--file` resolved
  against the server's working directory, not the caller's.
- `send text --no-preview` never reached the server: commander stores `--no-preview`
  as `preview`, and the handler read `noPreview`.

## Decision

An **operation** is one declaration in `core/ops/<domain>.js`, made with
`defineOperation`:

- `name`: the wire name the control API dispatches and the MCP tool name.
- `command`: the CLI path (`groups members add`), or `null` for an MCP-only operation;
  `mcp: false` keeps an operation off MCP. Either exception is visible in the
  declaration.
- `input`: one field per argument. A field is a zod schema plus its CLI spelling
  (flag, hidden aliases, positional, repeatable list). The fields build one strict zod
  object; `refine` adds checks across fields. Messages name fields as `{beforeId}`, and
  each surface prints its own spelling (`--before-id` on the CLI, `beforeId` over the
  control API and in the checks across fields over MCP; the MCP SDK reports a value
  of the wrong type in its own words before the operation sees it).
- `annotations`: `readOnly`, `destructive`, `idempotent`, sent to MCP clients as hints.
- `telegram`: whether the server logs in before running it (default true).
- `client`: what runs on the caller's side of the seam: `prepare` (make paths absolute
  against the caller's cwd), the default request timeout, and error mapping.
- `run(services, args)`: the handler. It returns the one JSON payload every surface
  shows: `--json` prints it, MCP returns it.
- `render(payload, { args, print, warn })`: the human CLI output.

From the declarations:

- `core/ops/cli.js` builds the commander commands; the CLI validates with the schema
  before it calls the server.
- `core/ops/mcp.js` registers one MCP tool per operation, with the same schema and the
  annotations.
- `core/ops/execute.js` is the server's single entry: it validates again (the control
  API no longer trusts its caller), logs in when the operation needs Telegram, and runs
  it. The control API's `/control/run` and every MCP tool go through it.

The operations travel to `/control/run`, a path older servers do not have: their
`/control/invoke` dispatched other names with other args (`chat` for `channelId`,
`foldersEdit` with a nested `modification`), and a same-named operation could run with
args it reads differently. An older server answers the new path with 404, and the CLI
says to restart it; a newer server answers an older CLI's `/control/invoke` with 400 and
says to update tgcli or restart the server.

Commands that manage the process or the store rather than run an operation on the
server stay hand-written in `cli.js`: `auth`, `config`, `feedback`, `backfill`/`sync`,
`server`, `service`, `doctor`.

### MCP compatibility

Tool names stay: an operation that had an MCP tool takes that tool's name as its own
(`setChannelTags`, `messagesSend`, `contactsGet`). Params follow one vocabulary
(`channelId`, `channelIds`, `userId`, `messageId`, `topicId`, `fromDate`, `toDate`,
`beforeId`, `afterId`), which is what most tools already used. The schemas are strict,
so a param a tool no longer takes fails loudly instead of being dropped. Changed for
MCP clients:

- `messagesList` takes `channelIds` (one chat or a list) instead of `channelId`, like
  `messagesSearch`; `messagesSearch` drops its `channelId` and `tag` aliases.
- `getChannelMetadata` falls back to Telegram for a chat the archive has no metadata
  for, as `metadata get` does, and so logs in first; `groupsJoin` takes a bare invite
  hash or `t.me/...` link, as `groups join` does; `listChannels` takes a `query`.
- `setChannelTags` returns the chat under the id the archive keeps it (as the CLI
  did), and takes an empty list to clear the tags.
- The MCP server names itself `tgcli` with the release version instead of
  `example-mcp-server` 1.0.0.
- `messagesSend` and `messagesSendFile` retry twice by default, as the CLI sends do;
  pass `retries: 0` for a single attempt.
- Results take the richer of the two former shapes: invite links carry their dates and
  limits on the CLI too, topics come formatted on the CLI too, send results carry
  `attempts` over MCP too, and message reads report `usedLiveFallback` in JSON.
  `messagesSendPhoto` keeps the snake_case payload `send photo --json` has always
  printed (`chat_id`, `message_id`), so scripts reading it keep working; the other two
  sends keep their camelCase one.

On the CLI, an option and its hidden alias given together must agree (`send --to`
and `--chat`), and options are checked against the same schema before the server
sees them.

A server the CLI does not replace by itself (one started with `tgcli server` or
`tgcli service`, one busy with backfill, or one still answering another CLI) that
predates `/control/run` needs one `tgcli server restart` after the upgrade: until then
its commands fail with that advice instead of running on the old code.

`docs/mcp-tools.md` is written from the declarations (`node
scripts/gen-mcp-tools-doc.js`), and a test fails when it falls behind them.

## Consequences

- A new operation is one declaration; the CLI command, the MCP tool, validation on both
  sides of the seam, and the help text come from it.
- MCP reaches every operation the CLI has, except those a declaration opts out.
- The control API rejects malformed args with a 400 that names the field.
- `--timeout` bounds every operation's request, not only sends.
- Out of scope: the hand-written commands above (their server calls use the bespoke
  `/control/backfill|retry|cancel|repair` endpoints), streaming progress events back to
  the CLI (#50), and the message read logic itself (source fallback, live paging).
