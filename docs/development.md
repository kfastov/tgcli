# Development

tgcli runs straight from a clone: `node cli.js` is the whole CLI, and the server it
starts runs from the same clone. Nothing needs to be copied, linked or installed
globally.

## Node

Use Node 22 (`.nvmrc`; `package.json` asks for 22 or later). CI runs the tests on
Node 22 and 24, and the Homebrew formula runs on `node@22`.

`better-sqlite3` is a native module built for the Node that ran `npm ci`. Run the
CLI and the tests with that same Node, or they fail with `NODE_MODULE_VERSION`. After
switching Node, run `npm rebuild better-sqlite3`.

With nvm:

```bash
nvm use
```

With Homebrew, put `node@22` first on the `PATH`:

```bash
export PATH="$(brew --prefix node@22)/bin:$PATH"
```

## Install and test

```bash
npm ci
```

```bash
npm test
```

A git worktree has its own `node_modules`, so run `npm ci` in each new one.

## Running from the clone

```bash
node cli.js --version
```

A clone reports a dev version: the next patch after `package.json`, marked `dev`,
with the commit, such as `2.2.6-dev+4ad3fa0`. It ranks above the release it grew
from and below the next release.

`node cli.js <command>` uses the same store as the installed `tgcli`
(`TGCLI_STORE` overrides it), so it works on your real account. There is one server
per store, and every command checks its version before it runs.

## Trying a change on the real account

1. Run any command from the clone, or bring the server over explicitly:

   ```bash
   node cli.js server restart
   ```

   The clone's version is newer than the installed release, so the first command
   from the clone replaces the installed CLI's server (once its backfill queue is
   empty; `server restart` does not wait).
2. Use it. `node cli.js server logs --follow` shows what the server does.
3. After you change code, restart the server so it runs the change. The version only
   follows commits, so the CLI does not notice uncommitted edits by itself:

   ```bash
   node cli.js server restart
   ```

4. Go back to the installed version:

   ```bash
   tgcli server restart
   ```

   Releases before `server restart` existed (2.2.5 and older) do not have it; stop
   the dev server instead, and the next `tgcli` command starts its own:

   ```bash
   node cli.js server stop
   ```

You do not have to go back by hand. While you test, `tgcli` commands from other
tools keep using the dev server and warn about it; once no command from the clone
has reached it for 10 minutes, the next `tgcli` command replaces it with the
installed version.

## How the server follows the CLI's version

Every command checks the running server's version first and replaces a server that
an older CLI started automatically, so after `brew upgrade` even `tgcli backfill
status` brings up the new version. The rules (see `versionTakeover` in
`core/control-client.js`):

- The newer version wins: a CLI replaces an older server, never a newer one, so two
  installed versions do not restart the server back and forth.
- A dev server that a CLI of its own version used in the last 10 minutes is left
  alone by every other version. After that, any CLI replaces it.
- A server you started yourself (`tgcli server`, `tgcli service`) and a server with
  queued backfill jobs are not replaced; the CLI warns on stderr instead.
- `tgcli server restart` switches to the caller's version in any direction.

## Server lifecycle

- `tgcli server stop` asks the server to shut down over the control API (an older
  server without that endpoint gets SIGTERM, which runs the same shutdown) and
  waits until the process has exited. The backfill job it was on goes back to the
  queue. `--force` kills a server that has not exited after a minute.
- `tgcli server restart` stops the server and starts this CLI's version.
- `tgcli server logs [-n N] [--follow]` prints the server log. Every server writes
  `server.log` in the store; at 5 MB it moves to `server.log.1` and a new file
  starts.

All three take `--json`.
