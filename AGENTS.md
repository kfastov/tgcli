# Repository Guidelines

## Project Structure & Module Organization
- `mcp-server.js`: MCP HTTP server built with `@modelcontextprotocol/sdk`, registering the Telegram tools (`listChannels`, `searchChannels`, `getChannelMessages`, `scheduleMessageSync`, `listMessageSyncJobs`).
- `telegram-client.js`: Domain logic for MTProto login, dialog traversal, and message helpers.
- `client.js`: Example CLI harness for manual testing without the MCP layer.
- `message-sync-service.js`: Background worker that archives messages into a local SQLite database.
- Store location: OS app-data dir (tgcli store; override with `TGCLI_STORE`); keep runtime artifacts out of version control.
- Documentation lives in `README.md` and `LIBRARY.md`; configuration relies on environment variables and the tgcli store.

## Build, Test, and Development Commands
- `npm install`: Restore dependencies whenever `package-lock.json` changes.
- `npm start`: Boot the MCP server on `http://localhost:8080/mcp` using Streamable HTTP transport; first run drives the Telegram login flow.
- `node client.js`: Run the sample script to exercise the client API and inspect dialog listings.
- `npm run build`: Currently a no-op placeholder—extend it only if a transpile/bundle step is introduced.
- `npm test`: Placeholder that echoes a notice; replace with real checks once tests exist.

## Coding Style & Naming Conventions
- Use ES modules with semicolons, two-space indentation, and `camelCase` identifiers.
- Keep tool names descriptive and aligned with Telegram operations (`listChannels`, `searchChannels`, etc.).
- Emit log lines that explain side effects (login state, MTProto calls, sync progress) and reference chat IDs/titles.
- Keep message sync queue strictly sequential; jobs transition through `pending → in_progress → idle` and may move to `error` if retries are needed.
- Store secrets in `.env`; never hard-code API credentials or session paths in commits.

## Testing Guidelines
- Add coverage when extending `telegram-client.js` or `message-sync-service.js` by mocking MTProto responses to validate session reuse and message archiving.
- Name new test files `<module>.test.js` under `tests/` or co-locate in `__tests__/`; make `npm test` execute them.
- Before pushing, run your test suite and a smoke `npm start` to verify authentication prompts and cache initialization remain intact.

## Commit & Pull Request Guidelines
- Follow Conventional Commits (`type: subject`) as seen in `docs(readme): add detailed login guide...`; use lowercase types for routine changes and `fix:` for bug patches.
- Keep commits atomic; include body details when altering session management or cache persistence.
- PR descriptions should summarize intent, list manual verification (commands run, Telegram scenarios exercised), link related issues, and attach console excerpts when tool output changes.
- Confirm no sensitive credentials or runtime artifacts from the tgcli store are committed before requesting review.

## Workflow
- On startup, read `backlog.md` and pull tasks from it.
- Thoroughly verify implemented functionality (tests, smoke runs, manual checks).
- Commit verified features before starting the next task.

## Agent skills

### Issue tracker

Track issues and specs in GitHub Issues for `kfastov/tgcli`; read `docs/agents/issue-tracker.md` before tracker operations.

### Triage labels

Use the five canonical triage labels; read `docs/agents/triage-labels.md` when triaging or applying status labels.

### Domain docs

Use a single-context layout (`CONTEXT.md` and `docs/adr/`); read `docs/agents/domain.md` before exploring the codebase.
