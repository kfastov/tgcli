# Releasing

Releases need no manual version bump, tag or release notes.

1. Land changes on `main` with Conventional Commits. `fix:` makes a patch release,
   `feat:` a minor one, and `feat!:` or a `BREAKING CHANGE:` footer a major one.
   `docs:`, `chore:`, `ci:`, `test:` and `refactor:` do not trigger a release.
   Squash-merge every pull request, with its title as the commit title: each `fix:` and
   `feat:` on `main` becomes a changelog line, so the title says what changed for users
   ("feat: add tgcli server stats"), and review fixes and refactors stay inside the
   squash instead of reaching the changelog as "fix: address review".
2. On every push to `main`, the Release workflow keeps a release PR open
   (`chore(main): release X.Y.Z`). It bumps `package.json` and `package-lock.json` and
   adds the entries to `CHANGELOG.md`. Before merging it, read the entries as a user
   would and rewrite them in that PR where they read like internals.
3. Merge the release PR. The workflow tags `vX.Y.Z`, creates the GitHub release, runs
   the tests, publishes the package to npm through trusted publishing (no token) and
   points the Homebrew formula in `kfastov/homebrew-tap` at it.

The CI workflow runs the tests on every push and pull request: Linux with Node 22 and
24, and macOS with Node 22, the version the Homebrew formula uses. Pull requests opened
by the release workflow itself get no CI run, because GitHub does not start workflows
for its own token; the commits in them were tested on `main`.

## When a release stops halfway

Run the Release workflow by hand (Actions → Release → Run workflow) with the tag, such
as `v2.2.5`. It skips npm if that version is there already and rewrites the formula
only if it points elsewhere.

## Setup it relies on

- npm trusted publishing for `@kfastov/tgcli`, bound to `.github/workflows/release.yml`.
  Renaming that file breaks publishing until the npm setting follows.
- The `HOMEBREW_TAP_SSH_KEY` secret: a deploy key with write access to the tap.
- Settings → Actions → General → "Allow GitHub Actions to create and approve pull
  requests", so the workflow can open the release PR.
- Settings → General → Pull Requests: squash merging only, with the pull request title
  as the commit title, so every pull request reaches `main` as one changelog line.
