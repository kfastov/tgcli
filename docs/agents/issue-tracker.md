# Issue tracker: GitHub

Issues and specs for this repo live in GitHub Issues at https://github.com/kfastov/tgcli/issues. Use the `gh` CLI with `--repo kfastov/tgcli` for tracker operations; the clone also has a separate `dapi` remote.

`backlog.md` remains a local planning summary. Fetch the relevant GitHub issue for its specification, discussion, and status.

## Conventions

For multiline bodies, write the exact Markdown to a temporary file and pass `--body-file <path>`.

- **Create an issue**: `gh issue create --repo kfastov/tgcli --title "..." --body-file <path>`.
- **Read an issue**: `gh issue view <number> --repo kfastov/tgcli --json number,title,body,labels,comments,state`.
- **List issues**: `gh issue list --repo kfastov/tgcli --state open --json number,title,body,labels,comments`, with appropriate `--label` and `--state` filters. Set `--limit` to cover the required queue; use pagination for exhaustive inventories.
- **Comment**: `gh issue comment <number> --repo kfastov/tgcli --body-file <path>`.
- **Apply or remove labels**: `gh issue edit <number> --repo kfastov/tgcli --add-label "..."` or `--remove-label "..."`. Use the role mapping in `docs/agents/triage-labels.md`.
- **Close**: `gh issue close <number> --repo kfastov/tgcli`. If an explanation is needed, post it first with the comment command above.

## Pull requests as a triage surface

**PRs as a request surface: no.**

GitHub shares a number space across issues and PRs. When a bare reference is ambiguous, resolve it with `gh pr view <number> --repo kfastov/tgcli` and fall back to `gh issue view` only when it is not a PR.

## When a skill says "publish to the issue tracker"

Create a GitHub issue using the conventions above.

## When a skill says "fetch the relevant ticket"

Read the GitHub issue, including its body, labels, state, and comments.

## Wayfinding operations

Used by `/wayfinder`. The map is one issue; its children are tickets.

- **Map**: an issue labelled `wayfinder:map`, with Notes / Decisions-so-far / Fog in its body.
- **Child ticket**: link the issue to the map as a GitHub sub-issue using `gh api`. If sub-issues are unavailable, add the child to a task list in the map body and put `Part of #<map>` at the top of the child body. Use `wayfinder:<type>` labels (`research`, `prototype`, `grilling`, or `task`).
- **Blocking**: use native GitHub issue dependencies where available. Add a blocker with `gh api --method POST repos/kfastov/tgcli/issues/<child>/dependencies/blocked_by -F issue_id=<blocker-database-id>`. Obtain the numeric database ID with `gh api repos/kfastov/tgcli/issues/<blocker> --jq .id`. If dependencies are unavailable, put `Blocked by: #<number>, ...` at the top of the child body; the ticket is unblocked when every blocker is closed.
- **Frontier**: enumerate the map's open children in map order. Skip assigned children and children with open blockers (`issue_dependencies_summary.blocked_by > 0`, or an open issue in the fallback `Blocked by` line). Choose the first remaining child.
- **Claim**: `gh issue edit <number> --repo kfastov/tgcli --add-assignee @me`, as the session's first tracker write.
- **Resolve**: comment with the result, close the child, then update the map's Decisions-so-far with a short explanation and a link.
