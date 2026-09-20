# Domain docs

## Layout and reading rules

This repo uses a **single-context** layout:

- `CONTEXT.md` at the repo root holds the domain glossary and model.
- `docs/adr/` holds architecture decision records.

Before exploring the codebase, read `CONTEXT.md` if present and the ADRs relevant to the area you are working on.

If these files do not exist, proceed silently. `/domain-modeling` creates them lazily as terminology and decisions are resolved, including when reached through `/grill-with-docs` or `/improve-codebase-architecture`.

## Use the glossary's vocabulary

Use the terms defined in `CONTEXT.md` in issue titles, proposals, hypotheses, and test names. If a needed concept is missing, check the project's existing language first; record a real vocabulary gap for `/domain-modeling`.

## Surface ADR conflicts

If a proposal contradicts an existing ADR, identify that ADR and explain why its decision should be revisited.
