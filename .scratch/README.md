# .scratch: the issue tracker

Plans and issues for this repository live here, as Markdown, in the repository itself: they
are reviewed in the same pull requests as the code they describe, and an agent reads them
with no tracker access.

This repository is public. Nothing here may contain a secret, a customer's data, or the
details of an unfixed vulnerability (those go through [SECURITY.md](../SECURITY.md)).

## Layout

```
.scratch/
  <feature-slug>/
    PRD.md                     the plan for the feature (optional)
    issues/
      01-<slug>.md             one implementation issue per file, numbered from 01
      02-<slug>.md
```

- One feature per directory, named with a short lowercase slug (`claim-check-gc`).
- Issues are vertical slices: each one is independently mergeable and testable.
- Number issues in the order they should land; never renumber, so references stay valid.

## An issue file

```markdown
# <title>

Status: needs-triage

## What

What is wrong or missing, and who it affects.

## Done when

- [ ] observable acceptance criteria, each one testable

## Notes

Seams touched (docs/architecture.md), PRD sections, decisions.

## Comments

<!-- appended below, newest last: "**<who>, <date>:** text" -->
```

## Status

The `Status:` line near the top of each issue holds exactly one of:

| Status | Meaning |
|---|---|
| `needs-triage` | a maintainer has to evaluate it |
| `needs-info` | waiting on the reporter for more information |
| `ready-for-agent` | fully specified; an agent can pick it up without asking |
| `ready-for-human` | needs a person (a decision, access, judgement) |
| `wontfix` | will not be done; the last comment says why |

When an issue is done, the pull request that closes it deletes the file and names it in the
description; the history stays in git.

## Comments

Conversation is appended under `## Comments` at the bottom of the file, newest last. Edit the
body only to make it more accurate, never to rewrite what was agreed.
