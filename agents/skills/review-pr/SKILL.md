---
name: review-pr
description: Reviews a GitHub PR in this repo (diff-only, no checkout) against AGENTS.md conventions and general correctness/test-coverage concerns, and prints a findings report. Does not post to GitHub. Use for general (non-cluster) PRs; for cluster/raft/discovery/replication PRs use review-cluster-pr instead.
argument-hint: "<pr-number>"
allowed-tools:
  - Bash(gh pr view *)
  - Bash(gh pr diff *)
  - Bash(gh api *)
  - Bash(git *)
  - Read
  - Grep
  - Glob
---

# PR Review

Review PR **$ARGUMENTS** and write a findings report to `_pr-reviews/$ARGUMENTS.adoc`. **Never checks out the PR, never posts to GitHub, never approves** — this skill is read-only and advisory. Posting the review is a separate, manual step left to you.

## 1. Gather

```bash
gh pr view <PR> --json number,title,body,baseRefName,headRefName,labels,url
gh pr diff <PR>
```

Read the diff directly rather than checking out the branch — keeps the working tree untouched and avoids branch-switch bookkeeping. If the diff is large, prefer `gh pr diff <PR> --name-only` first and pull individual file contents via `gh api repos/{owner}/{repo}/contents/{path}?ref={headRefOid}` only for files you need full context on.

## 2. Check repo conventions (from AGENTS.md)

- **Commit messages** follow Conventional Commits — flag the PR title/commits if they clearly don't (e.g. no `feat:`/`fix:`/`chore:` type prefix).
- **Labels** — the PR should carry a label for its target branch (`dev`, `5.26`, etc.) and, if it only touches test code, `test-only`. Check `labels` from step 1.
- **Formatting/license headers** — `spotless:apply` and `license:format` are available as ChatOps commands (`/format`, `/license`) rather than enforced on every push. Don't nitpick formatting or license-header nits; if you spot one, just note that `/format` or `/license` will fix it.

## 3. Review the diff

Read through the diff yourself (no need to spawn subagents for a typical PR) and flag:

- **Correctness** — logic bugs, incorrect null/error handling, obvious race conditions, resource leaks.
- **Test coverage** — new behavior without a matching unit or `*IT` test; deleted/weakened assertions without justification.
- **API/surface changes** — public method signature changes, removed settings/procedures without a deprecation path.
- **Scope creep** — unrelated changes bundled into the PR that make it harder to review.

Keep findings terse: lead with the file/line and the concrete problem, not a narration of the diff. Classify each as `blocker`, `suggestion`, or `nit`.

## 4. Report

Write a findings report as a short asciidoc summary to `_pr-reviews/$ARGUMENTS.adoc` — do not post anything to GitHub:

```
= PR #<N>: <title>

**Change description:** <n>
**Blockers:** <n>
**Suggestions:** <n>
**Nits:** <n>

- [blocker] <file>:<line> — <finding>
- [suggestion] <file>:<line> — <finding>
- [nit] <file>:<line> — <finding>

Labels: <ok | missing target-branch label | missing test-only label>
```

If there's nothing to flag, say so plainly rather than inventing nits.

If the user wants the review actually posted, that's a separate explicit step (`gh pr review <PR> --comment/--request-changes --body ...`) — confirm the content with them first since posting is visible to others.

Keep the change description brief and high-level. If the PR adds feature, then describe the feature. If the PR is a bug, then fix describe the fix. If you are not sure whether is it a feature or a fix, say so and state your best guest at what it is (e.g. test improvements, new test facility, refactoring, etc.) 

Do not add linebreaks mid-sentence.  The viewer uses soft-wrap.

# Writing style

Use 80% of the way to ASD-STE100