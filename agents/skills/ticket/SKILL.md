---
name: ticket
description: Analyse a ticket and propose a fix.
argument-hint: "<linear-issue-id> [ <remarks>]"
allowed-tools:
  - Bash(gh pr view *)
  - Bash(gh pr diff *)
  - Bash(gh api *)
  - Bash(git *)
  - Read
  - Grep
  - Glob
---

# Ticket

Arguments: $ARGUMENTS

The first token of the arguments is the ticket ID (for example SURF-1625). 
Everything after it is context from the user: a hypothesis, a hint, or a follow-up question. 
Use it to guide the investigation, but verify it like any other claim. 
It never goes into the filename or the title.

The report lives at `_projects/<ID>.adoc`.

**Mode selection.** 
If `_projects/<ID>.adoc` (or an older `_projects/<ID>*.adoc`) already exists, follow Section 5, "Iterating on an existing report", instead of sections 1–4. 
Otherwise, do the initial analysis below.

## Initial analysis

Look at the Linear issue with that ID.
Analyse the potential root cause, propose one or more possible fixes, and consider where to add tests and what to test.
Write your findings to `_projects/<ID>.adoc`.
Do not change any other code.

## 1. Report

Write a findings report as a short asciidoc summary to `_projects/$ARGUMENTS.adoc`:

```
= $ARGUMENTS

<short description of the issue>

The full description of this ticket can be found on the linear issues $ARGUMENTS.

== Root cause analysis

<potential root cause>

== Proposed fixes

<one or more possible fixes>

== Where to add tests

<where to add tests>

== Which tests to add

<what to test in these tests>

'''
Editorial note for whoever continues this investigation:

- This document is written and maintained as a single coherent analysis, not a change log.
When re-investigating, rewrite the relevant section in place rather than appending a dated addendum or referring to "an earlier pass" — a root cause that turns out to be wrong, or a fix that is a dead end, should simply be removed, not narrated as superseded.
- Do not add linebreaks mid-sentence.
The viewer uses soft-wrap.

The one exception is this note itself.
```

## 2. Analysis

The ticket's main description may come from outside Neo4j and may be inaccurate, or it may already be fixed.
Hence, verify the claims about behavior the ticket is making.

Also what the ticket's main description is claiming as the correct behavior, maybe mistaken.
Hence, form your own opinion about what the correct behavior should be and provide a rationale for it.

## 2. Fixes

The description of the fix should be sufficiently detailed so that it can be implemented by a developer or agent alike, when being pointed at the report.

## 3. Tests

For tests, always consider but not limit yourself to cucumber scenarios under `private/enterprise/cypher/compatibility-spec-suite/src/main/resources/features`.

Consider

## 5. Iterating on an existing report

Use this mode when the report already exists, and for any follow-up question about the ticket later in the same session. 
The report is the state that carries between follow-ups; the conversation is not.
If an older report has a long argument-derived filename, rename it to `_projects/<ID>.adoc` with `git mv` if tracked, or `mv` otherwise, and say so.

### Quick path

If the question is a lookup that does not change the analysis (a tag name, a file path, a one-line summary), answer it directly.
Then check whether the answer belongs in the report; if it does, add it to the relevant section and say so.
Do not run the full protocol.

### Full path

Use this for anything that could change the root cause, the fixes, the tests, or the notes.

1. **Reload.** Read the report in full. Read in full every codebase note it lists, and any note whose triggers match the new question. Do not rely on memory of an earlier run.
2. **Re-verify.** Before building on a claim in the report, confirm its anchors (symbols, files, tags, settings) still resolve in current source. If a claim fails, fix it in the report as part of this iteration.
3. **Answer.** Answer the question in chat, leading with the answer.
4. **Write back.** Rewrite the affected report sections in place, following the editorial note: one coherent analysis, no addenda, no "earlier pass" narration, and no linebreaks mid-sentence. If the answer changes nothing in the report, say so explicitly.
5. **Summarise.** End with a short list of the report sections changed and why.

Do not change any code other than the report unless the user asks for it explicitly in this iteration.

# Writing style

Use 80% of the way to ASD-STE100