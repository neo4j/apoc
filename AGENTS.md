# Awesome Procedures for Neo4j (APOC)

## Commands

Use the [Taskfile](https://taskfile.dev) tasks where they exist (`task set-version`, `task publish`) so agents, humans, and CI stay in sync; for everyday builds and tests invoke `./gradlew` directly, since there are no tasks for those.

OUTPUT: builds and test runs are long and noisy — pass `-q` to trim output to `[ERROR]` lines plus the build/test summary. Prefer that over redirecting to a log file and grepping it: a redirect or pipe makes the command a compound that needs separate approval for each part.

### Commit Messages
Prefix the subject with the Linear ticket (e.g. `SURF-123: Fix …`). In the body, explain *why* and *what* — the code already explains how.

### Branching & Pushing
- The upstream is `git@github.com:neo4j/apoc.git`, main branch is `dev`.
- Every PR must include a label for the branch it targets (e.g. `dev`, `5.26`).

### Code comments
Prefer no comment. Add one only when the *why* is non-obvious. Never reference Linear tickets from in-line comments — that belongs in the PR description and commit message.