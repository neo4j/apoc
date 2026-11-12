# Agent Skills & Subagents

This directory contains reusable Claude Code **skills** (in `skills/`) and **subagents** (in `subagents/`) for the neo4j repository.

- A **skill** is a workflow loaded into the main conversation when invoked. It lives at `skills/<name>/SKILL.md` and is activated by symlinking into `.claude/skills/<name>`.
- A **subagent** is a separate agent that the main session can delegate work to via the `Agent` tool. Each invocation runs in its own context window. It lives at `subagents/<name>.md` and is activated by symlinking into `.claude/agents/<name>.md`.

## Why symlinks?

Claude Code loads skills from `.claude/skills/` and subagents from `.claude/agents/`. If everything lived there directly, all developers would have everything active at once, causing workflows to collide.

Instead, definitions live here in `agents/` (versioned in git), and developers activate only the ones they need by symlinking them into the appropriate `.claude/` directory.

## Using the manage-skills skill

The `manage-skills` skill is a bootstrap skill that ships in `.claude/skills/` and is always available. It handles loading and unloading of both skills and subagents — it auto-detects which one you mean by looking up the name in both source directories.

### Load

```
/manage-skills load debug-flaky-test       # loads a skill
/manage-skills load todo-issue-solver      # loads a subagent
```

### Unload

```
/manage-skills unload debug-flaky-test
```

### List available and active

```
/manage-skills list
```

## Doing it manually

You can also manage symlinks yourself from the repository root.

### Load a skill

```bash
ln -s ../../agents/skills/debug-flaky-test .claude/skills/debug-flaky-test
```

### Load a subagent

```bash
ln -s ../../agents/subagents/todo-issue-solver.md .claude/agents/todo-issue-solver.md
```

### Unload

```bash
rm .claude/skills/<name>           # for a skill
rm .claude/agents/<name>.md        # for a subagent
```

### List

```bash
# Available
ls agents/skills/                 # skills
ls agents/subagents/              # subagents

# Active
ls -l .claude/skills/             # skills
ls -l .claude/agents/             # subagents
```

## Adding a new skill or subagent

1. Create the definition:
   - Skill: `agents/skills/<name>/SKILL.md`
   - Subagent: `agents/subagents/<name>.md`
2. Commit and push — it's now available to the team.
3. Each developer activates it locally via `/manage-skills load <name>` or a manual symlink.

Names are expected to be globally unique across both `skills/` and `subagents/` so `manage-skills` can auto-detect which kind you mean.

Symlinks in `.claude/skills/` and `.claude/agents/` are git-ignored, so activated items stay local and never conflict.
