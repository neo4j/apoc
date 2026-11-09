---
name: manage-skills
description: Load or unload agent skills and subagents by creating or removing symlinks from agents/skills/ or agents/subagents/ into the corresponding .claude/ directory. Use when the user wants to activate, deactivate, or list available skills or subagents that exist in this repository.
argument-hint: "load|unload|list [name]"
disable-model-invocation: false
allowed-tools:
   - Bash(ln *)
   - Bash(rm *)
   - Bash(ls *)
   - Bash(readlink *)
   - Bash(test *)
   - Bash([ *)
---

# Manage Skills

Load and unload **skills** and **subagents** by symlinking them into `.claude/`.

A *skill* lives in `agents/skills/<name>/SKILL.md` and is symlinked into `.claude/skills/<name>`.
A *subagent* lives in `agents/subagents/<name>.md` (single file) and is symlinked into `.claude/agents/<name>.md`.

The user does not need to specify which kind they want — this skill auto-detects by looking up the name in both source directories.

## Commands

The `$ARGUMENTS` will be one of:

- **`load <name>`** — Activate a skill or subagent
- **`unload <name>`** — Deactivate a skill or subagent
- **`list`** — Show available and active skills and subagents

## How to execute

### Resolve paths

From the repository root:

| Kind     | Source                          | Target (active)                  | Relative symlink target              |
|----------|---------------------------------|----------------------------------|--------------------------------------|
| skill    | `agents/skills/<name>/`         | `.claude/skills/<name>`          | `../../agents/skills/<name>`         |
| subagent | `agents/subagents/<name>.md`    | `.claude/agents/<name>.md`       | `../../agents/subagents/<name>.md`   |

### Auto-detect the kind

For `load <name>` and `unload <name>`, determine the kind:

1. If `agents/skills/<name>/` exists → it's a **skill**.
2. Else if `agents/subagents/<name>.md` exists → it's a **subagent**.
3. Else → report that no skill or subagent with that name exists, and run `list` to show what's available.

If both exist with the same name, that's a naming collision and you should refuse to proceed; tell the user to rename one. (Names are expected to be globally unique across both directories.)

### `load <name>`

1. Auto-detect the kind as above.
2. Check if the active path already exists. If so, report that it's already loaded.
3. Ensure the parent directory exists (`.claude/skills/` or `.claude/agents/`). Create it if needed (`mkdir -p`).
4. Create the symlink with the appropriate relative target.
5. Verify with `readlink` on the active path.
6. Report success, including whether a skill or subagent was loaded.

### `unload <name>`

1. Auto-detect the kind as above (it's fine to fall back to checking which active path exists if the source has been deleted).
2. Check if the active path exists and is a symlink. If not, report that it's not loaded.
3. Remove the symlink:
   ```bash
   rm <active-path>
   ```
4. Report success.

### `list`

1. List directories in `agents/skills/` — these are **available skills**.
2. List files in `agents/subagents/` — these are **available subagents**.
3. List symlinks in `.claude/skills/` — these are **active skills**.
4. List symlinks in `.claude/agents/` (if the directory exists) — these are **active subagents**.
5. Display a table grouped by kind (Skills / Subagents) showing each item and whether it is active.
