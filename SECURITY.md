# Security

## Supported versions

Agent World is a Phase‑1 executable slice. Only the current `main` branch receives
fixes; there are no maintained release branches yet.

## Reporting a vulnerability

Use GitHub's private reporting: **Security → Advisories → Report a vulnerability** on
[the repository](https://github.com/Viseriontarg/agent-world/security/advisories/new).
Please do not open a public issue for anything exploitable.

Include what you did, what happened, and — where possible — the `--self-check` output
or a minimal reproduction. Expect a first response within a week. Given the size of the
project, that is a best effort, not a service level agreement.

## What is in scope

Agent World runs locally, spawns local processes, and writes to a local database, so
the interesting boundaries are:

- **Process spawning.** The provider probe locates `codex` and `claude` via `where.exe`
  and may execute a `.cmd` shim through `cmd.exe`. Anything that turns a `PATH` entry,
  a directory name, or a repository path into arbitrary command execution is in scope.
- **Git worktree handling.** Worktree paths are derived from project and thread data.
  Path traversal out of the runtime root, or a plan that causes destructive Git
  operations, is in scope.
- **Durable state.** The SQLite database at `%LOCALAPPDATA%\AgentWorld` holds prompts
  and timeline messages. Anything that lets one project read or corrupt another's
  aggregate, or that bypasses payload-conflict rejection, is in scope.
- **The measurement script.** `scripts/measure.ps1` deletes a temporary fixture
  directory. A path that escapes `%TEMP%\AgentWorldResource-*` is in scope.

## What is out of scope

- The absence of live model turns. Provider lifecycle is deliberately gated, not
  accidentally missing.
- Whatever the installed `codex` or `claude` CLI does on its own; report those upstream.
- Resource exhaustion caused by pointing the app at an enormous repository.
- Findings that require an attacker who already has code execution as your user.

## Data handling

Agent World stores prompts, timeline messages, receipts, and worktree plans in a local
SQLite file. It sends nothing anywhere. Provider probes start no model turn and
transmit no prompt content — `--probe-providers` reports `"paid_model_turns": 0`
because that is a property of the probe, not a promise about the future.
