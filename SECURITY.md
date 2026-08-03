# Security

## Supported versions

Agent World is an executable product slice. Only the current `main` branch receives
fixes; there are no maintained release branches yet.

## Reporting a vulnerability

Use GitHub's private reporting: **Security → Advisories → Report a vulnerability** on
[the repository](https://github.com/Viseriontarg/agent-world/security/advisories/new).
Please do not open a public issue for anything exploitable.

Include what you did, what happened, and — where possible — the `--self-check` output
or a minimal reproduction. Expect a first response within a week. Given the size of the
project, that is a best effort, not a service level agreement.

## What is in scope

Agent World runs locally, starts local processes, writes durable state, and can submit one
bounded Codex turn. The important boundaries are:

- **Live-provider authority.** The live path must resolve a native `codex.exe`, enforce the
  pinned CLI/version/feature contract immediately before the prompt, bind workspace writes to
  the canonical isolated worktree, keep sandboxed-command network disabled, correlate every
  on-request approval/input response to its exact durable interaction, reject unreviewed protocol
  events, and admit only one turn globally.
  Bypassing any of those checks is in scope. The informational provider probes are separate and
  may resolve a command shim; a successful probe must never substitute for live admission.
- **Process containment and runtime ownership.** Live children are attached to a Windows Job
  Object after spawn and have bounded deadlines, output, reap, worker-join, and active-process
  checks. Unproven cleanup must retain the exclusive `agent-world.lock` lease so another process
  cannot recover the same database. Orphaned descendants, unsafe lease release, or a second
  runtime owner mutating live state are in scope.
- **Git worktree handling.** Worktree paths are derived from durable project and thread data.
  Path traversal, symlink/junction redirection, aliasing the shared source, destructive Git
  operations, or starting a turn after branch/path/OID identity changed are in scope.
- **Durable state and provider correlation.** The SQLite database at
  `%LOCALAPPDATA%\AgentWorld` holds prompts, results, provider session IDs, and timeline
  messages. Cross-project disclosure or corruption, payload-conflict bypass, replaying a paid
  turn, accepting a terminal result before its observed provider ID is durable, and invalid
  lifecycle rows are in scope.
- **The measurement script.** `scripts/measure.ps1` deletes a temporary fixture directory. A
  path that escapes `%TEMP%\AgentWorldResource-*` is in scope.

## What is out of scope

- Vulnerabilities in an unmodified installed `codex` or `claude` CLI itself; report those to its
  publisher. Agent World's resolution, configuration, supervision, and trust decisions remain in
  scope.
- Resource exhaustion caused solely by pointing the app at an enormous repository.
- Findings that require an attacker who already has arbitrary code execution as your user.

## Data handling and trust boundary

Agent World stores prompts, results, timeline messages, receipts, provider correlation,
diagnostics, and worktree plans in a local SQLite file. A live Codex turn sends the prompt and
any model-selected context that the CLI can read to the user's configured Codex service.

The verified worktree is a working-directory and Git-state boundary. It is **not** a
worktree-only read boundary or a host-secret boundary: other files readable by the user may also
be readable. Sandboxed commands are configured without network access, but the Codex provider
itself necessarily uses the network for the model request. The native CLI inherits Agent World's
process environment so installed authentication can work; model shell commands are separately
configured for Codex's `core` environment inheritance. Do not put secrets inside a boundary that
Agent World does not claim.

Codex 0.146.0 may expose write tools. Agent World deliberately requests `workspace-write` with
the canonical isolated worktree as the sole writable root and surfaces exact file/command
approval requests as Allow once or Deny. Request-shape fixtures are not Windows enforcement:
effective write-root, network, escalation, read-scope, and positive tool-exposure behavior still
need authenticated evidence. Similarly, the Job Object is attached after process creation.
Zero-race/no-orphan containment remains an external evidence gate, not a published guarantee.

`--probe-providers` starts no model turn and Agent World supplies it no prompt content; its
`"paid_model_turns": 0` field describes that probe only. Probe success says nothing about the
confidentiality or containment of a future authenticated turn.
