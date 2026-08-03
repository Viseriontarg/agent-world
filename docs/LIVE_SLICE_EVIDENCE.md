# Live-slice evidence

This document is the claim boundary for the one-Codex-operator app-server slice. The evidence is
adversarial by design: deterministic code-path checks may pass while release readiness remains
false. Nothing here turns AccessKit, fake providers, or hosted CI into real Narrator, NVDA,
authenticated Codex, Windows policy-enforcement, Job Object, or desktop-resource evidence.

## Machine-readable check

```powershell
cargo build --release --locked
.\target\release\agent-world.exe --live-slice-self-check > live-slice-evidence.json
```

The command creates disposable repositories, worktrees, and SQLite databases under the system
temporary directory, runs no model turn, and removes the fixtures. Committed fixtures contain no
prompts from users, tokens, credentials, or private source. Dynamic evidence is produced in CI
and uploaded as `live-slice-evidence-windows`; it is not committed with machine paths or random
identifiers.

Top-level semantics are intentionally asymmetric:

| Field | Required value | Meaning |
|---|---:|---|
| `automated_checks_passed` | `true` | Deterministic migration, lifecycle, crash, bounds, and stress checks passed |
| `passed` | `false` | The complete issue gate is not passed while external evidence is absent |
| `release_ready` | `false` | Do not promote the slice on fixture evidence alone |
| `issue_acceptance_ready` | `false` | The manual/external acceptance items remain open |
| `external_blocker_count` | `6` | The six evidence categories listed below are still missing |

## Automated result matrix

| Gate | Deterministic result | Evidence |
|---|---|---|
| Schema and replay | Pass | v1→v2 migration, pre-v2 backup reopened at version 1 with integrity/foreign-key checks, current DB integrity, identical command replay emits zero second dispatch |
| Lifecycle scripts | 10 terminal scripts | Complete stream; allow; deny; multiline input; pre-correlation interrupt; streaming interrupt; duplicate/out-of-order; malformed/oversized/unknown/stderr; premature zero exit; bounded crash |
| Crash windows | 10 named outcomes | Nine active/ambiguous windows restart as `indeterminate`; terminal-commit-before-UI reopens `completed`; automatic provider restarts remain zero |
| Duplicate events | Pass | One injected duplicate produces one receipt and one timeline delta; conflicting/out-of-order transitions fail transactionally |
| Stream bounds | Pass | 20,000 one-byte provider events, 20,000 durable bytes, 313 durable chunks, 315 provider-event SQLite transactions, maximum provider batch depth 64 |
| Queue bounds | Pass | UI→core 8, core→UI 32, core→provider 8, provider→core 64; the next `try_send` reports `Full` without a blocking send |
| Queue-dispatch uncertainty | Pass | Undispatched `Start` may fail; failed dispatch of approval/input/interrupt after provider activity becomes `indeterminate` |
| Resource-shaped fixture | Pass within deterministic scope | 50 operators and 20,000 persisted background messages remain while one logical live turn streams 20,000 events |
| Idle process model | Structural only | The deterministic fixture starts zero OS children; one logical global live slot is enforced |
| Keyboard and accessibility | Automated scope only | 900×560 AccessKit tree, 125/150/200% logical-point geometry, keyboard start/allow/deny/multiline/interrupt, F6 attention, 50-operator focus order |

The exact duration, database size, installed-Codex observation, and coalescing ratio are emitted by
each run. Timing from this fixture is diagnostic only and is not the desktop startup/CPU/memory
budget.

## Crash-window outcomes

The only accepted restart outcomes are conservative. Agent World does not silently start a
second provider turn.

| Injected boundary | Restart outcome |
|---|---|
| Command accepted, before adapter dispatch | `indeterminate` |
| Provider thread started, before session/cursor commit | `indeterminate` |
| Turn started, before start acknowledgement commit | `indeterminate` |
| Stream chunk received, before durable commit | `indeterminate` |
| Stream chunk committed, before UI observes it | `indeterminate`; committed output preserved |
| Approval received, before durable interaction commit | `indeterminate` |
| Response accepted, before provider receives it | `indeterminate` |
| Interrupt accepted, before provider receives it | `indeterminate` |
| Provider terminal received, before terminal commit | `indeterminate` |
| Terminal commit, before UI refresh | `completed` |

## Six external blockers

These are not waived by a green CI run:

1. Real Windows task-flow and visual evidence at 125%, 150%, and 200% scaling.
2. A complete Narrator path on the actual Windows executable.
3. A complete NVDA path on the actual Windows executable.
4. Real Windows Job Object ownership, descendant cleanup, and zero-leak observations across
   success, overflow, timeout, interrupt, crash, and forced close.
5. An opt-in authenticated Codex run proving installed-provider equivalence, effective canonical
   write-root/network/escalation behavior, redacted protocol records, Git manifests, and reopened
   SQLite state.
6. Current list-first desktop measurements for startup, private memory, idle CPU, active-stream
   CPU/memory, and process-tree totals. Historical Phase-1 numbers are not reused.

Fork remains explicitly unimplemented and unproven. Claude live turns remain outside this slice.

## Reproduce the automated gates

```powershell
cargo fmt --all -- --check
cargo test --release --locked -- --skip installed_provider_surfaces_are_probeable_without_a_turn
cargo clippy --all-targets --locked -- -D warnings
cargo build --release --locked
.\target\release\agent-world.exe --self-check
.\target\release\agent-world.exe --live-slice-self-check
```

The provider-surface test omitted above depends on exact locally installed CLIs and remains a
zero-model-turn probe. The paid smoke stays opt-in and is never enabled by CI.
