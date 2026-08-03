<p align="center">
  <img src="docs/assets/agent-world-hero.png" alt="Agent World command room with teal and orange operators at independent workstations" width="100%">
</p>

<h1 align="center">Agent World</h1>

<p align="center">
  <strong>A native command room for AI coding harnesses.</strong><br>
  Keep coding-agent work visible as operators; the implemented live path is one bounded Codex app-server lifecycle.
</p>

<p align="center">
  <a href="https://github.com/Viseriontarg/agent-world/actions/workflows/ci.yml"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/Viseriontarg/agent-world/ci.yml?branch=main&style=for-the-badge&label=ci&labelColor=13202b"></a>
  <img alt="Working native slice" src="https://img.shields.io/badge/status-working_native_slice-36c5a5?style=for-the-badge&labelColor=13202b">
  <img alt="Windows" src="https://img.shields.io/badge/platform-Windows-0078D4?style=for-the-badge&logo=windows11&logoColor=white&labelColor=13202b">
  <img alt="Rust 1.95+" src="https://img.shields.io/badge/Rust-1.95%2B-000000?style=for-the-badge&logo=rust&logoColor=white&labelColor=13202b">
  <a href="LICENSE"><img alt="MIT licence" src="https://img.shields.io/badge/licence-MIT-8b5cf6?style=for-the-badge&labelColor=13202b"></a>
</p>

<p align="center">
  <a href="#the-idea">Idea</a> ·
  <a href="#what-exists-today">What exists</a> ·
  <a href="#lean-by-measurement">Measurements</a> ·
  <a href="#quick-start">Quick start</a> ·
  <a href="docs/ARCHITECTURE.md">Architecture</a> ·
  <a href="#roadmap">Roadmap</a>
</p>

---

## The idea

Most multi-agent tools make concurrent work feel like managing browser tabs. Agent World makes it spatial:

- a **project** is a room;
- a **Git worktree** is a workstation;
- a **thread** is an operator;
- Codex and Claude are provider capabilities, not the character's identity.

The interface is only a projection. SQLite, command receipts, provider cursors, and Git remain the source of truth.
The shipped interface is list-first: all operators remain reachable in a standard, scrollable,
keyboard-focusable control while the room/workstation metaphor stays in the domain model.

## What exists today

Agent World is a working native executable slice—not a mock-up and not a completed roadmap. The durable core, list-first control surface, and one deliberately bounded Codex app-server lifecycle are implemented. Deterministic fixtures prove the in-repository state machine; authenticated Windows enforcement, assistive-technology use, current resource measurements, and production distribution remain open proof gates.

| ✅ Implemented in this repository | ⏳ Not yet proven |
|---|---|
| Native `eframe/egui/glow` application with a scrollable, focusable operator list and AccessKit enabled | Real-Windows validation at 125%, 150%, and 200% scaling with keyboard-only use, NVDA, and Narrator |
| Standard `Tab`/`Shift+Tab` traversal, F6 attention cycling, and documented shortcut behavior covered by tests | Published keyboard-only and screen-reader task-flow results |
| One SQLite writer, bounded queues, durable events, receipts, and payload-conflict rejection | Startup, peak private memory, idle CPU, and process-tree rerun for the current list-first executable |
| Native Git worktree creation with crash reconciliation and conservative conflict handling | In-app diff inspection, request-changes, merge, and final-commit recording |
| One globally admitted Codex app-server turn with coalesced streaming, exact approval/input correlation, interrupt, completed-session resume, conservative restart handling, and deterministic lifecycle/crash evidence | Authenticated Windows equivalence and policy enforcement, Job Object leak proof, current resource numbers, Claude live turns, and fork |
| Source-build instructions and Windows CI configuration | Signed installer, update/rollback, uninstall, and release-integrity proof |

A provider surface probe is not a model turn, AccessKit being enabled is not a screen-reader validation result, and the historical resource baseline does not describe the current list-first build.

## Lean by measurement

> **Measurement status:** the figures below are the published baseline for the original
> Phase‑1 spatial interface. The current list-first interface has not yet completed its
> required Windows measurement rerun, so these are historical comparison numbers—not a
> claim about the current executable.

Baseline fixture: 5 projects, 50 visible actors, 20,000 persisted messages, no live provider or terminal.

| Gate | Result | Limit | |
|---|---:|---:|:--|
| Startup to interactive window | **1.023 s** | ≤ 3 s | ✅ |
| Peak private memory | **75.87 MB** | ≤ 250 MB | ✅ |
| Average idle CPU | **0.026%** | ≤ 0.5% | ✅ |
| Node processes at idle | **0** | 0 | ✅ |
| Process per idle actor | **No** | No | ✅ |

The first DX12/wgpu shell measured 366.45 MB and was rejected. The production renderer is `glow`; there is no second renderer to maintain.

> `scripts/measure.ps1` measures the current executable on your machine. Its next published
> Windows result will replace this baseline; [measurement challenges remain welcome](.github/ISSUE_TEMPLATE/measurement_challenge.yml).

## How it works

```text
agent-world.exe
├─ eframe / egui / glow / AccessKit
│  ├─ scrollable operator list
│  └─ selected workspace: timeline · prompt · provider readiness
├─ bounded UI → core command queue
├─ single orchestration + SQLite writer thread
│  ├─ durable command receipts and events
│  ├─ bounded timeline projections
│  └─ native Git worktree reconciliation
├─ bounded core → UI event queue
├─ one-slot provider supervisor
│  └─ native Codex `app-server --stdio` · reverified worktree · bounded protocol
└─ lazy zero-turn provider probes
   ├─ Codex app-server
   └─ Claude CLI
```

Worktree intent is committed before Git runs. The immutable plan records the repository, common directory, path, branch, and full commit OID. Restart recovery proves both dangerous windows:

1. crash after durable acceptance but before Git;
2. crash after Git but before the terminal SQLite transaction.

Mismatched Git state becomes `indeterminate`; Agent World does not reset, prune, delete, or force its way through uncertainty.

The live runner resolves a native `codex.exe`, requires exactly `codex-cli 0.146.0`, and fails
closed on an unreviewed enabled feature. A fresh turn is durable before provider dispatch. The
app-server requests `workspace-write` plus `on-request` approval at thread start/resume and sends
an explicit turn sandbox policy whose sole writable root is the canonical isolated worktree,
with sandboxed-command network disabled and both temporary writable-root exclusions enabled. Approval and input
requests are durably correlated before the UI can answer them; stream chunks are bounded and
coalesced; interrupt targets the exact durable turn. An unfinished turn found after restart is
`indeterminate` and is never replayed automatically. A completed session/cursor may be used for
a later, explicitly admitted turn. Fork remains unimplemented and unproven.

That request contract is not a claim that this Linux/CI run proved Windows enforcement. The
worktree is the intended write root and Git-state boundary, not a host-secret or worktree-only
read boundary. Codex may read other user-readable files, and the prompt plus model-selected
context go to the configured Codex service. The provider itself uses the network even though
sandboxed commands request `networkAccess: false`. Authenticated Windows policy enforcement,
read-scope observation, and zero-race/no-orphan Job Object proof remain release gates. One
**live-turn** tree is admitted; zero-turn readiness/probe children are separate.

Promotion requires an external Windows evidence bundle: Windows build and CLI version, resolved
native executable, observed sandbox mode, exact launch and effective-feature inventory, redacted
raw JSONL, pre/post worktree and Git-common-directory manifests, read-scope attempts, process-tree
counts after success/timeout/overflow/forced close, and reopened SQLite turn/session/message state.
Unit fixtures and CI do not satisfy that gate.

**→ [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)** covers the thread model, the durable schema, the worktree recovery sequence, and exactly what each provider probe does and does not verify.

## Quick start

### Prerequisites

- Windows 11
- Rust 1.95+ MSVC (`rust-version = 1.95`)
- Visual C++ Build Tools and a Windows SDK
- Git
- Optional informational probes: installed `codex` and `claude` CLIs
- To run the bounded live slice: an authenticated native `codex.exe` at exactly `codex-cli 0.146.0`; the live runner rechecks the exact version and enabled-feature inventory immediately before app-server launch. The broader generated-schema contract is checked separately by `--probe-providers`, while unexpected runtime methods and shapes fail closed.

```powershell
git clone https://github.com/Viseriontarg/agent-world.git
cd agent-world
cargo build --release
.\target\release\agent-world.exe
```

Runtime data defaults to `%LOCALAPPDATA%\AgentWorld`.

> **Note**
> The bundled SQLite compiles from source, so `cl.exe` must be on `PATH`. Build from a
> **Developer PowerShell for VS**, or run `vcvars64.bat` first.

### Controls

| Input | Action |
|---|---|
| Click or `1`–`9` | Select an operator (`1`–`9` only when no control has focus) |
| `Enter` | Focus the prompt when no control has focus |
| `Ctrl+Enter` | Start a live Codex turn when the selected operator is durably eligible |
| `Ctrl+.` | Interrupt the selected turn only when its durable projection is interruptible |
| `F6` | Cycle to and focus each outstanding approval, input request, or other operator attention state |
| `Tab` / `Shift+Tab` | Move keyboard focus through every control and operator |

## Prove it locally

```powershell
.\target\release\agent-world.exe --self-check
.\target\release\agent-world.exe --live-slice-self-check
.\target\release\agent-world.exe --probe-providers
.\scripts\measure.ps1 -WarmupSeconds 300 -SampleSeconds 60
```

`--live-slice-self-check` emits machine-readable results for ten lifecycle scripts, ten named crash windows, queue saturation, duplicate suppression, coalescing/transaction counts, and a 50-operator/20,000-message stress fixture. Its `automated_checks_passed` field may be true while `release_ready` and `passed` remain false because Windows/manual gates are still absent. It starts no paid model turn. The resource script uses a unique temporary runtime root and removes it afterward. See [`docs/LIVE_SLICE_EVIDENCE.md`](docs/LIVE_SLICE_EVIDENCE.md).

## Roadmap

- [x] Native list-first shell with bounded queues and durable SQLite state
- [x] Conservative Git worktree creation and crash reconciliation
- [x] Zero-turn Codex and Claude protocol-surface probes
- [x] One globally admitted Codex app-server lifecycle with deterministic streaming, approval/input, interrupt, completed-session resume, exact feature/request-shape checks, and conservative restart handling
- [x] Historical Phase‑1 resource baseline for the original spatial interface
- [ ] Publish the current list-first Windows resource rerun: startup, memory, idle CPU, and process-tree totals
- [ ] Complete Windows validation at 125%, 150%, and 200% scaling with keyboard-only use, NVDA, and Narrator
- [ ] Ship the review loop: inspect diff, request changes, merge, and record the final commit
- [ ] Publish the authenticated Windows Codex evidence bundle: enforcement mode, read scope, JSONL, Git/SQLite state, and process cleanup
- [ ] Publish authenticated Windows equivalence for the Codex lifecycle; implement and prove Claude and fork
- [ ] Ship signed Windows distribution: install, update/rollback, uninstall, and release-integrity verification
- [ ] Complete orchestration leases, directed handoffs, and verified T3 import
- [ ] Terminals, attachments, richer Markdown, and repository surfaces

## Why public this early?

Because architecture is easier to trust when the uncomfortable measurements and unfinished boundaries are visible. Issues, profiling evidence, protocol corrections, and sharp technical criticism are welcome.

The most useful thing you can do is run `scripts/measure.ps1` and tell me the numbers are wrong.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) first — it explains the budgets, the one editorial
rule, and what makes a pull request easy to accept. Security reports go through
[SECURITY.md](SECURITY.md).

## Licence

[MIT](LICENSE) © 2026 Aminreza Khoshbahar
