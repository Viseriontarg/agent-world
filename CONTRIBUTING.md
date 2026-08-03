# Contributing

Agent World has one editorial rule, and everything else follows from it:

> **The repository never claims more than the build can demonstrate.**

That is why the README separates implemented behavior from external proof gates. The UI now has
one narrowly bounded Codex app-server lifecycle. Streaming, approvals, multiline input,
interruption, completed-session resume, and crash reconciliation have deterministic fixtures;
authenticated Windows equivalence, Narrator/NVDA, current resource numbers, Claude, fork,
positive tool allowlisting, and containment remain gated.
Contributions are welcome — including the ones that prove a claim wrong.

## Ways to help that are genuinely useful

| Contribution | Why it matters |
|---|---|
| **Reproduce the measurements** and report different numbers | The resource table is a central claim. Contradicting it is high-value evidence. |
| **Correct a provider assumption** | The Codex and Claude surfaces move. Version, feature, event, sandbox, read-scope, and tool-exposure corrections should fail closed and update the claim text. |
| **Break termination or recovery** | Find an orphan process, blocked pipe, missed session ID, premature runtime-lease release, turn replay, or a Git crash window the fixtures missed. |
| **Break admission** | Redirect or mutate a planned worktree, detach its branch, alter its OID, race the global slot, or archive active work without being rejected. |
| **Argue about scope** | Moving a capability into or out of this product slice with concrete reasoning is useful work. |
| **Sharp technical criticism** | Preferred over polite agreement. |

## Setting up

```powershell
git clone https://github.com/Viseriontarg/agent-world.git
cd agent-world
cargo build --release --locked
```

You need Windows 11, Rust 1.95+ MSVC (`rust-version = 1.95`), Visual C++ Build Tools
with a Windows SDK, and Git. The bundled SQLite is compiled from source, so `cl.exe`
must be on `PATH` — build from a **Developer PowerShell for VS**, or run
`vcvars64.bat` first, or `cargo build` will fail in `cc-rs` before it reaches your code.

Running the live slice additionally requires a native `codex.exe` reporting exactly
`codex-cli 0.146.0` and an already authenticated Codex installation. Do not spend a model turn
merely to test installation; the zero-turn probe and deterministic fake-runner fixtures exist for
that purpose.

## Before you open a pull request

```powershell
cargo fmt --all -- --check
cargo test --locked -- --skip installed_provider_surfaces_are_probeable_without_a_turn
cargo clippy --all-targets --locked -- -D warnings
cargo build --release --locked
./target/release/agent-world.exe --self-check
./target/release/agent-world.exe --live-slice-self-check
```

If your change touches the UI, core loop, renderer, provider runtime, or any dependency, also run:

```powershell
./scripts/measure.ps1 -WarmupSeconds 300 -SampleSeconds 60
```

and paste the JSON into the pull request. CI runs everything except `measure.ps1`, which needs a
real desktop session.

The unfiltered test suite includes one environment-specific zero-turn assertion against locally
installed `codex` and `claude` CLIs. Run it when your machine has the reviewed installations; CI
and deterministic local verification skip it deliberately. Never replace it with an
authenticated prompt as a convenience check.

Provider-runtime changes need deterministic fixtures proportional to the boundary they touch:
session-ID ordering and idempotency, JSONL/event allowlisting, output caps, absolute deadline,
blocked stdin/readers, process and worker cleanup, lease retention, terminal diagnostic bounds,
global admission, restart reconciliation, and worktree identity. A fixture can validate code; it
cannot by itself promote the real-Windows enforcement claim.

## The budgets are not negotiable by accident

| Budget | Limit |
|---|---:|
| Startup to interactive window | ≤ 3 s |
| Peak private memory | ≤ 250 MB |
| Average idle CPU | ≤ 0.5% |
| Node processes at idle | 0 |
| Process per idle actor | No |

A DX12/wgpu shell was already built, measured at 366.45 MB, and deleted. That is the
standard. A pull request may still argue that a budget should change — but it has to
argue, with numbers, rather than quietly spend the headroom.

## Design constraints worth knowing before you write code

- **One renderer.** `glow`. There is no second backend to keep working, and adding one is a scope
  change, not a patch.
- **One SQLite writer.** All persistence happens on a single orchestration thread. UI-to-core,
  core-to-UI, core-to-provider, and provider-to-core channels have capacities 8, 32, 8, and 64.
  They are bounded on purpose, so backpressure is visible rather than absorbed by an unbounded
  queue. The one-live-turn rule is a durable admission invariant, not a channel-size claim.
- **The interface is a projection.** SQLite, command receipts, provider correlation, and Git are
  the source of truth. Nothing may be true only in the UI.
- **Intent is durable before it is real.** Worktree plans and turn intent are committed before
  Git or the provider runs. Replayed receipts must never launch another paid turn.
- **Uncertainty is a state, not a cleanup task.** When Git or provider termination cannot be
  proven, Agent World preserves `indeterminate` state or retains the runtime lease. It does not
  reset, prune, delete, replay, or force its way out of ambiguity.
- **Authority is versioned and fail-closed.** A new CLI version, enabled feature, event type, tool
  surface, schema shape, or admissible state needs review and evidence before acceptance.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for how these fit together.

## Evidence for claim promotion

If a pull request moves something out of the README's “Deliberately not claimed yet” column, put
the evidence in that same pull request. Authenticated Windows promotion needs the build and CLI
version, native executable resolution, effective feature inventory and launch contract, actual
sandbox mode, redacted JSONL, pre/post Git manifests, read-scope attempts, process-tree counts for
success and failure paths, and reopened SQLite state. Do not publish a claim solely because Linux
CI or a fake runner is green.

## Commits and pull requests

- Small, single-purpose commits with an imperative subject line.
- Draft PRs are welcome early; an unfinished idea with a measurement beats a finished idea
  without one.
- Keep unrelated generated/site files out of native-product patches.

## Licence

Contributions are accepted under the [MIT Licence](LICENSE).
