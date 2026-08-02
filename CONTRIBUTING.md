# Contributing

Agent World has one editorial rule, and everything else follows from it:

> **The repository never claims more than the build can demonstrate.**

That is why the README has a "Deliberately not claimed yet" column, why live model
turns are disabled in the UI, and why the resource numbers come from a script you
can run yourself. Contributions are welcome — including the ones that prove a claim
wrong.

## Ways to help that are genuinely useful

| Contribution | Why it matters |
|---|---|
| **Reproduce the measurements** and report different numbers | The resource table is the project's central claim. Contradicting it is the highest-value issue you can file. |
| **Correct a protocol assumption** | The Codex app-server and Claude CLI surfaces move. If a probe asserts something the installed CLI no longer declares, say so. |
| **Break the crash recovery** | `--self-check` covers two windows. If you find a third, that is a real bug. |
| **Argue about scope** | Saying "this belongs in Phase 3, not Phase 2" with reasoning is a contribution. |
| **Sharp technical criticism** | Preferred over polite agreement. |

## Setting up

```powershell
git clone https://github.com/Viseriontarg/agent-world.git
cd agent-world
cargo build --release
```

You need Windows 11, Rust 1.95+ MSVC (`rust-version = 1.95`), Visual C++ Build Tools
with a Windows SDK, and Git. The bundled SQLite is compiled from source, so `cl.exe`
must be on `PATH` — build from a **Developer PowerShell for VS**, or run
`vcvars64.bat` first, or `cargo build` will fail in `cc-rs` before it reaches your code.

## Before you open a pull request

```powershell
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo build --release
.\target\release\agent-world.exe --self-check
```

If your change touches the UI, the core loop, the renderer, or any dependency, also run:

```powershell
.\scripts\measure.ps1 -WarmupSeconds 300 -SampleSeconds 60
```

and paste the JSON into the pull request. CI runs everything except `measure.ps1`,
which needs a real desktop session.

> **Note**
> `cargo test` includes one test that asserts against locally installed `codex` and
> `claude` CLIs. It fails on machines without them, and CI skips it deliberately.

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

- **One renderer.** `glow`. There is no second backend to keep working, and adding one
  is a scope change, not a patch.
- **One SQLite writer.** All persistence happens on a single orchestration thread.
  Channels between the UI and the core are `sync_channel(8)` and `sync_channel(32)`
  — bounded on purpose, so backpressure is visible rather than absorbed by an
  unbounded queue.
- **The interface is a projection.** SQLite, command receipts, provider cursors, and Git
  are the source of truth. Nothing may be true only in the UI.
- **Intent is durable before it is real.** Worktree plans are committed before Git
  runs, which is what makes both crash windows recoverable.
- **Uncertainty is a state, not a cleanup task.** When Git state does not match the
  plan, the worktree becomes `indeterminate`. Agent World does not reset, prune,
  delete, or force its way out of ambiguity, and neither should your patch.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for how these fit together.

## Commits and pull requests

- Small, single-purpose commits with an imperative subject line.
- If a pull request moves something out of the README's "Deliberately not claimed yet"
  column, the evidence has to be in that same pull request.
- Draft PRs are welcome early; an unfinished idea with a measurement beats a finished
  idea without one.

## Licence

Contributions are accepted under the [MIT Licence](LICENSE).
