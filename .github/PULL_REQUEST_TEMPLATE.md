<!--
Agent World's rule is simple: the repository never claims more than it can demonstrate.
Keep the diff and the claims the same size.
-->

## What this changes

<!-- One paragraph. What was true before, what is true after. -->

## Evidence

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --all-targets -- -D warnings`
- [ ] `cargo build --release`
- [ ] `.\target\release\agent-world.exe --self-check`
- [ ] `.\scripts\measure.ps1` — attach the JSON if this PR touches the UI, the core loop, the renderer, or dependencies

<details>
<summary>Measurement output</summary>

```json

```

</details>

## Budgets

Does this change move any of these? Say so plainly, including "no".

| Budget | Limit | After this PR |
|---|---:|---:|
| Startup to interactive window | ≤ 3 s | |
| Peak private memory | ≤ 250 MB | |
| Average idle CPU | ≤ 0.5% | |
| Node processes at idle | 0 | |
| Process per idle actor | No | |

## Claims

- [ ] The README's "Proven now" column still only lists things this build actually does.
- [ ] Nothing was moved out of "Deliberately not claimed yet" without evidence in this PR.
