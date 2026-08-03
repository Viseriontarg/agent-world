# Slice 2 epic acceptance audit: one trustworthy live Codex operator

This document is the close gate for GitHub issue #6. It maps the epic acceptance criteria to
evidence in the repository and, just as importantly, records which criteria still require an
external Windows proof run. Passing deterministic fixtures is necessary but does not prove that an
installed, authenticated Codex build or a Windows assistive-technology path works.

Last audited: 2026-08-03 against the shared Slice 2 working tree.

## Decision

**Not ready to close.** The provider-neutral contract, durable projections, operator controls, and
production-shaped Codex app-server runner have focused automated coverage. The epic still lacks the
external evidence required for a real installed turn, current host/provider resource accounting,
the complete Windows scaling/keyboard path, Narrator, NVDA, and the uncut demonstration run.

Status terms used below:

- **automated pass** — deterministic repository evidence currently covers the stated behavior;
- **pending falsification** — the focused behavior exists, but the full #5 matrix or its
  machine-readable evidence is not attached yet;
- **external evidence required** — repository fixtures cannot satisfy this criterion;
- **blocked** — the epic cannot close while any part of the criterion remains unproved.

The machine-readable lifecycle/crash/resource result owned by #5 is the authoritative detailed QA
artifact once available. Its human-readable companion is
[`LIVE_SLICE_EVIDENCE.md`](LIVE_SLICE_EVIDENCE.md). This epic audit summarizes those results; it
must not promote an `external_evidence_required` result to passed.

## Functional acceptance

| Requirement | Current status | Evidence and remaining proof |
|---|---|---|
| One supported installed Codex build completes a real turn in an isolated worktree | **blocked — external evidence required** | `core::tests::turn_admission_revalidates_durable_git_identity`, `turn_admission_rejects_worktree_path_redirected_to_shared_source`, and the app-server pipe harness prove admission and protocol behavior without spending a model turn. They do not prove a real authenticated turn. Run the explicit paid opt-in Windows smoke path with native `codex.exe`, record the exact version and worktree/Git manifests, and reopen SQLite to prove the result. |
| Assistant output streams into the durable timeline in bounded, deduplicated chunks | **automated pass; installed-provider proof required** | Adapter/core/UI tests cover ordering, coalescing, deduplication, and bounded rendering. `--live-slice-self-check` drives 20,000 provider events through a 64-event provider queue, stores 20,000 bytes as 313 durable chunks in 315 SQLite transactions, reports one duplicate suppression, and reaches one completed terminal state. A real installed turn is still part of the external demonstration gate. |
| Approval and provider user-input requests can be answered in Agent World | **automated pass** | Exact correlation and response handling are covered by `codex_app_server::tests::approval_round_trip_uses_only_the_exact_pending_rpc_id`, `user_input_round_trip_requires_every_exact_question_once`, the matching core fake tests, and `ui::tests::keyboard_only_flow_starts_answers_requests_and_interrupts`. A deterministic interaction is allowed in the demonstration when the real task does not request one. |
| Interrupt during generation reaches a visible terminal outcome | **automated pass; external demonstration required** | Adapter tests cover acknowledgement and bounded forced stop; core and UI tests cover durable interrupt intent and keyboard dispatch. The uncut Windows proof run must show a second real turn interrupted during output and the resulting terminal projection. |
| Restart at every tested crash boundary yields resume/reconcile, completed, failed, or `indeterminate`, never an implicit duplicate start | **automated pass; external demonstration required** | `--live-slice-self-check` records all ten named crash windows: nine conservatively reopen as `indeterminate`, terminal-commit-before-UI-refresh reopens as `completed`, and automatic duplicate starts remain zero. Focused core and adapter tests cover durable cursor use and receipt replay. Closure still requires reopened-SQLite evidence in the uncut installed-provider demonstration. |

## Trust acceptance

| Requirement | Current status | Evidence and remaining proof |
|---|---|---|
| One normalized authority policy matches the durable record, UI, and pinned Codex request | **automated pass for request shape; external enforcement required** | Schema v2 records `isolated_workspace_write_on_request_v1`. The 0.146.0 adapter requests `sandbox: "workspace-write"` plus `approvalPolicy: "on-request"` for thread start/resume, and pins TurnStart to `workspaceWrite` with the canonical verified worktree as its sole writable root, sandbox network disabled, both temporary writable-root exclusion flags enabled, and provider environments empty. Launch argv separately clears MCP servers and disables web search. `codex_app_server::tests::outgoing_requests_match_the_pinned_workspace_write_on_request_schema` and the launch-argument test lock the reviewed shapes. A real Windows run must still prove the effective sandbox and post-turn Git manifest; configuration is not enforcement evidence and this is not a host-secret or worktree-only read boundary. |
| Provider side effects occur only after durable acceptance | **automated pass** | `Store::execute_with_provider_command` derives a provider command only after a successful committed receipt and suppresses dispatch on receipt replay. `codex_app_server::tests::no_provider_request_exists_before_a_core_start_command` and `core::tests::isolated_workspace_write_codex_turn_is_admitted_once_and_records_terminal_answer` cover the boundary. |
| UI lifecycle state comes only from durable projections | **automated pass** | `LiveTurnSnapshot`, `InteractionSnapshot`, and committed timeline rows feed the UI. `ui::tests::interrupt_uses_only_the_durable_turn_id_and_interruptible_flag`, `live_turn_gate_exposes_each_normalized_durable_reason`, and receipt/draft tests cover the command/display boundary. |
| Ambiguity is visible and non-destructive | **automated pass; demonstration required** | Process loss after possible side effects becomes `indeterminate`; startup reconciliation never automatically replays it. Core restart/process-loss tests and the UI recovery/accessibility labels cover deterministic behavior. The demonstration must show the recovered explanation after a documented crash boundary. |
| No source, prompt, token, credential, or environment secret leaks into diagnostics or fixtures | **pending falsification; external artifact review required** | Protocol payloads and diagnostics have explicit byte limits and sanitization. `codex_app_server::tests::secret_user_input_and_sensitive_approval_text_are_never_normalized_for_persistence`, `unexpected_exit_keeps_bounded_sanitized_stderr_and_never_completes`, and the diagnostic-ring test use fake sentinel values, not real credentials. Synthetic sentinels cannot prove that arbitrary provider stderr is secret-free. Before attaching a real run, review/redact JSONL, diagnostics, manifests, screenshots, and SQLite exports. |
| README and marketing claims remain no stronger than attached evidence | **pending falsification** | #5 owns the final claim audit. Repository language must distinguish implemented/deterministic behavior from authenticated Windows, containment, resource, scaling, Narrator, NVDA, and demonstration proof. App-server lifecycle support must not be described as a paid real-turn result until the external bundle exists. |

## Performance and usability acceptance

| Requirement | Current status | Evidence and remaining proof |
|---|---|---|
| Existing host startup/private-memory/idle-CPU budgets remain enforced | **blocked — external evidence required** | The budgets and `scripts/measure.ps1` remain in the repository, but historical measurements do not describe the current live-slice executable. Rerun the current release build in a real Windows desktop session and attach the JSON. |
| One admitted Codex process is accounted separately from host overhead | **blocked — external evidence required** | The core has one global active-turn slot and the runner owns one admitted app-server child, with bounded cleanup tests. Attach host-only and active-provider process-tree/memory measurements for success, timeout, overflow, and forced-close paths. Zero-turn probe children must be reported separately. |
| Streaming remains bounded under a long synthetic response | **automated pass** | The machine-readable self-check reports a maximum provider/core batch depth of 64, 20,000 synthetic provider events, 313 durable chunks, a 63.9× coalescing ratio, 315 SQLite transactions, 50 operators, 20,000 background messages, and one completed terminal state. Protocol lines, diagnostics, output chunks, total assistant bytes, queues, and UI history also have explicit tested limits. |
| Complete path is keyboard reachable at 900×560 and 125%, 150%, and 200% Windows scaling | **blocked — external evidence required** | UI tests cover logical minimum-window layout, AccessKit nodes, F6 across 50 operators, and the keyboard-only fake flow. A real Windows run at every requested scale is still required; logical-point tests are not screenshot or reachability proof. |
| One Narrator and one NVDA task-flow result are attached | **blocked — external evidence required** | AccessKit labels and focus tests are implementation evidence only. Record both complete assistive-technology paths on the actual Windows executable, including start, interaction, interrupt, restart, and recovery explanation. |

## Demonstration gate

Issue #6 requires one uncut proof run. The recording and its sidecar evidence must show, in order:

1. the release executable launched against a clean runtime;
2. a Codex operator and verified isolated worktree;
3. one small auditable real turn;
4. durable streamed output;
5. one interaction (a deterministic fixture is acceptable only for this interaction step when the
   real turn does not request one);
6. a second turn interrupted during output;
7. restart at a named crash boundary;
8. recovered state plus SQLite/self-check evidence; and
9. the Git worktree and changed files without claiming in-app diff, review, merge, or commit
   recording.

**Current status: blocked — no uncut proof run or sidecar evidence is attached.**

## Reproducible automated checks

Run deterministic checks without an authenticated provider turn:

```powershell
cargo fmt --all -- --check
cargo test --locked -- --skip installed_provider_surfaces_are_probeable_without_a_turn
cargo clippy --all-targets --locked -- -D warnings
cargo build --release --locked
./target/release/agent-world.exe --self-check
./target/release/agent-world.exe --live-slice-self-check
```

The last command is owned by #5. Its output must report schema/replay results, every named crash
window, duplicate suppression, stream and transaction counts, maximum queue depths, child-process
ownership/terminal state, host/provider resource fields, and installed Codex version. Any field
reported as `external_evidence_required`, `not_run`, or equivalent keeps the matching epic row open.

## Close rule

Close #6 only when all rows above are supported by attached evidence and the uncut demonstration
exists. A green deterministic suite is not a waiver for the paid installed turn, current Windows
measurements, scaling checks, Narrator, NVDA, or artifact redaction review. Fork, Claude live turns,
in-app diff/review/merge, and final commit recording remain explicitly out of scope and unproven.
