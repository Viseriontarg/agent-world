use super::*;
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::mpsc::{self, TrySendError},
    time::Instant,
};

const EVIDENCE_SCHEMA: &str = "agent-world.live-slice-evidence.v1";
const STRESS_OPERATORS: usize = 50;
const STRESS_PERSISTED_MESSAGES: usize = 20_000;
const STRESS_STREAM_EVENTS: usize = 20_000;

/// Run deterministic, zero-model-turn evidence for the v2 live-provider slice.
///
/// This intentionally reports external Windows/manual gates as unavailable instead of turning
/// AccessKit fixtures or Linux CI into invented Narrator, NVDA, containment, or resource proof.
pub fn live_slice_self_check() -> AppResult<Value> {
    let check_started = Instant::now();
    let migration = migration_evidence()?;
    let mut fixture = QaFixture::new()?;
    let replay = replay_evidence(&mut fixture)?;
    let lifecycle = lifecycle_evidence(&mut fixture)?;
    let crash_windows = crash_window_evidence(&mut fixture)?;
    let stress = stress_evidence(&mut fixture)?;
    let queues = queue_evidence()?;
    let schema_version = fixture.schema_version()?;
    verify_quick_integrity(&fixture.store()?.conn)?;
    verify_foreign_keys(&fixture.store()?.conn)?;

    let sqlite_path = fixture.db_path.clone();
    let database_bytes = fs::metadata(&sqlite_path).map_err(err)?.len();
    fixture.close();

    Ok(json!({
        "schema": EVIDENCE_SCHEMA,
        "automated_checks_passed": true,
        "passed": false,
        "release_ready": false,
        "issue_acceptance_ready": false,
        "external_blocker_count": 6,
        "zero_model_turns": true,
        "duration_ms": check_started.elapsed().as_millis(),
        "schema_migration_and_replay": {
            "schema_version": schema_version,
            "migration": migration,
            "command_replay": replay,
            "quick_integrity": "ok",
            "foreign_keys": "ok"
        },
        "deterministic_provider_matrix": lifecycle,
        "crash_windows": crash_windows,
        "bounds_and_stress": stress,
        "queue_backpressure": queues,
        "database_bytes": database_bytes,
        "installed_codex_for_opt_in_smoke": installed_codex_evidence(),
        "automated_accessibility_scope": {
            "status": "covered_by_unit_tests",
            "evidence": [
                "minimum_window_accesskit_tree_contains_the_active_keyboard_controls",
                "scaled_minimum_window_accesskit_geometry_keeps_required_actions_reachable",
                "keyboard_only_flow_starts_answers_requests_and_interrupts",
                "keyboard_can_deny_the_exact_pending_approval",
                "responsive_metrics_are_stable_in_logical_points",
                "all_fifty_operators_remain_in_the_focusable_list_order",
                "f6_targets_every_attention_operator_in_a_fifty_operator_room"
            ],
            "proves": "deterministic keyboard commands, AccessKit labels, focus order, and logical-point layout geometry",
            "does_not_prove": [
                "real Windows rendering at 125%, 150%, or 200% scaling",
                "Narrator task-flow usability",
                "NVDA task-flow usability"
            ]
        },
        "external_evidence_required": {
            "status": "not_run_in_this_environment",
            "windows_scaling": ["125%", "150%", "200%"],
            "screen_readers": ["Narrator", "NVDA"],
            "process_containment": "real Windows Job Object descendant/leak observation",
            "authenticated_provider": "opt-in real Codex smoke with redacted protocol evidence",
            "current_list_first_resources": {
                "fixture_startup_ms": null,
                "private_memory_bytes": null,
                "idle_cpu_percent": null,
                "active_stream_cpu_percent": null,
                "active_stream_private_memory_bytes": null,
                "reason": "requires the actual Windows desktop executable and scripts/measure.ps1; historical numbers are not reused"
            }
        }
    }))
}

fn migration_evidence() -> AppResult<Value> {
    let root = QaRoot::new("migration")?;
    let runtime = root.path.join("runtime");
    fs::create_dir_all(&runtime).map_err(err)?;
    let db_path = runtime.join("state.sqlite");
    {
        let conn = Connection::open(&db_path).map_err(err)?;
        conn.execute_batch(SCHEMA_V1_SQL).map_err(err)?;
        conn.execute(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (1, 1)",
            [],
        )
        .map_err(err)?;
        conn.pragma_update(None, "user_version", 1_i64)
            .map_err(err)?;
    }
    let store = Store::open(db_path.clone(), runtime)?;
    let version: i64 = store
        .conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(err)?;
    ensure(
        version == SCHEMA_VERSION,
        "v1 fixture did not migrate to v2",
    )?;
    verify_quick_integrity(&store.conn)?;
    verify_foreign_keys(&store.conn)?;
    let backup = db_path.with_extension(format!("sqlite.pre-v{SCHEMA_VERSION}.bak"));
    ensure(backup.is_file(), "v1 migration did not preserve its backup")?;
    let backup_conn = Connection::open(&backup).map_err(err)?;
    let backup_version: i64 = backup_conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(err)?;
    ensure(
        backup_version == 1,
        "pre-v2 migration backup did not preserve schema version 1",
    )?;
    verify_quick_integrity(&backup_conn)?;
    verify_foreign_keys(&backup_conn)?;
    Ok(json!({
        "from": 1,
        "to": version,
        "transactional_open": "ok",
        "backup_created": true,
        "backup_schema_version": backup_version,
        "backup_quick_integrity": "ok",
        "backup_foreign_keys": "ok",
        "projection_validation": "ok"
    }))
}

fn replay_evidence(fixture: &mut QaFixture) -> AppResult<Value> {
    let turn_id = Uuid::new_v4();
    let envelope = CommandEnvelope::new(Command::LiveTurnStart {
        turn_id,
        thread_id: fixture.thread_id,
        text: "replay the accepted command id exactly once".into(),
    });
    let command_id = envelope.command_id;
    let (first, first_dispatch) = fixture
        .store_mut()?
        .execute_with_provider_command(envelope.clone())?;
    let (replay, replay_dispatch) = fixture
        .store_mut()?
        .execute_with_provider_command(envelope)?;
    ensure(
        first.status == "succeeded",
        "fresh replay fixture was rejected",
    )?;
    ensure(
        first.event_sequence == replay.event_sequence,
        "command replay produced a second durable acceptance",
    )?;
    ensure(
        first_dispatch.is_some(),
        "fresh command did not dispatch once",
    )?;
    ensure(
        replay_dispatch.is_none(),
        "accepted command replay produced a duplicate provider dispatch",
    )?;
    fixture.apply(&[ProviderEvent::ProcessLost {
        turn_id,
        provider_event_id: "qa-replay-terminal".into(),
        diagnostic: "deterministic replay fixture ended before provider activity".into(),
        side_effect_possible: false,
    }])?;
    ensure(
        fixture.turn_state(turn_id)? == LiveTurnState::Failed,
        "replay fixture did not reach a conservative terminal state",
    )?;
    Ok(json!({
        "command_id": command_id,
        "durable_acceptances": 1,
        "provider_dispatches": 1,
        "replay_dispatches": 0,
        "terminal": "failed"
    }))
}

fn lifecycle_evidence(fixture: &mut QaFixture) -> AppResult<Value> {
    let mut scripts = Vec::new();
    let mut duplicate_suppressions = 0_u64;
    let mut out_of_order_rejections = 0_u64;

    // 1. accepted -> started -> multiple chunks -> completed
    let normal = fixture.start_turn("normal stream")?;
    fixture.begin_stream(normal, "normal")?;
    fixture.apply(&[
        output(normal, "normal-output-1", "hello "),
        output(normal, "normal-output-2", "world"),
        completed(normal, "normal-completed", "normal"),
    ])?;
    scripts.push(fixture.terminal_script(
        "accepted_started_multiple_chunks_completed",
        normal,
        LiveTurnState::Completed,
    )?);

    // 2. approval -> allow once -> continued stream -> completed
    let approved = fixture.start_turn("approval allow once")?;
    fixture.begin_stream(approved, "approved")?;
    fixture.apply(&[approval_request(
        approved,
        "approved-request",
        "approval-allow",
    )])?;
    let thread_id = fixture.thread_id;
    let (receipt, provider_command) =
        fixture
            .store_mut()?
            .execute_with_provider_command(CommandEnvelope::new(Command::ApprovalRespond {
                turn_id: approved,
                thread_id,
                interaction_id: "approval-allow".into(),
                decision: ApprovalDecision::Approve,
            }))?;
    ensure(
        receipt.status == "succeeded",
        "allow-once response was not durable",
    )?;
    ensure(
        matches!(
            provider_command,
            Some(ProviderCommand::ApprovalResponse {
                turn_id,
                interaction_id,
                decision: ApprovalDecision::Approve,
            }) if turn_id == approved && interaction_id == "approval-allow"
        ),
        "allow-once response was not exactly correlated",
    )?;
    fixture.apply(&[
        output(approved, "approved-output", "approved"),
        completed(approved, "approved-completed", "approved"),
    ])?;
    scripts.push(fixture.terminal_script(
        "approval_allow_once_continued_stream_completed",
        approved,
        LiveTurnState::Completed,
    )?);

    // 3. approval -> deny -> provider terminal result
    let denied = fixture.start_turn("approval deny")?;
    fixture.begin_stream(denied, "denied")?;
    fixture.apply(&[approval_request(denied, "denied-request", "approval-deny")])?;
    let thread_id = fixture.thread_id;
    let (receipt, provider_command) =
        fixture
            .store_mut()?
            .execute_with_provider_command(CommandEnvelope::new(Command::ApprovalRespond {
                turn_id: denied,
                thread_id,
                interaction_id: "approval-deny".into(),
                decision: ApprovalDecision::Deny,
            }))?;
    ensure(
        receipt.status == "succeeded",
        "deny response was not durable",
    )?;
    ensure(
        matches!(
            provider_command,
            Some(ProviderCommand::ApprovalResponse {
                turn_id,
                interaction_id,
                decision: ApprovalDecision::Deny,
            }) if turn_id == denied && interaction_id == "approval-deny"
        ),
        "deny response was not exactly correlated",
    )?;
    fixture.apply(&[ProviderEvent::Failed {
        turn_id: denied,
        provider_event_id: "denied-terminal".into(),
        diagnostic: "provider stopped after the operator denied the request".into(),
    }])?;
    scripts.push(fixture.terminal_script(
        "approval_deny_provider_terminal",
        denied,
        LiveTurnState::Failed,
    )?);

    // 4. user input -> exact multiline response -> completed
    let input_turn = fixture.start_turn("multiline user input")?;
    fixture.begin_stream(input_turn, "input")?;
    fixture.apply(&[ProviderEvent::UserInputRequested {
        turn_id: input_turn,
        provider_event_id: "input-request".into(),
        interaction_id: "input-multiline".into(),
        prompt: "Answer every deterministic question".into(),
        questions: vec![
            crate::live_turn::UserInputQuestion {
                question_id: "scope".into(),
                prompt: "Which scope?".into(),
            },
            crate::live_turn::UserInputQuestion {
                question_id: "notes".into(),
                prompt: "Add notes".into(),
            },
        ],
    }])?;
    let answers = vec![
        UserInputAnswer {
            question_id: "scope".into(),
            answer: "workspace".into(),
        },
        UserInputAnswer {
            question_id: "notes".into(),
            answer: "line one\nline two".into(),
        },
    ];
    let thread_id = fixture.thread_id;
    let (receipt, provider_command) =
        fixture
            .store_mut()?
            .execute_with_provider_command(CommandEnvelope::new(Command::UserInputRespond {
                turn_id: input_turn,
                thread_id,
                interaction_id: "input-multiline".into(),
                answers: answers.clone(),
            }))?;
    ensure(
        receipt.status == "succeeded",
        "multiline response was not durable",
    )?;
    ensure(
        matches!(
            provider_command,
            Some(ProviderCommand::UserInputResponse {
                turn_id,
                interaction_id,
                answers: provider_answers,
            }) if turn_id == input_turn
                && interaction_id == "input-multiline"
                && provider_answers == answers
        ),
        "multiline response was not exactly correlated",
    )?;
    fixture.apply(&[
        output(input_turn, "input-output", "input accepted"),
        completed(input_turn, "input-completed", "input"),
    ])?;
    scripts.push(fixture.terminal_script(
        "user_input_multiline_completed",
        input_turn,
        LiveTurnState::Completed,
    )?);

    // 5. interrupt after local start but before provider session/turn acknowledgement
    let early_interrupt = fixture.start_turn("interrupt before provider acknowledgement")?;
    fixture.apply(&[ProviderEvent::Starting {
        turn_id: early_interrupt,
        provider_event_id: "early-interrupt-starting".into(),
    }])?;
    let thread_id = fixture.thread_id;
    let (_, command) = fixture
        .store_mut()?
        .execute_with_provider_command(CommandEnvelope::new(Command::LiveTurnInterrupt {
            turn_id: early_interrupt,
            thread_id,
        }))?;
    ensure(
        matches!(command, Some(ProviderCommand::Interrupt { turn_id }) if turn_id == early_interrupt),
        "pre-ack interrupt was not exactly correlated",
    )?;
    fixture.apply(&[ProviderEvent::InterruptAcknowledged {
        turn_id: early_interrupt,
        provider_event_id: "early-interrupt-terminal".into(),
        diagnostic: Some("provider stopped before start acknowledgement".into()),
    }])?;
    scripts.push(fixture.terminal_script(
        "interrupt_before_provider_start_acknowledgement",
        early_interrupt,
        LiveTurnState::Failed,
    )?);

    // 6. interrupt during stream
    let interrupted = fixture.start_turn("interrupt during stream")?;
    fixture.begin_stream(interrupted, "interrupt")?;
    fixture.apply(&[output(interrupted, "interrupt-output", "partial output")])?;
    let thread_id = fixture.thread_id;
    let (_, command) = fixture
        .store_mut()?
        .execute_with_provider_command(CommandEnvelope::new(Command::LiveTurnInterrupt {
            turn_id: interrupted,
            thread_id,
        }))?;
    ensure(
        matches!(command, Some(ProviderCommand::Interrupt { turn_id }) if turn_id == interrupted),
        "stream interrupt was not exactly correlated",
    )?;
    fixture.apply(&[ProviderEvent::InterruptAcknowledged {
        turn_id: interrupted,
        provider_event_id: "interrupt-terminal".into(),
        diagnostic: Some("operator interrupted deterministic stream".into()),
    }])?;
    scripts.push(fixture.terminal_script(
        "interrupt_during_stream",
        interrupted,
        LiveTurnState::Failed,
    )?);

    // 7. out-of-order transition rejected; exact duplicate id is suppressed once.
    let reordered = fixture.start_turn("duplicate and out-of-order events")?;
    let rejected = fixture.apply(&[ProviderEvent::SessionEstablished {
        turn_id: reordered,
        provider_event_id: "reordered-session".into(),
        session: session(reordered, "reordered"),
    }]);
    ensure(
        rejected.is_err(),
        "out-of-order session transition was accepted",
    )?;
    out_of_order_rejections += 1;
    fixture.begin_stream(reordered, "reordered")?;
    let duplicate = output(reordered, "duplicate-output", "once");
    fixture.apply(&[
        duplicate.clone(),
        duplicate,
        completed(reordered, "reordered-completed", "reordered"),
    ])?;
    let body = fixture.assistant_body(reordered)?.unwrap_or_default();
    ensure(
        body == "once",
        "duplicate provider event duplicated timeline output",
    )?;
    let duplicate_receipts = fixture.provider_receipt_count(reordered, "duplicate-output")?;
    ensure(
        duplicate_receipts == 1,
        "duplicate provider event produced duplicate receipts",
    )?;
    duplicate_suppressions += 1;
    scripts.push(fixture.terminal_script(
        "duplicate_and_out_of_order_provider_event_ids",
        reordered,
        LiveTurnState::Completed,
    )?);

    // 8. malformed/unknown/oversized normalized inputs fail before persistence. The lower-level
    // line reader, schema drift, and stderr ring are separately named adapter unit fixtures.
    let malformed_json = serde_json::from_str::<ProviderEvent>("{").is_err();
    let unknown_method = serde_json::from_str::<ProviderEvent>(
        r#"{"type":"future_provider_method","turn_id":"00000000-0000-0000-0000-000000000000","provider_event_id":"future"}"#,
    )
    .is_err();
    let oversized = ProviderEvent::AssistantOutput {
        turn_id: Uuid::nil(),
        provider_event_id: "oversized".into(),
        delta: "x".repeat(crate::live_turn::MAX_OUTPUT_DELTA_BYTES + 1),
        resume_cursor: None,
    }
    .validate()
    .is_err();
    ensure(
        malformed_json && unknown_method && oversized,
        "malformed/unknown/oversized normalized input did not fail closed",
    )?;
    scripts.push(json!({
        "name": "malformed_json_oversized_line_unknown_method_stderr_flood",
        "terminal_assertion": "adapter fail-closed fixtures",
        "terminal_status": "failed",
        "malformed_normalized_json_rejected": malformed_json,
        "unknown_normalized_method_rejected": unknown_method,
        "oversized_normalized_event_rejected": oversized,
        "adapter_tests": [
            "bounded_line_reader_drains_oversized_input_without_unbounded_allocation",
            "unknown_method_and_schema_drift_fail_closed_with_method_and_version",
            "diagnostic_ring_retains_only_its_sanitized_tail"
        ],
        "subprocesses_started": 0,
        "leaked_child_processes": 0
    }));

    // 9. exit code 0 without a provider terminal event is never invented as completion.
    let early_exit = fixture.start_turn("provider exited zero before terminal")?;
    fixture.begin_stream(early_exit, "exit-zero")?;
    fixture.apply(&[ProviderEvent::ProcessLost {
        turn_id: early_exit,
        provider_event_id: "exit-zero-terminal".into(),
        diagnostic: "provider process exited with code 0 before a terminal event".into(),
        side_effect_possible: true,
    }])?;
    scripts.push(fixture.terminal_script(
        "provider_exit_zero_before_terminal_event",
        early_exit,
        LiveTurnState::Indeterminate,
    )?);

    // 10. crash diagnostic remains explicitly bounded and terminal.
    let crash = fixture.start_turn("bounded provider crash")?;
    fixture.begin_stream(crash, "crash")?;
    let diagnostic = "x".repeat(crate::live_turn::MAX_DIAGNOSTIC_BYTES);
    fixture.apply(&[ProviderEvent::ProcessLost {
        turn_id: crash,
        provider_event_id: "bounded-crash-terminal".into(),
        diagnostic,
        side_effect_possible: true,
    }])?;
    let stored_diagnostic_bytes = fixture.turn_error_bytes(crash)?;
    ensure(
        stored_diagnostic_bytes <= MAX_TURN_ERROR_BYTES,
        "provider crash diagnostic exceeded its durable bound",
    )?;
    let mut crash_result = fixture.terminal_script(
        "provider_crash_with_bounded_diagnostic_context",
        crash,
        LiveTurnState::Indeterminate,
    )?;
    crash_result["stored_diagnostic_bytes"] = json!(stored_diagnostic_bytes);
    scripts.push(crash_result);

    ensure(
        scripts.iter().all(|script| {
            script["terminal_status"]
                .as_str()
                .is_some_and(|state| matches!(state, "completed" | "failed" | "indeterminate"))
        }),
        "one or more deterministic lifecycle scripts lacked a terminal assertion",
    )?;

    Ok(json!({
        "passed": true,
        "script_count": scripts.len(),
        "scripts": scripts,
        "duplicate_event_suppressions": duplicate_suppressions,
        "out_of_order_rejections": out_of_order_rejections,
        "automatic_duplicate_starts": 0,
        "deterministic_subprocesses_started": 0,
        "deterministic_leaked_child_processes": 0,
        "process_scope_note": "normalized provider fixtures start no OS child; real Job Object ownership remains an external Windows gate"
    }))
}

#[derive(Clone, Copy)]
enum CrashSetup {
    AcceptedOnly,
    Starting,
    Streaming,
    StreamingWithCommittedOutput,
    ApprovalObservedNotCommitted,
    ResponseCommitted,
    InterruptCommitted,
    TerminalObservedNotCommitted,
    TerminalCommitted,
}

fn crash_window_evidence(fixture: &mut QaFixture) -> AppResult<Value> {
    let cases = [
        (
            "command_accepted_before_adapter_dispatch",
            CrashSetup::AcceptedOnly,
            LiveTurnState::Indeterminate,
        ),
        (
            "provider_thread_started_before_session_cursor_commit",
            CrashSetup::AcceptedOnly,
            LiveTurnState::Indeterminate,
        ),
        (
            "turn_started_before_start_acknowledgement_commit",
            CrashSetup::Starting,
            LiveTurnState::Indeterminate,
        ),
        (
            "stream_chunk_received_before_durable_commit",
            CrashSetup::Streaming,
            LiveTurnState::Indeterminate,
        ),
        (
            "stream_chunk_committed_before_ui_observes_it",
            CrashSetup::StreamingWithCommittedOutput,
            LiveTurnState::Indeterminate,
        ),
        (
            "approval_request_received_before_interaction_commit",
            CrashSetup::ApprovalObservedNotCommitted,
            LiveTurnState::Indeterminate,
        ),
        (
            "response_accepted_before_provider_receives_it",
            CrashSetup::ResponseCommitted,
            LiveTurnState::Indeterminate,
        ),
        (
            "interrupt_accepted_before_provider_receives_it",
            CrashSetup::InterruptCommitted,
            LiveTurnState::Indeterminate,
        ),
        (
            "provider_terminal_received_before_terminal_commit",
            CrashSetup::TerminalObservedNotCommitted,
            LiveTurnState::Indeterminate,
        ),
        (
            "terminal_commit_before_ui_refresh",
            CrashSetup::TerminalCommitted,
            LiveTurnState::Completed,
        ),
    ];
    let mut outcomes = Vec::with_capacity(cases.len());
    for (name, setup, expected) in cases {
        let turn_id = fixture.start_turn(name)?;
        apply_crash_setup(fixture, turn_id, name, setup)?;
        fixture.reopen()?;
        let warnings = fixture.store_mut()?.reconcile_unfinished_turns()?;
        let actual = fixture.turn_state(turn_id)?;
        ensure(
            actual == expected,
            &format!("crash window {name} recovered as {actual:?}, expected {expected:?}"),
        )?;
        let expected_warnings = usize::from(expected == LiveTurnState::Indeterminate);
        ensure(
            warnings.len() == expected_warnings,
            &format!("crash window {name} emitted an unexpected recovery warning count"),
        )?;
        let starting_events = fixture.provider_event_type_count(turn_id, "starting")?;
        ensure(
            starting_events <= 1,
            &format!("crash window {name} produced duplicate provider starts"),
        )?;
        let turn_rows = fixture.turn_row_count(turn_id)?;
        ensure(
            turn_rows == 1,
            "crash recovery duplicated the accepted turn",
        )?;
        let committed_output_preserved =
            if matches!(setup, CrashSetup::StreamingWithCommittedOutput) {
                fixture.assistant_body(turn_id)?.as_deref() == Some("durable-before-ui")
            } else {
                true
            };
        ensure(
            committed_output_preserved,
            "crash recovery lost output committed before UI observation",
        )?;
        outcomes.push(json!({
            "name": name,
            "restart_outcome": actual.as_str(),
            "allowed_outcome": true,
            "recovery_warnings": warnings.len(),
            "provider_start_events": starting_events,
            "accepted_turn_rows": turn_rows,
            "automatic_provider_restarts": 0,
            "committed_output_preserved": committed_output_preserved
        }));
    }
    Ok(json!({
        "passed": true,
        "window_count": outcomes.len(),
        "automatic_duplicate_starts": 0,
        "outcomes": outcomes
    }))
}

fn apply_crash_setup(
    fixture: &mut QaFixture,
    turn_id: Uuid,
    label: &str,
    setup: CrashSetup,
) -> AppResult<()> {
    match setup {
        CrashSetup::AcceptedOnly => {}
        CrashSetup::Starting => {
            fixture.apply(&[ProviderEvent::Starting {
                turn_id,
                provider_event_id: format!("{label}:starting"),
            }])?;
        }
        CrashSetup::Streaming
        | CrashSetup::ApprovalObservedNotCommitted
        | CrashSetup::TerminalObservedNotCommitted => fixture.begin_stream(turn_id, label)?,
        CrashSetup::StreamingWithCommittedOutput => {
            fixture.begin_stream(turn_id, label)?;
            fixture.apply(&[output(
                turn_id,
                &format!("{label}:output"),
                "durable-before-ui",
            )])?;
        }
        CrashSetup::ResponseCommitted => {
            fixture.begin_stream(turn_id, label)?;
            let interaction_id = format!("{label}:approval");
            fixture.apply(&[approval_request(
                turn_id,
                &format!("{label}:request"),
                &interaction_id,
            )])?;
            let thread_id = fixture.thread_id;
            let (receipt, command) =
                fixture
                    .store_mut()?
                    .execute_with_provider_command(CommandEnvelope::new(
                        Command::ApprovalRespond {
                            turn_id,
                            thread_id,
                            interaction_id,
                            decision: ApprovalDecision::Approve,
                        },
                    ))?;
            ensure(
                receipt.status == "succeeded",
                "crash response was not durable",
            )?;
            ensure(
                command.is_some(),
                "crash response had no post-commit dispatch",
            )?;
        }
        CrashSetup::InterruptCommitted => {
            fixture.begin_stream(turn_id, label)?;
            let thread_id = fixture.thread_id;
            let (receipt, command) =
                fixture
                    .store_mut()?
                    .execute_with_provider_command(CommandEnvelope::new(
                        Command::LiveTurnInterrupt { turn_id, thread_id },
                    ))?;
            ensure(
                receipt.status == "succeeded",
                "crash interrupt was not durable",
            )?;
            ensure(
                command.is_some(),
                "crash interrupt had no post-commit dispatch",
            )?;
        }
        CrashSetup::TerminalCommitted => {
            fixture.begin_stream(turn_id, label)?;
            fixture.apply(&[completed(turn_id, &format!("{label}:completed"), label)])?;
        }
    }
    Ok(())
}

fn stress_evidence(fixture: &mut QaFixture) -> AppResult<Value> {
    let seed_started = Instant::now();
    let mut background_threads = Vec::with_capacity(STRESS_OPERATORS - 1);
    let project_id = fixture.project_id;
    for index in 1..STRESS_OPERATORS {
        let thread_id = Uuid::new_v4();
        let receipt =
            fixture
                .store_mut()?
                .execute(CommandEnvelope::new(Command::ThreadCreate {
                    thread_id,
                    project_id,
                    provider: if index.is_multiple_of(2) {
                        Provider::Codex
                    } else {
                        Provider::Claude
                    },
                    label: format!("QA operator {:02}", index + 1),
                }))?;
        ensure(
            receipt.status == "succeeded",
            "stress operator creation failed",
        )?;
        background_threads.push(thread_id);
    }
    {
        let now = now_ms();
        let tx = fixture.store_mut()?.conn.transaction().map_err(err)?;
        let mut versions = vec![1_u64; background_threads.len()];
        for index in 0..STRESS_PERSISTED_MESSAGES {
            let slot = index % background_threads.len();
            versions[slot] += 1;
            let thread_id = background_threads[slot];
            tx.execute(
                "INSERT INTO events
                 (aggregate_id, aggregate_version, event_type, payload_json, occurred_at)
                 VALUES (?1, ?2, 'qa.background_message', ?3, ?4)",
                params![
                    thread_id.to_string(),
                    versions[slot] as i64,
                    json!({"fixture_index": index}).to_string(),
                    now + index as i64
                ],
            )
            .map_err(err)?;
            let sequence = tx.last_insert_rowid();
            tx.execute(
                "INSERT INTO messages (sequence, thread_id, role, body, occurred_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    sequence,
                    thread_id.to_string(),
                    if index.is_multiple_of(3) {
                        "assistant"
                    } else {
                        "user"
                    },
                    format!("QA persisted message {index}"),
                    now + index as i64
                ],
            )
            .map_err(err)?;
            tx.execute(
                "UPDATE threads SET last_event_sequence = ?1 WHERE thread_id = ?2",
                params![sequence, thread_id.to_string()],
            )
            .map_err(err)?;
        }
        for (thread_id, version) in background_threads.iter().zip(versions) {
            tx.execute(
                "UPDATE aggregate_versions SET version = ?1 WHERE aggregate_id = ?2",
                params![version as i64, thread_id.to_string()],
            )
            .map_err(err)?;
        }
        tx.commit().map_err(err)?;
    }
    let fixture_seed_ms = seed_started.elapsed().as_millis();
    let operator_count: i64 = fixture
        .store()?
        .conn
        .query_row("SELECT COUNT(*) FROM threads", [], |row| row.get(0))
        .map_err(err)?;
    let background_message_count: i64 = fixture
        .store()?
        .conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE event_type = 'qa.background_message'",
            [],
            |row| row.get(0),
        )
        .map_err(err)?;
    ensure(
        operator_count == STRESS_OPERATORS as i64,
        "stress fixture did not retain 50 operators",
    )?;
    ensure(
        background_message_count == STRESS_PERSISTED_MESSAGES as i64,
        "stress fixture did not retain 20,000 messages",
    )?;

    let stream_started = Instant::now();
    let turn_id = fixture.start_turn("20k-event bounded stream")?;
    let mut sqlite_transactions = 0_u64;
    fixture.begin_stream(turn_id, "stress")?;
    sqlite_transactions += 1;
    let mut offset = 0_usize;
    let mut maximum_batch = 0_usize;
    while offset < STRESS_STREAM_EVENTS {
        let end = (offset + crate::live_turn::PROVIDER_EVENT_CAPACITY).min(STRESS_STREAM_EVENTS);
        let mut batch = Vec::with_capacity(end - offset);
        for index in offset..end {
            batch.push(output(turn_id, &format!("stress-output-{index}"), "x"));
        }
        maximum_batch = maximum_batch.max(batch.len());
        fixture.apply(&batch)?;
        sqlite_transactions += 1;
        offset = end;
    }
    let active_turns: i64 = fixture
        .store()?
        .conn
        .query_row(
            "SELECT COUNT(*) FROM turns WHERE status IN (
                'accepted', 'starting', 'streaming', 'awaiting_approval',
                'awaiting_user_input', 'interrupting'
            )",
            [],
            |row| row.get(0),
        )
        .map_err(err)?;
    ensure(
        active_turns == 1,
        "stress fixture violated the one-turn slot",
    )?;
    fixture.apply(&[completed(turn_id, "stress-completed", "stress")])?;
    sqlite_transactions += 1;
    let stream_duration_ms = stream_started.elapsed().as_millis();

    let (provider_output_events, durable_chunks): (i64, i64) = fixture
        .store()?
        .conn
        .query_row(
            "SELECT COUNT(*), COUNT(DISTINCT applied_sequence)
             FROM provider_event_receipts
             WHERE turn_id = ?1 AND event_type = 'assistant_output'",
            [turn_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(err)?;
    let (output_bytes, terminal): (i64, String) = fixture
        .store()?
        .conn
        .query_row(
            "SELECT output_bytes, status FROM turns WHERE turn_id = ?1",
            [turn_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(err)?;
    ensure(
        provider_output_events == STRESS_STREAM_EVENTS as i64,
        "stress stream lost provider event receipts",
    )?;
    ensure(
        output_bytes == STRESS_STREAM_EVENTS as i64,
        "stress stream byte accounting disagreed with durable output",
    )?;
    ensure(
        durable_chunks < provider_output_events,
        "stream persistence used one durable chunk per provider event",
    )?;
    ensure(
        sqlite_transactions < provider_output_events as u64,
        "stream persistence used one SQLite transaction per provider event",
    )?;
    ensure(terminal == "completed", "stress stream did not terminate")?;

    Ok(json!({
        "passed": true,
        "fixture_seed_ms": fixture_seed_ms,
        "active_stream_ms": stream_duration_ms,
        "operators": operator_count,
        "background_persisted_messages": background_message_count,
        "active_turns_peak": active_turns,
        "stream_provider_events": provider_output_events,
        "stream_bytes": output_bytes,
        "durable_chunks": durable_chunks,
        "sqlite_transaction_count": sqlite_transactions,
        "coalescing_ratio": provider_output_events as f64 / durable_chunks as f64,
        "maximum_provider_to_core_batch_depth": maximum_batch,
        "provider_event_capacity": crate::live_turn::PROVIDER_EVENT_CAPACITY,
        "assistant_output_limit_bytes": MAX_ASSISTANT_BYTES,
        "terminal": terminal,
        "idle_operator_processes_in_deterministic_fixture": 0,
        "owned_provider_processes_in_deterministic_fixture": 0,
        "logical_live_provider_slots": 1,
        "os_process_ownership_status": "external_windows_evidence_required"
    }))
}

fn queue_evidence() -> AppResult<Value> {
    let ui_core = saturate_channel(COMMAND_CAPACITY)?;
    let core_ui = saturate_channel(EVENT_CAPACITY)?;
    let provider_commands = saturate_channel(crate::live_turn::PROVIDER_COMMAND_CAPACITY)?;
    let provider_events = saturate_channel(crate::live_turn::PROVIDER_EVENT_CAPACITY)?;
    Ok(json!({
        "passed": true,
        "ui_to_core": ui_core,
        "core_to_ui": core_ui,
        "core_to_provider": provider_commands,
        "provider_to_core": provider_events,
        "submission": "try_send",
        "full_queue_result": "visible backpressure; no blocking send on the UI path",
        "dispatch_failure_outcomes": {
            "undispatched_start": "failed",
            "approval_response_after_provider_activity": "indeterminate",
            "user_input_response_after_provider_activity": "indeterminate",
            "interrupt_after_provider_activity": "indeterminate"
        }
    }))
}

fn saturate_channel(capacity: usize) -> AppResult<Value> {
    let (tx, _rx) = mpsc::sync_channel::<usize>(capacity);
    for value in 0..capacity {
        tx.try_send(value)
            .map_err(|error| format!("queue reached backpressure before capacity: {error}"))?;
    }
    let full = matches!(tx.try_send(capacity), Err(TrySendError::Full(_)));
    ensure(full, "bounded channel did not report full at its capacity")?;
    Ok(json!({
        "capacity": capacity,
        "maximum_depth": capacity,
        "next_try_send": "full",
        "blocked": false
    }))
}

fn installed_codex_evidence() -> Value {
    match crate::providers::locate_native_executable("codex") {
        Ok(path) => {
            let output = ProcessCommand::new(&path)
                .arg("--version")
                .output()
                .map_err(err)
                .and_then(|output| {
                    if output.status.success() {
                        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
                    } else {
                        Err(String::from_utf8_lossy(&output.stderr).trim().to_owned())
                    }
                });
            match output {
                Ok(version) => json!({
                    "status": "installed",
                    "path": path,
                    "version": version,
                    "supported": version == crate::providers::SUPPORTED_CODEX_VERSION,
                    "paid_smoke_run": false
                }),
                Err(error) => json!({
                    "status": "unavailable",
                    "path": path,
                    "diagnostic": terminal_diagnostic(&error),
                    "paid_smoke_run": false
                }),
            }
        }
        Err(error) => json!({
            "status": "not_installed",
            "diagnostic": terminal_diagnostic(&error),
            "paid_smoke_run": false
        }),
    }
}

struct QaFixture {
    store: Option<Store>,
    _root: QaRoot,
    db_path: PathBuf,
    runtime: PathBuf,
    project_id: Uuid,
    thread_id: Uuid,
}

impl QaFixture {
    fn new() -> AppResult<Self> {
        let root = QaRoot::new("live-slice")?;
        let source = create_git_fixture(&root.path)?;
        let runtime = root.path.join("runtime");
        fs::create_dir_all(runtime.join("worktrees")).map_err(err)?;
        let db_path = runtime.join("state.sqlite");
        let mut store = Store::open(db_path.clone(), runtime.clone())?;
        let project_id = Uuid::new_v4();
        let thread_id = Uuid::new_v4();
        let worktree_id = Uuid::new_v4();
        store.execute(CommandEnvelope::new(Command::ProjectCreate {
            project_id,
            name: "Live-slice QA".into(),
            repo_path: source,
        }))?;
        store.execute(CommandEnvelope::new(Command::ThreadCreate {
            thread_id,
            project_id,
            provider: Provider::Codex,
            label: "Deterministic Codex operator".into(),
        }))?;
        let worktree = store.execute(CommandEnvelope::new(Command::WorktreeCreate {
            worktree_id,
            thread_id,
        }))?;
        ensure(
            worktree.status == "succeeded",
            "QA worktree was not created",
        )?;
        Ok(Self {
            store: Some(store),
            _root: root,
            db_path,
            runtime,
            project_id,
            thread_id,
        })
    }

    fn store(&self) -> AppResult<&Store> {
        self.store
            .as_ref()
            .ok_or_else(|| "QA store is closed".into())
    }

    fn store_mut(&mut self) -> AppResult<&mut Store> {
        self.store
            .as_mut()
            .ok_or_else(|| "QA store is closed".into())
    }

    fn close(&mut self) {
        self.store.take();
    }

    fn reopen(&mut self) -> AppResult<()> {
        self.close();
        self.store = Some(Store::open(self.db_path.clone(), self.runtime.clone())?);
        Ok(())
    }

    fn schema_version(&self) -> AppResult<i64> {
        self.store()?
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(err)
    }

    fn start_turn(&mut self, label: &str) -> AppResult<Uuid> {
        let turn_id = Uuid::new_v4();
        let thread_id = self.thread_id;
        let (receipt, command) =
            self.store_mut()?
                .execute_with_provider_command(CommandEnvelope::new(Command::LiveTurnStart {
                    turn_id,
                    thread_id,
                    text: format!("QA script: {label}"),
                }))?;
        ensure(
            receipt.status == "succeeded",
            &format!("turn {label} was rejected"),
        )?;
        ensure(
            matches!(command, Some(ProviderCommand::Start { turn_id: dispatched, .. }) if dispatched == turn_id),
            &format!("turn {label} did not produce exactly one post-commit start"),
        )?;
        Ok(turn_id)
    }

    fn apply(&mut self, events: &[ProviderEvent]) -> AppResult<Vec<TurnChange>> {
        self.store_mut()?.apply_provider_events(events)
    }

    fn begin_stream(&mut self, turn_id: Uuid, label: &str) -> AppResult<()> {
        self.apply(&[
            ProviderEvent::Starting {
                turn_id,
                provider_event_id: format!("{label}:starting"),
            },
            ProviderEvent::SessionEstablished {
                turn_id,
                provider_event_id: format!("{label}:session"),
                session: session(turn_id, label),
            },
        ])?;
        Ok(())
    }

    fn turn_state(&self, turn_id: Uuid) -> AppResult<LiveTurnState> {
        let status: String = self
            .store()?
            .conn
            .query_row(
                "SELECT status FROM turns WHERE turn_id = ?1",
                [turn_id.to_string()],
                |row| row.get(0),
            )
            .map_err(err)?;
        LiveTurnState::parse(&status)
    }

    fn terminal_script(
        &self,
        name: &str,
        turn_id: Uuid,
        expected: LiveTurnState,
    ) -> AppResult<Value> {
        let actual = self.turn_state(turn_id)?;
        ensure(
            actual == expected,
            &format!("script {name} ended as {actual:?}"),
        )?;
        ensure(
            actual.is_terminal(),
            &format!("script {name} is not terminal"),
        )?;
        let starts = self.provider_event_type_count(turn_id, "starting")?;
        ensure(
            starts <= 1,
            &format!("script {name} started more than once"),
        )?;
        ensure(
            self.turn_row_count(turn_id)? == 1,
            &format!("script {name} duplicated its turn row"),
        )?;
        Ok(json!({
            "name": name,
            "terminal_assertion": true,
            "terminal_status": actual.as_str(),
            "provider_start_events": starts,
            "accepted_turn_rows": 1,
            "subprocesses_started": 0,
            "leaked_child_processes": 0
        }))
    }

    fn provider_event_type_count(&self, turn_id: Uuid, event_type: &str) -> AppResult<i64> {
        self.store()?
            .conn
            .query_row(
                "SELECT COUNT(*) FROM provider_event_receipts
                 WHERE turn_id = ?1 AND event_type = ?2",
                params![turn_id.to_string(), event_type],
                |row| row.get(0),
            )
            .map_err(err)
    }

    fn provider_receipt_count(&self, turn_id: Uuid, event_id: &str) -> AppResult<i64> {
        self.store()?
            .conn
            .query_row(
                "SELECT COUNT(*) FROM provider_event_receipts
                 WHERE turn_id = ?1 AND provider_event_id = ?2",
                params![turn_id.to_string(), event_id],
                |row| row.get(0),
            )
            .map_err(err)
    }

    fn turn_row_count(&self, turn_id: Uuid) -> AppResult<i64> {
        self.store()?
            .conn
            .query_row(
                "SELECT COUNT(*) FROM turns WHERE turn_id = ?1",
                [turn_id.to_string()],
                |row| row.get(0),
            )
            .map_err(err)
    }

    fn assistant_body(&self, turn_id: Uuid) -> AppResult<Option<String>> {
        self.store()?
            .conn
            .query_row(
                "SELECT m.body FROM turns t
                 LEFT JOIN messages m ON m.sequence = t.assistant_message_sequence
                 WHERE t.turn_id = ?1",
                [turn_id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(err)
            .map(Option::flatten)
    }

    fn turn_error_bytes(&self, turn_id: Uuid) -> AppResult<usize> {
        let bytes: i64 = self
            .store()?
            .conn
            .query_row(
                "SELECT length(CAST(error AS BLOB)) FROM turns WHERE turn_id = ?1",
                [turn_id.to_string()],
                |row| row.get(0),
            )
            .map_err(err)?;
        usize::try_from(bytes).map_err(err)
    }
}

struct QaRoot {
    path: PathBuf,
}

impl QaRoot {
    fn new(label: &str) -> AppResult<Self> {
        let path = std::env::temp_dir().join(format!("agent-world-{label}-{}", Uuid::new_v4()));
        fs::create_dir_all(&path).map_err(err)?;
        Ok(Self { path })
    }
}

impl Drop for QaRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn create_git_fixture(root: &Path) -> AppResult<PathBuf> {
    let source = root.join("source");
    fs::create_dir_all(&source).map_err(err)?;
    run_git(&source, &["init", "--initial-branch=main"])?;
    run_git(
        &source,
        &["config", "user.email", "agent-world@local.invalid"],
    )?;
    run_git(&source, &["config", "user.name", "Agent World QA"])?;
    fs::write(source.join("README.md"), "# deterministic fixture\n").map_err(err)?;
    run_git(&source, &["add", "README.md"])?;
    run_git(&source, &["commit", "-m", "fixture"])?;
    Ok(source)
}

fn session(turn_id: Uuid, label: &str) -> ProviderSessionCursor {
    ProviderSessionCursor {
        session_id: format!("qa-{label}-{turn_id}"),
        resume_cursor: format!("qa-cursor-{label}-{turn_id}"),
    }
}

fn output(turn_id: Uuid, provider_event_id: &str, delta: &str) -> ProviderEvent {
    ProviderEvent::AssistantOutput {
        turn_id,
        provider_event_id: provider_event_id.into(),
        delta: delta.into(),
        resume_cursor: Some(format!("cursor-{provider_event_id}")),
    }
}

fn completed(turn_id: Uuid, provider_event_id: &str, label: &str) -> ProviderEvent {
    ProviderEvent::Completed {
        turn_id,
        provider_event_id: provider_event_id.into(),
        session: session(turn_id, label),
    }
}

fn approval_request(turn_id: Uuid, event_id: &str, interaction_id: &str) -> ProviderEvent {
    ProviderEvent::ApprovalRequested {
        turn_id,
        provider_event_id: event_id.into(),
        interaction_id: interaction_id.into(),
        prompt: "Allow this deterministic operation once?".into(),
        operation: Some("run command".into()),
        path: Some("fixture".into()),
        command: Some("cargo test".into()),
        consequence: Some("Runs the deterministic fixture once".into()),
    }
}

fn ensure(condition: bool, message: &str) -> AppResult<()> {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn machine_readable_live_slice_matrix_passes_all_automated_gates() {
        let evidence = live_slice_self_check().expect("run live-slice evidence");
        assert_eq!(evidence["schema"], EVIDENCE_SCHEMA);
        assert_eq!(evidence["automated_checks_passed"], true);
        assert_eq!(evidence["passed"], false);
        assert_eq!(evidence["release_ready"], false);
        assert_eq!(evidence["external_blocker_count"], 6);
        assert_eq!(
            evidence["deterministic_provider_matrix"]["script_count"],
            10
        );
        assert_eq!(evidence["crash_windows"]["window_count"], 10);
        assert_eq!(evidence["bounds_and_stress"]["operators"], 50);
        assert_eq!(
            evidence["bounds_and_stress"]["background_persisted_messages"],
            20_000
        );
        assert_eq!(
            evidence["external_evidence_required"]["status"],
            "not_run_in_this_environment"
        );
    }
}
