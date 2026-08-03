use serde::{Deserialize, Serialize};
use std::{
    path::PathBuf,
    sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError},
    thread,
    time::Duration,
};
use uuid::Uuid;

pub const PROVIDER_COMMAND_CAPACITY: usize = 8;
pub const PROVIDER_EVENT_CAPACITY: usize = 64;
pub const MAX_PROVIDER_ID_BYTES: usize = 256;
pub const MAX_PROVIDER_EVENT_ID_BYTES: usize = 256;
pub const MAX_INTERACTION_ID_BYTES: usize = 256;
pub const MAX_RESUME_CURSOR_BYTES: usize = 4 * 1024;
pub const MAX_PROMPT_BYTES: usize = 32 * 1024;
pub const MAX_OUTPUT_DELTA_BYTES: usize = 64 * 1024;
pub const MAX_DIAGNOSTIC_BYTES: usize = 32 * 1024;
pub const MAX_INTERACTION_TEXT_BYTES: usize = 16 * 1024;
pub const MAX_USER_INPUT_QUESTIONS: usize = 32;
pub const MAX_USER_INPUT_ANSWERS: usize = 32;

/// Durable, provider-neutral authority policy for the Slice 2 live operator.
///
/// The verified isolated worktree is the only configured writable root. Operations that need
/// authority beyond that sandbox are routed back to the user for an explicit one-time decision.
/// The provider adapter must map this policy to its reviewed, version-pinned transport shape.
pub const ISOLATED_WORKSPACE_WRITE_POLICY: &str = "isolated_workspace_write_on_request_v1";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ProviderReadiness {
    Available,
    Unavailable {
        diagnostic: String,
    },
    UnsupportedVersion {
        installed: String,
        supported: String,
    },
}

impl ProviderReadiness {
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Available => Ok(()),
            Self::Unavailable { diagnostic } => bounded_nonempty(
                "provider readiness diagnostic",
                diagnostic,
                MAX_DIAGNOSTIC_BYTES,
            ),
            Self::UnsupportedVersion {
                installed,
                supported,
            } => {
                bounded_nonempty(
                    "installed provider version",
                    installed,
                    MAX_PROVIDER_ID_BYTES,
                )?;
                bounded_nonempty(
                    "supported provider version",
                    supported,
                    MAX_PROVIDER_ID_BYTES,
                )
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LiveTurnState {
    Accepted,
    Starting,
    Streaming,
    AwaitingApproval,
    AwaitingUserInput,
    Interrupting,
    Completed,
    Failed,
    Indeterminate,
}

impl LiveTurnState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Starting => "starting",
            Self::Streaming => "streaming",
            Self::AwaitingApproval => "awaiting_approval",
            Self::AwaitingUserInput => "awaiting_user_input",
            Self::Interrupting => "interrupting",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Indeterminate => "indeterminate",
        }
    }

    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "accepted" => Ok(Self::Accepted),
            "starting" => Ok(Self::Starting),
            "streaming" => Ok(Self::Streaming),
            "awaiting_approval" => Ok(Self::AwaitingApproval),
            "awaiting_user_input" => Ok(Self::AwaitingUserInput),
            "interrupting" => Ok(Self::Interrupting),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "indeterminate" => Ok(Self::Indeterminate),
            other => Err(format!("unknown live-turn state {other:?}")),
        }
    }

    pub const fn is_active(self) -> bool {
        matches!(
            self,
            Self::Accepted
                | Self::Starting
                | Self::Streaming
                | Self::AwaitingApproval
                | Self::AwaitingUserInput
                | Self::Interrupting
        )
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Indeterminate)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ProviderSessionCursor {
    pub session_id: String,
    pub resume_cursor: String,
}

impl ProviderSessionCursor {
    pub fn validate(&self) -> Result<(), String> {
        bounded_nonempty(
            "provider session id",
            &self.session_id,
            MAX_PROVIDER_ID_BYTES,
        )?;
        bounded_nonempty(
            "provider resume cursor",
            &self.resume_cursor,
            MAX_RESUME_CURSOR_BYTES,
        )
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    Approve,
    Deny,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct UserInputQuestion {
    pub question_id: String,
    pub prompt: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct UserInputAnswer {
    pub question_id: String,
    pub answer: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProviderCommand {
    Start {
        turn_id: Uuid,
        thread_id: Uuid,
        worktree_path: PathBuf,
        prompt: String,
        session: Option<ProviderSessionCursor>,
    },
    ApprovalResponse {
        turn_id: Uuid,
        interaction_id: String,
        decision: ApprovalDecision,
    },
    UserInputResponse {
        turn_id: Uuid,
        interaction_id: String,
        answers: Vec<UserInputAnswer>,
    },
    Interrupt {
        turn_id: Uuid,
    },
    Shutdown,
}

impl ProviderCommand {
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Start {
                worktree_path,
                prompt,
                session,
                ..
            } => {
                if worktree_path.as_os_str().is_empty() {
                    return Err("provider start requires a worktree path".into());
                }
                bounded_nonempty("live-turn prompt", prompt, MAX_PROMPT_BYTES)?;
                if let Some(session) = session {
                    session.validate()?;
                }
            }
            Self::ApprovalResponse { interaction_id, .. } => {
                bounded_nonempty(
                    "approval interaction id",
                    interaction_id,
                    MAX_INTERACTION_ID_BYTES,
                )?;
            }
            Self::UserInputResponse {
                interaction_id,
                answers,
                ..
            } => {
                bounded_nonempty(
                    "user-input interaction id",
                    interaction_id,
                    MAX_INTERACTION_ID_BYTES,
                )?;
                if answers.is_empty() || answers.len() > MAX_USER_INPUT_ANSWERS {
                    return Err(format!(
                        "user-input response must contain 1..={MAX_USER_INPUT_ANSWERS} answers"
                    ));
                }
                for answer in answers {
                    bounded_nonempty(
                        "user-input question id",
                        &answer.question_id,
                        MAX_INTERACTION_ID_BYTES,
                    )?;
                    bounded(
                        "user-input answer",
                        &answer.answer,
                        MAX_INTERACTION_TEXT_BYTES,
                    )?;
                }
            }
            Self::Interrupt { .. } | Self::Shutdown => {}
        }
        Ok(())
    }

    pub const fn turn_id(&self) -> Option<Uuid> {
        match self {
            Self::Start { turn_id, .. }
            | Self::ApprovalResponse { turn_id, .. }
            | Self::UserInputResponse { turn_id, .. }
            | Self::Interrupt { turn_id } => Some(*turn_id),
            Self::Shutdown => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProviderEvent {
    Starting {
        turn_id: Uuid,
        provider_event_id: String,
    },
    SessionEstablished {
        turn_id: Uuid,
        provider_event_id: String,
        session: ProviderSessionCursor,
    },
    Resumed {
        turn_id: Uuid,
        provider_event_id: String,
        session: ProviderSessionCursor,
    },
    AssistantOutput {
        turn_id: Uuid,
        provider_event_id: String,
        delta: String,
        resume_cursor: Option<String>,
    },
    ApprovalRequested {
        turn_id: Uuid,
        provider_event_id: String,
        interaction_id: String,
        prompt: String,
        operation: Option<String>,
        path: Option<String>,
        command: Option<String>,
        consequence: Option<String>,
    },
    UserInputRequested {
        turn_id: Uuid,
        provider_event_id: String,
        interaction_id: String,
        prompt: String,
        questions: Vec<UserInputQuestion>,
    },
    InterruptAcknowledged {
        turn_id: Uuid,
        provider_event_id: String,
        diagnostic: Option<String>,
    },
    Completed {
        turn_id: Uuid,
        provider_event_id: String,
        session: ProviderSessionCursor,
    },
    Failed {
        turn_id: Uuid,
        provider_event_id: String,
        diagnostic: String,
    },
    ProcessLost {
        turn_id: Uuid,
        provider_event_id: String,
        diagnostic: String,
        side_effect_possible: bool,
    },
}

impl ProviderEvent {
    pub const fn turn_id(&self) -> Uuid {
        match self {
            Self::Starting { turn_id, .. }
            | Self::SessionEstablished { turn_id, .. }
            | Self::Resumed { turn_id, .. }
            | Self::AssistantOutput { turn_id, .. }
            | Self::ApprovalRequested { turn_id, .. }
            | Self::UserInputRequested { turn_id, .. }
            | Self::InterruptAcknowledged { turn_id, .. }
            | Self::Completed { turn_id, .. }
            | Self::Failed { turn_id, .. }
            | Self::ProcessLost { turn_id, .. } => *turn_id,
        }
    }

    pub fn provider_event_id(&self) -> &str {
        match self {
            Self::Starting {
                provider_event_id, ..
            }
            | Self::SessionEstablished {
                provider_event_id, ..
            }
            | Self::Resumed {
                provider_event_id, ..
            }
            | Self::AssistantOutput {
                provider_event_id, ..
            }
            | Self::ApprovalRequested {
                provider_event_id, ..
            }
            | Self::UserInputRequested {
                provider_event_id, ..
            }
            | Self::InterruptAcknowledged {
                provider_event_id, ..
            }
            | Self::Completed {
                provider_event_id, ..
            }
            | Self::Failed {
                provider_event_id, ..
            }
            | Self::ProcessLost {
                provider_event_id, ..
            } => provider_event_id,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        bounded_nonempty(
            "provider event id",
            self.provider_event_id(),
            MAX_PROVIDER_EVENT_ID_BYTES,
        )?;
        match self {
            Self::SessionEstablished { session, .. }
            | Self::Resumed { session, .. }
            | Self::Completed { session, .. } => session.validate()?,
            Self::AssistantOutput {
                delta,
                resume_cursor,
                ..
            } => {
                bounded_nonempty("assistant output delta", delta, MAX_OUTPUT_DELTA_BYTES)?;
                if let Some(cursor) = resume_cursor {
                    bounded_nonempty("provider resume cursor", cursor, MAX_RESUME_CURSOR_BYTES)?;
                }
            }
            Self::ApprovalRequested {
                interaction_id,
                prompt,
                operation,
                path,
                command,
                consequence,
                ..
            } => {
                bounded_nonempty(
                    "approval interaction id",
                    interaction_id,
                    MAX_INTERACTION_ID_BYTES,
                )?;
                bounded_nonempty("approval prompt", prompt, MAX_INTERACTION_TEXT_BYTES)?;
                for (label, value) in [
                    ("approval operation", operation),
                    ("approval path", path),
                    ("approval command", command),
                    ("approval consequence", consequence),
                ] {
                    if let Some(value) = value {
                        bounded(label, value, MAX_INTERACTION_TEXT_BYTES)?;
                    }
                }
            }
            Self::UserInputRequested {
                interaction_id,
                prompt,
                questions,
                ..
            } => {
                bounded_nonempty(
                    "user-input interaction id",
                    interaction_id,
                    MAX_INTERACTION_ID_BYTES,
                )?;
                bounded_nonempty("user-input prompt", prompt, MAX_INTERACTION_TEXT_BYTES)?;
                if questions.is_empty() || questions.len() > MAX_USER_INPUT_QUESTIONS {
                    return Err(format!(
                        "user-input request must contain 1..={MAX_USER_INPUT_QUESTIONS} questions"
                    ));
                }
                for question in questions {
                    bounded_nonempty(
                        "user-input question id",
                        &question.question_id,
                        MAX_INTERACTION_ID_BYTES,
                    )?;
                    bounded_nonempty(
                        "user-input question prompt",
                        &question.prompt,
                        MAX_INTERACTION_TEXT_BYTES,
                    )?;
                }
            }
            Self::InterruptAcknowledged { diagnostic, .. } => {
                if let Some(diagnostic) = diagnostic {
                    bounded("interrupt diagnostic", diagnostic, MAX_DIAGNOSTIC_BYTES)?;
                }
            }
            Self::Failed { diagnostic, .. } | Self::ProcessLost { diagnostic, .. } => {
                bounded_nonempty("provider diagnostic", diagnostic, MAX_DIAGNOSTIC_BYTES)?;
            }
            Self::Starting { .. } => {}
        }
        Ok(())
    }
}

pub trait ProviderRunner: Send + 'static {
    fn readiness(&self) -> ProviderReadiness {
        ProviderReadiness::Available
    }

    fn run(
        self: Box<Self>,
        commands: Receiver<ProviderCommand>,
        events: SyncSender<ProviderEvent>,
    ) -> Result<(), String>;
}

pub struct ProviderPort {
    commands: Option<SyncSender<ProviderCommand>>,
    events: Receiver<ProviderEvent>,
    join: Option<thread::JoinHandle<Result<(), String>>>,
}

impl ProviderPort {
    pub fn spawn(runner: Box<dyn ProviderRunner>) -> Result<Self, String> {
        let (command_tx, command_rx) = mpsc::sync_channel(PROVIDER_COMMAND_CAPACITY);
        let (event_tx, event_rx) = mpsc::sync_channel(PROVIDER_EVENT_CAPACITY);
        let join = thread::Builder::new()
            .name("agent-world-provider-port".into())
            .spawn(move || runner.run(command_rx, event_tx))
            .map_err(|error| error.to_string())?;
        Ok(Self {
            commands: Some(command_tx),
            events: event_rx,
            join: Some(join),
        })
    }

    pub fn command_sender(&self) -> &SyncSender<ProviderCommand> {
        self.commands.as_ref().expect("provider port is running")
    }

    pub fn try_recv_event(&self) -> Result<ProviderEvent, TryRecvError> {
        self.events.try_recv()
    }

    pub fn recv_event_timeout(&self, timeout: Duration) -> Result<ProviderEvent, RecvTimeoutError> {
        self.events.recv_timeout(timeout)
    }

    pub fn is_finished(&self) -> bool {
        self.join
            .as_ref()
            .is_none_or(thread::JoinHandle::is_finished)
    }

    pub fn begin_shutdown(&mut self) {
        if let Some(commands) = self.commands.take() {
            let _ = commands.try_send(ProviderCommand::Shutdown);
        }
    }

    pub fn finish_if_stopped(&mut self) -> Result<bool, String> {
        if !self.is_finished() {
            return Ok(false);
        }
        let Some(join) = self.join.take() else {
            return Ok(true);
        };
        join.join()
            .map_err(|_| "provider runner panicked".to_owned())??;
        Ok(true)
    }
}

impl Drop for ProviderPort {
    fn drop(&mut self) {
        if self.join.as_ref().is_some_and(|join| join.is_finished()) {
            self.begin_shutdown();
            let _ = self.finish_if_stopped();
        } else {
            self.begin_shutdown();
        }
    }
}

fn bounded(label: &str, value: &str, max_bytes: usize) -> Result<(), String> {
    if value.len() > max_bytes {
        Err(format!(
            "{label} is {} bytes; maximum is {max_bytes}",
            value.len()
        ))
    } else {
        Ok(())
    }
}

fn bounded_nonempty(label: &str, value: &str, max_bytes: usize) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{label} must not be empty"));
    }
    bounded(label, value, max_bytes)
}

#[cfg(test)]
pub(crate) mod fake {
    use super::*;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum FakeScript {
        NormalStreamAndCompletion,
        ApprovalRequestAndResponse,
        UserInputRequestAndResponse,
        InterruptDuringStreaming,
        DuplicateProviderEvent,
        ProcessLossBeforeStartAcknowledgement,
        ProcessLossAfterOutputBeforeTerminal,
        RestartAndResume,
    }

    pub struct DeterministicFakeRunner {
        script: FakeScript,
    }

    impl DeterministicFakeRunner {
        pub const fn new(script: FakeScript) -> Self {
            Self { script }
        }
    }

    impl ProviderRunner for DeterministicFakeRunner {
        fn run(
            self: Box<Self>,
            commands: Receiver<ProviderCommand>,
            events: SyncSender<ProviderEvent>,
        ) -> Result<(), String> {
            let (turn_id, resumed_session) = recv_start(&commands)?;
            match self.script {
                FakeScript::NormalStreamAndCompletion => {
                    begin_stream(&events, turn_id)?;
                    send_output(&events, turn_id, "output-1", "hello ")?;
                    send_output(&events, turn_id, "output-2", "world")?;
                    complete(&events, turn_id, "cursor-terminal")?;
                }
                FakeScript::ApprovalRequestAndResponse => {
                    begin_stream(&events, turn_id)?;
                    send(
                        &events,
                        ProviderEvent::ApprovalRequested {
                            turn_id,
                            provider_event_id: "approval-request".into(),
                            interaction_id: "approval-1".into(),
                            prompt: "Allow this operation outside the workspace sandbox?".into(),
                            operation: Some("inspect".into()),
                            path: Some("src/core.rs".into()),
                            command: Some("git diff -- src/core.rs".into()),
                            consequence: Some("Reads repository state only".into()),
                        },
                    )?;
                    match commands.recv().map_err(|error| error.to_string())? {
                        ProviderCommand::ApprovalResponse {
                            turn_id: response_turn,
                            interaction_id,
                            decision: ApprovalDecision::Approve,
                        } if response_turn == turn_id && interaction_id == "approval-1" => {}
                        other => {
                            return Err(format!(
                                "fake expected matching approval response, received {other:?}"
                            ));
                        }
                    }
                    send_output(&events, turn_id, "output-after-approval", "approved")?;
                    complete(&events, turn_id, "cursor-terminal")?;
                }
                FakeScript::UserInputRequestAndResponse => {
                    begin_stream(&events, turn_id)?;
                    send(
                        &events,
                        ProviderEvent::UserInputRequested {
                            turn_id,
                            provider_event_id: "input-request".into(),
                            interaction_id: "input-1".into(),
                            prompt: "Choose a target".into(),
                            questions: vec![UserInputQuestion {
                                question_id: "target".into(),
                                prompt: "Which target?".into(),
                            }],
                        },
                    )?;
                    match commands.recv().map_err(|error| error.to_string())? {
                        ProviderCommand::UserInputResponse {
                            turn_id: response_turn,
                            interaction_id,
                            answers,
                        } if response_turn == turn_id
                            && interaction_id == "input-1"
                            && answers
                                == vec![UserInputAnswer {
                                    question_id: "target".into(),
                                    answer: "core".into(),
                                }] => {}
                        other => {
                            return Err(format!(
                                "fake expected matching user-input response, received {other:?}"
                            ));
                        }
                    }
                    send_output(&events, turn_id, "output-after-input", "target: core")?;
                    complete(&events, turn_id, "cursor-terminal")?;
                }
                FakeScript::InterruptDuringStreaming => {
                    begin_stream(&events, turn_id)?;
                    send_output(&events, turn_id, "output-before-interrupt", "partial")?;
                    match commands.recv().map_err(|error| error.to_string())? {
                        ProviderCommand::Interrupt {
                            turn_id: interrupted_turn,
                        } if interrupted_turn == turn_id => {}
                        other => {
                            return Err(format!(
                                "fake expected matching interrupt, received {other:?}"
                            ));
                        }
                    }
                    send(
                        &events,
                        ProviderEvent::InterruptAcknowledged {
                            turn_id,
                            provider_event_id: "interrupt-ack".into(),
                            diagnostic: Some("interrupted by deterministic fake".into()),
                        },
                    )?;
                }
                FakeScript::DuplicateProviderEvent => {
                    begin_stream(&events, turn_id)?;
                    let duplicate = ProviderEvent::AssistantOutput {
                        turn_id,
                        provider_event_id: "duplicate-output".into(),
                        delta: "once".into(),
                        resume_cursor: Some("cursor-output".into()),
                    };
                    send(&events, duplicate.clone())?;
                    send(&events, duplicate)?;
                    complete(&events, turn_id, "cursor-terminal")?;
                }
                FakeScript::ProcessLossBeforeStartAcknowledgement => {
                    send(
                        &events,
                        ProviderEvent::ProcessLost {
                            turn_id,
                            provider_event_id: "lost-before-start".into(),
                            diagnostic: "fake process was never started".into(),
                            side_effect_possible: false,
                        },
                    )?;
                }
                FakeScript::ProcessLossAfterOutputBeforeTerminal => {
                    begin_stream(&events, turn_id)?;
                    send_output(&events, turn_id, "output-before-loss", "ambiguous")?;
                    send(
                        &events,
                        ProviderEvent::ProcessLost {
                            turn_id,
                            provider_event_id: "lost-after-output".into(),
                            diagnostic: "fake process disappeared before terminal acknowledgement"
                                .into(),
                            side_effect_possible: true,
                        },
                    )?;
                }
                FakeScript::RestartAndResume => {
                    let session = resumed_session.ok_or_else(|| {
                        "restart-and-resume fake requires a durable session cursor".to_owned()
                    })?;
                    send(
                        &events,
                        ProviderEvent::Starting {
                            turn_id,
                            provider_event_id: "starting".into(),
                        },
                    )?;
                    send(
                        &events,
                        ProviderEvent::Resumed {
                            turn_id,
                            provider_event_id: "resumed".into(),
                            session,
                        },
                    )?;
                    send_output(&events, turn_id, "output-after-resume", "resumed")?;
                    complete(&events, turn_id, "cursor-after-resume")?;
                }
            }
            Ok(())
        }
    }

    fn recv_start(
        commands: &Receiver<ProviderCommand>,
    ) -> Result<(Uuid, Option<ProviderSessionCursor>), String> {
        match commands.recv().map_err(|error| error.to_string())? {
            ProviderCommand::Start {
                turn_id, session, ..
            } => Ok((turn_id, session)),
            other => Err(format!("fake expected a start command, received {other:?}")),
        }
    }

    fn begin_stream(events: &SyncSender<ProviderEvent>, turn_id: Uuid) -> Result<(), String> {
        send(
            events,
            ProviderEvent::Starting {
                turn_id,
                provider_event_id: "starting".into(),
            },
        )?;
        send(
            events,
            ProviderEvent::SessionEstablished {
                turn_id,
                provider_event_id: "session".into(),
                session: ProviderSessionCursor {
                    session_id: "fake-session".into(),
                    resume_cursor: "cursor-0".into(),
                },
            },
        )
    }

    fn send_output(
        events: &SyncSender<ProviderEvent>,
        turn_id: Uuid,
        provider_event_id: &str,
        delta: &str,
    ) -> Result<(), String> {
        send(
            events,
            ProviderEvent::AssistantOutput {
                turn_id,
                provider_event_id: provider_event_id.into(),
                delta: delta.into(),
                resume_cursor: Some(format!("cursor-{provider_event_id}")),
            },
        )
    }

    fn complete(
        events: &SyncSender<ProviderEvent>,
        turn_id: Uuid,
        cursor: &str,
    ) -> Result<(), String> {
        send(
            events,
            ProviderEvent::Completed {
                turn_id,
                provider_event_id: "completed".into(),
                session: ProviderSessionCursor {
                    session_id: "fake-session".into(),
                    resume_cursor: cursor.into(),
                },
            },
        )
    }

    fn send(events: &SyncSender<ProviderEvent>, event: ProviderEvent) -> Result<(), String> {
        event.validate()?;
        events.send(event).map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_payloads_are_bounded_and_serializable() {
        let turn_id = Uuid::new_v4();
        let event = ProviderEvent::AssistantOutput {
            turn_id,
            provider_event_id: "event-1".into(),
            delta: "hello".into(),
            resume_cursor: Some("cursor-1".into()),
        };
        event.validate().expect("valid event");
        let encoded = serde_json::to_string(&event).expect("serialize event");
        let decoded: ProviderEvent = serde_json::from_str(&encoded).expect("deserialize event");
        assert_eq!(decoded, event);

        let oversized = ProviderEvent::AssistantOutput {
            turn_id,
            provider_event_id: "event-2".into(),
            delta: "x".repeat(MAX_OUTPUT_DELTA_BYTES + 1),
            resume_cursor: None,
        };
        assert!(oversized.validate().is_err());
    }

    #[test]
    fn live_turn_state_has_only_the_normalized_v2_lifecycle() {
        let states = [
            LiveTurnState::Accepted,
            LiveTurnState::Starting,
            LiveTurnState::Streaming,
            LiveTurnState::AwaitingApproval,
            LiveTurnState::AwaitingUserInput,
            LiveTurnState::Interrupting,
            LiveTurnState::Completed,
            LiveTurnState::Failed,
            LiveTurnState::Indeterminate,
        ];
        for state in states {
            assert_eq!(LiveTurnState::parse(state.as_str()), Ok(state));
        }
        assert_eq!(states.iter().filter(|state| state.is_active()).count(), 6);
        assert_eq!(states.iter().filter(|state| state.is_terminal()).count(), 3);
    }
}
