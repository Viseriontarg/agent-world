use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    process::Command as ProcessCommand,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

use crate::live_turn::{
    ApprovalDecision, ISOLATED_WORKSPACE_WRITE_POLICY, LiveTurnState, MAX_INTERACTION_ID_BYTES,
    MAX_OUTPUT_DELTA_BYTES, MAX_PROMPT_BYTES, ProviderCommand, ProviderEvent, ProviderPort,
    ProviderReadiness, ProviderRunner, ProviderSessionCursor, UserInputAnswer,
};

mod live_slice_qa;
pub use live_slice_qa::live_slice_self_check;

pub const PROTOCOL_VERSION: u16 = 2;
const SCHEMA_VERSION: i64 = 2;
const COMMAND_CAPACITY: usize = 8;
const EVENT_CAPACITY: usize = 32;
const CORE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const CORE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(8);
const MAX_ASSISTANT_BYTES: usize = 1024 * 1024;
const MAX_TURN_ERROR_BYTES: usize = 32 * 1024;
const DIAGNOSTIC_TRUNCATION_MARKER: &str = "\n[diagnostic truncated]";

const SCHEMA_V1_SQL: &str = "
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version INTEGER PRIMARY KEY,
        applied_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS command_receipts (
        command_id TEXT PRIMARY KEY,
        protocol_version INTEGER NOT NULL,
        command_json TEXT NOT NULL,
        status TEXT NOT NULL,
        result_json TEXT NOT NULL,
        event_sequence INTEGER,
        recorded_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS worktree_plans (
        command_id TEXT PRIMARY KEY REFERENCES command_receipts(command_id),
        worktree_id TEXT NOT NULL,
        thread_id TEXT NOT NULL,
        project_id TEXT NOT NULL,
        repo_path TEXT NOT NULL,
        repo_common_dir TEXT NOT NULL,
        branch TEXT NOT NULL,
        path TEXT NOT NULL,
        commit_oid TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS aggregate_versions (
        aggregate_id TEXT PRIMARY KEY,
        version INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS events (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        aggregate_id TEXT NOT NULL,
        aggregate_version INTEGER NOT NULL,
        event_type TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        occurred_at INTEGER NOT NULL,
        UNIQUE(aggregate_id, aggregate_version)
    );
    CREATE TABLE IF NOT EXISTS projects (
        project_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        repo_path TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS worktrees (
        worktree_id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL REFERENCES projects(project_id),
        branch TEXT NOT NULL,
        path TEXT NOT NULL UNIQUE,
        status TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS threads (
        thread_id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL REFERENCES projects(project_id),
        worktree_id TEXT REFERENCES worktrees(worktree_id),
        provider TEXT NOT NULL,
        label TEXT NOT NULL,
        state TEXT NOT NULL,
        attention INTEGER NOT NULL DEFAULT 0,
        unread_count INTEGER NOT NULL DEFAULT 0,
        last_event_sequence INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS messages (
        sequence INTEGER PRIMARY KEY REFERENCES events(sequence),
        thread_id TEXT NOT NULL REFERENCES threads(thread_id),
        role TEXT NOT NULL,
        body TEXT NOT NULL,
        occurred_at INTEGER NOT NULL
    );
";

const TURNS_TABLE_SQL: &str = "
    CREATE TABLE turns (
        turn_id TEXT PRIMARY KEY,
        thread_id TEXT NOT NULL REFERENCES threads(thread_id),
        provider TEXT NOT NULL CHECK (provider IN ('codex', 'claude')),
        provider_session_id TEXT CHECK (
            provider_session_id IS NULL OR
            (length(trim(provider_session_id)) > 0 AND
             length(CAST(provider_session_id AS BLOB)) <= 256)
        ),
        resume_cursor TEXT CHECK (
            resume_cursor IS NULL OR
            (provider_session_id IS NOT NULL AND
             length(trim(resume_cursor)) > 0 AND
             length(CAST(resume_cursor AS BLOB)) <= 4096)
        ),
        worktree_path TEXT NOT NULL CHECK (length(trim(worktree_path)) > 0),
        policy TEXT NOT NULL CHECK (policy = 'isolated_workspace_write_on_request_v1'),
        status TEXT NOT NULL CHECK (
            status IN (
                'accepted', 'starting', 'streaming', 'awaiting_approval',
                'awaiting_user_input', 'interrupting', 'completed', 'failed', 'indeterminate'
            )
        ),
        prompt_sequence INTEGER NOT NULL REFERENCES messages(sequence),
        accepted_sequence INTEGER NOT NULL REFERENCES events(sequence),
        started_sequence INTEGER REFERENCES events(sequence),
        assistant_message_sequence INTEGER REFERENCES messages(sequence),
        terminal_sequence INTEGER REFERENCES events(sequence),
        output_bytes INTEGER NOT NULL DEFAULT 0 CHECK (output_bytes BETWEEN 0 AND 1048576),
        error TEXT CHECK (
            error IS NULL OR
            (length(CAST(error AS BLOB)) BETWEEN 1 AND 32768)
        ),
        accepted_at INTEGER NOT NULL CHECK (accepted_at >= 0),
        started_at INTEGER CHECK (started_at IS NULL OR started_at >= 0),
        finished_at INTEGER CHECK (finished_at IS NULL OR finished_at >= 0),
        CHECK (
            (status = 'accepted' AND provider_session_id IS NULL AND resume_cursor IS NULL AND
             started_sequence IS NULL AND terminal_sequence IS NULL AND error IS NULL AND
             started_at IS NULL AND finished_at IS NULL) OR
            (status IN (
                 'starting', 'streaming', 'awaiting_approval',
                 'awaiting_user_input', 'interrupting'
             ) AND started_sequence IS NOT NULL AND
             terminal_sequence IS NULL AND error IS NULL AND
             started_at IS NOT NULL AND finished_at IS NULL) OR
            (status = 'completed' AND provider_session_id IS NOT NULL AND resume_cursor IS NOT NULL AND
             started_sequence IS NOT NULL AND terminal_sequence IS NOT NULL AND error IS NULL AND
             started_at IS NOT NULL AND finished_at IS NOT NULL) OR
            (status = 'failed' AND terminal_sequence IS NOT NULL AND error IS NOT NULL AND
             finished_at IS NOT NULL) OR
            (status = 'indeterminate' AND terminal_sequence IS NOT NULL AND error IS NOT NULL AND
             finished_at IS NOT NULL)
        )
    );
";

const TURNS_ACTIVE_INDEX_SQL: &str = "
    CREATE UNIQUE INDEX turns_one_global_active
        ON turns((1)) WHERE status IN (
            'accepted', 'starting', 'streaming', 'awaiting_approval',
            'awaiting_user_input', 'interrupting'
        );
";

const PROVIDER_EVENTS_TABLE_SQL: &str = "
    CREATE TABLE provider_event_receipts (
        turn_id TEXT NOT NULL REFERENCES turns(turn_id),
        provider_event_id TEXT NOT NULL CHECK (
            length(trim(provider_event_id)) > 0 AND
            length(CAST(provider_event_id AS BLOB)) <= 256
        ),
        event_type TEXT NOT NULL CHECK (length(trim(event_type)) > 0),
        payload_json TEXT NOT NULL CHECK (
            length(CAST(payload_json AS BLOB)) BETWEEN 2 AND 131072
        ),
        applied_sequence INTEGER REFERENCES events(sequence),
        recorded_at INTEGER NOT NULL CHECK (recorded_at >= 0),
        PRIMARY KEY (turn_id, provider_event_id)
    );
";

const TURN_INTERACTIONS_TABLE_SQL: &str = "
    CREATE TABLE turn_interactions (
        interaction_id TEXT PRIMARY KEY CHECK (
            length(trim(interaction_id)) > 0 AND
            length(CAST(interaction_id AS BLOB)) <= 256
        ),
        turn_id TEXT NOT NULL REFERENCES turns(turn_id),
        kind TEXT NOT NULL CHECK (kind IN ('approval', 'user_input')),
        status TEXT NOT NULL CHECK (status IN ('pending', 'responded', 'stale')),
        request_json TEXT NOT NULL CHECK (
            length(CAST(request_json AS BLOB)) BETWEEN 2 AND 131072
        ),
        response_json TEXT CHECK (
            response_json IS NULL OR
            length(CAST(response_json AS BLOB)) BETWEEN 2 AND 131072
        ),
        requested_sequence INTEGER NOT NULL REFERENCES events(sequence),
        response_sequence INTEGER REFERENCES events(sequence),
        requested_at INTEGER NOT NULL CHECK (requested_at >= 0),
        responded_at INTEGER CHECK (responded_at IS NULL OR responded_at >= 0),
        CHECK (
            (status = 'pending' AND response_json IS NULL AND response_sequence IS NULL AND responded_at IS NULL) OR
            (status IN ('responded', 'stale') AND response_sequence IS NOT NULL AND responded_at IS NOT NULL)
        )
    );
";

pub type AppResult<T> = Result<T, String>;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    Codex,
    Claude,
}

impl Provider {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActorState {
    Idle,
    Starting,
    Running,
    AwaitingApproval,
    WaitingUser,
    Interrupting,
    Stopped,
    Failed,
    Indeterminate,
    Archived,
}

impl ActorState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::AwaitingApproval => "awaiting_approval",
            Self::WaitingUser => "waiting_user",
            Self::Interrupting => "interrupting",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
            Self::Indeterminate => "indeterminate",
            Self::Archived => "archived",
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "starting" => Self::Starting,
            "running" => Self::Running,
            "awaiting_approval" => Self::AwaitingApproval,
            "waiting_user" => Self::WaitingUser,
            "interrupting" => Self::Interrupting,
            "stopped" => Self::Stopped,
            "failed" => Self::Failed,
            "indeterminate" => Self::Indeterminate,
            "archived" => Self::Archived,
            _ => Self::Idle,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CommandEnvelope {
    pub protocol_version: u16,
    pub command_id: Uuid,
    pub expected_aggregate_version: Option<u64>,
    pub command: Command,
}

impl CommandEnvelope {
    pub fn new(command: Command) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            command_id: Uuid::new_v4(),
            expected_aggregate_version: None,
            command,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    ProjectCreate {
        project_id: Uuid,
        name: String,
        repo_path: PathBuf,
    },
    ThreadCreate {
        thread_id: Uuid,
        project_id: Uuid,
        provider: Provider,
        label: String,
    },
    WorktreeCreate {
        worktree_id: Uuid,
        thread_id: Uuid,
    },
    TurnSend {
        turn_id: Uuid,
        thread_id: Uuid,
        text: String,
    },
    LiveTurnStart {
        turn_id: Uuid,
        thread_id: Uuid,
        text: String,
    },
    ApprovalRespond {
        turn_id: Uuid,
        thread_id: Uuid,
        interaction_id: String,
        decision: ApprovalDecision,
    },
    UserInputRespond {
        turn_id: Uuid,
        thread_id: Uuid,
        interaction_id: String,
        answers: Vec<UserInputAnswer>,
    },
    LiveTurnInterrupt {
        turn_id: Uuid,
        thread_id: Uuid,
    },
    TurnInterrupt {
        thread_id: Uuid,
    },
    ThreadArchive {
        thread_id: Uuid,
    },
}

impl Command {
    fn aggregate_id(&self) -> Uuid {
        match self {
            Self::ProjectCreate { project_id, .. } => *project_id,
            Self::ThreadCreate { thread_id, .. }
            | Self::TurnSend { thread_id, .. }
            | Self::LiveTurnStart { thread_id, .. }
            | Self::ApprovalRespond { thread_id, .. }
            | Self::UserInputRespond { thread_id, .. }
            | Self::LiveTurnInterrupt { thread_id, .. }
            | Self::TurnInterrupt { thread_id }
            | Self::ThreadArchive { thread_id }
            | Self::WorktreeCreate { thread_id, .. } => *thread_id,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Receipt {
    pub command_id: Uuid,
    pub status: String,
    pub result: Value,
    pub event_sequence: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ThreadActorSnapshot {
    pub thread_id: Uuid,
    pub project_id: Uuid,
    pub worktree_id: Option<Uuid>,
    pub provider: Provider,
    pub label: String,
    pub state: ActorState,
    pub attention: bool,
    pub unread_count: u32,
    pub last_event_sequence: u64,
    pub worktree_path: Option<PathBuf>,
    pub start_gate: LiveTurnStartGate,
    pub live_turn: Option<LiveTurnSnapshot>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LiveTurnStartGate {
    Eligible,
    NoWorktree,
    ProviderUnavailable,
    UnsupportedVersion,
    PendingTurn,
    RecoveryError,
    QueuePressure,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryDisposition {
    None,
    Resumed,
    Completed,
    Failed,
    Indeterminate,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InteractionKind {
    Approval,
    UserInput,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InteractionStatus {
    Pending,
    Responded,
    Stale,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct InteractionSnapshot {
    pub interaction_id: String,
    pub kind: InteractionKind,
    pub prompt: String,
    pub operation: Option<String>,
    pub path: Option<String>,
    pub command: Option<String>,
    pub consequence: Option<String>,
    pub questions: Vec<crate::live_turn::UserInputQuestion>,
    pub status: InteractionStatus,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct LiveTurnSnapshot {
    pub turn_id: Uuid,
    pub state: LiveTurnState,
    pub session: Option<ProviderSessionCursor>,
    pub interruptible: bool,
    pub interaction: Option<InteractionSnapshot>,
    pub recovery: RecoveryDisposition,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct BootstrapSnapshot {
    pub actors: Vec<ThreadActorSnapshot>,
    pub last_sequence: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TimelineMessage {
    pub sequence: u64,
    pub role: String,
    pub body: String,
    pub occurred_at: i64,
    pub kind: TimelineRecordKind,
    pub turn_id: Option<Uuid>,
    pub event_type: String,
    pub metadata: Value,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TimelineRecordKind {
    User,
    Assistant,
    System,
    ApprovalRequest,
    UserInputRequest,
    Status,
}

pub enum CoreInput {
    Execute(CommandEnvelope),
    Bootstrap,
    Timeline { thread_id: Uuid, limit: usize },
}

pub enum CoreEvent {
    Bootstrap(BootstrapSnapshot),
    Receipt(Receipt),
    CommandError {
        command_id: Uuid,
        error: String,
    },
    Timeline {
        thread_id: Uuid,
        messages: Vec<TimelineMessage>,
    },
    TurnChanged {
        thread_id: Uuid,
        status: String,
    },
    Error(String),
}

pub struct CoreHandle {
    pub tx: SyncSender<CoreInput>,
    pub rx: Receiver<CoreEvent>,
    shutdown: Option<CoreShutdown>,
}

struct RuntimeLease {
    _file: File,
}

impl Drop for RuntimeLease {
    fn drop(&mut self) {
        let _ = self._file.unlock();
    }
}

impl RuntimeLease {
    fn acquire(runtime_root: &Path) -> AppResult<Self> {
        let path = runtime_root.join("agent-world.lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(err)?;
        file.try_lock().map_err(|error| {
            format!(
                "Agent World runtime {} is already owned by another process: {error}",
                runtime_root.display()
            )
        })?;
        Ok(Self { _file: file })
    }
}

struct CoreShutdown {
    requested: Arc<AtomicBool>,
    safe_to_release_lease: Arc<AtomicBool>,
    join: Option<thread::JoinHandle<()>>,
    _lease: RuntimeLease,
}

impl CoreHandle {
    pub fn spawn(
        runtime_root: PathBuf,
        wake_ui: impl Fn() + Send + 'static,
        provider_runner: Box<dyn ProviderRunner>,
    ) -> AppResult<Self> {
        let provider_readiness = provider_runner.readiness();
        provider_readiness.validate()?;
        fs::create_dir_all(&runtime_root).map_err(err)?;
        let lease = RuntimeLease::acquire(&runtime_root)?;
        fs::create_dir_all(runtime_root.join("worktrees")).map_err(err)?;
        fs::create_dir_all(runtime_root.join("artifacts")).map_err(err)?;
        let mut store = Store::open(runtime_root.join("state.sqlite"), runtime_root)?;
        store.provider_readiness = provider_readiness;
        let mut recovery_warnings = store.reconcile_unfinished_turns()?;
        recovery_warnings.extend(store.recover_accepted_worktrees()?);
        store.ensure_welcome()?;

        let (input_tx, input_rx) = mpsc::sync_channel(COMMAND_CAPACITY);
        let (event_tx, event_rx) = mpsc::sync_channel(EVENT_CAPACITY);
        let mut provider = ProviderPort::spawn(provider_runner)?;
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let thread_shutdown_requested = Arc::clone(&shutdown_requested);
        let safe_to_release_lease = Arc::new(AtomicBool::new(true));
        let thread_safe_to_release_lease = Arc::clone(&safe_to_release_lease);
        if !recovery_warnings.is_empty() {
            event_tx
                .try_send(CoreEvent::Error(format!(
                    "Worktree recovery needs attention:\n{}",
                    recovery_warnings.join("\n")
                )))
                .map_err(err)?;
            wake_ui();
        }
        let core_join = thread::Builder::new()
            .name("agent-world-core".into())
            .spawn(move || {
                loop {
                    if thread_shutdown_requested.load(Ordering::Acquire) {
                        break;
                    }
                    let provider_events = collect_provider_event_batch(&provider);
                    if !provider_events.is_empty() {
                        let event = apply_provider_event_batch(&mut store, &provider_events);
                        if event_tx.send(event).is_err() {
                            break;
                        }
                        wake_ui();
                        continue;
                    }
                    let input = match input_rx.recv_timeout(CORE_POLL_INTERVAL) {
                        Ok(input) => input,
                        Err(RecvTimeoutError::Timeout) => continue,
                        Err(RecvTimeoutError::Disconnected) => break,
                    };
                    let event = match input {
                        CoreInput::Execute(envelope) => {
                            let command_id = envelope.command_id;
                            match store.execute_with_provider_command(envelope) {
                                Ok((receipt, Some(command))) => {
                                    if let Err(error) =
                                        provider.command_sender().try_send(command.clone())
                                        && let Some(turn_id) = command.turn_id()
                                    {
                                        let side_effect_possible =
                                            dispatch_failure_side_effect_possible(&command);
                                        let failure = ProviderEvent::ProcessLost {
                                            turn_id,
                                            provider_event_id: format!(
                                                "core:{command_id}:dispatch-failed"
                                            ),
                                            diagnostic: format!(
                                                "provider command was durably accepted but could not be dispatched: {error}"
                                            ),
                                            side_effect_possible,
                                        };
                                        match store.apply_provider_events(&[failure]) {
                                            Ok(changes) => {
                                                if let Some(change) = changes.last() {
                                                    let _ = event_tx.try_send(
                                                        CoreEvent::TurnChanged {
                                                            thread_id: change.thread_id,
                                                            status: change.status.clone(),
                                                        },
                                                    );
                                                }
                                            }
                                            Err(failure_error) => {
                                                let _ = event_tx
                                                    .try_send(CoreEvent::Error(failure_error));
                                            }
                                        }
                                    }
                                    CoreEvent::Receipt(receipt)
                                }
                                Ok((receipt, None)) => CoreEvent::Receipt(receipt),
                                Err(error) => CoreEvent::CommandError { command_id, error },
                            }
                        }
                        CoreInput::Bootstrap => store
                            .bootstrap_snapshot()
                            .map(CoreEvent::Bootstrap)
                            .unwrap_or_else(CoreEvent::Error),
                        CoreInput::Timeline { thread_id, limit } => store
                            .timeline_page(thread_id, limit.min(100))
                            .map(|messages| CoreEvent::Timeline {
                                thread_id,
                                messages,
                            })
                            .unwrap_or_else(CoreEvent::Error),
                    };
                    if event_tx.send(event).is_err() {
                        break;
                    }
                    wake_ui();
                }
                provider.begin_shutdown();
                let provider_deadline = Instant::now() + Duration::from_secs(6);
                while !provider.is_finished() && Instant::now() < provider_deadline {
                    match provider.recv_event_timeout(CORE_POLL_INTERVAL) {
                        Ok(first) => {
                            let mut events = vec![first];
                            events.extend(collect_provider_event_batch(&provider));
                            let _ = store.apply_provider_events(&events);
                        }
                        Err(RecvTimeoutError::Timeout) => {}
                        Err(RecvTimeoutError::Disconnected) => {
                            thread::sleep(CORE_POLL_INTERVAL);
                        }
                    }
                }
                if provider.finish_if_stopped() != Ok(true) {
                    thread_safe_to_release_lease.store(false, Ordering::Release);
                }
                let _ = store.reconcile_unfinished_turns();
            })
            .map_err(err)?;

        Ok(Self {
            tx: input_tx,
            rx: event_rx,
            shutdown: Some(CoreShutdown {
                requested: shutdown_requested,
                safe_to_release_lease,
                join: Some(core_join),
                _lease: lease,
            }),
        })
    }

    pub fn command(&self, command: Command) -> AppResult<Uuid> {
        let envelope = CommandEnvelope::new(command);
        let command_id = envelope.command_id;
        self.tx
            .try_send(CoreInput::Execute(envelope))
            .map(|()| command_id)
            .map_err(err)
    }

    #[cfg(test)]
    pub(crate) fn from_channels(tx: SyncSender<CoreInput>, rx: Receiver<CoreEvent>) -> Self {
        Self {
            tx,
            rx,
            shutdown: None,
        }
    }
}

fn collect_provider_event_batch(provider: &ProviderPort) -> Vec<ProviderEvent> {
    let first = match provider.try_recv_event() {
        Ok(event) => event,
        Err(_) => return Vec::new(),
    };
    let mut events = vec![first];
    while events.len() < crate::live_turn::PROVIDER_EVENT_CAPACITY {
        match provider.recv_event_timeout(Duration::from_millis(2)) {
            Ok(event) => events.push(event),
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => break,
        }
    }
    events
}

fn dispatch_failure_side_effect_possible(command: &ProviderCommand) -> bool {
    matches!(
        command,
        ProviderCommand::ApprovalResponse { .. }
            | ProviderCommand::UserInputResponse { .. }
            | ProviderCommand::Interrupt { .. }
    )
}

fn apply_provider_event_batch(store: &mut Store, events: &[ProviderEvent]) -> CoreEvent {
    let last_turn_id = events.last().map(ProviderEvent::turn_id);
    match store.apply_provider_events(events) {
        Ok(changes) => changes
            .into_iter()
            .last()
            .map(|change| CoreEvent::TurnChanged {
                thread_id: change.thread_id,
                status: change.status,
            })
            .or_else(|| {
                last_turn_id.and_then(|turn_id| {
                    store
                        .turn_change(turn_id)
                        .ok()
                        .map(|change| CoreEvent::TurnChanged {
                            thread_id: change.thread_id,
                            status: change.status,
                        })
                })
            })
            .unwrap_or_else(|| CoreEvent::Error("empty provider event batch was ignored".into())),
        Err(error) => CoreEvent::Error(error),
    }
}

impl Drop for CoreHandle {
    fn drop(&mut self) {
        let Some(mut shutdown) = self.shutdown.take() else {
            return;
        };
        shutdown.requested.store(true, Ordering::Release);
        let (_disconnected_tx, replacement_rx) = mpsc::sync_channel(1);
        let event_rx = std::mem::replace(&mut self.rx, replacement_rx);
        drop(event_rx);
        let _ = self.tx.try_send(CoreInput::Bootstrap);

        let Some(join) = shutdown.join.take() else {
            return;
        };
        let deadline = Instant::now() + CORE_SHUTDOWN_TIMEOUT;
        while !join.is_finished() && Instant::now() < deadline {
            thread::sleep(
                CORE_POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())),
            );
        }
        if join.is_finished() {
            if join.join().is_err() {
                shutdown
                    .safe_to_release_lease
                    .store(false, Ordering::Release);
            }
            if !shutdown.safe_to_release_lease.load(Ordering::Acquire) {
                // A detached provider could still own a child/process handle. Keep the runtime
                // lease for the rest of this process rather than admitting a competing recovery.
                std::mem::forget(shutdown);
            }
        } else {
            // Keep the exclusive lease alive if shutdown itself cannot be proven complete. This
            // intentionally leaks only until process exit and prevents a second owner from running
            // recovery against a still-live core thread.
            shutdown.join = Some(join);
            std::mem::forget(shutdown);
        }
    }
}

fn migrate_schema(conn: &mut Connection, db_path: &Path, database_existed: bool) -> AppResult<()> {
    let current_version: i64 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(err)?;
    if current_version > SCHEMA_VERSION {
        return Err(format!(
            "database schema version {current_version} is newer than supported version {SCHEMA_VERSION}"
        ));
    }
    if current_version == SCHEMA_VERSION {
        return Ok(());
    }
    if !matches!(current_version, 0 | 1) {
        return Err(format!(
            "no migration path from database schema version {current_version}"
        ));
    }

    if database_existed {
        let backup_path = db_path.with_extension(format!("sqlite.pre-v{SCHEMA_VERSION}.bak"));
        if !backup_path.exists() {
            conn.execute("VACUUM INTO ?1", [backup_path.to_string_lossy().as_ref()])
                .map_err(|error| {
                    format!(
                        "could not create pre-migration backup at {}: {error}",
                        backup_path.display()
                    )
                })?;
        }
    }

    let tx = conn.transaction().map_err(err)?;
    if current_version == 0 {
        tx.execute_batch(SCHEMA_V1_SQL).map_err(err)?;
        tx.execute(
            "INSERT OR IGNORE INTO schema_migrations (version, applied_at) VALUES (1, ?1)",
            [now_ms()],
        )
        .map_err(err)?;
    }
    tx.execute_batch(TURNS_TABLE_SQL).map_err(err)?;
    tx.execute_batch(TURNS_ACTIVE_INDEX_SQL).map_err(err)?;
    tx.execute_batch(PROVIDER_EVENTS_TABLE_SQL).map_err(err)?;
    tx.execute_batch(TURN_INTERACTIONS_TABLE_SQL).map_err(err)?;
    tx.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (2, ?1)",
        [now_ms()],
    )
    .map_err(err)?;
    validate_schema(&tx)?;
    tx.execute_batch("PRAGMA user_version = 2;").map_err(err)?;
    tx.commit().map_err(err)
}

fn validate_schema(conn: &Connection) -> AppResult<()> {
    const REQUIRED_PROJECTIONS: &[(&str, &str)] = &[
        (
            "schema_migrations",
            "SELECT version, applied_at FROM schema_migrations LIMIT 0",
        ),
        (
            "command_receipts",
            "SELECT command_id, protocol_version, command_json, status, result_json, event_sequence, recorded_at FROM command_receipts LIMIT 0",
        ),
        (
            "worktree_plans",
            "SELECT command_id, worktree_id, thread_id, project_id, repo_path, repo_common_dir, branch, path, commit_oid FROM worktree_plans LIMIT 0",
        ),
        (
            "aggregate_versions",
            "SELECT aggregate_id, version FROM aggregate_versions LIMIT 0",
        ),
        (
            "events",
            "SELECT sequence, aggregate_id, aggregate_version, event_type, payload_json, occurred_at FROM events LIMIT 0",
        ),
        (
            "projects",
            "SELECT project_id, name, repo_path FROM projects LIMIT 0",
        ),
        (
            "worktrees",
            "SELECT worktree_id, project_id, branch, path, status FROM worktrees LIMIT 0",
        ),
        (
            "threads",
            "SELECT thread_id, project_id, worktree_id, provider, label, state, attention, unread_count, last_event_sequence FROM threads LIMIT 0",
        ),
        (
            "messages",
            "SELECT sequence, thread_id, role, body, occurred_at FROM messages LIMIT 0",
        ),
        (
            "turns",
            "SELECT turn_id, thread_id, provider, provider_session_id, resume_cursor, worktree_path, policy, status, prompt_sequence, accepted_sequence, started_sequence, assistant_message_sequence, terminal_sequence, output_bytes, error, accepted_at, started_at, finished_at FROM turns LIMIT 0",
        ),
        (
            "provider_event_receipts",
            "SELECT turn_id, provider_event_id, event_type, payload_json, applied_sequence, recorded_at FROM provider_event_receipts LIMIT 0",
        ),
        (
            "turn_interactions",
            "SELECT interaction_id, turn_id, kind, status, request_json, response_json, requested_sequence, response_sequence, requested_at, responded_at FROM turn_interactions LIMIT 0",
        ),
    ];
    for (name, query) in REQUIRED_PROJECTIONS {
        conn.prepare(query)
            .map_err(|error| format!("database schema validation failed for {name}: {error}"))?;
    }
    validate_turn_schema(conn)?;

    let applied: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM schema_migrations WHERE version BETWEEN 1 AND ?1",
            [SCHEMA_VERSION],
            |row| row.get(0),
        )
        .map_err(err)?;
    if applied != SCHEMA_VERSION {
        return Err(format!(
            "database migration history is incomplete: found {applied} of {SCHEMA_VERSION} records"
        ));
    }
    Ok(())
}

fn validate_turn_schema(conn: &Connection) -> AppResult<()> {
    for (kind, name, expected) in [
        ("table", "turns", TURNS_TABLE_SQL),
        ("index", "turns_one_global_active", TURNS_ACTIVE_INDEX_SQL),
        (
            "table",
            "provider_event_receipts",
            PROVIDER_EVENTS_TABLE_SQL,
        ),
        ("table", "turn_interactions", TURN_INTERACTIONS_TABLE_SQL),
    ] {
        let actual: Option<String> = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = ?1 AND name = ?2",
                params![kind, name],
                |row| row.get(0),
            )
            .optional()
            .map_err(err)?;
        let Some(actual) = actual else {
            return Err(format!(
                "database schema validation failed: missing {kind} {name}"
            ));
        };
        if normalize_schema_sql(&actual) != normalize_schema_sql(expected) {
            return Err(format!(
                "database schema validation failed: {kind} {name} has an unexpected definition"
            ));
        }
    }

    let invalid_turn: Option<String> = conn
        .query_row(
            "SELECT turn_id FROM turns
             WHERE provider NOT IN ('codex', 'claude')
                OR policy != 'isolated_workspace_write_on_request_v1'
                OR status NOT IN (
                    'accepted', 'starting', 'streaming', 'awaiting_approval',
                    'awaiting_user_input', 'interrupting', 'completed', 'failed', 'indeterminate'
                )
                OR length(trim(worktree_path)) = 0
                OR (provider_session_id IS NOT NULL AND
                    (length(trim(provider_session_id)) = 0 OR
                     length(CAST(provider_session_id AS BLOB)) > 256))
                OR (resume_cursor IS NOT NULL AND
                    (provider_session_id IS NULL OR length(trim(resume_cursor)) = 0 OR
                     length(CAST(resume_cursor AS BLOB)) > 4096))
                OR output_bytes NOT BETWEEN 0 AND 1048576
                OR accepted_at < 0
                OR (started_at IS NOT NULL AND started_at < 0)
                OR (finished_at IS NOT NULL AND finished_at < 0)
                OR NOT (
                    (status = 'accepted' AND provider_session_id IS NULL AND resume_cursor IS NULL AND
                     started_sequence IS NULL AND terminal_sequence IS NULL AND error IS NULL AND
                     started_at IS NULL AND finished_at IS NULL) OR
                    (status IN (
                         'starting', 'streaming', 'awaiting_approval',
                         'awaiting_user_input', 'interrupting'
                     ) AND started_sequence IS NOT NULL AND
                     terminal_sequence IS NULL AND error IS NULL AND
                     started_at IS NOT NULL AND finished_at IS NULL) OR
                    (status = 'completed' AND provider_session_id IS NOT NULL AND resume_cursor IS NOT NULL AND
                     started_sequence IS NOT NULL AND terminal_sequence IS NOT NULL AND
                     error IS NULL AND started_at IS NOT NULL AND finished_at IS NOT NULL) OR
                    (status = 'failed' AND terminal_sequence IS NOT NULL AND error IS NOT NULL AND
                     length(CAST(error AS BLOB)) BETWEEN 1 AND 32768 AND
                     finished_at IS NOT NULL) OR
                    (status = 'indeterminate' AND terminal_sequence IS NOT NULL AND
                     error IS NOT NULL AND length(CAST(error AS BLOB)) BETWEEN 1 AND 32768 AND
                     finished_at IS NOT NULL)
                )
             LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(err)?;
    if let Some(turn_id) = invalid_turn {
        return Err(format!(
            "database turn projection contains invalid lifecycle state for turn {turn_id}"
        ));
    }

    let active_turns: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM turns WHERE status IN (
                'accepted', 'starting', 'streaming', 'awaiting_approval',
                'awaiting_user_input', 'interrupting'
            )",
            [],
            |row| row.get(0),
        )
        .map_err(err)?;
    if active_turns > 1 {
        return Err(format!(
            "database turn projection has {active_turns} globally active turns"
        ));
    }
    validate_provider_event_projection(conn)?;
    validate_interaction_projection(conn)?;
    validate_output_projection(conn)?;
    Ok(())
}

fn validate_provider_event_projection(conn: &Connection) -> AppResult<()> {
    let mut statement = conn
        .prepare(
            "SELECT turn_id, provider_event_id, event_type, payload_json, applied_sequence
             FROM provider_event_receipts ORDER BY turn_id, provider_event_id",
        )
        .map_err(err)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<i64>>(4)?,
            ))
        })
        .map_err(err)?;
    for row in rows {
        let (turn_id, provider_event_id, event_type, payload_json, applied_sequence) =
            row.map_err(err)?;
        if applied_sequence.is_none() {
            return Err(format!(
                "provider event {provider_event_id} for turn {turn_id} has no durable application sequence"
            ));
        }
        let event: ProviderEvent = serde_json::from_str(&payload_json).map_err(|error| {
            format!("provider event {provider_event_id} for turn {turn_id} is malformed: {error}")
        })?;
        event.validate()?;
        if event.turn_id().to_string() != turn_id
            || event.provider_event_id() != provider_event_id
            || provider_event_type(&event) != event_type
        {
            return Err(format!(
                "provider event projection identity mismatch for turn {turn_id} event {provider_event_id}"
            ));
        }
    }
    Ok(())
}

fn validate_interaction_projection(conn: &Connection) -> AppResult<()> {
    let mut statement = conn
        .prepare(
            "SELECT interaction_id, turn_id, kind, status, request_json
             FROM turn_interactions ORDER BY interaction_id",
        )
        .map_err(err)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })
        .map_err(err)?;
    for row in rows {
        let (interaction_id, turn_id, kind, status, request_json) = row.map_err(err)?;
        let request: ProviderEvent = serde_json::from_str(&request_json).map_err(|error| {
            format!("interaction {interaction_id} request is malformed: {error}")
        })?;
        request.validate()?;
        let (request_turn_id, request_interaction_id, request_kind) = match &request {
            ProviderEvent::ApprovalRequested {
                turn_id,
                interaction_id,
                ..
            } => (*turn_id, interaction_id.as_str(), "approval"),
            ProviderEvent::UserInputRequested {
                turn_id,
                interaction_id,
                ..
            } => (*turn_id, interaction_id.as_str(), "user_input"),
            _ => {
                return Err(format!(
                    "interaction {interaction_id} stores a non-interaction provider event"
                ));
            }
        };
        if request_turn_id.to_string() != turn_id
            || request_interaction_id != interaction_id
            || request_kind != kind
        {
            return Err(format!(
                "interaction projection identity mismatch for {interaction_id}"
            ));
        }
        if !matches!(status.as_str(), "pending" | "responded" | "stale") {
            return Err(format!(
                "interaction {interaction_id} has invalid status {status}"
            ));
        }
    }
    Ok(())
}

fn validate_output_projection(conn: &Connection) -> AppResult<()> {
    let invalid: Option<String> = conn
        .query_row(
            "SELECT tr.turn_id
             FROM turns tr
             LEFT JOIN messages m ON m.sequence = tr.assistant_message_sequence
             WHERE (tr.assistant_message_sequence IS NULL AND tr.output_bytes != 0)
                OR (tr.assistant_message_sequence IS NOT NULL AND (
                    m.sequence IS NULL OR m.thread_id != tr.thread_id OR m.role != 'assistant'
                    OR length(CAST(m.body AS BLOB)) != tr.output_bytes
                ))
             LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(err)?;
    if let Some(turn_id) = invalid {
        return Err(format!(
            "turn {turn_id} has an inconsistent coalesced-output projection"
        ));
    }
    Ok(())
}

fn normalize_schema_sql(sql: &str) -> String {
    sql.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn verify_quick_integrity(conn: &Connection) -> AppResult<()> {
    let result: String = conn
        .query_row("PRAGMA quick_check(1)", [], |row| row.get(0))
        .map_err(err)?;
    if result.eq_ignore_ascii_case("ok") {
        Ok(())
    } else {
        Err(format!("SQLite quick integrity check failed: {result}"))
    }
}

fn verify_foreign_keys(conn: &Connection) -> AppResult<()> {
    let violation = conn
        .query_row("PRAGMA foreign_key_check", [], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .optional()
        .map_err(err)?;
    if let Some((table, row_id, parent, foreign_key)) = violation {
        return Err(format!(
            "foreign-key violation in {table} row {} referencing {parent} (constraint {foreign_key})",
            row_id.map_or_else(|| "unknown".into(), |value| value.to_string())
        ));
    }
    Ok(())
}

fn validate_projection_consistency(conn: &Connection) -> AppResult<()> {
    let duplicate_owner: Option<(String, String, i64)> = conn
        .query_row(
            "SELECT worktree_id, GROUP_CONCAT(thread_id, ', '), COUNT(*)
             FROM threads
             WHERE worktree_id IS NOT NULL
             GROUP BY worktree_id
             HAVING COUNT(*) > 1
             LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(err)?;
    if let Some((worktree_id, thread_ids, count)) = duplicate_owner {
        return Err(format!(
            "worktree {worktree_id} is attached to {count} threads ({thread_ids}); expected one owner"
        ));
    }

    let project_mismatch: Option<(String, String, String, String, String)> = conn
        .query_row(
            "SELECT wp.command_id, wp.worktree_id, wp.thread_id, wp.project_id, t.project_id
             FROM worktree_plans wp
             JOIN command_receipts cr ON cr.command_id = wp.command_id
             JOIN threads t ON t.thread_id = wp.thread_id
             WHERE cr.status = 'accepted' AND wp.project_id != t.project_id
             ORDER BY cr.recorded_at, wp.command_id
             LIMIT 1",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()
        .map_err(err)?;
    if let Some((command_id, worktree_id, thread_id, plan_project, thread_project)) =
        project_mismatch
    {
        return Err(format!(
            "accepted command {command_id} plans worktree {worktree_id} for project {plan_project}, but thread {thread_id} belongs to project {thread_project}"
        ));
    }
    Ok(())
}

struct Store {
    conn: Connection,
    runtime_root: PathBuf,
    provider_readiness: ProviderReadiness,
}

impl Store {
    fn open(db_path: PathBuf, runtime_root: PathBuf) -> AppResult<Self> {
        let database_existed = db_path
            .metadata()
            .map(|metadata| metadata.len() > 0)
            .unwrap_or(false);
        let mut conn = Connection::open(&db_path).map_err(err)?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(err)?;
        conn.busy_timeout(Duration::from_secs(3)).map_err(err)?;
        verify_quick_integrity(&conn)?;
        migrate_schema(&mut conn, &db_path, database_existed)?;
        validate_schema(&conn)?;
        verify_foreign_keys(&conn)?;
        validate_projection_consistency(&conn)?;
        let journal_mode: String = conn
            .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
            .map_err(err)?;
        if !journal_mode.eq_ignore_ascii_case("wal") {
            return Err(format!(
                "SQLite refused WAL journal mode (reported {journal_mode})"
            ));
        }
        conn.pragma_update(None, "synchronous", "FULL")
            .map_err(err)?;
        conn.pragma_update(None, "wal_autocheckpoint", 1_000_i64)
            .map_err(err)?;
        Ok(Self {
            conn,
            runtime_root,
            provider_readiness: ProviderReadiness::Available,
        })
    }

    fn ensure_welcome(&mut self) -> AppResult<()> {
        let count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM threads", [], |row| row.get(0))
            .map_err(err)?;
        if count > 0 {
            return Ok(());
        }
        let repo_path = git_toplevel(&std::env::current_dir().map_err(err)?)
            .unwrap_or_else(|_| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
        let project_id = Uuid::new_v4();
        self.execute(CommandEnvelope::new(Command::ProjectCreate {
            project_id,
            name: "Agent World".into(),
            repo_path,
        }))?;
        for (provider, label) in [
            (Provider::Codex, "Codex One"),
            (Provider::Claude, "Claude One"),
        ] {
            self.execute(CommandEnvelope::new(Command::ThreadCreate {
                thread_id: Uuid::new_v4(),
                project_id,
                provider,
                label: label.into(),
            }))?;
        }
        Ok(())
    }

    fn execute_with_provider_command(
        &mut self,
        envelope: CommandEnvelope,
    ) -> AppResult<(Receipt, Option<ProviderCommand>)> {
        let payload = serde_json::to_string(&envelope.command).map_err(err)?;
        let was_recorded = self.load_receipt(envelope.command_id, &payload)?.is_some();
        let command = envelope.command.clone();
        let receipt = self.execute(envelope)?;
        let provider_command = if !was_recorded && receipt.status == "succeeded" {
            self.load_provider_command(&command)?
        } else {
            None
        };
        Ok((receipt, provider_command))
    }

    fn load_provider_command(&self, command: &Command) -> AppResult<Option<ProviderCommand>> {
        let command = match command {
            Command::LiveTurnStart { turn_id, .. } => {
                let (thread_id, worktree_path, prompt): (String, String, String) = self
                    .conn
                    .query_row(
                        "SELECT tr.thread_id, tr.worktree_path, m.body
                         FROM turns tr
                         JOIN messages m ON m.sequence = tr.prompt_sequence
                         WHERE tr.turn_id = ?1 AND tr.status = 'accepted'",
                        [turn_id.to_string()],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .optional()
                    .map_err(err)?
                    .ok_or_else(|| format!("accepted turn {turn_id} does not exist"))?;
                let thread_id = Uuid::parse_str(&thread_id).map_err(err)?;
                let session = self
                    .conn
                    .query_row(
                        "SELECT provider_session_id, resume_cursor
                         FROM turns
                         WHERE thread_id = ?1 AND turn_id != ?2 AND status = 'completed'
                           AND provider_session_id IS NOT NULL AND resume_cursor IS NOT NULL
                         ORDER BY finished_at DESC, turn_id DESC LIMIT 1",
                        params![thread_id.to_string(), turn_id.to_string()],
                        |row| {
                            Ok(ProviderSessionCursor {
                                session_id: row.get(0)?,
                                resume_cursor: row.get(1)?,
                            })
                        },
                    )
                    .optional()
                    .map_err(err)?;
                ProviderCommand::Start {
                    turn_id: *turn_id,
                    thread_id,
                    worktree_path: PathBuf::from(worktree_path),
                    prompt,
                    session,
                }
            }
            Command::ApprovalRespond {
                turn_id,
                interaction_id,
                decision,
                ..
            } => ProviderCommand::ApprovalResponse {
                turn_id: *turn_id,
                interaction_id: interaction_id.clone(),
                decision: *decision,
            },
            Command::UserInputRespond {
                turn_id,
                interaction_id,
                answers,
                ..
            } => ProviderCommand::UserInputResponse {
                turn_id: *turn_id,
                interaction_id: interaction_id.clone(),
                answers: answers.clone(),
            },
            Command::LiveTurnInterrupt { turn_id, .. } => {
                ProviderCommand::Interrupt { turn_id: *turn_id }
            }
            _ => return Ok(None),
        };
        command.validate()?;
        Ok(Some(command))
    }

    fn apply_provider_events(&mut self, events: &[ProviderEvent]) -> AppResult<Vec<TurnChange>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }
        if events.len() > crate::live_turn::PROVIDER_EVENT_CAPACITY {
            return Err(format!(
                "provider event batch contains {} events; maximum is {}",
                events.len(),
                crate::live_turn::PROVIDER_EVENT_CAPACITY
            ));
        }
        for event in events {
            event.validate()?;
        }

        let now = now_ms();
        let tx = self.conn.transaction().map_err(err)?;
        let mut pending_output: Option<PendingOutput> = None;
        let mut changes = Vec::new();

        for event in events {
            let payload = serde_json::to_string(event).map_err(err)?;
            if let Some(existing) = tx
                .query_row(
                    "SELECT payload_json FROM provider_event_receipts
                     WHERE turn_id = ?1 AND provider_event_id = ?2",
                    params![event.turn_id().to_string(), event.provider_event_id()],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(err)?
            {
                if existing != payload {
                    return Err(format!(
                        "provider-event idempotency conflict for turn {} event {}",
                        event.turn_id(),
                        event.provider_event_id()
                    ));
                }
                continue;
            }

            if let ProviderEvent::AssistantOutput {
                turn_id,
                provider_event_id,
                delta,
                resume_cursor,
            } = event
            {
                let needs_flush = pending_output.as_ref().is_some_and(|pending| {
                    pending.turn_id != *turn_id
                        || pending.delta.len().saturating_add(delta.len()) > MAX_OUTPUT_DELTA_BYTES
                });
                if needs_flush
                    && let Some(change) = flush_pending_output(&tx, &mut pending_output, now)?
                {
                    changes.push(change);
                }
                insert_provider_event_receipt(&tx, event, &payload, now)?;
                let pending = pending_output.get_or_insert_with(|| PendingOutput {
                    turn_id: *turn_id,
                    provider_event_ids: Vec::new(),
                    delta: String::new(),
                    resume_cursor: None,
                });
                pending.provider_event_ids.push(provider_event_id.clone());
                pending.delta.push_str(delta);
                if let Some(cursor) = resume_cursor {
                    pending.resume_cursor = Some(cursor.clone());
                }
                continue;
            }

            if let Some(change) = flush_pending_output(&tx, &mut pending_output, now)? {
                changes.push(change);
            }
            insert_provider_event_receipt(&tx, event, &payload, now)?;
            let (change, sequence) = apply_provider_transition(&tx, event, now)?;
            tx.execute(
                "UPDATE provider_event_receipts SET applied_sequence = ?1
                 WHERE turn_id = ?2 AND provider_event_id = ?3",
                params![
                    sequence as i64,
                    event.turn_id().to_string(),
                    event.provider_event_id()
                ],
            )
            .map_err(err)?;
            changes.push(change);
        }

        if let Some(change) = flush_pending_output(&tx, &mut pending_output, now)? {
            changes.push(change);
        }
        tx.commit().map_err(err)?;
        Ok(changes)
    }

    fn turn_change(&self, turn_id: Uuid) -> AppResult<TurnChange> {
        let (thread_id, status): (String, String) = self
            .conn
            .query_row(
                "SELECT thread_id, status FROM turns WHERE turn_id = ?1",
                [turn_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(err)?
            .ok_or_else(|| format!("turn {turn_id} does not exist"))?;
        Ok(TurnChange {
            thread_id: Uuid::parse_str(&thread_id).map_err(err)?,
            status,
        })
    }

    fn reconcile_unfinished_turns(&mut self) -> AppResult<Vec<String>> {
        let turn_ids = {
            let mut statement = self
                .conn
                .prepare(
                    "SELECT turn_id FROM turns
                     WHERE status IN (
                        'accepted', 'starting', 'streaming', 'awaiting_approval',
                        'awaiting_user_input', 'interrupting'
                     ) ORDER BY accepted_at, turn_id",
                )
                .map_err(err)?;
            statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(err)?
        };
        let mut warnings = Vec::with_capacity(turn_ids.len());
        for value in turn_ids {
            let turn_id = Uuid::parse_str(&value).map_err(err)?;
            self.apply_provider_events(&[ProviderEvent::ProcessLost {
                turn_id,
                provider_event_id: format!("reconciliation:{turn_id}"),
                diagnostic: "Agent World restarted before the provider's terminal outcome was durably recorded; the turn was not replayed.".into(),
                side_effect_possible: true,
            }])?;
            warnings.push(format!(
                "turn {turn_id} has an unknown outcome and was not replayed"
            ));
        }
        Ok(warnings)
    }

    fn load_receipt(&self, command_id: Uuid, command_json: &str) -> AppResult<Option<Receipt>> {
        let stored = self
            .conn
            .query_row(
                "SELECT command_json, status, result_json, event_sequence
                 FROM command_receipts WHERE command_id = ?1",
                [command_id.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(err)?;
        let Some((stored_command, status, result_json, event_sequence)) = stored else {
            return Ok(None);
        };
        if stored_command != command_json {
            return Err(format!("idempotency conflict for command {}", command_id));
        }
        Ok(Some(Receipt {
            command_id,
            status,
            result: serde_json::from_str(&result_json).unwrap_or(Value::Null),
            event_sequence: event_sequence.map(|value| value as u64),
        }))
    }

    fn execute(&mut self, envelope: CommandEnvelope) -> AppResult<Receipt> {
        if envelope.protocol_version != PROTOCOL_VERSION {
            return Err(format!(
                "unsupported protocol version {}",
                envelope.protocol_version
            ));
        }
        let payload = serde_json::to_string(&envelope.command).map_err(err)?;
        if matches!(&envelope.command, Command::WorktreeCreate { .. }) {
            return self.execute_worktree(envelope, payload);
        }
        if let Some(receipt) = self.load_receipt(envelope.command_id, &payload)? {
            return Ok(receipt);
        }

        let aggregate_id = envelope.command.aggregate_id();
        let now = now_ms();
        let tx = self.conn.transaction().map_err(err)?;
        let current_version: u64 = tx
            .query_row(
                "SELECT version FROM aggregate_versions WHERE aggregate_id = ?1",
                [aggregate_id.to_string()],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(err)?
            .unwrap_or(0) as u64;

        if let Some(expected) = envelope.expected_aggregate_version
            && expected != current_version
        {
            return store_rejection(
                tx,
                &envelope,
                &payload,
                json!({"error":"aggregate version mismatch","expected":expected,"actual":current_version}),
                now,
            );
        }
        if let Err(error) = validate_command(&tx, &envelope.command, &self.provider_readiness) {
            return store_rejection(tx, &envelope, &payload, json!({"error":error}), now);
        }

        let next_version = current_version + 1;
        let event_type = event_type(&envelope.command);
        tx.execute(
            "INSERT INTO events
             (aggregate_id, aggregate_version, event_type, payload_json, occurred_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                aggregate_id.to_string(),
                next_version as i64,
                event_type,
                payload,
                now
            ],
        )
        .map_err(err)?;
        let sequence = tx.last_insert_rowid() as u64;
        apply_projection(&tx, &envelope.command, None, sequence, now)?;
        tx.execute(
            "INSERT INTO aggregate_versions (aggregate_id, version)
             VALUES (?1, ?2)
             ON CONFLICT(aggregate_id) DO UPDATE SET version = excluded.version",
            params![aggregate_id.to_string(), next_version as i64],
        )
        .map_err(err)?;

        let result = json!({"event_type":event_type,"sequence":sequence});
        let receipt = Receipt {
            command_id: envelope.command_id,
            status: "succeeded".into(),
            result: result.clone(),
            event_sequence: Some(sequence),
        };
        tx.execute(
            "INSERT INTO command_receipts
             (command_id, protocol_version, command_json, status, result_json, event_sequence, recorded_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                envelope.command_id.to_string(),
                envelope.protocol_version,
                payload,
                receipt.status,
                result.to_string(),
                sequence as i64,
                now
            ],
        )
        .map_err(err)?;
        tx.commit().map_err(err)?;
        Ok(receipt)
    }

    fn execute_worktree(
        &mut self,
        envelope: CommandEnvelope,
        payload: String,
    ) -> AppResult<Receipt> {
        let plan = match self.load_receipt(envelope.command_id, &payload)? {
            Some(receipt) if receipt.status != "accepted" => return Ok(receipt),
            Some(_) => self.load_worktree_plan(envelope.command_id)?,
            None => {
                let (worktree_id, thread_id) = match &envelope.command {
                    Command::WorktreeCreate {
                        worktree_id,
                        thread_id,
                    } => (*worktree_id, *thread_id),
                    _ => unreachable!(),
                };
                let current_version = connection_aggregate_version(&self.conn, thread_id)?;
                if let Some(expected) = envelope.expected_aggregate_version
                    && expected != current_version
                {
                    return self.reject_command(
                        &envelope,
                        &payload,
                        json!({"error":"aggregate version mismatch","expected":expected,"actual":current_version}),
                    );
                }
                if let Err(error) = self.validate_worktree_request(worktree_id, thread_id) {
                    return self.reject_command(&envelope, &payload, json!({"error":error}));
                }
                let plan = self.plan_worktree(worktree_id, thread_id)?;
                if let Some(rejected) = self.accept_worktree(&envelope, &payload, &plan)? {
                    return Ok(rejected);
                }
                plan
            }
        };

        if let Err(error) = self.validate_accepted_worktree_plan(&plan) {
            self.mark_worktree_indeterminate(&envelope, &payload, &error)?;
            return Err(error);
        }

        match create_or_reconcile_worktree(&plan) {
            Ok(()) => self.finalize_worktree(&envelope, &payload, &plan),
            Err(error) => {
                if error.indeterminate {
                    self.mark_worktree_indeterminate(&envelope, &payload, &error.message)?;
                }
                Err(error.message)
            }
        }
    }

    fn validate_accepted_worktree_plan(&self, plan: &WorktreePlan) -> AppResult<()> {
        let thread: Option<(String, Option<String>)> = self
            .conn
            .query_row(
                "SELECT project_id, worktree_id FROM threads WHERE thread_id = ?1",
                [plan.thread_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(err)?;
        let Some((thread_project_id, attached_worktree)) = thread else {
            return Err(format!(
                "thread {} for accepted worktree plan no longer exists",
                plan.thread_id
            ));
        };
        if thread_project_id != plan.project_id.to_string() {
            return Err(format!(
                "accepted worktree plan {} belongs to project {}, but thread {} belongs to project {}",
                plan.worktree_id, plan.project_id, plan.thread_id, thread_project_id
            ));
        }
        if let Some(attached_worktree) = attached_worktree
            && attached_worktree != plan.worktree_id.to_string()
        {
            return Err(format!(
                "thread {} is already attached to worktree {}; refusing accepted plan for {}",
                plan.thread_id, attached_worktree, plan.worktree_id
            ));
        }

        let projected: Option<(String, String, String)> = self
            .conn
            .query_row(
                "SELECT project_id, branch, path FROM worktrees WHERE worktree_id = ?1",
                [plan.worktree_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(err)?;
        if let Some((project_id, branch, path)) = projected
            && (project_id != plan.project_id.to_string()
                || branch != plan.branch
                || Path::new(&path) != plan.path.as_path())
        {
            return Err(format!(
                "projected worktree {} does not match its accepted durable plan",
                plan.worktree_id
            ));
        }

        let path_owner: Option<String> = self
            .conn
            .query_row(
                "SELECT worktree_id FROM worktrees WHERE path = ?1 AND worktree_id != ?2",
                params![plan.path.to_string_lossy(), plan.worktree_id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(err)?;
        if let Some(path_owner) = path_owner {
            return Err(format!(
                "worktree path {} is already owned by {path_owner}",
                plan.path.display()
            ));
        }
        Ok(())
    }

    fn reject_command(
        &mut self,
        envelope: &CommandEnvelope,
        payload: &str,
        result: Value,
    ) -> AppResult<Receipt> {
        let tx = self.conn.transaction().map_err(err)?;
        store_rejection(tx, envelope, payload, result, now_ms())
    }

    fn validate_worktree_request(&self, worktree_id: Uuid, thread_id: Uuid) -> AppResult<()> {
        let thread = self
            .conn
            .query_row(
                "SELECT worktree_id, state FROM threads WHERE thread_id = ?1",
                [thread_id.to_string()],
                |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(err)?;
        let Some((attached_worktree, state)) = thread else {
            return Err(format!("thread {thread_id} does not exist"));
        };
        if state == "archived" {
            return Err(format!("thread {thread_id} is archived"));
        }
        if let Some(attached_worktree) = attached_worktree {
            return Err(format!(
                "thread {thread_id} already has worktree {attached_worktree}"
            ));
        }

        let unresolved_plan: Option<(String, String)> = self
            .conn
            .query_row(
                "SELECT wp.command_id, wp.worktree_id
                 FROM worktree_plans wp
                 JOIN command_receipts cr ON cr.command_id = wp.command_id
                 WHERE wp.thread_id = ?1 AND cr.status = 'accepted'
                 ORDER BY cr.recorded_at, wp.command_id
                 LIMIT 1",
                [thread_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(err)?;
        if let Some((command_id, planned_worktree_id)) = unresolved_plan {
            return Err(format!(
                "thread {thread_id} already has unresolved accepted worktree command {command_id} for worktree {planned_worktree_id}"
            ));
        }

        let projected_owner: Option<String> = self
            .conn
            .query_row(
                "SELECT COALESCE(t.thread_id, w.project_id)
                 FROM worktrees w
                 LEFT JOIN threads t ON t.worktree_id = w.worktree_id
                 WHERE w.worktree_id = ?1",
                [worktree_id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(err)?;
        if let Some(owner) = projected_owner {
            return Err(format!(
                "worktree id {worktree_id} is already owned by {owner}"
            ));
        }
        let planned_owner: Option<String> = self
            .conn
            .query_row(
                "SELECT thread_id FROM worktree_plans WHERE worktree_id = ?1 LIMIT 1",
                [worktree_id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(err)?;
        if let Some(owner) = planned_owner {
            return Err(format!(
                "worktree id {worktree_id} is already planned for thread {owner}"
            ));
        }
        Ok(())
    }

    fn plan_worktree(&self, worktree_id: Uuid, thread_id: Uuid) -> AppResult<WorktreePlan> {
        let (project_id, repo_path, label): (String, String, String) = self
            .conn
            .query_row(
                "SELECT t.project_id, p.repo_path, t.label
                 FROM threads t JOIN projects p ON p.project_id = t.project_id
                 WHERE t.thread_id = ?1",
                [thread_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(err)?;
        let repo = git_toplevel(Path::new(&repo_path))?;
        let repo_common_dir = git_common_dir(&repo)?;
        let commit_oid = git_commit(&repo, "HEAD")?;
        let slug = slugify(&label);
        let short_id = &thread_id.simple().to_string()[..8];
        let branch = format!("agent-world/{slug}-{short_id}");
        let repo_name = repo
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("repo");
        let worktree_root = self.runtime_root.join("worktrees");
        fs::create_dir_all(worktree_root.join(repo_name)).map_err(err)?;
        let worktree_root = portable_windows_path(worktree_root.canonicalize().map_err(err)?);
        let destination = worktree_root
            .join(repo_name)
            .join(format!("{slug}-{short_id}"));
        if !destination.starts_with(&worktree_root) {
            return Err("worktree destination escaped the runtime root".into());
        }
        Ok(WorktreePlan {
            worktree_id,
            thread_id,
            project_id: Uuid::parse_str(&project_id).map_err(err)?,
            repo,
            repo_common_dir,
            branch,
            path: destination,
            commit_oid,
        })
    }

    fn accept_worktree(
        &mut self,
        envelope: &CommandEnvelope,
        payload: &str,
        plan: &WorktreePlan,
    ) -> AppResult<Option<Receipt>> {
        let aggregate_id = envelope.command.aggregate_id();
        let now = now_ms();
        let tx = self.conn.transaction().map_err(err)?;
        let current_version = aggregate_version(&tx, aggregate_id)?;
        if let Some(expected) = envelope.expected_aggregate_version
            && expected != current_version
        {
            return store_rejection(
                tx,
                envelope,
                payload,
                json!({"error":"aggregate version mismatch","expected":expected,"actual":current_version}),
                now,
            )
            .map(Some);
        }

        let next_version = current_version + 1;
        tx.execute(
            "INSERT INTO events
             (aggregate_id, aggregate_version, event_type, payload_json, occurred_at)
             VALUES (?1, ?2, 'command.accepted', ?3, ?4)",
            params![aggregate_id.to_string(), next_version as i64, payload, now],
        )
        .map_err(err)?;
        let sequence = tx.last_insert_rowid() as u64;
        tx.execute(
            "INSERT INTO aggregate_versions (aggregate_id, version)
             VALUES (?1, ?2)
             ON CONFLICT(aggregate_id) DO UPDATE SET version = excluded.version",
            params![aggregate_id.to_string(), next_version as i64],
        )
        .map_err(err)?;
        let result = json!({
            "phase": "accepted",
            "path": plan.path,
            "branch": plan.branch,
            "commit_oid": plan.commit_oid
        });
        tx.execute(
            "INSERT INTO command_receipts
             (command_id, protocol_version, command_json, status, result_json, event_sequence, recorded_at)
             VALUES (?1, ?2, ?3, 'accepted', ?4, ?5, ?6)",
            params![
                envelope.command_id.to_string(),
                envelope.protocol_version,
                payload,
                result.to_string(),
                sequence as i64,
                now
            ],
        )
        .map_err(err)?;
        tx.execute(
            "INSERT INTO worktree_plans
             (command_id, worktree_id, thread_id, project_id, repo_path, repo_common_dir,
              branch, path, commit_oid)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                envelope.command_id.to_string(),
                plan.worktree_id.to_string(),
                plan.thread_id.to_string(),
                plan.project_id.to_string(),
                plan.repo.to_string_lossy(),
                plan.repo_common_dir.to_string_lossy(),
                plan.branch,
                plan.path.to_string_lossy(),
                plan.commit_oid
            ],
        )
        .map_err(err)?;
        tx.commit().map_err(err)?;
        Ok(None)
    }

    fn load_worktree_plan(&self, command_id: Uuid) -> AppResult<WorktreePlan> {
        self.conn
            .query_row(
                "SELECT worktree_id, thread_id, project_id, repo_path, repo_common_dir,
                        branch, path, commit_oid
                 FROM worktree_plans WHERE command_id = ?1",
                [command_id.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                    ))
                },
            )
            .map_err(err)
            .and_then(
                |(
                    worktree_id,
                    thread_id,
                    project_id,
                    repo,
                    repo_common_dir,
                    branch,
                    path,
                    commit_oid,
                )| {
                    Ok(WorktreePlan {
                        worktree_id: Uuid::parse_str(&worktree_id).map_err(err)?,
                        thread_id: Uuid::parse_str(&thread_id).map_err(err)?,
                        project_id: Uuid::parse_str(&project_id).map_err(err)?,
                        repo: PathBuf::from(repo),
                        repo_common_dir: PathBuf::from(repo_common_dir),
                        branch,
                        path: PathBuf::from(path),
                        commit_oid,
                    })
                },
            )
    }

    fn finalize_worktree(
        &mut self,
        envelope: &CommandEnvelope,
        payload: &str,
        plan: &WorktreePlan,
    ) -> AppResult<Receipt> {
        if let Some(receipt) = self.load_receipt(envelope.command_id, payload)?
            && receipt.status != "accepted"
        {
            return Ok(receipt);
        }
        let aggregate_id = envelope.command.aggregate_id();
        let now = now_ms();
        let tx = self.conn.transaction().map_err(err)?;
        let next_version = aggregate_version(&tx, aggregate_id)? + 1;
        tx.execute(
            "INSERT INTO events
             (aggregate_id, aggregate_version, event_type, payload_json, occurred_at)
             VALUES (?1, ?2, 'worktree.created', ?3, ?4)",
            params![aggregate_id.to_string(), next_version as i64, payload, now],
        )
        .map_err(err)?;
        let sequence = tx.last_insert_rowid() as u64;
        apply_projection(
            &tx,
            &envelope.command,
            Some(&WorktreeReady::from(plan)),
            sequence,
            now,
        )?;
        tx.execute(
            "UPDATE aggregate_versions SET version = ?1 WHERE aggregate_id = ?2",
            params![next_version as i64, aggregate_id.to_string()],
        )
        .map_err(err)?;
        let result = json!({
            "event_type": "worktree.created",
            "sequence": sequence,
            "commit_oid": plan.commit_oid
        });
        tx.execute(
            "UPDATE command_receipts
             SET status = 'succeeded', result_json = ?1, event_sequence = ?2, recorded_at = ?3
             WHERE command_id = ?4 AND status = 'accepted'",
            params![
                result.to_string(),
                sequence as i64,
                now,
                envelope.command_id.to_string()
            ],
        )
        .map_err(err)?;
        tx.commit().map_err(err)?;
        Ok(Receipt {
            command_id: envelope.command_id,
            status: "succeeded".into(),
            result,
            event_sequence: Some(sequence),
        })
    }

    fn mark_worktree_indeterminate(
        &mut self,
        envelope: &CommandEnvelope,
        payload: &str,
        message: &str,
    ) -> AppResult<()> {
        let aggregate_id = envelope.command.aggregate_id();
        let now = now_ms();
        let tx = self.conn.transaction().map_err(err)?;
        let next_version = aggregate_version(&tx, aggregate_id)? + 1;
        tx.execute(
            "INSERT INTO events
             (aggregate_id, aggregate_version, event_type, payload_json, occurred_at)
             VALUES (?1, ?2, 'command.indeterminate', ?3, ?4)",
            params![
                aggregate_id.to_string(),
                next_version as i64,
                json!({"command":payload,"error":message}).to_string(),
                now
            ],
        )
        .map_err(err)?;
        let sequence = tx.last_insert_rowid();
        tx.execute(
            "UPDATE aggregate_versions SET version = ?1 WHERE aggregate_id = ?2",
            params![next_version as i64, aggregate_id.to_string()],
        )
        .map_err(err)?;
        tx.execute(
            "UPDATE command_receipts
             SET status = 'indeterminate', result_json = ?1, event_sequence = ?2, recorded_at = ?3
             WHERE command_id = ?4 AND status = 'accepted'",
            params![
                json!({"error":message}).to_string(),
                sequence,
                now,
                envelope.command_id.to_string()
            ],
        )
        .map_err(err)?;
        tx.commit().map_err(err)
    }

    fn recover_accepted_worktrees(&mut self) -> AppResult<Vec<String>> {
        let commands = {
            let mut statement = self
                .conn
                .prepare(
                    "SELECT command_id, protocol_version, command_json
                     FROM command_receipts
                     WHERE status = 'accepted'
                       AND command_id IN (SELECT command_id FROM worktree_plans)
                     ORDER BY recorded_at",
                )
                .map_err(err)?;
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, u16>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .map_err(err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(err)?
        };
        let mut warnings = Vec::new();
        for (command_id, _recorded_protocol_version, command_json) in commands {
            let recovery = (|| {
                self.execute(CommandEnvelope {
                    protocol_version: PROTOCOL_VERSION,
                    command_id: Uuid::parse_str(&command_id).map_err(err)?,
                    expected_aggregate_version: None,
                    command: serde_json::from_str(&command_json).map_err(err)?,
                })
            })();
            if let Err(error) = recovery {
                warnings.push(format!("command {command_id}: {error}"));
            }
        }
        Ok(warnings)
    }

    fn bootstrap_snapshot(&self) -> AppResult<BootstrapSnapshot> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT t.thread_id, t.project_id, t.worktree_id, t.provider,
                        t.label, t.state, t.attention, t.unread_count,
                        t.last_event_sequence,
                        CASE WHEN w.status = 'ready' THEN w.path END
                 FROM threads t
                 LEFT JOIN worktrees w ON w.worktree_id = t.worktree_id
                 WHERE t.state != 'archived'
                 ORDER BY t.label",
            )
            .map_err(err)?;
        let mut actors = statement
            .query_map([], |row| {
                let provider: String = row.get(3)?;
                let state: String = row.get(5)?;
                Ok(ThreadActorSnapshot {
                    thread_id: Uuid::parse_str(&row.get::<_, String>(0)?).map_err(to_sql_error)?,
                    project_id: Uuid::parse_str(&row.get::<_, String>(1)?).map_err(to_sql_error)?,
                    worktree_id: row
                        .get::<_, Option<String>>(2)?
                        .map(|value| Uuid::parse_str(&value).map_err(to_sql_error))
                        .transpose()?,
                    provider: if provider == "claude" {
                        Provider::Claude
                    } else {
                        Provider::Codex
                    },
                    label: row.get(4)?,
                    state: ActorState::parse(&state),
                    attention: row.get::<_, i64>(6)? != 0,
                    unread_count: row.get::<_, i64>(7)? as u32,
                    last_event_sequence: row.get::<_, i64>(8)? as u64,
                    worktree_path: row.get::<_, Option<String>>(9)?.map(PathBuf::from),
                    start_gate: LiveTurnStartGate::Eligible,
                    live_turn: None,
                })
            })
            .map_err(err)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(err)?;
        drop(statement);
        let globally_active_thread: Option<String> = self
            .conn
            .query_row(
                "SELECT thread_id FROM turns
                 WHERE status IN (
                    'accepted', 'starting', 'streaming', 'awaiting_approval',
                    'awaiting_user_input', 'interrupting'
                 ) LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(err)?;
        for actor in &mut actors {
            actor.live_turn = self.load_live_turn_snapshot(actor.thread_id)?;
            actor.start_gate = if actor.worktree_path.is_none() {
                LiveTurnStartGate::NoWorktree
            } else if actor
                .live_turn
                .as_ref()
                .is_some_and(|turn| turn.state.is_active())
            {
                LiveTurnStartGate::PendingTurn
            } else if actor
                .live_turn
                .as_ref()
                .is_some_and(|turn| turn.recovery == RecoveryDisposition::Indeterminate)
            {
                LiveTurnStartGate::RecoveryError
            } else if actor.provider == Provider::Claude
                || matches!(
                    self.provider_readiness,
                    ProviderReadiness::Unavailable { .. }
                )
            {
                LiveTurnStartGate::ProviderUnavailable
            } else if matches!(
                self.provider_readiness,
                ProviderReadiness::UnsupportedVersion { .. }
            ) {
                LiveTurnStartGate::UnsupportedVersion
            } else if globally_active_thread
                .as_deref()
                .is_some_and(|thread_id| thread_id != actor.thread_id.to_string())
            {
                LiveTurnStartGate::QueuePressure
            } else {
                LiveTurnStartGate::Eligible
            };
        }
        let last_sequence = self
            .conn
            .query_row("SELECT COALESCE(MAX(sequence), 0) FROM events", [], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(err)? as u64;
        Ok(BootstrapSnapshot {
            actors,
            last_sequence,
        })
    }

    fn load_live_turn_snapshot(&self, thread_id: Uuid) -> AppResult<Option<LiveTurnSnapshot>> {
        let turn = self
            .conn
            .query_row(
                "SELECT turn_id, status, provider_session_id, resume_cursor
                 FROM turns WHERE thread_id = ?1
                 ORDER BY accepted_at DESC, turn_id DESC LIMIT 1",
                [thread_id.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(err)?;
        let Some((turn_id, status, session_id, resume_cursor)) = turn else {
            return Ok(None);
        };
        let turn_id = Uuid::parse_str(&turn_id).map_err(err)?;
        let state = LiveTurnState::parse(&status)?;
        let session = match (session_id, resume_cursor) {
            (Some(session_id), Some(resume_cursor)) => Some(ProviderSessionCursor {
                session_id,
                resume_cursor,
            }),
            (None, None) | (Some(_), None) => None,
            (None, Some(_)) => {
                return Err(format!(
                    "turn {turn_id} has a resume cursor without a provider session"
                ));
            }
        };
        let interaction = self.load_interaction_snapshot(turn_id)?;
        let was_resumed: bool = self
            .conn
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM provider_event_receipts
                    WHERE turn_id = ?1 AND event_type = 'resumed'
                )",
                [turn_id.to_string()],
                |row| row.get(0),
            )
            .map_err(err)?;
        let terminal_state = state.is_terminal();
        let recovery = match state {
            LiveTurnState::Completed => RecoveryDisposition::Completed,
            LiveTurnState::Failed => RecoveryDisposition::Failed,
            LiveTurnState::Indeterminate => RecoveryDisposition::Indeterminate,
            _ if !terminal_state && was_resumed => RecoveryDisposition::Resumed,
            _ => RecoveryDisposition::None,
        };
        Ok(Some(LiveTurnSnapshot {
            turn_id,
            state,
            session,
            interruptible: matches!(
                state,
                LiveTurnState::Starting
                    | LiveTurnState::Streaming
                    | LiveTurnState::AwaitingApproval
                    | LiveTurnState::AwaitingUserInput
            ),
            interaction,
            recovery,
        }))
    }

    fn load_interaction_snapshot(&self, turn_id: Uuid) -> AppResult<Option<InteractionSnapshot>> {
        let interaction = self
            .conn
            .query_row(
                "SELECT interaction_id, kind, status, request_json
                 FROM turn_interactions WHERE turn_id = ?1
                 ORDER BY requested_sequence DESC LIMIT 1",
                [turn_id.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(err)?;
        let Some((interaction_id, kind, status, request_json)) = interaction else {
            return Ok(None);
        };
        let request: ProviderEvent = serde_json::from_str(&request_json).map_err(err)?;
        let interaction_status = match status.as_str() {
            "pending" => InteractionStatus::Pending,
            "responded" => InteractionStatus::Responded,
            "stale" => InteractionStatus::Stale,
            other => {
                return Err(format!(
                    "interaction {interaction_id} has invalid status {other}"
                ));
            }
        };
        let snapshot = match request {
            ProviderEvent::ApprovalRequested {
                prompt,
                operation,
                path,
                command,
                consequence,
                ..
            } => InteractionSnapshot {
                interaction_id: interaction_id.clone(),
                kind: InteractionKind::Approval,
                prompt,
                operation,
                path,
                command,
                consequence,
                questions: Vec::new(),
                status: interaction_status,
            },
            ProviderEvent::UserInputRequested {
                prompt, questions, ..
            } => InteractionSnapshot {
                interaction_id: interaction_id.clone(),
                kind: InteractionKind::UserInput,
                prompt,
                operation: None,
                path: None,
                command: None,
                consequence: None,
                questions,
                status: interaction_status,
            },
            _ => {
                return Err(format!(
                    "interaction projection for turn {turn_id} contains a non-interaction event"
                ));
            }
        };
        let expected_kind = match snapshot.kind {
            InteractionKind::Approval => "approval",
            InteractionKind::UserInput => "user_input",
        };
        if kind != expected_kind {
            return Err(format!(
                "interaction {interaction_id} kind {kind} does not match its request"
            ));
        }
        Ok(Some(snapshot))
    }

    fn timeline_page(&self, thread_id: Uuid, limit: usize) -> AppResult<Vec<TimelineMessage>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT sequence, role, body, occurred_at, event_type, payload_json FROM (
                    SELECT e.sequence, COALESCE(m.role, 'system') AS role, m.body,
                           e.occurred_at, e.event_type, e.payload_json
                    FROM events e
                    LEFT JOIN messages m ON m.sequence = e.sequence
                    WHERE e.aggregate_id = ?1 AND (
                        m.sequence IS NOT NULL OR e.event_type IN (
                            'turn.approval_requested', 'turn.user_input_requested',
                            'turn.approval_responded', 'turn.user_input_responded',
                            'turn.interrupt_requested', 'turn.resumed'
                        )
                    )
                    ORDER BY e.sequence DESC LIMIT ?2
                 ) ORDER BY sequence ASC",
            )
            .map_err(err)?;
        statement
            .query_map(params![thread_id.to_string(), limit as i64], |row| {
                let role: String = row.get(1)?;
                let stored_body: Option<String> = row.get(2)?;
                let event_type: String = row.get(4)?;
                let payload_json: String = row.get(5)?;
                let metadata: Value = serde_json::from_str(&payload_json).unwrap_or(Value::Null);
                let kind = match event_type.as_str() {
                    "turn.approval_requested" => TimelineRecordKind::ApprovalRequest,
                    "turn.user_input_requested" => TimelineRecordKind::UserInputRequest,
                    _ if stored_body.is_none() => TimelineRecordKind::Status,
                    _ if role == "user" => TimelineRecordKind::User,
                    _ if role == "assistant" => TimelineRecordKind::Assistant,
                    _ => TimelineRecordKind::System,
                };
                let body = stored_body.unwrap_or_else(|| {
                    metadata
                        .get("prompt")
                        .and_then(Value::as_str)
                        .unwrap_or(&event_type)
                        .to_owned()
                });
                let turn_id = metadata
                    .get("turn_id")
                    .and_then(Value::as_str)
                    .and_then(|value| Uuid::parse_str(value).ok());
                Ok(TimelineMessage {
                    sequence: row.get::<_, i64>(0)? as u64,
                    role,
                    body,
                    occurred_at: row.get(3)?,
                    kind,
                    turn_id,
                    event_type,
                    metadata,
                })
            })
            .map_err(err)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(err)
    }
}

#[derive(Debug)]
struct TurnChange {
    thread_id: Uuid,
    status: String,
}

struct DurableTurn {
    thread_id: Uuid,
    provider_session_id: Option<String>,
    status: String,
    assistant_message_sequence: Option<u64>,
    output_bytes: usize,
}

fn load_durable_turn(tx: &Transaction<'_>, turn_id: Uuid) -> AppResult<DurableTurn> {
    tx.query_row(
        "SELECT thread_id, provider_session_id, status,
                assistant_message_sequence, output_bytes
         FROM turns WHERE turn_id = ?1",
        [turn_id.to_string()],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, i64>(4)?,
            ))
        },
    )
    .optional()
    .map_err(err)?
    .ok_or_else(|| format!("turn {turn_id} does not exist"))
    .and_then(
        |(thread_id, provider_session_id, status, assistant_message_sequence, output_bytes)| {
            Ok(DurableTurn {
                thread_id: Uuid::parse_str(&thread_id).map_err(err)?,
                provider_session_id,
                status,
                assistant_message_sequence: assistant_message_sequence.map(|value| value as u64),
                output_bytes: usize::try_from(output_bytes).map_err(err)?,
            })
        },
    )
}

struct PendingOutput {
    turn_id: Uuid,
    provider_event_ids: Vec<String>,
    delta: String,
    resume_cursor: Option<String>,
}

fn insert_provider_event_receipt(
    tx: &Transaction<'_>,
    event: &ProviderEvent,
    payload: &str,
    now: i64,
) -> AppResult<()> {
    tx.execute(
        "INSERT INTO provider_event_receipts
         (turn_id, provider_event_id, event_type, payload_json, recorded_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            event.turn_id().to_string(),
            event.provider_event_id(),
            provider_event_type(event),
            payload,
            now
        ],
    )
    .map_err(err)?;
    Ok(())
}

fn provider_event_type(event: &ProviderEvent) -> &'static str {
    match event {
        ProviderEvent::Starting { .. } => "starting",
        ProviderEvent::SessionEstablished { .. } => "session_established",
        ProviderEvent::Resumed { .. } => "resumed",
        ProviderEvent::AssistantOutput { .. } => "assistant_output",
        ProviderEvent::ApprovalRequested { .. } => "approval_requested",
        ProviderEvent::UserInputRequested { .. } => "user_input_requested",
        ProviderEvent::InterruptAcknowledged { .. } => "interrupt_acknowledged",
        ProviderEvent::Completed { .. } => "completed",
        ProviderEvent::Failed { .. } => "failed",
        ProviderEvent::ProcessLost { .. } => "process_lost",
    }
}

fn flush_pending_output(
    tx: &Transaction<'_>,
    pending: &mut Option<PendingOutput>,
    now: i64,
) -> AppResult<Option<TurnChange>> {
    let Some(pending) = pending.take() else {
        return Ok(None);
    };
    let turn = load_durable_turn(tx, pending.turn_id)?;
    if turn.status != LiveTurnState::Streaming.as_str() {
        return Err(format!(
            "turn {} cannot accept assistant output while {}",
            pending.turn_id, turn.status
        ));
    }
    let output_bytes = turn.output_bytes.saturating_add(pending.delta.len());
    if output_bytes > MAX_ASSISTANT_BYTES {
        return Err(format!(
            "turn {} assistant output exceeds {MAX_ASSISTANT_BYTES} bytes",
            pending.turn_id
        ));
    }
    let sequence = append_turn_event(
        tx,
        turn.thread_id,
        "turn.output_coalesced",
        json!({
            "turn_id": pending.turn_id,
            "provider_event_ids": &pending.provider_event_ids,
            "delta": &pending.delta,
            "resume_cursor": &pending.resume_cursor,
        }),
        now,
    )?;
    let assistant_message_sequence = match turn.assistant_message_sequence {
        Some(message_sequence) => {
            tx.execute(
                "UPDATE messages SET body = body || ?1, occurred_at = ?2 WHERE sequence = ?3",
                params![pending.delta, now, message_sequence as i64],
            )
            .map_err(err)?;
            message_sequence
        }
        None => {
            tx.execute(
                "INSERT INTO messages (sequence, thread_id, role, body, occurred_at)
                 VALUES (?1, ?2, 'assistant', ?3, ?4)",
                params![
                    sequence as i64,
                    turn.thread_id.to_string(),
                    pending.delta,
                    now
                ],
            )
            .map_err(err)?;
            sequence
        }
    };
    tx.execute(
        "UPDATE turns
         SET assistant_message_sequence = ?1, output_bytes = ?2,
             resume_cursor = COALESCE(?3, resume_cursor)
         WHERE turn_id = ?4",
        params![
            assistant_message_sequence as i64,
            output_bytes as i64,
            pending.resume_cursor,
            pending.turn_id.to_string()
        ],
    )
    .map_err(err)?;
    tx.execute(
        "UPDATE threads SET state = 'running', last_event_sequence = ?1 WHERE thread_id = ?2",
        params![sequence as i64, turn.thread_id.to_string()],
    )
    .map_err(err)?;
    for provider_event_id in &pending.provider_event_ids {
        tx.execute(
            "UPDATE provider_event_receipts SET applied_sequence = ?1
             WHERE turn_id = ?2 AND provider_event_id = ?3",
            params![
                sequence as i64,
                pending.turn_id.to_string(),
                provider_event_id
            ],
        )
        .map_err(err)?;
    }
    Ok(Some(TurnChange {
        thread_id: turn.thread_id,
        status: LiveTurnState::Streaming.as_str().into(),
    }))
}

fn apply_provider_transition(
    tx: &Transaction<'_>,
    event: &ProviderEvent,
    now: i64,
) -> AppResult<(TurnChange, u64)> {
    let turn_id = event.turn_id();
    let turn = load_durable_turn(tx, turn_id)?;
    let payload = serde_json::to_value(event).map_err(err)?;
    let (status, sequence) = match event {
        ProviderEvent::Starting { .. } => {
            require_live_turn_state(&turn, turn_id, &[LiveTurnState::Accepted])?;
            let sequence = append_turn_event(tx, turn.thread_id, "turn.starting", payload, now)?;
            tx.execute(
                "UPDATE turns
                 SET status = 'starting', started_sequence = ?1, started_at = ?2
                 WHERE turn_id = ?3",
                params![sequence as i64, now, turn_id.to_string()],
            )
            .map_err(err)?;
            update_thread_for_turn(tx, turn.thread_id, "starting", false, sequence)?;
            (LiveTurnState::Starting, sequence)
        }
        ProviderEvent::SessionEstablished { session, .. }
        | ProviderEvent::Resumed { session, .. } => {
            require_live_turn_state(&turn, turn_id, &[LiveTurnState::Starting])?;
            if let Some(existing) = turn.provider_session_id.as_deref()
                && existing != session.session_id
            {
                return Err(format!(
                    "turn {turn_id} changed provider session from {existing} to {}",
                    session.session_id
                ));
            }
            let event_type = if matches!(event, ProviderEvent::Resumed { .. }) {
                "turn.resumed"
            } else {
                "turn.session_established"
            };
            let sequence = append_turn_event(tx, turn.thread_id, event_type, payload, now)?;
            tx.execute(
                "UPDATE turns
                 SET status = 'streaming', provider_session_id = ?1, resume_cursor = ?2
                 WHERE turn_id = ?3",
                params![
                    session.session_id,
                    session.resume_cursor,
                    turn_id.to_string()
                ],
            )
            .map_err(err)?;
            update_thread_for_turn(tx, turn.thread_id, "running", false, sequence)?;
            (LiveTurnState::Streaming, sequence)
        }
        ProviderEvent::ApprovalRequested { interaction_id, .. } => {
            require_live_turn_state(&turn, turn_id, &[LiveTurnState::Streaming])?;
            let sequence = append_turn_event(
                tx,
                turn.thread_id,
                "turn.approval_requested",
                payload.clone(),
                now,
            )?;
            insert_interaction(
                tx,
                turn_id,
                interaction_id,
                "approval",
                &payload,
                sequence,
                now,
            )?;
            tx.execute(
                "UPDATE turns SET status = 'awaiting_approval' WHERE turn_id = ?1",
                [turn_id.to_string()],
            )
            .map_err(err)?;
            update_thread_for_turn(tx, turn.thread_id, "awaiting_approval", true, sequence)?;
            (LiveTurnState::AwaitingApproval, sequence)
        }
        ProviderEvent::UserInputRequested { interaction_id, .. } => {
            require_live_turn_state(&turn, turn_id, &[LiveTurnState::Streaming])?;
            let sequence = append_turn_event(
                tx,
                turn.thread_id,
                "turn.user_input_requested",
                payload.clone(),
                now,
            )?;
            insert_interaction(
                tx,
                turn_id,
                interaction_id,
                "user_input",
                &payload,
                sequence,
                now,
            )?;
            tx.execute(
                "UPDATE turns SET status = 'awaiting_user_input' WHERE turn_id = ?1",
                [turn_id.to_string()],
            )
            .map_err(err)?;
            update_thread_for_turn(tx, turn.thread_id, "waiting_user", true, sequence)?;
            (LiveTurnState::AwaitingUserInput, sequence)
        }
        ProviderEvent::InterruptAcknowledged { diagnostic, .. } => {
            require_live_turn_state(&turn, turn_id, &[LiveTurnState::Interrupting])?;
            let message = diagnostic
                .as_deref()
                .unwrap_or("Provider acknowledged the live-turn interrupt.");
            let sequence = terminal_turn(
                tx,
                &turn,
                turn_id,
                TerminalTransition {
                    state: LiveTurnState::Failed,
                    event_type: "turn.interrupted",
                    payload,
                    diagnostic: message,
                    actor_state: "stopped",
                },
                now,
            )?;
            (LiveTurnState::Failed, sequence)
        }
        ProviderEvent::Completed { session, .. } => {
            require_live_turn_state(&turn, turn_id, &[LiveTurnState::Streaming])?;
            if let Some(existing) = turn.provider_session_id.as_deref()
                && existing != session.session_id
            {
                return Err(format!(
                    "turn {turn_id} completed in provider session {}, expected {existing}",
                    session.session_id
                ));
            }
            let sequence = append_turn_event(tx, turn.thread_id, "turn.completed", payload, now)?;
            tx.execute(
                "UPDATE turns
                 SET status = 'completed', provider_session_id = ?1, resume_cursor = ?2,
                     terminal_sequence = ?3, finished_at = ?4
                 WHERE turn_id = ?5",
                params![
                    session.session_id,
                    session.resume_cursor,
                    sequence as i64,
                    now,
                    turn_id.to_string()
                ],
            )
            .map_err(err)?;
            stale_pending_interactions(tx, turn_id, sequence, now)?;
            update_thread_terminal(tx, turn.thread_id, "waiting_user", true, sequence)?;
            (LiveTurnState::Completed, sequence)
        }
        ProviderEvent::Failed { diagnostic, .. } => {
            require_active_turn(&turn, turn_id)?;
            let sequence = terminal_turn(
                tx,
                &turn,
                turn_id,
                TerminalTransition {
                    state: LiveTurnState::Failed,
                    event_type: "turn.failed",
                    payload,
                    diagnostic,
                    actor_state: "failed",
                },
                now,
            )?;
            (LiveTurnState::Failed, sequence)
        }
        ProviderEvent::ProcessLost {
            diagnostic,
            side_effect_possible,
            ..
        } => {
            require_active_turn(&turn, turn_id)?;
            let state = if *side_effect_possible {
                LiveTurnState::Indeterminate
            } else {
                LiveTurnState::Failed
            };
            let actor_state = if state == LiveTurnState::Indeterminate {
                "indeterminate"
            } else {
                "failed"
            };
            let sequence = terminal_turn(
                tx,
                &turn,
                turn_id,
                TerminalTransition {
                    state,
                    event_type: if state == LiveTurnState::Indeterminate {
                        "turn.indeterminate"
                    } else {
                        "turn.failed_before_start"
                    },
                    payload,
                    diagnostic,
                    actor_state,
                },
                now,
            )?;
            (state, sequence)
        }
        ProviderEvent::AssistantOutput { .. } => {
            return Err("assistant output must pass through the coalescing path".into());
        }
    };
    Ok((
        TurnChange {
            thread_id: turn.thread_id,
            status: status.as_str().into(),
        },
        sequence,
    ))
}

fn require_live_turn_state(
    turn: &DurableTurn,
    turn_id: Uuid,
    allowed: &[LiveTurnState],
) -> AppResult<()> {
    if allowed.iter().any(|state| state.as_str() == turn.status) {
        Ok(())
    } else {
        Err(format!(
            "turn {turn_id} cannot apply provider transition while {}",
            turn.status
        ))
    }
}

fn require_active_turn(turn: &DurableTurn, turn_id: Uuid) -> AppResult<()> {
    let state = LiveTurnState::parse(&turn.status)?;
    if state.is_active() {
        Ok(())
    } else {
        Err(format!(
            "turn {turn_id} cannot apply a terminal provider transition while {}",
            turn.status
        ))
    }
}

fn update_thread_for_turn(
    tx: &Transaction<'_>,
    thread_id: Uuid,
    state: &str,
    attention: bool,
    sequence: u64,
) -> AppResult<()> {
    tx.execute(
        "UPDATE threads
         SET state = ?1, attention = ?2, last_event_sequence = ?3
         WHERE thread_id = ?4",
        params![
            state,
            i64::from(attention),
            sequence as i64,
            thread_id.to_string()
        ],
    )
    .map_err(err)?;
    Ok(())
}

fn update_thread_terminal(
    tx: &Transaction<'_>,
    thread_id: Uuid,
    state: &str,
    attention: bool,
    sequence: u64,
) -> AppResult<()> {
    tx.execute(
        "UPDATE threads
         SET state = ?1, attention = ?2, unread_count = unread_count + 1,
             last_event_sequence = ?3
         WHERE thread_id = ?4",
        params![
            state,
            i64::from(attention),
            sequence as i64,
            thread_id.to_string()
        ],
    )
    .map_err(err)?;
    Ok(())
}

fn insert_interaction(
    tx: &Transaction<'_>,
    turn_id: Uuid,
    interaction_id: &str,
    kind: &str,
    request: &Value,
    sequence: u64,
    now: i64,
) -> AppResult<()> {
    let request = serde_json::to_string(request).map_err(err)?;
    tx.execute(
        "INSERT INTO turn_interactions
         (interaction_id, turn_id, kind, status, request_json, requested_sequence, requested_at)
         VALUES (?1, ?2, ?3, 'pending', ?4, ?5, ?6)",
        params![
            interaction_id,
            turn_id.to_string(),
            kind,
            request,
            sequence as i64,
            now
        ],
    )
    .map_err(|error| {
        format!("could not persist {kind} interaction {interaction_id} for turn {turn_id}: {error}")
    })?;
    Ok(())
}

struct TerminalTransition<'a> {
    state: LiveTurnState,
    event_type: &'a str,
    payload: Value,
    diagnostic: &'a str,
    actor_state: &'a str,
}

fn terminal_turn(
    tx: &Transaction<'_>,
    turn: &DurableTurn,
    turn_id: Uuid,
    transition: TerminalTransition<'_>,
    now: i64,
) -> AppResult<u64> {
    let diagnostic = terminal_diagnostic(transition.diagnostic);
    let sequence = append_turn_event(
        tx,
        turn.thread_id,
        transition.event_type,
        transition.payload,
        now,
    )?;
    tx.execute(
        "INSERT INTO messages (sequence, thread_id, role, body, occurred_at)
         VALUES (?1, ?2, 'system', ?3, ?4)",
        params![sequence as i64, turn.thread_id.to_string(), diagnostic, now],
    )
    .map_err(err)?;
    tx.execute(
        "UPDATE turns
         SET status = ?1, terminal_sequence = ?2, error = ?3, finished_at = ?4
         WHERE turn_id = ?5",
        params![
            transition.state.as_str(),
            sequence as i64,
            diagnostic,
            now,
            turn_id.to_string()
        ],
    )
    .map_err(err)?;
    stale_pending_interactions(tx, turn_id, sequence, now)?;
    update_thread_terminal(tx, turn.thread_id, transition.actor_state, true, sequence)?;
    Ok(sequence)
}

fn stale_pending_interactions(
    tx: &Transaction<'_>,
    turn_id: Uuid,
    sequence: u64,
    now: i64,
) -> AppResult<()> {
    tx.execute(
        "UPDATE turn_interactions
         SET status = 'stale', response_json = '{\"reason\":\"turn_terminal\"}',
             response_sequence = ?1, responded_at = ?2
         WHERE turn_id = ?3 AND status = 'pending'",
        params![sequence as i64, now, turn_id.to_string()],
    )
    .map_err(err)?;
    Ok(())
}

fn append_turn_event(
    tx: &Transaction<'_>,
    thread_id: Uuid,
    event_type: &str,
    payload: Value,
    now: i64,
) -> AppResult<u64> {
    let next_version = aggregate_version(tx, thread_id)? + 1;
    tx.execute(
        "INSERT INTO events
         (aggregate_id, aggregate_version, event_type, payload_json, occurred_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            thread_id.to_string(),
            next_version as i64,
            event_type,
            payload.to_string(),
            now
        ],
    )
    .map_err(err)?;
    let sequence = tx.last_insert_rowid() as u64;
    tx.execute(
        "INSERT INTO aggregate_versions (aggregate_id, version)
         VALUES (?1, ?2)
         ON CONFLICT(aggregate_id) DO UPDATE SET version = excluded.version",
        params![thread_id.to_string(), next_version as i64],
    )
    .map_err(err)?;
    Ok(sequence)
}

struct WorktreeReady {
    worktree_id: Uuid,
    project_id: Uuid,
    branch: String,
    path: PathBuf,
}

#[derive(Clone)]
struct WorktreePlan {
    worktree_id: Uuid,
    thread_id: Uuid,
    project_id: Uuid,
    repo: PathBuf,
    repo_common_dir: PathBuf,
    branch: String,
    path: PathBuf,
    commit_oid: String,
}

type TurnAdmissionRow = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
);

fn load_admission_worktree_plan(
    tx: &Transaction<'_>,
    worktree_id: Uuid,
    thread_id: Uuid,
) -> AppResult<WorktreePlan> {
    let mut statement = tx
        .prepare(
            "SELECT wp.worktree_id, wp.thread_id, wp.project_id, wp.repo_path,
                    wp.repo_common_dir, wp.branch, wp.path, wp.commit_oid
             FROM worktree_plans wp
             JOIN command_receipts cr ON cr.command_id = wp.command_id
             WHERE wp.worktree_id = ?1 AND wp.thread_id = ?2 AND cr.status = 'succeeded'
             ORDER BY cr.recorded_at, wp.command_id
             LIMIT 2",
        )
        .map_err(err)?;
    let rows = statement
        .query_map(
            params![worktree_id.to_string(), thread_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                ))
            },
        )
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    if rows.len() != 1 {
        return Err(format!(
            "worktree {worktree_id} does not have exactly one succeeded durable plan for thread {thread_id}"
        ));
    }
    let (
        stored_worktree_id,
        stored_thread_id,
        project_id,
        repo,
        repo_common_dir,
        branch,
        path,
        commit_oid,
    ) = rows.into_iter().next().expect("one worktree plan row");
    Ok(WorktreePlan {
        worktree_id: Uuid::parse_str(&stored_worktree_id).map_err(err)?,
        thread_id: Uuid::parse_str(&stored_thread_id).map_err(err)?,
        project_id: Uuid::parse_str(&project_id).map_err(err)?,
        repo: PathBuf::from(repo),
        repo_common_dir: PathBuf::from(repo_common_dir),
        branch,
        path: PathBuf::from(path),
        commit_oid,
    })
}

impl From<&WorktreePlan> for WorktreeReady {
    fn from(plan: &WorktreePlan) -> Self {
        Self {
            worktree_id: plan.worktree_id,
            project_id: plan.project_id,
            branch: plan.branch.clone(),
            path: plan.path.clone(),
        }
    }
}

fn apply_projection(
    tx: &Transaction<'_>,
    command: &Command,
    worktree: Option<&WorktreeReady>,
    sequence: u64,
    now: i64,
) -> AppResult<()> {
    match command {
        Command::ProjectCreate {
            project_id,
            name,
            repo_path,
        } => {
            let repo_path = git_toplevel(repo_path)?;
            tx.execute(
                "INSERT INTO projects (project_id, name, repo_path) VALUES (?1, ?2, ?3)",
                params![
                    project_id.to_string(),
                    bounded(name, 120)?,
                    repo_path.to_string_lossy()
                ],
            )
            .map_err(err)?;
        }
        Command::ThreadCreate {
            thread_id,
            project_id,
            provider,
            label,
        } => {
            tx.execute(
                "INSERT INTO threads
                 (thread_id, project_id, provider, label, state, last_event_sequence)
                 VALUES (?1, ?2, ?3, ?4, 'idle', ?5)",
                params![
                    thread_id.to_string(),
                    project_id.to_string(),
                    provider.as_str(),
                    bounded(label, 80)?,
                    sequence as i64
                ],
            )
            .map_err(err)?;
        }
        Command::WorktreeCreate { thread_id, .. } => {
            let worktree = worktree.ok_or_else(|| "worktree preparation missing".to_owned())?;
            tx.execute(
                "INSERT INTO worktrees
                 (worktree_id, project_id, branch, path, status)
                 VALUES (?1, ?2, ?3, ?4, 'ready')
                 ON CONFLICT(worktree_id) DO NOTHING",
                params![
                    worktree.worktree_id.to_string(),
                    worktree.project_id.to_string(),
                    worktree.branch,
                    worktree.path.to_string_lossy()
                ],
            )
            .map_err(err)?;
            tx.execute(
                "UPDATE threads SET worktree_id = ?1, last_event_sequence = ?2
                 WHERE thread_id = ?3",
                params![
                    worktree.worktree_id.to_string(),
                    sequence as i64,
                    thread_id.to_string()
                ],
            )
            .map_err(err)?;
        }
        Command::TurnSend {
            thread_id, text, ..
        } => {
            let text = bounded(text, MAX_PROMPT_BYTES)?;
            tx.execute(
                "INSERT INTO messages (sequence, thread_id, role, body, occurred_at)
                 VALUES (?1, ?2, 'user', ?3, ?4)",
                params![sequence as i64, thread_id.to_string(), text, now],
            )
            .map_err(err)?;
            tx.execute(
                "UPDATE threads
                 SET state = 'waiting_user', attention = 0, unread_count = 0,
                     last_event_sequence = ?1
                 WHERE thread_id = ?2",
                params![sequence as i64, thread_id.to_string()],
            )
            .map_err(err)?;
        }
        Command::LiveTurnStart {
            turn_id,
            thread_id,
            text,
        } => {
            let text = bounded(text, MAX_PROMPT_BYTES)?;
            let (provider, worktree_path): (String, String) = tx
                .query_row(
                    "SELECT t.provider, w.path
                     FROM threads t
                     JOIN worktrees w ON w.worktree_id = t.worktree_id
                     WHERE t.thread_id = ?1 AND w.status = 'ready'",
                    [thread_id.to_string()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(err)?;
            tx.execute(
                "INSERT INTO messages (sequence, thread_id, role, body, occurred_at)
                 VALUES (?1, ?2, 'user', ?3, ?4)",
                params![sequence as i64, thread_id.to_string(), text, now],
            )
            .map_err(err)?;
            tx.execute(
                "INSERT INTO turns
                 (turn_id, thread_id, provider, worktree_path, policy, status,
                  prompt_sequence, accepted_sequence, accepted_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'accepted', ?6, ?6, ?7)",
                params![
                    turn_id.to_string(),
                    thread_id.to_string(),
                    provider,
                    worktree_path,
                    ISOLATED_WORKSPACE_WRITE_POLICY,
                    sequence as i64,
                    now
                ],
            )
            .map_err(err)?;
            tx.execute(
                "UPDATE threads
                 SET state = 'starting', attention = 0, unread_count = 0,
                     last_event_sequence = ?1
                 WHERE thread_id = ?2",
                params![sequence as i64, thread_id.to_string()],
            )
            .map_err(err)?;
        }
        Command::ApprovalRespond {
            turn_id,
            thread_id,
            interaction_id,
            ..
        }
        | Command::UserInputRespond {
            turn_id,
            thread_id,
            interaction_id,
            ..
        } => {
            let response_json = serde_json::to_string(command).map_err(err)?;
            tx.execute(
                "UPDATE turn_interactions
                 SET status = 'responded', response_json = ?1, response_sequence = ?2,
                     responded_at = ?3
                 WHERE interaction_id = ?4 AND turn_id = ?5 AND status = 'pending'",
                params![
                    response_json,
                    sequence as i64,
                    now,
                    interaction_id,
                    turn_id.to_string()
                ],
            )
            .map_err(err)?;
            tx.execute(
                "UPDATE turns SET status = 'streaming' WHERE turn_id = ?1",
                [turn_id.to_string()],
            )
            .map_err(err)?;
            tx.execute(
                "UPDATE threads
                 SET state = 'running', attention = 0, last_event_sequence = ?1
                 WHERE thread_id = ?2",
                params![sequence as i64, thread_id.to_string()],
            )
            .map_err(err)?;
        }
        Command::LiveTurnInterrupt { turn_id, thread_id } => {
            tx.execute(
                "UPDATE turns SET status = 'interrupting' WHERE turn_id = ?1",
                [turn_id.to_string()],
            )
            .map_err(err)?;
            tx.execute(
                "UPDATE threads
                 SET state = 'interrupting', last_event_sequence = ?1
                 WHERE thread_id = ?2",
                params![sequence as i64, thread_id.to_string()],
            )
            .map_err(err)?;
        }
        Command::TurnInterrupt { thread_id } => {
            tx.execute(
                "UPDATE threads
                 SET state = 'interrupting', last_event_sequence = ?1
                 WHERE thread_id = ?2",
                params![sequence as i64, thread_id.to_string()],
            )
            .map_err(err)?;
        }
        Command::ThreadArchive { thread_id } => {
            tx.execute(
                "UPDATE threads
                 SET state = 'archived', last_event_sequence = ?1
                 WHERE thread_id = ?2",
                params![sequence as i64, thread_id.to_string()],
            )
            .map_err(err)?;
        }
    }
    Ok(())
}

fn validate_command(
    tx: &Transaction<'_>,
    command: &Command,
    provider_readiness: &ProviderReadiness,
) -> AppResult<()> {
    match command {
        Command::ProjectCreate {
            project_id, name, ..
        } => {
            bounded(name, 120)?;
            let exists: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM projects WHERE project_id = ?1)",
                    [project_id.to_string()],
                    |row| row.get(0),
                )
                .map_err(err)?;
            if exists {
                return Err(format!("project {project_id} already exists"));
            }
        }
        Command::ThreadCreate {
            thread_id,
            project_id,
            label,
            ..
        } => {
            bounded(label, 80)?;
            let project_exists: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM projects WHERE project_id = ?1)",
                    [project_id.to_string()],
                    |row| row.get(0),
                )
                .map_err(err)?;
            if !project_exists {
                return Err(format!("project {project_id} does not exist"));
            }
            let thread_exists: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM threads WHERE thread_id = ?1)",
                    [thread_id.to_string()],
                    |row| row.get(0),
                )
                .map_err(err)?;
            if thread_exists {
                return Err(format!("thread {thread_id} already exists"));
            }
        }
        Command::TurnSend {
            thread_id, text, ..
        } => {
            bounded(text, MAX_PROMPT_BYTES)?;
            let state = required_thread_state(tx, *thread_id)?;
            if state == "archived" {
                return Err(format!("thread {thread_id} is archived"));
            }
        }
        Command::LiveTurnStart {
            turn_id,
            thread_id,
            text,
        } => {
            bounded(text, MAX_PROMPT_BYTES)?;
            let thread: Option<TurnAdmissionRow> = tx
                .query_row(
                    "SELECT t.provider, t.state, t.project_id, t.worktree_id,
                            w.status, w.path, w.project_id, w.branch, p.repo_path
                     FROM threads t
                     JOIN projects p ON p.project_id = t.project_id
                     LEFT JOIN worktrees w ON w.worktree_id = t.worktree_id
                     WHERE t.thread_id = ?1",
                    [thread_id.to_string()],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                            row.get(6)?,
                            row.get(7)?,
                            row.get(8)?,
                        ))
                    },
                )
                .optional()
                .map_err(err)?;
            let Some((
                provider,
                state,
                thread_project_id,
                worktree_id,
                worktree_status,
                worktree_path,
                worktree_project_id,
                worktree_branch,
                project_repo_path,
            )) = thread
            else {
                return Err(format!("thread {thread_id} does not exist"));
            };
            if !matches!(provider.as_str(), "codex" | "claude") {
                return Err(format!(
                    "thread {thread_id} has unsupported provider {provider}"
                ));
            }
            if provider != Provider::Codex.as_str() {
                return Err(format!(
                    "thread {thread_id} provider {provider} is unavailable for live turns"
                ));
            }
            match provider_readiness {
                ProviderReadiness::Available => {}
                ProviderReadiness::Unavailable { diagnostic } => {
                    return Err(format!("provider unavailable: {diagnostic}"));
                }
                ProviderReadiness::UnsupportedVersion {
                    installed,
                    supported,
                } => {
                    return Err(format!(
                        "unsupported provider version {installed}; supported version is {supported}"
                    ));
                }
            }
            if !turn_state_allows_new_turn(&state) {
                return Err(format!(
                    "thread {thread_id} cannot start a turn while {state}"
                ));
            }
            if worktree_status.as_deref() != Some("ready") {
                return Err(format!(
                    "thread {thread_id} needs a verified ready worktree before a live turn"
                ));
            }
            let worktree_id = worktree_id
                .ok_or_else(|| format!("thread {thread_id} has no worktree identity"))?;
            let worktree_id = Uuid::parse_str(&worktree_id).map_err(err)?;
            let path = worktree_path
                .map(PathBuf::from)
                .ok_or_else(|| format!("thread {thread_id} has no worktree path"))?;
            let plan = load_admission_worktree_plan(tx, worktree_id, *thread_id)?;
            if plan.project_id.to_string() != thread_project_id
                || worktree_project_id.as_deref() != Some(thread_project_id.as_str())
                || plan.path != path
                || worktree_branch.as_deref() != Some(plan.branch.as_str())
                || plan.repo.as_path() != Path::new(&project_repo_path)
            {
                return Err(format!(
                    "thread {thread_id} worktree projection does not match its durable plan"
                ));
            }
            verify_worktree(&plan).map_err(|error| {
                format!("thread {thread_id} worktree failed admission verification: {error}")
            })?;
            let turn_exists: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM turns WHERE turn_id = ?1)",
                    [turn_id.to_string()],
                    |row| row.get(0),
                )
                .map_err(err)?;
            if turn_exists {
                return Err(format!("turn {turn_id} already exists"));
            }
            let active: Option<String> = tx
                .query_row(
                    "SELECT turn_id FROM turns
                     WHERE status IN (
                        'accepted', 'starting', 'streaming', 'awaiting_approval',
                        'awaiting_user_input', 'interrupting'
                     ) LIMIT 1",
                    [],
                    |row| row.get(0),
                )
                .optional()
                .map_err(err)?;
            if let Some(active) = active {
                return Err(format!(
                    "turn {active} already occupies the single live-provider slot"
                ));
            }
        }
        Command::ApprovalRespond {
            turn_id,
            thread_id,
            interaction_id,
            ..
        } => validate_interaction_response(
            tx,
            *turn_id,
            *thread_id,
            interaction_id,
            "approval",
            LiveTurnState::AwaitingApproval,
        )?,
        Command::UserInputRespond {
            turn_id,
            thread_id,
            interaction_id,
            answers,
        } => {
            validate_user_input_answers(answers)?;
            validate_interaction_response(
                tx,
                *turn_id,
                *thread_id,
                interaction_id,
                "user_input",
                LiveTurnState::AwaitingUserInput,
            )?;
            let request_json: String = tx
                .query_row(
                    "SELECT request_json FROM turn_interactions WHERE interaction_id = ?1",
                    [interaction_id],
                    |row| row.get(0),
                )
                .map_err(err)?;
            let event: ProviderEvent = serde_json::from_str(&request_json).map_err(err)?;
            let ProviderEvent::UserInputRequested { questions, .. } = event else {
                return Err(format!(
                    "interaction {interaction_id} does not contain a user-input request"
                ));
            };
            let mut expected = questions
                .into_iter()
                .map(|question| question.question_id)
                .collect::<Vec<_>>();
            let mut actual = answers
                .iter()
                .map(|answer| answer.question_id.clone())
                .collect::<Vec<_>>();
            expected.sort();
            actual.sort();
            actual.dedup();
            if actual != expected {
                return Err(format!(
                    "interaction {interaction_id} answers must exactly match the pending questions"
                ));
            }
        }
        Command::LiveTurnInterrupt { turn_id, thread_id } => {
            let (stored_thread_id, status): (String, String) = tx
                .query_row(
                    "SELECT thread_id, status FROM turns WHERE turn_id = ?1",
                    [turn_id.to_string()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(err)?
                .ok_or_else(|| format!("turn {turn_id} does not exist"))?;
            if stored_thread_id != thread_id.to_string() {
                return Err(format!(
                    "turn {turn_id} does not belong to thread {thread_id}"
                ));
            }
            if !matches!(
                status.as_str(),
                "starting" | "streaming" | "awaiting_approval" | "awaiting_user_input"
            ) {
                return Err(format!(
                    "turn {turn_id} cannot be interrupted while {status}"
                ));
            }
        }
        Command::TurnInterrupt { thread_id } => {
            let _ = required_thread_state(tx, *thread_id)?;
            return Err("live interruption is not implemented for the bounded Codex slice".into());
        }
        Command::ThreadArchive { thread_id } => {
            let state = required_thread_state(tx, *thread_id)?;
            if state == "archived" {
                return Err(format!("thread {thread_id} is already archived"));
            }
            let active: bool = tx
                .query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM turns
                        WHERE thread_id = ?1 AND status IN (
                            'accepted', 'starting', 'streaming', 'awaiting_approval',
                            'awaiting_user_input', 'interrupting'
                        )
                    )",
                    [thread_id.to_string()],
                    |row| row.get(0),
                )
                .map_err(err)?;
            if active || !turn_state_allows_new_turn(&state) {
                return Err(format!(
                    "thread {thread_id} cannot be archived while live work is {state}"
                ));
            }
        }
        Command::WorktreeCreate { .. } => {
            return Err("worktree command bypassed its durable execution path".into());
        }
    }
    Ok(())
}

fn validate_interaction_response(
    tx: &Transaction<'_>,
    turn_id: Uuid,
    thread_id: Uuid,
    interaction_id: &str,
    expected_kind: &str,
    expected_state: LiveTurnState,
) -> AppResult<()> {
    bounded(interaction_id, MAX_INTERACTION_ID_BYTES)?;
    if interaction_id.trim().is_empty() {
        return Err("interaction id must not be empty".into());
    }
    let interaction: Option<(String, String, String, String)> = tx
        .query_row(
            "SELECT i.turn_id, i.kind, i.status, tr.status
             FROM turn_interactions i
             JOIN turns tr ON tr.turn_id = i.turn_id
             WHERE i.interaction_id = ?1 AND tr.thread_id = ?2",
            params![interaction_id, thread_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(err)?;
    let Some((stored_turn_id, kind, status, turn_status)) = interaction else {
        return Err(format!(
            "interaction {interaction_id} is not pending for thread {thread_id}"
        ));
    };
    if stored_turn_id != turn_id.to_string() {
        return Err(format!(
            "interaction {interaction_id} belongs to turn {stored_turn_id}, not {turn_id}"
        ));
    }
    if kind != expected_kind {
        return Err(format!(
            "interaction {interaction_id} expects {kind}, not {expected_kind}"
        ));
    }
    if status != "pending" {
        return Err(format!("interaction {interaction_id} is already {status}"));
    }
    if turn_status != expected_state.as_str() {
        return Err(format!(
            "interaction {interaction_id} cannot be answered while turn {turn_id} is {turn_status}"
        ));
    }
    Ok(())
}

fn validate_user_input_answers(answers: &[UserInputAnswer]) -> AppResult<()> {
    let command = ProviderCommand::UserInputResponse {
        turn_id: Uuid::nil(),
        interaction_id: "validation".into(),
        answers: answers.to_vec(),
    };
    command.validate()
}

fn required_thread_state(tx: &Transaction<'_>, thread_id: Uuid) -> AppResult<String> {
    tx.query_row(
        "SELECT state FROM threads WHERE thread_id = ?1",
        [thread_id.to_string()],
        |row| row.get(0),
    )
    .optional()
    .map_err(err)?
    .ok_or_else(|| format!("thread {thread_id} does not exist"))
}

fn turn_state_allows_new_turn(state: &str) -> bool {
    matches!(
        state,
        "idle" | "waiting_user" | "stopped" | "failed" | "indeterminate"
    )
}

fn store_rejection(
    tx: Transaction<'_>,
    envelope: &CommandEnvelope,
    command_json: &str,
    result: Value,
    now: i64,
) -> AppResult<Receipt> {
    tx.execute(
        "INSERT INTO command_receipts
         (command_id, protocol_version, command_json, status, result_json, recorded_at)
         VALUES (?1, ?2, ?3, 'rejected', ?4, ?5)",
        params![
            envelope.command_id.to_string(),
            envelope.protocol_version,
            command_json,
            result.to_string(),
            now
        ],
    )
    .map_err(err)?;
    tx.commit().map_err(err)?;
    Ok(Receipt {
        command_id: envelope.command_id,
        status: "rejected".into(),
        result,
        event_sequence: None,
    })
}

fn event_type(command: &Command) -> &'static str {
    match command {
        Command::ProjectCreate { .. } => "project.created",
        Command::ThreadCreate { .. } => "thread.created",
        Command::WorktreeCreate { .. } => "worktree.created",
        Command::TurnSend { .. } => "turn.saved",
        Command::LiveTurnStart { .. } => "turn.accepted",
        Command::ApprovalRespond { .. } => "turn.approval_responded",
        Command::UserInputRespond { .. } => "turn.user_input_responded",
        Command::LiveTurnInterrupt { .. } => "turn.interrupt_requested",
        Command::TurnInterrupt { .. } => "turn.interrupt_requested",
        Command::ThreadArchive { .. } => "thread.archived",
    }
}

fn aggregate_version(tx: &Transaction<'_>, aggregate_id: Uuid) -> AppResult<u64> {
    tx.query_row(
        "SELECT version FROM aggregate_versions WHERE aggregate_id = ?1",
        [aggregate_id.to_string()],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .map_err(err)
    .map(|version| version.unwrap_or(0) as u64)
}

fn connection_aggregate_version(conn: &Connection, aggregate_id: Uuid) -> AppResult<u64> {
    conn.query_row(
        "SELECT version FROM aggregate_versions WHERE aggregate_id = ?1",
        [aggregate_id.to_string()],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .map_err(err)
    .map(|version| version.unwrap_or(0) as u64)
}

fn git_toplevel(path: &Path) -> AppResult<PathBuf> {
    let output = ProcessCommand::new("git")
        .arg("-C")
        .arg(path)
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .map_err(err)?;
    if !output.status.success() {
        return Err(format!(
            "{} is not a Git worktree: {}",
            path.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    PathBuf::from(String::from_utf8_lossy(&output.stdout).trim())
        .canonicalize()
        .map(portable_windows_path)
        .map_err(err)
}

fn git_common_dir(repo: &Path) -> AppResult<PathBuf> {
    let value = git_stdout(repo, &["rev-parse", "--git-common-dir"])?;
    let path = PathBuf::from(value);
    let path = if path.is_absolute() {
        path
    } else {
        repo.join(path)
    };
    path.canonicalize().map(portable_windows_path).map_err(err)
}

fn git_commit(repo: &Path, revision: &str) -> AppResult<String> {
    let revision = format!("{revision}^{{commit}}");
    let oid = git_stdout(repo, &["rev-parse", "--verify", &revision])?;
    if oid.len() < 40 || !oid.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("Git returned an invalid commit object ID".into());
    }
    Ok(oid)
}

fn git_stdout(repo: &Path, args: &[&str]) -> AppResult<String> {
    let output = ProcessCommand::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .map_err(err)?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).trim().to_owned())
    }
}

struct WorktreeError {
    message: String,
    indeterminate: bool,
}

fn create_or_reconcile_worktree(plan: &WorktreePlan) -> Result<(), WorktreeError> {
    if plan.path.exists() {
        return verify_worktree(plan).map_err(|message| WorktreeError {
            message,
            indeterminate: true,
        });
    }

    let branch_exists = ProcessCommand::new("git")
        .arg("-C")
        .arg(&plan.repo)
        .args(["show-ref", "--verify", "--quiet"])
        .arg(format!("refs/heads/{}", plan.branch))
        .status()
        .map_err(|error| WorktreeError {
            message: error.to_string(),
            indeterminate: false,
        })?
        .success();
    if branch_exists {
        let branch_oid =
            git_commit(&plan.repo, &format!("refs/heads/{}", plan.branch)).map_err(|message| {
                WorktreeError {
                    message,
                    indeterminate: true,
                }
            })?;
        if branch_oid != plan.commit_oid {
            return Err(WorktreeError {
                message: format!(
                    "worktree branch {} points to {}, expected {}; refusing to reset it",
                    plan.branch, branch_oid, plan.commit_oid
                ),
                indeterminate: true,
            });
        }
    }

    let mut command = ProcessCommand::new("git");
    command.arg("-C").arg(&plan.repo).args(["worktree", "add"]);
    if !branch_exists {
        command.args(["-b", &plan.branch]);
    }
    command.arg(&plan.path);
    command.arg(if branch_exists {
        plan.branch.as_str()
    } else {
        plan.commit_oid.as_str()
    });
    command.env("GIT_TERMINAL_PROMPT", "0");
    let output = command.output().map_err(|error| WorktreeError {
        message: error.to_string(),
        indeterminate: false,
    })?;
    if !output.status.success() {
        return Err(WorktreeError {
            message: format!(
                "git worktree add failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ),
            indeterminate: true,
        });
    }
    verify_worktree(plan).map_err(|message| WorktreeError {
        message,
        indeterminate: true,
    })
}

fn verify_worktree(plan: &WorktreePlan) -> AppResult<()> {
    let parent = plan
        .path
        .parent()
        .ok_or_else(|| format!("worktree path {} has no parent", plan.path.display()))?;
    let name = plan.path.file_name().ok_or_else(|| {
        format!(
            "worktree path {} has no final component",
            plan.path.display()
        )
    })?;
    let expected_path = portable_windows_path(parent.canonicalize().map_err(err)?.join(name));
    let resolved_path = portable_windows_path(plan.path.canonicalize().map_err(err)?);
    if resolved_path != expected_path {
        return Err(format!(
            "worktree path {} resolves through a redirect to {}",
            plan.path.display(),
            resolved_path.display()
        ));
    }
    let source_path = portable_windows_path(plan.repo.canonicalize().map_err(err)?);
    if resolved_path == source_path {
        return Err("worktree destination resolves to the shared source repository".into());
    }
    let actual_path = git_toplevel(&plan.path)?;
    let actual_common_dir = git_common_dir(&plan.path)?;
    let branch = git_stdout(&plan.path, &["symbolic-ref", "--quiet", "HEAD"])?;
    let oid = git_commit(&plan.path, "HEAD")?;
    if actual_path != expected_path
        || actual_common_dir != plan.repo_common_dir
        || branch != format!("refs/heads/{}", plan.branch)
        || oid != plan.commit_oid
    {
        return Err(format!(
            "worktree state does not match the durable plan at {}",
            plan.path.display()
        ));
    }
    Ok(())
}

fn slugify(value: &str) -> String {
    let mut slug = String::with_capacity(value.len().min(40));
    let mut dash = false;
    for ch in value.chars().flat_map(char::to_lowercase) {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch);
            dash = false;
        } else if !dash && !slug.is_empty() {
            slug.push('-');
            dash = true;
        }
        if slug.len() >= 40 {
            break;
        }
    }
    slug.trim_matches('-').to_owned().if_empty("thread")
}

trait IfEmpty {
    fn if_empty(self, fallback: &str) -> String;
}

impl IfEmpty for String {
    fn if_empty(self, fallback: &str) -> String {
        if self.is_empty() {
            fallback.to_owned()
        } else {
            self
        }
    }
}

fn bounded(value: &str, max_bytes: usize) -> AppResult<&str> {
    if value.is_empty() {
        return Err("value may not be empty".into());
    }
    if value.len() > max_bytes {
        return Err(format!("value exceeds {max_bytes} UTF-8 bytes"));
    }
    Ok(value)
}

fn terminal_diagnostic(value: &str) -> String {
    let value = if value.is_empty() {
        "unspecified Codex runtime failure"
    } else {
        value
    };
    if value.len() <= MAX_TURN_ERROR_BYTES {
        return value.to_owned();
    }
    let keep = MAX_TURN_ERROR_BYTES.saturating_sub(DIAGNOSTIC_TRUNCATION_MARKER.len());
    let mut boundary = keep.min(value.len());
    while boundary > 0 && !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    let mut diagnostic = String::with_capacity(MAX_TURN_ERROR_BYTES);
    diagnostic.push_str(&value[..boundary]);
    diagnostic.push_str(DIAGNOSTIC_TRUNCATION_MARKER);
    diagnostic
}

fn portable_windows_path(path: PathBuf) -> PathBuf {
    let text = path.to_string_lossy();
    if let Some(rest) = text.strip_prefix(r"\\?\UNC\") {
        PathBuf::from(format!(r"\\{rest}"))
    } else if let Some(rest) = text.strip_prefix(r"\\?\") {
        PathBuf::from(rest)
    } else {
        path
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn to_sql_error(error: uuid::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(36, rusqlite::types::Type::Text, Box::new(error))
}

pub fn self_check() -> AppResult<Value> {
    let root = std::env::temp_dir().join(format!("agent-world-self-check-{}", Uuid::new_v4()));
    let source = root.join("source");
    fs::create_dir_all(&source).map_err(err)?;
    run_git(&source, &["init", "--initial-branch=main"])?;
    run_git(
        &source,
        &["config", "user.email", "agent-world@local.invalid"],
    )?;
    run_git(&source, &["config", "user.name", "Agent World Self Check"])?;
    fs::write(source.join("README.md"), "# fixture\n").map_err(err)?;
    run_git(&source, &["add", "README.md"])?;
    run_git(&source, &["commit", "-m", "fixture"])?;

    let runtime = root.join("runtime");
    fs::create_dir_all(runtime.join("worktrees")).map_err(err)?;
    let db_path = runtime.join("state.sqlite");
    let mut store = Store::open(db_path.clone(), runtime.clone())?;
    let project_id = Uuid::new_v4();
    let project_envelope = CommandEnvelope::new(Command::ProjectCreate {
        project_id,
        name: "Self check".into(),
        repo_path: source.clone(),
    });
    let first = store.execute(project_envelope.clone())?;
    let replay = store.execute(project_envelope.clone())?;
    if first.event_sequence != replay.event_sequence {
        return Err("idempotent replay produced a second event".into());
    }
    let mut conflicting_envelope = project_envelope;
    conflicting_envelope.command = Command::ProjectCreate {
        project_id,
        name: "Altered replay".into(),
        repo_path: source.clone(),
    };
    if store.execute(conflicting_envelope).is_ok() {
        return Err("altered replay was not rejected".into());
    }
    let events_before_invalid: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .map_err(err)?;
    let invalid_interrupt = store.execute(CommandEnvelope::new(Command::TurnInterrupt {
        thread_id: Uuid::new_v4(),
    }))?;
    let events_after_invalid: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .map_err(err)?;
    if invalid_interrupt.status != "rejected" || events_after_invalid != events_before_invalid {
        return Err("invalid thread mutation was not rejected without an event".into());
    }
    let schema_version: i64 = store
        .conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(err)?;
    if schema_version != SCHEMA_VERSION {
        return Err("database schema migration version was not recorded".into());
    }
    verify_foreign_keys(&store.conn)?;

    let crash_before_thread = Uuid::new_v4();
    store.execute(CommandEnvelope::new(Command::ThreadCreate {
        thread_id: crash_before_thread,
        project_id,
        provider: Provider::Codex,
        label: "Crash before Git".into(),
    }))?;
    let crash_before_envelope = CommandEnvelope::new(Command::WorktreeCreate {
        worktree_id: Uuid::new_v4(),
        thread_id: crash_before_thread,
    });
    let crash_before_payload =
        serde_json::to_string(&crash_before_envelope.command).map_err(err)?;
    let crash_before_plan = store.plan_worktree(
        match &crash_before_envelope.command {
            Command::WorktreeCreate { worktree_id, .. } => *worktree_id,
            _ => unreachable!(),
        },
        crash_before_thread,
    )?;
    store.accept_worktree(
        &crash_before_envelope,
        &crash_before_payload,
        &crash_before_plan,
    )?;
    if crash_before_plan.path.exists() {
        return Err("crash-before-Git fixture unexpectedly created a worktree".into());
    }

    drop(store);
    let mut store = Store::open(db_path.clone(), runtime.clone())?;
    if !store.recover_accepted_worktrees()?.is_empty() {
        return Err("crash-before-Git recovery reported a warning".into());
    }
    let recovered_before = store
        .load_receipt(crash_before_envelope.command_id, &crash_before_payload)?
        .ok_or_else(|| "crash-before-Git receipt disappeared".to_owned())?;
    if recovered_before.status != "succeeded" {
        return Err("crash-before-Git receipt did not recover".into());
    }
    verify_worktree(&crash_before_plan)?;
    let duplicate_worktree = store.execute(CommandEnvelope::new(Command::WorktreeCreate {
        worktree_id: Uuid::new_v4(),
        thread_id: crash_before_thread,
    }))?;
    if duplicate_worktree.status != "rejected" {
        return Err("second worktree for an attached thread was not rejected".into());
    }

    let crash_after_thread = Uuid::new_v4();
    store.execute(CommandEnvelope::new(Command::ThreadCreate {
        thread_id: crash_after_thread,
        project_id,
        provider: Provider::Codex,
        label: "Crash after Git".into(),
    }))?;
    let crash_after_envelope = CommandEnvelope::new(Command::WorktreeCreate {
        worktree_id: Uuid::new_v4(),
        thread_id: crash_after_thread,
    });
    let crash_after_payload = serde_json::to_string(&crash_after_envelope.command).map_err(err)?;
    let crash_after_worktree_id = match &crash_after_envelope.command {
        Command::WorktreeCreate { worktree_id, .. } => *worktree_id,
        _ => unreachable!(),
    };
    let crash_after_plan = store.plan_worktree(crash_after_worktree_id, crash_after_thread)?;
    store.accept_worktree(
        &crash_after_envelope,
        &crash_after_payload,
        &crash_after_plan,
    )?;
    create_or_reconcile_worktree(&crash_after_plan).map_err(|error| error.message)?;
    drop(store);

    let mut store = Store::open(db_path.clone(), runtime.clone())?;
    if !store.recover_accepted_worktrees()?.is_empty() {
        return Err("crash-after-Git recovery reported a warning".into());
    }
    let recovered_after = store
        .load_receipt(crash_after_envelope.command_id, &crash_after_payload)?
        .ok_or_else(|| "crash-after-Git receipt disappeared".to_owned())?;
    if recovered_after.status != "succeeded" {
        return Err("crash-after-Git receipt did not recover".into());
    }
    verify_worktree(&crash_after_plan)?;

    let live_turn_id = Uuid::new_v4();
    let live_turn_envelope = CommandEnvelope::new(Command::LiveTurnStart {
        turn_id: live_turn_id,
        thread_id: crash_after_thread,
        text: "Describe the fixture without changing it".into(),
    });
    let (live_turn_receipt, live_turn_command) =
        store.execute_with_provider_command(live_turn_envelope.clone())?;
    if live_turn_receipt.status != "succeeded" || live_turn_command.is_none() {
        return Err("isolated workspace-write turn intent was not admitted".into());
    }
    let (_, replay_command) = store.execute_with_provider_command(live_turn_envelope)?;
    if replay_command.is_some() {
        return Err(
            "isolated workspace-write turn receipt replay attempted a second launch".into(),
        );
    }
    let live_session = ProviderSessionCursor {
        session_id: "0199a213-81c0-7800-8aa1-bbab2a035a53".into(),
        resume_cursor: "self-check-cursor".into(),
    };
    store.apply_provider_events(&[
        ProviderEvent::Starting {
            turn_id: live_turn_id,
            provider_event_id: "self-check-starting".into(),
        },
        ProviderEvent::SessionEstablished {
            turn_id: live_turn_id,
            provider_event_id: "self-check-session".into(),
            session: live_session.clone(),
        },
        ProviderEvent::AssistantOutput {
            turn_id: live_turn_id,
            provider_event_id: "self-check-output".into(),
            delta: "Fixture answer".into(),
            resume_cursor: Some("self-check-output-cursor".into()),
        },
        ProviderEvent::Completed {
            turn_id: live_turn_id,
            provider_event_id: "self-check-completed".into(),
            session: live_session,
        },
    ])?;
    let live_messages = store.timeline_page(crash_after_thread, 10)?;
    if live_messages.len() != 2 || live_messages[1].role != "assistant" {
        return Err("isolated workspace-write turn result was not durably projected".into());
    }

    let interrupted_by_restart_id = Uuid::new_v4();
    let (_, interrupted_plan) =
        store.execute_with_provider_command(CommandEnvelope::new(Command::LiveTurnStart {
            turn_id: interrupted_by_restart_id,
            thread_id: crash_after_thread,
            text: "Remain unfinished for restart recovery".into(),
        }))?;
    if interrupted_plan.is_none() {
        return Err("restart fixture turn was not admitted".into());
    }
    store.apply_provider_events(&[
        ProviderEvent::Starting {
            turn_id: interrupted_by_restart_id,
            provider_event_id: "restart-starting".into(),
        },
        ProviderEvent::SessionEstablished {
            turn_id: interrupted_by_restart_id,
            provider_event_id: "restart-session".into(),
            session: ProviderSessionCursor {
                session_id: "0199a213-81c0-7800-8aa1-bbab2a035a54".into(),
                resume_cursor: "restart-cursor".into(),
            },
        },
    ])?;
    drop(store);
    let mut store = Store::open(db_path, runtime)?;
    let turn_recovery_warnings = store.reconcile_unfinished_turns()?;
    let recovered_turn: (String, Option<String>) = store
        .conn
        .query_row(
            "SELECT status, provider_session_id FROM turns WHERE turn_id = ?1",
            [interrupted_by_restart_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(err)?;
    if turn_recovery_warnings.len() != 1
        || recovered_turn.0 != "indeterminate"
        || recovered_turn.1.as_deref() != Some("0199a213-81c0-7800-8aa1-bbab2a035a54")
    {
        return Err(
            "unfinished isolated workspace-write turn was not conservatively reconciled".into(),
        );
    }

    let accepted_event_count: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE event_type = 'command.accepted'",
            [],
            |row| row.get(0),
        )
        .map_err(err)?;
    let worktree_event_count: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE event_type = 'worktree.created'",
            [],
            |row| row.get(0),
        )
        .map_err(err)?;
    let event_count: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .map_err(err)?;
    drop(store);
    for destination in [&crash_before_plan.path, &crash_after_plan.path] {
        let _ = ProcessCommand::new("git")
            .arg("-C")
            .arg(&source)
            .args(["worktree", "remove", "--force"])
            .arg(destination)
            .status();
    }
    fs::remove_dir_all(&root).map_err(err)?;
    Ok(json!({
        "sqlite_idempotency": true,
        "schema_version": schema_version,
        "foreign_keys_valid": true,
        "invalid_mutation_rejected_without_event": true,
        "native_git_worktree": true,
        "duplicate_worktree_rejected": true,
        "crash_before_git_recovered": true,
        "crash_after_git_recovered": true,
        "isolated_workspace_write_turn_durability": true,
        "turn_restart_not_replayed": true,
        "accepted_event_count": accepted_event_count,
        "worktree_event_count": worktree_event_count,
        "event_count": event_count
    }))
}

pub fn seed_resource_fixture(runtime_root: PathBuf) -> AppResult<Value> {
    fs::create_dir_all(runtime_root.join("worktrees")).map_err(err)?;
    let mut store = Store::open(runtime_root.join("state.sqlite"), runtime_root)?;
    let existing: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .map_err(err)?;
    if existing != 0 {
        return Err("resource fixture requires an empty runtime root".into());
    }
    let repo_path = git_toplevel(&std::env::current_dir().map_err(err)?)?;
    let now = now_ms();
    let tx = store.conn.transaction().map_err(err)?;
    let mut projects = Vec::with_capacity(5);
    for index in 0..5 {
        let project_id = Uuid::new_v4();
        projects.push(project_id);
        tx.execute(
            "INSERT INTO projects (project_id, name, repo_path) VALUES (?1, ?2, ?3)",
            params![
                project_id.to_string(),
                format!("Fixture Project {}", index + 1),
                repo_path.to_string_lossy()
            ],
        )
        .map_err(err)?;
        tx.execute(
            "INSERT INTO events
             (aggregate_id, aggregate_version, event_type, payload_json, occurred_at)
             VALUES (?1, 1, 'fixture.project', '{}', ?2)",
            params![project_id.to_string(), now],
        )
        .map_err(err)?;
        tx.execute(
            "INSERT INTO aggregate_versions (aggregate_id, version) VALUES (?1, 1)",
            [project_id.to_string()],
        )
        .map_err(err)?;
    }

    let mut threads = Vec::with_capacity(50);
    let mut versions = Vec::with_capacity(50);
    for index in 0..50 {
        let thread_id = Uuid::new_v4();
        threads.push(thread_id);
        versions.push(1_i64);
        tx.execute(
            "INSERT INTO events
             (aggregate_id, aggregate_version, event_type, payload_json, occurred_at)
             VALUES (?1, 1, 'fixture.thread', '{}', ?2)",
            params![thread_id.to_string(), now],
        )
        .map_err(err)?;
        let sequence = tx.last_insert_rowid();
        tx.execute(
            "INSERT INTO threads
             (thread_id, project_id, provider, label, state, last_event_sequence)
             VALUES (?1, ?2, ?3, ?4, 'idle', ?5)",
            params![
                thread_id.to_string(),
                projects[index % projects.len()].to_string(),
                if index % 2 == 0 { "codex" } else { "claude" },
                format!("Actor {:02}", index + 1),
                sequence
            ],
        )
        .map_err(err)?;
    }

    for index in 0..20_000 {
        let actor = index % threads.len();
        versions[actor] += 1;
        let thread_id = threads[actor];
        tx.execute(
            "INSERT INTO events
             (aggregate_id, aggregate_version, event_type, payload_json, occurred_at)
             VALUES (?1, ?2, 'fixture.message', ?3, ?4)",
            params![
                thread_id.to_string(),
                versions[actor],
                json!({"fixture_index":index}).to_string(),
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
                if index % 3 == 0 { "assistant" } else { "user" },
                format!("Bounded fixture message {index}"),
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
    for (thread_id, version) in threads.iter().zip(versions) {
        tx.execute(
            "INSERT INTO aggregate_versions (aggregate_id, version) VALUES (?1, ?2)",
            params![thread_id.to_string(), version],
        )
        .map_err(err)?;
    }
    tx.commit().map_err(err)?;
    Ok(json!({
        "projects": 5,
        "visible_threads": 50,
        "persisted_messages": 20_000
    }))
}

fn run_git(cwd: &Path, args: &[&str]) -> AppResult<()> {
    let output = ProcessCommand::new("git")
        .arg("-C")
        .arg(cwd)
        .args(args)
        .output()
        .map_err(err)?;
    if output.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).trim().to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live_turn::fake::{DeterministicFakeRunner, FakeScript};

    struct IdleTestRunner;

    impl ProviderRunner for IdleTestRunner {
        fn run(
            self: Box<Self>,
            commands: Receiver<ProviderCommand>,
            _events: SyncSender<ProviderEvent>,
        ) -> AppResult<()> {
            while let Ok(command) = commands.recv() {
                if matches!(command, ProviderCommand::Shutdown) {
                    break;
                }
            }
            Ok(())
        }
    }

    fn idle_test_runner() -> Box<dyn ProviderRunner> {
        Box::new(IdleTestRunner)
    }

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!("agent-world-{label}-{}", Uuid::new_v4()));
            fs::create_dir_all(&path).expect("create test root");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn create_legacy_database(path: &Path) -> Connection {
        let conn = Connection::open(path).expect("open legacy database");
        conn.pragma_update(None, "foreign_keys", "OFF")
            .expect("disable foreign keys for legacy fixture construction");
        conn.execute_batch(SCHEMA_V1_SQL)
            .expect("create legacy schema shape");
        conn.execute_batch("DROP TABLE schema_migrations; PRAGMA user_version = 0;")
            .expect("mark schema as legacy");
        conn
    }

    fn create_v1_database(path: &Path) -> Connection {
        let conn = Connection::open(path).expect("open v1 database");
        conn.execute_batch(SCHEMA_V1_SQL)
            .expect("create v1 schema shape");
        conn.execute(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (1, 1)",
            [],
        )
        .expect("record v1 migration");
        conn.pragma_update(None, "user_version", 1_i64)
            .expect("mark schema v1");
        conn
    }

    fn create_git_fixture(root: &Path) -> PathBuf {
        let source = root.join("source");
        fs::create_dir_all(&source).expect("create source repository");
        run_git(&source, &["init", "--initial-branch=main"]).expect("initialize Git");
        run_git(
            &source,
            &["config", "user.email", "agent-world@local.invalid"],
        )
        .expect("configure Git email");
        run_git(&source, &["config", "user.name", "Agent World Test"]).expect("configure Git name");
        fs::write(source.join("README.md"), "# fixture\n").expect("write fixture");
        run_git(&source, &["add", "README.md"]).expect("stage fixture");
        run_git(&source, &["commit", "-m", "fixture"]).expect("commit fixture");
        source
    }

    fn create_codex_thread_with_worktree(
        store: &mut Store,
        source: &Path,
        label: &str,
    ) -> (Uuid, Uuid, PathBuf) {
        let project_id = Uuid::new_v4();
        let thread_id = Uuid::new_v4();
        let worktree_id = Uuid::new_v4();
        store
            .execute(CommandEnvelope::new(Command::ProjectCreate {
                project_id,
                name: format!("{label} project"),
                repo_path: source.to_path_buf(),
            }))
            .expect("create live-turn project");
        store
            .execute(CommandEnvelope::new(Command::ThreadCreate {
                thread_id,
                project_id,
                provider: Provider::Codex,
                label: label.into(),
            }))
            .expect("create Codex thread");
        let worktree = store
            .execute(CommandEnvelope::new(Command::WorktreeCreate {
                worktree_id,
                thread_id,
            }))
            .expect("create Codex worktree");
        assert_eq!(worktree.status, "succeeded");
        let path: String = store
            .conn
            .query_row(
                "SELECT path FROM worktrees WHERE worktree_id = ?1",
                [worktree_id.to_string()],
                |row| row.get(0),
            )
            .expect("load Codex worktree path");
        (project_id, thread_id, PathBuf::from(path))
    }

    fn start_fake_turn(
        store: &mut Store,
        thread_id: Uuid,
        script: FakeScript,
    ) -> (Uuid, ProviderPort) {
        let turn_id = Uuid::new_v4();
        let envelope = CommandEnvelope::new(Command::LiveTurnStart {
            turn_id,
            thread_id,
            text: format!("deterministic fake script: {script:?}"),
        });
        let (receipt, provider_command) = store
            .execute_with_provider_command(envelope)
            .expect("durably accept fake turn");
        assert_eq!(receipt.status, "succeeded");
        let status: String = store
            .conn
            .query_row(
                "SELECT status FROM turns WHERE turn_id = ?1",
                [turn_id.to_string()],
                |row| row.get(0),
            )
            .expect("acceptance is visible before provider dispatch");
        assert_eq!(status, LiveTurnState::Accepted.as_str());

        let port = ProviderPort::spawn(Box::new(DeterministicFakeRunner::new(script)))
            .expect("spawn deterministic fake");
        port.command_sender()
            .send(provider_command.expect("new turn emits one provider command"))
            .expect("dispatch only after acceptance commit");
        (turn_id, port)
    }

    fn receive_fake_events_until(
        port: &ProviderPort,
        terminal: impl Fn(&ProviderEvent) -> bool,
    ) -> Vec<ProviderEvent> {
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut events = Vec::new();
        while Instant::now() < deadline {
            match port.recv_event_timeout(Duration::from_millis(100)) {
                Ok(event) => {
                    let done = terminal(&event);
                    events.push(event);
                    if done {
                        return events;
                    }
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        panic!("deterministic fake did not emit the expected terminal event: {events:?}");
    }

    fn stored_turn_state(store: &Store, turn_id: Uuid) -> LiveTurnState {
        let status: String = store
            .conn
            .query_row(
                "SELECT status FROM turns WHERE turn_id = ?1",
                [turn_id.to_string()],
                |row| row.get(0),
            )
            .expect("load turn state");
        LiveTurnState::parse(&status).expect("stored normalized state")
    }

    #[test]
    fn provider_dispatch_failure_is_indeterminate_after_provider_activity() {
        let turn_id = Uuid::new_v4();
        let thread_id = Uuid::new_v4();
        assert!(!dispatch_failure_side_effect_possible(
            &ProviderCommand::Start {
                turn_id,
                thread_id,
                worktree_path: PathBuf::from("fixture"),
                prompt: "fixture".into(),
                session: None,
            }
        ));
        for command in [
            ProviderCommand::ApprovalResponse {
                turn_id,
                interaction_id: "approval".into(),
                decision: ApprovalDecision::Approve,
            },
            ProviderCommand::UserInputResponse {
                turn_id,
                interaction_id: "input".into(),
                answers: vec![UserInputAnswer {
                    question_id: "scope".into(),
                    answer: "workspace".into(),
                }],
            },
            ProviderCommand::Interrupt { turn_id },
        ] {
            assert!(
                dispatch_failure_side_effect_possible(&command),
                "{command:?} must not be downgraded to a safe pre-start failure"
            );
        }
    }

    #[test]
    fn fake_normal_stream_and_completion_is_coalesced_and_terminal() {
        let root = TestRoot::new("fake-normal-stream");
        let source = create_git_fixture(root.path());
        let mut store = Store::open(root.path().join("state.sqlite"), root.path().to_path_buf())
            .expect("open store");
        let (_, thread_id, _) =
            create_codex_thread_with_worktree(&mut store, &source, "Fake normal");
        let (turn_id, port) =
            start_fake_turn(&mut store, thread_id, FakeScript::NormalStreamAndCompletion);
        let events = receive_fake_events_until(&port, |event| {
            matches!(event, ProviderEvent::Completed { .. })
        });
        store
            .apply_provider_events(&events)
            .expect("apply normal fake script in one bounded batch");

        assert_eq!(stored_turn_state(&store, turn_id), LiveTurnState::Completed);
        let messages = store.timeline_page(thread_id, 100).expect("load timeline");
        assert_eq!(
            messages
                .iter()
                .find(|message| message.kind == TimelineRecordKind::Assistant)
                .map(|message| message.body.as_str()),
            Some("hello world")
        );
        let (output_receipts, output_transactions): (i64, i64) = store
            .conn
            .query_row(
                "SELECT COUNT(*), COUNT(DISTINCT applied_sequence)
                 FROM provider_event_receipts
                 WHERE turn_id = ?1 AND event_type = 'assistant_output'",
                [turn_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("measure coalesced output");
        assert_eq!(output_receipts, 2);
        assert_eq!(
            output_transactions, 1,
            "multiple provider deltas share one durable output event/transaction"
        );
    }

    #[test]
    fn fake_approval_request_requires_exact_durable_response() {
        let root = TestRoot::new("fake-approval");
        let source = create_git_fixture(root.path());
        let mut store = Store::open(root.path().join("state.sqlite"), root.path().to_path_buf())
            .expect("open store");
        let (_, thread_id, _) =
            create_codex_thread_with_worktree(&mut store, &source, "Fake approval");
        let (turn_id, port) = start_fake_turn(
            &mut store,
            thread_id,
            FakeScript::ApprovalRequestAndResponse,
        );
        let request_events = receive_fake_events_until(&port, |event| {
            matches!(event, ProviderEvent::ApprovalRequested { .. })
        });
        store
            .apply_provider_events(&request_events)
            .expect("persist approval request");
        assert_eq!(
            stored_turn_state(&store, turn_id),
            LiveTurnState::AwaitingApproval
        );

        let event_count_before: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        let (rejected, provider_command) = store
            .execute_with_provider_command(CommandEnvelope::new(Command::ApprovalRespond {
                turn_id,
                thread_id,
                interaction_id: "wrong-approval".into(),
                decision: ApprovalDecision::Approve,
            }))
            .expect("invalid response is a durable rejection");
        assert_eq!(rejected.status, "rejected");
        assert!(provider_command.is_none());
        let event_count_after: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(event_count_after, event_count_before);

        let (accepted, provider_command) = store
            .execute_with_provider_command(CommandEnvelope::new(Command::ApprovalRespond {
                turn_id,
                thread_id,
                interaction_id: "approval-1".into(),
                decision: ApprovalDecision::Approve,
            }))
            .expect("persist matching approval response");
        assert_eq!(accepted.status, "succeeded");
        port.command_sender()
            .send(provider_command.expect("response dispatches after commit"))
            .unwrap();
        let terminal = receive_fake_events_until(&port, |event| {
            matches!(event, ProviderEvent::Completed { .. })
        });
        store.apply_provider_events(&terminal).unwrap();
        assert_eq!(stored_turn_state(&store, turn_id), LiveTurnState::Completed);
        let interaction_status: String = store
            .conn
            .query_row(
                "SELECT status FROM turn_interactions WHERE interaction_id = 'approval-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(interaction_status, "responded");
        let projected_status = store
            .bootstrap_snapshot()
            .unwrap()
            .actors
            .into_iter()
            .find(|actor| actor.thread_id == thread_id)
            .and_then(|actor| actor.live_turn)
            .and_then(|turn| turn.interaction)
            .map(|interaction| interaction.status);
        assert_eq!(projected_status, Some(InteractionStatus::Responded));
    }

    #[test]
    fn fake_user_input_request_requires_all_pending_questions() {
        let root = TestRoot::new("fake-user-input");
        let source = create_git_fixture(root.path());
        let mut store = Store::open(root.path().join("state.sqlite"), root.path().to_path_buf())
            .expect("open store");
        let (_, thread_id, _) =
            create_codex_thread_with_worktree(&mut store, &source, "Fake input");
        let (turn_id, port) = start_fake_turn(
            &mut store,
            thread_id,
            FakeScript::UserInputRequestAndResponse,
        );
        let request_events = receive_fake_events_until(&port, |event| {
            matches!(event, ProviderEvent::UserInputRequested { .. })
        });
        store.apply_provider_events(&request_events).unwrap();
        assert_eq!(
            stored_turn_state(&store, turn_id),
            LiveTurnState::AwaitingUserInput
        );

        let (rejected, provider_command) = store
            .execute_with_provider_command(CommandEnvelope::new(Command::UserInputRespond {
                turn_id,
                thread_id,
                interaction_id: "input-1".into(),
                answers: vec![UserInputAnswer {
                    question_id: "other".into(),
                    answer: "core".into(),
                }],
            }))
            .expect("wrong question set is durably rejected");
        assert_eq!(rejected.status, "rejected");
        assert!(provider_command.is_none());

        let (_, provider_command) = store
            .execute_with_provider_command(CommandEnvelope::new(Command::UserInputRespond {
                turn_id,
                thread_id,
                interaction_id: "input-1".into(),
                answers: vec![UserInputAnswer {
                    question_id: "target".into(),
                    answer: "core".into(),
                }],
            }))
            .expect("matching response persists");
        port.command_sender()
            .send(provider_command.expect("input response dispatches once"))
            .unwrap();
        let terminal = receive_fake_events_until(&port, |event| {
            matches!(event, ProviderEvent::Completed { .. })
        });
        store.apply_provider_events(&terminal).unwrap();
        assert_eq!(stored_turn_state(&store, turn_id), LiveTurnState::Completed);
    }

    #[test]
    fn fake_interrupt_during_streaming_requires_terminal_acknowledgement() {
        let root = TestRoot::new("fake-interrupt");
        let source = create_git_fixture(root.path());
        let mut store = Store::open(root.path().join("state.sqlite"), root.path().to_path_buf())
            .expect("open store");
        let (_, thread_id, _) =
            create_codex_thread_with_worktree(&mut store, &source, "Fake interrupt");
        let (turn_id, port) =
            start_fake_turn(&mut store, thread_id, FakeScript::InterruptDuringStreaming);
        let streaming = receive_fake_events_until(&port, |event| {
            matches!(event, ProviderEvent::AssistantOutput { .. })
        });
        store.apply_provider_events(&streaming).unwrap();
        let (_, provider_command) = store
            .execute_with_provider_command(CommandEnvelope::new(Command::LiveTurnInterrupt {
                turn_id,
                thread_id,
            }))
            .expect("persist interrupt intent");
        assert_eq!(
            stored_turn_state(&store, turn_id),
            LiveTurnState::Interrupting
        );
        port.command_sender()
            .send(provider_command.expect("interrupt dispatches after commit"))
            .unwrap();
        let acknowledgement = receive_fake_events_until(&port, |event| {
            matches!(event, ProviderEvent::InterruptAcknowledged { .. })
        });
        store.apply_provider_events(&acknowledgement).unwrap();
        assert_eq!(stored_turn_state(&store, turn_id), LiveTurnState::Failed);
        let actor = store
            .bootstrap_snapshot()
            .unwrap()
            .actors
            .into_iter()
            .find(|actor| actor.thread_id == thread_id)
            .unwrap();
        assert_eq!(actor.state, ActorState::Stopped);
        assert!(!actor.live_turn.unwrap().interruptible);
    }

    #[test]
    fn fake_duplicate_provider_event_is_applied_exactly_once() {
        let root = TestRoot::new("fake-duplicate");
        let source = create_git_fixture(root.path());
        let mut store = Store::open(root.path().join("state.sqlite"), root.path().to_path_buf())
            .expect("open store");
        let (_, thread_id, _) =
            create_codex_thread_with_worktree(&mut store, &source, "Fake duplicate");
        let (turn_id, port) =
            start_fake_turn(&mut store, thread_id, FakeScript::DuplicateProviderEvent);
        let events = receive_fake_events_until(&port, |event| {
            matches!(event, ProviderEvent::Completed { .. })
        });
        store.apply_provider_events(&events).unwrap();
        let messages = store.timeline_page(thread_id, 100).unwrap();
        assert_eq!(
            messages
                .iter()
                .find(|message| message.kind == TimelineRecordKind::Assistant)
                .map(|message| message.body.as_str()),
            Some("once")
        );
        let duplicate_receipts: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM provider_event_receipts
                 WHERE turn_id = ?1 AND provider_event_id = 'duplicate-output'",
                [turn_id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(duplicate_receipts, 1);
    }

    #[test]
    fn fake_process_loss_before_start_acknowledgement_is_failed_not_retried() {
        let root = TestRoot::new("fake-loss-before-start");
        let source = create_git_fixture(root.path());
        let mut store = Store::open(root.path().join("state.sqlite"), root.path().to_path_buf())
            .expect("open store");
        let (_, thread_id, _) =
            create_codex_thread_with_worktree(&mut store, &source, "Fake loss before");
        let (turn_id, port) = start_fake_turn(
            &mut store,
            thread_id,
            FakeScript::ProcessLossBeforeStartAcknowledgement,
        );
        let events = receive_fake_events_until(&port, |event| {
            matches!(event, ProviderEvent::ProcessLost { .. })
        });
        store.apply_provider_events(&events).unwrap();
        assert_eq!(stored_turn_state(&store, turn_id), LiveTurnState::Failed);
        let started_sequence: Option<i64> = store
            .conn
            .query_row(
                "SELECT started_sequence FROM turns WHERE turn_id = ?1",
                [turn_id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert!(started_sequence.is_none());
    }

    #[test]
    fn fake_process_loss_after_output_is_indeterminate_and_never_replayed() {
        let root = TestRoot::new("fake-loss-after-output");
        let source = create_git_fixture(root.path());
        let mut store = Store::open(root.path().join("state.sqlite"), root.path().to_path_buf())
            .expect("open store");
        let (_, thread_id, _) =
            create_codex_thread_with_worktree(&mut store, &source, "Fake loss after");
        let (turn_id, port) = start_fake_turn(
            &mut store,
            thread_id,
            FakeScript::ProcessLossAfterOutputBeforeTerminal,
        );
        let events = receive_fake_events_until(&port, |event| {
            matches!(event, ProviderEvent::ProcessLost { .. })
        });
        store.apply_provider_events(&events).unwrap();
        assert_eq!(
            stored_turn_state(&store, turn_id),
            LiveTurnState::Indeterminate
        );
        assert!(store.reconcile_unfinished_turns().unwrap().is_empty());
        let active: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM turns WHERE status IN (
                    'accepted', 'starting', 'streaming', 'awaiting_approval',
                    'awaiting_user_input', 'interrupting'
                )",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(active, 0);
    }

    #[test]
    fn fake_restart_and_resume_uses_the_durable_session_cursor() {
        let root = TestRoot::new("fake-restart-resume");
        let source = create_git_fixture(root.path());
        let db_path = root.path().join("state.sqlite");
        let thread_id;
        {
            let mut store =
                Store::open(db_path.clone(), root.path().to_path_buf()).expect("open first store");
            (_, thread_id, _) =
                create_codex_thread_with_worktree(&mut store, &source, "Fake resume");
            let (turn_id, port) =
                start_fake_turn(&mut store, thread_id, FakeScript::NormalStreamAndCompletion);
            let events = receive_fake_events_until(&port, |event| {
                matches!(event, ProviderEvent::Completed { .. })
            });
            store.apply_provider_events(&events).unwrap();
            assert_eq!(stored_turn_state(&store, turn_id), LiveTurnState::Completed);
        }

        let mut reopened = Store::open(db_path, root.path().to_path_buf()).expect("restart store");
        let (resumed_turn_id, port) =
            start_fake_turn(&mut reopened, thread_id, FakeScript::RestartAndResume);
        let events = receive_fake_events_until(&port, |event| {
            matches!(event, ProviderEvent::Completed { .. })
        });
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ProviderEvent::Resumed { .. }))
        );
        reopened.apply_provider_events(&events).unwrap();
        assert_eq!(
            stored_turn_state(&reopened, resumed_turn_id),
            LiveTurnState::Completed
        );
        let session: (String, String) = reopened
            .conn
            .query_row(
                "SELECT provider_session_id, resume_cursor FROM turns WHERE turn_id = ?1",
                [resumed_turn_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            session,
            ("fake-session".into(), "cursor-after-resume".into())
        );
    }

    #[test]
    fn provider_readiness_gates_bootstrap_and_rejects_before_durable_acceptance() {
        for (label, readiness, expected_gate, expected_error) in [
            (
                "unavailable",
                ProviderReadiness::Unavailable {
                    diagnostic: "provider executable is missing".into(),
                },
                LiveTurnStartGate::ProviderUnavailable,
                "provider unavailable",
            ),
            (
                "unsupported",
                ProviderReadiness::UnsupportedVersion {
                    installed: "provider-cli 9.9.9".into(),
                    supported: "provider-cli 1.0.0".into(),
                },
                LiveTurnStartGate::UnsupportedVersion,
                "unsupported provider version",
            ),
        ] {
            let root = TestRoot::new(&format!("readiness-{label}"));
            let source = create_git_fixture(root.path());
            let mut store =
                Store::open(root.path().join("state.sqlite"), root.path().to_path_buf())
                    .expect("open store");
            let (_, thread_id, _) =
                create_codex_thread_with_worktree(&mut store, &source, "Readiness");
            readiness.validate().expect("bounded readiness");
            store.provider_readiness = readiness;

            let actor = store
                .bootstrap_snapshot()
                .expect("bootstrap readiness")
                .actors
                .into_iter()
                .find(|actor| actor.thread_id == thread_id)
                .expect("readiness actor");
            assert_eq!(actor.start_gate, expected_gate);

            let events_before: i64 = store
                .conn
                .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
                .unwrap();
            let (receipt, provider_command) = store
                .execute_with_provider_command(CommandEnvelope::new(Command::LiveTurnStart {
                    turn_id: Uuid::new_v4(),
                    thread_id,
                    text: "must fail before provider dispatch".into(),
                }))
                .expect("readiness rejection is durable");
            assert_eq!(receipt.status, "rejected");
            assert!(provider_command.is_none());
            assert!(
                receipt.result["error"]
                    .as_str()
                    .unwrap()
                    .contains(expected_error)
            );
            let events_after: i64 = store
                .conn
                .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
                .unwrap();
            assert_eq!(events_after, events_before);
            let turns: i64 = store
                .conn
                .query_row("SELECT COUNT(*) FROM turns", [], |row| row.get(0))
                .unwrap();
            assert_eq!(turns, 0);
        }
    }

    #[test]
    fn core_handle_consumes_normalized_fake_events_through_the_bounded_port() {
        let root = TestRoot::new("core-handle-fake-port");
        let source = create_git_fixture(root.path());
        let thread_id;
        {
            let mut store =
                Store::open(root.path().join("state.sqlite"), root.path().to_path_buf())
                    .expect("open setup store");
            (_, thread_id, _) = create_codex_thread_with_worktree(&mut store, &source, "Core port");
        }
        let handle = CoreHandle::spawn(
            root.path().to_path_buf(),
            || {},
            Box::new(DeterministicFakeRunner::new(
                FakeScript::NormalStreamAndCompletion,
            )),
        )
        .expect("spawn core with provider-neutral fake");
        let turn_id = Uuid::new_v4();
        handle
            .command(Command::LiveTurnStart {
                turn_id,
                thread_id,
                text: "exercise the bounded provider port".into(),
            })
            .expect("queue live turn command");
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut completed = false;
        while Instant::now() < deadline {
            match handle.rx.recv_timeout(Duration::from_millis(100)) {
                Ok(CoreEvent::TurnChanged { status, .. }) if status == "completed" => {
                    completed = true;
                    break;
                }
                Ok(CoreEvent::Error(error)) => panic!("core/provider port failed: {error}"),
                Ok(_) | Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        assert!(
            completed,
            "normalized fake never reached durable completion"
        );
        let (status, output): (String, String) = Connection::open(root.path().join("state.sqlite"))
            .unwrap()
            .query_row(
                "SELECT tr.status, m.body
                 FROM turns tr JOIN messages m ON m.sequence = tr.assistant_message_sequence
                 WHERE tr.turn_id = ?1",
                [turn_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "completed");
        assert_eq!(output, "hello world");
        drop(handle);
        RuntimeLease::acquire(root.path()).expect("clean port shutdown releases the lease");
    }

    #[test]
    fn durable_core_and_worktree_smoke() {
        let result = super::self_check().expect("self-check");
        assert_eq!(result["sqlite_idempotency"], true);
        assert_eq!(result["native_git_worktree"], true);
        assert_eq!(result["foreign_keys_valid"], true);
        assert_eq!(result["invalid_mutation_rejected_without_event"], true);
        assert_eq!(result["duplicate_worktree_rejected"], true);
    }

    #[test]
    fn runtime_lease_rejects_contention_and_can_be_reacquired_after_release() {
        let root = TestRoot::new("runtime-lease");
        let first = RuntimeLease::acquire(root.path()).expect("acquire first runtime lease");
        let error = match RuntimeLease::acquire(root.path()) {
            Ok(_) => panic!("second runtime owner was accepted"),
            Err(error) => error,
        };
        assert!(error.contains("already owned by another process"));
        drop(first);
        RuntimeLease::acquire(root.path()).expect("lease can be reacquired after owner exits");
    }

    #[test]
    fn contending_core_cannot_reconcile_another_owners_unfinished_turn() {
        let root = TestRoot::new("runtime-lease-recovery");
        let source = create_git_fixture(root.path());
        let db_path = root.path().join("state.sqlite");
        let turn_id = Uuid::new_v4();
        {
            let mut store =
                Store::open(db_path.clone(), root.path().to_path_buf()).expect("open store");
            let (_, thread_id, _) =
                create_codex_thread_with_worktree(&mut store, &source, "Lease recovery");
            let (_, plan) = store
                .execute_with_provider_command(CommandEnvelope::new(Command::LiveTurnStart {
                    turn_id,
                    thread_id,
                    text: "remain active for lease fixture".into(),
                }))
                .expect("queue unfinished turn");
            assert!(plan.is_some());
        }

        let first_owner = RuntimeLease::acquire(root.path()).expect("hold runtime as first owner");
        let error = match CoreHandle::spawn(root.path().to_path_buf(), || {}, idle_test_runner()) {
            Ok(_) => panic!("contending core unexpectedly started"),
            Err(error) => error,
        };
        assert!(error.contains("already owned by another process"));
        let status: String = Connection::open(&db_path)
            .expect("open read connection")
            .query_row(
                "SELECT status FROM turns WHERE turn_id = ?1",
                [turn_id.to_string()],
                |row| row.get(0),
            )
            .expect("load untouched turn");
        assert_eq!(status, "accepted", "contender must not run recovery");

        drop(first_owner);
        let owner = CoreHandle::spawn(root.path().to_path_buf(), || {}, idle_test_runner())
            .expect("new owner starts after lease release");
        let status: String = Connection::open(&db_path)
            .expect("open post-recovery connection")
            .query_row(
                "SELECT status FROM turns WHERE turn_id = ?1",
                [turn_id.to_string()],
                |row| row.get(0),
            )
            .expect("load recovered turn");
        assert_eq!(status, "indeterminate");
        drop(owner);
        RuntimeLease::acquire(root.path()).expect("core drop releases runtime lease");
    }

    #[test]
    fn isolated_workspace_write_codex_turn_is_admitted_once_and_records_terminal_answer() {
        let root = TestRoot::new("codex-turn-success");
        let source = create_git_fixture(root.path());
        let db_path = root.path().join("state.sqlite");
        let mut store = Store::open(db_path, root.path().to_path_buf()).expect("open store");
        let (_, thread_id, worktree_path) =
            create_codex_thread_with_worktree(&mut store, &source, "Codex success");
        let turn_id = Uuid::new_v4();
        let envelope = CommandEnvelope {
            protocol_version: PROTOCOL_VERSION,
            command_id: Uuid::new_v4(),
            expected_aggregate_version: None,
            command: Command::LiveTurnStart {
                turn_id,
                thread_id,
                text: "Summarize this repository".into(),
            },
        };

        let events_before = store
            .conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("count events before turn");
        let (receipt, plan) = store
            .execute_with_provider_command(envelope.clone())
            .expect("admit turn");
        let plan = plan.expect("fresh turn has runtime plan");
        assert_eq!(receipt.status, "succeeded");
        let ProviderCommand::Start {
            turn_id: planned_turn,
            worktree_path: planned_worktree,
            prompt,
            ..
        } = plan
        else {
            panic!("live turn did not produce a start command");
        };
        assert_eq!(planned_turn, turn_id);
        assert_eq!(planned_worktree, worktree_path);
        assert_eq!(prompt, "Summarize this repository");

        let (replay, replay_plan) = store
            .execute_with_provider_command(envelope.clone())
            .expect("replay accepted turn");
        assert_eq!(replay.command_id, receipt.command_id);
        assert!(
            replay_plan.is_none(),
            "receipt replay must not relaunch Codex"
        );
        let events_after_replay = store
            .conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("count events after replay");
        assert_eq!(events_after_replay, events_before + 1);
        let mut altered = envelope;
        altered.command = Command::LiveTurnStart {
            turn_id,
            thread_id,
            text: "altered payload under the same command id".into(),
        };
        assert!(
            store
                .execute_with_provider_command(altered)
                .expect_err("altered command payload must conflict")
                .contains("idempotency conflict")
        );

        store
            .apply_provider_events(&[ProviderEvent::Starting {
                turn_id,
                provider_event_id: "turn-starting".into(),
            }])
            .expect("record durable start");
        let session_id = "0199a213-81c0-7800-8aa1-bbab2a035a53";
        let session_event = ProviderEvent::SessionEstablished {
            turn_id,
            provider_event_id: "turn-session".into(),
            session: ProviderSessionCursor {
                session_id: session_id.into(),
                resume_cursor: "cursor-1".into(),
            },
        };
        let events_before_session: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .expect("count events before session");
        store
            .apply_provider_events(std::slice::from_ref(&session_event))
            .expect("record observed Codex thread id");
        store
            .apply_provider_events(std::slice::from_ref(&session_event))
            .expect("same session replay is idempotent");
        let events_after_session_replay: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .expect("count events after session replay");
        assert_eq!(events_after_session_replay, events_before_session + 1);
        let changed_session_event = ProviderEvent::SessionEstablished {
            turn_id,
            provider_event_id: "turn-session".into(),
            session: ProviderSessionCursor {
                session_id: "changed-session".into(),
                resume_cursor: "cursor-1".into(),
            },
        };
        assert!(
            store
                .apply_provider_events(&[changed_session_event])
                .expect_err("changed session must fail")
                .contains("provider-event idempotency conflict")
        );
        store
            .apply_provider_events(&[
                ProviderEvent::AssistantOutput {
                    turn_id,
                    provider_event_id: "turn-output".into(),
                    delta: "The repository is a native Windows control room.".into(),
                    resume_cursor: Some("cursor-2".into()),
                },
                ProviderEvent::Completed {
                    turn_id,
                    provider_event_id: "turn-completed".into(),
                    session: ProviderSessionCursor {
                        session_id: session_id.into(),
                        resume_cursor: "cursor-terminal".into(),
                    },
                },
            ])
            .expect("record durable completion");

        let turn: (String, String, String, Option<i64>, Option<i64>) = store
            .conn
            .query_row(
                "SELECT status, policy, provider_session_id, started_sequence, terminal_sequence
                 FROM turns WHERE turn_id = ?1",
                [turn_id.to_string()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .expect("load terminal turn");
        assert_eq!(turn.0, "completed");
        assert_eq!(turn.1, ISOLATED_WORKSPACE_WRITE_POLICY);
        assert_eq!(turn.2, session_id);
        assert!(turn.3.is_some());
        assert!(turn.4.is_some());
        let messages = store
            .timeline_page(thread_id, 10)
            .expect("load durable turn messages");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].role, "user");
        assert_eq!(messages[1].role, "assistant");
        assert_eq!(
            messages[1].body,
            "The repository is a native Windows control room."
        );
        let actor = store
            .bootstrap_snapshot()
            .expect("load completed actor")
            .actors
            .into_iter()
            .find(|actor| actor.thread_id == thread_id)
            .expect("completed actor present");
        assert_eq!(actor.state, ActorState::WaitingUser);
        assert!(actor.attention);
        assert_eq!(actor.unread_count, 1);
    }

    #[test]
    fn one_global_turn_slot_rejects_a_second_operator_without_launch_plan() {
        let root = TestRoot::new("codex-turn-admission");
        let source = create_git_fixture(root.path());
        let db_path = root.path().join("state.sqlite");
        let mut store = Store::open(db_path, root.path().to_path_buf()).expect("open store");
        let (_, first_thread, _) =
            create_codex_thread_with_worktree(&mut store, &source, "Codex first");
        let (_, second_thread, _) =
            create_codex_thread_with_worktree(&mut store, &source, "Codex second");
        let first_turn = Uuid::new_v4();
        let (_, first_plan) = store
            .execute_with_provider_command(CommandEnvelope::new(Command::LiveTurnStart {
                turn_id: first_turn,
                thread_id: first_thread,
                text: "first".into(),
            }))
            .expect("admit first turn");
        assert!(first_plan.is_some());

        let (second_receipt, second_plan) = store
            .execute_with_provider_command(CommandEnvelope::new(Command::LiveTurnStart {
                turn_id: Uuid::new_v4(),
                thread_id: second_thread,
                text: "second".into(),
            }))
            .expect("durably reject second turn");
        assert_eq!(second_receipt.status, "rejected");
        assert!(second_plan.is_none());
        assert!(
            second_receipt.result["error"]
                .as_str()
                .is_some_and(|error| error.contains("single live-provider slot"))
        );
        let turn_count: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM turns", [], |row| row.get(0))
            .expect("count admitted turns");
        assert_eq!(turn_count, 1);
    }

    #[test]
    fn turn_admission_uses_an_explicit_state_allowlist() {
        for state in ["idle", "waiting_user", "stopped", "failed", "indeterminate"] {
            assert!(
                turn_state_allows_new_turn(state),
                "{state} should allow retry"
            );
        }
        for state in [
            "starting",
            "running",
            "awaiting_approval",
            "interrupting",
            "archived",
            "future_state",
        ] {
            assert!(
                !turn_state_allows_new_turn(state),
                "{state} must fail closed"
            );
        }

        let root = TestRoot::new("codex-turn-state-allowlist");
        let source = create_git_fixture(root.path());
        let mut store = Store::open(root.path().join("state.sqlite"), root.path().to_path_buf())
            .expect("open store");
        let (_, thread_id, _) =
            create_codex_thread_with_worktree(&mut store, &source, "State allowlist");
        for state in ["awaiting_approval", "interrupting"] {
            store
                .conn
                .execute(
                    "UPDATE threads SET state = ?1 WHERE thread_id = ?2",
                    params![state, thread_id.to_string()],
                )
                .expect("set legacy state fixture");
            let (receipt, plan) = store
                .execute_with_provider_command(CommandEnvelope::new(Command::LiveTurnStart {
                    turn_id: Uuid::new_v4(),
                    thread_id,
                    text: format!("reject {state}"),
                }))
                .expect("durably reject unresolved legacy state");
            assert_eq!(receipt.status, "rejected");
            assert!(plan.is_none());
            assert!(receipt.result["error"].as_str().unwrap().contains(state));
        }
    }

    #[test]
    fn turn_admission_revalidates_durable_git_identity() {
        let root = TestRoot::new("codex-turn-git-revalidation");
        let source = create_git_fixture(root.path());
        let mut store = Store::open(root.path().join("state.sqlite"), root.path().to_path_buf())
            .expect("open store");
        let (_, thread_id, worktree_path) =
            create_codex_thread_with_worktree(&mut store, &source, "Git revalidation");
        let output = ProcessCommand::new("git")
            .arg("-C")
            .arg(&worktree_path)
            .args(["checkout", "--detach"])
            .output()
            .expect("detach fixture worktree");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );

        let (receipt, plan) = store
            .execute_with_provider_command(CommandEnvelope::new(Command::LiveTurnStart {
                turn_id: Uuid::new_v4(),
                thread_id,
                text: "must not run in a changed worktree".into(),
            }))
            .expect("durably reject changed worktree");
        assert_eq!(receipt.status, "rejected");
        assert!(plan.is_none());
        assert!(
            receipt.result["error"]
                .as_str()
                .is_some_and(|error| error.contains("admission verification"))
        );
        let turns: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM turns", [], |row| row.get(0))
            .expect("count unlaunched turns");
        assert_eq!(turns, 0);
    }

    #[cfg(unix)]
    #[test]
    fn turn_admission_rejects_worktree_path_redirected_to_shared_source() {
        use std::os::unix::fs::symlink;

        let root = TestRoot::new("codex-turn-worktree-redirect");
        let source = create_git_fixture(root.path());
        let mut store = Store::open(root.path().join("state.sqlite"), root.path().to_path_buf())
            .expect("open store");
        let (_, thread_id, worktree_path) =
            create_codex_thread_with_worktree(&mut store, &source, "Redirect revalidation");
        let displaced = worktree_path.with_extension("displaced");
        fs::rename(&worktree_path, &displaced).expect("displace verified worktree");
        symlink(&source, &worktree_path).expect("redirect saved path to shared source");

        let (receipt, plan) = store
            .execute_with_provider_command(CommandEnvelope::new(Command::LiveTurnStart {
                turn_id: Uuid::new_v4(),
                thread_id,
                text: "must not run in shared source".into(),
            }))
            .expect("durably reject redirected path");
        assert_eq!(receipt.status, "rejected");
        assert!(plan.is_none());
        assert!(
            receipt.result["error"]
                .as_str()
                .is_some_and(|error| error.contains("resolves through a redirect"))
        );
    }

    #[test]
    fn active_turn_cannot_be_archived() {
        let root = TestRoot::new("codex-active-turn-archive");
        let source = create_git_fixture(root.path());
        let mut store = Store::open(root.path().join("state.sqlite"), root.path().to_path_buf())
            .expect("open store");
        let (_, thread_id, _) =
            create_codex_thread_with_worktree(&mut store, &source, "Archive guard");
        let turn_id = Uuid::new_v4();
        let (_, plan) = store
            .execute_with_provider_command(CommandEnvelope::new(Command::LiveTurnStart {
                turn_id,
                thread_id,
                text: "stay visible".into(),
            }))
            .expect("admit active turn");
        assert!(plan.is_some());

        let receipt = store
            .execute(CommandEnvelope::new(Command::ThreadArchive { thread_id }))
            .expect("durably reject archive");
        assert_eq!(receipt.status, "rejected");
        let (state, status): (String, String) = store
            .conn
            .query_row(
                "SELECT th.state, tr.status
                 FROM threads th JOIN turns tr ON tr.thread_id = th.thread_id
                 WHERE th.thread_id = ?1 AND tr.turn_id = ?2",
                params![thread_id.to_string(), turn_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("load guarded active turn");
        assert_eq!(state, "starting");
        assert_eq!(status, "accepted");
    }

    #[test]
    fn restart_marks_unfinished_turn_indeterminate_without_replay() {
        let root = TestRoot::new("codex-turn-restart");
        let source = create_git_fixture(root.path());
        let db_path = root.path().join("state.sqlite");
        let turn_id = Uuid::new_v4();
        let thread_id;
        {
            let mut store =
                Store::open(db_path.clone(), root.path().to_path_buf()).expect("open first store");
            (_, thread_id, _) =
                create_codex_thread_with_worktree(&mut store, &source, "Codex restart");
            let (_, plan) = store
                .execute_with_provider_command(CommandEnvelope::new(Command::LiveTurnStart {
                    turn_id,
                    thread_id,
                    text: "do not replay me".into(),
                }))
                .expect("queue turn before simulated restart");
            assert!(plan.is_some());
            store
                .apply_provider_events(&[
                    ProviderEvent::Starting {
                        turn_id,
                        provider_event_id: "restart-starting".into(),
                    },
                    ProviderEvent::SessionEstablished {
                        turn_id,
                        provider_event_id: "restart-session".into(),
                        session: ProviderSessionCursor {
                            session_id: "restart-session".into(),
                            resume_cursor: "restart-cursor".into(),
                        },
                    },
                ])
                .expect("record session before simulated restart");
        }

        let mut reopened = Store::open(db_path, root.path().to_path_buf()).expect("reopen store");
        let warnings = reopened
            .reconcile_unfinished_turns()
            .expect("reconcile unfinished turn");
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("was not replayed"));
        let turn: (String, Option<String>) = reopened
            .conn
            .query_row(
                "SELECT status, provider_session_id FROM turns WHERE turn_id = ?1",
                [turn_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("load reconciled status");
        assert_eq!(turn.0, "indeterminate");
        assert_eq!(turn.1.as_deref(), Some("restart-session"));
        let actor = reopened
            .bootstrap_snapshot()
            .expect("load reconciled actor")
            .actors
            .into_iter()
            .find(|actor| actor.thread_id == thread_id)
            .expect("reconciled actor present");
        assert_eq!(actor.state, ActorState::Indeterminate);
        assert!(actor.attention);
        let messages = reopened
            .timeline_page(thread_id, 10)
            .expect("load reconciled messages");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[1].role, "system");
        assert!(messages[1].body.contains("was not replayed"));
        assert!(reopened.reconcile_unfinished_turns().unwrap().is_empty());
    }

    #[test]
    fn runtime_failure_is_terminal_visible_and_releases_admission_slot() {
        let root = TestRoot::new("codex-turn-failure");
        let source = create_git_fixture(root.path());
        let db_path = root.path().join("state.sqlite");
        let mut store = Store::open(db_path, root.path().to_path_buf()).expect("open store");
        let (_, thread_id, _) =
            create_codex_thread_with_worktree(&mut store, &source, "Codex failure");
        let turn_id = Uuid::new_v4();
        let (_, plan) = store
            .execute_with_provider_command(CommandEnvelope::new(Command::LiveTurnStart {
                turn_id,
                thread_id,
                text: "fail safely".into(),
            }))
            .expect("admit failing turn");
        assert!(plan.is_some());
        store
            .apply_provider_events(&[
                ProviderEvent::Starting {
                    turn_id,
                    provider_event_id: "failure-starting".into(),
                },
                ProviderEvent::SessionEstablished {
                    turn_id,
                    provider_event_id: "failure-session".into(),
                    session: ProviderSessionCursor {
                        session_id: "failure-session".into(),
                        resume_cursor: "failure-cursor".into(),
                    },
                },
                ProviderEvent::Failed {
                    turn_id,
                    provider_event_id: "failure-terminal".into(),
                    diagnostic: "fixture authentication failure".into(),
                },
            ])
            .expect("record runtime failure");
        let messages = store
            .timeline_page(thread_id, 10)
            .expect("load failed messages");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[1].role, "system");
        assert!(messages[1].body.contains("authentication failure"));
        let turn: (String, Option<String>) = store
            .conn
            .query_row(
                "SELECT status, provider_session_id FROM turns WHERE turn_id = ?1",
                [turn_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("load failed turn");
        assert_eq!(turn.0, "failed");
        assert_eq!(turn.1.as_deref(), Some("failure-session"));

        let (_, retry_plan) = store
            .execute_with_provider_command(CommandEnvelope::new(Command::LiveTurnStart {
                turn_id: Uuid::new_v4(),
                thread_id,
                text: "retry explicitly".into(),
            }))
            .expect("admit explicit retry");
        assert!(retry_plan.is_some());
    }

    #[test]
    fn terminal_failure_truncates_diagnostics_without_losing_the_terminal_record() {
        let root = TestRoot::new("codex-turn-large-failure");
        let source = create_git_fixture(root.path());
        let db_path = root.path().join("state.sqlite");
        let mut store = Store::open(db_path, root.path().to_path_buf()).expect("open store");
        let (_, thread_id, _) =
            create_codex_thread_with_worktree(&mut store, &source, "Codex large failure");

        for (index, diagnostic) in [
            "x".repeat(MAX_TURN_ERROR_BYTES),
            "é".repeat(MAX_TURN_ERROR_BYTES / 2),
        ]
        .into_iter()
        .enumerate()
        {
            let turn_id = Uuid::new_v4();
            let (_, plan) = store
                .execute_with_provider_command(CommandEnvelope::new(Command::LiveTurnStart {
                    turn_id,
                    thread_id,
                    text: format!("failure fixture {index}"),
                }))
                .expect("admit failure fixture");
            assert!(plan.is_some(), "prior failure must release the global slot");
            store
                .apply_provider_events(&[ProviderEvent::Failed {
                    turn_id,
                    provider_event_id: format!("bounded-failure-{index}"),
                    diagnostic,
                }])
                .expect("oversized diagnostics must still become terminal");
            let (status, stored_error, error_bytes, message_bytes): (String, String, i64, i64) =
                store
                    .conn
                    .query_row(
                        "SELECT tr.status, tr.error, length(CAST(tr.error AS BLOB)),
                            length(CAST(m.body AS BLOB))
                     FROM turns tr
                     JOIN messages m ON m.sequence = tr.terminal_sequence
                     WHERE tr.turn_id = ?1",
                        [turn_id.to_string()],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                    )
                    .expect("load bounded terminal diagnostic");
            assert_eq!(status, "failed");
            assert!(error_bytes <= MAX_TURN_ERROR_BYTES as i64);
            assert!(message_bytes <= MAX_TURN_ERROR_BYTES as i64);
            assert!(!stored_error.is_empty());
        }
    }

    #[test]
    fn startup_rejects_missing_or_malformed_global_active_index() {
        for (suffix, replacement) in [
            ("missing", None),
            (
                "malformed",
                Some(
                    "CREATE UNIQUE INDEX turns_one_global_active
                     ON turns(turn_id) WHERE status IN ('queued', 'running')",
                ),
            ),
        ] {
            let root = TestRoot::new(&format!("turn-index-{suffix}"));
            let db_path = root.path().join("state.sqlite");
            let store = Store::open(db_path.clone(), root.path().to_path_buf())
                .expect("create current schema");
            store
                .conn
                .execute("DROP INDEX turns_one_global_active", [])
                .expect("drop active index");
            if let Some(sql) = replacement {
                store.conn.execute(sql, []).expect("replace active index");
            }
            drop(store);
            let error = match Store::open(db_path, root.path().to_path_buf()) {
                Ok(_) => panic!("invalid active index was accepted"),
                Err(error) => error,
            };
            assert!(error.contains("turns_one_global_active"));
        }
    }

    #[test]
    fn startup_rejects_invalid_existing_turn_lifecycle_shape() {
        let root = TestRoot::new("turn-invalid-state");
        let source = create_git_fixture(root.path());
        let db_path = root.path().join("state.sqlite");
        let mut store =
            Store::open(db_path.clone(), root.path().to_path_buf()).expect("open store");
        let (_, thread_id, _) =
            create_codex_thread_with_worktree(&mut store, &source, "Invalid state");
        let turn_id = Uuid::new_v4();
        let (_, plan) = store
            .execute_with_provider_command(CommandEnvelope::new(Command::LiveTurnStart {
                turn_id,
                thread_id,
                text: "remain queued".into(),
            }))
            .expect("admit fixture turn");
        assert!(plan.is_some());
        store
            .conn
            .pragma_update(None, "ignore_check_constraints", "ON")
            .expect("disable checks for corruption fixture");
        store
            .conn
            .execute(
                "UPDATE turns SET status = 'completed' WHERE turn_id = ?1",
                [turn_id.to_string()],
            )
            .expect("corrupt turn lifecycle");
        store
            .conn
            .pragma_update(None, "ignore_check_constraints", "OFF")
            .expect("restore checks");
        drop(store);

        let error = match Store::open(db_path, root.path().to_path_buf()) {
            Ok(_) => panic!("invalid turn lifecycle was accepted"),
            Err(error) => error,
        };
        assert!(
            error.contains("invalid lifecycle state")
                || (error.contains("quick integrity") && error.contains("CHECK constraint")),
            "{error}"
        );
    }

    #[test]
    fn startup_rejects_negative_existing_turn_timestamp() {
        let root = TestRoot::new("turn-negative-timestamp");
        let source = create_git_fixture(root.path());
        let db_path = root.path().join("state.sqlite");
        let mut store =
            Store::open(db_path.clone(), root.path().to_path_buf()).expect("open store");
        let (_, thread_id, _) =
            create_codex_thread_with_worktree(&mut store, &source, "Invalid timestamp");
        let turn_id = Uuid::new_v4();
        let (_, plan) = store
            .execute_with_provider_command(CommandEnvelope::new(Command::LiveTurnStart {
                turn_id,
                thread_id,
                text: "remain queued".into(),
            }))
            .expect("admit fixture turn");
        assert!(plan.is_some());
        store
            .conn
            .pragma_update(None, "ignore_check_constraints", "ON")
            .expect("disable checks for corruption fixture");
        store
            .conn
            .execute(
                "UPDATE turns SET accepted_at = -1 WHERE turn_id = ?1",
                [turn_id.to_string()],
            )
            .expect("corrupt turn timestamp");
        store
            .conn
            .pragma_update(None, "ignore_check_constraints", "OFF")
            .expect("restore checks");
        drop(store);

        let error = match Store::open(db_path, root.path().to_path_buf()) {
            Ok(_) => panic!("negative turn timestamp was accepted"),
            Err(error) => error,
        };
        assert!(
            error.contains("invalid lifecycle state")
                || (error.contains("quick integrity") && error.contains("CHECK constraint")),
            "{error}"
        );
    }

    #[test]
    fn migrates_legacy_database_without_losing_state_and_creates_backup() {
        let root = TestRoot::new("legacy-migration");
        let db_path = root.path().join("state.sqlite");
        let project_id = Uuid::new_v4();
        let legacy = create_legacy_database(&db_path);
        legacy
            .execute(
                "INSERT INTO projects (project_id, name, repo_path) VALUES (?1, 'Legacy', 'C:/legacy')",
                [project_id.to_string()],
            )
            .expect("seed legacy row");
        drop(legacy);

        let store = Store::open(db_path.clone(), root.path().to_path_buf())
            .expect("migrate legacy database");
        let schema_version: i64 = store
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("read schema version");
        let project_name: String = store
            .conn
            .query_row(
                "SELECT name FROM projects WHERE project_id = ?1",
                [project_id.to_string()],
                |row| row.get(0),
            )
            .expect("read migrated row");
        let migration_count: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .expect("read migration record");
        let journal_mode: String = store
            .conn
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .expect("read journal mode");
        assert_eq!(schema_version, SCHEMA_VERSION);
        assert_eq!(project_name, "Legacy");
        assert_eq!(migration_count, SCHEMA_VERSION);
        assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
        assert!(
            db_path
                .with_extension(format!("sqlite.pre-v{SCHEMA_VERSION}.bak"))
                .is_file()
        );
    }

    #[test]
    fn migrates_v1_database_to_turn_projection_without_losing_state() {
        let root = TestRoot::new("v1-to-v2-migration");
        let db_path = root.path().join("state.sqlite");
        let project_id = Uuid::new_v4();
        let v1 = create_v1_database(&db_path);
        v1.execute(
            "INSERT INTO projects (project_id, name, repo_path) VALUES (?1, 'V1', 'C:/v1')",
            [project_id.to_string()],
        )
        .expect("seed v1 row");
        drop(v1);

        let store =
            Store::open(db_path.clone(), root.path().to_path_buf()).expect("migrate v1 database");
        let project_name: String = store
            .conn
            .query_row(
                "SELECT name FROM projects WHERE project_id = ?1",
                [project_id.to_string()],
                |row| row.get(0),
            )
            .expect("read preserved v1 row");
        let turn_count: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM turns", [], |row| row.get(0))
            .expect("query v2 turn projection");
        let schema_version: i64 = store
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("read migrated schema version");

        assert_eq!(project_name, "V1");
        assert_eq!(turn_count, 0);
        assert_eq!(schema_version, SCHEMA_VERSION);
        assert!(
            db_path
                .with_extension(format!("sqlite.pre-v{SCHEMA_VERSION}.bak"))
                .is_file()
        );
    }

    #[test]
    fn rejects_database_from_a_newer_schema_version() {
        let root = TestRoot::new("future-schema");
        let db_path = root.path().join("state.sqlite");
        let conn = Connection::open(&db_path).expect("open database");
        conn.pragma_update(None, "user_version", SCHEMA_VERSION + 1)
            .expect("set future version");
        drop(conn);

        let error = match Store::open(db_path, root.path().to_path_buf()) {
            Ok(_) => panic!("future schema unexpectedly opened"),
            Err(error) => error,
        };
        assert!(error.contains("newer than supported"), "{error}");
        assert!(
            !root
                .path()
                .join(format!("state.sqlite.pre-v{SCHEMA_VERSION}.bak"))
                .exists()
        );
        let conn = Connection::open(root.path().join("state.sqlite"))
            .expect("reopen rejected future database");
        let journal_mode: String = conn
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .expect("read unchanged journal mode");
        assert_ne!(journal_mode.to_ascii_lowercase(), "wal");
    }

    #[test]
    fn consumes_foreign_key_check_and_rejects_corrupt_projection() {
        let root = TestRoot::new("foreign-key-check");
        let db_path = root.path().join("state.sqlite");
        let legacy = create_legacy_database(&db_path);
        legacy
            .execute(
                "INSERT INTO threads
                 (thread_id, project_id, provider, label, state, last_event_sequence)
                 VALUES (?1, ?2, 'codex', 'Orphan', 'idle', 0)",
                params![Uuid::new_v4().to_string(), Uuid::new_v4().to_string()],
            )
            .expect("seed orphan projection");
        drop(legacy);

        let error = match Store::open(db_path, root.path().to_path_buf()) {
            Ok(_) => panic!("foreign-key violation unexpectedly opened"),
            Err(error) => error,
        };
        assert!(error.contains("foreign-key violation"), "{error}");
        assert!(error.contains("threads"), "{error}");
    }

    #[test]
    fn rejects_duplicate_non_null_thread_worktree_ownership_on_open() {
        let root = TestRoot::new("duplicate-worktree-owner");
        let db_path = root.path().join("state.sqlite");
        let project_id = Uuid::new_v4();
        let worktree_id = Uuid::new_v4();
        let first_thread = Uuid::new_v4();
        let second_thread = Uuid::new_v4();
        let legacy = create_legacy_database(&db_path);
        legacy
            .execute(
                "INSERT INTO projects (project_id, name, repo_path) VALUES (?1, 'Legacy', '.')",
                [project_id.to_string()],
            )
            .expect("seed project");
        legacy
            .execute(
                "INSERT INTO worktrees (worktree_id, project_id, branch, path, status)
                 VALUES (?1, ?2, 'legacy/branch', 'legacy/path', 'ready')",
                params![worktree_id.to_string(), project_id.to_string()],
            )
            .expect("seed worktree");
        for thread_id in [first_thread, second_thread] {
            legacy
                .execute(
                    "INSERT INTO threads
                     (thread_id, project_id, worktree_id, provider, label, state, last_event_sequence)
                     VALUES (?1, ?2, ?3, 'codex', 'Legacy', 'idle', 0)",
                    params![
                        thread_id.to_string(),
                        project_id.to_string(),
                        worktree_id.to_string()
                    ],
                )
                .expect("seed duplicate owner");
        }
        drop(legacy);

        let error = match Store::open(db_path.clone(), root.path().to_path_buf()) {
            Ok(_) => panic!("duplicate worktree ownership unexpectedly opened"),
            Err(error) => error,
        };
        assert!(error.contains(&worktree_id.to_string()), "{error}");
        assert!(error.contains("attached to 2 threads"), "{error}");
        assert!(
            db_path
                .with_extension(format!("sqlite.pre-v{SCHEMA_VERSION}.bak"))
                .is_file()
        );
    }

    #[test]
    fn rejects_accepted_plan_with_a_different_thread_project_on_open() {
        let root = TestRoot::new("accepted-plan-project-open");
        let db_path = root.path().join("state.sqlite");
        let thread_project_id = Uuid::new_v4();
        let plan_project_id = Uuid::new_v4();
        let thread_id = Uuid::new_v4();
        let command_id = Uuid::new_v4();
        let worktree_id = Uuid::new_v4();
        let legacy = create_legacy_database(&db_path);
        for (project_id, name) in [
            (thread_project_id, "Thread project"),
            (plan_project_id, "Plan project"),
        ] {
            legacy
                .execute(
                    "INSERT INTO projects (project_id, name, repo_path) VALUES (?1, ?2, '.')",
                    params![project_id.to_string(), name],
                )
                .expect("seed project");
        }
        legacy
            .execute(
                "INSERT INTO threads
                 (thread_id, project_id, provider, label, state, last_event_sequence)
                 VALUES (?1, ?2, 'codex', 'Legacy', 'idle', 0)",
                params![thread_id.to_string(), thread_project_id.to_string()],
            )
            .expect("seed thread");
        legacy
            .execute(
                "INSERT INTO command_receipts
                 (command_id, protocol_version, command_json, status, result_json, recorded_at)
                 VALUES (?1, ?2, '{}', 'accepted', '{}', 1)",
                params![command_id.to_string(), PROTOCOL_VERSION],
            )
            .expect("seed accepted receipt");
        legacy
            .execute(
                "INSERT INTO worktree_plans
                 (command_id, worktree_id, thread_id, project_id, repo_path, repo_common_dir,
                  branch, path, commit_oid)
                 VALUES (?1, ?2, ?3, ?4, '.', '.git', 'legacy/branch', 'legacy/path', ?5)",
                params![
                    command_id.to_string(),
                    worktree_id.to_string(),
                    thread_id.to_string(),
                    plan_project_id.to_string(),
                    "0000000000000000000000000000000000000000"
                ],
            )
            .expect("seed mismatched plan");
        drop(legacy);

        let error = match Store::open(db_path.clone(), root.path().to_path_buf()) {
            Ok(_) => panic!("mismatched accepted plan unexpectedly opened"),
            Err(error) => error,
        };
        for expected in [
            command_id.to_string(),
            worktree_id.to_string(),
            plan_project_id.to_string(),
            thread_project_id.to_string(),
        ] {
            assert!(error.contains(&expected), "{error}");
        }
        assert!(
            db_path
                .with_extension(format!("sqlite.pre-v{SCHEMA_VERSION}.bak"))
                .is_file()
        );
    }

    #[test]
    fn nonexistent_thread_mutations_are_durable_rejections_without_events() {
        let root = TestRoot::new("phantom-mutations");
        let db_path = root.path().join("state.sqlite");
        let mut store = Store::open(db_path, root.path().to_path_buf()).expect("open empty store");
        let thread_id = Uuid::new_v4();
        let interrupt = store
            .execute(CommandEnvelope::new(Command::TurnInterrupt { thread_id }))
            .expect("reject interrupt");
        let archive = store
            .execute(CommandEnvelope::new(Command::ThreadArchive { thread_id }))
            .expect("reject archive");
        let archived_thread = Uuid::new_v4();
        let archived_project = Uuid::new_v4();
        store
            .conn
            .execute(
                "INSERT INTO projects (project_id, name, repo_path) VALUES (?1, 'Archived', '.')",
                [archived_project.to_string()],
            )
            .expect("seed archived project");
        store
            .conn
            .execute(
                "INSERT INTO threads
                 (thread_id, project_id, provider, label, state, last_event_sequence)
                 VALUES (?1, ?2, 'codex', 'Archived', 'archived', 0)",
                params![archived_thread.to_string(), archived_project.to_string()],
            )
            .expect("seed archived thread");
        let archived_send = store
            .execute(CommandEnvelope::new(Command::TurnSend {
                turn_id: Uuid::new_v4(),
                thread_id: archived_thread,
                text: "do not revive".into(),
            }))
            .expect("reject send to archived thread");
        let archived_interrupt = store
            .execute(CommandEnvelope::new(Command::TurnInterrupt {
                thread_id: archived_thread,
            }))
            .expect("reject interrupt for archived thread");
        let archived_archive = store
            .execute(CommandEnvelope::new(Command::ThreadArchive {
                thread_id: archived_thread,
            }))
            .expect("reject repeated archive");
        let event_count: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .expect("count events");
        let aggregate_count: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM aggregate_versions", [], |row| {
                row.get(0)
            })
            .expect("count aggregate versions");
        let receipt_count: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM command_receipts", [], |row| {
                row.get(0)
            })
            .expect("count receipts");

        assert_eq!(interrupt.status, "rejected");
        assert_eq!(archive.status, "rejected");
        assert_eq!(archived_send.status, "rejected");
        assert_eq!(archived_interrupt.status, "rejected");
        assert_eq!(archived_archive.status, "rejected");
        assert_eq!(event_count, 0);
        assert_eq!(aggregate_count, 0);
        assert_eq!(receipt_count, 5);
    }

    #[test]
    fn live_interrupt_is_a_durable_rejection_without_state_change() {
        let root = TestRoot::new("waiting-user-interrupt");
        let source = create_git_fixture(root.path());
        let db_path = root.path().join("state.sqlite");
        let mut store = Store::open(db_path, root.path().to_path_buf()).expect("open empty store");
        let project_id = Uuid::new_v4();
        let thread_id = Uuid::new_v4();
        store
            .execute(CommandEnvelope::new(Command::ProjectCreate {
                project_id,
                name: "Waiting user fixture".into(),
                repo_path: source,
            }))
            .expect("create project");
        store
            .execute(CommandEnvelope::new(Command::ThreadCreate {
                thread_id,
                project_id,
                provider: Provider::Codex,
                label: "Running fixture".into(),
            }))
            .expect("create thread");
        store
            .conn
            .execute(
                "UPDATE threads SET state = 'running' WHERE thread_id = ?1",
                [thread_id.to_string()],
            )
            .expect("mark fixture running");

        let (state_before, last_sequence_before): (String, i64) = store
            .conn
            .query_row(
                "SELECT state, last_event_sequence FROM threads WHERE thread_id = ?1",
                [thread_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("load waiting-user projection");
        let events_before: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .expect("count events before interrupt");
        let receipts_before: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM command_receipts", [], |row| {
                row.get(0)
            })
            .expect("count receipts before interrupt");
        let version_before =
            connection_aggregate_version(&store.conn, thread_id).expect("load thread version");
        assert_eq!(state_before, ActorState::Running.as_str());

        let interrupted = store
            .execute(CommandEnvelope::new(Command::TurnInterrupt { thread_id }))
            .expect("durably reject waiting-user interrupt");
        assert_eq!(interrupted.status, "rejected");
        assert_eq!(interrupted.event_sequence, None);
        assert!(
            interrupted.result["error"]
                .as_str()
                .is_some_and(|error| error.contains("live interruption is not implemented"))
        );

        let (state_after, last_sequence_after): (String, i64) = store
            .conn
            .query_row(
                "SELECT state, last_event_sequence FROM threads WHERE thread_id = ?1",
                [thread_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("reload waiting-user projection");
        let events_after: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .expect("count events after interrupt");
        let receipts_after: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM command_receipts", [], |row| {
                row.get(0)
            })
            .expect("count receipts after interrupt");
        assert_eq!(state_after, state_before);
        assert_eq!(last_sequence_after, last_sequence_before);
        assert_eq!(events_after, events_before);
        assert_eq!(receipts_after, receipts_before + 1);
        assert_eq!(
            connection_aggregate_version(&store.conn, thread_id)
                .expect("load unchanged thread version"),
            version_before
        );
    }

    #[test]
    fn unresolved_accepted_plan_blocks_fresh_worktree_but_remains_recoverable() {
        let root = TestRoot::new("unresolved-accepted-plan");
        let source = create_git_fixture(root.path());
        let runtime = root.path().join("runtime");
        fs::create_dir_all(runtime.join("worktrees")).expect("create worktree root");
        let mut store =
            Store::open(runtime.join("state.sqlite"), runtime).expect("open worktree store");
        let project_id = Uuid::new_v4();
        let thread_id = Uuid::new_v4();
        store
            .execute(CommandEnvelope::new(Command::ProjectCreate {
                project_id,
                name: "Recovery fixture".into(),
                repo_path: source.clone(),
            }))
            .expect("create project");
        store
            .execute(CommandEnvelope::new(Command::ThreadCreate {
                thread_id,
                project_id,
                provider: Provider::Codex,
                label: "Recover me".into(),
            }))
            .expect("create thread");

        let original_worktree_id = Uuid::new_v4();
        let original = CommandEnvelope::new(Command::WorktreeCreate {
            worktree_id: original_worktree_id,
            thread_id,
        });
        let original_payload =
            serde_json::to_string(&original.command).expect("serialize original command");
        let original_plan = store
            .plan_worktree(original_worktree_id, thread_id)
            .expect("plan original worktree");
        assert!(
            store
                .accept_worktree(&original, &original_payload, &original_plan)
                .expect("accept original plan")
                .is_none()
        );

        let events_before: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .expect("count events before retry");
        let accepted_events_before: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE event_type = 'command.accepted'",
                [],
                |row| row.get(0),
            )
            .expect("count accepted events before retry");
        let plans_before: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM worktree_plans", [], |row| row.get(0))
            .expect("count plans before retry");
        let aggregate_before =
            connection_aggregate_version(&store.conn, thread_id).expect("read aggregate version");

        store
            .conn
            .execute(
                "UPDATE projects SET repo_path = ?1 WHERE project_id = ?2",
                params![
                    root.path().join("missing-repository").to_string_lossy(),
                    project_id.to_string()
                ],
            )
            .expect("make accidental Git preflight fail");
        let retry = CommandEnvelope::new(Command::WorktreeCreate {
            worktree_id: Uuid::new_v4(),
            thread_id,
        });
        let retry_payload = serde_json::to_string(&retry.command).expect("serialize retry command");
        let rejected = store.execute(retry.clone()).expect("durably reject retry");
        assert_eq!(rejected.status, "rejected");
        let rejection = rejected.result["error"]
            .as_str()
            .expect("rejection message");
        assert!(rejection.contains("unresolved accepted worktree command"));
        assert!(rejection.contains(&original.command_id.to_string()));
        assert!(rejection.contains(&original_worktree_id.to_string()));
        assert_eq!(
            store
                .load_receipt(retry.command_id, &retry_payload)
                .expect("load retry receipt")
                .expect("retry receipt exists")
                .status,
            "rejected"
        );

        let events_after: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .expect("count events after retry");
        let accepted_events_after: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE event_type = 'command.accepted'",
                [],
                |row| row.get(0),
            )
            .expect("count accepted events after retry");
        let plans_after: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM worktree_plans", [], |row| row.get(0))
            .expect("count plans after retry");
        let retry_plan_count: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM worktree_plans WHERE command_id = ?1",
                [retry.command_id.to_string()],
                |row| row.get(0),
            )
            .expect("count retry plans");
        assert_eq!(events_after, events_before);
        assert_eq!(accepted_events_after, accepted_events_before);
        assert_eq!(plans_after, plans_before);
        assert_eq!(retry_plan_count, 0);
        assert_eq!(
            connection_aggregate_version(&store.conn, thread_id)
                .expect("read unchanged aggregate version"),
            aggregate_before
        );

        store
            .conn
            .execute(
                "UPDATE projects SET repo_path = ?1 WHERE project_id = ?2",
                params![source.to_string_lossy(), project_id.to_string()],
            )
            .expect("restore repository path");
        let recovered = store
            .execute(original.clone())
            .expect("replay original accepted command");
        assert_eq!(recovered.status, "succeeded");
        verify_worktree(&original_plan).expect("verify recovered worktree");
        assert_eq!(
            store
                .load_receipt(original.command_id, &original_payload)
                .expect("load original receipt")
                .expect("original receipt exists")
                .status,
            "succeeded"
        );

        drop(store);
        let status = ProcessCommand::new("git")
            .arg("-C")
            .arg(&source)
            .args(["worktree", "remove", "--force"])
            .arg(&original_plan.path)
            .status()
            .expect("remove recovered worktree");
        assert!(status.success());
    }

    #[test]
    fn accepted_plan_thread_project_mismatch_is_indeterminate_before_git() {
        let root = TestRoot::new("accepted-plan-project-runtime");
        let source = create_git_fixture(root.path());
        let runtime = root.path().join("runtime");
        fs::create_dir_all(runtime.join("worktrees")).expect("create worktree root");
        let mut store =
            Store::open(runtime.join("state.sqlite"), runtime).expect("open worktree store");
        let thread_project_id = Uuid::new_v4();
        let plan_project_id = Uuid::new_v4();
        for (project_id, name) in [
            (thread_project_id, "Thread project"),
            (plan_project_id, "Plan project"),
        ] {
            store
                .execute(CommandEnvelope::new(Command::ProjectCreate {
                    project_id,
                    name: name.into(),
                    repo_path: source.clone(),
                }))
                .expect("create project");
        }
        let thread_id = Uuid::new_v4();
        store
            .execute(CommandEnvelope::new(Command::ThreadCreate {
                thread_id,
                project_id: thread_project_id,
                provider: Provider::Codex,
                label: "Mismatch".into(),
            }))
            .expect("create thread");
        let worktree_id = Uuid::new_v4();
        let envelope = CommandEnvelope::new(Command::WorktreeCreate {
            worktree_id,
            thread_id,
        });
        let payload = serde_json::to_string(&envelope.command).expect("serialize command");
        let plan = store
            .plan_worktree(worktree_id, thread_id)
            .expect("plan worktree");
        assert!(
            store
                .accept_worktree(&envelope, &payload, &plan)
                .expect("accept worktree plan")
                .is_none()
        );
        store
            .conn
            .execute(
                "UPDATE worktree_plans SET project_id = ?1, repo_path = ?2 WHERE command_id = ?3",
                params![
                    plan_project_id.to_string(),
                    root.path().join("missing-repository").to_string_lossy(),
                    envelope.command_id.to_string()
                ],
            )
            .expect("corrupt accepted plan");

        let error = store
            .execute(envelope.clone())
            .expect_err("mismatched accepted plan must not reach Git");
        assert!(error.contains(&worktree_id.to_string()), "{error}");
        assert!(error.contains(&thread_project_id.to_string()), "{error}");
        assert!(error.contains(&plan_project_id.to_string()), "{error}");
        assert!(!plan.path.exists());
        let receipt = store
            .load_receipt(envelope.command_id, &payload)
            .expect("load receipt")
            .expect("receipt exists");
        assert_eq!(receipt.status, "indeterminate");
    }

    #[test]
    fn worktree_collisions_reject_before_git_and_legacy_acceptance_becomes_indeterminate() {
        let root = TestRoot::new("worktree-collisions");
        let source = create_git_fixture(root.path());
        let runtime = root.path().join("runtime");
        fs::create_dir_all(runtime.join("worktrees")).expect("create worktree root");
        let db_path = runtime.join("state.sqlite");
        let mut store = Store::open(db_path, runtime.clone()).expect("open store");
        let project_id = Uuid::new_v4();
        store
            .execute(CommandEnvelope::new(Command::ProjectCreate {
                project_id,
                name: "Collision fixture".into(),
                repo_path: source.clone(),
            }))
            .expect("create project");
        let first_thread = Uuid::new_v4();
        let second_thread = Uuid::new_v4();
        for thread_id in [first_thread, second_thread] {
            store
                .execute(CommandEnvelope::new(Command::ThreadCreate {
                    thread_id,
                    project_id,
                    provider: Provider::Codex,
                    label: format!("Thread {thread_id}"),
                }))
                .expect("create thread");
        }

        let first_worktree = Uuid::new_v4();
        let first_receipt = store
            .execute(CommandEnvelope::new(Command::WorktreeCreate {
                worktree_id: first_worktree,
                thread_id: first_thread,
            }))
            .expect("create first worktree");
        assert_eq!(first_receipt.status, "succeeded");
        let first_path: String = store
            .conn
            .query_row(
                "SELECT path FROM worktrees WHERE worktree_id = ?1",
                [first_worktree.to_string()],
                |row| row.get(0),
            )
            .expect("load first worktree path");

        let legacy_envelope = CommandEnvelope::new(Command::WorktreeCreate {
            worktree_id: Uuid::new_v4(),
            thread_id: first_thread,
        });
        let legacy_payload =
            serde_json::to_string(&legacy_envelope.command).expect("serialize legacy command");
        let legacy_worktree_id = match &legacy_envelope.command {
            Command::WorktreeCreate { worktree_id, .. } => *worktree_id,
            _ => unreachable!(),
        };
        let legacy_plan = store
            .plan_worktree(legacy_worktree_id, first_thread)
            .expect("plan legacy collision");
        store
            .accept_worktree(&legacy_envelope, &legacy_payload, &legacy_plan)
            .expect("seed legacy accepted command");
        store
            .conn
            .execute(
                "INSERT INTO command_receipts
                 (command_id, protocol_version, command_json, status, result_json, recorded_at)
                 VALUES ('malformed-command-id', ?1, '{}', 'accepted', '{}', -1)",
                [PROTOCOL_VERSION],
            )
            .expect("seed malformed accepted receipt");
        store
            .conn
            .execute(
                "INSERT INTO worktree_plans
                 (command_id, worktree_id, thread_id, project_id, repo_path, repo_common_dir,
                  branch, path, commit_oid)
                 VALUES ('malformed-command-id', ?1, ?2, ?3, ?4, ?5, 'invalid', ?6, ?7)",
                params![
                    Uuid::new_v4().to_string(),
                    first_thread.to_string(),
                    project_id.to_string(),
                    source.to_string_lossy(),
                    source.join(".git").to_string_lossy(),
                    root.path().join("never-created").to_string_lossy(),
                    "0000000000000000000000000000000000000000"
                ],
            )
            .expect("seed malformed accepted plan");
        let recovery_warnings = store
            .recover_accepted_worktrees()
            .expect("recover legacy collision");
        let legacy_receipt = store
            .load_receipt(legacy_envelope.command_id, &legacy_payload)
            .expect("load legacy receipt")
            .expect("legacy receipt exists");
        assert_eq!(recovery_warnings.len(), 2);
        assert!(
            recovery_warnings
                .iter()
                .any(|warning| warning.contains("malformed-command-id"))
        );
        assert_eq!(legacy_receipt.status, "indeterminate");

        store
            .conn
            .execute(
                "UPDATE projects SET repo_path = ?1 WHERE project_id = ?2",
                params![
                    root.path().join("missing-repository").to_string_lossy(),
                    project_id.to_string()
                ],
            )
            .expect("make any accidental Git preflight fail");
        let duplicate_envelope = CommandEnvelope::new(Command::WorktreeCreate {
            worktree_id: Uuid::new_v4(),
            thread_id: first_thread,
        });
        let duplicate = store
            .execute(duplicate_envelope.clone())
            .expect("reject attached thread before Git");
        let collision_envelope = CommandEnvelope::new(Command::WorktreeCreate {
            worktree_id: first_worktree,
            thread_id: second_thread,
        });
        let collision = store
            .execute(collision_envelope.clone())
            .expect("reject owned worktree id before Git");
        assert_eq!(duplicate.status, "rejected");
        assert_eq!(collision.status, "rejected");
        assert!(
            duplicate.result["error"]
                .as_str()
                .is_some_and(|message| message.contains("already has worktree"))
        );
        assert!(
            collision.result["error"]
                .as_str()
                .is_some_and(|message| message.contains("already owned"))
        );
        for command_id in [duplicate_envelope.command_id, collision_envelope.command_id] {
            let plan_count: i64 = store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM worktree_plans WHERE command_id = ?1",
                    [command_id.to_string()],
                    |row| row.get(0),
                )
                .expect("count rejected plans");
            assert_eq!(plan_count, 0);
        }
        drop(store);

        let status = ProcessCommand::new("git")
            .arg("-C")
            .arg(&source)
            .args(["worktree", "remove", "--force"])
            .arg(&first_path)
            .status()
            .expect("remove test worktree");
        assert!(status.success());
    }
}
