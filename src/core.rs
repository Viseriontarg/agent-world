use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command as ProcessCommand,
    sync::mpsc::{self, Receiver, SyncSender},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

pub const PROTOCOL_VERSION: u16 = 1;
const SCHEMA_VERSION: i64 = 1;
const COMMAND_CAPACITY: usize = 8;
const EVENT_CAPACITY: usize = 32;

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
        thread_id: Uuid,
        text: String,
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
}

pub enum CoreInput {
    Execute(CommandEnvelope),
    Bootstrap,
    Timeline { thread_id: Uuid, limit: usize },
}

pub enum CoreEvent {
    Bootstrap(BootstrapSnapshot),
    Receipt(Receipt),
    Timeline {
        thread_id: Uuid,
        messages: Vec<TimelineMessage>,
    },
    Error(String),
}

pub struct CoreHandle {
    pub tx: SyncSender<CoreInput>,
    pub rx: Receiver<CoreEvent>,
}

impl CoreHandle {
    pub fn spawn(runtime_root: PathBuf, wake_ui: impl Fn() + Send + 'static) -> AppResult<Self> {
        fs::create_dir_all(runtime_root.join("worktrees")).map_err(err)?;
        fs::create_dir_all(runtime_root.join("artifacts")).map_err(err)?;
        let mut store = Store::open(runtime_root.join("state.sqlite"), runtime_root)?;
        let recovery_warnings = store.recover_accepted_worktrees()?;
        store.ensure_welcome()?;

        let (input_tx, input_rx) = mpsc::sync_channel(COMMAND_CAPACITY);
        let (event_tx, event_rx) = mpsc::sync_channel(EVENT_CAPACITY);
        if !recovery_warnings.is_empty() {
            event_tx
                .try_send(CoreEvent::Error(format!(
                    "Worktree recovery needs attention:\n{}",
                    recovery_warnings.join("\n")
                )))
                .map_err(err)?;
            wake_ui();
        }
        thread::Builder::new()
            .name("agent-world-core".into())
            .spawn(move || {
                while let Ok(input) = input_rx.recv() {
                    let event = match input {
                        CoreInput::Execute(envelope) => {
                            store.execute(envelope).map(CoreEvent::Receipt)
                        }
                        CoreInput::Bootstrap => {
                            store.bootstrap_snapshot().map(CoreEvent::Bootstrap)
                        }
                        CoreInput::Timeline { thread_id, limit } => store
                            .timeline_page(thread_id, limit.min(100))
                            .map(|messages| CoreEvent::Timeline {
                                thread_id,
                                messages,
                            }),
                    }
                    .unwrap_or_else(CoreEvent::Error);
                    if event_tx.send(event).is_err() {
                        break;
                    }
                    wake_ui();
                }
            })
            .map_err(err)?;

        Ok(Self {
            tx: input_tx,
            rx: event_rx,
        })
    }

    pub fn command(&self, command: Command) -> AppResult<()> {
        self.tx
            .try_send(CoreInput::Execute(CommandEnvelope::new(command)))
            .map_err(err)
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
    if current_version != 0 {
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
    tx.execute_batch(SCHEMA_V1_SQL).map_err(err)?;
    tx.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (?1, ?2)",
        params![SCHEMA_VERSION, now_ms()],
    )
    .map_err(err)?;
    validate_schema(&tx)?;
    tx.execute_batch("PRAGMA user_version = 1;").map_err(err)?;
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
    ];
    for (name, query) in REQUIRED_PROJECTIONS {
        conn.prepare(query)
            .map_err(|error| format!("database schema validation failed for {name}: {error}"))?;
    }

    let applied: Option<i64> = conn
        .query_row(
            "SELECT version FROM schema_migrations WHERE version = ?1",
            [SCHEMA_VERSION],
            |row| row.get(0),
        )
        .optional()
        .map_err(err)?;
    if applied != Some(SCHEMA_VERSION) {
        return Err(format!(
            "database migration record for schema version {SCHEMA_VERSION} is missing"
        ));
    }
    Ok(())
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
        Ok(Self { conn, runtime_root })
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
        if let Err(error) = validate_command(&tx, &envelope.command) {
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
        for (command_id, protocol_version, command_json) in commands {
            let recovery = (|| {
                self.execute(CommandEnvelope {
                    protocol_version,
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
                        t.last_event_sequence, w.path
                 FROM threads t
                 LEFT JOIN worktrees w ON w.worktree_id = t.worktree_id
                 WHERE t.state != 'archived'
                 ORDER BY t.label",
            )
            .map_err(err)?;
        let actors = statement
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
                })
            })
            .map_err(err)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(err)?;
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

    fn timeline_page(&self, thread_id: Uuid, limit: usize) -> AppResult<Vec<TimelineMessage>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT sequence, role, body, occurred_at FROM (
                    SELECT sequence, role, body, occurred_at
                    FROM messages WHERE thread_id = ?1
                    ORDER BY sequence DESC LIMIT ?2
                 ) ORDER BY sequence ASC",
            )
            .map_err(err)?;
        statement
            .query_map(params![thread_id.to_string(), limit as i64], |row| {
                Ok(TimelineMessage {
                    sequence: row.get::<_, i64>(0)? as u64,
                    role: row.get(1)?,
                    body: row.get(2)?,
                    occurred_at: row.get(3)?,
                })
            })
            .map_err(err)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(err)
    }
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
        Command::TurnSend { thread_id, text } => {
            let text = bounded(text, 32 * 1024)?;
            tx.execute(
                "INSERT INTO messages (sequence, thread_id, role, body, occurred_at)
                 VALUES (?1, ?2, 'user', ?3, ?4)",
                params![sequence as i64, thread_id.to_string(), text, now],
            )
            .map_err(err)?;
            tx.execute(
                "UPDATE threads
                 SET state = 'waiting_user', last_event_sequence = ?1
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

fn validate_command(tx: &Transaction<'_>, command: &Command) -> AppResult<()> {
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
        Command::TurnSend { thread_id, text } => {
            bounded(text, 32 * 1024)?;
            let state = required_thread_state(tx, *thread_id)?;
            if state == "archived" {
                return Err(format!("thread {thread_id} is archived"));
            }
        }
        Command::TurnInterrupt { thread_id } => {
            let state = required_thread_state(tx, *thread_id)?;
            if !matches!(
                state.as_str(),
                "starting" | "running" | "awaiting_approval" | "waiting_user"
            ) {
                return Err(format!(
                    "thread {thread_id} cannot be interrupted while {state}"
                ));
            }
        }
        Command::ThreadArchive { thread_id } => {
            let state = required_thread_state(tx, *thread_id)?;
            if state == "archived" {
                return Err(format!("thread {thread_id} is already archived"));
            }
        }
        Command::WorktreeCreate { .. } => {
            return Err("worktree command bypassed its durable execution path".into());
        }
    }
    Ok(())
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
        Command::TurnSend { .. } => "turn.queued",
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
    let actual_path = git_toplevel(&plan.path)?;
    let expected_path = portable_windows_path(plan.path.canonicalize().map_err(err)?);
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
        provider: Provider::Claude,
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

    let mut store = Store::open(db_path, runtime)?;
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
        assert_eq!(migration_count, 1);
        assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
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
        assert!(!root.path().join("state.sqlite.pre-v1.bak").exists());
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
