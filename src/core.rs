use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command as ProcessCommand,
    sync::mpsc::{self, Receiver, SyncSender},
    thread,
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

pub const PROTOCOL_VERSION: u16 = 1;
const COMMAND_CAPACITY: usize = 8;
const EVENT_CAPACITY: usize = 32;

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
        store.recover_accepted_worktrees()?;
        store.ensure_welcome()?;

        let (input_tx, input_rx) = mpsc::sync_channel(COMMAND_CAPACITY);
        let (event_tx, event_rx) = mpsc::sync_channel(EVENT_CAPACITY);
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

struct Store {
    conn: Connection,
    runtime_root: PathBuf,
}

impl Store {
    fn open(db_path: PathBuf, runtime_root: PathBuf) -> AppResult<Self> {
        let conn = Connection::open(db_path).map_err(err)?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(err)?;
        conn.busy_timeout(std::time::Duration::from_secs(3))
            .map_err(err)?;
        conn.execute_batch(
            "
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
            ",
        )
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
                let plan = self.plan_worktree(worktree_id, thread_id)?;
                if let Some(rejected) = self.accept_worktree(&envelope, &payload, &plan)? {
                    return Ok(rejected);
                }
                plan
            }
        };

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

    fn recover_accepted_worktrees(&mut self) -> AppResult<()> {
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
        for (command_id, protocol_version, command_json) in commands {
            self.execute(CommandEnvelope {
                protocol_version,
                command_id: Uuid::parse_str(&command_id).map_err(err)?,
                expected_aggregate_version: None,
                command: serde_json::from_str(&command_json).map_err(err)?,
            })?;
        }
        Ok(())
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
    store.recover_accepted_worktrees()?;
    let recovered_before = store
        .load_receipt(crash_before_envelope.command_id, &crash_before_payload)?
        .ok_or_else(|| "crash-before-Git receipt disappeared".to_owned())?;
    if recovered_before.status != "succeeded" {
        return Err("crash-before-Git receipt did not recover".into());
    }
    verify_worktree(&crash_before_plan)?;

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
    store.recover_accepted_worktrees()?;
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
        "native_git_worktree": true,
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
    #[test]
    fn durable_core_and_worktree_smoke() {
        let result = super::self_check().expect("self-check");
        assert_eq!(result["sqlite_idempotency"], true);
        assert_eq!(result["native_git_worktree"], true);
    }
}
