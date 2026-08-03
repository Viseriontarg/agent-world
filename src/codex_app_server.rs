use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    io::{self, BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, ExitStatus, Stdio},
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError, TrySendError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use uuid::Uuid;

use crate::{
    live_turn::{
        ApprovalDecision, MAX_DIAGNOSTIC_BYTES, MAX_INTERACTION_ID_BYTES,
        MAX_INTERACTION_TEXT_BYTES, MAX_OUTPUT_DELTA_BYTES, MAX_PROVIDER_ID_BYTES, ProviderCommand,
        ProviderEvent, ProviderReadiness, ProviderRunner, ProviderSessionCursor, UserInputAnswer,
        UserInputQuestion,
    },
    providers::{
        SUPPORTED_CODEX_VERSION, codex_launch_arguments, locate_native_executable,
        verify_codex_disabled_features, verify_codex_version,
    },
};

/// Maximum encoded line accepted from Codex stdout. The reader drains an oversized line without
/// growing its buffer and then fails the owned process closed.
pub const MAX_PROTOCOL_LINE_BYTES: usize = 256 * 1024;
/// Maximum single stderr line retained for diagnostics. Larger lines are replaced by a marker.
pub const MAX_DIAGNOSTIC_LINE_BYTES: usize = 8 * 1024;
/// Total tail of sanitized stderr retained for an unexpected exit.
pub const STDERR_RING_BYTES: usize = 32 * 1024;
/// Assistant deltas are combined until this byte boundary or [`STREAM_FLUSH_INTERVAL`] elapses.
pub const STREAM_COALESCE_BYTES: usize = 8 * 1024;
pub const STREAM_FLUSH_INTERVAL: Duration = Duration::from_millis(40);

const READER_QUEUE_CAPACITY: usize = 32;
const MAX_PENDING_REQUESTS: usize = 8;
const MAX_PENDING_INTERACTIONS: usize = 8;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const INTERRUPT_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(10);
const EXIT_DRAIN_TIMEOUT: Duration = Duration::from_millis(250);
const CURSOR_PREFIX: &str = "codex-thread-v1:";
const CODEX_APPROVAL_POLICY: &str = "on-request";
const CODEX_THREAD_SANDBOX_MODE: &str = "workspace-write";
const CODEX_TURN_SANDBOX_TYPE: &str = "workspaceWrite";

/// Production Codex runner. It launches lazily on the first durably accepted `Start`, owns one
/// long-lived app-server child, and never retries a provider side effect on its own.
pub struct CodexAppServerRunner {
    executable: Option<PathBuf>,
    startup_timeout: Duration,
    interrupt_timeout: Duration,
}

impl Default for CodexAppServerRunner {
    fn default() -> Self {
        Self {
            executable: None,
            startup_timeout: STARTUP_TIMEOUT,
            interrupt_timeout: INTERRUPT_TIMEOUT,
        }
    }
}

impl CodexAppServerRunner {
    fn resolve_executable(&self) -> Result<PathBuf, String> {
        self.executable
            .clone()
            .map(Ok)
            .unwrap_or_else(|| locate_native_executable("codex"))
    }

    fn run_loop(
        self,
        commands: Receiver<ProviderCommand>,
        events: SyncSender<ProviderEvent>,
    ) -> Result<(), String> {
        let mut process: Option<OwnedProcess> = None;
        let mut protocol = ProtocolMachine::new(self.startup_timeout, self.interrupt_timeout);
        let mut owned_worktree: Option<PathBuf> = None;

        loop {
            if process.is_none() {
                let command = match commands.recv() {
                    Ok(command) => command,
                    Err(_) => return Ok(()),
                };
                command.validate()?;
                if matches!(command, ProviderCommand::Shutdown) {
                    return Ok(());
                }
                let ProviderCommand::Start { worktree_path, .. } = &command else {
                    emit_orphan_command_failure(&events, &command)?;
                    continue;
                };
                let turn_id = command.turn_id().expect("Start has a turn id");
                let canonical_worktree = match canonical_worktree(worktree_path) {
                    Ok(path) => path,
                    Err(error) => {
                        emit_start_failure(&events, turn_id, "worktree", &error)?;
                        continue;
                    }
                };
                let executable = match self.resolve_executable() {
                    Ok(path) => path,
                    Err(error) => {
                        emit_start_failure(&events, turn_id, "executable", &error)?;
                        continue;
                    }
                };
                if let Err(error) = verify_installed_version(&executable) {
                    emit_start_failure(&events, turn_id, "version", &error)?;
                    continue;
                }
                if let Err(error) = verify_codex_disabled_features(&executable) {
                    emit_start_failure(&events, turn_id, "feature-contract", &error)?;
                    continue;
                }
                let child = match OwnedProcess::spawn(&executable, &canonical_worktree) {
                    Ok(child) => child,
                    Err(error) => {
                        emit_start_failure(&events, turn_id, "launch", &error)?;
                        continue;
                    }
                };
                owned_worktree = Some(canonical_worktree);
                process = Some(child);
                let effects =
                    protocol.handle_command(command, Instant::now(), owned_worktree.as_deref());
                if let Some(reason) = apply_effects(&mut process, &events, effects)? {
                    handle_forced_stop(&mut process, &mut protocol, &events, &reason)?;
                    owned_worktree = None;
                }
                continue;
            }

            loop {
                match commands.try_recv() {
                    Ok(ProviderCommand::Shutdown) => {
                        if protocol.has_active_turn() {
                            emit_all(
                                &events,
                                protocol.process_lost(
                                    "Agent World shut down while the Codex turn was active".into(),
                                    String::new(),
                                ),
                            )?;
                        }
                        if let Some(mut child) = process.take() {
                            let _ = child.stop(false, SHUTDOWN_TIMEOUT);
                        }
                        return Ok(());
                    }
                    Ok(command) => {
                        command.validate()?;
                        let effects = protocol.handle_command(
                            command,
                            Instant::now(),
                            owned_worktree.as_deref(),
                        );
                        if let Some(reason) = apply_effects(&mut process, &events, effects)? {
                            handle_forced_stop(&mut process, &mut protocol, &events, &reason)?;
                            owned_worktree = None;
                            break;
                        }
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        if let Some(mut child) = process.take() {
                            let _ = child.stop(false, SHUTDOWN_TIMEOUT);
                        }
                        return Ok(());
                    }
                }
            }
            let Some(child) = process.as_mut() else {
                continue;
            };

            match child.incoming.recv_timeout(PROCESS_POLL_INTERVAL) {
                Ok(ReaderMessage::Line(line)) => {
                    let effects = protocol.handle_line(&line, Instant::now());
                    if let Some(reason) = apply_effects(&mut process, &events, effects)? {
                        handle_forced_stop(&mut process, &mut protocol, &events, &reason)?;
                        owned_worktree = None;
                        continue;
                    }
                }
                Ok(ReaderMessage::LineTooLong { limit }) => {
                    let reason = format!(
                        "Codex protocol line exceeded the explicit {limit}-byte limit ({SUPPORTED_CODEX_VERSION})"
                    );
                    emit_all(&events, protocol.protocol_failure(reason.clone()))?;
                    handle_forced_stop(&mut process, &mut protocol, &events, &reason)?;
                    owned_worktree = None;
                    continue;
                }
                Ok(ReaderMessage::ReadError(error)) => {
                    let diagnostics = process
                        .as_ref()
                        .map(OwnedProcess::diagnostics)
                        .unwrap_or_default();
                    emit_all(&events, protocol.process_lost(error, diagnostics))?;
                    handle_forced_stop(
                        &mut process,
                        &mut protocol,
                        &events,
                        "Codex stdout failed",
                    )?;
                    owned_worktree = None;
                    continue;
                }
                Ok(ReaderMessage::Eof) | Err(RecvTimeoutError::Disconnected) => {
                    let diagnostics = process
                        .as_ref()
                        .map(OwnedProcess::diagnostics)
                        .unwrap_or_default();
                    emit_all(
                        &events,
                        protocol.process_lost(
                            "Codex stdout closed before the owned process reached a terminal state"
                                .into(),
                            diagnostics,
                        ),
                    )?;
                    handle_forced_stop(
                        &mut process,
                        &mut protocol,
                        &events,
                        "Codex stdout closed unexpectedly",
                    )?;
                    owned_worktree = None;
                    continue;
                }
                Err(RecvTimeoutError::Timeout) => {}
            }

            let effects = protocol.on_tick(Instant::now());
            if let Some(reason) = apply_effects(&mut process, &events, effects)? {
                handle_forced_stop(&mut process, &mut protocol, &events, &reason)?;
                owned_worktree = None;
                continue;
            }

            let status = process
                .as_mut()
                .expect("process exists while polling")
                .try_wait()?;
            if let Some(status) = status {
                let containment_error = process
                    .as_ref()
                    .and_then(|child| child.terminate_contained().err());
                if let Some(child) = process.as_mut() {
                    for message in child.drain_after_exit(EXIT_DRAIN_TIMEOUT) {
                        if let ReaderMessage::Line(line) = message {
                            let effects = protocol.handle_line(&line, Instant::now());
                            let _ = apply_effects(&mut process, &events, effects)?;
                        }
                    }
                }
                let diagnostics = process
                    .as_ref()
                    .map(OwnedProcess::diagnostics)
                    .unwrap_or_default();
                if protocol.has_active_turn() {
                    let reason = containment_error.map_or_else(
                        || format!("Codex app-server exited unexpectedly with {status}"),
                        |error| {
                            format!(
                                "Codex app-server exited unexpectedly with {status}; containment cleanup failed: {error}"
                            )
                        },
                    );
                    emit_all(&events, protocol.process_lost(reason, diagnostics))?;
                }
                if let Some(mut child) = process.take() {
                    child.finish_readers();
                }
                protocol = ProtocolMachine::new(self.startup_timeout, self.interrupt_timeout);
                owned_worktree = None;
            }
        }
    }
}

impl ProviderRunner for CodexAppServerRunner {
    fn readiness(&self) -> ProviderReadiness {
        let executable = match self.resolve_executable() {
            Ok(executable) => executable,
            Err(error) => {
                return ProviderReadiness::Unavailable {
                    diagnostic: bounded_diagnostic(&error),
                };
            }
        };
        let readiness = readiness_from_version(installed_version(&executable));
        if readiness != ProviderReadiness::Available {
            return readiness;
        }
        match verify_codex_disabled_features(&executable) {
            Ok(()) => ProviderReadiness::Available,
            Err(error) => ProviderReadiness::Unavailable {
                diagnostic: bounded_diagnostic(&sanitize_diagnostic(&error)),
            },
        }
    }

    fn run(
        self: Box<Self>,
        commands: Receiver<ProviderCommand>,
        events: SyncSender<ProviderEvent>,
    ) -> Result<(), String> {
        self.run_loop(commands, events)
    }
}

fn readiness_from_version(version: Result<String, String>) -> ProviderReadiness {
    match version {
        Ok(installed) if installed == SUPPORTED_CODEX_VERSION => ProviderReadiness::Available,
        Ok(installed) => ProviderReadiness::UnsupportedVersion {
            installed,
            supported: SUPPORTED_CODEX_VERSION.into(),
        },
        Err(error) => ProviderReadiness::Unavailable {
            diagnostic: bounded_diagnostic(&error),
        },
    }
}

fn emit_orphan_command_failure(
    events: &SyncSender<ProviderEvent>,
    command: &ProviderCommand,
) -> Result<(), String> {
    let Some(turn_id) = command.turn_id() else {
        return Ok(());
    };
    let event = ProviderEvent::Failed {
        turn_id,
        provider_event_id: format!("codex:{turn_id}:orphan-command"),
        diagnostic:
            "Codex has no owned app-server process for this command; a durable Start is required"
                .into(),
    };
    event.validate()?;
    events.send(event).map_err(|error| error.to_string())
}

fn emit_start_failure(
    events: &SyncSender<ProviderEvent>,
    turn_id: Uuid,
    kind: &str,
    diagnostic: &str,
) -> Result<(), String> {
    let event = ProviderEvent::Failed {
        turn_id,
        provider_event_id: format!("codex:{turn_id}:start-{kind}"),
        diagnostic: bounded_diagnostic(&sanitize_diagnostic(diagnostic)),
    };
    event.validate()?;
    events.send(event).map_err(|error| error.to_string())
}

fn canonical_worktree(path: &Path) -> Result<PathBuf, String> {
    let canonical = std::fs::canonicalize(path).map_err(|error| {
        format!(
            "could not resolve verified worktree {}: {error}",
            path.display()
        )
    })?;
    if !canonical.is_dir() {
        return Err(format!(
            "verified worktree {} is not a directory",
            canonical.display()
        ));
    }
    Ok(canonical)
}

fn verify_installed_version(executable: &Path) -> Result<(), String> {
    let version = installed_version(executable)?;
    verify_codex_version(&version)
}

fn installed_version(executable: &Path) -> Result<String, String> {
    let output = Command::new(executable)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| format!("could not inspect Codex version: {error}"))?;
    let version = if output.stdout.is_empty() {
        String::from_utf8_lossy(&output.stderr).into_owned()
    } else {
        String::from_utf8_lossy(&output.stdout).into_owned()
    };
    let version = version.trim();
    if !output.status.success() {
        return Err(format!(
            "Codex --version failed: {}",
            sanitize_diagnostic(version)
        ));
    }
    if version.is_empty() {
        return Err("Codex --version returned no version".into());
    }
    if version.len() > MAX_PROVIDER_ID_BYTES {
        return Err(format!(
            "Codex --version returned {} bytes; maximum is {MAX_PROVIDER_ID_BYTES}",
            version.len()
        ));
    }
    Ok(version.to_owned())
}

const APP_SERVER_SUBCOMMAND: &[&str] = &["app-server", "--stdio", "--strict-config"];

fn app_server_arguments() -> Vec<String> {
    codex_launch_arguments(APP_SERVER_SUBCOMMAND)
}

/// Exact `TurnStartParams.sandboxPolicy` shape declared by Codex app-server 0.146.0.
///
/// The canonical verified worktree is the sole writable root. Temporary-directory exceptions and
/// sandbox network access are disabled explicitly so the transport matches the durable
/// `isolated_workspace_write_on_request_v1` policy instead of relying on provider defaults.
fn workspace_write_sandbox_policy(canonical_worktree: &str) -> Value {
    json!({
        "type": CODEX_TURN_SANDBOX_TYPE,
        "writableRoots": [canonical_worktree],
        "networkAccess": false,
        "excludeSlashTmp": true,
        "excludeTmpdirEnvVar": true
    })
}

struct OwnedProcess {
    child: Child,
    containment: ProcessContainment,
    stdin: Option<ChildStdin>,
    incoming: Receiver<ReaderMessage>,
    stderr: Arc<Mutex<DiagnosticRing>>,
    stdout_reader: Option<JoinHandle<()>>,
    stderr_reader: Option<JoinHandle<()>>,
}

impl OwnedProcess {
    fn spawn(executable: &Path, worktree: &Path) -> Result<Self, String> {
        let mut command = Command::new(executable);
        command
            .args(app_server_arguments())
            .current_dir(worktree)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command
            .spawn()
            .map_err(|error| format!("could not launch owned Codex app-server: {error}"))?;
        let containment = match ProcessContainment::attach(&child) {
            Ok(containment) => containment,
            Err(error) => return Err(stop_partially_spawned(&mut child, &error)),
        };
        let stdin = match child.stdin.take() {
            Some(stdin) => stdin,
            None => {
                return Err(stop_partially_spawned(
                    &mut child,
                    "Codex stdin was unavailable",
                ));
            }
        };
        let stdout = match child.stdout.take() {
            Some(stdout) => stdout,
            None => {
                return Err(stop_partially_spawned(
                    &mut child,
                    "Codex stdout was unavailable",
                ));
            }
        };
        let stderr = match child.stderr.take() {
            Some(stderr) => stderr,
            None => {
                return Err(stop_partially_spawned(
                    &mut child,
                    "Codex stderr was unavailable",
                ));
            }
        };
        let (incoming_tx, incoming) = mpsc::sync_channel(READER_QUEUE_CAPACITY);
        let stdout_reader = match thread::Builder::new()
            .name("agent-world-codex-stdout".into())
            .spawn(move || stdout_reader_loop(stdout, incoming_tx))
        {
            Ok(reader) => reader,
            Err(error) => {
                return Err(stop_partially_spawned(
                    &mut child,
                    &format!("could not start Codex stdout reader: {error}"),
                ));
            }
        };
        let diagnostic_ring = Arc::new(Mutex::new(DiagnosticRing::new(STDERR_RING_BYTES)));
        let stderr_ring = Arc::clone(&diagnostic_ring);
        let stderr_reader = match thread::Builder::new()
            .name("agent-world-codex-stderr".into())
            .spawn(move || stderr_reader_loop(stderr, stderr_ring))
        {
            Ok(reader) => reader,
            Err(error) => {
                let message = stop_partially_spawned(
                    &mut child,
                    &format!("could not start Codex stderr reader: {error}"),
                );
                let _ = stdout_reader.join();
                return Err(message);
            }
        };
        Ok(Self {
            child,
            containment,
            stdin: Some(stdin),
            incoming,
            stderr: diagnostic_ring,
            stdout_reader: Some(stdout_reader),
            stderr_reader: Some(stderr_reader),
        })
    }

    fn write_json(&mut self, value: &Value) -> Result<(), String> {
        let encoded = serde_json::to_vec(value).map_err(|error| error.to_string())?;
        if encoded.len() > MAX_PROTOCOL_LINE_BYTES {
            return Err(format!(
                "outgoing Codex protocol line is {} bytes; maximum is {MAX_PROTOCOL_LINE_BYTES}",
                encoded.len()
            ));
        }
        let stdin = self
            .stdin
            .as_mut()
            .ok_or_else(|| "Codex stdin is closed".to_owned())?;
        stdin
            .write_all(&encoded)
            .map_err(|error| error.to_string())?;
        stdin.write_all(b"\n").map_err(|error| error.to_string())?;
        stdin.flush().map_err(|error| error.to_string())
    }

    fn try_wait(&mut self) -> Result<Option<ExitStatus>, String> {
        self.child.try_wait().map_err(|error| error.to_string())
    }

    fn diagnostics(&self) -> String {
        self.stderr
            .lock()
            .map(|ring| ring.render())
            .unwrap_or_else(|_| "[Codex diagnostic ring unavailable]".into())
    }

    fn drain_after_exit(&mut self, timeout: Duration) -> Vec<ReaderMessage> {
        let deadline = Instant::now() + timeout;
        let mut messages = Vec::new();
        while Instant::now() < deadline {
            match self
                .incoming
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            {
                Ok(ReaderMessage::Eof) | Err(RecvTimeoutError::Disconnected) => break,
                Ok(message) => messages.push(message),
                Err(RecvTimeoutError::Timeout) => break,
            }
        }
        messages
    }

    fn stop(&mut self, force: bool, timeout: Duration) -> Result<bool, String> {
        self.stdin.take();
        let deadline = Instant::now() + timeout;
        while !force && Instant::now() < deadline {
            if self.try_wait()?.is_some() {
                self.containment.terminate()?;
                self.containment.prove_empty(deadline)?;
                self.finish_readers();
                return Ok(false);
            }
            thread::sleep(PROCESS_POLL_INTERVAL);
        }
        if self.try_wait()?.is_none() {
            self.containment.terminate()?;
            let _ = self.child.kill();
            self.child.wait().map_err(|error| error.to_string())?;
            self.containment
                .prove_empty(Instant::now() + SHUTDOWN_TIMEOUT)?;
            self.finish_readers();
            return Ok(true);
        }
        self.containment.terminate()?;
        self.containment
            .prove_empty(Instant::now() + SHUTDOWN_TIMEOUT)?;
        self.finish_readers();
        Ok(false)
    }

    fn terminate_contained(&self) -> Result<(), String> {
        self.containment.terminate()
    }

    fn finish_readers(&mut self) {
        if let Some(reader) = self.stdout_reader.take() {
            let _ = reader.join();
        }
        if let Some(reader) = self.stderr_reader.take() {
            let _ = reader.join();
        }
    }
}

fn stop_partially_spawned(child: &mut Child, message: &str) -> String {
    let _ = child.kill();
    let cleanup = child.wait();
    match cleanup {
        Ok(_) => message.to_owned(),
        Err(error) => format!("{message}; could not reap partial Codex child: {error}"),
    }
}

impl Drop for OwnedProcess {
    fn drop(&mut self) {
        let _ = self.containment.terminate();
        if self.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        self.finish_readers();
    }
}

#[cfg(windows)]
struct ProcessContainment {
    job: windows_sys::Win32::Foundation::HANDLE,
}

// SAFETY: the immutable Job Object handle remains owned by `ProcessContainment` and is closed only
// from its `Drop` implementation after the containing process supervisor is done.
#[cfg(windows)]
unsafe impl Send for ProcessContainment {}

#[cfg(windows)]
impl ProcessContainment {
    fn attach(child: &Child) -> Result<Self, String> {
        use std::{ffi::c_void, mem::size_of, os::windows::io::AsRawHandle, ptr};
        use windows_sys::Win32::System::JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
            SetInformationJobObject,
        };

        // SAFETY: pointers are null where allowed or reference correctly sized initialized values;
        // ownership of the returned handle transfers to `Self` and is closed exactly once.
        unsafe {
            let job = CreateJobObjectW(ptr::null(), ptr::null());
            if job.is_null() {
                return Err(format!(
                    "could not create Codex Job Object: {}",
                    io::Error::last_os_error()
                ));
            }
            let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
            limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            if SetInformationJobObject(
                job,
                JobObjectExtendedLimitInformation,
                (&raw const limits).cast::<c_void>(),
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            ) == 0
            {
                let error = io::Error::last_os_error();
                let _ = windows_sys::Win32::Foundation::CloseHandle(job);
                return Err(format!("could not configure Codex Job Object: {error}"));
            }
            if AssignProcessToJobObject(job, child.as_raw_handle().cast()) == 0 {
                let error = io::Error::last_os_error();
                let _ = windows_sys::Win32::Foundation::CloseHandle(job);
                return Err(format!("could not contain Codex process tree: {error}"));
            }
            Ok(Self { job })
        }
    }

    fn terminate(&self) -> Result<(), String> {
        // SAFETY: `self.job` is a live Job Object handle owned by `self`.
        if unsafe { windows_sys::Win32::System::JobObjects::TerminateJobObject(self.job, 1) } == 0 {
            Err(format!(
                "could not terminate owned Codex process tree: {}",
                io::Error::last_os_error()
            ))
        } else {
            Ok(())
        }
    }

    fn prove_empty(&self, deadline: Instant) -> Result<(), String> {
        use std::{ffi::c_void, mem::size_of, ptr};
        use windows_sys::Win32::System::JobObjects::{
            JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JobObjectBasicAccountingInformation,
            QueryInformationJobObject,
        };
        loop {
            let mut accounting = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
            // SAFETY: the output pointer references a correctly sized initialized structure for
            // the duration of this query.
            let queried = unsafe {
                QueryInformationJobObject(
                    self.job,
                    JobObjectBasicAccountingInformation,
                    (&raw mut accounting).cast::<c_void>(),
                    size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                    ptr::null_mut(),
                )
            };
            if queried == 0 {
                return Err(format!(
                    "could not query owned Codex process count: {}",
                    io::Error::last_os_error()
                ));
            }
            if accounting.ActiveProcesses == 0 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "{} owned Codex processes remained at cleanup deadline",
                    accounting.ActiveProcesses
                ));
            }
            thread::sleep(PROCESS_POLL_INTERVAL);
        }
    }
}

#[cfg(windows)]
impl Drop for ProcessContainment {
    fn drop(&mut self) {
        // SAFETY: this is the unique owned Job Object handle and it is closed exactly once.
        unsafe {
            let _ = windows_sys::Win32::Foundation::CloseHandle(self.job);
        }
    }
}

#[cfg(not(windows))]
struct ProcessContainment;

#[cfg(not(windows))]
impl ProcessContainment {
    fn attach(_child: &Child) -> Result<Self, String> {
        Ok(Self)
    }

    fn terminate(&self) -> Result<(), String> {
        Ok(())
    }

    fn prove_empty(&self, _deadline: Instant) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Debug)]
enum ReaderMessage {
    Line(String),
    LineTooLong { limit: usize },
    ReadError(String),
    Eof,
}

fn stdout_reader_loop(stdout: impl Read, tx: SyncSender<ReaderMessage>) {
    let mut reader = BufReader::new(stdout);
    loop {
        match read_bounded_line(&mut reader, MAX_PROTOCOL_LINE_BYTES) {
            Ok(BoundedLine::Line(bytes)) => match String::from_utf8(bytes) {
                Ok(line) => {
                    match tx.try_send(ReaderMessage::Line(line)) {
                        Ok(()) => {}
                        Err(TrySendError::Full(_)) => {
                            // Queue saturation is itself terminal. Dropping the sender makes the
                            // supervisor fail the owned process closed without allocating more.
                            break;
                        }
                        Err(TrySendError::Disconnected(_)) => break,
                    }
                }
                Err(_) => {
                    let _ = tx.try_send(ReaderMessage::ReadError(
                        "Codex stdout contained invalid UTF-8".into(),
                    ));
                    break;
                }
            },
            Ok(BoundedLine::TooLong) => {
                let _ = tx.try_send(ReaderMessage::LineTooLong {
                    limit: MAX_PROTOCOL_LINE_BYTES,
                });
                break;
            }
            Ok(BoundedLine::Eof) => {
                let _ = tx.try_send(ReaderMessage::Eof);
                break;
            }
            Err(error) => {
                let _ = tx.try_send(ReaderMessage::ReadError(error.to_string()));
                break;
            }
        }
    }
}

fn stderr_reader_loop(stderr: impl Read, ring: Arc<Mutex<DiagnosticRing>>) {
    let mut reader = BufReader::new(stderr);
    loop {
        let line = match read_bounded_line(&mut reader, MAX_DIAGNOSTIC_LINE_BYTES) {
            Ok(BoundedLine::Line(bytes)) => sanitize_diagnostic(&String::from_utf8_lossy(&bytes)),
            Ok(BoundedLine::TooLong) => format!(
                "[Codex diagnostic line rejected: exceeded {MAX_DIAGNOSTIC_LINE_BYTES} bytes]"
            ),
            Ok(BoundedLine::Eof) => break,
            Err(error) => format!("[Codex stderr read failed: {error}]"),
        };
        if let Ok(mut ring) = ring.lock() {
            ring.push(&line);
        }
    }
}

enum BoundedLine {
    Line(Vec<u8>),
    TooLong,
    Eof,
}

fn read_bounded_line<R: BufRead>(reader: &mut R, limit: usize) -> io::Result<BoundedLine> {
    let mut output = Vec::with_capacity(limit.min(4096));
    loop {
        let buffer = reader.fill_buf()?;
        if buffer.is_empty() {
            return if output.is_empty() {
                Ok(BoundedLine::Eof)
            } else {
                Ok(BoundedLine::Line(output))
            };
        }
        if let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
            if output.len().saturating_add(newline) > limit {
                reader.consume(newline + 1);
                return Ok(BoundedLine::TooLong);
            }
            output.extend_from_slice(&buffer[..newline]);
            reader.consume(newline + 1);
            if output.last() == Some(&b'\r') {
                output.pop();
            }
            return Ok(BoundedLine::Line(output));
        }
        let consumed = buffer.len();
        if output.len().saturating_add(consumed) > limit {
            reader.consume(consumed);
            drain_line(reader)?;
            return Ok(BoundedLine::TooLong);
        }
        output.extend_from_slice(buffer);
        reader.consume(consumed);
    }
}

fn drain_line<R: BufRead>(reader: &mut R) -> io::Result<()> {
    loop {
        let buffer = reader.fill_buf()?;
        if buffer.is_empty() {
            return Ok(());
        }
        if let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
            reader.consume(newline + 1);
            return Ok(());
        }
        let consumed = buffer.len();
        reader.consume(consumed);
    }
}

struct DiagnosticRing {
    bytes: VecDeque<u8>,
    capacity: usize,
}

impl DiagnosticRing {
    fn new(capacity: usize) -> Self {
        Self {
            bytes: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn push(&mut self, line: &str) {
        for byte in line.as_bytes().iter().copied().chain([b'\n']) {
            if self.bytes.len() == self.capacity {
                self.bytes.pop_front();
            }
            self.bytes.push_back(byte);
        }
    }

    fn render(&self) -> String {
        String::from_utf8_lossy(&self.bytes.iter().copied().collect::<Vec<_>>())
            .trim()
            .to_owned()
    }
}

fn sanitize_diagnostic(input: &str) -> String {
    let lower = input.to_ascii_lowercase();
    if [
        "authorization",
        "api_key",
        "api-key",
        "apikey",
        "access_token",
        "refresh_token",
        "cookie",
        "secret",
        "bearer ",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
    {
        "[redacted sensitive Codex diagnostic]".into()
    } else {
        input.to_owned()
    }
}

#[derive(Debug)]
enum Effect {
    Write(Value),
    Emit(ProviderEvent),
    ForceStop(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RequestKind {
    Initialize,
    ThreadStart,
    ThreadResume,
    TurnStart,
    Interrupt,
}

struct PendingRequest {
    kind: RequestKind,
    method: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InteractionKind {
    CommandApproval,
    FileApproval,
    UserInput,
}

struct PendingInteraction {
    rpc_id: Value,
    rpc_key: String,
    fingerprint: u64,
    kind: InteractionKind,
    turn_id: Uuid,
    question_ids: Vec<String>,
}

struct ActiveTurn {
    turn_id: Uuid,
    _thread_id: Uuid,
    worktree_path: PathBuf,
    prompt: String,
    requested_session: Option<ProviderSessionCursor>,
    provider_thread_id: Option<String>,
    provider_turn_id: Option<String>,
    turn_start_response_id: Option<String>,
    provider_side_effect_possible: bool,
    startup_deadline: Option<Instant>,
    interrupt_deadline: Option<Instant>,
    interrupt_acknowledged: bool,
}

struct CoalescedOutput {
    thread_id: String,
    turn_id: String,
    item_id: String,
    text: String,
    deadline: Instant,
}

struct ProtocolMachine {
    initialized: bool,
    startup_timeout: Duration,
    interrupt_timeout: Duration,
    next_request_id: u64,
    next_event_id: u64,
    next_interaction_id: u64,
    active: Option<ActiveTurn>,
    pending_requests: BTreeMap<String, PendingRequest>,
    stale_response_ids: VecDeque<String>,
    interactions: BTreeMap<String, PendingInteraction>,
    coalesced: Option<CoalescedOutput>,
    terminal_fingerprints: VecDeque<u64>,
}

impl ProtocolMachine {
    fn new(startup_timeout: Duration, interrupt_timeout: Duration) -> Self {
        Self {
            initialized: false,
            startup_timeout,
            interrupt_timeout,
            next_request_id: 1,
            next_event_id: 1,
            next_interaction_id: 1,
            active: None,
            pending_requests: BTreeMap::new(),
            stale_response_ids: VecDeque::with_capacity(MAX_PENDING_REQUESTS * 2),
            interactions: BTreeMap::new(),
            coalesced: None,
            terminal_fingerprints: VecDeque::with_capacity(8),
        }
    }

    fn has_active_turn(&self) -> bool {
        self.active.is_some()
    }

    fn reset_transport(&mut self) {
        self.initialized = false;
        self.pending_requests.clear();
        self.interactions.clear();
        self.coalesced = None;
        self.active = None;
    }

    fn handle_command(
        &mut self,
        command: ProviderCommand,
        now: Instant,
        owned_worktree: Option<&Path>,
    ) -> Vec<Effect> {
        match command {
            ProviderCommand::Start {
                turn_id,
                thread_id,
                worktree_path,
                prompt,
                session,
            } => self.start_turn(
                turn_id,
                thread_id,
                worktree_path,
                prompt,
                session,
                now,
                owned_worktree,
            ),
            ProviderCommand::ApprovalResponse {
                turn_id,
                interaction_id,
                decision,
            } => self.approval_response(turn_id, &interaction_id, decision),
            ProviderCommand::UserInputResponse {
                turn_id,
                interaction_id,
                answers,
            } => self.user_input_response(turn_id, &interaction_id, answers),
            ProviderCommand::Interrupt { turn_id } => self.interrupt(turn_id, now),
            ProviderCommand::Shutdown => Vec::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn start_turn(
        &mut self,
        turn_id: Uuid,
        thread_id: Uuid,
        worktree_path: PathBuf,
        prompt: String,
        session: Option<ProviderSessionCursor>,
        now: Instant,
        owned_worktree: Option<&Path>,
    ) -> Vec<Effect> {
        if self.active.is_some() {
            return vec![Effect::Emit(ProviderEvent::Failed {
                turn_id,
                provider_event_id: self.event_id(turn_id, "busy"),
                diagnostic: "the single Codex operator already has an active turn".into(),
            })];
        }
        let canonical = match canonical_worktree(&worktree_path) {
            Ok(path) => path,
            Err(error) => return vec![self.standalone_failure(turn_id, "worktree", error)],
        };
        if owned_worktree.is_some_and(|owned| owned != canonical) {
            return vec![self.standalone_failure(
                turn_id,
                "worktree-mismatch",
                "the owned Codex process cannot switch verified worktrees; start a new provider port"
                    .into(),
            )];
        }
        if let Some(session) = &session {
            let expected = resume_cursor(&session.session_id);
            if session.resume_cursor != expected {
                return vec![self.standalone_failure(
                    turn_id,
                    "resume-rejected",
                    "Codex resume was rejected because the durable session cursor does not match the durable session identity"
                        .into(),
                )];
            }
        }
        self.active = Some(ActiveTurn {
            turn_id,
            _thread_id: thread_id,
            worktree_path: canonical,
            prompt,
            requested_session: session,
            provider_thread_id: None,
            provider_turn_id: None,
            turn_start_response_id: None,
            provider_side_effect_possible: false,
            startup_deadline: Some(now + self.startup_timeout),
            interrupt_deadline: None,
            interrupt_acknowledged: false,
        });
        let mut effects = vec![Effect::Emit(ProviderEvent::Starting {
            turn_id,
            provider_event_id: self.event_id(turn_id, "starting"),
        })];
        if self.initialized {
            effects.extend(self.begin_thread_request());
        } else {
            effects.extend(self.request(
                RequestKind::Initialize,
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "agent-world",
                        "title": "Agent World",
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    "capabilities": {"experimentalApi": true}
                }),
            ));
        }
        effects
    }

    fn begin_thread_request(&mut self) -> Vec<Effect> {
        let (cwd, requested_session) = {
            let Some(active) = self.active.as_mut() else {
                return Vec::new();
            };
            active.provider_side_effect_possible = true;
            (
                active.worktree_path.to_string_lossy().into_owned(),
                active.requested_session.clone(),
            )
        };
        if let Some(session) = requested_session {
            self.request(
                RequestKind::ThreadResume,
                "thread/resume",
                json!({
                    "threadId": session.session_id,
                    "cwd": cwd,
                    "approvalPolicy": CODEX_APPROVAL_POLICY,
                    "sandbox": CODEX_THREAD_SANDBOX_MODE,
                    "runtimeWorkspaceRoots": [cwd]
                }),
            )
        } else {
            self.request(
                RequestKind::ThreadStart,
                "thread/start",
                json!({
                    "cwd": cwd,
                    "approvalPolicy": CODEX_APPROVAL_POLICY,
                    "sandbox": CODEX_THREAD_SANDBOX_MODE,
                    "ephemeral": false,
                    "experimentalRawEvents": false,
                    "runtimeWorkspaceRoots": [cwd],
                    "environments": []
                }),
            )
        }
    }

    fn begin_turn_request(&mut self) -> Vec<Effect> {
        let Some(active) = self.active.as_ref() else {
            return Vec::new();
        };
        let Some(provider_thread_id) = active.provider_thread_id.clone() else {
            return self.fail_closed("Codex thread response did not provide a thread id".into());
        };
        let prompt = active.prompt.clone();
        let cwd = active.worktree_path.to_string_lossy().into_owned();
        let sandbox_policy = workspace_write_sandbox_policy(&cwd);
        self.request(
            RequestKind::TurnStart,
            "turn/start",
            json!({
                "threadId": provider_thread_id,
                "input": [{"type": "text", "text": prompt}],
                "cwd": cwd,
                "clientUserMessageId": active.turn_id.to_string(),
                "approvalPolicy": CODEX_APPROVAL_POLICY,
                "sandboxPolicy": sandbox_policy,
                "runtimeWorkspaceRoots": [cwd],
                "environments": []
            }),
        )
    }

    fn request(&mut self, kind: RequestKind, method: &'static str, params: Value) -> Vec<Effect> {
        if self.pending_requests.len() >= MAX_PENDING_REQUESTS {
            return self.fail_closed(format!(
                "Codex request queue exceeded its {MAX_PENDING_REQUESTS}-request bound"
            ));
        }
        let id = format!("aw-{}", self.next_request_id);
        self.next_request_id = self.next_request_id.saturating_add(1);
        self.pending_requests
            .insert(id.clone(), PendingRequest { kind, method });
        vec![Effect::Write(
            json!({"id": id, "method": method, "params": params}),
        )]
    }

    fn handle_line(&mut self, line: &str, now: Instant) -> Vec<Effect> {
        let value: Value = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(error) => {
                return self.fail_closed(format!(
                    "Codex emitted invalid newline-delimited JSON for {SUPPORTED_CODEX_VERSION}: {error}"
                ));
            }
        };
        let Some(object) = value.as_object() else {
            return self.fail_closed(format!(
                "Codex emitted a non-object protocol message for {SUPPORTED_CODEX_VERSION}"
            ));
        };
        if let Some(method) = object.get("method").and_then(Value::as_str) {
            let fingerprint = fnv1a(line.as_bytes());
            if method == "turn/completed" && self.terminal_fingerprints.contains(&fingerprint) {
                return Vec::new();
            }
            if object.contains_key("id") {
                self.handle_server_request(&value, method, fingerprint)
            } else {
                self.handle_notification(&value, method, fingerprint, now)
            }
        } else if let Some(id) = object.get("id") {
            self.handle_response(id, &value)
        } else {
            self.fail_closed(format!(
                "Codex emitted a protocol message with neither method nor id ({SUPPORTED_CODEX_VERSION})"
            ))
        }
    }

    fn handle_response(&mut self, id: &Value, value: &Value) -> Vec<Effect> {
        let Some(key) = request_id_key(id) else {
            return self.fail_closed(format!(
                "Codex response used an unsupported request id shape ({SUPPORTED_CODEX_VERSION})"
            ));
        };
        let Some(request) = self.pending_requests.remove(&key) else {
            if self.stale_response_ids.contains(&key) {
                return Vec::new();
            }
            return self.fail_closed(format!(
                "Codex returned an uncorrelated response id for {SUPPORTED_CODEX_VERSION}"
            ));
        };
        if let Some(error) = value.get("error") {
            let diagnostic = bounded_diagnostic(&format!(
                "Codex {} failed: {}",
                request.method,
                sanitize_diagnostic(&error.to_string())
            ));
            return self.fail_closed(diagnostic);
        }
        match request.kind {
            RequestKind::Initialize => self.initialize_response(value),
            RequestKind::ThreadStart | RequestKind::ThreadResume => {
                self.thread_response(value, request.kind)
            }
            RequestKind::TurnStart => self.turn_start_response(value),
            RequestKind::Interrupt => self.interrupt_response(),
        }
    }

    fn initialize_response(&mut self, value: &Value) -> Vec<Effect> {
        let result = &value["result"];
        let valid = ["userAgent", "codexHome", "platformFamily", "platformOs"]
            .iter()
            .all(|field| result.get(field).and_then(Value::as_str).is_some());
        if !valid || result["platformOs"] != "windows" {
            return self.fail_closed(format!(
                "Codex initialize schema drifted or did not report Windows ({SUPPORTED_CODEX_VERSION})"
            ));
        }
        self.initialized = true;
        let mut effects = vec![Effect::Write(json!({"method": "initialized"}))];
        effects.extend(self.begin_thread_request());
        effects
    }

    fn thread_response(&mut self, value: &Value, kind: RequestKind) -> Vec<Effect> {
        let Some(provider_thread_id) = value
            .pointer("/result/thread/id")
            .and_then(Value::as_str)
            .map(str::to_owned)
        else {
            return self.fail_closed(format!(
                "Codex {} response omitted result.thread.id ({SUPPORTED_CODEX_VERSION})",
                if kind == RequestKind::ThreadResume {
                    "thread/resume"
                } else {
                    "thread/start"
                }
            ));
        };
        if let Err(error) = validate_provider_identifier("Codex thread id", &provider_thread_id) {
            return self.fail_closed(error);
        }
        let (turn_id, requested_session) = match self.active.as_ref() {
            Some(active) => (active.turn_id, active.requested_session.clone()),
            None => {
                return self
                    .fail_closed("Codex thread response arrived without an active turn".into());
            }
        };
        if let Some(requested) = &requested_session
            && requested.session_id != provider_thread_id
        {
            return self.fail_closed(format!(
                "Codex resumed thread {:?} instead of the durably recorded session {:?}",
                provider_thread_id, requested.session_id
            ));
        }
        if let Some(active) = self.active.as_mut() {
            active.provider_thread_id = Some(provider_thread_id.clone());
        }
        let session = session_cursor(&provider_thread_id);
        let event = if kind == RequestKind::ThreadResume {
            ProviderEvent::Resumed {
                turn_id,
                provider_event_id: self.event_id(turn_id, "resumed"),
                session,
            }
        } else {
            ProviderEvent::SessionEstablished {
                turn_id,
                provider_event_id: self.event_id(turn_id, "session"),
                session,
            }
        };
        let mut effects = vec![Effect::Emit(event)];
        effects.extend(self.begin_turn_request());
        effects
    }

    fn turn_start_response(&mut self, value: &Value) -> Vec<Effect> {
        let Some(provider_turn_id) = value
            .pointer("/result/turn/id")
            .and_then(Value::as_str)
            .map(str::to_owned)
        else {
            return self.fail_closed(format!(
                "Codex turn/start response omitted result.turn.id ({SUPPORTED_CODEX_VERSION})"
            ));
        };
        if let Err(error) =
            validate_provider_identifier("Codex turn/start response id", &provider_turn_id)
        {
            return self.fail_closed(error);
        }
        let Some(active) = self.active.as_mut() else {
            return self
                .fail_closed("Codex turn/start response arrived without an active turn".into());
        };
        if active
            .provider_turn_id
            .as_ref()
            .is_some_and(|seen| seen != &provider_turn_id)
        {
            return self
                .fail_closed("Codex turn/start response disagreed with turn/started".into());
        }
        active.turn_start_response_id = Some(provider_turn_id);
        Vec::new()
    }

    fn handle_notification(
        &mut self,
        value: &Value,
        method: &str,
        fingerprint: u64,
        _now: Instant,
    ) -> Vec<Effect> {
        match method {
            "turn/started" => self.turn_started(value),
            "item/agentMessage/delta" => self.assistant_delta(value),
            "turn/completed" => {
                self.remember_terminal(fingerprint);
                self.turn_completed(value)
            }
            "error" => {
                let message = value
                    .pointer("/params/error/message")
                    .or_else(|| value.pointer("/params/message"))
                    .and_then(Value::as_str)
                    .unwrap_or("Codex emitted an error notification");
                self.fail_closed(bounded_diagnostic(&sanitize_diagnostic(message)))
            }
            method if IGNORED_NOTIFICATIONS.contains(&method) => Vec::new(),
            _ => self.fail_closed(format!(
                "unknown Codex protocol method {method:?} from installed {SUPPORTED_CODEX_VERSION}; schema drift fails closed"
            )),
        }
    }

    fn turn_started(&mut self, value: &Value) -> Vec<Effect> {
        let Some(thread_id) = value.pointer("/params/threadId").and_then(Value::as_str) else {
            return self.schema_failure("turn/started", "params.threadId");
        };
        let Some(turn_id) = value.pointer("/params/turn/id").and_then(Value::as_str) else {
            return self.schema_failure("turn/started", "params.turn.id");
        };
        if let Err(error) = self.correlate_turn_started(thread_id, turn_id) {
            return self.fail_closed(error);
        }
        if let Some(active) = self.active.as_mut() {
            active.provider_turn_id = Some(turn_id.to_owned());
            active.startup_deadline = None;
        }
        Vec::new()
    }

    fn assistant_delta(&mut self, value: &Value) -> Vec<Effect> {
        let params = &value["params"];
        let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
            return self.schema_failure("item/agentMessage/delta", "params.threadId");
        };
        let Some(turn_id) = params.get("turnId").and_then(Value::as_str) else {
            return self.schema_failure("item/agentMessage/delta", "params.turnId");
        };
        let Some(item_id) = params.get("itemId").and_then(Value::as_str) else {
            return self.schema_failure("item/agentMessage/delta", "params.itemId");
        };
        let Some(delta) = params.get("delta").and_then(Value::as_str) else {
            return self.schema_failure("item/agentMessage/delta", "params.delta");
        };
        if delta.len() > MAX_OUTPUT_DELTA_BYTES {
            return self.fail_closed(format!(
                "Codex assistant delta is {} bytes; maximum is {MAX_OUTPUT_DELTA_BYTES}",
                delta.len()
            ));
        }
        if let Err(error) = self.correlate_provider_ids(thread_id, turn_id, Some(item_id)) {
            return self.fail_closed(error);
        }
        self.coalesce_delta(thread_id, turn_id, item_id, delta, Instant::now())
    }

    fn coalesce_delta(
        &mut self,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
        delta: &str,
        now: Instant,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.coalesced.as_ref().is_some_and(|chunk| {
            chunk.thread_id != thread_id || chunk.turn_id != turn_id || chunk.item_id != item_id
        }) && let Some(event) = self.flush_output()
        {
            effects.push(Effect::Emit(event));
        }
        for piece in utf8_chunks(delta, STREAM_COALESCE_BYTES) {
            let must_flush = self
                .coalesced
                .as_ref()
                .is_some_and(|chunk| chunk.text.len() + piece.len() > STREAM_COALESCE_BYTES);
            if must_flush && let Some(event) = self.flush_output() {
                effects.push(Effect::Emit(event));
            }
            let chunk = self.coalesced.get_or_insert_with(|| CoalescedOutput {
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item_id: item_id.to_owned(),
                text: String::new(),
                deadline: now + STREAM_FLUSH_INTERVAL,
            });
            chunk.text.push_str(piece);
            if chunk.text.len() == STREAM_COALESCE_BYTES
                && let Some(event) = self.flush_output()
            {
                effects.push(Effect::Emit(event));
            }
        }
        effects
    }

    fn turn_completed(&mut self, value: &Value) -> Vec<Effect> {
        let params = &value["params"];
        let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
            return self.schema_failure("turn/completed", "params.threadId");
        };
        let Some(turn_id) = params.pointer("/turn/id").and_then(Value::as_str) else {
            return self.schema_failure("turn/completed", "params.turn.id");
        };
        if let Err(error) = self.correlate_provider_ids(thread_id, turn_id, None) {
            return self.fail_closed(error);
        }
        let Some(status) = params.pointer("/turn/status").and_then(Value::as_str) else {
            return self.schema_failure("turn/completed", "params.turn.status");
        };
        let mut effects = Vec::new();
        if let Some(event) = self.flush_output() {
            effects.push(Effect::Emit(event));
        }
        let Some(active) = self.active.as_ref() else {
            return self.fail_closed("Codex completed a turn that is not active".into());
        };
        let local_turn_id = active.turn_id;
        let interrupt_was_requested = active.interrupt_deadline.is_some();
        let interrupt_rpc_acknowledged = active.interrupt_acknowledged;
        let session = session_cursor(thread_id);
        match status {
            "completed" => effects.push(Effect::Emit(ProviderEvent::Completed {
                turn_id: local_turn_id,
                provider_event_id: self.event_id(local_turn_id, "completed"),
                session,
            })),
            "interrupted" => {
                if !interrupt_was_requested {
                    return self.fail_closed(
                        "Codex reported an interrupted terminal turn without an outstanding durable interrupt"
                            .into(),
                    );
                }
                // The JSON-RPC response only acknowledges receipt of `turn/interrupt`. The
                // normalized acknowledgement is terminal by contract, so emit it exactly once
                // only after Codex reports the interrupted terminal state. Emitting a subsequent
                // `Completed` event would contradict the core's durable `interrupting` state.
                effects.push(Effect::Emit(ProviderEvent::InterruptAcknowledged {
                    turn_id: local_turn_id,
                    provider_event_id: self.event_id(local_turn_id, "interrupted"),
                    diagnostic: Some(if interrupt_rpc_acknowledged {
                        "Codex acknowledged the interrupt and reported the turn interrupted".into()
                    } else {
                        "Codex reported the turn interrupted before its interrupt response arrived"
                            .into()
                    }),
                }));
            }
            "failed" => {
                let message = params
                    .pointer("/turn/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("Codex turn failed without a diagnostic");
                effects.push(Effect::Emit(ProviderEvent::Failed {
                    turn_id: local_turn_id,
                    provider_event_id: self.event_id(local_turn_id, "failed"),
                    diagnostic: bounded_diagnostic(&sanitize_diagnostic(message)),
                }));
            }
            other => {
                return self.fail_closed(format!(
                    "Codex turn/completed had non-terminal status {other:?} ({SUPPORTED_CODEX_VERSION})"
                ));
            }
        }
        self.clear_active();
        effects
    }

    fn handle_server_request(
        &mut self,
        value: &Value,
        method: &str,
        fingerprint: u64,
    ) -> Vec<Effect> {
        let Some(rpc_id) = value.get("id").cloned() else {
            return self.schema_failure(method, "id");
        };
        let Some(rpc_key) = request_id_key(&rpc_id) else {
            return self.fail_closed(format!(
                "Codex server request {method:?} used an unsupported id shape"
            ));
        };
        if let Some(existing) = self
            .interactions
            .values()
            .find(|interaction| interaction.rpc_key == rpc_key)
        {
            return if existing.fingerprint == fingerprint {
                Vec::new()
            } else {
                self.fail_closed(format!(
                    "Codex reused server request id for altered {method:?} payload"
                ))
            };
        }
        if self.interactions.len() >= MAX_PENDING_INTERACTIONS {
            return self.fail_closed(format!(
                "Codex outstanding interaction queue exceeded its {MAX_PENDING_INTERACTIONS}-request bound"
            ));
        }
        match method {
            "item/commandExecution/requestApproval" => self.approval_request(
                value,
                rpc_id,
                rpc_key,
                fingerprint,
                InteractionKind::CommandApproval,
            ),
            "item/fileChange/requestApproval" => self.approval_request(
                value,
                rpc_id,
                rpc_key,
                fingerprint,
                InteractionKind::FileApproval,
            ),
            "item/tool/requestUserInput" => {
                self.user_input_request(value, rpc_id, rpc_key, fingerprint)
            }
            _ => self.fail_closed(format!(
                "unknown Codex server request method {method:?} from installed {SUPPORTED_CODEX_VERSION}; schema drift fails closed"
            )),
        }
    }

    fn approval_request(
        &mut self,
        value: &Value,
        rpc_id: Value,
        rpc_key: String,
        fingerprint: u64,
        kind: InteractionKind,
    ) -> Vec<Effect> {
        let method = match kind {
            InteractionKind::CommandApproval => "item/commandExecution/requestApproval",
            InteractionKind::FileApproval => "item/fileChange/requestApproval",
            InteractionKind::UserInput => unreachable!(),
        };
        let params = &value["params"];
        let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
            return self.schema_failure(method, "params.threadId");
        };
        let Some(turn_id) = params.get("turnId").and_then(Value::as_str) else {
            return self.schema_failure(method, "params.turnId");
        };
        let Some(item_id) = params.get("itemId").and_then(Value::as_str) else {
            return self.schema_failure(method, "params.itemId");
        };
        if let Err(error) = self.correlate_provider_ids(thread_id, turn_id, Some(item_id)) {
            return self.fail_closed(error);
        }
        let Some(local_turn_id) = self.active.as_ref().map(|active| active.turn_id) else {
            return self.fail_closed("Codex requested approval without an active turn".into());
        };
        let interaction_id = self.interaction_id(local_turn_id);
        let reason = params
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or(match kind {
                InteractionKind::CommandApproval => "Codex requests approval to run a command",
                InteractionKind::FileApproval => "Codex requests approval to change files",
                InteractionKind::UserInput => unreachable!(),
            });
        let command = params
            .get("command")
            .and_then(Value::as_str)
            .map(sanitized_interaction_text);
        let path = params
            .get("cwd")
            .or_else(|| params.get("grantRoot"))
            .and_then(Value::as_str)
            .map(sanitized_interaction_text);
        let event = ProviderEvent::ApprovalRequested {
            turn_id: local_turn_id,
            provider_event_id: self.event_id(local_turn_id, "approval"),
            interaction_id: interaction_id.clone(),
            prompt: sanitized_interaction_text(reason),
            operation: Some(match kind {
                InteractionKind::CommandApproval => "command_execution".into(),
                InteractionKind::FileApproval => "file_change".into(),
                InteractionKind::UserInput => unreachable!(),
            }),
            path,
            command,
            consequence: params
                .get("additionalPermissions")
                .filter(|value| !value.is_null())
                .map(|_| "grants additional permissions for this operation".into()),
        };
        if let Err(error) = event.validate() {
            return self.fail_closed(format!(
                "Codex approval request exceeded the durable interaction contract: {error}"
            ));
        }
        self.interactions.insert(
            interaction_id.clone(),
            PendingInteraction {
                rpc_id,
                rpc_key,
                fingerprint,
                kind,
                turn_id: local_turn_id,
                question_ids: Vec::new(),
            },
        );
        let mut effects = Vec::new();
        if let Some(event) = self.flush_output() {
            effects.push(Effect::Emit(event));
        }
        effects.push(Effect::Emit(event));
        effects
    }

    fn user_input_request(
        &mut self,
        value: &Value,
        rpc_id: Value,
        rpc_key: String,
        fingerprint: u64,
    ) -> Vec<Effect> {
        let method = "item/tool/requestUserInput";
        let params = &value["params"];
        let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
            return self.schema_failure(method, "params.threadId");
        };
        let Some(turn_id) = params.get("turnId").and_then(Value::as_str) else {
            return self.schema_failure(method, "params.turnId");
        };
        let Some(item_id) = params.get("itemId").and_then(Value::as_str) else {
            return self.schema_failure(method, "params.itemId");
        };
        if let Err(error) = self.correlate_provider_ids(thread_id, turn_id, Some(item_id)) {
            return self.fail_closed(error);
        }
        let Some(raw_questions) = params.get("questions").and_then(Value::as_array) else {
            return self.schema_failure(method, "params.questions");
        };
        if raw_questions.is_empty()
            || raw_questions.len() > crate::live_turn::MAX_USER_INPUT_QUESTIONS
        {
            return self.fail_closed(format!(
                "Codex user-input request has {} questions; expected 1..={} ({SUPPORTED_CODEX_VERSION})",
                raw_questions.len(),
                crate::live_turn::MAX_USER_INPUT_QUESTIONS
            ));
        }
        let mut questions = Vec::with_capacity(raw_questions.len());
        for question in raw_questions {
            if question
                .get("isSecret")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                return self.fail_closed(format!(
                    "Codex method {method:?} requested confidential credential input; Agent World refuses because the durable v2 contract cannot persist authentication material safely"
                ));
            }
            let Some(question_id) = question.get("id").and_then(Value::as_str) else {
                return self.schema_failure(method, "params.questions[].id");
            };
            let Some(prompt) = question.get("question").and_then(Value::as_str) else {
                return self.schema_failure(method, "params.questions[].question");
            };
            if let Err(error) = validate_identifier(
                "Codex user-input question id",
                question_id,
                MAX_INTERACTION_ID_BYTES,
            ) {
                return self.fail_closed(error);
            }
            if questions
                .iter()
                .any(|item: &UserInputQuestion| item.question_id == question_id)
            {
                return self.fail_closed(format!(
                    "Codex user-input request repeated question id {question_id:?}"
                ));
            }
            let header = question.get("header").and_then(Value::as_str).unwrap_or("");
            let combined = if header.is_empty() {
                prompt.to_owned()
            } else {
                format!("{header}: {prompt}")
            };
            questions.push(UserInputQuestion {
                question_id: question_id.to_owned(),
                prompt: sanitized_interaction_text(&combined),
            });
        }
        let Some(local_turn_id) = self.active.as_ref().map(|active| active.turn_id) else {
            return self.fail_closed("Codex requested user input without an active turn".into());
        };
        let interaction_id = self.interaction_id(local_turn_id);
        let event = ProviderEvent::UserInputRequested {
            turn_id: local_turn_id,
            provider_event_id: self.event_id(local_turn_id, "input"),
            interaction_id: interaction_id.clone(),
            prompt: "Codex needs user input before it can continue".into(),
            questions,
        };
        if let Err(error) = event.validate() {
            return self.fail_closed(format!(
                "Codex user-input request exceeded the durable interaction contract: {error}"
            ));
        }
        let question_ids = match &event {
            ProviderEvent::UserInputRequested { questions, .. } => questions
                .iter()
                .map(|question| question.question_id.clone())
                .collect(),
            _ => unreachable!(),
        };
        self.interactions.insert(
            interaction_id.clone(),
            PendingInteraction {
                rpc_id,
                rpc_key,
                fingerprint,
                kind: InteractionKind::UserInput,
                turn_id: local_turn_id,
                question_ids,
            },
        );
        let mut effects = Vec::new();
        if let Some(event) = self.flush_output() {
            effects.push(Effect::Emit(event));
        }
        effects.push(Effect::Emit(event));
        effects
    }

    fn approval_response(
        &mut self,
        turn_id: Uuid,
        interaction_id: &str,
        decision: ApprovalDecision,
    ) -> Vec<Effect> {
        let Some(interaction) = self.interactions.remove(interaction_id) else {
            return self.fail_closed(format!(
                "approval response did not match the exact outstanding interaction {interaction_id:?}"
            ));
        };
        if interaction.turn_id != turn_id
            || !matches!(
                interaction.kind,
                InteractionKind::CommandApproval | InteractionKind::FileApproval
            )
        {
            return self.fail_closed(format!(
                "approval response {interaction_id:?} did not match the active turn and interaction kind"
            ));
        }
        let decision = match decision {
            ApprovalDecision::Approve => "accept",
            ApprovalDecision::Deny => "decline",
        };
        vec![Effect::Write(json!({
            "id": interaction.rpc_id,
            "result": {"decision": decision}
        }))]
    }

    fn user_input_response(
        &mut self,
        turn_id: Uuid,
        interaction_id: &str,
        answers: Vec<UserInputAnswer>,
    ) -> Vec<Effect> {
        let Some(interaction) = self.interactions.remove(interaction_id) else {
            return self.fail_closed(format!(
                "user-input response did not match the exact outstanding interaction {interaction_id:?}"
            ));
        };
        if interaction.turn_id != turn_id || interaction.kind != InteractionKind::UserInput {
            return self.fail_closed(format!(
                "user-input response {interaction_id:?} did not match the active turn and interaction kind"
            ));
        }
        let mut answer_map = serde_json::Map::new();
        for answer in answers {
            if !interaction.question_ids.contains(&answer.question_id)
                || answer_map.contains_key(&answer.question_id)
            {
                return self.fail_closed(format!(
                    "user-input answer did not match exactly one outstanding question {:?}",
                    answer.question_id
                ));
            }
            answer_map.insert(answer.question_id, json!({"answers": [answer.answer]}));
        }
        if answer_map.len() != interaction.question_ids.len() {
            return self.fail_closed(
                "user-input response omitted one or more outstanding questions".into(),
            );
        }
        vec![Effect::Write(json!({
            "id": interaction.rpc_id,
            "result": {"answers": answer_map}
        }))]
    }

    fn interrupt(&mut self, turn_id: Uuid, now: Instant) -> Vec<Effect> {
        let Some(active) = self.active.as_ref() else {
            return vec![self.standalone_failure(
                turn_id,
                "interrupt-no-turn",
                "there is no active Codex turn to interrupt".into(),
            )];
        };
        if active.turn_id != turn_id {
            return self.fail_closed("interrupt did not target the exact active turn".into());
        }
        let (Some(thread_id), Some(provider_turn_id)) = (
            active.provider_thread_id.clone(),
            active.provider_turn_id.clone(),
        ) else {
            return self.fail_closed(
                "Codex cannot safely interrupt before provider thread and turn ids are correlated"
                    .into(),
            );
        };
        if active.interrupt_deadline.is_some() {
            return self.fail_closed("Codex interrupt is already outstanding".into());
        }
        if let Some(active) = self.active.as_mut() {
            active.interrupt_deadline = Some(now + self.interrupt_timeout);
        }
        self.request(
            RequestKind::Interrupt,
            "turn/interrupt",
            json!({"threadId": thread_id, "turnId": provider_turn_id}),
        )
    }

    fn interrupt_response(&mut self) -> Vec<Effect> {
        let Some(active) = self.active.as_mut() else {
            return Vec::new();
        };
        active.interrupt_acknowledged = true;
        Vec::new()
    }

    fn on_tick(&mut self, now: Instant) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self
            .coalesced
            .as_ref()
            .is_some_and(|chunk| now >= chunk.deadline)
            && let Some(event) = self.flush_output()
        {
            effects.push(Effect::Emit(event));
        }
        if let Some(active) = self.active.as_ref() {
            if active
                .startup_deadline
                .is_some_and(|deadline| now >= deadline)
            {
                effects.push(Effect::ForceStop(format!(
                    "Codex did not complete initialize/thread/turn correlation within {:?}",
                    self.startup_timeout
                )));
            } else if active
                .interrupt_deadline
                .is_some_and(|deadline| now >= deadline)
            {
                effects.push(Effect::ForceStop(format!(
                    "Codex did not reach a terminal interrupted state within {:?}; forced stop of the owned child",
                    self.interrupt_timeout
                )));
            }
        }
        effects
    }

    fn correlate_turn_started(&self, thread_id: &str, turn_id: &str) -> Result<(), String> {
        validate_provider_identifier("Codex event thread id", thread_id)?;
        validate_provider_identifier("Codex event turn id", turn_id)?;
        let active = self
            .active
            .as_ref()
            .ok_or_else(|| "Codex turn/started arrived without an active turn".to_owned())?;
        let expected_thread = active
            .provider_thread_id
            .as_deref()
            .ok_or_else(|| "Codex turn/started arrived before thread correlation".to_owned())?;
        if expected_thread != thread_id {
            return Err(format!(
                "Codex event thread id {thread_id:?} did not match the correlated thread id"
            ));
        }
        if active
            .provider_turn_id
            .as_deref()
            .is_some_and(|expected| expected != turn_id)
            || active
                .turn_start_response_id
                .as_deref()
                .is_some_and(|expected| expected != turn_id)
        {
            return Err(format!(
                "Codex turn/started id {turn_id:?} disagreed with the active turn correlation"
            ));
        }
        Ok(())
    }

    fn correlate_provider_ids(
        &self,
        thread_id: &str,
        turn_id: &str,
        item_id: Option<&str>,
    ) -> Result<(), String> {
        validate_provider_identifier("Codex event thread id", thread_id)?;
        validate_provider_identifier("Codex event turn id", turn_id)?;
        if let Some(item_id) = item_id {
            validate_provider_identifier("Codex event item id", item_id)?;
        }
        let active = self
            .active
            .as_ref()
            .ok_or_else(|| "Codex event arrived without an active turn".to_owned())?;
        let expected_thread = active.provider_thread_id.as_deref().ok_or_else(|| {
            "Codex turn-scoped event arrived before thread correlation".to_owned()
        })?;
        if expected_thread != thread_id {
            return Err(format!(
                "Codex event thread id {thread_id:?} did not match the correlated thread id"
            ));
        }
        let expected_turn = active.provider_turn_id.as_deref().ok_or_else(|| {
            "Codex turn-scoped event arrived before turn/started established its id".to_owned()
        })?;
        if expected_turn != turn_id {
            return Err(format!(
                "Codex event turn id {turn_id:?} did not match the correlated turn id"
            ));
        }
        Ok(())
    }

    fn flush_output(&mut self) -> Option<ProviderEvent> {
        let chunk = self.coalesced.take()?;
        if chunk.text.is_empty() {
            return None;
        }
        let active = self.active.as_ref()?;
        let turn_id = active.turn_id;
        let session_id = active.provider_thread_id.clone()?;
        Some(ProviderEvent::AssistantOutput {
            turn_id,
            provider_event_id: self.event_id(turn_id, "output"),
            delta: chunk.text,
            resume_cursor: Some(resume_cursor(&session_id)),
        })
    }

    fn process_lost(&mut self, reason: String, stderr: String) -> Vec<ProviderEvent> {
        let Some(active) = self.active.as_ref() else {
            self.reset_transport();
            return Vec::new();
        };
        let turn_id = active.turn_id;
        let side_effect_possible = active.provider_side_effect_possible;
        let mut events = Vec::new();
        if let Some(output) = self.flush_output() {
            events.push(output);
        }
        let diagnostic = if stderr.trim().is_empty() {
            reason
        } else {
            format!("{reason}\nCodex stderr tail:\n{stderr}")
        };
        events.push(ProviderEvent::ProcessLost {
            turn_id,
            provider_event_id: self.event_id(turn_id, "process-lost"),
            diagnostic: bounded_diagnostic(&sanitize_diagnostic(&diagnostic)),
            side_effect_possible,
        });
        self.clear_active();
        self.initialized = false;
        events
    }

    fn protocol_failure(&mut self, diagnostic: String) -> Vec<ProviderEvent> {
        let Some(active) = self.active.as_ref() else {
            return Vec::new();
        };
        let turn_id = active.turn_id;
        let mut events = Vec::new();
        if let Some(output) = self.flush_output() {
            events.push(output);
        }
        events.push(ProviderEvent::Failed {
            turn_id,
            provider_event_id: self.event_id(turn_id, "protocol-failed"),
            diagnostic: bounded_diagnostic(&sanitize_diagnostic(&diagnostic)),
        });
        self.clear_active();
        events
    }

    fn fail_closed(&mut self, diagnostic: String) -> Vec<Effect> {
        let mut effects = self
            .protocol_failure(diagnostic.clone())
            .into_iter()
            .map(Effect::Emit)
            .collect::<Vec<_>>();
        effects.push(Effect::ForceStop(diagnostic));
        effects
    }

    fn schema_failure(&mut self, method: &str, field: &str) -> Vec<Effect> {
        self.fail_closed(format!(
            "Codex method {method:?} omitted or changed {field:?} in installed {SUPPORTED_CODEX_VERSION}; schema drift fails closed"
        ))
    }

    fn clear_active(&mut self) {
        for (id, _) in std::mem::take(&mut self.pending_requests) {
            if self.stale_response_ids.len() == MAX_PENDING_REQUESTS * 2 {
                self.stale_response_ids.pop_front();
            }
            self.stale_response_ids.push_back(id);
        }
        self.interactions.clear();
        self.coalesced = None;
        self.active = None;
    }

    fn remember_terminal(&mut self, fingerprint: u64) {
        if self.terminal_fingerprints.len() == 8 {
            self.terminal_fingerprints.pop_front();
        }
        self.terminal_fingerprints.push_back(fingerprint);
    }

    fn event_id(&mut self, turn_id: Uuid, kind: &str) -> String {
        let id = format!("codex:{turn_id}:{}:{kind}", self.next_event_id);
        self.next_event_id = self.next_event_id.saturating_add(1);
        id
    }

    fn interaction_id(&mut self, turn_id: Uuid) -> String {
        let id = format!("codex-interaction:{turn_id}:{}", self.next_interaction_id);
        self.next_interaction_id = self.next_interaction_id.saturating_add(1);
        id
    }

    fn standalone_failure(&mut self, turn_id: Uuid, kind: &str, diagnostic: String) -> Effect {
        Effect::Emit(ProviderEvent::Failed {
            turn_id,
            provider_event_id: self.event_id(turn_id, kind),
            diagnostic: bounded_diagnostic(&diagnostic),
        })
    }
}

const IGNORED_NOTIFICATIONS: &[&str] = &[
    "thread/started",
    "thread/status/changed",
    "thread/tokenUsage/updated",
    "item/started",
    "item/completed",
    "item/commandExecution/outputDelta",
    "item/fileChange/outputDelta",
    "item/reasoning/summaryTextDelta",
    "item/reasoning/textDelta",
    "item/reasoning/summaryPartAdded",
    "turn/diff/updated",
    "turn/plan/updated",
    "serverRequest/resolved",
    "config/warning",
    "deprecationNotice",
];

fn apply_effects(
    process: &mut Option<OwnedProcess>,
    events: &SyncSender<ProviderEvent>,
    effects: Vec<Effect>,
) -> Result<Option<String>, String> {
    for effect in effects {
        match effect {
            Effect::Write(value) => {
                let Some(child) = process.as_mut() else {
                    return Ok(Some(
                        "Codex process disappeared before protocol write".into(),
                    ));
                };
                if let Err(error) = child.write_json(&value) {
                    return Ok(Some(format!("Codex protocol write failed: {error}")));
                }
            }
            Effect::Emit(event) => {
                event.validate()?;
                events.send(event).map_err(|error| error.to_string())?;
            }
            Effect::ForceStop(reason) => return Ok(Some(reason)),
        }
    }
    Ok(None)
}

fn emit_all(events: &SyncSender<ProviderEvent>, values: Vec<ProviderEvent>) -> Result<(), String> {
    for event in values {
        event.validate()?;
        events.send(event).map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn handle_forced_stop(
    process: &mut Option<OwnedProcess>,
    protocol: &mut ProtocolMachine,
    events: &SyncSender<ProviderEvent>,
    reason: &str,
) -> Result<(), String> {
    let mut diagnostics = String::new();
    let mut forced = false;
    if let Some(child) = process.as_mut() {
        diagnostics = child.diagnostics();
        forced = child.stop(true, Duration::ZERO).unwrap_or(true);
    }
    let reason = if forced {
        format!("{reason}; Agent World forcibly stopped only its owned Codex child")
    } else {
        reason.to_owned()
    };
    emit_all(events, protocol.process_lost(reason, diagnostics))?;
    process.take();
    protocol.reset_transport();
    Ok(())
}

fn request_id_key(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) if value.is_i64() || value.is_u64() => Some(value.to_string()),
        _ => None,
    }
}

fn session_cursor(session_id: &str) -> ProviderSessionCursor {
    ProviderSessionCursor {
        session_id: session_id.to_owned(),
        resume_cursor: resume_cursor(session_id),
    }
}

fn resume_cursor(session_id: &str) -> String {
    format!("{CURSOR_PREFIX}{session_id}")
}

fn bounded_diagnostic(value: &str) -> String {
    truncate_utf8(value, MAX_DIAGNOSTIC_BYTES, "\n[diagnostic truncated]")
}

fn bounded_interaction_text(value: &str) -> String {
    truncate_utf8(value, MAX_INTERACTION_TEXT_BYTES, " [truncated]")
}

fn sanitized_interaction_text(value: &str) -> String {
    bounded_interaction_text(&sanitize_diagnostic(value))
}

fn validate_provider_identifier(label: &str, value: &str) -> Result<(), String> {
    validate_identifier(label, value, MAX_PROVIDER_ID_BYTES)
}

fn validate_identifier(label: &str, value: &str, max_bytes: usize) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{label} must not be empty"));
    }
    if value.len() > max_bytes {
        return Err(format!(
            "{label} is {} bytes; maximum is {max_bytes}",
            value.len()
        ));
    }
    Ok(())
}

fn truncate_utf8(value: &str, max: usize, marker: &str) -> String {
    if value.len() <= max {
        return value.to_owned();
    }
    let target = max.saturating_sub(marker.len());
    let mut boundary = target;
    while !value.is_char_boundary(boundary) {
        boundary = boundary.saturating_sub(1);
    }
    format!("{}{marker}", &value[..boundary])
}

fn utf8_chunks(mut value: &str, max: usize) -> Vec<&str> {
    let mut chunks = Vec::new();
    while !value.is_empty() {
        let mut boundary = value.len().min(max);
        while !value.is_char_boundary(boundary) {
            boundary -= 1;
        }
        chunks.push(&value[..boundary]);
        value = &value[boundary..];
    }
    chunks
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestWorktree(PathBuf);

    impl TestWorktree {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("agent-world-codex-fixture-{}", Uuid::new_v4()));
            std::fs::create_dir_all(&path).expect("create fixture worktree");
            Self(path)
        }
    }

    impl Drop for TestWorktree {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    struct PipeHarness {
        machine: ProtocolMachine,
        now: Instant,
        writes: Vec<Value>,
        events: Vec<ProviderEvent>,
        forced: Vec<String>,
        worktree: TestWorktree,
        local_turn_id: Uuid,
    }

    impl PipeHarness {
        fn start(session: Option<ProviderSessionCursor>) -> Self {
            let worktree = TestWorktree::new();
            let local_turn_id = Uuid::new_v4();
            let mut harness = Self {
                machine: ProtocolMachine::new(Duration::from_secs(2), Duration::from_millis(100)),
                now: Instant::now(),
                writes: Vec::new(),
                events: Vec::new(),
                forced: Vec::new(),
                worktree,
                local_turn_id,
            };
            let command = ProviderCommand::Start {
                turn_id: local_turn_id,
                thread_id: Uuid::new_v4(),
                worktree_path: harness.worktree.0.clone(),
                prompt: "deterministic fixture prompt".into(),
                session,
            };
            command.validate().expect("valid fixture command");
            let effects = harness.machine.handle_command(
                command,
                harness.now,
                Some(&std::fs::canonicalize(&harness.worktree.0).expect("canonical fixture")),
            );
            harness.apply(effects);
            harness
        }

        fn apply(&mut self, effects: Vec<Effect>) {
            for effect in effects {
                match effect {
                    Effect::Write(value) => self.writes.push(value),
                    Effect::Emit(event) => {
                        event.validate().expect("valid normalized fixture event");
                        self.events.push(event);
                    }
                    Effect::ForceStop(reason) => self.forced.push(reason),
                }
            }
        }

        fn line(&mut self, value: Value) {
            let line = serde_json::to_string(&value).expect("encode fixture line");
            let effects = self.machine.handle_line(&line, self.now);
            self.apply(effects);
        }

        fn tick(&mut self, duration: Duration) {
            self.now += duration;
            let effects = self.machine.on_tick(self.now);
            self.apply(effects);
        }

        fn take_request(&mut self, method: &str) -> Value {
            let index = self
                .writes
                .iter()
                .position(|value| value.get("method") == Some(&json!(method)))
                .unwrap_or_else(|| panic!("missing fixture request {method}"));
            self.writes.remove(index)
        }

        fn initialize(&mut self) {
            let request = self.take_request("initialize");
            self.line(json!({
                "id": request["id"],
                "result": {
                    "userAgent": SUPPORTED_CODEX_VERSION,
                    "codexHome": "C:\\Users\\fixture\\.codex",
                    "platformFamily": "windows",
                    "platformOs": "windows"
                }
            }));
            assert!(
                self.writes
                    .iter()
                    .any(|value| value.get("method") == Some(&json!("initialized")))
            );
        }

        fn establish_new_thread(&mut self, provider_thread_id: &str) {
            self.initialize();
            let request = self.take_request("thread/start");
            self.line(json!({
                "id": request["id"],
                "result": {"thread": {"id": provider_thread_id}}
            }));
        }

        fn resume_thread(&mut self, provider_thread_id: &str) {
            self.initialize();
            let request = self.take_request("thread/resume");
            assert_eq!(request["params"]["threadId"], provider_thread_id);
            self.line(json!({
                "id": request["id"],
                "result": {"thread": {"id": provider_thread_id}}
            }));
        }

        fn start_provider_turn(&mut self, provider_turn_id: &str) {
            let request = self.take_request("turn/start");
            let provider_thread_id = request["params"]["threadId"]
                .as_str()
                .expect("turn/start thread id")
                .to_owned();
            self.line(json!({
                "id": request["id"],
                "result": {"turn": {"id": provider_turn_id}}
            }));
            self.line(json!({
                "method": "turn/started",
                "params": {
                    "threadId": provider_thread_id,
                    "turn": {"id": provider_turn_id}
                }
            }));
        }
    }

    fn event_names(events: &[ProviderEvent]) -> Vec<&'static str> {
        events
            .iter()
            .map(|event| match event {
                ProviderEvent::Starting { .. } => "starting",
                ProviderEvent::SessionEstablished { .. } => "session",
                ProviderEvent::Resumed { .. } => "resumed",
                ProviderEvent::AssistantOutput { .. } => "output",
                ProviderEvent::ApprovalRequested { .. } => "approval",
                ProviderEvent::UserInputRequested { .. } => "input",
                ProviderEvent::InterruptAcknowledged { .. } => "interrupt",
                ProviderEvent::Completed { .. } => "completed",
                ProviderEvent::Failed { .. } => "failed",
                ProviderEvent::ProcessLost { .. } => "lost",
            })
            .collect()
    }

    #[test]
    fn no_provider_request_exists_before_a_core_start_command() {
        let machine = ProtocolMachine::new(Duration::from_secs(2), Duration::from_secs(1));
        assert!(!machine.initialized);
        assert!(machine.pending_requests.is_empty());
        assert!(machine.active.is_none());
    }

    #[test]
    fn zero_turn_readiness_classifies_exact_version_unavailable_and_unsupported() {
        assert_eq!(
            readiness_from_version(Ok(SUPPORTED_CODEX_VERSION.into())),
            ProviderReadiness::Available
        );
        assert_eq!(
            readiness_from_version(Ok("codex-cli 0.147.0".into())),
            ProviderReadiness::UnsupportedVersion {
                installed: "codex-cli 0.147.0".into(),
                supported: SUPPORTED_CODEX_VERSION.into(),
            }
        );
        let unavailable = readiness_from_version(Err("native executable missing".into()));
        unavailable.validate().expect("bounded readiness result");
        assert!(matches!(
            unavailable,
            ProviderReadiness::Unavailable { diagnostic }
                if diagnostic == "native executable missing"
        ));
    }

    #[test]
    fn deterministic_pipe_normal_stream_is_ordered_coalesced_and_terminal_once() {
        let mut harness = PipeHarness::start(None);
        harness.establish_new_thread("thread-1");
        harness.start_provider_turn("turn-1");

        harness.line(json!({
            "method": "item/agentMessage/delta",
            "params": {"threadId":"thread-1","turnId":"turn-1","itemId":"item-1","delta":"Hello"}
        }));
        let repeated = json!({
            "method": "item/agentMessage/delta",
            "params": {"threadId":"thread-1","turnId":"turn-1","itemId":"item-1","delta":" world"}
        });
        harness.line(repeated.clone());
        harness.line(repeated);
        assert!(!event_names(&harness.events).contains(&"output"));
        harness.tick(STREAM_FLUSH_INTERVAL);

        let terminal = json!({
            "method": "turn/completed",
            "params": {"threadId":"thread-1","turn":{"id":"turn-1","status":"completed","items":[]}}
        });
        harness.line(terminal.clone());
        harness.line(terminal);

        assert_eq!(
            event_names(&harness.events),
            vec!["starting", "session", "output", "completed"]
        );
        let output = harness.events.iter().find_map(|event| match event {
            ProviderEvent::AssistantOutput { delta, .. } => Some(delta),
            _ => None,
        });
        assert_eq!(output.map(String::as_str), Some("Hello world world"));
        assert_eq!(
            harness
                .events
                .iter()
                .filter(|event| matches!(event, ProviderEvent::Completed { .. }))
                .count(),
            1
        );
        assert!(harness.forced.is_empty());
    }

    #[test]
    fn review_regression_turn_scoped_terminal_requires_turn_started_correlation() {
        let mut harness = PipeHarness::start(None);
        harness.establish_new_thread("thread-correlation");
        let request = harness.take_request("turn/start");
        harness.line(json!({
            "id": request["id"],
            "result": {"turn": {"id": "turn-correlation"}}
        }));
        harness.line(json!({
            "method": "turn/completed",
            "params": {
                "threadId": "thread-correlation",
                "turn": {"id": "turn-correlation", "status": "completed", "items": []}
            }
        }));

        assert_eq!(
            event_names(&harness.events),
            vec!["starting", "session", "failed"]
        );
        assert_eq!(harness.forced.len(), 1);
        let diagnostic = harness
            .events
            .iter()
            .find_map(|event| match event {
                ProviderEvent::Failed { diagnostic, .. } => Some(diagnostic.as_str()),
                _ => None,
            })
            .expect("correlation failure");
        assert!(diagnostic.contains("before turn/started"), "{diagnostic}");
    }

    #[test]
    fn review_regression_user_input_rejects_oversized_question_ids_without_truncation() {
        let mut harness = PipeHarness::start(None);
        harness.establish_new_thread("thread-question-id");
        harness.start_provider_turn("turn-question-id");
        harness.line(json!({
            "id": "oversized-question-rpc",
            "method": "item/tool/requestUserInput",
            "params": {
                "threadId": "thread-question-id",
                "turnId": "turn-question-id",
                "itemId": "item-question-id",
                "questions": [{
                    "id": "q".repeat(MAX_INTERACTION_ID_BYTES + 1),
                    "header": "Scope",
                    "question": "Choose a scope"
                }]
            }
        }));

        assert!(
            !harness
                .events
                .iter()
                .any(|event| { matches!(event, ProviderEvent::UserInputRequested { .. }) })
        );
        let diagnostic = harness
            .events
            .iter()
            .find_map(|event| match event {
                ProviderEvent::Failed { diagnostic, .. } => Some(diagnostic.as_str()),
                _ => None,
            })
            .expect("oversized question id failure");
        assert!(diagnostic.contains("maximum is 256"), "{diagnostic}");
        assert_eq!(harness.forced.len(), 1);
    }

    #[test]
    fn approval_round_trip_uses_only_the_exact_pending_rpc_id() {
        let mut harness = PipeHarness::start(None);
        harness.establish_new_thread("thread-approval");
        harness.start_provider_turn("turn-approval");
        harness.line(json!({
            "id": 41,
            "method": "item/commandExecution/requestApproval",
            "params": {
                "threadId":"thread-approval",
                "turnId":"turn-approval",
                "itemId":"item-command",
                "startedAtMs":1,
                "command":"cargo test",
                "cwd":"C:\\repo",
                "reason":"run the verified test suite"
            }
        }));
        let interaction_id = harness
            .events
            .iter()
            .find_map(|event| match event {
                ProviderEvent::ApprovalRequested { interaction_id, .. } => {
                    Some(interaction_id.clone())
                }
                _ => None,
            })
            .expect("approval event");
        let effects = harness.machine.handle_command(
            ProviderCommand::ApprovalResponse {
                turn_id: harness.local_turn_id,
                interaction_id,
                decision: ApprovalDecision::Approve,
            },
            harness.now,
            Some(&harness.worktree.0),
        );
        harness.apply(effects);
        let response = harness
            .writes
            .iter()
            .find(|value| value.get("id") == Some(&json!(41)))
            .expect("correlated approval response");
        assert_eq!(response["result"]["decision"], "accept");
    }

    #[test]
    fn interaction_ids_stay_unique_across_independent_protocol_machines() {
        fn first_interaction_id(harness: &mut PipeHarness, rpc_id: i64) -> String {
            harness.establish_new_thread("thread-unique");
            harness.start_provider_turn("turn-unique");
            harness.line(json!({
                "id": rpc_id,
                "method": "item/commandExecution/requestApproval",
                "params": {
                    "threadId":"thread-unique",
                    "turnId":"turn-unique",
                    "itemId":"item-command",
                    "command":"cargo test",
                    "cwd":"C:\\repo",
                    "reason":"run the verified test suite"
                }
            }));
            harness
                .events
                .iter()
                .find_map(|event| match event {
                    ProviderEvent::ApprovalRequested { interaction_id, .. } => {
                        Some(interaction_id.clone())
                    }
                    _ => None,
                })
                .expect("approval event")
        }

        let mut first = PipeHarness::start(None);
        let first_id = first_interaction_id(&mut first, 51);
        let mut second = PipeHarness::start(None);
        let second_id = first_interaction_id(&mut second, 51);

        assert_ne!(first_id, second_id);
        assert!(first_id.contains(&first.local_turn_id.to_string()));
        assert!(second_id.contains(&second.local_turn_id.to_string()));
        assert!(first_id.len() <= crate::live_turn::MAX_INTERACTION_ID_BYTES);
    }

    #[test]
    fn approval_deny_maps_to_the_exact_pending_rpc_and_decline_only() {
        let mut harness = PipeHarness::start(None);
        harness.establish_new_thread("thread-deny");
        harness.start_provider_turn("turn-deny");
        harness.line(json!({
            "id": 42,
            "method": "item/fileChange/requestApproval",
            "params": {
                "threadId":"thread-deny",
                "turnId":"turn-deny",
                "itemId":"item-file",
                "reason":"write the requested fixture file",
                "grantRoot":"C:\\repo"
            }
        }));
        let interaction_id = harness
            .events
            .iter()
            .find_map(|event| match event {
                ProviderEvent::ApprovalRequested { interaction_id, .. } => {
                    Some(interaction_id.clone())
                }
                _ => None,
            })
            .expect("file approval event");
        let effects = harness.machine.handle_command(
            ProviderCommand::ApprovalResponse {
                turn_id: harness.local_turn_id,
                interaction_id,
                decision: ApprovalDecision::Deny,
            },
            harness.now,
            Some(&harness.worktree.0),
        );
        harness.apply(effects);
        let response = harness
            .writes
            .iter()
            .find(|value| value.get("id") == Some(&json!(42)))
            .expect("correlated denial response");
        assert_eq!(response, &json!({"id":42,"result":{"decision":"decline"}}));
        assert_eq!(harness.machine.interactions.len(), 0);
    }

    #[test]
    fn user_input_round_trip_requires_every_exact_question_once() {
        let mut harness = PipeHarness::start(None);
        harness.establish_new_thread("thread-input");
        harness.start_provider_turn("turn-input");
        harness.line(json!({
            "id":"input-rpc",
            "method":"item/tool/requestUserInput",
            "params": {
                "threadId":"thread-input",
                "turnId":"turn-input",
                "itemId":"item-input",
                "questions":[
                    {"id":"branch","header":"Branch","question":"Which branch?"},
                    {"id":"scope","header":"Scope","question":"Which scope?"}
                ]
            }
        }));
        let interaction_id = harness
            .events
            .iter()
            .find_map(|event| match event {
                ProviderEvent::UserInputRequested { interaction_id, .. } => {
                    Some(interaction_id.clone())
                }
                _ => None,
            })
            .expect("input event");
        let effects = harness.machine.handle_command(
            ProviderCommand::UserInputResponse {
                turn_id: harness.local_turn_id,
                interaction_id,
                answers: vec![
                    UserInputAnswer {
                        question_id: "branch".into(),
                        answer: "main".into(),
                    },
                    UserInputAnswer {
                        question_id: "scope".into(),
                        answer: "tests".into(),
                    },
                ],
            },
            harness.now,
            Some(&harness.worktree.0),
        );
        harness.apply(effects);
        let response = harness
            .writes
            .iter()
            .find(|value| value.get("id") == Some(&json!("input-rpc")))
            .expect("correlated user-input response");
        assert_eq!(
            response["result"]["answers"]["branch"]["answers"][0],
            "main"
        );
        assert_eq!(
            response["result"]["answers"]["scope"]["answers"][0],
            "tests"
        );
    }

    #[test]
    fn user_input_round_trip_preserves_multiline_answers_exactly() {
        let mut harness = PipeHarness::start(None);
        harness.establish_new_thread("thread-multiline");
        harness.start_provider_turn("turn-multiline");
        harness.line(json!({
            "id":"multiline-rpc",
            "method":"item/tool/requestUserInput",
            "params": {
                "threadId":"thread-multiline",
                "turnId":"turn-multiline",
                "itemId":"item-multiline",
                "questions":[{"id":"notes","header":"Notes","question":"Add notes"}]
            }
        }));
        let interaction_id = harness
            .events
            .iter()
            .find_map(|event| match event {
                ProviderEvent::UserInputRequested { interaction_id, .. } => {
                    Some(interaction_id.clone())
                }
                _ => None,
            })
            .expect("multiline input event");
        let answer = "first line\nsecond line\nthird line";
        let effects = harness.machine.handle_command(
            ProviderCommand::UserInputResponse {
                turn_id: harness.local_turn_id,
                interaction_id,
                answers: vec![UserInputAnswer {
                    question_id: "notes".into(),
                    answer: answer.into(),
                }],
            },
            harness.now,
            Some(&harness.worktree.0),
        );
        harness.apply(effects);
        let response = harness
            .writes
            .iter()
            .find(|value| value.get("id") == Some(&json!("multiline-rpc")))
            .expect("correlated multiline response");
        assert_eq!(response["result"]["answers"]["notes"]["answers"][0], answer);
    }

    #[test]
    fn secret_user_input_and_sensitive_approval_text_are_never_normalized_for_persistence() {
        let mut harness = PipeHarness::start(None);
        harness.establish_new_thread("thread-secret");
        harness.start_provider_turn("turn-secret");
        harness.line(json!({
            "id":91,
            "method":"item/commandExecution/requestApproval",
            "params": {
                "threadId":"thread-secret",
                "turnId":"turn-secret",
                "itemId":"item-secret-command",
                "startedAtMs":1,
                "command":"curl -H 'Authorization: Bearer should-not-persist'",
                "reason":"send request"
            }
        }));
        let command = harness
            .events
            .iter()
            .find_map(|event| match event {
                ProviderEvent::ApprovalRequested { command, .. } => command.as_deref(),
                _ => None,
            })
            .expect("sanitized command");
        assert_eq!(command, "[redacted sensitive Codex diagnostic]");

        harness.line(json!({
            "id":92,
            "method":"item/tool/requestUserInput",
            "params": {
                "threadId":"thread-secret",
                "turnId":"turn-secret",
                "itemId":"item-secret-input",
                "questions":[{"id":"token","header":"Token","question":"Paste token","isSecret":true}]
            }
        }));
        let diagnostic = harness
            .events
            .iter()
            .find_map(|event| match event {
                ProviderEvent::Failed { diagnostic, .. } => Some(diagnostic),
                _ => None,
            })
            .expect("secret-input failure");
        assert!(diagnostic.contains("refuses"));
        assert!(!diagnostic.contains("should-not-persist"));
        assert_eq!(harness.forced.len(), 1);
    }

    #[test]
    fn interrupt_acknowledges_then_reaches_one_terminal_event() {
        let mut harness = PipeHarness::start(None);
        harness.establish_new_thread("thread-interrupt");
        harness.start_provider_turn("turn-interrupt");
        let effects = harness.machine.handle_command(
            ProviderCommand::Interrupt {
                turn_id: harness.local_turn_id,
            },
            harness.now,
            Some(&harness.worktree.0),
        );
        harness.apply(effects);
        let request = harness.take_request("turn/interrupt");
        assert_eq!(request["params"]["threadId"], "thread-interrupt");
        assert_eq!(request["params"]["turnId"], "turn-interrupt");
        harness.line(json!({"id":request["id"],"result":{}}));
        harness.line(json!({
            "method":"turn/completed",
            "params":{"threadId":"thread-interrupt","turn":{"id":"turn-interrupt","status":"interrupted","items":[]}}
        }));
        assert_eq!(
            event_names(&harness.events),
            vec!["starting", "session", "interrupt"]
        );
        assert_eq!(
            harness
                .events
                .iter()
                .filter(|event| matches!(event, ProviderEvent::InterruptAcknowledged { .. }))
                .count(),
            1,
            "the core contract has one terminal interrupt acknowledgement"
        );
        assert!(
            !harness
                .events
                .iter()
                .any(|event| matches!(event, ProviderEvent::Completed { .. }))
        );
        assert!(harness.forced.is_empty());
    }

    #[test]
    fn interrupt_before_provider_id_correlation_fails_closed_and_forces_stop() {
        let mut harness = PipeHarness::start(None);
        let effects = harness.machine.handle_command(
            ProviderCommand::Interrupt {
                turn_id: harness.local_turn_id,
            },
            harness.now,
            Some(&harness.worktree.0),
        );
        harness.apply(effects);
        assert_eq!(event_names(&harness.events), vec!["starting", "failed"]);
        assert_eq!(harness.forced.len(), 1);
        let diagnostic = harness
            .events
            .iter()
            .find_map(|event| match event {
                ProviderEvent::Failed { diagnostic, .. } => Some(diagnostic),
                _ => None,
            })
            .expect("pre-correlation interrupt failure");
        assert!(diagnostic.contains("cannot safely interrupt"));
        assert!(
            harness
                .writes
                .iter()
                .all(|value| value.get("method") != Some(&json!("turn/interrupt")))
        );
    }

    #[test]
    fn interrupt_timeout_requires_visible_owned_child_forced_stop() {
        let mut harness = PipeHarness::start(None);
        harness.establish_new_thread("thread-timeout");
        harness.start_provider_turn("turn-timeout");
        let effects = harness.machine.handle_command(
            ProviderCommand::Interrupt {
                turn_id: harness.local_turn_id,
            },
            harness.now,
            Some(&harness.worktree.0),
        );
        harness.apply(effects);
        harness.tick(Duration::from_millis(101));
        assert_eq!(harness.forced.len(), 1);
        assert!(harness.forced[0].contains("forced stop of the owned child"));
    }

    #[test]
    fn unexpected_exit_keeps_bounded_sanitized_stderr_and_never_completes() {
        let mut harness = PipeHarness::start(None);
        harness.establish_new_thread("thread-exit");
        harness.start_provider_turn("turn-exit");
        let events = harness.machine.process_lost(
            "Codex app-server exited with status 7".into(),
            "last safe diagnostic".into(),
        );
        assert_eq!(events.len(), 1);
        let ProviderEvent::ProcessLost {
            diagnostic,
            side_effect_possible,
            ..
        } = &events[0]
        else {
            panic!("expected process loss")
        };
        assert!(*side_effect_possible);
        assert!(diagnostic.contains("status 7"));
        assert!(diagnostic.contains("last safe diagnostic"));
        assert!(diagnostic.len() <= MAX_DIAGNOSTIC_BYTES);
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, ProviderEvent::Completed { .. }))
        );

        assert_eq!(
            sanitize_diagnostic("Authorization: Bearer top-secret"),
            "[redacted sensitive Codex diagnostic]"
        );
    }

    #[test]
    fn zero_exit_before_terminal_is_process_loss_not_completion() {
        let mut harness = PipeHarness::start(None);
        harness.establish_new_thread("thread-zero-exit");
        harness.start_provider_turn("turn-zero-exit");
        let events = harness.machine.process_lost(
            "Codex app-server exited unexpectedly with exit code 0".into(),
            "bounded stderr tail".into(),
        );
        assert_eq!(event_names(&events), vec!["lost"]);
        assert!(matches!(
            &events[0],
            ProviderEvent::ProcessLost {
                side_effect_possible: true,
                diagnostic,
                ..
            } if diagnostic.contains("exit code 0")
                && diagnostic.contains("bounded stderr tail")
                && diagnostic.len() <= MAX_DIAGNOSTIC_BYTES
        ));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, ProviderEvent::Completed { .. }))
        );
    }

    #[test]
    fn malformed_json_and_stderr_flood_are_bounded_terminal_failures() {
        let mut harness = PipeHarness::start(None);
        harness.establish_new_thread("thread-malformed");
        harness.start_provider_turn("turn-malformed");
        let effects = harness
            .machine
            .handle_line("{ definitely-not-json", harness.now);
        harness.apply(effects);
        assert_eq!(harness.forced.len(), 1);
        assert!(harness.events.iter().any(|event| matches!(
            event,
            ProviderEvent::Failed { diagnostic, .. }
                if diagnostic.contains("invalid newline-delimited JSON")
                    && diagnostic.len() <= MAX_DIAGNOSTIC_BYTES
        )));

        let stderr = (0..2_000)
            .map(|index| {
                format!(
                    "stderr-{index:04}-{}\n",
                    "x".repeat(MAX_DIAGNOSTIC_LINE_BYTES)
                )
            })
            .collect::<String>();
        let ring = Arc::new(Mutex::new(DiagnosticRing::new(STDERR_RING_BYTES)));
        stderr_reader_loop(stderr.as_bytes(), Arc::clone(&ring));
        let rendered = ring.lock().expect("diagnostic ring").render();
        assert!(rendered.len() <= STDERR_RING_BYTES);
        assert!(rendered.contains("diagnostic line rejected"));
    }

    #[test]
    fn restart_resume_requires_the_durably_recorded_session_cursor_pair() {
        let session = session_cursor("durable-thread");
        let mut harness = PipeHarness::start(Some(session.clone()));
        harness.resume_thread("durable-thread");
        harness.start_provider_turn("resumed-turn");
        assert!(event_names(&harness.events).contains(&"resumed"));

        let mut bad = session;
        bad.resume_cursor = "unrecorded-cursor".into();
        let rejected = PipeHarness::start(Some(bad));
        assert_eq!(event_names(&rejected.events), vec!["failed"]);
        assert!(
            rejected
                .writes
                .iter()
                .all(|value| value.get("method") != Some(&json!("thread/resume")))
        );
    }

    #[test]
    fn unknown_method_and_schema_drift_fail_closed_with_method_and_version() {
        let mut harness = PipeHarness::start(None);
        harness.establish_new_thread("thread-drift");
        harness.start_provider_turn("turn-drift");
        harness.line(json!({"method":"turn/futureShape","params":{}}));
        assert_eq!(harness.forced.len(), 1);
        let diagnostic = harness
            .events
            .iter()
            .find_map(|event| match event {
                ProviderEvent::Failed { diagnostic, .. } => Some(diagnostic),
                _ => None,
            })
            .expect("schema drift failure");
        assert!(diagnostic.contains("turn/futureShape"));
        assert!(diagnostic.contains(SUPPORTED_CODEX_VERSION));
    }

    #[test]
    fn bounded_line_reader_drains_oversized_input_without_unbounded_allocation() {
        let input = format!("{}\nnext\n", "x".repeat(17));
        let mut reader = BufReader::new(input.as_bytes());
        assert!(matches!(
            read_bounded_line(&mut reader, 16).expect("read oversized line"),
            BoundedLine::TooLong
        ));
        match read_bounded_line(&mut reader, 16).expect("read following line") {
            BoundedLine::Line(line) => assert_eq!(line, b"next"),
            _ => panic!("following line was not retained"),
        }
    }

    #[test]
    fn outgoing_requests_match_the_pinned_workspace_write_on_request_schema() {
        assert_eq!(
            crate::live_turn::ISOLATED_WORKSPACE_WRITE_POLICY,
            "isolated_workspace_write_on_request_v1"
        );

        let mut fresh = PipeHarness::start(None);
        fresh.initialize();
        let canonical_worktree = std::fs::canonicalize(&fresh.worktree.0)
            .expect("canonical fixture worktree")
            .to_string_lossy()
            .into_owned();
        let thread_start = fresh.take_request("thread/start");
        assert_eq!(
            thread_start["params"],
            json!({
                "cwd": canonical_worktree,
                "approvalPolicy": "on-request",
                "sandbox": "workspace-write",
                "ephemeral": false,
                "experimentalRawEvents": false,
                "runtimeWorkspaceRoots": [canonical_worktree],
                "environments": []
            })
        );
        fresh.line(json!({
            "id": thread_start["id"],
            "result": {"thread": {"id": "thread-policy"}}
        }));
        let turn_start = fresh.take_request("turn/start");
        assert_eq!(turn_start["params"]["approvalPolicy"], "on-request");
        assert_eq!(turn_start["params"]["cwd"], canonical_worktree);
        assert_eq!(turn_start["params"]["environments"], json!([]));
        assert_eq!(
            turn_start["params"]["runtimeWorkspaceRoots"],
            json!([canonical_worktree])
        );
        assert_eq!(
            turn_start["params"]["sandboxPolicy"],
            workspace_write_sandbox_policy(&canonical_worktree)
        );
        assert_eq!(
            turn_start["params"]["sandboxPolicy"]["writableRoots"],
            json!([canonical_worktree])
        );
        assert_eq!(
            turn_start["params"]["sandboxPolicy"]["networkAccess"],
            false
        );

        let mut resumed = PipeHarness::start(Some(session_cursor("thread-resume-policy")));
        resumed.initialize();
        let resumed_worktree = std::fs::canonicalize(&resumed.worktree.0)
            .expect("canonical resumed fixture worktree")
            .to_string_lossy()
            .into_owned();
        let thread_resume = resumed.take_request("thread/resume");
        assert_eq!(
            thread_resume["params"],
            json!({
                "threadId": "thread-resume-policy",
                "cwd": resumed_worktree,
                "approvalPolicy": "on-request",
                "sandbox": "workspace-write",
                "runtimeWorkspaceRoots": [resumed_worktree]
            })
        );
    }

    #[test]
    fn launch_arguments_share_the_reviewed_prefix_with_the_inventory_check() {
        let arguments = app_server_arguments();
        let prefix_len = crate::providers::codex_launch_prefix_len();
        assert_eq!(
            arguments[..prefix_len],
            crate::providers::codex_feature_arguments()[..prefix_len]
        );
        assert_eq!(
            arguments[prefix_len..],
            APP_SERVER_SUBCOMMAND
                .iter()
                .map(|element| (*element).to_owned())
                .collect::<Vec<_>>()[..]
        );
        assert!(!arguments.iter().any(|argument| argument.contains("cmd /c")));
    }

    #[test]
    fn diagnostic_ring_retains_only_its_sanitized_tail() {
        let mut ring = DiagnosticRing::new(12);
        ring.push("first");
        ring.push("second");
        ring.push("third");
        let rendered = ring.render();
        assert!(rendered.len() <= 12);
        assert!(rendered.ends_with("third"));
    }

    #[cfg(windows)]
    #[test]
    #[ignore = "spends a real Codex turn only with AGENT_WORLD_ALLOW_PAID_CODEX_SMOKE=1"]
    fn installed_windows_codex_smoke_is_explicitly_paid_opt_in() {
        if std::env::var("AGENT_WORLD_ALLOW_PAID_CODEX_SMOKE").as_deref() != Ok("1") {
            return;
        }
        let worktree = PathBuf::from(
            std::env::var_os("AGENT_WORLD_CODEX_SMOKE_WORKTREE")
                .expect("AGENT_WORLD_CODEX_SMOKE_WORKTREE is required for the paid smoke"),
        );
        let mut port =
            crate::live_turn::ProviderPort::spawn(Box::new(CodexAppServerRunner::default()))
                .expect("spawn Codex provider port");
        let turn_id = Uuid::new_v4();
        port.command_sender()
            .send(ProviderCommand::Start {
                turn_id,
                thread_id: Uuid::new_v4(),
                worktree_path: worktree,
                prompt: "Reply with exactly AGENT_WORLD_SMOKE_OK. Do not use tools.".into(),
                session: None,
            })
            .expect("send paid smoke turn");
        let deadline = Instant::now() + Duration::from_secs(10 * 60);
        loop {
            match port.try_recv_event() {
                Ok(ProviderEvent::Completed {
                    turn_id: completed, ..
                }) if completed == turn_id => {
                    break;
                }
                Ok(
                    ProviderEvent::Failed { diagnostic, .. }
                    | ProviderEvent::ProcessLost { diagnostic, .. },
                ) => panic!("{diagnostic}"),
                Ok(_) | Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => panic!("Codex provider port disconnected"),
            }
            assert!(Instant::now() < deadline, "paid Codex smoke timed out");
            thread::sleep(Duration::from_millis(20));
        }
        port.begin_shutdown();
        let shutdown_deadline = Instant::now() + Duration::from_secs(10);
        while !port.is_finished() && Instant::now() < shutdown_deadline {
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(
            port.finish_if_stopped(),
            Ok(true),
            "paid Codex smoke provider did not shut down cleanly"
        );
    }
}
