use serde::Serialize;
use serde_json::{Value, json};
use std::{
    fs,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;

use crate::core::{AppResult, Provider};

#[derive(Clone, Debug, Serialize)]
pub struct ProviderProbe {
    pub provider: Provider,
    pub installed: bool,
    pub version: String,
    pub verified_without_model_turn: Vec<String>,
    pub declared_by_installed_cli: Vec<String>,
    pub live_spike_still_required: Vec<String>,
    pub error: Option<String>,
}

pub fn probe_all() -> Vec<ProviderProbe> {
    vec![
        probe_codex().unwrap_or_else(|error| failed(Provider::Codex, error)),
        probe_claude().unwrap_or_else(|error| failed(Provider::Claude, error)),
    ]
}

pub fn probe_json() -> Value {
    json!({
        "paid_model_turns": 0,
        "providers": probe_all()
    })
}

fn probe_codex() -> AppResult<ProviderProbe> {
    let program = locate("codex")?;
    let version = text(run(&program, &["--version"])?);
    if version.is_empty() {
        return Err("codex --version returned no version".into());
    }

    let mut verified = vec!["installed CLI version".into()];
    initialize_codex_stdio(&program)?;
    verified.push("app-server stdio initialize handshake".into());

    let schema_dir =
        std::env::temp_dir().join(format!("agent-world-codex-probe-{}", Uuid::new_v4()));
    fs::create_dir_all(&schema_dir).map_err(err)?;
    let schema_path = schema_dir.to_string_lossy().into_owned();
    let schema_output = run(
        &program,
        &[
            "app-server",
            "generate-json-schema",
            "--experimental",
            "--out",
            &schema_path,
        ],
    )?;
    if !schema_output.status.success() {
        let _ = fs::remove_dir_all(&schema_dir);
        return Err(format!(
            "Codex schema generation failed: {}",
            String::from_utf8_lossy(&schema_output.stderr).trim()
        ));
    }
    let client = fs::read_to_string(schema_dir.join("ClientRequest.json")).map_err(err)?;
    let server = fs::read_to_string(schema_dir.join("ServerRequest.json")).map_err(err)?;
    let _ = fs::remove_dir_all(&schema_dir);
    let required = [
        "thread/start",
        "thread/resume",
        "thread/fork",
        "turn/start",
        "turn/interrupt",
    ];
    if required.iter().any(|method| !client.contains(method))
        || !server.contains("item/tool/requestUserInput")
        || !server.contains("item/commandExecution/requestApproval")
    {
        return Err("installed Codex schema lacks a required core method".into());
    }
    verified.push("generated protocol schema contains start/resume/fork/interrupt".into());
    verified.push("generated protocol schema contains approvals and user input".into());

    Ok(ProviderProbe {
        provider: Provider::Codex,
        installed: true,
        version,
        verified_without_model_turn: verified,
        declared_by_installed_cli: required
            .into_iter()
            .map(str::to_owned)
            .chain([
                "approval request/response".into(),
                "requestUserInput".into(),
            ])
            .collect(),
        live_spike_still_required: vec![
            "streamed model turn".into(),
            "approval round-trip".into(),
            "interrupt during generation".into(),
            "resume context integrity".into(),
            "fork context integrity".into(),
        ],
        error: None,
    })
}

fn initialize_codex_stdio(program: &Path) -> AppResult<()> {
    let mut command = command(program, &["app-server", "--stdio"]);
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(err)?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "Codex stdout was unavailable".to_owned())?;
    let (line_tx, line_rx) = mpsc::sync_channel(32);
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if line_tx.send(line).is_err() {
                break;
            }
        }
    });
    let stdin = child
        .stdin
        .as_mut()
        .ok_or_else(|| "Codex stdin was unavailable".to_owned())?;
    writeln!(
        stdin,
        "{}",
        json!({
            "id": 1,
            "method": "initialize",
            "params": {
                "clientInfo": {
                    "name": "agent-world-probe",
                    "title": "Agent World Probe",
                    "version": env!("CARGO_PKG_VERSION")
                }
            }
        })
    )
    .map_err(err)?;
    stdin.flush().map_err(err)?;

    let deadline = Instant::now() + Duration::from_secs(8);
    let mut initialized = false;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let Ok(line) = line_rx.recv_timeout(remaining) else {
            break;
        };
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if value.get("id") == Some(&json!(1)) {
            initialized = value.get("error").is_none()
                && ["userAgent", "codexHome", "platformFamily", "platformOs"]
                    .iter()
                    .all(|field| value["result"].get(field).is_some())
                && value["result"]["platformOs"] == "windows";
            break;
        }
    }
    if initialized && let Some(stdin) = child.stdin.as_mut() {
        let _ = writeln!(stdin, "{}", json!({"method":"initialized"}));
        let _ = stdin.flush();
    }
    stop(&mut child);
    if initialized {
        Ok(())
    } else {
        Err("Codex app-server did not complete initialize within 8 seconds".into())
    }
}

fn probe_claude() -> AppResult<ProviderProbe> {
    let program = locate("claude")?;
    let version = text(run(&program, &["--version"])?);
    let help_output = run(&program, &["--help"])?;
    let help = format!(
        "{}\n{}",
        String::from_utf8_lossy(&help_output.stdout),
        String::from_utf8_lossy(&help_output.stderr)
    );
    let required = [
        "--input-format",
        "stream-json",
        "--output-format",
        "--session-id",
        "--resume",
        "--fork-session",
        "--permission-mode",
    ];
    if required.iter().any(|flag| !help.contains(flag)) {
        return Err("installed Claude CLI lacks one or more direct-adapter flags".into());
    }
    Ok(ProviderProbe {
        provider: Provider::Claude,
        installed: true,
        version,
        verified_without_model_turn: vec![
            "installed native CLI version".into(),
            "stream-json input/output flags".into(),
            "stable session-id, resume, and fork flags".into(),
            "permission mode surface".into(),
        ],
        declared_by_installed_cli: vec![
            "streaming start/resume".into(),
            "stable session ID".into(),
            "session fork".into(),
            "permission mode".into(),
        ],
        live_spike_still_required: vec![
            "tool approval round-trip".into(),
            "immediate interrupt".into(),
            "safe fork with retained context".into(),
        ],
        error: None,
    })
}

fn locate(name: &str) -> AppResult<PathBuf> {
    let output = Command::new("where.exe").arg(name).output().map_err(err)?;
    if !output.status.success() {
        return Err(format!("{name} was not found on PATH"));
    }
    let paths: Vec<_> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(PathBuf::from)
        .collect();
    if name.eq_ignore_ascii_case("codex")
        && let Some(wrapper) = paths.iter().find(|path| {
            path.extension()
                .and_then(|value| value.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("cmd"))
        })
    {
        let native = wrapper.parent().unwrap_or(Path::new("")).join(
            "node_modules/@openai/codex/node_modules/@openai/codex-win32-x64/vendor/x86_64-pc-windows-msvc/bin/codex.exe",
        );
        if native.is_file() {
            return Ok(native);
        }
    }
    paths
        .iter()
        .find(|path| {
            path.extension()
                .and_then(|value| value.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("exe"))
        })
        .or_else(|| {
            paths.iter().find(|path| {
                path.extension()
                    .and_then(|value| value.to_str())
                    .is_some_and(|ext| {
                        ext.eq_ignore_ascii_case("cmd") || ext.eq_ignore_ascii_case("bat")
                    })
            })
        })
        .cloned()
        .ok_or_else(|| format!("{name} path was empty"))
}

fn command(program: &Path, args: &[&str]) -> Command {
    let extension = program
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    if extension.eq_ignore_ascii_case("cmd") || extension.eq_ignore_ascii_case("bat") {
        let mut command = Command::new("cmd.exe");
        command.args(["/d", "/s", "/c"]).arg(program).args(args);
        command
    } else {
        let mut command = Command::new(program);
        command.args(args);
        command
    }
}

fn run(program: &Path, args: &[&str]) -> AppResult<Output> {
    command(program, args).output().map_err(err)
}

fn stop(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn text(output: Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if stdout.is_empty() {
        String::from_utf8_lossy(&output.stderr).trim().to_owned()
    } else {
        stdout
    }
}

fn failed(provider: Provider, error: String) -> ProviderProbe {
    ProviderProbe {
        provider,
        installed: false,
        version: String::new(),
        verified_without_model_turn: vec![],
        declared_by_installed_cli: vec![],
        live_spike_still_required: vec![],
        error: Some(error),
    }
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    #[test]
    fn installed_provider_surfaces_are_probeable_without_a_turn() {
        let result = super::probe_all();
        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|probe| probe.error.is_none()));
    }
}
