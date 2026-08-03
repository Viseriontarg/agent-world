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

pub(crate) const CODEX_DISABLED_FEATURES: &[&str] = &[
    "apps",
    "auth_elicitation",
    "browser_use",
    "browser_use_external",
    "browser_use_full_cdp_access",
    "code_mode_host",
    "computer_use",
    "enable_mcp_apps",
    "goals",
    "guardian_approval",
    "hooks",
    "image_generation",
    "in_app_browser",
    "in_app_updates",
    "memories",
    "multi_agent",
    "multi_agent_v2",
    "network_proxy",
    "plugin_sharing",
    "plugins",
    "remote_plugin",
    "request_permissions_tool",
    "shell_snapshot",
    "skill_mcp_dependency_install",
    "skill_search",
    "standalone_web_search",
    "tool_call_mcp_elicitation",
    "tool_suggest",
    "workspace_dependencies",
];

pub(crate) const SUPPORTED_CODEX_VERSION: &str = "codex-cli 0.146.0";

/// Version-pinned launch overrides shared by the zero-turn inventory check and live app-server.
/// Each value is passed as its own argv element after `--config`; no shell parses these strings.
pub(crate) const CODEX_LIVE_CONFIG_OVERRIDES: &[&str] = &[
    "allow_login_shell=false",
    "shell_environment_policy.inherit=\"core\"",
    "mcp_servers={}",
    "web_search=\"disabled\"",
];

const CODEX_ALLOWED_ENABLED_FEATURES: &[&str] = &[
    "collaboration_modes",
    "enable_request_compression",
    "fast_mode",
    "item_ids",
    "mentions_v2",
    "personality",
    "remote_compaction_v2",
    "resize_all_images",
    "shell_tool",
    "sqlite",
    "steer",
    "terminal_resize_reflow",
    "tool_search_always_defer_mcp_tools",
    "tui_app_server",
    "unified_exec",
];

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
    verify_codex_version(&version)?;

    let mut verified = vec!["installed CLI version".into()];
    verify_codex_disabled_features(&program)?;
    verified.push("bounded adapter extension features disable cleanly".into());
    let exec_help_output = run(&program, &["--ask-for-approval", "never", "exec", "--help"])?;
    if !exec_help_output.status.success() {
        return Err(format!(
            "Codex exec contract check failed: {}",
            String::from_utf8_lossy(&exec_help_output.stderr).trim()
        ));
    }
    let exec_help = format!(
        "{}\n{}",
        String::from_utf8_lossy(&exec_help_output.stdout),
        String::from_utf8_lossy(&exec_help_output.stderr)
    );
    let required_exec = [
        "--json",
        "--sandbox",
        "read-only",
        "--cd",
        "--ignore-user-config",
        "--ignore-rules",
        "--ephemeral",
        "--strict-config",
        "--color",
        "read from stdin",
    ];
    if required_exec
        .iter()
        .any(|capability| !exec_help.contains(capability))
    {
        return Err("installed Codex CLI lacks the bounded exec contract".into());
    }
    verified.push(
        "legacy exec JSONL/stdin/worktree/read-only flags (zero-turn probe only; not the live adapter)"
            .into(),
    );
    verified.push("legacy exec never-approve flag (zero-turn probe only)".into());
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
    let thread_start_schema =
        fs::read_to_string(schema_dir.join("v2/ThreadStartParams.json")).map_err(err)?;
    let thread_resume_schema =
        fs::read_to_string(schema_dir.join("v2/ThreadResumeParams.json")).map_err(err)?;
    let turn_start_schema =
        fs::read_to_string(schema_dir.join("v2/TurnStartParams.json")).map_err(err)?;
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
    let thread_start_schema: Value = serde_json::from_str(&thread_start_schema).map_err(err)?;
    let thread_resume_schema: Value = serde_json::from_str(&thread_resume_schema).map_err(err)?;
    let turn_start_schema: Value = serde_json::from_str(&turn_start_schema).map_err(err)?;
    let thread_contract = thread_start_schema.pointer("/properties/sandbox").is_some()
        && thread_start_schema
            .pointer("/properties/approvalPolicy")
            .is_some()
        && thread_start_schema
            .pointer("/properties/environments")
            .is_some()
        && thread_start_schema
            .pointer("/properties/runtimeWorkspaceRoots")
            .is_some()
        && thread_start_schema
            .pointer("/properties/experimentalRawEvents")
            .is_some()
        && json_contains_string(
            &thread_start_schema["definitions"]["SandboxMode"],
            "workspace-write",
        )
        && json_contains_string(
            &thread_start_schema["definitions"]["AskForApproval"],
            "on-request",
        );
    let resume_contract = thread_resume_schema
        .pointer("/properties/sandbox")
        .is_some()
        && thread_resume_schema
            .pointer("/properties/approvalPolicy")
            .is_some()
        && thread_resume_schema
            .pointer("/properties/runtimeWorkspaceRoots")
            .is_some();
    let turn_contract = turn_start_schema
        .pointer("/properties/sandboxPolicy")
        .is_some()
        && turn_start_schema
            .pointer("/properties/approvalPolicy")
            .is_some()
        && turn_start_schema
            .pointer("/properties/environments")
            .is_some()
        && turn_start_schema
            .pointer("/properties/runtimeWorkspaceRoots")
            .is_some()
        && [
            "workspaceWrite",
            "writableRoots",
            "networkAccess",
            "excludeSlashTmp",
            "excludeTmpdirEnvVar",
        ]
        .iter()
        .all(|field| {
            json_contains_string(&turn_start_schema["definitions"]["SandboxPolicy"], field)
        });
    if !thread_contract || !resume_contract || !turn_contract {
        return Err(
            "installed Codex schema lacks the pinned workspace-write/on-request contract".into(),
        );
    }
    verified.push("generated protocol schema contains start/resume/fork/interrupt".into());
    verified.push("generated protocol schema contains approvals and user input".into());
    verified.push(
        "generated protocol schema contains workspace-write/on-request with explicit writable roots and network control"
            .into(),
    );

    Ok(ProviderProbe {
        provider: Provider::Codex,
        installed: true,
        version,
        verified_without_model_turn: verified,
        declared_by_installed_cli: required
            .into_iter()
            .map(str::to_owned)
            .chain([
                "legacy exec JSONL final-result surface (not the live adapter)".into(),
                "app-server thread/start and thread/resume".into(),
                "app-server workspace-write sandbox and on-request approval fields".into(),
                "app-server approval request/response".into(),
                "app-server requestUserInput".into(),
            ])
            .collect(),
        live_spike_still_required: vec![
            "authenticated app-server turn in a verified isolated worktree on real Windows".into(),
            "effective workspace-write root, network, and escalation enforcement".into(),
            "authenticated installed-provider equivalence for deterministic streaming and approval/input fixtures".into(),
            "authenticated installed-provider equivalence for deterministic interrupt fixtures".into(),
            "authenticated restart observation for deterministic resume/reconciliation fixtures".into(),
            "process containment and current host/provider resource measurements".into(),
            "fork context integrity".into(),
        ],
        error: None,
    })
}

fn json_contains_string(value: &Value, expected: &str) -> bool {
    match value {
        Value::String(value) => value == expected,
        Value::Array(values) => values
            .iter()
            .any(|value| json_contains_string(value, expected)),
        Value::Object(values) => values
            .iter()
            .any(|(key, value)| key == expected || json_contains_string(value, expected)),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

/// Builds the single reviewed launch prefix - one `--disable` pair per denied feature and one
/// `--config` pair per pinned override - followed by `trailing` as further argv elements. Every
/// Codex invocation goes through here so the inventory check and the live app-server are identical
/// by construction.
pub(crate) fn codex_launch_arguments(trailing: &[&str]) -> Vec<String> {
    let mut arguments = Vec::with_capacity(
        CODEX_DISABLED_FEATURES.len() * 2 + CODEX_LIVE_CONFIG_OVERRIDES.len() * 2 + trailing.len(),
    );
    for feature in CODEX_DISABLED_FEATURES {
        arguments.push("--disable".to_owned());
        arguments.push((*feature).to_owned());
    }
    for config in CODEX_LIVE_CONFIG_OVERRIDES {
        arguments.push("--config".into());
        arguments.push((*config).into());
    }
    arguments.extend(trailing.iter().map(|element| (*element).to_owned()));
    arguments
}

/// Number of argv elements every [`codex_launch_arguments`] call emits before its trailing
/// subcommand.
#[cfg(test)]
pub(crate) fn codex_launch_prefix_len() -> usize {
    CODEX_DISABLED_FEATURES.len() * 2 + CODEX_LIVE_CONFIG_OVERRIDES.len() * 2
}

pub(crate) fn codex_feature_arguments() -> Vec<String> {
    codex_launch_arguments(&["features", "list"])
}

pub(crate) fn verify_codex_disabled_features(program: &Path) -> AppResult<()> {
    let arguments = codex_feature_arguments();
    let borrowed: Vec<_> = arguments.iter().map(String::as_str).collect();
    let output = run(program, &borrowed)?;
    if !output.status.success() {
        return Err(format!(
            "Codex feature-isolation check failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    verify_codex_feature_output(&String::from_utf8_lossy(&output.stdout))
}

pub(crate) fn verify_codex_version(version: &str) -> AppResult<()> {
    let version = version.trim();
    if version == SUPPORTED_CODEX_VERSION {
        Ok(())
    } else {
        Err(format!(
            "unsupported Codex CLI version {version:?}; live turns require {SUPPORTED_CODEX_VERSION}"
        ))
    }
}

pub(crate) fn verify_codex_feature_output(stdout: &str) -> AppResult<()> {
    let mut seen = std::collections::BTreeMap::new();
    for line in stdout.lines().filter(|line| !line.trim().is_empty()) {
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() < 3 || !matches!(fields.last(), Some(&"true" | &"false")) {
            return Err(format!(
                "Codex feature-isolation check returned an unrecognized row: {line}"
            ));
        }
        let name = fields[0];
        let enabled = fields.last() == Some(&"true");
        if seen.insert(name, enabled).is_some() {
            return Err(format!(
                "Codex feature-isolation check returned duplicate feature {name}"
            ));
        }
    }
    if seen.is_empty() {
        return Err("Codex feature-isolation check returned no feature inventory".into());
    }
    for feature in CODEX_DISABLED_FEATURES {
        if seen.get(feature) != Some(&false) {
            return Err(format!(
                "installed Codex did not confirm feature {feature} is disabled"
            ));
        }
    }
    if let Some(feature) = seen.iter().find_map(|(name, enabled)| {
        (*enabled && !CODEX_ALLOWED_ENABLED_FEATURES.contains(name)).then_some(*name)
    }) {
        return Err(format!(
            "installed Codex exposed unreviewed enabled feature {feature}; live turns fail closed"
        ));
    }
    Ok(())
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
        "--print",
        "--input-format",
        "stream-json",
        "--output-format",
        "json",
        "--session-id",
        "--resume",
        "--fork-session",
        "--permission-mode",
        "--tools",
        "--allowedTools",
        "--safe-mode",
        "--strict-mcp-config",
        "--mcp-config",
        "--disable-slash-commands",
        "--no-chrome",
    ];
    if required.iter().any(|flag| !help.contains(flag)) {
        return Err("installed Claude CLI lacks one or more declared restriction flags".into());
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
            "read-only tool-restriction flags".into(),
        ],
        declared_by_installed_cli: vec![
            "streaming start/resume".into(),
            "stable session ID".into(),
            "session fork".into(),
            "permission mode".into(),
        ],
        live_spike_still_required: vec![
            "authenticated Claude turn on real Windows".into(),
            "incremental response streaming".into(),
            "tool approval round-trip".into(),
            "immediate interrupt".into(),
            "safe fork with retained context".into(),
        ],
        error: None,
    })
}

pub(crate) fn locate(name: &str) -> AppResult<PathBuf> {
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

pub(crate) fn locate_native_executable(name: &str) -> AppResult<PathBuf> {
    let path = std::env::var_os("PATH").ok_or_else(|| "PATH is unavailable".to_owned())?;
    for directory in std::env::split_paths(&path) {
        let direct = directory.join(format!("{name}.exe"));
        if direct.is_file() {
            return fs::canonicalize(&direct).map_err(err);
        }
        let wrapper = directory.join(format!("{name}.cmd"));
        if name.eq_ignore_ascii_case("codex") && wrapper.is_file() {
            let native = directory.join(
                "node_modules/@openai/codex/node_modules/@openai/codex-win32-x64/vendor/x86_64-pc-windows-msvc/bin/codex.exe",
            );
            if native.is_file() {
                return fs::canonicalize(&native).map_err(err);
            }
        }
    }
    Err(format!(
        "a native {name}.exe was not found on PATH; command wrappers are not allowed for live turns"
    ))
}

pub(crate) fn command(program: &Path, args: &[&str]) -> Command {
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
    fn reviewed_inventory(extra: &str) -> String {
        let mut output = super::CODEX_DISABLED_FEATURES
            .iter()
            .map(|feature| format!("{feature} stable false"))
            .collect::<Vec<_>>()
            .join("\n");
        output.push_str("\nshell_tool stable true\n");
        output.push_str(extra);
        output
    }

    #[test]
    fn live_feature_contract_is_exact_and_fails_closed_on_unknown_enabled_tools() {
        super::verify_codex_feature_output(&reviewed_inventory("unknown_future stable false\n"))
            .expect("disabled unknown feature is inert");
        assert!(
            super::verify_codex_feature_output(&reviewed_inventory("unknown_future stable true\n"))
                .expect_err("unreviewed enabled feature must fail closed")
                .contains("unreviewed enabled feature")
        );

        let mut missing = reviewed_inventory("");
        missing = missing
            .lines()
            .filter(|line| !line.starts_with("tool_suggest "))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            super::verify_codex_feature_output(&missing)
                .expect_err("missing denied feature must fail closed")
                .contains("tool_suggest")
        );
    }

    #[test]
    fn feature_and_environment_overrides_are_centralized_and_unique() {
        let arguments = super::codex_feature_arguments();
        for feature in super::CODEX_DISABLED_FEATURES {
            assert_eq!(
                arguments
                    .windows(2)
                    .filter(|pair| pair[0] == "--disable" && pair[1] == *feature)
                    .count(),
                1,
                "{feature} must be disabled exactly once"
            );
        }
        assert!(
            arguments
                .iter()
                .any(|value| value == "allow_login_shell=false")
        );
        assert!(
            arguments
                .iter()
                .any(|value| value == "shell_environment_policy.inherit=\"core\"")
        );
        for config in super::CODEX_LIVE_CONFIG_OVERRIDES {
            assert_eq!(
                arguments
                    .windows(2)
                    .filter(|pair| pair[0] == "--config" && pair[1] == *config)
                    .count(),
                1,
                "{config} must be passed as one unique config argv element"
            );
        }
        super::verify_codex_version(super::SUPPORTED_CODEX_VERSION)
            .expect("pinned version accepted");
        assert!(super::verify_codex_version("codex-cli 0.147.0").is_err());
    }

    #[test]
    fn installed_provider_surfaces_are_probeable_without_a_turn() {
        let result = super::probe_all();
        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|probe| probe.error.is_none()));
    }
}
