mod codex_app_server;
mod core;
mod live_turn;
mod providers;
mod ui;

use eframe::{NativeOptions, Renderer, egui};
use serde_json::json;
use std::path::PathBuf;

fn main() {
    if let Err(error) = run() {
        eprintln!("Agent World: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|arg| arg == "--self-check") {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "core": core::self_check()?,
                "provider_probe": providers::probe_json()
            }))
            .map_err(err)?
        );
        return Ok(());
    }
    if args.iter().any(|arg| arg == "--live-slice-self-check") {
        println!(
            "{}",
            serde_json::to_string_pretty(&core::live_slice_self_check()?).map_err(err)?
        );
        return Ok(());
    }
    if args.iter().any(|arg| arg == "--probe-providers") {
        println!(
            "{}",
            serde_json::to_string_pretty(&providers::probe_json()).map_err(err)?
        );
        return Ok(());
    }
    if args.iter().any(|arg| arg == "--seed-resource-fixture") {
        let runtime_root = runtime_root(&args)?;
        println!(
            "{}",
            serde_json::to_string_pretty(&core::seed_resource_fixture(runtime_root)?)
                .map_err(err)?
        );
        return Ok(());
    }

    let runtime_root = runtime_root(&args)?;
    let native_options = NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_app_id("agent-world")
            .with_inner_size([1240.0, 760.0])
            .with_min_inner_size([900.0, 560.0]),
        renderer: Renderer::Glow,
        centered: true,
        persist_window: false,
        ..Default::default()
    };
    eframe::run_native(
        "Agent World",
        native_options,
        Box::new(move |creation_context| {
            let app = ui::AgentWorldApp::new(
                runtime_root,
                creation_context.egui_ctx.clone(),
                Box::new(codex_app_server::CodexAppServerRunner::default()),
            )
            .map_err(std::io::Error::other)?;
            Ok(Box::new(app))
        }),
    )
    .map_err(err)
}

fn runtime_root(args: &[String]) -> Result<PathBuf, String> {
    if let Some(index) = args.iter().position(|arg| arg == "--runtime-root") {
        return args
            .get(index + 1)
            .map(PathBuf::from)
            .ok_or_else(|| "--runtime-root requires a path".into());
    }
    std::env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .map(|path| path.join("AgentWorld"))
        .ok_or_else(|| "LOCALAPPDATA is unavailable".into())
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}
