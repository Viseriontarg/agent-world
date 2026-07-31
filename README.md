# Agent World

Lean Windows-native command room for Codex and Claude.

This repository currently contains the Phase-1 executable slice:

- native `eframe/egui` command-room scene rendered with `Painter`;
- OpenGL `glow` (selected after the DX12 shell exceeded 200 MB), AccessKit, keyboard selection, prompt and interrupt controls;
- one SQLite writer, bounded `sync_channel(8/32)` queues, durable receipts/events/projections;
- idempotency conflict detection and deterministic native Git worktree reconciliation;
- paged actor timeline and a 50-actor/20,000-message resource fixture;
- zero-turn Codex app-server handshake/schema probe and Claude CLI capability probe.

Live provider turns are intentionally not presented as working yet. The installed-provider spike still has to prove streaming, approval, immediate interrupt, resume, and fork before Codex/Claude adapters are enabled. T3 is neither read nor modified.

## Build

Rust stable MSVC, Visual C++ Build Tools, and a Windows SDK are required:

```powershell
cargo build --release
```

On this machine the Visual Studio instance is usable but not registered, so builds run through:

```powershell
cmd /d /c 'call "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\Common7\Tools\VsDevCmd.bat" -no_logo -arch=x64 -host_arch=x64 && cargo build --release'
```

## Run and check

```powershell
.\target\release\agent-world.exe
.\target\release\agent-world.exe --self-check
.\target\release\agent-world.exe --probe-providers
.\scripts\measure.ps1 -WarmupSeconds 300 -SampleSeconds 60
```

Runtime data defaults to `%LOCALAPPDATA%\AgentWorld`. The resource script uses and removes a uniquely named temporary runtime root.
