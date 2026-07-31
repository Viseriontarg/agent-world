use eframe::egui::{
    self, Align2, Color32, FontId, Key, Pos2, Rect, RichText, Sense, Stroke, StrokeKind, Vec2,
};
use std::{
    path::PathBuf,
    sync::mpsc::{self, Receiver},
    thread,
    time::Duration,
};
use uuid::Uuid;

use crate::{
    core::{
        ActorState, BootstrapSnapshot, Command, CoreEvent, CoreHandle, CoreInput, Provider,
        ThreadActorSnapshot, TimelineMessage,
    },
    providers::{self, ProviderProbe},
};

pub struct AgentWorldApp {
    core: CoreHandle,
    actors: Vec<ThreadActorSnapshot>,
    selected: Option<Uuid>,
    timeline: Vec<TimelineMessage>,
    prompt: String,
    status: String,
    probe_rx: Option<Receiver<Vec<ProviderProbe>>>,
    probes: Vec<ProviderProbe>,
}

impl AgentWorldApp {
    pub fn new(runtime_root: PathBuf, context: egui::Context) -> Result<Self, String> {
        let core = CoreHandle::spawn(runtime_root, move || context.request_repaint())?;
        core.tx.try_send(CoreInput::Bootstrap).map_err(err)?;
        Ok(Self {
            core,
            actors: vec![],
            selected: None,
            timeline: vec![],
            prompt: String::new(),
            status: "Loading durable state…".into(),
            probe_rx: None,
            probes: vec![],
        })
    }

    fn poll(&mut self) {
        let mut refresh = false;
        while let Ok(event) = self.core.rx.try_recv() {
            match event {
                CoreEvent::Bootstrap(BootstrapSnapshot { actors, .. }) => {
                    self.actors = actors;
                    if self
                        .selected
                        .is_none_or(|id| !self.actors.iter().any(|actor| actor.thread_id == id))
                    {
                        self.selected = self.actors.first().map(|actor| actor.thread_id);
                    }
                    self.request_timeline();
                    self.status = "Durable core online".into();
                }
                CoreEvent::Receipt(receipt) => {
                    self.status = format!(
                        "{} · {}",
                        receipt.status,
                        receipt
                            .result
                            .get("event_type")
                            .and_then(|value| value.as_str())
                            .unwrap_or("command")
                    );
                    refresh = true;
                }
                CoreEvent::Timeline {
                    thread_id,
                    messages,
                } => {
                    if self.selected == Some(thread_id) {
                        self.timeline = messages;
                    }
                }
                CoreEvent::Error(error) => self.status = format!("Core error: {error}"),
            }
        }
        if refresh {
            let _ = self.core.tx.try_send(CoreInput::Bootstrap);
            self.request_timeline();
        }
        if let Some(rx) = &self.probe_rx
            && let Ok(probes) = rx.try_recv()
        {
            self.probes = probes;
            self.probe_rx = None;
            self.status = "Zero-turn provider probe complete".into();
        }
    }

    fn request_timeline(&self) {
        if let Some(thread_id) = self.selected {
            let _ = self.core.tx.try_send(CoreInput::Timeline {
                thread_id,
                limit: 100,
            });
        }
    }

    fn selected_actor(&self) -> Option<&ThreadActorSnapshot> {
        self.selected
            .and_then(|id| self.actors.iter().find(|actor| actor.thread_id == id))
    }

    fn send_prompt(&mut self) {
        let Some(thread_id) = self.selected else {
            return;
        };
        let text = self.prompt.trim();
        if text.is_empty() {
            return;
        }
        match self.core.command(Command::TurnSend {
            thread_id,
            text: text.to_owned(),
        }) {
            Ok(()) => {
                self.prompt.clear();
                self.status =
                    "Prompt persisted; live provider turn remains gated by the compatibility spike"
                        .into();
            }
            Err(error) => self.status = error,
        }
    }

    fn handle_shortcuts(&mut self, ctx: &egui::Context) {
        if ctx.input_mut(|input| input.consume_key(egui::Modifiers::CTRL, Key::Enter)) {
            self.send_prompt();
        }
        if ctx.input_mut(|input| input.consume_key(egui::Modifiers::CTRL, Key::Period))
            && let Some(thread_id) = self.selected
        {
            let _ = self.core.command(Command::TurnInterrupt { thread_id });
        }
        if ctx.input_mut(|input| input.consume_key(egui::Modifiers::NONE, Key::Tab)) {
            self.cycle_attention();
        }
        for (index, key) in [
            Key::Num1,
            Key::Num2,
            Key::Num3,
            Key::Num4,
            Key::Num5,
            Key::Num6,
            Key::Num7,
            Key::Num8,
            Key::Num9,
        ]
        .into_iter()
        .enumerate()
        {
            if ctx.input_mut(|input| input.consume_key(egui::Modifiers::NONE, key))
                && let Some(actor) = self.actors.get(index)
            {
                self.selected = Some(actor.thread_id);
                self.request_timeline();
            }
        }
    }

    fn cycle_attention(&mut self) {
        let attention: Vec<_> = self
            .actors
            .iter()
            .filter(|actor| actor.attention)
            .map(|actor| actor.thread_id)
            .collect();
        if attention.is_empty() {
            return;
        }
        let next = attention
            .iter()
            .position(|id| Some(*id) == self.selected)
            .map(|index| (index + 1) % attention.len())
            .unwrap_or(0);
        self.selected = Some(attention[next]);
        self.request_timeline();
    }

    fn paint_scene(&mut self, ui: &mut egui::Ui) {
        let available = ui.available_size().max(Vec2::new(420.0, 320.0));
        let (room, _) = ui.allocate_exact_size(available, Sense::hover());
        let painter = ui.painter_at(room);
        painter.rect_filled(room, 14.0, Color32::from_rgb(19, 25, 36));

        let inner = room.shrink(18.0);
        painter.rect_filled(inner, 10.0, Color32::from_rgb(27, 36, 50));
        for x in (0..8).map(|i| inner.left() + i as f32 * inner.width() / 7.0) {
            painter.line_segment(
                [Pos2::new(x, inner.top()), Pos2::new(x, inner.bottom())],
                Stroke::new(1.0, Color32::from_gray(45)),
            );
        }
        for y in (0..6).map(|i| inner.top() + i as f32 * inner.height() / 5.0) {
            painter.line_segment(
                [Pos2::new(inner.left(), y), Pos2::new(inner.right(), y)],
                Stroke::new(1.0, Color32::from_gray(45)),
            );
        }
        painter.text(
            inner.left_top() + Vec2::new(16.0, 14.0),
            Align2::LEFT_TOP,
            "OPERATIONS FLOOR",
            FontId::monospace(13.0),
            Color32::from_rgb(117, 137, 163),
        );

        let columns = ((inner.width() / 180.0).floor() as usize).max(1);
        let station_size = Vec2::new(150.0, 105.0);
        for (index, actor) in self.actors.clone().into_iter().enumerate() {
            let column = index % columns;
            let row = index / columns;
            let origin = Pos2::new(
                inner.left() + 28.0 + column as f32 * 180.0,
                inner.top() + 54.0 + row as f32 * 145.0,
            );
            let station = Rect::from_min_size(origin, station_size);
            let selected = self.selected == Some(actor.thread_id);
            let outline = if selected {
                Color32::from_rgb(97, 203, 255)
            } else {
                Color32::from_rgb(64, 78, 98)
            };
            painter.rect_filled(station, 8.0, Color32::from_rgb(34, 45, 62));
            painter.rect_stroke(
                station,
                8.0,
                Stroke::new(if selected { 2.0 } else { 1.0 }, outline),
                StrokeKind::Inside,
            );
            let desk = Rect::from_min_size(origin + Vec2::new(18.0, 20.0), Vec2::new(114.0, 35.0));
            painter.rect_filled(desk, 5.0, Color32::from_rgb(49, 62, 80));
            let screen = Rect::from_min_size(origin + Vec2::new(48.0, 7.0), Vec2::new(54.0, 27.0));
            painter.rect_filled(
                screen,
                3.0,
                provider_color(actor.provider).gamma_multiply(0.45),
            );

            let body = origin + Vec2::new(75.0, 74.0);
            painter.circle_filled(body, 18.0, provider_color(actor.provider));
            painter.circle_filled(
                body - Vec2::new(0.0, 21.0),
                10.0,
                Color32::from_rgb(224, 192, 165),
            );
            painter.text(
                Pos2::new(station.center().x, station.bottom() - 7.0),
                Align2::CENTER_BOTTOM,
                &actor.label,
                FontId::proportional(13.0),
                Color32::WHITE,
            );
            painter.text(
                station.right_top() + Vec2::new(-8.0, 7.0),
                Align2::RIGHT_TOP,
                state_label(actor.state),
                FontId::monospace(10.0),
                state_color(actor.state),
            );
            if actor.attention || actor.unread_count > 0 {
                painter.circle_filled(
                    station.right_top() + Vec2::new(-5.0, 5.0),
                    7.0,
                    Color32::from_rgb(255, 188, 74),
                );
            }
            if ui
                .interact(station, ui.id().with(actor.thread_id), Sense::click())
                .on_hover_text(format!(
                    "{} · {} · {}",
                    actor.label,
                    actor.provider.as_str(),
                    actor.state.as_str()
                ))
                .clicked()
            {
                self.selected = Some(actor.thread_id);
                self.request_timeline();
            }
        }
    }

    fn right_panel(&mut self, parent: &mut egui::Ui) {
        egui::Panel::right("command_panel")
            .resizable(true)
            .default_size(390.0)
            .min_size(310.0)
            .show(parent, |ui| {
                let Some(actor) = self.selected_actor().cloned() else {
                    ui.heading("No character selected");
                    return;
                };
                ui.horizontal(|ui| {
                    ui.heading(&actor.label);
                    ui.label(
                        RichText::new(actor.provider.as_str().to_uppercase())
                            .color(provider_color(actor.provider))
                            .monospace(),
                    );
                });
                ui.label(format!("State: {}", actor.state.as_str()));
                ui.small(
                    actor
                        .worktree_path
                        .as_ref()
                        .map(|path| path.display().to_string())
                        .unwrap_or_else(|| "Shared source view; isolated worktree not created".into()),
                );
                if actor.worktree_path.is_none()
                    && ui.button("Create isolated worktree").clicked()
                {
                    match self.core.command(Command::WorktreeCreate {
                        worktree_id: Uuid::new_v4(),
                        thread_id: actor.thread_id,
                    }) {
                        Ok(()) => self.status = "Creating native Git worktree…".into(),
                        Err(error) => self.status = error,
                    }
                }
                ui.separator();
                ui.label(RichText::new("Timeline").strong());
                egui::ScrollArea::vertical()
                    .id_salt("timeline")
                    .max_height(260.0)
                    .stick_to_bottom(true)
                    .show(ui, |ui| {
                        if self.timeline.is_empty() {
                            ui.weak("No durable messages yet.");
                        }
                        for message in &self.timeline {
                            ui.group(|ui| {
                                ui.small(
                                    RichText::new(format!(
                                        "{} · #{}",
                                        message.role, message.sequence
                                    ))
                                    .monospace()
                                    .color(Color32::from_gray(150)),
                                );
                                ui.label(&message.body);
                            });
                        }
                    });
                ui.separator();
                let prompt_id = ui.id().with("prompt");
                ui.add(
                    egui::TextEdit::multiline(&mut self.prompt)
                        .id(prompt_id)
                        .desired_rows(5)
                        .hint_text("Prompt the selected character"),
                );
                ui.horizontal(|ui| {
                    if ui.button("Persist prompt  Ctrl+Enter").clicked() {
                        self.send_prompt();
                    }
                    if ui.button("Interrupt  Ctrl+.").clicked() {
                        let _ = self.core.command(Command::TurnInterrupt {
                            thread_id: actor.thread_id,
                        });
                    }
                });
                ui.small(
                    "Live model turns are deliberately disabled until the installed-provider spike proves approval, interrupt, resume, and fork.",
                );
                ui.separator();
                if self.probe_rx.is_none()
                    && ui.button("Run zero-turn provider probe").clicked()
                {
                    let (tx, rx) = mpsc::channel();
                    thread::spawn(move || {
                        let _ = tx.send(providers::probe_all());
                    });
                    self.probe_rx = Some(rx);
                    self.status = "Probing installed protocols without a model turn…".into();
                }
                if self.probe_rx.is_some() {
                    ui.spinner();
                    ui.label("Protocol probe running");
                }
                for probe in &self.probes {
                    ui.collapsing(
                        format!(
                            "{} {}",
                            probe.provider.as_str(),
                            if probe.error.is_none() { "ready" } else { "blocked" }
                        ),
                        |ui| {
                            ui.monospace(&probe.version);
                            for item in &probe.verified_without_model_turn {
                                ui.label(format!("✓ {item}"));
                            }
                            for item in &probe.live_spike_still_required {
                                ui.label(format!("○ {item}"));
                            }
                            if let Some(error) = &probe.error {
                                ui.colored_label(Color32::LIGHT_RED, error);
                            }
                        },
                    );
                }
            });
    }
}

impl eframe::App for AgentWorldApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        self.poll();
        self.handle_shortcuts(&ctx);
        egui::Panel::top("status_bar").show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.heading("Agent World");
                ui.separator();
                ui.label(&self.status);
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.small("1–9 select · Tab attention · Enter prompt");
                });
            });
        });
        self.right_panel(ui);
        egui::CentralPanel::default().show(ui, |ui| self.paint_scene(ui));

        if self.probe_rx.is_some()
            || self.actors.iter().any(|actor| {
                matches!(
                    actor.state,
                    ActorState::Starting | ActorState::Running | ActorState::Interrupting
                )
            })
        {
            ctx.request_repaint_after(Duration::from_millis(66));
        }
    }
}

fn provider_color(provider: Provider) -> Color32 {
    match provider {
        Provider::Codex => Color32::from_rgb(72, 185, 151),
        Provider::Claude => Color32::from_rgb(218, 139, 91),
    }
}

fn state_color(state: ActorState) -> Color32 {
    match state {
        ActorState::AwaitingApproval | ActorState::WaitingUser => Color32::from_rgb(255, 188, 74),
        ActorState::Failed => Color32::from_rgb(255, 103, 117),
        ActorState::Running | ActorState::Starting => Color32::from_rgb(97, 203, 255),
        _ => Color32::from_gray(170),
    }
}

fn state_label(state: ActorState) -> &'static str {
    match state {
        ActorState::AwaitingApproval => "APPROVAL",
        ActorState::WaitingUser => "USER",
        ActorState::Failed => "FAILED",
        ActorState::Interrupting => "STOPPING",
        ActorState::Starting => "STARTING",
        ActorState::Running => "RUNNING",
        ActorState::Archived => "ARCHIVED",
        ActorState::Stopped => "STOPPED",
        ActorState::Idle => "IDLE",
    }
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}
