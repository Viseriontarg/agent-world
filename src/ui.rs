use eframe::egui::{self, Align, Color32, FontId, Key, RichText, Stroke, TextStyle, Vec2};
use std::{
    collections::HashMap,
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

const OPERATOR_ROW_HEIGHT: f32 = 52.0;
#[cfg(test)]
const README_CONTROLS: &str = "| Click or `1`–`9` | Select an operator (`1`–`9` only when no control has focus) |\n\
| `Enter` | Focus the prompt when no control has focus |\n\
| `Ctrl+Enter` | Save the selected prompt to the durable timeline |\n\
| `Ctrl+.` | Request interruption when the selected operator is interruptible |\n\
| `F6` | Cycle operators requiring attention |\n\
| `Tab` / `Shift+Tab` | Move keyboard focus through every control and operator |";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ShortcutAction {
    FocusPrompt,
    SavePrompt,
    Interrupt,
    CycleAttention,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ShortcutBinding {
    key: Key,
    ctrl: bool,
    action: ShortcutAction,
}

const SHORTCUTS: [ShortcutBinding; 4] = [
    ShortcutBinding {
        key: Key::Enter,
        ctrl: false,
        action: ShortcutAction::FocusPrompt,
    },
    ShortcutBinding {
        key: Key::Enter,
        ctrl: true,
        action: ShortcutAction::SavePrompt,
    },
    ShortcutBinding {
        key: Key::Period,
        ctrl: true,
        action: ShortcutAction::Interrupt,
    },
    ShortcutBinding {
        key: Key::F6,
        ctrl: false,
        action: ShortcutAction::CycleAttention,
    },
];

const NUMBER_KEYS: [Key; 9] = [
    Key::Num1,
    Key::Num2,
    Key::Num3,
    Key::Num4,
    Key::Num5,
    Key::Num6,
    Key::Num7,
    Key::Num8,
    Key::Num9,
];

#[derive(Clone, Copy, Debug, PartialEq)]
struct LayoutMetrics {
    operator_panel_width: f32,
}

impl LayoutMetrics {
    fn for_viewport(viewport: Vec2) -> Self {
        Self {
            operator_panel_width: (viewport.x * 0.28).clamp(248.0, 328.0),
        }
    }

    fn timeline_height(available_height: f32) -> f32 {
        (available_height - 225.0).clamp(120.0, 360.0)
    }
}

#[derive(Clone, Copy)]
struct Palette {
    canvas: Color32,
    panel: Color32,
    raised: Color32,
    selected: Color32,
    border: Color32,
    text: Color32,
    muted: Color32,
    accent: Color32,
    codex: Color32,
    claude: Color32,
    active: Color32,
    warning: Color32,
    danger: Color32,
}

impl Palette {
    fn agent_world() -> Self {
        Self {
            canvas: Color32::from_rgb(8, 15, 24),
            panel: Color32::from_rgb(14, 25, 38),
            raised: Color32::from_rgb(20, 35, 51),
            selected: Color32::from_rgb(24, 52, 67),
            border: Color32::from_rgb(48, 73, 94),
            text: Color32::from_rgb(232, 241, 247),
            muted: Color32::from_rgb(157, 178, 194),
            accent: Color32::from_rgb(91, 218, 190),
            codex: Color32::from_rgb(91, 218, 190),
            claude: Color32::from_rgb(244, 169, 112),
            active: Color32::from_rgb(104, 205, 255),
            warning: Color32::from_rgb(255, 202, 92),
            danger: Color32::from_rgb(255, 116, 132),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingPromptSave {
    thread_id: Uuid,
    draft: String,
}

pub struct AgentWorldApp {
    core: CoreHandle,
    actors: Vec<ThreadActorSnapshot>,
    selected: Option<Uuid>,
    timeline: Vec<TimelineMessage>,
    drafts: HashMap<Uuid, String>,
    pending_prompt_saves: HashMap<Uuid, PendingPromptSave>,
    status: String,
    status_is_error: bool,
    preserve_status_on_bootstrap: bool,
    persistent_error: Option<String>,
    last_sequence: u64,
    probe_rx: Option<Receiver<Vec<ProviderProbe>>>,
    probes: Vec<ProviderProbe>,
}

impl AgentWorldApp {
    pub fn new(runtime_root: PathBuf, context: egui::Context) -> Result<Self, String> {
        configure_style(&context);
        let repaint_context = context.clone();
        let core = CoreHandle::spawn(runtime_root, move || repaint_context.request_repaint())?;
        core.tx.try_send(CoreInput::Bootstrap).map_err(err)?;
        Ok(Self {
            core,
            actors: vec![],
            selected: None,
            timeline: vec![],
            drafts: HashMap::new(),
            pending_prompt_saves: HashMap::new(),
            status: "Loading durable state…".into(),
            status_is_error: false,
            preserve_status_on_bootstrap: false,
            persistent_error: None,
            last_sequence: 0,
            probe_rx: None,
            probes: vec![],
        })
    }

    fn poll(&mut self) {
        let mut refresh = false;
        while let Ok(event) = self.core.rx.try_recv() {
            match event {
                CoreEvent::Bootstrap(BootstrapSnapshot {
                    actors,
                    last_sequence,
                }) => {
                    self.actors = actors;
                    self.last_sequence = last_sequence;
                    if self
                        .selected
                        .is_none_or(|id| !self.actors.iter().any(|actor| actor.thread_id == id))
                    {
                        self.selected = operator_order(&self.actors)
                            .first()
                            .map(|index| self.actors[*index].thread_id);
                    }
                    self.request_timeline();
                    if self.preserve_status_on_bootstrap {
                        self.preserve_status_on_bootstrap = false;
                    } else {
                        self.set_status("Durable core online", false);
                    }
                }
                CoreEvent::Receipt(receipt) => {
                    reconcile_prompt_receipt(
                        &mut self.drafts,
                        &mut self.pending_prompt_saves,
                        receipt.command_id,
                        &receipt.status,
                    );
                    if let Some(sequence) = receipt.event_sequence {
                        self.last_sequence = self.last_sequence.max(sequence);
                    }
                    let event = receipt
                        .result
                        .get("event_type")
                        .and_then(|value| value.as_str())
                        .unwrap_or("command");
                    let detail = receipt
                        .result
                        .get("error")
                        .and_then(|value| value.as_str())
                        .map(|error| format!(" · {}", compact_text(error, 180)))
                        .unwrap_or_default();
                    self.set_status(
                        format!("{} · {event}{detail}", receipt.status),
                        receipt.status == "rejected" || receipt.status == "indeterminate",
                    );
                    self.preserve_status_on_bootstrap = true;
                    refresh = true;
                }
                CoreEvent::CommandError { command_id, error } => {
                    discard_pending_prompt_save(&mut self.pending_prompt_saves, command_id);
                    self.persistent_error = Some(error.clone());
                    self.set_status(
                        format!("Core needs attention · {}", compact_text(&error, 180)),
                        true,
                    );
                }
                CoreEvent::Timeline {
                    thread_id,
                    messages,
                } => {
                    if self.selected == Some(thread_id) {
                        self.timeline = messages;
                    }
                }
                CoreEvent::Error(error) => {
                    self.persistent_error = Some(error.clone());
                    self.set_status(
                        format!("Core needs attention · {}", compact_text(&error, 180)),
                        true,
                    );
                }
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
            self.set_status("Installed-provider surface check complete", false);
        }
    }

    fn set_status(&mut self, status: impl Into<String>, is_error: bool) {
        if !should_update_status(self.persistent_error.is_some(), is_error) {
            return;
        }
        self.status = status.into();
        self.status_is_error = is_error;
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

    fn select_actor(&mut self, thread_id: Uuid) {
        if self.selected != Some(thread_id) {
            self.selected = Some(thread_id);
            self.timeline.clear();
            self.request_timeline();
        }
    }

    fn save_prompt(&mut self) {
        let Some(thread_id) = self.selected else {
            self.set_status("Select an operator before saving a prompt", true);
            return;
        };
        if prompt_save_pending(&self.pending_prompt_saves, thread_id) {
            self.set_status("A prompt save is already pending for this operator", true);
            return;
        }
        let Some(draft) = self.drafts.get(&thread_id).cloned() else {
            return;
        };
        let text = draft.trim().to_owned();
        if text.is_empty() {
            self.set_status("Write a prompt before saving it", true);
            return;
        }
        match self.core.command(Command::TurnSend { thread_id, text }) {
            Ok(command_id) => {
                self.pending_prompt_saves
                    .insert(command_id, PendingPromptSave { thread_id, draft });
                self.set_status("Saving prompt to the durable timeline…", false);
            }
            Err(error) => self.set_status(error, true),
        }
    }

    fn request_interrupt(&mut self) {
        let Some(actor) = self.selected_actor().cloned() else {
            return;
        };
        if !actor.state.is_interruptible() {
            self.set_status(
                format!(
                    "{} is {}; there is no interruptible work",
                    actor.label,
                    state_label(actor.state).to_lowercase()
                ),
                true,
            );
            return;
        }
        match self.core.command(Command::TurnInterrupt {
            thread_id: actor.thread_id,
        }) {
            Ok(_) => self.set_status("Interruption request queued", false),
            Err(error) => self.set_status(error, true),
        }
    }

    fn handle_shortcuts(&mut self, ctx: &egui::Context) {
        let focused = ctx.memory(|memory| memory.focused());

        for binding in SHORTCUTS {
            if !shortcut_is_enabled(binding.action, focused.is_some()) {
                continue;
            }
            let modifiers = if binding.ctrl {
                egui::Modifiers::CTRL
            } else {
                egui::Modifiers::NONE
            };
            if !ctx.input_mut(|input| input.consume_key(modifiers, binding.key)) {
                continue;
            }
            match binding.action {
                ShortcutAction::FocusPrompt => {
                    ctx.memory_mut(|memory| memory.request_focus(prompt_id()));
                }
                ShortcutAction::SavePrompt => self.save_prompt(),
                ShortcutAction::Interrupt => self.request_interrupt(),
                ShortcutAction::CycleAttention => self.cycle_attention(),
            }
        }

        if focused.is_none() {
            let order = operator_order(&self.actors);
            for (slot, key) in NUMBER_KEYS.into_iter().enumerate() {
                if ctx.input_mut(|input| input.consume_key(egui::Modifiers::NONE, key))
                    && let Some(index) = order.get(slot)
                {
                    self.select_actor(self.actors[*index].thread_id);
                }
            }
        }
    }

    fn cycle_attention(&mut self) {
        let attention: Vec<_> = operator_order(&self.actors)
            .into_iter()
            .filter(|index| needs_attention(&self.actors[*index]))
            .map(|index| self.actors[index].thread_id)
            .collect();
        if attention.is_empty() {
            self.set_status("No operators currently need attention", false);
            return;
        }
        let next = attention
            .iter()
            .position(|id| Some(*id) == self.selected)
            .map(|index| (index + 1) % attention.len())
            .unwrap_or(0);
        self.select_actor(attention[next]);
    }

    fn header(&mut self, parent: &mut egui::Ui, palette: Palette) {
        egui::Panel::top("command_header")
            .frame(
                egui::Frame::new()
                    .fill(palette.panel)
                    .inner_margin(egui::Margin::symmetric(16, 10)),
            )
            .show(parent, |ui| {
                ui.horizontal_wrapped(|ui| {
                    ui.label(RichText::new("Agent World").heading().strong());
                    ui.label(
                        RichText::new("LOCAL CONTROL ROOM")
                            .monospace()
                            .small()
                            .color(palette.accent),
                    );
                    ui.separator();
                    summary_label(ui, palette.active, running_count(&self.actors), "running");
                    summary_label(
                        ui,
                        palette.warning,
                        self.actors
                            .iter()
                            .filter(|actor| needs_attention(actor))
                            .count(),
                        "need you",
                    );
                    ui.label(
                        RichText::new(format!("Last durable event #{}", self.last_sequence))
                            .small()
                            .color(palette.muted),
                    );
                });

                if let Some(error) = self.persistent_error.clone() {
                    ui.add_space(6.0);
                    egui::Frame::new()
                        .fill(palette.danger.gamma_multiply(0.12))
                        .stroke(Stroke::new(1.0, palette.danger))
                        .corner_radius(6)
                        .inner_margin(egui::Margin::symmetric(10, 7))
                        .show(ui, |ui| {
                            ui.horizontal_wrapped(|ui| {
                                ui.label(
                                    RichText::new("Needs attention")
                                        .strong()
                                        .color(palette.danger),
                                );
                                ui.label(
                                    RichText::new(compact_text(&error, 240)).color(palette.text),
                                )
                                .on_hover_text(error);
                                if ui.small_button("Dismiss").clicked() {
                                    self.persistent_error = None;
                                    self.set_status("Recovery warning dismissed", false);
                                }
                            });
                        });
                }
            });
    }

    fn status_bar(&mut self, parent: &mut egui::Ui, palette: Palette) {
        egui::Panel::bottom("status_bar")
            .frame(
                egui::Frame::new()
                    .fill(palette.panel)
                    .inner_margin(egui::Margin::symmetric(12, 7)),
            )
            .show(parent, |ui| {
                ui.horizontal_wrapped(|ui| {
                    ui.label(
                        RichText::new(&self.status)
                            .color(if self.status_is_error {
                                palette.danger
                            } else {
                                palette.muted
                            })
                            .small(),
                    );
                    ui.separator();
                    ui.label(
                        RichText::new("F6 attention · Enter prompt · Ctrl+Enter save")
                            .small()
                            .color(palette.muted),
                    );
                });
            });
    }

    fn operator_panel(&mut self, parent: &mut egui::Ui, palette: Palette, width: f32) {
        egui::Panel::left("operator_panel")
            .resizable(true)
            .default_size(width)
            .min_size(230.0)
            .max_size(360.0)
            .frame(
                egui::Frame::new()
                    .fill(palette.panel)
                    .inner_margin(egui::Margin::symmetric(10, 10)),
            )
            .show(parent, |ui| {
                ui.horizontal_wrapped(|ui| {
                    ui.label(RichText::new("Operators").strong().size(16.0));
                    ui.label(
                        RichText::new(format!("{} total", self.actors.len()))
                            .small()
                            .color(palette.muted),
                    );
                });
                ui.label(
                    RichText::new("Attention first · every row is keyboard focusable")
                        .small()
                        .color(palette.muted),
                );
                ui.add_space(6.0);

                let order = operator_order(&self.actors);
                egui::ScrollArea::vertical()
                    .id_salt("operator_list")
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        if order.is_empty() {
                            ui.label(RichText::new("No operators yet.").color(palette.muted));
                        }
                        for (position, index) in order.into_iter().enumerate() {
                            let actor = self.actors[index].clone();
                            let selected = self.selected == Some(actor.thread_id);
                            let hint = if position < 9 {
                                format!("{}  ", position + 1)
                            } else {
                                "   ".into()
                            };
                            let attention = attention_label(&actor)
                                .map(|label| format!("{label} · "))
                                .unwrap_or_default();
                            let isolation = if actor.worktree_path.is_some() {
                                "isolated"
                            } else {
                                "shared source"
                            };
                            let text = format!(
                                "{hint}{}    {}\n{attention}{} · {isolation}",
                                actor.label,
                                state_label(actor.state),
                                actor.provider.as_str().to_uppercase(),
                            );
                            let outline = if needs_attention(&actor) {
                                palette.warning
                            } else if actor.state == ActorState::Failed {
                                palette.danger
                            } else if is_running(actor.state) {
                                palette.active
                            } else {
                                provider_color(actor.provider, palette)
                            };
                            let button = egui::Button::new(
                                RichText::new(text).color(palette.text).size(13.0),
                            )
                            .selected(selected)
                            .stroke(Stroke::new(
                                if selected || needs_attention(&actor) {
                                    2.0
                                } else {
                                    1.0
                                },
                                outline,
                            ))
                            .corner_radius(6)
                            .wrap();
                            let response =
                                ui.add_sized([ui.available_width(), OPERATOR_ROW_HEIGHT], button);
                            if response.gained_focus() {
                                response.scroll_to_me(Some(Align::Center));
                            }
                            if response.clicked() {
                                self.select_actor(actor.thread_id);
                            }
                            response.on_hover_text(format!(
                                "{} · {} · {}",
                                actor.label,
                                actor.provider.as_str(),
                                actor.state.as_str()
                            ));
                            ui.add_space(4.0);
                        }
                    });
            });
    }

    fn workspace(&mut self, parent: &mut egui::Ui, palette: Palette) {
        egui::CentralPanel::default()
            .frame(
                egui::Frame::new()
                    .fill(palette.canvas)
                    .inner_margin(egui::Margin::symmetric(16, 12)),
            )
            .show(parent, |ui| {
                let timeline_height = LayoutMetrics::timeline_height(ui.available_height());
                egui::ScrollArea::vertical()
                    .id_salt("workspace_scroll")
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        let Some(actor) = self.selected_actor().cloned() else {
                            ui.vertical_centered(|ui| {
                                ui.add_space(64.0);
                                ui.heading("Select an operator");
                                ui.label(
                                    RichText::new(
                                        "Choose any row on the left to inspect its durable work.",
                                    )
                                    .color(palette.muted),
                                );
                            });
                            return;
                        };

                        self.actor_header(ui, palette, &actor);
                        ui.add_space(8.0);
                        ui.separator();
                        ui.add_space(8.0);

                        ui.horizontal_wrapped(|ui| {
                            ui.label(RichText::new("Durable timeline").strong().size(15.0));
                            ui.label(
                                RichText::new(format!("{} recent messages", self.timeline.len()))
                                    .small()
                                    .color(palette.muted),
                            );
                        });
                        self.timeline(ui, palette, timeline_height);
                        ui.add_space(8.0);
                        self.prompt_composer(ui, palette, &actor);
                        ui.add_space(8.0);
                        self.provider_readiness(ui, palette);
                    });
            });
    }

    fn actor_header(&mut self, ui: &mut egui::Ui, palette: Palette, actor: &ThreadActorSnapshot) {
        ui.horizontal_wrapped(|ui| {
            ui.label(RichText::new(&actor.label).heading().strong());
            ui.label(
                RichText::new(actor.provider.as_str().to_uppercase())
                    .monospace()
                    .strong()
                    .color(provider_color(actor.provider, palette)),
            );
            ui.label(
                RichText::new(state_label(actor.state))
                    .monospace()
                    .strong()
                    .color(state_color(actor.state, palette)),
            );
            if needs_attention(actor) {
                ui.label(
                    RichText::new(attention_label(actor).unwrap_or("ATTENTION"))
                        .monospace()
                        .strong()
                        .color(palette.warning),
                );
            }
        });
        ui.horizontal_wrapped(|ui| {
            let path = actor
                .worktree_path
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "Shared source view; no isolated worktree yet".into());
            ui.label(
                RichText::new(compact_text(&path, 140))
                    .small()
                    .color(palette.muted),
            )
            .on_hover_text(&path);
            if actor.worktree_path.is_none() && ui.button("Create isolated worktree").clicked() {
                match self.core.command(Command::WorktreeCreate {
                    worktree_id: Uuid::new_v4(),
                    thread_id: actor.thread_id,
                }) {
                    Ok(_) => self.set_status("Creating and verifying Git worktree…", false),
                    Err(error) => self.set_status(error, true),
                }
            }
        });
    }

    fn timeline(&self, ui: &mut egui::Ui, palette: Palette, max_height: f32) {
        egui::Frame::new()
            .fill(palette.panel)
            .stroke(Stroke::new(1.0, palette.border))
            .corner_radius(8)
            .inner_margin(egui::Margin::same(8))
            .show(ui, |ui| {
                egui::ScrollArea::vertical()
                    .id_salt("timeline")
                    .max_height(max_height)
                    .auto_shrink([false, false])
                    .stick_to_bottom(true)
                    .show(ui, |ui| {
                        if self.timeline.is_empty() {
                            ui.label(
                                RichText::new("No durable messages for this operator yet.")
                                    .color(palette.muted),
                            );
                        }
                        for message in &self.timeline {
                            egui::Frame::new()
                                .fill(palette.raised)
                                .corner_radius(6)
                                .inner_margin(egui::Margin::symmetric(9, 7))
                                .show(ui, |ui| {
                                    ui.label(
                                        RichText::new(format!(
                                            "{} · event #{}",
                                            message.role.to_uppercase(),
                                            message.sequence
                                        ))
                                        .monospace()
                                        .small()
                                        .color(palette.muted),
                                    );
                                    ui.add(egui::Label::new(&message.body).wrap());
                                });
                            ui.add_space(5.0);
                        }
                    });
            });
    }

    fn prompt_composer(
        &mut self,
        ui: &mut egui::Ui,
        palette: Palette,
        actor: &ThreadActorSnapshot,
    ) {
        ui.label(RichText::new("Prompt draft").strong());
        let draft = self.drafts.entry(actor.thread_id).or_default();
        ui.add(
            egui::TextEdit::multiline(draft)
                .id(prompt_id())
                .desired_rows(3)
                .desired_width(f32::INFINITY)
                .hint_text(format!("Write a prompt for {}", actor.label)),
        );
        ui.horizontal_wrapped(|ui| {
            let has_draft = self
                .drafts
                .get(&actor.thread_id)
                .is_some_and(|draft| !draft.trim().is_empty());
            let prompt_pending = prompt_save_pending(&self.pending_prompt_saves, actor.thread_id);
            let save_label = if prompt_pending {
                "Saving prompt…"
            } else {
                "Save prompt  Ctrl+Enter"
            };
            if ui
                .add_enabled(has_draft && !prompt_pending, egui::Button::new(save_label))
                .clicked()
            {
                self.save_prompt();
            }
            let interruptible = actor.state.is_interruptible();
            let interrupt = ui.add_enabled(
                interruptible,
                egui::Button::new("Request interrupt  Ctrl+."),
            );
            if interrupt.clicked() {
                self.request_interrupt();
            }
            if !interruptible {
                interrupt.on_disabled_hover_text("No interruptible work is recorded");
            }
        });
        ui.label(
            RichText::new(
                "This build saves prompts locally. It does not start a model turn. Live streaming, approvals, resume, and fork remain gated.",
            )
            .small()
            .color(palette.muted),
        );
    }

    fn provider_readiness(&mut self, ui: &mut egui::Ui, palette: Palette) {
        egui::CollapsingHeader::new("Installed-provider readiness")
            .id_salt("provider_readiness")
            .show(ui, |ui| {
                ui.label(
                    RichText::new(
                        "Checks CLI protocol surfaces only. It sends no prompt and starts no paid model turn.",
                    )
                    .small()
                    .color(palette.muted),
                );
                if self.probe_rx.is_none()
                    && ui.button("Check installed provider surfaces").clicked()
                {
                    let (tx, rx) = mpsc::channel();
                    thread::spawn(move || {
                        let _ = tx.send(providers::probe_all());
                    });
                    self.probe_rx = Some(rx);
                    self.set_status("Checking installed CLIs without a model turn…", false);
                }
                if self.probe_rx.is_some() {
                    ui.horizontal(|ui| {
                        ui.spinner();
                        ui.label("Provider surface check running");
                    });
                }
                egui::ScrollArea::vertical()
                    .id_salt("provider_results")
                    .max_height(140.0)
                    .show(ui, |ui| {
                        for probe in &self.probes {
                            let ready = probe.error.is_none();
                            ui.collapsing(
                                format!(
                                    "{} · {}",
                                    probe.provider.as_str().to_uppercase(),
                                    if ready { "surface found" } else { "unavailable" }
                                ),
                                |ui| {
                                    if !probe.version.is_empty() {
                                        ui.monospace(&probe.version);
                                    }
                                    for item in &probe.verified_without_model_turn {
                                        ui.label(format!("Verified · {item}"));
                                    }
                                    for item in &probe.live_spike_still_required {
                                        ui.label(format!("Still gated · {item}"));
                                    }
                                    if let Some(error) = &probe.error {
                                        ui.label(RichText::new(error).color(palette.danger));
                                    }
                                },
                            );
                        }
                    });
            });
    }
}

impl eframe::App for AgentWorldApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        let palette = Palette::agent_world();
        self.poll();
        self.handle_shortcuts(&ctx);

        let metrics = LayoutMetrics::for_viewport(ui.available_size());
        self.header(ui, palette);
        self.status_bar(ui, palette);
        self.operator_panel(ui, palette, metrics.operator_panel_width);
        self.workspace(ui, palette);

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

fn configure_style(context: &egui::Context) {
    let palette = Palette::agent_world();
    let mut style = (*context.style_of(egui::Theme::Dark)).clone();
    style.spacing.item_spacing = Vec2::new(8.0, 7.0);
    style.spacing.button_padding = Vec2::new(10.0, 6.0);
    style.spacing.interact_size.y = 30.0;
    style
        .text_styles
        .insert(TextStyle::Heading, FontId::proportional(21.0));
    style
        .text_styles
        .insert(TextStyle::Body, FontId::proportional(14.0));
    style
        .text_styles
        .insert(TextStyle::Button, FontId::proportional(13.5));
    style
        .text_styles
        .insert(TextStyle::Small, FontId::proportional(12.0));
    style
        .text_styles
        .insert(TextStyle::Monospace, FontId::monospace(12.0));

    style.visuals = egui::Visuals::dark();
    style.visuals.panel_fill = palette.canvas;
    style.visuals.window_fill = palette.panel;
    style.visuals.extreme_bg_color = palette.canvas;
    style.visuals.faint_bg_color = palette.raised;
    style.visuals.override_text_color = Some(palette.text);
    style.visuals.selection.bg_fill = palette.accent.gamma_multiply(0.22);
    style.visuals.selection.stroke = Stroke::new(2.0, palette.accent);
    style.visuals.widgets.inactive.bg_fill = palette.raised;
    style.visuals.widgets.inactive.bg_stroke = Stroke::new(1.0, palette.border);
    style.visuals.widgets.hovered.bg_fill = palette.selected;
    style.visuals.widgets.hovered.bg_stroke = Stroke::new(1.0, palette.accent);
    style.visuals.widgets.active.bg_fill = palette.selected;
    style.visuals.widgets.active.bg_stroke = Stroke::new(2.0, palette.accent);
    style.visuals.widgets.open.bg_fill = palette.selected;
    style.visuals.widgets.open.bg_stroke = Stroke::new(1.0, palette.accent);
    style.visuals.hyperlink_color = palette.active;
    style.visuals.warn_fg_color = palette.warning;
    style.visuals.error_fg_color = palette.danger;
    context.set_style_of(egui::Theme::Dark, style);
    context.set_theme(egui::Theme::Dark);
}

fn prompt_id() -> egui::Id {
    egui::Id::new("agent_world_prompt")
}

fn summary_label(ui: &mut egui::Ui, color: Color32, count: usize, label: &str) {
    ui.label(
        RichText::new(format!("● {count} {label}"))
            .strong()
            .color(color),
    );
}

fn provider_color(provider: Provider, palette: Palette) -> Color32 {
    match provider {
        Provider::Codex => palette.codex,
        Provider::Claude => palette.claude,
    }
}

fn state_color(state: ActorState, palette: Palette) -> Color32 {
    match state {
        ActorState::AwaitingApproval => palette.warning,
        ActorState::WaitingUser => palette.accent,
        ActorState::Failed => palette.danger,
        ActorState::Running | ActorState::Starting | ActorState::Interrupting => palette.active,
        _ => palette.muted,
    }
}

fn state_label(state: ActorState) -> &'static str {
    match state {
        ActorState::AwaitingApproval => "APPROVAL",
        ActorState::WaitingUser => "PROMPT SAVED",
        ActorState::Failed => "FAILED",
        ActorState::Interrupting => "STOPPING",
        ActorState::Starting => "STARTING",
        ActorState::Running => "RUNNING",
        ActorState::Archived => "ARCHIVED",
        ActorState::Stopped => "STOPPED",
        ActorState::Idle => "IDLE",
    }
}

fn is_running(state: ActorState) -> bool {
    matches!(
        state,
        ActorState::Starting | ActorState::Running | ActorState::Interrupting
    )
}

fn running_count(actors: &[ThreadActorSnapshot]) -> usize {
    actors
        .iter()
        .filter(|actor| is_running(actor.state))
        .count()
}

fn needs_attention(actor: &ThreadActorSnapshot) -> bool {
    attention_label(actor).is_some()
}

fn attention_label(actor: &ThreadActorSnapshot) -> Option<&'static str> {
    if actor.attention || actor.state == ActorState::AwaitingApproval {
        Some("NEEDS YOU")
    } else if actor.unread_count > 0 {
        Some("UNREAD")
    } else {
        None
    }
}

fn prompt_save_pending(
    pending_prompt_saves: &HashMap<Uuid, PendingPromptSave>,
    thread_id: Uuid,
) -> bool {
    pending_prompt_saves
        .values()
        .any(|pending| pending.thread_id == thread_id)
}

fn reconcile_prompt_receipt(
    drafts: &mut HashMap<Uuid, String>,
    pending_prompt_saves: &mut HashMap<Uuid, PendingPromptSave>,
    command_id: Uuid,
    status: &str,
) {
    if !matches!(status, "succeeded" | "rejected" | "indeterminate") {
        return;
    }
    let Some(pending) = pending_prompt_saves.remove(&command_id) else {
        return;
    };
    if status == "succeeded"
        && drafts
            .get(&pending.thread_id)
            .is_some_and(|draft| draft == &pending.draft)
    {
        drafts.remove(&pending.thread_id);
    }
}

fn discard_pending_prompt_save(
    pending_prompt_saves: &mut HashMap<Uuid, PendingPromptSave>,
    command_id: Uuid,
) {
    pending_prompt_saves.remove(&command_id);
}

fn shortcut_is_enabled(action: ShortcutAction, widget_focused: bool) -> bool {
    action != ShortcutAction::FocusPrompt || !widget_focused
}

fn should_update_status(has_persistent_error: bool, incoming_is_error: bool) -> bool {
    !has_persistent_error || incoming_is_error
}

fn operator_order(actors: &[ThreadActorSnapshot]) -> Vec<usize> {
    let mut order: Vec<_> = (0..actors.len()).collect();
    order.sort_by(|left, right| {
        operator_priority(&actors[*left])
            .cmp(&operator_priority(&actors[*right]))
            .then_with(|| actors[*left].label.cmp(&actors[*right].label))
            .then_with(|| actors[*left].thread_id.cmp(&actors[*right].thread_id))
    });
    order
}

fn operator_priority(actor: &ThreadActorSnapshot) -> u8 {
    if needs_attention(actor) {
        0
    } else if is_running(actor.state) {
        1
    } else if actor.state == ActorState::Failed {
        2
    } else {
        3
    }
}

fn compact_text(value: &str, max_chars: usize) -> String {
    let count = value.chars().count();
    if count <= max_chars {
        return value.to_owned();
    }
    if max_chars < 5 {
        return value.chars().take(max_chars).collect();
    }
    let head = (max_chars - 1) / 2;
    let tail = max_chars - head - 1;
    let start: String = value.chars().take(head).collect();
    let end: String = value
        .chars()
        .rev()
        .take(tail)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("{start}…{end}")
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actor(index: usize, state: ActorState, attention: bool) -> ThreadActorSnapshot {
        ThreadActorSnapshot {
            thread_id: Uuid::from_u128(index as u128 + 1),
            project_id: Uuid::from_u128(1_000 + index as u128),
            worktree_id: None,
            provider: if index.is_multiple_of(2) {
                Provider::Codex
            } else {
                Provider::Claude
            },
            label: format!("Actor {:02}", index + 1),
            state,
            attention,
            unread_count: 0,
            last_event_sequence: index as u64,
            worktree_path: None,
        }
    }

    fn app_with_draft(
        thread_id: Uuid,
        draft: &str,
    ) -> (AgentWorldApp, Receiver<CoreInput>, mpsc::Sender<CoreEvent>) {
        let (input_tx, input_rx) = mpsc::sync_channel(8);
        let (event_tx, event_rx) = mpsc::channel();
        (
            AgentWorldApp {
                core: CoreHandle {
                    tx: input_tx,
                    rx: event_rx,
                },
                actors: vec![],
                selected: Some(thread_id),
                timeline: vec![],
                drafts: HashMap::from([(thread_id, draft.to_owned())]),
                pending_prompt_saves: HashMap::new(),
                status: String::new(),
                status_is_error: false,
                preserve_status_on_bootstrap: false,
                persistent_error: None,
                last_sequence: 0,
                probe_rx: None,
                probes: vec![],
            },
            input_rx,
            event_tx,
        )
    }

    #[test]
    fn keymap_preserves_platform_tab_and_declares_required_shortcuts() {
        assert!(!SHORTCUTS.iter().any(|binding| binding.key == Key::Tab));
        assert!(SHORTCUTS.contains(&ShortcutBinding {
            key: Key::F6,
            ctrl: false,
            action: ShortcutAction::CycleAttention,
        }));
        assert!(SHORTCUTS.contains(&ShortcutBinding {
            key: Key::Enter,
            ctrl: false,
            action: ShortcutAction::FocusPrompt,
        }));
        assert!(SHORTCUTS.contains(&ShortcutBinding {
            key: Key::Enter,
            ctrl: true,
            action: ShortcutAction::SavePrompt,
        }));
        assert!(SHORTCUTS.contains(&ShortcutBinding {
            key: Key::Period,
            ctrl: true,
            action: ShortcutAction::Interrupt,
        }));
    }

    #[test]
    fn readme_controls_match_the_registered_ui_contract() {
        let readme = include_str!("../README.md");
        assert!(readme.contains(README_CONTROLS));
        assert!(!readme.contains("| `Tab` | Cycle operators requiring attention |"));
    }

    #[test]
    fn responsive_metrics_are_stable_in_logical_points() {
        let minimum = LayoutMetrics::for_viewport(Vec2::new(900.0, 560.0));
        let default = LayoutMetrics::for_viewport(Vec2::new(1240.0, 760.0));
        let same_logical_size_at_200_percent =
            LayoutMetrics::for_viewport(Vec2::new(1800.0, 1120.0) / 2.0);
        assert_eq!(minimum, same_logical_size_at_200_percent);
        assert_eq!(minimum.operator_panel_width, 252.0);
        assert_eq!(default.operator_panel_width, 328.0);
        assert_eq!(LayoutMetrics::timeline_height(345.0), 120.0);
        assert_eq!(LayoutMetrics::timeline_height(800.0), 360.0);
    }

    #[test]
    fn all_fifty_operators_remain_in_the_focusable_list_order() {
        let mut actors: Vec<_> = (0..50)
            .map(|index| actor(index, ActorState::Idle, false))
            .collect();
        actors[37].attention = true;
        actors[22].state = ActorState::Running;
        let order = operator_order(&actors);
        let mut unique = order.clone();
        unique.sort_unstable();
        assert_eq!(order.len(), 50);
        assert_eq!(unique, (0..50).collect::<Vec<_>>());
        assert_eq!(order[0], 37);
        assert_eq!(order[1], 22);
    }

    #[test]
    fn state_helpers_keep_attention_and_interrupt_actions_honest() {
        let saved = actor(0, ActorState::WaitingUser, false);
        let approval = actor(1, ActorState::AwaitingApproval, false);
        let idle = actor(2, ActorState::Idle, false);
        let mut unread = actor(3, ActorState::Idle, false);
        unread.unread_count = 4;
        assert_eq!(state_label(saved.state), "PROMPT SAVED");
        assert!(!needs_attention(&saved));
        assert!(needs_attention(&approval));
        assert!(needs_attention(&unread));
        assert_eq!(attention_label(&unread), Some("UNREAD"));
        assert!(!saved.state.is_interruptible());
        assert!(approval.state.is_interruptible());
        assert!(!idle.state.is_interruptible());
    }

    #[test]
    fn prompt_draft_clears_only_for_its_matching_succeeded_receipt() {
        let thread_id = Uuid::new_v4();
        let command_id = Uuid::new_v4();
        let unrelated_command_id = Uuid::new_v4();
        let mut drafts = HashMap::from([(thread_id, "  durable prompt  ".to_owned())]);
        let pending = PendingPromptSave {
            thread_id,
            draft: drafts[&thread_id].clone(),
        };
        let mut pending_prompt_saves = HashMap::from([(command_id, pending.clone())]);

        reconcile_prompt_receipt(
            &mut drafts,
            &mut pending_prompt_saves,
            unrelated_command_id,
            "succeeded",
        );
        assert_eq!(drafts[&thread_id], "  durable prompt  ");
        assert_eq!(pending_prompt_saves.get(&command_id), Some(&pending));

        reconcile_prompt_receipt(
            &mut drafts,
            &mut pending_prompt_saves,
            command_id,
            "accepted",
        );
        assert_eq!(pending_prompt_saves.get(&command_id), Some(&pending));

        reconcile_prompt_receipt(
            &mut drafts,
            &mut pending_prompt_saves,
            command_id,
            "rejected",
        );
        assert_eq!(drafts[&thread_id], "  durable prompt  ");
        assert!(!pending_prompt_saves.contains_key(&command_id));

        let indeterminate_command_id = Uuid::new_v4();
        pending_prompt_saves.insert(indeterminate_command_id, pending.clone());
        reconcile_prompt_receipt(
            &mut drafts,
            &mut pending_prompt_saves,
            indeterminate_command_id,
            "indeterminate",
        );
        assert_eq!(drafts[&thread_id], "  durable prompt  ");
        assert!(!pending_prompt_saves.contains_key(&indeterminate_command_id));

        let succeeded_command_id = Uuid::new_v4();
        pending_prompt_saves.insert(succeeded_command_id, pending);
        reconcile_prompt_receipt(
            &mut drafts,
            &mut pending_prompt_saves,
            succeeded_command_id,
            "succeeded",
        );
        assert!(!drafts.contains_key(&thread_id));
        assert!(!pending_prompt_saves.contains_key(&succeeded_command_id));
    }

    #[test]
    fn succeeded_prompt_receipt_preserves_a_newer_edit() {
        let thread_id = Uuid::new_v4();
        let command_id = Uuid::new_v4();
        let mut drafts = HashMap::from([(thread_id, "newer edit".to_owned())]);
        let mut pending_prompt_saves = HashMap::from([(
            command_id,
            PendingPromptSave {
                thread_id,
                draft: "submitted draft".into(),
            },
        )]);

        reconcile_prompt_receipt(
            &mut drafts,
            &mut pending_prompt_saves,
            command_id,
            "succeeded",
        );

        assert_eq!(drafts[&thread_id], "newer edit");
        assert!(!pending_prompt_saves.contains_key(&command_id));
    }

    #[test]
    fn prompt_save_waits_for_receipt_blocks_duplicates_and_recovers_from_execution_error() {
        let thread_id = Uuid::new_v4();
        let (mut app, input_rx, event_tx) = app_with_draft(thread_id, "  retry me  ");

        app.save_prompt();
        let envelope = match input_rx.try_recv().expect("prompt command queued") {
            CoreInput::Execute(envelope) => envelope,
            _ => panic!("prompt save queued a non-command input"),
        };
        match &envelope.command {
            Command::TurnSend {
                thread_id: queued_thread_id,
                text,
            } => {
                assert_eq!(*queued_thread_id, thread_id);
                assert_eq!(text, "retry me");
            }
            _ => panic!("prompt save queued the wrong command"),
        }
        assert_eq!(app.drafts[&thread_id], "  retry me  ");
        assert!(prompt_save_pending(&app.pending_prompt_saves, thread_id));
        assert_eq!(
            app.pending_prompt_saves[&envelope.command_id].draft,
            "  retry me  "
        );

        app.save_prompt();
        assert!(matches!(
            input_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        event_tx
            .send(CoreEvent::CommandError {
                command_id: envelope.command_id,
                error: "database unavailable".into(),
            })
            .expect("send command error");
        app.poll();
        assert!(!prompt_save_pending(&app.pending_prompt_saves, thread_id));
        assert_eq!(app.drafts[&thread_id], "  retry me  ");
        assert_eq!(
            app.persistent_error.as_deref(),
            Some("database unavailable")
        );
    }

    #[test]
    fn focused_widget_keeps_plain_enter_and_number_input() {
        assert!(!shortcut_is_enabled(ShortcutAction::FocusPrompt, true));
        assert!(shortcut_is_enabled(ShortcutAction::SavePrompt, true));
        assert!(shortcut_is_enabled(ShortcutAction::Interrupt, true));
        assert!(shortcut_is_enabled(ShortcutAction::CycleAttention, true));
    }

    #[test]
    fn long_paths_and_errors_are_compacted_without_losing_both_ends() {
        let path = format!("C:/repo/{}/important.rs", "nested/".repeat(80));
        let error = format!(
            "recovery failed: {}: inspect manually",
            "detail ".repeat(100)
        );
        let compact_path = compact_text(&path, 80);
        let compact_error = compact_text(&error, 120);
        assert_eq!(compact_path.chars().count(), 80);
        assert_eq!(compact_error.chars().count(), 120);
        assert!(compact_path.starts_with("C:/repo/"));
        assert!(compact_path.ends_with("important.rs"));
        assert!(compact_error.starts_with("recovery failed:"));
        assert!(compact_error.ends_with("inspect manually"));
        assert!(compact_path.contains('…'));
        assert!(compact_error.contains('…'));
    }

    #[test]
    fn recovery_error_has_precedence_over_routine_bootstrap_status() {
        assert!(!should_update_status(true, false));
        assert!(should_update_status(true, true));
        assert!(should_update_status(false, false));
    }
}
