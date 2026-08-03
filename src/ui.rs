use eframe::egui::{
    self, Align, Color32, FontId, Key, RichText, Stroke, TextStyle, Vec2, WidgetInfo, WidgetType,
};
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
        ActorState, BootstrapSnapshot, Command, CoreEvent, CoreHandle, CoreInput, InteractionKind,
        InteractionSnapshot, InteractionStatus, LiveTurnStartGate, Provider, RecoveryDisposition,
        ThreadActorSnapshot, TimelineMessage, TimelineRecordKind,
    },
    live_turn::{ApprovalDecision, LiveTurnState, ProviderRunner, UserInputAnswer},
    providers::{self, ProviderProbe},
};

const OPERATOR_ROW_HEIGHT: f32 = 52.0;
const TIMELINE_LIMIT: usize = 100;
#[cfg(test)]
const README_CONTROLS: &str = "| Click or `1`–`9` | Select an operator (`1`–`9` only when no control has focus) |\n\
| `Enter` | Focus the prompt when no control has focus |\n\
| `Ctrl+Enter` | Start a live Codex turn when the selected operator is durably eligible |\n\
| `Ctrl+.` | Interrupt the selected turn only when its durable projection is interruptible |\n\
| `F6` | Cycle to and focus each outstanding approval, input request, or other operator attention state |\n\
| `Tab` / `Shift+Tab` | Move keyboard focus through every control and operator |";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ShortcutAction {
    FocusPrompt,
    StartTurn,
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
        action: ShortcutAction::StartTurn,
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

#[derive(Clone, Debug, PartialEq, Eq)]
enum FocusRequest {
    Composer(Uuid),
    Operator(Uuid),
    Interaction {
        thread_id: Uuid,
        interaction_id: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct InputDraftKey {
    thread_id: Uuid,
    interaction_id: String,
    question_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingInteractionResponse {
    thread_id: Uuid,
    interaction_id: String,
    kind: InteractionKind,
    submitted_answers: Vec<(InputDraftKey, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AttentionTarget {
    thread_id: Uuid,
    interaction_id: Option<String>,
}

pub struct AgentWorldApp {
    core: CoreHandle,
    actors: Vec<ThreadActorSnapshot>,
    selected: Option<Uuid>,
    timeline: Vec<TimelineMessage>,
    drafts: HashMap<Uuid, String>,
    pending_prompt_saves: HashMap<Uuid, PendingPromptSave>,
    input_drafts: HashMap<InputDraftKey, String>,
    pending_interaction_responses: HashMap<Uuid, PendingInteractionResponse>,
    interaction_feedback: HashMap<(Uuid, String), String>,
    focus_request: Option<FocusRequest>,
    status: String,
    status_is_error: bool,
    preserve_status_on_bootstrap: bool,
    persistent_error: Option<String>,
    last_sequence: u64,
    probe_rx: Option<Receiver<Vec<ProviderProbe>>>,
    probes: Vec<ProviderProbe>,
}

impl AgentWorldApp {
    pub fn new(
        runtime_root: PathBuf,
        context: egui::Context,
        provider_runner: Box<dyn ProviderRunner>,
    ) -> Result<Self, String> {
        configure_style(&context);
        let repaint_context = context.clone();
        let core = CoreHandle::spawn(
            runtime_root,
            move || repaint_context.request_repaint(),
            provider_runner,
        )?;
        core.tx.try_send(CoreInput::Bootstrap).map_err(err)?;
        Ok(Self {
            core,
            actors: vec![],
            selected: None,
            timeline: vec![],
            drafts: HashMap::new(),
            pending_prompt_saves: HashMap::new(),
            input_drafts: HashMap::new(),
            pending_interaction_responses: HashMap::new(),
            interaction_feedback: HashMap::new(),
            focus_request: None,
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
                    let refocus_thread = reconcile_prompt_receipt(
                        &mut self.drafts,
                        &mut self.pending_prompt_saves,
                        receipt.command_id,
                        &receipt.status,
                    );
                    if let Some(thread_id) = refocus_thread {
                        self.select_actor(thread_id);
                        self.focus_request = Some(FocusRequest::Composer(thread_id));
                    }
                    if let Some((thread_id, interaction_id)) = reconcile_interaction_receipt(
                        &mut self.input_drafts,
                        &mut self.pending_interaction_responses,
                        &mut self.interaction_feedback,
                        receipt.command_id,
                        &receipt.status,
                    ) {
                        self.select_actor(thread_id);
                        self.focus_request = Some(FocusRequest::Interaction {
                            thread_id,
                            interaction_id,
                        });
                    }
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
                    if let Some(thread_id) =
                        discard_pending_prompt_save(&mut self.pending_prompt_saves, command_id)
                    {
                        self.select_actor(thread_id);
                        self.focus_request = Some(FocusRequest::Composer(thread_id));
                    }
                    if let Some(pending) = self.pending_interaction_responses.remove(&command_id) {
                        self.interaction_feedback.insert(
                            (pending.thread_id, pending.interaction_id.clone()),
                            format!("Response was not recorded: {error}"),
                        );
                        self.select_actor(pending.thread_id);
                        self.focus_request = Some(FocusRequest::Interaction {
                            thread_id: pending.thread_id,
                            interaction_id: pending.interaction_id,
                        });
                    }
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
                        self.timeline = bounded_timeline(messages, TIMELINE_LIMIT);
                    }
                }
                CoreEvent::TurnChanged { thread_id, status } => {
                    self.set_status(
                        match status.as_str() {
                            "running" => {
                            "Codex is running in the isolated worktree under the configured workspace-write sandbox; authority beyond that boundary requires approval".to_owned()
                            }
                            "completed" => "Codex response recorded · review requested".to_owned(),
                            "failed" => {
                                "Codex turn failed · details recorded in the timeline".to_owned()
                            }
                            "indeterminate" => {
                                "Codex turn outcome unknown · it was not replayed".to_owned()
                            }
                            _ => format!("Codex turn · {status}"),
                        },
                        matches!(status.as_str(), "failed" | "indeterminate"),
                    );
                    if self.selected == Some(thread_id) {
                        self.request_timeline();
                    }
                    refresh = true;
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
                limit: TIMELINE_LIMIT,
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

    fn start_turn(&mut self) {
        let Some(actor) = self.selected_actor().cloned() else {
            self.set_status("Select an operator before running a prompt", true);
            return;
        };
        let thread_id = actor.thread_id;
        if let Some(blocker) = turn_start_blocker(&self.actors, &actor) {
            self.set_status(blocker, true);
            return;
        }
        if prompt_save_pending(&self.pending_prompt_saves, thread_id) {
            self.set_status(
                "A Codex turn is already being admitted for this operator",
                true,
            );
            return;
        }
        let Some(draft) = self.drafts.get(&thread_id).cloned() else {
            return;
        };
        let text = draft.trim().to_owned();
        if text.is_empty() {
            self.set_status("Write a prompt before starting a turn", true);
            return;
        }
        match self.core.command(Command::LiveTurnStart {
            turn_id: Uuid::new_v4(),
            thread_id,
            text,
        }) {
            Ok(command_id) => {
                self.pending_prompt_saves
                    .insert(command_id, PendingPromptSave { thread_id, draft });
                self.set_status("Recording the turn before Codex starts…", false);
            }
            Err(error) => {
                self.focus_request = Some(FocusRequest::Composer(thread_id));
                self.set_status(error, true);
            }
        }
    }

    fn request_interrupt(&mut self) {
        let Some(actor) = self.selected_actor().cloned() else {
            return;
        };
        let Some(turn) = actor.live_turn.as_ref() else {
            self.set_status(
                format!("{} has no live turn to interrupt", actor.label),
                true,
            );
            return;
        };
        if !turn.interruptible {
            self.set_status(
                format!(
                    "{} is {}; its durable projection is not interruptible",
                    actor.label,
                    live_turn_state_label(turn.state).to_lowercase()
                ),
                true,
            );
            return;
        }
        match self.core.command(Command::LiveTurnInterrupt {
            turn_id: turn.turn_id,
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
                ShortcutAction::StartTurn => self.start_turn(),
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
        let attention = attention_targets(&self.actors);
        if attention.is_empty() {
            self.set_status("No operators currently need attention", false);
            return;
        }
        let next = attention
            .iter()
            .position(|target| Some(target.thread_id) == self.selected)
            .map(|index| (index + 1) % attention.len())
            .unwrap_or(0);
        let target = attention[next].clone();
        self.select_actor(target.thread_id);
        self.focus_request = Some(match target.interaction_id {
            Some(interaction_id) => FocusRequest::Interaction {
                thread_id: target.thread_id,
                interaction_id,
            },
            None => FocusRequest::Operator(target.thread_id),
        });
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
                        RichText::new("F6 attention · Enter prompt · Ctrl+Enter run")
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
                                actor_state_label(&actor),
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
                            let response = ui
                                .push_id(actor.thread_id, |ui| {
                                    ui.add_sized(
                                        [ui.available_width(), OPERATOR_ROW_HEIGHT],
                                        button,
                                    )
                                })
                                .inner;
                            let accessible_label = operator_accessible_label(&actor);
                            response.widget_info(|| {
                                WidgetInfo::selected(
                                    WidgetType::Button,
                                    true,
                                    selected,
                                    accessible_label.clone(),
                                )
                            });
                            if self.focus_request == Some(FocusRequest::Operator(actor.thread_id)) {
                                response.request_focus();
                                response.scroll_to_me(Some(Align::Center));
                                self.focus_request = None;
                            }
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
                        if self.recovery_notice(ui, palette, &actor) {
                            ui.add_space(8.0);
                        }
                        if let Some(turn) = actor.live_turn.as_ref()
                            && let Some(interaction) = turn.interaction.as_ref()
                        {
                            self.interaction_card(ui, palette, &actor, turn.turn_id, interaction);
                            ui.add_space(8.0);
                        }
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
            let heading = ui.label(RichText::new(&actor.label).heading().strong());
            let accessible_label = operator_accessible_label(actor);
            heading.widget_info(|| {
                WidgetInfo::labeled(WidgetType::Label, true, accessible_label.clone())
            });
            ui.label(
                RichText::new(actor.provider.as_str().to_uppercase())
                    .monospace()
                    .strong()
                    .color(provider_color(actor.provider, palette)),
            );
            ui.label(
                RichText::new(actor_state_label(actor))
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
            if actor.worktree_path.is_none() {
                let create = ui.button("Create isolated worktree");
                let accessible_label = format!(
                    "{} is {}. Create and verify an isolated worktree before starting a turn.",
                    actor.label,
                    actor_state_label(actor)
                );
                create.widget_info(|| {
                    WidgetInfo::labeled(WidgetType::Button, true, accessible_label.clone())
                });
                if create.clicked() {
                    match self.core.command(Command::WorktreeCreate {
                        worktree_id: Uuid::new_v4(),
                        thread_id: actor.thread_id,
                    }) {
                        Ok(_) => self.set_status("Creating and verifying Git worktree…", false),
                        Err(error) => self.set_status(error, true),
                    }
                }
            }
        });
    }

    fn recovery_notice(
        &self,
        ui: &mut egui::Ui,
        palette: Palette,
        actor: &ThreadActorSnapshot,
    ) -> bool {
        let recovery = actor
            .live_turn
            .as_ref()
            .map_or(RecoveryDisposition::None, |turn| turn.recovery);
        let (message, color) = match recovery {
            RecoveryDisposition::Resumed => (
                format!(
                    "{} RESUMED after restart from its durable provider cursor. New output and interactions remain committed before they appear here.",
                    actor.label
                ),
                palette.active,
            ),
            RecoveryDisposition::Completed => (
                format!(
                    "{} is COMPLETED. Its terminal record is committed in the durable timeline.",
                    actor.label
                ),
                palette.accent,
            ),
            RecoveryDisposition::Failed => (
                format!(
                    "{} is FAILED. The durable timeline contains the recorded failure; the turn will not be retried automatically.",
                    actor.label
                ),
                palette.danger,
            ),
            RecoveryDisposition::Indeterminate => (
                format!(
                    "{} is INDETERMINATE. Agent World cannot prove the live turn's outcome after recovery, did not replay it, and will not retry it automatically. Inspect the committed timeline before starting any new work.",
                    actor.label
                ),
                palette.danger,
            ),
            RecoveryDisposition::None => return false,
        };
        egui::Frame::new()
            .fill(color.gamma_multiply(0.12))
            .stroke(Stroke::new(2.0, color))
            .corner_radius(6)
            .inner_margin(egui::Margin::symmetric(10, 8))
            .show(ui, |ui| {
                let response = ui.add(
                    egui::Label::new(RichText::new(&message).strong().color(palette.text)).wrap(),
                );
                response
                    .widget_info(|| WidgetInfo::labeled(WidgetType::Label, true, message.clone()));
            });
        true
    }

    fn interaction_card(
        &mut self,
        ui: &mut egui::Ui,
        palette: Palette,
        actor: &ThreadActorSnapshot,
        turn_id: Uuid,
        interaction: &InteractionSnapshot,
    ) {
        let pending = interaction_response_pending(
            &self.pending_interaction_responses,
            actor.thread_id,
            &interaction.interaction_id,
        );
        let focus_requested = interaction_focus_requested(
            self.focus_request.as_ref(),
            actor.thread_id,
            &interaction.interaction_id,
        );
        let mut focus_was_applied = false;
        let mut approval_decision = None;
        let mut submit_answers = false;
        let is_pending = interaction.status == InteractionStatus::Pending;
        let is_stale = interaction.status == InteractionStatus::Stale;
        let is_responded = interaction.status == InteractionStatus::Responded;
        let controls_enabled = is_pending && !pending;
        let title = match interaction.kind {
            InteractionKind::Approval => "Approval required",
            InteractionKind::UserInput => "Input required",
        };
        let outline = if is_stale {
            palette.danger
        } else if is_responded {
            palette.muted
        } else {
            palette.warning
        };

        egui::Frame::new()
            .fill(outline.gamma_multiply(0.10))
            .stroke(Stroke::new(2.0, outline))
            .corner_radius(8)
            .inner_margin(egui::Margin::symmetric(12, 10))
            .show(ui, |ui| {
                let status = if is_stale {
                    "STALE"
                } else if is_responded {
                    "RESPONDED"
                } else if pending {
                    "RECORDING RESPONSE"
                } else {
                    "NEEDS YOU"
                };
                let heading = ui.label(
                    RichText::new(format!("{title} · {status}"))
                        .strong()
                        .color(outline),
                );
                let accessible_heading = format!(
                    "Operator {}. State {}. {}. {}",
                    actor.label,
                    actor_state_label(actor),
                    title,
                    if is_stale {
                        "This request is stale; no response can be sent."
                    } else if is_responded {
                        "A response has already been durably recorded; no further response can be sent."
                    } else if pending {
                        "Wait for the durable response receipt."
                    } else {
                        "Complete the controls in this inline card."
                    }
                );
                heading.widget_info(|| {
                    WidgetInfo::labeled(WidgetType::Label, true, accessible_heading.clone())
                });
                ui.add(egui::Label::new(&interaction.prompt).wrap());

                if is_stale {
                    ui.label(
                        RichText::new(
                            "Response rejected: this request is stale. No provider action was requested.",
                        )
                        .strong()
                        .color(palette.danger),
                    );
                } else if is_responded {
                    ui.label(
                        RichText::new(
                            "Response recorded. Provider delivery follows the durable receipt; this request cannot be answered again.",
                        )
                        .small()
                        .color(palette.muted),
                    );
                }
                if let Some(feedback) = self
                    .interaction_feedback
                    .get(&(actor.thread_id, interaction.interaction_id.clone()))
                {
                    ui.label(RichText::new(feedback).strong().color(palette.danger));
                }

                match interaction.kind {
                    InteractionKind::Approval => {
                        interaction_metadata(ui, palette, "Operation", &interaction.operation);
                        interaction_metadata(ui, palette, "Path", &interaction.path);
                        interaction_metadata(ui, palette, "Command", &interaction.command);
                        interaction_metadata(
                            ui,
                            palette,
                            "Consequence",
                            &interaction.consequence,
                        );
                        if interaction.consequence.is_none() {
                            ui.label(
                                RichText::new(
                                    "Allow once permits only this requested operation; Deny refuses it. No permanent policy is created.",
                                )
                                .small()
                                .color(palette.muted),
                            );
                        }
                        ui.add_space(4.0);
                        ui.horizontal_wrapped(|ui| {
                            let enabled = controls_enabled;
                            let allow = ui.add_enabled(enabled, egui::Button::new("Allow once"));
                            let allow_label = format!(
                                "{} is {}. Allow this operation once; no permanent policy is created.",
                                actor.label,
                                actor_state_label(actor)
                            );
                            allow.widget_info(|| {
                                WidgetInfo::labeled(
                                    WidgetType::Button,
                                    enabled,
                                    allow_label.clone(),
                                )
                            });
                            if focus_requested && enabled {
                                allow.request_focus();
                                allow.scroll_to_me(Some(Align::Center));
                                focus_was_applied = true;
                            }
                            if allow.clicked() {
                                approval_decision = Some(ApprovalDecision::Approve);
                            }

                            let deny = ui.add_enabled(enabled, egui::Button::new("Deny"));
                            let deny_label = format!(
                                "{} is {}. Deny this requested operation.",
                                actor.label,
                                actor_state_label(actor)
                            );
                            deny.widget_info(|| {
                                WidgetInfo::labeled(
                                    WidgetType::Button,
                                    enabled,
                                    deny_label.clone(),
                                )
                            });
                            if deny.clicked() {
                                approval_decision = Some(ApprovalDecision::Deny);
                            }
                        });
                    }
                    InteractionKind::UserInput => {
                        for (index, question) in interaction.questions.iter().enumerate() {
                            let key = InputDraftKey {
                                thread_id: actor.thread_id,
                                interaction_id: interaction.interaction_id.clone(),
                                question_id: question.question_id.clone(),
                            };
                            let label = ui.label(RichText::new(&question.prompt).strong());
                            let draft = self.input_drafts.entry(key).or_default();
                            let answer = ui
                                .add_enabled(
                                    controls_enabled,
                                    egui::TextEdit::multiline(draft)
                                        .id(interaction_input_id(
                                            &interaction.interaction_id,
                                            &question.question_id,
                                        ))
                                        .desired_rows(3)
                                        .desired_width(f32::INFINITY)
                                        .hint_text("Type a multiline answer"),
                                )
                                .labelled_by(label.id);
                            if focus_requested && index == 0 && controls_enabled {
                                answer.request_focus();
                                answer.scroll_to_me(Some(Align::Center));
                                focus_was_applied = true;
                            }
                        }
                        let submit = ui.add_enabled(
                            controls_enabled && !interaction.questions.is_empty(),
                            egui::Button::new(if pending {
                                "Recording answers…"
                            } else {
                                "Submit answers"
                            }),
                        );
                        let submit_label = format!(
                            "{} is {}. Submit all answers and wait for a durable receipt.",
                            actor.label,
                            actor_state_label(actor)
                        );
                        submit.widget_info(|| {
                            WidgetInfo::labeled(
                                WidgetType::Button,
                                controls_enabled && !interaction.questions.is_empty(),
                                submit_label.clone(),
                            )
                        });
                        submit_answers = submit.clicked();
                    }
                }
            });

        if focus_was_applied {
            self.focus_request = None;
        }
        if let Some(decision) = approval_decision {
            self.respond_to_approval(actor, turn_id, interaction, decision);
        }
        if submit_answers {
            self.respond_to_user_input(actor, turn_id, interaction);
        }
    }

    fn respond_to_approval(
        &mut self,
        actor: &ThreadActorSnapshot,
        turn_id: Uuid,
        interaction: &InteractionSnapshot,
        decision: ApprovalDecision,
    ) {
        if interaction.status != InteractionStatus::Pending {
            self.reject_inactive_interaction(actor, interaction);
            return;
        }
        let command = Command::ApprovalRespond {
            turn_id,
            thread_id: actor.thread_id,
            interaction_id: interaction.interaction_id.clone(),
            decision,
        };
        match self.core.command(command) {
            Ok(command_id) => {
                self.pending_interaction_responses.insert(
                    command_id,
                    PendingInteractionResponse {
                        thread_id: actor.thread_id,
                        interaction_id: interaction.interaction_id.clone(),
                        kind: InteractionKind::Approval,
                        submitted_answers: Vec::new(),
                    },
                );
                self.interaction_feedback
                    .remove(&(actor.thread_id, interaction.interaction_id.clone()));
                self.set_status(
                    "Recording the approval response before provider delivery…",
                    false,
                );
            }
            Err(error) => {
                self.interaction_feedback.insert(
                    (actor.thread_id, interaction.interaction_id.clone()),
                    format!("Response was not recorded: {error}"),
                );
                self.focus_request = Some(FocusRequest::Interaction {
                    thread_id: actor.thread_id,
                    interaction_id: interaction.interaction_id.clone(),
                });
                self.set_status(error, true);
            }
        }
    }

    fn respond_to_user_input(
        &mut self,
        actor: &ThreadActorSnapshot,
        turn_id: Uuid,
        interaction: &InteractionSnapshot,
    ) {
        if interaction.status != InteractionStatus::Pending {
            self.reject_inactive_interaction(actor, interaction);
            return;
        }
        let submitted_answers: Vec<_> = interaction
            .questions
            .iter()
            .map(|question| {
                let key = InputDraftKey {
                    thread_id: actor.thread_id,
                    interaction_id: interaction.interaction_id.clone(),
                    question_id: question.question_id.clone(),
                };
                let draft = self.input_drafts.get(&key).cloned().unwrap_or_default();
                (key, draft)
            })
            .collect();
        let answers = submitted_answers
            .iter()
            .map(|(key, answer)| UserInputAnswer {
                question_id: key.question_id.clone(),
                answer: answer.clone(),
            })
            .collect();
        let command = Command::UserInputRespond {
            turn_id,
            thread_id: actor.thread_id,
            interaction_id: interaction.interaction_id.clone(),
            answers,
        };
        match self.core.command(command) {
            Ok(command_id) => {
                self.pending_interaction_responses.insert(
                    command_id,
                    PendingInteractionResponse {
                        thread_id: actor.thread_id,
                        interaction_id: interaction.interaction_id.clone(),
                        kind: InteractionKind::UserInput,
                        submitted_answers,
                    },
                );
                self.interaction_feedback
                    .remove(&(actor.thread_id, interaction.interaction_id.clone()));
                self.set_status("Recording all answers before provider delivery…", false);
            }
            Err(error) => {
                self.interaction_feedback.insert(
                    (actor.thread_id, interaction.interaction_id.clone()),
                    format!("Answers were not recorded: {error}"),
                );
                self.focus_request = Some(FocusRequest::Interaction {
                    thread_id: actor.thread_id,
                    interaction_id: interaction.interaction_id.clone(),
                });
                self.set_status(error, true);
            }
        }
    }

    fn reject_inactive_interaction(
        &mut self,
        actor: &ThreadActorSnapshot,
        interaction: &InteractionSnapshot,
    ) {
        let (feedback, status, is_error) = match interaction.status {
            InteractionStatus::Stale => (
                "Response rejected: this request is stale. No provider action was requested.",
                "Response rejected · request is stale",
                true,
            ),
            InteractionStatus::Responded => (
                "Response not sent: this request already has a durable response. No provider action was requested.",
                "Response not sent · request already responded",
                false,
            ),
            InteractionStatus::Pending => return,
        };
        self.interaction_feedback.insert(
            (actor.thread_id, interaction.interaction_id.clone()),
            feedback.into(),
        );
        self.set_status(status, is_error);
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
                            let record_label = timeline_record_label(message);
                            let record_color = timeline_record_color(message, palette);
                            egui::Frame::new()
                                .fill(palette.raised)
                                .corner_radius(6)
                                .inner_margin(egui::Margin::symmetric(9, 7))
                                .show(ui, |ui| {
                                    ui.label(
                                        RichText::new(format!(
                                            "{} · committed event #{} · {}",
                                            record_label, message.sequence, message.event_type
                                        ))
                                        .monospace()
                                        .small()
                                        .color(record_color),
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
        let composer_label = ui.label(
            RichText::new(format!(
                "Start Codex turn for {} · {}",
                actor.label,
                actor_state_label(actor)
            ))
            .strong(),
        );
        let start_blocker = turn_start_blocker(&self.actors, actor);
        let draft = self.drafts.entry(actor.thread_id).or_default();
        let composer = ui
            .add(
                egui::TextEdit::multiline(draft)
                    .id(prompt_id())
                    .desired_rows(3)
                    .desired_width(f32::INFINITY)
                    .hint_text(format!(
                        "Prompt for {}. Ctrl+Enter starts the turn when available.",
                        actor.label
                    )),
            )
            .labelled_by(composer_label.id);
        if self.focus_request == Some(FocusRequest::Composer(actor.thread_id)) {
            composer.request_focus();
            composer.scroll_to_me(Some(Align::Center));
            self.focus_request = None;
        }
        ui.horizontal_wrapped(|ui| {
            let has_draft = self
                .drafts
                .get(&actor.thread_id)
                .is_some_and(|draft| !draft.trim().is_empty());
            let prompt_pending = prompt_save_pending(&self.pending_prompt_saves, actor.thread_id);
            let save_label = if prompt_pending {
                "Recording turn…"
            } else if actor.provider == Provider::Claude {
                "Claude live turns gated"
            } else if actor.worktree_path.is_none() {
                "Create isolated worktree first"
            } else if start_blocker.is_some() {
                "Start Codex turn unavailable"
            } else {
                "Start Codex turn  Ctrl+Enter"
            };
            let run = ui.add_enabled(
                has_draft && !prompt_pending && start_blocker.is_none(),
                egui::Button::new(save_label),
            );
            if run.clicked() {
                self.start_turn();
            }
            if let Some(blocker) = &start_blocker {
                run.on_disabled_hover_text(blocker);
            }
            let interruptible = actor
                .live_turn
                .as_ref()
                .is_some_and(|turn| turn.interruptible);
            let interrupt = ui.add_enabled(
                interruptible,
                egui::Button::new("Request interrupt  Ctrl+."),
            );
            if interrupt.clicked() {
                self.request_interrupt();
            }
            if !interruptible {
                interrupt.on_disabled_hover_text(
                    "The durable core does not mark this turn interruptible",
                );
            }
        });
        if let Some(blocker) = &start_blocker {
            ui.label(
                RichText::new(format!("Cannot start Codex turn: {blocker}"))
                    .small()
                    .color(palette.warning),
            );
        }
        if let Some(turn) = actor.live_turn.as_ref()
            && !turn.interruptible
            && turn.state.is_active()
        {
            ui.label(
                RichText::new(format!(
                    "Interrupt unavailable: {} is not marked interruptible by the durable core.",
                    live_turn_state_label(turn.state)
                ))
                .small()
                .color(palette.muted),
            );
        }
        ui.label(
            RichText::new(
                "Codex runs from the verified isolated worktree under the configured workspace-write sandbox with network disabled. Operations needing authority beyond that sandbox use explicit on-request approval. Committed coalesced output appears in the durable timeline; approval and user-input requests stay inline until a durable response receipt succeeds. Interrupt is available only when the durable live-turn projection permits it, and restart recovery never auto-retries an indeterminate turn. External Windows enforcement evidence, Claude live turns, and fork remain gated.",
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

    fn render(&mut self, ui: &mut egui::Ui) {
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

impl eframe::App for AgentWorldApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.render(ui);
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

fn interaction_input_id(interaction_id: &str, question_id: &str) -> egui::Id {
    egui::Id::new(("interaction_input", interaction_id, question_id))
}

fn interaction_focus_requested(
    focus_request: Option<&FocusRequest>,
    thread_id: Uuid,
    interaction_id: &str,
) -> bool {
    matches!(
        focus_request,
        Some(FocusRequest::Interaction {
            thread_id: requested_thread,
            interaction_id: requested_interaction,
        }) if *requested_thread == thread_id && requested_interaction == interaction_id
    )
}

fn interaction_response_pending(
    pending: &HashMap<Uuid, PendingInteractionResponse>,
    thread_id: Uuid,
    interaction_id: &str,
) -> bool {
    pending.values().any(|response| {
        response.thread_id == thread_id && response.interaction_id == interaction_id
    })
}

fn interaction_metadata(ui: &mut egui::Ui, palette: Palette, label: &str, value: &Option<String>) {
    let Some(value) = value else {
        return;
    };
    ui.horizontal_wrapped(|ui| {
        ui.label(
            RichText::new(format!("{label}:"))
                .monospace()
                .strong()
                .color(palette.muted),
        );
        ui.label(RichText::new(compact_text(value, 240)).monospace())
            .on_hover_text(value);
    });
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
        ActorState::Failed | ActorState::Indeterminate => palette.danger,
        ActorState::Running | ActorState::Starting | ActorState::Interrupting => palette.active,
        _ => palette.muted,
    }
}

fn timeline_record_label(message: &TimelineMessage) -> &'static str {
    match message.kind {
        TimelineRecordKind::User => "USER",
        TimelineRecordKind::Assistant => "ASSISTANT",
        TimelineRecordKind::System => "SYSTEM",
        TimelineRecordKind::ApprovalRequest => "APPROVAL",
        TimelineRecordKind::UserInputRequest => "USER INPUT",
        TimelineRecordKind::Status if message.event_type.contains("interrupt") => "INTERRUPT",
        TimelineRecordKind::Status if message.event_type.contains("failed") => "FAILURE",
        TimelineRecordKind::Status
            if message.event_type.contains("indeterminate")
                || message.event_type.contains("resumed")
                || message.event_type.contains("recovery") =>
        {
            "RECOVERY"
        }
        TimelineRecordKind::Status => "STATUS",
    }
}

fn timeline_record_color(message: &TimelineMessage, palette: Palette) -> Color32 {
    match timeline_record_label(message) {
        "APPROVAL" | "USER INPUT" => palette.warning,
        "FAILURE" | "RECOVERY" => palette.danger,
        "INTERRUPT" => palette.active,
        "USER" => palette.accent,
        "ASSISTANT" => palette.text,
        _ => palette.muted,
    }
}

fn state_label(state: ActorState) -> &'static str {
    match state {
        ActorState::AwaitingApproval => "NEEDS APPROVAL",
        ActorState::WaitingUser => "NEEDS INPUT",
        ActorState::Failed => "FAILED",
        ActorState::Indeterminate => "INDETERMINATE",
        ActorState::Interrupting => "STOPPING",
        ActorState::Starting => "STARTING",
        ActorState::Running => "RUNNING",
        ActorState::Archived => "ARCHIVED",
        ActorState::Stopped => "COMPLETED",
        ActorState::Idle => "IDLE",
    }
}

fn live_turn_state_label(state: LiveTurnState) -> &'static str {
    match state {
        LiveTurnState::Accepted | LiveTurnState::Starting => "STARTING",
        LiveTurnState::Streaming => "RUNNING",
        LiveTurnState::AwaitingApproval => "NEEDS APPROVAL",
        LiveTurnState::AwaitingUserInput => "NEEDS INPUT",
        LiveTurnState::Interrupting => "STOPPING",
        LiveTurnState::Completed => "COMPLETED",
        LiveTurnState::Failed => "FAILED",
        LiveTurnState::Indeterminate => "INDETERMINATE",
    }
}

fn actor_state_label(actor: &ThreadActorSnapshot) -> &'static str {
    actor.live_turn.as_ref().map_or_else(
        || state_label(actor.state),
        |turn| live_turn_state_label(turn.state),
    )
}

fn operator_accessible_label(actor: &ThreadActorSnapshot) -> String {
    let required_action = match actor.state {
        ActorState::AwaitingApproval => {
            "Review the pending approval and choose Allow once or Deny."
        }
        ActorState::WaitingUser => "Answer the pending provider question.",
        ActorState::Indeterminate => {
            "Inspect the durable timeline; Agent World will not retry automatically."
        }
        ActorState::Failed => "Inspect the durable failure before starting new work.",
        ActorState::Starting | ActorState::Running => {
            "Watch the committed timeline or request interrupt when enabled."
        }
        ActorState::Interrupting => "Wait for the durable interrupt acknowledgement.",
        ActorState::Idle | ActorState::Stopped if actor.worktree_path.is_none() => {
            "Create and verify an isolated worktree."
        }
        ActorState::Idle | ActorState::Stopped => "Enter a prompt to start a Codex turn.",
        ActorState::Archived => "No action is available for this archived operator.",
    };
    format!(
        "Operator {}. Provider {}. State {}. {}",
        actor.label,
        actor.provider.as_str(),
        actor_state_label(actor),
        required_action
    )
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
    let has_pending_interaction = actor
        .live_turn
        .as_ref()
        .and_then(|turn| turn.interaction.as_ref())
        .is_some_and(|interaction| interaction.status == InteractionStatus::Pending);
    if has_pending_interaction
        || actor.attention
        || matches!(
            actor.state,
            ActorState::AwaitingApproval | ActorState::WaitingUser
        )
    {
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
) -> Option<Uuid> {
    if !matches!(status, "succeeded" | "rejected" | "indeterminate") {
        return None;
    }
    let pending = pending_prompt_saves.remove(&command_id)?;
    if status == "succeeded"
        && drafts
            .get(&pending.thread_id)
            .is_some_and(|draft| draft == &pending.draft)
    {
        drafts.remove(&pending.thread_id);
    }
    (status != "succeeded").then_some(pending.thread_id)
}

fn discard_pending_prompt_save(
    pending_prompt_saves: &mut HashMap<Uuid, PendingPromptSave>,
    command_id: Uuid,
) -> Option<Uuid> {
    pending_prompt_saves
        .remove(&command_id)
        .map(|pending| pending.thread_id)
}

fn reconcile_interaction_receipt(
    input_drafts: &mut HashMap<InputDraftKey, String>,
    pending: &mut HashMap<Uuid, PendingInteractionResponse>,
    feedback: &mut HashMap<(Uuid, String), String>,
    command_id: Uuid,
    status: &str,
) -> Option<(Uuid, String)> {
    if !matches!(status, "succeeded" | "rejected" | "indeterminate") {
        return None;
    }
    let response = pending.remove(&command_id)?;
    let feedback_key = (response.thread_id, response.interaction_id.clone());
    if status == "succeeded" {
        for (draft_key, submitted) in response.submitted_answers {
            if input_drafts
                .get(&draft_key)
                .is_some_and(|current| current == &submitted)
            {
                input_drafts.remove(&draft_key);
            }
        }
        feedback.remove(&feedback_key);
        None
    } else {
        let response_name = match response.kind {
            InteractionKind::Approval => "Approval response",
            InteractionKind::UserInput => "Input response",
        };
        feedback.insert(
            feedback_key,
            format!(
                "{response_name} rejected because the request is stale, already resolved, or could not be durably reconciled. Your draft was preserved."
            ),
        );
        Some((response.thread_id, response.interaction_id))
    }
}

fn bounded_timeline(mut messages: Vec<TimelineMessage>, limit: usize) -> Vec<TimelineMessage> {
    messages.sort_by_key(|message| message.sequence);
    messages.dedup_by_key(|message| message.sequence);
    let remove = messages.len().saturating_sub(limit);
    messages.drain(0..remove);
    messages
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

fn attention_targets(actors: &[ThreadActorSnapshot]) -> Vec<AttentionTarget> {
    operator_order(actors)
        .into_iter()
        .filter(|index| needs_attention(&actors[*index]))
        .map(|index| {
            let actor = &actors[index];
            let interaction_id = actor
                .live_turn
                .as_ref()
                .and_then(|turn| turn.interaction.as_ref())
                .filter(|interaction| interaction.status == InteractionStatus::Pending)
                .map(|interaction| interaction.interaction_id.clone());
            AttentionTarget {
                thread_id: actor.thread_id,
                interaction_id,
            }
        })
        .collect()
}

fn operator_priority(actor: &ThreadActorSnapshot) -> u8 {
    if needs_attention(actor) {
        0
    } else if is_running(actor.state) {
        1
    } else if matches!(actor.state, ActorState::Failed | ActorState::Indeterminate) {
        2
    } else {
        3
    }
}

fn turn_start_blocker(
    _actors: &[ThreadActorSnapshot],
    actor: &ThreadActorSnapshot,
) -> Option<String> {
    match actor.start_gate {
        LiveTurnStartGate::Eligible => None,
        LiveTurnStartGate::NoWorktree => Some("no verified isolated worktree is attached".into()),
        LiveTurnStartGate::ProviderUnavailable => {
            Some("the selected provider is unavailable for live turns".into())
        }
        LiveTurnStartGate::UnsupportedVersion => {
            Some("the installed provider version is unsupported".into())
        }
        LiveTurnStartGate::PendingTurn => Some("a live turn is already pending or active".into()),
        LiveTurnStartGate::RecoveryError => {
            Some("migration or recovery state requires attention".into())
        }
        LiveTurnStartGate::QueuePressure => {
            Some("the bounded core command queue is under pressure".into())
        }
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
            start_gate: LiveTurnStartGate::NoWorktree,
            live_turn: None,
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
                core: CoreHandle::from_channels(input_tx, event_rx),
                actors: vec![ThreadActorSnapshot {
                    thread_id,
                    project_id: Uuid::new_v4(),
                    worktree_id: Some(Uuid::new_v4()),
                    provider: Provider::Codex,
                    label: "Codex fixture".into(),
                    state: ActorState::Idle,
                    attention: false,
                    unread_count: 0,
                    last_event_sequence: 0,
                    worktree_path: Some(PathBuf::from("C:/fixture")),
                    start_gate: LiveTurnStartGate::Eligible,
                    live_turn: None,
                }],
                selected: Some(thread_id),
                timeline: vec![],
                drafts: HashMap::from([(thread_id, draft.to_owned())]),
                pending_prompt_saves: HashMap::new(),
                input_drafts: HashMap::new(),
                pending_interaction_responses: HashMap::new(),
                interaction_feedback: HashMap::new(),
                focus_request: None,
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

    fn interaction(
        id: &str,
        kind: InteractionKind,
        status: InteractionStatus,
    ) -> InteractionSnapshot {
        InteractionSnapshot {
            interaction_id: id.into(),
            kind,
            prompt: "Provider needs an operator response".into(),
            operation: (kind == InteractionKind::Approval).then(|| "run command".into()),
            path: (kind == InteractionKind::Approval).then(|| "C:/fixture".into()),
            command: (kind == InteractionKind::Approval).then(|| "cargo test".into()),
            consequence: (kind == InteractionKind::Approval)
                .then(|| "Runs the command once in the verified worktree".into()),
            questions: if kind == InteractionKind::UserInput {
                vec![crate::live_turn::UserInputQuestion {
                    question_id: "scope".into(),
                    prompt: "Which scope should be tested?".into(),
                }]
            } else {
                Vec::new()
            },
            status,
        }
    }

    fn live_turn(
        turn_id: Uuid,
        state: LiveTurnState,
        interaction: Option<InteractionSnapshot>,
        interruptible: bool,
    ) -> crate::core::LiveTurnSnapshot {
        crate::core::LiveTurnSnapshot {
            turn_id,
            state,
            session: None,
            interruptible,
            interaction,
            recovery: RecoveryDisposition::None,
        }
    }

    fn raw_input(events: Vec<egui::Event>, modifiers: egui::Modifiers) -> egui::RawInput {
        egui::RawInput {
            screen_rect: Some(egui::Rect::from_min_size(
                egui::Pos2::ZERO,
                Vec2::new(1240.0, 760.0),
            )),
            modifiers,
            events,
            ..Default::default()
        }
    }

    fn key_input(key: Key, modifiers: egui::Modifiers) -> egui::RawInput {
        let event = |pressed| egui::Event::Key {
            key,
            physical_key: Some(key),
            pressed,
            repeat: false,
            modifiers,
        };
        raw_input(vec![event(true), event(false)], modifiers)
    }

    fn render_test_frame(app: &mut AgentWorldApp, context: &egui::Context, input: egui::RawInput) {
        let _ = context.run_ui(input, |ui| app.render(ui));
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
            action: ShortcutAction::StartTurn,
        }));
        assert!(SHORTCUTS.contains(&ShortcutBinding {
            key: Key::Period,
            ctrl: true,
            action: ShortcutAction::Interrupt,
        }));
    }

    #[test]
    fn readme_controls_match_the_registered_ui_contract() {
        let readme = include_str!("../README.md").replace("\r\n", "\n");
        assert!(readme.contains(README_CONTROLS));
        assert!(!readme.contains("| `Tab` | Cycle operators requiring attention |"));
    }

    #[test]
    fn responsive_metrics_are_stable_in_logical_points() {
        let minimum = LayoutMetrics::for_viewport(Vec2::new(900.0, 560.0));
        let default = LayoutMetrics::for_viewport(Vec2::new(1240.0, 760.0));
        for scale in [1.25, 1.5, 2.0] {
            let physical_size = Vec2::new(900.0, 560.0) * scale;
            let same_logical_size = LayoutMetrics::for_viewport(physical_size / scale);
            assert_eq!(minimum, same_logical_size);
        }
        assert_eq!(minimum.operator_panel_width, 252.0);
        assert_eq!(default.operator_panel_width, 328.0);
        assert_eq!(LayoutMetrics::timeline_height(345.0), 120.0);
        assert_eq!(LayoutMetrics::timeline_height(800.0), 360.0);
    }

    #[test]
    fn minimum_window_accesskit_tree_contains_the_active_keyboard_controls() {
        let thread_id = Uuid::new_v4();
        let turn_id = Uuid::new_v4();
        let (mut app, _, _) = app_with_draft(thread_id, "review the repository");
        app.actors[0].state = ActorState::AwaitingApproval;
        app.actors[0].attention = true;
        app.actors[0].start_gate = LiveTurnStartGate::PendingTurn;
        app.actors[0].live_turn = Some(live_turn(
            turn_id,
            LiveTurnState::AwaitingApproval,
            Some(interaction(
                "approval-accessibility",
                InteractionKind::Approval,
                InteractionStatus::Pending,
            )),
            true,
        ));

        let context = egui::Context::default();
        configure_style(&context);
        context.enable_accesskit();
        let output = context.run_ui(
            egui::RawInput {
                screen_rect: Some(egui::Rect::from_min_size(
                    egui::Pos2::ZERO,
                    Vec2::new(900.0, 560.0),
                )),
                ..Default::default()
            },
            |ui| app.render(ui),
        );
        let update = output
            .platform_output
            .accesskit_update
            .expect("AccessKit tree update at the minimum window");
        let accessible_text: Vec<_> = update
            .nodes
            .iter()
            .filter_map(|(_, node)| node.label().or_else(|| node.value()))
            .collect();
        assert!(accessible_text.iter().any(|label| {
            label.contains("Codex fixture")
                && label.contains("NEEDS APPROVAL")
                && label.contains("Allow once or Deny")
        }));
        assert!(accessible_text.iter().any(|label| {
            label.contains("Allow this operation once") && label.contains("no permanent policy")
        }));
        assert!(
            accessible_text
                .iter()
                .any(|label| label.contains("Deny this requested operation"))
        );
        assert!(
            accessible_text
                .iter()
                .any(|label| label.contains("Request interrupt"))
        );
    }

    #[test]
    fn scaled_minimum_window_accesskit_geometry_keeps_required_actions_reachable() {
        for scale in [1.25_f32, 1.5, 2.0] {
            let thread_id = Uuid::new_v4();
            let turn_id = Uuid::new_v4();
            let (mut app, input_rx, _) = app_with_draft(thread_id, "review the repository");
            app.actors[0].state = ActorState::AwaitingApproval;
            app.actors[0].attention = true;
            app.actors[0].start_gate = LiveTurnStartGate::PendingTurn;
            app.actors[0].live_turn = Some(live_turn(
                turn_id,
                LiveTurnState::AwaitingApproval,
                Some(interaction(
                    "approval-scaled",
                    InteractionKind::Approval,
                    InteractionStatus::Pending,
                )),
                true,
            ));

            let context = egui::Context::default();
            configure_style(&context);
            context.set_pixels_per_point(scale);
            context.enable_accesskit();
            let output = context.run_ui(
                egui::RawInput {
                    screen_rect: Some(egui::Rect::from_min_size(
                        egui::Pos2::ZERO,
                        Vec2::new(900.0, 560.0),
                    )),
                    ..Default::default()
                },
                |ui| app.render(ui),
            );
            let update = output
                .platform_output
                .accesskit_update
                .expect("scaled AccessKit tree update");
            let physical_width = 900.0_f64 * f64::from(scale);
            let physical_height = 560.0_f64 * f64::from(scale);
            for expected in [
                "Allow this operation once",
                "Deny this requested operation",
                "Request interrupt",
            ] {
                let node = update
                    .nodes
                    .iter()
                    .find_map(|(_, node)| {
                        node.label()
                            .is_some_and(|label| label.contains(expected))
                            .then_some(node)
                    })
                    .unwrap_or_else(|| panic!("missing {expected:?} at {scale}x"));
                let bounds = node
                    .bounds()
                    .unwrap_or_else(|| panic!("missing bounds for {expected:?} at {scale}x"));
                assert!(bounds.x1 > bounds.x0 && bounds.y1 > bounds.y0);
                assert!(bounds.x0 >= 0.0 && bounds.y0 >= 0.0);
                assert!(
                    bounds.x1 <= physical_width,
                    "{expected:?} is horizontally clipped at {scale}x: {bounds:?} outside {physical_width}x{physical_height}"
                );
                if expected != "Request interrupt" {
                    assert!(
                        bounds.y1 <= physical_height,
                        "{expected:?} is vertically clipped at {scale}x: {bounds:?} outside {physical_width}x{physical_height}"
                    );
                }
            }
            // The interrupt control follows the expanded interaction card in scroll content at
            // the minimum height. Its documented shortcut must remain reachable without a
            // pointer or a scroll gesture at every scale.
            render_test_frame(
                &mut app,
                &context,
                key_input(Key::Period, egui::Modifiers::CTRL),
            );
            let interrupt = match input_rx.try_recv() {
                Ok(CoreInput::Execute(envelope)) => envelope,
                _ => panic!("scaled interrupt shortcut was unreachable"),
            };
            assert!(matches!(
                interrupt.command,
                Command::LiveTurnInterrupt {
                    turn_id: queued_turn,
                    thread_id: queued_thread,
                } if queued_turn == turn_id && queued_thread == thread_id
            ));
        }
    }

    #[test]
    fn keyboard_can_deny_the_exact_pending_approval() {
        let thread_id = Uuid::new_v4();
        let turn_id = Uuid::new_v4();
        let (mut app, input_rx, _) = app_with_draft(thread_id, "review the repository");
        app.actors[0].state = ActorState::AwaitingApproval;
        app.actors[0].attention = true;
        app.actors[0].start_gate = LiveTurnStartGate::PendingTurn;
        app.actors[0].live_turn = Some(live_turn(
            turn_id,
            LiveTurnState::AwaitingApproval,
            Some(interaction(
                "approval-deny-keyboard",
                InteractionKind::Approval,
                InteractionStatus::Pending,
            )),
            true,
        ));
        let context = egui::Context::default();
        configure_style(&context);
        render_test_frame(
            &mut app,
            &context,
            key_input(Key::F6, egui::Modifiers::NONE),
        );
        render_test_frame(
            &mut app,
            &context,
            key_input(Key::Tab, egui::Modifiers::NONE),
        );
        render_test_frame(
            &mut app,
            &context,
            key_input(Key::Space, egui::Modifiers::NONE),
        );
        let denial = match input_rx.try_recv().expect("keyboard denial command") {
            CoreInput::Execute(envelope) => envelope,
            _ => panic!("denial queued a non-command input"),
        };
        assert!(matches!(
            denial.command,
            Command::ApprovalRespond {
                turn_id: queued_turn,
                thread_id: queued_thread,
                ref interaction_id,
                decision: ApprovalDecision::Deny,
            } if queued_turn == turn_id
                && queued_thread == thread_id
                && interaction_id == "approval-deny-keyboard"
        ));
    }

    #[test]
    fn keyboard_only_flow_starts_answers_requests_and_interrupts() {
        let thread_id = Uuid::new_v4();
        let turn_id = Uuid::new_v4();
        let (mut app, input_rx, _) = app_with_draft(thread_id, "review the repository");
        let context = egui::Context::default();
        configure_style(&context);

        render_test_frame(
            &mut app,
            &context,
            key_input(Key::Enter, egui::Modifiers::CTRL),
        );
        let start = match input_rx.try_recv().expect("Ctrl+Enter queues a live turn") {
            CoreInput::Execute(envelope) => envelope,
            _ => panic!("Ctrl+Enter queued a non-command input"),
        };
        assert!(matches!(
            start.command,
            Command::LiveTurnStart {
                thread_id: queued_thread,
                ref text,
                ..
            } if queued_thread == thread_id && text == "review the repository"
        ));

        app.actors[0].state = ActorState::AwaitingApproval;
        app.actors[0].attention = false;
        app.actors[0].start_gate = LiveTurnStartGate::PendingTurn;
        app.actors[0].live_turn = Some(live_turn(
            turn_id,
            LiveTurnState::AwaitingApproval,
            Some(interaction(
                "keyboard-approval",
                InteractionKind::Approval,
                InteractionStatus::Pending,
            )),
            true,
        ));
        render_test_frame(
            &mut app,
            &context,
            key_input(Key::F6, egui::Modifiers::NONE),
        );
        assert!(context.memory(|memory| memory.focused()).is_some());
        render_test_frame(
            &mut app,
            &context,
            key_input(Key::Space, egui::Modifiers::NONE),
        );
        let approval = match input_rx
            .try_recv()
            .expect("Space activates the F6-focused approval")
        {
            CoreInput::Execute(envelope) => envelope,
            _ => panic!("approval queued a non-command input"),
        };
        assert!(matches!(
            approval.command,
            Command::ApprovalRespond {
                turn_id: queued_turn,
                thread_id: queued_thread,
                ref interaction_id,
                decision: ApprovalDecision::Approve,
            } if queued_turn == turn_id
                && queued_thread == thread_id
                && interaction_id == "keyboard-approval"
        ));

        app.actors[0].state = ActorState::WaitingUser;
        app.actors[0].live_turn = Some(live_turn(
            turn_id,
            LiveTurnState::AwaitingUserInput,
            Some(interaction(
                "keyboard-input",
                InteractionKind::UserInput,
                InteractionStatus::Pending,
            )),
            true,
        ));
        render_test_frame(
            &mut app,
            &context,
            key_input(Key::F6, egui::Modifiers::NONE),
        );
        render_test_frame(
            &mut app,
            &context,
            raw_input(
                vec![egui::Event::Text("workspace\nand integration".into())],
                egui::Modifiers::NONE,
            ),
        );
        render_test_frame(
            &mut app,
            &context,
            key_input(Key::Tab, egui::Modifiers::NONE),
        );
        render_test_frame(
            &mut app,
            &context,
            key_input(Key::Space, egui::Modifiers::NONE),
        );
        let input = match input_rx
            .try_recv()
            .expect("Tab and Space submit the keyboard-entered answers")
        {
            CoreInput::Execute(envelope) => envelope,
            _ => panic!("input response queued a non-command input"),
        };
        assert!(matches!(
            input.command,
            Command::UserInputRespond {
                turn_id: queued_turn,
                thread_id: queued_thread,
                ref interaction_id,
                ref answers,
            } if queued_turn == turn_id
                && queued_thread == thread_id
                && interaction_id == "keyboard-input"
                && answers.len() == 1
                && answers[0].answer == "workspace\nand integration"
        ));

        app.actors[0].state = ActorState::Running;
        app.actors[0].live_turn = Some(live_turn(turn_id, LiveTurnState::Streaming, None, true));
        render_test_frame(
            &mut app,
            &context,
            key_input(Key::Period, egui::Modifiers::CTRL),
        );
        let interrupt = match input_rx
            .try_recv()
            .expect("Ctrl+. queues an interrupt for the durable live turn")
        {
            CoreInput::Execute(envelope) => envelope,
            _ => panic!("interrupt queued a non-command input"),
        };
        assert!(matches!(
            interrupt.command,
            Command::LiveTurnInterrupt {
                turn_id: queued_turn,
                thread_id: queued_thread,
            } if queued_turn == turn_id && queued_thread == thread_id
        ));
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
        let saved = actor(0, ActorState::WaitingUser, true);
        let approval = actor(1, ActorState::AwaitingApproval, false);
        let idle = actor(2, ActorState::Idle, false);
        let mut unread = actor(3, ActorState::Idle, false);
        unread.unread_count = 4;
        assert_eq!(state_label(saved.state), "NEEDS INPUT");
        assert!(needs_attention(&saved));
        assert!(needs_attention(&approval));
        assert!(needs_attention(&unread));
        assert_eq!(attention_label(&unread), Some("UNREAD"));
        assert!(saved.live_turn.is_none());
        assert!(approval.live_turn.is_none());
        assert!(idle.live_turn.is_none());
    }

    #[test]
    fn live_turn_gate_exposes_each_normalized_durable_reason() {
        let codex = actor(0, ActorState::Idle, false);
        assert!(
            turn_start_blocker(std::slice::from_ref(&codex), &codex)
                .is_some_and(|reason| reason.contains("worktree"))
        );

        let mut claude = actor(1, ActorState::Idle, false);
        claude.start_gate = LiveTurnStartGate::ProviderUnavailable;
        assert!(
            turn_start_blocker(std::slice::from_ref(&claude), &claude)
                .is_some_and(|reason| reason.contains("unavailable"))
        );
        let mut codex = codex;
        codex.worktree_id = Some(Uuid::new_v4());
        codex.worktree_path = Some(PathBuf::from("C:/fixture"));
        codex.start_gate = LiveTurnStartGate::Eligible;
        assert_eq!(
            turn_start_blocker(std::slice::from_ref(&codex), &codex),
            None
        );

        let mut running = actor(3, ActorState::Running, false);
        running.worktree_id = Some(Uuid::new_v4());
        running.worktree_path = Some(PathBuf::from("C:/running"));
        codex.start_gate = LiveTurnStartGate::PendingTurn;
        assert!(
            turn_start_blocker(&[codex.clone(), running], &codex)
                .is_some_and(|reason| reason.contains("pending or active"))
        );

        codex.state = ActorState::AwaitingApproval;
        codex.start_gate = LiveTurnStartGate::PendingTurn;
        assert!(
            turn_start_blocker(std::slice::from_ref(&codex), &codex)
                .is_some_and(|reason| reason.contains("pending or active"))
        );

        codex.state = ActorState::Indeterminate;
        codex.start_gate = LiveTurnStartGate::RecoveryError;
        assert!(
            turn_start_blocker(std::slice::from_ref(&codex), &codex)
                .is_some_and(|reason| reason.contains("recovery"))
        );
        assert_eq!(state_label(codex.state), "INDETERMINATE");

        codex.start_gate = LiveTurnStartGate::UnsupportedVersion;
        assert!(
            turn_start_blocker(std::slice::from_ref(&codex), &codex)
                .is_some_and(|reason| reason.contains("unsupported"))
        );
        codex.start_gate = LiveTurnStartGate::QueuePressure;
        assert!(
            turn_start_blocker(std::slice::from_ref(&codex), &codex)
                .is_some_and(|reason| reason.contains("queue"))
        );
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

        app.start_turn();
        let envelope = match input_rx.try_recv().expect("prompt command queued") {
            CoreInput::Execute(envelope) => envelope,
            _ => panic!("prompt save queued a non-command input"),
        };
        match &envelope.command {
            Command::LiveTurnStart {
                thread_id: queued_thread_id,
                text,
                ..
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

        app.start_turn();
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
        assert_eq!(app.focus_request, Some(FocusRequest::Composer(thread_id)));
        assert_eq!(
            app.persistent_error.as_deref(),
            Some("database unavailable")
        );
    }

    #[test]
    fn user_input_response_preserves_multiline_draft_until_durable_success() {
        let thread_id = Uuid::new_v4();
        let turn_id = Uuid::new_v4();
        let (mut app, input_rx, _) = app_with_draft(thread_id, "unused prompt");
        let actor = app.actors[0].clone();
        let request = interaction(
            "input-1",
            InteractionKind::UserInput,
            InteractionStatus::Pending,
        );
        let draft_key = InputDraftKey {
            thread_id,
            interaction_id: request.interaction_id.clone(),
            question_id: "scope".into(),
        };
        app.input_drafts
            .insert(draft_key.clone(), "workspace\nand integration".into());

        app.respond_to_user_input(&actor, turn_id, &request);
        let envelope = match input_rx.try_recv().expect("input response queued") {
            CoreInput::Execute(envelope) => envelope,
            _ => panic!("input response queued a non-command input"),
        };
        match &envelope.command {
            Command::UserInputRespond {
                turn_id: queued_turn,
                thread_id: queued_thread,
                interaction_id,
                answers,
            } => {
                assert_eq!(*queued_turn, turn_id);
                assert_eq!(*queued_thread, thread_id);
                assert_eq!(interaction_id, "input-1");
                assert_eq!(answers.len(), 1);
                assert_eq!(answers[0].question_id, "scope");
                assert_eq!(answers[0].answer, "workspace\nand integration");
            }
            _ => panic!("input response queued the wrong command"),
        }
        assert_eq!(
            app.input_drafts.get(&draft_key).map(String::as_str),
            Some("workspace\nand integration")
        );

        let refocus = reconcile_interaction_receipt(
            &mut app.input_drafts,
            &mut app.pending_interaction_responses,
            &mut app.interaction_feedback,
            envelope.command_id,
            "rejected",
        );
        assert_eq!(refocus, Some((thread_id, "input-1".into())));
        assert_eq!(
            app.input_drafts.get(&draft_key).map(String::as_str),
            Some("workspace\nand integration")
        );
        assert!(
            app.interaction_feedback[&(thread_id, "input-1".into())]
                .contains("draft was preserved")
        );

        let success_command = Uuid::new_v4();
        app.pending_interaction_responses.insert(
            success_command,
            PendingInteractionResponse {
                thread_id,
                interaction_id: "input-1".into(),
                kind: InteractionKind::UserInput,
                submitted_answers: vec![(draft_key.clone(), "workspace\nand integration".into())],
            },
        );
        assert_eq!(
            reconcile_interaction_receipt(
                &mut app.input_drafts,
                &mut app.pending_interaction_responses,
                &mut app.interaction_feedback,
                success_command,
                "succeeded",
            ),
            None
        );
        assert!(!app.input_drafts.contains_key(&draft_key));
    }

    #[test]
    fn newer_input_edit_survives_success_and_inactive_requests_send_nothing() {
        let thread_id = Uuid::new_v4();
        let turn_id = Uuid::new_v4();
        let (mut app, input_rx, _) = app_with_draft(thread_id, "unused prompt");
        let actor = app.actors[0].clone();
        let stale = interaction(
            "stale-approval",
            InteractionKind::Approval,
            InteractionStatus::Stale,
        );
        app.respond_to_approval(&actor, turn_id, &stale, ApprovalDecision::Approve);
        assert!(matches!(
            input_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        assert!(
            app.interaction_feedback[&(thread_id, "stale-approval".into())]
                .contains("request is stale")
        );

        let responded = interaction(
            "responded-approval",
            InteractionKind::Approval,
            InteractionStatus::Responded,
        );
        app.respond_to_approval(&actor, turn_id, &responded, ApprovalDecision::Approve);
        assert!(matches!(
            input_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        assert!(
            app.interaction_feedback[&(thread_id, "responded-approval".into())]
                .contains("already has a durable response")
        );

        let draft_key = InputDraftKey {
            thread_id,
            interaction_id: "input-2".into(),
            question_id: "scope".into(),
        };
        app.input_drafts
            .insert(draft_key.clone(), "newer edit".into());
        let command_id = Uuid::new_v4();
        app.pending_interaction_responses.insert(
            command_id,
            PendingInteractionResponse {
                thread_id,
                interaction_id: "input-2".into(),
                kind: InteractionKind::UserInput,
                submitted_answers: vec![(draft_key.clone(), "submitted edit".into())],
            },
        );
        reconcile_interaction_receipt(
            &mut app.input_drafts,
            &mut app.pending_interaction_responses,
            &mut app.interaction_feedback,
            command_id,
            "succeeded",
        );
        assert_eq!(app.input_drafts[&draft_key], "newer edit");
    }

    #[test]
    fn interrupt_uses_only_the_durable_turn_id_and_interruptible_flag() {
        let thread_id = Uuid::new_v4();
        let turn_id = Uuid::new_v4();
        let (mut app, input_rx, _) = app_with_draft(thread_id, "unused prompt");
        app.actors[0].state = ActorState::Running;
        app.actors[0].start_gate = LiveTurnStartGate::PendingTurn;
        app.actors[0].live_turn = Some(live_turn(turn_id, LiveTurnState::Streaming, None, false));

        app.request_interrupt();
        assert!(matches!(
            input_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        app.actors[0].live_turn = Some(live_turn(turn_id, LiveTurnState::Streaming, None, true));

        app.request_interrupt();
        let envelope = match input_rx.try_recv().expect("interrupt queued") {
            CoreInput::Execute(envelope) => envelope,
            _ => panic!("interrupt queued a non-command input"),
        };
        assert!(matches!(
            envelope.command,
            Command::LiveTurnInterrupt {
                turn_id: queued_turn,
                thread_id: queued_thread,
            } if queued_turn == turn_id && queued_thread == thread_id
        ));
    }

    #[test]
    fn focused_widget_keeps_plain_enter_and_number_input() {
        assert!(!shortcut_is_enabled(ShortcutAction::FocusPrompt, true));
        assert!(shortcut_is_enabled(ShortcutAction::StartTurn, true));
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

    #[test]
    fn bounded_timeline_keeps_unique_latest_committed_records() {
        let message = |sequence, body: &str| TimelineMessage {
            sequence,
            role: "assistant".into(),
            body: body.into(),
            occurred_at: sequence as i64,
            kind: TimelineRecordKind::Assistant,
            turn_id: None,
            event_type: "turn.output_coalesced".into(),
            metadata: serde_json::Value::Null,
        };
        let page = bounded_timeline(
            vec![
                message(4, "four"),
                message(2, "two"),
                message(3, "three"),
                message(3, "duplicate provider event"),
                message(1, "one"),
            ],
            3,
        );
        assert_eq!(
            page.iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
        assert_eq!(page.len(), 3);
    }

    #[test]
    fn f6_targets_every_attention_operator_in_a_fifty_operator_room() {
        let mut actors: Vec<_> = (0..50)
            .map(|index| actor(index, ActorState::Idle, false))
            .collect();
        for (index, candidate) in actors.iter_mut().enumerate() {
            let (state, kind, questions) = if index % 2 == 0 {
                candidate.state = ActorState::AwaitingApproval;
                (
                    LiveTurnState::AwaitingApproval,
                    InteractionKind::Approval,
                    Vec::new(),
                )
            } else {
                candidate.state = ActorState::WaitingUser;
                (
                    LiveTurnState::AwaitingUserInput,
                    InteractionKind::UserInput,
                    vec![crate::live_turn::UserInputQuestion {
                        question_id: format!("question-{index}"),
                        prompt: "What should happen next?".into(),
                    }],
                )
            };
            candidate.attention = false;
            candidate.start_gate = LiveTurnStartGate::PendingTurn;
            candidate.live_turn = Some(crate::core::LiveTurnSnapshot {
                turn_id: Uuid::from_u128(10_000 + index as u128),
                state,
                session: None,
                interruptible: false,
                interaction: Some(InteractionSnapshot {
                    interaction_id: format!("interaction-{index}"),
                    kind,
                    prompt: "Provider needs an operator response".into(),
                    operation: None,
                    path: None,
                    command: None,
                    consequence: None,
                    questions,
                    status: InteractionStatus::Pending,
                }),
                recovery: RecoveryDisposition::None,
            });
        }
        let targets = attention_targets(&actors);
        assert_eq!(targets.len(), 50);
        assert!(targets.iter().all(|target| target.interaction_id.is_some()));
        let mut unique: Vec<_> = targets.iter().map(|target| target.thread_id).collect();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), 50);

        let (mut app, _, _) = app_with_draft(actors[0].thread_id, "unused prompt");
        app.actors = actors;
        app.selected = None;
        let mut visited = Vec::new();
        for _ in 0..50 {
            app.cycle_attention();
            visited.push(app.selected.expect("F6 selects an attention operator"));
            assert!(matches!(
                app.focus_request.as_ref(),
                Some(FocusRequest::Interaction { .. })
            ));
        }
        visited.sort_unstable();
        visited.dedup();
        assert_eq!(visited.len(), 50);
    }

    #[test]
    fn accesskit_operator_labels_include_state_and_required_action() {
        let approval = actor(0, ActorState::AwaitingApproval, true);
        let approval_label = operator_accessible_label(&approval);
        assert!(approval_label.contains(&approval.label));
        assert!(approval_label.contains("NEEDS APPROVAL"));
        assert!(approval_label.contains("Allow once or Deny"));

        let indeterminate = actor(1, ActorState::Indeterminate, true);
        let indeterminate_label = operator_accessible_label(&indeterminate);
        assert!(indeterminate_label.contains("INDETERMINATE"));
        assert!(indeterminate_label.contains("will not retry automatically"));
    }
}
