//! The seam between the UI and a backend. [`run_ui`] is all a backend needs: it sends
//! [`Command`]s down one channel and listens for [`Event`]s on the other, and never sees a Slint
//! type. The demo backend and (later) `sonic-manager` plug in the same way.
//!
//! [`Controller`] keeps what the backend last said plus the UI's own state (selection, filter,
//! which download is open, dialog forms) and turns both into the UI's models through `view`.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use slint::{ComponentHandle, Model, ModelRc, SharedString, VecModel};
use sonic_core::{
    AppSettings, Command, DownloadId, DownloadState, DownloadStatus, Event, FinishedDownload,
    NetworkId, NetworkInterface, NetworkKind, PendingLink, ProbeResult, Reply, StartRequest,
};
use tokio::sync::{mpsc, oneshot};

use crate::format;
use crate::shell::{self, Shell};
use crate::system;
use crate::traffic::{self, Traffic};
use crate::ui::{self, App, AppWindow, NewDl, Prefs, Theme};
use crate::view::{self, Ctx, Item, Palette};

/// What the real app adds to the window beyond the backend's channels (the demo has neither).
#[derive(Default)]
pub struct Extras {
    /// System mode's supervisor.
    pub system: Option<system::Requests>,
    /// Today's traffic as saved by an earlier run, and how to save it.
    pub traffic: Option<(Option<traffic::Saved>, SaveTraffic)>,
}

pub type SaveTraffic = Box<dyn Fn(&traffic::Saved)>;

impl std::fmt::Debug for Extras {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extras").field("system", &self.system.is_some()).finish()
    }
}

/// Runs the window, talking to a backend only through the two channels, until the user quits:
/// closing the window when it doesn't stay in the tray, or Quit in the tray menu. `shell` brings
/// the tray's clicks, other launches' hand-overs and System mode's news. The caller then shuts
/// the backend (and System mode) down.
pub fn run_ui(
    commands: mpsc::Sender<Command>,
    events: mpsc::Receiver<Event>,
    shell_tx: mpsc::UnboundedSender<Shell>,
    shell_rx: mpsc::UnboundedReceiver<Shell>,
    extras: Extras,
) -> Result<(), slint::PlatformError> {
    let window = AppWindow::new()?;
    let controller = Controller::new(&window, commands);
    {
        let mut c = controller.borrow_mut();
        c.system = extras.system;
        if let Some((saved, save)) = extras.traffic {
            c.traffic = Traffic::new(saved, &today());
            c.save_traffic = Some(save);
        }
    }
    wire(&window, &controller);
    pump(controller.clone(), events);
    // Once a second: every network's speeds and the last minute's charts move on.
    let traffic_tick = slint::Timer::default();
    {
        let weak = Rc::downgrade(&controller);
        traffic_tick.start(slint::TimerMode::Repeated, Duration::from_secs(1), move || {
            let Some(this) = weak.upgrade() else { return };
            if let Ok(mut c) = this.try_borrow_mut() {
                c.tick_traffic();
                c.render_chrome();
            }
        });
    }

    let tray =
        shell::tray(shell_tx).inspect_err(|error| tracing::warn!("no tray icon: {error}")).ok();
    let has_tray = tray.is_some();
    let weak = Rc::downgrade(&controller);
    window.window().on_close_requested(move || {
        // Downloads go on in the tray only if there is a tray to come back from.
        let stay = has_tray
            && weak
                .upgrade()
                .is_some_and(|c| c.try_borrow().is_ok_and(|c| c.settings.close_to_tray));
        if !stay {
            let _ = slint::quit_event_loop();
        }
        slint::CloseRequestResponse::HideWindow
    });
    listen(&window, &controller, shell_rx);
    // The tooltip follows the total speed the window shows, hidden or not.
    let tip = slint::Timer::default();
    if let Some(tray) = tray.clone() {
        let weak = window.as_weak();
        let mut last = String::new();
        tip.start(slint::TimerMode::Repeated, Duration::from_secs(1), move || {
            let Some(w) = weak.upgrade() else { return };
            let app = w.global::<App>();
            let text = shell::tooltip(app.get_active(), &app.get_total_speed());
            if text != last {
                let _ = tray.set_tooltip(Some(&text));
                last = text;
            }
        });
    }

    window.show()?;
    // Not `window.run()`: that stops when the window hides, and a window in the tray is hidden.
    slint::run_event_loop_until_quit()?;
    window.hide()?;
    drop(traffic_tick);
    controller.borrow().persist_traffic();
    // Removes the icon now rather than leaving a ghost until the pointer passes over it.
    drop(tip);
    drop(tray);
    Ok(())
}

/// Acts on the tray and on other launches.
fn listen(
    window: &AppWindow,
    controller: &Rc<RefCell<Controller>>,
    mut shell: mpsc::UnboundedReceiver<Shell>,
) {
    let window = window.as_weak();
    let controller = Rc::downgrade(controller);
    let task = slint::spawn_local(async move {
        while let Some(request) = shell.recv().await {
            let Some(w) = window.upgrade() else { return };
            match request {
                Shell::Show { args } => {
                    // ponytail: every hand-over just shows the window (`sonicnet://open`);
                    // magnet / .torrent arguments get their own handling when those land.
                    tracing::debug!(?args, "another launch");
                    bring_to_front(&w);
                }
                Shell::PauseAll | Shell::ResumeAll => {
                    let Some(c) = controller.upgrade() else { return };
                    let c = c.borrow();
                    for d in &c.downloads {
                        if request == Shell::PauseAll {
                            c.pause(d);
                        } else {
                            c.resume(d);
                        }
                    }
                }
                Shell::Quit => {
                    let _ = w.hide();
                    let _ = slint::quit_event_loop();
                }
                Shell::System(status) => {
                    let Some(c) = controller.upgrade() else { return };
                    let Ok(mut c) = c.try_borrow_mut() else { continue };
                    c.set_system_status(status);
                    c.render_chrome();
                }
            }
        }
    });
    if let Err(error) = task {
        tracing::error!("can't listen to the tray: {error}");
    }
}

/// Shows the window (from the tray too), un-minimises it and gives it the keyboard.
fn bring_to_front(w: &AppWindow) {
    use slint::winit_030::WinitWindowAccessor;

    if let Err(error) = w.show() {
        tracing::warn!("can't show the window: {error}");
    }
    w.window().set_minimized(false);
    // winit's focus works around Windows' foreground lock for a window in the background.
    w.window().with_winit_window(|winit| winit.focus_window());
}

/// Applies the backend's events as they arrive, rendering once per burst rather than per event.
fn pump(controller: Rc<RefCell<Controller>>, mut events: mpsc::Receiver<Event>) {
    let task = slint::spawn_local(async move {
        while let Some(event) = events.recv().await {
            let mut c = controller.borrow_mut();
            c.apply(event);
            while let Ok(event) = events.try_recv() {
                c.apply(event);
            }
            c.render();
        }
    });
    if let Err(error) = task {
        tracing::error!("can't listen for backend events: {error}");
    }
}

/// The New download form's state that only Rust needs.
#[derive(Debug, Default)]
struct NewDownload {
    probe: Option<ProbeResult>,
    /// Bumped by every new probe and edit, so a late answer to an old probe is ignored.
    probe_seq: u64,
    chosen: BTreeSet<NetworkId>,
    folder: PathBuf,
    free: Option<u64>,
    link: Option<u64>,
}

/// What a confirmation dialog will do when confirmed.
#[derive(Debug)]
enum Pending {
    Remove(Vec<DownloadId>),
    Cancel(DownloadId),
}

pub struct Controller {
    window: slint::Weak<AppWindow>,
    commands: mpsc::Sender<Command>,
    /// For async answers: a way back to itself.
    this: std::rc::Weak<RefCell<Controller>>,
    palette: Palette,

    // What the backend last said.
    downloads: Vec<Arc<DownloadState>>,
    history: Arc<Vec<FinishedDownload>>,
    networks: Arc<Vec<NetworkInterface>>,
    usage: BTreeMap<NetworkId, u64>,
    settings: Arc<AppSettings>,

    // The UI's own state.
    colors: BTreeMap<NetworkId, i32>,
    selection: BTreeSet<String>,
    anchor: Option<String>,
    filter: i32,
    /// The card open in place, if any.
    open: Option<String>,
    /// The card the keyboard is on.
    focus: Option<String>,
    expanded: BTreeSet<NetworkId>,
    grid_width: f32,
    menu_targets: Vec<String>,
    pending: Option<Pending>,
    new: NewDownload,
    form: view::Form,
    /// (id, OS name, kind) of each network the settings form lists, in its order.
    form_nets: Vec<(NetworkId, String, NetworkKind)>,

    // System mode and what each network carries.
    system: Option<system::Requests>,
    /// What was last asked of the supervisor, so a settings echo doesn't ask again.
    system_asked: Option<bool>,
    system_status: system::Status,
    traffic: Traffic,
    save_traffic: Option<SaveTraffic>,
    ticks: u64,

    // Models the UI keeps for life, updated in place.
    group_rows: Vec<Rc<VecModel<ui::Row>>>,
    groups: Rc<VecModel<ui::Group>>,
    detail_nets: Rc<VecModel<ui::NetLine>>,
    detail_chart: Rc<VecModel<ui::Series>>,
    chips: Rc<VecModel<ui::Chip>>,
    online: Rc<VecModel<ui::NetLine>>,
    pref_nets: Rc<VecModel<ui::NetPref>>,
    traffic_nets: Rc<VecModel<ui::TrafficNet>>,
    traffic_bar: Rc<VecModel<ui::TrafficNet>>,
}

impl std::fmt::Debug for Controller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Controller")
            .field("downloads", &self.downloads.len())
            .field("open", &self.open)
            .finish()
    }
}

/// Replaces a model's rows with `items`, touching only rows that changed, so the UI keeps its
/// elements (and their hover and animation state) across updates.
fn sync<T: Clone + PartialEq + 'static>(model: &VecModel<T>, items: Vec<T>) {
    let len = items.len();
    for (i, item) in items.into_iter().enumerate() {
        if i < model.row_count() {
            if model.row_data(i).as_ref() != Some(&item) {
                model.set_row_data(i, item);
            }
        } else {
            model.push(item);
        }
    }
    while model.row_count() > len {
        model.remove(model.row_count() - 1);
    }
}

fn ss(text: impl AsRef<str>) -> SharedString {
    SharedString::from(text.as_ref())
}

fn is_link(text: &str) -> bool {
    let t = text.trim().to_ascii_lowercase();
    (t.starts_with("http://") || t.starts_with("https://")) && !t.contains(char::is_whitespace)
}

fn clipboard_text() -> Option<String> {
    arboard::Clipboard::new().and_then(|mut c| c.get_text()).ok()
}

fn read_palette(window: &AppWindow) -> Palette {
    let theme = window.global::<Theme>();
    let mut palette = Palette::default();
    for swatch in theme.get_swatches().iter() {
        let rgb = |c: slint::Color| [c.red(), c.green(), c.blue()];
        palette.ids.push(swatch.id.to_string());
        palette.light.push(rgb(swatch.light));
        palette.dark.push(rgb(swatch.dark));
    }
    palette.kind_ids = theme.get_kind_swatch().iter().map(|s| s.to_string()).collect();
    palette
}

impl Controller {
    pub fn new(window: &AppWindow, commands: mpsc::Sender<Command>) -> Rc<RefCell<Self>> {
        let group_rows: Vec<Rc<VecModel<ui::Row>>> =
            (0..view::GROUP_TITLES.len()).map(|_| Rc::new(VecModel::default())).collect();
        let groups = Rc::new(VecModel::from(
            group_rows
                .iter()
                .zip(view::GROUP_TITLES)
                .map(|(rows, title)| ui::Group {
                    title: ss(title),
                    rows: ModelRc::from(rows.clone()),
                    ..Default::default()
                })
                .collect::<Vec<_>>(),
        ));
        let controller = Rc::new_cyclic(|this| {
            RefCell::new(Controller {
                window: window.as_weak(),
                commands,
                this: this.clone(),
                palette: read_palette(window),
                downloads: Vec::new(),
                history: Arc::default(),
                networks: Arc::default(),
                usage: BTreeMap::new(),
                settings: Arc::default(),
                colors: BTreeMap::new(),
                selection: BTreeSet::new(),
                anchor: None,
                filter: 0,
                open: None,
                focus: None,
                expanded: BTreeSet::new(),
                grid_width: 800.0,
                menu_targets: Vec::new(),
                pending: None,
                new: NewDownload::default(),
                form: view::Form::default(),
                form_nets: Vec::new(),
                system: None,
                system_asked: None,
                system_status: system::Status::default(),
                traffic: Traffic::new(None, &today()),
                save_traffic: None,
                ticks: 0,
                group_rows,
                groups,
                detail_nets: Rc::default(),
                detail_chart: Rc::default(),
                chips: Rc::default(),
                online: Rc::default(),
                pref_nets: Rc::default(),
                traffic_nets: Rc::default(),
                traffic_bar: Rc::default(),
            })
        });
        {
            let c = controller.borrow();
            let app = window.global::<App>();
            app.set_groups(ModelRc::from(c.groups.clone()));
            app.set_detail_nets(ModelRc::from(c.detail_nets.clone()));
            app.set_detail_chart(ModelRc::from(c.detail_chart.clone()));
            app.set_online(ModelRc::from(c.online.clone()));
            app.set_traffic_nets(ModelRc::from(c.traffic_nets.clone()));
            app.set_traffic_bar(ModelRc::from(c.traffic_bar.clone()));
            window.global::<NewDl>().set_chips(ModelRc::from(c.chips.clone()));
            window.global::<Prefs>().set_nets(ModelRc::from(c.pref_nets.clone()));
        }
        controller
    }

    fn send(&self, command: Command) {
        if let Err(error) = self.commands.try_send(command) {
            tracing::warn!("backend didn't take a command: {error}");
        }
    }

    /// Sends a command that is answered, and runs `done` with the answer on the UI thread.
    fn ask<T: 'static>(
        &self,
        make: impl FnOnce(Reply<T>) -> Command,
        done: impl FnOnce(&mut Controller, Result<T, String>) + 'static,
    ) {
        let (reply, answer) = oneshot::channel();
        self.send(make(reply));
        let this = self.this.clone();
        let task = slint::spawn_local(async move {
            let result =
                answer.await.unwrap_or_else(|_| Err("The download engine didn't answer.".into()));
            if let Some(this) = this.upgrade() {
                let mut c = this.borrow_mut();
                done(&mut c, result);
                c.render();
            }
        });
        if let Err(error) = task {
            tracing::error!("can't wait for an answer: {error}");
        }
    }

    fn window(&self) -> Option<AppWindow> {
        self.window.upgrade()
    }

    fn toast(&self, title: &str, body: &str) {
        if let Some(w) = self.window() {
            let app = w.global::<App>();
            app.set_toast_title(ss(title));
            app.set_toast_body(ss(body));
            app.set_toast_shown(true);
        }
    }

    // -----------------------------------------------------------------------------------------
    // Events

    pub fn apply(&mut self, event: Event) {
        match event {
            Event::Downloads(list) => self.downloads = list,
            Event::DownloadUpdated(d) => {
                if d.status == DownloadStatus::Cancelled {
                    self.forget(&d.id.to_string());
                    self.downloads.retain(|x| x.id != d.id);
                } else if let Some(slot) = self.downloads.iter_mut().find(|x| x.id == d.id) {
                    *slot = d;
                } else {
                    self.downloads.push(d);
                }
            }
            Event::DownloadRemoved(id) => {
                self.downloads.retain(|d| d.id != id);
                // It may live on in history; otherwise it is gone from every view.
                if !self.history.iter().any(|f| f.id == id) {
                    self.forget(&id.to_string());
                }
            }
            Event::History(history) => {
                self.history = history;
                let listed: BTreeSet<String> = self.listed_ids();
                self.selection.retain(|id| listed.contains(id));
            }
            Event::Networks(networks) => self.networks = networks,
            Event::NetworkUsage(usage) => {
                self.traffic.usage(&usage, &today());
                self.usage = usage;
            }
            Event::Settings(settings) => self.set_settings(settings),
            Event::LinkReceived(link) => self.open_link(link),
            Event::Notice { title, body } => self.toast(&title, &body),
        }
    }

    fn forget(&mut self, id: &str) {
        self.selection.remove(id);
        if self.open.as_deref() == Some(id) {
            self.open = None;
        }
    }

    fn listed_ids(&self) -> BTreeSet<String> {
        self.downloads
            .iter()
            .map(|d| d.id.to_string())
            .chain(self.history.iter().map(|f| f.id.to_string()))
            .collect()
    }

    /// Settings the backend saved: the theme and System mode follow them.
    fn set_settings(&mut self, settings: Arc<AppSettings>) {
        self.settings = settings;
        self.apply_theme();
        self.sync_system();
    }

    /// Starts or stops System mode to match the setting (at start-up, and when it is changed).
    fn sync_system(&mut self) {
        let want = self.settings.system_mode;
        if let Some(requests) = &self.system
            && self.system_asked != Some(want)
        {
            let _ = requests.send(if want {
                system::Request::Enable
            } else {
                system::Request::Disable
            });
            self.system_asked = Some(want);
        }
    }

    fn retry_system(&self) {
        if let Some(requests) = &self.system {
            let _ = requests.send(system::Request::Enable);
        }
    }

    /// The helper's latest news.
    pub fn set_system_status(&mut self, status: system::Status) {
        if status.state == system::State::On {
            self.traffic.system(&status.networks, &today());
        } else {
            self.traffic.system_stopped();
        }
        self.system_status = status;
    }

    /// Once a second: the downloads' speeds per network, and a chart sample for each.
    pub fn tick_traffic(&mut self) {
        let mut downloads: BTreeMap<NetworkId, f64> = BTreeMap::new();
        for d in self.downloads.iter().filter(|d| d.status == DownloadStatus::Downloading) {
            for n in &d.networks {
                *downloads.entry(n.id.clone()).or_default() += n.speed_bps.max(0.0);
            }
        }
        self.traffic.tick(&downloads, &today());
        self.ticks += 1;
        // Today's totals survive a crash with at most half a minute lost.
        if self.ticks.is_multiple_of(30) {
            self.persist_traffic();
        }
    }

    pub fn persist_traffic(&self) {
        if let Some(save) = &self.save_traffic {
            save(&self.traffic.saved());
        }
    }

    fn apply_theme(&self) {
        if let Some(w) = self.window() {
            let mode = match self.settings.theme {
                sonic_core::Theme::System => 0,
                sonic_core::Theme::Light => 1,
                sonic_core::Theme::Dark => 2,
            };
            w.global::<Theme>().set_mode(mode);
        }
    }

    // -----------------------------------------------------------------------------------------
    // Rendering

    /// Every network the UI may name, in a stable order: connected ones first.
    fn known_networks(&self) -> Vec<(NetworkId, String, NetworkKind)> {
        let mut seen = BTreeSet::new();
        let mut out = Vec::new();
        let mut add = |id: &NetworkId, name: &str, kind: NetworkKind| {
            if seen.insert(id.clone()) {
                out.push((id.clone(), name.to_string(), kind));
            }
        };
        for n in self.networks.iter() {
            add(&n.id, &n.name, n.kind);
        }
        for d in &self.downloads {
            for n in &d.networks {
                add(&n.id, &n.label, n.kind);
            }
        }
        for f in self.history.iter() {
            for n in &f.networks {
                add(&n.id, &n.label, n.kind);
            }
        }
        for id in self.settings.network_preferences.keys() {
            add(id, id.as_str(), NetworkKind::Other);
        }
        out
    }

    fn dark(&self) -> bool {
        self.window().is_some_and(|w| w.global::<Theme>().get_dark())
    }

    pub fn render(&mut self) {
        let known: Vec<(NetworkId, NetworkKind)> =
            self.known_networks().into_iter().map(|(id, _, kind)| (id, kind)).collect();
        self.colors =
            view::assign_colors(&known, &self.settings.network_preferences, &self.palette);
        self.render_chrome();
        self.render_list();
        self.render_detail();
        self.render_new();
    }

    fn ctx(&self) -> Ctx<'_> {
        Ctx {
            unit: self.settings.speed_unit,
            now: sonic_core::now_millis(),
            colors: &self.colors,
            prefs: &self.settings.network_preferences,
        }
    }

    pub fn render_chrome(&self) {
        let Some(w) = self.window() else { return };
        let app = w.global::<App>();
        let ctx = self.ctx();
        let running: Vec<&DownloadState> = self.downloads.iter().map(|d| d.as_ref()).collect();
        let total: f64 = running
            .iter()
            .filter(|d| d.status == DownloadStatus::Downloading)
            .map(|d| d.speed_bps)
            .sum();
        let downloading = running.iter().any(|d| d.status == DownloadStatus::Downloading);
        // Everything coming in: downloads plus System mode's traffic.
        let system_down: f64 = self.traffic.nets.values().map(|n| n.system_down_bps).sum();
        let total = if downloading { total } else { 0.0 } + system_down;
        let active = downloading || system_down >= 1024.0;
        app.set_total_speed(ss(format::speed(total, ctx.unit)));
        app.set_active(active);

        let system_on = self.system_status.state == system::State::On;
        let (state, label, tip, detail) =
            view::system_texts(&self.system_status, &self.traffic, &ctx);
        app.set_sys_state(state);
        app.set_sys_label(ss(label));
        app.set_sys_tip(ss(tip));
        app.set_sys_detail(ss(detail));
        let nets = view::traffic_nets(&self.traffic, &self.networks, system_on, &ctx);
        let bar: Vec<ui::TrafficNet> = nets.iter().filter(|n| n.online).cloned().collect();
        app.set_traffic_speed(ss(format::speed(self.traffic.down_bps(), ctx.unit)));
        app.set_traffic_session(ss(format::bytes(self.traffic.session_total())));
        let width = w.window().size().to_logical(w.window().scale_factor()).width;
        app.set_bar_names(width > bar.len() as f32 * 150.0 + 560.0);
        sync(&self.traffic_nets, nets);
        sync(&self.traffic_bar, bar);
        app.set_running(ss(view::summary(&running)));
        let s = &self.settings;
        app.set_slow_mode(s.slow_mode);
        app.set_slow_text(ss(format::speed(s.slow_mode_speed as f64, ctx.unit)));
        app.set_limit_text(ss(match (s.slow_mode, s.speed_limit) {
            (false, Some(limit)) => format!("Limited to {}", format::speed(limit as f64, ctx.unit)),
            _ => String::new(),
        }));
        app.set_schedule_on(s.schedule.enabled);
        if s.schedule.enabled {
            let (text, open) = view::schedule_line(&s.schedule, chrono::Local::now());
            app.set_schedule_text(ss(text));
            app.set_schedule_open_now(open);
        }
        let online: Vec<ui::NetLine> = self
            .networks
            .iter()
            .map(|n| ui::NetLine {
                id: ss(n.id.as_str()),
                name: ss(ctx.name(&n.id, &n.name)),
                kind: view::kind_index(n.kind),
                color: ctx.color(&n.id),
                ..Default::default()
            })
            .collect();
        app.set_networks_text(ss(match online.len() {
            0 => "No networks".to_string(),
            1 => "1 network".to_string(),
            n => format!("{n} networks"),
        }));
        app.set_no_networks(online.is_empty());
        sync(&self.online, online);
    }

    /// Everything listed, in list order, with its group.
    fn listed(&self) -> Vec<(usize, Item<'_>)> {
        let mut live: Vec<&DownloadState> = self.downloads.iter().map(|d| d.as_ref()).collect();
        live.sort_by_key(|d| (d.queued_at.unwrap_or(0), d.started_at));
        let mut items: Vec<(usize, Item)> = live
            .into_iter()
            .filter_map(|d| Item::Live(d).group().map(|g| (g, Item::Live(d))))
            .collect();
        items.extend(self.history.iter().map(|f| (view::FINISHED_GROUP, Item::Done(f))));
        // Stable: within a group, list order is kept.
        items.sort_by_key(|(g, _)| *g);
        items
    }

    fn visible_ids(&self) -> Vec<String> {
        self.listed()
            .into_iter()
            .filter(|(g, _)| view::filter_shows(self.filter, *g))
            .map(|(_, i)| i.id())
            .collect()
    }

    fn render_list(&self) {
        let Some(w) = self.window() else { return };
        let app = w.global::<App>();
        let ctx = self.ctx();
        let listed = self.listed();
        let mut counts = [0usize; 4];
        let mut rows: Vec<Vec<ui::Row>> = vec![Vec::new(); view::GROUP_TITLES.len()];
        for (group, item) in &listed {
            counts[0] += 1;
            match group {
                0..=2 => counts[1] += 1,
                3 => counts[3] += 1,
                _ => counts[2] += 1,
            }
            if view::filter_shows(self.filter, *group) {
                let id = item.id();
                let mut row =
                    view::row(*item, self.selection.contains(&id), &self.settings.schedule, &ctx);
                row.expanded = self.open.as_deref() == Some(id.as_str());
                row.focused = self.focus.as_deref() == Some(id.as_str());
                rows[*group].push(row);
            }
        }
        for (g, group_rows) in rows.into_iter().enumerate() {
            let selected = group_rows.iter().filter(|r| r.selected).count();
            let group = ui::Group {
                title: ss(view::GROUP_TITLES[g]),
                count: group_rows.len() as i32,
                checked: selected > 0 && selected == group_rows.len(),
                partial: selected > 0 && selected < group_rows.len(),
                rows: ModelRc::from(self.group_rows[g].clone()),
                action: ss(if g == view::FINISHED_GROUP && !self.history.is_empty() {
                    "Clear list"
                } else {
                    ""
                }),
            };
            sync(&self.group_rows[g], group_rows);
            if self.groups.row_data(g).as_ref() != Some(&group) {
                self.groups.set_row_data(g, group);
            }
        }
        let labels = ["All", "Active", "Finished", "Failed"]
            .iter()
            .zip(counts)
            .map(|(label, n)| ss(if n > 0 { format!("{label}  {n}") } else { label.to_string() }))
            .collect::<Vec<_>>();
        app.set_filter_labels(ModelRc::new(VecModel::from(labels)));
        app.set_filter(self.filter);
        app.set_empty(listed.is_empty());
        app.set_filtered_empty(
            !listed.is_empty() && !listed.iter().any(|(g, _)| view::filter_shows(self.filter, *g)),
        );
        let live: Vec<&DownloadState> = self.downloads.iter().map(|d| d.as_ref()).collect();
        app.set_summary(ss(view::summary(&live)));

        let visible: BTreeSet<String> = self.visible_ids().into_iter().collect();
        let chosen: Vec<&DownloadState> =
            live.iter().copied().filter(|d| self.selection.contains(&d.id.to_string())).collect();
        app.set_selected_count(
            self.selection.iter().filter(|id| visible.contains(*id)).count() as i32
        );
        app.set_sel_can_pause(
            chosen
                .iter()
                .any(|d| matches!(d.status, DownloadStatus::Downloading | DownloadStatus::Queued)),
        );
        app.set_sel_can_resume(
            chosen
                .iter()
                .any(|d| matches!(d.status, DownloadStatus::Paused | DownloadStatus::Error)),
        );
    }

    fn render_detail(&mut self) {
        let Some(id) = self.open.clone() else { return };
        let Some(w) = self.window() else { return };
        let app = w.global::<App>();
        let ctx = self.ctx();
        let live = self.downloads.iter().find(|d| d.id.to_string() == id).cloned();
        let done = self.history.iter().find(|f| f.id.to_string() == id).cloned();
        let (mut detail, lines, history, slots) = match (&live, &done) {
            (Some(d), _) if d.status == DownloadStatus::Completed => {
                let f = view::as_finished(d);
                let lines = view::net_lines(&f.networks, &[], false, &BTreeSet::new(), &ctx);
                (view::complete_detail(&f, &ctx), lines, f.speed_history, None)
            }
            (Some(d), _) => {
                let running = d.status == DownloadStatus::Downloading;
                let lines = view::net_lines(&d.networks, &d.streams, running, &self.expanded, &ctx);
                let detail = if d.status == DownloadStatus::Error {
                    view::failed_detail(d, &ctx)
                } else {
                    view::running_detail(d, &ctx)
                };
                (detail, lines, d.speed_history.clone(), Some(sonic_core::SPEED_HISTORY_SECONDS))
            }
            (None, Some(f)) => {
                let lines = view::net_lines(&f.networks, &[], false, &BTreeSet::new(), &ctx);
                let lines = lines.into_iter().filter(|l| l.share > 0.0).collect();
                (view::complete_detail(f, &ctx), lines, f.speed_history.clone(), None)
            }
            (None, None) => {
                self.open = None;
                return;
            }
        };
        let mut lines = lines;
        if detail.kind == ui::DetailKind::Running {
            (detail.flow_top, detail.flow_bottom) = view::flow_ribbons(&mut lines);
        }
        let order: Vec<(NetworkId, i32)> =
            lines.iter().map(|l| (NetworkId::new(l.id.as_str()), l.color)).collect();
        let chart = view::chart(&history, &order, slots, ctx.unit);
        detail.has_chart = !chart.series.is_empty();
        detail.chart_max = ss(if chart.top_bps > 0.0 {
            format::speed(chart.top_bps, ctx.unit)
        } else {
            String::new()
        });
        detail.chart_mid = ss(if chart.top_bps > 0.0 {
            format::speed(chart.top_bps / 2.0, ctx.unit)
        } else {
            String::new()
        });
        let samples = history.values().map(Vec::len).max().unwrap_or(0);
        detail.chart_caption = ss(match slots {
            Some(_) => "Throughput · last 60 s",
            None if samples < sonic_core::SPEED_HISTORY_SECONDS => "Speed over the download",
            None => "Speed · last minute of the download",
        });
        if let Some(d) = live.as_ref().filter(|_| detail.kind == ui::DetailKind::Running) {
            let grid = self.grid(d);
            detail.blocks = grid.image;
            detail.grid_w = grid.w;
            detail.grid_h = grid.h;
            detail.grid_cols = grid.cols;
            detail.grid_pitch = grid.pitch;
        }
        app.set_detail(detail);
        sync(&self.detail_nets, lines);
        sync(&self.detail_chart, chart.series);
    }

    fn grid(&self, d: &DownloadState) -> view::Grid {
        let dark = self.dark();
        let scale = self.window().map_or(1.0, |w| w.window().scale_factor());
        let hedged = d.streams.iter().filter(|s| s.hedge).filter_map(|s| s.block).collect();
        let (track, hedge) = if dark {
            ([0x34, 0x34, 0x34], [0xe8, 0xa3, 0x3d])
        } else {
            ([0xd9, 0xd9, 0xd9], [0xb8, 0x73, 0x0a])
        };
        let color_of =
            |id: &NetworkId| self.palette.rgb(self.colors.get(id).copied().unwrap_or(-1), dark);
        view::block_grid(
            &d.blocks,
            &hedged,
            &color_of,
            view::GridColors { track, hedge },
            self.grid_width,
            scale,
        )
    }

    fn block_info(&self, index: i32) -> String {
        let Some(id) = &self.open else { return String::new() };
        let Some(d) = self.downloads.iter().find(|d| &d.id.to_string() == id) else {
            return String::new();
        };
        usize::try_from(index).map(|i| view::block_info(d, i, &self.ctx())).unwrap_or_default()
    }

    // -----------------------------------------------------------------------------------------
    // List actions

    fn find_live(&self, id: &str) -> Option<&Arc<DownloadState>> {
        self.downloads.iter().find(|d| d.id.to_string() == id)
    }

    fn id_of(&self, id: &str) -> Option<DownloadId> {
        self.find_live(id)
            .map(|d| d.id)
            .or_else(|| self.history.iter().find(|f| f.id.to_string() == id).map(|f| f.id))
    }

    /// Opens a card in place (closing any other: one at a time keeps the page calm).
    fn open_detail(&mut self, id: &str) {
        if self.open.as_deref() != Some(id) {
            self.expanded.clear();
        }
        self.open = Some(id.to_string());
        self.focus = Some(id.to_string());
        if let Some(w) = self.window() {
            let app = w.global::<App>();
            app.set_relink_url(SharedString::default());
            app.set_relink_error(SharedString::default());
            app.set_body_id(ss(id));
        }
    }

    /// Shuts the open card. Its body stays built until the card has slid shut (the UI lets go).
    fn collapse(&mut self) {
        self.open = None;
    }

    fn toggle(&mut self, id: &str) {
        if self.open.as_deref() == Some(id) {
            self.collapse();
        } else {
            self.open_detail(id);
        }
    }

    /// Moves the keyboard's place through the visible cards.
    fn move_focus(&mut self, delta: i32) {
        let visible = self.visible_ids();
        if visible.is_empty() {
            return;
        }
        let at = self.focus.as_ref().and_then(|f| visible.iter().position(|v| v == f));
        let next = match at {
            Some(i) => (i as i32 + delta).clamp(0, visible.len() as i32 - 1) as usize,
            None if delta < 0 => visible.len() - 1,
            None => 0,
        };
        self.focus = Some(visible[next].clone());
    }

    fn row_pointer(&mut self, id: String, ctrl: bool, shift: bool) {
        match (ctrl, shift, self.anchor.clone()) {
            (_, true, Some(anchor)) => {
                let visible = self.visible_ids();
                let a = visible.iter().position(|x| *x == anchor);
                let b = visible.iter().position(|x| *x == id);
                if let (Some(a), Some(b)) = (a, b) {
                    if !ctrl {
                        self.selection.clear();
                    }
                    self.selection.extend(visible[a.min(b)..=a.max(b)].iter().cloned());
                }
            }
            (true, _, _) | (_, true, None) => {
                if !self.selection.remove(&id) {
                    self.selection.insert(id.clone());
                }
                self.anchor = Some(id);
            }
            // A plain click opens or shuts the card; selecting takes Ctrl, Shift or the box.
            _ => {
                self.anchor = Some(id.clone());
                self.toggle(&id);
            }
        }
    }

    fn pause(&self, d: &DownloadState) {
        if matches!(d.status, DownloadStatus::Downloading | DownloadStatus::Queued) {
            self.send(Command::Pause(d.id));
        }
    }

    fn resume(&self, d: &DownloadState) {
        if matches!(d.status, DownloadStatus::Paused)
            || (d.status == DownloadStatus::Error && d.resumable)
        {
            self.send(Command::Resume(d.id));
        }
    }

    fn row_action(&mut self, id: String) {
        match self.find_live(&id).cloned() {
            Some(d) if d.status == DownloadStatus::Error && !d.resumable => self.open_detail(&id),
            Some(d) if d.status == DownloadStatus::Completed => self.send(Command::Reveal(d.id)),
            Some(d) => {
                self.pause(&d);
                self.resume(&d);
            }
            None => {
                if let Some(id) = self.id_of(&id) {
                    self.send(Command::Reveal(id));
                }
            }
        }
    }

    fn ask_remove(&mut self, ids: Vec<String>) {
        let ids: Vec<DownloadId> = ids.iter().filter_map(|id| self.id_of(id)).collect();
        if ids.is_empty() {
            return;
        }
        let finished = ids.iter().filter(|id| self.history.iter().any(|f| f.id == **id)).count();
        let unfinished = ids.len() - finished;
        let title = if ids.len() == 1 {
            "Remove this download?".to_string()
        } else {
            format!("Remove {} downloads?", ids.len())
        };
        let body = match (unfinished, finished) {
            (0, _) => "It leaves the list. The file stays where it was saved unless you choose otherwise.".to_string(),
            (_, 0) => "It stops, and what it downloaded so far is deleted.".to_string(),
            _ => "Unfinished downloads stop and what they downloaded is deleted. Finished files stay where they were saved unless you choose otherwise.".to_string(),
        };
        self.confirm(&title, &body, "Remove", finished > 0, Pending::Remove(ids));
    }

    fn confirm(
        &mut self,
        title: &str,
        body: &str,
        action: &str,
        trash_option: bool,
        pending: Pending,
    ) {
        self.pending = Some(pending);
        if let Some(w) = self.window() {
            let app = w.global::<App>();
            app.set_confirm_title(ss(title));
            app.set_confirm_body(ss(body));
            app.set_confirm_action(ss(action));
            app.set_confirm_trash_option(trash_option);
            app.set_confirm_trash(false);
            app.set_confirm_open(true);
        }
    }

    fn confirmed(&mut self) {
        let trash = self.window().is_some_and(|w| w.global::<App>().get_confirm_trash());
        match self.pending.take() {
            Some(Pending::Remove(ids)) => {
                for id in ids {
                    self.selection.remove(&id.to_string());
                    self.send(Command::Remove { id, trash_file: trash });
                }
            }
            Some(Pending::Cancel(id)) => self.send(Command::Cancel(id)),
            None => {}
        }
        if let Some(w) = self.window() {
            w.global::<App>().set_confirm_open(false);
        }
    }

    fn bulk(&mut self, action: &str) {
        let chosen: Vec<String> = self.selection.iter().cloned().collect();
        match action {
            "remove" => self.ask_remove(chosen),
            _ => {
                for id in &chosen {
                    if let Some(d) = self.find_live(id).cloned() {
                        if action == "pause" {
                            self.pause(&d);
                        } else {
                            self.resume(&d);
                        }
                    }
                }
            }
        }
    }

    pub fn open_menu(&mut self, id: String, x: f32, y: f32) {
        self.menu_targets = if self.selection.contains(&id) {
            self.selection.iter().cloned().collect()
        } else {
            vec![id.clone()]
        };
        let item = |id: &str, label: &str, icon: slint::Image, danger: bool| ui::MenuItem {
            id: ss(id),
            label: ss(label),
            icon,
            danger,
            separator: false,
        };
        let sep = || ui::MenuItem { separator: true, ..Default::default() };
        let Some(w) = self.window() else { return };
        let icons = w.global::<ui::Icons>();
        let mut items = Vec::new();
        if self.menu_targets.len() > 1 {
            let n = self.menu_targets.len();
            let chosen: Vec<&Arc<DownloadState>> =
                self.menu_targets.iter().filter_map(|id| self.find_live(id)).collect();
            let pausable = chosen
                .iter()
                .filter(|d| {
                    matches!(d.status, DownloadStatus::Downloading | DownloadStatus::Queued)
                })
                .count();
            let resumable = chosen
                .iter()
                .filter(|d| {
                    d.status == DownloadStatus::Paused
                        || (d.status == DownloadStatus::Error && d.resumable)
                })
                .count();
            if pausable > 0 {
                items.push(item("pause", &format!("Pause {pausable}"), icons.get_pause(), false));
            }
            if resumable > 0 {
                items.push(item("resume", &format!("Resume {resumable}"), icons.get_play(), false));
            }
            if pausable + resumable > 0 {
                items.push(sep());
            }
            items.push(item("remove", &format!("Remove {n}…"), icons.get_trash(), true));
        } else {
            let live = self.find_live(&id).cloned();
            items.push(item("open", "Open details", icons.get_chevron_right(), false));
            match live.as_ref().map(|d| d.status) {
                Some(DownloadStatus::Downloading | DownloadStatus::Queued) => {
                    items.push(item("pause", "Pause", icons.get_pause(), false));
                }
                Some(DownloadStatus::Paused) => {
                    items.push(item("resume", "Resume", icons.get_play(), false))
                }
                Some(DownloadStatus::Error) if live.as_ref().is_some_and(|d| d.resumable) => {
                    items.push(item("resume", "Try again", icons.get_retry(), false));
                }
                None | Some(DownloadStatus::Completed) => {
                    items.push(item("open-file", "Open file", icons.get_open(), false));
                }
                _ => {}
            }
            items.push(item("reveal", "Show in folder", icons.get_folder(), false));
            items.push(item("copy-link", "Copy link", icons.get_copy(), false));
            items.push(sep());
            let unfinished = live.is_some_and(|d| d.status != DownloadStatus::Completed);
            items.push(item(
                "remove",
                if unfinished { "Cancel and remove…" } else { "Remove from list…" },
                icons.get_trash(),
                true,
            ));
        }
        let app = w.global::<App>();
        app.set_menu_items(ModelRc::new(VecModel::from(items)));
        app.set_menu_x(x);
        app.set_menu_y(y);
        app.set_menu_open(true);
    }

    fn menu_action(&mut self, action: &str) {
        let targets = std::mem::take(&mut self.menu_targets);
        match action {
            "open" => {
                if let Some(id) = targets.first() {
                    self.open_detail(id);
                }
            }
            "remove" => self.ask_remove(targets),
            "pause" | "resume" => {
                for id in &targets {
                    if let Some(d) = self.find_live(id).cloned() {
                        if action == "pause" {
                            self.pause(&d);
                        } else {
                            self.resume(&d);
                        }
                    }
                }
            }
            other => {
                if let Some(id) = targets.first() {
                    self.act_on(id, other);
                }
            }
        }
    }

    /// One-download actions shared by the menu and the detail screens.
    fn act_on(&mut self, id: &str, action: &str) {
        let Some(download) = self.id_of(id) else { return };
        let url = self
            .find_live(id)
            .map(|d| d.url.clone())
            .or_else(|| self.history.iter().find(|f| f.id == download).map(|f| f.url.clone()));
        match action {
            "reveal" => self.send(Command::Reveal(download)),
            "open-file" | "open" => self.send(Command::Open(download)),
            "pause" | "resume" => {
                if let Some(d) = self.find_live(id).cloned() {
                    self.pause(&d);
                    self.resume(&d);
                }
            }
            "cancel" => self.confirm(
                "Cancel this download?",
                "It stops, and what it downloaded so far is deleted.",
                "Cancel download",
                false,
                Pending::Cancel(download),
            ),
            "remove" => self.ask_remove(vec![id.to_string()]),
            "copy-link" => {
                let copied = url.is_some_and(|url| {
                    arboard::Clipboard::new().and_then(|mut c| c.set_text(url)).is_ok()
                });
                if copied {
                    self.toast("Link copied", "");
                } else {
                    self.toast("Couldn't copy the link", "The clipboard is busy. Try again.");
                }
            }
            _ => {}
        }
    }

    fn relink(&mut self) {
        let (Some(w), Some(id)) = (self.window(), self.open.clone()) else { return };
        let Some(download) = self.id_of(&id) else { return };
        let app = w.global::<App>();
        let url = app.get_relink_url().trim().to_string();
        if !is_link(&url) {
            app.set_relink_error(ss(
                "That isn't a web link. Links start with http:// or https://.",
            ));
            return;
        }
        app.set_relink_error(SharedString::default());
        self.ask(
            |reply| Command::Relink { id: download, url, reply },
            |c, result| {
                if let Some(w) = c.window() {
                    let app = w.global::<App>();
                    match result {
                        Ok(()) => {
                            app.set_relink_url(SharedString::default());
                            c.toast(
                                "Link replaced",
                                "The download carries on from where it stopped.",
                            );
                        }
                        Err(error) => app.set_relink_error(ss(error)),
                    }
                }
            },
        );
    }

    fn toggle_slow_mode(&mut self) {
        let mut settings = (*self.settings).clone();
        settings.slow_mode = !settings.slow_mode;
        self.ask(
            |reply| Command::UpdateSettings { settings, reply },
            |c, result| match result {
                Ok(saved) => c.set_settings(Arc::new(saved)),
                Err(error) => c.toast("Couldn't switch slow mode", &error),
            },
        );
    }

    // -----------------------------------------------------------------------------------------
    // New download

    /// Opens New download, with the clipboard's link already in it when it holds one.
    pub fn open_new(&mut self, url: Option<String>) {
        let url = url.or_else(|| clipboard_text().filter(|t| is_link(t)));
        self.new = NewDownload {
            chosen: view::default_choice(
                &self.networks,
                &self.usage,
                &self.settings.network_preferences,
            ),
            folder: self.settings.destination_dir.clone().unwrap_or_else(default_folder),
            ..NewDownload::default()
        };
        if let Some(w) = self.window() {
            let nd = w.global::<NewDl>();
            nd.set_url(ss(url.as_deref().unwrap_or("").trim()));
            nd.set_file_name(SharedString::default());
            nd.set_streams(0);
            nd.set_from(SharedString::default());
            nd.set_error(SharedString::default());
            nd.set_probe(ui::ProbeState::Idle);
            w.global::<App>().set_new_open(true);
        }
        if let Some(url) = url {
            self.probe(url.trim().to_string());
        }
        self.fetch_free_space();
        self.render_new();
    }

    fn open_link(&mut self, link: PendingLink) {
        self.open_new(Some(link.url.clone()));
        self.new.link = Some(link.id);
        if let Some(w) = self.window() {
            let from = match (&link.browser_name, &link.from) {
                (Some(b), Some(page)) => format!("From {b} · {}", format::host(page)),
                (Some(b), None) => format!("From {b}"),
                (None, Some(page)) => format!("From {}", format::host(page)),
                (None, None) => String::new(),
            };
            w.global::<NewDl>().set_from(ss(from));
        }
        if let Some(probe) = link.probe {
            // Already asked: a single-use link would be spent by asking again.
            self.new.probe_seq += 1;
            self.probe_done(self.new.probe_seq, Ok(probe));
        }
    }

    fn url_edited(&mut self) {
        self.new.probe_seq += 1;
        self.new.probe = None;
        if let Some(w) = self.window() {
            w.global::<NewDl>().set_probe(ui::ProbeState::Idle);
        }
        self.render_new();
    }

    fn probe(&mut self, url: String) {
        self.new.probe_seq += 1;
        self.new.probe = None;
        let seq = self.new.probe_seq;
        let Some(w) = self.window() else { return };
        let nd = w.global::<NewDl>();
        if url.trim().is_empty() {
            nd.set_probe(ui::ProbeState::Idle);
            return;
        }
        if !is_link(&url) {
            nd.set_probe(ui::ProbeState::Failed);
            nd.set_probe_message(ss(
                "That isn't a web link. Links start with http:// or https://.",
            ));
            return;
        }
        nd.set_probe(ui::ProbeState::Checking);
        self.ask(
            |reply| Command::Probe { url: url.trim().to_string(), reply },
            move |c, result| c.probe_done(seq, result),
        );
    }

    /// A probe's answer, unless the link changed since it was asked.
    pub fn probe_done(&mut self, seq: u64, result: Result<ProbeResult, String>) {
        if seq != self.new.probe_seq {
            return;
        }
        let Some(w) = self.window() else { return };
        let nd = w.global::<NewDl>();
        match result {
            Ok(probe) => {
                let message = match (probe.total_bytes, probe.supports_ranges) {
                    (Some(size), true) => format!(
                        "{} · the server sends it in parts, so every network can help",
                        format::bytes(size)
                    ),
                    (Some(size), false) => format!(
                        "{} · the server sends it in one piece: one stream, one network",
                        format::bytes(size)
                    ),
                    (None, _) => {
                        "The server didn't say how big it is: one stream, one network".to_string()
                    }
                };
                nd.set_probe(ui::ProbeState::Ok);
                nd.set_probe_message(ss(message));
                nd.set_file_name(ss(&probe.suggested_file_name));
                nd.set_splittable(
                    probe.supports_ranges && probe.total_bytes.is_some_and(|n| n > 0),
                );
                self.new.probe = Some(probe);
            }
            Err(error) => {
                nd.set_probe(ui::ProbeState::Failed);
                nd.set_probe_message(ss(error));
            }
        }
        self.render_new();
    }

    fn fetch_free_space(&mut self) {
        let dir = self.new.folder.clone();
        let asked = dir.clone();
        self.ask(
            |reply| Command::FreeSpace { dir, reply },
            move |c, result| {
                if c.new.folder == asked {
                    c.free_space_done(result.ok().flatten());
                }
            },
        );
    }

    pub fn free_space_done(&mut self, free: Option<u64>) {
        self.new.free = free;
        self.render_new();
    }

    fn browse(&mut self) {
        let picked = rfd::FileDialog::new()
            .set_directory(&self.new.folder)
            .set_title("Save downloads to")
            .pick_folder();
        if let Some(folder) = picked {
            self.new.folder = folder;
            self.new.free = None;
            self.fetch_free_space();
            self.render_new();
        }
    }

    fn toggle_chip(&mut self, index: i32) {
        let Some(chip) = usize::try_from(index).ok().and_then(|i| self.chips.row_data(i)) else {
            return;
        };
        if chip.blocked {
            return;
        }
        let id = NetworkId::new(chip.id.as_str());
        if !self.new.chosen.remove(&id) {
            self.new.chosen.insert(id);
        }
        self.render_new();
    }

    fn render_new(&self) {
        let Some(w) = self.window() else { return };
        if !w.global::<App>().get_new_open() {
            return;
        }
        let nd = w.global::<NewDl>();
        let ctx = self.ctx();
        let chips = view::chips(&self.networks, &self.usage, &self.new.chosen, &ctx);
        let picked = chips.iter().filter(|c| c.selected).count();
        nd.set_chips_summary(ss(format!("{picked} of {} selected", chips.len())));
        sync(&self.chips, chips);

        let size = self.new.probe.as_ref().and_then(|p| p.total_bytes);
        nd.set_size(ss(size.map(format::bytes).unwrap_or_else(|| "Unknown".into())));
        nd.set_folder(ss(self.new.folder.display().to_string()));
        nd.set_free_space(ss(self
            .new
            .free
            .map(|f| format!("{} free", format::bytes(f)))
            .unwrap_or_default()));
        nd.set_low_space(matches!((size, self.new.free), (Some(size), Some(free)) if size > free));

        let splittable = nd.get_splittable() || self.new.probe.is_none();
        let streams = view::streams_choice(nd.get_streams());
        nd.set_streams_hint(ss(match (splittable, streams) {
            (false, _) => {
                "This server sends the file in one piece, so one stream on one network.".to_string()
            }
            (true, None) => {
                "Starts with 8 per network and adds more while the server keeps up.".to_string()
            }
            (true, Some(n)) => format!("{n} per network · {} in all", n as usize * picked.max(1)),
        }));
        let mut summary = vec![match picked {
            0 => "No network chosen".to_string(),
            1 => "1 network".to_string(),
            n => format!("{n} networks"),
        }];
        if let Some(size) = size {
            summary.push(format::bytes(size));
        }
        nd.set_summary(ss(summary.join(" · ")));
        nd.set_can_start(
            self.new.probe.is_some()
                && !nd.get_file_name().trim().is_empty()
                && picked > 0
                && !nd.get_low_space(),
        );
    }

    fn start(&mut self) {
        let Some(w) = self.window() else { return };
        let nd = w.global::<NewDl>();
        let Some(probe) = self.new.probe.clone() else { return };
        let file_name = nd.get_file_name().trim().to_string();
        let network_ids: Vec<NetworkId> = self
            .networks
            .iter()
            .filter(|n| self.new.chosen.contains(&n.id))
            .map(|n| n.id.clone())
            .collect();
        let request = StartRequest {
            url: probe.final_url.clone(),
            destination_dir: self.new.folder.clone(),
            file_name,
            total_bytes: probe.total_bytes,
            supports_ranges: probe.supports_ranges,
            network_ids,
            streams_per_network: view::streams_choice(nd.get_streams()),
            etag: probe.etag.clone(),
            last_modified: probe.last_modified.clone(),
            browser: None,
        };
        let link = self.new.link.take();
        self.ask(
            |reply| Command::Start { request, link, reply },
            |c, result| {
                let Some(w) = c.window() else { return };
                match result {
                    Ok(id) => {
                        w.global::<App>().set_new_open(false);
                        c.open_detail(&id.to_string());
                    }
                    Err(error) => w.global::<NewDl>().set_error(ss(error)),
                }
            },
        );
    }

    fn cancel_new(&mut self) {
        if let Some(link) = self.new.link.take() {
            self.send(Command::DismissLink(link));
        }
        self.new.probe_seq += 1;
        if let Some(w) = self.window() {
            w.global::<App>().set_new_open(false);
        }
    }

    // -----------------------------------------------------------------------------------------
    // Settings

    pub fn open_settings(&mut self, page: i32) {
        self.form_nets = self
            .known_networks()
            .into_iter()
            .filter(|(id, ..)| {
                self.networks.iter().any(|n| &n.id == id)
                    || self.settings.network_preferences.contains_key(id)
            })
            .collect();
        let names: Vec<(NetworkId, String)> =
            self.form_nets.iter().map(|(id, name, _)| (id.clone(), name.clone())).collect();
        self.form = view::form_of(&self.settings, &names);
        let Some(w) = self.window() else { return };
        let p = w.global::<Prefs>();
        let f = &self.form;
        p.set_page(page);
        p.set_theme(f.theme);
        p.set_unit(f.unit);
        p.set_at_once(f.at_once);
        p.set_tray(f.tray);
        p.set_system_mode(f.system_mode);
        p.set_limit_on(f.limit_on);
        p.set_limit(ss(&f.limit));
        p.set_slow_speed(ss(&f.slow_speed));
        p.set_sched_on(f.sched_on);
        p.set_sched_start(ss(&f.sched_start));
        p.set_sched_stop(ss(&f.sched_stop));
        p.set_sched_end_on(f.sched_end_on);
        p.set_sched_end(ss(&f.sched_end));
        p.set_error(SharedString::default());
        self.render_prefs();
        w.global::<App>().set_settings_open(true);
    }

    /// Reads the form's plain fields back from the UI (the network rows and days are kept here).
    fn read_form(&mut self) {
        let Some(w) = self.window() else { return };
        let p = w.global::<Prefs>();
        let f = &mut self.form;
        f.theme = p.get_theme();
        f.unit = p.get_unit();
        f.at_once = p.get_at_once();
        f.tray = p.get_tray();
        f.system_mode = p.get_system_mode();
        f.limit_on = p.get_limit_on();
        f.limit = p.get_limit().to_string();
        f.slow_speed = p.get_slow_speed().to_string();
        f.sched_on = p.get_sched_on();
        f.sched_start = p.get_sched_start().to_string();
        f.sched_stop = p.get_sched_stop().to_string();
        f.sched_end_on = p.get_sched_end_on();
        f.sched_end = p.get_sched_end().to_string();
    }

    fn render_prefs(&mut self) {
        self.read_form();
        let Some(w) = self.window() else { return };
        let p = w.global::<Prefs>();
        let unit = if self.form.unit == 1 {
            sonic_core::SpeedUnit::Bits
        } else {
            sonic_core::SpeedUnit::Bytes
        };
        p.set_unit_label(ss(format::speed_unit_label(unit)));
        p.set_sched_days(ModelRc::new(VecModel::from(self.form.sched_days.to_vec())));
        p.set_sched_summary(ss(view::schedule_sentence(
            self.form.sched_on,
            &self.form.sched_start,
            &self.form.sched_stop,
            &self.form.sched_days,
        )));
        // Colours as they would be with the form's picks, so a pick shows at once.
        let mut prefs = self.settings.network_preferences.clone();
        for net in &self.form.nets {
            prefs.entry(net.id.clone()).or_default().color_id = net.color.clone();
        }
        let known: Vec<(NetworkId, NetworkKind)> =
            self.known_networks().into_iter().map(|(id, _, kind)| (id, kind)).collect();
        let colors = view::assign_colors(&known, &prefs, &self.palette);
        let rows = self
            .form
            .nets
            .iter()
            .zip(&self.form_nets)
            .map(|(net, (_, os_name, kind))| {
                let pref = self.settings.network_preferences.get(&net.id);
                let used = self.usage.get(&net.id).copied().unwrap_or(0);
                let limit = pref.and_then(|p| p.data_limit);
                let period = match pref.map(|p| p.data_period) {
                    Some(sonic_core::DataPeriod::Day) => "today",
                    Some(sonic_core::DataPeriod::Week) => "this week",
                    _ => "this month",
                };
                let usage = match limit {
                    Some(limit) => {
                        format!("{} of {} {period}", format::bytes(used), format::bytes(limit))
                    }
                    None => format!("{} {period}", format::bytes(used)),
                };
                ui::NetPref {
                    id: ss(net.id.as_str()),
                    os_name: ss(os_name),
                    description: ss(self
                        .networks
                        .iter()
                        .find(|n| n.id == net.id)
                        .map_or("", |n| n.description.as_str())),
                    kind: view::kind_index(*kind),
                    color: colors.get(&net.id).copied().unwrap_or(-1),
                    custom_color: net
                        .color
                        .as_deref()
                        .and_then(|c| self.palette.ids.iter().position(|id| id == c))
                        .map_or(-1, |i| i as i32),
                    custom_name: ss(&net.name),
                    speed_limit: ss(&net.speed),
                    data_limit: ss(&net.data),
                    period: net.period,
                    usage: ss(usage),
                    usage_fraction: limit.map_or(0.0, |l| (used as f64 / l.max(1) as f64) as f32),
                    over: limit.is_some_and(|l| used >= l),
                    online: self.networks.iter().any(|n| n.id == net.id),
                }
            })
            .collect();
        sync(&self.pref_nets, rows);
    }

    fn net_edited(&mut self, index: i32, field: &str, value: &str) {
        let Some(net) = usize::try_from(index).ok().and_then(|i| self.form.nets.get_mut(i)) else {
            return;
        };
        match field {
            "name" => net.name = value.to_string(),
            "speed" => net.speed = value.to_string(),
            "data" => net.data = value.to_string(),
            "period" => net.period = value.parse().unwrap_or(2),
            _ => return,
        }
        self.render_prefs();
    }

    fn pick_color(&mut self, index: i32, swatch: i32) {
        let id = usize::try_from(swatch).ok().and_then(|s| self.palette.ids.get(s).cloned());
        if let Some(net) = usize::try_from(index).ok().and_then(|i| self.form.nets.get_mut(i)) {
            net.color = id;
            self.render_prefs();
        }
    }

    fn save_settings(&mut self) {
        self.read_form();
        let settings = match view::settings_from_form(&self.settings, &self.form) {
            Ok(settings) => settings,
            Err(error) => {
                if let Some(w) = self.window() {
                    w.global::<Prefs>().set_error(ss(error));
                }
                return;
            }
        };
        self.ask(
            |reply| Command::UpdateSettings { settings, reply },
            |c, result| {
                let Some(w) = c.window() else { return };
                match result {
                    Ok(saved) => {
                        c.set_settings(Arc::new(saved));
                        w.global::<App>().set_settings_open(false);
                    }
                    Err(error) => w.global::<Prefs>().set_error(ss(error)),
                }
            },
        );
    }

    // -----------------------------------------------------------------------------------------
    // Hooks for the snapshot renderer, which drives the same paths without an event loop.

    pub fn set_filter(&mut self, filter: i32) {
        self.filter = filter;
        self.selection.clear();
    }

    pub fn select(&mut self, ids: &[String]) {
        self.selection = ids.iter().cloned().collect();
    }

    pub fn show(&mut self, id: Option<&str>, expand: &[NetworkId]) {
        match id {
            Some(id) => {
                self.open_detail(id);
                self.expanded = expand.iter().cloned().collect();
            }
            None => {
                self.collapse();
                self.focus = None;
            }
        }
    }

    pub fn probe_seq(&self) -> u64 {
        self.new.probe_seq
    }

    pub fn set_grid_width(&mut self, width: f32) {
        self.grid_width = width;
    }

    pub fn downloads(&self) -> &[Arc<DownloadState>] {
        &self.downloads
    }

    pub fn history(&self) -> &[FinishedDownload] {
        &self.history
    }

    pub fn networks(&self) -> &[NetworkInterface] {
        &self.networks
    }

    pub fn open_traffic(&self) {
        if let Some(w) = self.window() {
            w.global::<App>().set_traffic_open(true);
        }
    }
}

/// Today's local date, `YYYY-MM-DD`: the day traffic totals count for.
fn today() -> String {
    chrono::Local::now().date_naive().to_string()
}

fn default_folder() -> PathBuf {
    std::env::var_os("USERPROFILE")
        .map(|home| PathBuf::from(home).join("Downloads"))
        .unwrap_or_else(|| PathBuf::from("Downloads"))
}

/// Runs `f` on the controller once the current Slint callback has returned. Callbacks that can
/// fire while Rust is itself updating the UI (a layout reporting its new width) use this, so the
/// controller is never borrowed twice.
fn later(this: &Rc<RefCell<Controller>>, f: impl FnOnce(&mut Controller) + 'static) {
    let this = Rc::downgrade(this);
    slint::Timer::single_shot(Duration::ZERO, move || {
        if let Some(this) = this.upgrade() {
            let mut c = this.borrow_mut();
            f(&mut c);
            c.render();
        }
    });
}

/// Connects every UI callback to the controller.
pub fn wire(window: &AppWindow, controller: &Rc<RefCell<Controller>>) {
    // Each handler holds the controller weakly: the window owns the handlers, and the
    // controller must not keep the window alive.
    macro_rules! on {
        ($global:ty, $setter:ident, |$c:ident $(, $arg:ident)*| $body:expr) => {{
            let weak = Rc::downgrade(controller);
            window.global::<$global>().$setter(move |$($arg),*| {
                let Some(this) = weak.upgrade() else { return };
                let Ok(mut guard) = this.try_borrow_mut() else {
                    tracing::warn!(concat!(stringify!($setter), " re-entered; dropped"));
                    return;
                };
                let $c: &mut Controller = &mut guard;
                $body;
                $c.render();
            });
        }};
    }

    on!(App, on_new_download, |c| c.open_new(None));
    on!(App, on_paste_link, |c| match clipboard_text().filter(|t| is_link(t)) {
        Some(url) => c.open_new(Some(url)),
        None => c.toast(
            "No link on the clipboard",
            "Copy a download link (https://…) and press Ctrl+V again."
        ),
    });
    on!(App, on_set_filter, |c, filter| c.set_filter(filter));
    on!(App, on_row_pointer, |c, id, ctrl, shift| c.row_pointer(id.to_string(), ctrl, shift));
    on!(App, on_row_open, |c, id| c.toggle(&id));
    on!(App, on_move_focus, |c, delta| c.move_focus(delta));
    on!(App, on_toggle_focused, |c| match c.focus.clone() {
        Some(id) => c.toggle(&id),
        None => c.move_focus(1),
    });
    on!(App, on_escape, |c| if c.open.is_some() {
        c.collapse();
    } else {
        c.selection.clear();
    });
    on!(App, on_row_menu, |c, id, x, y| c.open_menu(id.to_string(), x, y));
    on!(App, on_row_action, |c, id| c.row_action(id.to_string()));
    on!(App, on_select_group, |c, group, on| {
        let ids: Vec<String> = c
            .listed()
            .into_iter()
            .filter(|(g, _)| *g as i32 == group)
            .map(|(_, item)| item.id())
            .collect();
        for id in ids {
            if on {
                c.selection.insert(id);
            } else {
                c.selection.remove(&id);
            }
        }
    });
    on!(App, on_group_action, |c, group| if group as usize == view::FINISHED_GROUP {
        c.send(Command::ClearHistory);
    });
    on!(App, on_select_all, |c| c.selection = c.visible_ids().into_iter().collect());
    on!(App, on_clear_selection, |c| c.selection.clear());
    on!(App, on_bulk, |c, action| c.bulk(&action));
    on!(App, on_delete_pressed, |c| {
        let chosen: Vec<String> = c.selection.iter().cloned().collect();
        c.ask_remove(chosen);
    });
    on!(App, on_menu_action, |c, action| c.menu_action(&action));
    on!(App, on_detail_action, |c, action| if let Some(id) = c.open.clone() {
        c.act_on(&id, &action);
    });
    on!(App, on_toggle_network, |c, network, enabled| if let Some(id) =
        c.open.as_deref().and_then(|id| c.id_of(id))
    {
        c.send(Command::SetNetwork { id, network: NetworkId::new(network.as_str()), enabled });
    });
    on!(App, on_expand_network, |c, network| {
        let network = NetworkId::new(network.as_str());
        if !c.expanded.remove(&network) {
            c.expanded.insert(network);
        }
    });
    on!(App, on_relink, |c| c.relink());
    on!(App, on_confirm, |c| c.confirmed());
    on!(App, on_toggle_slow_mode, |c| c.toggle_slow_mode());
    on!(App, on_open_settings, |c, page| c.open_settings(page));
    on!(App, on_sys_retry, |c| c.retry_system());

    {
        let weak = Rc::downgrade(controller);
        window.global::<App>().on_grid_resized(move |width| {
            let Some(this) = weak.upgrade() else { return };
            if let Ok(mut c) = this.try_borrow_mut() {
                if (c.grid_width - width).abs() >= 1.0 {
                    c.grid_width = width;
                    c.render_detail();
                }
            } else {
                later(&this, move |c| c.grid_width = width);
            }
        });
        let weak = Rc::downgrade(controller);
        window.global::<App>().on_block_info(move |index| {
            let Some(this) = weak.upgrade() else { return SharedString::default() };
            // Asked while the UI paints; if Rust is mid-update, skip this frame's readout.
            this.try_borrow().map(|c| ss(c.block_info(index))).unwrap_or_default()
        });
    }

    on!(NewDl, on_url_edited, |c, _text| c.url_edited());
    on!(NewDl, on_probe_now, |c, url| c.probe(url.to_string()));
    on!(NewDl, on_paste, |c| if let Some(text) = clipboard_text() {
        if let Some(w) = c.window() {
            w.global::<NewDl>().set_url(ss(text.trim()));
        }
        c.probe(text.trim().to_string());
    });
    on!(NewDl, on_browse, |c| c.browse());
    on!(NewDl, on_toggle_chip, |c, index| c.toggle_chip(index));
    on!(NewDl, on_streams_changed, |c, _index| c.render_new());
    on!(NewDl, on_start, |c| c.start());
    on!(NewDl, on_cancel, |c| c.cancel_new());

    on!(Prefs, on_save, |c| c.save_settings());
    on!(Prefs, on_cancel, |c| if let Some(w) = c.window() {
        w.global::<App>().set_settings_open(false);
    });
    on!(Prefs, on_changed, |c| c.render_prefs());
    on!(Prefs, on_toggle_day, |c, day| {
        if let Some(d) = usize::try_from(day).ok().and_then(|d| c.form.sched_days.get_mut(d)) {
            *d = !*d;
        }
        c.render_prefs();
    });
    on!(Prefs, on_reset_usage, |c, network| c
        .send(Command::ResetNetworkUsage(NetworkId::new(network.as_str()))));
    on!(Prefs, on_pick_color, |c, index, swatch| c.pick_color(index, swatch));
    on!(Prefs, on_net_edited, |c, index, field, value| c.net_edited(index, &field, &value));
}
