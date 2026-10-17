//! The manager's task: the list of downloads and their queue, the settings, the networks, the
//! schedule, data usage and the browser bridge. It owns no download's state — each download's
//! task does (see [`crate::download`]) — only a mirror of where each stands in the queue, kept
//! current by the downloads' [`Note`]s.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use chrono::Local;
use sonic_core::{
    AppSettings, BrowserContext, Command, DownloadId, DownloadState, DownloadStatus, Event, Millis,
    NetworkId, NetworkInterface, PendingLink, Reply, StartRequest, now_millis, plan_blocks,
};
use sonic_engine::limits::{Limits, UsageSnapshot};
use sonic_engine::plan::{BLOCKS_PER_STREAM, MAX_BLOCK_BYTES, MIN_BLOCK_BYTES};
use sonic_engine::schedule;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::bridge_host::{BridgeEvent, BridgeLink};
use crate::download::{self, Control, Download};
use crate::platform::{self, KeepAwake};
use crate::util::{Clock, gigabytes, sleep_until_opt};
use crate::{Ctx, ManagerConfig, NetworkSource, Note, Tuning, probe_link, queue};

/// How often data usage is pushed to the UI when it changed.
const USAGE_PUSH: Duration = Duration::from_secs(2);
/// How often data usage is saved.
const USAGE_SAVE: Duration = Duration::from_secs(10);
/// The longest the manager goes without looking at the clock: the schedule, usage, sleep.
const HOUSEKEEPING: Duration = Duration::from_secs(1);
/// The wall clock moving this much further than the timer slept means the computer slept.
const SLEEP_JUMP: Duration = Duration::from_secs(5);

/// Where a download stands, as the manager last heard.
#[derive(Debug)]
struct Handle {
    control: mpsc::UnboundedSender<Control>,
    status: DownloadStatus,
    queued_at: Option<Millis>,
    scheduled: bool,
}

#[derive(Debug)]
struct Manager {
    ctx: Arc<Ctx>,
    default_destination: PathBuf,
    settings: AppSettings,
    downloads: HashMap<DownloadId, Handle>,
    /// Links from the browser waiting to be started: their sign-in, kept here, never in the UI.
    links: HashMap<u64, Option<BrowserContext>>,
    next_link: u64,
    seen_addresses: HashMap<NetworkId, Vec<IpAddr>>,
    awake: KeepAwake,
    shutting_down: bool,
    next_check: Instant,
    /// When housekeeping last ran, by the wall clock and by the timer.
    last_wall: SystemTime,
    last_timer: Instant,
    usage_pushed_at: Instant,
    usage_saved_at: Instant,
    last_usage: Option<BTreeMap<NetworkId, u64>>,
    at_limit: HashSet<NetworkId>,
    bridge: Option<sonic_bridge::Bridge>,
    _monitor: Option<sonic_net::NetworkMonitor>,
}

/// The manager's task, from restoring saved downloads to shutdown.
pub(crate) async fn run(
    config: ManagerConfig,
    mut commands: mpsc::UnboundedReceiver<Command>,
    events: mpsc::UnboundedSender<Event>,
) {
    let ManagerConfig { store, networks, tuning, bridge, default_destination } = config;
    let (mut networks_rx, monitor) = match networks {
        NetworkSource::System => {
            let monitor = sonic_net::NetworkMonitor::start();
            (monitor.subscribe(), Some(monitor))
        }
        NetworkSource::Fixed(rx) => (rx, None),
    };
    let settings = store.load_settings().await;
    let mut limits = Limits::new();
    limits.configure(&settings);
    if let Some(usage) = store.load_usage::<UsageSnapshot>().await {
        limits.import(usage, &Local::now());
    }
    let (notes_tx, mut notes_rx) = mpsc::unbounded_channel();
    let ctx = Arc::new(Ctx {
        store,
        tuning,
        events,
        networks: networks_rx.clone(),
        limits: Arc::new(Mutex::new(limits)),
        shares: Mutex::default(),
        notes: notes_tx,
        clock: Clock::new(),
    });
    let present = networks_rx.borrow_and_update().clone();
    let mut manager = Manager {
        ctx: ctx.clone(),
        default_destination,
        settings,
        downloads: HashMap::new(),
        links: HashMap::new(),
        next_link: 1,
        seen_addresses: HashMap::new(),
        awake: KeepAwake::default(),
        shutting_down: false,
        next_check: Instant::now(),
        last_wall: SystemTime::now(),
        last_timer: Instant::now(),
        usage_pushed_at: Instant::now(),
        usage_saved_at: Instant::now(),
        last_usage: None,
        at_limit: HashSet::new(),
        bridge: None,
        _monitor: monitor,
    };
    download::address_changes(&mut manager.seen_addresses, &present);

    // Restored first, then the full picture, in the order the UI expects.
    let restored = manager.restore().await;
    manager.emit(Event::Settings(Arc::new(manager.settings.clone())));
    manager.emit(Event::Networks(present));
    manager.emit(Event::Downloads(restored));
    manager.emit_history().await;

    let (bridge_tx, mut bridge_rx) = mpsc::unbounded_channel();
    if bridge {
        let host = Arc::new(BridgeLink::new(ctx.clone(), bridge_tx));
        match sonic_bridge::start(host).await {
            Ok(bridge) => manager.bridge = Some(bridge),
            Err(error) => tracing::warn!(%error, "the browser bridge could not start"),
        }
    } else {
        drop(bridge_tx);
    }

    manager.refresh_schedule();
    manager.update_awake();
    let mut networks_live = true;
    loop {
        let next_check = manager.next_check;
        tokio::select! {
            command = commands.recv() => match command {
                None => break,
                Some(Command::Shutdown { reply }) => {
                    manager.shutdown().await;
                    let _ = reply.send(Ok(()));
                    break;
                }
                Some(command) => manager.on_command(command).await,
            },
            Some(note) = notes_rx.recv() => manager.on_note(note).await,
            changed = networks_rx.changed(), if networks_live => match changed {
                Ok(()) => {
                    let present = networks_rx.borrow_and_update().clone();
                    manager.networks_changed(present);
                }
                // Nobody will tell of networks any more; the last list stands.
                Err(_) => networks_live = false,
            },
            Some(event) = bridge_rx.recv() => manager.on_bridge(event).await,
            () = sleep_until_opt(Some(next_check)) => manager.housekeeping().await,
        }
    }
    if let Some(bridge) = manager.bridge.take() {
        bridge.shutdown().await;
    }
}

impl Manager {
    fn emit(&self, event: Event) {
        let _ = self.ctx.events.send(event);
    }

    async fn emit_history(&self) {
        let history = self.ctx.store.history().await;
        self.emit(Event::History(Arc::new(history)));
    }

    /// Takes up the downloads saved before the last exit; returns their states, oldest first.
    async fn restore(&mut self) -> Vec<Arc<DownloadState>> {
        let mut listed = Vec::new();
        for restored in self.ctx.store.list_saved().await {
            // Its publication happened before the app went down: history keeps it from here.
            if restored.state.status == DownloadStatus::Completed {
                let entry = download::finished(&restored.state);
                match self.ctx.store.add_to_history(&entry).await {
                    Ok(()) => {
                        let _ = self.ctx.store.remove_download(restored.state.id).await;
                    }
                    Err(error) => {
                        tracing::warn!(%error, "could not move a finished download to history")
                    }
                }
                continue;
            }
            let download = Download::restored(self.ctx.clone(), restored);
            listed.push(Arc::new(download.state().clone()));
            self.add(download);
        }
        listed.sort_by_key(|state| state.started_at);
        listed
    }

    fn add(&mut self, download: Download) {
        let state = download.state();
        let (id, status, queued_at, scheduled) =
            (state.id, state.status, state.queued_at, state.scheduled);
        let control = download.spawn();
        self.downloads.insert(id, Handle { control, status, queued_at, scheduled });
    }

    fn send(&self, id: DownloadId, control: Control) -> bool {
        self.downloads.get(&id).is_some_and(|handle| handle.control.send(control).is_ok())
    }

    fn send_all(&self, control: impl Fn() -> Control) {
        for handle in self.downloads.values() {
            let _ = handle.control.send(control());
        }
    }

    // --- commands ------------------------------------------------------------------------------

    async fn on_command(&mut self, command: Command) {
        match command {
            Command::Probe { url, reply } => {
                let (networks, tuning) = (self.ctx.networks.clone(), self.ctx.tuning.clone());
                tokio::spawn(async move {
                    let _ = reply.send(probe_link(&networks, &tuning, &url, None).await);
                });
            }
            Command::Start { request, link, reply } => {
                let result = self.start(request, link).await;
                let _ = reply.send(result);
            }
            Command::Pause(id) => {
                self.send(id, Control::Pause);
            }
            Command::Resume(id) => {
                self.send(id, Control::Resume);
            }
            Command::Cancel(id) => {
                self.send(id, Control::Cancel);
            }
            Command::Remove { id, trash_file } => {
                if !self.send(id, Control::Remove { trash_file }) {
                    self.remove_finished(id, trash_file).await;
                }
            }
            Command::Relink { id, url, reply } => match self.downloads.get(&id) {
                Some(handle) => {
                    if let Err(mpsc::error::SendError(Control::Relink { reply, .. })) =
                        handle.control.send(Control::Relink { url, reply })
                    {
                        let _ = reply.send(Err("That download is no longer listed.".into()));
                    }
                }
                None => {
                    let _ = reply.send(Err("That download is no longer listed.".into()));
                }
            },
            Command::SetNetwork { id, network, enabled } => {
                self.send(id, Control::SetNetwork { network, enabled });
            }
            Command::UpdateSettings { settings, reply } => {
                self.update_settings(settings, reply).await
            }
            Command::Reveal(id) => {
                if !self.send(id, Control::Reveal) {
                    self.reveal_finished(id, false).await;
                }
            }
            Command::Open(id) => {
                if !self.send(id, Control::Open) {
                    self.reveal_finished(id, true).await;
                }
            }
            Command::ClearHistory => {
                if let Err(error) = self.ctx.store.clear_history().await {
                    tracing::warn!(%error, "could not clear history");
                }
                self.emit_history().await;
            }
            Command::ResetNetworkUsage(network) => {
                if let Ok(mut limits) = self.ctx.limits.lock() {
                    limits.reset(&network, &Local::now());
                }
                self.limits_changed();
                self.push_usage(true);
                self.save_usage().await;
            }
            Command::FreeSpace { dir, reply } => {
                tokio::spawn(async move {
                    let free =
                        platform::blocking(move || platform::free_space(&dir)).await.flatten();
                    let _ = reply.send(Ok(free));
                });
            }
            Command::DismissLink(id) => {
                self.links.remove(&id);
            }
            // Handled by the loop, which ends after it.
            Command::Shutdown { reply } => {
                let _ = reply.send(Ok(()));
            }
        }
    }

    /// Checks a request and starts the download, or queues it. Answers with its id.
    async fn start(
        &mut self,
        mut request: StartRequest,
        link: Option<u64>,
    ) -> Result<DownloadId, String> {
        if let Some(browser) = link.and_then(|link| self.links.remove(&link)) {
            request.browser = browser;
        }
        if let Some(browser) = &request.browser {
            sonic_http::validate_browser_context(browser).map_err(|e| {
                format!("The browser’s details for this download couldn’t be read ({e}).")
            })?;
        }
        let present = self.ctx.networks.borrow().clone();
        let selected: Vec<&NetworkInterface> =
            present.iter().filter(|iface| request.network_ids.contains(&iface.id)).collect();
        if selected.is_empty() {
            return Err("Select at least one network".into());
        }

        let mut dir = request.destination_dir.clone();
        if dir.as_os_str().is_empty() {
            dir = self
                .settings
                .destination_dir
                .clone()
                .unwrap_or_else(|| self.default_destination.clone());
        }
        let dir = std::path::absolute(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|e| format!("Couldn’t create the folder {}: {e}", dir.display()))?;
        request.destination_dir.clone_from(&dir);
        // The staging file is on the destination drive and becomes the final file by rename.
        if let Some(needed) = request.total_bytes.filter(|n| *n > 0) {
            let free = platform::blocking({
                let dir = dir.clone();
                move || platform::free_space(&dir)
            })
            .await
            .flatten();
            if let Some(free) = free.filter(|free| *free < needed) {
                return Err(format!(
                    "Not enough disk space: this download needs {} but only {} is free",
                    gigabytes(needed),
                    gigabytes(free)
                ));
            }
        }

        let splittable = download::splittable(&request);
        let block_size =
            block_size(&self.ctx.tuning, request.total_bytes, splittable, selected.len());
        let blocks = plan_blocks(request.total_bytes, block_size);
        let staging = sonic_http::reserve(&dir, &request.file_name)
            .await
            .map_err(|e| format!("Couldn’t create the file in {}: {e}", dir.display()))?;

        let mut networks: Vec<_> = present
            .iter()
            .map(|iface| download::new_network(iface, request.network_ids.contains(&iface.id)))
            .collect();
        if !splittable {
            // One request carries the whole file: one network.
            let first = selected[0].id.clone();
            for network in &mut networks {
                network.enabled &= network.id == first;
                if !network.enabled {
                    network.status = sonic_core::NetworkStatus::Off;
                }
            }
        }

        // Decided only now, in one go: nothing else starts meanwhile on this task.
        let room = self.has_room();
        let now = now_millis();
        let queued_at = (!room).then(|| self.place(false, None));
        let state = DownloadState {
            id: DownloadId::new(),
            url: request.url.clone(),
            file_name: staging
                .final_path()
                .file_name()
                .map_or_else(|| request.file_name.clone(), |n| n.to_string_lossy().into_owned()),
            destination_path: staging.final_path().to_path_buf(),
            total_bytes: request.total_bytes,
            bytes_downloaded: 0,
            speed_bps: 0.0,
            time_left_secs: None,
            status: if room { DownloadStatus::Downloading } else { DownloadStatus::Queued },
            error: None,
            resumable: true,
            started_at: now,
            queued_at,
            // Time spent waiting isn't time spent downloading.
            paused_at: (!room).then_some(now),
            total_paused_ms: 0,
            completed_at: None,
            scheduled: self.settings.schedule.enabled,
            networks,
            streams: Vec::new(),
            peak_streams: 0,
            blocks,
            block_size,
            speed_history: BTreeMap::new(),
            peak_speed_bps: None,
            disk_limited: false,
            from_browser: request
                .browser
                .as_ref()
                .map(|b| b.name.clone().unwrap_or_else(|| "your browser".into())),
        };
        let id = state.id;
        let mut download = Download::new(self.ctx.clone(), state, request, staging);
        download.save_first().await;
        self.add(download);
        if room {
            self.send(id, Control::Begin { scheduled: self.settings.schedule.enabled });
        }
        self.update_awake();
        Ok(id)
    }

    /// Removes a finished download, from history.
    async fn remove_finished(&mut self, id: DownloadId, trash_file: bool) {
        if trash_file {
            let history = self.ctx.store.history().await;
            if let Some(entry) = history.iter().find(|entry| entry.id == id && !entry.missing) {
                let path = entry.destination_path.clone();
                if let Some(Err(error)) = platform::blocking(move || platform::trash(&path)).await {
                    tracing::warn!(%error, "could not move a download to the Recycle Bin");
                }
            }
        }
        if let Err(error) = self.ctx.store.remove_from_history(&[id]).await {
            tracing::warn!(%error, "could not remove a download from history");
        }
        self.emit_history().await;
    }

    /// Shows (or opens) a finished download's file, by its own recorded path. When it's gone, the
    /// UI hears history again, which marks it missing.
    async fn reveal_finished(&self, id: DownloadId, open: bool) {
        let history = self.ctx.store.history().await;
        let path =
            history.iter().find(|entry| entry.id == id).map(|entry| entry.destination_path.clone());
        if let Some(path) = path
            && platform::exists(path.clone()).await
        {
            let shown = platform::blocking(move || {
                if open { platform::open_file(&path) } else { platform::reveal(&path) }
            })
            .await;
            if let Some(Err(error)) = shown {
                tracing::warn!(%error, "could not show a download");
            }
            return;
        }
        self.emit_history().await;
    }

    async fn update_settings(&mut self, settings: AppSettings, reply: Reply<AppSettings>) {
        if settings.schedule.enabled && !schedule::is_valid(&settings.schedule) {
            let _ = reply.send(Err(
                "The download schedule isn’t valid: its start and end must differ, it needs at least one day, and its end date must be a real date."
                    .into(),
            ));
            return;
        }
        let saved = match self.ctx.store.save_settings(&settings).await {
            Ok(saved) => saved,
            Err(error) => {
                let _ = reply.send(Err(format!("Couldn’t save the settings: {error}")));
                return;
            }
        };
        self.apply_settings(saved.clone());
        let _ = reply.send(Ok(saved.clone()));
        self.emit(Event::Settings(Arc::new(saved)));
    }

    /// Takes up what the user set: how many run at once, the limits and the schedule. One running
    /// past a lowered count is left to finish; a raised one starts the next in the queue.
    fn apply_settings(&mut self, settings: AppSettings) {
        let enabled = settings.schedule.enabled;
        for handle in self.downloads.values_mut() {
            let enrolled =
                matches!(handle.status, DownloadStatus::Queued | DownloadStatus::Downloading);
            if enrolled && handle.scheduled != enabled {
                handle.scheduled = enabled;
                let _ = handle.control.send(Control::SetScheduled(enabled));
            }
        }
        if let Ok(mut limits) = self.ctx.limits.lock() {
            limits.configure(&settings);
        }
        self.settings = settings;
        self.limits_changed();
        self.refresh_schedule();
        self.next_check = Instant::now();
    }

    /// A limit or allowance changed: every download looks at its networks again, so one at its
    /// limit stops being used and one no longer at it is used again.
    fn limits_changed(&mut self) {
        self.at_limit = self.networks_at_limit();
        self.send_all(|| Control::Reconcile);
    }

    fn networks_at_limit(&self) -> HashSet<NetworkId> {
        let now = Local::now();
        let Ok(mut limits) = self.ctx.limits.lock() else { return HashSet::new() };
        let ids: Vec<NetworkId> = self.settings.network_preferences.keys().cloned().collect();
        ids.into_iter().filter(|id| limits.limit_reached(id, &now)).collect()
    }

    // --- what downloads tell --------------------------------------------------------------------

    async fn on_note(&mut self, note: Note) {
        match note {
            Note::Status { id, status, queued_at, scheduled } => {
                let Some(handle) = self.downloads.get_mut(&id) else { return };
                let was_running = handle.status == DownloadStatus::Downloading;
                handle.status = status;
                handle.queued_at = queued_at;
                handle.scheduled = scheduled;
                if was_running && status != DownloadStatus::Downloading {
                    self.pump();
                }
                self.update_awake();
            }
            Note::WantsToRun { id, ack } => {
                if self.has_room() {
                    self.begin(id);
                } else {
                    // Asked for by name, so it goes next.
                    let queued_at = self.place(true, Some(id));
                    if let Some(handle) = self.downloads.get_mut(&id) {
                        handle.status = DownloadStatus::Queued;
                        handle.queued_at = Some(queued_at);
                    }
                    let scheduled = self.settings.schedule.enabled;
                    self.send(id, Control::Enqueue { queued_at, scheduled });
                }
                if let Some(ack) = ack
                    && let Some(handle) = self.downloads.get(&id)
                    && let Err(mpsc::error::SendError(Control::Ack(ack))) =
                        handle.control.send(Control::Ack(ack))
                {
                    let _ = ack.send(Ok(()));
                }
            }
            Note::Finished { id } | Note::Removed { id } => {
                let finished = matches!(note, Note::Finished { .. });
                // Dropping the handle lets its task end once it has handled what it was sent.
                self.downloads.remove(&id);
                if finished {
                    self.emit_history().await;
                }
                self.pump();
                self.update_awake();
            }
            Note::HistoryChanged => self.emit_history().await,
        }
    }

    // --- the queue -----------------------------------------------------------------------------

    fn running(&self) -> usize {
        self.downloads.values().filter(|h| h.status == DownloadStatus::Downloading).count()
    }

    fn schedule_allows(&self) -> bool {
        schedule::window(&self.settings.schedule, &Local::now()).allowed
    }

    fn has_room(&self) -> bool {
        !self.shutting_down
            && self.schedule_allows()
            && self.running() < self.settings.downloads_at_once as usize
    }

    /// A queue place: at the back, or (asked for by name) at the front.
    fn place(&self, front: bool, except: Option<DownloadId>) -> Millis {
        let others = self
            .downloads
            .iter()
            .filter(|(id, h)| Some(**id) != except && h.status == DownloadStatus::Queued)
            .filter_map(|(_, h)| h.queued_at);
        queue::place(front, now_millis(), others)
    }

    fn begin(&mut self, id: DownloadId) {
        let scheduled = self.settings.schedule.enabled;
        if let Some(handle) = self.downloads.get_mut(&id) {
            handle.status = DownloadStatus::Downloading;
            handle.queued_at = None;
            let _ = handle.control.send(Control::Begin { scheduled });
        }
        self.update_awake();
    }

    /// Starts queued downloads, first queued first, while fewer than `downloads_at_once` run.
    fn pump(&mut self) {
        if self.shutting_down || !self.schedule_allows() {
            return;
        }
        let queued: Vec<(DownloadId, Millis)> = self
            .downloads
            .iter()
            .filter(|(_, h)| h.status == DownloadStatus::Queued)
            .map(|(id, h)| (*id, h.queued_at.unwrap_or(0)))
            .collect();
        for id in queue::to_start(queued, self.running(), self.settings.downloads_at_once as usize)
        {
            self.begin(id);
        }
    }

    /// Outside a schedule window, running downloads stop and wait in the queue; inside one, the
    /// queue runs.
    fn refresh_schedule(&mut self) {
        if self.shutting_down {
            return;
        }
        if self.schedule_allows() {
            self.pump();
            return;
        }
        let running: Vec<DownloadId> = self
            .downloads
            .iter()
            .filter(|(_, h)| h.status == DownloadStatus::Downloading)
            .map(|(id, _)| *id)
            .collect();
        for id in running {
            let queued_at = self.place(false, Some(id));
            if let Some(handle) = self.downloads.get_mut(&id) {
                handle.status = DownloadStatus::Queued;
                handle.queued_at = Some(queued_at);
            }
            self.send(id, Control::PauseForSchedule { queued_at });
        }
        self.update_awake();
    }

    fn update_awake(&mut self) {
        let running = self.running() > 0;
        self.awake.set(running);
    }

    // --- the world outside -----------------------------------------------------------------------

    /// The computer's networks changed. A network whose addresses changed gets its streams going
    /// again at once: what they were waiting out may be what changed. One that lost an address
    /// also drops its sockets, which may be bound to it — as Chrome does on ERR_NETWORK_CHANGED.
    fn networks_changed(&mut self, present: Arc<Vec<NetworkInterface>>) {
        let (changed, moved) = download::address_changes(&mut self.seen_addresses, &present);
        let (changed, moved) = (Arc::new(changed), Arc::new(moved));
        self.emit(Event::Networks(present));
        self.send_all(|| Control::NetworksChanged {
            changed: changed.clone(),
            moved: moved.clone(),
        });
    }

    async fn on_bridge(&mut self, event: BridgeEvent) {
        match event {
            BridgeEvent::Offer(link) => {
                let link = *link;
                let id = self.next_link;
                self.next_link += 1;
                let browser_name = link.browser.as_ref().and_then(|b| b.name.clone());
                self.links.insert(id, link.browser);
                self.emit(Event::LinkReceived(PendingLink {
                    id,
                    url: link.url,
                    probe: Some(link.probe),
                    from: link.page_url,
                    browser_name,
                }));
            }
            BridgeEvent::ExtensionSeen => {
                // From now on closing the window should keep the app in the tray, where the
                // browser can still reach it.
                if !self.settings.close_to_tray {
                    let settings = AppSettings { close_to_tray: true, ..self.settings.clone() };
                    if let Ok(saved) = self.ctx.store.save_settings(&settings).await {
                        self.settings = saved.clone();
                        self.emit(Event::Settings(Arc::new(saved)));
                    }
                }
            }
        }
    }

    /// Looks at the clock: the computer waking, the schedule, data usage.
    async fn housekeeping(&mut self) {
        let now = Instant::now();
        let wall = SystemTime::now();
        let slept_for = now.saturating_duration_since(self.last_timer);
        let wall_elapsed = wall.duration_since(self.last_wall).unwrap_or_default();
        (self.last_timer, self.last_wall) = (now, wall);
        if wall_elapsed > slept_for + SLEEP_JUMP {
            // The computer slept (or its clock jumped): sockets are likely dead, and every
            // judgement made by the clock spans the sleep.
            tracing::info!(?wall_elapsed, "the computer seems to have slept");
            self.send_all(|| Control::SystemResumed);
        }
        self.refresh_schedule();

        if now.saturating_duration_since(self.usage_pushed_at) >= USAGE_PUSH {
            self.usage_pushed_at = now;
            self.push_usage(false);
            let at_limit = self.networks_at_limit();
            if at_limit != self.at_limit {
                self.limits_changed();
            }
        }
        if now.saturating_duration_since(self.usage_saved_at) >= USAGE_SAVE {
            self.usage_saved_at = now;
            self.save_usage().await;
        }

        // Re-checked at least every second, so a clock or time zone change is caught, and at the
        // schedule's next boundary exactly.
        let window = schedule::window(&self.settings.schedule, &Local::now());
        let boundary = if window.allowed { window.ends_at } else { window.next_start };
        let until = boundary
            .and_then(|at| (at - Local::now()).to_std().ok())
            .map_or(HOUSEKEEPING, |until| until.clamp(Duration::from_millis(1), HOUSEKEEPING));
        self.next_check = now + until;
    }

    fn push_usage(&mut self, always: bool) {
        let used = match self.ctx.limits.lock() {
            Ok(mut limits) => limits.used_by_network(&Local::now()),
            Err(_) => return,
        };
        if always || self.last_usage.as_ref() != Some(&used) {
            self.last_usage = Some(used.clone());
            self.emit(Event::NetworkUsage(used));
        }
    }

    async fn save_usage(&self) {
        let usage = match self.ctx.limits.lock() {
            Ok(mut limits) => limits.export(&Local::now()),
            Err(_) => return,
        };
        if let Err(error) = self.ctx.store.save_usage(&usage).await {
            tracing::warn!(%error, "could not save data usage");
        }
    }

    /// The app is quitting: every download stops (a scheduled one stays queued) and is saved.
    async fn shutdown(&mut self) {
        self.shutting_down = true;
        let ids: Vec<DownloadId> = self.downloads.keys().copied().collect();
        let mut waits = Vec::new();
        for id in ids {
            let Some(handle) = self.downloads.get(&id) else { continue };
            let queued_at = (handle.status == DownloadStatus::Downloading && handle.scheduled)
                .then(|| self.place(false, Some(id)));
            let (reply, wait) = oneshot::channel();
            if self.send(id, Control::Suspend { queued_at, reply }) {
                waits.push(wait);
            }
        }
        for wait in waits {
            let _ = wait.await;
        }
        self.save_usage().await;
        self.awake.set(false);
    }
}

/// Bytes per block: as the engine plans it (every network able to grow to its most streams with
/// a couple of blocks each), within the tuning's bounds — tests plan blocks below a megabyte.
pub(crate) fn block_size(
    tuning: &Tuning,
    total: Option<u64>,
    splittable: bool,
    networks: usize,
) -> u64 {
    let total = match total {
        Some(total) if splittable && total > 0 => total,
        // One request carries the whole file.
        other => return other.unwrap_or(0),
    };
    let max = tuning.max_block_bytes.max(1);
    let min = tuning.min_block_bytes.unwrap_or(MIN_BLOCK_BYTES.min(MAX_BLOCK_BYTES)).clamp(1, max);
    let target =
        networks.max(1) as u64 * u64::from(sonic_core::MAX_STREAMS_PER_NETWORK) * BLOCKS_PER_STREAM;
    total.div_ceil(target).clamp(min, max)
}

/// The network to ask through when nothing else decides: the first that can reach the internet.
pub(crate) fn probe_network(present: &[NetworkInterface]) -> Option<&NetworkInterface> {
    present.iter().find(|iface| iface.has_gateway).or(present.first())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_follow_the_engine_within_the_tunings_bounds() {
        let production = Tuning::default();
        assert_eq!(block_size(&production, Some(1 << 40), true, 1), MAX_BLOCK_BYTES);
        assert_eq!(block_size(&production, Some(10), true, 1), MIN_BLOCK_BYTES);
        assert_eq!(
            block_size(&production, Some(512 << 20), true, 2),
            sonic_engine::plan::block_size(Some(512 << 20), true, 2)
        );
        let small =
            Tuning { max_block_bytes: 64 * 1024, min_block_bytes: Some(4096), ..Tuning::default() };
        assert_eq!(block_size(&small, Some(1 << 20), true, 1), 16 * 1024);
        assert_eq!(block_size(&small, Some(10), true, 1), 4096);
        // Unsplittable or unknown: one block of the whole file.
        assert_eq!(block_size(&small, Some(1 << 20), false, 1), 1 << 20);
        assert_eq!(block_size(&small, None, true, 1), 0);
    }

    #[test]
    fn probes_go_through_a_network_with_a_way_out() {
        let mut a = sonic_net::loopback();
        a.id = NetworkId::new("a");
        let mut b = a.clone();
        b.id = NetworkId::new("b");
        b.has_gateway = true;
        assert_eq!(probe_network(&[a.clone(), b.clone()]).map(|i| i.id.as_str()), Some("b"));
        assert_eq!(probe_network(std::slice::from_ref(&a)).map(|i| i.id.as_str()), Some("a"));
        assert!(probe_network(&[]).is_none());
    }
}
