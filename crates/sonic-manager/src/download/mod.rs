//! One download, as a task that owns it for its whole life: its state, its request, its staging
//! file and, while it runs, its streams' bookkeeping (see [`transfer`]). The manager speaks to it
//! with [`Control`] messages and hears back through [`Note`]s; the UI hears from it directly
//! (`Event::DownloadUpdated`).
//!
//! Everything that changes a download happens on this one task, in the order it was asked for:
//! a pause that arrives while a sample check is in flight is handled after it, never beside it.

mod networks;
mod persist;
mod stream;
mod transfer;

use std::collections::HashSet;
use std::sync::Arc;

use sonic_core::{
    BlockStatus, BrowserContext, DownloadNetwork, DownloadState, DownloadStatus, Event, Millis,
    NetworkId, NetworkStatus, Reply, StartRequest, StreamStatus, now_millis,
};
use sonic_engine::meter::{PeakTracker, sample_speeds};
use sonic_http::StagingFile;
use sonic_store::{Publication, RestoredDownload};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

pub(crate) use networks::{address_changes, new_network};
pub(crate) use persist::finished;
pub(crate) use stream::connection;
pub(crate) use transfer::splittable;

use crate::util::sleep_until_opt;
use crate::{Ctx, Note, platform};
use persist::{Persister, Snapshot};
use stream::{SampleJob, Source, StreamMsg, Verdict};
use transfer::Transfer;

/// What the manager asks of a download.
#[derive(Debug)]
pub(crate) enum Control {
    /// Run now: the manager made room. `scheduled`: whether the schedule is on.
    Begin {
        scheduled: bool,
    },
    /// Wait in the queue at `queued_at` (a resume with no room: ahead of the others).
    Enqueue {
        queued_at: Millis,
        scheduled: bool,
    },
    /// The schedule's window closed: stop, but stay in the queue at `queued_at`.
    PauseForSchedule {
        queued_at: Millis,
    },
    Pause,
    /// The user asked: checks it can, then asks the manager for room (`Note::WantsToRun`).
    Resume,
    Cancel,
    Remove {
        trash_file: bool,
    },
    Relink {
        url: String,
        reply: Reply<()>,
    },
    /// A new link's check, done off this task.
    RelinkChecked {
        result: Result<(String, Option<BrowserContext>), String>,
        reply: Reply<()>,
    },
    SetNetwork {
        network: NetworkId,
        enabled: bool,
    },
    /// The schedule was switched on or off: a queued or running download follows it.
    SetScheduled(bool),
    /// Limits or allowances changed: look at the networks again.
    Reconcile,
    /// The computer's networks changed; `changed` got new addresses, `moved` lost one.
    NetworksChanged {
        changed: Arc<HashSet<NetworkId>>,
        moved: Arc<HashSet<NetworkId>>,
    },
    SystemResumed,
    Reveal,
    Open,
    /// The app is quitting: stop (staying queued at `queued_at` if given) and save.
    Suspend {
        queued_at: Option<Millis>,
        reply: oneshot::Sender<()>,
    },
    /// Answered in turn: sent after a `Begin` or `Enqueue`, it is answered once that is done.
    Ack(Reply<()>),
}

/// A download's task.
#[derive(Debug)]
pub(crate) struct Download {
    ctx: Arc<Ctx>,
    state: DownloadState,
    request: StartRequest,
    staging: StagingFile,
    publication: Option<Publication>,
    transfer: Transfer,
    persister: Persister,
    peak: PeakTracker,
    stream_tx: mpsc::UnboundedSender<StreamMsg>,
    stream_rx: mpsc::UnboundedReceiver<StreamMsg>,
    /// Its own control channel, for work done off the task to report back. Weak, so the task
    /// still ends when the manager lets go of it.
    me: Option<mpsc::WeakUnboundedSender<Control>>,
    /// Just started: save it and show it before anything else.
    fresh: bool,
    /// Still in the download list (not removed, not moved to history).
    listed: bool,
    /// Its saved state is gone: nothing may save it again.
    unsaved: bool,
    publishing: bool,
    relinking: bool,
    /// The network most recently switched off: what a resume switches back on if it finds none
    /// on. Not persisted: after a restart, the first network present is used instead.
    last_switched_off: Option<NetworkId>,
    /// Paused by switching off its last network, rather than by Pause: switching one back on
    /// resumes it.
    paused_for_no_network: bool,
    tick_at: Option<Instant>,
    ui_due: Option<Instant>,
    checkpoint_due: Option<Instant>,
    ticked_at: Millis,
    speed_sampled_at: Millis,
    /// What the manager was last told of its place: status, queue place, schedule enrollment.
    reported: Option<(DownloadStatus, Option<Millis>, bool)>,
    /// The task ends after this message.
    gone: bool,
}

impl Download {
    fn with(
        ctx: Arc<Ctx>,
        state: DownloadState,
        request: StartRequest,
        staging: StagingFile,
    ) -> Self {
        let (stream_tx, stream_rx) = mpsc::unbounded_channel();
        let transfer = Transfer::new(ctx.clone(), state.id, &request);
        let persister = Persister::start(ctx.store.clone(), state.id);
        let peak = PeakTracker::new(state.peak_speed_bps);
        Self {
            ctx,
            state,
            request,
            staging,
            publication: None,
            transfer,
            persister,
            peak,
            stream_tx,
            stream_rx,
            me: None,
            fresh: false,
            listed: true,
            unsaved: false,
            publishing: false,
            relinking: false,
            last_switched_off: None,
            paused_for_no_network: false,
            tick_at: None,
            ui_due: None,
            checkpoint_due: None,
            ticked_at: 0,
            speed_sampled_at: 0,
            reported: None,
            gone: false,
        }
    }

    /// A download just started (status `Downloading` to begin at once, or `Queued`).
    pub(crate) fn new(
        ctx: Arc<Ctx>,
        state: DownloadState,
        request: StartRequest,
        staging: StagingFile,
    ) -> Self {
        let mut download = Self::with(ctx, state, request, staging);
        download.fresh = true;
        download
    }

    /// A download saved before a restart.
    pub(crate) fn restored(ctx: Arc<Ctx>, restored: RestoredDownload) -> Self {
        let RestoredDownload { state, request, staging_path, publication } = restored;
        let staging = StagingFile::attach(strip_staging_suffix(&staging_path));
        let mut download = Self::with(ctx, state, request, staging);
        download.publication = publication;
        download.reconcile();
        download
    }

    /// Saves a download just started, so that it survives a crash from the moment Start answers.
    pub(crate) async fn save_first(&mut self) {
        self.persist_now().await;
    }

    pub(crate) fn state(&self) -> &DownloadState {
        &self.state
    }

    /// Runs the download's task; returns its control channel.
    pub(crate) fn spawn(mut self) -> mpsc::UnboundedSender<Control> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.me = Some(tx.downgrade());
        self.reported = Some((self.state.status, self.state.queued_at, self.state.scheduled));
        tokio::spawn(self.run(rx));
        tx
    }

    async fn run(mut self, mut control: mpsc::UnboundedReceiver<Control>) {
        if std::mem::take(&mut self.fresh) {
            self.push_update(true);
        }
        while !self.gone {
            if self.transfer.is_stopping() {
                self.finish_run().await;
                continue;
            }
            let (tick_at, ui_due, checkpoint_due) =
                (self.tick_at, self.ui_due, self.checkpoint_due);
            let has_tasks = self.transfer.has_tasks();
            tokio::select! {
                message = control.recv() => match message {
                    Some(message) => self.on_control(message).await,
                    None => {
                        self.close().await;
                        return;
                    }
                },
                Some(msg) = self.stream_rx.recv() => {
                    let now = self.ctx.clock.now();
                    self.transfer.on_message(&mut self.state, now, msg);
                    self.after_effects();
                }
                Some(id) = self.transfer.join_next(), if has_tasks => {
                    self.drain_streams();
                    self.transfer.on_stream_exit(&mut self.state, id);
                    self.after_effects();
                }
                () = sleep_until_opt(tick_at) => self.on_tick(),
                () = sleep_until_opt(ui_due) => self.push_update(true),
                () = sleep_until_opt(checkpoint_due) => self.checkpoint(),
            }
        }
    }

    async fn on_control(&mut self, message: Control) {
        match message {
            Control::Begin { scheduled } => self.begin(scheduled),
            Control::Enqueue { queued_at, scheduled } => self.enqueue(queued_at, scheduled).await,
            Control::PauseForSchedule { queued_at } => self.pause(Some(queued_at)).await,
            Control::Pause => self.pause(None).await,
            Control::Resume => self.resume(false, None).await,
            Control::Ack(ack) => {
                let _ = ack.send(Ok(()));
            }
            Control::Cancel => self.cancel().await,
            Control::Remove { trash_file } => self.remove(trash_file).await,
            Control::Relink { url, reply } => self.relink(url, reply),
            Control::RelinkChecked { result, reply } => self.relink_checked(result, reply).await,
            Control::SetNetwork { network, enabled } => self.set_network(network, enabled).await,
            Control::SetScheduled(scheduled) => {
                let enrolled = matches!(
                    self.state.status,
                    DownloadStatus::Queued | DownloadStatus::Downloading
                );
                if enrolled && self.state.scheduled != scheduled {
                    self.state.scheduled = scheduled;
                    self.push_update(true);
                    self.persist_now().await;
                }
            }
            Control::Reconcile => {
                self.reconcile();
                self.after_effects();
            }
            Control::NetworksChanged { changed, moved } => {
                self.reconcile();
                self.transfer.wake(|id| changed.contains(id), |id| moved.contains(id));
                self.after_effects();
            }
            Control::SystemResumed => {
                if self.transfer.is_running() {
                    self.transfer.system_resumed(self.ctx.clock.now());
                    self.after_effects();
                }
            }
            Control::Reveal => self.reveal().await,
            Control::Open => self.open().await,
            Control::Suspend { queued_at, reply } => {
                if self.state.status == DownloadStatus::Downloading {
                    self.pause(queued_at).await;
                } else if self.listed && !self.unsaved {
                    self.persist_now().await;
                }
                let _ = reply.send(());
            }
        }
    }

    // --- telling the UI and the manager ---------------------------------------------------------

    /// Applies what the transfer asked for.
    fn after_effects(&mut self) {
        // A reconcile can ask for another (a network failing as it is looked at); a few rounds
        // settle it.
        for _ in 0..4 {
            let effects = std::mem::take(&mut self.transfer.effects);
            if let Some((title, body)) = effects.notice {
                let _ = self.ctx.events.send(Event::Notice { title, body });
            }
            if effects.dirty {
                self.schedule_update();
            }
            if !effects.reconcile {
                break;
            }
            self.reconcile();
        }
        if self.reported.is_some_and(|(status, ..)| status != self.state.status) {
            // A status change is shown at once.
            self.push_update(true);
        }
    }

    fn schedule_update(&mut self) {
        if self.ui_due.is_none() {
            self.ui_due = Some(Instant::now() + self.ctx.tuning.ui_update);
        }
    }

    /// Sends the download's state to the UI now, and tells the manager if its place changed.
    fn push_update(&mut self, persist: bool) {
        self.ui_due = None;
        if !self.listed {
            return;
        }
        if matches!(self.state.status, DownloadStatus::Paused | DownloadStatus::Cancelled) {
            clear_speeds(&mut self.state);
        }
        if persist && !self.unsaved && self.checkpoint_due.is_none() {
            self.checkpoint_due = Some(Instant::now() + self.ctx.tuning.checkpoint_interval);
        }
        let _ = self.ctx.events.send(Event::DownloadUpdated(Arc::new(self.state.clone())));
        self.report_place(false);
    }

    /// Tells the manager the download's status and queue place, if they changed (or `always`).
    fn report_place(&mut self, always: bool) {
        let place = (self.state.status, self.state.queued_at, self.state.scheduled);
        if always || self.reported != Some(place) {
            self.reported = Some(place);
            let (status, queued_at, scheduled) = place;
            let _ = self.ctx.notes.send(Note::Status {
                id: self.state.id,
                status,
                queued_at,
                scheduled,
            });
        }
    }

    fn snapshot(&self) -> Snapshot {
        Snapshot {
            state: self.state.clone(),
            request: self.request.clone(),
            staging: self.staging.clone(),
            publication: self.publication.clone(),
        }
    }

    /// A routine checkpoint: queued, not waited for.
    fn checkpoint(&mut self) {
        self.checkpoint_due = None;
        if !self.unsaved {
            self.persister.save_later(self.snapshot());
        }
    }

    /// Saves now and waits until it is on disk.
    async fn persist_now(&mut self) -> bool {
        self.checkpoint_due = None;
        if self.unsaved {
            return false;
        }
        self.persister.save(self.snapshot()).await.is_ok()
    }

    // --- running -------------------------------------------------------------------------------

    /// Starts a run of a paused, failed or queued download, from where it stopped.
    fn begin(&mut self, scheduled: bool) {
        let state = &mut self.state;
        let startable = match state.status {
            DownloadStatus::Queued | DownloadStatus::Paused => true,
            DownloadStatus::Error => state.resumable,
            // Fresh from Start, its run not begun yet.
            DownloadStatus::Downloading => !self.transfer.is_running(),
            _ => false,
        };
        if !startable || self.transfer.is_running() || !self.listed {
            // The manager thought otherwise: set it right.
            self.report_place(true);
            return;
        }
        state.scheduled = scheduled;
        state.status = DownloadStatus::Downloading;
        state.queued_at = None;
        state.error = None;
        state.resumable = true;
        self.paused_for_no_network = false;
        // Paused by switching off every network: resuming switches back on the one switched off
        // last if it is still there, or else the first one present.
        if !state.networks.iter().any(|n| n.enabled) {
            let present = self.ctx.networks.borrow().clone();
            let is_present = |n: &DownloadNetwork| present.iter().any(|i| i.id == n.id);
            let last = self.last_switched_off.as_ref();
            let again = state
                .networks
                .iter()
                .position(|n| Some(&n.id) == last && is_present(n))
                .or_else(|| state.networks.iter().position(is_present))
                .or_else(|| state.networks.iter().position(|n| Some(&n.id) == last))
                .or(if state.networks.is_empty() { None } else { Some(0) });
            if let Some(again) = again {
                state.networks[again].enabled = true;
            }
        }
        // A network that had failed, or couldn't get through, gets another go.
        for network in &mut state.networks {
            if matches!(network.status, NetworkStatus::Failed | NetworkStatus::Unreachable) {
                network.status = NetworkStatus::On;
                network.error = None;
            }
        }
        if let Some(paused_at) = state.paused_at.take() {
            state.total_paused_ms += (now_millis() - paused_at).max(0) as u64;
        }
        for block in &mut state.blocks {
            if block.status == BlockStatus::Downloading {
                block.status = BlockStatus::Pending;
            }
        }
        for stream in &mut state.streams {
            if stream.status != StreamStatus::Completed {
                stream.status = StreamStatus::Pending;
            }
            stream.speed_bps = 0.0;
        }

        let now = self.ctx.clock.now();
        self.transfer.start_run(&self.request, &self.staging, self.stream_tx.clone());
        self.peak.resume(now, self.state.bytes_downloaded);
        self.ticked_at = now;
        self.tick_at = Some(Instant::now() + self.ctx.tuning.tick);
        self.reconcile();
        if self.state.blocks.iter().all(|b| b.status == BlockStatus::Completed) {
            // Everything is in already (a publish that failed): straight to publishing.
            self.transfer.stop();
        }
        self.push_update(true);
        self.after_effects();
    }

    /// Takes stock: speeds, the speed history, stuck connections, networks and stream counts.
    fn on_tick(&mut self) {
        self.tick_at = None;
        if !self.transfer.is_running() || self.transfer.is_stopping() {
            return;
        }
        let now = self.ctx.clock.now();
        let elapsed = (now - self.ticked_at).max(0) as f64 / 1000.0;
        self.ticked_at = now;
        // The only place speeds are read: on this clock, never as bytes arrive.
        if self.transfer.update_speeds(&mut self.state, now, elapsed) {
            self.schedule_update();
        }
        if now - self.speed_sampled_at >= 1000 {
            self.speed_sampled_at = now;
            sample_speeds(&mut self.state.speed_history, &self.state.networks);
            let active = self.state.active_ms(now_millis());
            self.peak.sample(now, self.state.bytes_downloaded, active);
            self.state.peak_speed_bps = self.peak.peak();
            self.schedule_update();
        }
        self.transfer.refresh_stuck(&mut self.state, now);
        self.reconcile();
        self.transfer.adjust_streams(&mut self.state, now);
        self.transfer.dispatch_waiting(&mut self.state, now);
        self.after_effects();
        self.tick_at = Some(Instant::now() + self.ctx.tuning.tick);
    }

    /// Brings the download's networks up to date with the computer's, and, while it runs, each
    /// network's streams in line with what it can do now. Safe to run any time.
    fn reconcile(&mut self) {
        let present = self.ctx.networks.borrow().clone();
        let limits = self.ctx.limits.clone();
        let now = chrono::Local::now();
        let at_limit = |id: &NetworkId| {
            limits.lock().map(|mut limits| limits.limit_reached(id, &now)).unwrap_or(false)
        };
        if networks::reconcile(&mut self.state.networks, &self.state.streams, &present, at_limit) {
            self.schedule_update();
        }
        if self.state.status != DownloadStatus::Downloading || self.transfer.is_stopping() {
            return;
        }
        let enabled: Vec<&DownloadNetwork> =
            self.state.networks.iter().filter(|n| n.enabled).collect();
        if !enabled.is_empty() && enabled.iter().all(|n| n.status == NetworkStatus::Failed) {
            let message = enabled[0]
                .error
                .clone()
                .unwrap_or_else(|| "No network could reach the server".into());
            self.transfer.fail_download(&mut self.state, &message, false);
            return;
        }
        let now = self.ctx.clock.now();
        self.transfer.reconcile_streams(&mut self.state, now);
    }

    /// Stops the run's streams and waits for them, still taking in what they report.
    async fn halt(&mut self) {
        self.transfer.stop();
        while self.transfer.has_tasks() {
            tokio::select! {
                Some(msg) = self.stream_rx.recv() => {
                    let now = self.ctx.clock.now();
                    self.transfer.on_message(&mut self.state, now, msg);
                }
                Some(id) = self.transfer.join_next() => {
                    self.drain_streams();
                    self.transfer.on_stream_exit(&mut self.state, id);
                }
            }
        }
        self.drain_streams();
    }

    /// Handles every stream message already sent — all of an ended stream's, since it sent them
    /// before it ended.
    fn drain_streams(&mut self) {
        while let Ok(msg) = self.stream_rx.try_recv() {
            let now = self.ctx.clock.now();
            self.transfer.on_message(&mut self.state, now, msg);
        }
    }

    /// The run is over — every block in, or stopped. Its streams wind down, and then the
    /// download publishes, or is saved as it now is.
    async fn finish_run(&mut self) {
        self.halt().await;
        let now = self.ctx.clock.now();
        // Its last part reads too, up to the moment it stopped (and where the next run picks up).
        self.peak.sample(now, self.state.bytes_downloaded, self.state.active_ms(now_millis()));
        self.transfer.end_run(&mut self.state);
        self.tick_at = None;
        // However it ended, nothing is moving now.
        clear_speeds(&mut self.state);
        let notice = self.transfer.effects.notice.take();
        if let Some((title, body)) = notice {
            let _ = self.ctx.events.send(Event::Notice { title, body });
        }
        match self.state.status {
            DownloadStatus::Downloading => self.publish().await,
            DownloadStatus::Error => {
                self.push_update(true);
                // An error keeps what it has unless it can't be resumed.
                if !self.state.resumable {
                    let _ = self.staging.discard().await;
                }
                self.persist_now().await;
            }
            _ => {
                self.push_update(true);
                self.persist_now().await;
            }
        }
    }

    async fn publish(&mut self) {
        self.publishing = true;
        let result = self.try_publish().await;
        match result {
            Ok(path) => {
                let state = &mut self.state;
                state.file_name = path
                    .file_name()
                    .map_or_else(|| state.file_name.clone(), |n| n.to_string_lossy().into_owned());
                state.destination_path = path;
                state.status = DownloadStatus::Completed;
                state.completed_at = Some(now_millis());
                state.error = None;
                // Done before it downloaded long enough for a peak: its speed start to finish.
                state.peak_speed_bps = self.peak.finish();
                if let Some(total) = state.total_bytes {
                    state.bytes_downloaded = total;
                }
                self.persist_now().await;
                let body = format!("{} has finished downloading.", self.state.file_name);
                let _ =
                    self.ctx.events.send(Event::Notice { title: "Download complete".into(), body });
            }
            Err(message) => {
                self.state.status = DownloadStatus::Error;
                self.state.error = Some(message.clone());
                let body = format!("{}: {message}", self.state.file_name);
                let _ =
                    self.ctx.events.send(Event::Notice { title: "Download failed".into(), body });
            }
        }
        self.publishing = false;
        self.push_update(true);
        if self.state.status == DownloadStatus::Completed {
            self.move_to_history().await;
        } else {
            self.persist_now().await;
        }
    }

    async fn try_publish(&mut self) -> Result<std::path::PathBuf, String> {
        if self.state.blocks.iter().any(|b| b.status != BlockStatus::Completed) {
            return Err("Download is incomplete — refusing to publish the file".into());
        }
        if !self.persist_now().await {
            return Err("Couldn’t save the download’s progress before finishing it".into());
        }
        let staging_path = self.staging.path().to_path_buf();
        let identity = platform::blocking(move || sonic_store::file_identity(&staging_path))
            .await
            .ok_or("Couldn’t read the downloaded file")?
            .map_err(|e| format!("Couldn’t read the downloaded file: {e}"))?;
        // Where it is about to go, with the staging file's identity, saved right before each
        // rename is tried: if the app dies between the rename and the next save, the restore
        // finds the published file and knows the download completed, instead of failing it for
        // a missing staging file — even when the name taken was a `name (n)` fallback.
        let persister = self.persister.clone();
        let base = self.snapshot();
        let expected = self.state.total_bytes.unwrap_or(self.state.bytes_downloaded);
        let staging = self.staging.clone();
        let path = staging
            .publish_with(expected, |candidate| {
                let persister = persister.clone();
                let snapshot = Snapshot {
                    publication: Some(Publication { path: candidate, identity }),
                    ..base.clone()
                };
                async move { persister.save(snapshot).await }
            })
            .await
            .map_err(|e| e.to_string())?;
        self.publication = Some(Publication { path: path.clone(), identity });
        Ok(path)
    }

    /// A finished download leaves the list for history. Its saved state goes only once history
    /// has it; if history can't be written it stays, to be moved on the next launch.
    async fn move_to_history(&mut self) {
        let entry = finished(&self.state);
        if let Err(error) = self.ctx.store.add_to_history(&entry).await {
            tracing::warn!(%error, "could not add a finished download to history");
            return;
        }
        let _ = self.persister.remove().await;
        self.unsaved = true;
        self.listed = false;
        let _ = self.ctx.events.send(Event::DownloadRemoved(self.state.id));
        let _ = self.ctx.notes.send(Note::Finished { id: self.state.id });
    }

    // --- what the user asks ---------------------------------------------------------------------

    async fn enqueue(&mut self, queued_at: Millis, scheduled: bool) {
        let state = &mut self.state;
        let queueable = match state.status {
            DownloadStatus::Paused | DownloadStatus::Queued => true,
            DownloadStatus::Error => state.resumable,
            _ => false,
        };
        if !queueable || self.transfer.is_running() {
            self.report_place(true);
            return;
        }
        state.status = DownloadStatus::Queued;
        state.queued_at = Some(queued_at);
        state.scheduled = scheduled;
        // Time spent waiting isn't time spent downloading (begin adds it to total_paused_ms).
        state.paused_at.get_or_insert_with(now_millis);
        state.error = None;
        state.resumable = true;
        self.paused_for_no_network = false;
        self.push_update(true);
        self.persist_now().await;
    }

    /// Pauses a running or queued download. `for_schedule`: the schedule's window closed, so it
    /// stays queued at that place.
    async fn pause(&mut self, for_schedule: Option<Millis>) {
        let state = &mut self.state;
        match state.status {
            DownloadStatus::Queued if for_schedule.is_none() => {
                state.scheduled = false;
                state.status = DownloadStatus::Paused;
                state.queued_at = None;
                state.paused_at.get_or_insert_with(now_millis);
                self.push_update(true);
                self.persist_now().await;
                return;
            }
            DownloadStatus::Downloading if !self.publishing => {}
            _ => {
                self.report_place(true);
                return;
            }
        }
        match for_schedule {
            Some(queued_at) => {
                state.status = DownloadStatus::Queued;
                state.queued_at = Some(queued_at);
            }
            None => {
                state.status = DownloadStatus::Paused;
                state.scheduled = false;
            }
        }
        state.paused_at = Some(now_millis());
        clear_speeds(state);
        for stream in &mut state.streams {
            if stream.status != StreamStatus::Completed {
                stream.status = StreamStatus::Paused;
            }
            stream.block = None;
            stream.hedge = false;
        }
        for block in &mut state.blocks {
            if block.status == BlockStatus::Downloading {
                block.status = BlockStatus::Pending;
            }
        }
        self.transfer.reset();
        self.transfer.stop();
        self.push_update(true);
        self.finish_run().await;
    }

    /// Resumes a paused or failed download: checks it can, then asks the manager for room.
    /// `by_network`: switching a network back on, rather than Resume.
    async fn resume(&mut self, by_network: bool, ack: Option<Reply<()>>) {
        let done = |ack: Option<Reply<()>>| {
            if let Some(ack) = ack {
                let _ = ack.send(Ok(()));
            }
        };
        let state = &self.state;
        let resumable = match state.status {
            DownloadStatus::Paused => true,
            DownloadStatus::Error => state.resumable,
            _ => false,
        };
        if !resumable || self.relinking {
            return done(ack);
        }
        if !platform::exists(self.staging.path().to_path_buf()).await {
            self.state.error = Some(
                "The partial download file is unavailable. Reconnect the destination drive and try again."
                    .into(),
            );
            self.push_update(false);
            return done(ack);
        }
        // Switched back on, then off again: it stays paused.
        if by_network && !self.state.networks.iter().any(|n| n.enabled) {
            return done(ack);
        }
        let _ = self.ctx.notes.send(Note::WantsToRun { id: self.state.id, ack });
    }

    async fn cancel(&mut self) {
        let cancellable = matches!(
            self.state.status,
            DownloadStatus::Downloading
                | DownloadStatus::Paused
                | DownloadStatus::Queued
                | DownloadStatus::Error
        );
        if !cancellable || self.publishing {
            return;
        }
        // Stop writing first, but keep it listed until its files and saved state are gone.
        self.pause(None).await;
        let state = &mut self.state;
        if state.status != DownloadStatus::Error {
            state.status = DownloadStatus::Paused;
            state.queued_at = None;
            state.paused_at.get_or_insert_with(now_millis);
        }
        clear_speeds(state);
        let discarded = self.staging.discard().await;
        let result = match discarded {
            Ok(()) => self.persister.remove().await,
            Err(error) => Err(error),
        };
        match result {
            Ok(()) => {
                self.unsaved = true;
                self.state.status = DownloadStatus::Cancelled;
                for stream in &mut self.state.streams {
                    stream.status = StreamStatus::Cancelled;
                }
            }
            Err(error) => {
                // Cleanup may have removed only some of it: cancelling again, not resuming, is
                // what makes sense now.
                self.state.status = DownloadStatus::Error;
                self.state.error = Some(format!("Couldn’t delete what was downloaded: {error}"));
                self.state.resumable = false;
                self.persist_now().await;
            }
        }
        self.push_update(false);
    }

    /// Removes the download, cancelling it if under way. A finished one's file stays, unless
    /// `trash_file`: then it goes to the Recycle Bin.
    async fn remove(&mut self, trash_file: bool) {
        if matches!(
            self.state.status,
            DownloadStatus::Downloading
                | DownloadStatus::Paused
                | DownloadStatus::Queued
                | DownloadStatus::Error
        ) {
            self.cancel().await;
            if self.state.status != DownloadStatus::Cancelled {
                return;
            }
        }
        let completed = self.state.status == DownloadStatus::Completed;
        if trash_file && completed {
            let path = self.state.destination_path.clone();
            if platform::exists(path.clone()).await
                && let Some(Err(error)) = platform::blocking(move || platform::trash(&path)).await
            {
                tracing::warn!(%error, "could not move a download to the Recycle Bin");
            }
        }
        if !self.unsaved {
            let _ = self.persister.remove().await;
            self.unsaved = true;
        }
        if completed {
            // Kept listed only because history couldn't be written; it may be there after all.
            let _ = self.ctx.store.remove_from_history(&[self.state.id]).await;
            let _ = self.ctx.notes.send(Note::HistoryChanged);
        }
        if self.listed {
            self.listed = false;
            let _ = self.ctx.events.send(Event::DownloadRemoved(self.state.id));
        }
        let _ = self.ctx.notes.send(Note::Removed { id: self.state.id });
        self.gone = true;
    }

    /// Switches one of the download's networks on or off, running or paused. Switching off the
    /// last network in use pauses it; switching one back on then resumes it.
    async fn set_network(&mut self, id: NetworkId, enabled: bool) {
        let status = self.state.status;
        if !matches!(
            status,
            DownloadStatus::Downloading | DownloadStatus::Paused | DownloadStatus::Queued
        ) {
            return;
        }
        let networks = &mut self.state.networks;
        let Some(index) = networks.iter().position(|n| n.id == id) else { return };
        if networks[index].enabled == enabled {
            return;
        }
        let was_last =
            !enabled && !networks.iter().enumerate().any(|(i, n)| i != index && n.enabled);
        // A download that can't be split runs over one network: switching one on switches it over.
        if enabled && !splittable(&self.request) {
            for network in networks.iter_mut() {
                network.enabled = false;
            }
        }
        networks[index].enabled = enabled;
        if !enabled {
            self.last_switched_off = Some(id);
        }
        if was_last && status == DownloadStatus::Downloading {
            self.paused_for_no_network = true;
            self.pause(None).await;
            if self.state.status != DownloadStatus::Paused {
                self.paused_for_no_network = false;
            }
            return;
        }
        self.reconcile();
        self.after_effects();
        self.push_update(true);
        if enabled && status == DownloadStatus::Paused && self.paused_for_no_network {
            self.resume(true, None).await;
        }
    }

    /// Gives a download whose link stopped working (a signed link ran out, say) a fresh link to
    /// the same file, and resumes it. The check runs off this task; see `relink_checked`.
    fn relink(&mut self, url: String, reply: Reply<()>) {
        if !self.state.resumable {
            let _ = reply.send(Err("This download can’t resume. Start it again.".into()));
            return;
        }
        if !self.replaceable() {
            let _ = reply.send(Err("Pause the download before replacing its link.".into()));
            return;
        }
        if self.relinking {
            let _ = reply.send(Err("A new link for this download is being checked.".into()));
            return;
        }
        let Some(me) = self.me.as_ref().and_then(mpsc::WeakUnboundedSender::upgrade) else {
            let _ = reply.send(Err("The download is closing.".into()));
            return;
        };
        self.relinking = true;
        let check = RelinkCheck {
            ctx: self.ctx.clone(),
            url,
            // A fresh link from the same site usually needs the same sign-in.
            browser: self.request.browser.clone(),
            total_bytes: self.state.total_bytes,
            splittable: splittable(&self.request),
            picks: stream::sample_picks(
                self.state.blocks.iter().map(|b| (b.range.start, b.bytes_downloaded)),
            ),
            staging: self.staging.clone(),
            enabled: self
                .state
                .networks
                .iter()
                .filter(|n| n.enabled)
                .map(|n| n.id.clone())
                .collect(),
        };
        tokio::spawn(async move {
            let result = check.run().await;
            let _ = me.send(Control::RelinkChecked { result, reply });
        });
    }

    fn replaceable(&self) -> bool {
        self.listed
            && self.state.resumable
            && matches!(self.state.status, DownloadStatus::Error | DownloadStatus::Paused)
    }

    async fn relink_checked(
        &mut self,
        result: Result<(String, Option<BrowserContext>), String>,
        reply: Reply<()>,
    ) {
        self.relinking = false;
        let (url, browser) = match result {
            Ok(checked) => checked,
            Err(message) => {
                let _ = reply.send(Err(message));
                return;
            }
        };
        if !self.replaceable() {
            let _ = reply.send(Err("The download changed while its new link was checked.".into()));
            return;
        }
        self.request.url.clone_from(&url);
        self.request.browser = browser;
        self.state.url = url;
        self.transfer.relinked(&self.request);
        self.persist_now().await;
        // Answered once it has resumed (or queued), so whoever asked sees it under way.
        self.resume(false, Some(reply)).await;
    }

    /// Shows the download's file in its folder: while it's unfinished, its staging file.
    async fn reveal(&mut self) {
        let unfinished = self.state.status != DownloadStatus::Completed;
        let candidates = [
            unfinished.then(|| self.staging.path().to_path_buf()),
            Some(self.state.destination_path.clone()),
        ];
        for path in candidates.into_iter().flatten() {
            if platform::exists(path.clone()).await {
                if let Some(Err(error)) = platform::blocking(move || platform::reveal(&path)).await
                {
                    tracing::warn!(%error, "could not show a download in its folder");
                }
                return;
            }
        }
        let _ = self.ctx.notes.send(Note::HistoryChanged);
    }

    async fn open(&mut self) {
        let path = self.state.destination_path.clone();
        if self.state.status == DownloadStatus::Completed && platform::exists(path.clone()).await {
            if let Some(Err(error)) = platform::blocking(move || platform::open_file(&path)).await {
                tracing::warn!(%error, "could not open a download");
            }
            return;
        }
        let _ = self.ctx.notes.send(Note::HistoryChanged);
    }

    /// The manager let go of it (the app is closing): stop any run and save where it got to.
    async fn close(&mut self) {
        if self.transfer.is_running() {
            self.halt().await;
            self.transfer.end_run(&mut self.state);
            clear_speeds(&mut self.state);
            if self.listed {
                self.persist_now().await;
            }
        }
    }
}

/// What checking a new link needs, taken off the download's task.
struct RelinkCheck {
    ctx: Arc<Ctx>,
    url: String,
    browser: Option<BrowserContext>,
    total_bytes: Option<u64>,
    splittable: bool,
    picks: Vec<(u64, u64)>,
    staging: StagingFile,
    enabled: Vec<NetworkId>,
}

impl RelinkCheck {
    async fn run(self) -> Result<(String, Option<BrowserContext>), String> {
        let tuning = &self.ctx.tuning;
        let probe =
            crate::probe_link(&self.ctx.networks, tuning, &self.url, self.browser.as_ref()).await?;
        if probe.total_bytes != self.total_bytes {
            let size = |bytes: Option<u64>| {
                bytes.map_or_else(|| "an unknown size".into(), crate::util::gigabytes)
            };
            return Err(format!(
                "That link is to a different file: {}, not {}",
                size(probe.total_bytes),
                size(self.total_bytes)
            ));
        }
        if self.splittable && !probe.supports_ranges {
            return Err("This server doesn’t support resuming downloads. Try another link to the same file.".into());
        }
        // An ETag or date from another link proves nothing about its bytes: two different files
        // can share them. Once resumed, only a label this download hasn't seen would make it
        // compare bytes, so they're compared now, before anything from the new link is written.
        let verdict = if self.picks.is_empty() {
            // Nothing downloaded, nothing a different file could spoil.
            Verdict::Same
        } else {
            let present = self.ctx.networks.borrow().clone();
            let Some(network) =
                present.iter().find(|i| self.enabled.contains(&i.id)).or(present.first())
            else {
                return Err("No network is connected.".into());
            };
            let mut conn = stream::connection(
                &self.ctx.networks,
                &network.id,
                tuning.stall_timeout,
                tuning.connect_timeout,
            );
            let job = SampleJob {
                picks: self.picks,
                label: None,
                source: Arc::new(Source {
                    url: probe.final_url.clone(),
                    browser: self.browser.clone(),
                    accepted: Vec::new(),
                }),
            };
            stream::compare_samples(&job, &self.staging, &mut conn, tuning.stall_timeout).await
        };
        match verdict {
            Verdict::Same => Ok((probe.final_url, self.browser)),
            Verdict::Different => {
                Err("That link is to a different file: its bytes don’t match what’s downloaded."
                    .into())
            }
            Verdict::Unknown => {
                Err("SonicNet couldn’t check that link against what’s downloaded. Try again."
                    .into())
            }
        }
    }
}

/// Nothing is moving: a paused or stopped download reads 0 everywhere.
fn clear_speeds(state: &mut DownloadState) {
    state.speed_bps = 0.0;
    state.time_left_secs = None;
    state.disk_limited = false;
    for stream in &mut state.streams {
        stream.speed_bps = 0.0;
    }
    for network in &mut state.networks {
        network.speed_bps = 0.0;
    }
}

/// `<final>.sonic` → `<final>`: how a saved staging path finds its download's final path.
fn strip_staging_suffix(path: &std::path::Path) -> std::path::PathBuf {
    let text = path.as_os_str().to_string_lossy();
    match text.strip_suffix(sonic_core::STAGING_SUFFIX) {
        Some(stem) => std::path::PathBuf::from(stem),
        None => path.to_path_buf(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_staging_path_names_its_final_path() {
        let staging = std::path::Path::new(r"C:\Downloads\setup (1).exe.sonic");
        assert_eq!(
            strip_staging_suffix(staging),
            std::path::Path::new(r"C:\Downloads\setup (1).exe")
        );
    }

    #[test]
    fn clearing_speeds_zeroes_everything_that_moves() {
        let mut state = crate::tests::state();
        state.speed_bps = 5.0;
        state.time_left_secs = Some(3.0);
        state.disk_limited = true;
        state.networks[0].speed_bps = 5.0;
        clear_speeds(&mut state);
        assert_eq!(state.speed_bps, 0.0);
        assert_eq!(state.time_left_secs, None);
        assert!(!state.disk_limited);
        assert_eq!(state.networks[0].speed_bps, 0.0);
    }
}
