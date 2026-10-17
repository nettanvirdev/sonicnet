//! One download's streams and the attempts they make — the port of Plexo's `httpTransfer.ts`.
//!
//! A stream's life is a loop: ask for work, run it as an attempt, report what became of it. The
//! asking and the reporting arrive here, on the download's own task, and every change to the
//! download's state happens in these synchronous handlers: a stream never sees another's
//! half-finished change, exactly as on Plexo's single thread.
//!
//! An attempt is one request by one stream, for one block — or, for a primary, a run of blocks
//! leased to it, written one after another and finished as it goes (see `lease_run`); it is at
//! one block at a time. A block normally has one (primary) attempt; near the end of a download a
//! second (hedge) one may race it (see
//! `sonic_engine::scheduler`), and the block is finished by whichever gets there first. Both write
//! straight into the staging file at the block's own offsets: every response is checked to be the
//! download's version before any of it is written, so racing attempts write identical bytes and
//! it doesn't matter which lands last.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use sonic_core::{
    BlockIndex, BlockStatus, DownloadState, DownloadStatus, FileVersion, Millis, NetworkId,
    NetworkStatus, StartRequest, StreamId, StreamState, StreamStatus, VersionCheck,
    compare_version,
};
use sonic_engine::concurrency::{
    Action, ConcurrencyController, DiskReading, DiskWatch, NetworkSnapshot, Snapshot,
};
use sonic_engine::meter::{Meters, time_left};
use sonic_engine::plan::{interleave, starting_streams};
use sonic_engine::progress::{advance_block, retract_block};
use sonic_engine::scheduler::{
    AttemptKind, AttemptView, Requester, SchedulerPolicy, SchedulerView, Work, pick_work,
};
use sonic_http::{ConnectionError, FetchError, StagingFile};
use sonic_net::DialError;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::stream::{
    self, Body, Job, Order, Progress, Report, SampleJob, Source, StreamEnv, StreamMsg,
    StreamShared, Verdict, sample_picks,
};
use crate::Ctx;
use crate::util::{backoff, jitter};

/// A server answering wrongly this many times in a row makes a stream give up.
const MAX_STRIKES: u32 = 5;
/// A network that can't reach the server keeps one stream asking (see `reconcile_streams`). That
/// is one cheap connection, so it asks often enough to find out soon after the network gets
/// through.
const UNREACHABLE_RETRY: Duration = Duration::from_secs(5);
/// The most a server's Retry-After is honoured for.
const RETRY_AFTER_MAX: Duration = Duration::from_secs(120);
/// A single TCP stream can get stuck at a crawl (loss-collapsed congestion window, a bad CDN node
/// or route) while its siblings on the same network run fine. It never goes silent, so the stall
/// watchdog can't catch it; a fresh connection usually lands somewhere healthy. Only relative
/// thresholds — nothing here knows how fast the network ought to be.
const SLOW_RATIO: f64 = 0.1;
/// Bounds every case where refreshing can't help (the whole network got slower, a stale
/// reference): at worst a block pays for a couple of cheap reconnects, then is left alone.
const MAX_REFRESHES_PER_BLOCK: u32 = 2;
const MAX_HEDGES_PER_BLOCK: u32 = 2;

/// Why an attempt was called off by the download rather than by a pause or a failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AbortReason {
    /// Its connection is stuck: drop it; the block resumes on a fresh one. Not a failure.
    Refresh,
    /// Another attempt finished its block first.
    Lost,
}

#[derive(Debug)]
struct Attempt {
    serial: u64,
    kind: AttemptKind,
    block: BlockIndex,
    stream: StreamId,
    network: NetworkId,
    /// Where the block it is writing now starts, in the file.
    block_start: u64,
    /// Where its request begins, in the file.
    request_start: u64,
    /// A primary's request runs on through the blocks after its own up to this one (exclusive),
    /// each leased to it: one request for a stream's share of the file rather than one per block
    /// (see `lease_run`). `block` moves on through them as it writes.
    run_end: BlockIndex,
    /// Where its request stops, in the file: the end of its run, brought forward when part of the
    /// run is handed to another stream (see `split_run`).
    cut: Arc<AtomicU64>,
    /// Its run was split: its request stops short of the body's end, and its connection closes.
    split: bool,
    /// Bytes the staging file has accepted, from `request_start`: safe to resume from this prefix.
    received: u64,
    /// Bytes the network has delivered, whatever the disk does with them.
    network_received: u64,
    last_network_at: Millis,
    /// From when silence on it counts: its start, its last byte, or the end of a wait the disk or
    /// a speed limit held its reading for. Neither wait says anything about the connection.
    quiet_from: Millis,
    write_waiting: bool,
    started_at: Millis,
    /// Whoever held the block before: whose tail bytes a retraction removes first.
    previous_writer: Option<NetworkId>,
    cancel: CancellationToken,
    abort: Option<AbortReason>,
}

impl Attempt {
    /// How far into its current block it has written.
    fn position(&self) -> u64 {
        (self.request_start + self.received).saturating_sub(self.block_start)
    }

    fn view(&self) -> AttemptView {
        AttemptView {
            kind: self.kind,
            stream: self.stream,
            network: self.network.clone(),
            started_at: self.started_at,
            write_waiting: self.write_waiting,
        }
    }
}

/// What became of an attempt's request.
#[derive(Debug)]
enum Outcome {
    Completed,
    /// The download was paused, stopped, or the stream retired, underneath it.
    Stopped,
    Aborted(AbortReason),
    Failed(FetchError),
}

/// A failed request whose verdict is being settled by comparing samples.
#[derive(Debug)]
struct PendingFailure {
    serial: u64,
    error: FetchError,
    seen: FileVersion,
}

/// What a download keeps about each of its streams.
#[derive(Debug)]
struct StreamRt {
    network: NetworkId,
    shared: Arc<StreamShared>,
    /// Stops this stream alone (retirement); a child of the run's.
    cancel: CancellationToken,
    /// Its task is running.
    running: bool,
    /// The serial of the attempt it is making, if any.
    attempt: Option<u64>,
    /// Asked for work when there was none: answered once there may be some.
    waiting: Option<oneshot::Sender<Order>>,
    /// Waiting out a failed request or a server's hold, since then. Asked again after it, it
    /// starts afresh.
    asleep_since: Option<Millis>,
    /// A remote-change verdict it is settling.
    pending: Option<PendingFailure>,
    /// When its connection last (re)connected; not judged slow until `slow_warmup` after.
    warm_since: Millis,
    slow_since: Option<Millis>,
    /// Speed of its last finished block, start to end; 0 until it finishes one.
    last_block_speed: f64,
    /// Everything it received, duplicates included: whether it has been answered at all.
    received_bytes: u64,
    /// Failed attempts in a row, for backoff.
    failures: u32,
    /// Of those, the ones the server answered wrongly (see `MAX_STRIKES`).
    strikes: u32,
    /// When the server first answered busy since this stream last made progress.
    busy_since: Option<Millis>,
    /// Its last request was cut short, closing its connection, and nothing has come over a new
    /// one yet: a server counting connections may still count the old one for a moment, so a
    /// refusal now says nothing about how many it serves.
    reconnecting: bool,
    /// Since the stream count last looked: refused by the server — with how many of its network's
    /// streams were asking at the time, the fewest if more than once (see `asking`) — and
    /// whether it received something.
    refused: Option<u32>,
    served: bool,
    /// Stopped for good; leaves the list once its task has ended.
    retiring: bool,
}

impl StreamRt {
    fn new(network: NetworkId) -> Self {
        Self {
            network,
            shared: Arc::default(),
            cancel: CancellationToken::new(),
            running: false,
            attempt: None,
            waiting: None,
            asleep_since: None,
            pending: None,
            warm_since: 0,
            slow_since: None,
            last_block_speed: 0.0,
            received_bytes: 0,
            failures: 0,
            strikes: 0,
            busy_since: None,
            reconnecting: false,
            refused: None,
            served: false,
            retiring: false,
        }
    }

    /// The server turned one of its requests away while `asking` of its network's streams were
    /// asking: the stream count takes it as a limit.
    fn refuse(&mut self, asking: u32) {
        self.refused = Some(self.refused.map_or(asking, |before| before.min(asking)));
    }
}

/// By network: when it last showed it was alive (received something, or came into use).
#[derive(Clone, Copy, Debug, Default)]
struct Traffic {
    alive_at: Millis,
}

/// One run's machinery: from Begin to the moment its streams have all stopped.
#[derive(Debug)]
struct Run {
    cancel: CancellationToken,
    tasks: JoinSet<StreamId>,
    env: Arc<StreamEnv>,
    stopping: bool,
}

/// What the download must do about something the transfer did.
#[derive(Debug, Default)]
pub(crate) struct Effects {
    /// Its state changed: show it soon.
    pub dirty: bool,
    /// Its networks need another look (one became unreachable, failed, or hit its limit).
    pub reconcile: bool,
    pub notice: Option<(String, String)>,
}

/// What to answer a stream that reported.
enum Next {
    /// Give it the next work there is.
    Dispatch,
    Reply(Order),
}

#[derive(Debug)]
pub(crate) struct Transfer {
    ctx: Arc<Ctx>,
    id: sonic_core::DownloadId,
    splittable: bool,
    supports_ranges: bool,
    /// The count each network starts with when it joins: a test's fixed count, the user's pick,
    /// or Auto's.
    picked: Option<u32>,
    /// Shares networks with other downloads (Auto, splittable).
    shares: bool,
    concurrency: Option<ConcurrencyController>,
    accepted: Vec<FileVersion>,
    source: Arc<Source>,
    disk: DiskWatch,
    /// Each network's status as reconcile last left it: how a network that just came into use
    /// is told apart. Cleared every run.
    reconciled: HashMap<NetworkId, NetworkStatus>,
    /// For a block whose last attempt delivered nothing, the network that made it (see the
    /// scheduler). Only steers the next hand-out; cleared every run.
    avoid: HashMap<BlockIndex, NetworkId>,
    hedges: HashMap<BlockIndex, u32>,
    refreshes: HashMap<BlockIndex, u32>,
    traffic: HashMap<NetworkId, Traffic>,
    /// Until when the server asked (Retry-After) not to be sent new requests over a network.
    hold_until: HashMap<NetworkId, Millis>,
    attempts: BTreeMap<BlockIndex, Vec<Attempt>>,
    rts: BTreeMap<StreamId, StreamRt>,
    next_stream: StreamId,
    next_serial: u64,
    meters: Meters,
    run: Option<Run>,
    pub effects: Effects,
}

/// Whether `request` can be fetched in parts by many streams.
pub(crate) fn splittable(request: &StartRequest) -> bool {
    request.splittable()
}

/// The streams per network the user picked, when it is a count they may pick.
fn picked_streams(request: &StartRequest) -> Option<u32> {
    request.streams_per_network.filter(|n| (1..=sonic_core::MAX_STREAMS_PER_NETWORK).contains(n))
}

impl Transfer {
    pub(crate) fn new(ctx: Arc<Ctx>, id: sonic_core::DownloadId, request: &StartRequest) -> Self {
        let splittable = splittable(request);
        let fixed = ctx.tuning.fixed_streams;
        let user = picked_streams(request);
        // Only a download that can split works out a count, and not while a test fixes it.
        let concurrency = match (splittable, fixed, user) {
            (false, _, _) | (_, Some(_), _) => None,
            (true, None, Some(picked)) => Some(ConcurrencyController::fixed(picked)),
            (true, None, None) => Some(ConcurrencyController::auto()),
        };
        let accepted = vec![FileVersion {
            etag: request.etag.clone(),
            last_modified: request.last_modified.clone(),
            total_bytes: request.total_bytes,
        }];
        let source = Arc::new(Source {
            url: request.url.clone(),
            browser: request.browser.clone(),
            accepted: accepted.clone(),
        });
        Self {
            ctx,
            id,
            splittable,
            supports_ranges: request.supports_ranges,
            picked: fixed.or(user),
            shares: splittable && fixed.is_none() && user.is_none(),
            concurrency,
            accepted,
            source,
            disk: DiskWatch::new(),
            reconciled: HashMap::new(),
            avoid: HashMap::new(),
            hedges: HashMap::new(),
            refreshes: HashMap::new(),
            traffic: HashMap::new(),
            hold_until: HashMap::new(),
            attempts: BTreeMap::new(),
            rts: BTreeMap::new(),
            next_stream: 0,
            next_serial: 0,
            meters: Meters::default(),
            run: None,
            effects: Effects::default(),
        }
    }

    // --- the run -------------------------------------------------------------------------------

    pub(crate) fn is_running(&self) -> bool {
        self.run.is_some()
    }

    /// Stopped, with streams still winding down (or about to be reaped).
    pub(crate) fn is_stopping(&self) -> bool {
        self.run.as_ref().is_some_and(|run| run.stopping)
    }

    fn live_run(&self) -> bool {
        self.run.as_ref().is_some_and(|run| !run.stopping)
    }

    /// Starts a run: streams join through `reconcile_streams`.
    pub(crate) fn start_run(
        &mut self,
        request: &StartRequest,
        staging: &StagingFile,
        tx: tokio::sync::mpsc::UnboundedSender<StreamMsg>,
    ) {
        self.reset();
        // Speeds are a run's own: nothing carries over from the last one.
        self.meters.clear();
        self.source = Arc::new(Source {
            url: request.url.clone(),
            browser: request.browser.clone(),
            accepted: self.accepted.clone(),
        });
        let env = Arc::new(StreamEnv {
            staging: staging.clone(),
            networks: self.ctx.networks.clone(),
            limits: self.ctx.limits.clone(),
            stall_timeout: self.ctx.tuning.stall_timeout,
            connect_timeout: self.ctx.tuning.connect_timeout,
            tx,
        });
        self.run = Some(Run {
            cancel: CancellationToken::new(),
            tasks: JoinSet::new(),
            env,
            stopping: false,
        });
    }

    /// Stops every stream. The run ends once their tasks have (see `join_next`).
    pub(crate) fn stop(&mut self) {
        let Some(run) = &mut self.run else { return };
        run.stopping = true;
        run.cancel.cancel();
        for rt in self.rts.values_mut() {
            rt.waiting = None;
        }
    }

    /// The next stream task to end, while any runs.
    pub(crate) async fn join_next(&mut self) -> Option<StreamId> {
        let run = self.run.as_mut()?;
        loop {
            match run.tasks.join_next().await? {
                Ok(id) => return Some(id),
                // A stream task that panicked: nothing to tell but that it ended.
                Err(error) => tracing::error!(%error, "a stream task failed"),
            }
        }
    }

    pub(crate) fn has_tasks(&self) -> bool {
        self.run.as_ref().is_some_and(|run| !run.tasks.is_empty())
    }

    /// The run is over: every stream task has ended and its reports are in.
    pub(crate) fn end_run(&mut self, state: &mut DownloadState) {
        self.run = None;
        self.attempts.clear();
        for rt in self.rts.values_mut() {
            rt.running = false;
            rt.attempt = None;
            rt.waiting = None;
            rt.pending = None;
        }
        // Retired streams whose tasks ended leave the list.
        let retired: Vec<StreamId> =
            self.rts.iter().filter(|(_, rt)| rt.retiring).map(|(id, _)| *id).collect();
        for id in retired {
            self.remove_stream(state, id);
        }
        self.register_share(state);
    }

    /// Forgets what only steers the current run. Done on a pause, a resume and as a run starts.
    pub(crate) fn reset(&mut self) {
        self.avoid.clear();
        self.reconciled.clear();
        self.disk.reset();
    }

    /// The URL or browser changed (a new link): the next run fetches from there.
    pub(crate) fn relinked(&mut self, request: &StartRequest) {
        self.supports_ranges = request.supports_ranges;
    }

    // --- streams -------------------------------------------------------------------------------

    /// A network's streams that aren't on their way out.
    fn live_streams(&self, state: &DownloadState, network: &NetworkId) -> Vec<StreamId> {
        state
            .streams
            .iter()
            .filter(|s| &s.network_id == network)
            .filter(|s| self.rts.get(&s.id).is_none_or(|rt| !rt.retiring))
            .map(|s| s.id)
            .collect()
    }

    /// How many of `network`'s streams were asking the server for something — running, and not
    /// waiting out a failed request or a server's Retry-After — at some point since `since`, when
    /// a request now refused was sent: a request out on each, or about to be, and a connection
    /// open. A server turning that request away is saying it serves fewer than this many from
    /// this address, whichever of them it happened to refuse (see `sonic_engine::concurrency`).
    ///
    /// Streams that went to sleep since count too: refusals sent together (a burst, as when a
    /// server refuses every request while too many are open) arrive here one after another, and
    /// each puts its stream to sleep before the next is handled, though the server saw them all
    /// at once. This errs high, which only costs another refused request before the count is
    /// right; erring low would cost a minute per stream.
    fn asking(&self, state: &DownloadState, network: &NetworkId, since: Millis) -> u32 {
        let asking = self.live_streams(state, network).into_iter().filter(|id| {
            self.rts
                .get(id)
                .is_some_and(|rt| rt.running && rt.asleep_since.is_none_or(|at| at >= since))
        });
        u32::try_from(asking.count()).unwrap_or(u32::MAX)
    }

    fn traffic(&mut self, network: &NetworkId) -> &mut Traffic {
        self.traffic.entry(network.clone()).or_default()
    }

    /// Running downloads on Auto that have `network` on, this one included.
    fn downloads_on(&self, network: &NetworkId) -> u32 {
        let shares = self.ctx.shares.lock().unwrap_or_else(|e| e.into_inner());
        shares.values().filter(|networks| networks.contains(network)).count() as u32
    }

    /// Tells the other downloads which networks this one shares with them now.
    fn register_share(&self, state: &DownloadState) {
        let mut shares = self.ctx.shares.lock().unwrap_or_else(|e| e.into_inner());
        if self.shares && self.live_run() && state.status == DownloadStatus::Downloading {
            let on = state
                .networks
                .iter()
                .filter(|n| n.status == NetworkStatus::On)
                .map(|n| n.id.clone())
                .collect();
            shares.insert(self.id, on);
        } else {
            shares.remove(&self.id);
        }
    }

    /// Each network's streams in line with what it can do now:
    /// - A network that has just come into use (on) starts a set of them (see
    ///   `starting_streams`); from there the concurrency controller sizes it.
    /// - One that can't reach the server (unreachable) keeps a single stream trying, to find out
    ///   when it can again.
    /// - One that is off, offline, failed or at its limit runs none. Retiring a stream costs
    ///   nothing: what it wrote stays, and whoever takes its block next resumes from there.
    ///
    /// A download that can't be split runs one stream, on the first network able to carry it.
    pub(crate) fn reconcile_streams(&mut self, state: &mut DownloadState, now: Millis) {
        self.register_share(state);
        if !self.live_run() {
            return;
        }
        let usable: Vec<NetworkId> = state
            .networks
            .iter()
            .filter(|n| matches!(n.status, NetworkStatus::On | NetworkStatus::Unreachable))
            .map(|n| n.id.clone())
            .collect();
        let running: Vec<NetworkId> =
            if self.splittable { usable } else { usable.into_iter().take(1).collect() };
        let joining: Vec<NetworkId> = running
            .iter()
            .filter(|id| {
                let on =
                    state.networks.iter().any(|n| &n.id == *id && n.status == NetworkStatus::On);
                on && (self.reconciled.get(*id) != Some(&NetworkStatus::On)
                    || self.live_streams(state, id).is_empty())
            })
            .cloned()
            .collect();
        let starting = if self.splittable {
            let waiting = state.blocks.iter().filter(|b| b.status == BlockStatus::Pending).count();
            starting_streams(waiting, joining.len(), self.picked)
        } else {
            1
        };

        let mut to_start = Vec::new();
        let networks: Vec<(NetworkId, NetworkStatus)> =
            state.networks.iter().map(|n| (n.id.clone(), n.status)).collect();
        for (id, status) in networks {
            let live = self.live_streams(state, &id).len();
            let target = if !running.contains(&id) {
                0
            } else if status == NetworkStatus::Unreachable {
                1
            } else if joining.contains(&id) {
                let limit = self
                    .concurrency
                    .as_ref()
                    .map_or(starting, |c| c.limit(&id, self.downloads_on(&id)));
                live.max(starting.min(limit) as usize)
            } else {
                live
            };
            if joining.contains(&id) {
                self.traffic(&id).alive_at = now;
            }
            if live > target {
                self.retire(state, &id, (live - target) as u32);
            }
            // Streams kept from before a pause start again.
            for stream in self.live_streams(state, &id).into_iter().take(target) {
                if self.rts.get(&stream).is_some_and(|rt| !rt.running) {
                    to_start.push((stream, id.clone()));
                }
            }
            if target > live {
                let added = self.add_streams(state, &id, (target - live) as u32);
                to_start.extend(added.into_iter().map(|s| (s, id.clone())));
            }
            self.reconciled.insert(id, status);
        }
        // Each stream claims a block the moment it starts, so every network gets its first
        // before any gets a second: a small file shouldn't all go to whichever came first.
        let order = interleave(to_start, |(_, network)| network.clone());
        self.start_streams(state, order.into_iter().map(|(s, _)| s).collect(), now);
    }

    /// New streams on `network`, listed for the caller to start.
    fn add_streams(
        &mut self,
        state: &mut DownloadState,
        network: &NetworkId,
        count: u32,
    ) -> Vec<StreamId> {
        let mut added = Vec::new();
        for _ in 0..count {
            let id = self.next_stream;
            self.next_stream += 1;
            state.streams.push(StreamState {
                id,
                network_id: network.clone(),
                status: StreamStatus::Pending,
                block: None,
                hedge: false,
                bytes_downloaded: 0,
                speed_bps: 0.0,
            });
            self.rts.insert(id, StreamRt::new(network.clone()));
            added.push(id);
        }
        state.peak_streams = state.peak_streams.max(state.streams.len() as u32);
        self.effects.dirty = true;
        added
    }

    fn start_streams(&mut self, state: &DownloadState, ids: Vec<StreamId>, now: Millis) {
        let Some(run) = &mut self.run else { return };
        for id in ids {
            let Some(network) =
                state.streams.iter().find(|s| s.id == id).map(|s| s.network_id.clone())
            else {
                continue;
            };
            // A fresh worker: what the last one learned about its connection is gone with it.
            let mut rt = StreamRt::new(network.clone());
            rt.cancel = run.cancel.child_token();
            rt.running = true;
            rt.warm_since = now;
            let task =
                stream::run(id, network, run.env.clone(), rt.shared.clone(), rt.cancel.clone());
            run.tasks.spawn(task);
            self.rts.insert(id, rt);
            self.effects.dirty = true;
        }
    }

    /// Stops `count` streams on `network` for good. Streams the server just refused go first,
    /// then ones whose last request failed, then the newest — and ones it is sending a block
    /// down right now only after every other. One refused a moment ago may have been let in on
    /// its retry since: cutting it mid-response throws away what it was getting, and a server
    /// that counts connections goes on counting the dead one for a moment, refusing whoever asks
    /// next and so pushing the count below what it serves.
    fn retire(&mut self, state: &mut DownloadState, network: &NetworkId, count: u32) {
        let rank = |id: &StreamId| {
            let Some(rt) = self.rts.get(id) else { return (true, false, false) };
            let receiving = rt
                .attempt
                .and_then(|serial| self.attempt(serial))
                .is_some_and(|attempt| attempt.network_received > 0);
            (!receiving, rt.refused.is_some(), rt.failures > 0)
        };
        let mut order = self.live_streams(state, network);
        order.sort_by(|a, b| rank(b).cmp(&rank(a)).then(b.cmp(a)));
        for id in order.into_iter().take(count as usize) {
            match self.rts.get_mut(&id) {
                Some(rt) if rt.running => {
                    // Its task's end takes it off the list.
                    rt.retiring = true;
                    rt.cancel.cancel();
                    rt.waiting = None;
                }
                _ => self.remove_stream(state, id),
            }
        }
    }

    fn remove_stream(&mut self, state: &mut DownloadState, id: StreamId) {
        self.rts.remove(&id);
        state.streams.retain(|s| s.id != id);
        self.effects.dirty = true;
    }

    /// A stream's task ended.
    pub(crate) fn on_stream_exit(&mut self, state: &mut DownloadState, id: StreamId) {
        let Some(rt) = self.rts.get_mut(&id) else { return };
        rt.running = false;
        rt.waiting = None;
        rt.pending = None;
        // Everything it sent has been handled (the caller drains them first), so an attempt still
        // on the books will never report: close it as stopped, or its block stays leased forever.
        if let Some(serial) = rt.attempt {
            self.finish_stopped(state, id, serial);
        }
        if self.rts.get(&id).is_some_and(|rt| rt.retiring) {
            self.remove_stream(state, id);
        }
        self.effects.dirty = true;
    }

    fn stream_mut(state: &mut DownloadState, id: StreamId) -> Option<&mut StreamState> {
        state.streams.iter_mut().find(|s| s.id == id)
    }

    /// The stream holds no block: it says so, rather than keep the last one's numbers on show.
    fn go_idle(state: &mut DownloadState, id: StreamId) {
        if let Some(stream) = Self::stream_mut(state, id) {
            stream.status = StreamStatus::Pending;
            stream.block = None;
            stream.hedge = false;
            stream.speed_bps = 0.0;
        }
    }

    // --- handing out work ----------------------------------------------------------------------

    fn policy(&self) -> SchedulerPolicy {
        let hedge_after = self.ctx.tuning.hedge_after.as_millis() as Millis;
        SchedulerPolicy {
            hedge_after_ms: hedge_after,
            max_hedges_per_block: MAX_HEDGES_PER_BLOCK,
            // What a new request costs before its first byte; production's 1 s at 2 s hedges.
            startup_ms: hedge_after / 2,
        }
    }

    /// Gives stream `id` its next order: work, a wait, or the end.
    fn dispatch(
        &mut self,
        state: &mut DownloadState,
        now: Millis,
        id: StreamId,
        reply: oneshot::Sender<Order>,
    ) {
        let live = self.live_run() && state.status == DownloadStatus::Downloading;
        let Some(rt) = self.rts.get_mut(&id).filter(|rt| live && !rt.retiring) else {
            let _ = reply.send(Order::Stop);
            return;
        };
        let network = rt.network.clone();

        // The server asked this network to hold off: no new request over it until then. Each
        // stream comes back a little apart from the others. Not a stream it has just served,
        // though: a server refusing a burst of new connections (thinkbroadband answers 429 with
        // Retry-After: 30 to most past the first 24) goes on serving the ones it let in, and
        // holding those too took the whole network down to nothing while its blocks ran out.
        let served = rt.failures == 0 && rt.received_bytes > 0;
        let hold = self.hold_until.get(&network).map_or(0, |until| until - now);
        if hold > 0 && !served {
            rt.asleep_since = Some(now);
            Self::go_idle(state, id);
            if let Some(stream) = Self::stream_mut(state, id) {
                stream.status = StreamStatus::Retrying;
            }
            self.effects.dirty = true;
            let spread = jitter() * (hold / 10).min(1000) as f64;
            let _ = reply.send(Order::Sleep(Duration::from_millis(hold as u64 + spread as u64)));
            return;
        }

        // Nothing waiting, but a stream holds a long run: it shares before anyone hedges.
        if !state.blocks.iter().any(|b| b.status == BlockStatus::Pending) {
            let speed = self.rts.get(&id).map_or(0.0, |rt| rt.last_block_speed);
            self.split_run(state, speed);
        }
        loop {
            let work = {
                let attempts: BTreeMap<BlockIndex, Vec<AttemptView>> = self
                    .attempts
                    .iter()
                    .map(|(index, list)| (*index, list.iter().map(Attempt::view).collect()))
                    .collect();
                let view = SchedulerView {
                    blocks: &state.blocks,
                    streams: &state.streams,
                    attempts: &attempts,
                    avoid: &self.avoid,
                    hedges_used: &self.hedges,
                    disk_behind: self.disk.reading() == DiskReading::Behind,
                };
                let speed = self.rts.get(&id).map(|rt| rt.last_block_speed).filter(|s| *s > 0.0);
                // One yet to finish a block is judged by its network: what that does now, split
                // between its streams fetching and this one. Taken for unknown, every stream on
                // a network starved by another behind the same router raced the slow blocks, to
                // no avail — and a server counting requests (thinkbroadband: about 180 a minute,
                // then 429 for the rest of it) had none left for the fast network's races.
                let speed = speed.or_else(|| {
                    let fetching = state
                        .streams
                        .iter()
                        .filter(|s| {
                            s.network_id == network && s.status == StreamStatus::Downloading
                        })
                        .count();
                    let entry = state.networks.iter().find(|n| n.id == network);
                    let now_bps = entry.map_or(0.0, |n| n.speed_bps);
                    (now_bps > 0.0).then(|| now_bps / (fetching + 1) as f64)
                });
                let who = Requester { stream: id, network: &network, speed_bps: speed };
                pick_work(&view, &who, now, &self.policy())
            };
            let Some(work) = work else {
                Self::go_idle(state, id);
                self.effects.dirty = true;
                // While blocks are still in flight, stay available in case one is handed back, or
                // turns out to be slow enough to be worth racing.
                if state.blocks.iter().any(|b| b.status != BlockStatus::Completed) {
                    if let Some(rt) = self.rts.get_mut(&id) {
                        rt.waiting = Some(reply);
                    }
                } else {
                    if let Some(stream) = Self::stream_mut(state, id) {
                        stream.status = StreamStatus::Completed;
                    }
                    let _ = reply.send(Order::Stop);
                }
                return;
            };
            let serial = self.begin_attempt(state, id, &network, work, now);
            match self.prepare(state, serial, now) {
                Some(job) => {
                    let _ = reply.send(Order::Fetch(job));
                    return;
                }
                // The staging file already holds the whole block.
                None if work.kind == AttemptKind::Primary => {
                    self.finish_completed(state, id, serial, now)
                }
                None => self.let_go(state, id, serial, false),
            }
        }
    }

    /// Answers the streams waiting for work, proven-fast ones first, when there may be some.
    pub(crate) fn dispatch_waiting(&mut self, state: &mut DownloadState, now: Millis) {
        let mut waiting: Vec<(StreamId, f64, oneshot::Sender<Order>)> = self
            .rts
            .iter_mut()
            .filter_map(|(id, rt)| rt.waiting.take().map(|reply| (*id, rt.last_block_speed, reply)))
            .collect();
        waiting.sort_by(|a, b| b.1.total_cmp(&a.1));
        for (id, _, reply) in waiting {
            self.dispatch(state, now, id, reply);
        }
    }

    fn begin_attempt(
        &mut self,
        state: &mut DownloadState,
        id: StreamId,
        network: &NetworkId,
        work: Work,
        now: Millis,
    ) -> u64 {
        let serial = self.next_serial;
        self.next_serial += 1;
        let block = &mut state.blocks[work.block as usize];
        // Whoever held this block before now is the one whose tail bytes a retraction would
        // discard — captured before the lease overwrites it.
        let previous_writer = block.network_id.clone();
        match work.kind {
            AttemptKind::Primary => {
                block.status = BlockStatus::Downloading;
                block.network_id = Some(network.clone());
                self.avoid.remove(&work.block);
            }
            AttemptKind::Hedge => *self.hedges.entry(work.block).or_default() += 1,
        }
        let block_start = block.range.start;
        let cancel =
            self.rts.get(&id).map_or_else(CancellationToken::new, |rt| rt.cancel.child_token());
        self.attempts.entry(work.block).or_default().push(Attempt {
            serial,
            kind: work.kind,
            block: work.block,
            stream: id,
            network: network.clone(),
            block_start,
            request_start: block_start,
            run_end: work.block + 1,
            cut: Arc::new(AtomicU64::new(u64::MAX)),
            split: false,
            received: 0,
            network_received: 0,
            last_network_at: 0,
            quiet_from: now,
            write_waiting: false,
            started_at: now,
            previous_writer,
            cancel,
            abort: None,
        });
        if let Some(rt) = self.rts.get_mut(&id) {
            rt.attempt = Some(serial);
        }
        if let Some(stream) = Self::stream_mut(state, id) {
            stream.status = StreamStatus::Downloading;
            stream.hedge = work.kind == AttemptKind::Hedge;
            stream.block = Some(work.block);
        }
        self.effects.dirty = true;
        serial
    }

    /// Where the attempt starts, and its request. `None` when the block is already whole.
    fn prepare(&mut self, state: &mut DownloadState, serial: u64, now: Millis) -> Option<Job> {
        let (index, kind, previous) = {
            let attempt = self.attempt(serial)?;
            (attempt.block, attempt.kind, attempt.previous_writer.clone())
        };
        let others = self.others_position(index, serial);
        let block = &mut state.blocks[index as usize];
        let start_offset = match kind {
            // A failed range left its written prefix in the staging file. Without range support
            // the server can only start again from byte zero.
            AttemptKind::Primary if self.supports_ranges => block.bytes_downloaded,
            AttemptKind::Primary => 0,
            AttemptKind::Hedge => block.bytes_downloaded,
        };
        if kind == AttemptKind::Primary {
            // What another attempt has already secured stays counted.
            let keep = start_offset.max(others);
            if retract_block(block, keep, previous.as_ref()) > 0 {
                recompute_aggregates(state);
            }
        }
        let range = state.blocks[index as usize].range;
        let (whole_file, source) = (!self.supports_ranges, self.source.clone());
        let attempt = self.attempt_mut(serial)?;
        attempt.request_start = range.start + start_offset;
        attempt.started_at = now;
        attempt.quiet_from = now;
        if range.len().is_some_and(|len| start_offset >= len) {
            return None;
        }
        let (run_end, end) = match kind {
            AttemptKind::Primary if self.supports_ranges && range.end.is_some() => {
                self.lease_run(state, serial)
            }
            _ => (index + 1, range.end),
        };
        let attempt = self.attempt_mut(serial)?;
        attempt.run_end = run_end;
        attempt.cut.store(end.unwrap_or(u64::MAX), Ordering::Relaxed);
        let cut = (run_end > index + 1).then(|| attempt.cut.clone());
        Some(Job {
            serial,
            range: sonic_core::ByteRange::new(range.start + start_offset, end),
            whole_file,
            cancel: attempt.cancel.clone(),
            source,
            cut,
        })
    }

    /// Leases the attempt the waiting blocks right after its own, up to its share of what is
    /// left, so that one request fetches them all: where its run ends, and where that is in the
    /// file.
    ///
    /// Every request costs a round trip before its first byte and starts its connection's speed
    /// over (a sender restarts slow start after idling), and some servers allow an address only
    /// so many requests a minute — speed.hetzner.com answers 429 after about 15. One request per
    /// block costs little while there are about as many streams as blocks, but a stream with much
    /// of the file to itself (a server allowing one or two connections, or the user's pick) would
    /// spend its time asking; and thinkbroadband allows an address about 120 a minute, then
    /// answers 429 for the rest of it. Its share is what is left of the file split between the
    /// streams: evenly between those the networks may run until its speed is known (with as many
    /// streams as blocks that is one block, as without runs), then in proportion to its speed,
    /// so that all finish about together. A leased block stays the attempt's until it writes it,
    /// lets go, or the far end of its run is handed on (see `split_run`).
    fn lease_run(&mut self, state: &mut DownloadState, serial: u64) -> (BlockIndex, Option<u64>) {
        let Some(attempt) = self.attempt(serial) else { return (0, None) };
        let (index, network, stream) = (attempt.block, attempt.network.clone(), attempt.stream);
        let left = state.blocks.iter().filter(|b| b.status != BlockStatus::Completed).count();
        let live = self.rts.values().filter(|rt| rt.running && !rt.retiring).count();
        let may_run = state
            .networks
            .iter()
            .map(|n| match n.status {
                NetworkStatus::On => self
                    .concurrency
                    .as_ref()
                    .map_or(self.picked.unwrap_or(sonic_core::START_STREAMS_PER_NETWORK), |c| {
                        c.limit(&n.id, self.downloads_on(&n.id))
                    }) as usize,
                NetworkStatus::Unreachable => 1,
                _ => 0,
            })
            .sum::<usize>()
            .max(live)
            .max(1);
        // Once its speed is known, its part of what is left is as big as its part of the speed:
        // all finish about together, and a fast stream asks once for what a slow one would take
        // many splits to hand on. Until then, an even share.
        let speed = self.rts.get(&stream).map_or(0.0, |rt| rt.last_block_speed);
        let share = match state.blocks[index as usize].range.len() {
            Some(len) if len > 0 && speed > 0.0 && state.speed_bps > 0.0 => {
                let left_bytes: u64 = state
                    .blocks
                    .iter()
                    .filter_map(|b| b.range.len().map(|len| len - b.bytes_downloaded))
                    .sum();
                let part = (speed / state.speed_bps).min(1.0);
                (left_bytes as f64 * part / len as f64).round() as usize
            }
            _ => left / may_run,
        };
        let mut end = index + 1;
        while ((end - index) as usize) < share {
            let Some(next) = state.blocks.get_mut(end as usize) else { break };
            // Only blocks with nothing written: the run's bytes follow on from its first block's.
            let free = next.status == BlockStatus::Pending
                && next.bytes_downloaded == 0
                && next.range.end.is_some()
                && self.avoid.get(&end) != Some(&network);
            if !free {
                break;
            }
            next.status = BlockStatus::Downloading;
            next.network_id = Some(network.clone());
            end += 1;
        }
        (end, state.blocks[end as usize - 1].range.end)
    }

    /// Hands the far end of the run that will take longest to get through its untouched blocks
    /// back to the queue, for a stream with nothing to do that fetches at `speed` (bytes a
    /// second; 0 if not known). Its request stops where they begin (see `FetchRequest::cut`).
    ///
    /// The two split what is left in proportion to their speeds, so they finish about together
    /// and a fast stream takes the bulk of a slow one's run in one request: a run leased to a
    /// slow network's stream is otherwise handed on a block or two at a time, each a request,
    /// and a server allowing so many a minute stops answering the fast network before the end.
    /// The block after the one the run is writing is only handed on while it is less than
    /// halfway through its own: what it has written but not yet reported may already reach in.
    fn split_run(&mut self, state: &mut DownloadState, speed: f64) {
        // What it does now, or did on its last block: a run that has only just started reads 0.
        let speed_of = |id: StreamId| {
            let now = state.streams.iter().find(|s| s.id == id).map_or(0.0, |s| s.speed_bps);
            now.max(self.rts.get(&id).map_or(0.0, |rt| rt.last_block_speed))
        };
        // (time to go, blocks it may hand on, its speed, serial)
        let mut slowest: Option<(f64, BlockIndex, f64, u64)> = None;
        for attempt in self.attempts.values().flatten() {
            if attempt.kind != AttemptKind::Primary || attempt.abort.is_some() {
                continue;
            }
            let Some(len) = state.blocks[attempt.block as usize].range.len() else { continue };
            let after = attempt.run_end.saturating_sub(attempt.block + 1);
            let near = BlockIndex::from(attempt.position() * 2 > len);
            let free = after.saturating_sub(near);
            if free == 0 {
                continue;
            }
            let left = state.blocks[attempt.block as usize..attempt.run_end as usize]
                .iter()
                .filter_map(|b| b.range.len().map(|len| len - b.bytes_downloaded))
                .sum::<u64>();
            let its = speed_of(attempt.stream);
            let eta = left as f64 / its.max(1.0);
            if slowest.is_none_or(|(most, ..)| eta > most) {
                slowest = Some((eta, free, its, attempt.serial));
            }
        }
        let Some((_, free, its, serial)) = slowest else { return };
        // Unknown either way: halves.
        let share = if speed > 0.0 && its > 0.0 { speed / (speed + its) } else { 0.5 };
        let take = ((f64::from(free) * share).round() as BlockIndex).clamp(1, free);
        let Some(attempt) = self.attempt_mut(serial) else { return };
        let split = attempt.run_end - take;
        let old_end = std::mem::replace(&mut attempt.run_end, split);
        attempt.split = true;
        attempt.cut.store(state.blocks[split as usize].range.start, Ordering::Relaxed);
        for block in &mut state.blocks[split as usize..old_end as usize] {
            if block.status == BlockStatus::Downloading {
                block.status = BlockStatus::Pending;
            }
        }
        self.effects.dirty = true;
    }

    /// The attempt has written its current block through and its request runs on into the next
    /// block of its run: the block is complete, and the attempt moves on.
    fn advance_run(&mut self, state: &mut DownloadState, serial: u64) {
        let Some((id, index, network)) =
            self.attempt(serial).map(|a| (a.stream, a.block, a.network.clone()))
        else {
            return;
        };
        self.move_on(state, serial);
        self.complete_block(state, id, index, &network);
        // Completing it idled the stream's row: it is on the next block.
        self.move_on_row(state, id, index + 1);
    }

    /// Moves the attempt on to the next block of its run.
    fn move_on(&mut self, state: &mut DownloadState, serial: u64) {
        let Some(mut attempt) = self.end_attempt(serial) else { return };
        let id = attempt.stream;
        attempt.block += 1;
        attempt.block_start = state.blocks[attempt.block as usize].range.start;
        let block = attempt.block;
        self.attempts.entry(block).or_default().push(attempt);
        if let Some(rt) = self.rts.get_mut(&id) {
            rt.attempt = Some(serial);
        }
        self.move_on_row(state, id, block);
    }

    fn move_on_row(&mut self, state: &mut DownloadState, id: StreamId, block: BlockIndex) {
        if let Some(stream) = Self::stream_mut(state, id) {
            stream.status = StreamStatus::Downloading;
            stream.hedge = false;
            stream.block = Some(block);
        }
        self.effects.dirty = true;
    }

    fn attempt(&self, serial: u64) -> Option<&Attempt> {
        self.attempts.values().flatten().find(|a| a.serial == serial)
    }

    fn attempt_mut(&mut self, serial: u64) -> Option<&mut Attempt> {
        self.attempts.values_mut().flatten().find(|a| a.serial == serial)
    }

    /// How far a block's other attempts have got: what is safe to keep counted when one lets go.
    fn others_position(&self, index: BlockIndex, serial: u64) -> u64 {
        self.attempts
            .get(&index)
            .into_iter()
            .flatten()
            .filter(|a| a.serial != serial)
            .map(Attempt::position)
            .max()
            .unwrap_or(0)
    }

    /// Takes the attempt off the books. Safe to repeat.
    fn end_attempt(&mut self, serial: u64) -> Option<Attempt> {
        let index = self.attempt(serial)?.block;
        let list = self.attempts.get_mut(&index)?;
        let position = list.iter().position(|a| a.serial == serial)?;
        let attempt = list.remove(position);
        if list.is_empty() {
            self.attempts.remove(&index);
        }
        if let Some(rt) = self.rts.get_mut(&attempt.stream)
            && rt.attempt == Some(serial)
        {
            rt.attempt = None;
        }
        Some(attempt)
    }

    fn abort(&mut self, serial: u64, reason: AbortReason) {
        if let Some(attempt) = self.attempt_mut(serial)
            && attempt.abort.is_none()
        {
            attempt.abort = Some(reason);
            attempt.cancel.cancel();
        }
    }

    // --- what streams report -------------------------------------------------------------------

    /// Handles one message from a stream.
    pub(crate) fn on_message(&mut self, state: &mut DownloadState, now: Millis, msg: StreamMsg) {
        let id = msg.stream;
        match msg.body {
            Body::Progress { serial, progress } => {
                self.on_progress(state, now, id, serial, progress)
            }
            Body::LimitReached => self.effects.reconcile = true,
            Body::Ask(report, reply) => {
                let next = match report {
                    Report::Ready => {
                        if let Some(rt) = self.rts.get_mut(&id)
                            && rt.asleep_since.take().is_some()
                        {
                            rt.warm_since = now;
                            rt.slow_since = None;
                            Self::go_idle(state, id);
                        }
                        Next::Dispatch
                    }
                    Report::Done { serial, result } => self.on_done(state, now, id, serial, result),
                    Report::Verdict(verdict) => self.on_verdict(state, now, id, verdict),
                };
                match next {
                    Next::Reply(order) => {
                        let _ = reply.send(order);
                        self.dispatch_waiting(state, now);
                    }
                    Next::Dispatch => {
                        self.dispatch(state, now, id, reply);
                        self.dispatch_waiting(state, now);
                    }
                }
                if let Some(rt) = self.rts.get(&id)
                    && rt.retiring
                    && !rt.running
                    && rt.attempt.is_none()
                {
                    self.remove_stream(state, id);
                }
            }
        }
    }

    fn on_progress(
        &mut self,
        state: &mut DownloadState,
        now: Millis,
        id: StreamId,
        serial: u64,
        progress: Progress,
    ) {
        match progress {
            Progress::Received(bytes) => {
                let network = match self.attempt_mut(serial) {
                    Some(attempt) => {
                        attempt.network_received += bytes;
                        attempt.last_network_at = now;
                        attempt.quiet_from = now;
                        attempt.network.clone()
                    }
                    None => match self.rts.get(&id) {
                        Some(rt) => rt.network.clone(),
                        None => return,
                    },
                };
                if let Some(rt) = self.rts.get_mut(&id) {
                    rt.received_bytes += bytes;
                    rt.served = true;
                    rt.reconnecting = false;
                }
                self.traffic(&network).alive_at = now;
                if let Some(entry) = state.networks.iter_mut().find(|n| n.id == network)
                    && entry.status == NetworkStatus::Unreachable
                {
                    // Through again: the next reconcile gives it back its streams.
                    entry.status = NetworkStatus::On;
                    if let Some(rt) = self.rts.get_mut(&id) {
                        rt.failures = 0;
                    }
                }
                // Read as speeds on the download's clock (see update_speeds).
                self.meters.add(id, &network, bytes);
                self.effects.dirty = true;
            }
            Progress::Written(total) => {
                let Some(attempt) = self.attempt_mut(serial) else { return };
                if total <= attempt.received {
                    return;
                }
                attempt.received = total;
                self.disk.landed(now);
                while let Some(attempt) = self.attempt(serial) {
                    let (index, network, position) =
                        (attempt.block, attempt.network.clone(), attempt.position());
                    let more = attempt.block + 1 < attempt.run_end;
                    let block = &mut state.blocks[index as usize];
                    // Only what gets the block further than it already was counts: a racing
                    // attempt re-fetches bytes the other already has.
                    let gained = advance_block(block, &network, position);
                    let through = block.range.len().is_some_and(|len| position >= len);
                    if gained > 0 {
                        if let Some(stream) = Self::stream_mut(state, id) {
                            stream.bytes_downloaded += gained;
                        }
                        if let Some(entry) = state.networks.iter_mut().find(|n| n.id == network) {
                            entry.bytes_downloaded += gained;
                        }
                        // Folded in rather than re-summed: this runs on every write, and summing
                        // is O(blocks). Every full recompute re-derives the truth, so no drift
                        // builds up.
                        state.bytes_downloaded += gained;
                        self.effects.dirty = true;
                    }
                    // Through its block with more of its run to come: on to the next. The last
                    // is finished when the request is.
                    if !(through && more) {
                        break;
                    }
                    self.advance_run(state, serial);
                }
            }
            Progress::WriteWait(waiting) => {
                if let Some(attempt) = self.attempt_mut(serial) {
                    attempt.write_waiting = waiting;
                    attempt.quiet_from = attempt.quiet_from.max(now);
                }
            }
            Progress::Throttled(wait) => {
                if let Some(attempt) = self.attempt_mut(serial) {
                    let until = now + Millis::try_from(wait.as_millis()).unwrap_or(Millis::MAX / 2);
                    attempt.quiet_from = attempt.quiet_from.max(until);
                }
            }
        }
    }

    fn on_done(
        &mut self,
        state: &mut DownloadState,
        now: Millis,
        id: StreamId,
        serial: u64,
        result: Result<u64, FetchError>,
    ) -> Next {
        let Some(attempt) = self.attempt(serial) else { return Next::Dispatch };
        let stopped = self.rts.get(&id).is_none_or(|rt| rt.cancel.is_cancelled())
            || state.status != DownloadStatus::Downloading;
        let outcome = match (result, attempt.abort) {
            (Ok(_), _) => Outcome::Completed,
            (Err(_), _) if stopped => Outcome::Stopped,
            (Err(_), Some(reason)) => Outcome::Aborted(reason),
            (Err(FetchError::Cancelled), None) => Outcome::Stopped,
            (Err(error), None) => Outcome::Failed(error),
        };
        tracing::debug!(
            download = %self.id,
            stream = id,
            network = %attempt.network,
            block = attempt.block,
            kind = ?attempt.kind,
            run = attempt.run_end - attempt.block,
            received = attempt.network_received,
            ms = now - attempt.started_at,
            ?outcome,
            "attempt"
        );
        match outcome {
            Outcome::Completed => {
                self.finish_completed(state, id, serial, now);
                Next::Dispatch
            }
            Outcome::Stopped => self.finish_stopped(state, id, serial),
            Outcome::Aborted(reason) => {
                let delivered_nothing = reason == AbortReason::Refresh
                    && self.attempt(serial).is_some_and(|a| a.network_received == 0);
                let primary = self.attempt(serial).is_some_and(|a| a.kind == AttemptKind::Primary);
                // 'lost': another attempt decided the block. 'refresh': not a failure — no retry
                // counted, no backoff; whoever takes the block next resumes it on a new
                // connection (streams waiting for work get first go at it).
                self.let_go(state, id, serial, delivered_nothing);
                if reason == AbortReason::Refresh
                    && primary
                    && let Some(rt) = self.rts.get_mut(&id)
                {
                    rt.warm_since = now;
                    rt.slow_since = None;
                }
                Next::Dispatch
            }
            Outcome::Failed(error) => self.finish_failed(state, now, id, serial, error),
        }
    }

    fn finish_completed(
        &mut self,
        state: &mut DownloadState,
        id: StreamId,
        serial: u64,
        now: Millis,
    ) {
        let Some(index) = self.attempt(serial).map(|a| a.block) else { return };
        // Another attempt already finished this block: these bytes are surplus.
        if state.blocks[index as usize].status == BlockStatus::Completed {
            self.let_go(state, id, serial, false);
            return;
        }
        let Some(attempt) = self.end_attempt(serial) else { return };
        // Normally written through to its end; whatever of its run is left goes back.
        self.release_run(state, &attempt);
        if attempt.split
            && let Some(rt) = self.rts.get_mut(&id)
        {
            rt.reconnecting = true;
        }
        if state.blocks[index as usize].range.len().is_some() {
            let end = if attempt.last_network_at > 0 { attempt.last_network_at } else { now };
            if let Some(rt) = self.rts.get_mut(&id) {
                rt.last_block_speed = attempt.network_received as f64
                    / ((end - attempt.started_at).max(1) as f64)
                    * 1000.0;
            }
        }
        self.complete_block(state, id, index, &attempt.network);
        if state.blocks.iter().all(|b| b.status == BlockStatus::Completed) {
            // Every block is in: the run winds down and the download publishes.
            self.stop();
        }
    }

    /// Block `index` is complete, by stream `id` on `network`, whose attempt at it has ended.
    fn complete_block(
        &mut self,
        state: &mut DownloadState,
        id: StreamId,
        index: BlockIndex,
        network: &NetworkId,
    ) {
        let block = &mut state.blocks[index as usize];
        block.status = BlockStatus::Completed;
        block.network_id = Some(network.clone());
        if let Some(len) = block.range.len() {
            // Progress can lag the final write: square the block up to its exact size and credit
            // the shortfall to the network that finished it.
            let gained = advance_block(block, network, len);
            if let Some(stream) = Self::stream_mut(state, id) {
                stream.bytes_downloaded += gained;
            }
        }
        if let Some(rt) = self.rts.get_mut(&id) {
            rt.failures = 0;
            rt.strikes = 0;
        }
        // Whoever is still racing for the block has lost. None of them holds it any more,
        // whenever their requests wind down: the rows show that at once.
        Self::go_idle(state, id);
        let rivals: Vec<(u64, StreamId, bool)> = self
            .attempts
            .get(&index)
            .into_iter()
            .flatten()
            .map(|a| (a.serial, a.stream, a.block + 1 < a.run_end))
            .collect();
        for (rival, stream, runs_on) in rivals {
            if runs_on {
                // A run whose block a hedge finished first: its request goes on to the next.
                self.move_on(state, rival);
            } else {
                self.abort(rival, AbortReason::Lost);
                Self::go_idle(state, stream);
            }
        }
        recompute_aggregates(state);
        self.effects.dirty = true;
    }

    /// The blocks of the attempt's run after its current one go back to the queue.
    fn release_run(&mut self, state: &mut DownloadState, attempt: &Attempt) {
        let tail = (attempt.block + 1) as usize..attempt.run_end as usize;
        for block in state.blocks.get_mut(tail).into_iter().flatten() {
            if block.status == BlockStatus::Downloading && !self.attempts.contains_key(&block.index)
            {
                block.status = BlockStatus::Pending;
                self.effects.dirty = true;
            }
        }
    }

    /// The stream was stopped while its request was in flight.
    fn finish_stopped(&mut self, state: &mut DownloadState, id: StreamId, serial: u64) -> Next {
        // Whatever stopped it, the block goes back as any other attempt's would, and one another
        // attempt has finished stays finished.
        self.let_go(state, id, serial, false);
        let status = match state.status {
            DownloadStatus::Paused | DownloadStatus::Queued => StreamStatus::Paused,
            _ => StreamStatus::Cancelled,
        };
        if let Some(stream) = Self::stream_mut(state, id) {
            stream.status = status;
        }
        Next::Reply(Order::Stop)
    }

    /// The stream stops working on the attempt's block without having finished it. A primary
    /// hands the block back to the queue; a hedge just drops out. Either way what it wrote stays
    /// counted. When the attempt delivered nothing, its network is remembered (see the scheduler).
    fn let_go(
        &mut self,
        state: &mut DownloadState,
        id: StreamId,
        serial: u64,
        delivered_nothing: bool,
    ) {
        let Some(attempt) = self.end_attempt(serial) else { return };
        self.release_run(state, &attempt);
        let block = &mut state.blocks[attempt.block as usize];
        if attempt.kind == AttemptKind::Primary && block.status == BlockStatus::Downloading {
            block.status = BlockStatus::Pending;
        }
        // A race that got nothing (turned away, or gone quiet) doesn't use up one of the block's.
        if attempt.kind == AttemptKind::Hedge
            && attempt.network_received == 0
            && let Some(used) = self.hedges.get_mut(&attempt.block)
        {
            *used = used.saturating_sub(1);
        }
        if delivered_nothing {
            self.avoid.insert(attempt.block, attempt.network);
        }
        Self::go_idle(state, id);
        self.effects.dirty = true;
    }

    fn finish_failed(
        &mut self,
        state: &mut DownloadState,
        now: Millis,
        id: StreamId,
        serial: u64,
        error: FetchError,
    ) -> Next {
        if let FetchError::RemoteChanged { check, seen } = &error {
            let verdict = match check {
                // Identical bytes can't have a different size.
                VersionCheck::Size { .. } => Some(Verdict::Different),
                // Another stream has meanwhile settled that this label serves the same bytes.
                _ if compare_version(&self.accepted, seen) == VersionCheck::Same => {
                    Some(Verdict::Same)
                }
                _ => None,
            };
            let seen = seen.clone();
            let pending = PendingFailure { serial, error, seen };
            return match verdict {
                Some(verdict) => self.apply_verdict(state, now, id, pending, verdict),
                None => {
                    // Every byte on disk came from an accepted version (mismatched responses are
                    // rejected before anything is written), so compare some of them with what a
                    // server presenting the new label serves.
                    let picks = sample_picks(
                        state.blocks.iter().map(|b| (b.range.start, b.bytes_downloaded)),
                    );
                    let job = SampleJob {
                        picks,
                        label: Some(pending.seen.clone()),
                        source: self.source.clone(),
                    };
                    if let Some(rt) = self.rts.get_mut(&id) {
                        rt.pending = Some(pending);
                    }
                    Next::Reply(Order::Sample(job))
                }
            };
        }
        self.failed(state, now, id, serial, error)
    }

    fn on_verdict(
        &mut self,
        state: &mut DownloadState,
        now: Millis,
        id: StreamId,
        verdict: Verdict,
    ) -> Next {
        match self.rts.get_mut(&id).and_then(|rt| rt.pending.take()) {
            Some(pending) => self.apply_verdict(state, now, id, pending, verdict),
            None => Next::Dispatch,
        }
    }

    fn apply_verdict(
        &mut self,
        state: &mut DownloadState,
        now: Millis,
        id: StreamId,
        pending: PendingFailure,
        verdict: Verdict,
    ) -> Next {
        let PendingFailure { serial, error, seen } = pending;
        // The check takes a moment; the download may have been paused or stopped meanwhile.
        if state.status != DownloadStatus::Downloading || !self.live_run() {
            return self.finish_stopped(state, id, serial);
        }
        match verdict {
            Verdict::Same => {
                // The same bytes under another label: accept it and fetch again straight away;
                // nothing from the rejected response was written.
                if compare_version(&self.accepted, &seen) != VersionCheck::Same {
                    self.accepted.push(seen);
                    self.source = Arc::new(Source {
                        url: self.source.url.clone(),
                        browser: self.source.browser.clone(),
                        accepted: self.accepted.clone(),
                    });
                }
                self.let_go(state, id, serial, false);
                Next::Dispatch
            }
            Verdict::Different => {
                // Every other stream would hit the same new version: stop them all now rather
                // than let each burn through its retries first. What's on disk is of the old
                // version: no use.
                self.fail_download(state, &error.to_string(), true);
                self.finish_stopped(state, id, serial)
            }
            // Nothing to compare yet, or the check itself failed: retry like any other failed
            // request. Nothing wrong was written either way.
            Verdict::Unknown => self.failed(state, now, id, serial, error),
        }
    }

    /// A failed request: retried, or the stream gives up (and maybe its network).
    fn failed(
        &mut self,
        state: &mut DownloadState,
        now: Millis,
        id: StreamId,
        serial: u64,
        error: FetchError,
    ) -> Next {
        let message = error.to_string();
        let class = Failure::of(&error);
        let Some((network, kind, network_received, received, asked_at)) = self
            .attempt(serial)
            .map(|a| (a.network.clone(), a.kind, a.network_received, a.received, a.started_at))
        else {
            return Next::Dispatch;
        };
        // A server turning the request away says nothing about whether this network can fetch
        // the block: once the server answers again (thinkbroadband: when its minute is out), it
        // may race it like any other.
        let delivered_nothing = network_received == 0 && !class.refused && !class.transient;

        if class.no_route {
            // A redirect can move a stream to a host its network cannot reach, and no retry over
            // it will change that. Its block goes back to the queue for another network.
            self.let_go(state, id, serial, delivered_nothing);
            self.fail_network(state, &network, &message);
            return Next::Reply(Order::Stop);
        }
        // A connection failing while the whole network has gone quiet means the network can't
        // reach the server — dropped, or connected with no way through. It stops being used, bar
        // one stream that keeps trying (see reconcile_streams).
        if class.connection && self.mark_unreachable(state, &network, now) {
            self.effects.reconcile = true;
        }
        // A hedge is optional work: its failure isn't retried, and what it did write stays. But a
        // server turning it away (429, 503) is waited out as for any request: asking again at
        // once, to race the next block, drew dozens of refusals a second from thinkbroadband.
        if kind == AttemptKind::Hedge && !class.transient {
            self.let_go(state, id, serial, delivered_nothing);
            return Next::Dispatch;
        }

        let status = state.networks.iter().find(|n| n.id == network).map(|n| n.status);
        // Counted before this stream goes to sleep: it was asking too.
        let asking = self.asking(state, &network, asked_at);
        // A network that can't get through retries as a matter of course: that's not news.
        if status == Some(NetworkStatus::On)
            && let Some(entry) = state.networks.iter_mut().find(|n| n.id == network)
        {
            entry.retries += 1;
        }
        let Some(rt) = self.rts.get_mut(&id) else {
            self.let_go(state, id, serial, delivered_nothing);
            return Next::Reply(Order::Stop);
        };
        // What arrived before it failed shows the network works, and what was written shows the
        // server does: the count in a row starts over, and so does the backoff.
        if network_received > 0 {
            rt.failures = 0;
        }
        if received > 0 {
            rt.strikes = 0;
            rt.busy_since = None;
        }
        rt.failures += 1;
        // The server turning a request away: the stream count takes it as a limit.
        if class.refused && !rt.reconnecting {
            rt.refuse(asking);
        }
        if class.transient {
            rt.busy_since.get_or_insert(now);
        }
        // A server answering busy is waited out for `server_busy_for`, however many retries
        // that takes.
        let busy_for = self.ctx.tuning.server_busy_for.as_millis() as Millis;
        let waiting_out =
            class.transient && rt.busy_since.is_some_and(|since| now - since < busy_for);
        let failures = rt.failures;
        // A connection that failed is the network's doing and is tried again for as long as the
        // network is there. A server that answered wrongly gets MAX_STRIKES in a row; then this
        // stream gives up, and once all its network's have, so does the network.
        let gives_up = !class.connection && {
            rt.strikes += 1;
            rt.strikes > MAX_STRIKES && !waiting_out
        };
        if gives_up {
            rt.retiring = true;
        }
        self.let_go(state, id, serial, delivered_nothing);
        if gives_up {
            if self.live_streams(state, &network).is_empty() {
                self.fail_network(state, &network, &message);
            }
            return Next::Reply(Order::Stop);
        }

        let tuning = &self.ctx.tuning;
        let mut wait = backoff(failures, tuning.retry_base, tuning.retry_max);
        if status == Some(NetworkStatus::Unreachable) {
            wait = wait.min(UNREACHABLE_RETRY);
        }
        if let (true, Some(asked)) = (class.transient, class.retry_after) {
            // Asked of whoever sent it — this network's address — not of this one request: none
            // of the network's streams sends another until then.
            let asked = asked.min(RETRY_AFTER_MAX);
            wait = wait.max(asked);
            let until = now + asked.as_millis() as Millis;
            let hold = self.hold_until.entry(network).or_insert(0);
            *hold = (*hold).max(until);
        }
        if let Some(rt) = self.rts.get_mut(&id) {
            rt.asleep_since = Some(now);
        }
        if let Some(stream) = Self::stream_mut(state, id) {
            stream.status = StreamStatus::Retrying;
        }
        self.effects.dirty = true;
        Next::Reply(Order::Sleep(wait))
    }

    /// Marks a network in use that has received nothing for `silent_after` as unable to reach
    /// the server. Whether it did.
    fn mark_unreachable(
        &mut self,
        state: &mut DownloadState,
        network: &NetworkId,
        now: Millis,
    ) -> bool {
        let silent_after = self.ctx.tuning.silent_after.as_millis() as Millis;
        // Quiet while the server asked it to hold off says nothing about the network.
        let held = self.hold_until.get(network).copied().unwrap_or(0);
        let alive_at = self.traffic.get(network).map_or(0, |t| t.alive_at).max(held);
        let Some(entry) = state.networks.iter_mut().find(|n| &n.id == network) else {
            return false;
        };
        if entry.status != NetworkStatus::On || now - alive_at < silent_after {
            return false;
        }
        entry.status = NetworkStatus::Unreachable;
        self.effects.dirty = true;
        true
    }

    /// The server keeps refusing requests over this network: it stops being used until the user
    /// switches it off and on, it reconnects, or the download is resumed.
    fn fail_network(&mut self, state: &mut DownloadState, network: &NetworkId, message: &str) {
        if let Some(entry) = state.networks.iter_mut().find(|n| &n.id == network) {
            entry.status = NetworkStatus::Failed;
            entry.error = Some(message.to_owned());
        }
        self.effects.reconcile = true;
        self.effects.dirty = true;
    }

    /// Ends the download in an error. What it has downloaded stays for a resume, unless
    /// `discard`: bytes that are no use any more.
    pub(crate) fn fail_download(
        &mut self,
        state: &mut DownloadState,
        message: &str,
        discard: bool,
    ) {
        if state.status != DownloadStatus::Downloading {
            return;
        }
        state.status = DownloadStatus::Error;
        state.error = Some(message.to_owned());
        state.resumable = !discard;
        self.effects.notice =
            Some(("Download failed".into(), format!("{}: {message}", state.file_name)));
        self.effects.dirty = true;
        self.stop();
    }

    // --- taking stock --------------------------------------------------------------------------

    /// Reads every meter as of `now` into the download's speeds and time left. Whether any changed.
    pub(crate) fn update_speeds(
        &mut self,
        state: &mut DownloadState,
        now: Millis,
        elapsed_secs: f64,
    ) -> bool {
        let mut changed = false;
        for stream in &mut state.streams {
            // Read every tick, moving or not, so its window is never stale when it moves again.
            let speed = self.meters.stream_speed(stream.id, now);
            // Between blocks or waiting to retry: 0, not what it last did.
            let speed = if stream.status == StreamStatus::Downloading { speed } else { 0.0 };
            changed |= stream.speed_bps != speed;
            stream.speed_bps = speed;
        }
        let mut total = 0.0;
        for network in &mut state.networks {
            let speed = self.meters.network_speed(&network.id, now);
            changed |= network.speed_bps != speed;
            network.speed_bps = speed;
            total += speed;
        }
        changed |= state.speed_bps != total;
        state.speed_bps = total;
        let remaining =
            state.total_bytes.filter(|t| *t > 0).map(|t| t.saturating_sub(state.bytes_downloaded));
        let left = time_left(state.time_left_secs, elapsed_secs, remaining, total);
        changed |= left != state.time_left_secs;
        state.time_left_secs = left;
        changed
    }

    /// Reconnects connections that aren't carrying their weight. Reconnecting is cheap — the
    /// block resumes from the staging file — and a fresh connection usually lands somewhere
    /// healthy.
    ///
    /// - Silent: nothing received for `silent_after` while the file is demonstrably being served
    ///   to others. Judged by the clock alone, so it also catches a network yet to deliver a byte.
    /// - Stalled: a network that was being served gone quiet all at once (see
    ///   `stalled_networks`). Every one of its connections is refreshed after `silent_after`
    ///   rather than left at 0 B/s until the stall timeout; if the fresh ones get nothing either,
    ///   they are silent connections, and the network is taken for unreachable.
    /// - Crawling: under `SLOW_RATIO` of its network's reference speed for `slow_for`. The
    ///   reference is the median of what connections on the same network are doing now and did
    ///   on their last finished block — its own included. Networks are never compared with each
    ///   other: cellular is expected to be slower than Wi-Fi.
    ///
    /// Reconnecting only swaps one connection for another, and stops after a couple of tries;
    /// what can finish a block whose connections all crawl is a hedge.
    pub(crate) fn refresh_stuck(&mut self, state: &mut DownloadState, now: Millis) {
        // Without ranges a reconnect restarts the whole file from byte 0.
        if !self.supports_ranges || !self.live_run() {
            return;
        }
        let silent_after = self.ctx.tuning.silent_after.as_millis() as Millis;
        let stalled = self.stalled_networks(now, silent_after);
        let ids: Vec<StreamId> = state.streams.iter().map(|s| s.id).collect();
        for id in ids {
            let Some(serial) = self.rts.get(&id).and_then(|rt| rt.attempt) else { continue };
            let Some(attempt) = self.attempt(serial) else { continue };
            if attempt.abort.is_some() {
                continue;
            }
            // A paused reader or final flush says nothing about the connection: the disk set its
            // pace. A crawling connection never fills its writer, so this never hides one.
            if attempt.write_waiting {
                if let Some(rt) = self.rts.get_mut(&id) {
                    rt.slow_since = None;
                }
                continue;
            }
            let (index, kind, network, started_at) =
                (attempt.block, attempt.kind, attempt.network.clone(), attempt.started_at);
            // Its whole network went quiet with it: no sign the server is turning it away, nor yet
            // that the network can't get through — the first answer is a fresh connection.
            let stalled = stalled.contains(&network);
            // Until something has arrived anywhere, a quiet origin is just a slow one.
            let silent = !stalled
                && attempt.network_received == 0
                && now - attempt.started_at >= silent_after
                && state.blocks.iter().any(|b| b.index != index && b.bytes_downloaded > 0);
            // A refresh isn't a failure, so no failed request would say the network can't get
            // through; a connection gone silent with the rest of its network says it instead.
            if silent && self.mark_unreachable(state, &network, now) {
                self.effects.reconcile = true;
            }
            // Silent while its network's others are served: the server took the connection and
            // left it waiting, which is how some turn away one too many.
            if silent {
                let asking = self.asking(state, &network, started_at);
                if let Some(rt) = self.rts.get_mut(&id) {
                    rt.refuse(asking);
                }
            }
            let refreshes = self.refreshes.get(&index).copied().unwrap_or(0);
            // A hedge is optional work: it is only ever dropped, never counted against its block.
            let primary = kind == AttemptKind::Primary;
            if primary && refreshes >= MAX_REFRESHES_PER_BLOCK {
                continue;
            }
            if stalled || silent || (primary && self.is_crawling(state, id, &network, now)) {
                if primary {
                    self.refreshes.insert(index, refreshes + 1);
                }
                self.abort(serial, AbortReason::Refresh);
            }
        }
    }

    /// Networks that were being served and have gone quiet all at once: every attempt on one has
    /// received nothing for `silent_after` — since its last byte, or its start — and at least one
    /// of them had received something. All of a network's connections stopping together (a route
    /// or CDN node gone bad under all of them) is never caught by comparing them with each
    /// other; left alone, they sit at 0 B/s until the stall timeout, though a fresh connection is
    /// usually served at once.
    ///
    /// Only a network that was receiving: one whose connections have yet to get a byte may just
    /// have a slow server, and is left to the silent rule, which waits for the file to be served
    /// somewhere first. A connection held by the disk or a speed limit isn't quiet (see
    /// `Attempt::quiet_from`), and one held by the disk now keeps its network from being judged.
    fn stalled_networks(&self, now: Millis, silent_after: Millis) -> HashSet<NetworkId> {
        let mut networks: HashMap<&NetworkId, (bool, bool)> = HashMap::new();
        for attempt in self.attempts.values().flatten().filter(|a| a.abort.is_none()) {
            let (quiet, received) = networks.entry(&attempt.network).or_insert((true, false));
            *quiet &= !attempt.write_waiting && now - attempt.quiet_from >= silent_after;
            *received |= attempt.network_received > 0;
        }
        networks
            .into_iter()
            .filter(|(_, (quiet, received))| *quiet && *received)
            .map(|(network, _)| network.clone())
            .collect()
    }

    fn is_crawling(
        &mut self,
        state: &DownloadState,
        id: StreamId,
        network: &NetworkId,
        now: Millis,
    ) -> bool {
        let warmup = self.ctx.tuning.slow_warmup.as_millis() as Millis;
        let slow_for = self.ctx.tuning.slow_for.as_millis() as Millis;
        let warm = |rt: &StreamRt| now - rt.warm_since >= warmup;
        let Some(own) = self.rts.get(&id) else { return false };
        if !warm(own) {
            return false;
        }
        let mut reference = Vec::new();
        let mut own_speed = 0.0;
        for other in state.streams.iter().filter(|s| &s.network_id == network) {
            let Some(peer) = self.rts.get(&other.id) else { continue };
            if peer.last_block_speed > 0.0 {
                reference.push(peer.last_block_speed);
            }
            if other.id == id {
                own_speed = other.speed_bps;
            } else if peer.attempt.is_some() && warm(peer) {
                reference.push(other.speed_bps);
            }
        }
        let Some(rt) = self.rts.get_mut(&id) else { return false };
        if reference.is_empty() || own_speed >= median(&mut reference) * SLOW_RATIO {
            rt.slow_since = None;
            return false;
        }
        let since = *rt.slow_since.get_or_insert(now);
        now - since >= slow_for
    }

    /// One look at the streams for the concurrency controller and the disk watch, and what the
    /// controller makes of it.
    pub(crate) fn adjust_streams(&mut self, state: &mut DownloadState, now: Millis) {
        if !self.live_run() {
            return;
        }
        let mut writing = 0;
        let mut held = 0;
        let mut networks = Vec::new();
        for network in state.networks.iter().filter(|n| n.status == NetworkStatus::On) {
            let mut snapshot = NetworkSnapshot {
                id: network.id.clone(),
                streams: 0,
                answered: 0,
                refused: 0,
                refused_among: u32::MAX,
                served: 0,
                downloads_on: self.downloads_on(&network.id),
                speed_bps: network.speed_bps,
            };
            for id in self.live_streams(state, &network.id) {
                let Some(rt) = self.rts.get(&id) else { continue };
                snapshot.streams += 1;
                snapshot.answered += u32::from(rt.received_bytes > 0);
                if let Some(asking) = rt.refused {
                    snapshot.refused += 1;
                    snapshot.refused_among = snapshot.refused_among.min(asking);
                }
                snapshot.served += u32::from(rt.served);
                if let Some(attempt) = rt.attempt.and_then(|serial| self.attempt(serial)) {
                    writing += 1;
                    held += u32::from(attempt.write_waiting);
                }
            }
            networks.push(snapshot);
        }
        // Whether the disk is what holds the download back: for Auto, and for the user, who is
        // told either way. A download waiting on its networks isn't blamed on the drive.
        self.disk.sample(now, writing, held);
        let limited = self.disk.limited(now, writing, state.disk_limited);
        if limited != state.disk_limited {
            state.disk_limited = limited;
            self.effects.dirty = true;
        }
        // Waiting blocks, and those of runs a new stream could be handed (see `split_run`).
        let leased: usize = self
            .attempts
            .values()
            .flatten()
            .filter(|a| a.kind == AttemptKind::Primary)
            .map(|a| a.run_end.saturating_sub(a.block + 2) as usize)
            .sum();
        let spare_work =
            state.blocks.iter().filter(|b| b.status == BlockStatus::Pending).count() + leased;
        let snapshot = Snapshot {
            now,
            networks,
            spare_work: u32::try_from(spare_work).unwrap_or(u32::MAX),
            disk: Some(self.disk.snapshot()),
        };
        let actions = self.concurrency.as_mut().map(|c| c.tick(&snapshot)).unwrap_or_default();
        for action in actions {
            match action {
                Action::Add { network, count } => {
                    let added = self.add_streams(state, &network, count);
                    self.start_streams(state, added, now);
                }
                Action::Retire { network, count } => self.retire(state, &network, count),
            }
        }
        // Each refusal is counted once.
        for rt in self.rts.values_mut() {
            rt.refused = None;
            rt.served = false;
        }
    }

    /// Gets streams going again now, rather than when their backoff runs out: what held them back
    /// has changed. The ones on `reconnect` networks also drop their sockets, and what they were
    /// fetching is taken up again on a new one: a refresh, so nothing is lost and no retry counted.
    pub(crate) fn wake(
        &mut self,
        pick: impl Fn(&NetworkId) -> bool,
        reconnect: impl Fn(&NetworkId) -> bool,
    ) {
        let mut refresh = Vec::new();
        for rt in self.rts.values_mut() {
            if rt.retiring || !rt.running || !pick(&rt.network) {
                continue;
            }
            rt.failures = 0;
            if reconnect(&rt.network) {
                refresh.extend(rt.attempt);
                rt.shared.reconnect.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            rt.shared.nudge.notify_one();
        }
        for serial in refresh {
            self.abort(serial, AbortReason::Refresh);
        }
    }

    /// The computer woke from sleep. Its sockets are likely dead, though nothing will say so until
    /// a stall timeout runs out, and every judgement made by the clock spans the sleep: all of it
    /// starts over.
    pub(crate) fn system_resumed(&mut self, now: Millis) {
        for traffic in self.traffic.values_mut() {
            traffic.alive_at = now;
        }
        for rt in self.rts.values_mut() {
            rt.warm_since = now;
            rt.slow_since = None;
        }
        self.disk.reset();
        self.wake(|_| true, |_| true);
    }
}

impl Drop for Transfer {
    fn drop(&mut self) {
        if let Ok(mut shares) = self.ctx.shares.lock() {
            shares.remove(&self.id);
        }
        if let Some(run) = &self.run {
            run.cancel.cancel();
        }
    }
}

/// Re-derives the byte counts from the blocks.
pub(crate) fn recompute_aggregates(state: &mut DownloadState) {
    let mut by_network: HashMap<&NetworkId, u64> = HashMap::new();
    let mut total = 0;
    for block in &state.blocks {
        total += block.bytes_downloaded;
        for (network, bytes) in &block.bytes_by_network {
            *by_network.entry(network).or_default() += bytes;
        }
    }
    let by_network: HashMap<NetworkId, u64> =
        by_network.into_iter().map(|(k, v)| (k.clone(), v)).collect();
    state.bytes_downloaded = total;
    for network in &mut state.networks {
        network.bytes_downloaded = by_network.get(&network.id).copied().unwrap_or(0);
    }
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    let mid = values.len() / 2;
    if values.len() % 2 == 1 { values[mid] } else { (values[mid - 1] + values[mid]) / 2.0 }
}

/// What a failed request says, for deciding what to do about it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Failure {
    /// No response, or the connection broke: the network's doing, retried while it is there.
    pub connection: bool,
    /// This network can never reach the host.
    pub no_route: bool,
    /// The server turned the request away (403/429/503): a limit on connections.
    pub refused: bool,
    /// The server is busy or briefly broken: waited out (see `server_busy_for`).
    pub transient: bool,
    pub retry_after: Option<Duration>,
}

impl Failure {
    pub(crate) fn of(error: &FetchError) -> Self {
        match error {
            FetchError::Connection(ConnectionError::Dial(DialError::NoCompatibleRoute {
                ..
            })) => Self { connection: true, no_route: true, ..Self::default() },
            FetchError::Connection(_) => Self { connection: true, ..Self::default() },
            FetchError::Status { code, retry_after } => Self {
                refused: matches!(code, 403 | 429 | 503),
                transient: error.is_transient(),
                retry_after: *retry_after,
                ..Self::default()
            },
            _ => Self::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failures_are_classified_by_what_they_say_about_network_and_server() {
        let gone = FetchError::Connection(ConnectionError::NetworkGone);
        assert_eq!(Failure::of(&gone), Failure { connection: true, ..Failure::default() });
        let stalled = FetchError::Connection(ConnectionError::Stalled);
        assert!(Failure::of(&stalled).connection);
        let no_route =
            FetchError::Connection(ConnectionError::Dial(DialError::NoCompatibleRoute {
                host: "x".into(),
            }));
        assert!(Failure::of(&no_route).no_route && Failure::of(&no_route).connection);

        let status = |code, retry_after| FetchError::Status { code, retry_after };
        let busy = Failure::of(&status(503, Some(Duration::from_secs(3))));
        assert!(busy.refused && busy.transient && !busy.connection);
        assert_eq!(busy.retry_after, Some(Duration::from_secs(3)));
        let forbidden = Failure::of(&status(403, None));
        assert!(forbidden.refused && !forbidden.transient);
        let broken = Failure::of(&status(500, None));
        assert!(!broken.refused && broken.transient);
        let missing = Failure::of(&status(404, None));
        assert_eq!(missing, Failure::default(), "a strike, nothing more");
        assert_eq!(Failure::of(&FetchError::Protocol("short".into())), Failure::default());
    }

    #[test]
    fn median_of_odd_and_even() {
        assert_eq!(median(&mut [3.0, 1.0, 2.0]), 2.0);
        assert_eq!(median(&mut [4.0, 1.0, 2.0, 3.0]), 2.5);
    }
}
