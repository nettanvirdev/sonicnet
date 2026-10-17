//! Drives one manager through `Command`/`Event` exactly as the app does, and checks it as it goes.
//!
//! - Each manager runs on **its own Tokio runtime**, so [`Harness::kill`] can drop it mid-await
//!   with no chance to save, like a crash or a power cut, while [`Harness::restart`] quits it
//!   properly (`Command::Shutdown`) and starts a new one on the same store, like a relaunch.
//! - Every event is kept, and every `DownloadUpdated` runs through [`crate::checks`]; a test
//!   waiting on anything fails at once, with the reason, when a rule is broken.
//! - When the harness is dropped at the end of a passing test, what's on disk is checked: a
//!   completed download's file equals the origin's bytes exactly with no staging file left, and a
//!   cancelled or removed one left nothing behind.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsString;
use std::net::Ipv4Addr;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Once};
use std::time::{Duration, Instant};

use sonic_core::{
    AppSettings, Command, DownloadId, DownloadNetwork, DownloadState, DownloadStatus, Event,
    NetworkId, NetworkInterface, ProbeResult, Reply, STAGING_SUFFIX, StartRequest,
};
use sonic_manager::{ManagerConfig, NetworkSource, Tuning};
use sonic_store::Store;
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::checks::{state_violations, transition_violation};
use crate::net::{Networks, fake_network};
use crate::origin::Origin;

/// The block size tests plan with, so a 1 MiB file is 16 blocks.
pub const BLOCK: u64 = 64 * 1024;

/// How long a wait or a command answer may take by default.
pub const WAIT: Duration = Duration::from_secs(20);

/// Production's timings shrunk to milliseconds and blocks shrunk to [`BLOCK`]. Hedging is off
/// (an hour) because a hedge is an extra request and many tests count requests; the tests about
/// hedging switch it on.
pub fn test_tuning() -> Tuning {
    let ms = Duration::from_millis;
    Tuning {
        max_block_bytes: BLOCK,
        min_block_bytes: Some(BLOCK),
        retry_base: ms(20),
        retry_max: ms(250),
        stall_timeout: ms(1000),
        connect_timeout: ms(2000),
        server_busy_for: ms(2000),
        slow_warmup: ms(500),
        slow_for: ms(1000),
        silent_after: ms(1000),
        hedge_after: Duration::from_secs(3600),
        tick: ms(50),
        ui_update: ms(50),
        checkpoint_interval: ms(200),
        fixed_streams: None,
    }
}

/// Test network `i` (1-based): id `net<i>`.
pub fn net(i: usize) -> NetworkId {
    NetworkId::new(format!("net{i}"))
}

/// Test network `i`'s source address: `127.0.0.<i+1>` (so never the origin's `127.0.0.1`).
pub fn net_ip(i: usize) -> Ipv4Addr {
    Ipv4Addr::new(127, 0, 0, u8::try_from(i + 1).expect("at most 254 test networks"))
}

/// Test network `i` as the OS would list it.
pub fn network(i: usize) -> NetworkInterface {
    fake_network(net(i).as_str(), net_ip(i))
}

/// Lets the system run for a while.
pub async fn settle(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Panics on manager threads, by thread name. Tokio swallows a panicking task, so a crashed
/// download task could otherwise go unnoticed while the test waits on something else or passes.
static PANICS: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());
static NEXT_INSTANCE: AtomicUsize = AtomicUsize::new(1);

/// Records panics on threads named `sonic-manager-<n>-…` (the process-wide hook is installed
/// once; every other panic goes on to the previous hook untouched).
fn watch_panics() {
    static HOOK: Once = Once::new();
    HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if let Some(name) = std::thread::current().name()
                && name.starts_with("sonic-manager-")
            {
                lock(&PANICS).push((name.to_owned(), info.to_string()));
            }
            previous(info);
        }));
    });
}

/// Everything the manager said, and what the checks made of it.
#[derive(Debug, Default)]
struct Recorder {
    events: Vec<Event>,
    latest: HashMap<DownloadId, Arc<DownloadState>>,
    history: HashMap<DownloadId, Vec<Arc<DownloadState>>>,
    /// This session's last status per download (transitions are checked within a session).
    status: HashMap<DownloadId, DownloadStatus>,
    removed: HashSet<DownloadId>,
    removed_this_session: HashSet<DownloadId>,
    /// This launch's `Event::Downloads`: what the manager restored.
    restored: Option<Vec<Arc<DownloadState>>>,
    settings: Option<Arc<AppSettings>>,
    usage: BTreeMap<NetworkId, u64>,
    violations: Vec<String>,
    /// Downloads the test started, with the bytes a completed one must have.
    tracked: HashMap<DownloadId, Option<Arc<Vec<u8>>>>,
    /// Downloads the test cancelled (`false`) or removed (`trash_file`).
    dropped: HashMap<DownloadId, bool>,
}

impl Recorder {
    fn record(&mut self, event: Event) {
        match &event {
            Event::Downloads(list) => {
                list.iter().for_each(|s| self.update(s.clone()));
                self.restored = Some(list.clone());
            }
            Event::DownloadUpdated(state) => self.update(state.clone()),
            Event::DownloadRemoved(id) => {
                self.removed.insert(*id);
                self.removed_this_session.insert(*id);
            }
            Event::Settings(settings) => self.settings = Some(settings.clone()),
            Event::NetworkUsage(usage) => self.usage = usage.clone(),
            _ => {}
        }
        self.events.push(event);
    }

    fn update(&mut self, state: Arc<DownloadState>) {
        let mut found = state_violations(&state);
        if let Some(previous) = self.status.get(&state.id)
            && let Some(wrong) = transition_violation(*previous, state.status)
        {
            found.push(format!("{}: {wrong}", state.id));
        }
        if self.removed_this_session.contains(&state.id) {
            found.push(format!("{}: updated after it was removed", state.id));
        }
        self.violations.extend(found);
        self.status.insert(state.id, state.status);
        self.latest.insert(state.id, state.clone());
        self.history.entry(state.id).or_default().push(state);
    }
}

/// One running manager.
#[derive(Debug)]
struct Instance {
    /// Its threads' name prefix, `sonic-manager-<n>-`.
    threads: String,
    runtime: Option<Runtime>,
    commands: mpsc::UnboundedSender<Command>,
    pump: JoinHandle<()>,
}

impl Drop for Instance {
    fn drop(&mut self) {
        self.pump.abort();
        // Never a plain drop: that blocks, which panics inside the test's runtime.
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

/// A manager under test, with a temporary store and destination, fake networks and every event.
#[derive(Debug)]
pub struct Harness {
    pub networks: Networks,
    /// Where downloads go.
    pub dest: PathBuf,
    /// Used by every launch; change it before [`restart`](Self::restart) to relaunch with others.
    pub tuning: Tuning,
    /// Streams per network [`start`](Self::start) asks for; `None` = Auto.
    pub streams: Option<u32>,
    store_root: PathBuf,
    instance: Option<Instance>,
    rec: Arc<Mutex<Recorder>>,
    _dir: TempDir,
}

impl Harness {
    /// A manager with test networks `net1..=net<networks>` and [`test_tuning`].
    pub fn new(networks: usize) -> Self {
        Self::with(networks, |_| {})
    }

    /// The same, with the tuning adjusted first.
    pub fn with(networks: usize, tune: impl FnOnce(&mut Tuning)) -> Self {
        let dir = tempfile::Builder::new().prefix("sonic-e2e-").tempdir().expect("temp dir");
        let dest = dir.path().join("dest");
        let store_root = dir.path().join("store");
        std::fs::create_dir_all(&dest).expect("destination folder");
        let mut tuning = test_tuning();
        tune(&mut tuning);
        let mut harness = Self {
            networks: Networks::new((1..=networks).map(network).collect()),
            dest,
            tuning,
            streams: Some(4),
            store_root,
            instance: None,
            rec: Arc::default(),
            _dir: dir,
        };
        harness.launch();
        harness
    }

    /// Starts a manager on the store (after [`kill`](Self::kill), or at first).
    pub fn launch(&mut self) {
        assert!(self.instance.is_none(), "the manager is already running");
        watch_panics();
        let threads = format!("sonic-manager-{}-", NEXT_INSTANCE.fetch_add(1, Ordering::Relaxed));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name(threads.clone())
            .enable_all()
            .build()
            .expect("manager runtime");
        let config = ManagerConfig {
            store: Store::at(&self.store_root),
            networks: NetworkSource::Fixed(self.networks.receiver()),
            tuning: self.tuning.clone(),
            bridge: false,
            default_destination: self.dest.clone(),
        };
        let spawned = catch_unwind(AssertUnwindSafe(|| {
            let _inside = runtime.enter();
            sonic_manager::spawn(config)
        }));
        let handle = match spawned {
            Ok(handle) => handle,
            Err(panic) => {
                runtime.shutdown_background();
                resume_unwind(panic);
            }
        };
        {
            let mut rec = lock(&self.rec);
            rec.status.clear();
            rec.removed_this_session.clear();
            rec.restored = None;
            rec.settings = None;
        }
        let rec = self.rec.clone();
        let mut events = handle.events;
        let pump = tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                lock(&rec).record(event);
            }
        });
        self.instance =
            Some(Instance { threads, runtime: Some(runtime), commands: handle.commands, pump });
    }

    /// Quits the manager as the app does (`Command::Shutdown`, answered once all is saved).
    pub async fn quit(&mut self) {
        self.assert_clean();
        self.ask(|reply| Command::Shutdown { reply }).await.expect("shutdown failed");
        let mut instance = self.instance.take().expect("the manager is running");
        // The manager stops after answering, closing its events; take what it said last.
        let _ = tokio::time::timeout(Duration::from_secs(2), &mut instance.pump).await;
    }

    /// Quits (unless already killed) and launches a new manager on the same store: a relaunch.
    pub async fn restart(&mut self) {
        if self.instance.is_some() {
            self.quit().await;
        }
        self.launch();
    }

    /// Drops the manager without a word: every task stops at its next await, nothing is saved
    /// that wasn't already. A crash. [`launch`](Self::launch) or [`restart`](Self::restart)
    /// brings it back.
    pub async fn kill(&mut self) {
        self.assert_clean();
        drop(self.instance.take());
        // Blocking file work the dropped runtime had started finishes on its own threads.
        settle(150).await;
    }

    // --- commands --------------------------------------------------------------------------

    pub fn send(&self, command: Command) {
        let instance = self.instance.as_ref().expect("the manager isn't running");
        if instance.commands.send(command).is_err() {
            panic!("the manager has stopped (did it panic?)");
        }
    }

    /// Sends a command that answers, and waits for the answer.
    pub async fn ask<T>(&self, make: impl FnOnce(Reply<T>) -> Command) -> Result<T, String> {
        let (tx, rx) = oneshot::channel();
        self.send(make(tx));
        match tokio::time::timeout(WAIT, rx).await {
            Ok(Ok(answer)) => answer,
            Ok(Err(_)) => Err("the manager dropped the reply unanswered".into()),
            Err(_) => panic!("the manager didn't answer within {WAIT:?}"),
        }
    }

    pub async fn probe(&self, url: &str) -> Result<ProbeResult, String> {
        self.ask(|reply| Command::Probe { url: url.to_owned(), reply }).await
    }

    /// What New download would start for `probe` on `networks` (all listed ones when empty).
    pub fn request(&self, probe: &ProbeResult, networks: &[NetworkId]) -> StartRequest {
        StartRequest {
            url: probe.final_url.clone(),
            destination_dir: self.dest.clone(),
            file_name: probe.suggested_file_name.clone(),
            total_bytes: probe.total_bytes,
            supports_ranges: probe.supports_ranges,
            network_ids: self.pick(networks),
            streams_per_network: self.streams,
            etag: probe.etag.clone(),
            last_modified: probe.last_modified.clone(),
            browser: None,
        }
    }

    /// The same without probing, from what the origin serves for `name` (for links the manager
    /// can't probe on its own, e.g. ones needing the browser's cookies).
    pub fn request_for(&self, origin: &Origin, name: &str, networks: &[NetworkId]) -> StartRequest {
        let file = origin.file(name);
        StartRequest {
            url: origin.url(name),
            destination_dir: self.dest.clone(),
            file_name: name.to_owned(),
            total_bytes: (!file.unknown_size).then_some(file.bytes.len() as u64),
            supports_ranges: file.ranges && !file.unknown_size && !file.refuse_ranges,
            network_ids: self.pick(networks),
            streams_per_network: self.streams,
            etag: file.etag,
            last_modified: file.last_modified,
            browser: None,
        }
    }

    fn pick(&self, networks: &[NetworkId]) -> Vec<NetworkId> {
        if networks.is_empty() {
            self.networks.list().into_iter().map(|n| n.id).collect()
        } else {
            networks.to_vec()
        }
    }

    /// Probes `name` on `origin` and starts it on `networks` (all when empty). A completed
    /// download of it must equal the bytes the origin serves now.
    pub async fn start(&self, origin: &Origin, name: &str, networks: &[NetworkId]) -> DownloadId {
        let url = origin.url(name);
        let expected = origin.expected_bytes(name);
        let probe = self.probe(&url).await.unwrap_or_else(|e| panic!("probing {url}: {e}"));
        self.start_request(self.request(&probe, networks), Some(expected))
            .await
            .unwrap_or_else(|e| panic!("starting {url}: {e}"))
    }

    /// Starts `request`; when completed, the file must equal `expected` (unchecked if `None`).
    pub async fn start_request(
        &self,
        request: StartRequest,
        expected: Option<Arc<Vec<u8>>>,
    ) -> Result<DownloadId, String> {
        let id = self.ask(|reply| Command::Start { request, link: None, reply }).await?;
        lock(&self.rec).tracked.insert(id, expected);
        Ok(id)
    }

    pub fn pause(&self, id: DownloadId) {
        self.send(Command::Pause(id));
    }

    pub fn resume(&self, id: DownloadId) {
        self.send(Command::Resume(id));
    }

    pub fn cancel(&self, id: DownloadId) {
        lock(&self.rec).dropped.insert(id, false);
        self.send(Command::Cancel(id));
    }

    pub fn remove(&self, id: DownloadId, trash_file: bool) {
        lock(&self.rec).dropped.insert(id, trash_file);
        self.send(Command::Remove { id, trash_file });
    }

    pub fn set_network(&self, id: DownloadId, network: &NetworkId, enabled: bool) {
        self.send(Command::SetNetwork { id, network: network.clone(), enabled });
    }

    pub async fn relink(&self, id: DownloadId, url: &str) -> Result<(), String> {
        self.ask(|reply| Command::Relink { id, url: url.to_owned(), reply }).await
    }

    /// The settings as the manager last sent them (waiting for the first `Event::Settings`).
    pub async fn settings(&self) -> AppSettings {
        self.eventually("the manager sends its settings", WAIT, || {
            lock(&self.rec).settings.is_some()
        })
        .await;
        lock(&self.rec).settings.as_deref().cloned().unwrap_or_default()
    }

    /// Changes the current settings and sends them; answers with what the manager saved.
    pub async fn update_settings(&self, change: impl FnOnce(&mut AppSettings)) -> AppSettings {
        let mut settings = self.settings().await;
        change(&mut settings);
        self.ask(|reply| Command::UpdateSettings { settings, reply })
            .await
            .unwrap_or_else(|e| panic!("UpdateSettings refused: {e}"))
    }

    // --- what happened ---------------------------------------------------------------------

    /// The download's last state, if the manager has sent one.
    pub fn latest(&self, id: DownloadId) -> Option<Arc<DownloadState>> {
        lock(&self.rec).latest.get(&id).cloned()
    }

    /// Every state the manager sent for the download, across launches, in order.
    pub fn states(&self, id: DownloadId) -> Vec<Arc<DownloadState>> {
        lock(&self.rec).history.get(&id).cloned().unwrap_or_default()
    }

    /// Every event so far, across launches.
    pub fn events(&self) -> Vec<Event> {
        lock(&self.rec).events.clone()
    }

    /// The download as this launch restored it (`Event::Downloads`); `None` if it wasn't listed
    /// (finished, or removed). Waits for the event.
    pub async fn wait_restored(&self, id: DownloadId) -> Option<Arc<DownloadState>> {
        self.eventually("the manager lists its downloads", WAIT, || {
            lock(&self.rec).restored.is_some()
        })
        .await;
        lock(&self.rec).restored.as_ref()?.iter().find(|s| s.id == id).cloned()
    }

    /// Whether the manager said the download left the list.
    pub fn was_removed(&self, id: DownloadId) -> bool {
        lock(&self.rec).removed.contains(&id)
    }

    /// Each network's data usage, as last sent.
    pub fn usage(&self) -> BTreeMap<NetworkId, u64> {
        lock(&self.rec).usage.clone()
    }

    /// Fails the test now if any state so far broke a rule.
    pub fn assert_clean(&self) {
        let mut violations = lock(&self.rec).violations.clone();
        violations.extend(self.panics());
        if !violations.is_empty() {
            panic!("invariants broken:\n  {}", violations.join("\n  "));
        }
    }

    /// Panics on the running manager's threads (a killed one's don't count: being torn down
    /// mid-await is the crash being simulated).
    fn panics(&self) -> Vec<String> {
        let Some(instance) = &self.instance else { return Vec::new() };
        lock(&PANICS)
            .iter()
            .filter(|(thread, _)| thread.starts_with(&instance.threads))
            .map(|(thread, message)| format!("a manager task panicked on {thread}: {message}"))
            .collect()
    }

    // --- waiting ---------------------------------------------------------------------------

    /// Waits (up to [`WAIT`]) until the download's latest state satisfies `predicate`.
    pub async fn wait_for(
        &self,
        id: DownloadId,
        what: &str,
        predicate: impl Fn(&DownloadState) -> bool,
    ) -> Arc<DownloadState> {
        self.wait_for_within(id, WAIT, what, predicate).await
    }

    pub async fn wait_for_within(
        &self,
        id: DownloadId,
        timeout: Duration,
        what: &str,
        predicate: impl Fn(&DownloadState) -> bool,
    ) -> Arc<DownloadState> {
        let deadline = Instant::now() + timeout;
        loop {
            self.assert_clean();
            let latest = self.latest(id);
            if let Some(state) = &latest
                && predicate(state)
            {
                return state.clone();
            }
            if Instant::now() > deadline {
                panic!(
                    "timed out after {timeout:?} waiting for {what}; last seen: {}",
                    latest.map_or("nothing".into(), |s| describe(&s))
                );
            }
            settle(10).await;
        }
    }

    /// Waits until the download is `status`. Fails at once if it ends up in another status it
    /// can't leave on its own (error, completed, cancelled).
    pub async fn wait_status(&self, id: DownloadId, status: DownloadStatus) -> Arc<DownloadState> {
        self.wait_status_within(id, status, WAIT).await
    }

    pub async fn wait_status_within(
        &self,
        id: DownloadId,
        status: DownloadStatus,
        timeout: Duration,
    ) -> Arc<DownloadState> {
        let what = format!("{status:?}");
        let state = self
            .wait_for_within(id, timeout, &what, |s| {
                s.status == status
                    || matches!(
                        s.status,
                        DownloadStatus::Error
                            | DownloadStatus::Completed
                            | DownloadStatus::Cancelled
                    )
            })
            .await;
        assert_eq!(state.status, status, "waiting for {what}, it ended: {}", describe(&state));
        state
    }

    /// Waits until `condition` holds, checking the invariants meanwhile.
    pub async fn eventually(
        &self,
        what: &str,
        timeout: Duration,
        mut condition: impl FnMut() -> bool,
    ) {
        let deadline = Instant::now() + timeout;
        loop {
            self.assert_clean();
            if condition() {
                return;
            }
            if Instant::now() > deadline {
                panic!("timed out after {timeout:?} waiting until {what}");
            }
            settle(10).await;
        }
    }

    // --- end-of-test checks ----------------------------------------------------------------

    /// What is wrong on disk (and in the states) now, given how each download ended.
    fn final_problems(&self) -> Vec<String> {
        let mut problems = self.panics();
        let rec = lock(&self.rec);
        problems.extend(rec.violations.iter().cloned());
        let mut all_over = true;
        let mut kept: HashSet<OsString> = HashSet::new();
        for (id, expected) in &rec.tracked {
            let Some(state) = rec.latest.get(id) else { continue };
            let path = &state.destination_path;
            let staging = staging_path(path);
            let dropped = rec.dropped.get(id).copied();
            let completed = state.status == DownloadStatus::Completed;
            if completed && dropped != Some(true) {
                kept.extend(path.file_name().map(OsString::from));
                match std::fs::read(path) {
                    Err(e) => problems.push(format!("{id} completed, but {path:?}: {e}")),
                    Ok(bytes) => {
                        if let Some(wrong) = expected.as_deref().and_then(|e| compare(&bytes, e)) {
                            problems.push(format!("{id} completed, but {path:?} {wrong}"));
                        }
                    }
                }
                if staging.exists() {
                    problems.push(format!("{id} completed, but left {staging:?}"));
                }
            } else if state.status == DownloadStatus::Cancelled || dropped.is_some() {
                if !completed && path.exists() {
                    problems.push(format!("{id} was cancelled/removed, but {path:?} exists"));
                }
                if staging.exists() {
                    problems.push(format!("{id} was cancelled/removed, but left {staging:?}"));
                }
            } else if !completed {
                all_over = false;
            }
        }
        // With every download over, the folder holds exactly the files that completed into it.
        if all_over && let Ok(entries) = std::fs::read_dir(&self.dest) {
            for entry in entries.flatten() {
                if !kept.contains(&entry.file_name()) {
                    problems
                        .push(format!("stray file in the destination: {:?}", entry.file_name()));
                }
            }
        }
        problems
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        // A failing test has said what it needed to; the manager goes either way.
        if std::thread::panicking() {
            return;
        }
        // Deleting files may still be under way (a cancel just answered): give it a moment.
        let deadline = Instant::now() + Duration::from_secs(5);
        let problems = loop {
            let problems = self.final_problems();
            if problems.is_empty() || Instant::now() > deadline {
                break problems;
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        drop(self.instance.take());
        if !problems.is_empty() {
            panic!("end-of-test checks failed:\n  {}", problems.join("\n  "));
        }
    }
}

pub const KIB: u64 = 1024;
pub const MIB: u64 = 1024 * 1024;

/// How the download sees network `id`.
pub fn network_of<'a>(s: &'a DownloadState, id: &NetworkId) -> Option<&'a DownloadNetwork> {
    s.networks.iter().find(|n| &n.id == id)
}

/// Bytes network `id` delivered to the download.
pub fn carried(s: &DownloadState, id: &NetworkId) -> u64 {
    network_of(s, id).map_or(0, |n| n.bytes_downloaded)
}

/// The download's streams on network `id`.
pub fn streams_on(s: &DownloadState, id: &NetworkId) -> usize {
    s.streams.iter().filter(|st| &st.network_id == id).count()
}

/// `<destination>.sonic`, where a download writes until it is complete.
pub fn staging_path(destination: &Path) -> PathBuf {
    let mut path = destination.as_os_str().to_owned();
    path.push(STAGING_SUFFIX);
    PathBuf::from(path)
}

/// How `actual` differs from `expected`, if it does.
pub fn compare(actual: &[u8], expected: &[u8]) -> Option<String> {
    if actual.len() != expected.len() {
        return Some(format!("is {} bytes, expected {}", actual.len(), expected.len()));
    }
    let at = actual.iter().zip(expected).position(|(a, b)| a != b)?;
    Some(format!("differs from the origin's bytes first at offset {at}"))
}

/// A one-line summary of a state, for failure messages.
pub fn describe(s: &DownloadState) -> String {
    let networks: Vec<String> = s
        .networks
        .iter()
        .map(|n| {
            let error = n.error.as_ref().map(|e| format!(" ({e})")).unwrap_or_default();
            let on = if n.enabled { "" } else { " [off]" };
            format!("{}:{:?}{on} {}B{error}", n.id, n.status, n.bytes_downloaded)
        })
        .collect();
    let streams: Vec<String> = s
        .streams
        .iter()
        .map(|st| {
            let hedge = if st.hedge { " hedge" } else { "" };
            format!("{}#{}:{:?}{:?}{hedge}", st.network_id, st.id, st.status, st.block)
        })
        .collect();
    format!(
        "status={:?} error={:?} resumable={} bytes={}/{:?} networks=[{}] streams=[{}]",
        s.status,
        s.error,
        s.resumable,
        s.bytes_downloaded,
        s.total_bytes,
        networks.join(", "),
        streams.join(", ")
    )
}
