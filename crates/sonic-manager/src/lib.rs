//! Runs downloads: the queue, each download's streams over its networks, pause and resume,
//! failures and recovery, persistence, limits and the schedule.
//!
//! The manager speaks only [`Command`] and [`Event`] (see `sonic_core::command`): the UI, the tests
//! and anything else drive it the same way. [`spawn`] starts it on the current Tokio runtime.
//!
//! **The items in this file are frozen** — the black-box test suite (`tests/`) and the app are
//! written against them. Everything else in the crate is internal.

mod bridge_host;
mod download;
mod manager;
mod platform;
mod queue;
mod util;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sonic_core::{
    BrowserContext, Command, DownloadId, DownloadStatus, Event, Millis, NetworkId,
    NetworkInterface, ProbeResult,
};
use sonic_engine::limits::Limits;
use sonic_store::Store;
use tokio::sync::{mpsc, watch};

/// Where the manager learns which networks the computer has.
#[derive(Debug)]
pub enum NetworkSource {
    /// The real adapters (`sonic_net::NetworkMonitor`).
    System,
    /// A list someone else controls: tests add and remove "networks" (e.g. `127.0.0.2`,
    /// `127.0.0.3` through `sonic_net::loopback`-style interfaces) by sending a new list.
    Fixed(watch::Receiver<Arc<Vec<NetworkInterface>>>),
}

/// Timings and sizes that production fixes and tests shrink, so a stall or a retry takes
/// milliseconds instead of seconds. [`Tuning::default`] is production.
#[derive(Clone, Debug)]
pub struct Tuning {
    /// The largest block (production 8 MiB; tests use small blocks to get many of them).
    pub max_block_bytes: u64,
    /// Overrides the smallest block too, so tests can plan blocks below 1 MiB. `None` = engine's.
    pub min_block_bytes: Option<u64>,
    /// First retry backoff; doubles per failure up to `retry_max`, with ±20 % jitter.
    pub retry_base: Duration,
    pub retry_max: Duration,
    /// A response's headers, or the next body bytes, taking longer than this = stalled.
    pub stall_timeout: Duration,
    /// Opening a connection (DNS + TCP + TLS) taking longer than this = can't connect.
    pub connect_timeout: Duration,
    /// How long a server answering busy (429/503…) is waited out before a stream gives up.
    pub server_busy_for: Duration,
    /// A connection isn't judged slow until it has run this long.
    pub slow_warmup: Duration,
    /// Under 10 % of its network's reference speed for this long = crawling → reconnect.
    pub slow_for: Duration,
    /// Nothing received for this long while others are served = silent → reconnect / unreachable.
    pub silent_after: Duration,
    /// A block is only raced (hedged) once every attempt at it has run this long.
    pub hedge_after: Duration,
    /// How often a running download takes stock (speeds, stuck connections, stream counts).
    pub tick: Duration,
    /// The most often one download's state is sent to the UI.
    pub ui_update: Duration,
    /// How often a running download's progress is saved to disk.
    pub checkpoint_interval: Duration,
    /// Fixes the streams each network runs and turns Auto off (tests counting requests).
    pub fixed_streams: Option<u32>,
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            max_block_bytes: 8 * 1024 * 1024,
            min_block_bytes: None,
            retry_base: Duration::from_secs(1),
            retry_max: Duration::from_secs(15),
            stall_timeout: Duration::from_secs(20),
            connect_timeout: Duration::from_secs(10),
            server_busy_for: Duration::from_secs(5 * 60),
            slow_warmup: Duration::from_secs(5),
            slow_for: Duration::from_secs(10),
            silent_after: Duration::from_secs(5),
            hedge_after: Duration::from_secs(2),
            tick: Duration::from_millis(500),
            ui_update: Duration::from_millis(200),
            checkpoint_interval: Duration::from_secs(15),
            fixed_streams: None,
        }
    }
}

/// Everything the manager needs to start.
#[derive(Debug)]
pub struct ManagerConfig {
    /// Where settings, history and saved downloads live.
    pub store: Store,
    pub networks: NetworkSource,
    pub tuning: Tuning,
    /// Start the browser-extension endpoint (`sonic-bridge`). Off in tests.
    pub bridge: bool,
    /// Where downloads go when the settings name no folder (the OS Downloads folder in the app).
    pub default_destination: PathBuf,
}

/// The manager's two ends: send it commands, receive its events. It runs until the command sender
/// is dropped or it has answered [`Command::Shutdown`].
#[derive(Debug)]
pub struct ManagerHandle {
    pub commands: mpsc::UnboundedSender<Command>,
    pub events: mpsc::UnboundedReceiver<Event>,
}

/// Starts the manager on the current Tokio runtime. It first restores saved downloads (paused, or
/// queued when enrolled in the schedule) and sends `Event::Settings`, `Event::Networks`,
/// `Event::Downloads` and `Event::History`, in that order, before handling any command.
pub fn spawn(config: ManagerConfig) -> ManagerHandle {
    let (commands, commands_rx) = mpsc::unbounded_channel();
    let (events_tx, events) = mpsc::unbounded_channel();
    tokio::spawn(manager::run(config, commands_rx, events_tx));
    ManagerHandle { commands, events }
}

/// What every task of the manager shares.
#[derive(Debug)]
pub(crate) struct Ctx {
    pub store: Store,
    pub tuning: Tuning,
    pub events: mpsc::UnboundedSender<Event>,
    pub networks: watch::Receiver<Arc<Vec<NetworkInterface>>>,
    /// Speed limits and data usage for every download together. Every received byte goes
    /// through it; the lock is never held across an await.
    pub limits: Arc<Mutex<Limits>>,
    /// By running download on Auto, the networks it has on: how many downloads share each
    /// network's streams.
    pub shares: Mutex<HashMap<DownloadId, Vec<NetworkId>>>,
    /// To the manager.
    pub notes: mpsc::UnboundedSender<Note>,
    pub clock: util::Clock,
}

/// What a download tells the manager.
#[derive(Debug)]
pub(crate) enum Note {
    /// Its status or queue place changed (or the manager's view of it was wrong).
    Status { id: DownloadId, status: DownloadStatus, queued_at: Option<Millis>, scheduled: bool },
    /// Resumed and ready to run: it starts if there is room, else goes to the queue's front.
    WantsToRun { id: DownloadId, ack: Option<sonic_core::Reply<()>> },
    /// Moved to history.
    Finished { id: DownloadId },
    /// Gone from the list.
    Removed { id: DownloadId },
    /// History may show a file as there that isn't: send it again.
    HistoryChanged,
}

/// What `url` would download, asked through a network that can reach the internet (the first
/// with a gateway, else the first).
pub(crate) async fn probe_link(
    networks: &watch::Receiver<Arc<Vec<NetworkInterface>>>,
    tuning: &Tuning,
    url: &str,
    browser: Option<&BrowserContext>,
) -> Result<ProbeResult, String> {
    let present = networks.borrow().clone();
    let Some(network) = manager::probe_network(&present) else {
        return Err("No network is connected.".into());
    };
    let mut conn =
        download::connection(networks, &network.id, tuning.stall_timeout, tuning.connect_timeout);
    sonic_http::probe(url, browser, &mut conn).await.map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sonic_core::{DownloadState, plan_blocks};

    /// A download's state for unit tests: one network, four 1000-byte blocks.
    pub(crate) fn state() -> DownloadState {
        let network = download::new_network(&sonic_net::loopback(), true);
        DownloadState {
            id: DownloadId::new(),
            url: "http://127.0.0.1/file".into(),
            file_name: "file".into(),
            destination_path: PathBuf::from("file"),
            total_bytes: Some(4000),
            bytes_downloaded: 0,
            speed_bps: 0.0,
            time_left_secs: None,
            status: DownloadStatus::Paused,
            error: None,
            resumable: true,
            started_at: 0,
            queued_at: None,
            paused_at: None,
            total_paused_ms: 0,
            completed_at: None,
            scheduled: false,
            networks: vec![network],
            streams: Vec::new(),
            peak_streams: 0,
            blocks: plan_blocks(Some(4000), 1000),
            block_size: 1000,
            speed_history: Default::default(),
            peak_speed_bps: None,
            disk_limited: false,
            from_browser: None,
        }
    }
}
