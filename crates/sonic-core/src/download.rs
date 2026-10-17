use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{BlockIndex, DownloadId, Millis, NetworkId, NetworkKind, StreamId};

/// A half-open byte range `start..end` of a file. `end: None` means "to the end of the file",
/// used only when the file's size is unknown (then the whole file is one open-ended block).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ByteRange {
    pub start: u64,
    pub end: Option<u64>,
}

impl ByteRange {
    pub fn new(start: u64, end: Option<u64>) -> Self {
        debug_assert!(end.is_none_or(|end| end >= start), "range {start}..{end:?} runs backwards");
        Self { start, end }
    }

    /// Its length, when it has an end.
    pub fn len(&self) -> Option<u64> {
        self.end.map(|end| end - self.start)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == Some(0)
    }

    /// The value of an HTTP `Range` header asking for this range (`bytes=a-b`, inclusive `b`;
    /// `bytes=a-` when open-ended). `None` for an empty range, which HTTP can't express.
    pub fn http_header(&self) -> Option<String> {
        match self.end {
            None => Some(format!("bytes={}-", self.start)),
            Some(end) if end > self.start => Some(format!("bytes={}-{}", self.start, end - 1)),
            Some(_) => None,
        }
    }
}

/// - `on`: in use.
/// - `off`: the user switched it off. A network that appears mid-download starts off.
/// - `offline`: not connected to this computer. Used again as soon as it is.
/// - `unreachable`: connected, but the server can't be reached through it. One stream keeps
///   trying, and the rest follow once it gets through.
/// - `failed`: the server kept refusing requests over it (see `DownloadNetwork::error`).
///   Switching it off and on, reconnecting it, or resuming tries again.
/// - `limit`: it has used up its data allowance for the period. Used again once the period ends
///   or the allowance is raised.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkStatus {
    On,
    Off,
    Offline,
    Unreachable,
    Failed,
    Limit,
}

/// A network as one download sees it: whether the user has it on, and how it is doing.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DownloadNetwork {
    pub id: NetworkId,
    /// Its name as the OS last reported it (the user's custom name is applied by the UI).
    pub label: String,
    pub kind: NetworkKind,
    /// The user's choice; `status` is what came of it.
    pub enabled: bool,
    pub status: NetworkStatus,
    pub error: Option<String>,
    /// Bytes of the file it delivered (secured on disk).
    pub bytes_downloaded: u64,
    pub speed_bps: f64,
    /// Requests over it that failed and were tried again.
    pub retries: u32,
}

/// What a download is doing.
/// - `queued`: waiting for a slot (see `AppSettings::downloads_at_once`) or the schedule.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DownloadStatus {
    Queued,
    Downloading,
    Paused,
    Completed,
    Error,
    Cancelled,
}

impl DownloadStatus {
    /// Still has work ahead of it (can be resumed, or will run on its own).
    pub fn is_unfinished(self) -> bool {
        matches!(self, Self::Queued | Self::Downloading | Self::Paused | Self::Error)
    }
}

/// - `pending`: waiting for work — holds no block.
/// - `downloading`: fetching `StreamState::block`.
/// - `retrying`: waiting out a backoff after a failed request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamStatus {
    Pending,
    Downloading,
    Retrying,
    Paused,
    Completed,
    Cancelled,
}

/// One connection to the server, through one network. Streams come and go as the download runs;
/// what a network has done is kept on its [`DownloadNetwork`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StreamState {
    pub id: StreamId,
    pub network_id: NetworkId,
    pub status: StreamStatus,
    /// The block it is fetching; `None` whenever it holds none.
    pub block: Option<BlockIndex>,
    /// True while it is racing another stream for `block` because that one was too slow.
    /// Whichever finishes first wins.
    pub hedge: bool,
    /// New bytes it has delivered (bytes another attempt already had don't count).
    pub bytes_downloaded: u64,
    pub speed_bps: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockStatus {
    Pending,
    Downloading,
    Completed,
}

/// One piece of the file, fetched by one request (or raced by two near the end).
///
/// Invariant: `bytes_downloaded` is the block's *frontier* — how much of it, from its first byte,
/// is secured on disk — and the values of `bytes_by_network` always add up to it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockState {
    pub index: BlockIndex,
    pub range: ByteRange,
    pub status: BlockStatus,
    pub bytes_downloaded: u64,
    /// Bytes of this block each network delivered. A block started on one network and finished on
    /// another after a retry is attributed to both; the block grid colors by the largest share.
    pub bytes_by_network: BTreeMap<NetworkId, u64>,
    /// The network currently leasing the block (or the last one to touch it).
    pub network_id: Option<NetworkId>,
}

impl BlockState {
    pub fn new(index: BlockIndex, range: ByteRange) -> Self {
        Self {
            index,
            range,
            status: BlockStatus::Pending,
            bytes_downloaded: 0,
            bytes_by_network: BTreeMap::new(),
            network_id: None,
        }
    }

    /// The network that delivered most of it, if any delivered anything.
    pub fn main_network(&self) -> Option<&NetworkId> {
        self.bytes_by_network.iter().max_by_key(|(_, bytes)| **bytes).map(|(id, _)| id)
    }
}

/// The blocks a file of `total_bytes` is cut into, none of them started: `block_size` each, the
/// last one holding the remainder. An unknown size is one block, open-ended to the end of the file;
/// an empty file is one empty block. The single source of a download's block layout — planning a
/// new download and restoring a saved one both come through here.
pub fn plan_blocks(total_bytes: Option<u64>, block_size: u64) -> Vec<BlockState> {
    match total_bytes {
        None => vec![BlockState::new(0, ByteRange::new(0, None))],
        Some(0) => vec![BlockState::new(0, ByteRange::new(0, Some(0)))],
        Some(total) => {
            let block_size = block_size.max(1);
            (0..total.div_ceil(block_size))
                .map(|index| {
                    let start = index * block_size;
                    let end = (start + block_size).min(total);
                    BlockState::new(index as BlockIndex, ByteRange::new(start, Some(end)))
                })
                .collect()
        }
    }
}

/// The fields of a browser cookie that decide where it is sent. Its `Debug` output leaves the
/// value out (see below), so a cookie can never reach a log through `{:?}`.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserCookie {
    pub name: String,
    pub value: String,
    /// With or without a leading dot: `host_only` decides whether subdomains get it.
    pub domain: String,
    pub host_only: bool,
    pub path: String,
    pub secure: bool,
}

impl std::fmt::Debug for BrowserCookie {
    /// A cookie value is a credential: everything but it, so logging a request or a download
    /// state with `{:?}` can't leak a signed-in session.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrowserCookie")
            .field("name", &self.name)
            .field("value", &"<redacted>")
            .field("domain", &self.domain)
            .field("host_only", &self.host_only)
            .field("path", &self.path)
            .field("secure", &self.secure)
            .finish()
    }
}

/// What the browser a link came from sends with it, so a link that needs a signed-in session, or
/// checks the page it came from, works as it does in the browser.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserContext {
    pub cookies: Vec<BrowserCookie>,
    pub referer: Option<String>,
    pub user_agent: Option<String>,
    /// The browser's name, for display ("Chrome").
    pub name: Option<String>,
}

/// Everything needed to start (and later resume) an HTTP download.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StartRequest {
    /// The URL after redirects (`ProbeResult::final_url`).
    pub url: String,
    pub destination_dir: PathBuf,
    pub file_name: String,
    /// `None` when the server didn't say.
    pub total_bytes: Option<u64>,
    pub supports_ranges: bool,
    /// The networks to start on. Every other one starts switched off.
    pub network_ids: Vec<NetworkId>,
    /// Streams per network the user picked; `None` = Auto (see `sonic-engine`'s concurrency).
    pub streams_per_network: Option<u32>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub browser: Option<BrowserContext>,
}

impl StartRequest {
    /// Whether the file can be fetched in parts. Otherwise one request carries all of it: one
    /// stream, on one network.
    pub fn splittable(&self) -> bool {
        self.supports_ranges && self.total_bytes.is_some_and(|total| total > 0)
    }
}

/// A download as the UI sees it, complete with blocks and streams.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DownloadState {
    pub id: DownloadId,
    pub url: String,
    pub file_name: String,
    /// Where the finished file goes (the staging file is beside it, named `<this>.sonic`).
    pub destination_path: PathBuf,
    /// `None` when the size couldn't be determined ahead of time.
    pub total_bytes: Option<u64>,
    pub bytes_downloaded: u64,
    pub speed_bps: f64,
    /// Seconds left at this speed, smoothed; `None` when there's no telling.
    pub time_left_secs: Option<f64>,
    pub status: DownloadStatus,
    pub error: Option<String>,
    /// For an error: whether resuming can pick up where it stopped. False when the progress was
    /// thrown away (e.g. the file changed on the server).
    pub resumable: bool,
    pub started_at: Millis,
    /// While queued: its place in the queue, lowest first.
    pub queued_at: Option<Millis>,
    pub paused_at: Option<Millis>,
    pub total_paused_ms: u64,
    pub completed_at: Option<Millis>,
    /// Enrolled in the download schedule: it runs inside the schedule's windows.
    pub scheduled: bool,
    pub networks: Vec<DownloadNetwork>,
    pub streams: Vec<StreamState>,
    pub peak_streams: u32,
    pub blocks: Vec<BlockState>,
    pub block_size: u64,
    /// Each network's speed, sampled once a second over the last
    /// [`SPEED_HISTORY_SECONDS`](crate::SPEED_HISTORY_SECONDS). Every series has the same length.
    pub speed_history: BTreeMap<NetworkId, Vec<f64>>,
    /// The best combined speed held for a few seconds; only ever rises.
    pub peak_speed_bps: Option<f64>,
    /// The disk can't keep up: most streams are waiting on their writes.
    pub disk_limited: bool,
    /// The browser it came from, when it came from one.
    pub from_browser: Option<String>,
}

impl DownloadState {
    /// 0.0–1.0, or `None` when the size is unknown.
    pub fn progress(&self) -> Option<f64> {
        match self.total_bytes {
            Some(0) => Some(1.0),
            Some(total) => Some((self.bytes_downloaded as f64 / total as f64).min(1.0)),
            None => None,
        }
    }

    /// Time spent downloading so far, pauses and queueing left out.
    pub fn active_ms(&self, now: Millis) -> u64 {
        let end = self.completed_at.or(self.paused_at).unwrap_or(now);
        ((end - self.started_at).max(0) as u64).saturating_sub(self.total_paused_ms)
    }
}

/// A finished download as history keeps it: no blocks or streams, which only a running download
/// needs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FinishedDownload {
    pub id: DownloadId,
    pub url: String,
    pub file_name: String,
    pub destination_path: PathBuf,
    pub total_bytes: u64,
    pub started_at: Millis,
    pub completed_at: Millis,
    pub total_paused_ms: u64,
    pub networks: Vec<DownloadNetwork>,
    pub speed_history: BTreeMap<NetworkId, Vec<f64>>,
    pub peak_speed_bps: Option<f64>,
    pub peak_streams: u32,
    pub blocks_written: u32,
    pub from_browser: Option<String>,
    /// Set when listed: its file is no longer where it was saved. Never persisted.
    #[serde(skip)]
    pub missing: bool,
}

impl FinishedDownload {
    pub fn active_ms(&self) -> u64 {
        ((self.completed_at - self.started_at).max(0) as u64).saturating_sub(self.total_paused_ms)
    }

    /// Average speed start to finish, pauses left out.
    pub fn average_speed_bps(&self) -> f64 {
        let secs = self.active_ms() as f64 / 1000.0;
        if secs > 0.0 { self.total_bytes as f64 / secs } else { 0.0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_header_is_inclusive_at_the_wire() {
        assert_eq!(ByteRange::new(0, Some(10)).http_header().as_deref(), Some("bytes=0-9"));
        assert_eq!(ByteRange::new(5, None).http_header().as_deref(), Some("bytes=5-"));
        assert_eq!(ByteRange::new(5, Some(5)).http_header(), None);
        assert_eq!(ByteRange::new(5, Some(9)).len(), Some(4));
    }

    #[test]
    fn blocks_cover_the_file_exactly() {
        let blocks = plan_blocks(Some(25), 10);
        let ranges: Vec<_> = blocks.iter().map(|b| (b.range.start, b.range.end)).collect();
        assert_eq!(ranges, [(0, Some(10)), (10, Some(20)), (20, Some(25))]);
        assert_eq!(plan_blocks(None, 10)[0].range, ByteRange::new(0, None));
        assert_eq!(plan_blocks(Some(0), 10)[0].range, ByteRange::new(0, Some(0)));
        assert_eq!(plan_blocks(Some(10), 10).len(), 1);
    }

    #[test]
    fn cookie_values_never_reach_debug_output() {
        let cookie = BrowserCookie {
            name: "session".into(),
            value: "s3cr3t-token".into(),
            domain: "example.com".into(),
            host_only: true,
            path: "/".into(),
            secure: true,
        };
        let shown = format!("{cookie:?}");
        assert!(!shown.contains("s3cr3t-token"), "{shown}");
        assert!(shown.contains("session") && shown.contains("<redacted>"), "{shown}");
    }

    #[test]
    fn main_network_is_the_largest_share() {
        let mut block = BlockState::new(0, ByteRange::new(0, Some(100)));
        assert_eq!(block.main_network(), None);
        block.bytes_by_network.insert(NetworkId::new("a"), 30);
        block.bytes_by_network.insert(NetworkId::new("b"), 70);
        assert_eq!(block.main_network(), Some(&NetworkId::new("b")));
    }
}
