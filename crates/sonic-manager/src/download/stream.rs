//! One stream: a task with one connection to the server through one network.
//!
//! The stream decides nothing. It asks its download for work, does what it is told — fetch a
//! range, wait, compare samples — and reports what came of it, and every byte as it arrives. All
//! the bookkeeping (which block, racing, retries, giving up) happens on the download's task, so
//! no two streams ever see each other's half-made changes: Plexo's single thread, as a channel.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sonic_core::{
    BrowserContext, ByteRange, FileVersion, NetworkId, NetworkInterface, StreamId, VersionCheck,
    compare_version,
};
use sonic_engine::limits::Limits;
use sonic_http::{
    FetchError, FetchEvents, FetchRequest, ResponseInfo, StagingFile, StreamConnection, fetch,
    fetch_sample,
};
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

/// How much of the already-downloaded file to re-fetch and compare when a server labels a
/// response with an ETag or Last-Modified this download hasn't seen before.
pub(crate) const SAMPLE_BYTES: u64 = 16 * 1024;
pub(crate) const MAX_SAMPLES: usize = 8;
/// A sample must come from a server presenting the new label; behind a load balancer the next
/// request may land on another one, so take a few tries at reaching it.
pub(crate) const SAMPLE_TRIES: usize = 4;

/// What a stream is told to do next.
#[derive(Debug)]
pub(crate) enum Order {
    Fetch(Job),
    /// Wait this long (a backoff, a server's Retry-After), then ask again. Cut short by a nudge.
    Sleep(Duration),
    /// Compare bytes on disk with what a server presenting `label` serves.
    Sample(SampleJob),
    Stop,
}

/// One attempt's request.
#[derive(Debug)]
pub(crate) struct Job {
    pub serial: u64,
    pub range: ByteRange,
    pub whole_file: bool,
    pub cancel: CancellationToken,
    pub source: Arc<Source>,
    /// Where to stop, when the request runs on over blocks after the first: brought forward when
    /// the rest of the run goes to another stream.
    pub cut: Option<Arc<AtomicU64>>,
}

/// Where a download's bytes come from, shared by its jobs.
#[derive(Debug)]
pub(crate) struct Source {
    pub url: String,
    pub browser: Option<BrowserContext>,
    /// The version the download started on, plus any since confirmed to serve identical bytes.
    pub accepted: Vec<FileVersion>,
}

#[derive(Debug)]
pub(crate) struct SampleJob {
    /// `(offset, length)` of bytes the download already has.
    pub picks: Vec<(u64, u64)>,
    /// Only a reply presenting it counts; `None` takes any.
    pub label: Option<FileVersion>,
    pub source: Arc<Source>,
}

/// What a stream says when it asks for its next order.
#[derive(Debug)]
pub(crate) enum Report {
    Ready,
    Done { serial: u64, result: Result<u64, FetchError> },
    Verdict(Verdict),
}

/// Whether a server serves the bytes already on disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    Same,
    Different,
    /// Nothing on disk to compare yet, or the samples couldn't be fetched.
    Unknown,
}

/// What an attempt tells its download as it goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Progress {
    /// Bytes off the network.
    Received(u64),
    /// The attempt's bytes secured in the staging file, from where it started.
    Written(u64),
    WriteWait(bool),
    /// A speed limit holds its reading this long: nothing arriving meanwhile says nothing about
    /// the connection.
    Throttled(Duration),
}

#[derive(Debug)]
pub(crate) enum Body {
    Ask(Report, oneshot::Sender<Order>),
    Progress {
        serial: u64,
        progress: Progress,
    },
    /// Its network has used up its data allowance.
    LimitReached,
}

/// A message from stream `stream` to its download.
#[derive(Debug)]
pub(crate) struct StreamMsg {
    pub stream: StreamId,
    pub body: Body,
}

/// What a download can do to a stream between orders.
#[derive(Debug, Default)]
pub(crate) struct StreamShared {
    /// Cuts a wait short: what it was waiting out has changed.
    pub nudge: Notify,
    /// Drop the kept connection before the next request: it may be bound to an address the
    /// network no longer has.
    pub reconnect: AtomicBool,
}

/// What every stream of a download shares.
#[derive(Debug)]
pub(crate) struct StreamEnv {
    pub staging: StagingFile,
    pub networks: watch::Receiver<Arc<Vec<NetworkInterface>>>,
    pub limits: Arc<Mutex<Limits>>,
    pub stall_timeout: Duration,
    pub connect_timeout: Duration,
    pub tx: mpsc::UnboundedSender<StreamMsg>,
}

/// A connection through network `id`, looked up afresh on every connect.
pub(crate) fn connection(
    networks: &watch::Receiver<Arc<Vec<NetworkInterface>>>,
    id: &NetworkId,
    stall_timeout: Duration,
    connect_timeout: Duration,
) -> StreamConnection {
    let networks = networks.clone();
    let id = id.clone();
    StreamConnection::new(
        move || networks.borrow().iter().find(|iface| iface.id == id).cloned(),
        stall_timeout,
        connect_timeout,
    )
}

/// Runs stream `id` on `network` until told to stop or `cancel` is cancelled. Returns its id.
pub(crate) async fn run(
    id: StreamId,
    network: NetworkId,
    env: Arc<StreamEnv>,
    shared: Arc<StreamShared>,
    cancel: CancellationToken,
) -> StreamId {
    let mut conn = connection(&env.networks, &network, env.stall_timeout, env.connect_timeout);
    let mut report = Report::Ready;
    loop {
        let (reply, order) = oneshot::channel();
        if env.tx.send(StreamMsg { stream: id, body: Body::Ask(report, reply) }).is_err() {
            break;
        }
        let order = tokio::select! {
            // An order already given is carried out even if the stream was stopped meanwhile: a
            // fetch on a cancelled token ends at once and reports, so its attempt is closed.
            biased;
            order = order => match order {
                Ok(order) => order,
                Err(_) => break,
            },
            _ = cancel.cancelled() => break,
        };
        report = match order {
            Order::Stop => break,
            Order::Sleep(wait) => {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    _ = shared.nudge.notified() => {}
                    _ = tokio::time::sleep(wait) => {}
                }
                Report::Ready
            }
            Order::Fetch(job) => {
                if shared.reconnect.swap(false, Ordering::Relaxed) {
                    conn.reconnect();
                }
                let request = FetchRequest {
                    url: &job.source.url,
                    browser: job.source.browser.as_ref(),
                    range: job.range,
                    whole_file: job.whole_file,
                    accepted_versions: &job.source.accepted,
                    stall_timeout: env.stall_timeout,
                    cut: job.cut.as_deref(),
                };
                let mut events = Events {
                    env: &env,
                    stream: id,
                    serial: job.serial,
                    network: &network,
                    cancel: &job.cancel,
                    wait: Duration::ZERO,
                    told_limit: false,
                };
                let result =
                    fetch(&mut conn, &request, &env.staging, &mut events, &job.cancel).await;
                Report::Done { serial: job.serial, result }
            }
            Order::Sample(job) => {
                if shared.reconnect.swap(false, Ordering::Relaxed) {
                    conn.reconnect();
                }
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    verdict = compare_samples(&job, &env.staging, &mut conn, env.stall_timeout) => {
                        Report::Verdict(verdict)
                    }
                }
            }
        };
    }
    id
}

struct Events<'a> {
    env: &'a StreamEnv,
    stream: StreamId,
    serial: u64,
    network: &'a NetworkId,
    /// The attempt's: stops its request.
    cancel: &'a CancellationToken,
    /// What the limits asked of the bytes last received (see `throttle`).
    wait: Duration,
    told_limit: bool,
}

impl Events<'_> {
    fn send(&self, progress: Progress) {
        let body = Body::Progress { serial: self.serial, progress };
        let _ = self.env.tx.send(StreamMsg { stream: self.stream, body });
    }
}

impl FetchEvents for Events<'_> {
    fn response(&mut self, info: ResponseInfo) {
        tracing::debug!(
            stream = self.stream,
            serial = self.serial,
            network = %self.network,
            reused = info.reused,
            ttfb_ms = info.ttfb.as_millis() as u64,
            "response"
        );
    }

    /// Every byte received goes through the shared limits: the wait they ask for is how a speed
    /// limit slows the sender (TCP's window fills, see `throttle`), and the bytes count against
    /// the network's data allowance.
    fn received(&mut self, bytes: u64) {
        self.send(Progress::Received(bytes));
        let now = chrono::Local::now();
        let reached = match self.env.limits.lock() {
            Ok(mut limits) => {
                self.wait = limits.take(self.network, bytes, &now);
                limits.limit_reached(self.network, &now)
            }
            // A panic elsewhere poisoned it; downloading on unlimited beats stopping.
            Err(_) => false,
        };
        if reached && !self.told_limit {
            self.told_limit = true;
            let body = Body::LimitReached;
            let _ = self.env.tx.send(StreamMsg { stream: self.stream, body });
            // Not a byte more over this network, these included: a request for a run of blocks
            // would go on well past the allowance before its stream is retired.
            self.cancel.cancel();
        }
    }

    fn written(&mut self, total: u64) {
        self.send(Progress::Written(total));
    }

    fn write_wait(&mut self, waiting: bool) {
        self.send(Progress::WriteWait(waiting));
    }

    fn throttle(&mut self, _bytes: u64) -> Duration {
        let wait = std::mem::take(&mut self.wait);
        if !wait.is_zero() {
            self.send(Progress::Throttled(wait));
        }
        wait
    }
}

/// Re-fetches a spread of the bytes already on disk and compares them. `Unknown` when there's
/// nothing to compare yet, or the samples couldn't be fetched.
pub(crate) async fn compare_samples(
    job: &SampleJob,
    staging: &StagingFile,
    conn: &mut StreamConnection,
    stall_timeout: Duration,
) -> Verdict {
    let mut compared = 0;
    for &(offset, len) in &job.picks {
        let Ok(local) = staging.read(offset, len as usize).await else { continue };
        if local.is_empty() {
            continue;
        }
        let range = ByteRange::new(offset, Some(offset + local.len() as u64));
        for _ in 0..SAMPLE_TRIES {
            let browser = job.source.browser.as_ref();
            match fetch_sample(conn, &job.source.url, browser, range, stall_timeout).await {
                Err(_) => return Verdict::Unknown,
                Ok((remote, version)) => {
                    let labelled = job.label.as_ref().is_none_or(|label| {
                        compare_version(std::slice::from_ref(label), &version) == VersionCheck::Same
                    });
                    if !labelled {
                        continue;
                    }
                    if remote.as_ref() != local.as_slice() {
                        return Verdict::Different;
                    }
                    compared += 1;
                    break;
                }
            }
        }
    }
    if compared > 0 { Verdict::Same } else { Verdict::Unknown }
}

/// Which bytes to compare: up to [`MAX_SAMPLES`] blocks spread evenly over those with data, the
/// first [`SAMPLE_BYTES`] of each (or what it has). `blocks` are `(start, bytes downloaded)`.
pub(crate) fn sample_picks(blocks: impl IntoIterator<Item = (u64, u64)>) -> Vec<(u64, u64)> {
    let with_data: Vec<(u64, u64)> = blocks.into_iter().filter(|(_, bytes)| *bytes > 0).collect();
    let step = (with_data.len() as f64 / MAX_SAMPLES as f64).max(1.0);
    (0..with_data.len().min(MAX_SAMPLES))
        .map(|i| with_data[(i as f64 * step) as usize])
        .map(|(start, bytes)| (start, bytes.min(SAMPLE_BYTES)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn samples_spread_over_the_blocks_with_data() {
        let blocks: Vec<(u64, u64)> =
            (0..32).map(|i| (i * 1000, if i % 2 == 0 { 1000 } else { 0 })).collect();
        let picks = sample_picks(blocks);
        // 16 blocks with data, 8 picks: every other one of them.
        assert_eq!(picks.len(), MAX_SAMPLES);
        assert_eq!(
            picks.iter().map(|p| p.0).collect::<Vec<_>>(),
            [0, 4000, 8000, 12000, 16000, 20000, 24000, 28000]
        );
        assert!(picks.iter().all(|p| p.1 == 1000));
    }

    #[test]
    fn few_blocks_are_each_sampled_and_long_ones_cut_to_the_sample_size() {
        let picks = sample_picks([(0, 1 << 20), (1 << 20, 5), (2 << 20, 0)]);
        assert_eq!(picks, [(0, SAMPLE_BYTES), (1 << 20, 5)]);
        assert!(sample_picks([(0, 0)]).is_empty());
    }
}
