//! The download server every end-to-end test talks to.
//!
//! It serves named files of deterministic bytes over raw HTTP/1.1 (keep-alive, ranges) on
//! `127.0.0.1`, and misbehaves on request in exactly the ways real servers and networks do. Raw
//! TCP rather than a server library, so a fault can break the protocol on purpose.
//!
//! Three things make it more than a file server:
//! - **Faults**, per file (`always` / `next`) or per request (a [`Origin::set_rule`] closure).
//! - **The peer address**: every request records which source address it came from. Tests bind
//!   each fake network to its own `127.0.0.x` (see [`crate::fake_network`]), so the server can
//!   slow each "network" down on its own ([`Origin::limit_ip`]) and tell which carried what.
//! - **Holding**: [`Origin::hold`] freezes every transfer just before one exact byte, so a test can
//!   pause, kill or change the file at a precise point instead of guessing with sleeps.
//! - **Connection limits and dead routes**: [`Origin::limit_connections`] turns an address away
//!   with 429 while it has too many connections open ([`Origin::limit_all_connections`]: every
//!   address together, as networks behind one router), [`Origin::limit_requests`] once too many
//!   requests came too fast ([`Origin::limit_requests_from`]: too many from one address in a
//!   window), and [`Origin::stall_connections`] silences every connection open at
//!   that moment.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};

/// Deterministic bytes (xorshift32), so a byte at the wrong offset shows and a seed reproduces
/// the exact same file.
pub fn seeded_bytes(size: usize, seed: u32) -> Vec<u8> {
    let mut state = seed.max(1);
    (0..size)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        })
        .collect()
}

/// What the server does wrong with one response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fault {
    /// 200 with the whole file, as a server without range support does.
    IgnoreRange,
    /// 206 whose Content-Range starts one byte later than asked.
    WrongStart,
    /// 206 with a correct Content-Range but one byte more body (and Content-Length to match it).
    Overlong,
    /// This many body bytes with a Content-Length to match, then a clean end: a short body.
    ShortBody(u64),
    /// 206 without a Content-Range header.
    NoContentRange,
    /// Never answers.
    StallHeaders,
    /// Headers and this many body bytes, then nothing, forever.
    StallBody(u64),
    /// This status with an empty body, and `Retry-After: <secs>` when given.
    Status { code: u16, retry_after: Option<u64> },
    /// The full Content-Length, this many body bytes, then the connection drops.
    CutAfter(u64),
    /// The body at this many bytes a second: slow, but never silent long enough to stall.
    Crawl(u64),
    /// 302 to this location (a path like `/other.bin`, or a full URL).
    Redirect(String),
    /// The same bytes under another ETag, as a second server behind a load balancer labels them.
    Etag(String),
}

impl Fault {
    /// A bare status, no Retry-After.
    pub fn status(code: u16) -> Self {
        Self::Status { code, retry_after: None }
    }
}

/// One file the origin serves, and how. Edit it with [`Origin::edit`].
#[derive(Clone, Debug)]
pub struct File {
    pub bytes: Arc<Vec<u8>>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    /// false: Range is ignored (200, whole file) and Accept-Ranges never advertised.
    pub ranges: bool,
    /// A request carrying a Range is refused with 403; one without is served in full.
    pub refuse_ranges: bool,
    /// No Content-Length (chunked), no Content-Range, no ranges: the size can't be told.
    pub unknown_size: bool,
    /// `(cookie, status)`: a request whose Cookie header lacks `cookie` (`name=value`) gets
    /// `status` (401/403).
    pub require_cookie: Option<(String, u16)>,
    /// `(location, once)`: requests are redirected there; only the next one when `once`.
    pub redirect: Option<(String, bool)>,
    /// Applied to every download request (not the probe) while set.
    pub always: Option<Fault>,
    /// Handed out one per download request (not the probe), before `always`.
    pub next: VecDeque<Fault>,
    /// Bumped by [`Origin::change_version`]; seeds the next version's bytes.
    pub version: u32,
}

/// One request as the server saw it, and what it answered.
#[derive(Clone, Debug)]
pub struct Request {
    /// 1-based count of requests the server has seen.
    pub n: usize,
    pub at: Instant,
    pub method: String,
    pub path: String,
    /// The file it asked for: the path without its leading `/` and query.
    pub name: String,
    /// Source address: which "network" it came in on.
    pub peer: IpAddr,
    /// Which TCP connection it came on, 1-based in the order they opened.
    pub connection: usize,
    pub headers: Vec<(String, String)>,
    /// The Range asked for: first byte and inclusive last.
    pub range: Option<(u64, Option<u64>)>,
    /// The fault it was given, if any.
    pub fault: Option<Fault>,
    /// 0 until the status line is decided.
    pub status: u16,
    /// Body bytes written to the socket (the client may not have read them all).
    pub bytes_sent: u64,
}

/// The range sonic-http's probe asks for (`bytes=0-1023`).
pub const PROBE_RANGE: (u64, Option<u64>) = (0, Some(1023));

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
    }

    /// The probe (`bytes=0-1023`), as opposed to a request for the file's bytes.
    pub fn is_probe(&self) -> bool {
        self.range == Some(PROBE_RANGE)
    }

    /// The first byte asked for (0 without a Range).
    pub fn start(&self) -> u64 {
        self.range.map_or(0, |(first, _)| first)
    }
}

type Rule = Box<dyn FnMut(&Request) -> Option<Fault> + Send>;

#[derive(Clone, Copy, Debug)]
struct Quota {
    max: usize,
    window: Duration,
    started: Instant,
    used: usize,
}

#[derive(Clone, Copy, Debug)]
struct RequestLimit {
    per_sec: f64,
    burst: f64,
    level: f64,
    at: Instant,
}

#[derive(Clone, Copy, Debug, Default)]
struct Hold {
    at: Option<u64>,
    reached: bool,
    generation: u64,
}

struct Shared {
    files: Mutex<HashMap<String, File>>,
    log: Mutex<Vec<Request>>,
    rule: Mutex<Option<Rule>>,
    /// Per source address: bytes a second, and when the link is next free.
    ip_limits: Mutex<HashMap<IpAddr, (u64, Instant)>>,
    /// Per source address: the most connections it may have open and still be served.
    conn_limits: Mutex<HashMap<IpAddr, usize>>,
    /// The most connections every source address together may have open and still be served.
    all_conn_limit: Mutex<Option<usize>>,
    /// Every source address together: requests a second, the burst allowed beyond that, and the
    /// bucket's level as of when.
    request_limit: Mutex<Option<RequestLimit>>,
    /// Per source address: requests it may make in each fixed window, and how this one goes.
    quotas: Mutex<HashMap<IpAddr, Quota>>,
    /// Per source address: connections open now.
    open: Mutex<HashMap<IpAddr, usize>>,
    hold: watch::Sender<Hold>,
    connections: AtomicUsize,
    /// Connections numbered up to this one have gone silent for good (see `stall_connections`).
    stalled_up_to: AtomicUsize,
    tasks: Mutex<JoinSet<()>>,
}

/// The server. Dropping it closes every connection.
pub struct Origin {
    addr: SocketAddr,
    shared: Arc<Shared>,
    accept: JoinHandle<()>,
}

impl fmt::Debug for Origin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Origin").field("addr", &self.addr).finish_non_exhaustive()
    }
}

impl Drop for Origin {
    fn drop(&mut self) {
        self.accept.abort();
        lock(&self.shared.tasks).abort_all();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A stable seed per file name and version, so each file has its own bytes.
fn seed_for(name: &str, version: u32) -> u32 {
    name.bytes().fold(2_166_136_261u32, |h, b| (h ^ u32::from(b)).wrapping_mul(16_777_619))
        ^ version.wrapping_mul(0x9E37_79B9)
}

impl Origin {
    /// Starts serving on `127.0.0.1` at a random port, with no files yet.
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind the origin");
        let addr = listener.local_addr().expect("origin address");
        let shared = Arc::new(Shared {
            files: Mutex::default(),
            log: Mutex::default(),
            rule: Mutex::default(),
            ip_limits: Mutex::default(),
            conn_limits: Mutex::default(),
            all_conn_limit: Mutex::default(),
            request_limit: Mutex::default(),
            quotas: Mutex::default(),
            open: Mutex::default(),
            hold: watch::Sender::new(Hold::default()),
            connections: AtomicUsize::new(0),
            stalled_up_to: AtomicUsize::new(0),
            tasks: Mutex::new(JoinSet::new()),
        });
        let accepting = shared.clone();
        let accept = tokio::spawn(async move {
            while let Ok((socket, peer)) = listener.accept().await {
                let id = accepting.connections.fetch_add(1, Ordering::SeqCst) + 1;
                let _ = socket.set_nodelay(true);
                let shared = accepting.clone();
                lock(&accepting.tasks).spawn(serve(socket, peer.ip(), id, shared));
            }
        });
        Self { addr, shared, accept }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// `http://127.0.0.1:<port>/<name>`.
    pub fn url(&self, name: &str) -> String {
        format!("http://127.0.0.1:{}/{}", self.addr.port(), name.trim_start_matches('/'))
    }

    /// Serves `size` seeded bytes as `name` (ETag `"<name>-v1"`), and returns them.
    pub fn add(&self, name: &str, size: usize) -> Arc<Vec<u8>> {
        let bytes = Arc::new(seeded_bytes(size, seed_for(name, 1)));
        self.add_bytes(name, bytes.clone(), Some(format!("\"{name}-v1\"")));
        bytes
    }

    /// Serves these exact bytes as `name`, with this ETag.
    pub fn add_bytes(&self, name: &str, bytes: Arc<Vec<u8>>, etag: Option<String>) {
        let file = File {
            bytes,
            etag,
            last_modified: Some("Wed, 21 Oct 2015 07:28:00 GMT".into()),
            ranges: true,
            refuse_ranges: false,
            unknown_size: false,
            require_cookie: None,
            redirect: None,
            always: None,
            next: VecDeque::new(),
            version: 1,
        };
        lock(&self.shared.files).insert(name.to_owned(), file);
    }

    /// Serves `from`'s current bytes and validators under a second name too (a fresh link to the
    /// same file).
    pub fn copy(&self, from: &str, to: &str) {
        let file = self.file(from);
        self.add_bytes(to, file.bytes.clone(), file.etag.clone());
        self.edit(to, |f| f.last_modified = file.last_modified.clone());
    }

    /// Changes how `name` is served, from the next request on.
    pub fn edit(&self, name: &str, change: impl FnOnce(&mut File)) {
        let mut files = lock(&self.shared.files);
        change(files.get_mut(name).unwrap_or_else(|| panic!("the origin serves no {name}")));
    }

    /// A copy of how `name` is served now.
    pub fn file(&self, name: &str) -> File {
        lock(&self.shared.files)
            .get(name)
            .cloned()
            .unwrap_or_else(|| panic!("the origin serves no {name}"))
    }

    /// The bytes `name` serves now: what a completed download of it must equal.
    pub fn expected_bytes(&self, name: &str) -> Arc<Vec<u8>> {
        self.file(name).bytes
    }

    /// Every download request for `name` (the probe excluded) gets `fault`, until cleared.
    pub fn fault(&self, name: &str, fault: Fault) {
        self.edit(name, |f| f.always = Some(fault));
    }

    /// The next download requests for `name` get these faults, one each.
    pub fn fault_next(&self, name: &str, faults: impl IntoIterator<Item = Fault>) {
        self.edit(name, |f| f.next.extend(faults));
    }

    pub fn clear_faults(&self, name: &str) {
        self.edit(name, |f| {
            f.always = None;
            f.next.clear();
        });
    }

    /// Decides faults per request, before the file's own: `None` leaves it to them. Called with
    /// the request (status 0) for every request, the probe included. Replaces any earlier rule.
    pub fn set_rule(&self, rule: impl FnMut(&Request) -> Option<Fault> + Send + 'static) {
        *lock(&self.shared.rule) = Some(Box::new(rule));
    }

    pub fn clear_rule(&self) {
        *lock(&self.shared.rule) = None;
    }

    /// Publishes a new version of `name`: new bytes (of `size`, or the same size), a new ETag
    /// and Last-Modified. Responses already under way finish with the old version.
    pub fn change_version(&self, name: &str, size: Option<usize>) -> Arc<Vec<u8>> {
        let mut new = None;
        self.edit(name, |f| {
            f.version += 1;
            let size = size.unwrap_or(f.bytes.len());
            f.bytes = Arc::new(seeded_bytes(size, seed_for(name, f.version)));
            f.etag = Some(format!("\"{name}-v{}\"", f.version));
            f.last_modified = Some("Thu, 01 Jan 2026 00:00:00 GMT".into());
            new = Some(f.bytes.clone());
        });
        new.unwrap_or_default()
    }

    /// The same bytes under a new ETag, as a re-upload or a CDN migration labels them.
    pub fn relabel(&self, name: &str, etag: &str) {
        self.edit(name, |f| f.etag = Some(etag.to_owned()));
    }

    /// Redirects requests for `name` to `location`; only the next one when `once`.
    pub fn redirect(&self, name: &str, location: &str, once: bool) {
        self.edit(name, |f| f.redirect = Some((location.to_owned(), once)));
    }

    /// Requests for `name` without `cookie` (`name=value`) in their Cookie header get `status`.
    pub fn require_cookie(&self, name: &str, cookie: &str, status: u16) {
        self.edit(name, |f| f.require_cookie = Some((cookie.to_owned(), status)));
    }

    /// Every response to `ip` together gets at most `bytes_per_sec`, as one network's link
    /// would. `None` lifts the limit.
    pub fn limit_ip(&self, ip: impl Into<IpAddr>, bytes_per_sec: Option<u64>) {
        let ip = ip.into();
        let mut limits = lock(&self.shared.ip_limits);
        match bytes_per_sec {
            Some(rate) if rate > 0 => {
                limits.insert(ip, (rate, Instant::now()));
            }
            _ => {
                limits.remove(&ip);
            }
        }
    }

    /// Serves `ip` only while it has at most `max` connections open: while it has more, every
    /// request from it — on any of its connections, ones being served a moment ago included — is
    /// answered 429 (no Retry-After) and that connection closed. HAProxy's usual per-client limit
    /// (`sc_conn_cur` … `deny_status 429`), as speed.hetzner.de-style servers allowing 2
    /// connections per client behave. `None` lifts the limit.
    pub fn limit_connections(&self, ip: impl Into<IpAddr>, max: Option<usize>) {
        let ip = ip.into();
        let mut limits = lock(&self.shared.conn_limits);
        match max {
            Some(max) => limits.insert(ip, max),
            None => limits.remove(&ip),
        };
    }

    /// [`limit_connections`](Self::limit_connections) for every source address together, as a
    /// server sees networks behind one router: one public address. `None` lifts the limit.
    pub fn limit_all_connections(&self, max: Option<usize>) {
        *lock(&self.shared.all_conn_limit) = max;
    }

    /// Serves every source address together at most `per_sec` requests a second, after a burst
    /// of `burst` more; a request beyond that is answered 429 (no Retry-After) on a connection
    /// kept open, and doesn't count. nginx's `limit_req … burst=… nodelay` with
    /// `limit_req_status 429`, as speed.hetzner.com behaves (about 15 a minute, burst 15).
    /// `None` lifts the limit.
    pub fn limit_requests(&self, limit: Option<(f64, f64)>) {
        *lock(&self.shared.request_limit) = limit.map(|(per_sec, burst)| RequestLimit {
            per_sec,
            burst,
            level: 0.0,
            at: Instant::now(),
        });
    }

    /// Serves `ip` at most `max` requests in each `window` (fixed windows, from now); a request
    /// beyond that is answered 429 with Retry-After for the rest of the window (in whole seconds,
    /// at least 1) on a connection kept open, and doesn't count. As thinkbroadband.com behaves:
    /// about 120 a minute from one address. `None` lifts the limit.
    pub fn limit_requests_from(&self, ip: impl Into<IpAddr>, quota: Option<(usize, Duration)>) {
        let ip = ip.into();
        let mut quotas = lock(&self.shared.quotas);
        match quota {
            Some((max, window)) => {
                quotas.insert(ip, Quota { max, window, started: Instant::now(), used: 0 })
            }
            None => quotas.remove(&ip),
        };
    }

    /// Every connection open now goes silent for good, as when the route they all took dies: what
    /// each is sending stops, and nothing asked on it later is answered. Connections opened from
    /// now on are served as usual.
    pub fn stall_connections(&self) {
        let opened = self.shared.connections.load(Ordering::SeqCst);
        self.shared.stalled_up_to.fetch_max(opened, Ordering::SeqCst);
    }

    /// Stops every download response just before it sends the byte at `offset` (and holds back
    /// any that would start past it) until [`release`](Self::release). The probe is never held.
    /// The future resolves once a response reaches the byte: the client then has exactly the
    /// bytes before it from that response.
    pub fn hold(&self, offset: u64) -> impl Future<Output = ()> + Send + 'static {
        let mut generation = 0;
        self.shared.hold.send_modify(|hold| {
            hold.generation += 1;
            generation = hold.generation;
            hold.at = Some(offset);
            hold.reached = false;
        });
        let mut rx = self.shared.hold.subscribe();
        async move {
            let _ = rx.wait_for(|hold| hold.generation != generation || hold.reached).await;
        }
    }

    pub fn release(&self) {
        self.shared.hold.send_modify(|hold| hold.at = None);
    }

    /// Every request so far, in arrival order.
    pub fn requests(&self) -> Vec<Request> {
        lock(&self.shared.log).clone()
    }

    /// The download requests for `name`: every GET but the probe.
    pub fn chunk_requests(&self, name: &str) -> Vec<Request> {
        self.requests()
            .into_iter()
            .filter(|r| r.name == name && r.method == "GET" && !r.is_probe())
            .collect()
    }

    /// Body bytes sent to `ip`, probes left out.
    pub fn bytes_to(&self, ip: impl Into<IpAddr>) -> u64 {
        let ip = ip.into();
        lock(&self.shared.log)
            .iter()
            .filter(|r| r.peer == ip && !r.is_probe())
            .map(|r| r.bytes_sent)
            .sum()
    }

    /// TCP connections opened so far.
    pub fn connections(&self) -> usize {
        self.shared.connections.load(Ordering::SeqCst)
    }
}

async fn serve(socket: TcpStream, peer: IpAddr, connection: usize, shared: Arc<Shared>) {
    let _open = Open::new(shared.clone(), peer);
    let (read, mut write) = socket.into_split();
    let mut reader = BufReader::new(read);
    while let Some((method, path, headers)) = read_head(&mut reader).await {
        let close = headers
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("connection") && v.eq_ignore_ascii_case("close"));
        let keep =
            respond(&shared, &mut write, method, path, headers, peer, connection).await && !close;
        if !keep {
            return;
        }
    }
}

type Head = (String, String, Vec<(String, String)>);

async fn read_head(reader: &mut BufReader<OwnedReadHalf>) -> Option<Head> {
    let mut line = String::new();
    if reader.read_line(&mut line).await.ok()? == 0 {
        return None;
    }
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_owned();
    let path = parts.next()?.to_owned();
    let mut headers = Vec::new();
    loop {
        line.clear();
        if reader.read_line(&mut line).await.ok()? == 0 {
            return None;
        }
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            return Some((method, path, headers));
        }
        let (name, value) = trimmed.split_once(':')?;
        headers.push((name.trim().to_owned(), value.trim().to_owned()));
    }
}

fn parse_range(value: &str) -> Option<(u64, Option<u64>)> {
    let (first, last) = value.trim().strip_prefix("bytes=")?.split_once('-')?;
    let last = if last.is_empty() { None } else { Some(last.parse().ok()?) };
    Some((first.parse().ok()?, last))
}

fn has_cookie(req: &Request, cookie: &str) -> bool {
    req.headers
        .iter()
        .filter(|(k, _)| k.eq_ignore_ascii_case("cookie"))
        .any(|(_, v)| v.split(';').any(|c| c.trim() == cookie))
}

/// What to do with a request, decided under the locks.
enum Plan {
    /// A bodiless answer.
    Bare(u16, Vec<(String, String)>),
    Serve(File, Option<Fault>),
}

impl Shared {
    fn log_set(&self, index: usize, status: u16) {
        lock(&self.log)[index].status = status;
    }

    fn log_sent(&self, index: usize, bytes: u64) {
        lock(&self.log)[index].bytes_sent += bytes;
    }

    fn stalled(&self, connection: usize) -> bool {
        connection <= self.stalled_up_to.load(Ordering::SeqCst)
    }

    /// Whether `ip` has more connections open than its limit allows, or every address together
    /// more than theirs.
    fn over_limit(&self, ip: IpAddr) -> bool {
        let open = lock(&self.open);
        let all: usize = open.values().sum();
        let mine = open.get(&ip).copied().unwrap_or(0);
        lock(&self.conn_limits).get(&ip).is_some_and(|max| mine > *max)
            || lock(&self.all_conn_limit).is_some_and(|max| all > max)
    }

    /// Whether a request now is within the request limit; it counts if so.
    fn take_request(&self) -> bool {
        let mut limit = lock(&self.request_limit);
        let Some(limit) = limit.as_mut() else { return true };
        let now = Instant::now();
        let drained = (now - limit.at).as_secs_f64() * limit.per_sec;
        limit.level = (limit.level - drained).max(0.0);
        limit.at = now;
        if limit.level > limit.burst {
            return false;
        }
        limit.level += 1.0;
        true
    }

    /// Whether `ip` may make a request now (it counts if so); if not, the seconds to wait.
    fn take_quota(&self, ip: IpAddr) -> Result<(), u64> {
        let mut quotas = lock(&self.quotas);
        let Some(quota) = quotas.get_mut(&ip) else { return Ok(()) };
        let now = Instant::now();
        while now - quota.started >= quota.window {
            quota.started += quota.window;
            quota.used = 0;
        }
        if quota.used >= quota.max {
            let left = quota.window - (now - quota.started);
            return Err(left.as_secs_f64().ceil().max(1.0) as u64);
        }
        quota.used += 1;
        Ok(())
    }

    fn ip_rate(&self, ip: IpAddr) -> Option<u64> {
        lock(&self.ip_limits).get(&ip).map(|(rate, _)| *rate)
    }

    /// Waits until `ip`'s link has carried `bytes` more at its rate.
    async fn throttle(&self, ip: IpAddr, bytes: u64) {
        let until = lock(&self.ip_limits).get_mut(&ip).map(|(rate, free_at)| {
            let now = Instant::now();
            *free_at = (*free_at).max(now) + Duration::from_secs_f64(bytes as f64 / *rate as f64);
            *free_at
        });
        if let Some(until) = until {
            tokio::time::sleep_until(until.into()).await;
        }
    }

    fn plan(&self, req: &mut Request) -> Plan {
        let ruled = lock(&self.rule).as_mut().and_then(|rule| rule(req));
        let mut files = lock(&self.files);
        let Some(file) = files.get_mut(&req.name) else {
            return Plan::Bare(404, Vec::new());
        };
        if let Some((cookie, status)) = &file.require_cookie
            && !has_cookie(req, cookie)
        {
            return Plan::Bare(*status, Vec::new());
        }
        if let Some((location, once)) = file.redirect.clone() {
            if once {
                file.redirect = None;
            }
            return Plan::Bare(302, vec![("Location".into(), location)]);
        }
        let download = req.method == "GET" && !req.is_probe();
        let fault = ruled.or_else(|| {
            download.then(|| file.next.pop_front().or_else(|| file.always.clone())).flatten()
        });
        req.fault = fault.clone();
        Plan::Serve(file.clone(), fault)
    }
}

/// One connection open from `ip`, counted until dropped.
struct Open {
    shared: Arc<Shared>,
    ip: IpAddr,
}

impl Open {
    fn new(shared: Arc<Shared>, ip: IpAddr) -> Self {
        *lock(&shared.open).entry(ip).or_default() += 1;
        Self { shared, ip }
    }
}

impl Drop for Open {
    fn drop(&mut self) {
        if let Some(open) = lock(&self.shared.open).get_mut(&self.ip) {
            *open = open.saturating_sub(1);
        }
    }
}

/// What happens once the body bytes meant for the wire are sent.
enum Then {
    End,
    Cut,
    Stall,
}

/// Answers one request. False when the connection is to close.
async fn respond(
    shared: &Shared,
    write: &mut OwnedWriteHalf,
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    peer: IpAddr,
    connection: usize,
) -> bool {
    let name = path.trim_start_matches('/').split('?').next().unwrap_or_default().to_owned();
    let mut req = Request {
        n: 0,
        at: Instant::now(),
        method,
        path,
        name,
        peer,
        connection,
        range: None,
        headers,
        fault: None,
        status: 0,
        bytes_sent: 0,
    };
    req.range = req.header("range").and_then(parse_range);
    let index = {
        let mut log = lock(&shared.log);
        req.n = log.len() + 1;
        log.push(req.clone());
        log.len() - 1
    };
    let plan = shared.plan(&mut req);
    lock(&shared.log)[index].fault = req.fault.clone();
    if shared.stalled(connection) {
        std::future::pending::<()>().await;
    }
    if shared.over_limit(peer) {
        shared.log_set(index, 429);
        let _ = send_bare(write, 429, &[("Connection".into(), "close".into())]).await;
        return false;
    }
    if !shared.take_request() {
        shared.log_set(index, 429);
        return send_bare(write, 429, &[]).await;
    }
    if let Err(wait) = shared.take_quota(peer) {
        shared.log_set(index, 429);
        return send_bare(write, 429, &[("Retry-After".into(), wait.to_string())]).await;
    }
    let probe = req.is_probe();
    let head_only = req.method == "HEAD";
    let range = req.range;

    let (file, fault) = match plan {
        Plan::Bare(status, extra) => {
            shared.log_set(index, status);
            return send_bare(write, status, &extra).await;
        }
        Plan::Serve(file, fault) => (file, fault),
    };
    match &fault {
        Some(Fault::StallHeaders) => std::future::pending().await,
        Some(Fault::Status { code, retry_after }) => {
            let extra: Vec<(String, String)> =
                retry_after.iter().map(|s| ("Retry-After".to_owned(), s.to_string())).collect();
            shared.log_set(index, *code);
            return send_bare(write, *code, &extra).await;
        }
        Some(Fault::Redirect(location)) => {
            shared.log_set(index, 302);
            return send_bare(write, 302, &[("Location".into(), location.clone())]).await;
        }
        _ => {}
    }
    if file.refuse_ranges && range.is_some() {
        shared.log_set(index, 403);
        return send_bare(write, 403, &[]).await;
    }

    let bytes = &file.bytes;
    let total = bytes.len() as u64;
    let ranges_on = file.ranges && !file.unknown_size && fault != Some(Fault::IgnoreRange);
    let mut headers: Vec<(String, String)> = Vec::new();
    let etag = match &fault {
        Some(Fault::Etag(etag)) => Some(etag.clone()),
        _ => file.etag.clone(),
    };
    if let Some(etag) = etag {
        headers.push(("ETag".into(), etag));
    }
    if let Some(date) = &file.last_modified {
        headers.push(("Last-Modified".into(), date.clone()));
    }
    headers.push(("Content-Type".into(), "application/octet-stream".into()));
    if file.ranges && !file.unknown_size {
        headers.push(("Accept-Ranges".into(), "bytes".into()));
    }

    let (status, start, end) = match (ranges_on, range) {
        (true, Some((first, _))) if first >= total => {
            shared.log_set(index, 416);
            return send_bare(write, 416, &[("Content-Range".into(), format!("bytes */{total}"))])
                .await;
        }
        (true, Some((first, last))) => {
            let end = last.map_or(total, |last| (last + 1).min(total)).max(first);
            if fault != Some(Fault::NoContentRange) {
                let shown = if fault == Some(Fault::WrongStart) { first + 1 } else { first };
                headers.push((
                    "Content-Range".into(),
                    format!("bytes {shown}-{}/{total}", end.saturating_sub(1)),
                ));
            }
            (206, first, end)
        }
        _ => (200, 0, total),
    };

    let mut body = bytes[start as usize..end as usize].to_vec();
    match fault {
        Some(Fault::Overlong) => body.push(bytes.get(end as usize).copied().unwrap_or(0xAB)),
        Some(Fault::ShortBody(n)) => body.truncate(n as usize),
        _ => {}
    }
    let (send_len, then) = match fault {
        Some(Fault::CutAfter(n)) => (body.len().min(n as usize), Then::Cut),
        Some(Fault::StallBody(n)) => (body.len().min(n as usize), Then::Stall),
        _ => (body.len(), Then::End),
    };
    let chunked = file.unknown_size;
    if chunked {
        headers.push(("Transfer-Encoding".into(), "chunked".into()));
    } else {
        headers.push(("Content-Length".into(), body.len().to_string()));
    }

    // Responses that would start past a held byte wait before saying anything.
    if !probe {
        let mut rx = shared.hold.subscribe();
        let held = rx.borrow().at;
        if let Some(at) = held
            && start > at
        {
            let _ = rx.wait_for(|hold| hold.at != Some(at)).await;
        }
    }

    shared.log_set(index, status);
    if write.write_all(head(status, &headers).as_bytes()).await.is_err() {
        return false;
    }
    if head_only {
        return true;
    }

    let crawl = match fault {
        Some(Fault::Crawl(rate)) => Some(rate.max(1)),
        _ => None,
    };
    let slowest = crawl.into_iter().chain(shared.ip_rate(peer)).min();
    let piece = slowest.map_or(16 * 1024, |rate| (rate / 20).clamp(256, 16 * 1024)) as usize;
    let mut position = 0usize;
    while position < send_len {
        let mut next = (position + piece).min(send_len);
        if !probe {
            let mut rx = shared.hold.subscribe();
            let held = rx.borrow().at;
            if let Some(at) = held {
                let here = start + position as u64;
                if here == at {
                    shared.hold.send_modify(|hold| {
                        if hold.at == Some(at) {
                            hold.reached = true;
                        }
                    });
                    let _ = rx.wait_for(|hold| hold.at != Some(at)).await;
                    continue;
                }
                if at > here && at < start + next as u64 {
                    next = (at - start) as usize;
                }
            }
        }
        if shared.stalled(connection) {
            std::future::pending::<()>().await;
        }
        let data = &body[position..next];
        let written = if chunked {
            let mut framed = format!("{:x}\r\n", data.len()).into_bytes();
            framed.extend_from_slice(data);
            framed.extend_from_slice(b"\r\n");
            write.write_all(&framed).await
        } else {
            write.write_all(data).await
        };
        if written.is_err() {
            return false;
        }
        shared.log_sent(index, data.len() as u64);
        position = next;
        if let Some(rate) = crawl {
            tokio::time::sleep(Duration::from_secs_f64(data.len() as f64 / rate as f64)).await;
        }
        shared.throttle(peer, data.len() as u64).await;
    }
    match then {
        Then::Cut => {
            let _ = write.flush().await;
            false
        }
        Then::Stall => std::future::pending().await,
        Then::End => !chunked || write.write_all(b"0\r\n\r\n").await.is_ok(),
    }
}

fn head(status: u16, headers: &[(String, String)]) -> String {
    let mut head = format!("HTTP/1.1 {status} Test\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    head
}

async fn send_bare(write: &mut OwnedWriteHalf, status: u16, extra: &[(String, String)]) -> bool {
    let mut headers = extra.to_vec();
    headers.push(("Content-Length".into(), "0".into()));
    write.write_all(head(status, &headers).as_bytes()).await.is_ok()
}
