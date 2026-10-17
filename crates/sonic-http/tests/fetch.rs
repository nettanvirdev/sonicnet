//! fetch() and fetch_sample() against the test origin: every response check, into a real
//! staging file.

mod support;

use std::time::Duration;

use sonic_core::{ByteRange, FileVersion, VersionCheck};
use sonic_http::{
    FetchError, FetchEvents, FetchRequest, ResponseInfo, StagingFile, fetch, fetch_sample, reserve,
};
use support::{Fault, Origin, Served, connection};
use tokio_util::sync::CancellationToken;

const SIZE: usize = 64 * 1024;

#[derive(Default, Debug)]
struct Recorder {
    responses: Vec<ResponseInfo>,
    received: u64,
    written: Vec<u64>,
    waits: Vec<bool>,
    throttled: u64,
    throttle_for: Duration,
}

impl FetchEvents for Recorder {
    fn response(&mut self, info: ResponseInfo) {
        self.responses.push(info);
    }
    fn received(&mut self, bytes: u64) {
        self.received += bytes;
    }
    fn written(&mut self, total: u64) {
        assert!(self.written.last().is_none_or(|&last| total >= last), "progress went back");
        self.written.push(total);
    }
    fn write_wait(&mut self, waiting: bool) {
        self.waits.push(waiting);
    }
    fn throttle(&mut self, bytes: u64) -> Duration {
        let first = self.throttled == 0;
        self.throttled += bytes;
        if first { self.throttle_for } else { Duration::ZERO }
    }
}

impl Recorder {
    fn frontier(&self) -> u64 {
        self.written.last().copied().unwrap_or(0)
    }
}

struct Setup {
    origin: Origin,
    served: Served,
    file: StagingFile,
    _dir: tempfile::TempDir,
}

async fn setup(
    handler: impl Fn(&support::Req, &mut Served) -> Fault + Send + Sync + 'static,
) -> Setup {
    let served = Served::new(SIZE);
    let origin = Origin::start(served.clone(), handler).await;
    let dir = tempfile::tempdir().unwrap();
    let file = reserve(dir.path(), "file.bin").await.unwrap();
    Setup { origin, served, file, _dir: dir }
}

fn accepted(served: &Served) -> Vec<FileVersion> {
    vec![FileVersion {
        etag: served.etag.clone(),
        last_modified: served.last_modified.clone(),
        total_bytes: Some(served.bytes.len() as u64),
    }]
}

fn request<'a>(
    url: &'a str,
    versions: &'a [FileVersion],
    start: u64,
    end: Option<u64>,
) -> FetchRequest<'a> {
    FetchRequest {
        url,
        browser: None,
        range: ByteRange::new(start, end),
        whole_file: false,
        accepted_versions: versions,
        stall_timeout: Duration::from_secs(2),
        cut: None,
    }
}

async fn run(s: &Setup, req: &FetchRequest<'_>, events: &mut Recorder) -> Result<u64, FetchError> {
    fetch(&mut connection(), req, &s.file, events, &CancellationToken::new()).await
}

async fn on_disk(s: &Setup, start: u64, len: usize) -> Vec<u8> {
    s.file.read(start, len).await.unwrap()
}

#[tokio::test]
async fn ranges_land_at_their_offsets() {
    let s = setup(|_, _| Fault::Ok).await;
    let url = s.origin.url("/f");
    let versions = accepted(&s.served);
    let mut conn = connection();
    // Out of order, on one kept-alive connection.
    for (start, end) in [(40_000, 64 * 1024), (0, 20_000), (20_000, 40_000)] {
        let mut events = Recorder::default();
        let req = request(&url, &versions, start, Some(end));
        let n =
            fetch(&mut conn, &req, &s.file, &mut events, &CancellationToken::new()).await.unwrap();
        assert_eq!(n, end - start);
        assert_eq!(events.frontier(), end - start);
        assert_eq!(events.received, end - start);
        assert_eq!(events.responses.len(), 1);
    }
    assert_eq!(on_disk(&s, 0, SIZE).await, *s.served.bytes);
    assert_eq!(s.origin.connections(), 1);
    let ranges: Vec<_> = s.origin.requests().iter().map(|r| r.range).collect();
    assert_eq!(
        ranges,
        [Some((40_000, Some(65_535))), Some((0, Some(19_999))), Some((20_000, Some(39_999)))]
    );
}

#[tokio::test]
async fn whole_file_mode_sends_no_range() {
    let s = setup(|_, _| Fault::IgnoreRange).await;
    let url = s.origin.url("/f");
    let versions = accepted(&s.served);
    let mut events = Recorder::default();
    let mut req = request(&url, &versions, 0, Some(SIZE as u64));
    req.whole_file = true;
    assert_eq!(run(&s, &req, &mut events).await.unwrap(), SIZE as u64);
    // Open-ended from 0 (size unknown) asks for no range either.
    let req = request(&url, &[], 0, None);
    assert_eq!(run(&s, &req, &mut Recorder::default()).await.unwrap(), SIZE as u64);
    assert!(s.origin.requests().iter().all(|r| r.range.is_none()));
    assert_eq!(on_disk(&s, 0, SIZE).await, *s.served.bytes);
}

#[tokio::test]
async fn ignored_range_from_byte_zero_keeps_only_the_block() {
    // The server sends the whole file for the first block: the block's bytes are right and are
    // kept; the rest would overwrite the next block, so it's cut and the response fails.
    let s = setup(|_, _| Fault::IgnoreRange).await;
    let url = s.origin.url("/f");
    let versions = accepted(&s.served);
    let mut events = Recorder::default();
    let error = run(&s, &request(&url, &versions, 0, Some(1000)), &mut events).await.unwrap_err();
    assert!(matches!(error, FetchError::Protocol(_)), "{error:?}");
    assert_eq!(events.frontier(), 1000);
    assert_eq!(on_disk(&s, 0, 1000).await, s.served.bytes[..1000]);
    assert_eq!(s.file.size().await.unwrap(), 1000, "nothing past the block was written");
}

#[tokio::test]
async fn ignored_range_past_byte_zero_is_refused() {
    let s = setup(|_, _| Fault::IgnoreRange).await;
    let url = s.origin.url("/f");
    let versions = accepted(&s.served);
    let mut events = Recorder::default();
    let error =
        run(&s, &request(&url, &versions, 1000, Some(2000)), &mut events).await.unwrap_err();
    assert!(matches!(error, FetchError::Status { code: 200, .. }), "{error:?}");
    assert!(events.written.is_empty() && events.responses.is_empty());
}

#[tokio::test]
async fn wrong_start_writes_nothing() {
    let s = setup(|_, _| Fault::WrongStart).await;
    let url = s.origin.url("/f");
    let versions = accepted(&s.served);
    let mut events = Recorder::default();
    let error = run(&s, &request(&url, &versions, 100, Some(200)), &mut events).await.unwrap_err();
    assert!(matches!(error, FetchError::Protocol(ref m) if m.contains("wrong range")), "{error:?}");
    assert!(events.written.is_empty());
    assert_eq!(s.file.size().await.unwrap(), 0);
}

#[tokio::test]
async fn missing_content_range_writes_nothing() {
    let s = setup(|_, _| Fault::NoContentRange).await;
    let url = s.origin.url("/f");
    let versions = accepted(&s.served);
    let error = run(&s, &request(&url, &versions, 100, Some(200)), &mut Recorder::default())
        .await
        .unwrap_err();
    assert!(matches!(error, FetchError::Protocol(_)), "{error:?}");
    assert_eq!(s.file.size().await.unwrap(), 0);
}

#[tokio::test]
async fn content_range_past_the_block_writes_nothing() {
    let s = setup(|_, _| Fault::OverrunHeader).await;
    let url = s.origin.url("/f");
    let versions = accepted(&s.served);
    let mut events = Recorder::default();
    let error = run(&s, &request(&url, &versions, 100, Some(200)), &mut events).await.unwrap_err();
    assert!(
        matches!(error, FetchError::Protocol(ref m) if m.contains("more than the range")),
        "{error:?}"
    );
    assert!(events.written.is_empty());
    assert_eq!(s.file.size().await.unwrap(), 0);
}

#[tokio::test]
async fn overlong_body_is_cut_at_the_boundary() {
    let s = setup(|_, _| Fault::Overlong).await;
    let url = s.origin.url("/f");
    let versions = accepted(&s.served);
    let mut events = Recorder::default();
    let error = run(&s, &request(&url, &versions, 100, Some(200)), &mut events).await.unwrap_err();
    assert!(matches!(error, FetchError::Protocol(ref m) if m.contains("more data")), "{error:?}");
    // The range itself was right and is kept; the extra byte never reached the file.
    assert_eq!(events.frontier(), 100);
    assert_eq!(on_disk(&s, 100, 100).await, s.served.bytes[100..200]);
    assert_eq!(s.file.size().await.unwrap(), 200);
}

/// A long range whose tail was handed to someone else: it stops at the cut, keeping exactly what
/// came before it, and the connection still serves the next request.
#[tokio::test]
async fn a_cut_range_stops_at_the_cut() {
    let s = setup(|_, _| Fault::Ok).await;
    let url = s.origin.url("/f");
    let versions = accepted(&s.served);
    let cut = std::sync::atomic::AtomicU64::new(30_000);
    let mut conn = connection();
    let mut events = Recorder::default();
    let req = FetchRequest { cut: Some(&cut), ..request(&url, &versions, 10_000, Some(60_000)) };
    let n = fetch(&mut conn, &req, &s.file, &mut events, &CancellationToken::new()).await.unwrap();
    assert_eq!(n, 20_000);
    assert_eq!(events.frontier(), 20_000);
    assert_eq!(on_disk(&s, 10_000, 20_000).await, s.served.bytes[10_000..30_000]);
    // A cut at the range's end is no cut.
    let cut = std::sync::atomic::AtomicU64::new(10_000);
    let req = FetchRequest { cut: Some(&cut), ..request(&url, &versions, 0, Some(10_000)) };
    let mut events = Recorder::default();
    let n = fetch(&mut conn, &req, &s.file, &mut events, &CancellationToken::new()).await.unwrap();
    assert_eq!(n, 10_000);
    assert_eq!(on_disk(&s, 0, 30_000).await, s.served.bytes[..30_000]);
}

#[tokio::test]
async fn short_body_fails_but_keeps_what_came() {
    let s = setup(|_, _| Fault::EndAfter(30)).await;
    let url = s.origin.url("/f");
    let versions = accepted(&s.served);
    let mut events = Recorder::default();
    let error = run(&s, &request(&url, &versions, 100, Some(200)), &mut events).await.unwrap_err();
    assert!(matches!(error, FetchError::Protocol(ref m) if m.contains("30 bytes")), "{error:?}");
    assert_eq!(events.frontier(), 30);
    assert_eq!(on_disk(&s, 100, 30).await, s.served.bytes[100..130]);
}

#[tokio::test]
async fn dropped_connection_mid_body() {
    let s = setup(|_, _| Fault::CutAfter(40)).await;
    let url = s.origin.url("/f");
    let versions = accepted(&s.served);
    let mut events = Recorder::default();
    let error = run(&s, &request(&url, &versions, 0, Some(1000)), &mut events).await.unwrap_err();
    assert!(matches!(error, FetchError::Connection(_)), "{error:?}");
    assert_eq!(events.frontier(), 40);
}

#[tokio::test]
async fn stalled_body_is_a_connection_error() {
    let s = setup(|_, _| Fault::StallBody).await;
    let url = s.origin.url("/f");
    let versions = accepted(&s.served);
    let mut req = request(&url, &versions, 0, Some(1000));
    req.stall_timeout = Duration::from_millis(300);
    let started = std::time::Instant::now();
    let error = run(&s, &req, &mut Recorder::default()).await.unwrap_err();
    assert!(
        matches!(error, FetchError::Connection(sonic_http::ConnectionError::Stalled)),
        "{error:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn throttling_does_not_count_as_a_stall() {
    // The body comes in two pieces; the first holds reading for longer than the stall timeout.
    let s = setup(|_, _| Fault::Pieces(Duration::from_millis(20))).await;
    let url = s.origin.url("/f");
    let versions = accepted(&s.served);
    let mut req = request(&url, &versions, 0, Some(SIZE as u64));
    req.stall_timeout = Duration::from_millis(200);
    let mut events = Recorder { throttle_for: Duration::from_millis(500), ..Default::default() };
    assert_eq!(run(&s, &req, &mut events).await.unwrap(), SIZE as u64);
    assert_eq!(events.throttled, SIZE as u64);
}

#[tokio::test]
async fn statuses_carry_retry_after() {
    let s = setup(|req, _| match req.n {
        1 => Fault::Status(503, vec![("Retry-After", "7".into())]),
        2 => Fault::Status(
            429,
            vec![(
                "Retry-After",
                httpdate::fmt_http_date(std::time::SystemTime::now() + Duration::from_secs(60)),
            )],
        ),
        _ => Fault::Status(404, vec![]),
    })
    .await;
    let url = s.origin.url("/f");
    let versions = accepted(&s.served);
    let req = request(&url, &versions, 0, Some(10));
    let error = run(&s, &req, &mut Recorder::default()).await.unwrap_err();
    assert!(error.is_transient());
    assert!(
        matches!(error, FetchError::Status { code: 503, retry_after: Some(d) } if d == Duration::from_secs(7)),
        "{error:?}"
    );
    let error = run(&s, &req, &mut Recorder::default()).await.unwrap_err();
    let FetchError::Status { code: 429, retry_after: Some(wait) } = error else {
        panic!("{error:?}")
    };
    assert!(wait > Duration::from_secs(50) && wait <= Duration::from_secs(60), "{wait:?}");
    let error = run(&s, &req, &mut Recorder::default()).await.unwrap_err();
    assert!(!error.is_transient());
    assert!(matches!(error, FetchError::Status { code: 404, retry_after: None }), "{error:?}");
}

#[tokio::test]
async fn redirects_mid_download() {
    let s = setup(|req, _| match req.path.as_str() {
        "/f" => Fault::Redirect("signed?sig=2".into()),
        "/signed?sig=2" => Fault::Redirect("/final".into()),
        _ => Fault::Ok,
    })
    .await;
    let url = s.origin.url("/f");
    let versions = accepted(&s.served);
    let n = run(&s, &request(&url, &versions, 500, Some(1500)), &mut Recorder::default())
        .await
        .unwrap();
    assert_eq!(n, 1000);
    assert_eq!(on_disk(&s, 500, 1000).await, s.served.bytes[500..1500]);
    let requests = s.origin.requests();
    assert_eq!(requests.last().unwrap().path, "/final");
    // The Range goes with every hop.
    assert!(requests.iter().all(|r| r.range == Some((500, Some(1499)))));
}

#[tokio::test]
async fn redirect_loop_fails() {
    let s = setup(|_, _| Fault::Redirect("/again".into())).await;
    let url = s.origin.url("/f");
    let error =
        run(&s, &request(&url, &[], 0, Some(10)), &mut Recorder::default()).await.unwrap_err();
    assert!(matches!(error, FetchError::Protocol(ref m) if m.contains("redirects")), "{error:?}");
}

#[tokio::test]
async fn changed_file_writes_nothing() {
    // From the second request on, the server has a new version of the file.
    let s = setup(|req, served| {
        if req.n == 2 {
            served.etag = Some("\"v2\"".into());
        }
        Fault::Ok
    })
    .await;
    let url = s.origin.url("/f");
    let versions = accepted(&s.served);
    run(&s, &request(&url, &versions, 0, Some(100)), &mut Recorder::default()).await.unwrap();
    let mut events = Recorder::default();
    let error = run(&s, &request(&url, &versions, 100, Some(200)), &mut events).await.unwrap_err();
    let FetchError::RemoteChanged { check, seen } = error else { panic!("{error:?}") };
    assert!(matches!(check, VersionCheck::Validators { .. }));
    assert_eq!(seen.etag.as_deref(), Some("\"v2\""));
    assert_eq!(seen.total_bytes, Some(SIZE as u64));
    assert!(events.written.is_empty());
    assert_eq!(s.file.size().await.unwrap(), 100);

    // Once the caller confirms v2 serves the same bytes, it is accepted.
    let mut both = versions.clone();
    both.push(seen);
    run(&s, &request(&url, &both, 100, Some(200)), &mut Recorder::default()).await.unwrap();
}

#[tokio::test]
async fn resized_file_is_proof_of_change() {
    let s = setup(|_, served| {
        served.bytes = std::sync::Arc::new(support::seeded_bytes(SIZE + 1, 7));
        Fault::Ok
    })
    .await;
    let url = s.origin.url("/f");
    let versions = accepted(&s.served);
    let error = run(&s, &request(&url, &versions, 0, Some(100)), &mut Recorder::default())
        .await
        .unwrap_err();
    assert!(
        matches!(error, FetchError::RemoteChanged { check: VersionCheck::Size { .. }, .. }),
        "{error:?}"
    );
    assert_eq!(s.file.size().await.unwrap(), 0);
}

#[tokio::test]
async fn cancelling_stops_and_keeps_what_was_written() {
    let s = setup(|_, _| Fault::Pieces(Duration::from_secs(5))).await;
    let url = s.origin.url("/f");
    let versions = accepted(&s.served);
    let cancel = CancellationToken::new();
    let canceller = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        canceller.cancel();
    });
    let mut events = Recorder::default();
    let req = request(&url, &versions, 0, Some(SIZE as u64));
    let started = std::time::Instant::now();
    let error = fetch(&mut connection(), &req, &s.file, &mut events, &cancel).await.unwrap_err();
    assert!(matches!(error, FetchError::Cancelled), "{error:?}");
    assert!(started.elapsed() < Duration::from_secs(2));
    let half = SIZE as u64 / 2;
    assert_eq!(events.frontier(), half);
    assert_eq!(on_disk(&s, 0, SIZE / 2).await, s.served.bytes[..SIZE / 2]);
}

#[tokio::test]
async fn large_bodies_report_waiting_on_the_disk() {
    let served = Served::new(4 * 1024 * 1024);
    let origin = Origin::ok(served.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let file = reserve(dir.path(), "big.bin").await.unwrap();
    let url = origin.url("/f");
    let versions = accepted(&served);
    let req = request(&url, &versions, 0, Some(served.bytes.len() as u64));
    let mut events = Recorder::default();
    let n = fetch(&mut connection(), &req, &file, &mut events, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(n, served.bytes.len() as u64);
    assert_eq!(file.read(0, served.bytes.len()).await.unwrap(), *served.bytes);
    // Every wait ends; the last is the final flush.
    assert!(events.waits.len() >= 2 && events.waits.len() % 2 == 0);
    assert!(events.waits.chunks(2).all(|pair| pair == [true, false]));
    assert!(events.written.len() > 1, "progress is reported as it goes");
}

#[tokio::test]
async fn samples_come_back_in_memory() {
    let s = setup(|_, _| Fault::Ok).await;
    let url = s.origin.url("/f");
    let (bytes, version) = fetch_sample(
        &mut connection(),
        &url,
        None,
        ByteRange::new(10, Some(20)),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    assert_eq!(&bytes[..], &s.served.bytes[10..20]);
    assert_eq!(version, accepted(&s.served)[0]);
    assert_eq!(s.file.size().await.unwrap(), 0);
}

#[tokio::test]
async fn samples_are_checked() {
    let s = setup(|req, _| match req.n {
        1 => Fault::WrongStart,
        2 => Fault::EndAfter(3),
        3 => Fault::IgnoreRange,
        _ => Fault::Overlong,
    })
    .await;
    let url = s.origin.url("/f");
    let mut conn = connection();
    for _ in 0..4 {
        let result = fetch_sample(
            &mut conn,
            &url,
            None,
            ByteRange::new(10, Some(20)),
            Duration::from_secs(2),
        )
        .await;
        assert!(result.is_err(), "{result:?}");
    }
    let open =
        fetch_sample(&mut conn, &url, None, ByteRange::new(10, None), Duration::from_secs(2)).await;
    assert!(matches!(open, Err(FetchError::Protocol(_))));
}

#[tokio::test]
async fn empty_range_needs_no_request() {
    let s = setup(|_, _| Fault::Ok).await;
    let url = s.origin.url("/f");
    assert_eq!(
        run(&s, &request(&url, &[], 5, Some(5)), &mut Recorder::default()).await.unwrap(),
        0
    );
    assert!(s.origin.requests().is_empty());
}
