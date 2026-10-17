//! The origin's rules behave as specified, the networks controller works, and the checks catch
//! what they are meant to — the scenario tests trust all three.

use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use sonic_core::{
    BlockStatus, DownloadId, DownloadNetwork, DownloadState, DownloadStatus, NetworkKind,
    NetworkStatus, StreamState, StreamStatus, plan_blocks,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpSocket, TcpStream};
use tokio::time::timeout;

use crate::*;

#[derive(Debug)]
struct Response {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Response {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
    }
}

type Conn = BufReader<TcpStream>;

async fn connect_from(origin: &Origin, from: Ipv4Addr) -> Conn {
    let socket = TcpSocket::new_v4().unwrap();
    socket.bind((from, 0).into()).unwrap();
    BufReader::new(socket.connect(origin.addr()).await.unwrap())
}

async fn connect(origin: &Origin) -> Conn {
    connect_from(origin, Ipv4Addr::LOCALHOST).await
}

/// One request on `conn`. The body is read per Content-Length (stopping early at EOF), chunked
/// encoding, or to EOF.
async fn send(conn: &mut Conn, method: &str, path: &str, headers: &[(&str, &str)]) -> Response {
    let mut head = format!("{method} /{path} HTTP/1.1\r\nHost: test\r\n");
    for (k, v) in headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str("\r\n");
    conn.get_mut().write_all(head.as_bytes()).await.unwrap();

    let mut line = String::new();
    conn.read_line(&mut line).await.unwrap();
    let status = line.split_whitespace().nth(1).expect("a status line").parse().unwrap();
    let mut headers = Vec::new();
    loop {
        line.clear();
        conn.read_line(&mut line).await.unwrap();
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            break;
        }
        let (k, v) = trimmed.split_once(':').unwrap();
        headers.push((k.trim().to_owned(), v.trim().to_owned()));
    }
    let mut response = Response { status, headers, body: Vec::new() };
    if method == "HEAD" {
        return response;
    }
    if let Some(len) = response.header("content-length") {
        let len: u64 = len.parse().unwrap();
        let mut body = Vec::new();
        let _ = (&mut *conn).take(len).read_to_end(&mut body).await;
        response.body = body;
    } else if response.header("transfer-encoding") == Some("chunked") {
        loop {
            line.clear();
            conn.read_line(&mut line).await.unwrap();
            let size = usize::from_str_radix(line.trim(), 16).unwrap();
            let mut chunk = vec![0; size + 2];
            conn.read_exact(&mut chunk).await.unwrap();
            if size == 0 {
                break;
            }
            response.body.extend_from_slice(&chunk[..size]);
        }
    } else {
        let _ = conn.read_to_end(&mut response.body).await;
    }
    response
}

async fn get(origin: &Origin, path: &str, headers: &[(&str, &str)]) -> Response {
    send(&mut connect(origin).await, "GET", path, headers).await
}

fn range(first: u64, last: u64) -> String {
    format!("bytes={first}-{last}")
}

#[tokio::test]
async fn serves_whole_files_and_ranges_on_one_kept_alive_connection() {
    let origin = Origin::start().await;
    let bytes = origin.add("a.bin", 100_000);
    assert_eq!(bytes.len(), 100_000);
    assert_ne!(*bytes, *origin.add("b.bin", 100_000), "each file has its own bytes");

    let mut conn = connect(&origin).await;
    let whole = send(&mut conn, "GET", "a.bin", &[]).await;
    assert_eq!(whole.status, 200);
    assert_eq!(whole.body, *bytes);
    assert_eq!(whole.header("accept-ranges"), Some("bytes"));
    assert_eq!(whole.header("etag"), Some("\"a.bin-v1\""));

    let part = send(&mut conn, "GET", "a.bin", &[("Range", &range(10, 19))]).await;
    assert_eq!(part.status, 206);
    assert_eq!(part.header("content-range"), Some("bytes 10-19/100000"));
    assert_eq!(part.body, bytes[10..20]);
    let open = send(&mut conn, "GET", "a.bin?x=1", &[("Range", "bytes=99990-")]).await;
    assert_eq!(open.body, bytes[99_990..]);
    let head = send(&mut conn, "HEAD", "a.bin", &[]).await;
    assert_eq!(head.header("content-length"), Some("100000"));
    assert_eq!(origin.connections(), 1, "keep-alive");

    let log = origin.requests();
    assert_eq!(log.len(), 4);
    assert_eq!(log[1].range, Some((10, Some(19))));
    assert_eq!(log[1].bytes_sent, 10);
    assert_eq!(log[2].name, "a.bin", "the query is not part of the name");
    assert_eq!(log[0].peer, Ipv4Addr::LOCALHOST);
    assert_eq!(get(&origin, "missing.bin", &[]).await.status, 404);
}

#[tokio::test]
async fn past_the_end_is_416_and_an_empty_file_says_so() {
    let origin = Origin::start().await;
    origin.add("a.bin", 1000);
    origin.add("empty.bin", 0);
    let past = get(&origin, "a.bin", &[("Range", "bytes=1000-")]).await;
    assert_eq!((past.status, past.header("content-range")), (416, Some("bytes */1000")));
    let probe = get(&origin, "empty.bin", &[("Range", &range(0, 1023))]).await;
    assert_eq!((probe.status, probe.header("content-range")), (416, Some("bytes */0")));
    let whole = get(&origin, "empty.bin", &[]).await;
    assert_eq!((whole.status, whole.body.len()), (200, 0));
}

#[tokio::test]
async fn wrong_answers_to_a_range() {
    let origin = Origin::start().await;
    let bytes = origin.add("a.bin", 1000);
    origin.fault_next(
        "a.bin",
        [
            Fault::WrongStart,
            Fault::Overlong,
            Fault::ShortBody(3),
            Fault::NoContentRange,
            Fault::IgnoreRange,
            Fault::Etag("\"other\"".into()),
        ],
    );
    let ask = || get(&origin, "a.bin", &[("Range", "bytes=10-19")]);

    let wrong_start = ask().await;
    assert_eq!(wrong_start.header("content-range"), Some("bytes 11-19/1000"));
    assert_eq!(wrong_start.body, bytes[10..20]);
    let overlong = ask().await;
    assert_eq!(overlong.header("content-range"), Some("bytes 10-19/1000"));
    assert_eq!(overlong.body, bytes[10..21]);
    let short = ask().await;
    assert_eq!((short.header("content-length"), short.body.len()), (Some("3"), 3));
    let no_range = ask().await;
    assert_eq!((no_range.status, no_range.header("content-range")), (206, None));
    let ignored = ask().await;
    assert_eq!((ignored.status, ignored.body.len()), (200, 1000));
    let relabelled = ask().await;
    assert_eq!(relabelled.header("etag"), Some("\"other\""));
    assert_eq!(relabelled.body, bytes[10..20]);
    assert_eq!(ask().await.body, bytes[10..20], "the queue is spent");

    let faults: Vec<_> = origin.requests().into_iter().map(|r| r.fault).collect();
    assert_eq!(faults[0], Some(Fault::WrongStart));
    assert_eq!(faults[6], None);
}

#[tokio::test]
async fn faults_skip_the_probe_and_always_applies_until_cleared() {
    let origin = Origin::start().await;
    origin.add("a.bin", 5000);
    origin.fault("a.bin", Fault::Status { code: 503, retry_after: Some(2) });
    let probe = get(&origin, "a.bin", &[("Range", &range(0, 1023))]).await;
    assert_eq!(probe.status, 206, "the probe is never faulted by the file's rules");
    for _ in 0..2 {
        let busy = get(&origin, "a.bin", &[("Range", "bytes=0-99")]).await;
        assert_eq!((busy.status, busy.header("retry-after")), (503, Some("2")));
    }
    origin.clear_faults("a.bin");
    assert_eq!(get(&origin, "a.bin", &[("Range", "bytes=0-99")]).await.status, 206);
}

#[tokio::test]
async fn a_rule_decides_per_request_by_peer() {
    let origin = Origin::start().await;
    origin.add("a.bin", 5000);
    let blocked = net_ip(2);
    origin.set_rule(move |r| (r.peer == blocked).then(|| Fault::status(500)));
    let mut over_two = connect_from(&origin, net_ip(2)).await;
    let mut over_one = connect_from(&origin, net_ip(1)).await;
    assert_eq!(send(&mut over_two, "GET", "a.bin", &[]).await.status, 500);
    assert_eq!(send(&mut over_one, "GET", "a.bin", &[]).await.status, 200);
    origin.clear_rule();
    assert_eq!(send(&mut over_two, "GET", "a.bin", &[]).await.status, 200);
    assert_eq!(origin.requests()[0].peer, net_ip(2));
}

#[tokio::test]
async fn cut_connections_and_stalls() {
    let origin = Origin::start().await;
    let bytes = origin.add("a.bin", 50_000);
    origin.fault_next("a.bin", [Fault::CutAfter(1000), Fault::StallBody(500), Fault::StallHeaders]);

    let cut = get(&origin, "a.bin", &[]).await;
    assert_eq!(cut.header("content-length"), Some("50000"));
    assert_eq!(cut.body, bytes[..1000], "1000 bytes, then the connection dropped");

    let mut conn = connect(&origin).await;
    conn.get_mut().write_all(b"GET /a.bin HTTP/1.1\r\nHost: t\r\n\r\n").await.unwrap();
    let mut received = Vec::new();
    let read = timeout(Duration::from_millis(400), async {
        loop {
            let mut buf = [0u8; 4096];
            let n = conn.read(&mut buf).await.unwrap();
            received.extend_from_slice(&buf[..n]);
        }
    })
    .await;
    assert!(read.is_err(), "still open, silent");
    let head_end = received.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    assert_eq!(received[head_end..], bytes[..500]);

    let stalled = timeout(Duration::from_millis(300), get(&origin, "a.bin", &[])).await;
    assert!(stalled.is_err(), "no headers ever");
}

#[tokio::test]
async fn crawling_and_per_address_limits_pace_the_body() {
    let origin = Origin::start().await;
    let bytes = origin.add("a.bin", 32 * 1024);
    origin.fault_next("a.bin", [Fault::Crawl(40_000)]);
    let started = Instant::now();
    assert_eq!(get(&origin, "a.bin", &[("Range", "bytes=0-7999")]).await.body, bytes[..8000]);
    assert!(started.elapsed() >= Duration::from_millis(150), "{:?}", started.elapsed());

    // 32 KiB at 64 KiB/s shared by two connections from 127.0.0.2: half a second at least.
    origin.limit_ip(net_ip(1), Some(64 * 1024));
    let started = Instant::now();
    let (a, b) = tokio::join!(
        async {
            send(
                &mut connect_from(&origin, net_ip(1)).await,
                "GET",
                "a.bin",
                &[("Range", "bytes=0-16383")],
            )
            .await
        },
        async {
            send(
                &mut connect_from(&origin, net_ip(1)).await,
                "GET",
                "a.bin",
                &[("Range", "bytes=16384-32767")],
            )
            .await
        },
    );
    assert_eq!([a.body, b.body].concat(), *bytes);
    let took = started.elapsed();
    assert!(took >= Duration::from_millis(400), "the limit is shared: {took:?}");
    assert_eq!(origin.bytes_to(net_ip(1)), 32 * 1024);
    assert_eq!(origin.bytes_to(net_ip(2)), 0);

    // Another address isn't held back by it.
    let started = Instant::now();
    send(&mut connect_from(&origin, net_ip(2)).await, "GET", "a.bin", &[]).await;
    assert!(started.elapsed() < Duration::from_millis(300));
}

#[tokio::test]
async fn files_without_ranges_unknown_size_or_refusing_ranges() {
    let origin = Origin::start().await;
    let plain = origin.add("plain.bin", 3000);
    origin.edit("plain.bin", |f| f.ranges = false);
    let answer = get(&origin, "plain.bin", &[("Range", &range(0, 1023))]).await;
    assert_eq!((answer.status, answer.header("accept-ranges")), (200, None));
    assert_eq!(answer.body, *plain);

    let unknown = origin.add("unknown.bin", 70_000);
    origin.edit("unknown.bin", |f| f.unknown_size = true);
    let mut conn = connect(&origin).await;
    let answer = send(&mut conn, "GET", "unknown.bin", &[("Range", "bytes=100-199")]).await;
    assert_eq!((answer.status, answer.header("content-length")), (200, None));
    assert_eq!(answer.body, *unknown);
    assert_eq!(send(&mut conn, "GET", "unknown.bin", &[]).await.body, *unknown, "kept alive");

    origin.add("refusing.bin", 3000);
    origin.edit("refusing.bin", |f| f.refuse_ranges = true);
    assert_eq!(get(&origin, "refusing.bin", &[("Range", "bytes=0-9")]).await.status, 403);
    assert_eq!(get(&origin, "refusing.bin", &[]).await.status, 200);
}

#[tokio::test]
async fn redirects_and_cookies() {
    let origin = Origin::start().await;
    origin.add("real.bin", 100);
    origin.copy("real.bin", "once.bin");
    origin.copy("real.bin", "always.bin");
    origin.redirect("once.bin", "/real.bin", true);
    origin.redirect("always.bin", "/real.bin", false);
    for _ in 0..2 {
        let answer = get(&origin, "always.bin", &[]).await;
        assert_eq!((answer.status, answer.header("location")), (302, Some("/real.bin")));
    }
    assert_eq!(get(&origin, "once.bin", &[]).await.status, 302);
    assert_eq!(get(&origin, "once.bin", &[]).await.status, 200);

    origin.require_cookie("real.bin", "session=s3cret", 403);
    assert_eq!(get(&origin, "real.bin", &[]).await.status, 403);
    assert_eq!(get(&origin, "real.bin", &[("Cookie", "a=1; session=wrong")]).await.status, 403);
    assert_eq!(get(&origin, "real.bin", &[("Cookie", "a=1; session=s3cret")]).await.status, 200);
}

#[tokio::test]
async fn new_versions_and_new_labels() {
    let origin = Origin::start().await;
    let v1 = origin.add("a.bin", 4096);
    origin.relabel("a.bin", "\"elsewhere\"");
    let relabelled = get(&origin, "a.bin", &[]).await;
    assert_eq!(
        (relabelled.header("etag"), relabelled.body.clone()),
        (Some("\"elsewhere\""), v1.to_vec())
    );

    let v2 = origin.change_version("a.bin", None);
    assert_eq!(v2.len(), v1.len());
    assert_ne!(v2, v1);
    let changed = get(&origin, "a.bin", &[]).await;
    assert_eq!(changed.header("etag"), Some("\"a.bin-v2\""));
    assert_eq!(changed.body, *v2);
    assert_eq!(origin.expected_bytes("a.bin"), v2);
    assert_eq!(origin.change_version("a.bin", Some(10)).len(), 10);
}

#[tokio::test]
async fn hold_freezes_every_transfer_at_one_byte() {
    let origin = Origin::start().await;
    let bytes = origin.add("a.bin", 20_000);
    let reached = origin.hold(5000);
    let probe = get(&origin, "a.bin", &[("Range", &range(0, 1023))]).await;
    assert_eq!(probe.body.len(), 1024, "the probe is never held");

    let crossing = tokio::spawn({
        let url_addr = origin.addr();
        async move {
            let mut conn = BufReader::new(TcpStream::connect(url_addr).await.unwrap());
            send(&mut conn, "GET", "a.bin", &[("Range", "bytes=0-9999")]).await
        }
    });
    let after = tokio::spawn({
        let url_addr = origin.addr();
        async move {
            let mut conn = BufReader::new(TcpStream::connect(url_addr).await.unwrap());
            send(&mut conn, "GET", "a.bin", &[("Range", "bytes=6000-6999")]).await
        }
    });
    timeout(Duration::from_secs(5), reached).await.expect("the hold is reached");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let log = origin.requests();
    assert_eq!(log[1].bytes_sent, 5000, "exactly the bytes before the held one");
    assert_eq!((log[2].status, log[2].bytes_sent), (0, 0), "a later range hasn't even started");

    origin.release();
    assert_eq!(crossing.await.unwrap().body, bytes[..10_000]);
    assert_eq!(after.await.unwrap().body, bytes[6000..7000]);
}

#[tokio::test]
async fn a_connection_limit_turns_an_address_away_while_it_has_too_many_open() {
    let origin = Origin::start().await;
    let bytes = origin.add("a.bin", 4096);
    origin.limit_connections(net_ip(1), Some(2));
    let mut a = connect_from(&origin, net_ip(1)).await;
    let mut b = connect_from(&origin, net_ip(1)).await;
    assert_eq!(send(&mut a, "GET", "a.bin", &[]).await.body, *bytes);
    assert_eq!(send(&mut b, "GET", "a.bin", &[]).await.status, 200);

    // A third open: every request from the address is turned away, the third's and those on
    // connections it was serving a moment ago alike, and each turned-away connection closes.
    let mut c = connect_from(&origin, net_ip(1)).await;
    let refused = send(&mut c, "GET", "a.bin", &[]).await;
    assert_eq!((refused.status, refused.header("connection")), (429, Some("close")));
    let (d, e) = (connect_from(&origin, net_ip(1)).await, connect_from(&origin, net_ip(1)).await);
    assert_eq!(send(&mut a, "GET", "a.bin", &[]).await.status, 429, "three are open");
    let other = send(&mut connect_from(&origin, net_ip(2)).await, "GET", "a.bin", &[]).await;
    assert_eq!(other.status, 200, "another address has its own");
    drop((c, d, e));

    // Back to two open (b, and a fresh one): served again.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(send(&mut b, "GET", "a.bin", &[]).await.body, *bytes);
    let mut fresh = connect_from(&origin, net_ip(1)).await;
    assert_eq!(send(&mut fresh, "GET", "a.bin", &[]).await.status, 200);
}

#[tokio::test]
async fn limits_on_every_address_together() {
    let origin = Origin::start().await;
    origin.add("a.bin", 4096);
    origin.limit_all_connections(Some(1));
    let mut a = connect_from(&origin, net_ip(1)).await;
    assert_eq!(send(&mut a, "GET", "a.bin", &[]).await.status, 200);
    let mut b = connect_from(&origin, net_ip(2)).await;
    assert_eq!(send(&mut b, "GET", "a.bin", &[]).await.status, 429, "one open between them");
    origin.limit_all_connections(None);

    // A burst of 2 beyond the first, then 10 a second: a refusal keeps its connection, and
    // doesn't count.
    origin.limit_requests(Some((10.0, 2.0)));
    let mut c = connect_from(&origin, net_ip(2)).await;
    let mut statuses = Vec::new();
    for _ in 0..5 {
        statuses.push(send(&mut c, "GET", "a.bin", &[]).await.status);
    }
    assert_eq!(statuses, [200, 200, 200, 429, 429]);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(send(&mut a, "GET", "a.bin", &[]).await.status, 200, "one more after 0.1 s");
}

#[tokio::test]
async fn stalled_connections_go_silent_and_new_ones_are_served() {
    let origin = Origin::start().await;
    let bytes = origin.add("a.bin", 64 * 1024);
    origin.limit_ip(Ipv4Addr::LOCALHOST, Some(32 * 1024));
    let mut old = connect(&origin).await;
    old.get_mut().write_all(b"GET /a.bin HTTP/1.1\r\nHost: t\r\n\r\n").await.unwrap();
    let mut first = [0u8; 1024];
    old.read_exact(&mut first).await.unwrap();

    origin.stall_connections();
    let read = timeout(Duration::from_millis(800), async {
        let mut rest = Vec::new();
        old.read_to_end(&mut rest).await.map(|_| rest.len())
    })
    .await;
    assert!(read.is_err(), "the open connection went silent before the end of the body");

    origin.limit_ip(Ipv4Addr::LOCALHOST, None);
    assert_eq!(get(&origin, "a.bin", &[]).await.body, *bytes, "a new connection is served");
}

#[test]
fn networks_come_and_go() {
    let networks = Networks::new(vec![network(1)]);
    let mut rx = networks.receiver();
    networks.add(network(2));
    assert!(rx.has_changed().unwrap());
    assert_eq!(rx.borrow_and_update().len(), 2);
    networks.replace(fake_network("net1", Ipv4Addr::new(127, 0, 0, 9)));
    assert_eq!(networks.list()[0].addresses[0].ip, Ipv4Addr::new(127, 0, 0, 9));
    networks.remove(&net(1));
    assert_eq!(networks.list().iter().map(|n| n.id.clone()).collect::<Vec<_>>(), [net(2)]);
    let n = network(3);
    assert_eq!((n.ipv4_index, n.has_gateway, n.addresses[0].ip), (1, true, net_ip(3).into()));
}

fn state(total: u64) -> DownloadState {
    DownloadState {
        id: DownloadId::new(),
        url: "http://x/a".into(),
        file_name: "a".into(),
        destination_path: PathBuf::from("a"),
        total_bytes: Some(total),
        bytes_downloaded: 0,
        speed_bps: 0.0,
        time_left_secs: None,
        status: DownloadStatus::Downloading,
        error: None,
        resumable: true,
        started_at: 0,
        queued_at: None,
        paused_at: None,
        total_paused_ms: 0,
        completed_at: None,
        scheduled: false,
        networks: vec![DownloadNetwork {
            id: net(1),
            label: "net1".into(),
            kind: NetworkKind::Ethernet,
            enabled: true,
            status: NetworkStatus::On,
            error: None,
            bytes_downloaded: 0,
            speed_bps: 0.0,
            retries: 0,
        }],
        streams: Vec::new(),
        peak_streams: 0,
        blocks: plan_blocks(Some(total), BLOCK),
        block_size: BLOCK,
        speed_history: BTreeMap::new(),
        peak_speed_bps: None,
        disk_limited: false,
        from_browser: None,
    }
}

fn credit(s: &mut DownloadState, block: usize, bytes: u64) {
    s.blocks[block].bytes_downloaded += bytes;
    *s.blocks[block].bytes_by_network.entry(net(1)).or_default() += bytes;
    s.networks[0].bytes_downloaded += bytes;
    s.bytes_downloaded += bytes;
}

#[test]
fn the_checks_pass_a_consistent_state_and_catch_each_kind_of_lie() {
    let mut good = state(3 * BLOCK);
    credit(&mut good, 0, BLOCK);
    good.blocks[0].status = BlockStatus::Completed;
    credit(&mut good, 1, 100);
    good.blocks[1].status = BlockStatus::Downloading;
    good.streams.push(StreamState {
        id: 1,
        network_id: net(1),
        status: StreamStatus::Downloading,
        block: Some(1),
        hedge: false,
        bytes_downloaded: 100,
        speed_bps: 1.0,
    });
    assert_eq!(state_violations(&good), Vec::<String>::new());

    let caught = |change: &dyn Fn(&mut DownloadState), what: &str| {
        let mut bad = good.clone();
        change(&mut bad);
        let found = state_violations(&bad);
        assert!(found.iter().any(|v| v.contains(what)), "{what:?} not in {found:?}");
    };
    caught(&|s| s.bytes_downloaded += 1, "blocks add up");
    caught(&|s| s.networks[0].bytes_downloaded += 1, "networks add up");
    caught(&|s| s.blocks[1].bytes_downloaded += 1, "bytes_by_network add up");
    caught(&|s| s.blocks[1].status = BlockStatus::Completed, "completed with 100");
    caught(&|s| s.total_bytes = Some(10), "> total");
    caught(
        &|s| s.streams.push(StreamState { id: 2, ..s.streams[0].clone() }),
        "2 streams fetching",
    );
    caught(
        &|s| {
            (2..5).for_each(|id| {
                s.streams.push(StreamState { id, hedge: true, ..s.streams[0].clone() })
            })
        },
        "3 hedges",
    );
    caught(&|s| s.streams[0].network_id = net(7), "doesn't list");
    caught(&|s| s.status = DownloadStatus::Completed, "completed with 65636");
    caught(&|s| s.streams[0].block = Some(0), "holds block 0, which is Completed");
    caught(&|s| s.streams[0].status = StreamStatus::Pending, "pending stream 1 holds");

    assert!(transition_violation(DownloadStatus::Completed, DownloadStatus::Downloading).is_some());
    assert!(transition_violation(DownloadStatus::Cancelled, DownloadStatus::Paused).is_some());
    assert!(transition_violation(DownloadStatus::Paused, DownloadStatus::Completed).is_none());
}

#[test]
fn compare_names_the_first_wrong_byte() {
    assert_eq!(compare(b"abc", b"abc"), None);
    assert_eq!(
        compare(b"abd", b"abc").unwrap(),
        "differs from the origin's bytes first at offset 2"
    );
    assert!(compare(b"ab", b"abc").unwrap().contains("is 2 bytes"));
    assert!(staging_path(std::path::Path::new("x/a.bin")).ends_with("a.bin.sonic"));
}
