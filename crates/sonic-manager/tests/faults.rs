//! A misbehaving server or network. The rule every case here must satisfy (and the harness
//! enforces at the end): a download never ends `completed` with wrong bytes. It either retries
//! its way to the exact file, or it errors with a reason.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use sonic_core::DownloadStatus;
use sonic_testkit::*;

const SIZE: u64 = 16 * BLOCK;

/// The first three download requests past the start of the file get `fault`; the rest are
/// served well. The download must come out exact, having retried.
async fn retried_to_the_exact_file(fault: Fault) {
    let origin = Origin::start().await;
    origin.add("a.bin", SIZE as usize);
    let faulted = Arc::new(AtomicUsize::new(0));
    let count = faulted.clone();
    origin.set_rule(move |r| {
        (!r.is_probe() && r.start() > 0 && count.fetch_add(1, Ordering::SeqCst) < 3)
            .then(|| fault.clone())
    });
    let h = Harness::new(1);

    let id = h.start(&origin, "a.bin", &[]).await;
    h.wait_status(id, DownloadStatus::Completed).await;
    assert!(faulted.load(Ordering::SeqCst) >= 3, "the fault was injected");
    // Retried as the server saw it: each faulted block was asked for again afterwards. (Not
    // `DownloadNetwork::retries`: the manager counts a stalled connection it replaces as a
    // refresh, not a failure.)
    // Only where the answer was short of the block: an overlong or mislabelled body may be
    // salvaged (its range's bytes kept, the rest dropped) or refused, either is fine.
    let short = |f: &Fault| {
        matches!(
            f,
            Fault::ShortBody(_)
                | Fault::CutAfter(_)
                | Fault::Status { .. }
                | Fault::StallBody(_)
                | Fault::StallHeaders
        )
    };
    let log = origin.requests();
    for bad in log.iter().filter(|r| r.fault.as_ref().is_some_and(short)) {
        // Asked for again from that block, or by a run of blocks reaching over it.
        let block = bad.start() / BLOCK;
        let again = log.iter().any(|r| {
            let reaches =
                r.range.and_then(|(_, last)| last).is_none_or(|last| last / BLOCK >= block);
            r.n > bad.n
                && r.fault.is_none()
                && !r.is_probe()
                && r.start() / BLOCK <= block
                && reaches
        });
        assert!(
            again,
            "block {} was never asked for again after request {}",
            bad.start() / BLOCK,
            bad.n
        );
    }
}

macro_rules! transient_faults {
    ($($name:ident: $fault:expr,)*) => {$(
        #[tokio::test(flavor = "multi_thread")]
        async fn $name() {
            retried_to_the_exact_file($fault).await;
        }
    )*};
}

// Each is a way a real server or middlebox answers a range request wrongly. Writing any of these
// bodies at the requested offset would corrupt the file silently.
transient_faults! {
    content_range_starting_at_the_wrong_byte: Fault::WrongStart,
    body_running_past_the_range: Fault::Overlong,
    short_body_then_a_clean_end: Fault::ShortBody(1000),
    partial_content_without_content_range: Fault::NoContentRange,
    whole_file_for_a_range: Fault::IgnoreRange,
    connection_cut_mid_body: Fault::CutAfter(5000),
    server_error_500: Fault::status(500),
    stall_after_the_headers: Fault::StallBody(0),
    stall_mid_body: Fault::StallBody(20_000),
    stall_before_the_headers: Fault::StallHeaders,
}

/// Every kind of wrong answer at once, over two networks and many blocks: still exact.
#[tokio::test(flavor = "multi_thread")]
async fn a_mix_of_wrong_answers_never_corrupts_the_file() {
    let origin = Origin::start().await;
    origin.add("a.bin", 2 * MIB as usize);
    let faults = [
        Fault::WrongStart,
        Fault::Overlong,
        Fault::ShortBody(777),
        Fault::NoContentRange,
        Fault::IgnoreRange,
        Fault::CutAfter(10_000),
        Fault::status(500),
        Fault::Etag("\"a.bin-v1\"".into()), // the same label, said differently: harmless
    ];
    let n = AtomicUsize::new(0);
    origin.set_rule(move |r| {
        let i = n.fetch_add(1, Ordering::SeqCst);
        (!r.is_probe() && i % 3 == 1 && i / 3 < faults.len()).then(|| faults[i / 3].clone())
    });
    let h = Harness::new(2);

    let id = h.start(&origin, "a.bin", &[]).await;
    h.wait_status(id, DownloadStatus::Completed).await;
}

/// The link dies mid-download (404, 410): retrying can't fix that, so it ends in an error with a
/// reason, rather than retrying forever or completing with a hole.
#[tokio::test(flavor = "multi_thread")]
async fn a_link_that_dies_ends_in_an_error() {
    for code in [404, 410] {
        let origin = Origin::start().await;
        origin.add("a.bin", 2 * MIB as usize);
        origin.limit_ip(net_ip(1), Some(MIB));
        let h = Harness::new(1);

        let id = h.start(&origin, "a.bin", &[]).await;
        h.wait_for(id, "progress", |s| s.bytes_downloaded > 0).await;
        origin.fault("a.bin", Fault::status(code));
        // A stream asks for its share of the file at once: what the server is sending now would
        // finish it. Those responses die too, so the file must be asked for again.
        origin.stall_connections();
        let failed = h.wait_status(id, DownloadStatus::Error).await;
        assert!(failed.error.as_deref().is_some_and(|e| !e.is_empty()), "says why ({code})");
        // Dismissed, it leaves nothing behind (checked when the harness drops).
        h.remove(id, false);
        h.eventually("removed", WAIT, || h.was_removed(id)).await;
    }
}

/// A busy server (503 + Retry-After) is waited out for as long as it asks — past the retries a
/// wrong answer gets — and nothing asks it again before the time it gave.
#[tokio::test(flavor = "multi_thread")]
async fn a_busy_server_is_waited_out() {
    let origin = Origin::start().await;
    origin.add("a.bin", SIZE as usize);
    let busy_until = Instant::now() + Duration::from_millis(2500);
    origin.set_rule(move |r| {
        (!r.is_probe() && Instant::now() < busy_until)
            .then_some(Fault::Status { code: 503, retry_after: Some(1) })
    });
    let h = Harness::with(1, |t| t.server_busy_for = Duration::from_secs(30));

    let id = h.start(&origin, "a.bin", &[]).await;
    h.wait_status(id, DownloadStatus::Completed).await;

    let log = origin.requests();
    let busy: Vec<_> = log.iter().filter(|r| r.status == 503).collect();
    assert!(busy.len() >= 2, "the server really was busy");
    for answer in &busy {
        let too_soon = log.iter().filter(|r| {
            r.peer == answer.peer
                && r.at > answer.at + Duration::from_millis(100)
                && r.at < answer.at + Duration::from_millis(800)
        });
        assert_eq!(
            too_soon.count(),
            0,
            "asked again within the second request {} asked for",
            answer.n
        );
    }
}

/// A network waiting out a server's Retry-After is quiet because it was asked to be, not because
/// it can't get through: a connection that fails as the wait ends is retried, and the network
/// isn't taken for unreachable (cut to one stream) for the silence it was told to keep.
#[tokio::test(flavor = "multi_thread")]
async fn the_quiet_of_a_retry_after_is_not_taken_for_an_unreachable_network() {
    let origin = Origin::start().await;
    origin.add("a.bin", SIZE as usize);
    let busy_until = Instant::now() + Duration::from_millis(1500);
    let cut = AtomicUsize::new(0);
    origin.set_rule(move |r| {
        if r.is_probe() {
            None
        } else if Instant::now() < busy_until {
            Some(Fault::Status { code: 503, retry_after: Some(1) })
        } else {
            (cut.fetch_add(1, Ordering::SeqCst) == 0).then_some(Fault::CutAfter(0))
        }
    });
    let mut h = Harness::with(1, |t| t.server_busy_for = Duration::from_secs(30));
    // One stream, so no other's bytes show the network alive meanwhile.
    h.streams = Some(1);

    let id = h.start(&origin, "a.bin", &[]).await;
    h.wait_status(id, DownloadStatus::Completed).await;

    assert!(origin.requests().iter().any(|r| r.fault == Some(Fault::CutAfter(0))));
    let unreachable = h
        .states(id)
        .into_iter()
        .any(|s| s.networks.iter().any(|n| n.status == sonic_core::NetworkStatus::Unreachable));
    assert!(!unreachable, "taken for unreachable after waiting as asked");
}

/// A file republished on the server mid-download (a different size proves it) must never be
/// stitched from two versions: the download stops with an error, and its progress is thrown
/// away (not resumable) because it belongs to the old file.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_that_changes_size_on_the_server_is_an_error() {
    changed_on_the_server(Some((SIZE + BLOCK) as usize)).await;
}

/// The same with the same size but new bytes and a new ETag: only comparing bytes can tell, and
/// it must.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_with_new_content_on_the_server_is_an_error() {
    changed_on_the_server(None).await;
}

async fn changed_on_the_server(new_size: Option<usize>) {
    let origin = Origin::start().await;
    origin.add("a.bin", SIZE as usize);
    let h = Harness::new(1);

    let reached = origin.hold(3 * BLOCK + 100);
    let id = h.start(&origin, "a.bin", &[]).await;
    tokio::time::timeout(WAIT, reached).await.expect("reached the held byte");
    origin.change_version("a.bin", new_size);
    // A stream asks for its share of the file at once: what the server is sending now, of the
    // old version, would finish it. Those responses die, so the file must be asked for again.
    origin.stall_connections();
    origin.release();

    let ended = h
        .wait_for(id, "an end", |s| {
            matches!(s.status, DownloadStatus::Error | DownloadStatus::Completed)
        })
        .await;
    assert_eq!(
        ended.status,
        DownloadStatus::Error,
        "never a mix of versions: {}",
        describe(&ended)
    );
    assert!(!ended.resumable, "its progress belongs to the old file");
    assert!(ended.error.is_some());
    h.remove(id, false);
    h.eventually("removed", WAIT, || h.was_removed(id)).await;
}

/// The same bytes under a new ETag (a CDN migration) is not a new file: comparing bytes settles
/// it, and the download carries on.
#[tokio::test(flavor = "multi_thread")]
async fn a_relabelled_file_with_the_same_bytes_carries_on() {
    let origin = Origin::start().await;
    origin.add("a.bin", SIZE as usize);
    let h = Harness::new(1);

    let reached = origin.hold(3 * BLOCK + 100);
    let id = h.start(&origin, "a.bin", &[]).await;
    tokio::time::timeout(WAIT, reached).await.expect("reached the held byte");
    origin.relabel("a.bin", "\"moved-to-new-cdn\"");
    origin.release();
    h.wait_status(id, DownloadStatus::Completed).await;
}

/// Load-balanced servers labelling identical bytes differently, request by request: none of it
/// is a changed file, so none of it may fail the download.
#[tokio::test(flavor = "multi_thread")]
async fn servers_labelling_the_same_bytes_differently_are_one_file() {
    let origin = Origin::start().await;
    origin.add("a.bin", SIZE as usize);
    let labels = ["\"server-b\"", "W/\"a.bin-v1\"", "\"a.bin-v1-gzip\""];
    origin.set_rule(move |r| (r.n % 2 == 0).then(|| Fault::Etag(labels[r.n % 3].into())));
    let h = Harness::new(1);

    let id = h.start(&origin, "a.bin", &[]).await;
    h.wait_status(id, DownloadStatus::Completed).await;
}

/// A link that stopped working (expired signature) is given a fresh one: the download resumes
/// from where it was, over the new link only, without fetching again what it has.
#[tokio::test(flavor = "multi_thread")]
async fn relinking_resumes_from_where_it_was() {
    let origin = Origin::start().await;
    let size = 3 * MIB;
    origin.add("old.bin", size as usize);
    origin.limit_ip(net_ip(1), Some(MIB));
    let h = Harness::new(1);

    let id = h.start(&origin, "old.bin", &[]).await;
    h.wait_for(id, "half a MiB", |s| s.bytes_downloaded >= MIB / 2).await;
    origin.fault("old.bin", Fault::status(410));
    // What the server is sending now would finish it (see a_link_that_dies_ends_in_an_error).
    origin.stall_connections();
    let failed = h.wait_status(id, DownloadStatus::Error).await;
    let kept = failed.bytes_downloaded;

    origin.copy("old.bin", "new.bin");
    let mark = origin.requests().len();
    h.relink(id, &origin.url("new.bin")).await.expect("relinked");
    // The reply may come before the update that says so: wait for it to leave the error first.
    h.wait_for(id, "the relinked download to run", |s| s.status != DownloadStatus::Error).await;
    let done = h.wait_status(id, DownloadStatus::Completed).await;

    assert_eq!(done.url, origin.url("new.bin"));
    let since = &origin.requests()[mark..];
    assert!(since.iter().all(|r| r.name == "new.bin"), "the dead link isn't asked again");
    let refetched: u64 = since.iter().filter(|r| !r.is_probe()).map(|r| r.bytes_sent).sum();
    assert!(refetched <= size - kept + 8 * BLOCK, "resumed, not restarted: {refetched} sent");
}

/// Racing a crawling block: near the end, an idle stream fetches the block a crawling
/// connection holds, and whichever finishes first wins. Without it, one bad connection holds
/// the whole download hostage (2 KB/s here: half a minute for one block).
#[tokio::test(flavor = "multi_thread")]
async fn hedging_rescues_a_crawling_tail() {
    const SLOW: u64 = 3;
    let origin = Origin::start().await;
    origin.add("a.bin", 12 * BLOCK as usize);
    let in_slow_block =
        |r: &Request| !r.is_probe() && r.start() / BLOCK == SLOW && r.range.is_some();
    let crawled = AtomicUsize::new(0);
    origin.set_rule(move |r| {
        (in_slow_block(r) && crawled.fetch_add(1, Ordering::SeqCst) == 0)
            .then_some(Fault::Crawl(2000))
    });
    // Crawl detection off, so only hedging can save it.
    let h = Harness::with(1, |t| {
        t.hedge_after = Duration::from_millis(300);
        t.slow_warmup = Duration::from_secs(60);
        t.slow_for = Duration::from_secs(60);
    });

    let started = Instant::now();
    let id = h.start(&origin, "a.bin", &[]).await;
    h.wait_status_within(id, DownloadStatus::Completed, Duration::from_secs(12)).await;
    assert!(started.elapsed() < Duration::from_secs(12));
    let slow: Vec<_> =
        origin.chunk_requests("a.bin").into_iter().filter(|r| in_slow_block(r)).collect();
    assert!((2..=3).contains(&slow.len()), "the holder and a hedge (at most two): {slow:?}");
}

/// The same block raced over a second network: the faster network rescues the slow one's
/// block, and the block's bytes are credited to whoever delivered them.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_network_rescues_a_crawling_block() {
    let origin = Origin::start().await;
    origin.add("a.bin", 12 * BLOCK as usize);
    let crawled = AtomicUsize::new(0);
    let slow = net_ip(1);
    origin.set_rule(move |r| {
        (!r.is_probe()
            && r.peer == slow
            && r.start() >= 2 * BLOCK
            && crawled.fetch_add(1, Ordering::SeqCst) == 0)
            .then_some(Fault::Crawl(2000))
    });
    let mut h = Harness::with(2, |t| {
        t.hedge_after = Duration::from_millis(300);
        t.slow_warmup = Duration::from_secs(60);
        t.slow_for = Duration::from_secs(60);
    });
    h.streams = Some(4);

    let id = h.start(&origin, "a.bin", &[]).await;
    let done = h.wait_status_within(id, DownloadStatus::Completed, Duration::from_secs(12)).await;
    assert!(carried(&done, &net(2)) > 0);
}

/// Every connection goes quiet at once mid-download, as when the route or CDN node they all
/// share dies, while a fresh connection would be served at once. The download reconnects as
/// soon as its networks have been silent for `silent_after`, rather than sitting at 0 B/s until
/// the stall timeout — a refresh, not a failure, and no network is taken for unreachable.
#[tokio::test(flavor = "multi_thread")]
async fn connections_gone_quiet_all_at_once_reconnect_well_before_the_stall_timeout() {
    let origin = Origin::start().await;
    origin.add("a.bin", 32 * BLOCK as usize);
    // Slow enough that every stream is in the middle of a block when they go quiet.
    origin.limit_ip(net_ip(1), Some(256 * KIB));
    origin.limit_ip(net_ip(2), Some(256 * KIB));
    let stall_timeout = Duration::from_secs(15);
    let h = Harness::with(2, |t| {
        t.stall_timeout = stall_timeout;
        // Crawl detection off: only the silence rule can notice.
        t.slow_warmup = Duration::from_secs(60);
        t.slow_for = Duration::from_secs(60);
    });

    let id = h.start(&origin, "a.bin", &[]).await;
    h.wait_for(id, "part of the file on both networks", |s| {
        carried(s, &net(1)) >= 2 * BLOCK && carried(s, &net(2)) >= 2 * BLOCK
    })
    .await;
    origin.stall_connections();
    let stalled_at = Instant::now();
    // What was already on its way lands; then nothing more comes over those connections.
    settle(300).await;
    let before = h.latest(id).map_or(0, |s| s.bytes_downloaded);
    h.wait_for_within(id, Duration::from_secs(6), "the download to move again", |s| {
        s.bytes_downloaded >= before + 2 * BLOCK
    })
    .await;
    let resumed = stalled_at.elapsed();
    assert!(resumed < stall_timeout / 3, "resumed after {resumed:?}");

    let done = h.wait_status(id, DownloadStatus::Completed).await;
    for network in &done.networks {
        assert_eq!(network.retries, 0, "a refresh isn't a failed request: {}", describe(&done));
    }
    let unreachable = h
        .states(id)
        .into_iter()
        .any(|s| s.networks.iter().any(|n| n.status == sonic_core::NetworkStatus::Unreachable));
    assert!(!unreachable, "a network that had been receiving was refreshed, not given up on");
}

/// A server that serves `limit` requests from one address at once and answers more with 429
/// (speed.hetzner.de allows 2): Auto starts more streams, is turned away, and settles at
/// exactly the number the server serves at once — not fewer.
async fn settles_at_a_servers_connection_limit(limit: usize) {
    let origin = Origin::start().await;
    origin.add("a.bin", 48 * BLOCK as usize);
    origin.limit_connections(net_ip(1), Some(limit));
    origin.limit_ip(net_ip(1), Some(768 * KIB));
    let mut h = Harness::new(1);
    h.streams = None;

    let id = h.start(&origin, "a.bin", &[]).await;
    // The first look that sees the refusals cuts the count; well settled after a second.
    settle(1000).await;
    let mut counts = Vec::new();
    loop {
        let state = h.latest(id).expect("a state");
        if state.status != DownloadStatus::Downloading {
            break;
        }
        counts.push(streams_on(&state, &net(1)));
        settle(20).await;
    }
    let done = h.wait_status(id, DownloadStatus::Completed).await;
    assert!(done.peak_streams as usize > limit, "Auto tried more: {}", done.peak_streams);
    assert!(origin.requests().iter().any(|r| r.status == 429), "the server turned some away");
    assert!(counts.len() > 10, "watched it run for a while: {counts:?}");
    assert!(counts.iter().all(|c| *c == limit), "settled at {limit}: {counts:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_server_allowing_two_connections_gets_two_streams() {
    settles_at_a_servers_connection_limit(2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_server_allowing_three_connections_gets_three_streams() {
    settles_at_a_servers_connection_limit(3).await;
}

/// thinkbroadband.com: a burst of new connections is partly turned away with 429 and
/// `Retry-After: 30`, while the ones let in go on being served. Taking the Retry-After as a hold
/// on the whole network kept the served streams from asking for more as they finished what they
/// had, so the network drained to nothing for the rest of the 30 s — and, quiet that long, was
/// then taken for unreachable. The streams being served go on asking.
#[tokio::test(flavor = "multi_thread")]
async fn a_retry_after_for_some_streams_does_not_hold_the_ones_being_served() {
    const HOLD: u64 = 8;
    let origin = Origin::start().await;
    origin.add("a.bin", 32 * BLOCK as usize);
    origin.limit_ip(net_ip(1), Some(512 * KIB));
    origin.fault_next("a.bin", [Fault::Status { code: 429, retry_after: Some(HOLD) }]);
    let h = Harness::new(1);

    let id = h.start(&origin, "a.bin", &[]).await;
    h.wait_status(id, DownloadStatus::Completed).await;

    let log = origin.requests();
    let refused = log.iter().find(|r| r.status == 429).expect("one was turned away");
    // Not the requests sent beside the refused one, as the streams started.
    let held = refused.at + Duration::from_millis(500)..refused.at + Duration::from_secs(HOLD);
    assert!(
        log.iter().any(|r| r.status == 206 && !r.is_probe() && held.contains(&r.at)),
        "the streams being served asked again within the {HOLD} s the server asked of one"
    );
    let unreachable = h
        .states(id)
        .into_iter()
        .any(|s| s.networks.iter().any(|n| n.status == sonic_core::NetworkStatus::Unreachable));
    assert!(!unreachable, "a network being served isn't unreachable");
}

/// speed.hetzner.com: two connections from one address — and the networks share one, behind the
/// router — and about 15 requests a minute. Asking for one block per request, the stream it
/// allowed on each network spent the download asking, the two taking turns at a fraction of what
/// one connection does; asking for a run of blocks at once, they get twice what one does.
#[tokio::test(flavor = "multi_thread")]
async fn two_connections_and_few_requests_allowed_get_twice_one_connections_speed() {
    let origin = Origin::start().await;
    let size = 64 * BLOCK;
    origin.add("a.bin", size as usize);
    origin.limit_all_connections(Some(2));
    origin.limit_requests(Some((2.0, 4.0)));
    for i in 1..=2 {
        origin.limit_ip(net_ip(i), Some(512 * KIB));
    }
    let mut h = Harness::new(2);
    h.streams = Some(1);

    let started = Instant::now();
    let id = h.start(&origin, "a.bin", &[]).await;
    let done = h.wait_status(id, DownloadStatus::Completed).await;
    let took = started.elapsed();

    for i in 1..=2 {
        assert!(carried(&done, &net(i)) > size / 4, "both carried their part: {}", describe(&done));
    }
    // One connection takes 11 s here (the origin paces a little below its nominal rate), and two
    // 5.5 s.
    assert!(took < Duration::from_secs(7), "took {took:?}: {}", describe(&done));
}

/// Streams keep their connections: a run cut short by a split closes its connection, so the
/// streams of a slow network whose runs are handed on reconnect about once each — and no more:
/// splits late in a download, run after run, closed connections by the dozen, and a server that
/// answers a burst of new connections with 429 and Retry-After: 30 (thinkbroadband) then held
/// those streams for half a minute.
#[tokio::test(flavor = "multi_thread")]
async fn streams_keep_their_connections() {
    let origin = Origin::start().await;
    origin.add("a.bin", 128 * BLOCK as usize);
    origin.limit_ip(net_ip(1), Some(4 * MIB));
    origin.limit_ip(net_ip(2), Some(MIB / 2));
    let mut h = Harness::new(2);
    h.streams = Some(16);

    let id = h.start(&origin, "a.bin", &[]).await;
    h.wait_status(id, DownloadStatus::Completed).await;

    let log = origin.requests();
    for i in 1..=2 {
        let opened: HashSet<usize> =
            log.iter().filter(|r| r.peer == net_ip(i)).map(|r| r.connection).collect();
        assert!(opened.len() <= 2 * 16, "net{i} opened {} connections", opened.len());
    }
}

/// A race near the end that the server turns away (429 with Retry-After) is waited out like any
/// request. A stream whose hedge was refused asked again at once, to race the next block, and
/// again: dozens of refused requests a second at thinkbroadband.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_hedge_waits_out_the_retry_after() {
    let origin = Origin::start().await;
    origin.add("a.bin", 16 * BLOCK as usize);
    // All but the first blocks crawl (a second a block); anyone racing one is turned away.
    let crawling = std::sync::Mutex::new(HashSet::new());
    origin.set_rule(move |r| {
        let block = r.start() / BLOCK;
        if r.is_probe() || block < 4 {
            None
        } else if crawling.lock().unwrap().insert(block) {
            Some(Fault::Crawl(BLOCK))
        } else {
            Some(Fault::Status { code: 429, retry_after: Some(1) })
        }
    });
    let mut h = Harness::with(1, |t| t.hedge_after = Duration::from_millis(200));
    h.streams = Some(16);

    let id = h.start(&origin, "a.bin", &[]).await;
    h.wait_status(id, DownloadStatus::Completed).await;

    let log = origin.requests();
    assert!(log.iter().any(|r| r.status == 429), "some race was turned away");
    for refused in log.iter().filter(|r| r.status == 429) {
        let next = log.iter().find(|r| r.connection == refused.connection && r.n > refused.n);
        if let Some(next) = next {
            let after = next.at - refused.at;
            assert!(after >= Duration::from_millis(900), "asked again {after:?} after a 429");
        }
    }
}
