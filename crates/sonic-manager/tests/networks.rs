//! Many networks at once — the reason SonicNet exists. Each test network is its own `127.0.0.x`
//! source address, and the origin limits each address's speed on its own, so the server sees
//! (and tests assert) exactly which network carried what.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use sonic_core::{DownloadStatus, NetworkPreference, NetworkStatus};
use sonic_testkit::*;

/// Downloads `size` bytes over networks `1..=rates.len()`, network `i` limited to `rates[i-1]`,
/// and checks every network carried part of it and the fastest carried the most. The point of
/// the product: a fast network takes more blocks, a slow one still helps, none is left idle.
async fn spread_over(rates: &[u64], size: u64) {
    let origin = Origin::start().await;
    origin.add("a.bin", size as usize);
    for (i, rate) in rates.iter().enumerate() {
        origin.limit_ip(net_ip(i + 1), Some(*rate));
    }
    let h = Harness::new(rates.len());

    let id = h.start(&origin, "a.bin", &[]).await;
    let done = h.wait_status(id, DownloadStatus::Completed).await;

    let shares: Vec<u64> = (1..=rates.len()).map(|i| carried(&done, &net(i))).collect();
    for (i, share) in shares.iter().enumerate() {
        let i = i + 1;
        assert!(*share > 0, "net{i} carried nothing: {shares:?}");
        // Nothing is credited to a network that the server didn't send over it.
        assert!(*share <= origin.bytes_to(net_ip(i)), "net{i} credited more than it was sent");
    }
    let fastest = rates.iter().enumerate().max_by_key(|(_, r)| **r).map(|(i, _)| i).unwrap();
    assert_eq!(
        shares.iter().max(),
        Some(&shares[fastest]),
        "the fastest network (net{}) carried the most: {shares:?}",
        fastest + 1
    );
    let slowest = rates.iter().enumerate().min_by_key(|(_, r)| **r).map(|(i, _)| i).unwrap();
    assert!(shares[fastest] > shares[slowest] * 2, "speed decides the shares: {shares:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn three_networks_of_different_speeds_all_carry_it() {
    spread_over(&[256 * KIB, 512 * KIB, 1536 * KIB], 4 * MIB).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn five_networks_of_different_speeds_all_carry_it() {
    spread_over(&[256 * KIB, 384 * KIB, 512 * KIB, 768 * KIB, 2 * MIB], 6 * MIB).await;
}

/// A network unplugged mid-download (the phone walks away): its streams stop, its in-flight
/// blocks go back to the queue, and the others finish the file. It stays the user's pick,
/// shown offline, rather than failing the download or being forgotten.
#[tokio::test(flavor = "multi_thread")]
async fn a_network_unplugged_mid_download_leaves_the_rest_to_finish() {
    let origin = Origin::start().await;
    origin.add("a.bin", 4 * MIB as usize);
    for i in 1..=3 {
        origin.limit_ip(net_ip(i), Some(512 * KIB));
    }
    let h = Harness::new(3);

    let id = h.start(&origin, "a.bin", &[]).await;
    h.wait_for(id, "net2 to carry something", |s| carried(s, &net(2)) > 0).await;
    h.networks.remove(&net(2));
    let gone = h
        .wait_for(id, "net2 offline with no streams", |s| {
            network_of(s, &net(2)).is_some_and(|n| n.status == NetworkStatus::Offline)
                && streams_on(s, &net(2)) == 0
        })
        .await;
    assert!(network_of(&gone, &net(2)).unwrap().enabled, "still the user's pick");
    assert_eq!(gone.status, DownloadStatus::Downloading);
    let mark = origin.requests().len();

    h.wait_status(id, DownloadStatus::Completed).await;
    assert!(
        origin.requests()[mark..].iter().all(|r| r.peer != net_ip(2)),
        "nothing went over the unplugged network"
    );
}

/// The only network drops for a while and comes back: the download waits (no error, nothing it
/// had is lost) and picks up where it was.
#[tokio::test(flavor = "multi_thread")]
async fn the_only_network_drops_and_comes_back() {
    let origin = Origin::start().await;
    origin.add("a.bin", 3 * MIB as usize);
    origin.limit_ip(net_ip(1), Some(MIB));
    let h = Harness::new(1);

    let id = h.start(&origin, "a.bin", &[]).await;
    h.wait_for(id, "progress", |s| s.bytes_downloaded > 0).await;
    h.networks.remove(&net(1));
    let gone = h
        .wait_for(id, "net1 offline", |s| {
            network_of(s, &net(1)).is_some_and(|n| n.status == NetworkStatus::Offline)
        })
        .await;
    // Far longer than every retry together (20 ms doubling to 250 ms): it must wait, not fail.
    settle(1500).await;
    let waited = h.latest(id).unwrap();
    assert_ne!(waited.status, DownloadStatus::Error, "{}", describe(&waited));
    assert!(waited.bytes_downloaded >= gone.bytes_downloaded, "nothing it had was lost");

    h.networks.add(network(1));
    h.wait_status(id, DownloadStatus::Completed).await;
}

/// A network that appears mid-download is listed, switched off: SonicNet never starts using a
/// connection (a metered phone, say) the user didn't pick. Switched on, it joins in.
#[tokio::test(flavor = "multi_thread")]
async fn a_network_that_turns_up_mid_download_starts_off_and_can_be_switched_on() {
    let origin = Origin::start().await;
    origin.add("a.bin", 4 * MIB as usize);
    origin.limit_ip(net_ip(1), Some(512 * KIB));
    let h = Harness::new(1);

    let id = h.start(&origin, "a.bin", &[]).await;
    h.wait_for(id, "progress", |s| s.bytes_downloaded > 0).await;
    h.networks.add(network(2));
    let listed = h.wait_for(id, "net2 listed", |s| network_of(s, &net(2)).is_some()).await;
    let net2 = network_of(&listed, &net(2)).unwrap();
    assert!(!net2.enabled && net2.status == NetworkStatus::Off, "listed off: {net2:?}");
    settle(400).await;
    assert!(origin.requests().iter().all(|r| r.peer != net_ip(2)), "not used while off");

    h.set_network(id, &net(2), true);
    h.wait_for(id, "net2 to carry something", |s| carried(s, &net(2)) > 0).await;
    h.wait_status(id, DownloadStatus::Completed).await;
}

/// SetNetwork off stops a network's streams — and nothing more goes over it — while what it
/// delivered stays credited to it; on again, it carries traffic again.
#[tokio::test(flavor = "multi_thread")]
async fn switching_a_network_off_and_on() {
    let origin = Origin::start().await;
    origin.add("a.bin", 6 * MIB as usize);
    origin.limit_ip(net_ip(1), Some(512 * KIB));
    origin.limit_ip(net_ip(2), Some(512 * KIB));
    let h = Harness::new(2);

    let id = h.start(&origin, "a.bin", &[]).await;
    h.wait_for(id, "net2 to carry something", |s| carried(s, &net(2)) > 0).await;
    h.set_network(id, &net(2), false);
    let off = h
        .wait_for(id, "net2 off with no streams", |s| {
            network_of(s, &net(2)).is_some_and(|n| n.status == NetworkStatus::Off && !n.enabled)
                && streams_on(s, &net(2)) == 0
        })
        .await;
    assert_eq!(off.status, DownloadStatus::Downloading, "net1 carries on");
    let credited = carried(&off, &net(2));
    assert!(credited > 0, "what it delivered stays its own");
    let mark = origin.requests().len();
    settle(400).await;
    assert!(origin.requests()[mark..].iter().all(|r| r.peer != net_ip(2)), "nothing over net2");

    h.set_network(id, &net(2), true);
    h.wait_for(id, "net2 to carry more", |s| carried(s, &net(2)) > credited).await;
    h.wait_status(id, DownloadStatus::Completed).await;
}

/// Switching off the last network in use pauses the download (nothing can carry it); switching
/// it back on resumes it — and while paused, not one request goes out.
#[tokio::test(flavor = "multi_thread")]
async fn switching_off_the_last_network_pauses_and_on_resumes() {
    let origin = Origin::start().await;
    origin.add("a.bin", 3 * MIB as usize);
    origin.limit_ip(net_ip(1), Some(MIB));
    let h = Harness::new(1);

    let id = h.start(&origin, "a.bin", &[]).await;
    h.wait_for(id, "progress", |s| s.bytes_downloaded > 0).await;
    h.set_network(id, &net(1), false);
    let paused = h.wait_status(id, DownloadStatus::Paused).await;
    assert!(!network_of(&paused, &net(1)).unwrap().enabled);
    settle(200).await;
    let mark = origin.requests().len();
    settle(400).await;
    assert_eq!(origin.requests().len(), mark, "nothing requested while every network is off");

    h.set_network(id, &net(1), true);
    h.wait_for(id, "downloading again", |s| s.status == DownloadStatus::Downloading).await;
    h.wait_status(id, DownloadStatus::Completed).await;
}

/// A network with a data allowance stops at it (status Limit) and the others finish the file.
/// Going past an allowance costs the user real money on a metered plan.
#[tokio::test(flavor = "multi_thread")]
async fn a_network_stops_at_its_data_allowance_and_the_others_finish() {
    let origin = Origin::start().await;
    origin.add("a.bin", 2 * MIB as usize);
    origin.limit_ip(net_ip(1), Some(512 * KIB));
    origin.limit_ip(net_ip(2), Some(512 * KIB));
    let h = Harness::new(2);
    let allowance = 4 * BLOCK;
    h.update_settings(|s| {
        s.network_preferences.insert(
            net(1),
            NetworkPreference { data_limit: Some(allowance), ..Default::default() },
        );
    })
    .await;

    let id = h.start(&origin, "a.bin", &[]).await;
    h.wait_for(id, "net1 at its limit", |s| {
        network_of(s, &net(1)).is_some_and(|n| n.status == NetworkStatus::Limit)
    })
    .await;
    let done = h.wait_status(id, DownloadStatus::Completed).await;
    // One read's worth of slack: the allowance may be checked between reads.
    assert!(carried(&done, &net(1)) <= allowance + BLOCK, "{}", describe(&done));
    if let Some(used) = h.usage().get(&net(1)) {
        assert!(*used <= allowance + BLOCK, "usage {used} past the allowance");
    }
}

/// The only network hits its allowance: the download waits (doesn't fail, doesn't overrun), and
/// raising the allowance lets it finish.
#[tokio::test(flavor = "multi_thread")]
async fn the_only_network_at_its_allowance_waits_until_it_is_raised() {
    let origin = Origin::start().await;
    origin.add("a.bin", MIB as usize);
    let h = Harness::new(1);
    let allowance = 4 * BLOCK;
    h.update_settings(|s| {
        s.network_preferences.insert(
            net(1),
            NetworkPreference { data_limit: Some(allowance), ..Default::default() },
        );
    })
    .await;

    let id = h.start(&origin, "a.bin", &[]).await;
    let stopped = h
        .wait_for(id, "net1 at its limit", |s| {
            network_of(s, &net(1)).is_some_and(|n| n.status == NetworkStatus::Limit)
        })
        .await;
    assert!(stopped.bytes_downloaded < MIB);
    settle(500).await;
    let waiting = h.latest(id).unwrap();
    assert!(waiting.status.is_unfinished() && waiting.status != DownloadStatus::Error);
    assert!(waiting.bytes_downloaded <= allowance + BLOCK, "{}", describe(&waiting));

    h.update_settings(|s| {
        s.network_preferences.insert(
            net(1),
            NetworkPreference { data_limit: Some(1024 * MIB), ..Default::default() },
        );
    })
    .await;
    h.wait_status_within(id, DownloadStatus::Completed, Duration::from_secs(20)).await;
}

/// Two networks, the fast one with connections that crawl among its fast ones (a congestion
/// window that collapsed among dozens of others recovers by a packet a round trip) and a server
/// allowing it so many requests a window (thinkbroadband: about 120 a minute, then 429 with
/// Retry-After for the rest of it). The last tenth of the file must come at about the fast
/// network's speed: what the crawling connections hold is handed on cheaply, rather than raced
/// request by request until the server stops answering the fast network.
#[tokio::test(flavor = "multi_thread")]
async fn the_tail_comes_at_the_fast_networks_speed() {
    let origin = Origin::start().await;
    let size = 64 * BLOCK;
    origin.add("a.bin", size as usize);
    origin.limit_ip(net_ip(1), Some(4 * MIB));
    origin.limit_ip(net_ip(2), Some(256 * KIB));
    // It needs about 33 when the last tenth is handed on well.
    origin.limit_requests_from(net_ip(1), Some((40, Duration::from_secs(5))));
    // Two of the fast network's first requests crawl to the end.
    let requests = AtomicUsize::new(0);
    origin.set_rule(move |r| {
        let n =
            (!r.is_probe() && r.peer == net_ip(1)).then(|| requests.fetch_add(1, Ordering::SeqCst));
        matches!(n, Some(1 | 5)).then_some(Fault::Crawl(8 * KIB))
    });
    let mut h = Harness::with(2, |t| t.hedge_after = Duration::from_millis(300));
    h.streams = Some(8);

    let started = Instant::now();
    let id = h.start(&origin, "a.bin", &[]).await;
    let mut ninety = None;
    while ninety.is_none() {
        if h.latest(id).is_some_and(|s| s.bytes_downloaded * 10 >= size * 9) {
            ninety = Some(started.elapsed());
        }
        settle(10).await;
    }
    let done = h.wait_status(id, DownloadStatus::Completed).await;
    let tail = started.elapsed() - ninety.unwrap_or_default();
    // A tenth of the file over the fast network alone: 0.1 s.
    assert!(tail < Duration::from_millis(500), "the last tenth took {tail:?}: {}", describe(&done));
}

/// Auto against a server capping each connection's speed: doubling makes the network faster, so
/// it grows.
#[tokio::test(flavor = "multi_thread")]
async fn auto_grows_against_a_server_capping_each_connection() {
    let origin = Origin::start().await;
    origin.add("a.bin", 8 * MIB as usize);
    origin.set_rule(|r| (!r.is_probe()).then_some(Fault::Crawl(64 * KIB)));
    let mut h = Harness::new(1);
    h.streams = None;

    let id = h.start(&origin, "a.bin", &[]).await;
    let done = h.wait_status_within(id, DownloadStatus::Completed, Duration::from_secs(30)).await;
    // Its eight are judged 7 s in; the doubling's own judgement would come after the end.
    assert!(done.peak_streams >= 16, "grew: {}", describe(&done));
}

/// Auto on a network whose own link is full at its first count: doubling gains nothing, so it
/// goes back to that count and stays — more connections would only cost requests, and leave
/// some crawling among the rest at the end.
#[tokio::test(flavor = "multi_thread")]
async fn auto_stops_growing_where_the_link_is_full() {
    let origin = Origin::start().await;
    let size = 18 * MIB;
    origin.add("a.bin", size as usize);
    origin.limit_ip(net_ip(1), Some(MIB));
    let mut h = Harness::new(1);
    h.streams = None;

    let id = h.start(&origin, "a.bin", &[]).await;
    let done = h.wait_status_within(id, DownloadStatus::Completed, Duration::from_secs(30)).await;
    assert!(done.peak_streams <= 16, "tried one doubling at most: {}", describe(&done));
    // By then (about 16 s in) it has tried, at 7 s, and gone back at 14 s.
    let late: Vec<usize> = h
        .states(id)
        .iter()
        .filter(|s| (size * 85 / 100..size * 95 / 100).contains(&s.bytes_downloaded))
        .map(|s| streams_on(s, &net(1)))
        .collect();
    assert!(!late.is_empty() && late.iter().all(|n| *n <= 8), "{late:?}");
}
