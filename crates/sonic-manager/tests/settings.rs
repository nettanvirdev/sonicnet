//! The user's settings as the manager applies them: speed limits, the queue, the schedule, and
//! the validation of whatever the UI sends.

use std::time::{Duration, Instant};

use chrono::Timelike;
use sonic_core::{
    DOWNLOADS_AT_ONCE_MAX, DOWNLOADS_AT_ONCE_MIN, DownloadId, DownloadSchedule, DownloadStatus,
    Event, NetworkPreference,
};
use sonic_testkit::*;

/// A total speed limit holds the download to it: 2 MiB at 512 KiB/s is four seconds, less at
/// most a second's worth a token bucket may let through at once.
#[tokio::test(flavor = "multi_thread")]
async fn a_total_speed_limit_is_respected() {
    let origin = Origin::start().await;
    origin.add("a.bin", 2 * MIB as usize);
    let h = Harness::new(2);
    h.update_settings(|s| s.speed_limit = Some(512 * KIB)).await;

    let started = Instant::now();
    let id = h.start(&origin, "a.bin", &[]).await;
    h.wait_status(id, DownloadStatus::Completed).await;
    let took = started.elapsed();
    assert!(took >= Duration::from_millis(2800), "too fast for the limit: {took:?}");
    assert!(took <= Duration::from_secs(10), "far slower than the limit: {took:?}");
}

/// A per-network speed limit holds that network (and only that one) to it.
#[tokio::test(flavor = "multi_thread")]
async fn a_network_speed_limit_holds_that_network() {
    let origin = Origin::start().await;
    origin.add("a.bin", 3 * MIB as usize);
    origin.limit_ip(net_ip(2), Some(MIB));
    let h = Harness::new(2);
    let limit = 128 * KIB;
    h.update_settings(|s| {
        s.network_preferences
            .insert(net(1), NetworkPreference { speed_limit: Some(limit), ..Default::default() });
    })
    .await;

    let started = Instant::now();
    let id = h.start(&origin, "a.bin", &[]).await;
    let done = h.wait_status(id, DownloadStatus::Completed).await;
    let secs = started.elapsed().as_secs_f64();
    let allowed = (limit as f64 * (secs + 1.0)) as u64 + BLOCK;
    assert!(carried(&done, &net(1)) <= allowed, "net1 past its limit: {}", describe(&done));
    assert!(carried(&done, &net(2)) > carried(&done, &net(1)), "net2 isn't held back");
}

/// Waits until `id` has completed, and says when (its `completed_at`).
async fn completed_at(h: &Harness, id: DownloadId) -> i64 {
    let done = h.wait_status(id, DownloadStatus::Completed).await;
    done.completed_at.expect("a completed download says when")
}

/// With one download at a time, the others wait in the queue and run in the order they were
/// started; never two at once.
#[tokio::test(flavor = "multi_thread")]
async fn downloads_at_once_queues_the_rest_in_order() {
    let origin = Origin::start().await;
    for name in ["a.bin", "b.bin", "c.bin"] {
        origin.add(name, MIB as usize);
    }
    origin.limit_ip(net_ip(1), Some(2 * MIB));
    let h = Harness::new(1);
    h.update_settings(|s| s.downloads_at_once = 1).await;

    let a = h.start(&origin, "a.bin", &[]).await;
    let b = h.start(&origin, "b.bin", &[]).await;
    let c = h.start(&origin, "c.bin", &[]).await;
    for id in [b, c] {
        let queued = h.wait_for(id, "queued", |s| s.status == DownloadStatus::Queued).await;
        assert_eq!(queued.bytes_downloaded, 0);
    }
    let times = [completed_at(&h, a).await, completed_at(&h, b).await, completed_at(&h, c).await];
    assert!(times[0] <= times[1] && times[1] <= times[2], "finished out of order: {times:?}");

    // On the wire: each started only after the one before had made its last request.
    let first_and_last = |name: &str| {
        let requests = origin.chunk_requests(name);
        (requests.first().unwrap().at, requests.last().unwrap().at)
    };
    let (ab, bb, cb) = (first_and_last("a.bin"), first_and_last("b.bin"), first_and_last("c.bin"));
    assert!(bb.0 >= ab.1 && cb.0 >= bb.1, "two downloads ran at once");

    // And as the UI was told: never two downloading at once.
    let mut status = std::collections::HashMap::new();
    for event in h.events() {
        if let Event::DownloadUpdated(state) = event {
            status.insert(state.id, state.status);
            let running = status.values().filter(|s| **s == DownloadStatus::Downloading).count();
            assert!(running <= 1, "{running} downloading at once with downloads_at_once = 1");
        }
    }
}

/// Raising downloads_at_once starts queued downloads at once, without waiting for a slot.
#[tokio::test(flavor = "multi_thread")]
async fn raising_downloads_at_once_starts_the_queue() {
    let origin = Origin::start().await;
    for name in ["a.bin", "b.bin", "c.bin"] {
        origin.add(name, 2 * MIB as usize);
    }
    origin.limit_ip(net_ip(1), Some(MIB));
    let h = Harness::new(1);
    h.update_settings(|s| s.downloads_at_once = 1).await;

    let a = h.start(&origin, "a.bin", &[]).await;
    let b = h.start(&origin, "b.bin", &[]).await;
    let c = h.start(&origin, "c.bin", &[]).await;
    h.wait_for(c, "queued", |s| s.status == DownloadStatus::Queued).await;
    h.update_settings(|s| s.downloads_at_once = 3).await;
    for id in [b, c] {
        h.wait_for(id, "started", |s| s.bytes_downloaded > 0).await;
    }
    assert_eq!(h.latest(a).unwrap().status, DownloadStatus::Downloading, "a still running");
    for id in [a, b, c] {
        h.wait_status(id, DownloadStatus::Completed).await;
    }
}

/// Whatever the UI sends, what the manager saves (and answers, and broadcasts) is in range — the
/// store's own sanitising rules — and it survives a relaunch. A schedule switched on but unusable
/// is refused outright (with a reason for the UI) rather than quietly saved as "never": the user
/// would otherwise wait for downloads that never start.
#[tokio::test(flavor = "multi_thread")]
async fn update_settings_validates_and_persists() {
    let mut h = Harness::new(1);
    let before = h.settings().await;
    let mut broken = before.clone();
    broken.schedule =
        DownloadSchedule { enabled: true, start_minute: 60, end_minute: 60, ..Default::default() };
    let refused =
        h.ask(|reply| sonic_core::Command::UpdateSettings { settings: broken, reply }).await;
    assert!(refused.is_err_and(|e| !e.is_empty()), "an enabled, invalid schedule is refused");
    assert_eq!(h.settings().await, before, "and nothing else changed with it");

    let mut sent = before;
    sent.downloads_at_once = 99;
    sent.speed_limit = Some(0);
    sent.slow_mode_speed = 0;
    sent.destination_dir = Some("relative/dir".into());
    // Off, an invalid window is harmless: saved as the store saves it.
    sent.schedule =
        DownloadSchedule { enabled: false, start_minute: 60, end_minute: 60, ..Default::default() };
    sent.network_preferences
        .insert(net(1), NetworkPreference { data_limit: Some(0), ..Default::default() });
    sent.network_preferences.insert(
        net(2),
        NetworkPreference { custom_name: Some("Phone".into()), ..Default::default() },
    );

    let saved = h
        .ask(|reply| sonic_core::Command::UpdateSettings { settings: sent.clone(), reply })
        .await
        .expect("accepted");
    assert_eq!(saved, sonic_store::sanitize_settings(sent));
    assert_eq!(saved.downloads_at_once, DOWNLOADS_AT_ONCE_MAX);
    assert_eq!(saved.speed_limit, None, "0 is no limit");
    assert_ne!(saved.slow_mode_speed, 0);
    assert_eq!(saved.destination_dir, None, "a relative folder is refused");
    assert!(
        !saved.schedule.enabled && saved.schedule.days.is_empty(),
        "an invalid window runs never"
    );
    assert!(!saved.network_preferences.contains_key(&net(1)), "nothing customised left");
    assert_eq!(saved.network_preferences[&net(2)].custom_name.as_deref(), Some("Phone"));
    h.eventually("the new settings are broadcast", WAIT, || {
        h.events().iter().any(|e| matches!(e, Event::Settings(s) if **s == saved))
    })
    .await;

    let low = h.update_settings(|s| s.downloads_at_once = 0).await;
    assert_eq!(low.downloads_at_once, DOWNLOADS_AT_ONCE_MIN);

    h.restart().await;
    assert_eq!(h.settings().await, low, "saved settings come back after a relaunch");
}

/// The local minute of the day, `offset` minutes from now.
fn minute_from_now(offset: u32) -> u16 {
    let now = chrono::Local::now();
    ((now.hour() * 60 + now.minute() + offset) % 1440) as u16
}

/// With the schedule on and its window hours away, a new download waits in the queue (enrolled)
/// and fetches nothing; switching the schedule off lets it run.
#[tokio::test(flavor = "multi_thread")]
async fn a_download_waits_for_the_schedule_window() {
    let origin = Origin::start().await;
    origin.add("a.bin", MIB as usize);
    let h = Harness::new(1);
    let schedule = DownloadSchedule {
        enabled: true,
        start_minute: minute_from_now(120),
        end_minute: minute_from_now(180),
        days: (0..7).collect(),
        end_date: None,
    };
    h.update_settings(|s| s.schedule = schedule.clone()).await;

    let id = h.start(&origin, "a.bin", &[]).await;
    let queued = h.wait_for(id, "queued", |s| s.status == DownloadStatus::Queued).await;
    assert!(queued.scheduled, "enrolled in the schedule");
    settle(600).await;
    let still = h.latest(id).unwrap();
    assert_eq!((still.status, still.bytes_downloaded), (DownloadStatus::Queued, 0));
    assert!(origin.chunk_requests("a.bin").is_empty(), "nothing fetched outside the window");

    h.update_settings(|s| s.schedule.enabled = false).await;
    h.wait_status(id, DownloadStatus::Completed).await;
}

/// The other side of the schedule: inside an open window an enrolled download runs at once — a
/// window computed wrongly (time zone, a day off) would otherwise hold it back for no reason.
#[tokio::test(flavor = "multi_thread")]
async fn a_download_enrolled_while_its_window_is_open_runs() {
    let origin = Origin::start().await;
    origin.add("a.bin", MIB as usize);
    let h = Harness::new(1);
    let schedule = DownloadSchedule {
        enabled: true,
        start_minute: minute_from_now(1439), // a minute ago
        end_minute: minute_from_now(120),
        days: (0..7).collect(),
        end_date: None,
    };
    h.update_settings(|s| s.schedule = schedule.clone()).await;

    let id = h.start(&origin, "a.bin", &[]).await;
    let done = h.wait_status(id, DownloadStatus::Completed).await;
    assert!(done.scheduled, "enrolled, and inside its window");
}
