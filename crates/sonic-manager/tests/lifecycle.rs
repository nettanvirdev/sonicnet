//! A download's life: pause and resume, quitting and relaunching, crashing, cancelling and
//! removing. Progress must survive everything that isn't a cancel, and a cancel must leave
//! nothing behind.

use std::time::Duration;

use sonic_core::{DownloadStatus, StreamStatus};
use sonic_testkit::*;

/// Body bytes the origin sent for `name` from request index `from` on.
fn sent_since(origin: &Origin, name: &str, from: usize) -> u64 {
    origin.requests()[from..]
        .iter()
        .filter(|r| r.name == name && !r.is_probe())
        .map(|r| r.bytes_sent)
        .sum()
}

/// Pause stops every transfer (no more bytes, no more requests) and resume finishes the file
/// from where it was, not from the start: the progress made before the pause is kept.
#[tokio::test(flavor = "multi_thread")]
async fn pause_stops_everything_and_resume_carries_on() {
    let origin = Origin::start().await;
    let size = 3 * MIB;
    origin.add("a.bin", size as usize);
    origin.limit_ip(net_ip(1), Some(MIB));
    let h = Harness::new(1);

    let id = h.start(&origin, "a.bin", &[]).await;
    h.wait_for(id, "half a MiB", |s| s.bytes_downloaded >= MIB / 2).await;
    h.pause(id);
    let paused = h.wait_status(id, DownloadStatus::Paused).await;
    assert!(
        paused.streams.iter().all(|s| s.status != StreamStatus::Downloading),
        "no stream still fetching: {}",
        describe(&paused)
    );
    settle(200).await;
    let mark = origin.requests().len();
    let before = h.latest(id).unwrap().bytes_downloaded;
    settle(500).await;
    assert_eq!(origin.requests().len(), mark, "no request while paused");
    assert_eq!(h.latest(id).unwrap().bytes_downloaded, before, "no bytes while paused");

    h.resume(id);
    h.wait_status(id, DownloadStatus::Completed).await;
    // What it had is kept: what's fetched after resuming is about what was missing.
    let after = sent_since(&origin, "a.bin", mark);
    assert!(after <= size - before + 8 * BLOCK, "refetched: {after} sent after resuming");
}

/// Pause and resume at once, many times: a resume racing the pause's wind-down must neither
/// lose the download nor leave two sets of streams on it (the invariants would catch the second).
#[tokio::test(flavor = "multi_thread")]
async fn rapid_pause_resume_ends_in_the_exact_file() {
    let origin = Origin::start().await;
    origin.add("a.bin", 3 * MIB as usize);
    origin.limit_ip(net_ip(1), Some(2 * MIB));
    let h = Harness::new(1);

    let id = h.start(&origin, "a.bin", &[]).await;
    h.wait_for(id, "progress", |s| s.bytes_downloaded > 0).await;
    for _ in 0..10 {
        h.pause(id);
        h.resume(id);
        settle(30).await;
    }
    h.pause(id);
    h.wait_for(id, "paused or done", |s| {
        matches!(s.status, DownloadStatus::Paused | DownloadStatus::Completed)
    })
    .await;
    h.resume(id);
    h.wait_status(id, DownloadStatus::Completed).await;
}

/// Quitting the app (Shutdown) saves every download; relaunched, it comes back paused with its
/// progress, and resumes rather than starting over.
#[tokio::test(flavor = "multi_thread")]
async fn progress_survives_a_restart() {
    let origin = Origin::start().await;
    let size = 4 * MIB;
    origin.add("a.bin", size as usize);
    origin.limit_ip(net_ip(1), Some(MIB));
    let mut h = Harness::new(1);

    let id = h.start(&origin, "a.bin", &[]).await;
    let before = h.wait_for(id, "a MiB", |s| s.bytes_downloaded >= MIB).await.bytes_downloaded;
    h.restart().await;

    let restored = h.wait_restored(id).await.expect("the download is restored");
    assert_eq!(restored.status, DownloadStatus::Paused, "{}", describe(&restored));
    assert!(
        restored.bytes_downloaded >= before,
        "Shutdown saves everything: {} restored, {before} seen before quitting",
        restored.bytes_downloaded
    );
    assert_eq!(restored.total_bytes, Some(size));
    let mark = origin.requests().len();

    h.resume(id);
    h.wait_status(id, DownloadStatus::Completed).await;
    let after = sent_since(&origin, "a.bin", mark);
    assert!(
        after <= size - restored.bytes_downloaded + 8 * BLOCK,
        "resumed, not restarted: {after} bytes sent after the relaunch"
    );
}

/// Killed without a word (crash, power cut) at a precise byte — early, before anything may have
/// been saved, and mid-block: relaunched, it comes back paused, never claiming more than it had,
/// and finishes byte-exact. Whatever the staging file and the saved manifest disagree about must
/// be settled in favour of a correct file.
#[tokio::test(flavor = "multi_thread")]
async fn a_crash_at_any_point_recovers() {
    for offset in [100, 20 * BLOCK + 777] {
        let origin = Origin::start().await;
        let size = 3 * MIB;
        origin.add("a.bin", size as usize);
        let mut h = Harness::new(1);

        let reached = origin.hold(offset);
        let id = h.start(&origin, "a.bin", &[]).await;
        tokio::time::timeout(WAIT, reached).await.expect("the transfer reaches the held byte");
        h.wait_for(id, "progress", |s| s.bytes_downloaded > 0).await;
        settle(400).await; // let a checkpoint (every 200 ms) or two happen, or not

        h.kill().await;
        origin.release();
        h.launch();

        let restored = h.wait_restored(id).await.expect("the download survives the crash");
        assert_eq!(restored.status, DownloadStatus::Paused, "{}", describe(&restored));
        assert!(
            restored.bytes_downloaded <= offset,
            "claims {} bytes, only {offset} were ever sent",
            restored.bytes_downloaded
        );
        h.resume(id);
        h.wait_status(id, DownloadStatus::Completed).await;
    }
}

/// A crash right after a quit-and-relaunch, then another relaunch: saved state written by one
/// launch must read back in the next, over and over.
#[tokio::test(flavor = "multi_thread")]
async fn repeated_restarts_and_crashes() {
    let origin = Origin::start().await;
    origin.add("a.bin", 4 * MIB as usize);
    origin.limit_ip(net_ip(1), Some(2 * MIB));
    let mut h = Harness::new(1);

    let id = h.start(&origin, "a.bin", &[]).await;
    for round in 0..3 {
        let now = h
            .wait_for(id, "running or done", |s| {
                matches!(s.status, DownloadStatus::Downloading | DownloadStatus::Completed)
            })
            .await;
        if now.status == DownloadStatus::Completed {
            break;
        }
        settle(250).await;
        if round % 2 == 0 {
            h.kill().await;
            h.launch();
        } else {
            h.restart().await;
        }
        match h.wait_restored(id).await {
            Some(restored) => {
                assert_eq!(restored.status, DownloadStatus::Paused);
                h.resume(id);
            }
            None => break, // finished before the restart
        }
    }
    h.wait_status(id, DownloadStatus::Completed).await;
}

/// Cancel stops the download and deletes what it downloaded: no partial file, no staging file
/// (the harness checks both once the test ends; this checks it promptly too).
#[tokio::test(flavor = "multi_thread")]
async fn cancel_deletes_everything() {
    let origin = Origin::start().await;
    origin.add("a.bin", 3 * MIB as usize);
    origin.limit_ip(net_ip(1), Some(MIB));
    let h = Harness::new(1);

    let id = h.start(&origin, "a.bin", &[]).await;
    let running = h.wait_for(id, "progress", |s| s.bytes_downloaded > 0).await;
    let staging = staging_path(&running.destination_path);
    h.eventually("the staging file exists", WAIT, || staging.exists()).await;

    h.cancel(id);
    h.eventually("cancelled", WAIT, || {
        h.was_removed(id) || h.latest(id).is_some_and(|s| s.status == DownloadStatus::Cancelled)
    })
    .await;
    h.eventually("nothing left on disk", Duration::from_secs(5), || {
        !staging.exists() && !running.destination_path.exists()
    })
    .await;
    let mark = origin.requests().len();
    settle(300).await;
    assert_eq!(origin.requests().len(), mark, "no request after the cancel");
}

/// Cancelling a paused download deletes its partial file just the same.
#[tokio::test(flavor = "multi_thread")]
async fn cancel_while_paused_deletes_everything() {
    let origin = Origin::start().await;
    origin.add("a.bin", 3 * MIB as usize);
    origin.limit_ip(net_ip(1), Some(MIB));
    let h = Harness::new(1);

    let id = h.start(&origin, "a.bin", &[]).await;
    h.wait_for(id, "progress", |s| s.bytes_downloaded > 0).await;
    h.pause(id);
    h.wait_status(id, DownloadStatus::Paused).await;
    h.cancel(id);
    h.eventually("cancelled", WAIT, || {
        h.was_removed(id) || h.latest(id).is_some_and(|s| s.status == DownloadStatus::Cancelled)
    })
    .await;
}

/// Removing a finished download takes it off the list and keeps its file (unless the user asks
/// for the file to go to the Recycle Bin, not exercised here).
#[tokio::test(flavor = "multi_thread")]
async fn removing_a_completed_download_keeps_its_file() {
    let origin = Origin::start().await;
    origin.add("a.bin", MIB as usize);
    let h = Harness::new(1);

    let id = h.start(&origin, "a.bin", &[]).await;
    let done = h.wait_status(id, DownloadStatus::Completed).await;
    h.remove(id, false);
    h.eventually("removed from the list", WAIT, || h.was_removed(id)).await;
    settle(200).await;
    assert!(done.destination_path.exists(), "the file stays");
}

/// Removing a download under way cancels it: nothing of it is left.
#[tokio::test(flavor = "multi_thread")]
async fn removing_a_running_download_leaves_nothing() {
    let origin = Origin::start().await;
    origin.add("a.bin", 3 * MIB as usize);
    origin.limit_ip(net_ip(1), Some(MIB));
    let h = Harness::new(1);

    let id = h.start(&origin, "a.bin", &[]).await;
    h.wait_for(id, "progress", |s| s.bytes_downloaded > 0).await;
    h.remove(id, false);
    h.eventually("removed from the list", WAIT, || h.was_removed(id)).await;
}

/// Two downloads of one file name into one folder must not write into each other: the second
/// gets its own name, and both files come out exact.
#[tokio::test(flavor = "multi_thread")]
async fn two_downloads_of_one_name_get_separate_files() {
    let origin = Origin::start().await;
    origin.add("a.bin", MIB as usize);
    origin.limit_ip(net_ip(1), Some(2 * MIB));
    let h = Harness::new(1);

    let first = h.start(&origin, "a.bin", &[]).await;
    let second = h.start(&origin, "a.bin", &[]).await;
    let a = h.wait_status(first, DownloadStatus::Completed).await;
    let b = h.wait_status(second, DownloadStatus::Completed).await;
    assert_ne!(a.destination_path, b.destination_path);
}
