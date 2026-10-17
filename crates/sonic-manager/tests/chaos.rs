//! Model-based chaos (Plexo's chaos.spec.ts). A seeded generator draws a random sequence of user
//! actions and failures — pause, resume, quit and relaunch, crash, networks switched off and
//! on, a flaky server, waiting — and runs it against a real manager and a throttled origin. The
//! property: whatever the sequence, once the faults stop and everything is switched back on, the
//! download completes with the exact bytes, and every update along the way obeyed the
//! invariants (the harness checks both).
//!
//! A failing run prints its seed and the sequence. Replay it with
//! `SONIC_CHAOS_SEED=<seed> cargo test -p sonic-manager --test chaos chaos_replay -- --ignored`;
//! `SONIC_CHAOS_RUNS=<n>` there runs n seeds from it (a soak).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sonic_core::{DownloadId, DownloadStatus};
use sonic_testkit::*;

/// xorshift64*: small, seedable, the same everywhere.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[derive(Clone, Debug)]
enum Action {
    Wait(u64),
    Pause,
    Resume,
    Quit,
    Crash,
    NetworkOff(usize),
    NetworkOn(usize),
    /// The next download requests get these faults, one each (replacing any left).
    Flaky(Vec<Fault>),
}

const NETWORKS: usize = 2;
const SIZE: u64 = 48 * BLOCK;

fn fault(rng: &mut Rng) -> Fault {
    match rng.below(8) {
        0 => Fault::CutAfter(rng.below(BLOCK)),
        1 => Fault::status(500),
        2 => Fault::Status { code: 503, retry_after: Some(0) },
        3 => Fault::WrongStart,
        4 => Fault::Overlong,
        5 => Fault::ShortBody(rng.below(BLOCK)),
        6 => Fault::NoContentRange,
        _ => Fault::StallBody(rng.below(BLOCK)),
    }
}

fn plan(rng: &mut Rng) -> Vec<Action> {
    let count = 4 + rng.below(9);
    (0..count)
        .map(|_| match rng.below(10) {
            0..=2 => Action::Wait(20 + rng.below(500)),
            3 => Action::Pause,
            4 => Action::Resume,
            5 => Action::Quit,
            6 => Action::Crash,
            7 => Action::NetworkOff(1 + rng.below(NETWORKS as u64) as usize),
            8 => Action::NetworkOn(1 + rng.below(NETWORKS as u64) as usize),
            _ => Action::Flaky((0..1 + rng.below(3)).map(|_| fault(rng)).collect()),
        })
        .collect()
}

/// The status now; fails the run if the download ended badly.
fn status(h: &Harness, id: DownloadId, seed: u64) -> Option<DownloadStatus> {
    let state = h.latest(id)?;
    if matches!(state.status, DownloadStatus::Error | DownloadStatus::Cancelled) {
        panic!("seed {seed}: the download ended {:?}: {}", state.status, describe(&state));
    }
    Some(state.status)
}

/// After a relaunch: unfinished downloads come back paused; a finished one isn't listed.
async fn check_restored(h: &Harness, id: DownloadId, seed: u64) {
    match h.wait_restored(id).await {
        Some(restored) => assert_eq!(
            restored.status,
            DownloadStatus::Paused,
            "seed {seed}: restored as {}",
            describe(&restored)
        ),
        None => assert_eq!(
            h.latest(id).map(|s| s.status),
            Some(DownloadStatus::Completed),
            "seed {seed}: the download vanished across the relaunch"
        ),
    }
}

async fn chaos(seed: u64) {
    let mut rng = Rng::new(seed);
    let streams = [Some(1), Some(2), Some(4), None][rng.below(4) as usize];
    let actions = plan(&mut rng);
    eprintln!(
        "chaos seed {seed} (replay: SONIC_CHAOS_SEED={seed}), streams {streams:?}: {actions:?}"
    );

    let origin = Origin::start().await;
    let bytes = Arc::new(seeded_bytes(SIZE as usize, seed as u32));
    origin.add_bytes("chaos.bin", bytes, Some("\"chaos\"".into()));
    for i in 1..=NETWORKS {
        origin.limit_ip(net_ip(i), Some(512 * KIB));
    }
    let faults: Arc<Mutex<VecDeque<Fault>>> = Arc::default();
    let pending = faults.clone();
    origin.set_rule(move |r| {
        (r.method == "GET" && !r.is_probe()).then(|| pending.lock().unwrap().pop_front()).flatten()
    });

    let mut h = Harness::with(NETWORKS, |t| t.hedge_after = Duration::from_millis(300));
    h.streams = streams;
    let id = h.start(&origin, "chaos.bin", &[]).await;

    for action in actions {
        if status(&h, id, seed) == Some(DownloadStatus::Completed) {
            break;
        }
        match action {
            Action::Wait(ms) => settle(ms).await,
            Action::Pause => {
                if status(&h, id, seed) == Some(DownloadStatus::Downloading) {
                    h.pause(id);
                    h.wait_for(id, "paused (or done)", |s| {
                        matches!(s.status, DownloadStatus::Paused | DownloadStatus::Completed)
                    })
                    .await;
                }
            }
            Action::Resume => {
                if status(&h, id, seed) == Some(DownloadStatus::Paused) {
                    h.resume(id);
                    h.wait_for(id, "resumed", |s| s.status != DownloadStatus::Paused).await;
                }
            }
            Action::Quit => {
                h.restart().await;
                check_restored(&h, id, seed).await;
            }
            Action::Crash => {
                h.kill().await;
                h.launch();
                check_restored(&h, id, seed).await;
            }
            Action::NetworkOff(i) => h.set_network(id, &net(i), false),
            Action::NetworkOn(i) => h.set_network(id, &net(i), true),
            Action::Flaky(new) => *faults.lock().unwrap() = new.into(),
        }
    }

    // Faults off, everything back on, and let it finish.
    faults.lock().unwrap().clear();
    for i in 1..=NETWORKS {
        h.set_network(id, &net(i), true);
    }
    settle(100).await;
    if status(&h, id, seed) == Some(DownloadStatus::Paused) {
        h.resume(id);
    }
    let done = h
        .wait_for_within(id, Duration::from_secs(40), "completion", |s| {
            s.status == DownloadStatus::Completed || s.status == DownloadStatus::Error
        })
        .await;
    assert_eq!(done.status, DownloadStatus::Completed, "seed {seed}: {}", describe(&done));
}

#[tokio::test(flavor = "multi_thread")]
async fn chaos_seed_1() {
    chaos(1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn chaos_seed_2() {
    chaos(2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn chaos_seed_3() {
    chaos(3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn chaos_seed_4() {
    chaos(4).await;
}

/// Replays `SONIC_CHAOS_SEED` (default: one from the clock, printed), `SONIC_CHAOS_RUNS` times
/// with consecutive seeds.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "replay / soak: set SONIC_CHAOS_SEED and SONIC_CHAOS_RUNS"]
async fn chaos_replay() {
    let seed =
        std::env::var("SONIC_CHAOS_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or_else(|| {
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
        });
    let runs: u64 =
        std::env::var("SONIC_CHAOS_RUNS").ok().and_then(|s| s.parse().ok()).unwrap_or(1);
    for seed in seed..seed + runs {
        chaos(seed).await;
    }
}
