//! Small helpers shared by the manager's tasks.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

use sonic_core::Millis;
use tokio::time::Instant;

/// The download tick clock: milliseconds since the manager started, on Tokio's clock. Everything
/// the engine judges by time (attempts, meters, the disk, stream counts) reads this one, so a test
/// on paused time sees every rule move with it. Wall-clock things (`started_at`, `paused_at`)
/// use [`sonic_core::now_millis`] instead.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Clock {
    base: Instant,
}

impl Clock {
    pub(crate) fn new() -> Self {
        Self { base: Instant::now() }
    }

    pub(crate) fn now(&self) -> Millis {
        Millis::try_from(self.base.elapsed().as_millis()).unwrap_or(Millis::MAX)
    }
}

/// A number in `0.0..1.0` that differs from call to call. Only for spreading retries apart, so
/// no PRNG crate: the standard library's randomly keyed hasher is enough.
pub(crate) fn jitter() -> f64 {
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(sonic_core::now_millis() as u64);
    (hasher.finish() >> 11) as f64 / (1u64 << 53) as f64
}

/// The wait before retry number `failures` (1-based): doubling from `base` up to `max`, with ±20 %
/// jitter as gRPC's backoff has — a network that drops fails all its streams at once, and they
/// shouldn't all come back at the same instant.
pub(crate) fn backoff(failures: u32, base: Duration, max: Duration) -> Duration {
    let exponent = failures.saturating_sub(1).min(30);
    let exact = base.saturating_mul(1u32 << exponent).min(max);
    exact.mul_f64(0.8 + 0.4 * jitter())
}

/// Sleeps until `at`, or forever when there is nothing to wait for — a `select!` branch that is
/// only sometimes armed.
pub(crate) async fn sleep_until_opt(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// `n` bytes as gigabytes, for messages.
pub(crate) fn gigabytes(bytes: u64) -> String {
    format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_up_to_the_cap_with_jitter() {
        let base = Duration::from_millis(100);
        let max = Duration::from_millis(1000);
        for _ in 0..50 {
            let first = backoff(1, base, max);
            assert!(first >= Duration::from_millis(80) && first <= Duration::from_millis(120));
            let third = backoff(3, base, max);
            assert!(third >= Duration::from_millis(320) && third <= Duration::from_millis(480));
            let capped = backoff(40, base, max);
            assert!(capped >= Duration::from_millis(800) && capped <= Duration::from_millis(1200));
        }
    }

    #[test]
    fn jitter_stays_in_range() {
        for _ in 0..100 {
            let j = jitter();
            assert!((0.0..1.0).contains(&j));
        }
    }
}
