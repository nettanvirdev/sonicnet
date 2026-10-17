//! Speeds, time left, the peak speed and the speed history, all read on the download's tick clock.

use std::collections::{BTreeMap, HashMap, VecDeque};

use sonic_core::{DownloadNetwork, Millis, NetworkId, SPEED_HISTORY_SECONDS, StreamId};

/// Raw per-arrival deltas are too noisy to show (socket buffers flush in irregular bursts a few ms
/// apart); averaged over a few seconds the speed tracks reality without jumping around.
pub const SPEED_WINDOW_MS: Millis = 3_000;

/// The shortest span a rate is taken over. The clock can come round again milliseconds later (a
/// stream ending wakes it), and one burst over a few ms reads as a speed the connection never had
/// — which would then stick as the peak.
pub const MIN_SPAN_MS: Millis = 1_000;

/// Bytes counted as they arrive, and read as a rate only on the download's clock.
///
/// Arrivals come in bursts — socket flushes, and a speed limit pausing reads between them — so a
/// window that starts or ends on one is off by up to a burst (sampled on arrival, a 7 MB/s limit
/// read 6.9). Between two clock readings, where the bursts fall doesn't matter.
#[derive(Clone, Debug, Default)]
pub struct Meter {
    total: u64,
    /// `(time, total)` at each read, back to the newest one at least a window old.
    readings: VecDeque<(Millis, u64)>,
}

impl Meter {
    pub fn add(&mut self, bytes: u64) {
        self.total += bytes;
    }

    /// The rate in bytes a second over the last [`SPEED_WINDOW_MS`], as of `now`. Bytes before the
    /// first read are the meter's starting point, not part of any rate.
    pub fn read(&mut self, now: Millis) -> f64 {
        self.readings.push_back((now, self.total));
        while self.readings.len() > 2 && self.readings[1].0 <= now - SPEED_WINDOW_MS {
            self.readings.pop_front();
        }
        // Invariant: just pushed, so never empty.
        let (since, bytes) = self.readings[0];
        let span_ms = (now - since).max(MIN_SPAN_MS);
        (self.total - bytes) as f64 / (span_ms as f64 / 1000.0)
    }
}

/// A download's meters: each stream's, and each network's. A network's is its own rather than its
/// streams' added up, so its bytes stay counted when a stream ends.
#[derive(Clone, Debug, Default)]
pub struct Meters {
    streams: HashMap<StreamId, Meter>,
    networks: HashMap<NetworkId, Meter>,
}

impl Meters {
    /// Counts `bytes` received by `stream`, on `network`.
    pub fn add(&mut self, stream: StreamId, network: &NetworkId, bytes: u64) {
        self.streams.entry(stream).or_default().add(bytes);
        self.networks.entry(network.clone()).or_default().add(bytes);
    }

    /// A stream's speed as of `now`; 0 for one that never received anything. Read every tick,
    /// moving or not, so its window is never stale when it moves again.
    pub fn stream_speed(&mut self, stream: StreamId, now: Millis) -> f64 {
        self.streams.get_mut(&stream).map_or(0.0, |meter| meter.read(now))
    }

    /// A network's speed as of `now`; 0 for one that never received anything.
    pub fn network_speed(&mut self, network: &NetworkId, now: Millis) -> f64 {
        self.networks.get_mut(network).map_or(0.0, |meter| meter.read(now))
    }

    /// Speeds are a run's own: nothing carries over a pause.
    pub fn clear(&mut self) {
        self.streams.clear();
        self.networks.clear();
    }
}

/// How far time left moves toward a new, lower estimate in a second. Further down than up, so a
/// speed-up shows at once while a wobble doesn't add minutes. Firefox's shares per update
/// (`DownloadUtils.getTimeLeft`), here per second so the tick's pace doesn't change them.
pub const TIME_LEFT_DOWN: f64 = 0.3;
/// How far time left moves toward a new, higher estimate in a second.
pub const TIME_LEFT_UP: f64 = 0.1;

/// Time left in seconds, `elapsed_secs` after `previous`, given a fresh `estimate`.
pub fn smooth_time_left(previous: Option<f64>, elapsed_secs: f64, estimate: f64) -> f64 {
    // Between estimates it counts down on its own.
    let expected = previous.map_or(0.0, |previous| previous - elapsed_secs);
    // The first estimate, or far under what's expected (a slow start picking up): as is.
    if expected <= 0.0 || estimate <= expected / 2.0 {
        return estimate;
    }
    let share = if estimate < expected { TIME_LEFT_DOWN } else { TIME_LEFT_UP };
    expected + (estimate - expected) * (1.0 - (1.0 - share).powf(elapsed_secs))
}

/// Brings time left up to date: `remaining` bytes at `speed_bps`, `elapsed_secs` after it was
/// `previous`. `None` when there's no telling (unknown size, nothing moving).
pub fn time_left(
    previous: Option<f64>,
    elapsed_secs: f64,
    remaining: Option<u64>,
    speed_bps: f64,
) -> Option<f64> {
    let remaining = remaining?;
    (speed_bps > 0.0)
        .then(|| smooth_time_left(previous, elapsed_secs, remaining as f64 / speed_bps))
}

/// The peak is the best speed held this long: longer than a start's burst (an ISP's burst
/// allowance, every connection ramping up at once), which isn't the line's speed.
pub const PEAK_MS: Millis = 5_000;

/// The best speed a download held for [`PEAK_MS`], read from its byte count once a second.
///
/// Read from the bytes themselves — what the average speed is made of — so the peak can't read
/// below the average (an average is never above its best stretch). Readings run on across a pause,
/// shifted up to the resume (see [`resume`](Self::resume)), so short runs between pauses count
/// too, as they do in the average.
#[derive(Clone, Debug, Default)]
pub struct PeakTracker {
    /// `(time, bytes)`, back to the newest reading at least [`PEAK_MS`] old.
    readings: VecDeque<(Millis, u64)>,
    peak: Option<f64>,
}

impl PeakTracker {
    /// Picks up where a saved download left off.
    pub fn new(peak: Option<f64>) -> Self {
        Self { readings: VecDeque::new(), peak }
    }

    pub fn peak(&self) -> Option<f64> {
        self.peak
    }

    /// A run starts at `now` with `bytes` downloaded. Time paused isn't time downloading, so the
    /// readings move up to now and a stretch can run across the pause.
    pub fn resume(&mut self, now: Millis, bytes: u64) {
        if let Some(&(last, _)) = self.readings.back() {
            let paused = now - last;
            self.readings.iter_mut().for_each(|(time, _)| *time += paused);
        }
        if self.readings.back().is_none_or(|&(_, last)| last != bytes) {
            self.readings.push_back((now, bytes));
        }
    }

    /// Once a second while downloading, and once as a run stops. `active_ms` is the time spent
    /// downloading so far, pauses left out (`DownloadState::active_ms`).
    pub fn sample(&mut self, now: Millis, bytes: u64, active_ms: u64) {
        self.readings.push_back((now, bytes));
        // By time rather than by count: a run that stops between ticks adds a reading, and counted,
        // a short stretch would pass for a full one.
        while self.readings.len() > 1 && self.readings[1].0 <= now - PEAK_MS {
            self.readings.pop_front();
        }
        let (first, first_bytes) = self.readings[0]; // Invariant: just pushed.
        if now - first < PEAK_MS {
            return;
        }
        let held = bytes.saturating_sub(first_bytes) as f64 / ((now - first) as f64 / 1000.0);
        // Stretches are only read where a reading falls, so the first or last few seconds can go
        // unread as one — and when those were the fastest, every stretch read is slower than the
        // average. The best stretch can't be, so past PEAK_MS the average is a floor.
        let average = if active_ms >= PEAK_MS as u64 {
            bytes as f64 / (active_ms as f64 / 1000.0)
        } else {
            0.0
        };
        self.peak = Some(self.peak.unwrap_or(0.0).max(held).max(average));
    }

    /// The download completed. One done before it downloaded for [`PEAK_MS`] has no peak yet: its
    /// speed start to finish (pauses closed up by the readings) is the best it held.
    pub fn finish(&mut self) -> Option<f64> {
        let run = self.readings.front().zip(self.readings.back());
        match (self.peak, run) {
            (None, Some((&(start, from), &(end, to)))) if end > start => {
                let secs = (end - start) as f64 / 1000.0;
                self.peak = Some(to.saturating_sub(from) as f64 / secs);
            }
            _ => {}
        }
        self.peak
    }
}

/// Adds each network's speed to the history, and 0 for one no longer listed. A network seen for
/// the first time is back-filled with 0, so every series has the same length and they line up.
/// Called once a second; keeps the last [`SPEED_HISTORY_SECONDS`].
pub fn sample_speeds(history: &mut BTreeMap<NetworkId, Vec<f64>>, networks: &[DownloadNetwork]) {
    let length = history.values().next().map_or(0, Vec::len);
    for network in networks {
        history.entry(network.id.clone()).or_insert_with(|| vec![0.0; length]);
    }
    for (id, series) in history.iter_mut() {
        let speed = networks.iter().find(|n| &n.id == id).map_or(0.0, |n| n.speed_bps);
        series.push(speed);
        if series.len() > SPEED_HISTORY_SECONDS {
            series.drain(..series.len() - SPEED_HISTORY_SECONDS);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use sonic_core::{NetworkKind, NetworkStatus};

    #[test]
    fn a_meter_reads_the_rate_over_the_last_three_seconds() {
        let mut meter = Meter::default();
        meter.add(500); // before the first read: the starting point
        assert_eq!(meter.read(0), 0.0);
        for second in 1..=10 {
            meter.add(1000 * second as u64);
            meter.read(second * 1000);
        }
        // Seconds 8, 9, 10 brought 8000 + 9000 + 10000.
        meter.add(0);
        let mut copy = meter.clone();
        assert_eq!(copy.read(10_000), 27_000.0 / 3.0);
        assert_eq!(meter.read(13_000), 0.0);
    }

    #[test]
    fn a_meter_never_reads_a_burst_over_ms_as_a_speed() {
        let mut meter = Meter::default();
        meter.read(0);
        meter.add(1_000_000);
        assert_eq!(meter.read(5), 1_000_000.0, "over a second, not 5 ms");
    }

    #[test]
    fn meters_count_streams_and_networks_apart() {
        let mut meters = Meters::default();
        let a = NetworkId::new("a");
        meters.stream_speed(1, 0);
        meters.add(1, &a, 0);
        meters.network_speed(&a, 0);
        meters.stream_speed(1, 0);
        meters.add(1, &a, 1000);
        meters.add(2, &a, 1000);
        assert_eq!(meters.stream_speed(1, 1000), 1000.0);
        assert_eq!(meters.network_speed(&a, 1000), 2000.0);
        assert_eq!(meters.stream_speed(9, 1000), 0.0);
        meters.clear();
        assert_eq!(meters.network_speed(&a, 2000), 0.0);
    }

    #[test]
    fn time_left_takes_the_first_estimate_as_is() {
        assert_eq!(smooth_time_left(None, 0.5, 100.0), 100.0);
    }

    #[test]
    fn time_left_counts_down_on_its_own_between_estimates() {
        assert!((smooth_time_left(Some(100.0), 1.0, 99.0) - 99.0).abs() < 1e-9);
    }

    #[test]
    fn time_left_falls_faster_than_it_rises() {
        let down = 100.0 - smooth_time_left(Some(101.0), 1.0, 60.0);
        let up = smooth_time_left(Some(101.0), 1.0, 140.0) - 100.0;
        assert!((down - 0.3 * 40.0).abs() < 1e-9);
        assert!((up - 0.1 * 40.0).abs() < 1e-9);
    }

    #[test]
    fn time_left_jumps_to_an_estimate_far_below_what_was_expected() {
        assert_eq!(smooth_time_left(Some(101.0), 1.0, 40.0), 40.0);
    }

    #[test]
    fn no_time_left_without_a_size_or_a_speed() {
        assert_eq!(time_left(None, 1.0, None, 100.0), None);
        assert_eq!(time_left(None, 1.0, Some(100), 0.0), None);
        assert_eq!(time_left(None, 1.0, Some(100), 50.0), Some(2.0));
    }

    #[test]
    fn the_peak_is_the_best_speed_held_five_seconds() {
        let mut peak = PeakTracker::new(None);
        // 1 s burst at 10 MB, then a steady 1 MB/s.
        peak.resume(0, 0);
        let mut bytes = 10_000_000;
        for second in 1..=10 {
            peak.sample(second * 1000, bytes, second as u64 * 1000);
            if second < 5 {
                assert_eq!(peak.peak(), None, "nothing held for five seconds yet");
            }
            bytes += 1_000_000;
        }
        // The burst counts spread over the first five seconds, never as a speed of its own.
        let first = (10_000_000 + 4 * 1_000_000) as f64 / 5.0;
        assert_eq!(peak.peak(), Some(first));
    }

    #[test]
    fn the_peak_never_reads_below_the_average_once_five_seconds_in() {
        let mut peak = PeakTracker::new(None);
        peak.resume(0, 0);
        // Everything arrived in the first second, which no five-second stretch read covers.
        peak.sample(1000, 5_000_000, 1000);
        peak.sample(6_000, 5_000_000, 6_000);
        assert_eq!(peak.peak(), Some(5_000_000.0 / 6.0));
    }

    #[test]
    fn the_peak_survives_a_pause_by_shifting_its_readings() {
        let mut peak = PeakTracker::new(None);
        peak.resume(0, 0);
        peak.sample(1000, 1000, 1000);
        peak.sample(2000, 2000, 2000);
        peak.sample(3000, 3000, 3000);
        // Paused for an hour, then three more seconds at the same pace.
        peak.resume(3_603_000, 3000);
        assert_eq!(peak.peak(), None);
        peak.sample(3_604_000, 4000, 4000);
        peak.sample(3_605_000, 5000, 5000);
        assert_eq!(
            peak.peak(),
            Some(1000.0),
            "a stretch across the pause, not 5000 B over an hour"
        );
    }

    #[test]
    fn a_download_done_before_five_seconds_gets_its_whole_run_as_peak() {
        let mut peak = PeakTracker::new(None);
        peak.resume(0, 0);
        peak.sample(1000, 4000, 1000);
        peak.sample(2000, 8000, 2000);
        assert_eq!(peak.finish(), Some(4000.0));
        // A saved peak is kept.
        let mut saved = PeakTracker::new(Some(99.0));
        saved.resume(0, 0);
        saved.sample(1000, 1_000_000, 1000);
        assert_eq!(saved.finish(), Some(99.0));
    }

    fn network(id: &str, speed: f64) -> DownloadNetwork {
        DownloadNetwork {
            id: NetworkId::new(id),
            label: id.into(),
            kind: NetworkKind::Other,
            enabled: true,
            status: NetworkStatus::On,
            error: None,
            bytes_downloaded: 0,
            speed_bps: speed,
            retries: 0,
        }
    }

    #[test]
    fn speed_history_series_line_up() {
        let mut history = BTreeMap::new();
        sample_speeds(&mut history, &[network("a", 1.0)]);
        sample_speeds(&mut history, &[network("a", 2.0), network("b", 5.0)]);
        sample_speeds(&mut history, &[network("b", 6.0)]);
        assert_eq!(history[&NetworkId::new("a")], [1.0, 2.0, 0.0]);
        assert_eq!(history[&NetworkId::new("b")], [0.0, 5.0, 6.0]);
    }

    proptest! {
        #[test]
        fn speed_history_is_equal_length_and_bounded(
            ticks in proptest::collection::vec(proptest::collection::vec(0usize..4, 0..4), 0..150),
        ) {
            let mut history = BTreeMap::new();
            for present in ticks {
                let networks: Vec<_> =
                    present.iter().map(|n| network(&n.to_string(), *n as f64)).collect();
                sample_speeds(&mut history, &networks);
                let lengths: Vec<_> = history.values().map(Vec::len).collect();
                prop_assert!(lengths.windows(2).all(|w| w[0] == w[1]));
                prop_assert!(lengths.iter().all(|l| *l <= SPEED_HISTORY_SECONDS));
            }
        }

        /// However bytes arrive, a meter read on a steady clock never reports more than came in
        /// over its span, nor a negative rate.
        #[test]
        fn a_meter_reads_what_came_in(
            arrivals in proptest::collection::vec((0u64..1_000_000, 1i64..2000), 1..60),
        ) {
            let mut meter = Meter::default();
            let mut now = 0;
            meter.read(now);
            let mut log = vec![(0i64, 0u64)];
            let mut total = 0;
            for (bytes, gap) in arrivals {
                now += gap;
                meter.add(bytes);
                total += bytes;
                let rate = meter.read(now);
                log.push((now, total));
                prop_assert!(rate >= 0.0);
                // Bounded by the most that arrived over any span the meter could have used.
                let most = log
                    .iter()
                    .map(|&(t, b)| (total - b) as f64 / ((now - t).max(MIN_SPAN_MS) as f64 / 1000.0))
                    .fold(0.0, f64::max);
                prop_assert!(rate <= most + 1e-6);
            }
        }
    }
}
