//! Speed limits and data usage, for every download together.
//!
//! Every byte received passes through [`Limits::take`] on the network it came in on, which says how
//! long to stop reading for. Stopping reading is what slows the sender: TCP's window fills, and it
//! waits. The same call counts the bytes against the network's data usage for the current day,
//! week and month, in local time; the period a network's allowance counts over is the user's pick.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use chrono::{DateTime, Datelike, Days, Months, NaiveDate, TimeZone};
use serde::{Deserialize, Serialize};
use sonic_core::{AppSettings, DataPeriod, Millis, NetworkId};

use crate::schedule::at_local;

/// A rate as a bucket of tokens (bytes) that refills at `rate` a second. Taking more than it holds
/// leaves it in debt, and the debt is the wait. A second's worth is all it ever holds, so a quiet
/// spell doesn't buy a burst over the limit.
#[derive(Clone, Debug)]
pub struct Bucket {
    rate: u64,
    tokens: f64,
    /// When it was last taken from; `None` before the first take.
    at: Option<Millis>,
}

impl Bucket {
    /// Starts empty: the first second is as limited as any other.
    pub fn new(rate: u64) -> Self {
        Self { rate: rate.max(1), tokens: 0.0, at: None }
    }

    pub fn rate(&self) -> u64 {
        self.rate
    }

    /// Changes the rate in place, so a debt carries over.
    pub fn set_rate(&mut self, rate: u64) {
        self.rate = rate.max(1);
    }

    /// Takes `bytes` at `now`, and says how long to wait before taking more. A clock that went
    /// backwards refills nothing.
    pub fn take(&mut self, bytes: u64, now: Millis) -> Duration {
        let rate = self.rate as f64;
        let elapsed = self.at.map_or(0, |at| (now - at).max(0)) as f64 / 1000.0;
        self.tokens = (self.tokens + elapsed * rate).min(rate) - bytes as f64;
        self.at = Some(now);
        if self.tokens >= 0.0 {
            Duration::ZERO
        } else {
            // Saturates rather than panics on an absurd debt (exabytes taken at 1 B/s).
            Duration::try_from_secs_f64(-self.tokens / rate).unwrap_or(Duration::MAX)
        }
    }
}

/// The bucket for `rate`: `current` changed in place so a debt carries over, or none for no limit.
fn adjust(current: Option<Bucket>, rate: Option<u64>) -> Option<Bucket> {
    let rate = rate.filter(|rate| *rate > 0)?;
    let mut bucket = current.unwrap_or_else(|| Bucket::new(rate));
    bucket.set_rate(rate);
    Some(bucket)
}

/// The first day of the period after the one starting on `start`.
fn next_period_start(period: DataPeriod, start: NaiveDate) -> NaiveDate {
    match period {
        DataPeriod::Day => start + Days::new(1),
        DataPeriod::Week => start + Days::new(7),
        DataPeriod::Month => start.checked_add_months(chrono::Months::new(1)).unwrap_or(start),
    }
}

/// The first local day of the period `date` falls in. Weeks start on Monday.
pub fn period_start(period: DataPeriod, date: NaiveDate) -> NaiveDate {
    match period {
        DataPeriod::Day => date,
        DataPeriod::Week => date - Days::new(u64::from(date.weekday().num_days_from_monday())),
        DataPeriod::Month => date.with_day(1).unwrap_or(date),
    }
}

/// When the period `now` falls in ends and usage starts again from 0: local midnight at the start
/// of the next one (or the first moment after it, should the clocks skip midnight).
pub fn next_reset<Tz: TimeZone>(period: DataPeriod, now: &DateTime<Tz>) -> Option<DateTime<Tz>> {
    let start = period_start(period, now.date_naive());
    let next = match period {
        DataPeriod::Day => start.checked_add_days(Days::new(1)),
        DataPeriod::Week => start.checked_add_days(Days::new(7)),
        DataPeriod::Month => start.checked_add_months(Months::new(1)),
    }?;
    at_local(&now.timezone(), next.and_hms_opt(0, 0, 0)?)
}

/// What every network used in one calendar period.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeriodUsage {
    /// The period's first local day: which day, week or month this is.
    pub start: NaiveDate,
    pub bytes: BTreeMap<NetworkId, u64>,
}

/// Every network's usage in the current day, week and month, as saved between runs. All three are
/// kept whichever the user picked, so switching a network's period shows what it used in that one.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageSnapshot {
    pub day: PeriodUsage,
    pub week: PeriodUsage,
    pub month: PeriodUsage,
}

const PERIODS: [DataPeriod; 3] = [DataPeriod::Day, DataPeriod::Week, DataPeriod::Month];

impl UsageSnapshot {
    fn period(&self, period: DataPeriod) -> &PeriodUsage {
        match period {
            DataPeriod::Day => &self.day,
            DataPeriod::Week => &self.week,
            DataPeriod::Month => &self.month,
        }
    }

    fn period_mut(&mut self, period: DataPeriod) -> &mut PeriodUsage {
        match period {
            DataPeriod::Day => &mut self.day,
            DataPeriod::Week => &mut self.week,
            DataPeriod::Month => &mut self.month,
        }
    }
}

/// Speed limits (all downloads together, and per network) and data usage per network.
///
/// The bucket clock is `now`'s Unix milliseconds; the periods are `now`'s local dates.
#[derive(Clone, Debug, Default)]
pub struct Limits {
    total: Option<Bucket>,
    per_network: HashMap<NetworkId, Bucket>,
    /// Bytes each network may receive in its period.
    data_limits: HashMap<NetworkId, u64>,
    /// The period each network's allowance counts over; Month when not set.
    periods: HashMap<NetworkId, DataPeriod>,
    usage: UsageSnapshot,
}

impl Limits {
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes up the limits in `settings`: slow mode, when on, stands in for the total limit. A
    /// bucket that stays keeps its debt.
    pub fn configure(&mut self, settings: &AppSettings) {
        self.total = adjust(self.total.take(), settings.effective_speed_limit());
        let preferences = &settings.network_preferences;
        let mut buckets = std::mem::take(&mut self.per_network);
        for (id, preference) in preferences {
            if let Some(bucket) = adjust(buckets.remove(id), preference.speed_limit) {
                self.per_network.insert(id.clone(), bucket);
            }
        }
        self.data_limits = preferences
            .iter()
            .filter_map(|(id, preference)| Some((id.clone(), preference.data_limit?)))
            .collect();
        self.periods = preferences
            .iter()
            .map(|(id, preference)| (id.clone(), preference.data_period))
            .collect();
    }

    /// Counts `bytes` just received on `network`, and says how long to stop reading for: the longer
    /// of the total limit's wait and the network's own.
    ///
    /// Whether this used up the network's allowance is [`limit_reached`](Self::limit_reached); the
    /// caller asks once it is done with these bytes, since what it does about it may stop the very
    /// connection they came in on.
    pub fn take<Tz: TimeZone>(
        &mut self,
        network: &NetworkId,
        bytes: u64,
        now: &DateTime<Tz>,
    ) -> Duration {
        self.roll(now);
        for period in PERIODS {
            let used = self.usage.period_mut(period).bytes.entry(network.clone()).or_insert(0);
            *used = used.saturating_add(bytes);
        }
        let millis = now.timestamp_millis();
        let total = self.total.as_mut().map_or(Duration::ZERO, |b| b.take(bytes, millis));
        let own =
            self.per_network.get_mut(network).map_or(Duration::ZERO, |b| b.take(bytes, millis));
        total.max(own)
    }

    fn period_of(&self, network: &NetworkId) -> DataPeriod {
        self.periods.get(network).copied().unwrap_or_default()
    }

    /// What `network` used in its selected period.
    pub fn used<Tz: TimeZone>(&mut self, network: &NetworkId, now: &DateTime<Tz>) -> u64 {
        self.roll(now);
        self.usage.period(self.period_of(network)).bytes.get(network).copied().unwrap_or(0)
    }

    /// Whether `network` has used up its data allowance for its selected period.
    pub fn limit_reached<Tz: TimeZone>(&mut self, network: &NetworkId, now: &DateTime<Tz>) -> bool {
        let limit = self.data_limits.get(network).copied();
        limit.is_some_and(|limit| self.used(network, now) >= limit)
    }

    /// Each network's usage in its selected period, for `Event::NetworkUsage`.
    pub fn used_by_network<Tz: TimeZone>(
        &mut self,
        now: &DateTime<Tz>,
    ) -> BTreeMap<NetworkId, u64> {
        self.roll(now);
        let ids: Vec<NetworkId> = PERIODS
            .iter()
            .flat_map(|period| self.usage.period(*period).bytes.keys().cloned())
            .collect();
        ids.into_iter()
            .map(|id| {
                let used = self.usage.period(self.period_of(&id)).bytes.get(&id).copied();
                (id, used.unwrap_or(0))
            })
            .collect()
    }

    /// Zeroes `network`'s usage in its selected period only: its other periods, and every other
    /// network, keep theirs.
    pub fn reset<Tz: TimeZone>(&mut self, network: &NetworkId, now: &DateTime<Tz>) {
        self.roll(now);
        let period = self.period_of(network);
        self.usage.period_mut(period).bytes.insert(network.clone(), 0);
    }

    /// The usage to save, current as of `now`.
    pub fn export<Tz: TimeZone>(&mut self, now: &DateTime<Tz>) -> UsageSnapshot {
        self.roll(now);
        self.usage.clone()
    }

    /// Takes up saved usage: each period that is still the current one replaces what is counted for
    /// it; a period that has since ended is dropped. Meant for startup, before any bytes flow.
    pub fn import<Tz: TimeZone>(&mut self, saved: UsageSnapshot, now: &DateTime<Tz>) {
        self.roll(now);
        for period in PERIODS {
            let saved = saved.period(period);
            let current = self.usage.period_mut(period);
            if saved.start == current.start {
                current.bytes.extend(saved.bytes.iter().map(|(id, bytes)| (id.clone(), *bytes)));
            }
        }
    }

    /// Starts each period over whose calendar period has ended.
    ///
    /// Only forwards: a clock stepped back (a time zone change, a manual correction) must not wipe
    /// the usage counted so far, or a capped network could go over its allowance. A stored period
    /// more than one period ahead of today can only come from a clock that was wrong, so that one
    /// is started over too rather than freezing the count until the calendar catches up.
    fn roll<Tz: TimeZone>(&mut self, now: &DateTime<Tz>) {
        let today = now.date_naive();
        for period in PERIODS {
            let start = period_start(period, today);
            let usage = self.usage.period_mut(period);
            let ahead_by_a_period = usage.start > next_period_start(period, start);
            if usage.start < start || ahead_by_a_period {
                *usage = PeriodUsage { start, bytes: BTreeMap::new() };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_tz::NewYork2026;
    use chrono::{FixedOffset, Timelike};
    use proptest::prelude::*;
    use sonic_core::NetworkPreference;

    fn tz() -> FixedOffset {
        FixedOffset::east_opt(6 * 3600).expect("valid offset")
    }
    fn at(year: i32, month: u32, day: u32, hour: u32) -> DateTime<FixedOffset> {
        tz().with_ymd_and_hms(year, month, day, hour, 0, 0).single().expect("exists")
    }
    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).expect("valid date")
    }
    fn id(name: &str) -> NetworkId {
        NetworkId::new(name)
    }
    fn settings(prefs: &[(&str, NetworkPreference)]) -> AppSettings {
        AppSettings {
            network_preferences: prefs.iter().map(|(n, p)| (id(n), p.clone())).collect(),
            ..AppSettings::default()
        }
    }
    fn allowance(limit: u64, period: DataPeriod) -> NetworkPreference {
        NetworkPreference { data_limit: Some(limit), data_period: period, ..Default::default() }
    }

    #[test]
    fn a_bucket_holds_at_most_a_second_and_debt_is_the_wait() {
        let mut bucket = Bucket::new(1000);
        assert_eq!(bucket.take(500, 0), Duration::from_millis(500), "starts empty");
        assert_eq!(bucket.take(0, 500), Duration::ZERO);
        // An hour idle buys one second's worth, not an hour's.
        assert_eq!(bucket.take(1000, 3_600_000), Duration::ZERO);
        assert_eq!(bucket.take(1000, 3_600_000), Duration::from_secs(1));
    }

    #[test]
    fn an_absurd_debt_saturates_instead_of_panicking() {
        let mut bucket = Bucket::new(1);
        assert_eq!(bucket.take(u64::MAX, 0), Duration::MAX);
    }

    #[test]
    fn a_clock_stepped_back_keeps_the_usage() {
        let mut limits = Limits::new();
        limits.configure(&settings(&[("a", allowance(1000, DataPeriod::Day))]));
        limits.take(&id("a"), 600, &at(2026, 10, 6, 12));
        // Back to yesterday (a time zone change): the 600 bytes still count.
        limits.take(&id("a"), 500, &at(2026, 10, 5, 23));
        assert!(limits.limit_reached(&id("a"), &at(2026, 10, 5, 23)));
        // Forward into a new day: a fresh allowance.
        assert!(!limits.limit_reached(&id("a"), &at(2026, 10, 7, 0)));
        // A count stored by a clock that ran far ahead is started over, not frozen.
        limits.take(&id("a"), 900, &at(2026, 12, 1, 0));
        assert!(!limits.limit_reached(&id("a"), &at(2026, 10, 8, 0)));
    }

    #[test]
    fn a_rate_change_keeps_the_debt() {
        let mut bucket = Bucket::new(1000);
        bucket.take(2000, 0);
        bucket.set_rate(2000);
        assert_eq!(bucket.take(0, 0), Duration::from_secs(1));
        assert_eq!(bucket.rate(), 2000);
    }

    #[test]
    fn the_wait_is_the_longer_of_the_total_and_the_networks_own() {
        let mut limits = Limits::new();
        let mut s =
            settings(&[("a", NetworkPreference { speed_limit: Some(100), ..Default::default() })]);
        s.speed_limit = Some(1000);
        limits.configure(&s);
        assert_eq!(limits.take(&id("a"), 200, &at(2026, 10, 5, 0)), Duration::from_secs(2));
        assert_eq!(limits.take(&id("b"), 0, &at(2026, 10, 5, 0)), Duration::from_millis(200));
        // Slow mode stands in for the total.
        s.slow_mode = true;
        s.slow_mode_speed = 10;
        limits.configure(&s);
        assert!(limits.take(&id("b"), 10, &at(2026, 10, 5, 0)) >= Duration::from_secs(1));
        // No limits at all: never a wait.
        limits.configure(&AppSettings::default());
        assert_eq!(limits.take(&id("a"), 1 << 30, &at(2026, 10, 5, 0)), Duration::ZERO);
    }

    #[test]
    fn calendar_periods_use_local_midnight_monday_weeks_and_year_ends() {
        assert_eq!(period_start(DataPeriod::Week, date(2026, 10, 4)), date(2026, 9, 28));
        assert_eq!(period_start(DataPeriod::Week, date(2026, 10, 5)), date(2026, 10, 5));
        assert_eq!(period_start(DataPeriod::Month, date(2027, 1, 1)), date(2027, 1, 1));
        let sunday_night = tz().with_ymd_and_hms(2026, 10, 4, 23, 59, 59).single().expect("ok");
        let monday = at(2026, 10, 5, 0);
        assert_eq!(next_reset(DataPeriod::Day, &sunday_night), Some(monday));
        assert_eq!(next_reset(DataPeriod::Week, &sunday_night), Some(monday));
        assert_eq!(next_reset(DataPeriod::Week, &monday), Some(at(2026, 10, 12, 0)));
        assert_eq!(next_reset(DataPeriod::Month, &at(2026, 12, 31, 23)), Some(at(2027, 1, 1, 0)));
    }

    #[test]
    fn next_reset_is_local_midnight_across_a_dst_change() {
        let before = at_local(&NewYork2026, date(2026, 3, 7).and_hms_opt(12, 0, 0).expect("ok"))
            .expect("exists");
        let reset = next_reset(DataPeriod::Week, &before).expect("exists");
        assert_eq!(reset.naive_local(), date(2026, 3, 9).and_hms_opt(0, 0, 0).expect("ok"));
        assert_eq!(reset.hour(), 0);
    }

    #[test]
    fn a_network_stops_at_its_allowance_for_its_period() {
        for period in PERIODS {
            let mut limits = Limits::new();
            limits.configure(&settings(&[("a", allowance(1000, period))]));
            let now = at(2026, 10, 7, 12);
            limits.take(&id("a"), 999, &now);
            assert!(!limits.limit_reached(&id("a"), &now));
            limits.take(&id("a"), 1, &now);
            assert!(limits.limit_reached(&id("a"), &now), "{period:?}");
            assert!(!limits.limit_reached(&id("b"), &now), "no allowance, never reached");
            // Raised: usable again.
            limits.configure(&settings(&[("a", allowance(5000, period))]));
            assert!(!limits.limit_reached(&id("a"), &now));
        }
    }

    #[test]
    fn usage_starts_over_when_its_period_ends() {
        let mut limits = Limits::new();
        let wednesday = at(2026, 10, 7, 12);
        limits.take(&id("a"), 100, &wednesday);
        let periods = |limits: &mut Limits, now| {
            PERIODS
                .map(|period| {
                    limits.configure(&settings(&[("a", allowance(1 << 40, period))]));
                    limits.used(&id("a"), &now)
                })
                .to_vec()
        };
        assert_eq!(periods(&mut limits, wednesday), [100, 100, 100]);
        assert_eq!(periods(&mut limits, at(2026, 10, 8, 0)), [0, 100, 100], "next day");
        assert_eq!(periods(&mut limits, at(2026, 10, 12, 0)), [0, 0, 100], "next week");
        assert_eq!(periods(&mut limits, at(2026, 11, 1, 0)), [0, 0, 0], "next month");
    }

    #[test]
    fn reset_zeroes_only_the_selected_period_of_one_network() {
        let mut limits = Limits::new();
        let now = at(2026, 10, 7, 12);
        limits.configure(&settings(&[("a", allowance(1 << 40, DataPeriod::Week))]));
        limits.take(&id("a"), 100, &now);
        limits.take(&id("b"), 50, &now);
        limits.reset(&id("a"), &now);
        assert_eq!(limits.used_by_network(&now), BTreeMap::from([(id("a"), 0), (id("b"), 50)]));
        limits.configure(&settings(&[("a", allowance(1 << 40, DataPeriod::Day))]));
        assert_eq!(limits.used(&id("a"), &now), 100);
    }

    #[test]
    fn usage_survives_export_and_import_but_not_an_ended_period() {
        let mut limits = Limits::new();
        let now = at(2026, 10, 7, 12);
        limits.take(&id("a"), 100, &now);
        let saved = limits.export(&now);
        let json = serde_json::to_string(&saved).expect("serialises");
        let saved: UsageSnapshot = serde_json::from_str(&json).expect("deserialises");
        assert_eq!(saved.week.start, date(2026, 10, 5));

        let mut restarted = Limits::new();
        restarted.import(saved.clone(), &now);
        assert_eq!(restarted.export(&now), limits.export(&now));

        // Restarted the next Monday: only the month is still current.
        let mut later = Limits::new();
        let monday = at(2026, 10, 12, 9);
        later.import(saved, &monday);
        let usage = later.export(&monday);
        assert!(usage.day.bytes.is_empty() && usage.week.bytes.is_empty());
        assert_eq!(usage.month.bytes[&id("a")], 100);
    }

    proptest! {
        /// A reader that waits as told never gets more than the rate over any stretch, plus the
        /// second's worth the bucket may hold and the read that put it in debt.
        #[test]
        fn a_bucket_never_lets_more_than_its_rate_through(
            rate in 1u64..1_000_000,
            reads in proptest::collection::vec((1u64..200_000, 0i64..3000), 1..80),
        ) {
            let mut bucket = Bucket::new(rate);
            let mut now: Millis = 0;
            let mut log: Vec<(Millis, u64)> = Vec::new();
            for (bytes, idle) in reads {
                now += idle;
                let wait = bucket.take(bytes, now);
                log.push((now, bytes));
                // Every window ending at this read.
                let mut sum = 0u64;
                for &(time, taken) in log.iter().rev() {
                    sum += taken;
                    let span = (now - time) as f64 / 1000.0;
                    let allowed = rate as f64 * (1.0 + span) + bytes as f64;
                    prop_assert!(sum as f64 <= allowed + 1.0, "{sum} over {span}s at {rate}/s");
                }
                now += wait.as_millis() as Millis + 1;
            }
        }
    }
}
