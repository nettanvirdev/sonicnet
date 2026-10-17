//! When the download schedule lets downloads run.
//!
//! Windows are in the computer's local wall-clock time, so everything here is generic over a
//! [`chrono::TimeZone`]: the app passes [`chrono::Local`], tests pass fixed or DST-changing zones.
//! Calendar arithmetic on local dates, rather than 24-hour offsets, keeps a window at the same
//! wall-clock time across DST changes.

use chrono::{
    DateTime, Datelike, Days, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, TimeZone, Timelike,
};
use sonic_core::DownloadSchedule;

/// What the schedule says about one moment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScheduleWindow<Tz: TimeZone> {
    /// Downloads may run now.
    pub allowed: bool,
    /// While allowed by a window: when it ends (exclusive). `None` when the schedule is off.
    pub ends_at: Option<DateTime<Tz>>,
    /// While not allowed: when the next window starts, if one does within the coming week.
    pub next_start: Option<DateTime<Tz>>,
    /// Not allowed, and no window will start again: the schedule's end date has passed.
    pub expired: bool,
}

/// The schedule's end date, when it is a real `YYYY-MM-DD` date. `Err` for one that isn't.
fn end_date(schedule: &DownloadSchedule) -> Result<Option<NaiveDate>, ()> {
    let Some(text) = &schedule.end_date else { return Ok(None) };
    let date = NaiveDate::parse_from_str(text, "%Y-%m-%d").map_err(|_| ())?;
    // The parser is lenient about padding; only the canonical spelling is accepted.
    if date.format("%Y-%m-%d").to_string() == *text { Ok(Some(date)) } else { Err(()) }
}

/// Whether a schedule can be followed: valid times and days, and an end date that exists.
pub fn is_valid(schedule: &DownloadSchedule) -> bool {
    schedule.is_valid() && end_date(schedule).is_ok()
}

/// Whether downloads may run at `now`, and until when or from when.
///
/// - The start is inclusive, the end exclusive.
/// - A window whose end is not after its start runs past midnight, and belongs to the day it
///   starts: the start days and end date both refer to that day, so the last window on the end
///   date runs to its end the next morning.
/// - A schedule that's off allows everything; an invalid one fails closed (nothing allowed).
/// - A start or end that falls in the hour the clocks skip happens the moment they jump; one that
///   falls in the hour they repeat happens the first time round.
pub fn window<Tz: TimeZone>(schedule: &DownloadSchedule, now: &DateTime<Tz>) -> ScheduleWindow<Tz> {
    let closed =
        |next_start, expired| ScheduleWindow { allowed: false, ends_at: None, next_start, expired };
    if !schedule.enabled {
        return ScheduleWindow { allowed: true, ends_at: None, next_start: None, expired: false };
    }
    let (true, Ok(last_day)) = (schedule.is_valid(), end_date(schedule)) else {
        return closed(None, false);
    };

    let tz = now.timezone();
    let today = now.date_naive();
    let overnight = schedule.end_minute <= schedule.start_minute;
    let mut next_start = None;
    // Yesterday's window may still be running; a week ahead covers every start day.
    for offset in -1..=7i64 {
        let Some(day) = today.checked_add_signed(TimeDelta::days(offset)) else { continue };
        if !schedule.days.contains(&(day.weekday().num_days_from_sunday() as u8)) {
            continue;
        }
        if last_day.is_some_and(|last| day > last) {
            continue;
        }
        let end_day = if overnight { day.checked_add_days(Days::new(1)) } else { Some(day) };
        let (Some(start), Some(end)) = (
            at_local_minute(&tz, day, schedule.start_minute),
            end_day.and_then(|end_day| at_local_minute(&tz, end_day, schedule.end_minute)),
        ) else {
            continue;
        };
        // Both ends inside one DST gap resolve to the same instant: that window never opens, so
        // it's no start to wait for.
        if end <= start {
            continue;
        }
        if *now >= start && *now < end {
            return ScheduleWindow {
                allowed: true,
                ends_at: Some(end),
                next_start: None,
                expired: false,
            };
        }
        if start > *now && next_start.is_none() {
            next_start = Some(start);
        }
    }
    let expired = next_start.is_none();
    closed(next_start, expired)
}

/// `date` at `minute` past local midnight. See [`at_local`].
fn at_local_minute<Tz: TimeZone>(tz: &Tz, date: NaiveDate, minute: u16) -> Option<DateTime<Tz>> {
    let time = NaiveTime::from_hms_opt(u32::from(minute / 60), u32::from(minute % 60), 0)?;
    at_local(tz, date.and_time(time))
}

/// The most local times a gap can skip: whole days have been skipped (Samoa, 2011), so two.
const MAX_GAP_MINUTES: i64 = 2 * 24 * 60;

/// The moment the wall clock in `tz` reads `local`.
///
/// - Read twice (clocks going back): the first time.
/// - Never read (clocks jumping forward over it): the moment of the jump — the clock passes 2:30
///   the instant it goes from 2:00 to 3:00, so a window set for 2:30 starts then. Found as the
///   first local minute after `local` that exists.
///
/// `None` only if the zone skips more than [`MAX_GAP_MINUTES`].
pub fn at_local<Tz: TimeZone>(tz: &Tz, local: NaiveDateTime) -> Option<DateTime<Tz>> {
    if let Some(at) = tz.from_local_datetime(&local).earliest() {
        return Some(at);
    }
    let minute_start = local.with_second(0)?.with_nanosecond(0)?;
    (1..=MAX_GAP_MINUTES).find_map(|minutes| {
        let candidate = minute_start.checked_add_signed(TimeDelta::minutes(minutes))?;
        tz.from_local_datetime(&candidate).earliest()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_tz::NewYork2026;
    use chrono::FixedOffset;
    use proptest::prelude::*;

    fn tz() -> FixedOffset {
        FixedOffset::east_opt(2 * 3600).expect("valid offset")
    }

    /// October 2026 in a fixed zone. The 9th is a Friday.
    fn at(day: u32, hour: u32, minute: u32) -> DateTime<FixedOffset> {
        tz().with_ymd_and_hms(2026, 10, day, hour, minute, 0).single().expect("exists")
    }

    fn enabled() -> DownloadSchedule {
        DownloadSchedule {
            enabled: true,
            start_minute: 0,
            end_minute: 7 * 60,
            days: (0..7).collect(),
            end_date: None,
        }
    }

    #[test]
    fn the_start_is_inclusive_and_the_end_exclusive() {
        let schedule = enabled();
        assert!(window(&schedule, &at(10, 0, 0)).allowed);
        assert_eq!(window(&schedule, &at(10, 0, 0)).ends_at, Some(at(10, 7, 0)));
        assert!(window(&schedule, &at(10, 6, 59)).allowed);
        let after = window(&schedule, &at(10, 7, 0));
        assert!(!after.allowed);
        assert_eq!(after.next_start, Some(at(11, 0, 0)));
        assert!(!after.expired);
    }

    #[test]
    fn a_schedule_that_is_off_allows_everything() {
        let off = DownloadSchedule { enabled: false, ..enabled() };
        let window = window(&off, &at(10, 12, 0));
        assert!(window.allowed && window.ends_at.is_none() && window.next_start.is_none());
    }

    #[test]
    fn overnight_windows_belong_to_their_starting_weekday() {
        let friday = DownloadSchedule { start_minute: 22 * 60, days: vec![5], ..enabled() };
        assert!(window(&friday, &at(9, 22, 0)).allowed);
        assert!(window(&friday, &at(10, 6, 0)).allowed);
        assert_eq!(window(&friday, &at(10, 6, 0)).ends_at, Some(at(10, 7, 0)));
        assert!(!window(&friday, &at(10, 7, 0)).allowed);
        assert!(!window(&friday, &at(9, 21, 59)).allowed);
        assert_eq!(window(&friday, &at(10, 22, 0)).next_start, Some(at(16, 22, 0)));
    }

    #[test]
    fn the_final_overnight_window_finishes_then_the_schedule_expires() {
        let last = DownloadSchedule {
            start_minute: 22 * 60,
            end_date: Some("2026-10-09".into()),
            ..enabled()
        };
        assert!(window(&last, &at(10, 6, 0)).allowed);
        let ended = window(&last, &at(10, 7, 0));
        assert_eq!(
            ended,
            ScheduleWindow { allowed: false, ends_at: None, next_start: None, expired: true }
        );
        assert!(!window(&last, &at(11, 22, 0)).allowed);
    }

    #[test]
    fn invalid_schedules_fail_closed() {
        let invalid = [
            DownloadSchedule { days: vec![], ..enabled() },
            DownloadSchedule { days: vec![7], ..enabled() },
            DownloadSchedule { end_minute: 1440, ..enabled() },
            DownloadSchedule { end_minute: 0, ..enabled() },
            DownloadSchedule { end_date: Some("2026-02-30".into()), ..enabled() },
            DownloadSchedule { end_date: Some("2026-2-3".into()), ..enabled() },
            DownloadSchedule { end_date: Some("bad".into()), ..enabled() },
        ];
        for schedule in invalid {
            assert!(!is_valid(&schedule), "{schedule:?}");
            let window = window(&schedule, &at(10, 0, 0));
            assert!(!window.allowed && !window.expired, "{schedule:?}");
        }
        assert!(is_valid(&DownloadSchedule { end_date: Some("2026-02-28".into()), ..enabled() }));
    }

    fn new_york(month: u32, day: u32, hour: u32, minute: u32) -> DateTime<NewYork2026> {
        let local = NaiveDate::from_ymd_opt(2026, month, day)
            .and_then(|d| d.and_hms_opt(hour, minute, 0))
            .expect("valid date");
        at_local(&NewYork2026, local).expect("exists")
    }

    #[test]
    fn wall_clock_windows_survive_daylight_saving_changes() {
        let schedule = enabled();
        // 8 March: clocks spring forward; 1 November: they fall back.
        for (month, day) in [(3, 8), (11, 1)] {
            let window_now = window(&schedule, &new_york(month, day, 6, 59));
            assert_eq!(window_now.ends_at, Some(new_york(month, day, 7, 0)));
            assert!(!window(&schedule, &new_york(month, day, 7, 0)).allowed);
        }
        // The window that night is 6 hours long in the spring and 8 in the autumn, by the clock.
        let spring = window(&schedule, &new_york(3, 8, 0, 0)).ends_at.expect("open");
        assert_eq!(spring - new_york(3, 8, 0, 0), TimeDelta::hours(6));
        let autumn = window(&schedule, &new_york(11, 1, 0, 0)).ends_at.expect("open");
        assert_eq!(autumn - new_york(11, 1, 0, 0), TimeDelta::hours(8));
    }

    #[test]
    fn a_time_the_clocks_skip_happens_the_moment_they_jump() {
        // 2026-03-08: 2:00 EST becomes 3:00 EDT, so 2:30 never happens that day.
        let early = DownloadSchedule { start_minute: 2 * 60 + 30, ..enabled() };
        let jump = new_york(3, 8, 3, 0);
        assert_eq!(window(&early, &new_york(3, 8, 1, 59)).next_start, Some(jump));
        assert!(window(&early, &jump).allowed);
        assert!(window(&early, &new_york(3, 8, 3, 15)).allowed);
        assert!(!window(&early, &(jump - TimeDelta::seconds(1))).allowed);

        let overnight =
            DownloadSchedule { start_minute: 22 * 60, end_minute: 2 * 60 + 30, ..enabled() };
        assert_eq!(window(&overnight, &new_york(3, 7, 23, 0)).ends_at, Some(jump));
        assert!(!window(&overnight, &new_york(3, 8, 3, 15)).allowed);
    }

    #[test]
    fn a_window_lost_in_a_dst_gap_is_skipped() {
        // 2:10–2:50 on 2026-03-08 never happens (2:00 jumps to 3:00): both ends land on the jump,
        // so that night's window must not be advertised as the next start.
        let lost =
            DownloadSchedule { start_minute: 2 * 60 + 10, end_minute: 2 * 60 + 50, ..enabled() };
        let next = window(&lost, &new_york(3, 8, 1, 0)).next_start.expect("a later night");
        assert_eq!(next, new_york(3, 9, 2, 10));
        assert!(!window(&lost, &new_york(3, 8, 3, 0)).allowed);
    }

    #[test]
    fn a_time_the_clocks_repeat_happens_the_first_time_round() {
        // 2026-11-01: 2:00 EDT becomes 1:00 EST, so 1:30 happens twice.
        let early = DownloadSchedule { start_minute: 60 + 30, ..enabled() };
        let first = window(&early, &new_york(11, 1, 0, 0)).next_start.expect("today");
        assert_eq!(first.naive_local().time(), NaiveTime::from_hms_opt(1, 30, 0).expect("time"));
        assert_eq!(first.offset().to_string(), "-04:00");
    }

    #[test]
    fn at_local_resolves_gaps_overlaps_and_ordinary_times() {
        let local = |h, m| {
            NaiveDate::from_ymd_opt(2026, 3, 8).and_then(|d| d.and_hms_opt(h, m, 0)).expect("ok")
        };
        let jump = at_local(&NewYork2026, local(3, 0)).expect("exists");
        assert_eq!(at_local(&NewYork2026, local(2, 0)), Some(jump));
        assert_eq!(at_local(&NewYork2026, local(2, 59)), Some(jump));
        assert_eq!(at_local(&tz(), local(2, 30)).map(|t| t.naive_local()), Some(local(2, 30)));
    }

    fn schedule_strategy() -> impl Strategy<Value = DownloadSchedule> {
        (0u16..1440, 0u16..1440, proptest::collection::btree_set(0u8..7, 1..=7))
            .prop_filter("start and end differ", |(start, end, _)| start != end)
            .prop_map(|(start_minute, end_minute, days)| DownloadSchedule {
                enabled: true,
                start_minute,
                end_minute,
                days: days.into_iter().collect(),
                end_date: None,
            })
    }

    proptest! {
        /// A window's boundaries agree with what the schedule says either side of them, and
        /// nothing is allowed between now and the next start.
        #[test]
        fn window_boundaries_are_consistent(
            schedule in schedule_strategy(),
            minutes in 0i64..(60 * 24 * 21),
            use_dst in any::<bool>(),
        ) {
            if use_dst {
                let now = new_york(3, 1, 0, 0) + TimeDelta::minutes(minutes);
                check(&schedule, &now)?;
            } else {
                let now = at(1, 0, 0) + TimeDelta::minutes(minutes);
                check(&schedule, &now)?;
            }
        }
    }

    fn check<Tz: TimeZone>(
        schedule: &DownloadSchedule,
        now: &DateTime<Tz>,
    ) -> Result<(), TestCaseError> {
        let found = window(schedule, now);
        let minute = TimeDelta::minutes(1);
        if found.allowed {
            let end = found.ends_at.clone().expect("an open window ends");
            prop_assert!(end > *now && end.clone() - now.clone() <= TimeDelta::hours(25));
            prop_assert!(found.next_start.is_none());
            prop_assert!(window(schedule, &(end.clone() - minute)).allowed);
            // Over at its end — unless a DST jump folds the next start onto it, which is then a
            // different window.
            let after = window(schedule, &end);
            prop_assert!(!after.allowed || after.ends_at != Some(end));
        } else {
            let next = found.next_start.clone().expect("a weekly schedule always comes round");
            prop_assert!(next > *now && next.clone() - now.clone() <= TimeDelta::days(8));
            prop_assert!(window(schedule, &next).allowed);
            prop_assert!(
                !window(schedule, &(next.clone() - minute)).allowed || next.clone() - minute < *now
            );
            // Spot checks between now and the start.
            let gap = next.clone() - now.clone();
            for k in 1..8 {
                let between = now.clone() + gap / 8 * k;
                prop_assert!(!window(schedule, &between).allowed);
            }
        }
        Ok(())
    }
}
