//! A time zone with daylight saving, for tests: New York's rules for 2026, without a tz database.

use chrono::{FixedOffset, MappedLocalTime, NaiveDate, NaiveDateTime, TimeZone};

/// UTC−5, and UTC−4 from 2026-03-08 07:00 UTC (2:00 EST → 3:00 EDT) until 2026-11-01 06:00 UTC
/// (2:00 EDT → 1:00 EST).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NewYork2026;

fn est() -> FixedOffset {
    FixedOffset::west_opt(5 * 3600).expect("valid offset")
}

fn edt() -> FixedOffset {
    FixedOffset::west_opt(4 * 3600).expect("valid offset")
}

fn utc(month: u32, day: u32, hour: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, month, day)
        .and_then(|date| date.and_hms_opt(hour, 0, 0))
        .expect("valid date")
}

impl TimeZone for NewYork2026 {
    type Offset = FixedOffset;

    fn from_offset(_: &FixedOffset) -> Self {
        NewYork2026
    }

    fn offset_from_local_date(&self, local: &NaiveDate) -> MappedLocalTime<FixedOffset> {
        self.offset_from_local_datetime(&local.and_hms_opt(0, 0, 0).expect("midnight"))
    }

    fn offset_from_local_datetime(&self, local: &NaiveDateTime) -> MappedLocalTime<FixedOffset> {
        // Each offset that maps this local time back to itself. EDT first: it is the earlier
        // instant of a repeated time.
        let fits =
            |offset: FixedOffset| self.offset_from_utc_datetime(&(*local - offset)) == offset;
        match (fits(edt()), fits(est())) {
            (true, true) => MappedLocalTime::Ambiguous(edt(), est()),
            (true, false) => MappedLocalTime::Single(edt()),
            (false, true) => MappedLocalTime::Single(est()),
            (false, false) => MappedLocalTime::None,
        }
    }

    fn offset_from_utc_date(&self, utc: &NaiveDate) -> FixedOffset {
        self.offset_from_utc_datetime(&utc.and_hms_opt(0, 0, 0).expect("midnight"))
    }

    fn offset_from_utc_datetime(&self, at: &NaiveDateTime) -> FixedOffset {
        if (utc(3, 8, 7)..utc(11, 1, 6)).contains(at) { edt() } else { est() }
    }
}
