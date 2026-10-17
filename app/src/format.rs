//! Every number the UI shows, as text. Kept apart from the view models so the rules (units,
//! rounding, how a duration reads) live in one place and are tested once.

use chrono::{DateTime, Datelike, Local, TimeZone};
use sonic_core::{Millis, SpeedUnit};

const KIB: f64 = 1024.0;

/// Three significant digits, so a column of sizes keeps its width as values grow: 9.87, 98.7, 987.
fn three_digits(value: f64) -> String {
    if value < 10.0 {
        format!("{value:.2}")
    } else if value < 100.0 {
        format!("{value:.1}")
    } else {
        format!("{value:.0}")
    }
}

/// A size in binary units with Windows' labels (1 KB = 1024 bytes), as Explorer shows them.
pub fn bytes(n: u64) -> String {
    let (value, unit) = scale(n as f64, KIB, &["B", "KB", "MB", "GB", "TB"]);
    if unit == "B" { format!("{n} B") } else { format!("{} {unit}", three_digits(value)) }
}

/// Scales `value` down by `step` until it is under 1000 (not `step`: "1010 KB" reads worse than
/// "0.99 MB"), returning it with the unit it landed on.
fn scale(mut value: f64, step: f64, units: &[&'static str]) -> (f64, &'static str) {
    let mut unit = 0;
    while value >= 1000.0 && unit + 1 < units.len() {
        value /= step;
        unit += 1;
    }
    (value, units[unit])
}

/// A speed split into number and unit, for places that set them in different sizes.
pub fn speed_parts(bytes_per_sec: f64, unit: SpeedUnit) -> (String, String) {
    let bytes_per_sec = if bytes_per_sec.is_finite() { bytes_per_sec.max(0.0) } else { 0.0 };
    match unit {
        SpeedUnit::Bytes => {
            // Under 1 KB/s is noise from a stalled stream; show it as 0 rather than "312 B/s".
            if bytes_per_sec < KIB {
                return ("0".into(), "KB/s".into());
            }
            let (value, unit) = scale(bytes_per_sec / KIB, KIB, &["KB/s", "MB/s", "GB/s"]);
            (three_digits(value), unit.into())
        }
        SpeedUnit::Bits => {
            // Network speeds count in decimal bits, as ISPs and speed tests do.
            let bits = bytes_per_sec * 8.0;
            if bits < 1000.0 {
                return ("0".into(), "kbps".into());
            }
            let (value, unit) = scale(bits / 1000.0, 1000.0, &["kbps", "Mbps", "Gbps"]);
            (three_digits(value), unit.into())
        }
    }
}

pub fn speed(bytes_per_sec: f64, unit: SpeedUnit) -> String {
    let (value, unit) = speed_parts(bytes_per_sec, unit);
    format!("{value} {unit}")
}

/// The unit a typed-in speed is in.
pub fn speed_unit_label(unit: SpeedUnit) -> &'static str {
    match unit {
        SpeedUnit::Bytes => "MB/s",
        SpeedUnit::Bits => "Mbps",
    }
}

/// A limit as the settings field shows it: in MB/s or Mbps, without trailing zeros.
pub fn speed_field(bytes_per_sec: u64, unit: SpeedUnit) -> String {
    let value = match unit {
        SpeedUnit::Bytes => bytes_per_sec as f64 / (KIB * KIB),
        SpeedUnit::Bits => bytes_per_sec as f64 * 8.0 / 1e6,
    };
    trim_number(value)
}

/// What the user typed into a speed field, as bytes a second. Empty means no limit (`Ok(None)`).
pub fn parse_speed_field(text: &str, unit: SpeedUnit) -> Result<Option<u64>, String> {
    let Some(value) = parse_positive(text)? else { return Ok(None) };
    let bytes = match unit {
        SpeedUnit::Bytes => value * KIB * KIB,
        SpeedUnit::Bits => value * 1e6 / 8.0,
    };
    Ok(Some(bytes.round() as u64))
}

/// A data allowance as the settings field shows it, in GB.
pub fn gb_field(bytes: u64) -> String {
    trim_number(bytes as f64 / (KIB * KIB * KIB))
}

pub fn parse_gb_field(text: &str) -> Result<Option<u64>, String> {
    Ok(parse_positive(text)?.map(|gb| (gb * KIB * KIB * KIB).round() as u64))
}

fn parse_positive(text: &str) -> Result<Option<f64>, String> {
    let text = text.trim().replace(',', ".");
    if text.is_empty() {
        return Ok(None);
    }
    match text.parse::<f64>() {
        Ok(value) if value.is_finite() && value > 0.0 => Ok(Some(value)),
        _ => Err(format!("\u{201c}{text}\u{201d} isn't a number above zero")),
    }
}

fn trim_number(value: f64) -> String {
    let text = format!("{value:.2}");
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

/// A span of time in its two largest units: "45s", "1m 0s", "2h 5m", "3d 4h".
pub fn duration(secs: f64) -> String {
    let secs = if secs.is_finite() { secs.max(0.0).round() as u64 } else { 0 };
    let (d, h, m, s) = (secs / 86_400, secs / 3600 % 24, secs / 60 % 60, secs % 60);
    if d > 0 {
        format!("{d}d {h}h")
    } else if h > 0 {
        format!("{h}h {m}m")
    } else if m > 0 {
        format!("{m}m {s}s")
    } else {
        format!("{s}s")
    }
}

/// Time left, or nothing when there's no telling.
pub fn eta(secs: Option<f64>) -> String {
    match secs {
        Some(secs) if secs.is_finite() && secs >= 0.0 => {
            if secs < 1.0 {
                "Almost done".into()
            } else {
                format!("{} left", duration(secs))
            }
        }
        _ => String::new(),
    }
}

/// A whole percentage that never claims 100% before the end: 99.7% shows as 99%.
pub fn percent(fraction: f64) -> String {
    let fraction = if fraction.is_finite() { fraction.clamp(0.0, 1.0) } else { 0.0 };
    format!("{}%", (fraction * 100.0).floor() as u32)
}

/// A count with thousands separators: 1,460.
pub fn count(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// When something happened, relative to `now` in local time: "14:02", "Yesterday 09:15", "12 Oct",
/// "12 Oct 2025".
pub fn when(at: Millis, now: Millis) -> String {
    let (Some(at), Some(now)) =
        (Local.timestamp_millis_opt(at).single(), Local.timestamp_millis_opt(now).single())
    else {
        return String::new();
    };
    when_in(at, now)
}

fn when_in<Tz: TimeZone>(at: DateTime<Tz>, now: DateTime<Tz>) -> String
where
    Tz::Offset: std::fmt::Display,
{
    let days = (now.date_naive() - at.date_naive()).num_days();
    match days {
        0 => at.format("Today %H:%M").to_string(),
        1 => at.format("Yesterday %H:%M").to_string(),
        _ if at.year() == now.year() => at.format("%-d %b").to_string(),
        _ => at.format("%-d %b %Y").to_string(),
    }
}

/// Minutes since midnight as "HH:MM".
pub fn hhmm(minutes: u16) -> String {
    format!("{:02}:{:02}", minutes / 60 % 24, minutes % 60)
}

/// "7:30", "07:30" or "730" as minutes since midnight.
pub fn parse_hhmm(text: &str) -> Option<u16> {
    let text = text.trim();
    let (h, m) = match text.split_once(':') {
        Some((h, m)) => (h.parse::<u16>().ok()?, m.parse::<u16>().ok()?),
        None if text.len() >= 3 && text.len() <= 4 => {
            let (h, m) = text.split_at(text.len() - 2);
            (h.parse().ok()?, m.parse().ok()?)
        }
        None => (text.parse().ok()?, 0),
    };
    (h < 24 && m < 60).then_some(h * 60 + m)
}

/// The extension shown on a file's tile: "ISO", "ZIP"; "FILE" when there is none.
pub fn ext_badge(file_name: &str) -> String {
    match file_name.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() && !ext.is_empty() && ext.len() <= 4 => {
            ext.to_ascii_uppercase()
        }
        _ => "FILE".into(),
    }
}

/// The host a link points at, for "from example.com".
pub fn host(url: &str) -> String {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    rest.split(['/', '?', '#']).next().unwrap_or("").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn sizes_use_binary_units_and_three_digits() {
        assert_eq!(bytes(0), "0 B");
        assert_eq!(bytes(999), "999 B");
        assert_eq!(bytes(1000), "0.98 KB");
        assert_eq!(bytes(1536), "1.50 KB");
        assert_eq!(bytes(10 * 1024 * 1024), "10.0 MB");
        assert_eq!(bytes(5_700_000_000), "5.31 GB");
        assert_eq!(bytes(300 * 1024 * 1024 * 1024), "300 GB");
    }

    #[test]
    fn speeds_in_bytes_and_bits() {
        assert_eq!(speed(12.3 * 1024.0 * 1024.0, SpeedUnit::Bytes), "12.3 MB/s");
        assert_eq!(speed(500.0, SpeedUnit::Bytes), "0 KB/s");
        assert_eq!(speed(850.0 * 1024.0, SpeedUnit::Bytes), "850 KB/s");
        // 12.5 MB/s (decimal) is 100 Mbps.
        assert_eq!(speed(12_500_000.0, SpeedUnit::Bits), "100 Mbps");
        assert_eq!(speed(150_000_000.0, SpeedUnit::Bits), "1.20 Gbps");
        assert_eq!(speed(f64::NAN, SpeedUnit::Bits), "0 kbps");
        assert_eq!(
            speed_parts(2.0 * 1024.0 * 1024.0, SpeedUnit::Bytes),
            ("2.00".into(), "MB/s".into())
        );
    }

    #[test]
    fn durations_read_in_two_units() {
        assert_eq!(duration(0.0), "0s");
        assert_eq!(duration(45.0), "45s");
        assert_eq!(duration(60.0), "1m 0s");
        assert_eq!(duration(130.4), "2m 10s");
        assert_eq!(duration(2.0 * 3600.0 + 5.0 * 60.0 + 7.0), "2h 5m");
        assert_eq!(duration(3.0 * 86_400.0 + 4.0 * 3600.0), "3d 4h");
        assert_eq!(duration(-5.0), "0s");
    }

    #[test]
    fn eta_reads_naturally() {
        assert_eq!(eta(None), "");
        assert_eq!(eta(Some(f64::INFINITY)), "");
        assert_eq!(eta(Some(0.4)), "Almost done");
        assert_eq!(eta(Some(61.0)), "1m 1s left");
    }

    #[test]
    fn percent_never_rounds_up_to_done() {
        assert_eq!(percent(0.997), "99%");
        assert_eq!(percent(1.0), "100%");
        assert_eq!(percent(-1.0), "0%");
    }

    #[test]
    fn counts_have_separators() {
        assert_eq!(count(7), "7");
        assert_eq!(count(1460), "1,460");
        assert_eq!(count(1_234_567), "1,234,567");
    }

    #[test]
    fn speed_fields_round_trip() {
        assert_eq!(parse_speed_field("", SpeedUnit::Bytes), Ok(None));
        assert_eq!(parse_speed_field("2", SpeedUnit::Bytes), Ok(Some(2 * 1024 * 1024)));
        assert_eq!(parse_speed_field("100", SpeedUnit::Bits), Ok(Some(12_500_000)));
        assert_eq!(parse_speed_field("1,5", SpeedUnit::Bytes), Ok(Some(1_572_864)));
        assert!(parse_speed_field("fast", SpeedUnit::Bytes).is_err());
        assert!(parse_speed_field("-3", SpeedUnit::Bytes).is_err());
        assert_eq!(speed_field(2 * 1024 * 1024, SpeedUnit::Bytes), "2");
        assert_eq!(speed_field(12_500_000, SpeedUnit::Bits), "100");
        assert_eq!(parse_gb_field("2"), Ok(Some(2 * 1024 * 1024 * 1024)));
        assert_eq!(gb_field(1024 * 1024 * 1024 / 2), "0.5");
    }

    #[test]
    fn times_of_day() {
        assert_eq!(hhmm(60), "01:00");
        assert_eq!(hhmm(23 * 60 + 59), "23:59");
        assert_eq!(parse_hhmm("7:30"), Some(450));
        assert_eq!(parse_hhmm("0730"), Some(450));
        assert_eq!(parse_hhmm("23"), Some(1380));
        assert_eq!(parse_hhmm("24:00"), None);
        assert_eq!(parse_hhmm("7:75"), None);
        assert_eq!(parse_hhmm("soon"), None);
    }

    #[test]
    fn when_is_relative_to_now() {
        let now = Utc.with_ymd_and_hms(2026, 10, 17, 15, 0, 0).unwrap();
        let at = |d, h| Utc.with_ymd_and_hms(2026, 10, d, h, 2, 0).unwrap();
        assert_eq!(when_in(at(17, 14), now), "Today 14:02");
        assert_eq!(when_in(at(16, 9), now), "Yesterday 09:02");
        assert_eq!(when_in(at(2, 9), now), "2 Oct");
        let last_year = Utc.with_ymd_and_hms(2025, 3, 4, 0, 0, 0).unwrap();
        assert_eq!(when_in(last_year, now), "4 Mar 2025");
    }

    #[test]
    fn badges_and_hosts() {
        assert_eq!(ext_badge("ubuntu.iso"), "ISO");
        assert_eq!(ext_badge("archive.tar.zst"), "ZST");
        assert_eq!(ext_badge("README"), "FILE");
        assert_eq!(ext_badge(".bashrc"), "FILE");
        assert_eq!(ext_badge("weird.extension"), "FILE");
        assert_eq!(host("https://cdn.example.com/a/b.zip?x=1"), "cdn.example.com");
        assert_eq!(host("example.org"), "example.org");
    }
}
