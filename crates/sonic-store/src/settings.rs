//! The user's settings, one JSON file.
//!
//! The file may be hand-edited or written by another version, so nothing in it is trusted past
//! "this is JSON": every field is read on its own, and one bad value loses only that value (a
//! whole-struct parse would throw every setting away over one typo).

use std::io;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use sonic_core::{
    AppSettings, DEFAULT_SLOW_MODE_SPEED, DOWNLOADS_AT_ONCE_MAX, DOWNLOADS_AT_ONCE_MIN,
    DownloadSchedule, NetworkPreference,
};

use crate::Store;

impl Store {
    /// The saved settings; defaults for a first run, a missing file or a corrupt one.
    pub async fn load_settings(&self) -> AppSettings {
        let raw: Option<Value> = self.read_json(&self.settings_path()).await;
        raw.map(|raw| settings_from(&raw)).unwrap_or_default()
    }

    /// Saves `settings` after sanitising them, and answers with what was saved.
    pub async fn save_settings(&self, settings: &AppSettings) -> io::Result<AppSettings> {
        let settings = sanitize_settings(settings.clone());
        self.write_json(&self.settings_path(), &settings).await?;
        Ok(settings)
    }
}

/// Settings read from untrusted JSON, field by field.
fn settings_from(raw: &Value) -> AppSettings {
    let Value::Object(fields) = raw else { return AppSettings::default() };
    let mut fields = fields.clone();
    // Nested values that get finer treatment than "keep or drop the whole field".
    if let Some(Value::Object(preferences)) = fields.get("network_preferences") {
        let kept: Map<String, Value> = preferences
            .iter()
            .filter_map(|(id, raw)| {
                let preference: NetworkPreference = lenient(raw);
                Some((id.clone(), serde_json::to_value(preference).ok()?))
            })
            .collect();
        fields.insert("network_preferences".into(), Value::Object(kept));
    }
    if let Some(raw) = fields.get("schedule") {
        let schedule = schedule_from(raw);
        if let Ok(value) = serde_json::to_value(schedule) {
            fields.insert("schedule".into(), value);
        }
    }
    sanitize_settings(lenient(&Value::Object(fields)))
}

/// A schedule is taken whole or not at all: half of one (say, its days reset to every day) could
/// start downloads at times the user never chose.
fn schedule_from(raw: &Value) -> DownloadSchedule {
    match serde_json::from_value::<DownloadSchedule>(raw.clone()) {
        Ok(schedule) => schedule,
        Err(_) => invalid_schedule(raw.get("enabled").and_then(Value::as_bool).unwrap_or(false)),
    }
}

/// What an unusable schedule becomes: as Plexo does, it keeps whether it was on, with no days.
/// While on, enrolled downloads wait (nothing runs at a time the user didn't pick) and the UI
/// shows a schedule to fix; off, it changes nothing.
fn invalid_schedule(enabled: bool) -> DownloadSchedule {
    DownloadSchedule { enabled, days: Vec::new(), ..DownloadSchedule::default() }
}

/// Puts every value in range. Idempotent, so it is safe on settings already sanitised (and the
/// manager can run what the UI sends through it too).
pub fn sanitize_settings(mut settings: AppSettings) -> AppSettings {
    // Clamped rather than reset: a count saved under an older, higher limit keeps the most allowed.
    settings.downloads_at_once =
        settings.downloads_at_once.clamp(DOWNLOADS_AT_ONCE_MIN, DOWNLOADS_AT_ONCE_MAX);
    settings.speed_limit = settings.speed_limit.filter(|&limit| limit > 0);
    if settings.slow_mode_speed == 0 {
        settings.slow_mode_speed = DEFAULT_SLOW_MODE_SPEED;
    }
    settings.destination_dir = settings.destination_dir.filter(|dir| dir.is_absolute());

    let schedule = &mut settings.schedule;
    if !schedule.is_valid() {
        *schedule = invalid_schedule(schedule.enabled);
    }
    schedule.end_date = schedule.end_date.take().filter(|date| is_date(date));

    for preference in settings.network_preferences.values_mut() {
        preference.speed_limit = preference.speed_limit.filter(|&limit| limit > 0);
        preference.data_limit = preference.data_limit.filter(|&limit| limit > 0);
    }
    // A network with nothing customised is dropped, so resetting one removes it from the file.
    settings
        .network_preferences
        .retain(|_, preference| *preference != NetworkPreference::default());
    settings
}

/// `YYYY-MM-DD` naming a day that exists.
fn is_date(date: &str) -> bool {
    let parts: Vec<&str> = date.split('-').collect();
    let [year, month, day] = parts.as_slice() else { return false };
    let digits =
        |part: &str, len: usize| part.len() == len && part.bytes().all(|b| b.is_ascii_digit());
    if !(digits(year, 4) && digits(month, 2) && digits(day, 2)) {
        return false;
    }
    // The digit checks above make these parses infallible.
    let (year, month, day): (u32, u32, u32) =
        (year.parse().unwrap_or(0), month.parse().unwrap_or(0), day.parse().unwrap_or(0));
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return false,
    };
    (1..=days_in_month).contains(&day)
}

/// `T` from untrusted JSON one field at a time: each field of `raw` is tried on top of the ones
/// already accepted, and kept only if the whole still parses. Unknown fields are dropped.
fn lenient<T: Default + Serialize + DeserializeOwned>(raw: &Value) -> T {
    let Value::Object(fields) = raw else { return T::default() };
    let Ok(Value::Object(mut accepted)) = serde_json::to_value(T::default()) else {
        return T::default();
    };
    for (key, value) in fields {
        if !accepted.contains_key(key) {
            continue;
        }
        let mut candidate = accepted.clone();
        candidate.insert(key.clone(), value.clone());
        if serde_json::from_value::<T>(Value::Object(candidate.clone())).is_ok() {
            accepted = candidate;
        } else {
            tracing::warn!(field = %key, "ignored an unusable saved setting");
        }
    }
    serde_json::from_value(Value::Object(accepted)).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sonic_core::{DOWNLOADS_AT_ONCE_DEFAULT, NetworkId, SpeedUnit, Theme};
    use std::path::PathBuf;

    fn absolute_dir() -> PathBuf {
        if cfg!(windows) { PathBuf::from(r"C:\Downloads") } else { PathBuf::from("/downloads") }
    }

    #[tokio::test]
    async fn missing_and_corrupt_load_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::at(dir.path());
        assert_eq!(store.load_settings().await, AppSettings::default());
        std::fs::write(dir.path().join("settings.json"), "][").unwrap();
        assert_eq!(store.load_settings().await, AppSettings::default());
        std::fs::write(dir.path().join("settings.json"), "[1,2]").unwrap();
        assert_eq!(store.load_settings().await, AppSettings::default());
    }

    #[tokio::test]
    async fn round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::at(dir.path());
        let mut settings = AppSettings {
            theme: Theme::Dark,
            destination_dir: Some(absolute_dir()),
            downloads_at_once: 3,
            speed_limit: Some(1000),
            speed_unit: SpeedUnit::Bits,
            ..AppSettings::default()
        };
        settings.network_preferences.insert(
            NetworkId::new("{A}"),
            NetworkPreference { custom_name: Some("Phone".into()), ..Default::default() },
        );
        let saved = store.save_settings(&settings).await.unwrap();
        assert_eq!(saved, settings);
        assert_eq!(store.load_settings().await, settings);
    }

    #[test]
    fn one_bad_value_loses_only_that_value() {
        let raw = json!({
            "theme": "dark",
            "downloads_at_once": "three",
            "speed_unit": "furlongs",
            "slow_mode": true,
            "unknown": 1,
        });
        let settings = settings_from(&raw);
        assert_eq!(settings.theme, Theme::Dark);
        assert!(settings.slow_mode);
        assert_eq!(settings.downloads_at_once, DOWNLOADS_AT_ONCE_DEFAULT);
        assert_eq!(settings.speed_unit, SpeedUnit::Bytes);
    }

    #[test]
    fn sanitisation_table() {
        // (field JSON, check on the result)
        type Check = fn(&AppSettings) -> bool;
        let cases: &[(Value, Check)] = &[
            (json!({"downloads_at_once": 0}), |s| s.downloads_at_once == DOWNLOADS_AT_ONCE_MIN),
            (json!({"downloads_at_once": 99}), |s| s.downloads_at_once == DOWNLOADS_AT_ONCE_MAX),
            (json!({"downloads_at_once": -1}), |s| {
                s.downloads_at_once == DOWNLOADS_AT_ONCE_DEFAULT
            }),
            (json!({"downloads_at_once": 3}), |s| s.downloads_at_once == 3),
            (json!({"speed_limit": 0}), |s| s.speed_limit.is_none()),
            (json!({"speed_limit": -5}), |s| s.speed_limit.is_none()),
            (json!({"speed_limit": 500}), |s| s.speed_limit == Some(500)),
            (json!({"slow_mode_speed": 0}), |s| s.slow_mode_speed == DEFAULT_SLOW_MODE_SPEED),
            (json!({"destination_dir": "relative/dir"}), |s| s.destination_dir.is_none()),
            (json!({"destination_dir": 7}), |s| s.destination_dir.is_none()),
            (json!({"theme": "neon"}), |s| s.theme == Theme::System),
            (json!({"close_to_tray": "yes"}), |s| !s.close_to_tray),
            (json!({"system_mode": "no"}), |s| s.system_mode),
            (json!({"system_mode": false}), |s| !s.system_mode),
        ];
        for (raw, check) in cases {
            assert!(check(&settings_from(raw)), "failed for {raw}");
        }
        let dir = absolute_dir();
        let settings = settings_from(&json!({ "destination_dir": dir }));
        assert_eq!(settings.destination_dir, Some(dir));
    }

    #[test]
    fn schedules() {
        let valid = json!({"enabled": true, "start_minute": 60, "end_minute": 120, "days": [1, 2],
            "end_date": "2026-02-28"});
        let schedule = settings_from(&json!({ "schedule": valid })).schedule;
        assert!(schedule.is_valid() && schedule.enabled);
        assert_eq!(schedule.days, [1, 2]);
        assert_eq!(schedule.end_date.as_deref(), Some("2026-02-28"));

        // Unusable: kept on, with no days, so nothing runs at a time the user didn't choose.
        for bad in [
            json!({"enabled": true, "start_minute": 60, "end_minute": 60, "days": [1]}),
            json!({"enabled": true, "start_minute": 60, "end_minute": 120, "days": [9]}),
            json!({"enabled": true, "start_minute": 60, "end_minute": 120, "days": "mon"}),
            json!({"enabled": true, "start_minute": 5000, "end_minute": 120, "days": [1]}),
        ] {
            let schedule = settings_from(&json!({ "schedule": bad })).schedule;
            assert!(schedule.enabled && schedule.days.is_empty(), "{bad}");
        }
        let off = settings_from(&json!({"schedule": {"enabled": false, "days": 3}})).schedule;
        assert!(!off.enabled && off.days.is_empty());

        // Only a bad end date is dropped, the rest kept.
        for date in ["2026-02-29", "2026-13-01", "26-01-01", "2026-1-01", "tomorrow"] {
            let raw = json!({"enabled": true, "start_minute": 60, "end_minute": 120, "days": [1],
                "end_date": date});
            let schedule = settings_from(&json!({ "schedule": raw })).schedule;
            assert!(schedule.is_valid() && schedule.end_date.is_none(), "{date}");
        }
        assert!(is_date("2024-02-29"));
        // Sanitising is idempotent, including for an unusable schedule.
        let settings = settings_from(&json!({"schedule": {"enabled": true, "days": []}}));
        assert_eq!(sanitize_settings(settings.clone()), settings);
    }

    #[test]
    fn network_preferences_entry_by_entry() {
        let raw = json!({"network_preferences": {
            "{A}": {"custom_name": "Phone", "speed_limit": 0, "data_limit": "lots",
                    "data_period": "week"},
            "{B}": "garbage",
            "{C}": {"speed_limit": 0},
            "{D}": {"off": true, "data_limit": 1000},
        }});
        let preferences = settings_from(&raw).network_preferences;
        let a = &preferences[&NetworkId::new("{A}")];
        assert_eq!(a.custom_name.as_deref(), Some("Phone"));
        assert_eq!(a.speed_limit, None);
        assert_eq!(a.data_limit, None);
        assert_eq!(a.data_period, sonic_core::DataPeriod::Week);
        // Nothing left customised: dropped.
        assert!(!preferences.contains_key(&NetworkId::new("{B}")));
        assert!(!preferences.contains_key(&NetworkId::new("{C}")));
        let d = &preferences[&NetworkId::new("{D}")];
        assert!(d.off && d.data_limit == Some(1000));
    }
}
