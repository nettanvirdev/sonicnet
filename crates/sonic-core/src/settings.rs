use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{DEFAULT_SLOW_MODE_SPEED, DOWNLOADS_AT_ONCE_DEFAULT, NetworkId};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Theme {
    /// Follow the OS.
    #[default]
    System,
    Light,
    Dark,
}

/// How speeds are shown everywhere; sizes stay in bytes either way.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpeedUnit {
    /// MB/s.
    #[default]
    Bytes,
    /// Mbps, as internet plans and speed tests count them.
    Bits,
}

/// The calendar period a data allowance counts over, in local time. Weeks start on Monday.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataPeriod {
    Day,
    Week,
    #[default]
    Month,
}

/// The user's customisations for one network, kept independently of any download.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct NetworkPreference {
    /// Replaces the OS's name for it in the UI.
    pub custom_name: Option<String>,
    /// One of the UI's palette swatch ids; `None` picks a default by kind.
    pub color_id: Option<String>,
    /// Left out of the last download started, so New download starts without it too.
    pub off: bool,
    /// Bytes a second all downloads together may take over it.
    pub speed_limit: Option<u64>,
    /// Bytes it may receive per `data_period`.
    pub data_limit: Option<u64>,
    pub data_period: DataPeriod,
}

/// A weekly window when downloads may run, in the computer's local time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DownloadSchedule {
    pub enabled: bool,
    /// Minutes since local midnight, `0..1440`. A window whose end is not after its start runs past
    /// midnight. Equal start and end are invalid.
    pub start_minute: u16,
    pub end_minute: u16,
    /// Days on which a window *starts*, Sunday = 0.
    pub days: Vec<u8>,
    /// The last local date on which a window may start (`YYYY-MM-DD`).
    pub end_date: Option<String>,
}

impl Default for DownloadSchedule {
    /// Overnight, every day: the common "download while I sleep" case.
    fn default() -> Self {
        Self {
            enabled: false,
            start_minute: 60,
            end_minute: 7 * 60,
            days: (0..7).collect(),
            end_date: None,
        }
    }
}

impl DownloadSchedule {
    /// Start and end differ, at least one day, every day and minute in range.
    pub fn is_valid(&self) -> bool {
        self.start_minute < 1440
            && self.end_minute < 1440
            && self.start_minute != self.end_minute
            && !self.days.is_empty()
            && self.days.iter().all(|&d| d < 7)
    }
}

/// Everything the user can set. Saved as one JSON file; a missing field takes its default, so a
/// file from an older version still loads.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AppSettings {
    pub theme: Theme,
    /// The last destination folder picked; `None` uses the OS Downloads folder.
    pub destination_dir: Option<PathBuf>,
    pub downloads_at_once: u32,
    /// Bytes a second every download together may take, over every network.
    pub speed_limit: Option<u64>,
    /// While on, `slow_mode_speed` stands in for `speed_limit`: a one-click limit for calls.
    pub slow_mode: bool,
    pub slow_mode_speed: u64,
    pub speed_unit: SpeedUnit,
    pub schedule: DownloadSchedule,
    pub network_preferences: BTreeMap<NetworkId, NetworkPreference>,
    /// Keep running in the tray when the window closes (so the browser bridge stays reachable).
    pub close_to_tray: bool,
    /// System mode: every app's connections spread over the networks through an elevated helper
    /// (docs/system-mode.md). On unless the user turns it off; a file without it reads as on.
    pub system_mode: bool,
}

impl Default for AppSettings {
    fn default() -> Self {
        Self {
            theme: Theme::System,
            destination_dir: None,
            downloads_at_once: DOWNLOADS_AT_ONCE_DEFAULT,
            speed_limit: None,
            slow_mode: false,
            slow_mode_speed: DEFAULT_SLOW_MODE_SPEED,
            speed_unit: SpeedUnit::Bytes,
            schedule: DownloadSchedule::default(),
            network_preferences: BTreeMap::new(),
            close_to_tray: false,
            system_mode: true,
        }
    }
}

impl AppSettings {
    /// The limit for all downloads together that applies now: slow mode's when it is on.
    pub fn effective_speed_limit(&self) -> Option<u64> {
        if self.slow_mode { Some(self.slow_mode_speed) } else { self.speed_limit }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_fields_take_defaults() {
        let settings: AppSettings = serde_json::from_str(r#"{"slow_mode": true}"#).unwrap();
        assert!(settings.slow_mode);
        assert_eq!(settings.downloads_at_once, DOWNLOADS_AT_ONCE_DEFAULT);
        assert_eq!(settings.effective_speed_limit(), Some(DEFAULT_SLOW_MODE_SPEED));
        assert!(settings.system_mode, "System mode defaults to on");
    }

    #[test]
    fn schedule_validation() {
        assert!(DownloadSchedule::default().is_valid());
        let same = DownloadSchedule { start_minute: 60, end_minute: 60, ..Default::default() };
        assert!(!same.is_valid());
        let no_days = DownloadSchedule { days: vec![], ..Default::default() };
        assert!(!no_days.is_valid());
    }
}
