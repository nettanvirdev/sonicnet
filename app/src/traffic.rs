//! How much each network carries: System mode's traffic (from the helper's stats) and SonicNet's
//! own downloads (from the manager's usage counters and download speeds), live, for the last
//! minute, for this session and for today.
//!
//! Pure bookkeeping, fed by the controller; today's totals are saved by the app through
//! `Store::save_traffic` so a restart the same day keeps them.

use std::collections::{BTreeMap, VecDeque};

use serde::{Deserialize, Serialize};
use sonic_core::NetworkId;
use sonic_system::protocol::NetStats;

/// Seconds of history each network's chart shows.
pub const HISTORY: usize = sonic_core::SPEED_HISTORY_SECONDS;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct NetTraffic {
    /// System mode's last second, bytes a second.
    pub system_down_bps: f64,
    pub system_up_bps: f64,
    /// SonicNet's downloads on it right now, bytes a second.
    pub downloads_bps: f64,
    /// System mode's open TCP connections and UDP flows.
    pub tcp: u32,
    pub udp: u32,
    /// Bytes (System mode: both directions; downloads: received).
    pub session_system: u64,
    pub session_downloads: u64,
    pub today_system: u64,
    pub today_downloads: u64,
    /// One sample a second, oldest first, at most [`HISTORY`]: System mode down, downloads.
    pub system_history: VecDeque<f64>,
    pub downloads_history: VecDeque<f64>,
}

impl NetTraffic {
    pub fn down_bps(&self) -> f64 {
        self.system_down_bps + self.downloads_bps
    }

    pub fn session(&self) -> u64 {
        self.session_system + self.session_downloads
    }
}

/// What is saved between runs: today's totals.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Saved {
    /// `YYYY-MM-DD`, local.
    pub day: String,
    pub networks: BTreeMap<NetworkId, DayTotals>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DayTotals {
    pub system: u64,
    pub downloads: u64,
}

#[derive(Debug, Default)]
pub struct Traffic {
    pub nets: BTreeMap<NetworkId, NetTraffic>,
    day: String,
    /// The manager's last usage counters: downloads are the growth between two of them.
    usage_seen: Option<BTreeMap<NetworkId, u64>>,
    /// The helper's last running totals (down + up) per network. A new helper starts again from
    /// zero, which shows as a counter that went down (see [`grown`]).
    system_seen: BTreeMap<NetworkId, u64>,
}

/// How far a counter moved. A counter that went down was reset (a new period, the Reset
/// button, a new helper) and has counted `now` since.
fn grown(before: Option<u64>, now: u64) -> u64 {
    match before {
        Some(before) if now >= before => now - before,
        _ => now,
    }
}

impl Traffic {
    pub fn new(saved: Option<Saved>, today: &str) -> Self {
        let mut traffic = Self { day: today.to_string(), ..Self::default() };
        if let Some(saved) = saved.filter(|s| s.day == today) {
            for (id, totals) in saved.networks {
                let net = traffic.nets.entry(id).or_default();
                net.today_system = totals.system;
                net.today_downloads = totals.downloads;
            }
        }
        traffic
    }

    fn roll(&mut self, today: &str) {
        if self.day != today {
            self.day = today.to_string();
            for net in self.nets.values_mut() {
                net.today_system = 0;
                net.today_downloads = 0;
            }
        }
    }

    /// System mode is off (or failed): nothing flows through it any more.
    pub fn system_stopped(&mut self) {
        for net in self.nets.values_mut() {
            (net.system_down_bps, net.system_up_bps, net.tcp, net.udp) = (0.0, 0.0, 0, 0);
        }
    }

    /// The helper's stats for the last second.
    pub fn system(&mut self, stats: &[NetStats], today: &str) {
        self.roll(today);
        self.system_stopped();
        for s in stats {
            let total = s.down_total.saturating_add(s.up_total);
            let added = grown(self.system_seen.insert(s.id.clone(), total), total);
            let net = self.nets.entry(s.id.clone()).or_default();
            net.system_down_bps = s.down_bps as f64;
            net.system_up_bps = s.up_bps as f64;
            (net.tcp, net.udp) = (s.tcp, s.udp);
            net.session_system += added;
            net.today_system += added;
        }
    }

    /// The manager's usage counters (bytes each network received for downloads in its period).
    /// The first ones only set the baseline: they count from before this session.
    pub fn usage(&mut self, usage: &BTreeMap<NetworkId, u64>, today: &str) {
        self.roll(today);
        if let Some(seen) = &self.usage_seen {
            for (id, &now) in usage {
                let added = grown(seen.get(id).copied().or(Some(0)), now);
                if added > 0 {
                    let net = self.nets.entry(id.clone()).or_default();
                    net.session_downloads += added;
                    net.today_downloads += added;
                }
            }
        }
        self.usage_seen = Some(usage.clone());
    }

    /// Once a second: takes the downloads' current speeds and records a chart sample.
    pub fn tick(&mut self, downloads: &BTreeMap<NetworkId, f64>, today: &str) {
        self.roll(today);
        for id in downloads.keys() {
            self.nets.entry(id.clone()).or_default();
        }
        for (id, net) in &mut self.nets {
            net.downloads_bps = downloads.get(id).copied().unwrap_or(0.0);
            for (history, value) in [
                (&mut net.system_history, net.system_down_bps),
                (&mut net.downloads_history, net.downloads_bps),
            ] {
                history.push_back(value);
                while history.len() > HISTORY {
                    history.pop_front();
                }
            }
        }
    }

    pub fn session_total(&self) -> u64 {
        self.nets.values().map(NetTraffic::session).sum()
    }

    pub fn down_bps(&self) -> f64 {
        self.nets.values().map(NetTraffic::down_bps).sum()
    }

    pub fn saved(&self) -> Saved {
        Saved {
            day: self.day.clone(),
            networks: self
                .nets
                .iter()
                .filter(|(_, n)| n.today_system + n.today_downloads > 0)
                .map(|(id, n)| {
                    (id.clone(), DayTotals { system: n.today_system, downloads: n.today_downloads })
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sonic_core::NetworkKind;

    fn id(s: &str) -> NetworkId {
        NetworkId::new(s)
    }

    fn stat(name: &str, down: u64, total: u64) -> NetStats {
        NetStats {
            id: id(name),
            name: name.into(),
            kind: NetworkKind::Wifi,
            tcp: 2,
            udp: 1,
            down_bps: down,
            up_bps: 0,
            down_total: total,
            up_total: 0,
            capacity_bps: 0,
        }
    }

    #[test]
    fn system_bytes_count_from_the_helpers_running_totals() {
        let mut t = Traffic::new(None, "2026-10-17");
        t.system(&[stat("a", 100, 1000)], "2026-10-17");
        t.system(&[stat("a", 100, 1500)], "2026-10-17");
        assert_eq!(t.nets[&id("a")].session_system, 1500);
        assert_eq!(t.nets[&id("a")].tcp, 2);
        // A new helper starts again from zero; the same totals again add nothing.
        t.system(&[stat("a", 0, 200)], "2026-10-17");
        t.system(&[stat("a", 0, 200)], "2026-10-17");
        assert_eq!(t.nets[&id("a")].session_system, 1700);
        // A network the helper no longer lists carries nothing now.
        t.system(&[stat("b", 5, 5)], "2026-10-17");
        assert_eq!(t.nets[&id("a")].system_down_bps, 0.0);
        assert_eq!(t.nets[&id("a")].tcp, 0);
        t.system_stopped();
        assert_eq!(t.down_bps(), 0.0);
    }

    #[test]
    fn downloads_are_the_growth_of_the_usage_counters() {
        let mut t = Traffic::new(None, "2026-10-17");
        t.usage(&BTreeMap::from([(id("a"), 5000)]), "2026-10-17"); // baseline only
        assert_eq!(t.session_total(), 0);
        t.usage(&BTreeMap::from([(id("a"), 5600), (id("b"), 40)]), "2026-10-17");
        assert_eq!(t.nets[&id("a")].session_downloads, 600);
        assert_eq!(t.nets[&id("b")].session_downloads, 40);
        // Reset to 0 and counted 10 since.
        t.usage(&BTreeMap::from([(id("a"), 10), (id("b"), 40)]), "2026-10-17");
        assert_eq!(t.nets[&id("a")].session_downloads, 610);
    }

    #[test]
    fn today_survives_a_restart_that_day_and_rolls_at_midnight() {
        let mut t = Traffic::new(None, "2026-10-17");
        t.system(&[stat("a", 0, 700)], "2026-10-17");
        let saved = t.saved();
        assert_eq!(saved.networks[&id("a")], DayTotals { system: 700, downloads: 0 });

        let again = Traffic::new(Some(saved.clone()), "2026-10-17");
        assert_eq!(again.nets[&id("a")].today_system, 700);
        assert_eq!(again.nets[&id("a")].session_system, 0, "a new session");
        let tomorrow = Traffic::new(Some(saved), "2026-10-18");
        assert!(tomorrow.nets.is_empty());

        t.system(&[stat("a", 0, 900)], "2026-10-18");
        assert_eq!(t.nets[&id("a")].today_system, 200);
        assert_eq!(t.nets[&id("a")].session_system, 900);
    }

    #[test]
    fn history_keeps_the_last_minute_of_both_kinds() {
        let mut t = Traffic::new(None, "d");
        t.system(&[stat("a", 300, 0)], "d");
        for i in 0..70 {
            t.tick(&BTreeMap::from([(id("a"), i as f64)]), "d");
        }
        let a = &t.nets[&id("a")];
        assert_eq!(a.system_history.len(), HISTORY);
        assert_eq!(a.downloads_history.back(), Some(&69.0));
        assert_eq!(a.downloads_history.front(), Some(&10.0));
        assert_eq!(a.down_bps(), 369.0);
    }
}
