//! What the System mode helper (`sonicnet-system.exe`, elevated) and the app (unelevated) say to
//! each other over the named pipe the app creates: one JSON object per line.
//!
//! Helper → app: [`HelperMessage`] — `starting`, then `on` (or `failed` with a reason), then a
//! `stats` line every second. App → helper: [`AppMessage::Stop`]. The pipe closing means the same
//! as `stop` to the helper, and "the helper is gone" to the app.

use serde::{Deserialize, Serialize};
use sonic_core::{NetworkId, NetworkKind};

/// One network's System mode traffic, as of the last second.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetStats {
    pub id: NetworkId,
    pub name: String,
    pub kind: NetworkKind,
    /// TCP connections and UDP flows open through it right now.
    pub tcp: u32,
    pub udp: u32,
    /// Bytes a second over the last second: from the internet, and to it.
    pub down_bps: u64,
    pub up_bps: u64,
    /// Bytes since the helper started.
    pub down_total: u64,
    pub up_total: u64,
    /// What System mode reckons it can carry (bytes a second), the weight new connections are
    /// placed by; 0 while unmeasured.
    #[serde(default)]
    pub capacity_bps: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HelperMessage {
    /// Running elevated and connected; setting up the adapter and routes.
    Starting,
    /// Routes in place: every app's traffic goes through System mode.
    On,
    /// Couldn't start (or had to stop); routing is (being) put back as it was.
    Failed {
        reason: String,
    },
    Stats {
        networks: Vec<NetStats>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AppMessage {
    /// Remove the routes and the adapter, then exit.
    Stop,
}

/// One message as a line ready to write (JSON plus `\n`).
pub fn line<T: Serialize>(message: &T) -> String {
    // A plain enum of strings and numbers always serialises.
    let mut text = serde_json::to_string(message).unwrap_or_default();
    text.push('\n');
    text
}

/// Reads one line back; `None` for anything that isn't a message of this protocol (logged by the
/// caller, never fatal: a newer helper may say things an older app doesn't know).
pub fn parse<T: for<'de> Deserialize<'de>>(line: &str) -> Option<T> {
    serde_json::from_str(line.trim()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_round_trip_as_single_lines() {
        let stats = HelperMessage::Stats {
            networks: vec![NetStats {
                id: NetworkId::new("{A}"),
                name: "Wi-Fi".into(),
                kind: NetworkKind::Wifi,
                tcp: 3,
                udp: 1,
                down_bps: 1_000_000,
                up_bps: 20_000,
                down_total: 5_000_000_000,
                up_total: 7,
                capacity_bps: 11_500_000,
            }],
        };
        for message in [
            HelperMessage::Starting,
            HelperMessage::On,
            HelperMessage::Failed { reason: "no \"wintun.dll\"\nhere".into() },
            stats,
        ] {
            let text = line(&message);
            assert!(text.ends_with('\n') && text.matches('\n').count() == 1, "{text}");
            assert_eq!(parse::<HelperMessage>(&text), Some(message));
        }
        assert_eq!(line(&AppMessage::Stop), "{\"type\":\"stop\"}\n");
        assert_eq!(parse::<AppMessage>("{\"type\":\"stop\"}\r\n"), Some(AppMessage::Stop));
    }

    #[test]
    fn unknown_or_broken_lines_are_none() {
        assert_eq!(parse::<HelperMessage>("{\"type\":\"teleport\"}"), None);
        assert_eq!(parse::<HelperMessage>("not json"), None);
        assert_eq!(parse::<AppMessage>(""), None);
        // Fields a newer helper adds are ignored.
        assert_eq!(
            parse::<HelperMessage>("{\"type\":\"on\",\"since\":5}"),
            Some(HelperMessage::On)
        );
    }
}
