//! SonicNet's shared vocabulary.
//!
//! Every other crate speaks these types: the network layer reports [`NetworkInterface`]s, the
//! engine works on [`BlockState`]s, the store persists [`DownloadState`]s and [`AppSettings`], and
//! the UI talks to the backend only through [`Command`] and [`Event`]. Nothing here does I/O.
//!
//! Conventions used throughout the workspace:
//! - Byte ranges are **half-open**: `start..end`, `end` exclusive. HTTP's inclusive
//!   `Range: bytes=a-b` is produced only at the wire (see [`ByteRange::http_header`]).
//! - Speeds are bytes per second as `f64`; sizes and counts are `u64`.
//! - Timestamps are milliseconds since the Unix epoch ([`Millis`]).

mod command;
mod download;
mod ids;
mod limits;
mod network;
mod probe;
mod settings;
mod version;

pub use command::*;
pub use download::*;
pub use ids::*;
pub use limits::*;
pub use network::*;
pub use probe::*;
pub use settings::*;
pub use version::*;

/// Milliseconds since the Unix epoch.
pub type Millis = i64;

/// The current time as [`Millis`].
pub fn now_millis() -> Millis {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as Millis).unwrap_or(0)
}
