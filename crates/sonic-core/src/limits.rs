//! Numbers shared by more than one crate. Tuning that only one crate uses lives in that crate.

/// How many downloads may run at once; the rest wait in the queue.
pub const DOWNLOADS_AT_ONCE_DEFAULT: u32 = 2;
pub const DOWNLOADS_AT_ONCE_MIN: u32 = 1;
pub const DOWNLOADS_AT_ONCE_MAX: u32 = 4;

/// Seconds of per-network speed history a download keeps (one sample a second), and so the span
/// the throughput chart shows.
pub const SPEED_HISTORY_SECONDS: usize = 60;

/// Slow mode's limit until the user picks another: 2 MiB/s, enough for a video call beside it.
pub const DEFAULT_SLOW_MODE_SPEED: u64 = 2 * 1024 * 1024;

/// Streams each network starts with on Auto (IDM and XDM start with 8 too). From there it doubles
/// once all of them are receiving.
pub const START_STREAMS_PER_NETWORK: u32 = 8;

/// The most streams one network may run. Enough for a far server, where TCP holds each connection
/// to a megabyte or so a second; a server that wants fewer says so by refusing.
pub const MAX_STREAMS_PER_NETWORK: u32 = 32;

/// The stream counts the user may pick instead of Auto.
pub const STREAM_CHOICES: [u32; 4] = [4, 8, 16, 32];

/// The suffix of the staging file a download writes into, beside its destination.
pub const STAGING_SUFFIX: &str = ".sonic";
