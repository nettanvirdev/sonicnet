//! System mode: every app's connections balanced across the user's networks through a
//! virtual adapter (Wintun). See docs/system-mode.md.
//!
//! - [`scheduler`]: which network each new connection or UDP flow leaves through (pure).
//! - [`relay`]: moves bytes between the app's side (the stack) and the upstream connection.
//! - [`udp`]: UDP sockets pinned to one network.
//! - `tun` (Windows): the Wintun adapter, the user-space stack on it, and host routes into it.
//! - [`probe`]: a short pinned download that measures what each network can carry.
//! - [`protocol`]: what the elevated helper and the app say over their pipe.
//! - `helper` (Windows): the helper process, `sonicnet-system.exe` (its `main` is in the app
//!   package so `cargo build -p sonicnet` builds both exes side by side).
//! - `watch` (Windows): noticing that another process ended.

#[cfg(windows)]
pub mod helper;
pub mod probe;
pub mod protocol;
pub mod relay;
pub mod scheduler;
#[cfg(windows)]
pub mod tun;
pub mod udp;
#[cfg(windows)]
pub mod watch;
