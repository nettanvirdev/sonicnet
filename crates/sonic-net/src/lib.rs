//! The computer's networks, and connections pinned to one of them.
//!
//! - [`list_interfaces`] and [`NetworkMonitor`]: which adapters are up and usable, kept current.
//! - [`dial()`]: a TCP (optionally TLS) connection to a server that leaves through one chosen
//!   adapter, so each network carries its own share of a download.
//!
//! **Public API is frozen** — other crates build against these signatures. Internals may change.

mod dial;
mod interfaces;
mod monitor;

pub use dial::{Connection, DialError, dial, resolve};
pub use interfaces::{list_all_interfaces, list_interfaces, loopback};
pub use monitor::NetworkMonitor;
