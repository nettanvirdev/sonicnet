//! Test-only helpers for SonicNet's end-to-end tests (a port of Plexo's `e2e/` fixtures):
//!
//! - [`Origin`]: an HTTP/1.1 server of seeded files that misbehaves on request, limits each source
//!   address's speed, and can freeze every transfer at one exact byte.
//! - [`fake_network`] / [`Networks`]: "networks" that are `127.0.0.x` source addresses, added and
//!   removed mid-test.
//! - [`Harness`]: one manager driven through `Command`/`Event`, with automatic invariant checks
//!   on every update and on what's left on disk at the end.

mod checks;
mod harness;
mod net;
mod origin;

pub use checks::{state_violations, transition_violation};
pub use harness::{
    BLOCK, Harness, KIB, MIB, WAIT, carried, compare, describe, net, net_ip, network, network_of,
    settle, staging_path, streams_on, test_tuning,
};
pub use net::{Networks, fake_network};
pub use origin::{Fault, File, Origin, PROBE_RANGE, Request, seeded_bytes};

#[cfg(test)]
mod tests;
