//! The computer's networks, kept current.
//!
//! The monitor simply looks every second. One `GetAdaptersAddresses` call is cheap, and a full
//! look catches every change the UI shows (addresses, gateways, names) in one place, where OS
//! change notifications each cover only some of them.

use std::sync::{Arc, Weak};
use std::time::Duration;

use sonic_core::{NetworkId, NetworkInterface};
use tokio::sync::{Mutex, watch};
use tokio::time::MissedTickBehavior;

const POLL_INTERVAL: Duration = Duration::from_secs(1);

type List = Arc<Vec<NetworkInterface>>;

/// Watches the adapters and publishes the list whenever it changes (an adapter appears, drops, or
/// gets a new address). Cloning shares one monitor.
#[derive(Clone, Debug)]
pub struct NetworkMonitor {
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    list: watch::Sender<List>,
    /// Held for each look, so a [`NetworkMonitor::refresh`] and the poller never interleave and
    /// publish an older list over a newer one.
    looking: Mutex<()>,
}

impl NetworkMonitor {
    /// Starts watching. Must be called inside a Tokio runtime.
    ///
    /// The first look happens here, synchronously, so [`current`](Self::current) is meaningful
    /// at once. The polling task holds only a weak reference, so it ends by itself once the last
    /// clone is dropped.
    pub fn start() -> Self {
        let initial = crate::list_interfaces().unwrap_or_else(|error| {
            tracing::warn!(%error, "could not list network adapters");
            Vec::new()
        });
        let shared = Arc::new(Shared {
            list: watch::Sender::new(Arc::new(initial)),
            looking: Mutex::new(()),
        });
        tokio::spawn(poll(Arc::downgrade(&shared)));
        Self { shared }
    }

    /// The networks as last seen.
    pub fn current(&self) -> Arc<Vec<NetworkInterface>> {
        self.shared.list.borrow().clone()
    }

    pub fn find(&self, id: &NetworkId) -> Option<NetworkInterface> {
        self.shared.list.borrow().iter().find(|iface| &iface.id == id).cloned()
    }

    /// Looks now rather than at the next poll, and returns what it found.
    pub async fn refresh(&self) -> Arc<Vec<NetworkInterface>> {
        self.shared.look().await
    }

    /// Hears of every change. Its `changed()` fails once the last monitor is dropped.
    pub fn subscribe(&self) -> watch::Receiver<Arc<Vec<NetworkInterface>>> {
        self.shared.list.subscribe()
    }
}

impl Shared {
    /// Lists the adapters and publishes the list if it differs from the last one. A failed look
    /// keeps the last list: an OS hiccup shouldn't make every network vanish for a second.
    async fn look(&self) -> List {
        let _looking = self.looking.lock().await;
        // The OS call blocks (briefly); keep it off the async worker threads.
        match tokio::task::spawn_blocking(crate::list_interfaces).await {
            Ok(Ok(found)) => {
                self.list.send_if_modified(|list| {
                    let changed = **list != found;
                    if changed {
                        *list = Arc::new(found);
                    }
                    changed
                });
            }
            Ok(Err(error)) => tracing::warn!(%error, "could not list network adapters"),
            Err(error) => tracing::warn!(%error, "network adapter listing panicked"),
        }
        self.list.borrow().clone()
    }
}

/// Looks every [`POLL_INTERVAL`] for as long as a monitor exists.
async fn poll(shared: Weak<Shared>) {
    let mut ticks = tokio::time::interval(POLL_INTERVAL);
    // After a stall (the computer slept), look once and carry on, rather than catching up.
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticks.tick().await; // The first tick is immediate, and `start` has just looked.
    loop {
        ticks.tick().await;
        let Some(shared) = shared.upgrade() else { return };
        shared.look().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn starts_with_the_current_list_and_refreshes_without_spurious_changes() {
        let monitor = NetworkMonitor::start();
        assert_eq!(*monitor.current(), crate::list_interfaces().unwrap());
        let changes = monitor.subscribe();
        assert_eq!(monitor.refresh().await, monitor.current());
        // An unchanged list is not announced again.
        assert!(!changes.has_changed().unwrap());
        if let Some(first) = monitor.current().first() {
            assert_eq!(monitor.find(&first.id).as_ref(), Some(first));
        }
        assert_eq!(monitor.find(&NetworkId::new("no such adapter")), None);
    }

    #[tokio::test(start_paused = true)]
    async fn the_poller_stops_with_the_last_monitor() {
        let monitor = NetworkMonitor::start();
        let weak = Arc::downgrade(&monitor.shared);
        let clone = monitor.clone();
        let mut changes = monitor.subscribe();
        drop(monitor);
        tokio::time::sleep(POLL_INTERVAL * 3).await;
        assert!(weak.upgrade().is_some(), "a clone still holds it");

        drop(clone);
        // A look already under way may hold it until it finishes, but no longer.
        let gone = async { while changes.changed().await.is_ok() {} };
        tokio::time::timeout(POLL_INTERVAL * 2, gone)
            .await
            .expect("the monitor outlived its clones");
        assert!(weak.upgrade().is_none(), "the poller must not keep the monitor alive");
    }
}
