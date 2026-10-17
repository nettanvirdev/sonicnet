//! Fake networks: each one is a `127.0.0.x` source address. A connection bound to `127.0.0.3`
//! still reaches the origin on `127.0.0.1`, but arrives from `127.0.0.3`, which is how the origin
//! tells the "networks" apart and slows each down on its own.

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use sonic_core::{InterfaceAddress, NetworkId, NetworkInterface, NetworkKind};
use tokio::sync::watch;

/// A network whose only address is `ip` (meant for `127.0.0.x`, x ≥ 2).
pub fn fake_network(id: &str, ip: Ipv4Addr) -> NetworkInterface {
    NetworkInterface {
        id: NetworkId::new(id),
        name: id.to_owned(),
        description: format!("Test network on {ip}"),
        kind: NetworkKind::Ethernet,
        addresses: vec![InterfaceAddress { ip: IpAddr::V4(ip), prefix_len: 8 }],
        ipv4_index: 1,
        ipv6_index: 0,
        has_gateway: true,
        mac: None,
    }
}

/// The computer's networks as the manager sees them (`NetworkSource::Fixed`): tests add, remove
/// and replace them mid-download. Clones control the same list.
#[derive(Clone, Debug)]
pub struct Networks {
    tx: Arc<watch::Sender<Arc<Vec<NetworkInterface>>>>,
}

impl Networks {
    pub fn new(list: Vec<NetworkInterface>) -> Self {
        Self { tx: Arc::new(watch::Sender::new(Arc::new(list))) }
    }

    /// A receiver for a manager to watch.
    pub fn receiver(&self) -> watch::Receiver<Arc<Vec<NetworkInterface>>> {
        self.tx.subscribe()
    }

    pub fn list(&self) -> Vec<NetworkInterface> {
        self.tx.borrow().as_ref().clone()
    }

    pub fn set(&self, list: Vec<NetworkInterface>) {
        self.tx.send_replace(Arc::new(list));
    }

    /// Plugs a network in (replacing one with the same id).
    pub fn add(&self, network: NetworkInterface) {
        let mut list = self.list();
        list.retain(|n| n.id != network.id);
        list.push(network);
        self.set(list);
    }

    /// Unplugs a network.
    pub fn remove(&self, id: &NetworkId) {
        let mut list = self.list();
        list.retain(|n| &n.id != id);
        self.set(list);
    }

    /// Changes a listed network in place (a new address, say), keeping its position.
    pub fn replace(&self, network: NetworkInterface) {
        let list = self
            .list()
            .into_iter()
            .map(|n| if n.id == network.id { network.clone() } else { n })
            .collect();
        self.set(list);
    }
}
