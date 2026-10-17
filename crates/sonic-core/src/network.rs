use std::net::IpAddr;

use serde::{Deserialize, Serialize};

use crate::NetworkId;

/// What kind of link an adapter is, as far as the OS lets us tell. Drives the icon and default
/// color in the UI; never changes how a network is used.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum NetworkKind {
    Wifi,
    Ethernet,
    /// A USB adapter — most often a tethered phone.
    Usb,
    /// A phone or modem on the mobile network (WWAN).
    Cellular,
    Bridge,
    /// A VPN or other virtual adapter.
    Virtual,
    #[default]
    Other,
}

/// One local address of an adapter, with its prefix length (e.g. `192.168.1.20/24`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InterfaceAddress {
    pub ip: IpAddr,
    pub prefix_len: u8,
}

/// A network adapter that is up and has at least one usable (non-loopback, non-link-local)
/// address. This is what the user picks networks from.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkInterface {
    pub id: NetworkId,
    /// What the OS shows people: "Wi-Fi", "Ethernet 2", a phone's adapter alias.
    pub name: String,
    /// The driver's own description ("Intel(R) Wi-Fi 6 AX201 160MHz"), when known.
    pub description: String,
    pub kind: NetworkKind,
    pub addresses: Vec<InterfaceAddress>,
    /// The OS interface index for IPv4 (Windows `IfIndex`); 0 when the adapter has no IPv4.
    pub ipv4_index: u32,
    /// The OS interface index for IPv6; 0 when the adapter has no IPv6.
    pub ipv6_index: u32,
    /// Whether the adapter has a default gateway. One without can reach only its local subnet,
    /// so it can't add internet bandwidth (shown as a warning, still selectable).
    pub has_gateway: bool,
    /// Hardware address as `aa:bb:cc:dd:ee:ff`, when it has one.
    pub mac: Option<String>,
}

impl NetworkInterface {
    /// The adapter's addresses of one IP family, in the order the OS listed them.
    pub fn addresses_of(&self, ipv6: bool) -> impl Iterator<Item = IpAddr> + '_ {
        self.addresses.iter().map(|a| a.ip).filter(move |ip| ip.is_ipv6() == ipv6)
    }
}
