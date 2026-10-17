//! A download's networks, squared with the computer's.

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;

use sonic_core::{DownloadNetwork, NetworkId, NetworkInterface, NetworkStatus, StreamState};

/// A network as a download that is about to use it (`enabled`) or not lists it.
pub(crate) fn new_network(iface: &NetworkInterface, enabled: bool) -> DownloadNetwork {
    DownloadNetwork {
        id: iface.id.clone(),
        label: iface.name.clone(),
        kind: iface.kind,
        enabled,
        status: if enabled { NetworkStatus::On } else { NetworkStatus::Off },
        error: None,
        bytes_downloaded: 0,
        speed_bps: 0.0,
        retries: 0,
    }
}

/// Brings a download's networks up to date with the computer's `present` ones.
///
/// - Every network on the computer is listed; one that turns up mid-download starts off, for the
///   user to switch on. A network the download never used is dropped once it goes.
/// - Each status follows from the user's choice and the network's situation: off, offline (gone),
///   at its data limit, or else on — except that a network judged unreachable or failed stays so
///   until something gives it another go (a resume, switching it off and on, reconnecting).
///
/// Whether anything changed.
pub(crate) fn reconcile(
    networks: &mut Vec<DownloadNetwork>,
    streams: &[StreamState],
    present: &[NetworkInterface],
    at_limit: impl Fn(&NetworkId) -> bool,
) -> bool {
    let before = networks.clone();
    networks.retain(|network| {
        network.enabled
            || network.bytes_downloaded > 0
            || present.iter().any(|iface| iface.id == network.id)
            || streams.iter().any(|stream| stream.network_id == network.id)
    });
    for iface in present {
        if !networks.iter().any(|network| network.id == iface.id) {
            networks.push(new_network(iface, false));
        }
    }
    for network in networks.iter_mut() {
        let iface = present.iter().find(|iface| iface.id == network.id);
        if let Some(iface) = iface {
            network.label.clone_from(&iface.name);
            network.kind = iface.kind;
        }
        let status = if !network.enabled {
            NetworkStatus::Off
        } else if iface.is_none() {
            NetworkStatus::Offline
        } else if at_limit(&network.id) {
            NetworkStatus::Limit
        } else if matches!(
            network.status,
            NetworkStatus::Off | NetworkStatus::Offline | NetworkStatus::Limit
        ) {
            NetworkStatus::On
        } else {
            network.status
        };
        if status != network.status {
            network.status = status;
            network.error = None;
        }
    }
    *networks != before
}

/// Which networks' addresses changed since `seen`, and which of those lost one (so sockets bound
/// to it are dead). Updates `seen`. A network seen for the first time counts as changed.
pub(crate) fn address_changes(
    seen: &mut HashMap<NetworkId, Vec<IpAddr>>,
    present: &[NetworkInterface],
) -> (HashSet<NetworkId>, HashSet<NetworkId>) {
    let mut changed = HashSet::new();
    let mut moved = HashSet::new();
    let mut now_seen = HashMap::new();
    for iface in present {
        let addresses: Vec<IpAddr> = iface.addresses.iter().map(|a| a.ip).collect();
        let before = seen.get(&iface.id);
        let same = before.is_some_and(|before| {
            before.len() == addresses.len() && before.iter().all(|a| addresses.contains(a))
        });
        if !same {
            changed.insert(iface.id.clone());
            if before.is_some_and(|before| before.iter().any(|a| !addresses.contains(a))) {
                moved.insert(iface.id.clone());
            }
        }
        now_seen.insert(iface.id.clone(), addresses);
    }
    *seen = now_seen;
    (changed, moved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sonic_core::{InterfaceAddress, NetworkKind};

    fn iface(id: &str, ip: [u8; 4]) -> NetworkInterface {
        NetworkInterface {
            id: NetworkId::new(id),
            name: format!("net {id}"),
            description: String::new(),
            kind: NetworkKind::Ethernet,
            addresses: vec![InterfaceAddress { ip: IpAddr::from(ip), prefix_len: 24 }],
            ipv4_index: 1,
            ipv6_index: 0,
            has_gateway: true,
            mac: None,
        }
    }

    fn statuses(networks: &[DownloadNetwork]) -> Vec<(&str, NetworkStatus)> {
        networks.iter().map(|n| (n.id.as_str(), n.status)).collect()
    }

    #[test]
    fn a_network_appearing_mid_download_starts_off() {
        let a = iface("a", [10, 0, 0, 1]);
        let mut networks = vec![new_network(&a, true)];
        assert!(reconcile(&mut networks, &[], &[a.clone(), iface("b", [10, 0, 1, 1])], |_| false));
        assert_eq!(statuses(&networks), [("a", NetworkStatus::On), ("b", NetworkStatus::Off)]);
    }

    #[test]
    fn a_gone_network_is_offline_if_used_and_dropped_if_not() {
        let (a, b) = (iface("a", [10, 0, 0, 1]), iface("b", [10, 0, 1, 1]));
        let mut networks = vec![new_network(&a, true), new_network(&b, false)];
        reconcile(&mut networks, &[], &[], |_| false);
        assert_eq!(statuses(&networks), [("a", NetworkStatus::Offline)]);
        // Back again: on.
        reconcile(&mut networks, &[], &[a], |_| false);
        assert_eq!(statuses(&networks), [("a", NetworkStatus::On)]);
    }

    #[test]
    fn failed_and_unreachable_stick_while_limit_and_off_follow_the_situation() {
        let (a, b) = (iface("a", [10, 0, 0, 1]), iface("b", [10, 0, 1, 1]));
        let mut networks = vec![new_network(&a, true), new_network(&b, true)];
        networks[0].status = NetworkStatus::Failed;
        networks[0].error = Some("refused".into());
        networks[1].status = NetworkStatus::Unreachable;
        let present = [a.clone(), b.clone()];
        reconcile(&mut networks, &[], &present, |_| false);
        assert_eq!(
            statuses(&networks),
            [("a", NetworkStatus::Failed), ("b", NetworkStatus::Unreachable)]
        );
        assert!(networks[0].error.is_some());

        let at_limit = |id: &NetworkId| id.as_str() == "b";
        reconcile(&mut networks, &[], &present, at_limit);
        assert_eq!(networks[1].status, NetworkStatus::Limit);
        reconcile(&mut networks, &[], &present, |_| false);
        assert_eq!(networks[1].status, NetworkStatus::On, "the allowance was raised");

        networks[0].enabled = false;
        reconcile(&mut networks, &[], &present, |_| false);
        assert_eq!(networks[0].status, NetworkStatus::Off);
        assert_eq!(networks[0].error, None);
    }

    #[test]
    fn a_network_with_bytes_or_streams_stays_listed_when_gone() {
        let a = iface("a", [10, 0, 0, 1]);
        let mut networks = vec![new_network(&a, false)];
        networks[0].bytes_downloaded = 5;
        reconcile(&mut networks, &[], &[], |_| false);
        assert_eq!(networks.len(), 1);
    }

    #[test]
    fn address_changes_tell_new_and_moved_networks() {
        let mut seen = HashMap::new();
        let a = iface("a", [10, 0, 0, 1]);
        let (changed, moved) = address_changes(&mut seen, std::slice::from_ref(&a));
        assert!(changed.contains(&a.id) && moved.is_empty());
        let (changed, _) = address_changes(&mut seen, std::slice::from_ref(&a));
        assert!(changed.is_empty());
        let (changed, moved) = address_changes(&mut seen, &[iface("a", [10, 0, 0, 2])]);
        assert!(changed.contains(&a.id) && moved.contains(&a.id));
    }
}
