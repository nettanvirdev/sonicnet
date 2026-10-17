//! Which network adapters are up and usable.
//!
//! On Windows the list comes from one `GetAdaptersAddresses` call: it is cheap (no child process,
//! unlike asking PowerShell), gives every field the UI and [`crate::dial()`] need, and the adapter
//! GUID it reports is stable across renames and replugs. Other platforms report no adapters yet;
//! only [`loopback`] works there.

// The classification helpers are only called by the Windows lister (and the tests).
#![cfg_attr(not(windows), allow(dead_code))]

use std::io;
use std::net::{IpAddr, Ipv4Addr};

use sonic_core::{InterfaceAddress, NetworkId, NetworkInterface, NetworkKind};

/// The adapters that can carry a download: up, with at least one usable address (not loopback,
/// not link-local) and a default gateway, in a stable order. Cheap enough to call every second.
///
/// An adapter without a gateway reaches only its own subnet — a Hyper-V / WSL switch, a
/// host-only VM network, a cable to another PC — so it can never add internet bandwidth, and
/// listing it would only be clutter the user has to understand and ignore.
pub fn list_interfaces() -> io::Result<Vec<NetworkInterface>> {
    let mut list = list_all_interfaces()?;
    list.retain(|iface| iface.has_gateway);
    Ok(list)
}

/// Every up adapter with a usable address, gateway or not, in the same order as
/// [`list_interfaces`]. For diagnostics and tests (an isolated adapter proves pinning works by
/// failing to reach the internet).
pub fn list_all_interfaces() -> io::Result<Vec<NetworkInterface>> {
    #[cfg(windows)]
    let mut list = windows::list()?;
    #[cfg(not(windows))]
    let mut list: Vec<NetworkInterface> = Vec::new();
    // The OS lists adapters in binding order, which can shuffle when one reconnects; sorting keeps
    // the UI from reordering and the monitor from reporting a change that isn't one.
    list.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.id.cmp(&b.id)));
    Ok(list)
}

/// The loopback adapter, for tests and local development servers: dialing through it reaches
/// `127.0.0.1` without any OS-level interface pinning.
pub fn loopback() -> NetworkInterface {
    NetworkInterface {
        id: NetworkId::new("loopback"),
        name: "Loopback".into(),
        description: "Loopback".into(),
        kind: NetworkKind::Other,
        addresses: vec![InterfaceAddress { ip: IpAddr::V4(Ipv4Addr::LOCALHOST), prefix_len: 8 }],
        ipv4_index: 1,
        ipv6_index: 0,
        has_gateway: false,
        mac: None,
    }
}

/// Whether a connection can usefully leave from `ip`. Loopback never leaves the computer, and a
/// link-local address (169.254/16 when DHCP failed, fe80::/10 on every IPv6 adapter) reaches only
/// the local link, never a server on the internet.
pub(crate) fn is_usable(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => !(v4.is_loopback() || v4.is_link_local() || v4.is_unspecified()),
        IpAddr::V6(v6) => {
            !(v6.is_loopback() || v6.is_unicast_link_local() || v6.is_unspecified())
                && v6.to_ipv4_mapped().is_none()
        }
    }
}

// IANA ifType numbers (the subset classification needs).
const IF_TYPE_ETHERNET: u32 = 6;
const IF_TYPE_PPP: u32 = 23;
const IF_TYPE_PROP_VIRTUAL: u32 = 53;
const IF_TYPE_IEEE80211: u32 = 71;
const IF_TYPE_TUNNEL: u32 = 131;
const IF_TYPE_WWANPP: u32 = 243;
const IF_TYPE_WWANPP2: u32 = 244;

/// What kind of link an adapter is, from its IANA `ifType` and driver description.
///
/// The order matters. The link type wins when it is specific (a USB Wi-Fi dongle is Wi-Fi). A
/// tethered phone reports plain Ethernet (6), so only its driver description gives it away, and
/// that check runs before the virtual-adapter one because those descriptions often say "virtual"
/// things too. VPN and hypervisor adapters also pose as Ethernet, hence the name list.
pub(crate) fn classify(if_type: u32, description: &str) -> NetworkKind {
    let desc = description.to_lowercase();
    // Whole words for short names that would otherwise match inside others ("tap" in "laptop").
    let has_word = |word: &str| desc.split(|c: char| !c.is_alphanumeric()).any(|w| w == word);
    let has = |needle: &str| desc.contains(needle);

    match if_type {
        IF_TYPE_IEEE80211 => return NetworkKind::Wifi,
        IF_TYPE_WWANPP | IF_TYPE_WWANPP2 => return NetworkKind::Cellular,
        _ => {}
    }
    if ["rndis", "remote ndis", "apple mobile device", "tether"].into_iter().any(has)
        || has_word("usb")
    {
        return NetworkKind::Usb;
    }
    // PPP is how Windows' built-in VPN client appears; dial-up modems are long gone.
    if matches!(if_type, IF_TYPE_PPP | IF_TYPE_PROP_VIRTUAL | IF_TYPE_TUNNEL)
        || ["hyper-v", "vmware", "virtualbox", "wireguard", "wintun", "tailscale", "zerotier"]
            .into_iter()
            .any(has)
        || ["tap", "vpn", "tunnel"].into_iter().any(has_word)
    {
        return NetworkKind::Virtual;
    }
    if has_word("bridge") {
        return NetworkKind::Bridge;
    }
    if if_type == IF_TYPE_ETHERNET { NetworkKind::Ethernet } else { NetworkKind::Other }
}

/// A hardware address as `aa:bb:cc:dd:ee:ff`, or `None` for adapters without one (tunnels report
/// zero length or all zeros).
pub(crate) fn format_mac(bytes: &[u8]) -> Option<String> {
    if bytes.iter().all(|&b| b == 0) {
        return None;
    }
    Some(bytes.iter().map(|b| format!("{b:02x}")).collect::<Vec<_>>().join(":"))
}

#[cfg(windows)]
mod windows {
    use std::ffi::CStr;
    use std::io;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use sonic_core::{InterfaceAddress, NetworkId, NetworkInterface};
    use windows_sys::Win32::Foundation::{ERROR_BUFFER_OVERFLOW, ERROR_NO_DATA, NO_ERROR};
    use windows_sys::Win32::NetworkManagement::IpHelper::{
        GAA_FLAG_INCLUDE_GATEWAYS, GAA_FLAG_SKIP_ANYCAST, GAA_FLAG_SKIP_DNS_SERVER,
        GAA_FLAG_SKIP_MULTICAST, GetAdaptersAddresses, IP_ADAPTER_ADDRESSES_LH,
    };
    use windows_sys::Win32::NetworkManagement::Ndis::IfOperStatusUp;
    use windows_sys::Win32::Networking::WinSock::{
        AF_INET, AF_INET6, AF_UNSPEC, IpDadStatePreferred, SOCKADDR, SOCKADDR_IN, SOCKADDR_IN6,
        SOCKET_ADDRESS,
    };

    use super::{classify, format_mac, is_usable};

    /// Microsoft's suggested first size: big enough for most machines, so one call usually does.
    const INITIAL_BUFFER: usize = 15 * 1024;
    /// Adapters can appear between the size query and the fetch; a few retries absorb that.
    const ATTEMPTS: usize = 4;

    pub(super) fn list() -> io::Result<Vec<NetworkInterface>> {
        let flags = GAA_FLAG_INCLUDE_GATEWAYS
            | GAA_FLAG_SKIP_ANYCAST
            | GAA_FLAG_SKIP_MULTICAST
            | GAA_FLAG_SKIP_DNS_SERVER;
        // u64 elements keep the buffer 8-byte aligned, as IP_ADAPTER_ADDRESSES_LH requires.
        let mut buffer: Vec<u64> = Vec::new();
        let mut size = INITIAL_BUFFER as u32;
        for _ in 0..ATTEMPTS {
            buffer.resize((size as usize).div_ceil(size_of::<u64>()), 0);
            let first = buffer.as_mut_ptr().cast::<IP_ADAPTER_ADDRESSES_LH>();
            // SAFETY: `first` points to a writable, 8-byte aligned buffer of at least `size`
            // bytes, which is what GetAdaptersAddresses is told; on overflow it only writes the
            // needed size back into `size`.
            let status = unsafe {
                GetAdaptersAddresses(AF_UNSPEC as u32, flags, std::ptr::null(), first, &mut size)
            };
            match status {
                NO_ERROR => {
                    // SAFETY: on success the buffer holds a valid linked list starting at
                    // `first`, and `buffer` outlives `collect`.
                    return Ok(unsafe { collect(first) });
                }
                ERROR_NO_DATA => return Ok(Vec::new()),
                ERROR_BUFFER_OVERFLOW => continue,
                code => return Err(io::Error::from_raw_os_error(code as i32)),
            }
        }
        Err(io::Error::other("the adapter list kept growing while it was read"))
    }

    /// Converts the adapter list into the usable interfaces.
    ///
    /// # Safety
    /// `first` must be null or the head of a list GetAdaptersAddresses filled, alive for the call.
    unsafe fn collect(first: *const IP_ADAPTER_ADDRESSES_LH) -> Vec<NetworkInterface> {
        let mut list = Vec::new();
        let mut next = first;
        // SAFETY: per the contract every `Next` is null or another valid entry of the same list.
        while let Some(adapter) = unsafe { next.as_ref() } {
            next = adapter.Next;
            if adapter.OperStatus != IfOperStatusUp {
                continue;
            }
            // SAFETY: the entry is valid, so are the pointers inside it.
            if let Some(iface) = unsafe { convert(adapter) } {
                list.push(iface);
            }
        }
        list
    }

    /// One adapter as a [`NetworkInterface`], or `None` when it has no usable address (which also
    /// drops the loopback pseudo-interface: its only addresses are 127.0.0.1 and ::1).
    ///
    /// # Safety
    /// `adapter` must be an entry of a list GetAdaptersAddresses filled.
    unsafe fn convert(adapter: &IP_ADAPTER_ADDRESSES_LH) -> Option<NetworkInterface> {
        let mut addresses = Vec::new();
        let mut unicast = adapter.FirstUnicastAddress;
        // SAFETY: the unicast list is null-terminated and lives in the same buffer.
        while let Some(entry) = unsafe { unicast.as_ref() } {
            unicast = entry.Next;
            // Tentative (duplicate-address detection still running), duplicate and deprecated
            // addresses must not source new connections.
            if entry.DadState != IpDadStatePreferred {
                continue;
            }
            // SAFETY: `Address` comes from the same OS-filled list.
            if let Some(ip) = unsafe { to_ip(&entry.Address) }.filter(|&ip| is_usable(ip)) {
                addresses.push(InterfaceAddress { ip, prefix_len: entry.OnLinkPrefixLength });
            }
        }
        if addresses.is_empty() {
            return None;
        }

        // SAFETY: the OS fills AdapterName with a NUL-terminated ANSI string (the GUID); the
        // union's `Anonymous` view is the one the documentation defines (Length, IfIndex).
        let (id, ipv4_index) = unsafe {
            (
                CStr::from_ptr(adapter.AdapterName.cast()).to_string_lossy().into_owned(),
                adapter.Anonymous1.Anonymous.IfIndex,
            )
        };
        // SAFETY: both are NUL-terminated wide strings in the same buffer.
        let (name, description) =
            unsafe { (wide(adapter.FriendlyName), wide(adapter.Description)) };
        let mac_len = (adapter.PhysicalAddressLength as usize).min(adapter.PhysicalAddress.len());
        Some(NetworkInterface {
            id: NetworkId::new(id),
            kind: classify(adapter.IfType, &description),
            name,
            description,
            addresses,
            ipv4_index,
            ipv6_index: adapter.Ipv6IfIndex,
            has_gateway: !adapter.FirstGatewayAddress.is_null(),
            mac: format_mac(&adapter.PhysicalAddress[..mac_len]),
        })
    }

    /// A NUL-terminated UTF-16 string, lossily; empty for null.
    ///
    /// # Safety
    /// `ptr` must be null or point to a NUL-terminated UTF-16 string.
    unsafe fn wide(ptr: *const u16) -> String {
        if ptr.is_null() {
            return String::new();
        }
        let mut len = 0;
        // SAFETY: the string is NUL-terminated, so every index up to the NUL is in bounds.
        while unsafe { *ptr.add(len) } != 0 {
            len += 1;
        }
        // SAFETY: the `len` units before the NUL were just read.
        String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(ptr, len) })
    }

    /// The IP in a socket address; `None` for other families or a short/null pointer.
    ///
    /// # Safety
    /// `address.lpSockaddr` must be null or point to `iSockaddrLength` readable bytes.
    unsafe fn to_ip(address: &SOCKET_ADDRESS) -> Option<IpAddr> {
        let len = usize::try_from(address.iSockaddrLength).ok()?;
        // SAFETY: non-null and at least a SOCKADDR long, checked first.
        let family = unsafe {
            if address.lpSockaddr.is_null() || len < size_of::<SOCKADDR>() {
                return None;
            }
            (*address.lpSockaddr).sa_family
        };
        match family {
            AF_INET if len >= size_of::<SOCKADDR_IN>() => {
                // SAFETY: the family says it is a SOCKADDR_IN and the length covers one; read
                // unaligned since the OS doesn't promise the cast type's alignment.
                let sin = unsafe { address.lpSockaddr.cast::<SOCKADDR_IN>().read_unaligned() };
                // SAFETY: every bit pattern of the union is a valid u32.
                let raw = unsafe { sin.sin_addr.S_un.S_addr };
                // S_addr is in network byte order, i.e. the octets in memory order.
                Some(IpAddr::V4(Ipv4Addr::from(raw.to_ne_bytes())))
            }
            AF_INET6 if len >= size_of::<SOCKADDR_IN6>() => {
                // SAFETY: as above, for SOCKADDR_IN6.
                let sin6 = unsafe { address.lpSockaddr.cast::<SOCKADDR_IN6>().read_unaligned() };
                // SAFETY: every bit pattern of the union is valid bytes.
                Some(IpAddr::V6(Ipv6Addr::from(unsafe { sin6.sin6_addr.u.Byte })))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn link_type_wins_over_description() {
        assert_eq!(classify(71, "Intel(R) Wi-Fi 6 AX201 160MHz"), NetworkKind::Wifi);
        assert_eq!(classify(71, "TP-Link USB Wireless Adapter"), NetworkKind::Wifi);
        assert_eq!(classify(243, "Qualcomm Snapdragon X55 5G"), NetworkKind::Cellular);
        assert_eq!(classify(244, "Generic Mobile Broadband Adapter"), NetworkKind::Cellular);
    }

    #[test]
    fn tethered_phones_are_usb() {
        for desc in [
            "Remote NDIS based Internet Sharing Device",
            "Remote NDIS Compatible Device #2",
            "Apple Mobile Device Ethernet",
            "Samsung Mobile USB Remote NDIS Network Device",
            "Realtek USB GbE Family Controller",
            "Android USB Ethernet/RNDIS",
            "Bluetooth tethering adapter",
        ] {
            assert_eq!(classify(6, desc), NetworkKind::Usb, "{desc}");
        }
    }

    #[test]
    fn vpns_and_hypervisors_are_virtual() {
        for desc in [
            "Hyper-V Virtual Ethernet Adapter",
            "VMware Virtual Ethernet Adapter for VMnet8",
            "VirtualBox Host-Only Ethernet Adapter",
            "TAP-Windows Adapter V9",
            "WireGuard Tunnel",
            "Wintun Userspace Tunnel",
            "Tailscale Tunnel",
            "ZeroTier Virtual Port",
            "Cisco AnyConnect Secure Mobility Client VPN Adapter",
        ] {
            assert_eq!(classify(6, desc), NetworkKind::Virtual, "{desc}");
        }
        assert_eq!(classify(131, "Teredo Tunneling Pseudo-Interface"), NetworkKind::Virtual);
        assert_eq!(classify(23, "WAN Miniport (IKEv2)"), NetworkKind::Virtual);
        assert_eq!(classify(53, "Some Proprietary Adapter"), NetworkKind::Virtual);
    }

    #[test]
    fn plain_adapters() {
        assert_eq!(classify(6, "Realtek PCIe GbE Family Controller"), NetworkKind::Ethernet);
        assert_eq!(
            classify(6, "Intel(R) Ethernet Connection (laptop dock)"),
            NetworkKind::Ethernet
        );
        assert_eq!(
            classify(6, "Microsoft Network Adapter Multiplexor Bridge"),
            NetworkKind::Bridge
        );
        assert_eq!(classify(1, "Something else"), NetworkKind::Other);
    }

    #[test]
    fn usable_addresses() {
        for ip in ["192.168.1.20", "10.0.0.5", "100.64.1.1", "2001:db8::1", "fd00::1"] {
            assert!(is_usable(ip.parse().unwrap()), "{ip}");
        }
        for ip in ["127.0.0.1", "127.8.9.10", "169.254.3.4", "0.0.0.0", "::1", "::", "fe80::1"] {
            assert!(!is_usable(ip.parse().unwrap()), "{ip}");
        }
        assert!(!is_usable("febf::1".parse().unwrap()), "top of fe80::/10");
        assert!(!is_usable("::ffff:192.168.1.1".parse().unwrap()), "IPv4-mapped");
    }

    #[test]
    fn mac_formatting() {
        assert_eq!(
            format_mac(&[0xaa, 0xbb, 0x0c, 0x00, 0xee, 0xff]).as_deref(),
            Some("aa:bb:0c:00:ee:ff")
        );
        assert_eq!(format_mac(&[]), None);
        assert_eq!(format_mac(&[0; 6]), None);
    }

    #[test]
    fn listed_interfaces_pass_the_filters() {
        let list = list_interfaces().expect("listing adapters");
        for iface in &list {
            assert!(!iface.id.as_str().is_empty(), "{iface:?}");
            assert!(!iface.addresses.is_empty(), "{iface:?}");
            assert!(iface.has_gateway, "an isolated adapter is never offered: {iface:?}");
            for addr in &iface.addresses {
                assert!(is_usable(addr.ip), "{iface:?}");
                let max = if addr.ip.is_ipv4() { 32 } else { 128 };
                assert!(addr.prefix_len <= max, "{iface:?}");
            }
            if iface.addresses_of(false).next().is_some() {
                assert_ne!(iface.ipv4_index, 0, "{iface:?}");
            }
            if iface.addresses_of(true).next().is_some() {
                assert_ne!(iface.ipv6_index, 0, "{iface:?}");
            }
        }
        assert!(list.windows(2).all(|w| w[0].name <= w[1].name), "sorted by name");
        assert_eq!(list, list_interfaces().unwrap(), "stable across calls");
    }
}
