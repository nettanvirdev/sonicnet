//! UDP sockets that leave through one chosen network, for relaying UDP flows (DNS, QUIC).
//!
//! The same double pinning `sonic_net::dial` uses for TCP: `IP_UNICAST_IF` / `IPV6_UNICAST_IF`
//! picks the interface whatever the routing table says (which is also what keeps our own
//! packets out of the TUN), and binding the adapter's address picks the source address replies
//! come back to.
//!
//! ponytail: duplicated from sonic-net's private `pin_to_interface`; move into sonic-net as a
//! public `bind_udp` once its frozen API is reopened.

use std::io;
use std::net::{IpAddr, SocketAddr};

use socket2::{Domain, Protocol, Socket, Type};
use sonic_core::NetworkInterface;
use tokio::net::UdpSocket;

/// A UDP socket connected to `dest` that sends through `iface`. Fails with
/// [`io::ErrorKind::AddrNotAvailable`] when the adapter has no address of `dest`'s family.
pub fn bind_udp(iface: &NetworkInterface, dest: SocketAddr) -> io::Result<UdpSocket> {
    let local: IpAddr = iface.addresses_of(dest.is_ipv6()).next().ok_or_else(|| {
        io::Error::new(io::ErrorKind::AddrNotAvailable, "network has no address of this family")
    })?;
    let socket = Socket::new(Domain::for_address(dest), Type::DGRAM, Some(Protocol::UDP))?;
    // Loopback never leaves through an adapter, and Windows refuses pinning it (as in sonic-net).
    #[cfg(windows)]
    if !dest.ip().is_loopback() {
        let index = if dest.is_ipv6() { iface.ipv6_index } else { iface.ipv4_index };
        pin_to_interface(&socket, dest.is_ipv6(), index)?;
    }
    socket.bind(&SocketAddr::new(local, 0).into())?;
    // Connecting filters replies to the one peer of this flow and lets us use send/recv.
    socket.connect(&dest.into())?;
    socket.set_nonblocking(true)?;
    UdpSocket::from_std(socket.into())
}

/// Makes every packet the socket sends leave through interface `index`.
#[cfg(windows)]
fn pin_to_interface(socket: &Socket, ipv6: bool, index: u32) -> io::Result<()> {
    use std::os::windows::io::AsRawSocket;
    use windows_sys::Win32::Networking::WinSock::{
        IP_UNICAST_IF, IPPROTO_IP, IPPROTO_IPV6, IPV6_UNICAST_IF, SOCKET, WSAGetLastError,
        setsockopt,
    };

    // IPv4 wants the index in network byte order, IPv6 in host order; getting it wrong silently
    // pins to some other interface (or none).
    let (level, option, value) = if ipv6 {
        (IPPROTO_IPV6, IPV6_UNICAST_IF, index)
    } else {
        (IPPROTO_IP, IP_UNICAST_IF, index.to_be())
    };
    // SAFETY: the socket handle is open for the duration of the call (borrowed from `socket`),
    // and `optval` points to a live u32 whose size is passed as `optlen`.
    let status = unsafe {
        setsockopt(
            socket.as_raw_socket() as SOCKET,
            level,
            option,
            (&raw const value).cast(),
            size_of::<u32>() as i32,
        )
    };
    if status == 0 {
        Ok(())
    } else {
        // SAFETY: reads this thread's last Winsock error, set by the failed call just above.
        Err(io::Error::from_raw_os_error(unsafe { WSAGetLastError() }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[tokio::test]
    async fn loopback_flow_round_trips() {
        let server = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let socket = bind_udp(&sonic_net::loopback(), server.local_addr().unwrap()).unwrap();
        socket.send(b"ping").await.unwrap();
        let mut buf = [0u8; 16];
        let (n, from) = server.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"ping");
        assert_eq!(from, socket.local_addr().unwrap());
        server.send_to(b"pong", from).await.unwrap();
        let n = socket.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"pong");
    }

    #[tokio::test]
    async fn refuses_a_family_the_network_lacks() {
        let v6 = SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, 53));
        let error = bind_udp(&sonic_net::loopback(), v6).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AddrNotAvailable);
    }

    /// Pinning to a real adapter needs no admin rights; check every gateway network accepts it
    /// (a wrong byte order or option would fail here with WSAEINVAL or similar).
    #[cfg(windows)]
    #[tokio::test]
    async fn pins_to_each_real_network() {
        for iface in sonic_net::list_interfaces().unwrap() {
            if iface.addresses_of(false).next().is_some() {
                let socket = bind_udp(&iface, SocketAddr::from((Ipv4Addr::new(1, 1, 1, 1), 53)));
                assert!(socket.is_ok(), "{}: {:?}", iface.name, socket.err());
            }
        }
    }
}
