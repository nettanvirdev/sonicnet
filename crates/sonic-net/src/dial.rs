//! Connections that leave through one chosen network.
//!
//! This is what lets every network carry its own share of a download. Each socket is pinned to
//! its adapter twice over:
//!
//! - **`IP_UNICAST_IF` / `IPV6_UNICAST_IF`** tell Windows which interface to send from. Binding a
//!   local address alone is not enough: Windows uses the weak-host model for sends, so a socket
//!   bound to the Wi-Fi address may still leave through the default (say, Ethernet) interface
//!   with the Wi-Fi source address — and that network's router drops it, or the reply comes back
//!   on the wrong link. Then both "networks" share one link and nothing is gained.
//! - **Binding the adapter's address** picks the source address the server replies to, which
//!   matters when an adapter has several (IPv4 and IPv6, temporary IPv6 addresses).
//!
//! An adapter may reach a host by several routes (each local address of the right family × each
//! address DNS returned). They are raced as RFC 8305 "Happy Eyeballs" describes, so an address
//! that doesn't answer (IPv6 with no way out, a stale record) costs a new connection 250 ms
//! rather than its whole timeout.

use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex, OnceLock, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use rustls_platform_verifier::BuilderVerifierExt;
use socket2::{Domain, Protocol, Socket, Type};
use sonic_core::{NetworkId, NetworkInterface};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpSocket, TcpStream};
use tokio::task::JoinSet;
use tokio_rustls::TlsConnector;

/// Why a connection couldn't be opened.
#[derive(Debug, thiserror::Error)]
pub enum DialError {
    /// The network has no address of the server's IP family (an IPv4-only adapter and an
    /// IPv6-only host). No retry over this network will change that.
    #[error("no address of this network can reach {host}")]
    NoCompatibleRoute { host: String },
    #[error("could not look up {host}: {source}")]
    Resolve { host: String, source: io::Error },
    #[error("connection timed out")]
    Timeout,
    #[error("TLS handshake failed: {0}")]
    Tls(String),
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// An open connection to the server: plain TCP, or TLS over it. Reads and writes like a socket.
pub struct Connection {
    inner: Pin<Box<dyn Io>>,
}

trait Io: AsyncRead + AsyncWrite + Send {}
impl<T: AsyncRead + AsyncWrite + Send> Io for T {}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection").finish_non_exhaustive()
    }
}

impl Connection {
    fn new(io: impl AsyncRead + AsyncWrite + Send + 'static) -> Self {
        Self { inner: Box::pin(io) }
    }
}

impl AsyncRead for Connection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.inner.as_mut().poll_read(cx, buf)
    }
}

impl AsyncWrite for Connection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.inner.as_mut().poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.as_mut().poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.as_mut().poll_shutdown(cx)
    }
}

/// The server's addresses, in the order DNS gave them. An IP literal (`[::1]`, `10.0.0.1`) is
/// returned as is. Fails with [`DialError::Timeout`] past `timeout`.
pub async fn resolve(host: &str, timeout: Duration) -> Result<Vec<IpAddr>, DialError> {
    let host = unbracket(host);
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(vec![ip]);
    }
    let lookup = tokio::net::lookup_host((host, 0));
    match tokio::time::timeout(timeout, lookup).await {
        Err(_) => Err(DialError::Timeout),
        Ok(Err(source)) => Err(DialError::Resolve { host: host.to_owned(), source }),
        Ok(Ok(addrs)) => Ok(addrs.map(|a| a.ip()).collect()),
    }
}

/// Opens a connection to `host:port` that leaves through `iface`, wrapped in TLS (verifying the
/// certificate for `host`, ALPN `http/1.1`) when `secure`. `timeout` bounds all of it: DNS, TCP and
/// the TLS handshake. Dropping the future abandons the attempt and closes its sockets.
pub async fn dial(
    iface: &NetworkInterface,
    host: &str,
    port: u16,
    secure: bool,
    timeout: Duration,
) -> Result<Connection, DialError> {
    let host = unbracket(host);
    let key: RouteKey = (iface.id.clone(), host.to_owned(), port);
    let attempt = async {
        let found = routes(iface, &resolve(host, timeout).await?);
        if found.is_empty() {
            return Err(DialError::NoCompatibleRoute { host: host.to_owned() });
        }
        let preferred = last_good_routes().get(&key).copied();
        let order = attempt_order(found, preferred);

        let tls = if secure {
            let name = ServerName::try_from(host.to_owned())
                .map_err(|e| DialError::Tls(format!("{host} is not a valid server name: {e}")))?;
            Some((TlsConnector::from(tls_config()?), name))
        } else {
            None
        };
        let (v4_index, v6_index) = (iface.ipv4_index, iface.ipv6_index);
        let connect = move |route: Route| {
            let tls = tls.clone();
            let index = if route.remote.is_ipv6() { v6_index } else { v4_index };
            async move {
                let tcp = connect_tcp(route, port, index).await?;
                // The handshake is part of the race: a route whose TCP connects but whose TLS
                // never finishes (a middlebox eating it) loses to one that completes.
                match tls {
                    None => Ok(Connection::new(tcp)),
                    Some((connector, name)) => {
                        connector.connect(name, tcp).await.map(Connection::new).map_err(tls_error)
                    }
                }
            }
        };
        race(order, ATTEMPT_DELAY, connect).await.map_err(|error| {
            error.unwrap_or_else(|| DialError::NoCompatibleRoute { host: host.to_owned() })
        })
    };

    let result = match tokio::time::timeout(timeout, attempt).await {
        Err(_) => Err(DialError::Timeout),
        Ok(result) => result,
    };
    let mut remembered = last_good_routes();
    match result {
        Ok((route, connection)) => {
            remembered.insert(key, route);
            Ok(connection)
        }
        Err(error) => {
            // The remembered route may be what failed; start the next dial from DNS order.
            remembered.remove(&key);
            Err(error)
        }
    }
}

/// `[::1]` → `::1`: URLs bracket IPv6 literals, sockets and certificates don't.
fn unbracket(host: &str) -> &str {
    host.strip_prefix('[').and_then(|h| h.strip_suffix(']')).unwrap_or(host)
}

/// RFC 8305 §5's recommended "Connection Attempt Delay": how long one attempt has before the
/// next one starts alongside it.
const ATTEMPT_DELAY: Duration = Duration::from_millis(250);

/// One way to reach the server: from this local address to that remote one (same IP family).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Route {
    local: IpAddr,
    remote: IpAddr,
}

/// Every route from `iface` to the server, remote addresses in DNS order and local addresses in
/// the OS's order. Empty when the adapter has no address of any family the server has.
fn routes(iface: &NetworkInterface, remotes: &[IpAddr]) -> Vec<Route> {
    remotes
        .iter()
        .flat_map(|&remote| {
            iface.addresses_of(remote.is_ipv6()).map(move |local| Route { local, remote })
        })
        .collect()
}

/// The order to try routes in: the one that last worked first, then the rest alternating between
/// IP families starting with the family DNS listed first (RFC 8305 §4), so one broken family
/// can't make every early attempt fail.
fn attempt_order(routes: Vec<Route>, preferred: Option<Route>) -> Vec<Route> {
    let preferred = preferred.filter(|p| routes.contains(p));
    let (mut first_family, mut other_family): (Vec<Route>, Vec<Route>) = (Vec::new(), Vec::new());
    let leading_v6 = routes.first().is_some_and(|r| r.remote.is_ipv6());
    for route in routes.into_iter().filter(|r| Some(*r) != preferred) {
        if route.remote.is_ipv6() == leading_v6 {
            first_family.push(route);
        } else {
            other_family.push(route);
        }
    }
    let mut order: Vec<Route> = preferred.into_iter().collect();
    let (mut a, mut b) = (first_family.into_iter(), other_family.into_iter());
    loop {
        match (a.next(), b.next()) {
            (None, None) => return order,
            (x, y) => order.extend(x.into_iter().chain(y)),
        }
    }
}

/// Races `routes` the Happy Eyeballs way: each starts [`ATTEMPT_DELAY`] after the previous one, or
/// at once when the previous one fails; the first to succeed wins and the rest are dropped
/// (closing their sockets). Fails with the most telling error once every route has failed, or
/// `None` for no routes at all.
///
/// Attempts run as tasks so a slow one never holds up another's progress; dropping the race
/// (the dial timed out, the caller gave up) aborts them all.
async fn race<T, F, Fut>(
    routes: Vec<Route>,
    delay: Duration,
    mut connect: F,
) -> Result<(Route, T), Option<DialError>>
where
    T: Send + 'static,
    F: FnMut(Route) -> Fut,
    Fut: Future<Output = Result<T, DialError>> + Send + 'static,
{
    let mut waiting = routes.into_iter();
    let mut running = JoinSet::new();
    let mut launch = |running: &mut JoinSet<_>, route: Route| {
        let attempt = connect(route);
        running.spawn(async move { (route, attempt.await) });
    };
    let mut error = None;

    if let Some(route) = waiting.next() {
        launch(&mut running, route);
    }
    loop {
        let more_waiting = waiting.len() > 0;
        // A failed attempt always launches the next one, so `running` is only empty once
        // `waiting` is too, and `join_next` returning `None` means every route has failed.
        tokio::select! {
            joined = running.join_next() => match joined {
                None => return Err(error),
                Some(Ok((route, Ok(connection)))) => return Ok((route, connection)),
                Some(Ok((_, Err(e)))) => {
                    keep_most_telling(&mut error, e);
                    if let Some(route) = waiting.next() {
                        launch(&mut running, route);
                    }
                }
                // A panic in an attempt surfaces rather than turning into a network failure. The
                // only other join error is cancellation, which happens when the runtime shuts
                // down mid-dial (the app quitting): that attempt simply didn't connect.
                Some(Err(join_error)) if join_error.is_panic() => {
                    std::panic::resume_unwind(join_error.into_panic())
                }
                Some(Err(_cancelled)) => {
                    if let Some(route) = waiting.next() {
                        launch(&mut running, route);
                    }
                }
            },
            () = tokio::time::sleep(delay), if more_waiting => {
                if let Some(route) = waiting.next() {
                    launch(&mut running, route);
                }
            }
        }
    }
}

/// Keeps the newest error, except that a TLS failure is never replaced: it shows the network did
/// reach the server, which says more than another route's refusal.
fn keep_most_telling(kept: &mut Option<DialError>, new: DialError) {
    if !matches!(kept, Some(DialError::Tls(_))) {
        *kept = Some(new);
    }
}

/// A TCP connection along `route`, sent out through interface `index` (the OS index for the
/// route's IP family).
async fn connect_tcp(route: Route, port: u16, index: u32) -> io::Result<TcpStream> {
    let remote = SocketAddr::new(route.remote, port);
    let socket = Socket::new(Domain::for_address(remote), Type::STREAM, Some(Protocol::TCP))?;
    // Loopback traffic never leaves through an adapter, and pinning it to one makes Windows
    // refuse the connection; tests and local servers dial through `loopback()`.
    #[cfg(windows)]
    if !route.remote.is_loopback() {
        pin_to_interface(&socket, route.remote.is_ipv6(), index)?;
    }
    #[cfg(not(windows))]
    let _ = index; // ponytail: no per-interface pinning off Windows (Linux needs SO_BINDTODEVICE).
    socket.bind(&SocketAddr::new(route.local, 0).into())?;
    // Tokio requires a nonblocking socket; it then drives the connect itself.
    socket.set_nonblocking(true)?;
    TcpSocket::from_std_stream(socket.into()).connect(remote).await
}

/// Makes every packet the socket sends leave through interface `index`, whatever the routing
/// table says (see the module docs for why binding alone isn't enough). Must happen before
/// `connect`, which is when Windows picks the route.
#[cfg(windows)]
fn pin_to_interface(socket: &Socket, ipv6: bool, index: u32) -> io::Result<()> {
    use std::os::windows::io::AsRawSocket;
    use windows_sys::Win32::Networking::WinSock::{
        IP_UNICAST_IF, IPPROTO_IP, IPPROTO_IPV6, IPV6_UNICAST_IF, SOCKET, WSAGetLastError,
        setsockopt,
    };

    // The two options disagree on byte order: IPv4 wants the index in network order (as if it
    // were an IPv4 address), IPv6 in host order. Getting this wrong silently pins to a
    // different (or no) interface.
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

/// The TLS settings every connection shares. One config means one session cache, so a stream's
/// next connection to the same server resumes the TLS session instead of redoing the full
/// handshake (and certificate check).
fn tls_config() -> Result<Arc<ClientConfig>, DialError> {
    static CONFIG: OnceLock<Arc<ClientConfig>> = OnceLock::new();
    if let Some(config) = CONFIG.get() {
        return Ok(config.clone());
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        // The OS verifier (the Windows certificate store) trusts what the user's browser trusts,
        // including enterprise roots, and checks revocation the way the OS does.
        .and_then(|builder| builder.with_platform_verifier())
        .map_err(|e| DialError::Tls(e.to_string()))?
        .with_no_client_auth();
    // HTTP/1.1 only: each stream is one connection doing one request at a time, and HTTP/2
    // would multiplex streams onto one connection, defeating the per-stream bandwidth split.
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    // Two threads may build at once; both configs are equivalent and the first one stored wins.
    Ok(CONFIG.get_or_init(|| Arc::new(config)).clone())
}

/// A handshake failure the TLS layer itself reported (bad certificate, not TLS at all, nothing in
/// common) is the server's doing: [`DialError::Tls`]. Anything else (reset, EOF) is the
/// connection failing: [`DialError::Io`].
fn tls_error(error: io::Error) -> DialError {
    match error.get_ref().and_then(|inner| inner.downcast_ref::<rustls::Error>()) {
        Some(tls) => DialError::Tls(tls.to_string()),
        None => DialError::Io(error),
    }
}

/// (adapter, host, port) → the route that last connected.
type RouteKey = (NetworkId, String, u16);

/// Which route last worked for each adapter and server, tried first next time so a known-good
/// route connects without waiting out a broken one.
// ponytail: grows by one entry per (adapter, host, port) ever dialed; bound it if hosts churn.
fn last_good_routes() -> std::sync::MutexGuard<'static, HashMap<RouteKey, Route>> {
    static ROUTES: LazyLock<Mutex<HashMap<RouteKey, Route>>> = LazyLock::new(Default::default);
    // A panic mid-insert can't leave the map inconsistent, so a poisoned lock is still usable.
    ROUTES.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Instant;

    use sonic_core::{InterfaceAddress, NetworkKind};
    use tokio::net::TcpListener;

    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn route(local: &str, remote: &str) -> Route {
        Route { local: ip(local), remote: ip(remote) }
    }

    fn iface(addresses: &[&str]) -> NetworkInterface {
        NetworkInterface {
            id: NetworkId::new("test"),
            name: "Test".into(),
            description: String::new(),
            kind: NetworkKind::Other,
            addresses: addresses
                .iter()
                .map(|a| InterfaceAddress { ip: ip(a), prefix_len: 24 })
                .collect(),
            ipv4_index: 0,
            ipv6_index: 0,
            has_gateway: true,
            mac: None,
        }
    }

    #[test]
    fn routes_pair_matching_families_in_dns_order() {
        let both = iface(&["10.0.0.2", "2001:db8::2", "10.0.0.3"]);
        let found = routes(&both, &[ip("2001:db8::1"), ip("192.0.2.1")]);
        assert_eq!(
            found,
            [
                route("2001:db8::2", "2001:db8::1"),
                route("10.0.0.2", "192.0.2.1"),
                route("10.0.0.3", "192.0.2.1"),
            ]
        );
        assert!(routes(&iface(&["10.0.0.2"]), &[ip("2001:db8::1")]).is_empty());
    }

    #[test]
    fn attempts_alternate_families_starting_with_dns_first() {
        let v6a = route("2001:db8::2", "2001:db8::1");
        let v6b = route("2001:db8::2", "2001:db8::9");
        let v4a = route("10.0.0.2", "192.0.2.1");
        let v4b = route("10.0.0.2", "192.0.2.9");
        let v4c = route("10.0.0.2", "192.0.2.10");
        assert_eq!(attempt_order(vec![v6a, v6b, v4a, v4b, v4c], None), [v6a, v4a, v6b, v4b, v4c]);
        assert_eq!(attempt_order(vec![v4a, v6a, v4b], None), [v4a, v6a, v4b]);
        assert_eq!(attempt_order(Vec::new(), None), []);
    }

    #[test]
    fn last_good_route_goes_first_only_if_still_offered() {
        let v6a = route("2001:db8::2", "2001:db8::1");
        let v4a = route("10.0.0.2", "192.0.2.1");
        let v4b = route("10.0.0.2", "192.0.2.9");
        assert_eq!(attempt_order(vec![v6a, v4a, v4b], Some(v4b)), [v4b, v6a, v4a]);
        let gone = route("10.0.0.7", "192.0.2.1");
        assert_eq!(attempt_order(vec![v6a, v4a], Some(gone)), [v6a, v4a]);
    }

    #[test]
    fn unbracketing() {
        assert_eq!(unbracket("[::1]"), "::1");
        assert_eq!(unbracket("example.com"), "example.com");
        assert_eq!(unbracket("[broken"), "[broken");
    }

    /// Routes 0, 1, 2… whose behaviour the test picks by index.
    fn numbered(n: u8) -> Vec<Route> {
        (0..n).map(|i| route("10.0.0.2", &format!("192.0.2.{i}"))).collect()
    }

    fn index(route: Route) -> u8 {
        match route.remote {
            IpAddr::V4(v4) => v4.octets()[3],
            IpAddr::V6(_) => unreachable!(),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_route_hands_over_after_the_delay() {
        let start = tokio::time::Instant::now();
        let (winner, ()) = race(numbered(2), ATTEMPT_DELAY, |r| async move {
            if index(r) == 0 { std::future::pending().await } else { Ok(()) }
        })
        .await
        .unwrap();
        assert_eq!(index(winner), 1);
        assert_eq!(start.elapsed(), ATTEMPT_DELAY);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_route_hands_over_at_once() {
        let start = tokio::time::Instant::now();
        let (winner, ()) = race(numbered(3), ATTEMPT_DELAY, |r| async move {
            match index(r) {
                2 => Ok(()),
                _ => Err(DialError::Io(io::ErrorKind::ConnectionRefused.into())),
            }
        })
        .await
        .unwrap();
        assert_eq!(index(winner), 2);
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn the_first_to_connect_wins_and_the_rest_are_dropped() {
        struct Flag(Arc<AtomicBool>);
        impl Drop for Flag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let loser_dropped = Arc::new(AtomicBool::new(false));
        let flag = loser_dropped.clone();
        let start = tokio::time::Instant::now();
        let (winner, ()) = race(numbered(2), ATTEMPT_DELAY, move |r| {
            let flag = Flag(flag.clone());
            async move {
                let _flag = flag;
                // Route 0 is slow; route 1 starts 250 ms later and connects in 100 ms.
                let took = if index(r) == 0 { 1000 } else { 100 };
                tokio::time::sleep(Duration::from_millis(took)).await;
                Ok(())
            }
        })
        .await
        .unwrap();
        assert_eq!(index(winner), 1);
        assert_eq!(start.elapsed(), Duration::from_millis(350));
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(loser_dropped.load(Ordering::SeqCst), "the slow attempt must be dropped");
    }

    #[tokio::test(start_paused = true)]
    async fn when_all_fail_a_tls_error_says_the_most() {
        let error = race(numbered(3), ATTEMPT_DELAY, |r| async move {
            Err::<(), _>(match index(r) {
                1 => DialError::Tls("bad certificate".into()),
                _ => DialError::Io(io::ErrorKind::ConnectionRefused.into()),
            })
        })
        .await
        .unwrap_err();
        assert!(matches!(error, Some(DialError::Tls(_))), "{error:?}");
        let none = race(Vec::new(), ATTEMPT_DELAY, |_| async { Ok(()) }).await.unwrap_err();
        assert!(none.is_none());
    }

    /// Real sockets: a route into a blackhole (10.255.255.1 never answers) is raced against a
    /// working loopback route, which must win without waiting the blackhole out.
    #[tokio::test]
    async fn a_blackholed_route_loses_to_a_working_one() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let routes = vec![route("0.0.0.0", "10.255.255.1"), route("127.0.0.1", "127.0.0.1")];
        let start = Instant::now();
        let (winner, _stream) =
            race(routes, ATTEMPT_DELAY, |r| async move { Ok(connect_tcp(r, port, 0).await?) })
                .await
                .unwrap();
        assert_eq!(winner, route("127.0.0.1", "127.0.0.1"));
        assert!(start.elapsed() < Duration::from_secs(2), "took {:?}", start.elapsed());
    }

    #[tokio::test]
    async fn dial_remembers_the_route_that_worked() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let mut iface = crate::loopback();
        iface.id = NetworkId::new("remember-test");
        dial(&iface, "127.0.0.1", port, false, Duration::from_secs(5)).await.unwrap();
        let key = (iface.id.clone(), "127.0.0.1".to_owned(), port);
        assert_eq!(last_good_routes().get(&key), Some(&route("127.0.0.1", "127.0.0.1")));

        drop(listener); // Now refused: a failed dial forgets the route.
        dial(&iface, "127.0.0.1", port, false, Duration::from_secs(5)).await.unwrap_err();
        assert_eq!(last_good_routes().get(&key), None);
    }
}
