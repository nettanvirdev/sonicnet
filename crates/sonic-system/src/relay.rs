//! Moving bytes between an app's connection (the stack side) and our upstream connection.
//!
//! TCP: [`relay`] copies both directions with TCP's half-close kept intact: when one side
//! finishes sending, the other side's write half is shut down (a FIN), while the other direction
//! keeps flowing until it finishes too. HTTP/1.0 clients, `nc -N`, and many protocols send their
//! request and then half-close; closing both directions at the first EOF would cut off the reply.
//!
//! UDP: [`relay_datagrams`] forwards one datagram per read/write between a flow on the stack and
//! a connected UDP socket.
//!
//! Both end after `idle` without a byte in either direction, so a connection whose app or server
//! vanished without a FIN/RST (a laptop lid, a NAT that forgot it) doesn't hold memory forever.

use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::UdpSocket;

/// Byte counters for one network, shared by every connection on it. Read by the stats display
/// and the capacity estimate.
#[derive(Debug, Default)]
pub struct Meter {
    /// Bytes sent to the internet (app → server).
    pub up: AtomicU64,
    /// Bytes received from the internet (server → app).
    pub down: AtomicU64,
}

/// How a relay ended, in bytes moved each way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Moved {
    pub up: u64,
    pub down: u64,
}

/// Copies `app` ↔ `upstream` until both directions have finished (EOF, then a half-close of the
/// other side), an error, or `idle` with no bytes either way ([`io::ErrorKind::TimedOut`]).
/// Every byte is added to each of `meters` (the network's, the destination's) as it moves, so
/// throughput is visible while the connection lives, not only when it ends.
pub async fn relay<A, U>(
    app: &mut A,
    upstream: &mut U,
    meters: &[&Meter],
    idle: Duration,
) -> io::Result<Moved>
where
    A: AsyncRead + AsyncWrite + Unpin + ?Sized,
    U: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let activity = AtomicU64::new(0);
    let mut counted = Counted { inner: upstream, meters, activity: &activity };
    let copy = tokio::io::copy_bidirectional(app, &mut counted);
    tokio::select! {
        result = copy => result.map(|(up, down)| Moved { up, down }),
        () = idle_watch(&activity, idle) => Err(io::ErrorKind::TimedOut.into()),
    }
}

/// Forwards datagrams `app` ↔ `socket` (already connected to the destination) until `app` ends,
/// an error, or `idle` with no datagram either way. `app` must preserve datagram boundaries, one
/// datagram per read and per write, as the stack's UDP flows do.
pub async fn relay_datagrams<A>(
    app: &mut A,
    socket: &UdpSocket,
    meters: &[&Meter],
    idle: Duration,
) -> io::Result<Moved>
where
    A: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    // 64 KiB: the largest a UDP datagram can be, so nothing is ever truncated.
    let mut from_app = vec![0u8; 65536];
    let mut from_net = vec![0u8; 65536];
    let mut moved = Moved { up: 0, down: 0 };
    loop {
        tokio::select! {
            read = app.read(&mut from_app) => {
                let n = read?;
                // The stack signals a closed flow with an empty read. A real zero-length datagram
                // is legal but carries nothing; ending on it is the lesser evil.
                if n == 0 {
                    return Ok(moved);
                }
                socket.send(&from_app[..n]).await?;
                add(meters, n, false);
                moved.up += n as u64;
            }
            received = socket.recv(&mut from_net) => {
                let n = received?;
                app.write_all(&from_net[..n]).await?;
                add(meters, n, true);
                moved.down += n as u64;
            }
            () = tokio::time::sleep(idle) => return Err(io::ErrorKind::TimedOut.into()),
        }
    }
}

/// How often [`relay`] re-polls both sides even without a wake-up. `ipstack` marks a connection
/// closed (the app's FIN was answered and acknowledged) without waking a pending reader, so
/// without this nudge the relay would only notice when the stack's own idle timer fires.
const NUDGE: Duration = Duration::from_secs(1);

/// Resolves once `activity` hasn't changed for a whole `idle` period. Ticks every [`NUDGE`]: each
/// tick wakes the task, and `select!` then polls the copy too (see [`NUDGE`]).
async fn idle_watch(activity: &AtomicU64, idle: Duration) {
    let mut seen = activity.load(Ordering::Relaxed);
    let mut quiet_since = tokio::time::Instant::now();
    loop {
        tokio::time::sleep(NUDGE.min(idle)).await;
        let now = activity.load(Ordering::Relaxed);
        if now != seen {
            seen = now;
            quiet_since = tokio::time::Instant::now();
        } else if quiet_since.elapsed() >= idle {
            return;
        }
    }
}

/// Adds `n` bytes to each meter, received (`down`) or sent.
fn add(meters: &[&Meter], n: usize, down: bool) {
    for meter in meters {
        let counter = if down { &meter.down } else { &meter.up };
        counter.fetch_add(n as u64, Ordering::Relaxed);
    }
}

/// The upstream side, counting bytes into the meters and the connection's activity.
struct Counted<'a, U: ?Sized> {
    inner: &'a mut U,
    meters: &'a [&'a Meter],
    activity: &'a AtomicU64,
}

impl<U: ?Sized> Counted<'_, U> {
    fn count(&self, n: usize, down: bool) {
        add(self.meters, n, down);
        self.activity.fetch_add(n as u64, Ordering::Relaxed);
    }
}

impl<U: AsyncRead + Unpin + ?Sized> AsyncRead for Counted<'_, U> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut *self.inner).poll_read(cx, buf);
        let n = buf.filled().len() - before;
        if n > 0 {
            self.count(n, true);
        }
        result
    }
}

impl<U: AsyncWrite + Unpin + ?Sized> AsyncWrite for Counted<'_, U> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut *self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = result {
            self.count(n, false);
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr};
    use tokio::io::duplex;
    use tokio::net::TcpListener;

    const IDLE: Duration = Duration::from_secs(30);

    #[tokio::test]
    async fn copies_both_ways_and_keeps_half_close() {
        // app_peer ⇄ [app_side | relay | up_side] ⇄ server
        let (mut app_peer, mut app_side) = duplex(1024);
        let (mut up_side, mut server) = duplex(1024);
        let (meter, dest) = (Meter::default(), Meter::default());

        let server_task = tokio::spawn(async move {
            // The server reads the whole request (to EOF, i.e. the client's half-close) and only
            // then answers: the reply must still get through.
            let mut request = Vec::new();
            server.read_to_end(&mut request).await.unwrap();
            assert_eq!(request, b"GET /");
            server.write_all(b"the reply").await.unwrap();
            server.shutdown().await.unwrap();
        });
        let client_task = tokio::spawn(async move {
            app_peer.write_all(b"GET /").await.unwrap();
            app_peer.shutdown().await.unwrap();
            let mut reply = Vec::new();
            app_peer.read_to_end(&mut reply).await.unwrap();
            reply
        });

        let moved = relay(&mut app_side, &mut up_side, &[&meter, &dest], IDLE).await.unwrap();
        server_task.await.unwrap();
        assert_eq!(client_task.await.unwrap(), b"the reply");
        assert_eq!((dest.up.load(Ordering::Relaxed), dest.down.load(Ordering::Relaxed)), (5, 9));
        assert_eq!(moved, Moved { up: 5, down: 9 });
        assert_eq!(meter.up.load(Ordering::Relaxed), 5);
        assert_eq!(meter.down.load(Ordering::Relaxed), 9);
    }

    #[tokio::test]
    async fn moves_more_than_the_buffer_both_ways() {
        let (mut app_peer, mut app_side) = duplex(4096);
        let (mut up_side, mut server) = duplex(4096);
        let meter = Meter::default();
        let data: Vec<u8> = (0..1_000_000u32).map(|i| (i % 251) as u8).collect();

        let expected = data.clone();
        let server_task = tokio::spawn(async move {
            // Echo everything back.
            let (mut r, mut w) = tokio::io::split(&mut server);
            tokio::io::copy(&mut r, &mut w).await.unwrap();
            w.shutdown().await.unwrap();
        });
        let client_task = tokio::spawn(async move {
            let (mut r, mut w) = tokio::io::split(&mut app_peer);
            let send = async {
                w.write_all(&data).await.unwrap();
                w.shutdown().await.unwrap();
            };
            let mut back = Vec::new();
            let receive = r.read_to_end(&mut back);
            let ((), got) = tokio::join!(send, receive);
            got.unwrap();
            back
        });

        let moved = relay(&mut app_side, &mut up_side, &[&meter], IDLE).await.unwrap();
        server_task.await.unwrap();
        assert_eq!(client_task.await.unwrap(), expected);
        assert_eq!(moved, Moved { up: 1_000_000, down: 1_000_000 });
    }

    #[tokio::test(start_paused = true)]
    async fn idle_connection_times_out() {
        let (_app_peer, mut app_side) = duplex(1024);
        let (mut up_side, _server) = duplex(1024);
        let meter = Meter::default();
        let error = relay(&mut app_side, &mut up_side, &[&meter], IDLE).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test(start_paused = true)]
    async fn trickling_connection_does_not_time_out() {
        let (mut app_peer, mut app_side) = duplex(1024);
        let (mut up_side, mut server) = duplex(1024);
        let meter = Meter::default();
        let server_task = tokio::spawn(async move {
            // One byte every 20 s for 2 minutes: never idle for the full 30 s.
            for _ in 0..6 {
                tokio::time::sleep(Duration::from_secs(20)).await;
                server.write_all(b"x").await.unwrap();
            }
            server.shutdown().await.unwrap();
            let mut rest = Vec::new();
            server.read_to_end(&mut rest).await.unwrap();
        });
        let client_task = tokio::spawn(async move {
            let mut got = Vec::new();
            app_peer.read_to_end(&mut got).await.unwrap();
            app_peer.shutdown().await.unwrap();
            got
        });
        let moved = relay(&mut app_side, &mut up_side, &[&meter], IDLE).await.unwrap();
        server_task.await.unwrap();
        assert_eq!(client_task.await.unwrap(), b"xxxxxx");
        assert_eq!(moved.down, 6);
    }

    /// An app side that reaches EOF at `closes_at` without ever waking its reader, as ipstack does
    /// when a connection closes.
    struct SilentClose {
        closes_at: tokio::time::Instant,
    }

    impl AsyncRead for SilentClose {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if tokio::time::Instant::now() >= self.closes_at {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending // and no waker registered: nothing will ever wake us
            }
        }
    }

    impl AsyncWrite for SilentClose {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            b: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(b.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn notices_a_close_nobody_signalled() {
        let start = tokio::time::Instant::now();
        let mut app = SilentClose { closes_at: start + Duration::from_secs(3) };
        let (mut up_side, mut server) = duplex(1024);
        tokio::spawn(async move {
            server.shutdown().await.unwrap();
            let mut rest = Vec::new();
            server.read_to_end(&mut rest).await.unwrap();
        });
        relay(&mut app, &mut up_side, &[&Meter::default()], IDLE).await.unwrap();
        // Seen within a nudge of the close, long before the 30 s idle timeout.
        assert!(start.elapsed() <= Duration::from_secs(3) + NUDGE, "{:?}", start.elapsed());
    }

    /// The real thing on loopback: an upstream from `sonic_net::dial` (as System mode opens it)
    /// to a TCP server, relayed to an in-memory app side.
    #[tokio::test]
    async fn relays_through_a_dialed_loopback_connection() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            socket.read_to_end(&mut request).await.unwrap();
            request.reverse();
            socket.write_all(&request).await.unwrap();
        });

        let mut upstream = sonic_net::dial(
            &sonic_net::loopback(),
            "127.0.0.1",
            port,
            false,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        let (mut app_peer, mut app_side) = duplex(1024);
        let client = tokio::spawn(async move {
            app_peer.write_all(b"abc").await.unwrap();
            app_peer.shutdown().await.unwrap();
            let mut reply = Vec::new();
            app_peer.read_to_end(&mut reply).await.unwrap();
            reply
        });
        let meter = Meter::default();
        let moved = relay(&mut app_side, &mut upstream, &[&meter], IDLE).await.unwrap();
        assert_eq!(client.await.unwrap(), b"cba");
        assert_eq!(moved, Moved { up: 3, down: 3 });
    }

    #[tokio::test]
    async fn relays_datagrams_through_loopback() {
        // An echo server that upper-cases each datagram.
        let server = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let server_addr: SocketAddr = server.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            loop {
                let (n, from) = server.recv_from(&mut buf).await.unwrap();
                let reply = buf[..n].to_ascii_uppercase();
                server.send_to(&reply, from).await.unwrap();
            }
        });

        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        socket.connect(server_addr).await.unwrap();
        let (mut app_peer, mut app_side) = duplex(4096);
        let meter = Meter::default();
        let relay_task = tokio::spawn(async move {
            let moved = relay_datagrams(&mut app_side, &socket, &[&meter], IDLE).await;
            moved.map(|m| (m, meter))
        });

        // One datagram at a time, waiting for each answer, keeps boundaries on the byte duplex.
        for word in [&b"one"[..], b"two", b"three"] {
            app_peer.write_all(word).await.unwrap();
            let mut buf = [0u8; 16];
            let n = app_peer.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], word.to_ascii_uppercase().as_slice());
        }
        drop(app_peer); // the flow ends → empty read → relay returns
        let (moved, meter) = relay_task.await.unwrap().unwrap();
        assert_eq!(moved, Moved { up: 11, down: 11 });
        assert_eq!(meter.down.load(Ordering::Relaxed), 11);
    }

    #[tokio::test(start_paused = true)]
    async fn idle_udp_flow_times_out() {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        socket.connect((Ipv4Addr::LOCALHOST, 9)).await.unwrap();
        let (_app_peer, mut app_side) = duplex(1024);
        let error =
            relay_datagrams(&mut app_side, &socket, &[&Meter::default()], IDLE).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }
}
