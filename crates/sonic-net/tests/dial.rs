//! `dial` end to end against local servers, plus (ignored) real HTTPS over every network.

use std::time::{Duration, Instant};

use sonic_net::{DialError, dial, list_all_interfaces, loopback};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const TIMEOUT: Duration = Duration::from_secs(5);

async fn listener() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

#[tokio::test]
async fn plain_connection_through_loopback() {
    let (listener, port) = listener().await;
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = [0; 4];
        socket.read_exact(&mut buf).await.unwrap();
        socket.write_all(&buf).await.unwrap();
    });

    let mut conn = dial(&loopback(), "127.0.0.1", port, false, TIMEOUT).await.unwrap();
    conn.write_all(b"ping").await.unwrap();
    let mut echo = [0; 4];
    conn.read_exact(&mut echo).await.unwrap();
    assert_eq!(&echo, b"ping");
    server.await.unwrap();
}

#[tokio::test]
async fn tls_against_a_plain_server_is_a_tls_error() {
    let (listener, port) = listener().await;
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut hello = [0; 512];
        let _ = socket.read(&mut hello).await;
        let _ = socket.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n").await;
    });

    let error = dial(&loopback(), "localhost", port, true, TIMEOUT).await.unwrap_err();
    assert!(matches!(error, DialError::Tls(_)), "{error:?}");
}

#[tokio::test]
async fn the_timeout_covers_the_tls_handshake() {
    let (listener, port) = listener().await;
    // Accepts, then never says a word.
    let server = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.unwrap();
        std::future::pending::<()>().await;
    });

    let start = Instant::now();
    let timeout = Duration::from_millis(300);
    let error = dial(&loopback(), "127.0.0.1", port, true, timeout).await.unwrap_err();
    assert!(matches!(error, DialError::Timeout), "{error:?}");
    assert!(start.elapsed() < Duration::from_secs(2), "took {:?}", start.elapsed());
    server.abort();
}

#[tokio::test]
async fn an_ipv4_only_network_cannot_reach_an_ipv6_host() {
    let error = dial(&loopback(), "[::1]", 443, false, TIMEOUT).await.unwrap_err();
    assert!(matches!(error, DialError::NoCompatibleRoute { .. }), "{error:?}");
}

/// Real HTTPS through every network that has a gateway: each must complete a verified TLS
/// handshake and get an HTTP/1.1 answer. A network without a gateway must *fail*: if it
/// succeeded, the socket left through another adapter and pinning is broken. Needs the internet,
/// so it's opt-in: `cargo test -p sonic-net -- --ignored`.
#[tokio::test]
#[ignore = "needs the internet"]
async fn https_over_every_network() {
    let (networks, isolated): (Vec<_>, Vec<_>) =
        list_all_interfaces().unwrap().into_iter().partition(|iface| iface.has_gateway);
    for iface in isolated {
        let result = dial(&iface, "example.com", 443, true, Duration::from_secs(3)).await;
        assert!(result.is_err(), "{} has no gateway yet reached the internet", iface.name);
    }
    assert!(!networks.is_empty(), "no network with a gateway");
    for iface in networks {
        let mut conn = dial(&iface, "example.com", 443, true, Duration::from_secs(10))
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", iface.name));
        conn.write_all(b"HEAD / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut head = [0; 12];
        conn.read_exact(&mut head).await.unwrap();
        assert!(head.starts_with(b"HTTP/1.1 "), "{}: {:?}", iface.name, head);
    }
}
