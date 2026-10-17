//! Measuring what a network can actually carry, so connections are placed by real capacity.
//!
//! Throughput observed while relaying is only a lower bound: a network that was given few
//! connections shows little, and would then be given even fewer. A short active download over
//! each network, pinned to it, gives the scheduler a fair starting point (at most
//! [`PROBE_TIME`] and [`PROBE_BYTES`] per source tried).

use std::io;
use std::time::{Duration, Instant};

use sonic_core::NetworkInterface;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Where probes download from, tried in order until one answers: (host, port, TLS, path). A
/// second source because a speed-test endpoint may rate-limit an address that used it a lot.
pub const SOURCES: [(&str, u16, bool, &str); 2] = [
    ("speed.cloudflare.com", 443, true, "/__down?bytes=10000000"),
    ("ipv4.download.thinkbroadband.com", 80, false, "/10MB.zip"),
];
pub const PROBE_TIME: Duration = Duration::from_secs(3);
pub const PROBE_BYTES: u64 = 10_000_000;

/// Parallel downloads per probe: one TCP connection over a long path is held back by its window
/// and round trip, not by the link, and would make a distant source look like a slow network.
pub const PROBE_STREAMS: usize = 4;

/// Downloads over `net` alone from the first source that answers, [`PROBE_STREAMS`] at once;
/// their combined bytes a second.
pub async fn probe(net: &NetworkInterface) -> io::Result<f64> {
    let mut last = io::Error::other("no probe source");
    for (host, port, secure, path) in SOURCES {
        let mut streams = tokio::task::JoinSet::new();
        for _ in 0..PROBE_STREAMS {
            let net = net.clone();
            streams.spawn(async move {
                let mut stream = sonic_net::dial(&net, host, port, secure, Duration::from_secs(5))
                    .await
                    .map_err(io::Error::other)?;
                let share = PROBE_BYTES / PROBE_STREAMS as u64;
                measure(&mut stream, host, path, PROBE_TIME, share).await
            });
        }
        let mut total = 0.0;
        while let Some(done) = streams.join_next().await {
            match done.map_err(io::Error::other).and_then(|rate| rate) {
                Ok(rate) => total += rate,
                Err(error) => last = io::Error::other(format!("{host}: {error}")),
            }
        }
        if total > 0.0 {
            return Ok(total);
        }
    }
    Err(last)
}

/// Sends a GET for `path` and times the body from its first byte, until `limit` bytes, `budget`
/// or the end. The handshake and time to first byte are left out: they are latency, not
/// capacity.
pub async fn measure<S>(
    stream: &mut S,
    host: &str,
    path: &str,
    budget: Duration,
    limit: u64,
) -> io::Result<f64>
where
    S: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: SonicNet\r\nAccept: */*\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await?;
    let mut buf = vec![0u8; 64 * 1024];
    let mut head = Vec::new();
    // The status line and headers; whatever follows them is the body's first bytes.
    let mut body = loop {
        let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "no response"));
        }
        head.extend_from_slice(&buf[..n]);
        if let Some(end) = head.windows(4).position(|w| w == b"\r\n\r\n") {
            let status = String::from_utf8_lossy(&head[..end]);
            if !status.starts_with("HTTP/1.1 200") && !status.starts_with("HTTP/1.0 200") {
                let line = status.lines().next().unwrap_or_default().to_string();
                return Err(io::Error::other(format!("probe answered {line}")));
            }
            break (head.len() - end - 4) as u64;
        }
        if head.len() > 64 * 1024 {
            return Err(io::Error::other("probe headers too long"));
        }
    };
    let started = Instant::now();
    let deadline = tokio::time::Instant::now() + budget;
    while body < limit {
        match tokio::time::timeout_at(deadline, stream.read(&mut buf)).await {
            Ok(Ok(0)) | Err(_) => break,
            Ok(Ok(n)) => body += n as u64,
            Ok(Err(error)) => return Err(error),
        }
    }
    let secs = started.elapsed().as_secs_f64();
    // Too little to say anything (a tiny body, or a first read that was all of it).
    if body < 64 * 1024 || secs < 0.05 {
        return Err(io::Error::other("probe too short to measure"));
    }
    Ok(body as f64 / secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn times_the_body_and_stops_at_the_limit() {
        let (mut client, mut server) = tokio::io::duplex(256 * 1024);
        let serve = tokio::spawn(async move {
            let mut request = vec![0u8; 1024];
            let n = server.read(&mut request).await.unwrap();
            let request = String::from_utf8_lossy(&request[..n]).to_string();
            server.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4000000\r\n\r\n").await.unwrap();
            // 100 KB every 10 ms: about 10 MB/s, until the client hangs up.
            let chunk = vec![7u8; 100_000];
            for _ in 0..40 {
                if server.write_all(&chunk).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            request
        });
        let rate =
            measure(&mut client, "example", "/x", Duration::from_secs(5), 2_000_000).await.unwrap();
        drop(client);
        let request = serve.await.unwrap();
        assert!(request.starts_with("GET /x HTTP/1.1\r\nHost: example\r\n"), "{request}");
        // Loose bounds: timers on a busy test machine run late, never early.
        assert!((2e6..=12e6).contains(&rate), "{rate}");
    }

    #[tokio::test]
    async fn refuses_anything_but_200() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        tokio::spawn(async move {
            let mut request = vec![0u8; 1024];
            let _ = server.read(&mut request).await;
            let _ = server.write_all(b"HTTP/1.1 429 Too Many Requests\r\n\r\n").await;
        });
        let error = measure(&mut client, "h", "/", Duration::from_secs(1), 1000).await.unwrap_err();
        assert!(error.to_string().contains("429"), "{error}");
    }
}
