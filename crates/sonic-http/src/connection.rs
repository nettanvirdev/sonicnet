//! One download stream's HTTP/1.1 connection, through one network.
//!
//! A stream fetches block after block from the same server. Keeping one connection alive across
//! those requests means it pays for DNS, the TCP and TLS handshakes and TCP slow start once,
//! not once per block. A new connection is opened only when there is none to reuse: the first
//! request, after the server closed an idle one, after a request was abandoned midway, or after
//! [`StreamConnection::reconnect`] — how a stuck connection, or one bound to an address the
//! network no longer has, is swapped for a fresh one.

use std::fmt;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::Empty;
use hyper::body::Incoming;
use hyper::client::conn::http1::{self, SendRequest};
use hyper::header::{HOST, HeaderMap, HeaderValue};
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use sonic_core::NetworkInterface;
use sonic_net::DialError;
use tokio::task::JoinHandle;
use url::{Host, Url};

/// Why a request got no response. Each says something about the network or the connection, not
/// about the file, so a download never gives up on a stream for one of these alone.
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    /// The stream's network isn't connected to the computer right now.
    #[error("the network is not connected")]
    NetworkGone,
    /// Opening the connection failed. [`DialError::NoCompatibleRoute`] means this network can
    /// never reach the host; the rest may pass.
    #[error(transparent)]
    Dial(#[from] DialError),
    /// No response headers within the header timeout, connecting included.
    #[error("connection stalled: no response from the server")]
    Timeout,
    /// The response body went silent for longer than the stall timeout.
    #[error("connection stalled: no data from the server")]
    Stalled,
    /// The connection broke, or the server spoke something that isn't HTTP/1.1.
    #[error("connection failed: {0}")]
    Http(#[from] hyper::Error),
    /// Not a URL this connection can request (not http/https, or no host).
    #[error("unsupported URL: {0}")]
    UnsupportedUrl(String),
}

/// The response headers of a request; its body is read from `response`.
#[derive(Debug)]
pub struct ResponseStart {
    pub response: Response<Incoming>,
    /// The request went out on a connection an earlier request had already warmed up.
    pub reused: bool,
    /// From sending the request to its response headers, connecting included.
    pub ttfb: Duration,
}

/// Where a connection goes. A request for anywhere else needs a connection of its own.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Origin {
    host: String,
    port: u16,
    secure: bool,
}

/// What a request URL must be: http or https with a host. Checked before anything is sent, and
/// again on every redirect, so a server can't send a stream to `file:` or a hostless URL.
pub(crate) fn origin_of(url: &Url) -> Result<Origin, String> {
    let secure = match url.scheme() {
        "http" => false,
        "https" => true,
        other => return Err(format!("{other}: links are not supported")),
    };
    // Bare, as DNS and TLS want it: no brackets around an IPv6 literal.
    let host = match url.host() {
        Some(Host::Domain(domain)) => domain.to_owned(),
        Some(Host::Ipv4(ip)) => ip.to_string(),
        Some(Host::Ipv6(ip)) => ip.to_string(),
        None => return Err(format!("{url} has no host")),
    };
    let port = url.port_or_known_default().ok_or_else(|| format!("{url} has no port"))?;
    Ok(Origin { host, port, secure })
}

/// Parses a link and checks it is one a [`StreamConnection`] can request.
pub(crate) fn http_url(raw: &str) -> Result<Url, String> {
    let url = Url::parse(raw.trim()).map_err(|error| format!("{raw}: {error}"))?;
    origin_of(&url)?;
    Ok(url)
}

/// An open connection, kept for the next request.
struct Pooled {
    origin: Origin,
    sender: SendRequest<Empty<Bytes>>,
    /// Drives the connection: reads responses, writes requests. Aborted with it.
    driver: JoinHandle<()>,
    /// A request on it has had its response, so the next one is a reuse.
    used: bool,
}

impl Drop for Pooled {
    fn drop(&mut self) {
        // Closes the socket now, rather than whenever the driver next notices nobody holds it.
        self.driver.abort();
    }
}

/// The network a stream runs over, looked up afresh on every connect: a network that comes back
/// with a new address is reached at the new one. `None` while it isn't connected.
type NetworkSource = Box<dyn Fn() -> Option<NetworkInterface> + Send + Sync>;

/// One download stream's connection to the server through one network, kept alive and reused
/// across requests. One request at a time, as HTTP/1.1 has it.
///
/// Dropping a [`request`](Self::request) future cancels it; the half-used connection is then
/// not reused. Dropping a response body before its end closes its connection too (hyper can't
/// reuse a connection with unread bytes on it), so the next request opens a new one.
pub struct StreamConnection {
    network: NetworkSource,
    header_timeout: Duration,
    connect_timeout: Duration,
    pooled: Option<Pooled>,
    /// Set while a request waits for its response. Still set when the next one starts means the
    /// last was abandoned mid-request, leaving its connection in an unknown state.
    in_flight: bool,
}

impl fmt::Debug for StreamConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamConnection")
            .field("header_timeout", &self.header_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("origin", &self.pooled.as_ref().map(|p| &p.origin))
            .finish_non_exhaustive()
    }
}

impl StreamConnection {
    /// A connection through the network `network` returns, opened on the first request.
    ///
    /// - `header_timeout`: how long a request may take to get its response headers, connecting
    ///   included.
    /// - `connect_timeout`: how long opening a connection may take (DNS, TCP, TLS). Shorter than
    ///   `header_timeout`, so a network that can't get through is found out before a slow server
    ///   would be.
    pub fn new(
        network: impl Fn() -> Option<NetworkInterface> + Send + Sync + 'static,
        header_timeout: Duration,
        connect_timeout: Duration,
    ) -> Self {
        Self {
            network: Box::new(network),
            header_timeout,
            connect_timeout: connect_timeout.min(header_timeout),
            pooled: None,
            in_flight: false,
        }
    }

    pub fn header_timeout(&self) -> Duration {
        self.header_timeout
    }

    /// Drops the kept connection, so the next request opens a new one: it may be bound to an
    /// address the network no longer has, have died while the computer slept, or be stuck.
    pub fn reconnect(&mut self) {
        self.pooled = None;
    }

    /// Sends `GET url` with `headers` (a `Host` header is added) and waits for the response
    /// headers.
    ///
    /// A kept-alive connection the server closed while it sat idle only fails once used, so a
    /// request that fails on a reused connection before any response gets one more go on a new
    /// connection — the transparent retry browsers make. Both share the header timeout.
    pub async fn request(
        &mut self,
        url: &Url,
        headers: &HeaderMap,
    ) -> Result<ResponseStart, ConnectionError> {
        let origin = origin_of(url).map_err(ConnectionError::UnsupportedUrl)?;
        if std::mem::take(&mut self.in_flight) {
            self.pooled = None;
        }
        let sent_at = Instant::now();
        match tokio::time::timeout(self.header_timeout, self.send(&origin, url, headers)).await {
            Ok(Ok((response, reused))) => {
                Ok(ResponseStart { response, reused, ttfb: sent_at.elapsed() })
            }
            Ok(Err(error)) => Err(error),
            Err(_) => {
                // Whatever it was doing, it's not to be trusted with the next request.
                self.in_flight = false;
                self.pooled = None;
                Err(ConnectionError::Timeout)
            }
        }
    }

    async fn send(
        &mut self,
        origin: &Origin,
        url: &Url,
        headers: &HeaderMap,
    ) -> Result<(Response<Incoming>, bool), ConnectionError> {
        let mut may_retry = true;
        loop {
            let reused = self.ready(origin).await?;
            let request = build_request(url, headers)?;
            // Invariant: `ready` leaves a connection in `pooled` whenever it succeeds.
            let pooled = self.pooled.as_mut().expect("ready leaves a connection");
            self.in_flight = true;
            let result = pooled.sender.send_request(request).await;
            self.in_flight = false;
            match result {
                Ok(response) => {
                    pooled.used = true;
                    return Ok((response, reused));
                }
                Err(error) => {
                    self.pooled = None;
                    if !(reused && may_retry) {
                        return Err(error.into());
                    }
                    tracing::debug!(%error, "kept-alive connection failed; retrying on a new one");
                    may_retry = false;
                }
            }
        }
    }

    /// Leaves a connection to `origin` ready in `pooled`: the kept one when it can take a request,
    /// else a new one. Whether it is a reuse.
    async fn ready(&mut self, origin: &Origin) -> Result<bool, ConnectionError> {
        let reusable = match &mut self.pooled {
            Some(pooled) if pooled.origin == *origin && !pooled.sender.is_closed() => {
                // Waits for the previous response to finish; fails if the connection closed.
                pooled.sender.ready().await.is_ok()
            }
            _ => false,
        };
        if reusable {
            return Ok(self.pooled.as_ref().is_some_and(|pooled| pooled.used));
        }
        self.pooled = None;
        self.pooled = Some(self.connect(origin).await?);
        Ok(false)
    }

    async fn connect(&self, origin: &Origin) -> Result<Pooled, ConnectionError> {
        let iface = (self.network)().ok_or(ConnectionError::NetworkGone)?;
        let io =
            sonic_net::dial(&iface, &origin.host, origin.port, origin.secure, self.connect_timeout)
                .await?;
        let (sender, connection) = http1::handshake(TokioIo::new(io)).await?;
        let driver = tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!(%error, "connection closed");
            }
        });
        Ok(Pooled { origin: origin.clone(), sender, driver, used: false })
    }
}

fn build_request(url: &Url, headers: &HeaderMap) -> Result<Request<Empty<Bytes>>, ConnectionError> {
    let unsupported = |e: &dyn fmt::Display| ConnectionError::UnsupportedUrl(format!("{url}: {e}"));
    // Origin-form, as HTTP/1.1 sends to an origin server; never the fragment.
    let target = match url.query() {
        Some(query) => format!("{}?{query}", url.path()),
        None => url.path().to_owned(),
    };
    let mut request =
        Request::get(target.as_str()).body(Empty::new()).map_err(|e| unsupported(&e))?;
    // hyper's connection-level client doesn't add Host. `host_str` keeps an IPv6 literal's
    // brackets, and `port` is only there when it isn't the scheme's default — as Host wants.
    let host = match (url.host_str(), url.port()) {
        (Some(host), Some(port)) => format!("{host}:{port}"),
        (Some(host), None) => host.to_owned(),
        (None, _) => return Err(unsupported(&"no host")),
    };
    let host = HeaderValue::from_str(&host).map_err(|e| unsupported(&e))?;
    let request_headers = request.headers_mut();
    request_headers.insert(HOST, host);
    for (name, value) in headers {
        request_headers.append(name, value.clone());
    }
    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_http_urls_with_a_host() {
        assert!(http_url("http://example.com/a").is_ok());
        assert!(http_url("https://[::1]:8443/a").is_ok());
        assert!(http_url("ftp://example.com/a").is_err());
        assert!(http_url("file:///C:/x").is_err());
        assert!(http_url("not a url").is_err());
        let origin = origin_of(&Url::parse("https://[::1]/x").unwrap()).unwrap();
        assert_eq!(origin, Origin { host: "::1".into(), port: 443, secure: true });
    }

    #[test]
    fn request_is_origin_form_with_host() {
        let url = Url::parse("http://example.com:8080/a%20b?x=1#frag").unwrap();
        let request = build_request(&url, &HeaderMap::new()).unwrap();
        assert_eq!(request.uri(), "/a%20b?x=1");
        assert_eq!(request.headers()[HOST], "example.com:8080");
        let url = Url::parse("http://[::1]/").unwrap();
        assert_eq!(build_request(&url, &HeaderMap::new()).unwrap().headers()[HOST], "[::1]");
    }
}
