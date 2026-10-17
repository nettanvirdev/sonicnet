//! A small origin server for the tests: serves one file of deterministic bytes over raw
//! HTTP/1.1 (keep-alive), and misbehaves on request in the ways real servers and networks do.
//! Raw TCP rather than a server library so a fault can break the protocol on purpose.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sonic_http::StreamConnection;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

/// Deterministic, non-repeating-looking bytes, so a byte at the wrong offset shows.
pub fn seeded_bytes(size: usize, seed: u32) -> Vec<u8> {
    let mut state = seed.max(1);
    (0..size)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        })
        .collect()
}

/// A connection through the loopback "network", with short timeouts.
pub fn connection() -> StreamConnection {
    StreamConnection::new(
        || Some(sonic_net::loopback()),
        Duration::from_secs(2),
        Duration::from_secs(2),
    )
}

#[derive(Clone, Debug)]
pub struct Served {
    pub bytes: Arc<Vec<u8>>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_disposition: Option<String>,
}

impl Served {
    pub fn new(size: usize) -> Self {
        Self {
            bytes: Arc::new(seeded_bytes(size, 7)),
            etag: Some("\"v1\"".into()),
            last_modified: Some("Wed, 21 Oct 2015 07:28:00 GMT".into()),
            content_disposition: None,
        }
    }
}

/// One request as the server saw it.
#[derive(Clone, Debug)]
pub struct Req {
    /// 1-based count of requests the server has seen.
    pub n: usize,
    /// Which TCP connection it came on, 1-based in the order they opened.
    pub connection: usize,
    pub path: String,
    pub headers: Vec<(String, String)>,
    /// The Range asked for: first byte and inclusive last.
    pub range: Option<(u64, Option<u64>)>,
}

impl Req {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
    }
}

/// What the server does with one request. Anything but `Ok` is a fault.
#[derive(Clone, Debug)]
pub enum Fault {
    Ok,
    /// 200 with the whole file, as a server without range support.
    IgnoreRange,
    /// 206 whose Content-Range starts one byte later than asked.
    WrongStart,
    /// 206 with a correct header but one byte more body.
    Overlong,
    /// 206 without Content-Range.
    NoContentRange,
    /// 206 whose Content-Range (and body) run 10 bytes past the range asked for.
    OverrunHeader,
    /// Headers, then nothing, forever.
    StallBody,
    /// Never answers.
    StallHeaders,
    /// This status, with these headers and an empty body.
    Status(u16, Vec<(&'static str, String)>),
    /// This many body bytes, then a clean end (a short body with a matching Content-Length).
    EndAfter(usize),
    /// Full Content-Length, this many body bytes, then the connection drops.
    CutAfter(usize),
    /// The body in two halves, this long apart.
    Pieces(Duration),
    /// 302 to this location.
    Redirect(String),
    /// Reads the request and closes the connection without a word.
    Close,
    /// 403 if the request has a Range, else as `Ok`.
    ForbidRange,
}

type Handler = dyn Fn(&Req, &mut Served) -> Fault + Send + Sync;

pub struct Origin {
    pub addr: SocketAddr,
    log: Arc<Mutex<Vec<Req>>>,
    connections: Arc<AtomicUsize>,
}

impl Origin {
    /// Serves `served`; `handler` picks each request's fault and may change the file.
    pub async fn start(
        served: Served,
        handler: impl Fn(&Req, &mut Served) -> Fault + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let log = Arc::new(Mutex::new(Vec::new()));
        let connections = Arc::new(AtomicUsize::new(0));
        let shared = Arc::new(Shared {
            served: Mutex::new(served),
            handler: Box::new(handler),
            log: log.clone(),
            requests: AtomicUsize::new(0),
        });
        let count = connections.clone();
        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else { return };
                let id = count.fetch_add(1, Ordering::SeqCst) + 1;
                tokio::spawn(serve(socket, id, shared.clone()));
            }
        });
        Self { addr, log, connections }
    }

    /// Serves the file well, every time.
    pub async fn ok(served: Served) -> Self {
        Self::start(served, |_, _| Fault::Ok).await
    }

    pub fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.addr.port())
    }

    pub fn requests(&self) -> Vec<Req> {
        self.log.lock().unwrap().clone()
    }

    pub fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
}

struct Shared {
    served: Mutex<Served>,
    handler: Box<Handler>,
    log: Arc<Mutex<Vec<Req>>>,
    requests: AtomicUsize,
}

async fn serve(socket: TcpStream, connection: usize, shared: Arc<Shared>) {
    let (read, mut write) = socket.into_split();
    let mut reader = BufReader::new(read);
    loop {
        let Some((path, headers)) = read_head(&mut reader).await else { return };
        let req = Req {
            n: shared.requests.fetch_add(1, Ordering::SeqCst) + 1,
            connection,
            path,
            range: None,
            headers,
        };
        let req = Req { range: req.header("range").and_then(parse_range), ..req };
        shared.log.lock().unwrap().push(req.clone());
        let (fault, served) = {
            let mut served = shared.served.lock().unwrap();
            let fault = (shared.handler)(&req, &mut served);
            (fault, served.clone())
        };
        let close = req.header("connection").is_some_and(|c| c.eq_ignore_ascii_case("close"));
        if !respond(&mut write, &req, fault, &served).await || close {
            return;
        }
    }
}

async fn read_head(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) -> Option<(String, Vec<(String, String)>)> {
    let mut line = String::new();
    if reader.read_line(&mut line).await.ok()? == 0 {
        return None;
    }
    let path = line.split_whitespace().nth(1)?.to_owned();
    let mut headers = Vec::new();
    loop {
        line.clear();
        if reader.read_line(&mut line).await.ok()? == 0 {
            return None;
        }
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            return Some((path, headers));
        }
        let (name, value) = trimmed.split_once(':')?;
        headers.push((name.trim().to_owned(), value.trim().to_owned()));
    }
}

fn parse_range(value: &str) -> Option<(u64, Option<u64>)> {
    let (first, last) = value.strip_prefix("bytes=")?.split_once('-')?;
    Some((first.parse().ok()?, if last.is_empty() { None } else { Some(last.parse().ok()?) }))
}

/// Writes the response; false when the connection is to close.
async fn respond(
    write: &mut tokio::net::tcp::OwnedWriteHalf,
    req: &Req,
    fault: Fault,
    served: &Served,
) -> bool {
    let len = served.bytes.len() as u64;
    let mut headers: Vec<(String, String)> = Vec::new();
    let mut add = |name: &str, value: String| headers.push((name.to_owned(), value));
    if let Some(etag) = &served.etag {
        add("ETag", etag.clone());
    }
    if let Some(date) = &served.last_modified {
        add("Last-Modified", date.clone());
    }
    if let Some(disposition) = &served.content_disposition {
        add("Content-Disposition", disposition.clone());
    }
    add("Content-Type", "application/octet-stream".into());

    let range = match fault {
        Fault::IgnoreRange => None,
        Fault::ForbidRange if req.range.is_some() => {
            return send(write, 403, &[], &[], None).await;
        }
        _ => req.range,
    };
    let (status, mut body, mut content_range) = match range {
        None => (200, served.bytes.to_vec(), None),
        Some((first, _)) if first >= len => (416, Vec::new(), Some(format!("bytes */{len}"))),
        Some((first, last)) => {
            let last = last.unwrap_or(len - 1).min(len - 1);
            let body = served.bytes[first as usize..=last as usize].to_vec();
            (206, body, Some(format!("bytes {first}-{last}/{len}")))
        }
    };

    match fault {
        Fault::StallHeaders => {
            hold_open().await;
            false
        }
        Fault::Close => false,
        Fault::Status(code, extra) => {
            let extra: Vec<(String, String)> =
                extra.into_iter().map(|(k, v)| (k.to_owned(), v)).collect();
            send(write, code, &extra, &[], None).await
        }
        Fault::Redirect(location) => {
            send(write, 302, &[("Location".into(), location)], &[], None).await
        }
        Fault::StallBody => {
            if let Some(range) = content_range {
                headers.push(("Content-Range".into(), range));
            }
            headers.push(("Content-Length".into(), body.len().to_string()));
            let _ = write.write_all(head(status, &headers).as_bytes()).await;
            hold_open().await;
            false
        }
        Fault::CutAfter(n) => {
            if let Some(range) = content_range {
                headers.push(("Content-Range".into(), range));
            }
            headers.push(("Content-Length".into(), body.len().to_string()));
            let _ = write.write_all(head(status, &headers).as_bytes()).await;
            let _ = write.write_all(&body[..n.min(body.len())]).await;
            let _ = write.flush().await;
            false
        }
        Fault::Pieces(delay) => {
            if let Some(range) = content_range {
                headers.push(("Content-Range".into(), range));
            }
            headers.push(("Content-Length".into(), body.len().to_string()));
            let half = body.len() / 2;
            let _ = write.write_all(head(status, &headers).as_bytes()).await;
            let _ = write.write_all(&body[..half]).await;
            tokio::time::sleep(delay).await;
            write.write_all(&body[half..]).await.is_ok()
        }
        fault => {
            match fault {
                Fault::WrongStart => {
                    if let Some((first, last)) = req.range {
                        let last = last.unwrap_or(len - 1).min(len - 1);
                        content_range = Some(format!("bytes {}-{last}/{len}", first + 1));
                    }
                }
                Fault::Overlong => body.push(0xAB),
                Fault::OverrunHeader => {
                    if let Some((first, Some(last))) = req.range {
                        let last = (last + 10).min(len - 1);
                        body = served.bytes[first as usize..=last as usize].to_vec();
                        content_range = Some(format!("bytes {first}-{last}/{len}"));
                    }
                }
                Fault::NoContentRange => content_range = None,
                Fault::EndAfter(n) => body.truncate(n),
                _ => {}
            }
            send(write, status, &headers, &body, content_range).await
        }
    }
}

fn head(status: u16, headers: &[(String, String)]) -> String {
    let mut head = format!("HTTP/1.1 {status} Test\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    head
}

async fn send(
    write: &mut tokio::net::tcp::OwnedWriteHalf,
    status: u16,
    headers: &[(String, String)],
    body: &[u8],
    content_range: Option<String>,
) -> bool {
    let mut headers = headers.to_vec();
    if let Some(range) = content_range {
        headers.push(("Content-Range".into(), range));
    }
    headers.push(("Content-Length".into(), body.len().to_string()));
    write.write_all(head(status, &headers).as_bytes()).await.is_ok()
        && write.write_all(body).await.is_ok()
}

async fn hold_open() {
    tokio::time::sleep(Duration::from_secs(3600)).await;
}
