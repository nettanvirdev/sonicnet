//! Fetching one byte range of a file into the staging file.
//!
//! A block's bytes land at a fixed offset in a file other blocks share, so a response that
//! starts somewhere else, overruns its range, or is of another version of the file would corrupt
//! the output rather than just fail the block. Every one of those is checked before the first
//! byte is written, and the body is cut off at the range's end. Compression is prevented by
//! asking for `Accept-Encoding: identity` (see `headers`); a `Content-Encoding` the server sends
//! anyway is not refused, because servers routinely label `.gz` / `.tar.gz` files that way while
//! sending the file's own bytes — and a range of truly re-encoded bytes fails the length checks. A
//! failed fetch keeps what it wrote — every byte of it was checked — and reports it, so whoever
//! takes the block next resumes after it.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use bytes::{Bytes, BytesMut};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::header::{
    CONTENT_LENGTH, CONTENT_RANGE, ETAG, HeaderMap, HeaderValue, LAST_MODIFIED, LOCATION, RANGE,
    RETRY_AFTER,
};
use hyper::{Response, StatusCode};
use sonic_core::{BrowserContext, ByteRange, FileVersion, VersionCheck, compare_version};
use tokio_util::sync::CancellationToken;

use crate::MAX_REDIRECTS;
use crate::connection::{ConnectionError, ResponseStart, StreamConnection, http_url, origin_of};
use crate::headers::request_headers;
use crate::staging::{StagingFile, StagingWriter};

/// Why a fetch failed.
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    /// No response, or the connection dropped or went silent midway: says something about the
    /// network, not the server.
    #[error(transparent)]
    Connection(#[from] ConnectionError),
    /// The server answered with a status other than the range asked for.
    #[error("the server answered with status {code}")]
    Status {
        code: u16,
        /// How long the server asked to be left alone (`Retry-After`), if it said.
        retry_after: Option<Duration>,
    },
    /// The response is of another version of the file than the download's. Nothing from it was
    /// written; the caller decides whether it's a new file (fail) or the same bytes under
    /// another label (compare a sample, then accept `seen`).
    #[error("the file on the server changed during the download ({})", describe(.check))]
    RemoteChanged { check: VersionCheck, seen: FileVersion },
    /// The server answered wrongly: a range other than asked, a body too long or too short, a
    /// redirect loop.
    #[error("{0}")]
    Protocol(String),
    /// Writing to the staging file failed.
    #[error("could not write the file: {0}")]
    Write(#[source] io::Error),
    #[error("cancelled")]
    Cancelled,
}

fn describe(check: &VersionCheck) -> &str {
    match check {
        VersionCheck::Same => "no change",
        VersionCheck::Size { detail } | VersionCheck::Validators { detail } => detail,
    }
}

impl FetchError {
    /// A server that is busy, briefly broken or limiting requests: worth waiting out, not a sign
    /// that asking again will never work. The statuses curl's `--retry` treats as transient.
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Status { code: 408 | 429 | 500 | 502 | 503 | 504, .. })
    }
}

/// `Retry-After` (RFC 9110 §10.2.3), in seconds or as an HTTP date, as a wait from `now`.
pub fn parse_retry_after(value: &str, now: SystemTime) -> Option<Duration> {
    let value = value.trim();
    if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) {
        return value.parse().ok().map(Duration::from_secs);
    }
    let at = httpdate::parse_http_date(value).ok()?;
    Some(at.duration_since(now).unwrap_or(Duration::ZERO))
}

/// What a fetch asks for.
#[derive(Clone, Debug)]
pub struct FetchRequest<'a> {
    pub url: &'a str,
    pub browser: Option<&'a BrowserContext>,
    /// The bytes wanted, half-open; `end: None` runs to the end of the file.
    pub range: ByteRange,
    /// The server doesn't take range requests: ask for the whole file with no `Range` header
    /// (only meaningful from byte 0). Some servers refuse any `Range` with 403.
    pub whole_file: bool,
    /// The version the download started on (first), plus any since confirmed to serve identical
    /// bytes. Empty accepts any.
    pub accepted_versions: &'a [FileVersion],
    /// How long the body may go without a byte before the connection is given up on. Time spent
    /// waiting on the disk or a speed limit doesn't count.
    pub stall_timeout: Duration,
    /// Where to stop, as a file offset, if before the range's end: lowered while the fetch runs
    /// when the rest of a long range is handed to someone else. Reaching it ends the fetch with
    /// what was written up to it (the connection, with the rest of the body unread, is not
    /// reused).
    pub cut: Option<&'a AtomicU64>,
}

/// How a response that was accepted came in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResponseInfo {
    /// Request sent to response headers (of the last redirect hop).
    pub ttfb: Duration,
    /// It went out on a connection an earlier request had already warmed up.
    pub reused: bool,
}

/// What a fetch tells its caller as it goes. Every method has a do-nothing default; `()` does
/// nothing at all.
pub trait FetchEvents {
    /// The response passed every check; its body is about to be read.
    fn response(&mut self, _info: ResponseInfo) {}
    /// `bytes` more body bytes came off the network (all of them, a cut-off overrun included):
    /// for speed meters and data usage.
    fn received(&mut self, _bytes: u64) {}
    /// Bytes of the range, from its start, now secured in the staging file: the block's
    /// frontier, safe to persist as progress. Only ever grows.
    fn written(&mut self, _total: u64) {}
    /// Whether reading waits on the disk: the writer is full, or writing out the last bytes.
    fn write_wait(&mut self, _waiting: bool) {}
    /// Told of every `bytes` received; answers how long to stop reading (a speed limit).
    fn throttle(&mut self, _bytes: u64) -> Duration {
        Duration::ZERO
    }
}

impl FetchEvents for () {}

/// `Content-Range: bytes <first>-<last>/<total|*>` (inclusive `last`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ServedRange {
    pub first: u64,
    pub last: u64,
    pub total: Option<u64>,
}

pub(crate) fn parse_content_range(value: &str) -> Option<ServedRange> {
    let value = value.trim();
    let unit = value.get(..6)?;
    if !unit.eq_ignore_ascii_case("bytes ") {
        return None;
    }
    let (span, total) = value[6..].trim_start().split_once('/')?;
    let (first, last) = span.split_once('-')?;
    let number = |s: &str| {
        let s = s.trim();
        (!s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())).then(|| s.parse().ok()).flatten()
    };
    let (first, last) = (number(first)?, number(last)?);
    let total = if total.trim() == "*" { None } else { Some(number(total)?) };
    (first <= last).then_some(ServedRange { first, last, total })
}

pub(crate) fn header_str(
    headers: &HeaderMap,
    name: impl hyper::header::AsHeaderName,
) -> Option<&str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

/// The version a response says it is of. A 206 states the file's size in Content-Range; a 200
/// is the whole file, so its length is the size.
fn version_of(status: StatusCode, headers: &HeaderMap, served: Option<ServedRange>) -> FileVersion {
    let length = || header_str(headers, CONTENT_LENGTH).and_then(|l| l.trim().parse().ok());
    FileVersion {
        etag: header_str(headers, ETAG).map(str::to_owned),
        last_modified: header_str(headers, LAST_MODIFIED).map(str::to_owned),
        total_bytes: served
            .and_then(|s| s.total)
            .or_else(|| (status == StatusCode::OK).then(length).flatten()),
    }
}

fn status_error(response: &Response<Incoming>) -> FetchError {
    FetchError::Status {
        code: response.status().as_u16(),
        retry_after: header_str(response.headers(), RETRY_AFTER)
            .and_then(|v| parse_retry_after(v, SystemTime::now())),
    }
}

/// Sends the request, following up to [`MAX_REDIRECTS`] redirects — a CDN may re-sign a URL
/// mid-download. Headers (cookies above all) are rebuilt for every hop. Returns the first
/// response that isn't a redirect.
async fn send_following(
    conn: &mut StreamConnection,
    url: &str,
    browser: Option<&BrowserContext>,
    range: Option<&str>,
    cancel: &CancellationToken,
) -> Result<ResponseStart, FetchError> {
    let mut current = http_url(url).map_err(FetchError::Protocol)?;
    for _ in 0..=MAX_REDIRECTS {
        let mut headers = request_headers(&current, browser);
        if let Some(range) = range {
            headers.insert(
                RANGE,
                HeaderValue::from_str(range).map_err(|e| FetchError::Protocol(e.to_string()))?,
            );
        }
        let start = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(FetchError::Cancelled),
            start = conn.request(&current, &headers) => start?,
        };
        if !start.response.status().is_redirection() {
            return Ok(start);
        }
        let Some(location) = header_str(start.response.headers(), LOCATION) else {
            return Err(status_error(&start.response));
        };
        let next = current
            .join(location)
            .map_err(|e| FetchError::Protocol(format!("bad redirect to {location}: {e}")))?;
        origin_of(&next).map_err(FetchError::Protocol)?;
        current = next;
    }
    Err(FetchError::Protocol("too many redirects".into()))
}

/// Fetches `request.range` of the file into `file` at the range's offset.
///
/// `Ok(n)`: exactly the range was written, `n` bytes (for an open-ended range, through the end
/// of the file), and nothing else. Accepted are a 206 whose Content-Range starts where asked and
/// ends within the range, or a 200 when asking from byte 0 (whole-file mode, or a server that
/// ignored the Range — then only the range's length is kept). The response's version must pass
/// [`compare_version`] against `accepted_versions` before anything is written.
///
/// On failure what was written stays written and was reported through
/// [`FetchEvents::written`]. Cancelling `cancel` stops it at the next await, after finishing
/// the writes already queued; dropping the future stops it at once (the writes in flight still
/// land, unreported — progress is only ever under-counted).
pub async fn fetch(
    conn: &mut StreamConnection,
    request: &FetchRequest<'_>,
    file: &StagingFile,
    events: &mut impl FetchEvents,
    cancel: &CancellationToken,
) -> Result<u64, FetchError> {
    let range = request.range;
    if range.is_empty() {
        return Ok(0);
    }
    // The whole file from the start needs no Range at all — and an empty file would answer
    // `bytes=0-` with 416, having no byte 0.
    let whole = range.start == 0 && (request.whole_file || range.end.is_none());
    let range_header = if whole { None } else { range.http_header() };
    let start =
        send_following(conn, request.url, request.browser, range_header.as_deref(), cancel).await?;
    let response = start.response;
    let status = response.status();

    // A 200 is the whole file from byte 0: right only when asking from byte 0. Otherwise the
    // server ignored the Range, and its bytes belong elsewhere.
    if status != StatusCode::PARTIAL_CONTENT && !(status == StatusCode::OK && range.start == 0) {
        return Err(status_error(&response));
    }

    // Each range is fetched by its own request, so a file republished mid-download would
    // otherwise be stitched together from two versions and still pass every length check.
    let served = header_str(response.headers(), CONTENT_RANGE).and_then(parse_content_range);
    let seen = version_of(status, response.headers(), served);
    let check = compare_version(request.accepted_versions, &seen);
    if check != VersionCheck::Same {
        return Err(FetchError::RemoteChanged { check, seen });
    }

    // How many bytes to keep. A 206 says where its bytes belong: it must start where asked and
    // not run past the range, and it may not deliver more than it says.
    let mut expected = range.len();
    if status == StatusCode::PARTIAL_CONTENT {
        let served = served.ok_or_else(|| {
            FetchError::Protocol("the server sent a 206 without a usable Content-Range".into())
        })?;
        if served.first != range.start {
            return Err(FetchError::Protocol(format!(
                "the server sent the wrong range: asked for byte {}, got {}",
                range.start, served.first
            )));
        }
        let served_end = served.last + 1;
        if range.end.is_some_and(|end| served_end > end) {
            return Err(FetchError::Protocol(format!(
                "the server sent more than the range: through byte {}, asked through {}",
                served.last,
                range.end.unwrap_or_default() - 1
            )));
        }
        expected =
            Some(expected.map_or(served_end - range.start, |e| e.min(served_end - range.start)));
    }

    events.response(ResponseInfo { ttfb: start.ttfb, reused: start.reused });
    let mut writer = file.writer(range.start).map_err(FetchError::Write)?;
    let body = response.into_body();
    let cut = request.cut.map(|cut| Cut { at: cut, start: range.start });
    let pumped =
        pump(body, &mut writer, expected, cut, request.stall_timeout, events, cancel).await;
    // Whatever happened, what was handed to the writer was checked: write it out and count it.
    // Closing the file isn't the disk falling behind, but the stream does wait on it.
    events.write_wait(true);
    let finished = writer.finish().await;
    events.write_wait(false);
    events.written(writer.secured());
    let received = pumped?;
    finished.map_err(FetchError::Write)?;
    // Short: the range's length when it has one, else what the 206 said it would send — or less,
    // if it was cut.
    let wanted = range.len().or(expected).map(|wanted| cut.map_or(wanted, |c| wanted.min(c.len())));
    if let Some(wanted) = wanted.filter(|&wanted| received < wanted) {
        return Err(FetchError::Protocol(format!(
            "the server sent {received} bytes for a {wanted}-byte range"
        )));
    }
    Ok(received)
}

/// [`FetchRequest::cut`], for a range starting at `start`.
#[derive(Clone, Copy)]
struct Cut<'a> {
    at: &'a AtomicU64,
    start: u64,
}

impl Cut<'_> {
    /// How many of the range's bytes to keep, as of now.
    fn len(self) -> u64 {
        self.at.load(Ordering::Relaxed).saturating_sub(self.start)
    }
}

/// Reads the body into `writer`, keeping at most `expected` bytes, or up to the cut. Returns how
/// many it kept.
async fn pump(
    mut body: Incoming,
    writer: &mut StagingWriter,
    expected: Option<u64>,
    cut: Option<Cut<'_>>,
    stall_timeout: Duration,
    events: &mut impl FetchEvents,
    cancel: &CancellationToken,
) -> Result<u64, FetchError> {
    let mut kept = 0u64;
    let mut reported = 0u64;
    loop {
        // The stall clock runs only while waiting on the network: never while the disk or a
        // speed limit holds reading, since neither says anything about the connection.
        let frame = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(FetchError::Cancelled),
            frame = tokio::time::timeout(stall_timeout, body.frame()) => frame,
        };
        let data = match frame {
            Err(_) => return Err(ConnectionError::Stalled.into()),
            Ok(None) => break,
            Ok(Some(Err(error))) => return Err(ConnectionError::Http(error).into()),
            Ok(Some(Ok(frame))) => match frame.into_data() {
                Ok(data) => data,
                Err(_) => continue, // trailers
            },
        };
        let len = data.len() as u64;
        events.received(len);
        // Told to stop on hearing of these bytes (a data allowance used up): not one is kept.
        if cancel.is_cancelled() {
            return Err(FetchError::Cancelled);
        }

        let cut_at = cut.map(Cut::len);
        let limit = match (expected, cut_at) {
            (Some(e), Some(c)) => Some(e.min(c)),
            (e, c) => e.or(c),
        };
        let usable = limit.map_or(len, |l| len.min(l.saturating_sub(kept)));
        if usable > 0 {
            kept += usable;
            // `usable <= data.len()`, so the cast is lossless.
            if !writer.write(&data[..usable as usize]).map_err(FetchError::Write)? {
                events.write_wait(true);
                let drained = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => Err(FetchError::Cancelled),
                    drained = writer.drain() => drained.map_err(FetchError::Write),
                };
                events.write_wait(false);
                drained?;
            }
            if writer.secured() > reported {
                reported = writer.secured();
                events.written(reported);
            }
        }
        // Reached the cut: the rest of the range is someone else's now.
        if usable < len && cut_at.is_some_and(|c| kept >= c) {
            return Ok(kept);
        }
        // An overrun would land on the next block: cut at the boundary, and fail the response.
        if usable < len {
            return Err(FetchError::Protocol(
                "the server sent more data than the requested range".into(),
            ));
        }
        // At the cut before the range's end: stop now, not on the next frame.
        if cut_at.is_some_and(|c| kept >= c) && expected.is_none_or(|e| kept < e) {
            return Ok(kept);
        }

        let wait = events.throttle(len);
        if !wait.is_zero() {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(FetchError::Cancelled),
                _ = tokio::time::sleep(wait) => {}
            }
        }
    }
    Ok(kept)
}

/// Fetches a bounded `range` into memory and reports the version that served it — to compare
/// bytes already on disk with what the server serves now, when only a validator changed.
/// Requires a 206 starting where asked and exactly the range's length.
pub async fn fetch_sample(
    conn: &mut StreamConnection,
    url: &str,
    browser: Option<&BrowserContext>,
    range: ByteRange,
    stall_timeout: Duration,
) -> Result<(Bytes, FileVersion), FetchError> {
    let (Some(len), Some(header)) = (range.len(), range.http_header()) else {
        return Err(FetchError::Protocol("a sample needs a bounded, non-empty range".into()));
    };
    let never = CancellationToken::new();
    let start = send_following(conn, url, browser, Some(&header), &never).await?;
    let response = start.response;
    let status = response.status();
    if status != StatusCode::PARTIAL_CONTENT {
        return Err(status_error(&response));
    }
    let served = header_str(response.headers(), CONTENT_RANGE).and_then(parse_content_range);
    if served.map(|s| s.first) != Some(range.start) {
        return Err(FetchError::Protocol("the sample came back from the wrong offset".into()));
    }
    let version = version_of(status, response.headers(), served);
    let mut body = response.into_body();
    let mut bytes = BytesMut::new();
    loop {
        let frame = tokio::time::timeout(stall_timeout, body.frame())
            .await
            .map_err(|_| ConnectionError::Stalled)?;
        match frame {
            None => break,
            Some(Err(error)) => return Err(ConnectionError::Http(error).into()),
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    bytes.extend_from_slice(&data);
                    if bytes.len() as u64 > len {
                        return Err(FetchError::Protocol("the sample came back too long".into()));
                    }
                }
            }
        }
    }
    if bytes.len() as u64 != len {
        return Err(FetchError::Protocol("the sample came back short".into()));
    }
    Ok((bytes.freeze(), version))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_range_forms() {
        let r = |first, last, total| Some(ServedRange { first, last, total });
        assert_eq!(parse_content_range("bytes 0-9/100"), r(0, 9, Some(100)));
        assert_eq!(parse_content_range(" Bytes 5-5/*"), r(5, 5, None));
        assert_eq!(parse_content_range("bytes 9-0/100"), None);
        assert_eq!(parse_content_range("bytes */100"), None);
        assert_eq!(parse_content_range("items 0-9/100"), None);
        assert_eq!(parse_content_range("bytes -1-9/100"), None);
        assert_eq!(parse_content_range("bytes 0-9/abc"), None);
    }

    #[test]
    fn retry_after_seconds_or_date() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000_000);
        assert_eq!(parse_retry_after("120", now), Some(Duration::from_secs(120)));
        assert_eq!(parse_retry_after(" 0 ", now), Some(Duration::ZERO));
        let later = httpdate::fmt_http_date(now + Duration::from_secs(90));
        assert_eq!(parse_retry_after(&later, now), Some(Duration::from_secs(90)));
        let past = httpdate::fmt_http_date(now - Duration::from_secs(90));
        assert_eq!(parse_retry_after(&past, now), Some(Duration::ZERO));
        assert_eq!(parse_retry_after("soon", now), None);
        assert_eq!(parse_retry_after("-5", now), None);
    }

    #[test]
    fn transient_statuses() {
        let status = |code| FetchError::Status { code, retry_after: None };
        for code in [408, 429, 500, 502, 503, 504] {
            assert!(status(code).is_transient(), "{code}");
        }
        for code in [200, 401, 403, 404, 410, 416, 501] {
            assert!(!status(code).is_transient(), "{code}");
        }
        assert!(!FetchError::Protocol("x".into()).is_transient());
    }
}
