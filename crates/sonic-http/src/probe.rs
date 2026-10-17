//! What a link would download, found out by asking the server for its first bytes.

use std::time::Duration;

use hyper::StatusCode;
use hyper::header::{
    CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, HeaderMap, HeaderValue,
    LAST_MODIFIED, LOCATION, RANGE,
};
use percent_encoding::percent_decode_str;
use sonic_core::{BrowserContext, ProbeResult};
use url::Url;

use crate::MAX_REDIRECTS;
use crate::connection::{ConnectionError, StreamConnection, http_url, origin_of};
use crate::disposition::content_disposition_filename;
use crate::fetch::{header_str, parse_content_range};
use crate::headers::request_headers;

/// The most a whole probe may take, redirects included, so a chain of slow hops can't stretch
/// it — and a server that never answers doesn't leave "Checking…" up forever.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(20);

/// Not 1 byte: some file hosts answer a range ending at byte 0 or 1 with the whole file, while
/// serving every real block as asked.
const PROBE_RANGE: &str = "bytes=0-1023";

/// Why a link couldn't be probed.
#[derive(Debug, thiserror::Error)]
pub enum ProbeError {
    #[error("not a link that can be downloaded: {0}")]
    InvalidUrl(String),
    #[error(transparent)]
    Connection(#[from] ConnectionError),
    #[error("the server answered with status {0}")]
    Status(u16),
    #[error("too many redirects")]
    TooManyRedirects,
    #[error("the server did not respond in time")]
    Timeout,
}

/// Asks for the link's first bytes (`GET` with `Range: bytes=0-1023`): cheaper than the body
/// and, unlike `HEAD`, the answer (206 or not) shows whether range requests actually work.
/// Follows up to [`MAX_REDIRECTS`] redirects; the body is dropped once the headers arrive.
///
/// - A 403 to the range request is asked once more without one: some servers refuse any Range
///   while serving the same URL in full. The file then downloads as one stream.
/// - A 416 with `Content-Range: bytes */0` is an empty file, not an error.
/// - `supports_ranges` only on a 206 — a 200 means the Range was ignored, whatever
///   `Accept-Ranges` claims.
/// - The size comes from Content-Range's total, else Content-Length on a 200 only (on a 206 it
///   is the length of the part).
pub async fn probe(
    url: &str,
    browser: Option<&BrowserContext>,
    conn: &mut StreamConnection,
) -> Result<ProbeResult, ProbeError> {
    tokio::time::timeout(PROBE_TIMEOUT, probe_following(url, browser, conn))
        .await
        .unwrap_or(Err(ProbeError::Timeout))
}

async fn probe_following(
    requested: &str,
    browser: Option<&BrowserContext>,
    conn: &mut StreamConnection,
) -> Result<ProbeResult, ProbeError> {
    let mut current = http_url(requested).map_err(ProbeError::InvalidUrl)?;
    let mut ranged = true;
    let mut redirects = 0;
    loop {
        let (mut status, mut headers) = first_bytes(conn, &current, browser, ranged).await?;
        if status == StatusCode::FORBIDDEN && ranged {
            ranged = false;
            (status, headers) = first_bytes(conn, &current, browser, ranged).await?;
        }
        if status.is_redirection() {
            let location =
                header_str(&headers, LOCATION).ok_or(ProbeError::Status(status.as_u16()))?;
            if redirects == MAX_REDIRECTS {
                return Err(ProbeError::TooManyRedirects);
            }
            redirects += 1;
            let next = current
                .join(location)
                .map_err(|e| ProbeError::InvalidUrl(format!("redirect to {location}: {e}")))?;
            origin_of(&next).map_err(ProbeError::InvalidUrl)?;
            current = next;
            continue;
        }
        return describe(requested, &current, status, &headers);
    }
}

async fn first_bytes(
    conn: &mut StreamConnection,
    url: &Url,
    browser: Option<&BrowserContext>,
    ranged: bool,
) -> Result<(StatusCode, HeaderMap), ProbeError> {
    let mut headers = request_headers(url, browser);
    if ranged {
        headers.insert(RANGE, HeaderValue::from_static(PROBE_RANGE));
    }
    let start = conn.request(url, &headers).await?;
    // The body is dropped unread; its connection closes with it.
    let (parts, _body) = start.response.into_parts();
    Ok((parts.status, parts.headers))
}

fn describe(
    requested: &str,
    url: &Url,
    status: StatusCode,
    headers: &HeaderMap,
) -> Result<ProbeResult, ProbeError> {
    let content_range = header_str(headers, CONTENT_RANGE);
    let empty = status == StatusCode::RANGE_NOT_SATISFIABLE
        && content_range.is_some_and(is_empty_unsatisfied_range);
    if !empty && !status.is_success() {
        return Err(ProbeError::Status(status.as_u16()));
    }
    let total_bytes = if empty {
        Some(0)
    } else {
        content_range.and_then(parse_content_range).and_then(|served| served.total).or_else(|| {
            (status == StatusCode::OK)
                .then(|| header_str(headers, CONTENT_LENGTH)?.trim().parse().ok())
                .flatten()
        })
    };
    let text = |name| header_str(headers, name).map(str::to_owned);
    Ok(ProbeResult {
        requested_url: requested.to_owned(),
        final_url: url.to_string(),
        supports_ranges: status == StatusCode::PARTIAL_CONTENT,
        total_bytes,
        suggested_file_name: file_name(headers, url),
        content_type: text(CONTENT_TYPE),
        etag: text(ETAG),
        last_modified: text(LAST_MODIFIED),
    })
}

/// `bytes */0`: the server's way of saying a file is empty when asked for its first bytes.
fn is_empty_unsatisfied_range(value: &str) -> bool {
    let value = value.trim();
    value.get(..5).is_some_and(|unit| unit.eq_ignore_ascii_case("bytes"))
        && value[5..].split_whitespace().collect::<String>() == "*/0"
}

/// From Content-Disposition, else the URL's last path segment, percent-decoded, else
/// "download". Not yet fit for the file system: see [`crate::sanitize_file_name`].
fn file_name(headers: &HeaderMap, url: &Url) -> String {
    if let Some(name) =
        header_str(headers, CONTENT_DISPOSITION).and_then(content_disposition_filename)
    {
        return name;
    }
    let segment = url.path_segments().and_then(|mut segments| segments.rfind(|s| !s.is_empty()));
    match segment {
        // A malformed escape or non-UTF-8 bytes: the raw segment still names the file well enough.
        Some(segment) => percent_decode_str(segment)
            .decode_utf8()
            .map_or_else(|_| segment.to_owned(), |name| name.into_owned()),
        None => "download".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_file_range() {
        assert!(is_empty_unsatisfied_range("bytes */0"));
        assert!(is_empty_unsatisfied_range(" Bytes */0 "));
        assert!(!is_empty_unsatisfied_range("bytes */10"));
        assert!(!is_empty_unsatisfied_range("bytes 0-0/0"));
    }

    #[test]
    fn name_from_url() {
        let name = |url: &str| file_name(&HeaderMap::new(), &Url::parse(url).unwrap());
        assert_eq!(name("http://a.com/files/My%20File.zip?x=1"), "My File.zip");
        assert_eq!(name("http://a.com/files/dir/"), "dir");
        assert_eq!(name("http://a.com/"), "download");
        assert_eq!(name("http://a.com/%E6%97%A5.txt"), "日.txt");
        assert_eq!(name("http://a.com/bad%FF.txt"), "bad%FF.txt");
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_DISPOSITION, HeaderValue::from_static("attachment; filename=x.bin"));
        assert_eq!(file_name(&headers, &Url::parse("http://a.com/y.zip").unwrap()), "x.bin");
    }
}
