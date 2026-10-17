//! HTTP over one network: probing a link, fetching a byte range, the staging file.
//!
//! - [`connection`]: [`StreamConnection`], one stream's kept-alive HTTP/1.1 connection through
//!   one network.
//! - [`headers`]: the request headers a link is fetched with (User-Agent, Referer, cookies).
//! - [`probe`]: what a link would download — size, range support, name, validators.
//! - [`fetch`]: one byte range into the staging file, with every check a response must pass
//!   before a byte of it is written.
//! - [`staging`]: the `<name>.sonic` file a download is written into, and publishing it.
//! - [`disposition`]: the file name in a `Content-Disposition` header.
//!
//! The rule everything here serves: a byte lands in the staging file only once the response it
//! came in has been shown to carry exactly those bytes of exactly that file. A download that
//! fails is retried; one that writes the wrong bytes is silently corrupt.

pub mod connection;
pub mod disposition;
pub mod fetch;
pub mod headers;
pub mod probe;
pub mod staging;

pub use connection::{ConnectionError, ResponseStart, StreamConnection};
pub use fetch::{FetchError, FetchEvents, FetchRequest, ResponseInfo, fetch, fetch_sample};
pub use headers::{
    DEFAULT_USER_AGENT, InvalidBrowserContext, request_headers, validate_browser_context,
};
pub use probe::{PROBE_TIMEOUT, ProbeError, probe};
pub use staging::{StagingFile, StagingWriter, WRITE_BUFFER_BYTES, reserve, sanitize_file_name};

/// How many redirects one request follows. A CDN re-signing a URL takes one or two; a loop
/// takes all of them.
pub const MAX_REDIRECTS: usize = 5;
