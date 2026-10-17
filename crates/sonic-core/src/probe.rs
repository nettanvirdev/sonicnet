use serde::{Deserialize, Serialize};

/// What a link would download, found out by asking the server for its first bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeResult {
    pub requested_url: String,
    /// The URL after following redirects: what the download fetches.
    pub final_url: String,
    /// The server answered a range request with 206 Partial Content.
    pub supports_ranges: bool,
    /// `None` when the server did not report a size.
    pub total_bytes: Option<u64>,
    /// From Content-Disposition, else the URL's last path segment, else "download". Not yet
    /// sanitised for the file system.
    pub suggested_file_name: String,
    pub content_type: Option<String>,
    /// Validators, used to detect the file changing on the server between requests.
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}
