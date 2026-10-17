//! Which version of a file a server is serving. Each block is fetched by its own request, so a
//! file republished mid-download would otherwise be stitched together from two versions and still
//! pass every length check.

use serde::{Deserialize, Serialize};

/// What a response says about the version of the file it carries.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileVersion {
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    /// `None` when the response didn't say.
    pub total_bytes: Option<u64>,
}

/// How a response's version compares with the ones a download has accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VersionCheck {
    /// Nothing suggests a different file.
    Same,
    /// The size differs. Identical bytes can't have a different size, so this is proof.
    Size { detail: String },
    /// The ETag or Last-Modified differs. Only a hint — load-balanced servers can label identical
    /// bytes differently — so it has to be settled by comparing bytes.
    Validators { detail: String },
}

/// Drops what varies between servers without the bytes changing: the weak-validator marker, the
/// quotes, and the suffix some servers add for a compressed variant (Apache's `-gzip`).
pub fn normalize_etag(etag: &str) -> &str {
    let mut tag = etag.trim();
    if tag.len() >= 2 && tag[..2].eq_ignore_ascii_case("W/") {
        tag = &tag[2..];
    }
    if tag.len() >= 2 && tag.starts_with('"') && tag.ends_with('"') {
        tag = &tag[1..tag.len() - 1];
    }
    for suffix in ["gzip", "br", "deflate", "zstd"] {
        for separator in ['-', ';'] {
            if let Some(stem) = tag.strip_suffix(suffix).and_then(|t| t.strip_suffix(separator)) {
                return stem;
            }
        }
    }
    tag
}

/// Whether an ETag is a strong validator (not `W/…`).
pub fn is_strong_etag(etag: &str) -> bool {
    let tag = etag.trim();
    !(tag.len() >= 2 && tag[..2].eq_ignore_ascii_case("W/"))
}

/// Compares `seen` with `accepted`: the version the download started on (first), plus any since
/// confirmed to serve identical bytes.
pub fn compare_version(accepted: &[FileVersion], seen: &FileVersion) -> VersionCheck {
    let expected = accepted.first().and_then(|v| v.total_bytes).filter(|&n| n > 0);
    if let (Some(expected), Some(now)) = (expected, seen.total_bytes.filter(|&n| n > 0))
        && now != expected
    {
        return VersionCheck::Size {
            detail: format!("it is now {now} bytes instead of {expected}"),
        };
    }

    let etags: Vec<&str> = accepted.iter().filter_map(|v| v.etag.as_deref()).collect();
    if let Some(seen_etag) = &seen.etag
        && !etags.is_empty()
    {
        let normalized = normalize_etag(seen_etag);
        return if etags.iter().any(|e| normalize_etag(e) == normalized) {
            VersionCheck::Same
        } else {
            VersionCheck::Validators { detail: format!("its ETag is now {seen_etag}") }
        };
    }

    let dates: Vec<&str> = accepted.iter().filter_map(|v| v.last_modified.as_deref()).collect();
    if let Some(seen_date) = &seen.last_modified
        && !dates.is_empty()
        && !dates.contains(&seen_date.as_str())
    {
        return VersionCheck::Validators { detail: format!("it was modified at {seen_date}") };
    }
    VersionCheck::Same
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(etag: Option<&str>, date: Option<&str>, size: Option<u64>) -> FileVersion {
        FileVersion {
            etag: etag.map(Into::into),
            last_modified: date.map(Into::into),
            total_bytes: size,
        }
    }

    #[test]
    fn etags_normalize_across_servers() {
        assert_eq!(normalize_etag(r#"W/"abc""#), "abc");
        assert_eq!(normalize_etag(r#""abc-gzip""#), "abc");
        assert_eq!(normalize_etag("abc;br"), "abc");
        assert_eq!(normalize_etag("  abc  "), "abc");
        assert!(is_strong_etag(r#""x""#));
        assert!(!is_strong_etag(r#"w/"x""#));
    }

    #[test]
    fn size_change_is_proof() {
        let accepted = [version(Some("a"), None, Some(100))];
        assert!(matches!(
            compare_version(&accepted, &version(Some("a"), None, Some(101))),
            VersionCheck::Size { .. }
        ));
        // An unknown size on either side proves nothing.
        assert_eq!(compare_version(&accepted, &version(Some("a"), None, None)), VersionCheck::Same);
    }

    #[test]
    fn validators_are_a_hint() {
        let accepted = [version(Some("a"), Some("d1"), Some(100))];
        assert!(matches!(
            compare_version(&accepted, &version(Some("b"), None, Some(100))),
            VersionCheck::Validators { .. }
        ));
        // A matching ETag wins over a differing date.
        assert_eq!(
            compare_version(&accepted, &version(Some("W/\"a\""), Some("d2"), Some(100))),
            VersionCheck::Same
        );
        // No ETag to compare: the date decides.
        let dated = [version(None, Some("d1"), Some(100))];
        assert!(matches!(
            compare_version(&dated, &version(None, Some("d2"), Some(100))),
            VersionCheck::Validators { .. }
        ));
        assert_eq!(compare_version(&dated, &version(None, None, Some(100))), VersionCheck::Same);
    }

    #[test]
    fn confirmed_versions_are_accepted() {
        let accepted = [version(Some("a"), None, Some(100)), version(Some("b"), None, Some(100))];
        assert_eq!(compare_version(&accepted, &version(Some("b"), None, None)), VersionCheck::Same);
    }
}
