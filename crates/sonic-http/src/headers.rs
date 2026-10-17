//! The headers a link is requested with: what the browser it came from would send, so a link
//! that needs a signed-in session, or checks the page it came from, works as it does there.

use std::cmp::Reverse;

use hyper::header::{
    ACCEPT_ENCODING, COOKIE, HeaderMap, HeaderName, HeaderValue, REFERER, USER_AGENT,
};
use sonic_core::{BrowserContext, BrowserCookie};
use url::{Host, Url};

/// The User-Agent sent when no browser supplied one.
pub const DEFAULT_USER_AGENT: &str = concat!("SonicNet/", env!("CARGO_PKG_VERSION"));

/// The headers for a request to `url`. Built again for every redirect hop, so a site's session
/// cookie never follows the download to another host (its CDN), while a cookie set for a whole
/// domain still follows a hop within it.
pub fn request_headers(url: &Url, browser: Option<&BrowserContext>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let agent =
        browser.and_then(|b| b.user_agent.as_deref()).filter(|ua| header_value(ua).is_some());
    insert(&mut headers, USER_AGENT, agent.unwrap_or(DEFAULT_USER_AGENT));
    // Bytes land at their offset in the file: a compressed body would put the wrong bytes there.
    // Without this header a server may pick any encoding (RFC 9110 §12.5.3).
    insert(&mut headers, ACCEPT_ENCODING, "identity");
    if let Some(referer) = browser.and_then(|b| b.referer.as_deref()) {
        insert(&mut headers, REFERER, referer);
    }
    if let Some(cookie) = browser.and_then(|b| cookie_header(url, &b.cookies)) {
        insert(&mut headers, COOKIE, &cookie);
    }
    headers
}

fn header_value(value: &str) -> Option<HeaderValue> {
    HeaderValue::from_bytes(value.as_bytes()).ok()
}

fn insert(headers: &mut HeaderMap, name: HeaderName, value: &str) {
    if let Some(value) = header_value(value) {
        headers.insert(name, value);
    }
}

/// The `Cookie` header for a request to `url` (RFC 6265 §5.4): the cookies whose domain, path
/// and `secure` flag match it, longest path first. `None` when none do.
pub fn cookie_header(url: &Url, cookies: &[BrowserCookie]) -> Option<String> {
    let host = url.host_str()?.to_ascii_lowercase();
    // §5.1.3: only a domain name gets cookies of a parent domain, never an IP address.
    let is_domain = matches!(url.host(), Some(Host::Domain(_)));
    let path = url.path();
    let secure = url.scheme() == "https";
    let mut matched: Vec<&BrowserCookie> = cookies
        .iter()
        .filter(|cookie| {
            let domain = cookie.domain.strip_prefix('.').unwrap_or(&cookie.domain);
            let domain = domain.to_ascii_lowercase();
            let domain_matches = host == domain
                || (!cookie.host_only
                    && is_domain
                    && !domain.is_empty()
                    && host.strip_suffix(&domain).is_some_and(|rest| rest.ends_with('.')));
            domain_matches && path_matches(path, &cookie.path) && (!cookie.secure || secure)
        })
        .collect();
    if matched.is_empty() {
        return None;
    }
    // §5.4 step 2: longer paths first; the sort is stable, so ties keep the browser's order.
    matched.sort_by_key(|cookie| Reverse(cookie.path.len()));
    let pairs: Vec<String> = matched
        .iter()
        .map(
            |c| if c.name.is_empty() { c.value.clone() } else { format!("{}={}", c.name, c.value) },
        )
        .collect();
    Some(pairs.join("; "))
}

/// RFC 6265 §5.1.4 path-match.
fn path_matches(request_path: &str, cookie_path: &str) -> bool {
    let request_path = if request_path.is_empty() { "/" } else { request_path };
    request_path == cookie_path
        || (request_path.starts_with(cookie_path)
            && (cookie_path.ends_with('/')
                || request_path.as_bytes().get(cookie_path.len()) == Some(&b'/')))
}

const MAX_COOKIES: usize = 500;
const MAX_TEXT_BYTES: usize = 8192;
const MAX_BROWSER_NAME_CHARS: usize = 40;

/// Why a [`BrowserContext`] was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid browser context: {0}")]
pub struct InvalidBrowserContext(pub &'static str);

/// Checks a [`BrowserContext`] that came from outside (the browser extension, a manifest read
/// back from disk) before any of it goes into a request. Refused whole rather than trimmed: a
/// context that breaks these rules wasn't made by our extension.
///
/// A control character in a header value would start a header of its own (CR/LF) and `;`
/// separates cookies, so a cookie name or value carrying either could forge others.
pub fn validate_browser_context(context: &BrowserContext) -> Result<(), InvalidBrowserContext> {
    let fail = |why| Err(InvalidBrowserContext(why));
    if context.cookies.len() > MAX_COOKIES {
        return fail("too many cookies");
    }
    if let Some(referer) = &context.referer {
        let web = url::Url::parse(referer).is_ok_and(|u| matches!(u.scheme(), "http" | "https"));
        if !is_text(referer, false) || !web {
            return fail("the referer is not a web address");
        }
    }
    if context.user_agent.as_deref().is_some_and(|ua| !is_text(ua, false)) {
        return fail("the user agent is not plain text");
    }
    if context
        .name
        .as_deref()
        .is_some_and(|n| !is_text(n, false) || n.chars().count() > MAX_BROWSER_NAME_CHARS)
    {
        return fail("the browser name is not plain text");
    }
    for cookie in &context.cookies {
        if !is_text(&cookie.name, true) || !is_text(&cookie.value, true) {
            return fail("a cookie name or value holds a control character or ';'");
        }
        if !is_text(&cookie.domain, false) || !is_text(&cookie.path, false) {
            return fail("a cookie domain or path is not plain text");
        }
        if !cookie.path.starts_with('/') {
            return fail("a cookie path does not start with '/'");
        }
    }
    Ok(())
}

fn is_text(value: &str, in_cookie: bool) -> bool {
    value.len() <= MAX_TEXT_BYTES
        && !value.chars().any(|c| c.is_ascii_control() || (in_cookie && c == ';'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cookie(
        name: &str,
        domain: &str,
        host_only: bool,
        path: &str,
        secure: bool,
    ) -> BrowserCookie {
        BrowserCookie {
            name: name.into(),
            value: "v".into(),
            domain: domain.into(),
            host_only,
            path: path.into(),
            secure,
        }
    }

    fn header(url: &str, cookies: &[BrowserCookie]) -> Option<String> {
        cookie_header(&Url::parse(url).unwrap(), cookies)
    }

    #[test]
    fn cookie_domains() {
        let jar = [
            cookie("host", "example.com", true, "/", false),
            cookie("dom", ".example.com", false, "/", false),
        ];
        assert_eq!(header("http://example.com/", &jar).as_deref(), Some("host=v; dom=v"));
        assert_eq!(header("http://cdn.example.com/", &jar).as_deref(), Some("dom=v"));
        assert_eq!(header("http://CDN.Example.com/", &jar).as_deref(), Some("dom=v"));
        assert_eq!(header("http://badexample.com/", &jar), None);
        assert_eq!(header("http://example.org/", &jar), None);
        // An IP address never gets a parent domain's cookie.
        let ip = [cookie("ip", "0.1", false, "/", false)];
        assert_eq!(header("http://10.0.0.1/", &ip), None);
    }

    #[test]
    fn cookie_paths_and_secure() {
        let jar = [
            cookie("root", "a.com", true, "/", false),
            cookie("docs", "a.com", true, "/docs", false),
            cookie("slash", "a.com", true, "/docs/", false),
            cookie("sec", "a.com", true, "/", true),
        ];
        assert_eq!(header("http://a.com/docs/x", &jar).as_deref(), Some("slash=v; docs=v; root=v"));
        assert_eq!(header("http://a.com/docs", &jar).as_deref(), Some("docs=v; root=v"));
        assert_eq!(header("http://a.com/docsx", &jar).as_deref(), Some("root=v"));
        assert_eq!(header("https://a.com/", &jar).as_deref(), Some("root=v; sec=v"));
    }

    #[test]
    fn headers_carry_the_browser() {
        let url = Url::parse("https://a.com/f").unwrap();
        let plain = request_headers(&url, None);
        assert_eq!(plain[USER_AGENT], DEFAULT_USER_AGENT);
        assert_eq!(plain[ACCEPT_ENCODING], "identity");
        assert!(!plain.contains_key(COOKIE) && !plain.contains_key(REFERER));

        let browser = BrowserContext {
            cookies: vec![cookie("s", "a.com", true, "/", true)],
            referer: Some("https://a.com/page".into()),
            user_agent: Some("Mozilla/5.0 Test".into()),
            name: Some("Chrome".into()),
        };
        let headers = request_headers(&url, Some(&browser));
        assert_eq!(headers[USER_AGENT], "Mozilla/5.0 Test");
        assert_eq!(headers[REFERER], "https://a.com/page");
        assert_eq!(headers[COOKIE], "s=v");
        let other = request_headers(&Url::parse("https://cdn.b.com/f").unwrap(), Some(&browser));
        assert!(!other.contains_key(COOKIE));
    }

    #[test]
    fn untrusted_context_is_checked() {
        let good = BrowserContext {
            cookies: vec![cookie("s", "a.com", true, "/", true)],
            referer: Some("https://a.com/".into()),
            user_agent: Some("UA".into()),
            name: Some("Firefox".into()),
        };
        assert_eq!(validate_browser_context(&good), Ok(()));
        assert_eq!(validate_browser_context(&BrowserContext::default()), Ok(()));

        let bad = |change: fn(&mut BrowserContext)| {
            let mut context = good.clone();
            change(&mut context);
            validate_browser_context(&context).is_err()
        };
        assert!(bad(|c| c.referer = Some("javascript:alert(1)".into())));
        assert!(bad(|c| c.referer = Some("https://a.com/\r\nX: y".into())));
        assert!(bad(|c| c.user_agent = Some("UA\nInjected: 1".into())));
        assert!(bad(|c| c.name = Some("x".repeat(41))));
        assert!(bad(|c| c.cookies[0].value = "a; admin=1".into()));
        assert!(bad(|c| c.cookies[0].name = "a\u{7f}".into()));
        assert!(bad(|c| c.cookies[0].path = "docs".into()));
        assert!(bad(|c| c.cookies[0].value = "x".repeat(MAX_TEXT_BYTES + 1)));
        assert!(bad(|c| c.cookies = vec![c.cookies[0].clone(); MAX_COOKIES + 1]));
    }
}
