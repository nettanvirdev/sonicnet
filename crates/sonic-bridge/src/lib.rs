//! The local HTTP endpoint the browser extension hands downloads to.
//!
//! Plain HTTP on 127.0.0.1 works in every browser with nothing to register. There's no pairing:
//! nothing here starts a download without the user (a link only lands in the app's list), web
//! pages can't get in (see [`guard`]), and a program on this computer could already do more.
//!
//! The conversation, all JSON:
//! - `GET /ping` → `{app: "sonicnet", version, api}`: the extension finds the app (and its port).
//! - `POST /add {url, browser?, pageUrl?, minBytes?, fileName?}` → the link is probed through the
//!   [`BridgeHost`]; `200 {token}` when the app will take it, `422 {reason}` when the browser
//!   should keep it. Answered within [`PROBE_DEADLINE`], since Chrome stops an extension's service
//!   worker whose fetch hangs.
//! - `POST /confirm {token}` → the link is offered to the app. The extension confirms only once
//!   the browser's own copy is gone (it can finish before it's cancelled); an unconfirmed token
//!   expires after [`TOKEN_TTL`], so a download never ends up in both.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::header::{
    ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
    ACCESS_CONTROL_MAX_AGE, CONTENT_TYPE, HOST, HeaderMap, HeaderValue, ORIGIN, VARY,
};
use axum::http::{Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::Deserialize;
use serde_json::json;
use sonic_core::{BrowserContext, BrowserCookie, ProbeResult};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// The app listens on the first that's free and the extension tries them all, so a port another
/// app holds needs no setting. Distinct from Plexo's (19288–19290), so both can run.
pub const PORTS: [u16; 3] = [19320, 19321, 19322];
/// Raised when the extension and the app can no longer follow each other's `/ping` or `/add`.
pub const API: u32 = 1;
/// Every request must carry `X-SonicNet-Client: extension`. A custom header makes any cross-origin
/// request "non-simple", so the browser asks first (a CORS preflight) and only an extension's
/// origin passes it: a web page can't even silently `/ping` the app to learn it is installed.
pub const CLIENT_HEADER: &str = "x-sonicnet-client";
pub const CLIENT_VALUE: &str = "extension";
pub const MAX_BODY_BYTES: usize = 64 * 1024;
/// How long `/add` waits for the probe. The extension waits a little longer than this, and the app
/// answers nothing once it has given up, so a download never ends up in both.
pub const PROBE_DEADLINE: Duration = Duration::from_secs(8);
/// How long a token from `/add` waits for its `/confirm`. The extension confirms within moments.
pub const TOKEN_TTL: Duration = Duration::from_secs(15);

const MAX_URL: usize = 8192;
const MAX_TEXT: usize = 8192;
const MAX_COOKIES: usize = 500;
const MAX_BROWSER_NAME: usize = 40;
const MAX_FILE_NAME: usize = 255;

/// A link the browser handed over and confirmed, already probed.
#[derive(Clone, Debug, PartialEq)]
pub struct OfferedLink {
    pub url: String,
    /// Its suggested name is the browser's pick when it sent one (only it saw the link's
    /// `download` attribute).
    pub probe: ProbeResult,
    /// The page the link was on.
    pub page_url: Option<String>,
    /// The browser's sign-in for the link. Keep it in the backend, never in the UI.
    pub browser: Option<BrowserContext>,
}

/// What the bridge needs from the app.
pub trait BridgeHost: Send + Sync + 'static {
    /// What `url` would download, asked with the browser's sign-in. `Err` is a reason the user
    /// can read; the browser keeps the download. Dropped (cancelled) if the extension gives up.
    fn probe(
        &self,
        url: String,
        browser: Option<BrowserContext>,
    ) -> impl Future<Output = Result<ProbeResult, String>> + Send;
    /// A confirmed link, for the app's list. Must not block.
    fn offer(&self, link: OfferedLink);
    /// The extension has been in touch (a ping or an add): from now on closing the window should
    /// keep the app in the tray, where the browser can still reach it. Must not block.
    fn extension_seen(&self);
}

/// Where and how the bridge runs. The defaults are the real ones; tests shorten the times.
#[derive(Clone, Debug)]
pub struct BridgeOptions {
    /// Tried in order; the first free one is used. `0` picks any free port.
    pub ports: Vec<u16>,
    pub probe_deadline: Duration,
    pub token_ttl: Duration,
}

impl Default for BridgeOptions {
    fn default() -> Self {
        Self { ports: PORTS.to_vec(), probe_deadline: PROBE_DEADLINE, token_ttl: TOKEN_TTL }
    }
}

/// A running bridge. Dropping it (or [`Bridge::shutdown`]) stops the server.
#[derive(Debug)]
pub struct Bridge {
    port: u16,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl Bridge {
    /// The port it listens on (on 127.0.0.1).
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Stops accepting connections and waits for the ones open to finish.
    pub async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let _ = (&mut self.task).await;
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        // The server stops when its stop channel closes; dropping the sender is enough.
        self.stop.take();
    }
}

/// Starts the bridge on the first free port of [`PORTS`]. Fails with `AddrInUse` when all are
/// taken (the app runs on without it). Must be called inside a Tokio runtime.
pub async fn start<H: BridgeHost>(host: Arc<H>) -> io::Result<Bridge> {
    start_with(host, BridgeOptions::default()).await
}

pub async fn start_with<H: BridgeHost>(host: Arc<H>, options: BridgeOptions) -> io::Result<Bridge> {
    let mut listener = None;
    for &port in &options.ports {
        match TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, port))).await {
            Ok(bound) => {
                listener = Some(bound);
                break;
            }
            Err(error) => tracing::debug!(port, %error, "bridge port unavailable"),
        }
    }
    let listener = listener.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::AddrInUse,
            format!("browser bridge ports {:?} are all taken", options.ports),
        )
    })?;
    let port = listener.local_addr()?.port();

    let shared = Arc::new(Shared { host, port, options, tokens: Mutex::default() });
    let app = Router::new()
        .route("/ping", get(ping))
        .route("/add", post(add))
        .route("/confirm", post(confirm))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(middleware::from_fn_with_state(shared.clone(), guard))
        .with_state(shared);

    let (stop, stopped) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let server = axum::serve(listener, app).with_graceful_shutdown(async {
            let _ = stopped.await;
        });
        if let Err(error) = server.await {
            tracing::warn!(%error, "browser bridge stopped");
        }
    });
    tracing::info!(port, "browser bridge listening");
    Ok(Bridge { port, stop: Some(stop), task })
}

struct Shared<H> {
    host: Arc<H>,
    port: u16,
    options: BridgeOptions,
    /// Probed links waiting for their `/confirm`, by token, with when they were held.
    tokens: Mutex<HashMap<String, (Instant, OfferedLink)>>,
}

impl<H> Shared<H> {
    fn tokens(&self) -> std::sync::MutexGuard<'_, HashMap<String, (Instant, OfferedLink)>> {
        // Held for an instant and never across a panic-prone call; a poisoned map is still whole.
        self.tokens.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn hold(&self, link: OfferedLink) -> String {
        let token = uuid::Uuid::new_v4().to_string();
        let now = Instant::now();
        let ttl = self.options.token_ttl;
        let mut tokens = self.tokens();
        // Expired ones go whenever a new one comes, so the map stays as small as what's in flight.
        tokens.retain(|_, (held_at, _)| now.duration_since(*held_at) < ttl);
        tokens.insert(token.clone(), (now, link));
        token
    }

    fn take(&self, token: &str) -> Option<OfferedLink> {
        let (held_at, link) = self.tokens().remove(token)?;
        (held_at.elapsed() < self.options.token_ttl).then_some(link)
    }
}

/// Runs before every route (and the 404), so nothing is reachable without passing it:
/// - `Host` must be `127.0.0.1:<port>` or `localhost:<port>`: a page on a domain that resolves to
///   127.0.0.1 (DNS rebinding) still sends its own name.
/// - `Origin`, when present, must be an extension's. A web page's request always carries its
///   origin; Firefox may leave it off an extension's.
/// - A preflight (`OPTIONS`) from an extension is answered here, granting the custom header.
/// - Every other request needs [`CLIENT_HEADER`], and a POST must be `application/json`.
async fn guard<H: BridgeHost>(
    State(shared): State<Arc<Shared<H>>>,
    request: Request,
    next: Next,
) -> Response {
    let headers = request.headers();
    if !host_is_local(headers, shared.port) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let origin = match headers.get(ORIGIN) {
        None => None,
        Some(origin) if origin.to_str().is_ok_and(is_extension_origin) => Some(origin.clone()),
        Some(_) => return StatusCode::FORBIDDEN.into_response(),
    };

    let mut response = if request.method() == Method::OPTIONS {
        let mut response = StatusCode::NO_CONTENT.into_response();
        let headers = response.headers_mut();
        headers.insert(
            ACCESS_CONTROL_ALLOW_HEADERS,
            HeaderValue::from_static("content-type, x-sonicnet-client"),
        );
        headers.insert(ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("GET, POST"));
        headers.insert(ACCESS_CONTROL_MAX_AGE, HeaderValue::from_static("600"));
        response
    } else if headers.get(CLIENT_HEADER).is_none_or(|value| value != CLIENT_VALUE) {
        StatusCode::FORBIDDEN.into_response()
    } else if request.method() == Method::POST && !is_json(headers) {
        StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response()
    } else {
        next.run(request).await
    };
    if let Some(origin) = origin {
        let headers = response.headers_mut();
        headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        headers.insert(VARY, HeaderValue::from_static("Origin"));
    }
    response
}

fn host_is_local(headers: &HeaderMap, port: u16) -> bool {
    let mut hosts = headers.get_all(HOST).iter();
    match (hosts.next().and_then(|host| host.to_str().ok()), hosts.next()) {
        (Some(host), None) => {
            host == format!("127.0.0.1:{port}") || host == format!("localhost:{port}")
        }
        _ => false,
    }
}

/// `^(chrome|moz)-extension://[a-z0-9-]+$`. Any extension's: Firefox gives each install its own
/// id, so ours can't be pinned.
fn is_extension_origin(origin: &str) -> bool {
    ["chrome-extension://", "moz-extension://"].iter().any(|scheme| {
        origin.strip_prefix(scheme).is_some_and(|id| {
            !id.is_empty()
                && id.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        })
    })
}

/// `application/json`, parameters (a charset) allowed.
fn is_json(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/json"))
}

async fn ping<H: BridgeHost>(State(shared): State<Arc<Shared<H>>>) -> Response {
    shared.host.extension_seen();
    axum::Json(json!({"app": "sonicnet", "version": env!("CARGO_PKG_VERSION"), "api": API}))
        .into_response()
}

/// `POST /add`, as sent. Field types are checked by serde, their contents by [`AddRequest::check`].
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AddRequest {
    url: String,
    browser: Option<WireBrowser>,
    page_url: Option<String>,
    min_bytes: Option<u64>,
    file_name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireBrowser {
    cookies: Vec<WireCookie>,
    referer: Option<String>,
    user_agent: Option<String>,
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireCookie {
    name: String,
    value: String,
    domain: String,
    host_only: bool,
    path: String,
    secure: bool,
}

/// Text that can go into a request header: bounded, and no control character (a line break would
/// start a header of its own).
fn is_text(text: &str, max: usize) -> bool {
    text.len() <= max && !text.chars().any(|c| c.is_control())
}

fn is_http(url: &str) -> bool {
    let bytes = url.as_bytes();
    let starts =
        |prefix: &[u8]| bytes.get(..prefix.len()).is_some_and(|p| p.eq_ignore_ascii_case(prefix));
    starts(b"http://") || starts(b"https://")
}

impl AddRequest {
    /// The browser context, if every field is acceptable; `Err` names the first that isn't.
    fn check(&self) -> Result<Option<BrowserContext>, &'static str> {
        if !is_text(&self.url, MAX_URL) {
            return Err("url");
        }
        if self.page_url.as_ref().is_some_and(|page| !(is_text(page, MAX_URL) && is_http(page))) {
            return Err("pageUrl");
        }
        if let Some(name) = &self.file_name {
            // A name, never a path.
            let ok = !name.is_empty()
                && is_text(name, MAX_FILE_NAME)
                && !name.contains(['/', '\\'])
                && name != "."
                && name != "..";
            if !ok {
                return Err("fileName");
            }
        }
        let Some(browser) = &self.browser else { return Ok(None) };
        if browser.cookies.len() > MAX_COOKIES {
            return Err("browser.cookies");
        }
        if browser.referer.as_ref().is_some_and(|r| !(is_text(r, MAX_URL) && is_http(r))) {
            return Err("browser.referer");
        }
        if browser.user_agent.as_ref().is_some_and(|agent| !is_text(agent, MAX_TEXT)) {
            return Err("browser.userAgent");
        }
        if browser.name.as_ref().is_some_and(|name| !is_text(name, MAX_BROWSER_NAME)) {
            return Err("browser.name");
        }
        let mut cookies = Vec::with_capacity(browser.cookies.len());
        for cookie in &browser.cookies {
            // `;` separates cookies in the Cookie header.
            let in_cookie = |text: &str| is_text(text, MAX_TEXT) && !text.contains(';');
            if !(in_cookie(&cookie.name) && in_cookie(&cookie.value))
                || !is_text(&cookie.domain, MAX_TEXT)
                || !(is_text(&cookie.path, MAX_TEXT) && cookie.path.starts_with('/'))
            {
                return Err("browser.cookies");
            }
            cookies.push(BrowserCookie {
                name: cookie.name.clone(),
                value: cookie.value.clone(),
                domain: cookie.domain.clone(),
                host_only: cookie.host_only,
                path: cookie.path.clone(),
                secure: cookie.secure,
            });
        }
        Ok(Some(BrowserContext {
            cookies,
            referer: browser.referer.clone(),
            user_agent: browser.user_agent.clone(),
            name: browser.name.clone(),
        }))
    }
}

fn bad_request(field: &str) -> Response {
    (StatusCode::BAD_REQUEST, axum::Json(json!({"error": format!("invalid {field}")})))
        .into_response()
}

/// The browser keeps the download, for this reason.
fn keep(reason: impl Into<String>) -> Response {
    (StatusCode::UNPROCESSABLE_ENTITY, axum::Json(json!({"reason": reason.into()}))).into_response()
}

/// If the extension disconnects while the probe runs, the server drops this future: the probe is
/// cancelled and no token is ever held, so the app can't take a download the browser kept.
async fn add<H: BridgeHost>(State(shared): State<Arc<Shared<H>>>, body: Bytes) -> Response {
    let Ok(request) = serde_json::from_slice::<AddRequest>(&body) else {
        return bad_request("body");
    };
    let browser = match request.check() {
        Ok(browser) => browser,
        Err(field) => return bad_request(field),
    };
    shared.host.extension_seen();
    // blob:, data:, file: and the like exist only inside the browser.
    if !is_http(&request.url) {
        return keep("Only http and https links can be downloaded.");
    }

    let probed = tokio::time::timeout(
        shared.options.probe_deadline,
        shared.host.probe(request.url.clone(), browser.clone()),
    )
    .await;
    let mut probe = match probed {
        Err(_) => return keep("The server took too long to answer."),
        Ok(Err(reason)) => return keep(reason),
        Ok(Ok(probe)) => probe,
    };
    if let (Some(min), Some(total)) = (request.min_bytes, probe.total_bytes)
        && total < min
    {
        return keep("Smaller than the minimum size.");
    }
    if let Some(name) = request.file_name {
        probe.suggested_file_name = name;
    }
    let token =
        shared.hold(OfferedLink { url: request.url, probe, page_url: request.page_url, browser });
    axum::Json(json!({ "token": token })).into_response()
}

#[derive(Deserialize)]
struct ConfirmRequest {
    token: String,
}

async fn confirm<H: BridgeHost>(State(shared): State<Arc<Shared<H>>>, body: Bytes) -> Response {
    let Ok(ConfirmRequest { token }) = serde_json::from_slice(&body) else {
        return bad_request("body");
    };
    match shared.take(&token) {
        Some(link) => {
            shared.host.offer(link);
            axum::Json(json!({})).into_response()
        }
        // Unknown, already confirmed, or expired: the app shows nothing.
        None => StatusCode::GONE.into_response(),
    }
}

#[cfg(test)]
mod tests;
