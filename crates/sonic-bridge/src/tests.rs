use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::*;

/// How the fake host's probe behaves.
#[derive(Clone)]
enum Probe {
    Ok(Option<u64>),
    Err(&'static str),
    /// Never answers; sets the flag when the probe is dropped (cancelled).
    Hang(Arc<AtomicBool>),
}

struct FakeHost {
    probe: Probe,
    probed: Mutex<Vec<(String, Option<BrowserContext>)>>,
    offers: Mutex<Vec<OfferedLink>>,
    seen: AtomicUsize,
}

impl FakeHost {
    fn new(probe: Probe) -> Arc<Self> {
        Arc::new(Self {
            probe,
            probed: Mutex::default(),
            offers: Mutex::default(),
            seen: AtomicUsize::new(0),
        })
    }

    fn offers(&self) -> Vec<OfferedLink> {
        self.offers.lock().unwrap().clone()
    }
}

struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn probe_result(url: &str, total: Option<u64>) -> ProbeResult {
    ProbeResult {
        requested_url: url.into(),
        final_url: url.into(),
        supports_ranges: true,
        total_bytes: total,
        suggested_file_name: "server.bin".into(),
        content_type: None,
        etag: None,
        last_modified: None,
    }
}

impl BridgeHost for FakeHost {
    async fn probe(
        &self,
        url: String,
        browser: Option<BrowserContext>,
    ) -> Result<ProbeResult, String> {
        self.probed.lock().unwrap().push((url.clone(), browser));
        match &self.probe {
            Probe::Ok(total) => Ok(probe_result(&url, *total)),
            Probe::Err(reason) => Err((*reason).into()),
            Probe::Hang(flag) => {
                let _flag = DropFlag(flag.clone());
                std::future::pending().await
            }
        }
    }

    fn offer(&self, link: OfferedLink) {
        self.offers.lock().unwrap().push(link);
    }

    fn extension_seen(&self) {
        self.seen.fetch_add(1, Ordering::SeqCst);
    }
}

fn test_options() -> BridgeOptions {
    BridgeOptions {
        ports: vec![0],
        probe_deadline: Duration::from_secs(5),
        token_ttl: Duration::from_secs(5),
    }
}

async fn bridge(probe: Probe) -> (Bridge, Arc<FakeHost>) {
    bridge_with(probe, test_options()).await
}

async fn bridge_with(probe: Probe, options: BridgeOptions) -> (Bridge, Arc<FakeHost>) {
    let host = FakeHost::new(probe);
    (start_with(host.clone(), options).await.unwrap(), host)
}

#[derive(Debug)]
struct Reply {
    status: u16,
    head: String,
    body: String,
}

impl Reply {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap()
    }

    fn header(&self, name: &str) -> Option<String> {
        self.head.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name).then(|| value.trim().to_string())
        })
    }
}

type Headers = Vec<(String, String)>;

/// The headers the extension sends.
fn extension_headers(port: u16) -> Headers {
    vec![
        ("Host".into(), format!("127.0.0.1:{port}")),
        ("X-SonicNet-Client".into(), "extension".into()),
        ("Content-Type".into(), "application/json".into()),
    ]
}

fn with(mut headers: Headers, name: &str, value: &str) -> Headers {
    headers.retain(|(key, _)| !key.eq_ignore_ascii_case(name));
    headers.push((name.into(), value.into()));
    headers
}

fn without(mut headers: Headers, name: &str) -> Headers {
    headers.retain(|(key, _)| !key.eq_ignore_ascii_case(name));
    headers
}

/// One request over a fresh connection, written by hand so any header can be forged.
async fn raw(port: u16, method: &str, path: &str, headers: &Headers, body: &[u8]) -> Reply {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut request = format!("{method} {path} HTTP/1.1\r\n");
    for (key, value) in headers {
        request.push_str(&format!("{key}: {value}\r\n"));
    }
    request.push_str(&format!("Content-Length: {}\r\nConnection: close\r\n\r\n", body.len()));
    stream.write_all(request.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    let mut response = Vec::new();
    let _ = stream.read_to_end(&mut response).await;
    let response = String::from_utf8_lossy(&response).into_owned();
    let (head, body) = response.split_once("\r\n\r\n").unwrap_or((&response, ""));
    let status = head.split(' ').nth(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    Reply { status, head: head.into(), body: body.into() }
}

async fn get(port: u16, path: &str) -> Reply {
    raw(port, "GET", path, &extension_headers(port), b"").await
}

async fn post(port: u16, path: &str, body: &Value) -> Reply {
    raw(port, "POST", path, &extension_headers(port), body.to_string().as_bytes()).await
}

fn add_body() -> Value {
    json!({
        "url": "https://example.com/file.zip",
        "pageUrl": "https://example.com/page",
        "fileName": "browser-name.zip",
        "browser": {
            "cookies": [{"name": "sid", "value": "v", "domain": "example.com", "hostOnly": true,
                         "path": "/", "secure": true}],
            "referer": "https://example.com/page",
            "userAgent": "Mozilla/5.0",
            "name": "Chrome"
        }
    })
}

#[tokio::test]
async fn ping_answers_and_notes_the_extension() {
    let (bridge, host) = bridge(Probe::Ok(None)).await;
    let reply = get(bridge.port(), "/ping").await;
    assert_eq!(reply.status, 200);
    assert_eq!(
        reply.json(),
        json!({"app": "sonicnet", "version": env!("CARGO_PKG_VERSION"), "api": 1})
    );
    assert_eq!(host.seen.load(Ordering::SeqCst), 1);
    // localhost works too.
    let headers =
        with(extension_headers(bridge.port()), "Host", &format!("localhost:{}", bridge.port()));
    assert_eq!(raw(bridge.port(), "GET", "/ping", &headers, b"").await.status, 200);
}

#[tokio::test]
async fn foreign_host_is_refused() {
    let (bridge, host) = bridge(Probe::Ok(None)).await;
    let port = bridge.port();
    for bad in [
        "evil.com",
        &format!("evil.com:{port}"),
        &format!("127.0.0.1:{}", port + 1),
        "127.0.0.1",
        &format!("LOCALHOST.evil.com:{port}"),
        &format!("127.0.0.1:{port}.evil.com"),
    ] {
        let headers = with(extension_headers(port), "Host", bad);
        assert_eq!(raw(port, "GET", "/ping", &headers, b"").await.status, 403, "{bad}");
    }
    let headers = without(extension_headers(port), "Host");
    assert_eq!(raw(port, "GET", "/ping", &headers, b"").await.status, 403);
    // Two Host headers: ambiguous, refused.
    let mut headers = extension_headers(port);
    headers.push(("Host".into(), "evil.com".into()));
    assert_eq!(raw(port, "GET", "/ping", &headers, b"").await.status, 403);
    // Guards cover unknown paths too.
    let headers = with(extension_headers(port), "Host", "evil.com");
    assert_eq!(raw(port, "GET", "/nope", &headers, b"").await.status, 403);
    assert_eq!(get(port, "/nope").await.status, 404);
    assert_eq!(host.seen.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn only_extension_origins() {
    let (bridge, _host) = bridge(Probe::Ok(None)).await;
    let port = bridge.port();
    for bad in [
        "https://evil.com",
        "null",
        "chrome-extension://",
        "chrome-extension://ABCDEF",
        "chrome-extension://abc/",
        "safari-web-extension://abc",
        "http://127.0.0.1",
    ] {
        let headers = with(extension_headers(port), "Origin", bad);
        assert_eq!(raw(port, "GET", "/ping", &headers, b"").await.status, 403, "{bad}");
    }
    for good in ["chrome-extension://abcdefghijklmnop", "moz-extension://0a1b2c3d-4e5f-6789"] {
        let headers = with(extension_headers(port), "Origin", good);
        let reply = raw(port, "GET", "/ping", &headers, b"").await;
        assert_eq!(reply.status, 200, "{good}");
        assert_eq!(reply.header("access-control-allow-origin").as_deref(), Some(good));
    }
}

#[tokio::test]
async fn custom_header_required_on_every_request() {
    let (bridge, host) = bridge(Probe::Ok(None)).await;
    let port = bridge.port();
    let missing = without(extension_headers(port), "X-SonicNet-Client");
    assert_eq!(raw(port, "GET", "/ping", &missing, b"").await.status, 403);
    let body = add_body().to_string();
    assert_eq!(raw(port, "POST", "/add", &missing, body.as_bytes()).await.status, 403);
    let wrong = with(extension_headers(port), "X-SonicNet-Client", "page");
    assert_eq!(raw(port, "GET", "/ping", &wrong, b"").await.status, 403);
    assert!(host.probed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn preflight_only_for_extensions() {
    let (bridge, _host) = bridge(Probe::Ok(None)).await;
    let port = bridge.port();
    // A preflight carries no custom header; the browser asks whether it may send one.
    let preflight = |origin: &str| {
        vec![
            ("Host".to_string(), format!("127.0.0.1:{port}")),
            ("Origin".into(), origin.into()),
            ("Access-Control-Request-Method".into(), "POST".into()),
            ("Access-Control-Request-Headers".into(), "content-type, x-sonicnet-client".into()),
        ]
    };
    let reply = raw(port, "OPTIONS", "/add", &preflight("chrome-extension://abc"), b"").await;
    assert_eq!(reply.status, 204);
    let allowed = reply.header("access-control-allow-headers").unwrap();
    assert!(allowed.contains("x-sonicnet-client") && allowed.contains("content-type"));
    assert_eq!(
        reply.header("access-control-allow-origin").as_deref(),
        Some("chrome-extension://abc")
    );

    let reply = raw(port, "OPTIONS", "/ping", &preflight("https://evil.com"), b"").await;
    assert_eq!(reply.status, 403);
    assert_eq!(reply.header("access-control-allow-origin"), None);
}

#[tokio::test]
async fn post_must_be_json() {
    let (bridge, _host) = bridge(Probe::Ok(None)).await;
    let port = bridge.port();
    let body = add_body().to_string();
    for content_type in ["text/plain", "application/x-www-form-urlencoded", "application/jsonx"] {
        let headers = with(extension_headers(port), "Content-Type", content_type);
        assert_eq!(raw(port, "POST", "/add", &headers, body.as_bytes()).await.status, 415);
    }
    let headers = without(extension_headers(port), "Content-Type");
    assert_eq!(raw(port, "POST", "/add", &headers, body.as_bytes()).await.status, 415);
    let headers = with(extension_headers(port), "Content-Type", "Application/JSON; charset=utf-8");
    assert_eq!(raw(port, "POST", "/add", &headers, body.as_bytes()).await.status, 200);
}

#[tokio::test]
async fn oversize_body_refused() {
    let (bridge, host) = bridge(Probe::Ok(None)).await;
    let port = bridge.port();
    let mut body = add_body();
    body["browser"]["userAgent"] = json!("a".repeat(MAX_BODY_BYTES));
    let reply =
        raw(port, "POST", "/add", &extension_headers(port), body.to_string().as_bytes()).await;
    assert_eq!(reply.status, 413);
    assert!(host.probed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn bad_fields_refused() {
    let (bridge, host) = bridge(Probe::Ok(None)).await;
    let port = bridge.port();
    let edit = |f: &dyn Fn(&mut Value)| {
        let mut body = add_body();
        f(&mut body);
        body
    };
    let cookie = |f: &dyn Fn(&mut Value)| edit(&|b| f(&mut b["browser"]["cookies"][0]));
    let many_cookies: Vec<Value> = (0..=MAX_COOKIES)
        .map(|_| json!({"name": "a", "value": "b", "domain": "d", "hostOnly": true, "path": "/", "secure": false}))
        .collect();
    let cases = [
        json!("just a string"),
        json!([1, 2]),
        edit(&|b| {
            b.as_object_mut().unwrap().remove("url");
        }),
        edit(&|b| b["url"] = json!(42)),
        edit(&|b| b["url"] = json!(format!("https://e.com/{}", "a".repeat(MAX_URL)))),
        edit(&|b| b["url"] = json!("https://e.com/\r\nX-Evil: 1")),
        edit(&|b| b["pageUrl"] = json!("javascript:alert(1)")),
        edit(&|b| b["minBytes"] = json!(-1)),
        edit(&|b| b["minBytes"] = json!(1.5)),
        edit(&|b| b["fileName"] = json!("../evil.exe")),
        edit(&|b| b["fileName"] = json!("a\\b")),
        edit(&|b| b["fileName"] = json!("")),
        edit(&|b| b["fileName"] = json!("..")),
        edit(&|b| b["fileName"] = json!("a".repeat(256))),
        edit(&|b| b["fileName"] = json!("bad\nname")),
        edit(&|b| b["browser"] = json!("Chrome")),
        edit(&|b| b["browser"]["cookies"] = json!({})),
        edit(&|b| b["browser"]["cookies"] = json!(many_cookies.clone())),
        edit(&|b| b["browser"]["referer"] = json!("ftp://example.com")),
        edit(&|b| b["browser"]["name"] = json!("x".repeat(41))),
        edit(&|b| b["browser"]["userAgent"] = json!("UA\u{0}")),
        cookie(&|c| c["value"] = json!("a; injected=1")),
        cookie(&|c| c["name"] = json!("a\r\nb")),
        cookie(&|c| c["path"] = json!("no-slash")),
        cookie(&|c| c["hostOnly"] = json!("yes")),
        cookie(&|c| {
            c.as_object_mut().unwrap().remove("secure");
        }),
    ];
    for body in &cases {
        let raw_body = body.to_string();
        assert!(raw_body.len() < MAX_BODY_BYTES);
        assert_eq!(post(port, "/add", body).await.status, 400, "{raw_body:.200}");
    }
    let reply = raw(port, "POST", "/add", &extension_headers(port), b"{not json").await;
    assert_eq!(reply.status, 400);
    assert!(host.probed.lock().unwrap().is_empty());
    // Optional fields may be absent or null.
    let minimal = json!({"url": "https://example.com/a", "browser": null, "minBytes": null});
    assert_eq!(post(port, "/add", &minimal).await.status, 200);
}

#[tokio::test]
async fn add_then_confirm_offers_the_link() {
    let (bridge, host) = bridge(Probe::Ok(Some(1000))).await;
    let port = bridge.port();
    let reply = post(port, "/add", &add_body()).await;
    assert_eq!(reply.status, 200);
    let token = reply.json()["token"].as_str().unwrap().to_string();
    // Nothing is offered until the extension confirms.
    assert!(host.offers().is_empty());
    let (url, browser) = host.probed.lock().unwrap()[0].clone();
    assert_eq!(url, "https://example.com/file.zip");
    let browser = browser.unwrap();
    assert_eq!(browser.cookies[0].name, "sid");
    assert!(browser.cookies[0].host_only);
    assert_eq!(browser.user_agent.as_deref(), Some("Mozilla/5.0"));

    assert_eq!(post(port, "/confirm", &json!({ "token": token })).await.status, 200);
    let offers = host.offers();
    assert_eq!(offers.len(), 1);
    let offer = &offers[0];
    assert_eq!(offer.url, "https://example.com/file.zip");
    assert_eq!(offer.page_url.as_deref(), Some("https://example.com/page"));
    assert_eq!(offer.probe.suggested_file_name, "browser-name.zip");
    assert_eq!(offer.browser.as_ref(), Some(&browser));
    assert_eq!(host.seen.load(Ordering::SeqCst), 1);

    // A token is good once.
    assert_eq!(post(port, "/confirm", &json!({ "token": token })).await.status, 410);
    assert_eq!(post(port, "/confirm", &json!({"token": "made-up"})).await.status, 410);
    assert_eq!(post(port, "/confirm", &json!({"nope": 1})).await.status, 400);
    assert_eq!(host.offers().len(), 1);
}

#[tokio::test]
async fn unconfirmed_tokens_expire() {
    let options = BridgeOptions { token_ttl: Duration::from_millis(100), ..test_options() };
    let (bridge, host) = bridge_with(Probe::Ok(Some(1000)), options).await;
    let port = bridge.port();
    let token = post(port, "/add", &add_body()).await.json()["token"].as_str().unwrap().to_string();
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(post(port, "/confirm", &json!({ "token": token })).await.status, 410);
    assert!(host.offers().is_empty());
}

#[tokio::test]
async fn browser_keeps_what_the_app_should_not_take() {
    let (bridge, host) = bridge(Probe::Ok(Some(1000))).await;
    let port = bridge.port();

    let mut body = add_body();
    body["url"] = json!("blob:https://example.com/123");
    let reply = post(port, "/add", &body).await;
    assert_eq!(reply.status, 422);
    assert!(reply.json()["reason"].as_str().unwrap().contains("http"));
    assert!(host.probed.lock().unwrap().is_empty());

    let mut body = add_body();
    body["minBytes"] = json!(1001);
    let reply = post(port, "/add", &body).await;
    assert_eq!(reply.status, 422);
    assert!(reply.json()["reason"].as_str().unwrap().contains("minimum"));
    body["minBytes"] = json!(1000);
    assert_eq!(post(port, "/add", &body).await.status, 200);

    let (bridge, _host) = bridge_with(Probe::Err("403 Forbidden"), test_options()).await;
    let reply = post(bridge.port(), "/add", &add_body()).await;
    assert_eq!(reply.status, 422);
    assert_eq!(reply.json(), json!({"reason": "403 Forbidden"}));

    // An unknown size can't be judged against the minimum: taken.
    let (bridge, _host) = bridge_with(Probe::Ok(None), test_options()).await;
    let mut body = add_body();
    body["minBytes"] = json!(1 << 30);
    assert_eq!(post(bridge.port(), "/add", &body).await.status, 200);
}

#[tokio::test]
async fn slow_probe_answers_by_the_deadline() {
    let flag = Arc::new(AtomicBool::new(false));
    let options = BridgeOptions { probe_deadline: Duration::from_millis(200), ..test_options() };
    let (bridge, host) = bridge_with(Probe::Hang(flag.clone()), options).await;
    let started = Instant::now();
    let reply = post(bridge.port(), "/add", &add_body()).await;
    assert_eq!(reply.status, 422);
    assert!(reply.json()["reason"].as_str().unwrap().contains("too long"));
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(flag.load(Ordering::SeqCst), "the probe is cancelled, not left running");
    assert!(host.offers().is_empty());
}

#[tokio::test]
async fn extension_giving_up_cancels_the_probe() {
    let flag = Arc::new(AtomicBool::new(false));
    let (bridge, host) = bridge(Probe::Hang(flag.clone())).await;
    let port = bridge.port();
    let body = add_body().to_string();
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut request = "POST /add HTTP/1.1\r\n".to_string();
    for (key, value) in extension_headers(port) {
        request.push_str(&format!("{key}: {value}\r\n"));
    }
    request.push_str(&format!("Content-Length: {}\r\n\r\n{body}", body.len()));
    stream.write_all(request.as_bytes()).await.unwrap();
    // Wait for the probe to start, then hang up.
    for _ in 0..100 {
        if !host.probed.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(host.probed.lock().unwrap().len(), 1);
    drop(stream);
    for _ in 0..200 {
        if flag.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(flag.load(Ordering::SeqCst), "probe dropped when the client disconnected");
    assert!(host.offers().is_empty());
}

#[tokio::test]
async fn falls_back_to_the_next_free_port() {
    let taken = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let taken_port = taken.local_addr().unwrap().port();
    let free_port = {
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        probe.local_addr().unwrap().port()
    };
    let options = BridgeOptions { ports: vec![taken_port, free_port], ..test_options() };
    let (bridge, _host) = bridge_with(Probe::Ok(None), options).await;
    assert_eq!(bridge.port(), free_port);
    assert_eq!(get(free_port, "/ping").await.status, 200);

    // All taken: an error, not a panic.
    let options = BridgeOptions { ports: vec![taken_port], ..test_options() };
    let error = start_with(FakeHost::new(Probe::Ok(None)), options).await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
}

#[tokio::test]
async fn default_ports_skip_a_taken_first_port() {
    // Hold 19320 (if something else already does, that serves as well).
    let _held = TcpListener::bind(("127.0.0.1", PORTS[0])).await;
    match start(FakeHost::new(Probe::Ok(None))).await {
        Ok(bridge) => {
            assert!(PORTS[1..].contains(&bridge.port()));
            assert_eq!(get(bridge.port(), "/ping").await.status, 200);
        }
        // Every port held by something else on this machine: nothing to check here.
        Err(error) => assert_eq!(error.kind(), io::ErrorKind::AddrInUse),
    }
}

#[tokio::test]
async fn shutdown_stops_listening() {
    let (bridge, _host) = bridge(Probe::Ok(None)).await;
    let port = bridge.port();
    assert_eq!(get(port, "/ping").await.status, 200);
    bridge.shutdown().await;
    assert!(TcpStream::connect(("127.0.0.1", port)).await.is_err());
}

#[test]
fn origin_pattern() {
    assert!(is_extension_origin("chrome-extension://abc-123"));
    assert!(is_extension_origin("moz-extension://abc"));
    assert!(!is_extension_origin("chrome-extension://abc.def"));
    assert!(!is_extension_origin("xchrome-extension://abc"));
    assert!(!is_extension_origin("chrome-extension://abc\n"));
}
