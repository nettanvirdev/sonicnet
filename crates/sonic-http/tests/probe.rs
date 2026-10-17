//! probe() against the test origin.

mod support;

use std::sync::Arc;
use std::time::Duration;

use sonic_core::{BrowserContext, BrowserCookie};
use sonic_http::{ProbeError, StreamConnection, probe};
use support::{Fault, Origin, Served, connection};

#[tokio::test]
async fn ranged_server() {
    let origin = Origin::ok(Served::new(5000)).await;
    let url = origin.url("/files/My%20Report.pdf?token=1");
    let result = probe(&url, None, &mut connection()).await.unwrap();
    assert!(result.supports_ranges);
    assert_eq!(result.total_bytes, Some(5000));
    assert_eq!(result.suggested_file_name, "My Report.pdf");
    assert_eq!(result.requested_url, url);
    assert_eq!(result.final_url, url);
    assert_eq!(result.etag.as_deref(), Some("\"v1\""));
    assert_eq!(result.last_modified.as_deref(), Some("Wed, 21 Oct 2015 07:28:00 GMT"));
    assert_eq!(result.content_type.as_deref(), Some("application/octet-stream"));
    assert_eq!(origin.requests()[0].range, Some((0, Some(1023))));
}

#[tokio::test]
async fn server_ignoring_range() {
    let origin = Origin::start(Served::new(5000), |_, _| Fault::IgnoreRange).await;
    let result = probe(&origin.url("/a.bin"), None, &mut connection()).await.unwrap();
    assert!(!result.supports_ranges);
    assert_eq!(result.total_bytes, Some(5000));
}

#[tokio::test]
async fn partial_content_length_is_not_the_size() {
    // A 206 without a total: its Content-Length is the part's, not the file's.
    let origin = Origin::start(Served::new(5000), |_, _| Fault::NoContentRange).await;
    let result = probe(&origin.url("/a.bin"), None, &mut connection()).await.unwrap();
    assert!(result.supports_ranges);
    assert_eq!(result.total_bytes, None);
}

#[tokio::test]
async fn redirects_relative_and_absolute() {
    let origin = Origin::start(Served::new(10), move |req, _| match req.path.as_str() {
        "/start" => Fault::Redirect("next".into()),
        "/next" => Fault::Redirect("/deeper/final.iso".into()),
        _ => Fault::Ok,
    })
    .await;
    let absolute = origin.url("/start");
    let first = Origin::start(Served::new(1), move |_, _| Fault::Redirect(absolute.clone())).await;
    let result = probe(&first.url("/x"), None, &mut connection()).await.unwrap();
    assert_eq!(result.final_url, origin.url("/deeper/final.iso"));
    assert_eq!(result.suggested_file_name, "final.iso");
    assert_eq!(result.total_bytes, Some(10));
}

#[tokio::test]
async fn redirect_loops_end() {
    let origin = Origin::start(Served::new(10), |_, _| Fault::Redirect("/again".into())).await;
    let error = probe(&origin.url("/x"), None, &mut connection()).await.unwrap_err();
    assert!(matches!(error, ProbeError::TooManyRedirects), "{error:?}");
    assert_eq!(origin.requests().len(), 6);
}

#[tokio::test]
async fn forbidden_range_is_asked_again_without() {
    let origin = Origin::start(Served::new(3000), |_, _| Fault::ForbidRange).await;
    let result = probe(&origin.url("/f.zip"), None, &mut connection()).await.unwrap();
    assert!(!result.supports_ranges);
    assert_eq!(result.total_bytes, Some(3000));
    let ranges: Vec<_> = origin.requests().iter().map(|r| r.range).collect();
    assert_eq!(ranges, [Some((0, Some(1023))), None]);
}

#[tokio::test]
async fn forbidden_either_way_is_an_error() {
    let origin = Origin::start(Served::new(10), |_, _| Fault::Status(403, vec![])).await;
    let error = probe(&origin.url("/f"), None, &mut connection()).await.unwrap_err();
    assert!(matches!(error, ProbeError::Status(403)), "{error:?}");
    assert_eq!(origin.requests().len(), 2);
}

#[tokio::test]
async fn empty_file() {
    let origin = Origin::ok(Served::new(0)).await;
    let result = probe(&origin.url("/empty.txt"), None, &mut connection()).await.unwrap();
    assert_eq!(result.total_bytes, Some(0));
    assert!(!result.supports_ranges);
    assert_eq!(result.suggested_file_name, "empty.txt");
}

#[tokio::test]
async fn unsatisfiable_range_of_a_non_empty_file_is_an_error() {
    let origin = Origin::start(Served::new(10), |_, _| {
        Fault::Status(416, vec![("Content-Range", "bytes */10".into())])
    })
    .await;
    let error = probe(&origin.url("/f"), None, &mut connection()).await.unwrap_err();
    assert!(matches!(error, ProbeError::Status(416)), "{error:?}");
}

#[tokio::test]
async fn not_found() {
    let origin = Origin::start(Served::new(10), |_, _| Fault::Status(404, vec![])).await;
    let error = probe(&origin.url("/f"), None, &mut connection()).await.unwrap_err();
    assert!(matches!(error, ProbeError::Status(404)), "{error:?}");
}

#[tokio::test]
async fn name_from_content_disposition() {
    let cases = [
        ("attachment; filename=\"plain.zip\"", "plain.zip"),
        ("attachment; filename*=UTF-8''%E2%82%AC%20rates.xlsx", "€ rates.xlsx"),
        ("attachment; filename=\"a.txt\"; filename*=UTF-8''b.txt", "b.txt"),
        ("attachment", "url-name.bin"),
    ];
    for (header, expected) in cases {
        let mut served = Served::new(10);
        served.content_disposition = Some(header.into());
        let origin = Origin::ok(served).await;
        let result = probe(&origin.url("/url-name.bin"), None, &mut connection()).await.unwrap();
        assert_eq!(result.suggested_file_name, expected, "{header}");
    }
    let origin = Origin::ok(Served::new(10)).await;
    let result = probe(&origin.url("/"), None, &mut connection()).await.unwrap();
    assert_eq!(result.suggested_file_name, "download");
}

#[tokio::test]
async fn cookies_are_chosen_per_hop() {
    // The link is on `localhost`; it redirects to `127.0.0.1` (its "CDN"), which must not get
    // the site's host-only cookie.
    let port = Arc::new(std::sync::OnceLock::<u16>::new());
    let seen_port = port.clone();
    let origin = Origin::start(Served::new(10), move |req, _| {
        if req.path == "/site" {
            Fault::Redirect(format!("http://127.0.0.1:{}/cdn", seen_port.get().unwrap()))
        } else {
            Fault::Ok
        }
    })
    .await;
    port.set(origin.addr.port()).unwrap();
    let browser = BrowserContext {
        cookies: vec![BrowserCookie {
            name: "session".into(),
            value: "secret".into(),
            domain: "localhost".into(),
            host_only: true,
            path: "/".into(),
            secure: false,
        }],
        referer: Some("http://localhost/page".into()),
        user_agent: Some("TestBrowser/1".into()),
        name: None,
    };
    let url = format!("http://localhost:{}/site", origin.addr.port());
    probe(&url, Some(&browser), &mut connection()).await.unwrap();
    let requests = origin.requests();
    assert_eq!(requests[0].header("cookie"), Some("session=secret"));
    assert_eq!(requests[1].path, "/cdn");
    assert_eq!(requests[1].header("cookie"), None);
    assert!(requests.iter().all(|r| r.header("user-agent") == Some("TestBrowser/1")));
    assert!(requests.iter().all(|r| r.header("referer") == Some("http://localhost/page")));
}

#[tokio::test]
async fn silent_server_fails_the_probe() {
    let origin = Origin::start(Served::new(10), |_, _| Fault::StallHeaders).await;
    let mut conn = StreamConnection::new(
        || Some(sonic_net::loopback()),
        Duration::from_millis(300),
        Duration::from_secs(1),
    );
    let error = probe(&origin.url("/f"), None, &mut conn).await.unwrap_err();
    assert!(matches!(error, ProbeError::Connection(_)), "{error:?}");
}

#[tokio::test]
async fn bad_links() {
    for url in ["ftp://example.com/f", "not a url", "file:///C:/x"] {
        let error = probe(url, None, &mut connection()).await.unwrap_err();
        assert!(matches!(error, ProbeError::InvalidUrl(_)), "{url}: {error:?}");
    }
}
