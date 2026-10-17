//! What a download is, end to end: one file, the exact bytes, whatever kind of server it comes
//! from. Every test here also gets the harness's automatic checks — each update obeys the
//! invariants, and a completed file equals the origin's bytes with no `.sonic` file left over.

use sonic_core::{BrowserContext, BrowserCookie, DownloadStatus};
use sonic_testkit::*;

/// The core promise on the simplest setup. The size is deliberately not a multiple of the block,
/// so the short last block (an off-by-one magnet) is part of it.
#[tokio::test(flavor = "multi_thread")]
async fn one_network_downloads_the_exact_file() {
    let origin = Origin::start().await;
    let size = 2 * MIB + 123;
    origin.add("a.bin", size as usize);
    let h = Harness::new(1);

    let id = h.start(&origin, "a.bin", &[]).await;
    let done = h.wait_status(id, DownloadStatus::Completed).await;

    assert_eq!(done.total_bytes, Some(size));
    assert_eq!(done.block_size, BLOCK, "the test tuning's block size is used");
    assert_eq!(done.blocks.len(), 33);
    assert_eq!(done.destination_path, h.dest.join("a.bin"));
    assert_eq!(carried(&done, &net(1)), size);
    let requests = origin.chunk_requests("a.bin");
    assert!(requests.iter().all(|r| r.peer == net_ip(1)), "every request left through net1");
    assert!(requests.iter().all(|r| r.status == 206), "every block by a range request");
}

/// The probe reports what the server says, for every kind of server the other tests use; a
/// wrong answer here (a range-less server reported as splittable, an empty file as unknown)
/// would send the download down the wrong path.
#[tokio::test(flavor = "multi_thread")]
async fn the_probe_reports_what_the_server_says() {
    let origin = Origin::start().await;
    origin.add("ranged.bin", 5000);
    origin.add("plain.bin", 5000);
    origin.edit("plain.bin", |f| f.ranges = false);
    origin.add("unknown.bin", 5000);
    origin.edit("unknown.bin", |f| f.unknown_size = true);
    origin.add("refusing.bin", 5000);
    origin.edit("refusing.bin", |f| f.refuse_ranges = true);
    origin.add("empty.bin", 0);
    let h = Harness::new(1);

    let ranged = h.probe(&origin.url("ranged.bin")).await.unwrap();
    assert_eq!((ranged.total_bytes, ranged.supports_ranges), (Some(5000), true));
    assert_eq!(ranged.etag.as_deref(), Some("\"ranged.bin-v1\""));
    assert_eq!(ranged.suggested_file_name, "ranged.bin");
    let plain = h.probe(&origin.url("plain.bin")).await.unwrap();
    assert_eq!((plain.total_bytes, plain.supports_ranges), (Some(5000), false));
    let unknown = h.probe(&origin.url("unknown.bin")).await.unwrap();
    assert_eq!((unknown.total_bytes, unknown.supports_ranges), (None, false));
    let refusing = h.probe(&origin.url("refusing.bin")).await.unwrap();
    assert_eq!((refusing.total_bytes, refusing.supports_ranges), (Some(5000), false));
    let empty = h.probe(&origin.url("empty.bin")).await.unwrap();
    assert_eq!(empty.total_bytes, Some(0));
    assert!(h.probe(&origin.url("missing.bin")).await.is_err(), "a 404 is an error");
}

/// A server that ignores Range can only be read start to finish by one request: a second
/// stream would get the whole file again, and a second network would double the traffic for
/// nothing. However many networks are picked, one stream on one network carries it.
#[tokio::test(flavor = "multi_thread")]
async fn a_server_without_ranges_is_one_stream_on_one_network() {
    let origin = Origin::start().await;
    origin.add("plain.bin", (MIB + 77) as usize);
    origin.edit("plain.bin", |f| f.ranges = false);
    let h = Harness::new(2);

    let id = h.start(&origin, "plain.bin", &[net(1), net(2)]).await;
    let done = h.wait_status(id, DownloadStatus::Completed).await;

    assert_eq!(origin.chunk_requests("plain.bin").len(), 1, "one request carried it all");
    assert!(h.states(id).iter().all(|s| s.streams.len() <= 1), "never a second stream");
    assert!(done.peak_streams <= 1);
    let carriers = done.networks.iter().filter(|n| n.bytes_downloaded > 0).count();
    assert_eq!(carriers, 1, "one network carried it");
}

/// Some servers refuse any request with a Range (403) yet serve the URL in full. The probe asks
/// again without one; the download must then never send a Range, or every request fails.
#[tokio::test(flavor = "multi_thread")]
async fn a_server_refusing_ranges_is_downloaded_whole() {
    let origin = Origin::start().await;
    origin.add("refusing.bin", (MIB / 2) as usize);
    origin.edit("refusing.bin", |f| f.refuse_ranges = true);
    let h = Harness::new(2);

    let id = h.start(&origin, "refusing.bin", &[]).await;
    h.wait_status(id, DownloadStatus::Completed).await;
    let requests = origin.chunk_requests("refusing.bin");
    assert!(requests.iter().all(|r| r.range.is_none()), "no Range after the probe: {requests:?}");
}

/// No Content-Length (chunked): the size is learnt only at the end. One open-ended block; the
/// file must still come out exact and the download complete rather than wait for a size.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_of_unknown_size() {
    let origin = Origin::start().await;
    origin.add("unknown.bin", (MIB + 5) as usize);
    origin.edit("unknown.bin", |f| f.unknown_size = true);
    let h = Harness::new(1);

    let id = h.start(&origin, "unknown.bin", &[]).await;
    let done = h.wait_status(id, DownloadStatus::Completed).await;
    assert_eq!(done.bytes_downloaded, MIB + 5);
}

/// An empty file has nothing to fetch, which is easy to get wrong both ways: waiting forever
/// for bytes, or "completing" without creating the file.
#[tokio::test(flavor = "multi_thread")]
async fn an_empty_file() {
    let origin = Origin::start().await;
    origin.add("empty.bin", 0);
    let h = Harness::new(1);

    let id = h.start(&origin, "empty.bin", &[]).await;
    let done = h.wait_status(id, DownloadStatus::Completed).await;
    assert_eq!(std::fs::metadata(&done.destination_path).expect("the file exists").len(), 0);
}

/// A link that redirects (a mirror picker): the probe follows it, and the download fetches the
/// final URL directly rather than bouncing every block off the redirect.
#[tokio::test(flavor = "multi_thread")]
async fn a_redirected_link_downloads_the_target() {
    let origin = Origin::start().await;
    origin.add("real.bin", MIB as usize);
    origin.copy("real.bin", "link.bin");
    origin.redirect("link.bin", "/real.bin", false);
    let h = Harness::new(1);

    let id = h.start(&origin, "link.bin", &[]).await;
    let done = h.wait_status(id, DownloadStatus::Completed).await;
    assert_eq!(done.url, origin.url("real.bin"));
    assert!(origin.chunk_requests("link.bin").is_empty());
    assert!(!origin.chunk_requests("real.bin").is_empty());
}

/// A link that redirects every request (a CDN signing each one): every block request must
/// follow it too, and still land its bytes at the right offsets.
#[tokio::test(flavor = "multi_thread")]
async fn every_request_redirected_still_downloads() {
    let origin = Origin::start().await;
    origin.add("real.bin", MIB as usize);
    origin.copy("real.bin", "signed.bin");
    origin.redirect("signed.bin", "/real.bin", false);
    let h = Harness::new(1);

    let request = h.request_for(&origin, "signed.bin", &[]);
    let id = h.start_request(request, Some(origin.expected_bytes("real.bin"))).await.unwrap();
    h.wait_status(id, DownloadStatus::Completed).await;
    assert!(origin.chunk_requests("signed.bin").iter().all(|r| r.status == 302));
    assert!(origin.chunk_requests("real.bin").iter().all(|r| r.status == 206));
}

/// A download behind a sign-in works only with the browser's cookies (StartRequest.browser).
/// Every request must carry them — a block fetched without them would get the login page, or
/// a 403.
#[tokio::test(flavor = "multi_thread")]
async fn a_cookie_protected_file_downloads_with_the_browsers_cookies() {
    let origin = Origin::start().await;
    origin.add("private.bin", MIB as usize);
    origin.require_cookie("private.bin", "session=s3cret", 403);
    let h = Harness::new(2);

    let mut request = h.request_for(&origin, "private.bin", &[]);
    request.browser = Some(BrowserContext {
        cookies: vec![BrowserCookie {
            name: "session".into(),
            value: "s3cret".into(),
            domain: "127.0.0.1".into(),
            host_only: true,
            path: "/".into(),
            secure: false,
        }],
        referer: Some("http://127.0.0.1/page".into()),
        user_agent: Some("TestBrowser/1.0".into()),
        name: Some("Chrome".into()),
    });
    let id = h.start_request(request, Some(origin.expected_bytes("private.bin"))).await.unwrap();
    let done = h.wait_status(id, DownloadStatus::Completed).await;

    let requests = origin.chunk_requests("private.bin");
    assert!(!requests.is_empty());
    assert!(requests.iter().all(|r| r.status == 206), "never refused: {requests:?}");
    assert_eq!(done.from_browser.as_deref(), Some("Chrome"));
}
