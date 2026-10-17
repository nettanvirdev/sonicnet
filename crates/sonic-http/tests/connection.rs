//! StreamConnection against the test origin: keep-alive, the stale-connection retry, timeouts.

mod support;

use std::time::{Duration, Instant};

use http_body_util::BodyExt;
use sonic_http::{ConnectionError, StreamConnection, request_headers};
use support::{Fault, Origin, Served, connection};
use url::Url;

async fn get(
    conn: &mut StreamConnection,
    url: &str,
) -> Result<(u16, Vec<u8>, bool), ConnectionError> {
    let url = Url::parse(url).unwrap();
    let start = conn.request(&url, &request_headers(&url, None)).await?;
    let status = start.response.status().as_u16();
    let body = start.response.into_body().collect().await?.to_bytes().to_vec();
    Ok((status, body, start.reused))
}

#[tokio::test]
async fn one_connection_serves_many_requests() {
    let origin = Origin::ok(Served::new(4096)).await;
    let mut conn = connection();
    let mut reused = Vec::new();
    for _ in 0..4 {
        let (status, body, was_reused) = get(&mut conn, &origin.url("/f")).await.unwrap();
        assert_eq!((status, body.len()), (200, 4096));
        reused.push(was_reused);
    }
    assert_eq!(reused, [false, true, true, true]);
    assert_eq!(origin.connections(), 1);
    let requests = origin.requests();
    assert!(requests.iter().all(|r| r.connection == 1));
    // Host is set, as hyper's connection-level client doesn't.
    assert_eq!(
        requests[0].header("host"),
        Some(format!("127.0.0.1:{}", origin.addr.port()).as_str())
    );
}

#[tokio::test]
async fn stale_kept_connection_is_retried_once_on_a_new_one() {
    // The second request arrives on the kept connection, which the server then closes without
    // a word — as a server does with an idle connection it already gave up on.
    let origin =
        Origin::start(Served::new(100), |req, _| if req.n == 2 { Fault::Close } else { Fault::Ok })
            .await;
    let mut conn = connection();
    get(&mut conn, &origin.url("/f")).await.unwrap();
    let (status, body, reused) = get(&mut conn, &origin.url("/f")).await.unwrap();
    assert_eq!((status, body.len(), reused), (200, 100, false));
    assert_eq!(origin.connections(), 2);
    let on: Vec<usize> = origin.requests().iter().map(|r| r.connection).collect();
    assert_eq!(on, [1, 1, 2]);
}

#[tokio::test]
async fn a_new_connection_that_fails_is_not_retried() {
    let origin = Origin::start(Served::new(100), |_, _| Fault::Close).await;
    let mut conn = connection();
    let error = get(&mut conn, &origin.url("/f")).await.unwrap_err();
    assert!(matches!(error, ConnectionError::Http(_)), "{error:?}");
    assert_eq!(origin.requests().len(), 1);
}

#[tokio::test]
async fn reused_connection_failing_twice_gives_up() {
    let origin =
        Origin::start(Served::new(100), |req, _| if req.n >= 2 { Fault::Close } else { Fault::Ok })
            .await;
    let mut conn = connection();
    get(&mut conn, &origin.url("/f")).await.unwrap();
    assert!(get(&mut conn, &origin.url("/f")).await.is_err());
    assert_eq!(origin.requests().len(), 3);
}

#[tokio::test]
async fn headers_that_never_come_time_out() {
    let origin = Origin::start(Served::new(100), |_, _| Fault::StallHeaders).await;
    let mut conn = StreamConnection::new(
        || Some(sonic_net::loopback()),
        Duration::from_millis(300),
        Duration::from_secs(1),
    );
    let started = Instant::now();
    let error = get(&mut conn, &origin.url("/f")).await.unwrap_err();
    assert!(matches!(error, ConnectionError::Timeout), "{error:?}");
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn no_network_no_connection() {
    let origin = Origin::ok(Served::new(10)).await;
    let mut conn = StreamConnection::new(|| None, Duration::from_secs(1), Duration::from_secs(1));
    let error = get(&mut conn, &origin.url("/f")).await.unwrap_err();
    assert!(matches!(error, ConnectionError::NetworkGone), "{error:?}");
    assert_eq!(origin.connections(), 0);
}

#[tokio::test]
async fn reconnect_opens_a_new_connection() {
    let origin = Origin::ok(Served::new(10)).await;
    let mut conn = connection();
    get(&mut conn, &origin.url("/f")).await.unwrap();
    conn.reconnect();
    let (_, _, reused) = get(&mut conn, &origin.url("/f")).await.unwrap();
    assert!(!reused);
    assert_eq!(origin.connections(), 2);
}

#[tokio::test]
async fn an_abandoned_body_or_request_is_not_reused() {
    // A body dropped midway leaves bytes on its connection: the next request needs a new one.
    let origin = Origin::start(Served::new(1000), |req, _| match req.n {
        1 => Fault::StallBody,
        3 => Fault::StallHeaders,
        _ => Fault::Ok,
    })
    .await;
    let mut conn = connection();
    let url = Url::parse(&origin.url("/f")).unwrap();
    let start = conn.request(&url, &request_headers(&url, None)).await.unwrap();
    drop(start);
    let (status, body, reused) = get(&mut conn, &origin.url("/f")).await.unwrap();
    assert_eq!((status, body.len(), reused), (200, 1000, false));

    // A request dropped while waiting for its response (cancelled).
    let headers = request_headers(&url, None);
    let cancelled = tokio::time::timeout(Duration::from_millis(200), conn.request(&url, &headers));
    assert!(cancelled.await.is_err());
    let (status, _, reused) = get(&mut conn, &origin.url("/f")).await.unwrap();
    assert_eq!((status, reused), (200, false));
    assert_eq!(origin.connections(), 3);
}

#[tokio::test]
async fn unsupported_urls_are_refused() {
    let mut conn = connection();
    let url = Url::parse("ftp://127.0.0.1/f").unwrap();
    let error = conn.request(&url, &request_headers(&url, None)).await.unwrap_err();
    assert!(matches!(error, ConnectionError::UnsupportedUrl(_)), "{error:?}");
}
