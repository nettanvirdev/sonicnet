use super::*;
use proptest::prelude::*;
use sonic_core::{BrowserContext, BrowserCookie, DownloadNetwork, NetworkKind, NetworkStatus};

const BLOCK: u64 = 10;

fn net(id: &str) -> NetworkId {
    NetworkId::new(id)
}

struct Fixture {
    dir: tempfile::TempDir,
    store: Store,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::at(dir.path().join("data"));
        Self { dir, store }
    }

    fn destination(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn staging(&self, name: &str) -> PathBuf {
        self.dir.path().join(format!("{name}{STAGING_SUFFIX}"))
    }

    fn manifest_dir(&self, id: DownloadId) -> PathBuf {
        self.store.downloads_dir().join(id.to_string())
    }
}

/// A 25-byte download in 10-byte blocks: network a delivered all of block 0 and 4 bytes of block
/// 1, network b 6 bytes of block 1.
fn state(destination: PathBuf, status: DownloadStatus) -> DownloadState {
    let mut blocks = plan_blocks(Some(25), BLOCK);
    blocks[0].bytes_downloaded = 10;
    blocks[0].bytes_by_network.insert(net("a"), 10);
    blocks[0].status = BlockStatus::Completed;
    blocks[1].bytes_downloaded = 10;
    blocks[1].bytes_by_network.insert(net("a"), 4);
    blocks[1].bytes_by_network.insert(net("b"), 6);
    blocks[1].status = BlockStatus::Completed;
    let network = |id: &str| DownloadNetwork {
        id: net(id),
        label: id.into(),
        kind: NetworkKind::Wifi,
        enabled: true,
        status: NetworkStatus::On,
        error: None,
        bytes_downloaded: 999,
        speed_bps: 123.0,
        retries: 0,
    };
    DownloadState {
        id: DownloadId::new(),
        url: "https://example.com/f.bin".into(),
        file_name: "f.bin".into(),
        destination_path: destination,
        total_bytes: Some(25),
        bytes_downloaded: 20,
        speed_bps: 500.0,
        time_left_secs: Some(3.0),
        status,
        error: None,
        resumable: true,
        started_at: 1_000,
        queued_at: None,
        paused_at: None,
        total_paused_ms: 0,
        completed_at: None,
        scheduled: false,
        networks: vec![network("a"), network("b")],
        streams: vec![sonic_core::StreamState {
            id: 0,
            network_id: net("a"),
            status: sonic_core::StreamStatus::Downloading,
            block: Some(2),
            hedge: false,
            bytes_downloaded: 0,
            speed_bps: 1.0,
        }],
        peak_streams: 8,
        blocks,
        block_size: BLOCK,
        speed_history: BTreeMap::from([(net("a"), vec![1.0; 60])]),
        peak_speed_bps: None,
        disk_limited: true,
        from_browser: Some("Chrome".into()),
    }
}

fn request(state: &DownloadState) -> StartRequest {
    StartRequest {
        url: state.url.clone(),
        destination_dir: state.destination_path.parent().unwrap().to_path_buf(),
        file_name: state.file_name.clone(),
        total_bytes: state.total_bytes,
        supports_ranges: true,
        network_ids: vec![net("a"), net("b")],
        streams_per_network: None,
        etag: Some("\"e\"".into()),
        last_modified: None,
        browser: Some(BrowserContext {
            cookies: vec![BrowserCookie {
                name: "sid".into(),
                value: "top-secret-cookie".into(),
                domain: "example.com".into(),
                host_only: true,
                path: "/".into(),
                secure: true,
            }],
            referer: Some("https://example.com/page".into()),
            user_agent: Some("UA".into()),
            name: Some("Chrome".into()),
        }),
    }
}

/// Saves `state` with a staging file of `staging_len` bytes (none for `None`).
async fn save(
    fx: &Fixture,
    state: &DownloadState,
    staging_len: Option<u64>,
    publication: Option<&Publication>,
) -> PathBuf {
    let staging = fx.staging(&state.file_name);
    if let Some(len) = staging_len {
        fs::File::create(&staging).unwrap().set_len(len).unwrap();
    }
    fx.store.save_download(state, &request(state), &staging, publication).await.unwrap();
    staging
}

#[tokio::test]
async fn round_trip_restores_paused_without_streams_or_speeds() {
    let fx = Fixture::new();
    let original = state(fx.destination("f.bin"), DownloadStatus::Downloading);
    let staging = save(&fx, &original, Some(25), None).await;

    let restored = fx.store.list_saved().await;
    assert_eq!(restored.len(), 1);
    let RestoredDownload { state, request: req, staging_path, publication } = &restored[0];
    assert_eq!(staging_path, &staging);
    assert_eq!(publication, &None);
    assert_eq!(state.status, DownloadStatus::Paused);
    assert!(state.paused_at.is_some_and(|at| at >= original.started_at));
    assert!(state.streams.is_empty());
    assert_eq!(state.speed_bps, 0.0);
    assert_eq!(state.time_left_secs, None);
    assert!(!state.disk_limited);
    assert!(state.speed_history.is_empty());
    assert!(state.networks.iter().all(|n| n.speed_bps == 0.0));
    // Blocks and totals come back from the columns.
    assert_eq!(state.blocks, original.blocks);
    assert_eq!(state.bytes_downloaded, 20);
    assert_eq!(state.networks[0].bytes_downloaded, 14);
    assert_eq!(state.networks[1].bytes_downloaded, 6);

    let expected = request(&original);
    let browser = req.browser.as_ref().unwrap();
    assert_eq!(browser.referer, expected.browser.as_ref().unwrap().referer);
    if cfg!(windows) {
        assert_eq!(req, &expected);
    } else {
        assert!(browser.cookies.is_empty());
    }
}

#[tokio::test]
async fn cookies_never_in_the_clear_and_dropped_once_completed() {
    let fx = Fixture::new();
    let mut st = state(fx.destination("f.bin"), DownloadStatus::Paused);
    save(&fx, &st, Some(25), None).await;
    let path = fx.store.manifest_path(st.id);
    let raw = fs::read_to_string(&path).unwrap();
    assert!(!raw.contains("top-secret-cookie"));
    assert_eq!(serde_json::from_str::<Value>(&raw).unwrap()["version"], MANIFEST_VERSION);

    st.status = DownloadStatus::Completed;
    save(&fx, &st, Some(25), None).await;
    let raw: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(raw["sealed_cookies"], Value::Null);
}

#[tokio::test]
async fn scheduled_comes_back_queued_in_its_place() {
    let fx = Fixture::new();
    let mut st = state(fx.destination("f.bin"), DownloadStatus::Queued);
    st.scheduled = true;
    st.queued_at = Some(5_000);
    save(&fx, &st, Some(25), None).await;
    let restored = &fx.store.list_saved().await[0].state;
    assert_eq!(restored.status, DownloadStatus::Queued);
    assert_eq!(restored.queued_at, Some(5_000));
}

#[tokio::test]
async fn error_state_kept_as_is() {
    let fx = Fixture::new();
    let mut st = state(fx.destination("f.bin"), DownloadStatus::Error);
    st.error = Some("server said no".into());
    save(&fx, &st, None, None).await;
    let restored = &fx.store.list_saved().await[0].state;
    assert_eq!(restored.status, DownloadStatus::Error);
    assert_eq!(restored.error.as_deref(), Some("server said no"));
    assert!(restored.resumable);
}

#[tokio::test]
async fn missing_staging_file_fails_unless_nothing_was_downloaded() {
    let fx = Fixture::new();
    let st = state(fx.destination("f.bin"), DownloadStatus::Paused);
    save(&fx, &st, None, None).await;
    let restored = &fx.store.list_saved().await[0].state;
    assert_eq!(restored.status, DownloadStatus::Error);
    assert!(!restored.resumable);
    assert_eq!(restored.bytes_downloaded, 0);

    let fx = Fixture::new();
    let mut st = state(fx.destination("g.bin"), DownloadStatus::Paused);
    st.blocks = plan_blocks(Some(25), BLOCK);
    save(&fx, &st, None, None).await;
    assert_eq!(fx.store.list_saved().await[0].state.status, DownloadStatus::Paused);
}

#[tokio::test]
async fn blocks_past_the_staging_files_end_start_over() {
    let fx = Fixture::new();
    let st = state(fx.destination("f.bin"), DownloadStatus::Paused);
    save(&fx, &st, Some(15), None).await;
    let restored = &fx.store.list_saved().await[0].state;
    assert_eq!(restored.blocks[0].status, BlockStatus::Completed);
    assert_eq!(restored.blocks[1].bytes_downloaded, 0);
    assert_eq!(restored.blocks[1].status, BlockStatus::Pending);
    assert_eq!(restored.bytes_downloaded, 10);
}

fn finished(fx: &Fixture) -> DownloadState {
    let mut st = state(fx.destination("f.bin"), DownloadStatus::Downloading);
    st.blocks[2].bytes_downloaded = 5;
    st.blocks[2].bytes_by_network.insert(net("b"), 5);
    st.blocks[2].status = BlockStatus::Completed;
    st
}

#[tokio::test]
async fn publish_interrupted_after_the_rename_completes() {
    let fx = Fixture::new();
    let st = finished(&fx);
    let staging = fx.staging("f.bin");
    fs::write(&staging, [7u8; 25]).unwrap();
    // The publication is a name with a suffix: the destination was taken.
    let published = fx.destination("f (1).bin");
    let publication =
        Publication { path: published.clone(), identity: file_identity(&staging).unwrap() };
    save(&fx, &st, None, Some(&publication)).await;
    // The rename happened; the app died before saving again.
    fs::rename(&staging, &published).unwrap();

    let restored = fx.store.list_saved().await;
    let restored = &restored[0].state;
    assert_eq!(restored.status, DownloadStatus::Completed);
    assert_eq!(restored.destination_path, published);
    assert_eq!(restored.file_name, "f (1).bin");
    assert!(restored.completed_at.is_some());
    assert!(published.exists());
}

#[tokio::test]
async fn publish_interrupted_before_the_rename_stays_paused() {
    let fx = Fixture::new();
    let st = finished(&fx);
    let staging = fx.staging("f.bin");
    fs::write(&staging, [7u8; 25]).unwrap();
    let publication =
        Publication { path: fx.destination("f.bin"), identity: file_identity(&staging).unwrap() };
    save(&fx, &st, None, Some(&publication)).await;

    let restored = fx.store.list_saved().await;
    assert_eq!(restored[0].state.status, DownloadStatus::Paused);
    assert!(restored[0].state.blocks.iter().all(|b| b.status == BlockStatus::Completed));
    assert_eq!(restored[0].publication, Some(publication));
    assert!(staging.exists());
}

#[tokio::test]
async fn another_file_at_the_publication_path_is_not_ours() {
    let fx = Fixture::new();
    let st = finished(&fx);
    let staging = fx.staging("f.bin");
    fs::write(&staging, [7u8; 25]).unwrap();
    let publication =
        Publication { path: fx.destination("f.bin"), identity: file_identity(&staging).unwrap() };
    save(&fx, &st, None, Some(&publication)).await;
    // The staging file vanished and some other 25-byte file sits at the destination.
    fs::remove_file(&staging).unwrap();
    fs::write(fx.destination("f.bin"), [1u8; 25]).unwrap();

    let restored = &fx.store.list_saved().await[0].state;
    assert_eq!(restored.status, DownloadStatus::Error);
    assert!(!restored.resumable);
    assert_eq!(fs::read(fx.destination("f.bin")).unwrap(), [1u8; 25]);
}

#[tokio::test]
async fn unrestorable_manifests_are_discarded_with_only_our_files() {
    let fx = Fixture::new();
    let downloads = fx.store.downloads_dir();

    // Corrupt JSON: folder goes, nothing else known.
    let corrupt = downloads.join(DownloadId::new().to_string());
    fs::create_dir_all(&corrupt).unwrap();
    fs::write(corrupt.join(MANIFEST_FILE), "{oops").unwrap();

    // No manifest at all.
    let empty = downloads.join(DownloadId::new().to_string());
    fs::create_dir_all(&empty).unwrap();

    // Another version: folder and its .sonic staging file go.
    let old_staging = fx.staging("old.bin");
    fs::write(&old_staging, b"x").unwrap();
    let old = downloads.join(DownloadId::new().to_string());
    fs::create_dir_all(&old).unwrap();
    let raw = serde_json::json!({"version": 99, "staging_path": old_staging});
    fs::write(old.join(MANIFEST_FILE), raw.to_string()).unwrap();

    // Naming a file that isn't a staging file: that file is never touched.
    let precious = fx.destination("precious.docx");
    fs::write(&precious, b"keep me").unwrap();
    let hostile = downloads.join(DownloadId::new().to_string());
    fs::create_dir_all(&hostile).unwrap();
    let raw = serde_json::json!({"version": 99, "staging_path": precious});
    fs::write(hostile.join(MANIFEST_FILE), raw.to_string()).unwrap();

    // A plan that can't be: progress for a 1 TB file in 1-byte blocks.
    let mut absurd = state(fx.destination("absurd.bin"), DownloadStatus::Paused);
    absurd.total_bytes = Some(1 << 40);
    absurd.block_size = 1;
    let absurd_staging = save(&fx, &absurd, Some(1), None).await;

    // A loose file in downloads/ is left alone.
    fs::write(downloads.join("notes.txt"), b"hi").unwrap();

    assert!(fx.store.list_saved().await.is_empty());
    for dir in [&corrupt, &empty, &old, &hostile, &fx.manifest_dir(absurd.id)] {
        assert!(!dir.exists(), "{} should be gone", dir.display());
    }
    assert!(!old_staging.exists());
    assert!(!absurd_staging.exists());
    assert_eq!(fs::read(&precious).unwrap(), b"keep me");
    assert!(downloads.join("notes.txt").exists());
}

#[tokio::test]
async fn manifest_in_the_wrong_folder_is_discarded() {
    let fx = Fixture::new();
    let st = state(fx.destination("f.bin"), DownloadStatus::Paused);
    save(&fx, &st, Some(25), None).await;
    let moved = fx.store.downloads_dir().join(DownloadId::new().to_string());
    fs::rename(fx.manifest_dir(st.id), &moved).unwrap();
    assert!(fx.store.list_saved().await.is_empty());
    assert!(!moved.exists());
}

#[tokio::test]
async fn one_staging_file_one_download_newest_kept() {
    let fx = Fixture::new();
    let mut older = state(fx.destination("f.bin"), DownloadStatus::Paused);
    older.started_at = 1;
    let mut newer = state(fx.destination("f.bin"), DownloadStatus::Paused);
    newer.started_at = 2;
    let staging = save(&fx, &older, Some(25), None).await;
    save(&fx, &newer, Some(25), None).await;
    // An unrestorable one naming the same staging file must not take it with it.
    let mut broken = state(fx.destination("f.bin"), DownloadStatus::Paused);
    broken.block_size = 0;
    save(&fx, &broken, Some(25), None).await;

    let restored = fx.store.list_saved().await;
    assert_eq!(restored.len(), 1);
    assert_eq!(restored[0].state.id, newer.id);
    assert!(!fx.manifest_dir(older.id).exists());
    assert!(!fx.manifest_dir(broken.id).exists());
    assert!(staging.exists());
}

#[tokio::test]
async fn oldest_first_and_remove() {
    let fx = Fixture::new();
    let mut ids = Vec::new();
    for (name, started_at) in [("b.bin", 20), ("a.bin", 10), ("c.bin", 30)] {
        let mut st = state(fx.destination(name), DownloadStatus::Paused);
        st.file_name = name.into();
        st.started_at = started_at;
        save(&fx, &st, Some(25), None).await;
        ids.push((started_at, st.id));
    }
    ids.sort();
    let order: Vec<_> = fx.store.list_saved().await.iter().map(|d| d.state.id).collect();
    assert_eq!(order, ids.iter().map(|(_, id)| *id).collect::<Vec<_>>());

    fx.store.remove_download(ids[0].1).await.unwrap();
    fx.store.remove_download(ids[0].1).await.unwrap(); // twice is fine
    assert_eq!(fx.store.list_saved().await.len(), 2);
}

#[tokio::test]
async fn stale_temp_files_are_cleared() {
    let fx = Fixture::new();
    let st = state(fx.destination("f.bin"), DownloadStatus::Paused);
    save(&fx, &st, Some(25), None).await;
    let temp = fx.manifest_dir(st.id).join("manifest.json.4242.tmp");
    fs::write(&temp, b"half").unwrap();
    assert_eq!(fx.store.list_saved().await.len(), 1);
    assert!(!temp.exists());
}

#[tokio::test]
async fn save_refuses_a_non_staging_path() {
    let fx = Fixture::new();
    let st = state(fx.destination("f.bin"), DownloadStatus::Paused);
    let result = fx.store.save_download(&st, &request(&st), &fx.destination("f.bin"), None).await;
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn unknown_size_is_one_block_completed_only_by_the_flag() {
    let progress = BTreeMap::from([(net("a"), vec![1234])]);
    let blocks = restore_blocks(None, BLOCK, &progress, false).unwrap();
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].bytes_downloaded, 1234);
    assert_eq!(blocks[0].status, BlockStatus::Pending);
    let blocks = restore_blocks(None, BLOCK, &progress, true).unwrap();
    assert_eq!(blocks[0].status, BlockStatus::Completed);
    // An empty file is complete as planned.
    let blocks = restore_blocks(Some(0), BLOCK, &BTreeMap::new(), false).unwrap();
    assert_eq!(blocks[0].status, BlockStatus::Completed);
}

#[test]
fn mismatched_progress_starts_over() {
    let fresh = plan_blocks(Some(25), BLOCK);
    let cases = [
        BTreeMap::from([(net("a"), vec![10, 10])]), // column too short
        BTreeMap::from([(net("a"), vec![10, 10, 5, 0])]), // column too long
        BTreeMap::from([(net("a"), vec![11, 0, 0])]), // over a block's length
        BTreeMap::from([(net("a"), vec![0, 0, 3]), (net("b"), vec![0, 0, 3])]), // together over
        BTreeMap::from([(net("a"), vec![u64::MAX, 0, 0]), (net("b"), vec![1, 0, 0])]), // overflow
    ];
    for progress in cases {
        assert_eq!(restore_blocks(Some(25), BLOCK, &progress, false), Some(fresh.clone()));
    }
    // Refused outright: no plan.
    assert_eq!(restore_blocks(Some(25), 0, &BTreeMap::new(), false), None);
    assert_eq!(restore_blocks(Some(u64::MAX), 1, &BTreeMap::new(), false), None);
}

fn plans() -> impl Strategy<Value = (u64, u64)> {
    (0u64..200, 1u64..40)
}

proptest! {
    /// Saving then restoring gives back exactly the blocks that were saved.
    #[test]
    fn save_restore_round_trip(
        (total, block_size) in plans(),
        shares in proptest::collection::vec((0u64..=100, 0u64..=100), 0..200),
    ) {
        let mut blocks = plan_blocks(Some(total), block_size);
        for (block, &(a, b)) in blocks.iter_mut().zip(&shares) {
            let len = block.range.len().unwrap();
            let a = a * len / 100;
            let b = (b * (len - a)) / 100;
            if a > 0 { block.bytes_by_network.insert(net("a"), a); }
            if b > 0 { block.bytes_by_network.insert(net("b"), b); }
            block.bytes_downloaded = a + b;
        }
        for block in &mut blocks {
            if Some(block.bytes_downloaded) == block.range.len() {
                block.status = BlockStatus::Completed;
            }
        }
        let (progress, complete) = progress_of(&blocks);
        prop_assert_eq!(restore_blocks(Some(total), block_size, &progress, complete), Some(blocks));
    }

    /// Whatever the columns say, what comes back is either the fresh plan or blocks that fit it:
    /// no block over its length, and every block's bytes the sum of its networks'.
    #[test]
    fn restore_never_trusts_what_does_not_fit(
        (total, block_size) in plans(),
        columns in proptest::collection::btree_map(
            "[a-c]", proptest::collection::vec(0u64..30, 0..30), 0..3),
        complete: bool,
    ) {
        let progress: BTreeMap<NetworkId, Vec<u64>> =
            columns.into_iter().map(|(id, column)| (NetworkId::new(id), column)).collect();
        let blocks = restore_blocks(Some(total), block_size, &progress, complete).unwrap();
        let plan = plan_blocks(Some(total), block_size);
        prop_assert_eq!(blocks.len(), plan.len());
        for (block, planned) in blocks.iter().zip(&plan) {
            prop_assert_eq!(block.range, planned.range);
            let len = block.range.len().unwrap();
            prop_assert!(block.bytes_downloaded <= len);
            prop_assert_eq!(block.bytes_downloaded, block.bytes_by_network.values().sum::<u64>());
            prop_assert_eq!(block.status == BlockStatus::Completed, block.bytes_downloaded == len);
        }
    }
}
