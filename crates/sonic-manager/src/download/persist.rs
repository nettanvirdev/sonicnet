//! Saving a download's progress, one save at a time, in order.
//!
//! Each download has a persister task. Saves go to it in the order they were asked for, so a
//! checkpoint can never land after a later pause's save, and once the saved state is removed no
//! save after it recreates it — the store's `remove_download` waits out a save under way, but a
//! save *queued* after it would bring the download back on the next launch.

use std::io;

use sonic_core::{DownloadId, DownloadState, DownloadStatus, FinishedDownload, StartRequest};
use sonic_http::StagingFile;
use sonic_store::{Publication, Store};
use tokio::sync::{mpsc, oneshot};

/// What one save writes.
#[derive(Clone, Debug)]
pub(crate) struct Snapshot {
    pub state: DownloadState,
    pub request: StartRequest,
    pub staging: StagingFile,
    pub publication: Option<Publication>,
}

#[derive(Debug)]
enum Message {
    Save(Box<Snapshot>, Option<oneshot::Sender<io::Result<()>>>),
    Remove(oneshot::Sender<io::Result<()>>),
}

/// The handle on a download's persister.
#[derive(Debug, Clone)]
pub(crate) struct Persister {
    tx: mpsc::UnboundedSender<Message>,
}

impl Persister {
    pub(crate) fn start(store: Store, id: DownloadId) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(run(store, id, rx));
        Self { tx }
    }

    /// Queues a save and returns at once: a routine checkpoint, best-effort.
    pub(crate) fn save_later(&self, snapshot: Snapshot) {
        let _ = self.tx.send(Message::Save(Box::new(snapshot), None));
    }

    /// Saves, and waits until it is on disk.
    pub(crate) async fn save(&self, snapshot: Snapshot) -> io::Result<()> {
        let (done, wait) = oneshot::channel();
        self.tx
            .send(Message::Save(Box::new(snapshot), Some(done)))
            .map_err(|_| io::Error::other("the persister stopped"))?;
        wait.await.unwrap_or_else(|_| Err(io::Error::other("the persister stopped")))
    }

    /// Removes the saved state, after any save queued before it; no later save happens.
    pub(crate) async fn remove(&self) -> io::Result<()> {
        let (done, wait) = oneshot::channel();
        self.tx
            .send(Message::Remove(done))
            .map_err(|_| io::Error::other("the persister stopped"))?;
        wait.await.unwrap_or_else(|_| Err(io::Error::other("the persister stopped")))
    }
}

async fn run(store: Store, id: DownloadId, mut rx: mpsc::UnboundedReceiver<Message>) {
    let mut removed = false;
    while let Some(message) = rx.recv().await {
        match message {
            Message::Save(snapshot, done) => {
                let result = if removed { Ok(()) } else { save(&store, &snapshot).await };
                if let Err(error) = &result {
                    tracing::warn!(id = %snapshot.state.id, %error, "could not save a download");
                }
                if let Some(done) = done {
                    let _ = done.send(result);
                }
            }
            Message::Remove(done) => {
                removed = true;
                let _ = done.send(store.remove_download(id).await);
            }
        }
    }
}

async fn save(store: &Store, snapshot: &Snapshot) -> io::Result<()> {
    // The manifest claims its bytes are on disk: flush them there first. A download still under
    // way only; a finished one was flushed by its publication.
    if matches!(
        snapshot.state.status,
        DownloadStatus::Downloading | DownloadStatus::Paused | DownloadStatus::Queued
    ) && let Err(error) = snapshot.staging.sync().await
    {
        // A staging file that is gone can't be flushed; the manifest still records the state.
        tracing::debug!(%error, "could not flush the staging file");
    }
    store
        .save_download(
            &snapshot.state,
            &snapshot.request,
            snapshot.staging.path(),
            snapshot.publication.as_ref(),
        )
        .await
}

/// A finished download as history keeps it.
pub(crate) fn finished(state: &DownloadState) -> FinishedDownload {
    FinishedDownload {
        id: state.id,
        url: state.url.clone(),
        file_name: state.file_name.clone(),
        destination_path: state.destination_path.clone(),
        total_bytes: state.total_bytes.unwrap_or(state.bytes_downloaded),
        started_at: state.started_at,
        completed_at: state.completed_at.unwrap_or_else(sonic_core::now_millis),
        total_paused_ms: state.total_paused_ms,
        networks: state.networks.clone(),
        speed_history: state.speed_history.clone(),
        peak_speed_bps: state.peak_speed_bps,
        peak_streams: state.peak_streams,
        blocks_written: u32::try_from(state.blocks.len()).unwrap_or(u32::MAX),
        from_browser: state.from_browser.clone(),
        missing: false,
    }
}
