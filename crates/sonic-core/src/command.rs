//! The whole conversation between the UI and the backend.
//!
//! The UI sends [`Command`]s down a channel; the backend answers the ones that need an answer
//! through the [`Reply`] they carry, and tells the UI everything else as [`Event`]s. The UI never
//! touches the backend's state directly, so the backend can be swapped for a simulation (the UI
//! crate's demo mode) or driven by tests.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::oneshot;

use crate::{
    AppSettings, DownloadId, DownloadState, FinishedDownload, NetworkId, NetworkInterface,
    ProbeResult, StartRequest,
};

/// Where a command's answer goes. Dropping it unanswered reads as an error on the other end.
pub type Reply<T> = oneshot::Sender<Result<T, String>>;

/// Something the UI asks of the backend.
#[derive(Debug)]
pub enum Command {
    /// What a link would download. Answered with what the server says.
    Probe {
        url: String,
        reply: Reply<ProbeResult>,
    },
    /// Starts a download (or queues it). Answered with its id. `link`: the [`PendingLink`] it was
    /// started from, whose browser sign-in it takes.
    Start {
        request: StartRequest,
        link: Option<u64>,
        reply: Reply<DownloadId>,
    },
    Pause(DownloadId),
    Resume(DownloadId),
    /// Stops a download and deletes what it downloaded.
    Cancel(DownloadId),
    /// Removes a download from the list, cancelling it if under way. A finished one's file stays,
    /// unless `trash_file`: then it goes to the Recycle Bin.
    Remove {
        id: DownloadId,
        trash_file: bool,
    },
    /// A fresh link to the same file, for a download whose link stopped working. It resumes.
    Relink {
        id: DownloadId,
        url: String,
        reply: Reply<()>,
    },
    /// Switches one of a download's networks on or off, running or paused. Switching off the last
    /// one in use pauses the download; switching one back on resumes it.
    SetNetwork {
        id: DownloadId,
        network: NetworkId,
        enabled: bool,
    },
    /// Replaces the settings. The backend validates them and answers with what it saved.
    UpdateSettings {
        settings: AppSettings,
        reply: Reply<AppSettings>,
    },
    /// Shows a download's file in Explorer.
    Reveal(DownloadId),
    /// Opens a finished download with the OS default app.
    Open(DownloadId),
    ClearHistory,
    /// Zeroes a network's data usage for its current period.
    ResetNetworkUsage(NetworkId),
    /// Bytes free on the drive holding a folder; `None` when it can't be told.
    FreeSpace {
        dir: PathBuf,
        reply: Reply<Option<u64>>,
    },
    /// Forgets a link the browser handed over that won't be started.
    DismissLink(u64),
    /// The app is quitting: suspend every download and save. Answered once everything is on disk.
    Shutdown {
        reply: Reply<()>,
    },
}

/// A link handed to the app from outside (the browser extension), waiting for the user to start
/// it. The browser's sign-in stays in the backend; the UI only learns where it is from.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingLink {
    pub id: u64,
    pub url: String,
    /// Already probed, so New download needn't ask again (a single-use link would be spent).
    pub probe: Option<ProbeResult>,
    /// The page it came from.
    pub from: Option<String>,
    /// The browser it came from ("Chrome"). Its cookies stay in the backend.
    pub browser_name: Option<String>,
}

/// Something the backend tells the UI. States are shared (`Arc`), so fan-out is cheap.
#[derive(Clone, Debug)]
pub enum Event {
    /// The full picture, once at start: every unfinished download, oldest first.
    Downloads(Vec<Arc<DownloadState>>),
    /// One download changed. Sent at most a few times a second per download.
    DownloadUpdated(Arc<DownloadState>),
    /// A download left the list (removed, or moved to history).
    DownloadRemoved(DownloadId),
    /// Finished downloads, newest first.
    History(Arc<Vec<FinishedDownload>>),
    /// The computer's usable networks changed.
    Networks(Arc<Vec<NetworkInterface>>),
    /// Each network's data usage in its current period, by id.
    NetworkUsage(BTreeMap<NetworkId, u64>),
    Settings(Arc<AppSettings>),
    /// A link arrived from the browser.
    LinkReceived(PendingLink),
    /// Something the user should hear about that isn't tied to one screen.
    Notice {
        title: String,
        body: String,
    },
}
