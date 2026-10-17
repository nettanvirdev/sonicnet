//! Everything SonicNet keeps on disk: settings, history, saved progress, data usage.
//!
//! All of it lives under one data root (`%APPDATA%\SonicNet` by default), held by a [`Store`].
//! There are no globals: tests root a `Store` in a temporary directory, and two stores rooted in
//! different places never touch each other's files.
//!
//! Every file is JSON, written atomically (see [`json`]), and every read trusts nothing past
//! "this parsed": a hand-edited or older file loses only the values that don't fit.

mod history;
mod json;
mod manifest;
mod seal;
mod settings;
mod usage;

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub use history::HISTORY_CAP;
pub use manifest::{
    FileIdentity, MANIFEST_VERSION, Publication, RestoredDownload, file_identity, restore_blocks,
};
pub use seal::{seal, seal_cookies, unseal, unseal_cookies};
pub use settings::sanitize_settings;

/// The folder under the OS's per-user data directory that holds everything.
const APP_DIR: &str = "SonicNet";
const SETTINGS_FILE: &str = "settings.json";
const HISTORY_FILE: &str = "history.json";
const USAGE_FILE: &str = "usage.json";
const DOWNLOADS_DIR: &str = "downloads";
const MANIFEST_FILE: &str = "manifest.json";

/// A handle on SonicNet's data root. Cheap to clone; clones share the per-file write queues, so
/// every part of the app should use clones of one `Store` rather than opening several.
#[derive(Clone, Debug)]
pub struct Store {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    root: PathBuf,
    /// One lock per file path: writes (and read-modify-writes) to one file run one at a time, so
    /// two quick saves can't interleave and drop each other's change.
    // ponytail: entries are never removed (one per file ever written in a session); prune on
    // remove_download if sessions with thousands of downloads show up.
    locks: Mutex<HashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>>,
}

impl Store {
    /// The user's data root: `%APPDATA%\SonicNet` on Windows (the roaming profile, like most
    /// desktop apps' settings), the platform equivalent elsewhere.
    pub fn open_default() -> io::Result<Self> {
        let base = directories::BaseDirs::new()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no home directory"))?;
        Ok(Self::at(base.data_dir().join(APP_DIR)))
    }

    /// A store rooted at `root`. Nothing is created until something is saved.
    pub fn at(root: impl Into<PathBuf>) -> Self {
        Self { inner: Arc::new(Inner { root: root.into(), locks: Mutex::default() }) }
    }

    pub fn root(&self) -> &Path {
        &self.inner.root
    }

    fn settings_path(&self) -> PathBuf {
        self.root().join(SETTINGS_FILE)
    }

    fn history_path(&self) -> PathBuf {
        self.root().join(HISTORY_FILE)
    }

    fn usage_path(&self) -> PathBuf {
        self.root().join(USAGE_FILE)
    }

    fn downloads_dir(&self) -> PathBuf {
        self.root().join(DOWNLOADS_DIR)
    }

    fn manifest_path(&self, id: sonic_core::DownloadId) -> PathBuf {
        self.downloads_dir().join(id.to_string()).join(MANIFEST_FILE)
    }
}
