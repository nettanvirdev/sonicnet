//! Atomic, serialised JSON files.
//!
//! - **Atomic**: a save goes to a temp file that is flushed and renamed over the real one, so a
//!   crash mid-write leaves the old file whole. A half-written file would read back as corrupt
//!   and lose every value in it.
//! - **Per process temp name** (`<file>.<pid>.tmp`): a second running instance can't truncate the
//!   temp file another is renaming.
//! - **Retried rename**: on Windows antivirus, indexers and sync clients briefly hold files open,
//!   which fails a rename over them; a few short retries ride that out.
//! - **Serialised per file**: writes to one path queue behind each other (see `Store::lock`), and
//!   a read-modify-write holds the queue for its whole cycle, so two quick saves can't drop each
//!   other's change.
//! - **Reads never crash**: a missing file is `None` (first run); a corrupt one is logged and
//!   `None`. Only an update refuses to act on a read that failed for another reason (a lock, a
//!   permission), so a save never overwrites what it couldn't read.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::AsyncWriteExt;

use crate::Store;

const RENAME_ATTEMPTS: u32 = 5;

impl Store {
    /// The write queue of one file.
    fn lock(&self, path: &Path) -> Arc<tokio::sync::Mutex<()>> {
        // The map is only touched for an instant and no code panics while holding it; a poisoned
        // lock still holds a usable map.
        let mut locks = self.inner.locks.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        locks.entry(path.to_path_buf()).or_default().clone()
    }

    /// The file's contents, or `None` when it is missing, corrupt or unreadable (logged).
    /// Waits for queued writes first, so a read right after a save sees it.
    pub(crate) async fn read_json<T: DeserializeOwned>(&self, path: &Path) -> Option<T> {
        let lock = self.lock(path);
        let _queued = lock.lock().await;
        read_now(path).await.unwrap_or_else(|error| {
            tracing::warn!(path = %path.display(), %error, "couldn't read");
            None
        })
    }

    pub(crate) async fn write_json<T: Serialize>(&self, path: &Path, value: &T) -> io::Result<()> {
        let bytes = serde_json::to_vec_pretty(value)?;
        let lock = self.lock(path);
        let _queued = lock.lock().await;
        write_atomic(path, &bytes).await
    }

    /// Reads, transforms and writes the file as one step in its queue. `update` gets `None` for a
    /// missing or corrupt file; any other read failure aborts without writing.
    pub(crate) async fn update_json<R, W, F>(&self, path: &Path, update: F) -> io::Result<()>
    where
        R: DeserializeOwned,
        W: Serialize,
        F: FnOnce(Option<R>) -> W,
    {
        let lock = self.lock(path);
        let _queued = lock.lock().await;
        let next = update(read_now(path).await?);
        write_atomic(path, &serde_json::to_vec_pretty(&next)?).await
    }

    /// Waits out any queued write to `path`, then runs `action` while holding its queue: for
    /// deleting a file without a save landing after it.
    pub(crate) async fn with_lock<R>(&self, path: &Path, action: impl AsyncFnOnce() -> R) -> R {
        let lock = self.lock(path);
        let _queued = lock.lock().await;
        action().await
    }
}

/// `Ok(None)` for a missing or corrupt file (corrupt is logged); `Err` for any other failure.
pub(crate) async fn read_now<T: DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    match serde_json::from_slice(&bytes) {
        Ok(value) => Ok(Some(value)),
        Err(error) => {
            tracing::warn!(path = %path.display(), %error, "corrupt file ignored");
            Ok(None)
        }
    }
}

async fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let temporary = temp_path(path);
    let written = async {
        let mut file = tokio::fs::File::create(&temporary).await?;
        file.write_all(bytes).await?;
        // On disk before the rename makes it the file: otherwise a power cut can leave the new
        // name pointing at nothing.
        file.sync_all().await
    }
    .await;
    let result = match written {
        Ok(()) => rename_with_retry(&temporary, path).await,
        Err(error) => Err(error),
    };
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    result
}

pub(crate) fn temp_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(format!(".{}.tmp", std::process::id()));
    PathBuf::from(name)
}

async fn rename_with_retry(from: &Path, to: &Path) -> io::Result<()> {
    let mut attempt = 0;
    loop {
        match tokio::fs::rename(from, to).await {
            Ok(()) => return Ok(()),
            Err(error) if attempt + 1 < RENAME_ATTEMPTS && is_transient(&error) => {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(50 * u64::from(attempt))).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Another process holding the file for a moment: EPERM/EACCES/EBUSY, and on Windows a sharing
/// or lock violation.
fn is_transient(error: &io::Error) -> bool {
    const ERROR_SHARING_VIOLATION: i32 = 32;
    const ERROR_LOCK_VIOLATION: i32 = 33;
    matches!(error.kind(), io::ErrorKind::PermissionDenied | io::ErrorKind::ResourceBusy)
        || (cfg!(windows)
            && matches!(error.raw_os_error(), Some(ERROR_SHARING_VIOLATION | ERROR_LOCK_VIOLATION)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    #[tokio::test]
    async fn round_trip_and_missing() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::at(dir.path());
        let path = dir.path().join("nested/file.json");
        assert_eq!(store.read_json::<Value>(&path).await, None);
        store.write_json(&path, &json!({"a": 1})).await.unwrap();
        assert_eq!(store.read_json::<Value>(&path).await, Some(json!({"a": 1})));
        // No temp file is left behind.
        assert!(!temp_path(&path).exists());
    }

    #[tokio::test]
    async fn corrupt_reads_as_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::at(dir.path());
        let path = dir.path().join("file.json");
        std::fs::write(&path, b"{not json").unwrap();
        assert_eq!(store.read_json::<Value>(&path).await, None);
        // An update over a corrupt file starts from nothing rather than failing forever.
        store.update_json::<Value, _, _>(&path, |old| json!(old.is_none())).await.unwrap();
        assert_eq!(store.read_json::<Value>(&path).await, Some(json!(true)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_updates_lose_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::at(dir.path());
        let path = dir.path().join("counter.json");
        let tasks: Vec<_> = (0..50)
            .map(|_| {
                let store = store.clone();
                let path = path.clone();
                tokio::spawn(async move {
                    store.update_json::<u32, _, _>(&path, |n| n.unwrap_or(0) + 1).await.unwrap();
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(store.read_json::<u32>(&path).await, Some(50));
    }

    #[test]
    fn transient_errors() {
        assert!(is_transient(&io::Error::from(io::ErrorKind::PermissionDenied)));
        assert!(!is_transient(&io::Error::from(io::ErrorKind::NotFound)));
        #[cfg(windows)]
        assert!(is_transient(&io::Error::from_raw_os_error(32)));
    }
}
