//! Saved progress: one manifest per unfinished download, in `downloads/<id>/manifest.json`.
//!
//! A manifest holds the download's state without its blocks and streams, the blocks' progress as
//! compact columns (per network, the bytes it delivered of each block), the staging file, the
//! publication intent, and the request to resume it with (its cookies sealed apart, see
//! [`crate::seal`]). Blocks aren't saved: their ranges follow from the plan, so only the bytes are
//! durable, and restoring rebuilds them with [`sonic_core::plan_blocks`].
//!
//! The file is read back from disk, so it is checked like any input. Progress that doesn't fit the
//! plan is refused (the download starts its blocks over rather than trusting bytes it can't account
//! for), and the staging file is checked against what the manifest claims.

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sonic_core::{
    BlockState, BlockStatus, DownloadId, DownloadState, DownloadStatus, Millis, NetworkId,
    STAGING_SUFFIX, StartRequest, now_millis, plan_blocks,
};

use crate::seal::{seal_cookies, unseal_cookies};
use crate::{MANIFEST_FILE, Store};

/// Raised whenever the layout changes; a manifest of another version is discarded, not guessed at.
pub const MANIFEST_VERSION: u32 = 1;

/// More blocks than any sane plan has (1 MiB blocks make this a 4 TiB file). A damaged manifest
/// claiming a huge file with tiny blocks is refused before anything is allocated for it.
const MAX_BLOCKS: u64 = 1 << 22;

/// Which file a path names, independent of the name: on Windows the volume serial number and file
/// index, elsewhere the device and inode. A rename keeps it, which is how a publication that
/// happened before a crash is recognised afterwards.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileIdentity {
    pub volume: u64,
    pub index: u64,
}

/// The identity of the file at `path` (following symlinks).
#[cfg(windows)]
pub fn file_identity(path: &Path) -> io::Result<FileIdentity> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };

    let file = fs::File::open(path)?;
    let mut info = BY_HANDLE_FILE_INFORMATION::default();
    // SAFETY: the handle is open for the duration of the call (`file` outlives it) and `info` is
    // a valid out-parameter of the right type.
    if unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(FileIdentity {
        volume: u64::from(info.dwVolumeSerialNumber),
        index: (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
    })
}

/// The identity of the file at `path` (following symlinks).
#[cfg(unix)]
pub fn file_identity(path: &Path) -> io::Result<FileIdentity> {
    use std::os::unix::fs::MetadataExt;
    let metadata = fs::metadata(path)?;
    Ok(FileIdentity { volume: metadata.dev(), index: metadata.ino() })
}

/// Where a download is about to be published: saved (by the manager) right before the staging
/// file is renamed to `path`, with the staging file's identity. If the app dies between the rename
/// and the save that follows it, the restore finds `path` holding that very file and knows the
/// download completed, instead of failing it for a missing staging file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Publication {
    pub path: PathBuf,
    pub identity: FileIdentity,
}

/// A saved download, read back and checked.
#[derive(Clone, Debug, PartialEq)]
pub struct RestoredDownload {
    /// Blocks rebuilt from the plan; no streams, no speeds. A download that was downloading or
    /// queued comes back paused (queued again if enrolled in the schedule). `Completed` when its
    /// publication turned out to have happened: the manager moves it to history.
    pub state: DownloadState,
    /// Its browser cookies are back if they could be unsealed, otherwise empty.
    pub request: StartRequest,
    pub staging_path: PathBuf,
    pub publication: Option<Publication>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    version: u32,
    saved_at: Millis,
    /// `blocks` and `streams` empty.
    state: DownloadState,
    /// Per network, the bytes it delivered of each block, by block index.
    progress: BTreeMap<NetworkId, Vec<u64>>,
    /// Every block is complete. Needed where a block's length is unknown (a file of unknown size
    /// is one open-ended block), which its bytes can't tell.
    complete: bool,
    staging_path: PathBuf,
    publication: Option<Publication>,
    /// Its browser's cookies emptied; they are in `sealed_cookies`.
    request: StartRequest,
    sealed_cookies: Option<String>,
}

impl Store {
    /// Saves a download's progress (a checkpoint, a pause, or a publication intent). `staging_path`
    /// must name a `.sonic` file: nothing else is ever deleted on the download's behalf.
    pub async fn save_download(
        &self,
        state: &DownloadState,
        request: &StartRequest,
        staging_path: &Path,
        publication: Option<&Publication>,
    ) -> io::Result<()> {
        if !is_staging(staging_path) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{} is not a staging file", staging_path.display()),
            ));
        }
        let (progress, complete) = progress_of(&state.blocks);
        let mut request = request.clone();
        let sealed_cookies = request.browser.as_mut().and_then(|browser| {
            let cookies = std::mem::take(&mut browser.cookies);
            // A finished download drops its cookies: nothing will ask the server for it again.
            (state.status != DownloadStatus::Completed).then(|| seal_cookies(&cookies)).flatten()
        });
        let manifest = Manifest {
            version: MANIFEST_VERSION,
            saved_at: now_millis(),
            // ponytail: clones the blocks only to drop them; a borrowed mirror of DownloadState
            // if checkpoints of huge plans show up in profiles.
            state: DownloadState { blocks: Vec::new(), streams: Vec::new(), ..state.clone() },
            progress,
            complete,
            staging_path: staging_path.to_path_buf(),
            publication: publication.cloned(),
            request,
            sealed_cookies,
        };
        self.write_json(&self.manifest_path(state.id), &manifest).await
    }

    /// Deletes a download's saved state (not its staging file: the manager owns that). Waits for a
    /// save under way, so none lands after it. A save called *after* this recreates it.
    pub async fn remove_download(&self, id: DownloadId) -> io::Result<()> {
        let manifest = self.manifest_path(id);
        let Some(dir) = manifest.parent() else { return Ok(()) };
        self.with_lock(&manifest, async || match tokio::fs::remove_dir_all(dir).await {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
            _ => Ok(()),
        })
        .await
    }

    /// Every saved download that can be restored, oldest first. Call once at startup, before any
    /// download is saved.
    ///
    /// Ones that never can be (no manifest, a corrupt or other-version one, progress or a staging
    /// path that doesn't add up) are discarded: their folder here, and their staging file when it
    /// is unmistakably ours (absolute, `.sonic`, a file, and no restored download uses it). One that
    /// can't be read for another reason (a permission, a drive not mounted) is left for a later
    /// launch. Of downloads sharing one staging file the newest is kept, since two writing one file
    /// would scramble it; the others' manifests go.
    pub async fn list_saved(&self) -> Vec<RestoredDownload> {
        let downloads = self.downloads_dir();
        tokio::task::spawn_blocking(move || scan(&downloads)).await.unwrap_or_default()
    }
}

/// The progress columns and completeness of `blocks`.
fn progress_of(blocks: &[BlockState]) -> (BTreeMap<NetworkId, Vec<u64>>, bool) {
    let mut progress: BTreeMap<NetworkId, Vec<u64>> = BTreeMap::new();
    for (position, block) in blocks.iter().enumerate() {
        for (network, &bytes) in &block.bytes_by_network {
            progress.entry(network.clone()).or_insert_with(|| vec![0; blocks.len()])[position] =
                bytes;
        }
    }
    let complete = !blocks.is_empty() && blocks.iter().all(|b| b.status == BlockStatus::Completed);
    (progress, complete)
}

/// The blocks of a download planned as `total_bytes` in `block_size` blocks, with the saved
/// progress applied. `None` when the plan itself is unusable (a zero block size for a sized file,
/// or absurdly many blocks): the download can't be restored. Progress that doesn't fit the plan
/// (a column of the wrong length, a block over its length) gives fresh blocks: started over rather
/// than trusted.
pub fn restore_blocks(
    total_bytes: Option<u64>,
    block_size: u64,
    progress: &BTreeMap<NetworkId, Vec<u64>>,
    complete: bool,
) -> Option<Vec<BlockState>> {
    let count = match total_bytes {
        None | Some(0) => 1,
        Some(_) if block_size == 0 => return None,
        Some(total) => total.div_ceil(block_size),
    };
    if count > MAX_BLOCKS {
        return None;
    }
    let mut blocks = plan_blocks(total_bytes, block_size);
    let fits = apply_progress(&mut blocks, progress);
    if !fits {
        blocks = plan_blocks(total_bytes, block_size);
    }
    for block in &mut blocks {
        let done = match block.range.len() {
            Some(len) => block.bytes_downloaded == len,
            None => complete && fits,
        };
        if done {
            block.status = BlockStatus::Completed;
        }
    }
    Some(blocks)
}

/// Adds the columns' bytes to fresh `blocks`; false when they don't fit (a column of the wrong
/// length, a sum that overflows, a block over its length), leaving `blocks` half-filled.
fn apply_progress(blocks: &mut [BlockState], progress: &BTreeMap<NetworkId, Vec<u64>>) -> bool {
    if progress.values().any(|column| column.len() != blocks.len()) {
        return false;
    }
    for (network, column) in progress {
        for (block, &bytes) in blocks.iter_mut().zip(column) {
            if bytes == 0 {
                continue;
            }
            let Some(sum) = block.bytes_downloaded.checked_add(bytes) else { return false };
            block.bytes_downloaded = sum;
            block.bytes_by_network.insert(network.clone(), bytes);
        }
    }
    blocks.iter().all(|block| block.range.len().is_none_or(|len| block.bytes_downloaded <= len))
}

/// A path the store may delete on a download's behalf: absolute and named `<something>.sonic`.
fn is_staging(path: &Path) -> bool {
    path.is_absolute()
        && path
            .file_name()
            .and_then(OsStr::to_str)
            .is_some_and(|name| name.len() > STAGING_SUFFIX.len() && name.ends_with(STAGING_SUFFIX))
}

/// The staging file a raw (possibly unreadable) manifest names, if it is unmistakably ours.
fn leftover_staging(raw: &Value) -> Option<PathBuf> {
    let path = PathBuf::from(raw.get("staging_path")?.as_str()?);
    is_staging(&path).then_some(path)
}

fn scan(downloads: &Path) -> Vec<RestoredDownload> {
    let Ok(entries) = fs::read_dir(downloads) else { return Vec::new() };
    let mut restored = Vec::new();
    // Folders to delete, each with the staging file it may take along.
    let mut discards: Vec<(PathBuf, Option<PathBuf>)> = Vec::new();

    for entry in entries.flatten() {
        if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        let dir = entry.path();
        remove_stale_temps(&dir);
        let bytes = match fs::read(dir.join(MANIFEST_FILE)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                discards.push((dir, None));
                continue;
            }
            Err(error) => {
                tracing::warn!(dir = %dir.display(), %error, "saved download left for later");
                continue;
            }
        };
        let Ok(raw) = serde_json::from_slice::<Value>(&bytes) else {
            tracing::warn!(dir = %dir.display(), "corrupt manifest discarded");
            discards.push((dir, None));
            continue;
        };
        let staging = leftover_staging(&raw);
        let manifest = serde_json::from_value::<Manifest>(raw);
        match manifest.ok().and_then(|manifest| restore(manifest, &entry.file_name())) {
            Some(download) => restored.push(download),
            None => {
                tracing::warn!(dir = %dir.display(), "unrestorable download discarded");
                discards.push((dir, staging));
            }
        }
    }

    restored.sort_by_key(|download| Reverse(download.state.started_at));
    let mut claimed = HashSet::new();
    let mut kept = Vec::new();
    for download in restored {
        if claimed.insert(download.staging_path.clone()) {
            kept.push(download);
        } else {
            discards.push((downloads.join(download.state.id.to_string()), None));
        }
    }
    for (dir, staging) in discards {
        if let Some(staging) = staging.filter(|path| !claimed.contains(path)) {
            remove_staging(&staging);
        }
        if let Err(error) = fs::remove_dir_all(&dir) {
            tracing::warn!(dir = %dir.display(), %error, "couldn't discard");
        }
    }
    kept.reverse();
    kept
}

/// Temp files of saves cut short (see `json`), which no save will ever finish.
fn remove_stale_temps(dir: &Path) {
    let Ok(entries) = fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(MANIFEST_FILE) && name.ends_with(".tmp") {
            let _ = fs::remove_file(entry.path());
        }
    }
}

/// Deletes a staging file, only if it is one (see [`is_staging`]) and a plain file.
fn remove_staging(path: &Path) {
    if is_staging(path)
        && fs::symlink_metadata(path).is_ok_and(|m| m.is_file())
        && let Err(error) = fs::remove_file(path)
    {
        tracing::warn!(path = %path.display(), %error, "couldn't delete staging file");
    }
}

/// A manifest read back, checked and turned into a download to show paused. `None` when it never
/// can be restored.
fn restore(manifest: Manifest, dir_name: &OsStr) -> Option<RestoredDownload> {
    let Manifest {
        version,
        saved_at,
        mut state,
        progress,
        complete,
        staging_path,
        publication,
        mut request,
        sealed_cookies,
    } = manifest;
    if version != MANIFEST_VERSION
        || OsStr::new(&state.id.to_string()) != dir_name
        || !is_staging(&staging_path)
        || state.status == DownloadStatus::Cancelled
    {
        return None;
    }
    state.blocks = restore_blocks(state.total_bytes, state.block_size, &progress, complete)?;
    state.streams.clear();

    let queued_at = state.queued_at;
    if matches!(state.status, DownloadStatus::Downloading | DownloadStatus::Queued) {
        // Paused first, so the staging file is checked before any work is queued for it.
        state.status = DownloadStatus::Paused;
        state.paused_at = Some(saved_at);
        state.queued_at = None;
    }
    state.speed_bps = 0.0;
    state.time_left_secs = None;
    state.disk_limited = false;
    for network in &mut state.networks {
        network.speed_bps = 0.0;
    }

    if state.status == DownloadStatus::Paused {
        check_files(&mut state, &staging_path, publication.as_ref(), saved_at);
    }
    if state.status == DownloadStatus::Paused && state.scheduled {
        state.status = DownloadStatus::Queued;
        state.queued_at = queued_at.or(Some(state.started_at));
    }
    // A resumed download's chart starts when it does; a finished one keeps its chart.
    if state.status != DownloadStatus::Completed {
        state.speed_history.clear();
    }
    recompute_totals(&mut state);

    if let Some(browser) = &mut request.browser {
        browser.cookies = sealed_cookies.as_deref().and_then(unseal_cookies).unwrap_or_default();
    }
    Some(RestoredDownload { state, request, staging_path, publication })
}

/// Squares a paused download with what is on disk:
/// - its publication happened (every block done, and the published file is the staging file,
///   renamed, of the expected size): completed;
/// - its staging file is gone: what it had is lost; an error, not resumable;
/// - otherwise blocks claiming bytes past the staging file's end start over.
fn check_files(
    state: &mut DownloadState,
    staging: &Path,
    publication: Option<&Publication>,
    saved_at: Millis,
) {
    let staging_size = fs::metadata(staging).ok().filter(|m| m.is_file()).map(|m| m.len());
    let published_path =
        publication.map_or_else(|| state.destination_path.clone(), |p| p.path.clone());
    let published_size =
        fs::metadata(&published_path).ok().filter(|m| m.is_file()).map(|m| m.len());
    let downloaded: u64 = state.blocks.iter().map(|b| b.bytes_downloaded).sum();
    let expected = state.total_bytes.unwrap_or(downloaded);
    let all_done = state.blocks.iter().all(|b| b.status == BlockStatus::Completed);

    // A manifest naming the staging file as its own publication (only a damaged or hand-edited
    // one can) must never count as published: that would delete the only copy below.
    let same_file = published_size.is_some()
        && published_path != staging
        && match staging_size {
            // Both names exist: published only if they are one file.
            Some(_) => {
                let staging_id = file_identity(staging).ok();
                staging_id.is_some() && staging_id == file_identity(&published_path).ok()
            }
            // The staging name is gone: published if the file there is the one it was.
            None => publication.is_some_and(|p| file_identity(&p.path).ok() == Some(p.identity)),
        };

    if all_done && published_size == Some(expected) && same_file {
        state.status = DownloadStatus::Completed;
        state.file_name = published_path
            .file_name()
            .map_or_else(|| state.file_name.clone(), |n| n.to_string_lossy().into_owned());
        state.destination_path = published_path;
        state.error = None;
        state.completed_at = state.completed_at.or(Some(saved_at));
        state.paused_at = None;
        if staging_size.is_some() {
            // A second name for the published file.
            remove_staging(staging);
        }
    } else if let Some(size) = staging_size {
        reset_blocks(&mut state.blocks, |block| block.range.start + block.bytes_downloaded > size);
    } else if downloaded > 0 {
        // (Nothing downloaded yet means nothing lost: it just starts when resumed.)
        state.status = DownloadStatus::Error;
        state.error = Some(
            "The partial download file is missing. Remove this download and start again.".into(),
        );
        state.resumable = false;
        reset_blocks(&mut state.blocks, |_| true);
    }
}

fn reset_blocks(blocks: &mut [BlockState], reset: impl Fn(&BlockState) -> bool) {
    for block in blocks {
        if reset(block) {
            *block = BlockState::new(block.index, block.range);
        }
    }
}

/// The download's and each network's byte counts, from the blocks.
fn recompute_totals(state: &mut DownloadState) {
    let mut by_network: BTreeMap<&NetworkId, u64> = BTreeMap::new();
    let mut total = 0;
    for block in &state.blocks {
        total += block.bytes_downloaded;
        for (network, bytes) in &block.bytes_by_network {
            *by_network.entry(network).or_default() += bytes;
        }
    }
    let by_network: BTreeMap<NetworkId, u64> =
        by_network.into_iter().map(|(id, bytes)| (id.clone(), bytes)).collect();
    state.bytes_downloaded = total;
    for network in &mut state.networks {
        network.bytes_downloaded = by_network.get(&network.id).copied().unwrap_or(0);
    }
}

#[cfg(test)]
mod tests;
