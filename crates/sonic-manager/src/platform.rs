//! What the manager asks of the operating system: free space, Explorer, the default app, the
//! Recycle Bin, and keeping the computer awake.

use std::io;
use std::path::{Path, PathBuf};

/// Bytes free to this user on the drive holding `dir`. `dir` need not exist yet (a destination
/// folder about to be created): its nearest existing ancestor is asked instead.
pub(crate) fn free_space(dir: &Path) -> Option<u64> {
    let existing = dir.ancestors().find(|p| p.exists())?;
    free_space_at(existing)
}

#[cfg(windows)]
fn free_space_at(dir: &Path) -> Option<u64> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;

    let wide: Vec<u16> = dir.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut available = 0u64;
    // SAFETY: `wide` is a NUL-terminated UTF-16 path alive for the call; `available` is a valid
    // out-parameter; the two totals we don't want may be null, as the API documents.
    let ok = unsafe {
        GetDiskFreeSpaceExW(
            wide.as_ptr(),
            &mut available,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    (ok != 0).then_some(available)
}

#[cfg(not(windows))]
fn free_space_at(_dir: &Path) -> Option<u64> {
    // ponytail: no statvfs without a libc dependency; SonicNet is Windows-first.
    None
}

/// Shows `path` selected in its folder.
pub(crate) fn reveal(path: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        // Quoted by hand: Explorer wants `/select,"C:\a b\c"`, which `arg` can't produce (it
        // would quote the whole argument). A Windows path can't hold a `"`.
        std::process::Command::new("explorer")
            .raw_arg(format!("/select,\"{}\"", path.display()))
            .spawn()
            .map(drop)
    }
    #[cfg(not(windows))]
    {
        open::that_detached(path.parent().unwrap_or(path))
    }
}

/// Opens `path` with the app the OS picks for it.
pub(crate) fn open_file(path: &Path) -> io::Result<()> {
    open::that_detached(path)
}

/// Moves `path` to the Recycle Bin, where the user can still get it back.
pub(crate) fn trash(path: &Path) -> io::Result<()> {
    trash::delete(path).map_err(io::Error::other)
}

/// Runs blocking OS work off the async threads.
pub(crate) async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> Option<T> {
    tokio::task::spawn_blocking(work).await.ok()
}

/// Whether anything is at `path`.
pub(crate) async fn exists(path: PathBuf) -> bool {
    tokio::fs::symlink_metadata(path).await.is_ok()
}

/// Keeps the computer from sleeping while a download runs, which would stop it — as qBittorrent
/// and Transmission offer to. The display can still sleep.
///
/// A power request (`PowerCreateRequest` + `PowerRequestSystemRequired`) belongs to the process,
/// unlike `SetThreadExecutionState`, which belongs to the calling thread — and a Tokio task moves
/// between threads, so a thread's flag would be dropped or stranded at random.
#[derive(Debug, Default)]
pub(crate) struct KeepAwake {
    /// The request's handle, as an integer so the manager's future stays `Send`. 0: none yet.
    handle: isize,
    on: bool,
}

impl KeepAwake {
    pub(crate) fn set(&mut self, on: bool) {
        if on == self.on {
            return;
        }
        self.on = on;
        self.apply(on);
    }

    #[cfg(windows)]
    fn apply(&mut self, on: bool) {
        use windows_sys::Win32::System::Power::{
            PowerClearRequest, PowerCreateRequest, PowerRequestSystemRequired, PowerSetRequest,
        };
        use windows_sys::Win32::System::Threading::{
            POWER_REQUEST_CONTEXT_SIMPLE_STRING, REASON_CONTEXT, REASON_CONTEXT_0,
        };

        if self.handle == 0 {
            let mut reason: Vec<u16> =
                "SonicNet is downloading".encode_utf16().chain(Some(0)).collect();
            let context = REASON_CONTEXT {
                Version: 0, // POWER_REQUEST_CONTEXT_VERSION
                Flags: POWER_REQUEST_CONTEXT_SIMPLE_STRING,
                Reason: REASON_CONTEXT_0 { SimpleReasonString: reason.as_mut_ptr() },
            };
            // SAFETY: `context` and the string it points to live for the call, which copies the
            // reason. A failure returns INVALID_HANDLE_VALUE (-1), checked below.
            let handle = unsafe { PowerCreateRequest(&context) } as isize;
            if handle == 0 || handle == -1 {
                tracing::warn!("could not create a power request; the computer may sleep");
                return;
            }
            self.handle = handle;
        }
        let handle = self.handle as windows_sys::Win32::Foundation::HANDLE;
        // SAFETY: `handle` came from PowerCreateRequest and is closed only on drop.
        let ok = unsafe {
            if on {
                PowerSetRequest(handle, PowerRequestSystemRequired)
            } else {
                PowerClearRequest(handle, PowerRequestSystemRequired)
            }
        };
        if ok == 0 {
            tracing::warn!(on, "could not change the power request");
        }
    }

    #[cfg(not(windows))]
    fn apply(&mut self, _on: bool) {}
}

impl Drop for KeepAwake {
    fn drop(&mut self) {
        #[cfg(windows)]
        if self.handle != 0 {
            if self.on {
                self.apply(false);
            }
            // SAFETY: the handle is ours, from PowerCreateRequest, and not used after this.
            unsafe {
                windows_sys::Win32::Foundation::CloseHandle(
                    self.handle as windows_sys::Win32::Foundation::HANDLE,
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn free_space_of_a_folder_yet_to_be_created_is_its_drives() {
        let dir = std::env::temp_dir().join("sonic-manager-no-such-dir").join("deeper");
        let free = free_space(&dir);
        if cfg!(windows) {
            assert!(free.is_some_and(|bytes| bytes > 0));
        }
    }

    #[test]
    fn keeping_awake_toggles() {
        let mut awake = KeepAwake::default();
        awake.set(true);
        awake.set(true);
        awake.set(false);
    }
}
