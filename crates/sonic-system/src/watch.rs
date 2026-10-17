//! Watching another process end, however it ends (exit, crash, `taskkill /F`, End task).
//!
//! The helper watches the app that started it: when the app goes, routing must go back to normal
//! at once, even if the app never got to say `stop`.

use std::io;

use tokio::sync::oneshot;
use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, WAIT_OBJECT_0};
use windows_sys::Win32::System::Threading::{
    INFINITE, OpenProcess, PROCESS_SYNCHRONIZE, WaitForSingleObject,
};

struct Handle(HANDLE);

// SAFETY: a process handle is a kernel object reference, usable from any thread.
unsafe impl Send for Handle {}

impl Drop for Handle {
    fn drop(&mut self) {
        // SAFETY: the handle came from OpenProcess and is closed exactly once, here.
        unsafe { CloseHandle(self.0) };
    }
}

/// Resolves when process `pid` has ended. A process that has already ended either can't be
/// opened (an error) or resolves at once; a caller watching its parent stops in both cases.
///
/// The process is opened now, so its pid can't be reused under us while we wait; one thread
/// blocks on it for as long as it runs.
pub fn process_exit(pid: u32) -> io::Result<oneshot::Receiver<()>> {
    // SAFETY: no pointers; SYNCHRONIZE is all waiting needs.
    let handle = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, pid) };
    if handle.is_null() {
        return Err(io::Error::last_os_error());
    }
    let handle = Handle(handle);
    let (tx, rx) = oneshot::channel();
    std::thread::Builder::new().name(format!("watch-{pid}")).spawn(move || {
        // The whole wrapper moves in (not just its raw field), so it is Send and closes on drop.
        let handle = handle;
        // SAFETY: `handle` is open until it drops at the end of this closure.
        let waited = unsafe { WaitForSingleObject(handle.0, INFINITE) };
        if waited != WAIT_OBJECT_0 {
            // A failed wait can't tell us anything; ending is the safe reading of it.
            tracing::warn!(pid, waited, "waiting for the process failed");
        }
        let _ = tx.send(());
    })?;
    Ok(rx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[tokio::test]
    async fn notices_a_killed_process_at_once() {
        let mut child = std::process::Command::new("ping")
            .args(["-n", "60", "127.0.0.1"])
            .stdout(std::process::Stdio::null())
            .spawn()
            .expect("start ping");
        let mut ended = process_exit(child.id()).expect("watch");
        // Alive: nothing yet.
        assert!(tokio::time::timeout(Duration::from_millis(300), &mut ended).await.is_err());
        let killed = Instant::now();
        child.kill().expect("kill");
        tokio::time::timeout(Duration::from_secs(2), ended).await.expect("noticed").expect("sent");
        assert!(killed.elapsed() < Duration::from_secs(2));
        let _ = child.wait();
    }

    #[tokio::test]
    async fn a_process_that_already_ended_is_noticed_at_once() {
        let mut child = std::process::Command::new("cmd").args(["/c", "exit"]).spawn().unwrap();
        let pid = child.id();
        child.wait().unwrap();
        // Our own handle keeps the ended process's object, so it opens and resolves at once.
        let ended = process_exit(pid).expect("still openable while we hold it");
        tokio::time::timeout(Duration::from_secs(1), ended).await.expect("at once").unwrap();
        drop(child);
    }
}
