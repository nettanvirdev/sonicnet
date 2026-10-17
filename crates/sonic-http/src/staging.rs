//! The staging file: `<final name>.sonic`, beside the destination, that a download's streams
//! write into at their own offsets. Renamed to the final name only once it is complete — in the
//! same directory, so publishing moves no bytes and never needs a second file's worth of space.
//!
//! Creating `<final>.sonic` exclusively is what claims a name: two downloads of `setup.exe` get
//! `setup.exe` and `setup (1).exe`, and a staging file left by a paused download keeps holding
//! its name across restarts. An existing file is never overwritten, not even at publish time.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::BytesMut;
use sonic_core::STAGING_SUFFIX;
use tokio::sync::oneshot;

/// How much one writer holds on its way to disk before telling its stream to wait. 32 streams
/// hold at most 16 MiB; and a writer that fills up is how a download learns the disk is behind —
/// larger, and it hears it later.
pub const WRITE_BUFFER_BYTES: usize = 512 * 1024;

/// The longest file name (one path component) Windows and most Unix file systems take.
const MAX_NAME_BYTES: usize = 255;
/// Longer than any real extension. Past this, what follows the last dot is part of a title
/// ("St. Louis Cardinals vs Chicago Cubs full game replay…") and is shortened with the rest.
const MAX_EXTENSION_BYTES: usize = 32;
/// Somewhere to stop rather than spin forever if every candidate is taken.
const MAX_NAME_ATTEMPTS: u32 = 10_000;

/// A server-provided name made safe as one file name on Windows (and so anywhere): never a path
/// or an NTFS stream (`\ / :`), none of `* ? " < > |` or control characters, no trailing dots or
/// spaces (Windows drops them, so the file would get another name than the one we claimed), not a
/// reserved device name (`CON`, `nul.txt`, `COM1`… get a leading `_`), never empty.
pub fn sanitize_file_name(name: &str) -> String {
    let replaced: String = name
        .chars()
        .map(|c| match c {
            '\\' | '/' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();
    let trimmed = replaced.trim_end_matches([' ', '.']);
    if trimmed.is_empty() {
        return "download".to_owned();
    }
    let device = trimmed.split('.').next().unwrap_or_default().trim_end().to_ascii_uppercase();
    let reserved = matches!(device.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || ["COM", "LPT"].iter().any(|prefix| {
            device.strip_prefix(prefix).is_some_and(|n| {
                matches!(n, "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³")
            })
        });
    if reserved { format!("_{trimmed}") } else { trimmed.to_owned() }
}

/// `(stem, extension)`, the extension with its dot, or empty when there is none worth keeping.
fn split_name(name: &str) -> (&str, &str) {
    match name.rfind('.') {
        Some(dot) if dot > 0 && name.len() - dot <= MAX_EXTENSION_BYTES => name.split_at(dot),
        _ => (name, ""),
    }
}

/// The `n`th candidate for a name: `stem ext`, then `stem (n)ext`, the stem shortened (on a
/// character boundary) so the whole name plus `reserve` more bytes fits [`MAX_NAME_BYTES`].
fn candidate_name(stem: &str, ext: &str, n: u32, reserve: usize) -> String {
    let suffix = if n == 0 { String::new() } else { format!(" ({n})") };
    // Never negative: the extension is at most 32 bytes, the suffix and reserve a dozen.
    let budget = MAX_NAME_BYTES.saturating_sub(reserve + suffix.len() + ext.len());
    let mut end = stem.len().min(budget);
    while !stem.is_char_boundary(end) {
        end -= 1;
    }
    // Shortening can leave a trailing dot or space, which Windows would drop.
    let stem = stem[..end].trim_end_matches([' ', '.']);
    let stem = if stem.is_empty() { "download" } else { stem };
    format!("{stem}{suffix}{ext}")
}

fn staging_path_for(final_path: &Path) -> PathBuf {
    let mut name = final_path.as_os_str().to_owned();
    name.push(STAGING_SUFFIX);
    PathBuf::from(name)
}

/// Whether anything (file, folder, link) is at `path`.
/// Fails unless the staging file is exactly `expected_len` bytes, then flushes it to disk: a
/// published file must be whole and durable before its final name appears.
fn check_and_sync(path: &Path, expected_len: u64) -> io::Result<()> {
    let file = OpenOptions::new().write(true).open(path)?;
    let len = file.metadata()?.len();
    if len != expected_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("the downloaded file is {len} bytes, expected {expected_len}"),
        ));
    }
    file.sync_all()
}

fn exists(path: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

/// Runs blocking file work off the async runtime's threads.
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    tokio::task::spawn_blocking(work).await.map_err(io::Error::other)?
}

/// Claims a name for `file_name` in `dir` (created if missing): the first of `name`,
/// `name (1)`, `name (2)`… for which neither the name nor its staging file exists, claimed by
/// creating the staging file exclusively. The staging file is made sparse where the file system
/// allows. `file_name` is sanitised here; it may come straight from a server.
pub async fn reserve(dir: &Path, file_name: &str) -> io::Result<StagingFile> {
    let (dir, file_name) = (dir.to_owned(), file_name.to_owned());
    blocking(move || reserve_blocking(&dir, &file_name)).await
}

fn reserve_blocking(dir: &Path, file_name: &str) -> io::Result<StagingFile> {
    fs::create_dir_all(dir)?;
    let name = sanitize_file_name(file_name);
    let (stem, ext) = split_name(&name);
    for n in 0..MAX_NAME_ATTEMPTS {
        let final_path = dir.join(candidate_name(stem, ext, n, STAGING_SUFFIX.len()));
        if exists(&final_path)? {
            continue;
        }
        let path = staging_path_for(&final_path);
        let file = match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        };
        // The final name appeared between the check and the claim: someone else has it.
        if exists(&final_path)? {
            drop(file);
            fs::remove_file(&path)?;
            continue;
        }
        let sparse = mark_sparse(&file);
        tracing::debug!(path = %path.display(), sparse, "claimed staging file");
        return Ok(StagingFile { path, final_path });
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!("no unused name for {name} in {}", dir.display()),
    ))
}

/// Makes the file sparse; whether it did. On NTFS a write past the last byte written so far
/// first fills the gap with zeros, on disk, while the write waits. Streams start all over the
/// staging file, so a block's first write could zero megabytes ahead of it and most of the file
/// would be written twice. A sparse file has no gap to fill. Only an optimisation: exFAT and
/// FAT32 refuse, and the file stays as it is.
#[cfg(windows)]
fn mark_sparse(file: &File) -> bool {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::System::IO::DeviceIoControl;
    use windows_sys::Win32::System::Ioctl::FSCTL_SET_SPARSE;

    let mut returned = 0u32;
    // SAFETY: the handle is borrowed from `file`, which outlives the call; no input or output
    // buffer is passed (null pointers with zero sizes, which FSCTL_SET_SPARSE allows, meaning
    // "set"); `returned` is a valid u32 for the call; no OVERLAPPED, so the call is synchronous.
    let ok = unsafe {
        DeviceIoControl(
            file.as_raw_handle(),
            FSCTL_SET_SPARSE,
            std::ptr::null(),
            0,
            std::ptr::null_mut(),
            0,
            &mut returned,
            std::ptr::null_mut(),
        )
    };
    ok != 0
}

#[cfg(not(windows))]
fn mark_sparse(_file: &File) -> bool {
    // Unix file systems leave unwritten ranges as holes anyway.
    true
}

/// Renames `from` to `to` unless something is already at `to` (then `AlreadyExists`). Plain
/// `fs::rename` replaces an existing file on Windows, which would destroy someone's file.
#[cfg(windows)]
fn rename_no_replace(from: &Path, to: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{MOVEFILE_WRITE_THROUGH, MoveFileExW};

    // Verbatim (`\\?\`) paths, as std uses for its own calls: a plain path past MAX_PATH (260)
    // fails here even though std created the staging file there without trouble.
    let to = match (to.parent(), to.file_name()) {
        (Some(dir), Some(name)) => fs::canonicalize(dir)?.join(name),
        _ => to.to_owned(),
    };
    let from = fs::canonicalize(from)?;
    let wide = |p: &Path| p.as_os_str().encode_wide().chain(Some(0)).collect::<Vec<u16>>();
    let (from, to) = (wide(&from), wide(&to));
    // SAFETY: both are NUL-terminated UTF-16 buffers alive for the call. Without
    // MOVEFILE_REPLACE_EXISTING the move fails (ERROR_ALREADY_EXISTS) rather than replace.
    let ok = unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), MOVEFILE_WRITE_THROUGH) };
    if ok != 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}

#[cfg(not(windows))]
fn rename_no_replace(from: &Path, to: &Path) -> io::Result<()> {
    // A hard link fails if `to` exists, atomically; then the staging name is dropped.
    fs::hard_link(from, to)?;
    fs::remove_file(from)
}

/// A download's staging file, `<final>.sonic`. Cheap to clone: it is two paths; every operation
/// opens its own handle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagingFile {
    path: PathBuf,
    final_path: PathBuf,
}

impl StagingFile {
    /// The staging file of a download already reserved, by its final path — how a download
    /// resumed after a restart finds it again. No I/O.
    pub fn attach(final_path: PathBuf) -> Self {
        Self { path: staging_path_for(&final_path), final_path }
    }

    /// `<final>.sonic`.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The name this download claimed. [`publish`](Self::publish) may still pick another if it
    /// was taken meanwhile.
    pub fn final_path(&self) -> &Path {
        &self.final_path
    }

    /// A writer that writes at `offset` onwards, on its own handle. The file must exist.
    pub fn writer(&self, offset: u64) -> io::Result<StagingWriter> {
        let file = OpenOptions::new().write(true).open(&self.path)?;
        Ok(StagingWriter {
            file: Arc::new(file),
            offset,
            pending: BytesMut::new(),
            in_flight: None,
            secured: 0,
            broken: false,
        })
    }

    /// Up to `len` bytes from `offset`; fewer only at the end of the file.
    pub async fn read(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let path = self.path.clone();
        blocking(move || {
            let file = File::open(path)?;
            let mut buf = vec![0; len];
            let mut filled = 0;
            while filled < len {
                match read_at(&file, &mut buf[filled..], offset + filled as u64) {
                    Ok(0) => break,
                    Ok(n) => filled += n,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(error) => return Err(error),
                }
            }
            buf.truncate(filled);
            Ok(buf)
        })
        .await
    }

    /// Flushes what is written to disk, for a checkpoint that will claim those bytes are there.
    pub async fn sync(&self) -> io::Result<()> {
        let path = self.path.clone();
        // Write access: FlushFileBuffers needs it on Windows.
        blocking(move || OpenOptions::new().write(true).open(path)?.sync_all()).await
    }

    pub async fn size(&self) -> io::Result<u64> {
        let path = self.path.clone();
        blocking(move || Ok(fs::metadata(path)?.len())).await
    }

    /// Checks the file is `expected_len` bytes, flushes it, and renames it to the final name —
    /// or, if something took that name meanwhile, to the next free `name (n)`. Never replaces an
    /// existing file. Returns where the file now is. Every writer must be finished first.
    pub async fn publish(&self, expected_len: u64) -> io::Result<PathBuf> {
        self.publish_with(expected_len, |_| async { Ok(()) }).await
    }

    /// [`publish`](Self::publish), calling `before_rename` with each candidate path right before
    /// the rename to it is tried. A crash between the rename and the caller's next save would
    /// otherwise leave the finished file at a `name (n)` nobody recorded, so recovery couldn't
    /// find it: the caller saves the candidate as its publication intent here. An error from
    /// `before_rename` stops the publish with the staging file untouched.
    pub async fn publish_with<F, Fut>(
        &self,
        expected_len: u64,
        mut before_rename: F,
    ) -> io::Result<PathBuf>
    where
        F: FnMut(PathBuf) -> Fut,
        Fut: Future<Output = io::Result<()>>,
    {
        let path = self.path.clone();
        blocking(move || check_and_sync(&path, expected_len)).await?;

        let dir = self.final_path.parent().unwrap_or(Path::new("")).to_path_buf();
        let name = self.final_path.file_name().unwrap_or_default().to_string_lossy().into_owned();
        let (stem, ext) = split_name(&name);
        for n in 0..MAX_NAME_ATTEMPTS {
            let candidate = if n == 0 {
                self.final_path.clone()
            } else {
                dir.join(candidate_name(stem, ext, n, 0))
            };
            // Another download's staging file has claimed that name.
            if n > 0 {
                let claim = staging_path_for(&candidate);
                if blocking(move || exists(&claim)).await? {
                    continue;
                }
            }
            before_rename(candidate.clone()).await?;
            let (from, to) = (self.path.clone(), candidate.clone());
            match blocking(move || rename_no_replace(&from, &to)).await {
                Ok(()) => return Ok(candidate),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("no unused name for {name} in {}", dir.display()),
        ))
    }

    /// Deletes the staging file; fine if it is already gone.
    pub async fn discard(&self) -> io::Result<()> {
        let path = self.path.clone();
        blocking(move || match fs::remove_file(path) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
            _ => Ok(()),
        })
        .await
    }
}

#[cfg(windows)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buf, offset)
}

#[cfg(unix)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buf, offset)
}

#[cfg(windows)]
fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match file.seek_write(buf, offset) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => {
                buf = &buf[n..];
                offset += n as u64;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(unix)]
fn write_all_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, buf, offset)
}

/// A write on its way to disk.
#[derive(Debug)]
struct InFlight {
    len: u64,
    done: oneshot::Receiver<io::Result<()>>,
}

/// Writes consecutive bytes into the staging file from a starting offset, on its own handle,
/// always at explicit offsets (never append, never a shared cursor), so streams writing all over
/// the file can't disturb each other.
///
/// The disk work runs on the blocking pool, one write at a time. While one is on its way the
/// next bytes queue, and go down together in one write: a disk that falls behind gets fewer,
/// larger writes. Holding [`WRITE_BUFFER_BYTES`] or more, [`write`](Self::write) says so and the
/// stream should [`drain`](Self::drain) before reading more from the network.
///
/// [`secured`](Self::secured) counts only bytes the OS has accepted, in order from the start —
/// what a block's progress may claim. After any failed write it stops counting for good: a later
/// write succeeding would otherwise count bytes beyond a hole.
#[derive(Debug)]
pub struct StagingWriter {
    file: Arc<File>,
    /// Where `pending`'s first byte goes.
    offset: u64,
    pending: BytesMut,
    in_flight: Option<InFlight>,
    secured: u64,
    broken: bool,
}

impl StagingWriter {
    /// Bytes written so far, from the starting offset, that the OS has accepted.
    pub fn secured(&self) -> u64 {
        self.secured
    }

    /// Bytes handed to it and not yet secured.
    pub fn buffered(&self) -> usize {
        self.pending.len() + self.in_flight.as_ref().map_or(0, |f| f.len as usize)
    }

    /// Queues `data` after the bytes before it. Never waits. Returns whether there is still room:
    /// `false` means wait on [`drain`](Self::drain) before writing more. A write error from an
    /// earlier write is returned here.
    pub fn write(&mut self, data: &[u8]) -> io::Result<bool> {
        self.check()?;
        self.reap()?;
        self.pending.extend_from_slice(data);
        if self.in_flight.is_none() {
            self.launch();
        }
        Ok(self.buffered() < WRITE_BUFFER_BYTES)
    }

    /// Waits until it holds less than [`WRITE_BUFFER_BYTES`].
    pub async fn drain(&mut self) -> io::Result<()> {
        self.check()?;
        while self.buffered() >= WRITE_BUFFER_BYTES {
            self.wait().await?;
            self.launch();
        }
        Ok(())
    }

    /// Writes out everything it holds and returns [`secured`](Self::secured). A writer dropped
    /// without this still finishes its write in flight, but not the queued bytes.
    pub async fn finish(&mut self) -> io::Result<u64> {
        self.check()?;
        loop {
            self.wait().await?;
            if self.pending.is_empty() {
                return Ok(self.secured);
            }
            self.launch();
        }
    }

    fn check(&self) -> io::Result<()> {
        if self.broken {
            Err(io::Error::other("an earlier write to this file failed"))
        } else {
            Ok(())
        }
    }

    fn settle(&mut self, len: u64, result: io::Result<()>) -> io::Result<()> {
        match result {
            Ok(()) => {
                self.secured += len;
                Ok(())
            }
            Err(error) => {
                self.broken = true;
                self.pending.clear();
                Err(error)
            }
        }
    }

    /// Takes in a finished write, if one has finished, and starts the next.
    fn reap(&mut self) -> io::Result<()> {
        let Some(flight) = &mut self.in_flight else { return Ok(()) };
        let result = match flight.done.try_recv() {
            Err(oneshot::error::TryRecvError::Empty) => return Ok(()),
            Ok(result) => result,
            Err(oneshot::error::TryRecvError::Closed) => Err(io::Error::other("a write was lost")),
        };
        let len = flight.len;
        self.in_flight = None;
        self.settle(len, result)?;
        self.launch();
        Ok(())
    }

    /// Cancel-safe: the write stays in `in_flight` until it has finished, so a `drain` dropped
    /// mid-wait (a cancelled fetch) leaves it for `finish` to wait for and count.
    async fn wait(&mut self) -> io::Result<()> {
        let Some(flight) = &mut self.in_flight else { return Ok(()) };
        let result =
            (&mut flight.done).await.unwrap_or_else(|_| Err(io::Error::other("a write was lost")));
        let len = flight.len;
        self.in_flight = None;
        self.settle(len, result)
    }

    /// Starts writing everything pending, if nothing is in flight.
    fn launch(&mut self) {
        if self.in_flight.is_some() || self.pending.is_empty() {
            return;
        }
        let data = self.pending.split().freeze();
        let offset = self.offset;
        self.offset += data.len() as u64;
        let file = Arc::clone(&self.file);
        let (done, receiver) = oneshot::channel();
        tokio::task::spawn_blocking(move || {
            let _ = done.send(write_all_at(&file, &data, offset));
        });
        self.in_flight = Some(InFlight { len: (self.offset - offset), done: receiver });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitisation_table() {
        let cases = [
            ("report.pdf", "report.pdf"),
            ("a/b\\c.txt", "a_b_c.txt"),
            ("../../etc/passwd", ".._.._etc_passwd"),
            ("C:\\Windows\\evil.dll", "C__Windows_evil.dll"),
            ("file:stream.txt", "file_stream.txt"),
            ("what?<>|*\".txt", "what______.txt"),
            ("tab\tnew\nline\u{7f}.txt", "tab_new_line_.txt"),
            ("trailing. . ", "trailing"),
            ("name.txt...", "name.txt"),
            ("CON", "_CON"),
            ("con.txt", "_con.txt"),
            ("Nul.tar.gz", "_Nul.tar.gz"),
            ("COM1", "_COM1"),
            ("lpt9.log", "_lpt9.log"),
            ("COM¹.txt", "_COM¹.txt"),
            ("CON .txt", "_CON .txt"),
            ("COM0", "COM0"),
            ("COM10.txt", "COM10.txt"),
            ("CONSOLE.txt", "CONSOLE.txt"),
            ("icon.png", "icon.png"),
            ("", "download"),
            ("...", "download"),
            ("   ", "download"),
            ("日本語.zip", "日本語.zip"),
        ];
        for (input, expected) in cases {
            assert_eq!(sanitize_file_name(input), expected, "input: {input:?}");
        }
    }

    #[test]
    fn extensions() {
        assert_eq!(split_name("a.tar.gz"), ("a.tar", ".gz"));
        assert_eq!(split_name(".bashrc"), (".bashrc", ""));
        assert_eq!(split_name("noext"), ("noext", ""));
        let title = "St. Louis Cardinals vs Chicago Cubs full game replay";
        assert_eq!(split_name(title), (title, ""));
    }

    #[test]
    fn long_names_keep_their_extension_within_255_bytes() {
        // 'é' is two bytes: the cut must land on a character boundary.
        let stem = "é".repeat(200);
        for n in [0, 1, 9999] {
            let name = candidate_name(&stem, ".mp4", n, STAGING_SUFFIX.len());
            assert!(name.len() + STAGING_SUFFIX.len() <= MAX_NAME_BYTES, "{n}: {}", name.len());
            assert!(name.ends_with(".mp4"));
            assert!(name.starts_with("éé"));
            if n > 0 {
                assert!(name.ends_with(&format!(" ({n}).mp4")));
            }
        }
        // A 4-byte character at the cut.
        let name = candidate_name(&"😀".repeat(100), "", 0, 0);
        assert!(name.len() <= MAX_NAME_BYTES && name.chars().all(|c| c == '😀'));
        assert_eq!(name.len(), 252);
        // A cut that leaves a trailing dot or space drops it.
        let name = candidate_name(&format!("{}. x", "a".repeat(253)), "", 0, 0);
        assert_eq!(name, "a".repeat(253));
    }
}
