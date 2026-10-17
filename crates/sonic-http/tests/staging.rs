//! The staging file on a real file system: claiming names, positional writes, publishing.

use std::fs;
use std::io;
use std::path::Path;

use sonic_http::{StagingFile, WRITE_BUFFER_BYTES, reserve};

fn names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> =
        fs::read_dir(dir).unwrap().map(|e| e.unwrap().file_name().into_string().unwrap()).collect();
    names.sort();
    names
}

#[tokio::test]
async fn names_are_claimed_once() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("setup.exe"), b"someone's file").unwrap();
    // Another download's staging file holds "setup (1).exe".
    fs::write(dir.path().join("setup (1).exe.sonic"), b"").unwrap();

    let a = reserve(dir.path(), "setup.exe").await.unwrap();
    let b = reserve(dir.path(), "setup.exe").await.unwrap();
    assert_eq!(a.final_path(), dir.path().join("setup (2).exe"));
    assert_eq!(a.path(), dir.path().join("setup (2).exe.sonic"));
    assert_eq!(b.final_path(), dir.path().join("setup (3).exe"));
    assert_eq!(fs::read(dir.path().join("setup.exe")).unwrap(), b"someone's file");
}

#[tokio::test]
async fn names_are_sanitised_and_the_directory_made() {
    let dir = tempfile::tempdir().unwrap();
    let nested = dir.path().join("new").join("folder");
    let file = reserve(&nested, "..\\evil/con.txt. ").await.unwrap();
    assert_eq!(file.final_path(), nested.join(".._evil_con.txt"));
    let file = reserve(&nested, "CON.txt").await.unwrap();
    assert_eq!(file.final_path(), nested.join("_CON.txt"));
    let file = reserve(&nested, "").await.unwrap();
    assert_eq!(file.final_path(), nested.join("download"));
}

#[tokio::test]
async fn long_names_fit_the_file_system() {
    let dir = tempfile::tempdir().unwrap();
    let long = format!("{}.mkv", "日本語".repeat(40)); // 360 bytes of stem
    let a = reserve(dir.path(), &long).await.unwrap();
    let b = reserve(dir.path(), &long).await.unwrap();
    for file in [&a, &b] {
        let name = file.path().file_name().unwrap().to_str().unwrap().to_owned();
        assert!(name.len() <= 255, "{}", name.len());
        assert!(name.ends_with(".mkv.sonic"));
    }
    let b_name = b.final_path().file_name().unwrap().to_str().unwrap().to_owned();
    assert!(b_name.ends_with(" (1).mkv"), "{b_name}");
}

#[tokio::test]
async fn writers_write_at_their_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let file = reserve(dir.path(), "f.bin").await.unwrap();
    let mut high = file.writer(1000).unwrap();
    let mut low = file.writer(0).unwrap();
    for chunk in [&b"hello "[..], b"world"] {
        assert!(high.write(chunk).unwrap());
    }
    assert!(low.write(b"start").unwrap());
    assert_eq!(high.finish().await.unwrap(), 11);
    assert_eq!(low.finish().await.unwrap(), 5);
    assert_eq!(file.size().await.unwrap(), 1011);
    assert_eq!(file.read(1000, 100).await.unwrap(), b"hello world");
    assert_eq!(file.read(0, 5).await.unwrap(), b"start");
    assert_eq!(file.read(5, 10).await.unwrap(), vec![0; 10]);
    file.sync().await.unwrap();
}

#[tokio::test]
async fn a_full_writer_asks_to_wait() {
    let dir = tempfile::tempdir().unwrap();
    let file = reserve(dir.path(), "f.bin").await.unwrap();
    let mut writer = file.writer(0).unwrap();
    let data = vec![7u8; WRITE_BUFFER_BYTES];
    assert!(!writer.write(&data).unwrap(), "a full buffer must say so");
    writer.drain().await.unwrap();
    assert!(writer.buffered() < WRITE_BUFFER_BYTES);
    assert!(writer.secured() > 0);
    writer.write(&data).unwrap();
    assert_eq!(writer.finish().await.unwrap(), 2 * WRITE_BUFFER_BYTES as u64);
    assert_eq!(
        file.read(0, 2 * WRITE_BUFFER_BYTES).await.unwrap(),
        vec![7u8; 2 * WRITE_BUFFER_BYTES]
    );
}

#[tokio::test]
async fn a_cancelled_drain_keeps_its_write_in_flight() {
    // `fetch` drops `drain()` when its download is cancelled, then calls `finish()`. The write
    // that was on its way must still be waited for and counted: forgetting it would let later
    // bytes be counted in its place, claiming a range whose write may yet fail.
    let dir = tempfile::tempdir().unwrap();
    let file = reserve(dir.path(), "f.bin").await.unwrap();
    let mut writer = file.writer(0).unwrap();
    // Large enough that the write is still on its way when `drain` is first polled.
    let data = vec![3u8; 64 * WRITE_BUFFER_BYTES];
    assert!(!writer.write(&data).unwrap());
    let cancelled = tokio::time::timeout(std::time::Duration::ZERO, writer.drain()).await;
    assert!(cancelled.is_err(), "the write finished before drain was polled; use more data");
    assert_eq!(writer.finish().await.unwrap(), data.len() as u64);
}

#[tokio::test]
async fn publish_checks_the_size_and_renames() {
    let dir = tempfile::tempdir().unwrap();
    let file = reserve(dir.path(), "movie.mp4").await.unwrap();
    let mut writer = file.writer(0).unwrap();
    writer.write(b"0123456789").unwrap();
    writer.finish().await.unwrap();
    drop(writer);

    let error = file.publish(11).await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(file.path().exists());

    let published = file.publish(10).await.unwrap();
    assert_eq!(published, dir.path().join("movie.mp4"));
    assert_eq!(fs::read(&published).unwrap(), b"0123456789");
    assert_eq!(names(dir.path()), ["movie.mp4"]);
}

#[tokio::test]
async fn publish_never_overwrites() {
    let dir = tempfile::tempdir().unwrap();
    let file = reserve(dir.path(), "doc.pdf").await.unwrap();
    // Something else creates the final name — and the next one's staging file — meanwhile.
    fs::write(dir.path().join("doc.pdf"), b"theirs").unwrap();
    fs::write(dir.path().join("doc (1).pdf.sonic"), b"").unwrap();
    let published = file.publish(0).await.unwrap();
    assert_eq!(published, dir.path().join("doc (2).pdf"));
    assert_eq!(fs::read(dir.path().join("doc.pdf")).unwrap(), b"theirs");
    assert_eq!(names(dir.path()), ["doc (1).pdf.sonic", "doc (2).pdf", "doc.pdf"]);
}

#[tokio::test]
async fn publish_with_announces_each_candidate_before_renaming() {
    // Crash recovery looks for the finished file where the caller last recorded it, so the caller
    // must hear of the exact name before the rename — `name (n)` included.
    let dir = tempfile::tempdir().unwrap();
    let file = reserve(dir.path(), "doc.pdf").await.unwrap();
    fs::write(dir.path().join("doc.pdf"), b"theirs").unwrap();
    let mut announced = Vec::new();
    let published = file
        .publish_with(0, |candidate| {
            // The file isn't there yet when the caller hears of it.
            assert!(!candidate.exists() || candidate == dir.path().join("doc.pdf"));
            announced.push(candidate);
            async { Ok(()) }
        })
        .await
        .unwrap();
    assert_eq!(announced, [dir.path().join("doc.pdf"), dir.path().join("doc (1).pdf")]);
    assert_eq!(published, dir.path().join("doc (1).pdf"));

    // A failing callback stops the publish and leaves the staging file alone.
    let other = reserve(dir.path(), "next.bin").await.unwrap();
    let error = other
        .publish_with(0, |_| async { Err(io::Error::other("could not save the intent")) })
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "could not save the intent");
    assert!(other.path().exists() && !other.final_path().exists());
}

#[tokio::test]
async fn publish_works_past_max_path() {
    // reserve() goes through std, which handles paths past Windows' 260-character MAX_PATH;
    // publishing must too, or a finished download could never be renamed into place.
    let dir = tempfile::tempdir().unwrap();
    let deep = dir.path().join("d".repeat(120)).join("e".repeat(120));
    let file = reserve(&deep, &format!("{}.bin", "f".repeat(200))).await.unwrap();
    assert!(file.path().as_os_str().len() > 300);
    let published = file.publish(0).await.unwrap();
    assert_eq!(published, file.final_path());
    assert!(published.exists() && !file.path().exists());
}

#[tokio::test]
async fn attach_and_discard() {
    let dir = tempfile::tempdir().unwrap();
    let file = reserve(dir.path(), "a.zip").await.unwrap();
    let again = StagingFile::attach(file.final_path().to_owned());
    assert_eq!(again, file);
    again.discard().await.unwrap();
    assert!(!file.path().exists());
    file.discard().await.unwrap(); // already gone is fine
    assert!(names(dir.path()).is_empty());
}

#[cfg(windows)]
#[tokio::test]
async fn staging_files_are_sparse_on_windows() {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{FILE_ATTRIBUTE_SPARSE_FILE, GetFileAttributesW};

    let dir = tempfile::tempdir().unwrap();
    let file = reserve(dir.path(), "sparse.bin").await.unwrap();
    let wide: Vec<u16> = file.path().as_os_str().encode_wide().chain(Some(0)).collect();
    // SAFETY: `wide` is a NUL-terminated UTF-16 path alive for the call.
    let attributes = unsafe { GetFileAttributesW(wide.as_ptr()) };
    assert_ne!(attributes, u32::MAX, "GetFileAttributesW failed");
    // Temp directories are on NTFS on any Windows we test on.
    assert_ne!(attributes & FILE_ATTRIBUTE_SPARSE_FILE, 0, "staging file is not sparse");
}
