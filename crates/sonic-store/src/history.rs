//! Finished downloads, newest first. A download moves here once complete, leaving its blocks
//! behind, so a long history costs a launch nothing.

use std::io;

use serde_json::Value;
use sonic_core::{DownloadId, FinishedDownload};

use crate::Store;

/// The most entries kept; adding past it drops the oldest.
// ponytail: a fixed cap; a setting if anyone wants more kept.
pub const HISTORY_CAP: usize = 500;

/// The entries of a history file. It may be hand-edited or from another version: an entry that
/// doesn't parse is dropped on its own rather than losing the whole list.
fn entries_of(raw: Option<Value>) -> Vec<FinishedDownload> {
    match raw {
        Some(Value::Array(items)) => {
            items.into_iter().filter_map(|item| serde_json::from_value(item).ok()).collect()
        }
        _ => Vec::new(),
    }
}

impl Store {
    /// Every finished download, newest first, each marked `missing` when its file is gone.
    pub async fn history(&self) -> Vec<FinishedDownload> {
        let mut entries = entries_of(self.read_json(&self.history_path()).await);
        // One blocking task for all the checks rather than one per entry.
        let paths: Vec<_> = entries.iter().map(|entry| entry.destination_path.clone()).collect();
        let missing = tokio::task::spawn_blocking(move || {
            paths.iter().map(|path| std::fs::metadata(path).is_err()).collect::<Vec<_>>()
        })
        .await
        .unwrap_or_default();
        for (entry, missing) in entries.iter_mut().zip(missing) {
            entry.missing = missing;
        }
        entries
    }

    /// Puts `entry` at the top (replacing an older entry with its id), dropping the oldest past
    /// [`HISTORY_CAP`].
    pub async fn add_to_history(&self, entry: &FinishedDownload) -> io::Result<()> {
        let entry = entry.clone();
        self.update_json(&self.history_path(), move |raw| {
            let mut entries = entries_of(raw);
            entries.retain(|other| other.id != entry.id);
            entries.insert(0, entry);
            entries.truncate(HISTORY_CAP);
            entries
        })
        .await
    }

    /// Forgets the entries with these ids. Their files stay: they are the user's.
    pub async fn remove_from_history(&self, ids: &[DownloadId]) -> io::Result<()> {
        self.update_json(&self.history_path(), |raw| {
            let mut entries = entries_of(raw);
            entries.retain(|entry| !ids.contains(&entry.id));
            entries
        })
        .await
    }

    /// Forgets every entry. Their files stay.
    pub async fn clear_history(&self) -> io::Result<()> {
        self.write_json(&self.history_path(), &Vec::<FinishedDownload>::new()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn entry(path: PathBuf) -> FinishedDownload {
        FinishedDownload {
            id: DownloadId::new(),
            url: "https://example.com/a.bin".into(),
            file_name: "a.bin".into(),
            destination_path: path,
            total_bytes: 10,
            started_at: 1,
            completed_at: 2,
            total_paused_ms: 0,
            networks: Vec::new(),
            speed_history: Default::default(),
            peak_speed_bps: None,
            peak_streams: 8,
            blocks_written: 1,
            from_browser: None,
            missing: false,
        }
    }

    #[tokio::test]
    async fn newest_first_capped_and_missing_marked() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::at(dir.path());
        let present = dir.path().join("present.bin");
        std::fs::write(&present, b"x").unwrap();

        let mut ids = Vec::new();
        for _ in 0..HISTORY_CAP + 3 {
            let entry = entry(dir.path().join("gone.bin"));
            ids.push(entry.id);
            store.add_to_history(&entry).await.unwrap();
        }
        let newest = entry(present.clone());
        store.add_to_history(&newest).await.unwrap();

        let history = store.history().await;
        assert_eq!(history.len(), HISTORY_CAP);
        assert_eq!(history[0].id, newest.id);
        assert!(!history[0].missing);
        assert!(history[1].missing);
        assert_eq!(history[1].id, *ids.last().unwrap());
        // The oldest four went.
        assert!(history.iter().all(|e| !ids[..4].contains(&e.id)));
        // `missing` is never written.
        let raw = std::fs::read_to_string(dir.path().join("history.json")).unwrap();
        assert!(!raw.contains("missing"));
    }

    #[tokio::test]
    async fn re_adding_moves_to_top_and_removal() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::at(dir.path());
        let (a, b, c) = (entry("a".into()), entry("b".into()), entry("c".into()));
        for e in [&a, &b, &c, &a] {
            store.add_to_history(e).await.unwrap();
        }
        let order: Vec<_> = store.history().await.iter().map(|e| e.id).collect();
        assert_eq!(order, [a.id, c.id, b.id]);

        store.remove_from_history(&[c.id]).await.unwrap();
        let order: Vec<_> = store.history().await.iter().map(|e| e.id).collect();
        assert_eq!(order, [a.id, b.id]);

        store.clear_history().await.unwrap();
        assert!(store.history().await.is_empty());
    }

    #[tokio::test]
    async fn bad_entries_dropped_one_by_one() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::at(dir.path());
        let good = entry("good".into());
        let raw = serde_json::json!([{"id": "nope"}, good, 42]);
        std::fs::write(dir.path().join("history.json"), raw.to_string()).unwrap();
        let history = store.history().await;
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].id, good.id);

        std::fs::write(dir.path().join("history.json"), "{corrupt").unwrap();
        assert!(store.history().await.is_empty());
        store.add_to_history(&good).await.unwrap();
        assert_eq!(store.history().await.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_adds_lose_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::at(dir.path());
        let tasks: Vec<_> = (0..40)
            .map(|_| {
                let store = store.clone();
                tokio::spawn(async move { store.add_to_history(&entry("x".into())).await.unwrap() })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(store.history().await.len(), 40);
    }
}
