//! The data-usage snapshot. Its shape belongs to the engine, so the store only moves it to and
//! from disk.

use std::io;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Store;

impl Store {
    /// The saved snapshot; `None` when there is none, or it doesn't parse as `T` (logged).
    pub async fn load_usage<T: DeserializeOwned>(&self) -> Option<T> {
        self.read_json(&self.usage_path()).await
    }

    pub async fn save_usage<T: Serialize>(&self, usage: &T) -> io::Result<()> {
        self.write_json(&self.usage_path(), usage).await
    }

    /// The app's per-day traffic totals (System mode and downloads per network); its shape
    /// belongs to the app. `None` when there is none or it doesn't parse as `T`.
    pub async fn load_traffic<T: DeserializeOwned>(&self) -> Option<T> {
        self.read_json(&self.root().join("traffic.json")).await
    }

    pub async fn save_traffic<T: Serialize>(&self, traffic: &T) -> io::Result<()> {
        self.write_json(&self.root().join("traffic.json"), traffic).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[tokio::test]
    async fn round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::at(dir.path());
        assert_eq!(store.load_usage::<BTreeMap<String, u64>>().await, None);
        let usage = BTreeMap::from([("{A}".to_string(), 42u64)]);
        store.save_usage(&usage).await.unwrap();
        assert_eq!(store.load_usage().await, Some(usage.clone()));
        // Another shape reads as nothing rather than failing.
        assert_eq!(store.load_usage::<Vec<String>>().await, None);
        // The app's traffic file is its own.
        assert_eq!(store.load_traffic::<BTreeMap<String, u64>>().await, None);
        store.save_traffic(&usage).await.unwrap();
        assert_eq!(store.load_traffic().await, Some(usage));
    }
}
