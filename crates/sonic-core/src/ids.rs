use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Identifies one download for its whole life, across restarts.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DownloadId(pub Uuid);

impl DownloadId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for DownloadId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for DownloadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DownloadId({})", self.0)
    }
}

impl fmt::Display for DownloadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Identifies one network adapter, stable across renames and reconnects. On Windows it is the
/// adapter's GUID (`{…}`), which survives the user renaming "Ethernet 2" and the phone being
/// unplugged and plugged back in.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NetworkId(pub String);

impl NetworkId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for NetworkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NetworkId({})", self.0)
    }
}

impl fmt::Display for NetworkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// One connection to the server within a download. Unique within that download, in the order
/// the streams were started.
pub type StreamId = u32;

/// A block's position in its download's block list.
pub type BlockIndex = u32;
