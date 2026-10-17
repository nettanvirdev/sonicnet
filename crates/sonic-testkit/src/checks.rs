//! Rules every download state must obey, whatever the scenario (Plexo's `checkEvents`). The
//! [`Harness`](crate::Harness) runs them on every `DownloadUpdated` it receives, so each test
//! checks them without asking: what the UI shows is only as true as these.

use std::collections::{BTreeSet, HashMap};

use sonic_core::{BlockStatus, DownloadState, DownloadStatus, StreamStatus};

/// What is wrong with one state, if anything (empty = fine).
pub fn state_violations(s: &DownloadState) -> Vec<String> {
    let mut found = Vec::new();
    let mut check = |ok: bool, what: String| {
        if !ok {
            found.push(format!("{} @ {:?}: {what}", s.id, s.status));
        }
    };

    if let Some(total) = s.total_bytes {
        check(
            s.bytes_downloaded <= total,
            format!("bytes_downloaded {} > total {total}", s.bytes_downloaded),
        );
    }
    check(s.speed_bps.is_finite() && s.speed_bps >= 0.0, format!("speed_bps is {}", s.speed_bps));

    // Blocks: in order, back to back, each within its range, attributed exactly.
    let mut next_start = 0;
    for (position, block) in s.blocks.iter().enumerate() {
        let i = block.index;
        check(block.index as usize == position, format!("block {i} listed at position {position}"));
        check(
            block.range.start == next_start,
            format!(
                "block {i} starts at {} but the previous ended at {next_start}",
                block.range.start
            ),
        );
        next_start = block.range.end.unwrap_or(u64::MAX);
        let attributed: u64 = block.bytes_by_network.values().sum();
        check(
            attributed == block.bytes_downloaded,
            format!(
                "block {i}: bytes_by_network add up to {attributed}, its bytes_downloaded is {}",
                block.bytes_downloaded
            ),
        );
        if let Some(len) = block.range.len() {
            check(
                block.bytes_downloaded <= len,
                format!("block {i}: {} bytes downloaded of {len}", block.bytes_downloaded),
            );
            if block.status == BlockStatus::Completed {
                check(
                    block.bytes_downloaded == len,
                    format!("block {i} completed with {} of {len} bytes", block.bytes_downloaded),
                );
            }
        }
    }
    if let (Some(total), Some(last)) = (s.total_bytes, s.blocks.last()) {
        check(
            last.range.end == Some(total),
            format!("the blocks end at {:?}, the file at {total}", last.range.end),
        );
    }
    if !s.blocks.is_empty() {
        let in_blocks: u64 = s.blocks.iter().map(|b| b.bytes_downloaded).sum();
        check(
            in_blocks == s.bytes_downloaded,
            format!("blocks add up to {in_blocks}, bytes_downloaded is {}", s.bytes_downloaded),
        );
    }

    // Networks: their bytes add up to the download's.
    let by_networks: u64 = s.networks.iter().map(|n| n.bytes_downloaded).sum();
    check(
        by_networks == s.bytes_downloaded,
        format!("networks add up to {by_networks}, bytes_downloaded is {}", s.bytes_downloaded),
    );
    let ids: BTreeSet<_> = s.networks.iter().map(|n| &n.id).collect();
    check(ids.len() == s.networks.len(), "a network is listed twice".into());

    // Streams: on a listed network; one fetching a block for real, at most two racing it.
    let mut primaries: HashMap<u32, u32> = HashMap::new();
    let mut hedges: HashMap<u32, u32> = HashMap::new();
    for stream in &s.streams {
        let id = stream.id;
        check(
            ids.contains(&stream.network_id),
            format!("stream {id} is on {}, which the download doesn't list", stream.network_id),
        );
        if stream.status == StreamStatus::Pending {
            check(stream.block.is_none(), format!("pending stream {id} holds a block"));
        }
        let Some(block) = stream.block else {
            check(!stream.hedge, format!("stream {id} races without a block"));
            continue;
        };
        check(
            (block as usize) < s.blocks.len() || s.blocks.is_empty(),
            format!("stream {id} holds block {block}, past the last"),
        );
        *(if stream.hedge { &mut hedges } else { &mut primaries }).entry(block).or_default() += 1;
        // What the stream rows show is only as true as this: a block fetched for real is in
        // flight, not pending, and not already complete.
        if !stream.hedge
            && let Some(held) = s.blocks.get(block as usize)
        {
            check(
                held.status == BlockStatus::Downloading,
                format!(
                    "stream {id} ({:?}) holds block {block}, which is {:?}",
                    stream.status, held.status
                ),
            );
        }
    }
    for (block, count) in primaries {
        check(count <= 1, format!("block {block} has {count} streams fetching it (not hedging)"));
    }
    for (block, count) in hedges {
        check(count <= 2, format!("block {block} has {count} hedges"));
    }

    if s.status == DownloadStatus::Completed {
        if let Some(total) = s.total_bytes {
            check(
                s.bytes_downloaded == total,
                format!("completed with {} of {total} bytes", s.bytes_downloaded),
            );
        }
        check(
            s.blocks.iter().all(|b| b.status == BlockStatus::Completed),
            "completed with blocks unfinished".into(),
        );
    }
    found
}

/// Whether going from `from` to `to` is possible. Updates are throttled, so a quick
/// `downloading` between two other states may never be seen; what can never happen is leaving a
/// final state.
pub fn transition_violation(from: DownloadStatus, to: DownloadStatus) -> Option<String> {
    let final_state = matches!(from, DownloadStatus::Completed | DownloadStatus::Cancelled);
    (final_state && to != from).then(|| format!("went from {from:?} to {to:?}"))
}
