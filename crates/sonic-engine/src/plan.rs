//! How a download is cut into blocks, and how many streams a network starts on them.
//!
//! How many streams a network ends up with is decided while the download runs (see
//! [`concurrency`](crate::concurrency)); the plan only has to leave them enough blocks. The block
//! layout itself is [`sonic_core::plan_blocks`]; this module only picks the size.

use std::collections::HashMap;
use std::hash::Hash;

use sonic_core::{BlockState, MAX_STREAMS_PER_NETWORK, START_STREAMS_PER_NETWORK, plan_blocks};

const MIB: u64 = 1024 * 1024;

/// The largest block. However big the file, a block stays small enough for one stream to finish
/// soon: a hedge re-fetches a block rather than splitting it (see
/// [`scheduler`](crate::scheduler)), so the last block's size bounds how long the slowest
/// connection can hold the whole download up.
pub const MAX_BLOCK_BYTES: u64 = 8 * MIB;

/// The smallest block planned. Each block is its own request, and below about a megabyte the round
/// trips cost more than splitting the file saves.
pub const MIN_BLOCK_BYTES: u64 = MIB;

/// Blocks planned per stream a network could grow to. Streams pull blocks as they free up, so a
/// fast network takes more of them — but only when there are more blocks than streams. With one
/// block each, a stream on a slow network would hold its block while the rest sat idle.
pub const BLOCKS_PER_STREAM: u64 = 2;

/// A download's block size and the blocks it makes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Plan {
    /// Bytes per block; the last block holds the remainder.
    pub block_size: u64,
    pub blocks: Vec<BlockState>,
}

/// Bytes per block for a file of `total_bytes`, fetched over `network_count` networks to start
/// with (more may join later; this only sizes the blocks).
///
/// Aims for every network to be able to grow to [`MAX_STREAMS_PER_NETWORK`] with
/// [`BLOCKS_PER_STREAM`] blocks each, within [`MIN_BLOCK_BYTES`]..=[`MAX_BLOCK_BYTES`]. A file
/// that can't be split (no range support, or no known size) is one block carrying all of it.
pub fn block_size(total_bytes: Option<u64>, splittable: bool, network_count: usize) -> u64 {
    let total = match total_bytes {
        Some(total) if splittable && total > 0 => total,
        // One request carries the whole file.
        other => return other.unwrap_or(0),
    };
    let networks = network_count.max(1) as u64;
    let target_blocks = networks * u64::from(MAX_STREAMS_PER_NETWORK) * BLOCKS_PER_STREAM;
    total.div_ceil(target_blocks).clamp(MIN_BLOCK_BYTES, MAX_BLOCK_BYTES)
}

/// Sizes the blocks (see [`block_size`]) and lays them out with [`plan_blocks`].
pub fn plan(total_bytes: Option<u64>, splittable: bool, network_count: usize) -> Plan {
    let block_size = block_size(total_bytes, splittable, network_count);
    Plan { block_size, blocks: plan_blocks(total_bytes, block_size) }
}

/// Streams to start on each of `joining_networks` networks joining a download with `waiting`
/// blocks nobody has taken: [`START_STREAMS_PER_NETWORK`], or the user's `requested` count.
///
/// A stream with no block to claim would only sit idle, so near the end, or on a small file, fewer
/// — but never none, so a network that joins late can still race a slow block.
pub fn starting_streams(waiting: usize, joining_networks: usize, requested: Option<u32>) -> u32 {
    let requested =
        requested.unwrap_or(START_STREAMS_PER_NETWORK).clamp(1, MAX_STREAMS_PER_NETWORK);
    let per_network = waiting.div_ceil(joining_networks.max(1));
    let per_network = u32::try_from(per_network).unwrap_or(u32::MAX);
    requested.min(per_network).max(1)
}

/// Lays `items` out one from each group in turn (a, b, a, b, …), keeping each group's own order.
/// Groups are visited in order of first appearance.
///
/// New streams claim a block the moment they start, so starting them interleaved by network gives
/// every network its first block before any gets a second: a small file shouldn't all go to
/// whichever network happened to be listed first.
pub fn interleave<T, K: Eq + Hash>(items: Vec<T>, group_of: impl Fn(&T) -> K) -> Vec<T> {
    let total = items.len();
    let mut lane_of: HashMap<K, usize> = HashMap::new();
    let mut grouped: Vec<Vec<T>> = Vec::new();
    for item in items {
        let lane = *lane_of.entry(group_of(&item)).or_insert_with(|| {
            grouped.push(Vec::new());
            grouped.len() - 1
        });
        grouped[lane].push(item);
    }

    let mut lanes: Vec<_> = grouped.into_iter().map(Vec::into_iter).collect();
    let mut out = Vec::with_capacity(total);
    while out.len() < total {
        out.extend(lanes.iter_mut().filter_map(Iterator::next));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn too_small_to_split_is_one_block_and_each_network_still_gets_a_stream() {
        let plan = plan(Some(300 * 1024), true, 2);
        assert_eq!(plan.blocks.len(), 1);
        assert_eq!(starting_streams(plan.blocks.len(), 2, Some(8)), 1);
    }

    #[test]
    fn a_big_file_has_blocks_for_every_network_to_grow_to_its_limit() {
        let plan = plan(Some(512 * MIB), true, 2);
        assert!(plan.blocks.len() as u64 >= 2 * u64::from(MAX_STREAMS_PER_NETWORK) * 2);
    }

    #[test]
    fn a_mid_size_file_is_cut_so_a_fast_network_can_out_pull_a_slow_one() {
        // Two 8 MiB-ish blocks would pin a third of the file to whichever network is slower.
        assert!(plan(Some(12 * MIB), true, 2).blocks.len() >= 8);
    }

    #[test]
    fn a_huge_file_gets_more_blocks_not_bigger_ones() {
        assert_eq!(block_size(Some(1 << 40), true, 1), MAX_BLOCK_BYTES);
    }

    #[test]
    fn unsplittable_or_unknown_size_is_one_block() {
        for (total, splittable) in [(Some(10 * MIB), false), (Some(0), true), (None, true)] {
            let plan = plan(total, splittable, 3);
            assert_eq!(plan.blocks.len(), 1, "{total:?} {splittable}");
            assert_eq!(plan.blocks[0].range.end, total);
        }
    }

    #[test]
    fn starting_streams_defaults_to_eight_and_honours_a_pick() {
        assert_eq!(starting_streams(1000, 1, None), START_STREAMS_PER_NETWORK);
        assert_eq!(starting_streams(1000, 1, Some(16)), 16);
        assert_eq!(starting_streams(1000, 1, Some(500)), MAX_STREAMS_PER_NETWORK);
        // Near the end: only as many as there are blocks to share out.
        assert_eq!(starting_streams(5, 2, None), 3);
        assert_eq!(starting_streams(0, 2, None), 1);
    }

    #[test]
    fn interleave_serves_every_group_before_any_twice() {
        assert_eq!(interleave(vec!['a', 'a', 'a', 'b', 'b'], |c| *c), ['a', 'b', 'a', 'b', 'a']);
        assert_eq!(interleave(Vec::<u8>::new(), |c| *c), Vec::<u8>::new());
    }

    proptest! {
        #[test]
        fn blocks_tile_the_file_exactly(total in 1u64..(1 << 42), networks in 1usize..=6) {
            let plan = plan(Some(total), true, networks);
            let size = plan.block_size;
            prop_assert!(size >= MIN_BLOCK_BYTES.min(total) && size <= MAX_BLOCK_BYTES);
            let mut next = 0;
            for (index, block) in plan.blocks.iter().enumerate() {
                prop_assert_eq!(block.index as usize, index);
                prop_assert_eq!(block.range.start, next);
                let end = block.range.end.unwrap_or(0);
                prop_assert!(end > next && end - next <= size);
                next = end;
            }
            prop_assert_eq!(next, total);
        }

        #[test]
        fn a_joining_network_gets_a_stream_but_none_without_a_block(
            waiting in 0usize..10_000,
            networks in 1usize..=6,
            requested in proptest::option::of(1u32..=20),
        ) {
            let count = starting_streams(waiting, networks, requested);
            prop_assert!(count >= 1);
            prop_assert!(count <= requested.unwrap_or(START_STREAMS_PER_NETWORK));
            prop_assert!(count <= MAX_STREAMS_PER_NETWORK);
            prop_assert!(count as usize <= waiting.div_ceil(networks).max(1));
        }

        #[test]
        fn interleave_keeps_every_item_and_each_group_in_order(
            items in proptest::collection::vec((0u8..4, any::<u32>()), 0..40),
        ) {
            let out = interleave(items.clone(), |(group, _)| *group);
            prop_assert_eq!(out.len(), items.len());
            for group in 0..4 {
                let of = |list: &[(u8, u32)]| -> Vec<(u8, u32)> {
                    list.iter().filter(|(g, _)| *g == group).copied().collect()
                };
                prop_assert_eq!(of(&out), of(&items));
            }
        }
    }
}
