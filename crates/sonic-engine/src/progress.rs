//! A block's progress: its frontier, and which network delivered it.
//!
//! `BlockState::bytes_downloaded` is the block's *frontier*: how much of it, from its first byte,
//! is secured. Near the end of a download two attempts can race for one block (see
//! [`scheduler`](crate::scheduler)), so the frontier is the furthest either has reached, not the
//! sum of what they received. `bytes_by_network` splits the frontier by network and always adds up
//! to it; [`advance_block`] and [`retract_block`] are the only ways either changes, which is what
//! keeps that true.

use sonic_core::{BlockState, NetworkId};

/// Moves the frontier forward to `position` (a byte offset into the block, clamped to its length),
/// crediting the bytes gained to `network`. Does nothing if something already got further: a
/// racing attempt behind the frontier delivers nothing new. Returns the gain.
pub fn advance_block(block: &mut BlockState, network: &NetworkId, position: u64) -> u64 {
    let next = block.range.len().map_or(position, |len| position.min(len));
    let gained = next.saturating_sub(block.bytes_downloaded);
    if gained == 0 {
        return 0;
    }
    block.bytes_downloaded = next;
    *block.bytes_by_network.entry(network.clone()).or_insert(0) += gained;
    gained
}

/// Pulls the frontier back to `position`, for bytes that turned out not to be secured (a failed
/// write) or a racing attempt that lost. Returns how much it removed.
///
/// The lost bytes are always the tail, so they come off `blamed` (the network that wrote last)
/// first, before spilling over to the others.
pub fn retract_block(block: &mut BlockState, position: u64, blamed: Option<&NetworkId>) -> u64 {
    let removed = block.bytes_downloaded.saturating_sub(position);
    if removed == 0 {
        return 0;
    }
    trim(block, position, blamed);
    block.bytes_downloaded = position;
    removed
}

/// Drops attribution beyond `keep` bytes, `blamed` first, so the tallies sum to `keep`.
fn trim(block: &mut BlockState, keep: u64, blamed: Option<&NetworkId>) {
    let attributed: u64 = block.bytes_by_network.values().sum();
    let mut excess = attributed.saturating_sub(keep);
    let blamed = blamed.filter(|id| block.bytes_by_network.contains_key(*id)).cloned();
    let order: Vec<NetworkId> = blamed
        .iter()
        .cloned()
        .chain(block.bytes_by_network.keys().filter(|id| Some(*id) != blamed.as_ref()).cloned())
        .collect();
    for id in order {
        if excess == 0 {
            break;
        }
        let Some(bytes) = block.bytes_by_network.get_mut(&id) else { continue };
        let taken = (*bytes).min(excess);
        *bytes -= taken;
        excess -= taken;
        if *bytes == 0 {
            block.bytes_by_network.remove(&id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use sonic_core::ByteRange;

    const LENGTH: u64 = 1000;

    fn fresh() -> BlockState {
        BlockState::new(0, ByteRange::new(0, Some(LENGTH)))
    }

    fn id(name: &str) -> NetworkId {
        NetworkId::new(name)
    }

    fn shares(block: &BlockState) -> Vec<(&str, u64)> {
        block.bytes_by_network.iter().map(|(id, bytes)| (id.as_str(), *bytes)).collect()
    }

    #[test]
    fn racing_attempts_count_only_the_ground_the_furthest_covers() {
        let mut block = fresh();
        assert_eq!(advance_block(&mut block, &id("a"), 300), 300);
        assert_eq!(advance_block(&mut block, &id("b"), 200), 0, "behind: nothing new");
        assert_eq!(advance_block(&mut block, &id("b"), 500), 200, "only what goes past a");
        assert_eq!(block.bytes_downloaded, 500);
        assert_eq!(shares(&block), [("a", 300), ("b", 200)]);
    }

    #[test]
    fn the_frontier_never_passes_the_blocks_end() {
        let mut block = fresh();
        advance_block(&mut block, &id("a"), LENGTH + 500);
        assert_eq!(block.bytes_downloaded, LENGTH);
    }

    #[test]
    fn an_open_ended_block_has_no_end_to_clamp_to() {
        let mut block = BlockState::new(0, ByteRange::new(0, None));
        assert_eq!(advance_block(&mut block, &id("a"), 1 << 40), 1 << 40);
    }

    #[test]
    fn retracting_takes_bytes_off_the_blamed_network_first() {
        let mut block = fresh();
        advance_block(&mut block, &id("a"), 300);
        advance_block(&mut block, &id("b"), 500);
        assert_eq!(retract_block(&mut block, 300, Some(&id("b"))), 200);
        assert_eq!(shares(&block), [("a", 300)]);
    }

    #[test]
    fn retracting_past_the_blamed_share_spills_over_to_the_others() {
        let mut block = fresh();
        advance_block(&mut block, &id("a"), 300);
        advance_block(&mut block, &id("b"), 500);
        assert_eq!(retract_block(&mut block, 100, Some(&id("b"))), 400);
        assert_eq!(shares(&block), [("a", 100)]);
        // Nobody blamed, or blamed a network with no share: still adds up.
        assert_eq!(retract_block(&mut block, 50, Some(&id("zz"))), 50);
        assert_eq!(shares(&block), [("a", 50)]);
        assert_eq!(retract_block(&mut block, 60, None), 0, "ahead of the frontier: nothing");
    }

    #[derive(Clone, Debug)]
    enum Step {
        Advance(u8, u64),
        Retract(u8, u64),
    }

    fn step() -> impl Strategy<Value = Step> {
        prop_oneof![
            (0u8..3, 0..LENGTH + 200).prop_map(|(n, p)| Step::Advance(n, p)),
            (0u8..4, 0..=LENGTH).prop_map(|(n, p)| Step::Retract(n, p)),
        ]
    }

    proptest! {
        #[test]
        fn attribution_always_adds_up_to_the_frontier(
            steps in proptest::collection::vec(step(), 0..60),
        ) {
            let names = ["a", "b", "c", "nobody"];
            let mut block = fresh();
            for step in steps {
                match step {
                    Step::Advance(n, p) => {
                        let before = block.bytes_downloaded;
                        let gained = advance_block(&mut block, &id(names[n as usize]), p);
                        prop_assert_eq!(block.bytes_downloaded, before + gained);
                    }
                    Step::Retract(n, p) => {
                        let blamed = (n < 3).then(|| id(names[n as usize]));
                        retract_block(&mut block, p, blamed.as_ref());
                    }
                }
                let attributed: u64 = block.bytes_by_network.values().sum();
                prop_assert_eq!(attributed, block.bytes_downloaded);
                prop_assert!(block.bytes_downloaded <= LENGTH);
                prop_assert!(block.bytes_by_network.values().all(|bytes| *bytes > 0));
            }
        }
    }
}
