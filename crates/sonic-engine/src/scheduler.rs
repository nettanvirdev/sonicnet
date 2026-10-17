//! Who fetches what, decided from a snapshot of the download and nothing else.
//!
//! A free stream is given one of two kinds of work:
//!
//! - **primary**: a block nobody is fetching. The normal case, and the only one until the queue
//!   runs dry.
//! - **hedge**: a second attempt at a block someone else is fetching too slowly. Only handed out
//!   once no block is left waiting, so it never takes bandwidth from work that still needs doing:
//!   what a hedge costs is bytes fetched twice at the very end, and what it buys is that one slow
//!   connection can no longer hold the whole download back. Whichever attempt finishes first wins;
//!   the others are dropped. A block whose hedge is stuck too can be raced again, a few times.

use std::collections::{BTreeMap, HashMap};

use sonic_core::{
    BlockIndex, BlockState, BlockStatus, Millis, NetworkId, StreamId, StreamState, StreamStatus,
};

/// Whether an attempt is a block's primary fetch or a race against it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AttemptKind {
    Primary,
    Hedge,
}

/// One attempt in flight at a block, as the scheduler needs to see it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttemptView {
    pub kind: AttemptKind,
    pub stream: StreamId,
    pub network: NetworkId,
    /// When its request was sent.
    pub started_at: Millis,
    /// Its reader is paused for writes, or its body ended and the writer is flushing: the disk, not
    /// the connection, sets its pace.
    pub write_waiting: bool,
}

/// What [`pick_work`] decides from. Borrowed from whoever runs the download.
#[derive(Clone, Copy, Debug)]
pub struct SchedulerView<'a> {
    /// Indexed by `BlockState::index`.
    pub blocks: &'a [BlockState],
    pub streams: &'a [StreamState],
    /// Attempts in flight, by block.
    pub attempts: &'a BTreeMap<BlockIndex, Vec<AttemptView>>,
    /// For a block whose last attempt on some network delivered nothing, that network.
    pub avoid: &'a HashMap<BlockIndex, NetworkId>,
    /// Hedges each block has had so far.
    pub hedges_used: &'a HashMap<BlockIndex, u32>,
    /// The disk can't keep up (see [`DiskWatch`](crate::concurrency::DiskWatch)): a hedge would
    /// only add writes.
    pub disk_behind: bool,
}

/// Tuning for hedges.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SchedulerPolicy {
    /// A block is only raced once every attempt at it has been going this long: long enough to
    /// have measured a real speed.
    pub hedge_after_ms: Millis,
    /// Bounds the duplicate work on a block whose hedges keep failing.
    pub max_hedges_per_block: u32,
    /// What a new request costs before its first byte (connecting, the server answering), counted
    /// into how long a hedge would take.
    pub startup_ms: Millis,
}

impl Default for SchedulerPolicy {
    fn default() -> Self {
        Self { hedge_after_ms: 2_000, max_hedges_per_block: 2, startup_ms: 1_000 }
    }
}

/// The stream asking for work.
#[derive(Clone, Copy, Debug)]
pub struct Requester<'a> {
    pub stream: StreamId,
    pub network: &'a NetworkId,
    /// How fast this stream fetched its last block, if it has finished one. Without it, a block is
    /// only raced once its attempts need `hedge_after_ms` more.
    pub speed_bps: Option<f64>,
}

/// What a stream should fetch next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Work {
    pub kind: AttemptKind,
    pub block: BlockIndex,
}

/// The next work for `who`: the first waiting block (see [`next_waiting_block`]), or else a block
/// worth racing (see [`next_hedge_target`]).
pub fn pick_work(
    view: &SchedulerView<'_>,
    who: &Requester<'_>,
    now: Millis,
    policy: &SchedulerPolicy,
) -> Option<Work> {
    if let Some(block) = next_waiting_block(view, who.network) {
        return Some(Work { kind: AttemptKind::Primary, block });
    }
    next_hedge_target(view, who, now, policy).map(|block| Work { kind: AttemptKind::Hedge, block })
}

/// A stream waiting for work.
fn is_free(stream: &StreamState) -> bool {
    stream.status == StreamStatus::Pending
}

/// The first waiting block — except that one whose last attempt on this network delivered nothing
/// is left to another network, for as long as a stream there is free to take it. Otherwise a
/// network that isn't answering would be handed the same block again and again while healthy
/// networks sat idle beside it. With no other network free the block is taken anyway, so it can
/// never be stranded.
fn next_waiting_block(view: &SchedulerView<'_>, network: &NetworkId) -> Option<BlockIndex> {
    let other_network_free = view.streams.iter().any(|s| &s.network_id != network && is_free(s));
    view.blocks
        .iter()
        .find(|block| {
            block.status == BlockStatus::Pending
                && !(other_network_free && view.avoid.get(&block.index) == Some(network))
        })
        .map(|block| block.index)
}

/// The block most worth another attempt: the one that will take longest yet, if the requester
/// would clearly beat it. A block is as slow as its fastest attempt.
fn next_hedge_target(
    view: &SchedulerView<'_>,
    who: &Requester<'_>,
    now: Millis,
    policy: &SchedulerPolicy,
) -> Option<BlockIndex> {
    // Only when everything left is already being fetched, and the disk has room for more writes.
    if view.disk_behind || view.blocks.iter().any(|block| block.status == BlockStatus::Pending) {
        return None;
    }
    let speed_of = |stream: StreamId| {
        view.streams.iter().find(|s| s.id == stream).map_or(0.0, |s| s.speed_bps)
    };

    let mut target = None;
    let mut latest = 0.0;
    for (&index, attempts) in view.attempts {
        let Some(block) = view.blocks.get(index as usize).filter(|b| b.index == index) else {
            continue;
        };
        // An open-ended block has no finish time to beat.
        let Some(length) = block.range.len() else { continue };
        if block.status != BlockStatus::Downloading {
            continue;
        }
        // Nothing to race if every holder let go: the block goes back to the queue instead.
        if !attempts.iter().any(|a| a.kind == AttemptKind::Primary) {
            continue;
        }
        if attempts.iter().any(|a| a.stream == who.stream) {
            continue;
        }
        if view.hedges_used.get(&index).copied().unwrap_or(0) >= policy.max_hedges_per_block {
            continue;
        }
        // A network that got nothing from this block won't do better on a second try.
        if view.avoid.get(&index) == Some(who.network) {
            continue;
        }
        // Speeds only mean something once every attempt has been going a while.
        if attempts.iter().any(|a| now - a.started_at < policy.hedge_after_ms) {
            continue;
        }
        // A holder waiting on its writer is as slow as the disk, which another attempt can't beat.
        if attempts.iter().any(|a| a.write_waiting) {
            continue;
        }
        let remaining = length.saturating_sub(block.bytes_downloaded);
        if remaining == 0 {
            continue;
        }
        let remaining = remaining as f64;
        // A holder that has gone quiet has no finish time at all.
        let eta_ms = attempts
            .iter()
            .map(|a| match speed_of(a.stream) {
                speed if speed > 0.0 => remaining / speed * 1000.0,
                _ => f64::INFINITY,
            })
            .fold(f64::INFINITY, f64::min);
        // Worth it only if the requester would finish in under half that time. One whose speed
        // isn't known yet races only a block that needs at least `hedge_after_ms` more.
        let worth_it = match who.speed_bps {
            Some(speed) if speed > 0.0 => {
                eta_ms >= 2.0 * (policy.startup_ms as f64 + remaining / speed * 1000.0)
            }
            _ => eta_ms >= policy.hedge_after_ms as f64,
        };
        if !worth_it {
            continue;
        }
        // The holders' network may be what is slow, so a stream on another network gets the first
        // go — unless none of those would take it either, or the requester's speed is known and
        // clearly beats the holders': then its network isn't what holds the block up (a few of
        // its many connections crawling is), and leaving the block to another network, maybe a
        // slower one, only kept the fast network idle through the end of the download.
        let held_by = |network: &NetworkId| attempts.iter().any(|a| &a.network == network);
        let other_network_free = view.streams.iter().any(|s| {
            !held_by(&s.network_id) && is_free(s) && view.avoid.get(&index) != Some(&s.network_id)
        });
        let proven = who.speed_bps.is_some_and(|speed| speed > 0.0);
        if held_by(who.network) && other_network_free && !proven {
            continue;
        }
        if eta_ms > latest {
            target = Some(index);
            latest = eta_ms;
        }
    }
    target
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use sonic_core::BlockStatus::Downloading;
    use sonic_core::ByteRange;

    const POLICY: SchedulerPolicy =
        SchedulerPolicy { hedge_after_ms: 5000, max_hedges_per_block: 2, startup_ms: 1000 };
    const NOW: Millis = 100_000;
    const LENGTH: u64 = 1000;

    fn block(index: BlockIndex, status: BlockStatus, bytes: u64) -> BlockState {
        let start = u64::from(index) * LENGTH;
        let mut block = BlockState::new(index, ByteRange::new(start, Some(start + LENGTH)));
        block.status = status;
        block.bytes_downloaded = bytes;
        block
    }
    fn pending(index: BlockIndex) -> BlockState {
        block(index, BlockStatus::Pending, 0)
    }
    fn done(index: BlockIndex) -> BlockState {
        block(index, BlockStatus::Completed, LENGTH)
    }
    fn busy(index: BlockIndex, bytes: u64) -> BlockState {
        block(index, Downloading, bytes)
    }

    fn stream(id: StreamId, network: &str, status: StreamStatus, speed: f64) -> StreamState {
        StreamState {
            id,
            network_id: NetworkId::new(network),
            status,
            block: None,
            hedge: false,
            bytes_downloaded: 0,
            speed_bps: speed,
        }
    }
    fn free(id: StreamId, network: &str) -> StreamState {
        stream(id, network, StreamStatus::Pending, 0.0)
    }
    fn fetching(id: StreamId, network: &str, speed: f64) -> StreamState {
        stream(id, network, StreamStatus::Downloading, speed)
    }

    fn attempt(stream: StreamId, network: &str, kind: AttemptKind, age: Millis) -> AttemptView {
        AttemptView {
            kind,
            stream,
            network: NetworkId::new(network),
            started_at: NOW - age,
            write_waiting: false,
        }
    }
    fn primary(stream: StreamId, network: &str) -> AttemptView {
        attempt(stream, network, AttemptKind::Primary, 10_000)
    }
    fn hedge(stream: StreamId, network: &str) -> AttemptView {
        attempt(stream, network, AttemptKind::Hedge, 10_000)
    }

    /// A download as owned data, borrowed into a [`SchedulerView`] per pick.
    #[derive(Clone, Default)]
    struct State {
        blocks: Vec<BlockState>,
        streams: Vec<StreamState>,
        attempts: BTreeMap<BlockIndex, Vec<AttemptView>>,
        avoid: HashMap<BlockIndex, NetworkId>,
        hedges_used: HashMap<BlockIndex, u32>,
        disk_behind: bool,
    }

    impl State {
        fn new(blocks: Vec<BlockState>, streams: Vec<StreamState>) -> Self {
            Self { blocks, streams, ..Default::default() }
        }
        fn attempts(mut self, index: BlockIndex, attempts: Vec<AttemptView>) -> Self {
            self.attempts.insert(index, attempts);
            self
        }
        fn avoid(mut self, index: BlockIndex, network: &str) -> Self {
            self.avoid.insert(index, NetworkId::new(network));
            self
        }
        fn hedges_used(mut self, index: BlockIndex, used: u32) -> Self {
            self.hedges_used.insert(index, used);
            self
        }
        fn view(&self) -> SchedulerView<'_> {
            SchedulerView {
                blocks: &self.blocks,
                streams: &self.streams,
                attempts: &self.attempts,
                avoid: &self.avoid,
                hedges_used: &self.hedges_used,
                disk_behind: self.disk_behind,
            }
        }
        fn pick_at(&self, id: StreamId, network: &str, speed: Option<f64>) -> Option<String> {
            let network = NetworkId::new(network);
            let who = Requester { stream: id, network: &network, speed_bps: speed };
            pick_work(&self.view(), &who, NOW, &POLICY).map(|work| {
                let kind = if work.kind == AttemptKind::Primary { "primary" } else { "hedge" };
                format!("{kind}:{}", work.block)
            })
        }
        fn pick(&self, id: StreamId, network: &str) -> Option<String> {
            self.pick_at(id, network, None)
        }
    }

    // ----- taking the next block -----

    #[test]
    fn takes_the_first_waiting_block() {
        let s = State::new(vec![done(0), busy(1, 0), pending(2), pending(3)], vec![]);
        assert_eq!(s.pick(9, "a").as_deref(), Some("primary:2"));
    }

    #[test]
    fn nothing_waiting_and_nothing_to_race_is_nothing_to_do() {
        assert_eq!(State::new(vec![done(0), done(1)], vec![]).pick(9, "a"), None);
    }

    #[test]
    fn a_block_a_network_got_nothing_from_goes_to_another_free_network() {
        let s = State::new(vec![pending(0), pending(1)], vec![free(0, "a"), free(1, "b")])
            .avoid(0, "b");
        assert_eq!(s.pick(1, "b").as_deref(), Some("primary:1"));
        assert_eq!(s.pick(0, "a").as_deref(), Some("primary:0"));
    }

    #[test]
    fn but_takes_it_back_when_no_other_network_is_free_so_it_is_never_stranded() {
        let streams =
            vec![fetching(0, "a", 0.0), stream(1, "a", StreamStatus::Retrying, 0.0), free(2, "b")];
        let s = State::new(vec![pending(0)], streams).avoid(0, "b");
        assert_eq!(s.pick(2, "b").as_deref(), Some("primary:0"));
    }

    #[test]
    fn another_free_network_only_matters_to_the_network_that_failed() {
        let s = State::new(vec![pending(0)], vec![free(0, "a"), free(1, "b")]).avoid(0, "b");
        assert_eq!(s.pick(0, "a").as_deref(), Some("primary:0"));
    }

    // ----- racing a slow block -----

    /// Block 0: 1000 bytes with 200 done, held by stream 0 on network a at `speed`.
    fn laggard(speed: f64) -> State {
        State::new(vec![busy(0, 200), done(1)], vec![fetching(0, "a", speed), free(1, "a")])
            .attempts(0, vec![primary(0, "a")])
    }

    #[test]
    fn a_free_stream_races_the_block_whose_holder_needs_longest() {
        // 800 bytes left at 10 B/s: 80 s to go, against a 5 s threshold.
        assert_eq!(laggard(10.0).pick(1, "a").as_deref(), Some("hedge:0"));
    }

    #[test]
    fn a_quiet_holder_has_no_finish_time_so_it_is_raced() {
        assert_eq!(laggard(0.0).pick(1, "a").as_deref(), Some("hedge:0"));
    }

    #[test]
    fn but_one_quiet_because_its_writer_is_full_is_left_alone() {
        let mut held = primary(0, "a");
        held.write_waiting = true;
        assert_eq!(laggard(0.0).attempts(0, vec![held]).pick(1, "a"), None);
    }

    #[test]
    fn a_holder_about_to_finish_is_left_alone() {
        assert_eq!(laggard(1000.0).pick(1, "a"), None); // 0.8 s to go
    }

    #[test]
    fn the_threshold_is_the_finish_time_not_the_speed() {
        // 10 bytes left at 10 B/s: 1 s.
        let s = State::new(vec![busy(0, 990), done(1)], vec![fetching(0, "a", 10.0), free(1, "a")])
            .attempts(0, vec![primary(0, "a")]);
        assert_eq!(s.pick(1, "a"), None);
    }

    #[test]
    fn a_holder_that_only_just_started_has_no_speed_yet() {
        let s = laggard(0.0).attempts(0, vec![attempt(0, "a", AttemptKind::Primary, 1000)]);
        assert_eq!(s.pick(1, "a"), None);
    }

    #[test]
    fn never_while_a_block_is_waiting() {
        let mut s = laggard(0.0);
        s.blocks[1] = pending(1);
        assert_eq!(s.pick(1, "a").as_deref(), Some("primary:1"));
        // A stream that has to decline the waiting block does not race instead: b sits it out.
        s.streams = vec![fetching(0, "a", 0.0), free(1, "b"), free(2, "a")];
        let s = s.avoid(1, "b");
        assert_eq!(s.pick(1, "b"), None);
        assert_eq!(s.pick(2, "a").as_deref(), Some("primary:1"));
    }

    #[test]
    fn a_block_whose_hedge_is_stuck_too_is_raced_again_up_to_the_limit() {
        let mut hedged =
            laggard(0.0).attempts(0, vec![primary(0, "a"), hedge(1, "a")]).hedges_used(0, 1);
        hedged.streams = vec![fetching(0, "a", 0.0), fetching(1, "a", 0.0), free(2, "a")];
        assert_eq!(hedged.pick(2, "a").as_deref(), Some("hedge:0"));
        assert_eq!(laggard(0.0).hedges_used(0, 2).pick(1, "a"), None);
    }

    #[test]
    fn but_not_while_its_hedge_is_doing_fine() {
        let mut hedged =
            laggard(0.0).attempts(0, vec![primary(0, "a"), hedge(1, "a")]).hedges_used(0, 1);
        hedged.streams = vec![fetching(0, "a", 0.0), fetching(1, "a", 1000.0), free(2, "a")];
        assert_eq!(hedged.pick(2, "a"), None);
    }

    #[test]
    fn a_stream_with_a_known_speed_races_as_soon_as_it_would_clearly_win() {
        // 800 bytes at 200 B/s: 4 s. Under the 5 s an unknown stream waits for, but at 800 B/s
        // this one needs 1 s + 1 s startup.
        let s = laggard(200.0);
        assert_eq!(s.pick(1, "a"), None);
        assert_eq!(s.pick_at(1, "a", Some(800.0)).as_deref(), Some("hedge:0"));
        // At 400 B/s it needs 3 s: over half of 4 s.
        assert_eq!(s.pick_at(1, "a", Some(400.0)), None);
    }

    #[test]
    fn a_stream_of_unknown_speed_races_a_block_needing_hedge_after_ms_more() {
        assert_eq!(laggard(160.0).pick(1, "a").as_deref(), Some("hedge:0")); // exactly 5 s
        assert_eq!(laggard(200.0).pick(1, "a"), None);
    }

    #[test]
    fn a_stream_never_races_its_own_block() {
        assert_eq!(laggard(0.0).pick(0, "a"), None);
    }

    #[test]
    fn a_block_whose_holder_let_go_goes_back_to_the_queue_not_raced() {
        let mut s = laggard(0.0).attempts(0, vec![hedge(1, "b")]);
        s.blocks[0] = block(0, BlockStatus::Pending, 200);
        assert_eq!(s.pick(2, "a").as_deref(), Some("primary:0"));
        // Even if its status still says downloading, a hedge alone is not raced.
        s.blocks[0].status = Downloading;
        assert_eq!(s.pick(2, "a"), None);
    }

    #[test]
    fn the_block_with_the_longest_way_to_go_is_raced_first() {
        let s = State::new(
            vec![busy(0, 200), busy(1, 200)],
            vec![fetching(0, "a", 100.0), fetching(1, "a", 20.0), free(2, "a")],
        )
        .attempts(0, vec![primary(0, "a")])
        .attempts(1, vec![primary(1, "a")]);
        assert_eq!(s.pick(2, "a").as_deref(), Some("hedge:1"));
    }

    #[test]
    fn a_network_that_got_nothing_from_a_block_is_not_sent_to_race_it() {
        assert_eq!(laggard(0.0).avoid(0, "b").pick(2, "b"), None);
    }

    #[test]
    fn another_network_gets_the_first_go() {
        let mut s = laggard(0.0);
        s.streams = vec![fetching(0, "a", 0.0), free(1, "a"), free(2, "b")];
        assert_eq!(s.pick(1, "a"), None, "b is free, and would take it");
        assert_eq!(s.pick(2, "b").as_deref(), Some("hedge:0"));
    }

    #[test]
    fn unless_the_requester_is_proven_clearly_faster() {
        let mut s = laggard(0.0);
        s.streams = vec![fetching(0, "a", 0.0), free(1, "a"), free(2, "b")];
        assert_eq!(s.pick_at(1, "a", Some(1_000_000.0)).as_deref(), Some("hedge:0"));
    }

    #[test]
    fn unless_that_network_would_decline_or_none_is_free() {
        let mut s = laggard(0.0);
        s.streams = vec![fetching(0, "a", 0.0), free(1, "a"), free(2, "b")];
        assert_eq!(s.clone().avoid(0, "b").pick(1, "a").as_deref(), Some("hedge:0"));
        s.streams[2] = fetching(2, "b", 0.0);
        assert_eq!(s.pick(1, "a").as_deref(), Some("hedge:0"));
    }

    #[test]
    fn a_block_of_unknown_length_is_never_raced() {
        let mut s = laggard(0.0);
        s.blocks =
            vec![BlockState { status: Downloading, ..BlockState::new(0, ByteRange::new(0, None)) }];
        assert_eq!(s.pick(1, "a"), None);
    }

    #[test]
    fn no_racing_while_the_disk_is_behind() {
        let mut s = State::new(vec![busy(0, 0)], vec![fetching(0, "a", 0.0), free(1, "b")])
            .attempts(0, vec![primary(0, "a")]);
        assert_eq!(s.pick(1, "b").as_deref(), Some("hedge:0"));
        s.disk_behind = true;
        assert_eq!(s.pick(1, "b"), None);
    }

    // ----- properties -----

    fn block_status() -> impl Strategy<Value = BlockStatus> {
        prop_oneof![Just(BlockStatus::Pending), Just(Downloading), Just(BlockStatus::Completed)]
    }
    fn stream_status() -> impl Strategy<Value = StreamStatus> {
        prop_oneof![
            Just(StreamStatus::Pending),
            Just(StreamStatus::Downloading),
            Just(StreamStatus::Retrying),
            Just(StreamStatus::Paused),
        ]
    }
    const NETWORKS: [&str; 3] = ["a", "b", "c"];

    proptest! {
        /// With a block waiting and a stream idle, some idle stream takes it.
        #[test]
        fn never_a_deadlock(
            statuses in proptest::collection::vec(block_status(), 1..8),
            streams in proptest::collection::vec((0usize..3, stream_status()), 1..8),
            avoided in proptest::collection::vec((0u32..8, 0usize..3), 0..8),
        ) {
            let blocks = statuses.iter().enumerate().map(|(i, s)| block(i as u32, *s, 0)).collect();
            let streams = streams
                .iter()
                .enumerate()
                .map(|(id, (n, status))| stream(id as u32, NETWORKS[*n], *status, 0.0))
                .collect();
            let mut s = State::new(blocks, streams);
            for (index, n) in avoided {
                s = s.avoid(index, NETWORKS[n]);
            }
            let idle: Vec<_> = s.streams.iter().filter(|c| is_free(c)).collect();
            prop_assume!(!idle.is_empty() && s.blocks.iter().any(|b| b.status == BlockStatus::Pending));
            let taken = idle.iter().any(|c| {
                s.pick(c.id, c.network_id.as_str()).is_some_and(|w| w.starts_with("primary"))
            });
            prop_assert!(taken);
        }

        /// Every free stream asks in turn and gets what it is given: no block ever has two
        /// primaries, nothing is hedged while a block waits, and each hedge is at a block a
        /// primary is still fetching.
        #[test]
        fn work_handed_out_is_always_real_and_never_doubled(
            rows in proptest::collection::vec(
                (block_status(), 0..=LENGTH, 0usize..2, 0..20_000i64, 0.0..500.0f64, any::<bool>()),
                1..6,
            ),
            askers in proptest::collection::vec((0usize..2, proptest::option::of(1.0..2000.0f64)), 1..10),
            disk_behind in any::<bool>(),
        ) {
            let mut s = State { disk_behind, ..Default::default() };
            for (i, (status, bytes, n, age, speed, hedged)) in rows.iter().enumerate() {
                let i = i as u32;
                s.blocks.push(block(i, *status, *bytes));
                s.streams.push(fetching(i, NETWORKS[*n], *speed));
                if *status == Downloading {
                    let mut held = vec![attempt(i, NETWORKS[*n], AttemptKind::Primary, *age)];
                    if *hedged {
                        held.push(hedge(100 + i, "b"));
                    }
                    s.attempts.insert(i, held);
                }
            }
            let first_asker = 1000;
            for (k, (n, _)) in askers.iter().enumerate() {
                s.streams.push(free(first_asker + k as u32, NETWORKS[*n]));
            }
            for (k, (n, speed)) in askers.iter().enumerate() {
                let id = first_asker + k as u32;
                let network = NetworkId::new(NETWORKS[*n]);
                let who = Requester { stream: id, network: &network, speed_bps: *speed };
                let Some(work) = pick_work(&s.view(), &who, NOW, &POLICY) else { continue };
                let index = work.block as usize;
                match work.kind {
                    AttemptKind::Primary => {
                        prop_assert_eq!(s.blocks[index].status, BlockStatus::Pending);
                        s.blocks[index].status = Downloading;
                    }
                    AttemptKind::Hedge => {
                        prop_assert!(!s.disk_behind);
                        prop_assert!(s.blocks.iter().all(|b| b.status != BlockStatus::Pending));
                        prop_assert_eq!(s.blocks[index].status, Downloading);
                        let held = &s.attempts[&work.block];
                        prop_assert!(held.iter().any(|a| a.kind == AttemptKind::Primary));
                        prop_assert!(held.iter().all(|a| a.stream != id));
                        *s.hedges_used.entry(work.block).or_insert(0) += 1;
                        prop_assert!(s.hedges_used[&work.block] <= POLICY.max_hedges_per_block);
                    }
                }
                let kind = work.kind;
                s.attempts
                    .entry(work.block)
                    .or_default()
                    .push(attempt(id, NETWORKS[*n], kind, 0));
                s.streams.iter_mut().filter(|c| c.id == id).for_each(|c| {
                    c.status = StreamStatus::Downloading;
                });
                let primaries = s.attempts[&work.block]
                    .iter()
                    .filter(|a| a.kind == AttemptKind::Primary)
                    .count();
                prop_assert!(primaries <= 1, "block {} has {} primaries", work.block, primaries);
            }
        }
    }
}
