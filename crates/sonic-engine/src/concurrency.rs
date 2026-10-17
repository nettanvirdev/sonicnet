//! How many streams each network runs. Deliberately simple, after Gopeed and aria2: no timers of
//! its own, and speeds only to judge whether growing paid (see below), averaged over seconds and
//! with a margin, so noise on a flaky network can't make it flap.
//!
//! - A network starts with [`START_STREAMS_PER_NETWORK`](sonic_core::START_STREAMS_PER_NETWORK)
//!   (see [`starting_streams`](crate::plan::starting_streams)), or with the count the user picked,
//!   which it then keeps rather than grows. Speeds are only read to decide growth; ceilings and
//!   the disk's cap never look at them.
//! - Once every one of its streams has received data, it may double, up to the limit — and keeps
//!   the doubling only if it pays. A server that caps each connection's speed is only outrun by
//!   more of them; but where the network's own link is what's full, more connections add nothing
//!   but requests (a server allowing an address 120 a minute stops answering it, then blocks it
//!   for most of an hour) and connections crawling among dozens of others at the end. So a
//!   network's speed is measured at its count once settled ([`SETTLE_MS`], then
//!   [`MEASURE_MS`]), the count doubled, and the speed measured again the same way: unless it
//!   rose by [`GROWTH_GAIN`], the doubling is undone (the newest streams retiring; not near the
//!   end, when they would only drop what they are fetching) and the network isn't grown again for [`REGROW_MS`], or until its speed falls well below what it was
//!   ([`REGROW_BELOW`]). Only a network's own speed is compared, before and after: never one
//!   network with another, and never while a server refuses or the disk holds it back.
//! - A server that refuses some of a network's streams (403/429/503, or silence) *while it is
//!   sending data down others* is saying it wants fewer connections from this address. It turned
//!   a request away while so many of the network's streams were asking at once, so it serves
//!   fewer than that: the network is cut to one fewer than were asking, the refused streams
//!   closing first. After [`RECOVER_MS`] without another refusal it may have one more, and so on
//!   back up to the limit: a server that was only briefly unwell doesn't cap the rest of a long
//!   download, and one that meant it costs one refused request a minute. Only refusals count; a
//!   stream that failed some other way, or hasn't been answered yet, isn't one.
//!
//!   Not "the streams it didn't refuse": that undercounts what a server serves whenever it turns
//!   away more than the excess, and a server that refuses *every* request while one address has
//!   too many connections open (HAProxy's usual per-client limit) does exactly that — it refuses
//!   streams it was serving a moment ago, and retrying streams that only collided with each
//!   other. A server allowing 2 was left at 1 that way, and only got its second back a minute
//!   later. Measured against the streams asking, a refusal never cuts below what the server
//!   serves: it can only overshoot, and the next refusal, among fewer, cuts again.
//! - A server that refuses while sending nothing isn't limiting connections: it is busy, or the
//!   link has expired. That is waited out or reported elsewhere, and the count stays. The network
//!   doesn't grow meanwhile.
//! - A disk that can't keep up holds each stream's reading until its writes catch up. Streams
//!   writing to scattered places are what slow a hard drive down, so once the disk has been
//!   behind for [`DISK_PATIENCE_MS`] no network grows and every network is cut to half the most
//!   any runs; each [`DISK_RECOVER_MS`] it keeps up at that cap, the cap doubles. A hard drive
//!   behind at every count is down to one stream within seconds, and a disk that was busy for a
//!   moment soon has its streams back. The disk is judged for the whole download, not per
//!   network: only a fast stream fills its writer, so per network the fast one alone would be cut
//!   while the slow one kept writing all over the disk. The disk's cap sits beside a server's
//!   ceiling and each network runs the lower. The user's own pick is left alone by the disk.
//! - Downloads running at once share a network's limit evenly: four downloads on one network run
//!   32 streams between them, not 128. A download starting lowers the others' share and their
//!   extra streams retire; one ending raises it and they grow back.
//! - Otherwise each network is decided on its own, so one that drops out or comes back never
//!   disturbs the others.

use std::collections::HashMap;

use sonic_core::{MAX_STREAMS_PER_NETWORK, Millis, NetworkId};

/// How long a lowered limit holds before it may rise by one.
pub const RECOVER_MS: Millis = 60_000;
/// How long a network runs a count, every stream answered, before its speed there counts: new
/// connections are still in slow start for a second or so after their first bytes, and the speed
/// meter reads over the last 3 s (`meter::SPEED_WINDOW_MS`), which must be past that. Measured
/// from 3 s on, the starting count read low on a 200 ms route — still ramping up — and a doubling
/// that gained nothing looked like one that paid.
pub const SETTLE_MS: Millis = 4_000;
/// How long the speed at a count is averaged over, once settled: a whole meter window, so one
/// burst or lull doesn't decide it.
pub const MEASURE_MS: Millis = 3_000;
/// How much faster a doubling must make a network for it to stay. Speeds wander by a few percent
/// from one few seconds to the next; doubling the connections on a link that was already full
/// gains nothing, while against a server capping each connection it gains up to 100 %.
pub const GROWTH_GAIN: f64 = 0.15;
/// How long a network whose doubling didn't pay is left at its count before trying again: what
/// limits it (its link, usually) rarely changes sooner, and every try costs requests.
pub const REGROW_MS: Millis = 60_000;
/// A network held at its count tries growing again sooner if its speed falls below this part of
/// what it was when the doubling failed: something changed (other traffic took the link, a
/// route moved), and more connections may help again.
pub const REGROW_BELOW: f64 = 0.6;
/// How long the disk must stay behind before streams are cut.
pub const DISK_PATIENCE_MS: Millis = 500;
/// How long the disk must keep up before the cap it set rises. Long against
/// [`DISK_PATIENCE_MS`], so a count the disk fell behind at isn't tried again until it has shown
/// it keeps up; short enough that a disk busy for a moment has its streams back within seconds.
pub const DISK_RECOVER_MS: Millis = 10_000;
/// No write landed for this long: the disk has stopped rather than slowed down. A working hard
/// drive lands a writer's worth in tens of milliseconds.
pub const DISK_STALLED_MS: Millis = 500;

/// One network as the controller sees it on one look.
#[derive(Clone, Debug, PartialEq)]
pub struct NetworkSnapshot {
    pub id: NetworkId,
    /// Its streams, not counting any retiring.
    pub streams: u32,
    /// Its streams that have received data.
    pub answered: u32,
    /// Its streams the server refused a request from since the last look.
    pub refused: u32,
    /// Of those refusals, the one that came while the fewest of its streams were asking the
    /// server for something (a request out, or about to be: not waiting out a failure),
    /// the refused one included: the server serves fewer than this. Unused when `refused` is 0.
    pub refused_among: u32,
    /// Its streams that received data since the last look.
    pub served: u32,
    /// Running downloads on Auto that have this network on, this one included: how many share
    /// its streams. 0 is taken as 1.
    pub downloads_on: u32,
    /// What its own meter reads now, in bytes a second: whether growing paid.
    pub speed_bps: f64,
}

/// What [`DiskWatch`] reads, and since when without a break.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiskSnapshot {
    pub reading: DiskReading,
    pub since: Millis,
}

/// One look at a download.
#[derive(Clone, Debug, PartialEq)]
pub struct Snapshot {
    pub now: Millis,
    /// The networks in use: the ones it may grow.
    pub networks: Vec<NetworkSnapshot>,
    /// How many more streams the waiting blocks could keep busy.
    pub spare_work: u32,
    /// `None`: the disk isn't judged.
    pub disk: Option<DiskSnapshot>,
}

/// A change to one network's stream count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Add { network: NetworkId, count: u32 },
    Retire { network: NetworkId, count: u32 },
}

/// - `Behind`: most of the download's writing streams hold bytes the disk hasn't taken, while
///   writes land.
/// - `KeepingUp`: most don't.
/// - `Unknown`: no stream writing, or held while nothing lands: a disk that has stopped altogether
///   (another program, a drive waking up) is waited out, as fewer streams wouldn't help it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DiskReading {
    Behind,
    KeepingUp,
    Unknown,
}

/// Whether the disk is what holds a download back, judged by the clock rather than by how often it
/// is looked at: a look between two others only extends a reading or breaks it.
#[derive(Clone, Debug)]
pub struct DiskWatch {
    reading: DiskReading,
    since: Millis,
    landed_at: Option<Millis>,
}

impl Default for DiskWatch {
    fn default() -> Self {
        Self { reading: DiskReading::Unknown, since: 0, landed_at: None }
    }
}

impl DiskWatch {
    pub fn new() -> Self {
        Self::default()
    }

    /// A write reached the disk.
    pub fn landed(&mut self, now: Millis) {
        self.landed_at = Some(now);
    }

    /// One look at the download: `writing` streams are fetching a block, `held` of them hold bytes
    /// the disk hasn't taken. A stream between blocks, or waiting to retry, says nothing either way.
    pub fn sample(&mut self, now: Millis, writing: u32, held: u32) {
        let landing = self.landed_at.is_some_and(|at| now - at < DISK_STALLED_MS);
        let reading = if writing == 0 {
            DiskReading::Unknown
        } else if u64::from(held) * 2 <= u64::from(writing) {
            DiskReading::KeepingUp
        } else if landing {
            DiskReading::Behind
        } else {
            DiskReading::Unknown
        };
        if reading != self.reading {
            self.reading = reading;
            self.since = now;
        }
    }

    pub fn reading(&self) -> DiskReading {
        self.reading
    }

    /// For [`Snapshot::disk`].
    pub fn snapshot(&self) -> DiskSnapshot {
        DiskSnapshot { reading: self.reading, since: self.since }
    }

    /// How long it has read `reading` without a break; 0 if it doesn't now.
    pub fn lasted(&self, reading: DiskReading, now: Millis) -> Millis {
        if self.reading == reading { now - self.since } else { 0 }
    }

    /// Whether to tell the user the disk is what limits the download (`DownloadState::disk_limited`),
    /// given whether they are told so now. Said as soon as Auto would act on it, and kept until the
    /// disk has kept up for as long as Auto waits to add streams back, so it doesn't flicker. A
    /// download with nothing writing is waiting on its networks, not the drive.
    pub fn limited(&self, now: Millis, writing: u32, limited_now: bool) -> bool {
        writing > 0
            && (self.lasted(DiskReading::Behind, now) >= DISK_PATIENCE_MS
                || (limited_now && self.lasted(DiskReading::KeepingUp, now) < DISK_RECOVER_MS))
    }

    /// Forgets everything, e.g. after the computer wakes: judgements by the clock start over.
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

/// A server's ceiling on one network, and when it last moved.
#[derive(Clone, Copy, Debug)]
struct Ceiling {
    limit: u32,
    since: Millis,
}

/// Whether growing a network (on Auto) pays: the count it runs, and how that is being judged.
#[derive(Clone, Debug)]
struct Growth {
    /// The streams it runs, below any ceiling, the disk's cap and its share.
    target: u32,
    /// Since when it has run `target` streams in a steady state (every one answered, nothing
    /// refused, the disk keeping up): its speed counts from [`SETTLE_MS`] after.
    settled_from: Option<Millis>,
    /// From when the current measurement runs, and the speeds it has taken.
    window_from: Millis,
    sum: f64,
    samples: u32,
    /// A doubling being judged: the count before it, and the speed there.
    trial: Option<(u32, f64)>,
    /// Not grown again until then, unless its speed falls below [`REGROW_BELOW`] of the second.
    hold: Option<(Millis, f64)>,
}

impl Growth {
    fn new(target: u32) -> Self {
        Self {
            target,
            settled_from: None,
            window_from: 0,
            sum: 0.0,
            samples: 0,
            trial: None,
            hold: None,
        }
    }

    fn unsettle(&mut self) {
        self.settled_from = None;
        self.window_from = 0;
        self.sum = 0.0;
        self.samples = 0;
    }

    /// One look at the network running `target` streams in a steady state, at `speed`: its
    /// average speed over the last [`MEASURE_MS`], each time that long has been measured.
    fn measure(&mut self, now: Millis, speed: f64) -> Option<f64> {
        let from = *self.settled_from.get_or_insert(now);
        if now - from < SETTLE_MS {
            return None;
        }
        // The first window starts with its first look; each later one where the last ended.
        if self.samples == 0 && self.window_from < from + SETTLE_MS {
            self.window_from = now;
        }
        self.sum += speed;
        self.samples += 1;
        if now - self.window_from < MEASURE_MS {
            return None;
        }
        let average = self.sum / f64::from(self.samples);
        self.window_from = now;
        self.sum = 0.0;
        self.samples = 0;
        Some(average)
    }
}

/// Sizes each network's streams for one download; see the module docs for the rules.
#[derive(Clone, Debug)]
pub struct ConcurrencyController {
    /// The most streams a network may run.
    max_per_network: u32,
    /// False: the user picked a count, which is kept rather than grown to and isn't shared.
    grows: bool,
    /// Networks a server has refused on.
    ceilings: HashMap<NetworkId, Ceiling>,
    /// On Auto: each network's count, and whether growing it pays.
    growth: HashMap<NetworkId, Growth>,
    /// The most streams any network may run while the disk can't keep up (`None`: no cap).
    disk_cap: Option<u32>,
    /// From when what the disk does counts toward moving its cap: when the cap last moved, or the
    /// streams last ran below it.
    disk_from: Millis,
}

impl ConcurrencyController {
    /// Auto: start at 8, double to [`MAX_STREAMS_PER_NETWORK`], share with other downloads.
    pub fn auto() -> Self {
        Self::new(MAX_STREAMS_PER_NETWORK, true)
    }

    /// The count the user picked: kept, never grown, only lowered by a refusing server.
    pub fn fixed(picked: u32) -> Self {
        Self::new(picked.clamp(1, MAX_STREAMS_PER_NETWORK), false)
    }

    fn new(max_per_network: u32, grows: bool) -> Self {
        Self {
            max_per_network,
            grows,
            ceilings: HashMap::new(),
            growth: HashMap::new(),
            disk_cap: None,
            disk_from: 0,
        }
    }

    /// One look: what to add and retire on each network.
    pub fn tick(&mut self, snapshot: &Snapshot) -> Vec<Action> {
        let now = snapshot.now;
        let mut spare = snapshot.spare_work;
        if self.grows {
            self.judge_disk(snapshot);
        }
        let disk_holds =
            self.grows && snapshot.disk.is_some_and(|disk| disk.reading != DiskReading::KeepingUp);

        let mut actions = Vec::new();
        for network in &snapshot.networks {
            let streams = network.streams;
            self.judge_server(network, now);
            let ceiling = self.limit(&network.id, network.downloads_on);
            // Every stream answered, nothing refused, the disk keeping up: only then may it
            // grow, and only then is its speed taken for what its count does.
            let steady =
                network.refused == 0 && !disk_holds && streams > 0 && network.answered >= streams;
            let target = if self.grows {
                self.judge_growth(network, now, ceiling, spare, steady)
            } else {
                ceiling
            };
            let want = target.min(ceiling);
            if streams > want {
                actions.push(Action::Retire { network: network.id.clone(), count: streams - want });
                continue;
            }
            if !steady {
                continue;
            }
            let count = (want - streams).min(spare);
            if count == 0 {
                continue;
            }
            spare -= count;
            actions.push(Action::Add { network: network.id.clone(), count });
        }
        actions
    }

    /// The streams `network` should run on Auto, before `ceiling`: its count, doubled for as long
    /// as doubling pays (see the module docs).
    fn judge_growth(
        &mut self,
        network: &NetworkSnapshot,
        now: Millis,
        ceiling: u32,
        spare: u32,
        steady: bool,
    ) -> u32 {
        let streams = network.streams;
        let growth =
            self.growth.entry(network.id.clone()).or_insert_with(|| Growth::new(streams.max(1)));
        if !steady || streams != growth.target.min(ceiling) {
            growth.unsettle();
            return growth.target;
        }
        let Some(speed) = growth.measure(now, network.speed_bps) else { return growth.target };
        if let Some((before, before_bps)) = growth.trial.take()
            && speed < before_bps * (1.0 + GROWTH_GAIN)
        {
            // It didn't pay: back to the count before, and left there a while. Unless the
            // download is nearly done (less work waiting than the streams it would retire):
            // they would drop what they are fetching for others to ask for again, saving nothing.
            if spare >= growth.target - before {
                growth.target = before;
            }
            growth.hold = Some((now + REGROW_MS, before_bps.max(speed)));
            growth.unsettle();
            return growth.target;
        }
        let held =
            growth.hold.is_some_and(|(until, was)| now < until && speed >= was * REGROW_BELOW);
        if held {
            return growth.target;
        }
        let doubled = (growth.target * 2).min(ceiling).min(growth.target + spare);
        if doubled > growth.target {
            growth.trial = Some((growth.target, speed));
            growth.target = doubled;
            growth.hold = None;
            growth.unsettle();
        }
        growth.target
    }

    /// The most streams a network may run now: the lowest of a server's ceiling, the disk's cap
    /// and this download's share of the network, never more than the limit. Also the most a
    /// network joining the download starts with.
    pub fn limit(&self, network: &NetworkId, downloads_on: u32) -> u32 {
        let share = if self.grows { self.max_per_network / downloads_on.max(1) } else { u32::MAX };
        let ceiling = self.ceilings.get(network).map_or(u32::MAX, |c| c.limit);
        self.max_per_network.min(share).min(ceiling).min(self.disk_cap.unwrap_or(u32::MAX))
    }

    /// Moves a server's ceiling on this network (see the module docs).
    fn judge_server(&mut self, network: &NetworkSnapshot, now: Millis) {
        let refused_now = network.refused > 0 && network.served > 0;
        if refused_now {
            let fewer = network.refused_among.saturating_sub(1);
            let limit = self.ceilings.get(&network.id).map_or(fewer, |c| fewer.min(c.limit)).max(1);
            self.ceilings.insert(network.id.clone(), Ceiling { limit, since: now });
            return;
        }
        let Some(ceiling) = self.ceilings.get_mut(&network.id) else { return };
        if network.refused == 0 && now - ceiling.since >= RECOVER_MS {
            ceiling.limit += 1;
            ceiling.since = now;
            if ceiling.limit >= self.max_per_network {
                // Back at the maximum: nothing left to remember.
                self.ceilings.remove(&network.id);
            }
        }
    }

    /// Moves the disk's cap (see the module docs).
    fn judge_disk(&mut self, snapshot: &Snapshot) {
        let Some(disk) = snapshot.disk else { return };
        let now = snapshot.now;
        let streams = snapshot.networks.iter().map(|n| n.streams).max().unwrap_or(0);
        // Only what was seen since the cap last moved says anything about the count it allows.
        let lasted = now - disk.since.max(self.disk_from);
        match (disk.reading, self.disk_cap) {
            (DiskReading::Behind, _) if lasted >= DISK_PATIENCE_MS && streams > 1 => {
                self.disk_cap = Some(streams.div_ceil(2));
            }
            // Keeping up below the cap (a server's ceiling, or too few blocks left) says nothing
            // about it: the wait starts over once the streams are back at it.
            (DiskReading::KeepingUp, Some(cap)) if streams < cap => {}
            (DiskReading::KeepingUp, Some(cap)) if lasted >= DISK_RECOVER_MS => {
                self.disk_cap = (cap * 2 < self.max_per_network).then_some(cap * 2);
            }
            _ => return,
        }
        self.disk_from = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX: u32 = MAX_STREAMS_PER_NETWORK;
    const LOOK_MS: Millis = 500;

    fn net(id: &str, streams: u32) -> NetworkSnapshot {
        NetworkSnapshot {
            id: NetworkId::new(id),
            streams,
            answered: streams,
            refused: 0,
            refused_among: 0,
            served: streams,
            downloads_on: 1,
            speed_bps: 0.0,
        }
    }
    /// `refused` turned away, each as the one asking beside the `served` the server was serving.
    fn refused(id: &str, streams: u32, served: u32, refused: u32) -> NetworkSnapshot {
        NetworkSnapshot { served, refused, refused_among: served + 1, ..net(id, streams) }
    }
    fn look(now: Millis, networks: Vec<NetworkSnapshot>) -> Snapshot {
        Snapshot { now, networks, spare_work: 1000, disk: None }
    }
    fn add(id: &str, count: u32) -> Action {
        Action::Add { network: NetworkId::new(id), count }
    }
    fn retire(id: &str, count: u32) -> Action {
        Action::Retire { network: NetworkId::new(id), count }
    }

    /// How long after a count settles its speed is known.
    const JUDGED: Millis = SETTLE_MS + MEASURE_MS;

    /// Looks at network `a` every [`LOOK_MS`] from `from` until `until`, its speed
    /// `speed(streams, now)`, doing what the controller says. The count after each look.
    fn drive(
        controller: &mut ConcurrencyController,
        streams: &mut u32,
        from: Millis,
        until: Millis,
        speed: impl Fn(u32, Millis) -> f64,
    ) -> Vec<u32> {
        let mut counts = Vec::new();
        for now in (from..until).step_by(LOOK_MS as usize) {
            let a = NetworkSnapshot { speed_bps: speed(*streams, now), ..net("a", *streams) };
            for action in controller.tick(&look(now, vec![a])) {
                match action {
                    Action::Add { count, .. } => *streams += count,
                    Action::Retire { count, .. } => *streams -= count,
                }
            }
            counts.push(*streams);
        }
        counts
    }

    /// A server capping each connection: every stream adds its 100 kB/s.
    fn capped_per_connection(streams: u32, _: Millis) -> f64 {
        f64::from(streams) * 100_000.0
    }

    /// A full link: 10 MB/s whatever the count.
    fn full_link(_: u32, _: Millis) -> f64 {
        10_000_000.0
    }

    #[test]
    fn doubles_while_doubling_pays_up_to_the_limit() {
        let mut controller = ConcurrencyController::auto();
        let mut streams = 8;
        let counts = drive(&mut controller, &mut streams, 0, 4 * JUDGED, capped_per_connection);
        // Each count is judged once settled, so no doubling comes sooner than that.
        let first = counts.iter().position(|c| *c == 16).unwrap() as Millis * LOOK_MS;
        assert!(first >= JUDGED, "doubled after {first} ms");
        assert_eq!(counts.last(), Some(&MAX), "{counts:?}");
    }

    #[test]
    fn a_doubling_that_does_not_pay_is_undone_and_held() {
        let mut controller = ConcurrencyController::auto();
        let mut streams = 8;
        let counts = drive(&mut controller, &mut streams, 0, REGROW_MS, full_link);
        let tried = counts.iter().position(|c| *c == 16).expect("it tried");
        let undone = tried + counts[tried..].iter().position(|c| *c == 8).expect("undone");
        // Judged as long as the count before it, then left alone for the rest of the minute.
        assert!((undone - tried) as Millis * LOOK_MS <= JUDGED + LOOK_MS);
        assert!(counts[undone..].iter().all(|c| *c == 8), "{counts:?}");
        // A minute on, it tries again.
        let later =
            drive(&mut controller, &mut streams, REGROW_MS, REGROW_MS + 3 * JUDGED, full_link);
        assert!(later.contains(&16), "{later:?}");
    }

    #[test]
    fn near_the_end_a_doubling_that_did_not_pay_is_not_undone() {
        let mut controller = ConcurrencyController::auto();
        let mut streams = 8;
        drive(&mut controller, &mut streams, 0, JUDGED + LOOK_MS, full_link);
        assert_eq!(streams, 16, "tried");
        // Nothing left waiting by the time it is judged.
        for now in (JUDGED + LOOK_MS..3 * JUDGED).step_by(LOOK_MS as usize) {
            let a = NetworkSnapshot { speed_bps: full_link(16, now), ..net("a", 16) };
            let snapshot = Snapshot { spare_work: 0, ..look(now, vec![a]) };
            assert_eq!(controller.tick(&snapshot), [], "at {now}");
        }
    }

    #[test]
    fn noise_does_not_make_it_flap() {
        let mut controller = ConcurrencyController::auto();
        let mut streams = 8;
        // 10 MB/s, 10 % up or down from one look to the next.
        let noisy = |_: u32, now: Millis| if (now / LOOK_MS) % 2 == 0 { 11e6 } else { 9e6 };
        let counts = drive(&mut controller, &mut streams, 0, REGROW_MS, noisy);
        let changes = counts.windows(2).filter(|w| w[0] != w[1]).count();
        assert!(changes <= 2, "one try and its undoing at most: {counts:?}");
        assert_eq!(counts.last(), Some(&8));
    }

    #[test]
    fn a_held_network_whose_speed_falls_tries_again_sooner() {
        let mut controller = ConcurrencyController::auto();
        let mut streams = 8;
        drive(&mut controller, &mut streams, 0, 3 * JUDGED, full_link);
        assert_eq!(streams, 8, "held");
        let fallen = |_: u32, _: Millis| 4e6;
        let counts = drive(&mut controller, &mut streams, 3 * JUDGED, 6 * JUDGED, fallen);
        assert!(counts.contains(&16), "{counts:?}");
    }

    #[test]
    fn grows_no_more_than_waiting_blocks_can_keep_busy() {
        let mut controller = ConcurrencyController::auto();
        let snapshot = |now, networks, spare_work| Snapshot { spare_work, ..look(now, networks) };
        let speedy = |id, streams| NetworkSnapshot {
            speed_bps: capped_per_connection(streams, 0),
            ..net(id, streams)
        };
        for now in (0..JUDGED).step_by(LOOK_MS as usize) {
            assert_eq!(controller.tick(&snapshot(now, vec![speedy("a", 8)], 3)), []);
        }
        assert_eq!(controller.tick(&snapshot(JUDGED, vec![speedy("a", 8)], 3)), [add("a", 3)]);
        // Shared: the first network takes what it can use.
        let mut controller = ConcurrencyController::auto();
        for now in (0..JUDGED).step_by(LOOK_MS as usize) {
            controller.tick(&snapshot(now, vec![speedy("a", 8), speedy("b", 8)], 10));
        }
        assert_eq!(
            controller.tick(&snapshot(JUDGED, vec![speedy("a", 8), speedy("b", 8)], 10)),
            [add("a", 8), add("b", 2)]
        );
    }

    #[test]
    fn refusing_while_sending_nothing_is_not_a_limit() {
        let mut controller = ConcurrencyController::auto();
        for count in [16, 5, 11] {
            assert_eq!(controller.tick(&look(0, vec![refused("a", 16, 0, count)])), []);
        }
        let mut streams = 16;
        let counts = drive(&mut controller, &mut streams, 0, 2 * JUDGED, capped_per_connection);
        assert_eq!(counts.last(), Some(&32));
    }

    #[test]
    fn each_network_is_decided_on_its_own() {
        let mut controller = ConcurrencyController::auto();
        let a = NetworkSnapshot { answered: 7, ..net("a", 8) };
        assert_eq!(controller.tick(&look(0, vec![a, refused("b", 16, 8, 8)])), [retire("b", 8)]);
        let speedy = |id, streams| NetworkSnapshot {
            speed_bps: capped_per_connection(streams, 0),
            ..net(id, streams)
        };
        // Settled from the second look on.
        for now in (LOOK_MS..=LOOK_MS + JUDGED).step_by(LOOK_MS as usize) {
            let actions = controller.tick(&look(now, vec![speedy("a", 8), speedy("b", 8)]));
            if now < LOOK_MS + JUDGED {
                assert_eq!(actions, []);
            } else {
                // a doubles; b stays at the 8 its server allows.
                assert_eq!(actions, [add("a", 8)]);
            }
        }
    }

    #[test]
    fn waits_while_any_stream_has_yet_to_receive() {
        let mut controller = ConcurrencyController::auto();
        let a = NetworkSnapshot { answered: 7, ..net("a", 8) };
        assert_eq!(controller.tick(&look(0, vec![a])), []);
    }

    #[test]
    fn refused_streams_close_and_it_never_grows_past_the_ones_left() {
        let mut controller = ConcurrencyController::auto();
        assert_eq!(controller.tick(&look(0, vec![refused("a", 16, 10, 6)])), [retire("a", 6)]);
        assert_eq!(controller.tick(&look(0, vec![net("a", 10)])), []);
        // A stream lost some other way is made up for, back to the limit and no further.
        assert_eq!(controller.tick(&look(0, vec![net("a", 7)])), [add("a", 3)]);
    }

    #[test]
    fn a_refusal_cuts_to_one_fewer_than_were_asking_not_to_the_streams_it_spared() {
        let mut controller = ConcurrencyController::auto();
        // Eight asking at once and two served: it serves fewer than eight, which is all that
        // says. Cutting to the two it spared would be a guess, and a wrong one whenever a server
        // turns away more than the excess.
        let burst = NetworkSnapshot { refused_among: 8, ..refused("a", 8, 2, 6) };
        assert_eq!(controller.tick(&look(0, vec![burst])), [retire("a", 1)]);
        // A server refusing every request while too many are open (allowing 3): two retrying
        // streams collided beside the two it serves, and were both turned away among four.
        let collided = NetworkSnapshot { refused_among: 4, ..refused("a", 4, 2, 2) };
        assert_eq!(controller.tick(&look(0, vec![collided])), [retire("a", 1)]);
        assert_eq!(controller.tick(&look(0, vec![net("a", 3)])), []);
        // One it was serving turned away too (both served and refused since the last look) by
        // the same rule, among three: it serves two, not one.
        let collateral = NetworkSnapshot { refused_among: 3, ..refused("a", 3, 2, 2) };
        assert_eq!(controller.tick(&look(0, vec![collateral])), [retire("a", 1)]);
        assert_eq!(controller.tick(&look(0, vec![net("a", 1)])), [add("a", 1)]);
    }

    #[test]
    fn streams_yet_to_answer_are_not_taken_for_refused_ones() {
        let mut controller = ConcurrencyController::auto();
        let early = NetworkSnapshot { answered: 0, ..refused("a", 8, 0, 1) };
        assert_eq!(controller.tick(&look(0, vec![early])), []);
        assert_eq!(controller.tick(&look(0, vec![refused("a", 8, 7, 1)])), [retire("a", 1)]);
        assert_eq!(controller.tick(&look(0, vec![net("a", 7)])), []);
    }

    #[test]
    fn a_later_refusal_lowers_the_limit_further() {
        let mut controller = ConcurrencyController::auto();
        controller.tick(&look(0, vec![refused("a", 16, 10, 6)]));
        assert_eq!(controller.tick(&look(0, vec![refused("a", 10, 8, 2)])), [retire("a", 2)]);
        assert_eq!(controller.tick(&look(0, vec![net("a", 6)])), [add("a", 2)]);
    }

    #[test]
    fn a_lowered_limit_climbs_back_one_a_minute() {
        let mut controller = ConcurrencyController::auto();
        let mut tick = |now, network| controller.tick(&look(now, vec![network]));
        assert_eq!(tick(0, refused("a", 16, 10, 6)), [retire("a", 6)]);
        assert_eq!(tick(RECOVER_MS - 1, net("a", 10)), []);
        assert_eq!(tick(RECOVER_MS, net("a", 10)), [add("a", 1)]);
        assert_eq!(tick(RECOVER_MS + 1, net("a", 11)), []);
        assert_eq!(tick(2 * RECOVER_MS, net("a", 11)), [add("a", 1)]);
        // Refused again at 12: back to 11, and the minute starts over.
        assert_eq!(tick(2 * RECOVER_MS + 1, refused("a", 12, 11, 1)), [retire("a", 1)]);
        assert_eq!(tick(3 * RECOVER_MS, net("a", 11)), []);
        assert_eq!(tick(3 * RECOVER_MS + 1, net("a", 11)), [add("a", 1)]);
    }

    #[test]
    fn a_limit_recovered_all_the_way_is_forgotten() {
        let mut controller = ConcurrencyController::fixed(8);
        controller.tick(&look(0, vec![refused("a", 8, 7, 1)]));
        assert_eq!(controller.tick(&look(RECOVER_MS, vec![net("a", 7)])), [add("a", 1)]);
        assert_eq!(controller.tick(&look(RECOVER_MS + 1, vec![net("a", 8)])), []);
        assert!(controller.ceilings.is_empty());
    }

    #[test]
    fn downloads_at_once_share_a_network() {
        let mut controller = ConcurrencyController::auto();
        assert_eq!(controller.limit(&NetworkId::new("a"), 1), 32);
        let shared = |streams, downloads_on| NetworkSnapshot { downloads_on, ..net("a", streams) };
        assert_eq!(controller.tick(&look(0, vec![shared(32, 4)])), [retire("a", 24)]);
        assert_eq!(controller.tick(&look(0, vec![shared(8, 2)])), [add("a", 8)]);
    }

    #[test]
    fn a_picked_count_is_kept_and_still_backs_off_when_refused() {
        let mut controller = ConcurrencyController::fixed(4);
        assert_eq!(controller.tick(&look(0, vec![net("a", 4)])), []);
        assert_eq!(controller.tick(&look(0, vec![net("a", 3)])), [add("a", 1)]);
        assert_eq!(controller.tick(&look(0, vec![refused("a", 4, 3, 1)])), [retire("a", 1)]);
        assert_eq!(controller.tick(&look(RECOVER_MS, vec![net("a", 3)])), [add("a", 1)]);
        assert_eq!(controller.tick(&look(2 * RECOVER_MS, vec![net("a", 4)])), []);
        // Not shared with other downloads either.
        assert_eq!(controller.limit(&NetworkId::new("a"), 4), 4);
    }

    fn behind_since(since: Millis) -> Option<DiskSnapshot> {
        Some(DiskSnapshot { reading: DiskReading::Behind, since })
    }

    #[test]
    fn the_disk_is_judged_by_the_clock_not_by_looks() {
        let mut controller = ConcurrencyController::auto();
        let mut tick = |now, streams| {
            controller
                .tick(&Snapshot { disk: behind_since(0), ..look(now, vec![net("a", streams)]) })
        };
        assert_eq!(tick(0, 16), []);
        assert_eq!(tick(1, 16), []);
        assert_eq!(tick(DISK_PATIENCE_MS - 1, 16), []);
        assert_eq!(tick(DISK_PATIENCE_MS, 16), [retire("a", 8)]);
        // The next cut needs as long again at the new count.
        assert_eq!(tick(DISK_PATIENCE_MS + 1, 8), []);
        assert_eq!(tick(2 * DISK_PATIENCE_MS, 8), [retire("a", 4)]);
    }

    #[test]
    fn the_disk_cuts_every_network_together() {
        let mut controller = ConcurrencyController::auto();
        let snapshot = Snapshot {
            spare_work: 0,
            disk: behind_since(0),
            ..look(DISK_PATIENCE_MS, vec![net("a", 8), net("b", 8)])
        };
        assert_eq!(controller.tick(&snapshot), [retire("a", 4), retire("b", 4)]);
    }

    #[test]
    fn keeping_up_below_the_cap_says_nothing_about_it() {
        let mut controller = ConcurrencyController::auto();
        let mut tick = |now: Millis, streams, spare_work| {
            let disk = if now <= DISK_PATIENCE_MS {
                behind_since(0)
            } else {
                Some(DiskSnapshot { reading: DiskReading::KeepingUp, since: DISK_PATIENCE_MS })
            };
            controller.tick(&Snapshot { spare_work, disk, ..look(now, vec![net("a", streams)]) })
        };
        assert_eq!(tick(DISK_PATIENCE_MS, 32, 1000), [retire("a", 16)]);
        // Few blocks left keep it at 3 while the disk keeps up; then more are waiting.
        assert_eq!(tick(DISK_PATIENCE_MS + DISK_RECOVER_MS, 3, 0), []);
        assert_eq!(tick(DISK_PATIENCE_MS + DISK_RECOVER_MS + 1, 6, 1000), [add("a", 10)]);
        // Back at the cap, it has to keep up there before the cap rises.
        let back = DISK_PATIENCE_MS + DISK_RECOVER_MS + 2;
        assert_eq!(tick(back, 16, 1000), []);
        assert_eq!(tick(back + DISK_RECOVER_MS, 16, 1000), [add("a", 16)]);
    }

    /// One network, looked at every [`LOOK_MS`] from 0 to `ms` as the transfer does: all its
    /// streams are held while `behind`, and the server refuses `refused_at(now)` of them. The
    /// streams it ran after each look.
    fn run(
        mut controller: ConcurrencyController,
        mut streams: u32,
        ms: Millis,
        behind: impl Fn(Millis) -> bool,
        refused_at: impl Fn(Millis) -> u32,
    ) -> Vec<u32> {
        let mut watch = DiskWatch::new();
        let mut counts = Vec::new();
        for now in (0..=ms).step_by(LOOK_MS as usize) {
            watch.landed(now);
            watch.sample(now, streams, if behind(now) { streams } else { 0 });
            let refused_now = refused_at(now);
            let snapshot = Snapshot {
                disk: Some(watch.snapshot()),
                ..look(now, vec![refused("a", streams, streams - refused_now, refused_now)])
            };
            for action in controller.tick(&snapshot) {
                match action {
                    Action::Add { count, .. } => streams += count,
                    Action::Retire { count, .. } => streams -= count,
                }
            }
            counts.push(streams);
        }
        counts
    }

    fn index_of(counts: &[u32], value: u32, from: usize) -> usize {
        from + counts[from..].iter().position(|c| *c == value).expect("count reached")
    }

    #[test]
    fn a_disk_behind_at_every_count_is_left_with_one_stream() {
        let counts = run(ConcurrencyController::auto(), 32, 30_000, |_| true, |_| 0);
        let one = index_of(&counts, 1, 0);
        assert!(one as Millis * LOOK_MS <= 6 * DISK_PATIENCE_MS);
        assert!(counts[one..].iter().all(|c| *c == 1));
    }

    #[test]
    fn a_disk_busy_for_a_moment_soon_gives_the_streams_back() {
        let counts = run(ConcurrencyController::auto(), 32, 30_000, |now| now < 1_000, |_| 0);
        assert_eq!(counts.iter().min(), Some(&16));
        let back = index_of(&counts, 32, 1);
        assert!(back as Millis * LOOK_MS <= 2 * DISK_PATIENCE_MS + DISK_RECOVER_MS);
        assert_eq!(counts.last(), Some(&32));
    }

    #[test]
    fn a_disk_cut_leaves_a_server_ceiling_and_its_pace_alone() {
        let counts = run(
            ConcurrencyController::auto(),
            32,
            RECOVER_MS - LOOK_MS,
            |now| (500..2_000).contains(&now),
            |now| if now == 0 { 2 } else { 0 },
        );
        assert_eq!(counts.iter().min(), Some(&8));
        assert_eq!(counts.last(), Some(&30));
        assert_eq!(counts[1..].iter().max(), Some(&30));
    }

    #[test]
    fn a_refusal_at_the_moment_the_disk_cuts_still_counts() {
        let counts = run(
            ConcurrencyController::auto(),
            32,
            RECOVER_MS,
            |now| now < 2_000,
            |now| if now == DISK_PATIENCE_MS { 5 } else { 0 },
        );
        assert_eq!(counts.iter().min(), Some(&4));
        assert_eq!(counts[1..].iter().max(), Some(&27));
    }

    #[test]
    fn a_disk_behind_only_now_and_then_neither_cuts_nor_raises() {
        let counts = run(
            ConcurrencyController::auto(),
            32,
            60_000,
            |now| now <= DISK_PATIENCE_MS || (now / LOOK_MS) % 2 == 1,
            |_| 0,
        );
        let from = (DISK_PATIENCE_MS / LOOK_MS) as usize;
        assert!(counts[from..].iter().all(|c| *c == 16), "{counts:?}");
    }

    #[test]
    fn the_disk_leaves_a_picked_count_alone() {
        let counts = run(ConcurrencyController::fixed(4), 4, 30_000, |_| true, |_| 0);
        assert!(counts.iter().all(|c| *c == 4));
    }

    #[test]
    fn disk_watch_reads_the_download_as_a_whole_by_the_clock() {
        let mut watch = DiskWatch::new();
        watch.landed(0);
        watch.sample(0, 8, 5);
        assert_eq!(watch.reading(), DiskReading::Behind);
        watch.sample(400, 8, 6);
        assert_eq!(watch.lasted(DiskReading::Behind, 400), 400);
        // Held, but nothing has landed for a while: a stalled disk isn't judged.
        watch.sample(DISK_STALLED_MS, 8, 8);
        assert_eq!(watch.reading(), DiskReading::Unknown);
        // Half held is not most.
        watch.landed(2_000);
        watch.sample(2_000, 8, 4);
        assert_eq!(watch.reading(), DiskReading::KeepingUp);
        assert_eq!(watch.lasted(DiskReading::Behind, 2_000), 0);
        // Nothing writing says nothing.
        watch.sample(2_100, 0, 0);
        assert_eq!(watch.reading(), DiskReading::Unknown);
    }

    #[test]
    fn disk_watch_never_landed_is_not_behind() {
        let mut watch = DiskWatch::new();
        watch.sample(1_000_000, 8, 8);
        assert_eq!(watch.reading(), DiskReading::Unknown);
    }

    #[test]
    fn the_user_is_told_about_the_disk_without_flicker() {
        let mut watch = DiskWatch::new();
        watch.landed(0);
        watch.sample(0, 8, 8);
        assert!(!watch.limited(DISK_PATIENCE_MS - 1, 8, false));
        assert!(watch.limited(DISK_PATIENCE_MS, 8, false));
        watch.sample(1_000, 8, 0);
        assert!(watch.limited(1_000 + DISK_RECOVER_MS - 1, 8, true), "kept while recovering");
        assert!(!watch.limited(1_000 + DISK_RECOVER_MS, 8, true));
        assert!(!watch.limited(1_000, 0, true), "nothing writing: not the disk");
    }
}
