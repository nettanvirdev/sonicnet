//! Which network a new connection (or UDP flow) leaves through.
//!
//! Without a server on the far side one TCP connection can't be split across two ISPs, so System
//! mode balances whole connections. The rules, in order:
//!
//! 1. **Eligible networks only**: a network the user turned off, or one at its limit (data cap,
//!    connection cap), never gets a new connection.
//! 2. **Heavy destinations cover every usable network.** A destination whose connections received
//!    [`HEAVY_BYTES`] within [`HEAVY_WINDOW`] (counted live by the relays through
//!    [`Scheduler::meter_for`]), or that has [`BULK_OPEN`] connections open at once, is a bulk
//!    transfer: a download manager, a launcher, a video stream. Its next connections go first to
//!    every *usable* network (eligible, capacity at least [`FLOOR`] of the best) that holds none
//!    of its connections yet, fastest first, then by capacity among the usable ones. So FDM's four
//!    connections to one CDN IP get one on the 1 MB/s Wi-Fi beside three on the 11.5 MB/s
//!    Ethernet, where capacity weighting alone (92 : 8) would have put all four on Ethernet.
//!    Connections already open can't move; only new ones are placed this way, from the first one
//!    after the destination turns heavy.
//! 3. **Stickiness** (light destinations): a destination that went out through a network keeps
//!    using it while it keeps being used (a sliding TTL). Logins, banks and streaming sites tie
//!    sessions to the client's IP; seeing it flip between two ISPs mid-session logs people out or
//!    trips fraud checks. A sticky network much slower than the best (below [`COMPARABLE`] of
//!    it) doesn't hold its destinations: System mode must never make anyone slower than their
//!    best network.
//! 4. **Failover**: when the sticky network is no longer eligible (dropped, turned off, at its
//!    limit) the destination is re-placed and sticks to its new network. Connections already open
//!    on the dead network die with it; only new ones move.
//! 5. **Weighted choice**: otherwise a smooth weighted round robin by capacity, so an 11.5 MB/s
//!    link gets about thirteen times the new connections of a 0.9 MB/s one. Capacity is the
//!    caller's measurement (an active probe and observed peaks); an unmeasured network counts as
//!    [`UNMEASURED`] of the best measured one, a small share that gets it measured without
//!    betting on it. Deterministic (no randomness), which keeps it testable and stops bursts of
//!    connections from landing on one network by chance.
//!
//! Pure apart from the byte counters the relays fill: no clock, no I/O. The caller passes `now`
//! and the current list of networks.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use sonic_core::NetworkId;

use crate::relay::Meter;

/// One network as the scheduler sees it at the moment of a choice.
#[derive(Clone, Debug, PartialEq)]
pub struct Candidate {
    pub id: NetworkId,
    /// Measured capacity in bytes per second, `None` until there is a measurement. Networks
    /// without one count as the average of those with one (or all equal when none has one), so a
    /// fresh network gets a fair share instead of none or all.
    pub capacity: Option<f64>,
    /// The user has this network on for System mode.
    pub enabled: bool,
    /// The network hit a limit (data cap, connection cap) and must take no new connections.
    pub at_limit: bool,
}

impl Candidate {
    /// An enabled network with no measurement yet.
    pub fn new(id: NetworkId) -> Self {
        Self { id, capacity: None, enabled: true, at_limit: false }
    }

    fn eligible(&self) -> bool {
        self.enabled && !self.at_limit
    }
}

/// Picks networks for new connections; see the module docs for the rules.
#[derive(Debug)]
pub struct Scheduler {
    sticky_ttl: Duration,
    dests: HashMap<IpAddr, Dest>,
    open: HashMap<NetworkId, u32>,
    /// Each network's credit in the weighted round robin.
    credit: HashMap<NetworkId, f64>,
}

/// What the scheduler knows about one destination IP.
#[derive(Debug)]
struct Dest {
    /// The network it sticks to, fresh while `last_used` is within the sticky TTL.
    sticky: Option<NetworkId>,
    /// When its last connection was placed.
    last_used: Instant,
    /// Its open connections per network.
    open: HashMap<NetworkId, u32>,
    /// Bytes its connections moved, counted by the relays as they move.
    meter: Arc<Meter>,
    /// `meter.down` and the time when heaviness was last settled; see [`Dest::heavy`].
    mark: (Instant, u64),
    /// Heavy until then.
    heavy_until: Option<Instant>,
}

impl Dest {
    fn new(now: Instant) -> Self {
        Self {
            sticky: None,
            last_used: now,
            open: HashMap::new(),
            meter: Arc::default(),
            mark: (now, 0),
            heavy_until: None,
        }
    }

    fn open_total(&self) -> u32 {
        self.open.values().sum()
    }

    /// Whether this destination is a bulk transfer now. By bytes: [`HEAVY_BYTES`] received since
    /// the mark, or, when the mark is older than [`HEAVY_WINDOW`], the same average rate over the
    /// whole time since (so a download whose next connection comes minutes later still counts,
    /// and a web app trickling all day never does). Once heavy it stays so for a window. Or by
    /// connections: [`BULK_OPEN`] open at once is a downloader too, before any byte arrives.
    fn heavy(&mut self, now: Instant) -> bool {
        let received = self.meter.down.load(Ordering::Relaxed);
        let (since, at) = self.mark;
        let elapsed = now.saturating_duration_since(since);
        let needed =
            HEAVY_BYTES as f64 * (elapsed.as_secs_f64() / HEAVY_WINDOW.as_secs_f64()).max(1.0);
        let crossed = received.saturating_sub(at) as f64 >= needed;
        if crossed {
            self.heavy_until = Some(now + HEAVY_WINDOW);
        }
        if crossed || elapsed >= HEAVY_WINDOW {
            self.mark = (now, received);
        }
        self.heavy_until.is_some_and(|until| now < until) || self.open_total() >= BULK_OPEN
    }
}

/// A sticky network keeps its destinations only while its capacity is at least this share of the
/// best eligible network's.
pub const COMPARABLE: f64 = 0.25;

/// Bytes received from one destination within [`HEAVY_WINDOW`] that make it a bulk transfer.
/// 8 MB is well above what a web page, a login or an API session pulls from one IP in two minutes
/// (a heavy page is 2–5 MB spread over many hosts), and a download at 11 MB/s passes it in under a
/// second, so a download manager's second connection already counts.
pub const HEAVY_BYTES: u64 = 8_000_000;

/// The window for [`HEAVY_BYTES`], and how long a destination stays heavy after its last
/// crossing. The helper's default stickiness.
pub const HEAVY_WINDOW: Duration = Duration::from_secs(120);

/// Open connections to one destination that make it a bulk transfer too, before any bytes: a
/// downloader that opens all its connections at once. Sites that care about the client IP speak
/// HTTP/2 or HTTP/3, one connection per host.
pub const BULK_OPEN: u32 = 4;

/// A heavy destination's connections only go to networks with at least this share of the best
/// eligible network's capacity: one connection on a nearly dead link would hold its byte range
/// back for the whole download.
pub const FLOOR: f64 = 0.05;

/// What an unmeasured network counts as, relative to the best measured one.
pub const UNMEASURED: f64 = 0.25;

/// The destination table is swept for expired entries only once it grows past this, so a picking
/// burst doesn't pay for a full scan every time.
const SWEEP_ABOVE: usize = 4096;

impl Scheduler {
    /// `sticky_ttl` is how long a destination stays on its network after its last connection
    /// opened; zero turns stickiness off (every connection is placed by weight alone).
    pub fn new(sticky_ttl: Duration) -> Self {
        Self { sticky_ttl, dests: HashMap::new(), open: HashMap::new(), credit: HashMap::new() }
    }

    /// Chooses the network for a new connection to `dest`, and counts it as open there until
    /// [`finished`](Self::finished). `None` when no network is eligible.
    pub fn pick(
        &mut self,
        dest: IpAddr,
        now: Instant,
        candidates: &[Candidate],
    ) -> Option<NetworkId> {
        let eligible: Vec<&Candidate> = candidates.iter().filter(|c| c.eligible()).collect();
        if eligible.is_empty() {
            return None;
        }
        let capacities = capacities(&eligible);
        let best = capacities.iter().copied().fold(0.0, f64::max);

        if self.dests.len() > SWEEP_ABOVE {
            let keep = self.sticky_ttl.max(HEAVY_WINDOW);
            self.dests.retain(|_, d| {
                !d.open.is_empty() || now.saturating_duration_since(d.last_used) < keep
            });
        }
        let ttl = self.sticky_ttl;
        let entry = self.dests.entry(dest).or_insert_with(|| Dest::new(now));
        let heavy = entry.heavy(now);
        // The sticky network, if fresh, still eligible and not much slower than the best.
        let sticky = entry
            .sticky
            .as_ref()
            .filter(|_| now.saturating_duration_since(entry.last_used) < ttl)
            .and_then(|s| eligible.iter().position(|c| &c.id == s))
            .filter(|&at| capacities[at] >= COMPARABLE * best)
            .map(|at| eligible[at].id.clone());

        let chosen = if heavy {
            // Usable: never a heavy connection on a network far below the best.
            let (usable, usable_caps): (Vec<&Candidate>, Vec<f64>) = eligible
                .iter()
                .zip(&capacities)
                .filter(|(_, cap)| **cap >= FLOOR * best)
                .map(|(c, cap)| (*c, *cap))
                .unzip();
            let uncovered = usable
                .iter()
                .zip(&usable_caps)
                .filter(|(c, _)| !entry.open.contains_key(&c.id))
                .max_by(|a, b| a.1.total_cmp(b.1))
                .map(|(c, _)| c.id.clone());
            match uncovered {
                Some(id) => id,
                None => self.weighted_choice(&usable, &usable_caps),
            }
        } else {
            match &sticky {
                Some(network) => network.clone(),
                None => self.weighted_choice(&eligible, &capacities),
            }
        };

        // Borrowed again: weighted_choice needed all of self.
        let entry = self.dests.get_mut(&dest).expect("inserted above");
        // A heavy connection spread elsewhere leaves the destination where it sticks.
        entry.sticky = Some(sticky.unwrap_or_else(|| chosen.clone()));
        entry.last_used = now;
        *entry.open.entry(chosen.clone()).or_insert(0) += 1;
        *self.open.entry(chosen.clone()).or_insert(0) += 1;
        Some(chosen)
    }

    /// The byte counters for connections to `dest`. The relay of every connection placed by
    /// [`pick`](Self::pick) adds to them, which is how the scheduler sees heavy destinations.
    pub fn meter_for(&mut self, dest: IpAddr, now: Instant) -> Arc<Meter> {
        self.dests.entry(dest).or_insert_with(|| Dest::new(now)).meter.clone()
    }

    /// A connection to `dest` that [`pick`](Self::pick) placed on `network` has closed (or
    /// failed to open).
    pub fn finished(&mut self, network: &NetworkId, dest: IpAddr) {
        if let Some(n) = self.open.get_mut(network) {
            *n = n.saturating_sub(1);
        }
        self.open.retain(|_, n| *n > 0);
        if let Some(d) = self.dests.get_mut(&dest) {
            if let Some(n) = d.open.get_mut(network) {
                *n = n.saturating_sub(1);
            }
            d.open.retain(|_, n| *n > 0);
        }
    }

    /// Forgets that `dest` is sticky, so its next connection is placed afresh. For a dial that
    /// failed on the sticky network although the network still looks up: the destination may be
    /// unreachable from that ISP alone.
    pub fn unstick(&mut self, dest: IpAddr) {
        if let Some(d) = self.dests.get_mut(&dest) {
            d.sticky = None;
        }
    }

    /// Open connections on `network`, as counted by pick/finished.
    pub fn open_on(&self, network: &NetworkId) -> u32 {
        self.open.get(network).copied().unwrap_or(0)
    }

    /// Smooth weighted round robin over the eligible networks, weighted by capacity: each pick
    /// every network earns its share of credit, the richest is chosen and pays a whole round. So
    /// picks interleave in exact proportion (11.5 : 0.9 → about 13 of every 14 on the first) and
    /// a burst never lands on one network by chance. Open connections don't count: most open
    /// connections are idle keep-alives, and counting them made a network that held a few idle
    /// ones look busy while it had all its bandwidth free.
    fn weighted_choice(&mut self, eligible: &[&Candidate], capacities: &[f64]) -> NetworkId {
        let total: f64 = capacities.iter().sum();
        let mut best = 0;
        for (i, c) in eligible.iter().enumerate() {
            let credit = self.credit.entry(c.id.clone()).or_insert(0.0);
            *credit += capacities[i] / total;
            if i > 0 && *credit > self.credit[&eligible[best].id] {
                best = i;
            }
        }
        if let Some(credit) = self.credit.get_mut(&eligible[best].id) {
            *credit -= 1.0;
        }
        // A network that left keeps no credit to spend when it returns.
        self.credit.retain(|id, _| eligible.iter().any(|c| &c.id == id));
        eligible[best].id.clone()
    }
}

/// Each eligible network's capacity for the choice: its measurement, or for an unmeasured one
/// [`UNMEASURED`] of the best measured (all equal while nothing is measured yet).
fn capacities(eligible: &[&Candidate]) -> Vec<f64> {
    let measured = |c: &Candidate| c.capacity.filter(|v| v.is_finite() && *v > 0.0);
    let best = eligible.iter().filter_map(|c| measured(c)).fold(0.0, f64::max);
    let prior = if best > 0.0 { UNMEASURED * best } else { 1.0 };
    eligible.iter().map(|c| measured(c).unwrap_or(prior)).collect()
}

/// Whether traffic to `dest` should go out to the internet at all. Windows treats the virtual
/// adapter as one more LAN and sends it its multicast and broadcast chatter (SSDP, LLMNR, mDNS,
/// NetBIOS) and link-local traffic; relaying those onto a physical network would leak local
/// discovery onto the wrong link, so they are dropped. `tun_net` is the adapter's own subnet
/// (address, prefix length), which has nothing behind it either.
pub fn relayable(dest: IpAddr, tun_net: (std::net::Ipv4Addr, u8)) -> bool {
    match dest {
        IpAddr::V4(ip) => {
            let (net, prefix) = tun_net;
            let mask = u32::MAX.checked_shl(32 - u32::from(prefix)).unwrap_or(0);
            let in_tun_net = u32::from(ip) & mask == u32::from(net) & mask;
            !(ip.is_multicast()
                || ip.is_broadcast()
                || ip.is_link_local()
                || ip.is_loopback()
                || ip.is_unspecified()
                || in_tun_net)
        }
        IpAddr::V6(ip) => {
            let link_local = ip.segments()[0] & 0xffc0 == 0xfe80;
            !(ip.is_multicast() || ip.is_loopback() || ip.is_unspecified() || link_local)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn local_chatter_is_not_relayed() {
        let tun = (Ipv4Addr::new(10, 71, 0, 1), 24);
        for dropped in [
            "239.255.255.250", // SSDP
            "224.0.0.252",     // LLMNR
            "224.0.0.251",     // mDNS
            "255.255.255.255",
            "10.71.0.255", // the adapter's own subnet broadcast (NetBIOS)
            "10.71.0.9",
            "169.254.1.1",
            "127.0.0.1",
            "0.0.0.0",
            "ff02::fb",
            "ff02::1:3",
            "fe80::1",
            "::1",
        ] {
            assert!(!relayable(dropped.parse().unwrap(), tun), "{dropped}");
        }
        for kept in ["80.249.99.148", "1.1.1.1", "10.72.0.1", "192.168.0.1", "2606:4700::1111"] {
            assert!(relayable(kept.parse().unwrap(), tun), "{kept}");
        }
    }

    fn id(s: &str) -> NetworkId {
        NetworkId::new(s)
    }

    fn ip(last: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, last))
    }

    fn with_capacity(name: &str, capacity: f64) -> Candidate {
        Candidate { capacity: Some(capacity), ..Candidate::new(id(name)) }
    }

    #[test]
    fn spreads_evenly_across_equal_networks() {
        let mut s = Scheduler::new(Duration::ZERO);
        let nets = [Candidate::new(id("eth")), Candidate::new(id("wifi"))];
        let now = Instant::now();
        for _ in 0..8 {
            s.pick(ip(1), now, &nets);
        }
        assert_eq!(s.open_on(&id("eth")), 4);
        assert_eq!(s.open_on(&id("wifi")), 4);
    }

    #[test]
    fn weights_by_capacity() {
        let mut s = Scheduler::new(Duration::ZERO);
        let nets = [with_capacity("fast", 300.0), with_capacity("slow", 100.0)];
        let now = Instant::now();
        for i in 0..40 {
            s.pick(ip(i), now, &nets);
        }
        assert_eq!(s.open_on(&id("fast")), 30);
        assert_eq!(s.open_on(&id("slow")), 10);
    }

    #[test]
    fn an_unmeasured_network_gets_a_small_share_so_it_gets_measured() {
        let mut s = Scheduler::new(Duration::ZERO);
        let nets = [with_capacity("fast", 400.0), Candidate::new(id("new"))];
        let now = Instant::now();
        for i in 0..50 {
            s.pick(ip(i), now, &nets);
        }
        // Counted as a quarter of the best: 400 : 100 → 40 : 10. Not starved, not trusted.
        assert_eq!(s.open_on(&id("fast")), 40);
        assert_eq!(s.open_on(&id("new")), 10);
    }

    /// The live bug: Ethernet 11.5 MB/s, Wi-Fi 0.9 MB/s. New connections must go overwhelmingly
    /// to Ethernet, sticky or not.
    #[test]
    fn twelve_to_one_capacity_sends_nearly_everything_to_the_fast_network() {
        for ttl in [Duration::ZERO, Duration::from_secs(300)] {
            let mut s = Scheduler::new(ttl);
            let nets = [with_capacity("wifi", 0.9e6), with_capacity("eth", 11.5e6)];
            let now = Instant::now();
            for i in 0..100 {
                s.pick(ip(i), now, &nets);
            }
            let eth = s.open_on(&id("eth"));
            assert!(eth >= 90, "{eth} of 100 on Ethernet (ttl {ttl:?})");
            assert!(s.open_on(&id("wifi")) >= 1, "the slow one still carries a little");
        }
    }

    /// The second live bug: idle connections made the fast network look busy. Placement
    /// follows capacity whatever is open.
    #[test]
    fn idle_open_connections_do_not_push_new_ones_to_a_slow_network() {
        let mut s = Scheduler::new(Duration::ZERO);
        let nets = [with_capacity("eth", 9.0e6), with_capacity("wifi", 1.0e6)];
        let now = Instant::now();
        for i in 0..10 {
            assert_eq!(s.pick(ip(100 + i), now, &[with_capacity("eth", 9.0e6)]), Some(id("eth")));
        }
        let picks: Vec<NetworkId> = (0..8).filter_map(|i| s.pick(ip(i), now, &nets)).collect();
        let eth = picks.iter().filter(|n| **n == id("eth")).count();
        assert!(eth >= 7, "{eth} of 8 on Ethernet despite its 10 idle connections");
    }

    #[test]
    fn a_sticky_destination_leaves_a_network_measured_much_slower() {
        let mut s = Scheduler::new(Duration::from_secs(300));
        let now = Instant::now();
        // Nothing measured yet: the destination lands on Wi-Fi (first in order) and sticks.
        let unmeasured = [Candidate::new(id("wifi")), Candidate::new(id("eth"))];
        assert_eq!(s.pick(ip(1), now, &unmeasured), Some(id("wifi")));
        // Measured: Wi-Fi is a twelfth of Ethernet. The destination moves, and sticks there.
        let measured = [with_capacity("wifi", 0.9e6), with_capacity("eth", 11.5e6)];
        assert_eq!(s.pick(ip(1), now, &measured), Some(id("eth")));
        assert_eq!(s.pick(ip(1), now, &measured), Some(id("eth")));
        // Comparable networks (within 4x) keep a sticky destination where it is.
        let comparable = [with_capacity("wifi", 5.0e6), with_capacity("eth", 11.5e6)];
        let mut s = Scheduler::new(Duration::from_secs(300));
        assert_eq!(s.pick(ip(2), now, &unmeasured), Some(id("wifi")));
        assert_eq!(s.pick(ip(2), now, &comparable), Some(id("wifi")));
    }

    #[test]
    fn a_bulk_destination_spreads_by_capacity() {
        let mut s = Scheduler::new(Duration::from_secs(300));
        let nets = [with_capacity("eth", 10.0), with_capacity("wifi", 10.0)];
        let now = Instant::now();
        let picks: Vec<NetworkId> = (0..12).filter_map(|_| s.pick(ip(1), now, &nets)).collect();
        // The first BULK_OPEN stick together; past that the destination is a bulk transfer: the
        // next one covers the other network, the rest alternate between the two equal networks.
        let bulk = BULK_OPEN as usize;
        assert!(picks[..bulk].iter().all(|n| *n == picks[0]));
        let other = if picks[0] == id("eth") { id("wifi") } else { id("eth") };
        assert_eq!(picks[bulk], other);
        let rest = picks[bulk + 1..].iter().filter(|n| **n == other).count();
        assert!((3..=4).contains(&rest), "{rest} of 7 on {other:?}");
        // It still sticks where it started once the bulk is over.
        for net in &picks {
            s.finished(net, ip(1));
        }
        assert_eq!(s.pick(ip(1), now, &nets), Some(picks[0].clone()));
    }

    #[test]
    fn finished_connections_free_their_slot() {
        let mut s = Scheduler::new(Duration::ZERO);
        let nets = [Candidate::new(id("eth")), Candidate::new(id("wifi"))];
        let now = Instant::now();
        let first = s.pick(ip(1), now, &nets).unwrap();
        let second = s.pick(ip(1), now, &nets).unwrap();
        assert_ne!(first, second);
        s.finished(&first, ip(1));
        assert_eq!(s.pick(ip(1), now, &nets), Some(first.clone()));
        s.finished(&first, ip(1));
        s.finished(&first, ip(1)); // an extra finish never underflows
        assert_eq!(s.open_on(&first), 0);
    }

    #[test]
    fn skips_disabled_and_limited_networks() {
        let mut s = Scheduler::new(Duration::ZERO);
        let nets = [
            Candidate { enabled: false, ..Candidate::new(id("off")) },
            Candidate { at_limit: true, ..Candidate::new(id("capped")) },
            Candidate::new(id("ok")),
        ];
        let now = Instant::now();
        for i in 0..5 {
            assert_eq!(s.pick(ip(i), now, &nets), Some(id("ok")));
        }
    }

    #[test]
    fn none_when_nothing_is_eligible() {
        let mut s = Scheduler::new(Duration::from_secs(60));
        let nets = [Candidate { enabled: false, ..Candidate::new(id("off")) }];
        assert_eq!(s.pick(ip(1), Instant::now(), &nets), None);
        assert_eq!(s.pick(ip(1), Instant::now(), &[]), None);
    }

    #[test]
    fn destination_sticks_to_its_network() {
        let mut s = Scheduler::new(Duration::from_secs(60));
        let nets = [Candidate::new(id("eth")), Candidate::new(id("wifi"))];
        let now = Instant::now();
        let first = s.pick(ip(1), now, &nets).unwrap();
        // The round robin alone would now send the next one elsewhere; stickiness keeps it (up
        // to the bulk threshold).
        for _ in 1..BULK_OPEN {
            assert_eq!(s.pick(ip(1), now, &nets), Some(first.clone()));
        }
        // A different destination is still balanced.
        assert_ne!(s.pick(ip(2), now, &nets), Some(first));
    }

    #[test]
    fn stickiness_slides_and_then_expires() {
        let ttl = Duration::from_secs(60);
        let mut s = Scheduler::new(ttl);
        let nets = [Candidate::new(id("eth")), Candidate::new(id("wifi"))];
        let t0 = Instant::now();
        let first = s.pick(ip(1), t0, &nets).unwrap();
        // Each use within the TTL extends it.
        let t1 = t0 + Duration::from_secs(50);
        assert_eq!(s.pick(ip(1), t1, &nets), Some(first.clone()));
        let t2 = t1 + Duration::from_secs(50);
        assert_eq!(s.pick(ip(1), t2, &nets), Some(first.clone()));
        // Two connections are open on `first`, none on the other: once expired, load decides.
        let t3 = t2 + ttl;
        assert_ne!(s.pick(ip(1), t3, &nets), Some(first));
    }

    #[test]
    fn sticky_destination_fails_over_and_re_sticks() {
        let mut s = Scheduler::new(Duration::from_secs(60));
        let both = [Candidate::new(id("eth")), Candidate::new(id("wifi"))];
        let now = Instant::now();
        assert_eq!(s.pick(ip(1), now, &both), Some(id("eth")));
        // Ethernet drops (gone from the list) → Wi-Fi, and the destination now sticks there.
        let wifi_only = [Candidate::new(id("wifi"))];
        assert_eq!(s.pick(ip(1), now, &wifi_only), Some(id("wifi")));
        // Ethernet is back and idle, but the destination stays on Wi-Fi.
        assert_eq!(s.pick(ip(1), now, &both), Some(id("wifi")));
        // Hitting a limit is failover too.
        let wifi_capped =
            [Candidate::new(id("eth")), Candidate { at_limit: true, ..Candidate::new(id("wifi")) }];
        assert_eq!(s.pick(ip(1), now, &wifi_capped), Some(id("eth")));
    }

    #[test]
    fn unstick_lets_the_next_connection_move() {
        let mut s = Scheduler::new(Duration::from_secs(60));
        let nets = [Candidate::new(id("eth")), Candidate::new(id("wifi"))];
        let now = Instant::now();
        let first = s.pick(ip(1), now, &nets).unwrap();
        s.unstick(ip(1));
        assert_ne!(s.pick(ip(1), now, &nets), Some(first));
    }

    #[test]
    fn expired_entries_are_swept_when_the_table_grows() {
        let ttl = Duration::from_secs(1);
        let mut s = Scheduler::new(ttl);
        let nets = [Candidate::new(id("eth"))];
        let t0 = Instant::now();
        for i in 0..=SWEEP_ABOVE as u32 {
            let dest = IpAddr::V4(Ipv4Addr::from(i));
            s.pick(dest, t0, &nets);
            if i > 0 {
                s.finished(&id("eth"), dest); // one stays open: open destinations are kept
            }
        }
        s.pick(ip(1), t0 + HEAVY_WINDOW * 2, &nets);
        assert_eq!(s.dests.len(), 2);
    }

    /// Feeds `bytes` as received from `dest`, as a relay would.
    fn receive(s: &mut Scheduler, dest: IpAddr, now: Instant, bytes: u64) {
        s.meter_for(dest, now).down.fetch_add(bytes, Ordering::Relaxed);
    }

    /// The FDM bug: Ethernet 11.5 MB/s, Wi-Fi 1 MB/s, a downloader's handful of connections to
    /// one CDN IP. Capacity weighting (92 : 8) and stickiness put all of them on Ethernet; a heavy
    /// destination covers both networks.
    #[test]
    fn a_heavy_destination_covers_every_usable_network() {
        let mut s = Scheduler::new(Duration::from_secs(120));
        let nets = [with_capacity("eth", 11.5e6), with_capacity("wifi", 1.0e6)];
        let now = Instant::now();
        assert_eq!(s.pick(ip(1), now, &nets), Some(id("eth")));
        receive(&mut s, ip(1), now, 20_000_000);
        let later = now + Duration::from_secs(2);
        let picks: Vec<NetworkId> = (0..3).filter_map(|_| s.pick(ip(1), later, &nets)).collect();
        assert_eq!(picks[0], id("wifi"), "the first connection after turning heavy covers Wi-Fi");
        assert!(picks[1..].iter().all(|n| *n == id("eth")), "the rest by capacity: {picks:?}");
        assert_eq!(s.open_on(&id("eth")), 3);
        assert_eq!(s.open_on(&id("wifi")), 1);
    }

    #[test]
    fn crossing_the_byte_threshold_starts_the_spreading() {
        let mut s = Scheduler::new(Duration::from_secs(120));
        let nets = [with_capacity("eth", 11.5e6), with_capacity("wifi", 1.0e6)];
        let t0 = Instant::now();
        assert_eq!(s.pick(ip(1), t0, &nets), Some(id("eth")));
        receive(&mut s, ip(1), t0, HEAVY_BYTES - 1);
        let t1 = t0 + Duration::from_secs(1);
        assert_eq!(s.pick(ip(1), t1, &nets), Some(id("eth")), "just below: still sticky");
        receive(&mut s, ip(1), t1, 1);
        assert_eq!(s.pick(ip(1), t1, &nets), Some(id("wifi")), "crossed: spread at once");
        // Minutes later the download still runs: the same rate over the longer time counts.
        let t2 = t1 + Duration::from_secs(300);
        receive(&mut s, ip(1), t2, 3 * HEAVY_BYTES);
        for net in [id("eth"), id("eth"), id("wifi")] {
            s.finished(&net, ip(1));
        }
        assert_eq!(s.pick(ip(1), t2, &nets), Some(id("eth")), "covers the fastest first");
        assert_eq!(s.pick(ip(1), t2, &nets), Some(id("wifi")));
    }

    #[test]
    fn a_light_destination_stays_sticky() {
        let mut s = Scheduler::new(Duration::from_secs(120));
        let nets = [with_capacity("eth", 11.5e6), with_capacity("wifi", 5.0e6)];
        let mut now = Instant::now();
        let first = s.pick(ip(1), now, &nets).unwrap();
        // A web app used all day: 2 MB a minute, three connections at a time at most.
        for _ in 0..60 {
            now += Duration::from_secs(60);
            receive(&mut s, ip(1), now, 2_000_000);
            assert_eq!(s.pick(ip(1), now, &nets), Some(first.clone()));
            assert_eq!(s.pick(ip(1), now, &nets), Some(first.clone()));
            s.finished(&first, ip(1));
            s.finished(&first, ip(1));
        }
    }

    #[test]
    fn a_heavy_destination_never_uses_a_network_below_the_floor() {
        let mut s = Scheduler::new(Duration::from_secs(120));
        let nets = [
            with_capacity("eth", 11.5e6),
            with_capacity("wifi", 1.0e6),
            with_capacity("dying", 0.2e6), // under 5 % of Ethernet
            Candidate { at_limit: true, ..with_capacity("capped", 11.5e6) },
        ];
        let now = Instant::now();
        s.pick(ip(1), now, &nets);
        receive(&mut s, ip(1), now, 50_000_000);
        for _ in 0..40 {
            s.pick(ip(1), now, &nets);
        }
        assert_eq!(s.open_on(&id("dying")), 0);
        assert_eq!(s.open_on(&id("capped")), 0);
        assert!(s.open_on(&id("wifi")) >= 1);
        // A light destination may still use the slow one (it gets its small weighted share).
        let light: u32 =
            (2..200).filter(|&i| s.pick(ip(i), now, &nets) == Some(id("dying"))).count() as u32;
        assert!(light >= 1);
    }
}
