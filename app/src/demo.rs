//! A pretend backend that speaks the real protocol, so the whole UI can be built, tried and
//! photographed before `sonic-manager` exists.
//!
//! It simulates a computer with several connections of different speeds and downloads that
//! behave like the real thing: blocks pulled by streams from one shared queue (so faster networks
//! take more of the file), Auto stream growth, hedged streams racing the last blocks, a data
//! allowance running out, a network that drops off and comes back, the queue, pause, resume,
//! cancel, a failed download that a fresh link revives. Nothing touches the network or disk.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use sonic_core::{
    AppSettings, BlockIndex, BlockStatus, Command, DataPeriod, DownloadId, DownloadNetwork,
    DownloadState, DownloadStatus, Event, FinishedDownload, InterfaceAddress, Millis, NetworkId,
    NetworkInterface, NetworkKind, NetworkPreference, NetworkStatus, ProbeResult,
    SPEED_HISTORY_SECONDS, StartRequest, StreamState, StreamStatus, plan_blocks,
};
use tokio::sync::mpsc;

const MIB: f64 = 1024.0 * 1024.0;
const GIB: u64 = 1024 * 1024 * 1024;
/// How fast the pretend disk writes; above it downloads report `disk_limited`.
const DISK_BPS: f64 = 46.0 * MIB;
/// Simulation step, and so the rate of `DownloadUpdated` per running download.
pub const TICK: Duration = Duration::from_millis(250);

/// Starts the demo backend on `runtime` and hands back the UI's ends of its channels.
pub fn spawn(runtime: &tokio::runtime::Handle) -> (mpsc::Sender<Command>, mpsc::Receiver<Event>) {
    let (command_tx, command_rx) = mpsc::channel(256);
    let (event_tx, event_rx) = mpsc::channel(1024);
    runtime.spawn(run(command_rx, event_tx));
    (command_tx, event_rx)
}

/// The backend loop: answers commands and advances the world every [`TICK`].
pub async fn run(mut commands: mpsc::Receiver<Command>, events: mpsc::Sender<Event>) {
    let mut world = World::new(sonic_core::now_millis());
    for event in world.initial_events() {
        if events.send(event).await.is_err() {
            return;
        }
    }
    let mut tick = tokio::time::interval(TICK);
    loop {
        let out = tokio::select! {
            command = commands.recv() => match command {
                None => return,
                Some(Command::Shutdown { reply }) => {
                    let _ = reply.send(Ok(()));
                    return;
                }
                Some(Command::Probe { url, reply }) => {
                    // Answered later, as a server would: the loop keeps ticking meanwhile.
                    tokio::spawn(async move {
                        tokio::time::sleep(probe_delay(&url)).await;
                        let _ = reply.send(probe(&url));
                    });
                    Vec::new()
                }
                Some(command) => world.handle(command),
            },
            _ = tick.tick() => world.step(TICK.as_secs_f64()),
        };
        for event in out {
            if events.send(event).await.is_err() {
                return;
            }
        }
    }
}

/// A tiny deterministic generator: the demo looks alive, and snapshots come out the same.
#[derive(Clone, Debug)]
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> f64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Uniform in `-1.0..1.0`.
    fn signed(&mut self) -> f64 {
        self.next() * 2.0 - 1.0
    }
}

fn hash(text: &str) -> u64 {
    // FNV-1a: stable across runs, unlike `DefaultHasher`.
    text.bytes()
        .fold(0xcbf2_9ce4_8422_2325, |h, b| (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3))
}

#[derive(Clone, Debug)]
struct SimNet {
    iface: NetworkInterface,
    /// Its best speed with plenty of streams.
    base_bps: f64,
    /// A slow random walk around 1.0: real links wobble.
    level: f64,
    online: bool,
    /// Auto stream growth stops here for this network (a server refusing more, say).
    max_streams: u32,
}

#[derive(Clone, Debug)]
struct Sim {
    state: DownloadState,
    /// Bytes each network delivered since the last speed-history sample.
    window: BTreeMap<NetworkId, f64>,
    since_sample: f64,
    /// Auto: streams each network runs now, and seconds since it last grew.
    target: BTreeMap<NetworkId, (u32, f64)>,
    next_stream: u32,
}

/// The whole simulated computer.
#[derive(Clone, Debug)]
pub struct World {
    now: Millis,
    rng: Rng,
    nets: Vec<SimNet>,
    sims: Vec<Sim>,
    history: Vec<FinishedDownload>,
    settings: AppSettings,
    usage: BTreeMap<NetworkId, u64>,
    usage_clock: f64,
    /// Seconds simulated, for scripted events (the phone unplugging).
    elapsed: f64,
}

fn iface(
    id: &str,
    name: &str,
    description: &str,
    kind: NetworkKind,
    ip: &str,
    gateway: bool,
) -> NetworkInterface {
    NetworkInterface {
        id: NetworkId::new(id),
        name: name.into(),
        description: description.into(),
        kind,
        addresses: ip
            .parse()
            .map(|ip| vec![InterfaceAddress { ip, prefix_len: 24 }])
            .unwrap_or_default(),
        ipv4_index: 1,
        ipv6_index: 0,
        has_gateway: gateway,
        mac: None,
    }
}

/// Streams carry a network's bandwidth only together: with few, each TCP connection's window
/// caps it. 8 streams get ~55% of a link, 16 ~80%, 32 nearly all — why Auto grows them.
fn stream_factor(streams: u32) -> f64 {
    1.0 - (-(streams as f64) / 10.0).exp()
}

/// Block size for a file: about a thousand blocks, 1–8 MiB each, a power of two.
fn block_size_for(total: u64) -> u64 {
    let ideal = (total / 1000).clamp(1 << 20, 8 << 20);
    1u64 << (63 - ideal.leading_zeros())
}

impl World {
    pub fn new(now: Millis) -> Self {
        let nets = vec![
            SimNet {
                iface: iface(
                    "{wifi}",
                    "Wi-Fi",
                    "Intel(R) Wi-Fi 6 AX201 160MHz",
                    NetworkKind::Wifi,
                    "192.168.1.23",
                    true,
                ),
                base_bps: 12.5 * MIB,
                level: 1.0,
                online: true,
                max_streams: 32,
            },
            SimNet {
                iface: iface(
                    "{ethernet}",
                    "Ethernet",
                    "Realtek PCIe 2.5GbE Family Controller",
                    NetworkKind::Ethernet,
                    "10.0.0.12",
                    true,
                ),
                base_bps: 23.0 * MIB,
                level: 1.0,
                online: true,
                max_streams: 32,
            },
            SimNet {
                iface: iface(
                    "{usb}",
                    "Ethernet 3",
                    "Remote NDIS based Internet Sharing Device",
                    NetworkKind::Usb,
                    "192.168.42.129",
                    true,
                ),
                base_bps: 7.5 * MIB,
                level: 1.0,
                online: true,
                max_streams: 16,
            },
            SimNet {
                iface: iface(
                    "{cellular}",
                    "Cellular",
                    "Qualcomm Snapdragon X55 5G",
                    NetworkKind::Cellular,
                    "100.72.14.9",
                    true,
                ),
                base_bps: 3.2 * MIB,
                level: 1.0,
                online: true,
                max_streams: 8,
            },
            SimNet {
                iface: iface(
                    "{wsl}",
                    "vEthernet (WSL)",
                    "Hyper-V Virtual Ethernet Adapter",
                    NetworkKind::Virtual,
                    "172.28.0.1",
                    false,
                ),
                base_bps: 0.0,
                level: 1.0,
                online: true,
                max_streams: 8,
            },
        ];
        let mut settings = AppSettings {
            destination_dir: Some(PathBuf::from(r"C:\Users\you\Downloads")),
            ..AppSettings::default()
        };
        settings.network_preferences.insert(
            NetworkId::new("{usb}"),
            NetworkPreference { custom_name: Some("Pixel 8".into()), ..Default::default() },
        );
        settings.network_preferences.insert(
            NetworkId::new("{cellular}"),
            NetworkPreference {
                data_limit: Some(2 * GIB),
                data_period: DataPeriod::Month,
                ..Default::default()
            },
        );
        let usage: BTreeMap<_, _> = [
            (NetworkId::new("{wifi}"), 84 * GIB + 310 * (1 << 20)),
            (NetworkId::new("{ethernet}"), 212 * GIB),
            (NetworkId::new("{usb}"), 6 * GIB + 700 * (1 << 20)),
            (NetworkId::new("{cellular}"), 2 * GIB + 40 * (1 << 20)),
        ]
        .into();
        let mut world = World {
            now,
            rng: Rng(0x5eed_cafe_f00d_1234),
            nets,
            sims: Vec::new(),
            history: Vec::new(),
            settings,
            usage,
            usage_clock: 0.0,
            elapsed: 0.0,
        };
        world.seed();
        world
    }

    /// The downloads and history the demo opens with.
    fn seed(&mut self) {
        let on = |ids: &[&str]| ids.iter().map(|id| NetworkId::new(*id)).collect::<Vec<_>>();
        let dir = self.download_dir();

        let iso = self.make(
            "https://releases.ubuntu.com/24.04.1/ubuntu-24.04.1-desktop-amd64.iso",
            "ubuntu-24.04.1-desktop-amd64.iso",
            6_114_656_256,
            &on(&["{wifi}", "{ethernet}", "{usb}", "{cellular}"]),
            &dir,
        );
        let iso = self.seeded(iso, 0.36, 160.0);
        self.sims.push(iso);

        let samples = self.make(
            "https://cdn.example-games.com/builds/unreal-samples-5.5.zip",
            "unreal-engine-samples-5.5.zip",
            3_350_000_000,
            &on(&["{wifi}", "{ethernet}", "{wsl}"]),
            &dir,
        );
        let samples = self.seeded(samples, 0.12, 40.0);
        self.sims.push(samples);

        let mut queued = self.make(
            "https://data.example.org/open-images/dataset-part-03.tar.zst",
            "dataset-part-03.tar.zst",
            12_884_901_888,
            &on(&["{wifi}", "{ethernet}", "{usb}"]),
            &dir,
        );
        queued.status = DownloadStatus::Queued;
        queued.queued_at = Some(self.now - 30_000);
        self.sims.push(Sim::new(queued));

        let paused = self.make(
            "https://files.example.net/backups/steam-library-backup.7z",
            "steam-library-backup.7z",
            1_503_238_553,
            &on(&["{wifi}", "{ethernet}"]),
            &dir,
        );
        let mut paused = self.seeded(paused, 0.62, 95.0);
        paused.state.status = DownloadStatus::Paused;
        paused.state.paused_at = Some(self.now - 600_000);
        self.sims.push(paused);

        let failed = self.make(
            "https://huggingface.example.com/models/llama-3.1-8b-instruct-q4_k_m.gguf?expires=1729",
            "llama-3.1-8b-instruct-q4_k_m.gguf",
            4_920_000_000,
            &on(&["{wifi}", "{ethernet}", "{usb}"]),
            &dir,
        );
        let mut failed = self.seeded(failed, 0.23, 70.0);
        failed.state.status = DownloadStatus::Error;
        failed.state.error = Some(
            "The link stopped working: the server answered 403 Forbidden. Links from this site expire after a few hours."
                .into(),
        );
        self.sims.push(failed);
        for sim in &mut self.sims {
            if sim.state.status != DownloadStatus::Downloading {
                sim.state.streams.clear();
                sim.state.speed_bps = 0.0;
                for n in &mut sim.state.networks {
                    n.speed_bps = 0.0;
                }
            }
        }

        let hour = 3_600_000;
        self.history = vec![
            self.finished(
                "blender-4.2.3-windows-x64.msi",
                372_244_480,
                2 * hour,
                &[("{wifi}", 0.34), ("{ethernet}", 0.66)],
                13,
                false,
            ),
            self.finished(
                "Fedora-Workstation-Live-x86_64-41.iso",
                2_362_232_012,
                26 * hour,
                &[("{wifi}", 0.3), ("{ethernet}", 0.55), ("{usb}", 0.15)],
                74,
                false,
            ),
            self.finished(
                "dataset-images-part2.tar.zst",
                10_522_669_875,
                75 * hour,
                &[("{wifi}", 0.35), ("{ethernet}", 0.65)],
                290,
                true,
            ),
            self.finished(
                "VSCodeUserSetup-x64-1.95.3.exe",
                102_760_448,
                122 * hour,
                &[("{wifi}", 1.0)],
                9,
                false,
            ),
        ];
    }

    fn download_dir(&self) -> PathBuf {
        self.settings.destination_dir.clone().unwrap_or_else(|| PathBuf::from("Downloads"))
    }

    fn net(&self, id: &NetworkId) -> Option<&SimNet> {
        self.nets.iter().find(|n| &n.iface.id == id)
    }

    fn over_allowance(&self, id: &NetworkId) -> bool {
        let limit = self.settings.network_preferences.get(id).and_then(|p| p.data_limit);
        limit.is_some_and(|limit| self.usage.get(id).copied().unwrap_or(0) >= limit)
    }

    /// What an enabled network's status should be right now.
    fn status_for(&self, id: &NetworkId) -> NetworkStatus {
        match self.net(id) {
            None => NetworkStatus::Offline,
            Some(n) if !n.online => NetworkStatus::Offline,
            Some(_) if self.over_allowance(id) => NetworkStatus::Limit,
            Some(n) if !n.iface.has_gateway => NetworkStatus::Unreachable,
            Some(_) => NetworkStatus::On,
        }
    }

    fn make(
        &self,
        url: &str,
        name: &str,
        total: u64,
        enabled: &[NetworkId],
        dir: &Path,
    ) -> DownloadState {
        let block_size = block_size_for(total);
        let networks = self
            .nets
            .iter()
            .map(|n| {
                let on = enabled.contains(&n.iface.id);
                DownloadNetwork {
                    id: n.iface.id.clone(),
                    label: n.iface.name.clone(),
                    kind: n.iface.kind,
                    enabled: on,
                    status: if on { self.status_for(&n.iface.id) } else { NetworkStatus::Off },
                    error: (!n.iface.has_gateway && on)
                        .then(|| "It has no route to the internet.".to_string()),
                    bytes_downloaded: 0,
                    speed_bps: 0.0,
                    retries: 0,
                }
            })
            .collect();
        DownloadState {
            id: DownloadId::new(),
            url: url.into(),
            file_name: name.into(),
            destination_path: dir.join(name),
            total_bytes: Some(total),
            bytes_downloaded: 0,
            speed_bps: 0.0,
            time_left_secs: None,
            status: DownloadStatus::Downloading,
            error: None,
            resumable: true,
            started_at: self.now,
            queued_at: None,
            paused_at: None,
            total_paused_ms: 0,
            completed_at: None,
            scheduled: false,
            networks,
            streams: Vec::new(),
            peak_streams: 0,
            blocks: plan_blocks(Some(total), block_size),
            block_size,
            speed_history: BTreeMap::new(),
            peak_speed_bps: None,
            disk_limited: false,
            from_browser: None,
        }
    }

    /// A download already `fraction` done, as if it had run for `secs`: early blocks delivered,
    /// each by a network picked in proportion to its speed (as the shared queue would), and a
    /// minute of speed history behind it.
    fn seeded(&mut self, mut d: DownloadState, fraction: f64, secs: f64) -> Sim {
        let weights: Vec<(NetworkId, f64)> = d
            .networks
            .iter()
            .filter(|n| n.status == NetworkStatus::On)
            .filter_map(|n| self.net(&n.id).map(|s| (n.id.clone(), s.base_bps)))
            .collect();
        let total_weight: f64 = weights.iter().map(|(_, w)| w).sum();
        let done = (d.blocks.len() as f64 * fraction) as usize;
        for block in d.blocks.iter_mut().take(done) {
            let mut pick = self.rng.next() * total_weight;
            let owner = weights
                .iter()
                .find(|(_, w)| {
                    pick -= w;
                    pick <= 0.0
                })
                .or(weights.last())
                .map(|(id, _)| id.clone());
            let len = block.range.len().unwrap_or(0);
            block.status = BlockStatus::Completed;
            block.bytes_downloaded = len;
            if let Some(owner) = owner {
                block.network_id = Some(owner.clone());
                block.bytes_by_network.insert(owner.clone(), len);
                if let Some(n) = d.networks.iter_mut().find(|n| n.id == owner) {
                    n.bytes_downloaded += len;
                }
            }
        }
        d.bytes_downloaded = d.blocks.iter().map(|b| b.bytes_downloaded).sum();
        d.started_at = self.now - (secs * 1000.0) as Millis;
        d.peak_streams = 40;
        for n in &d.networks {
            let base = self
                .net(&n.id)
                .filter(|_| n.status == NetworkStatus::On)
                .map_or(0.0, |s| s.base_bps);
            let mut level = 0.8;
            let series = (0..SPEED_HISTORY_SECONDS)
                .map(|_| {
                    level = (level + self.rng.signed() * 0.06).clamp(0.55, 1.05);
                    base * level * 0.85
                })
                .collect();
            d.speed_history.insert(n.id.clone(), series);
        }
        let totals: Vec<f64> = (0..SPEED_HISTORY_SECONDS)
            .map(|t| d.speed_history.values().map(|s| s[t]).sum())
            .collect();
        d.peak_speed_bps = totals.windows(3).map(|w| w.iter().sum::<f64>() / 3.0).reduce(f64::max);
        let mut sim = Sim::new(d);
        for n in &sim.state.networks {
            sim.target.insert(n.id.clone(), (16, 0.0));
        }
        sim
    }

    fn finished(
        &mut self,
        name: &str,
        total: u64,
        ago_ms: Millis,
        shares: &[(&str, f64)],
        secs: usize,
        missing: bool,
    ) -> FinishedDownload {
        let completed_at = self.now - ago_ms;
        let mut networks = Vec::new();
        let mut speed_history = BTreeMap::new();
        let samples = secs.min(SPEED_HISTORY_SECONDS);
        for (id, share) in shares {
            let id = NetworkId::new(*id);
            let Some(net) = self.net(&id).cloned() else { continue };
            let avg = total as f64 * share / secs as f64;
            // Ramps up as Auto adds streams, wobbles, and tails off as the last blocks finish.
            let series: Vec<f64> = (0..samples)
                .map(|t| {
                    let x = (t as f64 + 0.5) / samples as f64;
                    let shape = (x * 6.0).min(1.0) * ((1.0 - x) * 8.0).min(1.0);
                    avg * 1.18 * shape * (1.0 + self.rng.signed() * 0.08)
                })
                .collect();
            speed_history.insert(id.clone(), series);
            networks.push(DownloadNetwork {
                id: id.clone(),
                label: net.iface.name.clone(),
                kind: net.iface.kind,
                enabled: true,
                status: NetworkStatus::On,
                error: None,
                bytes_downloaded: (total as f64 * share) as u64,
                speed_bps: 0.0,
                retries: (self.rng.next() * 3.0) as u32,
            });
        }
        let totals: Vec<f64> =
            (0..samples).map(|t| speed_history.values().map(|s: &Vec<f64>| s[t]).sum()).collect();
        FinishedDownload {
            id: DownloadId::new(),
            url: format!("https://downloads.example.com/{name}"),
            file_name: name.into(),
            destination_path: self.download_dir().join(name),
            total_bytes: total,
            started_at: completed_at - secs as Millis * 1000,
            completed_at,
            total_paused_ms: 0,
            networks,
            speed_history,
            peak_speed_bps: totals.windows(3).map(|w| w.iter().sum::<f64>() / 3.0).reduce(f64::max),
            peak_streams: 8 * shares.len() as u32 * 3,
            blocks_written: total.div_ceil(block_size_for(total)) as u32,
            from_browser: None,
            missing,
        }
    }

    pub fn initial_events(&self) -> Vec<Event> {
        vec![
            Event::Settings(Arc::new(self.settings.clone())),
            Event::Networks(Arc::new(self.interfaces())),
            Event::NetworkUsage(self.usage.clone()),
            Event::History(Arc::new(self.history.clone())),
            Event::Downloads(self.sims.iter().map(|s| Arc::new(s.state.clone())).collect()),
        ]
    }

    fn interfaces(&self) -> Vec<NetworkInterface> {
        self.nets.iter().filter(|n| n.online).map(|n| n.iface.clone()).collect()
    }

    fn running(&self) -> usize {
        self.sims.iter().filter(|s| s.state.status == DownloadStatus::Downloading).count()
    }

    /// Starts queued downloads, oldest first, while there are free slots.
    fn promote(&mut self) -> Vec<Event> {
        let mut out = Vec::new();
        while self.running() < self.settings.downloads_at_once as usize {
            let next = self
                .sims
                .iter_mut()
                .filter(|s| s.state.status == DownloadStatus::Queued)
                .min_by_key(|s| s.state.queued_at.unwrap_or(0));
            let Some(sim) = next else { break };
            sim.state.status = DownloadStatus::Downloading;
            sim.state.queued_at = None;
            out.push(Event::DownloadUpdated(Arc::new(sim.state.clone())));
        }
        out
    }

    fn find(&mut self, id: DownloadId) -> Option<&mut Sim> {
        self.sims.iter_mut().find(|s| s.state.id == id)
    }

    /// Re-derives every network's status in every download (after a network, an allowance or a
    /// switch changed).
    fn refresh_statuses(&mut self) {
        let statuses: BTreeMap<NetworkId, NetworkStatus> =
            self.nets.iter().map(|n| (n.iface.id.clone(), self.status_for(&n.iface.id))).collect();
        for sim in &mut self.sims {
            for n in &mut sim.state.networks {
                n.status = if n.enabled {
                    statuses.get(&n.id).copied().unwrap_or(NetworkStatus::Offline)
                } else {
                    NetworkStatus::Off
                };
                if n.status != NetworkStatus::On {
                    n.speed_bps = 0.0;
                }
            }
            let usable: BTreeSet<NetworkId> = sim
                .state
                .networks
                .iter()
                .filter(|n| n.status == NetworkStatus::On)
                .map(|n| n.id.clone())
                .collect();
            release_streams(&mut sim.state, |s| !usable.contains(&s.network_id));
        }
    }

    pub fn handle(&mut self, command: Command) -> Vec<Event> {
        let mut out = Vec::new();
        match command {
            // Answered asynchronously by `run`; here only for tests that drive the world directly.
            Command::Probe { url, reply } => {
                let _ = reply.send(probe(&url));
            }
            Command::Start { request, link: _, reply } => {
                let id = self.start(request);
                let _ = reply.send(Ok(id));
                if let Some(sim) = self.find(id) {
                    out.push(Event::DownloadUpdated(Arc::new(sim.state.clone())));
                }
            }
            Command::Pause(id) => {
                let now = self.now;
                if let Some(sim) = self.find(id)
                    && matches!(
                        sim.state.status,
                        DownloadStatus::Downloading | DownloadStatus::Queued
                    )
                {
                    sim.pause(now);
                    out.push(Event::DownloadUpdated(Arc::new(sim.state.clone())));
                }
                out.extend(self.promote());
            }
            Command::Resume(id) => {
                let (now, slots) =
                    (self.now, self.running() < self.settings.downloads_at_once as usize);
                if let Some(sim) = self.find(id)
                    && matches!(sim.state.status, DownloadStatus::Paused | DownloadStatus::Error)
                {
                    sim.resume(now, slots);
                    out.push(Event::DownloadUpdated(Arc::new(sim.state.clone())));
                }
                self.refresh_statuses();
            }
            Command::Cancel(id) | Command::Remove { id, .. } => {
                if let Some(at) = self.sims.iter().position(|s| s.state.id == id) {
                    self.sims.remove(at);
                    out.push(Event::DownloadRemoved(id));
                    out.extend(self.promote());
                } else if let Some(at) = self.history.iter().position(|f| f.id == id) {
                    self.history.remove(at);
                    out.push(Event::History(Arc::new(self.history.clone())));
                }
            }
            Command::Relink { id, url, reply } => match probe(&url) {
                Err(error) => {
                    let _ = reply.send(Err(error));
                }
                Ok(_) => {
                    let (now, slots) =
                        (self.now, self.running() < self.settings.downloads_at_once as usize);
                    let Some(sim) = self.find(id) else {
                        let _ = reply.send(Err("That download is no longer listed.".into()));
                        return out;
                    };
                    sim.state.url = url;
                    sim.resume(now, slots);
                    out.push(Event::DownloadUpdated(Arc::new(sim.state.clone())));
                    let _ = reply.send(Ok(()));
                }
            },
            Command::SetNetwork { id, network, enabled } => {
                let (now, slots) =
                    (self.now, self.running() < self.settings.downloads_at_once as usize);
                if let Some(sim) = self.find(id) {
                    if let Some(n) = sim.state.networks.iter_mut().find(|n| n.id == network) {
                        n.enabled = enabled;
                    }
                    let any = sim.state.networks.iter().any(|n| n.enabled);
                    // Switching off the last network pauses; switching one back on resumes.
                    if !any && sim.state.status == DownloadStatus::Downloading {
                        sim.pause(now);
                    } else if any && enabled && sim.state.status == DownloadStatus::Paused {
                        sim.resume(now, slots);
                    }
                }
                self.refresh_statuses();
                if let Some(sim) = self.find(id) {
                    out.push(Event::DownloadUpdated(Arc::new(sim.state.clone())));
                }
                out.extend(self.promote());
            }
            Command::UpdateSettings { mut settings, reply } => {
                settings.downloads_at_once = settings
                    .downloads_at_once
                    .clamp(sonic_core::DOWNLOADS_AT_ONCE_MIN, sonic_core::DOWNLOADS_AT_ONCE_MAX);
                if !settings.schedule.is_valid() {
                    settings.schedule.enabled = false;
                }
                self.settings = settings.clone();
                let _ = reply.send(Ok(settings.clone()));
                out.push(Event::Settings(Arc::new(settings)));
                self.refresh_statuses();
                out.extend(self.promote());
            }
            Command::Reveal(id) | Command::Open(id) => {
                let name = self
                    .sims
                    .iter()
                    .map(|s| (s.state.id, s.state.file_name.clone()))
                    .chain(self.history.iter().map(|f| (f.id, f.file_name.clone())))
                    .find(|(candidate, _)| *candidate == id)
                    .map(|(_, name)| name)
                    .unwrap_or_default();
                out.push(Event::Notice {
                    title: "Demo mode".into(),
                    body: format!("The real app would now show {name} in File Explorer."),
                });
            }
            Command::ClearHistory => {
                self.history.clear();
                out.push(Event::History(Arc::new(Vec::new())));
            }
            Command::ResetNetworkUsage(network) => {
                self.usage.insert(network, 0);
                out.push(Event::NetworkUsage(self.usage.clone()));
                self.refresh_statuses();
            }
            Command::FreeSpace { dir, reply } => {
                // A plausible, stable figure per folder.
                let gib = 120 + hash(&dir.to_string_lossy()) % 700;
                let _ = reply.send(Ok(Some(gib * GIB)));
            }
            Command::DismissLink(_) => {}
            Command::Shutdown { reply } => {
                let _ = reply.send(Ok(()));
            }
        }
        out
    }

    fn start(&mut self, request: StartRequest) -> DownloadId {
        let splittable = request.splittable();
        let mut d = self.make(
            &request.url,
            &request.file_name,
            request.total_bytes.unwrap_or(0),
            &request.network_ids,
            &request.destination_dir,
        );
        if !splittable {
            // One request carries all of it: one block, one stream, on the first network. With
            // no size given the pretend server still ends somewhere (300 MiB); the UI isn't told.
            let size = request.total_bytes.filter(|&n| n > 0).unwrap_or(300 << 20);
            d.total_bytes = request.total_bytes;
            d.blocks = plan_blocks(Some(size), size);
            d.block_size = size;
            let first = request.network_ids.first().cloned();
            for n in &mut d.networks {
                n.enabled = Some(&n.id) == first.as_ref();
            }
        }
        let slots = self.running() < self.settings.downloads_at_once as usize;
        if !slots {
            d.status = DownloadStatus::Queued;
            d.queued_at = Some(self.now);
        }
        let mut sim = Sim::new(d);
        let per_network = if splittable {
            request.streams_per_network.unwrap_or(sonic_core::START_STREAMS_PER_NETWORK)
        } else {
            1
        };
        // Only Auto grows; a fixed pick's growth clock never reaches the next step.
        let auto = request.streams_per_network.is_none() && splittable;
        let clock = if auto { 0.0 } else { f64::NEG_INFINITY };
        for n in &sim.state.networks {
            sim.target.insert(n.id.clone(), (per_network, clock));
        }
        let id = sim.state.id;
        self.sims.push(sim);
        self.refresh_statuses();
        id
    }

    /// Advances everything by `dt` seconds and reports what changed.
    pub fn step(&mut self, dt: f64) -> Vec<Event> {
        self.now += (dt * 1000.0) as Millis;
        self.elapsed += dt;
        let mut out = Vec::new();

        // Links wobble.
        for n in &mut self.nets {
            n.level = (n.level + self.rng.signed() * 0.05 * dt.sqrt() * 2.0).clamp(0.7, 1.1);
        }
        // The phone is unplugged for ten seconds every two minutes, starting a minute in.
        let usb_online = !(60.0..70.0).contains(&(self.elapsed % 120.0));
        if let Some(usb) = self.nets.iter_mut().find(|n| n.iface.kind == NetworkKind::Usb)
            && usb.online != usb_online
        {
            usb.online = usb_online;
            out.push(Event::Networks(Arc::new(self.interfaces())));
            self.refresh_statuses();
        }

        // How many running downloads share each network: they split it.
        let mut users: BTreeMap<NetworkId, f64> = BTreeMap::new();
        for sim in self.sims.iter().filter(|s| s.state.status == DownloadStatus::Downloading) {
            for n in sim.state.networks.iter().filter(|n| n.status == NetworkStatus::On) {
                *users.entry(n.id.clone()).or_default() += 1.0;
            }
        }
        // Each running download's demand per network, before global caps.
        let mut plans: Vec<BTreeMap<NetworkId, f64>> = Vec::new();
        for sim in &mut self.sims {
            let mut plan = BTreeMap::new();
            if sim.state.status == DownloadStatus::Downloading {
                for n in sim.state.networks.iter().filter(|n| n.status == NetworkStatus::On) {
                    let Some(net) = self.nets.iter().find(|s| s.iface.id == n.id) else { continue };
                    let (streams, grown) = sim.target.entry(n.id.clone()).or_insert((8, 0.0));
                    // Auto: once all streams receive data, double, every few seconds, up to the cap.
                    *grown += dt;
                    if *grown >= 4.0 && *streams < net.max_streams {
                        *streams = (*streams * 2).min(net.max_streams);
                        *grown = 0.0;
                    }
                    let mut bps = net.base_bps * net.level * stream_factor(*streams) / users[&n.id];
                    let limit =
                        self.settings.network_preferences.get(&n.id).and_then(|p| p.speed_limit);
                    if let Some(limit) = limit {
                        bps = bps.min(limit as f64 / users[&n.id]);
                    }
                    plan.insert(n.id.clone(), bps);
                }
            }
            plans.push(plan);
        }
        let demand: f64 = plans.iter().flat_map(|p| p.values()).sum();
        let global = self.settings.effective_speed_limit().map_or(f64::INFINITY, |l| l as f64);
        let cap = global.min(DISK_BPS);
        let scale = if demand > cap { cap / demand } else { 1.0 };
        let disk_limited = demand > DISK_BPS && DISK_BPS <= global;

        let mut finished = Vec::new();
        for (sim, plan) in self.sims.iter_mut().zip(plans) {
            if sim.state.status != DownloadStatus::Downloading {
                continue;
            }
            sim.state.disk_limited = disk_limited;
            let delivered = sim.advance(&plan, scale, dt, &mut self.rng);
            for (id, bytes) in delivered {
                *self.usage.entry(id).or_default() += bytes as u64;
            }
            if sim.state.blocks.iter().all(|b| b.status == BlockStatus::Completed) {
                sim.complete(self.now);
                finished.push(sim.state.id);
            }
            out.push(Event::DownloadUpdated(Arc::new(sim.state.clone())));
        }
        for id in finished {
            if let Some(at) = self.sims.iter().position(|s| s.state.id == id) {
                let sim = self.sims.remove(at);
                self.history.insert(0, finished_from(&sim.state));
                out.push(Event::History(Arc::new(self.history.clone())));
                out.push(Event::DownloadRemoved(id));
            }
        }
        out.extend(self.promote());

        // Usage, and allowances running out, once a second.
        self.usage_clock += dt;
        if self.usage_clock >= 1.0 {
            self.usage_clock = 0.0;
            let before: Vec<bool> =
                self.nets.iter().map(|n| self.over_allowance(&n.iface.id)).collect();
            out.push(Event::NetworkUsage(self.usage.clone()));
            let after: Vec<bool> =
                self.nets.iter().map(|n| self.over_allowance(&n.iface.id)).collect();
            if before != after {
                self.refresh_statuses();
            }
            self.refresh_statuses();
        }
        out
    }
}

impl Sim {
    fn new(state: DownloadState) -> Self {
        let next_stream = state.streams.iter().map(|s| s.id + 1).max().unwrap_or(0);
        Sim {
            state,
            window: BTreeMap::new(),
            since_sample: 0.0,
            target: BTreeMap::new(),
            next_stream,
        }
    }

    fn pause(&mut self, now: Millis) {
        self.state.status = DownloadStatus::Paused;
        self.state.paused_at = Some(now);
        self.state.speed_bps = 0.0;
        self.state.time_left_secs = None;
        self.state.disk_limited = false;
        release_streams(&mut self.state, |_| true);
        for n in &mut self.state.networks {
            n.speed_bps = 0.0;
        }
    }

    fn resume(&mut self, now: Millis, slot_free: bool) {
        if let Some(at) = self.state.paused_at.take() {
            self.state.total_paused_ms += (now - at).max(0) as u64;
        }
        self.state.error = None;
        if slot_free {
            self.state.status = DownloadStatus::Downloading;
        } else {
            self.state.status = DownloadStatus::Queued;
            self.state.queued_at = Some(now);
        }
    }

    fn complete(&mut self, now: Millis) {
        self.state.status = DownloadStatus::Completed;
        self.state.completed_at = Some(now);
        self.state.speed_bps = 0.0;
        self.state.time_left_secs = None;
        self.state.streams.clear();
        for n in &mut self.state.networks {
            n.speed_bps = 0.0;
        }
    }

    /// Runs this download's streams for `dt` seconds with `plan` (bytes a second per network)
    /// scaled by `scale`. Returns the bytes each network delivered.
    fn advance(
        &mut self,
        plan: &BTreeMap<NetworkId, f64>,
        scale: f64,
        dt: f64,
        rng: &mut Rng,
    ) -> BTreeMap<NetworkId, f64> {
        let d = &mut self.state;
        // Keep each network's stream count at its target.
        for id in plan.keys() {
            let want = self.target.get(id).map_or(8, |t| t.0) as usize;
            let have = d.streams.iter().filter(|s| &s.network_id == id).count();
            for _ in have..want {
                d.streams.push(StreamState {
                    id: self.next_stream,
                    network_id: id.clone(),
                    status: StreamStatus::Pending,
                    block: None,
                    hedge: false,
                    bytes_downloaded: 0,
                    speed_bps: 0.0,
                });
                self.next_stream += 1;
            }
        }
        d.peak_streams = d.peak_streams.max(d.streams.len() as u32);

        let mut delivered: BTreeMap<NetworkId, f64> = BTreeMap::new();
        for (id, bps) in plan {
            let streams: Vec<usize> =
                (0..d.streams.len()).filter(|&i| &d.streams[i].network_id == id).collect();
            if streams.is_empty() {
                continue;
            }
            let budget = bps * scale * dt;
            for &si in &streams {
                // Each stream gets a slightly different share: real connections are uneven.
                let share = budget / streams.len() as f64 * (1.0 + rng.signed() * 0.25);
                if d.streams[si].block.is_none() {
                    lease(d, si);
                }
                let Some(index) = d.streams[si].block else {
                    d.streams[si].status = StreamStatus::Pending;
                    d.streams[si].speed_bps = 0.0;
                    continue;
                };
                let block = &mut d.blocks[index as usize];
                let left = block.range.len().unwrap_or(0).saturating_sub(block.bytes_downloaded);
                let take = (share.max(0.0) as u64).min(left);
                block.bytes_downloaded += take;
                *block.bytes_by_network.entry(id.clone()).or_default() += take;
                block.network_id = Some(id.clone());
                let stream = &mut d.streams[si];
                stream.status = StreamStatus::Downloading;
                stream.bytes_downloaded += take;
                stream.speed_bps = stream.speed_bps * 0.6 + take as f64 / dt * 0.4;
                *delivered.entry(id.clone()).or_default() += take as f64;
                if block.bytes_downloaded >= block.range.len().unwrap_or(0) {
                    block.status = BlockStatus::Completed;
                    // Whoever was racing for it lost: free them all, and let each take new work
                    // straight away (racing another block once none is left waiting).
                    let freed: Vec<usize> = (0..d.streams.len())
                        .filter(|&i| d.streams[i].block == Some(index))
                        .collect();
                    for &i in &freed {
                        d.streams[i].block = None;
                        d.streams[i].hedge = false;
                    }
                    for &i in &freed {
                        lease(d, i);
                    }
                }
            }
        }

        // A retry now and then on the wobblier links.
        for n in &mut d.networks {
            if matches!(n.kind, NetworkKind::Usb | NetworkKind::Wifi)
                && n.status == NetworkStatus::On
                && rng.next() < 0.004
            {
                n.retries += 1;
            }
        }

        // Totals and smoothed speeds.
        for n in &mut d.networks {
            let got = delivered.get(&n.id).copied().unwrap_or(0.0);
            n.bytes_downloaded += got as u64;
            n.speed_bps = if n.status == NetworkStatus::On {
                n.speed_bps * 0.5 + got / dt * 0.5
            } else {
                0.0
            };
            *self.window.entry(n.id.clone()).or_default() += got;
        }
        d.bytes_downloaded = d.blocks.iter().map(|b| b.bytes_downloaded).sum();
        d.speed_bps = d.networks.iter().map(|n| n.speed_bps).sum();
        let left = d.total_bytes.unwrap_or(0).saturating_sub(d.bytes_downloaded) as f64;
        let eta = (d.speed_bps > 1024.0).then(|| left / d.speed_bps);
        d.time_left_secs = match (d.time_left_secs, eta) {
            (Some(old), Some(new)) => Some(old * 0.8 + new * 0.2),
            (_, new) => new,
        };

        // One speed sample a second per network, a minute of them.
        self.since_sample += dt;
        if self.since_sample >= 1.0 {
            let secs = self.since_sample;
            self.since_sample = 0.0;
            for n in &d.networks {
                let sample = self.window.remove(&n.id).unwrap_or(0.0) / secs;
                let series = d.speed_history.entry(n.id.clone()).or_default();
                series.push(sample);
                if series.len() > SPEED_HISTORY_SECONDS {
                    series.remove(0);
                }
            }
            let len = d.speed_history.values().map(Vec::len).min().unwrap_or(0);
            if len >= 3 {
                let recent: f64 = (len - 3..len)
                    .map(|t| d.speed_history.values().map(|s| s[t]).sum::<f64>())
                    .sum::<f64>()
                    / 3.0;
                d.peak_speed_bps = Some(d.peak_speed_bps.unwrap_or(0.0).max(recent));
            }
        }
        delivered
    }
}

/// Gives stream `si` the next waiting block; with none waiting, has it race (hedge) the in-flight
/// block with the most left that nobody is racing yet.
fn lease(d: &mut DownloadState, si: usize) {
    let network = d.streams[si].network_id.clone();
    if let Some(block) = d.blocks.iter_mut().find(|b| b.status == BlockStatus::Pending) {
        block.status = BlockStatus::Downloading;
        block.network_id = Some(network);
        d.streams[si].block = Some(block.index);
        d.streams[si].hedge = false;
        return;
    }
    let raced: BTreeSet<BlockIndex> =
        d.streams.iter().filter(|s| s.hedge).filter_map(|s| s.block).collect();
    let slowest = d
        .blocks
        .iter()
        .filter(|b| b.status == BlockStatus::Downloading && !raced.contains(&b.index))
        .max_by_key(|b| b.range.len().unwrap_or(0).saturating_sub(b.bytes_downloaded))
        .map(|b| b.index);
    if let Some(index) = slowest {
        d.streams[si].block = Some(index);
        d.streams[si].hedge = true;
    }
}

/// Drops the streams `drop` picks, handing their unfinished blocks back to the queue.
fn release_streams(d: &mut DownloadState, drop: impl Fn(&StreamState) -> bool) {
    let mut freed = BTreeSet::new();
    d.streams.retain(|s| {
        let gone = drop(s);
        if gone && let Some(block) = s.block {
            freed.insert(block);
        }
        !gone
    });
    let still: BTreeSet<BlockIndex> = d.streams.iter().filter_map(|s| s.block).collect();
    for index in freed.difference(&still) {
        if let Some(block) = d.blocks.get_mut(*index as usize)
            && block.status == BlockStatus::Downloading
        {
            // A real backend keeps the bytes it has; so does this: the next stream continues.
            block.status = BlockStatus::Pending;
        }
    }
}

fn finished_from(d: &DownloadState) -> FinishedDownload {
    FinishedDownload {
        id: d.id,
        url: d.url.clone(),
        file_name: d.file_name.clone(),
        destination_path: d.destination_path.clone(),
        total_bytes: d.total_bytes.unwrap_or(d.bytes_downloaded),
        started_at: d.started_at,
        completed_at: d.completed_at.unwrap_or(d.started_at),
        total_paused_ms: d.total_paused_ms,
        networks: d.networks.clone(),
        speed_history: d.speed_history.clone(),
        peak_speed_bps: d.peak_speed_bps,
        peak_streams: d.peak_streams,
        blocks_written: d.blocks.len() as u32,
        from_browser: d.from_browser.clone(),
        missing: false,
    }
}

fn probe_delay(url: &str) -> Duration {
    Duration::from_millis(if url.contains("slow") { 3000 } else { 650 })
}

/// What the pretend server says about a link. Some words in the URL steer it, so every state of
/// New download can be tried: `404`/`missing`, `expired`/`403`, `norange` (no parallel parts),
/// `nosize` (no size), `slow` (takes three seconds).
pub fn probe(url: &str) -> Result<ProbeResult, String> {
    let url = url.trim();
    let lower = url.to_ascii_lowercase();
    let Some(rest) = lower.strip_prefix("https://").or_else(|| lower.strip_prefix("http://"))
    else {
        return Err("That isn't a web link. Links start with http:// or https://.".into());
    };
    if rest.is_empty() || !rest.contains('.') {
        return Err("That link has no server name in it.".into());
    }
    if lower.contains("404") || lower.contains("missing") {
        return Err("The server says there is no file at that address (404 Not Found).".into());
    }
    if lower.contains("403") || lower.contains("expired") {
        return Err("The server refused the link (403 Forbidden). It may have expired.".into());
    }
    let path = url.split(['?', '#']).next().unwrap_or(url);
    let last = path.rsplit('/').next().filter(|s| !s.is_empty() && !s.contains(':'));
    let name = last.map(|s| s.replace("%20", " ")).unwrap_or_else(|| "download".into());
    let size = 40 * (1 << 20) + hash(url) % (6 * GIB);
    let supports_ranges = !lower.contains("norange");
    Ok(ProbeResult {
        requested_url: url.into(),
        final_url: url.into(),
        supports_ranges,
        total_bytes: (!lower.contains("nosize")).then_some(size),
        suggested_file_name: name,
        content_type: Some("application/octet-stream".into()),
        etag: Some(format!("\"{:x}\"", hash(url))),
        last_modified: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    fn downloading(world: &World) -> Vec<&DownloadState> {
        world
            .sims
            .iter()
            .map(|s| &s.state)
            .filter(|d| d.status == DownloadStatus::Downloading)
            .collect()
    }

    #[test]
    fn probes_follow_the_url() {
        assert!(probe("ftp://x.com/a").is_err());
        assert!(probe("https://x.com/missing.zip").unwrap_err().contains("404"));
        let ok = probe("https://cdn.example.com/files/My%20File.iso?token=1").unwrap();
        assert_eq!(ok.suggested_file_name, "My File.iso");
        assert!(ok.supports_ranges && ok.total_bytes.is_some());
        assert!(!probe("https://x.com/norange.bin").unwrap().supports_ranges);
        assert_eq!(
            probe("https://x.com/a").unwrap().total_bytes,
            probe("https://x.com/a").unwrap().total_bytes
        );
    }

    #[test]
    fn block_sizes_stay_in_range() {
        assert_eq!(block_size_for(10), 1 << 20);
        assert_eq!(block_size_for(6_114_656_256), 4 << 20);
        assert_eq!(block_size_for(100 * GIB), 8 << 20);
    }

    #[test]
    fn downloads_progress_and_streams_grow() {
        let mut world = World::new(0);
        let iso = downloading(&world)[0].id;
        let before = world.sims[0].state.bytes_downloaded;
        for _ in 0..80 {
            world.step(0.25);
        }
        let d = &world.sims.iter().find(|s| s.state.id == iso).unwrap().state;
        assert!(d.bytes_downloaded > before);
        assert!(d.speed_bps > 0.0);
        // Bytes always add up: every block's frontier is the sum of what networks gave it.
        for b in &d.blocks {
            assert_eq!(b.bytes_by_network.values().sum::<u64>(), b.bytes_downloaded);
        }
        assert_eq!(d.networks.iter().map(|n| n.bytes_downloaded).sum::<u64>(), d.bytes_downloaded);
        // The allowance-exhausted cellular network carries nothing.
        let cell = d.networks.iter().find(|n| n.kind == NetworkKind::Cellular).unwrap();
        assert_eq!(cell.status, NetworkStatus::Limit);
        assert!(d.streams.iter().all(|s| s.network_id != cell.id));
        assert!(d.streams.len() > 24, "Auto grew past the starting 8 per network");
        assert!(d.speed_history.values().all(|s| s.len() == SPEED_HISTORY_SECONDS));
    }

    #[test]
    fn small_download_finishes_with_hedging_into_history() {
        let mut world = World::new(0);
        let (tx, mut rx) = oneshot::channel();
        let request = StartRequest {
            url: "https://x.com/small.zip".into(),
            destination_dir: PathBuf::from("C:/tmp"),
            file_name: "small.zip".into(),
            total_bytes: Some(40 << 20),
            supports_ranges: true,
            network_ids: vec![NetworkId::new("{wifi}"), NetworkId::new("{ethernet}")],
            streams_per_network: Some(4),
            etag: None,
            last_modified: None,
            browser: None,
        };
        // Free a slot for it.
        let busy: Vec<DownloadId> = downloading(&world).iter().map(|d| d.id).collect();
        for id in busy {
            world.handle(Command::Pause(id));
        }
        // Pausing promotes the queued one; pause it too.
        let queued: Vec<DownloadId> = downloading(&world).iter().map(|d| d.id).collect();
        for id in queued {
            world.handle(Command::Pause(id));
        }
        world.handle(Command::Start { request, link: None, reply: tx });
        let id = rx.try_recv().unwrap().unwrap();
        let mut hedged = false;
        let mut done = false;
        for _ in 0..200 {
            let events = world.step(0.25);
            if let Some(sim) = world.sims.iter().find(|s| s.state.id == id) {
                hedged |= sim.state.streams.iter().any(|s| s.hedge);
            }
            if events.iter().any(|e| matches!(e, Event::DownloadRemoved(r) if *r == id)) {
                done = true;
                break;
            }
        }
        assert!(done, "finished");
        assert!(hedged, "raced the last blocks");
        assert_eq!(world.history[0].id, id);
        assert_eq!(world.history[0].total_bytes, 40 << 20);
    }

    #[test]
    fn last_network_off_pauses_and_back_on_resumes() {
        let mut world = World::new(0);
        let d = downloading(&world)[1].clone();
        for n in d.networks.iter().filter(|n| n.enabled) {
            world.handle(Command::SetNetwork { id: d.id, network: n.id.clone(), enabled: false });
        }
        let state = |w: &World| w.sims.iter().find(|s| s.state.id == d.id).unwrap().state.clone();
        assert_eq!(state(&world).status, DownloadStatus::Paused);
        world.handle(Command::SetNetwork {
            id: d.id,
            network: NetworkId::new("{wifi}"),
            enabled: true,
        });
        assert_ne!(state(&world).status, DownloadStatus::Paused);
    }

    #[test]
    fn settings_are_validated() {
        let mut world = World::new(0);
        let (tx, mut rx) = oneshot::channel();
        let mut settings = AppSettings { downloads_at_once: 9, ..AppSettings::default() };
        settings.schedule.enabled = true;
        settings.schedule.days.clear();
        world.handle(Command::UpdateSettings { settings, reply: tx });
        let saved = rx.try_recv().unwrap().unwrap();
        assert_eq!(saved.downloads_at_once, 4);
        assert!(!saved.schedule.enabled, "an invalid schedule is switched off");
    }

    #[test]
    fn relink_revives_a_failed_download() {
        let mut world = World::new(0);
        let failed =
            world.sims.iter().find(|s| s.state.status == DownloadStatus::Error).unwrap().state.id;
        let (tx, mut rx) = oneshot::channel();
        world.handle(Command::Relink {
            id: failed,
            url: "https://x.com/expired.gguf".into(),
            reply: tx,
        });
        assert!(rx.try_recv().unwrap().is_err(), "a dead link is refused");
        let (tx, mut rx) = oneshot::channel();
        world.handle(Command::Relink {
            id: failed,
            url: "https://x.com/fresh.gguf".into(),
            reply: tx,
        });
        assert!(rx.try_recv().unwrap().is_ok());
        let d = &world.sims.iter().find(|s| s.state.id == failed).unwrap().state;
        assert!(matches!(d.status, DownloadStatus::Queued | DownloadStatus::Downloading));
        assert_eq!(d.url, "https://x.com/fresh.gguf");
    }
}
