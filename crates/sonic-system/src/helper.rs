//! `sonicnet-system.exe`: System mode as a small elevated process beside the unelevated app.
//!
//! ```text
//! sonicnet-system --parent-pid N --pipe \\.\pipe\NAME [--wintun PATH] [--sticky-secs N] [--mtu N]
//! ```
//!
//! The app launches it elevated (one UAC prompt) and creates the pipe it connects to. It takes
//! every IPv4 destination (`0.0.0.0/1` + `128.0.0.0/1`) into the Wintun adapter and spreads the
//! connections over the networks, as the `system_mode --all` prototype does. It reports over the
//! pipe ([`crate::protocol`]) and stops — routes deleted, adapter closed — on `stop`, when the pipe
//! closes, or the moment the app process ends however it ended. Should the helper itself die,
//! Windows removes the adapter and every route on it (docs/system-mode.md, "Crash safety").

use std::collections::HashMap;
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use ipstack::{IpStackStream, IpStackTcpStream, IpStackUdpStream};
use sonic_core::{NetworkId, NetworkInterface, NetworkKind};
use sonic_net::NetworkMonitor;
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient};

use crate::protocol::{self, AppMessage, HelperMessage, NetStats};
use crate::relay::{Meter, relay, relay_datagrams};
use crate::scheduler::{Candidate, Scheduler, relayable};
use crate::tun::{
    HostRoutes, InstanceLock, TUN_ADDRESS, TUN_PREFIX, adapter_exists, open_stack,
    purge_foreign_routes,
};
use crate::udp::bind_udp;

const ADAPTER: &str = "SonicNet";
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
/// About what home NATs allow an idle established connection (see the prototype).
const TCP_IDLE: Duration = Duration::from_secs(2 * 3600);
const UDP_IDLE: Duration = Duration::from_secs(60);
/// Errors cross the task boundary in tests (`tokio::spawn(run(..))`), so they must be Send.
type BoxError = Box<dyn Error + Send + Sync>;

const PIPE_PREFIX: &str = r"\\.\pipe\";

#[derive(Debug, PartialEq)]
pub struct Args {
    pub parent_pid: u32,
    pub pipe: String,
    pub wintun: PathBuf,
    /// How long a destination keeps its network after its last new connection (logins and
    /// streaming sites bind sessions to the client IP).
    pub sticky: Duration,
    pub mtu: u16,
}

pub fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Args, String> {
    let beside_exe = std::env::current_exe()
        .map(|exe| exe.with_file_name("wintun.dll"))
        .unwrap_or_else(|_| PathBuf::from("wintun.dll"));
    let (mut parent_pid, mut pipe) = (None, None);
    let mut parsed = Args {
        parent_pid: 0,
        pipe: String::new(),
        wintun: beside_exe,
        sticky: Duration::from_secs(120),
        mtu: 1500,
    };
    let mut it = args.into_iter();
    while let Some(arg) = it.next() {
        let mut value = || it.next().ok_or_else(|| format!("{arg} needs a value"));
        let number = |text: String| text.parse::<u64>().map_err(|e| format!("{arg}: {e}"));
        match arg.as_str() {
            "--parent-pid" => parent_pid = Some(number(value()?)? as u32),
            "--pipe" => pipe = Some(value()?),
            "--wintun" => parsed.wintun = value()?.into(),
            "--sticky-secs" => parsed.sticky = Duration::from_secs(number(value()?)?),
            "--mtu" => parsed.mtu = u16::try_from(number(value()?)?).map_err(|e| e.to_string())?,
            other => return Err(format!("unknown argument {other}")),
        }
    }
    parsed.parent_pid = parent_pid.ok_or("--parent-pid is required")?;
    let pipe = pipe.ok_or("--pipe is required")?;
    // Elevated, we open only what is plainly a local pipe, never an arbitrary path.
    if !pipe.starts_with(PIPE_PREFIX) || pipe.len() == PIPE_PREFIX.len() {
        return Err(format!("--pipe must be a local pipe name ({PIPE_PREFIX}…)"));
    }
    parsed.pipe = pipe;
    Ok(parsed)
}

pub fn main() -> ExitCode {
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(error) => {
            eprintln!("sonicnet-system: {error}");
            return ExitCode::from(2);
        }
    };
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("sonicnet-system: can't start the runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    let result = runtime.block_on(run(args));
    // Relays still running hold sockets only; dropping them here is fine.
    runtime.shutdown_timeout(Duration::from_millis(200));
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("sonicnet-system: {error}");
            ExitCode::FAILURE
        }
    }
}

/// Per-network counters: bytes for the display and the capacity estimate, open flows.
#[derive(Debug, Default)]
struct NetCounters {
    meter: Meter,
    tcp: AtomicU32,
    udp: AtomicU32,
}

/// What a network can carry, bytes a second, as far as we know.
#[derive(Debug, Default, Clone, Copy)]
struct Capacity {
    /// Peak throughput seen while relaying, decaying slowly. Only a lower bound: a network given
    /// few connections shows little.
    observed: f64,
    /// The last active probe ([`crate::probe`]): what it carries when it is the only one asked.
    probed: f64,
}

impl Capacity {
    /// What the scheduler weighs: the larger of the two, `None` until either says anything.
    fn value(self) -> Option<f64> {
        Some(self.observed.max(self.probed)).filter(|v| *v > 0.0)
    }
}

/// How often the network list is looked at for networks not yet probed.
const PROBE_CHECK: Duration = Duration::from_secs(5);

/// Whether a network may be probed at all. A probe downloads up to `probe::PROBE_BYTES` that the
/// user never asked for, so it is never spent on a link that is likely metered: a tethered phone
/// (USB) or a cellular modem. Those start with the unmeasured share and are judged by their real
/// traffic instead.
fn probe_allowed(net: &NetworkInterface) -> bool {
    !matches!(net.kind, NetworkKind::Usb | NetworkKind::Cellular)
}

struct Ctx {
    monitor: NetworkMonitor,
    scheduler: Mutex<Scheduler>,
    counters: Mutex<HashMap<NetworkId, Arc<NetCounters>>>,
    capacity: Mutex<HashMap<NetworkId, Capacity>>,
}

impl Ctx {
    /// Gateway networks, never our own adapter (it has no gateway, so the monitor skips it
    /// anyway; a loop through ourselves would be bad enough to check twice).
    fn networks(&self) -> Vec<NetworkInterface> {
        self.monitor.current().iter().filter(|n| n.name != ADAPTER).cloned().collect()
    }

    /// The network for a new connection to `dest`, and the destination's byte counters for its
    /// relay to fill (how the scheduler spots heavy destinations).
    fn pick(&self, dest: IpAddr, skip: &[NetworkId]) -> Option<(NetworkInterface, Arc<Meter>)> {
        let networks: Vec<NetworkInterface> =
            self.networks().into_iter().filter(|n| !skip.contains(&n.id)).collect();
        let capacity = self.capacity.lock().unwrap_or_else(PoisonError::into_inner);
        let candidates: Vec<Candidate> = networks
            .iter()
            .map(|n| Candidate {
                capacity: capacity.get(&n.id).and_then(|c| c.value()),
                ..Candidate::new(n.id.clone())
            })
            .collect();
        drop(capacity);
        let now = Instant::now();
        let mut scheduler = self.scheduler();
        let id = scheduler.pick(dest, now, &candidates)?;
        let meter = scheduler.meter_for(dest, now);
        Some((networks.into_iter().find(|n| n.id == id)?, meter))
    }

    fn scheduler(&self) -> std::sync::MutexGuard<'_, Scheduler> {
        self.scheduler.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn counters(&self, id: &NetworkId) -> Arc<NetCounters> {
        let mut counters = self.counters.lock().unwrap_or_else(PoisonError::into_inner);
        counters.entry(id.clone()).or_default().clone()
    }
}

async fn send(
    pipe: &mut (impl AsyncWrite + Unpin),
    message: &HelperMessage,
) -> std::io::Result<()> {
    pipe.write_all(protocol::line(message).as_bytes()).await?;
    pipe.flush().await
}

/// Opens the app's pipe, waiting a little while it is busy (only one client is ever expected).
async fn connect(name: &str) -> std::io::Result<NamedPipeClient> {
    const ERROR_PIPE_BUSY: i32 = 231;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match ClientOptions::new().open(name) {
            Err(e) if e.raw_os_error() == Some(ERROR_PIPE_BUSY) && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            other => return other,
        }
    }
}

pub async fn run(args: Args) -> Result<(), BoxError> {
    // First the parent: if the app is already gone, nothing may be set up for it.
    let mut parent_gone = crate::watch::process_exit(args.parent_pid)
        .map_err(|e| format!("the app (pid {}) isn't running: {e}", args.parent_pid))?;
    let pipe = connect(&args.pipe).await.map_err(|e| format!("can't reach the app: {e}"))?;
    let (from_app, mut to_app) = tokio::io::split(pipe);
    let mut from_app = BufReader::new(from_app).lines();
    send(&mut to_app, &HelperMessage::Starting).await?;

    if !args.wintun.is_file() {
        let reason = format!("wintun.dll is missing ({})", args.wintun.display());
        send(&mut to_app, &HelperMessage::Failed { reason: reason.clone() }).await?;
        return Err(reason.into());
    }
    let ctx = Arc::new(Ctx {
        monitor: NetworkMonitor::start(),
        scheduler: Mutex::new(Scheduler::new(args.sticky)),
        counters: Mutex::new(HashMap::new()),
        capacity: Mutex::new(HashMap::new()),
    });
    if ctx.networks().is_empty() {
        let reason = "No network with internet access".to_string();
        send(&mut to_app, &HelperMessage::Failed { reason: reason.clone() }).await?;
        return Err(reason.into());
    }

    // One instance at a time; holding the lock, a present adapter of our name belongs to some
    // other live program and is left alone (see `tun::adapter_exists`).
    let Some(_lock) = InstanceLock::acquire()? else {
        let reason = "System mode is already running for another copy of SonicNet".to_string();
        send(&mut to_app, &HelperMessage::Failed { reason: reason.clone() }).await?;
        return Err(reason.into());
    };
    if adapter_exists(ADAPTER)? {
        let reason = format!("Another program's adapter named {ADAPTER} is in the way");
        send(&mut to_app, &HelperMessage::Failed { reason: reason.clone() }).await?;
        return Err(reason.into());
    }
    let (if_index, mut stack) = match open_stack(ADAPTER, &args.wintun, args.mtu) {
        Ok(opened) => opened,
        Err(error) => {
            let reason = format!("Couldn't create the virtual adapter: {error}");
            send(&mut to_app, &HelperMessage::Failed { reason: reason.clone() }).await?;
            return Err(reason.into());
        }
    };
    match purge_foreign_routes(if_index) {
        Ok(stale) if !stale.is_empty() => eprintln!("removed stale routes {stale:?}"),
        Ok(_) => {}
        Err(error) => eprintln!("stale-route purge failed: {error}"),
    }
    let mut routes = HostRoutes::new(if_index);
    // From here on every exit path drops `routes` then `stack`: routes deleted, adapter gone.
    let result = async {
        for half in [Ipv4Addr::new(0, 0, 0, 0), Ipv4Addr::new(128, 0, 0, 0)] {
            routes.add_prefix(half, 1)?;
        }
        send(&mut to_app, &HelperMessage::On).await?;
        // Measured capacity from the start, rather than whatever the first connections show.
        let prober = tokio::spawn(probe_networks(ctx.clone()));
        let _stop_probing = AbortOnDrop(prober);
        serve(&ctx, &mut stack, &mut parent_gone, &mut from_app, &mut to_app).await
    }
    .await;
    if let Err(error) = &result {
        let reason = format!("System mode stopped: {error}");
        let _ = send(&mut to_app, &HelperMessage::Failed { reason }).await;
    }
    drop(routes);
    drop(stack);
    // Let the aborted stack task drop the device (closing the adapter) before we exit.
    tokio::time::sleep(Duration::from_millis(300)).await;
    result
}

async fn serve(
    ctx: &Arc<Ctx>,
    stack: &mut ipstack::IpStack,
    parent_gone: &mut tokio::sync::oneshot::Receiver<()>,
    from_app: &mut tokio::io::Lines<BufReader<tokio::io::ReadHalf<NamedPipeClient>>>,
    to_app: &mut (impl AsyncWrite + Unpin),
) -> Result<(), BoxError> {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut last = (Instant::now(), HashMap::<NetworkId, (u64, u64)>::new());
    loop {
        tokio::select! {
            _ = &mut *parent_gone => {
                eprintln!("the app ended: cleaning up");
                return Ok(());
            }
            line = from_app.next_line() => match line {
                Ok(Some(line)) => match protocol::parse::<AppMessage>(&line) {
                    Some(AppMessage::Stop) => return Ok(()),
                    None => eprintln!("ignored from the app: {line}"),
                },
                // The pipe closing means the app is gone or done with us.
                Ok(None) | Err(_) => {
                    eprintln!("the pipe closed: cleaning up");
                    return Ok(());
                }
            },
            _ = tick.tick() => {
                let networks = stats(ctx, &mut last);
                if send(to_app, &HelperMessage::Stats { networks }).await.is_err() {
                    return Ok(());
                }
            }
            accepted = stack.accept() => match accepted? {
                // Multicast/broadcast/link-local chatter Windows sends every adapter: dropped.
                s @ (IpStackStream::Tcp(_) | IpStackStream::Udp(_))
                    if !relayable(s.peer_addr().ip(), (TUN_ADDRESS, TUN_PREFIX)) => {}
                IpStackStream::Tcp(tcp) => { tokio::spawn(handle_tcp(tcp, ctx.clone())); }
                IpStackStream::Udp(udp) => { tokio::spawn(handle_udp(udp, ctx.clone())); }
                // ICMP and the rest are dropped (docs/system-mode.md, "ICMP").
                IpStackStream::UnknownTransport(_) | IpStackStream::UnknownNetwork(_) => {}
            },
        }
    }
}

/// Dials `dest` through the scheduler's pick; on failure tries the remaining networks.
async fn handle_tcp(mut app: IpStackTcpStream, ctx: Arc<Ctx>) {
    let dest = app.peer_addr();
    let mut failed: Vec<NetworkId> = Vec::new();
    while let Some((net, to_dest)) = ctx.pick(dest.ip(), &failed) {
        let host = dest.ip().to_string();
        match sonic_net::dial(&net, &host, dest.port(), false, DIAL_TIMEOUT).await {
            Ok(mut upstream) => {
                let counters = ctx.counters(&net.id);
                counters.tcp.fetch_add(1, Ordering::Relaxed);
                let meters = [&counters.meter, &*to_dest];
                let _ = relay(&mut app, &mut upstream, &meters, TCP_IDLE).await;
                counters.tcp.fetch_sub(1, Ordering::Relaxed);
                ctx.scheduler().finished(&net.id, dest.ip());
                return;
            }
            Err(error) => {
                tracing::debug!(%dest, network = %net.name, %error, "dial failed");
                let mut scheduler = ctx.scheduler();
                scheduler.finished(&net.id, dest.ip());
                scheduler.unstick(dest.ip());
                failed.push(net.id);
            }
        }
    }
}

async fn handle_udp(mut app: IpStackUdpStream, ctx: Arc<Ctx>) {
    let dest: SocketAddr = app.peer_addr();
    let Some((net, to_dest)) = ctx.pick(dest.ip(), &[]) else { return };
    if let Ok(socket) = bind_udp(&net, dest) {
        let counters = ctx.counters(&net.id);
        counters.udp.fetch_add(1, Ordering::Relaxed);
        let meters = [&counters.meter, &*to_dest];
        let _ = relay_datagrams(&mut app, &socket, &meters, UDP_IDLE).await;
        counters.udp.fetch_sub(1, Ordering::Relaxed);
    }
    ctx.scheduler().finished(&net.id, dest.ip());
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Probes each network once — when System mode starts, or when the network first appears — one at
/// a time so each is measured alone. Never again after that: a periodic re-probe would quietly
/// download gigabytes a day. From then on a network's capacity follows the traffic it actually
/// carries (see `Capacity::observed`).
async fn probe_networks(ctx: Arc<Ctx>) {
    let mut probed: std::collections::HashSet<NetworkId> = std::collections::HashSet::new();
    loop {
        for net in ctx.networks() {
            if probed.contains(&net.id) || !probe_allowed(&net) {
                continue;
            }
            probed.insert(net.id.clone());
            match crate::probe::probe(&net).await {
                Ok(rate) => {
                    eprintln!("probe {}: {:.2} MB/s", net.name, rate / 1e6);
                    let mut capacity = ctx.capacity.lock().unwrap_or_else(PoisonError::into_inner);
                    capacity.entry(net.id.clone()).or_default().probed = rate;
                }
                // Not fatal: the network keeps its observed capacity (or the unmeasured share).
                Err(error) => eprintln!("probe {} failed: {error}", net.name),
            }
        }
        tokio::time::sleep(PROBE_CHECK).await;
    }
}

/// Every network's last second; also updates the capacity estimates.
fn stats(ctx: &Ctx, last: &mut (Instant, HashMap<NetworkId, (u64, u64)>)) -> Vec<NetStats> {
    let elapsed = last.0.elapsed().as_secs_f64().max(0.001);
    last.0 = Instant::now();
    ctx.networks()
        .into_iter()
        .map(|net| {
            let counters = ctx.counters(&net.id);
            let down = counters.meter.down.load(Ordering::Relaxed);
            let up = counters.meter.up.load(Ordering::Relaxed);
            let (prev_down, prev_up) = last.1.insert(net.id.clone(), (down, up)).unwrap_or((0, 0));
            let (down_rate, up_rate) = (
                down.saturating_sub(prev_down) as f64 / elapsed,
                up.saturating_sub(prev_up) as f64 / elapsed,
            );
            let capacity_bps;
            {
                let mut capacity = ctx.capacity.lock().unwrap_or_else(PoisonError::into_inner);
                let estimate = capacity.entry(net.id.clone()).or_default();
                // Peak hold with a slow decay: one busy second sets it, a quiet minute lets a
                // network that got slower lose weight.
                estimate.observed = (down_rate + up_rate).max(estimate.observed * 0.99);
                capacity_bps = estimate.value().unwrap_or(0.0);
            }
            NetStats {
                id: net.id.clone(),
                name: net.name.clone(),
                kind: net.kind,
                tcp: counters.tcp.load(Ordering::Relaxed),
                udp: counters.udp.load(Ordering::Relaxed),
                down_bps: down_rate.round() as u64,
                up_bps: up_rate.round() as u64,
                down_total: down,
                up_total: up,
                capacity_bps: capacity_bps.round() as u64,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metered_links_are_never_probed() {
        let mut net = sonic_net::loopback();
        for (kind, allowed) in [
            (NetworkKind::Ethernet, true),
            (NetworkKind::Wifi, true),
            (NetworkKind::Usb, false),
            (NetworkKind::Cellular, false),
        ] {
            net.kind = kind;
            assert_eq!(probe_allowed(&net), allowed, "{kind:?}");
        }
    }

    fn args(list: &[&str]) -> Result<Args, String> {
        parse_args(list.iter().map(|s| s.to_string()))
    }

    #[test]
    fn arguments() {
        let parsed = args(&[
            "--parent-pid",
            "42",
            "--pipe",
            r"\\.\pipe\SonicNet.System.1",
            "--wintun",
            r"C:\x\wintun.dll",
        ])
        .unwrap();
        assert_eq!(parsed.parent_pid, 42);
        assert_eq!(parsed.wintun, PathBuf::from(r"C:\x\wintun.dll"));
        assert_eq!(parsed.sticky, Duration::from_secs(120));
        assert!(args(&["--pipe", r"\\.\pipe\x"]).is_err(), "parent required");
        assert!(args(&["--parent-pid", "1"]).is_err(), "pipe required");
        assert!(args(&["--parent-pid", "1", "--pipe", r"C:\Windows\x"]).is_err(), "not a pipe");
        assert!(args(&["--parent-pid", "1", "--pipe", r"\\.\pipe\"]).is_err(), "empty name");
        assert!(args(&["--parent-pid", "x", "--pipe", r"\\.\pipe\x"]).is_err());
        assert!(args(&["--parent-pid", "1", "--pipe", r"\\.\pipe\x", "--all"]).is_err());
    }

    /// The whole conversation up to the point that needs admin rights: a helper whose wintun.dll
    /// is missing connects, says `starting`, then `failed` with the reason, and exits.
    #[tokio::test]
    async fn reports_a_failure_over_the_pipe() {
        use tokio::io::AsyncBufReadExt;
        use tokio::net::windows::named_pipe::ServerOptions;

        let name = format!(r"\\.\pipe\SonicNet.System.test.{}", std::process::id());
        let server = ServerOptions::new().first_pipe_instance(true).create(&name).unwrap();
        let helper = tokio::spawn(run(Args {
            parent_pid: std::process::id(),
            pipe: name.clone(),
            wintun: PathBuf::from(r"C:\no\such\wintun.dll"),
            sticky: Duration::ZERO,
            mtu: 1500,
        }));
        server.connect().await.unwrap();
        let mut lines = BufReader::new(server).lines();
        let first = lines.next_line().await.unwrap().unwrap();
        assert_eq!(protocol::parse(&first), Some(HelperMessage::Starting));
        let second = lines.next_line().await.unwrap().unwrap();
        match protocol::parse(&second) {
            Some(HelperMessage::Failed { reason }) => {
                assert!(reason.contains("wintun.dll"), "{reason}")
            }
            other => panic!("expected failed, got {other:?}"),
        }
        assert!(helper.await.unwrap().is_err());
        assert_eq!(lines.next_line().await.unwrap(), None, "the helper hung up");
    }
}
