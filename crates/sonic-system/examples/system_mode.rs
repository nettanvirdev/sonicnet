//! System mode prototype: balances new TCP connections and UDP flows to chosen destinations
//! across every network with a gateway, through a Wintun adapter named "SonicNet".
//!
//! Needs an elevated (administrator) terminal and `wintun.dll` beside the executable (or
//! `--wintun PATH`). See docs/system-mode.md, "Running the prototype".
//!
//! ```text
//! system_mode [--sticky-secs N] [--mtu N] [--seconds N] [--stop-file PATH] [--wintun PATH]
//!             [--all] [HOST_OR_IPV4...]
//! ```
//!
//! The given destinations are routed into the adapter (/32 routes); `--all` routes every IPv4
//! destination through two /1 routes, leaving the physical default routes and the LAN's own routes
//! untouched. Routes and the adapter are removed on exit, Ctrl+C, `--seconds` or when the stop
//! file appears — and by Windows itself if the process dies.

#[cfg(not(windows))]
fn main() {
    eprintln!("system_mode runs on Windows only (it needs Wintun).");
}

#[cfg(windows)]
fn main() -> std::process::ExitCode {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("could not start the runtime: {error}");
            return std::process::ExitCode::FAILURE;
        }
    };
    match runtime.block_on(windows::run()) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(windows)]
mod windows {
    use std::collections::HashMap;
    use std::error::Error;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, PoisonError};
    use std::time::{Duration, Instant};

    use ipstack::{IpStackStream, IpStackTcpStream, IpStackUdpStream};
    use sonic_core::{NetworkId, NetworkInterface};
    use sonic_net::NetworkMonitor;
    use sonic_system::relay::{Meter, relay, relay_datagrams};
    use sonic_system::scheduler::{Candidate, Scheduler, relayable};
    use sonic_system::tun::{
        HostRoutes, InstanceLock, TUN_ADDRESS, TUN_PREFIX, adapter_exists, open_stack,
        purge_foreign_routes,
    };
    use sonic_system::udp::bind_udp;

    const ADAPTER: &str = "SonicNet";
    const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
    /// A TCP connection with no byte either way for this long is dropped: about what home NATs
    /// allow an idle established connection, so we're no stricter than the path already is.
    const TCP_IDLE: Duration = Duration::from_secs(2 * 3600);
    /// UDP has no close; a flow ends after this much silence (QUIC idles out sooner itself).
    const UDP_IDLE: Duration = Duration::from_secs(60);

    struct Args {
        targets: Vec<String>,
        sticky: Duration,
        mtu: u16,
        seconds: Option<u64>,
        stop_file: Option<PathBuf>,
        wintun: PathBuf,
        /// Test only: run without the single-instance lock, so a test can leave an adapter behind
        /// that a normal second run must treat as stale.
        skip_lock: bool,
        /// Take over every IPv4 destination (`0.0.0.0/1` + `128.0.0.0/1`), not just the given ones.
        all: bool,
    }

    fn parse_args() -> Result<Args, Box<dyn Error>> {
        let beside_exe = std::env::current_exe()?.with_file_name("wintun.dll");
        let mut args = Args {
            targets: Vec::new(),
            sticky: Duration::ZERO,
            mtu: 1500,
            seconds: None,
            stop_file: None,
            wintun: beside_exe,
            skip_lock: false,
            all: false,
        };
        let mut it = std::env::args().skip(1);
        while let Some(arg) = it.next() {
            let mut value = || it.next().ok_or_else(|| format!("{arg} needs a value"));
            match arg.as_str() {
                "--sticky-secs" => args.sticky = Duration::from_secs(value()?.parse()?),
                "--mtu" => args.mtu = value()?.parse()?,
                "--seconds" => args.seconds = Some(value()?.parse()?),
                "--stop-file" => args.stop_file = Some(value()?.into()),
                "--wintun" => args.wintun = value()?.into(),
                "--test-skip-instance-lock" => args.skip_lock = true,
                "--all" => args.all = true,
                flag if flag.starts_with("--") => return Err(format!("unknown flag {flag}").into()),
                _ => args.targets.push(arg),
            }
        }
        if args.targets.is_empty() && !args.all {
            return Err("give --all, or at least one destination host or IPv4 address".into());
        }
        Ok(args)
    }

    /// Per-network counters for the display and the capacity estimate.
    #[derive(Default)]
    struct NetStats {
        meter: Meter,
        tcp: AtomicU64,
        udp: AtomicU64,
    }

    struct Ctx {
        monitor: NetworkMonitor,
        scheduler: Mutex<Scheduler>,
        stats: Mutex<HashMap<NetworkId, Arc<NetStats>>>,
        /// Peak observed throughput (bytes/s) per network, decaying slowly: the capacity the
        /// scheduler weighs by.
        capacity: Mutex<HashMap<NetworkId, f64>>,
    }

    impl Ctx {
        /// Gateway networks, never our own adapter (it has no gateway so the monitor already
        /// skips it, but a loop through ourselves would be bad enough to check twice).
        fn networks(&self) -> Vec<NetworkInterface> {
            self.monitor.current().iter().filter(|n| n.name != ADAPTER).cloned().collect()
        }

        fn pick(&self, dest: IpAddr, skip: &[NetworkId]) -> Option<(NetworkInterface, Arc<Meter>)> {
            let networks: Vec<NetworkInterface> =
                self.networks().into_iter().filter(|n| !skip.contains(&n.id)).collect();
            let capacity = self.capacity.lock().unwrap_or_else(PoisonError::into_inner);
            let candidates: Vec<Candidate> = networks
                .iter()
                .map(|n| Candidate {
                    capacity: capacity.get(&n.id).copied(),
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

        fn stats(&self, id: &NetworkId) -> Arc<NetStats> {
            let mut stats = self.stats.lock().unwrap_or_else(PoisonError::into_inner);
            stats.entry(id.clone()).or_default().clone()
        }
    }

    pub async fn run() -> Result<(), Box<dyn Error>> {
        let args = parse_args()?;
        if !args.wintun.is_file() {
            return Err(format!("wintun.dll not found at {}", args.wintun.display()).into());
        }

        // Resolve before any route exists, through the system's normal DNS.
        let mut targets: Vec<Ipv4Addr> = Vec::new();
        for target in &args.targets {
            for addr in tokio::net::lookup_host((target.as_str(), 0)).await? {
                if let IpAddr::V4(ip) = addr.ip()
                    && !targets.contains(&ip)
                {
                    targets.push(ip);
                }
            }
        }
        if targets.is_empty() && !args.all {
            return Err("no IPv4 address found for the destinations (IPv6 isn't routed yet)".into());
        }

        let ctx = Arc::new(Ctx {
            monitor: NetworkMonitor::start(),
            scheduler: Mutex::new(Scheduler::new(args.sticky)),
            stats: Mutex::new(HashMap::new()),
            capacity: Mutex::new(HashMap::new()),
        });
        let networks = ctx.networks();
        if networks.is_empty() {
            return Err("no network with a gateway".into());
        }
        for n in &networks {
            println!("network {} (if {}): {:?}", n.name, n.ipv4_index, n.addresses);
        }

        // One instance at a time. Holding the lock, an adapter of our name can't be a System
        // mode instance's, and a dead one's is already gone (see `adapter_exists`): it belongs to
        // some other live process, so stay away from it.
        let _lock = if args.skip_lock {
            None
        } else {
            Some(InstanceLock::acquire()?.ok_or("System mode is already running")?)
        };
        let started = Instant::now();
        if !args.skip_lock && adapter_exists(ADAPTER)? {
            return Err(format!(
                "an adapter named {ADAPTER} exists and belongs to another running program; \
                 not touching it"
            )
            .into());
        }
        println!("startup: no adapter named {ADAPTER} present");

        let (if_index, mut stack) = open_stack(ADAPTER, &args.wintun, args.mtu)?;
        println!("adapter {ADAPTER} up: {TUN_ADDRESS}/24, interface {if_index}, mtu {}", args.mtu);
        let stale_routes = purge_foreign_routes(if_index)?;
        println!("startup: removed {} stale route(s) {stale_routes:?}", stale_routes.len());
        println!("startup took {} ms", started.elapsed().as_millis());
        let mut routes = HostRoutes::new(if_index);
        // From here on, every exit path drops `routes` then `stack`: routes deleted, adapter gone.
        let result = async {
            for &ip in &targets {
                routes.add(ip)?;
                println!("route {ip}/32 -> {ADAPTER}");
            }
            if args.all {
                // Every IPv4 destination; the LAN keeps its own, more specific routes.
                for half in [Ipv4Addr::new(0, 0, 0, 0), Ipv4Addr::new(128, 0, 0, 0)] {
                    routes.add_prefix(half, 1)?;
                    println!("route {half}/1 -> {ADAPTER}");
                }
            }
            serve(&ctx, &mut stack, &args).await
        }
        .await;
        drop(routes);
        drop(stack);
        // Let the aborted stack task drop the device (closing the adapter) before we exit.
        tokio::time::sleep(Duration::from_millis(500)).await;
        print_totals(&ctx);
        println!("routes removed, adapter closed");
        result
    }

    async fn serve(
        ctx: &Arc<Ctx>,
        stack: &mut ipstack::IpStack,
        args: &Args,
    ) -> Result<(), Box<dyn Error>> {
        let deadline = args.seconds.map(|s| tokio::time::Instant::now() + Duration::from_secs(s));
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let mut last = (Instant::now(), HashMap::<NetworkId, (u64, u64)>::new());
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    println!("Ctrl+C: cleaning up");
                    return Ok(());
                }
                () = async {
                    match deadline {
                        Some(at) => tokio::time::sleep_until(at).await,
                        None => std::future::pending().await,
                    }
                } => {
                    println!("time limit reached: cleaning up");
                    return Ok(());
                }
                _ = tick.tick() => {
                    report(ctx, &mut last);
                    if args.stop_file.as_ref().is_some_and(|f| f.exists()) {
                        println!("stop file found: cleaning up");
                        return Ok(());
                    }
                }
                accepted = stack.accept() => match accepted? {
                    // Multicast/broadcast/link-local chatter Windows sends every adapter: dropped.
                    s @ (IpStackStream::Tcp(_) | IpStackStream::Udp(_))
                        if !relayable(s.peer_addr().ip(), (TUN_ADDRESS, TUN_PREFIX)) => {}
                    IpStackStream::Tcp(tcp) => { tokio::spawn(handle_tcp(tcp, ctx.clone())); }
                    IpStackStream::Udp(udp) => { tokio::spawn(handle_udp(udp, ctx.clone())); }
                    IpStackStream::UnknownTransport(other) => {
                        // ICMP and the rest are dropped in the prototype (docs: "ICMP").
                        tracing::debug!(protocol = ?other.ip_protocol(), dst = %other.dst_addr(), "dropped");
                    }
                    IpStackStream::UnknownNetwork(_) => {}
                },
            }
        }
    }

    /// Dials `dest` through the scheduler's pick; on failure tries the remaining networks.
    async fn handle_tcp(mut app: IpStackTcpStream, ctx: Arc<Ctx>) {
        let (src, dest) = (app.local_addr(), app.peer_addr());
        let mut failed: Vec<NetworkId> = Vec::new();
        while let Some((net, to_dest)) = ctx.pick(dest.ip(), &failed) {
            let host = dest.ip().to_string();
            match sonic_net::dial(&net, &host, dest.port(), false, DIAL_TIMEOUT).await {
                Ok(mut upstream) => {
                    let stats = ctx.stats(&net.id);
                    stats.tcp.fetch_add(1, Ordering::Relaxed);
                    println!("tcp {src} -> {dest} via {}", net.name);
                    let started = Instant::now();
                    let result =
                        relay(&mut app, &mut upstream, &[&stats.meter, &to_dest], TCP_IDLE).await;
                    ctx.scheduler().finished(&net.id, dest.ip());
                    let secs = started.elapsed().as_secs_f64();
                    match result {
                        Ok(m) => println!(
                            "tcp {src} -> {dest} via {} closed: {} B down, {} B up, {secs:.1} s",
                            net.name, m.down, m.up
                        ),
                        Err(e) => println!("tcp {src} -> {dest} via {} ended: {e}", net.name),
                    }
                    return;
                }
                Err(error) => {
                    println!("tcp {src} -> {dest}: dial via {} failed: {error}", net.name);
                    let mut scheduler = ctx.scheduler();
                    scheduler.finished(&net.id, dest.ip());
                    scheduler.unstick(dest.ip());
                    failed.push(net.id);
                }
            }
        }
        println!("tcp {src} -> {dest}: no network could reach it");
    }

    async fn handle_udp(mut app: IpStackUdpStream, ctx: Arc<Ctx>) {
        let (src, dest): (SocketAddr, SocketAddr) = (app.local_addr(), app.peer_addr());
        let Some((net, to_dest)) = ctx.pick(dest.ip(), &[]) else {
            println!("udp {src} -> {dest}: no network");
            return;
        };
        let result = match bind_udp(&net, dest) {
            Ok(socket) => {
                let stats = ctx.stats(&net.id);
                stats.udp.fetch_add(1, Ordering::Relaxed);
                println!("udp {src} -> {dest} via {} (local {:?})", net.name, socket.local_addr());
                relay_datagrams(&mut app, &socket, &[&stats.meter, &to_dest], UDP_IDLE).await
            }
            Err(error) => Err(error),
        };
        ctx.scheduler().finished(&net.id, dest.ip());
        match result {
            Ok(m) => println!(
                "udp {src} -> {dest} via {} closed: {} B down, {} B up",
                net.name, m.down, m.up
            ),
            Err(e) => println!("udp {src} -> {dest} via {} ended: {e}", net.name),
        }
    }

    /// One line per network each second; also updates the capacity estimates.
    fn report(ctx: &Ctx, last: &mut (Instant, HashMap<NetworkId, (u64, u64)>)) {
        let elapsed = last.0.elapsed().as_secs_f64().max(0.001);
        last.0 = Instant::now();
        let mut line = String::new();
        for net in ctx.networks() {
            let stats = ctx.stats(&net.id);
            let down = stats.meter.down.load(Ordering::Relaxed);
            let up = stats.meter.up.load(Ordering::Relaxed);
            let (prev_down, prev_up) = last.1.insert(net.id.clone(), (down, up)).unwrap_or((0, 0));
            let rate = (down - prev_down + up - prev_up) as f64 / elapsed;
            {
                let mut capacity = ctx.capacity.lock().unwrap_or_else(PoisonError::into_inner);
                let estimate = capacity.entry(net.id.clone()).or_insert(0.0);
                // Peak hold with a slow decay: one busy second sets it, a quiet minute lets a
                // network that got slower lose weight.
                *estimate = rate.max(*estimate * 0.99);
            }
            let open = ctx.scheduler().open_on(&net.id);
            line.push_str(&format!(
                " | {}: open {open}, tcp {}, udp {}, down {:.2} MB/s, total {:.1} MB",
                net.name,
                stats.tcp.load(Ordering::Relaxed),
                stats.udp.load(Ordering::Relaxed),
                (down - prev_down) as f64 / elapsed / 1e6,
                down as f64 / 1e6,
            ));
        }
        println!("stats{line}");
    }

    fn print_totals(ctx: &Ctx) {
        let stats = ctx.stats.lock().unwrap_or_else(PoisonError::into_inner);
        for net in ctx.networks() {
            if let Some(s) = stats.get(&net.id) {
                println!(
                    "total {}: tcp {}, udp {}, down {} B, up {} B",
                    net.name,
                    s.tcp.load(Ordering::Relaxed),
                    s.udp.load(Ordering::Relaxed),
                    s.meter.down.load(Ordering::Relaxed),
                    s.meter.up.load(Ordering::Relaxed),
                );
            }
        }
    }
}
