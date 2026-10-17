# System mode

Download mode speeds up one file by splitting it into byte ranges across networks. System mode
does something else: it takes **every app's traffic** (browsers, game launchers, Steam, `git`)
and spreads **whole connections** over the user's networks. It needs no server.

Without a server on the far side, one TCP connection can't be split across two ISPs. The server
sees one source IP per connection, and a stream that changed IP mid-flight would be a different
connection. So System mode balances **per connection** (and, for UDP, per flow). An app that opens
many connections (browsers, download managers, Steam, cloud sync) gets the sum of the links. One
single-stream download still runs at one link's speed. Bonding a single stream needs a relay
server (Speedify-style), which is a possible later phase.

Code: `crates/sonic-system` (scheduler, relay, pinned UDP, TUN + stack) and the prototype
`crates/sonic-system/examples/system_mode.rs`.

## Architecture

```
  apps (Firefox, Steam, curl …)
        │  ordinary sockets
        ▼
  Windows TCP/IP ── routing table ──► physical adapters (LAN, VPN, anything not routed to us)
        │ routes that point at the virtual adapter
        ▼
  Wintun adapter "SonicNet" 10.71.0.1/24 ── raw IP packets ──┐
                                                             ▼
  ┌──────────────────────── sonic-system (user space) ───────────────────────────┐
  │ tun-rs AsyncDevice ⇄ TunIo ⇄ ipstack (user-space TCP/IP)                     │
  │        │ accept(): one stream per TCP connection / UDP flow                  │
  │        ▼                                                                     │
  │ relayable(dest)? ── no (multicast, broadcast, link-local, own subnet) → drop │
  │        │ yes                                                                 │
  │ Scheduler.pick(dest) ── stickiness · eligibility · weight by capacity        │
  │        │ network N                                                           │
  │ TCP: sonic_net::dial(N, dest ip, port, secure=false)                         │
  │ UDP: udp::bind_udp(N, dest)                                                  │
  │        │   (IP_UNICAST_IF = N + bind N's address)                            │
  │ relay / relay_datagrams (both directions, half-close, idle timeout, meters)  │
  └───────────────┬────────────────────────────────────────────┬─────────────────┘
                  ▼                                            ▼
            Ethernet → ISP A                             Wi-Fi → ISP B
```

TLS stays end to end between the app and the server. We relay raw TCP bytes to the original
destination IP:port, so we never see plaintext and never need a certificate.

## Scheduling (`scheduler.rs`, pure and unit-tested)

For each new TCP connection or UDP flow, in this order:

1. **Eligible networks only.** A network the user turned off, or one at its limit (a data cap from
   `sonic-engine`, or a connection cap), gets nothing new. A network that dropped is simply missing
   from the list `NetworkMonitor` publishes.
2. **Stickiness per destination IP, sliding TTL.** A destination that went out through network N
   keeps going through N while it keeps being used (`sticky_ttl` after the last new connection;
   the helper uses 2 minutes). Logins, banks and streaming sites bind sessions to the client IP.
   Flipping between ISPs mid-session logs people out, trips fraud checks or breaks DRM. With 0
   stickiness is off. Two limits, because **System mode must never make anyone slower than their
   best single network**:
   - **Only between comparable networks.** If the sticky network's capacity is below 25 %
     (`COMPARABLE`) of the best eligible network's, the destination is placed again by capacity and
     sticks to its new network. The trade-off: a login made over a much slower network can see its
     IP change once, when the measurement shows that network is slow. That is one move, to the
     faster network, after which it sticks again; the alternative (the live bug below) was every
     connection of a speed test or a video stream stuck on a network 12× slower.
   - **Heavy destinations aren't sticky; they cover every usable network.** See "Heavy
     destinations" below. The destination keeps its sticky network for when it turns light again.
3. **Failover.** When the sticky network becomes ineligible (dropped, off, at its limit), the
   destination is placed again and sticks to its new network. Connections already open on a dead
   network die with it, and the apps reconnect, which lands them on a live one. A dial that fails while
   the network still looks up calls `unstick`, and the example retries on the remaining networks.
4. **Weighted by measured capacity.** A smooth weighted round robin: at each pick every network
   earns credit in proportion to its capacity, the one with the most credit is chosen and pays one
   round. New connections interleave in exact proportion (Ethernet 11.5 MB/s, Wi-Fi 0.9 MB/s: about
   13 of every 14 to Ethernet, and Wi-Fi still adds its share). Open connections are deliberately
   not counted: an earlier `(open + 1) / capacity` rule let a network's idle keep-alives make it
   look busy, and live it sent all 8 parallel downloads to the slower Wi-Fi because Ethernet held
   10 idle connections.
   Capacity is the larger of two measurements (`helper.rs`):
   - an **active probe** (`probe.rs`): once per network — when System mode starts or the network first appears — and never on a tethered phone or cellular link, each network in turn
     downloads from `speed.cloudflare.com` (or, if that refuses, thinkbroadband) over a socket pinned
     to it, 4 connections at once, for at most 3 s and 10 MB in all.
     Time to first byte is excluded. One at a time, so each is measured alone.
   - the **observed peak** while relaying (bytes a second, decaying ×0.99 per second). On its own
     this misled: it is only a lower bound, and a network that happened to get few connections
     looked slow and got even fewer, while a slow network that got the first bulk looked fast.

   An unmeasured network counts as 25 % (`UNMEASURED`) of the best measured one: a small share,
   so it gets measured without being trusted. All count as equal while nothing is measured. The
   choice is deterministic, which keeps it testable and avoids random clumping. Each network's
   capacity is reported to the app in the stats (`capacity_bps`).

### Heavy destinations (`Scheduler::pick`, `HEAVY_BYTES`)

**The bug.** FDM downloading a Hugging Face CDN file through System mode ran at 11.4 MB/s, all on
Ethernet (Wi-Fi showed 36 KB/s), while SonicNet's own downloader reached 12.3 MB/s (Ethernet 11.3 +
Wi-Fi 1.03) on the same file. FDM's handful of connections all went to one CDN IP. The first one
made Ethernet that IP's sticky network, stickiness kept the next ones there, and the old bulk rule
(spread from the 5th open connection) never fired. Even had it fired, capacity weighting at
11.5 : 1 (92 : 8) puts two or three more connections on Ethernet before Wi-Fi's turn comes.

**The rule.** A destination IP is *heavy* when its connections received **8 MB (`HEAVY_BYTES`)
within 2 minutes (`HEAVY_WINDOW`)**, or when it has 4 (`BULK_OPEN`) connections open at once. Every
relay adds the bytes it moves to the destination's counter as well as to its network's
(`Scheduler::meter_for` hands the relay the counter), so the scheduler reads the bytes live at the
moment a connection is placed: a downloader's very next connection after the crossing is spread,
not one a second later. When the mark is older than the window, the same average rate counts
(8 MB per 2 minutes, about 67 KB/s, over the whole time since), so a download whose next connection
comes minutes later is still heavy, and a web app trickling all day never is. Once heavy, it stays
so for a window after the last crossing.

A heavy destination's new connections go:
1. first to each **usable** network that holds none of its connections yet, the fastest first.
   Usable means eligible (on, not at a limit, not failed for this connection) with capacity at
   least **5 % (`FLOOR`) of the best**. Wi-Fi at 1 MB/s against Ethernet at 11.5 MB/s is 8.7 %, so
   it gets one;
2. then by capacity-weighted round robin over the usable networks only.

A network below the floor never gets a heavy destination's connection: one connection on a nearly
dead link would hold its byte range back for the whole download. Light destinations may still use
it for its small weighted share. Connections already open can't move; only new ones are placed by
this rule. Why 8 MB: a web page, a login or an API session pulls far less from one IP in two minutes
(a heavy page is 2 to 5 MB spread over many hosts), while a download at 11 MB/s passes it in under
a second. The connection count stays as a second trigger because a downloader that opens all its
connections at once places them before any byte has arrived.

**The trade-off.** Light destinations (web pages, logins, APIs) keep stickiness: one IP, one
network, for the window, so sites don't see the client IP change. A heavy destination's connections
come from two IPs at once. That is what download managers, launchers, CDNs and video segments want,
and their servers don't tie anything to the client IP. Stickiness is per IP, and the IPs that serve
bulk bytes (CDN edges, Netflix's caches, Steam's content servers) are rarely the IPs that hold a
login session or a licence. The risk is a site that serves a session and 8 MB+ from the same IP and
checks the IP of every connection; we accept it, as the old bulk rule did for 5+ connections.

**The fast.com bug** (fixed by capacity probing and the comparable rule). Before capacity probing and the two stickiness limits, fast.com
measured 610 kbit/s through System mode with Ethernet idle: its servers' IPs had been placed on
Wi-Fi (then 0.9 MB/s against Ethernet's 11.5 MB/s, measured pinned) while nothing was measured,
the observed-peak capacity then favoured Wi-Fi, and 5-minute stickiness held every later
connection there. **Per-app rules** (an app always uses
network X, or is excluded from System mode) are a future item. They need the owning process of
each flow, which the user-space stack doesn't see. The likely source is `GetExtendedTcpTable` /
`GetExtendedUdpTable` lookups by local port (10.71.0.1:port), or a WFP callout later.

## DNS

**Decision: let DNS ride the tunnel like any other UDP/TCP flow. No hijacking.** When the TUN holds
the default route (full mode), queries to the user's resolvers (router, ISP, 1.1.1.1) are ordinary
UDP/TCP port-53 flows. The scheduler places them like everything else, through a socket pinned to
one network.

Why not hijack port 53 and resolve ourselves over pinned sockets:
- Browsers increasingly use DNS over HTTPS (Firefox's TRR, Chrome's Secure DNS). That is plain TCP/UDP
  443 to a resolver, which we can't and shouldn't intercept. It already works transparently as one
  more flow. A hijack would only ever see the legacy resolver path anyway.
- A hijack means a resolver implementation, caching, DNSSEC/EDNS edge cases, and answering for
  split-horizon/VPN names correctly. That is a lot of surface for little gain.
- The ISP-locality problem (a CDN answer computed for ISP A's resolver but used over ISP B) is
  mild in practice: CDNs mostly answer per region, not per ISP. Stickiness already keeps a
  destination on one network.

Caveat: the router's resolver (192.168.x.1) is on the LAN, so it's never routed into the TUN (the LAN
route is more specific) and resolves through whichever adapter owns that LAN. That is fine. A
resolver that answers only on one ISP (an ISP's own DNS) then shows up as a dial or answer failure
on the other ISP. Pinning port-53 flows to the network whose DHCP-provided resolver they target
fixes that. It's a small rule to add when we implement full mode.

Verified live: 16 `nslookup` queries to routed resolvers 1.1.1.1 and 9.9.9.9 went through the
tunnel as 16 UDP flows, spread 9 Ethernet / 7 Wi-Fi, and all were answered.

## UDP / QUIC, IPv6, ICMP

- **UDP is per flow** (src ip:port, dst ip:port). Each flow gets its own socket pinned to the
  chosen network (`udp::bind_udp`: `IP_UNICAST_IF` plus a bind to the adapter's address), connected
  to the destination. A flow ends after 60 s of silence. QUIC (HTTP/3) is UDP 443 and works the same
  way. QUIC's connection migration can't move a flow between our networks (the server would see a
  new path), and that's fine: we keep a flow on its network. The prototype relays UDP and DNS was
  verified. HTTP/3 itself wasn't tested, because the test PC's curl 8.21 (Schannel) has no HTTP/3.
- **IPv6:** the prototype routes only IPv4. Full mode adds a ULA address (e.g. `fd71::1/64`) to the
  adapter, takes the v6 default route with `::/1` + `8000::/1`, and dials over networks that have
  global v6. A destination whose family the chosen network lacks is placed on another network
  (`DialError::NoCompatibleRoute` / `AddrNotAvailable` → retry). If no network has v6, return
  ICMPv6 unreachable fast so Happy Eyeballs falls back to v4 at once. Until then, don't take v6
  routes at all, so v6 keeps going out natively.
- **ICMP:** a socket can't relay raw ICMP without admin raw sockets. The prototype drops it, so
  `ping` to a routed host fails. Plan: answer echo by doing a real `IcmpSendEcho2Ex` from the chosen
  network's source address, then synthesise the reply on the TUN (ipstack hands ICMP over as
  `UnknownTransport`, with a `send` for replies). Path-MTU "fragmentation needed" messages never come
  back through us, and they don't need to. See MTU.

## LAN and multicast exclusions

- **LAN traffic never enters the TUN.** Each physical adapter's own subnet routes (192.168.0.0/24 on-link)
  are more specific than any default-route takeover, so printers, NAS and the router stay direct.
- **Multicast, broadcast, link-local and the TUN's own subnet are dropped** (`scheduler::relayable`).
  Windows treats the virtual adapter as one more LAN and sends it SSDP (239.255.255.250), LLMNR
  (224.0.0.252, ff02::1:3), mDNS (224.0.0.251, ff02::fb) and NetBIOS broadcasts. The first live run,
  before this filter existed, showed exactly that chatter being relayed onto the physical networks.
  Full mode should also turn off NetBIOS over TCP/IP and LLMNR on the adapter so the chatter isn't
  generated at all.
- **RFC 1918 and CGNAT ranges** other than the attached LANs (another site over a VPN, Docker/WSL
  networks) are covered by their own more-specific routes. A /1 takeover doesn't touch them.

## Route setup

**Prototype:** one active-store /32 per destination given on the command line. It never touches the
default route:

```
netsh interface ipv4 add route prefix=<ip>/32 interface=<SonicNet ifindex> store=active
```

**Full mode:** two half-default routes on the adapter, `0.0.0.0/1` and `128.0.0.0/1`, the same
trick WireGuard and OpenVPN use. They beat the physical adapters' `0.0.0.0/0` routes by prefix
length without deleting or editing them. The physical default routes, gateways, metrics and LAN routes
stay exactly as they were, so removing our two routes (or the adapter) restores the old routing at
once. IPv6: `::/1` + `8000::/1`. All of these are active-store only.

**Why our own upstream traffic doesn't loop back into the TUN:** every upstream socket is created with
`IP_UNICAST_IF` (or `IPV6_UNICAST_IF`) set to the physical adapter's index *before* `connect` and is
bound to that adapter's address. With a unicast interface set, Windows chooses the route *within
that interface's* routes: the adapter's own `0.0.0.0/0` via its gateway. The /1 (or /32) routes
belong to another interface and are never considered. So packets from `sonic_net::dial` and `bind_udp` leave
on Ethernet or Wi-Fi even though the routing table as a whole says "SonicNet" for that
destination.

Verified live: with `80.249.99.148/32 → SonicNet` installed, a plain `curl` used source
10.71.0.1 (it entered the TUN). The relay's upstream sockets left from 192.168.0.10 (Ethernet)
and 192.168.0.20 (Wi-Fi), and 296 MB were relayed with no loop. The adapter is excluded from the
network list anyway: it has no gateway, so `list_interfaces` skips it, and the example also filters
it by name.

## Crash safety ("if SonicNet dies, normal routing must come back")

The design rule is: **nothing we change may outlive the process unless the OS removes it with the
process.**

- **The adapter dies with the process.** Wintun creates the adapter as a software device tied to
  the creating handle. When the process exits for any reason, including `Stop-Process -Force`, a crash
  or Task Manager, the OS closes the handle and removes the adapter. After a power loss only a
  non-present "phantom" device record remains. `tun-rs` recognises it and creates over it (run 4 started
  cleanly with such a phantom present).
- **Routes are bound to the adapter and only in the active store.** Interface-bound routes vanish
  with their interface. Active-store routes never survive a reboot. We never write the persistent
  store.
- **Startup cleanup** (`tun.rs`):
  1. A machine-wide named mutex (`Global\SonicNetSystemMode`) allows one instance. The OS releases it
     on process death, so a crashed run never blocks the next.
  2. Holding the mutex, if an adapter named SonicNet is still *present*, it belongs to another live
     program. We refuse to start and don't touch it. Removing a Wintun adapter whose owner is still
     alive (`pnputil /remove-device`) was tried live: it deadlocked with the owner's exit for over a
     minute, and the stale /32 kept black-holing that IP the whole time.
  3. After creating the adapter (it has a fixed GUID, so Windows sees the same interface every run), we
     delete every persistent-store route on it and every manually added active route. That removes
     anything an older build or another tool attached.
- **Clean shutdown** (Ctrl+C, stop file, time limit, error): delete our routes, drop the stack
  (closing the adapter), wait 500 ms, exit.

**Measured (live, Ethernet + Wi-Fi, Windows 11 26200).** t = 0 at the hard kill or at the stop
signal; "physical curl" means a 1 KiB request to the previously routed IP succeeded from a
192.168.0.x source.

| Event | Adapter gone | Route gone | Physical curl OK |
|---|---|---|---|
| Hard kill, run 3 (`Stop-Process -Force`) | 33 ms | 117 ms | yes (1 MiB curl done at 2032 ms) |
| Hard kill, run 4 | 345 ms | 261 ms | 848 ms |
| Hard kill of a lock-less "foreign owner", run 4 | 46 ms | 221 ms | 698 ms |
| Clean exit via stop file, run 4 (≤1 s polling + 0.5 s) | 1733 ms | 1817 ms | 2298 ms |

A planted persistent route (`203.0.113.77/32` on SonicNet, `store=persistent`) did survive a hard
kill. Windows kept it in the persistent store, as expected. The next start removed it ("removed 2
stale route(s) [PersistentStore 203.0.113.77/32, ActiveStore 203.0.113.77/32]"). A second
instance is refused in 85 ms ("System mode is already running"). A start while another program's
SonicNet adapter is present is refused, and that program keeps working (its curl still went through
the tunnel).

**Full mode adds a watchdog.** When the UI and the System-mode engine are separate processes (see
Admin rights), the elevated helper owns the adapter and routes, and it watches the UI process's handle.
When the UI exits it tears down. If the helper itself dies, the adapter and routes die with it, as
measured above. A kill switch ("block traffic if SonicNet dies") is the opposite of what users want
here and is deliberately not offered. Failing open to normal routing is the requirement.

## Admin rights and elevation

Creating a Wintun adapter and changing routes needs administrator rights. The options:

- **Run the whole app elevated:** simplest, but it's wrong for a UI app. Every download, the browser
  bridge and file dialogs would run as admin, and drag-and-drop from non-elevated apps breaks (UIPI).
- **Recommended: a small elevated helper.** It is installed once as a Windows service (one UAC prompt at
  install time), and it does only adapter, route and relay work. The UI talks to it over a named pipe
  whose ACL admits only the interactive user, with a tiny command set: start(networks, policy), stop,
  stats. The relay and stack run in the service, so their traffic is pinned there. The UI stays
  unprivileged. Variant: a per-session helper launched elevated on demand with one UAC prompt per
  enable, which needs no service install but prompts more often.

### What the app does (per-session helper)

The app ships the per-session variant: `sonicnet-system.exe` (code in `sonic_system::helper`, its
`main` in `app/src/bin/` so `cargo build -p sonicnet` builds it beside `sonicnet.exe`).

- **Start.** With the `system_mode` setting on (the default), the app creates a named pipe
  (`\\.\pipe\SonicNet.System.<pid>.<nonce>`, first instance only, local clients only) and runs
  `sonicnet-system --parent-pid <app pid> --pipe <name> --wintun <dir>\wintun.dll` through
  `ShellExecuteExW` with the `runas` verb: one UAC prompt per app start. It checks that the pipe's
  client is the process it launched. Declining the prompt (`ERROR_CANCELLED`) shows "Needs
  permission"; any failure shows an error. Either way System mode is off for the session and
  downloads are unaffected; Settings → System-wide offers "Try again". Toggling the setting starts
  or stops the helper.
- **Protocol** (`sonic_system::protocol`, JSON lines). Helper → app: `{"type":"starting"}`,
  `{"type":"on"}`, `{"type":"failed","reason":…}`, and every second
  `{"type":"stats","networks":[{id,name,kind,tcp,udp,down_bps,up_bps,down_total,up_total}]}`.
  App → helper: `{"type":"stop"}`.
- **Stop.** On `stop`, when the pipe closes, or when the app process ends however it ends (the
  helper waits on a `SYNCHRONIZE` handle to it), the helper deletes its routes, closes the adapter
  and exits. Quitting the app sends `stop` and waits up to 3 s for the helper, alongside the
  backend's bounded save.
- The helper does what `system_mode --all` does, with 2-minute stickiness and the capacity probe,
  and the same single-instance mutex, foreign-adapter refusal and stale-route purge.

## Coexistence

- **VPNs.** Full-tunnel VPNs (WireGuard, OpenVPN, corporate) also take `/1` routes or the default
  route, and their adapters have gateways. Rules:
  1. Never offer a VPN adapter as an upstream unless the user explicitly selects it.
  2. If another adapter already holds /1 or default routes with a better metric, System mode doesn't
     start and tells the user, rather than fighting over the table.
  3. Split-tunnel VPN routes are more specific than /1 and keep working untouched.

  Traffic we relay leaves pinned to a physical adapter and therefore **bypasses** a full-tunnel VPN,
  so the two must not run together silently.
- **Captive portals.** Hotel or airport Wi-Fi intercepts until the user logs in. Before using a network,
  probe it through a pinned socket (`http://www.msftconnecttest.com/connecttest.txt` must return
  "Microsoft Connect Test"). A network failing the probe is ineligible (`at_limit`/`enabled=false`)
  and gets no flows. Windows' own captive-portal browser keeps working, because the probe target
  is resolved and fetched by Windows itself.
- **Other TUN apps** (Tailscale, ZeroTier) use their own subnets and specific routes. They don't collide.

## MTU

The TUN MTU is 1500 (`--mtu`). That isn't a wire MTU. Our stack terminates every TCP connection,
and the real sockets on Ethernet and Wi-Fi do their own segmentation and path-MTU discovery, so app
TCP never fragments on the wire whatever the TUN says. A bigger TUN MTU (Wintun accepts up to 65535)
means fewer, larger packets through user space, which saves CPU for TCP. UDP is different: apps size
their datagrams from the interface MTU, and a 9000-byte QUIC datagram relayed onto a 1500-byte path
would be fragmented or dropped. Keep 1500 (or 1280 for v6-safe) while UDP is relayed. A split
(large MTU for TCP, MSS clamp for UDP) is a later optimisation only if CPU becomes the limit.

## Performance

Measured with Ethernet and a weaker Wi-Fi link on separate internet lines. The early runs below
predate the capacity probe.

| Test (8 parallel curl range requests) | Direct (default route, Ethernet) | Through System mode |
|---|---|---|
| 8 × 16 MiB fixed ranges, run 3 | 68.4 Mbit/s, 15.7 s | 45.9 Mbit/s, 23.4 s* |
| 15 s timed, 8 × up-to-48 MiB, run 3 | 78.3 Mbit/s | 84.9 Mbit/s |
| SHA-256 of the 128 MiB (run 3) and 32 MiB (run 4) | reference | identical |

\* Fixed equal ranges finish only when the slowest connection does, and here the Wi-Fi share was
slower (about 2–4 MB/s against Ethernet's 8–13 MB/s). The timed test is the fair comparison: the
tunnel matched or beat direct. The expected total is close to the sum of the links, minus stack
overhead, as long as placement follows real capacity.

**Stack CPU:** 6.27 s of process CPU to relay about 296 MB (run 3, release build, one process,
prints on every connection), or about 21 ms per MB. That works out to roughly 50 MB/s (400 Mbit/s)
per core of overhead. ipstack is a simple stack: it copies each segment through unbounded channels,
has no SACK and uses a fixed window. Expect a few hundred Mbit/s per core, enough for typical home
dual-WAN but not for 2 × 1 Gbit/s. If that matters later, move to smoltcp with tuned buffers or
batch the Wintun ring I/O.

## Choosing the stack

| | ipstack 1.0.1 + tun-rs 2.8.12 | smoltcp 0.14 | netstack-smoltcp 0.2.4 |
|---|---|---|---|
| API | `accept()` gives tokio `AsyncRead/Write` streams per TCP connection and per UDP flow | poll-based socket set; we would write the accept loop, buffers, timers and wakeups ourselves | smoltcp wrapped in tokio streams (listener + UDP socket) |
| Windows/Wintun | device from `tun-rs` (Wintun), with a 30-line adapter (`TunIo`) | needs the same device glue plus a full driver loop | device glue similar; less used on Windows |
| Maturity / users | used by tun2proxy; 37k recent downloads | very mature, embedded-grade TCP | 122k recent downloads, younger API |
| Effort to a working relay | smallest: it worked end to end on first live run | largest | medium |

**Pick: ipstack + tun-rs.** It is the shortest path to real per-connection relaying with tokio
streams that plug straight into `relay` and `relay_datagrams`. `tun-rs` handles Wintun loading,
adapter creation, addressing and MTU. Its gaps were found live and are handled or documented:
- ipstack doesn't wake a pending reader when a connection closes. Without a fix our relay only saw
  the close at ipstack's 60 s idle timer. `relay` now re-polls every second (`NUDGE`). In run 3 all 17
  TCP relays had logged "closed" within the 4 s the test waited after the last curl exited.
- ipstack's default TCP idle (60 s) would reset idle SSH, WebSocket and long-poll connections. We set
  it to 3 h; our relay's own idle limit is 2 h, about what home NATs allow.
- ipstack has no half-close from the app side: an app's FIN makes it close both directions once its
  send queue is empty, so a server's reply after the client's FIN can be cut. Our `relay` preserves
  half-close (tested). Rare in practice (HTTP/1.1, TLS and QUIC don't rely on it), but it is a
  reason to revisit smoltcp.
- ipstack completes the TCP handshake with the app *before* we dial upstream. An unreachable
  destination shows as "connected, then reset" instead of "connection refused/timed out". smoltcp
  would let us dial first and answer the SYN after.

## Wintun DLL and licence

Fetched only from the official site: the https://www.wintun.net/ page links
`builds/wintun-0.14.1.zip` and publishes SHA2-256
`07c256185d6ee3652e09fa55c0b673e2624b565e02c4b9091c79ca7d2f24ef51`. The download matched it. The
amd64 `wintun.dll` has a valid Authenticode signature by "WireGuard LLC" (DigiCert EV Code Signing CA)
and SHA-256 `E5DA8447DC2C320EDC0FC52FA01885C103DE8C118481F683643CACC3220DAFCE`. Layout:

```
target-system/wintun-download/wintun-0.14.1.zip      the download
target-system/wintun-download/extracted/wintun/...   unzipped (LICENSE.txt, bin/<arch>/wintun.dll, include/)
target-system/release/examples/wintun.dll            amd64 DLL copied where the example loads it
```

`target-system/` is a build directory and isn't committed. The example loads `wintun.dll` from beside
its exe, or from `--wintun PATH`.

**For the app** the same amd64 DLL and its `LICENSE.txt` are kept in `vendor/wintun/` (git-ignored:
copy them there from the verified zip, `extracted/wintun/bin/amd64/wintun.dll` and
`extracted/wintun/LICENSE.txt`, and check the DLL's SHA-256 above). `app/build.rs` copies the DLL
beside the exes cargo builds, and the installer ships it with its licence as `wintun-LICENSE.txt`.
Without it the build still works and System mode reports the DLL missing.

**Licence.** Wintun's *source* is GPL-2.0. The *prebuilt signed DLLs* come under a separate
"Prebuilt Binaries License" (`LICENSE.txt` in the zip, WireGuard LLC). The wintun.net page calls the
signed DLLs the "only supported way of distributing Wintun". On redistribution, clause 3(d)
forbids redistributing the Software without written consent, except when it is "distributed
alongside other software that uses the Software only via the Permitted API". The Permitted API is
the `wintun.h` interface (clause 3(b)). Other obligations: don't modify, reverse engineer or extract
from the DLL (3(a), 3(b)); keep its notices (3(c)); don't use the WireGuard or Wintun names to
endorse our product (3(e)). The licence terminates on non-compliance (6).

For SonicNet (MIT), this means we can ship the unmodified signed `wintun.dll` next to our exe, because we
call it only through its C API (via `tun-rs`). The DLL stays under its own licence, not MIT. Include
its `LICENSE.txt` in the installer, and don't name WireGuard or Wintun in marketing beyond factual
attribution.

## Running the prototype (elevated terminal)

```powershell
# Build (normal terminal is fine)
$env:CARGO_TARGET_DIR = 'target-system'
cargo build --release -p sonic-system --example system_mode
# wintun.dll beside the exe (from the verified zip above)
Copy-Item target-system\wintun-download\extracted\wintun\bin\amd64\wintun.dll `
          target-system\release\examples\

# Run as Administrator from the repo root. Routes ONLY these destinations; Ctrl+C removes routes and adapter.
.\target-system\release\examples\system_mode.exe --sticky-secs 0 `
    ipv4.download.thinkbroadband.com 1.1.1.1

# In a second terminal: 8 parallel ranges; watch the "stats |" lines split them across networks
$ip = ([Net.Dns]::GetHostAddresses('ipv4.download.thinkbroadband.com') | ? AddressFamily -eq InterNetwork)[0]
0..7 | % { $s = $_ * 16MB; Start-Process curl.exe -NoNewWindow -ArgumentList `
    "-s -o NUL --resolve ipv4.download.thinkbroadband.com:80:$ip -r $s-$($s + 16MB - 1) http://ipv4.download.thinkbroadband.com/512MB.zip" }
```

Flags: `--sticky-secs N` (0 = spread every connection), `--mtu N`, `--seconds N` (exit after N s),
`--stop-file PATH` (exit when it appears), `--wintun PATH`. `--test-skip-instance-lock` is for
tests only. The example prints one line per connection (`tcp 10.71.0.1:port -> ip:port via Wi-Fi`) and
a `stats |` line per second with open/total connections, MB/s and total MB per network.

The full scripted live test (direct vs. tunnel bytes and speed, UDP, stickiness, hard-kill and
clean-exit recovery, stale-route purge, single instance, foreign adapter) is
`target-system/live-test/run-live-test.ps1`. Run it elevated. It logs to
`target-system/live-test/run/live-test.log` and always cleans up in `finally`.

## Risks

- **ipstack edge cases** (see "Choosing the stack"): no app-side half-close; the handshake is completed
  before the upstream dial; modest throughput per core.
- **Sites that dislike IP changes** across connections: stickiness mitigates this, but sites that pin
  a session across *different* hostnames or IPs (SSO flows that hop between auth and app domains) can
  still see two IPs. A per-site-group stickiness (by registrable domain via SNI or DNS) may be needed.
- **One stream stays one link:** single-connection downloads, video calls and games get no
  aggregation, only placement.
- **ISP-specific resolvers and CDN locality:** see DNS.
- **VPN bypass:** relayed traffic skips a full-tunnel VPN unless we detect it and refuse (see
  Coexistence).
- **Security of the helper:** an elevated service that changes routes is a privilege boundary. It
  needs a minimal pipe protocol and a strict ACL.
- **Wintun licence:** redistribution is allowed only alongside software using the Permitted API, and
  the terms can change at WireGuard LLC's discretion (clause 8).
- **Startup speed:** the stale-route purge shells out to PowerShell (0.7–2.7 s measured). Replace it
  with IP Helper calls (`GetIpForwardTable2`, `DeleteIpForwardEntry2`) in the product.

## Phased plan

1. **Prototype (done):** scheduler, relay, pinned UDP, Wintun + ipstack, /32 routes for chosen hosts,
   live-verified spreading, byte-exact relay, stickiness, failover-ready, crash recovery.
2. **Engine in a service:** an elevated helper service with the pipe API. It takes networks, policy and
   limits from `sonic-manager`/`sonic-engine` (data caps → `at_limit`), and the UI gets a System mode toggle
   and per-network stats. Route changes and cleanup move to IP Helper calls. Captive-portal probing.
3. **Full mode:** `0.0.0.0/1` + `128.0.0.0/1` takeover, VPN detection and refusal, adapter
   NetBIOS/LLMNR off, the sticky default plus a spread list, and a watchdog on the UI process.
4. **IPv6 and ICMP:** ULA on the adapter, `::/1` + `8000::/1`, a family-aware scheduler, and an ICMP echo proxy.
5. **Per-app rules:** owning-process lookup per flow; include/exclude/pin-to-network rules.
6. **Optional relay (Speedify-style):** a server-side bonder for single-stream aggregation, opt-in.

## Changes wanted in sonic-net (not done here: other agents own that crate)

- Export a pinned UDP helper (`bind_udp(iface, dest) -> UdpSocket`) or the private
  `pin_to_interface`. `sonic-system/src/udp.rs` duplicates it for now (marked `ponytail:`).
- Optional: a `dial_addr(iface, SocketAddr, timeout)` that skips the host-string round trip and
  the per-host route cache, for relays that already have an IP.
