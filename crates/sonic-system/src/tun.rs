//! The virtual adapter (Wintun, through `tun-rs`) and the routes that send traffic into it.
//!
//! [`open_stack`] creates the adapter and puts `ipstack`, the user-space TCP/IP stack, on it: every
//! TCP connection and UDP flow an app opens through the adapter comes out of
//! [`ipstack::IpStack::accept`] as a stream. [`HostRoutes`] steers chosen destinations into the
//! adapter and takes them out again when dropped.

use std::io;
use std::net::Ipv4Addr;
use std::path::Path;
use std::pin::Pin;
use std::process::Command;
use std::task::{Context, Poll, ready};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// The adapter's own address. A private /24 nothing else on a home network uses, so its
/// on-link route can't shadow a real LAN.
pub const TUN_ADDRESS: Ipv4Addr = Ipv4Addr::new(10, 71, 0, 1);
pub const TUN_PREFIX: u8 = 24;

/// A fixed adapter GUID. Windows keys an interface's identity (LUID) and its persistent settings
/// on it, so every run gets the same interface, and anything an earlier run (or another tool)
/// left attached to it is found again by [`purge_foreign_routes`].
const ADAPTER_GUID: u128 = 0x5c0f_1c4e_7a2b_4d19_9e63_51a8_0b7c_d2e4;

/// How long the stack keeps a TCP connection with no traffic. ipstack's default (60 s) would reset
/// idle SSH sessions, WebSockets and long polls; this is longer than the relay's own idle limit
/// so the relay, which counts both directions, decides.
const STACK_TCP_IDLE: std::time::Duration = std::time::Duration::from_secs(3 * 3600);

/// Creates the Wintun adapter `name` (needs admin) with [`TUN_ADDRESS`], loading `wintun_dll`,
/// and starts the stack on it. Returns the adapter's interface index and the stack.
///
/// The stack owns the adapter: dropping the stack ends its task, which closes the Wintun adapter
/// and so removes it from the system together with every route through it.
pub fn open_stack(name: &str, wintun_dll: &Path, mtu: u16) -> io::Result<(u32, ipstack::IpStack)> {
    let device = tun_rs::DeviceBuilder::new()
        .name(name)
        .description("SonicNet System mode")
        .wintun_file(wintun_dll.to_string_lossy().into_owned())
        .device_guid(ADAPTER_GUID)
        .ipv4(TUN_ADDRESS, TUN_PREFIX, None)
        .mtu(mtu)
        .build_async()?;
    let if_index = device.if_index()?;
    let mut config = ipstack::IpStackConfig::default();
    config.mtu(mtu).map_err(io::Error::other)?;
    let mut tcp = ipstack::TcpConfig::default();
    tcp.timeout = STACK_TCP_IDLE;
    config.with_tcp_config(tcp);
    // QUIC and DNS flows: a minute of silence ends the flow (our relay has its own idle timer).
    config.udp_timeout(std::time::Duration::from_secs(60));
    Ok((if_index, ipstack::IpStack::new(config, TunIo(device))))
}

/// `tun-rs`'s device speaks `poll_recv`/`poll_send` (one packet each); `ipstack` wants
/// `AsyncRead`/`AsyncWrite` with one packet per read and per write, which this maps one to one.
struct TunIo(tun_rs::AsyncDevice);

impl AsyncRead for TunIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let n = ready!(self.0.poll_recv(cx, buf.initialize_unfilled()))?;
        buf.advance(n);
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for TunIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.poll_send(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// /32 routes for single destinations through the adapter. Only in the *active* store, never
/// persistent, so a reboot forgets them even if nothing else cleans up; they also vanish with the
/// adapter. Dropping this deletes them.
#[derive(Debug)]
pub struct HostRoutes {
    if_index: u32,
    added: Vec<(Ipv4Addr, u8)>,
}

impl HostRoutes {
    pub fn new(if_index: u32) -> Self {
        Self { if_index, added: Vec::new() }
    }

    /// Sends traffic for `ip` into the adapter. A /32 beats any default route by prefix length,
    /// whatever the metrics. On-link (no next hop): Wintun is layer 3, there is nothing to ARP.
    pub fn add(&mut self, ip: Ipv4Addr) -> io::Result<()> {
        self.add_prefix(ip, 32)
    }

    /// Sends every destination in `net/prefix_len` into the adapter, in the active store only
    /// (gone on reboot, and with the adapter when its process dies). The whole-internet takeover
    /// is the two halves `0.0.0.0/1` and `128.0.0.0/1`: each beats the physical `0.0.0.0/0`
    /// default routes by prefix length without editing them, while every more specific route —
    /// the LAN's own subnets, the router — still wins over them and stays on the physical adapter.
    /// Our own upstream sockets are pinned with `IP_UNICAST_IF`, which restricts their route
    /// lookup to their physical adapter, so they never loop back in here.
    pub fn add_prefix(&mut self, net: Ipv4Addr, prefix_len: u8) -> io::Result<()> {
        netsh(&[
            "interface",
            "ipv4",
            "add",
            "route",
            &format!("prefix={net}/{prefix_len}"),
            &format!("interface={}", self.if_index),
            "store=active",
        ])?;
        self.added.push((net, prefix_len));
        Ok(())
    }

    pub fn routes(&self) -> &[(Ipv4Addr, u8)] {
        &self.added
    }
}

impl Drop for HostRoutes {
    fn drop(&mut self) {
        for (net, prefix_len) in self.added.drain(..) {
            let deleted = netsh(&[
                "interface",
                "ipv4",
                "delete",
                "route",
                &format!("prefix={net}/{prefix_len}"),
                &format!("interface={}", self.if_index),
                "store=active",
            ]);
            if let Err(error) = deleted {
                // Not fatal: closing the adapter removes it too.
                tracing::warn!(%net, prefix_len, %error, "could not delete route");
            }
        }
    }
}

/// Holds the machine-wide "System mode is running" mutex. A second instance must not touch the
/// adapter or routes of a live one. The OS releases the mutex when the process dies, however it
/// dies, so a crashed run never blocks the next.
#[derive(Debug)]
pub struct InstanceLock(windows_sys::Win32::Foundation::HANDLE);

// SAFETY: a mutex handle is a kernel object reference, usable from any thread.
unsafe impl Send for InstanceLock {}

impl InstanceLock {
    /// `None` when another instance holds it.
    pub fn acquire() -> io::Result<Option<Self>> {
        Self::acquire_named(r"Global\SonicNetSystemMode")
    }

    /// [`acquire`](Self::acquire) under another name; tests use their own, so a System mode
    /// running on the developer's machine doesn't hold theirs.
    fn acquire_named(name: &str) -> io::Result<Option<Self>> {
        use windows_sys::Win32::Foundation::{CloseHandle, ERROR_ALREADY_EXISTS, GetLastError};
        use windows_sys::Win32::System::Threading::CreateMutexW;

        let name: Vec<u16> = name.encode_utf16().chain([0]).collect();
        // SAFETY: `name` is a NUL-terminated UTF-16 string that outlives the call; null security
        // attributes mean the default descriptor.
        let handle = unsafe { CreateMutexW(std::ptr::null(), 0, name.as_ptr()) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: reads this thread's last error, set by CreateMutexW just above.
        if unsafe { GetLastError() } == ERROR_ALREADY_EXISTS {
            // SAFETY: `handle` is the valid handle CreateMutexW returned; we close our reference.
            unsafe { CloseHandle(handle) };
            return Ok(None);
        }
        Ok(Some(Self(handle)))
    }
}

impl Drop for InstanceLock {
    fn drop(&mut self) {
        // SAFETY: the handle came from CreateMutexW and is closed exactly once, here.
        unsafe { windows_sys::Win32::Foundation::CloseHandle(self.0) };
    }
}

/// Whether a network adapter called `name` is present.
///
/// Wintun removes an adapter the moment its owning process exits, killed or not (measured: gone
/// 33 ms after `Stop-Process -Force`), and after a power loss only a non-present "phantom" device
/// record remains, which `tun-rs` recognises and creates over. So while we hold the
/// [`InstanceLock`], a *present* adapter of our name belongs to some other live process. It must
/// be left alone: removing a Wintun adapter while its owner still holds it (`pnputil
/// /remove-device`) was tried live and deadlocked with that owner's exit for over a minute,
/// leaving its routes black-holing traffic meanwhile.
pub fn adapter_exists(name: &str) -> io::Result<bool> {
    Ok(Command::new("netsh")
        .args(["interface", "show", "interface", &format!("name={name}")])
        .output()?
        .status
        .success())
}

/// Deletes routes on interface `if_index` that this run didn't add: anything in the persistent
/// store (System mode never writes there, so it's from an older build or another tool, and would
/// come back after every reboot) and manually added active routes. Call before adding ours.
/// Returns a line per route removed.
pub fn purge_foreign_routes(if_index: u32) -> io::Result<Vec<String>> {
    let script = r#"
        $failed = 0
        foreach ($store in 'PersistentStore', 'ActiveStore') {
            $routes = @(Get-NetRoute -InterfaceIndex {IF} -PolicyStore $store -ErrorAction SilentlyContinue)
            # In the active store only manual (NetMgmt) routes; the rest are the adapter's own.
            if ($store -eq 'ActiveStore') { $routes = @($routes | Where-Object Protocol -eq 'NetMgmt') }
            foreach ($r in $routes) {
                try {
                    $r | Remove-NetRoute -Confirm:$false -ErrorAction Stop
                    "$store $($r.DestinationPrefix)"
                } catch { "FAILED $store $($r.DestinationPrefix): $_"; $failed = 1 }
            }
        }
        exit $failed
    "#
    .replace("{IF}", &if_index.to_string());
    Ok(powershell(&script)?.lines().map(str::to_owned).collect())
}

fn powershell(script: &str) -> io::Result<String> {
    let output = Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", script])
        .output()?;
    let said = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if output.status.success() {
        Ok(said)
    } else {
        let err = String::from_utf8_lossy(&output.stderr);
        Err(io::Error::other(format!("powershell failed: {said} {}", err.trim())))
    }
}

/// Runs `netsh` and turns a failure into an error carrying what it printed (netsh reports
/// errors on stdout).
fn netsh(args: &[&str]) -> io::Result<()> {
    let output = Command::new("netsh").args(args).output()?;
    if output.status.success() {
        Ok(())
    } else {
        let said = String::from_utf8_lossy(&output.stdout);
        Err(io::Error::other(format!("netsh {}: {}", args.join(" "), said.trim())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // These run the real PowerShell/netsh commands; reading needs no admin rights.

    #[test]
    fn purge_on_an_interface_with_no_foreign_routes_succeeds() {
        assert_eq!(purge_foreign_routes(999_999).unwrap(), Vec::<String>::new());
    }

    #[test]
    fn adapter_presence_is_detected() {
        assert!(!adapter_exists("SonicNet-test-no-such-adapter").unwrap());
        if let Some(real) = sonic_net::list_interfaces().unwrap().first() {
            assert!(adapter_exists(&real.name).unwrap(), "{}", real.name);
        }
    }

    #[test]
    fn instance_lock_is_exclusive() {
        // Its own name, so a System mode running on this machine doesn't hold it.
        let name = format!(r"Local\SonicNetSystemModeTest.{}", std::process::id());
        let first = InstanceLock::acquire_named(&name).unwrap();
        assert!(first.is_some());
        assert!(InstanceLock::acquire_named(&name).unwrap().is_none());
        drop(first);
        assert!(InstanceLock::acquire_named(&name).unwrap().is_some());
    }
}
