# Using two (or more) internet connections on Windows

SonicNet only adds speed when the computer has **more than one working route to the internet at
the same time**, each going out through a *different* line — your home broadband plus your phone's
mobile data, two ISPs, a Wi-Fi network plus a separate wired one. This guide covers getting
Windows to keep them all up at once.

- [Phone USB tethering](#phone-usb-tethering)
- [Wi-Fi and Ethernet at the same time](#wi-fi-and-ethernet-at-the-same-time)
- [Interface metrics](#interface-metrics-which-connection-windows-prefers)
- [Why two adapters on the same router don't add speed](#why-two-adapters-on-the-same-router-dont-add-speed)
- [VPNs and virtual adapters](#vpns-and-virtual-adapters)
- [Troubleshooting](#troubleshooting)

To see what Windows has right now, open PowerShell and run:

```powershell
Get-NetIPConfiguration | Where-Object IPv4DefaultGateway | Format-Table InterfaceAlias, InterfaceDescription, IPv4Address, IPv4DefaultGateway
```

Every row is a connection with a route to the internet. You want at least two, with **different
default gateways**.

## Phone USB tethering

The easiest second connection: a phone on mobile data, plugged in with a USB cable. USB is faster
and steadier than Bluetooth, and unlike a phone's Wi-Fi hotspot it leaves your computer's Wi-Fi free
for your home network.

**Make sure the phone uses mobile data**, not your home Wi-Fi — otherwise both "connections" end
at the same router (see [below](#why-two-adapters-on-the-same-router-dont-add-speed)). Turn the
phone's Wi-Fi off while tethering.

### Android

1. Connect the phone with a cable that carries data (some charge-only cables don't).
2. On the phone: **Settings → Network & internet → Hotspot & tethering → USB tethering** (the
   wording varies by maker; the switch only becomes available once the cable is connected).
3. Windows adds a network adapter, usually named *Remote NDIS based Internet Sharing Device* or
   *UsbNcm Host Device*, within a few seconds.

**Driver notes**

- Windows 10 and 11 include both drivers Android uses: RNDIS (most phones) and NCM (newer Pixels
  and some others; built into Windows 11, and Windows 10 from version 2004). Nothing needs to be
  downloaded.
- If Device Manager shows the phone as *RNDIS* or *Unknown device* with a yellow warning instead:
  right-click it → **Update driver → Browse my computer → Let me pick from a list → Network
  adapters → Microsoft → Remote NDIS Compatible Device**, and accept the warning.
- If nothing appears at all, try another USB port (directly on the computer, not a hub) and another
  cable, then toggle USB tethering off and on.

### iPhone

1. On the iPhone: **Settings → Personal Hotspot → Allow Others to Join** on.
2. Install Apple's driver: the **Apple Devices** app from the Microsoft Store (or iTunes) — Windows
   has no built-in iPhone tethering driver.
3. Connect the cable and tap **Trust** on the phone. An adapter named *Apple Mobile Device
   Ethernet* appears.

### Mobile data costs

Tethered downloads use your mobile allowance quickly, and some plans limit or slow tethering.
In Windows, mark the phone connection as metered (**Settings → Network & internet → Ethernet** (the
tethered adapter) **→ Metered connection**) so Windows Update and others leave it alone, and use
SonicNet's data-usage limits to cap what a download may take from it.

## Wi-Fi and Ethernet at the same time

By default Windows **drops or idles Wi-Fi when Ethernet is plugged in**, to "minimise the number of
simultaneous connections". If your second connection is a different Wi-Fi network (say, a mobile
router or a neighbour's line you're allowed to use), turn that off.

### Windows Pro / Enterprise / Education — Group Policy

1. Run `gpedit.msc`.
2. Go to **Computer Configuration → Administrative Templates → Network → Windows Connection
   Manager**.
3. Open **Minimize the number of simultaneous connections to the Internet or a Windows Domain**,
   choose **Enabled**, and set *Minimize Policy Options* to **0 = Allow simultaneous connections**.
4. Also set **Enable Windows to soft-disconnect a computer from a network** to **Disabled**.
5. Restart the computer.

### Windows Home — registry

There is no `gpedit.msc` on Home; the same two policies are registry values. In an **administrator**
PowerShell:

```powershell
$key = 'HKLM:\SOFTWARE\Policies\Microsoft\Windows\WcmSvc\GroupPolicy'
New-Item -Path $key -Force | Out-Null
Set-ItemProperty -Path $key -Name fMinimizeConnections -Type DWord -Value 0
Set-ItemProperty -Path $key -Name fSoftDisconnectConnections -Type DWord -Value 0
```

Restart afterwards. To undo, delete the two values (`Remove-ItemProperty -Path $key -Name
fMinimizeConnections, fSoftDisconnectConnections`) and restart.

### Also check

- Both connections are set to connect automatically.
- Wi-Fi adapter power saving: Device Manager → your Wi-Fi adapter → Properties → **Power
  Management** → untick *Allow the computer to turn off this device to save power*.

## Interface metrics: which connection Windows prefers

When several connections are up, Windows sends ordinary traffic — your browser, updates, games —
over the one with the lowest **interface metric**. By default it sets metrics automatically from
link speed, so Ethernet usually wins.

**SonicNet doesn't depend on metrics**: each of its streams is bound to the adapter you selected, so
it uses every selected network whatever the metrics say. Change them only to control what *other*
apps use — for example, so everyday browsing never goes over the phone:

```powershell
Get-NetIPInterface -AddressFamily IPv4 -ConnectionState Connected | Sort-Object InterfaceMetric |
    Format-Table InterfaceAlias, InterfaceMetric, AutomaticMetric

# Administrator PowerShell: make the phone the last resort
Set-NetIPInterface -InterfaceAlias 'Ethernet 3' -InterfaceMetric 500
```

(Use your own adapter's name from the first command.) To return to automatic:
`Set-NetIPInterface -InterfaceAlias 'Ethernet 3' -AutomaticMetric Enabled`. The same setting is in
`ncpa.cpl` → adapter → **Properties → Internet Protocol Version 4 → Advanced → Automatic metric**.

## Why two adapters on the same router don't add speed

Connecting your computer to the same router twice — Wi-Fi *and* a cable to the same box, or two
Wi-Fi bands of one router — gives Windows two adapters but only **one internet line**. Every byte
still has to come down that line, so SonicNet will show both networks, but together they reach about
what the line delivers alone.

It is the line to your ISP that is usually the limit, not the link to your router. Extra speed
comes only from a connection with its own way out: mobile data, a second ISP, another building's
network.

How to tell: the gateway *address* proves nothing — most home routers use `192.168.0.1` or
`192.168.1.1`, so two different routers often share one. Compare what's behind it instead:

- **Public IP**, one request per network (use each adapter's own IP from `ipconfig`):
  `curl --interface 192.168.0.10 https://www.cloudflare.com/cdn-cgi/trace` and the same with the
  other adapter's IP. Different `ip=` lines mean different internet lines.
- **Router hardware address**: `arp -a` lists the gateway's MAC under each interface. Different
  MACs mean different routers.

(Exception: a *dual-WAN* router that balances two ISP lines connection by connection. Plugging into
it once is enough — SonicNet's many parallel streams get spread across both lines by the router.)

## VPNs and virtual adapters

**VPNs.** A full-tunnel VPN (most commercial and corporate VPNs) sends all traffic through one
encrypted tunnel that itself runs over a single physical connection. While it's connected:

- Selecting the VPN adapter in SonicNet works, but it is one connection, not two.
- Selecting physical adapters may be blocked by the VPN's "kill switch", or may send that traffic
  outside the VPN — which may be against your company's policy.

For combined speed, disconnect the VPN, or use a VPN with split tunnelling that excludes SonicNet.

**Virtual adapters** don't lead to the internet on their own and shouldn't be selected:
*vEthernet (Default Switch)* / *vEthernet (WSL)* (Hyper-V, WSL), *VirtualBox Host-Only*, *VMware
Network Adapter VMnet*, Bluetooth *Personal Area Network* with nothing paired, and inactive TAP /
WireGuard adapters. If you use a Hyper-V **external** switch, your real connection appears as
*vEthernet (&lt;switch name&gt;)* — that one is real.

## Troubleshooting

**A connection doesn't appear in SonicNet.** Check it has an IPv4 address and a default gateway
(command at the top). A phone that has just been plugged in can take ~10 seconds to get an address.
Disabled adapters (`ncpa.cpl`) don't count.

**Wi-Fi disconnects when I plug in Ethernet.** That's Windows' default; see
[Wi-Fi and Ethernet at the same time](#wi-fi-and-ethernet-at-the-same-time).

**The phone network is listed but carries nothing.** Check the phone has mobile data (open a page
on the phone itself), that the carrier allows tethering on your plan, and that data saver or a
tethering limit isn't on. Some phones pause tethering when the screen locks or the battery is low.

**Combined speed is no faster than one connection.**
- Both connections end at the same router or ISP ([why](#why-two-adapters-on-the-same-router-dont-add-speed)).
- The server is the bottleneck: some servers cap speed per user or per IP address. More networks
  still help with per-IP caps; nothing helps with a slow server.
- The disk can't keep up (a USB stick or a busy hard drive). SonicNet slows down to match; download
  to an internal SSD.

**The download fails or restarts only on one network.** Some links are bound to the IP address that
requested them (signed CDN links, some file hosts); requests from your second connection, which has
a different public IP, are refused. SonicNet backs that network off; if it keeps happening, use only
the network you opened the link on for that site.

**The server refuses many connections** (errors such as 403, 429 or 503). SonicNet reduces the
number of streams on its own; you can also lower the per-network stream count in settings.

**Third-party firewall or antivirus.** Some block a program from using a particular adapter or from
opening many connections. Allow `sonicnet.exe` on all networks.
