# SonicNet architecture

SonicNet downloads one file faster by splitting it across **every internet connection the user
selects** — any number of them — with many parallel streams on each. It is a Rust rewrite of the
ideas in [Plexo](https://github.com/anmolkapil/plexo) (MIT, © 2026 Anmol Kapil), with its own code
and UI. Windows first.

## Crates

```
sonic-core      shared types, the UI↔backend protocol (Command / Event). No I/O.
  ├─ sonic-net      adapters (GetAdaptersAddresses), NetworkMonitor, dial() pinned to one adapter
  │                 (IP_UNICAST_IF + source bind), Happy Eyeballs, TLS (rustls)
  ├─ sonic-engine   pure logic: block plan, scheduler (incl. hedging), stream-count controller,
  │                 disk watch, block progress, meters, time left, speed limits / data usage,
  │                 download schedule windows. Injected clocks, no I/O.
  ├─ sonic-http     one stream's HTTP/1.1 connection, probe, fetch of a byte range with every
  │                 response check, staging file (sparse on Windows), names, cookies/headers
  ├─ sonic-store    settings, history, per-download manifests, data usage, DPAPI-sealed cookies
  ├─ sonic-bridge   127.0.0.1 endpoint for the browser extension
  ├─ sonic-manager  runs downloads: queue, per-download transfer (streams over networks),
  │                 pause/resume, failures, persistence; speaks Command/Event
  └─ sonic-system   System mode (docs/system-mode.md): every app's connections spread over the
                    networks via a Wintun adapter; the elevated helper and its pipe protocol
app (sonicnet)  Slint UI; talks to the backend only through Command/Event, and to System mode's
                helper (sonicnet-system.exe, launched elevated) over a pipe
```

Dependencies only point down this tree. Every crate depends on `sonic-core`; only `sonic-manager`
and `app` combine the others.

## How a download runs

1. **Probe**: `GET` with `Range: bytes=0-1023` tells size, range support, name and validators.
2. **Plan**: the file is cut into blocks (1–8 MiB) — `sonic_core::plan_blocks`.
3. **Streams**: each selected network starts 8 streams (or the user's pick). Every stream owns
   one kept-alive connection through its network and pulls the next free block from one shared
   queue, so faster networks take more of the file. Each request leases a **run**: the free
   blocks after that one, as many as the stream's share of what is left — an even share until
   its speed is known, then its part of the download's speed — fetched as one range and
   finished block by block as it lands. Few streams (a server allowing one or two connections,
   or a cap on requests a minute) ask a few times per file rather than once per block; many
   streams get about a block each. A stream with nothing left to do takes the far end of the
   run that will take longest, in proportion to their speeds; that run's request stops where
   the handed-on blocks begin.
4. **Auto**: once all of a network's streams receive data it may double, up to 32, and keeps a
   doubling only if the network's own speed, measured once settled, rose by 15 %; otherwise the
   newest streams retire and it isn't grown again for a minute (sooner if its speed falls well
   below). A server refusing some (403/429/503/silence) cuts that network back; a disk that
   can't keep up halves everyone.
5. **Hedging**: when no block is left waiting and no run can be split, idle streams race the
   slowest in-flight blocks; the first to finish wins, and a run whose block was won goes on to
   its next.
6. **Safety**: every response is checked before a byte is written — status, Content-Range,
   length, and version (ETag / Last-Modified / size). Bytes go to `<name>.sonic` beside the
   destination at their own offsets; it is renamed to the final name only when complete.
7. **Persistence**: progress checkpoints every 15 s and on pause; a restart resumes paused.

## Conventions

- Byte ranges are half-open (`start..end`); only `ByteRange::http_header` speaks HTTP's inclusive
  form.
- Comments explain *why*. Every non-trivial rule has a test; pure logic gets property tests.
- No `unwrap`/`expect` outside tests unless the invariant is stated next to it.
- Every `unsafe` block has a `// SAFETY:` comment.
- `cargo clippy --all-targets -- -D warnings` and `cargo fmt` clean.
