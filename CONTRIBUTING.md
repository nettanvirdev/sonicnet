# Contributing to SonicNet

Read [ARCHITECTURE.md](ARCHITECTURE.md) first; it is short and the rules below lean on it.

## Before you push

CI runs exactly these on `windows-latest`; run them locally first:

```powershell
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

If you touched `extension/`, also run its build and tests (`npm ci`, `npm run build`, `npm test`
inside `extension/`). If you touched the icons, regenerate them (see
[packaging/README.md](packaging/README.md)) and commit the output.

Tests that need the real internet are `#[ignore = "needs the internet"]`; run them with
`cargo test --workspace -- --ignored` when you change `sonic-net` or `sonic-http`.

## Crate boundaries

Dependencies only point down the tree in ARCHITECTURE.md. In practice:

- **`sonic-core`** holds shared types and the `Command` / `Event` protocol. No I/O, ever.
- **`sonic-engine`** is pure logic. It never reads a clock or touches a socket or file; time is
  passed in. If a change needs I/O, the I/O belongs in another crate and the decision stays here.
- **`sonic-net`**, **`sonic-http`**, **`sonic-store`**, **`sonic-bridge`** each own one kind of I/O
  (adapters and sockets, HTTP, disk, the extension endpoint). The only link between them is
  `sonic-http` → `sonic-net` (HTTP rides on a pinned connection); don't add others.
- **`sonic-manager`** is the only backend crate that combines the others.
- **`app`** talks to the backend only through `Command` / `Event`. If the UI needs something new,
  add a command or event to `sonic-core`; don't reach into another crate.
- **`sonic-testkit`** is a dev-dependency only; nothing ships with it.
- Third-party versions are declared once, in the root `Cargo.toml`'s `[workspace.dependencies]`.

## Tests

- **Every non-trivial rule has a test**, ideally next to the code that states the rule. A bug fix
  comes with the test that would have caught it.
- **Pure logic gets property tests** (`proptest`): block plans cover the file exactly with no
  overlap, a block is only ever re-issued by hedging, meters and limits stay within bounds.
- **Time is injected, not waited for.** Engine code takes the clock as an argument, so tests step
  it; nothing sleeps to make a test pass.
- **End-to-end tests use real sockets, locally.** `sonic-testkit` provides an origin server that
  can throttle, refuse, drop, change the file mid-download or freeze at an exact byte, and
  "networks" that are `127.0.0.x` source addresses. A test should fail for one reason and say
  which.
- **No flaky tests.** A test that depends on timing races or the internet is either rewritten to
  control the cause, or marked `#[ignore]` with the reason.

## Code style

- Comments explain *why*, not what.
- No `unwrap` / `expect` outside tests unless the invariant that makes it safe is stated next to it.
- Every `unsafe` block has a `// SAFETY:` comment.
- Byte ranges are half-open (`start..end`) everywhere except the HTTP header itself.

## Releases

Bump `version` in the root `Cargo.toml`, then tag: `git tag v0.2.0 && git push origin v0.2.0`.
The Release workflow checks the tag matches, builds the installer and attaches it to a **draft**
GitHub release; publish the draft after a smoke test.
