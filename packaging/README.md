# Packaging

| Path | What |
| --- | --- |
| `../assets/sonicnet.svg`, `sonicnet-tray.svg` | Icon masters. Everything else in `assets/` is generated from them. |
| `tools/icon-gen/` | Rasterises the masters into `assets/png/*.png`, `assets/sonicnet.ico`, `assets/tray-{white,black}.ico`. |
| `windows/sonicnet.exe.manifest` | App manifest: per-monitor-v2 DPI, long paths, `asInvoker`, Windows 10/11 compatibility, Common Controls v6. |
| `windows/sonicnet.iss` | Inno Setup installer. |
| `windows/build.ps1` | Release build → optional signing → installer. |

## Regenerating the icons

After editing either SVG:

```powershell
cargo run --release --manifest-path packaging/tools/icon-gen/Cargo.toml
```

It is a standalone crate (its own `[workspace]`), so the app never builds `resvg`. Commit the
regenerated files. The app icon's geometry sits on a 16-unit grid with 32-unit strokes so that at
16 px every line lands on whole pixels; keep that when changing it.

## Embedding icon, manifest and version info in `sonicnet.exe`

Not applied yet: `app/` is owned by the app work. Three edits, all in the app crate (plus one line
in the root `Cargo.toml`):

**1. Root `Cargo.toml`, `[workspace.dependencies]`**

```toml
winresource = { version = "0.1.31", default-features = false }
```

**2. `app/Cargo.toml`, `[build-dependencies]`**

```toml
winresource.workspace = true
```

**3. `app/build.rs`** — keep the existing `slint_build` call and add the Windows block:

```rust
fn main() {
    slint_build::compile("ui/app.slint").expect("the UI's .slint files must compile");

    // Icon, manifest and version info for sonicnet.exe. CARGO_CFG_TARGET_OS rather than
    // cfg!(windows): build scripts run on the host, which may not be the target.
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        const ICON: &str = "../assets/sonicnet.ico";
        const TRAY_WHITE: &str = "../assets/tray-white.ico";
        const TRAY_BLACK: &str = "../assets/tray-black.ico";
        const MANIFEST: &str = "../packaging/windows/sonicnet.exe.manifest";
        for path in [ICON, TRAY_WHITE, TRAY_BLACK, MANIFEST] {
            println!("cargo:rerun-if-changed={path}");
        }
        winresource::WindowsResource::new()
            // ID 1, added first: Explorer, the taskbar and the installer's shortcuts use it.
            .set_icon(ICON)
            // The tray icons, for tray_icon::Icon::from_resource(2 | 3, None).
            .set_icon_with_id(TRAY_WHITE, "2")
            .set_icon_with_id(TRAY_BLACK, "3")
            .set_manifest_file(MANIFEST)
            // FileVersion / ProductVersion come from the package version automatically.
            .set("ProductName", "SonicNet")
            .set("FileDescription", "SonicNet") // what Task Manager shows
            .set("LegalCopyright", "Copyright (c) 2026 the SonicNet authors")
            .set("OriginalFilename", "sonicnet.exe")
            .compile()
            .expect("embedding the Windows icon and manifest needs rc.exe (Windows SDK)");
    }
}
```

Paths are relative to `app/` (winresource passes the crate directory to `rc.exe` as an include
path). `rc.exe` ships with the Windows SDK that MSVC Build Tools already install, and is on the
`windows-latest` runners.

**Also needed in the app (not resources, but required to ship):**

- `app/src/main.rs`, first line: `#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]`
  so the release exe doesn't open a console window. (Anything that prints, such as a snapshot
  mode, then needs `AttachConsole(ATTACH_PARENT_PROCESS)` or to write to a file.)
- Window icon: `icon: @image-url("../../assets/png/sonicnet-256.png");` on the main `Window` in
  `app/ui/app.slint` (adjust the relative path), so the title bar and Alt+Tab use it too.
- Tray: `tray_icon::Icon::from_resource(2, None)` (white, for dark taskbars — the Windows default)
  or `3` (black, light taskbars; `HKCU\Software\Microsoft\Windows\CurrentVersion\Themes\Personalize\SystemUsesLightTheme`
  says which).
- `sonicnet://` links: the installer registers `"<install dir>\sonicnet.exe" "%1"`, so the app
  receives the whole URL as its first argument. It should hand that to an already-running instance
  (single-instance check) rather than opening a second window.

Check after building: Explorer → `target\release\sonicnet.exe` → Properties → Details shows the
version and name; the file shows the icon.

## Building the installer

Needs Rust (MSVC) and [Inno Setup 6](https://jrsoftware.org/isdl.php)
(`winget install --id JRSoftware.InnoSetup -e`).

```powershell
pwsh packaging/windows/build.ps1            # or: powershell -File packaging\windows\build.ps1
pwsh packaging/windows/build.ps1 -SkipBuild # package the existing release exe
```

Output: `target\installer\SonicNet-Setup-<version>-x64.exe`. The script looks for Inno Setup first
and stops with install instructions if it's missing.

Signing is optional: set `SONICNET_SIGN_CERT` (path to a `.pfx`) and, if it has one,
`SONICNET_SIGN_PASSWORD`. The exe is signed with `signtool` (Windows SDK) before packaging; Inno
Setup then signs the setup and the uninstaller. `SONICNET_TIMESTAMP_URL` overrides the timestamp
server. In CI the same comes from the `WINDOWS_CERT_PFX_BASE64` and `WINDOWS_CERT_PASSWORD` secrets.

### What the installer does

- Installs for the current user by default (`%LOCALAPPDATA%\Programs\SonicNet`, no UAC prompt);
  the first page offers "Install for all users" (`Program Files`, needs admin). Silent installs:
  `/CURRENTUSER` or `/ALLUSERS`, plus `/VERYSILENT`.
- Start-menu shortcut; desktop shortcut if ticked (unticked by default).
- Registers the `sonicnet://` protocol under `HKCU\Software\Classes\sonicnet` (or
  `HKLM\Software\Classes\sonicnet` for an all-users install); the uninstaller deletes the key.
- Offers to launch SonicNet at the end. Upgrades close a running copy first.
- The uninstaller leaves the user's data (`%APPDATA%\SonicNet`: settings, history, unfinished
  downloads) in place, as most Windows apps do.
