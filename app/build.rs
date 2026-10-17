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
        let mut app = winresource::WindowsResource::new();
        app
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
            .set("OriginalFilename", "sonicnet.exe");
        // Per binary: the System mode helper gets its own name, which is what UAC shows.
        app.compile_for("sonicnet")
            .expect("embedding the Windows icon and manifest needs rc.exe (Windows SDK)");
        winresource::WindowsResource::new()
            .set_icon(ICON)
            .set_manifest_file(MANIFEST)
            .set("ProductName", "SonicNet")
            .set("FileDescription", "SonicNet System mode")
            .set("LegalCopyright", "Copyright (c) 2026 the SonicNet authors")
            .set("OriginalFilename", "sonicnet-system.exe")
            .compile_for("sonicnet-system")
            .expect("embedding the helper's resources needs rc.exe (Windows SDK)");
        place_wintun();
    }
}

/// System mode loads `wintun.dll` from beside `sonicnet-system.exe`, so it is copied next to the
/// exes cargo builds (target/<profile>/). The unmodified, signed DLL and its licence live in
/// `vendor/wintun/` (git-ignored; docs/system-mode.md, "Wintun DLL and licence", says where they
/// come from). Without it everything still builds; System mode then reports the DLL missing.
fn place_wintun() {
    const DLL: &str = "../vendor/wintun/wintun.dll";
    println!("cargo:rerun-if-changed=../vendor/wintun");
    let Some(out) = std::env::var_os("OUT_DIR") else { return };
    // OUT_DIR is <target>/<profile>/build/<pkg>-<hash>/out; the exes land in <target>/<profile>.
    let Some(exe_dir) = std::path::Path::new(&out).ancestors().nth(3) else { return };
    if !std::path::Path::new(DLL).is_file() {
        println!("cargo:warning=vendor/wintun/wintun.dll is missing: System mode won't start");
        return;
    }
    if let Err(error) = std::fs::copy(DLL, exe_dir.join("wintun.dll")) {
        // A running helper holds the DLL open; the copy already there is the same file.
        println!("cargo:warning=couldn't place wintun.dll beside the exe: {error}");
    }
}
