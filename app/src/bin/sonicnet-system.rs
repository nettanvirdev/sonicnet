//! `sonicnet-system.exe`, System mode's elevated helper. All of it lives in
//! `sonic_system::helper`; it is built from this package so `cargo build -p sonicnet` puts it
//! beside `sonicnet.exe`, where the app looks for it.

#[cfg(windows)]
fn main() -> std::process::ExitCode {
    sonic_system::helper::main()
}

#[cfg(not(windows))]
fn main() {
    eprintln!("sonicnet-system runs on Windows only (it needs Wintun).");
}
