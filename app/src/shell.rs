//! The app's place in Windows: one running copy per data folder, the tray icon, and the hand-off
//! from a second launch (a `sonicnet://` link from the browser extension, or a double-click on
//! the shortcut while SonicNet already runs) to the first.
//!
//! One copy per data folder because two managers on one store would overwrite each other's
//! manifests. A named mutex says whether a copy runs; a named pipe owned by that copy carries
//! the second launch's arguments to it. Both names carry a hash of the data folder, so a test
//! copy started with `--data-dir` never talks to the user's own.

use std::io;
use std::path::Path;
use std::time::Duration;

use tokio::sync::mpsc::UnboundedSender;
use tray_icon::menu::{Menu, MenuEvent, MenuItem, PredefinedMenuItem};
use tray_icon::{MouseButton, MouseButtonState, TrayIcon, TrayIconBuilder, TrayIconEvent};

/// What the tray, or another launch, asks of the window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Shell {
    /// Show, restore and focus the window. `args` are the other launch's (e.g. `sonicnet://open`).
    Show {
        args: Vec<String>,
    },
    PauseAll,
    ResumeAll,
    Quit,
    /// System mode's helper changed state or reported its stats.
    System(crate::system::Status),
}

/// Names this data folder's copy of the app: FNV-1a over the absolute, lower-cased path, so it
/// is the same for every build and every spelling of the folder (Windows paths ignore case).
pub fn instance_key(data_dir: &Path) -> String {
    let path = std::path::absolute(data_dir).unwrap_or_else(|_| data_dir.to_path_buf());
    let text = path.to_string_lossy().trim_end_matches(['\\', '/']).to_lowercase();
    let hash = text
        .bytes()
        .fold(0xcbf2_9ce4_8422_2325_u64, |h, b| (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3));
    format!("{hash:016x}")
}

fn pipe_name(key: &str) -> String {
    format!(r"\\.\pipe\SonicNet.{key}")
}

/// Arguments travel NUL-separated: no argument can hold a NUL.
fn encode(args: &[String]) -> Vec<u8> {
    args.join("\0").into_bytes()
}

fn decode(bytes: &[u8]) -> Vec<String> {
    let text = String::from_utf8_lossy(bytes);
    if text.is_empty() { Vec::new() } else { text.split('\0').map(str::to_string).collect() }
}

#[cfg(windows)]
fn wide(text: &str) -> Vec<u16> {
    text.encode_utf16().chain([0]).collect()
}

/// Takes this data folder's single-instance mutex. `false`: another copy already holds it.
/// The mutex is held for the life of the process (Windows releases it when the process ends).
#[cfg(windows)]
pub fn claim(key: &str) -> bool {
    use windows_sys::Win32::Foundation::{ERROR_ALREADY_EXISTS, GetLastError};
    use windows_sys::Win32::System::Threading::CreateMutexW;

    let name = wide(&format!(r"Local\SonicNet.SingleInstance.{key}"));
    // SAFETY: `name` is NUL-terminated and outlives the call; null security attributes are the
    // default. The handle is deliberately never closed: holding it is the point.
    let handle = unsafe { CreateMutexW(std::ptr::null(), 0, name.as_ptr()) };
    // SAFETY: no arguments; reads this thread's last error, set by CreateMutexW just above.
    let existed = unsafe { GetLastError() } == ERROR_ALREADY_EXISTS;
    if handle.is_null() {
        // Can't tell; running is better than refusing to start.
        tracing::warn!("single-instance mutex: {}", io::Error::last_os_error());
        return true;
    }
    !existed
}

#[cfg(not(windows))]
pub fn claim(_key: &str) -> bool {
    true
}

/// Hands `args` to the copy already running, waiting up to `patience` for it to listen (it may
/// be starting up at this very moment).
pub fn forward(key: &str, args: &[String], patience: Duration) -> io::Result<()> {
    use std::io::Write;

    #[cfg(windows)]
    {
        use windows_sys::Win32::UI::WindowsAndMessaging::{ASFW_ANY, AllowSetForegroundWindow};
        // This launch came from the user (or their browser), so it may take the foreground;
        // passing that right on lets the running copy raise its window.
        // SAFETY: no pointers; failure only means the window may flash instead of rising.
        unsafe { AllowSetForegroundWindow(ASFW_ANY) };
    }
    let deadline = std::time::Instant::now() + patience;
    loop {
        match std::fs::OpenOptions::new().write(true).open(pipe_name(key)) {
            Ok(mut pipe) => return pipe.write_all(&encode(args)),
            Err(error) if std::time::Instant::now() >= deadline => return Err(error),
            // Not created yet, or busy with another launch: try again shortly.
            Err(_) => std::thread::sleep(Duration::from_millis(50)),
        }
    }
}

/// Listens for later launches on `runtime`, telling the window through `to_ui`.
#[cfg(windows)]
pub fn serve(
    runtime: &tokio::runtime::Handle,
    key: &str,
    to_ui: UnboundedSender<Shell>,
) -> io::Result<()> {
    use tokio::io::AsyncReadExt;
    use tokio::net::windows::named_pipe::ServerOptions;

    let name = pipe_name(key);
    let _context = runtime.enter();
    // `first_pipe_instance`: fails if anyone else already made a pipe of this name.
    let mut server = ServerOptions::new().first_pipe_instance(true).create(&name)?;
    runtime.spawn(async move {
        loop {
            if let Err(error) = server.connect().await {
                tracing::warn!("launch pipe: {error}");
            }
            let mut connected = server;
            // The next instance exists before this one is read, so the name never lapses.
            server = match ServerOptions::new().create(&name) {
                Ok(next) => next,
                Err(error) => {
                    tracing::error!("launch pipe: {error}");
                    return;
                }
            };
            let to_ui = to_ui.clone();
            tokio::spawn(async move {
                let mut bytes = Vec::new();
                // Bounded: a launch's arguments are a link or two.
                let mut limited = (&mut connected).take(64 * 1024);
                if limited.read_to_end(&mut bytes).await.is_ok() {
                    let _ = to_ui.send(Shell::Show { args: decode(&bytes) });
                }
            });
        }
    });
    Ok(())
}

#[cfg(not(windows))]
pub fn serve(_: &tokio::runtime::Handle, _: &str, _: UnboundedSender<Shell>) -> io::Result<()> {
    Ok(())
}

/// Whether the taskbar is light (`SystemUsesLightTheme`). Unknown reads as dark, Windows'
/// default.
#[cfg(windows)]
fn light_taskbar() -> bool {
    use windows_sys::Win32::Foundation::ERROR_SUCCESS;
    use windows_sys::Win32::System::Registry::{HKEY_CURRENT_USER, RRF_RT_REG_DWORD, RegGetValueW};

    let key = wide(r"Software\Microsoft\Windows\CurrentVersion\Themes\Personalize");
    let value = wide("SystemUsesLightTheme");
    let mut data: u32 = 0;
    let mut size = size_of::<u32>() as u32;
    // SAFETY: both names are NUL-terminated; `data` is a u32 and `size` says so.
    let status = unsafe {
        RegGetValueW(
            HKEY_CURRENT_USER,
            key.as_ptr(),
            value.as_ptr(),
            RRF_RT_REG_DWORD,
            std::ptr::null_mut(),
            (&raw mut data).cast(),
            &mut size,
        )
    };
    status == ERROR_SUCCESS && data == 1
}

/// The tray icon and its menu. Clicks and menu picks arrive through `to_ui`. Must be made on
/// the thread that runs the event loop (it owns a hidden window there).
pub fn tray(to_ui: UnboundedSender<Shell>) -> Result<TrayIcon, String> {
    #[cfg(windows)]
    // Resources 2 and 3 are embedded by build.rs: white for a dark taskbar, black for a light one.
    let icon = tray_icon::Icon::from_resource(if light_taskbar() { 3 } else { 2 }, None)
        .map_err(|e| e.to_string())?;
    #[cfg(not(windows))]
    let icon =
        tray_icon::Icon::from_rgba(vec![255; 16 * 16 * 4], 16, 16).map_err(|e| e.to_string())?;

    let menu = Menu::new();
    menu.append_items(&[
        &MenuItem::with_id("open", "Open SonicNet", true, None),
        &PredefinedMenuItem::separator(),
        &MenuItem::with_id("pause", "Pause all", true, None),
        &MenuItem::with_id("resume", "Resume all", true, None),
        &PredefinedMenuItem::separator(),
        &MenuItem::with_id("quit", "Quit", true, None),
    ])
    .map_err(|e| e.to_string())?;

    let clicks = to_ui.clone();
    TrayIconEvent::set_event_handler(Some(move |event| {
        if let TrayIconEvent::Click {
            button: MouseButton::Left,
            button_state: MouseButtonState::Up,
            ..
        } = event
        {
            let _ = clicks.send(Shell::Show { args: Vec::new() });
        }
    }));
    MenuEvent::set_event_handler(Some(move |event: MenuEvent| {
        let shell = match event.id.0.as_str() {
            "open" => Shell::Show { args: Vec::new() },
            "pause" => Shell::PauseAll,
            "resume" => Shell::ResumeAll,
            "quit" => Shell::Quit,
            _ => return,
        };
        let _ = to_ui.send(shell);
    }));

    TrayIconBuilder::new()
        .with_icon(icon)
        .with_tooltip("SonicNet")
        .with_menu(Box::new(menu))
        // Left click opens the window; the menu is on the right button.
        .with_menu_on_left_click(false)
        .build()
        .map_err(|e| e.to_string())
}

/// The tray's tooltip: the name, and the total speed while anything downloads.
pub fn tooltip(active: bool, speed: &str) -> String {
    if active { format!("SonicNet · {speed}") } else { "SonicNet".to_string() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_ignores_case_and_trailing_separator_but_not_the_folder() {
        let a = instance_key(Path::new(r"C:\Users\Me\AppData\Roaming\SonicNet"));
        assert_eq!(a, instance_key(Path::new(r"c:\users\me\appdata\roaming\sonicnet\")));
        assert_ne!(a, instance_key(Path::new(r"C:\Users\Me\AppData\Roaming\SonicNet-test")));
        assert_eq!(a.len(), 16);
    }

    #[test]
    fn arguments_survive_the_pipe_encoding() {
        let args = vec!["sonicnet://open".to_string(), "C:\\a b\\c.torrent".to_string()];
        assert_eq!(decode(&encode(&args)), args);
        assert!(decode(&encode(&[])).is_empty());
    }

    /// A real pipe: the first copy serves, a second launch forwards, the first hears it.
    #[cfg(windows)]
    #[test]
    fn a_second_launch_reaches_the_first() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let key = format!("test-{}", std::process::id());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        serve(runtime.handle(), &key, tx.clone()).expect("serve");
        // A second server of the same name is refused: no one can squat on a running copy.
        assert!(serve(runtime.handle(), &key, tx).is_err());
        for _ in 0..2 {
            forward(&key, &["sonicnet://open".into()], Duration::from_secs(5)).expect("forward");
            let got = runtime
                .block_on(async { tokio::time::timeout(Duration::from_secs(5), rx.recv()).await });
            assert_eq!(got, Ok(Some(Shell::Show { args: vec!["sonicnet://open".into()] })));
        }
    }

    #[cfg(windows)]
    #[test]
    fn the_mutex_admits_one_copy_per_key() {
        let key = format!("test-mutex-{}", std::process::id());
        assert!(claim(&key));
        assert!(!claim(&key));
    }
}
