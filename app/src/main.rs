//! SonicNet's desktop app.
//!
//! `sonicnet` opens the window on the real backend (`sonic-manager`); `sonicnet --demo` opens it
//! on the built-in simulation instead.
//! `sonicnet --snapshot <dir>` renders every screen, light and dark, to PNGs in `<dir>`.
//!
//! One copy runs per data folder: a later launch (a `sonicnet://` link, the shortcut again)
//! hands its arguments to the running copy, which comes to the front, and exits. Hidden for
//! tests: `--data-dir <path>` keeps the data there instead of `%APPDATA%\SonicNet`, which also
//! makes it a separate copy from the user's own.

// A release build is a windowed app with no console behind it.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod backend;
mod controller;
mod demo;
mod format;
mod shell;
mod snapshot;
mod system;
mod traffic;
mod view;

// Slint's generated code: no Debug impls, and its own style.
#[allow(missing_debug_implementations, clippy::all)]
mod ui {
    slint::include_modules!();
}

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use sonic_core::Command;
use sonic_store::Store;
use tokio::sync::{mpsc, oneshot};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if let Some(at) = args.iter().position(|a| a == "--snapshot") {
        let dir = args.get(at + 1).map_or_else(|| PathBuf::from("snapshots"), PathBuf::from);
        return match snapshot::run(&dir) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("snapshot failed: {error}");
                ExitCode::FAILURE
            }
        };
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();

    let demo = args.iter().any(|a| a == "--demo");
    let data_dir = args.iter().position(|a| a == "--data-dir").and_then(|at| args.get(at + 1));
    let store = match (demo, data_dir) {
        (true, _) => None,
        (false, Some(dir)) => Some(Store::at(dir)),
        (false, None) => match Store::open_default() {
            Ok(store) => Some(store),
            Err(error) => {
                eprintln!("can't open SonicNet's data folder: {error}");
                return ExitCode::FAILURE;
            }
        },
    };
    // Two managers on one store would overwrite each other: a second copy only hands over. The
    // demo touches no store, so it may run beside anything.
    let key = store.as_ref().map(|store| shell::instance_key(store.root()));
    if let Some(key) = &key
        && !shell::claim(key)
    {
        return match shell::forward(key, &args, Duration::from_secs(5)) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("SonicNet is already running but didn't answer: {error}");
                ExitCode::FAILURE
            }
        };
    }

    let runtime =
        match tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build() {
            Ok(runtime) => runtime,
            Err(error) => {
                eprintln!("can't start: {error}");
                return ExitCode::FAILURE;
            }
        };
    let (shell_tx, shell_rx) = mpsc::unbounded_channel();
    let mut extras = controller::Extras::default();
    if let Some(store) = &store {
        // System mode starts (with its UAC prompt) once the saved settings say it is on.
        extras.system = Some(system::spawn(runtime.handle(), shell_tx.clone()));
        let saved = runtime.block_on(store.load_traffic::<traffic::Saved>());
        let (store, handle) = (store.clone(), runtime.handle().clone());
        let save: controller::SaveTraffic = Box::new(move |traffic| {
            if let Err(error) = handle.block_on(store.save_traffic(traffic)) {
                tracing::warn!("can't save today's traffic: {error}");
            }
        });
        extras.traffic = Some((saved, save));
    }
    let system = extras.system.clone();
    let (commands, events) = match store {
        Some(store) => backend::spawn(runtime.handle(), store),
        None => demo::spawn(runtime.handle()),
    };
    if let Some(key) = &key
        && let Err(error) = shell::serve(runtime.handle(), key, shell_tx.clone())
    {
        tracing::warn!("other launches can't reach this one: {error}");
    }
    let result = controller::run_ui(commands.clone(), events, shell_tx, shell_rx, extras);
    save_and_stop(&runtime, commands, system);
    runtime.shutdown_timeout(Duration::from_secs(2));
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("the window failed: {error}");
            ExitCode::FAILURE
        }
    }
}

/// Asks the backend to suspend and save every download and System mode to put routing back, and
/// waits for both at once, but not forever: a stuck disk mustn't keep a quit app around, and a
/// helper that doesn't answer within [`system::STOP_WAIT`] still stops when it sees us exit.
fn save_and_stop(
    runtime: &tokio::runtime::Runtime,
    commands: mpsc::Sender<Command>,
    system: Option<system::Requests>,
) {
    let (reply, answer) = oneshot::channel();
    let asked = async move {
        commands.send(Command::Shutdown { reply }).await.ok()?;
        answer.await.ok()
    };
    let routing = async move {
        if let Some(system) = system {
            let started = std::time::Instant::now();
            system::shutdown(&system).await;
            tracing::info!(ms = started.elapsed().as_millis() as u64, "System mode stopped");
        }
    };
    // The timer is made inside `block_on`: it needs the runtime's clock.
    let (saved, ()) = runtime.block_on(async {
        tokio::join!(tokio::time::timeout(Duration::from_secs(5), asked), routing)
    });
    match saved {
        Ok(Some(Ok(()))) => tracing::debug!("backend saved and stopped"),
        Ok(Some(Err(error))) => tracing::warn!("backend shutdown: {error}"),
        Ok(None) => tracing::warn!("backend was already gone at shutdown"),
        Err(_) => tracing::warn!("backend didn't finish saving within 5 s"),
    }
}
