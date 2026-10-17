//! The real backend: `sonic-manager` on this computer's networks, with its data in `store`
//! (`%APPDATA%\SonicNet` unless `--data-dir`) and downloads going to the user's Downloads folder by
//! default.

use std::path::PathBuf;

use sonic_core::{Command, Event};
use sonic_manager::{ManagerConfig, NetworkSource, Tuning};
use sonic_store::Store;
use tokio::sync::mpsc;

/// Starts the manager on `runtime` and hands back the UI's ends of its channels, matching
/// [`crate::demo::spawn`] so the window doesn't care which backend it talks to.
pub fn spawn(
    runtime: &tokio::runtime::Handle,
    store: Store,
) -> (mpsc::Sender<Command>, mpsc::Receiver<Event>) {
    let default_destination = directories::UserDirs::new()
        .and_then(|dirs| dirs.download_dir().map(PathBuf::from))
        .or_else(|| directories::UserDirs::new().map(|dirs| dirs.home_dir().join("Downloads")))
        .unwrap_or_else(std::env::temp_dir);

    let (command_tx, mut command_rx) = mpsc::channel::<Command>(256);
    let (event_tx, event_rx) = mpsc::channel::<Event>(1024);
    runtime.spawn(async move {
        // `spawn` needs the runtime it runs on, so it starts inside it.
        let mut manager = sonic_manager::spawn(ManagerConfig {
            store,
            networks: NetworkSource::System,
            tuning: Tuning::default(),
            bridge: true,
            default_destination,
        });
        // The manager's channels are unbounded and the window's bounded: this relays between
        // them, so a busy window slows the relay rather than the downloads.
        loop {
            tokio::select! {
                command = command_rx.recv() => match command {
                    Some(command) => {
                        if manager.commands.send(command).is_err() {
                            return;
                        }
                    }
                    None => return,
                },
                event = manager.events.recv() => match event {
                    Some(event) => {
                        if event_tx.send(event).await.is_err() {
                            return;
                        }
                    }
                    None => return,
                },
            }
        }
    });
    (command_tx, event_rx)
}
