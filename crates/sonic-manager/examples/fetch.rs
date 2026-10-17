//! Downloads one URL over every network that can reach the internet, printing each network's
//! share as it goes — a quick end-to-end check on real hardware, without the UI.
//!
//! ```text
//! cargo run --release -p sonic-manager --example fetch -- <url> [destination-dir] [options]
//! ```
//!
//! - `--streams N`: a fixed count per network (`StartRequest::streams_per_network`), not Auto.
//! - `--net TEXT`: only networks whose name or an address contains TEXT (repeatable).
//! - `--secs N`: cancel after N seconds (deleting what it downloaded).
//! - `--per-stream`: also print every stream each second.
//! - `RUST_LOG=sonic_manager=debug,sonic_http=debug`: per-request tracing on stderr.
//!
//! It uses a throwaway data folder, so it never touches the app's settings or downloads list.

use std::path::PathBuf;
use std::time::Duration;

use sonic_core::{Command, DownloadStatus, Event, StartRequest};
use sonic_manager::{ManagerConfig, NetworkSource, Tuning, spawn};
use sonic_store::Store;
use tokio::sync::oneshot;

#[tokio::main]
async fn main() -> Result<(), String> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
    let (mut positional, mut streams, mut only, mut secs, mut per_stream) =
        (Vec::new(), None, Vec::new(), None, false);
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || args.next().ok_or(format!("{arg} needs a value"));
        match arg.as_str() {
            "--streams" => streams = Some(value()?.parse::<u32>().map_err(|e| e.to_string())?),
            "--net" => only.push(value()?),
            "--secs" => secs = Some(value()?.parse::<u64>().map_err(|e| e.to_string())?),
            "--per-stream" => per_stream = true,
            _ => positional.push(arg),
        }
    }
    let mut positional = positional.into_iter();
    let url = positional.next().ok_or("usage: fetch <url> [destination-dir] [options]")?;
    let destination = positional.next().map(PathBuf::from).unwrap_or_else(std::env::temp_dir);
    let data = tempfile::tempdir().map_err(|e| e.to_string())?;

    let mut handle = spawn(ManagerConfig {
        store: Store::at(data.path()),
        networks: NetworkSource::System,
        tuning: Tuning::default(),
        bridge: false,
        default_destination: destination.clone(),
    });

    // The startup events: Settings, Networks, Downloads, History.
    let mut networks = Vec::new();
    while let Some(event) = handle.events.recv().await {
        match event {
            Event::Networks(list) => networks = list.to_vec(),
            Event::History(_) => break,
            _ => {}
        }
    }
    println!("Networks:");
    for n in &networks {
        let addresses: Vec<String> = n.addresses.iter().map(|a| a.ip.to_string()).collect();
        println!(
            "  {:<24} {:?}{} [{}]",
            n.name,
            n.kind,
            if n.has_gateway { "" } else { " (no gateway)" },
            addresses.join(", ")
        );
    }
    let picked = |n: &sonic_core::NetworkInterface| {
        only.is_empty()
            || only.iter().any(|text| {
                n.name.contains(text.as_str())
                    || n.addresses.iter().any(|a| a.ip.to_string().contains(text.as_str()))
            })
    };
    let chosen: Vec<_> =
        networks.iter().filter(|n| n.has_gateway && picked(n)).map(|n| n.id.clone()).collect();
    if chosen.is_empty() {
        return Err("no network with a gateway".into());
    }

    let (reply, answer) = oneshot::channel();
    handle.commands.send(Command::Probe { url: url.clone(), reply }).map_err(|e| e.to_string())?;
    let probe = answer.await.map_err(|e| e.to_string())??;
    println!(
        "\n{} — {} bytes, ranges: {}",
        probe.suggested_file_name,
        probe.total_bytes.map_or("unknown".into(), |b| b.to_string()),
        probe.supports_ranges
    );

    let (reply, answer) = oneshot::channel();
    let request = StartRequest {
        url: probe.final_url.clone(),
        destination_dir: destination,
        file_name: probe.suggested_file_name.clone(),
        total_bytes: probe.total_bytes,
        supports_ranges: probe.supports_ranges,
        network_ids: chosen,
        streams_per_network: streams,
        etag: probe.etag.clone(),
        last_modified: probe.last_modified.clone(),
        browser: None,
    };
    handle
        .commands
        .send(Command::Start { request, link: None, reply })
        .map_err(|e| e.to_string())?;
    let id = answer.await.map_err(|e| e.to_string())??;

    let started = std::time::Instant::now();
    let mut last_print = started - Duration::from_secs(1);
    let mut cancelled = false;
    while let Some(event) = handle.events.recv().await {
        if !cancelled && secs.is_some_and(|s| started.elapsed() >= Duration::from_secs(s)) {
            cancelled = true;
            println!(
                "
Time is up: cancelling (and deleting what it downloaded)."
            );
            let _ = handle.commands.send(Command::Cancel(id));
        }
        let state = match event {
            Event::DownloadUpdated(state) if state.id == id => state,
            Event::DownloadRemoved(removed) if removed == id => break,
            _ => continue,
        };
        let done = matches!(state.status, DownloadStatus::Completed | DownloadStatus::Error);
        if last_print.elapsed() >= Duration::from_secs(1) || done {
            last_print = std::time::Instant::now();
            let shares: Vec<String> = state
                .networks
                .iter()
                .filter(|n| n.enabled)
                .map(|n| {
                    let streams = state.streams.iter().filter(|s| s.network_id == n.id).count();
                    format!(
                        "{} {:.1} MB/s {:.1} MB ({streams} streams, {:?})",
                        n.label,
                        n.speed_bps / 1e6,
                        n.bytes_downloaded as f64 / 1e6,
                        n.status
                    )
                })
                .collect();
            println!(
                "{:>6.1}s {:>5.1}%  {:>6.1} MB/s  | {}",
                started.elapsed().as_secs_f64(),
                state.progress().unwrap_or(0.0) * 100.0,
                state.speed_bps / 1e6,
                shares.join("  |  ")
            );
            if per_stream {
                for s in &state.streams {
                    let label = state
                        .networks
                        .iter()
                        .find(|n| n.id == s.network_id)
                        .map_or("?", |n| n.label.as_str());
                    println!(
                        "         #{:<3} {label:<12} {:?}{} block {:?} {:.2} MB/s",
                        s.id,
                        s.status,
                        if s.hedge { " (hedge)" } else { "" },
                        s.block,
                        s.speed_bps / 1e6
                    );
                }
            }
        }
        if cancelled && matches!(state.status, DownloadStatus::Cancelled) {
            break;
        }
        match state.status {
            DownloadStatus::Completed => {
                println!("\nDone: {}", state.destination_path.display());
                break;
            }
            DownloadStatus::Error => return Err(state.error.clone().unwrap_or_default()),
            _ => {}
        }
    }

    let (reply, answer) = oneshot::channel();
    let _ = handle.commands.send(Command::Shutdown { reply });
    let _ = answer.await;
    Ok(())
}
