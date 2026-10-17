//! `--snapshot <dir>`: renders every screen with demo data, in light and dark, to PNGs.
//!
//! It runs headless on Slint's software renderer behind a minimal platform whose clock this code
//! moves, so animations have settled and every run draws the same frames. The screens are
//! reached through the same controller paths the live app uses.

use std::error::Error;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use slint::platform::software_renderer::{MinimalSoftwareWindow, RepaintBufferType};
use slint::platform::{Platform, WindowAdapter};
use slint::{ComponentHandle, PhysicalSize, Rgb8Pixel};
use sonic_core::{DownloadStatus, Event, NetworkId};
use tokio::sync::mpsc;

use crate::controller::{self, Controller};
use crate::demo;
use crate::system::{State, Status};
use crate::ui::{App, AppWindow, Theme};

const WIDTH: u32 = 1180;
const HEIGHT: u32 = 800;

/// Milliseconds the snapshot clock has been pushed ahead of real time.
static SKIPPED_MS: AtomicU64 = AtomicU64::new(0);

struct Headless {
    window: Rc<MinimalSoftwareWindow>,
    started: Instant,
}

impl Platform for Headless {
    fn create_window_adapter(&self) -> Result<Rc<dyn WindowAdapter>, slint::PlatformError> {
        Ok(self.window.clone())
    }

    fn duration_since_start(&self) -> Duration {
        self.started.elapsed() + Duration::from_millis(SKIPPED_MS.load(Ordering::Relaxed))
    }
}

/// Lets time pass (timers fire, animations finish) and draws a full frame.
fn frame(window: &MinimalSoftwareWindow, height: u32) -> Vec<Rgb8Pixel> {
    window.set_size(PhysicalSize::new(WIDTH, height));
    let mut last = vec![Rgb8Pixel::default(); (WIDTH * height) as usize];
    // A few rounds: a frame's layout can report sizes (the block grid's width) that Rust answers
    // with new content, which the next round draws.
    for _ in 0..4 {
        SKIPPED_MS.fetch_add(1500, Ordering::Relaxed);
        slint::platform::update_timers_and_animations();
        window.request_redraw();
        let mut buffer = vec![Rgb8Pixel::default(); (WIDTH * height) as usize];
        if window.draw_if_needed(|renderer| {
            renderer.render(&mut buffer, WIDTH as usize);
        }) {
            last = buffer;
        }
    }
    last
}

fn save(pixels: &[Rgb8Pixel], height: u32, path: &Path) -> Result<(), Box<dyn Error>> {
    let file = std::io::BufWriter::new(std::fs::File::create(path)?);
    let mut encoder = png::Encoder::new(file, WIDTH, height);
    encoder.set_color(png::ColorType::Rgb);
    encoder.set_depth(png::BitDepth::Eight);
    let mut writer = encoder.write_header()?;
    let bytes: Vec<u8> = pixels.iter().flat_map(|p| [p.r, p.g, p.b]).collect();
    writer.write_image_data(&bytes)?;
    Ok(())
}

/// System mode's stats for one second of the snapshot's minute: each network busy in its own
/// rhythm, with running totals.
fn system_stats(
    networks: &[sonic_core::NetworkInterface],
    second: u64,
    totals: &mut Vec<(u64, u64)>,
) -> Status {
    totals.resize(networks.len(), (0, 0));
    let networks = networks
        .iter()
        .zip(totals.iter_mut())
        .enumerate()
        .map(|(i, (n, total))| {
            let phase = (second as f64 / 7.0 + i as f64 * 1.3).sin().abs();
            let down = ((0.4 + phase) * 1.6e6 / (i + 1) as f64) as u64;
            let up = down / 18;
            total.0 += down;
            total.1 += up;
            sonic_system::protocol::NetStats {
                id: n.id.clone(),
                name: n.name.clone(),
                kind: n.kind,
                tcp: 14 + (second % 5) as u32 * 3 / (i as u32 + 1),
                udp: 3 / (i as u32 + 1),
                down_bps: down,
                up_bps: up,
                down_total: total.0,
                up_total: total.1,
                capacity_bps: 0,
            }
        })
        .collect();
    Status { state: State::On, networks }
}

pub fn run(dir: &Path) -> Result<(), Box<dyn Error>> {
    std::fs::create_dir_all(dir)?;
    let window = MinimalSoftwareWindow::new(RepaintBufferType::NewBuffer);
    slint::platform::set_platform(Box::new(Headless {
        window: window.clone(),
        started: Instant::now(),
    }))
    .map_err(|e| format!("can't set up headless rendering: {e}"))?;
    let app = AppWindow::new()?;
    // Commands go nowhere: every answer the screens need is fed in directly below. The receiver
    // is kept so sends don't fail (and log) for nothing.
    let (commands, _unanswered) = mpsc::channel(4096);
    let controller = Controller::new(&app, commands);
    controller::wire(&app, &controller);
    app.show()?;
    window.set_size(PhysicalSize::new(WIDTH, HEIGHT));

    // Twenty simulated seconds in: Auto has grown the streams and the charts have shape.
    let mut world = demo::World::new(sonic_core::now_millis());
    let mut events = world.initial_events();
    for _ in 0..80 {
        events.extend(world.step(demo::TICK.as_secs_f64()));
    }
    {
        let mut c = controller.borrow_mut();
        for event in events {
            c.apply(event);
        }
    }
    // A minute of System mode beside the downloads, a second at a time, so the status bar, the
    // pill and the Networks sheet have real-looking figures and charts.
    let mut on = Status::default();
    {
        let mut c = controller.borrow_mut();
        c.set_system_status(Status { state: State::Starting, networks: Vec::new() });
        let ticks_per_second = (1.0 / demo::TICK.as_secs_f64()).round().max(1.0) as usize;
        let mut totals: Vec<(u64, u64)> = Vec::new();
        for second in 0..60u64 {
            for _ in 0..ticks_per_second {
                for event in world.step(demo::TICK.as_secs_f64()) {
                    c.apply(event);
                }
            }
            on = system_stats(c.networks(), second, &mut totals);
            c.set_system_status(on.clone());
            c.tick_traffic();
        }
    }
    let (running, failed) = {
        let c = controller.borrow();
        let find =
            |status| c.downloads().iter().find(|d| d.status == status).map(|d| d.id.to_string());
        (find(DownloadStatus::Downloading), find(DownloadStatus::Error))
    };
    let finished = controller.borrow().history().get(1).map(|f| f.id.to_string());
    let (running, failed, finished) = (
        running.ok_or("no running download")?,
        failed.ok_or("no failed download")?,
        finished.ok_or("no history")?,
    );

    let mut written = Vec::new();
    for (theme, mode) in [("light", 1), ("dark", 2)] {
        app.global::<Theme>().set_mode(mode);
        let mut shot = |name: &str,
                        height: u32,
                        setup: &dyn Fn(&mut Controller)|
         -> Result<(), Box<dyn Error>> {
            {
                let mut c = controller.borrow_mut();
                setup(&mut c);
                c.render();
            }
            let pixels = frame(&window, height);
            let path = dir.join(format!("{name}-{theme}.png"));
            save(&pixels, height, &path)?;
            written.push(path);
            Ok(())
        };

        shot("list", HEIGHT, &|c| {
            c.show(None, &[]);
            c.set_filter(0);
        })?;
        shot("list-menu", HEIGHT, &|c| {
            let ids: Vec<String> = c.downloads().iter().take(2).map(|d| d.id.to_string()).collect();
            c.select(&ids);
            let id = c.downloads()[0].id.to_string();
            c.open_menu(id, 700.0, 200.0);
        })?;
        app.global::<App>().set_menu_open(false);
        shot("new-download", HEIGHT, &|c| {
            c.select(&[]);
            let url =
                "https://download.blender.org/release/Blender4.2/blender-4.2.3-windows-x64.msi";
            c.open_new(Some(url.into()));
            c.probe_done(c.probe_seq(), demo::probe(url));
            c.free_space_done(Some(412 * 1024 * 1024 * 1024));
        })?;
        shot("new-download-short", 640, &|c| {
            let url =
                "https://download.blender.org/release/Blender4.2/blender-4.2.3-windows-x64.msi";
            c.open_new(Some(url.into()));
            c.probe_done(c.probe_seq(), demo::probe(url));
            c.free_space_done(Some(412 * 1024 * 1024 * 1024));
        })?;
        app.global::<App>().set_new_open(false);
        // One card open in place per state; the window is as tall as the page so all of it shows.
        shot("open-downloading", 1640, &|c| {
            c.set_grid_width(1068.0);
            c.show(Some(&running), &[NetworkId::new("{usb}")]);
        })?;
        shot("open-complete", 1000, &|c| {
            c.set_filter(2);
            c.show(Some(&finished), &[]);
        })?;
        shot("open-failed", HEIGHT, &|c| {
            c.set_filter(3);
            c.show(Some(&failed), &[]);
        })?;
        shot("settings", HEIGHT, &|c| {
            c.set_filter(0);
            c.show(None, &[]);
            c.open_settings(0);
        })?;
        shot("settings-networks", HEIGHT, &|c| c.open_settings(2))?;
        shot("settings-system", HEIGHT, &|c| c.open_settings(4))?;
        shot("settings-system-error", HEIGHT, &|c| {
            let reason = "Another program's adapter named SonicNet is in the way".to_string();
            c.set_system_status(Status { state: State::Failed(reason), networks: Vec::new() });
            c.open_settings(4);
        })?;
        app.global::<App>().set_settings_open(false);
        shot("list-system-starting", HEIGHT, &|c| {
            c.set_system_status(Status { state: State::Starting, networks: Vec::new() });
        })?;
        shot("list-system-permission", HEIGHT, &|c| {
            c.set_system_status(Status { state: State::NeedsPermission, networks: Vec::new() });
        })?;
        shot("list-system-off", HEIGHT, &|c| c.set_system_status(Status::default()))?;
        shot("networks", 1000, &|c| {
            c.set_system_status(on.clone());
            c.open_traffic();
        })?;
        app.global::<App>().set_traffic_open(false);
        shot("empty", HEIGHT, &|c| {
            c.apply(Event::Downloads(Vec::new()));
            c.apply(Event::History(Arc::new(Vec::new())));
        })?;
        // Back to the full list for the next theme.
        let mut c = controller.borrow_mut();
        for event in world.initial_events() {
            c.apply(event);
        }
    }
    for path in &written {
        println!("{}", path.display());
    }
    Ok(())
}
