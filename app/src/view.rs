//! Backend state → the structs the screens lay out. Pure functions of their inputs (no UI
//! handle, no clock), so every rule about what is shown where is unit-tested here.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use chrono::{Datelike, NaiveDate, Timelike};
use slint::{Image, ModelRc, Rgba8Pixel, SharedPixelBuffer, SharedString, VecModel};
use sonic_core::{
    AppSettings, BlockIndex, BlockState, BlockStatus, DataPeriod, DownloadNetwork,
    DownloadSchedule, DownloadState, DownloadStatus, FinishedDownload, Millis, NetworkId,
    NetworkInterface, NetworkKind, NetworkPreference, NetworkStatus, SpeedUnit, StreamState,
    StreamStatus,
};

use crate::format;
use crate::ui;

fn model<T: Clone + 'static>(items: Vec<T>) -> ModelRc<T> {
    ModelRc::new(VecModel::from(items))
}

fn ss(text: impl AsRef<str>) -> SharedString {
    SharedString::from(text.as_ref())
}

/// `NetworkKind` as the UI's icon index (see `Icons.kind` in icons.slint).
pub fn kind_index(kind: NetworkKind) -> i32 {
    match kind {
        NetworkKind::Wifi => 0,
        NetworkKind::Ethernet => 1,
        NetworkKind::Usb => 2,
        NetworkKind::Cellular => 3,
        NetworkKind::Bridge => 4,
        NetworkKind::Virtual => 5,
        NetworkKind::Other => 6,
    }
}

fn kind_label(kind: NetworkKind) -> &'static str {
    match kind {
        NetworkKind::Wifi => "Wi-Fi",
        NetworkKind::Ethernet => "Ethernet",
        NetworkKind::Usb => "USB tethering",
        NetworkKind::Cellular => "Cellular",
        NetworkKind::Bridge => "Bridge",
        NetworkKind::Virtual => "Virtual adapter",
        NetworkKind::Other => "Network",
    }
}

/// The network colours, as theme.slint defines them (read once from the UI so the swatch list
/// lives in one place).
#[derive(Clone, Debug, Default)]
pub struct Palette {
    /// Swatch ids, in the UI's order: a colour index is a position here.
    pub ids: Vec<String>,
    pub light: Vec<[u8; 3]>,
    pub dark: Vec<[u8; 3]>,
    /// The swatch id each `NetworkKind` (by `kind_index`) starts with; empty for none.
    pub kind_ids: Vec<String>,
}

impl Palette {
    pub fn rgb(&self, index: i32, dark: bool) -> [u8; 3] {
        let list = if dark { &self.dark } else { &self.light };
        usize::try_from(index).ok().and_then(|i| list.get(i).copied()).unwrap_or([128, 128, 128])
    }

    fn index_of(&self, id: &str) -> Option<usize> {
        self.ids.iter().position(|candidate| candidate == id)
    }
}

/// Gives every network one colour, distinct from the others while swatches last: the user's pick
/// wins, then the kind's own hue if still free, then the first unused swatch. `networks` is in
/// display order, which decides who gets a contested hue — so it must be stable.
pub fn assign_colors(
    networks: &[(NetworkId, NetworkKind)],
    prefs: &BTreeMap<NetworkId, NetworkPreference>,
    palette: &Palette,
) -> BTreeMap<NetworkId, i32> {
    let mut colors = BTreeMap::new();
    let mut used = BTreeSet::new();
    for (id, _) in networks {
        let picked = prefs.get(id).and_then(|p| p.color_id.as_deref());
        if let Some(index) = picked.and_then(|c| palette.index_of(c)) {
            colors.insert(id.clone(), index as i32);
            used.insert(index);
        }
    }
    for (id, kind) in networks {
        if colors.contains_key(id) {
            continue;
        }
        let own = palette
            .kind_ids
            .get(kind_index(*kind) as usize)
            .and_then(|c| palette.index_of(c))
            .filter(|i| !used.contains(i));
        let free = own.or_else(|| (0..palette.ids.len()).find(|i| !used.contains(i)));
        // More networks than swatches: reuse, in order, rather than leave one colourless.
        let index = free.unwrap_or(colors.len() % palette.ids.len().max(1));
        used.insert(index);
        colors.insert(id.clone(), index as i32);
    }
    colors
}

/// What every view needs to know besides the item itself.
#[derive(Clone, Copy, Debug)]
pub struct Ctx<'a> {
    pub unit: SpeedUnit,
    pub now: Millis,
    pub colors: &'a BTreeMap<NetworkId, i32>,
    pub prefs: &'a BTreeMap<NetworkId, NetworkPreference>,
}

impl Ctx<'_> {
    pub fn color(&self, id: &NetworkId) -> i32 {
        self.colors.get(id).copied().unwrap_or(-1)
    }

    /// The user's name for a network, else the OS's.
    pub fn name(&self, id: &NetworkId, os_name: &str) -> String {
        self.prefs
            .get(id)
            .and_then(|p| p.custom_name.as_deref())
            .map(str::trim)
            .filter(|n| !n.is_empty())
            .unwrap_or(os_name)
            .to_string()
    }

    fn speed(&self, bps: f64) -> String {
        format::speed(bps, self.unit)
    }
}

// ---------------------------------------------------------------------------------------------
// The list

/// Something listed: a download still in the backend's list, or one from history.
#[derive(Clone, Copy, Debug)]
pub enum Item<'a> {
    Live(&'a DownloadState),
    Done(&'a FinishedDownload),
}

pub const GROUP_TITLES: [&str; 5] =
    ["Downloading", "Queued", "Paused", "Needs attention", "Finished"];
pub const FINISHED_GROUP: usize = 4;

impl Item<'_> {
    pub fn id(&self) -> String {
        match self {
            Item::Live(d) => d.id.to_string(),
            Item::Done(f) => f.id.to_string(),
        }
    }

    /// Which of `GROUP_TITLES` it is listed under; `None` for a cancelled one, which isn't listed.
    pub fn group(&self) -> Option<usize> {
        match self {
            Item::Done(_) => Some(FINISHED_GROUP),
            Item::Live(d) => match d.status {
                DownloadStatus::Downloading => Some(0),
                DownloadStatus::Queued => Some(1),
                DownloadStatus::Paused => Some(2),
                DownloadStatus::Error => Some(3),
                DownloadStatus::Completed => Some(FINISHED_GROUP),
                DownloadStatus::Cancelled => None,
            },
        }
    }
}

/// The list filters (All, Active, Finished, Failed) as the groups each shows.
pub fn filter_shows(filter: i32, group: usize) -> bool {
    match filter {
        1 => group <= 2,
        2 => group == FINISHED_GROUP,
        3 => group == 3,
        _ => true,
    }
}

/// Each network's slice of a bar, in network order. `total` is the whole bar.
fn segments(networks: &[DownloadNetwork], total: u64, ctx: &Ctx) -> Vec<ui::Seg> {
    if total == 0 {
        return Vec::new();
    }
    let mut start = 0.0;
    networks
        .iter()
        .filter(|n| n.bytes_downloaded > 0)
        .map(|n| {
            let share = (n.bytes_downloaded as f64 / total as f64).min(1.0 - start).max(0.0);
            let seg = ui::Seg { color: ctx.color(&n.id), start: start as f32, share: share as f32 };
            start += share;
            seg
        })
        .collect()
}

/// A card's networks: the ones working on it (or, once it has stopped, the ones that delivered
/// part of it) as icons in their colours, and a tooltip naming each with its share so far, largest
/// first: "Ethernet 64% · Wi-Fi 36%".
pub fn network_badges(
    networks: &[DownloadNetwork],
    running: bool,
    ctx: &Ctx,
) -> (Vec<ui::RowNet>, String) {
    let shown: Vec<&DownloadNetwork> = networks
        .iter()
        .filter(|n| if running { n.status == NetworkStatus::On } else { n.bytes_downloaded > 0 })
        .collect();
    let badges = shown
        .iter()
        .map(|n| ui::RowNet { kind: kind_index(n.kind), color: ctx.color(&n.id) })
        .collect();
    let total: u64 = networks.iter().map(|n| n.bytes_downloaded).sum();
    let mut by_share: Vec<&&DownloadNetwork> = shown.iter().collect();
    by_share.sort_by_key(|n| std::cmp::Reverse(n.bytes_downloaded));
    let tip = by_share
        .iter()
        .map(|n| {
            let name = ctx.name(&n.id, &n.label);
            if total > 0 {
                format!(
                    "{name} {}",
                    format::percent(n.bytes_downloaded as f64 / total as f64 + 0.005)
                )
            } else {
                name
            }
        })
        .collect::<Vec<_>>()
        .join(" · ");
    (badges, tip)
}

pub fn row(item: Item, selected: bool, schedule: &DownloadSchedule, ctx: &Ctx) -> ui::Row {
    match item {
        Item::Done(f) => finished_row(f, selected, ctx),
        Item::Live(d) if d.status == DownloadStatus::Completed => {
            let total = d.total_bytes.unwrap_or(d.bytes_downloaded);
            ui::Row {
                id: ss(d.id.to_string()),
                name: ss(&d.file_name),
                ext: ss(format::ext_badge(&d.file_name)),
                detail: ss(format!("{} · {}", format::bytes(total), format::host(&d.url))),
                state: ui::RowState::Finished,
                eta: ss(format::when(d.completed_at.unwrap_or(ctx.now), ctx.now)),
                progress: 1.0,
                selected,
                ..Default::default()
            }
        }
        Item::Live(d) => live_row(d, selected, schedule, ctx),
    }
}

fn finished_row(f: &FinishedDownload, selected: bool, ctx: &Ctx) -> ui::Row {
    let (nets, nets_tip) = network_badges(&f.networks, false, ctx);
    let mut detail = format!("{} · {}", format::bytes(f.total_bytes), format::host(&f.url));
    if f.missing {
        detail.push_str(" · moved or deleted");
    }
    ui::Row {
        id: ss(f.id.to_string()),
        name: ss(&f.file_name),
        ext: ss(format::ext_badge(&f.file_name)),
        detail: ss(detail),
        detail_tone: if f.missing { ui::Tone::Warning } else { ui::Tone::Neutral },
        nets: model(nets),
        nets_tip: ss(nets_tip),
        state: ui::RowState::Finished,
        eta: ss(format::when(f.completed_at, ctx.now)),
        progress: 1.0,
        selected,
        ..Default::default()
    }
}

fn sizes(d: &DownloadState) -> String {
    match d.total_bytes {
        Some(total) => format!("{} of {}", format::bytes(d.bytes_downloaded), format::bytes(total)),
        None => format::bytes(d.bytes_downloaded),
    }
}

fn live_row(d: &DownloadState, selected: bool, schedule: &DownloadSchedule, ctx: &Ctx) -> ui::Row {
    let progress = d.progress();
    let pct = progress.map(format::percent);
    let (state, detail, tone) = match d.status {
        DownloadStatus::Downloading => {
            let mut parts: Vec<String> = pct.iter().cloned().collect();
            parts.push(sizes(d));
            if d.disk_limited {
                parts.push("disk is the limit".into());
            }
            (ui::RowState::Downloading, parts.join(" · "), ui::Tone::Neutral)
        }
        DownloadStatus::Queued => {
            let why = if d.scheduled && schedule.enabled {
                format!("Waits for the schedule ({})", format::hhmm(schedule.start_minute))
            } else {
                "Waiting for a turn".into()
            };
            (ui::RowState::Queued, format!("{why} · {}", sizes(d)), ui::Tone::Neutral)
        }
        DownloadStatus::Paused => {
            let at = pct.map(|p| format!("Paused at {p}")).unwrap_or_else(|| "Paused".into());
            (ui::RowState::Paused, format!("{at} · {}", sizes(d)), ui::Tone::Neutral)
        }
        _ => {
            let error = d.error.clone().unwrap_or_else(|| "Something went wrong".into());
            (ui::RowState::Failed, error, ui::Tone::Danger)
        }
    };
    let downloading = d.status == DownloadStatus::Downloading;
    let (nets, nets_tip) = network_badges(&d.networks, downloading, ctx);
    ui::Row {
        id: ss(d.id.to_string()),
        name: ss(&d.file_name),
        ext: ss(format::ext_badge(&d.file_name)),
        detail: ss(detail),
        detail_tone: tone,
        state,
        speed: ss(if downloading { ctx.speed(d.speed_bps) } else { String::new() }),
        eta: ss(if downloading { format::eta(d.time_left_secs) } else { String::new() }),
        progress: progress.map_or(-1.0, |p| p as f32),
        segments: model(segments(&d.networks, d.total_bytes.unwrap_or(0), ctx)),
        nets: model(nets),
        nets_tip: ss(nets_tip),
        selected,
        ..Default::default()
    }
}

/// "2 downloading · 1 queued", or "All done".
pub fn summary(downloads: &[&DownloadState]) -> String {
    let count = |status| downloads.iter().filter(|d| d.status == status).count();
    let parts: Vec<String> = [
        (count(DownloadStatus::Downloading), "downloading"),
        (count(DownloadStatus::Queued), "queued"),
        (count(DownloadStatus::Paused), "paused"),
        (count(DownloadStatus::Error), "need attention"),
    ]
    .into_iter()
    .filter(|(n, _)| *n > 0)
    .map(|(n, label)| format!("{n} {label}"))
    .collect();
    if parts.is_empty() { "Nothing downloading".into() } else { parts.join(" · ") }
}

// ---------------------------------------------------------------------------------------------
// One download

/// How a network is doing, in a word, its tone, and a sentence on what it means.
pub fn network_status(n: &DownloadNetwork, running: bool) -> (String, ui::Tone, String) {
    let (word, tone, explain): (&str, ui::Tone, String) = match n.status {
        NetworkStatus::On if !running => ("Ready", ui::Tone::Neutral, String::new()),
        NetworkStatus::On if n.speed_bps > 0.0 => ("Receiving", ui::Tone::Success, String::new()),
        NetworkStatus::On => ("Connecting", ui::Tone::Neutral, String::new()),
        NetworkStatus::Off => (
            "Off",
            ui::Tone::Neutral,
            "Switched off for this download. Tick it to use it again.".into(),
        ),
        NetworkStatus::Offline => (
            "Offline",
            ui::Tone::Warning,
            "Not connected to this computer. It is used again as soon as it reconnects.".into(),
        ),
        NetworkStatus::Unreachable => (
            "Unreachable",
            ui::Tone::Warning,
            "Connected, but the server can't be reached through it. One stream keeps trying and the rest follow once it gets through.".into(),
        ),
        NetworkStatus::Failed => (
            "Failed",
            ui::Tone::Danger,
            format!(
                "{} Switch it off and on to try again.",
                n.error.as_deref().unwrap_or("The server kept refusing requests over it.")
            ),
        ),
        NetworkStatus::Limit => (
            "Data limit",
            ui::Tone::Warning,
            "It has used its data allowance for this period. It is used again when the period ends or the allowance is raised.".into(),
        ),
    };
    (word.into(), tone, explain)
}

/// The cells of a network's expanded stream list, four to a row (the last row padded, so cells
/// keep one width).
fn stream_rows(streams: &[&StreamState], ctx: &Ctx) -> Vec<ui::StreamRow> {
    const PER_ROW: usize = 4;
    streams
        .chunks(PER_ROW)
        .map(|chunk| {
            let mut cells: Vec<ui::StreamCell> = chunk
                .iter()
                .map(|s| {
                    let label = match (s.status, s.block) {
                        (StreamStatus::Retrying, _) => "Retrying".to_string(),
                        (_, Some(block)) => {
                            format!("Block #{}", format::count(u64::from(block) + 1))
                        }
                        _ => "Waiting".to_string(),
                    };
                    let active = s.status == StreamStatus::Downloading && s.speed_bps > 0.0;
                    ui::StreamCell {
                        label: ss(label),
                        speed: ss(if active { ctx.speed(s.speed_bps) } else { String::new() }),
                        hedge: s.hedge,
                        active,
                    }
                })
                .collect();
            cells.resize(PER_ROW, ui::StreamCell::default());
            ui::StreamRow { cells: model(cells) }
        })
        .collect()
}

/// A download's networks, in its order, as the detail screens show them.
pub fn net_lines(
    networks: &[DownloadNetwork],
    streams: &[StreamState],
    running: bool,
    expanded: &BTreeSet<NetworkId>,
    ctx: &Ctx,
) -> Vec<ui::NetLine> {
    let delivered: u64 = networks.iter().map(|n| n.bytes_downloaded).sum();
    let live = |n: &DownloadNetwork| running && n.status == NetworkStatus::On;
    let fastest = networks.iter().filter(|n| live(n)).map(|n| n.speed_bps).fold(0.0, f64::max);
    let count = networks.len().max(1) as f32;
    networks
        .iter()
        .enumerate()
        .map(|(i, n)| {
            let (status, status_tone, explain) = network_status(n, running);
            let mine: Vec<&StreamState> = streams.iter().filter(|s| s.network_id == n.id).collect();
            let share =
                if delivered > 0 { n.bytes_downloaded as f64 / delivered as f64 } else { 0.0 };
            let label_y = (i as f32 + 0.5) / count;
            let is_expanded = expanded.contains(&n.id);
            ui::NetLine {
                id: ss(n.id.as_str()),
                name: ss(ctx.name(&n.id, &n.label)),
                kind: kind_index(n.kind),
                color: ctx.color(&n.id),
                enabled: n.enabled,
                status: ss(status),
                status_tone,
                explain: ss(explain),
                speed: ss(if live(n) { ctx.speed(n.speed_bps) } else { "—".into() }),
                bytes: ss(format::bytes(n.bytes_downloaded)),
                share: share as f32,
                share_text: ss(format::percent(share + 0.005)),
                retries: n.retries as i32,
                stream_count: mine.len() as i32,
                hedges: mine.iter().filter(|s| s.hedge).count() as i32,
                streams: model(if is_expanded { stream_rows(&mine, ctx) } else { Vec::new() }),
                expanded: is_expanded,
                curve: SharedString::default(),
                weight: if live(n) && fastest > 0.0 { (n.speed_bps / fastest) as f32 } else { 0.0 },
                label_y,
            }
        })
        .collect()
}

/// Shapes the flow diagram: each network a filled ribbon (viewbox 100 × 100) from its label on
/// the left to one stack in the middle of the right edge, as thick as its share of the speed. A
/// network carrying nothing keeps a hairline-thin ribbon so its row still reads. Returns the
/// stack's top and bottom (0–1), where the arrow it flows into goes.
pub fn flow_ribbons(lines: &mut [ui::NetLine]) -> (f32, f32) {
    // Space between ribbons where they stack, so each stays its own shape.
    const GAP: f32 = 1.5;
    let n = lines.len().max(1) as f32;
    let thickness = |w: f32| if w > 0.0 { 6.0 + w * 14.0 } else { 1.2 };
    let total = lines.iter().map(|l| thickness(l.weight)).sum::<f32>() + GAP * (n - 1.0);
    let top = 50.0 - total / 2.0;
    let mut stack = top;
    for line in lines.iter_mut() {
        let t = thickness(line.weight);
        // At its label a ribbon is never wider than the label's row.
        let left = t.min(70.0 / n);
        let y = line.label_y * 100.0;
        let (a, b) = (y - left / 2.0, y + left / 2.0);
        let (s, e) = (stack, stack + t);
        line.curve = ss(format!(
            "M0 {a:.2}C50 {a:.2} 50 {s:.2} 100 {s:.2}L100 {e:.2}C50 {e:.2} 50 {b:.2} 0 {b:.2}Z"
        ));
        stack = e + GAP;
    }
    (top / 100.0, (50.0 + total / 2.0) / 100.0)
}

/// "2.4×" and "faster than Ethernet alone", when more than one network is pulling its weight.
pub fn multiplier(
    networks: &[DownloadNetwork],
    total_bps: f64,
    ctx: &Ctx,
) -> Option<(String, String)> {
    let live: Vec<&DownloadNetwork> =
        networks.iter().filter(|n| n.status == NetworkStatus::On && n.speed_bps > 1024.0).collect();
    let fastest = live.iter().max_by(|a, b| a.speed_bps.total_cmp(&b.speed_bps))?;
    let factor = total_bps / fastest.speed_bps;
    (live.len() >= 2 && factor >= 1.05).then(|| {
        (
            format!("{factor:.1}×"),
            format!("faster than {} alone", ctx.name(&fastest.id, &fastest.label)),
        )
    })
}

/// A rounded top for a chart axis, so its labels read 20, 50, 100… in the user's unit.
pub fn nice_top(bps: f64, unit: SpeedUnit) -> f64 {
    let per_unit = match unit {
        SpeedUnit::Bytes => 1024.0 * 1024.0,
        SpeedUnit::Bits => 1e6 / 8.0,
    };
    let value = (bps / per_unit).max(0.1);
    let magnitude = 10f64.powf(value.log10().floor());
    let step = [1.0, 2.0, 2.5, 5.0, 10.0]
        .into_iter()
        .map(|m| m * magnitude)
        .find(|top| *top >= value)
        .unwrap_or(10.0 * magnitude);
    step * per_unit
}

/// A stacked area chart of each network's speed (`order` sets the stacking, bottom first), in a
/// 600 × 100 viewbox. `slots` is how many samples the width holds: a chart of the last minute
/// keeps its scale while it fills (newest at the right), a finished one stretches to fit.
pub struct Chart {
    pub series: Vec<ui::Series>,
    pub top_bps: f64,
}

pub fn chart(
    history: &BTreeMap<NetworkId, Vec<f64>>,
    order: &[(NetworkId, i32)],
    slots: Option<usize>,
    unit: SpeedUnit,
) -> Chart {
    let rows: Vec<(&[f64], i32)> = order
        .iter()
        .filter_map(|(id, color)| history.get(id).map(|h| (h.as_slice(), *color)))
        .collect();
    let n = rows.iter().map(|(h, _)| h.len()).max().unwrap_or(0);
    if n < 2 {
        return Chart { series: Vec::new(), top_bps: 0.0 };
    }
    let at =
        |h: &[f64], t: usize| h.get(t).copied().filter(|v| v.is_finite()).unwrap_or(0.0).max(0.0);
    let mut stacked = vec![vec![0.0; n]; rows.len() + 1];
    for (i, (h, _)) in rows.iter().enumerate() {
        let (below, above) = stacked.split_at_mut(i + 1);
        for (t, v) in above[0].iter_mut().enumerate() {
            *v = below[i][t] + at(h, t);
        }
    }
    let peak = stacked[rows.len()].iter().copied().fold(0.0, f64::max);
    let top = nice_top(peak * 1.08, unit);
    let slots = slots.unwrap_or(n).max(n);
    let x = |t: usize| (t + slots - n) as f64 * 600.0 / (slots - 1) as f64;
    let y = |v: f64| 100.0 - (v / top * 100.0).min(100.0);
    let series = rows
        .iter()
        .enumerate()
        .map(|(i, (_, color))| {
            let mut path = String::new();
            for (t, v) in stacked[i + 1].iter().enumerate() {
                let cmd = if t == 0 { 'M' } else { 'L' };
                path.push_str(&format!("{cmd}{:.1} {:.2}", x(t), y(*v)));
            }
            for (t, v) in stacked[i].iter().enumerate().rev() {
                path.push_str(&format!("L{:.1} {:.2}", x(t), y(*v)));
            }
            path.push('Z');
            ui::Series { area: ss(path), color: *color, faded: false }
        })
        .collect();
    Chart { series, top_bps: top }
}

/// What a block grid is drawn with, in the current theme.
#[derive(Clone, Copy, Debug)]
pub struct GridColors {
    pub track: [u8; 3],
    pub hedge: [u8; 3],
}

#[derive(Clone, Debug)]
pub struct Grid {
    pub image: Image,
    pub w: f32,
    pub h: f32,
    pub cols: i32,
    pub pitch: f32,
}

/// The tallest the grid may get before its cells shrink, in logical pixels.
const GRID_MAX_HEIGHT: f32 = 132.0;

/// Cell pitch (cell + gap) and columns for `count` blocks across `width` logical pixels: the
/// largest cells that keep the grid under `GRID_MAX_HEIGHT`.
pub fn grid_layout(count: usize, width: f32) -> (f32, f32, usize) {
    const PITCHES: [f32; 10] = [16.0, 14.0, 12.0, 10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0];
    let count = count.max(1);
    let fit = |pitch: f32| {
        let gap = if pitch >= 7.0 { 2.0 } else { 1.0 };
        let cols = (((width + gap) / pitch).floor() as usize).clamp(1, count);
        let rows = count.div_ceil(cols);
        (pitch, gap, cols, rows as f32 * pitch - gap)
    };
    let (pitch, gap, cols, _) = PITCHES
        .into_iter()
        .map(fit)
        .find(|(.., height)| *height <= GRID_MAX_HEIGHT)
        .unwrap_or_else(|| fit(PITCHES[PITCHES.len() - 1]));
    (pitch, gap, cols)
}

fn mix(a: [u8; 3], b: [u8; 3], t: f32) -> [u8; 3] {
    let lerp = |x: u8, y: u8| (x as f32 + (y as f32 - x as f32) * t).round() as u8;
    [lerp(a[0], b[0]), lerp(a[1], b[1]), lerp(a[2], b[2])]
}

/// Paints one cell per block at the grid's exact physical size (so nothing is resampled): done
/// in the colour of the network that delivered most of it, in flight filled as far as it got
/// over a tint of its network, a raced (hedged) block tinted amber, waiting ones as track.
pub fn block_grid(
    blocks: &[BlockState],
    hedged: &BTreeSet<BlockIndex>,
    color_of: &dyn Fn(&NetworkId) -> [u8; 3],
    colors: GridColors,
    width: f32,
    scale: f32,
) -> Grid {
    let (pitch, gap, cols) = grid_layout(blocks.len(), width);
    let rows = blocks.len().max(1).div_ceil(cols);
    let px = |logical: f32| (logical * scale).round() as usize;
    let (img_w, img_h) =
        (px(cols as f32 * pitch - gap).max(1), px(rows as f32 * pitch - gap).max(1));
    let cell = px(pitch - gap).max(1);
    let mut buffer = SharedPixelBuffer::<Rgba8Pixel>::new(img_w as u32, img_h as u32);
    let pixels = buffer.make_mut_slice();
    for (i, block) in blocks.iter().enumerate() {
        let (x0, y0) = (px((i % cols) as f32 * pitch), px((i / cols) as f32 * pitch));
        let owner = block.main_network().or(block.network_id.as_ref());
        let net = owner.map(color_of);
        let (base, fill, filled) = match block.status {
            BlockStatus::Completed => (net.unwrap_or(colors.track), None, 0),
            BlockStatus::Pending => (colors.track, None, 0),
            BlockStatus::Downloading => {
                let net = net.unwrap_or(colors.hedge);
                let tint = if hedged.contains(&block.index) { colors.hedge } else { net };
                let fraction = block
                    .range
                    .len()
                    .filter(|len| *len > 0)
                    .map_or(0.0, |len| block.bytes_downloaded as f32 / len as f32);
                (
                    mix(colors.track, tint, 0.3),
                    Some(net),
                    (fraction.clamp(0.0, 1.0) * cell as f32).round() as usize,
                )
            }
        };
        // Rounded corners once cells are big enough for it to show.
        let round = cell >= 6;
        for dy in 0..cell {
            let y = y0 + dy;
            if y >= img_h {
                break;
            }
            for dx in 0..cell {
                let x = x0 + dx;
                if x >= img_w {
                    break;
                }
                let corner = round && (dx == 0 || dx == cell - 1) && (dy == 0 || dy == cell - 1);
                if corner {
                    continue;
                }
                let rgb = match fill {
                    Some(fill) if dx < filled => fill,
                    _ => base,
                };
                pixels[y * img_w + x] = Rgba8Pixel { r: rgb[0], g: rgb[1], b: rgb[2], a: 255 };
            }
        }
    }
    Grid {
        image: Image::from_rgba8(buffer),
        w: img_w as f32 / scale,
        h: img_h as f32 / scale,
        cols: cols as i32,
        pitch: pitch * (img_w as f32 / scale) / (cols as f32 * pitch - gap).max(1.0),
    }
}

/// The readout under the grid for the block under the pointer.
pub fn block_info(d: &DownloadState, index: usize, ctx: &Ctx) -> String {
    let Some(block) = d.blocks.get(index) else { return String::new() };
    let range = match block.range.end {
        Some(end) => format!("{} – {}", format::bytes(block.range.start), format::bytes(end)),
        None => format!("from {}", format::bytes(block.range.start)),
    };
    let name_of = |id: &NetworkId| {
        let os = d.networks.iter().find(|n| &n.id == id).map_or(id.as_str(), |n| n.label.as_str());
        ctx.name(id, os)
    };
    let total: u64 = block.bytes_by_network.values().sum();
    let mut who: Vec<(&NetworkId, &u64)> =
        block.bytes_by_network.iter().filter(|(_, b)| **b > 0).collect();
    who.sort_by(|a, b| b.1.cmp(a.1));
    let by = who
        .iter()
        .map(|(id, bytes)| {
            if who.len() == 1 {
                name_of(id)
            } else {
                format!(
                    "{} {}",
                    name_of(id),
                    format::percent(**bytes as f64 / total.max(1) as f64 + 0.005)
                )
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let state = match block.status {
        BlockStatus::Completed => format!("done by {by}"),
        BlockStatus::Pending => "waiting".into(),
        BlockStatus::Downloading => {
            let fraction = block
                .range
                .len()
                .map_or(0.0, |len| block.bytes_downloaded as f64 / len.max(1) as f64);
            let on = block.network_id.as_ref().map(name_of).unwrap_or_default();
            format!("{} on {on}", format::percent(fraction))
        }
    };
    format!("Block #{} · {range} · {state}", format::count(index as u64 + 1))
}

/// Paths for display: the folder, with the user's profile shortened to "~".
pub fn folder_of(path: &Path) -> String {
    path.parent().map(|p| p.display().to_string()).unwrap_or_default()
}

pub fn running_detail(d: &DownloadState, ctx: &Ctx) -> ui::Detail {
    let running = d.status == DownloadStatus::Downloading;
    let (value, unit) = format::speed_parts(if running { d.speed_bps } else { 0.0 }, ctx.unit);
    let secs = d.active_ms(ctx.now) as f64 / 1000.0;
    let avg = if secs > 0.0 { d.bytes_downloaded as f64 / secs } else { 0.0 };
    let (multiplier, multiplier_text) = if running {
        multiplier(&d.networks, d.speed_bps, ctx).unwrap_or_default()
    } else {
        Default::default()
    };
    let (state_text, state_tone) = match d.status {
        DownloadStatus::Downloading if d.disk_limited => ("DISK-LIMITED", ui::Tone::Warning),
        DownloadStatus::Downloading => ("DOWNLOADING", ui::Tone::Success),
        DownloadStatus::Queued => ("QUEUED", ui::Tone::Neutral),
        _ => ("PAUSED", ui::Tone::Neutral),
    };
    let done = d.blocks.iter().filter(|b| b.status == BlockStatus::Completed).count();
    let block_summary = if d.blocks.len() > 1 {
        format!(
            "{} of {} blocks · {} each",
            format::count(done as u64),
            format::count(d.blocks.len() as u64),
            format::bytes(d.block_size)
        )
    } else {
        "One block: the server can't send parts".into()
    };
    let mut footer = vec![
        format!("Saving to {}", folder_of(&d.destination_path)),
        format!("from {}", format::host(&d.url)),
    ];
    if let Some(browser) = &d.from_browser {
        footer.push(format!("via {browser}"));
    }
    ui::Detail {
        kind: ui::DetailKind::Running,
        id: ss(d.id.to_string()),
        name: ss(&d.file_name),
        ext: ss(format::ext_badge(&d.file_name)),
        url: ss(&d.url),
        folder: ss(folder_of(&d.destination_path)),
        state_text: ss(state_text),
        state_tone,
        paused: d.status == DownloadStatus::Paused,
        queued: d.status == DownloadStatus::Queued,
        speed_value: ss(value),
        speed_unit: ss(unit),
        avg: ss(ctx.speed(avg)),
        peak: ss(d.peak_speed_bps.map_or("—".into(), |p| ctx.speed(p))),
        multiplier: ss(multiplier),
        multiplier_text: ss(multiplier_text),
        sizes: ss(sizes(d)),
        percent: ss(d.progress().map(format::percent).unwrap_or_default()),
        eta: ss(if running { format::eta(d.time_left_secs) } else { String::new() }),
        progress: d.progress().map_or(-1.0, |p| p as f32),
        segments: model(segments(&d.networks, d.total_bytes.unwrap_or(0), ctx)),
        block_count: d.blocks.len() as i32,
        block_summary: ss(block_summary),
        disk_limited: d.disk_limited && running,
        footer: ss(footer.join(" · ")),
        resumable: d.resumable,
        ..Default::default()
    }
}

pub fn failed_detail(d: &DownloadState, ctx: &Ctx) -> ui::Detail {
    ui::Detail {
        kind: ui::DetailKind::Failed,
        error: ss(d.error.clone().unwrap_or_else(|| "Something went wrong.".into())),
        sizes: ss(format!("{} downloaded", sizes(d))),
        ..running_detail(d, ctx)
    }
}

/// A finished download, from history or just completed.
pub fn complete_detail(f: &FinishedDownload, ctx: &Ctx) -> ui::Detail {
    let (value, unit) = format::speed_parts(f.average_speed_bps(), ctx.unit);
    let used = f.networks.iter().filter(|n| n.bytes_downloaded > 0).count();
    let stats = vec![
        ("Size", format::bytes(f.total_bytes)),
        ("Time", format::duration(f.active_ms() as f64 / 1000.0)),
        ("Peak", f.peak_speed_bps.map_or("—".into(), |p| ctx.speed(p))),
        ("Networks", used.to_string()),
        ("Peak streams", f.peak_streams.to_string()),
    ];
    ui::Detail {
        kind: ui::DetailKind::Complete,
        id: ss(f.id.to_string()),
        name: ss(&f.file_name),
        ext: ss(format::ext_badge(&f.file_name)),
        url: ss(&f.url),
        folder: ss(folder_of(&f.destination_path)),
        speed_value: ss(value),
        speed_unit: ss(unit),
        stats: model(
            stats.into_iter().map(|(l, v)| ui::Stat { label: ss(l), value: ss(v) }).collect(),
        ),
        segments: model(segments(&f.networks, f.total_bytes, ctx)),
        footer: ss(format!(
            "{} · saved to {} · {}",
            format::bytes(f.total_bytes),
            folder_of(&f.destination_path),
            format::when(f.completed_at, ctx.now)
        )),
        missing: f.missing,
        progress: 1.0,
        ..Default::default()
    }
}

/// A just-completed download as history will keep it, so its screen shows before history does.
pub fn as_finished(d: &DownloadState) -> FinishedDownload {
    FinishedDownload {
        id: d.id,
        url: d.url.clone(),
        file_name: d.file_name.clone(),
        destination_path: d.destination_path.clone(),
        total_bytes: d.total_bytes.unwrap_or(d.bytes_downloaded),
        started_at: d.started_at,
        completed_at: d.completed_at.unwrap_or(d.started_at),
        total_paused_ms: d.total_paused_ms,
        networks: d.networks.clone(),
        speed_history: d.speed_history.clone(),
        peak_speed_bps: d.peak_speed_bps,
        peak_streams: d.peak_streams,
        blocks_written: d.blocks.len() as u32,
        from_browser: d.from_browser.clone(),
        missing: false,
    }
}

// ---------------------------------------------------------------------------------------------
// New download

/// The usage of a network over its allowance, if it has one: (used, allowance).
pub fn allowance(pref: Option<&NetworkPreference>, usage: u64) -> Option<(u64, u64)> {
    pref.and_then(|p| p.data_limit).map(|limit| (usage, limit))
}

fn period_word(period: DataPeriod) -> &'static str {
    match period {
        DataPeriod::Day => "a day",
        DataPeriod::Week => "a week",
        DataPeriod::Month => "a month",
    }
}

pub fn chips(
    networks: &[NetworkInterface],
    usage: &BTreeMap<NetworkId, u64>,
    chosen: &BTreeSet<NetworkId>,
    ctx: &Ctx,
) -> Vec<ui::Chip> {
    networks
        .iter()
        .map(|n| {
            let pref = ctx.prefs.get(&n.id);
            let used = usage.get(&n.id).copied().unwrap_or(0);
            let over = allowance(pref, used).filter(|(used, limit)| used >= limit);
            let address = n.addresses_of(false).next().or_else(|| n.addresses_of(true).next());
            let sub = if let Some((_, limit)) = over {
                let period = pref.map_or(DataPeriod::Month, |p| p.data_period);
                format!("Used its {} {}", format::bytes(limit), period_word(period))
            } else if !n.has_gateway {
                "No route to the internet".into()
            } else {
                match address {
                    Some(ip) => format!("{} · {ip}", kind_label(n.kind)),
                    None => kind_label(n.kind).into(),
                }
            };
            ui::Chip {
                id: ss(n.id.as_str()),
                name: ss(ctx.name(&n.id, &n.name)),
                sub: ss(sub),
                kind: kind_index(n.kind),
                color: ctx.color(&n.id),
                selected: chosen.contains(&n.id) && over.is_none(),
                warn: !n.has_gateway,
                blocked: over.is_some(),
            }
        })
        .collect()
}

/// The networks New download starts with ticked: all but those left out last time, over their
/// allowance, or without a route to the internet (which can only add a warning, not speed).
pub fn default_choice(
    networks: &[NetworkInterface],
    usage: &BTreeMap<NetworkId, u64>,
    prefs: &BTreeMap<NetworkId, NetworkPreference>,
) -> BTreeSet<NetworkId> {
    networks
        .iter()
        .filter(|n| {
            let pref = prefs.get(&n.id);
            let over = allowance(pref, usage.get(&n.id).copied().unwrap_or(0))
                .is_some_and(|(u, l)| u >= l);
            !pref.is_some_and(|p| p.off) && !over && n.has_gateway
        })
        .map(|n| n.id.clone())
        .collect()
}

/// The streams choice index (Auto, 4, 8, 16, 32) as `StartRequest::streams_per_network`.
pub fn streams_choice(index: i32) -> Option<u32> {
    usize::try_from(index)
        .ok()
        .and_then(|i| i.checked_sub(1))
        .and_then(|i| sonic_core::STREAM_CHOICES.get(i).copied())
}

// ---------------------------------------------------------------------------------------------
// Settings

/// Settings as the form holds them: text fields still text.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Form {
    pub theme: i32,
    pub unit: i32,
    pub at_once: i32,
    pub tray: bool,
    pub system_mode: bool,
    pub limit_on: bool,
    pub limit: String,
    pub slow_speed: String,
    pub sched_on: bool,
    pub sched_start: String,
    pub sched_stop: String,
    pub sched_days: [bool; 7],
    pub sched_end_on: bool,
    pub sched_end: String,
    pub nets: Vec<NetForm>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NetForm {
    pub id: NetworkId,
    pub name: String,
    pub color: Option<String>,
    pub speed: String,
    pub data: String,
    pub period: i32,
}

fn unit_of(index: i32) -> SpeedUnit {
    if index == 1 { SpeedUnit::Bits } else { SpeedUnit::Bytes }
}

pub fn form_of(settings: &AppSettings, nets: &[(NetworkId, String)]) -> Form {
    let unit = settings.speed_unit;
    let s = &settings.schedule;
    let mut days = [false; 7];
    for &d in &s.days {
        if let Some(day) = days.get_mut(d as usize) {
            *day = true;
        }
    }
    Form {
        theme: match settings.theme {
            sonic_core::Theme::System => 0,
            sonic_core::Theme::Light => 1,
            sonic_core::Theme::Dark => 2,
        },
        unit: if unit == SpeedUnit::Bits { 1 } else { 0 },
        at_once: settings.downloads_at_once.clamp(1, 4) as i32 - 1,
        tray: settings.close_to_tray,
        system_mode: settings.system_mode,
        limit_on: settings.speed_limit.is_some(),
        limit: settings
            .speed_limit
            .map(|l| format::speed_field(l, unit))
            .unwrap_or_else(|| "10".into()),
        slow_speed: format::speed_field(settings.slow_mode_speed, unit),
        sched_on: s.enabled,
        sched_start: format::hhmm(s.start_minute),
        sched_stop: format::hhmm(s.end_minute),
        sched_days: days,
        sched_end_on: s.end_date.is_some(),
        sched_end: s.end_date.clone().unwrap_or_default(),
        nets: nets
            .iter()
            .map(|(id, _)| {
                let p = settings.network_preferences.get(id).cloned().unwrap_or_default();
                NetForm {
                    id: id.clone(),
                    name: p.custom_name.unwrap_or_default(),
                    color: p.color_id,
                    speed: p.speed_limit.map(|l| format::speed_field(l, unit)).unwrap_or_default(),
                    data: p.data_limit.map(format::gb_field).unwrap_or_default(),
                    period: match p.data_period {
                        DataPeriod::Day => 0,
                        DataPeriod::Week => 1,
                        DataPeriod::Month => 2,
                    },
                }
            })
            .collect(),
    }
}

/// The form's values over `base` (what it doesn't show is kept), or what's wrong with them.
pub fn settings_from_form(base: &AppSettings, form: &Form) -> Result<AppSettings, String> {
    let unit = unit_of(form.unit);
    let mut s = base.clone();
    s.theme = match form.theme {
        1 => sonic_core::Theme::Light,
        2 => sonic_core::Theme::Dark,
        _ => sonic_core::Theme::System,
    };
    s.speed_unit = unit;
    s.downloads_at_once = (form.at_once + 1).clamp(1, 4) as u32;
    s.close_to_tray = form.tray;
    s.system_mode = form.system_mode;
    s.speed_limit = if form.limit_on {
        Some(
            format::parse_speed_field(&form.limit, unit)?
                .ok_or("Enter the total speed limit, or switch it off.")?,
        )
    } else {
        None
    };
    s.slow_mode_speed =
        format::parse_speed_field(&form.slow_speed, unit)?.ok_or("Enter a speed for slow mode.")?;
    let start = format::parse_hhmm(&form.sched_start)
        .ok_or_else(|| format!("\u{201c}{}\u{201d} isn't a time of day.", form.sched_start))?;
    let end = format::parse_hhmm(&form.sched_stop)
        .ok_or_else(|| format!("\u{201c}{}\u{201d} isn't a time of day.", form.sched_stop))?;
    let end_date = if form.sched_end_on {
        let text = form.sched_end.trim();
        NaiveDate::parse_from_str(text, "%Y-%m-%d")
            .map_err(|_| format!("\u{201c}{text}\u{201d} isn't a date like 2026-12-31."))?;
        Some(text.to_string())
    } else {
        None
    };
    s.schedule = DownloadSchedule {
        enabled: form.sched_on,
        start_minute: start,
        end_minute: end,
        days: (0..7u8).filter(|&d| form.sched_days[d as usize]).collect(),
        end_date,
    };
    if s.schedule.enabled && !s.schedule.is_valid() {
        return Err(if start == end {
            "The schedule needs different start and stop times.".into()
        } else {
            "Pick at least one day for the schedule.".into()
        });
    }
    for net in &form.nets {
        let pref = s.network_preferences.entry(net.id.clone()).or_default();
        let name = net.name.trim();
        pref.custom_name = (!name.is_empty()).then(|| name.to_string());
        pref.color_id = net.color.clone();
        pref.speed_limit = format::parse_speed_field(&net.speed, unit)?;
        pref.data_limit = format::parse_gb_field(&net.data)?;
        pref.data_period = match net.period {
            0 => DataPeriod::Day,
            1 => DataPeriod::Week,
            _ => DataPeriod::Month,
        };
    }
    Ok(s)
}

/// Whether the schedule lets downloads run at a local time (`weekday`: Sunday = 0).
pub fn schedule_open(s: &DownloadSchedule, date: NaiveDate, weekday: u8, minute: u16) -> bool {
    if !s.enabled {
        return true;
    }
    let starts_on = |day: u8, on: NaiveDate| {
        s.days.contains(&day)
            && s.end_date
                .as_deref()
                .and_then(|e| NaiveDate::parse_from_str(e, "%Y-%m-%d").ok())
                .is_none_or(|end| on <= end)
    };
    if s.start_minute < s.end_minute {
        starts_on(weekday, date) && minute >= s.start_minute && minute < s.end_minute
    } else {
        // Past midnight: tonight's window, or the tail of last night's.
        let yesterday = (weekday + 6) % 7;
        (starts_on(weekday, date) && minute >= s.start_minute)
            || (minute < s.end_minute && starts_on(yesterday, date.pred_opt().unwrap_or(date)))
    }
}

/// The status bar's schedule line and whether downloads may run now.
pub fn schedule_line(s: &DownloadSchedule, now: chrono::DateTime<chrono::Local>) -> (String, bool) {
    let weekday = now.weekday().num_days_from_sunday() as u8;
    let minute = (now.hour() * 60 + now.minute()) as u16;
    let open = schedule_open(s, now.date_naive(), weekday, minute);
    let text = if open {
        format!("Scheduled · running until {}", format::hhmm(s.end_minute))
    } else {
        format!("Scheduled · waits until {}", format::hhmm(s.start_minute))
    };
    (text, open)
}

/// The schedule in a sentence, for the settings page.
pub fn schedule_sentence(on: bool, start: &str, stop: &str, days: &[bool; 7]) -> String {
    if !on {
        return "Downloads run whenever they're started.".into();
    }
    const NAMES: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    let picked: Vec<&str> = (0..7).filter(|&d| days[d]).map(|d| NAMES[d]).collect();
    let when = match picked.len() {
        0 => "on no days yet".to_string(),
        7 => "every day".to_string(),
        _ => format!("on {}", picked.join(", ")),
    };
    match (format::parse_hhmm(start), format::parse_hhmm(stop)) {
        (Some(a), Some(b)) if b < a => {
            format!(
                "Downloads run from {} to {} the next morning, {when}.",
                format::hhmm(a),
                format::hhmm(b)
            )
        }
        (Some(a), Some(b)) => {
            format!("Downloads run from {} to {}, {when}.", format::hhmm(a), format::hhmm(b))
        }
        _ => "Enter times like 01:00 and 07:00.".into(),
    }
}

// ---------------------------------------------------------------------------------------------
// System mode and traffic per network

/// The pill's state and label, its tooltip, and the line Settings shows under the state.
pub fn system_texts(
    status: &crate::system::Status,
    traffic: &crate::traffic::Traffic,
    ctx: &Ctx,
) -> (ui::SysState, String, String, String) {
    use crate::system::State;
    match &status.state {
        State::Off => (
            ui::SysState::Off,
            "System-wide off".into(),
            "System-wide is off: only SonicNet's downloads use every network".into(),
            "Only SonicNet's own downloads are spread over your networks.".into(),
        ),
        State::Starting => (
            ui::SysState::Starting,
            "Starting System-wide".into(),
            "Waiting for Windows' permission, then setting up routing".into(),
            "Windows asks for permission to change routing; SonicNet itself stays unelevated."
                .into(),
        ),
        State::On => {
            let lines: Vec<String> = status
                .networks
                .iter()
                .map(|s| {
                    let session = traffic.nets.get(&s.id).map_or(0, |n| n.session_system);
                    format!(
                        "{} · ↓ {} · ↑ {} · {} · {} this session",
                        ctx.name(&s.id, &s.name),
                        format::speed(s.down_bps as f64, ctx.unit),
                        format::speed(s.up_bps as f64, ctx.unit),
                        connections(s.tcp, s.udp),
                        format::bytes(session),
                    )
                })
                .collect();
            let tip = if lines.is_empty() {
                "Every app's connections are spread over your networks".into()
            } else {
                lines.join("\n")
            };
            (
                ui::SysState::On,
                "System-wide on".into(),
                tip,
                "Every app's new connections are spread over your networks.".into(),
            )
        }
        State::NeedsPermission => (
            ui::SysState::Permission,
            "Needs permission".into(),
            "Windows' permission prompt was declined: System-wide is off until you try again"
                .into(),
            "The permission prompt was declined, so System-wide is off for now. Downloads work as usual."
                .into(),
        ),
        State::Failed(reason) => (
            ui::SysState::Error,
            "System-wide error".into(),
            format!("{reason}. Normal routing is in place."),
            format!("{reason}. Normal routing is in place; downloads work as usual."),
        ),
    }
}

/// "12 TCP · 3 UDP", or that nothing is open.
pub fn connections(tcp: u32, udp: u32) -> String {
    match (tcp, udp) {
        (0, 0) => "No connections".into(),
        (t, 0) => format!("{t} TCP"),
        (0, u) => format!("{u} UDP"),
        (t, u) => format!("{t} TCP · {u} UDP"),
    }
}

/// Every network worth showing, connected ones first: what it carries, System mode and
/// downloads, now, over the last minute, this session and today.
pub fn traffic_nets(
    traffic: &crate::traffic::Traffic,
    networks: &[NetworkInterface],
    system_on: bool,
    ctx: &Ctx,
) -> Vec<ui::TrafficNet> {
    let all = traffic.session_total();
    let mut ids: Vec<(NetworkId, String, String, NetworkKind, bool)> = networks
        .iter()
        .map(|n| (n.id.clone(), n.name.clone(), n.description.clone(), n.kind, true))
        .collect();
    for (id, net) in &traffic.nets {
        if net.session() > 0 && !ids.iter().any(|(known, ..)| known == id) {
            ids.push((
                id.clone(),
                id.as_str().to_string(),
                String::new(),
                NetworkKind::Other,
                false,
            ));
        }
    }
    ids.into_iter()
        .map(|(id, os_name, description, kind, online)| {
            let empty = crate::traffic::NetTraffic::default();
            let t = traffic.nets.get(&id).unwrap_or(&empty);
            let color = ctx.color(&id);
            let mut history = BTreeMap::new();
            history.insert(NetworkId::new("system"), t.system_history.iter().copied().collect());
            history
                .insert(NetworkId::new("downloads"), t.downloads_history.iter().copied().collect());
            let order = [(NetworkId::new("system"), color), (NetworkId::new("downloads"), color)];
            let mut chart = chart(&history, &order, Some(crate::traffic::HISTORY), ctx.unit);
            if let Some(downloads) = chart.series.get_mut(1) {
                downloads.faded = true;
            }
            let label = |top: f64| {
                if top > 0.0 { format::speed(top, ctx.unit) } else { String::new() }
            };
            let share = if all > 0 { t.session() as f64 / all as f64 } else { 0.0 };
            let (status, tone) = match (online, system_on && online) {
                (true, true) => ("System-wide", ui::Tone::Success),
                (true, false) => ("Connected", ui::Tone::Success),
                (false, _) => ("Offline", ui::Tone::Neutral),
            };
            let down = t.down_bps();
            ui::TrafficNet {
                id: ss(id.as_str()),
                name: ss(ctx.name(&id, &os_name)),
                description: ss(if description.is_empty() { os_name.clone() } else { description }),
                kind: kind_index(kind),
                color,
                online,
                status: ss(status),
                status_tone: tone,
                down: ss(format::speed(down, ctx.unit)),
                up: ss(format!("↑ {}", format::speed(t.system_up_bps, ctx.unit))),
                busy: down + t.system_up_bps >= 1024.0,
                system_line: ss(format!(
                    "↓ {} · ↑ {} · {} · {} this session",
                    format::speed(t.system_down_bps, ctx.unit),
                    format::speed(t.system_up_bps, ctx.unit),
                    connections(t.tcp, t.udp),
                    format::bytes(t.session_system),
                )),
                connections: ss(connections(t.tcp, t.udp)),
                session_system: ss(format!("System-wide {}", format::bytes(t.session_system))),
                session_downloads: ss(format!("Downloads {}", format::bytes(t.session_downloads))),
                today_system: ss(format!("System-wide {}", format::bytes(t.today_system))),
                today_downloads: ss(format!("Downloads {}", format::bytes(t.today_downloads))),
                share: share as f32,
                share_text: ss(if all > 0 {
                    format!("{} of all traffic this session", format::percent(share))
                } else {
                    "Nothing routed yet this session".into()
                }),
                chart_max: ss(label(chart.top_bps)),
                chart_mid: ss(label(chart.top_bps / 2.0)),
                chart: model(chart.series),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sonic_core::{ByteRange, DownloadId, plan_blocks};
    use std::path::PathBuf;

    #[test]
    fn connection_counts_read_naturally() {
        assert_eq!(connections(0, 0), "No connections");
        assert_eq!(connections(12, 0), "12 TCP");
        assert_eq!(connections(0, 3), "3 UDP");
        assert_eq!(connections(12, 3), "12 TCP · 3 UDP");
    }

    fn palette() -> Palette {
        let ids = ["teal", "blue", "amber", "violet", "pink"];
        Palette {
            ids: ids.iter().map(|s| s.to_string()).collect(),
            light: (0..5).map(|i| [i * 10, 0, 0]).collect(),
            dark: (0..5).map(|i| [0, i * 10, 0]).collect(),
            kind_ids: ["teal", "blue", "amber", "violet", "", "", ""]
                .iter()
                .map(|s| s.to_string())
                .collect(),
        }
    }

    fn net(
        id: &str,
        kind: NetworkKind,
        bytes: u64,
        speed: f64,
        status: NetworkStatus,
    ) -> DownloadNetwork {
        DownloadNetwork {
            id: NetworkId::new(id),
            label: id.to_uppercase(),
            kind,
            enabled: status != NetworkStatus::Off,
            status,
            error: None,
            bytes_downloaded: bytes,
            speed_bps: speed,
            retries: 0,
        }
    }

    fn download(status: DownloadStatus) -> DownloadState {
        DownloadState {
            id: DownloadId::new(),
            url: "https://cdn.example.com/files/big.iso".into(),
            file_name: "big.iso".into(),
            destination_path: PathBuf::from("D:/Downloads/big.iso"),
            total_bytes: Some(1000),
            bytes_downloaded: 400,
            speed_bps: 3.0 * 1024.0 * 1024.0,
            time_left_secs: Some(130.0),
            status,
            error: None,
            resumable: true,
            started_at: 0,
            queued_at: None,
            paused_at: None,
            total_paused_ms: 0,
            completed_at: None,
            scheduled: false,
            networks: vec![
                net("w", NetworkKind::Wifi, 100, 1024.0 * 1024.0, NetworkStatus::On),
                net("e", NetworkKind::Ethernet, 300, 2.0 * 1024.0 * 1024.0, NetworkStatus::On),
                net("c", NetworkKind::Cellular, 0, 0.0, NetworkStatus::Limit),
            ],
            streams: Vec::new(),
            peak_streams: 16,
            blocks: plan_blocks(Some(1000), 100),
            block_size: 100,
            speed_history: BTreeMap::new(),
            peak_speed_bps: None,
            disk_limited: false,
            from_browser: None,
        }
    }

    fn with_ctx<R>(f: impl FnOnce(&Ctx) -> R) -> R {
        let colors: BTreeMap<_, _> = [("w", 0), ("e", 1), ("c", 3)]
            .into_iter()
            .map(|(k, v)| (NetworkId::new(k), v))
            .collect();
        let mut prefs = BTreeMap::new();
        prefs.insert(
            NetworkId::new("e"),
            NetworkPreference { custom_name: Some("Desk".into()), ..Default::default() },
        );
        f(&Ctx { unit: SpeedUnit::Bytes, now: 10_000, colors: &colors, prefs: &prefs })
    }

    #[test]
    fn colors_prefer_picks_then_kinds_then_free() {
        let p = palette();
        let nets = vec![
            (NetworkId::new("a"), NetworkKind::Wifi),
            (NetworkId::new("b"), NetworkKind::Wifi),
            (NetworkId::new("c"), NetworkKind::Usb),
            (NetworkId::new("d"), NetworkKind::Other),
        ];
        let mut prefs = BTreeMap::new();
        // "d" picked amber, so the USB network can't have it.
        prefs.insert(
            NetworkId::new("d"),
            NetworkPreference { color_id: Some("amber".into()), ..Default::default() },
        );
        let colors = assign_colors(&nets, &prefs, &p);
        let of = |id: &str| colors[&NetworkId::new(id)];
        assert_eq!(of("a"), 0);
        assert_eq!(of("b"), 1, "a second Wi-Fi takes the first free hue");
        assert_eq!(of("d"), 2);
        assert_eq!(of("c"), 3);
        // A stale pick falls back as if unset.
        prefs.insert(
            NetworkId::new("a"),
            NetworkPreference { color_id: Some("gone".into()), ..Default::default() },
        );
        assert_eq!(assign_colors(&nets, &prefs, &p)[&NetworkId::new("a")], 0);
    }

    #[test]
    fn colors_cycle_when_swatches_run_out() {
        let p = palette();
        let nets: Vec<_> =
            (0..7).map(|i| (NetworkId::new(format!("n{i}")), NetworkKind::Other)).collect();
        let colors = assign_colors(&nets, &BTreeMap::new(), &p);
        assert_eq!(colors.values().copied().collect::<BTreeSet<_>>().len(), 5);
        assert!(colors.values().all(|c| (0..5).contains(c)));
    }

    #[test]
    fn downloading_row() {
        with_ctx(|ctx| {
            let d = download(DownloadStatus::Downloading);
            let row = row(Item::Live(&d), true, &DownloadSchedule::default(), ctx);
            assert_eq!(row.state, ui::RowState::Downloading);
            assert_eq!(row.detail, "40% · 400 B of 0.98 KB");
            assert_eq!(row.speed, "3.00 MB/s");
            assert_eq!(row.eta, "2m 10s left");
            assert!((row.progress - 0.4).abs() < 1e-6);
            assert!(row.selected);
            use slint::Model;
            // Only networks receiving right now get a dot.
            let nets: Vec<_> = row.nets.iter().map(|n| (n.kind, n.color)).collect();
            assert_eq!(nets, vec![(0, 0), (1, 1)]);
            assert_eq!(row.nets_tip, "Desk 75% · W 25%");
            let segs: Vec<_> = row.segments.iter().collect();
            assert_eq!(segs.len(), 2);
            assert!((segs[1].start - 0.1).abs() < 1e-6 && (segs[1].share - 0.3).abs() < 1e-6);
        });
    }

    #[test]
    fn other_rows_say_why_they_wait() {
        with_ctx(|ctx| {
            let schedule = DownloadSchedule { enabled: true, ..Default::default() };
            let mut d = download(DownloadStatus::Queued);
            d.scheduled = true;
            let queued = row(Item::Live(&d), false, &schedule, ctx);
            assert_eq!(queued.detail, "Waits for the schedule (01:00) · 400 B of 0.98 KB");
            assert_eq!(queued.speed, "");
            d.status = DownloadStatus::Paused;
            assert_eq!(
                row(Item::Live(&d), false, &schedule, ctx).detail,
                "Paused at 40% · 400 B of 0.98 KB"
            );
            d.status = DownloadStatus::Error;
            d.error = Some("The link expired".into());
            let failed = row(Item::Live(&d), false, &schedule, ctx);
            assert_eq!(
                (failed.detail.as_str(), failed.detail_tone),
                ("The link expired", ui::Tone::Danger)
            );
            assert_eq!(Item::Live(&d).group(), Some(3));
        });
    }

    #[test]
    fn summary_counts_states() {
        let a = download(DownloadStatus::Downloading);
        let b = download(DownloadStatus::Downloading);
        let c = download(DownloadStatus::Error);
        assert_eq!(summary(&[&a, &b, &c]), "2 downloading · 1 need attention");
        assert_eq!(summary(&[]), "Nothing downloading");
    }

    #[test]
    fn filters_pick_groups() {
        assert!(filter_shows(0, 4));
        assert!(filter_shows(1, 2) && !filter_shows(1, 3));
        assert!(filter_shows(2, FINISHED_GROUP) && !filter_shows(2, 0));
        assert!(filter_shows(3, 3) && !filter_shows(3, 4));
    }

    #[test]
    fn multiplier_compares_with_the_fastest_alone() {
        with_ctx(|ctx| {
            let d = download(DownloadStatus::Downloading);
            let (factor, text) = multiplier(&d.networks, d.speed_bps, ctx).unwrap();
            assert_eq!(factor, "1.5×");
            assert_eq!(text, "faster than Desk alone", "uses the custom name");
            // One network alone isn't "faster than itself".
            assert!(multiplier(&d.networks[1..2], 2e6, ctx).is_none());
        });
    }

    #[test]
    fn net_lines_explain_status_and_share() {
        with_ctx(|ctx| {
            let d = download(DownloadStatus::Downloading);
            let lines = net_lines(&d.networks, &d.streams, true, &BTreeSet::new(), ctx);
            assert_eq!(lines[1].name, "Desk");
            assert_eq!(lines[1].share_text, "75%");
            assert_eq!(lines[1].weight, 1.0);
            assert_eq!(lines[0].weight, 0.5);
            assert_eq!(lines[2].status, "Data limit");
            assert_eq!(lines[2].status_tone, ui::Tone::Warning);
            assert!(!lines[2].explain.is_empty());
            assert_eq!(lines[2].speed, "—");
            assert!((lines[0].label_y - 1.0 / 6.0).abs() < 1e-6);
        });
    }

    #[test]
    fn ribbons_stack_by_weight() {
        with_ctx(|ctx| {
            let d = download(DownloadStatus::Downloading);
            let mut lines = net_lines(&d.networks, &d.streams, true, &BTreeSet::new(), ctx);
            let (top, bottom) = flow_ribbons(&mut lines);
            // Weights 0.5, 1 and 0: thicknesses 13, 20 and 1.2 plus two gaps, around the middle.
            assert!((bottom - top - 0.372).abs() < 1e-3);
            assert!((top + bottom - 1.0).abs() < 1e-6, "centred");
            assert!(lines[0].curve.starts_with("M0 10.17C50 10.17 50 31.40 100 31.40L100 44.40"));
            assert!(lines.iter().all(|l| l.curve.ends_with('Z')));
        });
    }

    #[test]
    fn expanded_streams_come_four_to_a_row() {
        with_ctx(|ctx| {
            let mut d = download(DownloadStatus::Downloading);
            d.streams = (0..5)
                .map(|i| StreamState {
                    id: i,
                    network_id: NetworkId::new("w"),
                    status: StreamStatus::Downloading,
                    block: Some(i * 10),
                    hedge: i == 4,
                    bytes_downloaded: 0,
                    speed_bps: 2048.0,
                })
                .collect();
            let expanded: BTreeSet<_> = [NetworkId::new("w")].into();
            let lines = net_lines(&d.networks, &d.streams, true, &expanded, ctx);
            use slint::Model;
            let rows: Vec<_> = lines[0].streams.iter().collect();
            assert_eq!(rows.len(), 2);
            let last: Vec<_> = rows[1].cells.iter().collect();
            assert_eq!(last.len(), 4, "padded");
            assert_eq!(last[0].label, "Block #41");
            assert!(last[0].hedge);
            assert_eq!(last[1].label, "");
            assert_eq!(lines[0].hedges, 1);
            assert_eq!(lines[1].streams.row_count(), 0, "collapsed networks build nothing");
        });
    }

    #[test]
    fn chart_stacks_series() {
        let a = NetworkId::new("a");
        let b = NetworkId::new("b");
        let mb = 1024.0 * 1024.0;
        let history: BTreeMap<_, _> =
            [(a.clone(), vec![mb, mb]), (b.clone(), vec![mb, 3.0 * mb])].into();
        let chart = chart(&history, &[(a, 0), (b, 1)], None, SpeedUnit::Bytes);
        assert_eq!(chart.series.len(), 2);
        // Peak 4 MB/s, with headroom, rounds to a top of 5 MB/s.
        assert_eq!(chart.top_bps, 5.0 * mb);
        assert_eq!(chart.series[0].area, "M0.0 80.00L600.0 80.00L600.0 100.00L0.0 100.00Z");
        assert!(chart.series[1].area.starts_with("M0.0 60.00L600.0 20.00"));
        // A chart with room for 60 samples puts two at the right edge.
        let history: BTreeMap<_, _> = [(NetworkId::new("a"), vec![mb, mb])].into();
        let wide = super::chart(&history, &[(NetworkId::new("a"), 0)], Some(60), SpeedUnit::Bytes);
        assert!(wide.series[0].area.starts_with("M589.8 "));
        assert!(super::chart(&BTreeMap::new(), &[], None, SpeedUnit::Bytes).series.is_empty());
    }

    #[test]
    fn nice_tops() {
        let mb = 1024.0 * 1024.0;
        assert_eq!(nice_top(43.0 * mb, SpeedUnit::Bytes), 50.0 * mb);
        assert_eq!(nice_top(11.0 * mb, SpeedUnit::Bytes), 20.0 * mb);
        assert_eq!(nice_top(2.1 * mb, SpeedUnit::Bytes), 2.5 * mb);
        assert_eq!(nice_top(80e6 / 8.0, SpeedUnit::Bits), 100e6 / 8.0);
    }

    #[test]
    fn grid_layout_shrinks_cells_to_fit() {
        let (pitch, gap, cols) = grid_layout(20, 800.0);
        assert_eq!((pitch, gap), (16.0, 2.0));
        assert_eq!(cols, 20, "never more columns than blocks");
        let (pitch, _, cols) = grid_layout(1460, 800.0);
        let rows = 1460usize.div_ceil(cols);
        assert!(rows as f32 * pitch <= GRID_MAX_HEIGHT + 2.0);
        assert!(pitch >= 6.0, "1,460 blocks still get visible cells");
        let (pitch, _, _) = grid_layout(100_000, 300.0);
        assert_eq!(pitch, 4.0, "the smallest cell is the floor");
    }

    #[test]
    fn block_grid_paints_by_owner() {
        let mut blocks = plan_blocks(Some(400), 100);
        blocks[0].status = BlockStatus::Completed;
        blocks[0].bytes_downloaded = 100;
        blocks[0].bytes_by_network.insert(NetworkId::new("a"), 100);
        blocks[1].status = BlockStatus::Downloading;
        blocks[1].bytes_downloaded = 50;
        blocks[1].network_id = Some(NetworkId::new("a"));
        let colors = GridColors { track: [200, 200, 200], hedge: [255, 160, 0] };
        let grid = block_grid(&blocks, &BTreeSet::new(), &|_| [10, 20, 30], colors, 200.0, 1.0);
        assert_eq!(grid.cols, 4);
        assert_eq!((grid.w, grid.h), (62.0, 14.0));
        let buffer = grid.image.to_rgba8().unwrap();
        let px = |x: usize, y: usize| buffer.as_slice()[y * buffer.width() as usize + x];
        assert_eq!(px(5, 5).r, 10, "done: the owner's colour");
        assert_eq!(px(16 + 2, 5).r, 10, "in flight: filled as far as it got");
        assert_ne!(px(16 + 12, 5).r, 10, "…and tinted beyond");
        assert_eq!(px(32 + 5, 5).r, 200, "waiting: track");
        assert_eq!(px(0, 0).a, 0, "rounded corner");
        assert_eq!(px(14, 5).a, 0, "gap");
    }

    #[test]
    fn block_info_names_who_delivered() {
        with_ctx(|ctx| {
            let mut d = download(DownloadStatus::Downloading);
            d.blocks[0].status = BlockStatus::Completed;
            d.blocks[0].bytes_by_network.insert(NetworkId::new("e"), 70);
            d.blocks[0].bytes_by_network.insert(NetworkId::new("w"), 30);
            assert_eq!(block_info(&d, 0, ctx), "Block #1 · 0 B – 100 B · done by Desk 70%, W 30%");
            d.blocks[2].range = ByteRange::new(200, Some(300));
            assert_eq!(block_info(&d, 2, ctx), "Block #3 · 200 B – 300 B · waiting");
            assert_eq!(block_info(&d, 99, ctx), "");
        });
    }

    #[test]
    fn chips_warn_and_block() {
        use sonic_core::InterfaceAddress;
        let iface = |id: &str, gateway: bool| NetworkInterface {
            id: NetworkId::new(id),
            name: id.into(),
            description: String::new(),
            kind: NetworkKind::Wifi,
            addresses: vec![InterfaceAddress {
                ip: "192.168.1.20".parse().unwrap(),
                prefix_len: 24,
            }],
            ipv4_index: 1,
            ipv6_index: 0,
            has_gateway: gateway,
            mac: None,
        };
        let nets = vec![iface("w", true), iface("v", false), iface("c", true)];
        let colors = BTreeMap::new();
        let mut prefs = BTreeMap::new();
        prefs.insert(
            NetworkId::new("c"),
            NetworkPreference { data_limit: Some(100), ..Default::default() },
        );
        let usage: BTreeMap<_, _> = [(NetworkId::new("c"), 150)].into();
        let ctx = Ctx { unit: SpeedUnit::Bytes, now: 0, colors: &colors, prefs: &prefs };
        let chosen = default_choice(&nets, &usage, &prefs);
        assert_eq!(chosen.len(), 1, "over its allowance or no gateway: not ticked");
        let chips = chips(&nets, &usage, &chosen, &ctx);
        assert_eq!(chips[0].sub, "Wi-Fi · 192.168.1.20");
        assert!(chips[1].warn && !chips[1].selected);
        assert!(chips[2].blocked && !chips[2].selected);
        assert_eq!(chips[2].sub, "Used its 100 B a month");
    }

    #[test]
    fn stream_choices() {
        assert_eq!(streams_choice(0), None);
        assert_eq!(streams_choice(1), Some(4));
        assert_eq!(streams_choice(4), Some(32));
        assert_eq!(streams_choice(9), None);
    }

    #[test]
    fn settings_round_trip_through_the_form() {
        let base = AppSettings { speed_limit: Some(5 * 1024 * 1024), ..AppSettings::default() };
        let id = NetworkId::new("w");
        let form = form_of(&base, &[(id.clone(), "Wi-Fi".into())]);
        assert!(form.limit_on);
        assert_eq!(form.limit, "5");
        assert_eq!(settings_from_form(&base, &form).unwrap().speed_limit, base.speed_limit);

        let mut edited = form.clone();
        edited.unit = 1;
        edited.limit = "80".into();
        edited.nets[0].name = "  Home  ".into();
        edited.nets[0].data = "2".into();
        edited.nets[0].period = 0;
        let saved = settings_from_form(&base, &edited).unwrap();
        assert_eq!(saved.speed_unit, SpeedUnit::Bits);
        assert_eq!(saved.speed_limit, Some(10_000_000));
        let pref = &saved.network_preferences[&id];
        assert_eq!(pref.custom_name.as_deref(), Some("Home"));
        assert_eq!(pref.data_limit, Some(2 * 1024 * 1024 * 1024));
        assert_eq!(pref.data_period, DataPeriod::Day);
    }

    #[test]
    fn settings_form_rejects_nonsense() {
        let base = AppSettings::default();
        let form = form_of(&base, &[]);
        let bad = |f: &dyn Fn(&mut Form)| {
            let mut form = form.clone();
            f(&mut form);
            settings_from_form(&base, &form).unwrap_err()
        };
        assert!(bad(&|f| f.slow_speed = "lots".into()).contains("isn't a number"));
        assert!(bad(&|f| f.sched_start = "25:00".into()).contains("isn't a time"));
        assert!(
            bad(&|f| {
                f.sched_on = true;
                f.sched_days = [false; 7];
            })
            .contains("at least one day")
        );
        assert!(
            bad(&|f| {
                f.sched_end_on = true;
                f.sched_end = "tomorrow".into();
            })
            .contains("isn't a date")
        );
    }

    #[test]
    fn schedule_windows() {
        let s = DownloadSchedule { enabled: true, ..Default::default() }; // 01:00–07:00 every day
        let day = NaiveDate::from_ymd_opt(2026, 10, 17).unwrap();
        assert!(schedule_open(&s, day, 6, 90));
        assert!(!schedule_open(&s, day, 6, 7 * 60));
        let night = DownloadSchedule {
            start_minute: 22 * 60,
            end_minute: 6 * 60,
            days: vec![5],
            ..s.clone()
        };
        assert!(schedule_open(&night, day, 5, 23 * 60), "Friday night");
        assert!(schedule_open(&night, day, 6, 60), "early Saturday is still Friday's window");
        assert!(
            !schedule_open(&night, day, 0, 60),
            "Sunday morning follows Saturday: not scheduled"
        );
        let ended = DownloadSchedule { end_date: Some("2026-10-01".into()), ..s.clone() };
        assert!(!schedule_open(&ended, day, 6, 90));
        assert!(schedule_open(&DownloadSchedule::default(), day, 6, 90), "disabled: always open");
    }

    #[test]
    fn schedule_sentences() {
        let all = [true; 7];
        assert_eq!(
            schedule_sentence(true, "1:00", "7:00", &all),
            "Downloads run from 01:00 to 07:00, every day."
        );
        let weekend = [true, false, false, false, false, false, true];
        assert_eq!(
            schedule_sentence(true, "22:00", "06:00", &weekend),
            "Downloads run from 22:00 to 06:00 the next morning, on Sun, Sat."
        );
        assert_eq!(
            schedule_sentence(false, "", "", &all),
            "Downloads run whenever they're started."
        );
    }
}
