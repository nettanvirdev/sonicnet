//! Regenerates every raster icon from the SVG masters in `assets/`.
//!
//! Run from anywhere: `cargo run --release --manifest-path packaging/tools/icon-gen/Cargo.toml`.
//! Outputs (overwritten):
//! - `assets/png/sonicnet-{16..512}.png` and `assets/sonicnet.ico` (16–256, BMP ≤ 64, PNG above)
//! - `assets/png/tray-{white,black}-{16,20,24,32,48}.png` and `assets/tray-{white,black}.ico`

use ico::{IconDir, IconDirEntry, IconImage, ResourceType};
use resvg::{tiny_skia, usvg};
use std::{fs, path::Path};

const APP_SIZES: &[u32] = &[16, 24, 32, 48, 64, 128, 256, 512];
// 20 px is the tray size at 125 % scaling.
const TRAY_SIZES: &[u32] = &[16, 20, 24, 32, 48];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let assets = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../assets");
    let png_dir = assets.join("png");
    fs::create_dir_all(&png_dir)?;

    let app = load(&assets.join("sonicnet.svg"))?;
    let tray = load(&assets.join("sonicnet-tray.svg"))?;

    let mut app_ico = IconDir::new(ResourceType::Icon);
    for &size in APP_SIZES {
        let img = render(&app, size, None);
        img.write_png(fs::File::create(png_dir.join(format!("sonicnet-{size}.png")))?)?;
        // ICO entries top out at 256 px.
        if size <= 256 {
            app_ico.add_entry(IconDirEntry::encode(&img)?);
        }
    }
    app_ico.write(fs::File::create(assets.join("sonicnet.ico"))?)?;

    for (name, tint) in [("white", None), ("black", Some([0, 0, 0]))] {
        let mut tray_ico = IconDir::new(ResourceType::Icon);
        for &size in TRAY_SIZES {
            let img = render(&tray, size, tint);
            img.write_png(fs::File::create(png_dir.join(format!("tray-{name}-{size}.png")))?)?;
            tray_ico.add_entry(IconDirEntry::encode(&img)?);
        }
        tray_ico.write(fs::File::create(assets.join(format!("tray-{name}.ico")))?)?;
    }

    println!("icons written to {}", assets.display());
    Ok(())
}

fn load(path: &Path) -> Result<usvg::Tree, Box<dyn std::error::Error>> {
    Ok(usvg::Tree::from_data(&fs::read(path)?, &usvg::Options::default())?)
}

/// Renders the SVG to a `size`×`size` straight-alpha image, optionally recolouring every pixel.
fn render(tree: &usvg::Tree, size: u32, tint: Option<[u8; 3]>) -> IconImage {
    let mut pixmap = tiny_skia::Pixmap::new(size, size).expect("icon sizes are non-zero");
    let scale = size as f32 / tree.size().width();
    resvg::render(tree, tiny_skia::Transform::from_scale(scale, scale), &mut pixmap.as_mut());
    let rgba: Vec<u8> = pixmap
        .pixels()
        .iter()
        .flat_map(|p| {
            let c = p.demultiply();
            let [r, g, b] = tint.unwrap_or([c.red(), c.green(), c.blue()]);
            [r, g, b, c.alpha()]
        })
        .collect();
    IconImage::from_rgba_data(size, size, rgba)
}
