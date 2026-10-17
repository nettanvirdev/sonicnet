// The SonicNet icon, drawn once as shapes and exported both as SVG and as PNG.
//
// Three connections merging into one download arrow. The shapes are only a rounded square and
// round-capped strokes, so a ~40-line rasterizer (signed distances, 4×4 supersampling) renders
// them exactly, and the PNGs need no image library or native tool.

import { crc32, deflateSync } from 'node:zlib'

const GRID = 128
const BACK = { x: 4, y: 4, size: 120, radius: 28 }
const STROKE = 12
/** [x1, y1, x2, y2] on the 128 grid. */
const LINES = [
  [38, 26, 64, 58], // left connection
  [90, 26, 64, 58], // right connection
  [64, 24, 64, 84], // the merged stream
  [42, 62, 64, 84], // arrow head
  [86, 62, 64, 84],
  [38, 104, 90, 104] // the line it lands on
]

export const VARIANTS = {
  // Flat, like the rest of the design system: a solid black tile with a white mark.
  on: { back: '#000000', fore: '#ffffff' },
  // Grey: capture is off or the app isn't running.
  off: { back: '#9b9b9b', fore: '#ffffff' }
}

export function iconSvg(variant) {
  const { back, fore } = VARIANTS[variant]
  const path = LINES.map(([x1, y1, x2, y2]) => `M${x1} ${y1}L${x2} ${y2}`).join('')
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${GRID} ${GRID}">
  <rect x="${BACK.x}" y="${BACK.y}" width="${BACK.size}" height="${BACK.size}" rx="${BACK.radius}" fill="${back}"/>
  <path d="${path}" fill="none" stroke="${fore}" stroke-width="${STROKE}" stroke-linecap="round" stroke-linejoin="round"/>
</svg>
`
}

function hex(color) {
  return [1, 3, 5].map((i) => parseInt(color.slice(i, i + 2), 16))
}

function inBack(px, py) {
  const half = BACK.size / 2
  const qx = Math.abs(px - (BACK.x + half)) - (half - BACK.radius)
  const qy = Math.abs(py - (BACK.y + half)) - (half - BACK.radius)
  const outside = Math.hypot(Math.max(qx, 0), Math.max(qy, 0)) + Math.min(Math.max(qx, qy), 0)
  return outside <= BACK.radius
}

function onStroke(px, py) {
  return LINES.some(([x1, y1, x2, y2]) => {
    const dx = x2 - x1
    const dy = y2 - y1
    const t = Math.max(0, Math.min(1, ((px - x1) * dx + (py - y1) * dy) / (dx * dx + dy * dy)))
    return Math.hypot(px - (x1 + t * dx), py - (y1 + t * dy)) <= STROKE / 2
  })
}

export function iconPng(variant, size) {
  const back = hex(VARIANTS[variant].back)
  const fore = hex(VARIANTS[variant].fore)
  const SS = 4
  const scale = GRID / size
  // Each row starts with its filter byte (0: none).
  const raw = Buffer.alloc(size * (size * 4 + 1))
  for (let y = 0; y < size; y++) {
    for (let x = 0; x < size; x++) {
      // Premultiplied sums, so edges blend into transparency without dark fringes.
      let r = 0, g = 0, b = 0, a = 0
      for (let sy = 0; sy < SS; sy++) {
        for (let sx = 0; sx < SS; sx++) {
          const px = (x + (sx + 0.5) / SS) * scale
          const py = (y + (sy + 0.5) / SS) * scale
          if (!inBack(px, py)) continue
          const c = onStroke(px, py) ? fore : back
          r += c[0]; g += c[1]; b += c[2]; a += 1
        }
      }
      const at = y * (size * 4 + 1) + 1 + x * 4
      if (a > 0) raw.set([r / a, g / a, b / a, (255 * a) / (SS * SS)].map(Math.round), at)
    }
  }
  const chunk = (type, data) => {
    const body = Buffer.concat([Buffer.from(type, 'ascii'), data])
    const length = Buffer.alloc(4)
    length.writeUInt32BE(data.length)
    const crc = Buffer.alloc(4)
    crc.writeUInt32BE(crc32(body))
    return Buffer.concat([length, body, crc])
  }
  const header = Buffer.alloc(13)
  header.writeUInt32BE(size, 0)
  header.writeUInt32BE(size, 4)
  header.set([8, 6, 0, 0, 0], 8) // 8-bit RGBA, no interlace
  return Buffer.concat([
    Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
    chunk('IHDR', header),
    chunk('IDAT', deflateSync(raw)),
    chunk('IEND', Buffer.alloc(0))
  ])
}
