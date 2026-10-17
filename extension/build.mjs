// Builds dist/chrome (Chrome, Edge and other Chromium browsers) and dist/firefox from one source.
// Usage: node build.mjs   (npm run build also type-checks first)

import { cp, mkdir, readFile, rm, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { build } from 'esbuild'
import { iconPng, iconSvg } from './icons.mjs'

const root = dirname(fileURLToPath(import.meta.url))
const dist = join(root, 'dist')
const { version } = JSON.parse(await readFile(join(root, 'package.json'), 'utf8'))
const base = { ...JSON.parse(await readFile(join(root, 'manifest.json'), 'utf8')), version }

// Each browser gets only the background key it uses: given both, Chrome reports the other one as
// an error on the extensions page.
const manifests = {
  chrome: {
    ...base,
    // The first Chrome where every API we call returns a promise.
    minimum_chrome_version: '123',
    background: { service_worker: 'background.js' }
  },
  firefox: {
    ...base,
    background: { scripts: ['background.js'] },
    browser_specific_settings: {
      gecko: {
        id: 'browser-extension@sonicnet.app',
        // The first Firefox that reads data_collection_permissions.
        strict_min_version: '140.0',
        // Everything goes to the SonicNet app on 127.0.0.1; nothing leaves the computer.
        data_collection_permissions: { required: ['none'] }
      }
    }
  }
}

await rm(dist, { recursive: true, force: true })
for (const [browser, manifest] of Object.entries(manifests)) {
  const out = join(dist, browser)
  await build({
    entryPoints: ['background', 'popup', 'options'].map((name) => join(root, 'src', `${name}.ts`)),
    outdir: out,
    bundle: true,
    // Classic scripts: Firefox's background page and the HTML pages load them without modules.
    format: 'iife',
    target: browser === 'chrome' ? 'chrome123' : 'firefox140',
    logLevel: 'warning'
  })
  for (const file of ['popup.html', 'options.html', 'ui.css']) {
    await cp(join(root, 'src', file), join(out, file))
  }
  await mkdir(join(out, 'icons'))
  await writeFile(join(out, 'icons', 'icon.svg'), iconSvg('on'))
  for (const size of [16, 32, 48, 128]) {
    await writeFile(join(out, 'icons', `icon-${size}.png`), iconPng('on', size))
  }
  for (const size of [16, 32]) {
    await writeFile(join(out, 'icons', `icon-off-${size}.png`), iconPng('off', size))
  }
  await writeFile(join(out, 'manifest.json'), `${JSON.stringify(manifest, null, 2)}\n`)
}
console.log(`Built SonicNet ${version} into dist/chrome and dist/firefox`)
