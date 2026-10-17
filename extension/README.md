# SonicNet browser extension

Hands the browser's downloads to the SonicNet desktop app, which downloads them over all your
connections at once. When SonicNet isn't running, can't take a file, or anything goes wrong, the
browser keeps the download as if the extension weren't there.

Works in Chrome, Edge and other Chromium browsers (Manifest V3, service worker) and in Firefox 140+
(Manifest V3, background script). Both builds come from the same source.

## Build

Needs Node 22.18+ (the tests run TypeScript directly).

```sh
cd extension
npm install
npm run build   # type-checks, then writes dist/chrome and dist/firefox
npm test        # unit tests (node --test)
```

`build.mjs` bundles `src/*.ts` with esbuild, copies the pages, writes one manifest per browser
from `manifest.json`, and draws the icons from `icons.mjs` (SVG plus PNG at 16/32/48/128 and a
grey "off" variant at 16/32, rendered by a small built-in rasterizer, so no image tools needed).

## Load it unpacked

- **Chrome:** open `chrome://extensions`, turn on *Developer mode*, choose *Load unpacked*, and pick
  `extension/dist/chrome`.
- **Edge:** open `edge://extensions`, turn on *Developer mode*, choose *Load unpacked*, and pick
  `extension/dist/chrome`.
- **Firefox:** open `about:debugging#/runtime/this-firefox`, choose *Load Temporary Add-on…*, and
  pick `extension/dist/firefox/manifest.json`. A temporary add-on lasts until Firefox restarts.

To capture downloads in private windows, also allow the extension there (Chrome/Edge: *Allow in
Incognito* on its details page; Firefox: *Run in Private Windows*).

## How it works

1. The browser starts a download. In Chrome and Edge the extension answers
   `downloads.onDeterminingFilename` later, which holds the Save dialog while SonicNet decides.
   Firefox has no such hook, so it uses `downloads.onCreated` instead.
2. It's skipped (the browser keeps it) when capture is off, the link isn't http(s), the site is
   excluded (subdomains included; the download's address, where it redirected, and the page it came
   from all count), or the browser already knows it's under the minimum size.
3. Otherwise the extension finds SonicNet on `127.0.0.1` ports 19320–19322 (`GET /ping`) and sends
   the link with your cookies for it, the referring page, the user agent and the browser's file name
   (`POST /add`). SonicNet checks the link and answers within 8 seconds.
4. If SonicNet takes it, the extension cancels and erases the browser's copy, then confirms
   (`POST /confirm`), and only then does SonicNet show it. If the browser finished first, or the
   confirmation fails, the browser keeps (or restarts) its own download, so it's never lost and
   never in both.
5. "Download with SonicNet" on a link's right-click menu sends that link regardless of the capture
   settings, and offers to open SonicNet (`sonicnet://open`) if it isn't running.

The popup shows whether SonicNet is running, the capture switch, and a switch to exclude the
current site. The options page has the exclusion list and the minimum size.

## Permissions

| Permission | Why |
| --- | --- |
| `downloads` | See downloads as they start, hold Chromium's Save dialog while SonicNet decides, and cancel and erase the browser's copy once SonicNet has it. |
| `cookies` | Read the cookies for the download's address so SonicNet can fetch files that need you to be signed in. Read from the same store as the download (incognito, Firefox containers). |
| `contextMenus` | The "Download with SonicNet" item on links. |
| `storage` | Your settings: the capture switch, excluded sites, minimum size. |
| Host access to `http://*/*` and `https://*/*` | The cookies API only returns cookies for sites the extension has host access to, and downloads can come from any site. It also lets the popup read the current tab's address for "exclude this site" without the broader `tabs` permission, and covers the requests to SonicNet on 127.0.0.1. The extension injects nothing into pages. |

Not requested: `tabs`, `webRequest`, `alarms`, `notifications`, or any content scripts.

## Privacy

Nothing leaves your computer. The extension talks only to the SonicNet app on `127.0.0.1` (this
machine), and only sends it what it needs to download a file you started: its address, the page it
came from, the cookies for that site, your browser's user agent and name, and the file name. Your
settings stay in the browser's local extension storage. There's no analytics, telemetry or remote
server, and the Firefox build declares no data collection.

## Layout

```
manifest.json      shared manifest; build.mjs adds the per-browser keys
build.mjs          build script (esbuild)
icons.mjs          the icon as shapes → SVG and PNG
src/logic.ts       pure rules: protocol limits, exclusions, size, cookies, /add body
src/platform.ts    browser API shim, settings storage, talking to SonicNet, reading cookies
src/background.ts  capture, hand-off, context menu, toolbar icon
src/popup.*        toolbar popup
src/options.*      options page
src/ui.css         shared styles (SonicNet design tokens, light and dark)
test/              unit tests for src/logic.ts
```
