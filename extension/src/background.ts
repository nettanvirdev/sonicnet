// Watches the browser's downloads and hands them to SonicNet when it's running.
//
// The rule everything follows: the browser keeps a download unless SonicNet has confirmed it has
// it. Any failure along the way (app closed, a refusal, an exception) ends in the browser's own
// download, never in neither.

import { LAUNCH_URL, buildAddRequest, cookieStoreFor, isExcluded, isHttp, isTooSmall, knownSize, minBytes } from './logic.ts'
import {
  addDownload,
  api,
  confirmDownload,
  findApp,
  isFirefox,
  loadSettings,
  readCookies,
  thisBrowser,
  type AppState
} from './platform.ts'

// Firefox adds the container (or private store) a download or tab belongs to.
type Item = chrome.downloads.DownloadItem & { cookieStoreId?: string }
type Tab = chrome.tabs.Tab & { cookieStoreId?: string }
type Source = { incognito?: boolean; cookieStoreId?: string }

const MENU_ID = 'sonicnet-download-link'
const ICON = { 16: 'icons/icon-16.png', 32: 'icons/icon-32.png' }
// A greyed-out icon reads better than a badge, which browsers draw too small to notice.
const ICON_OFF = { 16: 'icons/icon-off-16.png', 32: 'icons/icon-off-32.png' }

/** Downloads we start ourselves, after a hand-off fell through: never capture those again. */
const ownUrls = new Set<string>()

// ponytail: the icon refreshes on startup, settings changes, each capture and each popup open,
// not on a timer; add an `alarms` poll if a stale "connected" icon after SonicNet quits matters.
async function refreshIcon(state?: AppState): Promise<void> {
  const { enabled } = await loadSettings()
  state ??= (await findApp()).state
  const title = !enabled
    ? 'SonicNet: capture is off'
    : state === 'not-running'
      ? 'SonicNet isn’t running: downloads stay in the browser'
      : state === 'outdated'
        ? 'SonicNet: update the app to use this extension'
        : 'SonicNet: capturing downloads'
  await api.action.setIcon({ path: enabled && state === 'connected' ? ICON : ICON_OFF })
  await api.action.setTitle({ title })
}

/** Starts `url` in the browser after SonicNet didn't end up with it, in the same profile or
 * container the original was in. */
async function downloadInBrowser(url: string, from: Source): Promise<void> {
  ownUrls.add(url)
  try {
    if (isFirefox) {
      const store = from.cookieStoreId
      const options = {
        url,
        // Firefox takes either a container or `incognito`, and names its private store itself.
        ...(store && store !== 'firefox-private' ? { cookieStoreId: store } : {}),
        ...(from.incognito ? { incognito: true } : {})
      }
      await api.downloads.download(options as chrome.downloads.DownloadOptions)
    } else if (from.incognito) {
      // Chromium can't start an incognito download from here; opening the link in an incognito
      // window keeps it there instead of leaking it into the regular profile's history.
      const window = (await api.windows.getAll()).find((w) => w.incognito)
      if (window?.id !== undefined) await api.tabs.create({ windowId: window.id, url, active: false })
    } else {
      await api.downloads.download({ url })
    }
  } catch {
    ownUrls.delete(url)
  }
}

/** Returns true when the browser's copy is gone (SonicNet has it, or we restarted it). */
async function capture(item: Item): Promise<boolean> {
  if (ownUrls.delete(item.url) || item.byExtensionId === api.runtime.id) return false
  const settings = await loadSettings()
  const url = item.finalUrl || item.url
  // blob:, data: and file: downloads exist only inside the browser.
  if (!settings.enabled || !isHttp(url) || item.state !== 'in_progress') return false
  if (isExcluded(settings.excludedSites, [item.url, item.finalUrl, item.referrer])) return false
  const min = minBytes(settings)
  if (isTooSmall(knownSize(item), min)) return false

  const request = buildAddRequest({
    url,
    pageUrl: item.referrer,
    referrer: item.referrer,
    fileName: item.filename,
    cookies: await readCookies([item.url, item.finalUrl], cookieStoreFor(item), item.referrer),
    ...thisBrowser(),
    minBytes: min
  })
  if (!request) return false
  const result = await addDownload(request)
  if (result.kind !== 'taken') {
    if (result.kind === 'unreachable') void refreshIcon('not-running')
    return false
  }

  // SonicNet only lists it once we confirm, and we confirm only once the browser's copy is gone.
  await api.downloads.cancel(item.id).catch(() => {})
  const [after] = await api.downloads.search({ id: item.id }).catch(() => [])
  // It finished before the cancel landed (Firefox can't hold a download while SonicNet probes it):
  // the browser has the file, so we never confirm and the token expires in the app.
  if (after?.state === 'complete') return false
  await api.downloads.erase({ id: item.id }).catch(() => {})
  if (await confirmDownload(result.port, result.token)) {
    void refreshIcon('connected')
  } else {
    // The browser's copy is gone and SonicNet didn't take it (it quit, or the token expired).
    await downloadInBrowser(item.url, item)
  }
  return true
}

// Chromium: answering asynchronously holds the browser's Save dialog until SonicNet has decided,
// so the user is never asked twice. Firefox has no such hook, so it captures right after start.
if (!isFirefox) {
  api.downloads.onDeterminingFilename.addListener((item, suggest) => {
    capture(item)
      .catch(() => false)
      .then((gone) => {
        if (!gone) suggest()
      })
    return true
  })
} else {
  api.downloads.onCreated.addListener((item) => void capture(item).catch(() => {}))
}

// ---- "Download with SonicNet" on a link -------------------------------------------------------

/** Only ever on a click: the browser asks the user whether to open SonicNet. */
async function launchApp(): Promise<boolean> {
  const tab = await api.tabs.create({ url: LAUNCH_URL }).catch(() => undefined)
  let state: AppState = 'not-running'
  for (let tries = 0; tries < 20 && state !== 'connected'; tries++) {
    await new Promise((resolve) => setTimeout(resolve, 500))
    state = (await findApp()).state
  }
  // The tab never shows anything: the scheme only hands off to the app (or isn't registered).
  if (tab?.id !== undefined) await api.tabs.remove(tab.id).catch(() => {})
  void refreshIcon(state)
  return state === 'connected'
}

/** True once SonicNet has the link in its list. */
async function sendToApp(url: string, pageUrl: string | undefined, source: Source): Promise<boolean> {
  // The user asked for SonicNet by name, so the capture switch, exclusions and size limit don't apply.
  const request = buildAddRequest({
    url,
    pageUrl,
    referrer: pageUrl,
    cookies: await readCookies([url], cookieStoreFor(source), pageUrl),
    ...thisBrowser()
  })
  if (!request) return false
  let result = await addDownload(request)
  if (result.kind === 'unreachable' && (await launchApp())) result = await addDownload(request)
  // No browser copy exists yet, so there's nothing to cancel before confirming.
  return result.kind === 'taken' && (await confirmDownload(result.port, result.token))
}

async function sendLink(info: chrome.contextMenus.OnClickData, tab?: Tab): Promise<void> {
  const url = info.linkUrl
  if (!isHttp(url)) return
  const source = { incognito: tab?.incognito, cookieStoreId: tab?.cookieStoreId }
  // Whatever went wrong, the click still downloads, in the browser.
  if (!(await sendToApp(url, info.pageUrl, source).catch(() => false))) await downloadInBrowser(url, source)
}

// Menus don't always survive a browser restart, so they're rebuilt on every start.
function setUp(): void {
  void api.contextMenus
    .removeAll()
    .then(() =>
      api.contextMenus.create({
        id: MENU_ID,
        title: 'Download with SonicNet',
        contexts: ['link'],
        // Only links SonicNet can fetch; blob:, data: and the like exist only in the browser.
        targetUrlPatterns: ['http://*/*', 'https://*/*']
      })
    )
  void refreshIcon()
}

api.runtime.onInstalled.addListener(setUp)
api.runtime.onStartup.addListener(setUp)
api.storage.onChanged.addListener(() => void refreshIcon())
api.contextMenus.onClicked.addListener((info, tab) => {
  if (info.menuItemId === MENU_ID) void sendLink(info, tab).catch(() => {})
})
api.runtime.onMessage.addListener((message: unknown) => {
  const type = (message as { type?: unknown } | null)?.type
  if (type === 'refresh') void refreshIcon()
  if (type === 'launch') void launchApp()
})
