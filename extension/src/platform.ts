// The browser-facing half: storage, talking to the app, reading cookies. Shared by the background
// script, the popup and the options page.

import {
  ADD_TIMEOUT_MS,
  API_VERSION,
  CLIENT_HEADER,
  CLIENT_VALUE,
  CONFIRM_TIMEOUT_MS,
  DEFAULT_SETTINGS,
  PING_TIMEOUT_MS,
  PORTS,
  browserName,
  hostOf,
  isHttp,
  normalizeSettings,
  selectCookies,
  type AddRequest,
  type BrowserCookie,
  type Settings,
  type WireCookie
} from './logic.ts'

// Firefox gives promises only on `browser` (its `chrome` takes callbacks); Chromium's `chrome`
// returns promises, and newer versions alias it as `browser` too.
export const api: typeof chrome = (globalThis as { browser?: typeof chrome }).browser ?? chrome

/** Chromium holds a download while we decide; Firefox has no such event. */
export const isFirefox = !api.downloads?.onDeterminingFilename

/** Cookie reading needs host access; Firefox lets the user withhold it after install. */
export const SITE_ACCESS = { origins: ['http://*/*', 'https://*/*'] }

export async function loadSettings(): Promise<Settings> {
  return normalizeSettings(await api.storage.local.get(DEFAULT_SETTINGS))
}

export function saveSettings(patch: Partial<Settings>): Promise<void> {
  return api.storage.local.set(patch)
}

// ---- The app ----------------------------------------------------------------------------------

export type AppState = 'connected' | 'not-running' | 'outdated'

export interface AppStatus {
  state: AppState
  port?: number
  version?: string
}

const headers = { [CLIENT_HEADER]: CLIENT_VALUE }
const jsonHeaders = { ...headers, 'Content-Type': 'application/json' }

async function ping(port: number): Promise<AppStatus> {
  try {
    const response = await fetch(`http://127.0.0.1:${port}/ping`, {
      headers,
      signal: AbortSignal.timeout(PING_TIMEOUT_MS)
    })
    const body = (await response.json()) as { app?: unknown; version?: unknown; api?: unknown }
    // Something other than SonicNet on our port is the same as SonicNet not running.
    if (!response.ok || body.app !== 'sonicnet') return { state: 'not-running' }
    const version = typeof body.version === 'string' ? body.version : undefined
    return { state: body.api === API_VERSION ? 'connected' : 'outdated', port, version }
  } catch {
    return { state: 'not-running' }
  }
}

/** Asks every port at once; the first (in port order) that answers as SonicNet wins. */
export async function findApp(): Promise<AppStatus> {
  const answers = await Promise.all(PORTS.map(ping))
  return (
    answers.find((a) => a.state === 'connected') ??
    answers.find((a) => a.state === 'outdated') ?? { state: 'not-running' }
  )
}

export type AddResult =
  | { kind: 'taken'; port: number; token: string }
  /** SonicNet said no (or took too long): the browser keeps the download. */
  | { kind: 'kept'; reason: string }
  | { kind: 'unreachable' }

export async function addDownload(request: AddRequest): Promise<AddResult> {
  // Pinged right before every add, so cookies only ever go to a port that has just answered as
  // SonicNet, never to whatever took the port after the app quit.
  const app = await findApp()
  if (app.state !== 'connected' || app.port === undefined) return { kind: 'unreachable' }
  const { port } = app
  let response: Response
  try {
    response = await fetch(`http://127.0.0.1:${port}/add`, {
      method: 'POST',
      headers: jsonHeaders,
      body: JSON.stringify(request),
      signal: AbortSignal.timeout(ADD_TIMEOUT_MS)
    })
  } catch (error) {
    // Timed out: the app has given up too (its deadline is shorter), so the browser keeps it.
    const timedOut = error instanceof DOMException && error.name === 'TimeoutError'
    return timedOut ? { kind: 'kept', reason: 'SonicNet took too long to answer.' } : { kind: 'unreachable' }
  }
  const body = (await response.json().catch(() => ({}))) as { token?: unknown; reason?: unknown }
  if (response.status === 200 && typeof body.token === 'string' && body.token) {
    return { kind: 'taken', port, token: body.token }
  }
  // 422 carries a reason for the user; anything else (400 for a field the app refused) is ours.
  const reason = typeof body.reason === 'string' ? body.reason : `SonicNet refused it (HTTP ${response.status}).`
  return { kind: 'kept', reason }
}

/** True once SonicNet has the download in its list. */
export async function confirmDownload(port: number, token: string): Promise<boolean> {
  try {
    const response = await fetch(`http://127.0.0.1:${port}/confirm`, {
      method: 'POST',
      headers: jsonHeaders,
      body: JSON.stringify({ token }),
      signal: AbortSignal.timeout(CONFIRM_TIMEOUT_MS)
    })
    return response.ok
  } catch {
    return false
  }
}

// ---- What the app needs to download as the browser would --------------------------------------

async function cookiesFor(url: string, storeId: string | undefined): Promise<BrowserCookie[]> {
  try {
    return await api.cookies.getAll({ url, storeId })
  } catch {
    // Firefox with first-party isolation refuses unless firstPartyDomain is given; null reads every
    // site's copy and selectCookies keeps only the right site's. Chromium refuses the key, which is
    // why it's only the second try.
    try {
      const details = { url, storeId, firstPartyDomain: null } as chrome.cookies.GetAllDetails
      return (await api.cookies.getAll(details)) as BrowserCookie[]
    } catch {
      return []
    }
  }
}

/** Cookies for where the download starts and where it ends up (a redirect to a CDN), from the
 * store the download belongs to (incognito, Firefox container). The app sends each request only
 * the ones that match it. */
export async function readCookies(
  urls: readonly (string | undefined)[],
  storeId: string | undefined,
  pageUrl: string | undefined
): Promise<WireCookie[]> {
  const targets = [...new Set(urls.filter(isHttp))]
  const lists = await Promise.all(targets.map((url) => cookiesFor(url, storeId)))
  const sites = [...targets, pageUrl].flatMap((u) => hostOf(u) ?? [])
  return selectCookies(lists, sites)
}

export function thisBrowser(): { userAgent: string; browserName: string } {
  const userAgent = navigator.userAgent
  return { userAgent, browserName: browserName(userAgent, 'brave' in navigator) }
}
