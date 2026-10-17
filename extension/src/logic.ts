// Everything here is pure: no browser API, no network. The background script, the popup and the
// options page share it, and the tests run this file directly (Node strips the types), so the
// rules that decide where a download goes are the ones that are tested.

// ---- The protocol (crates/sonic-bridge/src/lib.rs) -------------------------------------------

/** The app listens on the first free one; we ask all three, so a busy port needs no setting. */
export const PORTS = [19320, 19321, 19322] as const
/** Bumped by the app when an older extension can no longer follow `/ping` or `/add`. */
export const API_VERSION = 1
/** Every request carries it: the custom header forces a CORS preflight, which only an
 * extension's origin passes, so web pages can't talk to the app. */
export const CLIENT_HEADER = 'X-SonicNet-Client'
export const CLIENT_VALUE = 'extension'
/** The app answers `/add` within its 8 s probe deadline. Waiting a bit longer guarantees the app
 * gave up first, so it never holds a token for a download the browser kept. */
export const ADD_TIMEOUT_MS = 10_000
/** A token lives 15 s in the app; confirming is quick, so this only bounds a hung socket. */
export const CONFIRM_TIMEOUT_MS = 4_000
export const PING_TIMEOUT_MS = 1_500
export const LAUNCH_URL = 'sonicnet://open'

// The app rejects a whole `/add` with 400 if any field breaks these, and the browser then keeps the
// download. So we check them here first and drop just the offending field (or cookie) instead.
// Rust measures `str::len` in UTF-8 bytes, so we do too.
const MAX_URL = 8192
const MAX_TEXT = 8192
const MAX_COOKIES = 500
const MAX_BROWSER_NAME = 40
const MAX_FILE_NAME = 255

export interface WireCookie {
  name: string
  value: string
  domain: string
  hostOnly: boolean
  path: string
  secure: boolean
}

export interface AddRequest {
  url: string
  browser?: {
    cookies: WireCookie[]
    referer?: string
    userAgent?: string
    name?: string
  }
  pageUrl?: string
  minBytes?: number
  fileName?: string
}

const utf8 = new TextEncoder()

/** Bounded and free of control characters: the app copies these into request headers, where a
 * line break would start a header of its own. Matches Rust's `char::is_control` (Unicode Cc). */
export function isText(text: string, maxBytes: number): boolean {
  return utf8.encode(text).length <= maxBytes && !/[\u0000-\u001f\u007f-\u009f]/.test(text)
}

export function isHttp(url: string | undefined): url is string {
  return url !== undefined && /^https?:\/\//i.test(url)
}

// ---- Settings ---------------------------------------------------------------------------------

// A type alias, not an interface: storage.local.get wants an index-signature-compatible object.
export type Settings = {
  /** The global on/off switch. */
  enabled: boolean
  /** Site rules (bare hostnames); each also covers its subdomains. */
  excludedSites: string[]
  /** Files known to be smaller stay in the browser. 0 = no minimum. */
  minSizeMb: number
}

export const DEFAULT_SETTINGS: Settings = {
  enabled: true,
  excludedSites: [],
  // Splitting a tiny file across connections gains nothing, and the browser's Save dialog is held
  // while SonicNet looks at it.
  minSizeMb: 1
}

export const MAX_MIN_SIZE_MB = 100_000

/** Whatever storage holds (older versions, hand edits) becomes valid settings. */
export function normalizeSettings(raw: Record<string, unknown>): Settings {
  const sites = Array.isArray(raw.excludedSites) ? raw.excludedSites : []
  return {
    enabled: typeof raw.enabled === 'boolean' ? raw.enabled : DEFAULT_SETTINGS.enabled,
    excludedSites: [
      ...new Set(sites.flatMap((s) => (typeof s === 'string' ? (siteRule(s) ?? []) : [])))
    ],
    minSizeMb: parseMinSize(raw.minSizeMb) ?? DEFAULT_SETTINGS.minSizeMb
  }
}

/** A minimum size typed by the user, in MB, or null if it isn't one. */
export function parseMinSize(input: unknown): number | null {
  const n = typeof input === 'string' && input.trim() !== '' ? Number(input) : input
  if (typeof n !== 'number' || !Number.isFinite(n) || n < 0 || n > MAX_MIN_SIZE_MB) return null
  return n
}

// ---- Site exclusions --------------------------------------------------------------------------

/** The hostname of a URL, lowercased and without a trailing dot ("example.com." is the same site). */
export function hostOf(url: string | undefined): string | null {
  if (!url) return null
  try {
    const host = new URL(url).hostname.toLowerCase().replace(/\.$/, '')
    return host || null
  } catch {
    return null
  }
}

/** The rule for whatever the user typed or pasted ("https://Drive.Example.com/x", "*.example.com",
 * "example.com"), or null when it names no site. */
export function siteRule(input: string): string | null {
  const text = input.trim().toLowerCase().replace(/^\*\./, '')
  if (!text) return null
  const host = hostOf(/^[a-z][a-z0-9+.-]*:\/\//.test(text) ? text : `https://${text}`)
  // A single label ("intranet") is almost always a typo for a domain; localhost and IPv6 aren't.
  if (!host || !(host.includes('.') || host === 'localhost' || host.startsWith('['))) return null
  return host
}

/** The rule that covers `host` (the host itself or a parent domain), if any. Label-aligned, so
 * "example.com" covers "dl.example.com" but not "notexample.com". */
export function coveringRule(rules: readonly string[], host: string): string | undefined {
  return rules.find((rule) => host === rule || host.endsWith(`.${rule}`))
}

/** True when any of the URLs (download, redirect target, referring page) is on an excluded site:
 * a file served from a CDN still counts as the site the user excluded. */
export function isExcluded(rules: readonly string[], urls: readonly (string | undefined)[]): boolean {
  return urls.some((url) => {
    const host = hostOf(url)
    return host !== null && coveringRule(rules, host) !== undefined
  })
}

export function withExclusion(rules: readonly string[], rule: string): string[] {
  return rules.includes(rule) ? [...rules] : [...rules, rule]
}

/** Captures `host` again: drops every rule that covers it, parent domains included, since
 * otherwise the switch would flip back on and nothing would change. */
export function withoutExclusion(rules: readonly string[], host: string): string[] {
  return rules.filter((rule) => !(host === rule || host.endsWith(`.${rule}`)))
}

// ---- Size threshold ---------------------------------------------------------------------------

export function minBytes(settings: Pick<Settings, 'minSizeMb'>): number | undefined {
  return settings.minSizeMb > 0 ? Math.round(settings.minSizeMb * 1024 * 1024) : undefined
}

/** The size the browser already knows. Both browsers report 0 or -1 until the server sends a
 * length, which means "unknown", not "empty". */
export function knownSize(item: { totalBytes?: number; fileSize?: number }): number | undefined {
  for (const size of [item.totalBytes, item.fileSize]) {
    if (size !== undefined && size > 0) return size
  }
  return undefined
}

/** Known to be under the minimum: the browser keeps it without asking SonicNet. An unknown size
 * isn't small; SonicNet gets `minBytes` and decides once it has probed the link. */
export function isTooSmall(size: number | undefined, min: number | undefined): boolean {
  return size !== undefined && min !== undefined && size < min
}

// ---- Cookies ----------------------------------------------------------------------------------

/** A cookie as the browser API returns it; Firefox adds `firstPartyDomain` under first-party
 * isolation (empty when the cookie isn't isolated). */
export interface BrowserCookie extends WireCookie {
  firstPartyDomain?: string
}

function isSendable(c: WireCookie): boolean {
  // `;` separates cookies in the Cookie header the app builds.
  const part = (text: string): boolean => isText(text, MAX_TEXT) && !text.includes(';')
  return (
    part(c.name) &&
    part(c.value) &&
    isText(c.domain, MAX_TEXT) &&
    isText(c.path, MAX_TEXT) &&
    c.path.startsWith('/')
  )
}

/**
 * Merges the cookies read for each URL (the download and where it redirected) into what the app
 * needs:
 * - one per domain/path/name, which is how the browser itself keys them;
 * - with first-party isolation, only copies kept for one of `sites` (the download's or its page's),
 *   never another site's;
 * - none the app would refuse (it rejects the whole request for one bad cookie), capped at its
 *   limit.
 */
export function selectCookies(
  lists: readonly (readonly BrowserCookie[])[],
  sites: readonly string[]
): WireCookie[] {
  const byKey = new Map<string, WireCookie>()
  for (const list of lists) {
    for (const cookie of list) {
      const party = cookie.firstPartyDomain
      if (party && !sites.some((host) => host === party || host.endsWith(`.${party}`))) continue
      const { name, value, domain, hostOnly, path, secure } = cookie
      const wire = { name, value, domain, hostOnly, path, secure }
      if (isSendable(wire)) byKey.set(`${domain}\n${path}\n${name}`, wire)
    }
  }
  return [...byKey.values()].slice(0, MAX_COOKIES)
}

/** Chromium's incognito store is "1"; Firefox names its containers and private store itself. */
export function cookieStoreFor(source: {
  incognito?: boolean
  cookieStoreId?: string
}): string | undefined {
  return source.cookieStoreId ?? (source.incognito ? '1' : undefined)
}

// ---- The /add request -------------------------------------------------------------------------

/** The name part of what the browser picked (Chromium and Firefox may hand over a full path). */
export function fileNameOf(path: string | undefined): string | undefined {
  const name = path?.split(/[\\/]/).pop()
  return name && name !== '.' && name !== '..' && isText(name, MAX_FILE_NAME) ? name : undefined
}

export function browserName(userAgent: string, isBrave = false): string {
  if (/Firefox\//.test(userAgent)) return 'Firefox'
  if (/Edg\//.test(userAgent)) return 'Edge'
  if (/OPR\//.test(userAgent)) return 'Opera'
  if (/Vivaldi\//.test(userAgent)) return 'Vivaldi'
  // Brave hides itself in the user agent; it only shows as `navigator.brave`.
  if (isBrave) return 'Brave'
  return 'Chrome'
}

export interface DownloadFacts {
  url: string
  /** The page the link was on. */
  pageUrl?: string
  referrer?: string
  /** The browser's chosen name or path (it alone saw the link's `download` attribute). */
  fileName?: string
  cookies?: WireCookie[]
  userAgent?: string
  browserName?: string
  minBytes?: number
}

/** The body for `POST /add`, or null when the app couldn't take this link at all. Optional fields
 * that wouldn't pass the app's checks are left out rather than sinking the request. */
export function buildAddRequest(facts: DownloadFacts): AddRequest | null {
  const { url } = facts
  if (!isHttp(url) || !isText(url, MAX_URL)) return null
  const httpText = (u: string | undefined): u is string => isHttp(u) && isText(u, MAX_URL)
  const fileName = fileNameOf(facts.fileName)
  const request: AddRequest = { url }
  if (facts.cookies) {
    const { referrer, userAgent, browserName: name } = facts
    request.browser = {
      cookies: facts.cookies.filter(isSendable).slice(0, MAX_COOKIES),
      ...(httpText(referrer) && { referer: referrer }),
      ...(userAgent && isText(userAgent, MAX_TEXT) && { userAgent }),
      ...(name && isText(name, MAX_BROWSER_NAME) && { name })
    }
  }
  if (httpText(facts.pageUrl)) request.pageUrl = facts.pageUrl
  if (facts.minBytes !== undefined && facts.minBytes > 0) request.minBytes = Math.round(facts.minBytes)
  if (fileName) request.fileName = fileName
  return request
}
