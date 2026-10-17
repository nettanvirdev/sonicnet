/// <reference types="node" />
import assert from 'node:assert/strict'
import { test } from 'node:test'
import {
  DEFAULT_SETTINGS,
  browserName,
  buildAddRequest,
  cookieStoreFor,
  coveringRule,
  fileNameOf,
  isExcluded,
  isText,
  isTooSmall,
  knownSize,
  minBytes,
  normalizeSettings,
  parseMinSize,
  selectCookies,
  siteRule,
  withExclusion,
  withoutExclusion,
  type BrowserCookie
} from '../src/logic.ts'

const cookie = (over: Partial<BrowserCookie> = {}): BrowserCookie => ({
  name: 'sid',
  value: 'abc',
  domain: '.example.com',
  hostOnly: false,
  path: '/',
  secure: true,
  ...over
})

test('siteRule turns what users type into a hostname', () => {
  assert.equal(siteRule('example.com'), 'example.com')
  assert.equal(siteRule('  *.Example.COM '), 'example.com')
  assert.equal(siteRule('https://Dl.Example.com:8443/file.zip?x=1'), 'dl.example.com')
  assert.equal(siteRule('example.com/path'), 'example.com')
  assert.equal(siteRule('example.com.'), 'example.com')
  assert.equal(siteRule('localhost'), 'localhost')
  assert.equal(siteRule('192.168.1.10'), '192.168.1.10')
  assert.equal(siteRule('intranet'), null)
  assert.equal(siteRule(''), null)
  assert.equal(siteRule('http://'), null)
})

test('exclusions cover subdomains on label boundaries only', () => {
  const rules = ['example.com']
  assert.ok(isExcluded(rules, ['https://example.com/a.zip']))
  assert.ok(isExcluded(rules, ['https://dl.cdn.example.com/a.zip']))
  assert.ok(isExcluded(rules, ['https://EXAMPLE.com./a.zip']))
  assert.ok(!isExcluded(rules, ['https://notexample.com/a.zip']))
  assert.ok(!isExcluded(rules, ['https://example.com.evil.net/a.zip']))
  assert.ok(!isExcluded(rules, [undefined, '', 'not a url']))
  // The referring page counts: a file on a CDN is still the excluded site's download.
  assert.ok(isExcluded(rules, ['https://cdn.net/a.zip', undefined, 'https://www.example.com/page']))
  // A subdomain rule doesn't cover its parent.
  assert.ok(!isExcluded(['dl.example.com'], ['https://example.com/a.zip']))
})

test('re-including a site drops parent rules that cover it, and nothing else', () => {
  const rules = ['example.com', 'dl.example.com', 'other.org']
  assert.equal(coveringRule(rules, 'a.dl.example.com'), 'example.com')
  assert.deepEqual(withoutExclusion(rules, 'dl.example.com'), ['other.org'])
  assert.deepEqual(withExclusion(['a.com'], 'a.com'), ['a.com'])
  assert.deepEqual(withExclusion(['a.com'], 'b.com'), ['a.com', 'b.com'])
})

test('size threshold: only a known size below the minimum keeps the download', () => {
  assert.equal(minBytes({ minSizeMb: 0 }), undefined)
  assert.equal(minBytes({ minSizeMb: 1 }), 1_048_576)
  assert.equal(minBytes({ minSizeMb: 0.5 }), 524_288)
  // Browsers report 0 or -1 for "no Content-Length yet".
  assert.equal(knownSize({ totalBytes: -1, fileSize: 0 }), undefined)
  assert.equal(knownSize({ totalBytes: 0, fileSize: 2048 }), 2048)
  assert.equal(knownSize({ totalBytes: 4096 }), 4096)
  assert.ok(isTooSmall(1000, 1_048_576))
  assert.ok(!isTooSmall(1_048_576, 1_048_576))
  assert.ok(!isTooSmall(undefined, 1_048_576))
  assert.ok(!isTooSmall(10, undefined))
})

test('settings from storage are made valid', () => {
  assert.deepEqual(normalizeSettings({}), DEFAULT_SETTINGS)
  assert.deepEqual(
    normalizeSettings({ enabled: 'yes', excludedSites: ['*.A.com', 'a.com', 7, 'nope'], minSizeMb: -3 }),
    { enabled: true, excludedSites: ['a.com'], minSizeMb: DEFAULT_SETTINGS.minSizeMb }
  )
  assert.equal(parseMinSize('2.5'), 2.5)
  assert.equal(parseMinSize('0'), 0)
  assert.equal(parseMinSize(''), null)
  assert.equal(parseMinSize('abc'), null)
  assert.equal(parseMinSize(Infinity), null)
})

test('cookies are deduplicated by domain, path and name; later lists win', () => {
  const picked = selectCookies(
    [
      [cookie({ value: 'old' }), cookie({ name: 'theme', value: 'dark' })],
      [cookie({ value: 'new' }), cookie({ path: '/dl', value: 'scoped' })]
    ],
    ['example.com']
  )
  assert.deepEqual(
    picked.map((c) => `${c.name}@${c.path}=${c.value}`),
    ['sid@/=new', 'theme@/=dark', 'sid@/dl=scoped']
  )
  // Only the wire fields go to the app.
  assert.deepEqual(Object.keys(picked[0]!).sort(), ['domain', 'hostOnly', 'name', 'path', 'secure', 'value'])
})

test('first-party isolation keeps only the copies for the download or page site', () => {
  const picked = selectCookies(
    [
      [
        cookie({ value: 'mine', firstPartyDomain: 'example.com' }),
        cookie({ domain: 'cdn.net', value: 'tracker', firstPartyDomain: 'ads.org' }),
        cookie({ name: 'plain', firstPartyDomain: '' })
      ]
    ],
    ['dl.example.com', 'cdn.net']
  )
  assert.deepEqual(picked.map((c) => c.value), ['mine', 'abc'])
})

test('cookies the app would reject are dropped, not sent', () => {
  const picked = selectCookies(
    [
      [
        cookie({ name: 'a;b' }),
        cookie({ name: 'v', value: 'x;y' }),
        cookie({ name: 'nl', value: 'x\ny' }),
        cookie({ name: 'p', path: 'relative' }),
        cookie({ name: 'big', value: 'x'.repeat(8193) }),
        cookie({ name: 'ok' })
      ]
    ],
    []
  )
  assert.deepEqual(picked.map((c) => c.name), ['ok'])
  const many = Array.from({ length: 600 }, (_, i) => cookie({ name: `c${i}` }))
  assert.equal(selectCookies([many], []).length, 500)
})

test('cookie store: Firefox container, Chromium incognito, or default', () => {
  assert.equal(cookieStoreFor({ incognito: false }), undefined)
  assert.equal(cookieStoreFor({ incognito: true }), '1')
  assert.equal(cookieStoreFor({ incognito: true, cookieStoreId: 'firefox-private' }), 'firefox-private')
  assert.equal(cookieStoreFor({ incognito: false, cookieStoreId: 'firefox-container-2' }), 'firefox-container-2')
})

test('limits are measured in UTF-8 bytes and refuse control characters', () => {
  assert.ok(isText('é'.repeat(4), 8))
  assert.ok(!isText('é'.repeat(5), 8))
  assert.ok(!isText('a\u0085b', 100)) // NEL is a C1 control
  assert.ok(isText('a\u00a0b', 100))
})

test('fileNameOf keeps the name, never a path', () => {
  assert.equal(fileNameOf('C:\\Users\\me\\Downloads\\setup.exe'), 'setup.exe')
  assert.equal(fileNameOf('/home/me/a.tar.gz'), 'a.tar.gz')
  assert.equal(fileNameOf('report.pdf'), 'report.pdf')
  assert.equal(fileNameOf('dir/'), undefined)
  assert.equal(fileNameOf('..'), undefined)
  assert.equal(fileNameOf(''), undefined)
  assert.equal(fileNameOf('x'.repeat(256)), undefined)
})

test('buildAddRequest builds the exact /add body', () => {
  const request = buildAddRequest({
    url: 'https://dl.example.com/a.zip',
    pageUrl: 'https://example.com/downloads',
    referrer: 'https://example.com/downloads',
    fileName: 'C:\\Downloads\\a.zip',
    cookies: [cookie()],
    userAgent: 'Mozilla/5.0 Edg/130',
    browserName: 'Edge',
    minBytes: 1_048_576
  })
  assert.deepEqual(request, {
    url: 'https://dl.example.com/a.zip',
    browser: {
      cookies: [{ name: 'sid', value: 'abc', domain: '.example.com', hostOnly: false, path: '/', secure: true }],
      referer: 'https://example.com/downloads',
      userAgent: 'Mozilla/5.0 Edg/130',
      name: 'Edge'
    },
    pageUrl: 'https://example.com/downloads',
    minBytes: 1_048_576,
    fileName: 'a.zip'
  })
})

test('buildAddRequest drops fields the app would refuse instead of failing', () => {
  assert.equal(buildAddRequest({ url: 'blob:https://example.com/123' }), null)
  assert.equal(buildAddRequest({ url: 'data:text/plain,hi' }), null)
  assert.equal(buildAddRequest({ url: `https://a.com/${'x'.repeat(8200)}` }), null)
  assert.deepEqual(
    buildAddRequest({
      url: 'HTTPS://a.com/f',
      pageUrl: 'about:blank',
      referrer: 'chrome://newtab',
      fileName: '..',
      cookies: [],
      userAgent: 'bad\r\nX-Injected: 1',
      browserName: 'x'.repeat(41),
      minBytes: 0
    }),
    { url: 'HTTPS://a.com/f', browser: { cookies: [] } }
  )
  // No cookies read (nothing to send): no browser object at all.
  assert.deepEqual(buildAddRequest({ url: 'http://a.com/f' }), { url: 'http://a.com/f' })
})

test('browserName recognises the Chromium family and Firefox', () => {
  const chrome = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0 Safari/537.36'
  assert.equal(browserName(chrome), 'Chrome')
  assert.equal(browserName(`${chrome} Edg/130.0`), 'Edge')
  assert.equal(browserName(`${chrome} OPR/115.0`), 'Opera')
  assert.equal(browserName(chrome, true), 'Brave')
  assert.equal(browserName('Mozilla/5.0 (Windows NT 10.0; rv:140.0) Gecko/20100101 Firefox/140.0'), 'Firefox')
})
