import { coveringRule, hostOf, isHttp, withExclusion, withoutExclusion } from './logic.ts'
import { SITE_ACCESS, api, findApp, loadSettings, saveSettings } from './platform.ts'

const $ = <T extends HTMLElement = HTMLElement>(id: string): T => document.getElementById(id) as T

/** The site in the active tab; the host permission lets us read its address without `tabs`. */
async function activeHost(): Promise<string | null> {
  const [tab] = await api.tabs.query({ active: true, currentWindow: true })
  return isHttp(tab?.url) ? hostOf(tab.url) : null
}

async function render(): Promise<void> {
  const [settings, host, app, access] = await Promise.all([
    loadSettings(),
    activeHost(),
    findApp(),
    api.permissions.contains(SITE_ACCESS)
  ])
  const [tone, text] =
    app.state === 'not-running'
      ? ['', 'Not running']
      : app.state === 'outdated'
        ? ['warn', 'Update needed']
        : settings.enabled
          ? ['ok', app.version ? `Connected · ${app.version}` : 'Connected']
          : ['', 'Capture off']
  $('status').className = `pill ${tone}`
  $('status').textContent = text
  $('not-running').hidden = app.state !== 'not-running'
  $('outdated').hidden = app.state !== 'outdated'
  $('access').hidden = access

  $<HTMLInputElement>('enabled').checked = settings.enabled
  $('enabled-hint').textContent = settings.enabled
    ? 'Downloads go to SonicNet while it’s running.'
    : 'Every download stays in the browser.'

  $('site-row').hidden = host === null
  if (host) {
    const rule = coveringRule(settings.excludedSites, host)
    const site = $<HTMLInputElement>('site')
    site.checked = settings.enabled && rule === undefined
    site.disabled = !settings.enabled
    $('site-host').textContent = host
    $('site-hint').textContent = !settings.enabled
      ? 'Capture is off everywhere.'
      : rule === undefined
        ? 'Downloads from this site go to SonicNet.'
        : rule === host
          ? 'Excluded: downloads stay in the browser.'
          : `Excluded with ${rule}: downloads stay in the browser.`
  }
}

$<HTMLInputElement>('enabled').addEventListener('change', (event) => {
  void saveSettings({ enabled: (event.target as HTMLInputElement).checked })
})

$<HTMLInputElement>('site').addEventListener('change', async (event) => {
  const host = await activeHost()
  if (!host) return
  const { excludedSites } = await loadSettings()
  const on = (event.target as HTMLInputElement).checked
  await saveSettings({ excludedSites: on ? withoutExclusion(excludedSites, host) : withExclusion(excludedSites, host) })
})

$('grant').addEventListener('click', () => void api.permissions.request(SITE_ACCESS).then(render))
// The background does the launching: the popup closes as soon as the launch tab opens.
$('launch').addEventListener('click', () => void api.runtime.sendMessage({ type: 'launch' }).catch(() => {}))
$('options').addEventListener('click', () => void api.runtime.openOptionsPage())

api.storage.onChanged.addListener(() => void render())
void render()
// So the toolbar icon agrees with what the popup shows now, not at the next event.
void api.runtime.sendMessage({ type: 'refresh' }).catch(() => {})
