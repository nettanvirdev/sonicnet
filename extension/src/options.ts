import { parseMinSize, siteRule, withExclusion } from './logic.ts'
import { api, loadSettings, saveSettings } from './platform.ts'

const $ = <T extends HTMLElement = HTMLElement>(id: string): T => document.getElementById(id) as T
const enabled = $<HTMLInputElement>('enabled')
const minSize = $<HTMLInputElement>('min-size')
const addSite = $<HTMLInputElement>('add-site')
const addHint = $('add-hint')
const HINT = addHint.textContent ?? ''

async function render(): Promise<void> {
  const settings = await loadSettings()
  enabled.checked = settings.enabled
  // Don't overwrite what the user is typing.
  if (document.activeElement !== minSize) minSize.value = String(settings.minSizeMb)
  const list = $('sites')
  list.replaceChildren(
    ...settings.excludedSites.map((site) => {
      const item = document.createElement('li')
      const name = document.createElement('span')
      name.textContent = site
      const remove = document.createElement('button')
      remove.type = 'button'
      remove.className = 'ghost'
      remove.textContent = 'Remove'
      remove.setAttribute('aria-label', `Stop excluding ${site}`)
      remove.addEventListener('click', async () => {
        const { excludedSites } = await loadSettings()
        await saveSettings({ excludedSites: excludedSites.filter((s) => s !== site) })
      })
      item.append(name, remove)
      return item
    })
  )
  $('empty').hidden = settings.excludedSites.length > 0
}

enabled.addEventListener('change', () => void saveSettings({ enabled: enabled.checked }))

minSize.addEventListener('change', () => {
  const mb = parseMinSize(minSize.value)
  minSize.setAttribute('aria-invalid', String(mb === null))
  if (mb !== null) void saveSettings({ minSizeMb: mb })
})

$<HTMLFormElement>('add-form').addEventListener('submit', async (event) => {
  event.preventDefault()
  const rule = siteRule(addSite.value)
  addHint.classList.toggle('error', rule === null)
  if (rule === null) {
    addHint.textContent = 'That isn’t a site. Try a domain like example.com or paste a link.'
    return
  }
  addHint.textContent = HINT
  addSite.value = ''
  const { excludedSites } = await loadSettings()
  await saveSettings({ excludedSites: withExclusion(excludedSites, rule) })
})

api.storage.onChanged.addListener(() => void render())
void render()
