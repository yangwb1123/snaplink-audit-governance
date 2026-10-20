import { investigationHash, investigationLocation } from './investigationNavigation'

export type EvidenceTab = 'exports' | 'restores' | 'holds' | 'integrity'

const tabs = new Set<EvidenceTab>(['exports', 'restores', 'holds', 'integrity'])

export function evidenceLocation(hash: string): { tab?: EvidenceTab; id: string } {
  const [page, tab, encodedId = ''] = hash.replace(/^#\/?/, '').split('/')
  if (page !== 'evidence' || !tabs.has(tab as EvidenceTab)) return { id: '' }
  try {
    const id = decodeURIComponent(encodedId)
    if (id.length > 8192 || /[\s/\\]/.test(id) || [...id].some((char) => char.charCodeAt(0) < 32 || char.charCodeAt(0) === 127)) return { tab: tab as EvidenceTab, id: '' }
    return { tab: tab as EvidenceTab, id }
  } catch {
    return { tab: tab as EvidenceTab, id: '' }
  }
}

/** Only route names and server-shaped job identifiers may cross the login
 * redirect in one-time PKCE metadata. Never persist a complete URL/query. */
export function safeNavigationHash(raw = ''): string {
  const root = raw.replace(/^#\/?/, '')
  if (['overview', 'events', 'operations', 'evidence', 'catalog'].includes(root)) return `#${root}`
  const investigation = investigationLocation(raw)
  if (investigation) return investigationHash(investigation)
  const location = evidenceLocation(raw)
  if (!location.tab || /[?&=]/.test(root) || root.split('/').length > 3) return '#overview'
  if (!location.id) return `#evidence/${location.tab}`
  if ((location.tab === 'exports' && /^export-[0-9a-f]{16,64}$/.test(location.id)) || (location.tab === 'restores' && /^restore-[0-9a-f]{16,64}$/.test(location.id))) {
    return `#evidence/${location.tab}/${location.id}`
  }
  return `#evidence/${location.tab}`
}

export function navigateEvidence(tab: EvidenceTab, id = ''): void {
  window.location.hash = `#evidence/${tab}${id ? `/${encodeURIComponent(id)}` : ''}`
}
