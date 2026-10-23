// One allowlist for builders, live routes and one-time login return metadata.
// Identifiers only: never tenant selectors, URLs, queries, filters or tokens.
export type Page = 'overview' | 'events' | 'operations' | 'evidence' | 'catalog' | 'admin-actions'
export type EvidenceTab = 'exports' | 'restores' | 'holds' | 'integrity'
export type NavigationTarget =
  | { page: Page }
  | { page: 'events'; eventId: string }
  | { page: 'operations'; operationId: string }
  | { page: 'operations'; aggregateType: string; aggregateId: string }
  | { page: 'evidence'; tab: EvidenceTab; id?: string }

const pages = new Set<Page>(['overview', 'events', 'operations', 'evidence', 'catalog', 'admin-actions'])
const tabs = new Set<EvidenceTab>(['exports', 'restores', 'holds', 'integrity'])
const MAX_HASH = 4096
const MAX_ID = 256
const MAX_TYPE = 64
const jobPatterns = { exports: /^export-[0-9a-f]{16,64}$/, restores: /^restore-[0-9a-f]{16,64}$/ }

function validValue(value: string, max = MAX_ID): boolean {
  // This restricted alphabet also rejects malformed UTF-16, control/format
  // characters, double encodings, path delimiters and URL/query injection.
  return value.length > 0 && value.length <= max && /^[\p{L}\p{N}._:@+-]+$/u.test(value)
    && !/^(?:https?|javascript|data|vbscript|file|ftp|mailto):/i.test(value)
    && value !== '.' && value !== '..'
}

function segment(raw: string, max = MAX_ID): string {
  try {
    const value = decodeURIComponent(raw)
    return validValue(value, max) ? value : ''
  } catch { return '' }
}

function validJob(tab: EvidenceTab, id: string): boolean {
  return (tab === 'exports' || tab === 'restores') && jobPatterns[tab].test(id)
}

export function parseNavigationHash(raw: string): NavigationTarget | null {
  if (!raw || raw.length > MAX_HASH) return null
  const parts = raw.replace(/^#\/?/, '').split('/')
  if (parts.length === 1 && pages.has(parts[0] as Page)) return { page: parts[0] as Page }
  const [page, mode, encodedId = '', encodedAggregateId = ''] = parts
  if (page === 'events' && mode === 'event' && parts.length === 3) {
    const eventId = segment(encodedId)
    return eventId ? { page, eventId } : null
  }
  if (page === 'operations' && mode === 'op' && parts.length === 3) {
    const operationId = segment(encodedId)
    return operationId ? { page, operationId } : null
  }
  if (page === 'operations' && mode === 'aggregate' && parts.length === 4) {
    const aggregateType = segment(encodedId, MAX_TYPE)
    const aggregateId = segment(encodedAggregateId)
    return aggregateType && /^[A-Za-z0-9._:-]+$/.test(aggregateType) && aggregateId
      ? { page, aggregateType, aggregateId } : null
  }
  if (page === 'evidence' && tabs.has(mode as EvidenceTab)) {
    const tab = mode as EvidenceTab
    if (parts.length === 2) return { page, tab }
    const id = segment(encodedId)
    if (parts.length === 3 && id && validJob(tab, id)) return { page, tab, id }
  }
  return null
}

/** Invalid input falls back without ever embedding it in a URL. */
export function navigationHash(target: NavigationTarget): string {
  if (!pages.has(target.page)) return '#overview'
  let hash: string = `#${target.page}`
  if ('eventId' in target) {
    if (!validValue(target.eventId)) return '#events'
    hash += `/event/${encodeURIComponent(target.eventId)}`
  } else if ('operationId' in target) {
    if (!validValue(target.operationId)) return '#operations'
    hash += `/op/${encodeURIComponent(target.operationId)}`
  } else if ('aggregateType' in target) {
    if (!validValue(target.aggregateType, MAX_TYPE) || !validValue(target.aggregateId)) return '#operations'
    hash += `/aggregate/${encodeURIComponent(target.aggregateType)}/${encodeURIComponent(target.aggregateId)}`
  } else if ('tab' in target) {
    if (!tabs.has(target.tab)) return '#evidence'
    hash += `/${target.tab}`
    if (target.id) {
      if (!validJob(target.tab, target.id)) return hash
      hash += `/${target.id}`
    }
  }
  return parseNavigationHash(hash) ? hash : `#${target.page}`
}

export function safeNavigationHash(raw = ''): string {
  const target = parseNavigationHash(raw)
  return target ? navigationHash(target) : '#overview'
}

export function eventLocation(hash: string): string {
  const target = parseNavigationHash(hash)
  return target && 'eventId' in target ? target.eventId : ''
}

export function eventHash(eventId: string): string {
  return navigationHash({ page: 'events', eventId })
}

export function navigateEvent(eventId: string): void {
  window.location.hash = eventHash(eventId)
}

export type QuickJumpKind = 'event' | 'operation' | 'export'
export const quickJumpPermissions: Record<QuickJumpKind, string> = {
  event: 'audit:event:read', operation: 'audit:operation:read', export: 'audit:export:create',
}

export function quickJumpHash(kind: QuickJumpKind, id: string): string | null {
  const target: NavigationTarget = kind === 'event' ? { page: 'events', eventId: id }
    : kind === 'operation' ? { page: 'operations', operationId: id }
      : { page: 'evidence', tab: 'exports', id }
  const hash = navigationHash(target)
  const parsed = parseNavigationHash(hash)
  return parsed && ('eventId' in parsed || 'operationId' in parsed || ('id' in parsed && parsed.id)) ? hash : null
}
