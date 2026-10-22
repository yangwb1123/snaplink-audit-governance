import { navigationHash, parseNavigationHash, type EvidenceTab } from './navigation'

export type { EvidenceTab } from './navigation'
export { safeNavigationHash } from './navigation'

export function evidenceLocation(hash: string): { tab?: EvidenceTab; id: string } {
  const target = parseNavigationHash(hash)
  return target && 'tab' in target ? { tab: target.tab, id: target.id ?? '' } : { id: '' }
}

export function evidenceHash(tab: EvidenceTab, id = ''): string {
  return navigationHash({ page: 'evidence', tab, id })
}

export function navigateEvidence(tab: EvidenceTab, id = ''): void {
  window.location.hash = evidenceHash(tab, id)
}
