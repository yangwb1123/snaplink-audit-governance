import { navigationHash, parseNavigationHash } from './navigation'

export type InvestigationTarget =
  | { mode: 'operation'; operationId: string }
  | { mode: 'aggregate'; aggregateType: string; aggregateId: string }

export function investigationLocation(hash: string): InvestigationTarget | null {
  const target = parseNavigationHash(hash)
  if (target && 'operationId' in target) return { mode: 'operation', operationId: target.operationId }
  if (target && 'aggregateType' in target) return { mode: 'aggregate', aggregateType: target.aggregateType, aggregateId: target.aggregateId }
  return null
}

export function investigationHash(target: InvestigationTarget): string {
  return target.mode === 'operation'
    ? navigationHash({ page: 'operations', operationId: target.operationId })
    : navigationHash({ page: 'operations', aggregateType: target.aggregateType, aggregateId: target.aggregateId })
}

export function navigateInvestigation(target: InvestigationTarget): void {
  window.location.hash = investigationHash(target)
}
