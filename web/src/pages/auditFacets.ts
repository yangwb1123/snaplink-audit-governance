export interface FacetEntry {
  label: string
  count: number
}

const failedOutcomes = new Set(['failed', 'failure', 'error', 'denied'])

export function topFacetEntries(values: Record<string, number>, limit = 6): FacetEntry[] {
  return Object.entries(values)
    .filter(([label, count]) => label.trim() !== '' && Number.isFinite(count) && count > 0)
    .map(([label, count]) => ({ label, count }))
    .sort((left, right) => right.count - left.count || left.label.localeCompare(right.label))
    .slice(0, Math.max(0, limit))
}

export function failedFacetCount(outcomes: Record<string, number>): number {
  return Object.entries(outcomes).reduce(
    (total, [outcome, count]) =>
      failedOutcomes.has(outcome.toLowerCase()) && Number.isFinite(count) && count > 0
        ? total + count
        : total,
    0,
  )
}
