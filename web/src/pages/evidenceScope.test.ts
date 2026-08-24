import { describe, expect, it } from 'vitest'
import {
  buildEvidenceScopeQuery,
  defaultEvidenceRange,
  emptyEvidenceScope,
  evidenceScopeSummary,
} from './evidenceScope'

describe('evidence scope', () => {
  it('builds a trimmed cross-service governance query', () => {
    const query = buildEvidenceScopeQuery(
      {
        start: new Date('2026-08-01T00:00:00Z'),
        end: new Date('2026-08-23T00:00:00Z'),
      },
      {
        ...emptyEvidenceScope,
        sourceSystem: ' aero-vault.source ',
        eventType: ' aero.vault.security ',
        operationId: ' op-100 ',
        correlationId: ' flow-100 ',
      },
    )

    expect(query).toMatchObject({
      source_system: 'aero-vault.source',
      event_type: 'aero.vault.security',
      operation_id: 'op-100',
      correlation_id: 'flow-100',
      page_size: 1000,
    })
  })

  it('creates deterministic default windows without mutating the supplied time', () => {
    const now = new Date('2026-08-23T12:00:00Z')
    const range = defaultEvidenceRange(30, now)
    expect(range.start.toISOString()).toBe('2026-07-24T12:00:00.000Z')
    expect(range.end.toISOString()).toBe('2026-08-23T12:00:00.000Z')
    expect(now.toISOString()).toBe('2026-08-23T12:00:00.000Z')
  })

  it('summarizes the persisted legal-hold scope', () => {
    const query = buildEvidenceScopeQuery(
      {
        start: new Date('2026-08-01T00:00:00Z'),
        end: new Date('2026-08-23T00:00:00Z'),
      },
      { ...emptyEvidenceScope, actorId: 'user-1', outcome: 'failed' },
    )!
    expect(evidenceScopeSummary(query)).toContain('2026-08-01 — 2026-08-23')
    expect(evidenceScopeSummary(query)).toContain('主体 user-1')
    expect(evidenceScopeSummary(query)).toContain('结果 failed')
  })
})
