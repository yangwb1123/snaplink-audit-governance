import { describe, expect, it } from 'vitest'
import { evidenceLocation, safeNavigationHash } from './evidenceNavigation'

describe('safe evidence links', () => {
  it('recognizes subpages and identifiers without embedding tokens or payloads', () => {
    expect(evidenceLocation('#evidence/restores/restore-123')).toEqual({ tab: 'restores', id: 'restore-123' })
    expect(evidenceLocation('#/evidence/exports/export%3A123')).toEqual({ tab: 'exports', id: 'export:123' })
    expect(evidenceLocation('#evidence')).toEqual({ id: '' })
  })

  it('persists only safe navigation in one-time login metadata, never complete URLs, tokens or filters', () => {
    expect(safeNavigationHash('#evidence/restores/restore-0123456789abcdef')).toBe('#evidence/restores/restore-0123456789abcdef')
    expect(safeNavigationHash('#evidence/exports/export-0123456789abcdef?access_token=secret')).toBe('#overview')
    expect(safeNavigationHash('https://untrusted.example/')).toBe('#overview')
    expect(safeNavigationHash('#events?payload_field=email')).toBe('#overview')
    expect(safeNavigationHash('#evidence/exports/untrusted-identity')).toBe('#evidence/exports')
  })

  it('preserves only validated investigation links across login, never payloads or filters', () => {
    expect(safeNavigationHash('#operations/op/op-0123456789abcdef')).toBe('#operations/op/op-0123456789abcdef')
    expect(safeNavigationHash('#operations/aggregate/file/file-9')).toBe('#operations/aggregate/file/file-9')
    expect(safeNavigationHash('#operations/aggregate/file/file-9?payload_field=email')).toBe('#overview')
    expect(safeNavigationHash('#operations/op/bad%2Fid')).toBe('#overview')
    expect(safeNavigationHash('#operations/op/op-1#token')).toBe('#overview')
  })

  it('rejects malformed encodings, untrusted page names and identifier control characters', () => {
    expect(evidenceLocation('#evidence/exports/%')).toEqual({ tab: 'exports', id: '' })
    expect(evidenceLocation('#evidence/exports/x%00y').id).toBe('')
    expect(evidenceLocation('#evidence/exports/x%2Fy').id).toBe('')
    expect(evidenceLocation('#evidence/unregistered/secret')).toEqual({ id: '' })
  })
})
