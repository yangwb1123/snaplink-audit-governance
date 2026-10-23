import { describe, expect, it } from 'vitest'
import type { AdminAction } from '../api/types'
import { ADMIN_ACTION_LIMIT, filterAdminActions } from './adminActions'

const items: AdminAction[] = [
  { id: 'z', tenant_id: 'tenant-a', action: 'Source.Updated', actor: 'Alice', target_type: 'source', target_id: 'Vault-A', created_at: '2026-01-01' },
  { id: 'a', tenant_id: 'tenant-a', action: 'source.updated', actor: 'Bob', target_type: 'schema', target_id: 'Vault-B', created_at: '2026-10-01' },
  { id: 'm', tenant_id: 'tenant-a', action: 'export.created', actor: 'Alice', created_at: '' },
]

describe('recent admin-action local filters', () => {
  it('uses the fixed recent-window limit and preserves server order without mutating records', () => {
    expect(ADMIN_ACTION_LIMIT).toBe(100)
    const result = filterAdminActions(items, { action: '', actor: '', target: '' })
    expect(result.map(item => item.id)).toEqual(['z', 'a', 'm'])
    expect(result[0]).toBe(items[0])
    expect(items.map(item => item.id)).toEqual(['z', 'a', 'm'])
  })

  it('trims case-insensitive literal text and combines action, actor and target with AND', () => {
    expect(filterAdminActions(items, { action: ' UPDATED ', actor: ' ALI ', target: ' vault-a ' })).toEqual([items[0]])
    expect(filterAdminActions(items, { action: '', actor: '', target: 'SCHEMA' })).toEqual([items[1]])
    expect(filterAdminActions(items, { action: '', actor: '', target: 'vault' })).toEqual(items.slice(0, 2))
    expect(filterAdminActions(items, { action: 'export', actor: 'Bob', target: '' })).toEqual([])
    expect(filterAdminActions(items, { action: '', actor: '', target: 'undefined' })).toEqual([])
  })

  it('does not interpret patterns, markup or URL-looking filter text', () => {
    for (const value of ['.*', '[', '<script>', 'javascript:', '?tenant_id=other']) {
      expect(filterAdminActions(items, { action: value, actor: '', target: '' })).toEqual([])
    }
  })
})
