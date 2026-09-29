import { describe, expect, it } from 'vitest'
import { resolveTenantSelection } from './tenantScope'

const tenants = [{ id: 'tenant-b' }, { id: 'tenant-a' }]

describe('platform tenant selection', () => {
  it('preserves a current choice only while it remains available', () => {
    expect(resolveTenantSelection(tenants, 'tenant-a', 'tenant-b')).toBe('tenant-a')
    expect(resolveTenantSelection(tenants, 'removed-tenant', 'tenant-b')).toBe('tenant-b')
  })

  it('falls back to a preferred tenant, then to an available tenant', () => {
    expect(resolveTenantSelection(tenants, '', 'tenant-a')).toBe('tenant-a')
    expect(resolveTenantSelection(tenants, '', 'unknown')).toBe('tenant-b')
    expect(resolveTenantSelection([], '', 'tenant-a')).toBe('')
  })
})
