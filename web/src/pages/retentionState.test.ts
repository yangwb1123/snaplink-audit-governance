import { describe, expect, it } from 'vitest'
import { ApiError } from '../api/client'
import {
  canEvaluateRetention,
  canSaveRetention,
  defaultRetentionPolicy,
  retentionReadFailureState,
} from './retentionState'

describe('retention policy editor state', () => {
  it('distinguishes an explicitly missing policy from other read failures', () => {
    expect(retentionReadFailureState(new ApiError(404, 'not_found', 'not found'))).toBe(
      'unconfigured',
    )
    expect(retentionReadFailureState(new ApiError(404, 'http_404', 'not found'))).toBe('error')
    expect(retentionReadFailureState(new ApiError(403, 'forbidden', 'forbidden'))).toBe('error')
    expect(retentionReadFailureState(new ApiError(500, 'internal', 'failed'))).toBe('error')
    expect(retentionReadFailureState(new TypeError('network unavailable'))).toBe('error')
  })

  it('creates defaults only as an explicit editor action for the selected tenant', () => {
    expect(defaultRetentionPolicy('tenant-a')).toEqual({
      tenant_id: 'tenant-a',
      hot_days: 30,
      warm_days: 90,
      archive_days: 365,
      retention_class: 'default',
    })
  })

  it('allows saves only for configured or explicitly-created policies', () => {
    expect(canSaveRetention('loading', true, false)).toBe(false)
    expect(canSaveRetention('error', true, false)).toBe(false)
    expect(canSaveRetention('unconfigured', true, false)).toBe(false)
    expect(canSaveRetention('creating', true, false)).toBe(true)
    expect(canSaveRetention('configured', true, false)).toBe(true)
    expect(canSaveRetention('configured', false, false)).toBe(false)
    expect(canSaveRetention('configured', true, true)).toBe(false)
  })

  it('allows evaluation only for a configured policy', () => {
    expect(canEvaluateRetention('configured', false)).toBe(true)
    expect(canEvaluateRetention('creating', false)).toBe(false)
    expect(canEvaluateRetention('unconfigured', false)).toBe(false)
    expect(canEvaluateRetention('configured', true)).toBe(false)
  })
})
