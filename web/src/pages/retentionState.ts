import { ApiError } from '../api/client'
import type { RetentionPolicy } from '../api/types'

export type RetentionState =
  | 'unscoped'
  | 'loading'
  | 'configured'
  | 'unconfigured'
  | 'error'
  | 'creating'

export function defaultRetentionPolicy(tenantId: string): RetentionPolicy {
  return {
    tenant_id: tenantId,
    hot_days: 30,
    warm_days: 90,
    archive_days: 365,
    retention_class: 'default',
  }
}

export function retentionReadFailureState(error: unknown): 'unconfigured' | 'error' {
  return error instanceof ApiError && error.status === 404 && error.code === 'not_found'
    ? 'unconfigured'
    : 'error'
}

export function canSaveRetention(state: RetentionState, writable: boolean, busy: boolean): boolean {
  return writable && !busy && (state === 'configured' || state === 'creating')
}

export function canEvaluateRetention(state: RetentionState, busy: boolean): boolean {
  return state === 'configured' && !busy
}
