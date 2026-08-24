import { describe, expect, it } from 'vitest'
import type { RestoreRun } from '../api/types'
import { restoreDecisionBlockReason, shouldPollExport } from './governanceState'

const pending: RestoreRun = {
  id: 'restore-1',
  tenant_id: 'demo',
  operation_id: 'op-1',
  status: 'pending_approval',
  reason: 'rollback',
  created_by: 'requester-1',
  created_at: '2026-08-22T00:00:00Z',
}

describe('governance workflow state', () => {
  it('polls only non-terminal exports', () => {
    expect(shouldPollExport('pending')).toBe(true)
    expect(shouldPollExport('running')).toBe(true)
    expect(shouldPollExport('completed')).toBe(false)
    expect(shouldPollExport('failed')).toBe(false)
  })

  it('enforces restore separation of duties before calling the API', () => {
    expect(restoreDecisionBlockReason(pending, 'requester-1', true)).toContain('不同')
    expect(restoreDecisionBlockReason(pending, 'approver-1', false)).toContain('权限')
    expect(restoreDecisionBlockReason(pending, 'approver-1', true)).toBeNull()
    expect(
      restoreDecisionBlockReason({ ...pending, status: 'approved' }, 'approver-1', true),
    ).toContain('完成')
  })
})
