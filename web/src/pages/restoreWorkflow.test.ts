import { describe, expect, it } from 'vitest'
import type { RestorePreview, RestoreRequest } from '../api/types'
import {
  bindRestorePreview,
  restorePreviewBlockReason,
  restoreRequestKey,
} from './restoreWorkflow'

const request: RestoreRequest = { operation_id: ' operation-1 ', reason: ' rollback ' }
const preview: RestorePreview = {
  id: 'preview-1',
  tenant_id: 'demo',
  operation_id: 'operation-1',
  proposed_state: { status: 'active' },
  external_calls: [],
  requires_approval: true,
}

describe('restore preview workflow', () => {
  it('normalizes and binds the complete restore request', () => {
    expect(restoreRequestKey(request)).toBe(restoreRequestKey({
      operation_id: 'operation-1',
      reason: 'rollback',
    }))
    expect(restorePreviewBlockReason(bindRestorePreview(preview, request), request, 'demo')).toBeNull()
  })

  it('requires a preview and invalidates it when either request field changes', () => {
    const bound = bindRestorePreview(preview, request)
    expect(restorePreviewBlockReason(null, request, 'demo')).toContain('必须先完成')
    expect(
      restorePreviewBlockReason(bound, { ...request, operation_id: 'operation-2' }, 'demo'),
    ).toContain('参数已改变')
    expect(
      restorePreviewBlockReason(bound, { ...request, reason: 'different reason' }, 'demo'),
    ).toContain('参数已改变')
  })

  it('rejects a cross-tenant or non-approval preview', () => {
    expect(
      restorePreviewBlockReason(bindRestorePreview(preview, request), request, 'other-tenant'),
    ).toContain('其他租户')
    expect(
      restorePreviewBlockReason(
        bindRestorePreview({ ...preview, requires_approval: false }, request),
        request,
        'demo',
      ),
    ).toContain('未声明审批')
  })
})
