import type { RestorePreview, RestoreRequest } from '../api/types'

export interface BoundRestorePreview {
  preview: RestorePreview
  requestKey: string
}

export function restoreRequestKey(request: RestoreRequest): string {
  return JSON.stringify([request.operation_id.trim(), request.reason.trim()])
}

export function bindRestorePreview(
  preview: RestorePreview,
  request: RestoreRequest,
): BoundRestorePreview {
  return { preview, requestKey: restoreRequestKey(request) }
}

export function restorePreviewBlockReason(
  bound: BoundRestorePreview | null,
  request: RestoreRequest,
  tenantId?: string,
): string | null {
  if (!bound) return '创建恢复审批单前，必须先完成当前参数的恢复预演'
  if (bound.requestKey !== restoreRequestKey(request)) {
    return '恢复参数已改变，请重新执行预演'
  }
  if (bound.preview.operation_id !== request.operation_id.trim()) {
    return '预演结果与当前 Operation ID 不匹配，请重新执行预演'
  }
  if (tenantId && bound.preview.tenant_id !== tenantId) {
    return '预演结果属于其他租户，请重新执行预演'
  }
  if (!bound.preview.requires_approval) {
    return '服务端预演未声明审批要求，已阻止创建恢复审批单'
  }
  return null
}
