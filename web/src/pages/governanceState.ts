import type { RestoreRun } from '../api/types'

const terminalExportStatuses = new Set(['completed', 'failed'])
export const EXPORT_POLL_INTERVAL_MS = 1200
export const EXPORT_POLL_MAX_ATTEMPTS = 60

export function shouldPollExport(status: string): boolean {
  return !terminalExportStatuses.has(status.toLowerCase())
}

export function canAutoPollExport(status: string, attempts: number, paused: boolean): boolean {
  return shouldPollExport(status) && !paused && attempts < EXPORT_POLL_MAX_ATTEMPTS
}

export function restoreDecisionBlockReason(
  run: RestoreRun,
  actorSubject: string,
  hasPermission: boolean,
): string | null {
  if (run.status !== 'pending_approval') return '该恢复申请已经完成决策'
  if (!hasPermission) return '当前 Snaplink 会话没有恢复决策权限'
  if (run.created_by && run.created_by === actorSubject) {
    return '申请人与审批人必须是不同的 Snaplink subject，请切换审批账号'
  }
  return null
}
