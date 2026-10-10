import { ApiError } from './client'

export function errorPresentation(error: unknown): { title: string; guidance: string; retryable: boolean; warning: boolean } {
  if (!(error instanceof ApiError)) {
    return { title: error instanceof Error ? error.message : '请求失败', guidance: '检查连接后重试；变更请求请先刷新确认结果，避免重复提交。', retryable: true, warning: false }
  }
  if (error.reason === 'legal_hold') {
    return { title: '导出被法律保留阻断', guidance: '详情和下载仍受有效 Legal Hold 保护。请联系有保留管理权限的人员确认，不要通过重复导出来绕过。', retryable: false, warning: true }
  }
  if (error.status === 401) {
    return { title: '登录已失效', guidance: '请重新通过 Snaplink 登录。', retryable: false, warning: true }
  }
  if (error.status === 403) {
    return { title: '当前操作不被允许', guidance: '请确认会话权限、法律保留或申请人与审批人的职责分离要求。', retryable: false, warning: true }
  }
  if (error.status === 429) {
    return { title: '租户配额已达到上限', guidance: '请稍后重试，或联系管理员检查租户预算。', retryable: true, warning: true }
  }
  if (error.status === 503) {
    return { title: '服务暂时不可用', guidance: '读取可以重试；变更可能已提交，请先查询结果，再决定是否使用原幂等标识重试。', retryable: true, warning: false }
  }
  if (error.status === 409) {
    return { title: '状态或内容发生冲突', guidance: '请刷新最新状态。已完成的决策不能重复提交，幂等标识也不能复用给不同内容。', retryable: true, warning: true }
  }
  return { title: error.message, guidance: error.status === 400 || error.status === 422 ? '请检查输入、时间范围及分页游标。' : '请携带 request_id 联系运维；不要盲目重复变更请求。', retryable: error.status >= 500, warning: false }
}
