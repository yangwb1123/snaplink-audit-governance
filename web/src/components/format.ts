import { ApiError } from '../api/client'

export function formatDate(value?: string): string {
  if (!value) return '—'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString('zh-CN')
}

export function errorText(error: unknown): string {
  return error instanceof Error ? error.message : '请求失败'
}

export function errorDetail(error: unknown): string | null {
  if (!(error instanceof ApiError)) return null
  return `HTTP ${error.status} · ${error.code}${error.requestId ? ` · request_id=${error.requestId}` : ''}`
}
