import * as React from 'react'
import { IrisAlert, IrisButton, IrisSpinner } from '@iris-ui-kit/react'
import { ApiError } from '../api/client'

export function PageHeader({
  title,
  description,
  actions,
}: {
  title: string
  description: string
  actions?: React.ReactNode
}): React.ReactElement {
  return (
    <div className="page-header">
      <div>
        <h1 className="page-title">{title}</h1>
        <p className="page-description">{description}</p>
      </div>
      {actions ? <div className="page-actions">{actions}</div> : null}
    </div>
  )
}

export function ErrorNotice({ error, onRetry }: { error: unknown; onRetry?: () => void }) {
  if (!error) return null
  const apiError = error instanceof ApiError ? error : null
  const message = error instanceof Error ? error.message : '请求失败'
  return (
    <IrisAlert tone={apiError?.status === 403 ? 'warning' : 'danger'} title={message}>
      <div className="alert-detail">
        {apiError ? (
          <span>
            HTTP {apiError.status} · {apiError.code}
            {apiError.requestId ? ` · request_id=${apiError.requestId}` : ''}
          </span>
        ) : null}
        {onRetry ? (
          <IrisButton size="sm" variant="outline" onClick={onRetry}>
            重试
          </IrisButton>
        ) : null}
      </div>
    </IrisAlert>
  )
}

export function LoadingBlock({ label = '正在读取 Audit API…' }: { label?: string }) {
  return (
    <div className="loading-block" role="status">
      <IrisSpinner />
      <span>{label}</span>
    </div>
  )
}

export function EmptyBlock({ children }: { children: React.ReactNode }) {
  return <div className="empty-block">{children}</div>
}

export function formatDate(value?: string): string {
  if (!value) return '—'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString('zh-CN')
}
