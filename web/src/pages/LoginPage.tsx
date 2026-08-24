import { IrisAlert, IrisButton, IrisCard, IrisIcon, IrisSpinner } from '@iris-ui-kit/react'
import { useAuth } from '../auth/AuthProvider'

export function LoginPage() {
  const { login, status, error, clearError } = useAuth()
  const loading = status === 'checking'
  return (
    <main className="login-page">
      <IrisCard variant="outline" padding="lg" className="login-card">
        <div className="login-brand" aria-hidden="true">
          <IrisIcon name="shield" size={24} />
        </div>
        <p className="eyebrow">SNAPLINK PLATFORM</p>
        <h1>Audit Governance</h1>
        <p className="login-copy">
          通过 Snaplink 统一登录。浏览器使用 Authorization Code +
          PKCE，访问令牌只保存在当前页面内存中。
        </p>
        {error ? (
          <IrisAlert tone="danger" title="登录未完成" closable onClose={clearError}>
            {error}
          </IrisAlert>
        ) : null}
        <IrisButton
          variant="solid"
          size="lg"
          className="login-button"
          disabled={loading}
          onClick={() => void login()}
        >
          {loading ? <IrisSpinner size="sm" /> : <IrisIcon name="user" size={18} />}
          {loading ? '正在连接 Snaplink…' : '使用 Snaplink 登录'}
        </IrisButton>
        <p className="login-footnote">resource: audit-governance · scope: openid profile</p>
      </IrisCard>
    </main>
  )
}
