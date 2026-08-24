import { IrisSpinner } from '@iris-ui-kit/react'
import { useAuth } from './auth/AuthProvider'
import { LoginPage } from './pages/LoginPage'
import { Shell } from './Shell'

export function App() {
  const { status, session } = useAuth()
  if (status === 'checking') {
    return (
      <main className="bootstrap-page" role="status">
        <IrisSpinner size="lg" />
        <span>正在校验 Snaplink 登录回调…</span>
      </main>
    )
  }
  return session ? <Shell /> : <LoginPage />
}
