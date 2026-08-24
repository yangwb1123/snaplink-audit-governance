import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { IrisProvider } from '@iris-ui-kit/react'
import { App } from './App'
import { AuthProvider } from './auth/AuthProvider'
import { skinEngine } from './skin'
import './style.css'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <IrisProvider skin={skinEngine} locale="zh-CN">
      <AuthProvider>
        <App />
      </AuthProvider>
    </IrisProvider>
  </StrictMode>,
)
