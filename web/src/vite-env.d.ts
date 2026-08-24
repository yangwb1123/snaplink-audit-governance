/// <reference types="vite/client" />

interface Window {
  __AUDIT_GOVERNANCE_CONFIG__?: Partial<import('./config').RuntimeConfig>
}
