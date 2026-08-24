import { createSkinEngine, localStorageSkinStorage } from '@iris-ui-kit/react'

export const skinEngine = createSkinEngine({
  default: 'light',
  mode: 'fixed',
  storage: localStorageSkinStorage('audit-governance.skin'),
})
