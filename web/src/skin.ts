import { createSkinEngine, localStorageSkinStorage, type Skin } from '@iris-ui-kit/svelte'

export const STORAGE_KEY = 'audit-governance-web-skin'

const ocean: Skin = {
  id: 'ocean',
  name: 'Ocean',
  extends: 'dark',
  tokens: {
    'iris.background': '#0b1f33',
    'iris.surface': '#0f2c4a',
    'iris.surface.hover': '#143a61',
    'iris.border': '#1d4e7e',
    'iris.foreground': '#e6f1fb',
    'iris.muted': '#7fa8cc',
    'iris.primary': '#38bdf8',
    'iris.primary.foreground': '#04222f',
    'iris.accent': '#22d3ee',
  },
}

const sunrise: Skin = {
  id: 'sunrise',
  name: 'Sunrise',
  extends: 'light',
  tokens: {
    'iris.background': '#fff7ed',
    'iris.surface': '#ffffff',
    'iris.surface.hover': '#ffedd5',
    'iris.border': '#fed7aa',
    'iris.foreground': '#431407',
    'iris.muted': '#b45309',
    'iris.primary': '#f97316',
    'iris.primary.foreground': '#fff7ed',
    'iris.accent': '#f43f5e',
  },
}

const violet: Skin = {
  id: 'violet',
  name: 'Violet',
  extends: 'dark',
  tokens: {
    'iris.background': '#16121f',
    'iris.surface': '#211a30',
    'iris.surface.hover': '#2c2342',
    'iris.border': '#3b2f57',
    'iris.foreground': '#ece8f6',
    'iris.muted': '#a99fc8',
    'iris.primary': '#8b5cf6',
    'iris.primary.foreground': '#140f20',
    'iris.accent': '#ec4899',
  },
}

const auto: Skin = {
  id: 'auto',
  name: 'Auto (system)',
  extends: 'light',
  variants: { light: 'sunrise', dark: 'ocean' },
}

export const skinEngine = createSkinEngine({
  skins: [ocean, sunrise, violet, auto],
  default: 'sunrise',
  storage: localStorageSkinStorage(STORAGE_KEY),
})
