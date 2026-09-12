import { svelte } from '@sveltejs/vite-plugin-svelte'
import { defineConfig } from 'vite'

export default defineConfig({
  plugins: [svelte()],
  resolve: {
    // Svelte 5 exposes a browser-aware export condition. Without it Vite can
    // select the SSR entry during development and render a blank application.
    conditions: ['browser'],
  },
  server: {
    host: '127.0.0.1',
    port: 5178,
    strictPort: true,
    proxy: {
      '/audit-api': {
        target: process.env.AUDIT_API_PROXY ?? 'http://localhost:8089',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/audit-api/, ''),
      },
      '/snaplink-api': {
        target: process.env.SNAPLINK_API_PROXY ?? 'http://localhost:18082',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/snaplink-api/, ''),
      },
    },
  },
  build: {
    chunkSizeWarningLimit: 650,
    rollupOptions: {
      output: {
        manualChunks: {
          'oidc-vendor': ['jose'],
          'iris-vendor': ['@iris-ui-kit/svelte'],
        },
      },
    },
  },
  test: { environment: 'node' },
})
