import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

export default defineConfig({
  plugins: [react()],
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
    // IrisTable intentionally ships its enterprise interaction surface as one chunk.
    chunkSizeWarningLimit: 650,
    rollupOptions: {
      output: {
        manualChunks: {
          'react-vendor': ['react', 'react-dom/client'],
          'oidc-vendor': ['jose'],
          'iris-vendor': ['@iris-ui-kit/react'],
        },
      },
    },
  },
  test: { environment: 'node' },
})
