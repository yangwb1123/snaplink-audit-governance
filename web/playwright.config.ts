import { defineConfig } from '@playwright/test'

const baseURL = process.env.AUDIT_WEB_URL ?? 'http://localhost:15178'
for (const candidate of [baseURL, process.env.AUDIT_API_URL ?? 'http://localhost:19089', process.env.SNAPLINK_IDP_URL ?? 'http://localhost:18082', process.env.SNAPLINK_HOSTED_LOGIN_URL ?? 'http://localhost:4444/login/']) {
  const url = new URL(candidate)
  if (!['localhost', '127.0.0.1', '[::1]'].includes(url.hostname) || url.username || url.password || url.search || url.hash) {
    throw new Error('Browser fixtures require isolated loopback services, never production or credential-bearing URLs')
  }
}

export default defineConfig({
  testDir: './test/browser',
  testMatch: '**/*.e2e.ts',
  workers: 1,
  retries: 0,
  timeout: 60_000,
  expect: { timeout: 15_000 },
  reporter: 'line',
  outputDir: '/tmp/audit-governance-browser-results',
  use: {
    baseURL,
    // Never record access tokens, payloads or password requests into traces.
    trace: 'off',
    video: 'off',
    screenshot: 'off',
    channel: process.env.AUDIT_BROWSER_CHANNEL ?? 'chrome',
  },
})
