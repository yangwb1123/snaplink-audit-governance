import { randomUUID } from 'node:crypto'
import { expect, test } from '@playwright/test'
import { audit, bindSource, fixtureCall, headers, login } from './identity'

// Only controller-authorized, already running loopback fixtures. No service
// startup, trace, screenshots, video, or automatic failure DOM snapshots.
test.use({ trace: 'off', screenshot: 'off', video: 'off' })
process.env.PLAYWRIGHT_NO_COPY_PROMPT = '1'
test.beforeAll(() => {
  if (process.env.AUDIT_EXPORT_HOLD_FIXTURE_AUTHORIZED !== 'true') {
    throw new Error('Export download Hold journey not executed: controller authorization for this run is required')
  }
})

test('completed details, later Hold, direct download denial, bound read-only refresh, explicit download', async ({ page, browser }) => {
  // Include assertions/JSON parsing as well as credential-bearing transport in
  // the existing sanitized fixture boundary. No raw exception/cause escapes.
  await fixtureCall(page.request, async () => {
    let holdManagementRequests = 0
    let downloadRequests = 0
    let downloads = 0
    let observerFailed = false
    const mutations: string[] = []
    const observations: Array<{ method: string; path: string; tenantMatches: boolean }> = []
    page.on('download', () => { downloads += 1 })
    page.on('request', (request) => {
      try {
        const url = new URL(request.url())
        const start = url.pathname.indexOf('/api/v1/')
        if (start < 0) return
        const path = url.pathname.slice(start)
        // Only safe method/resource paths and a tenant comparison, never query
        // strings, headers, response bodies, owner or evidence diagnostics.
        observations.push({ method: request.method(), path, tenantMatches: url.searchParams.get('tenant_id') === 'demo' })
        if (path.includes('/legal-holds')) holdManagementRequests += 1
        if (path.endsWith('/download')) downloadRequests += 1
        if (['POST', 'PUT', 'PATCH', 'DELETE'].includes(request.method())) mutations.push(path)
      } catch { observerFailed = true }
    })
    await login(page, 'audit-operator', '#evidence/exports', ['openid', 'profile', 'audit:export:create'])
    await expect(page.getByRole('button', { name: '法律保留', exact: true })).toHaveCount(0)
    const managerContext = await browser.newContext({ baseURL: new URL(page.url()).origin })
    try {
      const manager = await managerContext.newPage()
      const managerToken = await login(manager, 'audit-operator', '#evidence/exports')
      await bindSource(manager, managerToken)
      const operation = `browser-export-hold-${randomUUID()}`
      await fixtureCall(manager.request, async () => {
        const seeded = await manager.request.post(`${audit}/api/v1/events?wait_for=ledgered`, {
          headers: headers(managerToken), data: {
            event_id: `event-${randomUUID()}`, tenant_id: 'demo', source_system: 'demo', event_type: 'audit.event',
            schema_id: 'audit.event', schema_version: 1, occurred_at: new Date(Date.now() - 5000).toISOString(), operation_id: operation,
            actor: { id: 'browser-fixture', type: 'user' }, action: 'update', outcome: 'success',
            aggregate_type: 'browser-fixture', aggregate_id: operation, aggregate_version: 1,
            changed_fields: { status: { before: 'open', after: 'paid' } }, payload: { fixture: true },
            data_classification: 'internal', retention_class: 'standard', idempotency_key: operation,
          },
        })
        expect(seeded.status()).toBeLessThan(300)
      })
      await page.getByRole('textbox', { name: 'Operation ID', exact: true }).fill(operation)
      const creation = fixtureCall(page.request, () => page.waitForResponse((response) =>
        new URL(response.url()).pathname.endsWith('/exports') && response.request().method() === 'POST'))
      void creation.catch(() => {})
      await page.getByRole('button', { name: '创建范围导出', exact: true }).click()
      const created = await creation
      expect(created.status()).toBe(202)
      const job = await created.json() as { id: string }
      const details = page.locator('.section-card').filter({ has: page.getByRole('heading', { name: '导出详情', exact: true }) })
      const downloadButton = page.getByRole('button', { name: '下载 JSONL', exact: true })
      await expect(downloadButton).toBeEnabled()
      await expect(details.getByText('1 条事件', { exact: false })).toBeVisible()
      const oldRange = await details.getByText(/^范围：/).textContent()
      const oldDigest = await details.locator('code.hash-value').textContent()
      expect(Boolean(oldRange?.includes(operation) && oldDigest)).toBe(true)
      const list = page.locator('.native-table').filter({ has: page.getByRole('link', { name: job.id, exact: true }) })
      await expect(list).toBeVisible()
      const safeList = await list.textContent()

      const hold = await fixtureCall(manager.request, async () => {
        const response = await manager.request.post(`${audit}/api/v1/legal-holds?tenant_id=demo`, {
          headers: headers(managerToken), data: {
            name: `browser-hold-${randomUUID()}`, reason: 'isolated download denial fixture',
            filter: { from: new Date(Date.now() - 60_000).toISOString(), to: new Date(Date.now() + 60_000).toISOString(), operation_id: operation },
          },
        })
        expect(response.status()).toBe(201)
        return await response.json() as { id: string }
      })
      // Essential reproduction: still showing the completed details. Do NOT
      // navigate, refresh or GET the job before clicking its download button.
      await expect(downloadButton).toBeEnabled()
      expect((await details.getByText(/^范围：/).textContent()) === oldRange).toBe(true)
      expect((await details.locator('code.hash-value').textContent()) === oldDigest).toBe(true)
      await expect(details.getByText('1 条事件', { exact: false })).toBeVisible()
      const denial = fixtureCall(page.request, () => page.waitForResponse((response) =>
        new URL(response.url()).pathname.endsWith(`/exports/${job.id}/download`)))
      void denial.catch(() => {})
      await downloadButton.click()
      const refused = await denial
      expect(refused.status()).toBe(403)
      expect((await refused.json() as { error: { reason: string } }).error.reason === 'legal_hold').toBe(true)
      await expect(page.getByText('导出被法律保留阻断', { exact: true })).toBeVisible()
      await expect(details.getByText(/^范围：/)).toHaveCount(0)
      await expect(details.locator('code.hash-value')).toHaveCount(0)
      await expect(details.getByText('1 条事件', { exact: false })).toHaveCount(0)
      await expect(downloadButton).toHaveCount(0)
      expect((await list.textContent()) === safeList).toBe(true)
      expect(downloads).toBe(0)
      expect(downloadRequests).toBe(1)

      await details.locator('input[placeholder="export job id"]').fill(`edited-${randomUUID()}`)
      const refresh = page.getByRole('button', { name: '刷新导出详情', exact: true })
      const detailPath = `/api/v1/exports/${job.id}`
      const readOriginal = () => fixtureCall(page.request, () => page.waitForResponse((response) =>
        new URL(response.url()).pathname.endsWith(detailPath) && response.request().method() === 'GET'))
      observations.length = 0
      const heldRead = readOriginal()
      void heldRead.catch(() => {})
      await refresh.click()
      expect((await heldRead).status()).toBe(403)
      await expect(refresh).toBeEnabled()
      await expect(downloadButton).toHaveCount(0)
      await expect(details.locator('code.hash-value')).toHaveCount(0)
      expect(observations.length).toBe(1)
      expect(observations.every((item) => item.method === 'GET' && item.path === detailPath && item.tenantMatches)).toBe(true)
      expect(downloadRequests).toBe(1)
      expect(downloads).toBe(0)

      // Only the independent, privileged fixture releases its own Hold. The
      // export-only UI must neither call this interface nor replay a download.
      await fixtureCall(manager.request, async () => {
        const released = await manager.request.post(`${audit}/api/v1/legal-holds/${hold.id}/release?tenant_id=demo`, { headers: headers(managerToken) })
        expect(released.status()).toBe(200)
      })
      observations.length = 0
      const validRead = readOriginal()
      void validRead.catch(() => {})
      await refresh.click()
      const restored = await validRead
      expect(restored.status()).toBe(200)
      const observed = await restored.json() as { id: string; tenant_id: string; status: string }
      expect(observed.id === job.id && observed.tenant_id === 'demo' && observed.status === 'completed').toBe(true)
      await expect(downloadButton).toBeEnabled()
      expect((await details.getByText(/^范围：/).textContent()) === oldRange).toBe(true)
      expect((await details.locator('code.hash-value').textContent()) === oldDigest).toBe(true)
      await expect(details.getByText('1 条事件', { exact: false })).toBeVisible()
      expect(observations.length).toBe(1)
      expect(observations.every((item) => item.method === 'GET' && item.path === detailPath && item.tenantMatches)).toBe(true)
      expect(downloadRequests).toBe(1)
      expect(downloads).toBe(0)

      const file = fixtureCall(page.request, () => page.waitForEvent('download'))
      void file.catch(() => {})
      await downloadButton.click()
      const downloaded = await file
      expect(downloaded.suggestedFilename() === `${job.id}.jsonl`).toBe(true)
      expect(await downloaded.failure()).toBeNull()
      expect(downloadRequests).toBe(2)
      expect(downloads).toBe(1)
      expect(holdManagementRequests).toBe(0)
      expect(mutations.length).toBe(1)
      expect(mutations.every((path) => path === '/api/v1/exports')).toBe(true)
      expect(observerFailed).toBe(false)
    } finally { await managerContext.close() }
  })
})
