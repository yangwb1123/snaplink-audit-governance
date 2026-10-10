import { randomUUID } from 'node:crypto'
import { expect, test } from '@playwright/test'
import { audit, bindSource, headers, login } from './identity'

test('export-only permission opens evidence without integrity or Hold privileges', async ({ page }) => {
  await login(page, 'audit-operator', '#evidence', ['openid', 'profile', 'audit:export:create'])
  await expect(page.getByRole('heading', { name: '创建合规导出', exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: '完整性', exact: true })).toHaveCount(0)
  await expect(page.getByRole('button', { name: '法律保留', exact: true })).toHaveCount(0)
  const requests: string[] = []
  page.on('request', (request) => requests.push(new URL(request.url()).pathname))
  await page.evaluate(() => { window.location.hash = '#evidence/holds' })
  await expect(page.getByText('当前会话没有该功能权限', { exact: true })).toBeVisible()
  expect(requests.some((path) => path.endsWith('/legal-holds'))).toBe(false)
})

test('real API: scoped export, later Hold denial, refresh/deep link and two-subject approval', async ({ page, browser }) => {
  const token = await login(page, 'audit-operator')
  await bindSource(page, token)
  const operation = `browser-${randomUUID()}`
  const seed = await page.request.post(`${audit}/api/v1/events?wait_for=ledgered`, {
    headers: headers(token), data: {
      event_id: `event-${randomUUID()}`, tenant_id: 'demo', source_system: 'demo', event_type: 'audit.event',
      schema_id: 'audit.event', schema_version: 1, occurred_at: new Date(Date.now() - 5000).toISOString(), operation_id: operation,
      actor: { id: 'browser-fixture', type: 'user' }, action: 'update', outcome: 'success',
      aggregate_type: 'browser-fixture', aggregate_id: operation, aggregate_version: 1,
      changed_fields: { status: { before: 'open', after: 'paid' } }, payload: { fixture: true },
      data_classification: 'internal', retention_class: 'standard', idempotency_key: operation,
    },
  })
  expect(seed.status()).toBeLessThan(300)
  await page.getByRole('textbox', { name: 'Operation ID', exact: true }).fill(operation)
  const createdResponse = page.waitForResponse((response) => new URL(response.url()).pathname.endsWith('/exports') && response.request().method() === 'POST')
  await page.getByRole('button', { name: '创建范围导出', exact: true }).click()
  const created = await createdResponse
  expect(created.status()).toBe(202)
  const job = await created.json() as { id: string }
  await expect(page.getByRole('button', { name: '下载 JSONL', exact: true })).toBeEnabled()
  await expect(page.getByText('1 条事件', { exact: false })).toBeVisible()
  const downloadEvent = page.waitForEvent('download')
  await page.getByRole('button', { name: '下载 JSONL', exact: true }).click()
  const completedDownload = await downloadEvent
  expect(completedDownload.suggestedFilename()).toBe(`${job.id}.jsonl`)
  expect(await completedDownload.failure()).toBeNull()

  const blocked = await page.request.post(`${audit}/api/v1/legal-holds?tenant_id=demo`, {
    headers: headers(token), data: { name: operation, reason: 'isolated browser Hold fixture', filter: { from: new Date(Date.now() - 60_000).toISOString(), to: new Date(Date.now() + 60_000).toISOString(), operation_id: operation } },
  })
  expect(blocked.status()).toBe(201)
  const hold = await blocked.json() as { id: string }
  await page.getByRole('button', { name: '导出任务', exact: true }).click()
  await page.getByRole('link', { name: job.id, exact: true }).click()
  await expect(page.getByText('导出被法律保留阻断', { exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: '下载 JSONL', exact: true })).toHaveCount(0)
  const download = await page.request.get(`${audit}/api/v1/exports/${job.id}/download?tenant_id=demo`, { headers: headers(token) })
  expect(download.status()).toBe(403)
  expect((await download.json() as { error: { reason: string } }).error.reason).toBe('legal_hold')
  const release = await page.request.post(`${audit}/api/v1/legal-holds/${hold.id}/release?tenant_id=demo`, { headers: headers(token) })
  expect(release.status()).toBe(200)

  // Tokens are in memory only. Refresh requires a new code exchange and then
  // returns to the safe job deep link in one-time PKCE metadata.
  await page.reload()
  const reloginToken = await login(page, 'audit-operator', `#evidence/exports/${job.id}`)
  await expect(page.getByRole('button', { name: '下载 JSONL', exact: true })).toBeEnabled()
  const leaked = await page.evaluate((secret) => [localStorage, sessionStorage].some((storage) => Object.values(storage).some((value) => String(value).includes(secret))), reloginToken)
  expect(leaked).toBe(false)
  expect(await page.evaluate(() => sessionStorage.getItem('audit-governance.snaplink.pending.v1') !== null)).toBe(false)

  await page.getByRole('button', { name: '恢复审批', exact: true }).click()
  await expect(page.getByRole('heading', { name: '恢复预演与申请', exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: '刷新任务', exact: true })).toBeEnabled()
  await page.getByRole('textbox', { name: 'Operation ID', exact: true }).fill(operation)
  await page.getByRole('textbox', { name: '恢复原因', exact: true }).fill('isolated browser recovery fixture')
  await page.getByRole('button', { name: '预演', exact: true }).click()
  await expect(page.getByRole('button', { name: '创建审批单', exact: true })).toBeEnabled()
  await page.getByRole('button', { name: '创建审批单', exact: true }).click()
  const restoreResponse = page.waitForResponse((response) => new URL(response.url()).pathname.endsWith('/restores') && response.request().method() === 'POST')
  await page.getByRole('button', { name: '确认创建', exact: true }).click()
  const restore = await (await restoreResponse).json() as { id: string }
  await expect(page.getByRole('button', { name: '批准', exact: true })).toBeDisabled()
  const selfDecision = await page.request.post(`${audit}/api/v1/restores/${restore.id}/approve?tenant_id=demo`, { headers: headers(reloginToken) })
  expect(selfDecision.status()).toBe(403)

  const approverContext = await browser.newContext({ baseURL: new URL(page.url()).origin })
  try {
    const approver = await approverContext.newPage()
    await login(approver, 'audit-approver', `#evidence/restores/${restore.id}`)
    await approver.getByLabel('任务视图', { exact: true }).selectOption('actionable')
    await expect(approver.getByRole('link', { name: restore.id, exact: true })).toBeVisible()
    await expect(approver.getByRole('button', { name: '批准', exact: true })).toBeEnabled()
    await approver.getByRole('button', { name: '批准', exact: true }).click()
    const approval = approver.waitForResponse((response) => new URL(response.url()).pathname.endsWith(`/restores/${restore.id}/approve`))
    await approver.getByRole('button', { name: '确认批准', exact: true }).click()
    expect((await approval).status()).toBe(200)
    await expect(approver.getByText('执行状态：本平台尚未接入。', { exact: false })).toBeVisible()
    await expect(approver.getByRole('button', { name: '批准', exact: true })).toHaveCount(0)
  } finally {
    await approverContext.close()
  }
})

test('A -> B -> A discards old list data when a fetcher ignores cancellation', async ({ page }) => {
  // Deliberately remove transport cancellation to exercise the independent
  // generation/disposal check, not merely the native AbortController path.
  await page.addInitScript(() => {
    const fetcher = window.fetch.bind(window)
    window.fetch = (input, init) => fetcher(input, { ...init, signal: undefined })
  })
  const token = await login(page, 'audit-operator')
  const tenant = `browser-${randomUUID()}`
  const create = await page.request.post(`${audit}/api/v1/tenants`, { headers: headers(token), data: { id: tenant, name: 'isolated browser tenant', active: true } })
  expect(create.status()).toBe(201)
  await page.getByRole('button', { name: '刷新租户', exact: true }).click()
  await expect(page.locator(`#tenant-scope option[value="${tenant}"]`)).toHaveCount(1)

  let release!: () => void
  const gate = new Promise<void>((resolve) => (release = resolve))
  let intercepted!: () => void
  const started = new Promise<void>((resolve) => (intercepted = resolve))
  let once = false
  await page.route('**/audit-api/api/v1/exports?**', async (route) => {
    if (once || new URL(route.request().url()).searchParams.get('tenant_id') !== 'demo') return route.continue()
    once = true
    intercepted()
    await gate
    await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ items: [{ id: 'stale-browser-task', tenant_id: 'demo', requested_by: 'audit-operator', created_at: new Date().toISOString(), status: 'completed' }], count: 1 }) })
  })
  await page.getByRole('button', { name: '刷新任务', exact: true }).click()
  await started
  await page.locator('#tenant-scope').selectOption(tenant)
  await expect(page.getByText('当前条件下没有任务。', { exact: true })).toBeVisible()
  await page.locator('#tenant-scope').selectOption('demo')
  await expect(page.getByRole('button', { name: '刷新任务', exact: true })).toBeEnabled()
  release()
  await page.waitForTimeout(100)
  await expect(page.getByRole('link', { name: 'stale-browser-task', exact: true })).toHaveCount(0)
})
