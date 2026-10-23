import { randomUUID } from 'node:crypto'
import { expect, test } from '@playwright/test'
import type { RestoreRun } from '../../src/api/types'
import { audit, bindSource, fixtureCall, headers, login } from './identity'

// No service startup and no credential/payload recording, including the
// automatic failure DOM snapshot (which can contain a reason or preview).
test.use({ trace: 'off', screenshot: 'off', video: 'off' })
process.env.PLAYWRIGHT_NO_COPY_PROMPT = '1'
test.beforeAll(() => {
  if (process.env.AUDIT_RESTORE_FIXTURE_AUTHORIZED !== 'true') {
    throw new Error('Real restore journey not executed: controller authorization for this run is required')
  }
})

function subject(token: string): string {
  try {
    const claims = JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString()) as { sub: string }
    if (typeof claims.sub === 'string' && claims.sub) return claims.sub
  } catch { /* No token/claims in diagnostics. */ }
  throw new Error('Fixture subject unavailable')
}

for (const action of ['approve', 'reject'] as const) {
  test(`real ${action}: one durable decision, browser response lost, GET reconciliation`, async ({ page, browser }) => {
    await fixtureCall(page.request, async () => {
      const creatorToken = await login(page, 'audit-operator', '#evidence/restores')
      await bindSource(page, creatorToken)
      const operation = `browser-reconcile-${randomUUID()}`
      // The same privacy boundary includes POST transport, status and JSON
      // failures; disabling tracing alone cannot sanitize APIRequestContext.
      const seeded = await page.request.post(`${audit}/api/v1/events?wait_for=ledgered`, {
        headers: headers(creatorToken), data: {
          event_id: `event-${randomUUID()}`, tenant_id: 'demo', source_system: 'demo', event_type: 'audit.event',
          schema_id: 'audit.event', schema_version: 1, occurred_at: new Date(Date.now() - 5000).toISOString(), operation_id: operation,
          actor: { id: 'browser-fixture', type: 'user' }, action: 'update', outcome: 'success',
          aggregate_type: 'browser-fixture', aggregate_id: operation, aggregate_version: 1,
          changed_fields: { status: { before: 'open', after: 'paid' } }, payload: { fixture: true },
          data_classification: 'internal', retention_class: 'standard', idempotency_key: operation,
        },
      })
      expect(seeded.status()).toBeLessThan(300)
      await seeded.json()
      await page.getByRole('textbox', { name: 'Operation ID', exact: true }).fill(operation)
      await page.getByRole('textbox', { name: '恢复原因', exact: true }).fill(randomUUID())
      await page.getByRole('button', { name: '预演', exact: true }).click()
      await expect(page.getByRole('button', { name: '创建审批单', exact: true })).toBeEnabled()
      await page.getByRole('button', { name: '创建审批单', exact: true }).click()
      const creation = fixtureCall(page.request, () => page.waitForResponse((response) =>
        new URL(response.url()).pathname.endsWith('/restores') && response.request().method() === 'POST'))
      void creation.catch(() => {})
      await page.getByRole('button', { name: '确认创建', exact: true }).click()
      const createdResponse = await creation
      expect(createdResponse.status()).toBe(202)
      const created = await createdResponse.json() as RestoreRun
      expect(created.tenant_id === 'demo' && created.created_by === subject(creatorToken)).toBe(true)
      await expect(page.getByRole('button', { name: '批准', exact: true })).toBeDisabled()
      await expect(page.getByRole('button', { name: '拒绝', exact: true })).toBeDisabled()

      const context = await browser.newContext({ baseURL: new URL(page.url()).origin })
      try {
        const approver = await context.newPage()
        const approverToken = await login(approver, 'audit-approver', `#evidence/restores/${created.id}`)
        const actor = subject(approverToken)
        expect(actor !== subject(creatorToken)).toBe(true)
        await expect(approver.getByRole('button', { name: '批准', exact: true })).toBeEnabled()
        await expect(approver.getByRole('button', { name: '拒绝', exact: true })).toBeEnabled()

        const detailPath = `/restores/${created.id}`
        const decisionPath = `${detailPath}/${action}`
        let attempted = 0
        let forwarded = 0
        let durable: RestoreRun | undefined
        let callbackFailed = false
        const mutations: string[] = []
        let committed!: () => void
        let fail!: (error: Error) => void
        const submission = new Promise<void>((done, reject) => { committed = done; fail = reject })
        void submission.catch(() => {})
        const failSafely = () => {
          callbackFailed = true
          fail(new Error('Real decision or response-loss injection failed; diagnostics withheld'))
        }
        approver.on('request', (request) => {
          try {
            if (['POST', 'PUT', 'PATCH', 'DELETE'].includes(request.method())) mutations.push(new URL(request.url()).pathname)
          } catch { failSafely() }
        })
        await approver.route((url) => url.pathname.endsWith(`${detailPath}/approve`) || url.pathname.endsWith(`${detailPath}/reject`), async (route) => {
          try {
            await fixtureCall(approver.request, async () => {
              if (route.request().method() !== 'POST') { await route.continue(); return }
              attempted += 1
              // Guard the real fixture from a regression replay; assertions still
              // fail on any second attempt rather than hiding the client defect.
              if (forwarded !== 0) { await route.abort('blockedbyclient'); failSafely(); return }
              forwarded += 1
              const real = await route.fetch({ maxRetries: 0, maxRedirects: 0 })
              if (real.status() !== 200) throw new Error('Decision was not accepted')
              durable = await real.json() as RestoreRun
              await route.abort('connectionreset') // discard only the browser response
              committed()
            })
          } catch {
            failSafely()
            try { await fixtureCall(approver.request, () => route.abort('failed')) } catch { /* No route/request cause escapes. */ }
          }
        })
        const label = action === 'approve' ? '批准' : '拒绝'
        await approver.getByRole('button', { name: label, exact: true }).click()
        await Promise.all([submission, approver.getByRole('button', { name: `确认${label}`, exact: true }).click()])
        await expect(approver.getByText('决策可能已提交', { exact: false })).toBeVisible()
        await expect(approver.getByRole('dialog')).toHaveCount(0)
        await expect(approver.getByRole('button', { name: '批准', exact: true })).toBeDisabled()
        await expect(approver.getByRole('button', { name: '拒绝', exact: true })).toBeDisabled()
        expect(attempted).toBe(1); expect(forwarded).toBe(1)

        const status = action === 'approve' ? 'approved' : 'rejected'
        const by = action === 'approve' ? durable?.approved_by : durable?.rejected_by
        const at = action === 'approve' ? durable?.approved_at : durable?.rejected_at
        expect(durable?.id === created.id && durable?.tenant_id === 'demo' && durable?.status === status && by === actor).toBe(true)
        expect(Boolean(at && Number.isFinite(Date.parse(at)))).toBe(true)
        const observations: Array<{ method: string; path: string }> = []
        approver.on('request', (request) => {
          try { observations.push({ method: request.method(), path: new URL(request.url()).pathname }) }
          catch { failSafely() }
        })
        const detail = fixtureCall(approver.request, () => approver.waitForResponse((response) =>
          new URL(response.url()).pathname.endsWith(detailPath) && response.request().method() === 'GET'))
        void detail.catch(() => {})
        await approver.getByRole('button', { name: '刷新核对', exact: true }).click()
        const response = await detail
        expect(response.status()).toBe(200)
        expect(new URL(response.url()).searchParams.get('tenant_id')).toBe('demo')
        const observed = await response.json() as RestoreRun
        expect(observed.id === created.id && observed.tenant_id === 'demo' && observed.status === status).toBe(true)
        expect((action === 'approve' ? observed.approved_by : observed.rejected_by) === actor).toBe(true)
        expect((action === 'approve' ? observed.approved_at : observed.rejected_at) === at).toBe(true)
        await expect(approver.locator('time')).toHaveAttribute('datetime', at!)
        expect((await approver.getByText('申请人：', { exact: false }).textContent())?.includes(`决策人：${actor}`)).toBe(true)
        await expect(approver.getByText('决策可能已提交', { exact: false })).toHaveCount(0)
        await expect(approver.getByRole('button', { name: '批准', exact: true })).toHaveCount(0)
        await expect(approver.getByRole('button', { name: '拒绝', exact: true })).toHaveCount(0)
        await expect(approver.getByText('执行状态：本平台尚未接入。', { exact: false })).toBeVisible()
        expect(observations.length).toBe(1)
        expect(observations.every((item) => item.method === 'GET' && item.path.endsWith(detailPath))).toBe(true)
        expect(attempted).toBe(1); expect(forwarded).toBe(1)
        expect(mutations.length).toBe(1)
        expect(mutations.every((path) => path.endsWith(decisionPath))).toBe(true)
        expect(callbackFailed).toBe(false)
      } finally { await context.close() }
    })
  })
}
