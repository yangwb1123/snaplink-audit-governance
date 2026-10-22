import { expect, test } from '@playwright/test'
import { audit, loginWithHeading } from './identity'

// Runs against the real local Aero platform stack: Snaplink Hosted Login
// (password provider), audit-governance built from this checkout, and the
// tenant/source/schema contract created by the platform audit bootstrap.
test('platform: Snaplink login, Aero catalog readiness and ledger facts', async ({ page }) => {
  const token = await loginWithHeading(
    page,
    'admin',
    '#catalog',
    [
      'openid',
      'profile',
      'audit:event:read',
      'audit:operation:read',
      'audit:export:create',
      'audit:integrity:verify',
      'audit:legal_hold:manage',
      'audit:policy:read',
      'audit:policy:write',
      'audit:platform:cross_tenant',
    ],
    '治理目录',
  )

  const leaked = await page.evaluate(
    (secret) => [localStorage, sessionStorage].some((storage) =>
      Object.values(storage).some((value) => String(value).includes(secret)),
    ),
    token,
  )
  expect(leaked).toBe(false)

  await expect(page.getByText('平台审计接入状态').first()).toBeVisible()
  for (const service of ['账户系统 · Aero ID', '消息通知 · Aero IM', '文件服务 · Aero Vault']) {
    await expect(page.getByText(service, { exact: false }).first()).toBeVisible()
  }
  await expect(page.getByText('valid', { exact: true }).first()).toBeVisible()

  const listing = await page.request.get(
    `${audit}/api/v1/events?from=2000-01-01T00:00:00Z&to=2300-01-01T00:00:00Z&tenant_id=platform-local&page_size=5`,
    { headers: { Authorization: `Bearer ${token}` } },
  )
  expect(listing.status()).toBe(200)
  const eventId = (await listing.json() as { items: Array<{ event_id: string }> }).items[0]?.event_id
  expect(eventId).toBeTruthy()

  // Direct, allowlisted event link: it must resolve without relying on the
  // current list page containing the event.
  await page.evaluate((id) => {
    window.location.hash = `#events/event/${encodeURIComponent(id)}`
  }, eventId!)
  await expect(page.getByText('审计事件详情', { exact: true })).toBeVisible()
  await expect(page.getByText('Schema ID (schema_id)', { exact: true })).toBeVisible()
  await expect(page.getByText('回执状态', { exact: true })).toBeVisible()
  await expect(page.getByText('archived', { exact: true }).first()).toBeVisible()
})
