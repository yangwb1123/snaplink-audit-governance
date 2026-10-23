import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { createServer } from 'node:http'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { request, type APIRequestContext } from '@playwright/test'
import { describe, expect, it, vi } from 'vitest'
import { fixtureCall } from './identity'

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..')
const identity = join(root, 'test/browser/identity.ts')
const playwright = join(root, 'node_modules/@playwright/test/index.mjs')
const cli = join(root, 'node_modules/@playwright/test/cli.js')
const safeMessage = 'Browser fixture failed; sensitive diagnostics withheld'

type Mode = 'disconnect' | 'timeout' | 'json' | 'status' | 'callback'
type Operation = 'source-read' | 'source-bind' | 'ingest' | 'login'

// Only this test's ephemeral loopback server is contacted. Synthetic secrets
// live in memory/child environment, never URLs, generated sources or reports.
async function loopback(operation: Operation, mode: Mode, body: string) {
  let requests = 0
  let credentialSeen = false
  const server = createServer((incoming, outgoing) => {
    requests += 1
    let received = ''
    incoming.on('data', (chunk) => { received += chunk.toString() })
    incoming.on('end', () => {
      credentialSeen ||= Boolean(incoming.headers.authorization) || received.includes('credential')
      const target = operation === 'source-bind' ? incoming.method === 'PUT' : true
      if (target && mode === 'disconnect') { incoming.socket.destroy(); return }
      if (target && mode === 'timeout') return
      outgoing.setHeader('content-type', 'application/json')
      if (target && mode === 'json') { outgoing.end(`invalid-json ${body}`); return }
      if (target && mode === 'status') { outgoing.statusCode = 503; outgoing.end(JSON.stringify({ error: body })); return }
      if (operation === 'login') outgoing.end(JSON.stringify({ code: 'synthetic-code', state: 'synthetic-state', iss: 'http://127.0.0.1' }))
      else outgoing.end(JSON.stringify({ items: [{ id: 'demo', name: body, active: true, allowed_client_ids: [] }] }))
    })
  })
  await new Promise<void>((done) => server.listen(0, '127.0.0.1', done))
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('Privacy fixture did not listen')
  return { url: `http://127.0.0.1:${address.port}`, count: () => requests, credentialSeen: () => credentialSeen,
    close: async () => { server.closeAllConnections(); await new Promise<void>((done) => server.close(() => done())) } }
}

async function captureChild(args: string[], env: NodeJS.ProcessEnv) {
  return new Promise<{ code: number | null; stdout: string; stderr: string }>((done, fail) => {
    const child = spawn(process.execPath, args, { cwd: root, env, stdio: ['ignore', 'pipe', 'pipe'] })
    let stdout = ''; let stderr = ''
    child.stdout.on('data', (chunk) => { stdout += chunk.toString() })
    child.stderr.on('data', (chunk) => { stderr += chunk.toString() })
    child.on('error', () => fail(new Error('Privacy fixture subprocess did not start')))
    child.on('close', (code) => done({ code, stdout, stderr }))
  })
}

// This is an actual failing Playwright test, not a handcrafted Error.toJSON.
// The parent requires exit 1 and inspects runner stdout/stderr AND JSON errors
// without ever echoing them (a regression must not leak its synthetic input).
async function reportedFailure(operation: Operation, mode: Mode) {
  const token = `synthetic-token-${randomUUID()}`
  const password = `synthetic-password-${randomUUID()}`
  const body = `synthetic-body-${randomUUID()}`
  const server = await loopback(operation, mode, body)
  const dir = await mkdtemp(join(tmpdir(), 'audit-fixture-privacy-'))
  try {
    await writeFile(join(dir, 'all-steps.cjs'), `module.exports = class {
      constructor() { this.steps = [] }
      onStepEnd(_test, _result, step) { this.steps.push({ title: step.title, error: step.error }) }
      onEnd() { process.stderr.write('PRIVACY_STEPS=' + JSON.stringify({ steps: this.steps }) + '\\n') }
    }`)
    await writeFile(join(dir, 'playwright.config.cjs'), `module.exports = {
      testDir: ${JSON.stringify(dir)}, testMatch: '*.spec.ts', workers: 1, retries: 0,
      reporter: [['json'], [${JSON.stringify(join(dir, 'all-steps.cjs'))}]], outputDir: ${JSON.stringify(join(dir, 'results'))},
      use: { trace: 'off', screenshot: 'off', video: 'off' }
    }`)
    await writeFile(join(dir, 'privacy.spec.ts'), `
      import { test, request } from ${JSON.stringify(playwright)}
      import { bindSource, fixtureCall, headers, loginWithHeading } from ${JSON.stringify(identity)}
      test('credential fixture failure stays a failure', async () => {
        const context = await request.newContext({ timeout: 100 })
        const token = process.env.PRIVACY_TOKEN
        const body = process.env.PRIVACY_BODY
        try {
          const operation = process.env.PRIVACY_OPERATION
          if (operation.startsWith('source-')) await bindSource({ request: context } as any, token)
          else if (operation === 'ingest') await fixtureCall(context, async () => {
            const response = await context.post(process.env.AUDIT_API_URL + '/api/v1/events?wait_for=ledgered', {
              headers: headers(token), data: { payload: body }
            })
            if (response.status() !== 200) throw new Error(body)
            await response.json()
          })
          else {
            let callback: any
            const page = {
              request: context,
              route: async (_pattern: string, handler: any) => { callback = handler },
              waitForResponse: () => new Promise(() => {}),
              goto: async () => {},
              getByRole: () => ({ click: async () => {
                // Match Playwright's detached route dispatch, not a callback
                // awaited by click (an outer catch alone cannot protect it).
                setImmediate(() => { void callback({
                  request: () => ({ url: () => 'http://127.0.0.1/login/?client_id=audit-governance-web&redirect_uri=http%3A%2F%2F127.0.0.1%2Fauth%2Fcallback&state=s&nonce=n&code_challenge=c' }),
                  fulfill: async () => { throw new Error(body) },
                  abort: async () => { throw new Error(body) }
                }) })
              } })
            }
            await loginWithHeading(page as any, 'synthetic-user', '#evidence', undefined, '证据与治理')
          }
        } finally { await context.dispose() }
      })
    `)
    const result = await captureChild([cli, 'test', '--config', join(dir, 'playwright.config.cjs')], {
      ...process.env, AUDIT_API_URL: server.url, SNAPLINK_IDP_URL: server.url,
      SNAPLINK_HOSTED_LOGIN_URL: `${server.url}/login/`, AUDIT_WEB_PASSWORD: password,
      PRIVACY_TOKEN: token, PRIVACY_BODY: body, PRIVACY_OPERATION: operation,
      PLAYWRIGHT_NO_COPY_PROMPT: '1', FORCE_COLOR: '0', DEBUG: '',
    })
    // Booleans only: assertion diagnostics cannot serialize raw credentials.
    expect(result.code).toBe(1)
    expect(server.count() > 0).toBe(true)
    expect(server.credentialSeen()).toBe(true)
    const output = result.stdout + result.stderr
    const forbidden = [token, password, body, 'synthetic-token-', 'synthetic-password-', 'synthetic-body-',
      'invalid-json', 'Call log:', 'Authorization: Bearer',
      ...[token, password, body].map((value) => Buffer.from(value).toString('base64'))]
    expect(forbidden.some((value) => output.includes(value))).toBe(false)
    type ReportResult = { status: string; error?: { message?: string; stack?: string; cause?: unknown }; errors: unknown[] }
    let report: { stats: { expected: number; unexpected: number; skipped: number }; suites: Array<{ specs: Array<{ tests: Array<{ results: ReportResult[] }> }> }> } | undefined
    try { report = JSON.parse(result.stdout) } catch { /* Assert without printing stdout. */ }
    expect(Boolean(report)).toBe(true)
    const results = report?.suites.flatMap((suite) => suite.specs.flatMap((spec) => spec.tests.flatMap((test) => test.results))) ?? []
    expect(results.length).toBe(1)
    expect(results.every((result) => result.status === 'failed' && result.errors.length === 1)).toBe(true)
    // JSON reporter's `errors` are formatted messages with source frames;
    // `error` is Playwright's actual serialized Error, including any cause.
    expect(results.every((result) => result.error?.message === `Error: ${safeMessage}` && !Object.hasOwn(result.error, 'cause'))).toBe(true)
    expect(JSON.stringify(results).includes(safeMessage)).toBe(true)
    expect(report?.stats).toMatchObject({ expected: 0, unexpected: 1, skipped: 0 })
    // Built-in JSON omits automatic pw:api steps. Serialize every public
    // reporter step too, like HTML/blob reports do, without a redacting reporter.
    let steps: { steps: Array<{ error?: { message?: string; cause?: unknown } }> } | undefined
    try { steps = JSON.parse(result.stderr.match(/^PRIVACY_STEPS=(.*)$/m)?.[1] ?? '') } catch { /* Do not echo captured diagnostics. */ }
    expect(Boolean(steps?.steps.length)).toBe(true)
    expect(steps?.steps.some((step) => step.error?.message?.includes(safeMessage))).toBe(true)
    expect(steps?.steps.every((step) => !step.error || !Object.hasOwn(step.error, 'cause'))).toBe(true)
    if (mode !== 'status') {
      expect(steps?.steps.every((step) => !step.error || step.error.message?.includes(safeMessage))).toBe(true)
    }
  } finally { await server.close(); await rm(dir, { recursive: true, force: true }) }
}

describe('credential-bearing Playwright fixture privacy', () => {
  it('fails closed before I/O if the pinned scoped error boundary is unavailable', async () => {
    const operation = vi.fn()
    await expect(fixtureCall({} as APIRequestContext, operation)).rejects.toThrow(safeMessage)
    expect(operation).not.toHaveBeenCalled()
  })

  it.each(['disconnect', 'timeout'] as const)('raw pinned Playwright %s deterministically exposes synthetic credentials in memory', async (mode) => {
    const token = `raw-control-${randomUUID()}`
    const body = `raw-body-${randomUUID()}`
    const server = await loopback('ingest', mode, body)
    const context = await request.newContext({ timeout: 100 })
    let diagnostic = ''
    try {
      await context.post(`${server.url}/api/v1/events`, { headers: { Authorization: `Bearer ${token}` }, data: { payload: body } })
    } catch (error) { diagnostic = String(error) }
    finally { await context.dispose(); await server.close() }
    expect(diagnostic.includes(token)).toBe(true)
    expect(diagnostic.includes('Call log:')).toBe(true)
  })

  for (const operation of ['source-read', 'source-bind', 'ingest', 'login'] as const) {
    it.each(['disconnect', 'timeout'] as const)(`${operation} %s: final error and actual runner report are redacted`, async (mode) => {
      await reportedFailure(operation, mode)
    })
  }
  it.each(['source-read', 'ingest', 'login'] as const)('%s JSON failure is redacted', async (operation) => {
    await reportedFailure(operation, 'json')
  })
  it.each(['source-read', 'source-bind', 'ingest', 'login'] as const)('%s HTTP failure is redacted and still fails', async (operation) => {
    await reportedFailure(operation, 'status')
  })
  it('login asynchronous route/abort failures cannot escape into the runner', async () => {
    await reportedFailure('login', 'callback')
  })
})
