<script lang="ts">
  import { onDestroy, untrack } from 'svelte'
  import { IrisButton, IrisCard } from '@iris-ui-kit/svelte'
  import { authState } from '../../auth/store'
  import type { ExportJob } from '../../api/types'
  import { RequestScope } from '../../api/requestScope'
  import {
    EXPORT_DIGEST_LIMIT_LABEL,
    checkLocalExportDigest,
    exportDigestMessages,
    isExpectedExportDigest,
    type ExportDigestFailureReason,
    type ExportDigestOutcome,
  } from './exportDigest'

  let { job, tenantId, scopeReady }: {
    job: ExportJob | null
    tenantId: string | undefined
    scopeReady: boolean
  } = $props()

  const requests = new RequestScope()
  const auth = $derived($authState)
  let selected = $state<File | null>(null)
  let input = $state<HTMLInputElement | null>(null)
  let busy = $state(false)
  let outcome = $state<ExportDigestOutcome | null>(null)
  let cancelled = $state(false)

  const renderable = $derived(Boolean(
    scopeReady && tenantId && job?.status === 'completed' && job.tenant_id === tenantId &&
    auth.status === 'authenticated' && auth.session,
  ))
  const digestProblem = $derived(digestProblemFor(job?.digest))
  const announcement = $derived(
    !renderable ? '无法核对'
      : busy ? '本地核对中'
      : cancelled ? '已取消本地等待'
      : outcome?.status === 'match' ? '摘要一致'
      : outcome?.status === 'mismatch' ? '摘要不一致'
      : outcome?.status === 'unavailable' || digestProblem ? '无法核对'
      : '尚未核对',
  )
  const detail = $derived(
    !renderable ? '当前会话、租户或任务不可用于本地核对。'
      : digestProblem ? exportDigestMessages[digestProblem]
      : busy ? '只读取所选本地文件，不上传。'
      : cancelled ? '取消只放弃本页等待，不保证浏览器停止底层读取或摘要计算；旧结果即使稍后完成也不会更新本页。'
      : outcome?.status === 'unavailable' ? exportDigestMessages[outcome.reason]
      : outcome?.status === 'match' ? '所选原始字节的 SHA-256 与当前任务摘要一致；仅此本地比较。'
      : outcome?.status === 'mismatch' ? '所选原始字节的 SHA-256 与当前任务摘要不一致。'
      : selected ? '已选择本地文件，尚未核对。'
      : '请选择本地文件；未选择时不会读取任何文件。',
  )

  function digestProblemFor(value: unknown): ExportDigestFailureReason | null {
    if (value === undefined || value === null || value === '') return 'missing_digest'
    return isExpectedExportDigest(value) ? null : 'invalid_digest'
  }

  function scopeSnapshot() {
    return {
      job,
      jobId: job?.id,
      digest: job?.digest,
      status: job?.status,
      jobTenant: job?.tenant_id,
      tenantId,
      scopeReady,
      auth: $authState,
      sessionToken: $authState.session?.accessToken,
      sessionSubject: $authState.session?.subject,
      sessionTenant: $authState.session?.tenantId,
    }
  }

  function sameScope(left: ReturnType<typeof scopeSnapshot>, right: ReturnType<typeof scopeSnapshot>): boolean {
    return left.job === right.job && left.jobId === right.jobId && left.digest === right.digest &&
      left.status === right.status && left.jobTenant === right.jobTenant &&
      left.tenantId === right.tenantId && left.scopeReady === right.scopeReady &&
      left.auth === right.auth && left.sessionToken === right.sessionToken &&
      left.sessionSubject === right.sessionSubject && left.sessionTenant === right.sessionTenant
  }

  function reset(): void {
    requests.invalidate()
    selected = null
    busy = false
    outcome = null
    cancelled = false
    if (input) input.value = ''
  }

  let lastScope = untrack(scopeSnapshot)
  $effect(() => {
    const next = scopeSnapshot()
    untrack(() => {
      if (sameScope(lastScope, next)) return
      lastScope = next
      reset()
    })
  })
  // Auth notifications are synchronous; Svelte effects can batch an A-B-A
  // transition back to the exact original object. Invalidate the local hash
  // ticket at each notification, not only when the final binding differs.
  // This observer lives for this component only and never exposes credentials.
  let observedInitialAuth = false
  const unsubscribeAuth = authState.subscribe(() => {
    if (observedInitialAuth) untrack(reset)
    observedInitialAuth = true
  })
  onDestroy(() => { unsubscribeAuth(); requests.dispose() })

  function choose(control: HTMLInputElement): void {
    const file = control.files?.[0] ?? null
    control.value = ''
    if (!file || !renderable || digestProblem) return
    requests.invalidate()
    selected = file
    busy = false
    outcome = null
    cancelled = false
  }

  async function check(): Promise<void> {
    const file = selected
    if (!file || busy || !renderable || digestProblem) return
    const ticket = requests.next()
    const scope = scopeSnapshot()
    const expected = job?.digest
    busy = true
    outcome = null
    cancelled = false
    try {
      const next = await checkLocalExportDigest(file, expected)
      if (!ticket.current() || !sameScope(scope, scopeSnapshot())) return
      outcome = next
    } catch {
      // The helper maps browser failures to fixed outcomes; this guard only
      // prevents an unexpected implementation error from reaching the UI.
      if (ticket.current() && sameScope(scope, scopeSnapshot())) {
        outcome = { status: 'unavailable', reason: 'hash_failed' }
      }
    } finally {
      if (ticket.current() && sameScope(scope, scopeSnapshot())) busy = false
      ticket.finish()
    }
  }

  function cancel(): void {
    if (!busy) return
    // This abandons the local wait only. It cannot stop browser file I/O or a
    // digest already being computed, but stale success/failure/finally cannot
    // publish into this page.
    requests.invalidate()
    selected = null
    busy = false
    outcome = null
    cancelled = true
    if (input) input.value = ''
  }
</script>

{#if renderable}
  <IrisCard variant="outline" class="digest-card" style="" header={undefined} footer={undefined}>
    <h3 class="card-heading">导出文件本地摘要核对</h3>
    <p class="card-copy">仅核对所选本地文件的原始字节与当前观察任务摘要的 SHA-256 是否一致。不是内容来源、签名、全链、所有业务事实完整性或当前 Hold/下载授权证明。</p>
    <p class="muted-copy">文件不会上传、不解析 JSONL、不改写换行/BOM，也不把文件名或内容写入 URL 或浏览器存储；仅在本页内存中读取。本地核对上限 {EXPORT_DIGEST_LIMIT_LABEL}，超限或缺/坏摘要会在读取前拒绝；空文件是合法字节输入。</p>
    <div class="inline-form" aria-busy={busy}>
      <label class="digest-file-label">选择本地文件
        <input bind:this={input} type="file" aria-label="选择本地文件" disabled={busy || Boolean(digestProblem)} onchange={(event) => choose(event.currentTarget)} />
      </label>
      <IrisButton variant="solid" disabled={!selected || busy || Boolean(digestProblem)} onclick={() => void check()}>{busy ? '本地核对中…' : '核对本地文件'}</IrisButton>
      {#if busy}<IrisButton variant="outline" onclick={cancel}>取消本地等待</IrisButton>{/if}
    </div>
    <p class="muted-copy" role="status" aria-live="polite" aria-atomic="true"><strong>{announcement}</strong></p>
    <p class="muted-copy">{detail}</p>
  </IrisCard>
{/if}
