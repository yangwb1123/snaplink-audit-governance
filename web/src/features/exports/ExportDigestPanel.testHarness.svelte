<script lang="ts">
  import { IrisProvider, SkinProvider } from '@iris-ui-kit/svelte'
  import { skinEngine } from '../../skin'
  import type { ExportJob } from '../../api/types'
  import ExportDigestPanel from './ExportDigestPanel.svelte'

  const ABC = 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad'
  const ABC_LF = 'edeaaff3f1774ad2888673770c6d64097e391bc362d7d6fb34982ddf0efd18cb'

  let job = $state<ExportJob | null>({
    id: 'export-a',
    tenant_id: 'tenant-a',
    requested_by: 'alice',
    status: 'completed',
    created_at: '2026-10-14T00:00:00Z',
    query: { from: '2026-10-13T00:00:00Z', to: '2026-10-14T00:00:00Z' },
    event_count: 17,
    digest: ABC,
  })
  let tenantId = $state('tenant-a')
  let scopeReady = $state(true)

  function update(patch: Partial<ExportJob>): void {
    if (job) job = { ...job, ...patch }
  }
</script>

<SkinProvider engine={skinEngine}>
  <IrisProvider locale="zh-CN">
    <button onclick={() => update({ id: 'export-b' })}>任务 B</button>
    <button onclick={() => update({ id: 'export-a' })}>任务 A</button>
    <button onclick={() => update({ digest: ABC_LF })}>摘要变更</button>
    <button onclick={() => update({ digest: undefined })}>摘要缺失</button>
    <button onclick={() => update({ digest: 'not-a-digest' })}>摘要无效</button>
    <button onclick={() => update({ status: 'running' })}>运行中</button>
    <button onclick={() => (scopeReady = false)}>范围不可用</button>
    <button onclick={() => (scopeReady = true)}>范围可用</button>
    <button onclick={() => { tenantId = 'tenant-b'; update({ tenant_id: 'tenant-b' }) }}>租户 B</button>
    <button onclick={() => { tenantId = 'tenant-a'; update({ tenant_id: 'tenant-a' }) }}>租户 A</button>
    <button onclick={() => (job = null)}>无任务</button>
    <ExportDigestPanel {job} {tenantId} {scopeReady} />
  </IrisProvider>
</SkinProvider>
