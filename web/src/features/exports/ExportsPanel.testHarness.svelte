<script lang="ts">
  import { IrisProvider, SkinProvider } from '@iris-ui-kit/svelte'
  import { skinEngine } from '../../skin'
  import ExportsPanel from './ExportsPanel.svelte'

  let tenantId = $state('tenant-a')
  let initialId = $state('export-a')
</script>

<SkinProvider engine={skinEngine}>
  <IrisProvider locale="zh-CN">
    <button onclick={() => (tenantId = 'tenant-b')}>租户 B</button>
    <button onclick={() => (tenantId = 'tenant-a')}>租户 A</button>
    <button onclick={() => (initialId = 'export-b')}>任务 B</button>
    <button onclick={() => (initialId = 'export-a')}>任务 A</button>
    <ExportsPanel {tenantId} {initialId} />
  </IrisProvider>
</SkinProvider>
