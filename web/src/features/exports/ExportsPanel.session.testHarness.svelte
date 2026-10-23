<script lang="ts">
  import { IrisProvider, SkinProvider } from '@iris-ui-kit/svelte'
  import { authState } from '../../auth/store'
  import { can } from '../../auth/claims'
  import { skinEngine } from '../../skin'
  import ExportsPanel from './ExportsPanel.svelte'

  let initialId = $state('export-a')
</script>

<SkinProvider engine={skinEngine}>
  <IrisProvider locale="zh-CN">
    <button onclick={() => (initialId = 'export-b')}>任务 B</button>
    {#if $authState.status === 'authenticated' && $authState.session && can($authState.session, 'audit:export:create')}
      <ExportsPanel tenantId="tenant-a" {initialId} />
    {:else}
      <p role="status">当前会话不可用</p>
    {/if}
  </IrisProvider>
</SkinProvider>
