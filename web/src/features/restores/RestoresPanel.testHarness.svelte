<script lang="ts">
  import { IrisProvider, SkinProvider } from '@iris-ui-kit/svelte'
  import { skinEngine } from '../../skin'
  import RestoresPanel from './RestoresPanel.svelte'

  let tenantId = $state('tenant-a')
  let initialId = $state('restore-aaaaaaaaaaaaaaaa')
</script>

<SkinProvider engine={skinEngine}>
  <IrisProvider locale="zh-CN">
    <button onclick={() => (tenantId = 'tenant-a')}>租户 A</button>
    <button onclick={() => (tenantId = 'tenant-b')}>租户 B</button>
    <button onclick={() => (initialId = 'restore-aaaaaaaaaaaaaaaa')}>申请 A</button>
    <button onclick={() => (initialId = 'restore-bbbbbbbbbbbbbbbb')}>申请 B</button>
    <button onclick={() => (initialId = '')}>清除申请</button>
    <RestoresPanel {tenantId} {initialId} />
  </IrisProvider>
</SkinProvider>
