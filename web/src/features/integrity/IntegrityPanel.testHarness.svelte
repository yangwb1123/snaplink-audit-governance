<script lang="ts">
  import { IrisProvider, SkinProvider } from '@iris-ui-kit/svelte'
  import { skinEngine } from '../../skin'
  import IntegrityPanel from './IntegrityPanel.svelte'
  let tenantId = $state('tenant-a')
</script>

<SkinProvider engine={skinEngine}>
  <IrisProvider locale="zh-CN">
    <button onclick={() => (tenantId = 'tenant-b')}>选择 B</button>
    <button onclick={() => (tenantId = 'tenant-a')}>选择 A</button>
    <IntegrityPanel {tenantId} />
  </IrisProvider>
</SkinProvider>
