<script lang="ts">
  import { IrisProvider, SkinProvider } from '@iris-ui-kit/svelte'
  import Shell from './Shell.svelte'
  import { skinEngine } from './skin'
</script>

<SkinProvider engine={skinEngine}>
  <IrisProvider locale="zh-CN">
    <Shell />
  </IrisProvider>
</SkinProvider>
