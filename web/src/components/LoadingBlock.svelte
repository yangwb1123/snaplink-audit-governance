<script lang="ts">
  import { IrisSpinner } from '@iris-ui-kit/svelte'

  let { label = '正在读取 Audit API…' }: { label?: string } = $props()
</script>

<div class="loading-block" role="status">
  <IrisSpinner style="" />
  <span>{label}</span>
</div>
