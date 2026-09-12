<script lang="ts">
  import type { Snippet } from 'svelte'

  let {
    title,
    description,
    actions,
  }: { title: string; description: string; actions?: Snippet } = $props()
</script>

<div class="page-header">
  <div>
    <h1 class="page-title">{title}</h1>
    <p class="page-description">{description}</p>
  </div>
  {#if actions}
    <div class="page-actions">{@render actions()}</div>
  {/if}
</div>
