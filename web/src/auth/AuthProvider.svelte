<script lang="ts">
  import { onMount, type Snippet } from 'svelte'
  import { bootstrapAuth } from './store'

  let { children }: { children?: Snippet } = $props()

  onMount(() => {
    void bootstrapAuth()
  })
</script>

{@render children?.()}
