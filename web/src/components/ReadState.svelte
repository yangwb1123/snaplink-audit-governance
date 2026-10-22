<script lang="ts">
  import type { Snippet } from 'svelte'
  import { IrisButton } from '@iris-ui-kit/svelte'
  import ErrorNotice from './ErrorNotice.svelte'
  import LoadingBlock from './LoadingBlock.svelte'

  let { loading, error = null, empty = false, loadingLabel = '正在读取 Audit API…', emptyLabel = '暂无数据', onRetry, children }: {
    loading: boolean
    error?: unknown
    empty?: boolean
    loadingLabel?: string
    emptyLabel?: string
    onRetry: () => void
    children?: Snippet
  } = $props()
</script>

{#if loading}
  <LoadingBlock label={loadingLabel} />
{:else if error}
  <ErrorNotice {error} {onRetry} />
{:else if empty}
  <div class="read-empty" role="status"><p class="muted-copy">{emptyLabel}</p><IrisButton size="sm" variant="outline" onclick={onRetry}>刷新</IrisButton></div>
{:else if children}
  {@render children()}
{/if}
