<script lang="ts">
  import type { AuditEvent } from '../api/types'

  let { changes }: { changes: AuditEvent['changed_fields'] } = $props()
  const fields = $derived(Object.entries(changes ?? {}).sort(([left], [right]) => left.localeCompare(right)))

  function display(value: unknown): string {
    if (value === undefined) return '未提供'
    if (typeof value === 'string' && value.startsWith('enc:')) return '已加密（不在浏览器解密）'
    return JSON.stringify(value, null, 2)
  }
</script>

{#if fields.length}
  <h3>字段变化（账本记录，非执行结果）</h3>
  <div class="field-diff-table">
    <table class="native-table">
      <thead><tr><th>字段</th><th>Before</th><th>After</th></tr></thead>
      <tbody>
        {#each fields as [field, change] (field)}
          <tr><th scope="row">{field}</th><td><pre>{display(change.before)}</pre></td><td><pre>{display(change.after)}</pre></td></tr>
        {/each}
      </tbody>
    </table>
  </div>
{:else}
  <p class="muted-copy">该事件没有记录字段变化。</p>
{/if}
