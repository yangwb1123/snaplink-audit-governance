<script lang="ts">
  import { IrisDateRangePicker, IrisFormField, IrisInput } from '@iris-ui-kit/svelte'
  import type { EvidenceScopeDraft } from './evidenceScope'

  export interface DateRange {
    start: Date | null
    end: Date | null
  }

  let {
    range,
    scope,
    disabled,
    onRangeChange,
    onScopeChange,
  }: {
    range: DateRange
    scope: EvidenceScopeDraft
    disabled: boolean
    onRangeChange: (range: DateRange) => void
    onScopeChange: (field: keyof EvidenceScopeDraft, value: string) => void
  } = $props()

  const fields: Array<{ key: keyof EvidenceScopeDraft; label: string; placeholder: string }> = [
    { key: 'sourceSystem', label: '来源系统', placeholder: '例如 aero-im.source' },
    { key: 'eventType', label: '事件类型', placeholder: '例如 aero.im.security' },
    { key: 'operationId', label: 'Operation ID', placeholder: '业务操作标识' },
    { key: 'actorId', label: 'Actor ID', placeholder: '用户或服务主体' },
    { key: 'correlationId', label: 'Correlation ID', placeholder: '端到端关联标识' },
    { key: 'outcome', label: '结果', placeholder: 'success / failed' },
  ]

  function onDateRangeChange(value: { start: Date | null; end: Date | null }): void {
    onRangeChange(value)
  }
</script>

<div class="evidence-scope-form">
  <IrisFormField label="时间范围">
    <IrisDateRangePicker value={range} disabled={disabled} onValueChange={onDateRangeChange} />
  </IrisFormField>
  {#each fields as field (field.key)}
    <IrisFormField label={field.label}>
      <IrisInput
        value={scope[field.key]}
        placeholder={field.placeholder}
        disabled={disabled}
        oninput={(event) => onScopeChange(field.key, event.currentTarget.value)}
      />
    </IrisFormField>
  {/each}
</div>
