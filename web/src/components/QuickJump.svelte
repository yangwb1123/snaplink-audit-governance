<script lang="ts">
  import { can } from '../auth/claims'
  import { authState } from '../auth/store'
  import { quickJumpHash, quickJumpPermissions, type QuickJumpKind } from '../pages/navigation'

  let { scopeReady, scopeKey }: { scopeReady: boolean; scopeKey: string } = $props()
  let kind = $state<QuickJumpKind | ''>('')
  let identifier = $state('')
  let error = $state('')
  const allowed = $derived(Boolean(kind && $authState.session && can($authState.session, quickJumpPermissions[kind])))
  // A new identity, token or tenant scope never inherits a pending jump draft.
  $effect(() => {
    void $authState.session
    void scopeReady
    void scopeKey
    kind = ''; identifier = ''; error = ''
  })

  function jump(event: SubmitEvent): void {
    event.preventDefault()
    if (!kind || !allowed || !scopeReady) return
    const hash = quickJumpHash(kind, identifier)
    if (!hash) { error = '请输入有效的 ID；不接受 URL、查询参数、分隔符或超长值。'; return }
    error = ''
    window.location.hash = hash
  }
</script>

<form class="quick-jump" aria-label="全局快速跳转" onsubmit={jump}>
  <select aria-label="快速跳转类型" value={kind} onchange={(event) => {
    const value = event.currentTarget.value
    kind = value === 'event' || value === 'operation' || value === 'export' ? value : ''
    error = ''
  }}>
    <option value="" disabled>选择跳转类型</option>
    <option value="event" disabled={!$authState.session || !can($authState.session, quickJumpPermissions.event)}>事件</option>
    <option value="operation" disabled={!$authState.session || !can($authState.session, quickJumpPermissions.operation)}>操作</option>
    <option value="export" disabled={!$authState.session || !can($authState.session, quickJumpPermissions.export)}>导出</option>
  </select>
  <input class="native-control" aria-label="快速跳转 ID" placeholder="输入所选类型的 ID" bind:value={identifier} disabled={!kind || !allowed || !scopeReady} />
  <button type="submit" class="link-button" disabled={!kind || !identifier || !allowed || !scopeReady}>跳转</button>
  {#if error}<small role="alert">{error}</small>{/if}
</form>
