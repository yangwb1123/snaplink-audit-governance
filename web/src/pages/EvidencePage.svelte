<script lang="ts">
  import { onMount } from 'svelte'
  import { IrisAlert, IrisButton } from '@iris-ui-kit/svelte'
  import { authState } from '../auth/store'
  import { can } from '../auth/claims'
  import PageHeader from '../components/PageHeader.svelte'
  import ExportsPanel from '../features/exports/ExportsPanel.svelte'
  import RestoresPanel from '../features/restores/RestoresPanel.svelte'
  import HoldsPanel from '../features/holds/HoldsPanel.svelte'
  import type { HoldCreateOwner } from '../features/holds/createOwner.svelte'
  import IntegrityPanel from '../features/integrity/IntegrityPanel.svelte'
  import { evidenceLocation, navigateEvidence, type EvidenceTab } from './evidenceNavigation'

  let { tenantId, holdCreateOwner }: { tenantId?: string; holdCreateOwner: HoldCreateOwner } = $props()
  const session = $derived($authState.session)
  const definitions: Array<{ key: EvidenceTab; title: string; permission: string }> = [
    { key: 'exports', title: '导出任务', permission: 'audit:export:create' },
    { key: 'restores', title: '恢复审批', permission: 'audit:operation:read' },
    { key: 'holds', title: '法律保留', permission: 'audit:legal_hold:manage' },
    { key: 'integrity', title: '完整性', permission: 'audit:integrity:verify' },
  ]
  const tabs = $derived(definitions.filter((tab) => session && can(session, tab.permission)))
  let location = $state(evidenceLocation(window.location.hash))
  const active = $derived(location.tab ?? tabs[0]?.key)
  const allowed = $derived(tabs.some((tab) => tab.key === active))
  const unscoped = $derived(!session || (session.platform ? !tenantId : !session.tenantId))

  onMount(() => {
    const update = () => (location = evidenceLocation(window.location.hash))
    window.addEventListener('hashchange', update)
    return () => window.removeEventListener('hashchange', update)
  })
</script>

<section>
  <PageHeader title="证据与治理" description="导出工作台、双人恢复审批、法律保留与完整性验证；每个子功能独立授权。" />
  <nav class="button-row section-card" aria-label="证据功能">
    {#each tabs as tab (tab.key)}<IrisButton variant={active === tab.key ? 'solid' : 'outline'} onclick={() => navigateEvidence(tab.key)}>{tab.title}</IrisButton>{/each}
  </nav>
  {#if unscoped}
    <IrisAlert tone="warning" title="请选择有效租户" style="" onclose={undefined} icon={undefined} titleSlot={undefined}>未确认租户前不会加载任务、证据或审批对象。</IrisAlert>
  {:else if !allowed}
    <IrisAlert tone="warning" title="当前会话没有该功能权限" style="" onclose={undefined} icon={undefined} titleSlot={undefined}>请选择已经授权的证据功能；页面导航不会替代服务端鉴权。</IrisAlert>
  {:else}
    {#key `${active}:${tenantId}:${session?.accessToken}`}
      {#if active === 'exports'}<ExportsPanel {tenantId} initialId={location.id} />
      {:else if active === 'restores'}<RestoresPanel {tenantId} initialId={location.id} />
      {:else if active === 'holds'}<HoldsPanel {tenantId} createOwner={holdCreateOwner} />
      {:else if active === 'integrity'}<IntegrityPanel {tenantId} />{/if}
    {/key}
  {/if}
</section>
