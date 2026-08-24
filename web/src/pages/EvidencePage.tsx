import * as React from 'react'
import {
  IrisAlert,
  IrisButton,
  IrisCard,
  IrisDateRangePicker,
  IrisDescriptions,
  IrisFormField,
  IrisInput,
  IrisProgress,
  IrisTable,
  type IrisDateRange,
  type IrisTableColumn,
} from '@iris-ui-kit/react'
import { useAuditApi } from '../api/useAuditApi'
import type {
  ExportJob,
  IntegrityResult,
  LegalHold,
  RestoreRun,
} from '../api/types'
import { can } from '../auth/claims'
import { useAuth } from '../auth/AuthProvider'
import { ConfirmDialog } from '../components/ConfirmDialog'
import { ErrorNotice, PageHeader, formatDate } from '../components/Page'
import { StatusBadge } from '../components/StatusBadge'
import {
  buildEvidenceScopeQuery,
  defaultEvidenceRange,
  emptyEvidenceScope,
  evidenceScopeSummary,
  type EvidenceScopeDraft,
} from './evidenceScope'
import { restoreDecisionBlockReason, shouldPollExport } from './governanceState'
import {
  bindRestorePreview,
  restorePreviewBlockReason,
  type BoundRestorePreview,
} from './restoreWorkflow'

const holdColumns: IrisTableColumn<LegalHold>[] = [
  { key: 'name', title: '保留名称', minWidth: 160 },
  { key: 'reason', title: '原因', minWidth: 220 },
  { key: 'created_by', title: '创建者', width: 140 },
  {
    key: 'created_at',
    title: '创建时间',
    width: 180,
    render: (value) => formatDate(String(value)),
  },
  {
    key: 'released_at',
    title: '状态',
    width: 110,
    render: (_value, row) => <StatusBadge value={row.released_at ? 'released' : 'active'} />,
  },
  {
    key: 'filter',
    title: '保留范围',
    minWidth: 320,
    render: (_value, row) => evidenceScopeSummary(row.filter),
  },
]

// Each conditional is a direct rendering of a server-enforced permission.
// eslint-disable-next-line complexity
export function EvidencePage({ tenantId }: { tenantId?: string }) {
  const { session } = useAuth()
  const integrity = Boolean(session && can(session, 'audit:integrity:verify'))
  const holds = Boolean(session && can(session, 'audit:legal_hold:manage'))
  const exports = Boolean(session && can(session, 'audit:export:create'))
  return (
    <section>
      <PageHeader
        title="证据与治理"
        description="完整性证明、法律保留、合规导出和双人审批恢复共用同一租户边界。"
      />
      {session?.platform && !tenantId ? (
        <IrisAlert tone="warning" title="请选择租户">
          创建导出、法律保留或恢复操作前，需要在顶栏选择明确的 tenant_id。
        </IrisAlert>
      ) : null}
      <div className="evidence-grid">
        {integrity ? (
          <IntegrityCard tenantId={tenantId} disabled={Boolean(session?.platform && !tenantId)} />
        ) : null}
        {exports ? (
          <ExportCard tenantId={tenantId} disabled={Boolean(session?.platform && !tenantId)} />
        ) : null}
        {holds ? (
          <LegalHoldCard tenantId={tenantId} disabled={Boolean(session?.platform && !tenantId)} />
        ) : null}
        {session && can(session, 'audit:operation:read') ? (
          <RestoreCard
            tenantId={tenantId}
            canDecide={holds}
            actorSubject={session.subject}
            disabled={Boolean(session.platform && !tenantId)}
          />
        ) : null}
      </div>
    </section>
  )
}

function IntegrityCard({ tenantId, disabled }: { tenantId?: string; disabled: boolean }) {
  const api = useAuditApi()
  const [streamId, setStreamId] = React.useState('')
  const [result, setResult] = React.useState<IntegrityResult | null>(null)
  const [error, setError] = React.useState<unknown>(null)
  const [loading, setLoading] = React.useState(false)
  const verify = async () => {
    setLoading(true)
    setError(null)
    try {
      setResult(await api.verifyIntegrity(streamId.trim(), tenantId))
    } catch (cause) {
      setError(cause)
    } finally {
      setLoading(false)
    }
  }
  return (
    <IrisCard variant="outline" header="账本完整性验证">
      <p className="card-copy">留空验证整个租户，或填写 stream_id 验证一条哈希链。</p>
      <div className="stack-form">
        <IrisFormField label="Stream ID">
          <IrisInput
            value={streamId}
            onChange={(event) => setStreamId(event.target.value)}
            placeholder="可选"
          />
        </IrisFormField>
        <IrisButton variant="solid" disabled={disabled || loading} onClick={() => void verify()}>
          {loading ? '验证中…' : '开始验证'}
        </IrisButton>
      </div>
      <ErrorNotice error={error} />
      {result ? (
        <div className="result-panel">
          <StatusBadge value={result.valid} />
          <span>
            事件 {result.event_count ?? 0} · 分段 {result.segment_count ?? 0}
          </span>
          {result.errors?.length ? (
            <ul>
              {result.errors.map((value) => (
                <li key={value}>{value}</li>
              ))}
            </ul>
          ) : null}
        </div>
      ) : null}
    </IrisCard>
  )
}

function ExportCard({ tenantId, disabled }: { tenantId?: string; disabled: boolean }) {
  const api = useAuditApi()
  const [range, setRange] = React.useState<IrisDateRange>(() => defaultEvidenceRange(1))
  const [scope, setScope] = React.useState<EvidenceScopeDraft>(emptyEvidenceScope)
  const [jobId, setJobId] = React.useState('')
  const [job, setJob] = React.useState<ExportJob | null>(null)
  const [error, setError] = React.useState<unknown>(null)
  const [busy, setBusy] = React.useState(false)

  React.useEffect(() => {
    if (!job || !shouldPollExport(job.status)) return
    let active = true
    let timer: number | undefined
    const poll = async () => {
      try {
        const next = await api.getExport(job.id, tenantId)
        if (!active) return
        setJob(next)
        if (shouldPollExport(next.status)) timer = window.setTimeout(() => void poll(), 2000)
      } catch (cause) {
        if (active) setError(cause)
      }
    }
    timer = window.setTimeout(() => void poll(), 1200)
    return () => {
      active = false
      if (timer !== undefined) window.clearTimeout(timer)
    }
  }, [api, job?.id, job?.status, tenantId])

  React.useEffect(() => {
    setRange(defaultEvidenceRange(1))
    setScope(emptyEvidenceScope)
    setJob(null)
    setJobId('')
    setError(null)
  }, [tenantId])

  const create = async () => {
    const query = buildEvidenceScopeQuery(range, scope)
    if (!query) {
      setError(new Error('请选择有效且非空的导出时间范围'))
      return
    }
    setBusy(true)
    setError(null)
    try {
      const next = await api.createExport(query, tenantId)
      setJob(next)
      setJobId(next.id)
    } catch (cause) {
      setError(cause)
    } finally {
      setBusy(false)
    }
  }
  const lookup = async () => {
    if (!jobId.trim()) return
    setBusy(true)
    setError(null)
    try {
      setJob(await api.getExport(jobId.trim(), tenantId))
    } catch (cause) {
      setError(cause)
    } finally {
      setBusy(false)
    }
  }
  const download = async () => {
    if (!job) return
    setError(null)
    try {
      const blob = await api.downloadExport(job.id, tenantId)
      const url = URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = `${job.id}.jsonl`
      link.click()
      window.setTimeout(() => URL.revokeObjectURL(url), 1000)
    } catch (cause) {
      setError(cause)
    }
  }
  return (
    <IrisCard variant="outline" header="合规导出" className="wide-card">
      <p className="card-copy">
        按时间与跨服务关联条件创建封存 JSONL；完成后服务端在下载前再次校验摘要和法律保留。
      </p>
      <div className="stack-form">
        <EvidenceScopeFields
          range={range}
          scope={scope}
          disabled={disabled || busy}
          onRangeChange={setRange}
          onScopeChange={(name, value) =>
            setScope((current) => ({ ...current, [name]: value }))
          }
        />
        <IrisButton variant="solid" disabled={disabled || busy} onClick={() => void create()}>
          {busy ? '提交中…' : '创建范围导出'}
        </IrisButton>
        <div className="inline-form">
          <IrisInput
            value={jobId}
            onChange={(event) => setJobId(event.target.value)}
            placeholder="export job id"
          />
          <IrisButton variant="outline" disabled={busy || !jobId.trim()} onClick={() => void lookup()}>
            查询
          </IrisButton>
        </div>
      </div>
      <ErrorNotice error={error} />
      {job ? (
        <div className="result-panel">
          <StatusBadge value={job.status} />
          <span>
            {job.id} · {job.event_count} 条事件
          </span>
          {job.status === 'completed' ? (
            <IrisButton size="sm" variant="outline" onClick={() => void download()}>
              下载 JSONL
            </IrisButton>
          ) : null}
          {shouldPollExport(job.status) ? <IrisProgress indeterminate size="sm" /> : null}
          {job.error ? <span className="error-copy">{job.error}</span> : null}
        </div>
      ) : null}
    </IrisCard>
  )
}

function LegalHoldCard({ tenantId, disabled }: { tenantId?: string; disabled: boolean }) {
  const api = useAuditApi()
  const [range, setRange] = React.useState<IrisDateRange>(() => defaultEvidenceRange(30))
  const [scope, setScope] = React.useState<EvidenceScopeDraft>(emptyEvidenceScope)
  const [items, setItems] = React.useState<LegalHold[]>([])
  const [name, setName] = React.useState('')
  const [reason, setReason] = React.useState('')
  const [error, setError] = React.useState<unknown>(null)
  const [pendingRelease, setPendingRelease] = React.useState<LegalHold | null>(null)
  const [busy, setBusy] = React.useState(false)
  const requestSequence = React.useRef(0)
  const load = React.useCallback(async () => {
    const sequence = ++requestSequence.current
    if (disabled) {
      setItems([])
      return
    }
    try {
      const result = await api.listLegalHolds(tenantId)
      if (sequence === requestSequence.current) setItems(result.items)
    } catch (cause) {
      if (sequence === requestSequence.current) setError(cause)
    }
  }, [api, disabled, tenantId])
  React.useEffect(() => {
    void load()
    return () => {
      requestSequence.current += 1
    }
  }, [load])
  React.useEffect(() => {
    setItems([])
    setName('')
    setReason('')
    setRange(defaultEvidenceRange(30))
    setScope(emptyEvidenceScope)
    setPendingRelease(null)
    setError(null)
  }, [tenantId])
  const create = async () => {
    const query = buildEvidenceScopeQuery(range, scope)
    if (!query) {
      setError(new Error('请选择有效且非空的法律保留时间范围'))
      return
    }
    setBusy(true)
    setError(null)
    try {
      await api.createLegalHold(
        { name: name.trim(), reason: reason.trim(), filter: query },
        tenantId,
      )
      setName('')
      setReason('')
      await load()
    } catch (cause) {
      setError(cause)
    } finally {
      setBusy(false)
    }
  }
  const release = async () => {
    if (!pendingRelease) return
    setBusy(true)
    setError(null)
    try {
      await api.releaseLegalHold(pendingRelease.id, tenantId)
      setPendingRelease(null)
      await load()
    } catch (cause) {
      setError(cause)
    } finally {
      setBusy(false)
    }
  }
  const columns = React.useMemo<IrisTableColumn<LegalHold>[]>(
    () => [
      ...holdColumns,
      {
        key: 'action',
        title: '操作',
        width: 90,
        render: (_value, row) =>
          row.released_at ? (
            '—'
          ) : (
            <IrisButton size="sm" variant="outline" onClick={() => setPendingRelease(row)}>
              释放
            </IrisButton>
          ),
      },
    ],
    [],
  )
  return (
    <IrisCard variant="outline" header="法律保留" className="wide-card">
      <div className="three-field-form">
        <IrisFormField label="名称">
          <IrisInput value={name} onChange={(event) => setName(event.target.value)} />
        </IrisFormField>
        <IrisFormField label="原因">
          <IrisInput value={reason} onChange={(event) => setReason(event.target.value)} />
        </IrisFormField>
        <IrisButton
          variant="solid"
          disabled={disabled || busy || !name.trim() || !reason.trim()}
          onClick={() => void create()}
        >
          {busy ? '处理中…' : '创建范围保留'}
        </IrisButton>
      </div>
      <EvidenceScopeFields
        range={range}
        scope={scope}
        disabled={disabled || busy}
        onRangeChange={setRange}
        onScopeChange={(field, value) =>
          setScope((current) => ({ ...current, [field]: value }))
        }
      />
      <ErrorNotice error={error} />
      <IrisTable rowKey="id" columns={columns} data={items} size="small" />
      <ConfirmDialog
        open={pendingRelease !== null}
        title="确认释放法律保留"
        description={`释放 ${pendingRelease?.name ?? ''} 后，新归档评估将不再受该保留保护；操作会写入治理自审计。`}
        confirmLabel="确认释放"
        busy={busy}
        onOpenChange={(open) => !open && setPendingRelease(null)}
        onConfirm={release}
      />
    </IrisCard>
  )
}

function EvidenceScopeFields({
  range,
  scope,
  disabled,
  onRangeChange,
  onScopeChange,
}: {
  range: IrisDateRange
  scope: EvidenceScopeDraft
  disabled: boolean
  onRangeChange: (range: IrisDateRange) => void
  onScopeChange: (field: keyof EvidenceScopeDraft, value: string) => void
}) {
  const fields: Array<{
    key: keyof EvidenceScopeDraft
    label: string
    placeholder: string
  }> = [
    { key: 'sourceSystem', label: '来源系统', placeholder: '例如 aero-im.source' },
    { key: 'eventType', label: '事件类型', placeholder: '例如 aero.im.security' },
    { key: 'operationId', label: 'Operation ID', placeholder: '业务操作标识' },
    { key: 'actorId', label: 'Actor ID', placeholder: '用户或服务主体' },
    { key: 'correlationId', label: 'Correlation ID', placeholder: '端到端关联标识' },
    { key: 'outcome', label: '结果', placeholder: 'success / failed' },
  ]
  return (
    <div className="evidence-scope-form">
      <IrisFormField label="时间范围">
        <IrisDateRangePicker
          value={range}
          disabled={disabled}
          onValueChange={onRangeChange}
        />
      </IrisFormField>
      {fields.map((field) => (
        <IrisFormField key={field.key} label={field.label}>
          <IrisInput
            value={scope[field.key]}
            placeholder={field.placeholder}
            disabled={disabled}
            onChange={(event) => onScopeChange(field.key, event.target.value)}
          />
        </IrisFormField>
      ))}
    </div>
  )
}

// This component directly renders the server's pending/approved/rejected
// state machine plus the requester/approver separation gate.
// eslint-disable-next-line complexity
function RestoreCard({
  tenantId,
  canDecide,
  actorSubject,
  disabled,
}: {
  tenantId?: string
  canDecide: boolean
  actorSubject: string
  disabled: boolean
}) {
  const api = useAuditApi()
  const [operationId, setOperationId] = React.useState('')
  const [reason, setReason] = React.useState('')
  const [runId, setRunId] = React.useState('')
  const [boundPreview, setBoundPreview] = React.useState<BoundRestorePreview | null>(null)
  const [run, setRun] = React.useState<RestoreRun | null>(null)
  const [error, setError] = React.useState<unknown>(null)
  const [createConfirmation, setCreateConfirmation] = React.useState(false)
  const [decision, setDecision] = React.useState<'approve' | 'reject' | null>(null)
  const [busy, setBusy] = React.useState(false)
  const requestSequence = React.useRef(0)
  const request = () => ({ operation_id: operationId.trim(), reason: reason.trim() })
  const preview = boundPreview?.preview ?? null
  const previewBlockReason = restorePreviewBlockReason(boundPreview, request(), tenantId)
  const decisionBlockReason = run
    ? restoreDecisionBlockReason(run, actorSubject, canDecide)
    : null

  React.useEffect(() => {
    requestSequence.current += 1
    setOperationId('')
    setReason('')
    setBoundPreview(null)
    setRun(null)
    setRunId('')
    setError(null)
    setCreateConfirmation(false)
    setDecision(null)
    setBusy(false)
  }, [tenantId])

  const previewRun = async () => {
    const currentRequest = request()
    const sequence = ++requestSequence.current
    setBusy(true)
    setError(null)
    try {
      const next = await api.previewRestore(currentRequest, tenantId)
      if (sequence === requestSequence.current) {
        setBoundPreview(bindRestorePreview(next, currentRequest))
      }
    } catch (cause) {
      if (sequence === requestSequence.current) setError(cause)
    } finally {
      if (sequence === requestSequence.current) setBusy(false)
    }
  }
  const create = async () => {
    const currentRequest = request()
    const blockReason = restorePreviewBlockReason(boundPreview, currentRequest, tenantId)
    if (blockReason) {
      setCreateConfirmation(false)
      setError(new Error(blockReason))
      return
    }
    const sequence = ++requestSequence.current
    setBusy(true)
    setError(null)
    try {
      const next = await api.createRestore(currentRequest, tenantId)
      if (sequence === requestSequence.current) {
        setRun(next)
        setRunId(next.id)
        setBoundPreview(null)
        setCreateConfirmation(false)
      }
    } catch (cause) {
      if (sequence === requestSequence.current) setError(cause)
    } finally {
      if (sequence === requestSequence.current) setBusy(false)
    }
  }
  const lookup = async () => {
    if (!runId.trim()) return
    const sequence = ++requestSequence.current
    setBusy(true)
    setError(null)
    try {
      const next = await api.getRestore(runId.trim(), tenantId)
      if (sequence === requestSequence.current) {
        setRun(next)
        setBoundPreview(null)
      }
    } catch (cause) {
      if (sequence === requestSequence.current) setError(cause)
    } finally {
      if (sequence === requestSequence.current) setBusy(false)
    }
  }
  const decide = async () => {
    if (!run || !decision || decisionBlockReason) return
    const sequence = ++requestSequence.current
    setBusy(true)
    setError(null)
    try {
      const next = await api.decideRestore(run.id, decision, tenantId)
      if (sequence === requestSequence.current) {
        setRun(next)
        setDecision(null)
      }
    } catch (cause) {
      if (sequence === requestSequence.current) setError(cause)
    } finally {
      if (sequence === requestSequence.current) setBusy(false)
    }
  }
  return (
    <IrisCard variant="outline" header="恢复预演与审批" className="wide-card">
      <div className="three-field-form">
        <IrisFormField label="Operation ID">
          <IrisInput value={operationId} onChange={(event) => setOperationId(event.target.value)} />
        </IrisFormField>
        <IrisFormField label="恢复原因">
          <IrisInput value={reason} onChange={(event) => setReason(event.target.value)} />
        </IrisFormField>
        <div className="button-row">
          <IrisButton
            variant="outline"
            disabled={disabled || busy || !operationId.trim() || !reason.trim()}
            onClick={() => void previewRun()}
          >
            预演
          </IrisButton>
          {canDecide ? (
            <IrisButton
              variant="solid"
              disabled={disabled || busy || Boolean(previewBlockReason)}
              onClick={() => setCreateConfirmation(true)}
            >
              创建审批单
            </IrisButton>
          ) : null}
        </div>
      </div>
      <div className="inline-form">
        <IrisInput
          value={runId}
          onChange={(event) => setRunId(event.target.value)}
          placeholder="restore run id"
        />
        <IrisButton variant="outline" disabled={busy || !runId.trim()} onClick={() => void lookup()}>
          查询运行
        </IrisButton>
      </div>
      <ErrorNotice error={error} />
      {canDecide && operationId.trim() && reason.trim() ? (
        previewBlockReason ? (
          <IrisAlert tone="warning" title="恢复预演门禁">
            {previewBlockReason}
          </IrisAlert>
        ) : (
          <IrisAlert tone="success" title="当前参数预演有效">
            创建审批单时服务端仍会重新回放操作，并进入双人审批流程。
          </IrisAlert>
        )
      ) : null}
      {preview ? (
        <div className="restore-preview">
          <IrisDescriptions
            bordered
            columns={2}
            items={[
              { label: 'Preview ID', value: preview.id },
              { label: 'Operation ID', value: preview.operation_id },
              { label: '租户', value: preview.tenant_id },
              { label: '需要审批', value: preview.requires_approval ? '是' : '否' },
              {
                label: '外部调用声明',
                value: preview.external_calls.length
                  ? preview.external_calls.join('、')
                  : '无（当前预演只产生恢复提案）',
              },
              { label: '状态字段数', value: Object.keys(preview.proposed_state).length },
            ]}
          />
          <h3>拟恢复状态</h3>
          <pre className="json-view">{JSON.stringify(preview.proposed_state, null, 2)}</pre>
        </div>
      ) : null}
      {run?.status === 'pending_approval' && decisionBlockReason ? (
        <IrisAlert tone="warning" title="双人审批门禁">
          {decisionBlockReason}
        </IrisAlert>
      ) : null}
      {run ? (
        <div className="result-panel">
          <StatusBadge value={run.status} />
          <span>
            {run.id} · {run.operation_id}
          </span>
          <span>
            申请人 {run.created_by || '—'} · 决策人 {run.approved_by || run.rejected_by || '待定'}
          </span>
          {run.status === 'pending_approval' ? (
            <span className="button-row">
              <IrisButton
                size="sm"
                variant="solid"
                disabled={busy || Boolean(decisionBlockReason)}
                onClick={() => setDecision('approve')}
              >
                批准
              </IrisButton>
              <IrisButton
                size="sm"
                variant="outline"
                disabled={busy || Boolean(decisionBlockReason)}
                onClick={() => setDecision('reject')}
              >
                拒绝
              </IrisButton>
            </span>
          ) : null}
        </div>
      ) : null}
      <ConfirmDialog
        open={createConfirmation}
        title="确认创建恢复审批单"
        description={`预演 ${preview?.id ?? ''} 已绑定 Operation ${operationId.trim()}。${actorSubject} 将以“${reason.trim()}”为原因创建双人审批申请；服务端会再次回放，且不会在此步骤执行外部调用。`}
        confirmLabel="确认创建"
        busy={busy}
        onOpenChange={setCreateConfirmation}
        onConfirm={create}
      />
      <ConfirmDialog
        open={decision !== null}
        title={decision === 'approve' ? '确认批准恢复申请' : '确认拒绝恢复申请'}
        description={`${run?.id ?? ''} 将记录由 ${actorSubject} 作出的${
          decision === 'approve' ? '批准' : '拒绝'
        }决定；该决定不可重复提交。`}
        confirmLabel={decision === 'approve' ? '确认批准' : '确认拒绝'}
        busy={busy}
        onOpenChange={(open) => !open && setDecision(null)}
        onConfirm={decide}
      />
    </IrisCard>
  )
}
