import { ApiClient, type ApiClientOptions } from './client'
import type {
  AdminAction,
  AuditEvent,
  BatchReceiptResponse,
  EventQuery,
  EventReceipt,
  EventReceiptResponse,
  EventFacetQuery,
  EventFacetsResponse,
  EventSchema,
  ExportJob,
  IntegrityResult,
  ItemList,
  LegalHold,
  OperationSummary,
  QueryResult,
  ReplayResult,
  RestorePreview,
  RestoreRequest,
  RestoreRun,
  RetentionPolicy,
  RetentionReport,
  SourceSystem,
  Tenant,
} from './types'

function queryString(values: Record<string, unknown>): string {
  const params = new URLSearchParams()
  for (const [name, value] of Object.entries(values)) {
    if ((typeof value === 'string' || typeof value === 'number') && value !== '') {
      params.set(name, String(value))
    }
  }
  const encoded = params.toString()
  return encoded ? `?${encoded}` : ''
}

function jsonBody(value: unknown): string {
  return JSON.stringify(value)
}

export class AuditApi {
  private readonly client: ApiClient
  private readonly platform: boolean

  constructor(options: ApiClientOptions) {
    this.client = new ApiClient(options)
    this.platform = options.platform ?? false
  }

  private scopedTenant(tenantId?: string): string | undefined {
    if (this.platform && !tenantId?.trim()) {
      throw new Error('tenant_id is required for platform-scoped operations')
    }
    return tenantId
  }

  queryEvents(query: EventQuery, tenantId?: string): Promise<QueryResult> {
    return this.client.request(
      `/api/v1/events${queryString({ ...query, tenant_id: this.scopedTenant(tenantId) })}`,
    )
  }

  getEventFacets(query: EventFacetQuery, tenantId?: string): Promise<EventFacetsResponse> {
    return this.client.request(
      `/api/v1/compat/snaplink/audit/facets${queryString({ ...query, tenant_id: tenantId })}`,
    )
  }

  ingestEvent(event: AuditEvent, waitFor?: string): Promise<EventReceiptResponse> {
    return this.client.request(`/api/v1/events${queryString({ wait_for: waitFor })}`, {
      method: 'POST',
      body: jsonBody(event),
    })
  }

  ingestBatch(events: AuditEvent[], waitFor?: string): Promise<BatchReceiptResponse> {
    return this.client.request('/api/v1/events:batch', {
      method: 'POST',
      body: jsonBody({ events, wait_for: waitFor }),
    })
  }

  getEvent(eventId: string, tenantId?: string): Promise<AuditEvent> {
    return this.client.request(
      `/api/v1/events/${encodeURIComponent(eventId)}${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
    )
  }

  getReceipt(eventId: string, tenantId?: string): Promise<EventReceipt> {
    return this.client.request(
      `/api/v1/events/${encodeURIComponent(eventId)}/receipt${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
    )
  }

  getOperation(operationId: string, tenantId?: string): Promise<OperationSummary> {
    return this.client.request(
      `/api/v1/operations/${encodeURIComponent(operationId)}${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
    )
  }

  getOperationTimeline(
    operationId: string,
    options: { pageSize?: number; cursor?: string; tenantId?: string } = {},
  ): Promise<QueryResult> {
    return this.client.request(
      `/api/v1/operations/${encodeURIComponent(operationId)}/timeline${queryString({
        page_size: options.pageSize,
        cursor: options.cursor,
        tenant_id: this.scopedTenant(options.tenantId),
      })}`,
    )
  }

  replayOperation(operationId: string, tenantId?: string): Promise<ReplayResult> {
    return this.client.request(
      `/api/v1/operations/${encodeURIComponent(operationId)}/replay${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
    )
  }

  getAggregateTimeline(
    aggregateType: string,
    aggregateId: string,
    options: { pageSize?: number; cursor?: string; tenantId?: string } = {},
  ): Promise<QueryResult> {
    return this.client.request(
      `/api/v1/aggregates/${encodeURIComponent(aggregateType)}/${encodeURIComponent(aggregateId)}/timeline${queryString(
        {
          page_size: options.pageSize,
          cursor: options.cursor,
          tenant_id: this.scopedTenant(options.tenantId),
        },
      )}`,
    )
  }

  verifyIntegrity(streamId: string, tenantId?: string): Promise<IntegrityResult> {
    return this.client.request(
      `/api/v1/integrity/verify${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
      {
        method: 'POST',
        body: jsonBody({ stream_id: streamId }),
      },
    )
  }

  listLegalHolds(tenantId?: string): Promise<ItemList<LegalHold>> {
    return this.client.request(
      `/api/v1/legal-holds${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
    )
  }

  createLegalHold(
    hold: Pick<LegalHold, 'name' | 'reason' | 'filter'>,
    tenantId?: string,
  ): Promise<LegalHold> {
    return this.client.request(
      `/api/v1/legal-holds${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
      {
        method: 'POST',
        body: jsonBody(hold),
      },
    )
  }

  releaseLegalHold(holdId: string, tenantId?: string): Promise<LegalHold> {
    return this.client.request(
      `/api/v1/legal-holds/${encodeURIComponent(holdId)}/release${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
      { method: 'POST' },
    )
  }

  createExport(query: EventQuery, tenantId?: string): Promise<ExportJob> {
    return this.client.request(
      `/api/v1/exports${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
      {
        method: 'POST',
        body: jsonBody(query),
      },
    )
  }

  getExport(jobId: string, tenantId?: string): Promise<ExportJob> {
    return this.client.request(
      `/api/v1/exports/${encodeURIComponent(jobId)}${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
    )
  }

  downloadExport(jobId: string, tenantId?: string): Promise<Blob> {
    return this.client.blob(
      `/api/v1/exports/${encodeURIComponent(jobId)}/download${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
    )
  }

  previewRestore(request: RestoreRequest, tenantId?: string): Promise<RestorePreview> {
    return this.client.request(
      `/api/v1/restores/preview${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
      {
        method: 'POST',
        body: jsonBody(request),
      },
    )
  }

  createRestore(request: RestoreRequest, tenantId?: string): Promise<RestoreRun> {
    return this.client.request(
      `/api/v1/restores${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
      {
        method: 'POST',
        body: jsonBody(request),
      },
    )
  }

  getRestore(runId: string, tenantId?: string): Promise<RestoreRun> {
    return this.client.request(
      `/api/v1/restores/${encodeURIComponent(runId)}${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
    )
  }

  decideRestore(
    runId: string,
    decision: 'approve' | 'reject',
    tenantId?: string,
  ): Promise<RestoreRun> {
    return this.client.request(
      `/api/v1/restores/${encodeURIComponent(runId)}/${decision}${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
      { method: 'POST' },
    )
  }

  listTenants(): Promise<ItemList<Tenant>> {
    return this.client.request('/api/v1/tenants')
  }

  createTenant(tenant: Omit<Tenant, 'created_at'>): Promise<Tenant> {
    return this.client.request('/api/v1/tenants', { method: 'POST', body: jsonBody(tenant) })
  }

  listSources(tenantId?: string): Promise<ItemList<SourceSystem>> {
    return this.client.request(
      `/api/v1/sources${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
    )
  }

  createSource(source: Omit<SourceSystem, 'created_at'>, tenantId?: string): Promise<SourceSystem> {
    return this.client.request(
      `/api/v1/sources${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
      {
        method: 'POST',
        body: jsonBody(source),
      },
    )
  }

  updateSource(
    source: Pick<SourceSystem, 'id' | 'name' | 'allowed_client_ids' | 'active'>,
    tenantId?: string,
  ): Promise<SourceSystem> {
    return this.client.request(
      `/api/v1/sources/${encodeURIComponent(source.id)}${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
      {
        method: 'PUT',
        body: jsonBody({
          name: source.name,
          allowed_client_ids: source.allowed_client_ids ?? [],
          active: source.active,
        }),
      },
    )
  }

  listSchemas(tenantId?: string): Promise<ItemList<EventSchema>> {
    return this.client.request(
      `/api/v1/schemas${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
    )
  }

  createSchema(schema: Omit<EventSchema, 'created_at'>, tenantId?: string): Promise<EventSchema> {
    return this.client.request(
      `/api/v1/schemas${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
      {
        method: 'POST',
        body: jsonBody(schema),
      },
    )
  }

  getRetention(tenantId?: string): Promise<RetentionPolicy> {
    return this.client.request(
      `/api/v1/policies/retention${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
    )
  }

  setRetention(policy: RetentionPolicy, tenantId?: string): Promise<RetentionPolicy> {
    return this.client.request(
      `/api/v1/policies/retention${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
      { method: 'PUT', body: jsonBody(policy) },
    )
  }

  evaluateRetention(tenantId?: string): Promise<RetentionReport> {
    return this.client.request(
      `/api/v1/retention/evaluate${queryString({ tenant_id: this.scopedTenant(tenantId) })}`,
      {
        method: 'POST',
      },
    )
  }

  listAdminActions(tenantId?: string, limit = 50): Promise<ItemList<AdminAction>> {
    return this.client.request(
      `/api/v1/admin/actions${queryString({ tenant_id: tenantId, limit })}`,
    )
  }
}
