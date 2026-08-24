export type JsonRecord = Record<string, unknown>

export interface Actor extends JsonRecord {
  id: string
  type?: string
  name?: string
  department?: string
  roles?: string[]
}

export interface AuditEvent extends JsonRecord {
  event_id: string
  tenant_id: string
  source_system: string
  event_type: string
  schema_id: string
  schema_version: number
  occurred_at: string
  received_at: string
  operation_id?: string
  causation_id?: string
  correlation_id?: string
  trace_id?: string
  span_id?: string
  actor: Actor
  targets?: JsonRecord[]
  aggregate_type?: string
  aggregate_id?: string
  aggregate_version?: number
  workflow_instance_id?: string
  execution_run_id?: string
  action: string
  outcome: string
  reason?: string
  changed_fields?: JsonRecord
  payload?: JsonRecord
  payload_ref?: string
  source_digest?: string
  data_classification: string
  retention_class: string
  idempotency_key?: string
  stream_id?: string
  sequence?: number
  prev_hash?: string
  hash?: string
  server_version?: string
}

export interface EventReceipt extends JsonRecord {
  event_id: string
  tenant_id: string
  status: string
  accepted_at: string
  ledgered_at?: string
  indexed_at?: string
  archived_at?: string
  stream_id?: string
  sequence?: number
  hash?: string
  duplicate?: boolean
  conflict?: boolean
  error_code?: string
  error_message?: string
}

export interface EventQuery {
  from: string
  to: string
  event_type?: string
  source_system?: string
  actor_id?: string
  target_id?: string
  aggregate_type?: string
  aggregate_id?: string
  operation_id?: string
  causation_id?: string
  correlation_id?: string
  trace_id?: string
  outcome?: string
  payload_field?: string
  payload_digest?: string
  stream_id?: string
  cursor?: string
  page_size?: number
}

export interface QueryResult extends JsonRecord {
  items: AuditEvent[]
  next_cursor?: string
  count: number
}

export interface EventFacetQuery {
  since: string
  until: string
  event_type?: string
  actor_id?: string
  client_id?: string
  outcome?: string
  trace_id?: string
}

export interface EventFacets extends JsonRecord {
  total: number
  outcomes: Record<string, number>
  types: Record<string, number>
  clients: Record<string, number>
  providers: Record<string, number>
}

export interface EventFacetsResponse extends JsonRecord {
  facets: EventFacets
}

export interface EventReceiptResponse extends JsonRecord {
  receipt: EventReceipt
  receipt_url: string
}

export interface BatchReceiptResponse extends JsonRecord {
  receipts: EventReceipt[]
}

export interface OperationSummary extends JsonRecord {
  operation_id: string
  tenant_id: string
  event_count: number
  first_at: string
  last_at: string
  outcomes: string[]
}

export interface ReplayResult extends JsonRecord {
  tenant_id: string
  operation_id?: string
  aggregate_id?: string
  event_count: number
  state: JsonRecord
  last_sequence: number
}

export interface IntegrityResult extends JsonRecord {
  tenant_id: string
  stream_id?: string
  valid: boolean
  event_count: number
  segment_count: number
  checked_at: string
  errors?: string[]
}

export interface LegalHold extends JsonRecord {
  id: string
  tenant_id: string
  name: string
  reason: string
  filter: EventQuery
  created_by: string
  created_at: string
  released_at?: string
  released_by?: string
}

export interface Tenant extends JsonRecord {
  id: string
  name: string
  home_region: string
  data_region: string
  active: boolean
  events_per_second: number
  burst: number
  created_at: string
}

export interface SourceSystem extends JsonRecord {
  id: string
  tenant_id: string
  name: string
  allowed_client_ids?: string[]
  active: boolean
  created_at: string
}

export interface EventSchema extends JsonRecord {
  tenant_id: string
  schema_id: string
  version: number
  event_type: string
  required_fields?: string[]
  allowed_fields?: string[]
  encrypted_fields?: string[]
  searchable_fields?: string[]
  classification: string
  active: boolean
  created_at: string
}

export interface RetentionPolicy extends JsonRecord {
  tenant_id: string
  hot_days: number
  warm_days: number
  archive_days: number
  retention_class: string
}

export interface RetentionReport extends JsonRecord {
  tenant_id: string
  evaluated_at: string
  policy: RetentionPolicy
  eligible_events: number
  protected_events: number
  hold_ids?: string[]
  action: string
}

export interface AdminAction extends JsonRecord {
  id: string
  tenant_id?: string
  actor: string
  action: string
  target_type?: string
  target_id?: string
  detail?: string
  created_at: string
}

export interface ExportJob extends JsonRecord {
  id: string
  tenant_id: string
  requested_by: string
  query: EventQuery
  status: string
  created_at: string
  finished_at?: string
  digest?: string
  event_count: number
  error?: string
}

export interface RestoreRequest extends JsonRecord {
  operation_id: string
  reason: string
}

export interface RestorePreview extends JsonRecord {
  id: string
  tenant_id: string
  operation_id: string
  proposed_state: JsonRecord
  external_calls: string[]
  requires_approval: boolean
}

export interface RestoreRun extends JsonRecord {
  id: string
  tenant_id: string
  operation_id: string
  status: string
  reason: string
  created_by?: string
  created_at: string
  approved_by?: string
  approved_at?: string
  rejected_by?: string
  rejected_at?: string
}

export interface ItemList<T> extends JsonRecord {
  items: T[]
  count: number
}
