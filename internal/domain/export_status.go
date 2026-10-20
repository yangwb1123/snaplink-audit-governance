package domain

const (
	ExportStatusCancelled  = "cancelled"
	ExportFailureExecution = "execution_failed"
	ExportFailureLegalHold = "legal_hold"
	ExportFailureLease     = "lease_expired"
	ExportFailureLegacy    = "legacy_stuck"
	ExportFailureCancelled = "cancelled"
	ExportFailureUnknown   = "unknown"
)

// PublicExportFailureCode is a closed, content-free vocabulary. Never return
// an arbitrary persisted diagnostic through the new operational summary field.
func PublicExportFailureCode(code string) string {
	switch code {
	case "", ExportFailureExecution, ExportFailureLegalHold, ExportFailureLease,
		ExportFailureLegacy, ExportFailureCancelled, ExportFailureUnknown:
		return code
	default:
		return ExportFailureUnknown
	}
}

func SummarizeExport(job ExportJob) ExportSummary {
	return ExportSummary{ID: job.ID, TenantID: job.TenantID, RequestedBy: job.RequestedBy,
		Status: job.Status, RetryOf: job.RetryOf, FailureCode: PublicExportFailureCode(job.FailureCode),
		CreatedAt: job.CreatedAt, StartedAt: job.StartedAt, FinishedAt: job.FinishedAt}
}
