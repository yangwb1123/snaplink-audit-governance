package domain

import "time"

// GovernanceStatus is an authorization-filtered control-plane observation.
// Omitted sections are not authorized, never zero-valued evidence reports.
// It neither reads nor attests ledger, publication, projection or business state.
type GovernanceStatus struct {
	TenantID     string                     `json:"tenant_id"`
	ObservedAt   time.Time                  `json:"observed_at"`
	Source       string                     `json:"source"`
	Completeness string                     `json:"completeness"`
	Exports      *GovernanceExportStatus    `json:"exports,omitempty"`
	Restores     *GovernanceRestoreStatus   `json:"restores,omitempty"`
	LegalHolds   *GovernanceHoldStatus      `json:"legal_holds,omitempty"`
	Retention    *GovernanceRetentionStatus `json:"retention,omitempty"`
}

type GovernanceExportStatus struct {
	Total              int        `json:"total"`
	Pending            int        `json:"pending"`
	Running            int        `json:"running"`
	Completed          int        `json:"completed"`
	Failed             int        `json:"failed"`
	Cancelled          int        `json:"cancelled"`
	Unknown            int        `json:"unknown"`
	TenantQueueLimit   int        `json:"tenant_queue_limit"`
	TenantRunningLimit int        `json:"tenant_running_limit"`
	OldestPendingAt    *time.Time `json:"oldest_pending_at,omitempty"`
}

type GovernanceRestoreStatus struct {
	Total               int    `json:"total"`
	PendingApproval     int    `json:"pending_approval"`
	Approved            int    `json:"approved"`
	Rejected            int    `json:"rejected"`
	Unknown             int    `json:"unknown"`
	MinePendingApproval int    `json:"mine_pending_approval"`
	Actionable          *int   `json:"actionable,omitempty"`
	Execution           string `json:"execution"`
}

type GovernanceHoldStatus struct {
	Total    int `json:"total"`
	Active   int `json:"active"`
	Released int `json:"released"`
}

// Configured policy is not an object-version Hold/COMPLIANCE or deletion proof.
type GovernanceRetentionStatus struct {
	Configured bool             `json:"configured"`
	Policy     *RetentionPolicy `json:"policy,omitempty"`
}

const AdminActionGovernanceStatusRead = "governance.status.read"
