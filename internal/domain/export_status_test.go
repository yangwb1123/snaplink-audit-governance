package domain

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestExportSummaryFailureCodesAreClosedAndContentFree(t *testing.T) {
	for _, code := range []string{"", ExportFailureExecution, ExportFailureLegalHold, ExportFailureLease, ExportFailureLegacy, ExportFailureCancelled, ExportFailureUnknown} {
		if PublicExportFailureCode(code) != code {
			t.Fatal("known code changed", code)
		}
	}
	job := ExportJob{ID: "source", TenantID: "tenant-a", RetryOf: "original", FailureCode: "/private/credential", Error: "private-error", ObjectPath: "private-path", Digest: "private-hash", Query: Query{ActorID: "private-selector"}}
	summary := SummarizeExport(job)
	raw, err := json.Marshal(summary)
	if err != nil || summary.FailureCode != ExportFailureUnknown || summary.RetryOf != job.RetryOf || strings.Contains(string(raw), "private") {
		t.Fatal("summary exposed arbitrary diagnostic", err)
	}
}
