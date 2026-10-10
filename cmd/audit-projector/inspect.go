package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"

	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/projection"
)

// inspect queries this destination's current state; no cached publication
// confirmation, Kafka offset or receipt status is reused as projection proof.
func runInspectCLI(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("inspect", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	tenant := flags.String("tenant", "", "required tenant")
	stream := flags.String("stream", "", "required stream")
	event := flags.String("event", "", "optional event id for exact confirmation")
	sequence := flags.Int64("sequence", 0, "ledger sequence for exact confirmation")
	hash := flags.String("hash", "", "ledger hash for exact confirmation")
	if err := flags.Parse(args); err != nil {
		return errors.New("invalid inspect flags")
	}
	if flags.NArg() != 0 || *tenant == "" || *stream == "" || domain.ValidTenantIDComponent("tenant_id", *tenant) != nil || domain.ValidKeyComponent("stream id", *stream) != nil {
		return domain.ErrInvalid
	}
	if *event == "" && (*sequence != 0 || *hash != "") {
		return domain.ErrInvalid
	}
	if *event != "" && (*sequence <= 0 || *hash == "" || domain.ValidKeyComponent("event id", *event) != nil) {
		return domain.ErrInvalid
	}
	st, err := projection.Open(envOr("AUDIT_CLICKHOUSE_DSN", "clickhouse://audit:audit-local-only@localhost:19000/audit"))
	if err != nil {
		return errors.New("projection inspection destination unavailable")
	}
	defer st.Close()
	watermark, err := st.Watermark(ctx, *tenant, *stream)
	if err != nil {
		return errors.New("projection watermark unavailable or conflicting")
	}
	result := struct {
		Watermark    projection.StreamWatermark `json:"watermark"`
		Confirmed    *bool                      `json:"confirmed,omitempty"`
		Completeness string                     `json:"completeness"`
	}{Watermark: watermark, Completeness: "not_asserted"}
	if *event != "" {
		confirmed, err := st.Confirmed(ctx, domain.EventReceipt{TenantID: *tenant, StreamID: *stream, EventID: *event, Sequence: *sequence, Hash: *hash})
		if err != nil {
			return errors.New("projection confirmation unavailable or conflicting")
		}
		result.Confirmed = &confirmed
	}
	return json.NewEncoder(output).Encode(result)
}
