package grpcapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Server struct {
	auditv1.UnimplementedIngestServer
	Service *service.Service
	Auth    auth.Authenticator
	// Logger receives the full detail of internal errors before they are
	// redacted for the client (M-1): diagnostics stay server-side while the
	// gRPC response carries a fixed message.
	Logger *log.Logger
}

func Register(server grpc.ServiceRegistrar, svc *service.Service, authenticator auth.Authenticator) {
	auditv1.RegisterIngestServer(server, &Server{Service: svc, Auth: authenticator, Logger: log.Default()})
}

func (s *Server) Write(ctx context.Context, request *auditv1.WriteRequest) (*auditv1.WriteResponse, error) {
	claims, err := s.authenticate(ctx, "audit:event:write")
	if err != nil {
		return nil, err
	}
	if request == nil || request.GetEvent() == nil {
		return nil, status.Error(codes.InvalidArgument, "event is required")
	}
	event, err := fromProto(request.GetEvent())
	if err != nil {
		return nil, s.statusError(err)
	}
	principal := domain.IngestPrincipal{ClientID: claims.ClientID}
	// F1: the durable ingest commit survives client disconnect — the write is
	// detached from RPC-context cancellation (values are kept) so a dropped
	// connection can never roll back a ledgered event.
	receipt, err := s.Service.Ingest(context.WithoutCancel(ctx), claims.TenantID, principal, event, request.GetWaitFor())
	if err != nil {
		return nil, s.statusError(err)
	}
	return toProtoReceipt(receipt), nil
}

func (s *Server) WriteBatch(ctx context.Context, request *auditv1.WriteBatchRequest) (*auditv1.WriteBatchResponse, error) {
	claims, err := s.authenticate(ctx, "audit:event:write")
	if err != nil {
		return nil, err
	}
	if request == nil || len(request.GetEvents()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "events must not be empty")
	}
	// Pre-flight count cap: a batch beyond MaxBatchEvents is rejected before
	// any fromProto call or Service.Ingest, so the violation can never
	// partially commit. Per-envelope failures inside the loop keep the
	// existing partial-commit semantics (valid prefix ledgered, wire receipts
	// dropped).
	if len(request.GetEvents()) > MaxBatchEvents {
		return nil, status.Error(codes.InvalidArgument,
			fmt.Sprintf("batch exceeds %d events, max %d", len(request.GetEvents()), MaxBatchEvents))
	}
	response := &auditv1.WriteBatchResponse{}
	principal := domain.IngestPrincipal{ClientID: claims.ClientID}
	commitCtx := context.WithoutCancel(ctx)
	for _, item := range request.GetEvents() {
		event, convertErr := fromProto(item)
		if convertErr != nil {
			return nil, s.statusError(convertErr)
		}
		receipt, ingestErr := s.Service.Ingest(commitCtx, claims.TenantID, principal, event, request.GetWaitFor())
		if ingestErr != nil {
			return response, s.statusError(ingestErr)
		}
		response.Receipts = append(response.Receipts, toProtoReceipt(receipt))
	}
	return response, nil
}

func (s *Server) WriteStream(stream auditv1.Ingest_WriteStreamServer) error {
	claims, err := s.authenticate(stream.Context(), "audit:event:write")
	if err != nil {
		return err
	}
	principal := domain.IngestPrincipal{ClientID: claims.ClientID}
	for {
		request, receiveErr := stream.Recv()
		if receiveErr != nil {
			if errors.Is(receiveErr, io.EOF) {
				return nil
			}
			return receiveErr
		}
		if request == nil || request.GetEvent() == nil {
			return status.Error(codes.InvalidArgument, "event is required")
		}
		event, convertErr := fromProto(request.GetEvent())
		if convertErr != nil {
			if errors.Is(convertErr, ErrEnvelopeTooLarge) {
				if s.Logger != nil {
					// Size rejections are the only skip-and-continue class; the
					// log line carries a truncated event_id prefix (never the
					// full attacker-controlled value) plus principal
					// attribution, and the missing receipt is the wire signal.
					s.Logger.Printf("grpc stream: rejecting over-cap message client_id=%q tenant=%q event_id=%q: %v",
						claims.ClientID, claims.TenantID, truncateEventID(request.GetEvent().GetEventId()), convertErr)
				}
				continue // no receipt; stream stays open; client reconciles by event_id
			}
			return s.statusError(convertErr)
		}
		receipt, ingestErr := s.Service.Ingest(context.WithoutCancel(stream.Context()), claims.TenantID, principal, event, request.GetWaitFor())
		if ingestErr != nil {
			return s.statusError(ingestErr)
		}
		if sendErr := stream.Send(toProtoReceipt(receipt)); sendErr != nil {
			return sendErr
		}
	}
}

func (s *Server) authenticate(ctx context.Context, permission string) (auth.Claims, error) {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) == 0 {
		return auth.Claims{}, status.Error(codes.Unauthenticated, "authorization metadata is required")
	}
	token := strings.TrimSpace(values[0])
	if strings.HasPrefix(strings.ToLower(token), "bearer ") {
		token = strings.TrimSpace(token[7:])
	}
	claims, err := s.Auth.AuthenticateTokenContext(ctx, token)
	if err != nil {
		return auth.Claims{}, status.Error(codes.Unauthenticated, err.Error())
	}
	if !claims.Allows(permission) {
		return claims, status.Error(codes.PermissionDenied, "permission denied")
	}
	return claims, nil
}

func fromProto(input *auditv1.EventEnvelope) (domain.Event, error) {
	if input == nil {
		return domain.Event{}, fmt.Errorf("%w: event is required", domain.ErrInvalid)
	}
	// Per-field size/arity caps run before any field is copied into the
	// ledger and before the occurred_at/actor/decode checks, so an envelope
	// that is both over-cap and malformed reports the cap violation first.
	if err := validateEnvelopeCaps(input); err != nil {
		return domain.Event{}, err
	}
	if input.GetOccurredAt() == nil || input.GetOccurredAt().CheckValid() != nil {
		return domain.Event{}, fmt.Errorf("%w: occurred_at is invalid", domain.ErrInvalid)
	}
	event := domain.Event{EventID: input.GetEventId(), SourceSystem: input.GetSourceSystem(), EventType: input.GetEventType(), SchemaID: input.GetSchemaId(), SchemaVersion: int(input.GetSchemaVersion()), OccurredAt: input.GetOccurredAt().AsTime(), OperationID: input.GetOperationId(), CausationID: input.GetCausationId(), CorrelationID: input.GetCorrelationId(), TraceID: input.GetTraceId(), SpanID: input.GetSpanId(), AggregateType: input.GetAggregateType(), AggregateID: input.GetAggregateId(), AggregateVersion: input.GetAggregateVersion(), WorkflowInstanceID: input.GetWorkflowInstanceId(), ExecutionRunID: input.GetExecutionRunId(), Action: input.GetAction(), Outcome: input.GetOutcome(), Reason: input.GetReason(), PayloadRef: input.GetPayloadRef(), DataClassification: input.GetDataClassification(), RetentionClass: input.GetRetentionClass(), IdempotencyKey: input.GetIdempotencyKey(), Actor: domain.Actor{ID: input.GetActor().GetId(), Type: input.GetActor().GetType(), Name: input.GetActor().GetName(), Department: input.GetActor().GetDepartment(), Roles: input.GetActor().GetRoles()}}
	if input.GetActor() == nil {
		return domain.Event{}, fmt.Errorf("%w: actor is required", domain.ErrInvalid)
	}
	if len(input.GetPayloadJson()) > 0 {
		if err := decodeJSONNumber(input.GetPayloadJson(), &event.Payload); err != nil {
			return domain.Event{}, fmt.Errorf("%w: payload_json is invalid", domain.ErrInvalid)
		}
	}
	for _, target := range input.GetTargets() {
		event.Targets = append(event.Targets, domain.Target{Type: target.GetType(), ID: target.GetId(), Name: target.GetName()})
	}
	if len(input.GetChangedFields()) > 0 {
		event.ChangedFields = map[string]domain.FieldChange{}
	}
	for _, change := range input.GetChangedFields() {
		var before, after any
		if change.GetBeforeJson() != "" {
			if err := decodeJSONNumber([]byte(change.GetBeforeJson()), &before); err != nil {
				return domain.Event{}, fmt.Errorf("%w: invalid before_json", domain.ErrInvalid)
			}
		}
		if change.GetAfterJson() != "" {
			if err := decodeJSONNumber([]byte(change.GetAfterJson()), &after); err != nil {
				return domain.Event{}, fmt.Errorf("%w: invalid after_json", domain.ErrInvalid)
			}
		}
		event.ChangedFields[change.GetField()] = domain.FieldChange{Before: before, After: after}
	}
	return event, nil
}

// decodeJSONNumber decodes JSON with UseNumber so payload and changed-field
// numbers reach digest derivation as exact json.Number values: a float64
// decode would collapse int64 values > 2^53 and make the gRPC ingest digest
// diverge from the HTTP ingest digest of the same event.
func decodeJSONNumber(data []byte, out any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	// Strict single-value decode: the first Decode consumes exactly one JSON
	// value; anything but io.EOF on the second Decode means trailing content
	// that the ledger would otherwise record truncated. Matches decodeBody's
	// exhaustion check on the HTTP surface so both transports reject the same
	// input class and cross-transport SourceDigest parity holds for every
	// accepted input.
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("%w: trailing content after first JSON value", domain.ErrInvalid)
	}
	return nil
}

func toProtoReceipt(receipt domain.EventReceipt) *auditv1.WriteResponse {
	return &auditv1.WriteResponse{EventId: receipt.EventID, TenantId: receipt.TenantID, Status: receipt.Status, StreamId: receipt.StreamID, Sequence: receipt.Sequence, Hash: receipt.Hash, Duplicate: receipt.Duplicate}
}

func toStatus(err error) error {
	switch {
	case errors.Is(err, domain.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrUnauthorized):
		return status.Error(codes.Unauthenticated, err.Error())
	case errors.Is(err, domain.ErrForbidden):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, domain.ErrQuotaExceeded):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, domain.ErrSchemaNotFound):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrTenantMismatch):
		// 与 HTTP 422 对齐：请求语义有效但租户一致性不成立。
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		// Internal errors never carry the underlying text (filesystem paths,
		// errno strings, Vault/pgx details): the detail is logged server-side
		// by statusError and the client gets a fixed message, mirroring the
		// HTTP boundary's errorBody redaction.
		return status.Error(codes.Internal, "internal server error")
	}
}

// statusError converts err for the client and logs the full detail of
// internal errors server-side before the redacted status is returned.
func (s *Server) statusError(err error) error {
	converted := toStatus(err)
	if status.Code(converted) == codes.Internal && s.Logger != nil {
		s.Logger.Printf("grpc internal error: %v", err)
	}
	return converted
}
