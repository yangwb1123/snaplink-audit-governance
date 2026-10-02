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
	"time"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
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
	// partially commit.
	if len(request.GetEvents()) > MaxBatchEvents {
		return nil, status.Error(codes.InvalidArgument,
			fmt.Sprintf("batch exceeds %d events, max %d", len(request.GetEvents()), MaxBatchEvents))
	}
	response := &auditv1.WriteBatchResponse{}
	principal := domain.IngestPrincipal{ClientID: claims.ClientID}
	commitCtx := context.WithoutCancel(ctx)
	for index, item := range request.GetEvents() {
		outcome := &auditv1.WriteBatchOutcome{InputIndex: int32(index), EventId: batchEventID(item)}
		event, convertErr := fromProto(item)
		if convertErr != nil {
			response.Outcomes = appendRejectedOutcome(response.Outcomes, outcome, s.statusError(convertErr))
			response.Outcomes = appendUnattemptedOutcomes(response.Outcomes, request.GetEvents(), index+1)
			return response, nil
		}
		receipt, ingestErr := s.Service.Ingest(commitCtx, claims.TenantID, principal, event, request.GetWaitFor())
		if ingestErr != nil {
			// Ingest may report a post-commit wait/archive failure. A populated
			// durable, non-conflict receipt is evidence that the event committed
			// and must not be exposed as a rejection; the suffix remains
			// unattempted.
			if hasDurableReceipt(receipt) {
				response = appendCommittedOutcome(response, outcome, receipt)
				response.Outcomes = appendUnattemptedOutcomes(response.Outcomes, request.GetEvents(), index+1)
				return response, nil
			}
			response.Outcomes = appendRejectedOutcome(response.Outcomes, outcome, s.statusError(ingestErr))
			response.Outcomes = appendUnattemptedOutcomes(response.Outcomes, request.GetEvents(), index+1)
			return response, nil
		}
		response = appendCommittedOutcome(response, outcome, receipt)
	}
	return response, nil
}

func hasDurableReceipt(receipt domain.EventReceipt) bool {
	return receipt.EventID != "" && receipt.Status != "" && !receipt.Conflict
}

func appendCommittedOutcome(response *auditv1.WriteBatchResponse, outcome *auditv1.WriteBatchOutcome, receipt domain.EventReceipt) *auditv1.WriteBatchResponse {
	protoReceipt := toProtoReceipt(receipt)
	response.Receipts = append(response.Receipts, protoReceipt)
	outcome.Status = auditv1.WriteBatchOutcome_COMMITTED
	outcome.Receipt = protoReceipt
	response.Outcomes = append(response.Outcomes, outcome)
	return response
}

func batchEventID(event *auditv1.EventEnvelope) string {
	if event == nil {
		return ""
	}
	return event.GetEventId()
}

func appendRejectedOutcome(outcomes []*auditv1.WriteBatchOutcome, outcome *auditv1.WriteBatchOutcome, err error) []*auditv1.WriteBatchOutcome {
	outcome.Status = auditv1.WriteBatchOutcome_REJECTED
	outcome.RejectionCode = rejectionCode(err)
	return append(outcomes, outcome)
}

func appendUnattemptedOutcomes(outcomes []*auditv1.WriteBatchOutcome, events []*auditv1.EventEnvelope, start int) []*auditv1.WriteBatchOutcome {
	for index := start; index < len(events); index++ {
		outcomes = append(outcomes, &auditv1.WriteBatchOutcome{
			InputIndex: int32(index), EventId: batchEventID(events[index]),
			Status: auditv1.WriteBatchOutcome_NOT_ATTEMPTED,
		})
	}
	return outcomes
}

// rejectionCode maps a gRPC status code to the machine-readable rejection
// token published on WriteBatchOutcome.rejection_code. The vocabulary is
// closed: every string below plus "unknown" (for a code outside the switch,
// which cannot happen today because every status the ingest surface returns
// is produced by toStatus or by an explicit status.Error on a listed code).
//
// Reachability from toStatus (the ingest error mapper) — "reachable" means the
// same transport maps the sentinel below to the code:
//
//	invalid_argument     reachable (domain.ErrInvalid, ErrEnvelopeTooLarge,
//	                              ErrOccurredAtOutOfRange)
//	unauthenticated      reachable (domain.ErrUnauthorized; also authenticate)
//	permission_denied    reachable (domain.ErrForbidden; also authenticate)
//	not_found            reachable (domain.ErrNotFound)
//	already_exists       reachable (domain.ErrConflict)
//	resource_exhausted   reachable (domain.ErrQuotaExceeded, stream skip cap,
//	                              transport cap)
//	failed_precondition  reachable (domain.ErrSchemaNotFound,
//	                              domain.ErrTenantMismatch)
//	internal             reachable (toStatus default)
//	unavailable          reachable (store.ErrSnapshotConflict — an exhausted
//	                              optimistic-lock retry, i.e. a transient
//	                              concurrent-writer outcome; and
//	                              store.ErrOperationDeadline — a store
//	                              operation that exhausted its deadline,
//	                              i.e. an interrupted statement with an
//	                              unknown outcome. Both are transient and
//	                              idempotently retryable, which is why they
//	                              share one code.)
//	aborted              reserved (no sentinel maps to it yet)
//	canceled             reserved
//	deadline_exceeded    reserved
//	out_of_range         reserved
//	unimplemented        reserved
//	data_loss            reserved
//	unknown              unreachable by construction (the switch default)
//
// The reserved entries keep the vocabulary stable for clients that branch on
// the token; they are not part of the retryable-transient class until a
// sentinel maps to them.
func rejectionCode(err error) string {
	code := status.Code(err)
	switch code {
	case codes.InvalidArgument:
		return "invalid_argument"
	case codes.AlreadyExists:
		return "already_exists"
	case codes.ResourceExhausted:
		return "resource_exhausted"
	case codes.FailedPrecondition:
		return "failed_precondition"
	case codes.Internal:
		return "internal"
	case codes.Unauthenticated:
		return "unauthenticated"
	case codes.PermissionDenied:
		return "permission_denied"
	case codes.NotFound:
		return "not_found"
	case codes.Aborted:
		return "aborted"
	case codes.Canceled:
		return "canceled"
	case codes.DeadlineExceeded:
		return "deadline_exceeded"
	case codes.OutOfRange:
		return "out_of_range"
	case codes.Unimplemented:
		return "unimplemented"
	case codes.Unavailable:
		return "unavailable"
	case codes.DataLoss:
		return "data_loss"
	default:
		return "unknown"
	}
}

func (s *Server) WriteStream(stream auditv1.Ingest_WriteStreamServer) error {
	claims, err := s.authenticate(stream.Context(), "audit:event:write")
	if err != nil {
		return err
	}
	expiry, _ := claims.EffectiveExpiry()
	skips := 0
	for {
		request, receiveErr := s.receiveWriteStreamRequest(stream, expiry)
		if receiveErr != nil {
			if errors.Is(receiveErr, io.EOF) {
				return nil
			}
			return receiveErr
		}
		// Re-run the same bearer-token verifier and permission gate before
		// looking at the envelope. The claims used for this message are the
		// freshly verified claims, not the initial stream snapshot.
		claims, err = s.authenticate(stream.Context(), "audit:event:write")
		if err != nil {
			return err
		}
		expiry, _ = claims.EffectiveExpiry()
		principal := domain.IngestPrincipal{ClientID: claims.ClientID}
		if request == nil || request.GetEvent() == nil {
			return status.Error(codes.InvalidArgument, "event is required")
		}
		event, convertErr := fromProto(request.GetEvent())
		if convertErr != nil {
			if errors.Is(convertErr, ErrEnvelopeTooLarge) {
				if skips >= MaxStreamSkips {
					return status.Error(codes.ResourceExhausted, "stream over-cap skip limit exceeded")
				}
				skips++
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

type streamReceiveResult struct {
	request *auditv1.WriteRequest
	err     error
}

// receiveWriteStreamRequest bounds a blocked Recv by the already verified
// effective JWT expiry. Recv has no context/deadline parameter, so the one
// active receive is isolated in a buffered-result goroutine; when the timer
// wins, the handler returns the authentication status and the gRPC transport
// closes the underlying receive. A result that races the timer is still
// checked against the authenticator clock before it can be processed.
func (s *Server) receiveWriteStreamRequest(stream auditv1.Ingest_WriteStreamServer, expiry time.Time) (*auditv1.WriteRequest, error) {
	if expiry.IsZero() {
		return stream.Recv()
	}
	wait := expiry.Sub(s.Auth.CurrentTime())
	if wait <= 0 {
		return nil, streamAuthenticationFailed()
	}
	resultCh := make(chan streamReceiveResult, 1)
	done := make(chan struct{})
	go func() {
		request, err := stream.Recv()
		select {
		case resultCh <- streamReceiveResult{request: request, err: err}:
		case <-done:
		}
	}()
	timer := time.NewTimer(wait)
	defer func() {
		close(done)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	select {
	case <-timer.C:
		return nil, streamAuthenticationFailed()
	case result := <-resultCh:
		if !s.Auth.CurrentTime().Before(expiry) {
			return nil, streamAuthenticationFailed()
		}
		select {
		case <-timer.C:
			return nil, streamAuthenticationFailed()
		default:
		}
		return result.request, result.err
	}
}

func streamAuthenticationFailed() error {
	return status.Error(codes.Unauthenticated, "authentication failed")
}

func (s *Server) authenticate(ctx context.Context, permission string) (auth.Claims, error) {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 {
		// Never choose a credential by position. This generic error also keeps
		// absent and repeated metadata indistinguishable to the caller.
		return auth.Claims{}, status.Error(codes.Unauthenticated, "authorization metadata is required")
	}
	token := strings.TrimSpace(values[0])
	if strings.HasPrefix(strings.ToLower(token), "bearer ") {
		token = strings.TrimSpace(token[7:])
	}
	claims, err := s.Auth.AuthenticateTokenContext(ctx, token)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Printf("grpc authentication failed: %v", err)
		}
		return auth.Claims{}, status.Error(codes.Unauthenticated, "authentication failed")
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
	// Validate only the supported envelope messages. This must precede caps so
	// an unknown field cannot be classified as an over-cap stream skip.
	if err := validateEnvelopeUnknownFields(input); err != nil {
		return domain.Event{}, err
	}
	// Per-field size/arity caps run before any field is copied into the
	// ledger and before the occurred_at/actor/decode checks, so an envelope
	// that is both over-cap and malformed reports the cap violation first.
	// The changed-field count and names are checked before their JSON values,
	// allowing duplicate detection to precede all changed-field JSON decoding.
	if err := validateEnvelopeCaps(input); err != nil {
		return domain.Event{}, err
	}
	if err := rejectDuplicateChangedFields(input.GetChangedFields()); err != nil {
		return domain.Event{}, err
	}
	if err := validateChangedFieldCaps(input); err != nil {
		return domain.Event{}, err
	}
	if input.GetOccurredAt() == nil || input.GetOccurredAt().CheckValid() != nil {
		return domain.Event{}, fmt.Errorf("%w: occurred_at is invalid", domain.ErrInvalid)
	}
	if input.GetActor() == nil {
		return domain.Event{}, fmt.Errorf("%w: actor is required", domain.ErrInvalid)
	}
	event := domain.Event{EventID: input.GetEventId(), SourceSystem: input.GetSourceSystem(), EventType: input.GetEventType(), SchemaID: input.GetSchemaId(), SchemaVersion: int(input.GetSchemaVersion()), OccurredAt: input.GetOccurredAt().AsTime(), OperationID: input.GetOperationId(), CausationID: input.GetCausationId(), CorrelationID: input.GetCorrelationId(), TraceID: input.GetTraceId(), SpanID: input.GetSpanId(), AggregateType: input.GetAggregateType(), AggregateID: input.GetAggregateId(), AggregateVersion: input.GetAggregateVersion(), WorkflowInstanceID: input.GetWorkflowInstanceId(), ExecutionRunID: input.GetExecutionRunId(), Action: input.GetAction(), Outcome: input.GetOutcome(), Reason: input.GetReason(), PayloadRef: input.GetPayloadRef(), DataClassification: input.GetDataClassification(), RetentionClass: input.GetRetentionClass(), IdempotencyKey: input.GetIdempotencyKey(), Actor: domain.Actor{ID: input.GetActor().GetId(), Type: input.GetActor().GetType(), Name: input.GetActor().GetName(), Department: input.GetActor().GetDepartment(), Roles: input.GetActor().GetRoles()}}
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
	case errors.Is(err, store.ErrSnapshotConflict):
		// Optimistic-lock exhaustion is transient, not an internal fault:
		// replicas share one audit_state_snapshot row, so losing
		// snapshotConflictRetries times is a normal concurrent-writer
		// outcome in which nothing was committed. The HTTP boundary maps
		// the same sentinel to 503 snapshot_conflict, so gRPC uses the
		// direct analogue — codes.Unavailable is retried by the standard
		// at-least-once gRPC policy while codes.Internal is not, and the
		// ingest request is idempotent, so a retry is safe.
		return status.Error(codes.Unavailable, snapshotConflictMessage)
	case errors.Is(err, store.ErrOperationDeadline):
		// The store operation exhausted its deadline, so the in-flight
		// statement was interrupted with an unknown outcome. That is the same
		// transient, idempotently-retryable class as an exhausted
		// optimistic-lock retry, and the HTTP boundary maps the same sentinel
		// to 503 store_timeout, so gRPC uses codes.Unavailable and a fixed
		// message instead of codes.Internal.
		return status.Error(codes.Unavailable, storeDeadlineMessage)
	default:
		// Internal errors never carry the underlying text (filesystem paths,
		// errno strings, Vault/pgx details): the detail is logged server-side
		// by statusError and the client gets a fixed message, mirroring the
		// HTTP boundary's errorBody redaction.
		return status.Error(codes.Internal, "internal server error")
	}
}

// snapshotConflictMessage is the fixed client-facing text for an exhausted
// optimistic-lock retry. It is a constant, never err.Error(): a backend may
// wrap the sentinel with a state path, an errno string or a pgx/Vault detail,
// and none of that may cross the boundary. The text is retry-guiding because
// the condition is transient and the ingest request is idempotent.
const snapshotConflictMessage = "state snapshot changed concurrently; retry the request"

// storeDeadlineMessage is the fixed client-facing text for a store operation
// that exhausted its configured deadline. Like snapshotConflictMessage it is a
// constant and never err.Error(): the wrapped cause can carry a DSN, a
// pgx detail or a state path, none of which may cross the boundary. The text
// is retry-guiding because the ingest request is idempotent and an idempotent
// retry converges the half-completed commit.
const storeDeadlineMessage = "store operation deadline exceeded; retry the request"

// statusError converts err for the client and logs the full detail of
// internal errors server-side before the redacted status is returned.
// An exhausted optimistic-lock conflict keeps its own log prefix: the detail
// is preserved for operators, but a normal concurrent-writer outcome must not
// be filed as an internal fault.
func (s *Server) statusError(err error) error {
	converted := toStatus(err)
	if s.Logger == nil {
		return converted
	}
	switch {
	case errors.Is(err, store.ErrSnapshotConflict):
		s.Logger.Printf("grpc snapshot conflict: %v", err)
	case errors.Is(err, store.ErrOperationDeadline):
		s.Logger.Printf("grpc store deadline: %v", err)
	case status.Code(converted) == codes.Internal:
		s.Logger.Printf("grpc internal error: %v", err)
	}
	return converted
}
