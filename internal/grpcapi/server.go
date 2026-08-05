package grpcapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
}

func Register(server grpc.ServiceRegistrar, svc *service.Service, authenticator auth.Authenticator) {
	auditv1.RegisterIngestServer(server, &Server{Service: svc, Auth: authenticator})
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
		return nil, toStatus(err)
	}
	principal := domain.IngestPrincipal{ClientID: claims.ClientID}
	receipt, err := s.Service.Ingest(claims.TenantID, principal, event, request.GetWaitFor())
	if err != nil {
		return nil, toStatus(err)
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
	response := &auditv1.WriteBatchResponse{}
	principal := domain.IngestPrincipal{ClientID: claims.ClientID}
	for _, item := range request.GetEvents() {
		event, convertErr := fromProto(item)
		if convertErr != nil {
			return nil, toStatus(convertErr)
		}
		receipt, ingestErr := s.Service.Ingest(claims.TenantID, principal, event, request.GetWaitFor())
		if ingestErr != nil {
			return response, toStatus(ingestErr)
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
			return toStatus(convertErr)
		}
		receipt, ingestErr := s.Service.Ingest(claims.TenantID, principal, event, request.GetWaitFor())
		if ingestErr != nil {
			return toStatus(ingestErr)
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
	if input.GetOccurredAt() == nil || input.GetOccurredAt().CheckValid() != nil {
		return domain.Event{}, fmt.Errorf("%w: occurred_at is invalid", domain.ErrInvalid)
	}
	event := domain.Event{EventID: input.GetEventId(), SourceSystem: input.GetSourceSystem(), EventType: input.GetEventType(), SchemaID: input.GetSchemaId(), SchemaVersion: int(input.GetSchemaVersion()), OccurredAt: input.GetOccurredAt().AsTime(), OperationID: input.GetOperationId(), CausationID: input.GetCausationId(), CorrelationID: input.GetCorrelationId(), TraceID: input.GetTraceId(), SpanID: input.GetSpanId(), AggregateType: input.GetAggregateType(), AggregateID: input.GetAggregateId(), AggregateVersion: input.GetAggregateVersion(), Action: input.GetAction(), Outcome: input.GetOutcome(), Reason: input.GetReason(), PayloadRef: input.GetPayloadRef(), DataClassification: input.GetDataClassification(), RetentionClass: input.GetRetentionClass(), IdempotencyKey: input.GetIdempotencyKey(), Actor: domain.Actor{ID: input.GetActor().GetId(), Type: input.GetActor().GetType(), Name: input.GetActor().GetName(), Department: input.GetActor().GetDepartment(), Roles: input.GetActor().GetRoles()}}
	if input.GetActor() == nil {
		return domain.Event{}, fmt.Errorf("%w: actor is required", domain.ErrInvalid)
	}
	if len(input.GetPayloadJson()) > 0 {
		if err := json.Unmarshal(input.GetPayloadJson(), &event.Payload); err != nil {
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
			if err := json.Unmarshal([]byte(change.GetBeforeJson()), &before); err != nil {
				return domain.Event{}, fmt.Errorf("%w: invalid before_json", domain.ErrInvalid)
			}
		}
		if change.GetAfterJson() != "" {
			if err := json.Unmarshal([]byte(change.GetAfterJson()), &after); err != nil {
				return domain.Event{}, fmt.Errorf("%w: invalid after_json", domain.ErrInvalid)
			}
		}
		event.ChangedFields[change.GetField()] = domain.FieldChange{Before: before, After: after}
	}
	return event, nil
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
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
