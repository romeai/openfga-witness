package interceptor_test

import (
	"context"
	"testing"

	"google.golang.org/grpc"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/witness/interceptor"
	"github.com/openfga/openfga/witness/ocsf"
	"github.com/openfga/openfga/witness/sink"
)

func TestAuditUnary_Check(t *testing.T) {
	ms := sink.NewMemorySink()
	auditInterceptor := interceptor.AuditUnary(ms)

	req := &openfgav1.CheckRequest{
		StoreId: "store-1",
		TupleKey: &openfgav1.CheckRequestTupleKey{
			User: "user:anne", Relation: "viewer", Object: "document:budget",
		},
	}

	handler := func(ctx context.Context, req any) (any, error) {
		return &openfgav1.CheckResponse{Allowed: true}, nil
	}

	info := &grpc.UnaryServerInfo{FullMethod: "/openfga.v1.OpenFGAService/Check"}
	resp, err := auditInterceptor(context.Background(), req, info, handler)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.(*openfgav1.CheckResponse).GetAllowed() {
		t.Fatal("expected allowed=true")
	}

	events := ms.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 audit event, got %d", len(events))
	}
	if events[0].API.Operation != "Check" {
		t.Fatalf("expected operation Check, got %s", events[0].API.Operation)
	}
	if events[0].Authorizations[0].Decision != "Allowed" {
		t.Fatalf("expected decision Allowed, got %s", events[0].Authorizations[0].Decision)
	}
}

func TestAuditUnary_UnknownMethod(t *testing.T) {
	ms := sink.NewMemorySink()
	auditInterceptor := interceptor.AuditUnary(ms)

	handler := func(ctx context.Context, req any) (any, error) {
		return nil, nil
	}

	info := &grpc.UnaryServerInfo{FullMethod: "/openfga.v1.OpenFGAService/SomeFutureMethod"}
	_, _ = auditInterceptor(context.Background(), "some-request", info, handler)

	events := ms.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 audit event, got %d", len(events))
	}
	if events[0].ActivityID != ocsf.ActivityIDOther {
		t.Fatalf("expected activity_id Other, got %d", events[0].ActivityID)
	}
	if events[0].API.Operation != "SomeFutureMethod" {
		t.Fatalf("expected operation SomeFutureMethod, got %s", events[0].API.Operation)
	}
}
