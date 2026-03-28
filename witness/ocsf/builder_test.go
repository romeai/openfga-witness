package ocsf_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc/peer"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/witness/ocsf"
)

func TestBuildCheckEvent_Allowed(t *testing.T) {
	ctx := context.Background()
	ctx = peer.NewContext(ctx, &peer.Peer{Addr: &net.TCPAddr{IP: net.IPv4(10, 0, 1, 42), Port: 52431}})
	start := time.Now().Add(-12 * time.Millisecond)

	req := &openfgav1.CheckRequest{
		StoreId:              "store-123",
		AuthorizationModelId: "model-456",
		TupleKey: &openfgav1.CheckRequestTupleKey{
			User:     "user:anne",
			Relation: "viewer",
			Object:   "document:budget",
		},
	}
	resp := &openfgav1.CheckResponse{Allowed: true}
	latency := 12 * time.Millisecond

	event := ocsf.BuildCheckEvent(ctx, start, req, resp, nil, latency)

	if event.ClassUID != ocsf.ClassUIDAPIActivity {
		t.Fatalf("expected class_uid %d, got %d", ocsf.ClassUIDAPIActivity, event.ClassUID)
	}
	if event.ActivityID != ocsf.ActivityIDRead {
		t.Fatalf("expected activity_id %d, got %d", ocsf.ActivityIDRead, event.ActivityID)
	}
	if event.TypeName != "API Activity: Read" {
		t.Fatalf("expected type_name 'API Activity: Read', got %s", event.TypeName)
	}
	if event.API.Operation != "Check" {
		t.Fatalf("expected operation Check, got %s", event.API.Operation)
	}
	if event.StatusID != ocsf.StatusIDSuccess {
		t.Fatalf("expected status_id %d, got %d", ocsf.StatusIDSuccess, event.StatusID)
	}
	if event.Duration != 12 {
		t.Fatalf("expected duration 12, got %d", event.Duration)
	}
	if event.SrcEndpoint.IP != "10.0.1.42" {
		t.Fatalf("expected src ip 10.0.1.42, got %s", event.SrcEndpoint.IP)
	}
	if event.SrcEndpoint.Port != 52431 {
		t.Fatalf("expected src port 52431, got %d", event.SrcEndpoint.Port)
	}
	if len(event.Resources) != 1 {
		t.Fatalf("expected 1 resource, got %d", len(event.Resources))
	}
	if event.Resources[0].Data["user"] != "user:anne" {
		t.Fatalf("expected user:anne, got %v", event.Resources[0].Data["user"])
	}
	if len(event.Authorizations) != 1 || event.Authorizations[0].Decision != "Allowed" {
		t.Fatalf("expected authorization decision Allowed, got %v", event.Authorizations)
	}
	if event.DispositionID == nil || *event.DispositionID != ocsf.DispositionIDAllowed {
		t.Fatal("expected disposition_id Allowed")
	}
	if event.Metadata.TenantUID != "store-123" {
		t.Fatalf("expected tenant_uid store-123, got %s", event.Metadata.TenantUID)
	}
}

func TestBuildCheckEvent_Denied(t *testing.T) {
	ctx := context.Background()
	start := time.Now()

	req := &openfgav1.CheckRequest{
		StoreId: "store-123",
		TupleKey: &openfgav1.CheckRequestTupleKey{
			User: "user:bob", Relation: "editor", Object: "document:secret",
		},
	}
	resp := &openfgav1.CheckResponse{Allowed: false}

	event := ocsf.BuildCheckEvent(ctx, start, req, resp, nil, 5*time.Millisecond)

	if event.Authorizations[0].Decision != "Denied" {
		t.Fatalf("expected decision Denied, got %s", event.Authorizations[0].Decision)
	}
	if event.DispositionID == nil || *event.DispositionID != ocsf.DispositionIDBlocked {
		t.Fatal("expected disposition_id Blocked")
	}
}

func TestBuildGenericEvent(t *testing.T) {
	ctx := context.Background()
	start := time.Now()

	event := ocsf.BuildGenericEvent(ctx, start, "/openfga.v1.OpenFGAService/WriteAuthorizationModel", nil, 3*time.Millisecond)

	if event.ClassUID != ocsf.ClassUIDAPIActivity {
		t.Fatalf("expected class_uid %d, got %d", ocsf.ClassUIDAPIActivity, event.ClassUID)
	}
	if event.ActivityID != ocsf.ActivityIDOther {
		t.Fatalf("expected activity_id %d, got %d", ocsf.ActivityIDOther, event.ActivityID)
	}
	if event.API.Operation != "WriteAuthorizationModel" {
		t.Fatalf("expected operation WriteAuthorizationModel, got %s", event.API.Operation)
	}
	if event.TypeName != "API Activity: Other" {
		t.Fatalf("expected type_name 'API Activity: Other', got %s", event.TypeName)
	}
}

func TestBuildWriteEvent_Creates(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	req := &openfgav1.WriteRequest{
		StoreId: "store-123",
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{User: "user:anne", Relation: "viewer", Object: "document:budget"},
				{User: "user:bob", Relation: "editor", Object: "document:budget"},
			},
		},
	}

	event := ocsf.BuildWriteEvent(ctx, start, req, nil, 5*time.Millisecond)

	if event.ActivityID != ocsf.ActivityIDCreate {
		t.Fatalf("expected activity_id Create, got %d", event.ActivityID)
	}
	if len(event.Resources) != 2 {
		t.Fatalf("expected 2 resources, got %d", len(event.Resources))
	}
}

func TestBuildWriteEvent_Deletes(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	req := &openfgav1.WriteRequest{
		StoreId: "store-123",
		Deletes: &openfgav1.WriteRequestDeletes{
			TupleKeys: []*openfgav1.TupleKeyWithoutCondition{
				{User: "user:anne", Relation: "viewer", Object: "document:budget"},
			},
		},
	}

	event := ocsf.BuildWriteEvent(ctx, start, req, nil, 3*time.Millisecond)

	if event.ActivityID != ocsf.ActivityIDDelete {
		t.Fatalf("expected activity_id Delete, got %d", event.ActivityID)
	}
}

func TestBuildWriteEvent_Mixed(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	req := &openfgav1.WriteRequest{
		StoreId: "store-123",
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{User: "user:anne", Relation: "viewer", Object: "document:budget"},
			},
		},
		Deletes: &openfgav1.WriteRequestDeletes{
			TupleKeys: []*openfgav1.TupleKeyWithoutCondition{
				{User: "user:bob", Relation: "viewer", Object: "document:budget"},
			},
		},
	}

	event := ocsf.BuildWriteEvent(ctx, start, req, nil, 5*time.Millisecond)

	if event.ActivityID != ocsf.ActivityIDUpdate {
		t.Fatalf("expected activity_id Update for mixed write, got %d", event.ActivityID)
	}
	if len(event.Resources) != 2 {
		t.Fatalf("expected 2 resources, got %d", len(event.Resources))
	}
}

func TestBuildListObjectsEvent(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	req := &openfgav1.ListObjectsRequest{
		StoreId: "store-123", Type: "document", Relation: "viewer", User: "user:anne",
	}
	resp := &openfgav1.ListObjectsResponse{
		Objects: []string{"document:a", "document:b"},
	}

	event := ocsf.BuildListObjectsEvent(ctx, start, req, resp, nil, 20*time.Millisecond)

	if event.API.Operation != "ListObjects" {
		t.Fatalf("expected operation ListObjects, got %s", event.API.Operation)
	}
	if event.Resources[0].Data["user"] != "user:anne" {
		t.Fatalf("expected user:anne, got %v", event.Resources[0].Data["user"])
	}
	if event.Unmapped["result_count"] != 2 {
		t.Fatalf("expected result_count 2, got %v", event.Unmapped["result_count"])
	}
}

func TestBuildListUsersEvent(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	req := &openfgav1.ListUsersRequest{
		StoreId:  "store-123",
		Relation: "viewer",
		Object:   &openfgav1.Object{Type: "document", Id: "budget"},
	}
	resp := &openfgav1.ListUsersResponse{
		Users: []*openfgav1.User{
			{User: &openfgav1.User_Object{Object: &openfgav1.Object{Type: "user", Id: "anne"}}},
		},
	}

	event := ocsf.BuildListUsersEvent(ctx, start, req, resp, nil, 15*time.Millisecond)

	if event.API.Operation != "ListUsers" {
		t.Fatalf("expected operation ListUsers, got %s", event.API.Operation)
	}
	if event.Resources[0].Name != "document:budget" {
		t.Fatalf("expected document:budget, got %s", event.Resources[0].Name)
	}
	if event.Unmapped["result_count"] != 1 {
		t.Fatalf("expected result_count 1, got %v", event.Unmapped["result_count"])
	}
}

func TestBuildBatchCheckEvent(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	req := &openfgav1.BatchCheckRequest{
		StoreId: "store-123",
		Checks: []*openfgav1.BatchCheckItem{
			{
				TupleKey:      &openfgav1.CheckRequestTupleKey{User: "user:anne", Relation: "viewer", Object: "document:a"},
				CorrelationId: "corr-1",
			},
			{
				TupleKey:      &openfgav1.CheckRequestTupleKey{User: "user:bob", Relation: "viewer", Object: "document:b"},
				CorrelationId: "corr-2",
			},
		},
	}
	resp := &openfgav1.BatchCheckResponse{
		Result: map[string]*openfgav1.BatchCheckSingleResult{
			"corr-1": {CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: true}},
			"corr-2": {CheckResult: &openfgav1.BatchCheckSingleResult_Allowed{Allowed: false}},
		},
	}

	event := ocsf.BuildBatchCheckEvent(ctx, start, req, resp, nil, 25*time.Millisecond)

	if event.API.Operation != "BatchCheck" {
		t.Fatalf("expected operation BatchCheck, got %s", event.API.Operation)
	}
	if len(event.Resources) != 2 {
		t.Fatalf("expected 2 resources, got %d", len(event.Resources))
	}
	if len(event.Authorizations) != 2 {
		t.Fatalf("expected 2 authorizations, got %d", len(event.Authorizations))
	}
}

func TestBuildReadEvent(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	req := &openfgav1.ReadRequest{
		StoreId: "store-123",
		TupleKey: &openfgav1.ReadRequestTupleKey{
			User: "user:anne", Relation: "viewer", Object: "document:budget",
		},
	}

	event := ocsf.BuildReadEvent(ctx, start, req, nil, 8*time.Millisecond)

	if event.API.Operation != "Read" {
		t.Fatalf("expected operation Read, got %s", event.API.Operation)
	}
	if event.Resources[0].Data["user"] != "user:anne" {
		t.Fatalf("expected user:anne, got %v", event.Resources[0].Data["user"])
	}
}
