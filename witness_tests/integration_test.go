package witness_tests

import (
	"context"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/tests"
	"github.com/openfga/openfga/witness/ocsf"
)

func setupStoreAndModel(t *testing.T, client tests.ClientInterface) (storeID, modelID string) {
	t.Helper()
	ctx := context.Background()

	store, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: "test-store"})
	if err != nil {
		t.Fatalf("CreateStore: %v", err)
	}

	typeDefs := parser.MustTransformDSLToProto(`
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user]
	`)
	model, err := client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         store.GetId(),
		TypeDefinitions: typeDefs.GetTypeDefinitions(),
		Conditions:      typeDefs.GetConditions(),
		SchemaVersion:   typesystem.SchemaVersion1_1,
	})
	if err != nil {
		t.Fatalf("WriteAuthorizationModel: %v", err)
	}

	_, err = client.Write(ctx, &openfgav1.WriteRequest{
		StoreId:              store.GetId(),
		AuthorizationModelId: model.GetAuthorizationModelId(),
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{User: "user:anne", Relation: "viewer", Object: "document:budget"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	return store.GetId(), model.GetAuthorizationModelId()
}

func TestIntegration_CheckEmitsAuditEvent(t *testing.T) {
	client, ms := startTestServer(t)
	storeID, modelID := setupStoreAndModel(t, client)
	ctx := context.Background()

	ms.Reset()

	checkResp, err := client.Check(ctx, &openfgav1.CheckRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		TupleKey:             &openfgav1.CheckRequestTupleKey{User: "user:anne", Relation: "viewer", Object: "document:budget"},
	})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if !checkResp.GetAllowed() {
		t.Fatal("expected allowed=true")
	}

	var checkEvents []ocsf.APIActivityEvent
	for _, e := range ms.Events() {
		if e.Api.Operation == "Check" {
			checkEvents = append(checkEvents, e)
		}
	}
	if len(checkEvents) != 1 {
		t.Fatalf("expected 1 Check audit event, got %d (total events: %d)", len(checkEvents), len(ms.Events()))
	}

	e := checkEvents[0]
	if e.ClassUID != ocsf.ClassUIDAPIActivity {
		t.Fatalf("expected class_uid %d, got %d", ocsf.ClassUIDAPIActivity, e.ClassUID)
	}
	if len(e.Authorizations) == 0 {
		t.Fatal("expected at least one authorization entry")
	}
	if e.Authorizations[0].Decision != "Allowed" {
		t.Fatalf("expected decision Allowed, got %s", e.Authorizations[0].Decision)
	}
	if len(e.Resources) == 0 {
		t.Fatal("expected at least one resource entry")
	}
	if e.Resources[0].Data["user"] != "user:anne" {
		t.Fatalf("expected user user:anne, got %v", e.Resources[0].Data["user"])
	}
	if e.Duration < 0 {
		t.Fatal("expected non-negative duration")
	}
	if e.StatusID != ocsf.StatusIDSuccess {
		t.Fatalf("expected status_id %d, got %d", ocsf.StatusIDSuccess, e.StatusID)
	}
}

func TestIntegration_WriteEmitsAuditEvent(t *testing.T) {
	client, ms := startTestServer(t)
	storeID, modelID := setupStoreAndModel(t, client)
	ctx := context.Background()

	ms.Reset()

	_, err := client.Write(ctx, &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{User: "user:bob", Relation: "viewer", Object: "document:roadmap"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	var writeEvents []ocsf.APIActivityEvent
	for _, e := range ms.Events() {
		if e.Api.Operation == "Write" {
			writeEvents = append(writeEvents, e)
		}
	}
	if len(writeEvents) != 1 {
		t.Fatalf("expected 1 Write audit event, got %d", len(writeEvents))
	}

	e := writeEvents[0]
	if e.ClassUID != ocsf.ClassUIDAPIActivity {
		t.Fatalf("expected class_uid %d, got %d", ocsf.ClassUIDAPIActivity, e.ClassUID)
	}
	if e.ActivityID != ocsf.ActivityIDCreate {
		t.Fatalf("expected activity_id %d (Create), got %d", ocsf.ActivityIDCreate, e.ActivityID)
	}
	if len(e.Resources) == 0 {
		t.Fatal("expected at least one resource entry")
	}
	if e.Resources[0].Data["user"] != "user:bob" {
		t.Fatalf("expected user user:bob, got %v", e.Resources[0].Data["user"])
	}
	if e.Resources[0].Data["object"] != "document:roadmap" {
		t.Fatalf("expected object document:roadmap, got %v", e.Resources[0].Data["object"])
	}
}

func TestIntegration_CheckDeniedEmitsAuditEvent(t *testing.T) {
	client, ms := startTestServer(t)
	storeID, modelID := setupStoreAndModel(t, client)
	ctx := context.Background()

	ms.Reset()

	checkResp, err := client.Check(ctx, &openfgav1.CheckRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		TupleKey:             &openfgav1.CheckRequestTupleKey{User: "user:unknown", Relation: "viewer", Object: "document:budget"},
	})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if checkResp.GetAllowed() {
		t.Fatal("expected allowed=false for unknown user")
	}

	var checkEvents []ocsf.APIActivityEvent
	for _, e := range ms.Events() {
		if e.Api.Operation == "Check" {
			checkEvents = append(checkEvents, e)
		}
	}
	if len(checkEvents) != 1 {
		t.Fatalf("expected 1 Check audit event, got %d", len(checkEvents))
	}

	e := checkEvents[0]
	if len(e.Authorizations) == 0 {
		t.Fatal("expected at least one authorization entry")
	}
	if e.Authorizations[0].Decision != "Denied" {
		t.Fatalf("expected decision Denied, got %s", e.Authorizations[0].Decision)
	}
	if e.DispositionID == nil || *e.DispositionID != ocsf.DispositionIDBlocked {
		t.Fatalf("expected disposition_id %d, got %v", ocsf.DispositionIDBlocked, e.DispositionID)
	}
}

func TestIntegration_CreateStoreEmitsGenericEvent(t *testing.T) {
	client, ms := startTestServer(t)
	ctx := context.Background()

	ms.Reset()

	_, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: "audit-test-store"})
	if err != nil {
		t.Fatalf("CreateStore: %v", err)
	}

	var createEvents []ocsf.APIActivityEvent
	for _, e := range ms.Events() {
		if e.Api.Operation == "CreateStore" {
			createEvents = append(createEvents, e)
		}
	}
	if len(createEvents) != 1 {
		t.Fatalf("expected 1 CreateStore audit event, got %d", len(createEvents))
	}

	e := createEvents[0]
	if e.ClassUID != ocsf.ClassUIDAPIActivity {
		t.Fatalf("expected class_uid %d, got %d", ocsf.ClassUIDAPIActivity, e.ClassUID)
	}
	if e.StatusID != ocsf.StatusIDSuccess {
		t.Fatalf("expected status_id %d, got %d", ocsf.StatusIDSuccess, e.StatusID)
	}
}
