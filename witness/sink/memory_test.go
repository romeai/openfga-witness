package sink_test

import (
	"context"
	"testing"

	"github.com/openfga/openfga/witness/ocsf"
	"github.com/openfga/openfga/witness/sink"
)

func TestMemorySink_EmitCollectsEvents(t *testing.T) {
	s := sink.NewMemorySink()
	ctx := context.Background()

	event := ocsf.APIActivityEvent{
		ClassUID:   ocsf.ClassUIDAPIActivity,
		ActivityID: ocsf.ActivityIDRead,
		API:        ocsf.API{Operation: "Check"},
	}

	if err := s.Emit(ctx, event); err != nil {
		t.Fatalf("Emit failed: %v", err)
	}

	events := s.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].API.Operation != "Check" {
		t.Fatalf("expected operation Check, got %s", events[0].API.Operation)
	}
}

func TestMemorySink_Reset(t *testing.T) {
	s := sink.NewMemorySink()
	ctx := context.Background()

	_ = s.Emit(ctx, ocsf.APIActivityEvent{ClassUID: ocsf.ClassUIDAPIActivity})
	s.Reset()

	if len(s.Events()) != 0 {
		t.Fatal("expected 0 events after reset")
	}
}
