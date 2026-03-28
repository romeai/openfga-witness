package sink_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/openfga/openfga/witness/ocsf"
	"github.com/openfga/openfga/witness/sink"
)

func TestStdoutSink_EmitsJSONLine(t *testing.T) {
	var buf bytes.Buffer
	s := sink.NewStdoutSink(&buf)
	ctx := context.Background()

	event := ocsf.APIActivityEvent{
		ClassUID:    ocsf.ClassUIDAPIActivity,
		CategoryUID: ocsf.CategoryUIDApplication,
		ActivityID:  ocsf.ActivityIDRead,
		API:         ocsf.API{Operation: "Check"},
		Duration:    42,
	}

	if err := s.Emit(ctx, event); err != nil {
		t.Fatalf("Emit failed: %v", err)
	}

	var decoded ocsf.APIActivityEvent
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	if decoded.ClassUID != ocsf.ClassUIDAPIActivity {
		t.Fatalf("expected class_uid %d, got %d", ocsf.ClassUIDAPIActivity, decoded.ClassUID)
	}
	if decoded.API.Operation != "Check" {
		t.Fatalf("expected operation Check, got %s", decoded.API.Operation)
	}
}

func TestStdoutSink_EndsWithNewline(t *testing.T) {
	var buf bytes.Buffer
	s := sink.NewStdoutSink(&buf)

	_ = s.Emit(context.Background(), ocsf.APIActivityEvent{ClassUID: ocsf.ClassUIDAPIActivity})

	output := buf.Bytes()
	if len(output) == 0 || output[len(output)-1] != '\n' {
		t.Fatal("expected output to end with newline")
	}
}
