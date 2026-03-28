package sink_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/openfga/openfga/witness/ocsf"
	"github.com/openfga/openfga/witness/sink"
)

type mockFirehoseClient struct {
	mu      sync.Mutex
	batches [][]json.RawMessage
	flushed chan struct{}
}

func newMockFirehoseClient() *mockFirehoseClient {
	return &mockFirehoseClient{flushed: make(chan struct{}, 100)}
}

func (m *mockFirehoseClient) PutRecordBatch(ctx context.Context, records []json.RawMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	batch := make([]json.RawMessage, len(records))
	copy(batch, records)
	m.batches = append(m.batches, batch)
	m.flushed <- struct{}{}
	return nil
}

func (m *mockFirehoseClient) BatchCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.batches)
}

func (m *mockFirehoseClient) RecordCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0
	for _, b := range m.batches {
		total += len(b)
	}
	return total
}

func TestFirehoseSink_FlushOnBatchSize(t *testing.T) {
	mock := newMockFirehoseClient()
	s := sink.NewFirehoseSinkWithClient(mock, 2, 10*time.Second)
	ctx := context.Background()
	event := ocsf.APIActivityEvent{ClassUID: ocsf.ClassUIDAPIActivity, API: ocsf.API{Operation: "Check"}}

	_ = s.Emit(ctx, event)
	if mock.BatchCount() != 0 {
		t.Fatal("should not flush before batch size reached")
	}

	_ = s.Emit(ctx, event)

	// Wait for async flush signal
	select {
	case <-mock.flushed:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for flush")
	}

	if mock.BatchCount() != 1 {
		t.Fatalf("expected 1 batch, got %d", mock.BatchCount())
	}
	if mock.RecordCount() != 2 {
		t.Fatalf("expected 2 records, got %d", mock.RecordCount())
	}

	_ = s.Close(ctx)
}

func TestFirehoseSink_FlushOnClose(t *testing.T) {
	mock := newMockFirehoseClient()
	s := sink.NewFirehoseSinkWithClient(mock, 100, 10*time.Second)
	ctx := context.Background()

	_ = s.Emit(ctx, ocsf.APIActivityEvent{ClassUID: ocsf.ClassUIDAPIActivity})
	_ = s.Close(ctx)

	if mock.BatchCount() != 1 {
		t.Fatalf("expected 1 batch after Close, got %d", mock.BatchCount())
	}
	if mock.RecordCount() != 1 {
		t.Fatalf("expected 1 record in final batch, got %d", mock.RecordCount())
	}
}

func TestFirehoseSink_DoubleCloseDoesNotPanic(t *testing.T) {
	mock := newMockFirehoseClient()
	s := sink.NewFirehoseSinkWithClient(mock, 100, 10*time.Second)
	ctx := context.Background()

	_ = s.Close(ctx)
	_ = s.Close(ctx) // must not panic
}

func TestFirehoseSink_EmitAfterCloseReturnsError(t *testing.T) {
	mock := newMockFirehoseClient()
	s := sink.NewFirehoseSinkWithClient(mock, 100, 10*time.Second)
	ctx := context.Background()

	_ = s.Close(ctx)

	err := s.Emit(ctx, ocsf.APIActivityEvent{ClassUID: ocsf.ClassUIDAPIActivity})
	if err == nil {
		t.Fatal("expected error from Emit after Close")
	}
}

func TestFirehoseSink_CloseRespectsContextDeadline(t *testing.T) {
	// Use a client that blocks until context is cancelled
	slowClient := &slowFirehoseClient{delay: 5 * time.Second}
	s := sink.NewFirehoseSinkWithClient(slowClient, 100, 10*time.Second)

	_ = s.Emit(context.Background(), ocsf.APIActivityEvent{ClassUID: ocsf.ClassUIDAPIActivity})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := s.Close(ctx)
	if err == nil {
		t.Fatal("expected error from Close with expired context")
	}
}

type slowFirehoseClient struct {
	delay time.Duration
}

func (c *slowFirehoseClient) PutRecordBatch(ctx context.Context, records []json.RawMessage) error {
	select {
	case <-time.After(c.delay):
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context expired: %w", ctx.Err())
	}
}
