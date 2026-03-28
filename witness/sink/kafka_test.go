package sink_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/segmentio/kafka-go"

	witnessconfig "github.com/openfga/openfga/witness/config"
	"github.com/openfga/openfga/witness/ocsf"
	"github.com/openfga/openfga/witness/sink"
)

// mockKafkaWriter captures messages written via WriteMessages.
type mockKafkaWriter struct {
	mu       sync.Mutex
	messages []kafka.Message
	closed   bool
}

func (m *mockKafkaWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msgs...)
	return nil
}

func (m *mockKafkaWriter) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockKafkaWriter) Messages() []kafka.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]kafka.Message, len(m.messages))
	copy(cp, m.messages)
	return cp
}

func (m *mockKafkaWriter) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func TestKafkaSink_EmitSerializesJSONWithCorrectKey(t *testing.T) {
	mock := &mockKafkaWriter{}
	s := sink.NewKafkaSinkWithWriter(mock)
	ctx := context.Background()

	event := ocsf.APIActivityEvent{
		ClassUID: ocsf.ClassUIDAPIActivity,
		API:      ocsf.API{Operation: "Check"},
		Actor:    ocsf.Actor{User: ocsf.ActorUser{UID: "user:alice"}},
	}

	if err := s.Emit(ctx, event); err != nil {
		t.Fatalf("Emit failed: %v", err)
	}

	msgs := mock.Messages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	// Verify key is Actor.User.UID
	if string(msgs[0].Key) != "user:alice" {
		t.Fatalf("expected key 'user:alice', got %q", string(msgs[0].Key))
	}

	// Verify value is valid JSON that round-trips back to the event
	var decoded ocsf.APIActivityEvent
	if err := json.Unmarshal(msgs[0].Value, &decoded); err != nil {
		t.Fatalf("value is not valid JSON: %v", err)
	}
	if decoded.API.Operation != "Check" {
		t.Fatalf("expected operation 'Check', got %q", decoded.API.Operation)
	}
	if decoded.Actor.User.UID != "user:alice" {
		t.Fatalf("expected actor UID 'user:alice', got %q", decoded.Actor.User.UID)
	}
}

func TestKafkaSink_EmitAfterCloseReturnsError(t *testing.T) {
	mock := &mockKafkaWriter{}
	s := sink.NewKafkaSinkWithWriter(mock)
	ctx := context.Background()

	_ = s.Close(ctx)

	err := s.Emit(ctx, ocsf.APIActivityEvent{ClassUID: ocsf.ClassUIDAPIActivity})
	if err == nil {
		t.Fatal("expected error from Emit after Close")
	}
}

func TestKafkaSink_CloseCallsWriterClose(t *testing.T) {
	mock := &mockKafkaWriter{}
	s := sink.NewKafkaSinkWithWriter(mock)
	ctx := context.Background()

	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if !mock.IsClosed() {
		t.Fatal("expected writer to be closed")
	}
}

func TestNewKafkaSink_RejectsEmptyBrokers(t *testing.T) {
	cfg := witnessconfig.KafkaConfig{
		Topic: "audit-events",
	}
	_, err := sink.NewKafkaSink(cfg)
	if err == nil {
		t.Fatal("expected error for empty brokers")
	}
}

func TestNewKafkaSink_RejectsEmptyTopic(t *testing.T) {
	cfg := witnessconfig.KafkaConfig{
		Brokers: []string{"localhost:9092"},
	}
	_, err := sink.NewKafkaSink(cfg)
	if err == nil {
		t.Fatal("expected error for empty topic")
	}
}

func TestNewKafkaSink_RejectsInvalidSASLMechanism(t *testing.T) {
	cfg := witnessconfig.KafkaConfig{
		Brokers:       []string{"localhost:9092"},
		Topic:         "audit-events",
		SASLMechanism: "INVALID",
	}
	_, err := sink.NewKafkaSink(cfg)
	if err == nil {
		t.Fatal("expected error for invalid SASL mechanism")
	}
}

func TestNewKafkaSink_RejectsInvalidCompression(t *testing.T) {
	cfg := witnessconfig.KafkaConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       "audit-events",
		Compression: "brotli",
	}
	_, err := sink.NewKafkaSink(cfg)
	if err == nil {
		t.Fatal("expected error for invalid compression")
	}
}

func TestNewKafkaSink_RejectsInvalidBalancer(t *testing.T) {
	cfg := witnessconfig.KafkaConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "audit-events",
		Balancer: "random",
	}
	_, err := sink.NewKafkaSink(cfg)
	if err == nil {
		t.Fatal("expected error for invalid balancer")
	}
}

func TestNewKafkaSink_ValidMinimalConfig(t *testing.T) {
	cfg := witnessconfig.KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "audit-events",
	}
	s, err := sink.NewKafkaSink(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = s.Close(context.Background())
}

// --- Additional mock writers ---

// failingKafkaWriter always returns an error from WriteMessages.
type failingKafkaWriter struct {
	err error
}

func (m *failingKafkaWriter) WriteMessages(_ context.Context, _ ...kafka.Message) error {
	return m.err
}

func (m *failingKafkaWriter) Close() error {
	return nil
}

// failingCloseWriter returns an error from Close.
type failingCloseWriter struct {
	mockKafkaWriter
	closeErr error
}

func (m *failingCloseWriter) Close() error {
	return m.closeErr
}

// --- New tests ---

func TestKafkaSink_DoubleCloseDoesNotPanic(t *testing.T) {
	mock := &mockKafkaWriter{}
	s := sink.NewKafkaSinkWithWriter(mock)
	ctx := context.Background()
	_ = s.Close(ctx)
	_ = s.Close(ctx) // must not panic
}

func TestKafkaSink_ConcurrentEmit(t *testing.T) {
	mock := &mockKafkaWriter{}
	s := sink.NewKafkaSinkWithWriter(mock)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			event := ocsf.APIActivityEvent{
				ClassUID: ocsf.ClassUIDAPIActivity,
				Actor:    ocsf.Actor{User: ocsf.ActorUser{UID: fmt.Sprintf("user:%d", n)}},
			}
			_ = s.Emit(ctx, event)
		}(i)
	}
	wg.Wait()

	msgs := mock.Messages()
	if len(msgs) != 100 {
		t.Fatalf("expected 100 messages, got %d", len(msgs))
	}
	_ = s.Close(ctx)
}

func TestKafkaSink_EmitPropagatesWriteError(t *testing.T) {
	mock := &failingKafkaWriter{err: fmt.Errorf("broker unavailable")}
	s := sink.NewKafkaSinkWithWriter(mock)

	err := s.Emit(context.Background(), ocsf.APIActivityEvent{ClassUID: ocsf.ClassUIDAPIActivity})
	if err == nil {
		t.Fatal("expected error from Emit when writer fails")
	}
	if err.Error() != "broker unavailable" {
		t.Fatalf("expected 'broker unavailable', got %q", err.Error())
	}
	_ = s.Close(context.Background())
}

func TestKafkaSink_ClosePropagatesWriterError(t *testing.T) {
	mock := &failingCloseWriter{closeErr: fmt.Errorf("flush failed")}
	s := sink.NewKafkaSinkWithWriter(mock)

	err := s.Close(context.Background())
	if err == nil {
		t.Fatal("expected error from Close")
	}
	if err.Error() != "flush failed" {
		t.Fatalf("expected 'flush failed', got %q", err.Error())
	}
}

func TestNewKafkaSink_ValidSASLPlain(t *testing.T) {
	cfg := witnessconfig.KafkaConfig{
		Brokers:       []string{"localhost:9092"},
		Topic:         "audit",
		SASLMechanism: "PLAIN",
		SASLUsername:   "user",
		SASLPassword:   "pass",
	}
	s, err := sink.NewKafkaSink(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = s.Close(context.Background())
}

func TestNewKafkaSink_ValidSASLScram256(t *testing.T) {
	cfg := witnessconfig.KafkaConfig{
		Brokers:       []string{"localhost:9092"},
		Topic:         "audit",
		SASLMechanism: "SCRAM-SHA-256",
		SASLUsername:   "user",
		SASLPassword:   "pass",
	}
	s, err := sink.NewKafkaSink(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = s.Close(context.Background())
}

func TestNewKafkaSink_ValidSASLScram512(t *testing.T) {
	cfg := witnessconfig.KafkaConfig{
		Brokers:       []string{"localhost:9092"},
		Topic:         "audit",
		SASLMechanism: "SCRAM-SHA-512",
		SASLUsername:   "user",
		SASLPassword:   "pass",
	}
	s, err := sink.NewKafkaSink(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = s.Close(context.Background())
}

func TestNewKafkaSink_RejectsSASLWithEmptyCredentials(t *testing.T) {
	cfg := witnessconfig.KafkaConfig{
		Brokers:       []string{"localhost:9092"},
		Topic:         "audit",
		SASLMechanism: "PLAIN",
		// no username or password
	}
	_, err := sink.NewKafkaSink(cfg)
	if err == nil {
		t.Fatal("expected error for SASL with empty credentials")
	}
}

func TestNewKafkaSink_ValidTLSConfig(t *testing.T) {
	cfg := witnessconfig.KafkaConfig{
		Brokers: []string{"localhost:9093"},
		Topic:   "audit",
		TLS:     true,
	}
	s, err := sink.NewKafkaSink(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = s.Close(context.Background())
}

func TestNewKafkaSink_ValidCompressionCodecs(t *testing.T) {
	for _, codec := range []string{"snappy", "gzip", "lz4", "zstd"} {
		t.Run(codec, func(t *testing.T) {
			cfg := witnessconfig.KafkaConfig{
				Brokers:     []string{"localhost:9092"},
				Topic:       "audit",
				Compression: codec,
			}
			s, err := sink.NewKafkaSink(cfg)
			if err != nil {
				t.Fatalf("unexpected error for compression %q: %v", codec, err)
			}
			_ = s.Close(context.Background())
		})
	}
}

func TestNewKafkaSink_ValidBalancers(t *testing.T) {
	for _, balancer := range []string{"round-robin", "least-bytes", "hash"} {
		t.Run(balancer, func(t *testing.T) {
			cfg := witnessconfig.KafkaConfig{
				Brokers:  []string{"localhost:9092"},
				Topic:    "audit",
				Balancer: balancer,
			}
			s, err := sink.NewKafkaSink(cfg)
			if err != nil {
				t.Fatalf("unexpected error for balancer %q: %v", balancer, err)
			}
			_ = s.Close(context.Background())
		})
	}
}

func TestNewKafkaSink_FiltersEmptyBrokers(t *testing.T) {
	cfg := witnessconfig.KafkaConfig{
		Brokers: []string{"localhost:9092", "", "  ", "broker2:9092"},
		Topic:   "audit",
	}
	s, err := sink.NewKafkaSink(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = s.Close(context.Background())
}

func TestNewKafkaSink_RejectsAllEmptyBrokers(t *testing.T) {
	cfg := witnessconfig.KafkaConfig{
		Brokers: []string{"", "  "},
		Topic:   "audit",
	}
	_, err := sink.NewKafkaSink(cfg)
	if err == nil {
		t.Fatal("expected error when all brokers are empty")
	}
}

func TestKafkaSink_MultipleEmits(t *testing.T) {
	mock := &mockKafkaWriter{}
	s := sink.NewKafkaSinkWithWriter(mock)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		event := ocsf.APIActivityEvent{
			ClassUID: ocsf.ClassUIDAPIActivity,
			Actor:    ocsf.Actor{User: ocsf.ActorUser{UID: fmt.Sprintf("user:%d", i)}},
		}
		if err := s.Emit(ctx, event); err != nil {
			t.Fatalf("Emit %d failed: %v", i, err)
		}
	}

	msgs := mock.Messages()
	if len(msgs) != 5 {
		t.Fatalf("expected 5 messages, got %d", len(msgs))
	}
	_ = s.Close(ctx)
}

func TestNewKafkaSink_ValidRequiredAcks(t *testing.T) {
	for _, acks := range []string{"none", "one", "all"} {
		t.Run(acks, func(t *testing.T) {
			cfg := witnessconfig.KafkaConfig{
				Brokers:      []string{"localhost:9092"},
				Topic:        "audit",
				RequiredAcks: acks,
			}
			s, err := sink.NewKafkaSink(cfg)
			if err != nil {
				t.Fatalf("unexpected error for acks %q: %v", acks, err)
			}
			_ = s.Close(context.Background())
		})
	}
}

func TestNewKafkaSink_RejectsInvalidRequiredAcks(t *testing.T) {
	cfg := witnessconfig.KafkaConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "audit",
		RequiredAcks: "maybe",
	}
	_, err := sink.NewKafkaSink(cfg)
	if err == nil {
		t.Fatal("expected error for invalid required-acks")
	}
}
