package sink

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	witnessconfig "github.com/openfga/openfga/witness/config"
	"github.com/openfga/openfga/witness/ocsf"
)

// KafkaWriter is the interface used by KafkaSink to produce messages.
// *kafka.Writer satisfies this interface.
type KafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// KafkaSink produces audit events to a Kafka topic.
type KafkaSink struct {
	writer    KafkaWriter
	closed    atomic.Bool
	closeOnce sync.Once
	mu        sync.RWMutex // guards Emit/Close coordination
}

// NewKafkaSink creates a KafkaSink from configuration.
func NewKafkaSink(cfg witnessconfig.KafkaConfig) (*KafkaSink, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: brokers are required")
	}

	var brokers []string
	for _, b := range cfg.Brokers {
		b = strings.TrimSpace(b)
		if b != "" {
			brokers = append(brokers, b)
		}
	}
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka: at least one non-empty broker is required")
	}

	if cfg.Topic == "" {
		return nil, fmt.Errorf("kafka: topic is required")
	}

	balancer, err := parseBalancer(cfg.Balancer)
	if err != nil {
		return nil, err
	}

	compression, err := parseCompression(cfg.Compression)
	if err != nil {
		return nil, err
	}

	requiredAcks, err := parseRequiredAcks(cfg.RequiredAcks)
	if err != nil {
		return nil, err
	}

	transport := &kafka.Transport{
		DialTimeout: cfg.WriteTimeout,
	}

	if cfg.TLS {
		transport.TLS = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	if cfg.SASLMechanism != "" {
		if cfg.SASLMechanism == "PLAIN" && !cfg.TLS {
			log.Printf("openfga-witness: WARNING: SASL PLAIN without TLS sends credentials in cleartext")
		}

		mechanism, err := parseSASL(cfg)
		if err != nil {
			return nil, err
		}
		transport.SASL = mechanism
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        cfg.Topic,
		Balancer:     balancer,
		Compression:  compression,
		Transport:    transport,
		RequiredAcks: requiredAcks,
		WriteTimeout: cfg.WriteTimeout,
	}

	return &KafkaSink{writer: w}, nil
}

func parseBalancer(name string) (kafka.Balancer, error) {
	switch name {
	case "", "round-robin":
		return &kafka.RoundRobin{}, nil
	case "least-bytes":
		return &kafka.LeastBytes{}, nil
	case "hash":
		return &kafka.Hash{}, nil
	default:
		return nil, fmt.Errorf("kafka: unknown balancer %q (valid: round-robin, least-bytes, hash)", name)
	}
}

func parseCompression(name string) (kafka.Compression, error) {
	switch name {
	case "":
		return 0, nil
	case "gzip":
		return kafka.Gzip, nil
	case "snappy":
		return kafka.Snappy, nil
	case "lz4":
		return kafka.Lz4, nil
	case "zstd":
		return kafka.Zstd, nil
	default:
		return 0, fmt.Errorf("kafka: unknown compression %q (valid: snappy, gzip, lz4, zstd)", name)
	}
}

func parseRequiredAcks(name string) (kafka.RequiredAcks, error) {
	switch name {
	case "none":
		return kafka.RequireNone, nil
	case "", "one":
		return kafka.RequireOne, nil
	case "all":
		return kafka.RequireAll, nil
	default:
		return 0, fmt.Errorf("kafka: unknown required-acks %q (valid: none, one, all)", name)
	}
}

func parseSASL(cfg witnessconfig.KafkaConfig) (sasl.Mechanism, error) {
	if cfg.SASLUsername == "" || cfg.SASLPassword == "" {
		return nil, fmt.Errorf("kafka: SASL username and password are required when mechanism is set")
	}

	switch cfg.SASLMechanism {
	case "PLAIN":
		return plain.Mechanism{
			Username: cfg.SASLUsername,
			Password: cfg.SASLPassword,
		}, nil
	case "SCRAM-SHA-256":
		return scram.Mechanism(scram.SHA256, cfg.SASLUsername, cfg.SASLPassword)
	case "SCRAM-SHA-512":
		return scram.Mechanism(scram.SHA512, cfg.SASLUsername, cfg.SASLPassword)
	default:
		return nil, fmt.Errorf("kafka: unknown SASL mechanism %q (valid: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", cfg.SASLMechanism)
	}
}

// NewKafkaSinkWithWriter creates a KafkaSink with an injected writer (for testing).
func NewKafkaSinkWithWriter(w KafkaWriter) *KafkaSink {
	return &KafkaSink{writer: w}
}

func (s *KafkaSink) Emit(ctx context.Context, event ocsf.APIActivityEvent) error {
	if s.closed.Load() {
		return fmt.Errorf("sink is closed")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed.Load() {
		return fmt.Errorf("sink is closed")
	}

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(event.Actor.User.UID),
		Value: data,
	}
	if err := s.writer.WriteMessages(ctx, msg); err != nil {
		log.Printf("openfga-witness: kafka WriteMessages failed: %v", err)
		return err
	}
	return nil
}

// Close flushes the writer's internal buffer and closes it.
// The write-lock ensures all in-flight Emit calls complete before closing.
// If the provided context expires, Close returns immediately with a timeout error.
func (s *KafkaSink) Close(ctx context.Context) error {
	var err error
	s.closeOnce.Do(func() {
		s.closed.Store(true)

		s.mu.Lock() // wait for in-flight Emits
		defer s.mu.Unlock()

		done := make(chan error, 1)
		go func() { done <- s.writer.Close() }()

		select {
		case err = <-done:
		case <-ctx.Done():
			err = fmt.Errorf("kafka sink close timed out: %w", ctx.Err())
		}
	})
	return err
}
