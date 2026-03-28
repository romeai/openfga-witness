package config_test

import (
	"testing"
	"time"

	"github.com/openfga/openfga/witness/config"
)

func TestDefaultAuditConfig_HasKafkaDefaults(t *testing.T) {
	cfg := config.DefaultAuditConfig()
	if cfg.Kafka.Balancer != "round-robin" {
		t.Fatalf("expected default balancer 'round-robin', got %q", cfg.Kafka.Balancer)
	}
	if cfg.Kafka.TLS {
		t.Fatal("expected default TLS to be false")
	}
}

func TestDefaultAuditConfig_HasKafkaRequiredAcksDefault(t *testing.T) {
	cfg := config.DefaultAuditConfig()
	if cfg.Kafka.RequiredAcks != "all" {
		t.Fatalf("expected default required-acks 'all', got %q", cfg.Kafka.RequiredAcks)
	}
}

func TestDefaultAuditConfig_HasKafkaWriteTimeoutDefault(t *testing.T) {
	cfg := config.DefaultAuditConfig()
	if cfg.Kafka.WriteTimeout != 30*time.Second {
		t.Fatalf("expected default write timeout 30s, got %v", cfg.Kafka.WriteTimeout)
	}
}
