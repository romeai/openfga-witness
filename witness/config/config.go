package config

import (
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type AuditConfig struct {
	Sink     string         `mapstructure:"sink"`
	Firehose FirehoseConfig `mapstructure:"firehose"`
	Kafka    KafkaConfig    `mapstructure:"kafka"`
}

type FirehoseConfig struct {
	StreamName    string        `mapstructure:"streamName"`
	BatchSize     int           `mapstructure:"batchSize"`
	FlushInterval time.Duration `mapstructure:"flushInterval"`
	Region        string        `mapstructure:"region"`
}

type KafkaConfig struct {
	Brokers       []string      `mapstructure:"brokers"`
	Topic         string        `mapstructure:"topic"`
	TLS           bool          `mapstructure:"tls"`
	SASLMechanism string        `mapstructure:"saslMechanism"`
	SASLUsername  string        `mapstructure:"saslUsername"`
	SASLPassword  string        `mapstructure:"saslPassword"`
	Compression   string        `mapstructure:"compression"`
	Balancer      string        `mapstructure:"balancer"`
	WriteTimeout  time.Duration `mapstructure:"writeTimeout"`
	RequiredAcks  string        `mapstructure:"requiredAcks"`
}

func DefaultAuditConfig() AuditConfig {
	return AuditConfig{
		Sink: "stdout",
		Firehose: FirehoseConfig{
			BatchSize:     500,
			FlushInterval: 5 * time.Second,
		},
		Kafka: KafkaConfig{
			Balancer:     "round-robin",
			WriteTimeout: 30 * time.Second,
			RequiredAcks: "all",
		},
	}
}

func ReadAuditConfig() AuditConfig {
	cfg := DefaultAuditConfig()
	_ = viper.UnmarshalKey("audit", &cfg)

	// viper binds the comma-separated string flag; split into []string.
	if raw := viper.GetString("audit.kafka.brokers"); raw != "" {
		cfg.Kafka.Brokers = strings.Split(raw, ",")
	}
	return cfg
}

func BindAuditFlags(cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.String("audit-sink", "stdout", "Audit sink type: stdout, firehose, kafka")
	flags.String("audit-firehose-stream-name", "", "AWS Firehose delivery stream name")
	flags.Int("audit-firehose-batch-size", 500, "Max records per Firehose PutRecordBatch")
	flags.Duration("audit-firehose-flush-interval", 5*time.Second, "Max time before flushing partial batch")
	flags.String("audit-firehose-region", "", "AWS region for Firehose")

	flags.String("audit-kafka-brokers", "", "Comma-separated Kafka broker addresses")
	flags.String("audit-kafka-topic", "", "Kafka topic for audit events")
	flags.Bool("audit-kafka-tls", false, "Enable TLS for Kafka connections")
	flags.String("audit-kafka-sasl-mechanism", "", "SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512")
	flags.String("audit-kafka-sasl-username", "", "SASL username")
	flags.String("audit-kafka-sasl-password", "", "SASL password (prefer OPENFGA_AUDIT_KAFKA_SASLPASSWORD env var)")
	flags.String("audit-kafka-compression", "", "Compression codec: snappy, gzip, lz4, zstd")
	flags.String("audit-kafka-balancer", "round-robin", "Partition balancer: round-robin, least-bytes, hash")
	flags.Duration("audit-kafka-write-timeout", 30*time.Second, "Timeout for Kafka write operations")
	flags.String("audit-kafka-required-acks", "all", "Required acks: none, one, all")

	_ = viper.BindPFlag("audit.sink", flags.Lookup("audit-sink"))
	_ = viper.BindPFlag("audit.firehose.streamName", flags.Lookup("audit-firehose-stream-name"))
	_ = viper.BindPFlag("audit.firehose.batchSize", flags.Lookup("audit-firehose-batch-size"))
	_ = viper.BindPFlag("audit.firehose.flushInterval", flags.Lookup("audit-firehose-flush-interval"))
	_ = viper.BindPFlag("audit.firehose.region", flags.Lookup("audit-firehose-region"))

	_ = viper.BindPFlag("audit.kafka.brokers", flags.Lookup("audit-kafka-brokers"))
	_ = viper.BindPFlag("audit.kafka.topic", flags.Lookup("audit-kafka-topic"))
	_ = viper.BindPFlag("audit.kafka.tls", flags.Lookup("audit-kafka-tls"))
	_ = viper.BindPFlag("audit.kafka.saslMechanism", flags.Lookup("audit-kafka-sasl-mechanism"))
	_ = viper.BindPFlag("audit.kafka.saslUsername", flags.Lookup("audit-kafka-sasl-username"))
	_ = viper.BindPFlag("audit.kafka.saslPassword", flags.Lookup("audit-kafka-sasl-password"))
	_ = viper.BindPFlag("audit.kafka.compression", flags.Lookup("audit-kafka-compression"))
	_ = viper.BindPFlag("audit.kafka.balancer", flags.Lookup("audit-kafka-balancer"))
	_ = viper.BindPFlag("audit.kafka.writeTimeout", flags.Lookup("audit-kafka-write-timeout"))
	_ = viper.BindPFlag("audit.kafka.requiredAcks", flags.Lookup("audit-kafka-required-acks"))

	_ = viper.BindEnv("audit.sink", "OPENFGA_AUDIT_SINK")
	_ = viper.BindEnv("audit.firehose.streamName", "OPENFGA_AUDIT_FIREHOSE_STREAMNAME")
	_ = viper.BindEnv("audit.firehose.batchSize", "OPENFGA_AUDIT_FIREHOSE_BATCHSIZE")
	_ = viper.BindEnv("audit.firehose.flushInterval", "OPENFGA_AUDIT_FIREHOSE_FLUSHINTERVAL")
	_ = viper.BindEnv("audit.firehose.region", "OPENFGA_AUDIT_FIREHOSE_REGION")

	_ = viper.BindEnv("audit.kafka.brokers", "OPENFGA_AUDIT_KAFKA_BROKERS")
	_ = viper.BindEnv("audit.kafka.topic", "OPENFGA_AUDIT_KAFKA_TOPIC")
	_ = viper.BindEnv("audit.kafka.tls", "OPENFGA_AUDIT_KAFKA_TLS")
	_ = viper.BindEnv("audit.kafka.saslMechanism", "OPENFGA_AUDIT_KAFKA_SASLMECHANISM")
	_ = viper.BindEnv("audit.kafka.saslUsername", "OPENFGA_AUDIT_KAFKA_SASLUSERNAME")
	_ = viper.BindEnv("audit.kafka.saslPassword", "OPENFGA_AUDIT_KAFKA_SASLPASSWORD")
	_ = viper.BindEnv("audit.kafka.compression", "OPENFGA_AUDIT_KAFKA_COMPRESSION")
	_ = viper.BindEnv("audit.kafka.balancer", "OPENFGA_AUDIT_KAFKA_BALANCER")
	_ = viper.BindEnv("audit.kafka.writeTimeout", "OPENFGA_AUDIT_KAFKA_WRITETIMEOUT")
	_ = viper.BindEnv("audit.kafka.requiredAcks", "OPENFGA_AUDIT_KAFKA_REQUIREDACKS")
}
