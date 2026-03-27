package config

import (
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type AuditConfig struct {
	Sink     string         `mapstructure:"sink"`
	Firehose FirehoseConfig `mapstructure:"firehose"`
}

type FirehoseConfig struct {
	StreamName    string        `mapstructure:"streamName"`
	BatchSize     int           `mapstructure:"batchSize"`
	FlushInterval time.Duration `mapstructure:"flushInterval"`
	Region        string        `mapstructure:"region"`
}

func DefaultAuditConfig() AuditConfig {
	return AuditConfig{
		Sink: "stdout",
		Firehose: FirehoseConfig{
			BatchSize:     500,
			FlushInterval: 5 * time.Second,
		},
	}
}

func ReadAuditConfig() AuditConfig {
	cfg := DefaultAuditConfig()
	_ = viper.UnmarshalKey("audit", &cfg)
	return cfg
}

func BindAuditFlags(cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.String("audit-sink", "stdout", "Audit sink type: stdout, firehose")
	flags.String("audit-firehose-stream-name", "", "AWS Firehose delivery stream name")
	flags.Int("audit-firehose-batch-size", 500, "Max records per Firehose PutRecordBatch")
	flags.Duration("audit-firehose-flush-interval", 5*time.Second, "Max time before flushing partial batch")
	flags.String("audit-firehose-region", "", "AWS region for Firehose")

	_ = viper.BindPFlag("audit.sink", flags.Lookup("audit-sink"))
	_ = viper.BindPFlag("audit.firehose.streamName", flags.Lookup("audit-firehose-stream-name"))
	_ = viper.BindPFlag("audit.firehose.batchSize", flags.Lookup("audit-firehose-batch-size"))
	_ = viper.BindPFlag("audit.firehose.flushInterval", flags.Lookup("audit-firehose-flush-interval"))
	_ = viper.BindPFlag("audit.firehose.region", flags.Lookup("audit-firehose-region"))

	_ = viper.BindEnv("audit.sink", "OPENFGA_AUDIT_SINK")
	_ = viper.BindEnv("audit.firehose.streamName", "OPENFGA_AUDIT_FIREHOSE_STREAMNAME")
	_ = viper.BindEnv("audit.firehose.batchSize", "OPENFGA_AUDIT_FIREHOSE_BATCHSIZE")
	_ = viper.BindEnv("audit.firehose.flushInterval", "OPENFGA_AUDIT_FIREHOSE_FLUSHINTERVAL")
	_ = viper.BindEnv("audit.firehose.region", "OPENFGA_AUDIT_FIREHOSE_REGION")
}
