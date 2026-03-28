package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	openfgacmd "github.com/openfga/openfga/cmd"
	"github.com/openfga/openfga/cmd/migrate"
	"github.com/openfga/openfga/cmd/run"
	"github.com/openfga/openfga/cmd/validatemodels"
	witnessconfig "github.com/openfga/openfga/witness/config"
	"github.com/openfga/openfga/witness/interceptor"
	"github.com/openfga/openfga/witness/sink"
)

func main() {
	rootCmd := openfgacmd.NewRootCommand()

	runCmd := newWitnessRunCommand()
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(migrate.NewMigrateCommand())
	rootCmd.AddCommand(validatemodels.NewValidateCommand())
	rootCmd.AddCommand(openfgacmd.NewVersionCommand())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func newWitnessRunCommand() *cobra.Command {
	// Get the stock run command (with all its flags pre-configured)
	cmd := run.NewRunCommand()

	// Add our audit flags
	witnessconfig.BindAuditFlags(cmd)

	// Replace the Run handler with our own that injects audit interceptors
	// via RunWithContext (added in Task 2's upstream modification)
	cmd.Run = func(c *cobra.Command, args []string) {
		auditCfg := witnessconfig.ReadAuditConfig()
		auditSink, err := createSink(auditCfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create audit sink: %v\n", err)
			os.Exit(1)
		}

		serverCtx := &run.ServerContext{
			ExtraUnaryInterceptors:  []grpc.UnaryServerInterceptor{interceptor.AuditUnary(auditSink)},
			ExtraStreamInterceptors: []grpc.StreamServerInterceptor{interceptor.AuditStream(auditSink)},
			OnShutdown: func(ctx context.Context) {
				if err := auditSink.Close(ctx); err != nil {
					fmt.Fprintf(os.Stderr, "warning: audit sink close: %v\n", err)
				}
			},
		}
		// RunWithContext reads config, initializes Logger if nil, and calls serverCtx.Run()
		run.RunWithContext(serverCtx)(c, args)
	}

	return cmd
}

func createSink(cfg witnessconfig.AuditConfig) (sink.AuditSink, error) {
	switch cfg.Sink {
	case "stdout":
		return sink.NewStdoutSink(os.Stdout), nil
	case "firehose":
		return sink.NewFirehoseSink(cfg.Firehose)
	case "kafka":
		return sink.NewKafkaSink(cfg.Kafka)
	default:
		return nil, fmt.Errorf("unknown audit sink: %s", cfg.Sink)
	}
}
