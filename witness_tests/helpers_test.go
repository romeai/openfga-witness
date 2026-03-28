package witnesstests

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/cmd/run"
	"github.com/openfga/openfga/pkg/logger"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/tests"
	"github.com/openfga/openfga/witness/interceptor"
	"github.com/openfga/openfga/witness/sink"
)

// startTestServer starts an in-memory OpenFGA server with audit interceptors
// and returns a client that satisfies both tests.ClientInterface (for the
// conformance suite) and openfgav1.OpenFGAServiceClient (for integration tests).
// The server is torn down automatically when the test finishes.
func startTestServer(t *testing.T) (tests.ClientInterface, *sink.MemorySink) {
	t.Helper()

	ms := sink.NewMemorySink()

	cfg := serverconfig.MustDefaultConfig()
	cfg.Datastore.Engine = "memory"
	cfg.Log.Level = "error"

	lgr := logger.MustNewLogger(cfg.Log.Format, cfg.Log.Level, cfg.Log.TimestampFormat)
	serverCtx := &run.ServerContext{
		Logger:                  lgr,
		ExtraUnaryInterceptors:  []grpc.UnaryServerInterceptor{interceptor.AuditUnary(ms)},
		ExtraStreamInterceptors: []grpc.StreamServerInterceptor{interceptor.AuditStream(ms)},
	}

	httpPort, httpPortReleaser := testutils.TCPRandomPort()
	cfg.HTTP.Addr = fmt.Sprintf("localhost:%d", httpPort)
	grpcPort, grpcPortReleaser := testutils.TCPRandomPort()
	cfg.GRPC.Addr = fmt.Sprintf("localhost:%d", grpcPort)
	httpPortReleaser()
	grpcPortReleaser()

	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error)
	go func() {
		serverDone <- serverCtx.Run(ctx, cfg)
	}()
	t.Cleanup(func() {
		cancel()
		<-serverDone
	})

	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil)

	conn := testutils.CreateGrpcConnection(t, cfg.GRPC.Addr)
	client := openfgav1.NewOpenFGAServiceClient(conn)

	return client, ms
}
