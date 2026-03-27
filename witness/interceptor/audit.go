package interceptor

import (
	"context"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/grpc"

	"github.com/openfga/openfga/witness/ocsf"
	"github.com/openfga/openfga/witness/sink"
)

func AuditUnary(s sink.AuditSink) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		latency := time.Since(start)

		var event ocsf.APIActivityEvent

		switch r := req.(type) {
		case *openfgav1.CheckRequest:
			checkResp, _ := resp.(*openfgav1.CheckResponse)
			event = ocsf.BuildCheckEvent(ctx, start, r, checkResp, err, latency)

		case *openfgav1.BatchCheckRequest:
			batchResp, _ := resp.(*openfgav1.BatchCheckResponse)
			event = ocsf.BuildBatchCheckEvent(ctx, start, r, batchResp, err, latency)

		case *openfgav1.WriteRequest:
			event = ocsf.BuildWriteEvent(ctx, start, r, err, latency)

		case *openfgav1.ReadRequest:
			event = ocsf.BuildReadEvent(ctx, start, r, err, latency)

		case *openfgav1.ListObjectsRequest:
			listResp, _ := resp.(*openfgav1.ListObjectsResponse)
			event = ocsf.BuildListObjectsEvent(ctx, start, r, listResp, err, latency)

		case *openfgav1.ListUsersRequest:
			listResp, _ := resp.(*openfgav1.ListUsersResponse)
			event = ocsf.BuildListUsersEvent(ctx, start, r, listResp, err, latency)

		default:
			event = ocsf.BuildGenericEvent(ctx, start, info.FullMethod, err, latency)
		}

		_ = s.Emit(ctx, event)

		return resp, err
	}
}

func AuditStream(s sink.AuditSink) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Wrap the stream to capture the first received message (the request)
		wrapped := &auditStreamWrapper{ServerStream: stream}
		start := time.Now()
		err := handler(srv, wrapped)
		latency := time.Since(start)

		var event ocsf.APIActivityEvent
		if req, ok := wrapped.firstReq.(*openfgav1.StreamedListObjectsRequest); ok {
			event = ocsf.BuildStreamedListObjectsEvent(stream.Context(), start, req, err, latency)
		} else {
			event = ocsf.BuildGenericEvent(stream.Context(), start, info.FullMethod, err, latency)
		}

		_ = s.Emit(stream.Context(), event)
		return err
	}
}

// auditStreamWrapper captures the first message received on the stream.
type auditStreamWrapper struct {
	grpc.ServerStream
	firstReq any
	captured bool
}

func (w *auditStreamWrapper) RecvMsg(m any) error {
	err := w.ServerStream.RecvMsg(m)
	if err == nil && !w.captured {
		w.firstReq = m
		w.captured = true
	}
	return err
}
