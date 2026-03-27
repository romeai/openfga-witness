package ocsf

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/authclaims"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	ocsfVersion = "1.4.0"
	productName = "openfga-witness"
	vendorName  = "RomeAI"
	serviceName = "openfga.v1.OpenFGAService"
)

func baseEvent(ctx context.Context, start time.Time, operation string, activityID int, err error, latency time.Duration) APIActivityEvent {
	event := APIActivityEvent{
		ClassUID:     ClassUIDAPIActivity,
		CategoryUID:  CategoryUIDApplication,
		ActivityID:   activityID,
		ActivityName: activityName(activityID),
		TypeUID:      ClassUIDAPIActivity*100 + activityID,
		TypeName:     "API Activity: " + activityName(activityID),
		Time:         start.UnixMilli(),
		SeverityID:   SeverityIDInformational,
		Severity:     "Informational",
		StatusID:     StatusIDSuccess,
		Status:       "Success",
		Duration:     latency.Milliseconds(),
		Metadata: Metadata{
			Version: ocsfVersion,
			Product: Product{Name: productName, VendorName: vendorName},
		},
		Api: Api{
			Operation: operation,
			Service:   ServiceInfo{Name: serviceName},
			Version:   "v1",
		},
	}

	if err != nil {
		event.SeverityID = SeverityIDMedium
		event.Severity = "Medium"
		event.StatusID = StatusIDFailure
		event.Status = "Failure"
		if st, ok := status.FromError(err); ok {
			event.StatusCode = st.Code().String()
			event.StatusDetail = st.Message()
		}
	}

	// Extract request_id from context tags (set by requestid middleware)
	if tags := grpc_ctxtags.Extract(ctx); tags != nil {
		if reqID, ok := tags.Values()["request_id"]; ok {
			event.Metadata.UID = fmt.Sprintf("%v", reqID)
		}
	}

	if claims, ok := authclaims.AuthClaimsFromContext(ctx); ok {
		event.Actor = Actor{
			User:   ActorUser{UID: claims.Subject},
			AppUID: claims.ClientID,
		}
	}

	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		host, portStr, splitErr := net.SplitHostPort(p.Addr.String())
		if splitErr == nil {
			event.SrcEndpoint.IP = host
			if portNum, atoiErr := strconv.Atoi(portStr); atoiErr == nil {
				event.SrcEndpoint.Port = portNum
			}
		}
	}

	return event
}

func BuildCheckEvent(ctx context.Context, start time.Time, req *openfgav1.CheckRequest, resp *openfgav1.CheckResponse, err error, latency time.Duration) APIActivityEvent {
	event := baseEvent(ctx, start, "Check", ActivityIDRead, err, latency)
	event.Metadata.TenantUID = req.GetStoreId()

	event.Resources = []Resource{{
		Name: req.GetTupleKey().GetObject(),
		Type: objectType(req.GetTupleKey().GetObject()),
		UID:  objectID(req.GetTupleKey().GetObject()),
		Data: map[string]any{
			"user":     req.GetTupleKey().GetUser(),
			"relation": req.GetTupleKey().GetRelation(),
			"object":   req.GetTupleKey().GetObject(),
		},
	}}

	if resp != nil {
		decision := "Denied"
		dispositionID := DispositionIDBlocked
		if resp.GetAllowed() {
			decision = "Allowed"
			dispositionID = DispositionIDAllowed
		}
		event.Authorizations = []Authorization{{Decision: decision}}
		event.DispositionID = &dispositionID
		event.Disposition = decision
	}

	unmapped := map[string]any{}
	if req.GetAuthorizationModelId() != "" {
		unmapped["authorization_model_id"] = req.GetAuthorizationModelId()
	}
	if req.GetContext() != nil {
		unmapped["context"] = req.GetContext().AsMap()
	}
	if ct := req.GetContextualTuples(); ct != nil && len(ct.GetTupleKeys()) > 0 {
		unmapped["contextual_tuples_count"] = len(ct.GetTupleKeys())
	}
	if req.GetConsistency() != 0 {
		unmapped["consistency"] = req.GetConsistency().String()
	}
	if len(unmapped) > 0 {
		event.Unmapped = unmapped
	}

	return event
}

func BuildGenericEvent(ctx context.Context, start time.Time, fullMethod string, err error, latency time.Duration) APIActivityEvent {
	operation := fullMethod
	if idx := strings.LastIndex(fullMethod, "/"); idx >= 0 {
		operation = fullMethod[idx+1:]
	}
	return baseEvent(ctx, start, operation, ActivityIDOther, err, latency)
}

func BuildWriteEvent(ctx context.Context, start time.Time, req *openfgav1.WriteRequest, err error, latency time.Duration) APIActivityEvent {
	activityID := ActivityIDCreate
	hasWrites := req.GetWrites() != nil && len(req.GetWrites().GetTupleKeys()) > 0
	hasDeletes := req.GetDeletes() != nil && len(req.GetDeletes().GetTupleKeys()) > 0
	if hasWrites && hasDeletes {
		activityID = ActivityIDUpdate
	} else if hasDeletes {
		activityID = ActivityIDDelete
	}

	event := baseEvent(ctx, start, "Write", activityID, err, latency)
	event.Metadata.TenantUID = req.GetStoreId()

	var resources []Resource
	for _, tk := range req.GetWrites().GetTupleKeys() {
		resources = append(resources, Resource{
			Name: tk.GetObject(),
			Type: objectType(tk.GetObject()),
			UID:  objectID(tk.GetObject()),
			Data: map[string]any{
				"user": tk.GetUser(), "relation": tk.GetRelation(),
				"object": tk.GetObject(), "operation": "create",
			},
		})
	}
	for _, tk := range req.GetDeletes().GetTupleKeys() {
		resources = append(resources, Resource{
			Name: tk.GetObject(),
			Type: objectType(tk.GetObject()),
			UID:  objectID(tk.GetObject()),
			Data: map[string]any{
				"user": tk.GetUser(), "relation": tk.GetRelation(),
				"object": tk.GetObject(), "operation": "delete",
			},
		})
	}
	event.Resources = resources

	if id := req.GetAuthorizationModelId(); id != "" {
		event.Unmapped = map[string]any{"authorization_model_id": id}
	}

	return event
}

func BuildReadEvent(ctx context.Context, start time.Time, req *openfgav1.ReadRequest, err error, latency time.Duration) APIActivityEvent {
	event := baseEvent(ctx, start, "Read", ActivityIDRead, err, latency)
	event.Metadata.TenantUID = req.GetStoreId()

	data := map[string]any{}
	if tk := req.GetTupleKey(); tk != nil {
		if v := tk.GetUser(); v != "" {
			data["user"] = v
		}
		if v := tk.GetRelation(); v != "" {
			data["relation"] = v
		}
		if v := tk.GetObject(); v != "" {
			data["object"] = v
		}
	}
	event.Resources = []Resource{{
		Name: req.GetTupleKey().GetObject(),
		Type: objectType(req.GetTupleKey().GetObject()),
		Data: data,
	}}

	return event
}

func BuildListObjectsEvent(ctx context.Context, start time.Time, req *openfgav1.ListObjectsRequest, resp *openfgav1.ListObjectsResponse, err error, latency time.Duration) APIActivityEvent {
	event := baseEvent(ctx, start, "ListObjects", ActivityIDRead, err, latency)
	event.Metadata.TenantUID = req.GetStoreId()

	event.Resources = []Resource{{
		Type: req.GetType(),
		Data: map[string]any{
			"user": req.GetUser(), "relation": req.GetRelation(), "type": req.GetType(),
		},
	}}

	unmapped := map[string]any{}
	if id := req.GetAuthorizationModelId(); id != "" {
		unmapped["authorization_model_id"] = id
	}
	if resp != nil {
		unmapped["result_count"] = len(resp.GetObjects())
	}
	if len(unmapped) > 0 {
		event.Unmapped = unmapped
	}
	return event
}

func BuildListUsersEvent(ctx context.Context, start time.Time, req *openfgav1.ListUsersRequest, resp *openfgav1.ListUsersResponse, err error, latency time.Duration) APIActivityEvent {
	event := baseEvent(ctx, start, "ListUsers", ActivityIDRead, err, latency)
	event.Metadata.TenantUID = req.GetStoreId()

	obj := req.GetObject()
	objName := obj.GetType() + ":" + obj.GetId()

	event.Resources = []Resource{{
		Name: objName,
		Type: obj.GetType(),
		UID:  obj.GetId(),
		Data: map[string]any{"object": objName, "relation": req.GetRelation()},
	}}

	unmapped := map[string]any{}
	if id := req.GetAuthorizationModelId(); id != "" {
		unmapped["authorization_model_id"] = id
	}
	if resp != nil {
		unmapped["result_count"] = len(resp.GetUsers())
	}
	if len(unmapped) > 0 {
		event.Unmapped = unmapped
	}
	return event
}

func BuildBatchCheckEvent(ctx context.Context, start time.Time, req *openfgav1.BatchCheckRequest, resp *openfgav1.BatchCheckResponse, err error, latency time.Duration) APIActivityEvent {
	event := baseEvent(ctx, start, "BatchCheck", ActivityIDRead, err, latency)
	event.Metadata.TenantUID = req.GetStoreId()

	var resources []Resource
	var authorizations []Authorization

	for _, check := range req.GetChecks() {
		tk := check.GetTupleKey()
		resources = append(resources, Resource{
			Name: tk.GetObject(),
			Type: objectType(tk.GetObject()),
			UID:  objectID(tk.GetObject()),
			Data: map[string]any{
				"user": tk.GetUser(), "relation": tk.GetRelation(),
				"object": tk.GetObject(), "correlation_id": check.GetCorrelationId(),
			},
		})

		decision := "Unknown"
		if resp != nil {
			if result, ok := resp.GetResult()[check.GetCorrelationId()]; ok {
				if allowed, isAllowed := result.GetCheckResult().(*openfgav1.BatchCheckSingleResult_Allowed); isAllowed {
					if allowed.Allowed {
						decision = "Allowed"
					} else {
						decision = "Denied"
					}
				} else {
					decision = "Error"
				}
			}
		}
		authorizations = append(authorizations, Authorization{Decision: decision})
	}

	event.Resources = resources
	event.Authorizations = authorizations

	if id := req.GetAuthorizationModelId(); id != "" {
		event.Unmapped = map[string]any{"authorization_model_id": id}
	}
	return event
}

func BuildStreamedListObjectsEvent(ctx context.Context, start time.Time, req *openfgav1.StreamedListObjectsRequest, err error, latency time.Duration) APIActivityEvent {
	event := baseEvent(ctx, start, "StreamedListObjects", ActivityIDRead, err, latency)
	event.Metadata.TenantUID = req.GetStoreId()

	event.Resources = []Resource{{
		Type: req.GetType(),
		Data: map[string]any{
			"user": req.GetUser(), "relation": req.GetRelation(), "type": req.GetType(),
		},
	}}

	unmapped := map[string]any{}
	if id := req.GetAuthorizationModelId(); id != "" {
		unmapped["authorization_model_id"] = id
	}
	if len(unmapped) > 0 {
		event.Unmapped = unmapped
	}
	return event
}

func activityName(id int) string {
	switch id {
	case ActivityIDCreate:
		return "Create"
	case ActivityIDRead:
		return "Read"
	case ActivityIDUpdate:
		return "Update"
	case ActivityIDDelete:
		return "Delete"
	default:
		return "Other"
	}
}

func objectType(object string) string {
	if idx := strings.Index(object, ":"); idx >= 0 {
		return object[:idx]
	}
	return object
}

func objectID(object string) string {
	if idx := strings.Index(object, ":"); idx >= 0 {
		return object[idx+1:]
	}
	return ""
}
