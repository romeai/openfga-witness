package sink

import (
	"context"

	"github.com/openfga/openfga/witness/ocsf"
)

type AuditSink interface {
	Emit(ctx context.Context, event ocsf.APIActivityEvent) error
	Close(ctx context.Context) error
}
