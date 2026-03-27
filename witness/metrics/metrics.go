package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	AuditEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "openfga_witness_audit_events_total",
			Help: "Total number of audit events emitted",
		},
		[]string{"method", "status"},
	)

	AuditEventsDropped = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "openfga_witness_audit_events_dropped_total",
			Help: "Total number of audit events dropped due to sink errors",
		},
	)
)

func init() {
	prometheus.MustRegister(AuditEventsTotal)
	prometheus.MustRegister(AuditEventsDropped)
}
