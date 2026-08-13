// Package metrics defines every Prometheus series the system exports.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Drop reasons, used as the label on RequestsDropped.
const (
	DropReasonTimeout   = "admission_timeout"
	DropReasonQueueFull = "queue_full"
)

// Metrics holds every collector the system exports.
type Metrics struct {
	RequestsOffered prometheus.Counter
	RequestsPlaced  prometheus.Counter
	RequestsDropped *prometheus.CounterVec

	PlacementLatency prometheus.Histogram

	InflightVMs prometheus.Gauge
	QueueDepth  prometheus.Gauge

	DesiredVMhostReplicas prometheus.Gauge
	ReadyVMhosts          prometheus.Gauge
	PendingVMhosts        prometheus.Gauge
	DrainingVMhosts       prometheus.Gauge

	IdleVMhosts        prometheus.Gauge
	UnderfilledVMhosts prometheus.Gauge

	SlotsTotal prometheus.Gauge
	SlotsUsed  prometheus.Gauge

	Nodes            prometheus.Gauge
	EstimatedCostUSD prometheus.Gauge

	VMBootLatency  *prometheus.HistogramVec
	VMBootFailures *prometheus.CounterVec
}

// New creates and registers every collector on reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		RequestsOffered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "microvm_requests_offered_total",
			Help: "Placement requests received.",
		}),
		RequestsPlaced: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "microvm_requests_placed_total",
			Help: "Placement requests that resulted in a running microVM.",
		}),
		RequestsDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "microvm_requests_dropped_total",
			Help: "Placement requests that never got a placement, by reason. Must stay at zero.",
		}, []string{"reason"}),

		PlacementLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "microvm_placement_latency_seconds",
			Help: "End to end admission latency, including time queued waiting for capacity.",
			Buckets: []float64{
				0.00001, 0.00005, 0.0001, 0.0005, // free capacity: microseconds
				0.001, 0.005, 0.01, 0.05, // mild contention
				0.1, 0.25, 0.5, 1, 2, 3, // waiting on a pod to come up
			},
		}),

		InflightVMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "microvm_inflight_vms",
			Help: "microVMs currently running across the fleet.",
		}),
		QueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "microvm_admission_queue_depth",
			Help: "Requests currently waiting in the admission queue.",
		}),

		DesiredVMhostReplicas: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "microvm_desired_vmhost_replicas",
			Help: "Desired vmhost pod count. This is the series KEDA scales on.",
		}),
		ReadyVMhosts: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "microvm_vmhosts_ready",
			Help: "vmhost pods ready to accept microVMs.",
		}),
		PendingVMhosts: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "microvm_vmhosts_pending",
			Help: "vmhost pods started but not yet ready.",
		}),
		DrainingVMhosts: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "microvm_vmhosts_draining",
			Help: "vmhost pods shutting down, still serving existing microVMs.",
		}),

		IdleVMhosts: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "microvm_vmhosts_idle",
			Help: "Ready vmhost pods running no microVMs. The quantity to minimise.",
		}),
		UnderfilledVMhosts: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "microvm_vmhosts_underfilled",
			Help: "Ready vmhost pods running exactly one microVM, below the two-per-pod floor.",
		}),

		SlotsTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "microvm_slots_total",
			Help: "Total microVM slots across ready vmhost pods.",
		}),
		SlotsUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "microvm_slots_used",
			Help: "Occupied microVM slots.",
		}),

		Nodes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "microvm_nodes",
			Help: "Kubernetes nodes backing the vmhost fleet.",
		}),
		EstimatedCostUSD: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "microvm_estimated_cost_usd",
			Help: "Running cost estimate from node-seconds against a static price table.",
		}),

		VMBootLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "microvm_boot_latency_seconds",
			Help:    "Time for the hypervisor to make a guest runnable.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
		}, []string{"hypervisor"}),
		VMBootFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "microvm_boot_failures_total",
			Help: "Guests that failed to start, by hypervisor.",
		}, []string{"hypervisor"}),
	}

	reg.MustRegister(
		m.RequestsOffered,
		m.RequestsPlaced,
		m.RequestsDropped,
		m.PlacementLatency,
		m.InflightVMs,
		m.QueueDepth,
		m.DesiredVMhostReplicas,
		m.ReadyVMhosts,
		m.PendingVMhosts,
		m.DrainingVMhosts,
		m.IdleVMhosts,
		m.UnderfilledVMhosts,
		m.SlotsTotal,
		m.SlotsUsed,
		m.Nodes,
		m.EstimatedCostUSD,
		m.VMBootLatency,
		m.VMBootFailures,
	)

	m.RequestsDropped.WithLabelValues(DropReasonTimeout)
	m.RequestsDropped.WithLabelValues(DropReasonQueueFull)

	return m
}
