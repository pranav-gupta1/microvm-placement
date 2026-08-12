// Package metrics defines every Prometheus series the system exports.
//
// Two audiences consume these, and they have different requirements.
//
// KEDA scrapes exactly one series, microvm_desired_vmhost_replicas, and scales
// the vmhost Deployment to match it. That series is the contract between the
// autoscaling policy in package autoscale and the controller that acts on it,
// so it is a plain gauge with no labels: anything KEDA has to aggregate or
// rate() is a chance for the query to be subtly wrong at exactly the moment
// load spikes.
//
// Humans read the rest on a Grafana dashboard. Those are shaped to answer the
// two questions the assignment grades, namely whether anything was dropped and
// how much capacity was paid for but unused.
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
//
// Collectors are created against a caller-supplied registry rather than the
// default one so that tests can build an isolated instance, and so a stray
// import cannot inject series into our exposition.
type Metrics struct {
	// RequestsOffered counts every request the API was asked to place. Paired
	// with RequestsPlaced this is the placement rate the assignment grades.
	RequestsOffered prometheus.Counter
	// RequestsPlaced counts successful placements.
	RequestsPlaced prometheus.Counter
	// RequestsDropped counts requests that never got a placement, by reason.
	// The objective requires this to stay at zero, so it is a counter with a
	// reason label rather than a single number: when it does move, the label
	// tells you whether the queue was full or capacity never arrived.
	RequestsDropped *prometheus.CounterVec

	// PlacementLatency measures admission end to end, including any time spent
	// waiting in the queue. Buckets are chosen around the interesting region:
	// microseconds when capacity is free, hundreds of milliseconds when the
	// request had to wait for a pod to come up.
	PlacementLatency prometheus.Histogram

	// InflightVMs is microVMs currently running across the fleet.
	InflightVMs prometheus.Gauge
	// QueueDepth is current admission queue occupancy. A persistently non-zero
	// value means pre-provisioned capacity is undersized.
	QueueDepth prometheus.Gauge

	// DesiredVMhostReplicas is the series KEDA scales on.
	DesiredVMhostReplicas prometheus.Gauge
	// ReadyVMhosts, PendingVMhosts and DrainingVMhosts break the fleet down by
	// lifecycle state. Plotting ready against desired is what makes
	// provisioning lag visible rather than merely suspected.
	ReadyVMhosts    prometheus.Gauge
	PendingVMhosts  prometheus.Gauge
	DrainingVMhosts prometheus.Gauge

	// IdleVMhosts is ready pods running no microVMs at all, the quantity the
	// assignment asks us to minimise.
	IdleVMhosts prometheus.Gauge
	// UnderfilledVMhosts is ready pods running exactly one microVM. The
	// assignment requires at least two per pod, so a sustained non-zero value
	// means the fleet is scaled too wide.
	UnderfilledVMhosts prometheus.Gauge

	// SlotsTotal and SlotsUsed are raw capacity, from which utilisation is
	// derived in the dashboard rather than precomputed here. Exporting the
	// numerator and denominator separately keeps the ratio correct across
	// aggregation, which a precomputed ratio would not be.
	SlotsTotal prometheus.Gauge
	SlotsUsed  prometheus.Gauge

	// Nodes is the count of Kubernetes nodes backing the fleet.
	Nodes prometheus.Gauge
	// EstimatedCostUSD is a running dollar total computed from node-seconds
	// against a static price table. It is an estimate for the dashboard, not
	// an accounting figure, and is labelled as such in Grafana.
	EstimatedCostUSD prometheus.Gauge

	// VMBootLatency is how long the hypervisor took to make a guest runnable.
	// Buckets span Firecracker snapshot restore in the low tens of
	// milliseconds through to QEMU software emulation in the seconds.
	VMBootLatency *prometheus.HistogramVec
	// VMBootFailures counts guests that failed to start, by hypervisor.
	VMBootFailures *prometheus.CounterVec
}

// New creates and registers every collector on reg.
//
// It uses MustRegister deliberately. A duplicate or malformed collector is a
// programming error that should stop the process at startup, not degrade the
// observability of a running system into silence.
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
			Name: "microvm_boot_latency_seconds",
			Help: "Time for the hypervisor to make a guest runnable.",
			// Spans Firecracker snapshot restore (tens of ms) through QEMU
			// software emulation (seconds), so one dashboard panel serves
			// every hypervisor implementation.
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

	// Initialise the drop counters so both reasons are present at zero from
	// the first scrape. Without this, a Grafana panel showing drops renders as
	// "No data" rather than a reassuring flat zero line, which looks identical
	// to a broken exporter at exactly the moment you want to trust it.
	m.RequestsDropped.WithLabelValues(DropReasonTimeout)
	m.RequestsDropped.WithLabelValues(DropReasonQueueFull)

	return m
}
