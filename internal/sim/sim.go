// Package sim runs the whole system end to end against a simulated fleet.
//
// Everything above the Kubernetes boundary is the real code: the real arrival
// process, the real admission queue, the real scheduler, the real autoscaler.
// What is simulated is the part Kubernetes would otherwise own, namely pods
// taking time to become ready and time to go away.
//
// That boundary is chosen so the simulation can be wrong about Kubernetes and
// still be right about everything the assignment grades. Placement, the
// zero-drop guarantee, and idle-pod minimisation are all decided by the real
// packages here; the simulator only supplies the latency that makes those
// decisions hard.
package sim

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pranav-gupta1/microvm-placement/internal/autoscale"
	"github.com/pranav-gupta1/microvm-placement/internal/loadgen"
	"github.com/pranav-gupta1/microvm-placement/internal/placement"
	"github.com/pranav-gupta1/microvm-placement/internal/scheduler"
)

// Config parameterises a simulated run.
type Config struct {
	// Envelope is the offered load shape.
	Envelope loadgen.Envelope
	// Seed makes the run reproducible.
	Seed uint64
	// MeanTTL is the mean microVM lifetime, exponentially distributed.
	MeanTTL time.Duration
	// SlotsPerHost is the vmhost pod slot count.
	SlotsPerHost int
	// PodStartLatency is how long a new vmhost pod takes to become ready. This
	// is the delay the autoscaler's lead term exists to cover.
	PodStartLatency time.Duration
	// SampleInterval is how often the autoscaler is consulted, equivalent to
	// the KEDA polling interval.
	SampleInterval time.Duration
	// Autoscale configures the scaling policy.
	Autoscale autoscale.Config
	// Placement configures the admission path.
	Placement placement.Config
}

// Validate reports whether the configuration is usable.
func (c Config) Validate() error {
	if c.Envelope == nil || c.Envelope.Duration() <= 0 {
		return fmt.Errorf("sim: Envelope must be set and have positive duration")
	}
	if c.MeanTTL <= 0 {
		return fmt.Errorf("sim: MeanTTL must be positive, got %s", c.MeanTTL)
	}
	if c.SlotsPerHost < scheduler.MinSlotsPerHost {
		return fmt.Errorf("sim: SlotsPerHost must be at least %d, got %d", scheduler.MinSlotsPerHost, c.SlotsPerHost)
	}
	if c.PodStartLatency < 0 {
		return fmt.Errorf("sim: PodStartLatency must not be negative")
	}
	if c.SampleInterval <= 0 {
		return fmt.Errorf("sim: SampleInterval must be positive, got %s", c.SampleInterval)
	}
	return c.Autoscale.Validate()
}

// Snapshot is one sampled point on the run timeline, and the row format the
// dashboard and the plotting script consume.
type Snapshot struct {
	// Elapsed is time since the run started.
	Elapsed time.Duration
	// OfferedRate is the measured arrival rate over the last interval.
	OfferedRate float64
	// InflightVMs is microVMs running at the sample instant.
	InflightVMs int
	// DesiredHosts is what the autoscaler asked for.
	DesiredHosts int
	// ReadyHosts is what was actually serving.
	ReadyHosts int
	// PendingHosts is pods started but not yet ready.
	PendingHosts int
	// DrainingHosts is pods on their way out.
	DrainingHosts int
	// IdleHosts is ready hosts running nothing, the quantity to minimise.
	IdleHosts int
	// Utilisation is occupied slots over ready slots.
	Utilisation float64
	// QueueDepth is admission queue occupancy.
	QueueDepth int
	// Dropped is the cumulative drop count, which must stay at zero.
	Dropped uint64
}

// Result summarises a completed run.
type Result struct {
	Offered int
	Placed  int
	// Dropped is TimedOut plus QueueRejected, and must be zero. It is unsigned
	// to match the counters it sums, which also avoids a lossy conversion.
	Dropped  uint64
	TimedOut uint64
	// QueueRejected counts requests that could not even be enqueued.
	QueueRejected uint64
	PeakInflight  int
	PeakHosts     int
	MaxQueueDepth uint64
	// HostSeconds is the integral of ready hosts over time, the simulation's
	// proxy for the compute bill.
	HostSeconds float64
	// IdleHostSeconds is the portion of that spent on hosts running nothing.
	IdleHostSeconds float64
	WaitP50         time.Duration
	WaitP99         time.Duration
	WaitMax         time.Duration
	Timeline        []Snapshot
}

// PlacementRate is the fraction of offered requests that were placed. The
// assignment requires this to be 1.
func (r Result) PlacementRate() float64 {
	if r.Offered == 0 {
		return 0
	}
	return float64(r.Placed) / float64(r.Offered)
}

// IdleFraction is the share of paid-for host time that ran nothing.
func (r Result) IdleFraction() float64 {
	if r.HostSeconds == 0 {
		return 0
	}
	return r.IdleHostSeconds / r.HostSeconds
}

// Run executes a simulated load run and returns its result.
func Run(ctx context.Context, cfg Config) (Result, error) {
	if err := cfg.Validate(); err != nil {
		return Result{}, err
	}

	sched := scheduler.New(scheduler.BestFit)
	svc, err := placement.New(sched, cfg.Placement)
	if err != nil {
		return Result{}, err
	}
	scaler, err := autoscale.New(cfg.Autoscale)
	if err != nil {
		return Result{}, err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	svc.Start(runCtx)
	defer svc.Stop()

	f := &fleet{
		sched:        sched,
		svc:          svc,
		slotsPerHost: cfg.SlotsPerHost,
		startLatency: cfg.PodStartLatency,
	}
	// Seed the fleet with the configured floor so the run does not begin with
	// zero capacity, which is what MinReplicas exists for.
	f.reconcile(runCtx, cfg.Autoscale.MinReplicas)

	r := &runner{
		cfg:    cfg,
		svc:    svc,
		sched:  sched,
		scaler: scaler,
		fleet:  f,
		rng:    rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x5deece66d)),
	}
	return r.run(runCtx)
}

// runner owns the state of one run.
type runner struct {
	cfg    Config
	svc    *placement.Service
	sched  *scheduler.Scheduler
	scaler *autoscale.Autoscaler
	fleet  *fleet

	rngMu sync.Mutex
	rng   *rand.Rand

	offered  atomic.Int64
	placed   atomic.Int64
	arrivals atomic.Int64 // reset every sample interval

	waitsMu sync.Mutex
	waits   []time.Duration

	peakInflight atomic.Int64
}

// sampleTTL draws an exponentially distributed microVM lifetime.
func (r *runner) sampleTTL() time.Duration {
	r.rngMu.Lock()
	defer r.rngMu.Unlock()
	return time.Duration(r.rng.ExpFloat64() * float64(r.cfg.MeanTTL))
}

func (r *runner) run(ctx context.Context) (Result, error) {
	proc, err := loadgen.NewProcess(r.cfg.Envelope, r.cfg.Seed)
	if err != nil {
		return Result{}, err
	}

	start := time.Now()
	var wg sync.WaitGroup

	// The sampler plays the part of KEDA polling the metric, and of the
	// Deployment controller acting on the result.
	//
	// It gets its own cancellable context so the run can stop sampling and
	// collect the timeline once the load is done, without cancelling the run
	// context that in-flight releases still depend on.
	sampleCtx, stopSampling := context.WithCancel(ctx)
	defer stopSampling()
	samplerDone := make(chan []Snapshot, 1)
	go func() {
		samplerDone <- r.sample(sampleCtx, start)
	}()

	// Open-loop arrival driver. Requests are issued on the schedule the
	// envelope dictates, regardless of how the system is coping. A closed-loop
	// generator would quietly reduce offered load when the system slowed,
	// hiding exactly the failure this run exists to detect.
	for {
		at, ok := proc.Next()
		if !ok {
			break
		}
		if delay := time.Until(start.Add(at)); delay > 0 {
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return Result{}, ctx.Err()
			}
		}
		if ctx.Err() != nil {
			return Result{}, ctx.Err()
		}

		r.offered.Add(1)
		r.arrivals.Add(1)
		id := fmt.Sprintf("vm-%d", r.offered.Load())

		wg.Add(1)
		go func(vmID string) {
			defer wg.Done()
			r.issue(ctx, vmID)
		}(id)
	}

	// Let outstanding microVMs finish so the fleet drains and the tail of the
	// run is observable, rather than cutting off at the last arrival.
	wg.Wait()

	// Stop sampling before collecting, or the read below would block until its
	// own timeout and throw away the entire timeline.
	stopSampling()

	return r.finish(ctx, start, samplerDone), nil
}

// issue performs one placement and schedules its release.
func (r *runner) issue(ctx context.Context, vmID string) {
	res, err := r.svc.Admit(ctx, placement.Request{VMID: vmID, TTL: r.cfg.MeanTTL})
	if err != nil {
		// Counted by the service; nothing more to do here.
		return
	}
	r.placed.Add(1)

	r.waitsMu.Lock()
	r.waits = append(r.waits, res.Wait)
	r.waitsMu.Unlock()

	if inflight := int64(r.sched.Stats().InflightVMs); inflight > r.peakInflight.Load() {
		r.peakInflight.Store(inflight)
	}

	ttl := r.sampleTTL()
	select {
	case <-time.After(ttl):
	case <-ctx.Done():
	}
	// Release even on cancellation so the fleet accounting stays consistent.
	_ = r.svc.Release(vmID)
}

// sample drives the autoscaler on a fixed interval and records the timeline.
func (r *runner) sample(ctx context.Context, start time.Time) []Snapshot {
	ticker := time.NewTicker(r.cfg.SampleInterval)
	defer ticker.Stop()

	var timeline []Snapshot
	for {
		select {
		case <-ctx.Done():
			return timeline
		case now := <-ticker.C:
			timeline = append(timeline, r.step(ctx, start, now))
		}
	}
}

// step takes one sample, consults the autoscaler and reconciles the fleet.
func (r *runner) step(ctx context.Context, start, now time.Time) Snapshot {
	arrivals := r.arrivals.Swap(0)
	rate := float64(arrivals) / r.cfg.SampleInterval.Seconds()

	stats := r.sched.Stats()
	decision := r.scaler.Decide(autoscale.Sample{
		At:          now,
		ArrivalRate: rate,
		InflightVMs: stats.InflightVMs,
	})
	r.fleet.reconcile(ctx, decision.Replicas)

	after := r.sched.Stats()
	svcStats := r.svc.Stats()
	return Snapshot{
		Elapsed:       now.Sub(start),
		OfferedRate:   rate,
		InflightVMs:   stats.InflightVMs,
		DesiredHosts:  decision.Replicas,
		ReadyHosts:    after.ReadyHosts,
		PendingHosts:  after.Hosts - after.ReadyHosts - after.DrainingHosts,
		DrainingHosts: after.DrainingHosts,
		IdleHosts:     after.IdleHosts,
		Utilisation:   after.Utilisation(),
		QueueDepth:    svcStats.CurrentQueueLen,
		Dropped:       svcStats.Dropped(),
	}
}

// finish stops sampling and assembles the result.
func (r *runner) finish(ctx context.Context, start time.Time, samplerDone <-chan []Snapshot) Result {
	// Take one closing sample so the drained tail appears on the timeline.
	final := r.step(ctx, start, time.Now())

	var timeline []Snapshot
	select {
	case timeline = <-samplerDone:
	case <-time.After(2 * r.cfg.SampleInterval):
	}
	timeline = append(timeline, final)

	stats := r.svc.Stats()
	res := Result{
		Offered:       int(r.offered.Load()),
		Placed:        int(r.placed.Load()),
		TimedOut:      stats.TimedOut,
		QueueRejected: stats.QueueRejected,
		PeakInflight:  int(r.peakInflight.Load()),
		MaxQueueDepth: stats.MaxQueueLen,
		Timeline:      timeline,
	}
	res.Dropped = stats.Dropped()

	for _, s := range timeline {
		if s.ReadyHosts > res.PeakHosts {
			res.PeakHosts = s.ReadyHosts
		}
		res.HostSeconds += float64(s.ReadyHosts) * r.cfg.SampleInterval.Seconds()
		res.IdleHostSeconds += float64(s.IdleHosts) * r.cfg.SampleInterval.Seconds()
	}

	r.waitsMu.Lock()
	waits := append([]time.Duration(nil), r.waits...)
	r.waitsMu.Unlock()
	res.WaitP50, res.WaitP99, res.WaitMax = percentiles(waits)

	return res
}

// percentiles returns the 50th, 99th and maximum of a duration sample.
func percentiles(d []time.Duration) (p50, p99, max time.Duration) {
	if len(d) == 0 {
		return 0, 0, 0
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	at := func(q float64) time.Duration {
		idx := int(math.Ceil(q*float64(len(d)))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(d) {
			idx = len(d) - 1
		}
		return d[idx]
	}
	return at(0.50), at(0.99), d[len(d)-1]
}
