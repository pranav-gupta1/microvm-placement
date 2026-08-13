// Package sim runs the whole system end to end against a simulated fleet.
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
	Envelope        loadgen.Envelope
	Seed            uint64
	MeanTTL         time.Duration
	SlotsPerHost    int
	PodStartLatency time.Duration
	SampleInterval  time.Duration
	Autoscale       autoscale.Config
	Placement       placement.Config
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
	Elapsed       time.Duration
	OfferedRate   float64
	InflightVMs   int
	DesiredHosts  int
	ReadyHosts    int
	PendingHosts  int
	DrainingHosts int
	IdleHosts     int
	Utilisation   float64
	QueueDepth    int
	Dropped       uint64
}

// Result summarises a completed run.
type Result struct {
	Offered         int
	Placed          int
	Dropped         uint64
	TimedOut        uint64
	QueueRejected   uint64
	PeakInflight    int
	PeakHosts       int
	MaxQueueDepth   uint64
	HostSeconds     float64
	IdleHostSeconds float64
	WaitP50         time.Duration
	WaitP99         time.Duration
	WaitMax         time.Duration
	Timeline        []Snapshot
}

// PlacementRate is the fraction of offered requests that were placed.
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

	sampleCtx, stopSampling := context.WithCancel(ctx)
	defer stopSampling()
	samplerDone := make(chan []Snapshot, 1)
	go func() {
		samplerDone <- r.sample(sampleCtx, start)
	}()

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

	wg.Wait()

	stopSampling()

	return r.finish(ctx, start, samplerDone), nil
}

// issue performs one placement and schedules its release.
func (r *runner) issue(ctx context.Context, vmID string) {
	res, err := r.svc.Admit(ctx, placement.Request{VMID: vmID, TTL: r.cfg.MeanTTL})
	if err != nil {
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
