// Command placement-api is the microVM placement service.
//
// It accepts placement requests, assigns each to a vmhost pod, and exports the
// desired replica count that KEDA scales the vmhost Deployment on. It holds no
// persistent state: vmhost agents register themselves and heartbeat, so a
// restart repopulates the fleet within one heartbeat interval.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/pranav-gupta1/microvm-placement/internal/autoscale"
	"github.com/pranav-gupta1/microvm-placement/internal/httpapi"
	"github.com/pranav-gupta1/microvm-placement/internal/metrics"
	"github.com/pranav-gupta1/microvm-placement/internal/placement"
	"github.com/pranav-gupta1/microvm-placement/internal/registry"
	"github.com/pranav-gupta1/microvm-placement/internal/scheduler"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

// options holds every tunable, so the deployment surface is one flag list and
// docs/capacity-planning.md maps onto it directly.
type options struct {
	addr              string
	slotsPerHost      int
	targetUtilisation float64
	meanTTL           time.Duration
	provisionLatency  time.Duration
	scaleDownWindow   time.Duration
	slopeWindow       time.Duration
	minReplicas       int
	maxReplicas       int
	queueDepth        int
	admissionDeadline time.Duration
	sampleInterval    time.Duration
	heartbeatTimeout  time.Duration
	bootTimeout       time.Duration
	maxConns          int
	logLevel          string
}

func parseFlags() *options {
	o := &options{}
	flag.StringVar(&o.addr, "addr", ":8080", "listen address")
	flag.IntVar(&o.slotsPerHost, "slots-per-host", 8, "microVM slots per vmhost pod, at least 2")
	flag.Float64Var(&o.targetUtilisation, "target-utilisation", 0.8, "target slot occupancy, headroom for Poisson burstiness")
	flag.DurationVar(&o.meanTTL, "mean-ttl", 500*time.Millisecond, "mean microVM lifetime, converts arrival rate to concurrency")
	flag.DurationVar(&o.provisionLatency, "provision-latency", 20*time.Second, "measured time for new capacity to become useful, the lead term horizon")
	flag.DurationVar(&o.scaleDownWindow, "scale-down-window", 30*time.Second, "how long demand must stay low before capacity is released")
	flag.DurationVar(&o.slopeWindow, "slope-window", autoscale.DefaultSlopeWindow, "window over which the arrival-rate derivative is regressed")
	flag.IntVar(&o.minReplicas, "min-replicas", 2, "vmhost floor, covers the cold start")
	flag.IntVar(&o.maxReplicas, "max-replicas", 200, "vmhost ceiling, bounds cost and blast radius")
	flag.IntVar(&o.queueDepth, "queue-depth", placement.DefaultQueueDepth, "admission queue depth")
	flag.DurationVar(&o.admissionDeadline, "admission-deadline", placement.DefaultAdmissionDeadline, "how long a request may wait for capacity")
	flag.DurationVar(&o.sampleInterval, "sample-interval", time.Second, "how often the autoscaling signal is recomputed")
	flag.DurationVar(&o.heartbeatTimeout, "heartbeat-timeout", registry.DefaultHeartbeatTimeout, "how long a vmhost may go silent before it is drained")
	flag.DurationVar(&o.bootTimeout, "boot-timeout", time.Second, "per-attempt timeout calling a vmhost agent to boot a guest")
	flag.IntVar(&o.maxConns, "max-agent-conns", 512, "connection pool size for agent calls")
	flag.StringVar(&o.logLevel, "log-level", "info", "debug, info, warn or error")
	flag.Parse()
	return o
}

func run() error {
	o := parseFlags()

	log := newLogger(o.logLevel)
	slog.SetDefault(log)

	reg := prometheus.NewRegistry()
	// Go runtime and process collectors are worth having: at 1000 requests per
	// second, GC pauses and goroutine counts are the first place to look when
	// admission latency moves.
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := metrics.New(reg)

	sched := scheduler.New(scheduler.BestFit)

	svc, err := placement.New(sched, placement.Config{
		QueueDepth:        o.queueDepth,
		AdmissionDeadline: o.admissionDeadline,
	})
	if err != nil {
		return fmt.Errorf("placement service: %w", err)
	}

	scaler, err := autoscale.New(autoscale.Config{
		SlotsPerHost:           o.slotsPerHost,
		TargetUtilisation:      o.targetUtilisation,
		MeanTTL:                o.meanTTL,
		ProvisionLatency:       o.provisionLatency,
		ScaleDownStabilization: o.scaleDownWindow,
		SlopeWindow:            o.slopeWindow,
		MinReplicas:            o.minReplicas,
		MaxReplicas:            o.maxReplicas,
	})
	if err != nil {
		return fmt.Errorf("autoscaler: %w", err)
	}

	reggy, err := registry.New(sched, svc, registry.Config{HeartbeatTimeout: o.heartbeatTimeout})
	if err != nil {
		return fmt.Errorf("registry: %w", err)
	}

	// Cancelled by SIGINT or SIGTERM, which is what Kubernetes sends on pod
	// deletion.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Booting is what turns a placement into a running guest. Its timeout is
	// kept below the admission deadline so a slow agent surfaces as a retry
	// onto another host rather than consuming the caller's whole budget.
	svc.WithBooter(newHTTPBooter(reggy, o.bootTimeout, o.maxConns))

	svc.Start(ctx)
	defer svc.Stop()

	done := make(chan struct{})
	defer close(done)
	go reggy.Run(done)

	exporter := &signalExporter{
		svc:      svc,
		sched:    sched,
		scaler:   scaler,
		metrics:  m,
		interval: o.sampleInterval,
	}
	go exporter.run(ctx)

	srv := &http.Server{
		Addr:    o.addr,
		Handler: httpapi.New(svc, sched, m, reg, log).WithHostRegistry(reggy).Routes(),
		// Generous relative to the admission deadline: a request legitimately
		// blocks while waiting for capacity, and cutting it off at the HTTP
		// layer would manufacture the very drop the queue exists to prevent.
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      o.admissionDeadline + 10*time.Second,
		IdleTimeout:       120 * time.Second,
	}

	errs := make(chan error, 1)
	go func() {
		log.Info("placement-api listening",
			"addr", o.addr,
			"slots_per_host", o.slotsPerHost,
			"mean_ttl", o.meanTTL,
			"provision_latency", o.provisionLatency)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- err
		}
	}()

	select {
	case err := <-errs:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
		log.Info("shutting down")
	}

	// Drain in-flight requests before exiting. The grace period exceeds the
	// admission deadline so a request already waiting for capacity gets its
	// full chance rather than being dropped at the door on deploy.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), o.admissionDeadline+5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	return nil
}

// signalExporter recomputes the autoscaling signal on a fixed interval and
// publishes it, along with everything the dashboard reads.
//
// This is the only writer of microvm_desired_vmhost_replicas, which is the
// series KEDA scales on.
type signalExporter struct {
	svc      *placement.Service
	sched    *scheduler.Scheduler
	scaler   *autoscale.Autoscaler
	metrics  *metrics.Metrics
	interval time.Duration

	lastOffered uint64
}

func (e *signalExporter) run(ctx context.Context) {
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			e.step(now)
		}
	}
}

func (e *signalExporter) step(now time.Time) {
	svcStats := e.svc.Stats()
	fleet := e.sched.Stats()

	// Arrival rate from the delta in offered requests. Derived here rather
	// than with a PromQL rate() so the autoscaler sees exactly the number the
	// dashboard shows, with no window-alignment mismatch between them.
	offered := svcStats.Accepted
	delta := offered - e.lastOffered
	e.lastOffered = offered
	rate := float64(delta) / e.interval.Seconds()

	decision := e.scaler.Decide(autoscale.Sample{
		At:          now,
		ArrivalRate: rate,
		InflightVMs: fleet.InflightVMs,
	})

	e.metrics.DesiredVMhostReplicas.Set(float64(decision.Replicas))
	e.metrics.InflightVMs.Set(float64(fleet.InflightVMs))
	e.metrics.QueueDepth.Set(float64(svcStats.CurrentQueueLen))
	e.metrics.ReadyVMhosts.Set(float64(fleet.ReadyHosts))
	e.metrics.PendingVMhosts.Set(float64(fleet.Hosts - fleet.ReadyHosts - fleet.DrainingHosts))
	e.metrics.DrainingVMhosts.Set(float64(fleet.DrainingHosts))
	e.metrics.IdleVMhosts.Set(float64(fleet.IdleHosts))
	e.metrics.UnderfilledVMhosts.Set(float64(fleet.UnderfilledHosts))
	e.metrics.SlotsTotal.Set(float64(fleet.Capacity))
	e.metrics.SlotsUsed.Set(float64(fleet.Used))
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	// JSON so cluster log collection can parse it without a regex.
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}
