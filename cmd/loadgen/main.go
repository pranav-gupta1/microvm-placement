// Command loadgen is the test harness.
//
// It offers a double ramp of traffic to the placement API: zero up to a peak,
// back to zero, twice, with Poisson interarrivals layered on the smooth
// envelope so the traffic is bursty rather than metronomic.
//
// # Open loop, and why it matters
//
// The generator issues requests on the schedule the envelope dictates,
// regardless of whether earlier requests have completed. The obvious
// alternative, a fixed pool of workers each looping "send, wait for reply,
// send again", is closed loop: when the system slows down, the generator
// automatically slows down with it. That silently reduces offered load exactly
// when the system is struggling, which is precisely the condition this harness
// exists to detect. A closed-loop generator cannot fail to meet its target rate,
// because its target rate is defined by the system under test.
//
// The cost of open loop is that the generator must be able to hold as many
// in-flight requests as the system is behind by, which is why results record
// in-flight high-water marks alongside latency.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pranav-gupta1/microvm-placement/internal/loadgen"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "loadgen: %v\n", err)
		os.Exit(1)
	}
}

type options struct {
	target     string
	peakRPS    float64
	rampUp     time.Duration
	hold       time.Duration
	rampDown   time.Duration
	cycles     int
	ttl        time.Duration
	seed       uint64
	resultsOut string
	timeout    time.Duration
	maxConns   int
}

func parseFlags() *options {
	o := &options{}
	flag.StringVar(&o.target, "target", "http://127.0.0.1:18080", "placement API base URL")
	flag.Float64Var(&o.peakRPS, "peak-rps", 1000, "peak offered rate")
	flag.DurationVar(&o.rampUp, "ramp-up", 30*time.Second, "climb from zero to peak")
	flag.DurationVar(&o.hold, "hold", 0, "time held at peak")
	flag.DurationVar(&o.rampDown, "ramp-down", 30*time.Second, "descent from peak to zero")
	flag.IntVar(&o.cycles, "cycles", 2, "back to back ramp cycles")
	flag.DurationVar(&o.ttl, "ttl", 500*time.Millisecond, "requested microVM lifetime")
	flag.Uint64Var(&o.seed, "seed", 1, "seed, making a run reproducible")
	flag.StringVar(&o.resultsOut, "results", "", "write per-request JSON lines here")
	flag.DurationVar(&o.timeout, "timeout", 30*time.Second, "per-request timeout, well above the server admission deadline")
	flag.IntVar(&o.maxConns, "max-conns", 512, "connection pool size")
	flag.Parse()
	return o
}

// result is one line of the JSON Lines output, so a run can be replotted
// without rerunning it.
type result struct {
	Seq         int    `json:"seq"`
	OffsetMS    int64  `json:"offset_ms"`
	LatencyUS   int64  `json:"latency_us"`
	Status      int    `json:"status"`
	Host        string `json:"host,omitempty"`
	QueueWaitUS int64  `json:"queue_wait_us,omitempty"`
	Err         string `json:"error,omitempty"`
}

func run() error {
	o := parseFlags()

	ramp := loadgen.Ramp{Up: o.rampUp, Hold: o.hold, Down: o.rampDown, Peak: o.peakRPS}
	if err := ramp.Validate(); err != nil {
		return err
	}
	env := loadgen.Repeat{Inner: ramp, Times: o.cycles}

	proc, err := loadgen.NewProcess(env, o.seed)
	if err != nil {
		return err
	}

	// A connection pool sized to the expected in-flight count. Without this the
	// default transport caps idle connections per host at 2 and the generator
	// spends its time in TCP handshakes instead of offering load.
	client := &http.Client{
		Timeout: o.timeout,
		Transport: &http.Transport{
			MaxIdleConns:        o.maxConns,
			MaxIdleConnsPerHost: o.maxConns,
			MaxConnsPerHost:     o.maxConns,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	expected := loadgen.ExpectedArrivals(env, 100000)
	fmt.Printf("offering %.0f requests over %s (peak %.0f rps, %d cycles, ttl %s)\n",
		expected, env.Duration(), o.peakRPS, o.cycles, o.ttl)
	fmt.Printf("target %s\n\n", o.target)

	h := &harness{opts: o, client: client}
	if o.resultsOut != "" {
		f, err := os.Create(o.resultsOut)
		if err != nil {
			return fmt.Errorf("create results file: %w", err)
		}
		defer func() { _ = f.Close() }()
		h.results = f
	}

	start := time.Now()
	var wg sync.WaitGroup
	progress := time.NewTicker(5 * time.Second)
	defer progress.Stop()
	go h.reportProgress(ctx, progress.C, start)

	seq := 0
	for {
		at, ok := proc.Next()
		if !ok {
			break
		}
		// Absolute-time targeting rather than sleeping for the interarrival
		// gap. A sleep that overshoots would otherwise push every subsequent
		// arrival later and the run would drift below its target rate.
		if delay := time.Until(start.Add(at)); delay > 0 {
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return h.finish(start, &wg)
			}
		}
		if ctx.Err() != nil {
			break
		}

		seq++
		wg.Add(1)
		go func(seq int, at time.Duration) {
			defer wg.Done()
			h.issue(ctx, seq, at)
		}(seq, at)
	}

	return h.finish(start, &wg)
}

// harness owns counters and result recording for a run.
type harness struct {
	opts    *options
	client  *http.Client
	results io.Writer

	offered      atomic.Int64
	placed       atomic.Int64
	dropped      atomic.Int64
	failed       atomic.Int64
	inflight     atomic.Int64
	peakInflight atomic.Int64

	mu        sync.Mutex
	latencies []time.Duration
}

func (h *harness) issue(ctx context.Context, seq int, at time.Duration) {
	h.offered.Add(1)
	n := h.inflight.Add(1)
	for {
		peak := h.peakInflight.Load()
		if n <= peak || h.peakInflight.CompareAndSwap(peak, n) {
			break
		}
	}
	defer h.inflight.Add(-1)

	body, _ := json.Marshal(map[string]any{"ttl_ms": h.opts.ttl.Milliseconds()})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.opts.target+"/v1/vms", bytes.NewReader(body))
	if err != nil {
		h.failed.Add(1)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	reqStart := time.Now()
	resp, err := h.client.Do(req)
	latency := time.Since(reqStart)

	r := result{Seq: seq, OffsetMS: at.Milliseconds(), LatencyUS: latency.Microseconds()}
	switch {
	case err != nil:
		h.failed.Add(1)
		r.Err = err.Error()
	default:
		r.Status = resp.StatusCode
		var decoded struct {
			Host        string `json:"host"`
			QueueWaitUS int64  `json:"queue_wait_us"`
		}
		// Always drain and close, or the connection cannot be reused and the
		// pool churns sockets at exactly the moment it should be steady.
		if resp.StatusCode == http.StatusCreated {
			_ = json.NewDecoder(resp.Body).Decode(&decoded)
			r.Host = decoded.Host
			r.QueueWaitUS = decoded.QueueWaitUS
			h.placed.Add(1)
		} else {
			_, _ = io.Copy(io.Discard, resp.Body)
			// 503 is the server admitting it lost the request. Anything else
			// in this position is a client bug, and both are counted, but only
			// the first is a violation of the objective.
			if resp.StatusCode == http.StatusServiceUnavailable {
				h.dropped.Add(1)
			} else {
				h.failed.Add(1)
			}
		}
		_ = resp.Body.Close()

		h.mu.Lock()
		h.latencies = append(h.latencies, latency)
		h.mu.Unlock()
	}

	if h.results != nil {
		line, _ := json.Marshal(r)
		h.mu.Lock()
		_, _ = h.results.Write(append(line, '\n'))
		h.mu.Unlock()
	}
}

func (h *harness) reportProgress(ctx context.Context, tick <-chan time.Time, start time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			fmt.Printf("  t=%-6s offered=%-7d placed=%-7d dropped=%-4d inflight=%d\n",
				time.Since(start).Round(time.Second),
				h.offered.Load(), h.placed.Load(), h.dropped.Load(), h.inflight.Load())
		}
	}
}

func (h *harness) finish(start time.Time, wg *sync.WaitGroup) error {
	wg.Wait()
	elapsed := time.Since(start)

	offered := h.offered.Load()
	placed := h.placed.Load()
	dropped := h.dropped.Load()
	failed := h.failed.Load()

	h.mu.Lock()
	lat := append([]time.Duration(nil), h.latencies...)
	h.mu.Unlock()
	p50, p95, p99, max := percentiles(lat)

	rate := 0.0
	if offered > 0 {
		rate = float64(placed) / float64(offered) * 100
	}

	fmt.Printf("\n%s\n", "=== run complete ===")
	fmt.Printf("  duration          %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  offered           %d\n", offered)
	fmt.Printf("  placed            %d  (%.4f%%)\n", placed, rate)
	fmt.Printf("  dropped (503)     %d\n", dropped)
	fmt.Printf("  transport errors  %d\n", failed)
	fmt.Printf("  peak in-flight    %d\n", h.peakInflight.Load())
	fmt.Printf("  latency           p50 %s  p95 %s  p99 %s  max %s\n",
		p50.Round(time.Microsecond), p95.Round(time.Microsecond),
		p99.Round(time.Microsecond), max.Round(time.Microsecond))

	if dropped > 0 || failed > 0 {
		// Non-zero exit so `make demo` and CI fail loudly rather than printing
		// a bad number that someone has to notice.
		return fmt.Errorf("objective not met: %d dropped, %d failed", dropped, failed)
	}
	fmt.Printf("\n  100%% placement, zero drops.\n")
	return nil
}

func percentiles(d []time.Duration) (p50, p95, p99, max time.Duration) {
	if len(d) == 0 {
		return 0, 0, 0, 0
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	at := func(q float64) time.Duration {
		i := int(math.Ceil(q*float64(len(d)))) - 1
		if i < 0 {
			i = 0
		}
		if i >= len(d) {
			i = len(d) - 1
		}
		return d[i]
	}
	return at(0.50), at(0.95), at(0.99), d[len(d)-1]
}
