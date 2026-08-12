package sim

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/pranav-gupta1/microvm-placement/internal/autoscale"
	"github.com/pranav-gupta1/microvm-placement/internal/loadgen"
	"github.com/pranav-gupta1/microvm-placement/internal/placement"
)

// doubleRamp is the shape the assignment specifies: climb to peak, fall back to
// zero, twice back to back.
func doubleRamp(ramp time.Duration, peak float64) loadgen.Envelope {
	return loadgen.Repeat{Inner: loadgen.Ramp{Up: ramp, Down: ramp, Peak: peak}, Times: 2}
}

// config builds a run whose timings are scaled so that pod start latency stays
// in proportion to ramp duration. Compressing wall-clock without compressing
// the latency the autoscaler must anticipate would make the problem trivially
// easy and the result meaningless.
func config(ramp time.Duration, peak float64, podStart time.Duration) Config {
	return Config{
		Envelope:        doubleRamp(ramp, peak),
		Seed:            1,
		MeanTTL:         500 * time.Millisecond,
		SlotsPerHost:    8,
		PodStartLatency: podStart,
		SampleInterval:  250 * time.Millisecond,
		Autoscale: autoscale.Config{
			SlotsPerHost:           8,
			TargetUtilisation:      0.8,
			MeanTTL:                500 * time.Millisecond,
			ProvisionLatency:       podStart,
			ScaleDownStabilization: 3 * time.Second,
			MinReplicas:            4,
			MaxReplicas:            400,
		},
		Placement: placement.Config{
			QueueDepth:        2048,
			AdmissionDeadline: 3 * time.Second,
			RetryInterval:     2 * time.Millisecond,
		},
	}
}

func report(t *testing.T, name string, res Result) {
	t.Helper()
	t.Logf("%s\n"+
		"  offered            %d\n"+
		"  placed             %d  (%.4f%%)\n"+
		"  dropped            %d  (timeout %d, queue full %d)\n"+
		"  peak inflight VMs  %d\n"+
		"  peak ready hosts   %d\n"+
		"  max queue depth    %d\n"+
		"  host-seconds       %.1f  (idle %.1f, %.1f%%)\n"+
		"  admission wait     p50 %s  p99 %s  max %s",
		name,
		res.Offered,
		res.Placed, res.PlacementRate()*100,
		res.Dropped, res.TimedOut, res.QueueRejected,
		res.PeakInflight,
		res.PeakHosts,
		res.MaxQueueDepth,
		res.HostSeconds, res.IdleHostSeconds, res.IdleFraction()*100,
		res.WaitP50, res.WaitP99, res.WaitMax)
}

func TestConfigValidate(t *testing.T) {
	valid := config(time.Second, 100, 100*time.Millisecond)
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"valid", func(*Config) {}, false},
		{"nil envelope", func(c *Config) { c.Envelope = nil }, true},
		{"zero ttl", func(c *Config) { c.MeanTTL = 0 }, true},
		{"one slot per host", func(c *Config) { c.SlotsPerHost = 1 }, true},
		{"zero sample interval", func(c *Config) { c.SampleInterval = 0 }, true},
		{"bad autoscale config", func(c *Config) { c.Autoscale.TargetUtilisation = 0 }, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			tc.mutate(&cfg)
			if _, err := Run(context.Background(), cfg); (err != nil) != tc.wantErr {
				t.Errorf("Run() error presence = %v, wantErr %v", err != nil, tc.wantErr)
			}
		})
	}
}

// TestModestDoubleRampPlacesEverything is the fast always-on version of the
// headline claim, kept short enough to run on every save.
func TestModestDoubleRampPlacesEverything(t *testing.T) {
	cfg := config(2*time.Second, 200, 400*time.Millisecond)

	res, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	report(t, "modest double ramp, 200 rps peak", res)

	if res.Dropped != 0 {
		t.Errorf("dropped %d requests, the objective requires zero", res.Dropped)
	}
	if res.PlacementRate() != 1.0 {
		t.Errorf("placement rate = %.4f, want exactly 1.0", res.PlacementRate())
	}
	if res.Offered == 0 {
		t.Fatal("no requests were offered")
	}
}

// TestFullDoubleRampAtPeakRPS is the assignment's actual load: 1000 requests
// per second, two cycles, 100% placement required.
//
// Ramp duration is compressed relative to a production run so this completes in
// under a minute, but the rate axis is full scale and pod start latency is
// scaled in proportion, so the autoscaler faces the same problem shape.
func TestFullDoubleRampAtPeakRPS(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the full 1000 rps run in short mode")
	}
	cfg := config(8*time.Second, 1000, 1500*time.Millisecond)

	start := time.Now()
	res, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	report(t, "full double ramp, 1000 rps peak", res)
	t.Logf("  wall clock         %s", time.Since(start).Round(time.Millisecond))

	// The requirement, stated plainly.
	if res.Dropped != 0 {
		t.Errorf("dropped %d requests, the objective requires zero", res.Dropped)
	}
	if res.PlacementRate() != 1.0 {
		t.Errorf("placement rate = %.6f, want exactly 1.0", res.PlacementRate())
	}

	// The harness must actually have delivered the load it promised, or a
	// zero-drop result would be meaningless.
	want := loadgen.ExpectedArrivals(cfg.Envelope, 100000)
	if rel := math.Abs(float64(res.Offered)-want) / want; rel > 0.05 {
		t.Errorf("offered %d requests, want about %.0f (off by %.1f%%)", res.Offered, want, rel*100)
	}

	// Little's Law: 1000 rps at a 500 ms mean TTL is about 500 concurrent
	// microVMs. A wildly different peak means the TTL model is not being
	// honoured and the whole sizing argument would be void.
	if res.PeakInflight < 350 || res.PeakInflight > 750 {
		t.Errorf("peak inflight microVMs = %d, want roughly 500", res.PeakInflight)
	}
}

// TestFleetShrinksInTheTroughAndRecovers is the cost half of the objective.
// Zero drops is easy if you simply never scale down, so this asserts that
// capacity is genuinely reclaimed between the two cycles and then rebuilt.
func TestFleetShrinksInTheTroughAndRecovers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the timeline-shape run in short mode")
	}
	cfg := config(6*time.Second, 600, time.Second)

	res, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	report(t, "shape check, 600 rps peak", res)

	if res.Dropped != 0 {
		t.Errorf("dropped %d requests, the objective requires zero", res.Dropped)
	}
	if len(res.Timeline) < 8 {
		t.Fatalf("timeline has only %d samples, too few to judge shape", len(res.Timeline))
	}

	// Split the timeline into first cycle, trough, and second cycle. The
	// trough is the quarter of the run centred on the boundary between cycles.
	total := res.Timeline[len(res.Timeline)-1].Elapsed
	var firstPeak, troughMin, secondPeak int
	troughMin = math.MaxInt32
	for _, s := range res.Timeline {
		frac := float64(s.Elapsed) / float64(total)
		switch {
		case frac < 0.375:
			if s.ReadyHosts > firstPeak {
				firstPeak = s.ReadyHosts
			}
		case frac < 0.625:
			if s.ReadyHosts < troughMin {
				troughMin = s.ReadyHosts
			}
		default:
			if s.ReadyHosts > secondPeak {
				secondPeak = s.ReadyHosts
			}
		}
	}

	t.Logf("  first peak %d hosts, trough %d hosts, second peak %d hosts", firstPeak, troughMin, secondPeak)

	if firstPeak == 0 || secondPeak == 0 {
		t.Fatalf("fleet never scaled up: first peak %d, second peak %d", firstPeak, secondPeak)
	}
	// Capacity must actually be given back, otherwise the compute bill never
	// falls and the idle-pod objective is unmet.
	if troughMin >= firstPeak {
		t.Errorf("trough held %d hosts against a first peak of %d, capacity is not being reclaimed", troughMin, firstPeak)
	}
	// And it must come back for the second climb, which is where a scaler that
	// shed capacity too aggressively would start dropping.
	if secondPeak < firstPeak/2 {
		t.Errorf("second peak reached only %d hosts against a first peak of %d", secondPeak, firstPeak)
	}
}
