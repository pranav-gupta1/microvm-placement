package autoscale

import (
	"math"
	"testing"
	"time"
)

// productionConfig mirrors the sizing derived in docs/capacity-planning.md.
func productionConfig() Config {
	return Config{
		SlotsPerHost:           8,
		TargetUtilisation:      0.8,
		MeanTTL:                500 * time.Millisecond,
		ProvisionLatency:       20 * time.Second,
		ScaleDownStabilization: 30 * time.Second,
		MinReplicas:            2,
		MaxReplicas:            200,
	}
}

func mustNew(t *testing.T, cfg Config) *Autoscaler {
	t.Helper()
	a, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return a
}

func TestConfigValidate(t *testing.T) {
	valid := productionConfig()
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"valid", func(*Config) {}, false},
		{"one slot per host", func(c *Config) { c.SlotsPerHost = 1 }, true},
		{"zero utilisation", func(c *Config) { c.TargetUtilisation = 0 }, true},
		{"utilisation above one", func(c *Config) { c.TargetUtilisation = 1.5 }, true},
		{"zero ttl", func(c *Config) { c.MeanTTL = 0 }, true},
		{"negative provision latency", func(c *Config) { c.ProvisionLatency = -time.Second }, true},
		{"negative stabilization", func(c *Config) { c.ScaleDownStabilization = -time.Second }, true},
		{"negative min", func(c *Config) { c.MinReplicas = -1 }, true},
		{"max below min", func(c *Config) { c.MinReplicas = 10; c.MaxReplicas = 5 }, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			tc.mutate(&cfg)
			if _, err := New(cfg); (err != nil) != tc.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// TestSteadyStateMatchesTheCapacityPlan pins the arithmetic in the design doc.
// At the assignment's peak the fleet must land on the 79 pods derived there.
func TestSteadyStateMatchesTheCapacityPlan(t *testing.T) {
	a := mustNew(t, productionConfig())
	now := time.Now()

	// Two identical samples so the slope is zero and no lead term applies.
	a.Decide(Sample{At: now, ArrivalRate: 1000, InflightVMs: 500})
	d := a.Decide(Sample{At: now.Add(time.Second), ArrivalRate: 1000, InflightVMs: 500})

	// 1000 req/s x 0.5 s = 500 microVMs; 500 / 0.8 = 625 slots; 625 / 8 = 79 pods.
	if d.Replicas != 79 {
		t.Errorf("Replicas = %d, want 79 to match the capacity plan", d.Replicas)
	}
	if d.LeadTerm != 0 {
		t.Errorf("LeadTerm = %v, want 0 at steady state", d.LeadTerm)
	}
}

// TestLeadTermAnticipatesTheRamp is the reason this package exists. On a
// climbing ramp the fleet must be sized for the traffic that will exist once
// pods are ready, not for the traffic that already arrived.
func TestLeadTermAnticipatesTheRamp(t *testing.T) {
	cfg := productionConfig()
	cfg.ScaleDownStabilization = 0
	a := mustNew(t, cfg)
	now := time.Now()

	// A ramp climbing at 100 requests per second, per second.
	a.Decide(Sample{At: now, ArrivalRate: 100, InflightVMs: 50})
	d := a.Decide(Sample{At: now.Add(time.Second), ArrivalRate: 200, InflightVMs: 100})

	// Slope is 100 rps/s over a 20 s horizon, so the lead term is 2000 rps.
	if math.Abs(d.LeadTerm-2000) > 1e-6 {
		t.Errorf("LeadTerm = %v, want 2000", d.LeadTerm)
	}
	// Predicted rate 200 + 2000 = 2200 rps, so 1100 concurrent microVMs.
	if math.Abs(d.PredictedVMs-1100) > 1e-6 {
		t.Errorf("PredictedVMs = %v, want 1100", d.PredictedVMs)
	}

	// A reactive scaler would size for the 100 microVMs in flight right now,
	// which is 13 pods. The lead compensator asks for far more, and that gap
	// is exactly the requests a reactive scaler would drop.
	reactive := int(math.Ceil(100 / 0.8 / 8))
	if d.Replicas <= reactive {
		t.Errorf("Replicas = %d, want more than the reactive answer of %d", d.Replicas, reactive)
	}
}

func TestFallingRateProducesNoNegativeLead(t *testing.T) {
	cfg := productionConfig()
	cfg.ScaleDownStabilization = 0
	a := mustNew(t, cfg)
	now := time.Now()

	a.Decide(Sample{At: now, ArrivalRate: 1000, InflightVMs: 500})
	d := a.Decide(Sample{At: now.Add(time.Second), ArrivalRate: 500, InflightVMs: 250})

	// A negative lead term would shed capacity faster than demand is actually
	// falling, dropping work that is still arriving.
	if d.LeadTerm != 0 {
		t.Errorf("LeadTerm = %v on a falling ramp, want 0", d.LeadTerm)
	}
	if d.PredictedRate != 500 {
		t.Errorf("PredictedRate = %v, want 500", d.PredictedRate)
	}
}

// TestInflightActsAsAFloor guards against trusting the model over reality. If
// the predicted concurrency is somehow low, the microVMs actually running must
// still be covered.
func TestInflightActsAsAFloor(t *testing.T) {
	cfg := productionConfig()
	cfg.ScaleDownStabilization = 0
	a := mustNew(t, cfg)
	now := time.Now()

	// Zero measured arrival rate but 400 microVMs still running, which is what
	// the tail of a ramp looks like.
	a.Decide(Sample{At: now, ArrivalRate: 0, InflightVMs: 400})
	d := a.Decide(Sample{At: now.Add(time.Second), ArrivalRate: 0, InflightVMs: 400})

	// 400 / 0.8 / 8 = 63 pods.
	if d.Replicas != 63 {
		t.Errorf("Replicas = %d, want 63 to cover the microVMs actually running", d.Replicas)
	}
}

func TestScaleUpIsImmediate(t *testing.T) {
	a := mustNew(t, productionConfig())
	now := time.Now()

	a.Decide(Sample{At: now, ArrivalRate: 10, InflightVMs: 5})
	low := a.Decide(Sample{At: now.Add(time.Second), ArrivalRate: 10, InflightVMs: 5})
	high := a.Decide(Sample{At: now.Add(2 * time.Second), ArrivalRate: 1000, InflightVMs: 500})

	if high.Replicas <= low.Replicas {
		t.Errorf("Replicas did not grow on a load spike: %d then %d", low.Replicas, high.Replicas)
	}
	if high.HeldByStabilization {
		t.Error("scale-up must never be held by the stabilisation window")
	}
}

// TestScaleDownIsHeldByTheStabilizationWindow covers the trough between the
// assignment's two load cycles, where a naive controller scales to zero and
// then cannot recover in time for the second climb.
func TestScaleDownIsHeldByTheStabilizationWindow(t *testing.T) {
	cfg := productionConfig()
	cfg.ScaleDownStabilization = 30 * time.Second
	a := mustNew(t, cfg)
	now := time.Now()

	a.Decide(Sample{At: now, ArrivalRate: 1000, InflightVMs: 500})
	peak := a.Decide(Sample{At: now.Add(time.Second), ArrivalRate: 1000, InflightVMs: 500})

	// Demand collapses. Inside the window the fleet must hold its size.
	held := a.Decide(Sample{At: now.Add(6 * time.Second), ArrivalRate: 0, InflightVMs: 0})
	if held.Replicas != peak.Replicas {
		t.Errorf("Replicas = %d inside the stabilisation window, want %d held", held.Replicas, peak.Replicas)
	}
	if !held.HeldByStabilization {
		t.Error("HeldByStabilization = false, want true while the window is open")
	}

	// Once the window elapses with sustained low demand, capacity is released.
	released := a.Decide(Sample{At: now.Add(45 * time.Second), ArrivalRate: 0, InflightVMs: 0})
	if released.Replicas != cfg.MinReplicas {
		t.Errorf("Replicas = %d after the window elapsed, want the floor of %d", released.Replicas, cfg.MinReplicas)
	}
	if released.HeldByStabilization {
		t.Error("HeldByStabilization = true after the window elapsed, want false")
	}
}

func TestStabilizationWindowRestartsOnEachNewPeak(t *testing.T) {
	cfg := productionConfig()
	cfg.ScaleDownStabilization = 30 * time.Second
	a := mustNew(t, cfg)
	now := time.Now()

	a.Decide(Sample{At: now, ArrivalRate: 1000, InflightVMs: 500})
	a.Decide(Sample{At: now.Add(time.Second), ArrivalRate: 1000, InflightVMs: 500})
	// A brief lull, then load returns before the window expires.
	a.Decide(Sample{At: now.Add(20 * time.Second), ArrivalRate: 0, InflightVMs: 0})
	a.Decide(Sample{At: now.Add(25 * time.Second), ArrivalRate: 1000, InflightVMs: 500})

	// The window restarted at t=25s, so at t=45s it is still open.
	held := a.Decide(Sample{At: now.Add(45 * time.Second), ArrivalRate: 0, InflightVMs: 0})
	if !held.HeldByStabilization {
		t.Error("the stabilisation window did not restart on the new peak")
	}
}

func TestMinAndMaxReplicasAreEnforced(t *testing.T) {
	cfg := productionConfig()
	cfg.MinReplicas = 3
	cfg.MaxReplicas = 10
	cfg.ScaleDownStabilization = 0
	a := mustNew(t, cfg)
	now := time.Now()

	// Idle: the floor covers the cold start, where there is no traffic to
	// measure and no history to extrapolate from.
	a.Decide(Sample{At: now, ArrivalRate: 0, InflightVMs: 0})
	idle := a.Decide(Sample{At: now.Add(time.Second), ArrivalRate: 0, InflightVMs: 0})
	if idle.Replicas != 3 {
		t.Errorf("Replicas = %d when idle, want the floor of 3", idle.Replicas)
	}

	// Overload: the ceiling bounds both cost and the damage a bad signal can do.
	huge := a.Decide(Sample{At: now.Add(2 * time.Second), ArrivalRate: 100000, InflightVMs: 50000})
	if huge.Replicas != 10 {
		t.Errorf("Replicas = %d under extreme load, want the ceiling of 10", huge.Replicas)
	}
}

func TestOutOfOrderAndDuplicateSamplesDoNotBreakTheSlope(t *testing.T) {
	cfg := productionConfig()
	cfg.ScaleDownStabilization = 0
	a := mustNew(t, cfg)
	now := time.Now()

	a.Decide(Sample{At: now, ArrivalRate: 100, InflightVMs: 50})
	a.Decide(Sample{At: now.Add(time.Second), ArrivalRate: 200, InflightVMs: 100})

	// A duplicate timestamp must not divide by zero or invent a discontinuity.
	dup := a.Decide(Sample{At: now.Add(time.Second), ArrivalRate: 200, InflightVMs: 100})
	if math.IsNaN(dup.PredictedRate) || math.IsInf(dup.PredictedRate, 0) {
		t.Fatalf("PredictedRate = %v on a duplicate sample", dup.PredictedRate)
	}
	// An out-of-order sample must be equally harmless.
	old := a.Decide(Sample{At: now, ArrivalRate: 150, InflightVMs: 75})
	if math.IsNaN(old.PredictedRate) || math.IsInf(old.PredictedRate, 0) {
		t.Fatalf("PredictedRate = %v on an out-of-order sample", old.PredictedRate)
	}
}

func TestFirstSampleHasNoSlope(t *testing.T) {
	a := mustNew(t, productionConfig())
	d := a.Decide(Sample{At: time.Now(), ArrivalRate: 500, InflightVMs: 250})
	// With no history there is nothing to extrapolate from, so the lead term
	// must be zero rather than an artefact of a zero-valued previous sample.
	if d.LeadTerm != 0 {
		t.Errorf("LeadTerm = %v on the first sample, want 0", d.LeadTerm)
	}
}

func TestReset(t *testing.T) {
	a := mustNew(t, productionConfig())
	now := time.Now()
	a.Decide(Sample{At: now, ArrivalRate: 1000, InflightVMs: 500})
	a.Decide(Sample{At: now.Add(time.Second), ArrivalRate: 1000, InflightVMs: 500})

	a.Reset()

	d := a.Decide(Sample{At: now.Add(2 * time.Second), ArrivalRate: 100, InflightVMs: 50})
	if d.HeldByStabilization {
		t.Error("stabilisation state survived Reset")
	}
	if d.LeadTerm != 0 {
		t.Errorf("LeadTerm = %v after Reset, want 0", d.LeadTerm)
	}
}

// TestFleetTracksTheDoubleRamp walks the actual assignment shape and asserts
// the qualitative behaviour the objective asks for: enough capacity at both
// peaks, and genuine shrinkage in between rather than a flat allocation.
func TestFleetTracksTheDoubleRamp(t *testing.T) {
	cfg := productionConfig()
	cfg.ScaleDownStabilization = 10 * time.Second
	a := mustNew(t, cfg)

	start := time.Now()
	const cycle = 60 // seconds per cycle: 30 up, 30 down

	var atFirstPeak, atTrough, atSecondPeak int
	for sec := 0; sec <= 2*cycle; sec++ {
		// Triangular rate, two cycles back to back.
		phase := sec % cycle
		var rate float64
		if phase < cycle/2 {
			rate = 1000 * float64(phase) / float64(cycle/2)
		} else {
			rate = 1000 * (1 - float64(phase-cycle/2)/float64(cycle/2))
		}
		inflight := int(rate * cfg.MeanTTL.Seconds())

		d := a.Decide(Sample{At: start.Add(time.Duration(sec) * time.Second), ArrivalRate: rate, InflightVMs: inflight})

		switch sec {
		case cycle / 2:
			atFirstPeak = d.Replicas
		case cycle:
			atTrough = d.Replicas
		case cycle + cycle/2:
			atSecondPeak = d.Replicas
		}
	}

	if atFirstPeak < 79 {
		t.Errorf("first peak allocated %d pods, want at least the 79 the plan requires", atFirstPeak)
	}
	if atSecondPeak < 79 {
		t.Errorf("second peak allocated %d pods, want at least 79", atSecondPeak)
	}
	// The whole objective is that the trough is genuinely cheaper. A scaler
	// that just holds peak capacity forever would pass a zero-drop test while
	// failing the cost half of the assignment.
	if atTrough >= atFirstPeak {
		t.Errorf("trough allocated %d pods against a peak of %d, capacity is not being reclaimed", atTrough, atFirstPeak)
	}
}
