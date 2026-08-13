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
func TestSteadyStateMatchesTheCapacityPlan(t *testing.T) {
	a := mustNew(t, productionConfig())
	now := time.Now()

	a.Decide(Sample{At: now, ArrivalRate: 1000, InflightVMs: 500})
	d := a.Decide(Sample{At: now.Add(time.Second), ArrivalRate: 1000, InflightVMs: 500})

	if d.Replicas != 79 {
		t.Errorf("Replicas = %d, want 79 to match the capacity plan", d.Replicas)
	}
	if d.LeadTerm != 0 {
		t.Errorf("LeadTerm = %v, want 0 at steady state", d.LeadTerm)
	}
}

// TestLeadTermAnticipatesTheRamp is the reason this package exists.
func TestLeadTermAnticipatesTheRamp(t *testing.T) {
	cfg := productionConfig()
	cfg.ScaleDownStabilization = 0
	a := mustNew(t, cfg)
	now := time.Now()

	a.Decide(Sample{At: now, ArrivalRate: 100, InflightVMs: 50})
	d := a.Decide(Sample{At: now.Add(time.Second), ArrivalRate: 200, InflightVMs: 100})

	if math.Abs(d.LeadTerm-2000) > 1e-6 {
		t.Errorf("LeadTerm = %v, want 2000", d.LeadTerm)
	}
	if math.Abs(d.PredictedVMs-1100) > 1e-6 {
		t.Errorf("PredictedVMs = %v, want 1100", d.PredictedVMs)
	}

	reactive := int(math.Ceil(100 / 0.8 / 8))
	if d.Replicas <= reactive {
		t.Errorf("Replicas = %d, want more than the reactive answer of %d", d.Replicas, reactive)
	}
}

// TestNoisyFlatLoadDoesNotInflateTheFleet is a regression test for a real
// over-provisioning bug, and the reason the slope is estimated by regression
// rather than by differencing consecutive samples.
func TestNoisyFlatLoadDoesNotInflateTheFleet(t *testing.T) {
	cfg := productionConfig()
	cfg.ScaleDownStabilization = 0
	a := mustNew(t, cfg)

	const (
		trueRate = 500.0
		interval = 250 * time.Millisecond
		noise    = 45.0
	)
	now := time.Now()
	var maxReplicas int
	for i := 0; i < 80; i++ {
		sign := 1.0
		if i%2 == 1 {
			sign = -1.0
		}
		rate := trueRate + sign*noise
		d := a.Decide(Sample{
			At:          now.Add(time.Duration(i) * interval),
			ArrivalRate: rate,
			InflightVMs: int(trueRate * cfg.MeanTTL.Seconds()),
		})
		if i > 12 && d.Replicas > maxReplicas {
			maxReplicas = d.Replicas
		}
	}

	const steadyState = 40
	if maxReplicas > steadyState+8 {
		t.Errorf("flat but noisy load provisioned %d pods, want no more than %d: the derivative is amplifying noise",
			maxReplicas, steadyState+8)
	}
}

func TestFallingRateProducesNoNegativeLead(t *testing.T) {
	cfg := productionConfig()
	cfg.ScaleDownStabilization = 0
	a := mustNew(t, cfg)
	now := time.Now()

	a.Decide(Sample{At: now, ArrivalRate: 1000, InflightVMs: 500})
	d := a.Decide(Sample{At: now.Add(time.Second), ArrivalRate: 500, InflightVMs: 250})

	if d.LeadTerm != 0 {
		t.Errorf("LeadTerm = %v on a falling ramp, want 0", d.LeadTerm)
	}
	if d.PredictedRate != 500 {
		t.Errorf("PredictedRate = %v, want 500", d.PredictedRate)
	}
}

// TestInflightActsAsAFloor guards against trusting the model over reality.
func TestInflightActsAsAFloor(t *testing.T) {
	cfg := productionConfig()
	cfg.ScaleDownStabilization = 0
	a := mustNew(t, cfg)
	now := time.Now()

	a.Decide(Sample{At: now, ArrivalRate: 0, InflightVMs: 400})
	d := a.Decide(Sample{At: now.Add(time.Second), ArrivalRate: 0, InflightVMs: 400})

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

	held := a.Decide(Sample{At: now.Add(6 * time.Second), ArrivalRate: 0, InflightVMs: 0})
	if held.Replicas != peak.Replicas {
		t.Errorf("Replicas = %d inside the stabilisation window, want %d held", held.Replicas, peak.Replicas)
	}
	if !held.HeldByStabilization {
		t.Error("HeldByStabilization = false, want true while the window is open")
	}

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
	a.Decide(Sample{At: now.Add(20 * time.Second), ArrivalRate: 0, InflightVMs: 0})
	a.Decide(Sample{At: now.Add(25 * time.Second), ArrivalRate: 1000, InflightVMs: 500})

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

	a.Decide(Sample{At: now, ArrivalRate: 0, InflightVMs: 0})
	idle := a.Decide(Sample{At: now.Add(time.Second), ArrivalRate: 0, InflightVMs: 0})
	if idle.Replicas != 3 {
		t.Errorf("Replicas = %d when idle, want the floor of 3", idle.Replicas)
	}

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

	dup := a.Decide(Sample{At: now.Add(time.Second), ArrivalRate: 200, InflightVMs: 100})
	if math.IsNaN(dup.PredictedRate) || math.IsInf(dup.PredictedRate, 0) {
		t.Fatalf("PredictedRate = %v on a duplicate sample", dup.PredictedRate)
	}
	old := a.Decide(Sample{At: now, ArrivalRate: 150, InflightVMs: 75})
	if math.IsNaN(old.PredictedRate) || math.IsInf(old.PredictedRate, 0) {
		t.Fatalf("PredictedRate = %v on an out-of-order sample", old.PredictedRate)
	}
}

func TestFirstSampleHasNoSlope(t *testing.T) {
	a := mustNew(t, productionConfig())
	d := a.Decide(Sample{At: time.Now(), ArrivalRate: 500, InflightVMs: 250})
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
	if atTrough >= atFirstPeak {
		t.Errorf("trough allocated %d pods against a peak of %d, capacity is not being reclaimed", atTrough, atFirstPeak)
	}
}
