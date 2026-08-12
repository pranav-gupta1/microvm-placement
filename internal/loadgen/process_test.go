package loadgen

import (
	"math"
	"sort"
	"testing"
	"time"
)

// productionEnvelope is the shape the assignment actually asks for, scaled down
// in wall-clock time so the unit tests stay fast. The rate axis is full scale.
func productionEnvelope() Envelope {
	return Repeat{Inner: Ramp{Up: 10 * time.Second, Down: 10 * time.Second, Peak: 1000}, Times: 2}
}

func TestNewProcessRejectsBadInput(t *testing.T) {
	tests := []struct {
		name string
		env  Envelope
	}{
		{"nil envelope", nil},
		{"zero duration", Ramp{Peak: 100}},
		{"negative peak", Ramp{Up: time.Second, Down: time.Second, Peak: -5}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewProcess(tc.env, 1); err == nil {
				t.Fatal("NewProcess() error = nil, want non-nil")
			}
		})
	}
}

func TestArrivalsAreMonotonicAndInsideEnvelope(t *testing.T) {
	env := productionEnvelope()
	got, err := Arrivals(env, 42)
	if err != nil {
		t.Fatalf("Arrivals() error = %v", err)
	}
	if len(got) == 0 {
		t.Fatal("Arrivals() returned no arrivals")
	}
	if !sort.SliceIsSorted(got, func(i, j int) bool { return got[i] < got[j] }) {
		t.Error("arrival offsets are not monotonically increasing")
	}
	for i, at := range got {
		if at < 0 || at > env.Duration() {
			t.Fatalf("arrival %d at %s falls outside envelope [0, %s]", i, at, env.Duration())
		}
	}
}

func TestArrivalsAreReproducibleForAGivenSeed(t *testing.T) {
	env := productionEnvelope()

	first, err := Arrivals(env, 7)
	if err != nil {
		t.Fatalf("Arrivals() error = %v", err)
	}
	second, err := Arrivals(env, 7)
	if err != nil {
		t.Fatalf("Arrivals() error = %v", err)
	}
	if len(first) != len(second) {
		t.Fatalf("same seed produced %d and %d arrivals", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("same seed diverged at index %d: %s vs %s", i, first[i], second[i])
		}
	}

	// A different seed must actually produce a different sample path,
	// otherwise the seed is not being threaded into the generator at all.
	other, err := Arrivals(env, 8)
	if err != nil {
		t.Fatalf("Arrivals() error = %v", err)
	}
	if len(other) == len(first) {
		identical := true
		for i := range first {
			if first[i] != other[i] {
				identical = false
				break
			}
		}
		if identical {
			t.Error("different seeds produced an identical arrival sequence")
		}
	}
}

func TestProcessIsExhaustedAfterEnvelopeEnds(t *testing.T) {
	p, err := NewProcess(Ramp{Up: time.Second, Down: time.Second, Peak: 50}, 3)
	if err != nil {
		t.Fatalf("NewProcess() error = %v", err)
	}
	for {
		if _, ok := p.Next(); !ok {
			break
		}
	}
	// Once exhausted it must stay exhausted, so a caller draining the process
	// in a loop terminates.
	for i := 0; i < 5; i++ {
		if _, ok := p.Next(); ok {
			t.Fatal("Next() produced an arrival after the envelope was exhausted")
		}
	}
}

func TestProcessWithZeroPeakTerminatesImmediately(t *testing.T) {
	// A zero-rate envelope must not spin forever hunting for an arrival that
	// can never be accepted.
	p, err := NewProcess(Ramp{Up: time.Second, Down: time.Second, Peak: 0}, 1)
	if err != nil {
		t.Fatalf("NewProcess() error = %v", err)
	}
	if _, ok := p.Next(); ok {
		t.Fatal("Next() produced an arrival from a zero-rate envelope")
	}
}

func TestArrivalCountMatchesEnvelopeIntegral(t *testing.T) {
	// For a Poisson process the arrival count has mean and variance both equal
	// to the integral of the rate function. Averaging over many seeds shrinks
	// the sampling error to well inside the tolerance below, so this test is
	// deterministic in practice rather than flaky.
	env := productionEnvelope()
	want := ExpectedArrivals(env, 100000)

	const runs = 24
	var total float64
	for seed := uint64(0); seed < runs; seed++ {
		got, err := Arrivals(env, seed)
		if err != nil {
			t.Fatalf("Arrivals() error = %v", err)
		}
		total += float64(len(got))
	}
	mean := total / runs

	// Standard error of the mean is sqrt(want/runs), about 29 for want=20000.
	// A 2% band is therefore roughly 13 sigma.
	if rel := math.Abs(mean-want) / want; rel > 0.02 {
		t.Errorf("mean arrivals over %d runs = %.1f, want %.1f (off by %.2f%%)", runs, mean, want, rel*100)
	}
}

func TestLocalArrivalRateTracksEnvelope(t *testing.T) {
	// A long hold gives a region of genuinely constant rate, so we can check
	// that the sampler delivers the requested rate locally and not merely in
	// aggregate. An envelope-wide count would pass even if the sampler put all
	// its arrivals in the wrong place.
	env := Ramp{Up: 2 * time.Second, Hold: 30 * time.Second, Down: 2 * time.Second, Peak: 1000}
	arrivals, err := Arrivals(env, 11)
	if err != nil {
		t.Fatalf("Arrivals() error = %v", err)
	}

	// Count arrivals in each one-second slice of the hold region.
	const holdStart, holdEnd = 2, 32
	buckets := make([]int, holdEnd-holdStart)
	for _, at := range arrivals {
		sec := int(at.Seconds())
		if sec >= holdStart && sec < holdEnd {
			buckets[sec-holdStart]++
		}
	}
	for i, count := range buckets {
		// Each bucket is Poisson with mean 1000, so sigma is about 32. A 15%
		// band is roughly 4.7 sigma.
		if rel := math.Abs(float64(count)-1000) / 1000; rel > 0.15 {
			t.Errorf("second %d of hold saw %d arrivals, want about 1000", holdStart+i, count)
		}
	}
}

func TestInterarrivalsAreExponential(t *testing.T) {
	// Burstiness is the point of using a Poisson process rather than a fixed
	// tick. If the interarrival distribution were not exponential we would be
	// generating smooth traffic while claiming otherwise, and the system would
	// never see the transient bursts it is supposed to absorb.
	//
	// Within the flat hold the process is homogeneous with rate 1000, so gaps
	// should be Exp(1000). We check that with a Kolmogorov-Smirnov statistic.
	env := Ramp{Up: 1 * time.Second, Hold: 60 * time.Second, Down: 1 * time.Second, Peak: 1000}
	arrivals, err := Arrivals(env, 5)
	if err != nil {
		t.Fatalf("Arrivals() error = %v", err)
	}

	const holdStart, holdEnd = 1.0, 61.0
	var gaps []float64
	var prev float64
	for _, at := range arrivals {
		s := at.Seconds()
		if s < holdStart || s > holdEnd {
			continue
		}
		if prev != 0 {
			gaps = append(gaps, s-prev)
		}
		prev = s
	}
	if len(gaps) < 1000 {
		t.Fatalf("only %d interarrival gaps in the hold region, need at least 1000", len(gaps))
	}
	sort.Float64s(gaps)

	// Empirical CDF against the Exp(1000) CDF, F(x) = 1 - exp(-lambda x).
	const lambda = 1000.0
	var ks float64
	n := float64(len(gaps))
	for i, g := range gaps {
		want := 1 - math.Exp(-lambda*g)
		// Compare both edges of the empirical step to avoid a systematic 1/n bias.
		below := math.Abs(float64(i)/n - want)
		above := math.Abs(float64(i+1)/n - want)
		if d := math.Max(below, above); d > ks {
			ks = d
		}
	}
	// Two-sided KS critical value at alpha = 0.001 is about 1.95/sqrt(n).
	critical := 1.95 / math.Sqrt(n)
	if ks > critical {
		t.Errorf("KS statistic %.5f exceeds critical value %.5f for n=%d, interarrivals are not Exp(%v)", ks, critical, len(gaps), lambda)
	}
}

func TestArrivalsCoverBothCycles(t *testing.T) {
	// Regression guard for the Repeat wiring: a modulo bug that collapsed the
	// two cycles into one would still produce a plausible-looking count, so
	// assert that load actually lands in the second cycle.
	env := productionEnvelope()
	arrivals, err := Arrivals(env, 99)
	if err != nil {
		t.Fatalf("Arrivals() error = %v", err)
	}

	half := env.Duration() / 2
	var first, second int
	for _, at := range arrivals {
		if at < half {
			first++
		} else {
			second++
		}
	}
	if first == 0 || second == 0 {
		t.Fatalf("expected load in both cycles, got first=%d second=%d", first, second)
	}
	// The two cycles are identical, so their counts should be close.
	if rel := math.Abs(float64(first-second)) / float64(first+second); rel > 0.05 {
		t.Errorf("cycles are unbalanced: first=%d second=%d", first, second)
	}
}

func BenchmarkProcessNext(b *testing.B) {
	env := Ramp{Up: time.Hour, Hold: time.Hour, Down: time.Hour, Peak: 1000}
	p, err := NewProcess(env, 1)
	if err != nil {
		b.Fatalf("NewProcess() error = %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := p.Next(); !ok {
			b.Fatal("envelope exhausted during benchmark")
		}
	}
}
