package loadgen

import (
	"math"
	"testing"
	"time"
)

const tolerance = 1e-9

func assertRate(t *testing.T, env Envelope, at time.Duration, want float64) {
	t.Helper()
	if got := env.RateAt(at); math.Abs(got-want) > tolerance {
		t.Errorf("RateAt(%s) = %v, want %v", at, got, want)
	}
}

func TestRampRateAtTriangle(t *testing.T) {
	r := Ramp{Up: 10 * time.Second, Down: 10 * time.Second, Peak: 1000}

	if got, want := r.Duration(), 20*time.Second; got != want {
		t.Fatalf("Duration() = %s, want %s", got, want)
	}

	assertRate(t, r, 0, 0)
	assertRate(t, r, 5*time.Second, 500)
	assertRate(t, r, 10*time.Second, 1000)
	assertRate(t, r, 15*time.Second, 500)
	assertRate(t, r, 20*time.Second, 0)
}

func TestRampRateAtTrapezoid(t *testing.T) {
	r := Ramp{Up: 4 * time.Second, Hold: 10 * time.Second, Down: 6 * time.Second, Peak: 800}

	assertRate(t, r, 0, 0)
	assertRate(t, r, 2*time.Second, 400)
	assertRate(t, r, 4*time.Second, 800)
	assertRate(t, r, 9*time.Second, 800)
	assertRate(t, r, 14*time.Second, 800)
	assertRate(t, r, 17*time.Second, 400)
	assertRate(t, r, 20*time.Second, 0)
}

func TestRampRateAtOutsideEnvelopeIsZero(t *testing.T) {
	r := Ramp{Up: time.Second, Down: time.Second, Peak: 100}
	assertRate(t, r, -time.Second, 0)
	assertRate(t, r, 5*time.Second, 0)
}

func TestRampNeverExceedsPeak(t *testing.T) {
	r := Ramp{Up: 7 * time.Second, Hold: 3 * time.Second, Down: 11 * time.Second, Peak: 1000}
	const steps = 5000
	for i := 0; i <= steps; i++ {
		at := r.Duration() * time.Duration(i) / steps
		if got := r.RateAt(at); got > r.PeakRate()+tolerance {
			t.Fatalf("RateAt(%s) = %v exceeds PeakRate() = %v", at, got, r.PeakRate())
		}
	}
}

func TestRampValidate(t *testing.T) {
	tests := []struct {
		name    string
		ramp    Ramp
		wantErr bool
	}{
		{"triangle", Ramp{Up: time.Second, Down: time.Second, Peak: 10}, false},
		{"trapezoid", Ramp{Up: time.Second, Hold: time.Second, Down: time.Second, Peak: 10}, false},
		{"zero peak is valid but idle", Ramp{Up: time.Second, Down: time.Second}, false},
		{"zero duration", Ramp{Peak: 10}, true},
		{"negative duration", Ramp{Up: -time.Second, Down: time.Second, Peak: 10}, true},
		{"negative peak", Ramp{Up: time.Second, Down: time.Second, Peak: -1}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.ramp.Validate(); (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestRepeatPlaysCyclesBackToBack(t *testing.T) {
	inner := Ramp{Up: 10 * time.Second, Down: 10 * time.Second, Peak: 1000}
	r := Repeat{Inner: inner, Times: 2}

	if got, want := r.Duration(), 40*time.Second; got != want {
		t.Fatalf("Duration() = %s, want %s", got, want)
	}
	if got, want := r.PeakRate(), 1000.0; got != want {
		t.Fatalf("PeakRate() = %v, want %v", got, want)
	}

	assertRate(t, r, 0, 0)
	assertRate(t, r, 10*time.Second, 1000)
	assertRate(t, r, 20*time.Second, 0)
	assertRate(t, r, 30*time.Second, 1000)
	assertRate(t, r, 25*time.Second, 500)
	assertRate(t, r, 40*time.Second, 0)
}

func TestRepeatDegenerateCases(t *testing.T) {
	inner := Ramp{Up: time.Second, Down: time.Second, Peak: 100}
	for _, tc := range []struct {
		name string
		r    Repeat
	}{
		{"zero times", Repeat{Inner: inner, Times: 0}},
		{"negative times", Repeat{Inner: inner, Times: -1}},
		{"nil inner", Repeat{Times: 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.r.Duration(); got != 0 {
				t.Errorf("Duration() = %s, want 0", got)
			}
			if got := tc.r.RateAt(0); got != 0 {
				t.Errorf("RateAt(0) = %v, want 0", got)
			}
			if got := tc.r.PeakRate(); got != 0 {
				t.Errorf("PeakRate() = %v, want 0", got)
			}
		})
	}
}

func TestExpectedArrivalsMatchesAnalyticIntegral(t *testing.T) {
	r := Ramp{Up: 10 * time.Second, Down: 10 * time.Second, Peak: 1000}
	want := 0.5 * 20 * 1000

	got := ExpectedArrivals(r, 10000)
	if math.Abs(got-want)/want > 1e-6 {
		t.Errorf("ExpectedArrivals() = %v, want %v", got, want)
	}
}

func TestExpectedArrivalsTrapezoidAndRepeat(t *testing.T) {
	r := Ramp{Up: 4 * time.Second, Hold: 10 * time.Second, Down: 6 * time.Second, Peak: 800}
	want := 0.5*4*800 + 10*800 + 0.5*6*800

	if got := ExpectedArrivals(r, 20000); math.Abs(got-want)/want > 1e-5 {
		t.Errorf("ExpectedArrivals(ramp) = %v, want %v", got, want)
	}
	if got := ExpectedArrivals(Repeat{Inner: r, Times: 2}, 40000); math.Abs(got-2*want)/(2*want) > 1e-5 {
		t.Errorf("ExpectedArrivals(repeat) = %v, want %v", got, 2*want)
	}
}

func TestExpectedArrivalsDegenerateCases(t *testing.T) {
	r := Ramp{Up: time.Second, Down: time.Second, Peak: 100}
	if got := ExpectedArrivals(nil, 100); got != 0 {
		t.Errorf("ExpectedArrivals(nil) = %v, want 0", got)
	}
	if got := ExpectedArrivals(r, 0); got != 0 {
		t.Errorf("ExpectedArrivals(steps=0) = %v, want 0", got)
	}
}
