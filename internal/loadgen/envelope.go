// Package loadgen implements the offered-load model for the test harness.
//
// Load is described in two independent layers. An Envelope is the deterministic
// shape of the run: the mean arrival rate lambda(t) at every instant. A Process
// turns that mean rate into concrete arrival timestamps by sampling a
// non-homogeneous Poisson process, which supplies the burstiness that makes the
// traffic resemble production rather than a metronome.
//
// Splitting the two matters. The envelope is what we assert against in tests and
// plot on the dashboard, while the process is what actually stresses the system.
package loadgen

import (
	"fmt"
	"time"
)

// Envelope is the deterministic mean-arrival-rate function of a load run.
//
// Implementations must be pure and safe for concurrent use: RateAt is called
// many times per generated arrival and must not carry state between calls.
type Envelope interface {
	// RateAt returns the mean arrival rate in requests per second at offset t
	// from the start of the run. It returns 0 for t outside [0, Duration()].
	RateAt(t time.Duration) float64

	// Duration is the total wall-clock length of the envelope.
	Duration() time.Duration

	// PeakRate is an upper bound on RateAt over the whole envelope. The
	// thinning sampler uses it as its proposal rate, so it must never be
	// smaller than the true maximum or the sampler will silently under-deliver
	// load, which is precisely the failure mode this harness exists to avoid.
	PeakRate() float64
}

// Ramp is a single trapezoidal load cycle: a linear climb from zero to Peak, an
// optional flat hold, then a linear descent back to zero.
//
// With Hold set to zero this is the triangular "crescendo then decay" shape the
// assignment asks for. A non-zero hold is useful when you want to observe
// steady-state behaviour at peak, for example to let autoscaling settle before
// measuring idle capacity.
type Ramp struct {
	// Up is how long the climb from 0 to Peak takes.
	Up time.Duration
	// Hold is how long the rate stays at Peak. May be zero.
	Hold time.Duration
	// Down is how long the descent from Peak back to 0 takes.
	Down time.Duration
	// Peak is the maximum mean arrival rate in requests per second.
	Peak float64
}

// Validate reports whether the ramp describes a usable load shape.
func (r Ramp) Validate() error {
	if r.Up < 0 || r.Hold < 0 || r.Down < 0 {
		return fmt.Errorf("ramp durations must be non-negative, got up=%s hold=%s down=%s", r.Up, r.Hold, r.Down)
	}
	if r.Up == 0 && r.Hold == 0 && r.Down == 0 {
		return fmt.Errorf("ramp has zero total duration")
	}
	if r.Peak < 0 {
		return fmt.Errorf("ramp peak must be non-negative, got %v", r.Peak)
	}
	return nil
}

// RateAt implements Envelope.
func (r Ramp) RateAt(t time.Duration) float64 {
	switch {
	case t < 0 || t > r.Duration():
		return 0
	case t < r.Up:
		// Linear climb. Guarded above: t < r.Up implies r.Up > 0.
		return r.Peak * (float64(t) / float64(r.Up))
	case t < r.Up+r.Hold:
		return r.Peak
	default:
		if r.Down == 0 {
			return 0
		}
		elapsed := t - r.Up - r.Hold
		return r.Peak * (1 - float64(elapsed)/float64(r.Down))
	}
}

// Duration implements Envelope.
func (r Ramp) Duration() time.Duration { return r.Up + r.Hold + r.Down }

// PeakRate implements Envelope.
func (r Ramp) PeakRate() float64 { return r.Peak }

// Repeat plays an inner envelope Times times back to back with no gap between
// cycles. The assignment calls for two consecutive ramp cycles, which is
// Repeat{Inner: Ramp{...}, Times: 2}.
//
// The trough between cycles is the interesting part of the run: it is where a
// naive autoscaler either thrashes its replicas to zero and then cannot recover
// in time for the second climb, or holds capacity it is not using and burns
// money. Both failures are visible on the dashboard.
type Repeat struct {
	Inner Envelope
	Times int
}

// RateAt implements Envelope.
func (r Repeat) RateAt(t time.Duration) float64 {
	if r.Times <= 0 || r.Inner == nil || t < 0 || t > r.Duration() {
		return 0
	}
	inner := r.Inner.Duration()
	if inner <= 0 {
		return 0
	}
	// The final instant of the run belongs to the last cycle, not to a
	// non-existent cycle Times+1.
	if t == r.Duration() {
		return r.Inner.RateAt(inner)
	}
	return r.Inner.RateAt(t % inner)
}

// Duration implements Envelope.
func (r Repeat) Duration() time.Duration {
	if r.Times <= 0 || r.Inner == nil {
		return 0
	}
	return r.Inner.Duration() * time.Duration(r.Times)
}

// PeakRate implements Envelope.
func (r Repeat) PeakRate() float64 {
	if r.Times <= 0 || r.Inner == nil {
		return 0
	}
	return r.Inner.PeakRate()
}

// ExpectedArrivals is the integral of the rate function over the whole
// envelope, which for a Poisson process is the expected number of requests a
// run will offer.
//
// It is computed numerically with the trapezoid rule rather than analytically so
// that it stays correct for any Envelope implementation, including ones added
// later. Tests use it to assert that the sampler delivers the load it promises.
func ExpectedArrivals(env Envelope, steps int) float64 {
	if env == nil || steps < 1 || env.Duration() <= 0 {
		return 0
	}
	total := env.Duration()
	step := total / time.Duration(steps)
	if step <= 0 {
		return 0
	}
	var sum float64
	for i := 0; i < steps; i++ {
		left := env.RateAt(step * time.Duration(i))
		right := env.RateAt(step * time.Duration(i+1))
		sum += (left + right) / 2 * step.Seconds()
	}
	return sum
}
