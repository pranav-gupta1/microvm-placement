// Package loadgen implements the offered-load model for the test harness.
package loadgen

import (
	"fmt"
	"time"
)

// Envelope is the deterministic mean-arrival-rate function of a load run.
type Envelope interface {
	RateAt(t time.Duration) float64

	Duration() time.Duration

	PeakRate() float64
}

// Ramp is a single trapezoidal load cycle: a linear climb from zero to Peak,
// an optional flat hold, then a linear descent back to zero.
type Ramp struct {
	Up   time.Duration
	Hold time.Duration
	Down time.Duration
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
// cycles.
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
