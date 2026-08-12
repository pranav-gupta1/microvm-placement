package loadgen

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// Process samples arrival times from a non-homogeneous Poisson process whose
// mean rate is given by an Envelope.
//
// The sampler is Lewis and Shedler's thinning algorithm. Candidate arrivals are
// drawn from a homogeneous Poisson process at the envelope's peak rate, then each
// candidate at time t is kept with probability lambda(t)/lambdaMax. The survivors
// are exactly a Poisson process with intensity lambda(t).
//
// Thinning is worth the rejected samples because it needs no closed form for the
// inverse of the integrated rate function, so the envelope shape stays a free
// parameter. Rejection cost is bounded by the ratio of peak to mean rate, which
// for a symmetric ramp is 2, so roughly half the candidates are discarded.
//
// A Process is not safe for concurrent use. Each caller should hold its own.
type Process struct {
	env  Envelope
	rng  *rand.Rand
	cur  time.Duration
	done bool
}

// NewProcess returns a Process that samples from env, seeded deterministically.
//
// Two Processes built with the same envelope and seed emit an identical arrival
// sequence, which is what makes load runs reproducible and the tests below
// meaningful.
func NewProcess(env Envelope, seed uint64) (*Process, error) {
	if env == nil {
		return nil, fmt.Errorf("loadgen: envelope must not be nil")
	}
	if env.Duration() <= 0 {
		return nil, fmt.Errorf("loadgen: envelope duration must be positive, got %s", env.Duration())
	}
	if env.PeakRate() < 0 {
		return nil, fmt.Errorf("loadgen: envelope peak rate must be non-negative, got %v", env.PeakRate())
	}
	return &Process{
		env: env,
		// Two distinct stream values keep the PCG state well mixed for seeds
		// that are small integers, which is what callers actually pass.
		rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
	}, nil
}

// Next returns the offset of the next arrival from the start of the run.
//
// It reports ok=false once the envelope is exhausted, after which every
// subsequent call also reports false. An envelope whose peak rate is zero is
// exhausted immediately rather than looping forever looking for an arrival that
// can never happen.
func (p *Process) Next() (time.Duration, bool) {
	if p.done {
		return 0, false
	}
	peak := p.env.PeakRate()
	total := p.env.Duration()
	if peak <= 0 {
		p.done = true
		return 0, false
	}

	for {
		// Interarrival of a homogeneous Poisson process at the proposal rate.
		gap := p.rng.ExpFloat64() / peak
		p.cur += time.Duration(gap * float64(time.Second))
		if p.cur > total {
			p.done = true
			return 0, false
		}
		// Thinning: keep this candidate with probability lambda(t)/lambdaMax.
		if p.rng.Float64()*peak < p.env.RateAt(p.cur) {
			return p.cur, true
		}
	}
}

// Arrivals materialises every arrival offset for a run.
//
// This is a convenience for tests and for the offline plotting path. The live
// harness streams from Next instead, so that a long run does not hold the whole
// schedule in memory and so that arrival times are computed lazily against the
// real clock.
func Arrivals(env Envelope, seed uint64) ([]time.Duration, error) {
	p, err := NewProcess(env, seed)
	if err != nil {
		return nil, err
	}
	var out []time.Duration
	for {
		t, ok := p.Next()
		if !ok {
			return out, nil
		}
		out = append(out, t)
	}
}
