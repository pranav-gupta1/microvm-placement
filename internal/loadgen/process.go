package loadgen

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// Process samples arrival times from a non-homogeneous Poisson process whose
// mean rate is given by an Envelope.
type Process struct {
	env  Envelope
	rng  *rand.Rand
	cur  time.Duration
	done bool
}

// NewProcess returns a Process that samples from env, seeded
// deterministically.
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
		rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
	}, nil
}

// Next returns the offset of the next arrival from the start of the run.
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
		gap := p.rng.ExpFloat64() / peak
		p.cur += time.Duration(gap * float64(time.Second))
		if p.cur > total {
			p.done = true
			return 0, false
		}
		if p.rng.Float64()*peak < p.env.RateAt(p.cur) {
			return p.cur, true
		}
	}
}

// Arrivals materialises every arrival offset for a run.
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
