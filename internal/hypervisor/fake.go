package hypervisor

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// Default boot timing for the fake, chosen to match measured Firecracker
// snapshot restore on a Graviton metal host.
const (
	DefaultBootLatency = 25 * time.Millisecond
	DefaultBootJitter  = 10 * time.Millisecond
	DefaultStopLatency = 2 * time.Millisecond
)

// FakeConfig configures the in-process hypervisor model.
type FakeConfig struct {
	Slots       int
	BootLatency time.Duration
	BootJitter  time.Duration
	StopLatency time.Duration
	FailureRate float64
	Seed        uint64
}

func (c *FakeConfig) applyDefaults() {
	if c.BootLatency == 0 {
		c.BootLatency = DefaultBootLatency
	}
	if c.BootJitter == 0 {
		c.BootJitter = DefaultBootJitter
	}
	if c.StopLatency == 0 {
		c.StopLatency = DefaultStopLatency
	}
}

// Validate reports whether the configuration is usable.
func (c FakeConfig) Validate() error {
	if c.Slots < 2 {
		return fmt.Errorf("fake: Slots must be at least 2, got %d", c.Slots)
	}
	if c.BootLatency < 0 || c.BootJitter < 0 || c.StopLatency < 0 {
		return fmt.Errorf("fake: latencies must not be negative")
	}
	if c.BootJitter > c.BootLatency {
		return fmt.Errorf("fake: BootJitter %s exceeds BootLatency %s, which would allow negative boot times", c.BootJitter, c.BootLatency)
	}
	if c.FailureRate < 0 || c.FailureRate > 1 {
		return fmt.Errorf("fake: FailureRate must be in [0,1], got %v", c.FailureRate)
	}
	return nil
}

// Fake is an in-process Hypervisor that models timing and capacity without
// needing hardware virtualisation.
type Fake struct {
	cfg FakeConfig

	mu      sync.Mutex
	rng     *rand.Rand
	running map[string]*fakeVM
	closed  bool

	expired chan string
	wg      sync.WaitGroup
}

type fakeVM struct {
	instance Instance
	timer    *time.Timer
}

var (
	_ Hypervisor = (*Fake)(nil)
	_ Reaper     = (*Fake)(nil)
)

// NewFake returns a configured in-process hypervisor.
func NewFake(cfg FakeConfig) (*Fake, error) {
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Fake{
		cfg:     cfg,
		rng:     rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15)),
		running: make(map[string]*fakeVM, cfg.Slots),
		expired: make(chan string, cfg.Slots*4),
	}, nil
}

// Capacity implements Hypervisor.
func (f *Fake) Capacity() int { return f.cfg.Slots }

// Running implements Hypervisor.
func (f *Fake) Running() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.running)
}

// Expired implements Reaper.
func (f *Fake) Expired() <-chan string { return f.expired }

// sampleBootLatency draws a boot time.
func (f *Fake) sampleBootLatency() time.Duration {
	if f.cfg.BootJitter == 0 {
		return f.cfg.BootLatency
	}
	offset := time.Duration(f.rng.Int64N(int64(2*f.cfg.BootJitter))) - f.cfg.BootJitter
	return f.cfg.BootLatency + offset
}

// Start implements Hypervisor.
func (f *Fake) Start(ctx context.Context, spec Spec) (Instance, error) {
	if err := spec.Validate(); err != nil {
		return Instance{}, err
	}

	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return Instance{}, ErrClosed
	}
	if _, exists := f.running[spec.ID]; exists {
		f.mu.Unlock()
		return Instance{}, fmt.Errorf("%w: %s", ErrDuplicateVM, spec.ID)
	}
	if len(f.running) >= f.cfg.Slots {
		f.mu.Unlock()
		return Instance{}, ErrNoCapacity
	}
	latency := f.sampleBootLatency()
	fail := f.cfg.FailureRate > 0 && f.rng.Float64() < f.cfg.FailureRate
	vm := &fakeVM{}
	f.running[spec.ID] = vm
	f.mu.Unlock()

	release := func() {
		f.mu.Lock()
		delete(f.running, spec.ID)
		f.mu.Unlock()
	}

	if err := sleepCtx(ctx, latency); err != nil {
		release()
		return Instance{}, err
	}
	if fail {
		release()
		return Instance{}, fmt.Errorf("%w: simulated failure booting %s", ErrBootFailed, spec.ID)
	}

	now := time.Now()
	inst := Instance{ID: spec.ID, StartedAt: now, BootLatency: latency}
	if spec.TTL > 0 {
		inst.ExpiresAt = now.Add(spec.TTL)
	}

	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return Instance{}, ErrClosed
	}
	vm.instance = inst
	if spec.TTL > 0 {
		f.wg.Add(1)
		vm.timer = time.AfterFunc(spec.TTL, func() {
			defer f.wg.Done()
			f.reap(spec.ID)
		})
	}
	f.mu.Unlock()

	return inst, nil
}

// reap expires a microVM whose TTL has elapsed and notifies the consumer.
func (f *Fake) reap(id string) {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	if _, ok := f.running[id]; !ok {
		f.mu.Unlock()
		return
	}
	delete(f.running, id)
	f.mu.Unlock()

	select {
	case f.expired <- id:
	default:
	}
}

// Stop implements Hypervisor.
func (f *Fake) Stop(ctx context.Context, id string) error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return ErrClosed
	}
	vm, ok := f.running[id]
	if !ok {
		f.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrUnknownVM, id)
	}
	delete(f.running, id)
	if vm.timer != nil && vm.timer.Stop() {
		f.wg.Done()
	}
	latency := f.cfg.StopLatency
	f.mu.Unlock()

	return sleepCtx(ctx, latency)
}

// Close implements Hypervisor.
func (f *Fake) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	for id, vm := range f.running {
		if vm.timer != nil && vm.timer.Stop() {
			f.wg.Done()
		}
		delete(f.running, id)
	}
	f.mu.Unlock()

	f.wg.Wait()
	close(f.expired)
	return nil
}

// sleepCtx sleeps for d, returning early if ctx is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
