package hypervisor

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// Default boot timing for the fake, chosen to match measured Firecracker
// snapshot restore on a Graviton metal host. Restoring a 1 GiB guest from a
// snapshot lands in the low tens of milliseconds, an order of magnitude faster
// than the roughly 125 ms cold boot, which is what makes a microVM per request
// viable at 1000 requests per second in the first place.
const (
	DefaultBootLatency = 25 * time.Millisecond
	DefaultBootJitter  = 10 * time.Millisecond
	DefaultStopLatency = 2 * time.Millisecond
)

// FakeConfig configures the in-process hypervisor model.
type FakeConfig struct {
	// Slots is the number of microVMs this host can run at once. It must be at
	// least 2 to satisfy the assignment's floor of several microVMs per pod.
	Slots int
	// BootLatency is the mean time Start blocks for.
	BootLatency time.Duration
	// BootJitter is the half-width of a uniform distribution around
	// BootLatency. Real restore times vary with page-fault behaviour, and a
	// constant latency would hide queueing effects that only appear when
	// service times have spread.
	BootJitter time.Duration
	// StopLatency is the mean time Stop blocks for.
	StopLatency time.Duration
	// FailureRate is the probability in [0,1] that a Start fails. Defaults to
	// zero. A hypervisor that never fails lets the layers above it be written
	// without error handling, so tests turn this up deliberately.
	FailureRate float64
	// Seed makes latency and failure sampling reproducible.
	Seed uint64
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
// needing hardware virtualisation. It implements Reaper.
type Fake struct {
	cfg FakeConfig

	mu      sync.Mutex
	rng     *rand.Rand
	running map[string]*fakeVM
	closed  bool

	// expired carries reaped microVM IDs to the agent. It is generously
	// buffered so that a brief stall in the consumer cannot block a reaper
	// timer, which would otherwise delay the slot being returned.
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

// sampleBootLatency draws a boot time. Callers hold f.mu.
func (f *Fake) sampleBootLatency() time.Duration {
	if f.cfg.BootJitter == 0 {
		return f.cfg.BootLatency
	}
	// Uniform over [mean-jitter, mean+jitter]. Validate guarantees this stays
	// non-negative.
	offset := time.Duration(f.rng.Int64N(int64(2*f.cfg.BootJitter))) - f.cfg.BootJitter
	return f.cfg.BootLatency + offset
}

// Start implements Hypervisor.
//
// The slot is reserved before the simulated boot delay, so concurrent callers
// cannot oversubscribe the host by racing through the latency window. That
// mirrors the real implementation, where the jailer allocates the slot up front.
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
	// Reserve the slot with a placeholder so the capacity check above is
	// authoritative for the whole boot window.
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
	// Close may have run while we were booting, in which case the placeholder
	// is already gone and this microVM must not be registered.
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
		// Already stopped explicitly. Nothing to do, and crucially no
		// notification, or the agent would double-free the scheduler slot.
		f.mu.Unlock()
		return
	}
	delete(f.running, id)
	f.mu.Unlock()

	select {
	case f.expired <- id:
	default:
		// The buffer is sized at four times the slot count, so this is
		// effectively unreachable. Dropping is still better than blocking a
		// timer goroutine forever if the consumer has gone away.
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
		// We beat the reaper to it, so its wg.Done will never run.
		f.wg.Done()
	}
	latency := f.cfg.StopLatency
	f.mu.Unlock()

	return sleepCtx(ctx, latency)
}

// Close implements Hypervisor. It is idempotent.
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

	// Wait for any reaper timer that already fired to finish before closing
	// the channel it sends on.
	f.wg.Wait()
	close(f.expired)
	return nil
}

// sleepCtx sleeps for d, returning early if ctx is cancelled. A zero or
// negative duration returns immediately without touching the timer subsystem,
// which matters because unit tests configure zero latency and run tens of
// thousands of operations.
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
