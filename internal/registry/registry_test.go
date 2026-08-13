package registry

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pranav-gupta1/microvm-placement/internal/scheduler"
)

// fakeClock drives expiry deterministically, so liveness tests neither sleep
// nor flake.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// countingNotifier records capacity wakeups.
type countingNotifier struct {
	mu sync.Mutex
	n  int
}

func (c *countingNotifier) SignalCapacity() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
}

func (c *countingNotifier) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func newRegistry(t *testing.T) (*Registry, *scheduler.Scheduler, *fakeClock, *countingNotifier) {
	t.Helper()
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	notifier := &countingNotifier{}
	sched := scheduler.New(scheduler.BestFit)
	r, err := New(sched, notifier, Config{
		HeartbeatTimeout: 6 * time.Second,
		SweepInterval:    time.Second,
		Now:              clock.Now,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return r, sched, clock, notifier
}

func TestNewValidation(t *testing.T) {
	sched := scheduler.New(scheduler.BestFit)
	if _, err := New(nil, nil, Config{}); err == nil {
		t.Error("New(nil scheduler) error = nil, want non-nil")
	}
	if _, err := New(sched, nil, Config{HeartbeatTimeout: -time.Second}); err == nil {
		t.Error("New() with negative timeout error = nil, want non-nil")
	}
	if _, err := New(sched, nil, Config{SweepInterval: -time.Second}); err == nil {
		t.Error("New() with negative sweep interval error = nil, want non-nil")
	}
	if _, err := New(sched, nil, Config{}); err != nil {
		t.Errorf("New() with defaults error = %v", err)
	}
}

func TestRegisterMakesAHostImmediatelyPlaceable(t *testing.T) {
	r, sched, _, notifier := newRegistry(t)

	if err := r.Register("vmhost-0", 8, "10.0.0.1:9090"); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if stats := sched.Stats(); stats.ReadyHosts != 1 || stats.Capacity != 8 {
		t.Errorf("stats = %+v, want 1 ready host with 8 slots", stats)
	}
	if _, err := sched.Place("vm-1"); err != nil {
		t.Errorf("Place() error = %v, want a registered host to be placeable", err)
	}
	if notifier.count() != 1 {
		t.Errorf("capacity signals = %d, want 1", notifier.count())
	}
}

func TestRegisterRejectsEmptyID(t *testing.T) {
	r, _, _, _ := newRegistry(t)
	if err := r.Register("", 8, "10.0.0.1:9090"); err == nil {
		t.Error("Register(\"\") error = nil, want non-nil")
	}
}

func TestRegisterRejectsCapacityBelowTheTwoVMFloor(t *testing.T) {
	r, _, _, _ := newRegistry(t)
	if err := r.Register("vmhost-0", 1, "10.0.0.1:9090"); !errors.Is(err, scheduler.ErrInvalidCapacity) {
		t.Errorf("Register(capacity=1) error = %v, want ErrInvalidCapacity", err)
	}
	if r.Tracked() != 0 {
		t.Errorf("Tracked() = %d after a rejected registration, want 0", r.Tracked())
	}
}

// TestRepeatedRegistrationIsIdempotent covers the agent restarting in place
// and the case where its first response was lost.
func TestRepeatedRegistrationIsIdempotent(t *testing.T) {
	r, sched, _, _ := newRegistry(t)

	for i := 0; i < 3; i++ {
		if err := r.Register("vmhost-0", 8, "10.0.0.1:9090"); err != nil {
			t.Fatalf("Register() attempt %d error = %v", i, err)
		}
	}
	if stats := sched.Stats(); stats.Hosts != 1 || stats.Capacity != 8 {
		t.Errorf("stats = %+v, want a single 8 slot host", stats)
	}
}

func TestHeartbeatKeepsAHostAlive(t *testing.T) {
	r, sched, clock, _ := newRegistry(t)
	if err := r.Register("vmhost-0", 8, "10.0.0.1:9090"); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	for i := 0; i < 6; i++ {
		clock.advance(2 * time.Second)
		if err := r.Heartbeat("vmhost-0"); err != nil {
			t.Fatalf("Heartbeat() error = %v", err)
		}
		if evicted := r.Sweep(); evicted != 0 {
			t.Fatalf("sweep evicted %d hosts despite heartbeats", evicted)
		}
	}
	if stats := sched.Stats(); stats.ReadyHosts != 1 {
		t.Errorf("ReadyHosts = %d, want 1", stats.ReadyHosts)
	}
}

func TestHeartbeatFromUnknownHost(t *testing.T) {
	r, _, _, _ := newRegistry(t)
	if err := r.Heartbeat("ghost"); !errors.Is(err, ErrUnknownHost) {
		t.Errorf("Heartbeat() error = %v, want ErrUnknownHost", err)
	}
}

// TestSilentHostIsDrainedNotDeleted is the important one.
func TestSilentHostIsDrainedNotDeleted(t *testing.T) {
	r, sched, clock, _ := newRegistry(t)
	if err := r.Register("vmhost-0", 8, "10.0.0.1:9090"); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if _, err := sched.Place("vm-1"); err != nil {
		t.Fatalf("Place() error = %v", err)
	}

	clock.advance(10 * time.Second) // well past the timeout
	if evicted := r.Sweep(); evicted != 1 {
		t.Fatalf("sweep evicted %d hosts, want 1", evicted)
	}

	hosts := sched.Hosts()
	if len(hosts) != 1 {
		t.Fatalf("got %d hosts, want the silent host retained while it holds a microVM", len(hosts))
	}
	if hosts[0].State != scheduler.HostDraining {
		t.Errorf("state = %s, want Draining", hosts[0].State)
	}
	if _, ok := sched.HostOf("vm-1"); !ok {
		t.Error("the microVM was orphaned by eviction")
	}
	if _, err := sched.Place("vm-2"); !errors.Is(err, scheduler.ErrNoCapacity) {
		t.Errorf("Place() onto a drained host error = %v, want ErrNoCapacity", err)
	}
}

func TestDrainedHostIsRemovedOnceEmpty(t *testing.T) {
	r, sched, clock, _ := newRegistry(t)
	if err := r.Register("vmhost-0", 8, "10.0.0.1:9090"); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if _, err := sched.Place("vm-1"); err != nil {
		t.Fatalf("Place() error = %v", err)
	}

	clock.advance(10 * time.Second)
	r.Sweep()

	if len(sched.Hosts()) != 1 {
		t.Fatalf("host was removed while still holding a microVM")
	}

	if err := sched.Release("vm-1"); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	r.Sweep()

	if got := len(sched.Hosts()); got != 0 {
		t.Errorf("got %d hosts after the drained host emptied, want 0", got)
	}
}

func TestDeregisterDrainsGracefully(t *testing.T) {
	r, sched, _, _ := newRegistry(t)
	if err := r.Register("vmhost-0", 8, "10.0.0.1:9090"); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if _, err := sched.Place("vm-1"); err != nil {
		t.Fatalf("Place() error = %v", err)
	}

	if err := r.Deregister("vmhost-0"); err != nil {
		t.Fatalf("Deregister() error = %v", err)
	}
	if _, ok := sched.HostOf("vm-1"); !ok {
		t.Error("Deregister orphaned a running microVM")
	}
	if hosts := sched.Hosts(); len(hosts) != 1 || hosts[0].State != scheduler.HostDraining {
		t.Errorf("hosts = %+v, want one draining host", hosts)
	}
	if r.Tracked() != 0 {
		t.Errorf("Tracked() = %d after deregistration, want 0", r.Tracked())
	}
}

func TestDeregisterUnknownHost(t *testing.T) {
	r, _, _, _ := newRegistry(t)
	if err := r.Deregister("ghost"); !errors.Is(err, ErrUnknownHost) {
		t.Errorf("Deregister() error = %v, want ErrUnknownHost", err)
	}
}

func TestSweepIsSafeWithNoHosts(t *testing.T) {
	r, _, clock, _ := newRegistry(t)
	clock.advance(time.Minute)
	if evicted := r.Sweep(); evicted != 0 {
		t.Errorf("sweep evicted %d hosts from an empty registry, want 0", evicted)
	}
}

func TestRunStopsWhenDoneIsClosed(t *testing.T) {
	r, _, _, _ := newRegistry(t)
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		r.Run(done)
		close(stopped)
	}()

	close(done)
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after done was closed")
	}
}

func TestConcurrentRegistrationAndSweep(t *testing.T) {
	r, _, clock, _ := newRegistry(t)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := scheduler.HostID("vmhost-" + string(rune('a'+i%26)) + string(rune('0'+i/26)))
			_ = r.Register(id, 8, "10.0.0.1:9090")
			_ = r.Heartbeat(id)
		}(i)
	}
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			clock.advance(time.Millisecond)
			r.Sweep()
		}()
	}
	wg.Wait()
}
