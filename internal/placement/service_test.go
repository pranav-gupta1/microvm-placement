package placement

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pranav-gupta1/microvm-placement/internal/scheduler"
)

// newService builds a running service backed by a fleet of ready hosts.
func newService(t *testing.T, hosts, capacity int, cfg Config) (*Service, *scheduler.Scheduler) {
	t.Helper()
	sched := scheduler.New(scheduler.BestFit)
	for i := 0; i < hosts; i++ {
		id := scheduler.HostID(fmt.Sprintf("host-%d", i))
		if err := sched.AddHost(id, capacity); err != nil {
			t.Fatalf("AddHost() error = %v", err)
		}
		if err := sched.SetHostState(id, scheduler.HostReady); err != nil {
			t.Fatalf("SetHostState() error = %v", err)
		}
	}
	svc, err := New(sched, cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	svc.Start(ctx)
	t.Cleanup(func() {
		svc.Stop()
		cancel()
	})
	return svc, sched
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"defaults", Config{}, false},
		{"explicit", Config{QueueDepth: 10, AdmissionDeadline: time.Second, RetryInterval: time.Millisecond}, false},
		{"negative depth", Config{QueueDepth: -1, AdmissionDeadline: time.Second, RetryInterval: time.Millisecond}, true},
		{"negative deadline", Config{QueueDepth: 10, AdmissionDeadline: -time.Second, RetryInterval: time.Millisecond}, true},
		{"negative retry", Config{QueueDepth: 10, AdmissionDeadline: time.Second, RetryInterval: -time.Millisecond}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sched := scheduler.New(scheduler.BestFit)
			_, err := New(sched, tc.cfg)
			if (err != nil) != tc.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestNewRejectsNilScheduler(t *testing.T) {
	if _, err := New(nil, Config{}); err == nil {
		t.Error("New(nil) error = nil, want non-nil")
	}
}

func TestAdmitPlacesImmediatelyWhenCapacityExists(t *testing.T) {
	svc, _ := newService(t, 2, 4, Config{})

	res, err := svc.Admit(context.Background(), Request{VMID: "vm-1", TTL: 500 * time.Millisecond})
	if err != nil {
		t.Fatalf("Admit() error = %v", err)
	}
	if res.Host == "" {
		t.Error("Result.Host is empty")
	}
	if res.Attempts != 1 {
		t.Errorf("Result.Attempts = %d, want 1 when capacity is free", res.Attempts)
	}
	if stats := svc.Stats(); stats.Placed != 1 || stats.Dropped() != 0 {
		t.Errorf("Stats = %+v, want Placed=1 Dropped=0", stats)
	}
}

func TestAdmitRejectsEmptyVMID(t *testing.T) {
	svc, _ := newService(t, 1, 4, Config{})
	if _, err := svc.Admit(context.Background(), Request{}); err == nil {
		t.Error("Admit() with empty VMID error = nil, want non-nil")
	}
}

// TestAdmitWaitsForCapacityRatherThanDropping is the assignment's central
// requirement expressed as a test. The fleet is full when the request arrives,
// so a naive implementation would return 503. This one must wait and succeed.
func TestAdmitWaitsForCapacityRatherThanDropping(t *testing.T) {
	svc, _ := newService(t, 1, 2, Config{AdmissionDeadline: 2 * time.Second})

	// Fill every slot.
	for i := 0; i < 2; i++ {
		if _, err := svc.Admit(context.Background(), Request{VMID: fmt.Sprintf("vm-%d", i)}); err != nil {
			t.Fatalf("Admit(vm-%d) error = %v", i, err)
		}
	}

	// Free one slot shortly after the next request starts waiting.
	go func() {
		time.Sleep(50 * time.Millisecond)
		if err := svc.Release("vm-0"); err != nil {
			t.Errorf("Release() error = %v", err)
		}
	}()

	start := time.Now()
	res, err := svc.Admit(context.Background(), Request{VMID: "vm-waiter"})
	if err != nil {
		t.Fatalf("Admit() error = %v, want the request to wait and succeed", err)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Errorf("Admit() returned after %s, expected it to have waited for the release", elapsed)
	}
	if res.Attempts < 2 {
		t.Errorf("Result.Attempts = %d, want at least 2 since the first attempt had no capacity", res.Attempts)
	}
	if stats := svc.Stats(); stats.Dropped() != 0 {
		t.Errorf("Dropped() = %d, want 0", stats.Dropped())
	}
}

// TestReleaseWakesAWaiterPromptly checks that retries are driven by the release
// signal, not by the fallback poll. A long retry interval makes the difference
// observable: if the waiter were polling, it could not return this fast.
func TestReleaseWakesAWaiterPromptly(t *testing.T) {
	svc, _ := newService(t, 1, 2, Config{
		AdmissionDeadline: 5 * time.Second,
		RetryInterval:     3 * time.Second,
	})
	for i := 0; i < 2; i++ {
		if _, err := svc.Admit(context.Background(), Request{VMID: fmt.Sprintf("vm-%d", i)}); err != nil {
			t.Fatalf("Admit(vm-%d) error = %v", i, err)
		}
	}

	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		if _, err := svc.Admit(context.Background(), Request{VMID: "vm-waiter"}); err != nil {
			t.Errorf("Admit() error = %v", err)
		}
		done <- time.Since(start)
	}()

	time.Sleep(30 * time.Millisecond)
	if err := svc.Release("vm-1"); err != nil {
		t.Fatalf("Release() error = %v", err)
	}

	select {
	case elapsed := <-done:
		// Woken by the signal, so far below the 3 second poll interval.
		if elapsed > time.Second {
			t.Errorf("waiter took %s, expected the release signal to wake it promptly", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter was not woken by the release signal")
	}
}

func TestAdmitTimesOutWhenCapacityNeverArrives(t *testing.T) {
	svc, _ := newService(t, 1, 2, Config{
		AdmissionDeadline: 100 * time.Millisecond,
		RetryInterval:     5 * time.Millisecond,
	})
	for i := 0; i < 2; i++ {
		if _, err := svc.Admit(context.Background(), Request{VMID: fmt.Sprintf("vm-%d", i)}); err != nil {
			t.Fatalf("Admit(vm-%d) error = %v", i, err)
		}
	}

	// Nothing is ever released, so this must eventually give up rather than
	// block forever.
	if _, err := svc.Admit(context.Background(), Request{VMID: "vm-doomed"}); !errors.Is(err, ErrAdmissionTimeout) {
		t.Errorf("Admit() error = %v, want ErrAdmissionTimeout", err)
	}
	stats := svc.Stats()
	if stats.TimedOut != 1 {
		t.Errorf("Stats.TimedOut = %d, want 1", stats.TimedOut)
	}
	if stats.Dropped() != 1 {
		t.Errorf("Dropped() = %d, want 1", stats.Dropped())
	}
}

func TestCallerContextCancellationIsHonoured(t *testing.T) {
	svc, _ := newService(t, 1, 2, Config{AdmissionDeadline: 10 * time.Second})
	for i := 0; i < 2; i++ {
		if _, err := svc.Admit(context.Background(), Request{VMID: fmt.Sprintf("vm-%d", i)}); err != nil {
			t.Fatalf("Admit(vm-%d) error = %v", i, err)
		}
	}

	// A caller that gives up early must not be held for the full service
	// deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	start := time.Now()
	if _, err := svc.Admit(ctx, Request{VMID: "vm-impatient"}); !errors.Is(err, ErrAdmissionTimeout) {
		t.Errorf("Admit() error = %v, want ErrAdmissionTimeout", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Admit() honoured the service deadline (%s) instead of the caller's", elapsed)
	}
}

// TestNoSlotIsLeakedWhenAdmissionTimesOut guards the race that motivated
// letting the dispatcher be the sole authority on a request's outcome. If a
// timeout could be reported for a request that was in fact placed, the slot
// would never be released and the fleet would leak capacity under load.
func TestNoSlotIsLeakedWhenAdmissionTimesOut(t *testing.T) {
	const (
		hosts    = 4
		capacity = 8
		total    = hosts * capacity
	)
	svc, sched := newService(t, hosts, capacity, Config{
		AdmissionDeadline: 40 * time.Millisecond,
		RetryInterval:     time.Millisecond,
	})

	var placed, dropped atomic.Int64
	var wg sync.WaitGroup
	// Offer far more than the fleet can hold so many requests time out.
	for i := 0; i < total*3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			switch _, err := svc.Admit(context.Background(), Request{VMID: fmt.Sprintf("vm-%d", i)}); {
			case err == nil:
				placed.Add(1)
			case errors.Is(err, ErrAdmissionTimeout), errors.Is(err, ErrQueueFull):
				dropped.Add(1)
			default:
				t.Errorf("Admit() unexpected error = %v", err)
			}
		}(i)
	}
	wg.Wait()

	// Whatever the split, the scheduler's occupancy must exactly equal the
	// number of successful admissions. Any mismatch is a leaked slot.
	stats := sched.Stats()
	if int64(stats.InflightVMs) != placed.Load() {
		t.Errorf("scheduler holds %d microVMs but %d admissions succeeded", stats.InflightVMs, placed.Load())
	}
	if placed.Load() != int64(total) {
		t.Errorf("placed %d microVMs, want the fleet to be exactly full at %d", placed.Load(), total)
	}
	if placed.Load()+dropped.Load() != int64(total*3) {
		t.Errorf("placed %d + dropped %d does not account for all %d requests", placed.Load(), dropped.Load(), total*3)
	}
}

func TestQueueFullAppliesBackpressureThenRejects(t *testing.T) {
	// One slot of queue and a full fleet, so the second waiter cannot even be
	// enqueued.
	svc, _ := newService(t, 1, 2, Config{
		QueueDepth:        1,
		AdmissionDeadline: 80 * time.Millisecond,
		RetryInterval:     time.Millisecond,
	})
	for i := 0; i < 2; i++ {
		if _, err := svc.Admit(context.Background(), Request{VMID: fmt.Sprintf("vm-%d", i)}); err != nil {
			t.Fatalf("Admit(vm-%d) error = %v", i, err)
		}
	}

	var wg sync.WaitGroup
	var timeouts, full atomic.Int64
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			switch _, err := svc.Admit(context.Background(), Request{VMID: fmt.Sprintf("waiter-%d", i)}); {
			case errors.Is(err, ErrAdmissionTimeout):
				timeouts.Add(1)
			case errors.Is(err, ErrQueueFull):
				full.Add(1)
			default:
				t.Errorf("Admit() error = %v, want a timeout or a full queue", err)
			}
		}(i)
	}
	wg.Wait()

	if got := timeouts.Load() + full.Load(); got != 8 {
		t.Errorf("accounted for %d of 8 waiters", got)
	}
	// The point of backpressure: a full queue makes callers wait for room
	// rather than failing instantly, so some must have been enqueued.
	if timeouts.Load() == 0 {
		t.Error("no waiter was ever enqueued, backpressure is not working")
	}
	if stats := svc.Stats(); stats.Dropped() != 8 {
		t.Errorf("Dropped() = %d, want 8", stats.Dropped())
	}
}

func TestQueueIsFirstInFirstOut(t *testing.T) {
	svc, _ := newService(t, 1, 2, Config{AdmissionDeadline: 5 * time.Second, RetryInterval: time.Millisecond})
	for i := 0; i < 2; i++ {
		if _, err := svc.Admit(context.Background(), Request{VMID: fmt.Sprintf("filler-%d", i)}); err != nil {
			t.Fatalf("Admit(filler-%d) error = %v", i, err)
		}
	}

	const waiters = 5
	order := make(chan int, waiters)
	var started sync.WaitGroup
	for i := 0; i < waiters; i++ {
		started.Add(1)
		go func(i int) {
			started.Done()
			if _, err := svc.Admit(context.Background(), Request{VMID: fmt.Sprintf("waiter-%d", i)}); err != nil {
				t.Errorf("Admit(waiter-%d) error = %v", i, err)
				return
			}
			order <- i
		}(i)
		// Serialise enqueueing so arrival order is well defined.
		started.Wait()
		time.Sleep(15 * time.Millisecond)
	}

	// Release slots one at a time; waiters should be served in arrival order.
	for i := 0; i < waiters; i++ {
		if i < 2 {
			if err := svc.Release(fmt.Sprintf("filler-%d", i)); err != nil {
				t.Fatalf("Release() error = %v", err)
			}
		} else {
			if err := svc.Release(fmt.Sprintf("waiter-%d", i-2)); err != nil {
				t.Fatalf("Release() error = %v", err)
			}
		}
		select {
		case got := <-order:
			if got != i {
				t.Errorf("waiter %d was served in position %d, queue is not FIFO", got, i)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for position %d to be served", i)
		}
	}
}

func TestStopDrainsWaitersInsteadOfHangingThem(t *testing.T) {
	sched := scheduler.New(scheduler.BestFit)
	if err := sched.AddHost("host-0", 2); err != nil {
		t.Fatalf("AddHost() error = %v", err)
	}
	if err := sched.SetHostState("host-0", scheduler.HostReady); err != nil {
		t.Fatalf("SetHostState() error = %v", err)
	}
	svc, err := New(sched, Config{AdmissionDeadline: 30 * time.Second, RetryInterval: time.Millisecond})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	svc.Start(context.Background())

	for i := 0; i < 2; i++ {
		if _, err := svc.Admit(context.Background(), Request{VMID: fmt.Sprintf("vm-%d", i)}); err != nil {
			t.Fatalf("Admit(vm-%d) error = %v", i, err)
		}
	}

	errs := make(chan error, 1)
	go func() {
		_, err := svc.Admit(context.Background(), Request{VMID: "vm-waiter"})
		errs <- err
	}()
	time.Sleep(50 * time.Millisecond)

	svc.Stop()

	select {
	case err := <-errs:
		// A shutdown must not leave callers blocked on a channel nobody will
		// ever write to.
		if !errors.Is(err, ErrShuttingDown) {
			t.Errorf("Admit() during shutdown error = %v, want ErrShuttingDown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() left a waiter hanging")
	}
}

func TestStopIsIdempotent(t *testing.T) {
	svc, _ := newService(t, 1, 2, Config{})
	svc.Stop()
	svc.Stop()
}

func TestReleaseUnknownVMPropagatesTheError(t *testing.T) {
	svc, _ := newService(t, 1, 2, Config{})
	if err := svc.Release("ghost"); !errors.Is(err, scheduler.ErrUnknownVM) {
		t.Errorf("Release() error = %v, want scheduler.ErrUnknownVM", err)
	}
	if stats := svc.Stats(); stats.Released != 0 {
		t.Errorf("Stats.Released = %d, want 0 after a failed release", stats.Released)
	}
}

func TestStatsTrackQueueHighWaterMark(t *testing.T) {
	svc, _ := newService(t, 1, 2, Config{
		QueueDepth:        64,
		AdmissionDeadline: 60 * time.Millisecond,
		RetryInterval:     time.Millisecond,
	})
	for i := 0; i < 2; i++ {
		if _, err := svc.Admit(context.Background(), Request{VMID: fmt.Sprintf("vm-%d", i)}); err != nil {
			t.Fatalf("Admit(vm-%d) error = %v", i, err)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = svc.Admit(context.Background(), Request{VMID: fmt.Sprintf("waiter-%d", i)})
		}(i)
	}
	wg.Wait()

	// The high-water mark is what tells us after a run whether pre-provisioned
	// capacity was actually doing its job.
	if stats := svc.Stats(); stats.MaxQueueLen == 0 {
		t.Error("Stats.MaxQueueLen = 0, expected the queue to have held waiters")
	}
}
