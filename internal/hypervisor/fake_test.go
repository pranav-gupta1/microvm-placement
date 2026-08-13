package hypervisor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// instant is a hypervisor with negligible simulated latency, for tests that
// care about bookkeeping rather than timing.
func instant(t *testing.T, slots int) *Fake {
	t.Helper()
	f, err := NewFake(FakeConfig{
		Slots:       slots,
		BootLatency: time.Nanosecond,
		BootJitter:  time.Nanosecond,
		StopLatency: time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("NewFake() error = %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func spec(id string, ttl time.Duration) Spec {
	return Spec{ID: id, VCPUs: 1, MemMiB: 1024, TTL: ttl}
}

func TestSpecValidate(t *testing.T) {
	tests := []struct {
		name    string
		spec    Spec
		wantErr bool
	}{
		{"valid", Spec{ID: "a", VCPUs: 1, MemMiB: 1024}, false},
		{"valid with ttl", Spec{ID: "a", VCPUs: 1, MemMiB: 1024, TTL: time.Second}, false},
		{"empty id", Spec{VCPUs: 1, MemMiB: 1024}, true},
		{"no vcpus", Spec{ID: "a", MemMiB: 1024}, true},
		{"no memory", Spec{ID: "a", VCPUs: 1}, true},
		{"negative ttl", Spec{ID: "a", VCPUs: 1, MemMiB: 1024, TTL: -time.Second}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.spec.Validate(); (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestFakeConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     FakeConfig
		wantErr bool
	}{
		{"valid", FakeConfig{Slots: 8}, false},
		{"minimum slots", FakeConfig{Slots: 2}, false},
		{"one slot", FakeConfig{Slots: 1}, true},
		{"zero slots", FakeConfig{Slots: 0}, true},
		{"jitter exceeds mean", FakeConfig{Slots: 4, BootLatency: time.Millisecond, BootJitter: time.Second}, true},
		{"failure rate above one", FakeConfig{Slots: 4, FailureRate: 1.5}, true},
		{"negative failure rate", FakeConfig{Slots: 4, FailureRate: -0.1}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewFake(tc.cfg)
			if (err != nil) != tc.wantErr {
				t.Errorf("NewFake() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestStartAndStop(t *testing.T) {
	f := instant(t, 4)
	ctx := context.Background()

	inst, err := f.Start(ctx, spec("vm-1", 0))
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if inst.ID != "vm-1" {
		t.Errorf("Instance.ID = %q, want vm-1", inst.ID)
	}
	if inst.StartedAt.IsZero() {
		t.Error("Instance.StartedAt is zero")
	}
	if !inst.ExpiresAt.IsZero() {
		t.Error("Instance.ExpiresAt should be zero for a microVM with no TTL")
	}
	if got := f.Running(); got != 1 {
		t.Errorf("Running() = %d, want 1", got)
	}

	if err := f.Stop(ctx, "vm-1"); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if got := f.Running(); got != 0 {
		t.Errorf("Running() after Stop = %d, want 0", got)
	}
}

func TestStartRejectsDuplicateAndUnknownStop(t *testing.T) {
	f := instant(t, 4)
	ctx := context.Background()

	if _, err := f.Start(ctx, spec("vm-1", 0)); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if _, err := f.Start(ctx, spec("vm-1", 0)); !errors.Is(err, ErrDuplicateVM) {
		t.Errorf("Start() duplicate error = %v, want ErrDuplicateVM", err)
	}
	if err := f.Stop(ctx, "ghost"); !errors.Is(err, ErrUnknownVM) {
		t.Errorf("Stop() unknown error = %v, want ErrUnknownVM", err)
	}
}

func TestStartRejectsInvalidSpec(t *testing.T) {
	f := instant(t, 4)
	if _, err := f.Start(context.Background(), Spec{}); err == nil {
		t.Error("Start() with empty spec error = nil, want non-nil")
	}
}

func TestCapacityIsEnforced(t *testing.T) {
	f := instant(t, 2)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := f.Start(ctx, spec(fmt.Sprintf("vm-%d", i), 0)); err != nil {
			t.Fatalf("Start(vm-%d) error = %v", i, err)
		}
	}
	if _, err := f.Start(ctx, spec("vm-overflow", 0)); !errors.Is(err, ErrNoCapacity) {
		t.Errorf("Start() beyond capacity error = %v, want ErrNoCapacity", err)
	}
	if got, want := f.Running(), f.Capacity(); got != want {
		t.Errorf("Running() = %d, want %d", got, want)
	}
}

// TestConcurrentStartsDoNotOversubscribe is the reason the slot is reserved
// before the simulated boot delay rather than after.
func TestConcurrentStartsDoNotOversubscribe(t *testing.T) {
	const (
		slots   = 8
		callers = 200
	)
	f, err := NewFake(FakeConfig{
		Slots:       slots,
		BootLatency: 5 * time.Millisecond,
		BootJitter:  2 * time.Millisecond,
		StopLatency: time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("NewFake() error = %v", err)
	}
	defer func() { _ = f.Close() }()

	var succeeded, rejected atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			switch _, err := f.Start(context.Background(), spec(fmt.Sprintf("vm-%d", i), 0)); {
			case err == nil:
				succeeded.Add(1)
			case errors.Is(err, ErrNoCapacity):
				rejected.Add(1)
			default:
				t.Errorf("Start() unexpected error = %v", err)
			}
		}(i)
	}
	wg.Wait()

	if got := succeeded.Load(); got != slots {
		t.Errorf("%d starts succeeded, want exactly %d", got, slots)
	}
	if got := rejected.Load(); got != callers-slots {
		t.Errorf("%d starts rejected, want %d", got, callers-slots)
	}
	if got := f.Running(); got != slots {
		t.Errorf("Running() = %d, want %d", got, slots)
	}
}

func TestTTLReapsAndNotifies(t *testing.T) {
	f := instant(t, 4)
	ctx := context.Background()

	inst, err := f.Start(ctx, spec("vm-ttl", 30*time.Millisecond))
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if inst.ExpiresAt.IsZero() {
		t.Error("Instance.ExpiresAt is zero for a microVM with a TTL")
	}

	select {
	case id, ok := <-f.Expired():
		if !ok {
			t.Fatal("Expired() channel closed before delivering the reaped microVM")
		}
		if id != "vm-ttl" {
			t.Errorf("Expired() delivered %q, want vm-ttl", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the microVM to be reaped")
	}

	if got := f.Running(); got != 0 {
		t.Errorf("Running() after reap = %d, want 0", got)
	}
}

// TestExplicitStopSuppressesTheExpiryNotification guards a double-free.
func TestExplicitStopSuppressesTheExpiryNotification(t *testing.T) {
	f := instant(t, 4)
	ctx := context.Background()

	if _, err := f.Start(ctx, spec("vm-early", 40*time.Millisecond)); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := f.Stop(ctx, "vm-early"); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	select {
	case id := <-f.Expired():
		t.Fatalf("Expired() delivered %q for a microVM that was explicitly stopped", id)
	case <-time.After(150 * time.Millisecond):
	}
}

func TestCloseIsIdempotentAndClosesTheExpiryChannel(t *testing.T) {
	f, err := NewFake(FakeConfig{Slots: 4, BootLatency: time.Nanosecond, BootJitter: time.Nanosecond, StopLatency: time.Nanosecond})
	if err != nil {
		t.Fatalf("NewFake() error = %v", err)
	}
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := f.Start(ctx, spec(fmt.Sprintf("vm-%d", i), time.Hour)); err != nil {
			t.Fatalf("Start(vm-%d) error = %v", i, err)
		}
	}

	if err := f.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := f.Close(); err != nil {
		t.Errorf("second Close() error = %v, want nil", err)
	}
	if got := f.Running(); got != 0 {
		t.Errorf("Running() after Close = %d, want 0", got)
	}
	if _, ok := <-f.Expired(); ok {
		t.Error("Expired() channel should be closed and drained after Close")
	}
	if _, err := f.Start(ctx, spec("vm-after-close", 0)); !errors.Is(err, ErrClosed) {
		t.Errorf("Start() after Close error = %v, want ErrClosed", err)
	}
	if err := f.Stop(ctx, "vm-0"); !errors.Is(err, ErrClosed) {
		t.Errorf("Stop() after Close error = %v, want ErrClosed", err)
	}
}

func TestFailureRateIsHonouredAndReleasesTheSlot(t *testing.T) {
	f, err := NewFake(FakeConfig{
		Slots:       8,
		BootLatency: time.Nanosecond,
		BootJitter:  time.Nanosecond,
		StopLatency: time.Nanosecond,
		FailureRate: 1.0, // every boot fails
		Seed:        1,
	})
	if err != nil {
		t.Fatalf("NewFake() error = %v", err)
	}
	defer func() { _ = f.Close() }()

	for i := 0; i < 20; i++ {
		if _, err := f.Start(context.Background(), spec(fmt.Sprintf("vm-%d", i), 0)); !errors.Is(err, ErrBootFailed) {
			t.Fatalf("Start() error = %v, want ErrBootFailed", err)
		}
	}
	if got := f.Running(); got != 0 {
		t.Errorf("Running() after 20 failed boots = %d, want 0", got)
	}
}

func TestPartialFailureRate(t *testing.T) {
	f, err := NewFake(FakeConfig{
		Slots: 8, BootLatency: time.Nanosecond, BootJitter: time.Nanosecond,
		StopLatency: time.Nanosecond, FailureRate: 0.25, Seed: 42,
	})
	if err != nil {
		t.Fatalf("NewFake() error = %v", err)
	}
	defer func() { _ = f.Close() }()

	const n = 4000
	failures := 0
	ctx := context.Background()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("vm-%d", i)
		switch _, err := f.Start(ctx, spec(id, 0)); {
		case errors.Is(err, ErrBootFailed):
			failures++
		case err != nil:
			t.Fatalf("Start() unexpected error = %v", err)
		default:
			if err := f.Stop(ctx, id); err != nil {
				t.Fatalf("Stop() error = %v", err)
			}
		}
	}
	if rel := float64(failures)/float64(n)/0.25 - 1; rel > 0.15 || rel < -0.15 {
		t.Errorf("observed failure rate %.3f, want about 0.25", float64(failures)/float64(n))
	}
}

func TestCancelledContextDuringBootReleasesTheSlot(t *testing.T) {
	f, err := NewFake(FakeConfig{Slots: 2, BootLatency: time.Second, BootJitter: time.Millisecond, StopLatency: time.Nanosecond})
	if err != nil {
		t.Fatalf("NewFake() error = %v", err)
	}
	defer func() { _ = f.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if _, err := f.Start(ctx, spec("vm-slow", 0)); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Start() error = %v, want context.DeadlineExceeded", err)
	}
	if got := f.Running(); got != 0 {
		t.Errorf("Running() after cancelled boot = %d, want 0", got)
	}
}

func TestBootLatencyStaysWithinTheConfiguredBand(t *testing.T) {
	const (
		mean   = 20 * time.Millisecond
		jitter = 5 * time.Millisecond
	)
	f, err := NewFake(FakeConfig{Slots: 64, BootLatency: mean, BootJitter: jitter, StopLatency: time.Nanosecond, Seed: 7})
	if err != nil {
		t.Fatalf("NewFake() error = %v", err)
	}
	defer func() { _ = f.Close() }()

	var sawBelow, sawAbove bool
	for i := 0; i < 40; i++ {
		inst, err := f.Start(context.Background(), spec(fmt.Sprintf("vm-%d", i), 0))
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}
		if inst.BootLatency < mean-jitter || inst.BootLatency > mean+jitter {
			t.Fatalf("BootLatency %s outside [%s, %s]", inst.BootLatency, mean-jitter, mean+jitter)
		}
		if inst.BootLatency < mean {
			sawBelow = true
		}
		if inst.BootLatency > mean {
			sawAbove = true
		}
	}
	if !sawBelow || !sawAbove {
		t.Error("boot latency did not vary on both sides of the mean")
	}
}

// TestSpecValidateRejectsDangerousIDs guards the two places a microVM
// identifier escapes into something that interprets it: the guest kernel
// command line, and the filesystem path a jailer builds.
func TestSpecValidateRejectsDangerousIDs(t *testing.T) {
	tests := []struct {
		name string
		id   string
	}{
		{"kernel parameter injection", "vm1 init=/bin/sh"},
		{"tab", "vm1\tinit=/bin/sh"},
		{"newline", "vm1\ninit=/bin/sh"},
		{"path separator", "../../etc/passwd"},
		{"parent reference", ".."},
		{"leading dot", ".hidden"},
		{"semicolon", "vm1;rm -rf /"},
		{"backtick", "vm1`whoami`"},
		{"dollar", "vm1$(id)"},
		{"null byte", "vm1\x00"},
		{"too long", strings.Repeat("a", 129)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := Spec{ID: tc.id, VCPUs: 1, MemMiB: 1024}
			if err := spec.Validate(); err == nil {
				t.Errorf("Validate() accepted dangerous id %q", tc.id)
			}
		})
	}

	for _, id := range []string{
		"vm-123",
		"3f2504e0-4f89-11d3-9a0c-0305e82c3301",
		"vmhost-0.slot_7",
	} {
		if err := (Spec{ID: id, VCPUs: 1, MemMiB: 1024}).Validate(); err != nil {
			t.Errorf("Validate() rejected legitimate id %q: %v", id, err)
		}
	}
}
