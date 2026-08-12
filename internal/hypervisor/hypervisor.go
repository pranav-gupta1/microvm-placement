// Package hypervisor abstracts the thing that actually runs a microVM.
//
// There are two implementations. Firecracker is the real one: it restores a
// microVM from a snapshot on a bare metal host with /dev/kvm. Fake is an
// in-process model that reproduces the timing and failure behaviour of the real
// one without needing hardware virtualisation.
//
// The split is deliberate and load bearing. Hardware virtualisation is only
// available on bare metal, so without a fake there is no way to exercise the
// control plane, the placement algorithm, or the 1000 requests per second path
// anywhere except an expensive cluster. With it, the entire system above the
// hypervisor boundary is testable on a laptop and in CI, and the boundary
// itself stays small enough to audit.
//
// The fake is not a stub that returns success. It models boot latency as a
// distribution, enforces slot capacity, reaps microVMs on their TTL, and can be
// told to fail, because a hypervisor that never fails would let the layers
// above it be written without error handling.
package hypervisor

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Errors returned by hypervisor implementations.
var (
	// ErrNoCapacity means the host has no free slot. The caller should not
	// have routed here; it indicates the scheduler's view is stale.
	ErrNoCapacity = errors.New("hypervisor: host has no free slot")
	// ErrUnknownVM is returned when addressing a microVM that is not running.
	ErrUnknownVM = errors.New("hypervisor: unknown microVM")
	// ErrDuplicateVM is returned when booting an ID that is already running.
	ErrDuplicateVM = errors.New("hypervisor: microVM already running")
	// ErrBootFailed wraps a failure to start a microVM.
	ErrBootFailed = errors.New("hypervisor: boot failed")
	// ErrClosed is returned once the hypervisor has been shut down.
	ErrClosed = errors.New("hypervisor: closed")
)

// Spec describes the microVM to create.
//
// The assignment fixes the shape of every request at 1 vCPU and 1 GiB, so these
// fields are constant in practice. They are carried explicitly anyway because a
// hypervisor that hardcodes its guest shape cannot be tested against the
// resource accounting that the scheduler depends on.
type Spec struct {
	// ID uniquely identifies this microVM across the fleet.
	ID string
	// VCPUs is the guest vCPU count.
	VCPUs int
	// MemMiB is the guest memory size in MiB.
	MemMiB int
	// TTL is how long the microVM should live before it is reaped. A zero TTL
	// means the microVM runs until explicitly stopped.
	TTL time.Duration
}

// Validate reports whether the spec is usable.
func (s Spec) Validate() error {
	if s.ID == "" {
		return fmt.Errorf("spec: ID must not be empty")
	}
	if s.VCPUs < 1 {
		return fmt.Errorf("spec: VCPUs must be at least 1, got %d", s.VCPUs)
	}
	if s.MemMiB < 1 {
		return fmt.Errorf("spec: MemMiB must be at least 1, got %d", s.MemMiB)
	}
	if s.TTL < 0 {
		return fmt.Errorf("spec: TTL must not be negative, got %s", s.TTL)
	}
	return nil
}

// Instance is a running microVM.
type Instance struct {
	// ID echoes Spec.ID.
	ID string
	// StartedAt is when the guest became runnable.
	StartedAt time.Time
	// BootLatency is how long Start took. For the Firecracker implementation
	// this is snapshot restore time, which is the number the whole design
	// hangs on: it is what makes per-request microVMs viable at all.
	BootLatency time.Duration
	// ExpiresAt is when the microVM will be reaped, zero if it has no TTL.
	ExpiresAt time.Time
}

// Hypervisor runs microVMs on a single host.
//
// Implementations must be safe for concurrent use: one vmhost pod fields many
// concurrent placement requests.
type Hypervisor interface {
	// Start boots a microVM and returns once the guest is runnable. It returns
	// ErrNoCapacity if every slot is occupied.
	Start(ctx context.Context, spec Spec) (Instance, error)

	// Stop shuts a microVM down and frees its slot. Stopping an unknown
	// microVM returns ErrUnknownVM.
	Stop(ctx context.Context, id string) error

	// Running returns the number of live microVMs.
	Running() int

	// Capacity returns the total number of slots on this host.
	Capacity() int

	// Close shuts down every running microVM and releases host resources.
	Close() error
}

// Reaper is implemented by hypervisors that expire microVMs on their own once
// a TTL elapses, rather than requiring the caller to call Stop.
//
// The agent uses this to learn about expiries so it can free the scheduler slot
// without polling. It is a separate interface because expiry is a property of
// how a given implementation manages guests, not of running a microVM.
type Reaper interface {
	// Expired returns a channel delivering the ID of each microVM as it is
	// reaped. The channel is closed when the hypervisor is closed.
	Expired() <-chan string
}
