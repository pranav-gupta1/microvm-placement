// Package hypervisor abstracts the thing that actually runs a microVM.
package hypervisor

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Errors returned by hypervisor implementations.
var (
	ErrNoCapacity  = errors.New("hypervisor: host has no free slot")
	ErrUnknownVM   = errors.New("hypervisor: unknown microVM")
	ErrDuplicateVM = errors.New("hypervisor: microVM already running")
	ErrBootFailed  = errors.New("hypervisor: boot failed")
	ErrClosed      = errors.New("hypervisor: closed")
)

// Spec describes the microVM to create.
type Spec struct {
	ID     string
	VCPUs  int
	MemMiB int
	TTL    time.Duration
}

// maxIDLen bounds the identifier so it cannot overflow a kernel command line
// or produce an unwieldy jailer directory name.
const maxIDLen = 128

// validIDChar reports whether c is allowed in a microVM identifier.
func validIDChar(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	case c == '-', c == '_', c == '.':
		return true
	default:
		return false
	}
}

// Validate reports whether the spec is usable.
func (s Spec) Validate() error {
	if s.ID == "" {
		return fmt.Errorf("spec: ID must not be empty")
	}
	if len(s.ID) > maxIDLen {
		return fmt.Errorf("spec: ID must be at most %d bytes, got %d", maxIDLen, len(s.ID))
	}
	for i := 0; i < len(s.ID); i++ {
		if !validIDChar(s.ID[i]) {
			return fmt.Errorf("spec: ID contains disallowed byte %q at offset %d, want only letters, digits, dash, underscore or dot", s.ID[i], i)
		}
	}
	if s.ID[0] == '.' {
		return fmt.Errorf("spec: ID must not begin with a dot")
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
	ID          string
	StartedAt   time.Time
	BootLatency time.Duration
	ExpiresAt   time.Time
}

// Hypervisor runs microVMs on a single host.
type Hypervisor interface {
	Start(ctx context.Context, spec Spec) (Instance, error)

	Stop(ctx context.Context, id string) error

	Running() int

	Capacity() int

	Close() error
}

// Reaper is implemented by hypervisors that expire microVMs on their own once
// a TTL elapses, rather than requiring the caller to call Stop.
type Reaper interface {
	Expired() <-chan string
}
