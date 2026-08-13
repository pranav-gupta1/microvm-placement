// Package scheduler decides which vmhost pod a microVM is placed on.
package scheduler

import (
	"errors"
	"fmt"
	"sync"
)

// MinSlotsPerHost is the floor the assignment imposes: a pod virtualises
// several microVMs, never a single one.
const MinSlotsPerHost = 2

// Errors returned by the scheduler.
var (
	ErrNoCapacity      = errors.New("scheduler: no ready host has a free slot")
	ErrUnknownHost     = errors.New("scheduler: unknown host")
	ErrUnknownVM       = errors.New("scheduler: unknown vm")
	ErrDuplicateHost   = errors.New("scheduler: host already exists")
	ErrDuplicateVM     = errors.New("scheduler: vm already placed")
	ErrInvalidCapacity = errors.New("scheduler: host capacity below minimum")
)

// HostID identifies a vmhost pod.
type HostID string

// VMID identifies a single placed microVM.
type VMID string

// HostState controls whether a host may receive new microVMs.
type HostState int

// Host lifecycle states. Only HostReady accepts new microVMs; HostDraining
// keeps serving the ones it has so scale-down never orphans a guest.
const (
	HostPending HostState = iota
	HostReady
	HostDraining
)

// String implements fmt.Stringer.
func (s HostState) String() string {
	switch s {
	case HostPending:
		return "Pending"
	case HostReady:
		return "Ready"
	case HostDraining:
		return "Draining"
	default:
		return fmt.Sprintf("HostState(%d)", int(s))
	}
}

// Policy selects among hosts that have free capacity.
type Policy int

// Placement policies. BestFit packs onto the fullest host so the rest drain
// to empty and become reclaimable; WorstFit exists for comparison in tests.
const (
	BestFit Policy = iota
	WorstFit
)

// String implements fmt.Stringer.
func (p Policy) String() string {
	switch p {
	case BestFit:
		return "BestFit"
	case WorstFit:
		return "WorstFit"
	default:
		return fmt.Sprintf("Policy(%d)", int(p))
	}
}

// host is the scheduler's private view of a vmhost pod.
type host struct {
	id       HostID
	capacity int
	used     int
	state    HostState
}

func (h *host) free() int { return h.capacity - h.used }

// Scheduler tracks host capacity and assigns microVMs to hosts.
type Scheduler struct {
	mu       sync.Mutex
	policy   Policy
	hosts    map[HostID]*host
	assigned map[VMID]HostID
	buckets  []hostSet
	maxCap   int
}

// New returns an empty scheduler using the given policy.
func New(policy Policy) *Scheduler {
	return &Scheduler{
		policy:   policy,
		hosts:    make(map[HostID]*host),
		assigned: make(map[VMID]HostID),
	}
}

// AddHost registers a host with the given slot capacity, in HostPending state.
func (s *Scheduler) AddHost(id HostID, capacity int) error {
	if capacity < MinSlotsPerHost {
		return fmt.Errorf("%w: host %s declared %d slots, minimum is %d", ErrInvalidCapacity, id, capacity, MinSlotsPerHost)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.hosts[id]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateHost, id)
	}
	s.growBuckets(capacity)
	s.hosts[id] = &host{id: id, capacity: capacity, state: HostPending}
	return nil
}

// growBuckets widens the bucket slice so index `capacity` is addressable.
func (s *Scheduler) growBuckets(capacity int) {
	for len(s.buckets) <= capacity {
		s.buckets = append(s.buckets, newHostSet())
	}
	if capacity > s.maxCap {
		s.maxCap = capacity
	}
}

// SetHostState moves a host between pending, ready and draining.
func (s *Scheduler) SetHostState(id HostID, state HostState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	h, ok := s.hosts[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownHost, id)
	}
	if h.state == state {
		return nil
	}
	if h.state == HostReady {
		s.buckets[h.free()].remove(id)
	}
	h.state = state
	if state == HostReady {
		s.buckets[h.free()].add(id)
	}
	return nil
}

// RemoveHost forgets a host entirely and returns the IDs of microVMs that were
// running on it.
func (s *Scheduler) RemoveHost(id HostID) ([]VMID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	h, ok := s.hosts[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownHost, id)
	}
	if h.state == HostReady {
		s.buckets[h.free()].remove(id)
	}
	delete(s.hosts, id)

	var orphaned []VMID
	for vm, hostID := range s.assigned {
		if hostID == id {
			orphaned = append(orphaned, vm)
			delete(s.assigned, vm)
		}
	}
	return orphaned, nil
}

// Place assigns a microVM to a host and returns the chosen host.
func (s *Scheduler) Place(vm VMID) (HostID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.assigned[vm]; exists {
		return "", fmt.Errorf("%w: %s", ErrDuplicateVM, vm)
	}
	id, ok := s.selectHost()
	if !ok {
		return "", ErrNoCapacity
	}

	h := s.hosts[id]
	s.buckets[h.free()].remove(id)
	h.used++
	s.buckets[h.free()].add(id)
	s.assigned[vm] = id
	return id, nil
}

// selectHost applies the policy over the free-slot buckets.
func (s *Scheduler) selectHost() (HostID, bool) {
	if s.policy == WorstFit {
		for k := s.maxCap; k >= 1; k-- {
			if id, ok := s.buckets[k].any(); ok {
				return id, true
			}
		}
		return "", false
	}
	for k := 1; k <= s.maxCap; k++ {
		if id, ok := s.buckets[k].any(); ok {
			return id, true
		}
	}
	return "", false
}

// Release frees the slot a microVM was occupying.
func (s *Scheduler) Release(vm VMID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	id, ok := s.assigned[vm]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownVM, vm)
	}
	delete(s.assigned, vm)

	h, ok := s.hosts[id]
	if !ok {
		return nil
	}
	if h.state == HostReady {
		s.buckets[h.free()].remove(id)
	}
	h.used--
	if h.state == HostReady {
		s.buckets[h.free()].add(id)
	}
	return nil
}

// Stats is a point-in-time summary of fleet state, used for metrics and for
// the autoscaling signal.
type Stats struct {
	Hosts            int
	ReadyHosts       int
	DrainingHosts    int
	IdleHosts        int
	UnderfilledHosts int
	Capacity         int
	Used             int
	InflightVMs      int
}

// Utilisation is the fraction of ready-host slots currently occupied.
func (s Stats) Utilisation() float64 {
	if s.Capacity == 0 {
		return 0
	}
	return float64(s.Used) / float64(s.Capacity)
}

// Stats returns a snapshot of fleet state.
func (s *Scheduler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := Stats{
		Hosts:       len(s.hosts),
		InflightVMs: len(s.assigned),
	}
	for _, h := range s.hosts {
		out.Used += h.used
		switch h.state {
		case HostReady:
			out.ReadyHosts++
			out.Capacity += h.capacity
			switch h.used {
			case 0:
				out.IdleHosts++
			case 1:
				out.UnderfilledHosts++
			}
		case HostDraining:
			out.DrainingHosts++
		case HostPending:
		}
	}
	return out
}

// HostSnapshot is an exported view of a single host, ordered by ID when
// returned from Hosts.
type HostSnapshot struct {
	ID       HostID
	Capacity int
	Used     int
	State    HostState
}

// Hosts returns a snapshot of every host, useful for the debug endpoint.
func (s *Scheduler) Hosts() []HostSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]HostSnapshot, 0, len(s.hosts))
	for _, h := range s.hosts {
		out = append(out, HostSnapshot{ID: h.id, Capacity: h.capacity, Used: h.used, State: h.state})
	}
	return out
}

// HostOf returns the host a microVM is placed on.
func (s *Scheduler) HostOf(vm VMID) (HostID, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.assigned[vm]
	return id, ok
}
