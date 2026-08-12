// Package scheduler decides which vmhost pod a microVM is placed on.
//
// The scheduler is deliberately free of I/O. It is fed host state by the
// registry, which watches pods, and it hands back a host ID. That separation is
// what lets the placement algorithm be tested exhaustively without a cluster.
//
// # Why best fit
//
// The objective is to minimise idle pods without dropping requests, so the
// placement policy is the first lever. Best fit sends each new microVM to the
// most heavily loaded host that still has room. Load concentrates on a small
// set of hosts and the remainder drain to empty as their microVMs expire.
// Empty pods are what KEDA can scale away and what Karpenter can then
// consolidate off a node, so packing tightly is directly what shrinks the bill.
//
// Worst fit, the obvious alternative, spreads microVMs across every host. It
// gives better tail latency under a hot-spot workload but it is actively
// harmful here: every host ends up holding a few microVMs, none ever reaches
// zero, and nothing is ever reclaimable. WorstFit is implemented anyway so the
// two can be compared under the same test harness.
package scheduler

import (
	"errors"
	"fmt"
	"sync"
)

// MinSlotsPerHost is the floor the assignment imposes: a pod virtualises
// several microVMs, never a single one. Enforcing it at admission means a
// misconfigured slot count fails loudly at startup rather than silently
// producing one-VM-per-pod, which would defeat the point of the exercise.
const MinSlotsPerHost = 2

// Errors returned by the scheduler. Callers distinguish these with errors.Is.
var (
	// ErrNoCapacity means every ready host is full. The caller must queue and
	// retry rather than reject: dropping is the one outcome that fails the
	// assignment outright.
	ErrNoCapacity = errors.New("scheduler: no ready host has a free slot")
	// ErrUnknownHost is returned when addressing a host the scheduler has
	// never been told about, or has already removed.
	ErrUnknownHost = errors.New("scheduler: unknown host")
	// ErrUnknownVM is returned when releasing a microVM that is not placed.
	ErrUnknownVM = errors.New("scheduler: unknown vm")
	// ErrDuplicateHost is returned when adding a host ID that already exists.
	ErrDuplicateHost = errors.New("scheduler: host already exists")
	// ErrDuplicateVM is returned when placing a microVM ID that is already placed.
	ErrDuplicateVM = errors.New("scheduler: vm already placed")
	// ErrInvalidCapacity is returned when a host declares fewer slots than
	// MinSlotsPerHost.
	ErrInvalidCapacity = errors.New("scheduler: host capacity below minimum")
)

// HostID identifies a vmhost pod. In the cluster this is the pod name.
type HostID string

// VMID identifies a single placed microVM.
type VMID string

// HostState controls whether a host may receive new microVMs.
type HostState int

const (
	// HostPending is a host that exists but is not yet serving, for example a
	// pod that has not passed its readiness probe. It holds no microVMs.
	HostPending HostState = iota
	// HostReady is a host accepting placements.
	HostReady
	// HostDraining is a host that is shutting down. It keeps serving the
	// microVMs it already holds but receives no new ones, so it can reach
	// zero and be reclaimed. This is the mechanism behind graceful scale-down.
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

const (
	// BestFit picks the fullest host that still has a free slot. This is the
	// default and the one the deployed system uses.
	BestFit Policy = iota
	// WorstFit picks the emptiest host. Present for comparison in tests and
	// benchmarks; it deliberately maximises the number of occupied pods.
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
//
// It is safe for concurrent use. Placement is on the hot path at 1000 requests
// per second, so the implementation keeps hosts bucketed by free-slot count:
// selection is a scan over at most maxCapacity+1 buckets rather than a sort or
// a scan over every host, which keeps placement O(1) in the number of hosts.
type Scheduler struct {
	mu       sync.Mutex
	policy   Policy
	hosts    map[HostID]*host
	assigned map[VMID]HostID
	// buckets[k] holds every ready host with exactly k free slots. Hosts that
	// are pending or draining are in no bucket at all, which is what makes
	// them ineligible without needing a filter on the hot path.
	buckets []hostSet
	maxCap  int
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
// The registry promotes it to ready once the pod reports healthy.
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
// Capacity is uniform in practice, so this runs once.
func (s *Scheduler) growBuckets(capacity int) {
	for len(s.buckets) <= capacity {
		s.buckets = append(s.buckets, newHostSet())
	}
	if capacity > s.maxCap {
		s.maxCap = capacity
	}
}

// SetHostState moves a host between pending, ready and draining.
//
// Transitioning to ready makes the host eligible for placement. Transitioning
// away from ready makes it ineligible immediately, but does not disturb the
// microVMs it is already running: those keep their placement until released.
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
//
// A pod deleted while holding microVMs has lost them, so the caller is
// responsible for deciding what that means. In this system the pod's
// terminationGracePeriod is set well above the microVM TTL and scale-down goes
// through HostDraining, so a removal with a non-empty return value is a signal
// that something went wrong and is surfaced as a metric.
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
//
// It returns ErrNoCapacity when every ready host is full. The caller must not
// translate that into a dropped request: the placement API queues and retries
// while capacity is provisioned.
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

// selectHost applies the policy over the free-slot buckets. Callers hold s.mu.
//
// Buckets are indexed by free slots, so best fit is the first non-empty bucket
// scanning up from 1 and worst fit is the first scanning down from maxCap.
// Bucket 0 is skipped: those hosts are full.
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

	// The host may already be gone if the pod was removed underneath us. The
	// assignment map is the source of truth for the microVM, so this is not an
	// error, just nothing left to credit the slot back to.
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
	// Hosts is every host known to the scheduler, in any state.
	Hosts int
	// ReadyHosts is the subset eligible for placement.
	ReadyHosts int
	// DrainingHosts is the subset on its way out.
	DrainingHosts int
	// IdleHosts counts ready hosts running no microVMs at all. This is the
	// number the objective asks us to minimise, and the direct input to the
	// scale-down decision.
	IdleHosts int
	// UnderfilledHosts counts ready hosts running exactly one microVM. The
	// assignment requires at least two microVMs per pod, so a sustained
	// non-zero value here means the fleet is scaled too wide.
	UnderfilledHosts int
	// Capacity is total slots across ready hosts.
	Capacity int
	// Used is slots currently occupied across all hosts, equal to the number
	// of placed microVMs.
	Used int
	// InflightVMs is the number of placed microVMs.
	InflightVMs int
}

// Utilisation is the fraction of ready-host slots currently occupied. It
// returns 0 when there is no ready capacity.
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
			// Counted in Hosts only: no capacity, no placements.
		}
	}
	return out
}

// HostSnapshot is an exported view of a single host, ordered by ID when
// returned from Hosts. It exists for metrics and debugging endpoints.
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
