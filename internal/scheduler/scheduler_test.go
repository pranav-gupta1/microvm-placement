package scheduler

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"
)

// checkInvariants asserts the internal consistency the whole design rests on.
// It runs after every mutation in the property test below, so a bug in bucket
// bookkeeping surfaces at the operation that caused it rather than as a wrong
// placement thousands of operations later.
func (s *Scheduler) checkInvariants(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()

	totalUsed, readyCount := 0, 0
	for id, h := range s.hosts {
		if h.used < 0 || h.used > h.capacity {
			t.Fatalf("host %s has used=%d outside [0, %d]", id, h.used, h.capacity)
		}
		if h.capacity < MinSlotsPerHost {
			t.Fatalf("host %s has capacity %d below minimum %d", id, h.capacity, MinSlotsPerHost)
		}
		totalUsed += h.used

		// A ready host belongs to exactly the bucket matching its free slots.
		// Anything else is either unreachable capacity or a double booking.
		for k := range s.buckets {
			present := s.buckets[k].contains(id)
			want := h.state == HostReady && k == h.free()
			if present != want {
				t.Fatalf("host %s (state=%s free=%d) presence in bucket %d = %v, want %v",
					id, h.state, h.free(), k, present, want)
			}
		}
		if h.state == HostReady {
			readyCount++
		}
	}

	if totalUsed != len(s.assigned) {
		t.Fatalf("sum of host.used = %d but %d microVMs are assigned", totalUsed, len(s.assigned))
	}

	bucketed := 0
	for k := range s.buckets {
		bucketed += s.buckets[k].len()
	}
	if bucketed != readyCount {
		t.Fatalf("%d hosts across buckets but %d hosts are ready", bucketed, readyCount)
	}

	// Every assignment must name a host that still exists.
	for vm, id := range s.assigned {
		if _, ok := s.hosts[id]; !ok {
			t.Fatalf("microVM %s is assigned to unknown host %s", vm, id)
		}
	}
}

// readyFleet builds n ready hosts of the given capacity, named host-0..host-n-1.
func readyFleet(t *testing.T, s *Scheduler, n, capacity int) {
	t.Helper()
	for i := 0; i < n; i++ {
		id := HostID(fmt.Sprintf("host-%d", i))
		if err := s.AddHost(id, capacity); err != nil {
			t.Fatalf("AddHost(%s) error = %v", id, err)
		}
		if err := s.SetHostState(id, HostReady); err != nil {
			t.Fatalf("SetHostState(%s) error = %v", id, err)
		}
	}
}

// usageHistogram returns the per-host microVM counts, sorted descending, which
// is the shape-independent way to assert how a policy distributed load.
func usageHistogram(s *Scheduler) []int {
	snaps := s.Hosts()
	counts := make([]int, 0, len(snaps))
	for _, h := range snaps {
		counts = append(counts, h.Used)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(counts)))
	return counts
}

func TestAddHostRejectsCapacityBelowMinimum(t *testing.T) {
	s := New(BestFit)
	// The assignment requires at least two microVMs per pod, so a one-slot
	// host is a configuration error we refuse rather than quietly honour.
	for _, capacity := range []int{-1, 0, 1} {
		err := s.AddHost(HostID(fmt.Sprintf("h%d", capacity)), capacity)
		if !errors.Is(err, ErrInvalidCapacity) {
			t.Errorf("AddHost(capacity=%d) error = %v, want ErrInvalidCapacity", capacity, err)
		}
	}
	if err := s.AddHost("ok", MinSlotsPerHost); err != nil {
		t.Errorf("AddHost(capacity=%d) error = %v, want nil", MinSlotsPerHost, err)
	}
}

func TestAddHostRejectsDuplicates(t *testing.T) {
	s := New(BestFit)
	if err := s.AddHost("h1", 4); err != nil {
		t.Fatalf("AddHost() error = %v", err)
	}
	if err := s.AddHost("h1", 4); !errors.Is(err, ErrDuplicateHost) {
		t.Errorf("AddHost() duplicate error = %v, want ErrDuplicateHost", err)
	}
}

func TestPendingHostsAreNotPlaceable(t *testing.T) {
	s := New(BestFit)
	if err := s.AddHost("h1", 4); err != nil {
		t.Fatalf("AddHost() error = %v", err)
	}
	// A pod that exists but has not passed readiness must not receive traffic.
	if _, err := s.Place("vm-1"); !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("Place() on pending host error = %v, want ErrNoCapacity", err)
	}

	if err := s.SetHostState("h1", HostReady); err != nil {
		t.Fatalf("SetHostState() error = %v", err)
	}
	got, err := s.Place("vm-1")
	if err != nil {
		t.Fatalf("Place() after ready error = %v", err)
	}
	if got != "h1" {
		t.Errorf("Place() = %s, want h1", got)
	}
	s.checkInvariants(t)
}

func TestBestFitPacksOntoTheFullestHost(t *testing.T) {
	s := New(BestFit)
	readyFleet(t, s, 3, 4)

	// Five microVMs across three four-slot hosts. Best fit fills one host
	// completely before touching the next, so exactly one host should be left
	// entirely empty and therefore reclaimable.
	for i := 0; i < 5; i++ {
		if _, err := s.Place(VMID(fmt.Sprintf("vm-%d", i))); err != nil {
			t.Fatalf("Place(vm-%d) error = %v", i, err)
		}
	}
	s.checkInvariants(t)

	if got, want := usageHistogram(s), []int{4, 1, 0}; !equalInts(got, want) {
		t.Errorf("usage histogram = %v, want %v", got, want)
	}
	stats := s.Stats()
	if stats.IdleHosts != 1 {
		t.Errorf("IdleHosts = %d, want 1", stats.IdleHosts)
	}
	if stats.InflightVMs != 5 {
		t.Errorf("InflightVMs = %d, want 5", stats.InflightVMs)
	}
}

func TestWorstFitSpreadsAndStrandsCapacity(t *testing.T) {
	s := New(WorstFit)
	readyFleet(t, s, 3, 4)

	for i := 0; i < 5; i++ {
		if _, err := s.Place(VMID(fmt.Sprintf("vm-%d", i))); err != nil {
			t.Fatalf("Place(vm-%d) error = %v", i, err)
		}
	}
	s.checkInvariants(t)

	// This is the case against spreading, stated as an assertion. The same
	// five microVMs now touch every host, so nothing can be scaled away even
	// though the fleet is only 42% utilised.
	if got, want := usageHistogram(s), []int{2, 2, 1}; !equalInts(got, want) {
		t.Errorf("usage histogram = %v, want %v", got, want)
	}
	if stats := s.Stats(); stats.IdleHosts != 0 {
		t.Errorf("IdleHosts = %d, want 0: worst fit is expected to strand capacity", stats.IdleHosts)
	}
}

func TestReleaseReturnsSlotsAndEmptiesHosts(t *testing.T) {
	s := New(BestFit)
	readyFleet(t, s, 2, 4)

	var placed []VMID
	for i := 0; i < 6; i++ {
		vm := VMID(fmt.Sprintf("vm-%d", i))
		if _, err := s.Place(vm); err != nil {
			t.Fatalf("Place(%s) error = %v", vm, err)
		}
		placed = append(placed, vm)
	}
	if got, want := usageHistogram(s), []int{4, 2}; !equalInts(got, want) {
		t.Fatalf("usage histogram = %v, want %v", got, want)
	}

	// Draining the two microVMs off the second host should return it to idle,
	// which is exactly the transition scale-down depends on.
	for _, vm := range placed[4:] {
		if err := s.Release(vm); err != nil {
			t.Fatalf("Release(%s) error = %v", vm, err)
		}
	}
	s.checkInvariants(t)

	if got, want := usageHistogram(s), []int{4, 0}; !equalInts(got, want) {
		t.Errorf("usage histogram after release = %v, want %v", got, want)
	}
	if stats := s.Stats(); stats.IdleHosts != 1 {
		t.Errorf("IdleHosts = %d, want 1", stats.IdleHosts)
	}
}

func TestDrainingHostKeepsItsVMsButTakesNoNewOnes(t *testing.T) {
	s := New(BestFit)
	readyFleet(t, s, 2, 4)

	first, err := s.Place("vm-0")
	if err != nil {
		t.Fatalf("Place() error = %v", err)
	}
	if err := s.SetHostState(first, HostDraining); err != nil {
		t.Fatalf("SetHostState() error = %v", err)
	}
	s.checkInvariants(t)

	// Existing placement survives the transition.
	if got, ok := s.HostOf("vm-0"); !ok || got != first {
		t.Errorf("HostOf(vm-0) = %s, %v, want %s, true", got, ok, first)
	}
	// New placements avoid it entirely, letting it reach zero and go away.
	for i := 1; i <= 4; i++ {
		got, err := s.Place(VMID(fmt.Sprintf("vm-%d", i)))
		if err != nil {
			t.Fatalf("Place(vm-%d) error = %v", i, err)
		}
		if got == first {
			t.Fatalf("Place(vm-%d) landed on draining host %s", i, first)
		}
	}
	// The remaining ready host has four slots and now holds all four.
	if _, err := s.Place("vm-5"); !errors.Is(err, ErrNoCapacity) {
		t.Errorf("Place() with only a draining host free, error = %v, want ErrNoCapacity", err)
	}
	s.checkInvariants(t)
}

func TestPlaceReturnsErrNoCapacityWhenFull(t *testing.T) {
	s := New(BestFit)
	readyFleet(t, s, 2, 2)

	for i := 0; i < 4; i++ {
		if _, err := s.Place(VMID(fmt.Sprintf("vm-%d", i))); err != nil {
			t.Fatalf("Place(vm-%d) error = %v", i, err)
		}
	}
	if _, err := s.Place("vm-overflow"); !errors.Is(err, ErrNoCapacity) {
		t.Errorf("Place() when full error = %v, want ErrNoCapacity", err)
	}
	// A failed placement must not have consumed anything.
	s.checkInvariants(t)
	if stats := s.Stats(); stats.InflightVMs != 4 {
		t.Errorf("InflightVMs = %d, want 4", stats.InflightVMs)
	}
}

func TestPlaceRejectsDuplicateVM(t *testing.T) {
	s := New(BestFit)
	readyFleet(t, s, 1, 4)

	if _, err := s.Place("vm-1"); err != nil {
		t.Fatalf("Place() error = %v", err)
	}
	if _, err := s.Place("vm-1"); !errors.Is(err, ErrDuplicateVM) {
		t.Errorf("Place() duplicate error = %v, want ErrDuplicateVM", err)
	}
	// The rejected duplicate must not have burned a second slot.
	if stats := s.Stats(); stats.Used != 1 {
		t.Errorf("Used = %d, want 1", stats.Used)
	}
	s.checkInvariants(t)
}

func TestReleaseUnknownVM(t *testing.T) {
	s := New(BestFit)
	if err := s.Release("ghost"); !errors.Is(err, ErrUnknownVM) {
		t.Errorf("Release() error = %v, want ErrUnknownVM", err)
	}
}

func TestUnknownHostOperations(t *testing.T) {
	s := New(BestFit)
	if err := s.SetHostState("ghost", HostReady); !errors.Is(err, ErrUnknownHost) {
		t.Errorf("SetHostState() error = %v, want ErrUnknownHost", err)
	}
	if _, err := s.RemoveHost("ghost"); !errors.Is(err, ErrUnknownHost) {
		t.Errorf("RemoveHost() error = %v, want ErrUnknownHost", err)
	}
}

func TestRemoveHostReportsOrphanedVMs(t *testing.T) {
	s := New(BestFit)
	readyFleet(t, s, 2, 4)

	for i := 0; i < 5; i++ {
		if _, err := s.Place(VMID(fmt.Sprintf("vm-%d", i))); err != nil {
			t.Fatalf("Place(vm-%d) error = %v", i, err)
		}
	}
	// Best fit filled host-1 first, so removing it should orphan four microVMs.
	orphaned, err := s.RemoveHost("host-1")
	if err != nil {
		t.Fatalf("RemoveHost() error = %v", err)
	}
	if len(orphaned) != 4 {
		t.Errorf("RemoveHost() orphaned %d microVMs, want 4", len(orphaned))
	}
	s.checkInvariants(t)

	// The orphans must no longer resolve to any host.
	for _, vm := range orphaned {
		if _, ok := s.HostOf(vm); ok {
			t.Errorf("HostOf(%s) still resolves after host removal", vm)
		}
	}
	if stats := s.Stats(); stats.InflightVMs != 1 {
		t.Errorf("InflightVMs = %d, want 1", stats.InflightVMs)
	}
}

func TestStatsTracksIdleAndUnderfilledHosts(t *testing.T) {
	s := New(BestFit)
	readyFleet(t, s, 4, 4)

	// One host full, one holding a single microVM, two untouched.
	for i := 0; i < 5; i++ {
		if _, err := s.Place(VMID(fmt.Sprintf("vm-%d", i))); err != nil {
			t.Fatalf("Place(vm-%d) error = %v", i, err)
		}
	}
	stats := s.Stats()
	if stats.Hosts != 4 || stats.ReadyHosts != 4 {
		t.Errorf("Hosts=%d ReadyHosts=%d, want 4 and 4", stats.Hosts, stats.ReadyHosts)
	}
	if stats.IdleHosts != 2 {
		t.Errorf("IdleHosts = %d, want 2", stats.IdleHosts)
	}
	// One host runs a single microVM, which the assignment's two-per-pod floor
	// treats as underfilled.
	if stats.UnderfilledHosts != 1 {
		t.Errorf("UnderfilledHosts = %d, want 1", stats.UnderfilledHosts)
	}
	if got, want := stats.Utilisation(), 5.0/16.0; got != want {
		t.Errorf("Utilisation() = %v, want %v", got, want)
	}
}

func TestStatsUtilisationWithNoCapacity(t *testing.T) {
	s := New(BestFit)
	if got := s.Stats().Utilisation(); got != 0 {
		t.Errorf("Utilisation() with no hosts = %v, want 0", got)
	}
}

func TestHostStateAndPolicyStrings(t *testing.T) {
	// These strings end up in logs and metric labels, so pin them.
	if got, want := HostReady.String(), "Ready"; got != want {
		t.Errorf("HostReady.String() = %q, want %q", got, want)
	}
	if got, want := HostDraining.String(), "Draining"; got != want {
		t.Errorf("HostDraining.String() = %q, want %q", got, want)
	}
	if got, want := HostPending.String(), "Pending"; got != want {
		t.Errorf("HostPending.String() = %q, want %q", got, want)
	}
	if got, want := BestFit.String(), "BestFit"; got != want {
		t.Errorf("BestFit.String() = %q, want %q", got, want)
	}
	if got, want := WorstFit.String(), "WorstFit"; got != want {
		t.Errorf("WorstFit.String() = %q, want %q", got, want)
	}
}

func TestSetHostStateToSameStateIsANoop(t *testing.T) {
	s := New(BestFit)
	readyFleet(t, s, 1, 4)
	// Repeated readiness events from the informer are normal. They must not
	// double-insert the host into its bucket.
	for i := 0; i < 3; i++ {
		if err := s.SetHostState("host-0", HostReady); err != nil {
			t.Fatalf("SetHostState() error = %v", err)
		}
	}
	s.checkInvariants(t)
}

// TestSchedulerInvariantsUnderRandomOperations drives the scheduler through a
// long random sequence of every operation, including the awkward interleavings
// a real informer produces: hosts going ready and draining mid-flight, pods
// vanishing while holding microVMs, releases arriving after removal.
func TestSchedulerInvariantsUnderRandomOperations(t *testing.T) {
	const (
		capacity = 8
		ops      = 20000
	)
	rng := rand.New(rand.NewPCG(1, 2))
	s := New(BestFit)

	live := map[VMID]struct{}{}
	hosts := map[HostID]struct{}{}
	nextHost, nextVM := 0, 0

	pickHost := func() (HostID, bool) {
		if len(hosts) == 0 {
			return "", false
		}
		ids := make([]HostID, 0, len(hosts))
		for id := range hosts {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		return ids[rng.IntN(len(ids))], true
	}
	pickVM := func() (VMID, bool) {
		if len(live) == 0 {
			return "", false
		}
		ids := make([]VMID, 0, len(live))
		for id := range live {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		return ids[rng.IntN(len(ids))], true
	}

	for i := 0; i < ops; i++ {
		switch rng.IntN(100) {
		case 0, 1, 2, 3, 4: // add a host
			id := HostID(fmt.Sprintf("h-%d", nextHost))
			nextHost++
			if err := s.AddHost(id, capacity); err != nil {
				t.Fatalf("op %d: AddHost(%s) error = %v", i, id, err)
			}
			hosts[id] = struct{}{}

		case 5, 6, 7, 8, 9, 10: // flip a host's state
			if id, ok := pickHost(); ok {
				state := HostState(rng.IntN(3))
				if err := s.SetHostState(id, state); err != nil {
					t.Fatalf("op %d: SetHostState(%s, %s) error = %v", i, id, state, err)
				}
			}

		case 11, 12: // remove a host, possibly while it holds microVMs
			if id, ok := pickHost(); ok {
				orphaned, err := s.RemoveHost(id)
				if err != nil {
					t.Fatalf("op %d: RemoveHost(%s) error = %v", i, id, err)
				}
				delete(hosts, id)
				for _, vm := range orphaned {
					delete(live, vm)
				}
			}

		case 13, 14, 15, 16, 17, 18, 19: // release a microVM
			if vm, ok := pickVM(); ok {
				if err := s.Release(vm); err != nil {
					t.Fatalf("op %d: Release(%s) error = %v", i, vm, err)
				}
				delete(live, vm)
			}

		default: // place a microVM
			vm := VMID(fmt.Sprintf("vm-%d", nextVM))
			nextVM++
			switch _, err := s.Place(vm); {
			case err == nil:
				live[vm] = struct{}{}
			case errors.Is(err, ErrNoCapacity):
				// Legitimate: the fleet is genuinely full.
			default:
				t.Fatalf("op %d: Place(%s) unexpected error = %v", i, vm, err)
			}
		}

		s.checkInvariants(t)
	}

	// The test is only meaningful if it actually exercised placement.
	if nextVM < 1000 {
		t.Fatalf("only attempted %d placements, the random walk is not exercising the scheduler", nextVM)
	}
}

// TestBestFitLeavesMoreHostsIdleThanWorstFit is the design decision expressed as
// a measurement: under identical load, best fit must leave strictly more hosts
// empty, because empty hosts are the only ones that can be scaled away.
func TestBestFitLeavesMoreHostsIdleThanWorstFit(t *testing.T) {
	const (
		hosts    = 20
		capacity = 8
		vms      = 40 // a quarter of total capacity
	)
	idleUnder := func(p Policy) int {
		s := New(p)
		readyFleet(t, s, hosts, capacity)
		for i := 0; i < vms; i++ {
			if _, err := s.Place(VMID(fmt.Sprintf("vm-%d", i))); err != nil {
				t.Fatalf("Place(vm-%d) under %s error = %v", i, p, err)
			}
		}
		return s.Stats().IdleHosts
	}

	best, worst := idleUnder(BestFit), idleUnder(WorstFit)
	// Best fit needs ceil(40/8) = 5 hosts, leaving 15 idle. Worst fit touches
	// every host, leaving none.
	if best != 15 {
		t.Errorf("BestFit left %d hosts idle, want 15", best)
	}
	if worst != 0 {
		t.Errorf("WorstFit left %d hosts idle, want 0", worst)
	}
	if best <= worst {
		t.Errorf("BestFit idle=%d must exceed WorstFit idle=%d", best, worst)
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// BenchmarkPlaceRelease measures the hot path at the fleet size the system runs
// at peak: about 79 hosts of 8 slots. Placement must stay far below the
// per-request budget at 1000 requests per second.
func BenchmarkPlaceRelease(b *testing.B) {
	const (
		hosts    = 80
		capacity = 8
	)
	s := New(BestFit)
	for i := 0; i < hosts; i++ {
		id := HostID(fmt.Sprintf("host-%d", i))
		if err := s.AddHost(id, capacity); err != nil {
			b.Fatalf("AddHost() error = %v", err)
		}
		if err := s.SetHostState(id, HostReady); err != nil {
			b.Fatalf("SetHostState() error = %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm := VMID(fmt.Sprintf("vm-%d", i))
		if _, err := s.Place(vm); err != nil {
			b.Fatalf("Place() error = %v", err)
		}
		if err := s.Release(vm); err != nil {
			b.Fatalf("Release() error = %v", err)
		}
	}
}
