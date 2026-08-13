package sim

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/pranav-gupta1/microvm-placement/internal/placement"
	"github.com/pranav-gupta1/microvm-placement/internal/scheduler"
)

// fleet models the pod lifecycle that Kubernetes would otherwise own: a new
// vmhost pod is scheduled, pulls an image and passes readiness before it can
// serve, and a removed pod finishes the microVMs it is already running before
// it goes away.
type fleet struct {
	sched        *scheduler.Scheduler
	svc          *placement.Service
	slotsPerHost int
	startLatency time.Duration

	mu       sync.Mutex
	next     int
	draining map[scheduler.HostID]struct{}
}

// reconcile drives the fleet towards the desired ready-pod count.
func (f *fleet) reconcile(ctx context.Context, desired int) {
	f.mu.Lock()
	if f.draining == nil {
		f.draining = make(map[scheduler.HostID]struct{})
	}
	f.mu.Unlock()

	f.reapDrained()

	snaps := f.sched.Hosts()
	var live []scheduler.HostSnapshot
	for _, h := range snaps {
		if h.State != scheduler.HostDraining {
			live = append(live, h)
		}
	}

	switch {
	case len(live) < desired:
		f.startHosts(ctx, desired-len(live))
	case len(live) > desired:
		f.drainHosts(live, len(live)-desired)
	}
}

// startHosts adds n pods, each becoming ready after the start latency.
func (f *fleet) startHosts(ctx context.Context, n int) {
	for i := 0; i < n; i++ {
		f.mu.Lock()
		id := scheduler.HostID(fmt.Sprintf("vmhost-%d", f.next))
		f.next++
		f.mu.Unlock()

		if err := f.sched.AddHost(id, f.slotsPerHost); err != nil {
			continue
		}
		if f.startLatency <= 0 {
			f.markReady(id)
			continue
		}
		time.AfterFunc(f.startLatency, func() {
			if ctx.Err() != nil {
				return
			}
			f.markReady(id)
		})
	}
}

// markReady promotes a pod to serving and wakes anything waiting for capacity.
func (f *fleet) markReady(id scheduler.HostID) {
	f.mu.Lock()
	_, isDraining := f.draining[id]
	f.mu.Unlock()
	if isDraining {
		return
	}
	if err := f.sched.SetHostState(id, scheduler.HostReady); err != nil {
		return
	}
	f.svc.SignalCapacity()
}

// drainHosts marks n pods for shutdown, emptiest first.
func (f *fleet) drainHosts(live []scheduler.HostSnapshot, n int) {
	ready := make([]scheduler.HostSnapshot, 0, len(live))
	for _, h := range live {
		if h.State == scheduler.HostReady {
			ready = append(ready, h)
		}
	}
	sort.Slice(ready, func(i, j int) bool {
		if ready[i].Used != ready[j].Used {
			return ready[i].Used < ready[j].Used
		}
		return ready[i].ID < ready[j].ID
	})

	for i := 0; i < n && i < len(ready); i++ {
		id := ready[i].ID
		if err := f.sched.SetHostState(id, scheduler.HostDraining); err != nil {
			continue
		}
		f.mu.Lock()
		f.draining[id] = struct{}{}
		f.mu.Unlock()
	}
	f.reapDrained()
}

// reapDrained removes draining pods once their last microVM has exited.
func (f *fleet) reapDrained() {
	f.mu.Lock()
	if len(f.draining) == 0 {
		f.mu.Unlock()
		return
	}
	pending := make([]scheduler.HostID, 0, len(f.draining))
	for id := range f.draining {
		pending = append(pending, id)
	}
	f.mu.Unlock()

	used := make(map[scheduler.HostID]int, len(pending))
	for _, h := range f.sched.Hosts() {
		used[h.ID] = h.Used
	}

	for _, id := range pending {
		u, stillPresent := used[id]
		if stillPresent && u > 0 {
			continue
		}
		if stillPresent {
			if _, err := f.sched.RemoveHost(id); err != nil {
				continue
			}
		}
		f.mu.Lock()
		delete(f.draining, id)
		f.mu.Unlock()
	}
}
