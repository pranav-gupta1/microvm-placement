// Package registry tracks which vmhost pods exist and whether they are
// healthy.
package registry

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pranav-gupta1/microvm-placement/internal/scheduler"
)

// Defaults for host liveness.
const (
	DefaultHeartbeatInterval = 2 * time.Second
	DefaultHeartbeatTimeout  = 6 * time.Second
	DefaultSweepInterval     = time.Second
)

// Errors returned by the registry.
var (
	ErrUnknownHost = errors.New("registry: unknown host")
)

// Config tunes host liveness detection.
type Config struct {
	HeartbeatTimeout time.Duration
	SweepInterval    time.Duration
	Now              func() time.Time
}

func (c *Config) applyDefaults() {
	if c.HeartbeatTimeout == 0 {
		c.HeartbeatTimeout = DefaultHeartbeatTimeout
	}
	if c.SweepInterval == 0 {
		c.SweepInterval = DefaultSweepInterval
	}
	if c.Now == nil {
		c.Now = time.Now
	}
}

// Validate reports whether the configuration is usable.
func (c Config) Validate() error {
	if c.HeartbeatTimeout <= 0 {
		return fmt.Errorf("registry: HeartbeatTimeout must be positive, got %s", c.HeartbeatTimeout)
	}
	if c.SweepInterval <= 0 {
		return fmt.Errorf("registry: SweepInterval must be positive, got %s", c.SweepInterval)
	}
	return nil
}

// Notifier is told when capacity may have appeared, so queued requests can be
// woken rather than waiting out their retry interval.
type Notifier interface {
	SignalCapacity()
}

// Registry owns host membership and liveness, and drives the scheduler.
type Registry struct {
	cfg      Config
	sched    *scheduler.Scheduler
	notifier Notifier
	orphaned atomic.Uint64

	mu       sync.Mutex
	lastSeen map[scheduler.HostID]time.Time
	address  map[scheduler.HostID]string
	// drainingSince records when a host began draining, so one whose agent has
	// gone away can eventually be force-removed rather than holding its slots
	// for ever.
	drainingSince map[scheduler.HostID]time.Time
}

// ForceRemoveAfter bounds how long a draining host may hold slots once its
// agent has stopped heartbeating.
//
// A pod deleted by a scale-down takes its agent with it, so no TTL will ever
// fire for the microVMs it was running and no expiry will ever be reported.
// Without this the host sits in Draining with Used above zero, is never reaped,
// and its slots leak permanently. Observed as inflight microVMs sticking at a
// non-zero floor long after a load run had finished.
//
// The grace period is generous because the normal path is for guests to expire
// on their own. This only catches the case where nothing is left to expire them.
const ForceRemoveAfter = 30 * time.Second

// New returns a Registry.
func New(sched *scheduler.Scheduler, notifier Notifier, cfg Config) (*Registry, error) {
	if sched == nil {
		return nil, errors.New("registry: scheduler must not be nil")
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Registry{
		cfg:           cfg,
		sched:         sched,
		notifier:      notifier,
		lastSeen:      make(map[scheduler.HostID]time.Time),
		address:       make(map[scheduler.HostID]string),
		drainingSince: make(map[scheduler.HostID]time.Time),
	}, nil
}

// Register adds a host and marks it ready, or refreshes one that already
// exists.
func (r *Registry) Register(id scheduler.HostID, capacity int, address string) error {
	if id == "" {
		return errors.New("registry: host id must not be empty")
	}

	r.mu.Lock()
	_, known := r.lastSeen[id]
	r.lastSeen[id] = r.cfg.Now()
	if address != "" {
		r.address[id] = address
	}
	r.mu.Unlock()

	if known {
		return nil
	}

	if err := r.sched.AddHost(id, capacity); err != nil {
		if !errors.Is(err, scheduler.ErrDuplicateHost) {
			r.mu.Lock()
			delete(r.lastSeen, id)
			delete(r.address, id)
			r.mu.Unlock()
			return err
		}
	}
	if err := r.sched.SetHostState(id, scheduler.HostReady); err != nil {
		return err
	}
	if r.notifier != nil {
		r.notifier.SignalCapacity()
	}
	return nil
}

// Heartbeat refreshes a host's liveness.
func (r *Registry) Heartbeat(id scheduler.HostID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.lastSeen[id]; !ok {
		return fmt.Errorf("%w: %s", ErrUnknownHost, id)
	}
	r.lastSeen[id] = r.cfg.Now()
	return nil
}

// Deregister drains a host that is shutting down cleanly.
func (r *Registry) Deregister(id scheduler.HostID) error {
	r.mu.Lock()
	_, known := r.lastSeen[id]
	delete(r.lastSeen, id)
	delete(r.address, id)
	r.mu.Unlock()

	if !known {
		return fmt.Errorf("%w: %s", ErrUnknownHost, id)
	}
	r.mu.Lock()
	r.drainingSince[id] = r.cfg.Now()
	r.mu.Unlock()
	return r.sched.SetHostState(id, scheduler.HostDraining)
}

// Sweep drains hosts that have stopped heartbeating and removes drained hosts
// that are now empty.
func (r *Registry) Sweep() int {
	now := r.cfg.Now()

	r.mu.Lock()
	var expired []scheduler.HostID
	for id, seen := range r.lastSeen {
		if now.Sub(seen) > r.cfg.HeartbeatTimeout {
			expired = append(expired, id)
		}
	}
	for _, id := range expired {
		delete(r.lastSeen, id)
		delete(r.address, id)
	}
	r.mu.Unlock()

	for _, id := range expired {
		_ = r.sched.SetHostState(id, scheduler.HostDraining)
		r.mu.Lock()
		if _, ok := r.drainingSince[id]; !ok {
			r.drainingSince[id] = now
		}
		r.mu.Unlock()
	}

	r.reapEmptyDrainingHosts()
	return len(expired)
}

// reapEmptyDrainingHosts removes draining hosts once their last microVM exits,
// which is what actually returns capacity to the cluster.
func (r *Registry) reapEmptyDrainingHosts() {
	now := r.cfg.Now()
	for _, h := range r.sched.Hosts() {
		if h.State != scheduler.HostDraining {
			continue
		}
		r.mu.Lock()
		_, stillTracked := r.lastSeen[h.ID]
		since, known := r.drainingSince[h.ID]
		r.mu.Unlock()
		if stillTracked {
			continue
		}

		// Still serving, and its agent may yet report those guests expiring.
		// Give it until ForceRemoveAfter before assuming nothing will.
		if h.Used != 0 && known && now.Sub(since) < ForceRemoveAfter {
			continue
		}

		orphaned, err := r.sched.RemoveHost(h.ID)
		if err != nil {
			continue
		}
		if len(orphaned) > 0 {
			// The agent is gone, so these guests are already dead. Releasing
			// their slots is recovering leaked capacity, not discarding work.
			r.orphaned.Add(uint64(len(orphaned)))
		}
		r.mu.Lock()
		delete(r.drainingSince, h.ID)
		r.mu.Unlock()
	}
}

// Orphaned counts microVMs whose slots were reclaimed because their host
// vanished. A rising value means pods are being removed faster than their
// guests retire.
func (r *Registry) Orphaned() uint64 { return r.orphaned.Load() }

// Run sweeps on an interval until done is closed.
func (r *Registry) Run(done <-chan struct{}) {
	ticker := time.NewTicker(r.cfg.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			r.Sweep()
		}
	}
}

// Address returns where a host can be reached to boot guests.
func (r *Registry) Address(id scheduler.HostID) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.address[id]
	return a, ok
}

// Tracked returns how many hosts are currently sending heartbeats.
func (r *Registry) Tracked() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.lastSeen)
}
