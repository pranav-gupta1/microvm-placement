// Package registry tracks which vmhost pods exist and whether they are healthy.
//
// # Why self-registration rather than a pod informer
//
// The obvious Kubernetes-native design is to watch Pod objects and derive host
// state from pod phase and readiness. This package does something simpler:
// each vmhostd registers itself with the placement API on startup and sends
// periodic heartbeats, and a host that stops heartbeating is drained.
//
// The trade is deliberate. An informer needs RBAC, a cache that can go stale
// during API server contention, and a mapping from pod readiness to "can this
// process actually accept a microVM right now". Readiness is a proxy for that
// question; a heartbeat from the process that owns the slots is a direct
// answer. It also means the same code path runs identically under Kubernetes,
// under docker-compose, and in a unit test, with no fake clientset anywhere.
//
// What it costs is detection latency on an abrupt pod death: a crashed vmhostd
// is noticed after HeartbeatTimeout rather than immediately. That is bounded
// and tunable, and placement failures against a dead host are already handled
// by the admission queue retrying elsewhere.
package registry

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/pranav-gupta1/microvm-placement/internal/scheduler"
)

// Defaults for host liveness.
//
// The timeout is three heartbeat intervals so a single dropped heartbeat, or
// one lost to a garbage collection pause, does not evict a healthy host and
// churn its microVMs for nothing.
const (
	DefaultHeartbeatInterval = 2 * time.Second
	DefaultHeartbeatTimeout  = 6 * time.Second
	DefaultSweepInterval     = time.Second
)

// Errors returned by the registry.
var (
	// ErrUnknownHost is returned for operations on a host that never
	// registered, or has already been evicted.
	ErrUnknownHost = errors.New("registry: unknown host")
)

// Config tunes host liveness detection.
type Config struct {
	// HeartbeatTimeout is how long a host may go silent before it is drained.
	HeartbeatTimeout time.Duration
	// SweepInterval is how often expiry is checked.
	SweepInterval time.Duration
	// Now is injectable so tests can drive expiry without sleeping.
	Now func() time.Time
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

	mu       sync.Mutex
	lastSeen map[scheduler.HostID]time.Time
}

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
		cfg:      cfg,
		sched:    sched,
		notifier: notifier,
		lastSeen: make(map[scheduler.HostID]time.Time),
	}, nil
}

// Register adds a host and marks it ready, or refreshes one that already
// exists.
//
// Re-registration is treated as a heartbeat rather than an error because a
// vmhostd that restarts in place, or one whose first response was lost, will
// legitimately call this twice. Making it idempotent removes a whole class of
// startup race from the agent.
func (r *Registry) Register(id scheduler.HostID, capacity int) error {
	if id == "" {
		return errors.New("registry: host id must not be empty")
	}

	r.mu.Lock()
	_, known := r.lastSeen[id]
	r.lastSeen[id] = r.cfg.Now()
	r.mu.Unlock()

	if known {
		// Already present: a repeat registration is just a liveness signal.
		return nil
	}

	if err := r.sched.AddHost(id, capacity); err != nil {
		if !errors.Is(err, scheduler.ErrDuplicateHost) {
			r.mu.Lock()
			delete(r.lastSeen, id)
			r.mu.Unlock()
			return err
		}
		// The scheduler knows it but we did not, for example after a registry
		// restart. Adopt it rather than failing.
	}
	if err := r.sched.SetHostState(id, scheduler.HostReady); err != nil {
		return err
	}
	// New capacity is exactly what a queued request is waiting for.
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
//
// The host is moved to draining rather than removed, so the microVMs it is
// still running finish rather than being orphaned. This is the graceful path a
// pod takes on SIGTERM; the sweeper handles the ungraceful one.
func (r *Registry) Deregister(id scheduler.HostID) error {
	r.mu.Lock()
	_, known := r.lastSeen[id]
	delete(r.lastSeen, id)
	r.mu.Unlock()

	if !known {
		return fmt.Errorf("%w: %s", ErrUnknownHost, id)
	}
	return r.sched.SetHostState(id, scheduler.HostDraining)
}

// Sweep drains hosts that have stopped heartbeating and removes drained hosts
// that are now empty. It returns the number of hosts evicted.
//
// It is exported so tests can drive it directly instead of waiting on a ticker.
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
	}
	r.mu.Unlock()

	for _, id := range expired {
		// Drain rather than delete. If the process is merely wedged its
		// microVMs may still be alive, and yanking the host would orphan them.
		_ = r.sched.SetHostState(id, scheduler.HostDraining)
	}

	r.reapEmptyDrainingHosts()
	return len(expired)
}

// reapEmptyDrainingHosts removes draining hosts once their last microVM exits,
// which is what actually returns capacity to the cluster.
func (r *Registry) reapEmptyDrainingHosts() {
	for _, h := range r.sched.Hosts() {
		if h.State != scheduler.HostDraining || h.Used != 0 {
			continue
		}
		r.mu.Lock()
		_, stillTracked := r.lastSeen[h.ID]
		r.mu.Unlock()
		if stillTracked {
			// Re-registered while draining, so leave it alone.
			continue
		}
		if _, err := r.sched.RemoveHost(h.ID); err != nil {
			continue
		}
	}
}

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

// Tracked returns how many hosts are currently sending heartbeats.
func (r *Registry) Tracked() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.lastSeen)
}
