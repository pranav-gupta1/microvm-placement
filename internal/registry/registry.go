// Package registry tracks which vmhost pods exist and whether they are
// healthy.
package registry

import (
	"errors"
	"fmt"
	"sync"
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

	mu       sync.Mutex
	lastSeen map[scheduler.HostID]time.Time
	address  map[scheduler.HostID]string
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
		address:  make(map[scheduler.HostID]string),
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
