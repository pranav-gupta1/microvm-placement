// Package placement turns an incoming request into a placed microVM.
package placement

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pranav-gupta1/microvm-placement/internal/scheduler"
)

// Errors returned by the service.
var (
	ErrAdmissionTimeout = errors.New("placement: admission deadline exceeded")
	ErrQueueFull        = errors.New("placement: admission queue full")
	ErrShuttingDown     = errors.New("placement: service is shutting down")
	ErrBootFailed       = errors.New("placement: microVM failed to boot")
)

// Defaults for the admission path, derived in docs/capacity-planning.md.
const (
	DefaultQueueDepth        = 1024
	DefaultAdmissionDeadline = 3 * time.Second
	DefaultRetryInterval     = 5 * time.Millisecond
)

// Config tunes the admission path.
type Config struct {
	QueueDepth        int
	AdmissionDeadline time.Duration
	RetryInterval     time.Duration
}

func (c *Config) applyDefaults() {
	if c.QueueDepth == 0 {
		c.QueueDepth = DefaultQueueDepth
	}
	if c.AdmissionDeadline == 0 {
		c.AdmissionDeadline = DefaultAdmissionDeadline
	}
	if c.RetryInterval == 0 {
		c.RetryInterval = DefaultRetryInterval
	}
}

// Validate reports whether the configuration is usable.
func (c Config) Validate() error {
	if c.QueueDepth < 1 {
		return fmt.Errorf("placement: QueueDepth must be positive, got %d", c.QueueDepth)
	}
	if c.AdmissionDeadline <= 0 {
		return fmt.Errorf("placement: AdmissionDeadline must be positive, got %s", c.AdmissionDeadline)
	}
	if c.RetryInterval <= 0 {
		return fmt.Errorf("placement: RetryInterval must be positive, got %s", c.RetryInterval)
	}
	return nil
}

// Request is a placement request.
type Request struct {
	VMID string
	TTL  time.Duration
}

// Result describes a successful placement.
type Result struct {
	Host     scheduler.HostID
	Wait     time.Duration
	Attempts int
}

// Stats are cumulative counters for metrics and for the end-of-run report.
type Stats struct {
	Accepted        uint64
	Placed          uint64
	TimedOut        uint64
	QueueRejected   uint64
	Released        uint64
	BootFailed      uint64
	CurrentQueueLen int
	MaxQueueLen     uint64
}

// Dropped is the number of requests that never got a placement.
func (s Stats) Dropped() uint64 { return s.TimedOut + s.QueueRejected + s.BootFailed }

// Booter starts a microVM on the host the scheduler chose.
//
// It is separate from the scheduler because placing and booting fail in
// different ways and at different speeds: placement is a bookkeeping decision
// costing hundreds of nanoseconds, while booting is a network call to another
// pod that may be slow, may fail, and may reveal that the scheduler's view of
// that host was stale.
type Booter interface {
	Boot(ctx context.Context, host scheduler.HostID, vmID string, ttl time.Duration) error
}

// Service admits placement requests and assigns them to hosts.
type Service struct {
	cfg   Config
	sched *scheduler.Scheduler

	booter Booter

	queue chan *pending
	freed chan struct{}

	stopped chan struct{}
	done    chan struct{}

	accepted      atomic.Uint64
	placed        atomic.Uint64
	timedOut      atomic.Uint64
	queueRejected atomic.Uint64
	released      atomic.Uint64
	bootFailed    atomic.Uint64
	maxQueueLen   atomic.Uint64
}

// pending is a request waiting in the admission queue.
type pending struct {
	req      Request
	ctx      context.Context
	enqueued time.Time
	result   chan admission
}

type admission struct {
	host     scheduler.HostID
	attempts int
	err      error
}

// New returns a Service.
func New(sched *scheduler.Scheduler, cfg Config) (*Service, error) {
	if sched == nil {
		return nil, errors.New("placement: scheduler must not be nil")
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Service{
		cfg:     cfg,
		sched:   sched,
		queue:   make(chan *pending, cfg.QueueDepth),
		freed:   make(chan struct{}, 1),
		stopped: make(chan struct{}),
		done:    make(chan struct{}),
	}, nil
}

// WithBooter attaches a Booter.
func (s *Service) WithBooter(b Booter) *Service {
	s.booter = b
	return s
}

// maxBootAttempts bounds how many hosts a single request will try before
// giving up.
const maxBootAttempts = 3

// Start runs the dispatcher until ctx is cancelled or Stop is called.
//
// A single dispatcher is enough. Placement costs a few hundred nanoseconds, so
// one goroutine sustains millions of admissions per second, far above the 1000
// per second peak, and serialising admission is what makes the queue strictly
// first in, first out.
func (s *Service) Start(ctx context.Context) {
	go func() {
		defer close(s.done)
		for {
			select {
			case <-ctx.Done():
				s.drain(ErrShuttingDown)
				return
			case <-s.stopped:
				s.drain(ErrShuttingDown)
				return
			case p := <-s.queue:
				s.dispatch(ctx, p)
			}
		}
	}()
}

// Stop shuts the dispatcher down and waits for it to finish.
func (s *Service) Stop() {
	select {
	case <-s.stopped:
	default:
		close(s.stopped)
	}
	<-s.done
}

// drain fails every queued request during shutdown rather than leaving callers
// blocked on a channel nobody will ever write to.
func (s *Service) drain(err error) {
	for {
		select {
		case p := <-s.queue:
			p.result <- admission{err: err}
		default:
			return
		}
	}
}

// Admit places a microVM, waiting for capacity if necessary.
func (s *Service) Admit(ctx context.Context, req Request) (Result, error) {
	if req.VMID == "" {
		return Result{}, errors.New("placement: VMID must not be empty")
	}
	s.accepted.Add(1)

	ctx, cancel := context.WithTimeout(ctx, s.cfg.AdmissionDeadline)
	defer cancel()

	p := &pending{
		req:      req,
		ctx:      ctx,
		enqueued: time.Now(),
		result:   make(chan admission, 1),
	}

	select {
	case s.queue <- p:
		s.recordQueueDepth()
	case <-ctx.Done():
		s.queueRejected.Add(1)
		return Result{}, ErrQueueFull
	}

	res := <-p.result
	if res.err != nil {
		return Result{}, res.err
	}

	host := res.host
	attempts := res.attempts
	if s.booter != nil {
		var err error
		host, attempts, err = s.bootWithRetry(ctx, p, host, attempts)
		if err != nil {
			return Result{}, err
		}
	}

	s.placed.Add(1)
	return Result{Host: host, Wait: time.Since(p.enqueued), Attempts: attempts}, nil
}

// bootWithRetry starts the guest, re-placing onto another host if the chosen
// one turns out to be full.
func (s *Service) bootWithRetry(ctx context.Context, p *pending, host scheduler.HostID, attempts int) (scheduler.HostID, int, error) {
	for i := 0; ; i++ {
		err := s.booter.Boot(ctx, host, p.req.VMID, p.req.TTL)
		if err == nil {
			return host, attempts, nil
		}

		_ = s.sched.Release(scheduler.VMID(p.req.VMID))
		s.signalCapacity()

		if i+1 >= maxBootAttempts || p.ctx.Err() != nil {
			s.bootFailed.Add(1)
			return "", attempts, fmt.Errorf("%w: %w", ErrBootFailed, err)
		}

		next, perr := s.sched.Place(scheduler.VMID(p.req.VMID))
		if perr != nil {
			s.bootFailed.Add(1)
			return "", attempts, fmt.Errorf("%w: %w", ErrBootFailed, err)
		}
		host = next
		attempts++
	}
}

// recordQueueDepth tracks the high-water mark, which tells us after a run
// whether the buffer was doing its job.
func (s *Service) recordQueueDepth() {
	depth := uint64(len(s.queue))
	for {
		current := s.maxQueueLen.Load()
		if depth <= current || s.maxQueueLen.CompareAndSwap(current, depth) {
			return
		}
	}
}

// dispatch resolves a single queued request, retrying until it is placed or
// its deadline expires.
func (s *Service) dispatch(ctx context.Context, p *pending) {
	attempts := 0
	for {
		if err := p.ctx.Err(); err != nil {
			s.timedOut.Add(1)
			p.result <- admission{err: ErrAdmissionTimeout}
			return
		}

		attempts++
		host, err := s.sched.Place(scheduler.VMID(p.req.VMID))
		if err == nil {
			p.result <- admission{host: host, attempts: attempts}
			return
		}
		if !errors.Is(err, scheduler.ErrNoCapacity) {
			p.result <- admission{err: err, attempts: attempts}
			return
		}

		timer := time.NewTimer(s.cfg.RetryInterval)
		select {
		case <-s.freed:
			timer.Stop()
		case <-timer.C:
		case <-p.ctx.Done():
			timer.Stop()
			s.timedOut.Add(1)
			p.result <- admission{err: ErrAdmissionTimeout}
			return
		case <-ctx.Done():
			timer.Stop()
			p.result <- admission{err: ErrShuttingDown}
			return
		case <-s.stopped:
			timer.Stop()
			p.result <- admission{err: ErrShuttingDown}
			return
		}
	}
}

// Release frees the slot held by a microVM and wakes a waiting request.
func (s *Service) Release(vmID string) error {
	if err := s.sched.Release(scheduler.VMID(vmID)); err != nil {
		return err
	}
	s.released.Add(1)
	s.signalCapacity()
	return nil
}

// SignalCapacity tells the dispatcher that capacity may have appeared, for
// reasons other than a release: a new host going ready, for example.
func (s *Service) SignalCapacity() { s.signalCapacity() }

func (s *Service) signalCapacity() {
	select {
	case s.freed <- struct{}{}:
	default:
	}
}

// Stats returns cumulative counters.
func (s *Service) Stats() Stats {
	return Stats{
		Accepted:        s.accepted.Load(),
		Placed:          s.placed.Load(),
		TimedOut:        s.timedOut.Load(),
		QueueRejected:   s.queueRejected.Load(),
		Released:        s.released.Load(),
		BootFailed:      s.bootFailed.Load(),
		CurrentQueueLen: len(s.queue),
		MaxQueueLen:     s.maxQueueLen.Load(),
	}
}
