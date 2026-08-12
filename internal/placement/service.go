// Package placement turns an incoming request into a placed microVM.
//
// Its whole reason for existing is the assignment's hard constraint: every
// request must be placed, so the service is not allowed to answer "no". When
// there is no free slot the request waits instead of failing, and the waiting
// is bounded so a caller is never hung indefinitely.
//
// # What the queue is for, and what it is not for
//
// The admission queue absorbs short transients: Poisson clumping in the arrival
// stream, a scheduling hiccup, the few hundred milliseconds between a pod going
// ready and the informer noticing. It is explicitly *not* sized to absorb
// autoscaling lag. Covering the tens of seconds it takes to start a pod, let
// alone the minutes to provision a node, is the job of pre-provisioned capacity
// via CapacityBuffer.
//
// That distinction is worth stating because it makes queue depth a diagnostic.
// A queue that is regularly deep does not mean the queue is too small, it means
// the buffer is. The dashboard plots both for exactly that reason.
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
	// ErrAdmissionTimeout means the request waited the full admission deadline
	// without a slot becoming free. This is the drop the objective forbids, so
	// it is counted separately and alerted on rather than merely logged.
	ErrAdmissionTimeout = errors.New("placement: admission deadline exceeded")
	// ErrQueueFull means the admission queue had no room before the caller's
	// deadline. Like a timeout, this is a drop.
	ErrQueueFull = errors.New("placement: admission queue full")
	// ErrShuttingDown means the service stopped while the request was waiting.
	ErrShuttingDown = errors.New("placement: service is shutting down")
)

// Defaults for the admission path, derived in docs/capacity-planning.md.
//
// DefaultQueueDepth is about one second of arrivals at the 1000 requests per
// second peak. At peak the fleet retires roughly 1000 microVMs per second as
// their TTLs elapse, so a queue this deep drains in about a second even with no
// new capacity at all, while still swallowing any plausible Poisson burst.
//
// DefaultAdmissionDeadline is long enough to ride out a pod that is already
// starting, and short enough that a caller is not left hanging.
const (
	DefaultQueueDepth        = 1024
	DefaultAdmissionDeadline = 3 * time.Second
	DefaultRetryInterval     = 5 * time.Millisecond
)

// Config tunes the admission path.
type Config struct {
	// QueueDepth is the maximum number of requests waiting for a slot.
	QueueDepth int
	// AdmissionDeadline caps how long a single request may wait. A caller
	// whose own context expires sooner wins.
	AdmissionDeadline time.Duration
	// RetryInterval is the fallback poll period. Retries are normally driven
	// by capacity-release signals; this only bounds the wait if a signal is
	// missed, so it is a safety net rather than the primary mechanism.
	RetryInterval time.Duration
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

// Request is a placement request. The assignment fixes the shape of every
// request, so only identity and lifetime vary.
type Request struct {
	// VMID uniquely identifies the microVM to create.
	VMID string
	// TTL is how long the microVM should live.
	TTL time.Duration
}

// Result describes a successful placement.
type Result struct {
	// Host is the vmhost pod the microVM was placed on.
	Host scheduler.HostID
	// Wait is how long the request spent queued before it was placed. At
	// healthy capacity this is microseconds; a rising value is the earliest
	// warning that the fleet is falling behind.
	Wait time.Duration
	// Attempts is how many times placement was tried. More than one means the
	// request had to wait for a slot to free.
	Attempts int
}

// Stats are cumulative counters for metrics and for the end-of-run report.
type Stats struct {
	Accepted        uint64
	Placed          uint64
	TimedOut        uint64
	QueueRejected   uint64
	Released        uint64
	CurrentQueueLen int
	MaxQueueLen     uint64
}

// Dropped is the number of requests that never got a placement. The objective
// requires this to be zero.
func (s Stats) Dropped() uint64 { return s.TimedOut + s.QueueRejected }

// Service admits placement requests and assigns them to hosts.
type Service struct {
	cfg   Config
	sched *scheduler.Scheduler

	queue chan *pending
	// freed carries capacity-release notifications. It is depth 1 and sent to
	// without blocking, so it coalesces: one pending wakeup is enough, and a
	// release never waits on the dispatcher.
	freed chan struct{}

	stopped chan struct{}
	done    chan struct{}

	accepted      atomic.Uint64
	placed        atomic.Uint64
	timedOut      atomic.Uint64
	queueRejected atomic.Uint64
	released      atomic.Uint64
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

// New returns a Service. Start must be called before Admit.
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

// Stop shuts the dispatcher down and waits for it to finish. It is idempotent.
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
//
// It returns ErrQueueFull or ErrAdmissionTimeout only when the request could
// not be placed within the deadline. Both are drops and both are counted.
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
		// Buffered so the dispatcher never blocks handing back a result, even
		// if this caller has already given up.
		result: make(chan admission, 1),
	}

	select {
	case s.queue <- p:
		s.recordQueueDepth()
	case <-ctx.Done():
		// The queue was full for the entire deadline. Backpressure rather than
		// an immediate rejection, which is the point: a momentarily full queue
		// is a reason to wait, not a reason to fail.
		s.queueRejected.Add(1)
		return Result{}, ErrQueueFull
	}

	// Wait unconditionally for the dispatcher's verdict, rather than also
	// selecting on ctx.Done().
	//
	// Racing the context here would be a slot leak: if the deadline fired at
	// the same moment the dispatcher completed a placement, the caller would
	// report a drop while the microVM was in fact placed, and nothing would
	// ever release its slot. The dispatcher already enforces the same deadline
	// and resolves every request it dequeues exactly once, so letting it be the
	// sole authority keeps the accounting honest.
	res := <-p.result
	if res.err != nil {
		return Result{}, res.err
	}
	s.placed.Add(1)
	return Result{Host: res.host, Wait: time.Since(p.enqueued), Attempts: res.attempts}, nil
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

// dispatch resolves a single queued request, retrying until it is placed or its
// deadline expires.
//
// Retrying the head of the queue rather than skipping past it is deliberate.
// Every request has the same shape, 1 vCPU and 1 GiB, so if the head cannot be
// placed then nothing behind it can either. Strict first in, first out costs
// nothing here and gives predictable tail latency instead of the starvation a
// work-stealing scheme would allow.
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

		// No slot right now. Wait for one to be released rather than spinning.
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
		// A wakeup is already pending. Coalescing is correct: the dispatcher
		// re-checks capacity when it wakes, so one signal covers any number of
		// concurrent releases and a release never blocks on the dispatcher.
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
		CurrentQueueLen: len(s.queue),
		MaxQueueLen:     s.maxQueueLen.Load(),
	}
}
