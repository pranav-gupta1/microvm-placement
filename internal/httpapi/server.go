// Package httpapi exposes the placement service over HTTP.
//
// The handlers are deliberately thin. All the interesting behaviour, the
// waiting, the retrying, the refusal to drop a request, lives in package
// placement, and this layer's job is only to translate between JSON and that
// service and to be honest about what happened in the status code.
//
// The status code mapping is the part worth reading. A 503 here means a
// request was genuinely lost, which the assignment forbids, so it is never
// returned for transient conditions that the admission queue is designed to
// absorb. If you see one, something is actually wrong.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/pranav-gupta1/microvm-placement/internal/metrics"
	"github.com/pranav-gupta1/microvm-placement/internal/placement"
	"github.com/pranav-gupta1/microvm-placement/internal/scheduler"
)

// Fixed guest shape. The assignment specifies one vCPU and one GiB for every
// request, so these are the only values the API accepts. Rejecting anything
// else keeps the resource accounting in the scheduler trustworthy: a scheduler
// that counts slots cannot also honour heterogeneous guest sizes.
const (
	FixedVCPUs  = 1
	FixedMemMiB = 1024
)

// DefaultTTL is used when a request does not specify one.
const DefaultTTL = 500 * time.Millisecond

// MaxTTL bounds how long a caller may pin a slot. Without a ceiling a single
// client could hold capacity indefinitely and starve everyone else.
const MaxTTL = 5 * time.Minute

// Placer is the subset of placement.Service the handlers need. Narrowing it to
// an interface keeps the handler tests free of a real dispatcher goroutine.
type Placer interface {
	Admit(ctx context.Context, req placement.Request) (placement.Result, error)
	Release(vmID string) error
	Stats() placement.Stats
}

// Fleet reports host state for the readiness probe and the debug endpoint.
type Fleet interface {
	Stats() scheduler.Stats
	Hosts() []scheduler.HostSnapshot
}

// HostRegistry is the membership side of the API, which vmhostd agents drive
// by registering themselves and heartbeating. Optional: when nil, the host
// endpoints are not served, which is what the simulation and the handler tests
// want.
type HostRegistry interface {
	Register(id scheduler.HostID, capacity int) error
	Heartbeat(id scheduler.HostID) error
	Deregister(id scheduler.HostID) error
}

// Server wires the placement service to HTTP.
type Server struct {
	placer  Placer
	fleet   Fleet
	hosts   HostRegistry
	metrics *metrics.Metrics
	log     *slog.Logger
	reg     *prometheus.Registry
}

// New returns a Server.
func New(placer Placer, fleet Fleet, m *metrics.Metrics, reg *prometheus.Registry, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{placer: placer, fleet: fleet, metrics: m, log: log, reg: reg}
}

// WithHostRegistry enables the host membership endpoints.
func (s *Server) WithHostRegistry(hosts HostRegistry) *Server {
	s.hosts = hosts
	return s
}

// Routes returns the HTTP handler for the whole API.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/vms", s.handleCreateVM)
	mux.HandleFunc("DELETE /v1/vms/{id}", s.handleDeleteVM)
	mux.HandleFunc("GET /v1/hosts", s.handleListHosts)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	if s.hosts != nil {
		mux.HandleFunc("POST /v1/hosts", s.handleRegisterHost)
		mux.HandleFunc("POST /v1/hosts/{id}/heartbeat", s.handleHeartbeatHost)
		mux.HandleFunc("DELETE /v1/hosts/{id}", s.handleDeregisterHost)
	}
	if s.reg != nil {
		mux.Handle("GET /metrics", promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{}))
	}
	return mux
}

// RegisterHostRequest is the POST /v1/hosts body, sent by a vmhostd on startup.
type RegisterHostRequest struct {
	// ID is the pod name in the cluster, or any stable identifier locally.
	ID string `json:"id"`
	// Capacity is how many microVM slots the agent is offering. It must be at
	// least scheduler.MinSlotsPerHost.
	Capacity int `json:"capacity"`
}

func (s *Server) handleRegisterHost(w http.ResponseWriter, r *http.Request) {
	var req RegisterHostRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	if err := s.hosts.Register(scheduler.HostID(req.ID), req.Capacity); err != nil {
		if errors.Is(err, scheduler.ErrInvalidCapacity) {
			s.writeError(w, http.StatusBadRequest, "invalid_capacity", err.Error())
			return
		}
		s.writeError(w, http.StatusBadRequest, "registration_failed", err.Error())
		return
	}
	s.log.Info("vmhost registered", "host", req.ID, "capacity", req.Capacity)
	s.writeJSON(w, http.StatusCreated, map[string]any{"id": req.ID, "capacity": req.Capacity})
}

func (s *Server) handleHeartbeatHost(w http.ResponseWriter, r *http.Request) {
	if err := s.hosts.Heartbeat(scheduler.HostID(r.PathValue("id"))); err != nil {
		// A heartbeat from a host we have forgotten, for example after the
		// placement API restarted. 404 tells the agent to register again
		// rather than keep heartbeating into the void.
		s.writeError(w, http.StatusNotFound, "unknown_host", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleDeregisterHost(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.hosts.Deregister(scheduler.HostID(id)); err != nil {
		s.writeError(w, http.StatusNotFound, "unknown_host", err.Error())
		return
	}
	s.log.Info("vmhost deregistered", "host", id)
	w.WriteHeader(http.StatusNoContent)
}

// CreateVMRequest is the POST /v1/vms body.
type CreateVMRequest struct {
	// VCPUs must be FixedVCPUs when present.
	VCPUs int `json:"vcpus,omitempty"`
	// MemoryMiB must be FixedMemMiB when present.
	MemoryMiB int `json:"memory_mib,omitempty"`
	// TTLMillis is the requested microVM lifetime. Defaults to DefaultTTL.
	TTLMillis int64 `json:"ttl_ms,omitempty"`
	// ID lets a caller supply its own identifier for idempotency. Generated
	// when empty.
	ID string `json:"id,omitempty"`
}

// CreateVMResponse is the 201 body.
type CreateVMResponse struct {
	ID          string `json:"id"`
	Host        string `json:"host"`
	VCPUs       int    `json:"vcpus"`
	MemoryMiB   int    `json:"memory_mib"`
	TTLMillis   int64  `json:"ttl_ms"`
	QueueWaitUS int64  `json:"queue_wait_us"`
	Attempts    int    `json:"attempts"`
}

// ErrorResponse is the body of every non-2xx reply.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func (s *Server) handleCreateVM(w http.ResponseWriter, r *http.Request) {
	var req CreateVMRequest
	// An empty body is valid and means "use the defaults", which keeps the
	// load generator's hot path free of JSON encoding.
	if r.ContentLength != 0 {
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			s.writeError(w, http.StatusBadRequest, "invalid_body", err.Error())
			return
		}
	}

	ttl, err := validate(req)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	id := req.ID
	if id == "" {
		id = uuid.NewString()
	}

	s.metrics.RequestsOffered.Inc()
	start := time.Now()

	res, err := s.placer.Admit(r.Context(), placement.Request{VMID: id, TTL: ttl})
	if err != nil {
		s.writeAdmissionError(w, id, err)
		return
	}

	s.metrics.RequestsPlaced.Inc()
	s.metrics.PlacementLatency.Observe(time.Since(start).Seconds())

	s.writeJSON(w, http.StatusCreated, CreateVMResponse{
		ID:          id,
		Host:        string(res.Host),
		VCPUs:       FixedVCPUs,
		MemoryMiB:   FixedMemMiB,
		TTLMillis:   ttl.Milliseconds(),
		QueueWaitUS: res.Wait.Microseconds(),
		Attempts:    res.Attempts,
	})
}

// validate checks the fixed guest shape and returns the effective TTL.
func validate(req CreateVMRequest) (time.Duration, error) {
	if req.VCPUs != 0 && req.VCPUs != FixedVCPUs {
		return 0, fmt.Errorf("vcpus must be %d, got %d", FixedVCPUs, req.VCPUs)
	}
	if req.MemoryMiB != 0 && req.MemoryMiB != FixedMemMiB {
		return 0, fmt.Errorf("memory_mib must be %d, got %d", FixedMemMiB, req.MemoryMiB)
	}
	if req.TTLMillis < 0 {
		return 0, fmt.Errorf("ttl_ms must not be negative, got %d", req.TTLMillis)
	}
	ttl := DefaultTTL
	if req.TTLMillis > 0 {
		ttl = time.Duration(req.TTLMillis) * time.Millisecond
	}
	if ttl > MaxTTL {
		return 0, fmt.Errorf("ttl_ms must not exceed %d, got %d", MaxTTL.Milliseconds(), req.TTLMillis)
	}
	return ttl, nil
}

// writeAdmissionError maps a placement failure to a status code.
//
// Both queue-full and timeout become 503 with Retry-After, because from the
// caller's perspective they are the same thing: capacity did not arrive in
// time. They are counted separately so the operator can tell them apart, since
// a full queue and an empty fleet call for different fixes.
func (s *Server) writeAdmissionError(w http.ResponseWriter, id string, err error) {
	switch {
	case errors.Is(err, placement.ErrAdmissionTimeout):
		s.metrics.RequestsDropped.WithLabelValues(metrics.DropReasonTimeout).Inc()
		s.log.Error("dropped request: admission deadline exceeded", "vm_id", id)
		w.Header().Set("Retry-After", "1")
		s.writeError(w, http.StatusServiceUnavailable, "admission_timeout",
			"no capacity became available within the admission deadline")

	case errors.Is(err, placement.ErrQueueFull):
		s.metrics.RequestsDropped.WithLabelValues(metrics.DropReasonQueueFull).Inc()
		s.log.Error("dropped request: admission queue full", "vm_id", id)
		w.Header().Set("Retry-After", "1")
		s.writeError(w, http.StatusServiceUnavailable, "queue_full",
			"the admission queue was full for the whole deadline")

	case errors.Is(err, placement.ErrShuttingDown):
		w.Header().Set("Retry-After", "5")
		s.writeError(w, http.StatusServiceUnavailable, "shutting_down", "the service is shutting down")

	case errors.Is(err, scheduler.ErrDuplicateVM):
		// A caller-supplied ID that is already placed. This is the client's
		// mistake, not ours, so it must not be counted as a drop.
		s.writeError(w, http.StatusConflict, "duplicate_id", "a microVM with this id is already placed")

	default:
		s.log.Error("placement failed", "vm_id", id, "error", err)
		s.writeError(w, http.StatusInternalServerError, "internal", "placement failed")
	}
}

func (s *Server) handleDeleteVM(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		s.writeError(w, http.StatusBadRequest, "invalid_request", "missing id")
		return
	}
	if err := s.placer.Release(id); err != nil {
		if errors.Is(err, scheduler.ErrUnknownVM) {
			s.writeError(w, http.StatusNotFound, "not_found", "no such microVM")
			return
		}
		s.log.Error("release failed", "vm_id", id, "error", err)
		s.writeError(w, http.StatusInternalServerError, "internal", "release failed")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// HostView is one row of GET /v1/hosts.
type HostView struct {
	ID       string `json:"id"`
	State    string `json:"state"`
	Capacity int    `json:"capacity"`
	Used     int    `json:"used"`
}

func (s *Server) handleListHosts(w http.ResponseWriter, _ *http.Request) {
	snaps := s.fleet.Hosts()
	out := make([]HostView, 0, len(snaps))
	for _, h := range snaps {
		out = append(out, HostView{
			ID:       string(h.ID),
			State:    h.State.String(),
			Capacity: h.Capacity,
			Used:     h.Used,
		})
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"hosts": out})
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	// Liveness only asks whether the process is functioning. It must not
	// depend on fleet capacity, or an empty fleet would cause Kubernetes to
	// restart the very component responsible for asking for more of it.
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	stats := s.fleet.Stats()
	if stats.ReadyHosts == 0 {
		// No host can serve a placement yet, so taking traffic would only
		// build queue depth.
		s.writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "no ready vmhosts",
			"hosts":  stats.Hosts,
		})
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"status":      "ready",
		"ready_hosts": stats.ReadyHosts,
		"slots_free":  stats.Capacity - stats.Used,
	})
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// The status line is already sent, so this can only be logged.
		s.log.Error("failed to encode response", "error", err)
	}
}

func (s *Server) writeError(w http.ResponseWriter, status int, code, msg string) {
	s.writeJSON(w, status, ErrorResponse{Error: code, Message: msg})
}
