package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/pranav-gupta1/microvm-placement/internal/metrics"
	"github.com/pranav-gupta1/microvm-placement/internal/placement"
	"github.com/pranav-gupta1/microvm-placement/internal/scheduler"
)

// stubPlacer lets a test choose exactly what the placement layer returns, so
// the handler's error mapping can be exercised without building the conditions
// that would provoke each error for real.
type stubPlacer struct {
	result     placement.Result
	admitErr   error
	releaseErr error

	admitted []placement.Request
	released []string
}

func (s *stubPlacer) Admit(_ context.Context, req placement.Request) (placement.Result, error) {
	s.admitted = append(s.admitted, req)
	if s.admitErr != nil {
		return placement.Result{}, s.admitErr
	}
	return s.result, nil
}

func (s *stubPlacer) Release(vmID string) error {
	s.released = append(s.released, vmID)
	return s.releaseErr
}

func (s *stubPlacer) Stats() placement.Stats { return placement.Stats{} }

// stubFleet reports whatever host state a test needs.
type stubFleet struct {
	stats scheduler.Stats
	hosts []scheduler.HostSnapshot
}

func (s *stubFleet) Stats() scheduler.Stats          { return s.stats }
func (s *stubFleet) Hosts() []scheduler.HostSnapshot { return s.hosts }

func newTestServer(t *testing.T, p Placer, f Fleet) (http.Handler, *metrics.Metrics, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	// Discard handler logs: the error paths below log deliberately and would
	// otherwise bury the real test output.
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(p, f, m, reg, log).Routes(), m, reg
}

// counterValue reads a counter out of the registry so assertions can be made
// on what was actually exported, not on internal state.
func counterValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, metric := range f.GetMetric() {
			if !labelsMatch(metric, labels) {
				continue
			}
			return metric.GetCounter().GetValue()
		}
	}
	return 0
}

func labelsMatch(m *dto.Metric, want map[string]string) bool {
	for k, v := range want {
		found := false
		for _, lp := range m.GetLabel() {
			if lp.GetName() == k && lp.GetValue() == v {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func post(t *testing.T, h http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(http.MethodPost, "/v1/vms", nil)
	} else {
		r = httptest.NewRequest(http.MethodPost, "/v1/vms", strings.NewReader(body))
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func readyFleet() *stubFleet {
	return &stubFleet{stats: scheduler.Stats{Hosts: 2, ReadyHosts: 2, Capacity: 16, Used: 3}}
}

func TestCreateVMWithEmptyBodyUsesDefaults(t *testing.T) {
	p := &stubPlacer{result: placement.Result{Host: "vmhost-3", Attempts: 1}}
	h, _, _ := newTestServer(t, p, readyFleet())

	w := post(t, h, "")
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}

	var got CreateVMResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.Host != "vmhost-3" {
		t.Errorf("Host = %q, want vmhost-3", got.Host)
	}
	if got.VCPUs != FixedVCPUs || got.MemoryMiB != FixedMemMiB {
		t.Errorf("shape = %d vCPU / %d MiB, want %d / %d", got.VCPUs, got.MemoryMiB, FixedVCPUs, FixedMemMiB)
	}
	if got.TTLMillis != DefaultTTL.Milliseconds() {
		t.Errorf("TTLMillis = %d, want %d", got.TTLMillis, DefaultTTL.Milliseconds())
	}
	// An empty body must still produce a usable identifier.
	if got.ID == "" {
		t.Error("ID is empty, want a generated identifier")
	}
}

func TestCreateVMHonoursCallerSuppliedID(t *testing.T) {
	p := &stubPlacer{result: placement.Result{Host: "vmhost-1"}}
	h, _, _ := newTestServer(t, p, readyFleet())

	w := post(t, h, `{"id":"my-vm-42","ttl_ms":250}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if len(p.admitted) != 1 || p.admitted[0].VMID != "my-vm-42" {
		t.Fatalf("admitted = %+v, want VMID my-vm-42", p.admitted)
	}
	if p.admitted[0].TTL.Milliseconds() != 250 {
		t.Errorf("TTL = %s, want 250ms", p.admitted[0].TTL)
	}
}

func TestCreateVMRejectsInvalidRequests(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		// The guest shape is fixed. Accepting other shapes would silently
		// invalidate the scheduler's slot accounting.
		{"wrong vcpus", `{"vcpus":4}`},
		{"wrong memory", `{"memory_mib":2048}`},
		{"negative ttl", `{"ttl_ms":-1}`},
		{"ttl above ceiling", `{"ttl_ms":999999999}`},
		{"unknown field", `{"nonsense":true}`},
		{"malformed json", `{`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &stubPlacer{result: placement.Result{Host: "vmhost-1"}}
			h, _, reg := newTestServer(t, p, readyFleet())

			w := post(t, h, tc.body)
			if w.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400: %s", w.Code, w.Body.String())
			}
			if len(p.admitted) != 0 {
				t.Error("an invalid request reached the placement layer")
			}
			// A client error is not a dropped request and must not pollute
			// the metric the objective is graded on.
			for _, reason := range []string{metrics.DropReasonTimeout, metrics.DropReasonQueueFull} {
				if v := counterValue(t, reg, "microvm_requests_dropped_total", map[string]string{"reason": reason}); v != 0 {
					t.Errorf("drop counter %s = %v after a 400, want 0", reason, v)
				}
			}
		})
	}
}

func TestAdmissionFailuresMapTo503AndCountAsDrops(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantReason string
		wantCode   string
	}{
		{"timeout", placement.ErrAdmissionTimeout, metrics.DropReasonTimeout, "admission_timeout"},
		{"queue full", placement.ErrQueueFull, metrics.DropReasonQueueFull, "queue_full"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &stubPlacer{admitErr: tc.err}
			h, _, reg := newTestServer(t, p, readyFleet())

			w := post(t, h, "")
			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503", w.Code)
			}
			// Retry-After tells a well-behaved client to come back rather
			// than hammer, which matters when capacity is already scarce.
			if w.Header().Get("Retry-After") == "" {
				t.Error("missing Retry-After header on a 503")
			}
			var body ErrorResponse
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode error body: %v", err)
			}
			if body.Error != tc.wantCode {
				t.Errorf("error code = %q, want %q", body.Error, tc.wantCode)
			}
			if v := counterValue(t, reg, "microvm_requests_dropped_total", map[string]string{"reason": tc.wantReason}); v != 1 {
				t.Errorf("drop counter %s = %v, want 1", tc.wantReason, v)
			}
		})
	}
}

// TestDuplicateIDIsAConflictNotADrop keeps the drop metric trustworthy. A
// client reusing an identifier is its own mistake, and counting it against the
// zero-drop objective would make the number meaningless.
func TestDuplicateIDIsAConflictNotADrop(t *testing.T) {
	p := &stubPlacer{admitErr: scheduler.ErrDuplicateVM}
	h, _, reg := newTestServer(t, p, readyFleet())

	w := post(t, h, `{"id":"already-there"}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", w.Code)
	}
	for _, reason := range []string{metrics.DropReasonTimeout, metrics.DropReasonQueueFull} {
		if v := counterValue(t, reg, "microvm_requests_dropped_total", map[string]string{"reason": reason}); v != 0 {
			t.Errorf("drop counter %s = %v after a 409, want 0", reason, v)
		}
	}
}

func TestSuccessfulPlacementIncrementsOfferedAndPlaced(t *testing.T) {
	p := &stubPlacer{result: placement.Result{Host: "vmhost-0"}}
	h, _, reg := newTestServer(t, p, readyFleet())

	for i := 0; i < 3; i++ {
		if w := post(t, h, ""); w.Code != http.StatusCreated {
			t.Fatalf("status = %d, want 201", w.Code)
		}
	}
	if v := counterValue(t, reg, "microvm_requests_offered_total", nil); v != 3 {
		t.Errorf("offered = %v, want 3", v)
	}
	if v := counterValue(t, reg, "microvm_requests_placed_total", nil); v != 3 {
		t.Errorf("placed = %v, want 3", v)
	}
}

func TestDeleteVM(t *testing.T) {
	p := &stubPlacer{}
	h, _, _ := newTestServer(t, p, readyFleet())

	r := httptest.NewRequest(http.MethodDelete, "/v1/vms/vm-7", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if len(p.released) != 1 || p.released[0] != "vm-7" {
		t.Errorf("released = %v, want [vm-7]", p.released)
	}
}

func TestDeleteUnknownVMIs404(t *testing.T) {
	p := &stubPlacer{releaseErr: scheduler.ErrUnknownVM}
	h, _, _ := newTestServer(t, p, readyFleet())

	r := httptest.NewRequest(http.MethodDelete, "/v1/vms/ghost", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

// TestHealthzIgnoresFleetState guards a nasty failure mode: if liveness
// depended on capacity, an empty fleet would make Kubernetes restart the very
// component whose job is to ask for more capacity.
func TestHealthzIgnoresFleetState(t *testing.T) {
	h, _, _ := newTestServer(t, &stubPlacer{}, &stubFleet{})

	r := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d with an empty fleet, want 200", w.Code)
	}
}

// TestReadyzDoesNotDependOnFleetCapacity is a regression test for a deadlock
// found on first deployment. vmhost agents register over this same server, so a
// Service that withholds endpoints until hosts exist prevents the registrations
// that would create them, and the rollout never completes.
func TestReadyzReflectsFleetCapacity(t *testing.T) {
	t.Run("ready with an empty fleet", func(t *testing.T) {
		h, _, _ := newTestServer(t, &stubPlacer{}, &stubFleet{stats: scheduler.Stats{Hosts: 3}})
		r := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusOK {
			t.Errorf("status = %d with no ready hosts, want 200: readiness must not gate registration", w.Code)
		}
	})
	t.Run("ready", func(t *testing.T) {
		h, _, _ := newTestServer(t, &stubPlacer{}, readyFleet())
		r := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusOK {
			t.Errorf("status = %d with ready hosts, want 200", w.Code)
		}
	})
}

func TestListHosts(t *testing.T) {
	f := &stubFleet{
		stats: scheduler.Stats{Hosts: 2, ReadyHosts: 1},
		hosts: []scheduler.HostSnapshot{
			{ID: "vmhost-0", Capacity: 8, Used: 3, State: scheduler.HostReady},
			{ID: "vmhost-1", Capacity: 8, Used: 0, State: scheduler.HostDraining},
		},
	}
	h, _, _ := newTestServer(t, &stubPlacer{}, f)

	r := httptest.NewRequest(http.MethodGet, "/v1/hosts", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var body struct {
		Hosts []HostView `json:"hosts"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Hosts) != 2 {
		t.Fatalf("got %d hosts, want 2", len(body.Hosts))
	}
	if body.Hosts[1].State != "Draining" {
		t.Errorf("state = %q, want Draining", body.Hosts[1].State)
	}
}

// TestMetricsEndpointExportsTheKEDASeries pins the contract between this
// service and the autoscaling controller. If this series is renamed or
// disappears, KEDA silently stops scaling, which is a failure that looks like
// a capacity problem rather than a wiring problem.
func TestMetricsEndpointExportsTheKEDASeries(t *testing.T) {
	h, m, _ := newTestServer(t, &stubPlacer{}, readyFleet())
	m.DesiredVMhostReplicas.Set(79)

	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	body := w.Body.String()
	for _, want := range []string{
		"microvm_desired_vmhost_replicas 79",
		"microvm_requests_offered_total",
		"microvm_requests_dropped_total",
		"microvm_vmhosts_idle",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics output is missing %q", want)
		}
	}
}

// TestDropCountersStartAtZero matters for the dashboard. Without pre-created
// label values, a drops panel renders "No data", which is visually identical
// to a broken exporter at exactly the moment you need to trust the zero.
func TestDropCountersStartAtZero(t *testing.T) {
	h, _, _ := newTestServer(t, &stubPlacer{}, readyFleet())

	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	body := w.Body.String()
	for _, want := range []string{
		`microvm_requests_dropped_total{reason="admission_timeout"} 0`,
		`microvm_requests_dropped_total{reason="queue_full"} 0`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics output is missing %q", want)
		}
	}
}
