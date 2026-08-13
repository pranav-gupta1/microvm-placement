// Command vmhostd is the per-pod agent that actually runs microVMs.
//
// It owns a fixed number of slots, boots and reaps guests through a
// hypervisor.Hypervisor, and registers itself with the placement API so the
// scheduler knows the capacity exists. One of these runs per vmhost pod, and
// the pod's slot count is what makes "several microVMs per pod" true.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/pranav-gupta1/microvm-placement/internal/hypervisor"
	"github.com/pranav-gupta1/microvm-placement/internal/metrics"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

type options struct {
	addr              string
	advertiseAddr     string
	hostID            string
	slots             int
	placementAPI      string
	heartbeatInterval time.Duration
	hypervisorKind    string
	bootLatency       time.Duration
	bootJitter        time.Duration
	qemuBinary        string
	kernelImage       string
	initrdImage       string
	rootfsImage       string
	guestMemMiB       int
}

func parseFlags() *options {
	o := &options{}
	flag.StringVar(&o.addr, "addr", ":9090", "listen address")
	flag.StringVar(&o.advertiseAddr, "advertise-addr", "", "address the placement API should call back on, defaults to hostname:port")
	flag.StringVar(&o.hostID, "host-id", "", "stable host identifier, defaults to POD_NAME then hostname")
	flag.IntVar(&o.slots, "slots", 8, "microVM slots, at least 2")
	flag.StringVar(&o.placementAPI, "placement-api", "", "base URL of the placement API, for example http://placement-api:8080. Empty runs standalone")
	flag.DurationVar(&o.heartbeatInterval, "heartbeat-interval", 2*time.Second, "how often to heartbeat")
	flag.StringVar(&o.hypervisorKind, "hypervisor", "fake", "fake, qemu or firecracker")
	flag.DurationVar(&o.bootLatency, "fake-boot-latency", hypervisor.DefaultBootLatency, "modelled boot latency, fake hypervisor only")
	flag.DurationVar(&o.bootJitter, "fake-boot-jitter", hypervisor.DefaultBootJitter, "modelled boot jitter, fake hypervisor only")
	flag.StringVar(&o.qemuBinary, "qemu-binary", "", "path to qemu-system binary, qemu hypervisor only. Auto-detected when empty")
	flag.StringVar(&o.kernelImage, "kernel", "", "guest kernel path, qemu and firecracker only")
	flag.StringVar(&o.initrdImage, "initrd", "", "guest initramfs path, qemu only")
	flag.StringVar(&o.rootfsImage, "rootfs", "", "guest root filesystem path, used instead of an initrd")
	flag.IntVar(&o.guestMemMiB, "guest-mem-mib", 128, "guest memory for real hypervisors. The API accounts 1 GiB per microVM for scheduling regardless")
	flag.Parse()
	return o
}

func run() error {
	o := parseFlags()
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	hostID, err := resolveHostID(o.hostID)
	if err != nil {
		return err
	}

	hv, err := buildHypervisor(o)
	if err != nil {
		return fmt.Errorf("hypervisor: %w", err)
	}
	defer func() { _ = hv.Close() }()

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	agent := &agent{
		hostID:     hostID,
		hypervisor: hv,
		kind:       o.hypervisorKind,
		metrics:    m,
		log:        log,
	}

	// Expired guests must give their slot back to the scheduler, or capacity
	// would leak away one TTL at a time.
	if reaper, ok := hv.(hypervisor.Reaper); ok {
		go agent.watchExpiries(ctx, reaper, o.placementAPI)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /vms", agent.handleStartVM)
	mux.HandleFunc("DELETE /vms/{id}", agent.handleStopVM)
	mux.HandleFunc("GET /healthz", agent.handleHealthz)
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	srv := &http.Server{Addr: o.addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	errs := make(chan error, 1)
	go func() {
		log.Info("vmhostd listening", "addr", o.addr, "host_id", hostID, "slots", o.slots, "hypervisor", o.hypervisorKind)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- err
		}
	}()

	var reg2 *placementClient
	if o.placementAPI != "" {
		reg2 = &placementClient{base: strings.TrimRight(o.placementAPI, "/"), log: log}
		advertise := o.advertiseAddr
		if advertise == "" {
			advertise = defaultAdvertise(hostID, o.addr)
		}
		// Registration is retried rather than fatal: the agent may well start
		// before the placement API is reachable, which is routine during a
		// rollout and not a reason to crashloop.
		go reg2.registerAndHeartbeat(ctx, hostID, o.slots, advertise, o.heartbeatInterval)
	}

	select {
	case err := <-errs:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
		log.Info("shutting down")
	}

	// Deregister first so the scheduler stops sending work here while we still
	// have time to finish what is already running.
	if reg2 != nil {
		reg2.deregister(hostID)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// resolveHostID prefers an explicit flag, then the downward-API pod name, then
// the hostname, which is the pod name anyway under Kubernetes.
func resolveHostID(flagValue string) (string, error) {
	if flagValue != "" {
		return flagValue, nil
	}
	if pod := os.Getenv("POD_NAME"); pod != "" {
		return pod, nil
	}
	name, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("resolve host id: %w", err)
	}
	return name, nil
}

func defaultAdvertise(hostID, addr string) string {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		port = "9090"
	}
	if ip := os.Getenv("POD_IP"); ip != "" {
		return net.JoinHostPort(ip, port)
	}
	return net.JoinHostPort(hostID, port)
}

func buildHypervisor(o *options) (hypervisor.Hypervisor, error) {
	switch o.hypervisorKind {
	case "fake":
		return hypervisor.NewFake(hypervisor.FakeConfig{
			Slots:       o.slots,
			BootLatency: o.bootLatency,
			BootJitter:  o.bootJitter,
		})
	case "qemu":
		return hypervisor.NewQEMU(hypervisor.QEMUConfig{
			Slots:       o.slots,
			Binary:      o.qemuBinary,
			KernelImage: o.kernelImage,
			InitrdImage: o.initrdImage,
			RootfsImage: o.rootfsImage,
			MemMiB:      o.guestMemMiB,
		})
	case "firecracker":
		return nil, errors.New("the firecracker hypervisor requires /dev/kvm, see docs/decisions/0003-virtualization.md")
	default:
		return nil, fmt.Errorf("unknown hypervisor %q, want fake, qemu or firecracker", o.hypervisorKind)
	}
}

// agent serves the boot and stop API that the placement service calls.
type agent struct {
	hostID     string
	hypervisor hypervisor.Hypervisor
	kind       string
	metrics    *metrics.Metrics
	log        *slog.Logger
}

type startVMRequest struct {
	ID        string `json:"id"`
	TTLMillis int64  `json:"ttl_ms"`
}

type startVMResponse struct {
	ID            string `json:"id"`
	Host          string `json:"host"`
	BootLatencyUS int64  `json:"boot_latency_us"`
	Running       int    `json:"running"`
}

func (a *agent) handleStartVM(w http.ResponseWriter, r *http.Request) {
	var req startVMRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	inst, err := a.hypervisor.Start(r.Context(), hypervisor.Spec{
		ID:     req.ID,
		VCPUs:  1,
		MemMiB: 1024,
		TTL:    time.Duration(req.TTLMillis) * time.Millisecond,
	})
	if err != nil {
		a.metrics.VMBootFailures.WithLabelValues(a.kind).Inc()
		status := http.StatusInternalServerError
		if errors.Is(err, hypervisor.ErrNoCapacity) {
			// The scheduler's view of this host was stale. Conflict rather
			// than 500 so the caller knows to place elsewhere and retry.
			status = http.StatusConflict
		}
		a.log.Error("boot failed", "vm_id", req.ID, "error", err)
		http.Error(w, err.Error(), status)
		return
	}

	a.metrics.VMBootLatency.WithLabelValues(a.kind).Observe(inst.BootLatency.Seconds())
	running := a.hypervisor.Running()
	a.metrics.InflightVMs.Set(float64(running))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(startVMResponse{
		ID:            inst.ID,
		Host:          a.hostID,
		BootLatencyUS: inst.BootLatency.Microseconds(),
		Running:       running,
	})
}

func (a *agent) handleStopVM(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := a.hypervisor.Stop(r.Context(), id); err != nil {
		if errors.Is(err, hypervisor.ErrUnknownVM) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	a.metrics.InflightVMs.Set(float64(a.hypervisor.Running()))
	w.WriteHeader(http.StatusNoContent)
}

func (a *agent) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":   "ok",
		"host":     a.hostID,
		"running":  a.hypervisor.Running(),
		"capacity": a.hypervisor.Capacity(),
	})
}

// watchExpiries tells the placement API when a guest reaped itself, so the
// scheduler slot is freed without anyone having to poll for it.
func (a *agent) watchExpiries(ctx context.Context, reaper hypervisor.Reaper, placementAPI string) {
	var client *placementClient
	if placementAPI != "" {
		client = &placementClient{base: strings.TrimRight(placementAPI, "/"), log: a.log}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case id, ok := <-reaper.Expired():
			if !ok {
				return
			}
			a.metrics.InflightVMs.Set(float64(a.hypervisor.Running()))
			if client != nil {
				client.releaseVM(ctx, id)
			}
		}
	}
}

// placementClient is the agent's side of the control plane conversation.
type placementClient struct {
	base string
	log  *slog.Logger
	http http.Client
}

func (c *placementClient) registerAndHeartbeat(ctx context.Context, hostID string, slots int, advertise string, interval time.Duration) {
	registered := false
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if !registered {
			if err := c.register(ctx, hostID, slots, advertise); err != nil {
				c.log.Warn("registration failed, will retry", "error", err)
			} else {
				c.log.Info("registered with placement API", "host", hostID, "advertise", advertise)
				registered = true
			}
		} else if err := c.heartbeat(ctx, hostID); err != nil {
			// A 404 means the placement API forgot us, typically because it
			// restarted. Re-register rather than heartbeat into the void.
			c.log.Warn("heartbeat failed, re-registering", "error", err)
			registered = false
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *placementClient) register(ctx context.Context, hostID string, slots int, advertise string) error {
	body, err := json.Marshal(map[string]any{"id": hostID, "capacity": slots, "address": advertise})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/v1/hosts", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.do(req, http.StatusCreated)
}

func (c *placementClient) heartbeat(ctx context.Context, hostID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/v1/hosts/"+hostID+"/heartbeat", nil)
	if err != nil {
		return err
	}
	return c.do(req, http.StatusNoContent)
}

func (c *placementClient) releaseVM(ctx context.Context, vmID string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.base+"/v1/vms/"+vmID, nil)
	if err != nil {
		return
	}
	// A 404 is fine and common: the placement API may already have released
	// this microVM itself.
	if err := c.do(req, http.StatusNoContent, http.StatusNotFound); err != nil {
		c.log.Warn("failed to report expiry", "vm_id", vmID, "error", err)
	}
}

func (c *placementClient) deregister(hostID string) {
	// Deliberately not tied to the cancelled shutdown context, and given a
	// short independent budget, so a clean drain still gets announced.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.base+"/v1/hosts/"+hostID, nil)
	if err != nil {
		return
	}
	if err := c.do(req, http.StatusNoContent, http.StatusNotFound); err != nil {
		c.log.Warn("deregistration failed", "error", err)
	}
}

func (c *placementClient) do(req *http.Request, okStatuses ...int) error {
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	for _, s := range okStatuses {
		if resp.StatusCode == s {
			return nil
		}
	}
	return fmt.Errorf("%s %s: unexpected status %s", req.Method, req.URL.Path, resp.Status)
}
