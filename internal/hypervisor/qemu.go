package hypervisor

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// QEMU runs each microVM as a real QEMU guest: a separate kernel, a separate
// address space, real virtual hardware.
type QEMU struct {
	cfg QEMUConfig

	mu      sync.Mutex
	running map[string]*qemuVM
	closed  bool

	expired chan string
	wg      sync.WaitGroup
}

// QEMUConfig configures the QEMU-backed hypervisor.
type QEMUConfig struct {
	Slots       int
	Binary      string
	KernelImage string
	InitrdImage string
	RootfsImage string
	MemMiB      int
	BootTimeout time.Duration
	ReadyMarker string
	Accel       string
}

// Defaults for the QEMU hypervisor.
const (
	DefaultReadyMarker     = "MICROVM_READY"
	DefaultQEMUBootTimeout = 90 * time.Second
	DefaultGuestMemMiB     = 128
)

type qemuVM struct {
	cmd    *exec.Cmd
	cancel context.CancelFunc
	timer  *time.Timer
	inst   Instance
}

var (
	_ Hypervisor = (*QEMU)(nil)
	_ Reaper     = (*QEMU)(nil)
)

func (c *QEMUConfig) applyDefaults() {
	if c.MemMiB == 0 {
		c.MemMiB = DefaultGuestMemMiB
	}
	if c.BootTimeout == 0 {
		c.BootTimeout = DefaultQEMUBootTimeout
	}
	if c.ReadyMarker == "" {
		c.ReadyMarker = DefaultReadyMarker
	}
	if c.Binary == "" {
		c.Binary = defaultQEMUBinary()
	}
	if c.Accel == "" {
		c.Accel = detectAccel()
	}
}

// defaultQEMUBinary picks the emulator matching the host architecture, since a
// guest kernel built for the host arch is the only one TCG can run at
// tolerable speed.
func defaultQEMUBinary() string {
	switch runtime.GOARCH {
	case "arm64":
		return "qemu-system-aarch64"
	case "amd64":
		return "qemu-system-x86_64"
	default:
		return "qemu-system-" + runtime.GOARCH
	}
}

// detectAccel prefers hardware acceleration when the host offers it, so this
// implementation is not permanently second class.
func detectAccel() string {
	if _, err := os.Stat("/dev/kvm"); err == nil {
		return "kvm"
	}
	return "tcg"
}

// Validate reports whether the configuration is usable.
func (c QEMUConfig) Validate() error {
	if c.Slots < MinSlotsPerHost {
		return fmt.Errorf("qemu: Slots must be at least %d, got %d", MinSlotsPerHost, c.Slots)
	}
	if c.KernelImage == "" {
		return errors.New("qemu: KernelImage is required")
	}
	if c.InitrdImage == "" && c.RootfsImage == "" {
		return errors.New("qemu: one of InitrdImage or RootfsImage is required, or the guest has no userspace to run")
	}
	if c.InitrdImage != "" {
		if _, err := os.Stat(c.InitrdImage); err != nil {
			return fmt.Errorf("qemu: initrd image %q: %w", c.InitrdImage, err)
		}
	}
	if _, err := os.Stat(c.KernelImage); err != nil {
		return fmt.Errorf("qemu: kernel image %q: %w", c.KernelImage, err)
	}
	if c.RootfsImage != "" {
		if _, err := os.Stat(c.RootfsImage); err != nil {
			return fmt.Errorf("qemu: rootfs image %q: %w", c.RootfsImage, err)
		}
	}
	if c.MemMiB < 16 {
		return fmt.Errorf("qemu: MemMiB must be at least 16, got %d", c.MemMiB)
	}
	return nil
}

// MinSlotsPerHost mirrors the scheduler's floor: a pod virtualises several
// microVMs, never one.
const MinSlotsPerHost = 2

// NewQEMU returns a QEMU-backed hypervisor.
func NewQEMU(cfg QEMUConfig) (*QEMU, error) {
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if _, err := exec.LookPath(cfg.Binary); err != nil {
		return nil, fmt.Errorf("qemu: %q not found on PATH: %w", cfg.Binary, err)
	}
	return &QEMU{
		cfg:     cfg,
		running: make(map[string]*qemuVM, cfg.Slots),
		expired: make(chan string, cfg.Slots*4),
	}, nil
}

// Capacity implements Hypervisor.
func (q *QEMU) Capacity() int { return q.cfg.Slots }

// Running implements Hypervisor.
func (q *QEMU) Running() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.running)
}

// Expired implements Reaper.
func (q *QEMU) Expired() <-chan string { return q.expired }

// Accel reports the accelerator in use, for logging and the dashboard.
func (q *QEMU) Accel() string { return q.cfg.Accel }

// Start boots a guest and returns once it has printed the readiness marker.
func (q *QEMU) Start(ctx context.Context, spec Spec) (Instance, error) {
	if err := spec.Validate(); err != nil {
		return Instance{}, err
	}

	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return Instance{}, ErrClosed
	}
	if _, exists := q.running[spec.ID]; exists {
		q.mu.Unlock()
		return Instance{}, fmt.Errorf("%w: %s", ErrDuplicateVM, spec.ID)
	}
	if len(q.running) >= q.cfg.Slots {
		q.mu.Unlock()
		return Instance{}, ErrNoCapacity
	}
	vm := &qemuVM{}
	q.running[spec.ID] = vm
	q.mu.Unlock()

	release := func() {
		q.mu.Lock()
		delete(q.running, spec.ID)
		q.mu.Unlock()
	}

	start := time.Now()
	inst, err := q.boot(ctx, spec, vm)
	if err != nil {
		release()
		return Instance{}, err
	}
	inst.BootLatency = time.Since(start)

	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		vm.cancel()
		return Instance{}, ErrClosed
	}
	vm.inst = inst
	if spec.TTL > 0 {
		q.wg.Add(1)
		vm.timer = time.AfterFunc(spec.TTL, func() {
			defer q.wg.Done()
			q.reap(spec.ID)
		})
	}
	q.mu.Unlock()

	return inst, nil
}

// boot launches QEMU and waits for the guest readiness marker on the console.
func (q *QEMU) boot(ctx context.Context, spec Spec, vm *qemuVM) (Instance, error) {
	vmCtx, cancel := context.WithCancel(context.Background())
	vm.cancel = cancel

	// The binary comes from operator configuration, never from a request, and
	// Spec.Validate has already constrained spec.ID to a character set that
	// cannot inject a kernel parameter. No shell is involved either way.
	// #nosec G204 -- arguments are operator config plus a validated identifier
	cmd := exec.CommandContext(vmCtx, q.cfg.Binary, q.args(spec)...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return Instance{}, fmt.Errorf("%w: stdout pipe: %w", ErrBootFailed, err)
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		cancel()
		return Instance{}, fmt.Errorf("%w: start qemu: %w", ErrBootFailed, err)
	}
	vm.cmd = cmd

	ready := make(chan error, 1)
	go func() { ready <- awaitMarker(stdout, q.cfg.ReadyMarker) }()

	bootCtx, bootCancel := context.WithTimeout(ctx, q.cfg.BootTimeout)
	defer bootCancel()

	select {
	case err := <-ready:
		if err != nil {
			cancel()
			return Instance{}, fmt.Errorf("%w: %s: %w", ErrBootFailed, spec.ID, err)
		}
	case <-bootCtx.Done():
		cancel()
		return Instance{}, fmt.Errorf("%w: %s did not signal ready within %s", ErrBootFailed, spec.ID, q.cfg.BootTimeout)
	}

	now := time.Now()
	inst := Instance{ID: spec.ID, StartedAt: now}
	if spec.TTL > 0 {
		inst.ExpiresAt = now.Add(spec.TTL)
	}
	return inst, nil
}

// args builds the QEMU command line.
func (q *QEMU) args(spec Spec) []string {
	args := []string{
		"-accel", q.cfg.Accel,
		"-m", strconv.Itoa(q.cfg.MemMiB),
		"-smp", strconv.Itoa(spec.VCPUs),
		"-kernel", q.cfg.KernelImage,
		"-nographic",
		"-no-reboot",
		"-nodefaults",
		"-serial", "stdio",
	}

	switch runtime.GOARCH {
	case "arm64":
		args = append(args, "-machine", "virt,highmem=off", "-cpu", "cortex-a72")
	case "amd64":
		args = append(args, "-machine", "microvm,acpi=off,rtc=off", "-cpu", "max")
	}

	switch {
	case q.cfg.InitrdImage != "":
		args = append(args,
			"-initrd", q.cfg.InitrdImage,
			"-append", "console="+consoleDevice()+" loglevel=3 vm_id="+spec.ID,
		)
	case q.cfg.RootfsImage != "":
		args = append(args,
			"-drive", "file="+q.cfg.RootfsImage+",format=raw,if=none,id=root,readonly=on",
			"-device", "virtio-blk-device,drive=root",
			"-append", "console="+consoleDevice()+" root=/dev/vda ro loglevel=3 vm_id="+spec.ID,
		)
	}
	return args
}

// consoleDevice is the serial port the guest kernel logs to, which differs by
// architecture.
func consoleDevice() string {
	if runtime.GOARCH == "arm64" {
		return "ttyAMA0"
	}
	return "ttyS0"
}

// awaitMarker scans guest console output for the readiness marker.
func awaitMarker(r io.Reader, marker string) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), marker) {
			go func() {
				for scanner.Scan() { //nolint:revive // discard remaining output
				}
			}()
			return nil
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return errors.New("guest exited before signalling ready")
}

// Stop terminates a guest and frees its slot.
func (q *QEMU) Stop(_ context.Context, id string) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return ErrClosed
	}
	vm, ok := q.running[id]
	if !ok {
		q.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrUnknownVM, id)
	}
	delete(q.running, id)
	if vm.timer != nil && vm.timer.Stop() {
		q.wg.Done()
	}
	q.mu.Unlock()

	return q.terminate(vm)
}

// terminate kills the QEMU process and reaps it.
func (q *QEMU) terminate(vm *qemuVM) error {
	if vm.cancel != nil {
		vm.cancel()
	}
	if vm.cmd == nil {
		return nil
	}
	_ = vm.cmd.Wait()
	return nil
}

// reap expires a guest whose TTL elapsed and notifies the agent.
func (q *QEMU) reap(id string) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	vm, ok := q.running[id]
	if !ok {
		q.mu.Unlock()
		return
	}
	delete(q.running, id)
	q.mu.Unlock()

	_ = q.terminate(vm)

	select {
	case q.expired <- id:
	default:
	}
}

// Close terminates every guest.
func (q *QEMU) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	q.closed = true
	vms := make([]*qemuVM, 0, len(q.running))
	for id, vm := range q.running {
		if vm.timer != nil && vm.timer.Stop() {
			q.wg.Done()
		}
		vms = append(vms, vm)
		delete(q.running, id)
	}
	q.mu.Unlock()

	for _, vm := range vms {
		_ = q.terminate(vm)
	}
	q.wg.Wait()
	close(q.expired)
	return nil
}

// GuestArtifacts reports where the kernel and rootfs were loaded from, for
// startup logging so a misconfigured image is obvious immediately.
func (q *QEMU) GuestArtifacts() (kernel, rootfs string) {
	kernel = filepath.Clean(q.cfg.KernelImage)
	switch {
	case q.cfg.InitrdImage != "":
		rootfs = filepath.Clean(q.cfg.InitrdImage)
	case q.cfg.RootfsImage != "":
		rootfs = filepath.Clean(q.cfg.RootfsImage)
	}
	return kernel, rootfs
}
