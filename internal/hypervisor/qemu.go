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
//
// # Why QEMU here and Firecracker in production
//
// Firecracker requires /dev/kvm and offers no software fallback. KVM needs
// hardware virtualisation, which on a cloud VM means nested virtualisation, and
// on Apple Silicon means an M3 or later host. On an M1 development machine
// there is simply no way to expose it, so Firecracker cannot run at all.
//
// QEMU can, because it falls back to TCG, its portable binary translator. The
// guests are genuinely virtual machines rather than containers, which is what
// the requirement asks for. What they are not is fast: TCG is roughly an order
// of magnitude slower than hardware-accelerated virtualisation, so boots take
// seconds rather than the tens of milliseconds a Firecracker snapshot restore
// takes.
//
// That is the whole trade, stated plainly. This implementation proves microVMs
// are real and that several run per pod. The fake implementation proves the
// control plane survives 1000 requests per second. Only bare metal proves both
// at once, and that costs money this project does not have.
//
// Accel picks hardware acceleration automatically when it is available, so the
// same code path is used unchanged on a host that does have KVM.
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
	// Slots is the microVM capacity of this host, at least 2.
	Slots int
	// Binary is the qemu-system executable. Auto-detected for the host
	// architecture when empty.
	Binary string
	// KernelImage is an uncompressed kernel the guest boots directly. Booting
	// a kernel with -kernel skips firmware and bootloader entirely, which is
	// what makes a guest this small viable at all.
	KernelImage string
	// RootfsImage is an optional root filesystem. When empty the kernel is
	// expected to carry an embedded initramfs.
	RootfsImage string
	// MemMiB is guest memory. This is the real allocation, deliberately much
	// smaller than the 1 GiB the scheduler accounts per microVM, exactly as
	// documented in docs/capacity-planning.md.
	MemMiB int
	// BootTimeout bounds how long to wait for the guest's readiness marker
	// before giving up and killing it.
	BootTimeout time.Duration
	// ReadyMarker is the console string that means the guest is up. Defaults
	// to DefaultReadyMarker.
	ReadyMarker string
	// Accel overrides accelerator selection. Empty means auto-detect.
	Accel string
}

// Defaults for the QEMU hypervisor.
const (
	// DefaultReadyMarker is printed by the guest init once it is running. It
	// is the only reliable signal that the guest reached userspace, as opposed
	// to the QEMU process merely having been spawned.
	DefaultReadyMarker = "MICROVM_READY"
	// DefaultQEMUBootTimeout is generous because TCG is slow, and a guest that
	// has not reached userspace in this long is not going to.
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
// guest kernel built for the host arch is the only one TCG can run at tolerable
// speed.
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
// implementation is not permanently second class. On a KVM-capable host it is
// a genuine microVM runner; elsewhere it degrades to TCG rather than failing.
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
//
// Returning only after the marker is what makes this honest. A QEMU process
// that has been spawned is not a running virtual machine, and reporting success
// at spawn time would let the scheduler count capacity that cannot yet do work.
func (q *QEMU) Start(ctx context.Context, spec Spec) (Instance, error) {
	if err := spec.Validate(); err != nil {
		return Instance{}, err
	}

	// Reserve the slot before the boot, so concurrent callers cannot
	// oversubscribe the host through the boot window.
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
	// The guest's lifetime is owned by this context, not the caller's request
	// context, so a returned HTTP handler does not kill a running microVM.
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
//
// The machine type is the minimal one for the architecture and every optional
// device is switched off. A smaller virtual machine boots faster, which matters
// a great deal when every instruction is being translated in software.
func (q *QEMU) args(spec Spec) []string {
	args := []string{
		"-accel", q.cfg.Accel,
		"-m", strconv.Itoa(q.cfg.MemMiB),
		"-smp", strconv.Itoa(spec.VCPUs),
		"-kernel", q.cfg.KernelImage,
		"-nographic",
		"-no-reboot",
		// No default NIC, disk controller, or serial mouse. Each one is
		// hardware the guest kernel would otherwise probe for and time out on.
		"-nodefaults",
		"-serial", "stdio",
	}

	switch runtime.GOARCH {
	case "arm64":
		// virt is the paravirtual board; without a real CPU model TCG has to
		// guess, and highmem off keeps the address space small.
		args = append(args, "-machine", "virt,highmem=off", "-cpu", "cortex-a72")
	case "amd64":
		args = append(args, "-machine", "microvm,acpi=off,rtc=off", "-cpu", "max")
	}

	if q.cfg.RootfsImage != "" {
		args = append(args,
			"-drive", "file="+q.cfg.RootfsImage+",format=raw,if=none,id=root,readonly=on",
			"-device", "virtio-blk-device,drive=root",
			"-append", "console=ttyAMA0 root=/dev/vda ro quiet vm_id="+spec.ID,
		)
	} else {
		// Embedded initramfs: the kernel carries its own userspace.
		args = append(args, "-append", "console=ttyAMA0 quiet vm_id="+spec.ID)
	}
	return args
}

// awaitMarker scans guest console output for the readiness marker.
func awaitMarker(r io.Reader, marker string) error {
	scanner := bufio.NewScanner(r)
	// Guest consoles can emit long lines, and a truncated line could hide the
	// marker and cause a spurious boot timeout.
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), marker) {
			// Keep draining in the background so a chatty guest cannot block
			// on a full pipe once we stop reading.
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
//
// A microVM has no graceful shutdown worth waiting for: there is no state to
// flush, and the whole point of the model is that guests are disposable.
func (q *QEMU) terminate(vm *qemuVM) error {
	if vm.cancel != nil {
		vm.cancel()
	}
	if vm.cmd == nil {
		return nil
	}
	// Reap the child so it does not linger as a zombie. The error is expected
	// because we just killed it.
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
		// Already stopped explicitly, so no notification: sending one would
		// make the agent free the scheduler slot twice.
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

// Close terminates every guest. It is idempotent.
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
	if q.cfg.RootfsImage != "" {
		rootfs = filepath.Clean(q.cfg.RootfsImage)
	}
	return kernel, rootfs
}
