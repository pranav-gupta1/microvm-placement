# Results

Every number here was produced by a command in this repository. Where a result
was measured under conditions that differ from the assignment's, that is stated
next to the number rather than in a footnote.

## 1. Full rate: 1000 rps, double ramp, 100% placement

`make test` (`internal/sim`). Real arrival process, real admission queue, real
best-fit scheduler, real autoscaler. The fleet is simulated: pods take time to
become ready and drain gracefully when removed.

```
offered            16172
placed             16172  (100.0000%)
dropped            0  (timeout 0, queue full 0)
peak inflight VMs  500
peak ready hosts   104
max queue depth    192
host-seconds       1638.0  (idle 503.0, 30.7%)
admission wait     p50 14.875us  p99 445.9ms  max 1.405s
wall clock         33.7s
```

**Peak concurrency of 500 against a predicted 500.** Little's Law says
`1000 rps x 0.5 s = 500`, and the run produced exactly that. The model and the
implementation agree, which is the single most reassuring number in this
document.

Ramp duration is compressed so the test finishes in half a minute, but the rate
axis is full scale and pod-start latency is scaled in proportion, so the
autoscaler faces the same problem shape.

## 2. On Kubernetes: the same rate, real pods, real HTTP

`make demo`. kind, Karpenter with CapacityBuffer, KEDA, and a load generator
running as a Job inside the cluster.

```
offered            120135
placed             120135  (100.0000%)
dropped (503)      0
transport errors   0
peak in-flight     128
latency            p50 26.583ms  p95 35.7ms  p99 48.301ms  max 353.005ms
duration           3m59s
```

Measured from the cluster side over the same window:

```
peak concurrent microVMs   544
peak ready vmhosts          22
peak slots                 704
dropped                      0
inflight after the run       0
```

544 concurrent microVMs against the 500 Little's Law predicts from 1000 rps and
a 500 ms mean lifetime. The 9% excess is Poisson burstiness, which is exactly
why the target utilisation is 80% rather than 100%.

KEDA moved the fleet in both directions while the ramp ran, driven by the gauge
the placement API exports: 19, 26, 32 climbing, then 24, 18, 21 as demand fell.

### Three failures on the way to this number

**Driving load from the host.** The first full-rate attempt ran the generator
outside the cluster and produced 117,375 transport errors alongside **zero**
server-side drops. That combination is diagnostic: the requests never reached
the service. kind publishes ports through a userland TCP proxy, which saturates
at a thousand new connections a second. Moving the generator inside the cluster
removed it from the path.

**Seventy-nine pods on one node.** At the production shape of 8 slots per pod,
1000 rps needs 79 pods, which is 79 kubelet lifecycles and 79 Prometheus scrape
targets inside a 5 GiB VM. It OOM-killed the Docker daemon. The assignment sets
no maximum microVMs per pod, only a floor of two, so the same 625 slots now come
from 20 pods of 32. The production manifest keeps 8, because on metal the blast
radius of losing a pod matters more than the pod count does.

**The generator starving itself.** A later run reached 99.9992%, one failure in
120,135, with peak in-flight of 1697 against a connection pool of 1024. Roughly
670 requests were queued waiting for a socket inside the generator, not waiting
on the service. Raising the pool to 3072 took p99 from 963 ms to 48 ms and the
last failure disappeared. Worth recording because the symptom looked like a
service problem and was entirely a measurement artefact.

## 3. Real microVMs, three in one pod

`make guest`, then vmhostd with `-hypervisor=qemu`.

```
agent capacity 4, running 3

{"id":"vm-1","boot_latency_us":2587581,"running":3}
{"id":"vm-2","boot_latency_us":2546478,"running":3}
{"id":"vm-3","boot_latency_us":2537070,"running":3}

qemu-system processes inside the pod: 3
  vm_id=vm-1
  vm_id=vm-2
  vm_id=vm-3

guest console:
  MICROVM_READY
  guest kernel: 6.12.103-0-virt
  guest pid1:   1
```

Three genuine virtual machines: separate kernels, separate address spaces,
separate PID 1, three distinct QEMU processes. This satisfies the requirement
that a pod virtualises at least two VMs.

Boot takes **2.5 seconds**, roughly a hundred times slower than a Firecracker
snapshot restore, because no hardware virtualisation is available on the
development machine and QEMU falls back to translating every guest instruction
in software. That is why this demonstration runs at three microVMs and section 1
runs at five hundred. See
[decisions/0003-virtualization.md](decisions/0003-virtualization.md).

## 4. CapacityBuffer reconciling

```
NAME           REPLICAS   STRATEGY
vmhost-floor   4          active-capacity
vmhost-ramp    5          active-capacity

ReadyForProvisioning=True  reason=Resolved
  msg=Pod template resolved successfully
Provisioning=True          reason=FitsExistingCapacity
  msg=All 5 virtual pods fit on existing capacity
```

`vmhost-ramp` is 20% of the 24-replica Deployment, so 4.8 rounded up to 5. The
controller resolved the pod template, computed a target and reported status.

The gate this depends on **defaults to off**:

```
FEATURE_GATES=StaticCapacity=false,SpotToSpotConsolidation=false,NodeRepair=false,CapacityBuffer=true
controller: capacitybuffer  group: autoscaling.x-k8s.io  kind: CapacityBuffer
```

Without `settings.featureGates.capacityBuffer=true` the CRD applies cleanly and
the controller does nothing at all. `deploy/kind/up.sh` asserts on the rendered
environment variable rather than trusting the Helm values, because the failure
mode is silence.

## 5. Hot paths

`make bench`, Apple M1, native arm64.

```
BenchmarkPlaceRelease-8    490.2 ns/op    19 B/op   1 allocs/op
BenchmarkProcessNext-8     213.1 ns/op     0 B/op   0 allocs/op
```

Placement at 490 ns means 1000 requests per second costs about 0.05% of one
core, so the scheduler is nowhere near being the bottleneck. The single
allocation is `fmt.Sprintf` in the benchmark itself, not in the scheduler.

## 6. The cost bug, and the 30% it was worth

The first full-rate run placed everything and cost far too much:

| | Differencing | Least-squares regression |
|---|---|---|
| Peak pods | 198 | **104** |
| Host-seconds | 2340.8 | **1638.0** |
| Idle share | 51.5% | **30.7%** |
| Placement | 100% | 100% |

The autoscaler's lead term differentiates the arrival rate. Arrivals are
Poisson, so a rate measured over a 250 ms window carries about 45 rps of noise
at peak; differencing two such samples produces slope noise near 250 rps/s
against a true ramp slope of 125. Because only a *rising* rate feeds the lead
term, `max(0, slope)` rectified symmetric noise into a systematic
over-provision. The scaler was reacting to ramps that did not exist.

Estimating the slope by least squares over a window cut the compute bill by 30%
with placement unchanged. `TestNoisyFlatLoadDoesNotInflateTheFleet` feeds
worst-case alternating noise around a dead-flat rate so the bias cannot return
unnoticed.

## 7. Bugs found by deploying rather than by reasoning

Recorded because each was invisible in unit tests and obvious within seconds of
running the real thing.

**Scale-down leaked capacity.** When KEDA removed pods, their agents went with
them, so no TTL ever fired for the microVMs those pods held and no expiry was
ever reported. The host sat in Draining with slots occupied, was never reaped,
and its capacity was gone for good. It showed up as inflight microVMs stuck at
63 minutes after a run had finished. Draining hosts whose agent has stopped
heartbeating are now force-reclaimed after a grace period, and
`TestVanishedHostDoesNotLeakSlotsForever` pins the behaviour. Verified on the
cluster: inflight returns to zero after a run.

**Placements never booted.** The first cluster load run placed exactly 191 of
6087 requests. 191 against a fleet of 192 slots was the whole diagnosis: the
scheduler was handing out slots that no agent ever heard about, so no guest
existed, no TTL fired, and nothing was ever released. From the outside this is
indistinguishable from a capacity shortage.

**Readiness deadlocked registration.** Gating `/readyz` on having ready hosts
meant the Service withheld endpoints until hosts existed, which prevented the
registrations that would have created them.

**Registration rejected its own payload.** The agent sends a callback address;
the handler did not declare the field and used `DisallowUnknownFields`, so every
registration failed with 400. The retry-rather-than-crashloop design meant the
agents sat waiting politely instead of falling over, which is the behaviour we
wanted but did make the failure quiet.

## Reproducing

```sh
make test        # sections 1, 5, 6
make demo        # sections 2, 4
make guest       # section 3 artifacts
```
