# microvm-placement

A placement service for Firecracker-style microVMs, with autoscaling driven by
Karpenter's CapacityBuffer API and KEDA. Every request gets placed, and idle
capacity is given back as soon as it is safe to.

```
POST /v1/vms  ->  admission queue  ->  best-fit scheduler  ->  vmhostd  ->  microVM
                        |                     |
                        |                     +-- exports desired replica count
                        |                         |
                        +-- never drops           +-- KEDA scales the Deployment
                            waits instead             |
                                                      +-- Karpenter provisions nodes
                                                          with CapacityBuffer headroom
```

## Results

Full 1000 requests per second double ramp, in process, against the real
scheduler, admission queue and autoscaler:

```
offered            16172
placed             16172  (100.0000%)
dropped            0
peak inflight VMs  500
peak ready hosts   104
host-seconds       1638.0  (idle 503.0, 30.7%)
admission wait     p50 14.9us  p99 445.9ms
```

The same system deployed on Kubernetes, over real HTTP, real pods, real
registration:

```
offered            6087
placed             6087  (100.0000%)
dropped (503)      0
latency            p50 26.7ms  p95 35.6ms  p99 36.8ms
```

Peak concurrency landing on 500 is a useful independent check: Little's Law
predicts exactly 500 from 1000 requests per second at a 500 ms mean lifetime.

## Two assumptions, stated up front

The assignment leaves two things open that have to be closed before anything can
be sized. Both are stated here rather than buried.

**microVM lifetime is 500 ms, exponentially distributed.** The assignment fixes
the arrival rate but not how long a microVM lives, and without a lifetime there
is no concurrency to provision for. A persistent VM per request is
arithmetically impossible: an hour at 1000 requests per second would mean 3.6
million live microVMs. Everything follows from Little's Law:

```
peak_concurrency = peak_rps x mean_ttl = 1000 x 0.5 = 500 concurrent microVMs
```

**Memory is accounted at 1 GiB per microVM, not measured.** Real guest resident
memory is far lower, because guests restore from a snapshot and share page
cache. Reserving nominal rather than observed memory is what real clouds do, and
under-reserving would let a memory spike take down a whole node.

Full derivation, including the EC2 quota arithmetic, is in
[docs/capacity-planning.md](docs/capacity-planning.md).

## Virtualization, and what is real where

The honest answer is that no single free environment runs both real microVMs and
1000 requests per second, so the proof is split and the split is visible in the
code as three implementations of one interface.

| Implementation | Proves | Scale | Runs on |
|---|---|---|---|
| `fake` | 1000 rps, 100% placement, zero drops | 500 concurrent | anywhere |
| `qemu` | genuinely virtual machines, several per pod | 6 to 12 | any Linux host |
| `firecracker` | the production path, snapshot restore | 500+ | bare metal only |

Firecracker requires `/dev/kvm` and has no software fallback. KVM needs hardware
virtualization, which on a cloud instance means bare metal and on Apple Silicon
means an M3 or later. On the M1 this was developed on there is no way to expose
it at any price. QEMU can still run real guests by falling back to TCG, its
binary translator, which is roughly an order of magnitude slower: boots take
seconds rather than the tens of milliseconds a Firecracker snapshot restore
takes.

So the fake hypervisor carries the scale proof and QEMU carries the real-VM
proof. The fake is not a stub that returns success: it models boot latency as a
distribution, enforces slot capacity, reaps guests on their TTL, and can be told
to fail. See [docs/decisions/0003-virtualization.md](docs/decisions/).

## How it stays at 100%

**The admission queue waits instead of failing.** When no slot is free the
request is queued with a hard deadline rather than rejected. The queue is sized
to absorb short transients, Poisson clumping and scheduling hiccups, and
explicitly *not* to absorb autoscaling lag. That makes queue depth a diagnostic:
a persistently deep queue means the pre-provisioned buffer is undersized, not
the queue.

**The autoscaler targets future demand, not past load.** Scaling on current
utilization is always late, because by the time utilization rises the traffic
has already arrived and a new node takes minutes:

```
predicted_rate = arrival_rate + d(arrival_rate)/dt * provision_latency
desired_pods   = ceil(max(inflight, predicted_rate * ttl)
                      / target_utilisation / slots_per_pod)
```

The derivative term is a lead compensator that buys back the time the
infrastructure cannot. It is estimated by least squares over a window rather
than by differencing consecutive samples, which is not cosmetic: arrivals are
Poisson, so a rate measured over a short interval is noisy, and because only a
*rising* rate feeds the lead term, clamping at zero turns symmetric noise into
systematic over-provisioning. Differencing inflated the fleet from 79 pods to
198 and left half the compute bill idle. Regression cut host-seconds 30% with
placement unchanged at 100%.

**CapacityBuffer keeps nodes warm ahead of the pods.** KEDA can scale the
Deployment instantly, but a pod is useless until a node exists. Two buffers,
because the CRD permits exactly one selector each: a percentage against the live
Deployment so headroom grows with the fleet, and a fixed floor against a
PodTemplate for the cold start at t=0.

## How it minimises idle pods

Best-fit placement sends each microVM to the fullest host that still has room.
Load concentrates, and the remaining hosts drain to empty as their guests
expire. Empty pods are the only thing KEDA can remove and the only thing
Karpenter can then consolidate off a node, so packing tightly is directly what
shrinks the bill.

Worst-fit is implemented too, purely so the comparison can be measured under the
same harness. Under identical load, 40 microVMs across 20 hosts of 8 slots,
best-fit leaves 15 hosts idle and worst-fit leaves none. Spreading strands every
host at partial occupancy where nothing is ever reclaimable.

Scale-down is deliberately slower than scale-up, because the two mistakes are
not symmetric: over-scaling costs money, under-scaling drops requests. The
trough between the two load cycles is exactly where a naive controller scales to
zero and cannot recover in time for the second climb.

## Running it

Requires Go 1.25+, Docker, kind, kubectl and helm. On Apple Silicon they must be
`arm64` builds; Intel Homebrew under Rosetta will emulate an x86 VM and
Kubernetes will fail its own health timeouts.

```sh
make test          # unit tests with the race detector
make demo          # kind cluster, Karpenter, the app, and a load run
make bench         # scheduler and arrival-process hot paths
```

The full 1000 rps proof runs in CI on every push. It is skipped only under
`-short`.

## Layout

```
cmd/{placement-api,vmhostd,loadgen}   binaries
internal/
  scheduler/    best-fit placement, pure, 94% covered
  placement/    admission queue, the never-drop guarantee
  autoscale/    the replica-count policy, 100% covered
  loadgen/      arrival process, non-homogeneous Poisson
  hypervisor/   interface + fake + qemu
  registry/     host membership and liveness
  httpapi/      HTTP surface
  sim/          end-to-end run against a simulated fleet
deploy/         kind, Karpenter, KEDA, app manifests
docs/           capacity planning, decisions, results
```

## Licence

MIT. See [LICENSE](LICENSE).
