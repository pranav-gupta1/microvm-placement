# Capacity planning and the EC2 quota request

This document derives the size of the fleet from the assignment's stated peak of
1,000 requests per second, and turns that into the exact service quota increase
the AWS account needs. File the quota request first: approval takes days, and it
is the only dependency in the whole build with a multi-day lead time.

## 1. Concurrency, not rate, is what we provision for

The assignment fixes the arrival rate but says nothing about how long a microVM
lives. That gap has to be closed before anything can be sized, because a
persistent VM per request is arithmetically impossible: at 1,000 requests per
second, an hour of traffic would mean 3.6 million live microVMs.

So we state the assumption explicitly rather than hide it:

> **Assumption.** Each placement request creates a microVM with a mean lifetime
> of 500 ms, exponentially distributed. The value is configurable, and the load
> generator emits the TTL on every request so the model is visible in the data,
> not just in this document.

An exponential distribution is the honest choice for a workload of independent,
memoryless tasks (a function invocation, a sandboxed build step, an agent tool
call). It also produces a long tail, so the system has to cope with microVMs
that live many times the mean rather than a tidy fixed duration.

Little's Law converts rate into concurrency:

```
peak_concurrency = peak_rps x mean_ttl
                 = 1000 req/s x 0.5 s
                 = 500 concurrent microVMs
```

Everything below follows from that 500.

## 2. From microVMs to pods

| Knob | Value | Why |
|---|---|---|
| Slots per vmhost pod | 8 | Comfortably clears the assignment's floor of at least two microVMs per pod, while keeping the blast radius of one pod loss at 8 microVMs. |
| Target slot utilisation | 80% | Headroom for Poisson burstiness. Arrivals are not smooth, so provisioning to exactly the mean guarantees transient overflow. |

```
slots_needed = peak_concurrency / target_utilisation
             = 500 / 0.8
             = 625 slots

pods_at_peak = ceil(625 / 8)
             = 79 vmhost pods
```

## 3. From pods to nodes

Each vmhost pod requests 8 vCPU and 10 GiB.

> **Assumption.** We *account* 1 GiB per microVM for scheduling purposes while
> actual guest resident memory is far lower, because Firecracker restores from a
> snapshot and shares page cache. The 10 GiB request is 8 GiB of accounted guest
> memory plus roughly 2 GiB for the supervisor, the jailer, and the snapshot
> page cache. Reserving nominal rather than observed memory is what real clouds
> do, and under-reserving would let a memory spike take down a whole node.

Nominal packing on a `c6g.metal` (64 vCPU, 128 GiB):

```
by CPU:    floor(64 / 8)   = 8 pods
by memory: floor(128 / 10) = 12 pods
                           -> CPU-bound at 8 pods per node
```

In practice kubelet reserves capacity for the system and for DaemonSets (CNI,
kube-proxy, node exporter, the KVM device plugin), so allocatable lands nearer
56 to 58 vCPU:

```
realistic: floor(56 / 8) = 7 pods per node
nodes_at_peak = ceil(79 / 7) = 12 nodes
```

Plan for **12 metal nodes at peak**. The nominal figure of 10 is what the
spreadsheet says; 12 is what the cluster will actually do, and quota is the
wrong place to be optimistic.

## 4. The quota number

Metal instances draw from the ordinary Standard vCPU quota. There is no separate
"metal" quota, a point worth stating because it is a common misconception. The
per-family entries in the AWS quota console (`Running Dedicated c6g Hosts` and
friends) are for **Dedicated Hosts**, which is a different product and not what
Karpenter launches.

```
steady peak         12 nodes x 64 vCPU  = 768 vCPU
CapacityBuffer      ~2 nodes            = 128 vCPU   (pre-provisioned headroom)
consolidation churn ~1 node             =  64 vCPU   (old and new node overlap)
                                        -----------
transient worst case                    = 960 vCPU
```

Round up to **1,024 vCPU**. The control-plane side (EKS managed nodes for
Karpenter, KEDA, Prometheus, and the load generator, roughly 20 to 30 vCPU of
non-metal instances) also draws on the same Standard quota, and is comfortably
inside that rounding.

### What to file

Two separate quotas, both under service `ec2`, both measured in vCPUs, both
**per region**. Spot and On-Demand are tracked independently, so raising one
does nothing for the other.

| Quota name | Code | Default | Request |
|---|---|---|---|
| Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances | `L-1216C47A` | 5 | **1024** |
| All Standard (A, C, D, H, I, M, R, T, Z) Spot Instance Requests | `L-34B43A08` | 5 | **1024** |

Console links, which open directly on the request form:

- <https://console.aws.amazon.com/servicequotas/home/services/ec2/quotas/L-1216C47A>
- <https://console.aws.amazon.com/servicequotas/home/services/ec2/quotas/L-34B43A08>

Or by CLI, substituting the region you intend to run in:

```sh
aws service-quotas request-service-quota-increase \
  --service-code ec2 --quota-code L-1216C47A \
  --desired-value 1024 --region us-east-2

aws service-quotas request-service-quota-increase \
  --service-code ec2 --quota-code L-34B43A08 \
  --desired-value 1024 --region us-east-2
```

Check the current values before and after:

```sh
aws service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A --region us-east-2
aws service-quotas list-requested-service-quota-change-history --service-code ec2 --region us-east-2
```

### Justification text for the request form

AWS approves faster when the case is concrete. Something like:

> Running a Kubernetes cluster that uses Karpenter to autoscale EC2 bare metal
> instances (c6g.metal, m6g.metal). Bare metal is required because the workload
> runs Firecracker microVMs, which need hardware virtualisation via /dev/kvm,
> and KVM is not exposed on non-metal instance types. Peak sizing is 12
> concurrent c6g.metal instances at 64 vCPU each, plus headroom for autoscaler
> churn. Requesting 1024 vCPU for both On-Demand Standard and Spot Standard.

### Region

Pick one region and file against it. `us-east-2` is the recommendation: good
Graviton metal availability with less capacity contention than `us-east-1`.
Once credentials exist, confirm which AZs actually offer the instance types
before committing:

```sh
aws ec2 describe-instance-type-offerings \
  --location-type availability-zone \
  --filters Name=instance-type,Values=c6g.metal,m6g.metal \
  --region us-east-2 --output table
```

## 5. Why bare metal at all

AWS does not expose nested virtualisation on ordinary EC2 instance types, so
`/dev/kvm` is absent and Firecracker cannot start. Bare metal instances run the
OS directly on the hardware, so KVM is available. This is the single constraint
that drives the entire cost profile of the system, and it is why the autoscaling
work matters: metal nodes are expensive and slow to provision, which is exactly
the situation where idle capacity hurts and where a pre-provisioned buffer earns
its keep.

## 6. A note on spot

Spot capacity for `.metal` instance types is thinner than for ordinary sizes,
and interruption during a recorded demo would be unfortunate. The NodePool
requests spot with on-demand fallback, so a spot shortfall degrades cost rather
than availability. For the final recorded run, on-demand is the safer default.

## 7. Architecture and the guest image

`c6g.metal` and `m6g.metal` are Graviton, so `arm64`. `c5.metal` and `m5.metal`
are `x86_64`. A NodePool spanning both families needs the service containers
**and** the Firecracker guest kernel and root filesystem built for both
architectures, or Karpenter will eventually launch a node whose pods cannot run.

The Go services are trivially multi-arch. The guest image is the expensive half,
so this is tracked as a real decision rather than an afterthought. See
`docs/decisions/` once phase 2 settles it. The local development machine is
Apple Silicon, which makes `arm64` the path of least resistance for building and
testing guest artifacts natively.
