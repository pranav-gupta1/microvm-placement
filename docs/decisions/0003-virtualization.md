# 3. Three hypervisor implementations, because no free environment runs both

Status: accepted

## Context

The assignment requires each pod to virtualise several real VMs, and separately
requires 1000 requests per second at 100% placement. Meeting both at once needs
hardware virtualisation at scale, which means bare metal: roughly $2.18 per hour
per node and about 12 nodes at peak.

This project has no budget, and the development machine is an Apple M1. Apple's
Virtualization.framework exposes nested virtualisation only on M3 and later, so
no Linux VM on this host can offer `/dev/kvm`. Firecracker requires KVM and has
no software fallback, so it cannot start here at any price.

## Decision

One `hypervisor.Hypervisor` interface, three implementations, and an explicit
statement of what each proves.

| Implementation | Proves | Scale | Requires |
|---|---|---|---|
| `fake` | 1000 rps, 100% placement, zero drops | 500 concurrent | nothing |
| `qemu` | genuinely virtual machines, several per pod | 6 to 12 | any Linux |
| `firecracker` | the production path, snapshot restore | 500+ | `/dev/kvm` |

QEMU falls back to TCG, its binary translator, so guests are real virtual
machines with their own kernels but boot in seconds rather than the tens of
milliseconds a Firecracker snapshot restore takes. Accelerator selection is
automatic, so the identical code path is a fast microVM runner on any host that
does have KVM.

## Why the fake is not a stub

A stub returning success would let every layer above it be written without error
handling, and would make the scale result meaningless. The fake models boot
latency as a distribution, enforces slot capacity, reaps guests on their TTL, and
can be told to fail.

Two behaviours are load bearing and have tests named for them:

- The slot is reserved **before** the boot delay, so 200 concurrent callers
  cannot oversubscribe 8 slots by racing through the boot window.
- A guest stopped before its TTL **suppresses** the expiry notification, so a
  scheduler slot is never freed twice.

## Consequences

The scale number and the real-VM number come from different runs, and that is
stated plainly in the README rather than glossed. A reviewer who discovers a
fake hypervisor presented as real will discount everything; one told the boundary
up front can judge each result on its merits.

If a budget appears, only the Firecracker path changes. The interface is
deliberately small enough to audit in one sitting.
