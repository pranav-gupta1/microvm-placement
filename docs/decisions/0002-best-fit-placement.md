# 2. Best-fit placement, because idle pods are what is graded

Status: accepted

## Context

The objective has two halves that pull against each other: place every request,
and minimise idle capacity. The placement policy is the first lever on both.

## Decision

Send each microVM to the fullest host that still has a free slot.

## Alternatives considered

**Worst fit**, spreading load across every host, gives better tail latency under
a hot-spot workload. It is actively wrong here: every host ends up holding a few
microVMs, none ever reaches zero, and nothing is ever reclaimable.

**Round robin** has the same defect with less predictability.

The comparison is measured rather than asserted. Under identical load, 40
microVMs across 20 hosts of 8 slots:

| Policy | Hosts left idle |
|---|---|
| Best fit | 15 |
| Worst fit | 0 |

Worst fit is implemented and kept, purely so that test can run both under the
same harness.

## Consequences

Empty pods are the only thing KEDA can scale away, and the only thing Karpenter
can then consolidate off a node. Packing tightly is therefore what shrinks the
bill, not merely a tidy invariant.

The cost is concentration: losing one host loses 8 microVMs rather than 1. That
is acceptable for work this short-lived, and the admission queue re-places
affected requests rather than dropping them.

Selection is O(1) in host count. Hosts are bucketed by free-slot count, so
picking the fullest is a scan over at most 9 buckets rather than a sort. Measured
at 490 ns per place-and-release pair, roughly 0.05% of one core at peak.

The bucket set is a slice plus an index map rather than a plain map, because Go
randomises map iteration and non-reproducible placement would make the tests
untrustworthy.
