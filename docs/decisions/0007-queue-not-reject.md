# 7. The admission queue waits instead of returning 503

Status: accepted

## Context

The objective forbids dropping requests. A service that returns 503 the moment
no slot is free fails immediately, because autoscaling always lags demand by at
least the time it takes to start a pod.

## Decision

When no slot is free, the request is queued with a hard deadline rather than
rejected. It is placed as soon as capacity appears. Only if the deadline expires
is a 503 returned, and that is counted as a drop and alerted on.

Defaults: 1024 queue slots, 3 second deadline.

## What the queue is for, and what it is not for

The queue absorbs **short transients**: Poisson clumping, a scheduling hiccup,
the gap between a pod going ready and registration completing.

It is explicitly **not** sized to absorb autoscaling lag. Covering the tens of
seconds it takes to start a pod, let alone the minutes to provision a node, is
CapacityBuffer's job.

That distinction turns queue depth into a diagnostic rather than just a limit. A
persistently deep queue means the buffer is undersized, not the queue. The
dashboard plots both for exactly that reason.

Depth is derived, not guessed: 1024 is about one second of arrivals at peak, and
at peak the fleet retires roughly 1000 microVMs per second as TTLs elapse, so a
full queue drains in about a second even with no new capacity at all.

## Two implementation details that carry the guarantee

**`Admit` waits unconditionally on the dispatcher's verdict** rather than also
racing the caller's context. Selecting on both would leak slots: if the deadline
fired at the same instant a placement completed, the caller would report a drop
while the microVM was in fact placed, and nothing would ever release it. The
dispatcher enforces the same deadline and resolves every request exactly once, so
it is the sole authority. A concurrent test asserts that scheduler occupancy
always equals the number of successful admissions.

**The dispatcher retries the head of the queue** rather than skipping past it.
Every request has the same 1 vCPU and 1 GiB shape, so if the head cannot be
placed, nothing behind it can either. Strict first-in-first-out therefore costs
nothing and buys predictable tail latency instead of the starvation a
work-stealing scheme would permit.

## Consequences

A caller can block for up to the deadline. That is the correct trade for this
workload: a microVM placement that takes 200 ms is fine, one that fails is not.

Retries are woken by capacity-release signals rather than polling. The signal
channel coalesces at depth one, so a release never blocks on the dispatcher no
matter how many land at once.

Booting happens outside the serialised dispatcher. The dispatcher is serialised
to keep admission ordered, and a boot is a network round trip to another pod;
doing it inline would cap the system at roughly 40 requests per second.
