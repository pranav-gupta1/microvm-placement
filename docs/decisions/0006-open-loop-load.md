# 6. Open-loop load generation

Status: accepted

## Context

The harness must offer a specified rate: a double ramp from zero to 1000
requests per second and back, twice, with sporadic rather than metronomic
arrivals.

## Decision

Issue requests on the schedule the envelope dictates, regardless of whether
earlier requests have completed. Arrival times come from a non-homogeneous
Poisson process sampled by thinning.

## Why not a worker pool

The common shape is a fixed pool of goroutines each looping "send, wait for
reply, send again". That is closed loop, and it cannot fail to meet its target
rate, because its rate is defined by the system under test. When the system
slows, the generator quietly offers less load. It would report a clean run
against a system that had actually fallen over, which is precisely the failure
this harness exists to detect.

## Why Poisson rather than a fixed tick

A fixed tick generates perfectly smooth traffic. Real traffic clumps, and the
clumping is what tests the admission queue: a burst that momentarily exceeds
capacity is exactly the transient the queue exists to absorb. A metronome would
never produce one.

This is asserted rather than assumed. A Kolmogorov-Smirnov test checks
interarrival times against `Exp(1000)` inside the flat region of a trapezoidal
envelope at alpha = 0.001.

## Why thinning

Lewis and Shedler thinning draws candidates from a homogeneous process at the
envelope's peak rate and keeps each with probability `lambda(t)/lambda_max`. It
needs no closed form for the inverse of the integrated rate function, so the
envelope shape stays a free parameter. Rejection cost is bounded by the
peak-to-mean ratio, about 2 for a symmetric ramp.

## Consequences

The generator must hold as many in-flight requests as the system is behind by,
so it records an in-flight high-water mark alongside latency.

Arrival times are targeted **absolutely** against the run start, not by sleeping
for each interarrival gap. Sleeping accumulates: an overshoot of a millisecond
on every one of 16,000 arrivals would silently push the run well below its
target rate. Absolute targeting means an overshoot is corrected by the next gap.

Delivery is verified rather than trusted. Tests assert the arrival count matches
the analytic integral of the rate function, and separately that the *local*
per-second rate tracks the envelope, since an aggregate count alone would pass
even if every arrival landed in the wrong place.
