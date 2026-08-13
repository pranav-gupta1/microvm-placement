# 1. microVM lifetime is a stated assumption, not a measurement

Status: accepted

## Context

The assignment fixes the arrival rate at 1000 requests per second but says
nothing about how long a microVM lives. Nothing can be sized without that
number: capacity is a function of concurrency, and concurrency is rate times
lifetime.

A persistent microVM per request is arithmetically impossible. One hour of
traffic at 1000 requests per second would mean 3.6 million live microVMs, which
at even 128 MiB each is over 400 TiB of memory.

## Decision

Every microVM has a mean lifetime of 500 ms, exponentially distributed. The
value is configurable, and the load generator sends the TTL on every request so
the model is visible in the data rather than only in prose.

Little's Law then gives everything else:

```
peak_concurrency = peak_rps x mean_ttl = 1000 x 0.5 = 500 concurrent microVMs
```

## Why exponential

A memoryless distribution is the honest default for independent short tasks: a
function invocation, a sandboxed build step, an agent tool call. It also has a
long tail, so the system must cope with microVMs living many times the mean
rather than a tidy fixed duration that would flatter the scheduler.

## Consequences

Every capacity number in this repository inherits this assumption. If the real
workload has a different lifetime the arithmetic changes, but nothing structural
does: the derivation is one multiplication in `docs/capacity-planning.md`.

The end-to-end run measured peak concurrency of 500 against a predicted 500,
which is a useful independent check that the model and the implementation agree.
