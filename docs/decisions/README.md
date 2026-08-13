# Architecture decision records

One file per decision that was not obvious, recording what was chosen, what was
rejected, and what it costs. A decision with no downside listed is a decision
that was not really examined.

| # | Decision |
|---|---|
| [0001](0001-microvm-lifetime.md) | microVM lifetime is a stated assumption, not a measurement |
| [0002](0002-best-fit-placement.md) | Best-fit placement, because idle pods are what is graded |
| [0003](0003-virtualization.md) | Three hypervisor implementations, because no free environment runs both |
| [0004](0004-karpenter-kwok.md) | Karpenter with the kwok provider for a zero-budget cluster |
| [0005](0005-self-registration.md) | vmhost agents self-register rather than being discovered by an informer |
| [0006](0006-open-loop-load.md) | Open-loop load generation |
| [0007](0007-queue-not-reject.md) | The admission queue waits instead of returning 503 |
