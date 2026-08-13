# 5. vmhost agents self-register rather than being discovered by an informer

Status: accepted

## Context

The placement API needs a live view of which vmhost pods exist and how many free
slots each has. The Kubernetes-native answer is a Pod informer: watch pods,
derive host state from phase and readiness.

## Decision

Each `vmhostd` registers itself with the placement API on startup and sends a
heartbeat every 2 seconds. A host that stops heartbeating for 6 seconds is
drained.

## Why

An informer answers the wrong question. Pod readiness is a proxy for "can this
process accept a microVM right now"; a heartbeat from the process that owns the
slots is a direct answer. The informer also brings RBAC, a watch cache that can
go stale exactly when the API server is busiest, and a fake clientset in every
test.

Self-registration runs identically under Kubernetes, under docker-compose, and in
a unit test with no Kubernetes at all.

## Cost

Detection of an abrupt pod death is delayed by up to the heartbeat timeout rather
than being immediate. That is bounded, tunable, and already tolerated: the
admission queue retries a failed boot on another host.

The timeout is three heartbeat intervals so that one dropped heartbeat, or one
lost to a garbage collection pause, does not evict a healthy host and churn its
microVMs for nothing.

## Two details that matter

**Eviction drains, it does not delete.** A wedged agent may still have live
microVMs. Removing its host outright would orphan them and leak their slots, so
the host is moved to `Draining`, stops receiving new work, and is removed only
once its last microVM has exited.

**Registration is idempotent.** An agent that restarts in place, or whose first
response was lost, will legitimately register twice. Treating a repeat
registration as a heartbeat removes a whole class of startup race.

## A bug this design caused, and the fix

Gating the placement API's `/readyz` on having ready hosts deadlocked the
rollout: agents register over that same server, so a Service withholding
endpoints until hosts exist prevents the registrations that would create them.

Readiness now reports process health only. An empty fleet is not an unready
service, it is a service with no capacity, which the admission queue already
handles by making requests wait. Capacity is alerted on through
`microvm_vmhosts_ready` instead.
