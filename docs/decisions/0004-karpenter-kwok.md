# 4. Karpenter with the kwok provider for a zero-budget cluster

Status: accepted

## Context

The assignment names Karpenter and the CapacityBuffer API specifically. Karpenter
provisions real cloud instances, which needs a cloud account and a budget. This
project has neither.

## Decision

Run Karpenter with its **kwok** provider on a local kind cluster. kwok fabricates
`Node` objects instead of calling a cloud API. Karpenter's scheduling,
disruption, consolidation, drift and CapacityBuffer logic all execute for real
against those nodes.

## What is real and what is not

Real: the Karpenter controller, the `NodePool` and `NodeClaim` lifecycle, the
`CapacityBuffer` CRD and its controller, all scheduling and consolidation
decisions, and the placement path end to end over HTTP.

Not real: the machines. A kwok node runs no kubelet, so a pod "scheduled" onto
one does not execute a container.

That second point shapes the local demo. vmhost pods that must genuinely run,
register and serve traffic are pinned to the real kind node with shrunken
resource requests via `deploy/overlays/local`. Karpenter and CapacityBuffer
operate on the `NodePool` alongside. Both halves are real; they are just not the
same nodes, and the README says so.

## The gate that would have failed silently

`CapacityBuffer` is an alpha feature gate that **defaults to false** in both
upstream Karpenter and the AWS chart. The kwok chart's `values.yaml` does not
even declare the key, so the deployment template renders it as an empty string
unless it is set explicitly.

The CRD applies cleanly either way and the controller simply does nothing. This
was verified from the running controller rather than assumed:

```
FEATURE_GATES=StaticCapacity=false,SpotToSpotConsolidation=false,NodeRepair=false,CapacityBuffer=true
controller: capacitybuffer  controllerGroup: autoscaling.x-k8s.io  controllerKind: CapacityBuffer
```

## Consequences

The `NodePool` keeps faithful metal instance types (`c6g.metal`, `m6g.metal`,
`r6g.metal`) and spot-with-fallback, so moving to EC2 is a change of
`nodeClassRef` rather than a rewrite. `infra/terraform/` holds the EC2 shape for
when a budget exists.

Node provisioning latency on kwok is seconds rather than minutes, so the
autoscaler's `provision-latency` is tuned to what this environment actually
exhibits. On EC2 it must be re-measured, which is the entire point of making it
a flag rather than a constant.
