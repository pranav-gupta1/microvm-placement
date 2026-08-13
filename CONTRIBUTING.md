# Contributing

## Getting set up

Requires Go 1.25+, Docker, kind, kubectl, helm and ko.

On Apple Silicon these must be **arm64** builds. Intel Homebrew under Rosetta
installs x86_64 binaries, which makes Colima emulate an x86 VM and Kubernetes
miss its own health timeouts. Check with `file $(which kind)`; if it says
x86_64, install from `/opt/homebrew`.

```sh
make verify   # fmt, tidy, lint and the full test suite
```

## Before opening a pull request

`make verify` must pass. CI runs the same checks plus the full 1000 requests per
second run, which is skipped only under `-short`.

## Conventions

**Commits** follow Conventional Commits. The body should explain *why*, not
restate the diff. If a change fixes something subtle, say what the symptom was
and how it was diagnosed; several commits here exist mainly to record that.

**Comments** explain reasoning, not mechanics. A comment restating the code
earns nothing. A comment explaining why the slot is reserved before the boot
delay, or why readiness must not depend on fleet capacity, saves the next person
an afternoon.

**Tests** are named for the property they defend, not the function they call.
`TestNoSlotIsLeakedWhenAdmissionTimesOut` says what breaks if it fails;
`TestAdmit3` does not.

**Prose** uses no em dashes. Commas, colons, parentheses and full stops are
enough.

## Where the interesting parts are

- `internal/scheduler` placement, and why best fit rather than spreading
- `internal/placement` the admission queue, and the never-drop guarantee
- `internal/autoscale` the replica-count policy, and the noise bias it corrects
- `internal/hypervisor` the boundary between real and simulated virtualisation

`docs/decisions/` records the choices that were not obvious, including their
costs. If you change one of those decisions, update the record rather than
silently diverging from it.
