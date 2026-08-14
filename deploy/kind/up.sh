#!/usr/bin/env bash
#
# Brings up the whole local stack from nothing: kind cluster, kwok, Karpenter
# with CapacityBuffer, KEDA, Prometheus, Grafana, and the application.
#
# Idempotent. Re-running it reconciles rather than starting over, so it is safe
# to use to repair a half-built cluster.
set -euo pipefail

CLUSTER="${CLUSTER:-microvm}"
NAMESPACE="${NAMESPACE:-microvm}"
KWOK_VERSION="${KWOK_VERSION:-v0.8.0}"
# Pinned rather than tracking latest. CapacityBuffer support landed in
# kubernetes-sigs/karpenter v1.14.0 and the API is v1beta1 there; an unpinned
# build would silently change the CRD schema under us.
KARPENTER_VERSION="${KARPENTER_VERSION:-v1.14.0}"
KEDA_VERSION="${KEDA_VERSION:-2.17.1}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK_DIR="${WORK_DIR:-${TMPDIR:-/tmp}/microvm-build}"

log() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
die() { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }

require() {
  for tool in "$@"; do
    command -v "$tool" >/dev/null 2>&1 || die "$tool is required but not installed"
  done
}

require docker kind kubectl helm go

# On Apple Silicon an x86_64 toolchain from Intel Homebrew will emulate the
# whole VM and Kubernetes will miss its own health timeouts. Catch it here
# rather than after a confusing ten-minute failure.
if [[ "$(uname -m)" == "arm64" ]] && file "$(command -v kind)" | grep -q x86_64; then
  die "kind is an x86_64 binary on an arm64 host. Install native tools from /opt/homebrew."
fi

log "creating kind cluster '${CLUSTER}'"
if kind get clusters 2>/dev/null | grep -qx "${CLUSTER}"; then
  echo "    already exists, reusing"
else
  kind create cluster --config "${REPO_ROOT}/deploy/kind/cluster.yaml" --wait 240s
fi
kubectl config use-context "kind-${CLUSTER}" >/dev/null

# Applied here rather than via a kubeadm patch. kind already untaints a
# single-node cluster, and patching it too makes kind's own untaint step fail.
log "labelling the control-plane node as system capacity"
kubectl label node "${CLUSTER}-control-plane" microvm.io/role=system --overwrite >/dev/null

log "installing kwok ${KWOK_VERSION}"
kubectl apply -f "https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VERSION}/kwok.yaml" >/dev/null
kubectl apply -f "https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VERSION}/stage-fast.yaml" >/dev/null

log "building Karpenter ${KARPENTER_VERSION} (kwok provider)"
mkdir -p "${WORK_DIR}"
if [[ ! -d "${WORK_DIR}/karpenter" ]]; then
  git clone --depth 1 --branch "${KARPENTER_VERSION}" \
    https://github.com/kubernetes-sigs/karpenter.git "${WORK_DIR}/karpenter" >/dev/null 2>&1
fi
command -v ko >/dev/null 2>&1 || die "ko is required to build the kwok provider: brew install ko"

pushd "${WORK_DIR}/karpenter" >/dev/null
# kind.local makes ko load straight into the cluster, so no registry is needed.
KARPENTER_IMG="$(KO_DOCKER_REPO=kind.local KIND_CLUSTER_NAME="${CLUSTER}" CGO_ENABLED=0 \
  ko build -B --platform=linux/"$(go env GOARCH)" sigs.k8s.io/karpenter/kwok 2>/dev/null | tail -1)"
[[ -n "${KARPENTER_IMG}" ]] || die "ko build produced no image"

kubectl apply -f kwok/charts/crds >/dev/null

log "deploying Karpenter with the CapacityBuffer feature gate ENABLED"
# capacityBuffer is alpha and defaults to false. The kwok chart's values.yaml
# does not even declare the key, so the template renders it empty unless it is
# set here. Without this the CRD applies cleanly and the controller does
# nothing at all.
helm upgrade --install karpenter kwok/charts \
  --namespace kube-system --skip-crds \
  --set controller.image.repository="${KARPENTER_IMG%%:*}" \
  --set controller.image.tag="${KARPENTER_IMG##*:}" \
  --set settings.preferencePolicy=Ignore \
  --set settings.featureGates.capacityBuffer=true \
  --set settings.featureGates.staticCapacity=false \
  --set settings.featureGates.spotToSpotConsolidation=false \
  --set settings.featureGates.nodeRepair=false \
  --wait --timeout 5m >/dev/null
popd >/dev/null

# Assert the gate actually took effect, because the failure mode is silence.
gates="$(kubectl -n kube-system get deploy karpenter \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="FEATURE_GATES")].value}')"
[[ "${gates}" == *"CapacityBuffer=true"* ]] \
  || die "CapacityBuffer gate is not enabled, got: ${gates}"
echo "    FEATURE_GATES=${gates}"

log "installing KEDA ${KEDA_VERSION}"
helm repo add kedacore https://kedacore.github.io/charts >/dev/null 2>&1 || true
helm repo update kedacore >/dev/null 2>&1 || true
helm upgrade --install keda kedacore/keda \
  --namespace keda --create-namespace \
  --version "${KEDA_VERSION}" \
  --wait --timeout 5m >/dev/null

log "building application images"
pushd "${REPO_ROOT}" >/dev/null
for binary in placement-api vmhostd loadgen; do
  target=service
  [[ "${binary}" == vmhostd ]] && target=vmhostd
  DOCKER_BUILDKIT=1 docker build \
    --target "${target}" --build-arg BINARY="${binary}" \
    -t "microvm/${binary}:dev" -f images/Dockerfile . >/dev/null
done
kind load docker-image microvm/placement-api:dev microvm/vmhostd:dev microvm/loadgen:dev --name "${CLUSTER}" >/dev/null

log "deploying observability"
kubectl apply -f deploy/observability/prometheus.yaml >/dev/null
kubectl create configmap grafana-dashboard-placement \
  --from-file=placement.json=deploy/observability/dashboard.json \
  -n observability --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl apply -f deploy/observability/grafana.yaml >/dev/null

log "deploying the application"
kubectl apply -k deploy/overlays/local >/dev/null
kubectl apply -f deploy/karpenter/nodepool.yaml >/dev/null
kubectl apply -f deploy/karpenter/capacitybuffer.yaml >/dev/null
kubectl apply -f deploy/keda/scaledobject.yaml >/dev/null
popd >/dev/null

log "waiting for rollouts"
kubectl -n "${NAMESPACE}" rollout status deploy/placement-api --timeout=180s
kubectl -n "${NAMESPACE}" rollout status deploy/vmhost --timeout=180s
kubectl -n observability rollout status deploy/prometheus --timeout=180s
kubectl -n observability rollout status deploy/grafana --timeout=180s

# Registration is asynchronous, so wait for the fleet to actually appear rather
# than racing a load run against an empty scheduler.
log "waiting for vmhost agents to register"
for _ in $(seq 1 60); do
  ready="$(curl -fsS http://127.0.0.1:18080/readyz 2>/dev/null | sed -n 's/.*"ready_hosts":\([0-9]*\).*/\1/p')"
  [[ -n "${ready}" && "${ready}" -gt 0 ]] && break
  sleep 2
done
[[ -n "${ready:-}" && "${ready}" -gt 0 ]] || die "no vmhost registered; check: kubectl -n ${NAMESPACE} logs -l app=vmhost"

cat <<EOF

  Cluster is up.

    placement API   http://127.0.0.1:18080
    Grafana         http://127.0.0.1:3000
    Prometheus      http://127.0.0.1:9090

    vmhosts ready   ${ready}

  Run a load test:   make load
  Tear down:         make demo-down

EOF
