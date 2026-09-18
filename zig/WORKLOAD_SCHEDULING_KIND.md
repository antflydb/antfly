# Local workload qualification with kind

This is optional deployment integration tooling. Scheduler correctness and
performance qualification use deterministic tests and direct resource-limited
containers; they do not depend on Kubernetes. Use this setup when a test needs
operator reconciliation, pod/PVC lifecycle, service routing, or deployment HA.

`scripts/workload_kind.py` creates an isolated Kubernetes environment using the
Antfly operator and the Cloud package envelopes. It requires Docker, kind,
kubectl, Python 3.11+, and locally built Linux runtime/operator images. The default
node image is pinned to the multiarchitecture digest published with
[kind v0.32.0](https://github.com/kubernetes-sigs/kind/releases/tag/v0.32.0).
Use kind 0.32.0 or newer with that image.

The harness owns a dedicated `antfly-workload-*` cluster and a private kubeconfig
inside a new evidence directory. Every kubectl operation supplies that file and
context explicitly. Deletion verifies the cluster UID, collects evidence, and
removes only the recorded cluster. It never removes the evidence directory.

## Package fidelity

| Tier | CPU per node | Memory per node | Data PVC request |
| --- | ---: | ---: | ---: |
| Starter | 1 | 4 GiB | 50 GiB |
| Standard | 2 | 4 GiB | 100 GiB |
| Pro | 4 | 8 GiB | 200 GiB |

Requests equal limits. Single mode runs one combined node. Replicated mode runs
three metadata and three data nodes; metadata PVC requests are 2 GiB each. These
are database envelopes. Inference workloads need an additional, separately
accounted provider fixture. Hot-standby fault orchestration remains to be added.

Preflight checks Docker's aggregate CPU/memory capacity, because multiple kind
nodes share the same Docker VM. It reserves another 2 CPUs/4 GiB for test and
control-plane overhead. Starter replicated therefore needs 8 CPUs/28 GiB; Pro
replicated needs 26 CPUs/52 GiB. Insufficient capacity fails before cluster
creation and never reduces the package limits. Run one qualification case at a
time and keep unrelated workloads off the measurement host.

kind's local-path storage honors PVC requests as Kubernetes metadata; those
requests do **not** enforce filesystem quotas or reproduce Cloud disk latency.
Record actual storage and available space alongside the results. This setup
qualifies scheduling under local CPU/memory limits; it cannot establish a Cloud
storage SLA or independent-machine fault tolerance.

## Create and inspect

Build images from the candidate checkout using the repository Dockerfiles:

```sh
docker build -t antfly-workload-runtime:candidate -f zig/Dockerfile .
docker build -t antfly-workload-operator:candidate -f go/pkg/operator/Dockerfile .
python3 scripts/workload_kind.py create \
  --directory /tmp/antfly-workload-candidate-starter \
  --tier starter --topology single \
  --runtime-image antfly-workload-runtime:candidate \
  --operator-image antfly-workload-operator:candidate \
  --config /tmp/candidate-admission-policy.json
```

The JSON config is the exact engine policy under test. Omit `--config` to retain
legacy admission defaults. Add `--render-only` to generate reviewable manifests
without Docker or Kubernetes. `--topology replicated` selects the six-node
layout. Images use `Never` pull policy after explicit kind loading; the receipt
retains Docker image identities separately from the harness source revision.

`create` installs the repository's CRDs/RBAC and operator, waits for readiness,
and retains diagnostics. A failed deployment leaves its cluster and evidence for
inspection. After fixing an image or prerequisite, `deploy --directory ...`
retries against the same recorded cluster.

Expose the database on localhost while running a workload driver:

```sh
python3 scripts/workload_kind.py connect \
  --directory /tmp/antfly-workload-candidate-starter --port 18080
```

The public API is then `http://127.0.0.1:18080/db/v1`. Port forwarding is useful
for correctness and smoke tests; a release throughput run must also demonstrate
that its transport/load generator is not the bottleneck, preferably using an
in-cluster driver with its own reserved resources.

## Evidence and cleanup

```sh
python3 scripts/workload_kind.py collect --directory /tmp/antfly-workload-candidate-starter
python3 scripts/workload_kind.py delete --directory /tmp/antfly-workload-candidate-starter
```

Evidence includes engine policy, manifests, Docker capacity and image metadata,
node/pod/PVC/service/events/status objects, logs, cgroup CPU throttling and
memory limits/current/peak/events, and SHA-256 checksums. The kubeconfig contains
local cluster credentials and is excluded from the receipt checksums; do not
publish it. Missing cgroup readings remain recorded errors, never invented zeros.

The environment receipt always starts `qualified: false`. Creating a cluster or
passing a smoke test does not pass the
[performance and correctness matrix](WORKLOAD_SCHEDULING_QUALIFICATION.md).
Retain workload samples, original-submission latency, failures/retries, recall,
and baseline/candidate identities in the same run directory. Fixed, automatic,
and adaptive policies need separate qualified runs.
