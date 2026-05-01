# CLAUDE.md

Kubernetes operator for deploying and managing Antfly database clusters and Termite ML pools. Built with Kubebuilder/controller-runtime. Supports GKE Autopilot and AWS EKS with Spot instances.

## Commands

```bash
make build                  # Build operator binary (output: bin/manager)
make test                   # Generate + fmt + vet + test with coverage
make manifests generate     # Required after API type changes (CRDs, DeepCopy, RBAC)
make run                    # Run operator locally against cluster
make docker-build           # Build container image
make lint                   # golangci-lint
```

All Go commands use `GOWORK=off` (set in Makefile).

## Architecture

**Two-layer cluster design:**
- **Metadata Nodes** — Raft consensus, API coordination. Fixed replica count (odd for Raft). Ports: 12377 (API), 9017 (Raft), 4200 (Health).
- **Data Nodes** — Storage, replication, autoscaling. Default 3 replicas. Ports: 12380 (API), 9021 (Raft), 4200 (Health).

Both deployed as StatefulSets with persistent volumes. Defaults applied in `controllers/antflycluster_controller.go` `applyDefaults()`.

**CRDs:** AntflyCluster, AntflyBackup, AntflyRestore, TermitePool, TermiteRoute (defined in `antfly/api/v1/` and `termite/api/v1alpha1/`).
Startup CRD bootstrap installs all five CRDs unless `--skip-crd-install` is set,
even when `--enable-termite-controllers=false`.

**Reconciliation order** (`controllers/antflycluster_controller.go`):
1. Apply defaults to DeepCopy (avoids API server conflicts)
2. ConfigMap → Services → Leader StatefulSet → Data StatefulSet → Autoscaling → Status

## Code Organization

```
cmd/antfly-operator/main.go   # Integrated operator entrypoint
antfly/api/v1/                # Antfly CRD types, webhooks, deepcopy
antfly/controllers/           # Antfly reconcilers + autoscaler
antfly/bootstrap/             # CRD self-installation at startup
termite/api/v1alpha1/         # Termite CRD types, webhooks, deepcopy
termite/controllers/          # Termite reconcilers
manifests/                    # Shared embedded CRD/RBAC YAML (go:embed)
config/manager/               # Deployment manifest
examples/                     # Sample cluster YAMLs
docs/                         # User-facing documentation
```

## Key Development Patterns

**After modifying `antfly/api/v1/antflycluster_types.go`:**
1. `make manifests generate`
2. Update reconciler in `antfly/controllers/antflycluster_controller.go`
3. Add/update tests

**StatefulSet changes:** Use `controllerutil.CreateOrUpdate`. Only update mutable fields to avoid recreation. Preserve pod identity and PVC retention.

**Cloud-specific logic:**
- GKE: `applyGKEPodSpec()`, `reconcilePodDisruptionBudget()` — triggered by `spec.gke.autopilot: true`
- EKS: Spot node selectors, tolerations, IRSA annotations — triggered by `spec.eks.enabled: true`
- Cannot enable both GKE Autopilot and EKS simultaneously (webhook-enforced)

**Webhook validation** (`api/v1/antflycluster_webhook.go`): Enforces immutability, enum validation, conflict detection. Controller has fallback validation with exponential backoff if webhook disabled.

## Autoscaling

Data nodes only (metadata nodes never autoscaled). Uses Kubernetes metrics API. Gradual scaling: +50%/+2 up, -25%/-1 down. Cooldowns: 60s up, 300s down. AutoScaler initialized in `pkg/operator/cmd/antfly-operator/main.go` and injected into reconciler.

## RBAC

Service account must be exactly `antfly-operator-service-account`. RBAC defined in `manifests/rbac.go` and generated `manifests/rbac/role.yaml`. The `policy/poddisruptionbudgets` permission is always required (controller watches PDBs).

## Backup Credentials

Injected via `spec.{metadataNodes,dataNodes}.envFrom` referencing a Secret. Operator hashes secret data into pod annotation `antfly.io/envfrom-hash` to trigger rolling updates on rotation. Tracks `SecretsReady` status condition.

## Testing

```bash
make test                           # Unit + envtest integration tests
go test ./controllers/...           # Specific package
kubectl apply -f examples/small-dev-cluster.yaml  # Manual testing
```

Minikube targets use `--context=minikube` explicitly. Kind targets: `make kind-create`, `make kind-deploy`, `make kind-delete`.
