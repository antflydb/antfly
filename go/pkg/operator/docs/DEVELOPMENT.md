# Antfly Operator Development Guide

This guide is for engineers working on the Antfly Kubernetes Operator itself. If you're a user looking to deploy Antfly clusters, see [README.md](../README.md).

## 🏗️ Development Environment Setup

### Prerequisites

- **Go 1.26+**
- **Docker** or **Podman**
- **kubectl** and access to a Kubernetes cluster
- **Make**
- **Minikube** (for local testing)

### Local Development Setup

```bash
# Clone the repository
git clone https://github.com/antflydb/antfly.git
cd antfly/go/pkg/operator

# Set up development environment
make dev-setup

# Generate manifests
make manifests

# Run tests
make test

# Build locally
make build

# Run locally (requires cluster access)
make run
```

## 🔄 Development Workflow

### Inference runtime contract smoke test

Operator CI runs the generated model-puller and inference-server arguments
against the checksum-pinned Antfly v0.2.1 Linux GNU release, using the native CPU
backend, a small public BGE embedding model, and a tiny GGUF generator
(`shibatch/tiny1m:gguf:Q4_K_M`). It requires eager and lazy
readiness plus a finite, nonzero 384-dimensional embedding, verifies eager
warming before the first request, and checks that removing the model-directory
flag and config field reproduces an unready server. It needs neither Kubernetes
nor GPUs.

The same reconciler-generated contract runs against the PR's candidate binary
in `zig-tests.yml`. Both binaries exercise nested overrides (including empty
preloads and zero model limits), tagged GGUF preloads with and without a nested
`api_url`, and omitted `kind`/backend defaults. Generator warming is asserted
before serving an embedding request. Missing downloads/binaries fail the test.
The CPU fixture uses policy-neutral `residency_mode: auto` and
`memory_budget_mb: 0`; non-default GPU/A4B policies retain focused config tests
and require separate GPU execution validation.
The contract also checks omitted task hints with lazy discovery and an explicit
eager preload kind, plus empty/null optional identity fields. Unit tests verify
that ambiguous eager task hints report a validation failure before workload
creation, including on an operator upgrade with an unchanged pool generation,
and that correcting the hint allows reconciliation to recover.

To run locally on Linux or macOS, supply an absolute path to a verified released
or newly built Antfly binary:

```bash
ANTFLY_RUNTIME_BIN=/absolute/path/to/antfly GOWORK=off go test \
  -tags runtimeintegration ./controllers/inference \
  -run '^TestInferenceRuntimeContract$' -count=1 -v -timeout=20m
```

This explicit integration target fails if its binary is absent or its model
cannot be downloaded; ordinary offline unit tests do not run it. Downloads and
runtime state use isolated temporary directories, and server process groups are
terminated on completion or timeout. When advancing the supported runtime,
update both the release version and SHA256 in `antfly-operator-go.yml` and run
this same target against the candidate binary. This test does not exercise
container packaging, real Kubernetes scheduling, or GPU execution; retain those
rollout checks separately.

Runtime PR CI also runs `zig/e2e/inference/test_run_config.py` against the newly
built binary: config-only flat/nested startup, eager warming and embeddings,
and CLI precedence in either argument order. This covers the config handoff
independently of the operator's compatibility flags.

### Quick Development Cycle

```bash
# 1. Make code changes to the operator
# 2. Quick rebuild and redeploy
make docker-build && minikube image load antfly-operator:latest
kubectl rollout restart deployment/antfly-operator -n antfly-operator-namespace

# 3. Test with existing cluster or create new one
kubectl apply -f examples/small-dev-cluster.yaml

# 4. Watch logs for debugging
kubectl logs deployment/antfly-operator -n antfly-operator-namespace -f
```

### Code Generation

```bash
# Generate CRD manifests
make manifests

# Generate deepcopy methods
make generate

# Regenerate CRDs, RBAC, and deepcopy after API changes
make manifests generate
```

## 🧪 Testing

### Unit Tests

```bash
# Run all tests
make test

# Run specific test package
go test ./controllers/...

# make test already writes a coverage profile
make test
```

### Integration Tests

```bash
# Unit and envtest integration tests
make test
```

### Testing with Kind

```bash
# Create test cluster
make kind-create

# Deploy to kind
make kind-deploy

# Build and load the operator image into kind, then apply samples
make kind-deploy
make deploy-samples

# Clean up
make kind-delete
```

## 🚀 Minikube Development

### Complete Cleanup & Redeploy

This section provides a complete workflow for cleaning up everything in your local minikube environment and performing a fresh deployment.

#### Prerequisites

- **Minikube** installed and running
- **Docker** for building images
- **kubectl** configured for minikube
- **Make** for build automation

#### Complete Cleanup Process

1. **Remove All Antfly Resources**

```bash
# Remove all database clusters
kubectl delete antflyclusters --all --all-namespaces

# Remove the operator
kubectl delete -f https://antfly.io/antfly-operator-install.yaml --ignore-not-found=true

# Remove any leftover resources
kubectl delete all,pvc,secrets,configmaps -l app=antfly --all-namespaces

# Clean up operator namespace
kubectl delete namespace antfly-operator-namespace --ignore-not-found=true

# Remove any leftover custom resource definitions
kubectl delete crd antflyclusters.antfly.io --ignore-not-found=true
```

2. **Clean Local Build Artifacts**

```bash
# Clean all build artifacts
make clean

# Remove any local Docker images
docker rmi antfly-operator:latest antfly:latest --force 2>/dev/null || true

# Clean up minikube docker cache (optional)
minikube ssh -- docker system prune -f
```

3. **Restart Minikube (Optional but Recommended)**

```bash
# Stop minikube
minikube stop

# Delete and recreate for complete fresh start
minikube delete
minikube start

# Verify minikube is ready
kubectl get nodes
```

#### Fresh Deployment Process

1. **Build and Load Images**

```bash
# Build the operator image
make docker-build

# Pull and tag the published Zig runtime image
docker pull ghcr.io/antflydb/antfly:latest
docker tag ghcr.io/antflydb/antfly:latest antfly:latest

# Load images into minikube
minikube image load antfly-operator:latest
minikube image load antfly:latest

# Verify images are loaded
minikube image list | grep -E "(antfly|antfly-operator)"
```

2. **Deploy the Operator**

```bash
# Generate fresh manifests, then deploy the dev cluster to minikube
make manifests generate
make dev-cluster-deploy

# Verify operator is running
kubectl get pods -n antfly-operator-namespace
kubectl logs deployment/antfly-operator -n antfly-operator-namespace
```

3. **Deploy Database Clusters**

```bash
# Deploy a simple test cluster
kubectl apply -f examples/small-dev-cluster.yaml

# Or deploy the development cluster for minimal resources
kubectl apply -f examples/development-cluster.yaml

# Monitor deployment progress
kubectl get antflyclusters --watch
```

4. **Verify Deployment**

```bash
# Check all resources
kubectl get all -l app=antfly

# Check cluster status
kubectl get antflyclusters -o wide

# Check persistent volumes
kubectl get pvc -l app=antfly

# Test connectivity
kubectl port-forward service/simple-antfly-cluster-public-api 8080:80 &
curl -f http://localhost:8080/health || echo "Waiting for cluster to be ready..."
```

#### Quick One-Command Cleanup & Redeploy

For rapid development cycles, use this streamlined approach:

```bash
# Complete cleanup and redeploy script
#!/bin/bash
set -e

echo "🧹 Cleaning up everything..."
kubectl delete antflyclusters --all --all-namespaces --ignore-not-found=true
kubectl delete -f https://antfly.io/antfly-operator-install.yaml --ignore-not-found=true
make clean

echo "🔨 Building fresh..."
make docker-build
minikube image load antfly-operator:latest

echo "🚀 Deploying fresh..."
make dev-cluster-deploy
kubectl apply -f examples/small-dev-cluster.yaml

echo "⏳ Waiting for cluster to be ready..."
kubectl wait --for=condition=Ready pod -l app=antfly --timeout=300s

echo "✅ Deployment complete!"
kubectl get antflyclusters
```

Save this as `scripts/minikube-redeploy.sh` and run:

```bash
chmod +x scripts/minikube-redeploy.sh
./scripts/minikube-redeploy.sh

# Or use the Makefile target
make dev-cluster-deploy
```

### Troubleshooting Development Issues

#### Image Pull Issues

```bash
# Ensure images are loaded into minikube
minikube image list | grep antfly

# If missing, reload images
minikube image load antfly-operator:latest
minikube image load antfly:latest
```

#### Resource Issues

```bash
# Check minikube resources
kubectl top nodes
kubectl describe nodes

# Increase minikube resources if needed
minikube stop
minikube config set memory 8192
minikube config set cpus 4
minikube start
```

#### Persistent Volume Issues

```bash
# Check storage provisioner
kubectl get storageclass

# If using custom storage class, ensure it exists
kubectl get storageclass standard -o yaml
```

## 🔧 Operator Configuration

### Operator Metrics

The operator exposes Prometheus metrics on port `:8080/metrics`:

- `antfly_cluster_status` - Cluster health status
- `antfly_nodes_ready` - Number of ready nodes per type
- `antfly_reconcile_duration` - Reconciliation time

## 🏗️ Architecture Deep Dive

### Operator Components

```
┌─────────────────────────────────────────────────────────────┐
│                    Antfly Operator                         │
│                                                             │
│  ┌─────────────────┐    ┌─────────────────────────────────┐ │
│  │   Controller    │    │         Reconciler              │ │
│  │   Manager       │    │                                 │ │
│  │                 │────▶  • AntflyCluster Controller     │ │
│  │ • Leader Election│    │  • StatefulSet Management      │ │
│  │ • Health Checks │    │  • Service Creation             │ │
│  │ • Metrics       │    │  • PVC Management               │ │
│  └─────────────────┘    └─────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────┐
│                 Kubernetes Resources                        │
│                                                             │
│ ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│ │ StatefulSet │  │ StatefulSet │  │   Services  │         │
│ │ (Leaders)   │  │   (Data)    │  │             │         │
│ └─────────────┘  └─────────────┘  └─────────────┘         │
│                                                             │
│ ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│ │    PVCs     │  │ ConfigMaps  │  │   Secrets   │         │
│ │             │  │             │  │             │         │
│ └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
```

### Reconciliation Logic

1. **Resource Validation**: Validate AntflyCluster spec
2. **ConfigMap Management**: Create/update configuration
3. **StatefulSet Creation**: Create leader and data StatefulSets
4. **Service Management**: Create internal and public services
5. **Status Updates**: Update cluster status based on pod readiness

## 📝 Contributing Guidelines

### Code Standards

- Follow Go conventions and best practices
- Use `make fmt` and `make vet` before committing
- Write unit tests for new functionality
- Update documentation for API changes

### Pull Request Process

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes and add tests
4. Run the full test suite: `make test`
5. Update documentation if needed
6. Submit a pull request

### Release Process

```bash
# Prepare release
make release

# Complete release with Docker push
make release-all
```

## 🔍 Debugging

### Common Debug Commands

```bash
# Check operator logs
kubectl logs deployment/antfly-operator -n antfly-operator-namespace -f

# Check operator metrics
kubectl port-forward -n antfly-operator-namespace deployment/antfly-operator 8080:8080
curl http://localhost:8080/metrics

# Check operator health
curl http://localhost:8081/healthz
curl http://localhost:8081/readyz

# Debug specific cluster
kubectl describe antflycluster my-cluster
kubectl get events --field-selector involvedObject.name=my-cluster
```

### Performance Profiling

```bash
# Enable pprof in development
go tool pprof http://localhost:6060/debug/pprof/profile

# Memory profiling
go tool pprof http://localhost:6060/debug/pprof/heap
```

## 📚 Additional Resources

- [Kubebuilder Documentation](https://book.kubebuilder.io/)
- [Controller Runtime](https://github.com/kubernetes-sigs/controller-runtime)
- [Kubernetes Operator Pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/)
- [Antfly Database Documentation](https://github.com/antflydb/antfly)

---

For user-focused documentation, see [README.md](../README.md).
