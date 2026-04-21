# Antfly Operator Development Guide

This guide is for engineers working on the Antfly Kubernetes Operator itself. If you're a user looking to deploy Antfly clusters, see [README.md](README.md).

## 🏗️ Development Environment Setup

### Prerequisites

- **Go 1.24+**
- **Docker** or **Podman**
- **kubectl** and access to a Kubernetes cluster
- **Make**
- **Minikube** (for local testing)

### Local Development Setup

```bash
# Clone the repository
git clone https://github.com/antflydb/antfly.git
cd antfly/pkg/antfly-operator

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

# Update API documentation
make api-docs
```

## 🧪 Testing

### Unit Tests

```bash
# Run all tests
make test

# Run specific test package
go test ./controllers/...

# Run with coverage
make test-coverage
```

### Integration Tests

```bash
# Run integration tests with envtest
make test-integration

# Run with existing cluster
make test-e2e
```

### Testing with Kind

```bash
# Create test cluster
make kind-create

# Deploy to kind
make kind-deploy

# Run tests against kind
make kind-test

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
kubectl delete -f deploy/install.yaml --ignore-not-found=true

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

# Build the Antfly database image (assumes you have the Antfly source)
# Replace with your actual Antfly build process
docker build -t antfly:latest /path/to/antfly/source

# Load images into minikube
minikube image load antfly-operator:latest
minikube image load antfly:latest

# Verify images are loaded
minikube image list | grep -E "(antfly|antfly-operator)"
```

2. **Deploy the Operator**

```bash
# Generate fresh manifests and installation bundle
make install

# Deploy the operator to minikube
make deploy

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
kubectl delete -f deploy/install.yaml --ignore-not-found=true
make clean

echo "🔨 Building fresh..."
make docker-build
minikube image load antfly-operator:latest

echo "🚀 Deploying fresh..."
make deploy
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
make minikube-redeploy
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

For user-focused documentation, see [README.md](README.md).
