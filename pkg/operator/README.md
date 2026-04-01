# Antfly Database Operator

[![Go Report Card](https://goreportcard.com/badge/github.com/antflydb/antfly/pkg/operator)](https://goreportcard.com/report/github.com/antflydb/antfly/pkg/operator)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-1.20+-blue.svg)](https://kubernetes.io/)

A cloud-native Kubernetes operator for deploying and managing [Antfly](https://github.com/antflydb/antfly) database clusters with built-in high availability, automatic scaling, and operational simplicity.

## 🚀 Quick Start

Deploy the operator and create your first database cluster in under 60 seconds:

```bash
# 1. Copy and deploy the operator
cp deploy/example_install.yaml deploy/install.yaml
kubectl apply -f ./deploy/install.yaml

# 2. Create a database cluster
kubectl create namespace antfly-dev-ns
kubectl apply -f ./examples/development-cluster.yaml

# 3. Check the status
kubectl get antflyclusters --namespace antfly-dev-ns
```

That's it! You now have a highly available Antfly database cluster running in Kubernetes.

## ✨ Features

- **🎯 One-Command Deployment**: Deploy production-ready database clusters with a single `kubectl apply`
- **🔄 High Availability**: Built-in Raft consensus with 3-node leader and data layers
- **📈 Horizontal Scaling**: Scale leader and data nodes based on demand
- **💾 Persistent Storage**: Automatic persistent volume management with configurable storage classes
- **🔐 Security**: RBAC-enabled with minimal required permissions
- **🔒 Service Mesh Integration**: Native support for Istio, Linkerd, and Consul Connect with automatic mTLS
- **📊 Observability**: Built-in metrics and logging integration
- **🛠️ Cloud Native**: Follows Kubernetes best practices and conventions
- **⚡ Performance**: Optimized for cloud environments with configurable caching layers
- **☁️ GKE Autopilot Support**: Native support for GKE Autopilot with Spot Pods for cost optimization

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐
│   Leader Nodes  │    │   Data Nodes    │
│   (StatefulSet) │    │   (StatefulSet) │
│                 │    │                 │
│ • Raft Leader   │    │ • Data Storage  │
│ • Coordination  │    │ • Replication   │
│ • Public API    │    │ • 3 Replicas    │
│ • 3 Replicas    │    │                 │
└─────────────────┘    └─────────────────┘
         │                       │
         └─────────┬─────────────┘
                   │
           ┌─────────────────────────────────────┐
           │        Load Balancer                │
           │     (Public API Service)            │
           └─────────────────────────────────────┘
```

## 📦 Installation

### Prerequisites

#### Required

- **Kubernetes 1.20+** with RBAC enabled
- **kubectl** configured to access your cluster
- **Storage class** available for persistent volumes
- **RBAC enabled** on your cluster

#### Optional

- **metrics-server** (required for autoscaling features)
- **Istio or Linkerd** (for service mesh integration)

#### Validation Commands

Verify your cluster meets the requirements:

```bash
# Check Kubernetes version (should be 1.20 or higher)
kubectl version --short

# Check storage classes are available
kubectl get storageclass
```

### Installation

The operator requires a custom installation manifest tailored to your environment. Use the example as a starting point:

```bash
# 1. Copy the example installation manifest
cp deploy/example_install.yaml deploy/install.yaml

# 2. Customize the manifest (optional - review and adjust as needed):
#    - Container image version/registry
#    - Resource limits for operator pod
#    - Namespace names
#    - RBAC permissions

# 3. Deploy the operator
kubectl apply -f ./deploy/install.yaml
```

This installs:
- **Namespace**: `antfly-operator-namespace`
- **Custom Resource Definitions** (CRDs): `AntflyCluster`
- **RBAC** roles and bindings
- **Operator Deployment**: Uses container image `ghcr.io/antflydb/antfly-operator:latest`

See `deploy/example_install.yaml` for the complete installation manifest structure.

### RBAC Requirements

The operator requires specific RBAC configuration. **When deploying via infrastructure-as-code tools (Pulumi, Terraform, etc.), ensure the following:**

#### Service Account Name
The operator **must** use service account name: `antfly-operator-service-account`

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: antfly-operator-service-account
  namespace: antfly-operator-namespace
```

#### Required ClusterRole Permissions
The ClusterRole must include permissions for PodDisruptionBudgets (required for GKE Autopilot support):

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: antfly-operator-cluster-role
rules:
  # ... other permissions ...
  - apiGroups:
      - policy
    resources:
      - poddisruptionbudgets
    verbs:
      - get
      - list
      - watch
      - create
      - update
      - patch
      - delete
```

#### ClusterRoleBinding
The ClusterRoleBinding must bind the ClusterRole to the correct service account:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: antfly-operator-cluster-rolebinding
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: antfly-operator-cluster-role
subjects:
  - kind: ServiceAccount
    name: antfly-operator-service-account
    namespace: antfly-operator-namespace
```

#### Operator Deployment Configuration
The Deployment must reference the correct service account:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: antfly-operator
  namespace: antfly-operator-namespace
spec:
  template:
    spec:
      serviceAccountName: antfly-operator-service-account
      # ... rest of spec ...
```

#### Common RBAC Issues

**Symptom**: Error message like:
```
poddisruptionbudgets.policy is forbidden: User "system:serviceaccount:antfly-operator-namespace:WRONG-NAME" cannot list resource "poddisruptionbudgets"
```

**Cause**: Service account name mismatch or missing PodDisruptionBudget permissions.

**Solution**:
1. Verify service account name: `kubectl get serviceaccount -n antfly-operator-namespace` (should show `antfly-operator-service-account`)
2. Verify deployment uses correct SA: `kubectl get deployment antfly-operator -n antfly-operator-namespace -o jsonpath='{.spec.template.spec.serviceAccountName}'` (should show `antfly-operator-service-account`)
3. Verify ClusterRole includes PDB permissions: `kubectl get clusterrole antfly-operator-cluster-role -o yaml`
4. Verify ClusterRoleBinding is correct: `kubectl get clusterrolebinding antfly-operator-cluster-rolebinding -o yaml`

For complete RBAC configuration, see `config/rbac/` directory.

### Verify Installation

```bash
# Check operator is running
kubectl get pods -n antfly-operator-namespace

# Check CRDs are installed
kubectl get crd antflyclusters.antfly.io
```

## 🔐 Container Image Verification

All container images are signed using [Sigstore cosign](https://github.com/sigstore/cosign) with keyless OIDC signing via GitHub Actions. This provides transparent, verifiable signatures without the need for key management.

### Install cosign

```bash
# macOS
brew install cosign

# Linux
wget https://github.com/sigstore/cosign/releases/latest/download/cosign-linux-amd64
sudo mv cosign-linux-amd64 /usr/local/bin/cosign
sudo chmod +x /usr/local/bin/cosign

# Or use the container image
alias cosign="docker run --rm gcr.io/projectsigstore/cosign"
```

### Verify Image Signature

Verify the operator image signature using GitHub's OIDC identity:

```bash
# Verify the latest operator image
cosign verify ghcr.io/antflydb/antfly-operator:latest \
  --certificate-identity-regexp="^https://github.com/antflydb/" \
  --certificate-oidc-issuer="https://token.actions.githubusercontent.com"

# Verify a specific version
cosign verify ghcr.io/antflydb/antfly-operator:v1.0.0 \
  --certificate-identity-regexp="^https://github.com/antflydb/" \
  --certificate-oidc-issuer="https://token.actions.githubusercontent.com"
```

### View Signature Annotations

Each signature includes metadata about the build:

```bash
# View signature details including annotations
cosign verify ghcr.io/antflydb/antfly-operator:latest \
  --certificate-identity-regexp="^https://github.com/antflydb/" \
  --certificate-oidc-issuer="https://token.actions.githubusercontent.com" | jq

# Expected annotations:
# - repo: github.com/antflydb/antfly-operator
# - workflow: Container
# - ref: <git commit SHA>
```

### Security Notes

- **No Secret Keys**: Images are signed using GitHub's OIDC provider - no private keys to manage or leak
- **Transparency Log**: All signatures are recorded in [Rekor](https://rekor.sigstore.dev/), Sigstore's immutable transparency log
- **Verification**: Anyone can verify signatures using only the public certificate and OIDC issuer
- **Attestations**: Signatures include workflow metadata (repository, workflow name, commit SHA) for full provenance

For more information about Sigstore and keyless signing, see the [Sigstore documentation](https://docs.sigstore.dev/).

## 🎯 Usage

### Create a Database Cluster

```yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: my-database
spec:
  image: antfly:latest

  metadataNodes:
    replicas: 3
    metadataAPI: {}
    metadataRaft: {}
    resources:
      cpu: "500m"
      memory: "512Mi"

  dataNodes:
    replicas: 3
    api: {}
    raft: {}
    resources:
      cpu: "1000m"
      memory: "2Gi"

  storage:
    storageClass: "standard"
    metadataStorage: "1Gi"
    dataStorage: "10Gi"

  config: |
    {
      "log": {
        "level": "info",
        "style": "json"
      },
      "cache_size": 1000,
      "enable_metrics": true
    }
```

### Scale Your Cluster

```bash
# Scale data nodes to 5 replicas
kubectl patch antflycluster my-database --type='merge' -p='{"spec":{"dataNodes":{"replicas":5}}}'

# Scale metadata nodes (must be odd number for Raft consensus)
kubectl patch antflycluster my-database --type='merge' -p='{"spec":{"metadataNodes":{"replicas":5}}}'
```

## 📚 Examples

The `examples/` directory contains ready-to-use configurations organized by complexity. Start with Level 1 and progress as needed:

### Level 1: Minimal Cluster
**[Development Cluster](examples/development-cluster.yaml)**: Simplest possible setup for local development and testing
```bash
kubectl apply -f examples/development-cluster.yaml
```

### Level 2: Production Configuration
**[Production Cluster](examples/production-cluster.yaml)**: Adds resource limits, larger storage allocations, and production-ready settings
```bash
kubectl apply -f examples/production-cluster.yaml
```

### Level 3: Autoscaling
**[Autoscaling Cluster](examples/autoscaling-cluster.yaml)**: Builds on production with metrics-based autoscaling for data nodes
```bash
kubectl apply -f examples/autoscaling-cluster.yaml
```

### Level 4: Service Mesh Integration
Adds mTLS encryption and advanced traffic management:
- **[Istio Service Mesh](examples/service-mesh-istio-cluster.yaml)**: Integrated with Istio
- **[Linkerd Service Mesh](examples/service-mesh-linkerd-cluster.yaml)**: Integrated with Linkerd

```bash
# Deploy with Istio
kubectl apply -f examples/service-mesh-istio-cluster.yaml
```

### Level 5: GKE Autopilot + Spot Pods
**[GKE Spot Cluster](examples/gke-autopilot-spot-cluster.yaml)**: Cloud-optimized deployment with Spot Pods for cost savings (up to 71%)
```bash
kubectl apply -f examples/gke-autopilot-spot-cluster.yaml
```

**Additional Examples**:
- **[Small Dev Cluster](examples/small-dev-cluster.yaml)**: Small test cluster configuration
- **[GKE Autopilot Cluster](examples/gke-autopilot-cluster.yaml)**: GKE Autopilot without Spot Pods

### Service Mesh Integration

Enable automatic mTLS encryption and advanced traffic management with Istio, Linkerd, or Consul Connect:

```yaml
spec:
  serviceMesh:
    enabled: true
    annotations:
      sidecar.istio.io/inject: "true"
      traffic.sidecar.istio.io/excludeOutboundPorts: "9017,9021"
```

See [Service Mesh Guide](docs/security/service-mesh.md) for detailed configuration and troubleshooting.

### GKE Autopilot

The operator has native support for Google Kubernetes Engine (GKE) Autopilot:

```yaml
spec:
  gke:
    autopilot: true
    podDisruptionBudget:
      enabled: true
      maxUnavailable: 1

  dataNodes:
    useSpotPods: true  # Save up to 71% on compute costs
```

See [GKE Autopilot Guide](docs/cloud-platforms/gcp-gke.md) for detailed configuration.

## 💾 Backup and Restore

The operator provides declarative backup and restore through two CRDs:

### Scheduled Backups (AntflyBackup)

Create scheduled backups using a CronJob:

```yaml
apiVersion: antfly.io/v1
kind: AntflyBackup
metadata:
  name: daily-backup
spec:
  clusterRef:
    name: my-database
  schedule: "0 2 * * *"  # Daily at 2am
  destination:
    location: s3://my-bucket/antfly-backups
    credentialsSecret:
      name: s3-credentials
```

**Supported storage backends:**
- Amazon S3: `s3://bucket/path`
- Google Cloud Storage: `s3://bucket/path` (via S3-compatible API with `AWS_ENDPOINT`)
- Local filesystem: `file:///path` (for testing)

**GCS Example:**
```yaml
# Secret for GCS (using S3-compatible API)
apiVersion: v1
kind: Secret
metadata:
  name: gcs-credentials
stringData:
  AWS_ACCESS_KEY_ID: "GOOG..."      # GCS HMAC key
  AWS_SECRET_ACCESS_KEY: "..."
  AWS_ENDPOINT: "https://storage.googleapis.com"
```

### On-Demand Restore (AntflyRestore)

Restore from a backup (one-shot operation):

```yaml
apiVersion: antfly.io/v1
kind: AntflyRestore
metadata:
  name: restore-from-backup
spec:
  clusterRef:
    name: staging-cluster
  source:
    backupId: daily-backup-20250118020000
    location: s3://my-bucket/antfly-backups
    credentialsSecret:
      name: s3-credentials
  restoreMode: skip_if_exists  # or fail_if_exists, overwrite
```

**Restore modes:**
| Mode | Behavior |
|------|----------|
| `fail_if_exists` | Abort if any target table exists (default, safest) |
| `skip_if_exists` | Skip existing tables, restore the rest |
| `overwrite` | Drop and recreate existing tables (destructive!) |

**How restore works:**
1. User creates `AntflyRestore` CR
2. Operator creates a Kubernetes Job running `antfly restore`
3. Job connects to target cluster and restores data
4. Status updated to `Completed` or `Failed`
5. To re-run: delete the CR and create a new one

See `examples/backup-schedule.yaml` and `examples/restore-operation.yaml` for complete examples.

## ⚙️ Configuration

### Database Configuration

Configure Antfly database settings via the `config` field:

```yaml
config: |
  {
    "log": {
      "level": "info",
      "style": "json"
    },
    "max_shard_size_bytes": 134217728,
    "raft_snapshot_threshold": 10000,
    "raft_heartbeat_timeout": "150ms",
    "raft_election_timeout": "150ms",
    "cache_size": 1000,
    "batch_size": 100,
    "enable_metrics": true
  }
```

### Resource Requirements

#### Minimum Requirements (Development)
- **CPU**: 0.5 cores total
- **Memory**: 1GB total
- **Storage**: 2GB total

#### Production Recommendations
- **CPU**: 4+ cores total
- **Memory**: 8GB+ total
- **Storage**: 100GB+ with fast SSDs
- **Network**: Low-latency between nodes

## 📊 Monitoring

### Health Checks

Each cluster provides health endpoints:

```bash
# Check cluster health
kubectl port-forward service/my-database-public-api 8080:80
curl http://localhost:8080/health

# Check metrics
curl http://localhost:8080/metrics
```

### Logging

View database logs:

```bash
# Leader node logs
kubectl logs my-database-leader-0

# Data node logs
kubectl logs my-database-data-0
```

## 🐛 Troubleshooting

### Common Issues

#### 1. Pods Stuck in Pending
```bash
# Check storage class
kubectl get storageclass

# Check node resources
kubectl describe nodes
```

#### 2. Image Pull Errors
```bash
# Check image availability
docker pull antfly:latest

# Use correct image pull policy
kubectl patch antflycluster my-database --type='merge' -p='{"spec":{"imagePullPolicy":"IfNotPresent"}}'
```

#### 3. Service Not Accessible
```bash
# Check service status
kubectl get svc -l app=antfly

# Check endpoints
kubectl get endpoints
```

### Debug Commands

```bash
# Check cluster status
kubectl describe antflycluster my-database

# Check all resources
kubectl get all -l app=antfly

# Check events
kubectl get events --sort-by=.metadata.creationTimestamp
```

### Getting Help

- [Documentation](https://github.com/antflydb/antfly/wiki)
- [Issues](https://github.com/antflydb/antfly/issues)
- [Discussions](https://github.com/antflydb/antfly/discussions)

## ☁️ Cloud-Specific Guidance

### Generic Kubernetes (Default)

The Antfly operator is designed for cross-platform compatibility and works on any conformant Kubernetes distribution (1.20+) including:
- **Minikube** and **kind** (local development)
- **Vanilla Kubernetes** (self-managed clusters)
- **Cloud-managed Kubernetes** (GKE, EKS, AKS, and others)

All core features work without cloud-specific configuration.

### Optional: GKE Optimizations

For Google Kubernetes Engine, enable Autopilot mode and Spot Pods for cost optimization:

```yaml
spec:
  gke:
    autopilot: true
    autopilotComputeClass: "general-purpose"  # or "scale-out", "performance"
    podDisruptionBudget:
      enabled: true
      maxUnavailable: 1
  dataNodes:
    useSpotPods: true  # Up to 71% cost savings (data nodes only)
  metadataNodes:
    useSpotPods: false  # NOT recommended for metadata nodes
```

See [GKE Autopilot Guide](docs/cloud-platforms/gcp-gke.md) for complete configuration details.

### Optional: AWS EKS Notes

For Amazon Elastic Kubernetes Service:

- **Storage**: Ensure the EBS CSI driver is installed for persistent volumes
- **IRSA**: Use IAM Roles for Service Accounts for pod-level permissions (optional)
- **Node Selectors**: Target specific instance types if needed

```bash
# Verify EBS CSI driver
kubectl get csidriver ebs.csi.aws.com
```

### Optional: Azure AKS Notes

For Azure Kubernetes Service:

- **Storage**: Use Azure Disk storage classes (`managed-premium` recommended for production)
- **Managed Identity**: Configure for pod authentication (optional)
- **Availability Zones**: Distribute nodes across zones for high availability

```bash
# Check Azure storage classes
kubectl get storageclass managed-premium
```

## 🔄 Upgrading

This guide primarily focuses on fresh installations. For operator upgrade procedures and version migration, see:
- [GitHub Releases](https://github.com/antflydb/antfly/releases) for release notes and upgrade instructions
- Upgrade documentation (link will be provided when available)

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

---

For more information about Antfly database, visit the [main Antfly repository](https://github.com/antflydb/antfly).

**For developers working on the operator itself, see [DEVELOPMENT.md](docs/DEVELOPMENT.md).**
