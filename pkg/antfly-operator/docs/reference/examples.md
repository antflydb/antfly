# Example Configurations

This page indexes all example YAML configurations in the `examples/` directory.

## Cluster Examples

### Development Clusters

| File | Description | Use Case |
|------|-------------|----------|
| [`small-dev-cluster.yaml`](#small-dev-cluster) | Minimal resource cluster | Local development, quick testing |
| [`development-cluster.yaml`](#development-cluster) | Development cluster with debug logging | Feature development, debugging |

### Production Clusters

| File | Description | Use Case |
|------|-------------|----------|
| [`production-cluster.yaml`](#production-cluster) | Production-ready configuration | Production deployments |
| [`autoscaling-cluster.yaml`](#autoscaling-cluster) | With autoscaling enabled | Variable workloads |

### Cloud-Specific

| File | Description | Use Case |
|------|-------------|----------|
| [`gke-autopilot-cluster.yaml`](#gke-autopilot) | GKE Autopilot deployment | GKE Autopilot clusters |
| [`gke-autopilot-spot-cluster.yaml`](#gke-autopilot-spot) | GKE with Spot Pods | Cost-optimized GKE |
| [`eks-cluster.yaml`](#eks-basic) | Basic EKS deployment | AWS EKS clusters |
| [`eks-spot-cluster.yaml`](#eks-spot) | EKS with Spot Instances | Cost-optimized EKS |
| [`eks-irsa-cluster.yaml`](#eks-irsa) | EKS with IRSA for backups | Secure AWS integration |

### Features

| File | Description | Use Case |
|------|-------------|----------|
| [`service-mesh-istio-cluster.yaml`](#istio) | Istio service mesh | mTLS encryption |
| [`service-mesh-linkerd-cluster.yaml`](#linkerd) | Linkerd service mesh | Lightweight mTLS |
| [`public-api-configurations.yaml`](#public-api) | Public API service options | Service exposure |
| [`cluster-with-backup-credentials.yaml`](#backup-creds) | Backup credentials via envFrom | S3/GCS backups |

### Backup and Restore

| File | Description | Use Case |
|------|-------------|----------|
| [`backup-schedule.yaml`](#backup-schedule) | Scheduled backup configuration | Regular backups |
| [`restore-operation.yaml`](#restore) | Restore from backup | Data recovery |

---

## Example Details

### small-dev-cluster

Minimal resources for local development with minikube or kind.

```yaml
# examples/small-dev-cluster.yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: dev-cluster
spec:
  image: ghcr.io/antflydb/antfly:latest
  metadataNodes:
    replicas: 1
    resources:
      cpu: "100m"
      memory: "128Mi"
  dataNodes:
    replicas: 1
    resources:
      cpu: "100m"
      memory: "256Mi"
  storage:
    metadataStorage: "500Mi"
    dataStorage: "1Gi"
```

**When to use**: Quick testing, CI pipelines, local development

---

### development-cluster

Development cluster with verbose logging.

```yaml
# examples/development-cluster.yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: dev-cluster
spec:
  image: ghcr.io/antflydb/antfly:latest
  metadataNodes:
    replicas: 3
    resources:
      cpu: "250m"
      memory: "256Mi"
  dataNodes:
    replicas: 3
    resources:
      cpu: "500m"
      memory: "512Mi"
  config: |
    {
      "log": {"level": "debug", "style": "terminal"},
      "enable_debug_endpoints": true
    }
```

**When to use**: Feature development, debugging issues

---

### production-cluster

Production-ready configuration with proper resources.

```yaml
# examples/production-cluster.yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: prod-cluster
spec:
  image: ghcr.io/antflydb/antfly:latest
  metadataNodes:
    replicas: 3
    resources:
      cpu: "500m"
      memory: "512Mi"
      limits:
        cpu: "1000m"
        memory: "1Gi"
  dataNodes:
    replicas: 5
    resources:
      cpu: "1000m"
      memory: "2Gi"
      limits:
        cpu: "2000m"
        memory: "4Gi"
  storage:
    storageClass: "premium-rwo"
    metadataStorage: "5Gi"
    dataStorage: "50Gi"
  config: |
    {
      "log": {"level": "info", "style": "json"},
      "enable_metrics": true,
      "replication_factor": 3
    }
```

**When to use**: Production deployments

---

### autoscaling-cluster

Cluster with autoscaling enabled.

```yaml
# examples/autoscaling-cluster.yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: autoscaling-cluster
spec:
  image: ghcr.io/antflydb/antfly:latest
  dataNodes:
    replicas: 3
    resources:
      cpu: "500m"
      memory: "1Gi"
    autoScaling:
      enabled: true
      minReplicas: 3
      maxReplicas: 10
      targetCPUUtilizationPercentage: 70
      targetMemoryUtilizationPercentage: 80
```

**When to use**: Workloads with variable traffic patterns

---

### gke-autopilot

GKE Autopilot deployment.

```yaml
# examples/gke-autopilot-cluster.yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: gke-cluster
spec:
  image: ghcr.io/antflydb/antfly:latest
  gke:
    autopilot: true
    autopilotComputeClass: "Balanced"
    podDisruptionBudget:
      enabled: true
      maxUnavailable: 1
  storage:
    storageClass: "standard-rwo"
```

**When to use**: GKE Autopilot clusters

---

### gke-autopilot-spot

GKE with Spot Pods for cost savings.

```yaml
# examples/gke-autopilot-spot-cluster.yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: gke-spot-cluster
spec:
  image: ghcr.io/antflydb/antfly:latest
  gke:
    autopilot: true
    autopilotComputeClass: "autopilot-spot"
    podDisruptionBudget:
      enabled: true
  dataNodes:
    replicas: 5  # Extra replicas for Spot tolerance
```

**When to use**: Cost-optimized GKE deployments

---

### eks-basic

Basic AWS EKS deployment.

```yaml
# examples/eks-cluster.yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: eks-cluster
spec:
  image: ghcr.io/antflydb/antfly:latest
  eks:
    enabled: true
    ebsVolumeType: "gp3"
    ebsEncrypted: true
    podDisruptionBudget:
      enabled: true
```

**When to use**: AWS EKS clusters

---

### eks-spot

EKS with Spot Instances.

```yaml
# examples/eks-spot-cluster.yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: eks-spot-cluster
spec:
  eks:
    enabled: true
    useSpotInstances: true
    instanceTypes: ["m5.large", "m5.xlarge"]
  dataNodes:
    replicas: 5
```

**When to use**: Cost-optimized EKS deployments

---

### eks-irsa

EKS with IRSA for secure S3 access.

```yaml
# examples/eks-irsa-cluster.yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: eks-irsa-cluster
spec:
  serviceAccountName: antfly-sa
  eks:
    enabled: true
    irsaRoleARN: "arn:aws:iam::123456789012:role/antfly-backup-role"
```

**When to use**: EKS with S3 backups using IRSA

---

### istio

Istio service mesh integration.

```yaml
# examples/service-mesh-istio-cluster.yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: istio-cluster
spec:
  serviceMesh:
    enabled: true
    annotations:
      sidecar.istio.io/inject: "true"
      traffic.sidecar.istio.io/excludeOutboundPorts: "9017,9021"
```

**When to use**: Istio service mesh environments

---

### linkerd

Linkerd service mesh integration.

```yaml
# examples/service-mesh-linkerd-cluster.yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: linkerd-cluster
spec:
  serviceMesh:
    enabled: true
    annotations:
      linkerd.io/inject: enabled
      config.linkerd.io/skip-outbound-ports: "9017,9021"
```

**When to use**: Linkerd service mesh environments

---

### public-api

Various public API service configurations.

```yaml
# examples/public-api-configurations.yaml
# LoadBalancer (default, cloud environments)
publicAPI:
  serviceType: LoadBalancer
  port: 80

# NodePort (on-premises, bare metal)
publicAPI:
  serviceType: NodePort
  nodePort: 30100

# ClusterIP (internal only, use with Ingress)
publicAPI:
  serviceType: ClusterIP

# Disabled (custom networking)
publicAPI:
  enabled: false
```

**When to use**: Customizing external access

---

### backup-creds

Cluster with backup credentials injected.

```yaml
# examples/cluster-with-backup-credentials.yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: backup-enabled-cluster
spec:
  metadataNodes:
    envFrom:
      - secretRef:
          name: backup-credentials
  dataNodes:
    envFrom:
      - secretRef:
          name: backup-credentials
```

**When to use**: Clusters that need backup credentials

---

### backup-schedule

Scheduled backup configuration.

```yaml
# examples/backup-schedule.yaml
apiVersion: antfly.io/v1
kind: AntflyBackup
metadata:
  name: daily-backup
spec:
  clusterRef:
    name: my-cluster
  schedule: "0 2 * * *"
  destination:
    location: s3://my-bucket/backups
    credentialsSecret:
      name: backup-credentials
  successfulJobsHistoryLimit: 7
```

**When to use**: Setting up scheduled backups

---

### restore

Restore from backup.

```yaml
# examples/restore-operation.yaml
apiVersion: antfly.io/v1
kind: AntflyRestore
metadata:
  name: restore-latest
spec:
  clusterRef:
    name: my-cluster
  source:
    backupId: "backup-20250115-020000"
    location: s3://my-bucket/backups
    credentialsSecret:
      name: backup-credentials
  restoreMode: fail_if_exists
```

**When to use**: Restoring data from backups

---

## Applying Examples

```bash
# Apply an example
kubectl apply -f examples/small-dev-cluster.yaml

# Apply from URL
kubectl apply -f https://raw.githubusercontent.com/antflydb/antfly/main/pkg/antfly-operator/examples/small-dev-cluster.yaml

# Apply with customizations
kubectl apply -f - <<EOF
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: custom-cluster
  namespace: my-namespace
spec:
  # Copy and customize from examples
EOF
```

## See Also

- [Quickstart](../getting-started/quickstart.md): Deploy your first cluster
- [AntflyCluster API](antflycluster-api.md): Complete API reference
- [AntflyBackup API](antflybackup-api.md): Backup API reference
- [AntflyRestore API](antflyrestore-api.md): Restore API reference
