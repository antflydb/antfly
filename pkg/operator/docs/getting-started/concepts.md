# Core Concepts

This guide explains the architecture and key concepts of the Antfly Operator.

## Architecture Overview

The Antfly database uses a two-tier architecture with separate node types for coordination and data storage:

```
                         ┌─────────────────────────────────────────────────┐
                         │              Kubernetes Cluster                 │
                         │                                                 │
┌─────────────┐          │  ┌──────────────────────────────────────────┐  │
│   Clients   │◄─────────┼──│           Public API Service             │  │
└─────────────┘          │  │         (LoadBalancer/NodePort)          │  │
                         │  └──────────────────┬───────────────────────┘  │
                         │                     │                          │
                         │                     ▼                          │
                         │  ┌──────────────────────────────────────────┐  │
                         │  │         Metadata Nodes (StatefulSet)     │  │
                         │  │  ┌───────┐  ┌───────┐  ┌───────┐        │  │
                         │  │  │Node 0 │◄─│Node 1 │◄─│Node 2 │        │  │
                         │  │  │Leader │  │Follower│ │Follower│        │  │
                         │  │  └───┬───┘  └───┬───┘  └───┬───┘        │  │
                         │  │      │  Raft    │         │             │  │
                         │  │      │Consensus │         │             │  │
                         │  └──────┼──────────┼─────────┼─────────────┘  │
                         │         │          │         │                │
                         │         ▼          ▼         ▼                │
                         │  ┌──────────────────────────────────────────┐  │
                         │  │          Data Nodes (StatefulSet)        │  │
                         │  │  ┌───────┐  ┌───────┐  ┌───────┐  ...   │  │
                         │  │  │Node 0 │  │Node 1 │  │Node 2 │        │  │
                         │  │  └───────┘  └───────┘  └───────┘        │  │
                         │  │         Data Replication                 │  │
                         │  └──────────────────────────────────────────┘  │
                         │                                                 │
                         └─────────────────────────────────────────────────┘
```

## Node Types

### Metadata Nodes

Metadata nodes handle cluster coordination and client requests:

| Responsibility | Description |
|----------------|-------------|
| Raft Consensus | Maintain cluster state consistency |
| Client API | Handle client connections and queries |
| Cluster Coordination | Manage data node membership |
| Schema Management | Store table and index definitions |

**Key Characteristics:**
- **Fixed replica count**: Always 3 or 5 (odd number for Raft quorum)
- **Not autoscaled**: Replica count is static for consensus stability
- **Not recommended for Spot Pods**: Raft leader stability is critical
- **Ports**: 12377 (API), 9017 (Raft), 4200 (Health)

### Data Nodes

Data nodes store and replicate actual data:

| Responsibility | Description |
|----------------|-------------|
| Data Storage | Store table data on persistent volumes |
| Replication | Replicate data across nodes |
| Query Processing | Execute queries on local data |

**Key Characteristics:**
- **Autoscalable**: Can scale based on CPU/memory metrics
- **Spot-compatible**: Safe to use Spot Pods/Instances with 3+ replicas
- **Horizontal scaling**: Add nodes to increase capacity
- **Ports**: 12380 (API), 9021 (Raft), 4200 (Health)

## Custom Resource Definitions (CRDs)

The operator manages three CRDs:

### AntflyCluster

The primary resource defining a database cluster:

```yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: my-cluster
spec:
  image: ghcr.io/antflydb/antfly:latest
  metadataNodes:
    replicas: 3
    resources: {...}
  dataNodes:
    replicas: 3
    resources: {...}
  storage: {...}
  config: |
    {...}
```

### AntflyBackup

Defines scheduled backup operations:

```yaml
apiVersion: antfly.io/v1
kind: AntflyBackup
metadata:
  name: daily-backup
spec:
  clusterRef:
    name: my-cluster
  schedule: "0 2 * * *"  # Daily at 2am
  destination:
    location: s3://my-bucket/backups
```

### AntflyRestore

Defines restore operations:

```yaml
apiVersion: antfly.io/v1
kind: AntflyRestore
metadata:
  name: restore-from-backup
spec:
  clusterRef:
    name: my-cluster
  source:
    backupId: "backup-20250101-020000"
    location: s3://my-bucket/backups
```

## Kubernetes Resources Created

When you create an AntflyCluster, the operator creates:

| Resource | Name Pattern | Purpose |
|----------|--------------|---------|
| StatefulSet | `{cluster}-metadata` | Metadata nodes |
| StatefulSet | `{cluster}-data` | Data nodes |
| Service | `{cluster}-metadata` | Internal metadata service |
| Service | `{cluster}-data` | Internal data service |
| Service | `{cluster}-public-api` | External API service |
| ConfigMap | `{cluster}-config` | Antfly configuration |
| PVCs | `data-{cluster}-*-{n}` | Persistent storage |
| PDB | `{cluster}-metadata-pdb` | Pod disruption budget (if enabled) |
| PDB | `{cluster}-data-pdb` | Pod disruption budget (if enabled) |

## Reconciliation Loop

The operator continuously reconciles the desired state with actual state:

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Reconciliation Loop                           │
│                                                                      │
│  ┌─────────┐    ┌─────────────┐    ┌─────────────┐    ┌──────────┐ │
│  │  Watch  │───►│   Compare   │───►│   Update    │───►│  Status  │ │
│  │ Events  │    │   Desired   │    │  Resources  │    │  Update  │ │
│  │         │    │  vs Actual  │    │             │    │          │ │
│  └─────────┘    └─────────────┘    └─────────────┘    └──────────┘ │
│       ▲                                                      │      │
│       └──────────────────────────────────────────────────────┘      │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

**Reconciliation Steps:**
1. Apply default values to cluster spec
2. Reconcile ConfigMap (Antfly configuration)
3. Reconcile Services (internal + public API)
4. Reconcile Metadata StatefulSet
5. Reconcile Data StatefulSet
6. Evaluate autoscaling (if enabled)
7. Update cluster status

## Status and Conditions

The cluster status provides operational information:

```yaml
status:
  phase: Running
  metadataNodesReady: 3
  dataNodesReady: 3
  conditions:
    - type: ConfigurationValid
      status: "True"
    - type: SecretsReady
      status: "True"
  autoScalingStatus:
    currentReplicas: 3
    desiredReplicas: 3
```

### Phases

| Phase | Description |
|-------|-------------|
| Pending | Cluster is being created |
| Running | All nodes are ready |
| Degraded | Some nodes are not ready |
| Failed | Critical error |

### Condition Types

| Condition | Description |
|-----------|-------------|
| ConfigurationValid | Configuration passes validation |
| SecretsReady | Referenced secrets exist |
| ServiceMeshReady | Service mesh sidecars injected (if enabled) |

## Port Defaults

The operator uses these default ports:

| Service | Port | Protocol |
|---------|------|----------|
| Metadata API | 12377 | TCP |
| Metadata Raft | 9017 | TCP |
| Data API | 12380 | TCP |
| Data Raft | 9021 | TCP |
| Health Check | 4200 | HTTP |
| Public API | 80 | TCP |

## Health Checks

The operator configures health probes for all pods:

| Probe | Endpoint | Purpose |
|-------|----------|---------|
| Startup | `:4200/healthz` | Allow slow starts |
| Liveness | `:4200/healthz` | Restart unhealthy pods |
| Readiness | `:4200/readyz` | Traffic routing |

**Probe Configuration:**
- Startup: 30s initial delay, 10s period, 30 failure threshold
- Liveness: 15s period, 3 failure threshold
- Readiness: 5s period, 5 failure threshold

## Configuration

Antfly configuration is passed via `spec.config` as JSON:

```yaml
spec:
  config: |
    {
      "log": {
        "level": "info",
        "style": "json"
      },
      "enable_metrics": true,
      "replication_factor": 3
    }
```

The operator:
1. Parses the user-provided JSON
2. Merges with auto-generated network configuration
3. Stores in a ConfigMap
4. Mounts at `/config/config.json` in all pods

## Scaling

### Manual Scaling

Update the replica count in the spec:

```yaml
spec:
  dataNodes:
    replicas: 5  # Changed from 3
```

### Autoscaling

Enable metrics-based autoscaling for data nodes:

```yaml
spec:
  dataNodes:
    replicas: 3
    autoScaling:
      enabled: true
      minReplicas: 3
      maxReplicas: 10
      targetCPUUtilizationPercentage: 70
```

**Autoscaling Behavior:**
- Metrics collected every 30 seconds
- Scale-up: Max 50% increase or +2 replicas
- Scale-down: Max 25% decrease or -1 replica
- Cooldown periods prevent flapping

See [Autoscaling](../operations/autoscaling.md) for details.

## Storage

Each node gets a Persistent Volume Claim:

```yaml
spec:
  storage:
    storageClass: "standard"    # Use cluster default
    metadataStorage: "1Gi"      # Per metadata node
    dataStorage: "10Gi"         # Per data node
```

**Important Notes:**
- PVCs are retained when pods restart
- PVCs are retained when scaling down (data is preserved)
- Storage class must support dynamic provisioning

## Cloud Provider Integration

### GKE Autopilot

```yaml
spec:
  gke:
    autopilot: true
    autopilotComputeClass: "Balanced"
    podDisruptionBudget:
      enabled: true
```

See [GKE Guide](../cloud-platforms/gcp-gke.md) for details.

### AWS EKS

```yaml
spec:
  eks:
    enabled: true
    useSpotInstances: true
    ebsVolumeType: "gp3"
    irsaRoleARN: "arn:aws:iam::123456789:role/antfly"
```

See [EKS Guide](../cloud-platforms/aws-eks.md) for details.

## Next Steps

- [Installation](installation.md): Install the operator
- [Quickstart](quickstart.md): Deploy your first cluster
- [API Reference](../reference/antflycluster-api.md): Complete spec reference
