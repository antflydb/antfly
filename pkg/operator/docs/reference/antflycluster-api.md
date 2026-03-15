# AntflyCluster API Reference

Complete API reference for the AntflyCluster custom resource.

## Overview

```yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: my-cluster
  namespace: default
spec:
  # ... spec fields
status:
  # ... status fields (read-only)
```

## Spec

### Top-Level Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `image` | string | Yes | Container image for Antfly (e.g., `ghcr.io/antflydb/antfly:latest`) |
| `imagePullPolicy` | string | No | Image pull policy (`Always`, `IfNotPresent`, `Never`) |
| `metadataNodes` | [MetadataNodesSpec](#metadatanodesspec) | Yes | Metadata node configuration |
| `dataNodes` | [DataNodesSpec](#datanodesspec) | Yes | Data node configuration |
| `storage` | [StorageSpec](#storagespec) | Yes | Storage configuration |
| `config` | string | Yes | Antfly configuration (JSON) |
| `gke` | [GKESpec](#gkespec) | No | GKE-specific configuration |
| `eks` | [EKSSpec](#eksspec) | No | AWS EKS-specific configuration |
| `serviceMesh` | [ServiceMeshSpec](#servicemeshspec) | No | Service mesh configuration |
| `publicAPI` | [PublicAPIConfig](#publicapiconfig) | No | Public API service configuration |
| `serviceAccountName` | string | No | Kubernetes ServiceAccount for pods |

### MetadataNodesSpec

Configuration for metadata nodes (Raft consensus, API coordination).

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `replicas` | int32 | No | 3 | Number of metadata nodes |
| `resources` | [ResourceSpec](#resourcespec) | Yes | - | Resource requirements |
| `metadataAPI` | [APISpec](#apispec) | Yes | - | Metadata API configuration |
| `metadataRaft` | [APISpec](#apispec) | Yes | - | Metadata Raft configuration |
| `health` | [APISpec](#apispec) | No | port: 4200 | Health check endpoint |
| `useSpotPods` | bool | No | false | Use GKE Spot Pods (standard GKE only) |
| `envFrom` | []EnvFromSource | No | - | Environment variables from secrets/configmaps |
| `tolerations` | []Toleration | No | - | Pod scheduling tolerations |
| `nodeSelector` | map[string]string | No | - | Node selector labels for scheduling |
| `affinity` | *Affinity | No | - | Pod affinity/anti-affinity rules |
| `topologySpreadConstraints` | []TopologySpreadConstraint | No | - | Pod topology spread constraints |

**Notes**:
- `replicas` should be an odd number (3 or 5) for Raft quorum
- `useSpotPods` must be false when `spec.gke.autopilot=true`
- `nodeSelector` must not be set when `spec.gke.autopilot=true` (Autopilot uses compute classes)
- Scheduling fields (`tolerations`, `nodeSelector`, `affinity`, `topologySpreadConstraints`) are merged with cloud-provider-specific values (e.g., EKS Spot tolerations)
- Metadata nodes are never autoscaled

### DataNodesSpec

Configuration for data nodes (data storage, replication).

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `replicas` | int32 | No | 3 | Initial number of data nodes |
| `autoScaling` | [AutoScalingSpec](#autoscalingspec) | No | - | Autoscaling configuration |
| `resources` | [ResourceSpec](#resourcespec) | Yes | - | Resource requirements |
| `api` | [APISpec](#apispec) | Yes | - | Data API configuration |
| `raft` | [APISpec](#apispec) | Yes | - | Data Raft configuration |
| `health` | [APISpec](#apispec) | No | port: 4200 | Health check endpoint |
| `useSpotPods` | bool | No | false | Use GKE Spot Pods (standard GKE only) |
| `envFrom` | []EnvFromSource | No | - | Environment variables from secrets/configmaps |
| `tolerations` | []Toleration | No | - | Pod scheduling tolerations |
| `nodeSelector` | map[string]string | No | - | Node selector labels for scheduling |
| `affinity` | *Affinity | No | - | Pod affinity/anti-affinity rules |
| `topologySpreadConstraints` | []TopologySpreadConstraint | No | - | Pod topology spread constraints |

### APISpec

Port and host configuration.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `port` | int32 | No | varies | Port number |
| `host` | string | No | 0.0.0.0 | Host to bind to |

**Default Ports**:
- Metadata API: 12377
- Metadata Raft: 9017
- Data API: 12380
- Data Raft: 9021
- Health: 4200

### ResourceSpec

Resource requirements and limits.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `cpu` | string | No | CPU request (e.g., "500m") |
| `memory` | string | No | Memory request (e.g., "512Mi") |
| `limits` | [ResourceLimits](#resourcelimits) | Yes | Resource limits |

### ResourceLimits

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `cpu` | string | No | CPU limit (e.g., "1000m") |
| `memory` | string | No | Memory limit (e.g., "1Gi") |

### AutoScalingSpec

Autoscaling configuration for data nodes.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | bool | Yes | - | Enable autoscaling |
| `minReplicas` | int32 | Yes | - | Minimum replicas |
| `maxReplicas` | int32 | Yes | - | Maximum replicas |
| `targetCPUUtilizationPercentage` | *int32 | No | - | Target CPU utilization |
| `targetMemoryUtilizationPercentage` | *int32 | No | - | Target memory utilization |
| `scaleUpCooldown` | *duration | No | 60s | Cooldown between scale-up operations |
| `scaleDownCooldown` | *duration | No | 300s | Cooldown between scale-down operations |

**Notes**:
- At least one of `targetCPUUtilizationPercentage` or `targetMemoryUtilizationPercentage` must be set
- Requires metrics-server and resource requests on pods

### StorageSpec

Storage configuration.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `storageClass` | string | No | Storage class name |
| `metadataStorage` | string | No | Storage size for metadata nodes (e.g., "1Gi") |
| `dataStorage` | string | No | Storage size for data nodes (e.g., "10Gi") |

### GKESpec

GKE-specific configuration.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `autopilot` | bool | No | false | Enable GKE Autopilot optimizations |
| `autopilotComputeClass` | string | No | "Balanced" | Autopilot compute class |
| `podDisruptionBudget` | [PodDisruptionBudgetSpec](#poddisruptionbudgetspec) | No | - | PDB configuration |

**Valid `autopilotComputeClass` values**:
- `Accelerator` - GPU/TPU workloads
- `Balanced` - General-purpose (default)
- `Performance` - CPU/memory intensive
- `Scale-Out` - Distributed workloads
- `autopilot` - Default Autopilot behavior
- `autopilot-spot` - Spot Pods

**Immutable fields**: `autopilot` and `autopilotComputeClass` cannot be changed after creation.

### EKSSpec

AWS EKS-specific configuration.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | bool | No | false | Enable EKS optimizations |
| `useSpotInstances` | bool | No | false | Use Spot Instances for data nodes |
| `instanceTypes` | []string | No | - | Preferred EC2 instance types |
| `irsaRoleARN` | string | No | - | IAM role ARN for IRSA |
| `ebsVolumeType` | string | No | gp3 | EBS volume type |
| `ebsEncrypted` | bool | No | false | Enable EBS encryption |
| `ebsKmsKeyId` | string | No | - | KMS key for encryption |
| `ebsIOPs` | *int32 | No | - | Provisioned IOPS (io1/io2 only) |
| `ebsThroughput` | *int32 | No | - | Throughput in MiB/s (gp3 only) |
| `podDisruptionBudget` | [PodDisruptionBudgetSpec](#poddisruptionbudgetspec) | No | - | PDB configuration |

**Valid `ebsVolumeType` values**: `gp3`, `gp2`, `io1`, `io2`, `st1`, `sc1`

**IRSA ARN format**: `arn:aws(-cn|-us-gov)?:iam::\d{12}:role/.+`

**Immutable fields**: `enabled` cannot be changed after creation.

### PodDisruptionBudgetSpec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | bool | Yes | - | Enable PDB creation |
| `maxUnavailable` | *int32 | No | 1 | Max unavailable pods |
| `minAvailable` | *int32 | No | - | Min available pods |

**Note**: Specify either `maxUnavailable` or `minAvailable`, not both.

### ServiceMeshSpec

Service mesh configuration.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | bool | No | false | Enable service mesh integration |
| `annotations` | map[string]string | No | - | Mesh-specific annotations |

### PublicAPIConfig

Public API service configuration.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | *bool | No | false | Create public API service |
| `serviceType` | *ServiceType | No | LoadBalancer | Service type |
| `port` | int32 | No | 80 | Service port |
| `nodePort` | *int32 | No | - | Node port (NodePort type only) |

**Valid `serviceType` values**: `ClusterIP`, `NodePort`, `LoadBalancer`

## Status

The status section is read-only and managed by the operator.

### Top-Level Status Fields

| Field | Type | Description |
|-------|------|-------------|
| `phase` | string | Cluster phase (`Pending`, `Running`, `Degraded`, `Failed`) |
| `conditions` | []Condition | Current conditions |
| `metadataNodesReady` | int32 | Ready metadata node count |
| `dataNodesReady` | int32 | Ready data node count |
| `autoScalingStatus` | [AutoScalingStatus](#autoscalingstatus) | Autoscaling state |
| `serviceMeshStatus` | [ServiceMeshStatus](#servicemeshstatus) | Service mesh state |

### Conditions

| Type | Description |
|------|-------------|
| `ConfigurationValid` | Configuration validation status |
| `SecretsReady` | Referenced secrets availability |
| `ServiceMeshReady` | Service mesh sidecar injection status |

### AutoScalingStatus

| Field | Type | Description |
|-------|------|-------------|
| `currentReplicas` | int32 | Current replica count |
| `desiredReplicas` | int32 | Desired replica count |
| `lastScaleTime` | *Time | Last scaling timestamp |
| `lastScaleDirection` | string | "up", "down", or "" |
| `currentCPUUtilizationPercentage` | *int32 | Current CPU utilization |
| `currentMemoryUtilizationPercentage` | *int32 | Current memory utilization |

### ServiceMeshStatus

| Field | Type | Description |
|-------|------|-------------|
| `enabled` | bool | Service mesh enabled |
| `sidecarInjectionStatus` | string | "Complete", "Partial", "None", "Unknown" |
| `podsWithSidecars` | int32 | Pods with sidecars |
| `totalPods` | int32 | Total expected pods |
| `lastTransitionTime` | *Time | Last status change |

## Validation Rules

The operator validates configurations via admission webhook:

### GKE Validation
- `autopilotComputeClass` must be valid enum value
- `useSpotPods=true` conflicts with `autopilot=true`
- `autopilotComputeClass` requires `autopilot=true`
- `Accelerator` compute class requires GPU resources
- `autopilot` and `autopilotComputeClass` are immutable

### EKS Validation
- `irsaRoleARN` must match AWS ARN format
- `ebsVolumeType` must be valid enum value
- `ebsIOPs` only valid for io1/io2
- `ebsThroughput` only valid for gp3 (125-1000)
- `ebsKmsKeyId` requires `ebsEncrypted=true`
- Cannot enable both GKE and EKS
- `enabled` is immutable

### Scheduling Validation
- `nodeSelector` conflicts with `gke.autopilot=true` (Autopilot manages scheduling via compute classes)
- `tolerations`, `affinity`, and `topologySpreadConstraints` are allowed with all cloud providers

### General Validation
- Metadata and data replicas must be > 0
- `publicAPI.nodePort` only valid for NodePort service type

## Example

Complete example with all fields:

```yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: production-cluster
  namespace: production
spec:
  image: ghcr.io/antflydb/antfly:latest
  imagePullPolicy: IfNotPresent
  serviceAccountName: antfly-workload-sa

  metadataNodes:
    replicas: 3
    metadataAPI:
      port: 12377
    metadataRaft:
      port: 9017
    health:
      port: 4200
    resources:
      cpu: "500m"
      memory: "512Mi"
      limits:
        cpu: "1000m"
        memory: "1Gi"
    envFrom:
      - secretRef:
          name: backup-credentials
    tolerations:
      - key: "dedicated"
        operator: "Equal"
        value: "antfly"
        effect: "NoSchedule"
    nodeSelector:
      node-pool: "antfly-metadata"

  dataNodes:
    replicas: 5
    api:
      port: 12380
    raft:
      port: 9021
    health:
      port: 4200
    resources:
      cpu: "1000m"
      memory: "2Gi"
      limits:
        cpu: "2000m"
        memory: "4Gi"
    autoScaling:
      enabled: true
      minReplicas: 5
      maxReplicas: 20
      targetCPUUtilizationPercentage: 70
      targetMemoryUtilizationPercentage: 80
      scaleUpCooldown: 60s
      scaleDownCooldown: 300s
    envFrom:
      - secretRef:
          name: backup-credentials
    tolerations:
      - key: "dedicated"
        operator: "Equal"
        value: "antfly"
        effect: "NoSchedule"
    nodeSelector:
      node-pool: "antfly-data"
    topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: topology.kubernetes.io/zone
        whenUnsatisfiable: ScheduleAnyway
        labelSelector:
          matchLabels:
            app.kubernetes.io/name: antfly-database
            app.kubernetes.io/component: data

  storage:
    storageClass: "premium-rwo"
    metadataStorage: "5Gi"
    dataStorage: "50Gi"

  gke:
    autopilot: true
    autopilotComputeClass: "Balanced"
    podDisruptionBudget:
      enabled: true
      maxUnavailable: 1

  serviceMesh:
    enabled: true
    annotations:
      sidecar.istio.io/inject: "true"
      traffic.sidecar.istio.io/excludeOutboundPorts: "9017,9021"

  publicAPI:
    enabled: true
    serviceType: LoadBalancer
    port: 80

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

## See Also

- [Concepts](../getting-started/concepts.md): Architecture overview
- [AntflyBackup API](antflybackup-api.md): Backup API reference
- [AntflyRestore API](antflyrestore-api.md): Restore API reference
