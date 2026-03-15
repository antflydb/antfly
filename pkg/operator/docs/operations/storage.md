# Storage Operations

Configure PVC retention, volume expansion, and storage lifecycle management for Antfly clusters.

## PVC Retention Policy

Control what happens to PersistentVolumeClaims when an AntflyCluster is deleted or scaled down.

```yaml
spec:
  storage:
    storageClass: "gp3"
    metadataStorage: "5Gi"
    dataStorage: "50Gi"
    pvcRetentionPolicy:
      whenDeleted: Delete   # Delete PVCs when the AntflyCluster is deleted
      whenScaled: Retain    # Keep PVCs when scaling down (default)
```

### Fields

| Field | Values | Default | Description |
|-------|--------|---------|-------------|
| `whenDeleted` | `Retain`, `Delete` | `Retain` | What happens to PVCs when the AntflyCluster CR is deleted |
| `whenScaled` | `Retain`, `Delete` | `Retain` | What happens to PVCs when StatefulSet replicas decrease |

### `whenDeleted`

- **`Retain`** (default): PVCs are preserved after cluster deletion. This is the Kubernetes default behavior. You must manually delete PVCs after confirming backups or verifying you no longer need the data.
- **`Delete`**: PVCs are automatically deleted when the AntflyCluster is deleted. The operator adds a `antfly.io/pvc-cleanup` finalizer to the cluster and performs ordered cleanup: StatefulSets are deleted first, then pods are waited on, then PVCs are removed.

### `whenScaled`

- **`Retain`** (default): PVCs are preserved when scaling down. If a node later scales back up to the same ordinal, it reuses the existing PVC with its data intact.
- **`Delete`**: PVCs are deleted when a pod is removed due to scale-down.

**Warning**: `whenScaled: Delete` causes a **full Raft snapshot resync** for every shard when a node rejoins after scale-up. When a data node's PVC is deleted on scale-down and the node later returns (scale-up reuses the same ordinal), the node starts with empty storage. Antfly's Raft layer detects the empty log directory, calls `RestartNode()` with `join=true`, and the leader sends snapshots for every shard the node hosts. This is expensive — it saturates network and disk I/O on the leader and temporarily reduces cluster fault tolerance while the resync is in progress. Only use `whenScaled: Delete` when storage costs outweigh resync costs.

### Restrictions

The validating webhook enforces these safety rules:

- **`whenScaled: Delete` is rejected when autoscaling is enabled**: The autoscaler could trigger scale-down events that permanently destroy PVCs, requiring expensive full resyncs on every scale-up.

### Kubernetes Version Compatibility

The `pvcRetentionPolicy` maps to the Kubernetes StatefulSet `PersistentVolumeClaimRetentionPolicy` feature:

| Kubernetes Version | Status |
|-------------------|--------|
| < 1.27 | Not available (silently ignored) |
| 1.27 - 1.31 | Beta, enabled by default |
| >= 1.32 | GA |

On clusters running K8s < 1.27, the StatefulSet retention policy is silently ignored. The operator's finalizer-based cleanup (`antfly.io/pvc-cleanup`) provides a fallback for `whenDeleted: Delete` on these clusters.

## Volume Expansion

The operator supports increasing PVC storage sizes. Update the storage size in the CRD spec:

```yaml
spec:
  storage:
    dataStorage: "100Gi"    # Increased from 50Gi
```

On the next reconciliation, the operator patches existing PVCs to request the larger size. The underlying StorageClass must have `allowVolumeExpansion: true`.

**Note**: Storage size decreases are rejected by the validating webhook. Kubernetes does not support shrinking PVCs.

**Note**: The `storageClass` field is immutable after creation. To change the StorageClass, delete and recreate the cluster.

## See Also

- [Troubleshooting: Storage Issues](../troubleshooting.md#storage-issues)
- [Pod Scheduling: Zone-Aware Scheduling](pod-scheduling.md#zone-aware-scheduling)
- [AWS EKS: Multi-AZ Storage](../cloud-platforms/aws-eks.md#multi-az-storage-best-practices)
- [Installation: Uninstalling](../getting-started/installation.md#uninstalling)
