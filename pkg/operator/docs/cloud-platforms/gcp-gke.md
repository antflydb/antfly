# GKE Autopilot Deployment Guide

This guide covers deploying Antfly clusters on Google Kubernetes Engine (GKE) Autopilot with cost optimization using Spot Pods.

## Overview

GKE Autopilot is a fully managed Kubernetes service that handles cluster infrastructure, node management, and scaling. The Antfly Operator has built-in support for GKE Autopilot including:

- **Spot Pods**: Up to 71% cost savings for fault-tolerant workloads
- **Pod Disruption Budgets**: Automatic protection during cluster maintenance
- **Compute Classes**: Optimized node selection for different workload types
- **Automatic Scaling**: Works seamlessly with GKE's node auto-provisioning

## Prerequisites

- Google Cloud account with billing enabled
- `gcloud` CLI installed and configured
- `kubectl` installed
- A GKE Autopilot cluster

## Creating a GKE Autopilot Cluster

```bash
# Set your project
gcloud config set project YOUR_PROJECT_ID

# Create an Autopilot cluster
gcloud container clusters create-auto antfly-cluster \
  --region=us-central1 \
  --release-channel=regular

# Get credentials
gcloud container clusters get-credentials antfly-cluster --region=us-central1
```

## Deploying the Operator

```bash
# Deploy the Antfly operator
kubectl apply -f https://antfly.io/antfly-operator-install.yaml

# Verify operator is running
kubectl get pods -n antfly-operator-namespace
```

## GKE Configuration

Add the `gke` section to your AntflyCluster spec:

```yaml
spec:
  gke:
    # Enable Autopilot optimizations
    autopilot: true

    # Specify compute class (optional, defaults to "Balanced")
    autopilotComputeClass: "Balanced"  # or "autopilot-spot", "Performance", "Scale-Out", "Accelerator"

    # Enable Pod Disruption Budgets (recommended)
    podDisruptionBudget:
      enabled: true
      maxUnavailable: 1  # Allow max 1 pod unavailable during maintenance
```

## Spot Pods Configuration

For **GKE Autopilot**, use compute class for Spot Pods:

```yaml
spec:
  gke:
    autopilot: true
    autopilotComputeClass: "autopilot-spot"  # Spot pods via compute class

  # Do NOT set useSpotPods when autopilot=true - they conflict
```

For **standard GKE clusters** (non-Autopilot), use `useSpotPods`:

```yaml
spec:
  gke:
    autopilot: false  # Standard GKE

  metadataNodes:
    useSpotPods: false  # NOT recommended for metadata nodes

  dataNodes:
    useSpotPods: true   # Safe for data nodes with replication
```

**Important Notes:**
- **GKE Autopilot**: Use `autopilotComputeClass` for spot pods (NOT `useSpotPods`)
- **Standard GKE**: Use `useSpotPods` field
- **Metadata nodes**: Should NOT use Spot Pods (maintain Raft consensus stability)
- **Data nodes**: Can safely use Spot Pods with proper replication (3+ replicas)
- Spot Pods can be evicted at any time with 25-second notice
- The operator sets `terminationGracePeriodSeconds: 15` automatically
- **Immutability**: `autopilot` and `autopilotComputeClass` fields cannot be changed after deployment

## Compute Classes

GKE Autopilot offers different compute classes for workload optimization:

| Compute Class | Use Case | Characteristics |
|---------------|----------|-----------------|
| `Balanced` | Default, general-purpose workloads | Standard CPU/memory ratio (default) |
| `autopilot-spot` | Cost-optimized Spot Pods | Up to 71% savings, preemptible |
| `Performance` | CPU/memory intensive | Higher limits, premium hardware |
| `Scale-Out` | Large-scale distributed workloads | Optimized for horizontal scaling |
| `Accelerator` | GPU/TPU workloads | Requires GPU resources in pod spec |
| `autopilot` | Standard Autopilot scheduling | Default Autopilot behavior |

**Default Behavior**: If `autopilotComputeClass` is not specified and `autopilot=true`, the operator defaults to `"Balanced"`.

Specify in your cluster configuration:

```yaml
spec:
  gke:
    autopilotComputeClass: "Balanced"
```

## Pod Disruption Budgets

PodDisruptionBudgets protect your cluster during GKE maintenance:

```yaml
spec:
  gke:
    podDisruptionBudget:
      enabled: true
      maxUnavailable: 1  # Recommended: allows rolling updates while maintaining availability
      # OR
      # minAvailable: 2  # Alternative: ensure minimum pods always available
```

**Best Practices:**
- Use `maxUnavailable` (recommended by Google) - automatically scales with replica count
- For 3-replica clusters, `maxUnavailable: 1` ensures 2 pods always available
- For larger clusters, adjust based on replication factor and load requirements

## Storage Configuration

GKE Autopilot uses specific storage classes:

```yaml
spec:
  storage:
    storageClass: "standard-rwo"  # Default GKE Autopilot storage class
    # OR
    # storageClass: "premium-rwo"  # For higher IOPS requirements
    metadataStorage: "1Gi"
    dataStorage: "10Gi"
```

**Available Storage Classes:**
- `standard-rwo`: Standard persistent disks (balanced pd-balanced) — **recommended for multi-AZ**
- `premium-rwo`: SSD persistent disks (pd-ssd) — **recommended for multi-AZ with high IOPS**

**Warning (GKE Standard only)**: The default `standard` StorageClass uses `Immediate` volume binding mode, which provisions PVs before pods are scheduled. This can place PVs in the wrong AZ, causing `volume node affinity conflict` errors. Always use `standard-rwo` or `premium-rwo` (CSI-based, `WaitForFirstConsumer`) for multi-AZ deployments.

GKE Autopilot defaults to `standard-rwo`, which uses `WaitForFirstConsumer` — no action needed.

## Example Deployments

### Standard GKE Autopilot Cluster

```yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: gke-antfly-cluster
  namespace: default
spec:
  image: ghcr.io/antflydb/antfly:latest

  gke:
    autopilot: true
    podDisruptionBudget:
      enabled: true
      maxUnavailable: 1

  metadataNodes:
    replicas: 3
    resources:
      cpu: "500m"
      memory: "512Mi"
      limits:
        cpu: "1000m"
        memory: "1Gi"

  dataNodes:
    replicas: 3
    resources:
      cpu: "1000m"
      memory: "2Gi"

  storage:
    storageClass: "standard-rwo"
    metadataStorage: "1Gi"
    dataStorage: "10Gi"

  config: |
    {
      "log": {
        "level": "info",
        "style": "json"
      },
      "enable_metrics": true
    }
```

### Cost-Optimized with Spot Pods

```yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: gke-spot-cluster
  namespace: default
spec:
  image: ghcr.io/antflydb/antfly:latest

  gke:
    autopilot: true
    autopilotComputeClass: "autopilot-spot"  # Use compute class for Spot Pods
    podDisruptionBudget:
      enabled: true
      maxUnavailable: 1

  metadataNodes:
    replicas: 3
    resources:
      cpu: "500m"
      memory: "512Mi"

  dataNodes:
    replicas: 5  # More replicas for better fault tolerance
    resources:
      cpu: "1000m"
      memory: "2Gi"

    # Optional: Autoscaling with Spot Pods
    autoScaling:
      enabled: true
      minReplicas: 3
      maxReplicas: 10
      targetCPUUtilizationPercentage: 70

  storage:
    storageClass: "standard-rwo"
    dataStorage: "10Gi"

  config: |
    {
      "log": {
        "level": "info",
        "style": "json"
      },
      "replication_factor": 3
    }
```

## Deploying from Examples

```bash
# Standard GKE Autopilot deployment
kubectl apply -f examples/gke-autopilot-cluster.yaml

# Cost-optimized with Spot Pods
kubectl apply -f examples/gke-autopilot-spot-cluster.yaml

# Check cluster status
kubectl get antflyclusters
kubectl describe antflycluster gke-antfly-cluster
```

## Monitoring and Verification

### Verify Spot Pod Usage

```bash
# Check if pods are running on Spot nodes
kubectl get pods -o wide -l app=antfly,role=data

# Check node labels
kubectl get nodes --show-labels | grep gke-spot
```

### Check Pod Disruption Budgets

```bash
# List PDBs
kubectl get pdb

# Check PDB details
kubectl describe pdb <cluster-name>-data-pdb
kubectl describe pdb <cluster-name>-metadata-pdb
```

### Monitor Resource Usage

```bash
# View pod resource usage
kubectl top pods -l app=antfly

# View node resource usage
kubectl top nodes
```

## Cost Optimization Tips

1. **Use Spot Pods for Data Nodes**: Save up to 71% on compute costs
2. **Right-size Resources**: GKE Autopilot charges for actual resource requests
3. **Use Standard Storage**: Unless you need high IOPS, standard-rwo is more cost-effective
4. **Enable Autoscaling**: Scale down during low-traffic periods
5. **Use Scale-Out Compute Class**: For high replica counts, consider `Scale-Out` class

## Migration from useSpotPods to Compute Classes

If you have existing clusters using `useSpotPods` on GKE Autopilot, migrate to the compute class approach:

### Before (Old Configuration - May Not Work)

```yaml
spec:
  gke:
    autopilot: true
  dataNodes:
    useSpotPods: true  # This conflicts with Autopilot mode
```

### After (New Configuration)

```yaml
spec:
  gke:
    autopilot: true
    autopilotComputeClass: "autopilot-spot"  # Use compute class instead
  # Do NOT set useSpotPods when autopilot=true
```

### Migration Steps

1. **For existing clusters**: Delete and recreate (fields are immutable)
   ```bash
   kubectl delete antflycluster my-cluster
   # Update manifest to use autopilotComputeClass
   kubectl apply -f updated-cluster.yaml
   ```

2. **For new clusters**: Use compute classes from the start

3. **Validation**: The operator will reject configurations that mix `autopilot=true` with `useSpotPods=true`

**Note**: `useSpotPods` still works for standard GKE clusters (non-Autopilot).

## Troubleshooting

### Pods Pending with "Unschedulable"

GKE Autopilot automatically provisions nodes, but it may take 2-5 minutes:

```bash
# Check pod events
kubectl describe pod <pod-name>

# Check cluster autoscaling status
kubectl get events --sort-by=.metadata.creationTimestamp
```

### Spot Pod Evictions

Spot Pods can be evicted with 25-second notice:

```bash
# Check pod eviction events
kubectl get events --field-selector reason=Evicted

# Verify data node count remains adequate
kubectl get pods -l app=antfly,role=data
```

The operator automatically handles evictions by:
- Maintaining minimum replica count
- Restarting evicted pods on available nodes
- Using PodDisruptionBudgets to limit concurrent evictions

### Storage Class Issues

```bash
# List available storage classes
kubectl get storageclass

# Verify PVC binding
kubectl get pvc -l app=antfly
kubectl describe pvc <pvc-name>
```

### Configuration Validation Errors

If the webhook rejects your configuration:

```bash
# Check webhook logs
kubectl logs -n antfly-operator-namespace -l app.kubernetes.io/name=antfly-operator | grep -i webhook

# Common errors:
# - "useSpotPods conflicts with autopilot=true"
# - "autopilotComputeClass requires autopilot=true"
# - "invalid compute class value"
```

## Best Practices

1. **Always Enable PodDisruptionBudgets**: Protects against excessive disruption during maintenance
2. **Don't Use Spot Pods for Metadata Nodes**: Raft consensus requires stable metadata nodes
3. **Maintain 3+ Data Replicas with Spot**: Ensures availability during evictions
4. **Set Appropriate Resource Requests**: GKE Autopilot bills based on requests
5. **Use Resource Limits**: Prevents pods from consuming excessive resources
6. **Monitor Spot Eviction Rates**: High eviction rates may indicate undersized cluster
7. **Test Failover**: Verify cluster handles Spot evictions gracefully

## Pricing Comparison

Example pricing for `us-central1` region (estimates):

| Configuration | Monthly Cost* | Savings |
|---------------|---------------|---------|
| Standard (no Spot) | $250 | Baseline |
| Data nodes on Spot | $100 | 60% |
| All nodes on Spot (not recommended) | $72 | 71% (risky) |

*Estimates for 3 metadata + 5 data nodes, standard resources

## Workload Identity

For accessing Google Cloud services (GCS backups), use Workload Identity instead of static credentials:

```bash
# Create GCP service account
gcloud iam service-accounts create antfly-backup-sa \
  --project=YOUR_PROJECT_ID

# Grant permissions
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:antfly-backup-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

# Bind to Kubernetes service account
gcloud iam service-accounts add-iam-policy-binding \
  antfly-backup-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com \
  --role="roles/iam.workloadIdentityUser" \
  --member="serviceAccount:YOUR_PROJECT_ID.svc.id.goog[NAMESPACE/K8S_SA_NAME]"
```

Then reference in your cluster:

```yaml
spec:
  serviceAccountName: antfly-k8s-sa  # Kubernetes SA bound to GCP SA
```

## Additional Resources

- [GKE Autopilot Overview](https://cloud.google.com/kubernetes-engine/docs/concepts/autopilot-overview)
- [Spot Pods Documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/autopilot-spot-pods)
- [Pod Disruption Budgets](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-disruption-readiness)
- [Compute Classes](https://cloud.google.com/kubernetes-engine/docs/how-to/autopilot-compute-classes)
- [GKE Autopilot Troubleshooting](https://cloud.google.com/kubernetes-engine/docs/troubleshooting/autopilot-clusters)
