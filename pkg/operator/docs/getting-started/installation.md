# Installation

This guide covers installing the Antfly Operator in your Kubernetes cluster.

## Prerequisites

Before installing the operator, ensure you have:

- **Kubernetes 1.20+** cluster running
- **kubectl** installed and configured to access your cluster
- **Storage class** with dynamic provisioning (most cloud providers include this by default)
- **Cluster admin permissions** to create CRDs and cluster-scoped resources

### Optional Prerequisites

- **metrics-server**: Required for autoscaling (most managed Kubernetes services include this)
- **Service mesh**: For mTLS encryption (Istio, Linkerd, or Consul)

## Quick Install

Install the operator with a single command:

```bash
kubectl apply -f https://antfly.io/antfly-operator-install.yaml
```

This installs:
- Custom Resource Definitions (CRDs)
- Operator namespace (`antfly-operator-namespace`)
- ServiceAccount and RBAC permissions
- Operator Deployment

## Verify Installation

Check that the operator is running:

```bash
# Check operator pod status
kubectl get pods -n antfly-operator-namespace

# Expected output:
# NAME                              READY   STATUS    RESTARTS   AGE
# antfly-operator-xxxxxxxxx-xxxxx   1/1     Running   0          30s
```

Verify CRDs are installed:

```bash
kubectl get crd | grep antfly

# Expected output:
# antflybackups.antfly.io     2025-01-15T00:00:00Z
# antflyclusters.antfly.io    2025-01-15T00:00:00Z
# antflyrestores.antfly.io    2025-01-15T00:00:00Z
# termitepools.antfly.io      2025-01-15T00:00:00Z
# termiteroutes.antfly.io     2025-01-15T00:00:00Z
```

## What Gets Installed

The installation creates:

| Resource | Name | Description |
|----------|------|-------------|
| Namespace | `antfly-operator-namespace` | Operator runs here |
| CRDs | `antflyclusters.antfly.io` | Cluster definition |
| CRDs | `antflybackups.antfly.io` | Backup schedules |
| CRDs | `antflyrestores.antfly.io` | Restore operations |
| CRDs | `termitepools.antfly.io` | Termite ML pool definition |
| CRDs | `termiteroutes.antfly.io` | Termite routing definition |
| ServiceAccount | `antfly-operator-service-account` | Operator identity |
| ClusterRole | `antfly-operator-cluster-role` | RBAC permissions |
| ClusterRoleBinding | `antfly-operator-cluster-role-binding` | Binds role to SA |
| Deployment | `antfly-operator` | Operator deployment |

## Install with Helm (Coming Soon)

Helm chart installation will be available in a future release. Track progress in [GitHub Issues](https://github.com/antflydb/antfly/issues).

## Air-Gapped Installation

For environments without internet access:

1. Download the install manifest:
   ```bash
   curl -LO https://antfly.io/antfly-operator-install.yaml
   ```

2. Pull and push the operator image to your private registry:
   ```bash
   # Pull from public registry
   docker pull ghcr.io/antflydb/antfly-operator:latest

   # Tag for private registry
   docker tag ghcr.io/antflydb/antfly-operator:latest \
     your-registry.example.com/antfly-operator:latest

   # Push to private registry
   docker push your-registry.example.com/antfly-operator:latest
   ```

3. Update the image in `install.yaml`:
   ```yaml
   # Change image from ghcr.io/antflydb/antfly-operator:latest
   # to your-registry.example.com/antfly-operator:latest
   ```

4. Apply the manifest:
   ```bash
   kubectl apply -f install.yaml
   ```

## Namespace Configuration

By default, the operator watches all namespaces. To restrict to specific namespaces, modify the deployment:

```yaml
# In the operator Deployment spec
env:
  - name: WATCH_NAMESPACE
    value: "namespace1,namespace2"
```

## Resource Requirements

The operator has minimal resource requirements:

| Resource | Request | Limit |
|----------|---------|-------|
| CPU | 100m | 500m |
| Memory | 128Mi | 256Mi |

## Upgrading

To upgrade the operator:

```bash
# Apply the new version
kubectl apply -f https://antfly.io/antfly-operator-install.yaml
```

The operator handles CRD upgrades automatically. Existing clusters continue running during the upgrade.

**Note**: Always review release notes before upgrading for any breaking changes.

## Uninstalling

To completely remove the operator:

```bash
# Delete all Antfly clusters first
kubectl delete antflyclusters --all-namespaces --all
kubectl delete antflybackups --all-namespaces --all
kubectl delete antflyrestores --all-namespaces --all

# Uninstall the operator
kubectl delete -f https://antfly.io/antfly-operator-install.yaml

# Remove CRDs (the operator self-installs these but does not remove them on deletion)
kubectl delete crd antflyclusters.antfly.io antflybackups.antfly.io antflyrestores.antfly.io

# Remove PVCs left behind by StatefulSets (retained by default)
kubectl delete pvc -l app.kubernetes.io/name=antfly-database --all-namespaces
```

**Warning**: Deleting the operator does not delete existing clusters. Delete clusters first to avoid orphaned resources.

**Note**: CRDs are not removed when the operator is deleted. Removing CRDs will also delete any remaining custom resources of that type. PersistentVolumeClaims created by StatefulSets are retained after cluster deletion to prevent accidental data loss — delete them manually once you have confirmed backups or no longer need the data.

**Tip**: To have PVCs automatically cleaned up when an AntflyCluster is deleted, set `pvcRetentionPolicy.whenDeleted: Delete` in the storage spec:

```yaml
spec:
  storage:
    pvcRetentionPolicy:
      whenDeleted: Delete
```

See [Storage Operations](../operations/storage.md) for details.

## Troubleshooting Installation

### Operator Pod Not Starting

Check pod events:
```bash
kubectl describe pod -n antfly-operator-namespace -l app.kubernetes.io/name=antfly-operator
```

Common issues:
- **ImagePullBackOff**: Registry access issue or incorrect image name
- **CrashLoopBackOff**: Check operator logs with `kubectl logs -n antfly-operator-namespace -l app.kubernetes.io/name=antfly-operator`

### RBAC Errors

If you see permission errors:
```bash
# Verify ClusterRoleBinding
kubectl get clusterrolebinding antfly-operator-cluster-role-binding -o yaml

# Test permissions
kubectl auth can-i create statefulsets --as=system:serviceaccount:antfly-operator-namespace:antfly-operator-service-account
```

### CRDs Not Installed

```bash
# Check if CRDs exist
kubectl get crd | grep antfly

# Manually apply CRDs if missing
kubectl apply -f https://antfly.io/antfly-operator-install.yaml
```

## Next Steps

- [Quickstart](quickstart.md): Deploy your first cluster
- [Concepts](concepts.md): Understand the architecture
- [Cloud Platforms](../cloud-platforms/): Platform-specific guides
