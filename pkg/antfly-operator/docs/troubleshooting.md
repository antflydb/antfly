# Troubleshooting Guide

Common issues and solutions for the Antfly Operator.

## Quick Diagnostics

Run these commands to gather diagnostic information:

```bash
# Check operator status
kubectl get pods -n antfly-operator-namespace
kubectl logs -n antfly-operator-namespace deployment/antfly-operator --tail=100

# Check cluster status
kubectl get antflycluster -A
kubectl describe antflycluster <name> -n <namespace>

# Check events
kubectl get events -n <namespace> --sort-by='.lastTimestamp'

# Check pods
kubectl get pods -n <namespace> -l app.kubernetes.io/name=antfly-database
```

## Operator Issues

### Operator Not Starting

**Symptoms**: Operator pod is not running or is in CrashLoopBackOff.

**Check**:
```bash
kubectl get pods -n antfly-operator-namespace
kubectl describe pod -n antfly-operator-namespace -l app.kubernetes.io/name=antfly-operator
kubectl logs -n antfly-operator-namespace -l app.kubernetes.io/name=antfly-operator
```

**Common causes**:

| Issue | Solution |
|-------|----------|
| ImagePullBackOff | Check image name, registry access, pull secrets |
| CrashLoopBackOff | Check logs for errors, verify CRDs installed |
| Insufficient resources | Increase node resources or operator limits |

### RBAC Permission Errors

**Symptoms**: Operator logs show "forbidden" errors.

**Example**:
```
poddisruptionbudgets.policy is forbidden: User "system:serviceaccount:antfly-operator-namespace:WRONG-NAME" cannot list resource
```

**Solution**:
1. Verify ServiceAccount name matches:
   ```bash
   kubectl get deployment antfly-operator -n antfly-operator-namespace \
     -o jsonpath='{.spec.template.spec.serviceAccountName}'
   ```

2. Check ClusterRoleBinding:
   ```bash
   kubectl get clusterrolebinding antfly-operator-cluster-role-binding -o yaml
   ```

3. Test permissions:
   ```bash
   kubectl auth can-i list poddisruptionbudgets \
     --as=system:serviceaccount:antfly-operator-namespace:antfly-operator-service-account
   ```

See [RBAC](security/rbac.md) for detailed RBAC configuration.

### CRDs Not Found

**Symptoms**: `error: the server doesn't have a resource type "antflycluster"`

**Solution**:
```bash
# Check if CRDs are installed
kubectl get crd | grep antfly

# Reinstall if missing
kubectl apply -f https://antfly.io/antfly-operator-install.yaml
```

## Cluster Issues

### Cluster Stuck in Pending

**Symptoms**: AntflyCluster shows `Phase: Pending` for extended time.

**Check**:
```bash
kubectl describe antflycluster <name>
kubectl get pods -l app.kubernetes.io/name=antfly-database -o wide
kubectl get events --field-selector involvedObject.name=<name>
```

**Common causes**:

| Issue | Solution |
|-------|----------|
| Insufficient resources | Add nodes or reduce resource requests |
| Storage class issues | Verify storage class exists and provisions |
| Image pull issues | Check image name and registry access |
| Secret not found | Create referenced secrets |

### Pods Not Scheduling

**Symptoms**: Pods stuck in Pending state.

**Check**:
```bash
kubectl describe pod <pod-name>
kubectl get events --field-selector involvedObject.name=<pod-name>
```

**Common causes**:

| Cause | Message | Solution |
|-------|---------|----------|
| No nodes | `0/3 nodes are available` | Add nodes, reduce requests |
| Taints | `node(s) had taints that the pod didn't tolerate` | Add tolerations or untaint nodes |
| Affinity | `node(s) didn't match Pod's node affinity` | Fix affinity rules |
| Resources | `Insufficient cpu/memory` | Scale cluster or reduce requests |

### Pods in CrashLoopBackOff

**Symptoms**: Pods restart repeatedly.

**Check**:
```bash
kubectl logs <pod-name> --previous
kubectl describe pod <pod-name>
```

**Common causes**:

| Issue | Solution |
|-------|----------|
| Configuration error | Check `spec.config` JSON validity |
| Port conflict | Verify ports are not in use |
| Storage issues | Check PVC binding and permissions |
| OOMKilled | Increase memory limits |

### CreateContainerConfigError

**Symptoms**: Pods stuck with `CreateContainerConfigError`.

**Check**:
```bash
kubectl describe pod <pod-name>
```

**Common cause**: Secret referenced in `envFrom` doesn't exist.

**Solution**:
```bash
# Check referenced secrets
kubectl get antflycluster <name> -o jsonpath='{.spec.dataNodes.envFrom}'

# Create missing secret
kubectl create secret generic backup-credentials \
  --from-literal=AWS_ACCESS_KEY_ID='...' \
  --from-literal=AWS_SECRET_ACCESS_KEY='...'
```

### Configuration Validation Failed

**Symptoms**: Cluster shows `ConfigurationValid: False`.

**Check**:
```bash
kubectl get antflycluster <name> -o jsonpath='{.status.conditions}' | jq '.[] | select(.type=="ConfigurationValid")'
```

**Common causes**:

| Reason | Fix |
|--------|-----|
| `ConflictingSettings` | Remove `useSpotPods` when `autopilot=true` |
| `InvalidComputeClass` | Use valid compute class value |
| `InvalidEBSVolumeType` | Use valid EBS volume type |
| `ImmutableFieldChanged` | Delete and recreate cluster |

## Storage Issues

### PVC/AZ Topology Mismatch

**Symptoms**: Pods stuck in Pending with `volume node affinity conflict`. The `StorageHealthy` condition on the AntflyCluster shows `False` with reason `PVCAZMismatch`.

**Root cause**: PersistentVolumes backed by zone-bound storage (EBS, GCE PD, Azure Disk LRS) are tied to the availability zone where they were provisioned. If a node autoscaler creates nodes in a different AZ than existing PVCs, pods cannot mount their volumes.

**Check**:
```bash
# Check StorageHealthy condition
kubectl get antflycluster <name> -o jsonpath='{.status.conditions}' | jq '.[] | select(.type=="StorageHealthy")'

# Check pod events for volume affinity issues
kubectl describe pod <pending-pod-name>
# Look for: "volume node affinity conflict"

# Check which AZ the PVC's PV is in
kubectl get pv $(kubectl get pvc <pvc-name> -o jsonpath='{.spec.volumeName}') -o jsonpath='{.spec.nodeAffinity}'
```

**Solutions**:

1. **Verify StorageClass uses `WaitForFirstConsumer`**:
   ```bash
   kubectl get storageclass <name> -o yaml | grep volumeBindingMode
   ```
   If it shows `Immediate`, switch to a StorageClass with `WaitForFirstConsumer`. See the cross-cloud StorageClass table below.

2. **Delete stale PVCs and let new ones be provisioned**:
   ```bash
   # Scale down the StatefulSet first
   kubectl scale statefulset <name> --replicas=0
   # Delete the mismatched PVCs
   kubectl delete pvc <pvc-name>
   # Scale back up — new PVCs will be provisioned in the correct AZ
   kubectl scale statefulset <name> --replicas=3
   ```

3. **Use Karpenter instead of cluster-autoscaler on EKS** — Karpenter can be configured with explicit AZ topology requirements, avoiding the ASG-from-zero AZ mismatch entirely. See [AWS EKS](cloud-platforms/aws-eks.md).

**Cross-cloud StorageClass reference:**

| Provider | Recommended StorageClass | volumeBindingMode | Notes |
|----------|--------------------------|-------------------|-------|
| EKS < 1.30 | `gp3` (custom) or default `gp2` | `WaitForFirstConsumer` | Must use `ebs.csi.aws.com` provisioner for gp3 |
| EKS >= 1.30 | `gp3` (custom, **must create**) | `WaitForFirstConsumer` | **No default StorageClass on EKS 1.30+** |
| GKE Standard | `standard-rwo` or `premium-rwo` | `WaitForFirstConsumer` | **Default `standard` uses `Immediate` — do NOT use for multi-AZ** |
| GKE Autopilot | `standard-rwo` (default) | `WaitForFirstConsumer` | Autopilot handles topology internally |
| AKS < 1.29 | `managed-csi` or `managed-csi-premium` | `WaitForFirstConsumer` | LRS disks are AZ-bound |
| AKS >= 1.29 | `managed-csi` (default) | `WaitForFirstConsumer` | Multi-zone clusters auto-use ZRS — AZ problem eliminated |
| Generic | Must verify | Must be `WaitForFirstConsumer` | Check with `kubectl get sc <name> -o yaml` |

### Stale PVCs After Cluster Recreation

**Symptoms**: After deleting an AntflyCluster and recreating one with the same name, pods go Pending with `volume node affinity conflict` or bind to PVCs containing data from the old cluster.

**Root cause**: Kubernetes retains PVCs by default after StatefulSet deletion. When a new cluster reuses the same name, the new StatefulSet binds to old PVCs that may be in different AZs or contain stale data.

**Solutions**:

1. **Use `pvcRetentionPolicy.whenDeleted: Delete`** to automatically clean up PVCs on cluster deletion:
   ```yaml
   spec:
     storage:
       pvcRetentionPolicy:
         whenDeleted: Delete
         whenScaled: Retain
   ```

2. **Manually delete PVCs before recreating**:
   ```bash
   kubectl delete pvc -l app.kubernetes.io/name=antfly-database,app.kubernetes.io/instance=<cluster-name>
   ```

3. **Use a different cluster name** when recreating to avoid binding to old PVCs.

### Stuck Finalizer

**Symptoms**: AntflyCluster deletion hangs. The resource has a `antfly.io/pvc-cleanup` finalizer that is not being removed.

**Root cause**: The finalizer-based cleanup (`cleanupStorageResources`) deletes StatefulSets, waits for pods to terminate, then deletes PVCs. If this process gets stuck (e.g., pod stuck in Terminating, PVC stuck in Released), the finalizer prevents CR deletion.

**Solution**: Manually remove the finalizer:
```bash
kubectl edit antflycluster <name>
# Remove "antfly.io/pvc-cleanup" from metadata.finalizers
```

Then manually clean up any remaining resources:
```bash
kubectl delete statefulset <name>-metadata <name>-data
kubectl delete pvc -l app.kubernetes.io/name=antfly-database,app.kubernetes.io/instance=<name>
```

### PVCs Not Binding

**Symptoms**: PVCs stuck in Pending state.

**Check**:
```bash
kubectl get pvc -l app.kubernetes.io/name=antfly-database
kubectl describe pvc <pvc-name>
kubectl get storageclass
```

**Solutions**:

1. **Verify storage class exists**:
   ```bash
   kubectl get storageclass <storage-class-name>
   ```

2. **Check for provisioner issues**:
   ```bash
   kubectl get pods -n kube-system | grep -E "(provisioner|csi)"
   ```

3. **Use default storage class**:
   ```yaml
   spec:
     storage:
       storageClass: ""  # Use cluster default
   ```

### Storage Quota Exceeded

**Symptoms**: PVC creation fails with quota error.

**Check**:
```bash
kubectl describe resourcequota -n <namespace>
```

**Solution**: Increase quota or reduce storage requests.

## Networking Issues

### Services Not Accessible

**Symptoms**: Cannot connect to cluster services.

**Check**:
```bash
kubectl get svc -l app.kubernetes.io/name=antfly-database
kubectl get endpoints -l app.kubernetes.io/name=antfly-database
```

**Solutions**:

1. **Check endpoints have addresses**:
   ```bash
   kubectl get endpoints <service-name>
   ```

2. **Verify pods are ready**:
   ```bash
   kubectl get pods -l app.kubernetes.io/name=antfly-database -o wide
   ```

3. **Test connectivity from another pod**:
   ```bash
   kubectl run debug --rm -it --image=busybox -- nc -zv <service-name> <port>
   ```

### LoadBalancer Pending

**Symptoms**: External IP shows `<pending>`.

**Check**:
```bash
kubectl describe svc <cluster>-public-api
```

**Solutions**:

| Environment | Solution |
|-------------|----------|
| Cloud | Check cloud provider quotas and permissions |
| On-premises | Install MetalLB or use NodePort |
| minikube | Run `minikube tunnel` (for LoadBalancer) or `minikube service <service-name>` (for NodePort) |
| kind | Use NodePort or port-forward |

### Minikube Docker Driver Access

**Symptoms**: Services are not accessible from the host when using Minikube with the Docker driver. NodePort services cannot be reached via `localhost:<nodePort>`.

With Minikube's Docker driver, the Kubernetes node runs inside a Docker container, so NodePort services are not directly accessible on the host network.

**Solutions** (in order of simplicity):

1. **`kubectl port-forward`** (simplest, works with any driver):
   ```bash
   kubectl port-forward svc/<service-name> -n <namespace> <local-port>:<service-port>
   ```

2. **`minikube service`** (opens browser automatically):
   ```bash
   minikube service <service-name> -n <namespace>
   ```

3. **`minikube tunnel`** (assigns external IPs to LoadBalancer services):
   ```bash
   minikube tunnel
   ```
   This runs in the foreground and requires `sudo` access. It assigns real external IPs to LoadBalancer-type services.

## Autoscaling Issues

### Autoscaling Not Working

**Symptoms**: Replicas don't scale despite high utilization.

**Check**:
```bash
# Verify metrics-server
kubectl top pods -l app.kubernetes.io/name=antfly-database

# Check autoscaling status
kubectl get antflycluster <name> -o jsonpath='{.status.autoScalingStatus}'

# Check operator logs
kubectl logs -n antfly-operator-namespace deployment/antfly-operator | grep -i autoscal
```

**Common causes**:

| Issue | Solution |
|-------|----------|
| metrics-server not installed | Install metrics-server |
| No resource requests | Add CPU/memory requests to pods |
| Cooldown period active | Wait for cooldown to expire |
| At max/min replicas | Adjust limits |

### Metrics Not Available

**Symptoms**: `kubectl top pods` returns error.

**Solution**:
```bash
# Install metrics-server
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml

# For kind, add insecure TLS flag
kubectl patch deployment metrics-server -n kube-system --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
```

## Backup/Restore Issues

### Backup Failing

**Symptoms**: AntflyBackup shows `Phase: Failed`.

**Check**:
```bash
kubectl describe antflybackup <name>
kubectl logs -l job-name=<backup-job-name>
```

**Common causes**:

| Issue | Solution |
|-------|----------|
| Invalid credentials | Verify secret contents |
| Bucket doesn't exist | Create S3/GCS bucket |
| Network issues | Check egress rules |
| Timeout | Increase `backupTimeout` |

### Restore Failing

**Symptoms**: AntflyRestore shows `Phase: Failed`.

**Check**:
```bash
kubectl describe antflyrestore <name>
kubectl get antflyrestore <name> -o jsonpath='{.status.tables}'
```

**Common causes**:

| Issue | Solution |
|-------|----------|
| Backup not found | Verify backupId and location |
| Table exists | Use `skip_if_exists` or `overwrite` mode |
| Cluster not ready | Wait for cluster to be Running |
| Timeout | Increase `restoreTimeout` |

### Credentials Issues

**Check**:
```bash
# Verify secret exists
kubectl get secret backup-credentials

# Check SecretsReady condition
kubectl get antflycluster <name> -o jsonpath='{.status.conditions}' | jq '.[] | select(.type=="SecretsReady")'

# Test credentials (from debug pod)
kubectl run debug --rm -it --image=amazon/aws-cli -- aws s3 ls s3://bucket/
```

## Service Mesh Issues

### Partial Sidecar Injection

**Symptoms**: `ServiceMeshReady: False` with `PartialInjection`.

**Check**:
```bash
kubectl get antflycluster <name> -o jsonpath='{.status.serviceMeshStatus}'
```

**Solutions**:

1. **Check mesh control plane**:
   ```bash
   # Istio
   istioctl analyze -n <namespace>

   # Linkerd
   linkerd check
   ```

2. **Verify annotations**:
   ```bash
   kubectl get pods -l app.kubernetes.io/name=antfly-database -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.annotations}{"\n"}{end}'
   ```

3. **Force pod recreation**:
   ```bash
   kubectl rollout restart statefulset/<cluster>-metadata
   kubectl rollout restart statefulset/<cluster>-data
   ```

### High Latency with Mesh

**Symptoms**: Database operations slow after enabling mesh.

**Solution**: Exclude Raft ports from mesh:
```yaml
spec:
  serviceMesh:
    annotations:
      traffic.sidecar.istio.io/excludeOutboundPorts: "9017,9021"
```

## Cloud-Specific Issues

### GKE Autopilot

**Pods pending for extended time**:
- GKE Autopilot provisions nodes on-demand
- Wait 2-5 minutes for node provisioning
- Check events for provisioning status

**Compute class conflicts**:
- Don't use `useSpotPods` with `autopilot=true`
- Use `autopilotComputeClass: "autopilot-spot"` instead

### AWS EKS

**EBS CSI driver issues**:
```bash
kubectl get csidriver ebs.csi.aws.com
kubectl get pods -n kube-system -l app.kubernetes.io/name=aws-ebs-csi-driver
```

**IRSA not working**:
```bash
# Verify OIDC provider
aws eks describe-cluster --name <cluster> --query "cluster.identity.oidc"

# Test from pod
kubectl exec -it <pod> -- aws sts get-caller-identity
```

## Debugging Commands

### Operator Logs

```bash
# Recent logs
kubectl logs -n antfly-operator-namespace deployment/antfly-operator --tail=100

# Follow logs
kubectl logs -n antfly-operator-namespace deployment/antfly-operator -f

# Filter for specific cluster
kubectl logs -n antfly-operator-namespace deployment/antfly-operator | grep <cluster-name>
```

### Pod Inspection

```bash
# Full pod details
kubectl describe pod <pod-name>

# Container status
kubectl get pod <pod-name> -o jsonpath='{.status.containerStatuses}' | jq

# Previous container logs
kubectl logs <pod-name> --previous
```

### Resource Status

```bash
# All Antfly resources
kubectl get antflycluster,antflybackup,antflyrestore -A

# Detailed cluster status
kubectl get antflycluster <name> -o yaml | yq '.status'

# Conditions only
kubectl get antflycluster <name> -o jsonpath='{.status.conditions}' | jq
```

### Network Debugging

```bash
# Service endpoints
kubectl get endpoints -l app.kubernetes.io/name=antfly-database

# DNS resolution
kubectl run debug --rm -it --image=busybox -- nslookup <service-name>

# Port connectivity
kubectl run debug --rm -it --image=busybox -- nc -zv <service-name> <port>
```

## Getting Help

If you can't resolve an issue:

1. **Check existing issues**: [GitHub Issues](https://github.com/antflydb/antfly/issues)

2. **Gather diagnostics**:
   ```bash
   kubectl get antflycluster -A -o yaml > cluster-status.yaml
   kubectl logs -n antfly-operator-namespace deployment/antfly-operator > operator-logs.txt
   kubectl get events -A --sort-by='.lastTimestamp' > events.txt
   ```

3. **Open a new issue** with:
   - Kubernetes version (`kubectl version`)
   - Operator version
   - Cloud provider (if applicable)
   - Cluster configuration (sanitized)
   - Error messages and logs
   - Steps to reproduce
