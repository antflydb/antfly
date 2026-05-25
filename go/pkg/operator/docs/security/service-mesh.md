# Service Mesh Integration

> **Warning:** Service mesh integration is experimental. APIs and behavior may change in future releases.

The Antfly Operator provides native support for service mesh integration, enabling automatic mTLS encryption and traffic management for your database clusters.

## Overview

Service mesh integration allows you to:

- **Automatic mTLS encryption** between all Antfly pods
- **Traffic observability** through service mesh telemetry
- **Advanced traffic management** (circuit breaking, retries, timeouts)
- **Zero-trust security** with automatic certificate rotation
- **Network policy enforcement** at the sidecar level

The operator automatically detects sidecar injection and updates cluster status accordingly.

## Supported Service Meshes

The Antfly Operator is designed to work with any Kubernetes service mesh that uses sidecar injection:

| Mesh | Status | Notes |
|------|--------|-------|
| **Istio** | Recommended | Best tested integration |
| **Linkerd** | Supported | Lightweight option |
| **Consul Connect** | Supported | HashiCorp ecosystem |

## Quick Start

### Prerequisites

1. Antfly Operator installed in your cluster
2. Service mesh control plane installed (e.g., Istio, Linkerd)
3. Service mesh sidecar injection configured (namespace-level or pod-level)

### Enable Service Mesh on a New Cluster

```yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: my-cluster
  namespace: production
spec:
  image: ghcr.io/antflydb/antfly:latest
  serviceMesh:
    enabled: true
    annotations:
      sidecar.istio.io/inject: "true"
  metadataNodes:
    replicas: 3
    resources:
      cpu: "500m"
      memory: "512Mi"
  dataNodes:
    replicas: 3
    resources:
      cpu: "1000m"
      memory: "2Gi"
  storage:
    storageClass: "standard"
    metadataStorage: "1Gi"
    dataStorage: "10Gi"
```

### Enable Service Mesh on Existing Cluster

Patch an existing cluster to enable service mesh:

```bash
kubectl patch antflycluster my-cluster -n production --type='merge' -p='
{
  "spec": {
    "serviceMesh": {
      "enabled": true,
      "annotations": {
        "sidecar.istio.io/inject": "true"
      }
    }
  }
}'
```

The operator will perform a rolling restart, injecting sidecars into each pod while maintaining cluster availability.

## Configuration

### Spec Fields

```yaml
spec:
  serviceMesh:
    enabled: true              # Enable service mesh integration
    annotations:               # Mesh-specific annotations
      key: value
```

#### `enabled` (boolean, optional, default: `false`)

Controls whether service mesh sidecar injection is enabled for the cluster.

#### `annotations` (map[string]string, optional)

Mesh-specific annotations to apply to pod templates. These annotations trigger sidecar injection and configure mesh behavior.

### Status Fields

The operator automatically populates the following status fields:

```yaml
status:
  serviceMeshStatus:
    enabled: true                        # Reflects spec.serviceMesh.enabled
    sidecarInjectionStatus: "Complete"   # Complete | Partial | None | Unknown
    podsWithSidecars: 6                  # Number of pods with sidecars
    totalPods: 6                         # Total number of pods
    lastTransitionTime: "2025-10-04T..."
  conditions:
  - type: ServiceMeshReady
    status: "True"
    reason: SidecarInjectionComplete
    message: "All 6 pods have sidecars injected"
```

#### Sidecar Injection Status Values

| Status | Description |
|--------|-------------|
| `Complete` | All pods have sidecars injected |
| `Partial` | Some pods have sidecars, others don't (blocks reconciliation) |
| `None` | No pods have sidecars |
| `Unknown` | Pod count is zero or status cannot be determined |

## Mesh-Specific Configuration

### Istio

```yaml
spec:
  serviceMesh:
    enabled: true
    annotations:
      sidecar.istio.io/inject: "true"
      # Exclude Raft ports from proxy (recommended for performance)
      traffic.sidecar.istio.io/excludeOutboundPorts: "9017,9021"
      # Resource limits for sidecar (optional)
      sidecar.istio.io/proxyCPU: "100m"
      sidecar.istio.io/proxyMemory: "128Mi"
```

**Important Ports:**

| Port | Service | Recommendation |
|------|---------|----------------|
| 12377 | Metadata API | Include in mesh |
| 9017 | Metadata Raft | Exclude from mesh |
| 12380 | Data API | Include in mesh |
| 9021 | Data Raft | Exclude from mesh |

Consider excluding Raft ports (9017, 9021) from the service mesh to reduce latency for consensus traffic.

### Linkerd

```yaml
spec:
  serviceMesh:
    enabled: true
    annotations:
      linkerd.io/inject: enabled
      # Skip Raft ports (recommended)
      config.linkerd.io/skip-outbound-ports: "9017,9021"
      config.linkerd.io/skip-inbound-ports: "9017,9021"
```

### Consul Connect

```yaml
spec:
  serviceMesh:
    enabled: true
    annotations:
      consul.hashicorp.com/connect-inject: "true"
      consul.hashicorp.com/connect-service-upstreams: "antfly-metadata:12377,antfly-data:12380"
```

## Observability

### Check Service Mesh Status

View the current service mesh status:

```bash
kubectl get antflycluster my-cluster -o jsonpath='{.status.serviceMeshStatus}' | jq
```

### Check ServiceMeshReady Condition

```bash
kubectl get antflycluster my-cluster -o jsonpath='{.status.conditions[?(@.type=="ServiceMeshReady")]}' | jq
```

### View Operator Logs

Monitor service mesh integration events:

```bash
kubectl logs -n antfly-operator-namespace deployment/antfly-operator -f | grep -i "service mesh"
```

### View Cluster Events

Check for service mesh-related events:

```bash
kubectl get events --field-selector involvedObject.name=my-cluster -n production
```

## Performance Optimization

### Exclude Raft Ports

Raft consensus traffic is latency-sensitive. Exclude Raft ports from the mesh:

```yaml
# Istio
annotations:
  traffic.sidecar.istio.io/excludeOutboundPorts: "9017,9021"

# Linkerd
annotations:
  config.linkerd.io/skip-outbound-ports: "9017,9021"
  config.linkerd.io/skip-inbound-ports: "9017,9021"
```

### Tune Sidecar Resources

Set appropriate resource limits for sidecars:

```yaml
annotations:
  sidecar.istio.io/proxyCPU: "100m"
  sidecar.istio.io/proxyMemory: "128Mi"
  sidecar.istio.io/proxyCPULimit: "500m"
  sidecar.istio.io/proxyMemoryLimit: "512Mi"
```

### Sidecar Concurrency

Tune proxy concurrency based on workload:

```yaml
annotations:
  sidecar.istio.io/concurrency: "2"
```

## Security Configuration

### Strict mTLS

For maximum security, use strict mTLS mode:

```yaml
# Istio PeerAuthentication
apiVersion: security.istio.io/v1beta1
kind: PeerAuthentication
metadata:
  name: antfly-mtls
  namespace: production
spec:
  selector:
    matchLabels:
      app: antfly
  mtls:
    mode: STRICT
```

### Network Policies

Combine service mesh with Kubernetes NetworkPolicies for defense in depth:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: antfly-mesh-only
  namespace: production
spec:
  podSelector:
    matchLabels:
      app: antfly
  policyTypes:
    - Ingress
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app: antfly
```

## Troubleshooting

### Partial Sidecar Injection

**Problem**: The operator detects partial sidecar injection and blocks reconciliation.

**Symptoms**:
- `ServiceMeshReady` condition is `False` with reason `PartialInjection`
- Operator logs show: `"Blocking reconciliation" ... "partial sidecar injection"`
- Kubernetes events show: `Warning PartialSidecarInjection`

**Solutions**:

1. **Check mesh control plane**:
   ```bash
   # Istio
   istioctl analyze -n production

   # Linkerd
   linkerd check
   ```

2. **Verify pod annotations**:
   ```bash
   kubectl get pods -n production -l app.kubernetes.io/name=antfly-database \
     -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.annotations}{"\n"}{end}'
   ```

3. **Check admission webhooks**:
   ```bash
   kubectl get mutatingwebhookconfigurations | grep -i istio
   ```

4. **Force pod recreation**:
   ```bash
   kubectl delete pod <pod-name> -n production
   ```

### Sidecars Not Injected

**Problem**: Service mesh is enabled but sidecars are not being injected.

**Solutions**:

1. **Verify annotations are correct**:
   ```bash
   kubectl get antflycluster my-cluster -o yaml | grep -A 5 serviceMesh
   ```

2. **Check namespace labels** (if using namespace-level injection):
   ```bash
   kubectl get namespace production --show-labels
   ```

3. **Verify StatefulSet pod template**:
   ```bash
   kubectl get statefulset my-cluster-metadata -o jsonpath='{.spec.template.metadata.annotations}' | jq
   ```

4. **Test manual injection** (debugging):
   ```bash
   # Istio
   istioctl kube-inject -f examples/service-mesh-istio-cluster.yaml

   # Linkerd
   linkerd inject examples/service-mesh-linkerd-cluster.yaml
   ```

### High Latency After Enabling Mesh

**Problem**: Database latency increases significantly after enabling service mesh.

**Solutions**:

1. **Exclude Raft ports from mesh** (see Performance Optimization above)

2. **Tune sidecar resource limits**:
   ```yaml
   annotations:
     sidecar.istio.io/proxyCPU: "200m"
     sidecar.istio.io/proxyMemory: "256Mi"
   ```

3. **Check mTLS overhead**:
   ```bash
   # Istio - view proxy stats
   istioctl proxy-config endpoint <pod-name> -n production
   ```

### Rolling Restart Failures

**Problem**: Pods fail to restart with sidecars during rolling update.

**Solutions**:

1. **Check resource quotas**:
   ```bash
   kubectl describe resourcequota -n production
   ```

2. **Verify PodDisruptionBudget** (if using GKE):
   ```bash
   kubectl get pdb -n production
   ```

3. **Check StatefulSet events**:
   ```bash
   kubectl describe statefulset my-cluster-metadata -n production
   ```

## Best Practices

### Production Deployments

1. **Start with data nodes**: Enable service mesh on data nodes first, verify stability, then enable for metadata nodes

2. **Use resource limits**: Set appropriate sidecar resource limits to prevent OOM
   ```yaml
   annotations:
     sidecar.istio.io/proxyCPU: "100m"
     sidecar.istio.io/proxyMemory: "128Mi"
     sidecar.istio.io/proxyCPULimit: "500m"
     sidecar.istio.io/proxyMemoryLimit: "512Mi"
   ```

3. **Exclude Raft ports**: Reduce latency by excluding consensus traffic from mesh
   ```yaml
   annotations:
     traffic.sidecar.istio.io/excludeOutboundPorts: "9017,9021"
   ```

4. **Monitor during rollout**: Watch cluster status during rolling restart
   ```bash
   watch kubectl get antflycluster my-cluster -o jsonpath='{.status.serviceMeshStatus}'
   ```

### Security Considerations

1. **mTLS mode**: Use STRICT mode for maximum security
2. **Network policies**: Combine service mesh with Kubernetes NetworkPolicies
3. **Certificate rotation**: Service mesh handles automatic rotation - no operator action needed

### Connection Pooling

Configure at service mesh level:

```yaml
# Istio DestinationRule
apiVersion: networking.istio.io/v1beta1
kind: DestinationRule
metadata:
  name: antfly-connection-pool
  namespace: production
spec:
  host: my-cluster-metadata
  trafficPolicy:
    connectionPool:
      tcp:
        maxConnections: 100
```

## Limitations

1. **Metadata nodes**: Service mesh adds latency to Raft consensus. Consider excluding Raft ports or disabling mesh on metadata nodes for latency-sensitive workloads.

2. **Partial injection**: The operator blocks reconciliation when partial injection is detected to prevent split-brain scenarios. Resolve the injection issue before proceeding.

3. **Mesh upgrades**: Upgrade the service mesh control plane independently. The operator will detect sidecar version changes but does not manage mesh upgrades.

## Examples

See the `examples/` directory for complete configuration examples:

- `examples/service-mesh-istio-cluster.yaml` - Istio integration
- `examples/service-mesh-linkerd-cluster.yaml` - Linkerd integration

## See Also

- [Monitoring](../operations/monitoring.md): Observability setup
- [GKE Autopilot](../cloud-platforms/gcp-gke.md): GKE-specific considerations
- [Troubleshooting](../troubleshooting.md): Common issues
