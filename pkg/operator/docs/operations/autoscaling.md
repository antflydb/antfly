# Autoscaling

The Antfly Operator supports automatic scaling of data nodes based on CPU and memory utilization metrics.

## Overview

Autoscaling allows your Antfly cluster to automatically adjust the number of data nodes based on resource utilization, helping to:

- Handle traffic spikes by scaling up
- Reduce costs during low-traffic periods by scaling down
- Maintain consistent performance under varying loads

**Important**: Only data nodes can be autoscaled. Metadata nodes maintain a fixed replica count for Raft consensus stability.

## Features

- **Metrics-based scaling**: Scale based on CPU and/or memory utilization
- **Configurable boundaries**: Set minimum and maximum replica counts
- **Cooldown periods**: Prevent flapping with configurable scale-up and scale-down cooldowns
- **Gradual scaling**: Controlled scaling to prevent sudden resource spikes
- **Metadata node stability**: Metadata nodes remain at fixed count for Raft consensus

## Prerequisites

1. **metrics-server**: Must be installed and running in your cluster
2. **Resource requests**: Data nodes must have CPU and memory requests defined

### Install metrics-server

Most managed Kubernetes services include metrics-server. For self-managed clusters:

```bash
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
```

Verify metrics are available:

```bash
kubectl top nodes
kubectl top pods
```

## Configuration

Add the `autoScaling` section to your `dataNodes` specification:

```yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: my-cluster
spec:
  dataNodes:
    replicas: 3  # Initial replicas
    resources:
      cpu: "500m"      # Required for CPU-based scaling
      memory: "1Gi"    # Required for memory-based scaling
    autoScaling:
      enabled: true
      minReplicas: 3
      maxReplicas: 10
      targetCPUUtilizationPercentage: 70
      targetMemoryUtilizationPercentage: 80
      scaleUpCooldown: 60s
      scaleDownCooldown: 300s
```

## Parameters

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| `enabled` | Enable/disable autoscaling | Yes | - |
| `minReplicas` | Minimum number of data node replicas | Yes | - |
| `maxReplicas` | Maximum number of data node replicas | Yes | - |
| `targetCPUUtilizationPercentage` | Target CPU utilization percentage | No | - |
| `targetMemoryUtilizationPercentage` | Target memory utilization percentage | No | - |
| `scaleUpCooldown` | Minimum time between scale-up operations | No | 60s |
| `scaleDownCooldown` | Minimum time between scale-down operations | No | 300s |

**Note**: At least one of `targetCPUUtilizationPercentage` or `targetMemoryUtilizationPercentage` must be specified.

## How It Works

### Metrics Collection

The operator collects CPU and memory metrics from data node pods every 30 seconds via the Kubernetes Metrics API.

### Scaling Decision

The operator compares current utilization against target thresholds:

- **Scale Up**: When average utilization exceeds the target (e.g., CPU > 70%)
- **Scale Down**: When average utilization is significantly below the target
- **No Change**: When utilization is within acceptable range

### Gradual Scaling

To prevent sudden resource spikes, scaling is gradual:

| Direction | Maximum Change |
|-----------|----------------|
| Scale Up | 50% increase or +2 replicas (whichever is greater) |
| Scale Down | 25% decrease or -1 replica (whichever is greater) |

**Example**: If current replicas = 4 and scale-up is needed:
- 50% increase = 2 replicas
- Maximum = +2 replicas
- New replicas = min(6, maxReplicas)

### Cooldown Enforcement

Cooldown periods prevent rapid scaling operations (flapping):

- **Scale-up cooldown** (default: 60s): Minimum time between scale-up operations
- **Scale-down cooldown** (default: 300s): Minimum time between scale-down operations

The longer scale-down cooldown prevents premature scale-down after a traffic spike subsides.

## Monitoring

### Check Autoscaling Status

```bash
kubectl get antflycluster my-cluster -o jsonpath='{.status.autoScalingStatus}' | jq
```

Output:
```json
{
  "currentReplicas": 5,
  "desiredReplicas": 5,
  "lastScaleTime": "2025-01-15T10:30:00Z",
  "lastScaleDirection": "up",
  "currentCPUUtilizationPercentage": 68,
  "currentMemoryUtilizationPercentage": 75
}
```

### View Operator Logs

```bash
kubectl logs -n antfly-operator-namespace -l app.kubernetes.io/name=antfly-operator | grep -i autoscal
```

### Watch Scaling Events

```bash
kubectl get events --field-selector involvedObject.name=my-cluster -w
```

## Example Configurations

### CPU-Only Scaling

```yaml
spec:
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
```

### Memory-Only Scaling

```yaml
spec:
  dataNodes:
    replicas: 3
    resources:
      cpu: "500m"
      memory: "1Gi"
    autoScaling:
      enabled: true
      minReplicas: 3
      maxReplicas: 10
      targetMemoryUtilizationPercentage: 80
```

### Combined CPU and Memory

```yaml
spec:
  dataNodes:
    replicas: 3
    resources:
      cpu: "1000m"
      memory: "2Gi"
    autoScaling:
      enabled: true
      minReplicas: 3
      maxReplicas: 20
      targetCPUUtilizationPercentage: 70
      targetMemoryUtilizationPercentage: 80
      scaleUpCooldown: 30s    # Faster scale-up
      scaleDownCooldown: 600s # Slower scale-down
```

### Conservative Scaling

For workloads that prefer stability over responsiveness:

```yaml
spec:
  dataNodes:
    replicas: 5
    resources:
      cpu: "1000m"
      memory: "2Gi"
    autoScaling:
      enabled: true
      minReplicas: 5
      maxReplicas: 10
      targetCPUUtilizationPercentage: 50  # Lower threshold
      scaleUpCooldown: 120s   # Wait longer before scaling up
      scaleDownCooldown: 900s # Wait much longer before scaling down
```

### Aggressive Scaling

For workloads that need fast response to traffic changes:

```yaml
spec:
  dataNodes:
    replicas: 3
    resources:
      cpu: "500m"
      memory: "1Gi"
    autoScaling:
      enabled: true
      minReplicas: 2
      maxReplicas: 50
      targetCPUUtilizationPercentage: 80  # Higher threshold
      scaleUpCooldown: 30s    # Scale up quickly
      scaleDownCooldown: 180s # Scale down fairly quickly too
```

## Scaling with Spot Pods/Instances

Autoscaling works well with Spot Pods (GKE) or Spot Instances (EKS):

```yaml
spec:
  gke:
    autopilot: true
    autopilotComputeClass: "autopilot-spot"
    podDisruptionBudget:
      enabled: true
      maxUnavailable: 1

  dataNodes:
    replicas: 5
    resources:
      cpu: "500m"
      memory: "1Gi"
    autoScaling:
      enabled: true
      minReplicas: 5   # Higher minimum for Spot tolerance
      maxReplicas: 20
      targetCPUUtilizationPercentage: 60  # Lower threshold for buffer
```

**Best Practice**: When using Spot, set a higher `minReplicas` to handle potential pod evictions.

## Limitations

1. **Metadata Nodes Not Scaled**: Metadata nodes maintain a fixed replica count for Raft consensus
2. **PVCs Retained**: PersistentVolumeClaims are retained when scaling down (data is preserved, but storage costs remain)
3. **metrics-server Required**: Autoscaling won't work without metrics-server
4. **Resource Requests Required**: Pods must have resource requests for accurate utilization calculation

## Troubleshooting

### Autoscaling Not Working

1. **Check metrics-server**:
   ```bash
   kubectl get pods -n kube-system | grep metrics-server
   kubectl top pods  # Should return data
   ```

2. **Verify resource requests are set**:
   ```bash
   kubectl get pod <data-pod> -o jsonpath='{.spec.containers[*].resources.requests}'
   ```

3. **Check operator logs**:
   ```bash
   kubectl logs -n antfly-operator-namespace -l app.kubernetes.io/name=antfly-operator | grep -i autoscal
   ```

4. **Verify autoscaling is enabled**:
   ```bash
   kubectl get antflycluster my-cluster -o jsonpath='{.spec.dataNodes.autoScaling}'
   ```

### Scaling Too Aggressively

Increase cooldown periods:

```yaml
autoScaling:
  scaleUpCooldown: 120s
  scaleDownCooldown: 600s
```

### Scaling Too Slowly

Decrease cooldown periods:

```yaml
autoScaling:
  scaleUpCooldown: 30s
  scaleDownCooldown: 180s
```

### Not Scaling Down

The operator won't scale below `minReplicas`. Also check:
- Scale-down cooldown hasn't elapsed since last scale operation
- Current utilization might still be above scale-down threshold

### Metrics Not Accurate

Ensure pods have been running long enough (>30 seconds) for metrics to stabilize.

## Best Practices

1. **Set Appropriate Thresholds**: Start with 70% CPU / 80% memory and adjust based on your workload
2. **Configure Minimum Replicas**: Set `minReplicas` to handle your baseline traffic
3. **Use Adequate Maximum**: Set `maxReplicas` high enough for peak load, but consider costs
4. **Longer Scale-Down Cooldown**: Use longer scale-down cooldown to prevent oscillation
5. **Monitor Scaling Events**: Set up alerts for scaling operations
6. **Test Under Load**: Verify autoscaling behavior with load testing
7. **Consider PDB**: Use PodDisruptionBudgets to protect against disruption during scaling

## See Also

- [Monitoring](monitoring.md): Health checks and observability
- [AntflyCluster API Reference](../reference/antflycluster-api.md): Complete API reference
