# Monitoring

This guide covers health checks, metrics, and observability for Antfly clusters.

## Overview

The Antfly Operator provides multiple monitoring capabilities:

- **Health Check Endpoints**: Kubernetes probes for pod health
- **Operator Metrics**: Prometheus metrics from the operator
- **Cluster Status**: Status conditions and fields on CRDs
- **Kubernetes Events**: Operational events for troubleshooting
- **Logging**: Configurable log formats and levels

## Health Check Endpoints

All Antfly pods expose health endpoints on port 4200:

| Endpoint | Purpose | Used By |
|----------|---------|---------|
| `/healthz` | Liveness check | Startup and liveness probes |
| `/readyz` | Readiness check | Readiness probe |

### Probe Configuration

The operator automatically configures probes for all pods:

**Startup Probe**:
- Endpoint: `:4200/healthz`
- Initial delay: 30 seconds
- Period: 10 seconds
- Failure threshold: 30 (allows up to 5 minutes for startup)

**Liveness Probe**:
- Endpoint: `:4200/healthz`
- Period: 15 seconds
- Failure threshold: 3 (pod restarted after 45 seconds of failures)

**Readiness Probe**:
- Endpoint: `:4200/readyz`
- Period: 5 seconds
- Failure threshold: 5 (pod removed from service after 25 seconds of failures)

### Custom Health Port

Configure a custom health check port:

```yaml
spec:
  metadataNodes:
    health:
      port: 4200  # Default
  dataNodes:
    health:
      port: 4200  # Default
```

### Manual Health Checks

Check pod health manually:

```bash
# Port-forward to a pod
kubectl port-forward my-cluster-metadata-0 4200:4200

# Check liveness
curl http://localhost:4200/healthz

# Check readiness
curl http://localhost:4200/readyz
```

## Cluster Status

### Viewing Status

```bash
# Quick overview
kubectl get antflycluster my-cluster

# NAME         PHASE     METADATA   DATA   AGE
# my-cluster   Running   3          3      24h

# Detailed status
kubectl get antflycluster my-cluster -o yaml
```

### Status Fields

```yaml
status:
  phase: Running
  metadataNodesReady: 3
  dataNodesReady: 3
  conditions:
    - type: ConfigurationValid
      status: "True"
      reason: ValidationPassed
      lastTransitionTime: "2025-01-15T00:00:00Z"
    - type: SecretsReady
      status: "True"
      reason: AllSecretsFound
      lastTransitionTime: "2025-01-15T00:00:00Z"
  autoScalingStatus:
    currentReplicas: 3
    desiredReplicas: 3
    lastScaleTime: "2025-01-15T10:30:00Z"
    currentCPUUtilizationPercentage: 45
```

### Cluster Phases

| Phase | Description |
|-------|-------------|
| `Pending` | Cluster is being created |
| `Running` | All nodes are ready |
| `Degraded` | Some nodes are not ready |
| `Failed` | Critical error |

### Condition Types

| Condition | Description |
|-----------|-------------|
| `ConfigurationValid` | Configuration passes validation |
| `SecretsReady` | All referenced secrets exist |
| `ServiceMeshReady` | Service mesh sidecars injected (if enabled) |

### Check Specific Conditions

```bash
# Configuration validation status
kubectl get antflycluster my-cluster -o jsonpath='{.status.conditions[?(@.type=="ConfigurationValid")]}' | jq

# Secrets status
kubectl get antflycluster my-cluster -o jsonpath='{.status.conditions[?(@.type=="SecretsReady")]}' | jq

# Service mesh status
kubectl get antflycluster my-cluster -o jsonpath='{.status.serviceMeshStatus}' | jq
```

## Operator Metrics

The operator exposes Prometheus metrics on port 8080.

### Accessing Metrics

```bash
# Port-forward to operator
kubectl port-forward -n antfly-operator-namespace deployment/antfly-operator 8080:8080

# View metrics
curl http://localhost:8080/metrics
```

### Key Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `controller_runtime_reconcile_total` | Counter | Total reconciliations |
| `controller_runtime_reconcile_errors_total` | Counter | Reconciliation errors |
| `controller_runtime_reconcile_time_seconds` | Histogram | Reconciliation duration |
| `workqueue_depth` | Gauge | Reconciliation queue depth |
| `workqueue_adds_total` | Counter | Items added to queue |

### Prometheus ServiceMonitor

For Prometheus Operator, create a ServiceMonitor:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: antfly-operator
  namespace: antfly-operator-namespace
spec:
  selector:
    matchLabels:
      control-plane: antfly-operator
  endpoints:
    - port: metrics
      path: /metrics
      interval: 30s
```

### Operator Health Endpoints

The operator also exposes health endpoints:

```bash
# Health check
curl http://localhost:8081/healthz

# Readiness check
curl http://localhost:8081/readyz
```

## Kubernetes Events

The operator emits events for important operations:

```bash
# View cluster events
kubectl get events --field-selector involvedObject.name=my-cluster

# Watch events in real-time
kubectl get events -w --field-selector involvedObject.name=my-cluster
```

### Common Events

| Event | Type | Description |
|-------|------|-------------|
| `ClusterCreated` | Normal | Cluster resources created |
| `ClusterUpdated` | Normal | Cluster configuration updated |
| `ScalingUp` | Normal | Autoscaling increased replicas |
| `ScalingDown` | Normal | Autoscaling decreased replicas |
| `ValidationFailed` | Warning | Configuration validation failed |
| `SecretNotFound` | Warning | Referenced secret not found |
| `ReconcileError` | Warning | Error during reconciliation |

## Logging Configuration

### Antfly Database Logs

Configure logging via the cluster config:

```yaml
spec:
  config: |
    {
      "log": {
        "level": "info",      // debug, info, warn, error
        "style": "json"       // terminal, json, noop
      }
    }
```

**Log Styles**:
- `terminal`: Colorized console output with stack traces (development)
- `json`: Structured JSON output (production, log aggregation)
- `noop`: No logging output (benchmarking)

**Log Levels**:
- `debug`: Verbose debugging information
- `info`: Normal operational information
- `warn`: Warnings that don't affect operation
- `error`: Errors that may affect operation

### View Pod Logs

```bash
# Metadata node logs
kubectl logs my-cluster-metadata-0

# Data node logs
kubectl logs my-cluster-data-0

# Follow logs
kubectl logs -f my-cluster-metadata-0

# Previous container logs (after restart)
kubectl logs --previous my-cluster-metadata-0
```

### Operator Logs

```bash
# View operator logs
kubectl logs -n antfly-operator-namespace deployment/antfly-operator

# Follow operator logs
kubectl logs -n antfly-operator-namespace deployment/antfly-operator -f

# Filter for specific cluster
kubectl logs -n antfly-operator-namespace deployment/antfly-operator | grep my-cluster
```

## Log Aggregation

### JSON Logging for Aggregation

For production environments, use JSON logging:

```yaml
spec:
  config: |
    {
      "log": {
        "level": "info",
        "style": "json"
      }
    }
```

JSON logs integrate well with:
- Google Cloud Logging (Stackdriver)
- AWS CloudWatch
- Elastic Stack (ELK)
- Splunk
- Datadog

### Fluent Bit Example

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: fluent-bit-config
data:
  fluent-bit.conf: |
    [INPUT]
        Name              tail
        Path              /var/log/containers/my-cluster-*.log
        Parser            docker

    [FILTER]
        Name              kubernetes
        Match             *
        Merge_Log         On

    [OUTPUT]
        Name              es
        Match             *
        Host              elasticsearch
        Port              9200
        Index             antfly-logs
```

## Dashboard Integration

### Grafana Dashboards

Key panels for an Antfly monitoring dashboard:

1. **Cluster Overview**
   - Cluster phase
   - Metadata nodes ready
   - Data nodes ready
   - Autoscaling status

2. **Pod Health**
   - Pod restart count
   - Container ready status
   - Resource utilization

3. **Operator Health**
   - Reconciliation rate
   - Reconciliation errors
   - Queue depth

### Sample PromQL Queries

```promql
# Reconciliation error rate
rate(controller_runtime_reconcile_errors_total{controller="antflycluster"}[5m])

# Reconciliation latency (p99)
histogram_quantile(0.99, rate(controller_runtime_reconcile_time_seconds_bucket{controller="antflycluster"}[5m]))

# Queue depth
workqueue_depth{name="antflycluster"}

# Pod restarts
kube_pod_container_status_restarts_total{pod=~"my-cluster-.*"}
```

## Alerting

### Common Alerts

```yaml
groups:
  - name: antfly-alerts
    rules:
      # Cluster not running
      - alert: AntflyClusterDegraded
        expr: |
          kube_customresource_antfly_cluster_status_phase != "Running"
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Antfly cluster {{ $labels.name }} is not running"

      # High reconciliation error rate
      - alert: AntflyOperatorReconcileErrors
        expr: |
          rate(controller_runtime_reconcile_errors_total{controller="antflycluster"}[5m]) > 0.1
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Antfly operator has high reconciliation error rate"

      # Pod restarts
      - alert: AntflyPodRestarting
        expr: |
          increase(kube_pod_container_status_restarts_total{pod=~".*-metadata-.*|.*-data-.*"}[1h]) > 3
        labels:
          severity: warning
        annotations:
          summary: "Antfly pod {{ $labels.pod }} has restarted multiple times"
```

## Debugging

### Enable Debug Logging

For troubleshooting, enable debug endpoints and logging:

```yaml
spec:
  config: |
    {
      "log": {
        "level": "debug",
        "style": "terminal"
      },
      "enable_debug_endpoints": true
    }
```

### Inspect Pod Details

```bash
# Describe pod for events and conditions
kubectl describe pod my-cluster-metadata-0

# Check container status
kubectl get pod my-cluster-metadata-0 -o jsonpath='{.status.containerStatuses}' | jq

# Check resource usage
kubectl top pod my-cluster-metadata-0
```

### Network Debugging

```bash
# Check service endpoints
kubectl get endpoints my-cluster-metadata

# Test connectivity between pods
kubectl exec my-cluster-data-0 -- nc -zv my-cluster-metadata-0.my-cluster-metadata 12377
```

## Best Practices

1. **Use JSON Logging in Production**: Enable structured logging for log aggregation
2. **Set Up Alerting**: Configure alerts for degraded clusters and operator errors
3. **Monitor Resource Usage**: Track CPU and memory for capacity planning
4. **Review Events Regularly**: Check Kubernetes events for operational issues
5. **Retain Logs**: Configure log retention for troubleshooting historical issues
6. **Dashboard Visibility**: Create dashboards for cluster health visibility
7. **Test Monitoring**: Verify alerts fire correctly during failure scenarios

## See Also

- [Autoscaling](autoscaling.md): Autoscaling configuration and monitoring
- [Troubleshooting](../troubleshooting.md): Common issues and solutions
- [AntflyCluster API Reference](../reference/antflycluster-api.md): Complete status reference
