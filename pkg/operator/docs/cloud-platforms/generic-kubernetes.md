# Generic Kubernetes Deployment Guide

This guide covers deploying Antfly clusters on any Kubernetes distribution, including minikube, kind, on-premises clusters, and other cloud providers.

## Overview

The Antfly Operator works on any Kubernetes cluster that meets the basic requirements. This guide is for deployments that don't use GKE Autopilot or AWS EKS-specific features.

## Prerequisites

- Kubernetes 1.20+ cluster
- `kubectl` installed and configured
- Storage class with dynamic provisioning
- (Optional) metrics-server for autoscaling

## Supported Distributions

The operator has been tested on:

| Distribution | Notes |
|--------------|-------|
| minikube | Local development |
| kind | Local testing |
| k3s | Lightweight production |
| kubeadm | Standard Kubernetes |
| Rancher | Enterprise Kubernetes |
| OpenShift | May require SCC configuration |
| DigitalOcean Kubernetes | Works out of the box |
| Linode Kubernetes Engine | Works out of the box |
| Azure AKS | Works out of the box |

## Installation

```bash
# Install the operator
kubectl apply -f https://antfly.io/antfly-operator-install.yaml

# Verify installation
kubectl get pods -n antfly-operator-namespace
```

## Basic Deployment

### Minimal Cluster

For local development or testing:

```yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: dev-cluster
  namespace: default
spec:
  image: ghcr.io/antflydb/antfly:latest

  metadataNodes:
    replicas: 1  # Single node for development
    metadataAPI:
      port: 12377
    metadataRaft:
      port: 9017
    resources:
      cpu: "100m"
      memory: "128Mi"
      limits:
        cpu: "500m"
        memory: "512Mi"

  dataNodes:
    replicas: 1  # Single node for development
    api:
      port: 12380
    raft:
      port: 9021
    resources:
      cpu: "100m"
      memory: "256Mi"
      limits:
        cpu: "500m"
        memory: "512Mi"

  storage:
    storageClass: "standard"  # Use your cluster's default
    metadataStorage: "500Mi"
    dataStorage: "1Gi"

  config: |
    {
      "log": {
        "level": "debug",
        "style": "terminal"
      }
    }
```

### Production Cluster

For production environments:

```yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: prod-cluster
  namespace: production
spec:
  image: ghcr.io/antflydb/antfly:latest

  metadataNodes:
    replicas: 3
    metadataAPI:
      port: 12377
    metadataRaft:
      port: 9017
    resources:
      cpu: "500m"
      memory: "512Mi"
      limits:
        cpu: "1000m"
        memory: "1Gi"

  dataNodes:
    replicas: 3
    api:
      port: 12380
    raft:
      port: 9021
    resources:
      cpu: "1000m"
      memory: "2Gi"
      limits:
        cpu: "2000m"
        memory: "4Gi"
    autoScaling:
      enabled: true
      minReplicas: 3
      maxReplicas: 10
      targetCPUUtilizationPercentage: 70

  storage:
    storageClass: "fast-ssd"  # Use your high-performance storage class
    metadataStorage: "5Gi"
    dataStorage: "50Gi"

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

## Storage Configuration

### Determine Available Storage Classes

```bash
kubectl get storageclass
```

### Common Storage Classes

| Provider | Storage Class | Description |
|----------|---------------|-------------|
| minikube | `standard` | Default hostPath storage |
| kind | `standard` | Default local storage |
| k3s | `local-path` | Local path provisioner |
| DigitalOcean | `do-block-storage` | Block storage |
| Linode | `linode-block-storage` | Block storage |
| Azure AKS < 1.29 | `managed-csi` or `managed-csi-premium` | Premium SSD with `WaitForFirstConsumer` (LRS, AZ-bound) |
| Azure AKS >= 1.29 | `managed-csi` (default) | Multi-zone clusters auto-use ZRS (zone-redundant) — AZ topology issue eliminated |

### Multi-AZ Storage Considerations

For multi-AZ deployments with zone-bound storage (EBS, GCE PD, Azure Disk LRS), verify your StorageClass uses `WaitForFirstConsumer` binding mode:

```bash
kubectl get storageclass <name> -o yaml | grep volumeBindingMode
```

If it shows `Immediate`, volumes may be provisioned in a different AZ than your pods, causing `volume node affinity conflict` errors. See the [Troubleshooting](../troubleshooting.md#pvcaz-topology-mismatch) guide for details.

| Provider | Recommended StorageClass | volumeBindingMode | Notes |
|----------|--------------------------|-------------------|-------|
| EKS < 1.30 | `gp3` (custom) or `gp2` | `WaitForFirstConsumer` | Must use `ebs.csi.aws.com` provisioner for gp3 |
| EKS >= 1.30 | `gp3` (**must create**) | `WaitForFirstConsumer` | No default StorageClass on EKS 1.30+ |
| GKE Standard | `standard-rwo` or `premium-rwo` | `WaitForFirstConsumer` | Default `standard` uses `Immediate` — avoid for multi-AZ |
| GKE Autopilot | `standard-rwo` (default) | `WaitForFirstConsumer` | Autopilot handles topology internally |
| AKS < 1.29 | `managed-csi` | `WaitForFirstConsumer` | LRS disks are AZ-bound |
| AKS >= 1.29 | `managed-csi` | `WaitForFirstConsumer` | ZRS for multi-zone — AZ problem eliminated |
| Generic | Must verify | Must be `WaitForFirstConsumer` | Check with `kubectl get sc <name> -o yaml` |

### Using Custom Storage Class

```yaml
spec:
  storage:
    storageClass: "your-storage-class"
    metadataStorage: "1Gi"
    dataStorage: "10Gi"
```

## Local Development

### minikube Setup

```bash
# Start minikube with sufficient resources
minikube start --cpus=4 --memory=7500 --disk-size=50g

# Enable metrics-server for autoscaling
minikube addons enable metrics-server

# Install operator
kubectl apply -f https://antfly.io/antfly-operator-install.yaml

# Deploy development cluster (600m total CPU: 3x100m metadata + 3x100m data)
kubectl apply -f https://antfly.io/examples/development-cluster.yaml

# Access the cluster
kubectl port-forward svc/antfly-dev-cluster-metadata -n antfly-dev 12377:12377
```

> **Docker Desktop users:** If using `--memory` values above 7500, ensure Docker Desktop's memory allocation is set to at least 8GB in Docker Desktop Settings > Resources.

> **Minikube Docker driver:** With the Docker driver, NodePort services are not directly accessible from the host. Use `kubectl port-forward` (recommended for development), `minikube service <service-name>` to open in a browser, or `minikube tunnel` for LoadBalancer external IPs.

### kind Setup

```bash
# Create cluster
kind create cluster --name antfly-dev

# Install metrics-server (optional)
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml

# Patch metrics-server for kind (insecure TLS)
kubectl patch deployment metrics-server -n kube-system --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'

# Install operator
kubectl apply -f https://antfly.io/antfly-operator-install.yaml

# Deploy cluster
kubectl apply -f examples/small-dev-cluster.yaml
```

### k3s Setup

```bash
# Install k3s (already includes metrics-server)
curl -sfL https://get.k3s.io | sh -

# Configure kubectl
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

# Install operator
kubectl apply -f https://antfly.io/antfly-operator-install.yaml

# Deploy cluster
kubectl apply -f examples/production-cluster.yaml
```

## Service Exposure

### ClusterIP (Internal Only)

```yaml
spec:
  publicAPI:
    enabled: true
    serviceType: ClusterIP
    port: 80
```

Access via port-forward:
```bash
kubectl port-forward svc/<cluster>-public-api 8080:80
```

### NodePort

```yaml
spec:
  publicAPI:
    enabled: true
    serviceType: NodePort
    port: 80
    nodePort: 30100  # Optional: specify port (30000-32767)
```

Access via any node IP:
```bash
curl http://<node-ip>:30100
```

### LoadBalancer

```yaml
spec:
  publicAPI:
    enabled: true
    serviceType: LoadBalancer
    port: 80
```

Works on cloud providers with LoadBalancer support. For bare metal, use MetalLB:

```bash
# Install MetalLB
kubectl apply -f https://raw.githubusercontent.com/metallb/metallb/v0.13.7/config/manifests/metallb-native.yaml

# Configure IP pool (example)
kubectl apply -f - <<EOF
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: default-pool
  namespace: metallb-system
spec:
  addresses:
  - 192.168.1.240-192.168.1.250
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: default
  namespace: metallb-system
EOF
```

### Ingress

For custom Ingress configuration, disable the public API service:

```yaml
spec:
  publicAPI:
    enabled: false
```

Then create your own Ingress:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: antfly-ingress
  annotations:
    nginx.ingress.kubernetes.io/backend-protocol: "HTTP"
spec:
  rules:
  - host: antfly.example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: my-cluster-metadata
            port:
              number: 12377
```

## Resource Considerations

### Minimum Requirements

| Node Type | CPU Request | Memory Request |
|-----------|-------------|----------------|
| Metadata | 100m | 128Mi |
| Data | 100m | 256Mi |

### Recommended Production

| Node Type | CPU Request | Memory Request | CPU Limit | Memory Limit |
|-----------|-------------|----------------|-----------|--------------|
| Metadata | 500m | 512Mi | 1000m | 1Gi |
| Data | 1000m | 2Gi | 2000m | 4Gi |

### Resource Quotas

If your namespace has resource quotas, ensure sufficient allocation:

```yaml
apiVersion: v1
kind: ResourceQuota
metadata:
  name: antfly-quota
spec:
  hard:
    requests.cpu: "10"
    requests.memory: "20Gi"
    limits.cpu: "20"
    limits.memory: "40Gi"
    persistentvolumeclaims: "10"
```

## Autoscaling

### Prerequisites

Install metrics-server if not present:

```bash
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
```

Verify metrics are available:

```bash
kubectl top nodes
kubectl top pods
```

### Enable Autoscaling

```yaml
spec:
  dataNodes:
    replicas: 3
    resources:
      cpu: "500m"       # Required for CPU-based scaling
      memory: "1Gi"     # Required for memory-based scaling
    autoScaling:
      enabled: true
      minReplicas: 3
      maxReplicas: 10
      targetCPUUtilizationPercentage: 70
      targetMemoryUtilizationPercentage: 80
```

## Network Policies

If your cluster uses NetworkPolicies, allow traffic between pods:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-antfly-internal
  namespace: default
spec:
  podSelector:
    matchLabels:
      app: antfly
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - podSelector:
        matchLabels:
          app: antfly
  egress:
  - to:
    - podSelector:
        matchLabels:
          app: antfly
```

## Pod Security

### Pod Security Standards

For clusters with Pod Security Standards (PSS), the Antfly containers run as non-root by default and should work with `restricted` policy.

### OpenShift Security Context Constraints

For OpenShift, create an SCC:

```yaml
apiVersion: security.openshift.io/v1
kind: SecurityContextConstraints
metadata:
  name: antfly-scc
allowPrivilegedContainer: false
runAsUser:
  type: MustRunAsNonRoot
seLinuxContext:
  type: MustRunAs
fsGroup:
  type: MustRunAs
volumes:
  - configMap
  - emptyDir
  - persistentVolumeClaim
  - secret
```

## Troubleshooting

### Storage Issues

```bash
# Check PVCs
kubectl get pvc -l app=antfly

# Check storage provisioner
kubectl get pods -n kube-system | grep -E "(provisioner|csi)"

# Describe PVC for errors
kubectl describe pvc <pvc-name>
```

### Pods Not Scheduling

```bash
# Check pod events
kubectl describe pod <pod-name>

# Check node resources
kubectl describe nodes | grep -A 5 "Allocated resources"

# Check resource quotas
kubectl describe resourcequota
```

### Networking Issues

```bash
# Test pod connectivity
kubectl exec -it <metadata-pod> -- ping <data-pod-ip>

# Check services
kubectl get svc -l app=antfly
kubectl describe svc <service-name>

# Check endpoints
kubectl get endpoints -l app=antfly
```

### Metrics Server Issues

```bash
# Check metrics-server
kubectl get pods -n kube-system | grep metrics-server
kubectl logs -n kube-system -l k8s-app=metrics-server

# Test metrics API
kubectl top pods
```

## Best Practices

1. **Use Namespaces**: Isolate Antfly clusters in dedicated namespaces
2. **Set Resource Limits**: Prevent runaway resource consumption
3. **Enable Autoscaling**: For production workloads with variable load
4. **Configure Storage**: Use appropriate storage class for your workload
5. **Monitor Resources**: Set up monitoring for cluster health
6. **Backup Regularly**: Configure AntflyBackup for data protection
7. **Test Failover**: Verify high availability works as expected

## Example Configurations

See the `examples/` directory for ready-to-use configurations:

| Example | Use Case |
|---------|----------|
| `small-dev-cluster.yaml` | Minimal resources for development |
| `development-cluster.yaml` | Development with debug logging |
| `production-cluster.yaml` | Production-ready configuration |
| `autoscaling-cluster.yaml` | With autoscaling enabled |

## Next Steps

- [Backup & Restore](../operations/backup-restore.md): Configure data protection
- [Autoscaling](../operations/autoscaling.md): Fine-tune autoscaling
- [Monitoring](../operations/monitoring.md): Set up observability
- [Service Mesh](../security/service-mesh.md): Enable mTLS
