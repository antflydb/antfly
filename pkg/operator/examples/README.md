# Antfly Operator Examples

This directory contains example configurations for deploying Antfly database clusters using the Antfly Operator.

## Quick Start

1. **Deploy the operator** (if not already deployed):
   ```bash
   kubectl apply -f deploy/install.yaml
   ```

2. **Choose an example** and deploy it:
   ```bash
   kubectl apply -f examples/small-dev-cluster.yaml
   ```

3. **Check the cluster status**:
   ```bash
   kubectl get antflyclusters
   kubectl get pods -l app=antfly
   ```

## Available Examples

### Simple Cluster (`small-dev-cluster.yaml`)
A basic Antfly cluster suitable for testing and small workloads.

**Features:**
- 3 leader nodes (consensus layer)
- 3 data nodes (storage layer)
- Moderate resource allocation
- Standard storage class

**Use Case:** Testing, development, small applications

**Deploy:**
```bash
kubectl apply -f examples/small-dev-cluster.yaml
```

### Production Cluster (`production-cluster.yaml`)
A production-ready Antfly cluster with high availability and performance.

**Features:**
- 3 leader nodes with high resources
- 3 data nodes with high storage and compute
- High-performance storage
- Production-optimized configuration

**Use Case:** Production workloads, high availability requirements

**Prerequisites:** Ensure you have a `fast-ssd` storage class or modify the `storageClass` field.

**Deploy:**
```bash
# Create production namespace first
kubectl create namespace production

kubectl apply -f examples/production-cluster.yaml
```

### Development Cluster (`development-cluster.yaml`)
A minimal Antfly cluster optimized for development and testing.

**Features:**
- 3 leader nodes with minimal resources
- 3 data nodes with minimal storage
- Debug logging enabled
- Minimal resource usage

**Use Case:** Local development, CI/CD testing, resource-constrained environments

**Deploy:**
```bash
# Create development namespace first
kubectl create namespace antfly-dev-ns


kubectl apply -f examples/development-cluster.yaml
```

## Configuration Overview

### Core Components

Each Antfly cluster consists of two main components:

1. **Leader Nodes** (StatefulSet)
   - Handle cluster coordination and consensus
   - Always requires exactly 3 replicas for Raft consensus
   - Stores cluster metadata
   - Serves the public API

2. **Data Nodes** (StatefulSet)
   - Handle data storage and replication
   - Always requires exactly 3 replicas for Raft consensus
   - Stores the actual database data

### Key Configuration Options

```yaml
spec:
  image: antfly:latest              # Container image
  imagePullPolicy: IfNotPresent     # Image pull policy

  metadataNodes:
    replicas: 3                     # Always 3 for Raft
    resources: {...}                # CPU/memory allocation

  dataNodes:
    replicas: 3                     # Always 3 for Raft
    resources: {...}                # CPU/memory allocation

  storage:
    storageClass: "standard"        # Kubernetes storage class
    metadataStorage: "1Gi"         # Storage for metadata nodes
    dataStorage: "5Gi"             # Storage for data nodes

  config: |                        # JSON configuration
    {
      "log": {
        "level": "info",
        "style": "json"
      },
      "cache_size": 1000,
      ...
    }
```

## Accessing Your Database

Once deployed, you can access your Antfly database:

### 1. Port Forward (Development)
```bash
kubectl port-forward service/simple-antfly-cluster-public-api 8080:80
```

Then access at: `http://localhost:8080`

### 2. NodePort Service (Minikube)
```bash
minikube service simple-antfly-cluster-public-api
```

### 3. LoadBalancer/Ingress (Production)
Configure an Ingress or LoadBalancer service to expose the `*-public-api` service.

## Monitoring and Management

### Check Cluster Status
```bash
# View cluster status
kubectl get antflyclusters

# View pods
kubectl get pods -l app=antfly

# View services
kubectl get svc -l app=antfly

# View storage
kubectl get pvc -l app=antfly
```

### Scale Data Nodes
```bash
# Scale data nodes for more storage capacity (optional)
kubectl patch antflycluster simple-antfly-cluster --type='merge' -p='{"spec":{"dataNodes":{"replicas":5}}}'
```

### View Logs
```bash
# Leader node logs
kubectl logs simple-antfly-cluster-leader-0

# Data node logs
kubectl logs simple-antfly-cluster-data-0

# Another data node logs
kubectl logs simple-antfly-cluster-data-1
```

## Customization

### Custom Storage Classes
Modify the `storageClass` field to use your preferred storage:
```yaml
storage:
  storageClass: "fast-ssd"  # or "gp2", "premium-ssd", etc.
```

### Resource Tuning
Adjust resources based on your needs:
```yaml
dataNodes:
  resources:
    cpu: "2000m"     # 2 CPU cores
    memory: "4Gi"    # 4GB RAM
    limits:
      cpu: "4000m"   # 4 CPU cores max
      memory: "8Gi"  # 8GB RAM max
```

### Configuration Tuning
Modify the `config` section for database-specific settings:
```yaml
config: |
  {
    "log": {
      "level": "debug",
      "style": "terminal"
    },
    "cache_size": 5000,
    "max_connections": 1000,
    "raft_heartbeat_timeout": "100ms"
  }
```

## Troubleshooting

### Common Issues

1. **Pods stuck in Pending**
   - Check storage class availability: `kubectl get storageclass`
   - Check resource quotas: `kubectl describe namespace <namespace>`

2. **Pods stuck in ImagePullBackOff**
   - Verify image exists and is accessible
   - Check image pull secrets if using private registry

3. **Database not accessible**
   - Check service status: `kubectl get svc`
   - Verify port forwarding or ingress configuration
   - Check pod logs for errors

### Getting Help

```bash
# Check operator logs
kubectl logs deployment/antfly-operator -n antfly-operator-namespace

# Check cluster events
kubectl get events --sort-by=.metadata.creationTimestamp

# Describe cluster for detailed status
kubectl describe antflycluster <cluster-name>
```

## Cleanup

To remove a cluster:
```bash
kubectl delete antflycluster simple-antfly-cluster
```

To remove the operator:
```bash
kubectl delete -f deploy/install.yaml
