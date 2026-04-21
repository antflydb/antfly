# Quickstart

Deploy your first Antfly cluster in 5 minutes.

## Prerequisites

- Operator installed (see [Installation](installation.md))
- `kubectl` configured for your cluster

## Local Development (Minikube/kind)

> **Note:** The default quickstart manifest below is sized for cloud Kubernetes (2250m total CPU, EKS storage class). If you're using **Minikube** or **kind**, use the `development-cluster.yaml` example instead — it requests only 600m total CPU and uses the cluster's default storage class:
>
> ```bash
> kubectl create namespace antfly-dev
> kubectl apply -f https://antfly.io/examples/development-cluster.yaml
> ```
>
> Then skip to [Step 3: Monitor Cluster Status](#step-3-monitor-cluster-status).

## Step 1: Create a Namespace

```bash
kubectl create namespace antfly-demo
```

## Step 2: Create an AntflyCluster

Create a file named `my-cluster.yaml`:

```yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: my-cluster
  namespace: antfly-demo
spec:
  image: ghcr.io/antflydb/antfly:latest

  metadataNodes:
    replicas: 3
    metadataAPI:
      port: 12377
    metadataRaft:
      port: 9017
    resources:
      cpu: "250m"
      memory: "256Mi"
      limits:
        cpu: "500m"
        memory: "512Mi"

  dataNodes:
    replicas: 3
    api:
      port: 12380
    raft:
      port: 9021
    resources:
      cpu: "500m"
      memory: "512Mi"
      limits:
        cpu: "1000m"
        memory: "1Gi"

  # Creates an internet-facing LoadBalancer service for the API.
  # Set enabled: false if you don't want this exposed to the internet
  # (you can still access the API via kubectl port-forward).
  publicAPI:
    enabled: true

  storage:
    # Choose the storage class for your platform:
    #   EKS:      "gp2" (default) or create "gp3" (see AWS EKS guide)
    #   GKE:      "standard-rwo"
    #   minikube: "standard"
    #   Other:    "" (uses cluster default)
    storageClass: ""
    metadataStorage: "1Gi"
    dataStorage: "5Gi"

  config: |
    {
      "log": {
        "level": "info",
        "style": "terminal"
      }
    }
```

Apply the manifest:

```bash
kubectl apply -f my-cluster.yaml
```

## Step 3: Monitor Cluster Status

Watch the cluster come up:

```bash
# Watch pods being created
kubectl get pods -n antfly-demo -w

# Check cluster status
kubectl get antflycluster -n antfly-demo
```

Example output:
```
NAME         PHASE     METADATA   DATA   AGE
my-cluster   Running   3          3      2m
```

## Step 4: Verify the Cluster

Check that all components are running:

```bash
# All pods should be Running
kubectl get pods -n antfly-demo

# NAME                      READY   STATUS    RESTARTS   AGE
# my-cluster-data-0         1/1     Running   0          2m
# my-cluster-data-1         1/1     Running   0          90s
# my-cluster-data-2         1/1     Running   0          60s
# my-cluster-metadata-0     1/1     Running   0          2m
# my-cluster-metadata-1     1/1     Running   0          90s
# my-cluster-metadata-2     1/1     Running   0          60s
```

Check services:

```bash
kubectl get svc -n antfly-demo

# NAME                      TYPE           CLUSTER-IP      EXTERNAL-IP   PORT(S)
# my-cluster-metadata       ClusterIP      None            <none>        12377/TCP,9017/TCP
# my-cluster-data           ClusterIP      None            <none>        12380/TCP,9021/TCP
# my-cluster-public-api     LoadBalancer   10.0.123.47     <pending>     80/TCP
```

## Step 5: Connect to the Cluster

### Option A: Port Forward (Development)

```bash
# Forward the metadata API port
kubectl port-forward -n antfly-demo svc/my-cluster-metadata 12377:12377
```

In another terminal, test the connection:
```bash
# Verify the API is accessible
curl http://localhost:12377/api/v1/tables
```

### Option B: Use the Public API (Production)

If using LoadBalancer (cloud environments):

```bash
# Get the external IP
kubectl get svc -n antfly-demo my-cluster-public-api

# Connect using the external IP
curl http://<EXTERNAL-IP>/api/v1/tables
```

## Using Your Cluster

Once connected, try some basic API operations:

```bash
# Create a table
curl -X POST http://localhost:12377/api/v1/tables/my-table \
  -H "Content-Type: application/json" \
  -d '{}'
```

> **Note:** After creating a table, wait a few seconds for shard assignment to complete before inserting or querying data. You can check readiness with `curl http://localhost:12377/api/v1/tables/my-table` — once the table metadata includes shard information, it's ready.

```bash
# Insert a document
curl -X POST http://localhost:12377/api/v1/tables/my-table/batch \
  -H "Content-Type: application/json" \
  -d '{"inserts": {"doc1": {"title": "Hello World"}}}'

# Query
curl -X POST http://localhost:12377/api/v1/tables/my-table/query \
  -H "Content-Type: application/json" \
  -d '{"full_text_search": {"query": "Hello"}}'
```

For full API documentation, see the [API Reference](/docs/api/getting-started). Client SDKs are available for [Go](https://github.com/antflydb/antfly/tree/main/pkg), [TypeScript](https://github.com/antflydb/antfly/tree/main/ts), and [Python](https://github.com/antflydb/antfly/tree/main/py).

## Step 6: Clean Up

When you're done testing:

```bash
# Delete the cluster
kubectl delete antflycluster my-cluster -n antfly-demo

# Delete the namespace
kubectl delete namespace antfly-demo
```

## What's Next?

Now that you have a basic cluster running, explore:

- [Concepts](concepts.md): Understand the architecture
- [Cloud Platforms](../cloud-platforms/): Platform-specific optimizations
  - [AWS EKS](../cloud-platforms/aws-eks.md): Spot Instances, IRSA
  - [GCP GKE](../cloud-platforms/gcp-gke.md): Autopilot, Spot Pods
- [Autoscaling](../operations/autoscaling.md): Automatic scaling
- [Backup & Restore](../operations/backup-restore.md): Data protection

## Example Configurations

The repository includes ready-to-use examples:

| Example | Description |
|---------|-------------|
| `examples/small-dev-cluster.yaml` | Minimal resources for development |
| `examples/development-cluster.yaml` | Development cluster with debug logging |
| `examples/production-cluster.yaml` | Production-ready configuration |
| `examples/autoscaling-cluster.yaml` | With autoscaling enabled |

Apply any example:
```bash
kubectl apply -f https://antfly.io/examples/small-dev-cluster.yaml
```

## Quick Reference

### Check Cluster Status

```bash
kubectl get antflycluster -n <namespace>
kubectl describe antflycluster <name> -n <namespace>
```

### View Logs

```bash
# Operator logs
kubectl logs -n antfly-operator-namespace -l app.kubernetes.io/name=antfly-operator

# Pod logs
kubectl logs -n <namespace> <pod-name>
```

### Common Commands

```bash
# List all clusters
kubectl get antflycluster --all-namespaces

# Watch cluster events
kubectl get events -n <namespace> --field-selector involvedObject.name=<cluster-name>

# Check PVCs
kubectl get pvc -n <namespace> -l app.kubernetes.io/instance=<cluster-name>
```
