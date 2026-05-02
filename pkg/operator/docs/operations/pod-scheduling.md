# Pod Scheduling

Configure pod placement for Antfly clusters using node selectors, affinities, tolerations, and topology spread constraints.

## Overview

Proper pod scheduling is critical for running Antfly in production:

- **High Availability**: Spread pods across failure domains (zones, nodes) to survive outages
- **Performance Isolation**: Dedicate nodes to Antfly to avoid noisy-neighbor issues
- **Cost Optimization**: Run data nodes on Spot instances while keeping metadata nodes on stable, on-demand capacity
- **Compliance**: Pin workloads to specific regions or node types

Antfly's two node types have different scheduling needs. Metadata nodes run Raft consensus and require stability — even brief disruptions can cause leader elections and temporary unavailability. Data nodes are replicated and tolerate disruption better, making them candidates for Spot capacity and more aggressive spreading.

## Scheduling Concepts

| Mechanism | What It Does | CRD Field |
|-----------|-------------|-----------|
| Node Selectors | Hard constraint: pods only run on nodes with matching labels | `nodeSelector` |
| Tolerations | Allow pods to schedule on tainted nodes | `tolerations` |
| Node Affinity | Soft or hard preference for nodes with specific labels | `affinity.nodeAffinity` |
| Pod Anti-Affinity | Spread pods away from each other | `affinity.podAntiAffinity` |
| Topology Spread | Even distribution across zones or nodes | `topologySpreadConstraints` |
| Taints | Node-side: repel pods that lack matching tolerations | Infrastructure-level (node pool config) |

## CRD Scheduling Fields

Both `metadataNodes` and `dataNodes` support these scheduling fields:

```yaml
spec:
  metadataNodes:
    tolerations: []              # []corev1.Toleration
    nodeSelector: {}             # map[string]string
    affinity: {}                 # corev1.Affinity
    topologySpreadConstraints: [] # []corev1.TopologySpreadConstraint
  dataNodes:
    tolerations: []
    nodeSelector: {}
    affinity: {}
    topologySpreadConstraints: []
```

All fields use standard Kubernetes types. The operator applies user-specified values **first**, then merges cloud-provider-specific scheduling on top (see [How Cloud-Provider Scheduling Composes](#how-cloud-provider-scheduling-composes)).

**Important**: `nodeSelector` is rejected by the validating webhook when `spec.gke.autopilot: true`. GKE Autopilot manages scheduling via compute classes — use `spec.gke.autopilotComputeClass` instead.

## Common Scenarios

### Dedicated Node Pools with Taints

Isolate Antfly on dedicated nodes by tainting a node pool and adding matching tolerations and selectors in the CRD.

**Step 1: Create and taint the node pool**

EKS (eksctl):

```bash
eksctl create nodegroup \
  --cluster my-cluster \
  --name antfly-pool \
  --node-type m6i.2xlarge \
  --nodes 3 \
  --node-labels workload=antfly \
  --node-taints dedicated=antfly:NoSchedule
```

GKE Standard:

```bash
gcloud container node-pools create antfly-pool \
  --cluster my-cluster \
  --machine-type e2-standard-8 \
  --num-nodes 3 \
  --node-labels workload=antfly \
  --node-taints dedicated=antfly:NoSchedule
```

Generic Kubernetes:

```bash
kubectl label nodes node-1 node-2 node-3 workload=antfly
kubectl taint nodes node-1 node-2 node-3 dedicated=antfly:NoSchedule
```

**Step 2: Configure the CRD**

```yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: my-cluster
spec:
  metadataNodes:
    replicas: 3
    nodeSelector:
      workload: antfly
    tolerations:
      - key: dedicated
        operator: Equal
        value: antfly
        effect: NoSchedule
  dataNodes:
    replicas: 3
    nodeSelector:
      workload: antfly
    tolerations:
      - key: dedicated
        operator: Equal
        value: antfly
        effect: NoSchedule
```

### Zone-Aware Scheduling

**Default behavior**: New AntflyCluster deployments automatically get a soft zone topology spread constraint (`whenUnsatisfiable: ScheduleAnyway`, `maxSkew: 1`, `topologyKey: topology.kubernetes.io/zone`) applied to both metadata and data StatefulSets. This ensures pods are distributed across AZs when possible, without blocking scheduling on single-zone or imbalanced clusters.

The default spread is skipped when:
- You specify explicit `topologySpreadConstraints` in the CRD (your constraints take precedence)
- GKE Autopilot is enabled (Autopilot manages topology internally)

To override the default with a hard zone spread, specify explicit constraints:

```yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: my-cluster
spec:
  metadataNodes:
    replicas: 3
    topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: topology.kubernetes.io/zone
        whenUnsatisfiable: DoNotSchedule
        labelSelector:
          matchLabels:
            app.kubernetes.io/name: antfly-database
            app.kubernetes.io/component: metadata
  dataNodes:
    replicas: 6
    topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: topology.kubernetes.io/zone
        whenUnsatisfiable: ScheduleAnyway
        labelSelector:
          matchLabels:
            app.kubernetes.io/name: antfly-database
            app.kubernetes.io/component: data
```

**StorageClass requirement**: For zone-aware scheduling with persistent volumes, use a `StorageClass` with `volumeBindingMode: WaitForFirstConsumer` so that PVs are provisioned in the same zone as the pod. Using `Immediate` binding can cause PVs to be provisioned in the wrong AZ.

**Cross-cloud StorageClass reference:**

| Provider | Recommended StorageClass | volumeBindingMode | Notes |
|----------|--------------------------|-------------------|-------|
| EKS < 1.30 | `gp3` (custom) or default `gp2` | `WaitForFirstConsumer` | Must use `ebs.csi.aws.com` provisioner for gp3 |
| EKS >= 1.30 | `gp3` (custom, **must create**) | `WaitForFirstConsumer` | **No default StorageClass on EKS 1.30+** |
| GKE Standard | `standard-rwo` or `premium-rwo` | `WaitForFirstConsumer` | **Default `standard` uses `Immediate`** |
| GKE Autopilot | `standard-rwo` (default) | `WaitForFirstConsumer` | Autopilot handles topology internally |
| AKS < 1.29 | `managed-csi` | `WaitForFirstConsumer` | LRS disks are AZ-bound |
| AKS >= 1.29 | `managed-csi` (default) | `WaitForFirstConsumer` | Multi-zone auto-uses ZRS |
| Generic | Must verify | Must be `WaitForFirstConsumer` | Check with `kubectl get sc <name> -o yaml` |

### Pod Anti-Affinity (One Pod per Node)

Prevent multiple pods of the same type from landing on the same node:

```yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: my-cluster
spec:
  metadataNodes:
    replicas: 3
    affinity:
      podAntiAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          - labelSelector:
              matchLabels:
                app.kubernetes.io/name: antfly
                app.kubernetes.io/component: metadata
            topologyKey: kubernetes.io/hostname
  dataNodes:
    replicas: 3
    affinity:
      podAntiAffinity:
        preferredDuringSchedulingIgnoredDuringExecution:
          - weight: 100
            podAffinityTerm:
              labelSelector:
                matchLabels:
                  app.kubernetes.io/name: antfly
                  app.kubernetes.io/component: data
              topologyKey: kubernetes.io/hostname
```

This example uses a hard requirement for metadata nodes (Raft consensus requires distinct failure domains) and a soft preference for data nodes (so pods can still schedule when node count is limited).

### Combining with EKS Spot Instances

When EKS Spot is enabled (`spec.eks.enabled: true`, `spec.eks.spot.dataNodes: true`), the operator automatically adds Spot tolerations and node selectors to data node pods. Your user-specified scheduling fields compose with these.

**CRD input:**

```yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: my-cluster
spec:
  eks:
    enabled: true
    spot:
      dataNodes: true
  dataNodes:
    replicas: 5
    nodeSelector:
      workload: antfly
    tolerations:
      - key: dedicated
        operator: Equal
        value: antfly
        effect: NoSchedule
    topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: topology.kubernetes.io/zone
        whenUnsatisfiable: ScheduleAnyway
        labelSelector:
          matchLabels:
            app.kubernetes.io/name: antfly
            app.kubernetes.io/component: data
```

**Resulting pod template (after operator merges):**

```yaml
# nodeSelector (user + EKS Spot merged)
nodeSelector:
  workload: antfly                          # from CRD
  eks.amazonaws.com/capacityType: "SPOT"    # added by operator

# tolerations (user + EKS Spot appended)
tolerations:
  - key: dedicated                          # from CRD
    operator: Equal
    value: antfly
    effect: NoSchedule
  - key: eks.amazonaws.com/spot             # added by operator
    operator: Exists
    effect: NoSchedule

# topologySpreadConstraints (passed through)
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: topology.kubernetes.io/zone
    whenUnsatisfiable: ScheduleAnyway
    ...
```

### Karpenter (EKS)

If you use [Karpenter](https://karpenter.sh/) for node provisioning on EKS, create a `NodePool` that provisions nodes matching your CRD scheduling constraints:

```yaml
apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: antfly
spec:
  template:
    metadata:
      labels:
        workload: antfly
    spec:
      taints:
        - key: dedicated
          value: antfly
          effect: NoSchedule
      requirements:
        - key: karpenter.sh/capacity-type
          operator: In
          values: ["on-demand"]
        - key: node.kubernetes.io/instance-type
          operator: In
          values: ["m6i.2xlarge", "m6i.4xlarge", "m7i.2xlarge", "m7i.4xlarge"]
        - key: topology.kubernetes.io/zone
          operator: In
          values: ["us-east-1a", "us-east-1b", "us-east-1c"]
  limits:
    cpu: "128"
    memory: 512Gi
```

For data nodes on Spot, create a separate NodePool:

```yaml
apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: antfly-spot
spec:
  template:
    metadata:
      labels:
        workload: antfly-data
    spec:
      taints:
        - key: dedicated
          value: antfly
          effect: NoSchedule
      requirements:
        - key: karpenter.sh/capacity-type
          operator: In
          values: ["spot"]
        - key: node.kubernetes.io/instance-type
          operator: In
          values: ["m6i.2xlarge", "m6i.4xlarge", "m7i.2xlarge", "m7i.4xlarge", "c6i.2xlarge"]
        - key: topology.kubernetes.io/zone
          operator: In
          values: ["us-east-1a", "us-east-1b", "us-east-1c"]
  limits:
    cpu: "256"
    memory: 1Ti
  disruption:
    consolidationPolicy: WhenEmptyOrUnderutilized
    consolidateAfter: 60s
```

## How Cloud-Provider Scheduling Composes

The operator applies scheduling in two phases:

1. **User constraints** from the CRD are applied via `applySchedulingConstraints`
2. **Cloud-provider values** are merged on top by `applyEKSPodSpec` or `applyGKEPodSpec`

| Field | Merge Strategy | Detail |
|-------|---------------|--------|
| `tolerations` | Appended | Cloud-provider tolerations are appended to user list (duplicates avoided) |
| `nodeSelector` | Key-merged | Cloud-provider keys are added to user map; duplicate keys are overwritten by cloud-provider |
| `affinity` (preferred terms) | Appended | Cloud-provider preferred scheduling terms are appended to user list |
| `affinity` (required terms) | User wins | If user specifies required node affinity, it takes precedence |
| `affinity` (pod affinity/anti-affinity) | User wins | User-specified pod affinity/anti-affinity is preserved as-is |
| `topologySpreadConstraints` | Appended | Cloud-provider constraints are appended to user list |

**What each cloud provider adds automatically:**

| Provider Mode | nodeSelector | Tolerations | Affinity | Other |
|--------------|-------------|-------------|----------|-------|
| EKS + Spot | `eks.amazonaws.com/capacityType: SPOT` | Spot toleration | Instance type preference (weight 100) | terminationGracePeriod: 25s |
| GKE Standard + Spot | `cloud.google.com/gke-spot: "true"` | — | — | terminationGracePeriod: 15s |
| GKE Autopilot | nodeSelector cleared | — | — | Compute class annotation; terminationGracePeriod: 15s |

## Best Practices

### Metadata Nodes vs. Data Nodes

| Consideration | Metadata Nodes | Data Nodes |
|--------------|---------------|------------|
| Spot/Preemptible | Never — Raft leader election disrupts the cluster | Safe with 3+ replicas and PDBs |
| Dedicated nodes | Recommended for production | Optional; depends on workload isolation needs |
| Instance types | Memory-optimized (Raft state is in-memory) | Storage-optimized or general-purpose |
| Anti-affinity | Hard (required) — one per node | Soft (preferred) — allows co-location when needed |
| Zone spread | Required for HA (3 zones for 3 replicas) | Recommended; use `ScheduleAnyway` to avoid unschedulable pods |

### General Recommendations

- **Always enable PodDisruptionBudgets** for production clusters to protect against voluntary disruptions during upgrades and scaling
- **Use soft topology spread** (`whenUnsatisfiable: ScheduleAnyway`) for data nodes to avoid pods stuck in Pending when zone capacity is limited
- **Set resource requests and limits** on all pods to prevent noisy-neighbor effects and enable accurate bin-packing
- **Use `WaitForFirstConsumer`** storage class when combining zone-aware scheduling with persistent volumes
- **Test scheduling in staging** before production — use `kubectl describe pod` to verify placement matches expectations

## Troubleshooting

### Pods Stuck in Pending

Check the pod events for scheduling failure reasons:

```bash
kubectl describe pod my-cluster-metadata-0
```

Common messages and solutions:

| Event Message | Cause | Solution |
|--------------|-------|---------|
| `0/N nodes are available: N node(s) had untolerated taint` | Missing toleration | Add matching toleration to CRD |
| `0/N nodes are available: N node(s) didn't match Pod's node affinity/selector` | No nodes match nodeSelector or affinity | Verify node labels match CRD selectors |
| `0/N nodes are available: N node(s) didn't satisfy topology spread constraint` | Cannot satisfy `maxSkew` with `DoNotSchedule` | Switch to `ScheduleAnyway` or add nodes in under-represented zones |
| `0/N nodes are available: N too many pods` | Node is full | Add more nodes or reduce pod resource requests |

### Verifying Applied Scheduling

Inspect the StatefulSet to confirm the operator applied your scheduling fields:

```bash
# Check nodeSelector
kubectl get statefulset my-cluster-metadata -o jsonpath='{.spec.template.spec.nodeSelector}' | jq

# Check tolerations
kubectl get statefulset my-cluster-metadata -o jsonpath='{.spec.template.spec.tolerations}' | jq

# Check affinity
kubectl get statefulset my-cluster-metadata -o jsonpath='{.spec.template.spec.affinity}' | jq

# Check topology spread constraints
kubectl get statefulset my-cluster-metadata -o jsonpath='{.spec.template.spec.topologySpreadConstraints}' | jq
```

### GKE Autopilot nodeSelector Error

If you see a webhook validation error like:

```
spec.metadataNodes.nodeSelector conflicts with spec.gke.autopilot=true
```

Remove `nodeSelector` from your CRD and use `spec.gke.autopilotComputeClass` to control scheduling on GKE Autopilot.

## See Also

- [AWS EKS](../cloud-platforms/aws-eks.md): EKS deployment guide with Spot Instances and IRSA
- [GCP GKE](../cloud-platforms/gcp-gke.md): GKE deployment guide with Autopilot and Spot Pods
- [Generic Kubernetes](../cloud-platforms/generic-kubernetes.md): Deployment on any Kubernetes distribution
- [Autoscaling](autoscaling.md): Automatic scaling of data nodes
- [AntflyCluster API Reference](../reference/antflycluster-api.md): Complete CRD field reference
