# AWS EKS Deployment Guide

This guide covers deploying Antfly clusters on Amazon Elastic Kubernetes Service (EKS) with cost optimization using Spot Instances.

## Overview

The Antfly Operator has built-in support for AWS EKS including:

- **Spot Instances**: Up to 90% cost savings for fault-tolerant workloads
- **Pod Disruption Budgets**: Automatic protection during cluster maintenance
- **IRSA Integration**: IAM Roles for Service Accounts for secure AWS API access
- **EBS Configuration**: Customizable volume types and encryption
- **Instance Type Affinity**: Prefer specific EC2 instance types

## Prerequisites

- AWS account with appropriate permissions
- `aws` CLI installed and configured
- `kubectl` installed
- [`eksctl`](https://eksctl.io/installation/) installed (recommended for cluster creation)
- EKS cluster with:
  - VPC CNI addon (`vpc-cni`) — included by default with `eksctl`, but must be explicitly enabled if provisioning via Terraform or the AWS console
  - EBS CSI driver installed
  - metrics-server (for autoscaling)

## Creating an EKS Cluster

### Using eksctl

```bash
# Create a basic EKS cluster
# Check latest EKS version: aws eks describe-addon-versions
eksctl create cluster \
  --name antfly-cluster \
  --region us-east-2 \
  --version 1.31 \
  --nodegroup-name standard-nodes \
  --node-type m5.large \
  --nodes 4 \
  --nodes-min 3 \
  --nodes-max 6 \
  --managed
```

> **Note:** Replace `us-east-2` with your preferred AWS region throughout this guide.

> **Capacity planning:** The example AntflyCluster below requests 4.5 vCPU total (3 metadata × 500m + 3 data × 1000m). An m5.large provides 2 vCPU, but each node reserves capacity for system pods (kube-proxy, CoreDNS, VPC CNI, EBS CSI, metrics-server) and kubelet overhead. With 3 nodes, there is not enough schedulable capacity — use 4+ m5.large nodes or 3+ m5.xlarge (4 vCPU) nodes.

```bash
# Associate an OIDC provider (required for IRSA and EBS CSI driver)
eksctl utils associate-iam-oidc-provider \
  --region us-east-2 \
  --cluster antfly-cluster \
  --approve

# Create the IAM role for the EBS CSI driver
eksctl create iamserviceaccount \
  --name ebs-csi-controller-sa \
  --namespace kube-system \
  --cluster antfly-cluster \
  --role-name AmazonEKS_EBS_CSI_DriverRole \
  --role-only \
  --attach-policy-arn arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy \
  --approve

# Install the EBS CSI driver addon
eksctl create addon \
  --name aws-ebs-csi-driver \
  --cluster antfly-cluster \
  --service-account-role-arn arn:aws:iam::<account-id>:role/AmazonEKS_EBS_CSI_DriverRole \
  --force

# Verify EBS CSI driver
kubectl get csidriver ebs.csi.aws.com
```

### Mixed Node Group (On-Demand + Spot)

For cost optimization with Spot Instances:

```bash
# Create on-demand node group for metadata nodes
eksctl create nodegroup \
  --cluster antfly-cluster \
  --name on-demand-nodes \
  --node-type m5.large \
  --nodes 3 \
  --managed

# Create Spot node group for data nodes
eksctl create nodegroup \
  --cluster antfly-cluster \
  --name spot-nodes \
  --node-type m5.large \
  --nodes 3 \
  --nodes-min 3 \
  --nodes-max 10 \
  --spot \
  --instance-types m5.large,m5.xlarge,m6i.large,m6i.xlarge \
  --managed
```

## Deploying the Operator

```bash
# Deploy the Antfly operator
kubectl apply -f https://antfly.io/antfly-operator-install.yaml

# Verify operator is running
kubectl get pods -n antfly-operator-namespace
```

## EKS Configuration

Add the `eks` section to your AntflyCluster spec:

```yaml
spec:
  eks:
    # Enable EKS optimizations
    enabled: true

    # Use Spot Instances for data nodes (up to 90% savings)
    useSpotInstances: true

    # Prefer specific instance types
    instanceTypes:
      - "m5.large"
      - "m5.xlarge"
      - "m6i.large"

    # IRSA for AWS API access (S3 backups, etc.)
    irsaRoleARN: "arn:aws:iam::123456789012:role/antfly-backup-role"

    # EBS volume configuration
    ebsVolumeType: "gp3"       # gp3, gp2, io1, io2, st1, sc1
    ebsEncrypted: true
    ebsKmsKeyId: ""            # Optional: use specific KMS key
    ebsIOPs: null              # For io1/io2 only
    ebsThroughput: null        # For gp3 only (125-1000 MiB/s)

    # Pod Disruption Budget (recommended)
    podDisruptionBudget:
      enabled: true
      maxUnavailable: 1
```

## Spot Instance Configuration

**Important Notes:**
- **Metadata nodes**: Should NOT use Spot Instances (Raft consensus requires stability)
- **Data nodes**: Can safely use Spot Instances with 3+ replicas
- AWS provides 2-minute warning before Spot termination
- The operator sets `terminationGracePeriodSeconds: 25` automatically

```yaml
spec:
  eks:
    enabled: true
    useSpotInstances: true  # Applied to data nodes only

  metadataNodes:
    replicas: 3  # Run on On-Demand instances

  dataNodes:
    replicas: 3  # Minimum 3 for Spot safety (data is replicated)
```

When `useSpotInstances: true`, the operator automatically:
1. Adds node selector: `eks.amazonaws.com/capacityType: SPOT`
2. Adds toleration for Spot taint
3. Sets termination grace period to 25 seconds

## Instance Type Affinity

Specify preferred EC2 instance types for your workload:

```yaml
spec:
  eks:
    enabled: true
    instanceTypes:
      - "m5.large"      # Primary preference
      - "m5.xlarge"     # Fallback
      - "m6i.large"     # Fallback
      - "m6i.xlarge"    # Fallback
```

The operator uses **preferred scheduling** (soft affinity) so pods can still schedule on other instance types if preferred ones aren't available.

## EBS Volume Configuration

| Volume Type | Use Case | Characteristics |
|-------------|----------|-----------------|
| `gp3` | Default, general-purpose | Best price/performance, 3000 IOPS baseline |
| `gp2` | Legacy general-purpose | Burstable IOPS based on size |
| `io1` | High-performance | Provisioned IOPS (up to 64,000) |
| `io2` | High-performance, durable | Provisioned IOPS with higher durability |
| `st1` | Throughput-optimized | Low-cost HDD for sequential workloads |
| `sc1` | Cold storage | Lowest cost HDD for infrequent access |

**Recommended:** Use `gp3` for most workloads:
- 3,000 baseline IOPS (can provision up to 16,000)
- 125 MiB/s baseline throughput (can provision up to 1,000 MiB/s)
- Better price/performance than gp2

```yaml
spec:
  eks:
    enabled: true
    ebsVolumeType: "gp3"
    ebsEncrypted: true
    # Optional: increase throughput for write-heavy workloads
    ebsThroughput: 250  # MiB/s (125-1000)
```

### Create EBS Storage Class

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: gp3
provisioner: ebs.csi.aws.com
parameters:
  type: gp3
  encrypted: "true"
  # Optional: specify KMS key
  # kmsKeyId: "arn:aws:kms:us-east-2:123456789012:key/..."
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
```

## IRSA (IAM Roles for Service Accounts)

IRSA allows pods to securely access AWS services without static credentials.

### 1. Create IAM Role

```bash
# Get OIDC provider URL
OIDC_PROVIDER=$(aws eks describe-cluster \
  --name antfly-cluster \
  --query "cluster.identity.oidc.issuer" \
  --output text | sed 's|https://||')

# Create trust policy
cat > trust-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "Federated": "arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):oidc-provider/${OIDC_PROVIDER}"
    },
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": {
        "${OIDC_PROVIDER}:sub": "system:serviceaccount:antfly-ns:antfly-sa"
      }
    }
  }]
}
EOF

# Create IAM role
aws iam create-role \
  --role-name antfly-backup-role \
  --assume-role-policy-document file://trust-policy.json

# Attach S3 policy for backups
aws iam put-role-policy \
  --role-name antfly-backup-role \
  --policy-name antfly-s3-backup \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::my-backup-bucket", "arn:aws:s3:::my-backup-bucket/*"]
    }]
  }'
```

### 2. Create ServiceAccount

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: antfly-sa
  namespace: antfly-ns
  annotations:
    eks.amazonaws.com/role-arn: "arn:aws:iam::123456789012:role/antfly-backup-role"
```

### 3. Reference in AntflyCluster

```yaml
spec:
  serviceAccountName: antfly-sa
  eks:
    enabled: true
    irsaRoleARN: "arn:aws:iam::123456789012:role/antfly-backup-role"
```

## Pod Disruption Budgets

PodDisruptionBudgets protect your cluster during EKS maintenance and node upgrades:

```yaml
spec:
  eks:
    podDisruptionBudget:
      enabled: true
      maxUnavailable: 1  # Recommended: allows rolling updates
      # OR
      # minAvailable: 2  # Alternative: ensure minimum pods always available
```

## Example Deployments

### Basic EKS Cluster

```yaml
apiVersion: antfly.io/v1
kind: AntflyCluster
metadata:
  name: eks-antfly-cluster
  namespace: default
spec:
  image: ghcr.io/antflydb/antfly:latest

  eks:
    enabled: true
    ebsVolumeType: "gp3"
    ebsEncrypted: true
    podDisruptionBudget:
      enabled: true
      maxUnavailable: 1

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

  storage:
    storageClass: "gp3"
    metadataStorage: "1Gi"
    dataStorage: "10Gi"

  config: |
    {
      "log": {"level": "info", "style": "json"},
      "enable_metrics": true
    }
```

### Cost-Optimized with Spot Instances

See `examples/eks-spot-cluster.yaml` for a complete example.

### IRSA with S3 Backups

See `examples/eks-irsa-cluster.yaml` for a complete example.

## Monitoring and Verification

### Verify Spot Instance Usage

```bash
# Check if pods are running on Spot nodes
kubectl get pods -o wide -l app.kubernetes.io/name=antfly-database

# Check node capacity type
kubectl get nodes -L eks.amazonaws.com/capacityType
```

### Check Pod Disruption Budgets

```bash
# List PDBs
kubectl get pdb

# Check PDB details
kubectl describe pdb <cluster-name>-data-pdb
kubectl describe pdb <cluster-name>-metadata-pdb
```

### Verify IRSA

```bash
# Check ServiceAccount annotation
kubectl get sa antfly-sa -o yaml | grep eks.amazonaws.com/role-arn

# Verify pod can access AWS (from inside pod)
kubectl exec -it <pod-name> -- aws sts get-caller-identity
```

## Spot Instance Interruption Handling

AWS Spot Instances can be terminated with 2-minute warning. The operator handles this by:

1. **Termination Grace Period**: 25 seconds for graceful shutdown
2. **Node Selector**: Ensures Spot pods only run on Spot nodes
3. **Tolerations**: Allows scheduling on tainted Spot nodes
4. **PDB Protection**: Limits concurrent terminations
5. **Replication**: With 3+ replicas, data remains available

### Monitoring Spot Interruptions

```bash
# Check for Spot interruption events
kubectl get events --field-selector reason=SpotInterruption

# View pod termination events
kubectl get events --field-selector reason=Evicted
```

## Cost Optimization Tips

1. **Use Spot Instances for Data Nodes**: Save up to 90% on compute costs
2. **Right-size Resources**: EKS charges for actual EC2 instances used
3. **Use gp3 Storage**: Better price/performance than gp2
4. **Enable Autoscaling**: Scale down during low-traffic periods
5. **Use Reserved Instances**: For metadata nodes that need stability
6. **Choose Appropriate Instance Types**: Match instance type to workload

## Troubleshooting

### EBS CSI Driver Issues

```bash
# Verify CSI driver is installed
kubectl get csidriver ebs.csi.aws.com

# Check CSI driver pods
kubectl get pods -n kube-system -l app.kubernetes.io/name=aws-ebs-csi-driver

# Check PVC binding
kubectl get pvc -l app.kubernetes.io/name=antfly-database
kubectl describe pvc <pvc-name>
```

### IRSA Issues

```bash
# Verify OIDC provider
aws eks describe-cluster --name antfly-cluster --query "cluster.identity.oidc"

# Check IAM role trust policy
aws iam get-role --role-name antfly-backup-role --query "Role.AssumeRolePolicyDocument"

# Test from pod
kubectl exec -it <pod-name> -- aws sts get-caller-identity
```

### Spot Instance Scheduling Issues

```bash
# Check node availability
kubectl get nodes -L eks.amazonaws.com/capacityType

# Check pending pods
kubectl get pods --field-selector=status.phase=Pending

# Check pod events
kubectl describe pod <pending-pod-name>
```

## Multi-AZ Storage Best Practices

### Automatic Zone Spread

New AntflyCluster deployments automatically get a soft zone topology spread constraint applied to both metadata and data StatefulSets. This distributes pods across availability zones when possible.

For explicit control, specify your own `topologySpreadConstraints` in the CRD — these take precedence over the default.

### StorageClass Configuration

EBS volumes are AZ-bound. Your StorageClass must use `volumeBindingMode: WaitForFirstConsumer` to ensure volumes are provisioned in the same zone as the pod.

**EKS 1.30+**: No default StorageClass exists. You must create one:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: gp3
provisioner: ebs.csi.aws.com
parameters:
  type: gp3
  encrypted: "true"
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
```

**EKS < 1.30**: The default `gp2` StorageClass uses `WaitForFirstConsumer` and works correctly.

### PVC Retention Policy

To avoid stale PVCs causing AZ mismatch after cluster recreation, configure automatic cleanup:

```yaml
spec:
  storage:
    pvcRetentionPolicy:
      whenDeleted: Delete   # Clean up PVCs when cluster is deleted
      whenScaled: Retain    # Keep PVCs when scaling down (recommended)
```

### Karpenter for Multi-AZ

For multi-AZ EKS deployments, [Karpenter](https://karpenter.sh/) is recommended over cluster-autoscaler. Karpenter can be configured with explicit AZ topology requirements, avoiding the ASG-from-zero AZ mismatch that occurs when cluster-autoscaler scales a node group from zero and places nodes in an AZ without existing PVCs.

See the [Pod Scheduling](../operations/pod-scheduling.md) guide for a Karpenter NodePool example.

## Best Practices

1. **Always Enable PodDisruptionBudgets**: Protects against excessive disruption
2. **Don't Use Spot for Metadata Nodes**: Raft consensus requires stability
3. **Maintain 3+ Data Replicas with Spot**: Ensures availability during interruptions
4. **Use IRSA over Static Credentials**: More secure and easier to rotate
5. **Encrypt EBS Volumes**: Enable `ebsEncrypted: true`
6. **Use gp3 Volumes**: Best price/performance ratio
7. **Set Resource Limits**: Prevents pods from consuming excessive resources
8. **Monitor Spot Interruption Rates**: High rates may indicate capacity issues
9. **Verify StorageClass on EKS 1.30+**: No default StorageClass exists — create a gp3 class
10. **Use Karpenter for Multi-AZ**: Avoids ASG-from-zero AZ mismatch

## Comparison with GKE

| Feature | AWS EKS | GKE Autopilot |
|---------|---------|---------------|
| Spot/Preemptible | Spot Instances | Spot Pods |
| Cost Savings | Up to 90% | Up to 71% |
| Interruption Warning | 2 minutes | 25 seconds |
| IAM Integration | IRSA | Workload Identity |
| Storage | EBS CSI | PD CSI |
| Compute Classes | Instance types | Autopilot classes |
| Node Management | Self-managed or Managed | Fully managed |

## Additional Resources

- [EKS Documentation](https://docs.aws.amazon.com/eks/latest/userguide/)
- [EBS CSI Driver](https://docs.aws.amazon.com/eks/latest/userguide/ebs-csi.html)
- [IRSA Documentation](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html)
- [Spot Instance Best Practices](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-best-practices.html)
