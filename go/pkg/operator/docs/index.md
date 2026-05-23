# Antfly Operator Documentation

Welcome to the Antfly Operator documentation. The Antfly Operator is a Kubernetes operator for deploying and managing Antfly database clusters with built-in high availability, autoscaling, and operational simplicity.

**Container Image**: `ghcr.io/antflydb/antfly-operator:latest`

## Quick Links

| Section | Description |
|---------|-------------|
| [Installation](getting-started/installation.md) | Install the operator in your cluster |
| [Quickstart](getting-started/quickstart.md) | Deploy your first cluster in 5 minutes |
| [Concepts](getting-started/concepts.md) | Understand the architecture |

## Cloud Platform Guides

| Platform | Description |
|----------|-------------|
| [AWS EKS](cloud-platforms/aws-eks.md) | Deploy on Amazon EKS with Spot Instances |
| [GCP GKE](cloud-platforms/gcp-gke.md) | Deploy on GKE Autopilot with Spot Pods |
| [Generic Kubernetes](cloud-platforms/generic-kubernetes.md) | Deploy on any Kubernetes cluster |

## Operations

| Topic | Description |
|-------|-------------|
| [Backup & Restore](operations/backup-restore.md) | Schedule backups and restore data |
| [Autoscaling](operations/autoscaling.md) | Configure automatic scaling |
| [Monitoring](operations/monitoring.md) | Health checks and observability |
| [Pod Scheduling](operations/pod-scheduling.md) | Taints, tolerations, affinities, and workload placement |
| [Storage](operations/storage.md) | PVC retention, volume expansion, and storage lifecycle |

## Security

| Topic | Description |
|-------|-------------|
| [RBAC](security/rbac.md) | Role-based access control |
| [Secrets Management](security/secrets-management.md) | Manage credentials securely |
| [Service Mesh](security/service-mesh.md) | Istio, Linkerd integration |

## Reference

| Resource | Description |
|----------|-------------|
| [AntflyCluster API](reference/antflycluster-api.md) | Complete CRD reference |
| [AntflyBackup API](reference/antflybackup-api.md) | Backup CRD reference |
| [AntflyRestore API](reference/antflyrestore-api.md) | Restore CRD reference |
| [Examples](reference/examples.md) | Example configurations |

## Troubleshooting

See the [Troubleshooting Guide](troubleshooting.md) for common issues and solutions.

## Key Features

- **High Availability**: Raft-based consensus for metadata nodes ensures data consistency
- **Autoscaling**: Automatic scaling of data nodes based on CPU and memory metrics
- **Cloud-Native**: Native support for GKE Autopilot and AWS EKS
- **Cost Optimization**: Spot Pod/Instance support for up to 90% cost savings
- **Backup & Restore**: Scheduled backups to S3/GCS with point-in-time recovery
- **Service Mesh**: Optional Istio/Linkerd integration for mTLS
- **Observability**: Built-in health checks and Prometheus metrics

## Architecture Overview

The Antfly Operator manages two types of nodes:

```
                    ┌─────────────────────────────────────────┐
                    │           AntflyCluster                 │
                    └─────────────────────────────────────────┘
                                       │
                    ┌──────────────────┴──────────────────┐
                    ▼                                      ▼
        ┌───────────────────┐                  ┌───────────────────┐
        │  Metadata Nodes   │                  │    Data Nodes     │
        │   (StatefulSet)   │                  │   (StatefulSet)   │
        ├───────────────────┤                  ├───────────────────┤
        │ • Raft consensus  │                  │ • Data storage    │
        │ • Cluster coord.  │◄────────────────►│ • Replication     │
        │ • Public API      │                  │ • Autoscalable    │
        │ • Fixed replicas  │                  │ • Spot-compatible │
        └───────────────────┘                  └───────────────────┘
```

**Metadata Nodes**: Handle cluster coordination via Raft consensus. Fixed replica count (typically 3 or 5) for quorum stability.

**Data Nodes**: Store and replicate data. Support autoscaling and Spot Pods/Instances for cost optimization.

## Requirements

- Kubernetes 1.20+
- kubectl configured for your cluster
- Storage class with dynamic provisioning
- (Optional) metrics-server for autoscaling
- (Optional) Service mesh for mTLS

## Getting Help

- **GitHub Issues**: [antflydb/antfly/issues](https://github.com/antflydb/antfly/issues)
- **Documentation**: You're reading it!

## Contributing

For development setup and contribution guidelines, see [DEVELOPMENT.md](DEVELOPMENT.md).
