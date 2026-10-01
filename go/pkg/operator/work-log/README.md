# Antfly Operator Work Log

This directory tracks major features and architectural changes in the Antfly Kubernetes operator. Each document preserves design decisions and implementation context for future reference.

## Completed Features

| Feature | Document | Summary |
|---------|----------|---------|
| Service Mesh Integration with mTLS | [completed/README.md#001-we-will-be-service-mesh-integration-with-mtls](completed/README.md#001-we-will-be-service-mesh-integration-with-mtls) | Optional mutual TLS between AntflyCluster pods via service mesh integration (Istio, Linkerd, Consul Connect, etc.), with certificate lifecycle delegated to the mesh |
| GKE Autopilot Compute Class Support | [completed/README.md#003-operator-fix-gke-gke-autopilot-compute-class-support](completed/README.md#003-operator-fix-gke-gke-autopilot-compute-class-support) | StatefulSet configuration for GKE Autopilot compute classes instead of conflicting node selectors, enabling cost-optimized spot-instance deployments |

## Planned Features

| Feature | Document | Summary |
|---------|----------|---------|
| Admission Webhooks for Antfly Operators | [planned/admission-webhooks.md](planned/admission-webhooks.md) | Kubernetes admission webhook infrastructure for both operators so validation errors surface at `kubectl apply` time instead of during reconciliation |
| PVC Lifecycle, AZ Topology, and Storage Resilience | [planned/storage-resilience-and-az-topology.md](planned/storage-resilience-and-az-topology.md) | PVC retention, AZ-aware scheduling, and operator status/events for topology-constrained storage providers (EBS, GCE PD, Azure Disk) |

## Quick Links

- **Completed Features**: [completed/](completed/)
- **Planned Features**: [planned/](planned/)
- **Main Documentation**: [../README.md](../README.md)
- **Operator Design**: [../OPERATOR.md](../OPERATOR.md)
