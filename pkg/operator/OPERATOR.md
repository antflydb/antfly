# Antfly Operator Design

This document describes the public design contract for the Antfly Kubernetes
operator. It focuses on Antfly database clusters and the custom resources that
users or external control planes should manage.

## Ownership Boundary

The operator is the authority for Kubernetes realization of an Antfly cluster:

- StatefulSets
- Services
- PersistentVolumeClaims
- PodDisruptionBudgets
- autoscaling resources
- status conditions

Users and control planes should express intent through Antfly custom resources,
primarily `AntflyCluster`. Direct edits to operator-owned child resources are not
part of the public contract and may be reconciled back to the custom resource
state.

The operator owns Kubernetes safety and database lifecycle checks. Product
layers outside the operator may own plans, limits, billing, user workflows, and
allowed sizes, but they should drive changes through the public Antfly APIs.

## Public API Contract

`AntflyCluster` is the public control surface for deploying and resizing Antfly
database clusters.

Supported intent includes:

- metadata node count and resources
- data node count and resources
- swarm-mode resources
- metadata, data, and swarm storage sizes
- data-node autoscaling bounds
- cloud-specific placement and service settings
- backup and restore integration through the backup/restore CRDs

The operator reports progress through `status.observedGeneration` and status
conditions. Consumers should use those fields to determine whether a requested
operation has completed. Inspecting child StatefulSets, PVCs, or HPAs directly is
useful for debugging, but should not be the primary completion signal.

## Storage Resize

Antfly storage changes are grow-only.

Users may increase:

- `spec.storage.swarmStorage`
- `spec.storage.metadataStorage`
- `spec.storage.dataStorage`

The operator rejects disk shrink and storage class changes. Storage expansion is
tracked against both existing PVCs and StatefulSet volume claim templates so that
current pods and future replicas converge on the requested size.

Storage progress is reported through the `PVCExpansion` condition. Its reason
identifies the current state:

- `PVCExpansionPending`
- `PVCExpansionInProgress`
- `PVCExpansionComplete`
- `PVCExpansionFailed`

Condition messages should identify the affected component and PVC where
possible.

## Resource Resize

CPU and memory changes are expressed through the `AntflyCluster` spec.

Swarm mode uses:

- `spec.swarm.resources`

Clustered mode uses:

- `spec.metadataNodes.resources`
- `spec.dataNodes.resources`

The operator applies resource changes by updating the managed StatefulSet pod
templates and reporting rollout progress through status.

Rollout progress is reported through the `Rollout` condition. Its reason
identifies the current state:

- `RolloutInProgress`
- `RolloutComplete`
- `RolloutFailed`

## Data Node Scaling

Data nodes are horizontally scalable. Increasing `spec.dataNodes.replicas` or
raising autoscaling bounds is safe when the requested shape passes validation.

Scale-down is more sensitive because the database must drain or rebalance data
before a StatefulSet replica can be removed. The operator handles data-node
scale-down one ordinal at a time and reports the active step in
`status.dataScaleDownStatus`.

The scale-down workflow:

1. Observe desired data replicas below current replicas.
2. Select candidate ordinals, typically highest ordinal first.
3. Mark the selected node as draining in status.
4. Ask the Antfly runtime to drain, rebalance, and remove membership.
5. Wait for runtime confirmation.
6. Reduce StatefulSet replicas.
7. Report completion or failure conditions.

The `Scaling` condition reports whether scaling can proceed safely. Its reasons
include:

- `ScalingReady`
- `DataScaleDownBlocked`
- `DataScaleDownInProgress`
- `DataScaleDownFailed`

## Metadata Node Safety

Metadata nodes participate in consensus, so metadata scaling has stricter
validation than data-node scaling.

The operator enforces:

- odd metadata replica counts
- no unsafe metadata scale-down
- production configurations with enough replicas for quorum, typically at least
  three metadata nodes

Validation errors are returned by the webhook when enabled. The reconciler still
defends the same safety invariants when webhooks are unavailable.

## Autoscaling

When data-node autoscaling is disabled, desired data replicas come from
`spec.dataNodes.replicas`.

When data-node autoscaling is enabled, the operator computes desired replicas
within the configured minimum and maximum bounds. `status.autoScalingStatus`
reports enough information for users and control planes to understand
autoscaling decisions:

- current replicas
- desired replicas
- autoscaler recommendation
- blocked scale-down reason, if any
- rollout or resize progress

Autoscaling follows the same safety rules as manual scaling. Autoscaler
scale-down uses the same one-ordinal-at-a-time data-node scale-down workflow.

## Public Status Model

Consumers should wait for:

- `status.observedGeneration` to match the resource generation they submitted
- relevant resize, rollout, storage, or scaling conditions to become complete
- failure conditions to remain absent

The operator should prefer explicit, user-actionable conditions over requiring
users to infer progress from Kubernetes child resources.

Useful public condition types include:

- `Available`
- `ConfigurationValid`
- `SecretsReady`
- `StorageHealthy`
- `PVCExpansion`
- `StorageAutoGrow`
- `Rollout`
- `Scaling`
- `MetadataReady`
- `DataReady`
- `SwarmReady`

Common public reason values include:

- `ValidationPassed`
- `ValidationFailed`
- `AllSecretsFound`
- `StorageHealthy`
- `PVCExpansionPending`
- `PVCExpansionInProgress`
- `PVCExpansionComplete`
- `PVCExpansionFailed`
- `StorageAutoGrowDisabled`
- `StorageAutoGrowReady`
- `StorageAutoGrowInProgress`
- `StorageAutoGrowMaxReached`
- `RolloutInProgress`
- `RolloutComplete`
- `RolloutFailed`
- `ScalingReady`
- `DataScaleDownBlocked`
- `DataScaleDownInProgress`
- `DataScaleDownFailed`

## Out Of Scope

The public Antfly operator design does not expose implementation details such as
internal controller package layout, release migration history, or downstream
repository cleanup tasks. Those details belong in work logs or development notes,
not in this public-facing design contract.
