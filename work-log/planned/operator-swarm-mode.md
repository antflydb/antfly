# Operator Swarm Mode

## Summary

Add explicit operator-managed swarm mode to `AntflyCluster` without breaking the
existing clustered topology.

The current operator is built around a split deployment model:

- one metadata `StatefulSet`
- one data `StatefulSet`
- metadata/data headless services
- a public API service targeting metadata
- status derived from metadata/data readiness

That model is correct for the current CRD, but it does not map cleanly to the
existing `antfly swarm` runtime, which runs a single combined node.

The correct design is:

- add an explicit topology discriminator
- introduce a dedicated `spec.swarm` block
- branch reconciliation early by mode
- keep `public-api` as the stable external endpoint
- make status, storage, and cleanup topology-aware

This should be implemented as a staged rollout, not as a partial patch to the
current `metadataNodes`/`dataNodes` model.

## Goals

- support operator-managed single-node swarm mode
- preserve backward compatibility for all existing `AntflyCluster` manifests
- keep the current clustered operator path behaviorally unchanged
- avoid ambiguous specs and hidden topology inference
- keep external API/service contracts as stable as possible

## Non-Goals

- in-place conversion between clustered and swarm topologies
- swarm autoscaling in the first pass
- reusing clustered status fields as-is for swarm
- mixing clustered and swarm fields in one active topology

## Current Constraints

Today the operator assumes a split topology throughout:

- `AntflyClusterSpec` exposes `metadataNodes` and `dataNodes`
- validation requires odd metadata replicas and non-negative data replicas
- reconciliation always creates metadata/data services and StatefulSets
- autoscaling is explicitly data-node-specific
- PVC expansion and storage-health logic iterate metadata/data components
- phase calculation derives readiness from metadata/data counts

The runtime already supports swarm mode independently:

- `go run ./cmd/antfly swarm`
- `SwarmMode=true` runtime path
- swarm-specific defaults and readiness behavior in the CLI/runtime

That means the operator should model swarm as a first-class topology rather than
trying to approximate it using reduced clustered replica counts.

## Proposed API

### New mode discriminator

Add an explicit mode field:

```go
type ClusterMode string

const (
    ClusterModeClustered ClusterMode = "Clustered"
    ClusterModeSwarm     ClusterMode = "Swarm"
)
```

### `AntflyClusterSpec`

```go
type AntflyClusterSpec struct {
    Mode ClusterMode `json:"mode,omitempty"`

    Cluster *ClusteredSpec `json:"cluster,omitempty"`
    Swarm   *SwarmSpec     `json:"swarm,omitempty"`

    Image              string           `json:"image"`
    ImagePullPolicy    string           `json:"imagePullPolicy,omitempty"`
    Config             string           `json:"config"`
    Storage            StorageSpec      `json:"storage"`
    GKE                *GKESpec         `json:"gke,omitempty"`
    EKS                *EKSSpec         `json:"eks,omitempty"`
    ServiceMesh        *ServiceMeshSpec `json:"serviceMesh,omitempty"`
    PublicAPI          *PublicAPIConfig `json:"publicAPI,omitempty"`
    ServiceAccountName string           `json:"serviceAccountName,omitempty"`
}
```

### Clustered topology

Move the current split-node topology under a dedicated block:

```go
type ClusteredSpec struct {
    MetadataNodes MetadataNodesSpec `json:"metadataNodes"`
    DataNodes     DataNodesSpec     `json:"dataNodes"`
}
```

If we need strict backward compatibility in the current API version, the
existing top-level `metadataNodes` and `dataNodes` fields can be retained as
deprecated aliases initially and normalized internally to `ClusteredSpec`.

### Swarm topology

```go
type SwarmSpec struct {
    Replicas int32 `json:"replicas,omitempty"`
    NodeID   uint64 `json:"nodeID,omitempty"`

    MetadataAPI  APISpec `json:"metadataAPI,omitempty"`
    MetadataRaft APISpec `json:"metadataRaft,omitempty"`
    StoreAPI     APISpec `json:"storeAPI,omitempty"`
    StoreRaft    APISpec `json:"storeRaft,omitempty"`
    Health       APISpec `json:"health,omitempty"`

    Termite *SwarmTermiteSpec `json:"termite,omitempty"`

    PodTemplate SwarmPodTemplateSpec `json:"podTemplate,omitempty"`
}

type SwarmTermiteSpec struct {
    Enabled bool   `json:"enabled,omitempty"`
    APIURL  string `json:"apiURL,omitempty"`
}

type SwarmPodTemplateSpec struct {
    Resources                 ResourceSpec                    `json:"resources,omitempty"`
    EnvFrom                   []corev1.EnvFromSource         `json:"envFrom,omitempty"`
    Tolerations               []corev1.Toleration            `json:"tolerations,omitempty"`
    NodeSelector              map[string]string              `json:"nodeSelector,omitempty"`
    Affinity                  *corev1.Affinity               `json:"affinity,omitempty"`
    TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
}
```

### Storage

The current storage model is split between metadata and data. Swarm should not
try to overload that.

Add a dedicated storage field:

```go
type StorageSpec struct {
    StorageClass    string `json:"storageClass,omitempty"`
    MetadataStorage string `json:"metadataStorage,omitempty"`
    DataStorage     string `json:"dataStorage,omitempty"`
    SwarmStorage    string `json:"swarmStorage,omitempty"`

    PVCRetentionPolicy *PVCRetentionPolicy `json:"pvcRetentionPolicy,omitempty"`
}
```

## Defaults

Default:

- `mode = Clustered`

Clustered mode keeps current defaults unchanged.

Swarm mode defaults should match the swarm CLI/runtime:

- `replicas = 1`
- `nodeID = 1`
- `metadataAPI.port = 8080`
- `metadataRaft.port = 9017`
- `storeAPI.port = 12380`
- `storeRaft.port = 9021`
- `health.port = 4200`
- `termite.enabled = true`
- `termite.apiURL = http://0.0.0.0:11433`

Swarm config generation should enforce:

- `swarm_mode = true`
- `replication_factor = 1`
- `default_shards_per_table = 1`
- `disable_shard_alloc = true`
- a single-entry `metadata.orchestration_urls`

Those keys should be operator-owned in swarm mode even if `spec.config` is
still present as a user override channel for unrelated settings.

## Validation

### Shared

- `mode` must be one of `Clustered` or `Swarm`
- `mode` is immutable after create
- cloud-provider configuration remains orthogonal to topology mode
- resource quantity validation remains shared

### Clustered mode

Keep today’s rules:

- metadata replicas must be odd and `>= 1`
- data replicas must be `>= 0`
- current autoscaling validation
- current PVC retention validation
- current GKE/EKS scheduling validation

### Swarm mode

- require `spec.swarm`
- reject active clustered topology fields
- require `swarm.replicas >= 1`
- require `swarm.nodeID >= 1`
- validate swarm ports are non-zero and non-colliding
- validate `termite.apiURL` when termite is enabled
- reject clustered autoscaling semantics in swarm mode
- validate `storage.swarmStorage` and reject reliance on metadata/data storage

Critical rule:

- do not infer mode from replica counts, ports, or missing fields

## Status Model

The current status shape is cluster-specific and should not be reused as the
sole source of truth for swarm.

Proposed extension:

```go
type AntflyClusterStatus struct {
    Phase string `json:"phase,omitempty"`
    Mode  string `json:"mode,omitempty"`

    ObservedGeneration int64 `json:"observedGeneration,omitempty"`
    Conditions         []metav1.Condition `json:"conditions,omitempty"`

    ReadyReplicas      int32 `json:"readyReplicas,omitempty"`
    MetadataNodesReady int32 `json:"metadataNodesReady,omitempty"`
    DataNodesReady     int32 `json:"dataNodesReady,omitempty"`
    SwarmNodesReady    int32 `json:"swarmNodesReady,omitempty"`

    SwarmStatus       *SwarmStatus       `json:"swarmStatus,omitempty"`
    AutoScalingStatus *AutoScalingStatus `json:"autoScalingStatus,omitempty"`
    ServiceMeshStatus *ServiceMeshStatus `json:"serviceMeshStatus,omitempty"`
}

type SwarmStatus struct {
    Ready              bool         `json:"ready,omitempty"`
    MetadataReady      bool         `json:"metadataReady,omitempty"`
    StoreReady         bool         `json:"storeReady,omitempty"`
    TermiteReady       bool         `json:"termiteReady,omitempty"`
    NodeID             uint64       `json:"nodeID,omitempty"`
    PodName            string       `json:"podName,omitempty"`
    PodIP              string       `json:"podIP,omitempty"`
    ObservedConfigHash string       `json:"observedConfigHash,omitempty"`
    LastTransitionTime *metav1.Time `json:"lastTransitionTime,omitempty"`
}
```

Mode-specific readiness:

- `Clustered`
  - derive phase from metadata/data readiness
- `Swarm`
  - derive phase from the single swarm workload and runtime readiness

Do not make swarm pretend to be a metadata/data cluster.

## Controller Design

### High-level flow

Keep one shared preflight block:

- fetch object
- deletion/finalizer handling
- defaulting
- validation
- `envFrom` secret checks
- configmap reconciliation

Then branch early:

```go
switch cluster.Spec.Mode {
case antflyv1.ClusterModeSwarm:
    return r.reconcileSwarmCluster(ctx, workingCluster, efCache)
default:
    return r.reconcileManagedCluster(ctx, workingCluster, efCache)
}
```

### Shared helpers

These should stay shared:

- `buildPVCRetentionPolicy`
- `buildResourceRequirements`
- `envFromCache`
- `buildPodAnnotations`
- `computeEnvFromHash`
- `applySchedulingConstraints`
- condition update helpers
- shared configmap reconciliation wrapper

### Split helpers

These need mode-specific behavior:

- `generateCompleteConfig`
- `reconcileServices`
- workload reconciliation
- `updateStatus`
- `reconcilePVCExpansion`
- `checkPVCTopologyHealth`
- cleanup/finalizer resource enumeration
- backup/restore endpoint resolution helper

These remain clustered-only in MVP:

- autoscaler evaluation
- data-node deregistration

## Swarm Workload Model

Use a dedicated singleton `StatefulSet`, not a `Deployment`.

Reason:

- persistence matters
- stable identity is useful
- PVC lifecycle semantics align with the current operator model
- future controlled scaling remains possible even if not supported initially

Proposed resource names:

- `clusterName-swarm` `StatefulSet`
- `clusterName-swarm` internal service
- `clusterName-public-api` public service

In swarm mode:

- `public-api` should target the swarm pod
- do not create metadata/data headless services

The container entrypoint should use:

- `antfly swarm`

not separate metadata/store sidecars.

## Config Generation

Split the current config builder into:

- `generateClusteredConfig`
- `generateSwarmConfig`

Clustered mode continues generating metadata orchestration URLs for a split
cluster.

Swarm mode should generate a config compatible with `runSwarm`, including the
runtime-owned keys listed above.

## Backup and Restore

Recommendation for MVP:

- preserve `clusterName-public-api` as the stable cluster API endpoint
- keep backup/restore controllers using that service contract if swarm honors it

If swarm cannot safely reuse the same API endpoint contract, extract a
topology-aware endpoint helper first and branch backup/restore explicitly.

Do not assume swarm backup/restore works just because the operator compiles.

## Autoscaling

Do not support swarm autoscaling in the first pass.

Validation should reject autoscaling when `mode = Swarm`.

Rationale:

- current autoscaler is data-node-specific
- swarm scaling semantics are a different design problem

## Cleanup and Storage

The current cleanup path assumes:

- metadata PVCs
- data PVCs
- two StatefulSet families

Swarm mode needs a topology-aware cleanup list:

- one `StatefulSet`
- one PVC family

Likewise:

- PVC expansion must branch by topology
- storage-health checks must branch by topology

## Backward Compatibility

Backward compatibility strategy:

- default `mode` to `Clustered`
- keep existing manifests working unchanged
- preserve current clustered behavior exactly
- avoid any hidden implicit migration

Do not support in-place topology conversion:

- `Clustered -> Swarm`
- `Swarm -> Clustered`

That should require delete/recreate.

## Phased Rollout

### Phase 1: API and guardrails

- add `mode`
- add `swarm`
- add `swarmStorage`
- add mode-aware defaulting and validation
- make `mode` immutable

### Phase 2: controller split

- refactor current reconcile path into `reconcileManagedCluster`
- keep behavior identical for clustered mode
- introduce shared topology-neutral helper boundaries

### Phase 3: swarm workload path

- add swarm config generation
- add swarm service reconciliation
- add swarm `StatefulSet`
- add swarm status calculation

### Phase 4: topology-aware operations

- make PVC expansion topology-aware
- make storage-health topology-aware
- make cleanup/finalizer topology-aware
- explicitly disable autoscaling in swarm mode

### Phase 5: backup/restore verification

- centralize cluster API endpoint resolution
- verify whether swarm can reuse `public-api`
- branch backup/restore only if required

### Phase 6: tests and docs

- add CRD validation tests
- add controller tests for both modes
- add e2e smoke coverage for clustered and swarm
- document that swarm is single-node and not HA

## Operational Risks

- selector/resource-name collisions across modes
- false readiness if swarm uses cluster-specific status fields
- orphaned or misdeleted PVCs during finalizer cleanup
- backup/restore breakage if the external API service contract changes
- autoscaler or storage logic accidentally running on swarm resources
- ambiguous ownership of runtime keys between `spec.config` and typed swarm fields

## Recommendation

Proceed with swarm mode only behind an explicit mode-aware design.

Do not attempt to:

- infer swarm from small replica counts
- make swarm look like a reduced clustered deployment
- partially branch only `Phase`

The clean implementation is an explicit topology model with early reconcile
branching and topology-aware status/storage semantics.
