# PVC Lifecycle, AZ Topology, and Storage Resilience for the Antfly Operator

## Context

A customer deployed Antfly on EKS with ARM nodes (m8g.large) and hit three interrelated issues:

1. **AZ topology mismatch**: ASG scaled from zero and created nodes in AZs that didn't have existing PVCs. Since EBS is AZ-bound, pods went Pending forever with "volume node affinity conflict".
2. **PVC retention confusion**: After deleting the AntflyCluster, PVCs were retained (Kubernetes default). Recreating a cluster with the same name bound to stale PVCs in wrong AZs.
3. **No operator feedback**: The operator didn't surface any conditions or events about the PVC/AZ mismatch, making it hard to diagnose.

These affect any topology-constrained storage provider (EBS, GCE PD, Azure Disk).

## Changes

### 0. Add Instance Label (Prerequisite)

The current pod labels (`app.kubernetes.io/name: antfly-database`, `app.kubernetes.io/component: metadata|data`) lack an instance identifier. This means topology spread constraints spread pods across ALL AntflyCluster instances in a namespace as one group, health detection can't use label selectors, and PDBs for one cluster match another cluster's pods.

**`pkg/operator/controllers/antflycluster_controller.go`**:
- Add `app.kubernetes.io/instance: {cluster.Name}` and `app.kubernetes.io/managed-by: antfly-operator` to:
  - Pod template labels (mutable — triggers rolling restart)
  - Service selectors (mutable)
  - PDB selectors (mutable — set in `CreateOrUpdate` callback at line 1700)
  - Topology spread constraint's `labelSelector` (in pod template, mutable)
- Do **NOT** add to `spec.selector.matchLabels` on StatefulSets — this field is **immutable** after creation and would require StatefulSet recreation. The existing selector (`app.kubernetes.io/name` + `app.kubernetes.io/component`) is sufficient to match pods.

**Upgrade impact**: Changing pod template labels triggers a rolling restart. Since `ParallelPodManagement` is used, all pods restart simultaneously. This is the same restart that the topology spread change (section 3) would cause, so combining them into a single change is the right approach. The instance label fix is a prerequisite for sections 3 and 4 to work correctly.

### 1. PVC Retention Policy (CRD + Controller)

Expose Kubernetes `PersistentVolumeClaimRetentionPolicy` via the CRD. This feature was beta (enabled by default) in K8s 1.27 and GA in K8s 1.32. On clusters running < 1.27, the StatefulSet retention policy will be silently ignored — the finalizer (section 2) provides a fallback for those clusters.

**`pkg/operator/api/v1/antflycluster_types.go`**:
- Add `PVCRetentionPolicy` struct with `WhenDeleted` and `WhenScaled` string fields (enum: `Retain`, `Delete`)
- Add `PVCRetentionPolicy *PVCRetentionPolicy` field to `StorageSpec`
- Add `FinalizerPVCCleanup = "antfly.io/pvc-cleanup"` constant

**`pkg/operator/controllers/antflycluster_controller.go`**:
- Add `buildPVCRetentionPolicy()` helper mapping CRD strings to `appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy`
- Set `statefulSet.Spec.PersistentVolumeClaimRetentionPolicy` in both `reconcileMetadataStatefulSet` and `reconcileDataStatefulSet` (inside the `CreateOrUpdate` callback, which is mutable)

Default is `Retain`/`Retain` — no behavior change for existing clusters.

**Safety checks** in `validatePVCRetentionPolicy()` (webhook):
1. Reject `WhenScaled: Delete` when `dataNodes.autoScaling.enabled: true` — the autoscaler could scale down and permanently destroy PVCs
2. Reject `WhenScaled: Delete` when `dataNodes.replicas < 2` or `metadataNodes.replicas < 2` — scaling to 0 or 1 with `Delete` can destroy all PVCs, leaving no Raft leader to resync from (total data loss)

**Controller-side warning** (in `reconcileDataStatefulSet()`, not the webhook — webhooks don't have event recorders):
3. When scaling down with `WhenScaled: Delete`, emit a Warning event on the AntflyCluster noting the Raft resync cost when scaling back up

**Raft implications of `WhenScaled: Delete`**: When a data node's PVC is deleted on scale-down and the node later rejoins (scale-up reuses the same ordinal), the node starts with empty storage. Antfly's Raft layer handles this — `startRaft()` detects an empty log directory, calls `RestartNode()` with `join=true`, and waits for the leader to send snapshots for every shard the node hosts. The happy path works, but the cost is a **full data resync via snapshot transfer for every shard** on that node. This is expensive (network + disk I/O on the leader) and temporarily reduces cluster fault tolerance while the resync is in progress. The documentation (section 7) should include a warning about this cost for manual scaling scenarios even when the webhook allows it.

**Pre-existing gap (fixed in section 9)**: The operator's autoscaler previously only adjusted StatefulSet replicas without handling Raft membership removal. Section 9 adds graceful deregistration before scale-down.

### 2. Finalizer-Based Storage Cleanup

When `pvcRetentionPolicy.whenDeleted` is `Delete`, the operator adds a finalizer to actively clean up storage resources on CR deletion. This is belt-and-suspenders alongside the StatefulSet retention policy.

**`pkg/operator/controllers/antflycluster_controller.go`**:
- Insert finalizer logic at the top of `Reconcile()`, after fetching the cluster (line ~413) and before applying defaults:
  - If cluster is being deleted and has finalizer: run `cleanupStorageResources()`, remove finalizer, return
  - If `WhenDeleted == "Delete"`: ensure finalizer is present
  - If `WhenDeleted != "Delete"`: ensure finalizer is removed
- Add `cleanupStorageResources()` function with ordered deletion to avoid deadlock:
  1. Delete both StatefulSets (metadata + data)
  2. Check if pods still exist — if yes, return `requeueAfter(5s)` to retry (pods are terminating)
  3. Once pods are gone, delete PVCs matching StatefulSet naming convention (`metadata-storage-{name}-metadata-*`, `data-storage-{name}-data-*`)
  4. Remove finalizer

**Why ordered deletion is required**: Without it, there's a deadlock: CR deletion is blocked by the finalizer → finalizer tries to delete PVCs → PVCs have `kubernetes.io/pvc-protection` finalizer blocking deletion while pods use them → pods are owned by StatefulSets owned by the CR → StatefulSets won't be garbage-collected until the CR is deleted. Deleting StatefulSets first breaks this cycle.

**Note**: The finalizer is a fallback mechanism for the StatefulSet retention policy. The retention policy is beta/enabled-by-default in K8s 1.27 and GA in 1.32, but is silently ignored on clusters < 1.27. The finalizer ensures cleanup works on all K8s versions. The troubleshooting docs should explain how to manually remove the finalizer (`kubectl edit`) if PVC cleanup gets stuck.

### 3. Default Zone Topology Spread (All Clusters)

When the user has NOT specified `topologySpreadConstraints`, automatically add a soft zone spread to both StatefulSets. This applies to all clusters, not just EKS — GCE PDs and Azure Disks also have AZ topology constraints.

**`pkg/operator/controllers/antflycluster_controller.go`**:
- Add `applyDefaultZoneTopologySpread(podTemplate, component, hasUserConstraints)` function
- Adds `TopologySpreadConstraint` with `maxSkew: 1`, `topologyKey: topology.kubernetes.io/zone`, `whenUnsatisfiable: ScheduleAnyway`
- Call in both `reconcileMetadataStatefulSet` and `reconcileDataStatefulSet`, after all cloud-provider scheduling is applied
- Skip if user already provided any topology constraints (respect explicit config)
- Skip for GKE Autopilot (Autopilot manages topology internally)

Soft constraint (`ScheduleAnyway`) chosen over hard (`DoNotSchedule`) because:
- ASG/node pool capacity is dynamic and hard constraints block scheduling when zones are imbalanced
- On single-zone clusters or clusters without zone labels, the soft constraint is harmlessly ignored
- Users who need hard enforcement can specify `DoNotSchedule` explicitly

**Upgrade impact**: Use annotation-based tracking instead of creation-time detection. Add an annotation `antfly.io/default-topology-spread: "true"` on the StatefulSet:
- If user has explicit topology constraints in CRD: apply those, remove the annotation
- If no user constraints AND (new StatefulSet OR annotation already present): apply default spread, set annotation
- If no user constraints AND existing StatefulSet without annotation: skip (preserves existing clusters that predate this feature)

This is self-healing (if someone manually removes the constraint, the operator re-adds it) and supports opt-in for existing clusters by adding the annotation. The annotation approach replaces the `CreationTimestamp.IsZero()` check, which was fragile and broke the declarative reconciliation model.

### 4. PVC/AZ Health Detection (Status Condition)

Surface PVC topology issues as a status condition so users can diagnose problems via `kubectl get antflycluster`.

**`pkg/operator/api/v1/antflycluster_types.go`**:
- Add condition constants: `TypeStorageHealthy`, `ReasonPVCAZMismatch`, `ReasonStalePVCDetected`, `ReasonStorageHealthy`

**`pkg/operator/controllers/antflycluster_controller.go`**:
- Add `checkPVCTopologyHealth()` function that lists cluster pods using `app.kubernetes.io/instance: {cluster.Name}` label selector (from section 0), checks for Pending pods with "volume node affinity" in their PodScheduled condition message
- Sets `StorageHealthy` condition to `False` with `PVCAZMismatch` reason and actionable guidance
- Emits a Warning event on the AntflyCluster resource
- Call after StatefulSet reconciliation, before `updateStatus()`

### 5. PVC Expansion + Storage Immutability

Support storage size increases (a common day-2 operation) while preventing changes that can't take effect.

**`pkg/operator/controllers/antflycluster_controller.go`**:
- Add `reconcilePVCExpansion()` function called at the start of each StatefulSet reconciliation
- Lists existing PVCs for the StatefulSet, compares their requested size to the CRD spec
- If the CRD requests a larger size, patches each PVC's `spec.resources.requests.storage`
- If the patch fails (e.g., StorageClass doesn't support expansion), sets a `StorageHealthy: False` condition with `ReasonPVCExpansionFailed`

**`pkg/operator/api/v1/antflycluster_webhook.go`**:
- Add to `validateImmutability()`: reject changes to `spec.storage.storageClass`
- For `spec.storage.metadataStorage` and `spec.storage.dataStorage`: allow size **increases** but reject decreases. The webhook should look up the StorageClass and check `allowVolumeExpansion: true` before admitting a size increase — if the StorageClass doesn't support expansion, reject at admission time with a clear message. This requires a client in the webhook (use the injected `Client` from the webhook handler) and an RBAC addition (see below).
- Add `validatePVCRetentionPolicy()` for enum validation + cross-field check (reject `WhenScaled: Delete` with autoscaling enabled)
- Note: `pvcRetentionPolicy` itself IS mutable (it maps to a mutable StatefulSet field)

**RBAC addition** for StorageClass lookup in webhook:
```go
//+kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch
```

### 6. Cross-Cloud StorageClass Guidance

The troubleshooting and cloud platform docs need a StorageClass reference table, because the default StorageClass on some providers uses `Immediate` binding (which breaks zone-aware scheduling):

| Provider | Recommended StorageClass | volumeBindingMode | Notes |
|----------|--------------------------|-------------------|-------|
| **EKS < 1.30** | `gp3` (custom) or default `gp2` | `WaitForFirstConsumer` | Both gp2 and gp3 use WFFC. Must use `ebs.csi.aws.com` provisioner for gp3 |
| **EKS >= 1.30** | `gp3` (custom, **must create**) | `WaitForFirstConsumer` | **No default StorageClass exists on EKS 1.30+** — users must create one |
| **GKE Standard** | `standard-rwo` or `premium-rwo` | `WaitForFirstConsumer` | **Default `standard` uses `Immediate` — do NOT use for multi-AZ** |
| **GKE Autopilot** | `standard-rwo` (default) | `WaitForFirstConsumer` | Autopilot also handles topology internally |
| **AKS < 1.29** | `managed-csi` or `managed-csi-premium` | `WaitForFirstConsumer` | Default `managed-csi` uses WFFC. LRS disks are AZ-bound |
| **AKS >= 1.29** | `managed-csi` (default) | `WaitForFirstConsumer` | **Multi-zone clusters auto-use ZRS (zone-redundant) — AZ problem eliminated** |
| **Generic** | Must verify | Must be `WaitForFirstConsumer` | Check with `kubectl get sc <name> -o yaml` |

This table should appear in:
- `troubleshooting.md` under the new PVC/AZ section
- `pod-scheduling.md` under the zone-aware scheduling note

The EKS docs should also recommend Karpenter over cluster-autoscaler for multi-AZ deployments (Karpenter can be configured with explicit AZ topology requirements, avoiding the ASG-from-zero AZ mismatch entirely).

### 7. Documentation

**`pkg/operator/docs/troubleshooting.md`** — New sections under "Storage Issues":
- "PVC/AZ Topology Mismatch": symptoms, root cause, `StorageHealthy` condition, cross-cloud StorageClass table, solutions
- "Stale PVCs After Cluster Recreation": symptoms, root cause, solutions (pvcRetentionPolicy, manual cleanup, different name)
- "Stuck Finalizer": how to manually remove `antfly.io/pvc-cleanup` finalizer if PVC cleanup fails

**`pkg/operator/docs/operations/storage.md`** (new) — "PVC Retention Policy" reference:
- Explain `WhenDeleted` and `WhenScaled` semantics
- **Warning**: `WhenScaled: Delete` causes a full Raft snapshot resync for every shard when a node rejoins after scale-up. This is expensive (network + disk I/O on the leader, temporarily reduced fault tolerance). Only use when storage costs outweigh resync costs.
- Note that `WhenScaled: Delete` is rejected by the webhook when autoscaling is enabled

**`pkg/operator/docs/cloud-platforms/aws-eks.md`** — New section "Multi-AZ Storage Best Practices":
- Automatic zone spread behavior
- **Warning**: EKS 1.30+ has no default StorageClass — users must create a gp3 StorageClass with `ebs.csi.aws.com` provisioner
- Recommend Karpenter over cluster-autoscaler for multi-AZ (Karpenter can pin AZ topology)
- PVC retention policy option

**`pkg/operator/docs/cloud-platforms/gcp-gke.md`** — Add StorageClass warning:
- **Explicit warning**: GKE Standard default `standard` StorageClass uses `Immediate` binding — breaks multi-AZ scheduling
- Recommend `standard-rwo` or `premium-rwo` (CSI-based, `WaitForFirstConsumer`)
- Note that Autopilot defaults to `standard-rwo` (correct)

**`pkg/operator/docs/cloud-platforms/generic-kubernetes.md`** — Add cross-cloud StorageClass table and AKS guidance:
- AKS < 1.29: `managed-csi` with `WaitForFirstConsumer` (LRS, AZ-bound)
- AKS >= 1.29: `managed-csi` auto-uses ZRS for multi-zone clusters — AZ topology issue is eliminated
- General: verify `volumeBindingMode` with `kubectl get sc`

**`pkg/operator/docs/operations/pod-scheduling.md`** — Update "Zone-Aware Scheduling":
- Note that new clusters get automatic soft zone spread by default
- Explain override behavior (user constraints take precedence)
- Note that GKE Autopilot is excluded (it manages topology internally)
- Add cross-cloud StorageClass reference table

**`pkg/operator/docs/getting-started/installation.md`** — Update "Uninstalling":
- Mention `pvcRetentionPolicy.whenDeleted: Delete` as alternative to manual PVC cleanup

**`pkg/operator/examples/eks-cluster.yaml`** — Add commented `pvcRetentionPolicy` example

### 8. Tests

**`pkg/operator/controllers/antflycluster_controller_test.go`**:
- `TestInstanceLabels` (instance label present on StatefulSet pod templates, Services, PDB selectors)
- `TestBuildPVCRetentionPolicy` (nil, Retain, Delete, mixed)
- `TestApplyDefaultZoneTopologySpread` (adds constraint + annotation, skips when user-provided, skips for GKE Autopilot, re-adds if annotation present but constraint removed)
- `TestReconcile_StorageCleanup` (deletes StatefulSets first, requeues while pods exist, deletes PVCs after pods gone, removes finalizer)
- `TestCheckPVCTopologyHealth` (detects pending pods with volume affinity issues, uses instance label selector)
- `TestReconcilePVCExpansion` (patches PVCs when size increases)

**`pkg/operator/api/v1/antflycluster_webhook_test.go`**:
- Storage class immutability on update
- Storage size increase allowed when StorageClass supports expansion
- Storage size increase rejected when StorageClass does not support expansion
- Storage size decrease rejected
- PVC retention policy mutable on update
- PVC retention policy enum validation on create
- `WhenScaled: Delete` rejected when autoscaling enabled

### 9. Graceful Data Node Deregistration Before Scale-Down

The operator's autoscaler and manual scaling currently reduce StatefulSet replicas without removing nodes from Raft voter groups. This leaves phantom voters in Raft configurations and causes instability when nodes rejoin. Fix this by calling the Antfly deregistration API before reducing replicas.

**Background**:
- Store ID is deterministic: `store_id = pod_ordinal + 1` (set in the data StatefulSet entrypoint command at `antflycluster_controller.go:1151-1158`)
- Deregistration API: `DELETE /_internal/v1/store/{store_id}` on the metadata service (`src/metadata/runner.go:315`)
- This calls `TombstoneStore()` which marks the store as `Terminating` and triggers the metadata reconciler to remove it from all Raft voter groups (`src/tablemgr/table.go:587-603`)
- Completion: the metadata reconciler removes the store from all shard Raft groups (sync peer removal), then deletes the tombstone. Timeline: 1-30s depending on shard count.

**`pkg/operator/controllers/antflycluster_controller.go`**:
- Add `deregisterDataNodes(cluster, currentReplicas, desiredReplicas)` function:
  1. Calculate ordinals being removed: `desiredReplicas` to `currentReplicas - 1` (highest ordinals first)
  2. For each ordinal, compute `storeID = ordinal + 1`
  3. Call `DELETE http://{cluster.Name}-metadata.{namespace}.svc:12377/_internal/v1/store/{storeID}`
  4. If the call fails with connection refused (metadata not ready), requeue with backoff
  5. If the call succeeds, proceed — the metadata reconciler handles the rest asynchronously
- Call `deregisterDataNodes()` in `Reconcile()` between the autoscaler evaluation and `reconcileDataStatefulSet()`. By this point, `workingCluster.Spec.DataNodes.Replicas` already reflects either the manual value (from the CRD spec) or the autoscaler's calculated value (written at line 470). This single call site handles both scaling paths.
- Compare `workingCluster.Spec.DataNodes.Replicas` (desired) against the existing StatefulSet's current replicas to detect scale-down

**No need to wait for full tombstone cleanup**: The deregistration API call is sufficient — it marks the store as `Terminating` (preventing routing) and triggers Raft peer removal. The metadata reconciler handles cleanup asynchronously. The StatefulSet pod deletion can proceed because:
- The store is already marked `Terminating` (no new requests routed to it)
- Raft peer removal is in progress (reconciler is working on it)
- If the pod is deleted before peer removal completes, the reconciler still removes the phantom voter in the next cycle

**Metadata service discovery**: The operator already creates the metadata headless service. The address is `{cluster.Name}-metadata.{namespace}.svc:{metadataAPIPort}`. The metadata API port (12377) is already defined in the controller's constants.

**HTTP client**: Add a simple HTTP client to the reconciler struct (reuse `http.DefaultClient` or inject via controller setup). No authentication needed — the `/_internal/v1/` endpoints are cluster-internal.

**Known limitation: rapid scale-down/scale-up race**: If a user scales down (triggers tombstone), then immediately scales back up before the tombstone is cleaned up (1-30s), the returning stores will conflict with the metadata reconciler that's actively removing them from Raft groups. The autoscaler's 300s cooldown prevents this in the automated case. For manual scaling, the operator should check for existing tombstones on the target ordinals before allowing scale-up and requeue if tombstones are still being cleaned up. This can be implemented as a pre-check in `deregisterDataNodes()` (or a separate `checkTombstones()` step): query the metadata service for tombstoned stores and requeue if any overlap with the ordinals being scaled up.

**Tests**:
- `TestDeregisterDataNodes` (calls deregistration API for correct store IDs, handles connection errors with requeue)
- `TestReconcileDataStatefulSet_ScaleDown` (deregistration called before replica reduction)
- Integration: mock HTTP server simulating metadata deregistration endpoint

## Reconciliation Order (within `Reconcile()`)

```
1.  Fetch cluster
2.  Handle deletion (finalizer cleanup → cleanupStorageResources())  ← NEW
3.  Ensure finalizer if needed                                       ← NEW
4.  DeepCopy + applyDefaults
5.  Validate
6.  checkEnvFromSecrets
7.  reconcileConfigMap
8.  reconcileServices
9.  reconcileMetadataStatefulSet (+ instance label, topology spread) ← MODIFIED
10. reconcilePVCExpansion (metadata)                                 ← NEW
11. EvaluateScaling (updates workingCluster.Spec.DataNodes.Replicas) ← EXISTING (unchanged position)
12. deregisterDataNodes (if scaling down)                            ← NEW
13. reconcileDataStatefulSet (+ instance label, topology spread)     ← MODIFIED
14. reconcilePVCExpansion (data)                                     ← NEW
15. checkPVCTopologyHealth                                           ← NEW
16. updateStatus
```

Ordering notes:
- The autoscaler (step 11) runs between metadata and data StatefulSet reconciliation, matching the existing code (lines 463-474). It updates `workingCluster.Spec.DataNodes.Replicas` in memory, which steps 12 and 13 then use.
- Deregistration (step 12) is a **single call site** that handles both manual and autoscaler scaling. By this point, `workingCluster.Spec.DataNodes.Replicas` reflects the final desired count from either source. Deregistration compares this against the existing StatefulSet's current replicas.
- PVC expansion (steps 10, 14) must come after StatefulSet reconciliation (steps 9, 13) because the StatefulSet creates PVCs in the first place.
- Note: newly scaled-up pods get PVCs sized from the immutable VolumeClaimTemplate (old size). `reconcilePVCExpansion()` patches them on the next reconcile cycle. For CSI drivers supporting online expansion, this is seamless.

## Implementation Order

1. Instance label addition (section 0) — prerequisite for topology spread and health detection
2. CRD types + `make manifests generate` (everything depends on this)
3. Controller: PVC retention policy mapping + finalizer logic + `cleanupStorageResources()`
4. Controller: Graceful data node deregistration (section 9) — HTTP client + deregister before scale-down
5. Controller: Default zone topology spread with annotation tracking
6. Controller: PVC expansion + topology health check
7. Webhook: Storage immutability + StorageClass expansion check + retention policy validation + RBAC
8. Tests for all of the above
9. Documentation updates + example YAMLs

## Verification

```bash
# After all changes:
cd pkg/operator && make manifests generate
cd pkg/operator && make test

# Verify CRD has new fields:
grep -A 10 "pvcRetentionPolicy" manifests/crd/antfly.io_antflyclusters.yaml

# Verify tests pass with coverage:
cd pkg/operator && go test -cover ./...
```
