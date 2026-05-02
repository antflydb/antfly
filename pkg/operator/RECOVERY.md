# Operator Recovery Design

This document describes how the Antfly operator should handle workloads that are
stuck in runtime failure states such as `CrashLoopBackOff`, `ImagePullBackOff`,
failed init containers, failed readiness probes, and unschedulable pods.

The core principle is:

> Diagnose continuously, reconcile desired state continuously, and do not let
> runtime failure status block a later valid spec update from repairing the
> workload.

## Responsibilities

Kubernetes owns restart mechanics. If a container crashes, kubelet restarts it
according to the pod restart policy, and the owning StatefulSet or Deployment
keeps the desired replica count.

The operator owns higher-level intent:

- Apply the desired child resources for the CR.
- Observe owned workloads and pods.
- Surface actionable status on the CR.
- Emit events for operator-visible failures.
- Avoid unsafe higher-level operations while the workload is unhealthy.
- Apply deterministic fixes when a spec/config/image/model change provides one.

The operator should not blindly delete crashlooping pods. If the image, command,
config, secret, model artifact, permissions, or storage is wrong, deleting the
pod just repeats the same failure and hides the root cause.

## Reconcile Shape

Controllers should keep this order:

1. Fetch the CR.
2. Validate the current CR spec.
3. Reconcile desired children, such as ConfigMaps, Services, StatefulSets,
   HPAs, PDBs, and owned TermitePools.
4. Observe owned workloads and pods.
5. Write status conditions from observation.
6. Requeue while not healthy.

Runtime health must not gate child reconciliation.

Do not do this:

```go
if pool.Status.Phase == TermitePoolPhaseDegraded {
    return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}
```

Do this:

```go
if err := validateSpec(pool); err != nil {
    updateValidationStatus(pool, err)
    return ctrl.Result{RequeueAfter: validationBackoff}, nil
}

reconcileChildren(pool)
findings := observePods(pool)
updateRuntimeStatus(pool, findings)
return requeueFor(findings), nil
```

Validation failures are the exception: if the new spec is invalid, the operator
should not apply children from that invalid spec. Runtime failures from the
previous spec must not prevent a new valid spec from being applied.

## Conditions

The operator should use conditions to separate spec validity, reconciliation,
and runtime health.

Recommended `TermitePool` conditions:

- `ConfigurationValid`: spec validation succeeded.
- `WorkloadReconciled`: Service, ConfigMap, StatefulSet, HPA, and PDB were
  successfully applied.
- `PodsScheduled`: pods are schedulable.
- `ImageAvailable`: main and init container images can be pulled.
- `ModelsReady`: model-puller init containers completed successfully.
- `PodsReady`: ready replicas match desired replicas.
- `Available`: the pool is serving.

Recommended `AntflyCluster` conditions:

- `ConfigurationValid`
- `WorkloadReconciled`
- `MetadataReady`
- `DataReady`
- `SwarmReady`
- `TermiteReady`
- `Available`

Every condition should set:

- `ObservedGeneration`
- `Status`
- `Reason`
- `Message`
- `LastTransitionTime`

`ObservedGeneration` should match the CR generation used to produce the
condition. This lets users distinguish a stale failure from a failure observed
after the latest deploy.

Example `TermitePool` status:

```yaml
status:
  phase: Degraded
  replicas:
    desired: 1
    total: 1
    ready: 0
  conditions:
    - type: ConfigurationValid
      status: "True"
      observedGeneration: 12
      reason: ValidationPassed
      message: Configuration is valid
    - type: WorkloadReconciled
      status: "True"
      observedGeneration: 12
      reason: ReconcileSucceeded
      message: Child resources are reconciled
    - type: ImageAvailable
      status: "True"
      observedGeneration: 12
      reason: ImagesPulled
      message: Container images are available
    - type: ModelsReady
      status: "False"
      observedGeneration: 12
      reason: ModelPullFailed
      message: 'pod termite-read-heavy-embedders-0 init container model-puller-0 failed: registry blob sha256:... returned 404'
    - type: PodsReady
      status: "False"
      observedGeneration: 12
      reason: WaitingForPods
      message: 0/1 pods are ready
    - type: Available
      status: "False"
      observedGeneration: 12
      reason: RuntimeDegraded
      message: Model pull failed
```

## Failure Classification

Add a shared pod diagnosis helper used by both `TermitePoolReconciler` and
`AntflyClusterReconciler`.

The helper should inspect:

- `pod.Status.Phase`
- `pod.Status.Conditions`
- `pod.Status.InitContainerStatuses`
- `pod.Status.ContainerStatuses`
- waiting reasons
- terminated reasons and exit codes
- readiness state
- scheduler messages

Suggested typed findings:

```go
type PodFindingType string

const (
    PodFindingUnschedulable     PodFindingType = "Unschedulable"
    PodFindingImagePullFailed   PodFindingType = "ImagePullFailed"
    PodFindingInitFailed        PodFindingType = "InitContainerFailed"
    PodFindingModelPullFailed   PodFindingType = "ModelPullFailed"
    PodFindingCrashLooping      PodFindingType = "CrashLooping"
    PodFindingProbeFailed       PodFindingType = "ProbeFailed"
    PodFindingNotReady          PodFindingType = "NotReady"
)

type PodFinding struct {
    Type      PodFindingType
    Severity  string
    Pod       string
    Container string
    Reason    string
    Message   string
}
```

The helper should produce concise messages suitable for CR status and events.
It should avoid copying unbounded container logs into status.

### Image Pull Failures

Classify `ErrImagePull` and `ImagePullBackOff` as `ImagePullFailed`.

Map this to:

- `ImageAvailable=False`
- `Available=False`
- `phase=Degraded`

If a later deploy changes `spec.image` or an init container image, the operator
must still update the StatefulSet pod template. Kubernetes will retry with the
new image, and the next observation pass should clear `ImageAvailable`.

### Model Pull Failures

TermitePool model pulls run in init containers. The operator should recognize
model-puller init containers by name, for example `model-puller-0`.

Classify:

- init container terminated with nonzero exit code
- init container waiting in `CrashLoopBackOff`
- init container waiting with a message from the registry/model puller

Map this to:

- `ModelsReady=False`
- `Available=False`
- `phase=Degraded`

If a later deploy changes `spec.models`, variants, registry config, env, or the
image, the operator must still reconcile the StatefulSet. The pod template hash
should change so Kubernetes reruns init containers. Once the model-puller init
containers complete, the next observation pass should set `ModelsReady=True`.

### Crash Loops

Classify main containers waiting in `CrashLoopBackOff` as `CrashLooping`.

Map this to:

- component-specific readiness condition, such as `PodsReady=False`,
  `TermiteReady=False`, `MetadataReady=False`, `DataReady=False`, or
  `SwarmReady=False`
- `Available=False`
- `phase=Degraded`

Crash loops should not trigger generic pod deletion. They should trigger status,
events, and requeues.

### Unschedulable Pods

Classify `PodScheduled=False` with reason `Unschedulable` as
`Unschedulable`.

Map this to:

- `PodsScheduled=False`
- `Available=False`
- `phase=Degraded`

This is important for resource requests, node selectors, topology constraints,
PVC topology issues, and GKE Autopilot admission behavior.

## Events

When a condition transitions to a failure state, emit a Kubernetes warning event
on the owning CR.

Examples:

- `ImagePullFailed`
- `ModelPullFailed`
- `CrashLooping`
- `Unschedulable`
- `ProbeFailed`

Do not emit the same event on every requeue. Event emission should be tied to
condition transitions or rate-limited per finding.

## Watches And Requeues

Use both watches and periodic requeues.

For watches:

- Continue watching owned StatefulSets, Services, ConfigMaps, HPAs, and PDBs.
- Add pod watches mapped back to the owning CR using stable labels.
- Pods are owned by StatefulSets, not directly by the CR, so a custom pod watch
  mapper is usually clearer than relying on `Owns(&corev1.Pod{})`.

For requeues:

- Requeue quickly while runtime health is degraded or pending, for example
  15-30 seconds.
- Requeue more slowly while healthy, for example 2-5 minutes.
- Keep validation backoff separate from runtime-health requeue behavior.

## Safe Recovery Actions

The operator should apply recovery only when it has a deterministic fix:

- ConfigMap changed: update the ConfigMap and roll pods using the pod template
  config hash.
- Image changed: update the StatefulSet pod template image.
- Model list, variant, or strategy changed: update the pod template hash so init
  containers rerun.
- Secret or envFrom changed: update the existing envFrom hash annotation to
  trigger rollout.
- PVC expansion needed: patch PVCs when supported.
- Autoscaler target changed: update the HPA or StatefulSet replica count.

Avoid generic recovery actions:

- Do not delete every crashlooping pod on a timer.
- Do not recreate StatefulSets unless the operator has detected an immutable
  field change and the CRD explicitly allows disruptive replacement.
- Do not clear PVCs or model volumes automatically unless the CR explicitly asks
  for destructive cleanup.

## User Workflow

The intended user experience should be:

1. A bad deploy creates pods that fail.
2. The CR reports an actionable condition, for example:
   `ModelsReady=False, Reason=ModelPullFailed`.
3. The user deploys a fixed spec, such as a valid model variant or image.
4. The operator reconciles the new child resources even though the old pod is
   degraded.
5. Kubernetes rolls/retries pods from the new pod template.
6. The operator observes successful image/model/container readiness.
7. Conditions transition back to healthy, and `phase` returns to `Running`.

## Implementation Plan

1. Add shared pod diagnosis helpers under `pkg/operator/controllers/internal`
   or another controller-local internal package.
2. Add TermitePool conditions for image, model, pod readiness, workload
   reconciliation, and availability.
3. Update `TermitePoolReconciler.updateStatus` to call the diagnosis helper and
   map findings to conditions.
4. Add pod watches for TermitePool pods using labels such as
   `antfly.io/pool=<pool-name>`.
5. Add equivalent diagnosis mapping for AntflyCluster metadata, data, and swarm
   pods using `app.kubernetes.io/instance` and
   `app.kubernetes.io/component`.
6. Add tests for:
   - init model pull failure
   - image pull failure
   - main container crashloop
   - unschedulable pod
   - fixed spec still updates StatefulSet while current pods are degraded
   - condition clears after pods become ready

The critical test is the repair path: a degraded runtime status must not block a
new valid spec from updating child resources.
