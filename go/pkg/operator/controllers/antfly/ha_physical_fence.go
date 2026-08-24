package controllers

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const haKubernetesPhysicalFenceName = "kubernetes-physical-fence"

var errHAPhysicalIsolationGracePending = errors.New("isolate former primary: local watchdog fence-latency barrier pending")

type haPhysicalIsolationGraceKey struct {
	clusterUID          string
	leaseUID            string
	leaseGeneration     uint64
	leaseTransferUnixNS int64
	processBootID       string
}

// reconcileHAFormerPrimaryIsolation is the fail-safe path for an automatic
// failover whose former primary cannot be reached through its admin endpoint.
// A Kubernetes Lease or Pod API absence alone does not stop the old writer.
// This action holds the old StatefulSet at zero, binds an authenticated
// pre-transfer watchdog proof to the exact old process, observes the exact
// Lease transfer uncached, and waits the proof-bound maximum fence latency on
// a process-local monotonic clock before releasing promotion dependencies.
func (r *AntflyClusterReconciler) reconcileHAFormerPrimaryIsolation(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	if r == nil || r.Client == nil || cluster == nil || cluster.Status.HAStatus == nil {
		return nil
	}
	for i := range cluster.Status.HAStatus.PlannedActions {
		action := &cluster.Status.HAStatus.PlannedActions[i]
		if haActionKind(action.Kind) != haActionIsolateFormerPrimary {
			continue
		}
		if err := validateHAFormerPrimaryIsolationAction(cluster, action); err != nil {
			return err
		}
		// Once an exact promotion receipt exists, the completed isolation receipt
		// is immutable historical authority. The shared Lease may expire or advance
		// while the route and durable topology are still converging. Requiring that
		// mutable Lease to remain current here would block the very reconciliation
		// that commits those later steps. Before promotion, a restarted controller
		// must still repeat its local monotonic watchdog barrier below.
		if action.AdminJobPhase == haAdminJobPhaseSucceeded && haPhysicalIsolationPromotionRecorded(cluster, action) {
			if !haPhysicalIsolationSucceededWithEvidence(cluster, *action) {
				return fmt.Errorf("isolate former primary: completed action lacks a complete matching physical-isolation receipt")
			}
			return nil
		}
		lease := &coordinationv1.Lease{}
		reader := r.haBoundaryReader()
		if err := reader.Get(ctx, types.NamespacedName{Name: haFencingLeaseName(cluster), Namespace: cluster.Namespace}, lease); err != nil {
			return fmt.Errorf("isolate former primary: read fencing Lease: %w", err)
		}
		scope, ok := haCurrentFencingLeaseScope(cluster)
		if action.PhysicalIsolationReceipt != nil {
			scope, ok = haPhysicalIsolationReceiptScope(action.PhysicalIsolationReceipt)
		}
		if !ok {
			return fmt.Errorf("isolate former primary: fencing Lease scope is unavailable")
		}
		if err := validateCurrentPhysicalIsolationLease(lease, action, scope); err != nil {
			return err
		}
		ready, reason := haLeaseFenceReady(lease, action.FenceGeneration, r.haNow())
		if !ready {
			return fmt.Errorf("isolate former primary: fencing Lease is not current: %s", reason)
		}
		if !haLeaseFenceScopeMatches(lease, scope) {
			return fmt.Errorf("isolate former primary: fencing Lease scope does not match the planned topology")
		}

		statefulSet := &appsv1.StatefulSet{}
		key := types.NamespacedName{Name: standaloneStatefulSetName(cluster), Namespace: cluster.Namespace}
		if err := reader.Get(ctx, key, statefulSet); err != nil {
			return fmt.Errorf("isolate former primary: read StatefulSet: %w", err)
		}
		owner := metav1.GetControllerOf(statefulSet)
		if owner == nil || owner.UID != cluster.UID {
			return fmt.Errorf("isolate former primary: StatefulSet %s is not controlled by AntflyCluster UID %s", statefulSet.Name, cluster.UID)
		}
		if action.AdminJobPhase == "" || action.AdminJobPhase == haAdminJobPhaseWaitingDependency {
			proofPodUID := ""
			if cluster.Status.HAStatus.PrimaryWatchdogProof != nil {
				proofPodUID = cluster.Status.HAStatus.PrimaryWatchdogProof.PodUID
			}
			initialPods, err := listStatefulSetPodsForPhysicalIsolation(ctx, reader, statefulSet, proofPodUID)
			if err != nil {
				return fmt.Errorf("isolate former primary: list initial runtime pods: %w", err)
			}
			receipt, err := newHAPhysicalIsolationIntentReceipt(cluster, action, statefulSet, initialPods, lease, scope)
			if err != nil {
				return err
			}
			action.AdminJobName = haKubernetesPhysicalFenceName
			action.AdminJobPhase = haAdminJobPhaseRunning
			action.AdminError = ""
			action.ErrorClass = ""
			action.PhysicalIsolationReceipt = receipt
			now := metav1.NewTime(r.haNow())
			action.FirstAttemptAt = &now
			// Persist the irreversible-intent barrier before scaling the old
			// writer. If the controller crashes after the Kubernetes mutation,
			// ordinary StatefulSet reconciliation must already know that it may
			// not recreate the former primary.
			if err := r.persistHAActionPlanBarrier(ctx, cluster); err != nil {
				return fmt.Errorf("isolate former primary: persist physical-fence intent: %w", err)
			}
			return errHAStatusCheckpointed
		}
		if err := validateHAPhysicalIsolationIntent(cluster, action); err != nil {
			return err
		}
		if err := validateCurrentPhysicalIsolationObjects(cluster, action, statefulSet, lease, scope); err != nil {
			return err
		}
		if action.AdminJobPhase == haAdminJobPhaseSucceeded && !haPhysicalIsolationSucceededWithEvidence(cluster, *action) {
			return fmt.Errorf("isolate former primary: succeeded action lacks a complete matching physical-isolation receipt")
		}
		if statefulSet.Spec.Replicas == nil || *statefulSet.Spec.Replicas != 0 {
			patch := client.MergeFrom(statefulSet.DeepCopy())
			zero := int32(0)
			statefulSet.Spec.Replicas = &zero
			statefulSet.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			}
			if err := r.Patch(ctx, statefulSet, patch); err != nil {
				return fmt.Errorf("isolate former primary: hold StatefulSet at zero: %w", err)
			}
			// Stop this reconciliation after the irreversible workload mutation.
			// Continuing through admin execution and a broad status write only
			// increases the conflict window before the watchdog barrier can start.
			return errHAStatusCheckpointed
		}

		proofPodUID := ""
		if action.PhysicalIsolationReceipt != nil && action.PhysicalIsolationReceipt.WatchdogProof != nil {
			proofPodUID = action.PhysicalIsolationReceipt.WatchdogProof.PodUID
		}
		pods, err := listStatefulSetPodsForPhysicalIsolation(ctx, reader, statefulSet, proofPodUID)
		if err != nil {
			return fmt.Errorf("isolate former primary: list runtime pods: %w", err)
		}
		absenceProven := true
		for j := range pods.Items {
			pod := &pods.Items[j]
			// A deletion timestamp is only intent; kubelet may still be running
			// the container throughout its grace period. Require the API snapshot
			// to show either no Pod object or a terminal process state.
			if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
				absenceProven = false
				break
			}
		}
		// A selector-only List can be defeated by a stale/mutated label. Re-read
		// every Pod name frozen in the intent receipt through the same uncached
		// reader and fold any live incarnation into the fallback proof set.
		for _, initial := range action.PhysicalIsolationReceipt.InitialOldPods {
			current := &corev1.Pod{}
			err := reader.Get(ctx, types.NamespacedName{Name: initial.Name, Namespace: cluster.Namespace}, current)
			if apierrors.IsNotFound(err) {
				continue
			}
			if err != nil {
				return fmt.Errorf("isolate former primary: re-read initial Pod %s: %w", initial.Name, err)
			}
			if current.Status.Phase == corev1.PodSucceeded || current.Status.Phase == corev1.PodFailed {
				continue
			}
			absenceProven = false
			found := false
			for j := range pods.Items {
				if pods.Items[j].Name == current.Name && pods.Items[j].UID == current.UID {
					found = true
					break
				}
			}
			if !found {
				pods.Items = append(pods.Items, *current)
			}
		}
		if statefulSet.Generation <= 0 || statefulSet.Status.ObservedGeneration < statefulSet.Generation {
			return nil
		}
		if absenceProven && (statefulSet.Status.Replicas != 0 || statefulSet.Status.CurrentReplicas != 0 || statefulSet.Status.ReadyReplicas != 0) {
			return nil
		}
		receipt := action.PhysicalIsolationReceipt
		if receipt == nil {
			return fmt.Errorf("isolate former primary: physical-isolation intent receipt disappeared")
		}
		// Kubernetes object absence is not a power/process fence: force deletion
		// can remove the Pod while an unreachable kubelet keeps the old container
		// writing. Every Lease-transfer path therefore requires a prior proof that
		// this exact old process had the fail-closed watchdog active.
		if !haPhysicalIsolationWatchdogProofStructurallyValid(*action) {
			return nil
		}
		// This wait is deliberately process-local. Persisted wall-clock timestamps
		// cannot prove that a restarted controller actually observed the transfer
		// and waited the old runtime's complete self-fence bound.
		if !r.haPhysicalIsolationWatchdogBarrierElapsed(action) {
			if action.AdminJobPhase == haAdminJobPhaseSucceeded {
				return errHAPhysicalIsolationGracePending
			}
			return nil
		}
		if !absenceProven {
			if !haPhysicalIsolationWatchdogFallbackProven(*action, pods) {
				return nil
			}
			// A Service-selector partition can make the StatefulSet controller
			// orphan its still-running Pod before the scale-to-zero update. Once
			// the exact pre-transfer process has exceeded its fail-closed watchdog
			// bound, remove that orphan with UID and resource-version
			// preconditions. The zero grace period only cleans up the Kubernetes
			// object; promotion safety continues to come from the process-bound
			// watchdog proof because a partitioned kubelet may keep a deleted
			// container alive.
			deleted, err := r.deletePhysicallyIsolatedOrphan(ctx, action, pods)
			if err != nil {
				return err
			}
			if deleted {
				return errHAStatusCheckpointed
			}
		}
		if receipt.ObservedAt == nil {
			if !absenceProven && !haPhysicalIsolationWatchdogFallbackProven(*action, pods) {
				return nil
			}
			// Checkpoint the first post-watchdog-bound topology observation and
			// require another reconciliation before freezing the boundary. That next
			// pass refreshes candidate applied/safe LSNs after write authority has
			// failed closed; Pod absence itself does not assert process exit.
			now := metav1.NewTime(r.haNow())
			receipt.IsolatedStatefulSetGeneration = statefulSet.Generation
			receipt.IsolatedStatefulSetObservedGeneration = statefulSet.Status.ObservedGeneration
			receipt.IsolatedStatefulSetResourceVersion = statefulSet.ResourceVersion
			receipt.ObservedLeaseResourceVersion = lease.ResourceVersion
			receipt.AbsenceProven = absenceProven
			if absenceProven {
				receipt.AbsencePodListResourceVersion = pods.ResourceVersion
			} else {
				receipt.AbsencePodListResourceVersion = ""
			}
			receipt.ObservedAt = &now
			action.LastAttemptAt = &now
			if err := r.persistHAActionPlanBarrier(ctx, cluster); err != nil {
				return fmt.Errorf("isolate former primary: persist physical-isolation observation: %w", err)
			}
			return errHAStatusCheckpointed
		}
		if err := validateRecordedPhysicalIsolationObservation(action, statefulSet, lease, pods, absenceProven); err != nil {
			return err
		}
		if !absenceProven && !haPhysicalIsolationWatchdogFallbackProven(*action, pods) {
			return nil
		}
		if action.AdminJobPhase == haAdminJobPhaseSucceeded {
			return nil
		}

		boundary, ok := haIsolatedPromotionBoundary(cluster.Status.HAStatus, *action)
		if !ok {
			return nil
		}
		action.TargetLSN = boundary
		action.ObservedLSN = boundary
		receipt.IsolatedStatefulSetGeneration = statefulSet.Generation
		receipt.IsolatedStatefulSetObservedGeneration = statefulSet.Status.ObservedGeneration
		receipt.IsolatedStatefulSetResourceVersion = statefulSet.ResourceVersion
		receipt.ObservedLeaseResourceVersion = lease.ResourceVersion
		receipt.AbsenceProven = absenceProven
		if absenceProven {
			receipt.AbsencePodListResourceVersion = pods.ResourceVersion
		} else {
			receipt.AbsencePodListResourceVersion = ""
		}
		receipt.FrozenBoundaryLSN = boundary
		action.AdminJobPhase = haAdminJobPhaseSucceeded
		action.AdminError = ""
		action.ErrorClass = ""
		now := metav1.NewTime(r.haNow())
		action.CompletedAt = &now
		receipt.CompletedAt = now.DeepCopy()
		if !haPhysicalIsolationSucceededWithEvidence(cluster, *action) {
			return fmt.Errorf("isolate former primary: refusing to publish an incomplete physical-isolation receipt")
		}
		// The final success receipt is itself an irreversible dependency barrier.
		// Persist it before any dependent admin action can run in this reconcile.
		if err := r.persistHAActionPlanBarrier(ctx, cluster); err != nil {
			return fmt.Errorf("isolate former primary: persist final isolation receipt: %w", err)
		}
		return errHAStatusCheckpointed
	}
	return nil
}

func (r *AntflyClusterReconciler) deletePhysicallyIsolatedOrphan(ctx context.Context, action *antflyv1.HAPlannedActionStatus, pods *corev1.PodList) (bool, error) {
	if r == nil || r.Client == nil || action == nil || action.PhysicalIsolationReceipt == nil ||
		action.PhysicalIsolationReceipt.WatchdogProof == nil || pods == nil {
		return false, nil
	}
	proof := action.PhysicalIsolationReceipt.WatchdogProof
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Name != proof.PodName || string(pod.UID) != proof.PodUID ||
			pod.UID == "" || strings.TrimSpace(pod.ResourceVersion) == "" ||
			pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		uid := pod.UID
		resourceVersion := pod.ResourceVersion
		err := r.Delete(ctx, pod,
			client.GracePeriodSeconds(0),
			client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion},
		)
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			// Re-read through the uncached boundary reader before deciding
			// whether this exact process disappeared or merely changed version.
			return true, nil
		}
		if err != nil {
			return false, fmt.Errorf("isolate former primary: delete exact orphan Pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		return true, nil
	}
	return false, nil
}

// listStatefulSetPodsForPhysicalIsolation discovers old-writer processes by
// controller ownership and by the exact runtime-attested Pod UID. StatefulSet
// ownership is not durable across a selector-label mutation: the controller
// can orphan the still-running Pod. The proof UID plus the stable ordinal Pod
// name preserves process identity across that orphaning without trusting the
// mutable Service selector.
func listStatefulSetPodsForPhysicalIsolation(ctx context.Context, reader client.Reader, statefulSet *appsv1.StatefulSet, proofPodUID string) (*corev1.PodList, error) {
	if reader == nil || statefulSet == nil || statefulSet.UID == "" || statefulSet.Namespace == "" {
		return nil, fmt.Errorf("StatefulSet namespace and UID are required")
	}
	var namespacePods corev1.PodList
	if err := reader.List(ctx, &namespacePods, client.InNamespace(statefulSet.Namespace)); err != nil {
		return nil, err
	}
	owned := &corev1.PodList{ListMeta: namespacePods.ListMeta}
	for i := range namespacePods.Items {
		pod := &namespacePods.Items[i]
		owner := metav1.GetControllerOf(pod)
		ownedByStatefulSet := owner != nil && owner.UID == statefulSet.UID
		proofBoundOrphan := string(pod.UID) == strings.TrimSpace(proofPodUID) && statefulSetOrdinalPodName(statefulSet.Name, pod.Name)
		if ownedByStatefulSet || proofBoundOrphan {
			owned.Items = append(owned.Items, *pod.DeepCopy())
		}
	}
	return owned, nil
}

func statefulSetOrdinalPodName(statefulSetName, podName string) bool {
	prefix := strings.TrimSpace(statefulSetName) + "-"
	if prefix == "-" || !strings.HasPrefix(podName, prefix) {
		return false
	}
	ordinalText := strings.TrimPrefix(podName, prefix)
	ordinal, err := strconv.Atoi(ordinalText)
	return err == nil && ordinal >= 0 && strconv.Itoa(ordinal) == ordinalText
}

func newHAPhysicalIsolationIntentReceipt(
	cluster *antflyv1.AntflyCluster,
	action *antflyv1.HAPlannedActionStatus,
	statefulSet *appsv1.StatefulSet,
	pods *corev1.PodList,
	lease *coordinationv1.Lease,
	scope haFencingLeaseScope,
) (*antflyv1.HAPhysicalIsolationReceiptStatus, error) {
	if cluster == nil || action == nil || statefulSet == nil || pods == nil || lease == nil ||
		cluster.UID == "" || statefulSet.UID == "" || statefulSet.Generation <= 0 || strings.TrimSpace(statefulSet.ResourceVersion) == "" ||
		strings.TrimSpace(pods.ResourceVersion) == "" || lease.UID == "" || strings.TrimSpace(lease.ResourceVersion) == "" ||
		lease.Spec.AcquireTime == nil {
		return nil, fmt.Errorf("isolate former primary: current Kubernetes object identities, resource versions, generation, and Lease transfer time are required")
	}
	topologyID := haFencingLeaseTopologyID(cluster)
	if topologyID == "" || strings.TrimSpace(lease.Annotations[haFencingLeaseAnnotationTopologyID]) != topologyID {
		return nil, fmt.Errorf("isolate former primary: shared fencing Lease topology identity is missing or stale")
	}
	initialPods := make([]antflyv1.HAPhysicalIsolationPodIdentity, 0, len(pods.Items))
	seen := make(map[string]struct{}, len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		if strings.TrimSpace(pod.Name) == "" || pod.UID == "" {
			return nil, fmt.Errorf("isolate former primary: initial Pod identity is incomplete")
		}
		key := pod.Name + "\x00" + string(pod.UID)
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("isolate former primary: duplicate initial Pod identity %s", pod.Name)
		}
		seen[key] = struct{}{}
		initialPods = append(initialPods, antflyv1.HAPhysicalIsolationPodIdentity{Name: pod.Name, UID: string(pod.UID)})
	}
	sort.Slice(initialPods, func(i, j int) bool {
		if initialPods[i].Name == initialPods[j].Name {
			return initialPods[i].UID < initialPods[j].UID
		}
		return initialPods[i].Name < initialPods[j].Name
	})
	watchdogMaxFenceLatencyMS, ok := haRuntimeLeaseMaxFenceLatencyMS(cluster)
	if !ok {
		return nil, fmt.Errorf("isolate former primary: configured runtime watchdog maximum fence latency is invalid")
	}
	receipt := &antflyv1.HAPhysicalIsolationReceiptStatus{
		ClusterUID:                        string(cluster.UID),
		StatefulSetName:                   statefulSet.Name,
		StatefulSetUID:                    string(statefulSet.UID),
		InitialStatefulSetGeneration:      statefulSet.Generation,
		InitialStatefulSetResourceVersion: statefulSet.ResourceVersion,
		InitialOldPods:                    initialPods,
		InitialPodListResourceVersion:     pods.ResourceVersion,
		LeaseName:                         lease.Name,
		LeaseUID:                          string(lease.UID),
		LeaseResourceVersion:              lease.ResourceVersion,
		LeaseHolder:                       strings.TrimSpace(*lease.Spec.HolderIdentity),
		LeaseGeneration:                   action.FenceGeneration,
		LeaseScope:                        haPhysicalIsolationLeaseScope(scope, topologyID),
		LeaseTransferTime:                 metav1.NewMicroTime(lease.Spec.AcquireTime.Time),
		WatchdogMaxFenceLatencyMS:         watchdogMaxFenceLatencyMS,
	}
	receipt.WatchdogProof = haBindPhysicalIsolationWatchdogProof(cluster, action, pods, receipt)
	if receipt.WatchdogProof == nil {
		return nil, fmt.Errorf("isolate former primary: exact pre-transfer runtime watchdog proof is unavailable")
	}
	return receipt, nil
}

func haBindPhysicalIsolationWatchdogProof(cluster *antflyv1.AntflyCluster, action *antflyv1.HAPlannedActionStatus, pods *corev1.PodList, receipt *antflyv1.HAPhysicalIsolationReceiptStatus) *antflyv1.HAPhysicalIsolationWatchdogProofStatus {
	if cluster == nil || cluster.Status.HAStatus == nil || action == nil || pods == nil || receipt == nil {
		return nil
	}
	proof := cluster.Status.HAStatus.PrimaryWatchdogProof
	if proof == nil || proof.CapabilityVersion != 1 || !proof.Active || !proof.AuthorityGranted || proof.LeaseName != receipt.LeaseName ||
		proof.LeaseNamespace != cluster.Namespace || strings.TrimSpace(proof.TopologyID) == "" ||
		strings.TrimSpace(proof.LocalNodeID) != strings.TrimSpace(action.StandbyName) ||
		strings.TrimSpace(proof.ObservedHolderNodeID) != strings.TrimSpace(action.StandbyName) ||
		strings.TrimSpace(proof.PodUID) == "" ||
		!isLowerHexDigest(proof.ProcessBootID) || proof.ObservedLeaseTransitions <= 0 ||
		proof.MaxFenceLatencyMS <= 0 || proof.MaxFenceLatencyMS != receipt.WatchdogMaxFenceLatencyMS ||
		uint64(proof.ObservedLeaseTransitions)+1 != action.FenceGeneration || proof.ObservedAt.IsZero() ||
		!haWatchdogProofObservedBeforeTransfer(proof.ObservedAt, receipt.LeaseTransferTime) {
		return nil
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if string(pod.UID) != proof.PodUID {
			continue
		}
		for j := range pod.Status.ContainerStatuses {
			container := &pod.Status.ContainerStatuses[j]
			if container.Name != "antfly" || strings.TrimSpace(container.ContainerID) == "" ||
				container.State.Running == nil || container.State.Running.StartedAt.IsZero() ||
				proof.ObservedAt.Before(&container.State.Running.StartedAt) {
				continue
			}
			return &antflyv1.HAPhysicalIsolationWatchdogProofStatus{
				CapabilityVersion: proof.CapabilityVersion, Active: proof.Active, AuthorityGranted: proof.AuthorityGranted,
				LeaseName: proof.LeaseName, LeaseNamespace: proof.LeaseNamespace,
				TopologyID: proof.TopologyID, LocalNodeID: proof.LocalNodeID, ObservedHolderNodeID: proof.ObservedHolderNodeID,
				PodName: pod.Name, PodUID: string(pod.UID), ContainerName: container.Name,
				ContainerID: container.ContainerID, ContainerRestartCount: container.RestartCount,
				ContainerStartedAt: *container.State.Running.StartedAt.DeepCopy(), ProcessBootID: proof.ProcessBootID,
				ObservedLeaseTransitions: proof.ObservedLeaseTransitions, MaxFenceLatencyMS: proof.MaxFenceLatencyMS,
				RuntimeObservedAt: *proof.ObservedAt.DeepCopy(),
			}
		}
	}
	return nil
}

func haPhysicalIsolationLeaseScope(scope haFencingLeaseScope, topologyID string) antflyv1.HAPhysicalIsolationLeaseScope {
	return antflyv1.HAPhysicalIsolationLeaseScope{
		TopologyID: topologyID,
		ClusterID:  scope.clusterID, ShardID: scope.shardID, TableID: scope.tableID,
		TimelineID: scope.timelineID, Epoch: scope.epoch,
		CurrentPrimaryID: scope.currentPrimaryID, PrimaryLSN: scope.primaryLSN,
	}
}

func haPhysicalIsolationReceiptScope(receipt *antflyv1.HAPhysicalIsolationReceiptStatus) (haFencingLeaseScope, bool) {
	if receipt == nil {
		return haFencingLeaseScope{}, false
	}
	scope := receipt.LeaseScope
	if strings.TrimSpace(scope.TopologyID) == "" || scope.ClusterID == 0 || scope.TimelineID == 0 || scope.Epoch == 0 ||
		strings.TrimSpace(scope.CurrentPrimaryID) == "" || scope.PrimaryLSN == 0 {
		return haFencingLeaseScope{}, false
	}
	return haFencingLeaseScope{
		clusterID: scope.ClusterID, shardID: scope.ShardID, tableID: scope.TableID,
		timelineID: scope.TimelineID, epoch: scope.Epoch,
		currentPrimaryID: scope.CurrentPrimaryID, primaryLSN: scope.PrimaryLSN,
	}, true
}

func validateCurrentPhysicalIsolationLease(lease *coordinationv1.Lease, action *antflyv1.HAPlannedActionStatus, scope haFencingLeaseScope) error {
	if lease == nil || action == nil || lease.Spec.LeaseTransitions == nil || *lease.Spec.LeaseTransitions <= 0 ||
		uint64(*lease.Spec.LeaseTransitions) != action.FenceGeneration {
		return fmt.Errorf("isolate former primary: fencing Lease generation does not match planned generation %d", action.FenceGeneration)
	}
	if lease.Spec.HolderIdentity == nil || strings.TrimSpace(*lease.Spec.HolderIdentity) != strings.TrimSpace(action.RouteTo) {
		return fmt.Errorf("isolate former primary: fencing Lease holder does not match promotion candidate %q", action.RouteTo)
	}
	scopeMatches := haLeaseFenceScopeMatches(lease, scope)
	if !scopeMatches && haPhysicalIsolationSucceededStructurallyWithEvidence(*action) {
		// Once isolation freezes the old writer, the former holder advances the
		// same Lease incarnation from the election lower bound to the proven tail.
		// Revalidation must accept that one-way strengthening or it would revoke
		// the receipt immediately before promotion. No other field or boundary is
		// allowed to drift.
		frozenScope := scope
		frozenScope.primaryLSN = action.PhysicalIsolationReceipt.FrozenBoundaryLSN
		scopeMatches = haLeaseFenceScopeMatches(lease, frozenScope)
	}
	if !scopeMatches {
		return fmt.Errorf("isolate former primary: fencing Lease scope does not match the planned topology")
	}
	expectedTopologyID := ""
	if action.PhysicalIsolationReceipt != nil {
		expectedTopologyID = strings.TrimSpace(action.PhysicalIsolationReceipt.LeaseScope.TopologyID)
	} else {
		expectedTopologyID = strings.TrimSpace(lease.Annotations[haFencingLeaseAnnotationTopologyID])
	}
	if expectedTopologyID == "" || strings.TrimSpace(lease.Annotations[haFencingLeaseAnnotationTopologyID]) != expectedTopologyID {
		return fmt.Errorf("isolate former primary: fencing Lease topology identity does not match the receipt")
	}
	return nil
}

func validateCurrentPhysicalIsolationObjects(cluster *antflyv1.AntflyCluster, action *antflyv1.HAPlannedActionStatus, statefulSet *appsv1.StatefulSet, lease *coordinationv1.Lease, scope haFencingLeaseScope) error {
	receipt := action.PhysicalIsolationReceipt
	if receipt == nil || statefulSet == nil || lease == nil {
		return fmt.Errorf("isolate former primary: physical-isolation intent evidence is missing")
	}
	if string(cluster.UID) != receipt.ClusterUID || statefulSet.Name != receipt.StatefulSetName || string(statefulSet.UID) != receipt.StatefulSetUID {
		return fmt.Errorf("isolate former primary: StatefulSet or cluster UID changed after physical-isolation intent")
	}
	if lease.Name != receipt.LeaseName || string(lease.UID) != receipt.LeaseUID || lease.Spec.AcquireTime == nil ||
		!lease.Spec.AcquireTime.Time.Equal(receipt.LeaseTransferTime.Time) {
		return fmt.Errorf("isolate former primary: fencing Lease incarnation or holder-transfer time changed after physical-isolation intent")
	}
	if err := validateCurrentPhysicalIsolationLease(lease, action, scope); err != nil {
		return err
	}
	return nil
}

func validateRecordedPhysicalIsolationObservation(action *antflyv1.HAPlannedActionStatus, statefulSet *appsv1.StatefulSet, lease *coordinationv1.Lease, pods *corev1.PodList, absenceProven bool) error {
	receipt := action.PhysicalIsolationReceipt
	if receipt == nil || receipt.ObservedAt == nil || statefulSet == nil || lease == nil || pods == nil {
		return fmt.Errorf("isolate former primary: physical-isolation observation is incomplete")
	}
	if receipt.IsolatedStatefulSetGeneration != statefulSet.Generation ||
		receipt.IsolatedStatefulSetObservedGeneration > statefulSet.Status.ObservedGeneration || receipt.AbsenceProven != absenceProven {
		return fmt.Errorf("isolate former primary: current Kubernetes objects no longer match the checkpointed physical-isolation observation")
	}
	return nil
}

func haPhysicalIsolationWatchdogFallbackProven(action antflyv1.HAPlannedActionStatus, pods *corev1.PodList) bool {
	receipt := action.PhysicalIsolationReceipt
	if receipt == nil || receipt.WatchdogProof == nil || pods == nil {
		return false
	}
	proof := receipt.WatchdogProof
	livePods := 0
	matched := false
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		livePods++
		if pod.Name != proof.PodName || string(pod.UID) != proof.PodUID {
			continue
		}
		for j := range pod.Status.ContainerStatuses {
			container := &pod.Status.ContainerStatuses[j]
			if container.Name == proof.ContainerName && container.ContainerID == proof.ContainerID &&
				container.RestartCount == proof.ContainerRestartCount && container.State.Running != nil &&
				container.State.Running.StartedAt.Equal(&proof.ContainerStartedAt) {
				matched = true
				break
			}
		}
	}
	return livePods == 1 && matched
}

func (r *AntflyClusterReconciler) haBoundaryReader() client.Reader {
	if r != nil && r.BoundaryReader != nil {
		return r.BoundaryReader
	}
	return r.Client
}

func (r *AntflyClusterReconciler) haPhysicalIsolationWatchdogBarrierElapsed(action *antflyv1.HAPlannedActionStatus) bool {
	if r == nil || action == nil || action.PhysicalIsolationReceipt == nil || action.PhysicalIsolationReceipt.WatchdogProof == nil {
		return false
	}
	receipt := action.PhysicalIsolationReceipt
	if receipt.WatchdogMaxFenceLatencyMS <= 0 {
		return false
	}
	key := haPhysicalIsolationGraceKey{
		clusterUID:          receipt.ClusterUID,
		leaseUID:            receipt.LeaseUID,
		leaseGeneration:     receipt.LeaseGeneration,
		leaseTransferUnixNS: receipt.LeaseTransferTime.UnixNano(),
		processBootID:       receipt.WatchdogProof.ProcessBootID,
	}
	now := r.haMonotonicNow()
	value, loaded := r.haIsolationGraceStarts.LoadOrStore(key, now)
	if !loaded {
		return false
	}
	started, ok := value.(time.Time)
	if !ok || now.Before(started) {
		// A test clock regression, or any corrupted local value, restarts the
		// full conservative wait instead of borrowing elapsed wall time.
		r.haIsolationGraceStarts.Store(key, now)
		return false
	}
	return now.Sub(started) >= time.Duration(receipt.WatchdogMaxFenceLatencyMS)*time.Millisecond
}

func (r *AntflyClusterReconciler) haMonotonicNow() time.Time {
	if r != nil && r.MonotonicNow != nil {
		return r.MonotonicNow()
	}
	// Do not call UTC/In/Round here: those operations can strip Go's monotonic
	// clock reading. This value is intentionally never persisted or serialized.
	return time.Now()
}

func validateHAFormerPrimaryIsolationAction(cluster *antflyv1.AntflyCluster, action *antflyv1.HAPlannedActionStatus) error {
	if cluster == nil || action == nil || cluster.Spec.HighAvailability == nil || cluster.Spec.HighAvailability.AutomaticFailover == nil {
		return fmt.Errorf("isolate former primary: automatic HA configuration is missing")
	}
	ha := cluster.Spec.HighAvailability
	identity := haReplicationIdentity(ha)
	if identity == nil || strings.TrimSpace(identity.CurrentPrimaryID) == "" ||
		!haPhysicalIsolationActionMatchesIdentity(cluster, action, identity) ||
		strings.TrimSpace(action.RouteTo) == "" || strings.TrimSpace(action.RouteTo) != strings.TrimSpace(action.FenceHolder) ||
		action.FenceAuthority != antflyv1.HAFencingAuthorityKubernetesLease ||
		ha.AutomaticFailover.FencingAuthority != antflyv1.HAFencingAuthorityKubernetesLease || action.FenceGeneration == 0 || action.TargetLSN == 0 {
		return fmt.Errorf("isolate former primary: action identity or Kubernetes Lease authority is incomplete")
	}
	return nil
}

func haPhysicalIsolationActionMatchesIdentity(cluster *antflyv1.AntflyCluster, action *antflyv1.HAPlannedActionStatus, identity *antflyv1.HAReplicationIdentitySpec) bool {
	if cluster == nil || action == nil || identity == nil {
		return false
	}
	if strings.TrimSpace(action.StandbyName) == strings.TrimSpace(identity.CurrentPrimaryID) {
		return true
	}
	promotion := haPromotionReceipt(cluster.Status.HAStatus)
	return promotion != nil && haIdentityMatchesPromotionParentOrChild(identity, promotion) &&
		strings.TrimSpace(action.StandbyName) == strings.TrimSpace(promotion.OldPrimaryID) &&
		strings.TrimSpace(action.RouteTo) == strings.TrimSpace(promotion.PromotedStandbyID) &&
		action.FenceGeneration == promotion.FenceGeneration
}

func haPhysicalIsolationTopologyAdvanced(cluster *antflyv1.AntflyCluster, action *antflyv1.HAPlannedActionStatus) bool {
	if !haPhysicalIsolationPromotionRecorded(cluster, action) {
		return false
	}
	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	promotion := haPromotionReceipt(cluster.Status.HAStatus)
	return identity != nil &&
		strings.TrimSpace(identity.CurrentPrimaryID) == strings.TrimSpace(promotion.PromotedStandbyID) &&
		identity.TimelineID == promotion.NewTimelineID && identity.Epoch == promotion.NewEpoch
}

func haPhysicalIsolationPromotionRecorded(cluster *antflyv1.AntflyCluster, action *antflyv1.HAPlannedActionStatus) bool {
	if cluster == nil || action == nil || action.AdminJobPhase != haAdminJobPhaseSucceeded {
		return false
	}
	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	promotion := haPromotionReceipt(cluster.Status.HAStatus)
	return identity != nil && promotion != nil && haIdentityMatchesPromotionParentOrChild(identity, promotion) &&
		strings.TrimSpace(action.StandbyName) == strings.TrimSpace(promotion.OldPrimaryID) &&
		strings.TrimSpace(action.RouteTo) == strings.TrimSpace(promotion.PromotedStandbyID) &&
		action.FenceGeneration == promotion.FenceGeneration
}

func validateHAPhysicalIsolationIntent(cluster *antflyv1.AntflyCluster, action *antflyv1.HAPlannedActionStatus) error {
	if err := validateHAFormerPrimaryIsolationAction(cluster, action); err != nil {
		return err
	}
	receipt := action.PhysicalIsolationReceipt
	if !haPhysicalIsolationIntentStructurallyMatches(*action) || cluster.UID == "" || receipt.ClusterUID != string(cluster.UID) ||
		receipt.StatefulSetName != standaloneStatefulSetName(cluster) || strings.TrimSpace(receipt.StatefulSetUID) == "" ||
		receipt.LeaseName != haFencingLeaseName(cluster) {
		return fmt.Errorf("isolate former primary: physical-isolation intent receipt is incomplete or does not match the action")
	}
	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	scope, ok := haPhysicalIsolationReceiptScope(receipt)
	configuredMaxFenceLatencyMS, latencyOK := haRuntimeLeaseMaxFenceLatencyMS(cluster)
	identityMatchesScope := identity != nil && scope.clusterID == identity.ClusterID && scope.shardID == identity.ShardID &&
		scope.tableID == identity.TableID && scope.timelineID == identity.TimelineID && scope.epoch == identity.Epoch &&
		strings.TrimSpace(scope.currentPrimaryID) == strings.TrimSpace(identity.CurrentPrimaryID)
	if !identityMatchesScope && identity != nil {
		promotion := haPromotionReceipt(cluster.Status.HAStatus)
		identityMatchesScope = promotion != nil && haIdentityMatchesPromotionParentOrChild(identity, promotion) &&
			scope.clusterID == identity.ClusterID && scope.shardID == identity.ShardID && scope.tableID == identity.TableID &&
			scope.timelineID == promotion.ParentTimelineID && scope.epoch == promotion.ParentEpoch &&
			strings.TrimSpace(scope.currentPrimaryID) == strings.TrimSpace(promotion.OldPrimaryID) &&
			strings.TrimSpace(action.StandbyName) == strings.TrimSpace(promotion.OldPrimaryID) &&
			strings.TrimSpace(action.RouteTo) == strings.TrimSpace(promotion.PromotedStandbyID) &&
			action.FenceGeneration == promotion.FenceGeneration
	}
	if identity == nil || !ok || !identityMatchesScope ||
		scope.primaryLSN == 0 || scope.primaryLSN > action.TargetLSN ||
		strings.TrimSpace(receipt.LeaseScope.TopologyID) != haFencingLeaseTopologyID(cluster) || !latencyOK ||
		receipt.WatchdogMaxFenceLatencyMS != configuredMaxFenceLatencyMS {
		return fmt.Errorf("isolate former primary: physical-isolation Lease scope does not match the HA identity and boundary")
	}
	seenNames := make(map[string]struct{}, len(receipt.InitialOldPods))
	seenUIDs := make(map[string]struct{}, len(receipt.InitialOldPods))
	for _, pod := range receipt.InitialOldPods {
		name := strings.TrimSpace(pod.Name)
		uid := strings.TrimSpace(pod.UID)
		if name == "" || uid == "" {
			return fmt.Errorf("isolate former primary: initial old-Pod receipt contains an incomplete identity")
		}
		if _, duplicate := seenNames[name]; duplicate {
			return fmt.Errorf("isolate former primary: initial old-Pod receipt reuses Pod name %s", name)
		}
		if _, duplicate := seenUIDs[uid]; duplicate {
			return fmt.Errorf("isolate former primary: initial old-Pod receipt reuses Pod UID %s", uid)
		}
		seenNames[name] = struct{}{}
		seenUIDs[uid] = struct{}{}
	}
	return nil
}

func haPhysicalIsolationIntentStructurallyMatches(action antflyv1.HAPlannedActionStatus) bool {
	receipt := action.PhysicalIsolationReceipt
	if haActionKind(action.Kind) != haActionIsolateFormerPrimary || receipt == nil ||
		strings.TrimSpace(receipt.ClusterUID) == "" || strings.TrimSpace(receipt.StatefulSetName) == "" ||
		strings.TrimSpace(receipt.StatefulSetUID) == "" || receipt.InitialStatefulSetGeneration <= 0 ||
		strings.TrimSpace(receipt.InitialStatefulSetResourceVersion) == "" || strings.TrimSpace(receipt.InitialPodListResourceVersion) == "" ||
		strings.TrimSpace(receipt.LeaseName) == "" || strings.TrimSpace(receipt.LeaseUID) == "" ||
		strings.TrimSpace(receipt.LeaseResourceVersion) == "" ||
		strings.TrimSpace(receipt.LeaseHolder) != strings.TrimSpace(action.FenceHolder) ||
		receipt.LeaseGeneration != action.FenceGeneration || receipt.LeaseTransferTime.IsZero() ||
		receipt.WatchdogMaxFenceLatencyMS <= 0 {
		return false
	}
	_, ok := haPhysicalIsolationReceiptScope(receipt)
	return ok
}

// haPhysicalIsolationSucceededWithEvidence deliberately revalidates every
// receipt field even when AdminJobPhase already says Succeeded. Status phase is
// not authority at this split-brain boundary.
func haPhysicalIsolationSucceededWithEvidence(cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) bool {
	if cluster == nil || validateHAPhysicalIsolationIntent(cluster, &action) != nil ||
		!haPhysicalIsolationSucceededStructurallyWithEvidence(action) {
		return false
	}
	return true
}

func haPhysicalIsolationSucceededStructurallyWithEvidence(action antflyv1.HAPlannedActionStatus) bool {
	if action.AdminJobPhase != haAdminJobPhaseSucceeded || action.AdminJobName != haKubernetesPhysicalFenceName ||
		action.CompletedAt == nil || !haPhysicalIsolationIntentStructurallyMatches(action) {
		return false
	}
	receipt := action.PhysicalIsolationReceipt
	if receipt == nil || receipt.IsolatedStatefulSetGeneration <= receipt.InitialStatefulSetGeneration ||
		receipt.IsolatedStatefulSetObservedGeneration < receipt.IsolatedStatefulSetGeneration ||
		strings.TrimSpace(receipt.IsolatedStatefulSetResourceVersion) == "" ||
		strings.TrimSpace(receipt.ObservedLeaseResourceVersion) == "" ||
		receipt.FrozenBoundaryLSN == 0 || receipt.FrozenBoundaryLSN != action.TargetLSN ||
		action.ObservedLSN != receipt.FrozenBoundaryLSN || receipt.ObservedAt == nil || receipt.CompletedAt == nil ||
		receipt.ObservedAt.After(receipt.CompletedAt.Time) ||
		!receipt.CompletedAt.Equal(action.CompletedAt) {
		return false
	}
	if receipt.AbsenceProven {
		if strings.TrimSpace(receipt.AbsencePodListResourceVersion) == "" || !haPhysicalIsolationWatchdogProofStructurallyValid(action) {
			return false
		}
	} else {
		if strings.TrimSpace(receipt.AbsencePodListResourceVersion) != "" || !haPhysicalIsolationWatchdogProofStructurallyValid(action) {
			return false
		}
	}
	return true
}

func haPhysicalIsolationWatchdogProofStructurallyValid(action antflyv1.HAPlannedActionStatus) bool {
	receipt := action.PhysicalIsolationReceipt
	if receipt == nil || receipt.WatchdogProof == nil {
		return false
	}
	proof := receipt.WatchdogProof
	if proof.CapabilityVersion != 1 || !proof.Active || !proof.AuthorityGranted || proof.LeaseName != receipt.LeaseName || proof.LeaseNamespace == "" ||
		strings.TrimSpace(proof.TopologyID) != strings.TrimSpace(receipt.LeaseScope.TopologyID) || strings.TrimSpace(proof.LocalNodeID) != strings.TrimSpace(action.StandbyName) ||
		strings.TrimSpace(proof.ObservedHolderNodeID) != strings.TrimSpace(action.StandbyName) ||
		strings.TrimSpace(proof.PodName) == "" || strings.TrimSpace(proof.PodUID) == "" || proof.ContainerName != "antfly" ||
		strings.TrimSpace(proof.ContainerID) == "" || proof.ContainerStartedAt.IsZero() || !isLowerHexDigest(proof.ProcessBootID) ||
		proof.MaxFenceLatencyMS <= 0 || proof.MaxFenceLatencyMS != receipt.WatchdogMaxFenceLatencyMS ||
		proof.ObservedLeaseTransitions <= 0 || uint64(proof.ObservedLeaseTransitions)+1 != receipt.LeaseGeneration ||
		proof.RuntimeObservedAt.IsZero() || !haWatchdogProofObservedBeforeTransfer(proof.RuntimeObservedAt, receipt.LeaseTransferTime) ||
		proof.RuntimeObservedAt.Before(&proof.ContainerStartedAt) {
		return false
	}
	for _, pod := range receipt.InitialOldPods {
		if pod.Name == proof.PodName && pod.UID == proof.PodUID {
			return true
		}
	}
	return false
}

func haWatchdogProofObservedBeforeTransfer(observedAt metav1.Time, transferAt metav1.MicroTime) bool {
	if observedAt.IsZero() || transferAt.IsZero() {
		return false
	}
	return observedAt.Time.Before(transferAt.Time)
}

func haIsolatedPromotionBoundary(status *antflyv1.HAStatus, action antflyv1.HAPlannedActionStatus) (uint64, bool) {
	if status == nil {
		return 0, false
	}
	boundary := action.TargetLSN
	if status.PrimaryLSN > boundary {
		boundary = status.PrimaryLSN
	}
	standbys := haStandbyStatusByName(status)
	standby, ok := standbys[action.RouteTo]
	if !ok || !standbyPromotionEligible(standby) || !standby.CaughtUpToReceived || !standby.CanServeSafeReads {
		return 0, false
	}
	if standby.AppliedLSN > boundary {
		boundary = standby.AppliedLSN
	}
	if boundary == 0 || standby.ReceivedLSN < boundary || standby.AppliedLSN < boundary || standby.SafeReadLSN < boundary {
		return 0, false
	}
	return boundary, true
}

func haFormerPrimaryIsolationActive(cluster *antflyv1.AntflyCluster) bool {
	if cluster == nil || cluster.Status.HAStatus == nil {
		return false
	}
	for i := range cluster.Status.HAStatus.PlannedActions {
		action := &cluster.Status.HAStatus.PlannedActions[i]
		if haActionKind(action.Kind) != haActionIsolateFormerPrimary ||
			(action.AdminJobPhase != haAdminJobPhaseRunning && action.AdminJobPhase != haAdminJobPhaseSucceeded) {
			continue
		}
		if validateHAFormerPrimaryIsolationAction(cluster, action) == nil {
			return true
		}
	}
	return false
}
