package controllers

import (
	"context"
	"fmt"
	"strings"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const haKubernetesPhysicalFenceName = "kubernetes-physical-fence"

// reconcileHAFormerPrimaryIsolation is the fail-safe path for an automatic
// failover whose former primary cannot be reached through its admin endpoint.
// A Kubernetes Lease alone is only an election record; it does not stop the
// old writer. This controller action first holds the old StatefulSet at zero,
// then waits for a linearizable PodList to prove that no old runtime remains,
// and only then releases the candidate fence/promotion dependency.
func (r *AntflyClusterReconciler) reconcileHAFormerPrimaryIsolation(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	if r == nil || r.Client == nil || cluster == nil || cluster.Status.HAStatus == nil {
		return nil
	}
	for i := range cluster.Status.HAStatus.PlannedActions {
		action := &cluster.Status.HAStatus.PlannedActions[i]
		if haActionKind(action.Kind) != haActionIsolateFormerPrimary || action.AdminJobPhase == haAdminJobPhaseSucceeded {
			continue
		}
		if err := validateHAFormerPrimaryIsolationAction(cluster, action); err != nil {
			return err
		}
		lease := &coordinationv1.Lease{}
		reader := r.haBoundaryReader()
		if err := reader.Get(ctx, types.NamespacedName{Name: haFencingLeaseName(cluster), Namespace: cluster.Namespace}, lease); err != nil {
			return fmt.Errorf("isolate former primary: read fencing Lease: %w", err)
		}
		if generation := haLeaseFenceGeneration(lease); generation != action.FenceGeneration {
			return fmt.Errorf("isolate former primary: fencing Lease generation %d does not match planned generation %d", generation, action.FenceGeneration)
		}
		if lease.Spec.HolderIdentity == nil || strings.TrimSpace(*lease.Spec.HolderIdentity) != strings.TrimSpace(action.RouteTo) {
			return fmt.Errorf("isolate former primary: fencing Lease holder does not match promotion candidate %q", action.RouteTo)
		}
		ready, reason := haLeaseFenceReady(lease, action.FenceGeneration, r.haNow())
		if !ready {
			return fmt.Errorf("isolate former primary: fencing Lease is not current: %s", reason)
		}
		scope, ok := haCurrentFencingLeaseScope(cluster)
		if !ok || !haLeaseFenceScopeMatches(lease, scope) {
			return fmt.Errorf("isolate former primary: fencing Lease scope does not match the planned topology")
		}

		statefulSet := &appsv1.StatefulSet{}
		key := types.NamespacedName{Name: cluster.Name + "-standalone", Namespace: cluster.Namespace}
		if err := reader.Get(ctx, key, statefulSet); err != nil {
			return fmt.Errorf("isolate former primary: read StatefulSet: %w", err)
		}
		owner := metav1.GetControllerOf(statefulSet)
		if owner == nil || owner.UID != cluster.UID {
			return fmt.Errorf("isolate former primary: StatefulSet %s is not controlled by AntflyCluster UID %s", statefulSet.Name, cluster.UID)
		}
		if action.AdminJobPhase == "" || action.AdminJobPhase == haAdminJobPhaseWaitingDependency {
			action.AdminJobName = haKubernetesPhysicalFenceName
			action.AdminJobPhase = haAdminJobPhaseRunning
			action.AdminError = ""
			action.ErrorClass = ""
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
			return nil
		}

		var pods corev1.PodList
		if err := reader.List(ctx, &pods, client.InNamespace(cluster.Namespace), client.MatchingLabels(serviceSelectorLabels(cluster.Name, "standalone"))); err != nil {
			return fmt.Errorf("isolate former primary: list runtime pods: %w", err)
		}
		for j := range pods.Items {
			pod := &pods.Items[j]
			// A deletion timestamp is only intent; kubelet may still be running
			// the container throughout its grace period. Require the API snapshot
			// to show either no Pod object or a terminal process state.
			if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
				return nil
			}
		}
		if (statefulSet.Generation > 0 && statefulSet.Status.ObservedGeneration < statefulSet.Generation) ||
			statefulSet.Status.Replicas != 0 || statefulSet.Status.CurrentReplicas != 0 || statefulSet.Status.ReadyReplicas != 0 {
			return nil
		}
		if action.LastAttemptAt == nil {
			// The standby observation earlier in this reconciliation may predate
			// the final old-writer exit by a few milliseconds. Checkpoint the first
			// exact absence observation and require another reconciliation before
			// freezing the boundary; that next pass refreshes candidate applied/safe
			// LSNs strictly after the former writer was proven gone.
			now := metav1.NewTime(r.haNow())
			action.LastAttemptAt = &now
			if err := r.persistHAActionPlanBarrier(ctx, cluster); err != nil {
				return fmt.Errorf("isolate former primary: persist pod-absence barrier: %w", err)
			}
			return errHAStatusCheckpointed
		}

		boundary, ok := haIsolatedPromotionBoundary(cluster.Status.HAStatus, *action)
		if !ok {
			return nil
		}
		action.TargetLSN = boundary
		action.ObservedLSN = boundary
		action.AdminJobPhase = haAdminJobPhaseSucceeded
		action.AdminError = ""
		action.ErrorClass = ""
		now := metav1.NewTime(r.haNow())
		action.CompletedAt = &now
		return nil
	}
	return nil
}

func (r *AntflyClusterReconciler) haBoundaryReader() client.Reader {
	if r != nil && r.BoundaryReader != nil {
		return r.BoundaryReader
	}
	return r.Client
}

func validateHAFormerPrimaryIsolationAction(cluster *antflyv1.AntflyCluster, action *antflyv1.HAPlannedActionStatus) error {
	if cluster == nil || action == nil || cluster.Spec.HighAvailability == nil || cluster.Spec.HighAvailability.AutomaticFailover == nil {
		return fmt.Errorf("isolate former primary: automatic HA configuration is missing")
	}
	ha := cluster.Spec.HighAvailability
	identity := haReplicationIdentity(ha)
	if identity == nil || strings.TrimSpace(identity.CurrentPrimaryID) == "" ||
		strings.TrimSpace(action.StandbyName) != strings.TrimSpace(identity.CurrentPrimaryID) ||
		strings.TrimSpace(action.RouteTo) == "" || strings.TrimSpace(action.RouteTo) != strings.TrimSpace(action.FenceHolder) ||
		action.FenceAuthority != antflyv1.HAFencingAuthorityKubernetesLease ||
		ha.AutomaticFailover.FencingAuthority != antflyv1.HAFencingAuthorityKubernetesLease || action.FenceGeneration == 0 || action.TargetLSN == 0 {
		return fmt.Errorf("isolate former primary: action identity or Kubernetes Lease authority is incomplete")
	}
	return nil
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
