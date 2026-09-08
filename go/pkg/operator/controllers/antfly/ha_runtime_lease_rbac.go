package controllers

import (
	"context"
	"fmt"
	"strings"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	haRuntimeLeaseRBACSuffix                  = "-ha-runtime-lease"
	defaultHARuntimeLeaseWatchdogGraceSeconds = int32(10)
)

// reconcileHARuntimeLeaseRBAC grants the runtime only get/watch access to its
// exact fencing Lease. It does not grant list, create, update, patch, or delete:
// ownership transfer remains exclusively an operator responsibility.
func (r *AntflyClusterReconciler) reconcileHARuntimeLeaseRBAC(ctx context.Context, cluster *antflyv1.AntflyCluster) (string, error) {
	if cluster == nil {
		return "", fmt.Errorf("reconcile HA runtime Lease RBAC: cluster is nil")
	}
	configured := strings.TrimSpace(cluster.Spec.ServiceAccountName)
	if !haRuntimeLeaseWatchdogEnabled(cluster) {
		if err := r.cleanupHARuntimeLeaseRBAC(ctx, cluster, true); err != nil {
			return "", err
		}
		return configured, nil
	}
	serviceAccountName := configured
	if serviceAccountName == "" {
		serviceAccountName = cluster.Name + "-ha-runtime"
		serviceAccount := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
			Name: serviceAccountName, Namespace: cluster.Namespace,
		}}
		if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, serviceAccount, func() error {
			return controllerutil.SetControllerReference(cluster, serviceAccount, r.Scheme)
		}); err != nil {
			return "", fmt.Errorf("reconcile HA runtime ServiceAccount: %w", err)
		}
	} else if err := r.cleanupGeneratedHARuntimeServiceAccount(ctx, cluster); err != nil {
		return "", err
	}

	roleName := cluster.Name + haRuntimeLeaseRBACSuffix
	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: roleName, Namespace: cluster.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, role, func() error {
		if err := controllerutil.SetControllerReference(cluster, role, r.Scheme); err != nil {
			return err
		}
		role.Rules = []rbacv1.PolicyRule{{
			APIGroups:     []string{"coordination.k8s.io"},
			Resources:     []string{"leases"},
			ResourceNames: []string{haFencingLeaseName(cluster)},
			Verbs:         []string{"get", "watch"},
		}}
		return nil
	}); err != nil {
		return "", fmt.Errorf("reconcile HA runtime Lease Role: %w", err)
	}

	binding := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: roleName, Namespace: cluster.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, binding, func() error {
		if err := controllerutil.SetControllerReference(cluster, binding, r.Scheme); err != nil {
			return err
		}
		binding.RoleRef = rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: roleName}
		binding.Subjects = []rbacv1.Subject{{
			Kind: "ServiceAccount", Name: serviceAccountName, Namespace: cluster.Namespace,
		}}
		return nil
	}); err != nil {
		return "", fmt.Errorf("reconcile HA runtime Lease RoleBinding: %w", err)
	}
	return serviceAccountName, nil
}

// cleanupHARuntimeLeaseRBAC removes only resources owned by this cluster. A
// resource with the conventional name but different ownership is never
// adopted or deleted. RoleBinding is removed first so access is revoked before
// the Role and generated identity disappear.
func (r *AntflyClusterReconciler) cleanupHARuntimeLeaseRBAC(ctx context.Context, cluster *antflyv1.AntflyCluster, removeGeneratedServiceAccount bool) error {
	roleName := cluster.Name + haRuntimeLeaseRBACSuffix
	objects := []struct {
		object client.Object
		kind   string
	}{
		{object: &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: roleName, Namespace: cluster.Namespace}}, kind: "RoleBinding"},
		{object: &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: roleName, Namespace: cluster.Namespace}}, kind: "Role"},
	}
	for _, item := range objects {
		if err := r.deleteHAOwnedObject(ctx, cluster, item.object); err != nil {
			return fmt.Errorf("cleanup HA runtime Lease %s: %w", item.kind, err)
		}
	}
	if removeGeneratedServiceAccount {
		return r.cleanupGeneratedHARuntimeServiceAccount(ctx, cluster)
	}
	return nil
}

func (r *AntflyClusterReconciler) cleanupGeneratedHARuntimeServiceAccount(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	serviceAccount := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Name: cluster.Name + "-ha-runtime", Namespace: cluster.Namespace,
	}}
	if err := r.deleteHAOwnedObject(ctx, cluster, serviceAccount); err != nil {
		return fmt.Errorf("cleanup generated HA runtime ServiceAccount: %w", err)
	}
	return nil
}

func (r *AntflyClusterReconciler) deleteHAOwnedObject(ctx context.Context, cluster *antflyv1.AntflyCluster, object client.Object) error {
	// Some focused reconcilers intentionally install only the API groups they
	// exercise. Such a client cannot contain this object kind, so cleanup is a
	// no-op rather than turning an unrelated reconciliation into a hard failure.
	if _, _, err := r.Scheme.ObjectKinds(object); err != nil {
		if k8sruntime.IsNotRegisteredError(err) {
			return nil
		}
		return err
	}
	key := client.ObjectKeyFromObject(object)
	if err := r.Get(ctx, key, object); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	owner := metav1.GetControllerOf(object)
	if owner == nil || owner.UID != cluster.UID {
		return nil
	}
	if err := r.Delete(ctx, object); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func haRuntimeLeaseEnv(cluster *antflyv1.AntflyCluster) []corev1.EnvVar {
	if !haRuntimeLeaseWatchdogEnabled(cluster) {
		return nil
	}
	lease := cluster.Spec.HighAvailability.Runtime.FencingLease
	maxFenceLatencyMS, ok := haRuntimeLeaseMaxFenceLatencyMS(cluster)
	if !ok {
		return nil
	}
	return []corev1.EnvVar{
		{Name: "ANTFLY_HA_LEASE_NAME", Value: lease.Name},
		{Name: "ANTFLY_HA_LEASE_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}},
		{Name: "ANTFLY_HA_LEASE_TOPOLOGY_ID", Value: lease.TopologyID},
		{Name: "ANTFLY_HA_LEASE_GRACE_MS", Value: fmt.Sprintf("%d", maxFenceLatencyMS)},
		{Name: "ANTFLY_HA_LEASE_SENTINEL_PATH", Value: "/antflydb/ha/lease-fenced"},
	}
}

// haRuntimeLeaseMaxFenceLatencyMS is the single conversion used both to
// configure the runtime and to validate its authenticated proof. The receipt
// must not invent an independent grace period.
func haRuntimeLeaseMaxFenceLatencyMS(cluster *antflyv1.AntflyCluster) (int32, bool) {
	if !haRuntimeLeaseWatchdogEnabled(cluster) {
		return 0, false
	}
	seconds := cluster.Spec.HighAvailability.Runtime.FencingLease.WatchdogGraceSeconds
	if seconds == 0 {
		seconds = defaultHARuntimeLeaseWatchdogGraceSeconds
	}
	milliseconds := int64(seconds) * 1000
	if seconds <= 0 || milliseconds > int64(^uint32(0)>>1) {
		return 0, false
	}
	return int32(milliseconds), true
}

func haRuntimeLeaseWatchdogEnabled(cluster *antflyv1.AntflyCluster) bool {
	if cluster == nil || cluster.Spec.HighAvailability == nil {
		return false
	}
	ha := cluster.Spec.HighAvailability
	if ha.Mode == "" || ha.Mode == antflyv1.HAModeDisabled ||
		ha.AutomaticFailover == nil || !ha.AutomaticFailover.Enabled ||
		ha.AutomaticFailover.FencingAuthority != antflyv1.HAFencingAuthorityKubernetesLease ||
		ha.Runtime == nil ||
		(ha.Runtime.Role != antflyv1.HARuntimeRolePrimary && ha.Runtime.Role != antflyv1.HARuntimeRoleStandby) ||
		ha.Runtime.FencingLease == nil {
		return false
	}
	// A standby only observes authority and must do so before it is promotable;
	// its CR intentionally cannot execute topology actions. A primary, however,
	// must never arm a fail-closed watchdog unless this controller also owns
	// Lease renewal.
	if ha.Runtime.Role == antflyv1.HARuntimeRolePrimary && !haAutomaticFailoverExecutionEnabled(ha) {
		return false
	}
	lease := ha.Runtime.FencingLease
	return strings.TrimSpace(lease.Name) != "" && strings.TrimSpace(lease.TopologyID) != ""
}
