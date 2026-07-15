package controllers

import (
	"context"
	"fmt"
	"strings"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const haRuntimeLeaseRBACSuffix = "-ha-runtime-lease"

// reconcileHARuntimeLeaseRBAC grants the runtime only get/watch access to its
// exact fencing Lease. It does not grant list, create, update, patch, or delete:
// ownership transfer remains exclusively an operator responsibility.
func (r *AntflyClusterReconciler) reconcileHARuntimeLeaseRBAC(ctx context.Context, cluster *antflyv1.AntflyCluster) (string, error) {
	if cluster == nil {
		return "", fmt.Errorf("reconcile HA runtime Lease RBAC: cluster is nil")
	}
	configured := strings.TrimSpace(cluster.Spec.ServiceAccountName)
	if !haKubernetesLeaseRenewalEnabled(cluster) {
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

func haRuntimeLeaseEnv(cluster *antflyv1.AntflyCluster) []corev1.EnvVar {
	if !haKubernetesLeaseRenewalEnabled(cluster) || cluster.Spec.HighAvailability == nil ||
		cluster.Spec.HighAvailability.Identity == nil || cluster.Spec.HighAvailability.Runtime == nil {
		return nil
	}
	identity := cluster.Spec.HighAvailability.Identity
	return []corev1.EnvVar{
		{Name: "ANTFLY_HA_LEASE_NAME", Value: haFencingLeaseName(cluster)},
		{Name: "ANTFLY_HA_LEASE_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}},
		{Name: "ANTFLY_HA_LEASE_CURRENT_PRIMARY_ID", Value: strings.TrimSpace(identity.CurrentPrimaryID)},
		{Name: "ANTFLY_HA_LEASE_GRACE_MS", Value: "10000"},
		{Name: "ANTFLY_HA_LEASE_SENTINEL_PATH", Value: "/antflydb/ha/lease-fenced"},
	}
}
