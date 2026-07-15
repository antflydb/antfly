package controllers

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestReconcileHARuntimeLeaseRBACIsExactAndReadOnly(t *testing.T) {
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Spec.ServiceAccountName = ""
	reconciler := testHAReconciler(t, cluster)

	serviceAccountName, err := reconciler.reconcileHARuntimeLeaseRBAC(context.Background(), cluster)
	if err != nil {
		t.Fatalf("reconcile HA runtime lease RBAC: %v", err)
	}
	if serviceAccountName != cluster.Name+"-ha-runtime" {
		t.Fatalf("service account = %q", serviceAccountName)
	}
	serviceAccount := &corev1.ServiceAccount{}
	if err := reconciler.Get(context.Background(), client.ObjectKey{Name: serviceAccountName, Namespace: cluster.Namespace}, serviceAccount); err != nil {
		t.Fatalf("get service account: %v", err)
	}
	role := &rbacv1.Role{}
	roleName := cluster.Name + haRuntimeLeaseRBACSuffix
	if err := reconciler.Get(context.Background(), client.ObjectKey{Name: roleName, Namespace: cluster.Namespace}, role); err != nil {
		t.Fatalf("get role: %v", err)
	}
	if len(role.Rules) != 1 {
		t.Fatalf("rules = %#v", role.Rules)
	}
	rule := role.Rules[0]
	if len(rule.APIGroups) != 1 || rule.APIGroups[0] != "coordination.k8s.io" ||
		len(rule.Resources) != 1 || rule.Resources[0] != "leases" ||
		len(rule.ResourceNames) != 1 || rule.ResourceNames[0] != haFencingLeaseName(cluster) ||
		len(rule.Verbs) != 2 || rule.Verbs[0] != "get" || rule.Verbs[1] != "watch" {
		t.Fatalf("runtime role is not exact read-only Lease access: %#v", rule)
	}
	binding := &rbacv1.RoleBinding{}
	if err := reconciler.Get(context.Background(), client.ObjectKey{Name: roleName, Namespace: cluster.Namespace}, binding); err != nil {
		t.Fatalf("get binding: %v", err)
	}
	if len(binding.Subjects) != 1 || binding.Subjects[0].Name != serviceAccountName ||
		binding.RoleRef.Name != roleName || binding.RoleRef.Kind != "Role" {
		t.Fatalf("binding = %#v", binding)
	}
}

func TestHARuntimeLeaseEnvBindsExactAuthorityAndPersistentSentinel(t *testing.T) {
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	env := haRuntimeLeaseEnv(cluster)
	values := map[string]string{}
	for _, variable := range env {
		values[variable.Name] = variable.Value
	}
	if values["ANTFLY_HA_LEASE_NAME"] != haFencingLeaseName(cluster) ||
		values["ANTFLY_HA_LEASE_CURRENT_PRIMARY_ID"] != cluster.Spec.HighAvailability.Identity.CurrentPrimaryID ||
		values["ANTFLY_HA_LEASE_GRACE_MS"] != "10000" ||
		values["ANTFLY_HA_LEASE_SENTINEL_PATH"] != "/antflydb/ha/lease-fenced" {
		t.Fatalf("unexpected runtime Lease env: %#v", env)
	}
	var namespaceDownward bool
	for _, variable := range env {
		if variable.Name == "ANTFLY_HA_LEASE_NAMESPACE" && variable.ValueFrom != nil &&
			variable.ValueFrom.FieldRef != nil && variable.ValueFrom.FieldRef.FieldPath == "metadata.namespace" {
			namespaceDownward = true
		}
	}
	if !namespaceDownward {
		t.Fatalf("namespace must use downward API: %#v", env)
	}
}
