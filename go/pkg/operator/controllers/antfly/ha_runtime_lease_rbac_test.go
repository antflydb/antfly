package controllers

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
		values["ANTFLY_HA_LEASE_TOPOLOGY_ID"] != cluster.Spec.HighAvailability.Runtime.FencingLease.TopologyID ||
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

func TestHARuntimeLeaseWatchdogConfiguresStandbyWithoutRenewal(t *testing.T) {
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Spec.HighAvailability.Runtime.Role = "Standby"
	if !haRuntimeLeaseWatchdogEnabled(cluster) {
		t.Fatal("standby promotion candidate must observe the fencing Lease")
	}
	if haKubernetesLeaseRenewalEnabled(cluster) {
		t.Fatal("standby promotion candidate must not renew primary authority")
	}
	if len(haRuntimeLeaseEnv(cluster)) == 0 {
		t.Fatal("standby promotion candidate is missing its watchdog environment")
	}

	reconciler := testHAReconciler(t, cluster)
	serviceAccountName, err := reconciler.reconcileHARuntimeLeaseRBAC(context.Background(), cluster)
	if err != nil {
		t.Fatalf("reconcile standby HA runtime Lease RBAC: %v", err)
	}
	if serviceAccountName != cluster.Name+"-ha-runtime" {
		t.Fatalf("standby runtime service account = %q", serviceAccountName)
	}
	role := &rbacv1.Role{}
	if err := reconciler.Get(context.Background(), client.ObjectKey{
		Name: cluster.Name + haRuntimeLeaseRBACSuffix, Namespace: cluster.Namespace,
	}, role); err != nil {
		t.Fatalf("get standby runtime Lease Role: %v", err)
	}
	if len(role.Rules) != 1 || len(role.Rules[0].Verbs) != 2 ||
		role.Rules[0].Verbs[0] != "get" || role.Rules[0].Verbs[1] != "watch" {
		t.Fatalf("standby runtime Lease access is not read-only: %#v", role.Rules)
	}
}

func TestReconcileHARuntimeLeaseRBACCleansOwnedResourcesWhenFailoverIsDisabled(t *testing.T) {
	ctx := context.Background()
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	cluster.Spec.ServiceAccountName = ""
	reconciler := testHAReconciler(t, cluster)
	if _, err := reconciler.reconcileHARuntimeLeaseRBAC(ctx, cluster); err != nil {
		t.Fatalf("create HA runtime Lease RBAC: %v", err)
	}

	cluster.Spec.HighAvailability.AutomaticFailover.Enabled = false
	if serviceAccountName, err := reconciler.reconcileHARuntimeLeaseRBAC(ctx, cluster); err != nil {
		t.Fatalf("cleanup HA runtime Lease RBAC: %v", err)
	} else if serviceAccountName != "" {
		t.Fatalf("disabled runtime service account = %q", serviceAccountName)
	}
	if haRuntimeLeaseWatchdogEnabled(cluster) || haKubernetesLeaseRenewalEnabled(cluster) || len(haRuntimeLeaseEnv(cluster)) != 0 {
		t.Fatal("disabled automatic failover must disable renewal, watchdog env, and RBAC together")
	}

	roleName := cluster.Name + haRuntimeLeaseRBACSuffix
	for _, object := range []client.Object{
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: roleName, Namespace: cluster.Namespace}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: roleName, Namespace: cluster.Namespace}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: cluster.Name + "-ha-runtime", Namespace: cluster.Namespace}},
	} {
		if err := reconciler.Get(ctx, client.ObjectKeyFromObject(object), object); !apierrors.IsNotFound(err) {
			t.Fatalf("expected %T to be deleted, got %v", object, err)
		}
	}
}

func TestReconcileHARuntimeLeaseRBACPreservesCustomServiceAccount(t *testing.T) {
	ctx := context.Background()
	cluster := haClusterWithAutomaticKubernetesLeaseFailover()
	custom := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "database-runtime", Namespace: cluster.Namespace}}
	cluster.Spec.ServiceAccountName = custom.Name
	reconciler := testHAReconciler(t, cluster, custom)

	if serviceAccountName, err := reconciler.reconcileHARuntimeLeaseRBAC(ctx, cluster); err != nil {
		t.Fatalf("create custom-account HA runtime Lease RBAC: %v", err)
	} else if serviceAccountName != custom.Name {
		t.Fatalf("service account = %q", serviceAccountName)
	}
	cluster.Spec.HighAvailability.AutomaticFailover.Enabled = false
	if _, err := reconciler.reconcileHARuntimeLeaseRBAC(ctx, cluster); err != nil {
		t.Fatalf("cleanup custom-account HA runtime Lease RBAC: %v", err)
	}
	if err := reconciler.Get(ctx, client.ObjectKeyFromObject(custom), &corev1.ServiceAccount{}); err != nil {
		t.Fatalf("custom ServiceAccount was deleted: %v", err)
	}
}
