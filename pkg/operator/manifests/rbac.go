package manifests

import (
	_ "embed"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

// Operator identity constants
const (
	// OperatorNamespace is the default namespace for the Antfly operator.
	OperatorNamespace = "antfly-operator-namespace"

	// ServiceAccountName is the name of the operator's ServiceAccount.
	// CRITICAL: This must be EXACTLY this value - mismatches cause RBAC errors.
	ServiceAccountName = "antfly-operator-service-account"

	// ClusterRoleName is the name of the operator's ClusterRole.
	ClusterRoleName = "antfly-operator-cluster-role"

	// ClusterRoleBindingName is the name of the operator's ClusterRoleBinding.
	ClusterRoleBindingName = "antfly-operator-cluster-role-binding"

	// LeaderElectionRoleName is the name of the leader election Role.
	LeaderElectionRoleName = "antfly-operator-leader-election-role"

	// LeaderElectionRoleBindingName is the name of the leader election RoleBinding.
	LeaderElectionRoleBindingName = "antfly-operator-leader-election-role-binding"
)

// Embed generated RBAC YAML files for raw access
//
//go:embed rbac/role.yaml
var clusterRoleYAML []byte

// Namespace returns the Namespace resource for the Antfly operator.
func Namespace() *corev1.Namespace {
	return &corev1.Namespace{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Namespace",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: OperatorNamespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "antfly-operator",
				"app.kubernetes.io/component":  "namespace",
				"app.kubernetes.io/managed-by": "antfly-operator",
			},
		},
	}
}

// ServiceAccount returns the ServiceAccount for the Antfly operator.
// CRITICAL: The name must match exactly what the deployment uses.
func ServiceAccount() *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ServiceAccount",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      ServiceAccountName,
			Namespace: OperatorNamespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "antfly-operator",
				"app.kubernetes.io/component":  "rbac",
				"app.kubernetes.io/managed-by": "antfly-operator",
			},
		},
	}
}

// ClusterRole returns the ClusterRole for the Antfly operator.
// This is generated from kubebuilder RBAC annotations in the controller.
func ClusterRole() *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "rbac.authorization.k8s.io/v1",
			Kind:       "ClusterRole",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: ClusterRoleName,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "antfly-operator",
				"app.kubernetes.io/component":  "rbac",
				"app.kubernetes.io/managed-by": "antfly-operator",
			},
		},
		Rules: []rbacv1.PolicyRule{
			// Core resources: ConfigMaps, PVCs, Services
			{
				APIGroups: []string{""},
				Resources: []string{"configmaps", "persistentvolumeclaims", "services"},
				Verbs:     []string{"create", "delete", "get", "list", "patch", "update", "watch"},
			},
			// Events (for recording)
			{
				APIGroups: []string{""},
				Resources: []string{"events"},
				Verbs:     []string{"create", "patch"},
			},
			// AntflyBackup CRD management
			{
				APIGroups: []string{"antfly.io"},
				Resources: []string{"antflybackups"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			{
				APIGroups: []string{"antfly.io"},
				Resources: []string{"antflybackups/status"},
				Verbs:     []string{"get", "update", "patch"},
			},
			{
				APIGroups: []string{"antfly.io"},
				Resources: []string{"antflybackups/finalizers"},
				Verbs:     []string{"update"},
			},
			// AntflyRestore CRD management
			{
				APIGroups: []string{"antfly.io"},
				Resources: []string{"antflyrestores"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			{
				APIGroups: []string{"antfly.io"},
				Resources: []string{"antflyrestores/status"},
				Verbs:     []string{"get", "update", "patch"},
			},
			{
				APIGroups: []string{"antfly.io"},
				Resources: []string{"antflyrestores/finalizers"},
				Verbs:     []string{"update"},
			},
			// Job management (for backup/restore operations)
			{
				APIGroups: []string{"batch"},
				Resources: []string{"jobs"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			// CronJob management (for scheduled backups)
			{
				APIGroups: []string{"batch"},
				Resources: []string{"cronjobs"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			// StatefulSet management
			// Pods and Secrets (read-only for status/backup)
			{
				APIGroups: []string{""},
				Resources: []string{"pods", "secrets"},
				Verbs:     []string{"get", "list", "watch"},
			},
			// Antfly CRDs: clusters, backups, restores
			{
				APIGroups: []string{"antfly.io"},
				Resources: []string{"antflybackups", "antflyclusters", "antflyrestores"},
				Verbs:     []string{"create", "delete", "get", "list", "patch", "update", "watch"},
			},
			// Antfly CRD finalizers
			{
				APIGroups: []string{"antfly.io"},
				Resources: []string{"antflybackups/finalizers", "antflyclusters/finalizers", "antflyrestores/finalizers"},
				Verbs:     []string{"update"},
			},
			// Antfly CRD status
			{
				APIGroups: []string{"antfly.io"},
				Resources: []string{"antflybackups/status", "antflyclusters/status", "antflyrestores/status"},
				Verbs:     []string{"get", "patch", "update"},
			},
			// Termite CRDs: pools and routes
			{
				APIGroups: []string{"antfly.io"},
				Resources: []string{"termitepools", "termiteroutes"},
				Verbs:     []string{"create", "delete", "get", "list", "patch", "update", "watch"},
			},
			// Termite CRD finalizers
			{
				APIGroups: []string{"antfly.io"},
				Resources: []string{"termitepools/finalizers", "termiteroutes/finalizers"},
				Verbs:     []string{"update"},
			},
			// Termite CRD status
			{
				APIGroups: []string{"antfly.io"},
				Resources: []string{"termitepools/status", "termiteroutes/status"},
				Verbs:     []string{"get", "patch", "update"},
			},
			// StatefulSet management
			{
				APIGroups: []string{"apps"},
				Resources: []string{"statefulsets"},
				Verbs:     []string{"create", "delete", "get", "list", "patch", "update", "watch"},
			},
			// Batch API: CronJobs and Jobs (for backup/restore)
			{
				APIGroups: []string{"batch"},
				Resources: []string{"cronjobs", "jobs"},
				Verbs:     []string{"create", "delete", "get", "list", "patch", "update", "watch"},
			},
			// Metrics API (for autoscaling)
			{
				APIGroups: []string{"metrics.k8s.io"},
				Resources: []string{"pods"},
				Verbs:     []string{"get", "list"},
			},
			// PodDisruptionBudget management (required for GKE Autopilot support)
			{
				APIGroups: []string{"policy"},
				Resources: []string{"poddisruptionbudgets"},
				Verbs:     []string{"create", "delete", "get", "list", "patch", "update", "watch"},
			},
			// HorizontalPodAutoscaler management for Termite pools
			{
				APIGroups: []string{"autoscaling"},
				Resources: []string{"horizontalpodautoscalers"},
				Verbs:     []string{"create", "delete", "get", "list", "patch", "update", "watch"},
			},
			// CRD management (for self-managing CRDs at startup)
			{
				APIGroups: []string{"apiextensions.k8s.io"},
				Resources: []string{"customresourcedefinitions"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch"},
			},
		},
	}
}

// ClusterRoleBinding returns the ClusterRoleBinding for the Antfly operator.
func ClusterRoleBinding() *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "rbac.authorization.k8s.io/v1",
			Kind:       "ClusterRoleBinding",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: ClusterRoleBindingName,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "antfly-operator",
				"app.kubernetes.io/component":  "rbac",
				"app.kubernetes.io/managed-by": "antfly-operator",
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     ClusterRoleName,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      ServiceAccountName,
				Namespace: OperatorNamespace,
			},
		},
	}
}

// LeaderElectionRole returns the Role for leader election.
func LeaderElectionRole() *rbacv1.Role {
	return &rbacv1.Role{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "rbac.authorization.k8s.io/v1",
			Kind:       "Role",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      LeaderElectionRoleName,
			Namespace: OperatorNamespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "antfly-operator",
				"app.kubernetes.io/component":  "rbac",
				"app.kubernetes.io/managed-by": "antfly-operator",
			},
		},
		Rules: []rbacv1.PolicyRule{
			// Leader election via leases
			{
				APIGroups: []string{"coordination.k8s.io"},
				Resources: []string{"leases"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			// Leader election via configmaps (legacy)
			{
				APIGroups: []string{""},
				Resources: []string{"configmaps"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			// Events for leader election
			{
				APIGroups: []string{""},
				Resources: []string{"events"},
				Verbs:     []string{"create", "patch"},
			},
		},
	}
}

// LeaderElectionRoleBinding returns the RoleBinding for leader election.
func LeaderElectionRoleBinding() *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "rbac.authorization.k8s.io/v1",
			Kind:       "RoleBinding",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      LeaderElectionRoleBindingName,
			Namespace: OperatorNamespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "antfly-operator",
				"app.kubernetes.io/component":  "rbac",
				"app.kubernetes.io/managed-by": "antfly-operator",
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     LeaderElectionRoleName,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      ServiceAccountName,
				Namespace: OperatorNamespace,
			},
		},
	}
}

// ClusterRoleFromYAML returns the ClusterRole parsed from the generated YAML.
// This ensures the Go types stay in sync with kubebuilder-generated RBAC.
func ClusterRoleFromYAML() (*rbacv1.ClusterRole, error) {
	var role rbacv1.ClusterRole
	if err := yaml.Unmarshal(clusterRoleYAML, &role); err != nil {
		return nil, err
	}
	return &role, nil
}

// ClusterRoleYAML returns the raw generated ClusterRole YAML.
func ClusterRoleYAML() string {
	return string(clusterRoleYAML)
}

// AllRBACResources returns all RBAC resources needed for the Antfly operator.
// Resources are returned in the order they should be applied.
func AllRBACResources() []any {
	return []any{
		Namespace(),
		ServiceAccount(),
		ClusterRole(),
		ClusterRoleBinding(),
		LeaderElectionRole(),
		LeaderElectionRoleBinding(),
	}
}

// AllClusterScopedRBAC returns cluster-scoped RBAC resources.
func AllClusterScopedRBAC() []any {
	return []any{
		ClusterRole(),
		ClusterRoleBinding(),
	}
}

// AllNamespacedRBAC returns namespace-scoped RBAC resources.
func AllNamespacedRBAC() []any {
	return []any{
		Namespace(),
		ServiceAccount(),
		LeaderElectionRole(),
		LeaderElectionRoleBinding(),
	}
}
