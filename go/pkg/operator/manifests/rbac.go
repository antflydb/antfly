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

	// StorageAutoGrowClusterRoleName is the name of the optional ClusterRole
	// that grants kubelet stats access for storage auto-grow.
	StorageAutoGrowClusterRoleName = "antfly-operator-storage-auto-grow-cluster-role"

	// StorageAutoGrowClusterRoleBindingName is the name of the optional
	// ClusterRoleBinding for storage auto-grow.
	StorageAutoGrowClusterRoleBindingName = "antfly-operator-storage-auto-grow-cluster-role-binding"

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
// This is parsed from the kubebuilder-generated embedded role.yaml so there is
// one canonical ClusterRole definition.
func ClusterRole() *rbacv1.ClusterRole {
	role, err := ClusterRoleFromYAML()
	if err != nil {
		panic(err)
	}
	return role
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

// StorageAutoGrowClusterRole returns the optional ClusterRole needed only when
// spec.storage.storageAutoGrow.enabled is true.
func StorageAutoGrowClusterRole() *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "rbac.authorization.k8s.io/v1",
			Kind:       "ClusterRole",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: StorageAutoGrowClusterRoleName,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "antfly-operator",
				"app.kubernetes.io/component":  "rbac",
				"app.kubernetes.io/managed-by": "antfly-operator",
				"antfly.io/rbac-purpose":       "storage-auto-grow",
			},
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"nodes/proxy"},
				Verbs:     []string{"get"},
			},
		},
	}
}

// StorageAutoGrowClusterRoleBinding returns the optional ClusterRoleBinding
// needed only when spec.storage.storageAutoGrow.enabled is true.
func StorageAutoGrowClusterRoleBinding() *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "rbac.authorization.k8s.io/v1",
			Kind:       "ClusterRoleBinding",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: StorageAutoGrowClusterRoleBindingName,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "antfly-operator",
				"app.kubernetes.io/component":  "rbac",
				"app.kubernetes.io/managed-by": "antfly-operator",
				"antfly.io/rbac-purpose":       "storage-auto-grow",
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     StorageAutoGrowClusterRoleName,
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

// StorageAutoGrowRBACResources returns the optional RBAC resources needed when
// spec.storage.storageAutoGrow.enabled is true.
func StorageAutoGrowRBACResources() []any {
	return []any{
		StorageAutoGrowClusterRole(),
		StorageAutoGrowClusterRoleBinding(),
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
