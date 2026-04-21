// Copyright 2025 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	// OperatorNamespace is the default namespace for the Termite operator.
	OperatorNamespace = "termite-operator-namespace"

	// ServiceAccountName is the name of the operator's ServiceAccount.
	ServiceAccountName = "termite-operator-service-account"

	// ClusterRoleName is the name of the operator's ClusterRole.
	ClusterRoleName = "termite-operator-cluster-role"

	// ClusterRoleBindingName is the name of the operator's ClusterRoleBinding.
	ClusterRoleBindingName = "termite-operator-cluster-role-binding"

	// LeaderElectionRoleName is the name of the leader election Role.
	LeaderElectionRoleName = "termite-operator-leader-election-role"

	// LeaderElectionRoleBindingName is the name of the leader election RoleBinding.
	LeaderElectionRoleBindingName = "termite-operator-leader-election-role-binding"

	// ProxyServiceAccountName is the name of the proxy's ServiceAccount.
	ProxyServiceAccountName = "termite-proxy-service-account"

	// ProxyClusterRoleName is the name of the proxy's ClusterRole.
	ProxyClusterRoleName = "termite-proxy-cluster-role"

	// ProxyClusterRoleBindingName is the name of the proxy's ClusterRoleBinding.
	ProxyClusterRoleBindingName = "termite-proxy-cluster-role-binding"
)

// Embed generated RBAC YAML files for raw access
//
//go:embed rbac/role.yaml
var clusterRoleYAML []byte

// Namespace returns the Namespace resource for the Termite operator.
func Namespace() *corev1.Namespace {
	return &corev1.Namespace{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Namespace",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: OperatorNamespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "termite-operator",
				"app.kubernetes.io/component":  "namespace",
				"app.kubernetes.io/managed-by": "termite-operator",
			},
		},
	}
}

// ServiceAccount returns the ServiceAccount for the Termite operator.
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
				"app.kubernetes.io/name":       "termite-operator",
				"app.kubernetes.io/component":  "rbac",
				"app.kubernetes.io/managed-by": "termite-operator",
			},
		},
	}
}

// ClusterRole returns the ClusterRole for the Termite operator.
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
				"app.kubernetes.io/name":       "termite-operator",
				"app.kubernetes.io/component":  "rbac",
				"app.kubernetes.io/managed-by": "termite-operator",
			},
		},
		Rules: []rbacv1.PolicyRule{
			// TermitePool CRD management
			{
				APIGroups: []string{"antfly.io"},
				Resources: []string{"termitepools", "termitepools/status", "termitepools/finalizers"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			// TermiteRoute CRD management
			{
				APIGroups: []string{"antfly.io"},
				Resources: []string{"termiteroutes", "termiteroutes/status", "termiteroutes/finalizers"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			// StatefulSet management (created by operator for TermitePools)
			{
				APIGroups: []string{"apps"},
				Resources: []string{"statefulsets"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			// Service management
			{
				APIGroups: []string{""},
				Resources: []string{"services"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			// ConfigMap management (for model configs)
			{
				APIGroups: []string{""},
				Resources: []string{"configmaps"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			// PodDisruptionBudget management
			{
				APIGroups: []string{"policy"},
				Resources: []string{"poddisruptionbudgets"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			// HorizontalPodAutoscaler management
			{
				APIGroups: []string{"autoscaling"},
				Resources: []string{"horizontalpodautoscalers"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			// Pod watching (for status)
			{
				APIGroups: []string{""},
				Resources: []string{"pods"},
				Verbs:     []string{"get", "list", "watch"},
			},
			// Events (for recording)
			{
				APIGroups: []string{""},
				Resources: []string{"events"},
				Verbs:     []string{"create", "patch"},
			},
		},
	}
}

// ClusterRoleBinding returns the ClusterRoleBinding for the Termite operator.
func ClusterRoleBinding() *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "rbac.authorization.k8s.io/v1",
			Kind:       "ClusterRoleBinding",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: ClusterRoleBindingName,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "termite-operator",
				"app.kubernetes.io/component":  "rbac",
				"app.kubernetes.io/managed-by": "termite-operator",
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
				"app.kubernetes.io/name":       "termite-operator",
				"app.kubernetes.io/component":  "rbac",
				"app.kubernetes.io/managed-by": "termite-operator",
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
				"app.kubernetes.io/name":       "termite-operator",
				"app.kubernetes.io/component":  "rbac",
				"app.kubernetes.io/managed-by": "termite-operator",
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

// AllRBACResources returns all RBAC resources needed for the Termite operator.
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

// ProxyServiceAccount returns the ServiceAccount for the Termite proxy.
func ProxyServiceAccount() *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ServiceAccount",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      ProxyServiceAccountName,
			Namespace: OperatorNamespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "termite-proxy",
				"app.kubernetes.io/component":  "rbac",
				"app.kubernetes.io/part-of":    "termite-operator",
				"app.kubernetes.io/managed-by": "termite-operator",
			},
		},
	}
}

// ProxyClusterRole returns the ClusterRole for the Termite proxy.
// The proxy needs cluster-wide access to watch pods/endpoints across all namespaces
// where TermitePools may be deployed.
func ProxyClusterRole() *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "rbac.authorization.k8s.io/v1",
			Kind:       "ClusterRole",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: ProxyClusterRoleName,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "termite-proxy",
				"app.kubernetes.io/component":  "rbac",
				"app.kubernetes.io/part-of":    "termite-operator",
				"app.kubernetes.io/managed-by": "termite-operator",
			},
		},
		Rules: []rbacv1.PolicyRule{
			// Pod and endpoint watching for service discovery (cluster-wide)
			{
				APIGroups: []string{""},
				Resources: []string{"pods", "endpoints"},
				Verbs:     []string{"get", "list", "watch"},
			},
			// EndpointSlices (preferred over endpoints)
			{
				APIGroups: []string{"discovery.k8s.io"},
				Resources: []string{"endpointslices"},
				Verbs:     []string{"get", "list", "watch"},
			},
			// TermitePool watching for routing configuration (cluster-wide)
			{
				APIGroups: []string{"antfly.io"},
				Resources: []string{"termitepools"},
				Verbs:     []string{"get", "list", "watch"},
			},
			// TermiteRoute watching for routing rules (cluster-wide)
			{
				APIGroups: []string{"antfly.io"},
				Resources: []string{"termiteroutes"},
				Verbs:     []string{"get", "list", "watch"},
			},
		},
	}
}

// ProxyClusterRoleBinding returns the ClusterRoleBinding for the Termite proxy.
func ProxyClusterRoleBinding() *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "rbac.authorization.k8s.io/v1",
			Kind:       "ClusterRoleBinding",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: ProxyClusterRoleBindingName,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "termite-proxy",
				"app.kubernetes.io/component":  "rbac",
				"app.kubernetes.io/part-of":    "termite-operator",
				"app.kubernetes.io/managed-by": "termite-operator",
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     ProxyClusterRoleName,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      ProxyServiceAccountName,
				Namespace: OperatorNamespace,
			},
		},
	}
}

// AllProxyRBACResources returns all RBAC resources needed for the Termite proxy.
func AllProxyRBACResources() []any {
	return []any{
		ProxyServiceAccount(),
		ProxyClusterRole(),
		ProxyClusterRoleBinding(),
	}
}
