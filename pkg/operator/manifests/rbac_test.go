package manifests

import "testing"

func TestAllRBACResourcesAvoidsStorageAutoGrowRBAC(t *testing.T) {
	for _, resource := range AllRBACResources() {
		switch typed := resource.(type) {
		case interface{ GetName() string }:
			switch typed.GetName() {
			case StorageAutoGrowClusterRoleName, StorageAutoGrowClusterRoleBindingName:
				t.Fatalf("AllRBACResources should not include optional storage auto-grow RBAC resource %q", typed.GetName())
			}
		}
	}
}

func TestClusterRoleAvoidsOptionalHighRiskPermissions(t *testing.T) {
	role := ClusterRole()
	for _, rule := range role.Rules {
		for _, resource := range rule.Resources {
			if resource == "secrets" {
				t.Fatalf("ClusterRole should not grant secrets access, got verbs %v", rule.Verbs)
			}
			if resource == "nodes/proxy" {
				t.Fatalf("ClusterRole should not grant nodes/proxy by default, got verbs %v", rule.Verbs)
			}
		}
	}
}

func TestStorageAutoGrowRBACGrantsNodeProxyOnly(t *testing.T) {
	role := StorageAutoGrowClusterRole()
	if role.Name != StorageAutoGrowClusterRoleName {
		t.Fatalf("StorageAutoGrowClusterRole name = %q, want %q", role.Name, StorageAutoGrowClusterRoleName)
	}
	if len(role.Rules) != 1 {
		t.Fatalf("StorageAutoGrowClusterRole should have exactly one rule, got %d", len(role.Rules))
	}

	rule := role.Rules[0]
	if len(rule.APIGroups) != 1 || rule.APIGroups[0] != "" {
		t.Fatalf("StorageAutoGrowClusterRole APIGroups = %v, want core API group", rule.APIGroups)
	}
	if len(rule.Resources) != 1 || rule.Resources[0] != "nodes/proxy" {
		t.Fatalf("StorageAutoGrowClusterRole resources = %v, want [nodes/proxy]", rule.Resources)
	}
	if len(rule.Verbs) != 1 || rule.Verbs[0] != "get" {
		t.Fatalf("StorageAutoGrowClusterRole verbs = %v, want [get]", rule.Verbs)
	}

	binding := StorageAutoGrowClusterRoleBinding()
	if binding.RoleRef.Name != StorageAutoGrowClusterRoleName {
		t.Fatalf("StorageAutoGrowClusterRoleBinding roleRef = %q, want %q", binding.RoleRef.Name, StorageAutoGrowClusterRoleName)
	}
	if len(binding.Subjects) != 1 {
		t.Fatalf("StorageAutoGrowClusterRoleBinding should have exactly one subject, got %d", len(binding.Subjects))
	}
	subject := binding.Subjects[0]
	if subject.Kind != "ServiceAccount" || subject.Name != ServiceAccountName || subject.Namespace != OperatorNamespace {
		t.Fatalf("StorageAutoGrowClusterRoleBinding subject = %#v", subject)
	}
}
