package manifests

import (
	"os"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

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

func TestClusterRoleGrantsStructuredEventPermissions(t *testing.T) {
	role := ClusterRole()
	for _, rule := range role.Rules {
		if !containsString(rule.APIGroups, "events.k8s.io") || !containsString(rule.Resources, "events") {
			continue
		}
		for _, verb := range []string{"create", "patch"} {
			if !containsString(rule.Verbs, verb) {
				t.Fatalf("ClusterRole events.k8s.io events rule missing verb %q: %#v", verb, rule.Verbs)
			}
		}
		return
	}

	t.Fatal("ClusterRole must grant structured Event permissions used by the event recorder")
}

func TestClusterRoleGrantsLeasePermissionsForHAFencing(t *testing.T) {
	role := ClusterRole()
	requiredVerbs := []string{"get", "list", "watch", "create", "update", "patch", "delete"}

	for _, rule := range role.Rules {
		if !containsString(rule.APIGroups, "coordination.k8s.io") || !containsString(rule.Resources, "leases") {
			continue
		}
		for _, verb := range requiredVerbs {
			if !containsString(rule.Verbs, verb) {
				t.Fatalf("ClusterRole leases rule missing verb %q: %#v", verb, rule.Verbs)
			}
		}
		return
	}

	t.Fatal("ClusterRole must grant coordination.k8s.io leases permissions for HA KubernetesLease fencing")
}

func TestClusterRoleGrantsHARuntimeRBACReconciliationPermissions(t *testing.T) {
	role := ClusterRole()
	requiredVerbs := []string{"create", "get", "list", "update", "watch"}
	required := map[string][]string{
		"":                          {"serviceaccounts"},
		"rbac.authorization.k8s.io": {"roles", "rolebindings"},
	}

	for apiGroup, resources := range required {
		for _, resource := range resources {
			var matchingRuleFound bool
			for _, rule := range role.Rules {
				if !containsString(rule.APIGroups, apiGroup) || !containsString(rule.Resources, resource) {
					continue
				}
				matchingRuleFound = true
				for _, verb := range requiredVerbs {
					if !containsString(rule.Verbs, verb) {
						t.Fatalf("ClusterRole %s rule missing verb %q: %#v", resource, verb, rule.Verbs)
					}
				}
				break
			}
			if !matchingRuleFound {
				t.Fatalf("ClusterRole must grant %s permissions needed to reconcile HA runtime RBAC", resource)
			}
		}
	}
}

func TestHAAdminTokenSecretInjectionManifestsAreAligned(t *testing.T) {
	rawDeployment, err := os.ReadFile("../config/manager/deployment.yaml")
	if err != nil {
		t.Fatalf("read deployment manifest: %v", err)
	}
	var deployment appsv1.Deployment
	if err := yaml.Unmarshal(rawDeployment, &deployment); err != nil {
		t.Fatalf("parse deployment manifest: %v", err)
	}
	if deployment.Namespace != OperatorNamespace {
		t.Fatalf("deployment namespace = %q, want %q", deployment.Namespace, OperatorNamespace)
	}

	tokenEnv, ok := findContainerEnv(deployment.Spec.Template.Spec.Containers, "antfly-operator-manager", "ANTFLY_HA_ADMIN_TOKEN")
	if !ok {
		t.Fatal("deployment must inject ANTFLY_HA_ADMIN_TOKEN into antfly-operator-manager")
	}
	if tokenEnv.ValueFrom == nil || tokenEnv.ValueFrom.SecretKeyRef == nil {
		t.Fatalf("ANTFLY_HA_ADMIN_TOKEN must come from a Secret key ref: %#v", tokenEnv)
	}
	secretRef := tokenEnv.ValueFrom.SecretKeyRef
	if secretRef.Name != "antfly-ha-admin-token" {
		t.Fatalf("ANTFLY_HA_ADMIN_TOKEN Secret name = %q, want antfly-ha-admin-token", secretRef.Name)
	}
	if secretRef.Key != "token" {
		t.Fatalf("ANTFLY_HA_ADMIN_TOKEN Secret key = %q, want token", secretRef.Key)
	}
	if secretRef.Optional == nil || !*secretRef.Optional {
		t.Fatal("ANTFLY_HA_ADMIN_TOKEN Secret key ref must be optional for deployments without HA admin automation")
	}

	rawExample, err := os.ReadFile("../examples/ha-hot-standby-standalone.yaml")
	if err != nil {
		t.Fatalf("read HA example manifest: %v", err)
	}
	firstDoc := strings.SplitN(string(rawExample), "\n---\n", 2)[0]
	var secret corev1.Secret
	if err := yaml.Unmarshal([]byte(firstDoc), &secret); err != nil {
		t.Fatalf("parse HA example Secret: %v", err)
	}
	if secret.Name != secretRef.Name {
		t.Fatalf("example Secret name = %q, want %q", secret.Name, secretRef.Name)
	}
	if secret.Namespace != deployment.Namespace {
		t.Fatalf("example Secret namespace = %q, want deployment namespace %q", secret.Namespace, deployment.Namespace)
	}
	if _, ok := secret.StringData[secretRef.Key]; !ok {
		t.Fatalf("example Secret stringData missing key %q", secretRef.Key)
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

func findContainerEnv(containers []corev1.Container, containerName string, envName string) (corev1.EnvVar, bool) {
	for _, container := range containers {
		if container.Name != containerName {
			continue
		}
		for _, env := range container.Env {
			if env.Name == envName {
				return env, true
			}
		}
	}
	return corev1.EnvVar{}, false
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
