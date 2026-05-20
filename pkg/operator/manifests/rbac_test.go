package manifests

import "testing"

func TestClusterRoleAvoidsHighRiskBroadPermissions(t *testing.T) {
	role := ClusterRole()
	for _, rule := range role.Rules {
		for _, resource := range rule.Resources {
			if resource == "nodes/proxy" {
				t.Fatalf("ClusterRole should not grant nodes/proxy access")
			}
			if resource == "secrets" {
				t.Fatalf("ClusterRole should not grant secrets access, got verbs %v", rule.Verbs)
			}
		}
	}
}
