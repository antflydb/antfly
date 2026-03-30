package manifests

import (
	"strings"
	"testing"
)

func TestOperatorInstallYAMLIncludesServerlessWebhookAndManagerConfig(t *testing.T) {
	yaml, err := OperatorInstallYAML(InstallOptions{})
	if err != nil {
		t.Fatalf("OperatorInstallYAML() error = %v", err)
	}

	for _, want := range []string{
		"kind: ValidatingWebhookConfiguration",
		"vantflyserverlessproject.kb.io",
		"/validate-antfly-io-v1-antflyserverlessproject",
		"name: ENABLE_WEBHOOKS",
		"value: \"true\"",
		"containerPort: 9443",
		"cert-manager.io/inject-ca-from: antfly-operator-namespace/antfly-operator-serving-cert",
	} {
		if !strings.Contains(yaml, want) {
			t.Fatalf("install manifest missing %q", want)
		}
	}
}

func TestOperatorInstallYAMLRespectsOptions(t *testing.T) {
	yaml, err := OperatorInstallYAML(InstallOptions{
		OperatorImage: "example.com/custom/operator:v1",
		IncludeCRDs:   false,
	})
	if err != nil {
		t.Fatalf("OperatorInstallYAML() error = %v", err)
	}

	if strings.Contains(yaml, "kind: CustomResourceDefinition") {
		t.Fatalf("expected CRDs to be omitted")
	}
	if !strings.Contains(yaml, "image: example.com/custom/operator:v1") {
		t.Fatalf("expected custom operator image to be rendered")
	}
}

func TestOperatorUninstallYAMLDefaultsToKeepingCRDs(t *testing.T) {
	yaml, err := OperatorUninstallYAML(UninstallOptions{})
	if err != nil {
		t.Fatalf("OperatorUninstallYAML() error = %v", err)
	}

	if strings.Contains(yaml, "kind: CustomResourceDefinition") {
		t.Fatalf("expected CRDs to be omitted by default")
	}
	for _, want := range []string{
		"kind: ValidatingWebhookConfiguration",
		"kind: Deployment",
		"kind: ClusterRoleBinding",
		"kind: Namespace",
	} {
		if !strings.Contains(yaml, want) {
			t.Fatalf("uninstall manifest missing %q", want)
		}
	}
}

func TestOperatorUninstallYAMLCanIncludeCRDs(t *testing.T) {
	yaml, err := OperatorUninstallYAML(UninstallOptions{IncludeCRDs: true})
	if err != nil {
		t.Fatalf("OperatorUninstallYAML() error = %v", err)
	}

	if !strings.Contains(yaml, "kind: CustomResourceDefinition") {
		t.Fatalf("expected CRDs to be included")
	}
}
