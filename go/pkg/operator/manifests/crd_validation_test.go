package manifests

import (
	"strings"
	"testing"
)

func TestMetadataReplicaTransitionRuleIsScopedToClusterSpec(t *testing.T) {
	crd, err := AntflyClusterCRD()
	if err != nil {
		t.Fatal(err)
	}
	if len(crd.Spec.Versions) == 0 || crd.Spec.Versions[0].Schema == nil {
		t.Fatal("AntflyCluster CRD is missing its OpenAPI schema")
	}

	root := crd.Spec.Versions[0].Schema.OpenAPIV3Schema
	specSchema, ok := root.Properties["spec"]
	if !ok {
		t.Fatal("AntflyCluster CRD is missing spec schema")
	}

	var foundParentRule bool
	for _, validation := range specSchema.XValidations {
		if strings.Contains(validation.Rule, "oldSelf.metadataNodes") {
			foundParentRule = true
			if !strings.Contains(validation.Rule, "has(oldSelf.metadataNodes)") || !strings.Contains(validation.Rule, ": 3") {
				t.Fatalf("metadata transition rule must handle an omitted metadataNodes object as the default replica count: %s", validation.Rule)
			}
		}
	}
	if !foundParentRule {
		t.Fatal("spec schema is missing the metadata replica transition rule")
	}

	metadataSchema, ok := specSchema.Properties["metadataNodes"]
	if !ok {
		t.Fatal("AntflyCluster CRD is missing spec.metadataNodes schema")
	}
	for _, validation := range metadataSchema.XValidations {
		if strings.Contains(validation.Rule, "oldSelf.replicas") {
			t.Fatalf("metadata replica transition rule must not be scoped to optional spec.metadataNodes: %s", validation.Rule)
		}
	}
}
