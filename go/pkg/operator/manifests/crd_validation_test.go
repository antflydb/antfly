package manifests

import (
	_ "embed"
	"encoding/json"
	"strings"
	"testing"

	extensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

const metadataReplicaTransitionRule = "(has(self.metadataNodes) ? (has(self.metadataNodes.replicas) ? self.metadataNodes.replicas : 3) : 3) == (has(oldSelf.metadataNodes) ? (has(oldSelf.metadataNodes.replicas) ? oldSelf.metadataNodes.replicas : 3) : 3)"

//go:embed overlays/kubernetes-1.25/antflycluster-metadata-replicas-cel.json
var metadataReplicaCELPatch []byte

func TestBaseCRDOmitsMetadataReplicaTransitionRule(t *testing.T) {
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

	for _, validation := range specSchema.XValidations {
		if strings.Contains(validation.Rule, "oldSelf.metadataNodes") {
			t.Fatalf("base CRD must remain compatible with Kubernetes 1.23-1.24; found CEL transition rule: %s", validation.Rule)
		}
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

func TestKubernetes125OverlayAddsMetadataReplicaTransitionRule(t *testing.T) {
	var patch []struct {
		Op    string                        `json:"op"`
		Path  string                        `json:"path"`
		Value []extensionsv1.ValidationRule `json:"value"`
	}
	if err := json.Unmarshal(metadataReplicaCELPatch, &patch); err != nil {
		t.Fatal(err)
	}
	if len(patch) != 1 {
		t.Fatalf("CEL overlay must contain exactly one focused operation, got %d", len(patch))
	}
	operation := patch[0]
	if operation.Op != "add" || operation.Path != "/spec/versions/0/schema/openAPIV3Schema/properties/spec/x-kubernetes-validations" {
		t.Fatalf("unexpected CEL overlay operation: %s %s", operation.Op, operation.Path)
	}
	if len(operation.Value) != 1 || operation.Value[0].Rule != metadataReplicaTransitionRule {
		t.Fatalf("CEL overlay is missing the metadata replica transition rule: %#v", operation.Value)
	}
	if !strings.Contains(operation.Value[0].Message, "metadata replica count is immutable") {
		t.Fatalf("CEL overlay has an unhelpful validation message: %q", operation.Value[0].Message)
	}
}
