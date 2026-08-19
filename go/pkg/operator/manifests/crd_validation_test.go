package manifests

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	extensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	sigyaml "sigs.k8s.io/yaml"
)

const metadataReplicaTransitionRule = "(has(self.metadataNodes) ? (has(self.metadataNodes.replicas) ? self.metadataNodes.replicas : 3) : 3) == (has(oldSelf.metadataNodes) ? (has(oldSelf.metadataNodes.replicas) ? oldSelf.metadataNodes.replicas : 3) : 3)"

func TestCRDDirectoryContainsOnlyCRDs(t *testing.T) {
	entries, err := os.ReadDir("crd")
	if err != nil {
		t.Fatal(err)
	}

	crdCount := 0
	for _, entry := range entries {
		if entry.IsDir() || (filepath.Ext(entry.Name()) != ".yaml" && filepath.Ext(entry.Name()) != ".yml") {
			continue
		}
		crdCount++

		data, err := os.ReadFile(filepath.Join("crd", entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		var typeMeta struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
		}
		if err := sigyaml.Unmarshal(data, &typeMeta); err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		if typeMeta.APIVersion != "apiextensions.k8s.io/v1" || typeMeta.Kind != "CustomResourceDefinition" {
			t.Fatalf("%s is not a v1 CustomResourceDefinition: apiVersion=%q kind=%q", entry.Name(), typeMeta.APIVersion, typeMeta.Kind)
		}
	}
	if crdCount != len(AllCRDYAMLBytes()) {
		t.Fatalf("crd directory contains %d YAML resources, embedded bundle contains %d", crdCount, len(AllCRDYAMLBytes()))
	}
}

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
	metadataReplicaCELPatch, err := os.ReadFile(filepath.Join("..", "kustomize", "overlays", "kubernetes-1.25", "antflycluster-metadata-replicas-cel.json"))
	if err != nil {
		t.Fatal(err)
	}
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
