package manifests

import (
	_ "embed"

	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

//go:embed crd/antfly.io_antflyclusters.yaml
var antflyClusterCRDYAML []byte

//go:embed crd/antfly.io_antflybackups.yaml
var antflyBackupCRDYAML []byte

//go:embed crd/antfly.io_antflyrestores.yaml
var antflyRestoreCRDYAML []byte

//go:embed crd/antfly.io_inferencepools.yaml
var inferencePoolCRDYAML []byte

//go:embed crd/antfly.io_externalinferencepools.yaml
var externalInferencePoolCRDYAML []byte

//go:embed crd/antfly.io_inferenceproxies.yaml
var inferenceRouteCRDYAML []byte

// AntflyClusterCRD returns the parsed CustomResourceDefinition for AntflyCluster.
func AntflyClusterCRD() (*apiextv1.CustomResourceDefinition, error) {
	var crd apiextv1.CustomResourceDefinition
	if err := yaml.Unmarshal(antflyClusterCRDYAML, &crd); err != nil {
		return nil, err
	}
	return &crd, nil
}

// AntflyClusterCRDYAML returns the raw CRD YAML for AntflyCluster.
// This can be used directly with kubectl apply or similar tools.
func AntflyClusterCRDYAML() string {
	return string(antflyClusterCRDYAML)
}

// AntflyBackupCRD returns the parsed CustomResourceDefinition for AntflyBackup.
func AntflyBackupCRD() (*apiextv1.CustomResourceDefinition, error) {
	var crd apiextv1.CustomResourceDefinition
	if err := yaml.Unmarshal(antflyBackupCRDYAML, &crd); err != nil {
		return nil, err
	}
	return &crd, nil
}

// AntflyBackupCRDYAML returns the raw CRD YAML for AntflyBackup.
func AntflyBackupCRDYAML() string {
	return string(antflyBackupCRDYAML)
}

// AntflyRestoreCRD returns the parsed CustomResourceDefinition for AntflyRestore.
func AntflyRestoreCRD() (*apiextv1.CustomResourceDefinition, error) {
	var crd apiextv1.CustomResourceDefinition
	if err := yaml.Unmarshal(antflyRestoreCRDYAML, &crd); err != nil {
		return nil, err
	}
	return &crd, nil
}

// AntflyRestoreCRDYAML returns the raw CRD YAML for AntflyRestore.
func AntflyRestoreCRDYAML() string {
	return string(antflyRestoreCRDYAML)
}

// InferencePoolCRD returns the parsed CustomResourceDefinition for InferencePool.
func InferencePoolCRD() (*apiextv1.CustomResourceDefinition, error) {
	var crd apiextv1.CustomResourceDefinition
	if err := yaml.Unmarshal(inferencePoolCRDYAML, &crd); err != nil {
		return nil, err
	}
	return &crd, nil
}

// InferencePoolCRDYAML returns the raw CRD YAML for InferencePool.
func InferencePoolCRDYAML() string {
	return string(inferencePoolCRDYAML)
}

// ExternalInferencePoolCRD returns the parsed CustomResourceDefinition for ExternalInferencePool.
func ExternalInferencePoolCRD() (*apiextv1.CustomResourceDefinition, error) {
	var crd apiextv1.CustomResourceDefinition
	if err := yaml.Unmarshal(externalInferencePoolCRDYAML, &crd); err != nil {
		return nil, err
	}
	return &crd, nil
}

// ExternalInferencePoolCRDYAML returns the raw CRD YAML for ExternalInferencePool.
func ExternalInferencePoolCRDYAML() string {
	return string(externalInferencePoolCRDYAML)
}

// InferenceProxyCRD returns the parsed CustomResourceDefinition for InferenceProxy.
func InferenceProxyCRD() (*apiextv1.CustomResourceDefinition, error) {
	var crd apiextv1.CustomResourceDefinition
	if err := yaml.Unmarshal(inferenceRouteCRDYAML, &crd); err != nil {
		return nil, err
	}
	return &crd, nil
}

// InferenceProxyCRDYAML returns the raw CRD YAML for InferenceProxy.
func InferenceProxyCRDYAML() string {
	return string(inferenceRouteCRDYAML)
}

// AllCRDs returns all CRDs needed for the omni Antfly operator as parsed objects.
func AllCRDs() ([]*apiextv1.CustomResourceDefinition, error) {
	clusterCRD, err := AntflyClusterCRD()
	if err != nil {
		return nil, err
	}
	backupCRD, err := AntflyBackupCRD()
	if err != nil {
		return nil, err
	}
	restoreCRD, err := AntflyRestoreCRD()
	if err != nil {
		return nil, err
	}
	inferencePoolCRD, err := InferencePoolCRD()
	if err != nil {
		return nil, err
	}
	externalInferencePoolCRD, err := ExternalInferencePoolCRD()
	if err != nil {
		return nil, err
	}
	inferenceRouteCRD, err := InferenceProxyCRD()
	if err != nil {
		return nil, err
	}
	return []*apiextv1.CustomResourceDefinition{
		clusterCRD,
		backupCRD,
		restoreCRD,
		inferencePoolCRD,
		externalInferencePoolCRD,
		inferenceRouteCRD,
	}, nil
}

// AllCRDYAMLBytes returns raw YAML bytes for each CRD.
func AllCRDYAMLBytes() [][]byte {
	return [][]byte{
		antflyClusterCRDYAML,
		antflyBackupCRDYAML,
		antflyRestoreCRDYAML,
		inferencePoolCRDYAML,
		externalInferencePoolCRDYAML,
		inferenceRouteCRDYAML,
	}
}

// AllCRDsYAML returns all CRD YAML files concatenated.
// This can be used directly with kubectl apply -f.
func AllCRDsYAML() string {
	return AntflyClusterCRDYAML() +
		"---\n" + AntflyBackupCRDYAML() +
		"---\n" + AntflyRestoreCRDYAML() +
		"---\n" + InferencePoolCRDYAML() +
		"---\n" + ExternalInferencePoolCRDYAML() +
		"---\n" + InferenceProxyCRDYAML()
}
