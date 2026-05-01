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

//go:embed crd/antfly.io_termitepools.yaml
var termitePoolCRDYAML []byte

//go:embed crd/antfly.io_termiteroutes.yaml
var termiteRouteCRDYAML []byte

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

// TermitePoolCRD returns the parsed CustomResourceDefinition for TermitePool.
func TermitePoolCRD() (*apiextv1.CustomResourceDefinition, error) {
	var crd apiextv1.CustomResourceDefinition
	if err := yaml.Unmarshal(termitePoolCRDYAML, &crd); err != nil {
		return nil, err
	}
	return &crd, nil
}

// TermitePoolCRDYAML returns the raw CRD YAML for TermitePool.
func TermitePoolCRDYAML() string {
	return string(termitePoolCRDYAML)
}

// TermiteRouteCRD returns the parsed CustomResourceDefinition for TermiteRoute.
func TermiteRouteCRD() (*apiextv1.CustomResourceDefinition, error) {
	var crd apiextv1.CustomResourceDefinition
	if err := yaml.Unmarshal(termiteRouteCRDYAML, &crd); err != nil {
		return nil, err
	}
	return &crd, nil
}

// TermiteRouteCRDYAML returns the raw CRD YAML for TermiteRoute.
func TermiteRouteCRDYAML() string {
	return string(termiteRouteCRDYAML)
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
	termitePoolCRD, err := TermitePoolCRD()
	if err != nil {
		return nil, err
	}
	termiteRouteCRD, err := TermiteRouteCRD()
	if err != nil {
		return nil, err
	}
	return []*apiextv1.CustomResourceDefinition{
		clusterCRD,
		backupCRD,
		restoreCRD,
		termitePoolCRD,
		termiteRouteCRD,
	}, nil
}

// AllCRDYAMLBytes returns raw YAML bytes for each CRD.
func AllCRDYAMLBytes() [][]byte {
	return [][]byte{
		antflyClusterCRDYAML,
		antflyBackupCRDYAML,
		antflyRestoreCRDYAML,
		termitePoolCRDYAML,
		termiteRouteCRDYAML,
	}
}

// AllCRDsYAML returns all CRD YAML files concatenated.
// This can be used directly with kubectl apply -f.
func AllCRDsYAML() string {
	return AntflyClusterCRDYAML() +
		"---\n" + AntflyBackupCRDYAML() +
		"---\n" + AntflyRestoreCRDYAML() +
		"---\n" + TermitePoolCRDYAML() +
		"---\n" + TermiteRouteCRDYAML()
}
