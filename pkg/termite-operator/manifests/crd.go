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

	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

//go:embed crd/antfly.io_termitepools.yaml
var termitePoolCRDYAML []byte

//go:embed crd/antfly.io_termiteroutes.yaml
var termiteRouteCRDYAML []byte

// TermitePoolCRD returns the parsed CustomResourceDefinition for TermitePool.
func TermitePoolCRD() (*apiextv1.CustomResourceDefinition, error) {
	var crd apiextv1.CustomResourceDefinition
	if err := yaml.Unmarshal(termitePoolCRDYAML, &crd); err != nil {
		return nil, err
	}
	return &crd, nil
}

// TermitePoolCRDYAML returns the raw CRD YAML for TermitePool.
// This can be used directly with kubectl apply or similar tools.
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
// This can be used directly with kubectl apply or similar tools.
func TermiteRouteCRDYAML() string {
	return string(termiteRouteCRDYAML)
}

// AllCRDs returns all CRDs needed for the Termite operator as parsed objects.
func AllCRDs() ([]*apiextv1.CustomResourceDefinition, error) {
	pool, err := TermitePoolCRD()
	if err != nil {
		return nil, err
	}
	route, err := TermiteRouteCRD()
	if err != nil {
		return nil, err
	}
	return []*apiextv1.CustomResourceDefinition{pool, route}, nil
}

// AllCRDsYAML returns all CRD YAML files concatenated with YAML document separators.
// This can be used directly with kubectl apply -f.
func AllCRDsYAML() string {
	return TermitePoolCRDYAML() + "\n---\n" + TermiteRouteCRDYAML()
}
