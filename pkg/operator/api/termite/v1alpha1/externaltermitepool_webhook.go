// Copyright 2026 Antfly, Inc.
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

package v1alpha1

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
)

// ValidateCreate validates ExternalTermitePool when creating a new pool.
func (p *ExternalTermitePool) ValidateCreate() error {
	return p.ValidateExternalTermitePool()
}

// ValidateUpdate validates ExternalTermitePool when updating an existing pool.
func (p *ExternalTermitePool) ValidateUpdate(old runtime.Object) error {
	return p.ValidateExternalTermitePool()
}

// ValidateExternalTermitePool performs all validation checks.
func (p *ExternalTermitePool) ValidateExternalTermitePool() error {
	var allErrors []string

	switch p.Spec.WorkloadType {
	case "", WorkloadTypeReadHeavy, WorkloadTypeWriteHeavy, WorkloadTypeBurst, WorkloadTypeGeneral:
	default:
		allErrors = append(allErrors, fmt.Sprintf("spec.workloadType must be one of: read-heavy, write-heavy, burst, general"))
	}

	if len(p.Spec.Endpoints) == 0 {
		allErrors = append(allErrors, "spec.endpoints must have at least one endpoint")
	}

	names := make(map[string]bool, len(p.Spec.Endpoints))
	for i, endpoint := range p.Spec.Endpoints {
		name := strings.TrimSpace(endpoint.Name)
		if name == "" {
			allErrors = append(allErrors, fmt.Sprintf("spec.endpoints[%d].name is required", i))
		} else if names[name] {
			allErrors = append(allErrors, fmt.Sprintf("duplicate endpoint name %q", name))
		}
		names[name] = true

		if strings.TrimSpace(endpoint.APIServiceRef) == "" {
			allErrors = append(allErrors, fmt.Sprintf("spec.endpoints[%d].apiServiceRef is required", i))
		}
		if endpoint.APIPort < 0 || endpoint.APIPort > 65535 {
			allErrors = append(allErrors, fmt.Sprintf("spec.endpoints[%d].apiPort must be between 1 and 65535", i))
		}
		if endpoint.HealthPort < 0 || endpoint.HealthPort > 65535 {
			allErrors = append(allErrors, fmt.Sprintf("spec.endpoints[%d].healthPort must be between 1 and 65535", i))
		}
	}

	for i, model := range p.Spec.Models {
		if strings.TrimSpace(model.Name) == "" {
			allErrors = append(allErrors, fmt.Sprintf("spec.models[%d].name is required", i))
		}
	}

	if len(allErrors) > 0 {
		return fmt.Errorf("ExternalTermitePool validation failed:\n  - %s", strings.Join(allErrors, "\n  - "))
	}

	return nil
}
