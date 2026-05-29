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
	"strings"
	"testing"
)

func TestExternalInferencePoolValidationAcceptsExternalServices(t *testing.T) {
	pool := &ExternalInferencePool{
		Spec: ExternalInferencePoolSpec{
			WorkloadType: WorkloadTypeWriteHeavy,
			Endpoints: []ExternalInferenceEndpoint{
				{
					Name:             "mac-studio-1",
					APIServiceRef:    "inference-mac-studio-1",
					APIPort:          DefaultInferenceAPIPort,
					HealthServiceRef: "inference-mac-studio-1-health",
					HealthPort:       DefaultInferenceHealthPort,
				},
			},
			Models: []ModelSpec{{Name: "gemma"}},
		},
	}

	if err := pool.ValidateExternalInferencePool(); err != nil {
		t.Fatalf("ValidateExternalInferencePool() unexpected error: %v", err)
	}
}

func TestExternalInferencePoolValidationRejectsMissingEndpoints(t *testing.T) {
	pool := &ExternalInferencePool{}

	err := pool.ValidateExternalInferencePool()
	if err == nil {
		t.Fatal("ValidateExternalInferencePool() expected error")
	}
	if !strings.Contains(err.Error(), "spec.endpoints must have at least one endpoint") {
		t.Fatalf("ValidateExternalInferencePool() error = %q", err.Error())
	}
}

func TestExternalInferencePoolValidationRejectsDuplicateEndpointNames(t *testing.T) {
	pool := &ExternalInferencePool{
		Spec: ExternalInferencePoolSpec{
			Endpoints: []ExternalInferenceEndpoint{
				{Name: "mac-studio", APIServiceRef: "inference-mac-studio-1"},
				{Name: "mac-studio", APIServiceRef: "inference-mac-studio-2"},
			},
		},
	}

	err := pool.ValidateExternalInferencePool()
	if err == nil {
		t.Fatal("ValidateExternalInferencePool() expected error")
	}
	if !strings.Contains(err.Error(), `duplicate endpoint name "mac-studio"`) {
		t.Fatalf("ValidateExternalInferencePool() error = %q", err.Error())
	}
}
