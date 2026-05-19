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

func TestExternalTermitePoolValidationAcceptsExternalServices(t *testing.T) {
	pool := &ExternalTermitePool{
		Spec: ExternalTermitePoolSpec{
			WorkloadType: WorkloadTypeWriteHeavy,
			Endpoints: []ExternalTermiteEndpoint{
				{
					Name:             "mac-studio-1",
					APIServiceRef:    "termite-mac-studio-1",
					APIPort:          DefaultTermiteAPIPort,
					HealthServiceRef: "termite-mac-studio-1-health",
					HealthPort:       DefaultTermiteHealthPort,
				},
			},
			Models: []ModelSpec{{Name: "gemma"}},
		},
	}

	if err := pool.ValidateExternalTermitePool(); err != nil {
		t.Fatalf("ValidateExternalTermitePool() unexpected error: %v", err)
	}
}

func TestExternalTermitePoolValidationRejectsMissingEndpoints(t *testing.T) {
	pool := &ExternalTermitePool{}

	err := pool.ValidateExternalTermitePool()
	if err == nil {
		t.Fatal("ValidateExternalTermitePool() expected error")
	}
	if !strings.Contains(err.Error(), "spec.endpoints must have at least one endpoint") {
		t.Fatalf("ValidateExternalTermitePool() error = %q", err.Error())
	}
}

func TestExternalTermitePoolValidationRejectsDuplicateEndpointNames(t *testing.T) {
	pool := &ExternalTermitePool{
		Spec: ExternalTermitePoolSpec{
			Endpoints: []ExternalTermiteEndpoint{
				{Name: "mac-studio", APIServiceRef: "termite-mac-studio-1"},
				{Name: "mac-studio", APIServiceRef: "termite-mac-studio-2"},
			},
		},
	}

	err := pool.ValidateExternalTermitePool()
	if err == nil {
		t.Fatal("ValidateExternalTermitePool() expected error")
	}
	if !strings.Contains(err.Error(), `duplicate endpoint name "mac-studio"`) {
		t.Fatalf("ValidateExternalTermitePool() error = %q", err.Error())
	}
}
