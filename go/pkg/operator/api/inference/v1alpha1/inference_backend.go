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

import "strings"

// InferenceBackendProfile is the shared scheduling and runtime contract for a
// pool. Admission and reconciliation must consume this profile rather than
// independently interpreting accelerator fields.
type InferenceBackendProfile struct {
	PreferredRuntime   string
	RequiredRuntime    string
	RequiresGPU        bool
	RequiresTPU        bool
	ForbidsAccelerator bool
}

// ResolveInferenceBackendProfile resolves the backend contract for one pool.
// Empty and auto preserve the legacy preference behavior; explicit backends
// also declare the resources admission must require.
func ResolveInferenceBackendProfile(pool *InferencePool) InferenceBackendProfile {
	backend := pool.Spec.Hardware.InferenceBackend
	switch backend {
	case InferenceRuntimeBackendCPU:
		return InferenceBackendProfile{
			PreferredRuntime:   "native",
			RequiredRuntime:    "native",
			ForbidsAccelerator: true,
		}
	case InferenceRuntimeBackendCUDA:
		return InferenceBackendProfile{
			PreferredRuntime: "cuda",
			RequiredRuntime:  "cuda",
			RequiresGPU:      true,
		}
	case InferenceRuntimeBackendPJRT:
		return InferenceBackendProfile{
			PreferredRuntime: "pjrt",
			RequiredRuntime:  "pjrt",
			RequiresTPU:      true,
		}
	}

	// Auto is intentionally compatibility-only. It can express a preference,
	// but explicit profiles above are the enforceable scheduling contract.
	if IsTPUAccelerator(pool.Spec.Hardware.Accelerator) {
		return InferenceBackendProfile{PreferredRuntime: "pjrt"}
	}
	if pool.Spec.Hardware.Accelerator != "" || HasGPUResources(pool.Spec.Resources) {
		return InferenceBackendProfile{
			PreferredRuntime: "cuda",
			RequiredRuntime:  "cuda",
		}
	}
	return InferenceBackendProfile{}
}

// IsTPUAccelerator reports whether an accelerator identifier selects TPU.
func IsTPUAccelerator(accelerator string) bool {
	return strings.Contains(strings.ToLower(accelerator), "tpu")
}
