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

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const (
	DefaultInferenceAPIPort    int32 = 11433
	DefaultInferenceHealthPort int32 = 4200
)

// ExternalInferencePoolSpec defines externally managed Inference-compatible endpoints.
type ExternalInferencePoolSpec struct {
	// WorkloadType classifies this pool for routing decisions.
	// +kubebuilder:validation:Enum=read-heavy;write-heavy;burst;general
	// +kubebuilder:default=general
	// +optional
	WorkloadType WorkloadType `json:"workloadType,omitempty"`

	// Endpoints are Kubernetes Service-backed Inference API endpoints.
	// The proxy resolves these service names inside the cluster.
	// +kubebuilder:validation:MinItems=1
	Endpoints []ExternalInferenceEndpoint `json:"endpoints"`

	// Models optionally declares expected models before runtime discovery completes.
	// Runtime /ml/v1/models discovery remains authoritative.
	// +optional
	Models []ModelSpec `json:"models,omitempty"`
}

// ExternalInferenceEndpoint defines one externally managed Inference service endpoint.
type ExternalInferenceEndpoint struct {
	// Name is a stable identifier for this endpoint within the pool.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// APIServiceRef is the Kubernetes Service name for the Inference ML API.
	// +kubebuilder:validation:MinLength=1
	APIServiceRef string `json:"apiServiceRef"`

	// APIPort is the Inference ML API port.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=11433
	// +optional
	APIPort int32 `json:"apiPort,omitempty"`

	// HealthServiceRef is the Kubernetes Service name for the operational health server.
	// Defaults to APIServiceRef when omitted.
	// +optional
	HealthServiceRef string `json:"healthServiceRef,omitempty"`

	// HealthPort is the operational health server port. Health is checked at /readyz.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=4200
	// +optional
	HealthPort int32 `json:"healthPort,omitempty"`
}

// ExternalInferencePoolStatus defines the observed state of ExternalInferencePool.
// Reserved for a future controller; the embedded CloudAF proxy reads spec directly.
type ExternalInferencePoolStatus struct {
	// Phase is a coarse summary of pool availability.
	// +optional
	Phase InferencePoolPhase `json:"phase,omitempty"`

	// ObservedGeneration is the most recent generation observed by a controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Endpoints mirrors the resolved endpoint URLs observed by the proxy/operator.
	// +optional
	Endpoints []ExternalInferenceEndpointStatus `json:"endpoints,omitempty"`
}

// ExternalInferenceEndpointStatus reports one resolved external endpoint.
type ExternalInferenceEndpointStatus struct {
	Name      string       `json:"name,omitempty"`
	APIURL    string       `json:"apiURL,omitempty"`
	HealthURL string       `json:"healthURL,omitempty"`
	Ready     bool         `json:"ready,omitempty"`
	Message   string       `json:"message,omitempty"`
	CheckedAt *metav1.Time `json:"checkedAt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Workload",type=string,JSONPath=`.spec.workloadType`
// +kubebuilder:printcolumn:name="Endpoints",type=string,JSONPath=`.spec.endpoints[*].name`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ExternalInferencePool is the Schema for externally managed Inference pools.
type ExternalInferencePool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ExternalInferencePoolSpec `json:"spec,omitempty"`
	// +optional
	Status ExternalInferencePoolStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ExternalInferencePoolList contains a list of ExternalInferencePool.
type ExternalInferencePoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ExternalInferencePool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ExternalInferencePool{}, &ExternalInferencePoolList{})
}
