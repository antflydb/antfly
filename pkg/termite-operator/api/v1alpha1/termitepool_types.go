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

// Package v1alpha1 contains API Schema definitions for the antfly v1alpha1 API group
package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WorkloadType defines the type of workload this pool handles
type WorkloadType string

const (
	WorkloadTypeReadHeavy  WorkloadType = "read-heavy"
	WorkloadTypeWriteHeavy WorkloadType = "write-heavy"
	WorkloadTypeBurst      WorkloadType = "burst"
	WorkloadTypeGeneral    WorkloadType = "general"
)

// ModelPriority defines the priority of a model for loading/eviction
type ModelPriority string

const (
	ModelPriorityHigh   ModelPriority = "high"
	ModelPriorityMedium ModelPriority = "medium"
	ModelPriorityLow    ModelPriority = "low"
)

// LoadingStrategy defines how models are loaded
type LoadingStrategy string

const (
	LoadingStrategyEager   LoadingStrategy = "eager"   // Load all at startup
	LoadingStrategyLazy    LoadingStrategy = "lazy"    // Load on first request
	LoadingStrategyBounded LoadingStrategy = "bounded" // LRU eviction
)

// TermitePoolSpec defines the desired state of TermitePool
type TermitePoolSpec struct {
	// WorkloadType classifies this pool for routing decisions
	// +kubebuilder:validation:Enum=read-heavy;write-heavy;burst;general
	// +kubebuilder:default=general
	WorkloadType WorkloadType `json:"workloadType,omitempty"`

	// Config is the Termite configuration as a JSON string.
	// This is merged with auto-generated configuration and passed to termite via --config.
	// Supports all termite config options including logging, GPU settings, keep_alive, etc.
	// Example: {"log": {"level": "debug", "style": "json"}, "gpu": "auto"}
	// +optional
	Config string `json:"config,omitempty"`

	// Models defines which models to load and how
	Models ModelConfig `json:"models"`

	// Replicas defines scaling bounds
	Replicas ReplicaConfig `json:"replicas"`

	// Hardware defines TPU/accelerator configuration
	Hardware HardwareConfig `json:"hardware"`

	// Autoscaling defines autoscaling behavior
	// +optional
	Autoscaling *AutoscalingConfig `json:"autoscaling,omitempty"`

	// Burst defines burst handling configuration
	// +optional
	Burst *BurstConfig `json:"burst,omitempty"`

	// Resources defines compute resource requirements
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// Availability defines availability configuration
	// +optional
	Availability *AvailabilityConfig `json:"availability,omitempty"`

	// Routing defines routing hints for the proxy
	// +optional
	Routing *RoutingConfig `json:"routing,omitempty"`

	// GKE defines GKE-specific configuration for Autopilot and Standard clusters
	// +optional
	GKE *GKEConfig `json:"gke,omitempty"`

	// EKS defines AWS EKS-specific configuration (optional)
	// +optional
	EKS *EKSConfig `json:"eks,omitempty"`

	// Tolerations defines tolerations for pod scheduling.
	// Merged with any cloud-provider-specific tolerations (e.g., EKS Spot).
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// NodeSelector defines node selector labels for pod scheduling.
	// Merged with any cloud-provider-specific node selectors.
	// Note: GKE Autopilot mode overrides node selectors with compute class annotations.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Affinity defines affinity rules for pod scheduling.
	// Cloud-provider-specific affinity rules (e.g., EKS instance type preference)
	// are appended to any user-specified node affinity preferred terms.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// TopologySpreadConstraints describes how pods should spread across topology domains.
	// +optional
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`

	// Image is the Termite container image
	// +optional
	Image string `json:"image,omitempty"`

	// ImagePullSecrets for private registries
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`
}

// ModelConfig defines model loading configuration
type ModelConfig struct {
	// Preload lists models to preload on this pool
	Preload []ModelSpec `json:"preload"`

	// LoadingStrategy defines how models are loaded
	// +kubebuilder:validation:Enum=eager;lazy;bounded
	// +kubebuilder:default=eager
	LoadingStrategy LoadingStrategy `json:"loadingStrategy,omitempty"`

	// MaxLoadedModels limits concurrent loaded models (for bounded strategy)
	// +optional
	MaxLoadedModels *int `json:"maxLoadedModels,omitempty"`

	// KeepAlive duration before unloading idle models (for lazy strategy)
	// +optional
	KeepAlive *metav1.Duration `json:"keepAlive,omitempty"`

	// RegistryURL is the model registry URL
	// +optional
	RegistryURL string `json:"registryURL,omitempty"`
}

// ModelSpec defines a single model to load
type ModelSpec struct {
	// Name is the model name (e.g., "bge-small-en-v1.5")
	Name string `json:"name"`

	// Variant specifies a model variant (e.g., "quantized")
	// +optional
	Variant string `json:"variant,omitempty"`

	// Priority determines loading order and eviction priority
	// +kubebuilder:validation:Enum=high;medium;low
	// +kubebuilder:default=medium
	Priority ModelPriority `json:"priority,omitempty"`

	// Strategy overrides the pool-level loading strategy for this model.
	// If not specified, uses the pool's loadingStrategy.
	// +optional
	// +kubebuilder:validation:Enum=eager;lazy;bounded
	Strategy LoadingStrategy `json:"strategy,omitempty"`
}

// ReplicaConfig defines replica scaling bounds
type ReplicaConfig struct {
	// Min is the minimum number of replicas
	// +kubebuilder:validation:Minimum=0
	Min int32 `json:"min"`

	// Max is the maximum number of replicas
	// +kubebuilder:validation:Minimum=1
	Max int32 `json:"max"`

	// PerModel allows per-model minimum replicas
	// +optional
	PerModel map[string]PerModelReplica `json:"perModel,omitempty"`
}

// PerModelReplica defines per-model replica requirements
type PerModelReplica struct {
	// Min replicas that must have this model loaded
	Min int32 `json:"min"`
}

// HardwareConfig defines TPU/accelerator configuration
type HardwareConfig struct {
	// Accelerator is the accelerator type label (empty = no accelerator/CPU only)
	// +optional
	Accelerator string `json:"accelerator,omitempty"`

	// Topology is the TPU topology (e.g., "2x2", "2x4"). Only required when accelerator is set.
	// +optional
	Topology string `json:"topology,omitempty"`

	// MachineType is the GKE machine type
	// +optional
	MachineType string `json:"machineType,omitempty"`

	// Spot enables spot/preemptible instances
	// +kubebuilder:default=false
	Spot bool `json:"spot,omitempty"`
}

// AutoscalingConfig defines autoscaling behavior
type AutoscalingConfig struct {
	// Enabled toggles autoscaling
	// +kubebuilder:default=true
	Enabled bool `json:"enabled,omitempty"`

	// Metrics defines scaling triggers
	Metrics []ScalingMetric `json:"metrics,omitempty"`

	// ModelLoadingGracePeriod prevents scale-down during model loading
	// +optional
	ModelLoadingGracePeriod *metav1.Duration `json:"modelLoadingGracePeriod,omitempty"`

	// WarmupReplicas is the number of replicas to pre-warm before traffic
	// +optional
	WarmupReplicas *int32 `json:"warmupReplicas,omitempty"`

	// ScaleDownStabilization is the stabilization window for scale-down
	// +optional
	ScaleDownStabilization *metav1.Duration `json:"scaleDownStabilization,omitempty"`
}

// MetricType defines the type of scaling metric
type MetricType string

const (
	MetricTypeQueueDepth MetricType = "queue-depth"
	MetricTypeLatencyP99 MetricType = "latency-p99"
	MetricTypeLatencyP95 MetricType = "latency-p95"
	MetricTypeRPS        MetricType = "requests-per-second"
	MetricTypeCPU        MetricType = "cpu"
	MetricTypeMemory     MetricType = "memory"
	MetricTypeThroughput MetricType = "throughput"
)

// ScalingMetric defines a single scaling trigger
type ScalingMetric struct {
	// Type is the metric type
	// +kubebuilder:validation:Enum=queue-depth;latency-p99;latency-p95;requests-per-second;cpu;memory;throughput
	Type MetricType `json:"type"`

	// Target is the target value (interpretation depends on type)
	Target string `json:"target"`

	// Endpoint is the API endpoint to measure (for latency metrics)
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// ScaleUp defines scale-up behavior
	// +optional
	ScaleUp *ScalingBehavior `json:"scaleUp,omitempty"`

	// ScaleDown defines scale-down behavior
	// +optional
	ScaleDown *ScalingBehavior `json:"scaleDown,omitempty"`
}

// ScalingBehavior defines scaling behavior for a direction
type ScalingBehavior struct {
	// StabilizationWindow is the time to wait before scaling
	// +optional
	StabilizationWindow *metav1.Duration `json:"stabilizationWindow,omitempty"`

	// Policies defines scaling policies
	// +optional
	Policies []ScalingPolicy `json:"policies,omitempty"`
}

// ScalingPolicy defines a single scaling policy
type ScalingPolicy struct {
	// Type is the policy type (Pods or Percent)
	Type string `json:"type"`

	// Value is the scaling amount
	Value int32 `json:"value"`

	// PeriodSeconds is the time window for this policy
	PeriodSeconds int32 `json:"periodSeconds"`
}

// BurstConfig defines burst handling
type BurstConfig struct {
	// Enabled toggles burst handling
	Enabled bool `json:"enabled"`

	// MaxSurge is the maximum extra replicas during burst
	// +kubebuilder:default=5
	MaxSurge int32 `json:"maxSurge,omitempty"`

	// BurstThreshold is the queue depth that triggers burst mode
	// +kubebuilder:default=100
	BurstThreshold int32 `json:"burstThreshold,omitempty"`

	// CooldownPeriod is the time before scaling down burst replicas
	// +optional
	CooldownPeriod *metav1.Duration `json:"cooldownPeriod,omitempty"`
}

// AvailabilityConfig defines availability settings
type AvailabilityConfig struct {
	// PodDisruptionBudget configuration
	// +optional
	PodDisruptionBudget *PDBConfig `json:"podDisruptionBudget,omitempty"`

	// StartupProbe configuration
	// +optional
	StartupProbe *ProbeConfig `json:"startupProbe,omitempty"`

	// ReadinessProbe configuration
	// +optional
	ReadinessProbe *ProbeConfig `json:"readinessProbe,omitempty"`

	// LivenessProbe configuration
	// +optional
	LivenessProbe *ProbeConfig `json:"livenessProbe,omitempty"`
}

// PDBConfig defines PodDisruptionBudget settings
type PDBConfig struct {
	// Enabled indicates if PodDisruptionBudget should be created
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// MinAvailable is the minimum available pods
	// +optional
	MinAvailable *int32 `json:"minAvailable,omitempty"`

	// MaxUnavailable is the maximum unavailable pods
	// +optional
	MaxUnavailable *int32 `json:"maxUnavailable,omitempty"`
}

// GKEConfig defines GKE-specific configuration
type GKEConfig struct {
	// Autopilot enables GKE Autopilot-specific optimizations.
	// When true, uses compute class annotations instead of node selectors.
	// +optional
	Autopilot bool `json:"autopilot,omitempty"`

	// AutopilotComputeClass specifies the GKE Autopilot compute class.
	// Valid values: "Accelerator", "Balanced", "Performance", "Scale-Out", "autopilot", "autopilot-spot"
	// Defaults to "Balanced" when Autopilot=true and this field is empty.
	// Note: "Accelerator" is for GPU workloads only, NOT TPUs.
	// +optional
	// +kubebuilder:validation:Enum=Accelerator;Balanced;Performance;Scale-Out;autopilot;autopilot-spot;""
	AutopilotComputeClass string `json:"autopilotComputeClass,omitempty"`

	// PodDisruptionBudget enables automatic PodDisruptionBudget creation
	// +optional
	PodDisruptionBudget *PDBConfig `json:"podDisruptionBudget,omitempty"`
}

// EKSConfig defines AWS EKS-specific configuration
type EKSConfig struct {
	// Enabled enables EKS-specific optimizations and configurations
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// UseSpotInstances enables EC2 Spot Instances for cost savings (up to 90%)
	// When enabled, pods will be scheduled on Spot Instance nodes
	// +optional
	UseSpotInstances bool `json:"useSpotInstances,omitempty"`

	// InstanceTypes specifies preferred EC2 instance types for node scheduling
	// Examples: ["m5.large", "m5.xlarge", "m6i.large"]
	// Used with node affinity to target specific instance types
	// +optional
	InstanceTypes []string `json:"instanceTypes,omitempty"`

	// IRSARoleARN is the ARN of the IAM role for IRSA (IAM Roles for Service Accounts)
	// Format: arn:aws:iam::<account-id>:role/<role-name>
	// When specified, the operator will annotate the ServiceAccount with this role
	// This enables pods to assume IAM roles for AWS API access (e.g., S3 model registry)
	// +optional
	IRSARoleARN string `json:"irsaRoleARN,omitempty"`

	// PodDisruptionBudget enables automatic PodDisruptionBudget creation
	// Recommended for production deployments to prevent excessive disruption
	// +optional
	PodDisruptionBudget *PDBConfig `json:"podDisruptionBudget,omitempty"`
}

// ProbeConfig defines probe settings
type ProbeConfig struct {
	// FailureThreshold is the number of failures before marking unhealthy
	// +optional
	FailureThreshold *int32 `json:"failureThreshold,omitempty"`

	// PeriodSeconds is the probe interval
	// +optional
	PeriodSeconds *int32 `json:"periodSeconds,omitempty"`

	// TimeoutSeconds is the probe timeout
	// +optional
	TimeoutSeconds *int32 `json:"timeoutSeconds,omitempty"`
}

// RoutingConfig defines routing hints for the proxy
type RoutingConfig struct {
	// Weight is the relative routing weight (0-100)
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=100
	Weight int32 `json:"weight,omitempty"`

	// DrainTimeout is the time to drain before termination
	// +optional
	DrainTimeout *metav1.Duration `json:"drainTimeout,omitempty"`

	// CircuitBreaker configuration
	// +optional
	CircuitBreaker *CircuitBreakerConfig `json:"circuitBreaker,omitempty"`
}

// CircuitBreakerConfig defines circuit breaker settings
type CircuitBreakerConfig struct {
	// Enabled toggles circuit breaker
	Enabled bool `json:"enabled"`

	// ErrorThreshold is the error count to open circuit
	// +kubebuilder:default=5
	ErrorThreshold int32 `json:"errorThreshold,omitempty"`

	// Timeout is the circuit breaker timeout
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`
}

// TermitePoolPhase represents the phase of a TermitePool
type TermitePoolPhase string

const (
	TermitePoolPhasePending  TermitePoolPhase = "Pending"
	TermitePoolPhaseRunning  TermitePoolPhase = "Running"
	TermitePoolPhaseScaling  TermitePoolPhase = "Scaling"
	TermitePoolPhaseDegraded TermitePoolPhase = "Degraded"
)

// Condition type and reason constants for TermitePool and TermiteRoute status.
const (
	TypeConfigurationValid = "ConfigurationValid"
	ReasonValidationPassed = "ValidationPassed"
	ReasonValidationFailed = "ValidationFailed"
)

// TermitePoolStatus defines the observed state of TermitePool
type TermitePoolStatus struct {
	// Phase is the current phase of the pool
	Phase TermitePoolPhase `json:"phase,omitempty"`

	// ObservedGeneration is the most recent generation observed by the controller.
	// Used to skip expensive validation when the spec has not changed since
	// the last successful reconciliation.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Replicas shows replica counts
	Replicas ReplicaStatus `json:"replicas,omitempty"`

	// LoadedModels shows which models are loaded and where
	LoadedModels []LoadedModelStatus `json:"loadedModels,omitempty"`

	// Conditions represent the latest available observations
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// LastScaleTime is the last time the pool scaled
	// +optional
	LastScaleTime *metav1.Time `json:"lastScaleTime,omitempty"`

	// Endpoints lists the current Termite endpoints
	Endpoints []string `json:"endpoints,omitempty"`
}

// ReplicaStatus shows replica counts
type ReplicaStatus struct {
	// Ready is the number of ready replicas
	Ready int32 `json:"ready"`

	// Total is the total number of replicas
	Total int32 `json:"total"`

	// Desired is the desired number of replicas
	Desired int32 `json:"desired"`
}

// LoadedModelStatus shows model loading status
type LoadedModelStatus struct {
	// Name is the model name
	Name string `json:"name"`

	// Replicas is the number of replicas with this model
	Replicas int32 `json:"replicas"`

	// AvgLoadTimeMs is the average model load time
	AvgLoadTimeMs int64 `json:"avgLoadTimeMs,omitempty"`

	// Status is the model status (loaded, loading, error)
	Status string `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas.min,statuspath=.status.replicas.ready,selectorpath=.status.selector
// +kubebuilder:printcolumn:name="Workload",type=string,JSONPath=`.spec.workloadType`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.replicas.ready`
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.status.replicas.desired`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// TermitePool is the Schema for the termitepools API
type TermitePool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec TermitePoolSpec `json:"spec,omitempty"`
	// +optional
	Status TermitePoolStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TermitePoolList contains a list of TermitePool
type TermitePoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TermitePool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TermitePool{}, &TermitePoolList{})
}
