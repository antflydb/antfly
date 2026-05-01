package v1

import (
	"encoding/json"

	termitev1alpha1 "github.com/antflydb/antfly/pkg/operator/api/termite/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AntflyClusterTermiteSpec is the stable AntflyCluster-facing shape for an
// operator-managed TermitePool. The controller translates it to the current
// TermitePool API version during reconciliation.
type AntflyClusterTermiteSpec struct {
	// +kubebuilder:validation:Enum=read-heavy;write-heavy;burst;general
	// +kubebuilder:default=general
	WorkloadType TermiteWorkloadType   `json:"workloadType,omitempty"`
	Config       string                `json:"config,omitempty"`
	Models       TermiteModelConfig    `json:"models"`
	Replicas     TermiteReplicaConfig  `json:"replicas"`
	Hardware     TermiteHardwareConfig `json:"hardware"`
	// +optional
	Autoscaling *TermiteAutoscalingConfig `json:"autoscaling,omitempty"`
	// +optional
	Burst *TermiteBurstConfig `json:"burst,omitempty"`
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
	// +optional
	Availability *TermiteAvailabilityConfig `json:"availability,omitempty"`
	// +optional
	Routing *TermiteRoutingConfig `json:"routing,omitempty"`
	// +optional
	GKE *TermiteGKEConfig `json:"gke,omitempty"`
	// +optional
	EKS *TermiteEKSConfig `json:"eks,omitempty"`
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`
	// +optional
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
	// Image is the Antfly container image used for the managed TermitePool.
	// The image must provide the `/antfly termite ...` runtime contract.
	// +optional
	Image string `json:"image,omitempty"`
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`
}

type TermiteWorkloadType string

const (
	TermiteWorkloadTypeReadHeavy  TermiteWorkloadType = "read-heavy"
	TermiteWorkloadTypeWriteHeavy TermiteWorkloadType = "write-heavy"
	TermiteWorkloadTypeBurst      TermiteWorkloadType = "burst"
	TermiteWorkloadTypeGeneral    TermiteWorkloadType = "general"
)

type TermiteModelPriority string

const (
	TermiteModelPriorityHigh   TermiteModelPriority = "high"
	TermiteModelPriorityMedium TermiteModelPriority = "medium"
	TermiteModelPriorityLow    TermiteModelPriority = "low"
)

type TermiteLoadingStrategy string

const (
	TermiteLoadingStrategyEager   TermiteLoadingStrategy = "eager"
	TermiteLoadingStrategyLazy    TermiteLoadingStrategy = "lazy"
	TermiteLoadingStrategyBounded TermiteLoadingStrategy = "bounded"
)

type TermiteModelConfig struct {
	Preload []TermiteModelSpec `json:"preload"`
	// +kubebuilder:validation:Enum=eager;lazy;bounded
	// +kubebuilder:default=eager
	LoadingStrategy TermiteLoadingStrategy `json:"loadingStrategy,omitempty"`
	// +optional
	MaxLoadedModels *int `json:"maxLoadedModels,omitempty"`
	// +optional
	KeepAlive *metav1.Duration `json:"keepAlive,omitempty"`
	// +optional
	RegistryURL string `json:"registryURL,omitempty"`
}

type TermiteModelSpec struct {
	Name    string `json:"name"`
	Variant string `json:"variant,omitempty"`
	// +kubebuilder:validation:Enum=high;medium;low
	// +kubebuilder:default=medium
	Priority TermiteModelPriority `json:"priority,omitempty"`
	// +optional
	// +kubebuilder:validation:Enum=eager;lazy;bounded
	Strategy TermiteLoadingStrategy `json:"strategy,omitempty"`
}

type TermiteReplicaConfig struct {
	// +kubebuilder:validation:Minimum=0
	Min int32 `json:"min"`
	// +kubebuilder:validation:Minimum=1
	Max int32 `json:"max"`
	// +optional
	PerModel map[string]TermitePerModelReplica `json:"perModel,omitempty"`
}

type TermitePerModelReplica struct {
	Min int32 `json:"min"`
}

type TermiteHardwareConfig struct {
	// +optional
	Accelerator string `json:"accelerator,omitempty"`
	// +optional
	Topology string `json:"topology,omitempty"`
	// +optional
	MachineType string `json:"machineType,omitempty"`
	// +kubebuilder:default=false
	Spot bool `json:"spot,omitempty"`
}

type TermiteAutoscalingConfig struct {
	// +kubebuilder:default=true
	Enabled bool                   `json:"enabled,omitempty"`
	Metrics []TermiteScalingMetric `json:"metrics,omitempty"`
	// +optional
	ModelLoadingGracePeriod *metav1.Duration `json:"modelLoadingGracePeriod,omitempty"`
	// +optional
	WarmupReplicas *int32 `json:"warmupReplicas,omitempty"`
	// +optional
	ScaleDownStabilization *metav1.Duration `json:"scaleDownStabilization,omitempty"`
}

type TermiteMetricType string

const (
	TermiteMetricTypeQueueDepth TermiteMetricType = "queue-depth"
	TermiteMetricTypeLatencyP99 TermiteMetricType = "latency-p99"
	TermiteMetricTypeLatencyP95 TermiteMetricType = "latency-p95"
	TermiteMetricTypeRPS        TermiteMetricType = "requests-per-second"
	TermiteMetricTypeCPU        TermiteMetricType = "cpu"
	TermiteMetricTypeMemory     TermiteMetricType = "memory"
	TermiteMetricTypeThroughput TermiteMetricType = "throughput"
)

type TermiteScalingMetric struct {
	// +kubebuilder:validation:Enum=queue-depth;latency-p99;latency-p95;requests-per-second;cpu;memory;throughput
	Type   TermiteMetricType `json:"type"`
	Target string            `json:"target"`
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
	// +optional
	ScaleUp *TermiteScalingBehavior `json:"scaleUp,omitempty"`
	// +optional
	ScaleDown *TermiteScalingBehavior `json:"scaleDown,omitempty"`
}

type TermiteScalingBehavior struct {
	// +optional
	StabilizationWindow *metav1.Duration `json:"stabilizationWindow,omitempty"`
	// +optional
	Policies []TermiteScalingPolicy `json:"policies,omitempty"`
}

type TermiteScalingPolicy struct {
	Type          string `json:"type"`
	Value         int32  `json:"value"`
	PeriodSeconds int32  `json:"periodSeconds"`
}

type TermiteBurstConfig struct {
	Enabled bool `json:"enabled"`
	// +kubebuilder:default=5
	MaxSurge int32 `json:"maxSurge,omitempty"`
	// +kubebuilder:default=100
	BurstThreshold int32 `json:"burstThreshold,omitempty"`
	// +optional
	CooldownPeriod *metav1.Duration `json:"cooldownPeriod,omitempty"`
}

type TermiteAvailabilityConfig struct {
	// +optional
	PodDisruptionBudget *TermitePDBConfig `json:"podDisruptionBudget,omitempty"`
	// +optional
	StartupProbe *TermiteProbeConfig `json:"startupProbe,omitempty"`
	// +optional
	ReadinessProbe *TermiteProbeConfig `json:"readinessProbe,omitempty"`
	// +optional
	LivenessProbe *TermiteProbeConfig `json:"livenessProbe,omitempty"`
}

type TermitePDBConfig struct {
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`
	// +optional
	MinAvailable *int32 `json:"minAvailable,omitempty"`
	// +optional
	MaxUnavailable *int32 `json:"maxUnavailable,omitempty"`
}

type TermiteGKEConfig struct {
	// +optional
	Autopilot bool `json:"autopilot,omitempty"`
	// +optional
	// +kubebuilder:validation:Enum=Accelerator;Balanced;Performance;Scale-Out;autopilot;autopilot-spot;""
	AutopilotComputeClass string `json:"autopilotComputeClass,omitempty"`
	// +optional
	PodDisruptionBudget *TermitePDBConfig `json:"podDisruptionBudget,omitempty"`
}

type TermiteEKSConfig struct {
	// +optional
	Enabled bool `json:"enabled,omitempty"`
	// +optional
	UseSpotInstances bool `json:"useSpotInstances,omitempty"`
	// +optional
	InstanceTypes []string `json:"instanceTypes,omitempty"`
	// +optional
	IRSARoleARN string `json:"irsaRoleARN,omitempty"`
	// +optional
	PodDisruptionBudget *TermitePDBConfig `json:"podDisruptionBudget,omitempty"`
}

type TermiteProbeConfig struct {
	// +optional
	FailureThreshold *int32 `json:"failureThreshold,omitempty"`
	// +optional
	PeriodSeconds *int32 `json:"periodSeconds,omitempty"`
	// +optional
	TimeoutSeconds *int32 `json:"timeoutSeconds,omitempty"`
}

type TermiteRoutingConfig struct {
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=100
	Weight int32 `json:"weight,omitempty"`
	// +optional
	DrainTimeout *metav1.Duration `json:"drainTimeout,omitempty"`
	// +optional
	CircuitBreaker *TermiteCircuitBreakerConfig `json:"circuitBreaker,omitempty"`
}

type TermiteCircuitBreakerConfig struct {
	Enabled bool `json:"enabled"`
	// +kubebuilder:default=5
	ErrorThreshold int32 `json:"errorThreshold,omitempty"`
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`
}

// ToTermitePoolSpec converts the stable AntflyCluster termite spec into the
// current TermitePool API version used by the managed child resource.
func (in *AntflyClusterTermiteSpec) ToTermitePoolSpec() (termitev1alpha1.TermitePoolSpec, error) {
	if in == nil {
		return termitev1alpha1.TermitePoolSpec{}, nil
	}

	data, err := json.Marshal(in)
	if err != nil {
		return termitev1alpha1.TermitePoolSpec{}, err
	}
	var out termitev1alpha1.TermitePoolSpec
	if err := json.Unmarshal(data, &out); err != nil {
		return termitev1alpha1.TermitePoolSpec{}, err
	}
	return out, nil
}
