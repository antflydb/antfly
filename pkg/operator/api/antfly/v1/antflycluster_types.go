package v1

import (
	termitev1alpha1 "github.com/antflydb/antfly/pkg/operator/api/termite/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Condition constants
const (
	// TypeConfigurationValid indicates whether the AntflyCluster configuration is valid
	TypeConfigurationValid = "ConfigurationValid"

	// TypeSecretsReady indicates whether all referenced secrets exist and are accessible
	TypeSecretsReady = "SecretsReady"

	// ReasonValidationFailed indicates configuration validation failed
	ReasonValidationFailed = "ValidationFailed"

	// ReasonValidationPassed indicates configuration validation succeeded
	ReasonValidationPassed = "ValidationPassed"

	// ReasonInvalidConfiguration indicates the configuration contains invalid values
	ReasonInvalidConfiguration = "InvalidConfiguration"

	// ReasonConflictingSettings indicates mutually exclusive settings are both enabled
	ReasonConflictingSettings = "ConflictingSettings"

	// ReasonInvalidComputeClass indicates an invalid GKE Autopilot compute class value
	ReasonInvalidComputeClass = "InvalidComputeClass"

	// ReasonImmutableFieldChanged indicates an attempt to change an immutable field
	ReasonImmutableFieldChanged = "ImmutableFieldChanged"

	// ReasonSecretNotFound indicates a referenced secret was not found
	ReasonSecretNotFound = "SecretNotFound"

	// ReasonInvalidEKSConfig indicates an invalid EKS configuration
	ReasonInvalidEKSConfig = "InvalidEKSConfig"

	// ReasonInvalidIRSARoleARN indicates an invalid IRSA role ARN format
	ReasonInvalidIRSARoleARN = "InvalidIRSARoleARN"

	// ReasonInvalidEBSVolumeType indicates an invalid EBS volume type
	ReasonInvalidEBSVolumeType = "InvalidEBSVolumeType"

	// ReasonConflictingCloudProviders indicates both GKE and EKS are enabled
	ReasonConflictingCloudProviders = "ConflictingCloudProviders"

	// ReasonAllSecretsFound indicates all referenced secrets exist
	ReasonAllSecretsFound = "AllSecretsFound"

	// TypeStorageHealthy indicates whether PVC/storage topology is healthy
	TypeStorageHealthy = "StorageHealthy"

	// TypePVCExpansion indicates whether requested PVC expansion has completed
	TypePVCExpansion = "PVCExpansion"

	// TypeStorageAutoGrow indicates whether storage auto-grow is active and healthy
	TypeStorageAutoGrow = "StorageAutoGrow"

	// TypeRollout indicates whether StatefulSet template changes have rolled out
	TypeRollout = "Rollout"

	// TypeScaling indicates whether replica scaling can proceed safely
	TypeScaling = "Scaling"

	// TypeTermitePoolReady indicates whether the operator-managed TermitePool is reconciled
	TypeTermitePoolReady = "TermitePoolReady"

	// ReasonTermitePoolReady indicates the managed TermitePool reconcile completed
	ReasonTermitePoolReady = "TermitePoolReady"

	// ReasonTermitePoolNameConflict indicates a same-name TermitePool is not owned by the cluster
	ReasonTermitePoolNameConflict = "TermitePoolNameConflict"

	// ReasonTermitePoolManagementDisabled indicates termite pool management is disabled by operator flag
	ReasonTermitePoolManagementDisabled = "TermitePoolManagementDisabled"

	// ReasonPVCAZMismatch indicates PVCs are bound to a different AZ than available nodes
	ReasonPVCAZMismatch = "PVCAZMismatch"

	// ReasonStalePVCDetected indicates orphaned PVCs from a previous cluster were detected
	ReasonStalePVCDetected = "StalePVCDetected"

	// ReasonStorageHealthy indicates storage topology is healthy
	ReasonStorageHealthy = "StorageHealthy"

	// ReasonPVCExpansionFailed indicates a PVC expansion request failed
	ReasonPVCExpansionFailed = "PVCExpansionFailed"

	// ReasonPVCExpansionPending indicates PVCs have not appeared for a resize check yet
	ReasonPVCExpansionPending = "PVCExpansionPending"

	// ReasonPVCExpansionInProgress indicates requested PVC expansion is still applying
	ReasonPVCExpansionInProgress = "PVCExpansionInProgress"

	// ReasonPVCExpansionComplete indicates all observed PVCs satisfy requested storage
	ReasonPVCExpansionComplete = "PVCExpansionComplete"

	// ReasonStorageAutoGrowDisabled indicates storage auto-grow is not enabled
	ReasonStorageAutoGrowDisabled = "StorageAutoGrowDisabled"

	// ReasonStorageAutoGrowReady indicates storage usage is below the grow threshold
	ReasonStorageAutoGrowReady = "StorageAutoGrowReady"

	// ReasonStorageAutoGrowInProgress indicates the operator requested automatic storage growth
	ReasonStorageAutoGrowInProgress = "StorageAutoGrowInProgress"

	// ReasonStorageAutoGrowUsageUnavailable indicates PVC usage metrics are unavailable
	ReasonStorageAutoGrowUsageUnavailable = "StorageAutoGrowUsageUnavailable"

	// ReasonStorageAutoGrowMaxReached indicates a PVC is at the configured auto-grow maximum
	ReasonStorageAutoGrowMaxReached = "StorageAutoGrowMaxReached"

	// ReasonStorageAutoGrowFailed indicates storage auto-grow could not be evaluated
	ReasonStorageAutoGrowFailed = "StorageAutoGrowFailed"

	// ReasonRolloutInProgress indicates StatefulSet changes are still rolling out
	ReasonRolloutInProgress = "RolloutInProgress"

	// ReasonRolloutComplete indicates StatefulSet changes have rolled out
	ReasonRolloutComplete = "RolloutComplete"

	// ReasonRolloutFailed indicates StatefulSet rollout failed or could not be observed
	ReasonRolloutFailed = "RolloutFailed"

	// ReasonScalingReady indicates scaling is not currently blocked
	ReasonScalingReady = "ScalingReady"

	// ReasonDataScaleDownBlocked indicates data-node scale-down is blocked by a safety gate
	ReasonDataScaleDownBlocked = "DataScaleDownBlocked"

	// ReasonDataScaleDownInProgress indicates data-node scale-down is draining one ordinal
	ReasonDataScaleDownInProgress = "DataScaleDownInProgress"

	// ReasonDataScaleDownFailed indicates data-node scale-down could not drain the selected ordinal
	ReasonDataScaleDownFailed = "DataScaleDownFailed"

	// FinalizerPVCCleanup is the finalizer used for PVC cleanup on cluster deletion
	FinalizerPVCCleanup = "antfly.io/pvc-cleanup"
)

// ClusterMode selects the topology managed by the operator.
type ClusterMode string

const (
	// ClusterModeClustered is the existing split metadata/data topology.
	ClusterModeClustered ClusterMode = "Clustered"

	// ClusterModeSwarm is the single-node operator-managed swarm topology.
	ClusterModeSwarm ClusterMode = "Swarm"
)

// AntflyClusterSpec defines the desired state of AntflyCluster
type AntflyClusterSpec struct {
	// Mode selects the runtime topology managed by the operator.
	// +optional
	// +kubebuilder:validation:Enum=Clustered;Swarm
	// +kubebuilder:default=Clustered
	Mode ClusterMode `json:"mode,omitempty"`

	// Image is the container image to use for Antfly
	Image string `json:"image"`

	// ImagePullPolicy defines the image pull policy
	ImagePullPolicy string `json:"imagePullPolicy,omitempty"`

	// Swarm defines the single-node swarm topology when Mode=Swarm.
	// +optional
	Swarm *SwarmSpec `json:"swarm,omitempty"`

	// Termite defines an operator-managed TermitePool associated with this cluster.
	// When set, the Antfly operator creates or updates a TermitePool named
	// "<cluster-name>-termite" in the same namespace and owned by this cluster.
	// +optional
	Termite *termitev1alpha1.TermitePoolSpec `json:"termite,omitempty"`

	// ProductTier records the CloudAF/product tier intent that was expanded
	// into the explicit operator fields below. The operator does not resolve
	// prices or tier catalogs; it validates that a stamped tier has concrete
	// resources, storage, and autoscaling intent in the normal fields.
	// +optional
	ProductTier *ProductTierSpec `json:"productTier,omitempty"`

	// MetadataNodes defines the configuration for metadata nodes (StatefulSet).
	// Required for Clustered mode and must be omitted in Swarm mode.
	// +optional
	MetadataNodes MetadataNodesSpec `json:"metadataNodes,omitempty"`

	// DataNodes defines the configuration for data nodes (StatefulSet).
	// Required for Clustered mode and must be omitted in Swarm mode.
	// +optional
	DataNodes DataNodesSpec `json:"dataNodes,omitempty"`

	// Config is the configuration file content for Antfly
	Config string `json:"config"`

	// Storage defines the storage configuration
	Storage StorageSpec `json:"storage"`

	// GKE defines GKE-specific configuration (optional)
	GKE *GKESpec `json:"gke,omitempty"`

	// EKS defines AWS EKS-specific configuration (optional)
	EKS *EKSSpec `json:"eks,omitempty"`

	// ServiceMesh configures optional service mesh integration
	// +optional
	ServiceMesh *ServiceMeshSpec `json:"serviceMesh,omitempty"`

	// PublicAPI defines the public API service configuration (optional)
	// Controls the external-facing service that exposes the cluster API
	// +optional
	PublicAPI *PublicAPIConfig `json:"publicAPI,omitempty"`

	// ServiceAccountName is the name of the Kubernetes ServiceAccount to use for pods
	// This allows pods to authenticate with cloud providers (GCP, AWS) for Workload Identity
	// If not specified, the default ServiceAccount for the namespace is used
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`
}

// ProductTierSpec records product-tier provenance for a CR whose concrete
// sizing has already been expanded into explicit operator fields.
type ProductTierSpec struct {
	// Name is the external product tier name, such as "starter" or "pro".
	Name string `json:"name,omitempty"`

	// Revision identifies the tier catalog revision used to expand this CR.
	// +optional
	Revision string `json:"revision,omitempty"`

	// ManagedBy identifies the system that expanded the tier, for example
	// "cloudaf".
	// +optional
	ManagedBy string `json:"managedBy,omitempty"`

	// SwarmTier optionally records the swarm sub-tier name when Mode=Swarm.
	// +optional
	SwarmTier string `json:"swarmTier,omitempty"`

	// MetadataTier optionally records the metadata-node sub-tier name when
	// Mode=Clustered.
	// +optional
	MetadataTier string `json:"metadataTier,omitempty"`

	// DataTier optionally records the data-node sub-tier name when
	// Mode=Clustered.
	// +optional
	DataTier string `json:"dataTier,omitempty"`

	// TermiteTier optionally records the TermitePool sub-tier name when
	// spec.termite is set.
	// +optional
	TermiteTier string `json:"termiteTier,omitempty"`
}

// MetadataNodesSpec defines the configuration for metadata nodes
type MetadataNodesSpec struct {
	// Replicas is the number of metadata nodes (default: 3)
	Replicas int32 `json:"replicas,omitempty"`

	// Resources defines the resource requirements
	Resources ResourceSpec `json:"resources"`

	// MetadataAPI defines the metadata API configuration
	MetadataAPI APISpec `json:"metadataAPI"`

	// MetadataRaft defines the metadata Raft configuration
	MetadataRaft APISpec `json:"metadataRaft"`

	// Health defines the health check endpoint configuration
	// +optional
	Health APISpec `json:"health,omitempty"`

	// UseSpotPods enables GKE Spot Pods for metadata nodes (standard GKE only)
	// MUST be false when spec.gke.autopilot=true (use spec.gke.autopilotComputeClass instead)
	// Not recommended for production metadata nodes as they maintain Raft consensus
	// +optional
	UseSpotPods bool `json:"useSpotPods,omitempty"`

	// EnvFrom is a list of sources to populate environment variables in the container.
	// This is commonly used to inject backup credentials from Secrets or ConfigMaps.
	// The keys within a source must be a C_IDENTIFIER. All invalid keys will be
	// reported as an event when the container is starting.
	// Example usage for S3/GCS backup credentials:
	//   envFrom:
	//     - secretRef:
	//         name: backup-credentials
	// The secret should contain AWS SDK compatible keys:
	//   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_URL (optional), AWS_REGION (optional)
	// +optional
	EnvFrom []corev1.EnvFromSource `json:"envFrom,omitempty"`

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
}

// DataNodesSpec defines the configuration for data nodes
type DataNodesSpec struct {
	// Replicas is the number of data nodes (default: 3)
	Replicas int32 `json:"replicas,omitempty"`

	// AutoScaling defines autoscaling configuration
	AutoScaling *AutoScalingSpec `json:"autoScaling,omitempty"`

	// Resources defines the resource requirements
	Resources ResourceSpec `json:"resources"`

	// API defines the API configuration
	API APISpec `json:"api"`

	// Raft defines the Raft configuration
	Raft APISpec `json:"raft"`

	// Health defines the health check endpoint configuration
	// +optional
	Health APISpec `json:"health,omitempty"`

	// UseSpotPods enables GKE Spot Pods for data nodes (standard GKE only)
	// MUST be false when spec.gke.autopilot=true (use spec.gke.autopilotComputeClass instead)
	// Safe for data nodes with 3+ replicas
	// +optional
	UseSpotPods bool `json:"useSpotPods,omitempty"`

	// EnvFrom is a list of sources to populate environment variables in the container.
	// This is commonly used to inject backup credentials from Secrets or ConfigMaps.
	// The keys within a source must be a C_IDENTIFIER. All invalid keys will be
	// reported as an event when the container is starting.
	// Example usage for S3/GCS backup credentials:
	//   envFrom:
	//     - secretRef:
	//         name: backup-credentials
	// The secret should contain AWS SDK compatible keys:
	//   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_URL (optional), AWS_REGION (optional)
	// +optional
	EnvFrom []corev1.EnvFromSource `json:"envFrom,omitempty"`

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
}

// SwarmSpec defines the configuration for operator-managed swarm mode.
type SwarmSpec struct {
	// Replicas is the number of swarm replicas. MVP only supports 1.
	Replicas int32 `json:"replicas,omitempty"`

	// NodeID is the swarm node ID used for local orchestration URLs.
	NodeID int32 `json:"nodeID,omitempty"`

	// Resources defines the resource requirements.
	Resources ResourceSpec `json:"resources"`

	// MetadataAPI defines the metadata API configuration.
	MetadataAPI APISpec `json:"metadataAPI,omitempty"`

	// MetadataRaft defines the metadata raft configuration.
	MetadataRaft APISpec `json:"metadataRaft,omitempty"`

	// StoreAPI defines the store API configuration.
	StoreAPI APISpec `json:"storeAPI,omitempty"`

	// StoreRaft defines the store raft configuration.
	StoreRaft APISpec `json:"storeRaft,omitempty"`

	// Health defines the health endpoint configuration.
	Health APISpec `json:"health,omitempty"`

	// Termite controls the optional termite sidecar runtime integrated into swarm mode.
	// +optional
	Termite *SwarmTermiteSpec `json:"termite,omitempty"`

	// EnvFrom is a list of sources to populate environment variables in the container.
	// +optional
	EnvFrom []corev1.EnvFromSource `json:"envFrom,omitempty"`

	// Tolerations defines tolerations for pod scheduling.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// NodeSelector defines node selector labels for pod scheduling.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Affinity defines affinity rules for pod scheduling.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// TopologySpreadConstraints describes how pods should spread across topology domains.
	// +optional
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
}

// SwarmTermiteSpec defines termite configuration for swarm mode.
type SwarmTermiteSpec struct {
	// Enabled controls whether termite runs alongside the swarm node.
	Enabled bool `json:"enabled,omitempty"`

	// APIURL is the termite API URL.
	APIURL string `json:"apiURL,omitempty"`
}

// APISpec defines API configuration
type APISpec struct {
	// Port is the port number (optional, operator sets defaults)
	Port int32 `json:"port,omitempty"`

	// Host is the host to bind to (default: 0.0.0.0)
	Host string `json:"host,omitempty"`
}

// ResourceSpec defines resource requirements
type ResourceSpec struct {
	// CPU resource requirements
	CPU string `json:"cpu,omitempty"`

	// Memory resource requirements
	Memory string `json:"memory,omitempty"`

	// Limits defines the resource limits
	Limits ResourceLimits `json:"limits"`
}

// ResourceLimits defines resource limits
type ResourceLimits struct {
	// CPU limit
	CPU string `json:"cpu,omitempty"`

	// Memory limit
	Memory string `json:"memory,omitempty"`

	// GPU limit (maps to nvidia.com/gpu resource)
	GPU string `json:"gpu,omitempty"`
}

// AutoScalingSpec defines autoscaling configuration
type AutoScalingSpec struct {
	// Enabled indicates if autoscaling is enabled
	Enabled bool `json:"enabled"`

	// MinReplicas is the minimum number of replicas
	MinReplicas int32 `json:"minReplicas"`

	// MaxReplicas is the maximum number of replicas
	MaxReplicas int32 `json:"maxReplicas"`

	// TargetCPUUtilizationPercentage is the target CPU utilization percentage
	TargetCPUUtilizationPercentage *int32 `json:"targetCPUUtilizationPercentage,omitempty"`

	// TargetMemoryUtilizationPercentage is the target memory utilization percentage
	TargetMemoryUtilizationPercentage *int32 `json:"targetMemoryUtilizationPercentage,omitempty"`

	// ScaleUpCooldown is the cooldown period before another scale up (default: 60s)
	ScaleUpCooldown *metav1.Duration `json:"scaleUpCooldown,omitempty"`

	// ScaleDownCooldown is the cooldown period before another scale down (default: 300s)
	ScaleDownCooldown *metav1.Duration `json:"scaleDownCooldown,omitempty"`
}

// StorageSpec defines storage configuration
type StorageSpec struct {
	// StorageClass is the storage class to use
	StorageClass string `json:"storageClass,omitempty"`

	// MetadataStorage defines storage for metadata nodes
	MetadataStorage string `json:"metadataStorage,omitempty"`

	// DataStorage defines storage for data nodes
	DataStorage string `json:"dataStorage,omitempty"`

	// SwarmStorage defines storage for the swarm topology.
	// Used when spec.mode=Swarm.
	// +optional
	SwarmStorage string `json:"swarmStorage,omitempty"`

	// PVCRetentionPolicy controls what happens to PVCs when the cluster is deleted or scaled down.
	// Maps to StatefulSet's persistentVolumeClaimRetentionPolicy (beta in K8s 1.27, GA in 1.32).
	// On clusters < 1.27, this field is silently ignored by the StatefulSet controller;
	// the finalizer provides a fallback for WhenDeleted=Delete.
	// +optional
	PVCRetentionPolicy *PVCRetentionPolicy `json:"pvcRetentionPolicy,omitempty"`

	// StorageAutoGrow configures operator-owned grow-only disk autoscaling.
	// Clustered mode currently applies this policy only to data PVCs. Swarm
	// mode applies it to the swarm PVC.
	// +optional
	StorageAutoGrow *StorageAutoGrowSpec `json:"storageAutoGrow,omitempty"`
}

// StorageAutoGrowSpec configures automatic grow-only PVC expansion.
type StorageAutoGrowSpec struct {
	// Enabled controls whether the operator automatically grows storage.
	Enabled bool `json:"enabled,omitempty"`

	// MaxDataStorage is the maximum size for clustered data PVC auto-grow.
	MaxDataStorage string `json:"maxDataStorage,omitempty"`

	// MaxSwarmStorage is the maximum size for swarm PVC auto-grow. If omitted
	// in swarm mode, MaxDataStorage is used as the limit.
	MaxSwarmStorage string `json:"maxSwarmStorage,omitempty"`

	// GrowThresholdPercent is the percent-used threshold that triggers growth.
	// Defaults to 85 when omitted.
	GrowThresholdPercent int32 `json:"growThresholdPercent,omitempty"`

	// GrowIncrement is the amount added per grow step. Defaults to 10Gi when
	// omitted.
	GrowIncrement string `json:"growIncrement,omitempty"`
}

// PVCRetentionPolicyType defines the retention behavior for PVCs
type PVCRetentionPolicyType string

const (
	// PVCRetentionDelete deletes PVCs when the associated resource is removed
	PVCRetentionDelete PVCRetentionPolicyType = "Delete"
	// PVCRetentionRetain retains PVCs when the associated resource is removed
	PVCRetentionRetain PVCRetentionPolicyType = "Retain"
)

// PVCRetentionPolicy controls PVC lifecycle for StatefulSet volumes
type PVCRetentionPolicy struct {
	// WhenDeleted controls PVC retention when the AntflyCluster is deleted.
	// Valid values: Retain (default), Delete.
	// +optional
	// +kubebuilder:validation:Enum=Retain;Delete
	// +kubebuilder:default=Retain
	WhenDeleted PVCRetentionPolicyType `json:"whenDeleted,omitempty"`

	// WhenScaled controls PVC retention when the StatefulSet is scaled down.
	// Valid values: Retain (default), Delete.
	// WARNING: Delete causes a full Raft snapshot resync per shard when nodes rejoin after scale-up.
	// Cannot be set to Delete when dataNodes.autoScaling.enabled is true (webhook-enforced).
	// +optional
	// +kubebuilder:validation:Enum=Retain;Delete
	// +kubebuilder:default=Retain
	WhenScaled PVCRetentionPolicyType `json:"whenScaled,omitempty"`
}

// GKESpec defines GKE-specific configuration
type GKESpec struct {
	// Autopilot enables GKE Autopilot-specific optimizations
	// +optional
	Autopilot bool `json:"autopilot,omitempty"`

	// AutopilotComputeClass specifies the GKE Autopilot compute class
	// Valid values: "Accelerator", "Balanced", "Performance", "Scale-Out", "autopilot", "autopilot-spot"
	// Defaults to "Balanced" when Autopilot=true and this field is empty
	// +optional
	// +kubebuilder:validation:Enum=Accelerator;Balanced;Performance;Scale-Out;autopilot;autopilot-spot;""
	AutopilotComputeClass string `json:"autopilotComputeClass,omitempty"`

	// PodDisruptionBudget enables automatic PodDisruptionBudget creation for StatefulSets
	// +optional
	PodDisruptionBudget *PodDisruptionBudgetSpec `json:"podDisruptionBudget,omitempty"`
}

// PodDisruptionBudgetSpec defines PodDisruptionBudget configuration
type PodDisruptionBudgetSpec struct {
	// Enabled indicates if PodDisruptionBudget should be created
	Enabled bool `json:"enabled"`

	// MaxUnavailable is the maximum number of pods that can be unavailable (default: 1)
	MaxUnavailable *int32 `json:"maxUnavailable,omitempty"`

	// MinAvailable is the minimum number of pods that must be available
	MinAvailable *int32 `json:"minAvailable,omitempty"`
}

// EKSSpec defines AWS EKS-specific configuration
type EKSSpec struct {
	// Enabled enables EKS-specific optimizations and configurations
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// UseSpotInstances enables EC2 Spot Instances for cost savings (up to 90%)
	// When enabled, pods will be scheduled on Spot Instance nodes
	// Recommended for data nodes with 3+ replicas; not recommended for metadata nodes
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
	// This enables pods to assume IAM roles for AWS API access (e.g., S3 backups)
	// +optional
	IRSARoleARN string `json:"irsaRoleARN,omitempty"`

	// EBSVolumeType specifies the EBS volume type for persistent storage
	// Valid values: gp3 (default), gp2, io1, io2, st1, sc1
	// gp3 is recommended for most workloads (better price/performance)
	// io1/io2 for high-performance requirements
	// +optional
	// +kubebuilder:validation:Enum=gp3;gp2;io1;io2;st1;sc1;""
	EBSVolumeType string `json:"ebsVolumeType,omitempty"`

	// EBSEncrypted enables encryption for EBS volumes
	// When true, volumes will be encrypted using the specified KMS key or the default EBS encryption key
	// +optional
	EBSEncrypted bool `json:"ebsEncrypted,omitempty"`

	// EBSKmsKeyId is the KMS key ID or ARN for EBS volume encryption
	// Only used when EBSEncrypted is true
	// If not specified, the default EBS encryption key for the account is used
	// +optional
	EBSKmsKeyId string `json:"ebsKmsKeyId,omitempty"`

	// EBSIOPs specifies the provisioned IOPS for io1/io2 volumes
	// Only applicable when EBSVolumeType is io1 or io2
	// +optional
	EBSIOPs *int32 `json:"ebsIOPs,omitempty"`

	// EBSThroughput specifies the throughput in MiB/s for gp3 volumes
	// Only applicable when EBSVolumeType is gp3
	// Default is 125 MiB/s, maximum is 1000 MiB/s
	// +optional
	EBSThroughput *int32 `json:"ebsThroughput,omitempty"`

	// PodDisruptionBudget enables automatic PodDisruptionBudget creation for StatefulSets
	// Recommended for production deployments to prevent excessive disruption
	// +optional
	PodDisruptionBudget *PodDisruptionBudgetSpec `json:"podDisruptionBudget,omitempty"`
}

// ServiceMeshSpec defines service mesh integration configuration
type ServiceMeshSpec struct {
	// Enabled controls whether service mesh sidecar injection is enabled
	// +optional
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// Annotations contains mesh-specific annotations to apply to pod templates
	// Common examples:
	//   Istio: {"sidecar.istio.io/inject": "true"}
	//   Linkerd: {"linkerd.io/inject": "enabled"}
	//   Consul: {"consul.hashicorp.com/connect-inject": "true"}
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// PublicAPIConfig defines the public API service configuration
type PublicAPIConfig struct {
	// Enabled controls whether the public API service is created
	// When false, no external service is created (users manage their own Ingress)
	// +optional
	// +kubebuilder:default=false
	Enabled *bool `json:"enabled,omitempty"`

	// ServiceType specifies the Kubernetes service type
	// Valid values: ClusterIP, NodePort, LoadBalancer
	// Default: LoadBalancer
	// +optional
	// +kubebuilder:validation:Enum=ClusterIP;NodePort;LoadBalancer
	// +kubebuilder:default=LoadBalancer
	ServiceType *corev1.ServiceType `json:"serviceType,omitempty"`

	// Port is the service port to expose (default: 80)
	// +optional
	// +kubebuilder:default=80
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port,omitempty"`

	// NodePort specifies the node port when ServiceType is NodePort
	// Only valid when ServiceType=NodePort
	// If not specified, Kubernetes will auto-assign a port in the range 30000-32767
	// +optional
	// +kubebuilder:validation:Minimum=30000
	// +kubebuilder:validation:Maximum=32767
	NodePort *int32 `json:"nodePort,omitempty"`
}

// AntflyClusterStatus defines the observed state of AntflyCluster
type AntflyClusterStatus struct {
	// Phase represents the current phase of the cluster
	Phase string `json:"phase,omitempty"`

	// Mode reports the observed topology mode.
	// +optional
	Mode ClusterMode `json:"mode,omitempty"`

	// ObservedGeneration is the most recent generation observed by the controller.
	// Used to skip expensive validation when the spec has not changed since
	// the last successful reconciliation.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the current conditions of the cluster
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ReadyReplicas is the total number of ready replicas across the active topology.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// MetadataNodesReady represents the number of ready metadata nodes
	MetadataNodesReady int32 `json:"metadataNodesReady,omitempty"`

	// DataNodesReady represents the number of ready data nodes
	DataNodesReady int32 `json:"dataNodesReady,omitempty"`

	// SwarmNodesReady represents the number of ready swarm nodes.
	// +optional
	SwarmNodesReady int32 `json:"swarmNodesReady,omitempty"`

	// AutoScalingStatus tracks autoscaling state
	AutoScalingStatus *AutoScalingStatus `json:"autoScalingStatus,omitempty"`

	// DataScaleDownStatus tracks the operator-owned data-node scale-down workflow.
	// +optional
	DataScaleDownStatus *DataScaleDownStatus `json:"dataScaleDownStatus,omitempty"`

	// StorageAutoGrowStatus tracks the latest operator-owned storage auto-grow evaluation.
	// +optional
	StorageAutoGrowStatus *StorageAutoGrowStatus `json:"storageAutoGrowStatus,omitempty"`

	// ProductTierStatus reports the concrete shape observed for spec.productTier.
	// +optional
	ProductTierStatus *ProductTierStatus `json:"productTierStatus,omitempty"`

	// SwarmStatus reports swarm-specific operational state.
	// +optional
	SwarmStatus *SwarmStatus `json:"swarmStatus,omitempty"`

	// ServiceMeshStatus reports service mesh operational state
	// +optional
	ServiceMeshStatus *ServiceMeshStatus `json:"serviceMeshStatus,omitempty"`
}

// AutoScalingStatus tracks the current autoscaling state
type AutoScalingStatus struct {
	// CurrentReplicas is the current number of replicas
	CurrentReplicas int32 `json:"currentReplicas"`

	// DesiredReplicas is the replica count the operator is currently applying.
	// When scale-down is blocked, this remains at CurrentReplicas even if
	// RecommendationReplicas is lower.
	DesiredReplicas int32 `json:"desiredReplicas"`

	// RecommendationReplicas is the latest replica recommendation from the
	// operator autoscaler before safety gates are applied.
	// +optional
	RecommendationReplicas int32 `json:"recommendationReplicas,omitempty"`

	// BlockedReason explains why the autoscaler recommendation was not applied.
	// +optional
	BlockedReason string `json:"blockedReason,omitempty"`

	// BlockedMessage provides human-readable detail for BlockedReason.
	// +optional
	BlockedMessage string `json:"blockedMessage,omitempty"`

	// LastScaleTime is the last time scaling occurred
	LastScaleTime *metav1.Time `json:"lastScaleTime,omitempty"`

	// LastScaleDirection indicates the direction of the last scaling operation
	// Values: "up", "down", or empty string if no scaling has occurred
	// +optional
	LastScaleDirection string `json:"lastScaleDirection,omitempty"`

	// CurrentCPUUtilizationPercentage is the current CPU utilization
	CurrentCPUUtilizationPercentage *int32 `json:"currentCPUUtilizationPercentage,omitempty"`

	// CurrentMemoryUtilizationPercentage is the current memory utilization
	CurrentMemoryUtilizationPercentage *int32 `json:"currentMemoryUtilizationPercentage,omitempty"`
}

// DataScaleDownStatus tracks a one-ordinal-at-a-time data-node scale-down.
type DataScaleDownStatus struct {
	// FromReplicas is the observed/applied replica count before this scale-down step.
	FromReplicas int32 `json:"fromReplicas,omitempty"`

	// TargetReplicas is the user or autoscaler requested final replica count.
	TargetReplicas int32 `json:"targetReplicas,omitempty"`

	// AppliedReplicas is the replica count applied to the StatefulSet for this step.
	AppliedReplicas int32 `json:"appliedReplicas,omitempty"`

	// DrainingOrdinal is the StatefulSet ordinal selected for this step.
	DrainingOrdinal int32 `json:"drainingOrdinal,omitempty"`

	// DrainingStoreID is the Antfly store ID selected for this step.
	DrainingStoreID string `json:"drainingStoreID,omitempty"`

	// Phase is the current scale-down workflow phase.
	Phase string `json:"phase,omitempty"`

	// Message describes the current scale-down workflow state.
	Message string `json:"message,omitempty"`

	// LastTransitionTime records the last phase transition.
	LastTransitionTime *metav1.Time `json:"lastTransitionTime,omitempty"`
}

// StorageAutoGrowStatus tracks the latest storage auto-grow decision.
type StorageAutoGrowStatus struct {
	// Component is the component evaluated by the latest auto-grow pass.
	Component string `json:"component,omitempty"`

	// CurrentSize is the current effective requested PVC size.
	CurrentSize string `json:"currentSize,omitempty"`

	// RecommendedSize is the size the operator selected when growth is needed.
	RecommendedSize string `json:"recommendedSize,omitempty"`

	// MaxSize is the configured maximum size for the evaluated component.
	MaxSize string `json:"maxSize,omitempty"`

	// UsedBytes is the observed used bytes across matching PVC volumes.
	UsedBytes int64 `json:"usedBytes,omitempty"`

	// CapacityBytes is the observed capacity bytes across matching PVC volumes.
	CapacityBytes int64 `json:"capacityBytes,omitempty"`

	// UsagePercent is the observed storage usage percentage.
	UsagePercent int32 `json:"usagePercent,omitempty"`

	// Reason is the reason for the latest auto-grow decision.
	Reason string `json:"reason,omitempty"`

	// Message describes the latest auto-grow decision.
	Message string `json:"message,omitempty"`

	// LastEvaluationTime records when auto-grow was last evaluated.
	LastEvaluationTime *metav1.Time `json:"lastEvaluationTime,omitempty"`
}

// ProductTierStatus reports the concrete operator fields produced from a tier.
type ProductTierStatus struct {
	// Name is the observed tier name.
	Name string `json:"name,omitempty"`

	// Revision is the observed tier catalog revision.
	Revision string `json:"revision,omitempty"`

	// ManagedBy is the observed tier owner.
	ManagedBy string `json:"managedBy,omitempty"`

	// Mode is the topology mode for this tier shape.
	Mode ClusterMode `json:"mode,omitempty"`

	// SwarmTier records the observed swarm sub-tier name.
	SwarmTier string `json:"swarmTier,omitempty"`

	// MetadataTier records the observed metadata sub-tier name.
	MetadataTier string `json:"metadataTier,omitempty"`

	// DataTier records the observed data sub-tier name.
	DataTier string `json:"dataTier,omitempty"`

	// TermiteTier records the observed termite sub-tier name.
	TermiteTier string `json:"termiteTier,omitempty"`

	// SwarmResources summarizes swarm CPU/memory requests and limits.
	SwarmResources string `json:"swarmResources,omitempty"`

	// SwarmStorage is the observed swarm storage size.
	SwarmStorage string `json:"swarmStorage,omitempty"`

	// MetadataReplicas is the observed metadata replica count.
	MetadataReplicas int32 `json:"metadataReplicas,omitempty"`

	// MetadataResources summarizes metadata CPU/memory requests and limits.
	MetadataResources string `json:"metadataResources,omitempty"`

	// MetadataStorage is the observed metadata storage size.
	MetadataStorage string `json:"metadataStorage,omitempty"`

	// DataReplicas is the observed data replica count.
	DataReplicas int32 `json:"dataReplicas,omitempty"`

	// DataResources summarizes data CPU/memory requests and limits.
	DataResources string `json:"dataResources,omitempty"`

	// DataStorage is the observed data storage size.
	DataStorage string `json:"dataStorage,omitempty"`

	// DataAutoscaling reports the observed data autoscaling bounds.
	DataAutoscaling string `json:"dataAutoscaling,omitempty"`

	// TermiteEnabled reports whether this tier has an operator-managed TermitePool.
	TermiteEnabled bool `json:"termiteEnabled,omitempty"`

	// TermiteReplicas reports the observed TermitePool replica bounds.
	TermiteReplicas string `json:"termiteReplicas,omitempty"`

	// ObservedGeneration is the AntflyCluster generation used for this status.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// ServiceMeshStatus reports service mesh operational status
type ServiceMeshStatus struct {
	// Enabled reflects whether service mesh is currently enabled (mirrors spec)
	Enabled bool `json:"enabled,omitempty"`

	// SidecarInjectionStatus indicates sidecar injection state
	// Values: "Complete", "Partial", "None", "Unknown"
	SidecarInjectionStatus string `json:"sidecarInjectionStatus,omitempty"`

	// PodsWithSidecars count of pods with sidecars injected
	PodsWithSidecars int32 `json:"podsWithSidecars,omitempty"`

	// TotalPods total expected pods (metadata + data replicas)
	TotalPods int32 `json:"totalPods,omitempty"`

	// LastTransitionTime when status last changed
	LastTransitionTime *metav1.Time `json:"lastTransitionTime,omitempty"`
}

// SwarmStatus reports swarm mode operational status.
type SwarmStatus struct {
	// Ready indicates that the combined swarm workload is ready.
	Ready bool `json:"ready,omitempty"`

	// MetadataReady indicates that the metadata API is ready.
	MetadataReady bool `json:"metadataReady,omitempty"`

	// StoreReady indicates that the store API is ready.
	StoreReady bool `json:"storeReady,omitempty"`

	// TermiteReady indicates that termite is ready when enabled.
	TermiteReady bool `json:"termiteReady,omitempty"`

	// NodeID is the configured swarm node ID.
	NodeID int32 `json:"nodeID,omitempty"`

	// PodName is the name of the backing swarm pod.
	PodName string `json:"podName,omitempty"`

	// PodIP is the IP of the backing swarm pod.
	PodIP string `json:"podIP,omitempty"`

	// ObservedConfigHash records the config hash seen by the controller.
	ObservedConfigHash string `json:"observedConfigHash,omitempty"`

	// LastTransitionTime records the last swarm status transition.
	LastTransitionTime *metav1.Time `json:"lastTransitionTime,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
//+kubebuilder:printcolumn:name="Metadata",type="integer",JSONPath=".status.metadataNodesReady"
//+kubebuilder:printcolumn:name="Data",type="integer",JSONPath=".status.dataNodesReady"
//+kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// AntflyCluster is the Schema for the antflyclusters API
type AntflyCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec AntflyClusterSpec `json:"spec"`
	// +optional
	Status AntflyClusterStatus `json:"status"`
}

//+kubebuilder:object:root=true

// AntflyClusterList contains a list of AntflyCluster
type AntflyClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []AntflyCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AntflyCluster{}, &AntflyClusterList{})
}
