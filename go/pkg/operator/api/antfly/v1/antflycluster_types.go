package v1

import (
	inferencev1alpha1 "github.com/antflydb/antfly/go/pkg/operator/api/inference/v1alpha1"
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

	// TypeInferencePoolReady indicates whether the operator-managed InferencePool is reconciled
	TypeInferencePoolReady = "InferencePoolReady"

	// TypeMetadataReady indicates whether metadata pods are ready.
	TypeMetadataReady = "MetadataReady"

	// TypeDataReady indicates whether data pods are ready.
	TypeDataReady = "DataReady"

	// TypeStandaloneReady indicates whether standalone pods are ready.
	TypeStandaloneReady = "StandaloneReady"

	// TypeInferenceReady indicates whether inference is ready when managed in standalone mode.
	TypeInferenceReady = "InferenceReady"

	// TypeHAAvailable indicates whether hot-standby HA has an available standby.
	TypeHAAvailable = "HAAvailable"

	// TypeHADegraded indicates whether the configured HA durability policy is degraded.
	TypeHADegraded = "HADegraded"

	// TypeHAUnhealthy indicates whether one or more desired HA standbys are unhealthy.
	TypeHAUnhealthy = "HAUnhealthy"

	// TypeHALagging indicates whether one or more desired HA standbys are lagging.
	TypeHALagging = "HALagging"

	// TypeHARetentionPressure indicates whether HA slots are forcing WAL retention beyond policy.
	TypeHARetentionPressure = "HARetentionPressure"

	// TypeHAReseedRequired indicates whether one or more HA standbys require reseeding.
	TypeHAReseedRequired = "HAReseedRequired"

	// TypeHAAutomaticFailoverReady indicates whether fenced automatic promotion is allowed.
	TypeHAAutomaticFailoverReady = "HAAutomaticFailoverReady"

	// TypeAvailable indicates whether the cluster is serving.
	TypeAvailable = "Available"

	// ReasonInferencePoolReady indicates the managed InferencePool reconcile completed
	ReasonInferencePoolReady = "InferencePoolReady"

	// ReasonInferencePoolNameConflict indicates a same-name InferencePool is not owned by the cluster
	ReasonInferencePoolNameConflict = "InferencePoolNameConflict"

	// ReasonInferencePoolManagementDisabled indicates inference pool management is disabled by operator flag
	ReasonInferencePoolManagementDisabled = "InferencePoolManagementDisabled"

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

	// ReasonRolloutBlocked indicates StatefulSet changes are blocked by stale unhealthy pods
	ReasonRolloutBlocked = "RolloutBlocked"

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

	// ReasonComponentReady indicates a cluster component has enough ready replicas.
	ReasonComponentReady = "ComponentReady"

	// ReasonWaitingForPods indicates a component is waiting for ready pods.
	ReasonWaitingForPods = "WaitingForPods"

	// ReasonRuntimeDegraded indicates pod diagnostics found a runtime failure.
	ReasonRuntimeDegraded = "RuntimeDegraded"

	// ReasonUnschedulable indicates pods cannot be scheduled.
	ReasonUnschedulable = "Unschedulable"

	// ReasonImagePullFailed indicates a container image could not be pulled.
	ReasonImagePullFailed = "ImagePullFailed"

	// ReasonCrashLooping indicates a container is crashlooping.
	ReasonCrashLooping = "CrashLooping"

	// ReasonProbeFailed indicates a probe failure is preventing readiness.
	ReasonProbeFailed = "ProbeFailed"

	// ReasonAvailable indicates the cluster is available.
	ReasonAvailable = "Available"

	// ReasonHADisabled indicates hot-standby HA is disabled.
	ReasonHADisabled = "HADisabled"

	// ReasonHAStandbyAvailable indicates at least one standby is caught up enough to serve.
	ReasonHAStandbyAvailable = "HAStandbyAvailable"

	// ReasonHANoHealthyStandby indicates no desired standby is currently healthy.
	ReasonHANoHealthyStandby = "HANoHealthyStandby"

	// ReasonHASyncPolicyUnsatisfied indicates synchronous HA durability is not satisfied.
	ReasonHASyncPolicyUnsatisfied = "HASyncPolicyUnsatisfied"

	// ReasonHASyncPolicySatisfied indicates synchronous HA durability is satisfied or not configured.
	ReasonHASyncPolicySatisfied = "HASyncPolicySatisfied"

	// ReasonHAPrimaryAdminUnavailable indicates the primary HA admin endpoint could not be observed.
	ReasonHAPrimaryAdminUnavailable = "HAPrimaryAdminUnavailable"

	// ReasonHAAdminStatusUnavailable indicates one or more HA admin status endpoints could not be observed.
	ReasonHAAdminStatusUnavailable = "HAAdminStatusUnavailable"

	// ReasonHAAdminJobFailed indicates an HA admin execution failed.
	ReasonHAAdminJobFailed = "HAAdminJobFailed"

	// ReasonHAAdminUnauthorized indicates an HA admin execution was rejected by authentication.
	ReasonHAAdminUnauthorized = "HAAdminUnauthorized"

	// ReasonHAAdminActionRetrying indicates an HA admin action hit a retryable error and will be retried.
	ReasonHAAdminActionRetrying = "HAAdminActionRetrying"

	// ReasonHAAdminRetryBudgetExhausted indicates an exact HA action reached its bounded retry limit.
	ReasonHAAdminRetryBudgetExhausted = "HAAdminRetryBudgetExhausted"

	// ReasonHAStandbyUnhealthy indicates at least one desired standby is unhealthy.
	ReasonHAStandbyUnhealthy = "HAStandbyUnhealthy"

	// ReasonHAStandbysHealthy indicates desired standbys are healthy.
	ReasonHAStandbysHealthy = "HAStandbysHealthy"

	// ReasonHAStandbyLagging indicates at least one desired standby has replication lag.
	ReasonHAStandbyLagging = "HAStandbyLagging"

	// ReasonHANoLaggingStandbys indicates no desired standby has replication lag.
	ReasonHANoLaggingStandbys = "HANoLaggingStandbys"

	// ReasonHARetentionCapExceeded indicates HA WAL retention exceeds policy.
	ReasonHARetentionCapExceeded = "HARetentionCapExceeded"

	// ReasonHARetentionWithinPolicy indicates HA WAL retention is within policy.
	ReasonHARetentionWithinPolicy = "HARetentionWithinPolicy"

	// ReasonHAStandbyRequiresReseed indicates at least one desired standby requires reseed.
	ReasonHAStandbyRequiresReseed = "HAStandbyRequiresReseed"

	// ReasonHANoReseedRequired indicates no desired standby requires reseed.
	ReasonHANoReseedRequired = "HANoReseedRequired"

	// ReasonHAFencedPromotionReady indicates automatic failover can perform a fenced promotion.
	ReasonHAFencedPromotionReady = "HAFencedPromotionReady"

	// ReasonHAAutomaticFailoverDisabled indicates automatic failover is disabled.
	ReasonHAAutomaticFailoverDisabled = "HAAutomaticFailoverDisabled"

	// ReasonHAAutomaticFailoverExecutionDisabled indicates automatic failover cannot execute planned admin actions.
	ReasonHAAutomaticFailoverExecutionDisabled = "HAAutomaticFailoverExecutionDisabled"

	// ReasonHAFencingAuthorityMissing indicates automatic failover lacks fencing.
	ReasonHAFencingAuthorityMissing = "HAFencingAuthorityMissing"

	// ReasonHAFencingAuthorityUnsupported indicates automatic failover is configured with a fencing authority this operator cannot manage.
	ReasonHAFencingAuthorityUnsupported = "HAFencingAuthorityUnsupported"

	// ReasonHAFencingNotReady indicates automatic failover lacks an observed ready fence.
	ReasonHAFencingNotReady = "HAFencingNotReady"

	// ReasonHAPrimaryStillReachable indicates automatic failover is blocked while the primary admin endpoint is reachable.
	ReasonHAPrimaryStillReachable = "HAPrimaryStillReachable"

	// ReasonHAPromotionBoundaryMissing indicates automatic failover lacks an observed primary LSN boundary.
	ReasonHAPromotionBoundaryMissing = "HAPromotionBoundaryMissing"

	// ReasonHAPromotionAlreadyRecorded indicates automatic failover already recorded a promotion for this identity.
	ReasonHAPromotionAlreadyRecorded = "HAPromotionAlreadyRecorded"

	// ReasonHAPromotionReceiptMissing indicates a completed promotion lacks the receipt needed for fenced rejoin.
	ReasonHAPromotionReceiptMissing = "HAPromotionReceiptMissing"

	// ReasonHAAdminResultMissing indicates a completed HA admin action lacks typed result evidence.
	ReasonHAAdminResultMissing = "HAAdminResultMissing"

	// ReasonHAAdminURLMissing indicates an executable HA admin action lacks a target admin endpoint.
	ReasonHAAdminURLMissing = "HAAdminURLMissing"

	// ReasonHAPrimaryRouteSelectorMissing indicates the promoted standby lacks a public-api route selector.
	ReasonHAPrimaryRouteSelectorMissing = "HAPrimaryRouteSelectorMissing"

	// DataScaleDownSourceManual indicates the scale-down target came from spec.dataNodes.replicas.
	DataScaleDownSourceManual = "Manual"

	// DataScaleDownSourceAutoscaler indicates the scale-down target came from the operator autoscaler.
	DataScaleDownSourceAutoscaler = "Autoscaler"

	// FinalizerPVCCleanup is the finalizer used for PVC cleanup on cluster deletion
	FinalizerPVCCleanup = "antfly.io/pvc-cleanup"
)

// ClusterMode selects the topology managed by the operator.
type ClusterMode string

const (
	// ClusterModeDistributed is the existing split metadata/data topology.
	ClusterModeDistributed ClusterMode = "Distributed"

	// ClusterModeStandalone is the single-node operator-managed standalone topology.
	ClusterModeStandalone ClusterMode = "Standalone"

	// ClusterModeSwarm is retained during the one-way migration from the legacy
	// single-node resource layout to the Standalone API/runtime.
	ClusterModeSwarm ClusterMode = "Swarm"
)

// AntflyClusterSpec defines the desired state of AntflyCluster
// +kubebuilder:validation:XValidation:rule="(has(self.metadataNodes) ? (has(self.metadataNodes.replicas) ? self.metadataNodes.replicas : 3) : 3) == (has(oldSelf.metadataNodes) ? (has(oldSelf.metadataNodes.replicas) ? oldSelf.metadataNodes.replicas : 3) : 3)",message="metadata replica count is immutable; create a differently named cluster with fresh metadata PVCs at the target topology and restore from backup"
type AntflyClusterSpec struct {
	// Mode selects the runtime topology managed by the operator.
	// +optional
	// +kubebuilder:validation:Enum=Distributed;Standalone;Swarm
	// +kubebuilder:default=Distributed
	Mode ClusterMode `json:"mode,omitempty"`

	// Image is the container image to use for Antfly
	Image string `json:"image"`

	// ImagePullPolicy defines the image pull policy
	ImagePullPolicy string `json:"imagePullPolicy,omitempty"`

	// Standalone defines the single-node standalone topology when Mode=Standalone.
	// +optional
	Standalone *StandaloneSpec `json:"standalone,omitempty"`

	// Swarm is the deprecated single-node shape retained only for rolling
	// compatibility. It is normalized to Standalone with LegacySwarmV1 layout.
	// +optional
	Swarm *SwarmSpec `json:"swarm,omitempty"`

	// Inference configures inference pools used by this cluster.
	// Pools may be owned by this cluster, referenced as customer-managed shared
	// pools, or referenced as platform-managed shared pools.
	// +optional
	Inference *AntflyInferenceSpec `json:"inference,omitempty"`

	// HighAvailability configures Postgres-style hot-standby HA for the Zig runtime.
	// This mode is separate from Raft-backed distributed write ownership.
	// +optional
	HighAvailability *HighAvailabilitySpec `json:"highAvailability,omitempty"`

	// ProductTier records the CloudAF/product tier intent that was expanded
	// into the explicit operator fields below. The operator does not resolve
	// prices or tier catalogs; it validates that a stamped tier has concrete
	// resources, storage, and autoscaling intent in the normal fields.
	// +optional
	ProductTier *ProductTierSpec `json:"productTier,omitempty"`

	// MetadataNodes defines the configuration for metadata nodes (StatefulSet).
	// Required for Distributed mode and must be omitted in Standalone mode.
	// +optional
	MetadataNodes MetadataNodesSpec `json:"metadataNodes,omitempty"`

	// DataNodes defines the configuration for data nodes (StatefulSet).
	// Required for Distributed mode and must be omitted in Standalone mode.
	// +optional
	DataNodes DataNodesSpec `json:"dataNodes,omitempty"`

	// Config is the configuration file content for Antfly
	Config string `json:"config"`

	// SecretStore mounts a Kubernetes Secret containing an Antfly secrets.json
	// file for runtime secret resolution.
	// +optional
	SecretStore *SecretStoreSpec `json:"secretStore,omitempty"`

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

// SecretStoreSpec configures a mounted Antfly secrets.json file.
type SecretStoreSpec struct {
	// SecretName is the Kubernetes Secret name in the AntflyCluster namespace.
	SecretName string `json:"secretName"`

	// Key is the Secret data key containing the Antfly secrets file.
	// Defaults to secrets.json.
	// +optional
	Key string `json:"key,omitempty"`

	// Path is the absolute file path where the secret file should be mounted.
	// Defaults to /run/antfly/secrets/secrets.json.
	// +optional
	Path string `json:"path,omitempty"`
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

	// StandaloneTier optionally records the standalone sub-tier name when Mode=Standalone.
	// +optional
	StandaloneTier string `json:"standaloneTier,omitempty"`

	// MetadataTier optionally records the metadata-node sub-tier name when
	// Mode=Distributed.
	// +optional
	MetadataTier string `json:"metadataTier,omitempty"`

	// DataTier optionally records the data-node sub-tier name when
	// Mode=Distributed.
	// +optional
	DataTier string `json:"dataTier,omitempty"`

	// InferenceTier optionally records the InferencePool sub-tier name when
	// spec.inference is set.
	// +optional
	InferenceTier string `json:"inferenceTier,omitempty"`
}

// AntflyInferenceMode selects how an AntflyCluster uses Inference inference pools.
type AntflyInferenceMode string

const (
	// AntflyInferenceModeDisabled disables cluster-level inference integration.
	AntflyInferenceModeDisabled AntflyInferenceMode = "Disabled"

	// AntflyInferenceModePlatformShared uses platform-operated shared inference pools.
	AntflyInferenceModePlatformShared AntflyInferenceMode = "PlatformShared"

	// AntflyInferenceModeManaged creates InferencePools owned by this AntflyCluster.
	AntflyInferenceModeManaged AntflyInferenceMode = "Managed"

	// AntflyInferenceModeSharedRef uses existing customer-managed InferencePools.
	AntflyInferenceModeSharedRef AntflyInferenceMode = "SharedRef"
)

// AntflyInferenceSpec configures Inference inference pools for an AntflyCluster.
type AntflyInferenceSpec struct {
	// Mode selects how Inference pools are provided.
	// +kubebuilder:validation:Enum=Disabled;PlatformShared;Managed;SharedRef
	// +kubebuilder:default=Managed
	// +optional
	Mode AntflyInferenceMode `json:"mode,omitempty"`

	// ManagedPools are InferencePools created and owned by this AntflyCluster.
	// Valid when mode is Managed.
	// +optional
	ManagedPools []ManagedInferencePoolSpec `json:"managedPools,omitempty"`

	// SharedPools references existing customer-managed InferencePools.
	// Valid when mode is SharedRef.
	// +optional
	SharedPools []InferencePoolReference `json:"sharedPools,omitempty"`

	// PlatformPools references platform-operated shared InferencePools.
	// Valid when mode is PlatformShared.
	// +optional
	PlatformPools []InferencePoolReference `json:"platformPools,omitempty"`
}

// ManagedInferencePoolSpec describes one InferencePool owned by an AntflyCluster.
type ManagedInferencePoolSpec struct {
	// Name is the child InferencePool name. If omitted for a single managed pool,
	// the operator uses "<antflycluster-name>-inference".
	// +optional
	Name string `json:"name,omitempty"`

	// Spec is the InferencePool spec to apply to the child pool.
	Spec inferencev1alpha1.InferencePoolSpec `json:"spec"`
}

// InferencePoolReference points at an existing shared InferencePool or service.
type InferencePoolReference struct {
	// Name is the referenced InferencePool or logical platform pool name.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Namespace is the referenced pool namespace. If omitted, the cluster
	// namespace is used.
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// APIURL optionally pins the Inference API URL for this reference. When omitted,
	// higher-level platform wiring may resolve the URL from the referenced pool.
	// +optional
	APIURL string `json:"apiURL,omitempty"`
}

// HAMode selects the HA strategy managed by the operator.
type HAMode string

const (
	// HAModeDisabled disables hot-standby HA management.
	HAModeDisabled HAMode = "Disabled"

	// HAModeHotStandby enables single-primary hot standby WAL replication.
	HAModeHotStandby HAMode = "HotStandby"
)

// HADurabilityMode selects when a primary write may be acknowledged.
type HADurabilityMode string

const (
	HADurabilityModeAsync       HADurabilityMode = "Async"
	HADurabilityModeRemoteWrite HADurabilityMode = "RemoteWrite"
	HADurabilityModeRemoteApply HADurabilityMode = "RemoteApply"
)

// HAStandbySelection selects which named standbys satisfy synchronous commit.
type HAStandbySelection string

const (
	HAStandbySelectionAny   HAStandbySelection = "Any"
	HAStandbySelectionFirst HAStandbySelection = "First"
	HAStandbySelectionAll   HAStandbySelection = "All"
)

// HAFailurePolicy selects what happens when synchronous durability cannot be met.
type HAFailurePolicy string

const (
	HAFailurePolicyBlock          HAFailurePolicy = "Block"
	HAFailurePolicyFailClosed     HAFailurePolicy = "FailClosed"
	HAFailurePolicyDegradeToAsync HAFailurePolicy = "DegradeToAsync"
)

// HAFencingAuthority selects the authority used before automatic promotion.
type HAFencingAuthority string

const (
	HAFencingAuthorityNone            HAFencingAuthority = "None"
	HAFencingAuthorityKubernetesLease HAFencingAuthority = "KubernetesLease"
	HAFencingAuthorityStorageFence    HAFencingAuthority = "StorageFence"
	HAFencingAuthorityMetadataRaft    HAFencingAuthority = "MetadataRaft"
	HAFencingAuthorityExternal        HAFencingAuthority = "External"
)

// HARuntimeRole selects how this Antfly process opens the HA runtime.
type HARuntimeRole string

const (
	// HARuntimeRolePrimary opens the local process as the HA primary.
	HARuntimeRolePrimary HARuntimeRole = "Primary"

	// HARuntimeRoleStandby opens the local process as an HA standby.
	HARuntimeRoleStandby HARuntimeRole = "Standby"
)

// HAStartupGatePolicy selects the fail-closed runtime startup policy.
type HAStartupGatePolicy string

const (
	// HAStartupGatePolicySuspend is an explicit declarative hold used while a
	// demoted/former-primary runtime awaits exact rewind or reseed repair.
	HAStartupGatePolicySuspend HAStartupGatePolicy = "Suspend"
	// HAStartupGatePolicyRequireActivatedSeed keeps the runtime offline until the
	// operator has observed an exact, durable target-volume activation receipt.
	HAStartupGatePolicyRequireActivatedSeed HAStartupGatePolicy = "RequireActivatedSeed"
)

// HAReceiptMatchPolicy selects how startup receipt evidence is compared.
type HAReceiptMatchPolicy string

const (
	// HAReceiptMatchPolicyExact requires every configured identity and optional
	// digest/PVC UID field to match exactly.
	HAReceiptMatchPolicyExact HAReceiptMatchPolicy = "Exact"
)

// HighAvailabilitySpec configures hot-standby HA for an AntflyCluster.
type HighAvailabilitySpec struct {
	// Mode selects whether hot standby is managed.
	// +kubebuilder:validation:Enum=Disabled;HotStandby
	// +kubebuilder:default=Disabled
	// +optional
	Mode HAMode `json:"mode,omitempty"`

	// Standbys declares desired standby replicas and replication slots.
	// +optional
	Standbys []HAStandbySpec `json:"standbys,omitempty"`

	// Identity identifies the replicated HA unit for admin/fencing commands.
	// +optional
	Identity *HAReplicationIdentitySpec `json:"identity,omitempty"`

	// Admin configures HA admin endpoints and optional operator execution.
	// +optional
	Admin *HAAdminSpec `json:"admin,omitempty"`

	// Runtime configures the local Zig antfly standalone HA runtime role and durable WAL paths.
	// Supported only when spec.mode=Standalone; split metadata/data topology HA process wiring is not yet modeled here.
	// +optional
	Runtime *HARuntimeSpec `json:"runtime,omitempty"`

	// SyncPolicy configures async, remote-write, or remote-apply durability.
	// +optional
	SyncPolicy *HASyncPolicy `json:"syncPolicy,omitempty"`

	// Retention caps WAL retention pressure from replication slots.
	// +optional
	Retention *HARetentionPolicy `json:"retention,omitempty"`

	// AutomaticFailover configures fenced automatic promotion.
	// +optional
	AutomaticFailover *HAAutomaticFailoverPolicy `json:"automaticFailover,omitempty"`
}

// HAStandbySpec declares one desired hot standby.
type HAStandbySpec struct {
	// Name is the logical standby and default replication slot name.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// SlotName overrides the replication slot name. Defaults to name.
	// +optional
	SlotName string `json:"slotName,omitempty"`

	// Desired controls whether the operator should keep this standby attached.
	// +kubebuilder:default=true
	// +optional
	Desired *bool `json:"desired,omitempty"`

	// DropSlotOnRemoval lets the operator drop this standby's replication slot after desired is set false.
	// This releases WAL retention and is destructive for the standby; keep false to pause the slot instead.
	// +optional
	DropSlotOnRemoval bool `json:"dropSlotOnRemoval,omitempty"`

	// InitialLSN optionally pins slot creation to an existing primary LSN.
	// +optional
	InitialLSN *uint64 `json:"initialLSN,omitempty"`

	// AdminURL is the standby HA admin endpoint used for typed status and promotion actions.
	// +optional
	AdminURL string `json:"adminURL,omitempty"`

	// SeedManifestPath is the base-backup manifest path visible to CLI-backed HA admin Jobs.
	// When set, the operator can run seed finish on the primary and seed bootstrap on this standby.
	// +optional
	SeedManifestPath string `json:"seedManifestPath,omitempty"`

	// SeedContentRoot is the copied base-backup content root visible to the standby CLI-backed HA admin Job.
	// Defaults to the manifest parent directory when omitted.
	// +optional
	SeedContentRoot string `json:"seedContentRoot,omitempty"`

	// SeedArtifact enables operator-managed immutable transport for this standby's
	// base backup. The operator publishes the source manifest and all referenced
	// files to object storage, restores them into a target staging root, verifies
	// identity and checksums, and only then invokes standby bootstrap.
	// +optional
	SeedArtifact *HASeedArtifactSpec `json:"seedArtifact,omitempty"`

	// RouteSelector is the public-api Service selector to use when this standby is promoted.
	// +optional
	RouteSelector map[string]string `json:"routeSelector,omitempty"`
}

// HASeedArtifactSpec configures portable, publish-last seed transport.
type HASeedArtifactSpec struct {
	// Location is an object-store URI. Production deployments should use s3://
	// or gs://; file:// is supported for local development and KinD fixtures.
	// +kubebuilder:validation:Pattern=`^(s3://|gs://|file://).+`
	Location string `json:"location"`

	// Generation is the exact immutable generation shared by capture, publish,
	// restore, activation, startup gating, and restart. Production startup gates
	// require this field; it is never recomputed from mutable observed LSN state.
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_.:-]+$`
	// +optional
	Generation string `json:"generation,omitempty"`

	// TopologyID and TopologyGeneration identify the Colony topology revision
	// that authorized this entire seed chain. The operator freezes both into
	// every planned artifact action and refuses execution when they are absent.
	// +optional
	TopologyID string `json:"topologyID,omitempty"`
	// +optional
	// +kubebuilder:validation:Minimum=1
	TopologyGeneration int64 `json:"topologyGeneration,omitempty"`

	// NodeID is the exact target standby identity carried by the portable
	// artifact and checked again before restore and activation.
	// +optional
	NodeID string `json:"nodeID,omitempty"`

	// TargetPVCUID optionally pins the desired target claim incarnation. The
	// controller always resolves the live UID and rejects a configured mismatch.
	// +optional
	TargetPVCUID string `json:"targetPVCUID,omitempty"`

	// GenerationPrefix is prepended to the deterministic generation selected by
	// the operator. It must be a safe HA identifier and defaults to "seed".
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_.:-]+$`
	// +optional
	GenerationPrefix string `json:"generationPrefix,omitempty"`

	// StagingRoot is the absolute target path into which the verified generation
	// is restored. It must be on target standby durable storage and must not be a
	// live primary data root.
	// +kubebuilder:validation:Pattern=`^/.*`
	StagingRoot string `json:"stagingRoot"`

	// SourcePVC is mounted read-only only by the publish Job. Keeping source and
	// target claims action-scoped allows an RWO primary PVC and an RWO standby
	// PVC to be attached on different nodes without one Job requesting both.
	// +optional
	SourcePVC *HASeedArtifactPVCSpec `json:"sourcePVC,omitempty"`

	// TargetPVC is mounted read-write only by the restore Job.
	// +optional
	TargetPVC *HASeedArtifactPVCSpec `json:"targetPVC,omitempty"`

	// CredentialsSecretRef injects object-store credentials into artifact Jobs.
	// For S3 this Secret normally contains AWS_ACCESS_KEY_ID,
	// AWS_SECRET_ACCESS_KEY, and optionally AWS_REGION/AWS_ENDPOINT_URL.
	// Workload identity may be used by omitting this field.
	// +optional
	CredentialsSecretRef *corev1.LocalObjectReference `json:"credentialsSecretRef,omitempty"`

	// RetainGenerations is the desired number of complete immutable generations
	// to retain after a newer seed becomes ready. Zero defaults to two.
	// +kubebuilder:validation:Minimum=1
	// +optional
	RetainGenerations int32 `json:"retainGenerations,omitempty"`
}

// HASeedArtifactPVCSpec mounts one seed source or target PVC into an artifact Job.
type HASeedArtifactPVCSpec struct {
	// ClaimName is the PVC in the AntflyCluster namespace.
	ClaimName string `json:"claimName"`

	// MountPath is the absolute path used by the corresponding artifact action.
	MountPath string `json:"mountPath"`
}

// HARuntimeSpec configures how the operator starts this Antfly process in the HA runtime.
// It is only supported when spec.mode=Standalone because those flags are currently wired through the Standalone StatefulSet.
type HARuntimeSpec struct {
	// Role selects whether this process opens primary or standby HA runtime state.
	// +kubebuilder:validation:Enum=Primary;Standby
	Role HARuntimeRole `json:"role"`

	// NodeID is the logical HA node id used in typed admin receipts and fencing.
	// +kubebuilder:validation:MinLength=1
	NodeID string `json:"nodeID"`

	// FencingLease configures the single topology-stable Kubernetes Lease that
	// every runtime watches before promotion. Only the topology anchor operator
	// may renew or transfer it; runtime ServiceAccounts receive exact read-only
	// access. This reference must be copied unchanged to every standby CR.
	// +optional
	FencingLease *HARuntimeFencingLeaseSpec `json:"fencingLease,omitempty"`

	// FencePath is the durable promotion fence WAL path shared by HA admin fence and promotion operations.
	// Defaults to /antflydb/ha/fence.wal.
	// +optional
	FencePath string `json:"fencePath,omitempty"`

	// FormerPrimaryLogPath is the durable HA replication log used by former-primary rewind admin workflows.
	// Set this on nodes that may need to rejoin after failover; for a primary this is usually the same path
	// as primary.logPath, and the Standalone runtime wiring defaults it to primary.logPath when omitted.
	// +optional
	FormerPrimaryLogPath string `json:"formerPrimaryLogPath,omitempty"`

	// SeedCaptureRoot is the durable node-local root used for immutable,
	// runtime-owned point-in-time seed generations. It is configured on both
	// primary and standby roles so an in-place promoted standby can seed its
	// replacement without restarting. Defaults to /antflydb/ha/seed-captures.
	// +optional
	SeedCaptureRoot string `json:"seedCaptureRoot,omitempty"`

	// StartupGate keeps a standby runtime at zero replicas until the operator has
	// observed an exact activation receipt on its deterministic target PVC.
	// +optional
	StartupGate *HAStartupGateSpec `json:"startupGate,omitempty"`

	// AdminTokenEnvVar is the Antfly process environment variable containing the bearer token required by /admin/v1/ha.
	// When set, the operator passes --admin-token-env and the runtime rejects typed admin requests without a matching Authorization header.
	// Populate it with adminTokenSecretRef or spec.standalone.envFrom for Antfly runtime pods and CLI fallback Jobs.
	// The operator does not read this Secret; operator status probes and typed admin actions use spec.highAvailability.admin.tokenEnvVar.
	// +kubebuilder:validation:Pattern=`^$|^[A-Za-z_][A-Za-z0-9_]*$`
	// +optional
	AdminTokenEnvVar string `json:"adminTokenEnvVar,omitempty"`

	// AdminTokenSecretRef injects AdminTokenEnvVar from a required Secret key into Antfly runtime pods and CLI fallback Jobs.
	// Use this when the token is not already provided by spec.standalone.envFrom. optional must not be true.
	// This Secret is passed as a Kubernetes SecretKeySelector and is not read by the operator process.
	// +optional
	AdminTokenSecretRef *corev1.SecretKeySelector `json:"adminTokenSecretRef,omitempty"`

	// Primary configures primary-side durable HA state. Defaults are under /antflydb/ha.
	// +optional
	Primary *HAPrimaryRuntimeSpec `json:"primary,omitempty"`

	// Standby configures standby-side durable HA state and optional continuous pull source.
	// +optional
	Standby *HAStandbyRuntimeSpec `json:"standby,omitempty"`
}

// HARuntimeFencingLeaseSpec binds one runtime to the shared HA authority.
type HARuntimeFencingLeaseSpec struct {
	// Name is the exact Lease name in the runtime Pod namespace.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// TopologyID is stable across primary handoff and distinct from a local
	// standby AntflyCluster UID. Colony uses the durable Cloud instance ID,
	// which exists before the first CR is created, and propagates it unchanged
	// to every topology member.
	// +kubebuilder:validation:MinLength=1
	TopologyID string `json:"topologyID"`

	// WatchdogGraceSeconds is the maximum API-unreachable interval before the
	// runtime durably self-fences. It must be less than the Lease duration.
	// +kubebuilder:validation:Minimum=10
	// +kubebuilder:validation:Maximum=29
	// +kubebuilder:default=10
	// +optional
	WatchdogGraceSeconds int32 `json:"watchdogGraceSeconds,omitempty"`
}

// HAStartupGateSpec declares the exact receipt required before a runtime may
// start. runtimeEligible is a declarative suspension fence: false always keeps
// replicas at zero, while true remains subject to policy-specific verification.
type HAStartupGateSpec struct {
	// Policy selects either an unconditional Suspend hold or exact activated-seed startup.
	// +kubebuilder:validation:Enum=Suspend;RequireActivatedSeed
	Policy HAStartupGatePolicy `json:"policy"`

	// RuntimeEligible is necessary but never sufficient for startup. False always
	// forces replicas to zero, including during declarative role handoff. True is
	// honored only after the operator-observed policy evidence matches.
	RuntimeEligible bool `json:"runtimeEligible"`

	// ReceiptMatchPolicy must be Exact for RequireActivatedSeed and omitted for Suspend.
	// +kubebuilder:validation:Enum=Exact
	// +optional
	ReceiptMatchPolicy HAReceiptMatchPolicy `json:"receiptMatchPolicy,omitempty"`

	// RequiredReceipt binds the one target volume generation this runtime may open.
	// It is required for RequireActivatedSeed and forbidden for Suspend.
	// +optional
	RequiredReceipt *HARequiredSeedActivationReceipt `json:"requiredReceipt,omitempty"`
}

// HARequiredSeedActivationReceipt is the desired exact startup identity.
type HARequiredSeedActivationReceipt struct {
	TopologyID string `json:"topologyID"`
	// TopologyGeneration optionally rejects a receipt from an older replacement
	// topology generation. When non-zero it is matched exactly.
	// +optional
	TopologyGeneration int64  `json:"topologyGeneration,omitempty"`
	NodeID             string `json:"nodeID"`
	SlotName           string `json:"slotName"`
	Generation         string `json:"generation"`
	TargetPVCName      string `json:"targetPVCName"`

	// Optional exact evidence. When configured, omission or mismatch fails closed.
	// +optional
	ManifestSHA256 string `json:"manifestSHA256,omitempty"`
	// +optional
	AggregateSHA256 string `json:"aggregateSHA256,omitempty"`
	// +optional
	SeedReceiptSHA256 string `json:"seedReceiptSHA256,omitempty"`
	// +optional
	CaptureReceiptSHA256 string `json:"captureReceiptSHA256,omitempty"`
	// +optional
	MaterializedReceiptSHA256 string `json:"materializedReceiptSHA256,omitempty"`
	// +optional
	MaterializedAggregateSHA256 string `json:"materializedAggregateSHA256,omitempty"`
	// +optional
	TargetLocalNodeID uint64 `json:"targetLocalNodeID,omitempty"`
	// +optional
	TargetReplicaID uint64 `json:"targetReplicaID,omitempty"`
	// +optional
	TargetPVCUID string `json:"targetPVCUID,omitempty"`
}

// HAPrimaryRuntimeSpec configures primary-side HA WAL and replication slot state.
type HAPrimaryRuntimeSpec struct {
	// LogPath is the durable HA primary replication log path.
	// Defaults to /antflydb/ha/primary.wal.
	// +optional
	LogPath string `json:"logPath,omitempty"`

	// SlotsPath is the durable HA replication slot store path.
	// Defaults to /antflydb/ha/slots.
	// +optional
	SlotsPath string `json:"slotsPath,omitempty"`
}

// HAStandbyRuntimeSpec configures standby-side HA WAL, progress state, and pull source.
type HAStandbyRuntimeSpec struct {
	// LogPath is the durable received-WAL log path.
	// Defaults to /antflydb/ha/standby.wal.
	// +optional
	LogPath string `json:"logPath,omitempty"`

	// ProgressPath is the durable standby apply progress WAL path.
	// Defaults to /antflydb/ha/standby-progress.wal.
	// +optional
	ProgressPath string `json:"progressPath,omitempty"`

	// UpstreamURL is the primary internal replication base URL for continuous pull/apply.
	// +optional
	UpstreamURL string `json:"upstreamURL,omitempty"`

	// SlotName is the upstream replication slot used by this standby.
	// +optional
	SlotName string `json:"slotName,omitempty"`
}

// HAAdminSpec configures operator access to HA admin endpoints.
type HAAdminSpec struct {
	// PrimaryURL is the primary HA admin endpoint used for typed status, slot, seed, and fence actions.
	// +optional
	PrimaryURL string `json:"primaryURL,omitempty"`

	// ExecutePlannedActions lets the operator execute planned HA actions.
	// The operator prefers typed /admin/v1/ha calls and uses CLI-backed Kubernetes Jobs for pod-local files, shared backup volumes, or break-glass workflows.
	// +optional
	ExecutePlannedActions bool `json:"executePlannedActions,omitempty"`

	// TokenEnvVar is the operator process environment variable containing the bearer token for typed /admin/v1/ha calls.
	// This lets Kubernetes inject the token from a Secret into the operator pod without granting the operator Secret read permissions.
	// Defaults to ANTFLY_HA_ADMIN_TOKEN when omitted and that environment variable is set.
	// CLI-backed HA admin Jobs pass --ha-token-env only when this field is explicitly set; inject the same variable into those Jobs with envFrom when authenticated CLI fallback is needed.
	// +kubebuilder:validation:Pattern=`^$|^[A-Za-z_][A-Za-z0-9_]*$`
	// +optional
	TokenEnvVar string `json:"tokenEnvVar,omitempty"`

	// JobBackoffLimit is the CLI-backed HA admin Job retry count before Kubernetes marks it failed.
	// +optional
	JobBackoffLimit *int32 `json:"jobBackoffLimit,omitempty"`

	// JobTimeoutSeconds is the CLI-backed HA admin Job active deadline.
	// +optional
	JobTimeoutSeconds *int64 `json:"jobTimeoutSeconds,omitempty"`

	// JobTTLSecondsAfterFinished controls how long completed CLI-backed HA admin Jobs are retained
	// after their terminal evidence has been checkpointed in AntflyCluster status.
	// +optional
	JobTTLSecondsAfterFinished *int32 `json:"jobTTLSecondsAfterFinished,omitempty"`

	// DirectRetryLimit bounds retryable typed HA admin failures and expired uncertain
	// reservations charged to the action's retry budget. Successful prerequisite
	// polls do not consume it. Once exhausted, the action becomes terminally Failed
	// until its semantic identity changes or RetryGeneration is incremented. Defaults to 8.
	// +kubebuilder:validation:Minimum=1
	// +optional
	DirectRetryLimit *int32 `json:"directRetryLimit,omitempty"`

	// DirectRetryBaseSeconds is the initial typed HA admin retry delay. Backoff is
	// exponential and persisted in action status. Defaults to 5 seconds.
	// +kubebuilder:validation:Minimum=1
	// +optional
	DirectRetryBaseSeconds *int32 `json:"directRetryBaseSeconds,omitempty"`

	// DirectRetryMaxSeconds caps the typed HA admin retry delay. Defaults to 120 seconds.
	// +kubebuilder:validation:Minimum=1
	// +optional
	DirectRetryMaxSeconds *int32 `json:"directRetryMaxSeconds,omitempty"`

	// DirectReservationSeconds bounds an in-flight typed HA admin attempt reservation.
	// After an operator crash, the exact frozen request may be replayed only after this
	// lease expires; runtime HA actions are required to return idempotent receipts.
	// Defaults to 30 seconds.
	// +kubebuilder:validation:Minimum=1
	// +optional
	DirectReservationSeconds *int32 `json:"directReservationSeconds,omitempty"`

	// DirectPrerequisiteTimeoutSeconds bounds a successful typed assessment that
	// is waiting for a promotion candidate to apply its frozen boundary. Defaults
	// to 600 seconds and is independent of the request-failure retry budget.
	// +kubebuilder:validation:Minimum=1
	// +optional
	DirectPrerequisiteTimeoutSeconds *int32 `json:"directPrerequisiteTimeoutSeconds,omitempty"`

	// RetryGeneration is an operator-controlled recovery nonce. Incrementing it
	// intentionally creates a new execution identity for terminal HA actions while
	// leaving ordinary observation changes unable to reset the retry budget. It is
	// monotonic and the admission webhook rejects decreases.
	// +kubebuilder:validation:Minimum=0
	// +optional
	RetryGeneration int64 `json:"retryGeneration,omitempty"`

	// EnvFrom is applied to CLI-backed HA admin Job containers, commonly for backup object-store credentials.
	// +optional
	EnvFrom []corev1.EnvFromSource `json:"envFrom,omitempty"`

	// Volumes are added to CLI-backed HA admin Job pods, commonly for shared base-backup manifests and contents.
	// +optional
	Volumes []corev1.Volume `json:"volumes,omitempty"`

	// VolumeMounts are added to the CLI-backed HA admin Job container.
	// +optional
	VolumeMounts []corev1.VolumeMount `json:"volumeMounts,omitempty"`
}

// HAReplicationIdentitySpec identifies one replicated HA unit.
type HAReplicationIdentitySpec struct {
	// ClusterID is the stable replicated cluster identifier.
	// +optional
	ClusterID uint64 `json:"clusterID,omitempty"`

	// ShardID is the replicated shard identifier. Zero means whole-instance or default shard scope.
	// +optional
	ShardID uint64 `json:"shardID,omitempty"`

	// TableID is the replicated table identifier. Zero means whole-instance or default table scope.
	// +optional
	TableID uint64 `json:"tableID,omitempty"`

	// TimelineID is the current primary timeline.
	// +optional
	TimelineID uint64 `json:"timelineID,omitempty"`

	// Epoch is the current primary epoch.
	// +optional
	Epoch uint64 `json:"epoch,omitempty"`

	// CurrentPrimaryID is the logical id of the current primary.
	// +optional
	CurrentPrimaryID string `json:"currentPrimaryID,omitempty"`
}

// HASyncPolicy configures synchronous standby durability.
type HASyncPolicy struct {
	// Mode selects async, remote-write, or remote-apply acknowledgement.
	// +kubebuilder:validation:Enum=Async;RemoteWrite;RemoteApply
	// +kubebuilder:default=Async
	// +optional
	Mode HADurabilityMode `json:"mode,omitempty"`

	// Selection chooses how named standbys satisfy the policy.
	// +kubebuilder:validation:Enum=Any;First;All
	// +kubebuilder:default=Any
	// +optional
	Selection HAStandbySelection `json:"selection,omitempty"`

	// Required is the number of standbys needed for Any/First policies.
	// +kubebuilder:validation:Minimum=1
	// +optional
	Required int32 `json:"required,omitempty"`

	// StandbyNames are the synchronous standby names in priority order.
	// +optional
	StandbyNames []string `json:"standbyNames,omitempty"`

	// FailurePolicy controls writes when synchronous durability is unavailable.
	// +kubebuilder:validation:Enum=Block;FailClosed;DegradeToAsync
	// +kubebuilder:default=Block
	// +optional
	FailurePolicy HAFailurePolicy `json:"failurePolicy,omitempty"`
}

// HARetentionPolicy caps WAL retained for HA slots.
type HARetentionPolicy struct {
	// MaxLagLSN marks standbys for reseed after this many retained LSNs.
	// Zero disables the cap.
	// +optional
	MaxLagLSN uint64 `json:"maxLagLSN,omitempty"`

	// MaxRetainedBytes marks oldest standbys for reseed when retained WAL bytes exceed this cap.
	// Zero disables the cap.
	// +optional
	MaxRetainedBytes uint64 `json:"maxRetainedBytes,omitempty"`

	// MaxRetainedAgeNS marks oldest standbys for reseed when retained WAL timestamp span exceeds this cap.
	// Zero disables the cap.
	// +optional
	MaxRetainedAgeNS uint64 `json:"maxRetainedAgeNS,omitempty"`
}

// HAAutomaticFailoverPolicy configures automatic standby promotion.
type HAAutomaticFailoverPolicy struct {
	// Enabled allows the operator to promote a standby automatically.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// FencingAuthority must be set to a concrete authority before automatic promotion.
	// +kubebuilder:validation:Enum=None;KubernetesLease;StorageFence;MetadataRaft;External
	// +kubebuilder:default=None
	// +optional
	FencingAuthority HAFencingAuthority `json:"fencingAuthority,omitempty"`

	// RequireRemoteApply requires an applied standby before automatic promotion.
	// +kubebuilder:default=true
	// +optional
	RequireRemoteApply *bool `json:"requireRemoteApply,omitempty"`

	// MaximumLagLSN is the tolerated standby lag for automatic promotion.
	// +optional
	MaximumLagLSN uint64 `json:"maximumLagLSN,omitempty"`

	// MinimumConsecutiveFailures is the number of consecutive primary admin
	// observations that must fail before automatic failover may begin. This
	// prevents one transient transport failure from reserving an irreversible
	// fencing transaction.
	// +kubebuilder:validation:Minimum=2
	// +kubebuilder:default=3
	// +optional
	MinimumConsecutiveFailures int32 `json:"minimumConsecutiveFailures,omitempty"`

	// MinimumUnreachableDurationSeconds is the minimum continuous primary admin
	// outage required before automatic failover may begin. A successful probe
	// resets the outage window and consecutive-failure count.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=3600
	// +kubebuilder:default=30
	// +optional
	MinimumUnreachableDurationSeconds int32 `json:"minimumUnreachableDurationSeconds,omitempty"`
}

// MetadataNodesSpec defines the configuration for metadata nodes
type MetadataNodesSpec struct {
	// Replicas is the number of metadata nodes (default: 3). It is immutable
	// after cluster creation until the operator supports quorum-aware metadata
	// membership changes.
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

	// StartupProbe defines the startup probe budget for metadata node recovery.
	// +optional
	StartupProbe *ProbeConfig `json:"startupProbe,omitempty"`

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
	// Replicas is the number of data nodes. Zero or omitted uses the controller
	// default of 3; use suspend for intentional scale-to-zero.
	Replicas int32 `json:"replicas,omitempty"`

	// Suspend scales data nodes to zero while retaining PVCs. This is a
	// pause/resume operation, not permanent node removal.
	// +optional
	Suspend bool `json:"suspend,omitempty"`

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

	// StartupProbe defines the startup probe budget for data node recovery.
	// +optional
	StartupProbe *ProbeConfig `json:"startupProbe,omitempty"`

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

// StandaloneSpec defines the configuration for operator-managed standalone mode.
type StandaloneSpec struct {
	// ResourceIdentity selects the immutable Kubernetes identity layout for the
	// single-node workload. LegacySwarmV1 preserves pre-Standalone PVC names.
	// +optional
	// +kubebuilder:validation:Enum=StandaloneV1;LegacySwarmV1
	// +kubebuilder:default=StandaloneV1
	ResourceIdentity StandaloneResourceIdentity `json:"resourceIdentity,omitempty"`

	// Replicas is the number of standalone replicas. MVP only supports 1.
	Replicas int32 `json:"replicas,omitempty"`

	// NodeID is the standalone node ID used for local orchestration URLs.
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

	// StartupProbe defines the startup probe budget for standalone node recovery.
	// +optional
	StartupProbe *ProbeConfig `json:"startupProbe,omitempty"`

	// Inference controls the optional inference sidecar runtime integrated into standalone mode.
	// +optional
	Inference *StandaloneInferenceSpec `json:"inference,omitempty"`

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

// SwarmSpec is the deprecated wire-compatible predecessor of StandaloneSpec.
type SwarmSpec = StandaloneSpec

type StandaloneResourceIdentity string

const (
	StandaloneResourceIdentityV1          StandaloneResourceIdentity = "StandaloneV1"
	StandaloneResourceIdentityLegacySwarm StandaloneResourceIdentity = "LegacySwarmV1"
)

// StandaloneInferenceSpec defines inference configuration for standalone mode.
type StandaloneInferenceSpec struct {
	// Enabled controls whether inference runs alongside the standalone node.
	Enabled bool `json:"enabled,omitempty"`

	// APIURL is the inference API URL.
	APIURL string `json:"apiURL,omitempty"`
}

// APISpec defines API configuration
type APISpec struct {
	// Port is the port number (optional, operator sets defaults)
	Port int32 `json:"port,omitempty"`

	// Host is the host to bind to (default: 0.0.0.0)
	Host string `json:"host,omitempty"`
}

// ProbeConfig defines basic Kubernetes probe timing settings.
type ProbeConfig struct {
	// FailureThreshold is the number of failed probes before Kubernetes restarts the container.
	// +optional
	// +kubebuilder:validation:Minimum=1
	FailureThreshold *int32 `json:"failureThreshold,omitempty"`

	// PeriodSeconds is the interval between probe attempts.
	// +optional
	// +kubebuilder:validation:Minimum=1
	PeriodSeconds *int32 `json:"periodSeconds,omitempty"`

	// TimeoutSeconds is the timeout for each probe attempt.
	// +optional
	// +kubebuilder:validation:Minimum=1
	TimeoutSeconds *int32 `json:"timeoutSeconds,omitempty"`
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

	// StandaloneStorage defines storage for the standalone topology.
	// Used when spec.mode=Standalone.
	// +optional
	StandaloneStorage string `json:"standaloneStorage,omitempty"`

	// SwarmStorage is the deprecated name read during rolling migration.
	// +optional
	SwarmStorage string `json:"swarmStorage,omitempty"`

	// Engine selects the persistence engine. Local stores a directory tree on
	// PVCs. Lite stores the complete database in one .aflite file and is valid
	// only with spec.mode=Standalone.
	// +optional
	// +kubebuilder:validation:Enum=local;lite
	// +kubebuilder:default=local
	Engine string `json:"engine,omitempty"`

	// LiteFileName is the basename of the Lite database on the standalone PVC.
	// It must not contain path separators and must end in .aflite.
	// +optional
	LiteFileName string `json:"liteFileName,omitempty"`

	// PVCRetentionPolicy controls what happens to PVCs when the cluster is deleted or scaled down.
	// Maps to StatefulSet's persistentVolumeClaimRetentionPolicy (beta in K8s 1.27, GA in 1.32).
	// On clusters < 1.27, this field is silently ignored by the StatefulSet controller;
	// the finalizer provides a fallback for WhenDeleted=Delete.
	// +optional
	PVCRetentionPolicy *PVCRetentionPolicy `json:"pvcRetentionPolicy,omitempty"`

	// StorageAutoGrow configures operator-owned grow-only disk autoscaling.
	// Distributed mode currently applies this policy only to data PVCs. Standalone
	// mode applies it to the standalone PVC.
	// +optional
	StorageAutoGrow *StorageAutoGrowSpec `json:"storageAutoGrow,omitempty"`
}

// StorageAutoGrowSpec configures automatic grow-only PVC expansion.
type StorageAutoGrowSpec struct {
	// Enabled controls whether the operator automatically grows storage.
	Enabled bool `json:"enabled,omitempty"`

	// MaxDataStorage is the maximum size for distributed data PVC auto-grow.
	MaxDataStorage string `json:"maxDataStorage,omitempty"`

	// MaxStandaloneStorage is the maximum size for standalone PVC auto-grow. If omitted
	// in standalone mode, MaxDataStorage is used as the limit.
	MaxStandaloneStorage string `json:"maxStandaloneStorage,omitempty"`

	// MaxSwarmStorage is the deprecated legacy-layout storage ceiling.
	// +optional
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

	// ConfigPublication identifies the exact non-secret config.json image most
	// recently generated by the operator. Controllers can bind reconciliation
	// acknowledgements to a specific AntflyCluster generation without reading
	// the generated ConfigMap.
	// +optional
	ConfigPublication *ConfigPublicationStatus `json:"configPublication,omitempty"`

	// Conditions represent the current conditions of the cluster
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ReadyReplicas is the total number of ready replicas across the active topology.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// MetadataNodesReady represents the number of ready metadata nodes
	MetadataNodesReady int32 `json:"metadataNodesReady,omitempty"`

	// DataNodesReady represents the number of ready data nodes
	DataNodesReady int32 `json:"dataNodesReady,omitempty"`

	// StandaloneNodesReady represents the number of ready standalone nodes.
	// +optional
	StandaloneNodesReady int32 `json:"standaloneNodesReady,omitempty"`

	// AutoScalingStatus tracks autoscaling state
	AutoScalingStatus *AutoScalingStatus `json:"autoScalingStatus,omitempty"`

	// DataScaleDownStatus tracks the operator-owned data-node scale-down workflow.
	// +optional
	DataScaleDownStatus *DataScaleDownStatus `json:"dataScaleDownStatus,omitempty"`

	// StorageAutoGrowStatus tracks the latest operator-owned storage auto-grow evaluation.
	// +optional
	StorageAutoGrowStatus *StorageAutoGrowStatus `json:"storageAutoGrowStatus,omitempty"`

	// HAStatus reports hot-standby replication and failover state.
	// +optional
	HAStatus *HAStatus `json:"haStatus,omitempty"`

	// ProductTierStatus reports the concrete shape observed for spec.productTier.
	// +optional
	ProductTierStatus *ProductTierStatus `json:"productTierStatus,omitempty"`

	// StandaloneStatus reports standalone-specific operational state.
	// +optional
	StandaloneStatus *StandaloneStatus `json:"standaloneStatus,omitempty"`

	// ServiceMeshStatus reports service mesh operational state
	// +optional
	ServiceMeshStatus *ServiceMeshStatus `json:"serviceMeshStatus,omitempty"`
}

// ConfigPublicationStatus identifies an operator-generated config.json image.
// SHA256 is computed over the complete generated bytes; those bytes contain
// secret references but never Kubernetes Secret values.
type ConfigPublicationStatus struct {
	// ObservedGeneration is the AntflyCluster metadata.generation from which the
	// config image was generated.
	ObservedGeneration int64 `json:"observedGeneration"`

	// SHA256 is the lowercase full SHA-256 of the generated config.json bytes.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	SHA256 string `json:"sha256"`
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
	// Source reports whether the current scale-down step was requested by the
	// manual replica field or by the operator autoscaler.
	// +optional
	Source string `json:"source,omitempty"`

	// FromReplicas is the observed/applied replica count before this scale-down step.
	FromReplicas int32 `json:"fromReplicas,omitempty"`

	// TargetReplicas is the user or autoscaler requested final replica count.
	TargetReplicas int32 `json:"targetReplicas,omitempty"`

	// AppliedReplicas is the replica count applied to the StatefulSet for this step.
	AppliedReplicas int32 `json:"appliedReplicas,omitempty"`

	// DrainingOrdinal is the StatefulSet ordinal selected for this step.
	DrainingOrdinal int32 `json:"drainingOrdinal,omitempty"`

	// DrainingNodeID is the Antfly node ID selected for this step.
	DrainingNodeID string `json:"drainingNodeID,omitempty"`

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

// HAStatus reports hot-standby replication and failover state.
type HAStatus struct {
	// Mode is the observed HA mode.
	// +optional
	Mode HAMode `json:"mode,omitempty"`

	// PrimaryLSN is the current primary replication LSN.
	// +optional
	PrimaryLSN uint64 `json:"primaryLSN,omitempty"`

	// PrimaryAdminReachable reports whether the operator most recently reached the primary HA admin endpoint.
	// Automatic promotion requires this to be false with PrimaryAdminLastError set.
	// +optional
	PrimaryAdminReachable bool `json:"primaryAdminReachable,omitempty"`

	// PrimaryAdminLastError reports the latest primary HA admin observation error.
	// +optional
	PrimaryAdminLastError string `json:"primaryAdminLastError,omitempty"`

	// PrimaryAdminStatusCode records the latest typed /admin/v1 HA status-observation HTTP status code.
	// +optional
	PrimaryAdminStatusCode int `json:"primaryAdminStatusCode,omitempty"`

	// PrimaryAdminConsecutiveFailures is the persisted number of consecutive
	// failed primary admin observations in the current outage window.
	// +optional
	PrimaryAdminConsecutiveFailures int32 `json:"primaryAdminConsecutiveFailures,omitempty"`

	// PrimaryAdminUnreachableSince records the first failed observation in the
	// current uninterrupted outage window.
	// +optional
	PrimaryAdminUnreachableSince *metav1.Time `json:"primaryAdminUnreachableSince,omitempty"`

	// PrimaryAdminFailureThresholdMet reports that both configured automatic
	// failover debounce thresholds have been crossed. Candidate selection must
	// never infer this from one transport error.
	// +optional
	PrimaryAdminFailureThresholdMet bool `json:"primaryAdminFailureThresholdMet,omitempty"`

	// PrimaryWatchdogProof is the last runtime-originated, authenticated proof
	// that the exact primary process had the Kubernetes Lease watchdog active.
	// Active capability without authority is retained only for a same-owner
	// bootstrap/restart Lease renewal and is never authoritative primary health.
	// The operator retains it across an admin outage only for binding to the
	// same live Pod/container incarnation at a later isolation boundary.
	// +optional
	PrimaryWatchdogProof *HAWatchdogProofStatus `json:"primaryWatchdogProof,omitempty"`

	// DesiredStandbyCount is the count requested by spec.highAvailability.
	// +optional
	DesiredStandbyCount int32 `json:"desiredStandbyCount,omitempty"`

	// HealthyStandbyCount is the count of desired standbys caught up to apply.
	// +optional
	HealthyStandbyCount int32 `json:"healthyStandbyCount,omitempty"`

	// UnhealthyStandbyCount is the count of desired standbys missing, inactive, or reporting replication errors.
	// +optional
	UnhealthyStandbyCount int32 `json:"unhealthyStandbyCount,omitempty"`

	// LaggingStandbyCount is the count of desired standbys with non-zero replication lag.
	// +optional
	LaggingStandbyCount int32 `json:"laggingStandbyCount,omitempty"`

	// ReadSafeStandbyCount is the count of desired standbys safe for reads.
	// +optional
	ReadSafeStandbyCount int32 `json:"readSafeStandbyCount,omitempty"`

	// ReseedRequiredCount is the count of desired standbys requiring reseed.
	// +optional
	ReseedRequiredCount int32 `json:"reseedRequiredCount,omitempty"`

	// AutomaticPromotionAllowed reports whether the operator planner allows promotion.
	// +optional
	AutomaticPromotionAllowed bool `json:"automaticPromotionAllowed,omitempty"`

	// Standbys contains per-standby slot state.
	// +optional
	Standbys []HAStandbyStatus `json:"standbys,omitempty"`

	// PlannedActions reports HA reconciliation actions the operator plans to take.
	// +optional
	PlannedActions []HAPlannedActionStatus `json:"plannedActions,omitempty"`

	// PrimaryRoute reports the operator-facing primary endpoint target.
	// +optional
	PrimaryRoute HAPrimaryRouteStatus `json:"primaryRoute,omitempty"`

	// Sync reports the observed synchronous durability policy state.
	// +optional
	Sync HASyncStatus `json:"sync,omitempty"`

	// Fencing reports the observed fencing authority state used for automatic promotion.
	// +optional
	Fencing HAFencingStatus `json:"fencing,omitempty"`

	// Retention summarizes primary WAL retention pressure.
	// +optional
	Retention HARetentionStatus `json:"retention,omitempty"`

	// FormerPrimary reports the old primary's rejoin disposition after promotion.
	// +optional
	FormerPrimary *HAFormerPrimaryStatus `json:"formerPrimary,omitempty"`

	// LastPromotion records the last completed promotion.
	// +optional
	LastPromotion *HAPromotionStatus `json:"lastPromotion,omitempty"`

	// StartupGate is operator-observed state. In particular, activationReceipt
	// never appears in spec and cannot be self-asserted by a caller.
	// +optional
	StartupGate *HAStartupGateStatus `json:"startupGate,omitempty"`
}

// HAWatchdogProofStatus records an authenticated runtime observation. It is not
// itself a fencing receipt: automatic failover additionally binds it to the
// exact Kubernetes Pod/container incarnation and an uncached Lease transfer.
type HAWatchdogProofStatus struct {
	CapabilityVersion int32 `json:"capabilityVersion"`

	Active bool `json:"active"`

	AuthorityGranted bool `json:"authorityGranted"`

	// AuthorityRemainingMS is the conservative operator-side remainder after
	// subtracting the full authenticated admin request RTT and safety margin.
	AuthorityRemainingMS int32 `json:"authorityRemainingMS"`

	LeaseName string `json:"leaseName"`

	LeaseNamespace string `json:"leaseNamespace"`

	TopologyID string `json:"topologyID"`

	LocalNodeID string `json:"localNodeID"`

	ObservedHolderNodeID string `json:"observedHolderNodeID"`

	PodUID string `json:"podUID"`

	ProcessBootID string `json:"processBootID"`

	ObservedLeaseTransitions int32 `json:"observedLeaseTransitions"`

	MaxFenceLatencyMS int32 `json:"maxFenceLatencyMS"`

	ObservedAt metav1.Time `json:"observedAt"`
}

// HAStartupGateStatus reports whether the exact activated target is eligible.
type HAStartupGateStatus struct {
	RuntimeEligible bool   `json:"runtimeEligible"`
	Reason          string `json:"reason,omitempty"`
	// +optional
	ActivationReceipt *HASeedActivationReceiptStatus `json:"activationReceipt,omitempty"`
}

// HASeedActivationReceiptStatus is operator-observed evidence from the
// activation Job and the Kubernetes PVC object it wrote.
type HASeedActivationReceiptStatus struct {
	TopologyID                  string `json:"topologyID"`
	TopologyGeneration          int64  `json:"topologyGeneration,omitempty"`
	NodeID                      string `json:"nodeID"`
	SlotName                    string `json:"slotName"`
	Generation                  string `json:"generation"`
	TargetPVCName               string `json:"targetPVCName"`
	TargetPVCUID                string `json:"targetPVCUID,omitempty"`
	ClusterID                   uint64 `json:"clusterID,omitempty"`
	ShardID                     uint64 `json:"shardID,omitempty"`
	TableID                     uint64 `json:"tableID,omitempty"`
	TimelineID                  uint64 `json:"timelineID,omitempty"`
	Epoch                       uint64 `json:"epoch,omitempty"`
	BackupLSN                   uint64 `json:"backupLSN,omitempty"`
	CheckpointLSN               uint64 `json:"checkpointLSN,omitempty"`
	ManifestID                  string `json:"manifestID,omitempty"`
	ManifestSHA256              string `json:"manifestSHA256,omitempty"`
	AggregateSHA256             string `json:"aggregateSHA256,omitempty"`
	SeedReceiptSHA256           string `json:"seedReceiptSHA256,omitempty"`
	CaptureReceiptSHA256        string `json:"captureReceiptSHA256,omitempty"`
	MaterializedReceiptSHA256   string `json:"materializedReceiptSHA256,omitempty"`
	MaterializedAggregateSHA256 string `json:"materializedAggregateSHA256,omitempty"`
	TargetLocalNodeID           uint64 `json:"targetLocalNodeID,omitempty"`
	TargetReplicaID             uint64 `json:"targetReplicaID,omitempty"`
	GenerationPath              string `json:"generationPath,omitempty"`
	RawGenerationPath           string `json:"rawGenerationPath,omitempty"`
}

// HAStandbyStatus reports one hot standby slot.
type HAStandbyStatus struct {
	Name string `json:"name,omitempty"`

	SlotName string `json:"slotName,omitempty"`

	Active bool `json:"active,omitempty"`

	ReseedRequired bool `json:"reseedRequired,omitempty"`

	TimelineID uint64 `json:"timelineID,omitempty"`

	RestartLSN uint64 `json:"restartLSN,omitempty"`

	ReceivedLSN uint64 `json:"receivedLSN,omitempty"`

	AppliedLSN uint64 `json:"appliedLSN,omitempty"`

	SafeReadLSN uint64 `json:"safeReadLSN,omitempty"`

	UpstreamLSN uint64 `json:"upstreamLSN,omitempty"`

	WriteLagLSN uint64 `json:"writeLagLSN,omitempty"`

	ReceiveLagLSN uint64 `json:"receiveLagLSN,omitempty"`

	ApplyLagLSN uint64 `json:"applyLagLSN,omitempty"`

	SafeReadLagLSN uint64 `json:"safeReadLagLSN,omitempty"`

	UnappliedLSNCount uint64 `json:"unappliedLSNCount,omitempty"`

	CaughtUpToReceived bool `json:"caughtUpToReceived,omitempty"`

	CanServeSafeReads bool `json:"canServeSafeReads,omitempty"`

	Status string `json:"status,omitempty"`

	LastError string `json:"lastError,omitempty"`

	// AdminStatusCode records the latest typed /admin/v1 HA standby status-observation HTTP status code.
	// +optional
	AdminStatusCode int `json:"adminStatusCode,omitempty"`

	LastAttemptNs uint64 `json:"lastAttemptNs,omitempty"`

	LastSuccessNs uint64 `json:"lastSuccessNs,omitempty"`

	ReplicationFailuresTotal uint64 `json:"replicationFailuresTotal,omitempty"`

	// WatchdogProof is authenticated runtime evidence from this exact standby
	// process before any in-place promotion is attempted.
	// +optional
	WatchdogProof *HAWatchdogProofStatus `json:"watchdogProof,omitempty"`
}

// HASyncStatus reports the current synchronous durability policy state.
type HASyncStatus struct {
	Mode HADurabilityMode `json:"mode,omitempty"`

	Selection HAStandbySelection `json:"selection,omitempty"`

	Required int32 `json:"required,omitempty"`

	Satisfied int32 `json:"satisfied,omitempty"`

	Candidates int32 `json:"candidates,omitempty"`

	FailurePolicy HAFailurePolicy `json:"failurePolicy,omitempty"`

	Degraded bool `json:"degraded,omitempty"`

	Action string `json:"action,omitempty"`
}

// HAFencingStatus reports the observed fencing state for automatic promotion.
type HAFencingStatus struct {
	// Authority is the observed fencing authority.
	// +kubebuilder:validation:Enum=None;KubernetesLease;StorageFence;MetadataRaft;External
	// +optional
	Authority HAFencingAuthority `json:"authority,omitempty"`

	// Ready reports whether the fencing authority has been observed and can fence a primary.
	// +optional
	Ready bool `json:"ready,omitempty"`

	// Holder identifies the actor that currently holds or can acquire the fence.
	// +optional
	Holder string `json:"holder,omitempty"`

	// Generation is the observed fencing epoch used to order promotions.
	// +optional
	Generation uint64 `json:"generation,omitempty"`

	// Reason describes the latest fencing readiness decision.
	// +optional
	Reason string `json:"reason,omitempty"`
}

// HAPhysicalIsolationPodIdentity binds a physical-isolation intent to one
// exact former-primary Pod incarnation. Pod names alone are reusable and are
// therefore never sufficient evidence at this safety boundary.
type HAPhysicalIsolationPodIdentity struct {
	Name string `json:"name"`

	UID string `json:"uid"`
}

// HAPhysicalIsolationLeaseScope records the complete topology scope frozen in
// the Kubernetes fencing Lease at holder transfer.
type HAPhysicalIsolationLeaseScope struct {
	TopologyID string `json:"topologyID"`

	ClusterID uint64 `json:"clusterID"`

	ShardID uint64 `json:"shardID,omitempty"`

	TableID uint64 `json:"tableID,omitempty"`

	TimelineID uint64 `json:"timelineID"`

	Epoch uint64 `json:"epoch"`

	CurrentPrimaryID string `json:"currentPrimaryID"`

	PrimaryLSN uint64 `json:"primaryLSN"`
}

// HAPhysicalIsolationReceiptStatus is the compatibility-named, typed receipt
// that an automatic failover crossed its logical write-authority boundary. It
// is not proof of power-off or process exit. The intent half is checkpointed
// before the old StatefulSet is scaled. The final half is populated only after
// exact runtime watchdog proof, an uncached Lease transfer, and a fresh
// controller-local monotonic wait of the proof-bound maximum fence latency.
// Pod absence is supplemental Kubernetes topology evidence only.
type HAPhysicalIsolationReceiptStatus struct {
	ClusterUID string `json:"clusterUID"`

	StatefulSetName string `json:"statefulSetName"`

	StatefulSetUID string `json:"statefulSetUID"`

	InitialStatefulSetGeneration int64 `json:"initialStatefulSetGeneration"`

	InitialStatefulSetResourceVersion string `json:"initialStatefulSetResourceVersion"`

	// +optional
	// +kubebuilder:validation:MaxItems=256
	InitialOldPods []HAPhysicalIsolationPodIdentity `json:"initialOldPods,omitempty"`

	InitialPodListResourceVersion string `json:"initialPodListResourceVersion"`

	LeaseName string `json:"leaseName"`

	LeaseUID string `json:"leaseUID"`

	LeaseResourceVersion string `json:"leaseResourceVersion"`

	LeaseHolder string `json:"leaseHolder"`

	LeaseGeneration uint64 `json:"leaseGeneration"`

	LeaseScope HAPhysicalIsolationLeaseScope `json:"leaseScope"`

	LeaseTransferTime metav1.Time `json:"leaseTransferTime"`

	// WatchdogMaxFenceLatencyMS is copied exactly from the authenticated old
	// runtime proof and must equal the configured runtime watchdog bound.
	WatchdogMaxFenceLatencyMS int32 `json:"watchdogMaxFenceLatencyMS"`

	// WatchdogProof is mandatory for every automatic Lease-transfer path. Pod
	// API absence can result from force deletion while the old process survives.
	// +optional
	WatchdogProof *HAPhysicalIsolationWatchdogProofStatus `json:"watchdogProof,omitempty"`

	// Final observations below are absent until the controller has re-read the
	// exact objects through its uncached APIReader after scaling to zero.
	// +optional
	IsolatedStatefulSetGeneration int64 `json:"isolatedStatefulSetGeneration,omitempty"`

	// +optional
	IsolatedStatefulSetObservedGeneration int64 `json:"isolatedStatefulSetObservedGeneration,omitempty"`

	// +optional
	IsolatedStatefulSetResourceVersion string `json:"isolatedStatefulSetResourceVersion,omitempty"`

	// +optional
	ObservedLeaseResourceVersion string `json:"observedLeaseResourceVersion,omitempty"`

	// AbsenceProven records an uncached PodList observation. It never asserts
	// that a force-deleted or partitioned process has exited.
	// +optional
	AbsenceProven bool `json:"absenceProven,omitempty"`

	// +optional
	AbsencePodListResourceVersion string `json:"absencePodListResourceVersion,omitempty"`

	// +optional
	FrozenBoundaryLSN uint64 `json:"frozenBoundaryLSN,omitempty"`

	// +optional
	ObservedAt *metav1.Time `json:"observedAt,omitempty"`

	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
}

// HAPhysicalIsolationWatchdogProofStatus binds the runtime-originated proof to
// the exact old Pod and container process observed through the uncached API.
type HAPhysicalIsolationWatchdogProofStatus struct {
	CapabilityVersion int32 `json:"capabilityVersion"`

	Active bool `json:"active"`

	AuthorityGranted bool `json:"authorityGranted"`

	LeaseName string `json:"leaseName"`

	LeaseNamespace string `json:"leaseNamespace"`

	TopologyID string `json:"topologyID"`

	LocalNodeID string `json:"localNodeID"`

	ObservedHolderNodeID string `json:"observedHolderNodeID"`

	PodName string `json:"podName"`

	PodUID string `json:"podUID"`

	ContainerName string `json:"containerName"`

	ContainerID string `json:"containerID"`

	ContainerRestartCount int32 `json:"containerRestartCount"`

	ContainerStartedAt metav1.Time `json:"containerStartedAt"`

	ProcessBootID string `json:"processBootID"`

	ObservedLeaseTransitions int32 `json:"observedLeaseTransitions"`

	MaxFenceLatencyMS int32 `json:"maxFenceLatencyMS"`

	RuntimeObservedAt metav1.Time `json:"runtimeObservedAt"`
}

// HAPlannedActionStatus reports one planned HA operator action.
type HAPlannedActionStatus struct {
	Kind string `json:"kind,omitempty"`

	// Phase groups the action into the HA workflow stage.
	// +optional
	Phase string `json:"phase,omitempty"`

	// Executor identifies whether typed /admin/v1, a CLI-backed Job, or the Kubernetes controller executes this action.
	// +optional
	Executor string `json:"executor,omitempty"`

	// DependsOn names the action kind that must complete before this action runs.
	// +optional
	DependsOn string `json:"dependsOn,omitempty"`

	StandbyName string `json:"standbyName,omitempty"`

	SlotName string `json:"slotName,omitempty"`

	TargetLSN uint64 `json:"targetLSN,omitempty"`

	ObservedLSN uint64 `json:"observedLSN,omitempty"`

	RetainedFromLSN uint64 `json:"retainedFromLSN,omitempty"`

	RouteFrom string `json:"routeFrom,omitempty"`

	RouteTo string `json:"routeTo,omitempty"`

	FenceAuthority HAFencingAuthority `json:"fenceAuthority,omitempty"`

	FenceHolder string `json:"fenceHolder,omitempty"`

	FenceGeneration uint64 `json:"fenceGeneration,omitempty"`

	FenceReason string `json:"fenceReason,omitempty"`

	// AdminCommand is a compatibility CLI argv hint for HA actions that need a Kubernetes Job or break-glass execution.
	// +optional
	AdminCommand []string `json:"adminCommand,omitempty"`

	// AdminURL is the HA admin endpoint for typed /admin/v1/ha execution or CLI-backed job targeting.
	// +optional
	AdminURL string `json:"adminURL,omitempty"`

	// AdminNodeID is the node id expected in the typed HA admin action receipt from AdminURL.
	// +optional
	AdminNodeID string `json:"adminNodeID,omitempty"`

	// AdminMethod is the HTTP method for the typed /admin/v1 operation that executes this action.
	// +optional
	AdminMethod string `json:"adminMethod,omitempty"`

	// AdminPath is the typed /admin/v1 operation path that executes this action.
	// +optional
	AdminPath string `json:"adminPath,omitempty"`

	// AdminResult records stable identifiers returned by a successful typed /admin/v1 HA operation.
	// +optional
	AdminResult *HAAdminActionResultStatus `json:"adminResult,omitempty"`

	// SeedManifestPath is the base-backup manifest path used by seed finish/bootstrap actions.
	// +optional
	SeedManifestPath string `json:"seedManifestPath,omitempty"`

	// SeedContentRoot is the copied base-backup content root used by seed bootstrap actions.
	// +optional
	SeedContentRoot string `json:"seedContentRoot,omitempty"`

	// SeedArtifactTargetRoot is the target PVC root that owns immutable activated generations.
	// +optional
	SeedArtifactTargetRoot string `json:"seedArtifactTargetRoot,omitempty"`

	// SeedArtifactLocation is the object-store URI used by portable seed actions.
	// +optional
	SeedArtifactLocation string `json:"seedArtifactLocation,omitempty"`

	// SeedArtifactGeneration is the immutable generation selected by the operator.
	// +optional
	SeedArtifactGeneration string `json:"seedArtifactGeneration,omitempty"`

	// SeedArtifactRetainGenerations is the retention bound applied after bootstrap.
	// +optional
	SeedArtifactRetainGenerations int32 `json:"seedArtifactRetainGenerations,omitempty"`

	// SeedArtifactCaptureRoot is the durable source capture root proven by the
	// successful runtime capture response. It is never accepted from spec.
	// +optional
	SeedArtifactCaptureRoot string `json:"seedArtifactCaptureRoot,omitempty"`

	// SeedCaptureReceiptPath and SeedCaptureReceiptSHA256 bind portable
	// publication, restore, and activation to the exact immutable runtime-owned
	// capture COMPLETE receipt.
	// +optional
	SeedCaptureReceiptPath string `json:"seedCaptureReceiptPath,omitempty"`
	// +optional
	SeedCaptureReceiptSHA256 string `json:"seedCaptureReceiptSHA256,omitempty"`

	// SeedArtifactProtectedGenerations freezes the current, in-flight and rollback
	// generations that a local GC operation must preserve.
	// +optional
	// +kubebuilder:validation:MaxItems=256
	SeedArtifactProtectedGenerations []string `json:"seedArtifactProtectedGenerations,omitempty"`

	// Topology fields bind every seed action in this chain to one exact desired
	// topology generation and target PVC identity.
	// +optional
	TopologyID string `json:"topologyID,omitempty"`
	// +optional
	TopologyGeneration int64 `json:"topologyGeneration,omitempty"`
	// +optional
	TopologyNodeID string `json:"topologyNodeID,omitempty"`
	// +optional
	TargetPVCName string `json:"targetPVCName,omitempty"`
	// +optional
	TargetPVCUID string `json:"targetPVCUID,omitempty"`
	// +optional
	TargetLocalNodeID uint64 `json:"targetLocalNodeID,omitempty"`
	// +optional
	TargetReplicaID uint64 `json:"targetReplicaID,omitempty"`
	// +optional
	SourcePVCName string `json:"sourcePVCName,omitempty"`
	// +optional
	SourcePVCUID string `json:"sourcePVCUID,omitempty"`

	// SeedArtifactReceipt is the typed, validated receipt emitted by a completed
	// portable publish, restore, or prune Job.
	// +optional
	SeedArtifactReceipt *HASeedArtifactReceiptStatus `json:"seedArtifactReceipt,omitempty"`

	// PhysicalIsolationReceipt is the typed Kubernetes object-incarnation and
	// timing proof required for a successful IsolateFormerPrimary action.
	// +optional
	PhysicalIsolationReceipt *HAPhysicalIsolationReceiptStatus `json:"physicalIsolationReceipt,omitempty"`

	// AdminJobName records direct-admin-api for typed execution or the Kubernetes Job created for CLI-backed execution.
	// +optional
	AdminJobName string `json:"adminJobName,omitempty"`

	// AdminJobPhase summarizes dependency wait state, direct admin API execution, or CLI-backed Kubernetes Job state.
	// +optional
	AdminJobPhase string `json:"adminJobPhase,omitempty"`

	// AdminError records the latest direct /admin/v1 HA execution or result-validation error for this action.
	// +optional
	AdminError string `json:"adminError,omitempty"`

	// AdminStatusCode records the latest typed /admin/v1 HA HTTP status code for this action.
	// +optional
	AdminStatusCode int `json:"adminStatusCode,omitempty"`

	// OperationID is the stable controller identity for this desired HA operation.
	// It excludes mutable observations such as LSN progress and human-readable
	// reasons so replanning cannot orphan an in-flight execution.
	// +optional
	OperationID string `json:"operationID,omitempty"`

	// ExecutionStateVersion distinguishes durable bounded-retry state from legacy
	// Pending/Failed status written before attempt reservations were introduced.
	// +optional
	ExecutionStateVersion int32 `json:"executionStateVersion,omitempty"`

	// RetryGeneration is the explicit recovery nonce captured when this operation
	// was planned.
	// +optional
	RetryGeneration int64 `json:"retryGeneration,omitempty"`

	// AttemptCount is the durable number of direct requests or Kubernetes Job pod
	// attempts observed for this exact action identity.
	// +optional
	AttemptCount int32 `json:"attemptCount,omitempty"`

	// RetryBudgetUsed is the durable number of retryable direct-request failures
	// and expired uncertain reservations charged to the bounded retry budget.
	// Successful prerequisite polls do not consume this budget; AttemptCount still
	// records every dispatched request.
	// +optional
	RetryBudgetUsed int32 `json:"retryBudgetUsed,omitempty"`

	// Retryable reports whether the latest failure remains within its retry budget.
	// +optional
	Retryable bool `json:"retryable,omitempty"`

	// ErrorClass is a stable machine-readable terminal or retry classification.
	// +optional
	ErrorClass string `json:"errorClass,omitempty"`

	// FirstAttemptAt records when execution of this exact action identity began.
	// +optional
	FirstAttemptAt *metav1.Time `json:"firstAttemptAt,omitempty"`

	// LastAttemptAt records the most recent execution attempt.
	// +optional
	LastAttemptAt *metav1.Time `json:"lastAttemptAt,omitempty"`

	// NextRetryAt records the persisted earliest time for the next direct retry.
	// +optional
	NextRetryAt *metav1.Time `json:"nextRetryAt,omitempty"`

	// InFlightAttempt is the attempt number durably reserved before a typed admin
	// request is sent. A nonzero value prevents concurrent or immediate crash replay.
	// +optional
	InFlightAttempt int32 `json:"inFlightAttempt,omitempty"`

	// AttemptID identifies the exact in-flight reservation for result checkpointing.
	// +optional
	AttemptID string `json:"attemptID,omitempty"`

	// ReservationExpiresAt is the earliest time an uncheckpointed in-flight request
	// may be replayed with its frozen, idempotent operation payload.
	// +optional
	ReservationExpiresAt *metav1.Time `json:"reservationExpiresAt,omitempty"`

	// PrerequisiteDeadlineAt bounds a non-failure wait such as promotion boundary
	// application. It is preserved across observation replans and request polls.
	// +optional
	PrerequisiteDeadlineAt *metav1.Time `json:"prerequisiteDeadlineAt,omitempty"`

	// CompletedAt records terminal success or failure.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	Reason string `json:"reason,omitempty"`
}

// HASeedArtifactReceiptStatus summarizes a durable portable seed receipt.
type HASeedArtifactReceiptStatus struct {
	ActionKind                  string `json:"actionKind,omitempty"`
	Scope                       string `json:"scope,omitempty"`
	FormatVersion               int32  `json:"formatVersion"`
	Generation                  string `json:"generation"`
	SlotName                    string `json:"slotName"`
	ClusterID                   uint64 `json:"clusterID,omitempty"`
	ShardID                     uint64 `json:"shardID,omitempty"`
	TableID                     uint64 `json:"tableID,omitempty"`
	TimelineID                  uint64 `json:"timelineID,omitempty"`
	Epoch                       uint64 `json:"epoch,omitempty"`
	ManifestID                  string `json:"manifestID,omitempty"`
	BackupLSN                   uint64 `json:"backupLSN,omitempty"`
	CheckpointLSN               uint64 `json:"checkpointLSN,omitempty"`
	ManifestSHA256              string `json:"manifestSHA256,omitempty"`
	AggregateSHA256             string `json:"aggregateSHA256,omitempty"`
	SeedReceiptSHA256           string `json:"seedReceiptSHA256,omitempty"`
	CaptureReceiptSHA256        string `json:"captureReceiptSHA256,omitempty"`
	GenerationPath              string `json:"generationPath,omitempty"`
	RawGenerationPath           string `json:"rawGenerationPath,omitempty"`
	MaterializedReceiptSHA256   string `json:"materializedReceiptSHA256,omitempty"`
	MaterializedAggregateSHA256 string `json:"materializedAggregateSHA256,omitempty"`
	TargetLocalNodeID           uint64 `json:"targetLocalNodeID,omitempty"`
	TargetReplicaID             uint64 `json:"targetReplicaID,omitempty"`
	TotalBytes                  uint64 `json:"totalBytes,omitempty"`
	FileCount                   int32  `json:"fileCount,omitempty"`
	RetainedCount               int32  `json:"retainedCount,omitempty"`
	DeletedCount                int32  `json:"deletedCount,omitempty"`
	ProtectedCount              int32  `json:"protectedCount,omitempty"`
	ResumedTombstoneCount       int32  `json:"resumedTombstoneCount,omitempty"`
	SkippedIneligibleCount      int32  `json:"skippedIneligibleCount,omitempty"`
	CheckpointSHA256            string `json:"checkpointSHA256,omitempty"`
	TopologyID                  string `json:"topologyID,omitempty"`
	TopologyGeneration          int64  `json:"topologyGeneration,omitempty"`
	NodeID                      string `json:"nodeID,omitempty"`
	TargetPVCName               string `json:"targetPVCName,omitempty"`
	TargetPVCUID                string `json:"targetPVCUID,omitempty"`
}

// HAAdminActionResultStatus records correlation fields from a typed HA admin action response.
type HAAdminActionResultStatus struct {
	// RawReceiptJSON is the bounded exact JSON response for lifecycle receipts
	// that a later action must re-verify independently.
	// +optional
	// +kubebuilder:validation:MaxLength=65536
	RawReceiptJSON string `json:"rawReceiptJSON,omitempty"`

	// SchemaVersion is the response schema version returned by the HA admin API.
	// +optional
	SchemaVersion uint32 `json:"schemaVersion,omitempty"`

	// ActionID is the stable typed HA admin action correlation id.
	// +optional
	ActionID string `json:"actionID,omitempty"`

	// ActionKind is the typed HA admin action kind that produced this result.
	// +optional
	ActionKind string `json:"actionKind,omitempty"`

	// ActionTarget is the node id, slot name, manifest id, or promotion boundary acted on.
	// +optional
	ActionTarget string `json:"actionTarget,omitempty"`

	// ActionState is the idempotency state returned by the HA admin action.
	// +optional
	ActionState string `json:"actionState,omitempty"`

	// ActionNodeID is the node id of the node-local HA admin endpoint that produced this result.
	// +optional
	ActionNodeID string `json:"actionNodeID,omitempty"`

	// SlotAction is the executed replication slot action.
	// +optional
	SlotAction string `json:"slotAction,omitempty"`

	// SlotName is the affected replication slot or base-backup standby slot.
	// +optional
	SlotName string `json:"slotName,omitempty"`

	// ManifestID is the base-backup manifest/action id returned by seed operations.
	// +optional
	ManifestID string `json:"manifestID,omitempty"`

	// BackupLSN is the base-backup stream boundary returned by seed operations.
	// +optional
	BackupLSN uint64 `json:"backupLSN,omitempty"`

	// StartRecordLSN is the durable backup_start record LSN returned by seed begin.
	// +optional
	StartRecordLSN uint64 `json:"startRecordLSN,omitempty"`

	// EndRecordLSN is the durable backup_end record LSN returned by seed finish.
	// +optional
	EndRecordLSN uint64 `json:"endRecordLSN,omitempty"`

	// CheckpointLSN is the standby checkpoint LSN returned by seed bootstrap.
	// +optional
	CheckpointLSN uint64 `json:"checkpointLSN,omitempty"`

	// SeedArtifactGeneration binds a slot activation receipt to an immutable target generation.
	// +optional
	SeedArtifactGeneration string `json:"seedArtifactGeneration,omitempty"`

	// SeedReceiptSHA256 binds slot activation to the durable target activation receipt.
	// +optional
	SeedReceiptSHA256 string `json:"seedReceiptSHA256,omitempty"`

	// ManifestSHA256 and AggregateSHA256 preserve the verified artifact digest evidence.
	// +optional
	ManifestSHA256 string `json:"manifestSHA256,omitempty"`
	// +optional
	AggregateSHA256 string `json:"aggregateSHA256,omitempty"`

	// CaptureReceiptSHA256 is the digest of the exact immutable runtime capture
	// COMPLETE bytes that authorized every downstream portable seed action.
	// +optional
	CaptureReceiptSHA256 string `json:"captureReceiptSHA256,omitempty"`

	// Runtime-owned seed capture evidence and the primary-PVC paths consumed by
	// the dependent publish action.
	// +optional
	SeedClusterID uint64 `json:"seedClusterID,omitempty"`
	// +optional
	SeedShardID uint64 `json:"seedShardID,omitempty"`
	// +optional
	SeedTableID uint64 `json:"seedTableID,omitempty"`
	// +optional
	SeedTimelineID uint64 `json:"seedTimelineID,omitempty"`
	// +optional
	SeedEpoch uint64 `json:"seedEpoch,omitempty"`
	// +optional
	SeedSourcePlanSHA256 string `json:"seedSourcePlanSHA256,omitempty"`
	// +optional
	SeedFileCount uint64 `json:"seedFileCount,omitempty"`
	// +optional
	SeedTotalBytes uint64 `json:"seedTotalBytes,omitempty"`
	// +optional
	SeedGenerationRoot string `json:"seedGenerationRoot,omitempty"`
	// +optional
	SeedContentRoot string `json:"seedContentRoot,omitempty"`
	// +optional
	SeedManifestPath string `json:"seedManifestPath,omitempty"`
	// +optional
	SeedAlreadyCaptured bool `json:"seedAlreadyCaptured,omitempty"`

	// PromotionRequiredLSN is the minimum LSN checked by a promotion assessment.
	// +optional
	PromotionRequiredLSN uint64 `json:"promotionRequiredLSN,omitempty"`

	// PromotionReceivedLSN is the standby received LSN observed by a promotion assessment.
	// +optional
	PromotionReceivedLSN uint64 `json:"promotionReceivedLSN,omitempty"`

	// PromotionAppliedLSN is the standby applied LSN observed by a promotion assessment.
	// +optional
	PromotionAppliedLSN uint64 `json:"promotionAppliedLSN,omitempty"`

	// PromotionCanPromote reports whether a promotion assessment permits promotion.
	// +optional
	PromotionCanPromote bool `json:"promotionCanPromote,omitempty"`

	// PromotionFenced reports whether promotion assessment observed a fence.
	// +optional
	PromotionFenced bool `json:"promotionFenced,omitempty"`

	// PromotionSafe reports whether a promotion assessment found no safety violation.
	// +optional
	PromotionSafe bool `json:"promotionSafe,omitempty"`

	// PromotionForce reports whether a promotion assessment used force.
	// +optional
	PromotionForce bool `json:"promotionForce,omitempty"`

	// PromotionMode is the explicit promotion assessment mode reported by the admin API.
	// +optional
	PromotionMode string `json:"promotionMode,omitempty"`

	// PromotionDataLossPossible reports whether a promotion assessment found possible data loss.
	// +optional
	PromotionDataLossPossible bool `json:"promotionDataLossPossible,omitempty"`

	// PromotionRequiresFencing reports whether a promotion assessment still needs fencing.
	// +optional
	PromotionRequiresFencing bool `json:"promotionRequiresFencing,omitempty"`

	// PromotionRequiresForce reports whether a promotion assessment needs force to proceed.
	// +optional
	PromotionRequiresForce bool `json:"promotionRequiresForce,omitempty"`

	// FenceGeneration is the promotion fence generation returned by fence operations.
	// +optional
	FenceGeneration uint64 `json:"fenceGeneration,omitempty"`

	// FenceToken is the opaque promotion fence receipt token returned by fence operations.
	// +optional
	FenceToken string `json:"fenceToken,omitempty"`

	// FenceClusterID is the cluster identity carried by a promotion fence receipt.
	// +optional
	FenceClusterID uint64 `json:"fenceClusterID,omitempty"`

	// FenceShardID is the shard identity carried by a promotion fence receipt.
	// +optional
	FenceShardID uint64 `json:"fenceShardID,omitempty"`

	// FenceTableID is the table identity carried by a promotion fence receipt.
	// +optional
	FenceTableID uint64 `json:"fenceTableID,omitempty"`

	// FenceOldPrimaryID is the fenced primary node id carried by a promotion fence receipt.
	// +optional
	FenceOldPrimaryID string `json:"fenceOldPrimaryID,omitempty"`

	// FencePromotedNodeID is the promoted standby node id carried by a promotion fence receipt.
	// +optional
	FencePromotedNodeID string `json:"fencePromotedNodeID,omitempty"`

	// FenceParentTimelineID is the parent timeline carried by a promotion fence receipt.
	// +optional
	FenceParentTimelineID uint64 `json:"fenceParentTimelineID,omitempty"`

	// FenceParentEpoch is the parent epoch carried by a promotion fence receipt.
	// +optional
	FenceParentEpoch uint64 `json:"fenceParentEpoch,omitempty"`

	// FenceNewTimelineID is the promoted timeline carried by a promotion fence receipt.
	// +optional
	FenceNewTimelineID uint64 `json:"fenceNewTimelineID,omitempty"`

	// FenceNewEpoch is the promoted epoch carried by a promotion fence receipt.
	// +optional
	FenceNewEpoch uint64 `json:"fenceNewEpoch,omitempty"`

	// FenceRequiredLSN is the minimum promotion LSN carried by a promotion fence receipt.
	// +optional
	FenceRequiredLSN uint64 `json:"fenceRequiredLSN,omitempty"`

	// FenceObservedLSN is the observed standby LSN carried by a promotion fence receipt.
	// +optional
	FenceObservedLSN uint64 `json:"fenceObservedLSN,omitempty"`

	// FenceForced reports whether the promotion fence was acquired with force.
	// +optional
	FenceForced bool `json:"fenceForced,omitempty"`

	// FenceReason is the reason carried by a promotion fence receipt.
	// +optional
	FenceReason string `json:"fenceReason,omitempty"`

	// RejoinAction is the former-primary rejoin action returned by a rejoin assessment.
	// +optional
	RejoinAction string `json:"rejoinAction,omitempty"`

	// RejoinReason is the former-primary rejoin reason returned by a rejoin assessment.
	// +optional
	RejoinReason string `json:"rejoinReason,omitempty"`

	// FormerNodeID is the former-primary node id returned by a rejoin assessment.
	// +optional
	FormerNodeID string `json:"formerNodeID,omitempty"`

	// TargetTimelineID is the promoted timeline returned by a rejoin assessment.
	// +optional
	TargetTimelineID uint64 `json:"targetTimelineID,omitempty"`

	// TargetEpoch is the promoted epoch returned by a rejoin assessment.
	// +optional
	TargetEpoch uint64 `json:"targetEpoch,omitempty"`

	// ForkLSN is the timeline fork LSN returned by a rejoin assessment.
	// +optional
	ForkLSN uint64 `json:"forkLSN,omitempty"`

	// FormerLastLSN is the former primary's last observed LSN returned by a rejoin assessment.
	// +optional
	FormerLastLSN uint64 `json:"formerLastLSN,omitempty"`

	// RetainedFromLSN is the earliest retained WAL LSN used by a rejoin assessment.
	// +optional
	RetainedFromLSN uint64 `json:"retainedFromLSN,omitempty"`

	// DataLossDiscarded reports whether rewind would discard former-primary data.
	// +optional
	DataLossDiscarded bool `json:"dataLossDiscarded,omitempty"`

	// RewindExecuted reports that `/admin/v1/ha/rejoin/rewind` returned a concrete rewind result.
	// +optional
	RewindExecuted bool `json:"rewindExecuted,omitempty"`

	// RewindPreviousLastLSN is the former-primary log tail before rewind execution.
	// +optional
	RewindPreviousLastLSN uint64 `json:"rewindPreviousLastLSN,omitempty"`

	// RewindCurrentLastLSN is the former-primary log tail after rewind execution.
	// +optional
	RewindCurrentLastLSN uint64 `json:"rewindCurrentLastLSN,omitempty"`

	// RewindNextLSN is the next append LSN after rewind execution.
	// +optional
	RewindNextLSN uint64 `json:"rewindNextLSN,omitempty"`

	// RewindDiscardedLSNCount is the count of divergent former-primary LSNs discarded by rewind.
	// +optional
	RewindDiscardedLSNCount uint64 `json:"rewindDiscardedLSNCount,omitempty"`

	// ReseedExecuted reports that `/admin/v1/ha/rejoin/reseed` marked the former-primary slot for reseed.
	// +optional
	ReseedExecuted bool `json:"reseedExecuted,omitempty"`

	// ReseedSlotName is the replication slot marked for former-primary reseed.
	// +optional
	ReseedSlotName string `json:"reseedSlotName,omitempty"`

	// ReseedRequired reports that the former-primary slot is now reseed-required.
	// +optional
	ReseedRequired bool `json:"reseedRequired,omitempty"`

	// ReseedBaseBackupRequired reports that a new base backup must be taken before bootstrap.
	// +optional
	ReseedBaseBackupRequired bool `json:"reseedBaseBackupRequired,omitempty"`
}

// HAPrimaryRouteStatus reports the operator-facing primary endpoint target.
type HAPrimaryRouteStatus struct {
	ServiceName string `json:"serviceName,omitempty"`

	CurrentTarget string `json:"currentTarget,omitempty"`

	DesiredTarget string `json:"desiredTarget,omitempty"`

	FenceAuthority HAFencingAuthority `json:"fenceAuthority,omitempty"`

	FenceGeneration uint64 `json:"fenceGeneration,omitempty"`

	Stale bool `json:"stale,omitempty"`

	Action string `json:"action,omitempty"`

	Reason string `json:"reason,omitempty"`
}

// HARetentionStatus reports HA WAL retention pressure.
type HARetentionStatus struct {
	OldestRestartLSN uint64 `json:"oldestRestartLSN,omitempty"`

	RetainedLSNCount uint64 `json:"retainedLSNCount,omitempty"`

	// RetainedByteCount is the encoded HA WAL bytes retained for active slots.
	RetainedByteCount uint64 `json:"retainedByteCount,omitempty"`

	// RetainedAgeNS is the HA WAL timestamp span retained for active slots.
	RetainedAgeNS uint64 `json:"retainedAgeNS,omitempty"`

	ActiveSlots int32 `json:"activeSlots,omitempty"`

	ReseedRecommended int32 `json:"reseedRecommended,omitempty"`
}

// HAFormerPrimaryStatus reports the old primary's rejoin disposition after promotion.
type HAFormerPrimaryStatus struct {
	NodeID string `json:"nodeID,omitempty"`

	Fenced bool `json:"fenced,omitempty"`

	RejoinRequired bool `json:"rejoinRequired,omitempty"`

	RewindPossible bool `json:"rewindPossible,omitempty"`

	ReseedRequired bool `json:"reseedRequired,omitempty"`

	Diverged bool `json:"diverged,omitempty"`

	ParentTimelineID uint64 `json:"parentTimelineID,omitempty"`

	NewTimelineID uint64 `json:"newTimelineID,omitempty"`

	ObservedTimelineID uint64 `json:"observedTimelineID,omitempty"`

	SwitchLSN uint64 `json:"switchLSN,omitempty"`

	ObservedLSN uint64 `json:"observedLSN,omitempty"`

	FenceAuthority HAFencingAuthority `json:"fenceAuthority,omitempty"`

	FenceHolder string `json:"fenceHolder,omitempty"`

	FenceGeneration uint64 `json:"fenceGeneration,omitempty"`

	TargetTimelineID uint64 `json:"targetTimelineID,omitempty"`

	TargetEpoch uint64 `json:"targetEpoch,omitempty"`

	ForkLSN uint64 `json:"forkLSN,omitempty"`

	FormerLastLSN uint64 `json:"formerLastLSN,omitempty"`

	RetainedFromLSN uint64 `json:"retainedFromLSN,omitempty"`

	DataLossDiscarded bool `json:"dataLossDiscarded,omitempty"`

	AssessedAction string `json:"assessedAction,omitempty"`

	AssessedReason string `json:"assessedReason,omitempty"`

	Action string `json:"action,omitempty"`

	Reason string `json:"reason,omitempty"`
}

// HAPromotionStatus reports a completed HA promotion.
type HAPromotionStatus struct {
	// ClusterID is the replicated cluster identity of the promotion scope.
	// +optional
	ClusterID uint64 `json:"clusterID,omitempty"`

	// ShardID is the replicated shard identity of the promotion scope. Zero means whole-instance or default shard scope.
	// +optional
	ShardID uint64 `json:"shardID,omitempty"`

	// TableID is the replicated table identity of the promotion scope. Zero means whole-instance or default table scope.
	// +optional
	TableID uint64 `json:"tableID,omitempty"`

	OldPrimaryID string `json:"oldPrimaryID,omitempty"`

	PromotedStandbyID string `json:"promotedStandbyID,omitempty"`

	ParentTimelineID uint64 `json:"parentTimelineID,omitempty"`

	ParentEpoch uint64 `json:"parentEpoch,omitempty"`

	NewTimelineID uint64 `json:"newTimelineID,omitempty"`

	NewEpoch uint64 `json:"newEpoch,omitempty"`

	SwitchLSN uint64 `json:"switchLSN,omitempty"`

	RequiredLSN uint64 `json:"requiredLSN,omitempty"`

	ObservedLSN uint64 `json:"observedLSN,omitempty"`

	FenceGeneration uint64 `json:"fenceGeneration,omitempty"`

	FenceAuthority HAFencingAuthority `json:"fenceAuthority,omitempty"`

	FenceToken string `json:"fenceToken,omitempty"`

	FenceReason string `json:"fenceReason,omitempty"`

	Forced bool `json:"forced,omitempty"`

	DataLossPossible bool `json:"dataLossPossible,omitempty"`

	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
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

	// StandaloneTier records the observed standalone sub-tier name.
	StandaloneTier string `json:"standaloneTier,omitempty"`

	// MetadataTier records the observed metadata sub-tier name.
	MetadataTier string `json:"metadataTier,omitempty"`

	// DataTier records the observed data sub-tier name.
	DataTier string `json:"dataTier,omitempty"`

	// InferenceTier records the observed inference sub-tier name.
	InferenceTier string `json:"inferenceTier,omitempty"`

	// StandaloneResources summarizes standalone CPU/memory requests and limits.
	StandaloneResources string `json:"standaloneResources,omitempty"`

	// StandaloneStorage is the observed standalone storage size.
	StandaloneStorage string `json:"standaloneStorage,omitempty"`

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

	// InferenceEnabled reports whether this tier has an operator-managed InferencePool.
	InferenceEnabled bool `json:"inferenceEnabled,omitempty"`

	// InferenceReplicas reports the observed InferencePool replica bounds.
	InferenceReplicas string `json:"inferenceReplicas,omitempty"`

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

// StandaloneStatus reports standalone mode operational status.
type StandaloneStatus struct {
	// Ready indicates that the combined standalone workload is ready.
	Ready bool `json:"ready,omitempty"`

	// MetadataReady indicates that the metadata API is ready.
	MetadataReady bool `json:"metadataReady,omitempty"`

	// StoreReady indicates that the store API is ready.
	StoreReady bool `json:"storeReady,omitempty"`

	// InferenceReady indicates that inference is ready when enabled.
	InferenceReady bool `json:"inferenceReady,omitempty"`

	// NodeID is the configured standalone node ID.
	NodeID int32 `json:"nodeID,omitempty"`

	// PodName is the name of the backing standalone pod.
	PodName string `json:"podName,omitempty"`

	// PodIP is the IP of the backing standalone pod.
	PodIP string `json:"podIP,omitempty"`

	// ObservedConfigHash records the config hash seen by the controller.
	ObservedConfigHash string `json:"observedConfigHash,omitempty"`

	// LastTransitionTime records the last standalone status transition.
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
