package v1

import (
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	inferencev1alpha1 "github.com/antflydb/antfly/go/pkg/operator/api/inference/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
)

const haTopologyIDAnnotation = "antfly.io/ha-topology-id"

var (
	// irsaARNPattern matches AWS IAM Role ARNs including China and GovCloud partitions.
	irsaARNPattern = regexp.MustCompile(`^arn:aws(-cn|-us-gov)?:iam::\d{12}:role/.+$`)
	// ec2InstancePattern matches AWS EC2 instance type names (e.g. m5.large, u-6tb1.56xlarge).
	ec2InstancePattern = regexp.MustCompile(`^[a-z][a-z0-9-]*\.[a-z0-9]+$`)
	// productTierTokenPattern accepts stable external tier/catalog identifiers.
	productTierTokenPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
	// envVarNamePattern accepts shell-safe environment variable names accepted by
	// the Zig HA CLI/runtime token resolvers.
	envVarNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	// haIdentifierPattern matches HA node IDs and slot names accepted by the Zig
	// HA runtime validators.
	haIdentifierPattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,128}$`)
	haSHA256Pattern     = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// ValidateCreate validates the cluster configuration when creating a new cluster.
// Called by controller fallback when webhooks are disabled.
func (r *AntflyCluster) ValidateCreate() error {
	return r.ValidateAntflyCluster()
}

// ValidateUpdate validates the cluster configuration when updating an existing cluster.
// Called by controller fallback when webhooks are disabled (note: controllers cannot
// provide the old object, so this is only called by the deprecated webhook interface).
func (r *AntflyCluster) ValidateUpdate(old runtime.Object) error {
	oldCluster, ok := old.(*AntflyCluster)
	if !ok {
		return fmt.Errorf("expected *AntflyCluster, got %T", old)
	}
	if err := r.ValidateImmutability(oldCluster); err != nil {
		return err
	}
	return r.ValidateAntflyCluster()
}

// Default applies admission defaults to AntflyCluster.
func (r *AntflyCluster) Default() {
	if r.Spec.Mode == "" {
		r.Spec.Mode = ClusterModeDistributed
	}

	defaultStorageAutoGrow(&r.Spec.Storage)

	if r.Spec.Mode != ClusterModeStandalone || r.Spec.Standalone == nil {
		return
	}
	if r.Spec.Storage.Engine == "" {
		r.Spec.Storage.Engine = "local"
	}
	if r.Spec.Storage.Engine == "lite" && r.Spec.Storage.LiteFileName == "" {
		r.Spec.Storage.LiteFileName = "antfly.aflite"
	}

	if r.Spec.Standalone.Replicas == 0 {
		r.Spec.Standalone.Replicas = 1
	}

	if r.Spec.Standalone.NodeID == 0 {
		r.Spec.Standalone.NodeID = 1
	}

	if r.Spec.Standalone.MetadataAPI.Port == 0 {
		r.Spec.Standalone.MetadataAPI.Port = 8080
	}

	if r.Spec.Standalone.MetadataRaft.Port == 0 {
		r.Spec.Standalone.MetadataRaft.Port = 9017
	}

	if r.Spec.Standalone.StoreAPI.Port == 0 {
		r.Spec.Standalone.StoreAPI.Port = 12380
	}

	if r.Spec.Standalone.StoreRaft.Port == 0 {
		r.Spec.Standalone.StoreRaft.Port = 9021
	}

	if r.Spec.Standalone.Health.Port == 0 {
		r.Spec.Standalone.Health.Port = 4200
	}

	if r.Spec.Standalone.Inference == nil {
		r.Spec.Standalone.Inference = &StandaloneInferenceSpec{
			Enabled: true,
			APIURL:  "http://0.0.0.0:11433",
		}
		return
	}

	if r.Spec.Standalone.Inference.APIURL == "" {
		r.Spec.Standalone.Inference.APIURL = "http://0.0.0.0:11433"
	}
}

// ValidateAntflyCluster performs all validation checks
func (r *AntflyCluster) ValidateAntflyCluster() error {
	var allErrors []string

	if err := r.validateModeConfig(); err != nil {
		allErrors = append(allErrors, err.Error())
	}
	if err := ValidateOperatorManagedStorageConfig(r.Spec.Config); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validateGKEConfig(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validateEKSConfig(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validateNoConflictingSettings(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validateNoConflictingCloudProviders(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validateNodeCounts(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validatePublicAPIConfig(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validateEnvFrom(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validatePVCRetentionPolicy(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validateStorageAutoGrowConfig(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validateAutoScalingConfig(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validateResourceQuantities(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validateInferenceSpec(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validateSecretStore(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validateProductTierMapping(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validateHighAvailabilitySpec(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if len(allErrors) > 0 {
		return fmt.Errorf("AntflyCluster validation failed:\n  - %s",
			strings.Join(allErrors, "\n  - "))
	}

	return nil
}

func (r *AntflyCluster) validateSecretStore() error {
	store := r.Spec.SecretStore
	if store == nil {
		return nil
	}
	var errors []string
	if strings.TrimSpace(store.SecretName) == "" {
		errors = append(errors, "spec.secretStore.secretName is required")
	}
	if strings.Contains(store.SecretName, "/") {
		errors = append(errors, "spec.secretStore.secretName must be a name in the AntflyCluster namespace, not a path")
	}
	if store.Key != "" && strings.Contains(store.Key, "/") {
		errors = append(errors, "spec.secretStore.key must be a single Secret data key")
	}
	if store.Path != "" {
		if !strings.HasPrefix(store.Path, "/") {
			errors = append(errors, "spec.secretStore.path must be an absolute file path")
		}
		if strings.HasSuffix(store.Path, "/") {
			errors = append(errors, "spec.secretStore.path must include a file name")
		}
	}
	if len(errors) > 0 {
		return fmt.Errorf("SecretStore validation failed:\n  - %s", strings.Join(errors, "\n  - "))
	}
	return nil
}

func (r *AntflyCluster) validateInferenceSpec() error {
	if r.Spec.Inference == nil {
		return nil
	}

	inference := r.Spec.Inference
	mode := inference.modeOrDefault()
	var validationErrors []string

	switch mode {
	case AntflyInferenceModeDisabled:
		if len(inference.ManagedPools) > 0 || len(inference.SharedPools) > 0 || len(inference.PlatformPools) > 0 {
			validationErrors = append(validationErrors, "spec.inference must not set pools when mode=Disabled")
		}
	case AntflyInferenceModeManaged:
		if len(inference.ManagedPools) == 0 {
			validationErrors = append(validationErrors, "spec.inference.managedPools is required when mode=Managed")
		}
		if len(inference.SharedPools) > 0 || len(inference.PlatformPools) > 0 {
			validationErrors = append(validationErrors, "spec.inference shared pool references are only valid when mode=SharedRef or mode=PlatformShared")
		}
		for i, managed := range inference.ManagedPools {
			if len(inference.ManagedPools) > 1 && strings.TrimSpace(managed.Name) == "" {
				validationErrors = append(validationErrors, fmt.Sprintf("spec.inference.managedPools[%d].name is required when multiple managed pools are set", i))
			}
			pool := &inferencev1alpha1.InferencePool{
				Spec: *managed.Spec.DeepCopy(),
			}
			if err := pool.ValidateInferencePool(); err != nil {
				validationErrors = append(validationErrors, fmt.Sprintf("spec.inference.managedPools[%d].spec is invalid: %v", i, err))
			}
		}
	case AntflyInferenceModeSharedRef:
		if len(inference.SharedPools) == 0 {
			validationErrors = append(validationErrors, "spec.inference.sharedPools is required when mode=SharedRef")
		}
		if len(inference.ManagedPools) > 0 || len(inference.PlatformPools) > 0 {
			validationErrors = append(validationErrors, "spec.inference.managedPools and platformPools are not valid when mode=SharedRef")
		}
		validateInferencePoolRefs("spec.inference.sharedPools", inference.SharedPools, &validationErrors)
	case AntflyInferenceModePlatformShared:
		if len(inference.PlatformPools) == 0 {
			validationErrors = append(validationErrors, "spec.inference.platformPools is required when mode=PlatformShared")
		}
		if len(inference.ManagedPools) > 0 || len(inference.SharedPools) > 0 {
			validationErrors = append(validationErrors, "spec.inference.managedPools and sharedPools are not valid when mode=PlatformShared")
		}
		validateInferencePoolRefs("spec.inference.platformPools", inference.PlatformPools, &validationErrors)
	default:
		validationErrors = append(validationErrors, fmt.Sprintf("spec.inference.mode %q is invalid", mode))
	}

	if len(validationErrors) > 0 {
		return fmt.Errorf("spec.inference is invalid:\n  - %s", strings.Join(validationErrors, "\n  - "))
	}

	return nil
}

func (s *AntflyInferenceSpec) modeOrDefault() AntflyInferenceMode {
	if s == nil {
		return AntflyInferenceModeDisabled
	}
	if s.Mode != "" {
		return s.Mode
	}
	if len(s.SharedPools) > 0 {
		return AntflyInferenceModeSharedRef
	}
	if len(s.PlatformPools) > 0 {
		return AntflyInferenceModePlatformShared
	}
	if len(s.ManagedPools) > 0 {
		return AntflyInferenceModeManaged
	}
	return AntflyInferenceModeManaged
}

func validateInferencePoolRefs(path string, refs []InferencePoolReference, validationErrors *[]string) {
	for i, ref := range refs {
		if strings.TrimSpace(ref.Name) == "" {
			*validationErrors = append(*validationErrors, fmt.Sprintf("%s[%d].name is required", path, i))
		}
		if strings.TrimSpace(ref.APIURL) != "" {
			parsed, err := url.Parse(ref.APIURL)
			if err != nil || parsed.Scheme == "" || parsed.Host == "" {
				*validationErrors = append(*validationErrors, fmt.Sprintf("%s[%d].apiURL must include a scheme and host", path, i))
			}
		}
	}
}

// validateGKEConfig validates GKE-specific configuration
func (r *AntflyCluster) validateGKEConfig() error {
	if r.Spec.GKE == nil {
		return nil
	}

	gke := r.Spec.GKE

	// Check Autopilot requirement first — this gives the most helpful error
	if gke.AutopilotComputeClass != "" && !gke.Autopilot {
		return fmt.Errorf(`spec.gke.autopilotComputeClass is set but spec.gke.autopilot=false

Problem: Compute classes only work with GKE Autopilot clusters.

Solution: Either:
  Option 1 (Use Autopilot): Set spec.gke.autopilot=true
  Option 2 (Standard GKE): Remove spec.gke.autopilotComputeClass and use spec.*.useSpotPods instead`)
	}

	// Validate compute class enum (only if non-empty)
	if gke.AutopilotComputeClass != "" {
		validClasses := []string{"Accelerator", "Balanced", "Performance", "Scale-Out", "autopilot", "autopilot-spot"}
		if !slices.Contains(validClasses, gke.AutopilotComputeClass) {
			return fmt.Errorf("invalid GKE Autopilot compute class '%s'. Must be one of: %s",
				gke.AutopilotComputeClass, strings.Join(validClasses, ", "))
		}
	}

	// Validate Accelerator compute class requires GPU
	if gke.AutopilotComputeClass == "Accelerator" {
		hasGPU := false
		if r.isStandaloneMode() {
			hasGPU = hasGPUInResourceSpec(r.Spec.Standalone.Resources)
		} else {
			// Check if metadata nodes have GPU
			if hasGPUInResourceSpec(r.Spec.MetadataNodes.Resources) {
				hasGPU = true
			}

			// Check if data nodes have GPU
			if hasGPUInResourceSpec(r.Spec.DataNodes.Resources) {
				hasGPU = true
			}
		}

		if !hasGPU {
			return fmt.Errorf(`spec.gke.autopilotComputeClass='Accelerator' requires GPU resources

Problem: GKE Autopilot's Accelerator compute class is for GPU/TPU workloads.
Your cluster spec does not include GPU resource requests.

Solution: Add GPU resources to metadataNodes or dataNodes, or use a different compute class.

Example:
  spec:
    dataNodes:
      resources:
        limits:
          gpu: "1"     # ADD THIS
          memory: "8Gi"
          cpu: "2"
    gke:
      autopilot: true
      autopilotComputeClass: "Accelerator"`)
		}
	}

	return nil
}

// validateEKSConfig validates AWS EKS-specific configuration
func (r *AntflyCluster) validateEKSConfig() error {
	if r.Spec.EKS == nil || !r.Spec.EKS.Enabled {
		return nil
	}

	eks := r.Spec.EKS

	// Validate IRSA role ARN format if specified
	if eks.IRSARoleARN != "" {
		// AWS IAM Role ARN format: arn:aws:iam::<account-id>:role/<role-name>
		// Also supports arn:aws-cn (China) and arn:aws-us-gov (GovCloud)
		if !irsaARNPattern.MatchString(eks.IRSARoleARN) {
			return fmt.Errorf(`invalid IRSA role ARN format: '%s'

Problem: The IRSARoleARN must be a valid AWS IAM role ARN.

Expected format: arn:aws:iam::<account-id>:role/<role-name>

Example:
  spec:
    eks:
      enabled: true
      irsaRoleARN: "arn:aws:iam::123456789012:role/antfly-backup-role"`, eks.IRSARoleARN)
		}
	}

	// Validate EBS volume type enum
	if eks.EBSVolumeType != "" {
		validEBSTypes := []string{"gp3", "gp2", "io1", "io2", "st1", "sc1"}
		if !slices.Contains(validEBSTypes, eks.EBSVolumeType) {
			return fmt.Errorf("invalid EBS volume type '%s'. Must be one of: %s",
				eks.EBSVolumeType, strings.Join(validEBSTypes, ", "))
		}
	}

	// Validate EBS IOPS is only set for io1/io2 volumes (skip if volume type unset — defaults vary)
	if eks.EBSIOPs != nil && eks.EBSVolumeType != "" {
		if eks.EBSVolumeType != "io1" && eks.EBSVolumeType != "io2" {
			return fmt.Errorf(`spec.eks.ebsIOPs is set but ebsVolumeType is '%s'

Problem: Provisioned IOPS can only be specified for io1 or io2 volume types.

Solution: Either:
  Option 1: Change ebsVolumeType to 'io1' or 'io2'
  Option 2: Remove the ebsIOPs field`, eks.EBSVolumeType)
		}
	}

	// Validate EBS Throughput is only set for gp3 volumes (skip if volume type unset — defaults vary)
	if eks.EBSThroughput != nil {
		if eks.EBSVolumeType != "gp3" && eks.EBSVolumeType != "" {
			return fmt.Errorf(`spec.eks.ebsThroughput is set but ebsVolumeType is '%s'

Problem: Throughput can only be specified for gp3 volume types.

Solution: Either:
  Option 1: Change ebsVolumeType to 'gp3'
  Option 2: Remove the ebsThroughput field`, eks.EBSVolumeType)
		}
		// Validate throughput range (125-1000 MiB/s for gp3) — only when type is explicitly gp3
		if eks.EBSVolumeType == "gp3" && (*eks.EBSThroughput < 125 || *eks.EBSThroughput > 1000) {
			return fmt.Errorf("spec.eks.ebsThroughput must be between 125 and 1000 MiB/s, got %d", *eks.EBSThroughput)
		}
	}

	// Validate KMS key ID requires encryption to be enabled
	if eks.EBSKmsKeyId != "" && !eks.EBSEncrypted {
		return fmt.Errorf(`spec.eks.ebsKmsKeyId is set but ebsEncrypted is false

Problem: KMS key ID is only used when EBS encryption is enabled.

Solution: Either:
  Option 1: Set ebsEncrypted to true
  Option 2: Remove the ebsKmsKeyId field`)
	}

	// Validate instance types format (basic validation)
	for _, instanceType := range eks.InstanceTypes {
		if instanceType == "" {
			return fmt.Errorf("spec.eks.instanceTypes contains an empty string")
		}
		// Basic format validation: should match patterns like m5.large, c5.xlarge, u-6tb1.56xlarge, etc.
		if !ec2InstancePattern.MatchString(instanceType) {
			return fmt.Errorf(`invalid instance type format: '%s'

Problem: Instance type should follow AWS naming convention.

Expected format: <family><generation>.<size>
Examples: m5.large, c5.xlarge, r6i.2xlarge, t3.medium`, instanceType)
		}
	}

	return nil
}

// validateNoConflictingCloudProviders validates that GKE and EKS are not both enabled
func (r *AntflyCluster) validateNoConflictingCloudProviders() error {
	gkeEnabled := r.Spec.GKE != nil && r.Spec.GKE.Autopilot
	eksEnabled := r.Spec.EKS != nil && r.Spec.EKS.Enabled

	if gkeEnabled && eksEnabled {
		return fmt.Errorf(`both spec.gke.autopilot=true and spec.eks.enabled=true are set

Problem: A cluster cannot be configured for both GKE and EKS simultaneously.

Solution: Enable only one cloud provider configuration:
  Option 1 (GKE): Remove or set spec.eks.enabled=false
  Option 2 (EKS): Remove or set spec.gke.autopilot=false`)
	}

	return nil
}

// validateNoConflictingSettings validates that useSpotPods doesn't conflict with Autopilot
func (r *AntflyCluster) validateNoConflictingSettings() error {
	if r.Spec.GKE == nil || !r.Spec.GKE.Autopilot {
		return nil
	}

	if r.isStandaloneMode() {
		if len(r.Spec.Standalone.NodeSelector) > 0 {
			return fmt.Errorf(`spec.standalone.nodeSelector conflicts with spec.gke.autopilot=true

Problem: GKE Autopilot manages node scheduling via compute classes, not node selectors.
Any custom nodeSelector values will be overridden.

Solution: Remove spec.standalone.nodeSelector when using GKE Autopilot.
Use spec.gke.autopilotComputeClass to control scheduling instead`)
		}
		return nil
	}

	// Check metadata nodes
	if r.Spec.MetadataNodes.UseSpotPods {
		return fmt.Errorf(`spec.metadataNodes.useSpotPods=true conflicts with spec.gke.autopilot=true

Problem: GKE Autopilot uses compute classes for spot scheduling, not node selectors.

Solution: Remove 'useSpotPods: true' and use 'gke.autopilotComputeClass: autopilot-spot' instead

Example:
  spec:
    metadataNodes:
      # useSpotPods: true  # REMOVE THIS
    gke:
      autopilot: true
      autopilotComputeClass: 'autopilot-spot'  # ADD THIS`)
	}

	// Check data nodes
	if r.Spec.DataNodes.UseSpotPods {
		return fmt.Errorf(`spec.dataNodes.useSpotPods=true conflicts with spec.gke.autopilot=true

Problem: GKE Autopilot uses compute classes for spot scheduling, not node selectors.

Solution: Remove 'useSpotPods: true' and use 'gke.autopilotComputeClass: autopilot-spot' instead

Example:
  spec:
    dataNodes:
      # useSpotPods: true  # REMOVE THIS
    gke:
      autopilot: true
      autopilotComputeClass: 'autopilot-spot'  # ADD THIS`)
	}

	// GKE Autopilot overrides node selectors with compute class annotations.
	// User-specified node selectors would be silently dropped.
	if len(r.Spec.MetadataNodes.NodeSelector) > 0 {
		return fmt.Errorf(`spec.metadataNodes.nodeSelector conflicts with spec.gke.autopilot=true

Problem: GKE Autopilot manages node scheduling via compute classes, not node selectors.
Any custom nodeSelector values will be overridden.

Solution: Remove spec.metadataNodes.nodeSelector when using GKE Autopilot.
Use spec.gke.autopilotComputeClass to control scheduling instead`)
	}

	if len(r.Spec.DataNodes.NodeSelector) > 0 {
		return fmt.Errorf(`spec.dataNodes.nodeSelector conflicts with spec.gke.autopilot=true

Problem: GKE Autopilot manages node scheduling via compute classes, not node selectors.
Any custom nodeSelector values will be overridden.

Solution: Remove spec.dataNodes.nodeSelector when using GKE Autopilot.
Use spec.gke.autopilotComputeClass to control scheduling instead`)
	}

	return nil
}

// validateNodeCounts validates that replica counts are valid.
// Metadata nodes run Raft consensus and require an odd number of replicas >= 1
// for quorum. Data nodes just need non-negative counts.
func (r *AntflyCluster) validateNodeCounts() error {
	if r.isStandaloneMode() {
		if r.Spec.Standalone.Replicas < 1 {
			return fmt.Errorf("spec.standalone.replicas must be >= 1, got %d", r.Spec.Standalone.Replicas)
		}
		if r.Spec.Standalone.NodeID < 1 {
			return fmt.Errorf("spec.standalone.nodeID must be >= 1, got %d", r.Spec.Standalone.NodeID)
		}
		return nil
	}

	if r.Spec.MetadataNodes.Replicas < 1 {
		//nolint:staticcheck // ST1005: intentionally capitalized user-facing webhook error
		return fmt.Errorf(`spec.metadataNodes.replicas must be >= 1, got %d

Problem: At least one metadata node is required for the cluster to function.

Solution: Set spec.metadataNodes.replicas to an odd number (1, 3, or 5 recommended for Raft quorum).`, r.Spec.MetadataNodes.Replicas)
	}

	if r.Spec.MetadataNodes.Replicas%2 == 0 {
		return fmt.Errorf(`spec.metadataNodes.replicas must be odd for Raft consensus, got %d

Problem: Metadata nodes use Raft consensus which requires an odd number of replicas
to maintain quorum. An even number (e.g. 2) provides no fault-tolerance advantage
over one fewer node and wastes resources.

Solution: Use an odd replica count:
  1 - Development/testing (no fault tolerance)
  3 - Production (tolerates 1 failure)
  5 - High availability (tolerates 2 failures)`, r.Spec.MetadataNodes.Replicas)
	}

	if r.Spec.DataNodes.Replicas < 0 {
		return fmt.Errorf("spec.dataNodes.replicas must be >= 0, got %d", r.Spec.DataNodes.Replicas)
	}

	if r.Spec.DataNodes.Suspend && r.Spec.DataNodes.AutoScaling != nil && r.Spec.DataNodes.AutoScaling.Enabled {
		return fmt.Errorf(`spec.dataNodes.suspend conflicts with spec.dataNodes.autoScaling.enabled=true

Problem: Suspension is an explicit pause/resume operation, while autoscaling continuously manages the data replica target.

Solution: Disable data-node autoscaling before suspending the data StatefulSet`)
	}

	return nil
}

// ValidateImmutability validates that immutable fields haven't changed
func (r *AntflyCluster) ValidateImmutability(old *AntflyCluster) error {
	var errors []string

	oldMode := old.effectiveMode()
	newMode := r.effectiveMode()
	if newMode != oldMode {
		errors = append(errors, fmt.Sprintf(
			`field 'spec.mode' is immutable after deployment

Problem: Changing topology mode requires replacing the workload shape and storage layout.

Solution: Delete and recreate the cluster to change this setting.

Current value: "%s"
Attempted change: "%s"`,
			oldMode, newMode))
	}

	oldStorageEngine := effectiveStorageEngine(old.Spec.Storage)
	newStorageEngine := effectiveStorageEngine(r.Spec.Storage)
	if newStorageEngine != oldStorageEngine {
		errors = append(errors, fmt.Sprintf(
			`field 'spec.storage.engine' is immutable after deployment

Problem: Changing the storage engine changes the on-disk format. Restarting the existing workload with a different engine can make its data inaccessible or initialize an empty database.

Solution: Back up the cluster, create a new cluster with the desired storage engine, and restore the backup.

Current value: "%s"
Attempted change: "%s"`,
			oldStorageEngine, newStorageEngine))
	}

	if oldStorageEngine == "lite" && newStorageEngine == "lite" {
		oldFileName := effectiveLiteFileName(old.Spec.Storage)
		newFileName := effectiveLiteFileName(r.Spec.Storage)
		if newFileName != oldFileName {
			errors = append(errors, fmt.Sprintf(
				`field 'spec.storage.liteFileName' is immutable after deployment

Problem: Changing the Lite filename points the workload at a different database file and can initialize an empty database.

Solution: Use backup and restore to migrate to a differently named Lite file.

Current value: "%s"
Attempted change: "%s"`,
				oldFileName, newFileName))
		}
	}

	// Check if both old and new have GKE config
	if r.Spec.GKE != nil && old.Spec.GKE != nil {
		// Check Autopilot mode immutability
		if r.Spec.GKE.Autopilot != old.Spec.GKE.Autopilot {
			errors = append(errors, fmt.Sprintf(
				`field 'spec.gke.autopilot' is immutable after deployment

Problem: Changing Autopilot mode requires pod recreation, which risks data loss.

Solution: Delete and recreate the cluster to change this setting.

Current value: %v
Attempted change: %v`,
				old.Spec.GKE.Autopilot, r.Spec.GKE.Autopilot))
		}

		// Check compute class immutability (only when Autopilot is enabled)
		if r.Spec.GKE.Autopilot && r.Spec.GKE.AutopilotComputeClass != old.Spec.GKE.AutopilotComputeClass {
			errors = append(errors, fmt.Sprintf(
				`field 'spec.gke.autopilotComputeClass' is immutable after deployment

Problem: Changing compute class requires pod recreation, which risks Raft quorum loss.

Solution: Delete and recreate the cluster to change this setting.

Current value: "%s"
Attempted change: "%s"`,
				old.Spec.GKE.AutopilotComputeClass, r.Spec.GKE.AutopilotComputeClass))
		}
	}

	// Check if both old and new have EKS config
	if r.Spec.EKS != nil && old.Spec.EKS != nil {
		// Check EKS enabled immutability
		if r.Spec.EKS.Enabled != old.Spec.EKS.Enabled {
			errors = append(errors, fmt.Sprintf(
				`field 'spec.eks.enabled' is immutable after deployment

Problem: Changing EKS mode requires pod recreation, which risks data loss.

Solution: Delete and recreate the cluster to change this setting.

Current value: %v
Attempted change: %v`,
				old.Spec.EKS.Enabled, r.Spec.EKS.Enabled))
		}
	}

	// Check if EKS section was added after initial creation (old had no EKS section at all)
	if r.Spec.EKS != nil && r.Spec.EKS.Enabled && old.Spec.EKS == nil {
		errors = append(errors, `field 'spec.eks.enabled' cannot be enabled after cluster creation

Problem: Enabling EKS mode on an existing cluster requires pod recreation, which risks data loss.

Solution: Delete and recreate the cluster with EKS configuration.`)
	}

	// Check if GKE section was added after initial creation (old had no GKE section at all)
	if r.Spec.GKE != nil && r.Spec.GKE.Autopilot && old.Spec.GKE == nil {
		errors = append(errors, `field 'spec.gke.autopilot' cannot be enabled after cluster creation

Problem: Enabling GKE Autopilot mode on an existing cluster requires pod recreation, which risks data loss.

Solution: Delete and recreate the cluster with GKE Autopilot configuration.`)
	}

	// Check if GKE section was removed after creation (new has no GKE section at all)
	if old.Spec.GKE != nil && old.Spec.GKE.Autopilot && r.Spec.GKE == nil {
		errors = append(errors, `cannot remove spec.gke configuration after deployment when autopilot was enabled

Problem: Removing GKE Autopilot configuration would change the scheduling behavior.

Solution: Delete and recreate the cluster to change this setting.`)
	}

	// Check storage class immutability
	if r.Spec.Storage.StorageClass != old.Spec.Storage.StorageClass {
		errors = append(errors, fmt.Sprintf(
			`field 'spec.storage.storageClass' is immutable after deployment

Problem: Changing the StorageClass requires recreating PVCs, which risks data loss.
Existing PVCs are bound to the original StorageClass.

Solution: Delete and recreate the cluster to change the StorageClass.

Current value: "%s"
Attempted change: "%s"`,
			old.Spec.Storage.StorageClass, r.Spec.Storage.StorageClass))
	}

	if newMode == ClusterModeDistributed && oldMode == ClusterModeDistributed {
		if r.Spec.MetadataNodes.Replicas < old.Spec.MetadataNodes.Replicas {
			errors = append(errors, fmt.Sprintf(
				`field 'spec.metadataNodes.replicas' cannot be decreased yet (current: %d, attempted: %d)

Problem: Metadata nodes are quorum-bearing. The operator does not yet have a quorum-aware metadata scale-down workflow.

Solution: Keep the existing metadata replica count, or recreate the cluster with the smaller topology.`,
				old.Spec.MetadataNodes.Replicas, r.Spec.MetadataNodes.Replicas))
		}

		// Data-node scale-down is mutable: the controller drains and deregisters
		// one highest ordinal at a time before shrinking the StatefulSet.
	}

	// Check storage size decrease (increases are allowed for online expansion)
	// Use resource.Quantity comparison instead of string comparison to handle
	// cases like "8Gi" → "10Gi" correctly (string comparison would reject this).
	if old.Spec.Storage.MetadataStorage != "" && r.Spec.Storage.MetadataStorage != "" {
		oldQ, errOld := resource.ParseQuantity(old.Spec.Storage.MetadataStorage)
		newQ, errNew := resource.ParseQuantity(r.Spec.Storage.MetadataStorage)
		if errNew != nil {
			errors = append(errors, fmt.Sprintf(
				"spec.storage.metadataStorage: %q is not a valid storage quantity", r.Spec.Storage.MetadataStorage))
		} else if errOld == nil && newQ.Cmp(oldQ) < 0 {
			errors = append(errors, fmt.Sprintf(
				`field 'spec.storage.metadataStorage' cannot be decreased (current: %s, attempted: %s)

Problem: PVC storage size cannot be reduced. Kubernetes only supports volume expansion, not shrinking.`,
				old.Spec.Storage.MetadataStorage, r.Spec.Storage.MetadataStorage))
		}
	}
	if old.Spec.Storage.DataStorage != "" && r.Spec.Storage.DataStorage != "" {
		oldQ, errOld := resource.ParseQuantity(old.Spec.Storage.DataStorage)
		newQ, errNew := resource.ParseQuantity(r.Spec.Storage.DataStorage)
		if errNew != nil {
			errors = append(errors, fmt.Sprintf(
				"spec.storage.dataStorage: %q is not a valid storage quantity", r.Spec.Storage.DataStorage))
		} else if errOld == nil && newQ.Cmp(oldQ) < 0 {
			errors = append(errors, fmt.Sprintf(
				`field 'spec.storage.dataStorage' cannot be decreased (current: %s, attempted: %s)

Problem: PVC storage size cannot be reduced. Kubernetes only supports volume expansion, not shrinking.`,
				old.Spec.Storage.DataStorage, r.Spec.Storage.DataStorage))
		}
	}
	if old.Spec.Storage.StandaloneStorage != "" && r.Spec.Storage.StandaloneStorage != "" {
		oldQ, errOld := resource.ParseQuantity(old.Spec.Storage.StandaloneStorage)
		newQ, errNew := resource.ParseQuantity(r.Spec.Storage.StandaloneStorage)
		if errNew != nil {
			errors = append(errors, fmt.Sprintf(
				"spec.storage.standaloneStorage: %q is not a valid storage quantity", r.Spec.Storage.StandaloneStorage))
		} else if errOld == nil && newQ.Cmp(oldQ) < 0 {
			errors = append(errors, fmt.Sprintf(
				`field 'spec.storage.standaloneStorage' cannot be decreased (current: %s, attempted: %s)

Problem: PVC storage size cannot be reduced. Kubernetes only supports volume expansion, not shrinking.`,
				old.Spec.Storage.StandaloneStorage, r.Spec.Storage.StandaloneStorage))
		}
	}
	if old.Spec.HighAvailability != nil && old.Spec.HighAvailability.Admin != nil &&
		r.Spec.HighAvailability != nil && r.Spec.HighAvailability.Admin != nil &&
		r.Spec.HighAvailability.Admin.RetryGeneration < old.Spec.HighAvailability.Admin.RetryGeneration {
		errors = append(errors, fmt.Sprintf(
			"spec.highAvailability.admin.retryGeneration cannot decrease (current: %d, attempted: %d)",
			old.Spec.HighAvailability.Admin.RetryGeneration,
			r.Spec.HighAvailability.Admin.RetryGeneration,
		))
	}

	if len(errors) > 0 {
		return fmt.Errorf("%s", strings.Join(errors, "\n\n"))
	}

	return nil
}

func effectiveStorageEngine(storage StorageSpec) string {
	if storage.Engine == "" {
		return "local"
	}
	return storage.Engine
}

func effectiveLiteFileName(storage StorageSpec) string {
	if storage.LiteFileName == "" {
		return "antfly.aflite"
	}
	return storage.LiteFileName
}

// validatePublicAPIConfig validates PublicAPI configuration
func (r *AntflyCluster) validatePublicAPIConfig() error {
	if r.Spec.PublicAPI == nil {
		return nil
	}

	publicAPI := r.Spec.PublicAPI

	// Validate ServiceType enum (if specified)
	if publicAPI.ServiceType != nil {
		validTypes := []corev1.ServiceType{
			corev1.ServiceTypeClusterIP,
			corev1.ServiceTypeNodePort,
			corev1.ServiceTypeLoadBalancer,
		}
		valid := slices.Contains(validTypes, *publicAPI.ServiceType)
		if !valid {
			return fmt.Errorf("spec.publicAPI.serviceType must be one of: ClusterIP, NodePort, LoadBalancer")
		}
	}

	// Validate NodePort only specified for NodePort or LoadBalancer service types
	if publicAPI.NodePort != nil {
		if publicAPI.ServiceType == nil {
			// This shouldn't happen after defaults are applied, but validate anyway
			return fmt.Errorf("spec.publicAPI.nodePort can only be set when serviceType is NodePort or LoadBalancer")
		}

		serviceType := *publicAPI.ServiceType
		if serviceType != corev1.ServiceTypeNodePort && serviceType != corev1.ServiceTypeLoadBalancer {
			return fmt.Errorf(`spec.publicAPI.nodePort is set but serviceType is '%s'

Problem: The nodePort field can only be used with NodePort or LoadBalancer service types.

Solution: Either:
  Option 1: Change serviceType to 'NodePort'
  Option 2: Change serviceType to 'LoadBalancer' (nodePort will be auto-assigned)
  Option 3: Remove the nodePort field and use serviceType 'ClusterIP'

Current configuration:
  serviceType: %s
  nodePort: %d`, serviceType, serviceType, *publicAPI.NodePort)
		}

		// Validate NodePort is in valid range
		if *publicAPI.NodePort < 30000 || *publicAPI.NodePort > 32767 {
			return fmt.Errorf("spec.publicAPI.nodePort must be in range 30000-32767, got %d", *publicAPI.NodePort)
		}
	}

	// Validate Port is in valid range (if specified)
	if publicAPI.Port != 0 {
		if publicAPI.Port < 1 || publicAPI.Port > 65535 {
			return fmt.Errorf("spec.publicAPI.port must be in range 1-65535, got %d", publicAPI.Port)
		}
	}

	return nil
}

// validateEnvFrom validates the EnvFrom configuration for metadata and data nodes
func (r *AntflyCluster) validateEnvFrom() error {
	var errors []string

	if r.isStandaloneMode() {
		for i, source := range r.Spec.Standalone.EnvFrom {
			if err := validateEnvFromSource(source, fmt.Sprintf("spec.standalone.envFrom[%d]", i)); err != nil {
				errors = append(errors, err.Error())
			}
		}
		if len(errors) > 0 {
			return fmt.Errorf("EnvFrom validation failed:\n  - %s", strings.Join(errors, "\n  - "))
		}
		return nil
	}

	// Validate metadata nodes EnvFrom
	for i, source := range r.Spec.MetadataNodes.EnvFrom {
		if err := validateEnvFromSource(source, fmt.Sprintf("spec.metadataNodes.envFrom[%d]", i)); err != nil {
			errors = append(errors, err.Error())
		}
	}

	// Validate data nodes EnvFrom
	for i, source := range r.Spec.DataNodes.EnvFrom {
		if err := validateEnvFromSource(source, fmt.Sprintf("spec.dataNodes.envFrom[%d]", i)); err != nil {
			errors = append(errors, err.Error())
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("EnvFrom validation failed:\n  - %s", strings.Join(errors, "\n  - "))
	}

	return nil
}

// validateEnvFromSource validates a single EnvFromSource
func validateEnvFromSource(source corev1.EnvFromSource, path string) error {
	// Must have exactly one of SecretRef or ConfigMapRef
	hasSecretRef := source.SecretRef != nil
	hasConfigMapRef := source.ConfigMapRef != nil

	if hasSecretRef && hasConfigMapRef {
		return fmt.Errorf("%s: must specify exactly one of secretRef or configMapRef, not both", path)
	}

	if !hasSecretRef && !hasConfigMapRef {
		return fmt.Errorf("%s: must specify either secretRef or configMapRef", path)
	}

	// Validate SecretRef if present
	if hasSecretRef {
		if source.SecretRef.Name == "" {
			return fmt.Errorf("%s.secretRef.name: must not be empty", path)
		}
	}

	// Validate ConfigMapRef if present
	if hasConfigMapRef {
		if source.ConfigMapRef.Name == "" {
			return fmt.Errorf("%s.configMapRef.name: must not be empty", path)
		}
	}

	return nil
}

// validatePVCRetentionPolicy validates PVC retention policy cross-field constraints
func (r *AntflyCluster) validatePVCRetentionPolicy() error {
	if r.Spec.Storage.PVCRetentionPolicy == nil {
		return nil
	}

	policy := r.Spec.Storage.PVCRetentionPolicy

	if r.isStandaloneMode() {
		return nil
	}

	// Reject WhenScaled: Delete with autoscaling enabled
	if policy.WhenScaled == PVCRetentionDelete && r.Spec.DataNodes.AutoScaling != nil && r.Spec.DataNodes.AutoScaling.Enabled {
		return fmt.Errorf(`spec.storage.pvcRetentionPolicy.whenScaled=Delete conflicts with spec.dataNodes.autoScaling.enabled=true

Problem: The autoscaler could scale down data nodes, permanently destroying their PVCs.
When the autoscaler scales back up, new nodes must perform a full Raft snapshot resync
for every shard, which is expensive and temporarily reduces cluster fault tolerance.

Solution: Either:
  Option 1: Set spec.storage.pvcRetentionPolicy.whenScaled=Retain (recommended for autoscaling)
  Option 2: Disable autoscaling (spec.dataNodes.autoScaling.enabled=false)`)
	}

	if policy.WhenScaled == PVCRetentionDelete && r.Spec.DataNodes.Suspend {
		return fmt.Errorf(`spec.storage.pvcRetentionPolicy.whenScaled=Delete conflicts with spec.dataNodes.suspend=true

Problem: Suspension scales data pods to zero and relies on retained PVCs so the same ordinals can resume with their existing data.

Solution: Set spec.storage.pvcRetentionPolicy.whenScaled=Retain before suspending data nodes`)
	}

	return nil
}

func (r *AntflyCluster) validateAutoScalingConfig() error {
	if r.isStandaloneMode() || r.Spec.DataNodes.AutoScaling == nil {
		return nil
	}

	autoScaling := r.Spec.DataNodes.AutoScaling
	if !autoScaling.Enabled {
		return nil
	}

	var errors []string
	if autoScaling.MinReplicas < 1 {
		errors = append(errors, fmt.Sprintf("spec.dataNodes.autoScaling.minReplicas must be >= 1, got %d", autoScaling.MinReplicas))
	}
	if autoScaling.MaxReplicas < 1 {
		errors = append(errors, fmt.Sprintf("spec.dataNodes.autoScaling.maxReplicas must be >= 1, got %d", autoScaling.MaxReplicas))
	}
	if autoScaling.MinReplicas > autoScaling.MaxReplicas {
		errors = append(errors, fmt.Sprintf("spec.dataNodes.autoScaling.minReplicas (%d) cannot be greater than maxReplicas (%d)", autoScaling.MinReplicas, autoScaling.MaxReplicas))
	}
	if autoScaling.TargetCPUUtilizationPercentage != nil {
		target := *autoScaling.TargetCPUUtilizationPercentage
		if target < 1 || target > 100 {
			errors = append(errors, fmt.Sprintf("spec.dataNodes.autoScaling.targetCPUUtilizationPercentage must be between 1 and 100, got %d", target))
		}
	}
	if autoScaling.TargetMemoryUtilizationPercentage != nil {
		target := *autoScaling.TargetMemoryUtilizationPercentage
		if target < 1 || target > 100 {
			errors = append(errors, fmt.Sprintf("spec.dataNodes.autoScaling.targetMemoryUtilizationPercentage must be between 1 and 100, got %d", target))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("autoscaling validation failed:\n  - %s", strings.Join(errors, "\n  - "))
	}
	return nil
}

func defaultStorageAutoGrow(storage *StorageSpec) {
	if storage == nil || storage.StorageAutoGrow == nil || !storage.StorageAutoGrow.Enabled {
		return
	}
	if storage.StorageAutoGrow.GrowThresholdPercent == 0 {
		storage.StorageAutoGrow.GrowThresholdPercent = 85
	}
	if storage.StorageAutoGrow.GrowIncrement == "" {
		storage.StorageAutoGrow.GrowIncrement = "10Gi"
	}
}

func (r *AntflyCluster) validateStorageAutoGrowConfig() error {
	autoGrow := r.Spec.Storage.StorageAutoGrow
	if autoGrow == nil || !autoGrow.Enabled {
		return nil
	}
	defaulted := *autoGrow
	storage := r.Spec.Storage
	storage.StorageAutoGrow = &defaulted
	defaultStorageAutoGrow(&storage)
	autoGrow = storage.StorageAutoGrow

	var errors []string
	if autoGrow.GrowThresholdPercent < 1 || autoGrow.GrowThresholdPercent > 99 {
		errors = append(errors, fmt.Sprintf("spec.storage.storageAutoGrow.growThresholdPercent must be between 1 and 99, got %d", autoGrow.GrowThresholdPercent))
	}
	if autoGrow.GrowIncrement == "" {
		errors = append(errors, "spec.storage.storageAutoGrow.growIncrement is required when storage auto-grow is enabled")
	} else if q, err := resource.ParseQuantity(autoGrow.GrowIncrement); err != nil {
		errors = append(errors, fmt.Sprintf("spec.storage.storageAutoGrow.growIncrement: %q is not a valid resource quantity", autoGrow.GrowIncrement))
	} else if q.Sign() <= 0 {
		errors = append(errors, "spec.storage.storageAutoGrow.growIncrement must be greater than zero")
	}

	if r.isStandaloneMode() {
		maxSize := autoGrow.MaxStandaloneStorage
		if maxSize == "" {
			maxSize = autoGrow.MaxDataStorage
		}
		if maxSize == "" {
			errors = append(errors, "spec.storage.storageAutoGrow.maxStandaloneStorage or maxDataStorage is required when storage auto-grow is enabled in standalone mode")
		} else if _, err := resource.ParseQuantity(maxSize); err != nil {
			errors = append(errors, fmt.Sprintf("spec.storage.storageAutoGrow.maxStandaloneStorage: %q is not a valid resource quantity", maxSize))
		}
	} else if autoGrow.MaxDataStorage == "" {
		errors = append(errors, "spec.storage.storageAutoGrow.maxDataStorage is required when storage auto-grow is enabled in distributed mode")
	} else if _, err := resource.ParseQuantity(autoGrow.MaxDataStorage); err != nil {
		errors = append(errors, fmt.Sprintf("spec.storage.storageAutoGrow.maxDataStorage: %q is not a valid resource quantity", autoGrow.MaxDataStorage))
	}

	if len(errors) > 0 {
		return fmt.Errorf("storage auto-grow validation failed:\n  - %s", strings.Join(errors, "\n  - "))
	}
	return nil
}

func (r *AntflyCluster) validateProductTierMapping() error {
	tier := r.Spec.ProductTier
	if tier == nil {
		return nil
	}

	var errors []string
	validateTierToken := func(path, value string, required bool) {
		if value == "" {
			if required {
				errors = append(errors, fmt.Sprintf("%s is required when spec.productTier is set", path))
			}
			return
		}
		if len(value) > 128 || !productTierTokenPattern.MatchString(value) {
			errors = append(errors, fmt.Sprintf("%s must be 1-128 characters and contain only letters, numbers, '.', '_' or '-'", path))
		}
	}

	validateTierToken("spec.productTier.name", tier.Name, true)
	validateTierToken("spec.productTier.revision", tier.Revision, false)
	validateTierToken("spec.productTier.managedBy", tier.ManagedBy, false)
	validateTierToken("spec.productTier.standaloneTier", tier.StandaloneTier, false)
	validateTierToken("spec.productTier.metadataTier", tier.MetadataTier, false)
	validateTierToken("spec.productTier.dataTier", tier.DataTier, false)
	validateTierToken("spec.productTier.inferenceTier", tier.InferenceTier, false)

	if r.isStandaloneMode() {
		if r.Spec.Standalone == nil {
			errors = append(errors, "spec.standalone is required for a standalone product tier")
		} else {
			if !resourceSpecHasCPUAndMemory(r.Spec.Standalone.Resources) {
				errors = append(errors, "spec.standalone.resources must include cpu and memory requests or limits for a standalone product tier")
			}
			if r.Spec.Storage.StandaloneStorage == "" {
				errors = append(errors, "spec.storage.standaloneStorage is required for a standalone product tier")
			}
		}
	} else {
		if !resourceSpecHasCPUAndMemory(r.Spec.MetadataNodes.Resources) {
			errors = append(errors, "spec.metadataNodes.resources must include cpu and memory requests or limits for a distributed product tier")
		}
		if !resourceSpecHasCPUAndMemory(r.Spec.DataNodes.Resources) {
			errors = append(errors, "spec.dataNodes.resources must include cpu and memory requests or limits for a distributed product tier")
		}
		if r.Spec.Storage.MetadataStorage == "" {
			errors = append(errors, "spec.storage.metadataStorage is required for a distributed product tier")
		}
		if r.Spec.Storage.DataStorage == "" {
			errors = append(errors, "spec.storage.dataStorage is required for a distributed product tier")
		}
	}

	if tier.InferenceTier != "" {
		if r.Spec.Inference == nil {
			errors = append(errors, "spec.inference is required when spec.productTier.inferenceTier is set")
		} else if r.Spec.Inference.modeOrDefault() != AntflyInferenceModeManaged {
			errors = append(errors, "spec.inference.mode must be Managed when spec.productTier.inferenceTier is set")
		} else {
			for i, pool := range r.Spec.Inference.ManagedPools {
				if pool.Spec.Resources == nil {
					errors = append(errors, fmt.Sprintf("spec.inference.managedPools[%d].spec.resources is required when spec.productTier.inferenceTier is set", i))
				}
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("product tier validation failed:\n  - %s", strings.Join(errors, "\n  - "))
	}
	return nil
}

func resourceSpecHasCPUAndMemory(resources ResourceSpec) bool {
	hasCPU := resources.CPU != "" || resources.Limits.CPU != ""
	hasMemory := resources.Memory != "" || resources.Limits.Memory != ""
	return hasCPU && hasMemory
}

func (r *AntflyCluster) validateHighAvailabilitySpec() error {
	ha := r.Spec.HighAvailability
	if ha == nil {
		return nil
	}
	if ha.modeOrDefault() == HAModeDisabled {
		if highAvailabilityHasManagedConfig(ha) {
			return fmt.Errorf("high availability validation failed:\n  - spec.highAvailability.mode must be HotStandby when HA configuration fields are set")
		}
		return nil
	}

	var errors []string
	names := map[string]struct{}{}
	desiredNames := map[string]struct{}{}
	slotNames := map[string]int{}
	desiredStandbys := 0
	desiredStandbyWithRouteSelector := false
	for i, standby := range ha.Standbys {
		name := strings.TrimSpace(standby.Name)
		if name == "" {
			errors = append(errors, fmt.Sprintf("spec.highAvailability.standbys[%d].name is required", i))
			continue
		}
		if standby.Name != name {
			errors = append(errors, fmt.Sprintf("spec.highAvailability.standbys[%d].name must not have leading or trailing whitespace", i))
		} else if !validHAIdentifier(name) {
			errors = append(errors, fmt.Sprintf("spec.highAvailability.standbys[%d].name must be a valid HA identifier", i))
		}
		if _, exists := names[name]; exists {
			errors = append(errors, fmt.Sprintf("spec.highAvailability.standbys[%d].name %q is duplicated", i, name))
		}
		names[name] = struct{}{}
		slotName := strings.TrimSpace(standby.SlotName)
		if slotName == "" && standby.SlotName != "" {
			errors = append(errors, fmt.Sprintf("spec.highAvailability.standbys[%d].slotName must not be whitespace", i))
		} else if standby.SlotName != "" && standby.SlotName != slotName {
			errors = append(errors, fmt.Sprintf("spec.highAvailability.standbys[%d].slotName must not have leading or trailing whitespace", i))
		} else if standby.SlotName != "" && !validHAIdentifier(slotName) {
			errors = append(errors, fmt.Sprintf("spec.highAvailability.standbys[%d].slotName must be a valid HA identifier", i))
		}
		if slotName == "" {
			slotName = name
		}
		if slotName != "" {
			if first, exists := slotNames[slotName]; exists {
				errors = append(errors, fmt.Sprintf("spec.highAvailability.standbys[%d].slotName %q duplicates standby slot identity from spec.highAvailability.standbys[%d]", i, slotName, first))
			}
			slotNames[slotName] = i
		}
		errors = append(errors, validateHAAdminURL(standby.AdminURL, fmt.Sprintf("spec.highAvailability.standbys[%d].adminURL", i))...)
		errors = append(errors, validateHARouteSelector(standby.RouteSelector, fmt.Sprintf("spec.highAvailability.standbys[%d].routeSelector", i))...)
		errors = append(errors, validateHAOptionalPath(standby.SeedManifestPath, fmt.Sprintf("spec.highAvailability.standbys[%d].seedManifestPath", i))...)
		errors = append(errors, validateHAOptionalPath(standby.SeedContentRoot, fmt.Sprintf("spec.highAvailability.standbys[%d].seedContentRoot", i))...)
		if strings.TrimSpace(standby.SeedContentRoot) != "" && strings.TrimSpace(standby.SeedManifestPath) == "" {
			errors = append(errors, fmt.Sprintf("spec.highAvailability.standbys[%d].seedManifestPath is required when seedContentRoot is set", i))
		}
		if artifact := standby.SeedArtifact; artifact != nil {
			fieldPath := fmt.Sprintf("spec.highAvailability.standbys[%d].seedArtifact", i)
			errors = append(errors, validateHASeedArtifact(artifact, fieldPath)...)
			hasManifest := strings.TrimSpace(standby.SeedManifestPath) != ""
			hasContent := strings.TrimSpace(standby.SeedContentRoot) != ""
			targetOnly := standbyLocalTargetOnlySeedArtifactBound(ha, standby, artifact)
			if hasManifest != hasContent {
				errors = append(errors, fmt.Sprintf("spec.highAvailability.standbys[%d].seedManifestPath and seedContentRoot must either both be set or both be omitted for runtime-owned capture", i))
			}
			if targetOnly {
				if artifact.SourcePVC != nil {
					errors = append(errors, fmt.Sprintf("spec.highAvailability.standbys[%d].seedArtifact.sourcePVC must be omitted for standby-local target-only seed artifact", i))
				}
			} else if !hasManifest && !hasContent {
				if ha.Runtime == nil || ha.Runtime.Role != HARuntimeRolePrimary {
					errors = append(errors, fmt.Sprintf("spec.highAvailability.standbys[%d] runtime-owned seed capture requires runtime.role Primary", i))
				}
				if artifact.SourcePVC == nil {
					errors = append(errors, fmt.Sprintf("spec.highAvailability.standbys[%d].seedArtifact.sourcePVC is required for runtime-owned seed publication", i))
				} else {
					captureRoot := "/antflydb/ha/seed-captures"
					if ha.Runtime != nil && strings.TrimSpace(ha.Runtime.SeedCaptureRoot) != "" {
						captureRoot = ha.Runtime.SeedCaptureRoot
					}
					if !haPathWithinMount(captureRoot, artifact.SourcePVC.MountPath) {
						errors = append(errors, fmt.Sprintf("spec.highAvailability.standbys[%d] runtime seedCaptureRoot must be within seedArtifact.sourcePVC.mountPath", i))
					}
				}
			}
			if artifact.SourcePVC != nil && hasManifest && hasContent {
				if !haPathWithinMount(standby.SeedManifestPath, artifact.SourcePVC.MountPath) {
					errors = append(errors, fmt.Sprintf("spec.highAvailability.standbys[%d].seedManifestPath must be within seedArtifact.sourcePVC.mountPath", i))
				}
				if !haPathWithinMount(standby.SeedContentRoot, artifact.SourcePVC.MountPath) {
					errors = append(errors, fmt.Sprintf("spec.highAvailability.standbys[%d].seedContentRoot must be within seedArtifact.sourcePVC.mountPath", i))
				}
			}
			if artifact.TargetPVC != nil && !haPathWithinMount(artifact.StagingRoot, artifact.TargetPVC.MountPath) {
				errors = append(errors, fmt.Sprintf("spec.highAvailability.standbys[%d].seedArtifact.stagingRoot must be within targetPVC.mountPath", i))
			}
		}
		if standby.DropSlotOnRemoval && standbyDesiredBySpec(standby) {
			errors = append(errors, fmt.Sprintf("spec.highAvailability.standbys[%d].dropSlotOnRemoval requires desired=false", i))
		}
		if standbyDesiredBySpec(standby) {
			desiredStandbys++
			desiredNames[name] = struct{}{}
			if len(standby.RouteSelector) > 0 {
				desiredStandbyWithRouteSelector = true
			}
		}
	}

	if admin := ha.Admin; admin != nil {
		errors = append(errors, validateHAAdminURL(admin.PrimaryURL, "spec.highAvailability.admin.primaryURL")...)
		if strings.TrimSpace(admin.TokenEnvVar) == "" && admin.TokenEnvVar != "" {
			errors = append(errors, "spec.highAvailability.admin.tokenEnvVar must not be whitespace")
		}
		if tokenEnvVar := admin.TokenEnvVar; tokenEnvVar != "" && !envVarNamePattern.MatchString(tokenEnvVar) {
			errors = append(errors, "spec.highAvailability.admin.tokenEnvVar must be a valid environment variable name")
		}
		if admin.JobBackoffLimit != nil && *admin.JobBackoffLimit < 0 {
			errors = append(errors, "spec.highAvailability.admin.jobBackoffLimit must not be negative")
		}
		if admin.JobTimeoutSeconds != nil && *admin.JobTimeoutSeconds <= 0 {
			errors = append(errors, "spec.highAvailability.admin.jobTimeoutSeconds must be greater than 0")
		}
		if admin.JobTTLSecondsAfterFinished != nil && *admin.JobTTLSecondsAfterFinished < 0 {
			errors = append(errors, "spec.highAvailability.admin.jobTTLSecondsAfterFinished must not be negative")
		}
		if admin.DirectRetryLimit != nil && *admin.DirectRetryLimit <= 0 {
			errors = append(errors, "spec.highAvailability.admin.directRetryLimit must be greater than 0")
		}
		if admin.DirectRetryBaseSeconds != nil && *admin.DirectRetryBaseSeconds <= 0 {
			errors = append(errors, "spec.highAvailability.admin.directRetryBaseSeconds must be greater than 0")
		}
		if admin.DirectRetryMaxSeconds != nil && *admin.DirectRetryMaxSeconds <= 0 {
			errors = append(errors, "spec.highAvailability.admin.directRetryMaxSeconds must be greater than 0")
		}
		effectiveRetryBase := int32(5)
		if admin.DirectRetryBaseSeconds != nil {
			effectiveRetryBase = *admin.DirectRetryBaseSeconds
		}
		effectiveRetryMaximum := int32(120)
		if admin.DirectRetryMaxSeconds != nil {
			effectiveRetryMaximum = *admin.DirectRetryMaxSeconds
		}
		if effectiveRetryMaximum < effectiveRetryBase {
			errors = append(errors, "spec.highAvailability.admin.directRetryMaxSeconds must be greater than or equal to directRetryBaseSeconds")
		}
		if admin.DirectReservationSeconds != nil && *admin.DirectReservationSeconds <= 0 {
			errors = append(errors, "spec.highAvailability.admin.directReservationSeconds must be greater than 0")
		}
		if admin.DirectPrerequisiteTimeoutSeconds != nil && *admin.DirectPrerequisiteTimeoutSeconds <= 0 {
			errors = append(errors, "spec.highAvailability.admin.directPrerequisiteTimeoutSeconds must be greater than 0")
		}
		if admin.RetryGeneration < 0 {
			errors = append(errors, "spec.highAvailability.admin.retryGeneration must not be negative")
		}
		errors = append(errors, validateHAAdminJobPodSpec(admin)...)
		if admin.ExecutePlannedActions {
			if strings.TrimSpace(admin.PrimaryURL) == "" {
				errors = append(errors, "spec.highAvailability.admin.primaryURL is required when executePlannedActions is true")
			}
			if ha.Identity == nil {
				errors = append(errors, "spec.highAvailability.admin.executePlannedActions requires spec.highAvailability.identity")
			}
		}
	}

	if ha.Runtime != nil && r.effectiveMode() != ClusterModeStandalone {
		errors = append(errors, "spec.highAvailability.runtime is only supported when spec.mode=Standalone")
	}
	errors = append(errors, validateHARuntime(ha)...)
	errors = append(errors, r.validateHAStartupGate(ha)...)
	errors = append(errors, r.validateHARuntimeAdminTokenSource(ha)...)

	if identity := ha.Identity; identity != nil {
		if identity.ClusterID == 0 {
			errors = append(errors, "spec.highAvailability.identity.clusterID must be greater than 0")
		}
		if identity.TimelineID == 0 {
			errors = append(errors, "spec.highAvailability.identity.timelineID must be greater than 0")
		}
		if identity.Epoch == 0 {
			errors = append(errors, "spec.highAvailability.identity.epoch must be greater than 0")
		}
		if strings.TrimSpace(identity.CurrentPrimaryID) == "" {
			errors = append(errors, "spec.highAvailability.identity.currentPrimaryID is required")
		} else if identity.CurrentPrimaryID != strings.TrimSpace(identity.CurrentPrimaryID) {
			errors = append(errors, "spec.highAvailability.identity.currentPrimaryID must not have leading or trailing whitespace")
		} else if !validHAIdentifier(identity.CurrentPrimaryID) {
			errors = append(errors, "spec.highAvailability.identity.currentPrimaryID must be a valid HA identifier")
		}
	}

	if sync := ha.SyncPolicy; sync != nil {
		if sync.Required < 0 {
			errors = append(errors, "spec.highAvailability.syncPolicy.required must not be negative")
		}
		if sync.modeOrDefault() == HADurabilityModeAsync {
			if sync.Required != 0 {
				errors = append(errors, "spec.highAvailability.syncPolicy.required must be omitted when mode is Async")
			}
			if len(sync.StandbyNames) > 0 {
				errors = append(errors, "spec.highAvailability.syncPolicy.standbyNames must be omitted when mode is Async")
			}
			if sync.Selection != "" && sync.Selection != HAStandbySelectionAny {
				errors = append(errors, "spec.highAvailability.syncPolicy.selection must be Any or omitted when mode is Async")
			}
			if sync.FailurePolicy != "" && sync.FailurePolicy != HAFailurePolicyBlock {
				errors = append(errors, "spec.highAvailability.syncPolicy.failurePolicy must be Block or omitted when mode is Async")
			}
		} else {
			if sync.requiredOrDefault() == 0 {
				errors = append(errors, "spec.highAvailability.syncPolicy.required must be at least 1 for synchronous modes")
			}
			if len(sync.StandbyNames) == 0 {
				errors = append(errors, "spec.highAvailability.syncPolicy.standbyNames is required for synchronous modes")
			}
			required := sync.requiredOrDefault()
			selection := sync.selectionOrDefault()
			if selection == HAStandbySelectionAll && sync.Required != 0 {
				errors = append(errors, "spec.highAvailability.syncPolicy.required must be omitted when selection is All")
			}
			if selection != HAStandbySelectionAll && int64(required) > int64(len(sync.StandbyNames)) {
				errors = append(errors, fmt.Sprintf("spec.highAvailability.syncPolicy.required (%d) cannot exceed standbyNames length (%d) for %s selection", required, len(sync.StandbyNames), selection))
			}
			seenSyncNames := map[string]int{}
			for i, name := range sync.StandbyNames {
				trimmedName := strings.TrimSpace(name)
				if trimmedName == "" {
					errors = append(errors, fmt.Sprintf("spec.highAvailability.syncPolicy.standbyNames[%d] is empty", i))
					continue
				}
				if name != trimmedName {
					errors = append(errors, fmt.Sprintf("spec.highAvailability.syncPolicy.standbyNames[%d] must not have leading or trailing whitespace", i))
				} else if !validHAIdentifier(trimmedName) {
					errors = append(errors, fmt.Sprintf("spec.highAvailability.syncPolicy.standbyNames[%d] must be a valid HA identifier", i))
				}
				if first, exists := seenSyncNames[trimmedName]; exists {
					errors = append(errors, fmt.Sprintf("spec.highAvailability.syncPolicy.standbyNames[%d] %q duplicates standbyNames[%d]", i, trimmedName, first))
				}
				seenSyncNames[trimmedName] = i
				if _, ok := names[trimmedName]; !ok {
					errors = append(errors, fmt.Sprintf("spec.highAvailability.syncPolicy.standbyNames[%d] %q is not declared in spec.highAvailability.standbys", i, name))
				} else if _, desired := desiredNames[trimmedName]; !desired {
					errors = append(errors, fmt.Sprintf("spec.highAvailability.syncPolicy.standbyNames[%d] %q must reference a desired standby", i, name))
				}
			}
		}
	}

	if failover := ha.AutomaticFailover; failover != nil && failover.Enabled {
		if len(names) == 0 {
			errors = append(errors, "spec.highAvailability.automaticFailover requires at least one declared standby")
		}
		if len(names) > 0 && desiredStandbys == 0 {
			errors = append(errors, "spec.highAvailability.automaticFailover requires at least one desired standby")
		}
		if !desiredStandbyWithRouteSelector {
			errors = append(errors, "spec.highAvailability.automaticFailover requires at least one desired standby with routeSelector")
		}
		fencingAuthority := failover.fencingAuthorityOrDefault()
		if fencingAuthority == HAFencingAuthorityNone {
			errors = append(errors, "spec.highAvailability.automaticFailover.fencingAuthority must not be None when automatic failover is enabled")
		} else if fencingAuthority != HAFencingAuthorityKubernetesLease {
			errors = append(errors, "spec.highAvailability.automaticFailover.fencingAuthority must be KubernetesLease for operator-managed automatic failover")
		}
		if ha.Admin == nil || !ha.Admin.ExecutePlannedActions {
			errors = append(errors, "spec.highAvailability.automaticFailover requires spec.highAvailability.admin.executePlannedActions=true")
		}
		for i, standby := range ha.Standbys {
			if standbyDesiredBySpec(standby) && strings.TrimSpace(standby.AdminURL) == "" {
				errors = append(errors, fmt.Sprintf("spec.highAvailability.standbys[%d].adminURL is required when automaticFailover is enabled", i))
			}
		}
		if ha.Identity == nil {
			errors = append(errors, "spec.highAvailability.automaticFailover requires spec.highAvailability.identity")
		}
		if fencingAuthority == HAFencingAuthorityKubernetesLease {
			if ha.Runtime == nil || ha.Runtime.FencingLease == nil {
				errors = append(errors, "spec.highAvailability.automaticFailover with KubernetesLease requires runtime.fencingLease shared by every topology member")
			} else if strings.TrimSpace(ha.Runtime.FencingLease.Name) == "" ||
				strings.TrimSpace(ha.Runtime.FencingLease.TopologyID) == "" {
				errors = append(errors, "spec.highAvailability.runtime.fencingLease.name and topologyID are required")
			} else if ha.Runtime.FencingLease.WatchdogGraceSeconds > 0 && ha.Runtime.FencingLease.WatchdogGraceSeconds < 10 {
				errors = append(errors, "spec.highAvailability.runtime.fencingLease.watchdogGraceSeconds must be at least 10 seconds so polling and request latency fit inside the authority window")
			} else if ha.Runtime.FencingLease.WatchdogGraceSeconds >= 30 {
				errors = append(errors, "spec.highAvailability.runtime.fencingLease.watchdogGraceSeconds must be less than the 30 second Lease duration")
			}
		}
		if !failover.requireRemoteApplyOrDefault() {
			errors = append(errors, "spec.highAvailability.automaticFailover.requireRemoteApply must be true for no-loss automatic failover")
		}
		if ha.SyncPolicy == nil || ha.SyncPolicy.modeOrDefault() != HADurabilityModeRemoteApply {
			errors = append(errors, "spec.highAvailability.automaticFailover requires syncPolicy.mode RemoteApply")
		} else if ha.SyncPolicy.FailurePolicy == HAFailurePolicyDegradeToAsync {
			errors = append(errors, "spec.highAvailability.automaticFailover requires syncPolicy.failurePolicy Block or FailClosed")
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("high availability validation failed:\n  - %s", strings.Join(errors, "\n  - "))
	}
	return nil
}

func validateHASeedArtifact(artifact *HASeedArtifactSpec, fieldPath string) []string {
	if artifact == nil {
		return nil
	}
	var errors []string
	hasTopologyBinding := strings.TrimSpace(artifact.TopologyID) != "" || artifact.TopologyGeneration != 0 ||
		strings.TrimSpace(artifact.NodeID) != "" || strings.TrimSpace(artifact.TargetPVCUID) != ""
	if hasTopologyBinding {
		if value := strings.TrimSpace(artifact.TopologyID); value == "" || value != artifact.TopologyID || !validHAIdentifier(value) {
			errors = append(errors, fmt.Sprintf("%s.topologyID must be a valid HA identifier when seed topology binding is configured", fieldPath))
		}
		if artifact.TopologyGeneration <= 0 {
			errors = append(errors, fmt.Sprintf("%s.topologyGeneration must be greater than zero when seed topology binding is configured", fieldPath))
		}
		if value := strings.TrimSpace(artifact.NodeID); value == "" || value != artifact.NodeID || !validHAIdentifier(value) {
			errors = append(errors, fmt.Sprintf("%s.nodeID must be a valid HA identifier when seed topology binding is configured", fieldPath))
		}
		if value := strings.TrimSpace(artifact.TargetPVCUID); value == "" || value != artifact.TargetPVCUID || containsASCIIWhitespace(value) {
			errors = append(errors, fmt.Sprintf("%s.targetPVCUID is required without whitespace when seed topology binding is configured", fieldPath))
		}
	}
	location := strings.TrimSpace(artifact.Location)
	if location == "" {
		errors = append(errors, fmt.Sprintf("%s.location is required", fieldPath))
	} else if location != artifact.Location || containsASCIIWhitespace(location) {
		errors = append(errors, fmt.Sprintf("%s.location must not contain leading, trailing, or embedded whitespace", fieldPath))
	} else if parsed, err := url.Parse(location); err != nil || parsed.Scheme == "" ||
		(parsed.Scheme != "s3" && parsed.Scheme != "gs" && parsed.Scheme != "file") {
		errors = append(errors, fmt.Sprintf("%s.location must be an s3://, gs://, or file:// URI", fieldPath))
	}
	if prefix := strings.TrimSpace(artifact.GenerationPrefix); prefix != "" {
		if prefix != artifact.GenerationPrefix || !validHAIdentifier(prefix) {
			errors = append(errors, fmt.Sprintf("%s.generationPrefix must be a valid HA identifier without surrounding whitespace", fieldPath))
		}
	}
	if generation := strings.TrimSpace(artifact.Generation); generation != "" {
		if generation != artifact.Generation || !validHAIdentifier(generation) {
			errors = append(errors, fmt.Sprintf("%s.generation must be a valid exact HA identifier without surrounding whitespace", fieldPath))
		}
	} else if artifact.Generation != "" {
		errors = append(errors, fmt.Sprintf("%s.generation must not be whitespace", fieldPath))
	}
	errors = append(errors, validateHAOptionalPath(artifact.StagingRoot, fieldPath+".stagingRoot")...)
	if strings.TrimSpace(artifact.StagingRoot) == "" {
		errors = append(errors, fmt.Sprintf("%s.stagingRoot is required", fieldPath))
	}
	if artifact.RetainGenerations < 0 {
		errors = append(errors, fmt.Sprintf("%s.retainGenerations must not be negative", fieldPath))
	}
	if ref := artifact.CredentialsSecretRef; ref != nil {
		name := strings.TrimSpace(ref.Name)
		if name == "" {
			errors = append(errors, fmt.Sprintf("%s.credentialsSecretRef.name is required", fieldPath))
		} else if name != ref.Name {
			errors = append(errors, fmt.Sprintf("%s.credentialsSecretRef.name must not have leading or trailing whitespace", fieldPath))
		} else if nameErrs := utilvalidation.IsDNS1123Subdomain(name); len(nameErrs) > 0 {
			errors = append(errors, fmt.Sprintf("%s.credentialsSecretRef.name %q is invalid: %s", fieldPath, name, strings.Join(nameErrs, "; ")))
		}
	}
	errors = append(errors, validateHASeedPVC(artifact.SourcePVC, fieldPath+".sourcePVC")...)
	errors = append(errors, validateHASeedPVC(artifact.TargetPVC, fieldPath+".targetPVC")...)
	return errors
}

func (r *AntflyCluster) validateHAStartupGate(ha *HighAvailabilitySpec) []string {
	if ha == nil || ha.Runtime == nil || ha.Runtime.StartupGate == nil {
		return nil
	}
	gate := ha.Runtime.StartupGate
	fieldPath := "spec.highAvailability.runtime.startupGate"
	var errors []string
	if ha.Runtime.Role != HARuntimeRoleStandby {
		errors = append(errors, fieldPath+" requires runtime.role Standby")
	}
	if gate.Policy == HAStartupGatePolicySuspend {
		if gate.RuntimeEligible {
			errors = append(errors, fieldPath+".policy Suspend requires runtimeEligible=false")
		}
		if gate.ReceiptMatchPolicy != "" {
			errors = append(errors, fieldPath+".receiptMatchPolicy must be omitted for Suspend")
		}
		if gate.RequiredReceipt != nil {
			errors = append(errors, fieldPath+".requiredReceipt must be omitted for Suspend")
		}
		return errors
	}
	if gate.Policy != HAStartupGatePolicyRequireActivatedSeed {
		errors = append(errors, fieldPath+".policy must be Suspend or RequireActivatedSeed")
	}
	if gate.ReceiptMatchPolicy != HAReceiptMatchPolicyExact {
		errors = append(errors, fieldPath+".receiptMatchPolicy must be Exact for RequireActivatedSeed")
	}
	if gate.RequiredReceipt == nil {
		errors = append(errors, fieldPath+".requiredReceipt is required for RequireActivatedSeed")
		return errors
	}
	required := *gate.RequiredReceipt

	validateID := func(value, name string) {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			errors = append(errors, fieldPath+".requiredReceipt."+name+" is required")
		} else if value != trimmed || !validHAIdentifier(trimmed) {
			errors = append(errors, fieldPath+".requiredReceipt."+name+" must be a valid HA identifier without surrounding whitespace")
		}
	}
	validateID(required.TopologyID, "topologyID")
	validateID(required.NodeID, "nodeID")
	validateID(required.SlotName, "slotName")
	validateID(required.Generation, "generation")
	expectedTopologyID := strings.TrimSpace(r.Name)
	topologySource := "metadata.name"
	if annotated, present := r.Annotations[haTopologyIDAnnotation]; present {
		expectedTopologyID = strings.TrimSpace(annotated)
		topologySource = "metadata.annotations[" + haTopologyIDAnnotation + "]"
		if annotated != expectedTopologyID || !validHAIdentifier(expectedTopologyID) {
			errors = append(errors, topologySource+" must be a valid HA identifier without surrounding whitespace")
		}
	}
	if strings.TrimSpace(required.TopologyID) != expectedTopologyID {
		errors = append(errors, fieldPath+".requiredReceipt.topologyID must match "+topologySource)
	}
	if required.TopologyGeneration < 0 {
		errors = append(errors, fieldPath+".requiredReceipt.topologyGeneration must not be negative")
	}
	if strings.TrimSpace(required.NodeID) != strings.TrimSpace(ha.Runtime.NodeID) {
		errors = append(errors, fieldPath+".requiredReceipt.nodeID must match runtime.nodeID")
	}
	if ha.Runtime.Standby == nil || strings.TrimSpace(required.SlotName) != strings.TrimSpace(ha.Runtime.Standby.SlotName) {
		errors = append(errors, fieldPath+".requiredReceipt.slotName must match runtime.standby.slotName")
	}

	targetPVCName := strings.TrimSpace(required.TargetPVCName)
	if targetPVCName == "" {
		errors = append(errors, fieldPath+".requiredReceipt.targetPVCName is required")
	} else if required.TargetPVCName != targetPVCName {
		errors = append(errors, fieldPath+".requiredReceipt.targetPVCName must not have leading or trailing whitespace")
	} else if nameErrs := utilvalidation.IsDNS1123Subdomain(targetPVCName); len(nameErrs) > 0 {
		errors = append(errors, fmt.Sprintf("%s.requiredReceipt.targetPVCName %q is invalid: %s", fieldPath, targetPVCName, strings.Join(nameErrs, "; ")))
	}

	var artifact *HASeedArtifactSpec
	for i := range ha.Standbys {
		standby := &ha.Standbys[i]
		slotName := strings.TrimSpace(standby.SlotName)
		if slotName == "" {
			slotName = strings.TrimSpace(standby.Name)
		}
		if slotName == strings.TrimSpace(required.SlotName) {
			artifact = standby.SeedArtifact
			break
		}
	}
	if artifact == nil {
		errors = append(errors, fieldPath+".requiredReceipt.slotName must reference a standby with seedArtifact")
	} else {
		if strings.TrimSpace(required.Generation) != strings.TrimSpace(artifact.Generation) {
			errors = append(errors, fieldPath+".requiredReceipt.generation must match seedArtifact.generation")
		}
		if artifact.TargetPVC == nil || targetPVCName != strings.TrimSpace(artifact.TargetPVC.ClaimName) {
			errors = append(errors, fieldPath+".requiredReceipt.targetPVCName must match seedArtifact.targetPVC.claimName")
		}
	}

	for name, value := range map[string]string{
		"manifestSHA256":    required.ManifestSHA256,
		"aggregateSHA256":   required.AggregateSHA256,
		"seedReceiptSHA256": required.SeedReceiptSHA256,
	} {
		if value != "" && !haSHA256Pattern.MatchString(value) {
			errors = append(errors, fieldPath+".requiredReceipt."+name+" must be a lowercase SHA-256 digest")
		}
	}
	if required.TargetPVCUID != "" && required.TargetPVCUID != strings.TrimSpace(required.TargetPVCUID) {
		errors = append(errors, fieldPath+".requiredReceipt.targetPVCUID must not have leading or trailing whitespace")
	}
	return errors
}

func validateHASeedPVC(pvc *HASeedArtifactPVCSpec, fieldPath string) []string {
	if pvc == nil {
		return nil
	}
	var errors []string
	claimName := strings.TrimSpace(pvc.ClaimName)
	if claimName == "" {
		errors = append(errors, fieldPath+".claimName is required")
	} else if claimName != pvc.ClaimName {
		errors = append(errors, fieldPath+".claimName must not have leading or trailing whitespace")
	} else if nameErrs := utilvalidation.IsDNS1123Subdomain(claimName); len(nameErrs) > 0 {
		errors = append(errors, fmt.Sprintf("%s.claimName %q is invalid: %s", fieldPath, claimName, strings.Join(nameErrs, "; ")))
	}
	if !filepath.IsAbs(pvc.MountPath) || filepath.Clean(pvc.MountPath) != pvc.MountPath {
		errors = append(errors, fieldPath+".mountPath must be an absolute normalized path")
	}
	return errors
}

func haPathWithinMount(value, mountPath string) bool {
	value = strings.TrimSpace(value)
	mountPath = filepath.Clean(strings.TrimSpace(mountPath))
	if value == "" || mountPath == "" || !filepath.IsAbs(value) || !filepath.IsAbs(mountPath) {
		return false
	}
	if mountPath == string(filepath.Separator) {
		return true
	}
	return value == mountPath || strings.HasPrefix(value, mountPath+string(filepath.Separator))
}

func validateHAAdminURL(raw string, fieldPath string) []string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		if raw != "" {
			return []string{fmt.Sprintf("%s must not be whitespace", fieldPath)}
		}
		return nil
	}
	if raw != trimmed {
		return []string{fmt.Sprintf("%s must not have leading or trailing whitespace", fieldPath)}
	}
	if containsASCIIWhitespace(raw) {
		return []string{fmt.Sprintf("%s must not contain whitespace", fieldPath)}
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return []string{fmt.Sprintf("%s must be an absolute http or https URL", fieldPath)}
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return []string{fmt.Sprintf("%s must use http or https", fieldPath)}
	}
	return nil
}

func containsASCIIWhitespace(raw string) bool {
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case ' ', '\t', '\n', '\r', '\v', '\f':
			return true
		}
	}
	return false
}

func validateHARuntime(ha *HighAvailabilitySpec) []string {
	if ha == nil || ha.Runtime == nil {
		return nil
	}
	runtime := ha.Runtime
	var errors []string
	errors = append(errors, validateHAOptionalPath(runtime.SeedCaptureRoot, "spec.highAvailability.runtime.seedCaptureRoot")...)
	nodeID := strings.TrimSpace(runtime.NodeID)
	currentPrimaryID := ""
	if ha.Identity != nil {
		currentPrimaryID = strings.TrimSpace(ha.Identity.CurrentPrimaryID)
	}
	switch runtime.Role {
	case HARuntimeRolePrimary:
		if nodeID != "" && currentPrimaryID != "" && nodeID != currentPrimaryID {
			errors = append(errors, "spec.highAvailability.runtime.nodeID must match spec.highAvailability.identity.currentPrimaryID when runtime.role is Primary")
		}
		if runtime.Standby != nil {
			errors = append(errors, "spec.highAvailability.runtime.standby may only be set when runtime.role is Standby")
		}
		if primary := runtime.Primary; primary != nil {
			errors = append(errors, validateHAOptionalPath(primary.LogPath, "spec.highAvailability.runtime.primary.logPath")...)
			errors = append(errors, validateHAOptionalPath(primary.SlotsPath, "spec.highAvailability.runtime.primary.slotsPath")...)
		}
	case HARuntimeRoleStandby:
		if nodeID != "" && currentPrimaryID != "" && nodeID == currentPrimaryID {
			errors = append(errors, "spec.highAvailability.runtime.nodeID must not match spec.highAvailability.identity.currentPrimaryID when runtime.role is Standby")
		}
		if runtime.Primary != nil {
			errors = append(errors, "spec.highAvailability.runtime.primary may only be set when runtime.role is Primary")
		}
		if standby := runtime.Standby; standby != nil {
			errors = append(errors, validateHAOptionalPath(standby.LogPath, "spec.highAvailability.runtime.standby.logPath")...)
			errors = append(errors, validateHAOptionalPath(standby.ProgressPath, "spec.highAvailability.runtime.standby.progressPath")...)
			errors = append(errors, validateHAAdminURL(standby.UpstreamURL, "spec.highAvailability.runtime.standby.upstreamURL")...)
			slotName := strings.TrimSpace(standby.SlotName)
			upstreamURL := strings.TrimSpace(standby.UpstreamURL)
			if slotName == "" && standby.SlotName != "" {
				errors = append(errors, "spec.highAvailability.runtime.standby.slotName must not be whitespace")
			} else if standby.SlotName != "" && standby.SlotName != slotName {
				errors = append(errors, "spec.highAvailability.runtime.standby.slotName must not have leading or trailing whitespace")
			} else if standby.SlotName != "" && !validHAIdentifier(slotName) {
				errors = append(errors, "spec.highAvailability.runtime.standby.slotName must be a valid HA identifier")
			}
			if upstreamURL != "" && slotName == "" {
				errors = append(errors, "spec.highAvailability.runtime.standby.slotName is required when upstreamURL is set")
			}
			if slotName != "" && upstreamURL == "" {
				errors = append(errors, "spec.highAvailability.runtime.standby.upstreamURL is required when slotName is set")
			}
		}
	default:
		errors = append(errors, "spec.highAvailability.runtime.role must be Primary or Standby")
	}
	if nodeID == "" {
		errors = append(errors, "spec.highAvailability.runtime.nodeID is required")
	} else if runtime.NodeID != nodeID {
		errors = append(errors, "spec.highAvailability.runtime.nodeID must not have leading or trailing whitespace")
	} else if !validHAIdentifier(nodeID) {
		errors = append(errors, "spec.highAvailability.runtime.nodeID must be a valid HA identifier")
	}
	errors = append(errors, validateHAOptionalPath(runtime.FencePath, "spec.highAvailability.runtime.fencePath")...)
	errors = append(errors, validateHAOptionalPath(runtime.FormerPrimaryLogPath, "spec.highAvailability.runtime.formerPrimaryLogPath")...)
	if strings.TrimSpace(runtime.AdminTokenEnvVar) == "" && runtime.AdminTokenEnvVar != "" {
		errors = append(errors, "spec.highAvailability.runtime.adminTokenEnvVar must not be whitespace")
	}
	if envVar := runtime.AdminTokenEnvVar; envVar != "" && !envVarNamePattern.MatchString(envVar) {
		errors = append(errors, "spec.highAvailability.runtime.adminTokenEnvVar must be a valid environment variable name")
	}
	if ref := runtime.AdminTokenSecretRef; ref != nil {
		if strings.TrimSpace(runtime.AdminTokenEnvVar) == "" {
			errors = append(errors, "spec.highAvailability.runtime.adminTokenEnvVar is required when adminTokenSecretRef is set")
		}
		refName := strings.TrimSpace(ref.Name)
		refKey := strings.TrimSpace(ref.Key)
		if refName == "" {
			errors = append(errors, "spec.highAvailability.runtime.adminTokenSecretRef.name is required")
		} else if ref.Name != refName {
			errors = append(errors, "spec.highAvailability.runtime.adminTokenSecretRef.name must not have leading or trailing whitespace")
		} else if nameErrs := utilvalidation.IsDNS1123Subdomain(refName); len(nameErrs) > 0 {
			errors = append(errors, fmt.Sprintf("spec.highAvailability.runtime.adminTokenSecretRef.name %q is invalid: %s", refName, strings.Join(nameErrs, "; ")))
		}
		if refKey == "" {
			errors = append(errors, "spec.highAvailability.runtime.adminTokenSecretRef.key is required")
		} else if ref.Key != refKey {
			errors = append(errors, "spec.highAvailability.runtime.adminTokenSecretRef.key must not have leading or trailing whitespace")
		} else if keyErrs := utilvalidation.IsConfigMapKey(refKey); len(keyErrs) > 0 {
			errors = append(errors, fmt.Sprintf("spec.highAvailability.runtime.adminTokenSecretRef.key %q is invalid: %s", refKey, strings.Join(keyErrs, "; ")))
		}
		if ref.Optional != nil && *ref.Optional {
			errors = append(errors, "spec.highAvailability.runtime.adminTokenSecretRef.optional must be false")
		}
	}
	if ha.Identity == nil {
		errors = append(errors, "spec.highAvailability.runtime requires spec.highAvailability.identity")
	}
	return errors
}

func validateHAOptionalTrimmedString(value string, fieldPath string) []string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		if value == "" {
			return nil
		}
		return []string{fmt.Sprintf("%s must not be whitespace", fieldPath)}
	}
	if value != trimmed {
		return []string{fmt.Sprintf("%s must not have leading or trailing whitespace", fieldPath)}
	}
	return nil
}

func validateHAOptionalPath(value string, fieldPath string) []string {
	if errors := validateHAOptionalTrimmedString(value, fieldPath); len(errors) > 0 || strings.TrimSpace(value) == "" {
		return errors
	}
	if !filepath.IsAbs(value) {
		return []string{fmt.Sprintf("%s must be an absolute normalized path", fieldPath)}
	}
	if filepath.Clean(value) != value {
		return []string{fmt.Sprintf("%s must be an absolute normalized path", fieldPath)}
	}
	return nil
}

func validHAIdentifier(value string) bool {
	return haIdentifierPattern.MatchString(value)
}

func (r *AntflyCluster) validateHARuntimeAdminTokenSource(ha *HighAvailabilitySpec) []string {
	if ha == nil || ha.Runtime == nil || strings.TrimSpace(ha.Runtime.AdminTokenEnvVar) == "" {
		return nil
	}
	if ha.Runtime.AdminTokenSecretRef != nil {
		return nil
	}
	if r.effectiveMode() == ClusterModeStandalone && r.Spec.Standalone != nil && len(r.Spec.Standalone.EnvFrom) > 0 {
		return nil
	}
	return []string{"spec.highAvailability.runtime.adminTokenEnvVar requires spec.highAvailability.runtime.adminTokenSecretRef or spec.standalone.envFrom"}
}

func validateHARouteSelector(selector map[string]string, fieldPath string) []string {
	if len(selector) == 0 {
		return nil
	}
	var errors []string
	for key, value := range selector {
		if keyErrs := utilvalidation.IsQualifiedName(key); len(keyErrs) > 0 {
			errors = append(errors, fmt.Sprintf("%s key %q is invalid: %s", fieldPath, key, strings.Join(keyErrs, "; ")))
		}
		if valueErrs := utilvalidation.IsValidLabelValue(value); len(valueErrs) > 0 {
			errors = append(errors, fmt.Sprintf("%s[%q] value %q is invalid: %s", fieldPath, key, value, strings.Join(valueErrs, "; ")))
		}
	}
	return errors
}

func validateHAAdminJobPodSpec(admin *HAAdminSpec) []string {
	var errors []string
	for i, source := range admin.EnvFrom {
		if err := validateEnvFromSource(source, fmt.Sprintf("spec.highAvailability.admin.envFrom[%d]", i)); err != nil {
			errors = append(errors, err.Error())
		}
	}
	volumes := map[string]struct{}{}
	for i, volume := range admin.Volumes {
		path := fmt.Sprintf("spec.highAvailability.admin.volumes[%d]", i)
		name := strings.TrimSpace(volume.Name)
		if name == "" {
			errors = append(errors, fmt.Sprintf("%s.name is required", path))
			continue
		}
		if nameErrs := utilvalidation.IsDNS1123Label(name); len(nameErrs) > 0 {
			errors = append(errors, fmt.Sprintf("%s.name %q is invalid: %s", path, name, strings.Join(nameErrs, "; ")))
			continue
		}
		if _, exists := volumes[name]; exists {
			errors = append(errors, fmt.Sprintf("%s.name %q is duplicated", path, name))
		}
		volumes[name] = struct{}{}
	}
	for i, mount := range admin.VolumeMounts {
		path := fmt.Sprintf("spec.highAvailability.admin.volumeMounts[%d]", i)
		name := strings.TrimSpace(mount.Name)
		if name == "" {
			errors = append(errors, fmt.Sprintf("%s.name is required", path))
			continue
		}
		if _, ok := volumes[name]; !ok {
			errors = append(errors, fmt.Sprintf("%s.name %q must reference spec.highAvailability.admin.volumes", path, name))
		}
		if strings.TrimSpace(mount.MountPath) == "" {
			errors = append(errors, fmt.Sprintf("%s.mountPath is required", path))
		}
	}
	return errors
}

func standbyDesiredBySpec(standby HAStandbySpec) bool {
	return standby.Desired == nil || *standby.Desired
}

// standbyLocalTargetOnlySeedArtifactBound recognizes the deliberately narrow
// seed descriptor rendered into a standby runtime CR. It is not a publication
// source: its only purpose is to bind the startup gate to the exact generation
// already activated on the target PVC. All other source-less artifacts retain
// the primary-only runtime capture validation path.
func standbyLocalTargetOnlySeedArtifactBound(ha *HighAvailabilitySpec, standby HAStandbySpec, artifact *HASeedArtifactSpec) bool {
	if ha == nil || ha.Runtime == nil || ha.Runtime.Role != HARuntimeRoleStandby ||
		standby.Desired == nil || *standby.Desired || artifact == nil ||
		strings.TrimSpace(standby.SeedManifestPath) != "" || strings.TrimSpace(standby.SeedContentRoot) != "" {
		return false
	}
	gate := ha.Runtime.StartupGate
	if gate == nil || gate.Policy != HAStartupGatePolicyRequireActivatedSeed ||
		gate.ReceiptMatchPolicy != HAReceiptMatchPolicyExact || gate.RequiredReceipt == nil {
		return false
	}
	required := gate.RequiredReceipt
	slotName := strings.TrimSpace(standby.SlotName)
	if slotName == "" {
		slotName = strings.TrimSpace(standby.Name)
	}
	return slotName != "" && slotName == strings.TrimSpace(required.SlotName) &&
		strings.TrimSpace(artifact.Generation) != "" &&
		strings.TrimSpace(artifact.Generation) == strings.TrimSpace(required.Generation) &&
		artifact.TargetPVC != nil &&
		strings.TrimSpace(artifact.TargetPVC.ClaimName) != "" &&
		strings.TrimSpace(artifact.TargetPVC.ClaimName) == strings.TrimSpace(required.TargetPVCName)
}

func highAvailabilityHasManagedConfig(ha *HighAvailabilitySpec) bool {
	if ha == nil {
		return false
	}
	return len(ha.Standbys) > 0 ||
		ha.Identity != nil ||
		ha.Admin != nil ||
		ha.Runtime != nil ||
		ha.SyncPolicy != nil ||
		ha.Retention != nil ||
		ha.AutomaticFailover != nil
}

func (s *HighAvailabilitySpec) modeOrDefault() HAMode {
	if s == nil || s.Mode == "" {
		return HAModeDisabled
	}
	return s.Mode
}

func (p *HASyncPolicy) modeOrDefault() HADurabilityMode {
	if p == nil || p.Mode == "" {
		return HADurabilityModeAsync
	}
	return p.Mode
}

func (p *HASyncPolicy) selectionOrDefault() HAStandbySelection {
	if p == nil || p.Selection == "" {
		return HAStandbySelectionAny
	}
	return p.Selection
}

func (p *HASyncPolicy) requiredOrDefault() int32 {
	if p == nil || p.Required == 0 {
		return 1
	}
	return p.Required
}

func (p *HAAutomaticFailoverPolicy) fencingAuthorityOrDefault() HAFencingAuthority {
	if p == nil || p.FencingAuthority == "" {
		return HAFencingAuthorityNone
	}
	return p.FencingAuthority
}

func (p *HAAutomaticFailoverPolicy) requireRemoteApplyOrDefault() bool {
	if p == nil || p.RequireRemoteApply == nil {
		return true
	}
	return *p.RequireRemoteApply
}

// validateResourceQuantities validates that resource quantity strings are parseable.
func (r *AntflyCluster) validateResourceQuantities() error {
	var errors []string

	validateQuantity := func(path, value string) {
		if value != "" {
			if _, err := resource.ParseQuantity(value); err != nil {
				errors = append(errors, fmt.Sprintf("%s: %q is not a valid resource quantity", path, value))
			}
		}
	}

	validateQuantity("spec.metadataNodes.resources.limits.gpu", r.Spec.MetadataNodes.Resources.Limits.GPU)
	validateQuantity("spec.dataNodes.resources.limits.gpu", r.Spec.DataNodes.Resources.Limits.GPU)
	if r.Spec.Standalone != nil {
		validateQuantity("spec.standalone.resources.limits.gpu", r.Spec.Standalone.Resources.Limits.GPU)
	}

	if len(errors) > 0 {
		return fmt.Errorf("%s", strings.Join(errors, "; "))
	}
	return nil
}

func (r *AntflyCluster) validateModeConfig() error {
	if err := ValidateOperatorManagedStorageSpec(r.Spec.Mode, r.Spec.Storage); err != nil {
		return err
	}
	if r.Spec.Mode == "" {
		if r.Spec.Standalone != nil {
			return fmt.Errorf("spec.standalone may only be set when spec.mode=Standalone")
		}
		return nil
	}

	switch r.Spec.Mode {
	case ClusterModeDistributed:
		if r.Spec.Standalone != nil {
			return fmt.Errorf("spec.standalone may only be set when spec.mode=Standalone")
		}
	case ClusterModeStandalone:
		if r.Spec.Standalone == nil {
			return fmt.Errorf("spec.standalone is required when spec.mode=Standalone")
		}
		if err := r.validateStandaloneConfig(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("spec.mode must be one of: Distributed, Standalone")
	}

	return nil
}

func (r *AntflyCluster) effectiveMode() ClusterMode {
	if r.Spec.Mode == "" {
		return ClusterModeDistributed
	}
	return r.Spec.Mode
}

func (r *AntflyCluster) validateStandaloneConfig() error {
	standalone := r.Spec.Standalone
	if standalone == nil {
		return nil
	}

	if standalone.Inference != nil && standalone.Inference.Enabled && strings.TrimSpace(standalone.Inference.APIURL) == "" {
		return fmt.Errorf("spec.standalone.inference.apiURL must be set when inference is enabled")
	}

	if standalone.Inference != nil && strings.TrimSpace(standalone.Inference.APIURL) != "" {
		parsed, err := url.Parse(standalone.Inference.APIURL)
		if err != nil {
			return fmt.Errorf("spec.standalone.inference.apiURL is invalid: %w", err)
		}
		if parsed.Scheme == "" || parsed.Host == "" {
			return fmt.Errorf("spec.standalone.inference.apiURL must include a scheme and host")
		}
	}

	ports := map[string]int32{
		"metadataAPI":  standalone.MetadataAPI.Port,
		"metadataRaft": standalone.MetadataRaft.Port,
		"storeAPI":     standalone.StoreAPI.Port,
		"storeRaft":    standalone.StoreRaft.Port,
		"health":       standalone.Health.Port,
	}
	seen := map[int32]string{}
	var portErrors []string
	for name, port := range ports {
		if port <= 0 {
			portErrors = append(portErrors, fmt.Sprintf("spec.standalone.%s.port must be greater than 0", name))
			continue
		}
		if prev, ok := seen[port]; ok {
			portErrors = append(portErrors, fmt.Sprintf("spec.standalone.%s.port conflicts with spec.standalone.%s.port: %d", name, prev, port))
			continue
		}
		seen[port] = name
	}

	if len(portErrors) > 0 {
		return fmt.Errorf("standalone port validation failed:\n  - %s", strings.Join(portErrors, "\n  - "))
	}

	if standalone.Replicas > 1 {
		return fmt.Errorf("spec.standalone.replicas > 1 is not supported in the MVP, got %d", standalone.Replicas)
	}

	if strings.TrimSpace(r.Spec.Storage.StandaloneStorage) == "" {
		return fmt.Errorf("spec.storage.standaloneStorage is required when spec.mode=Standalone")
	}
	if err := r.validateStandaloneTopologyIsolation(); err != nil {
		return err
	}

	return nil
}

// ValidateOperatorManagedStorageSpec validates the typed storage tagged union.
// It is shared by admission and reconciliation so a webhook outage cannot make
// the controller silently reinterpret an unsupported engine as local storage.
func ValidateOperatorManagedStorageSpec(mode ClusterMode, storage StorageSpec) error {
	engine := storage.Engine
	if engine == "" {
		engine = "local"
	}
	if engine != "local" && engine != "lite" {
		return fmt.Errorf("spec.storage.engine must be local or lite, got %q", engine)
	}

	effectiveMode := mode
	if effectiveMode == "" {
		effectiveMode = ClusterModeDistributed
	}
	switch effectiveMode {
	case ClusterModeDistributed:
		if engine != "local" {
			return fmt.Errorf("spec.storage.engine=%q is not supported when spec.mode=Distributed", engine)
		}
		if storage.LiteFileName != "" {
			return fmt.Errorf("spec.storage.liteFileName may only be set when spec.storage.engine=lite")
		}
	case ClusterModeStandalone:
		if engine == "lite" {
			fileName := storage.LiteFileName
			if fileName == "" {
				fileName = "antfly.aflite"
			}
			if strings.ContainsAny(fileName, `/\\`) || fileName == "." || fileName == ".." || !strings.HasSuffix(fileName, ".aflite") {
				return fmt.Errorf("spec.storage.liteFileName must be a basename ending in .aflite, got %q", fileName)
			}
		} else if storage.LiteFileName != "" {
			return fmt.Errorf("spec.storage.liteFileName may only be set when spec.storage.engine=lite")
		}
	}
	return nil
}

// ValidateOperatorManagedStorageConfig keeps the raw runtime JSON
// from competing with the typed, operator-owned storage contract.
func ValidateOperatorManagedStorageConfig(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var config map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &config); err != nil {
		return fmt.Errorf("spec.config must contain valid JSON: %w", err)
	}
	rawStorage, exists := config["storage"]
	if !exists {
		return nil
	}
	var storage map[string]json.RawMessage
	if err := json.Unmarshal(rawStorage, &storage); err != nil {
		return fmt.Errorf("spec.config.storage must be an object: %w", err)
	}
	return fmt.Errorf("spec.config.storage is operator-managed; configure persistence under spec.storage")
}

func (r *AntflyCluster) validateStandaloneTopologyIsolation() error {
	var errors []string

	if r.Spec.MetadataNodes.Replicas != 0 {
		errors = append(errors, "spec.metadataNodes.replicas must be unset when spec.mode=Standalone")
	}

	if r.Spec.DataNodes.Replicas != 0 {
		errors = append(errors, "spec.dataNodes.replicas must be unset when spec.mode=Standalone")
	}

	if r.Spec.DataNodes.Suspend {
		errors = append(errors, "spec.dataNodes.suspend must be unset when spec.mode=Standalone")
	}

	if r.Spec.MetadataNodes.Resources != (ResourceSpec{}) {
		errors = append(errors, "spec.metadataNodes.resources must be unset when spec.mode=Standalone")
	}

	if r.Spec.DataNodes.Resources != (ResourceSpec{}) {
		errors = append(errors, "spec.dataNodes.resources must be unset when spec.mode=Standalone")
	}

	if r.Spec.MetadataNodes.MetadataAPI != (APISpec{}) {
		errors = append(errors, "spec.metadataNodes.metadataAPI must be unset when spec.mode=Standalone")
	}

	if r.Spec.MetadataNodes.MetadataRaft != (APISpec{}) {
		errors = append(errors, "spec.metadataNodes.metadataRaft must be unset when spec.mode=Standalone")
	}

	if r.Spec.DataNodes.API != (APISpec{}) {
		errors = append(errors, "spec.dataNodes.api must be unset when spec.mode=Standalone")
	}

	if r.Spec.DataNodes.Raft != (APISpec{}) {
		errors = append(errors, "spec.dataNodes.raft must be unset when spec.mode=Standalone")
	}

	if r.Spec.MetadataNodes.Health != (APISpec{}) {
		errors = append(errors, "spec.metadataNodes.health must be unset when spec.mode=Standalone")
	}

	if r.Spec.DataNodes.Health != (APISpec{}) {
		errors = append(errors, "spec.dataNodes.health must be unset when spec.mode=Standalone")
	}

	if r.Spec.MetadataNodes.UseSpotPods {
		errors = append(errors, "spec.metadataNodes.useSpotPods must be false when spec.mode=Standalone")
	}

	if r.Spec.DataNodes.UseSpotPods {
		errors = append(errors, "spec.dataNodes.useSpotPods must be false when spec.mode=Standalone")
	}

	if r.Spec.DataNodes.AutoScaling != nil && r.Spec.DataNodes.AutoScaling.Enabled {
		errors = append(errors, "spec.dataNodes.autoScaling.enabled must be false when spec.mode=Standalone")
	}

	if len(r.Spec.MetadataNodes.EnvFrom) > 0 {
		errors = append(errors, "spec.metadataNodes.envFrom must be empty when spec.mode=Standalone")
	}

	if len(r.Spec.DataNodes.EnvFrom) > 0 {
		errors = append(errors, "spec.dataNodes.envFrom must be empty when spec.mode=Standalone")
	}

	if len(r.Spec.MetadataNodes.Tolerations) > 0 {
		errors = append(errors, "spec.metadataNodes.tolerations must be empty when spec.mode=Standalone")
	}

	if len(r.Spec.DataNodes.Tolerations) > 0 {
		errors = append(errors, "spec.dataNodes.tolerations must be empty when spec.mode=Standalone")
	}

	if len(r.Spec.MetadataNodes.NodeSelector) > 0 {
		errors = append(errors, "spec.metadataNodes.nodeSelector must be empty when spec.mode=Standalone")
	}

	if len(r.Spec.DataNodes.NodeSelector) > 0 {
		errors = append(errors, "spec.dataNodes.nodeSelector must be empty when spec.mode=Standalone")
	}

	if r.Spec.MetadataNodes.Affinity != nil {
		errors = append(errors, "spec.metadataNodes.affinity must be unset when spec.mode=Standalone")
	}

	if r.Spec.DataNodes.Affinity != nil {
		errors = append(errors, "spec.dataNodes.affinity must be unset when spec.mode=Standalone")
	}

	if len(r.Spec.MetadataNodes.TopologySpreadConstraints) > 0 {
		errors = append(errors, "spec.metadataNodes.topologySpreadConstraints must be empty when spec.mode=Standalone")
	}

	if len(r.Spec.DataNodes.TopologySpreadConstraints) > 0 {
		errors = append(errors, "spec.dataNodes.topologySpreadConstraints must be empty when spec.mode=Standalone")
	}

	if r.Spec.Storage.MetadataStorage != "" || r.Spec.Storage.DataStorage != "" {
		errors = append(errors, "spec.storage.metadataStorage and spec.storage.dataStorage must be empty when spec.mode=Standalone")
	}

	if len(errors) > 0 {
		return fmt.Errorf("standalone topology validation failed:\n  - %s", strings.Join(errors, "\n  - "))
	}

	return nil
}

func (r *AntflyCluster) isStandaloneMode() bool {
	return r.effectiveMode() == ClusterModeStandalone
}

// hasGPUInResourceSpec checks if GPU resources are present in ResourceSpec.
func hasGPUInResourceSpec(spec ResourceSpec) bool {
	return spec.Limits.GPU != ""
}
