package v1

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	// irsaARNPattern matches AWS IAM Role ARNs including China and GovCloud partitions.
	irsaARNPattern = regexp.MustCompile(`^arn:aws(-cn|-us-gov)?:iam::\d{12}:role/.+$`)
	// ec2InstancePattern matches AWS EC2 instance type names (e.g. m5.large, u-6tb1.56xlarge).
	ec2InstancePattern = regexp.MustCompile(`^[a-z][a-z0-9-]*\.[a-z0-9]+$`)
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

// ValidateAntflyCluster performs all validation checks
func (r *AntflyCluster) ValidateAntflyCluster() error {
	var allErrors []string

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

	if err := r.validateResourceQuantities(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if len(allErrors) > 0 {
		return fmt.Errorf("AntflyCluster validation failed:\n  - %s",
			strings.Join(allErrors, "\n  - "))
	}

	return nil
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

		// Check if metadata nodes have GPU
		if hasGPUInResourceSpec(r.Spec.MetadataNodes.Resources) {
			hasGPU = true
		}

		// Check if data nodes have GPU
		if hasGPUInResourceSpec(r.Spec.DataNodes.Resources) {
			hasGPU = true
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

	return nil
}

// ValidateImmutability validates that immutable fields haven't changed
func (r *AntflyCluster) ValidateImmutability(old *AntflyCluster) error {
	var errors []string

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

	if len(errors) > 0 {
		return fmt.Errorf("%s", strings.Join(errors, "\n\n"))
	}

	return nil
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

	return nil
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

	if len(errors) > 0 {
		return fmt.Errorf("%s", strings.Join(errors, "; "))
	}
	return nil
}

// hasGPUInResourceSpec checks if GPU resources are present in ResourceSpec.
func hasGPUInResourceSpec(spec ResourceSpec) bool {
	return spec.Limits.GPU != ""
}
