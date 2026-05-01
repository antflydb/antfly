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

package v1alpha1

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
)

var (
	// irsaARNPattern matches AWS IAM Role ARNs including China and GovCloud partitions.
	irsaARNPattern = regexp.MustCompile(`^arn:aws(-cn|-us-gov)?:iam::\d{12}:role/.+$`)
	// ec2InstancePattern matches AWS EC2 instance type names (e.g. m5.large, u-6tb1.56xlarge).
	ec2InstancePattern = regexp.MustCompile(`^[a-z][a-z0-9-]*\.[a-z0-9]+$`)
)

// ValidateCreate validates the pool configuration when creating a new pool.
// Called by controller fallback when webhooks are disabled.
func (r *TermitePool) ValidateCreate() error {
	return r.ValidateTermitePool()
}

// ValidateUpdate validates the pool configuration when updating an existing pool.
// Called by controller fallback when webhooks are disabled (note: controllers cannot
// provide the old object, so this is only called by the deprecated webhook interface).
func (r *TermitePool) ValidateUpdate(old runtime.Object) error {
	oldPool, ok := old.(*TermitePool)
	if !ok {
		return fmt.Errorf("expected *TermitePool, got %T", old)
	}
	if err := r.ValidateImmutability(oldPool); err != nil {
		return err
	}
	return r.ValidateTermitePool()
}

// ValidateTermitePool performs all validation checks
func (r *TermitePool) ValidateTermitePool() error {
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

	if err := r.validateReplicaCounts(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if len(allErrors) > 0 {
		return fmt.Errorf("TermitePool validation failed:\n  - %s",
			strings.Join(allErrors, "\n  - "))
	}

	return nil
}

// validateGKEConfig validates GKE-specific configuration
func (r *TermitePool) validateGKEConfig() error {
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
  Option 2 (Standard GKE): Remove spec.gke.autopilotComputeClass and use spec.hardware.spot instead`)
	}

	// Validate compute class enum (only if non-empty)
	if gke.AutopilotComputeClass != "" {
		validClasses := []string{"Accelerator", "Balanced", "Performance", "Scale-Out", "autopilot", "autopilot-spot"}
		if !slices.Contains(validClasses, gke.AutopilotComputeClass) {
			return fmt.Errorf("invalid GKE Autopilot compute class '%s'. Must be one of: %s",
				gke.AutopilotComputeClass, strings.Join(validClasses, ", "))
		}
	}

	// Validate Accelerator compute class requires GPU (NOT TPU)
	// TPU workloads should NOT use Accelerator class - they use node selectors instead
	if gke.AutopilotComputeClass == "Accelerator" {
		hasGPU := r.hasGPUResources()

		if !hasGPU {
			return fmt.Errorf(`spec.gke.autopilotComputeClass='Accelerator' requires GPU resources

Problem: GKE Autopilot's Accelerator compute class is for GPU workloads ONLY.
For TPU workloads, do NOT use 'Accelerator' class - TPU provisioning uses node selectors.

Solution for GPU workloads: Add GPU resources to spec.resources
Solution for TPU workloads: Remove autopilotComputeClass='Accelerator' and use TPU node selectors

Example (GPU):
  spec:
    resources:
      limits:
        nvidia.com/gpu: "1"
    gke:
      autopilot: true
      autopilotComputeClass: "Accelerator"

Example (TPU with Spot pricing):
  spec:
    hardware:
      accelerator: "tpu-v4-podslice"
      topology: "2x2x1"
    gke:
      autopilot: true
      autopilotComputeClass: "autopilot-spot"  # Use this for spot, NOT "Accelerator"
    resources:
      limits:
        google.com/tpu: "4"`)
		}
	}

	return nil
}

// validateNoConflictingSettings validates that hardware.spot and nodeSelector don't conflict with Autopilot
func (r *TermitePool) validateNoConflictingSettings() error {
	if r.Spec.GKE == nil || !r.Spec.GKE.Autopilot {
		return nil
	}

	// Check hardware.spot conflicts with Autopilot
	// Exception: TPU workloads CAN use hardware.spot=true even in Autopilot mode
	// because TPU provisioning doesn't use compute class (node selectors drive it)
	isTPUWorkload := strings.Contains(r.Spec.Hardware.Accelerator, "tpu")
	if r.Spec.Hardware.Spot && !isTPUWorkload {
		return fmt.Errorf(`spec.hardware.spot=true conflicts with spec.gke.autopilot=true

Problem: GKE Autopilot uses compute classes for spot scheduling, not node selectors.

Solution: Remove 'hardware.spot: true' and use 'gke.autopilotComputeClass: autopilot-spot' instead

Example:
  spec:
    hardware:
      # spot: true  # REMOVE THIS
      accelerator: "tpu-v5-lite-podslice"
      topology: "2x2"
    gke:
      autopilot: true
      autopilotComputeClass: 'autopilot-spot'  # ADD THIS`)
	}

	// GKE Autopilot overrides node selectors with compute class annotations.
	// User-specified node selectors would be silently dropped.
	if len(r.Spec.NodeSelector) > 0 {
		return fmt.Errorf(`spec.nodeSelector conflicts with spec.gke.autopilot=true

Problem: GKE Autopilot manages node scheduling via compute classes, not node selectors.
Any custom nodeSelector values will be overridden.

Solution: Remove spec.nodeSelector when using GKE Autopilot.
Use spec.gke.autopilotComputeClass to control scheduling instead`)
	}

	return nil
}

// validateEKSConfig validates AWS EKS-specific configuration
func (r *TermitePool) validateEKSConfig() error {
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
      irsaRoleARN: "arn:aws:iam::123456789012:role/termite-model-registry-role"`, eks.IRSARoleARN)
		}
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

// validateNoConflictingCloudProviders validates that GKE and EKS are not both configured
func (r *TermitePool) validateNoConflictingCloudProviders() error {
	gkeConfigured := r.Spec.GKE != nil
	eksEnabled := r.Spec.EKS != nil && r.Spec.EKS.Enabled

	if gkeConfigured && eksEnabled {
		return fmt.Errorf(`both spec.gke and spec.eks.enabled=true are set

Problem: A pool cannot be configured for both GKE and EKS simultaneously.

Solution: Enable only one cloud provider configuration:
  Option 1 (GKE): Remove or set spec.eks.enabled=false
  Option 2 (EKS): Remove spec.gke section`)
	}

	return nil
}

// validateReplicaCounts validates that replica counts are valid
func (r *TermitePool) validateReplicaCounts() error {
	if r.Spec.Replicas.Min < 0 {
		return fmt.Errorf("spec.replicas.min must be >= 0, got %d", r.Spec.Replicas.Min)
	}

	if r.Spec.Replicas.Max <= 0 {
		return fmt.Errorf("spec.replicas.max must be > 0, got %d", r.Spec.Replicas.Max)
	}

	if r.Spec.Replicas.Min > r.Spec.Replicas.Max {
		return fmt.Errorf("spec.replicas.min (%d) cannot be greater than spec.replicas.max (%d)",
			r.Spec.Replicas.Min, r.Spec.Replicas.Max)
	}

	return nil
}

// ValidateImmutability validates that immutable fields haven't changed
func (r *TermitePool) ValidateImmutability(old *TermitePool) error {
	var errors []string

	// Check if both old and new have GKE config
	if r.Spec.GKE != nil && old.Spec.GKE != nil {
		// Check Autopilot mode immutability
		if r.Spec.GKE.Autopilot != old.Spec.GKE.Autopilot {
			errors = append(errors, fmt.Sprintf(
				`field 'spec.gke.autopilot' is immutable after deployment

Problem: Changing Autopilot mode requires pod recreation, which may disrupt model serving.

Solution: Delete and recreate the pool to change this setting.

Current value: %v
Attempted change: %v`,
				old.Spec.GKE.Autopilot, r.Spec.GKE.Autopilot))
		}

		// Check compute class immutability (only when Autopilot is enabled)
		if r.Spec.GKE.Autopilot && r.Spec.GKE.AutopilotComputeClass != old.Spec.GKE.AutopilotComputeClass {
			errors = append(errors, fmt.Sprintf(
				`field 'spec.gke.autopilotComputeClass' is immutable after deployment

Problem: Changing compute class requires pod recreation, which may disrupt model serving.

Solution: Delete and recreate the pool to change this setting.

Current value: "%s"
Attempted change: "%s"`,
				old.Spec.GKE.AutopilotComputeClass, r.Spec.GKE.AutopilotComputeClass))
		}
	}

	// Handle case where GKE config is being added/removed after creation
	if (r.Spec.GKE != nil && old.Spec.GKE == nil) || (r.Spec.GKE == nil && old.Spec.GKE != nil) {
		if old.Spec.GKE != nil && old.Spec.GKE.Autopilot {
			errors = append(errors, `cannot remove spec.gke configuration after deployment when autopilot was enabled

Problem: Removing GKE configuration would change the scheduling behavior.

Solution: Delete and recreate the pool to change this setting.`)
		}
	}

	// Check if both old and new have EKS config
	if r.Spec.EKS != nil && old.Spec.EKS != nil {
		// Check EKS enabled immutability
		if r.Spec.EKS.Enabled != old.Spec.EKS.Enabled {
			errors = append(errors, fmt.Sprintf(
				`field 'spec.eks.enabled' is immutable after deployment

Problem: Changing EKS mode requires pod recreation, which may disrupt model serving.

Solution: Delete and recreate the pool to change this setting.

Current value: %v
Attempted change: %v`,
				old.Spec.EKS.Enabled, r.Spec.EKS.Enabled))
		}
	}

	// Check if EKS section was added after initial creation (old had no EKS section at all)
	if r.Spec.EKS != nil && r.Spec.EKS.Enabled && old.Spec.EKS == nil {
		errors = append(errors, `field 'spec.eks.enabled' cannot be enabled after pool creation

Problem: Enabling EKS mode on an existing pool requires pod recreation, which may disrupt model serving.

Solution: Delete and recreate the pool with EKS configuration.`)
	}

	// Check if GKE section was added after initial creation (old had no GKE section at all)
	if r.Spec.GKE != nil && r.Spec.GKE.Autopilot && old.Spec.GKE == nil {
		errors = append(errors, `field 'spec.gke.autopilot' cannot be enabled after pool creation

Problem: Enabling GKE Autopilot mode on an existing pool requires pod recreation, which may disrupt model serving.

Solution: Delete and recreate the pool with GKE Autopilot configuration.`)
	}

	if len(errors) > 0 {
		return fmt.Errorf("%s", strings.Join(errors, "\n\n"))
	}

	return nil
}

// hasGPUResources checks if GPU resources are present in spec.resources
func (r *TermitePool) hasGPUResources() bool {
	if r.Spec.Resources == nil || r.Spec.Resources.Limits == nil {
		return false
	}
	_, hasNvidiaGPU := r.Spec.Resources.Limits["nvidia.com/gpu"]
	_, hasGoogleGPU := r.Spec.Resources.Limits["cloud.google.com/gke-gpu"]
	return hasNvidiaGPU || hasGoogleGPU
}
