package v1

import (
	"fmt"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
)

// ValidateCreate validates the restore configuration when creating a new restore.
// Called by controller fallback when webhooks are disabled.
func (r *AntflyRestore) ValidateCreate() error {
	return r.ValidateAntflyRestore()
}

// ValidateUpdate validates the restore configuration when updating an existing restore.
// Called by controller fallback when webhooks are disabled (note: controllers cannot
// provide the old object, so this is only called by the deprecated webhook interface).
func (r *AntflyRestore) ValidateUpdate(old runtime.Object) error {
	oldRestore, ok := old.(*AntflyRestore)
	if !ok {
		return fmt.Errorf("expected *AntflyRestore, got %T", old)
	}
	return r.ValidateRestoreUpdate(oldRestore)
}

// ValidateRestoreUpdate checks phase-lock immutability and validates the new spec.
// Shared by both the typed webhook validator and the deprecated fallback.
func (r *AntflyRestore) ValidateRestoreUpdate(old *AntflyRestore) error {
	if old.Status.Phase == RestorePhaseRunning ||
		old.Status.Phase == RestorePhaseCompleted ||
		old.Status.Phase == RestorePhaseFailed {
		//nolint:staticcheck // ST1005: intentionally capitalized user-facing webhook error
		return fmt.Errorf(`AntflyRestore cannot be modified after it has started

Problem: The restore operation is already in phase '%s'.

Solution: Delete this AntflyRestore and create a new one if you need different settings.`, old.Status.Phase)
	}
	return r.ValidateAntflyRestore()
}

// ValidateAntflyRestore performs all validation checks
func (r *AntflyRestore) ValidateAntflyRestore() error {
	var allErrors []string

	if err := r.validateClusterRef(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validateSource(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validateRestoreMode(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if len(allErrors) > 0 {
		return fmt.Errorf("AntflyRestore validation failed:\n  - %s",
			strings.Join(allErrors, "\n  - "))
	}

	return nil
}

// validateClusterRef validates the cluster reference
func (r *AntflyRestore) validateClusterRef() error {
	if r.Spec.ClusterRef.Name == "" {
		return fmt.Errorf("spec.clusterRef.name is required")
	}
	return nil
}

// validateSource validates the restore source
func (r *AntflyRestore) validateSource() error {
	source := r.Spec.Source

	if source.BackupID == "" {
		return fmt.Errorf("spec.source.backupId is required")
	}

	if source.Location == "" {
		return fmt.Errorf("spec.source.location is required")
	}

	// Validate location format
	// Note: s3:// can also be used for GCS via S3-compatible API with AWS_ENDPOINT_URL
	if !strings.HasPrefix(source.Location, "s3://") && !strings.HasPrefix(source.Location, "file://") {
		//nolint:staticcheck // ST1005: intentionally capitalized user-facing webhook error
		return fmt.Errorf(`spec.source.location '%s' is invalid

Problem: The location must start with 's3://' or 'file://'.

Solution: Use a valid backup location URL.

Examples:
  s3://my-bucket/antfly-backups     - Amazon S3 bucket
  s3://my-gcs-bucket/backups        - GCS bucket (via S3-compatible API, requires AWS_ENDPOINT_URL)
  file:///mnt/backups               - Local filesystem (for testing)

For GCS: Use s3:// URLs with HMAC credentials and set AWS_ENDPOINT_URL=https://storage.googleapis.com
in your credentials secret.`, source.Location)
	}

	return nil
}

// validateRestoreMode validates the restore mode
func (r *AntflyRestore) validateRestoreMode() error {
	mode := r.Spec.RestoreMode

	// Empty is allowed (defaults to fail_if_exists)
	if mode == "" {
		return nil
	}

	validModes := []RestoreMode{
		RestoreModeFailIfExists,
		RestoreModeSkipIfExists,
		RestoreModeOverwrite,
	}

	if slices.Contains(validModes, mode) {
		return nil
	}

	return fmt.Errorf(`spec.restoreMode '%s' is invalid

Problem: The restore mode must be one of: fail_if_exists, skip_if_exists, overwrite.

Solution: Use a valid restore mode.

Options:
  fail_if_exists (default) - Abort if any table already exists
  skip_if_exists           - Skip existing tables, restore others
  overwrite                - Drop and recreate existing tables`, mode)
}
