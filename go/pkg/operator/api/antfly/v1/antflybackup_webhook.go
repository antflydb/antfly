package v1

import (
	"fmt"
	"strings"

	"github.com/robfig/cron/v3"
	"k8s.io/apimachinery/pkg/runtime"
)

// cronParser is reused across validation calls to avoid re-allocating per request.
var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

// ValidateCreate validates the backup configuration when creating a new backup.
// Called by controller fallback when webhooks are disabled.
func (r *AntflyBackup) ValidateCreate() error {
	return r.ValidateAntflyBackup()
}

// ValidateUpdate validates the backup configuration when updating an existing backup.
// Called by controller fallback when webhooks are disabled (note: controllers cannot
// provide the old object, so this is only called by the deprecated webhook interface).
func (r *AntflyBackup) ValidateUpdate(old runtime.Object) error {
	oldBackup, ok := old.(*AntflyBackup)
	if !ok {
		return fmt.Errorf("expected *AntflyBackup, got %T", old)
	}
	if err := r.ValidateBackupImmutability(oldBackup); err != nil {
		return err
	}
	return r.ValidateAntflyBackup()
}

// ValidateAntflyBackup performs all validation checks
func (r *AntflyBackup) ValidateAntflyBackup() error {
	var allErrors []string

	if err := r.validateClusterRef(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validateSchedule(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if err := r.validateDestination(); err != nil {
		allErrors = append(allErrors, err.Error())
	}

	if len(allErrors) > 0 {
		return fmt.Errorf("AntflyBackup validation failed:\n  - %s",
			strings.Join(allErrors, "\n  - "))
	}

	return nil
}

// validateClusterRef validates the cluster reference
func (r *AntflyBackup) validateClusterRef() error {
	if r.Spec.ClusterRef.Name == "" {
		return fmt.Errorf("spec.clusterRef.name is required")
	}
	return nil
}

// validateSchedule validates the cron schedule format
func (r *AntflyBackup) validateSchedule() error {
	if r.Spec.Schedule == "" {
		return fmt.Errorf("spec.schedule is required")
	}

	_, err := cronParser.Parse(r.Spec.Schedule)
	if err != nil {
		return fmt.Errorf(`spec.schedule '%s' is invalid: %v

Problem: The schedule must be a valid cron expression.

Solution: Use a 5-field cron expression (minute hour day-of-month month day-of-week).

Examples:
  "0 2 * * *"     - Daily at 2:00 AM
  "0 */6 * * *"   - Every 6 hours
  "0 0 * * 0"     - Weekly on Sunday at midnight
  "0 0 1 * *"     - Monthly on the 1st at midnight`, r.Spec.Schedule, err)
	}

	return nil
}

// validateDestination validates the backup destination
func (r *AntflyBackup) validateDestination() error {
	location := r.Spec.Destination.Location

	if location == "" {
		return fmt.Errorf("spec.destination.location is required")
	}

	// Validate location format
	// Note: s3:// can also be used for GCS via S3-compatible API with AWS_ENDPOINT_URL
	if !strings.HasPrefix(location, "s3://") && !strings.HasPrefix(location, "file://") {
		//nolint:staticcheck // ST1005: intentionally capitalized user-facing webhook error
		return fmt.Errorf(`spec.destination.location '%s' is invalid

Problem: The location must start with 's3://' or 'file://'.

Solution: Use a valid backup location URL.

Examples:
  s3://my-bucket/antfly-backups     - Amazon S3 bucket
  s3://my-gcs-bucket/backups        - GCS bucket (via S3-compatible API, requires AWS_ENDPOINT_URL)
  file:///mnt/backups               - Local filesystem (for testing)

For GCS: Use s3:// URLs with HMAC credentials and set AWS_ENDPOINT_URL=https://storage.googleapis.com
in your credentials secret.`, location)
	}

	// S3 destinations should have credentials (warn if missing, don't error)
	// The controller will handle credential validation at runtime

	return nil
}

// ValidateBackupImmutability validates that immutable fields haven't changed
func (r *AntflyBackup) ValidateBackupImmutability(old *AntflyBackup) error {
	// clusterRef is immutable after creation
	if r.Spec.ClusterRef.Name != old.Spec.ClusterRef.Name {
		return fmt.Errorf(`spec.clusterRef.name is immutable after creation

Problem: The target cluster cannot be changed for an existing backup schedule.

Solution: Delete this AntflyBackup and create a new one with the desired cluster reference.

Current: %s
Attempted: %s`, old.Spec.ClusterRef.Name, r.Spec.ClusterRef.Name)
	}

	// namespace change is also immutable
	oldNs := old.Spec.ClusterRef.Namespace
	newNs := r.Spec.ClusterRef.Namespace
	if oldNs != newNs {
		return fmt.Errorf(`spec.clusterRef.namespace is immutable after creation

Problem: The target cluster namespace cannot be changed for an existing backup schedule.

Solution: Delete this AntflyBackup and create a new one with the desired cluster reference.

Current: %s
Attempted: %s`, oldNs, newNs)
	}

	return nil
}
