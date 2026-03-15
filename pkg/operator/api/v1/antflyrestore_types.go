package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Restore condition constants
const (
	// TypeRestoreJobReady indicates whether the restore job is ready
	TypeRestoreJobReady = "JobReady"

	// TypeRestoreClusterReady indicates whether the referenced cluster is ready
	TypeRestoreClusterReady = "ClusterReady"

	// ReasonRestoreJobCreated indicates the Job was created successfully
	ReasonRestoreJobCreated = "JobCreated"

	// ReasonRestoreJobRunning indicates the Job is running
	ReasonRestoreJobRunning = "JobRunning"

	// ReasonRestoreJobCompleted indicates the Job completed successfully
	ReasonRestoreJobCompleted = "JobCompleted"

	// ReasonRestoreJobFailed indicates the Job failed
	ReasonRestoreJobFailed = "JobFailed"

	// ReasonRestoreClusterNotFound indicates the referenced cluster was not found
	ReasonRestoreClusterNotFound = "ClusterNotFound"

	// ReasonInvalidSource indicates the restore source is invalid
	ReasonInvalidSource = "InvalidSource"

	// ReasonRestoreValidationFailed indicates the restore spec failed validation
	ReasonRestoreValidationFailed = "ValidationFailed"

	// ReasonRestoreJobCreationFailed indicates the restore job could not be created
	ReasonRestoreJobCreationFailed = "JobCreationFailed"
)

// RestoreMode defines behavior when target tables exist
// +kubebuilder:validation:Enum=fail_if_exists;skip_if_exists;overwrite
type RestoreMode string

const (
	// RestoreModeFailIfExists aborts if any table already exists (default)
	RestoreModeFailIfExists RestoreMode = "fail_if_exists"

	// RestoreModeSkipIfExists skips existing tables, restores others
	RestoreModeSkipIfExists RestoreMode = "skip_if_exists"

	// RestoreModeOverwrite drops and recreates existing tables
	RestoreModeOverwrite RestoreMode = "overwrite"
)

// RestorePhase represents the current state of a restore
type RestorePhase string

const (
	// RestorePhasePending indicates the restore has not started yet
	RestorePhasePending RestorePhase = "Pending"

	// RestorePhaseRunning indicates the restore is in progress
	RestorePhaseRunning RestorePhase = "Running"

	// RestorePhaseCompleted indicates the restore completed successfully
	RestorePhaseCompleted RestorePhase = "Completed"

	// RestorePhaseFailed indicates the restore failed
	RestorePhaseFailed RestorePhase = "Failed"
)

// RestoreSource defines where to restore from
type RestoreSource struct {
	// BackupID identifies which backup to restore
	BackupID string `json:"backupId"`

	// Location is the backup source URL
	// Supports: s3://bucket/path or file:///path
	// +kubebuilder:validation:Pattern=`^(s3://|file://).+`
	Location string `json:"location"`

	// CredentialsSecret references a Secret containing storage credentials
	// For S3: expects keys AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and optionally AWS_REGION
	// +optional
	CredentialsSecret *SecretReference `json:"credentialsSecret,omitempty"`
}

// TableRestoreStatus tracks restore status per table
type TableRestoreStatus struct {
	// Name of the table
	Name string `json:"name"`

	// Status: Pending, Restoring, Completed, Failed, Skipped
	Status string `json:"status"`

	// Error message if failed
	// +optional
	Error string `json:"error,omitempty"`
}

// AntflyRestoreSpec defines the desired state of AntflyRestore
type AntflyRestoreSpec struct {
	// ClusterRef references the target AntflyCluster to restore to
	ClusterRef ClusterReference `json:"clusterRef"`

	// Source defines where to restore from
	Source RestoreSource `json:"source"`

	// Tables to restore (nil = all tables from backup)
	// +optional
	Tables []string `json:"tables,omitempty"`

	// RestoreMode defines behavior when tables exist
	// +optional
	// +kubebuilder:default=fail_if_exists
	RestoreMode RestoreMode `json:"restoreMode,omitempty"`

	// RestoreTimeout is max duration for the restore operation (default: 2h)
	// +optional
	RestoreTimeout *metav1.Duration `json:"restoreTimeout,omitempty"`

	// BackoffLimit specifies the number of retries before marking restore as failed (default: 3)
	// +optional
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=0
	BackoffLimit *int32 `json:"backoffLimit,omitempty"`
}

// AntflyRestoreStatus defines the observed state of AntflyRestore
type AntflyRestoreStatus struct {
	// Phase of the restore operation
	Phase RestorePhase `json:"phase,omitempty"`

	// StartTime when restore began
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime when restore finished
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Tables tracks per-table restore status
	// +optional
	Tables []TableRestoreStatus `json:"tables,omitempty"`

	// Message provides details about current state
	// +optional
	Message string `json:"message,omitempty"`

	// Conditions represent the current state
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// JobName is the name of the Job executing this restore
	// +optional
	JobName string `json:"jobName,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Cluster",type="string",JSONPath=".spec.clusterRef.name"
//+kubebuilder:printcolumn:name="Backup ID",type="string",JSONPath=".spec.source.backupId"
//+kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
//+kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// AntflyRestore is the Schema for on-demand Antfly restores
type AntflyRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec AntflyRestoreSpec `json:"spec"`
	// +optional
	Status AntflyRestoreStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// AntflyRestoreList contains a list of AntflyRestore
type AntflyRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []AntflyRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AntflyRestore{}, &AntflyRestoreList{})
}
