package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Backup condition constants
const (
	// TypeBackupScheduleReady indicates whether the backup schedule is ready
	TypeBackupScheduleReady = "ScheduleReady"

	// TypeBackupClusterReady indicates whether the referenced cluster is ready
	TypeBackupClusterReady = "ClusterReady"

	// ReasonCronJobCreated indicates the CronJob was created successfully
	ReasonCronJobCreated = "CronJobCreated"

	// ReasonClusterNotFound indicates the referenced cluster was not found
	ReasonClusterNotFound = "ClusterNotFound"

	// ReasonInvalidSchedule indicates the cron schedule is invalid
	ReasonInvalidSchedule = "InvalidSchedule"

	// ReasonInvalidDestination indicates the backup destination is invalid
	ReasonInvalidDestination = "InvalidDestination"

	// ReasonBackupValidationFailed indicates the backup spec failed validation
	ReasonBackupValidationFailed = "ValidationFailed"

	// ReasonCronJobFailed indicates the CronJob failed to create or update
	ReasonCronJobFailed = "CronJobFailed"
)

// BackupPhase represents the current state of a backup schedule
type BackupPhase string

const (
	// BackupPhasePending indicates the backup is waiting for dependencies
	// (e.g. the referenced cluster) to become available.
	BackupPhasePending BackupPhase = "Pending"

	// BackupPhaseActive indicates the backup schedule is active
	BackupPhaseActive BackupPhase = "Active"

	// BackupPhaseSuspended indicates the backup schedule is suspended
	BackupPhaseSuspended BackupPhase = "Suspended"

	// BackupPhaseFailed indicates the backup schedule has failed
	BackupPhaseFailed BackupPhase = "Failed"
)

// ClusterReference identifies an AntflyCluster
type ClusterReference struct {
	// Name of the AntflyCluster
	Name string `json:"name"`

	// Namespace of the AntflyCluster (defaults to same namespace as this resource)
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// SecretReference identifies a Secret in the same namespace
type SecretReference struct {
	// Name of the Secret
	Name string `json:"name"`
}

// BackupDestination defines where backups are stored
type BackupDestination struct {
	// Location is the backup destination URL
	// Supports: s3://bucket/path or file:///path
	// +kubebuilder:validation:Pattern=`^(s3://|file://).+`
	Location string `json:"location"`

	// CredentialsSecret references a Secret containing storage credentials
	// For S3: expects keys AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and optionally AWS_REGION
	// +optional
	CredentialsSecret *SecretReference `json:"credentialsSecret,omitempty"`
}

// BackupRecord stores information about a single backup execution
type BackupRecord struct {
	// BackupID is the unique identifier for this backup
	BackupID string `json:"backupId"`

	// StartTime when backup started
	StartTime metav1.Time `json:"startTime"`

	// CompletionTime when backup completed (nil if still running)
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Status of this backup: Running, Completed, Failed
	Status string `json:"status"`

	// Tables that were backed up
	// +optional
	Tables []string `json:"tables,omitempty"`

	// Error message if failed
	// +optional
	Error string `json:"error,omitempty"`
}

// AntflyBackupSpec defines the desired state of AntflyBackup
type AntflyBackupSpec struct {
	// ClusterRef references the AntflyCluster to back up
	ClusterRef ClusterReference `json:"clusterRef"`

	// Schedule in Cron format (e.g., "0 2 * * *" for daily at 2am)
	Schedule string `json:"schedule"`

	// Destination defines where backups are stored
	Destination BackupDestination `json:"destination"`

	// Tables to back up (nil = all tables)
	// +optional
	Tables []string `json:"tables,omitempty"`

	// Suspend stops scheduled backups when true
	// +optional
	// +kubebuilder:default=false
	Suspend bool `json:"suspend,omitempty"`

	// BackupTimeout is max duration for a backup operation (default: 1h)
	// +optional
	BackupTimeout *metav1.Duration `json:"backupTimeout,omitempty"`

	// SuccessfulJobsHistoryLimit is the number of successful finished jobs to retain (default: 3)
	// +optional
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=0
	SuccessfulJobsHistoryLimit *int32 `json:"successfulJobsHistoryLimit,omitempty"`

	// FailedJobsHistoryLimit is the number of failed finished jobs to retain (default: 1)
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	FailedJobsHistoryLimit *int32 `json:"failedJobsHistoryLimit,omitempty"`
}

// AntflyBackupStatus defines the observed state of AntflyBackup
type AntflyBackupStatus struct {
	// Phase of the backup schedule
	Phase BackupPhase `json:"phase,omitempty"`

	// ObservedGeneration is the most recent generation observed by the controller.
	// Used to detect spec changes when the backup is in a Failed state,
	// allowing the controller to re-validate after user corrections.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// LastScheduledTime is when the last backup was scheduled
	// +optional
	LastScheduledTime *metav1.Time `json:"lastScheduledTime,omitempty"`

	// LastSuccessfulBackup records the most recent successful backup
	// +optional
	LastSuccessfulBackup *BackupRecord `json:"lastSuccessfulBackup,omitempty"`

	// RecentBackups stores recent backup records
	// +optional
	RecentBackups []BackupRecord `json:"recentBackups,omitempty"`

	// Conditions represent the current state
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// NextScheduledBackup is the next planned backup time
	// +optional
	NextScheduledBackup *metav1.Time `json:"nextScheduledBackup,omitempty"`

	// CronJobName is the name of the CronJob managing this backup
	// +optional
	CronJobName string `json:"cronJobName,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Cluster",type="string",JSONPath=".spec.clusterRef.name"
//+kubebuilder:printcolumn:name="Schedule",type="string",JSONPath=".spec.schedule"
//+kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
//+kubebuilder:printcolumn:name="Last Success",type="date",JSONPath=".status.lastSuccessfulBackup.completionTime"
//+kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// AntflyBackup is the Schema for scheduled Antfly backups
type AntflyBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec AntflyBackupSpec `json:"spec"`
	// +optional
	Status AntflyBackupStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// AntflyBackupList contains a list of AntflyBackup
type AntflyBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []AntflyBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AntflyBackup{}, &AntflyBackupList{})
}
