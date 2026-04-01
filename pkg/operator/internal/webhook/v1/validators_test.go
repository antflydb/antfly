package v1

import (
	"context"
	"strings"
	"testing"

	antflyv1 "github.com/antflydb/antfly/pkg/operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// --- AntflyRestoreValidator tests ---

func TestAntflyRestoreValidator_ValidateCreate_Valid(t *testing.T) {
	v := &AntflyRestoreValidator{}
	restore := &antflyv1.AntflyRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "test-restore", Namespace: "default"},
		Spec: antflyv1.AntflyRestoreSpec{
			ClusterRef: antflyv1.ClusterReference{Name: "my-cluster"},
			Source: antflyv1.RestoreSource{
				BackupID: "backup-001",
				Location: "s3://my-bucket/backups",
			},
		},
	}

	warnings, err := v.ValidateCreate(context.Background(), restore)
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}
	if warnings != nil {
		t.Errorf("expected no warnings, got: %v", warnings)
	}
}

func TestAntflyRestoreValidator_ValidateCreate_Invalid(t *testing.T) {
	v := &AntflyRestoreValidator{}
	restore := &antflyv1.AntflyRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "test-restore", Namespace: "default"},
		Spec: antflyv1.AntflyRestoreSpec{
			// Missing clusterRef.name
			Source: antflyv1.RestoreSource{
				BackupID: "backup-001",
				Location: "s3://my-bucket/backups",
			},
		},
	}

	_, err := v.ValidateCreate(context.Background(), restore)
	if err == nil {
		t.Error("expected validation error for missing clusterRef.name")
	}
}

func TestAntflyRestoreValidator_ValidateUpdate_RejectsRunning(t *testing.T) {
	v := &AntflyRestoreValidator{}
	spec := antflyv1.AntflyRestoreSpec{
		ClusterRef: antflyv1.ClusterReference{Name: "my-cluster"},
		Source: antflyv1.RestoreSource{
			BackupID: "backup-001",
			Location: "s3://my-bucket/backups",
		},
	}

	phases := []antflyv1.RestorePhase{
		antflyv1.RestorePhaseRunning,
		antflyv1.RestorePhaseCompleted,
		antflyv1.RestorePhaseFailed,
	}

	for _, phase := range phases {
		t.Run(string(phase), func(t *testing.T) {
			oldObj := &antflyv1.AntflyRestore{
				ObjectMeta: metav1.ObjectMeta{Name: "test-restore", Namespace: "default"},
				Spec:       spec,
				Status:     antflyv1.AntflyRestoreStatus{Phase: phase},
			}
			newObj := oldObj.DeepCopy()
			newObj.Spec.Source.BackupID = "backup-002" // Try to change

			_, err := v.ValidateUpdate(context.Background(), oldObj, newObj)
			if err == nil {
				t.Errorf("expected error for phase %s, got nil", phase)
			}
			if !strings.Contains(err.Error(), "cannot be modified") {
				t.Errorf("expected 'cannot be modified' in error, got: %v", err)
			}
			if !strings.Contains(err.Error(), string(phase)) {
				t.Errorf("expected phase '%s' in error, got: %v", phase, err)
			}
		})
	}
}

func TestAntflyRestoreValidator_ValidateUpdate_AllowsPending(t *testing.T) {
	v := &AntflyRestoreValidator{}
	oldObj := &antflyv1.AntflyRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "test-restore", Namespace: "default"},
		Spec: antflyv1.AntflyRestoreSpec{
			ClusterRef: antflyv1.ClusterReference{Name: "my-cluster"},
			Source: antflyv1.RestoreSource{
				BackupID: "backup-001",
				Location: "s3://my-bucket/backups",
			},
		},
		Status: antflyv1.AntflyRestoreStatus{Phase: antflyv1.RestorePhasePending},
	}
	newObj := oldObj.DeepCopy()
	newObj.Spec.Source.BackupID = "backup-002"

	_, err := v.ValidateUpdate(context.Background(), oldObj, newObj)
	if err != nil {
		t.Errorf("expected no error for Pending phase update, got: %v", err)
	}
}

func TestAntflyRestoreValidator_ValidateUpdate_AllowsEmptyPhase(t *testing.T) {
	v := &AntflyRestoreValidator{}
	oldObj := &antflyv1.AntflyRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "test-restore", Namespace: "default"},
		Spec: antflyv1.AntflyRestoreSpec{
			ClusterRef: antflyv1.ClusterReference{Name: "my-cluster"},
			Source: antflyv1.RestoreSource{
				BackupID: "backup-001",
				Location: "s3://my-bucket/backups",
			},
		},
		// Status.Phase is empty (zero value)
	}
	newObj := oldObj.DeepCopy()
	newObj.Spec.Source.BackupID = "backup-002"

	_, err := v.ValidateUpdate(context.Background(), oldObj, newObj)
	if err != nil {
		t.Errorf("expected no error for empty phase update, got: %v", err)
	}
}

func TestAntflyRestoreValidator_ValidateUpdate_PendingRejectsInvalidSpec(t *testing.T) {
	v := &AntflyRestoreValidator{}
	oldObj := &antflyv1.AntflyRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "test-restore", Namespace: "default"},
		Spec: antflyv1.AntflyRestoreSpec{
			ClusterRef: antflyv1.ClusterReference{Name: "my-cluster"},
			Source: antflyv1.RestoreSource{
				BackupID: "backup-001",
				Location: "s3://my-bucket/backups",
			},
		},
		Status: antflyv1.AntflyRestoreStatus{Phase: antflyv1.RestorePhasePending},
	}
	newObj := oldObj.DeepCopy()
	// Remove the clusterRef name to make the spec invalid
	newObj.Spec.ClusterRef.Name = ""

	_, err := v.ValidateUpdate(context.Background(), oldObj, newObj)
	if err == nil {
		t.Error("expected validation error for invalid spec during Pending phase update")
	}
}

// --- AntflyBackupValidator tests ---

func TestAntflyBackupValidator_ValidateCreate_Valid(t *testing.T) {
	v := &AntflyBackupValidator{}
	backup := &antflyv1.AntflyBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "test-backup", Namespace: "default"},
		Spec: antflyv1.AntflyBackupSpec{
			ClusterRef:  antflyv1.ClusterReference{Name: "my-cluster"},
			Schedule:    "0 2 * * *",
			Destination: antflyv1.BackupDestination{Location: "s3://my-bucket/backups"},
		},
	}

	warnings, err := v.ValidateCreate(context.Background(), backup)
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}
	if warnings != nil {
		t.Errorf("expected no warnings, got: %v", warnings)
	}
}

func TestAntflyBackupValidator_ValidateCreate_Invalid(t *testing.T) {
	v := &AntflyBackupValidator{}
	backup := &antflyv1.AntflyBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "test-backup", Namespace: "default"},
		Spec: antflyv1.AntflyBackupSpec{
			// Missing clusterRef.name
			Schedule:    "0 2 * * *",
			Destination: antflyv1.BackupDestination{Location: "s3://my-bucket/backups"},
		},
	}

	_, err := v.ValidateCreate(context.Background(), backup)
	if err == nil {
		t.Error("expected validation error for missing clusterRef.name")
	}
}

func TestAntflyBackupValidator_ValidateUpdate_ImmutableClusterRef(t *testing.T) {
	v := &AntflyBackupValidator{}
	oldObj := &antflyv1.AntflyBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "test-backup", Namespace: "default"},
		Spec: antflyv1.AntflyBackupSpec{
			ClusterRef:  antflyv1.ClusterReference{Name: "cluster-a"},
			Schedule:    "0 2 * * *",
			Destination: antflyv1.BackupDestination{Location: "s3://my-bucket/backups"},
		},
	}
	newObj := oldObj.DeepCopy()
	newObj.Spec.ClusterRef.Name = "cluster-b"

	_, err := v.ValidateUpdate(context.Background(), oldObj, newObj)
	if err == nil {
		t.Error("expected error for changing clusterRef.name")
	}
	if !strings.Contains(err.Error(), "immutable") {
		t.Errorf("expected 'immutable' in error, got: %v", err)
	}
}

func TestAntflyBackupValidator_ValidateUpdate_AllowsScheduleChange(t *testing.T) {
	v := &AntflyBackupValidator{}
	oldObj := &antflyv1.AntflyBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "test-backup", Namespace: "default"},
		Spec: antflyv1.AntflyBackupSpec{
			ClusterRef:  antflyv1.ClusterReference{Name: "my-cluster"},
			Schedule:    "0 2 * * *",
			Destination: antflyv1.BackupDestination{Location: "s3://my-bucket/backups"},
		},
	}
	newObj := oldObj.DeepCopy()
	newObj.Spec.Schedule = "0 */6 * * *" // Change schedule (mutable)

	_, err := v.ValidateUpdate(context.Background(), oldObj, newObj)
	if err != nil {
		t.Errorf("expected no error for schedule change, got: %v", err)
	}
}

func TestAntflyBackupValidator_ValidateDelete(t *testing.T) {
	v := &AntflyBackupValidator{}
	backup := &antflyv1.AntflyBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "test-backup", Namespace: "default"},
	}

	warnings, err := v.ValidateDelete(context.Background(), backup)
	if err != nil {
		t.Errorf("expected no error on delete, got: %v", err)
	}
	if warnings != nil {
		t.Errorf("expected no warnings on delete, got: %v", warnings)
	}
}

// --- AntflyClusterValidator tests ---

func TestAntflyClusterValidator_ValidateCreate_Valid(t *testing.T) {
	v := &AntflyClusterValidator{}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{
			MetadataNodes: antflyv1.MetadataNodesSpec{Replicas: 3},
		},
	}

	warnings, err := v.ValidateCreate(context.Background(), cluster)
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}
	if warnings != nil {
		t.Errorf("expected no warnings, got: %v", warnings)
	}
}

func TestAntflyClusterValidator_ValidateCreate_Invalid(t *testing.T) {
	v := &AntflyClusterValidator{}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{
			MetadataNodes: antflyv1.MetadataNodesSpec{Replicas: 3},
			GKE: &antflyv1.GKESpec{
				Autopilot:             false,
				AutopilotComputeClass: "Balanced",
			},
		},
	}

	_, err := v.ValidateCreate(context.Background(), cluster)
	if err == nil {
		t.Error("expected validation error for compute class without autopilot")
	}
	if !strings.Contains(err.Error(), "autopilotComputeClass") {
		t.Errorf("expected error about autopilotComputeClass, got: %v", err)
	}
}

func TestAntflyClusterValidator_ValidateUpdate_ImmutableAutopilot(t *testing.T) {
	v := &AntflyClusterValidator{}
	oldCluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{
			MetadataNodes: antflyv1.MetadataNodesSpec{Replicas: 3},
			GKE:           &antflyv1.GKESpec{Autopilot: true, AutopilotComputeClass: "Balanced"},
		},
	}
	newCluster := oldCluster.DeepCopy()
	newCluster.Spec.GKE.Autopilot = false

	_, err := v.ValidateUpdate(context.Background(), oldCluster, newCluster)
	if err == nil {
		t.Error("expected error for changing autopilot mode")
	}
	if !strings.Contains(err.Error(), "immutable") {
		t.Errorf("expected 'immutable' in error, got: %v", err)
	}
}

func TestAntflyClusterValidator_ValidateUpdate_MutableFields(t *testing.T) {
	v := &AntflyClusterValidator{}
	oldCluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{
			MetadataNodes: antflyv1.MetadataNodesSpec{Replicas: 3},
			DataNodes:     antflyv1.DataNodesSpec{Replicas: 3},
		},
	}
	newCluster := oldCluster.DeepCopy()
	newCluster.Spec.DataNodes.Replicas = 5

	_, err := v.ValidateUpdate(context.Background(), oldCluster, newCluster)
	if err != nil {
		t.Errorf("expected no error for mutable field change, got: %v", err)
	}
}

func TestAntflyClusterValidator_ValidateDelete(t *testing.T) {
	v := &AntflyClusterValidator{}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
	}

	warnings, err := v.ValidateDelete(context.Background(), cluster)
	if err != nil {
		t.Errorf("expected no error on delete, got: %v", err)
	}
	if warnings != nil {
		t.Errorf("expected no warnings on delete, got: %v", warnings)
	}
}
