package v1

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestValidateAntflyRestore_Valid(t *testing.T) {
	restore := &AntflyRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "test-restore", Namespace: "default"},
		Spec: AntflyRestoreSpec{
			ClusterRef: ClusterReference{Name: "my-cluster"},
			Source: RestoreSource{
				BackupID: "backup-001",
				Location: "s3://my-bucket/backups",
			},
		},
	}

	if err := restore.ValidateAntflyRestore(); err != nil {
		t.Errorf("expected no error for valid restore, got: %v", err)
	}
}

func TestValidateAntflyRestore_MissingClusterRef(t *testing.T) {
	restore := &AntflyRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "test-restore", Namespace: "default"},
		Spec: AntflyRestoreSpec{
			Source: RestoreSource{
				BackupID: "backup-001",
				Location: "s3://my-bucket/backups",
			},
		},
	}

	err := restore.ValidateAntflyRestore()
	if err == nil {
		t.Error("expected error for missing clusterRef.name")
	} else if !strings.Contains(err.Error(), "clusterRef.name") {
		t.Errorf("expected error about clusterRef.name, got: %v", err)
	}
}

func TestValidateAntflyRestore_MissingBackupID(t *testing.T) {
	restore := &AntflyRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "test-restore", Namespace: "default"},
		Spec: AntflyRestoreSpec{
			ClusterRef: ClusterReference{Name: "my-cluster"},
			Source: RestoreSource{
				Location: "s3://my-bucket/backups",
			},
		},
	}

	err := restore.ValidateAntflyRestore()
	if err == nil {
		t.Error("expected error for missing backupId")
	} else if !strings.Contains(err.Error(), "backupId") {
		t.Errorf("expected error about backupId, got: %v", err)
	}
}

func TestValidateAntflyRestore_MissingLocation(t *testing.T) {
	restore := &AntflyRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "test-restore", Namespace: "default"},
		Spec: AntflyRestoreSpec{
			ClusterRef: ClusterReference{Name: "my-cluster"},
			Source: RestoreSource{
				BackupID: "backup-001",
			},
		},
	}

	err := restore.ValidateAntflyRestore()
	if err == nil {
		t.Error("expected error for missing location")
	} else if !strings.Contains(err.Error(), "location") {
		t.Errorf("expected error about location, got: %v", err)
	}
}

func TestValidateAntflyRestore_InvalidLocation(t *testing.T) {
	restore := &AntflyRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "test-restore", Namespace: "default"},
		Spec: AntflyRestoreSpec{
			ClusterRef: ClusterReference{Name: "my-cluster"},
			Source: RestoreSource{
				BackupID: "backup-001",
				Location: "gs://wrong-scheme",
			},
		},
	}

	err := restore.ValidateAntflyRestore()
	if err == nil {
		t.Error("expected error for invalid location scheme")
	}
}

func TestValidateAntflyRestore_ValidRestoreModes(t *testing.T) {
	modes := []RestoreMode{
		RestoreModeFailIfExists,
		RestoreModeSkipIfExists,
		RestoreModeOverwrite,
	}

	for _, mode := range modes {
		t.Run(string(mode), func(t *testing.T) {
			restore := &AntflyRestore{
				ObjectMeta: metav1.ObjectMeta{Name: "test-restore", Namespace: "default"},
				Spec: AntflyRestoreSpec{
					ClusterRef: ClusterReference{Name: "my-cluster"},
					Source: RestoreSource{
						BackupID: "backup-001",
						Location: "s3://my-bucket/backups",
					},
					RestoreMode: mode,
				},
			}

			if err := restore.ValidateAntflyRestore(); err != nil {
				t.Errorf("expected no error for mode %s, got: %v", mode, err)
			}
		})
	}
}

func TestValidateAntflyRestore_InvalidRestoreMode(t *testing.T) {
	restore := &AntflyRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "test-restore", Namespace: "default"},
		Spec: AntflyRestoreSpec{
			ClusterRef: ClusterReference{Name: "my-cluster"},
			Source: RestoreSource{
				BackupID: "backup-001",
				Location: "s3://my-bucket/backups",
			},
			RestoreMode: "invalid_mode",
		},
	}

	err := restore.ValidateAntflyRestore()
	if err == nil {
		t.Error("expected error for invalid restore mode")
	} else if !strings.Contains(err.Error(), "restoreMode") {
		t.Errorf("expected error about restoreMode, got: %v", err)
	}
}

func TestValidateUpdate_RestoreRejectsModificationAfterStart(t *testing.T) {
	phases := []RestorePhase{
		RestorePhaseRunning,
		RestorePhaseCompleted,
		RestorePhaseFailed,
	}

	for _, phase := range phases {
		t.Run(string(phase), func(t *testing.T) {
			old := &AntflyRestore{
				ObjectMeta: metav1.ObjectMeta{Name: "test-restore", Namespace: "default"},
				Spec: AntflyRestoreSpec{
					ClusterRef: ClusterReference{Name: "my-cluster"},
					Source: RestoreSource{
						BackupID: "backup-001",
						Location: "s3://my-bucket/backups",
					},
				},
				Status: AntflyRestoreStatus{Phase: phase},
			}
			new := old.DeepCopy()

			err := new.ValidateUpdate(old)
			if err == nil {
				t.Errorf("expected error for modifying restore in phase %s", phase)
			} else if !strings.Contains(err.Error(), "cannot be modified") {
				t.Errorf("expected 'cannot be modified' in error, got: %v", err)
			}
		})
	}
}

func TestValidateUpdate_RestoreAllowsModificationWhenPending(t *testing.T) {
	old := &AntflyRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "test-restore", Namespace: "default"},
		Spec: AntflyRestoreSpec{
			ClusterRef: ClusterReference{Name: "my-cluster"},
			Source: RestoreSource{
				BackupID: "backup-001",
				Location: "s3://my-bucket/backups",
			},
		},
		Status: AntflyRestoreStatus{Phase: RestorePhasePending},
	}
	new := old.DeepCopy()

	if err := new.ValidateUpdate(old); err != nil {
		t.Errorf("expected no error for modifying restore in Pending phase, got: %v", err)
	}
}
