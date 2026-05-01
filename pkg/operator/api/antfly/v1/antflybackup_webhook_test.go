package v1

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestValidateAntflyBackup_Valid(t *testing.T) {
	backup := &AntflyBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "test-backup", Namespace: "default"},
		Spec: AntflyBackupSpec{
			ClusterRef:  ClusterReference{Name: "my-cluster"},
			Schedule:    "0 2 * * *",
			Destination: BackupDestination{Location: "s3://my-bucket/backups"},
		},
	}

	if err := backup.ValidateAntflyBackup(); err != nil {
		t.Errorf("expected no error for valid backup, got: %v", err)
	}
}

func TestValidateAntflyBackup_MissingClusterRef(t *testing.T) {
	backup := &AntflyBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "test-backup", Namespace: "default"},
		Spec: AntflyBackupSpec{
			Schedule:    "0 2 * * *",
			Destination: BackupDestination{Location: "s3://my-bucket/backups"},
		},
	}

	err := backup.ValidateAntflyBackup()
	if err == nil {
		t.Error("expected error for missing clusterRef.name")
	} else if !strings.Contains(err.Error(), "clusterRef.name") {
		t.Errorf("expected error about clusterRef.name, got: %v", err)
	}
}

func TestValidateAntflyBackup_MissingSchedule(t *testing.T) {
	backup := &AntflyBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "test-backup", Namespace: "default"},
		Spec: AntflyBackupSpec{
			ClusterRef:  ClusterReference{Name: "my-cluster"},
			Destination: BackupDestination{Location: "s3://my-bucket/backups"},
		},
	}

	err := backup.ValidateAntflyBackup()
	if err == nil {
		t.Error("expected error for missing schedule")
	} else if !strings.Contains(err.Error(), "schedule") {
		t.Errorf("expected error about schedule, got: %v", err)
	}
}

func TestValidateAntflyBackup_InvalidSchedule(t *testing.T) {
	backup := &AntflyBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "test-backup", Namespace: "default"},
		Spec: AntflyBackupSpec{
			ClusterRef:  ClusterReference{Name: "my-cluster"},
			Schedule:    "not-a-cron",
			Destination: BackupDestination{Location: "s3://my-bucket/backups"},
		},
	}

	err := backup.ValidateAntflyBackup()
	if err == nil {
		t.Error("expected error for invalid cron schedule")
	} else if !strings.Contains(err.Error(), "schedule") {
		t.Errorf("expected error about schedule, got: %v", err)
	}
}

func TestValidateAntflyBackup_InvalidDestination(t *testing.T) {
	backup := &AntflyBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "test-backup", Namespace: "default"},
		Spec: AntflyBackupSpec{
			ClusterRef:  ClusterReference{Name: "my-cluster"},
			Schedule:    "0 2 * * *",
			Destination: BackupDestination{Location: "gs://wrong-scheme/backups"},
		},
	}

	err := backup.ValidateAntflyBackup()
	if err == nil {
		t.Error("expected error for invalid destination location scheme")
	}
}

func TestValidateAntflyBackup_FileDestination(t *testing.T) {
	backup := &AntflyBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "test-backup", Namespace: "default"},
		Spec: AntflyBackupSpec{
			ClusterRef:  ClusterReference{Name: "my-cluster"},
			Schedule:    "0 2 * * *",
			Destination: BackupDestination{Location: "file:///mnt/backups"},
		},
	}

	if err := backup.ValidateAntflyBackup(); err != nil {
		t.Errorf("expected no error for file:// destination, got: %v", err)
	}
}

func TestValidateBackupImmutability_ClusterRefName(t *testing.T) {
	old := &AntflyBackup{
		Spec: AntflyBackupSpec{
			ClusterRef: ClusterReference{Name: "cluster-a"},
		},
	}
	new := &AntflyBackup{
		Spec: AntflyBackupSpec{
			ClusterRef: ClusterReference{Name: "cluster-b"},
		},
	}

	err := new.ValidateBackupImmutability(old)
	if err == nil {
		t.Error("expected error for changing clusterRef.name")
	} else if !strings.Contains(err.Error(), "immutable") {
		t.Errorf("expected 'immutable' in error, got: %v", err)
	}
}

func TestValidateBackupImmutability_ClusterRefNamespace(t *testing.T) {
	old := &AntflyBackup{
		Spec: AntflyBackupSpec{
			ClusterRef: ClusterReference{Name: "cluster-a", Namespace: "ns-a"},
		},
	}
	new := &AntflyBackup{
		Spec: AntflyBackupSpec{
			ClusterRef: ClusterReference{Name: "cluster-a", Namespace: "ns-b"},
		},
	}

	err := new.ValidateBackupImmutability(old)
	if err == nil {
		t.Error("expected error for changing clusterRef.namespace")
	} else if !strings.Contains(err.Error(), "immutable") {
		t.Errorf("expected 'immutable' in error, got: %v", err)
	}
}

func TestValidateBackupImmutability_NoChange(t *testing.T) {
	old := &AntflyBackup{
		Spec: AntflyBackupSpec{
			ClusterRef: ClusterReference{Name: "cluster-a", Namespace: "ns-a"},
		},
	}
	new := &AntflyBackup{
		Spec: AntflyBackupSpec{
			ClusterRef: ClusterReference{Name: "cluster-a", Namespace: "ns-a"},
		},
	}

	if err := new.ValidateBackupImmutability(old); err != nil {
		t.Errorf("expected no error when clusterRef is unchanged, got: %v", err)
	}
}
