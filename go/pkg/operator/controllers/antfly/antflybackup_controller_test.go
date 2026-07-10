package controllers

import (
	"context"
	"strings"
	"testing"
	"time"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestShellQuote(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "simple string",
			input: "s3://my-bucket/backups",
			want:  "'s3://my-bucket/backups'",
		},
		{
			name:  "string with spaces",
			input: "path with spaces",
			want:  "'path with spaces'",
		},
		{
			name:  "string with single quotes",
			input: "it's a test",
			want:  "'it'\\''s a test'",
		},
		{
			name:  "command injection attempt with $(...)",
			input: "$(rm -rf /)",
			want:  "'$(rm -rf /)'",
		},
		{
			name:  "command injection attempt with backticks",
			input: "`rm -rf /`",
			want:  "'`rm -rf /`'",
		},
		{
			name:  "string with double quotes",
			input: `he said "hello"`,
			want:  `'he said "hello"'`,
		},
		{
			name:  "string with semicolon",
			input: "value; rm -rf /",
			want:  "'value; rm -rf /'",
		},
		{
			name:  "string with pipe",
			input: "value | cat /etc/passwd",
			want:  "'value | cat /etc/passwd'",
		},
		{
			name:  "empty string",
			input: "",
			want:  "''",
		},
		{
			name:  "multiple single quotes",
			input: "it's Bob's test",
			want:  "'it'\\''s Bob'\\''s test'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shellQuote(tt.input)
			if got != tt.want {
				t.Errorf("shellQuote(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestUpdateBackupHistoryReportsLatestFailedJob(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add batch scheme: %v", err)
	}

	oldSuccessTime := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	newFailureTime := metav1.NewTime(time.Now().Add(-1 * time.Hour))
	success := backupJob("backup-success", "default", "my-backup", batchv1.JobComplete, oldSuccessTime, "Complete", "")
	failed := backupJob("backup-failed", "default", "my-backup", batchv1.JobFailed, newFailureTime, "BackoffLimitExceeded", "Job has reached the specified backoff limit")

	backup := &antflyv1.AntflyBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "my-backup", Namespace: "default"},
		Spec: antflyv1.AntflyBackupSpec{
			Tables: []string{"docs"},
		},
		Status: antflyv1.AntflyBackupStatus{Phase: antflyv1.BackupPhaseActive},
	}
	r := &AntflyBackupReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(success, failed).Build(),
	}

	if err := r.updateBackupHistory(context.Background(), backup); err != nil {
		t.Fatalf("updateBackupHistory: %v", err)
	}

	if backup.Status.Phase != antflyv1.BackupPhaseActive {
		t.Fatalf("phase = %q, want Active", backup.Status.Phase)
	}
	if backup.Status.LastFailedBackup == nil || backup.Status.LastFailedBackup.BackupID != "backup-failed" {
		t.Fatalf("last failed backup = %#v", backup.Status.LastFailedBackup)
	}
	if backup.Status.LastSuccessfulBackup == nil || backup.Status.LastSuccessfulBackup.BackupID != "backup-success" {
		t.Fatalf("last successful backup = %#v", backup.Status.LastSuccessfulBackup)
	}
	if backup.Status.LastFailedBackup.Error != "Job has reached the specified backoff limit" {
		t.Fatalf("failed backup error = %q", backup.Status.LastFailedBackup.Error)
	}
	if len(backup.Status.Conditions) != 1 ||
		backup.Status.Conditions[0].Type != antflyv1.TypeBackupRunHealthy ||
		backup.Status.Conditions[0].Status != metav1.ConditionFalse ||
		backup.Status.Conditions[0].Reason != antflyv1.ReasonBackupJobFailed {
		t.Fatalf("conditions = %#v", backup.Status.Conditions)
	}
}

func TestUpdateBackupHistoryKeepsActiveWhenSuccessIsLatest(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add batch scheme: %v", err)
	}

	oldFailureTime := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	newSuccessTime := metav1.NewTime(time.Now().Add(-1 * time.Hour))
	failed := backupJob("backup-failed", "default", "my-backup", batchv1.JobFailed, oldFailureTime, "BackoffLimitExceeded", "failed")
	success := backupJob("backup-success", "default", "my-backup", batchv1.JobComplete, newSuccessTime, "Complete", "")

	backup := &antflyv1.AntflyBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "my-backup", Namespace: "default"},
		Status:     antflyv1.AntflyBackupStatus{Phase: antflyv1.BackupPhaseActive},
	}
	r := &AntflyBackupReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(failed, success).Build(),
	}

	if err := r.updateBackupHistory(context.Background(), backup); err != nil {
		t.Fatalf("updateBackupHistory: %v", err)
	}

	if backup.Status.Phase != antflyv1.BackupPhaseActive {
		t.Fatalf("phase = %q, want Active", backup.Status.Phase)
	}
	if backup.Status.LastSuccessfulBackup == nil || backup.Status.LastSuccessfulBackup.BackupID != "backup-success" {
		t.Fatalf("last successful backup = %#v", backup.Status.LastSuccessfulBackup)
	}
	if len(backup.Status.Conditions) != 1 ||
		backup.Status.Conditions[0].Type != antflyv1.TypeBackupRunHealthy ||
		backup.Status.Conditions[0].Status != metav1.ConditionTrue ||
		backup.Status.Conditions[0].Reason != antflyv1.ReasonBackupJobSucceeded {
		t.Fatalf("conditions = %#v", backup.Status.Conditions)
	}
}

func TestRequestsForBackupJobMapsLabeledJobs(t *testing.T) {
	r := &AntflyBackupReconciler{}
	job := backupJob("backup-job", "default", "my-backup", batchv1.JobComplete, metav1.Now(), "Complete", "")

	requests := r.requestsForBackupJob(context.Background(), job)
	if len(requests) != 1 {
		t.Fatalf("requests = %#v, want one", requests)
	}
	want := types.NamespacedName{Name: "my-backup", Namespace: "default"}
	if requests[0].NamespacedName != want {
		t.Fatalf("request = %#v, want %#v", requests[0].NamespacedName, want)
	}

	delete(job.Labels, "antfly.io/backup")
	if requests := r.requestsForBackupJob(context.Background(), job); len(requests) != 0 {
		t.Fatalf("unlabeled job requests = %#v, want none", requests)
	}
}

func TestBackupJobTerminalPredicateOnlyEnqueuesTerminalBackupJobs(t *testing.T) {
	pred := backupJobTerminalPredicate()
	started := backupJob("backup-job", "default", "my-backup", batchv1.JobComplete, metav1.Now(), "Complete", "")
	started.Status.Conditions = nil
	failed := backupJob("backup-job", "default", "my-backup", batchv1.JobFailed, metav1.Now(), "BackoffLimitExceeded", "failed")

	if pred.Create(event.CreateEvent{Object: started}) {
		t.Fatal("non-terminal backup job create enqueued")
	}
	if !pred.Update(event.UpdateEvent{ObjectOld: started, ObjectNew: failed}) {
		t.Fatal("terminal backup job update did not enqueue")
	}

	unlabeled := failed.DeepCopy()
	delete(unlabeled.Labels, "antfly.io/backup")
	if pred.Create(event.CreateEvent{Object: unlabeled}) {
		t.Fatal("unlabeled terminal job create enqueued")
	}
}

func TestHasTerminalBackupFailureConditionIgnoresRunFailures(t *testing.T) {
	backup := &antflyv1.AntflyBackup{
		Status: antflyv1.AntflyBackupStatus{
			Conditions: []metav1.Condition{{
				Type:   antflyv1.TypeBackupRunHealthy,
				Status: metav1.ConditionFalse,
				Reason: antflyv1.ReasonBackupJobFailed,
			}},
		},
	}
	if hasTerminalBackupFailureCondition(backup) {
		t.Fatal("backup run failure treated as terminal schedule failure")
	}

	backup.Status.Conditions = []metav1.Condition{{
		Type:   antflyv1.TypeBackupScheduleReady,
		Status: metav1.ConditionFalse,
		Reason: antflyv1.ReasonCronJobFailed,
	}}
	if !hasTerminalBackupFailureCondition(backup) {
		t.Fatal("cronjob reconciliation failure was not terminal")
	}
}

func backupJob(name, namespace, backupName string, conditionType batchv1.JobConditionType, when metav1.Time, reason, message string) *batchv1.Job {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         namespace,
			CreationTimestamp: when,
			Labels: map[string]string{
				"antfly.io/backup": backupName,
			},
		},
		Status: batchv1.JobStatus{
			StartTime: &when,
			Conditions: []batchv1.JobCondition{
				{
					Type:               conditionType,
					Status:             corev1.ConditionTrue,
					LastTransitionTime: when,
					Reason:             reason,
					Message:            message,
				},
			},
		},
	}
	if conditionType == batchv1.JobComplete {
		job.Status.CompletionTime = &when
	}
	return job
}

func TestBuildCronJobSpec_CommandStructure(t *testing.T) {
	r := &AntflyBackupReconciler{}
	backup := &antflyv1.AntflyBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "my-backup", Namespace: "default"},
		Spec: antflyv1.AntflyBackupSpec{
			ClusterRef:  antflyv1.ClusterReference{Name: "my-cluster"},
			Schedule:    "0 2 * * *",
			Destination: antflyv1.BackupDestination{Location: "s3://my-bucket/backups"},
		},
	}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "my-cluster", Namespace: "default"},
		Spec:       antflyv1.AntflyClusterSpec{Image: "antfly:latest"},
	}

	spec := r.buildCronJobSpec(backup, cluster)
	container := spec.JobTemplate.Spec.Template.Spec.Containers[0]
	cmd := container.Args[0]

	// Command should start with the antfly backup command
	if !strings.HasPrefix(cmd, "/antfly backup") {
		t.Errorf("command should start with '/antfly backup', got: %s", cmd)
	}

	// Backup name should be shell-quoted in the backup-id
	if !strings.Contains(cmd, "'my-backup'-$(date +%Y%m%d%H%M%S)") {
		t.Errorf("backup name not properly quoted in backup-id: %s", cmd)
	}

	if strings.Contains(cmd, "--url") {
		t.Errorf("command should not use stale --url flag: %s", cmd)
	}

	if got := envValue(container.Env, "ANTFLY_URL"); got != "http://my-cluster-public-api.default.svc.cluster.local" {
		t.Errorf("ANTFLY_URL = %q, want cluster public API URL", got)
	}

	// Location should be shell-quoted
	if !strings.Contains(cmd, "--location 's3://my-bucket/backups'") {
		t.Errorf("location not properly quoted: %s", cmd)
	}

	// $(date ...) should be present for shell expansion
	if !strings.Contains(cmd, "$(date +%Y%m%d%H%M%S)") {
		t.Errorf("date substitution missing: %s", cmd)
	}
}

func TestBuildCronJobSpec_InjectionPrevention(t *testing.T) {
	r := &AntflyBackupReconciler{}

	tests := []struct {
		name       string
		backupName string
		location   string
		check      func(t *testing.T, cmd string)
	}{
		{
			name:       "malicious backup name with command substitution",
			backupName: "$(rm -rf /)",
			location:   "s3://bucket/path",
			check: func(t *testing.T, cmd string) {
				// The malicious name should be safely quoted
				if !strings.Contains(cmd, "'$(rm -rf /)'") {
					t.Errorf("malicious backup name not quoted: %s", cmd)
				}
			},
		},
		{
			name:       "malicious location with semicolons",
			backupName: "test",
			location:   "s3://bucket'; rm -rf / ; echo '",
			check: func(t *testing.T, cmd string) {
				// The malicious location should be safely quoted
				if !strings.Contains(cmd, shellQuote("s3://bucket'; rm -rf / ; echo '")) {
					t.Errorf("malicious location not quoted: %s", cmd)
				}
			},
		},
		{
			name:       "backup name with single quotes",
			backupName: "it's-a-backup",
			location:   "s3://bucket/path",
			check: func(t *testing.T, cmd string) {
				if !strings.Contains(cmd, "'it'\\''s-a-backup'") {
					t.Errorf("single quotes not escaped in backup name: %s", cmd)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backup := &antflyv1.AntflyBackup{
				ObjectMeta: metav1.ObjectMeta{Name: tt.backupName, Namespace: "default"},
				Spec: antflyv1.AntflyBackupSpec{
					ClusterRef:  antflyv1.ClusterReference{Name: "cluster"},
					Schedule:    "0 2 * * *",
					Destination: antflyv1.BackupDestination{Location: tt.location},
				},
			}
			cluster := &antflyv1.AntflyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"},
				Spec:       antflyv1.AntflyClusterSpec{Image: "antfly:latest"},
			}

			spec := r.buildCronJobSpec(backup, cluster)
			cmd := spec.JobTemplate.Spec.Template.Spec.Containers[0].Args[0]
			tt.check(t, cmd)
		})
	}
}

func TestBuildCronJobSpec_WithTables(t *testing.T) {
	r := &AntflyBackupReconciler{}
	backup := &antflyv1.AntflyBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "bk", Namespace: "default"},
		Spec: antflyv1.AntflyBackupSpec{
			ClusterRef:  antflyv1.ClusterReference{Name: "cluster"},
			Schedule:    "0 2 * * *",
			Destination: antflyv1.BackupDestination{Location: "s3://bucket/path"},
			Tables:      []string{"table1", "table2"},
		},
	}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"},
		Spec:       antflyv1.AntflyClusterSpec{Image: "antfly:latest"},
	}

	spec := r.buildCronJobSpec(backup, cluster)
	cmd := spec.JobTemplate.Spec.Template.Spec.Containers[0].Args[0]

	if !strings.Contains(cmd, "--tables 'table1,table2'") {
		t.Errorf("tables flag not properly set: %s", cmd)
	}
}

func TestBuildCronJobSpec_StandaloneStillUsesPublicAPIService(t *testing.T) {
	r := &AntflyBackupReconciler{}
	backup := &antflyv1.AntflyBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "standalone-backup", Namespace: "default"},
		Spec: antflyv1.AntflyBackupSpec{
			ClusterRef:  antflyv1.ClusterReference{Name: "standalone-cluster"},
			Schedule:    "0 2 * * *",
			Destination: antflyv1.BackupDestination{Location: "s3://my-bucket/backups"},
		},
	}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "standalone-cluster", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{
			Mode:  antflyv1.ClusterModeStandalone,
			Image: "antfly:latest",
			Standalone: &antflyv1.StandaloneSpec{
				Replicas:     1,
				NodeID:       1,
				MetadataAPI:  antflyv1.APISpec{Port: 8080},
				MetadataRaft: antflyv1.APISpec{Port: 9017},
				StoreAPI:     antflyv1.APISpec{Port: 12380},
				StoreRaft:    antflyv1.APISpec{Port: 9021},
				Health:       antflyv1.APISpec{Port: 4200},
			},
			Storage: antflyv1.StorageSpec{
				StorageClass:      "standard",
				StandaloneStorage: "1Gi",
			},
		},
	}

	spec := r.buildCronJobSpec(backup, cluster)
	container := spec.JobTemplate.Spec.Template.Spec.Containers[0]
	cmd := container.Args[0]

	if strings.Contains(cmd, "--url") {
		t.Fatalf("expected backup command to use ANTFLY_URL instead of --url, got: %s", cmd)
	}
	if got := envValue(container.Env, "ANTFLY_URL"); got != "http://standalone-cluster-public-api.default.svc.cluster.local" {
		t.Fatalf("expected backup URL to continue using public-api service in standalone mode, got: %q", got)
	}
}

func TestRequestsForClusterEnqueuesReferencingBackups(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := antflyv1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme failed: %v", err)
	}

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "cluster-ns"},
	}
	sameNamespaceBackup := &antflyv1.AntflyBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "same-ns", Namespace: "cluster-ns"},
		Spec: antflyv1.AntflyBackupSpec{
			ClusterRef: antflyv1.ClusterReference{Name: "cluster"},
		},
	}
	crossNamespaceBackup := &antflyv1.AntflyBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "cross-ns", Namespace: "backup-ns"},
		Spec: antflyv1.AntflyBackupSpec{
			ClusterRef: antflyv1.ClusterReference{Name: "cluster", Namespace: "cluster-ns"},
		},
	}
	otherBackup := &antflyv1.AntflyBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "cluster-ns"},
		Spec: antflyv1.AntflyBackupSpec{
			ClusterRef: antflyv1.ClusterReference{Name: "other-cluster"},
		},
	}

	r := &AntflyBackupReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(sameNamespaceBackup, crossNamespaceBackup, otherBackup).
			Build(),
	}

	requests := r.requestsForCluster(context.Background(), cluster)
	got := make(map[string]bool, len(requests))
	for _, req := range requests {
		got[req.NamespacedName.String()] = true
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 requests, got %d: %#v", len(got), got)
	}
	if !got["cluster-ns/same-ns"] {
		t.Fatalf("expected same-namespace backup request, got %#v", got)
	}
	if !got["backup-ns/cross-ns"] {
		t.Fatalf("expected cross-namespace backup request, got %#v", got)
	}
	if got["cluster-ns/other"] {
		t.Fatalf("did not expect unrelated backup request, got %#v", got)
	}
}

func TestBackupClusterDependencyChangedPredicate(t *testing.T) {
	pred := backupClusterDependencyChangedPredicate()

	base := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "cluster",
			Namespace:  "default",
			Generation: 10,
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:v1",
			Mode:  antflyv1.ClusterModeClustered,
		},
	}

	if !pred.Create(event.CreateEvent{Object: base}) {
		t.Fatalf("expected create event to enqueue backups")
	}
	if !pred.Delete(event.DeleteEvent{Object: base}) {
		t.Fatalf("expected delete event to enqueue backups")
	}
	if !pred.Generic(event.GenericEvent{Object: base}) {
		t.Fatalf("expected generic event to enqueue backups")
	}

	statusOnly := base.DeepCopy()
	statusOnly.Status.Phase = "Running"
	statusOnly.Generation = base.Generation
	if pred.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: statusOnly}) {
		t.Fatalf("expected status-only cluster update to be filtered")
	}

	unrelatedSpec := base.DeepCopy()
	unrelatedSpec.Generation = base.Generation + 1
	unrelatedSpec.Spec.Mode = antflyv1.ClusterModeStandalone
	if pred.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: unrelatedSpec}) {
		t.Fatalf("expected non-image spec update to be filtered")
	}

	imageChanged := base.DeepCopy()
	imageChanged.Generation = base.Generation + 1
	imageChanged.Spec.Image = "antfly:v2"
	if !pred.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: imageChanged}) {
		t.Fatalf("expected image update to enqueue backups")
	}
}

func envValue(env []corev1.EnvVar, name string) string {
	for _, item := range env {
		if item.Name == name {
			return item.Value
		}
	}
	return ""
}
