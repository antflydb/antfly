package controllers

import (
	"strings"
	"testing"

	antflyv1 "github.com/antflydb/antfly/pkg/operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	cmd := spec.JobTemplate.Spec.Template.Spec.Containers[0].Args[0]

	// Command should start with the antfly backup command
	if !strings.HasPrefix(cmd, "/antfly backup") {
		t.Errorf("command should start with '/antfly backup', got: %s", cmd)
	}

	// Backup name should be shell-quoted in the backup-id
	if !strings.Contains(cmd, "'my-backup'-$(date +%Y%m%d%H%M%S)") {
		t.Errorf("backup name not properly quoted in backup-id: %s", cmd)
	}

	// URL should be shell-quoted
	if !strings.Contains(cmd, "--url 'http://my-cluster-public-api.default.svc.cluster.local'") {
		t.Errorf("URL not properly quoted: %s", cmd)
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
