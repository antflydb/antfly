package controllers

import (
	"testing"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildRestoreJob_StandaloneStillUsesPublicAPIService(t *testing.T) {
	r := &AntflyRestoreReconciler{}
	restore := &antflyv1.AntflyRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "standalone-restore", Namespace: "default"},
		Spec: antflyv1.AntflyRestoreSpec{
			ClusterRef: antflyv1.ClusterReference{Name: "standalone-cluster"},
			Source: antflyv1.RestoreSource{
				BackupID:   "backup-123",
				Location:   "s3://my-bucket/backups",
				Connection: "archive-reader",
			},
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

	job := r.buildRestoreJob(restore, cluster)
	container := job.Spec.Template.Spec.Containers[0]
	args := container.Args

	if len(container.Command) != 1 || container.Command[0] != "/antfly" {
		t.Fatalf("expected restore job to invoke zig antfly directly, got command: %#v", container.Command)
	}

	for _, arg := range args {
		if arg == "--url" {
			t.Fatalf("expected restore job to use ANTFLY_URL instead of --url, got args: %#v", args)
		}
	}
	foundConnection := false
	for i := 0; i+1 < len(args); i++ {
		if args[i] == "--connection" && args[i+1] == "archive-reader" {
			foundConnection = true
		}
	}
	if !foundConnection {
		t.Fatalf("expected named restore connection in args: %#v", args)
	}
	if got := envValue(container.Env, "ANTFLY_URL"); got != "http://standalone-cluster-public-api.default.svc.cluster.local" {
		t.Fatalf("expected restore URL to continue using public-api service in standalone mode, got: %q", got)
	}
}
