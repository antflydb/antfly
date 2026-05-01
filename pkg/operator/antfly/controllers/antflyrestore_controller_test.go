package controllers

import (
	"testing"

	antflyv1 "github.com/antflydb/antfly/pkg/operator/antfly/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildRestoreJob_SwarmStillUsesPublicAPIService(t *testing.T) {
	r := &AntflyRestoreReconciler{}
	restore := &antflyv1.AntflyRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "swarm-restore", Namespace: "default"},
		Spec: antflyv1.AntflyRestoreSpec{
			ClusterRef: antflyv1.ClusterReference{Name: "swarm-cluster"},
			Source: antflyv1.RestoreSource{
				BackupID: "backup-123",
				Location: "s3://my-bucket/backups",
			},
		},
	}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "swarm-cluster", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{
			Mode:  antflyv1.ClusterModeSwarm,
			Image: "antfly:latest",
			Swarm: &antflyv1.SwarmSpec{
				Replicas:     1,
				NodeID:       1,
				MetadataAPI:  antflyv1.APISpec{Port: 8080},
				MetadataRaft: antflyv1.APISpec{Port: 9017},
				StoreAPI:     antflyv1.APISpec{Port: 12380},
				StoreRaft:    antflyv1.APISpec{Port: 9021},
				Health:       antflyv1.APISpec{Port: 4200},
			},
			Storage: antflyv1.StorageSpec{
				StorageClass: "standard",
				SwarmStorage: "1Gi",
			},
		},
	}

	job := r.buildRestoreJob(restore, cluster)
	args := job.Spec.Template.Spec.Containers[0].Args

	if len(args) < 3 {
		t.Fatalf("expected restore args to include URL, got: %#v", args)
	}
	if args[1] != "--url" {
		t.Fatalf("expected second restore arg to be --url, got: %q", args[1])
	}
	if args[2] != "http://swarm-cluster-public-api.default.svc.cluster.local" {
		t.Fatalf("expected restore URL to continue using public-api service in swarm mode, got: %q", args[2])
	}
}
