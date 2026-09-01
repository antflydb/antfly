package controllers

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func validHASeedPrefixCleanupRequest(t *testing.T) (haSeedPrefixCleanupRequest, *antflyv1.AntflyCluster) {
	t.Helper()
	location := "s3://antfly-cloud-backups/orgs/org-a/instances/instance-a/ha-seeds/"
	request := haSeedPrefixCleanupRequest{
		Version: 1, Kind: haSeedPrefixCleanupKind,
		OperationID: "ha-seed-delete-0123456789abcdef0123456789abcdef",
		RetryToken:  sha256Hex("retry"), InstanceID: "instance-a", TopologyID: "instance-a",
		TopologyGeneration: 2, Location: location, PrefixSHA256: sha256Hex(location),
		CredentialsSecretName: "cloud-ha-seed-credentials", DeleteAll: true,
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	request.RequestSHA256 = sha256Hex(string(encoded))
	cluster := &antflyv1.AntflyCluster{}
	cluster.Name = "antflydb-standby-a"
	cluster.Namespace = "tenant-a"
	cluster.Labels = map[string]string{"cloud.antfly.io/instance-id": request.InstanceID}
	cluster.Annotations = map[string]string{haTopologyGenerationAnnotation: "2"}
	cluster.Spec.Image = "antfly@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	cluster.Spec.ServiceAccountName = "antfly-runtime"
	return request, cluster
}

func TestParseHASeedPrefixCleanupRequestValidatesExactAuthority(t *testing.T) {
	request, cluster := validHASeedPrefixCleanupRequest(t)
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parseHASeedPrefixCleanupRequest(string(raw), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.RequestSHA256 != request.RequestSHA256 {
		t.Fatalf("request digest = %q, want %q", parsed.RequestSHA256, request.RequestSHA256)
	}

	request.Location = "s3://antfly-cloud-backups/orgs/org-a/instances/other/ha-seeds/"
	raw, _ = json.Marshal(request)
	if _, err := parseHASeedPrefixCleanupRequest(string(raw), cluster); err == nil {
		t.Fatal("cross-instance cleanup location was accepted")
	}
}

func TestBuildHASeedPrefixCleanupJobCarriesEveryBoundField(t *testing.T) {
	request, cluster := validHASeedPrefixCleanupRequest(t)
	job := buildHASeedPrefixCleanupJob(cluster, request)
	container := job.Spec.Template.Spec.Containers[0]
	expectedArgs := []string{
		"ha", "artifact", "delete-prefix", "--location", request.Location,
		"--operation-id", request.OperationID, "--retry-token", request.RetryToken,
		"--instance-id", request.InstanceID, "--topology-id", request.TopologyID,
		"--topology-generation", "2", "--prefix-sha256", request.PrefixSHA256,
		"--credentials-secret-name", request.CredentialsSecretName, "--delete-all",
		"--request-sha256", request.RequestSHA256,
	}
	if !reflect.DeepEqual(container.Args, expectedArgs) {
		t.Fatalf("cleanup Job args = %v, want exact ordered authority %v", container.Args, expectedArgs)
	}
	if got := container.EnvFrom[0].SecretRef.Name; got != request.CredentialsSecretName {
		t.Fatalf("credentials Secret = %q, want %q", got, request.CredentialsSecretName)
	}
	if job.Annotations[haSeedPrefixCleanupJobRequestHash] != request.RequestSHA256 {
		t.Fatal("cleanup Job is not annotated with the immutable request digest")
	}
}

func TestReconcileHASeedPrefixCleanupPersistsIdentityBeforeCreatingOwnedJob(t *testing.T) {
	request, cluster := validHASeedPrefixCleanupRequest(t)
	cluster.UID = types.UID("cluster-uid")
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	cluster.Annotations[haSeedPrefixCleanupRequestAnnotation] = string(raw)

	scheme := runtime.NewScheme()
	if err := antflyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&antflyv1.AntflyCluster{}).
		WithObjects(cluster).
		Build()
	reconciler := &AntflyClusterReconciler{Client: k8sClient, Scheme: scheme}

	result, handled, err := reconciler.reconcileHASeedPrefixCleanup(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if !handled || result.RequeueAfter <= 0 {
		t.Fatalf("cleanup reconcile = handled %v, result %#v", handled, result)
	}

	observed := &antflyv1.AntflyCluster{}
	key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
	if err := k8sClient.Get(context.Background(), key, observed); err != nil {
		t.Fatal(err)
	}
	status := observed.Status.HAStatus
	if status == nil || status.SeedPrefixCleanup == nil ||
		status.SeedPrefixCleanup.Phase != "requested" ||
		status.SeedPrefixCleanup.OperationID != request.OperationID ||
		status.SeedPrefixCleanup.RetryToken != request.RetryToken ||
		status.SeedPrefixCleanup.RequestSHA256 != request.RequestSHA256 {
		t.Fatalf("cleanup request identity was not checkpointed before execution: %#v", status)
	}

	job := &batchv1.Job{}
	jobName := buildHASeedPrefixCleanupJob(cluster, request).Name
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: jobName, Namespace: cluster.Namespace}, job); err != nil {
		t.Fatal(err)
	}
	if len(job.OwnerReferences) != 1 || job.OwnerReferences[0].UID != cluster.UID || !*job.OwnerReferences[0].Controller {
		t.Fatalf("cleanup Job is not exclusively owned by the request controller: %#v", job.OwnerReferences)
	}
}

func TestValidateHASeedPrefixCleanupReceiptRejectsPartialOrStaleProof(t *testing.T) {
	request, _ := validHASeedPrefixCleanupRequest(t)
	receipt := &antflyv1.HASeedPrefixCleanupReceipt{
		Version: request.Version, Kind: request.Kind, OperationID: request.OperationID,
		RetryToken: request.RetryToken, InstanceID: request.InstanceID, TopologyID: request.TopologyID,
		TopologyGeneration: request.TopologyGeneration, Location: request.Location,
		PrefixSHA256: request.PrefixSHA256, RequestSHA256: request.RequestSHA256,
		DeletedGenerations: 2, DeletedObjects: 9, PrefixEmpty: true, Complete: true,
		CompletedAt: "2026-08-28T16:00:00.000000000Z",
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	receipt.ReceiptSHA256 = sha256Hex(string(encoded))
	if err := validateHASeedPrefixCleanupReceipt(request, receipt); err != nil {
		t.Fatal(err)
	}

	receipt.PrefixEmpty = false
	if err := validateHASeedPrefixCleanupReceipt(request, receipt); err == nil {
		t.Fatal("partial cleanup receipt was accepted")
	}
	receipt.PrefixEmpty = true
	receipt.RequestSHA256 = sha256Hex("stale")
	if err := validateHASeedPrefixCleanupReceipt(request, receipt); err == nil {
		t.Fatal("stale cleanup receipt was accepted")
	}
}
