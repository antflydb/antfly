package controllers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	haSeedPrefixCleanupRequestAnnotation = "cloud.antfly.io/ha-seed-prefix-cleanup-request"
	haSeedPrefixCleanupKind              = "DeleteHASeedPrefix"
	haSeedPrefixCleanupJobRequestHash    = "antfly.io/ha-seed-prefix-cleanup-request-sha256"
)

type haSeedPrefixCleanupRequest struct {
	Version               int32  `json:"version"`
	Kind                  string `json:"kind"`
	OperationID           string `json:"operation_id"`
	RetryToken            string `json:"retry_token"`
	InstanceID            string `json:"instance_id"`
	TopologyID            string `json:"topology_id"`
	TopologyGeneration    int64  `json:"topology_generation"`
	Location              string `json:"location"`
	PrefixSHA256          string `json:"prefix_sha256"`
	CredentialsSecretName string `json:"credentials_secret_name"`
	DeleteAll             bool   `json:"delete_all"`
	RequestSHA256         string `json:"request_sha256"`
}

func (r *AntflyClusterReconciler) reconcileHASeedPrefixCleanup(
	ctx context.Context,
	cluster *antflyv1.AntflyCluster,
) (ctrl.Result, bool, error) {
	raw := strings.TrimSpace(cluster.Annotations[haSeedPrefixCleanupRequestAnnotation])
	if raw == "" {
		return ctrl.Result{}, false, nil
	}
	request, err := parseHASeedPrefixCleanupRequest(raw, cluster)
	if err != nil {
		return ctrl.Result{}, true, err
	}
	if status := cluster.Status.HAStatus; status != nil && status.SeedPrefixCleanup != nil &&
		status.SeedPrefixCleanup.Phase == "succeeded" && status.SeedPrefixCleanup.Receipt != nil {
		if err := validateHASeedPrefixCleanupReceipt(request, status.SeedPrefixCleanup.Receipt); err != nil {
			return ctrl.Result{}, true, fmt.Errorf("validate persisted HA seed prefix cleanup receipt: %w", err)
		}
		return ctrl.Result{}, true, nil
	}

	job := buildHASeedPrefixCleanupJob(cluster, request)
	if err := controllerutil.SetControllerReference(cluster, job, r.Scheme); err != nil {
		return ctrl.Result{}, true, err
	}
	existing := &batchv1.Job{}
	err = r.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Name}, existing)
	if apierrors.IsNotFound(err) {
		if err := r.persistHASeedPrefixCleanupStatus(ctx, cluster, &antflyv1.HASeedPrefixCleanupStatus{
			Phase: "requested", OperationID: request.OperationID, RetryToken: request.RetryToken,
			RequestSHA256: request.RequestSHA256,
		}); err != nil {
			return ctrl.Result{}, true, err
		}
		if err := r.Create(ctx, job); err != nil {
			return ctrl.Result{}, true, fmt.Errorf("create HA seed prefix cleanup Job: %w", err)
		}
		return ctrl.Result{RequeueAfter: time.Second}, true, nil
	}
	if err != nil {
		return ctrl.Result{}, true, err
	}
	if existing.Annotations[haSeedPrefixCleanupJobRequestHash] != request.RequestSHA256 {
		return ctrl.Result{}, true, fmt.Errorf("HA seed prefix cleanup Job %s has a different immutable request", existing.Name)
	}

	attemptCount := existing.Status.Active + existing.Status.Failed + existing.Status.Succeeded
	if attemptCount < 1 {
		attemptCount = 1
	}
	switch haAdminJobPhase(existing) {
	case haAdminJobPhaseSucceeded:
		body, ok := r.haAdminJobLogBody(ctx, cluster, existing.Name)
		if !ok {
			return ctrl.Result{RequeueAfter: time.Second}, true, nil
		}
		receipt, err := parseHASeedPrefixCleanupReceipt(body)
		if err != nil {
			return ctrl.Result{}, true, err
		}
		if err := validateHASeedPrefixCleanupReceipt(request, receipt); err != nil {
			return ctrl.Result{}, true, err
		}
		if err := r.persistHASeedPrefixCleanupStatus(ctx, cluster, &antflyv1.HASeedPrefixCleanupStatus{
			Phase: "succeeded", OperationID: request.OperationID, RetryToken: request.RetryToken,
			RequestSHA256: request.RequestSHA256, AttemptCount: attemptCount, Receipt: receipt,
		}); err != nil {
			return ctrl.Result{}, true, err
		}
		return ctrl.Result{}, true, nil
	case haAdminJobPhaseFailed:
		message := haSeedPrefixCleanupJobFailure(existing)
		if err := r.persistHASeedPrefixCleanupStatus(ctx, cluster, &antflyv1.HASeedPrefixCleanupStatus{
			Phase: "failed", OperationID: request.OperationID, RetryToken: request.RetryToken,
			RequestSHA256: request.RequestSHA256, AttemptCount: attemptCount, LastError: message,
		}); err != nil {
			return ctrl.Result{}, true, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, true, nil
	default:
		if err := r.persistHASeedPrefixCleanupStatus(ctx, cluster, &antflyv1.HASeedPrefixCleanupStatus{
			Phase: "running", OperationID: request.OperationID, RetryToken: request.RetryToken,
			RequestSHA256: request.RequestSHA256, AttemptCount: attemptCount,
		}); err != nil {
			return ctrl.Result{}, true, err
		}
		return ctrl.Result{RequeueAfter: time.Second}, true, nil
	}
}

func parseHASeedPrefixCleanupRequest(raw string, cluster *antflyv1.AntflyCluster) (haSeedPrefixCleanupRequest, error) {
	var request haSeedPrefixCleanupRequest
	if err := strictHASeedCleanupJSON([]byte(raw), &request); err != nil {
		return request, fmt.Errorf("decode HA seed prefix cleanup request: %w", err)
	}
	if request.Version != 1 || request.Kind != haSeedPrefixCleanupKind || !request.DeleteAll ||
		strings.TrimSpace(request.OperationID) == "" || !isLowerHexDigest(request.RetryToken) ||
		!isLowerHexDigest(request.PrefixSHA256) || !isLowerHexDigest(request.RequestSHA256) ||
		strings.TrimSpace(request.InstanceID) == "" || request.InstanceID != strings.TrimSpace(request.TopologyID) ||
		request.TopologyGeneration <= 0 || strings.TrimSpace(request.CredentialsSecretName) == "" {
		return request, fmt.Errorf("HA seed prefix cleanup request has invalid authority fields")
	}
	instanceID := strings.TrimSpace(cluster.Labels["cloud.antfly.io/instance-id"])
	if request.InstanceID != instanceID {
		return request, fmt.Errorf("HA seed prefix cleanup request instance %q does not match cluster %q", request.InstanceID, instanceID)
	}
	expectedLocationFragment := "/instances/" + request.InstanceID + "/ha-seeds/"
	if !strings.HasPrefix(request.Location, "s3://") || !strings.HasSuffix(request.Location, expectedLocationFragment) ||
		sha256Hex(request.Location) != request.PrefixSHA256 {
		return request, fmt.Errorf("HA seed prefix cleanup request is not bound to the exact instance prefix")
	}
	if rawGeneration := strings.TrimSpace(cluster.Annotations[haTopologyGenerationAnnotation]); rawGeneration != "" {
		generation, err := strconv.ParseInt(rawGeneration, 10, 64)
		if err != nil || generation != request.TopologyGeneration {
			return request, fmt.Errorf("HA seed prefix cleanup topology generation does not match the cluster")
		}
	}
	expectedDigest := request.RequestSHA256
	request.RequestSHA256 = ""
	encoded, err := json.Marshal(request)
	request.RequestSHA256 = expectedDigest
	if err != nil || sha256Hex(string(encoded)) != expectedDigest {
		return request, fmt.Errorf("HA seed prefix cleanup request digest does not match")
	}
	return request, nil
}

func parseHASeedPrefixCleanupReceipt(body string) (*antflyv1.HASeedPrefixCleanupReceipt, error) {
	var receipt antflyv1.HASeedPrefixCleanupReceipt
	if err := strictHASeedCleanupJSON([]byte(strings.TrimSpace(body)), &receipt); err != nil {
		return nil, fmt.Errorf("decode HA seed prefix cleanup receipt: %w", err)
	}
	return &receipt, nil
}

func validateHASeedPrefixCleanupReceipt(request haSeedPrefixCleanupRequest, receipt *antflyv1.HASeedPrefixCleanupReceipt) error {
	if receipt == nil || receipt.Version != request.Version || receipt.Kind != request.Kind ||
		receipt.OperationID != request.OperationID || receipt.RetryToken != request.RetryToken ||
		receipt.InstanceID != request.InstanceID || receipt.TopologyID != request.TopologyID ||
		receipt.TopologyGeneration != request.TopologyGeneration || receipt.Location != request.Location ||
		receipt.PrefixSHA256 != request.PrefixSHA256 || receipt.RequestSHA256 != request.RequestSHA256 ||
		receipt.DeletedGenerations < 0 || receipt.DeletedObjects < 0 || receipt.RetainedObjects != 0 ||
		!receipt.PrefixEmpty || !receipt.Complete || !isLowerHexDigest(receipt.ReceiptSHA256) {
		return fmt.Errorf("HA seed prefix cleanup receipt does not prove the exact prefix empty")
	}
	if _, err := time.Parse(time.RFC3339Nano, receipt.CompletedAt); err != nil {
		return fmt.Errorf("HA seed prefix cleanup receipt completion time is invalid: %w", err)
	}
	expected := receipt.ReceiptSHA256
	receipt.ReceiptSHA256 = ""
	encoded, err := json.Marshal(receipt)
	receipt.ReceiptSHA256 = expected
	if err != nil || sha256Hex(string(encoded)) != expected {
		return fmt.Errorf("HA seed prefix cleanup receipt digest does not match")
	}
	return nil
}

func strictHASeedCleanupJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}

func buildHASeedPrefixCleanupJob(cluster *antflyv1.AntflyCluster, request haSeedPrefixCleanupRequest) *batchv1.Job {
	hash := request.RequestSHA256[:10]
	base := cluster.Name + "-ha-seed-prefix-cleanup"
	if limit := 63 - len(hash) - 1; len(base) > limit {
		base = strings.TrimRight(base[:limit], "-")
	}
	name := base + "-" + hash
	backoff := int32(3)
	deadline := int64(600)
	labels := podLabels(cluster, "ha-admin")
	labels["antfly.io/ha-action-kind"] = "delete-seed-prefix"
	annotations := map[string]string{haSeedPrefixCleanupJobRequestHash: request.RequestSHA256}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace, Labels: labels, Annotations: annotations},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff, ActiveDeadlineSeconds: &deadline,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations},
				Spec: corev1.PodSpec{
					ServiceAccountName: cluster.Spec.ServiceAccountName,
					RestartPolicy:      corev1.RestartPolicyNever,
					SecurityContext:    antflyPodSecurityContext(),
					Containers: []corev1.Container{{
						Name: "ha-admin", Image: cluster.Spec.Image,
						ImagePullPolicy: corev1.PullPolicy(cluster.Spec.ImagePullPolicy),
						Command:         []string{"/antfly"},
						Args: []string{
							"ha", "artifact", "delete-prefix", "--location", request.Location,
							"--operation-id", request.OperationID, "--retry-token", request.RetryToken,
							"--instance-id", request.InstanceID, "--topology-id", request.TopologyID,
							"--topology-generation", strconv.FormatInt(request.TopologyGeneration, 10),
							"--prefix-sha256", request.PrefixSHA256,
							"--credentials-secret-name", request.CredentialsSecretName,
							"--delete-all", "--request-sha256", request.RequestSHA256,
						},
						EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: request.CredentialsSecretName},
						}}},
					}},
				},
			},
		},
	}
}

func (r *AntflyClusterReconciler) persistHASeedPrefixCleanupStatus(
	ctx context.Context,
	cluster *antflyv1.AntflyCluster,
	status *antflyv1.HASeedPrefixCleanupStatus,
) error {
	latest := &antflyv1.AntflyCluster{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), latest); err != nil {
		return err
	}
	if latest.Status.HAStatus == nil {
		latest.Status.HAStatus = &antflyv1.HAStatus{}
	}
	current := latest.Status.HAStatus.SeedPrefixCleanup
	if current != nil && current.RequestSHA256 != "" && current.RequestSHA256 != status.RequestSHA256 {
		return fmt.Errorf("HA seed prefix cleanup status belongs to a different immutable request")
	}
	if current != nil && current.Phase == "succeeded" && status.Phase != "succeeded" {
		return fmt.Errorf("verified HA seed prefix cleanup success is terminal")
	}
	patch := client.MergeFrom(latest.DeepCopy())
	copyStatus := *status
	if status.Receipt != nil {
		copyReceipt := *status.Receipt
		copyStatus.Receipt = &copyReceipt
	}
	if reflect.DeepEqual(current, &copyStatus) {
		cluster.Status = latest.Status
		cluster.ResourceVersion = latest.ResourceVersion
		return nil
	}
	latest.Status.HAStatus.SeedPrefixCleanup = &copyStatus
	if err := r.Status().Patch(ctx, latest, patch); err != nil {
		return fmt.Errorf("persist HA seed prefix cleanup status: %w", err)
	}
	cluster.Status = latest.Status
	cluster.ResourceVersion = latest.ResourceVersion
	return nil
}

func haSeedPrefixCleanupJobFailure(job *batchv1.Job) string {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			if message := strings.TrimSpace(condition.Message); message != "" {
				return message
			}
			if reason := strings.TrimSpace(condition.Reason); reason != "" {
				return reason
			}
		}
	}
	return "HA seed prefix cleanup Job failed"
}

func sha256Hex(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
