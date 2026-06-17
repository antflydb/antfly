package controllers

//go:generate go tool controller-gen rbac:roleName=antfly-operator-cluster-role paths="../..." output:rbac:artifacts:config=../../manifests/rbac

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	inferencev1alpha1 "github.com/antflydb/antfly/go/pkg/operator/api/inference/v1alpha1"
	"github.com/antflydb/antfly/go/pkg/operator/controllers/internal/poddiagnostics"
)

// AntflyClusterReconciler reconciles an AntflyCluster object
type AntflyClusterReconciler struct {
	client.Client
	Scheme                *runtime.Scheme
	AutoScaler            *AutoScaler
	KubeClient            kubernetes.Interface
	NodeStatsFetcher      func(context.Context, string) (*kubeletStatsSummary, error)
	HTTPClient            *http.Client
	Recorder              events.EventRecorder
	ManageInferencePools  bool
	DefaultInferenceImage string

	// validationAttempts tracks consecutive validation failure counts per cluster
	// (namespace/name -> int). Reset on successful validation. Used for
	// exponential backoff on repeated validation failures.
	validationAttempts sync.Map
}

var defaultOperatorHTTPClient = &http.Client{Timeout: 10 * time.Second}

const (
	antflyRuntimeUID int64 = 10001
	antflyRuntimeGID int64 = 10001

	antflySecretStoreVolumeName  = "secret-store"
	antflySecretStoreDefaultKey  = "secrets.json"
	antflySecretStoreDefaultPath = "/run/antfly/secrets/secrets.json" // #nosec G101 -- file path, not a credential
	antflySecretStoreEnvVar      = "ANTFLY_SECRET_STORE_PATH"         // #nosec G101 -- environment variable name, not a credential

	haPrimaryRouteTargetAnnotation          = "antfly.io/ha-primary-route-target"
	haPrimaryRouteFenceAuthorityAnnotation  = "antfly.io/ha-primary-route-fence-authority"
	haPrimaryRouteFenceGenerationAnnotation = "antfly.io/ha-primary-route-fence-generation"
	haPrimaryRouteSelectorAnnotation        = "antfly.io/ha-primary-route-selector-applied"

	haAdminJobPhaseWaitingDependency = "WaitingDependency"
	haAdminJobPhasePending           = "Pending"
	haAdminJobPhaseRunning           = "Running"
	haAdminJobPhaseSucceeded         = "Succeeded"
	haAdminJobPhaseFailed            = "Failed"
)

//+kubebuilder:rbac:groups=antfly.io,resources=antflyclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=antfly.io,resources=antflyclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=antfly.io,resources=antflyclusters/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete
//+kubebuilder:rbac:groups="",resources=pods/log,verbs=get
//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch
//+kubebuilder:rbac:groups=metrics.k8s.io,resources=pods,verbs=get;list
//+kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch
//+kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=antfly.io,resources=inferencepools,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=antfly.io,resources=inferencepools/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=antfly.io,resources=inferencepools/finalizers,verbs=update
// No delete on CRDs: startup bootstrap applies CRDs but never removes them.
//+kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch;create;update;patch

var reservedPodLabelPrefixes = []string{"app.kubernetes.io/"}

func int64Ptr(v int64) *int64 {
	return &v
}

func secretStoreKey(store *antflyv1.SecretStoreSpec) string {
	if store == nil || store.Key == "" {
		return antflySecretStoreDefaultKey
	}
	return store.Key
}

func secretStorePath(store *antflyv1.SecretStoreSpec) string {
	if store == nil || store.Path == "" {
		return antflySecretStoreDefaultPath
	}
	return store.Path
}

func secretStoreEnv(store *antflyv1.SecretStoreSpec) []corev1.EnvVar {
	if store == nil {
		return nil
	}
	return []corev1.EnvVar{{
		Name:  antflySecretStoreEnvVar,
		Value: secretStorePath(store),
	}}
}

func secretStoreArg(store *antflyv1.SecretStoreSpec) string {
	if store == nil {
		return ""
	}
	return fmt.Sprintf(" \\\n  --secret-store-path %s", secretStorePath(store))
}

func secretStoreVolumeMounts(store *antflyv1.SecretStoreSpec) []corev1.VolumeMount {
	if store == nil {
		return nil
	}
	return []corev1.VolumeMount{{
		Name:      antflySecretStoreVolumeName,
		MountPath: path.Dir(secretStorePath(store)),
		ReadOnly:  true,
	}}
}

func secretStoreVolumes(store *antflyv1.SecretStoreSpec) []corev1.Volume {
	if store == nil {
		return nil
	}
	return []corev1.Volume{{
		Name: antflySecretStoreVolumeName,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: store.SecretName,
				Items: []corev1.KeyToPath{{
					Key:  secretStoreKey(store),
					Path: path.Base(secretStorePath(store)),
				}},
			},
		},
	}}
}

func podFSGroupChangePolicyPtr(v corev1.PodFSGroupChangePolicy) *corev1.PodFSGroupChangePolicy {
	return &v
}

func antflyPodSecurityContext() *corev1.PodSecurityContext {
	return &corev1.PodSecurityContext{
		FSGroup:             int64Ptr(antflyRuntimeGID),
		FSGroupChangePolicy: podFSGroupChangePolicyPtr(corev1.FSGroupChangeOnRootMismatch),
	}
}

// podLabels returns the standard labels for pod templates including the instance identifier.
// These are a superset of serviceSelectorLabels — they include managed-by labels
// that MUST NOT be added to StatefulSet spec.selector.matchLabels (immutable after creation).
func podLabels(cluster *antflyv1.AntflyCluster, component string) map[string]string {
	labels := map[string]string{
		"app.kubernetes.io/name":       "antfly-database",
		"app.kubernetes.io/component":  component,
		"app.kubernetes.io/instance":   cluster.Name,
		"app.kubernetes.io/managed-by": "antfly-operator",
	}

	for k, v := range cluster.Labels {
		reserved := false
		for _, prefix := range reservedPodLabelPrefixes {
			if strings.HasPrefix(k, prefix) {
				reserved = true
				break
			}
		}
		if reserved {
			continue
		}
		labels[k] = v
	}

	return labels
}

// serviceSelectorLabels returns the labels used for Service and StatefulSet selectors.
// These are immutable after StatefulSet creation and must remain stable.
// Includes instance to prevent two AntflyClusters in the same namespace from
// adopting each other's pods. Existing StatefulSets will require recreation
// (PVCs are preserved) when upgrading to this version.
func serviceSelectorLabels(clusterName, component string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "antfly-database",
		"app.kubernetes.io/component": component,
		"app.kubernetes.io/instance":  clusterName,
	}
}

func haCurrentPrimaryRouteTarget(cluster *antflyv1.AntflyCluster) string {
	if cluster.Status.HAStatus != nil && cluster.Status.HAStatus.PrimaryRoute.CurrentTarget != "" {
		return cluster.Status.HAStatus.PrimaryRoute.CurrentTarget
	}
	return "primary"
}

func haPrimaryRouteManaged(cluster *antflyv1.AntflyCluster) bool {
	return cluster.Spec.HighAvailability != nil &&
		cluster.Spec.HighAvailability.Mode != "" &&
		cluster.Spec.HighAvailability.Mode != antflyv1.HAModeDisabled
}

func haPublicAPISelector(cluster *antflyv1.AntflyCluster, swarmMode bool, target string) (map[string]string, bool) {
	if target == "" || target == "primary" {
		component := "metadata"
		if swarmMode {
			component = "swarm"
		}
		return serviceSelectorLabels(cluster.Name, component), true
	}
	ha := cluster.Spec.HighAvailability
	if ha == nil {
		return nil, false
	}
	for _, standby := range ha.Standbys {
		if standby.Name == target && len(standby.RouteSelector) > 0 {
			return maps.Clone(standby.RouteSelector), true
		}
	}
	return nil, false
}

func haPrimaryRouteServiceAnnotations(cluster *antflyv1.AntflyCluster, target string, selectorApplied bool) map[string]string {
	annotations := map[string]string{
		haPrimaryRouteTargetAnnotation:   target,
		haPrimaryRouteSelectorAnnotation: strconv.FormatBool(selectorApplied),
	}
	if cluster.Status.HAStatus == nil || target == "" || target == "primary" {
		return annotations
	}
	route := cluster.Status.HAStatus.PrimaryRoute
	if route.FenceAuthority != "" {
		annotations[haPrimaryRouteFenceAuthorityAnnotation] = string(route.FenceAuthority)
	}
	if route.FenceGeneration > 0 {
		annotations[haPrimaryRouteFenceGenerationAnnotation] = strconv.FormatUint(route.FenceGeneration, 10)
	}
	return annotations
}

func syncHAPrimaryRouteServiceAnnotations(service *corev1.Service, desired map[string]string) {
	if service.Annotations == nil {
		service.Annotations = map[string]string{}
	}
	managedKeys := [...]string{
		haPrimaryRouteTargetAnnotation,
		haPrimaryRouteFenceAuthorityAnnotation,
		haPrimaryRouteFenceGenerationAnnotation,
		haPrimaryRouteSelectorAnnotation,
	}
	for _, key := range managedKeys {
		if value, ok := desired[key]; ok {
			service.Annotations[key] = value
		} else {
			delete(service.Annotations, key)
		}
	}
}

// buildPVCRetentionPolicy maps CRD PVCRetentionPolicy to the Kubernetes StatefulSet retention policy.
// Returns nil if no retention policy is configured (Kubernetes defaults to Retain/Retain).
func buildPVCRetentionPolicy(policy *antflyv1.PVCRetentionPolicy) *appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy {
	if policy == nil {
		return nil
	}

	mapPolicy := func(val antflyv1.PVCRetentionPolicyType) appsv1.PersistentVolumeClaimRetentionPolicyType {
		if val == antflyv1.PVCRetentionDelete {
			return appsv1.DeletePersistentVolumeClaimRetentionPolicyType
		}
		return appsv1.RetainPersistentVolumeClaimRetentionPolicyType
	}

	return &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenDeleted: mapPolicy(policy.WhenDeleted),
		WhenScaled:  mapPolicy(policy.WhenScaled),
	}
}

// cleanupStorageResources handles ordered deletion of StatefulSets, pods, and PVCs
// on cluster deletion. Returns a non-nil Result if the caller should requeue.
// Deletion order: StatefulSets → wait for pods → PVCs → remove finalizer.
// This avoids a deadlock where PVCs have kubernetes.io/pvc-protection finalizer
// blocking deletion while pods still reference them.
func (r *AntflyClusterReconciler) cleanupStorageResources(ctx context.Context, cluster *antflyv1.AntflyCluster) (*ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Step 1: Delete all known StatefulSets
	for _, suffix := range []string{"-metadata", "-data", "-swarm"} {
		sts := &appsv1.StatefulSet{}
		stsName := cluster.Name + suffix
		err := r.Get(ctx, types.NamespacedName{Name: stsName, Namespace: cluster.Namespace}, sts)
		if err == nil {
			log.Info("Deleting StatefulSet for PVC cleanup", "statefulset", stsName)
			if err := r.Delete(ctx, sts); err != nil && !errors.IsNotFound(err) {
				return nil, fmt.Errorf("failed to delete StatefulSet %s: %w", stsName, err)
			}
		} else if !errors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get StatefulSet %s: %w", stsName, err)
		}
	}

	// Step 2: Check if pods still exist — requeue if they do
	var podList corev1.PodList
	for _, component := range []string{"metadata", "data", "swarm"} {
		if err := r.List(ctx, &podList, client.InNamespace(cluster.Namespace),
			client.MatchingLabels(serviceSelectorLabels(cluster.Name, component))); err != nil {
			return nil, fmt.Errorf("failed to list %s pods: %w", component, err)
		}
		if len(podList.Items) > 0 {
			log.Info("Waiting for pods to terminate", "component", component, "remaining", len(podList.Items))
			result := ctrl.Result{RequeueAfter: 5 * time.Second}
			return &result, nil
		}
	}

	// Step 3: Delete PVCs belonging to this cluster.
	// First try a label-scoped listing (works for clusters created with labeled VolumeClaimTemplates).
	// Fall back to a namespace-wide listing with name prefix matching for older clusters
	// whose PVCs lack instance labels.
	prefixes := []string{
		"metadata-storage-" + cluster.Name + "-metadata-",
		"data-storage-" + cluster.Name + "-data-",
		"swarm-storage-" + cluster.Name + "-swarm-",
	}

	var pvcList corev1.PersistentVolumeClaimList
	if err := r.List(ctx, &pvcList, client.InNamespace(cluster.Namespace),
		client.MatchingLabels{"app.kubernetes.io/instance": cluster.Name}); err != nil {
		return nil, fmt.Errorf("failed to list PVCs: %w", err)
	}

	// If no labeled PVCs found, fall back to namespace-wide list with prefix matching
	// (backward compatibility for clusters created before labels were added to VolumeClaimTemplates)
	if len(pvcList.Items) == 0 {
		if err := r.List(ctx, &pvcList, client.InNamespace(cluster.Namespace)); err != nil {
			return nil, fmt.Errorf("failed to list PVCs: %w", err)
		}
	}

	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		if hasAnyPrefix(pvc.Name, prefixes) {
			// In the fallback path (namespace-wide listing), skip PVCs that are
			// labeled for a different cluster to avoid cross-cluster deletion.
			if inst, ok := pvc.Labels["app.kubernetes.io/instance"]; ok && inst != cluster.Name {
				continue
			}
			log.Info("Deleting PVC", "pvc", pvc.Name)
			if err := r.Delete(ctx, pvc); err != nil && !errors.IsNotFound(err) {
				return nil, fmt.Errorf("failed to delete PVC %s: %w", pvc.Name, err)
			}
		}
	}

	return nil, nil
}

type dataNodeShutdownStatus struct {
	NodeID          int64  `json:"node_id"`
	Phase           string `json:"phase"`
	SafeToTerminate bool   `json:"safe_to_terminate"`
	Blocked         bool   `json:"blocked,omitempty"`
	BlockedReason   string `json:"blocked_reason,omitempty"`
	Message         string `json:"message,omitempty"`
}

func (r *AntflyClusterReconciler) httpClient() *http.Client {
	if r.HTTPClient != nil {
		return r.HTTPClient
	}
	return defaultOperatorHTTPClient
}

// requestDataNodeShutdown asks Antfly metadata to drain a data node and returns
// the current runtime safety status. Node ID is deterministic:
// node_id = pod_ordinal + 1.
func (r *AntflyClusterReconciler) requestDataNodeShutdown(ctx context.Context, cluster *antflyv1.AntflyCluster, nodeIDString string) (*dataNodeShutdownStatus, error) {
	log := log.FromContext(ctx)
	metadataAddr := fmt.Sprintf("http://%s-metadata.%s.svc:%d",
		cluster.Name, cluster.Namespace, cluster.Spec.MetadataNodes.MetadataAPI.Port)
	url := fmt.Sprintf("%s/internal/v1/nodes/%s/shutdown", metadataAddr, nodeIDString)

	log.Info("Requesting data node shutdown before scale-down", "nodeID", nodeIDString)
	body := strings.NewReader(`{"type":"remove","reason":"operator scale-down"}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create node shutdown request for node %s: %w", nodeIDString, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient().Do(req) //nolint:gosec // URL is constructed from cluster-internal service address, not external user input
	if err != nil {
		log.Error(err, "Failed to request data node shutdown, will retry", "nodeID", nodeIDString)
		return nil, fmt.Errorf("failed to request shutdown for node %s: %w", nodeIDString, err)
	}
	statusCode := resp.StatusCode
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if statusCode < 200 || statusCode >= 300 {
		return nil, fmt.Errorf("shutdown request for node %s returned status %d", nodeIDString, statusCode)
	}

	statusReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create node shutdown status request for node %s: %w", nodeIDString, err)
	}
	statusResp, err := r.httpClient().Do(statusReq) //nolint:gosec // URL is constructed from cluster-internal service address, not external user input
	if err != nil {
		return nil, fmt.Errorf("failed to get shutdown status for node %s: %w", nodeIDString, err)
	}
	defer func() { _ = statusResp.Body.Close() }()
	if statusResp.StatusCode < 200 || statusResp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, statusResp.Body)
		return nil, fmt.Errorf("shutdown status for node %s returned status %d", nodeIDString, statusResp.StatusCode)
	}

	var status dataNodeShutdownStatus
	if err := json.NewDecoder(statusResp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to decode shutdown status for node %s: %w", nodeIDString, err)
	}
	return &status, nil
}

func (r *AntflyClusterReconciler) cancelDataNodeShutdown(ctx context.Context, cluster *antflyv1.AntflyCluster, nodeIDString string) (*dataNodeShutdownStatus, error) {
	log := log.FromContext(ctx)
	metadataAddr := fmt.Sprintf("http://%s-metadata.%s.svc:%d",
		cluster.Name, cluster.Namespace, cluster.Spec.MetadataNodes.MetadataAPI.Port)
	url := fmt.Sprintf("%s/internal/v1/nodes/%s/shutdown", metadataAddr, nodeIDString)

	log.Info("Canceling data node shutdown", "nodeID", nodeIDString)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create node shutdown cancellation request for node %s: %w", nodeIDString, err)
	}
	resp, err := r.httpClient().Do(req) //nolint:gosec // URL is constructed from cluster-internal service address, not external user input
	if err != nil {
		return nil, fmt.Errorf("failed to cancel shutdown for node %s: %w", nodeIDString, err)
	}
	statusCode := resp.StatusCode
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if statusCode < 200 || statusCode >= 300 {
		return nil, fmt.Errorf("shutdown cancellation for node %s returned status %d", nodeIDString, statusCode)
	}

	statusReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create node shutdown status request after cancellation for node %s: %w", nodeIDString, err)
	}
	statusResp, err := r.httpClient().Do(statusReq) //nolint:gosec // URL is constructed from cluster-internal service address, not external user input
	if err != nil {
		return nil, fmt.Errorf("failed to get shutdown status after cancellation for node %s: %w", nodeIDString, err)
	}
	defer func() { _ = statusResp.Body.Close() }()
	if statusResp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, statusResp.Body)
		return &dataNodeShutdownStatus{Phase: "not_found"}, nil
	}
	if statusResp.StatusCode < 200 || statusResp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, statusResp.Body)
		return nil, fmt.Errorf("shutdown status after cancellation for node %s returned status %d", nodeIDString, statusResp.StatusCode)
	}

	var status dataNodeShutdownStatus
	if err := json.NewDecoder(statusResp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to decode shutdown status after cancellation for node %s: %w", nodeIDString, err)
	}
	return &status, nil
}

func (r *AntflyClusterReconciler) finalizeDataNodeShutdown(ctx context.Context, cluster *antflyv1.AntflyCluster, nodeIDString string) error {
	metadataAddr := fmt.Sprintf("http://%s-metadata.%s.svc:%d",
		cluster.Name, cluster.Namespace, cluster.Spec.MetadataNodes.MetadataAPI.Port)
	url := fmt.Sprintf("%s/internal/v1/nodes/%s", metadataAddr, nodeIDString)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create node shutdown finalization request for node %s: %w", nodeIDString, err)
	}
	resp, err := r.httpClient().Do(req) //nolint:gosec // URL is constructed from cluster-internal service address, not external user input
	if err != nil {
		return fmt.Errorf("failed to finalize shutdown for node %s: %w", nodeIDString, err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("shutdown finalization for node %s returned status %d", nodeIDString, resp.StatusCode)
	}
	return nil
}

const (
	// annotationDefaultTopologySpread tracks whether the operator applied default topology spread
	annotationDefaultTopologySpread = "antfly.io/default-topology-spread"
)

type topologyMode string

const (
	topologyModeClustered topologyMode = "clustered"
	topologyModeSwarm     topologyMode = "swarm"
)

func effectiveTopologyMode(cluster *antflyv1.AntflyCluster) topologyMode {
	switch cluster.Spec.Mode {
	case antflyv1.ClusterModeSwarm:
		return topologyModeSwarm
	case antflyv1.ClusterModeClustered, "":
		return topologyModeClustered
	default:
		return topologyModeClustered
	}
}

func isSwarmMode(cluster *antflyv1.AntflyCluster) bool {
	return effectiveTopologyMode(cluster) == topologyModeSwarm
}

func shouldCancelDataScaleDown(status *antflyv1.DataScaleDownStatus, currentReplicas, desiredReplicas int32) bool {
	if status == nil || status.DrainingNodeID == "" {
		return false
	}
	switch status.Phase {
	case "Draining", "Blocked", "Failed", "Canceling", "Scaling":
	default:
		return false
	}
	return currentReplicas > status.DrainingOrdinal && desiredReplicas > status.DrainingOrdinal
}

func shouldCancelDataScaleDownForSuspend(status *antflyv1.DataScaleDownStatus, currentReplicas int32) bool {
	if status == nil || status.DrainingNodeID == "" {
		return false
	}
	switch status.Phase {
	case "Draining", "Blocked", "Failed", "Canceling", "Scaling":
	default:
		return false
	}
	return currentReplicas > status.DrainingOrdinal
}

func shouldFinalizeDataScaleDown(status *antflyv1.DataScaleDownStatus, currentReplicas, desiredReplicas int32, dataScaleDownRequested bool) bool {
	if status == nil || status.DrainingNodeID == "" || shouldCancelDataScaleDown(status, currentReplicas, desiredReplicas) {
		return false
	}
	switch status.Phase {
	case "Scaling":
		return true
	case "Failed":
		return !dataScaleDownRequested && status.AppliedReplicas < status.FromReplicas && currentReplicas <= status.AppliedReplicas
	default:
		return false
	}
}

func (r *AntflyClusterReconciler) ensureTopologyResourcesMatchMode(ctx context.Context, cluster *antflyv1.AntflyCluster, mode topologyMode) error {
	if mode == topologyModeSwarm {
		for _, name := range []string{cluster.Name + "-metadata", cluster.Name + "-data"} {
			sts := &appsv1.StatefulSet{}
			if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, sts); err == nil {
				return fmt.Errorf("swarm mode cannot reconcile while clustered StatefulSet %q exists; recreate the cluster instead", name)
			} else if !errors.IsNotFound(err) {
				return fmt.Errorf("failed to check existing clustered StatefulSet %q: %w", name, err)
			}
		}
		return nil
	}

	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name + "-swarm", Namespace: cluster.Namespace}, sts); err == nil {
		return fmt.Errorf("clustered mode cannot reconcile while swarm StatefulSet %q exists; recreate the cluster instead", cluster.Name+"-swarm")
	} else if !errors.IsNotFound(err) {
		return fmt.Errorf("failed to check existing swarm StatefulSet %q: %w", cluster.Name+"-swarm", err)
	}

	return nil
}

// applyDefaultZoneTopologySpread adds a soft zone topology spread constraint when:
// - User has not specified explicit topology constraints in the CRD
// - GKE Autopilot is not enabled (Autopilot manages topology internally)
// - The StatefulSet is new OR already has the operator annotation (supports existing clusters opting in)
func applyDefaultZoneTopologySpread(statefulSet *appsv1.StatefulSet, podTemplate *corev1.PodTemplateSpec, component string, clusterName string, userConstraints []corev1.TopologySpreadConstraint, isGKEAutopilot bool) {
	// User has explicit constraints — respect them, remove our annotation if present
	if len(userConstraints) > 0 {
		delete(statefulSet.Annotations, annotationDefaultTopologySpread)
		return
	}

	// GKE Autopilot manages topology internally
	if isGKEAutopilot {
		return
	}

	// Only apply to new StatefulSets or those already tracked by our annotation
	if statefulSet.CreationTimestamp.IsZero() || statefulSet.Annotations[annotationDefaultTopologySpread] == "true" {
		// Ensure StatefulSet has annotations map
		if statefulSet.Annotations == nil {
			statefulSet.Annotations = make(map[string]string)
		}
		statefulSet.Annotations[annotationDefaultTopologySpread] = "true"

		podTemplate.Spec.TopologySpreadConstraints = append(podTemplate.Spec.TopologySpreadConstraints,
			corev1.TopologySpreadConstraint{
				MaxSkew:           1,
				TopologyKey:       "topology.kubernetes.io/zone",
				WhenUnsatisfiable: corev1.ScheduleAnyway,
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: serviceSelectorLabels(clusterName, component),
				},
			},
		)
	}
}

// applyDefaults sets default port values if not specified
func (r *AntflyClusterReconciler) applyDefaults(cluster *antflyv1.AntflyCluster) {
	swarmMode := isSwarmMode(cluster)

	if cluster.Spec.Mode == "" {
		cluster.Spec.Mode = antflyv1.ClusterModeClustered
	}

	if swarmMode && cluster.Spec.Swarm != nil {
		if cluster.Spec.Swarm.Replicas == 0 {
			cluster.Spec.Swarm.Replicas = 1
		}
		if cluster.Spec.Swarm.NodeID == 0 {
			cluster.Spec.Swarm.NodeID = 1
		}
		if cluster.Spec.Swarm.MetadataAPI.Port == 0 {
			cluster.Spec.Swarm.MetadataAPI.Port = 8080
		}
		if cluster.Spec.Swarm.MetadataRaft.Port == 0 {
			cluster.Spec.Swarm.MetadataRaft.Port = 9017
		}
		if cluster.Spec.Swarm.StoreAPI.Port == 0 {
			cluster.Spec.Swarm.StoreAPI.Port = 12380
		}
		if cluster.Spec.Swarm.StoreRaft.Port == 0 {
			cluster.Spec.Swarm.StoreRaft.Port = 9021
		}
		if cluster.Spec.Swarm.Health.Port == 0 {
			cluster.Spec.Swarm.Health.Port = 4200
		}
		if cluster.Spec.Swarm.Inference == nil {
			cluster.Spec.Swarm.Inference = &antflyv1.SwarmInferenceSpec{
				Enabled: true,
				APIURL:  "http://0.0.0.0:11433",
			}
		} else if cluster.Spec.Swarm.Inference.APIURL == "" {
			cluster.Spec.Swarm.Inference.APIURL = "http://0.0.0.0:11433"
		}
	}

	if !swarmMode {
		// Default ports for metadata nodes
		if cluster.Spec.MetadataNodes.MetadataAPI.Port == 0 {
			cluster.Spec.MetadataNodes.MetadataAPI.Port = 12377
		}
		if cluster.Spec.MetadataNodes.MetadataRaft.Port == 0 {
			cluster.Spec.MetadataNodes.MetadataRaft.Port = 9017
		}
		if cluster.Spec.MetadataNodes.Health.Port == 0 {
			cluster.Spec.MetadataNodes.Health.Port = 4200
		}

		// Default ports for data nodes
		if cluster.Spec.DataNodes.API.Port == 0 {
			cluster.Spec.DataNodes.API.Port = 12380
		}
		if cluster.Spec.DataNodes.Raft.Port == 0 {
			cluster.Spec.DataNodes.Raft.Port = 9021
		}
		if cluster.Spec.DataNodes.Health.Port == 0 {
			cluster.Spec.DataNodes.Health.Port = 4200
		}
	}

	// Default service mesh configuration
	if cluster.Spec.ServiceMesh == nil {
		cluster.Spec.ServiceMesh = &antflyv1.ServiceMeshSpec{
			Enabled: false,
		}
	}

	if cluster.Spec.Storage.StorageAutoGrow != nil && cluster.Spec.Storage.StorageAutoGrow.Enabled {
		if cluster.Spec.Storage.StorageAutoGrow.GrowThresholdPercent == 0 {
			cluster.Spec.Storage.StorageAutoGrow.GrowThresholdPercent = 85
		}
		if cluster.Spec.Storage.StorageAutoGrow.GrowIncrement == "" {
			cluster.Spec.Storage.StorageAutoGrow.GrowIncrement = "10Gi"
		}
	}

	// Default PublicAPI configuration
	if cluster.Spec.PublicAPI == nil {
		enabled := false
		serviceType := corev1.ServiceTypeLoadBalancer
		cluster.Spec.PublicAPI = &antflyv1.PublicAPIConfig{
			Enabled:     &enabled,
			ServiceType: &serviceType,
			Port:        80,
		}
	} else {
		if cluster.Spec.PublicAPI.Enabled == nil {
			enabled := false
			cluster.Spec.PublicAPI.Enabled = &enabled
		}
		if cluster.Spec.PublicAPI.ServiceType == nil {
			serviceType := corev1.ServiceTypeLoadBalancer
			cluster.Spec.PublicAPI.ServiceType = &serviceType
		}
		if cluster.Spec.PublicAPI.Port == 0 {
			cluster.Spec.PublicAPI.Port = 80
		}
	}

	// GKE Autopilot defaults (T024)
	if cluster.Spec.GKE != nil && cluster.Spec.GKE.Autopilot {
		if cluster.Spec.GKE.AutopilotComputeClass == "" {
			cluster.Spec.GKE.AutopilotComputeClass = "Balanced"
		}
	}

	// EKS defaults
	if cluster.Spec.EKS != nil && cluster.Spec.EKS.Enabled {
		// Default EBS volume type to gp3 (best price/performance)
		if cluster.Spec.EKS.EBSVolumeType == "" {
			cluster.Spec.EKS.EBSVolumeType = "gp3"
		}
	}
}

// validateClusterConfiguration validates the cluster configuration (T025)
// This is a fallback validation for cases where webhook validation is disabled
func (r *AntflyClusterReconciler) validateClusterConfiguration(cluster *antflyv1.AntflyCluster) error {
	// Call the webhook validation methods as fallback
	return cluster.ValidateCreate()
}

// calculateBackoff calculates exponential backoff duration for validation failures (T027)
// Schedule: 1s, 2s, 4s, 8s, 16s, 32s, 60s (max)
func calculateBackoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	// Cap to avoid int overflow (1<<63 wraps negative).
	if attempt > 6 {
		return 60 * time.Second
	}
	delay := time.Duration(1<<attempt) * time.Second
	if delay > 60*time.Second {
		return 60 * time.Second
	}
	return delay
}

func (r *AntflyClusterReconciler) getValidationAttempts(key string) int {
	if val, ok := r.validationAttempts.Load(key); ok {
		return val.(int)
	}
	return 0
}

func (r *AntflyClusterReconciler) incrementValidationAttempts(key string) int {
	count := r.getValidationAttempts(key) + 1
	r.validationAttempts.Store(key, count)
	return count
}

func (r *AntflyClusterReconciler) resetValidationAttempts(key string) {
	r.validationAttempts.Delete(key)
}

// updateStatusWithValidationError updates the cluster status with validation error (T026).
// Skips the API call if the condition already reflects the same error.
func (r *AntflyClusterReconciler) updateStatusWithValidationError(ctx context.Context, cluster *antflyv1.AntflyCluster, validationErr error) error {
	log := log.FromContext(ctx)

	errMsg := validationErr.Error()

	// Skip update if condition already reflects the same error
	for _, existing := range cluster.Status.Conditions {
		if existing.Type == antflyv1.TypeConfigurationValid &&
			existing.Status == metav1.ConditionFalse &&
			existing.Message == errMsg {
			return nil
		}
	}

	condition := metav1.Condition{
		Type:               antflyv1.TypeConfigurationValid,
		Status:             metav1.ConditionFalse,
		Reason:             antflyv1.ReasonValidationFailed,
		Message:            errMsg,
		LastTransitionTime: metav1.Now(),
	}

	// Find and update or append the condition
	found := false
	for i, existing := range cluster.Status.Conditions {
		if existing.Type == antflyv1.TypeConfigurationValid {
			cluster.Status.Conditions[i] = condition
			found = true
			break
		}
	}
	if !found {
		cluster.Status.Conditions = append(cluster.Status.Conditions, condition)
	}

	if err := r.Status().Update(ctx, cluster); err != nil {
		log.Error(err, "Failed to update status with validation error")
		return err
	}

	r.Recorder.Eventf(cluster, nil, corev1.EventTypeWarning, antflyv1.ReasonValidationFailed, antflyv1.ReasonValidationFailed, "%s", errMsg)

	return nil
}

// updateStatusWithValidationSuccess updates the cluster status with successful validation (T026).
// Skips the API call if the condition is already True and ObservedGeneration is current.
func (r *AntflyClusterReconciler) updateStatusWithValidationSuccess(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	log := log.FromContext(ctx)

	// Skip update if already valid for this generation
	if cluster.Status.ObservedGeneration == cluster.Generation {
		for _, existing := range cluster.Status.Conditions {
			if existing.Type == antflyv1.TypeConfigurationValid &&
				existing.Status == metav1.ConditionTrue {
				return nil
			}
		}
	}

	condition := metav1.Condition{
		Type:               antflyv1.TypeConfigurationValid,
		Status:             metav1.ConditionTrue,
		Reason:             antflyv1.ReasonValidationPassed,
		Message:            "All validation rules passed",
		LastTransitionTime: metav1.Now(),
	}

	found := false
	for i, existing := range cluster.Status.Conditions {
		if existing.Type == antflyv1.TypeConfigurationValid {
			if existing.Status != metav1.ConditionTrue {
				cluster.Status.Conditions[i] = condition
			}
			found = true
			break
		}
	}
	if !found {
		cluster.Status.Conditions = append(cluster.Status.Conditions, condition)
	}

	cluster.Status.ObservedGeneration = cluster.Generation

	if err := r.Status().Update(ctx, cluster); err != nil {
		log.Error(err, "Failed to update status with validation success")
		return err
	}

	return nil
}

// hasSidecarInjected checks if a pod has sidecar containers injected
// by comparing the actual container count to the expected container count
func (r *AntflyClusterReconciler) hasSidecarInjected(pod *corev1.Pod, expectedContainers int) bool {
	return len(pod.Status.ContainerStatuses) > expectedContainers
}

// detectSidecarInjectionStatus scans all pods for a cluster and counts how many have sidecars
func (r *AntflyClusterReconciler) detectSidecarInjectionStatus(ctx context.Context, cluster *antflyv1.AntflyCluster) (int32, int32, error) {
	expectedContainers := 1 // Each pod should have 1 application container (antfly)

	// List all pods for this cluster
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(cluster.Namespace), client.MatchingLabels{
		"app.kubernetes.io/name":     "antfly-database",
		"app.kubernetes.io/instance": cluster.Name,
	}); err != nil {
		return 0, 0, fmt.Errorf("failed to list pods: %w", err)
	}

	var totalPods int32
	var podsWithSidecars int32

	for _, pod := range podList.Items {
		// Only count running or pending pods (ignore terminated/failed)
		if pod.Status.Phase != corev1.PodRunning && pod.Status.Phase != corev1.PodPending {
			continue
		}

		totalPods++
		if r.hasSidecarInjected(&pod, expectedContainers) {
			podsWithSidecars++
		}
	}

	return podsWithSidecars, totalPods, nil
}

// envFromCache caches configmap fetches for a single reconcile cycle,
// avoiding duplicate API calls when envFrom hashing references the same resources.
type envFromCache struct {
	client     client.Reader
	configMaps map[types.NamespacedName]*corev1.ConfigMap
	// notFound tracks keys that returned NotFound so we don't retry them.
	notFound map[types.NamespacedName]bool
}

func newEnvFromCache(c client.Reader) *envFromCache {
	return &envFromCache{
		client:     c,
		configMaps: make(map[types.NamespacedName]*corev1.ConfigMap),
		notFound:   make(map[types.NamespacedName]bool),
	}
}

func (c *envFromCache) getConfigMap(ctx context.Context, key types.NamespacedName) (*corev1.ConfigMap, error) {
	if cm, ok := c.configMaps[key]; ok {
		return cm, nil
	}
	if c.notFound[key] {
		return nil, errors.NewNotFound(corev1.Resource("configmaps"), key.Name)
	}
	cm := &corev1.ConfigMap{}
	if err := c.client.Get(ctx, key, cm); err != nil {
		if errors.IsNotFound(err) {
			c.notFound[key] = true
		}
		return nil, err
	}
	c.configMaps[key] = cm
	return cm, nil
}

// computeEnvFromHash computes a hash of envFrom references plus referenced ConfigMap data.
// Secret data is intentionally not read by the operator, so this ClusterRole does not need
// the dangerous secrets permission flagged by Snyk. Secret reference names are still hashed
// so changing which Secret is referenced rolls pods; rotating Secret contents should be
// handled by the workload or by updating an annotation/spec field.
func (r *AntflyClusterReconciler) computeEnvFromHash(ctx context.Context, cache *envFromCache, namespace string, envFrom []corev1.EnvFromSource) string {
	if len(envFrom) == 0 {
		return ""
	}

	h := sha256.New()

	for _, source := range envFrom {
		if source.SecretRef != nil {
			h.Write([]byte("secret:"))
			h.Write([]byte(namespace))
			h.Write([]byte("/"))
			h.Write([]byte(source.SecretRef.Name))
		}
		if source.ConfigMapRef != nil {
			key := types.NamespacedName{Name: source.ConfigMapRef.Name, Namespace: namespace}
			configMap, err := cache.getConfigMap(ctx, key)
			if err == nil {
				// Sort keys for deterministic hash
				keys := make([]string, 0, len(configMap.Data))
				for k := range configMap.Data {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				for _, k := range keys {
					h.Write([]byte(k))
					h.Write([]byte(configMap.Data[k]))
				}
			}
		}
	}

	return fmt.Sprintf("%x", h.Sum(nil))[:16]
}

// markEnvFromSecretsUnchecked records that Secret references are intentionally not
// read by the operator. Kubernetes will validate envFrom Secret existence when it
// creates pods, and avoiding direct Secret reads keeps the operator ClusterRole from
// carrying dangerous Secret permissions.
func (r *AntflyClusterReconciler) markEnvFromSecretsUnchecked(cluster *antflyv1.AntflyCluster) {
	r.setSecretsReadyCondition(cluster, metav1.ConditionUnknown, antflyv1.ReasonAllSecretsFound, "Secret references are delegated to Kubernetes and are not read by the operator")
}

// setSecretsReadyCondition updates the SecretsReady condition on the cluster status
func (r *AntflyClusterReconciler) setSecretsReadyCondition(cluster *antflyv1.AntflyCluster, status metav1.ConditionStatus, reason, message string) {
	condition := metav1.Condition{
		Type:               antflyv1.TypeSecretsReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	}

	// Find and update or append the condition
	found := false
	for i, existing := range cluster.Status.Conditions {
		if existing.Type == antflyv1.TypeSecretsReady {
			// Only update if status changed
			if existing.Status != status || existing.Reason != reason {
				cluster.Status.Conditions[i] = condition
			}
			found = true
			break
		}
	}
	if !found {
		cluster.Status.Conditions = append(cluster.Status.Conditions, condition)
	}
}

func (r *AntflyClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling AntflyCluster", "name", req.Name, "namespace", req.Namespace)

	// Fetch the AntflyCluster instance
	var antflyCluster antflyv1.AntflyCluster
	if err := r.Get(ctx, req.NamespacedName, &antflyCluster); err != nil {
		if errors.IsNotFound(err) {
			r.validationAttempts.Delete(req.String())
			log.Info("AntflyCluster resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get AntflyCluster")
		return ctrl.Result{}, err
	}

	// Handle deletion: if the cluster is being deleted and has our finalizer, clean up storage
	if !antflyCluster.DeletionTimestamp.IsZero() {
		r.validationAttempts.Delete(req.String())
		if controllerutil.ContainsFinalizer(&antflyCluster, antflyv1.FinalizerPVCCleanup) {
			// Only run PVC cleanup if the policy still requests deletion.
			// The finalizer is kept even when the policy changes back to Retain
			// (to avoid a race), so we check the current policy here.
			stillWantsDelete := antflyCluster.Spec.Storage.PVCRetentionPolicy != nil &&
				antflyCluster.Spec.Storage.PVCRetentionPolicy.WhenDeleted == antflyv1.PVCRetentionDelete
			if stillWantsDelete {
				result, err := r.cleanupStorageResources(ctx, &antflyCluster)
				if err != nil {
					return ctrl.Result{}, err
				}
				if result != nil {
					return *result, nil
				}
			}
			// Cleanup complete (or skipped) — remove the finalizer
			controllerutil.RemoveFinalizer(&antflyCluster, antflyv1.FinalizerPVCCleanup)
			if err := r.Update(ctx, &antflyCluster); err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to remove finalizer: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}

	// Ensure finalizer is present when WhenDeleted=Delete.
	// The finalizer is only removed inside the deletion handler (above) after
	// cleanup completes. Removing it here on policy change would race with
	// concurrent kubectl delete, defeating the cleanup guarantee.
	wantsDelete := antflyCluster.Spec.Storage.PVCRetentionPolicy != nil &&
		antflyCluster.Spec.Storage.PVCRetentionPolicy.WhenDeleted == antflyv1.PVCRetentionDelete
	if wantsDelete && !controllerutil.ContainsFinalizer(&antflyCluster, antflyv1.FinalizerPVCCleanup) {
		controllerutil.AddFinalizer(&antflyCluster, antflyv1.FinalizerPVCCleanup)
		if err := r.Update(ctx, &antflyCluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to add finalizer: %w", err)
		}
	}

	// Apply defaults to a working copy, not the original
	// this avoids an error from our caller `reconcileHandler` because of a version missmatch.
	workingCluster := antflyCluster.DeepCopy()
	topologyMode := effectiveTopologyMode(workingCluster)
	swarmMode := topologyMode == topologyModeSwarm
	if err := r.ensureTopologyResourcesMatchMode(ctx, &antflyCluster, topologyMode); err != nil {
		return ctrl.Result{}, err
	}

	// Apply default values for ports
	r.applyDefaults(workingCluster) // Use workingCluster for all processing, keep original cluster for status updates

	// Per-reconcile cache for ConfigMap lookups used by buildPodAnnotations.
	efCache := newEnvFromCache(r.Client)

	// Validate cluster configuration (T026)
	// Generation guard: skip validation if spec hasn't changed since last
	// successful validation (ObservedGeneration matches current Generation
	// and the ConfigurationValid condition is already True).
	needsValidation := antflyCluster.Status.ObservedGeneration != antflyCluster.Generation
	if !needsValidation {
		for _, c := range antflyCluster.Status.Conditions {
			if c.Type == antflyv1.TypeConfigurationValid && c.Status != metav1.ConditionTrue {
				needsValidation = true
				break
			}
		}
	}

	clusterKey := req.String()

	if needsValidation {
		if err := r.validateClusterConfiguration(workingCluster); err != nil {
			log.Error(err, "Cluster configuration validation failed")
			if statusErr := r.updateStatusWithValidationError(ctx, &antflyCluster, err); statusErr != nil {
				log.Error(statusErr, "Failed to update status with validation error")
			}
			attempt := r.incrementValidationAttempts(clusterKey)
			backoff := calculateBackoff(attempt - 1)
			return ctrl.Result{RequeueAfter: backoff}, nil
		}

		r.resetValidationAttempts(clusterKey)
		if err := r.updateStatusWithValidationSuccess(ctx, &antflyCluster); err != nil {
			log.Error(err, "Failed to update status with validation success")
			// Don't block reconciliation if status update fails
		}
	}

	// Do not read Secrets from the operator. Kubernetes validates referenced Secrets
	// during pod admission/startup, which avoids granting dangerous Secret RBAC here.
	r.markEnvFromSecretsUnchecked(workingCluster)

	if err := r.reconcileInferencePool(ctx, workingCluster); err != nil {
		return ctrl.Result{}, err
	}

	if swarmMode {
		// Swarm mode is a single topology and does not support clustered autoscaling.
		if workingCluster.Spec.DataNodes.AutoScaling != nil && workingCluster.Spec.DataNodes.AutoScaling.Enabled {
			log.Info("Ignoring data node autoscaling because swarm mode is enabled")
		}

		if err := r.reconcileConfigMap(ctx, workingCluster); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.reconcileServices(ctx, workingCluster); err != nil {
			return ctrl.Result{}, err
		}
		workingCluster.Spec.Storage.SwarmStorage = r.reconcileStorageAutoGrow(ctx, workingCluster, "swarm", "swarm-storage", workingCluster.Name+"-swarm", chooseSwarmStorageSize(workingCluster), maxSwarmAutoGrowSize(workingCluster))
		if err := r.reconcileSwarmStatefulSet(ctx, efCache, workingCluster); err != nil {
			return ctrl.Result{}, err
		}
		if repaired, err := r.repairBlockedStatefulSetRollouts(ctx, workingCluster); err != nil {
			return ctrl.Result{}, err
		} else if repaired {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}

		r.setPVCExpansionCondition(workingCluster, []pvcExpansionResult{
			r.reconcilePVCExpansion(ctx, workingCluster, "swarm", "swarm-storage", workingCluster.Name+"-swarm", chooseSwarmStorageSize(workingCluster)),
		})

		if err := r.reconcilePodDisruptionBudget(ctx, workingCluster, workingCluster.Name+"-swarm-pdb", "swarm"); err != nil {
			return ctrl.Result{}, err
		}

		if err := r.reconcileServiceMeshStatus(ctx, workingCluster); err != nil {
			return ctrl.Result{}, err
		}

		r.checkPVCTopologyHealth(ctx, workingCluster)

		if err := r.updateStatus(ctx, workingCluster); err != nil {
			return ctrl.Result{}, err
		}

		if requeueAfter := periodicRequeueAfter(workingCluster); requeueAfter > 0 {
			return ctrl.Result{RequeueAfter: requeueAfter}, nil
		}
		return ctrl.Result{}, nil
	}

	// Create ConfigMap for Antfly configuration
	if err := r.reconcileConfigMap(ctx, workingCluster); err != nil {
		return ctrl.Result{}, err
	}

	// Create Services
	if err := r.reconcileServices(ctx, workingCluster); err != nil {
		return ctrl.Result{}, err
	}

	// Create Metadata StatefulSet
	if err := r.reconcileMetadataStatefulSet(ctx, efCache, workingCluster); err != nil {
		return ctrl.Result{}, err
	}

	// Decide the data-node replica target before creating/updating the StatefulSet.
	// In manual mode, spec.dataNodes.replicas is authoritative. In autoscaling
	// mode, the operator autoscaler is authoritative and computes from observed
	// StatefulSet replicas, not the manual-mode spec field.
	dataSts := &appsv1.StatefulSet{}
	dataStsKey := types.NamespacedName{Name: workingCluster.Name + "-data", Namespace: workingCluster.Namespace}
	dataStsExists := true
	if err := r.Get(ctx, dataStsKey, dataSts); err != nil {
		if !errors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		dataStsExists = false
	}

	currentDataReplicas := effectiveDataReplicas(dataSts, dataStsExists, effectiveDataNodeReplicas(workingCluster))
	currentDataTarget := effectiveDataReplicaTarget(dataSts, dataStsExists, effectiveDataNodeReplicas(workingCluster))
	scaleSafetyReplicas := max(currentDataReplicas, currentDataTarget)
	desiredDataReplicas := effectiveDataNodeReplicas(workingCluster)
	requestedDataReplicas := desiredDataReplicas
	dataScaleDownSource := antflyv1.DataScaleDownSourceManual
	dataScaleDownRequested := false
	autoscalingEnabled := r.AutoScaler != nil && workingCluster.Spec.DataNodes.AutoScaling != nil && workingCluster.Spec.DataNodes.AutoScaling.Enabled
	dataNodesSuspended := workingCluster.Spec.DataNodes.Suspend
	if dataNodesSuspended {
		workingCluster.Status.AutoScalingStatus = nil
		requestedDataReplicas = 0
		desiredDataReplicas = 0
		workingCluster.Spec.DataNodes.Replicas = 0
		r.setScalingCondition(workingCluster, metav1.ConditionTrue, antflyv1.ReasonScalingReady, "Data nodes are suspended with PVCs retained")
	} else if autoscalingEnabled {
		dataScaleDownSource = antflyv1.DataScaleDownSourceAutoscaler
		recommendationReplicas, err := r.AutoScaler.EvaluateScaling(ctx, workingCluster, currentDataTarget)
		if err != nil {
			log.Error(err, "Failed to evaluate autoscaling")
			recommendationReplicas = currentDataTarget
		}

		requestedDataReplicas = recommendationReplicas
		desiredDataReplicas = recommendationReplicas
		blockedReason := ""
		blockedMessage := ""
		if recommendationReplicas < scaleSafetyReplicas {
			desiredDataReplicas = scaleSafetyReplicas
			dataScaleDownRequested = true
			blockedReason = antflyv1.ReasonDataScaleDownInProgress
			blockedMessage = fmt.Sprintf("Autoscaler recommended data-node scale-down from %d to %d; waiting for runtime drain before removing the next ordinal", scaleSafetyReplicas, recommendationReplicas)
		} else {
			r.setScalingCondition(workingCluster, metav1.ConditionTrue, antflyv1.ReasonScalingReady, "Scaling is not blocked")
		}

		workingCluster.Spec.DataNodes.Replicas = desiredDataReplicas
		r.AutoScaler.UpdateScalingStatus(workingCluster, currentDataReplicas, desiredDataReplicas, recommendationReplicas, blockedReason, blockedMessage)
		if recommendationReplicas != currentDataReplicas {
			log.Info("Autoscaling data nodes", "currentReplicas", currentDataReplicas, "desiredReplicas", desiredDataReplicas, "recommendationReplicas", recommendationReplicas, "blockedReason", blockedReason)
		}
	} else {
		workingCluster.Status.AutoScalingStatus = nil
		if desiredDataReplicas < scaleSafetyReplicas {
			message := fmt.Sprintf("Data-node scale-down from %d to %d requested; waiting for runtime drain before removing the next ordinal", scaleSafetyReplicas, desiredDataReplicas)
			requestedDataReplicas = desiredDataReplicas
			desiredDataReplicas = scaleSafetyReplicas
			dataScaleDownRequested = true
			workingCluster.Spec.DataNodes.Replicas = desiredDataReplicas
			r.setScalingCondition(workingCluster, metav1.ConditionUnknown, antflyv1.ReasonDataScaleDownInProgress, message)
		} else {
			r.setScalingCondition(workingCluster, metav1.ConditionTrue, antflyv1.ReasonScalingReady, "Scaling is not blocked")
		}
	}

	if dataStsExists && shouldFinalizeDataScaleDown(workingCluster.Status.DataScaleDownStatus, currentDataReplicas, desiredDataReplicas, dataScaleDownRequested) {
		scalingStatus := workingCluster.Status.DataScaleDownStatus
		if currentDataReplicas > scalingStatus.AppliedReplicas {
			message := fmt.Sprintf("Waiting for StatefulSet to remove data ordinal %d/node %s; current replicas=%d target replicas=%d", scalingStatus.DrainingOrdinal, scalingStatus.DrainingNodeID, currentDataReplicas, scalingStatus.AppliedReplicas)
			r.setDataScaleDownStatus(workingCluster, scalingStatus.Source, scalingStatus.FromReplicas, scalingStatus.TargetReplicas, scalingStatus.AppliedReplicas, scalingStatus.DrainingOrdinal, scalingStatus.DrainingNodeID, "Scaling", message)
			r.setScalingCondition(workingCluster, metav1.ConditionUnknown, antflyv1.ReasonDataScaleDownInProgress, message)
			if statusErr := r.updateStatus(ctx, workingCluster); statusErr != nil {
				log.Error(statusErr, "Failed to update status while waiting for data StatefulSet scale-down")
			}
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		if err := r.finalizeDataNodeShutdown(ctx, workingCluster, scalingStatus.DrainingNodeID); err != nil {
			failureMessage := fmt.Sprintf("Failed to finalize data-node shutdown for ordinal %d/node %s after StatefulSet scale-down: %v", scalingStatus.DrainingOrdinal, scalingStatus.DrainingNodeID, err)
			r.setDataScaleDownStatus(workingCluster, scalingStatus.Source, scalingStatus.FromReplicas, scalingStatus.TargetReplicas, scalingStatus.AppliedReplicas, scalingStatus.DrainingOrdinal, scalingStatus.DrainingNodeID, "Failed", failureMessage)
			r.setScalingCondition(workingCluster, metav1.ConditionFalse, antflyv1.ReasonDataScaleDownFailed, failureMessage)
			if statusErr := r.updateStatus(ctx, workingCluster); statusErr != nil {
				log.Error(statusErr, "Failed to update status with data scale-down finalization failure")
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		message := fmt.Sprintf("Finalized data-node shutdown for ordinal %d/node %s", scalingStatus.DrainingOrdinal, scalingStatus.DrainingNodeID)
		r.setDataScaleDownStatus(workingCluster, scalingStatus.Source, scalingStatus.FromReplicas, scalingStatus.TargetReplicas, scalingStatus.AppliedReplicas, scalingStatus.DrainingOrdinal, scalingStatus.DrainingNodeID, "Complete", message)
		if requestedDataReplicas < scaleSafetyReplicas {
			r.setScalingCondition(workingCluster, metav1.ConditionUnknown, antflyv1.ReasonDataScaleDownInProgress, "Continuing data-node scale-down after finalizing the previous ordinal")
		} else {
			r.setScalingCondition(workingCluster, metav1.ConditionTrue, antflyv1.ReasonScalingReady, "Scaling is not blocked")
		}
		if statusErr := r.updateStatus(ctx, workingCluster); statusErr != nil {
			log.Error(statusErr, "Failed to update status after data scale-down finalization")
		}
		return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
	}

	if dataStsExists && !dataScaleDownRequested && (shouldCancelDataScaleDown(workingCluster.Status.DataScaleDownStatus, currentDataReplicas, desiredDataReplicas) || (dataNodesSuspended && shouldCancelDataScaleDownForSuspend(workingCluster.Status.DataScaleDownStatus, currentDataReplicas))) {
		drainingStatus := workingCluster.Status.DataScaleDownStatus
		status, err := r.cancelDataNodeShutdown(ctx, workingCluster, drainingStatus.DrainingNodeID)
		if err != nil {
			failureMessage := fmt.Sprintf("Failed to cancel data-node shutdown for ordinal %d/node %s: %v", drainingStatus.DrainingOrdinal, drainingStatus.DrainingNodeID, err)
			r.setDataScaleDownStatus(workingCluster, drainingStatus.Source, drainingStatus.FromReplicas, desiredDataReplicas, scaleSafetyReplicas, drainingStatus.DrainingOrdinal, drainingStatus.DrainingNodeID, "Failed", failureMessage)
			r.setScalingCondition(workingCluster, metav1.ConditionFalse, antflyv1.ReasonDataScaleDownFailed, failureMessage)
			if statusErr := r.updateStatus(ctx, workingCluster); statusErr != nil {
				log.Error(statusErr, "Failed to update status with data scale-down cancellation failure")
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if status.Phase != "active" && status.Phase != "not_found" {
			message := fmt.Sprintf("Canceling data-node shutdown for ordinal %d/node %s; runtime phase is %q", drainingStatus.DrainingOrdinal, drainingStatus.DrainingNodeID, status.Phase)
			r.setDataScaleDownStatus(workingCluster, drainingStatus.Source, drainingStatus.FromReplicas, desiredDataReplicas, scaleSafetyReplicas, drainingStatus.DrainingOrdinal, drainingStatus.DrainingNodeID, "Canceling", message)
			r.setScalingCondition(workingCluster, metav1.ConditionUnknown, antflyv1.ReasonDataScaleDownInProgress, message)
			if statusErr := r.updateStatus(ctx, workingCluster); statusErr != nil {
				log.Error(statusErr, "Failed to update status with data scale-down cancellation progress")
			}
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		message := fmt.Sprintf("Canceled data-node shutdown for ordinal %d/node %s; runtime phase is %q", drainingStatus.DrainingOrdinal, drainingStatus.DrainingNodeID, status.Phase)
		r.setDataScaleDownStatus(workingCluster, drainingStatus.Source, drainingStatus.FromReplicas, desiredDataReplicas, scaleSafetyReplicas, drainingStatus.DrainingOrdinal, drainingStatus.DrainingNodeID, "Canceled", message)
		r.setScalingCondition(workingCluster, metav1.ConditionTrue, antflyv1.ReasonScalingReady, "Scaling is not blocked")
		if r.AutoScaler != nil && workingCluster.Status.AutoScalingStatus != nil {
			r.AutoScaler.UpdateScalingStatus(workingCluster, currentDataReplicas, desiredDataReplicas, requestedDataReplicas, "", "")
		}
		if statusErr := r.updateStatus(ctx, workingCluster); statusErr != nil {
			log.Error(statusErr, "Failed to update status with data scale-down cancellation")
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	if dataNodesSuspended {
		workingCluster.Status.DataScaleDownStatus = nil
	} else if dataStsExists && dataScaleDownRequested {
		drainingOrdinal := scaleSafetyReplicas - 1
		drainingNodeID := nodeIDForDataOrdinal(drainingOrdinal)
		nextReplicas := scaleSafetyReplicas - 1
		message := fmt.Sprintf("Draining data ordinal %d/node %s before scaling StatefulSet from %d to %d", drainingOrdinal, drainingNodeID, scaleSafetyReplicas, nextReplicas)
		r.setDataScaleDownStatus(workingCluster, dataScaleDownSource, scaleSafetyReplicas, requestedDataReplicas, scaleSafetyReplicas, drainingOrdinal, drainingNodeID, "Draining", message)
		r.setScalingCondition(workingCluster, metav1.ConditionUnknown, antflyv1.ReasonDataScaleDownInProgress, message)
		shutdownStatus, err := r.requestDataNodeShutdown(ctx, workingCluster, drainingNodeID)
		if err != nil {
			failureMessage := fmt.Sprintf("Failed to drain data ordinal %d/node %s: %v", drainingOrdinal, drainingNodeID, err)
			r.setDataScaleDownStatus(workingCluster, dataScaleDownSource, scaleSafetyReplicas, requestedDataReplicas, scaleSafetyReplicas, drainingOrdinal, drainingNodeID, "Failed", failureMessage)
			r.setScalingCondition(workingCluster, metav1.ConditionFalse, antflyv1.ReasonDataScaleDownFailed, failureMessage)
			if statusErr := r.Status().Update(ctx, workingCluster); statusErr != nil {
				log.Error(statusErr, "Failed to update status with data scale-down failure")
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if shutdownStatus.Blocked || shutdownStatus.Phase == "blocked" {
			blockedMessage := shutdownStatus.Message
			if blockedMessage == "" {
				blockedMessage = fmt.Sprintf("Data ordinal %d/node %s cannot be drained safely; runtime reports phase %q", drainingOrdinal, drainingNodeID, shutdownStatus.Phase)
			}
			r.setDataScaleDownStatus(workingCluster, dataScaleDownSource, scaleSafetyReplicas, requestedDataReplicas, scaleSafetyReplicas, drainingOrdinal, drainingNodeID, "Blocked", blockedMessage)
			r.setScalingCondition(workingCluster, metav1.ConditionFalse, antflyv1.ReasonDataScaleDownBlocked, blockedMessage)
			if r.AutoScaler != nil && workingCluster.Status.AutoScalingStatus != nil {
				r.AutoScaler.UpdateScalingStatus(workingCluster, currentDataReplicas, scaleSafetyReplicas, requestedDataReplicas, antflyv1.ReasonDataScaleDownBlocked, blockedMessage)
			}
			if statusErr := r.updateStatus(ctx, workingCluster); statusErr != nil {
				log.Error(statusErr, "Failed to update status with blocked data scale-down")
			}
			return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
		}
		if !shutdownStatus.SafeToTerminate {
			message = fmt.Sprintf("Data ordinal %d/node %s is draining in runtime phase %q; StatefulSet remains at %d replicas", drainingOrdinal, drainingNodeID, shutdownStatus.Phase, scaleSafetyReplicas)
			r.setDataScaleDownStatus(workingCluster, dataScaleDownSource, scaleSafetyReplicas, requestedDataReplicas, scaleSafetyReplicas, drainingOrdinal, drainingNodeID, "Draining", message)
			r.setScalingCondition(workingCluster, metav1.ConditionUnknown, antflyv1.ReasonDataScaleDownInProgress, message)
			if statusErr := r.updateStatus(ctx, workingCluster); statusErr != nil {
				log.Error(statusErr, "Failed to update status with data scale-down drain progress")
			}
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}

		desiredDataReplicas = nextReplicas
		workingCluster.Spec.DataNodes.Replicas = desiredDataReplicas
		message = fmt.Sprintf("Runtime reports data ordinal %d/node %s is safe to terminate; scaling StatefulSet from %d to %d", drainingOrdinal, drainingNodeID, scaleSafetyReplicas, desiredDataReplicas)
		r.setDataScaleDownStatus(workingCluster, dataScaleDownSource, scaleSafetyReplicas, requestedDataReplicas, desiredDataReplicas, drainingOrdinal, drainingNodeID, "Scaling", message)
		r.setScalingCondition(workingCluster, metav1.ConditionUnknown, antflyv1.ReasonDataScaleDownInProgress, message)
		if r.AutoScaler != nil && workingCluster.Status.AutoScalingStatus != nil {
			r.AutoScaler.UpdateScalingStatus(workingCluster, currentDataReplicas, desiredDataReplicas, requestedDataReplicas, antflyv1.ReasonDataScaleDownInProgress, message)
		}
	} else if dataStsExists {
		if workingCluster.Status.DataScaleDownStatus != nil {
			switch workingCluster.Status.DataScaleDownStatus.Phase {
			case "Complete", "Canceled":
				completedSource := workingCluster.Status.DataScaleDownStatus.Source
				if completedSource == "" {
					completedSource = antflyv1.DataScaleDownSourceManual
				}
				r.setDataScaleDownStatus(workingCluster, completedSource, scaleSafetyReplicas, scaleSafetyReplicas, desiredDataReplicas, 0, "", "Complete", "Data-node scale-down is complete")
			}
		}
	}

	workingCluster.Spec.Storage.DataStorage = r.reconcileStorageAutoGrow(ctx, workingCluster, "data", "data-storage", workingCluster.Name+"-data", effectiveDataStorageSize(workingCluster), maxDataAutoGrowSize(workingCluster))

	// Create Data StatefulSet
	if err := r.reconcileDataStatefulSet(ctx, efCache, workingCluster); err != nil {
		return ctrl.Result{}, err
	}

	if repaired, err := r.repairBlockedStatefulSetRollouts(ctx, workingCluster); err != nil {
		return ctrl.Result{}, err
	} else if repaired {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Reconcile PVC expansion (metadata and data)
	r.setPVCExpansionCondition(workingCluster, []pvcExpansionResult{
		r.reconcilePVCExpansion(ctx, workingCluster, "metadata", "metadata-storage", workingCluster.Name+"-metadata", workingCluster.Spec.Storage.MetadataStorage),
		r.reconcilePVCExpansion(ctx, workingCluster, "data", "data-storage", workingCluster.Name+"-data", workingCluster.Spec.Storage.DataStorage),
	})

	// Reconcile PodDisruptionBudgets for GKE
	if err := r.reconcilePodDisruptionBudget(ctx, workingCluster, workingCluster.Name+"-metadata-pdb", "metadata"); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcilePodDisruptionBudget(ctx, workingCluster, workingCluster.Name+"-data-pdb", "data"); err != nil {
		return ctrl.Result{}, err
	}

	// Detect service mesh sidecar injection status
	if err := r.reconcileServiceMeshStatus(ctx, workingCluster); err != nil {
		return ctrl.Result{}, err
	}

	// Check PVC/AZ topology health and set StorageHealthy condition
	r.checkPVCTopologyHealth(ctx, workingCluster)

	// Update status
	if err := r.updateStatus(ctx, workingCluster); err != nil {
		return ctrl.Result{}, err
	}

	if requeueAfter := periodicRequeueAfter(workingCluster); requeueAfter > 0 {
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}

	return ctrl.Result{}, nil
}

func periodicRequeueAfter(cluster *antflyv1.AntflyCluster) time.Duration {
	var requeueAfter time.Duration
	if haKubernetesLeaseRenewalEnabled(cluster) {
		requeueAfter = minPositiveDuration(requeueAfter, haFencingLeaseRenewalRequeueAfter())
	}
	if cluster.Spec.DataNodes.AutoScaling != nil && cluster.Spec.DataNodes.AutoScaling.Enabled {
		requeueAfter = minPositiveDuration(requeueAfter, 30*time.Second)
	}
	if storageAutoGrowEnabled(cluster) {
		requeueAfter = minPositiveDuration(requeueAfter, 60*time.Second)
	}
	return requeueAfter
}

func minPositiveDuration(current time.Duration, candidate time.Duration) time.Duration {
	if candidate <= 0 {
		return current
	}
	if current <= 0 || candidate < current {
		return candidate
	}
	return current
}

func (r *AntflyClusterReconciler) reconcileInferencePool(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	mode := antflyInferenceMode(cluster.Spec.Inference)
	if !r.ManageInferencePools {
		if mode == antflyv1.AntflyInferenceModeManaged {
			r.setInferencePoolReadyCondition(cluster, metav1.ConditionUnknown, antflyv1.ReasonInferencePoolManagementDisabled,
				"InferencePool management is disabled by --enable-inference-controllers=false; existing owned pools are left unchanged")
		}
		return nil
	}

	logger := log.FromContext(ctx)

	desired := map[string]struct{}{}
	if mode == antflyv1.AntflyInferenceModeManaged && cluster.Spec.Inference != nil {
		for i, managed := range cluster.Spec.Inference.ManagedPools {
			name := managedInferencePoolName(cluster, managed, len(cluster.Spec.Inference.ManagedPools), i)
			key := types.NamespacedName{Name: name, Namespace: cluster.Namespace}
			desired[name] = struct{}{}
			if err := r.reconcileManagedInferencePool(ctx, cluster, managed, name); err != nil {
				var conflictErr *inferencePoolNameConflictError
				if stderrors.As(err, &conflictErr) {
					logger.Info("Refusing to adopt same-name InferencePool because it is not controlled by this AntflyCluster", "inferencePool", key.String())
					r.setInferencePoolReadyCondition(cluster, metav1.ConditionFalse, antflyv1.ReasonInferencePoolNameConflict, conflictErr.Error())
					return nil
				}
				return err
			}
			if i == len(cluster.Spec.Inference.ManagedPools)-1 {
				r.setInferencePoolReadyCondition(cluster, metav1.ConditionTrue, antflyv1.ReasonInferencePoolReady,
					fmt.Sprintf("%d managed InferencePool(s) reconciled", len(cluster.Spec.Inference.ManagedPools)))
			}
		}
	}

	if err := r.deleteStaleOwnedInferencePools(ctx, cluster, desired); err != nil {
		return err
	}

	switch mode {
	case antflyv1.AntflyInferenceModeDisabled:
		r.setInferencePoolReadyCondition(cluster, metav1.ConditionTrue, antflyv1.ReasonInferencePoolReady, "Inference integration is disabled")
	case antflyv1.AntflyInferenceModeSharedRef:
		r.setInferencePoolReadyCondition(cluster, metav1.ConditionTrue, antflyv1.ReasonInferencePoolReady,
			fmt.Sprintf("%d shared InferencePool reference(s) configured", len(cluster.Spec.Inference.SharedPools)))
	case antflyv1.AntflyInferenceModePlatformShared:
		r.setInferencePoolReadyCondition(cluster, metav1.ConditionTrue, antflyv1.ReasonInferencePoolReady,
			fmt.Sprintf("%d platform InferencePool reference(s) configured", len(cluster.Spec.Inference.PlatformPools)))
	}
	return nil
}

func antflyInferenceMode(spec *antflyv1.AntflyInferenceSpec) antflyv1.AntflyInferenceMode {
	if spec == nil {
		return antflyv1.AntflyInferenceModeDisabled
	}
	if spec.Mode != "" {
		return spec.Mode
	}
	if len(spec.SharedPools) > 0 {
		return antflyv1.AntflyInferenceModeSharedRef
	}
	if len(spec.PlatformPools) > 0 {
		return antflyv1.AntflyInferenceModePlatformShared
	}
	return antflyv1.AntflyInferenceModeManaged
}

func managedInferencePoolName(cluster *antflyv1.AntflyCluster, managed antflyv1.ManagedInferencePoolSpec, total, index int) string {
	if managed.Name != "" {
		return managed.Name
	}
	if total == 1 {
		return cluster.Name + "-inference"
	}
	return fmt.Sprintf("%s-inference-%d", cluster.Name, index)
}

func (r *AntflyClusterReconciler) reconcileManagedInferencePool(
	ctx context.Context,
	cluster *antflyv1.AntflyCluster,
	managed antflyv1.ManagedInferencePoolSpec,
	name string,
) error {
	key := types.NamespacedName{Name: name, Namespace: cluster.Namespace}
	pool := &inferencev1alpha1.InferencePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pool, func() error {
		if pool.UID != "" && !metav1.IsControlledBy(pool, cluster) {
			return &inferencePoolNameConflictError{
				namespacedName: key.String(),
				cluster:        types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}.String(),
			}
		}

		if pool.Labels == nil {
			pool.Labels = map[string]string{}
		}
		pool.Labels["app.kubernetes.io/name"] = "inference"
		pool.Labels["app.kubernetes.io/component"] = "inferencepool"
		pool.Labels["app.kubernetes.io/instance"] = cluster.Name
		pool.Labels["app.kubernetes.io/managed-by"] = "antfly-operator"

		// AntflyCluster.spec.inference.managedPools is authoritative for managed pools.
		// Do not add InferencePool mutating-webhook defaults for spec fields
		// unless the same defaults are applied before this assignment.
		pool.Spec = *managed.Spec.DeepCopy()
		if pool.Spec.Image == "" {
			pool.Spec.Image = r.DefaultInferenceImage
		}
		if err := controllerutil.SetControllerReference(cluster, pool, r.Scheme); err != nil {
			return fmt.Errorf("failed to set owner reference on InferencePool %s: %w", name, err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to reconcile managed InferencePool %s: %w", name, err)
	}
	return nil
}

func (r *AntflyClusterReconciler) deleteStaleOwnedInferencePools(ctx context.Context, cluster *antflyv1.AntflyCluster, desired map[string]struct{}) error {
	defaultName := cluster.Name + "-inference"
	if _, ok := desired[defaultName]; !ok {
		pool := &inferencev1alpha1.InferencePool{}
		key := types.NamespacedName{Name: defaultName, Namespace: cluster.Namespace}
		if err := r.Get(ctx, key, pool); err != nil {
			if !errors.IsNotFound(err) {
				return fmt.Errorf("failed to get managed InferencePool %s: %w", key.String(), err)
			}
		} else if metav1.IsControlledBy(pool, cluster) {
			if err := client.IgnoreNotFound(r.Delete(ctx, pool)); err != nil {
				return fmt.Errorf("failed to delete stale managed InferencePool %s: %w", key.String(), err)
			}
		}
	}

	var pools inferencev1alpha1.InferencePoolList
	if err := r.List(ctx, &pools, client.InNamespace(cluster.Namespace), client.MatchingLabels{
		"app.kubernetes.io/instance":   cluster.Name,
		"app.kubernetes.io/managed-by": "antfly-operator",
	}); err != nil {
		return fmt.Errorf("failed to list managed InferencePools for %s/%s: %w", cluster.Namespace, cluster.Name, err)
	}
	for i := range pools.Items {
		pool := &pools.Items[i]
		if !metav1.IsControlledBy(pool, cluster) {
			continue
		}
		if _, ok := desired[pool.Name]; ok {
			continue
		}
		if err := client.IgnoreNotFound(r.Delete(ctx, pool)); err != nil {
			return fmt.Errorf("failed to delete stale managed InferencePool %s/%s: %w", pool.Namespace, pool.Name, err)
		}
	}
	return nil
}

type inferencePoolNameConflictError struct {
	namespacedName string
	cluster        string
}

func (e *inferencePoolNameConflictError) Error() string {
	return fmt.Sprintf("InferencePool %s already exists and is not controlled by AntflyCluster %s", e.namespacedName, e.cluster)
}

func (r *AntflyClusterReconciler) setInferencePoolReadyCondition(cluster *antflyv1.AntflyCluster, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               antflyv1.TypeInferencePoolReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: cluster.Generation,
	})
}

func (r *AntflyClusterReconciler) setScalingCondition(cluster *antflyv1.AntflyCluster, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               antflyv1.TypeScaling,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: cluster.Generation,
	})
}

func (r *AntflyClusterReconciler) setDataScaleDownStatus(cluster *antflyv1.AntflyCluster, source string, fromReplicas, targetReplicas, appliedReplicas, drainingOrdinal int32, drainingNodeID, phase, message string) {
	now := metav1.Now()
	if cluster.Status.DataScaleDownStatus != nil &&
		cluster.Status.DataScaleDownStatus.LastTransitionTime != nil &&
		cluster.Status.DataScaleDownStatus.Source == source &&
		cluster.Status.DataScaleDownStatus.Phase == phase &&
		cluster.Status.DataScaleDownStatus.DrainingOrdinal == drainingOrdinal &&
		cluster.Status.DataScaleDownStatus.TargetReplicas == targetReplicas {
		now = *cluster.Status.DataScaleDownStatus.LastTransitionTime
	}
	cluster.Status.DataScaleDownStatus = &antflyv1.DataScaleDownStatus{
		Source:             source,
		FromReplicas:       fromReplicas,
		TargetReplicas:     targetReplicas,
		AppliedReplicas:    appliedReplicas,
		DrainingOrdinal:    drainingOrdinal,
		DrainingNodeID:     drainingNodeID,
		Phase:              phase,
		Message:            message,
		LastTransitionTime: &now,
	}
}

func (r *AntflyClusterReconciler) reconcileConfigMap(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	// Generate complete configuration with metadata section
	completeConfig, err := r.generateCompleteConfig(cluster)
	if err != nil {
		return fmt.Errorf("failed to generate complete config: %w", err)
	}

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-config",
			Namespace: cluster.Namespace,
		},
	}

	// Use CreateOrUpdate to ensure ConfigMap is updated with latest configuration
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, configMap, func() error {
		// Set controller reference
		if err := controllerutil.SetControllerReference(cluster, configMap, r.Scheme); err != nil {
			return err
		}

		// Update ConfigMap data
		configMap.Data = map[string]string{
			"config.json": completeConfig,
		}

		return nil
	})

	return err
}

// generateCompleteConfig creates a complete Antfly configuration by merging user config with generated metadata network config
func (r *AntflyClusterReconciler) generateCompleteConfig(cluster *antflyv1.AntflyCluster) (string, error) {
	if effectiveTopologyMode(cluster) == topologyModeSwarm {
		return r.generateSwarmConfig(cluster)
	}

	return r.generateClusteredConfig(cluster)
}

func (r *AntflyClusterReconciler) generateClusteredConfig(cluster *antflyv1.AntflyCluster) (string, error) {
	// Parse user-provided configuration
	var userConfig map[string]any
	if err := json.Unmarshal([]byte(cluster.Spec.Config), &userConfig); err != nil {
		return "", fmt.Errorf("failed to parse user config: %w", err)
	}

	// Generate metadata orchestration URLs
	metadataReplicas := int32(3)
	if cluster.Spec.MetadataNodes.Replicas > 0 {
		metadataReplicas = cluster.Spec.MetadataNodes.Replicas
	}

	orchestrationURLs := make(map[string]string, metadataReplicas)
	for i := uint64(1); i <= uint64(max(metadataReplicas, 0)); i++ { //nolint:gosec // G115: metadataReplicas is a small positive Kubernetes replica count
		url := fmt.Sprintf("http://%s-metadata-%d.%s-metadata.%s.svc.cluster.local:%d",
			cluster.Name, i-1, cluster.Name, cluster.Namespace, cluster.Spec.MetadataNodes.MetadataAPI.Port)
		s := strconv.FormatUint(i, 16)
		orchestrationURLs[s] = url
	}

	// Build complete configuration structure
	completeConfig := map[string]any{
		"storage": map[string]any{
			"local": map[string]any{
				"base_dir": "/antflydb", // Must match PVC mount path
			},
		},
		"metadata": map[string]any{
			"orchestration_urls": orchestrationURLs,
		},
		"max_shard_size_bytes":     67108864, // Default 64MB
		"replication_factor":       3,        // Default
		"enable_auth":              false,    // Default
		"disable_shard_alloc":      true,     // Default
		"default_shards_per_table": uint64(1),
		"max_shards_per_table":     uint64(0),
	}

	// Merge user configuration on top of defaults
	maps.Copy(completeConfig, userConfig)

	// Ensure we don't override the generated network configuration
	completeConfig["metadata"] = map[string]any{
		"orchestration_urls": orchestrationURLs,
	}

	// Ensure storage base_dir matches the PVC mount path (cannot be overridden)
	// but preserve user's S3 configuration for backup/restore operations
	storageConfig := map[string]any{
		"local": map[string]any{
			"base_dir": "/antflydb",
		},
	}
	if userStorage, ok := userConfig["storage"].(map[string]any); ok {
		if s3Config, ok := userStorage["s3"]; ok {
			storageConfig["s3"] = s3Config
		}
	}
	completeConfig["storage"] = storageConfig

	// Convert back to JSON
	configBytes, err := json.MarshalIndent(completeConfig, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal complete config: %w", err)
	}

	return string(configBytes), nil
}

func (r *AntflyClusterReconciler) generateSwarmConfig(cluster *antflyv1.AntflyCluster) (string, error) {
	swarm := cluster.Spec.Swarm
	if swarm == nil {
		return "", fmt.Errorf("spec.swarm is required when spec.mode=Swarm")
	}
	inferenceEnabled := swarm.Inference == nil || swarm.Inference.Enabled
	inferenceAPIURL := "http://0.0.0.0:11433"
	if swarm.Inference != nil && swarm.Inference.APIURL != "" {
		inferenceAPIURL = swarm.Inference.APIURL
	}

	// Parse user-provided configuration
	var userConfig map[string]any
	if err := json.Unmarshal([]byte(cluster.Spec.Config), &userConfig); err != nil {
		return "", fmt.Errorf("failed to parse user config: %w", err)
	}

	orchestrationURLs := map[string]string{
		strconv.FormatInt(int64(swarm.NodeID), 16): fmt.Sprintf("http://%s-swarm.%s.svc.cluster.local:%d", cluster.Name, cluster.Namespace, swarm.MetadataAPI.Port),
	}

	completeConfig := map[string]any{
		"storage": map[string]any{
			"local": map[string]any{
				"base_dir": "/antflydb", // Must match PVC mount path
			},
		},
		"metadata": map[string]any{
			"orchestration_urls": orchestrationURLs,
		},
		"max_shard_size_bytes":     67108864, // Default 64MB
		"replication_factor":       uint64(1),
		"enable_auth":              false,
		"disable_shard_alloc":      true,
		"default_shards_per_table": uint64(1),
		"swarm_mode":               true,
	}

	maps.Copy(completeConfig, userConfig)

	completeConfig["metadata"] = map[string]any{
		"orchestration_urls": orchestrationURLs,
	}
	completeConfig["replication_factor"] = uint64(1)
	completeConfig["default_shards_per_table"] = uint64(1)
	completeConfig["disable_shard_alloc"] = true
	completeConfig["swarm_mode"] = true

	if inferenceEnabled {
		inferenceConfig := map[string]any{}
		if userInference, ok := userConfig["inference"].(map[string]any); ok {
			maps.Copy(inferenceConfig, userInference)
		}
		inferenceConfig["api_url"] = inferenceAPIURL
		completeConfig["inference"] = inferenceConfig
	}

	storageConfig := map[string]any{
		"local": map[string]any{
			"base_dir": "/antflydb",
		},
	}
	if userStorage, ok := userConfig["storage"].(map[string]any); ok {
		if s3Config, ok := userStorage["s3"]; ok {
			storageConfig["s3"] = s3Config
		}
	}
	completeConfig["storage"] = storageConfig

	configBytes, err := json.MarshalIndent(completeConfig, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal complete config: %w", err)
	}

	return string(configBytes), nil
}

func (r *AntflyClusterReconciler) reconcileServices(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	mode := effectiveTopologyMode(cluster)

	// Build list of services to reconcile
	serviceDefs := []*corev1.Service{}

	// Only add public API service if enabled
	publicAPIService := r.createPublicAPIService(cluster, mode == topologyModeSwarm)
	if publicAPIService != nil {
		serviceDefs = append(serviceDefs, publicAPIService)
	}

	if mode == topologyModeSwarm {
		serviceDefs = append(serviceDefs, r.createSwarmService(cluster))
	} else {
		serviceDefs = append(serviceDefs,
			r.createMetadataService(cluster),
			r.createDataService(cluster),
		)
	}

	// If public API is disabled, delete any existing public-api service
	if publicAPIService == nil {
		existingSvc := &corev1.Service{}
		publicAPISvcName := fmt.Sprintf("%s-public-api", cluster.Name)
		err := r.Get(ctx, types.NamespacedName{
			Name:      publicAPISvcName,
			Namespace: cluster.Namespace,
		}, existingSvc)
		if err == nil {
			log.FromContext(ctx).Info("Deleting public API service because publicAPI is disabled", "service", publicAPISvcName)
			if err := r.Delete(ctx, existingSvc); err != nil {
				return fmt.Errorf("failed to delete public API service %s: %w", publicAPISvcName, err)
			}
		} else if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to check for existing public API service %s: %w", publicAPISvcName, err)
		}
	}

	for _, serviceDef := range serviceDefs {
		service := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      serviceDef.Name,
				Namespace: serviceDef.Namespace,
			},
		}

		// Use CreateOrUpdate to ensure services are updated with latest configuration
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
			// Set controller reference
			if err := controllerutil.SetControllerReference(cluster, service, r.Scheme); err != nil {
				return err
			}

			// Update mutable service spec fields from the desired definition
			// Note: ClusterIP cannot be changed after creation (immutable)
			if service.Spec.ClusterIP == "" {
				// Only set ClusterIP on creation
				service.Spec.ClusterIP = serviceDef.Spec.ClusterIP
			}

			// Always sync Type to match desired state
			service.Spec.Type = serviceDef.Spec.Type

			service.Spec.PublishNotReadyAddresses = serviceDef.Spec.PublishNotReadyAddresses
			service.Spec.Selector = serviceDef.Spec.Selector
			if serviceDef.Name == cluster.Name+"-public-api" {
				syncHAPrimaryRouteServiceAnnotations(service, serviceDef.Annotations)
			}

			// Copy ports, handling NodePort specially
			service.Spec.Ports = make([]corev1.ServicePort, len(serviceDef.Spec.Ports))
			for i, port := range serviceDef.Spec.Ports {
				service.Spec.Ports[i] = port

				// Only preserve NodePort for NodePort and LoadBalancer service types
				// For ClusterIP, explicitly clear NodePort field
				if serviceDef.Spec.Type != corev1.ServiceTypeNodePort &&
					serviceDef.Spec.Type != corev1.ServiceTypeLoadBalancer {
					service.Spec.Ports[i].NodePort = 0
				}
			}

			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *AntflyClusterReconciler) createPublicAPIService(cluster *antflyv1.AntflyCluster, swarmMode bool) *corev1.Service {
	// Return nil if public API service is disabled
	if cluster.Spec.PublicAPI != nil && cluster.Spec.PublicAPI.Enabled != nil && !*cluster.Spec.PublicAPI.Enabled {
		return nil
	}

	// Get configuration with defaults already applied
	serviceType := corev1.ServiceTypeLoadBalancer
	if cluster.Spec.PublicAPI != nil && cluster.Spec.PublicAPI.ServiceType != nil {
		serviceType = *cluster.Spec.PublicAPI.ServiceType
	}

	port := int32(80)
	if cluster.Spec.PublicAPI != nil && cluster.Spec.PublicAPI.Port != 0 {
		port = cluster.Spec.PublicAPI.Port
	}

	targetPort := cluster.Spec.MetadataNodes.MetadataAPI.Port
	if swarmMode && cluster.Spec.Swarm != nil {
		targetPort = cluster.Spec.Swarm.MetadataAPI.Port
	}

	servicePort := corev1.ServicePort{
		Protocol:   corev1.ProtocolTCP,
		Port:       port,
		TargetPort: intstr.FromInt(int(targetPort)),
	}

	// Only set NodePort if service type is NodePort and a specific port is configured
	if serviceType == corev1.ServiceTypeNodePort &&
		cluster.Spec.PublicAPI != nil &&
		cluster.Spec.PublicAPI.NodePort != nil {
		servicePort.NodePort = *cluster.Spec.PublicAPI.NodePort
	}

	selector := serviceSelectorLabels(cluster.Name, func() string {
		if swarmMode {
			return "swarm"
		}
		return "metadata"
	}())
	var annotations map[string]string
	if haPrimaryRouteManaged(cluster) {
		target := haCurrentPrimaryRouteTarget(cluster)
		routeSelectorApplied := false
		if routeSelector, ok := haPublicAPISelector(cluster, swarmMode, target); ok {
			selector = routeSelector
			routeSelectorApplied = true
		}
		annotations = haPrimaryRouteServiceAnnotations(cluster, target, routeSelectorApplied)
	} else if routeSelector, ok := haPublicAPISelector(cluster, swarmMode, "primary"); ok {
		selector = routeSelector
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        cluster.Name + "-public-api",
			Namespace:   cluster.Namespace,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Type:     serviceType,
			Selector: selector,
			Ports:    []corev1.ServicePort{servicePort},
		},
	}
}

func (r *AntflyClusterReconciler) createMetadataService(cluster *antflyv1.AntflyCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-metadata",
			Namespace: cluster.Namespace,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                "None",
			PublishNotReadyAddresses: true,
			Selector:                 serviceSelectorLabels(cluster.Name, "metadata"),
			Ports: []corev1.ServicePort{
				{
					Name:       "metadata-api",
					Port:       cluster.Spec.MetadataNodes.MetadataAPI.Port,
					TargetPort: intstr.FromInt(int(cluster.Spec.MetadataNodes.MetadataAPI.Port)),
				},
				{
					Name:       "metadata-raft",
					Port:       cluster.Spec.MetadataNodes.MetadataRaft.Port,
					TargetPort: intstr.FromInt(int(cluster.Spec.MetadataNodes.MetadataRaft.Port)),
				},
			},
		},
	}
}

func (r *AntflyClusterReconciler) createDataService(cluster *antflyv1.AntflyCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-data",
			Namespace: cluster.Namespace,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                "None",
			PublishNotReadyAddresses: true,
			Selector:                 serviceSelectorLabels(cluster.Name, "data"),
			Ports: []corev1.ServicePort{
				{
					Name:       "data-api",
					Port:       cluster.Spec.DataNodes.API.Port,
					TargetPort: intstr.FromInt(int(cluster.Spec.DataNodes.API.Port)),
				},
				{
					Name:       "data-raft",
					Port:       cluster.Spec.DataNodes.Raft.Port,
					TargetPort: intstr.FromInt(int(cluster.Spec.DataNodes.Raft.Port)),
				},
			},
		},
	}
}

func (r *AntflyClusterReconciler) createSwarmService(cluster *antflyv1.AntflyCluster) *corev1.Service {
	swarm := cluster.Spec.Swarm
	if swarm == nil {
		swarm = &antflyv1.SwarmSpec{}
	}

	healthPort := swarm.Health.Port
	if healthPort == 0 {
		healthPort = 4200
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-swarm",
			Namespace: cluster.Namespace,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                "None",
			PublishNotReadyAddresses: true,
			Selector:                 serviceSelectorLabels(cluster.Name, "swarm"),
			Ports: []corev1.ServicePort{
				{
					Name:       "metadata-api",
					Port:       swarm.MetadataAPI.Port,
					TargetPort: intstr.FromInt(int(swarm.MetadataAPI.Port)),
				},
				{
					Name:       "metadata-raft",
					Port:       swarm.MetadataRaft.Port,
					TargetPort: intstr.FromInt(int(swarm.MetadataRaft.Port)),
				},
				{
					Name:       "store-api",
					Port:       swarm.StoreAPI.Port,
					TargetPort: intstr.FromInt(int(swarm.StoreAPI.Port)),
				},
				{
					Name:       "store-raft",
					Port:       swarm.StoreRaft.Port,
					TargetPort: intstr.FromInt(int(swarm.StoreRaft.Port)),
				},
				{
					Name:       "health",
					Port:       healthPort,
					TargetPort: intstr.FromInt(int(healthPort)),
				},
			},
		},
	}
}

func (r *AntflyClusterReconciler) reconcileSwarmStatefulSet(ctx context.Context, cache *envFromCache, cluster *antflyv1.AntflyCluster) error {
	swarm := cluster.Spec.Swarm
	if swarm == nil {
		return fmt.Errorf("spec.swarm is required when spec.mode=Swarm")
	}
	replicas := swarm.Replicas
	if replicas == 0 {
		replicas = 1
	}
	storageSize := chooseSwarmStorageSize(cluster)

	var storageClassName *string
	if cluster.Spec.Storage.StorageClass != "" {
		storageClassName = &cluster.Spec.Storage.StorageClass
	}

	envFromSources := append([]corev1.EnvFromSource{}, swarm.EnvFrom...)

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-swarm",
			Namespace: cluster.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName:         cluster.Name + "-swarm",
			Replicas:            &replicas,
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector: &metav1.LabelSelector{
				MatchLabels: serviceSelectorLabels(cluster.Name, "swarm"),
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "swarm-storage",
						Labels: serviceSelectorLabels(cluster.Name, "swarm"),
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						StorageClassName: storageClassName,
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse(storageSize),
							},
						},
					},
				},
			},
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, statefulSet, func() error {
		if err := controllerutil.SetControllerReference(cluster, statefulSet, r.Scheme); err != nil {
			return err
		}

		statefulSet.Spec.Replicas = &replicas
		statefulSet.Spec.PersistentVolumeClaimRetentionPolicy = buildPVCRetentionPolicy(cluster.Spec.Storage.PVCRetentionPolicy)
		statefulSet.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      podLabels(cluster, "swarm"),
				Annotations: r.buildPodAnnotations(ctx, cache, cluster, envFromSources),
			},
			Spec: corev1.PodSpec{
				ServiceAccountName: cluster.Spec.ServiceAccountName,
				SecurityContext:    antflyPodSecurityContext(),
				InitContainers: []corev1.Container{
					r.buildStorageInitContainer("swarm-storage"),
				},
				Containers: []corev1.Container{
					{
						Name:            "antfly",
						Image:           cluster.Spec.Image,
						ImagePullPolicy: corev1.PullPolicy(cluster.Spec.ImagePullPolicy),
						EnvFrom:         envFromSources,
						Env:             secretStoreEnv(cluster.Spec.SecretStore),
						Ports: []corev1.ContainerPort{
							{
								Name:          "metadata-api",
								ContainerPort: swarm.MetadataAPI.Port,
								Protocol:      corev1.ProtocolTCP,
							},
							{
								Name:          "metadata-raft",
								ContainerPort: swarm.MetadataRaft.Port,
								Protocol:      corev1.ProtocolTCP,
							},
							{
								Name:          "store-api",
								ContainerPort: swarm.StoreAPI.Port,
								Protocol:      corev1.ProtocolTCP,
							},
							{
								Name:          "store-raft",
								ContainerPort: swarm.StoreRaft.Port,
								Protocol:      corev1.ProtocolTCP,
							},
							{
								Name:          "health",
								ContainerPort: swarm.Health.Port,
								Protocol:      corev1.ProtocolTCP,
							},
						},
						VolumeMounts: append([]corev1.VolumeMount{
							{
								Name:      "swarm-storage",
								MountPath: "/antflydb",
							},
							{
								Name:      "config",
								MountPath: "/config",
							},
						}, secretStoreVolumeMounts(cluster.Spec.SecretStore)...),
						Command: []string{"/bin/sh", "-c"},
						Args: []string{
							fmt.Sprintf(`
exec /antfly swarm --id %d --config /config/config.json \
  --host 0.0.0.0 \
  --port %d \
  --health-port %d%s
							`,
								swarm.NodeID,
								swarm.MetadataAPI.Port,
								swarm.Health.Port,
								secretStoreArg(cluster.Spec.SecretStore),
							),
						},
						Resources: r.buildResourceRequirements(swarm.Resources),
						StartupProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/healthz",
									Port: intstr.FromInt(int(swarm.Health.Port)),
								},
							},
							InitialDelaySeconds: 30,
							PeriodSeconds:       10,
							FailureThreshold:    30,
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/healthz",
									Port: intstr.FromInt(int(swarm.Health.Port)),
								},
							},
							PeriodSeconds:    15,
							FailureThreshold: 3,
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/readyz",
									Port: intstr.FromInt(int(swarm.Health.Port)),
								},
							},
							PeriodSeconds:    5,
							FailureThreshold: 5,
						},
					},
				},
				Volumes: append([]corev1.Volume{
					{
						Name: "config",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: cluster.Name + "-config",
								},
							},
						},
					},
				}, secretStoreVolumes(cluster.Spec.SecretStore)...),
			},
		}

		applySchedulingConstraints(&statefulSet.Spec.Template,
			swarm.Tolerations,
			swarm.NodeSelector,
			swarm.Affinity,
			swarm.TopologySpreadConstraints)

		r.applyGKEPodSpec(&statefulSet.Spec.Template, cluster, false)
		r.applyEKSPodSpec(&statefulSet.Spec.Template, cluster, false)

		isGKEAutopilot := cluster.Spec.GKE != nil && cluster.Spec.GKE.Autopilot
		applyDefaultZoneTopologySpread(statefulSet, &statefulSet.Spec.Template, "swarm", cluster.Name,
			swarm.TopologySpreadConstraints, isGKEAutopilot)

		return nil
	})

	return err
}

// buildPodAnnotations returns the complete annotations for pod templates including:
// - Service mesh annotations
// - EnvFrom hash annotation for secret rotation detection
func (r *AntflyClusterReconciler) buildPodAnnotations(ctx context.Context, cache *envFromCache, cluster *antflyv1.AntflyCluster, envFrom []corev1.EnvFromSource) map[string]string {
	annotations := make(map[string]string)

	// Add service mesh annotations
	if cluster.Spec.ServiceMesh != nil && cluster.Spec.ServiceMesh.Enabled {
		maps.Copy(annotations, cluster.Spec.ServiceMesh.Annotations)
	}

	// Add envFrom hash annotation if there are envFrom sources
	if len(envFrom) > 0 {
		hash := r.computeEnvFromHash(ctx, cache, cluster.Namespace, envFrom)
		if hash != "" {
			annotations["antfly.io/envfrom-hash"] = hash
		}
	}

	// Return nil if no annotations to avoid creating empty map
	if len(annotations) == 0 {
		return nil
	}

	return annotations
}

func (r *AntflyClusterReconciler) reconcileMetadataStatefulSet(ctx context.Context, cache *envFromCache, cluster *antflyv1.AntflyCluster) error {
	replicas := int32(3)
	if cluster.Spec.MetadataNodes.Replicas > 0 {
		replicas = cluster.Spec.MetadataNodes.Replicas
	}

	storageSize := "500Mi"
	if cluster.Spec.Storage.MetadataStorage != "" {
		storageSize = cluster.Spec.Storage.MetadataStorage
	}

	// Get storage class pointer - nil means use cluster default
	var storageClassName *string
	if cluster.Spec.Storage.StorageClass != "" {
		storageClassName = &cluster.Spec.Storage.StorageClass
	}

	// Build metadata cluster configuration
	metadataCluster := r.buildMetadataClusterConfig(cluster, replicas)

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-metadata",
			Namespace: cluster.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName:         cluster.Name + "-metadata",
			Replicas:            &replicas,
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector: &metav1.LabelSelector{
				MatchLabels: serviceSelectorLabels(cluster.Name, "metadata"),
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "metadata-storage",
						Labels: serviceSelectorLabels(cluster.Name, "metadata"),
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						StorageClassName: storageClassName,
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse(storageSize),
							},
						},
					},
				},
			},
			// Template is populated in the CreateOrUpdate callback below
		},
	}

	// Use CreateOrUpdate to properly handle all spec changes (image, resources, etc.)
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, statefulSet, func() error {
		// Set controller reference
		if err := controllerutil.SetControllerReference(cluster, statefulSet, r.Scheme); err != nil {
			return err
		}

		// Update mutable fields
		// Note: VolumeClaimTemplates cannot be updated after creation
		statefulSet.Spec.Replicas = &replicas
		statefulSet.Spec.PersistentVolumeClaimRetentionPolicy = buildPVCRetentionPolicy(cluster.Spec.Storage.PVCRetentionPolicy)
		statefulSet.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      podLabels(cluster, "metadata"),
				Annotations: r.buildPodAnnotations(ctx, cache, cluster, cluster.Spec.MetadataNodes.EnvFrom),
			},
			Spec: corev1.PodSpec{
				ServiceAccountName: cluster.Spec.ServiceAccountName,
				SecurityContext:    antflyPodSecurityContext(),
				InitContainers: []corev1.Container{
					r.buildStorageInitContainer("metadata-storage"),
				},
				Containers: []corev1.Container{
					{
						Name:            "antfly",
						Image:           cluster.Spec.Image,
						ImagePullPolicy: corev1.PullPolicy(cluster.Spec.ImagePullPolicy),
						EnvFrom:         cluster.Spec.MetadataNodes.EnvFrom,
						Env:             secretStoreEnv(cluster.Spec.SecretStore),
						Ports: []corev1.ContainerPort{
							{
								Name:          "metadata-api",
								ContainerPort: cluster.Spec.MetadataNodes.MetadataAPI.Port,
								Protocol:      corev1.ProtocolTCP,
							},
							{
								Name:          "metadata-raft",
								ContainerPort: cluster.Spec.MetadataNodes.MetadataRaft.Port,
								Protocol:      corev1.ProtocolTCP,
							},
							{
								Name:          "health",
								ContainerPort: cluster.Spec.MetadataNodes.Health.Port,
								Protocol:      corev1.ProtocolTCP,
							},
						},
						VolumeMounts: append([]corev1.VolumeMount{
							{
								Name:      "metadata-storage",
								MountPath: "/antflydb",
							},
							{
								Name:      "config",
								MountPath: "/config",
							},
						}, secretStoreVolumeMounts(cluster.Spec.SecretStore)...),
						Command: []string{"/bin/sh", "-c"},
						Args: []string{
							fmt.Sprintf(`
ORDINAL=${HOSTNAME##*-}
ID=$((ORDINAL + 1))
exec /antfly metadata --id $ID --config /config/config.json \
  --api-host 0.0.0.0 \
  --api-port %d \
  --raft-host 0.0.0.0 \
  --raft-port %d \
  --health-port %d \
  --cluster '%s'%s
								`,
								cluster.Spec.MetadataNodes.MetadataAPI.Port,
								cluster.Spec.MetadataNodes.MetadataRaft.Port,
								cluster.Spec.MetadataNodes.Health.Port,
								metadataCluster,
								secretStoreArg(cluster.Spec.SecretStore),
							),
						},
						Resources: r.buildResourceRequirements(cluster.Spec.MetadataNodes.Resources),
						StartupProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/healthz",
									Port: intstr.FromInt(int(cluster.Spec.MetadataNodes.Health.Port)),
								},
							},
							InitialDelaySeconds: 30,
							PeriodSeconds:       10,
							FailureThreshold:    30,
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/healthz",
									Port: intstr.FromInt(int(cluster.Spec.MetadataNodes.Health.Port)),
								},
							},
							PeriodSeconds:    15,
							FailureThreshold: 3,
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/readyz",
									Port: intstr.FromInt(int(cluster.Spec.MetadataNodes.Health.Port)),
								},
							},
							PeriodSeconds:    5,
							FailureThreshold: 5,
						},
					},
				},
				Volumes: append([]corev1.Volume{
					{
						Name: "config",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: cluster.Name + "-config",
								},
							},
						},
					},
				}, secretStoreVolumes(cluster.Spec.SecretStore)...),
			},
		}

		// Apply user-specified scheduling constraints first
		applySchedulingConstraints(&statefulSet.Spec.Template,
			cluster.Spec.MetadataNodes.Tolerations,
			cluster.Spec.MetadataNodes.NodeSelector,
			cluster.Spec.MetadataNodes.Affinity,
			cluster.Spec.MetadataNodes.TopologySpreadConstraints)

		// Apply GKE-specific configurations
		r.applyGKEPodSpec(&statefulSet.Spec.Template, cluster, cluster.Spec.MetadataNodes.UseSpotPods)

		// Apply EKS-specific configurations
		r.applyEKSPodSpec(&statefulSet.Spec.Template, cluster, false) // Spot not recommended for metadata nodes

		// Apply default zone topology spread if user hasn't specified explicit constraints
		isGKEAutopilot := cluster.Spec.GKE != nil && cluster.Spec.GKE.Autopilot
		applyDefaultZoneTopologySpread(statefulSet, &statefulSet.Spec.Template, "metadata", cluster.Name,
			cluster.Spec.MetadataNodes.TopologySpreadConstraints, isGKEAutopilot)

		return nil
	})

	return err
}

func (r *AntflyClusterReconciler) buildMetadataClusterConfig(cluster *antflyv1.AntflyCluster, replicas int32) string {
	var config strings.Builder
	config.WriteString("{ ")
	for i := int32(1); i <= replicas; i++ {
		if i > 1 {
			config.WriteString(", ")
		}
		fmt.Fprintf(&config, `"%d": "http://%s-metadata-%d.%s-metadata.%s.svc.cluster.local:%d"`,
			i, cluster.Name, i-1, cluster.Name, cluster.Namespace, cluster.Spec.MetadataNodes.MetadataRaft.Port)
	}
	config.WriteString(" }")
	return config.String()
}

func (r *AntflyClusterReconciler) reconcileDataStatefulSet(ctx context.Context, cache *envFromCache, cluster *antflyv1.AntflyCluster) error {
	replicas := effectiveDataNodeReplicas(cluster)

	storageSize := "1Gi"
	if cluster.Spec.Storage.DataStorage != "" {
		storageSize = cluster.Spec.Storage.DataStorage
	}

	// Get storage class pointer - nil means use cluster default
	var storageClassName *string
	if cluster.Spec.Storage.StorageClass != "" {
		storageClassName = &cluster.Spec.Storage.StorageClass
	}

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-data",
			Namespace: cluster.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName:         cluster.Name + "-data",
			Replicas:            &replicas,
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector: &metav1.LabelSelector{
				MatchLabels: serviceSelectorLabels(cluster.Name, "data"),
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "data-storage",
						Labels: serviceSelectorLabels(cluster.Name, "data"),
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						StorageClassName: storageClassName,
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse(storageSize),
							},
						},
					},
				},
			},
			// Template is populated in the CreateOrUpdate callback below
		},
	}

	// Determine if EKS Spot should be used for data nodes (safe with 3+ replicas)
	useEKSSpot := cluster.Spec.EKS != nil && cluster.Spec.EKS.UseSpotInstances

	// Use CreateOrUpdate to properly handle all spec changes (image, resources, etc.)
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, statefulSet, func() error {
		// Set controller reference
		if err := controllerutil.SetControllerReference(cluster, statefulSet, r.Scheme); err != nil {
			return err
		}

		// Update mutable fields
		// Note: VolumeClaimTemplates cannot be updated after creation
		statefulSet.Spec.Replicas = &replicas
		statefulSet.Spec.PersistentVolumeClaimRetentionPolicy = buildPVCRetentionPolicy(cluster.Spec.Storage.PVCRetentionPolicy)
		statefulSet.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      podLabels(cluster, "data"),
				Annotations: r.buildPodAnnotations(ctx, cache, cluster, cluster.Spec.DataNodes.EnvFrom),
			},
			Spec: corev1.PodSpec{
				ServiceAccountName: cluster.Spec.ServiceAccountName,
				SecurityContext:    antflyPodSecurityContext(),
				InitContainers: []corev1.Container{
					r.buildStorageInitContainer("data-storage"),
				},
				Containers: []corev1.Container{
					{
						Name:            "antfly",
						Image:           cluster.Spec.Image,
						ImagePullPolicy: corev1.PullPolicy(cluster.Spec.ImagePullPolicy),
						EnvFrom:         cluster.Spec.DataNodes.EnvFrom,
						Ports: []corev1.ContainerPort{
							{
								Name:          "data-api",
								ContainerPort: cluster.Spec.DataNodes.API.Port,
								Protocol:      corev1.ProtocolTCP,
							},
							{
								Name:          "data-raft",
								ContainerPort: cluster.Spec.DataNodes.Raft.Port,
								Protocol:      corev1.ProtocolTCP,
							},
							{
								Name:          "health",
								ContainerPort: cluster.Spec.DataNodes.Health.Port,
								Protocol:      corev1.ProtocolTCP,
							},
						},
						VolumeMounts: append([]corev1.VolumeMount{
							{
								Name:      "data-storage",
								MountPath: "/antflydb",
							},
							{
								Name:      "config",
								MountPath: "/config",
							},
						}, secretStoreVolumeMounts(cluster.Spec.SecretStore)...),
						Env: append([]corev1.EnvVar{
							{
								Name: "POD_IP",
								ValueFrom: &corev1.EnvVarSource{
									FieldRef: &corev1.ObjectFieldSelector{
										FieldPath: "status.podIP",
									},
								},
							},
						}, secretStoreEnv(cluster.Spec.SecretStore)...),
						Command: []string{"/bin/sh", "-c"},
						Args: []string{
							fmt.Sprintf(`
ORDINAL=${HOSTNAME##*-}
ID=$((ORDINAL + 1))
exec /antfly data --node-id $ID --store-id $ID --config /config/config.json \
  --api-host ${POD_IP} \
  --api-port %d \
  --raft-host ${POD_IP} \
  --raft-port %d \
  --health-port %d%s
								`,
								cluster.Spec.DataNodes.API.Port,
								cluster.Spec.DataNodes.Raft.Port,
								cluster.Spec.DataNodes.Health.Port,
								secretStoreArg(cluster.Spec.SecretStore),
							),
						},
						Resources: r.buildResourceRequirements(cluster.Spec.DataNodes.Resources),
						StartupProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/healthz",
									Port: intstr.FromInt(int(cluster.Spec.DataNodes.Health.Port)),
								},
							},
							InitialDelaySeconds: 30,
							PeriodSeconds:       10,
							FailureThreshold:    30,
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/healthz",
									Port: intstr.FromInt(int(cluster.Spec.DataNodes.Health.Port)),
								},
							},
							PeriodSeconds:    15,
							FailureThreshold: 3,
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/readyz",
									Port: intstr.FromInt(int(cluster.Spec.DataNodes.Health.Port)),
								},
							},
							PeriodSeconds:    5,
							FailureThreshold: 5,
						},
					},
				},
				Volumes: append([]corev1.Volume{
					{
						Name: "config",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: cluster.Name + "-config",
								},
							},
						},
					},
				}, secretStoreVolumes(cluster.Spec.SecretStore)...),
			},
		}

		// Apply user-specified scheduling constraints first
		applySchedulingConstraints(&statefulSet.Spec.Template,
			cluster.Spec.DataNodes.Tolerations,
			cluster.Spec.DataNodes.NodeSelector,
			cluster.Spec.DataNodes.Affinity,
			cluster.Spec.DataNodes.TopologySpreadConstraints)

		// Apply GKE-specific configurations
		r.applyGKEPodSpec(&statefulSet.Spec.Template, cluster, cluster.Spec.DataNodes.UseSpotPods)

		// Apply EKS-specific configurations
		r.applyEKSPodSpec(&statefulSet.Spec.Template, cluster, useEKSSpot)

		// Apply default zone topology spread if user hasn't specified explicit constraints
		isGKEAutopilot := cluster.Spec.GKE != nil && cluster.Spec.GKE.Autopilot
		applyDefaultZoneTopologySpread(statefulSet, &statefulSet.Spec.Template, "data", cluster.Name,
			cluster.Spec.DataNodes.TopologySpreadConstraints, isGKEAutopilot)

		return nil
	})

	return err
}

func (r *AntflyClusterReconciler) buildResourceRequirements(resourceSpec antflyv1.ResourceSpec) corev1.ResourceRequirements {
	requirements := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{},
		Limits:   corev1.ResourceList{},
	}

	if resourceSpec.CPU != "" {
		requirements.Requests[corev1.ResourceCPU] = resource.MustParse(resourceSpec.CPU)
	}
	if resourceSpec.Memory != "" {
		requirements.Requests[corev1.ResourceMemory] = resource.MustParse(resourceSpec.Memory)
	}
	if resourceSpec.Limits.CPU != "" {
		requirements.Limits[corev1.ResourceCPU] = resource.MustParse(resourceSpec.Limits.CPU)
	}
	if resourceSpec.Limits.Memory != "" {
		requirements.Limits[corev1.ResourceMemory] = resource.MustParse(resourceSpec.Limits.Memory)
	}
	if resourceSpec.Limits.GPU != "" {
		requirements.Limits[corev1.ResourceName("nvidia.com/gpu")] = resource.MustParse(resourceSpec.Limits.GPU)
	}

	return requirements
}

func chooseSwarmStorageSize(cluster *antflyv1.AntflyCluster) string {
	if cluster.Spec.Storage.SwarmStorage != "" {
		return cluster.Spec.Storage.SwarmStorage
	}
	return "1Gi"
}

func effectiveDataStorageSize(cluster *antflyv1.AntflyCluster) string {
	if cluster.Spec.Storage.DataStorage != "" {
		return cluster.Spec.Storage.DataStorage
	}
	return "1Gi"
}

func maxDataAutoGrowSize(cluster *antflyv1.AntflyCluster) string {
	if cluster.Spec.Storage.StorageAutoGrow == nil {
		return ""
	}
	return cluster.Spec.Storage.StorageAutoGrow.MaxDataStorage
}

func maxSwarmAutoGrowSize(cluster *antflyv1.AntflyCluster) string {
	if cluster.Spec.Storage.StorageAutoGrow == nil {
		return ""
	}
	if cluster.Spec.Storage.StorageAutoGrow.MaxSwarmStorage != "" {
		return cluster.Spec.Storage.StorageAutoGrow.MaxSwarmStorage
	}
	return cluster.Spec.Storage.StorageAutoGrow.MaxDataStorage
}

func storageAutoGrowEnabled(cluster *antflyv1.AntflyCluster) bool {
	return cluster.Spec.Storage.StorageAutoGrow != nil && cluster.Spec.Storage.StorageAutoGrow.Enabled
}

func effectiveDataNodeReplicas(cluster *antflyv1.AntflyCluster) int32 {
	if cluster.Spec.DataNodes.Suspend {
		return 0
	}
	if cluster.Spec.DataNodes.Replicas > 0 {
		return cluster.Spec.DataNodes.Replicas
	}
	return 3
}

func effectiveDataReplicas(sts *appsv1.StatefulSet, exists bool, fallback int32) int32 {
	if !exists || sts == nil {
		return fallback
	}
	if sts.Status.Replicas > 0 {
		return sts.Status.Replicas
	}
	if sts.Spec.Replicas != nil {
		return *sts.Spec.Replicas
	}
	return fallback
}

func effectiveDataReplicaTarget(sts *appsv1.StatefulSet, exists bool, fallback int32) int32 {
	if !exists || sts == nil {
		return fallback
	}
	if sts.Spec.Replicas != nil {
		return *sts.Spec.Replicas
	}
	if sts.Status.Replicas > 0 {
		return sts.Status.Replicas
	}
	return fallback
}

func nodeIDForDataOrdinal(ordinal int32) string {
	return strconv.FormatInt(int64(ordinal+1), 10)
}

// buildStorageInitContainer creates an init container that waits for the PVC to
// be mounted and prepares ownership for the non-root Antfly runtime user. This
// prevents a race where the main container starts before the PVC is attached,
// and recovers existing PVCs that were created with root-owned directories.
func (r *AntflyClusterReconciler) buildStorageInitContainer(volumeName string) corev1.Container {
	return corev1.Container{
		Name:    "wait-for-storage",
		Image:   "busybox:1.36",
		Command: []string{"/bin/sh", "-c"},
		Args: []string{fmt.Sprintf(`
set -eu

echo "Checking PVC mount status..."
timeout=120
while [ $timeout -gt 0 ]; do
    if mountpoint -q /antflydb; then
        echo "PVC mounted successfully at /antflydb"
        if [ -d "/antflydb/metadata" ] || [ -d "/antflydb/store" ]; then
            echo "Found existing data directories - recovery mode"
            ls -la /antflydb/
        else
            echo "No existing data - fresh cluster"
        fi
        marker=/antflydb/.antflydb-storage-prepared
        echo "Preparing PVC root for antfly runtime user %d:%d"
        if [ ! -f "$marker" ]; then
            chown -R %d:%d /antflydb
            chmod -R ug+rwX /antflydb
            touch "$marker"
            chown %d:%d "$marker"
            chmod ug+rw "$marker"
        else
            chown %d:%d /antflydb
            chmod ug+rwX /antflydb
        fi
        exit 0
    fi
    echo "Waiting for PVC mount... ($timeout seconds remaining)"
    sleep 2
    timeout=$((timeout - 2))
done
echo "ERROR: PVC mount timeout after 120 seconds"
exit 1
`,
			antflyRuntimeUID,
			antflyRuntimeGID,
			antflyRuntimeUID,
			antflyRuntimeGID,
			antflyRuntimeUID,
			antflyRuntimeGID,
			antflyRuntimeUID,
			antflyRuntimeGID,
		)},
		SecurityContext: &corev1.SecurityContext{
			RunAsUser: int64Ptr(0),
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      volumeName,
				MountPath: "/antflydb",
			},
		},
		// Minimal resources for the init container
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("10m"),
				corev1.ResourceMemory: resource.MustParse("16Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("50m"),
				corev1.ResourceMemory: resource.MustParse("32Mi"),
			},
		},
	}
}

// reconcileServiceMeshStatus detects sidecar injection status and updates cluster status accordingly
func (r *AntflyClusterReconciler) reconcileServiceMeshStatus(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	log := log.FromContext(ctx)

	// Initialize ServiceMeshStatus if not present
	if cluster.Status.ServiceMeshStatus == nil {
		cluster.Status.ServiceMeshStatus = &antflyv1.ServiceMeshStatus{}
	}

	// Update enabled status from spec
	cluster.Status.ServiceMeshStatus.Enabled = cluster.Spec.ServiceMesh != nil && cluster.Spec.ServiceMesh.Enabled

	// If service mesh is disabled, set status to None and return
	if !cluster.Status.ServiceMeshStatus.Enabled {
		if cluster.Status.ServiceMeshStatus.SidecarInjectionStatus != "None" {
			log.Info("Service mesh disabled", "cluster", cluster.Name)
			cluster.Status.ServiceMeshStatus.SidecarInjectionStatus = "None"
			cluster.Status.ServiceMeshStatus.PodsWithSidecars = 0
			cluster.Status.ServiceMeshStatus.TotalPods = 0
			cluster.Status.ServiceMeshStatus.LastTransitionTime = &metav1.Time{Time: time.Now()}
		}
		return nil
	}

	// Detect sidecar injection status
	podsWithSidecars, totalPods, err := r.detectSidecarInjectionStatus(ctx, cluster)
	if err != nil {
		log.Error(err, "Failed to detect sidecar injection status")
		return err
	}

	// Calculate injection status
	var newStatus string
	if totalPods == 0 {
		newStatus = "Unknown"
	} else if podsWithSidecars == totalPods {
		newStatus = "Complete"
	} else if podsWithSidecars == 0 {
		newStatus = "None"
	} else {
		newStatus = "Partial"
	}

	// Check if status changed
	statusChanged := cluster.Status.ServiceMeshStatus.SidecarInjectionStatus != newStatus

	// Update status fields
	cluster.Status.ServiceMeshStatus.PodsWithSidecars = podsWithSidecars
	cluster.Status.ServiceMeshStatus.TotalPods = totalPods
	cluster.Status.ServiceMeshStatus.SidecarInjectionStatus = newStatus

	if statusChanged {
		cluster.Status.ServiceMeshStatus.LastTransitionTime = &metav1.Time{Time: time.Now()}
		log.Info("Service mesh status changed",
			"cluster", cluster.Name,
			"status", newStatus,
			"podsWithSidecars", podsWithSidecars,
			"totalPods", totalPods)
	}

	// Handle partial injection - emit event and block reconciliation
	if newStatus == "Partial" {
		// Create warning event
		r.Recorder.Eventf(cluster, nil, corev1.EventTypeWarning, "PartialSidecarInjection", "PartialSidecarInjection",
			"Partial sidecar injection detected: %d/%d pods have sidecars", podsWithSidecars, totalPods)

		log.Error(fmt.Errorf("partial sidecar injection"), "Blocking reconciliation",
			"podsWithSidecars", podsWithSidecars,
			"totalPods", totalPods)

		return fmt.Errorf("partial sidecar injection detected: %d/%d pods have sidecars", podsWithSidecars, totalPods)
	}

	// Log successful complete injection
	if newStatus == "Complete" && statusChanged {
		log.Info("Service mesh sidecar injection complete",
			"cluster", cluster.Name,
			"totalPods", totalPods)
	}

	return nil
}

func (r *AntflyClusterReconciler) updateStatus(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	originalConditions := append([]metav1.Condition(nil), cluster.Status.Conditions...)
	mode := effectiveTopologyMode(cluster)

	if mode == topologyModeSwarm {
		swarm := cluster.Spec.Swarm
		if swarm == nil {
			return fmt.Errorf("spec.swarm is required when spec.mode=Swarm")
		}

		swarmSts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name + "-swarm", Namespace: cluster.Namespace}, swarmSts); err != nil && !errors.IsNotFound(err) {
			return err
		}

		readyReplicas := swarmSts.Status.ReadyReplicas
		cluster.Status.Mode = antflyv1.ClusterModeSwarm
		cluster.Status.ReadyReplicas = readyReplicas
		cluster.Status.SwarmNodesReady = readyReplicas
		cluster.Status.MetadataNodesReady = 0
		cluster.Status.DataNodesReady = 0
		if readyReplicas >= swarm.Replicas && swarm.Replicas > 0 {
			cluster.Status.Phase = "Running"
		} else {
			cluster.Status.Phase = "Pending"
		}

		if cluster.Status.SwarmStatus == nil {
			cluster.Status.SwarmStatus = &antflyv1.SwarmStatus{}
		}
		oldStatus := *cluster.Status.SwarmStatus
		inferenceEnabled := swarm.Inference == nil || swarm.Inference.Enabled
		cluster.Status.SwarmStatus.Ready = readyReplicas >= swarm.Replicas && swarm.Replicas > 0
		cluster.Status.SwarmStatus.MetadataReady = cluster.Status.SwarmStatus.Ready
		cluster.Status.SwarmStatus.StoreReady = cluster.Status.SwarmStatus.Ready
		cluster.Status.SwarmStatus.InferenceReady = !inferenceEnabled || cluster.Status.SwarmStatus.Ready
		cluster.Status.SwarmStatus.NodeID = swarm.NodeID

		if completeConfig, err := r.generateSwarmConfig(cluster); err == nil {
			sum := sha256.Sum256([]byte(completeConfig))
			cluster.Status.SwarmStatus.ObservedConfigHash = fmt.Sprintf("%x", sum)[:16]
		}

		var podList corev1.PodList
		if err := r.List(ctx, &podList, client.InNamespace(cluster.Namespace), client.MatchingLabels(serviceSelectorLabels(cluster.Name, "swarm"))); err != nil {
			return err
		}
		for _, pod := range podList.Items {
			if pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodPending {
				cluster.Status.SwarmStatus.PodName = pod.Name
				cluster.Status.SwarmStatus.PodIP = pod.Status.PodIP
				break
			}
		}
		swarmFindings := poddiagnostics.DiagnosePods(podList.Items)
		if len(swarmFindings) > 0 {
			cluster.Status.Phase = "Degraded"
			cluster.Status.SwarmStatus.Ready = false
			cluster.Status.SwarmStatus.MetadataReady = false
			cluster.Status.SwarmStatus.StoreReady = false
			cluster.Status.SwarmStatus.InferenceReady = !inferenceEnabled
		}

		if oldStatus.LastTransitionTime == nil ||
			cluster.Status.SwarmStatus.Ready != oldStatus.Ready ||
			cluster.Status.SwarmStatus.MetadataReady != oldStatus.MetadataReady ||
			cluster.Status.SwarmStatus.StoreReady != oldStatus.StoreReady ||
			cluster.Status.SwarmStatus.InferenceReady != oldStatus.InferenceReady ||
			cluster.Status.SwarmStatus.PodName != oldStatus.PodName ||
			cluster.Status.SwarmStatus.PodIP != oldStatus.PodIP ||
			cluster.Status.SwarmStatus.NodeID != oldStatus.NodeID ||
			cluster.Status.SwarmStatus.ObservedConfigHash != oldStatus.ObservedConfigHash {
			now := metav1.Now()
			cluster.Status.SwarmStatus.LastTransitionTime = &now
		}

		r.updateRolloutCondition(cluster, swarmSts)
		r.setComponentCondition(cluster, antflyv1.TypeSwarmReady, readyReplicas, swarm.Replicas, swarmFindings, "swarm")
		r.setComponentCondition(cluster, antflyv1.TypeMetadataReady, readyReplicas, swarm.Replicas, swarmFindings, "swarm metadata")
		r.setComponentCondition(cluster, antflyv1.TypeDataReady, readyReplicas, swarm.Replicas, swarmFindings, "swarm store")
		if inferenceEnabled {
			r.setComponentCondition(cluster, antflyv1.TypeInferenceReady, readyReplicas, swarm.Replicas, swarmFindings, "swarm inference")
		} else {
			meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
				Type:               antflyv1.TypeInferenceReady,
				Status:             metav1.ConditionTrue,
				ObservedGeneration: cluster.Generation,
				Reason:             antflyv1.ReasonComponentReady,
				Message:            "Inference is disabled",
			})
		}
		r.setAvailableCondition(cluster, swarmFindings, readyReplicas >= swarm.Replicas && swarm.Replicas > 0)
		r.recordClusterRuntimeFailureEvents(cluster, originalConditions)
		r.updateProductTierStatus(cluster)
		if err := r.observeHAPrimaryRouteStatus(ctx, cluster); err != nil {
			return err
		}
		haAdminStatusErr := r.observeHAPrimaryAdminStatus(ctx, cluster)
		if standbyErr := r.observeHAStandbyAdminStatuses(ctx, cluster); standbyErr != nil && haAdminStatusErr == nil {
			haAdminStatusErr = standbyErr
		}
		if err := r.reconcileHAFencingLease(ctx, cluster); err != nil {
			return err
		}
		if err := r.observeHAFencingStatus(ctx, cluster); err != nil {
			return err
		}
		r.updateHAStatusAndConditions(cluster)
		if haAdminStatusErr != nil {
			setHACondition(
				cluster,
				antflyv1.TypeHADegraded,
				metav1.ConditionTrue,
				"HAAdminStatusUnavailable",
				fmt.Sprintf("Unable to observe HA admin status: %v", haAdminStatusErr),
			)
		}
		if err := r.reconcileHAAdminJobs(ctx, cluster); err != nil {
			return err
		}
		r.updateHALastPromotionFromAdminJobs(ctx, cluster)
		r.updateHAFormerPrimaryFromAdminJobs(ctx, cluster)
		if err := r.reconcileHAPrimaryRoute(ctx, cluster); err != nil {
			return err
		}
		r.updateHAAdminJobExecutionCondition(cluster)
		r.updateServiceMeshReadyCondition(cluster)
		return r.Status().Update(ctx, cluster)
	}

	// Get current status of StatefulSets and Deployment
	metadataSts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name + "-metadata", Namespace: cluster.Namespace}, metadataSts); err != nil && !errors.IsNotFound(err) {
		return err
	}

	dataSts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name + "-data", Namespace: cluster.Namespace}, dataSts); err != nil && !errors.IsNotFound(err) {
		return err
	}

	cluster.Status.MetadataNodesReady = metadataSts.Status.ReadyReplicas
	cluster.Status.DataNodesReady = dataSts.Status.ReadyReplicas
	cluster.Status.Mode = antflyv1.ClusterModeClustered
	cluster.Status.ReadyReplicas = metadataSts.Status.ReadyReplicas + dataSts.Status.ReadyReplicas
	cluster.Status.SwarmNodesReady = 0
	cluster.Status.SwarmStatus = nil

	// Update autoscaling status if enabled
	if cluster.Spec.DataNodes.AutoScaling != nil && cluster.Spec.DataNodes.AutoScaling.Enabled {
		if cluster.Status.AutoScalingStatus != nil {
			cluster.Status.AutoScalingStatus.CurrentReplicas = effectiveDataReplicas(dataSts, dataSts.Name != "", effectiveDataNodeReplicas(cluster))
		}
	}

	// Determine phase based on the configured replica counts rather than a
	// hardcoded production-sized cluster. Local dev intentionally runs 1+1.
	metadataReplicas := int32(3)
	if cluster.Spec.MetadataNodes.Replicas > 0 {
		metadataReplicas = cluster.Spec.MetadataNodes.Replicas
	}
	dataReplicas := effectiveDataNodeReplicas(cluster)

	metadataPods, err := r.listComponentPods(ctx, cluster, "metadata")
	if err != nil {
		return err
	}
	dataPods, err := r.listComponentPods(ctx, cluster, "data")
	if err != nil {
		return err
	}
	metadataFindings := poddiagnostics.DiagnosePods(metadataPods)
	dataFindings := poddiagnostics.DiagnosePods(dataPods)
	r.setComponentCondition(cluster, antflyv1.TypeMetadataReady, cluster.Status.MetadataNodesReady, metadataReplicas, metadataFindings, "metadata")
	r.setComponentCondition(cluster, antflyv1.TypeDataReady, cluster.Status.DataNodesReady, dataReplicas, dataFindings, "data")
	allRuntimeFindings := append(append([]poddiagnostics.Finding{}, metadataFindings...), dataFindings...)

	if len(allRuntimeFindings) > 0 {
		cluster.Status.Phase = "Degraded"
	} else if cluster.Status.MetadataNodesReady >= metadataReplicas && cluster.Status.DataNodesReady >= dataReplicas {
		cluster.Status.Phase = "Running"
	} else {
		cluster.Status.Phase = "Pending"
	}

	r.updateRolloutCondition(cluster, metadataSts, dataSts)
	r.setAvailableCondition(cluster, allRuntimeFindings, cluster.Status.Phase == "Running")
	r.recordClusterRuntimeFailureEvents(cluster, originalConditions)
	r.updateProductTierStatus(cluster)
	if err := r.observeHAPrimaryRouteStatus(ctx, cluster); err != nil {
		return err
	}
	haAdminStatusErr := r.observeHAPrimaryAdminStatus(ctx, cluster)
	if standbyErr := r.observeHAStandbyAdminStatuses(ctx, cluster); standbyErr != nil && haAdminStatusErr == nil {
		haAdminStatusErr = standbyErr
	}
	if err := r.reconcileHAFencingLease(ctx, cluster); err != nil {
		return err
	}
	if err := r.observeHAFencingStatus(ctx, cluster); err != nil {
		return err
	}
	r.updateHAStatusAndConditions(cluster)
	if haAdminStatusErr != nil {
		setHACondition(
			cluster,
			antflyv1.TypeHADegraded,
			metav1.ConditionTrue,
			"HAAdminStatusUnavailable",
			fmt.Sprintf("Unable to observe HA admin status: %v", haAdminStatusErr),
		)
	}
	if err := r.reconcileHAAdminJobs(ctx, cluster); err != nil {
		return err
	}
	r.updateHALastPromotionFromAdminJobs(ctx, cluster)
	r.updateHAFormerPrimaryFromAdminJobs(ctx, cluster)
	if err := r.reconcileHAPrimaryRoute(ctx, cluster); err != nil {
		return err
	}
	r.updateHAAdminJobExecutionCondition(cluster)

	// Update ServiceMeshReady condition
	r.updateServiceMeshReadyCondition(cluster)

	return r.Status().Update(ctx, cluster)
}

func (r *AntflyClusterReconciler) reconcileHAAdminJobs(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	ha := cluster.Spec.HighAvailability
	if ha == nil || ha.Admin == nil || !ha.Admin.ExecutePlannedActions ||
		ha.Mode == "" || ha.Mode == antflyv1.HAModeDisabled ||
		cluster.Status.HAStatus == nil {
		return nil
	}

	for i := range cluster.Status.HAStatus.PlannedActions {
		action := &cluster.Status.HAStatus.PlannedActions[i]
		if len(action.AdminCommand) == 0 || strings.TrimSpace(action.AdminURL) == "" {
			continue
		}
		if action.AdminJobPhase == haAdminJobPhaseSucceeded || action.AdminJobPhase == haAdminJobPhaseFailed {
			if action.AdminJobPhase == haAdminJobPhaseSucceeded && action.AdminResult == nil {
				r.updateHAAdminActionResultFromJobLogs(ctx, cluster, action)
			}
			continue
		}
		if !haPlannedActionDependenciesSucceeded(cluster.Status.HAStatus.PlannedActions, i) {
			if action.AdminJobName == "" && action.AdminJobPhase == "" {
				action.AdminJobPhase = haAdminJobPhaseWaitingDependency
			}
			continue
		}
		if handled, err := r.executeHAPlannedActionTyped(ctx, cluster, action); handled {
			action.AdminJobName = haAdminDirectAPIName
			if err != nil {
				action.AdminJobPhase = haAdminJobPhaseFailed
			} else {
				action.AdminJobPhase = haAdminJobPhaseSucceeded
			}
			continue
		}

		job := buildHAAdminJob(cluster, ha.Admin, *action)
		action.AdminJobName = job.Name
		if err := controllerutil.SetControllerReference(cluster, job, r.Scheme); err != nil {
			return err
		}

		existing := &batchv1.Job{}
		err := r.Get(ctx, types.NamespacedName{Name: job.Name, Namespace: job.Namespace}, existing)
		if errors.IsNotFound(err) {
			action.AdminJobPhase = haAdminJobPhasePending
			return r.Create(ctx, job)
		}
		if err != nil {
			return err
		}
		action.AdminJobPhase = haAdminJobPhase(existing)
		if action.AdminJobPhase == haAdminJobPhaseSucceeded && action.AdminResult == nil {
			r.updateHAAdminActionResultFromJobLogs(ctx, cluster, action)
		}
		if !haAdminJobComplete(existing) {
			continue
		}
	}
	return nil
}

const haAdminDirectAPIName = "direct-admin-api"

func (r *AntflyClusterReconciler) executeHAPlannedActionTyped(ctx context.Context, cluster *antflyv1.AntflyCluster, action *antflyv1.HAPlannedActionStatus) (bool, error) {
	if action == nil {
		return false, nil
	}
	if !haPlannedActionSupportsDirectAdminAPI(haActionKind(action.Kind)) {
		return false, nil
	}
	if err := haValidatePlannedActionAdminOperation(*action); err != nil {
		return true, err
	}
	switch action.Kind {
	case string(haActionCreateSlot):
		slotName := haActionSlotName(*action)
		if slotName == "" {
			return false, nil
		}
		body := map[string]any{"slot_name": slotName}
		if action.TargetLSN > 0 {
			body["initial_lsn"] = action.TargetLSN
		}
		raw, err := r.postHAAdminJSONRaw(ctx, action.AdminURL, "/admin/v1/ha/replication-slots", body)
		if err == nil {
			err = requireHADirectAdminActionResult(action, raw)
		}
		return true, err
	case string(haActionPauseSlot):
		slotName := haActionSlotName(*action)
		if slotName == "" {
			return false, nil
		}
		raw, err := r.requestHAAdminJSONRaw(ctx, http.MethodPut, action.AdminURL, "/admin/v1/ha/replication-slots/"+url.PathEscape(slotName)+"/pause", nil)
		if err == nil {
			err = requireHADirectAdminActionResult(action, raw)
		}
		return true, err
	case string(haActionResumeSlot):
		slotName := haActionSlotName(*action)
		if slotName == "" {
			return false, nil
		}
		raw, err := r.requestHAAdminJSONRaw(ctx, http.MethodPut, action.AdminURL, "/admin/v1/ha/replication-slots/"+url.PathEscape(slotName)+"/resume", nil)
		if err == nil {
			err = requireHADirectAdminActionResult(action, raw)
		}
		return true, err
	case string(haActionDropSlot):
		slotName := haActionSlotName(*action)
		if slotName == "" {
			return false, nil
		}
		raw, err := r.requestHAAdminJSONRaw(ctx, http.MethodDelete, action.AdminURL, "/admin/v1/ha/replication-slots/"+url.PathEscape(slotName), nil)
		if err == nil {
			err = requireHADirectAdminActionResult(action, raw)
		}
		return true, err
	case string(haActionSeedStandby), string(haActionMarkReseed):
		slotName := haActionSlotName(*action)
		if slotName == "" {
			return false, nil
		}
		body := map[string]any{
			"slot_name":   slotName,
			"manifest_id": haSeedBeginManifestID(*action, slotName),
		}
		raw, err := r.postHAAdminJSONRaw(ctx, action.AdminURL, "/admin/v1/ha/base-backups", body)
		if err == nil {
			err = requireHADirectAdminActionResult(action, raw)
		}
		return true, err
	case string(haActionAcquireFence):
		body, ok := haFenceAcquireBody(cluster, *action)
		if !ok {
			return false, nil
		}
		raw, err := r.postHAAdminJSONRaw(ctx, action.AdminURL, "/admin/v1/ha/fence", body)
		if err == nil {
			err = requireHADirectAdminActionResult(action, raw)
		}
		return true, err
	case string(haActionPromoteStandby):
		raw, err := r.requestHAAdminJSONRaw(ctx, http.MethodPost, action.AdminURL, "/admin/v1/ha/promotion/current-fence", nil)
		if err == nil && !r.applyHADirectPromotionResult(cluster, action, raw) {
			err = fmt.Errorf("HA admin action %s succeeded without typed promotion receipt", action.Kind)
		}
		return true, err
	case string(haActionDemoteFormerPrimary), string(haActionRewindFormerPrimary), string(haActionReseedFormerPrimary):
		body, ok := haRejoinAssessBody(cluster, *action)
		if !ok {
			return false, nil
		}
		encoded, err := json.Marshal(body)
		if err != nil {
			return true, err
		}
		raw, err := r.requestHAAdminJSONRaw(ctx, http.MethodPost, action.AdminURL, "/admin/v1/ha/rejoin/assess", encoded)
		if err == nil && !r.applyHADirectRejoinAssessResult(cluster, action, raw) {
			err = fmt.Errorf("HA admin action %s succeeded without typed rejoin assessment", action.Kind)
		}
		return true, err
	default:
		return false, nil
	}
}

func haPlannedActionSupportsDirectAdminAPI(kind haActionKind) bool {
	switch kind {
	case haActionCreateSlot,
		haActionPauseSlot,
		haActionResumeSlot,
		haActionDropSlot,
		haActionSeedStandby,
		haActionMarkReseed,
		haActionAcquireFence,
		haActionPromoteStandby,
		haActionDemoteFormerPrimary,
		haActionRewindFormerPrimary,
		haActionReseedFormerPrimary:
		return true
	default:
		return false
	}
}

func haValidatePlannedActionAdminOperation(action antflyv1.HAPlannedActionStatus) error {
	if strings.TrimSpace(action.AdminMethod) == "" && strings.TrimSpace(action.AdminPath) == "" {
		return nil
	}
	expectedMethod, expectedPath := haAdminOperation(haPlannedAction{
		Kind:        haActionKind(action.Kind),
		StandbyName: action.StandbyName,
		SlotName:    action.SlotName,
	})
	if expectedMethod == "" || expectedPath == "" {
		return fmt.Errorf("planned HA action %q has typed admin operation %s %s but no matching direct admin API operation", action.Kind, action.AdminMethod, action.AdminPath)
	}
	if strings.TrimSpace(action.AdminMethod) != expectedMethod || strings.TrimSpace(action.AdminPath) != expectedPath {
		return fmt.Errorf("planned HA action %q typed admin operation mismatch: status has %s %s, expected %s %s", action.Kind, action.AdminMethod, action.AdminPath, expectedMethod, expectedPath)
	}
	return nil
}

func haFenceAcquireBody(cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) (map[string]any, bool) {
	if cluster == nil {
		return nil, false
	}
	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	if identity == nil || identity.CurrentPrimaryID == "" || strings.TrimSpace(action.StandbyName) == "" {
		return nil, false
	}
	reason := action.Reason
	if strings.TrimSpace(reason) == "" {
		reason = action.FenceReason
	}
	return map[string]any{
		"identity": map[string]any{
			"cluster_id":  identity.ClusterID,
			"shard_id":    identity.ShardID,
			"table_id":    identity.TableID,
			"timeline_id": identity.TimelineID,
			"epoch":       identity.Epoch,
		},
		"old_primary_id":   identity.CurrentPrimaryID,
		"promoted_node_id": strings.TrimSpace(action.StandbyName),
		"new_timeline_id":  identity.TimelineID + 1,
		"new_epoch":        identity.Epoch + 1,
		"required_lsn":     action.TargetLSN,
		"observed_lsn":     action.TargetLSN,
		"reason":           reason,
	}, true
}

func haRejoinAssessBody(cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) (map[string]any, bool) {
	if cluster == nil {
		return nil, false
	}
	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	nodeID := strings.TrimSpace(action.StandbyName)
	if identity == nil || nodeID == "" {
		return nil, false
	}
	lastLSN := action.ObservedLSN
	if lastLSN == 0 {
		lastLSN = action.TargetLSN
	}
	body := map[string]any{
		"node_id": nodeID,
		"identity": map[string]any{
			"cluster_id":  identity.ClusterID,
			"shard_id":    identity.ShardID,
			"table_id":    identity.TableID,
			"timeline_id": identity.TimelineID,
			"epoch":       identity.Epoch,
		},
		"last_lsn":          lastLSN,
		"retained_from_lsn": action.RetainedFromLSN,
	}
	if receipt, ok := haRejoinFenceReceipt(cluster.Status.HAStatus, identity); ok {
		body["receipt"] = receipt
	}
	return body, true
}

func haRejoinFenceReceipt(status *antflyv1.HAStatus, identity *antflyv1.HAReplicationIdentitySpec) (map[string]any, bool) {
	if status == nil || status.LastPromotion == nil {
		return nil, false
	}
	promotion := status.LastPromotion
	if identity == nil {
		return nil, false
	}
	if promotion.OldPrimaryID == "" || promotion.PromotedStandbyID == "" ||
		promotion.ParentTimelineID == 0 || promotion.ParentEpoch == 0 ||
		promotion.NewTimelineID == 0 || promotion.NewEpoch == 0 ||
		haPromotionRequiredLSN(promotion) == 0 || haPromotionObservedLSN(promotion) == 0 ||
		promotion.FenceGeneration == 0 || strings.TrimSpace(promotion.FenceToken) == "" {
		return nil, false
	}
	if promotion.OldPrimaryID != identity.CurrentPrimaryID ||
		promotion.ParentTimelineID != identity.TimelineID ||
		promotion.ParentEpoch != identity.Epoch {
		return nil, false
	}
	return map[string]any{
		"identity": map[string]any{
			"cluster_id":  identity.ClusterID,
			"shard_id":    identity.ShardID,
			"table_id":    identity.TableID,
			"timeline_id": promotion.NewTimelineID,
			"epoch":       promotion.NewEpoch,
		},
		"old_primary_id":     promotion.OldPrimaryID,
		"promoted_node_id":   promotion.PromotedStandbyID,
		"parent_timeline_id": promotion.ParentTimelineID,
		"parent_epoch":       promotion.ParentEpoch,
		"new_timeline_id":    promotion.NewTimelineID,
		"new_epoch":          promotion.NewEpoch,
		"required_lsn":       haPromotionRequiredLSN(promotion),
		"observed_lsn":       haPromotionObservedLSN(promotion),
		"generation":         promotion.FenceGeneration,
		"forced":             promotion.Forced,
		"token":              strings.TrimSpace(promotion.FenceToken),
		"reason":             promotion.FenceReason,
	}, true
}

func haActionSlotName(action antflyv1.HAPlannedActionStatus) string {
	if strings.TrimSpace(action.SlotName) != "" {
		return strings.TrimSpace(action.SlotName)
	}
	return strings.TrimSpace(action.StandbyName)
}

func haSeedBeginManifestID(action antflyv1.HAPlannedActionStatus, slotName string) string {
	for i := 0; i+1 < len(action.AdminCommand); i++ {
		if action.AdminCommand[i] == "--manifest-id" {
			if manifestID := strings.TrimSpace(action.AdminCommand[i+1]); manifestID != "" {
				return manifestID
			}
		}
	}
	return fmt.Sprintf("base-%s-%d", slotName, action.TargetLSN)
}

type haDirectAdminActionResultJSON struct {
	SchemaVersion uint32 `json:"schema_version"`
	SlotAction    string `json:"slot_action"`
	Slot          struct {
		SlotName string `json:"slot_name"`
	} `json:"slot"`
	SlotName       string `json:"slot_name"`
	ManifestID     string `json:"manifest_id"`
	BackupLSN      uint64 `json:"backup_lsn"`
	StartRecordLSN uint64 `json:"start_record_lsn"`
	EndRecordLSN   uint64 `json:"end_record_lsn"`
	CheckpointLSN  uint64 `json:"checkpoint_lsn"`
	Receipt        struct {
		Generation uint64 `json:"generation"`
		Token      string `json:"token"`
	} `json:"receipt"`
}

type haDirectAdminActionResultEnvelope struct {
	Result struct {
		Slot         *haDirectAdminActionResultJSON `json:"slot,omitempty"`
		SeedBegin    *haDirectAdminActionResultJSON `json:"seed_begin,omitempty"`
		FenceAcquire *haDirectAdminActionResultJSON `json:"fence_acquire,omitempty"`
	} `json:"result"`
}

func requireHADirectAdminActionResult(action *antflyv1.HAPlannedActionStatus, raw []byte) error {
	if action == nil || !haActionRequiresAdminResult(haActionKind(action.Kind)) {
		return nil
	}
	if applyHADirectAdminActionResult(action, raw) && haActionHasRequiredAdminResult(*action) {
		return nil
	}
	action.AdminResult = nil
	return fmt.Errorf("HA admin action %s succeeded without typed result evidence", action.Kind)
}

func applyHADirectAdminActionResult(action *antflyv1.HAPlannedActionStatus, raw []byte) bool {
	if action == nil {
		return false
	}
	result, ok := parseHADirectAdminActionResult(raw)
	if !ok {
		return false
	}
	action.AdminResult = result
	return true
}

func parseHADirectAdminActionResult(raw []byte) (*antflyv1.HAAdminActionResultStatus, bool) {
	var direct haDirectAdminActionResultJSON
	if err := json.Unmarshal(raw, &direct); err != nil {
		return nil, false
	}
	result := &direct
	if !haDirectAdminActionResultHasCorrelationFields(direct) {
		var envelope haDirectAdminActionResultEnvelope
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return nil, false
		}
		switch {
		case envelope.Result.Slot != nil:
			result = envelope.Result.Slot
		case envelope.Result.SeedBegin != nil:
			result = envelope.Result.SeedBegin
		case envelope.Result.FenceAcquire != nil:
			result = envelope.Result.FenceAcquire
		default:
			return nil, false
		}
		if result.SchemaVersion == 0 {
			result.SchemaVersion = direct.SchemaVersion
		}
	}
	status := &antflyv1.HAAdminActionResultStatus{
		SchemaVersion:   result.SchemaVersion,
		SlotAction:      strings.TrimSpace(result.SlotAction),
		SlotName:        strings.TrimSpace(result.SlotName),
		ManifestID:      strings.TrimSpace(result.ManifestID),
		BackupLSN:       result.BackupLSN,
		StartRecordLSN:  result.StartRecordLSN,
		EndRecordLSN:    result.EndRecordLSN,
		CheckpointLSN:   result.CheckpointLSN,
		FenceGeneration: result.Receipt.Generation,
		FenceToken:      strings.TrimSpace(result.Receipt.Token),
	}
	if status.SlotName == "" {
		status.SlotName = strings.TrimSpace(result.Slot.SlotName)
	}
	if status.SlotAction == "" &&
		status.SlotName == "" &&
		status.ManifestID == "" &&
		status.BackupLSN == 0 &&
		status.StartRecordLSN == 0 &&
		status.EndRecordLSN == 0 &&
		status.CheckpointLSN == 0 &&
		status.FenceGeneration == 0 &&
		status.FenceToken == "" {
		return nil, false
	}
	return status, true
}

func haDirectAdminActionResultHasCorrelationFields(result haDirectAdminActionResultJSON) bool {
	return strings.TrimSpace(result.SlotAction) != "" ||
		strings.TrimSpace(result.Slot.SlotName) != "" ||
		strings.TrimSpace(result.SlotName) != "" ||
		strings.TrimSpace(result.ManifestID) != "" ||
		result.BackupLSN != 0 ||
		result.StartRecordLSN != 0 ||
		result.EndRecordLSN != 0 ||
		result.CheckpointLSN != 0 ||
		result.Receipt.Generation != 0 ||
		strings.TrimSpace(result.Receipt.Token) != ""
}

func (r *AntflyClusterReconciler) updateHAAdminActionResultFromJobLogs(ctx context.Context, cluster *antflyv1.AntflyCluster, action *antflyv1.HAPlannedActionStatus) {
	if r.KubeClient == nil || action == nil || action.AdminJobName == "" || action.AdminJobName == haAdminDirectAPIName {
		return
	}
	body, ok := r.haAdminJobLogBody(ctx, cluster, action.AdminJobName)
	if !ok {
		return
	}
	result, ok := parseHAAdminActionResultTable(body)
	if !ok {
		return
	}
	action.AdminResult = result
}

func parseHAAdminActionResultTable(body string) (*antflyv1.HAAdminActionResultStatus, bool) {
	lines := parseHATableLines(body)
	resultName := strings.TrimSpace(lines["result"])
	if resultName == "" {
		return nil, false
	}
	result := &antflyv1.HAAdminActionResultStatus{
		SlotAction: strings.TrimSpace(lines["slot_action"]),
		SlotName:   strings.TrimSpace(lines["slot_name"]),
		ManifestID: strings.TrimSpace(lines["manifest_id"]),
		FenceToken: strings.TrimSpace(lines["token"]),
	}
	if result.FenceToken == "" {
		result.FenceToken = strings.TrimSpace(lines["fence_token"])
	}
	result.BackupLSN, _ = parseHAResultUint(lines, "backup_lsn")
	result.StartRecordLSN, _ = parseHAResultUint(lines, "start_record_lsn")
	result.EndRecordLSN, _ = parseHAResultUint(lines, "end_record_lsn")
	result.CheckpointLSN, _ = parseHAResultUint(lines, "checkpoint_lsn")
	result.FenceGeneration, _ = parseHAResultUint(lines, "generation")
	if result.FenceGeneration == 0 {
		result.FenceGeneration, _ = parseHAResultUint(lines, "fence_generation")
	}
	if resultName == "rejoin_assess" {
		rejoin, ok := parseHARejoinJobResult(body)
		if !ok {
			return nil, false
		}
		applyHARejoinAdminActionResult(result, rejoin)
	}
	if result.SlotName == "" {
		result.SlotName = strings.TrimSpace(lines["name"])
	}
	if result.SlotAction == "" && strings.HasPrefix(resultName, "slot") {
		result.SlotAction = resultName
	}
	if result.SlotAction == "" &&
		result.SlotName == "" &&
		result.ManifestID == "" &&
		result.BackupLSN == 0 &&
		result.StartRecordLSN == 0 &&
		result.EndRecordLSN == 0 &&
		result.CheckpointLSN == 0 &&
		result.FenceGeneration == 0 &&
		result.FenceToken == "" &&
		result.RejoinAction == "" {
		return nil, false
	}
	return result, true
}

func (r *AntflyClusterReconciler) applyHADirectPromotionResult(cluster *antflyv1.AntflyCluster, action *antflyv1.HAPlannedActionStatus, raw []byte) bool {
	if cluster == nil || cluster.Status.HAStatus == nil || action == nil {
		return false
	}
	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	if identity == nil || action.StandbyName == "" {
		return false
	}
	result, ok := parseHAPromotionAPIResult(raw)
	if !ok {
		return false
	}
	if !haDirectPromotionResultMatchesAction(result, identity, action) {
		return false
	}
	action.AdminResult = haPromotionAdminActionResult(result)
	if cluster.Status.HAStatus.LastPromotion == nil ||
		!haPromotionStatusMatches(cluster.Status.HAStatus.LastPromotion, identity, *action) {
		now := metav1.Now()
		cluster.Status.HAStatus.LastPromotion = &antflyv1.HAPromotionStatus{
			OldPrimaryID:      identity.CurrentPrimaryID,
			PromotedStandbyID: action.StandbyName,
			FenceAuthority:    action.FenceAuthority,
			FenceReason:       haPromotionFenceReason(*action),
			CompletionTime:    &now,
		}
	}
	applyHAPromotionJobResult(cluster.Status.HAStatus.LastPromotion, result)
	if cluster.Status.HAStatus.LastPromotion.FenceAuthority == "" {
		cluster.Status.HAStatus.LastPromotion.FenceAuthority = action.FenceAuthority
	}
	if cluster.Status.HAStatus.LastPromotion.FenceReason == "" {
		cluster.Status.HAStatus.LastPromotion.FenceReason = haPromotionFenceReason(*action)
	}
	return true
}

func haPromotionAdminActionResult(result haPromotionJobResult) *antflyv1.HAAdminActionResultStatus {
	return &antflyv1.HAAdminActionResultStatus{
		FenceGeneration: result.FenceGeneration,
		FenceToken:      result.FenceToken,
	}
}

func haDirectPromotionResultMatchesAction(result haPromotionJobResult, identity *antflyv1.HAReplicationIdentitySpec, action *antflyv1.HAPlannedActionStatus) bool {
	if identity == nil || action == nil {
		return false
	}
	if result.ParentTimelineID != identity.TimelineID || result.ParentEpoch != identity.Epoch {
		return false
	}
	if result.NewTimelineID != identity.TimelineID+1 || result.NewEpoch != identity.Epoch+1 {
		return false
	}
	if action.TargetLSN > 0 && (result.SwitchLSN != action.TargetLSN || result.RequiredLSN != action.TargetLSN) {
		return false
	}
	if result.ObservedLSN < result.RequiredLSN {
		return false
	}
	if action.FenceGeneration > 0 && result.FenceGeneration != action.FenceGeneration {
		return false
	}
	return true
}

func (r *AntflyClusterReconciler) updateHALastPromotionFromAdminJobs(ctx context.Context, cluster *antflyv1.AntflyCluster) {
	if cluster.Status.HAStatus == nil {
		return
	}
	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	if identity == nil {
		return
	}
	for i, action := range cluster.Status.HAStatus.PlannedActions {
		if action.Kind != string(haActionPromoteStandby) ||
			action.AdminJobPhase != haAdminJobPhaseSucceeded ||
			action.StandbyName == "" {
			continue
		}
		if !haPlannedActionDependenciesSucceeded(cluster.Status.HAStatus.PlannedActions, i) {
			continue
		}
		if haPromotionStatusMatches(cluster.Status.HAStatus.LastPromotion, identity, action) {
			if cluster.Status.HAStatus.LastPromotion.FenceAuthority == "" {
				cluster.Status.HAStatus.LastPromotion.FenceAuthority = action.FenceAuthority
			}
			r.updateHAPromotionStatusFromAdminJobLogs(ctx, cluster, action, cluster.Status.HAStatus.LastPromotion)
			return
		}
		now := metav1.Now()
		cluster.Status.HAStatus.LastPromotion = &antflyv1.HAPromotionStatus{
			OldPrimaryID:      identity.CurrentPrimaryID,
			PromotedStandbyID: action.StandbyName,
			ParentTimelineID:  identity.TimelineID,
			ParentEpoch:       identity.Epoch,
			NewTimelineID:     identity.TimelineID + 1,
			NewEpoch:          identity.Epoch + 1,
			SwitchLSN:         action.TargetLSN,
			RequiredLSN:       action.TargetLSN,
			ObservedLSN:       action.TargetLSN,
			FenceGeneration:   action.FenceGeneration,
			FenceAuthority:    action.FenceAuthority,
			FenceReason:       haPromotionFenceReason(action),
			CompletionTime:    &now,
		}
		r.updateHAPromotionStatusFromAdminJobLogs(ctx, cluster, action, cluster.Status.HAStatus.LastPromotion)
		return
	}
}

func (r *AntflyClusterReconciler) applyHADirectRejoinAssessResult(cluster *antflyv1.AntflyCluster, action *antflyv1.HAPlannedActionStatus, raw []byte) bool {
	if cluster == nil || cluster.Status.HAStatus == nil || action == nil {
		return false
	}
	result, ok := parseHARejoinAPIResult(raw)
	if !ok {
		return false
	}
	if !haDirectRejoinResultMatchesAction(result, cluster.Status.HAStatus, *action) {
		return false
	}
	action.AdminResult = haRejoinAdminActionResult(result)
	if !haActionHasRequiredAdminResult(*action) {
		action.AdminResult = nil
		return false
	}
	if cluster.Status.HAStatus.FormerPrimary == nil {
		cluster.Status.HAStatus.FormerPrimary = &antflyv1.HAFormerPrimaryStatus{NodeID: action.StandbyName}
	}
	applyHAFormerPrimaryActionStatus(cluster.Status.HAStatus.FormerPrimary, *action, cluster.Status.HAStatus.LastPromotion)
	applyHARejoinJobResult(cluster.Status.HAStatus.FormerPrimary, result)
	return true
}

func haDirectRejoinResultMatchesAction(result haRejoinJobResult, status *antflyv1.HAStatus, action antflyv1.HAPlannedActionStatus) bool {
	if action.StandbyName != "" && result.FormerNodeID != action.StandbyName {
		return false
	}
	if action.TargetLSN > 0 && result.ForkLSN != action.TargetLSN {
		return false
	}
	if action.ObservedLSN > 0 && result.FormerLastLSN != action.ObservedLSN {
		return false
	}
	if action.RetainedFromLSN > 0 && result.RetainedFromLSN != action.RetainedFromLSN {
		return false
	}
	if status != nil && status.LastPromotion != nil {
		promotion := status.LastPromotion
		if promotion.NewTimelineID > 0 && result.TargetTimelineID != promotion.NewTimelineID {
			return false
		}
		if promotion.NewEpoch > 0 && result.TargetEpoch != promotion.NewEpoch {
			return false
		}
	}
	return true
}

func haPromotionFenceReason(action antflyv1.HAPlannedActionStatus) string {
	if action.FenceReason != "" {
		return action.FenceReason
	}
	return action.Reason
}

func (r *AntflyClusterReconciler) updateHAPromotionStatusFromAdminJobLogs(ctx context.Context, cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus, promotion *antflyv1.HAPromotionStatus) {
	if r.KubeClient == nil || action.AdminJobName == "" || promotion == nil {
		return
	}
	body, ok := r.haAdminJobLogBody(ctx, cluster, action.AdminJobName)
	if !ok {
		return
	}
	result, ok := parseHAPromotionJobResult(body)
	if !ok {
		return
	}
	applyHAPromotionJobResult(promotion, result)
}

func (r *AntflyClusterReconciler) updateHAFormerPrimaryFromAdminJobs(ctx context.Context, cluster *antflyv1.AntflyCluster) {
	if cluster.Status.HAStatus == nil {
		return
	}
	for i, action := range cluster.Status.HAStatus.PlannedActions {
		if !haFormerPrimaryActionKind(action.Kind) ||
			action.AdminJobPhase != haAdminJobPhaseSucceeded ||
			action.AdminJobName == "" {
			continue
		}
		if !haPlannedActionDependenciesSucceeded(cluster.Status.HAStatus.PlannedActions, i) {
			continue
		}
		if cluster.Status.HAStatus.FormerPrimary == nil {
			cluster.Status.HAStatus.FormerPrimary = &antflyv1.HAFormerPrimaryStatus{NodeID: action.StandbyName}
		}
		applyHAFormerPrimaryActionStatus(cluster.Status.HAStatus.FormerPrimary, action, cluster.Status.HAStatus.LastPromotion)
		r.updateHAFormerPrimaryStatusFromAdminJobLogs(ctx, cluster, action, cluster.Status.HAStatus.FormerPrimary)
		return
	}
}

func haFormerPrimaryActionKind(kind string) bool {
	return kind == string(haActionDemoteFormerPrimary) ||
		kind == string(haActionRewindFormerPrimary) ||
		kind == string(haActionReseedFormerPrimary)
}

func (r *AntflyClusterReconciler) updateHAFormerPrimaryStatusFromAdminJobLogs(ctx context.Context, cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus, former *antflyv1.HAFormerPrimaryStatus) {
	if r.KubeClient == nil || former == nil {
		return
	}
	body, ok := r.haAdminJobLogBody(ctx, cluster, action.AdminJobName)
	if !ok {
		return
	}
	result, ok := parseHARejoinJobResult(body)
	if !ok {
		return
	}
	applyHARejoinJobResult(former, result)
}

func applyHAFormerPrimaryActionStatus(former *antflyv1.HAFormerPrimaryStatus, action antflyv1.HAPlannedActionStatus, promotion *antflyv1.HAPromotionStatus) {
	if former == nil {
		return
	}
	if action.StandbyName != "" {
		former.NodeID = action.StandbyName
	}
	if action.TargetLSN != 0 {
		former.SwitchLSN = action.TargetLSN
	}
	if action.ObservedLSN != 0 {
		former.ObservedLSN = action.ObservedLSN
	}
	if action.RetainedFromLSN != 0 {
		former.RetainedFromLSN = action.RetainedFromLSN
	}
	former.FenceAuthority = action.FenceAuthority
	former.FenceHolder = action.FenceHolder
	former.FenceGeneration = action.FenceGeneration
	if promotion != nil {
		if former.NodeID == "" {
			former.NodeID = promotion.OldPrimaryID
		}
		if former.ParentTimelineID == 0 {
			former.ParentTimelineID = promotion.ParentTimelineID
		}
		if former.NewTimelineID == 0 {
			former.NewTimelineID = promotion.NewTimelineID
		}
		if former.SwitchLSN == 0 {
			former.SwitchLSN = promotion.SwitchLSN
		}
		if former.FenceAuthority == "" {
			former.FenceAuthority = promotion.FenceAuthority
		}
		if former.FenceHolder == "" {
			former.FenceHolder = promotion.PromotedStandbyID
		}
		if former.FenceGeneration == 0 {
			former.FenceGeneration = promotion.FenceGeneration
		}
	}
	if former.Action == "" {
		former.Action = action.Kind
	}
	if former.Reason == "" {
		former.Reason = action.Reason
	}
}

func (r *AntflyClusterReconciler) haAdminJobLogBody(ctx context.Context, cluster *antflyv1.AntflyCluster, jobName string) (string, bool) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(cluster.Namespace), client.MatchingLabels{"job-name": jobName}); err != nil {
		log.FromContext(ctx).V(1).Info("Unable to list HA admin job pods", "job", jobName, "error", err)
		return "", false
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase != corev1.PodSucceeded {
			continue
		}
		raw, err := r.KubeClient.CoreV1().Pods(cluster.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{
			Container: "ha-admin",
		}).DoRaw(ctx)
		if err != nil {
			log.FromContext(ctx).V(1).Info("Unable to read HA admin job logs", "job", jobName, "pod", pod.Name, "error", err)
			continue
		}
		return string(raw), true
	}
	return "", false
}

func haPromotionStatusMatches(status *antflyv1.HAPromotionStatus, identity *antflyv1.HAReplicationIdentitySpec, action antflyv1.HAPlannedActionStatus) bool {
	return status != nil &&
		status.OldPrimaryID == identity.CurrentPrimaryID &&
		status.PromotedStandbyID == action.StandbyName &&
		status.ParentTimelineID == identity.TimelineID &&
		status.ParentEpoch == identity.Epoch &&
		status.NewTimelineID == identity.TimelineID+1 &&
		status.NewEpoch == identity.Epoch+1 &&
		status.SwitchLSN == action.TargetLSN &&
		(status.FenceAuthority == "" || status.FenceAuthority == action.FenceAuthority) &&
		status.FenceGeneration == action.FenceGeneration
}

type haPromotionJobResult struct {
	SwitchLSN        uint64
	ParentTimelineID uint64
	ParentEpoch      uint64
	NewTimelineID    uint64
	NewEpoch         uint64
	RequiredLSN      uint64
	ObservedLSN      uint64
	FenceGeneration  uint64
	FenceToken       string
	Forced           bool
	DataLossPossible bool
}

func parseHAPromotionJobResult(body string) (haPromotionJobResult, bool) {
	lines := parseHATableLines(body)
	resultName := lines["result"]
	if resultName != "promote_current_fence" && resultName != "promote" {
		return haPromotionJobResult{}, false
	}

	var result haPromotionJobResult
	var ok bool
	if result.SwitchLSN, ok = parseHAResultUint(lines, "promotion.switch_lsn"); !ok {
		return haPromotionJobResult{}, false
	}
	if result.ParentTimelineID, ok = parseHAResultUint(lines, "promotion.old_identity.timeline_id"); !ok {
		return haPromotionJobResult{}, false
	}
	if result.ParentEpoch, ok = parseHAResultUint(lines, "promotion.old_identity.epoch"); !ok {
		return haPromotionJobResult{}, false
	}
	if result.NewTimelineID, ok = parseHAResultUint(lines, "promotion.new_identity.timeline_id"); !ok {
		return haPromotionJobResult{}, false
	}
	if result.NewEpoch, ok = parseHAResultUint(lines, "promotion.new_identity.epoch"); !ok {
		return haPromotionJobResult{}, false
	}
	if result.FenceGeneration, ok = parseHAResultUint(lines, "fence_generation"); !ok {
		return haPromotionJobResult{}, false
	}
	result.FenceToken = strings.TrimSpace(lines["fence_token"])
	if result.FenceToken == "" {
		return haPromotionJobResult{}, false
	}
	result.RequiredLSN, _ = parseHAResultUint(lines, "assessment.required_lsn")
	receivedLSN, _ := parseHAResultUint(lines, "assessment.received_lsn")
	appliedLSN, _ := parseHAResultUint(lines, "assessment.applied_lsn")
	result.ObservedLSN = receivedLSN
	if appliedLSN > result.ObservedLSN {
		result.ObservedLSN = appliedLSN
	}
	if result.RequiredLSN == 0 {
		result.RequiredLSN = result.SwitchLSN
	}
	if result.ObservedLSN == 0 {
		result.ObservedLSN = result.RequiredLSN
	}
	result.Forced, _ = parseHAResultBool(lines, "promotion.forced")
	result.DataLossPossible, _ = parseHAResultBool(lines, "promotion.data_loss_possible")
	return result, true
}

type haPromotionAPIResultEnvelope struct {
	haPromotionAPIResult
	Result struct {
		PromoteCurrentFence *haPromotionAPIResult `json:"promote_current_fence,omitempty"`
		Promote             *haPromotionAPIResult `json:"promote,omitempty"`
	} `json:"result"`
}

type haPromotionAPIResult struct {
	Assessment struct {
		RequiredLSN uint64 `json:"required_lsn"`
		ReceivedLSN uint64 `json:"received_lsn"`
		AppliedLSN  uint64 `json:"applied_lsn"`
	} `json:"assessment"`
	Promotion struct {
		SwitchLSN   uint64 `json:"switch_lsn"`
		OldIdentity struct {
			TimelineID uint64 `json:"timeline_id"`
			Epoch      uint64 `json:"epoch"`
		} `json:"old_identity"`
		NewIdentity struct {
			TimelineID uint64 `json:"timeline_id"`
			Epoch      uint64 `json:"epoch"`
		} `json:"new_identity"`
		Forced           bool `json:"forced"`
		DataLossPossible bool `json:"data_loss_possible"`
	} `json:"promotion"`
	FenceGeneration uint64 `json:"fence_generation"`
	FenceToken      string `json:"fence_token"`
	Forced          bool   `json:"forced"`
}

func parseHAPromotionAPIResult(raw []byte) (haPromotionJobResult, bool) {
	var envelope haPromotionAPIResultEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return haPromotionJobResult{}, false
	}
	result := &envelope.haPromotionAPIResult
	if result.Promotion.SwitchLSN == 0 {
		result = envelope.Result.PromoteCurrentFence
	}
	if result == nil {
		result = envelope.Result.Promote
	}
	if result == nil || result.Promotion.SwitchLSN == 0 ||
		result.Promotion.OldIdentity.TimelineID == 0 ||
		result.Promotion.OldIdentity.Epoch == 0 ||
		result.Promotion.NewIdentity.TimelineID == 0 ||
		result.Promotion.NewIdentity.Epoch == 0 ||
		result.FenceGeneration == 0 ||
		strings.TrimSpace(result.FenceToken) == "" {
		return haPromotionJobResult{}, false
	}
	observedLSN := result.Assessment.ReceivedLSN
	if result.Assessment.AppliedLSN > observedLSN {
		observedLSN = result.Assessment.AppliedLSN
	}
	requiredLSN := result.Assessment.RequiredLSN
	if requiredLSN == 0 {
		requiredLSN = result.Promotion.SwitchLSN
	}
	if observedLSN == 0 {
		observedLSN = requiredLSN
	}
	return haPromotionJobResult{
		SwitchLSN:        result.Promotion.SwitchLSN,
		ParentTimelineID: result.Promotion.OldIdentity.TimelineID,
		ParentEpoch:      result.Promotion.OldIdentity.Epoch,
		NewTimelineID:    result.Promotion.NewIdentity.TimelineID,
		NewEpoch:         result.Promotion.NewIdentity.Epoch,
		RequiredLSN:      requiredLSN,
		ObservedLSN:      observedLSN,
		FenceGeneration:  result.FenceGeneration,
		FenceToken:       strings.TrimSpace(result.FenceToken),
		Forced:           result.Forced || result.Promotion.Forced,
		DataLossPossible: result.Promotion.DataLossPossible,
	}, true
}

type haRejoinJobResult struct {
	Action            string
	Reason            string
	FormerNodeID      string
	TargetTimelineID  uint64
	TargetEpoch       uint64
	ForkLSN           uint64
	FormerLastLSN     uint64
	RetainedFromLSN   uint64
	DataLossDiscarded bool
}

func parseHARejoinJobResult(body string) (haRejoinJobResult, bool) {
	lines := parseHATableLines(body)
	if lines["result"] != "rejoin_assess" {
		return haRejoinJobResult{}, false
	}
	result := haRejoinJobResult{
		Action:       strings.TrimSpace(lines["action"]),
		Reason:       strings.TrimSpace(lines["reason"]),
		FormerNodeID: strings.TrimSpace(lines["former_node_id"]),
	}
	if result.Action == "" || result.FormerNodeID == "" {
		return haRejoinJobResult{}, false
	}
	var ok bool
	if result.TargetTimelineID, ok = parseHAResultUint(lines, "target_timeline_id"); !ok {
		return haRejoinJobResult{}, false
	}
	if result.TargetEpoch, ok = parseHAResultUint(lines, "target_epoch"); !ok {
		return haRejoinJobResult{}, false
	}
	if result.ForkLSN, ok = parseHAResultUint(lines, "fork_lsn"); !ok {
		return haRejoinJobResult{}, false
	}
	if result.FormerLastLSN, ok = parseHAResultUint(lines, "former_last_lsn"); !ok {
		return haRejoinJobResult{}, false
	}
	if result.RetainedFromLSN, ok = parseHAResultUint(lines, "retained_from_lsn"); !ok {
		return haRejoinJobResult{}, false
	}
	result.DataLossDiscarded, _ = parseHAResultBool(lines, "data_loss_discarded")
	return result, true
}

type haRejoinAPIResultEnvelope struct {
	Assessment haRejoinAPIResult `json:"assessment"`
}

type haRejoinAPIResult struct {
	Action            string `json:"action"`
	Reason            string `json:"reason"`
	FormerNodeID      string `json:"former_node_id"`
	TargetTimelineID  uint64 `json:"target_timeline_id"`
	TargetEpoch       uint64 `json:"target_epoch"`
	ForkLSN           uint64 `json:"fork_lsn"`
	FormerLastLSN     uint64 `json:"former_last_lsn"`
	RetainedFromLSN   uint64 `json:"retained_from_lsn"`
	DataLossDiscarded bool   `json:"data_loss_discarded"`
}

func parseHARejoinAPIResult(raw []byte) (haRejoinJobResult, bool) {
	var envelope haRejoinAPIResultEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return haRejoinJobResult{}, false
	}
	assessment := envelope.Assessment
	if strings.TrimSpace(assessment.Action) == "" ||
		strings.TrimSpace(assessment.FormerNodeID) == "" ||
		assessment.TargetTimelineID == 0 ||
		assessment.TargetEpoch == 0 {
		return haRejoinJobResult{}, false
	}
	return haRejoinJobResult{
		Action:            strings.TrimSpace(assessment.Action),
		Reason:            strings.TrimSpace(assessment.Reason),
		FormerNodeID:      strings.TrimSpace(assessment.FormerNodeID),
		TargetTimelineID:  assessment.TargetTimelineID,
		TargetEpoch:       assessment.TargetEpoch,
		ForkLSN:           assessment.ForkLSN,
		FormerLastLSN:     assessment.FormerLastLSN,
		RetainedFromLSN:   assessment.RetainedFromLSN,
		DataLossDiscarded: assessment.DataLossDiscarded,
	}, true
}

func haRejoinAdminActionResult(result haRejoinJobResult) *antflyv1.HAAdminActionResultStatus {
	status := &antflyv1.HAAdminActionResultStatus{}
	applyHARejoinAdminActionResult(status, result)
	return status
}

func applyHARejoinAdminActionResult(status *antflyv1.HAAdminActionResultStatus, result haRejoinJobResult) {
	if status == nil {
		return
	}
	status.RejoinAction = result.Action
	status.RejoinReason = result.Reason
	status.FormerNodeID = result.FormerNodeID
	status.TargetTimelineID = result.TargetTimelineID
	status.TargetEpoch = result.TargetEpoch
	status.ForkLSN = result.ForkLSN
	status.FormerLastLSN = result.FormerLastLSN
	status.RetainedFromLSN = result.RetainedFromLSN
	status.DataLossDiscarded = result.DataLossDiscarded
}

func applyHARejoinJobResult(former *antflyv1.HAFormerPrimaryStatus, result haRejoinJobResult) {
	if former == nil {
		return
	}
	former.NodeID = result.FormerNodeID
	former.TargetTimelineID = result.TargetTimelineID
	former.TargetEpoch = result.TargetEpoch
	former.ForkLSN = result.ForkLSN
	former.FormerLastLSN = result.FormerLastLSN
	former.RetainedFromLSN = result.RetainedFromLSN
	former.DataLossDiscarded = result.DataLossDiscarded
	former.AssessedAction = result.Action
	former.AssessedReason = result.Reason
	former.ObservedLSN = result.FormerLastLSN
	former.RejoinRequired = true
	former.RewindPossible = false
	former.ReseedRequired = false
	former.Diverged = false
	switch result.Action {
	case "reject_unfenced":
		former.Fenced = false
		former.Action = string(haActionDemoteFormerPrimary)
	case "already_current":
		former.Fenced = true
		former.RejoinRequired = false
		former.Action = "None"
	case "rewind":
		former.Fenced = true
		former.RewindPossible = true
		former.Action = string(haActionRewindFormerPrimary)
	case "reseed":
		former.Fenced = true
		former.ReseedRequired = true
		former.Diverged = true
		former.Action = string(haActionReseedFormerPrimary)
	default:
		former.Action = result.Action
	}
	if result.Reason != "" {
		former.Reason = result.Reason
	}
}

func parseHAResultUint(lines map[string]string, key string) (uint64, bool) {
	raw, ok := lines[key]
	if !ok {
		return 0, false
	}
	value, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	return value, err == nil
}

func parseHAResultBool(lines map[string]string, key string) (bool, bool) {
	raw, ok := lines[key]
	if !ok {
		return false, false
	}
	switch strings.TrimSpace(raw) {
	case "true":
		return true, true
	case "false":
		return false, true
	default:
		return false, false
	}
}

func applyHAPromotionJobResult(promotion *antflyv1.HAPromotionStatus, result haPromotionJobResult) {
	promotion.ParentTimelineID = result.ParentTimelineID
	promotion.ParentEpoch = result.ParentEpoch
	promotion.NewTimelineID = result.NewTimelineID
	promotion.NewEpoch = result.NewEpoch
	promotion.SwitchLSN = result.SwitchLSN
	promotion.RequiredLSN = result.RequiredLSN
	promotion.ObservedLSN = result.ObservedLSN
	promotion.FenceGeneration = result.FenceGeneration
	promotion.FenceToken = result.FenceToken
	promotion.Forced = result.Forced
	promotion.DataLossPossible = result.DataLossPossible
}

func (r *AntflyClusterReconciler) observeHAPrimaryRouteStatus(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	if cluster.Spec.HighAvailability == nil || cluster.Spec.HighAvailability.Mode == "" ||
		cluster.Spec.HighAvailability.Mode == antflyv1.HAModeDisabled {
		return nil
	}

	service := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: cluster.Name + "-public-api", Namespace: cluster.Namespace}, service)
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	target := strings.TrimSpace(service.Annotations[haPrimaryRouteTargetAnnotation])
	if target == "" {
		target = "primary"
	}
	if cluster.Status.HAStatus == nil {
		cluster.Status.HAStatus = &antflyv1.HAStatus{Mode: cluster.Spec.HighAvailability.Mode}
	}
	cluster.Status.HAStatus.PrimaryRoute.CurrentTarget = target
	cluster.Status.HAStatus.PrimaryRoute.FenceAuthority = antflyv1.HAFencingAuthority(strings.TrimSpace(service.Annotations[haPrimaryRouteFenceAuthorityAnnotation]))
	cluster.Status.HAStatus.PrimaryRoute.FenceGeneration = 0
	if raw := strings.TrimSpace(service.Annotations[haPrimaryRouteFenceGenerationAnnotation]); raw != "" {
		if fenceGeneration, err := strconv.ParseUint(raw, 10, 64); err == nil {
			cluster.Status.HAStatus.PrimaryRoute.FenceGeneration = fenceGeneration
		}
	}
	return nil
}

func (r *AntflyClusterReconciler) observeHAPrimaryAdminStatus(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	ha := cluster.Spec.HighAvailability
	if ha == nil || ha.Mode == "" || ha.Mode == antflyv1.HAModeDisabled ||
		ha.Admin == nil || strings.TrimSpace(ha.Admin.PrimaryURL) == "" {
		return nil
	}
	if cluster.Status.HAStatus == nil {
		cluster.Status.HAStatus = &antflyv1.HAStatus{Mode: ha.Mode}
	}
	status, err := r.observeHAPrimaryStatusTyped(ctx, ha.Admin.PrimaryURL, ha)
	if err != nil {
		cluster.Status.HAStatus.PrimaryAdminReachable = false
		cluster.Status.HAStatus.PrimaryAdminLastError = err.Error()
		return err
	}
	cluster.Status.HAStatus.PrimaryAdminReachable = true
	cluster.Status.HAStatus.PrimaryAdminLastError = ""
	cluster.Status.HAStatus.PrimaryLSN = status.PrimaryLSN
	cluster.Status.HAStatus.Retention = status.Retention
	cluster.Status.HAStatus.Standbys = status.Standbys
	return nil
}

func (r *AntflyClusterReconciler) observeHAStandbyAdminStatuses(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	ha := cluster.Spec.HighAvailability
	if ha == nil || ha.Mode == "" || ha.Mode == antflyv1.HAModeDisabled {
		return nil
	}
	if cluster.Status.HAStatus == nil {
		cluster.Status.HAStatus = &antflyv1.HAStatus{Mode: ha.Mode}
	}

	var observedErr error
	for _, standby := range ha.Standbys {
		if !standbyDesired(standby) || strings.TrimSpace(standby.AdminURL) == "" {
			continue
		}
		status, err := r.observeHAStandbyStatusTyped(ctx, standby.AdminURL, standby.Name, standbySlotName(standby), cluster.Status.HAStatus.PrimaryLSN)
		if err != nil {
			if observedErr == nil {
				observedErr = fmt.Errorf("standby %s: %w", standby.Name, err)
			}
			continue
		}
		mergeHAStandbyStatus(cluster.Status.HAStatus, status)
	}
	return observedErr
}

func (r *AntflyClusterReconciler) observeHAPrimaryStatusTyped(ctx context.Context, baseURL string, ha *antflyv1.HighAvailabilitySpec) (haObservedPrimaryStatus, error) {
	raw, err := r.getHAAdminJSON(ctx, baseURL, "/admin/v1/ha/primary/status", haPrimaryStatusQuery(ha))
	if err != nil {
		return haObservedPrimaryStatus{}, err
	}
	return parseHAPrimaryStatusJSON(raw)
}

func (r *AntflyClusterReconciler) observeHAStandbyStatusTyped(ctx context.Context, baseURL string, standbyName string, slotName string, upstreamLSN uint64) (antflyv1.HAStandbyStatus, error) {
	raw, err := r.getHAAdminJSON(ctx, baseURL, "/admin/v1/ha/standby/status", haStandbyStatusQuery(upstreamLSN))
	if err != nil {
		return antflyv1.HAStandbyStatus{}, err
	}
	return parseHAStandbyStatusJSON(raw, standbyName, slotName)
}

func (r *AntflyClusterReconciler) getHAAdminJSON(ctx context.Context, baseURL string, apiPath string, query url.Values) ([]byte, error) {
	endpoint := strings.TrimRight(baseURL, "/") + apiPath
	if encoded := query.Encode(); encoded != "" {
		endpoint += "?" + encoded
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := r.httpClient().Do(req) //nolint:gosec // HA admin URL is explicitly configured for the cluster by the operator user.
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HA admin API returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return raw, nil
}

func (r *AntflyClusterReconciler) postHAAdminJSON(ctx context.Context, baseURL string, apiPath string, body any) error {
	_, err := r.postHAAdminJSONRaw(ctx, baseURL, apiPath, body)
	return err
}

func (r *AntflyClusterReconciler) postHAAdminJSONRaw(ctx context.Context, baseURL string, apiPath string, body any) ([]byte, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return r.requestHAAdminJSONRaw(ctx, http.MethodPost, baseURL, apiPath, raw)
}

func (r *AntflyClusterReconciler) requestHAAdminNoBody(ctx context.Context, method string, baseURL string, apiPath string) error {
	_, err := r.requestHAAdminJSONRaw(ctx, method, baseURL, apiPath, nil)
	return err
}

func (r *AntflyClusterReconciler) requestHAAdminJSONRaw(ctx context.Context, method string, baseURL string, apiPath string, body []byte) ([]byte, error) {
	endpoint := strings.TrimRight(baseURL, "/") + apiPath
	var reader io.Reader
	if body != nil {
		reader = strings.NewReader(string(body))
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := r.httpClient().Do(req) //nolint:gosec // HA admin URL is explicitly configured for the cluster by the operator user.
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HA admin API returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return raw, nil
}

func haPrimaryStatusQuery(ha *antflyv1.HighAvailabilitySpec) url.Values {
	query := url.Values{}
	if ha == nil {
		return query
	}
	if ha.Retention != nil && ha.Retention.MaxLagLSN > 0 {
		query.Set("max_lag_lsn", strconv.FormatUint(ha.Retention.MaxLagLSN, 10))
	}
	if ha.SyncPolicy != nil && ha.SyncPolicy.Mode != "" && ha.SyncPolicy.Mode != antflyv1.HADurabilityModeAsync {
		query.Set("sync_mode", haDurabilityModeCLI(ha.SyncPolicy.Mode))
		if ha.SyncPolicy.Selection != "" {
			query.Set("sync_selection", haStandbySelectionCLI(ha.SyncPolicy.Selection))
		}
		if ha.SyncPolicy.Required > 0 {
			query.Set("sync_required", strconv.FormatInt(int64(ha.SyncPolicy.Required), 10))
		}
		for _, name := range ha.SyncPolicy.StandbyNames {
			if strings.TrimSpace(name) != "" {
				query.Add("sync_standby", name)
			}
		}
		if ha.SyncPolicy.FailurePolicy != "" {
			query.Set("sync_failure", haFailurePolicyCLI(ha.SyncPolicy.FailurePolicy))
		}
	}
	return query
}

func haStandbyStatusQuery(upstreamLSN uint64) url.Values {
	query := url.Values{}
	if upstreamLSN > 0 {
		query.Set("upstream_lsn", strconv.FormatUint(upstreamLSN, 10))
	}
	return query
}

func haDurabilityModeCLI(mode antflyv1.HADurabilityMode) string {
	switch mode {
	case antflyv1.HADurabilityModeRemoteWrite:
		return "remote-write"
	case antflyv1.HADurabilityModeRemoteApply:
		return "remote-apply"
	default:
		return "async"
	}
}

func haStandbySelectionCLI(selection antflyv1.HAStandbySelection) string {
	switch selection {
	case antflyv1.HAStandbySelectionFirst:
		return "first"
	case antflyv1.HAStandbySelectionAll:
		return "all"
	default:
		return "any"
	}
}

func haFailurePolicyCLI(policy antflyv1.HAFailurePolicy) string {
	switch policy {
	case antflyv1.HAFailurePolicyFailClosed:
		return "fail-closed"
	case antflyv1.HAFailurePolicyDegradeToAsync:
		return "degrade-to-async"
	default:
		return "block"
	}
}

type haObservedPrimaryStatus struct {
	PrimaryLSN uint64
	Retention  antflyv1.HARetentionStatus
	Standbys   []antflyv1.HAStandbyStatus
}

type haAdminStatusJSON struct {
	Result struct {
		PrimaryStatus *haPrimaryStatusJSON `json:"primary_status,omitempty"`
		StandbyStatus *haStandbyStatusJSON `json:"standby_status,omitempty"`
	} `json:"result"`
}

type haPrimaryStatusEnvelopeJSON struct {
	Snapshot *haPrimaryStatusJSON `json:"snapshot,omitempty"`
}

type haStandbyStatusEnvelopeJSON struct {
	Snapshot *haStandbyStatusJSON `json:"snapshot,omitempty"`
}

type haPrimaryStatusJSON struct {
	CurrentLSN *uint64 `json:"current_lsn"`
	Retention  struct {
		OldestRestartLSN  uint64 `json:"oldest_restart_lsn"`
		RetainedLSNCount  uint64 `json:"retained_lsn_count"`
		ActiveSlots       int32  `json:"active_slots"`
		ReseedRecommended int32  `json:"reseed_recommended"`
	} `json:"retention"`
	Slots []struct {
		Name            string `json:"name"`
		TimelineID      uint64 `json:"timeline_id"`
		Active          bool   `json:"active"`
		ReseedRequired  bool   `json:"reseed_required"`
		RestartLSN      uint64 `json:"restart_lsn"`
		ReceivedLSN     uint64 `json:"received_lsn"`
		AppliedLSN      uint64 `json:"applied_lsn"`
		SafeReadLSN     uint64 `json:"safe_read_lsn"`
		WriteLagLSN     uint64 `json:"write_lag_lsn"`
		ApplyLagLSN     uint64 `json:"apply_lag_lsn"`
		SafeReadLagLSN  uint64 `json:"safe_read_lag_lsn"`
		RetentionLagLSN uint64 `json:"retention_lag_lsn"`
		Status          string `json:"status"`
		LastError       string `json:"last_error"`
	} `json:"slots"`
}

type haStandbyStatusJSON struct {
	Identity struct {
		TimelineID uint64 `json:"timeline_id"`
	} `json:"identity"`
	ReceivedLSN        *uint64 `json:"received_lsn"`
	AppliedLSN         *uint64 `json:"applied_lsn"`
	SafeReadLSN        uint64  `json:"safe_read_lsn"`
	UpstreamLSN        *uint64 `json:"upstream_lsn"`
	WriteLagLSN        *uint64 `json:"write_lag_lsn"`
	ReceiveLagLSN      *uint64 `json:"receive_lag_lsn"`
	ApplyLagLSN        *uint64 `json:"apply_lag_lsn"`
	UnappliedLSNCount  uint64  `json:"unapplied_lsn_count"`
	CaughtUpToReceived bool    `json:"caught_up_to_received"`
	CanServeSafeReads  bool    `json:"can_serve_safe_reads"`
}

func parseHAPrimaryStatusJSON(raw []byte) (haObservedPrimaryStatus, error) {
	var direct haPrimaryStatusEnvelopeJSON
	if err := json.Unmarshal(raw, &direct); err != nil {
		return haObservedPrimaryStatus{}, err
	}
	snapshot := direct.Snapshot
	if snapshot == nil {
		var doc haAdminStatusJSON
		if err := json.Unmarshal(raw, &doc); err != nil {
			return haObservedPrimaryStatus{}, err
		}
		snapshot = doc.Result.PrimaryStatus
	}
	if snapshot == nil {
		return haObservedPrimaryStatus{}, fmt.Errorf("missing primary status snapshot")
	}
	if snapshot.CurrentLSN == nil {
		return haObservedPrimaryStatus{}, fmt.Errorf("missing current_lsn")
	}
	status := haObservedPrimaryStatus{
		PrimaryLSN: *snapshot.CurrentLSN,
		Retention: antflyv1.HARetentionStatus{
			OldestRestartLSN:  snapshot.Retention.OldestRestartLSN,
			RetainedLSNCount:  snapshot.Retention.RetainedLSNCount,
			ActiveSlots:       snapshot.Retention.ActiveSlots,
			ReseedRecommended: snapshot.Retention.ReseedRecommended,
		},
	}
	for _, slot := range snapshot.Slots {
		if strings.TrimSpace(slot.Name) == "" {
			continue
		}
		status.Standbys = append(status.Standbys, antflyv1.HAStandbyStatus{
			Name:           slot.Name,
			SlotName:       slot.Name,
			TimelineID:     slot.TimelineID,
			Active:         slot.Active,
			ReseedRequired: slot.ReseedRequired,
			RestartLSN:     slot.RestartLSN,
			ReceivedLSN:    slot.ReceivedLSN,
			AppliedLSN:     slot.AppliedLSN,
			SafeReadLSN:    slot.SafeReadLSN,
			WriteLagLSN:    slot.WriteLagLSN,
			ApplyLagLSN:    slot.ApplyLagLSN,
			SafeReadLagLSN: slot.SafeReadLagLSN,
			Status:         strings.TrimSpace(slot.Status),
			LastError:      strings.TrimSpace(slot.LastError),
		})
	}
	return status, nil
}

func parseHAStandbyStatusJSON(raw []byte, standbyName string, slotName string) (antflyv1.HAStandbyStatus, error) {
	var direct haStandbyStatusEnvelopeJSON
	if err := json.Unmarshal(raw, &direct); err != nil {
		return antflyv1.HAStandbyStatus{}, err
	}
	snapshot := direct.Snapshot
	if snapshot == nil {
		var doc haAdminStatusJSON
		if err := json.Unmarshal(raw, &doc); err != nil {
			return antflyv1.HAStandbyStatus{}, err
		}
		snapshot = doc.Result.StandbyStatus
	}
	if snapshot == nil {
		return antflyv1.HAStandbyStatus{}, fmt.Errorf("missing standby status snapshot")
	}
	status := antflyv1.HAStandbyStatus{
		Name:               standbyName,
		SlotName:           slotName,
		Active:             true,
		TimelineID:         snapshot.Identity.TimelineID,
		SafeReadLSN:        snapshot.SafeReadLSN,
		UnappliedLSNCount:  snapshot.UnappliedLSNCount,
		CaughtUpToReceived: snapshot.CaughtUpToReceived,
		CanServeSafeReads:  snapshot.CanServeSafeReads,
	}
	if snapshot.ReceivedLSN == nil {
		return antflyv1.HAStandbyStatus{}, fmt.Errorf("missing received_lsn")
	}
	status.ReceivedLSN = *snapshot.ReceivedLSN
	if snapshot.AppliedLSN == nil {
		return antflyv1.HAStandbyStatus{}, fmt.Errorf("missing applied_lsn")
	}
	status.AppliedLSN = *snapshot.AppliedLSN
	if snapshot.UpstreamLSN != nil {
		status.UpstreamLSN = *snapshot.UpstreamLSN
	}
	if snapshot.WriteLagLSN != nil {
		status.WriteLagLSN = *snapshot.WriteLagLSN
	}
	if snapshot.ReceiveLagLSN != nil {
		status.ReceiveLagLSN = *snapshot.ReceiveLagLSN
	}
	if snapshot.ApplyLagLSN != nil {
		status.ApplyLagLSN = *snapshot.ApplyLagLSN
	}
	if status.ReceiveLagLSN > 0 || status.ApplyLagLSN > 0 {
		status.Status = "lagging"
	} else if status.CanServeSafeReads && status.Status == "" {
		status.Status = "healthy"
	}
	return status, nil
}

func mergeHAStandbyStatus(status *antflyv1.HAStatus, observed antflyv1.HAStandbyStatus) {
	if status == nil || observed.Name == "" {
		return
	}
	for i := range status.Standbys {
		existing := &status.Standbys[i]
		if existing.Name != observed.Name && existing.SlotName != observed.SlotName {
			continue
		}
		if observed.TimelineID != 0 {
			existing.TimelineID = observed.TimelineID
		}
		existing.Active = existing.Active || observed.Active
		if observed.ReceivedLSN != 0 {
			existing.ReceivedLSN = observed.ReceivedLSN
		}
		if observed.AppliedLSN != 0 {
			existing.AppliedLSN = observed.AppliedLSN
		}
		if observed.SafeReadLSN != 0 {
			existing.SafeReadLSN = observed.SafeReadLSN
		}
		if observed.UpstreamLSN != 0 {
			existing.UpstreamLSN = observed.UpstreamLSN
		}
		existing.WriteLagLSN = observed.WriteLagLSN
		existing.ReceiveLagLSN = observed.ReceiveLagLSN
		existing.ApplyLagLSN = observed.ApplyLagLSN
		existing.UnappliedLSNCount = observed.UnappliedLSNCount
		existing.CaughtUpToReceived = observed.CaughtUpToReceived
		existing.CanServeSafeReads = observed.CanServeSafeReads
		if observed.Status != "" {
			existing.Status = observed.Status
		}
		return
	}
	status.Standbys = append(status.Standbys, observed)
}

func parseHATableLines(body string) map[string]string {
	lines := map[string]string{}
	for _, line := range strings.Split(body, "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok || key == "" {
			continue
		}
		lines[key] = value
	}
	return lines
}

func (r *AntflyClusterReconciler) reconcileHAPrimaryRoute(ctx context.Context, cluster *antflyv1.AntflyCluster) error {
	if cluster.Spec.HighAvailability == nil || cluster.Spec.HighAvailability.Mode == "" ||
		cluster.Spec.HighAvailability.Mode == antflyv1.HAModeDisabled ||
		cluster.Status.HAStatus == nil {
		return nil
	}
	for i, action := range cluster.Status.HAStatus.PlannedActions {
		if action.Kind != string(haActionUpdatePrimaryRoute) {
			continue
		}
		if !haPlannedActionDependenciesSucceeded(cluster.Status.HAStatus.PlannedActions, i) {
			return nil
		}
		if action.RouteTo == "" {
			return nil
		}
		return r.updateHAPrimaryRouteService(ctx, cluster, action)
	}
	return nil
}

func (r *AntflyClusterReconciler) updateHAPrimaryRouteService(ctx context.Context, cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) error {
	service := &corev1.Service{}
	if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name + "-public-api", Namespace: cluster.Namespace}, service); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	patch := client.MergeFrom(service.DeepCopy())
	if service.Annotations == nil {
		service.Annotations = map[string]string{}
	}
	service.Annotations[haPrimaryRouteTargetAnnotation] = action.RouteTo
	if action.FenceAuthority != "" {
		service.Annotations[haPrimaryRouteFenceAuthorityAnnotation] = string(action.FenceAuthority)
	} else {
		delete(service.Annotations, haPrimaryRouteFenceAuthorityAnnotation)
	}
	if action.FenceGeneration > 0 {
		service.Annotations[haPrimaryRouteFenceGenerationAnnotation] = strconv.FormatUint(action.FenceGeneration, 10)
	} else {
		delete(service.Annotations, haPrimaryRouteFenceGenerationAnnotation)
	}
	if selector, ok := haPublicAPISelector(cluster, effectiveTopologyMode(cluster) == topologyModeSwarm, action.RouteTo); ok {
		service.Spec.Selector = selector
		service.Annotations[haPrimaryRouteSelectorAnnotation] = "true"
	} else {
		service.Annotations[haPrimaryRouteSelectorAnnotation] = "false"
	}
	if err := r.Patch(ctx, service, patch); err != nil {
		return err
	}
	cluster.Status.HAStatus.PrimaryRoute.CurrentTarget = action.RouteTo
	cluster.Status.HAStatus.PrimaryRoute.FenceAuthority = action.FenceAuthority
	cluster.Status.HAStatus.PrimaryRoute.FenceGeneration = action.FenceGeneration
	cluster.Status.HAStatus.PrimaryRoute.Stale = false
	cluster.Status.HAStatus.PrimaryRoute.Action = "None"
	cluster.Status.HAStatus.PrimaryRoute.Reason = "PrimaryRouteCurrent"
	return nil
}

func haPriorAdminActionsSucceeded(actions []antflyv1.HAPlannedActionStatus) bool {
	for _, action := range actions {
		if len(action.AdminCommand) == 0 {
			continue
		}
		if !haAdminActionSucceededWithEvidence(action) {
			return false
		}
	}
	return true
}

func haPlannedActionDependenciesSucceeded(actions []antflyv1.HAPlannedActionStatus, index int) bool {
	if index < 0 || index >= len(actions) {
		return false
	}
	action := actions[index]
	if action.DependsOn == "" {
		return haPriorAdminActionsSucceeded(actions[:index])
	}
	for i := 0; i < index; i++ {
		dependency := actions[i]
		if dependency.Kind != action.DependsOn {
			continue
		}
		if len(dependency.AdminCommand) == 0 {
			return true
		}
		return haAdminActionSucceededWithEvidence(dependency)
	}
	return false
}

func haAdminActionSucceededWithEvidence(action antflyv1.HAPlannedActionStatus) bool {
	if action.AdminJobPhase != haAdminJobPhaseSucceeded {
		return false
	}
	if !haActionRequiresAdminResult(haActionKind(action.Kind)) {
		return true
	}
	return haActionHasRequiredAdminResult(action)
}

func haActionRequiresAdminResult(kind haActionKind) bool {
	switch kind {
	case haActionCreateSlot,
		haActionResumeSlot,
		haActionPauseSlot,
		haActionDropSlot,
		haActionSeedStandby,
		haActionFinishStandbySeed,
		haActionBootstrapStandbySeed,
		haActionMarkReseed,
		haActionAcquireFence,
		haActionPromoteStandby,
		haActionDemoteFormerPrimary,
		haActionRewindFormerPrimary,
		haActionReseedFormerPrimary:
		return true
	default:
		return false
	}
}

func haActionHasRequiredAdminResult(action antflyv1.HAPlannedActionStatus) bool {
	result := action.AdminResult
	if result == nil {
		return false
	}
	expectedSlotName := haActionSlotName(action)
	switch haActionKind(action.Kind) {
	case haActionCreateSlot:
		return result.SlotAction == "create" && haResultSlotNameMatches(result.SlotName, expectedSlotName)
	case haActionResumeSlot:
		return result.SlotAction == "resume" && haResultSlotNameMatches(result.SlotName, expectedSlotName)
	case haActionPauseSlot:
		return result.SlotAction == "pause" && haResultSlotNameMatches(result.SlotName, expectedSlotName)
	case haActionDropSlot:
		return result.SlotAction == "drop" && haResultSlotNameMatches(result.SlotName, expectedSlotName)
	case haActionSeedStandby, haActionMarkReseed:
		return haResultSlotNameMatches(result.SlotName, expectedSlotName) &&
			haResultManifestMatches(result.ManifestID, haExpectedSeedBeginManifestID(action, expectedSlotName)) &&
			result.BackupLSN > 0 &&
			result.StartRecordLSN > 0
	case haActionFinishStandbySeed:
		return result.ManifestID != "" &&
			result.BackupLSN > 0 &&
			result.EndRecordLSN > 0
	case haActionBootstrapStandbySeed:
		return result.ManifestID != "" &&
			result.BackupLSN > 0 &&
			result.CheckpointLSN > 0
	case haActionAcquireFence:
		return result.FenceGeneration > 0 &&
			(action.FenceGeneration == 0 || result.FenceGeneration == action.FenceGeneration) &&
			result.FenceToken != ""
	case haActionPromoteStandby:
		return result.FenceGeneration > 0 &&
			(action.FenceGeneration == 0 || result.FenceGeneration == action.FenceGeneration) &&
			result.FenceToken != ""
	case haActionDemoteFormerPrimary, haActionRewindFormerPrimary, haActionReseedFormerPrimary:
		return haRejoinResultMatchesRequiredAdminResult(action, result)
	default:
		return true
	}
}

func haRejoinResultMatchesRequiredAdminResult(action antflyv1.HAPlannedActionStatus, result *antflyv1.HAAdminActionResultStatus) bool {
	if result == nil ||
		strings.TrimSpace(result.RejoinAction) == "" ||
		strings.TrimSpace(result.FormerNodeID) == "" ||
		result.TargetTimelineID == 0 ||
		result.TargetEpoch == 0 {
		return false
	}
	switch haActionKind(action.Kind) {
	case haActionDemoteFormerPrimary:
		if result.RejoinAction != "reject_unfenced" {
			return false
		}
	case haActionRewindFormerPrimary:
		if result.RejoinAction != "rewind" {
			return false
		}
	case haActionReseedFormerPrimary:
		if result.RejoinAction != "reseed" {
			return false
		}
	default:
		return false
	}
	if action.StandbyName != "" && result.FormerNodeID != action.StandbyName {
		return false
	}
	if action.TargetLSN > 0 && result.ForkLSN != action.TargetLSN {
		return false
	}
	if action.ObservedLSN > 0 && result.FormerLastLSN != action.ObservedLSN {
		return false
	}
	if action.RetainedFromLSN > 0 && result.RetainedFromLSN != action.RetainedFromLSN {
		return false
	}
	return true
}

func haResultSlotNameMatches(resultSlotName string, expectedSlotName string) bool {
	resultSlotName = strings.TrimSpace(resultSlotName)
	if resultSlotName == "" {
		return false
	}
	expectedSlotName = strings.TrimSpace(expectedSlotName)
	return expectedSlotName == "" || resultSlotName == expectedSlotName
}

func haResultManifestMatches(resultManifestID string, expectedManifestID string) bool {
	resultManifestID = strings.TrimSpace(resultManifestID)
	if resultManifestID == "" {
		return false
	}
	expectedManifestID = strings.TrimSpace(expectedManifestID)
	return expectedManifestID == "" || resultManifestID == expectedManifestID
}

func haExpectedSeedBeginManifestID(action antflyv1.HAPlannedActionStatus, slotName string) string {
	for i := 0; i+1 < len(action.AdminCommand); i++ {
		if action.AdminCommand[i] == "--manifest-id" {
			return strings.TrimSpace(action.AdminCommand[i+1])
		}
	}
	if strings.TrimSpace(slotName) == "" || action.TargetLSN == 0 {
		return ""
	}
	return fmt.Sprintf("base-%s-%d", strings.TrimSpace(slotName), action.TargetLSN)
}

func (r *AntflyClusterReconciler) updateHAAdminJobExecutionCondition(cluster *antflyv1.AntflyCluster) {
	if cluster.Status.HAStatus == nil {
		return
	}
	for _, action := range cluster.Status.HAStatus.PlannedActions {
		if action.AdminJobPhase != haAdminJobPhaseFailed {
			continue
		}
		jobName := action.AdminJobName
		if jobName == "" {
			jobName = "unknown"
		}
		setHACondition(
			cluster,
			antflyv1.TypeHADegraded,
			metav1.ConditionTrue,
			"HAAdminJobFailed",
			fmt.Sprintf("HA admin action %s failed in Job %s", action.Kind, jobName),
		)
		return
	}
	for _, action := range cluster.Status.HAStatus.PlannedActions {
		if action.AdminJobPhase != haAdminJobPhaseSucceeded ||
			!haActionRequiresAdminResult(haActionKind(action.Kind)) ||
			haActionHasRequiredAdminResult(action) {
			continue
		}
		jobName := action.AdminJobName
		if jobName == "" {
			jobName = "unknown"
		}
		setHACondition(
			cluster,
			antflyv1.TypeHADegraded,
			metav1.ConditionTrue,
			antflyv1.ReasonHAAdminResultMissing,
			fmt.Sprintf("HA admin action %s succeeded in Job %s, but the operator has not observed typed result evidence; dependent HA actions remain blocked", action.Kind, jobName),
		)
		return
	}

	identity := haReplicationIdentity(cluster.Spec.HighAvailability)
	if identity == nil {
		return
	}
	for i, action := range cluster.Status.HAStatus.PlannedActions {
		if action.Kind != string(haActionPromoteStandby) ||
			action.AdminJobPhase != haAdminJobPhaseSucceeded ||
			action.StandbyName == "" {
			continue
		}
		if !haPlannedActionDependenciesSucceeded(cluster.Status.HAStatus.PlannedActions, i) {
			continue
		}
		if !haPromotionStatusMatches(cluster.Status.HAStatus.LastPromotion, identity, action) {
			continue
		}
		if haPromotionReceipt(cluster.Status.HAStatus) != nil {
			continue
		}
		jobName := action.AdminJobName
		if jobName == "" {
			jobName = "unknown"
		}
		setHACondition(
			cluster,
			antflyv1.TypeHADegraded,
			metav1.ConditionTrue,
			antflyv1.ReasonHAPromotionReceiptMissing,
			fmt.Sprintf("HA promotion action %s succeeded in Job %s, but the operator has not observed a complete promotion receipt with a fence token; former-primary rejoin remains blocked", action.Kind, jobName),
		)
		return
	}
}

func buildHAAdminJob(cluster *antflyv1.AntflyCluster, admin *antflyv1.HAAdminSpec, action antflyv1.HAPlannedActionStatus) *batchv1.Job {
	args := append([]string{"ha", "--ha-url", action.AdminURL, "--"}, action.AdminCommand...)
	labels := haAdminJobLabels(cluster, action)
	annotations := map[string]string{
		"antfly.io/ha-action-kind":  action.Kind,
		"antfly.io/ha-admin-url":    action.AdminURL,
		"antfly.io/ha-command-hash": haAdminActionHash(action),
	}
	if action.DependsOn != "" {
		annotations["antfly.io/ha-action-depends-on"] = action.DependsOn
	}
	if action.AdminMethod != "" {
		annotations["antfly.io/ha-admin-method"] = action.AdminMethod
	}
	if action.AdminPath != "" {
		annotations["antfly.io/ha-admin-path"] = action.AdminPath
	}
	deadlineSeconds := haAdminJobTimeoutSeconds(admin)
	backoffLimit := haAdminJobBackoffLimit(admin)
	ttlSecondsAfterFinished := haAdminJobTTLSecondsAfterFinished(admin)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        haAdminJobName(cluster, action),
			Namespace:   cluster.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds:   &deadlineSeconds,
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttlSecondsAfterFinished,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: cluster.Spec.ServiceAccountName,
					RestartPolicy:      corev1.RestartPolicyOnFailure,
					SecurityContext:    antflyPodSecurityContext(),
					Containers: []corev1.Container{{
						Name:            "ha-admin",
						Image:           cluster.Spec.Image,
						ImagePullPolicy: corev1.PullPolicy(cluster.Spec.ImagePullPolicy),
						Command:         []string{"/antfly"},
						Args:            args,
						EnvFrom:         append([]corev1.EnvFromSource{}, admin.EnvFrom...),
						VolumeMounts:    append([]corev1.VolumeMount{}, admin.VolumeMounts...),
					}},
					Volumes: append([]corev1.Volume{}, admin.Volumes...),
				},
			},
		},
	}
}

func haAdminJobBackoffLimit(admin *antflyv1.HAAdminSpec) int32 {
	if admin != nil && admin.JobBackoffLimit != nil {
		return *admin.JobBackoffLimit
	}
	return 3
}

func haAdminJobTimeoutSeconds(admin *antflyv1.HAAdminSpec) int64 {
	if admin != nil && admin.JobTimeoutSeconds != nil {
		return *admin.JobTimeoutSeconds
	}
	return 600
}

func haAdminJobTTLSecondsAfterFinished(admin *antflyv1.HAAdminSpec) int32 {
	if admin != nil && admin.JobTTLSecondsAfterFinished != nil {
		return *admin.JobTTLSecondsAfterFinished
	}
	return 86400
}

func haAdminJobLabels(cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) map[string]string {
	labels := podLabels(cluster, "ha-admin")
	labels["antfly.io/ha-action-kind"] = strings.ToLower(action.Kind)
	if action.StandbyName != "" {
		labels["antfly.io/ha-standby"] = action.StandbyName
	}
	return labels
}

func haAdminJobComplete(job *batchv1.Job) bool {
	return haAdminJobPhase(job) == haAdminJobPhaseSucceeded
}

func haAdminJobPhase(job *batchv1.Job) string {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return haAdminJobPhaseFailed
		}
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			return haAdminJobPhaseSucceeded
		}
	}
	if job.Status.Active > 0 {
		return haAdminJobPhaseRunning
	}
	return haAdminJobPhasePending
}

func haAdminJobName(cluster *antflyv1.AntflyCluster, action antflyv1.HAPlannedActionStatus) string {
	hash := haAdminActionHash(action)[:10]
	kind := strings.ToLower(action.Kind)
	base := fmt.Sprintf("%s-ha-%s", cluster.Name, kind)
	maxBaseLen := 63 - len(hash) - 1
	if len(base) > maxBaseLen {
		base = strings.TrimRight(base[:maxBaseLen], "-")
	}
	if base == "" {
		base = "ha"
	}
	return fmt.Sprintf("%s-%s", base, hash)
}

func haAdminActionHash(action antflyv1.HAPlannedActionStatus) string {
	rendered, err := json.Marshal(struct {
		Kind         string   `json:"kind"`
		Phase        string   `json:"phase,omitempty"`
		Executor     string   `json:"executor,omitempty"`
		DependsOn    string   `json:"dependsOn,omitempty"`
		StandbyName  string   `json:"standbyName,omitempty"`
		SlotName     string   `json:"slotName,omitempty"`
		TargetLSN    uint64   `json:"targetLSN,omitempty"`
		ObservedLSN  uint64   `json:"observedLSN,omitempty"`
		RetainedLSN  uint64   `json:"retainedFromLSN,omitempty"`
		RouteFrom    string   `json:"routeFrom,omitempty"`
		RouteTo      string   `json:"routeTo,omitempty"`
		FenceAuth    string   `json:"fenceAuthority,omitempty"`
		FenceHolder  string   `json:"fenceHolder,omitempty"`
		FenceGen     uint64   `json:"fenceGeneration,omitempty"`
		FenceReason  string   `json:"fenceReason,omitempty"`
		SeedManifest string   `json:"seedManifestPath,omitempty"`
		SeedRoot     string   `json:"seedContentRoot,omitempty"`
		AdminURL     string   `json:"adminURL,omitempty"`
		AdminCommand []string `json:"adminCommand,omitempty"`
		Reason       string   `json:"reason,omitempty"`
	}{
		Kind:         action.Kind,
		Phase:        action.Phase,
		Executor:     action.Executor,
		DependsOn:    action.DependsOn,
		StandbyName:  action.StandbyName,
		SlotName:     action.SlotName,
		TargetLSN:    action.TargetLSN,
		ObservedLSN:  action.ObservedLSN,
		RetainedLSN:  action.RetainedFromLSN,
		RouteFrom:    action.RouteFrom,
		RouteTo:      action.RouteTo,
		FenceAuth:    string(action.FenceAuthority),
		FenceHolder:  action.FenceHolder,
		FenceGen:     action.FenceGeneration,
		FenceReason:  action.FenceReason,
		SeedManifest: action.SeedManifestPath,
		SeedRoot:     action.SeedContentRoot,
		AdminURL:     action.AdminURL,
		AdminCommand: action.AdminCommand,
		Reason:       action.Reason,
	})
	if err != nil {
		rendered = []byte(fmt.Sprintf("%s/%s/%s/%d/%s/%d/%s/%s/%v/%s", action.Kind, action.StandbyName, action.SlotName, action.TargetLSN, action.FenceAuthority, action.FenceGeneration, action.FenceHolder, action.AdminURL, action.AdminCommand, action.Reason))
	}
	sum := sha256.Sum256(rendered)
	return fmt.Sprintf("%x", sum)
}

func (r *AntflyClusterReconciler) updateRolloutCondition(cluster *antflyv1.AntflyCluster, statefulSets ...*appsv1.StatefulSet) {
	var waiting []string
	var blocked []string
	for _, sts := range statefulSets {
		if sts == nil || sts.Name == "" {
			waiting = append(waiting, "StatefulSet is not observed yet")
			continue
		}
		replicas := int32(1)
		if sts.Spec.Replicas != nil {
			replicas = *sts.Spec.Replicas
		}
		if sts.Generation > sts.Status.ObservedGeneration {
			waiting = append(waiting, fmt.Sprintf("%s observedGeneration %d is behind generation %d", sts.Name, sts.Status.ObservedGeneration, sts.Generation))
			continue
		}
		if sts.Status.UpdatedReplicas < replicas {
			message := fmt.Sprintf("%s has %d/%d updated replicas", sts.Name, sts.Status.UpdatedReplicas, replicas)
			if statefulSetRolloutAppearsBlocked(sts, replicas) {
				blocked = append(blocked, message)
			} else {
				waiting = append(waiting, message)
			}
			continue
		}
		if sts.Status.ReadyReplicas < replicas {
			waiting = append(waiting, fmt.Sprintf("%s has %d/%d ready replicas", sts.Name, sts.Status.ReadyReplicas, replicas))
		}
	}

	condition := metav1.Condition{
		Type:               antflyv1.TypeRollout,
		ObservedGeneration: cluster.Generation,
	}
	if len(blocked) > 0 {
		condition.Status = metav1.ConditionFalse
		condition.Reason = antflyv1.ReasonRolloutBlocked
		condition.Message = strings.Join(blocked, "; ")
	} else if len(waiting) > 0 {
		condition.Status = metav1.ConditionUnknown
		condition.Reason = antflyv1.ReasonRolloutInProgress
		condition.Message = strings.Join(waiting, "; ")
	} else {
		condition.Status = metav1.ConditionTrue
		condition.Reason = antflyv1.ReasonRolloutComplete
		condition.Message = "All StatefulSet replicas are updated and ready"
	}

	meta.SetStatusCondition(&cluster.Status.Conditions, condition)
}

func statefulSetRolloutAppearsBlocked(sts *appsv1.StatefulSet, replicas int32) bool {
	if sts == nil || replicas <= 0 {
		return false
	}
	if sts.Status.UpdateRevision == "" || sts.Status.CurrentRevision == "" || sts.Status.UpdateRevision == sts.Status.CurrentRevision {
		return false
	}
	return sts.Status.UpdatedReplicas == 0 && sts.Status.ReadyReplicas == 0
}

func (r *AntflyClusterReconciler) repairBlockedStatefulSetRollouts(ctx context.Context, cluster *antflyv1.AntflyCluster) (bool, error) {
	if effectiveTopologyMode(cluster) == topologyModeSwarm {
		swarmSts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name + "-swarm", Namespace: cluster.Namespace}, swarmSts); err != nil {
			if !errors.IsNotFound(err) {
				return false, err
			}
			return false, nil
		}
		return r.repairBlockedStatefulSetRollout(ctx, cluster, swarmSts, "swarm")
	}

	metadataSts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name + "-metadata", Namespace: cluster.Namespace}, metadataSts); err != nil {
		if !errors.IsNotFound(err) {
			return false, err
		}
	} else if repaired, err := r.repairBlockedStatefulSetRollout(ctx, cluster, metadataSts, "metadata"); err != nil || repaired {
		return repaired, err
	}

	dataSts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name + "-data", Namespace: cluster.Namespace}, dataSts); err != nil {
		if !errors.IsNotFound(err) {
			return false, err
		}
	} else if repaired, err := r.repairBlockedStatefulSetRollout(ctx, cluster, dataSts, "data"); err != nil || repaired {
		return repaired, err
	}

	return false, nil
}

func (r *AntflyClusterReconciler) repairBlockedStatefulSetRollout(ctx context.Context, cluster *antflyv1.AntflyCluster, sts *appsv1.StatefulSet, component string) (bool, error) {
	if sts == nil || sts.Name == "" || sts.Status.UpdateRevision == "" {
		return false, nil
	}

	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	if replicas <= 0 || sts.Status.UpdatedReplicas >= replicas {
		return false, nil
	}

	pods, err := r.listComponentPods(ctx, cluster, component)
	if err != nil {
		return false, err
	}
	sort.Slice(pods, func(i, j int) bool {
		return pods[i].Name < pods[j].Name
	})

	desiredImage := ""
	if len(sts.Spec.Template.Spec.Containers) > 0 {
		desiredImage = sts.Spec.Template.Spec.Containers[0].Image
	}
	for i := range pods {
		pod := &pods[i]
		if !isPodControlledByStatefulSet(pod, sts.Name) ||
			!isStaleStatefulSetPod(pod, sts.Status.UpdateRevision, desiredImage) ||
			!isUnhealthyPod(pod) {
			continue
		}
		if pod.DeletionTimestamp != nil {
			continue
		}
		log.FromContext(ctx).Info(
			"Deleting stale unhealthy pod to unblock StatefulSet rollout",
			"statefulset", sts.Name,
			"pod", pod.Name,
			"component", component,
			"currentRevision", pod.Labels["controller-revision-hash"],
			"updateRevision", sts.Status.UpdateRevision,
			"desiredImage", desiredImage,
		)
		if r.Recorder != nil {
			r.Recorder.Eventf(cluster, nil, corev1.EventTypeNormal, "RepairingBlockedRollout", "DeleteStalePod", "Deleting stale unhealthy %s pod %s to unblock rollout to revision %s", component, pod.Name, sts.Status.UpdateRevision)
		}
		return true, r.Delete(ctx, pod)
	}

	return false, nil
}

func isPodControlledByStatefulSet(pod *corev1.Pod, statefulSetName string) bool {
	if pod == nil {
		return false
	}
	controller := metav1.GetControllerOf(pod)
	return controller != nil && controller.Kind == "StatefulSet" && controller.Name == statefulSetName
}

func isStaleStatefulSetPod(pod *corev1.Pod, updateRevision, desiredImage string) bool {
	if pod == nil {
		return false
	}
	if pod.Labels["controller-revision-hash"] != "" && pod.Labels["controller-revision-hash"] != updateRevision {
		return true
	}
	if desiredImage == "" || len(pod.Spec.Containers) == 0 {
		return false
	}
	for _, container := range pod.Spec.Containers {
		if container.Name == "antfly" {
			return container.Image != desiredImage
		}
	}
	return pod.Spec.Containers[0].Image != desiredImage
}

func isUnhealthyPod(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodUnknown {
		return true
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return false
		}
	}
	return true
}

func (r *AntflyClusterReconciler) listComponentPods(ctx context.Context, cluster *antflyv1.AntflyCluster, component string) ([]corev1.Pod, error) {
	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.InNamespace(cluster.Namespace), client.MatchingLabels(serviceSelectorLabels(cluster.Name, component))); err != nil {
		return nil, err
	}
	return podList.Items, nil
}

func (r *AntflyClusterReconciler) setComponentCondition(cluster *antflyv1.AntflyCluster, conditionType string, ready, desired int32, findings []poddiagnostics.Finding, component string) {
	status := metav1.ConditionFalse
	reason := antflyv1.ReasonWaitingForPods
	message := fmt.Sprintf("%s has %d/%d ready pods", component, ready, desired)
	if finding, ok := poddiagnostics.First(findings,
		poddiagnostics.FindingUnschedulable,
		poddiagnostics.FindingImagePullFailed,
		poddiagnostics.FindingCrashLooping,
		poddiagnostics.FindingProbeFailed,
		poddiagnostics.FindingInitFailed,
	); ok {
		reason = antflyReasonForFinding(finding)
		message = poddiagnostics.Message(finding)
	} else if ready >= desired && desired > 0 {
		status = metav1.ConditionTrue
		reason = antflyv1.ReasonComponentReady
		message = fmt.Sprintf("%s is ready", component)
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		ObservedGeneration: cluster.Generation,
		Reason:             reason,
		Message:            message,
	})
}

func (r *AntflyClusterReconciler) setAvailableCondition(cluster *antflyv1.AntflyCluster, findings []poddiagnostics.Finding, ready bool) {
	status := metav1.ConditionTrue
	reason := antflyv1.ReasonAvailable
	message := "Cluster is available"
	if len(findings) > 0 {
		status = metav1.ConditionFalse
		reason = antflyv1.ReasonRuntimeDegraded
		message = poddiagnostics.Summary(findings)
	} else if !ready {
		status = metav1.ConditionFalse
		reason = antflyv1.ReasonWaitingForPods
		message = "Cluster is waiting for ready pods"
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               antflyv1.TypeAvailable,
		Status:             status,
		ObservedGeneration: cluster.Generation,
		Reason:             reason,
		Message:            message,
	})
}

func antflyReasonForFinding(finding poddiagnostics.Finding) string {
	switch finding.Type {
	case poddiagnostics.FindingUnschedulable:
		return antflyv1.ReasonUnschedulable
	case poddiagnostics.FindingImagePullFailed:
		return antflyv1.ReasonImagePullFailed
	case poddiagnostics.FindingCrashLooping:
		return antflyv1.ReasonCrashLooping
	case poddiagnostics.FindingProbeFailed:
		return antflyv1.ReasonProbeFailed
	default:
		return antflyv1.ReasonRuntimeDegraded
	}
}

func (r *AntflyClusterReconciler) recordClusterRuntimeFailureEvents(cluster *antflyv1.AntflyCluster, originalConditions []metav1.Condition) {
	if r.Recorder == nil {
		return
	}
	for _, condition := range cluster.Status.Conditions {
		if condition.Status != metav1.ConditionFalse {
			continue
		}
		if condition.Type != antflyv1.TypeMetadataReady &&
			condition.Type != antflyv1.TypeDataReady &&
			condition.Type != antflyv1.TypeSwarmReady &&
			condition.Type != antflyv1.TypeInferenceReady &&
			condition.Type != antflyv1.TypeAvailable {
			continue
		}
		previous := meta.FindStatusCondition(originalConditions, condition.Type)
		if previous != nil && previous.Status == condition.Status && previous.Reason == condition.Reason && previous.Message == condition.Message {
			continue
		}
		r.Recorder.Eventf(cluster, nil, corev1.EventTypeWarning, condition.Reason, condition.Reason, "%s", condition.Message)
	}
}

func (r *AntflyClusterReconciler) updateProductTierStatus(cluster *antflyv1.AntflyCluster) {
	if cluster.Spec.ProductTier == nil {
		cluster.Status.ProductTierStatus = nil
		return
	}

	tier := cluster.Spec.ProductTier
	status := &antflyv1.ProductTierStatus{
		Name:               tier.Name,
		Revision:           tier.Revision,
		ManagedBy:          tier.ManagedBy,
		Mode:               effectiveTopologyModeForAPI(cluster),
		SwarmTier:          tier.SwarmTier,
		MetadataTier:       tier.MetadataTier,
		DataTier:           tier.DataTier,
		InferenceTier:      tier.InferenceTier,
		InferenceEnabled:   cluster.Spec.Inference != nil && antflyInferenceMode(cluster.Spec.Inference) != antflyv1.AntflyInferenceModeDisabled,
		ObservedGeneration: cluster.Generation,
	}

	if isSwarmMode(cluster) {
		if cluster.Spec.Swarm != nil {
			status.SwarmResources = resourceSpecSummary(cluster.Spec.Swarm.Resources)
		}
		status.SwarmStorage = chooseSwarmStorageSize(cluster)
	} else {
		status.MetadataReplicas = cluster.Spec.MetadataNodes.Replicas
		if status.MetadataReplicas == 0 {
			status.MetadataReplicas = 3
		}
		status.MetadataResources = resourceSpecSummary(cluster.Spec.MetadataNodes.Resources)
		status.MetadataStorage = cluster.Spec.Storage.MetadataStorage

		status.DataReplicas = effectiveDataNodeReplicas(cluster)
		status.DataResources = resourceSpecSummary(cluster.Spec.DataNodes.Resources)
		status.DataStorage = effectiveDataStorageSize(cluster)
		if cluster.Spec.DataNodes.AutoScaling != nil && cluster.Spec.DataNodes.AutoScaling.Enabled {
			status.DataAutoscaling = fmt.Sprintf("enabled min=%d max=%d", cluster.Spec.DataNodes.AutoScaling.MinReplicas, cluster.Spec.DataNodes.AutoScaling.MaxReplicas)
		} else {
			status.DataAutoscaling = "disabled"
		}
	}

	if cluster.Spec.Inference != nil {
		switch antflyInferenceMode(cluster.Spec.Inference) {
		case antflyv1.AntflyInferenceModeManaged:
			status.InferenceReplicas = fmt.Sprintf("managed=%d", len(cluster.Spec.Inference.ManagedPools))
		case antflyv1.AntflyInferenceModeSharedRef:
			status.InferenceReplicas = fmt.Sprintf("shared=%d", len(cluster.Spec.Inference.SharedPools))
		case antflyv1.AntflyInferenceModePlatformShared:
			status.InferenceReplicas = fmt.Sprintf("platform=%d", len(cluster.Spec.Inference.PlatformPools))
		default:
			status.InferenceReplicas = "disabled"
		}
	}

	cluster.Status.ProductTierStatus = status
}

func effectiveTopologyModeForAPI(cluster *antflyv1.AntflyCluster) antflyv1.ClusterMode {
	if isSwarmMode(cluster) {
		return antflyv1.ClusterModeSwarm
	}
	return antflyv1.ClusterModeClustered
}

func resourceSpecSummary(resources antflyv1.ResourceSpec) string {
	parts := []string{}
	if resources.CPU != "" {
		parts = append(parts, "cpu="+resources.CPU)
	}
	if resources.Memory != "" {
		parts = append(parts, "memory="+resources.Memory)
	}
	if resources.Limits.CPU != "" {
		parts = append(parts, "limitCPU="+resources.Limits.CPU)
	}
	if resources.Limits.Memory != "" {
		parts = append(parts, "limitMemory="+resources.Limits.Memory)
	}
	if resources.Limits.GPU != "" {
		parts = append(parts, "limitGPU="+resources.Limits.GPU)
	}
	return strings.Join(parts, " ")
}

// updateServiceMeshReadyCondition updates the ServiceMeshReady condition based on current status
func (r *AntflyClusterReconciler) updateServiceMeshReadyCondition(cluster *antflyv1.AntflyCluster) {
	if cluster.Status.ServiceMeshStatus == nil {
		return
	}

	var condition metav1.Condition
	condition.Type = "ServiceMeshReady"
	condition.ObservedGeneration = cluster.Generation
	condition.LastTransitionTime = metav1.Now()

	if !cluster.Status.ServiceMeshStatus.Enabled {
		condition.Status = metav1.ConditionTrue
		condition.Reason = "Disabled"
		condition.Message = "Service mesh disabled"
	} else {
		switch cluster.Status.ServiceMeshStatus.SidecarInjectionStatus {
		case "Complete":
			condition.Status = metav1.ConditionTrue
			condition.Reason = "SidecarInjectionComplete"
			condition.Message = fmt.Sprintf("All %d pods have sidecars injected", cluster.Status.ServiceMeshStatus.TotalPods)
		case "Partial":
			condition.Status = metav1.ConditionFalse
			condition.Reason = "PartialInjection"
			condition.Message = fmt.Sprintf("%d/%d pods have sidecars injected", cluster.Status.ServiceMeshStatus.PodsWithSidecars, cluster.Status.ServiceMeshStatus.TotalPods)
		case "None":
			condition.Status = metav1.ConditionFalse
			condition.Reason = "NoSidecarInjection"
			condition.Message = "No sidecars injected"
		default:
			condition.Status = metav1.ConditionUnknown
			condition.Reason = "Unknown"
			condition.Message = "Sidecar injection status unknown"
		}
	}

	// Update or append condition
	found := false
	for i, existingCondition := range cluster.Status.Conditions {
		if existingCondition.Type == "ServiceMeshReady" {
			// Only update if status or reason changed
			if existingCondition.Status != condition.Status || existingCondition.Reason != condition.Reason {
				cluster.Status.Conditions[i] = condition
			}
			found = true
			break
		}
	}
	if !found {
		cluster.Status.Conditions = append(cluster.Status.Conditions, condition)
	}
}

// applySchedulingConstraints applies user-specified scheduling constraints to the pod template.
// This is called before cloud-provider-specific functions so that their entries merge on top.
func applySchedulingConstraints(podTemplate *corev1.PodTemplateSpec, tolerations []corev1.Toleration, nodeSelector map[string]string, affinity *corev1.Affinity, topologySpreadConstraints []corev1.TopologySpreadConstraint) {
	// Apply tolerations
	podTemplate.Spec.Tolerations = append(podTemplate.Spec.Tolerations, tolerations...)

	// Apply node selector (merge into existing map)
	if len(nodeSelector) > 0 {
		if podTemplate.Spec.NodeSelector == nil {
			podTemplate.Spec.NodeSelector = make(map[string]string)
		}
		maps.Copy(podTemplate.Spec.NodeSelector, nodeSelector)
	}

	// Apply affinity (deep merge to coexist with cloud-provider entries)
	if affinity != nil {
		if podTemplate.Spec.Affinity == nil {
			podTemplate.Spec.Affinity = affinity.DeepCopy()
		} else {
			if affinity.NodeAffinity != nil {
				if podTemplate.Spec.Affinity.NodeAffinity == nil {
					podTemplate.Spec.Affinity.NodeAffinity = affinity.NodeAffinity.DeepCopy()
				} else {
					podTemplate.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
						podTemplate.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
						affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution...,
					)
					if affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
						podTemplate.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.DeepCopy()
					}
				}
			}
			if affinity.PodAffinity != nil {
				podTemplate.Spec.Affinity.PodAffinity = affinity.PodAffinity.DeepCopy()
			}
			if affinity.PodAntiAffinity != nil {
				podTemplate.Spec.Affinity.PodAntiAffinity = affinity.PodAntiAffinity.DeepCopy()
			}
		}
	}

	// Apply topology spread constraints
	if len(topologySpreadConstraints) > 0 {
		podTemplate.Spec.TopologySpreadConstraints = append(podTemplate.Spec.TopologySpreadConstraints, topologySpreadConstraints...)
	}
}

// applyGKEPodSpec applies GKE-specific configuration to pod template spec
func (r *AntflyClusterReconciler) applyGKEPodSpec(podTemplate *corev1.PodTemplateSpec, cluster *antflyv1.AntflyCluster, useSpotPods bool) {
	// GKE Autopilot mode: use compute class annotations
	if cluster.Spec.GKE != nil && cluster.Spec.GKE.Autopilot {
		// Initialize annotations if nil
		if podTemplate.Annotations == nil {
			podTemplate.Annotations = make(map[string]string)
		}

		// Apply compute class annotation (required for GKE Autopilot)
		// This controls pod scheduling on GKE Autopilot
		if cluster.Spec.GKE.AutopilotComputeClass != "" {
			podTemplate.Annotations["cloud.google.com/compute-class"] = cluster.Spec.GKE.AutopilotComputeClass
		}

		// Set termination grace period for graceful shutdown
		gracePeriod := int64(15)
		podTemplate.Spec.TerminationGracePeriodSeconds = &gracePeriod

		// Ensure NO node selectors for GKE Autopilot (conflicts with compute class)
		podTemplate.Spec.NodeSelector = nil

		return
	}

	// Standard GKE mode (non-Autopilot): use node selectors
	if useSpotPods {
		// Initialize nodeSelector if nil
		if podTemplate.Spec.NodeSelector == nil {
			podTemplate.Spec.NodeSelector = make(map[string]string)
		}

		// Apply Spot Nodes configuration using node selector
		podTemplate.Spec.NodeSelector["cloud.google.com/gke-spot"] = "true"

		// Set termination grace period for graceful shutdown on eviction
		gracePeriod := int64(15)
		podTemplate.Spec.TerminationGracePeriodSeconds = &gracePeriod
	}
}

// applyEKSPodSpec applies AWS EKS-specific configuration to pod template spec
func (r *AntflyClusterReconciler) applyEKSPodSpec(podTemplate *corev1.PodTemplateSpec, cluster *antflyv1.AntflyCluster, useSpotInstances bool) {
	// Only apply EKS configuration if EKS is enabled
	if cluster.Spec.EKS == nil || !cluster.Spec.EKS.Enabled {
		return
	}

	eks := cluster.Spec.EKS

	// Initialize annotations if nil
	if podTemplate.Annotations == nil {
		podTemplate.Annotations = make(map[string]string)
	}

	// Initialize nodeSelector if nil
	if podTemplate.Spec.NodeSelector == nil {
		podTemplate.Spec.NodeSelector = make(map[string]string)
	}

	// Apply Spot Instance configuration
	if useSpotInstances || eks.UseSpotInstances {
		// EKS Spot Instances use the capacity type label
		// This works with both managed node groups and Karpenter
		podTemplate.Spec.NodeSelector["eks.amazonaws.com/capacityType"] = "SPOT"

		// Alternative label for self-managed node groups or older EKS versions
		// podTemplate.Spec.NodeSelector["node.kubernetes.io/lifecycle"] = "spot"

		// Set termination grace period for graceful shutdown on Spot interruption
		// AWS gives 2-minute warning before Spot termination
		gracePeriod := int64(25)
		podTemplate.Spec.TerminationGracePeriodSeconds = &gracePeriod

		// Add toleration for Spot Instance taint (common pattern)
		spotToleration := corev1.Toleration{
			Key:      "eks.amazonaws.com/capacityType",
			Operator: corev1.TolerationOpEqual,
			Value:    "SPOT",
			Effect:   corev1.TaintEffectNoSchedule,
		}
		podTemplate.Spec.Tolerations = appendTolerationIfNotExists(podTemplate.Spec.Tolerations, spotToleration)
	}

	// Apply instance type node affinity if specified
	if len(eks.InstanceTypes) > 0 {
		r.applyEKSInstanceTypeAffinity(podTemplate, eks.InstanceTypes)
	}
}

// applyEKSInstanceTypeAffinity adds node affinity to prefer specific EC2 instance types
func (r *AntflyClusterReconciler) applyEKSInstanceTypeAffinity(podTemplate *corev1.PodTemplateSpec, instanceTypes []string) {
	if len(instanceTypes) == 0 {
		return
	}

	// Create node affinity for instance types
	instanceTypeRequirement := corev1.NodeSelectorRequirement{
		Key:      "node.kubernetes.io/instance-type",
		Operator: corev1.NodeSelectorOpIn,
		Values:   instanceTypes,
	}

	// Initialize affinity if nil
	if podTemplate.Spec.Affinity == nil {
		podTemplate.Spec.Affinity = &corev1.Affinity{}
	}
	if podTemplate.Spec.Affinity.NodeAffinity == nil {
		podTemplate.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}

	// Use preferred scheduling (soft affinity) to allow fallback to other instance types
	// This prevents pods from being unschedulable if preferred types aren't available
	weight := int32(100)
	preferredTerm := corev1.PreferredSchedulingTerm{
		Weight: weight,
		Preference: corev1.NodeSelectorTerm{
			MatchExpressions: []corev1.NodeSelectorRequirement{instanceTypeRequirement},
		},
	}

	podTemplate.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
		podTemplate.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
		preferredTerm,
	)
}

// appendTolerationIfNotExists adds a toleration if it doesn't already exist
func appendTolerationIfNotExists(tolerations []corev1.Toleration, newToleration corev1.Toleration) []corev1.Toleration {
	for _, t := range tolerations {
		if t.Key == newToleration.Key && t.Operator == newToleration.Operator && t.Value == newToleration.Value {
			return tolerations
		}
	}
	return append(tolerations, newToleration)
}

// reconcilePodDisruptionBudget creates or updates PodDisruptionBudgets for StatefulSets
func (r *AntflyClusterReconciler) reconcilePodDisruptionBudget(ctx context.Context, cluster *antflyv1.AntflyCluster, name string, role string) error {
	// Check if PDB is enabled via GKE or EKS configuration
	var pdbSpec *antflyv1.PodDisruptionBudgetSpec

	// Check GKE PDB configuration
	if cluster.Spec.GKE != nil && cluster.Spec.GKE.PodDisruptionBudget != nil && cluster.Spec.GKE.PodDisruptionBudget.Enabled {
		pdbSpec = cluster.Spec.GKE.PodDisruptionBudget
	}

	// Check EKS PDB configuration (EKS takes precedence if both are somehow set)
	if cluster.Spec.EKS != nil && cluster.Spec.EKS.Enabled && cluster.Spec.EKS.PodDisruptionBudget != nil && cluster.Spec.EKS.PodDisruptionBudget.Enabled {
		pdbSpec = cluster.Spec.EKS.PodDisruptionBudget
	}

	// Return if no PDB configuration is enabled
	if pdbSpec == nil {
		return nil
	}

	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
		},
	}

	// Use CreateOrUpdate to ensure PDB is updated with latest configuration
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		// Set controller reference
		if err := controllerutil.SetControllerReference(cluster, pdb, r.Scheme); err != nil {
			return err
		}

		// Update PDB spec — include instance label to scope to this cluster
		pdb.Spec.Selector = &metav1.LabelSelector{
			MatchLabels: serviceSelectorLabels(cluster.Name, role),
		}

		// Set MaxUnavailable or MinAvailable (prefer MaxUnavailable as recommended)
		if pdbSpec.MaxUnavailable != nil {
			maxUnavailable := intstr.FromInt(int(*pdbSpec.MaxUnavailable))
			pdb.Spec.MaxUnavailable = &maxUnavailable
			pdb.Spec.MinAvailable = nil // Clear MinAvailable when MaxUnavailable is set
		} else if pdbSpec.MinAvailable != nil {
			minAvailable := intstr.FromInt(int(*pdbSpec.MinAvailable))
			pdb.Spec.MinAvailable = &minAvailable
			pdb.Spec.MaxUnavailable = nil // Clear MaxUnavailable when MinAvailable is set
		} else {
			// Default to MaxUnavailable=1
			maxUnavailable := intstr.FromInt(1)
			pdb.Spec.MaxUnavailable = &maxUnavailable
			pdb.Spec.MinAvailable = nil
		}

		return nil
	})

	return err
}

type pvcUsageObservation struct {
	matched       int
	usedBytes     int64
	capacityBytes int64
	maxRequest    resource.Quantity
}

type kubeletStatsSummary struct {
	Pods []kubeletPodStats `json:"pods"`
}

type kubeletPodStats struct {
	PodRef kubeletPodReference  `json:"podRef"`
	Volume []kubeletVolumeStats `json:"volume"`
}

type kubeletPodReference struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type kubeletVolumeStats struct {
	PVCRef        *kubeletPVCReference `json:"pvcRef,omitempty"`
	UsedBytes     *uint64              `json:"usedBytes,omitempty"`
	CapacityBytes *uint64              `json:"capacityBytes,omitempty"`
}

type kubeletPVCReference struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

func (r *AntflyClusterReconciler) reconcileStorageAutoGrow(ctx context.Context, cluster *antflyv1.AntflyCluster, component, vctName, stsName, currentSizeStr, maxSizeStr string) string {
	policy := cluster.Spec.Storage.StorageAutoGrow
	if policy == nil || !policy.Enabled {
		r.setStorageAutoGrowCondition(cluster, metav1.ConditionTrue, antflyv1.ReasonStorageAutoGrowDisabled, "Storage auto-grow is disabled")
		return currentSizeStr
	}

	if maxSizeStr == "" {
		message := fmt.Sprintf("%s storage auto-grow is enabled but no max size is configured", component)
		r.setStorageAutoGrowStatus(cluster, component, currentSizeStr, "", "", 0, 0, 0, antflyv1.ReasonStorageAutoGrowFailed, message)
		r.setStorageAutoGrowCondition(cluster, metav1.ConditionFalse, antflyv1.ReasonStorageAutoGrowFailed, message)
		return currentSizeStr
	}

	currentSize, err := resource.ParseQuantity(currentSizeStr)
	if err != nil {
		message := fmt.Sprintf("%s current storage size %q is invalid: %v", component, currentSizeStr, err)
		r.setStorageAutoGrowStatus(cluster, component, currentSizeStr, "", maxSizeStr, 0, 0, 0, antflyv1.ReasonStorageAutoGrowFailed, message)
		r.setStorageAutoGrowCondition(cluster, metav1.ConditionFalse, antflyv1.ReasonStorageAutoGrowFailed, message)
		return currentSizeStr
	}
	maxSize, err := resource.ParseQuantity(maxSizeStr)
	if err != nil {
		message := fmt.Sprintf("%s max storage size %q is invalid: %v", component, maxSizeStr, err)
		r.setStorageAutoGrowStatus(cluster, component, currentSize.String(), "", maxSizeStr, 0, 0, 0, antflyv1.ReasonStorageAutoGrowFailed, message)
		r.setStorageAutoGrowCondition(cluster, metav1.ConditionFalse, antflyv1.ReasonStorageAutoGrowFailed, message)
		return currentSizeStr
	}
	growIncrement, err := resource.ParseQuantity(policy.GrowIncrement)
	if err != nil || growIncrement.Sign() <= 0 {
		message := fmt.Sprintf("%s grow increment %q is invalid", component, policy.GrowIncrement)
		r.setStorageAutoGrowStatus(cluster, component, currentSize.String(), "", maxSize.String(), 0, 0, 0, antflyv1.ReasonStorageAutoGrowFailed, message)
		r.setStorageAutoGrowCondition(cluster, metav1.ConditionFalse, antflyv1.ReasonStorageAutoGrowFailed, message)
		return currentSizeStr
	}

	observation, err := r.observePVCUsage(ctx, cluster, component, vctName, stsName)
	if observation.maxRequest.Cmp(currentSize) > 0 {
		currentSize = observation.maxRequest
		currentSizeStr = currentSize.String()
	}
	if err != nil {
		message := fmt.Sprintf("%s storage usage is unavailable: %v", component, err)
		r.setStorageAutoGrowStatus(cluster, component, currentSize.String(), "", maxSize.String(), 0, 0, 0, antflyv1.ReasonStorageAutoGrowUsageUnavailable, message)
		r.setStorageAutoGrowCondition(cluster, metav1.ConditionUnknown, antflyv1.ReasonStorageAutoGrowUsageUnavailable, message)
		return currentSizeStr
	}
	if observation.matched == 0 || observation.capacityBytes <= 0 {
		message := fmt.Sprintf("Waiting for %s PVC usage metrics before evaluating auto-grow", component)
		r.setStorageAutoGrowStatus(cluster, component, currentSize.String(), "", maxSize.String(), observation.usedBytes, observation.capacityBytes, 0, antflyv1.ReasonStorageAutoGrowUsageUnavailable, message)
		r.setStorageAutoGrowCondition(cluster, metav1.ConditionUnknown, antflyv1.ReasonStorageAutoGrowUsageUnavailable, message)
		return currentSizeStr
	}

	usagePercentValue := (observation.usedBytes/observation.capacityBytes)*100 + ((observation.usedBytes%observation.capacityBytes)*100)/observation.capacityBytes
	const maxInt32 = int64(1<<31 - 1)
	if usagePercentValue > maxInt32 {
		usagePercentValue = maxInt32
	}
	usagePercent := int32(usagePercentValue)
	threshold := policy.GrowThresholdPercent
	if threshold == 0 {
		threshold = 85
	}
	if usagePercent < threshold {
		message := fmt.Sprintf("%s storage usage is %d%%, below auto-grow threshold %d%%", component, usagePercent, threshold)
		r.setStorageAutoGrowStatus(cluster, component, currentSize.String(), "", maxSize.String(), observation.usedBytes, observation.capacityBytes, usagePercent, antflyv1.ReasonStorageAutoGrowReady, message)
		r.setStorageAutoGrowCondition(cluster, metav1.ConditionTrue, antflyv1.ReasonStorageAutoGrowReady, message)
		return currentSizeStr
	}

	if currentSize.Cmp(maxSize) >= 0 {
		message := fmt.Sprintf("%s storage usage is %d%% but current size %s has reached max %s", component, usagePercent, currentSize.String(), maxSize.String())
		r.setStorageAutoGrowStatus(cluster, component, currentSize.String(), currentSize.String(), maxSize.String(), observation.usedBytes, observation.capacityBytes, usagePercent, antflyv1.ReasonStorageAutoGrowMaxReached, message)
		r.setStorageAutoGrowCondition(cluster, metav1.ConditionFalse, antflyv1.ReasonStorageAutoGrowMaxReached, message)
		return currentSizeStr
	}

	recommended := resource.NewQuantity(currentSize.Value()+growIncrement.Value(), resource.BinarySI)
	if recommended.Cmp(maxSize) > 0 {
		maxCopy := maxSize.DeepCopy()
		recommended = &maxCopy
	}
	message := fmt.Sprintf("%s storage usage is %d%%, growing from %s to %s", component, usagePercent, currentSize.String(), recommended.String())
	r.setStorageAutoGrowStatus(cluster, component, currentSize.String(), recommended.String(), maxSize.String(), observation.usedBytes, observation.capacityBytes, usagePercent, antflyv1.ReasonStorageAutoGrowInProgress, message)
	r.setStorageAutoGrowCondition(cluster, metav1.ConditionUnknown, antflyv1.ReasonStorageAutoGrowInProgress, message)
	return recommended.String()
}

func (r *AntflyClusterReconciler) observePVCUsage(ctx context.Context, cluster *antflyv1.AntflyCluster, component, vctName, stsName string) (pvcUsageObservation, error) {
	prefix := vctName + "-" + stsName + "-"
	var pvcList corev1.PersistentVolumeClaimList
	if err := r.List(ctx, &pvcList, client.InNamespace(cluster.Namespace), client.MatchingLabels{"app.kubernetes.io/instance": cluster.Name}); err != nil {
		return pvcUsageObservation{}, fmt.Errorf("list PVCs: %w", err)
	}

	observation := pvcUsageObservation{}
	pvcNames := map[string]struct{}{}
	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		if !strings.HasPrefix(pvc.Name, prefix) {
			continue
		}
		observation.matched++
		pvcNames[pvc.Name] = struct{}{}
		if request := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; request.Cmp(observation.maxRequest) > 0 {
			observation.maxRequest = request
		}
	}
	if len(pvcNames) == 0 {
		return observation, nil
	}
	if r.KubeClient == nil && r.NodeStatsFetcher == nil {
		return observation, fmt.Errorf("kubernetes client is not configured")
	}

	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(cluster.Namespace), client.MatchingLabels(serviceSelectorLabels(cluster.Name, component))); err != nil {
		return observation, fmt.Errorf("list %s pods: %w", component, err)
	}

	summaries := map[string]*kubeletStatsSummary{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Spec.NodeName == "" {
			continue
		}
		summary, ok := summaries[pod.Spec.NodeName]
		if !ok {
			fetched, err := r.fetchNodeStatsSummary(ctx, pod.Spec.NodeName)
			if err != nil {
				return observation, err
			}
			summary = fetched
			summaries[pod.Spec.NodeName] = summary
		}
		for _, podStats := range summary.Pods {
			if podStats.PodRef.Namespace != pod.Namespace || podStats.PodRef.Name != pod.Name {
				continue
			}
			for _, volume := range podStats.Volume {
				if volume.PVCRef == nil || volume.PVCRef.Namespace != cluster.Namespace {
					continue
				}
				if _, matched := pvcNames[volume.PVCRef.Name]; !matched {
					continue
				}
				if volume.UsedBytes != nil {
					observation.usedBytes += int64(*volume.UsedBytes) //nolint:gosec // kubelet volume stats fit int64 byte counters in practice
				}
				if volume.CapacityBytes != nil {
					observation.capacityBytes += int64(*volume.CapacityBytes) //nolint:gosec // kubelet volume stats fit int64 byte counters in practice
				}
			}
		}
	}

	return observation, nil
}

func (r *AntflyClusterReconciler) fetchNodeStatsSummary(ctx context.Context, nodeName string) (*kubeletStatsSummary, error) {
	if r.NodeStatsFetcher != nil {
		return r.NodeStatsFetcher(ctx, nodeName)
	}
	raw, err := r.KubeClient.CoreV1().RESTClient().Get().
		Resource("nodes").
		Name(nodeName).
		SubResource("proxy").
		Suffix("stats/summary").
		DoRaw(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetch node %s stats summary: %w", nodeName, err)
	}
	var summary kubeletStatsSummary
	if err := json.Unmarshal(raw, &summary); err != nil {
		return nil, fmt.Errorf("decode node %s stats summary: %w", nodeName, err)
	}
	return &summary, nil
}

func (r *AntflyClusterReconciler) setStorageAutoGrowStatus(cluster *antflyv1.AntflyCluster, component, currentSize, recommendedSize, maxSize string, usedBytes, capacityBytes int64, usagePercent int32, reason, message string) {
	now := metav1.Now()
	cluster.Status.StorageAutoGrowStatus = &antflyv1.StorageAutoGrowStatus{
		Component:          component,
		CurrentSize:        currentSize,
		RecommendedSize:    recommendedSize,
		MaxSize:            maxSize,
		UsedBytes:          usedBytes,
		CapacityBytes:      capacityBytes,
		UsagePercent:       usagePercent,
		Reason:             reason,
		Message:            message,
		LastEvaluationTime: &now,
	}
}

func (r *AntflyClusterReconciler) setStorageAutoGrowCondition(cluster *antflyv1.AntflyCluster, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               antflyv1.TypeStorageAutoGrow,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: cluster.Generation,
	})
}

type pvcExpansionState string

const (
	pvcExpansionSkipped    pvcExpansionState = "skipped"
	pvcExpansionPending    pvcExpansionState = "pending"
	pvcExpansionInProgress pvcExpansionState = "inProgress"
	pvcExpansionComplete   pvcExpansionState = "complete"
	pvcExpansionFailed     pvcExpansionState = "failed"
)

type pvcExpansionResult struct {
	component string
	state     pvcExpansionState
	message   string
}

// reconcilePVCExpansion patches existing PVCs when the CRD specifies a larger storage size.
// This is a best-effort operation — failures are reported as status conditions but don't block reconciliation.
func (r *AntflyClusterReconciler) reconcilePVCExpansion(ctx context.Context, cluster *antflyv1.AntflyCluster, component, vctName, stsName, desiredSizeStr string) pvcExpansionResult {
	if desiredSizeStr == "" {
		return pvcExpansionResult{
			component: component,
			state:     pvcExpansionSkipped,
			message:   fmt.Sprintf("%s storage uses the operator default size", component),
		}
	}

	log := log.FromContext(ctx)
	desiredSize := resource.MustParse(desiredSizeStr)

	// List PVCs matching this StatefulSet's VolumeClaimTemplate naming: {vctName}-{stsName}-{ordinal}
	prefix := vctName + "-" + stsName + "-"
	var pvcList corev1.PersistentVolumeClaimList
	if err := r.List(ctx, &pvcList, client.InNamespace(cluster.Namespace),
		client.MatchingLabels{"app.kubernetes.io/instance": cluster.Name}); err != nil {
		log.Error(err, "Failed to list PVCs for expansion check")
		return pvcExpansionResult{
			component: component,
			state:     pvcExpansionFailed,
			message:   fmt.Sprintf("Failed to list %s PVCs for expansion: %v", component, err),
		}
	}

	matched := 0
	updated := 0
	waiting := 0
	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		if !strings.HasPrefix(pvc.Name, prefix) {
			continue // secondary guard: multiple StatefulSets share the instance label
		}
		matched++

		currentSize := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		if desiredSize.Cmp(currentSize) > 0 {
			log.Info("Expanding PVC", "pvc", pvc.Name, "from", currentSize.String(), "to", desiredSize.String())
			pvc.Spec.Resources.Requests[corev1.ResourceStorage] = desiredSize
			if err := r.Update(ctx, pvc); err != nil {
				log.Error(err, "Failed to expand PVC", "pvc", pvc.Name)
				return pvcExpansionResult{
					component: component,
					state:     pvcExpansionFailed,
					message:   fmt.Sprintf("Failed to expand PVC %s: %v", pvc.Name, err),
				}
			}
			updated++
			waiting++
			continue
		}

		capacity := pvc.Status.Capacity[corev1.ResourceStorage]
		if capacity.IsZero() || desiredSize.Cmp(capacity) > 0 || pvcHasFileSystemResizePending(pvc) {
			waiting++
		}
	}

	switch {
	case matched == 0:
		return pvcExpansionResult{
			component: component,
			state:     pvcExpansionPending,
			message:   fmt.Sprintf("Waiting for %s PVCs with prefix %q to be created", component, prefix),
		}
	case updated > 0:
		return pvcExpansionResult{
			component: component,
			state:     pvcExpansionInProgress,
			message:   fmt.Sprintf("Expansion requested for %d %s PVC(s) to %s", updated, component, desiredSize.String()),
		}
	case waiting > 0:
		return pvcExpansionResult{
			component: component,
			state:     pvcExpansionInProgress,
			message:   fmt.Sprintf("Waiting for %d %s PVC(s) to report capacity %s", waiting, component, desiredSize.String()),
		}
	default:
		return pvcExpansionResult{
			component: component,
			state:     pvcExpansionComplete,
			message:   fmt.Sprintf("All %d %s PVC(s) report requested capacity %s", matched, component, desiredSize.String()),
		}
	}
}

func pvcHasFileSystemResizePending(pvc *corev1.PersistentVolumeClaim) bool {
	for _, condition := range pvc.Status.Conditions {
		if condition.Type == corev1.PersistentVolumeClaimFileSystemResizePending && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (r *AntflyClusterReconciler) setPVCExpansionCondition(cluster *antflyv1.AntflyCluster, results []pvcExpansionResult) {
	var relevant []pvcExpansionResult
	for _, result := range results {
		if result.state != pvcExpansionSkipped {
			relevant = append(relevant, result)
		}
	}
	if len(relevant) == 0 {
		return
	}

	status := metav1.ConditionTrue
	reason := antflyv1.ReasonPVCExpansionComplete
	for _, result := range relevant {
		switch result.state {
		case pvcExpansionFailed:
			status = metav1.ConditionFalse
			reason = antflyv1.ReasonPVCExpansionFailed
		case pvcExpansionPending:
			if reason != antflyv1.ReasonPVCExpansionFailed {
				status = metav1.ConditionUnknown
				reason = antflyv1.ReasonPVCExpansionPending
			}
		case pvcExpansionInProgress:
			if reason != antflyv1.ReasonPVCExpansionFailed && reason != antflyv1.ReasonPVCExpansionPending {
				status = metav1.ConditionUnknown
				reason = antflyv1.ReasonPVCExpansionInProgress
			}
		}
	}

	messages := make([]string, 0, len(relevant))
	for _, result := range relevant {
		messages = append(messages, result.message)
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               antflyv1.TypePVCExpansion,
		Status:             status,
		Reason:             reason,
		Message:            strings.Join(messages, "; "),
		ObservedGeneration: cluster.Generation,
	})
}

// checkPVCTopologyHealth detects PVC/AZ topology issues by checking for Pending pods
// with "volume node affinity conflict" messages. Sets the StorageHealthy condition.
func (r *AntflyClusterReconciler) checkPVCTopologyHealth(ctx context.Context, cluster *antflyv1.AntflyCluster) {
	log := log.FromContext(ctx)

	mode := effectiveTopologyMode(cluster)
	components := []string{"metadata", "data"}
	if mode == topologyModeSwarm {
		components = []string{"swarm"}
	}

	// Check pods for topology issues
	for _, component := range components {
		var podList corev1.PodList
		if err := r.List(ctx, &podList, client.InNamespace(cluster.Namespace),
			client.MatchingLabels(serviceSelectorLabels(cluster.Name, component))); err != nil {
			log.Error(err, "Failed to list pods for topology health check", "component", component)
			continue
		}

		for _, pod := range podList.Items {
			if pod.Status.Phase != corev1.PodPending {
				continue
			}

			// Check PodScheduled condition for volume affinity issues
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionFalse {
					if strings.HasPrefix(cond.Message, "0/") && containsVolumeAffinityMessage(cond.Message) {
						msg := fmt.Sprintf("Pod %s is Pending due to PVC/AZ topology mismatch: %s. "+
							"Verify your StorageClass uses volumeBindingMode: WaitForFirstConsumer and "+
							"nodes are available in the AZ where PVCs are bound.", pod.Name, cond.Message)
						r.setStorageCondition(cluster, metav1.ConditionFalse, antflyv1.ReasonPVCAZMismatch, msg)
						r.Recorder.Eventf(cluster, nil, corev1.EventTypeWarning, antflyv1.ReasonPVCAZMismatch,
							"StorageTopologyMismatch", msg)
						return
					}
				}
			}
		}
	}

	// All good — set healthy
	r.setStorageCondition(cluster, metav1.ConditionTrue, antflyv1.ReasonStorageHealthy, "Storage topology is healthy")
}

// setStorageCondition updates the StorageHealthy condition on the cluster status.
func (r *AntflyClusterReconciler) setStorageCondition(cluster *antflyv1.AntflyCluster, status metav1.ConditionStatus, reason, message string) {
	condition := metav1.Condition{
		Type:               antflyv1.TypeStorageHealthy,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	}

	for i, existing := range cluster.Status.Conditions {
		if existing.Type == antflyv1.TypeStorageHealthy {
			if existing.Status != status || existing.Reason != reason {
				cluster.Status.Conditions[i] = condition
			}
			return
		}
	}
	cluster.Status.Conditions = append(cluster.Status.Conditions, condition)
}

// containsVolumeAffinityMessage checks if a scheduler message indicates a volume node affinity conflict.
func containsVolumeAffinityMessage(msg string) bool {
	return strings.Contains(msg, "volume node affinity")
}

func hasAnyPrefix(name string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// SetupWithManager sets up the controller with the Manager.
func (r *AntflyClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	builder := ctrl.NewControllerManagedBy(mgr).
		For(&antflyv1.AntflyCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&coordinationv1.Lease{}).
		Owns(&batchv1.Job{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.requestsForPod))
	if r.ManageInferencePools {
		builder = builder.Owns(&inferencev1alpha1.InferencePool{})
	}
	return builder.Complete(r)
}

func (r *AntflyClusterReconciler) requestsForPod(ctx context.Context, obj client.Object) []reconcile.Request {
	clusterName := obj.GetLabels()["app.kubernetes.io/instance"]
	component := obj.GetLabels()["app.kubernetes.io/component"]
	if clusterName == "" || (component != "metadata" && component != "data" && component != "swarm") {
		return nil
	}
	cluster := &antflyv1.AntflyCluster{}
	key := types.NamespacedName{Name: clusterName, Namespace: obj.GetNamespace()}
	if err := r.Get(ctx, key, cluster); err != nil {
		return nil
	}
	return []reconcile.Request{{NamespacedName: key}}
}
