package controllers

//go:generate go tool controller-gen rbac:roleName=antfly-operator-cluster-role paths="." output:rbac:artifacts:config=../manifests/rbac

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	antflyv1 "github.com/antflydb/antfly/pkg/antfly-operator/api/v1"
)

// AntflyClusterReconciler reconciles an AntflyCluster object
type AntflyClusterReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	AutoScaler *AutoScaler
	Recorder   events.EventRecorder

	// validationAttempts tracks consecutive validation failure counts per cluster
	// (namespace/name -> int). Reset on successful validation. Used for
	// exponential backoff on repeated validation failures.
	validationAttempts sync.Map
}

//+kubebuilder:rbac:groups=antfly.io,resources=antflyclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=antfly.io,resources=antflyclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=antfly.io,resources=antflyclusters/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch
//+kubebuilder:rbac:groups=metrics.k8s.io,resources=pods,verbs=get;list
//+kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch

// podLabels returns the standard labels for pod templates including the instance identifier.
// These are a superset of serviceSelectorLabels — they include managed-by labels
// that MUST NOT be added to StatefulSet spec.selector.matchLabels (immutable after creation).
func podLabels(clusterName, component string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "antfly-database",
		"app.kubernetes.io/component":  component,
		"app.kubernetes.io/instance":   clusterName,
		"app.kubernetes.io/managed-by": "antfly-operator",
	}
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

// deregisterDataNodes calls the Antfly metadata deregistration API for data nodes
// being removed during scale-down. This triggers Raft peer removal before the pods
// are deleted, preventing phantom voters in Raft configurations.
// Store ID is deterministic: store_id = pod_ordinal + 1.
func (r *AntflyClusterReconciler) deregisterDataNodes(ctx context.Context, cluster *antflyv1.AntflyCluster, currentReplicas, desiredReplicas int32) error {
	if desiredReplicas >= currentReplicas {
		return nil // not scaling down
	}

	log := log.FromContext(ctx)
	metadataAddr := fmt.Sprintf("http://%s-metadata.%s.svc:%d",
		cluster.Name, cluster.Namespace, cluster.Spec.MetadataNodes.MetadataAPI.Port)

	// Deregister highest ordinals first (they are removed by StatefulSet on scale-down)
	for ordinal := currentReplicas - 1; ordinal >= desiredReplicas; ordinal-- {
		storeID := ordinal + 1
		url := fmt.Sprintf("%s/_internal/v1/store/%d", metadataAddr, storeID)

		log.Info("Deregistering data node before scale-down", "ordinal", ordinal, "storeID", storeID)

		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
		if err != nil {
			return fmt.Errorf("failed to create deregistration request for store %d: %w", storeID, err)
		}

		resp, err := http.DefaultClient.Do(req) //nolint:gosec // URL is constructed from cluster-internal service address, not external user input
		if err != nil {
			// Connection errors mean metadata isn't ready — requeue
			log.Error(err, "Failed to deregister data node, will retry", "storeID", storeID)
			return fmt.Errorf("failed to deregister store %d: %w", storeID, err)
		}
		statusCode := resp.StatusCode
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

		if statusCode >= 200 && statusCode < 300 {
			log.Info("Successfully deregistered data node", "storeID", storeID)
		} else if statusCode == http.StatusNotFound {
			// Store already deregistered or doesn't exist — safe to proceed
			log.Info("Data node already deregistered or not found", "storeID", storeID)
		} else {
			log.Error(nil, "Unexpected response from deregistration API", "storeID", storeID, "status", statusCode)
			return fmt.Errorf("deregistration of store %d returned status %d", storeID, statusCode)
		}
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
		if cluster.Spec.Swarm.Termite == nil {
			cluster.Spec.Swarm.Termite = &antflyv1.SwarmTermiteSpec{
				Enabled: true,
				APIURL:  "http://0.0.0.0:11433",
			}
		} else if cluster.Spec.Swarm.Termite.APIURL == "" {
			cluster.Spec.Swarm.Termite.APIURL = "http://0.0.0.0:11433"
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

// envFromCache caches secret and configmap fetches for a single reconcile cycle,
// avoiding duplicate API calls when checkEnvFromSecrets and computeEnvFromHash
// reference the same resources.
type envFromCache struct {
	client     client.Reader
	secrets    map[types.NamespacedName]*corev1.Secret
	configMaps map[types.NamespacedName]*corev1.ConfigMap
	// notFound tracks keys that returned NotFound so we don't retry them.
	notFound map[types.NamespacedName]bool
}

func newEnvFromCache(c client.Reader) *envFromCache {
	return &envFromCache{
		client:     c,
		secrets:    make(map[types.NamespacedName]*corev1.Secret),
		configMaps: make(map[types.NamespacedName]*corev1.ConfigMap),
		notFound:   make(map[types.NamespacedName]bool),
	}
}

func (c *envFromCache) getSecret(ctx context.Context, key types.NamespacedName) (*corev1.Secret, error) {
	if s, ok := c.secrets[key]; ok {
		return s, nil
	}
	if c.notFound[key] {
		return nil, errors.NewNotFound(corev1.Resource("secrets"), key.Name)
	}
	s := &corev1.Secret{}
	if err := c.client.Get(ctx, key, s); err != nil {
		if errors.IsNotFound(err) {
			c.notFound[key] = true
		}
		return nil, err
	}
	c.secrets[key] = s
	return s, nil
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

// computeEnvFromHash computes a hash of the data in referenced secrets and configmaps.
// This hash is used as a pod annotation to trigger rolling updates when secret/configmap data changes.
func (r *AntflyClusterReconciler) computeEnvFromHash(ctx context.Context, cache *envFromCache, namespace string, envFrom []corev1.EnvFromSource) string {
	if len(envFrom) == 0 {
		return ""
	}

	h := sha256.New()

	for _, source := range envFrom {
		if source.SecretRef != nil {
			key := types.NamespacedName{Name: source.SecretRef.Name, Namespace: namespace}
			secret, err := cache.getSecret(ctx, key)
			if err == nil {
				// Sort keys for deterministic hash
				keys := make([]string, 0, len(secret.Data))
				for k := range secret.Data {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				for _, k := range keys {
					h.Write([]byte(k))
					h.Write(secret.Data[k])
				}
			}
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

// checkEnvFromSecrets checks if all secrets referenced in EnvFrom exist and updates the cluster status.
// Returns an error if any referenced secret is not found.
func (r *AntflyClusterReconciler) checkEnvFromSecrets(ctx context.Context, cache *envFromCache, cluster *antflyv1.AntflyCluster) error {
	log := log.FromContext(ctx)

	// Collect envFrom sources for the active topology.
	var allEnvFrom []corev1.EnvFromSource
	if isSwarmMode(cluster) {
		if cluster.Spec.Swarm != nil {
			allEnvFrom = append(allEnvFrom, cluster.Spec.Swarm.EnvFrom...)
		}
	} else {
		allEnvFrom = append(allEnvFrom, cluster.Spec.MetadataNodes.EnvFrom...)
		allEnvFrom = append(allEnvFrom, cluster.Spec.DataNodes.EnvFrom...)
	}

	// If no envFrom sources, set status to True and return
	if len(allEnvFrom) == 0 {
		r.setSecretsReadyCondition(cluster, metav1.ConditionTrue, antflyv1.ReasonAllSecretsFound, "No secrets referenced")
		return nil
	}

	// Check each secret reference
	var missingSecrets []string
	for _, source := range allEnvFrom {
		if source.SecretRef != nil {
			key := types.NamespacedName{Name: source.SecretRef.Name, Namespace: cluster.Namespace}
			_, err := cache.getSecret(ctx, key)
			if err != nil {
				if errors.IsNotFound(err) {
					missingSecrets = append(missingSecrets, source.SecretRef.Name)
					log.Info("Referenced secret not found", "secret", source.SecretRef.Name, "namespace", cluster.Namespace)
				} else {
					return fmt.Errorf("failed to check secret %s: %w", source.SecretRef.Name, err)
				}
			}
		}
		// ConfigMaps are also checked but are less critical for backup credentials
		if source.ConfigMapRef != nil {
			key := types.NamespacedName{Name: source.ConfigMapRef.Name, Namespace: cluster.Namespace}
			_, err := cache.getConfigMap(ctx, key)
			if err != nil {
				if errors.IsNotFound(err) {
					log.Info("Referenced configmap not found", "configmap", source.ConfigMapRef.Name, "namespace", cluster.Namespace)
					// ConfigMaps are optional, don't block on missing configmaps
				} else {
					return fmt.Errorf("failed to check configmap %s: %w", source.ConfigMapRef.Name, err)
				}
			}
		}
	}

	if len(missingSecrets) > 0 {
		message := fmt.Sprintf("Secret(s) not found: %v", missingSecrets)
		r.setSecretsReadyCondition(cluster, metav1.ConditionFalse, antflyv1.ReasonSecretNotFound, message)
		r.Recorder.Eventf(cluster, nil, corev1.EventTypeWarning, antflyv1.ReasonSecretNotFound, antflyv1.ReasonSecretNotFound, "%s", message)
		// Don't return error - allow reconciliation to continue so pods can be created
		// Pods will be stuck in CreateContainerConfigError if secrets don't exist
		return nil
	}

	r.setSecretsReadyCondition(cluster, metav1.ConditionTrue, antflyv1.ReasonAllSecretsFound, "All referenced secrets exist")
	return nil
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

	// Per-reconcile cache for secret/configmap lookups — shared by checkEnvFromSecrets,
	// buildPodAnnotations (metadata + data), avoiding 2-3x duplicate API calls.
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

	// Check if referenced secrets exist and update SecretsReady condition
	if err := r.checkEnvFromSecrets(ctx, efCache, workingCluster); err != nil {
		log.Error(err, "Failed to check envFrom secrets")
		// Don't block reconciliation - pods will fail with CreateContainerConfigError if secrets don't exist
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
		if err := r.reconcileSwarmStatefulSet(ctx, efCache, workingCluster); err != nil {
			return ctrl.Result{}, err
		}

		r.reconcilePVCExpansion(ctx, workingCluster, "swarm-storage", workingCluster.Name+"-swarm", chooseSwarmStorageSize(workingCluster))

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

	// Evaluate autoscaling before creating/updating Data StatefulSet
	if r.AutoScaler != nil && workingCluster.Spec.DataNodes.AutoScaling != nil && workingCluster.Spec.DataNodes.AutoScaling.Enabled {
		desiredReplicas, err := r.AutoScaler.EvaluateScaling(ctx, workingCluster)
		if err != nil {
			log.Error(err, "Failed to evaluate autoscaling")
			// Continue with current replicas on error
		} else if desiredReplicas != workingCluster.Spec.DataNodes.Replicas {
			// Update the desired replicas
			workingCluster.Spec.DataNodes.Replicas = desiredReplicas
			r.AutoScaler.UpdateScalingStatus(workingCluster, desiredReplicas)
			log.Info("Autoscaling data nodes", "currentReplicas", antflyCluster.Spec.DataNodes.Replicas, "desiredReplicas", desiredReplicas)
		}
	}

	// Deregister data nodes from Raft before scaling down.
	// By this point, workingCluster.Spec.DataNodes.Replicas reflects the final desired
	// count from either the CRD spec (manual) or the autoscaler.
	// We use Status.Replicas (actual running pods) rather than Spec.Replicas (desired)
	// because a prior failed reconcile may have already written the reduced desired
	// count to Spec without completing deregistration.
	{
		existingSts := &appsv1.StatefulSet{}
		stsName := types.NamespacedName{Name: workingCluster.Name + "-data", Namespace: workingCluster.Namespace}
		if err := r.Get(ctx, stsName, existingSts); err == nil {
			currentReplicas := existingSts.Status.Replicas
			if currentReplicas == 0 && existingSts.Spec.Replicas != nil {
				// Status may not be populated yet for a newly created STS;
				// fall back to Spec in that case.
				currentReplicas = *existingSts.Spec.Replicas
			}
			desiredReplicas := workingCluster.Spec.DataNodes.Replicas
			if desiredReplicas == 0 {
				desiredReplicas = 3 // default
			}
			if err := r.deregisterDataNodes(ctx, workingCluster, currentReplicas, desiredReplicas); err != nil {
				log.Error(err, "Failed to deregister data nodes, will retry")
				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}
		}
		// If the StatefulSet doesn't exist yet, no deregistration needed
	}

	// Create Data StatefulSet
	if err := r.reconcileDataStatefulSet(ctx, efCache, workingCluster); err != nil {
		return ctrl.Result{}, err
	}

	// Reconcile PVC expansion (metadata and data)
	r.reconcilePVCExpansion(ctx, workingCluster, "metadata-storage", workingCluster.Name+"-metadata", workingCluster.Spec.Storage.MetadataStorage)
	r.reconcilePVCExpansion(ctx, workingCluster, "data-storage", workingCluster.Name+"-data", workingCluster.Spec.Storage.DataStorage)

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

	// If autoscaling is enabled, requeue for periodic evaluation
	if workingCluster.Spec.DataNodes.AutoScaling != nil && workingCluster.Spec.DataNodes.AutoScaling.Enabled {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	return ctrl.Result{}, nil
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
		"max_shard_size_bytes": 67108864, // Default 64MB
		"replication_factor":   3,        // Default
		"enable_auth":          false,    // Default
		"disable_shard_alloc":  true,     // Default
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
	termiteEnabled := swarm.Termite == nil || swarm.Termite.Enabled
	termiteAPIURL := "http://0.0.0.0:11433"
	if swarm.Termite != nil && swarm.Termite.APIURL != "" {
		termiteAPIURL = swarm.Termite.APIURL
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

	if termiteEnabled {
		termiteConfig := map[string]any{}
		if userTermite, ok := userConfig["termite"].(map[string]any); ok {
			maps.Copy(termiteConfig, userTermite)
		}
		termiteConfig["api_url"] = termiteAPIURL
		completeConfig["termite"] = termiteConfig
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

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-public-api",
			Namespace: cluster.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Type: serviceType,
			Selector: serviceSelectorLabels(cluster.Name, func() string {
				if swarmMode {
					return "swarm"
				}
				return "metadata"
			}()),
			Ports: []corev1.ServicePort{servicePort},
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
	termiteEnabled := swarm.Termite == nil || swarm.Termite.Enabled
	termiteArgs := "--termite=false"
	if termiteEnabled {
		termiteArgs = "--termite"
		if swarm.Termite != nil && swarm.Termite.APIURL != "" {
			termiteArgs = fmt.Sprintf("%s --termite-api-url %s", termiteArgs, swarm.Termite.APIURL)
		}
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
				Labels:      podLabels(cluster.Name, "swarm"),
				Annotations: r.buildPodAnnotations(ctx, cache, cluster, envFromSources),
			},
			Spec: corev1.PodSpec{
				ServiceAccountName: cluster.Spec.ServiceAccountName,
				InitContainers: []corev1.Container{
					r.buildStorageInitContainer("swarm-storage"),
				},
				Containers: []corev1.Container{
					{
						Name:            "antfly",
						Image:           cluster.Spec.Image,
						ImagePullPolicy: corev1.PullPolicy(cluster.Spec.ImagePullPolicy),
						EnvFrom:         envFromSources,
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
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "swarm-storage",
								MountPath: "/antflydb",
							},
							{
								Name:      "config",
								MountPath: "/config",
							},
						},
						Command: []string{"/bin/sh", "-c"},
						Args: []string{
							fmt.Sprintf(`
exec /antfly swarm --id %d --config /config/config.json \
  --metadata-api http://0.0.0.0:%d \
  --metadata-raft http://0.0.0.0:%d \
  --metadata-cluster '{ "%s": "http://0.0.0.0:%d" }' \
  --store-api http://0.0.0.0:%d \
  --store-raft http://0.0.0.0:%d \
  --health-port %d \
  %s
							`,
								swarm.NodeID,
								swarm.MetadataAPI.Port,
								swarm.MetadataRaft.Port,
								strconv.FormatInt(int64(swarm.NodeID), 16),
								swarm.MetadataRaft.Port,
								swarm.StoreAPI.Port,
								swarm.StoreRaft.Port,
								swarm.Health.Port,
								termiteArgs,
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
				Volumes: []corev1.Volume{
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
				},
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
				Labels:      podLabels(cluster.Name, "metadata"),
				Annotations: r.buildPodAnnotations(ctx, cache, cluster, cluster.Spec.MetadataNodes.EnvFrom),
			},
			Spec: corev1.PodSpec{
				ServiceAccountName: cluster.Spec.ServiceAccountName,
				InitContainers: []corev1.Container{
					r.buildStorageInitContainer("metadata-storage"),
				},
				Containers: []corev1.Container{
					{
						Name:            "antfly",
						Image:           cluster.Spec.Image,
						ImagePullPolicy: corev1.PullPolicy(cluster.Spec.ImagePullPolicy),
						EnvFrom:         cluster.Spec.MetadataNodes.EnvFrom,
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
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "metadata-storage",
								MountPath: "/antflydb",
							},
							{
								Name:      "config",
								MountPath: "/config",
							},
						},
						Command: []string{"/bin/sh", "-c"},
						Args: []string{
							fmt.Sprintf(`
ORDINAL=${HOSTNAME##*-}
ID=$((ORDINAL + 1))
exec /antfly metadata --id $ID --config /config/config.json \
  --api http://0.0.0.0:%d \
  --raft http://0.0.0.0:%d \
  --health-port %d \
  --cluster '%s'
							`,
								cluster.Spec.MetadataNodes.MetadataAPI.Port,
								cluster.Spec.MetadataNodes.MetadataRaft.Port,
								cluster.Spec.MetadataNodes.Health.Port,
								metadataCluster,
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
				Volumes: []corev1.Volume{
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
				},
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
	replicas := int32(3)
	if cluster.Spec.DataNodes.Replicas > 0 {
		replicas = cluster.Spec.DataNodes.Replicas
	}

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
				Labels:      podLabels(cluster.Name, "data"),
				Annotations: r.buildPodAnnotations(ctx, cache, cluster, cluster.Spec.DataNodes.EnvFrom),
			},
			Spec: corev1.PodSpec{
				ServiceAccountName: cluster.Spec.ServiceAccountName,
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
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "data-storage",
								MountPath: "/antflydb",
							},
							{
								Name:      "config",
								MountPath: "/config",
							},
						},
						Command: []string{"/bin/sh", "-c"},
						Args: []string{
							fmt.Sprintf(`
ORDINAL=${HOSTNAME##*-}
ID=$((ORDINAL + 1))
exec /antfly store --id $ID --config /config/config.json \
  --api http://0.0.0.0:%d \
  --raft http://0.0.0.0:%d \
  --health-port %d \
  --service ${HOSTNAME}.%s-data.%s.svc.cluster.local
							`,
								cluster.Spec.DataNodes.API.Port,
								cluster.Spec.DataNodes.Raft.Port,
								cluster.Spec.DataNodes.Health.Port,
								cluster.Name,
								cluster.Namespace,
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
				Volumes: []corev1.Volume{
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
				},
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

// buildStorageInitContainer creates an init container that waits for the PVC to be properly mounted.
// This prevents a race condition where the main container starts before the PVC is attached,
// which could cause Antfly to bootstrap a fresh cluster instead of recovering from existing data.
func (r *AntflyClusterReconciler) buildStorageInitContainer(volumeName string) corev1.Container {
	return corev1.Container{
		Name:    "wait-for-storage",
		Image:   "busybox:1.36",
		Command: []string{"/bin/sh", "-c"},
		Args: []string{`
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
        exit 0
    fi
    echo "Waiting for PVC mount... ($timeout seconds remaining)"
    sleep 2
    timeout=$((timeout - 2))
done
echo "ERROR: PVC mount timeout after 120 seconds"
exit 1
`},
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
		termiteEnabled := swarm.Termite == nil || swarm.Termite.Enabled
		cluster.Status.SwarmStatus.Ready = readyReplicas >= swarm.Replicas && swarm.Replicas > 0
		cluster.Status.SwarmStatus.MetadataReady = cluster.Status.SwarmStatus.Ready
		cluster.Status.SwarmStatus.StoreReady = cluster.Status.SwarmStatus.Ready
		cluster.Status.SwarmStatus.TermiteReady = !termiteEnabled || cluster.Status.SwarmStatus.Ready
		cluster.Status.SwarmStatus.NodeID = swarm.NodeID

		if completeConfig, err := r.generateSwarmConfig(cluster); err == nil {
			sum := sha256.Sum256([]byte(completeConfig))
			cluster.Status.SwarmStatus.ObservedConfigHash = fmt.Sprintf("%x", sum)[:16]
		}

		var podList corev1.PodList
		if err := r.List(ctx, &podList, client.InNamespace(cluster.Namespace), client.MatchingLabels(serviceSelectorLabels(cluster.Name, "swarm"))); err == nil {
			for _, pod := range podList.Items {
				if pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodPending {
					cluster.Status.SwarmStatus.PodName = pod.Name
					cluster.Status.SwarmStatus.PodIP = pod.Status.PodIP
					break
				}
			}
		}

		if oldStatus.LastTransitionTime == nil ||
			cluster.Status.SwarmStatus.Ready != oldStatus.Ready ||
			cluster.Status.SwarmStatus.MetadataReady != oldStatus.MetadataReady ||
			cluster.Status.SwarmStatus.StoreReady != oldStatus.StoreReady ||
			cluster.Status.SwarmStatus.TermiteReady != oldStatus.TermiteReady ||
			cluster.Status.SwarmStatus.PodName != oldStatus.PodName ||
			cluster.Status.SwarmStatus.PodIP != oldStatus.PodIP ||
			cluster.Status.SwarmStatus.NodeID != oldStatus.NodeID ||
			cluster.Status.SwarmStatus.ObservedConfigHash != oldStatus.ObservedConfigHash {
			now := metav1.Now()
			cluster.Status.SwarmStatus.LastTransitionTime = &now
		}

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
			cluster.Status.AutoScalingStatus.CurrentReplicas = dataSts.Status.Replicas
		}
	}

	// Determine phase based on the configured replica counts rather than a
	// hardcoded production-sized cluster. Local dev intentionally runs 1+1.
	metadataReplicas := int32(3)
	if cluster.Spec.MetadataNodes.Replicas > 0 {
		metadataReplicas = cluster.Spec.MetadataNodes.Replicas
	}
	dataReplicas := int32(3)
	if cluster.Spec.DataNodes.Replicas > 0 {
		dataReplicas = cluster.Spec.DataNodes.Replicas
	}

	if cluster.Status.MetadataNodesReady >= metadataReplicas && cluster.Status.DataNodesReady >= dataReplicas {
		cluster.Status.Phase = "Running"
	} else {
		cluster.Status.Phase = "Pending"
	}

	// Update ServiceMeshReady condition
	r.updateServiceMeshReadyCondition(cluster)

	return r.Status().Update(ctx, cluster)
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

// reconcilePVCExpansion patches existing PVCs when the CRD specifies a larger storage size.
// This is a best-effort operation — failures are reported as status conditions but don't block reconciliation.
func (r *AntflyClusterReconciler) reconcilePVCExpansion(ctx context.Context, cluster *antflyv1.AntflyCluster, vctName, stsName, desiredSizeStr string) {
	if desiredSizeStr == "" {
		return // no size specified, using default
	}

	log := log.FromContext(ctx)
	desiredSize := resource.MustParse(desiredSizeStr)

	// List PVCs matching this StatefulSet's VolumeClaimTemplate naming: {vctName}-{stsName}-{ordinal}
	prefix := vctName + "-" + stsName + "-"
	var pvcList corev1.PersistentVolumeClaimList
	if err := r.List(ctx, &pvcList, client.InNamespace(cluster.Namespace),
		client.MatchingLabels{"app.kubernetes.io/instance": cluster.Name}); err != nil {
		log.Error(err, "Failed to list PVCs for expansion check")
		return
	}

	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		if !strings.HasPrefix(pvc.Name, prefix) {
			continue // secondary guard: multiple StatefulSets share the instance label
		}

		currentSize := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		if desiredSize.Cmp(currentSize) <= 0 {
			continue // already at or above desired size
		}

		log.Info("Expanding PVC", "pvc", pvc.Name, "from", currentSize.String(), "to", desiredSize.String())
		pvc.Spec.Resources.Requests[corev1.ResourceStorage] = desiredSize
		if err := r.Update(ctx, pvc); err != nil {
			log.Error(err, "Failed to expand PVC", "pvc", pvc.Name)
			r.setStorageCondition(cluster, metav1.ConditionFalse, antflyv1.ReasonPVCExpansionFailed,
				fmt.Sprintf("Failed to expand PVC %s: %v", pvc.Name, err))
		}
	}
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
	return ctrl.NewControllerManagedBy(mgr).
		For(&antflyv1.AntflyCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Complete(r)
}
