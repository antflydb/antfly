package controllers

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strings"
	"testing"
	"time"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	inferencev1alpha1 "github.com/antflydb/antfly/go/pkg/operator/api/inference/v1alpha1"
	adminsdk "github.com/antflydb/antfly/go/pkg/sdk/admin"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gstruct"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func TestEffectiveTopologyModeFailsClosedForUnknownStoredMode(t *testing.T) {
	cluster := &antflyv1.AntflyCluster{Spec: antflyv1.AntflyClusterSpec{Mode: antflyv1.ClusterMode("RemovedMode")}}
	if got := effectiveTopologyMode(cluster); got != topologyModeInvalid {
		t.Fatalf("expected invalid topology for unknown stored mode, got %q", got)
	}
}

func TestTopologySafetyRejectsUnexpectedOwnedStatefulSet(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := antflyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	controller := true
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default", UID: types.UID("cluster-uid")},
		Spec:       antflyv1.AntflyClusterSpec{Mode: antflyv1.ClusterModeStandalone},
	}
	oldWorkload := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name:      "example-old-topology",
		Namespace: "default",
		Labels:    map[string]string{"app.kubernetes.io/instance": "example"},
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: antflyv1.GroupVersion.String(), Kind: "AntflyCluster", Name: cluster.Name,
			UID: cluster.UID, Controller: &controller,
		}},
	}}
	reconciler := &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, oldWorkload).Build()}
	err := reconciler.ensureTopologyResourcesMatchMode(context.Background(), cluster, topologyModeStandalone)
	if err == nil || !strings.Contains(err.Error(), "migrate or remove") {
		t.Fatalf("expected explicit migration safety error, got %v", err)
	}
}

func TestTopologySafetyRejectsUnownedCanonicalStatefulSet(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := antflyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default", UID: types.UID("cluster-uid")},
		Spec:       antflyv1.AntflyClusterSpec{Mode: antflyv1.ClusterModeStandalone},
	}
	conflict := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: "example-standalone", Namespace: "default",
	}}
	reconciler := &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, conflict).Build()}
	err := reconciler.ensureTopologyResourcesMatchMode(context.Background(), cluster, topologyModeStandalone)
	if err == nil || !strings.Contains(err.Error(), "not controlled by AntflyCluster UID") {
		t.Fatalf("expected canonical-name ownership collision, got %v", err)
	}
}

func TestCleanupDeletesUIDBoundPVCFromOwnedStatefulSet(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{antflyv1.AddToScheme, appsv1.AddToScheme, corev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	controller := true
	replicas := int32(1)
	cluster := &antflyv1.AntflyCluster{ObjectMeta: metav1.ObjectMeta{
		Name: "example", Namespace: "default", UID: types.UID("cluster-uid"),
	}}
	historical := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "example-historical", Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: antflyv1.GroupVersion.String(), Kind: "AntflyCluster", Name: cluster.Name,
				UID: cluster.UID, Controller: &controller,
			}},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "database"},
			}},
		},
	}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "database-example-historical-0", Namespace: "default",
		Labels: map[string]string{labelClusterUID: string(cluster.UID)},
	}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, historical, pvc).Build()
	reconciler := &AntflyClusterReconciler{Client: client}
	result, err := reconciler.cleanupStorageResources(context.Background(), cluster)
	if err != nil || result != nil {
		t.Fatalf("cleanup failed: result=%v err=%v", result, err)
	}
	err = client.Get(context.Background(), types.NamespacedName{Name: pvc.Name, Namespace: pvc.Namespace}, &corev1.PersistentVolumeClaim{})
	if !errors.IsNotFound(err) {
		t.Fatalf("expected UID-bound PVC to be deleted, got %v", err)
	}
}

func TestCleanupDoesNotDeleteUnownedCanonicalNameStatefulSet(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{antflyv1.AddToScheme, appsv1.AddToScheme, corev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	cluster := &antflyv1.AntflyCluster{ObjectMeta: metav1.ObjectMeta{
		Name: "example", Namespace: "default", UID: types.UID("cluster-uid"),
	}}
	unowned := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: "example-data", Namespace: "default",
	}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, unowned).Build()
	reconciler := &AntflyClusterReconciler{Client: client}
	result, err := reconciler.cleanupStorageResources(context.Background(), cluster)
	if err != nil || result != nil {
		t.Fatalf("cleanup failed: result=%v err=%v", result, err)
	}
	err = client.Get(context.Background(), types.NamespacedName{Name: unowned.Name, Namespace: unowned.Namespace}, &appsv1.StatefulSet{})
	if err != nil {
		t.Fatalf("unowned canonical-name StatefulSet was deleted: %v", err)
	}
}

func TestCleanupDoesNotTreatInstanceLabelAsOwnership(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{antflyv1.AddToScheme, appsv1.AddToScheme, corev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	cluster := &antflyv1.AntflyCluster{ObjectMeta: metav1.ObjectMeta{
		Name: "example", Namespace: "default", UID: types.UID("cluster-uid"),
	}}
	foreign := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: "another-application", Namespace: "default",
		Labels: map[string]string{"app.kubernetes.io/instance": cluster.Name},
	}}
	foreignPVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "foreign-data", Namespace: "default",
		Labels: map[string]string{"app.kubernetes.io/instance": cluster.Name},
	}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, foreign, foreignPVC).Build()
	reconciler := &AntflyClusterReconciler{Client: client}
	result, err := reconciler.cleanupStorageResources(context.Background(), cluster)
	if err != nil || result != nil {
		t.Fatalf("cleanup failed: result=%v err=%v", result, err)
	}
	if err := client.Get(context.Background(), types.NamespacedName{Name: foreign.Name, Namespace: foreign.Namespace}, &appsv1.StatefulSet{}); err != nil {
		t.Fatalf("label-only StatefulSet was deleted: %v", err)
	}
	if err := client.Get(context.Background(), types.NamespacedName{Name: foreignPVC.Name, Namespace: foreignPVC.Namespace}, &corev1.PersistentVolumeClaim{}); err != nil {
		t.Fatalf("label-only PVC was deleted: %v", err)
	}
}

func TestCleanupFailsClosedBeforeDeletingAnyPVCWithoutUIDOwnership(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{antflyv1.AddToScheme, appsv1.AddToScheme, corev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	cluster := &antflyv1.AntflyCluster{ObjectMeta: metav1.ObjectMeta{
		Name: "example", Namespace: "default", UID: types.UID("cluster-uid"),
	}}
	labeled := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "standalone-storage-example-standalone-0", Namespace: "default",
		Labels: map[string]string{
			"app.kubernetes.io/instance": cluster.Name,
			labelClusterUID:              string(cluster.UID),
		},
	}}
	unlabeled := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "data-storage-example-data-0", Namespace: "default",
	}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, labeled, unlabeled).Build()
	reconciler := &AntflyClusterReconciler{Client: client}
	result, err := reconciler.cleanupStorageResources(context.Background(), cluster)
	if result != nil || err == nil || !strings.Contains(err.Error(), labelClusterUID) {
		t.Fatalf("expected UID ownership failure: result=%v err=%v", result, err)
	}
	for _, name := range []string{labeled.Name, unlabeled.Name} {
		err := client.Get(context.Background(), types.NamespacedName{Name: name, Namespace: cluster.Namespace}, &corev1.PersistentVolumeClaim{})
		if err != nil {
			t.Fatalf("PVC %s was deleted before ownership preflight completed: %v", name, err)
		}
	}
}

func TestCleanupFailsClosedOnConflictingDiscoveredPVCLabel(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{antflyv1.AddToScheme, appsv1.AddToScheme, corev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	controller := true
	cluster := &antflyv1.AntflyCluster{ObjectMeta: metav1.ObjectMeta{
		Name: "example", Namespace: "default", UID: types.UID("cluster-uid"),
	}}
	historical := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "example-historical", Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: antflyv1.GroupVersion.String(), Kind: "AntflyCluster", Name: cluster.Name,
				UID: cluster.UID, Controller: &controller,
			}},
		},
		Spec: appsv1.StatefulSetSpec{VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
			ObjectMeta: metav1.ObjectMeta{Name: "database"},
		}}},
	}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "database-example-historical-0", Namespace: "default",
		Labels: map[string]string{
			"app.kubernetes.io/instance": "another-cluster",
			labelClusterUID:              string(cluster.UID),
		},
	}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, historical, pvc).Build()
	reconciler := &AntflyClusterReconciler{Client: client}
	result, err := reconciler.cleanupStorageResources(context.Background(), cluster)
	if result != nil || err == nil || !strings.Contains(err.Error(), "refusing PVC cleanup") {
		t.Fatalf("expected fail-closed ownership conflict, result=%v err=%v", result, err)
	}
	if err := client.Get(context.Background(), types.NamespacedName{Name: pvc.Name, Namespace: pvc.Namespace}, &corev1.PersistentVolumeClaim{}); err != nil {
		t.Fatalf("conflicting PVC was deleted: %v", err)
	}
	if err := client.Get(context.Background(), types.NamespacedName{Name: historical.Name, Namespace: historical.Namespace}, &appsv1.StatefulSet{}); err != nil {
		t.Fatalf("historical StatefulSet was deleted before PVC preflight completed: %v", err)
	}
	// A second reconcile must fail closed in exactly the same way; this catches
	// orphaning bugs where the first attempt deletes the workload that supplied
	// the non-canonical claim prefix.
	result, err = reconciler.cleanupStorageResources(context.Background(), cluster)
	if result != nil || err == nil || !strings.Contains(err.Error(), "refusing PVC cleanup") {
		t.Fatalf("expected retry to preserve fail-closed conflict, result=%v err=%v", result, err)
	}
}

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func haFenceResponseJSON(oldPrimaryID, promotedNodeID string, generation uint64, token string) string {
	body, err := json.Marshal(map[string]any{
		"schema_version": 1,
		"action": map[string]any{
			"action_id":   "fence_acquire:" + promotedNodeID,
			"action_kind": "fence_acquire",
			"target":      promotedNodeID,
			"state":       "applied",
			"node_id":     promotedNodeID,
		},
		"receipt": map[string]any{
			"identity": map[string]any{
				"cluster_id":  100,
				"shard_id":    10,
				"table_id":    20,
				"timeline_id": 5,
				"epoch":       7,
			},
			"old_primary_id":     oldPrimaryID,
			"promoted_node_id":   promotedNodeID,
			"parent_timeline_id": 4,
			"parent_epoch":       6,
			"new_timeline_id":    5,
			"new_epoch":          7,
			"required_lsn":       12,
			"observed_lsn":       12,
			"generation":         generation,
			"forced":             false,
			"token":              token,
			"reason":             "LeaseAcquired",
		},
	})
	if err != nil {
		panic(err)
	}
	return string(body)
}

func haFenceResponseJSONWithoutReceiptPath(path ...string) string {
	var body map[string]any
	if err := json.Unmarshal([]byte(haFenceResponseJSON("primary-a", "standby-a", 3, "ha-fence-token")), &body); err != nil {
		panic(err)
	}
	var current map[string]any
	receipt, ok := body["receipt"].(map[string]any)
	if !ok {
		panic("fence response missing receipt")
	}
	current = receipt
	for _, segment := range path[:len(path)-1] {
		next, ok := current[segment].(map[string]any)
		if !ok {
			panic("fence response missing nested receipt path")
		}
		current = next
	}
	delete(current, path[len(path)-1])
	mutated, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	return string(mutated)
}

func haPromotionResponseJSON() string {
	body, err := json.Marshal(map[string]any{
		"schema_version": 1,
		"action": map[string]any{
			"action_id":   "promotion:standby-a",
			"action_kind": "promotion",
			"target":      "standby-a",
			"state":       "applied",
			"node_id":     "standby-a",
		},
		"assessment": map[string]any{
			"required_lsn":          12,
			"received_lsn":          12,
			"applied_lsn":           12,
			"has_required_lsn":      true,
			"caught_up_to_received": true,
			"fencing_confirmed":     true,
			"force":                 false,
			"mode":                  "safe",
			"data_loss_possible":    false,
			"safe":                  true,
			"requires_fencing":      false,
			"requires_force":        false,
			"can_promote":           true,
		},
		"promotion": map[string]any{
			"node_id":    "standby-a",
			"switch_lsn": 13,
			"old_identity": map[string]any{
				"cluster_id":  100,
				"shard_id":    10,
				"table_id":    20,
				"timeline_id": 4,
				"epoch":       6,
			},
			"new_identity": map[string]any{
				"cluster_id":  100,
				"shard_id":    10,
				"table_id":    20,
				"timeline_id": 5,
				"epoch":       7,
			},
			"forced":             false,
			"data_loss_possible": false,
		},
		"fence_generation": 3,
		"fence_token":      "ha-fence-token",
		"forced":           false,
	})
	if err != nil {
		panic(err)
	}
	return string(body)
}

func haPromotionResponseJSONWithoutPath(path ...string) string {
	var body map[string]any
	if err := json.Unmarshal([]byte(haPromotionResponseJSON()), &body); err != nil {
		panic(err)
	}
	current := body
	for _, segment := range path[:len(path)-1] {
		next, ok := current[segment].(map[string]any)
		if !ok {
			panic("promotion response missing nested path")
		}
		current = next
	}
	delete(current, path[len(path)-1])
	mutated, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	return string(mutated)
}

func haReplicationSlotActionResponseJSON(actionKind, slotAction, slotName, nodeID string) string {
	body, err := json.Marshal(map[string]any{
		"schema_version": 1,
		"action": map[string]any{
			"action_id":   actionKind + ":" + slotName,
			"action_kind": actionKind,
			"target":      slotName,
			"state":       "applied",
			"node_id":     nodeID,
		},
		"slot_action": slotAction,
		"slot": map[string]any{
			"slot_name":       slotName,
			"timeline_id":     4,
			"restart_lsn":     5,
			"received_lsn":    5,
			"applied_lsn":     5,
			"safe_read_lsn":   5,
			"active":          true,
			"reseed_required": false,
			"current_lsn":     9,
		},
	})
	if err != nil {
		panic(err)
	}
	return string(body)
}

func haPromotionAdminResult(generation uint64, token string, promotedNodeID string) *antflyv1.HAAdminActionResultStatus {
	return &antflyv1.HAAdminActionResultStatus{
		SchemaVersion:         1,
		ActionID:              "promotion:" + promotedNodeID,
		ActionKind:            "promotion",
		ActionTarget:          promotedNodeID,
		ActionState:           "applied",
		ActionNodeID:          promotedNodeID,
		FenceGeneration:       generation,
		FenceToken:            token,
		FenceClusterID:        100,
		FenceShardID:          10,
		FenceTableID:          20,
		FenceOldPrimaryID:     "primary-a",
		FencePromotedNodeID:   promotedNodeID,
		FenceParentTimelineID: 4,
		FenceParentEpoch:      6,
		FenceNewTimelineID:    5,
		FenceNewEpoch:         7,
		FenceRequiredLSN:      12,
		FenceObservedLSN:      12,
		PromotionMode:         "safe",
	}
}

func mergeStringMaps(base map[string]string, overlay map[string]string) map[string]string {
	merged := maps.Clone(base)
	maps.Copy(merged, overlay)
	return merged
}

func statefulSetOwnerRef(name string) metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{
		APIVersion: "apps/v1",
		Kind:       "StatefulSet",
		Name:       name,
		Controller: &controller,
	}
}

// T004: Unit test for applyDefaults() setting ServiceMesh.Enabled=false
func TestApplyDefaults_ServiceMeshDefaults(t *testing.T) {
	g := NewWithT(t)

	// Setup scheme
	s := runtime.NewScheme()
	err := antflyv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())

	// Create reconciler
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).Build(),
		Scheme: s,
	}

	// Test Case 1: Cluster without ServiceMesh field (nil)
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 3,
			},
			ServiceMesh: nil, // Explicitly nil
		},
	}

	// Apply defaults
	reconciler.applyDefaults(cluster)

	// Verify ServiceMesh is initialized with default Enabled=false
	g.Expect(cluster.Spec.ServiceMesh).ToNot(BeNil())
	g.Expect(cluster.Spec.ServiceMesh.Enabled).To(BeFalse())

	// Test Case 2: Cluster with ServiceMesh field but Enabled not set
	clusterWithMesh := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-mesh",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 3,
			},
			ServiceMesh: &antflyv1.ServiceMeshSpec{
				Annotations: map[string]string{
					"sidecar.istio.io/inject": "true",
				},
			},
		},
	}

	// Apply defaults
	reconciler.applyDefaults(clusterWithMesh)

	// Verify default Enabled is false (Go zero value)
	g.Expect(clusterWithMesh.Spec.ServiceMesh.Enabled).To(BeFalse())

	// Test Case 3: Cluster with ServiceMesh explicitly enabled
	clusterEnabled := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-enabled",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 3,
			},
			ServiceMesh: &antflyv1.ServiceMeshSpec{
				Enabled: true,
				Annotations: map[string]string{
					"sidecar.istio.io/inject": "true",
				},
			},
		},
	}

	// Apply defaults
	reconciler.applyDefaults(clusterEnabled)

	// Verify Enabled remains true
	g.Expect(clusterEnabled.Spec.ServiceMesh.Enabled).To(BeTrue())
}

func TestReconcileHAAdminJobsExecutesPlannedActionsInOrder(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	initial := uint64(5)
	backoffLimit := int32(1)
	timeoutSeconds := int64(120)
	ttlSecondsAfterFinished := int32(3600)
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image:              "antfly:test",
			ImagePullPolicy:    "IfNotPresent",
			ServiceAccountName: "antfly-ha-admin",
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:                 "http://primary-ha.default.svc:8081",
					ExecutePlannedActions:      true,
					JobBackoffLimit:            &backoffLimit,
					JobTimeoutSeconds:          &timeoutSeconds,
					JobTTLSecondsAfterFinished: &ttlSecondsAfterFinished,
					EnvFrom: []corev1.EnvFromSource{{
						SecretRef: &corev1.SecretEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "backup-credentials"},
						},
					}},
					Volumes: []corev1.Volume{{
						Name: "ha-seed",
						VolumeSource: corev1.VolumeSource{
							EmptyDir: &corev1.EmptyDirVolumeSource{},
						},
					}},
					VolumeMounts: []corev1.VolumeMount{{
						Name:      "ha-seed",
						MountPath: "/backup",
					}},
				},
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID:        100,
					TimelineID:       4,
					Epoch:            6,
					CurrentPrimaryID: "primary-a",
				},
				Standbys: []antflyv1.HAStandbySpec{{
					Name:       "standby-a",
					InitialLSN: &initial,
					AdminURL:   "http://standby-a-ha.default.svc:8081",
				}},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{PrimaryLSN: 9},
		},
	}

	var observed []string
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			observed = append(observed, req.Method+" "+req.URL.Path)
			switch req.URL.Path {
			case "/admin/v1/ha/replication-slots":
				g.Expect(req.Method).To(Equal(http.MethodPost))
				var payload map[string]any
				g.Expect(json.NewDecoder(req.Body).Decode(&payload)).To(Succeed())
				g.Expect(payload["slot_name"]).To(Equal("standby-a"))
				g.Expect(payload["initial_lsn"]).To(Equal(float64(5)))
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(haReplicationSlotActionResponseJSON("replication_slot_create", "create", "standby-a", "primary-a"))),
				}, nil
			case "/admin/v1/ha/base-backups":
				g.Expect(req.Method).To(Equal(http.MethodPost))
				var payload map[string]any
				g.Expect(json.NewDecoder(req.Body).Decode(&payload)).To(Succeed())
				g.Expect(payload["slot_name"]).To(Equal("standby-a"))
				g.Expect(payload["manifest_id"]).To(Equal("base-standby-a-10"))
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(`{"schema_version":1,"action":{"action_id":"base_backup_begin:base-standby-a-10","action_kind":"base_backup_begin","target":"base-standby-a-10","state":"applied","node_id":"primary-a"},"slot_name":"standby-a","manifest_id":"base-standby-a-10","backup_lsn":10,"start_record_lsn":10}`)),
				}, nil
			default:
				t.Fatalf("unexpected HA admin API request: %s %s", req.Method, req.URL.Path)
				return nil, nil
			}
		})},
	}
	reconciler.updateHAStatusAndConditions(cluster)

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(observed).To(Equal([]string{
		"POST /admin/v1/ha/replication-slots",
		"POST /admin/v1/ha/base-backups",
	}))

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(BeEmpty())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	jobs = batchv1.JobList{}
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(BeEmpty())
	g.Expect(observed).To(HaveLen(2))
}

func TestReconcileHAAdminJobsExecutesTypedActionWithoutCLIArgv(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:        string(haActionCreateSlot),
					StandbyName: "standby-a",
					TargetLSN:   5,
					AdminURL:    "http://primary-ha.default.svc:8081",
					AdminNodeID: "primary-a",
					AdminMethod: "POST",
					AdminPath:   "/admin/v1/ha/replication-slots",
				}},
			},
		},
	}

	var observed []string
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			observed = append(observed, req.Method+" "+req.URL.Path)
			g.Expect(req.Method).To(Equal(http.MethodPost))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/replication-slots"))
			var payload map[string]any
			g.Expect(json.NewDecoder(req.Body).Decode(&payload)).To(Succeed())
			g.Expect(payload["slot_name"]).To(Equal("standby-a"))
			g.Expect(payload["initial_lsn"]).To(Equal(float64(5)))
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(haReplicationSlotActionResponseJSON("replication_slot_create", "create", "standby-a", "primary-a"))),
			}, nil
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(observed).To(Equal([]string{"POST /admin/v1/ha/replication-slots"}))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult).NotTo(BeNil())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.SlotAction).To(Equal("create"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.SlotName).To(Equal("standby-a"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.ActionNodeID).To(Equal("primary-a"))

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(BeEmpty())
}

func TestReconcileHAAdminJobsUsesConfiguredAdminTokenEnvVar(t *testing.T) {
	g := NewWithT(t)
	t.Setenv("CUSTOM_HA_ADMIN_TOKEN", "operator-token")

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
					TokenEnvVar:           "CUSTOM_HA_ADMIN_TOKEN",
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:        string(haActionCreateSlot),
					StandbyName: "standby-a",
					TargetLSN:   5,
					AdminURL:    "http://primary-ha.default.svc:8081",
					AdminNodeID: "primary-a",
					AdminMethod: "POST",
					AdminPath:   "/admin/v1/ha/replication-slots",
				}},
			},
		},
	}

	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Header.Get("Authorization")).To(Equal("Bearer operator-token"))
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(haReplicationSlotActionResponseJSON("replication_slot_create", "create", "standby-a", "primary-a"))),
			}, nil
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
}

func TestReconcileHAAdminJobsFailsWhenConfiguredAdminTokenEnvVarMissing(t *testing.T) {
	g := NewWithT(t)
	t.Setenv("MISSING_HA_ADMIN_TOKEN", "")

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
					TokenEnvVar:           "MISSING_HA_ADMIN_TOKEN",
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:        string(haActionCreateSlot),
					StandbyName: "standby-a",
					TargetLSN:   5,
					AdminURL:    "http://primary-ha.default.svc:8081",
					AdminNodeID: "primary-a",
					AdminMethod: "POST",
					AdminPath:   "/admin/v1/ha/replication-slots",
				}},
			},
		},
	}

	called := false
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			called = true
			return nil, fmt.Errorf("unexpected direct admin request: %s %s", req.Method, req.URL.String())
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(called).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseFailed))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(ContainSubstring("configured HA admin token env var MISSING_HA_ADMIN_TOKEN is empty or unset"))
}

func TestReconcileHAAdminJobsFallsBackToCLIJobWhenConfiguredAdminTokenEnvVarComesFromEnvFrom(t *testing.T) {
	g := NewWithT(t)
	t.Setenv("MISSING_HA_ADMIN_TOKEN", "")

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:test",
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
					TokenEnvVar:           "MISSING_HA_ADMIN_TOKEN",
					EnvFrom: []corev1.EnvFromSource{{
						SecretRef: &corev1.SecretEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "ha-admin-token"},
						},
					}},
				},
				Runtime: &antflyv1.HARuntimeSpec{
					AdminTokenEnvVar: "MISSING_HA_ADMIN_TOKEN",
					AdminTokenSecretRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "ha-admin-token"},
						Key:                  "token",
					},
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:         string(haActionResumeSlot),
					Executor:     string(haActionExecutorAdminAPI),
					SlotName:     "standby-a",
					AdminCommand: []string{"slot", "resume", "--slot", "standby-a"},
					AdminURL:     "http://primary-ha.default.svc:8081",
					AdminNodeID:  "primary-a",
					AdminMethod:  "PUT",
					AdminPath:    "/admin/v1/ha/replication-slots/standby-a/resume",
				}},
			},
		},
	}

	called := false
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			called = true
			return nil, fmt.Errorf("unexpected direct admin request: %s %s", req.Method, req.URL.String())
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(called).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).NotTo(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhasePending))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(BeEmpty())

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(HaveLen(1))
	container := jobs.Items[0].Spec.Template.Spec.Containers[0]
	g.Expect(container.Args).To(Equal([]string{
		"ha",
		"--ha-url", "http://primary-ha.default.svc:8081",
		"--ha-token-env", "MISSING_HA_ADMIN_TOKEN",
		"--",
		"slot", "resume", "--slot", "standby-a",
	}))
	g.Expect(container.EnvFrom).To(Equal(cluster.Spec.HighAvailability.Admin.EnvFrom))
	g.Expect(container.Env).To(HaveLen(1))
	g.Expect(container.Env[0].Name).To(Equal("MISSING_HA_ADMIN_TOKEN"))
	g.Expect(container.Env[0].ValueFrom).NotTo(BeNil())
	g.Expect(container.Env[0].ValueFrom.SecretKeyRef).NotTo(BeNil())
	g.Expect(container.Env[0].ValueFrom.SecretKeyRef.Name).To(Equal("ha-admin-token"))
	g.Expect(container.Env[0].ValueFrom.SecretKeyRef.Key).To(Equal("token"))
	g.Expect(container.Env[0].ValueFrom.SecretKeyRef.Optional).NotTo(BeNil())
	g.Expect(*container.Env[0].ValueFrom.SecretKeyRef.Optional).To(BeFalse())
}

func TestReconcileHAAdminJobsFallsBackToCLIJobWhenConfiguredAdminTokenEnvVarComesFromRuntimeSecret(t *testing.T) {
	g := NewWithT(t)
	t.Setenv("MISSING_HA_ADMIN_TOKEN", "")

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:test",
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
					TokenEnvVar:           "MISSING_HA_ADMIN_TOKEN",
				},
				Runtime: &antflyv1.HARuntimeSpec{
					AdminTokenEnvVar: "MISSING_HA_ADMIN_TOKEN",
					AdminTokenSecretRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "ha-admin-token"},
						Key:                  "token",
					},
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:         string(haActionResumeSlot),
					Executor:     string(haActionExecutorAdminAPI),
					SlotName:     "standby-a",
					AdminCommand: []string{"slot", "resume", "--slot", "standby-a"},
					AdminURL:     "http://primary-ha.default.svc:8081",
					AdminNodeID:  "primary-a",
					AdminMethod:  "PUT",
					AdminPath:    "/admin/v1/ha/replication-slots/standby-a/resume",
				}},
			},
		},
	}

	called := false
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			called = true
			return nil, fmt.Errorf("unexpected direct admin request: %s %s", req.Method, req.URL.String())
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(called).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).NotTo(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhasePending))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(BeEmpty())

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(HaveLen(1))
	container := jobs.Items[0].Spec.Template.Spec.Containers[0]
	g.Expect(container.EnvFrom).To(BeEmpty())
	g.Expect(container.Env).To(HaveLen(1))
	g.Expect(container.Env[0].Name).To(Equal("MISSING_HA_ADMIN_TOKEN"))
	g.Expect(container.Env[0].ValueFrom).NotTo(BeNil())
	g.Expect(container.Env[0].ValueFrom.SecretKeyRef).NotTo(BeNil())
	g.Expect(container.Env[0].ValueFrom.SecretKeyRef.Name).To(Equal("ha-admin-token"))
	g.Expect(container.Env[0].ValueFrom.SecretKeyRef.Key).To(Equal("token"))
	g.Expect(container.Env[0].ValueFrom.SecretKeyRef.Optional).NotTo(BeNil())
	g.Expect(*container.Env[0].ValueFrom.SecretKeyRef.Optional).To(BeFalse())
}

func TestReconcileHAAdminJobsRecoversStaleDirectAPIMissingTokenFailureWithEnvFromFallback(t *testing.T) {
	g := NewWithT(t)
	t.Setenv("MISSING_HA_ADMIN_TOKEN", "")

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:test",
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
					TokenEnvVar:           "MISSING_HA_ADMIN_TOKEN",
					EnvFrom: []corev1.EnvFromSource{{
						SecretRef: &corev1.SecretEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "ha-admin-token"},
						},
					}},
				},
				Runtime: &antflyv1.HARuntimeSpec{
					AdminTokenEnvVar: "MISSING_HA_ADMIN_TOKEN",
					AdminTokenSecretRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "ha-admin-token"},
						Key:                  "token",
					},
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:          string(haActionResumeSlot),
					Executor:      string(haActionExecutorAdminAPI),
					SlotName:      "standby-a",
					AdminCommand:  []string{"slot", "resume", "--slot", "standby-a"},
					AdminURL:      "http://primary-ha.default.svc:8081",
					AdminNodeID:   "primary-a",
					AdminMethod:   "PUT",
					AdminPath:     "/admin/v1/ha/replication-slots/standby-a/resume",
					AdminJobName:  haAdminDirectAPIName,
					AdminJobPhase: haAdminJobPhaseFailed,
					AdminError:    "configured HA admin token env var MISSING_HA_ADMIN_TOKEN is empty or unset",
				}},
			},
		},
	}

	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).NotTo(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhasePending))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(BeEmpty())

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(HaveLen(1))
	container := jobs.Items[0].Spec.Template.Spec.Containers[0]
	g.Expect(container.Env).To(HaveLen(1))
	g.Expect(container.Env[0].Name).To(Equal("MISSING_HA_ADMIN_TOKEN"))
	g.Expect(container.Env[0].ValueFrom.SecretKeyRef.Name).To(Equal("ha-admin-token"))
	g.Expect(container.Env[0].ValueFrom.SecretKeyRef.Key).To(Equal("token"))
}

func TestHAAdminBearerTokenDoesNotReadRuntimeSecretRef(t *testing.T) {
	g := NewWithT(t)
	t.Setenv("MISSING_HA_ADMIN_TOKEN", "")

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Admin: &antflyv1.HAAdminSpec{
					TokenEnvVar: "MISSING_HA_ADMIN_TOKEN",
				},
				Runtime: &antflyv1.HARuntimeSpec{
					AdminTokenSecretRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "ha-admin-token"},
						Key:                  "token",
					},
				},
			},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "ha-admin-token", Namespace: "default"},
		Data:       map[string][]byte{"token": []byte("secret-token")},
	}
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster, secret).Build(),
		Scheme: s,
	}

	_, err := reconciler.haAdminBearerToken(cluster)
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("configured HA admin token env var MISSING_HA_ADMIN_TOKEN is empty or unset"))
}

func TestReconcileHAAdminJobsRecreatesMissingFailedFallbackJob(t *testing.T) {
	g := NewWithT(t)
	t.Setenv("MISSING_HA_ADMIN_TOKEN", "")

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	action := antflyv1.HAPlannedActionStatus{
		Kind:         string(haActionResumeSlot),
		Executor:     string(haActionExecutorAdminAPI),
		SlotName:     "standby-a",
		AdminCommand: []string{"slot", "resume", "--slot", "standby-a"},
		AdminURL:     "http://primary-ha.default.svc:8081",
		AdminNodeID:  "primary-a",
		AdminMethod:  "PUT",
		AdminPath:    "/admin/v1/ha/replication-slots/standby-a/resume",
	}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:test",
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
					TokenEnvVar:           "MISSING_HA_ADMIN_TOKEN",
					EnvFrom: []corev1.EnvFromSource{{
						SecretRef: &corev1.SecretEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "ha-admin-token"},
						},
					}},
				},
				Runtime: &antflyv1.HARuntimeSpec{
					AdminTokenEnvVar: "MISSING_HA_ADMIN_TOKEN",
					AdminTokenSecretRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "ha-admin-token"},
						Key:                  "token",
					},
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{action},
			},
		},
	}
	cluster.Status.HAStatus.PlannedActions[0].AdminJobName = haAdminJobName(cluster, action)
	cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase = haAdminJobPhaseFailed

	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(Equal(haAdminJobName(cluster, action)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhasePending))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(BeEmpty())

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(HaveLen(1))
	container := jobs.Items[0].Spec.Template.Spec.Containers[0]
	g.Expect(container.Env).To(HaveLen(1))
	g.Expect(container.Env[0].Name).To(Equal("MISSING_HA_ADMIN_TOKEN"))
	g.Expect(container.Env[0].ValueFrom.SecretKeyRef.Name).To(Equal("ha-admin-token"))
	g.Expect(container.Env[0].ValueFrom.SecretKeyRef.Key).To(Equal("token"))
}

func TestHAAdminSDKResponseHelpersPreserveTypedErrors(t *testing.T) {
	g := NewWithT(t)

	apiErr := &adminsdk.HAAPIError{
		Operation:  "create HA replication slot",
		StatusCode: http.StatusServiceUnavailable,
		Body:       "primary unavailable",
	}
	_, err := haAdminSDKResponseValue[adminsdk.HAReplicationSlotActionResponse](nil, apiErr)
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("HA admin API returned status 503"))
	var observedAPIError *adminsdk.HAAPIError
	g.Expect(stderrors.As(err, &observedAPIError)).To(BeTrue())
	g.Expect(adminsdk.HAIsRetryable(err)).To(BeTrue())

	validationErr := &adminsdk.HAResponseValidationError{
		Operation: "create HA replication slot",
		Err:       fmt.Errorf("missing action receipt"),
	}
	_, err = haAdminSDKResponseValue[adminsdk.HAReplicationSlotActionResponse](nil, validationErr)
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("typed result evidence"))
	var observedValidationError *adminsdk.HAResponseValidationError
	g.Expect(stderrors.As(err, &observedValidationError)).To(BeTrue())
	g.Expect(adminsdk.HAIsRetryable(err)).To(BeFalse())
}

func TestReconcileHAAdminJobsUsesDefaultAdminTokenEnvVar(t *testing.T) {
	g := NewWithT(t)
	t.Setenv(haAdminTokenDefaultEnvVar, "default-operator-token")

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:        string(haActionCreateSlot),
					StandbyName: "standby-a",
					TargetLSN:   5,
					AdminURL:    "http://primary-ha.default.svc:8081",
					AdminNodeID: "primary-a",
					AdminMethod: "POST",
					AdminPath:   "/admin/v1/ha/replication-slots",
				}},
			},
		},
	}

	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Header.Get("Authorization")).To(Equal("Bearer default-operator-token"))
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(haReplicationSlotActionResponseJSON("replication_slot_create", "create", "standby-a", "primary-a"))),
			}, nil
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
}

func TestReconcileHAAdminJobsPassesConfiguredTokenEnvToCLIJob(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image:              "antfly:test",
			ImagePullPolicy:    "IfNotPresent",
			ServiceAccountName: "antfly-ha-admin",
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
					TokenEnvVar:           "CUSTOM_HA_ADMIN_TOKEN",
					EnvFrom: []corev1.EnvFromSource{{
						SecretRef: &corev1.SecretEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "ha-admin-token"},
						},
					}},
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:         string(haActionUpdatePrimaryRoute),
					Executor:     string(haActionExecutorCLIJob),
					AdminCommand: []string{"identify"},
					AdminURL:     "http://primary-ha.default.svc:8081",
				}},
			},
		},
	}

	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhasePending))

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(HaveLen(1))
	container := jobs.Items[0].Spec.Template.Spec.Containers[0]
	g.Expect(container.Args).To(Equal([]string{
		"ha",
		"--ha-url", "http://primary-ha.default.svc:8081",
		"--ha-token-env", "CUSTOM_HA_ADMIN_TOKEN",
		"--",
		"identify",
	}))
	g.Expect(container.EnvFrom).To(Equal(cluster.Spec.HighAvailability.Admin.EnvFrom))
}

func TestReconcileHAAdminJobsDoesNotPassDefaultTokenEnvToCLIJob(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:test",
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:         string(haActionUpdatePrimaryRoute),
					Executor:     string(haActionExecutorCLIJob),
					AdminCommand: []string{"identify"},
					AdminURL:     "http://primary-ha.default.svc:8081",
				}},
			},
		},
	}

	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(HaveLen(1))
	g.Expect(jobs.Items[0].Spec.Template.Spec.Containers[0].Args).To(Equal([]string{
		"ha",
		"--ha-url", "http://primary-ha.default.svc:8081",
		"--",
		"identify",
	}))
}

func TestReconcileHAAdminJobsRejectsDirectAPIMissingAdminNodeID(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:        string(haActionCreateSlot),
					StandbyName: "standby-a",
					TargetLSN:   5,
					AdminURL:    "http://primary-ha.default.svc:8081",
					AdminMethod: "POST",
					AdminPath:   "/admin/v1/ha/replication-slots",
				}},
			},
		},
	}

	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			t.Fatalf("direct HA admin action without AdminNodeID must not issue HTTP request: %s %s", req.Method, req.URL.Path)
			return nil, nil
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseFailed))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(ContainSubstring("adminNodeID"))

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(BeEmpty())
}

func TestReconcileHAAdminJobsDoesNotFallbackFromAdminAPIToCLIJob(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:test",
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:         string(haActionCreateSlot),
					Executor:     string(haActionExecutorAdminAPI),
					AdminCommand: []string{"slot", "create", "--slot", "standby-a"},
					AdminURL:     "http://primary-ha.default.svc:8081",
				}},
			},
		},
	}

	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			t.Fatalf("AdminAPI action without typed request inputs must not fall back or issue HTTP request: %s %s", req.Method, req.URL.Path)
			return nil, nil
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseFailed))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(ContainSubstring("marked AdminAPI"))

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(BeEmpty())
}

func TestReconcileHAAdminJobsDoesNotRunImplicitCLIJob(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:test",
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:         string(haActionUpdatePrimaryRoute),
					AdminCommand: []string{"slot", "create", "--slot", "standby-a"},
					AdminURL:     "http://primary-ha.default.svc:8081",
				}},
			},
		},
	}

	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			t.Fatalf("blank-executor action must not issue typed admin API request: %s %s", req.Method, req.URL.Path)
			return nil, nil
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(BeEmpty())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(BeEmpty())

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(BeEmpty())
}

func TestReconcileHAAdminJobsRejectsCLIJobForTypedAdminAction(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image:              "antfly:test",
			ImagePullPolicy:    "IfNotPresent",
			ServiceAccountName: "antfly-ha-admin",
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:         string(haActionCreateSlot),
					Executor:     string(haActionExecutorCLIJob),
					StandbyName:  "standby-a",
					SlotName:     "standby-a",
					AdminCommand: []string{"slot", "create", "--slot", "standby-a"},
					AdminURL:     "http://primary-ha.default.svc:8081",
				}},
			},
		},
	}

	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			t.Fatalf("CLIJob action must not issue typed admin API request: %s %s", req.Method, req.URL.Path)
			return nil, nil
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseFailed))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(BeEmpty())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(ContainSubstring("marked CLIJob"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(ContainSubstring("typed /admin/v1"))

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(BeEmpty())
}

func TestReconcileHAAdminJobsMarksDirectAPIFailure(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:         string(haActionCreateSlot),
					SlotName:     "standby-a",
					AdminCommand: []string{"slot", "create", "--slot", "standby-a"},
					AdminURL:     "http://primary-ha.default.svc:8081",
					AdminNodeID:  "primary-a",
				}},
			},
		},
	}
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/replication-slots"))
			return &http.Response{
				StatusCode: http.StatusConflict,
				Body:       io.NopCloser(strings.NewReader("slot conflict")),
			}, nil
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseFailed))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminStatusCode).To(Equal(http.StatusConflict))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(ContainSubstring("status 409"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(ContainSubstring("slot conflict"))

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(BeEmpty())

	reconciler.updateHAAdminJobExecutionCondition(cluster)
	degraded := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	g.Expect(degraded).NotTo(BeNil())
	g.Expect(degraded.Status).To(Equal(metav1.ConditionTrue))
	g.Expect(degraded.Reason).To(Equal(antflyv1.ReasonHAAdminJobFailed))
	g.Expect(degraded.Message).To(ContainSubstring(haAdminDirectAPIName))
	g.Expect(degraded.Message).To(ContainSubstring("status 409"))
}

func TestReconcileHAAdminJobsReportsUnauthorizedDirectAPIFailure(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:         string(haActionCreateSlot),
					SlotName:     "standby-a",
					AdminCommand: []string{"slot", "create", "--slot", "standby-a"},
					AdminURL:     "http://primary-ha.default.svc:8081",
					AdminNodeID:  "primary-a",
				}},
			},
		},
	}
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/replication-slots"))
			return &http.Response{
				StatusCode: http.StatusUnauthorized,
				Body:       io.NopCloser(strings.NewReader("missing bearer token")),
			}, nil
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	action := cluster.Status.HAStatus.PlannedActions[0]
	g.Expect(action.AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(action.AdminJobPhase).To(Equal(haAdminJobPhaseFailed))
	g.Expect(action.AdminStatusCode).To(Equal(http.StatusUnauthorized))
	g.Expect(action.AdminError).To(ContainSubstring("status 401"))
	g.Expect(action.AdminError).To(ContainSubstring("missing bearer token"))

	reconciler.updateHAAdminJobExecutionCondition(cluster)
	degraded := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	g.Expect(degraded).NotTo(BeNil())
	g.Expect(degraded.Status).To(Equal(metav1.ConditionTrue))
	g.Expect(degraded.Reason).To(Equal(antflyv1.ReasonHAAdminUnauthorized))
	g.Expect(degraded.Message).To(ContainSubstring(haAdminDirectAPIName))
	g.Expect(degraded.Message).To(ContainSubstring("status 401"))
}

func TestReconcileHAAdminJobsRetriesRetryableDirectAPIFailure(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:         string(haActionCreateSlot),
					SlotName:     "standby-a",
					AdminCommand: []string{"slot", "create", "--slot", "standby-a"},
					AdminURL:     "http://primary-ha.default.svc:8081",
					AdminNodeID:  "primary-a",
				}},
			},
		},
	}
	requests := 0
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			requests++
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/replication-slots"))
			if requests == 1 {
				return &http.Response{
					StatusCode: http.StatusServiceUnavailable,
					Body:       io.NopCloser(strings.NewReader("primary restarting")),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(haReplicationSlotActionResponseJSON("replication_slot_create", "create", "standby-a", "primary-a"))),
			}, nil
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhasePending))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminStatusCode).To(Equal(http.StatusServiceUnavailable))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(ContainSubstring("status 503"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(ContainSubstring("primary restarting"))
	reconciler.updateHAAdminJobExecutionCondition(cluster)
	degraded := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	g.Expect(degraded).NotTo(BeNil())
	g.Expect(degraded.Status).To(Equal(metav1.ConditionTrue))
	g.Expect(degraded.Reason).To(Equal(antflyv1.ReasonHAAdminActionRetrying))
	g.Expect(degraded.Message).To(ContainSubstring("primary restarting"))

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(requests).To(Equal(2))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(BeEmpty())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminStatusCode).To(BeZero())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult).NotTo(BeNil())

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(BeEmpty())
}

func TestReconcileHAAdminJobsRejectsDirectAPIMissingTypedResult(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:         string(haActionCreateSlot),
					SlotName:     "standby-a",
					AdminCommand: []string{"slot", "create", "--slot", "standby-a"},
					AdminURL:     "http://primary-ha.default.svc:8081",
					AdminNodeID:  "primary-a",
				}},
			},
		},
	}
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/replication-slots"))
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"schema_version":1,"slot":{"slot_name":"standby-a"}}`)),
			}, nil
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseFailed))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(ContainSubstring("typed result evidence"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult).To(BeNil())

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(BeEmpty())
}

func TestReconcileHAAdminJobsRejectsDirectAPIMismatchedResultEvidence(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:         string(haActionCreateSlot),
					SlotName:     "standby-a",
					AdminCommand: []string{"slot", "create", "--slot", "standby-a"},
					AdminURL:     "http://primary-ha.default.svc:8081",
					AdminNodeID:  "primary-a",
				}},
			},
		},
	}
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/replication-slots"))
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(haReplicationSlotActionResponseJSON("replication_slot_create", "create", "standby-a", "primary-b"))),
			}, nil
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseFailed))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(ContainSubstring("typed result evidence"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult).To(BeNil())
}

func TestReconcileHAAdminJobsRejectsDirectSeedWithoutTargetLSN(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:     string(haActionSeedStandby),
					SlotName: "standby-a",
					AdminURL: "http://primary-ha.default.svc:8081",
				}},
			},
		},
	}
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			t.Fatalf("seed without target LSN must fail before HTTP request, got %s %s", req.Method, req.URL.String())
			return nil, nil
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseFailed))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(ContainSubstring("nonzero target LSN"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult).To(BeNil())

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(BeEmpty())
}

func TestUpdateHAAdminJobExecutionConditionReportsMissingResultEvidence(t *testing.T) {
	g := NewWithT(t)

	cluster := &antflyv1.AntflyCluster{
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:          string(haActionFinishStandbySeed),
					AdminCommand:  []string{"seed", "finish"},
					AdminJobName:  "finish-seed-job",
					AdminJobPhase: haAdminJobPhaseSucceeded,
				}, {
					Kind:         string(haActionBootstrapStandbySeed),
					DependsOn:    string(haActionFinishStandbySeed),
					AdminCommand: []string{"seed", "bootstrap"},
				}},
			},
		},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAAdminJobExecutionCondition(cluster)

	degraded := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	g.Expect(degraded).NotTo(BeNil())
	g.Expect(degraded.Status).To(Equal(metav1.ConditionTrue))
	g.Expect(degraded.Reason).To(Equal(antflyv1.ReasonHAAdminResultMissing))
	g.Expect(degraded.Message).To(ContainSubstring("FinishStandbySeed"))
	g.Expect(degraded.Message).To(ContainSubstring("finish-seed-job"))
	g.Expect(degraded.Message).To(ContainSubstring("typed result evidence"))
}

func TestUpdateHAAdminJobExecutionConditionReportsFormerPrimaryResultWithoutPromotionReceipt(t *testing.T) {
	g := NewWithT(t)

	cluster := &antflyv1.AntflyCluster{
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				LastPromotion: &antflyv1.HAPromotionStatus{
					OldPrimaryID:      "primary-a",
					PromotedStandbyID: "standby-a",
					ParentTimelineID:  4,
					ParentEpoch:       6,
					NewTimelineID:     5,
					NewEpoch:          7,
					SwitchLSN:         12,
					RequiredLSN:       12,
					ObservedLSN:       12,
					FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
					FenceGeneration:   3,
				},
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:            string(haActionRewindFormerPrimary),
					StandbyName:     "primary-a",
					TargetLSN:       12,
					ObservedLSN:     13,
					RetainedFromLSN: 8,
					FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
					FenceGeneration: 3,
					AdminNodeID:     "primary-a",
					AdminJobName:    haAdminDirectAPIName,
					AdminJobPhase:   haAdminJobPhaseSucceeded,
					AdminResult: &antflyv1.HAAdminActionResultStatus{
						SchemaVersion:           1,
						ActionID:                "rejoin_rewind:primary-a",
						ActionKind:              "rejoin_rewind",
						ActionTarget:            "primary-a",
						ActionState:             "applied",
						ActionNodeID:            "primary-a",
						RejoinAction:            "rewind",
						FormerNodeID:            "primary-a",
						TargetTimelineID:        5,
						TargetEpoch:             7,
						ForkLSN:                 12,
						FormerLastLSN:           13,
						RetainedFromLSN:         8,
						RewindExecuted:          true,
						RewindPreviousLastLSN:   13,
						RewindCurrentLastLSN:    12,
						RewindNextLSN:           13,
						RewindDiscardedLSNCount: 1,
					},
				}},
			},
		},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAAdminJobExecutionCondition(cluster)

	degraded := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	g.Expect(degraded).NotTo(BeNil())
	g.Expect(degraded.Status).To(Equal(metav1.ConditionTrue))
	g.Expect(degraded.Reason).To(Equal(antflyv1.ReasonHAAdminResultMissing))
	g.Expect(degraded.Message).To(ContainSubstring("RewindFormerPrimary"))
	g.Expect(degraded.Message).To(ContainSubstring("dependent HA actions remain blocked"))
}

func TestUpdateHAAdminJobExecutionConditionReportsMismatchedDirectAdminReceipt(t *testing.T) {
	g := NewWithT(t)

	cluster := &antflyv1.AntflyCluster{
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:          string(haActionCreateSlot),
					SlotName:      "standby-a",
					AdminJobName:  haAdminDirectAPIName,
					AdminJobPhase: haAdminJobPhaseSucceeded,
					AdminResult: &antflyv1.HAAdminActionResultStatus{
						SchemaVersion: 1,
						ActionID:      "replication_slot_create:standby-b",
						ActionKind:    "replication_slot_create",
						ActionTarget:  "standby-b",
						ActionState:   "applied",
						ActionNodeID:  "primary-a",
						SlotAction:    "create",
						SlotName:      "standby-a",
					},
				}, {
					Kind:      string(haActionSeedStandby),
					DependsOn: string(haActionCreateSlot),
				}},
			},
		},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAAdminJobExecutionCondition(cluster)

	degraded := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	g.Expect(degraded).NotTo(BeNil())
	g.Expect(degraded.Status).To(Equal(metav1.ConditionTrue))
	g.Expect(degraded.Reason).To(Equal(antflyv1.ReasonHAAdminResultMissing))
	g.Expect(degraded.Message).To(ContainSubstring("CreateSlot"))
	g.Expect(degraded.Message).To(ContainSubstring(haAdminDirectAPIName))
	g.Expect(degraded.Message).To(ContainSubstring("typed result evidence"))
}

func TestUpdateHAAdminJobExecutionConditionReportsMismatchedCLIReceipt(t *testing.T) {
	g := NewWithT(t)

	cluster := &antflyv1.AntflyCluster{
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:          string(haActionCreateSlot),
					SlotName:      "standby-a",
					AdminCommand:  []string{"slot", "create", "--slot", "standby-a"},
					AdminJobName:  "create-slot-job",
					AdminJobPhase: haAdminJobPhaseSucceeded,
					AdminResult: &antflyv1.HAAdminActionResultStatus{
						ActionID:     "replication_slot_create:standby-b",
						ActionKind:   "replication_slot_create",
						ActionTarget: "standby-b",
						ActionState:  "applied",
						SlotAction:   "create",
						SlotName:     "standby-a",
					},
				}, {
					Kind:      string(haActionSeedStandby),
					DependsOn: string(haActionCreateSlot),
				}},
			},
		},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAAdminJobExecutionCondition(cluster)

	degraded := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	g.Expect(degraded).NotTo(BeNil())
	g.Expect(degraded.Status).To(Equal(metav1.ConditionTrue))
	g.Expect(degraded.Reason).To(Equal(antflyv1.ReasonHAAdminResultMissing))
	g.Expect(degraded.Message).To(ContainSubstring("CreateSlot"))
	g.Expect(degraded.Message).To(ContainSubstring("create-slot-job"))
	g.Expect(degraded.Message).To(ContainSubstring("matching action receipt"))
}

func TestReconcileHAAdminJobsRejectsMismatchedTypedAdminOperation(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:         string(haActionCreateSlot),
					SlotName:     "standby-a",
					AdminCommand: []string{"slot", "create", "--slot", "standby-a"},
					AdminURL:     "http://primary-ha.default.svc:8081",
					AdminMethod:  "DELETE",
					AdminPath:    "/admin/v1/ha/replication-slots/standby-a",
				}},
			},
		},
	}
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			t.Fatalf("mismatched typed admin metadata must fail before issuing HTTP request: %s %s", req.Method, req.URL.Path)
			return nil, nil
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseFailed))

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(BeEmpty())
}

func TestReconcileHAAdminJobsExecutesFenceAndPromoteViaAdminAPI(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
				},
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID:        100,
					ShardID:          10,
					TableID:          20,
					TimelineID:       4,
					Epoch:            6,
					CurrentPrimaryID: "primary-a",
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:            string(haActionAcquireFence),
					StandbyName:     "standby-a",
					TargetLSN:       12,
					FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
					FenceHolder:     "standby-a",
					FenceGeneration: 3,
					FenceReason:     "LeaseAcquired",
					Reason:          "AutomaticFailoverReady",
					AdminCommand:    []string{"fence", "acquire"},
					AdminURL:        "http://standby-a-ha.default.svc:8081",
					AdminNodeID:     "standby-a",
				}, {
					Kind:            string(haActionAssessPromotion),
					DependsOn:       string(haActionAcquireFence),
					StandbyName:     "standby-a",
					TargetLSN:       12,
					FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
					FenceGeneration: 3,
					FenceReason:     "LeaseAcquired",
					AdminCommand:    []string{"promote", "assess", "--current-fence"},
					AdminURL:        "http://standby-a-ha.default.svc:8081",
					AdminNodeID:     "standby-a",
				}, {
					Kind:            string(haActionPromoteStandby),
					DependsOn:       string(haActionAssessPromotion),
					StandbyName:     "standby-a",
					TargetLSN:       12,
					FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
					FenceGeneration: 3,
					FenceReason:     "LeaseAcquired",
					AdminCommand:    []string{"promote", "--current-fence"},
					AdminURL:        "http://standby-a-ha.default.svc:8081",
					AdminNodeID:     "standby-a",
				}},
			},
		},
	}
	var observed []string
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			observed = append(observed, req.Method+" "+req.URL.Path)
			switch req.URL.Path {
			case "/admin/v1/ha/fence":
				g.Expect(req.Method).To(Equal(http.MethodPost))
				var payload map[string]any
				g.Expect(json.NewDecoder(req.Body).Decode(&payload)).To(Succeed())
				g.Expect(payload["old_primary_id"]).To(Equal("primary-a"))
				g.Expect(payload["promoted_node_id"]).To(Equal("standby-a"))
				g.Expect(payload["new_timeline_id"]).To(Equal(float64(5)))
				g.Expect(payload["new_epoch"]).To(Equal(float64(7)))
				g.Expect(payload["required_lsn"]).To(Equal(float64(12)))
				g.Expect(payload["observed_lsn"]).To(Equal(float64(12)))
				g.Expect(payload["reason"]).To(Equal("LeaseAcquired"))
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(haFenceResponseJSON("primary-a", "standby-a", 3, "ha-fence-token"))),
				}, nil
			case "/admin/v1/ha/promotion/assess":
				g.Expect(req.Method).To(Equal(http.MethodPost))
				var payload map[string]any
				g.Expect(json.NewDecoder(req.Body).Decode(&payload)).To(Succeed())
				g.Expect(payload["required_lsn"]).To(Equal(float64(12)))
				g.Expect(payload["fencing_confirmed"]).To(Equal(false))
				g.Expect(payload["force"]).To(Equal(false))
				g.Expect(payload["use_current_fence"]).To(Equal(true))
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(`{"schema_version":1,"action":{"action_id":"promotion_assess:standby-a","action_kind":"promotion_assess","target":"standby-a","state":"assessed","node_id":"standby-a"},"assessment":{"required_lsn":12,"received_lsn":12,"applied_lsn":12,"has_required_lsn":true,"caught_up_to_received":true,"fencing_confirmed":true,"force":false,"mode":"safe","data_loss_possible":false,"safe":true,"requires_fencing":false,"requires_force":false,"can_promote":true}}`)),
				}, nil
			case "/admin/v1/ha/promotion/current-fence":
				g.Expect(req.Method).To(Equal(http.MethodPost))
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(`{"schema_version":1,"action":{"action_id":"promotion:standby-a","action_kind":"promotion","target":"standby-a","state":"applied","node_id":"standby-a"},"assessment":{"required_lsn":12,"received_lsn":12,"applied_lsn":12,"has_required_lsn":true,"caught_up_to_received":true,"fencing_confirmed":true,"force":false,"mode":"safe","data_loss_possible":false,"safe":true,"requires_fencing":false,"requires_force":false,"can_promote":true},"promotion":{"node_id":"standby-a","switch_lsn":13,"old_identity":{"cluster_id":100,"shard_id":10,"table_id":20,"timeline_id":4,"epoch":6},"new_identity":{"cluster_id":100,"shard_id":10,"table_id":20,"timeline_id":5,"epoch":7},"forced":false,"data_loss_possible":false},"fence_generation":3,"fence_token":"ha-fence-token","forced":false}`)),
				}, nil
			default:
				t.Fatalf("unexpected HA admin API request: %s", req.URL.Path)
				return nil, nil
			}
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(observed).To(Equal([]string{
		"POST /admin/v1/ha/fence",
		"POST /admin/v1/ha/promotion/assess",
		"POST /admin/v1/ha/promotion/current-fence",
	}))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult).NotTo(BeNil())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.FenceGeneration).To(Equal(uint64(3)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.FenceToken).To(Equal("ha-fence-token"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.FenceClusterID).To(Equal(uint64(100)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.FencePromotedNodeID).To(Equal("standby-a"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.FenceNewTimelineID).To(Equal(uint64(5)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminResult).NotTo(BeNil())
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminResult.ActionID).To(Equal("promotion_assess:standby-a"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminResult.ActionKind).To(Equal("promotion_assess"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminResult.ActionState).To(Equal("assessed"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminResult.PromotionRequiredLSN).To(Equal(uint64(12)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminResult.PromotionAppliedLSN).To(Equal(uint64(12)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminResult.PromotionCanPromote).To(BeTrue())
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminResult.PromotionFenced).To(BeTrue())
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminResult.PromotionSafe).To(BeTrue())
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminResult.PromotionForce).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminResult.PromotionDataLossPossible).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminResult.PromotionRequiresFencing).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminResult.PromotionRequiresForce).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminResult).NotTo(BeNil())
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminResult.ActionID).To(Equal("promotion:standby-a"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminResult.ActionKind).To(Equal("promotion"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminResult.ActionState).To(Equal("applied"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminResult.FenceGeneration).To(Equal(uint64(3)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminResult.FenceToken).To(Equal("ha-fence-token"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminResult.FenceOldPrimaryID).To(Equal("primary-a"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminResult.FencePromotedNodeID).To(Equal("standby-a"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminResult.FenceReason).To(Equal("LeaseAcquired"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminResult.PromotionForce).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminResult.PromotionDataLossPossible).To(BeFalse())

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(BeEmpty())

	promotion := cluster.Status.HAStatus.LastPromotion
	g.Expect(promotion).NotTo(BeNil())
	g.Expect(promotion.PromotedStandbyID).To(Equal("standby-a"))
	g.Expect(promotion.ParentTimelineID).To(Equal(uint64(4)))
	g.Expect(promotion.NewTimelineID).To(Equal(uint64(5)))
	g.Expect(promotion.SwitchLSN).To(Equal(uint64(13)))
	g.Expect(promotion.ObservedLSN).To(Equal(uint64(12)))
	g.Expect(promotion.FenceGeneration).To(Equal(uint64(3)))
	g.Expect(promotion.FenceToken).To(Equal("ha-fence-token"))
	g.Expect(promotion.FenceAuthority).To(Equal(antflyv1.HAFencingAuthorityKubernetesLease))
}

func TestReconcileHAAdminJobsRejectsUnsafePromotionAssessment(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{
			name: "cannot promote",
			body: `{"schema_version":1,"action":{"action_id":"promotion_assess:standby-a","action_kind":"promotion_assess","target":"standby-a","state":"assessed","node_id":"standby-a"},"assessment":{"required_lsn":12,"received_lsn":12,"applied_lsn":12,"has_required_lsn":true,"caught_up_to_received":true,"fencing_confirmed":true,"force":false,"mode":"blocked","data_loss_possible":false,"safe":false,"requires_fencing":false,"requires_force":false,"can_promote":false}}`,
		},
		{
			name: "missing applied lsn",
			body: `{"schema_version":1,"action":{"action_id":"promotion_assess:standby-a","action_kind":"promotion_assess","target":"standby-a","state":"assessed","node_id":"standby-a"},"assessment":{"required_lsn":12,"received_lsn":12,"has_required_lsn":true,"caught_up_to_received":true,"fencing_confirmed":true,"force":false,"mode":"safe","data_loss_possible":false,"safe":true,"requires_fencing":false,"requires_force":false,"can_promote":true}}`,
		},
		{
			name: "missing fence confirmation",
			body: `{"schema_version":1,"action":{"action_id":"promotion_assess:standby-a","action_kind":"promotion_assess","target":"standby-a","state":"assessed","node_id":"standby-a"},"assessment":{"required_lsn":12,"received_lsn":12,"applied_lsn":12,"has_required_lsn":true,"caught_up_to_received":true,"force":false,"mode":"safe","data_loss_possible":false,"safe":true,"requires_fencing":false,"requires_force":false,"can_promote":true}}`,
		},
		{
			name: "missing force field",
			body: `{"schema_version":1,"action":{"action_id":"promotion_assess:standby-a","action_kind":"promotion_assess","target":"standby-a","state":"assessed","node_id":"standby-a"},"assessment":{"required_lsn":12,"received_lsn":12,"applied_lsn":12,"has_required_lsn":true,"caught_up_to_received":true,"fencing_confirmed":true,"mode":"safe","data_loss_possible":false,"safe":true,"requires_fencing":false,"requires_force":false,"can_promote":true}}`,
		},
		{
			name: "forced assessment",
			body: `{"schema_version":1,"action":{"action_id":"promotion_assess:standby-a","action_kind":"promotion_assess","target":"standby-a","state":"assessed","node_id":"standby-a"},"assessment":{"required_lsn":12,"received_lsn":12,"applied_lsn":12,"has_required_lsn":true,"caught_up_to_received":true,"fencing_confirmed":true,"force":true,"mode":"forced","data_loss_possible":false,"safe":true,"requires_fencing":false,"requires_force":false,"can_promote":true}}`,
		},
		{
			name: "lossy force assessment",
			body: `{"schema_version":1,"action":{"action_id":"promotion_assess:standby-a","action_kind":"promotion_assess","target":"standby-a","state":"assessed","node_id":"standby-a"},"assessment":{"required_lsn":12,"received_lsn":12,"applied_lsn":11,"has_required_lsn":true,"caught_up_to_received":false,"fencing_confirmed":true,"force":true,"mode":"lossy","data_loss_possible":true,"safe":false,"requires_fencing":false,"requires_force":false,"can_promote":true}}`,
		},
		{
			name: "requires force",
			body: `{"schema_version":1,"action":{"action_id":"promotion_assess:standby-a","action_kind":"promotion_assess","target":"standby-a","state":"assessed","node_id":"standby-a"},"assessment":{"required_lsn":12,"received_lsn":12,"applied_lsn":11,"has_required_lsn":true,"caught_up_to_received":false,"fencing_confirmed":true,"force":false,"mode":"lossy","data_loss_possible":true,"safe":false,"requires_fencing":false,"requires_force":true,"can_promote":true}}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)

			s := runtime.NewScheme()
			g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
			g.Expect(batchv1.AddToScheme(s)).To(Succeed())

			cluster := &antflyv1.AntflyCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
				},
				Spec: antflyv1.AntflyClusterSpec{
					HighAvailability: &antflyv1.HighAvailabilitySpec{
						Mode: antflyv1.HAModeHotStandby,
						Admin: &antflyv1.HAAdminSpec{
							PrimaryURL:            "http://primary-ha.default.svc:8081",
							ExecutePlannedActions: true,
						},
					},
				},
				Status: antflyv1.AntflyClusterStatus{
					HAStatus: &antflyv1.HAStatus{
						PlannedActions: []antflyv1.HAPlannedActionStatus{{
							Kind:        string(haActionAssessPromotion),
							StandbyName: "standby-a",
							TargetLSN:   12,
							AdminURL:    "http://standby-a-ha.default.svc:8081",
							AdminNodeID: "standby-a",
						}},
					},
				},
			}
			var observed []string
			reconciler := &AntflyClusterReconciler{
				Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
				Scheme: s,
				HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					observed = append(observed, req.Method+" "+req.URL.Path)
					g.Expect(req.Method).To(Equal(http.MethodPost))
					g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/promotion/assess"))
					var payload map[string]any
					g.Expect(json.NewDecoder(req.Body).Decode(&payload)).To(Succeed())
					g.Expect(payload["required_lsn"]).To(Equal(float64(12)))
					g.Expect(payload["fencing_confirmed"]).To(Equal(false))
					g.Expect(payload["force"]).To(Equal(false))
					g.Expect(payload["use_current_fence"]).To(Equal(true))
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     http.Header{"Content-Type": []string{"application/json"}},
						Body:       io.NopCloser(strings.NewReader(tc.body)),
					}, nil
				})},
			}

			g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
			g.Expect(observed).To(Equal([]string{"POST /admin/v1/ha/promotion/assess"}))
			g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(Equal(haAdminDirectAPIName))
			g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseFailed))
			g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(SatisfyAny(
				ContainSubstring("safe typed promotion assessment"),
				ContainSubstring("promotion assessment field evidence"),
				ContainSubstring("promotion assessment"),
			))
			g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult).To(BeNil())
		})
	}
}

func TestReconcileHAAdminJobsRejectsMismatchedDirectFenceReceipt(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
				},
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID:        100,
					ShardID:          10,
					TableID:          20,
					TimelineID:       4,
					Epoch:            6,
					CurrentPrimaryID: "primary-a",
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:            string(haActionAcquireFence),
					StandbyName:     "standby-a",
					TargetLSN:       12,
					FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
					FenceHolder:     "standby-a",
					FenceGeneration: 3,
					FenceReason:     "LeaseAcquired",
					Reason:          "AutomaticFailoverReady",
					AdminCommand:    []string{"fence", "acquire"},
					AdminURL:        "http://standby-a-ha.default.svc:8081",
					AdminNodeID:     "standby-a",
				}, {
					Kind:            string(haActionAssessPromotion),
					DependsOn:       string(haActionAcquireFence),
					StandbyName:     "standby-a",
					TargetLSN:       12,
					FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
					FenceGeneration: 3,
					FenceReason:     "LeaseAcquired",
					AdminCommand:    []string{"promote", "assess", "--current-fence"},
					AdminURL:        "http://standby-a-ha.default.svc:8081",
					AdminNodeID:     "standby-a",
				}},
			},
		},
	}
	var observed []string
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			observed = append(observed, req.Method+" "+req.URL.Path)
			switch req.URL.Path {
			case "/admin/v1/ha/fence":
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(haFenceResponseJSON("primary-a", "standby-b", 3, "ha-fence-token"))),
				}, nil
			default:
				t.Fatalf("unexpected HA admin API request: %s", req.URL.Path)
				return nil, nil
			}
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(observed).To(Equal([]string{"POST /admin/v1/ha/fence"}))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseFailed))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult).To(BeNil())
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminJobPhase).To(Equal(haAdminJobPhaseWaitingDependency))
	g.Expect(cluster.Status.HAStatus.LastPromotion).To(BeNil())
}

func TestReconcileHAAdminJobsRejectsDirectPromotionMissingReceipt(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
				},
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID:        100,
					ShardID:          10,
					TableID:          20,
					TimelineID:       4,
					Epoch:            6,
					CurrentPrimaryID: "primary-a",
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:            string(haActionAcquireFence),
					StandbyName:     "standby-a",
					TargetLSN:       12,
					FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
					FenceHolder:     "standby-a",
					FenceGeneration: 3,
					AdminCommand:    []string{"fence", "acquire"},
					AdminURL:        "http://standby-a-ha.default.svc:8081",
					AdminNodeID:     "standby-a",
				}, {
					Kind:            string(haActionAssessPromotion),
					DependsOn:       string(haActionAcquireFence),
					StandbyName:     "standby-a",
					TargetLSN:       12,
					FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
					FenceGeneration: 3,
					AdminCommand:    []string{"promote", "assess", "--current-fence"},
					AdminURL:        "http://standby-a-ha.default.svc:8081",
					AdminNodeID:     "standby-a",
				}},
			},
		},
	}
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/admin/v1/ha/fence":
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(haFenceResponseJSON("primary-a", "standby-a", 3, "ha-fence-token"))),
				}, nil
			case "/admin/v1/ha/promotion/assess":
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(`{"schema_version":1,"action":{"action_id":"promotion_assess:standby-a","action_kind":"promotion_assess","target":"standby-a","state":"assessed","node_id":"standby-a"},"assessment":{"required_lsn":12,"received_lsn":13,"applied_lsn":11,"has_required_lsn":true,"caught_up_to_received":false,"fencing_confirmed":true,"force":false,"mode":"blocked","data_loss_possible":false,"safe":false,"requires_fencing":false,"requires_force":false,"can_promote":false}}`)),
				}, nil
			default:
				t.Fatalf("unexpected HA admin API request: %s", req.URL.Path)
				return nil, nil
			}
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminJobPhase).To(Equal(haAdminJobPhaseFailed))
	g.Expect(cluster.Status.HAStatus.LastPromotion).To(BeNil())
}

func TestHAPromotionResultMatchesPlannedBoundary(t *testing.T) {
	g := NewWithT(t)

	identity := &antflyv1.HAReplicationIdentitySpec{
		ClusterID:  100,
		ShardID:    10,
		TableID:    20,
		TimelineID: 4,
		Epoch:      6,
	}
	action := antflyv1.HAPlannedActionStatus{
		StandbyName:     "standby-a",
		TargetLSN:       12,
		FenceGeneration: 3,
	}
	result := haPromotionJobResult{
		ActionID:         "promotion:standby-a",
		ActionKind:       "promotion",
		ActionTarget:     "standby-a",
		ActionState:      "applied",
		PromotedNodeID:   "standby-a",
		SwitchLSN:        13,
		RequiredLSN:      12,
		ObservedLSN:      12,
		ParentClusterID:  100,
		ParentShardID:    10,
		ParentTableID:    20,
		ParentTimelineID: 4,
		ParentEpoch:      6,
		NewClusterID:     100,
		NewShardID:       10,
		NewTableID:       20,
		NewTimelineID:    5,
		NewEpoch:         7,
		FenceGeneration:  3,
	}

	g.Expect(haPromotionResultMatchesAction(result, identity, &action)).To(BeTrue())

	mismatchedLSN := result
	mismatchedLSN.SwitchLSN = 12
	g.Expect(haPromotionResultMatchesAction(mismatchedLSN, identity, &action)).To(BeFalse())

	mismatchedTimeline := result
	mismatchedTimeline.NewTimelineID = 6
	g.Expect(haPromotionResultMatchesAction(mismatchedTimeline, identity, &action)).To(BeFalse())

	mismatchedScope := result
	mismatchedScope.NewTableID = 21
	g.Expect(haPromotionResultMatchesAction(mismatchedScope, identity, &action)).To(BeFalse())

	unappliedBoundary := result
	unappliedBoundary.ObservedLSN = 10
	g.Expect(haPromotionResultMatchesAction(unappliedBoundary, identity, &action)).To(BeFalse())

	wrongNode := result
	wrongNode.PromotedNodeID = "standby-b"
	g.Expect(haPromotionResultMatchesAction(wrongNode, identity, &action)).To(BeFalse())
}

func TestReconcileHAAdminJobsExecutesRejoinWorkflowViaAdminAPI(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
				},
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID:        100,
					ShardID:          10,
					TableID:          20,
					TimelineID:       4,
					Epoch:            6,
					CurrentPrimaryID: "primary-a",
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				LastPromotion: &antflyv1.HAPromotionStatus{
					OldPrimaryID:      "primary-a",
					PromotedStandbyID: "standby-a",
					ParentTimelineID:  4,
					ParentEpoch:       6,
					NewTimelineID:     5,
					NewEpoch:          7,
					SwitchLSN:         12,
					RequiredLSN:       12,
					ObservedLSN:       13,
					FenceGeneration:   3,
					FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
					FenceToken:        "ha-fence-token",
					FenceReason:       "LeaseAcquired",
				},
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:            string(haActionRewindFormerPrimary),
					StandbyName:     "primary-a",
					TargetLSN:       12,
					ObservedLSN:     13,
					RetainedFromLSN: 8,
					FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
					FenceHolder:     "standby-a",
					FenceGeneration: 3,
					AdminCommand:    []string{"rejoin", "rewind"},
					AdminURL:        "http://old-primary-ha.default.svc:8081",
					AdminNodeID:     "primary-a",
				}},
			},
		},
	}

	var observed []string
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			observed = append(observed, req.Method+" "+req.URL.Path)
			g.Expect(req.Method).To(Equal(http.MethodPost))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/rejoin/rewind"))
			var payload map[string]any
			g.Expect(json.NewDecoder(req.Body).Decode(&payload)).To(Succeed())
			g.Expect(payload["node_id"]).To(Equal("primary-a"))
			g.Expect(payload["last_lsn"]).To(Equal(float64(13)))
			g.Expect(payload["retained_from_lsn"]).To(Equal(float64(8)))
			g.Expect(payload["allow_rewind_after_forced_promotion"]).To(Equal(false))
			identity := payload["identity"].(map[string]any)
			g.Expect(identity["cluster_id"]).To(Equal(float64(100)))
			g.Expect(identity["timeline_id"]).To(Equal(float64(4)))
			receipt := payload["receipt"].(map[string]any)
			g.Expect(receipt["old_primary_id"]).To(Equal("primary-a"))
			g.Expect(receipt["promoted_node_id"]).To(Equal("standby-a"))
			g.Expect(receipt["token"]).To(Equal("ha-fence-token"))
			receiptIdentity := receipt["identity"].(map[string]any)
			g.Expect(receiptIdentity["timeline_id"]).To(Equal(float64(5)))
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"schema_version":1,"action":{"action_id":"rejoin_rewind:primary-a","action_kind":"rejoin_rewind","target":"primary-a","state":"applied","node_id":"primary-a"},"assessment":{"action":"rewind","reason":"parent_timeline_retained","former_node_id":"primary-a","target_timeline_id":5,"target_epoch":7,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":4,"parent_epoch":6,"fork_lsn":12,"former_last_lsn":13,"retained_from_lsn":8,"data_loss_discarded":true},"rewind":{"node_id":"primary-a","fork_lsn":12,"previous_last_lsn":13,"current_last_lsn":12,"next_lsn":13,"discarded_lsn_count":1,"target_timeline_id":5,"target_epoch":7,"data_loss_discarded":true}}`)),
			}, nil
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(observed).To(Equal([]string{"POST /admin/v1/ha/rejoin/rewind"}))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult).NotTo(BeNil())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.ActionID).To(Equal("rejoin_rewind:primary-a"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.ActionKind).To(Equal("rejoin_rewind"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.ActionState).To(Equal("applied"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.RejoinAction).To(Equal("rewind"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.RejoinReason).To(Equal("parent_timeline_retained"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.FormerNodeID).To(Equal("primary-a"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.TargetTimelineID).To(Equal(uint64(5)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.TargetEpoch).To(Equal(uint64(7)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.ForkLSN).To(Equal(uint64(12)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.FormerLastLSN).To(Equal(uint64(13)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.RetainedFromLSN).To(Equal(uint64(8)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.DataLossDiscarded).To(BeTrue())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.RewindExecuted).To(BeTrue())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.RewindPreviousLastLSN).To(Equal(uint64(13)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.RewindCurrentLastLSN).To(Equal(uint64(12)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.RewindNextLSN).To(Equal(uint64(13)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.RewindDiscardedLSNCount).To(Equal(uint64(1)))
	g.Expect(cluster.Status.HAStatus.FormerPrimary).NotTo(BeNil())
	g.Expect(cluster.Status.HAStatus.FormerPrimary.NodeID).To(Equal("primary-a"))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.Fenced).To(BeTrue())
	g.Expect(cluster.Status.HAStatus.FormerPrimary.RewindPossible).To(BeTrue())
	g.Expect(cluster.Status.HAStatus.FormerPrimary.TargetTimelineID).To(Equal(uint64(5)))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.TargetEpoch).To(Equal(uint64(7)))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.ForkLSN).To(Equal(uint64(12)))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.FormerLastLSN).To(Equal(uint64(13)))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.RetainedFromLSN).To(Equal(uint64(8)))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.DataLossDiscarded).To(BeTrue())
	g.Expect(cluster.Status.HAStatus.FormerPrimary.AssessedAction).To(Equal("rewind"))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.AssessedReason).To(Equal("parent_timeline_retained"))

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(BeEmpty())
}

func TestReconcileHAAdminJobsRejectsDirectRejoinWorkflowMismatchedAssessment(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
				},
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID:        100,
					ShardID:          10,
					TableID:          20,
					TimelineID:       4,
					Epoch:            6,
					CurrentPrimaryID: "primary-a",
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				LastPromotion: &antflyv1.HAPromotionStatus{
					OldPrimaryID:      "primary-a",
					PromotedStandbyID: "standby-a",
					ParentTimelineID:  4,
					ParentEpoch:       6,
					NewTimelineID:     5,
					NewEpoch:          7,
					SwitchLSN:         12,
					RequiredLSN:       12,
					ObservedLSN:       13,
					FenceGeneration:   3,
					FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
					FenceToken:        "ha-fence-token",
				},
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:            string(haActionRewindFormerPrimary),
					StandbyName:     "primary-a",
					TargetLSN:       12,
					ObservedLSN:     13,
					RetainedFromLSN: 8,
					FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
					FenceHolder:     "standby-a",
					FenceGeneration: 3,
					AdminCommand:    []string{"rejoin", "rewind"},
					AdminURL:        "http://old-primary-ha.default.svc:8081",
				}},
			},
		},
	}
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/rejoin/rewind"))
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"schema_version":1,"assessment":{"action":"rewind","reason":"parent_timeline_retained","former_node_id":"primary-b","target_timeline_id":5,"target_epoch":7,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":4,"parent_epoch":6,"fork_lsn":12,"former_last_lsn":13,"retained_from_lsn":8}}`)),
			}, nil
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseFailed))
	g.Expect(cluster.Status.HAStatus.FormerPrimary).To(BeNil())
}

func TestReconcileHAAdminJobsMarksExecutableActionMissingAdminURL(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:         string(haActionRewindFormerPrimary),
					StandbyName:  "old-primary",
					AdminCommand: []string{"rejoin", "rewind"},
					AdminMethod:  http.MethodPost,
					AdminPath:    "/admin/v1/ha/rejoin/rewind",
				}, {
					Kind:    string(haActionUpdatePrimaryRoute),
					RouteTo: "standby-a",
				}, {
					Kind:        string(haActionPromoteStandby),
					Executor:    string(haActionExecutorAdminAPI),
					StandbyName: "standby-a",
				}},
			},
		},
	}
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseMissingAdminURL))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(BeEmpty())
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminJobPhase).To(BeEmpty())
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminJobPhase).To(Equal(haAdminJobPhaseMissingAdminURL))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminJobName).To(BeEmpty())

	reconciler.updateHAAdminJobExecutionCondition(cluster)
	degraded := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	g.Expect(degraded).NotTo(BeNil())
	g.Expect(degraded.Status).To(Equal(metav1.ConditionTrue))
	g.Expect(degraded.Reason).To(Equal(antflyv1.ReasonHAAdminURLMissing))
	g.Expect(degraded.Message).To(ContainSubstring("RewindFormerPrimary"))
}

func TestHADirectRejoinResultMatchesPlannedAssessment(t *testing.T) {
	g := NewWithT(t)

	status := &antflyv1.HAStatus{
		LastPromotion: &antflyv1.HAPromotionStatus{
			OldPrimaryID:      "primary-a",
			PromotedStandbyID: "standby-a",
			ClusterID:         100,
			ShardID:           10,
			TableID:           20,
			ParentTimelineID:  4,
			ParentEpoch:       6,
			NewTimelineID:     5,
			NewEpoch:          7,
			SwitchLSN:         12,
			RequiredLSN:       12,
			ObservedLSN:       13,
			FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
			FenceGeneration:   3,
			FenceToken:        "ha-fence-token",
		},
	}
	action := antflyv1.HAPlannedActionStatus{
		Kind:            string(haActionRewindFormerPrimary),
		StandbyName:     "primary-a",
		TargetLSN:       12,
		ObservedLSN:     13,
		RetainedFromLSN: 8,
		FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
		FenceGeneration: 3,
		AdminNodeID:     "primary-a",
	}
	result := haRejoinJobResult{
		FormerNodeID:     "primary-a",
		TargetTimelineID: 5,
		TargetEpoch:      7,
		ParentClusterID:  100,
		ParentShardID:    10,
		ParentTableID:    20,
		ParentTimelineID: 4,
		ParentEpoch:      6,
		ForkLSN:          12,
		FormerLastLSN:    13,
		RetainedFromLSN:  8,
	}

	g.Expect(haDirectRejoinResultMatchesAction(result, status, action)).To(BeFalse())
	result.ActionID = "rejoin_rewind:primary-a"
	result.ActionKind = "rejoin_rewind"
	result.ActionTarget = "primary-a"
	result.ActionState = "applied"
	result.ActionNodeID = "primary-a"
	result.Action = "rewind"
	result.RewindExecuted = true
	result.RewindPreviousLastLSN = 13
	result.RewindCurrentLastLSN = 12
	result.RewindNextLSN = 13
	result.RewindDiscardedLSNCount = 1
	g.Expect(haDirectRejoinResultMatchesAction(result, status, action)).To(BeTrue())

	status.LastPromotion.FenceAuthority = ""
	g.Expect(haDirectRejoinResultMatchesAction(result, status, action)).To(BeFalse())
	status.LastPromotion.FenceAuthority = antflyv1.HAFencingAuthorityKubernetesLease

	reseedResult := haRejoinJobResult{
		ActionID:         "rejoin_reseed:primary-a",
		ActionKind:       "rejoin_reseed",
		ActionTarget:     "primary-a",
		ActionState:      "applied",
		ActionNodeID:     "primary-a",
		Action:           "reseed",
		FormerNodeID:     "primary-a",
		TargetTimelineID: 5,
		TargetEpoch:      7,
		ParentClusterID:  100,
		ParentShardID:    10,
		ParentTableID:    20,
		ParentTimelineID: 4,
		ParentEpoch:      6,
		ForkLSN:          12,
		FormerLastLSN:    13,
		RetainedFromLSN:  8,
	}
	action.Kind = string(haActionReseedFormerPrimary)
	g.Expect(haDirectRejoinResultMatchesAction(reseedResult, status, action)).To(BeFalse())
	reseedResult.ReseedExecuted = true
	reseedResult.ReseedSlotName = "primary-a"
	reseedResult.ReseedRequired = true
	reseedResult.ReseedBaseBackupRequired = true
	g.Expect(haDirectRejoinResultMatchesAction(reseedResult, status, action)).To(BeTrue())

	action.Kind = string(haActionRewindFormerPrimary)

	wrongTimeline := result
	wrongTimeline.TargetTimelineID = 6
	g.Expect(haDirectRejoinResultMatchesAction(wrongTimeline, status, action)).To(BeFalse())

	wrongParentTimeline := result
	wrongParentTimeline.ParentTimelineID = 3
	g.Expect(haDirectRejoinResultMatchesAction(wrongParentTimeline, status, action)).To(BeFalse())

	wrongFork := result
	wrongFork.ForkLSN = 11
	g.Expect(haDirectRejoinResultMatchesAction(wrongFork, status, action)).To(BeFalse())

	wrongObserved := result
	wrongObserved.FormerLastLSN = 12
	g.Expect(haDirectRejoinResultMatchesAction(wrongObserved, status, action)).To(BeFalse())
}

func TestHAFormerPrimaryActionRequiresPromotionReceiptEvidence(t *testing.T) {
	g := NewWithT(t)

	status := &antflyv1.HAStatus{
		LastPromotion: &antflyv1.HAPromotionStatus{
			OldPrimaryID:      "primary-a",
			PromotedStandbyID: "standby-a",
			ClusterID:         100,
			ShardID:           10,
			TableID:           20,
			ParentTimelineID:  4,
			ParentEpoch:       6,
			NewTimelineID:     5,
			NewEpoch:          7,
			SwitchLSN:         12,
			RequiredLSN:       12,
			ObservedLSN:       12,
			FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
			FenceGeneration:   3,
			FenceToken:        "ha-fence-token",
		},
	}
	action := antflyv1.HAPlannedActionStatus{
		Kind:            string(haActionRewindFormerPrimary),
		StandbyName:     "primary-a",
		TargetLSN:       12,
		ObservedLSN:     13,
		RetainedFromLSN: 8,
		FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
		FenceGeneration: 3,
		AdminJobPhase:   haAdminJobPhaseSucceeded,
		AdminResult: &antflyv1.HAAdminActionResultStatus{
			ActionID:                "rejoin_rewind:primary-a",
			ActionKind:              "rejoin_rewind",
			ActionTarget:            "primary-a",
			ActionState:             "applied",
			ActionNodeID:            "primary-a",
			RejoinAction:            "rewind",
			FormerNodeID:            "primary-a",
			TargetTimelineID:        5,
			TargetEpoch:             7,
			ForkLSN:                 12,
			FormerLastLSN:           13,
			RetainedFromLSN:         8,
			RewindExecuted:          true,
			RewindPreviousLastLSN:   13,
			RewindCurrentLastLSN:    12,
			RewindNextLSN:           13,
			RewindDiscardedLSNCount: 1,
		},
	}

	g.Expect(haFormerPrimaryActionSucceededWithPromotionEvidence(status, action)).To(BeTrue())
	actions := []antflyv1.HAPlannedActionStatus{action, {
		Kind:      string(haActionSeedStandby),
		DependsOn: string(haActionRewindFormerPrimary),
	}}
	g.Expect(haPlannedActionDependenciesSucceededForStatus(status, actions, 1)).To(BeTrue())

	status.LastPromotion.FenceToken = ""
	g.Expect(haFormerPrimaryActionSucceededWithPromotionEvidence(status, action)).To(BeFalse())

	status.LastPromotion.FenceToken = "ha-fence-token"
	action.AdminResult.TargetTimelineID = 6
	g.Expect(haFormerPrimaryActionSucceededWithPromotionEvidence(status, action)).To(BeFalse())

	action.AdminResult.TargetTimelineID = 5
	action.AdminResult.ForkLSN = 11
	g.Expect(haFormerPrimaryActionSucceededWithPromotionEvidence(status, action)).To(BeFalse())
	actions[0] = action
	g.Expect(haPlannedActionDependenciesSucceededForStatus(status, actions, 1)).To(BeFalse())

	action.AdminResult.ForkLSN = 12
	status.LastPromotion.Forced = true
	actions[0] = action
	g.Expect(haFormerPrimaryActionSucceededWithPromotionEvidence(status, action)).To(BeFalse())
	g.Expect(haPlannedActionDependenciesSucceededForStatus(status, actions, 1)).To(BeFalse())

	status.LastPromotion.Forced = false
	status.LastPromotion.DataLossPossible = true
	g.Expect(haFormerPrimaryActionSucceededWithPromotionEvidence(status, action)).To(BeFalse())
}

func TestReconcileHAAdminJobsExecutesSeedFinishAndBootstrap(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	initial := uint64(5)
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image:           "antfly:test",
			ImagePullPolicy: "IfNotPresent",
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
				},
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID:        100,
					TimelineID:       4,
					Epoch:            6,
					CurrentPrimaryID: "primary-a",
				},
				Standbys: []antflyv1.HAStandbySpec{{
					Name:             "standby-a",
					InitialLSN:       &initial,
					AdminURL:         "http://standby-a-ha.default.svc:8081",
					SeedManifestPath: "/backup/base-standby-a-10.afha",
					SeedContentRoot:  "/backup/base-standby-a-10",
				}},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{PrimaryLSN: 9},
		},
	}

	var observed []string
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			observed = append(observed, req.Method+" "+req.URL.Path)
			switch req.URL.Path {
			case "/admin/v1/ha/replication-slots":
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(haReplicationSlotActionResponseJSON("replication_slot_create", "create", "standby-a", "primary-a"))),
				}, nil
			case "/admin/v1/ha/base-backups":
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(`{"schema_version":1,"action":{"action_id":"base_backup_begin:base-standby-a-10","action_kind":"base_backup_begin","target":"base-standby-a-10","state":"applied","node_id":"primary-a"},"slot_name":"standby-a","manifest_id":"base-standby-a-10","backup_lsn":10,"start_record_lsn":10}`)),
				}, nil
			case "/admin/v1/ha/base-backups/finish":
				var body map[string]any
				g.Expect(json.NewDecoder(req.Body).Decode(&body)).To(Succeed())
				g.Expect(body).To(HaveKeyWithValue("manifest_path", "/backup/base-standby-a-10.afha"))
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(`{"schema_version":1,"action":{"action_id":"base_backup_finish:base-standby-a-10","action_kind":"base_backup_finish","target":"base-standby-a-10","state":"applied","node_id":"primary-a"},"manifest_id":"base-standby-a-10","backup_lsn":10,"end_record_lsn":10}`)),
				}, nil
			case "/admin/v1/ha/standby/bootstrap":
				var body map[string]any
				g.Expect(json.NewDecoder(req.Body).Decode(&body)).To(Succeed())
				g.Expect(body).To(HaveKeyWithValue("manifest_path", "/backup/base-standby-a-10.afha"))
				g.Expect(body).To(HaveKeyWithValue("content_root", "/backup/base-standby-a-10"))
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(`{"schema_version":1,"action":{"action_id":"standby_bootstrap:base-standby-a-10","action_kind":"standby_bootstrap","target":"base-standby-a-10","state":"applied","node_id":"standby-a"},"manifest_id":"base-standby-a-10","backup_lsn":10,"checkpoint_lsn":10}`)),
				}, nil
			default:
				t.Fatalf("unexpected direct HA admin request: %s", req.URL.Path)
				return nil, nil
			}
		})},
	}
	reconciler.updateHAStatusAndConditions(cluster)
	g.Expect(cluster.Status.HAStatus.PlannedActions).To(HaveLen(4))

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult).NotTo(BeNil())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.ActionID).To(Equal("replication_slot_create:standby-a"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.ActionKind).To(Equal("replication_slot_create"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.ActionTarget).To(Equal("standby-a"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.ActionState).To(Equal("applied"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.SlotAction).To(Equal("create"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.SlotName).To(Equal("standby-a"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminResult).NotTo(BeNil())
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminResult.ActionID).To(Equal("base_backup_begin:base-standby-a-10"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminResult.ManifestID).To(Equal("base-standby-a-10"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminResult.BackupLSN).To(Equal(uint64(10)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminResult.StartRecordLSN).To(Equal(uint64(10)))
	finish := cluster.Status.HAStatus.PlannedActions[2]
	g.Expect(finish.Kind).To(Equal(string(haActionFinishStandbySeed)))
	g.Expect(finish.AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
	g.Expect(finish.AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(finish.AdminResult).NotTo(BeNil())
	g.Expect(finish.AdminResult.ActionID).To(Equal("base_backup_finish:base-standby-a-10"))
	g.Expect(finish.AdminResult.EndRecordLSN).To(Equal(uint64(10)))

	bootstrap := cluster.Status.HAStatus.PlannedActions[3]
	g.Expect(bootstrap.Kind).To(Equal(string(haActionBootstrapStandbySeed)))
	g.Expect(bootstrap.AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
	g.Expect(bootstrap.AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(bootstrap.AdminResult).NotTo(BeNil())
	g.Expect(bootstrap.AdminResult.ActionID).To(Equal("standby_bootstrap:base-standby-a-10"))
	g.Expect(bootstrap.AdminResult.CheckpointLSN).To(Equal(uint64(10)))
	g.Expect(observed).To(Equal([]string{
		"POST /admin/v1/ha/replication-slots",
		"POST /admin/v1/ha/base-backups",
		"POST /admin/v1/ha/base-backups/finish",
		"POST /admin/v1/ha/standby/bootstrap",
	}))
}

func TestHAPlannedActionDependenciesPreferExplicitDependsOn(t *testing.T) {
	g := NewWithT(t)

	actions := []antflyv1.HAPlannedActionStatus{{
		Kind:          string(haActionPauseSlot),
		AdminCommand:  []string{"slot", "pause"},
		AdminJobPhase: haAdminJobPhaseFailed,
	}, {
		Kind:          string(haActionCreateSlot),
		SlotName:      "standby-a",
		AdminCommand:  []string{"slot", "create"},
		AdminJobPhase: haAdminJobPhaseSucceeded,
		AdminResult: &antflyv1.HAAdminActionResultStatus{
			ActionID:     "replication_slot_create:standby-a",
			ActionKind:   "replication_slot_create",
			ActionTarget: "standby-a",
			ActionState:  "applied",
			ActionNodeID: "primary-a",
			SlotAction:   "create",
			SlotName:     "standby-a",
		},
	}, {
		Kind:         string(haActionSeedStandby),
		DependsOn:    string(haActionCreateSlot),
		AdminCommand: []string{"seed", "begin"},
	}}

	dependentHash := haAdminActionHash(actions[2])
	g.Expect(haPlannedActionDependenciesSucceeded(actions, 2)).To(BeTrue())

	actions[2].DependsOn = ""
	g.Expect(haAdminActionHash(actions[2])).NotTo(Equal(dependentHash))
	g.Expect(haPlannedActionDependenciesSucceeded(actions, 2)).To(BeFalse())

	actions[2].DependsOn = string(haActionDropSlot)
	g.Expect(haPlannedActionDependenciesSucceeded(actions, 2)).To(BeFalse())

	g.Expect(haPlannedActionDependenciesSucceeded(actions, -1)).To(BeFalse())
	g.Expect(haPlannedActionDependenciesSucceeded(actions, len(actions))).To(BeFalse())

	route := antflyv1.HAPlannedActionStatus{Kind: string(haActionUpdatePrimaryRoute), RouteFrom: "primary", RouteTo: "standby-a"}
	routeHash := haAdminActionHash(route)
	route.RouteFrom = "standby-b"
	g.Expect(haAdminActionHash(route)).NotTo(Equal(routeHash))

	fenced := antflyv1.HAPlannedActionStatus{
		Kind:            string(haActionAcquireFence),
		FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
		FenceHolder:     "standby-a",
		FenceGeneration: 1,
		FenceReason:     "LeaseAcquired",
	}
	fencedHash := haAdminActionHash(fenced)
	fenced.FenceReason = "LeaseRenewed"
	g.Expect(haAdminActionHash(fenced)).NotTo(Equal(fencedHash))
	fenced.FenceReason = "LeaseAcquired"
	fenced.FenceGeneration = 2
	g.Expect(haAdminActionHash(fenced)).NotTo(Equal(fencedHash))
	fenced.FenceGeneration = 1
	fenced.FenceHolder = "standby-b"
	g.Expect(haAdminActionHash(fenced)).NotTo(Equal(fencedHash))
	fenced.FenceHolder = "standby-a"
	fenced.FenceAuthority = antflyv1.HAFencingAuthorityExternal
	g.Expect(haAdminActionHash(fenced)).NotTo(Equal(fencedHash))

	typed := antflyv1.HAPlannedActionStatus{
		Kind:        string(haActionCreateSlot),
		AdminMethod: http.MethodPost,
		AdminPath:   "/admin/v1/ha/replication-slots",
	}
	typedHash := haAdminActionHash(typed)
	typed.AdminMethod = http.MethodPut
	g.Expect(haAdminActionHash(typed)).NotTo(Equal(typedHash))
	typed.AdminMethod = http.MethodPost
	typed.AdminPath = "/admin/v1/ha/replication-slots/standby-a"
	g.Expect(haAdminActionHash(typed)).NotTo(Equal(typedHash))
}

func TestHAPlannedActionDependenciesRequireAdminResultEvidence(t *testing.T) {
	g := NewWithT(t)

	actions := []antflyv1.HAPlannedActionStatus{{
		Kind:          string(haActionCreateSlot),
		SlotName:      "standby-a",
		AdminCommand:  []string{"slot", "create", "--slot", "standby-a"},
		AdminJobPhase: haAdminJobPhaseSucceeded,
	}, {
		Kind:         string(haActionSeedStandby),
		DependsOn:    string(haActionCreateSlot),
		AdminCommand: []string{"seed", "begin", "--slot", "standby-a"},
	}}

	g.Expect(haPlannedActionDependenciesSucceeded(actions, 1)).To(BeFalse())

	actions[0].AdminResult = &antflyv1.HAAdminActionResultStatus{
		SlotName: "standby-a",
	}
	g.Expect(haPlannedActionDependenciesSucceeded(actions, 1)).To(BeFalse())

	actions[0].AdminResult = &antflyv1.HAAdminActionResultStatus{
		SlotAction: "create",
		SlotName:   "standby-b",
	}
	g.Expect(haPlannedActionDependenciesSucceeded(actions, 1)).To(BeFalse())

	actions[0].AdminResult = &antflyv1.HAAdminActionResultStatus{
		SlotAction: "create",
		SlotName:   "standby-a",
	}
	g.Expect(haPlannedActionDependenciesSucceeded(actions, 1)).To(BeFalse())

	actions[0].AdminResult.ActionID = "replication_slot_create:standby-a"
	actions[0].AdminResult.ActionKind = "replication_slot_create"
	actions[0].AdminResult.ActionTarget = "standby-a"
	actions[0].AdminResult.ActionState = "applied"
	actions[0].AdminResult.ActionNodeID = "primary-a"
	g.Expect(haPlannedActionDependenciesSucceeded(actions, 1)).To(BeTrue())

	seedBeginAction := antflyv1.HAPlannedActionStatus{
		Kind:          string(haActionSeedStandby),
		StandbyName:   "standby-a",
		SlotName:      "standby-a",
		TargetLSN:     5,
		AdminNodeID:   "primary-a",
		AdminCommand:  []string{"seed", "begin", "--slot", "standby-a"},
		AdminJobPhase: haAdminJobPhaseSucceeded,
		AdminResult: &antflyv1.HAAdminActionResultStatus{
			SchemaVersion:  1,
			ActionID:       "base_backup_begin:base-standby-a-5",
			ActionKind:     "base_backup_begin",
			ActionTarget:   "base-standby-a-5",
			ActionState:    "applied",
			ActionNodeID:   "primary-a",
			SlotName:       "standby-a",
			ManifestID:     "base-standby-a-5",
			BackupLSN:      5,
			StartRecordLSN: 5,
		},
	}
	g.Expect(haAdminActionSucceededWithEvidence(seedBeginAction)).To(BeTrue())
	seedBeginAction.AdminResult.BackupLSN = 4
	g.Expect(haAdminActionSucceededWithEvidence(seedBeginAction)).To(BeFalse())

	seedFileActions := []antflyv1.HAPlannedActionStatus{{
		Kind:          string(haActionFinishStandbySeed),
		StandbyName:   "standby-a",
		SlotName:      "standby-a",
		TargetLSN:     5,
		AdminCommand:  []string{"seed", "finish", "--manifest", "/backup/base-standby-a-5.afha"},
		AdminJobPhase: haAdminJobPhaseSucceeded,
		AdminResult: &antflyv1.HAAdminActionResultStatus{
			ManifestID:   "base-standby-b-5",
			BackupLSN:    5,
			EndRecordLSN: 5,
		},
	}, {
		Kind:      string(haActionBootstrapStandbySeed),
		DependsOn: string(haActionFinishStandbySeed),
		AdminCommand: []string{
			"seed", "bootstrap",
			"--manifest", "/backup/base-standby-a-5.afha",
			"--content-root", "/backup/base-standby-a-5",
		},
	}}
	g.Expect(haPlannedActionDependenciesSucceeded(seedFileActions, 1)).To(BeFalse())

	seedFileActions[0].AdminResult.ManifestID = "base-standby-a-5"
	seedFileActions[0].AdminResult.ActionID = "base_backup_finish:base-standby-a-5"
	seedFileActions[0].AdminResult.ActionKind = "base_backup_finish"
	seedFileActions[0].AdminResult.ActionTarget = "base-standby-a-5"
	seedFileActions[0].AdminResult.ActionState = "applied"
	g.Expect(haPlannedActionDependenciesSucceeded(seedFileActions, 1)).To(BeFalse())
	seedFileActions[0].AdminResult.ActionNodeID = "primary-a"
	g.Expect(haPlannedActionDependenciesSucceeded(seedFileActions, 1)).To(BeTrue())
	seedFileActions[0].AdminResult.BackupLSN = 4
	g.Expect(haPlannedActionDependenciesSucceeded(seedFileActions, 1)).To(BeFalse())
	seedFileActions[0].AdminResult.BackupLSN = 5

	seedFileActions[1].AdminJobPhase = haAdminJobPhaseSucceeded
	seedFileActions[1].StandbyName = "standby-a"
	seedFileActions[1].SlotName = "standby-a"
	seedFileActions[1].TargetLSN = 5
	seedFileActions[1].AdminResult = &antflyv1.HAAdminActionResultStatus{
		ManifestID:    "base-standby-b-5",
		BackupLSN:     5,
		CheckpointLSN: 5,
	}
	g.Expect(haAdminActionSucceededWithEvidence(seedFileActions[1])).To(BeFalse())

	seedFileActions[1].AdminResult.ManifestID = "base-standby-a-5"
	seedFileActions[1].AdminResult.ActionID = "standby_bootstrap:base-standby-a-5"
	seedFileActions[1].AdminResult.ActionKind = "standby_bootstrap"
	seedFileActions[1].AdminResult.ActionTarget = "base-standby-a-5"
	seedFileActions[1].AdminResult.ActionState = "applied"
	g.Expect(haAdminActionSucceededWithEvidence(seedFileActions[1])).To(BeFalse())
	seedFileActions[1].AdminResult.ActionNodeID = "standby-a"
	g.Expect(haAdminActionSucceededWithEvidence(seedFileActions[1])).To(BeTrue())
	seedFileActions[1].AdminResult.BackupLSN = 4
	g.Expect(haAdminActionSucceededWithEvidence(seedFileActions[1])).To(BeFalse())
	seedFileActions[1].AdminResult.BackupLSN = 5

	promotionActions := []antflyv1.HAPlannedActionStatus{{
		Kind:            string(haActionPromoteStandby),
		StandbyName:     "standby-a",
		FenceGeneration: 3,
		AdminCommand:    []string{"promote", "--current-fence"},
		AdminJobPhase:   haAdminJobPhaseSucceeded,
	}, {
		Kind:      string(haActionUpdatePrimaryRoute),
		DependsOn: string(haActionPromoteStandby),
		RouteTo:   "standby-a",
	}}
	g.Expect(haPlannedActionDependenciesSucceeded(promotionActions, 1)).To(BeFalse())

	promotionActions[0].AdminResult = &antflyv1.HAAdminActionResultStatus{
		FenceGeneration: 2,
		FenceToken:      "old-token",
	}
	g.Expect(haPlannedActionDependenciesSucceeded(promotionActions, 1)).To(BeFalse())

	promotionActions[0].AdminResult = haPromotionAdminResult(3, "ha-fence-token", "standby-a")
	g.Expect(haPlannedActionDependenciesSucceeded(promotionActions, 1)).To(BeTrue())

	rejoinActions := []antflyv1.HAPlannedActionStatus{{
		Kind:            string(haActionRewindFormerPrimary),
		StandbyName:     "primary-a",
		TargetLSN:       12,
		ObservedLSN:     13,
		RetainedFromLSN: 8,
		AdminCommand:    []string{"rejoin", "assess"},
		AdminJobPhase:   haAdminJobPhaseSucceeded,
	}, {
		Kind:      string(haActionUpdatePrimaryRoute),
		DependsOn: string(haActionRewindFormerPrimary),
	}}
	g.Expect(haPlannedActionDependenciesSucceeded(rejoinActions, 1)).To(BeFalse())

	rejoinActions[0].AdminResult = &antflyv1.HAAdminActionResultStatus{
		RejoinAction:     "reseed",
		FormerNodeID:     "primary-a",
		TargetTimelineID: 5,
		TargetEpoch:      7,
		ForkLSN:          12,
		FormerLastLSN:    13,
		RetainedFromLSN:  8,
	}
	g.Expect(haPlannedActionDependenciesSucceeded(rejoinActions, 1)).To(BeFalse())

	rejoinActions[0].AdminResult = &antflyv1.HAAdminActionResultStatus{
		RejoinAction:     "rewind",
		FormerNodeID:     "primary-a",
		TargetTimelineID: 5,
		TargetEpoch:      7,
		ForkLSN:          12,
		FormerLastLSN:    13,
		RetainedFromLSN:  8,
	}
	g.Expect(haPlannedActionDependenciesSucceeded(rejoinActions, 1)).To(BeFalse())

	rejoinActions[0].AdminResult.RewindExecuted = true
	rejoinActions[0].AdminResult.RewindPreviousLastLSN = 13
	rejoinActions[0].AdminResult.RewindCurrentLastLSN = 12
	rejoinActions[0].AdminResult.RewindNextLSN = 13
	rejoinActions[0].AdminResult.RewindDiscardedLSNCount = 1
	rejoinActions[0].AdminResult.ActionID = "rejoin_rewind:primary-a"
	rejoinActions[0].AdminResult.ActionKind = "rejoin_rewind"
	rejoinActions[0].AdminResult.ActionTarget = "primary-a"
	rejoinActions[0].AdminResult.ActionState = "applied"
	rejoinActions[0].AdminResult.ActionNodeID = "primary-a"
	g.Expect(haPlannedActionDependenciesSucceeded(rejoinActions, 1)).To(BeTrue())

	reseedActions := []antflyv1.HAPlannedActionStatus{{
		Kind:            string(haActionReseedFormerPrimary),
		StandbyName:     "primary-a",
		TargetLSN:       12,
		ObservedLSN:     13,
		RetainedFromLSN: 8,
		AdminCommand:    []string{"rejoin", "assess"},
		AdminJobPhase:   haAdminJobPhaseSucceeded,
		AdminResult: &antflyv1.HAAdminActionResultStatus{
			RejoinAction:     "reseed",
			FormerNodeID:     "primary-a",
			TargetTimelineID: 5,
			TargetEpoch:      7,
			ForkLSN:          12,
			FormerLastLSN:    13,
			RetainedFromLSN:  8,
		},
	}, {
		Kind:      string(haActionSeedStandby),
		DependsOn: string(haActionReseedFormerPrimary),
	}}
	g.Expect(haPlannedActionDependenciesSucceeded(reseedActions, 1)).To(BeFalse())
	reseedActions[0].AdminResult.ReseedExecuted = true
	reseedActions[0].AdminResult.ReseedSlotName = "primary-a"
	reseedActions[0].AdminResult.ReseedRequired = true
	reseedActions[0].AdminResult.ReseedBaseBackupRequired = true
	reseedActions[0].AdminResult.ActionID = "rejoin_reseed:primary-a"
	reseedActions[0].AdminResult.ActionKind = "rejoin_reseed"
	reseedActions[0].AdminResult.ActionTarget = "primary-a"
	reseedActions[0].AdminResult.ActionState = "applied"
	reseedActions[0].AdminResult.ActionNodeID = "primary-a"
	g.Expect(haPlannedActionDependenciesSucceeded(reseedActions, 1)).To(BeTrue())
}

func TestHAPrimaryRouteActionRequiresMatchingPromotionEvidence(t *testing.T) {
	g := NewWithT(t)

	action := antflyv1.HAPlannedActionStatus{
		Kind:            string(haActionUpdatePrimaryRoute),
		RouteTo:         "standby-a",
		TargetLSN:       12,
		FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
		FenceGeneration: 7,
	}
	status := &antflyv1.HAStatus{
		LastPromotion: &antflyv1.HAPromotionStatus{
			OldPrimaryID:      "primary-a",
			PromotedStandbyID: "standby-a",
			ClusterID:         100,
			ShardID:           10,
			TableID:           20,
			ParentTimelineID:  4,
			ParentEpoch:       6,
			NewTimelineID:     5,
			NewEpoch:          7,
			SwitchLSN:         12,
			RequiredLSN:       12,
			ObservedLSN:       12,
			FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
			FenceGeneration:   7,
			FenceToken:        "ha-fence-token",
		},
	}
	actions := []antflyv1.HAPlannedActionStatus{action}

	g.Expect(haPrimaryRouteActionHasPromotionEvidence(status, actions, 0)).To(BeTrue())

	status.LastPromotion.PromotedStandbyID = "standby-b"
	g.Expect(haPrimaryRouteActionHasPromotionEvidence(status, actions, 0)).To(BeFalse())

	status.LastPromotion.PromotedStandbyID = "standby-a"
	status.LastPromotion.FenceGeneration = 6
	g.Expect(haPrimaryRouteActionHasPromotionEvidence(status, actions, 0)).To(BeFalse())

	status.LastPromotion.FenceGeneration = 7
	status.LastPromotion.Forced = true
	g.Expect(haPrimaryRouteActionHasPromotionEvidence(status, actions, 0)).To(BeFalse())

	status.LastPromotion.Forced = false
	status.LastPromotion.DataLossPossible = true
	g.Expect(haPrimaryRouteActionHasPromotionEvidence(status, actions, 0)).To(BeFalse())
}

func TestHAPrimaryRouteActionRequiresDirectPromotionResultToMatchRecordedPromotion(t *testing.T) {
	g := NewWithT(t)

	promote := antflyv1.HAPlannedActionStatus{
		Kind:            string(haActionPromoteStandby),
		StandbyName:     "standby-a",
		TargetLSN:       12,
		FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
		FenceGeneration: 7,
		AdminJobName:    haAdminDirectAPIName,
		AdminJobPhase:   haAdminJobPhaseSucceeded,
		AdminNodeID:     "standby-a",
		AdminResult: &antflyv1.HAAdminActionResultStatus{
			SchemaVersion:         1,
			ActionID:              "promotion:standby-a",
			ActionKind:            "promotion",
			ActionTarget:          "standby-a",
			ActionState:           "applied",
			ActionNodeID:          "standby-a",
			FenceGeneration:       7,
			FenceToken:            "ha-fence-token",
			FenceClusterID:        100,
			FenceShardID:          10,
			FenceTableID:          20,
			FenceOldPrimaryID:     "primary-a",
			FencePromotedNodeID:   "standby-a",
			FenceParentTimelineID: 4,
			FenceParentEpoch:      6,
			FenceNewTimelineID:    5,
			FenceNewEpoch:         7,
			FenceRequiredLSN:      12,
			FenceObservedLSN:      13,
			PromotionMode:         "safe",
		},
	}
	route := antflyv1.HAPlannedActionStatus{
		Kind:            string(haActionUpdatePrimaryRoute),
		DependsOn:       string(haActionPromoteStandby),
		RouteTo:         "standby-a",
		TargetLSN:       12,
		FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
		FenceGeneration: 7,
	}
	actions := []antflyv1.HAPlannedActionStatus{promote, route}

	g.Expect(haPrimaryRouteActionHasPromotionEvidence(&antflyv1.HAStatus{}, actions, 1)).To(BeTrue())

	status := &antflyv1.HAStatus{
		LastPromotion: &antflyv1.HAPromotionStatus{
			OldPrimaryID:      "primary-a",
			PromotedStandbyID: "standby-a",
			ClusterID:         100,
			ShardID:           10,
			TableID:           20,
			ParentTimelineID:  4,
			ParentEpoch:       6,
			NewTimelineID:     5,
			NewEpoch:          7,
			RequiredLSN:       12,
			ObservedLSN:       13,
			FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
			FenceGeneration:   7,
			FenceToken:        "ha-fence-token",
		},
	}
	g.Expect(haPrimaryRouteActionHasPromotionEvidence(status, actions, 1)).To(BeTrue())

	status.LastPromotion.NewTimelineID = 6
	g.Expect(haPrimaryRouteActionHasPromotionEvidence(status, actions, 1)).To(BeFalse())

	status.LastPromotion.NewTimelineID = 5
	status.LastPromotion.FenceToken = "different-token"
	g.Expect(haPrimaryRouteActionHasPromotionEvidence(status, actions, 1)).To(BeFalse())

	status.LastPromotion.FenceToken = "ha-fence-token"
	promote.AdminResult.FenceTableID = 21
	actions = []antflyv1.HAPlannedActionStatus{promote, route}
	g.Expect(haPrimaryRouteActionHasPromotionEvidence(status, actions, 1)).To(BeFalse())
}

func TestHACLIActionDependenciesRequireMatchingActionReceipt(t *testing.T) {
	g := NewWithT(t)

	actions := []antflyv1.HAPlannedActionStatus{{
		Kind:          string(haActionCreateSlot),
		SlotName:      "standby-a",
		AdminCommand:  []string{"slot", "create", "--slot", "standby-a"},
		AdminJobPhase: haAdminJobPhaseSucceeded,
		AdminResult: &antflyv1.HAAdminActionResultStatus{
			SlotAction: "create",
			SlotName:   "standby-a",
		},
	}, {
		Kind:      string(haActionSeedStandby),
		DependsOn: string(haActionCreateSlot),
	}}

	g.Expect(haPlannedActionDependenciesSucceeded(actions, 1)).To(BeFalse())

	actions[0].AdminResult.ActionID = "replication_slot_create:standby-b"
	actions[0].AdminResult.ActionKind = "replication_slot_create"
	actions[0].AdminResult.ActionTarget = "standby-b"
	actions[0].AdminResult.ActionState = "applied"
	g.Expect(haPlannedActionDependenciesSucceeded(actions, 1)).To(BeFalse())

	actions[0].AdminResult.ActionID = "replication_slot_create:standby-a"
	actions[0].AdminResult.ActionTarget = "standby-a"
	g.Expect(haPlannedActionDependenciesSucceeded(actions, 1)).To(BeFalse())
	actions[0].AdminResult.ActionNodeID = "primary-a"
	g.Expect(haPlannedActionDependenciesSucceeded(actions, 1)).To(BeTrue())
}

func TestHADirectAdminActionDependenciesRequireVersionedActionReceipt(t *testing.T) {
	g := NewWithT(t)

	actions := []antflyv1.HAPlannedActionStatus{{
		Kind:          string(haActionCreateSlot),
		SlotName:      "standby-a",
		AdminNodeID:   "primary-a",
		AdminJobName:  haAdminDirectAPIName,
		AdminJobPhase: haAdminJobPhaseSucceeded,
		AdminResult: &antflyv1.HAAdminActionResultStatus{
			SchemaVersion: 1,
			SlotAction:    "create",
			SlotName:      "standby-a",
		},
	}, {
		Kind:      string(haActionSeedStandby),
		DependsOn: string(haActionCreateSlot),
	}}

	g.Expect(haPlannedActionDependenciesSucceeded(actions, 1)).To(BeFalse())

	actions[0].AdminResult.ActionID = "replication_slot_create:standby-a"
	actions[0].AdminResult.ActionKind = "replication_slot_create"
	actions[0].AdminResult.ActionTarget = "standby-a"
	actions[0].AdminResult.ActionState = "applied"
	actions[0].AdminResult.ActionNodeID = "primary-a"
	g.Expect(haPlannedActionDependenciesSucceeded(actions, 1)).To(BeTrue())

	actions[0].AdminResult.SchemaVersion = 0
	g.Expect(haPlannedActionDependenciesSucceeded(actions, 1)).To(BeFalse())
}

func TestHAImplicitDependenciesRequireTypedAdminActionWithoutCLIArgv(t *testing.T) {
	g := NewWithT(t)

	actions := []antflyv1.HAPlannedActionStatus{{
		Kind:          string(haActionCreateSlot),
		SlotName:      "standby-a",
		AdminMethod:   "POST",
		AdminPath:     "/admin/v1/ha/replication-slots",
		AdminNodeID:   "primary-a",
		AdminJobName:  haAdminDirectAPIName,
		AdminJobPhase: haAdminJobPhasePending,
	}, {
		Kind:        string(haActionSeedStandby),
		StandbyName: "standby-a",
		AdminMethod: "POST",
		AdminPath:   "/admin/v1/ha/base-backups",
	}}

	g.Expect(haPlannedActionDependenciesSucceeded(actions, 1)).To(BeFalse())

	actions[0].AdminJobPhase = haAdminJobPhaseSucceeded
	g.Expect(haPlannedActionDependenciesSucceeded(actions, 1)).To(BeFalse())

	actions[0].AdminResult = &antflyv1.HAAdminActionResultStatus{
		SchemaVersion: 1,
		ActionID:      "replication_slot_create:standby-a",
		ActionKind:    "replication_slot_create",
		ActionTarget:  "standby-a",
		ActionState:   "applied",
		ActionNodeID:  "primary-a",
		SlotAction:    "create",
		SlotName:      "standby-a",
	}
	g.Expect(haPlannedActionDependenciesSucceeded(actions, 1)).To(BeTrue())
}

func TestHADirectAdminSeedManifestPathRequiresMatchingActionReceipt(t *testing.T) {
	g := NewWithT(t)

	action := antflyv1.HAPlannedActionStatus{
		Kind:             string(haActionFinishStandbySeed),
		SeedManifestPath: "/backup/base-standby-a-5.afha",
		TargetLSN:        5,
		AdminNodeID:      "primary-a",
		AdminJobName:     haAdminDirectAPIName,
		AdminJobPhase:    haAdminJobPhaseSucceeded,
		AdminResult: &antflyv1.HAAdminActionResultStatus{
			SchemaVersion: 1,
			ActionID:      "base_backup_finish:base-standby-b-5",
			ActionKind:    "base_backup_finish",
			ActionTarget:  "base-standby-b-5",
			ActionState:   "applied",
			ActionNodeID:  "primary-a",
			ManifestID:    "base-standby-a-5",
			BackupLSN:     5,
			EndRecordLSN:  5,
		},
	}

	g.Expect(haAdminActionSucceededWithEvidence(action)).To(BeFalse())

	action.AdminResult.ActionID = "base_backup_finish:base-standby-a-5"
	action.AdminResult.ActionTarget = "base-standby-a-5"
	g.Expect(haAdminActionSucceededWithEvidence(action)).To(BeTrue())

	action.AdminResult.BackupLSN = 4
	g.Expect(haAdminActionSucceededWithEvidence(action)).To(BeFalse())
}

func TestHAPromotionAdminResultRequiresFenceScopeEvidence(t *testing.T) {
	g := NewWithT(t)

	action := antflyv1.HAPlannedActionStatus{
		Kind:            string(haActionPromoteStandby),
		StandbyName:     "standby-a",
		TargetLSN:       12,
		FenceGeneration: 3,
		AdminNodeID:     "standby-a",
		AdminJobName:    haAdminDirectAPIName,
		AdminJobPhase:   haAdminJobPhaseSucceeded,
		AdminResult:     haPromotionAdminResult(3, "ha-fence-token", "standby-a"),
	}

	g.Expect(haAdminActionSucceededWithEvidence(action)).To(BeTrue())

	missingCluster := *action.AdminResult
	missingCluster.FenceClusterID = 0
	action.AdminResult = &missingCluster
	g.Expect(haAdminActionSucceededWithEvidence(action)).To(BeFalse())

	missingOldPrimary := *haPromotionAdminResult(3, "ha-fence-token", "standby-a")
	missingOldPrimary.FenceOldPrimaryID = ""
	action.AdminResult = &missingOldPrimary
	g.Expect(haAdminActionSucceededWithEvidence(action)).To(BeFalse())
}

func TestHADirectAdminActionReceiptExpectationsCoverDirectActions(t *testing.T) {
	tests := []struct {
		name       string
		action     antflyv1.HAPlannedActionStatus
		wantKind   string
		wantTarget string
		wantState  string
	}{{
		name: "create slot",
		action: antflyv1.HAPlannedActionStatus{
			Kind:     string(haActionCreateSlot),
			SlotName: "standby-a",
		},
		wantKind:   "replication_slot_create",
		wantTarget: "standby-a",
		wantState:  "applied",
	}, {
		name: "resume slot",
		action: antflyv1.HAPlannedActionStatus{
			Kind:     string(haActionResumeSlot),
			SlotName: "standby-a",
		},
		wantKind:   "replication_slot_resume",
		wantTarget: "standby-a",
		wantState:  "applied",
	}, {
		name: "pause slot",
		action: antflyv1.HAPlannedActionStatus{
			Kind:     string(haActionPauseSlot),
			SlotName: "standby-a",
		},
		wantKind:   "replication_slot_pause",
		wantTarget: "standby-a",
		wantState:  "applied",
	}, {
		name: "drop slot",
		action: antflyv1.HAPlannedActionStatus{
			Kind:     string(haActionDropSlot),
			SlotName: "standby-a",
		},
		wantKind:   "replication_slot_drop",
		wantTarget: "standby-a",
		wantState:  "applied",
	}, {
		name: "seed standby",
		action: antflyv1.HAPlannedActionStatus{
			Kind:      string(haActionSeedStandby),
			SlotName:  "standby-a",
			TargetLSN: 5,
		},
		wantKind:   "base_backup_begin",
		wantTarget: "base-standby-a-5",
		wantState:  "applied",
	}, {
		name: "mark reseed",
		action: antflyv1.HAPlannedActionStatus{
			Kind:      string(haActionMarkReseed),
			SlotName:  "standby-a",
			TargetLSN: 5,
		},
		wantKind:   "base_backup_begin",
		wantTarget: "base-standby-a-5",
		wantState:  "applied",
	}, {
		name: "finish standby seed",
		action: antflyv1.HAPlannedActionStatus{
			Kind:             string(haActionFinishStandbySeed),
			SeedManifestPath: "/backups/base-standby-a-5.afha",
		},
		wantKind:   "base_backup_finish",
		wantTarget: "base-standby-a-5",
		wantState:  "applied",
	}, {
		name: "bootstrap standby seed",
		action: antflyv1.HAPlannedActionStatus{
			Kind:             string(haActionBootstrapStandbySeed),
			SeedManifestPath: "/backups/base-standby-a-5.afha",
		},
		wantKind:   "standby_bootstrap",
		wantTarget: "base-standby-a-5",
		wantState:  "applied",
	}, {
		name: "acquire fence",
		action: antflyv1.HAPlannedActionStatus{
			Kind:        string(haActionAcquireFence),
			StandbyName: "standby-a",
		},
		wantKind:   "fence_acquire",
		wantTarget: "standby-a",
		wantState:  "applied",
	}, {
		name: "promote standby",
		action: antflyv1.HAPlannedActionStatus{
			Kind:        string(haActionPromoteStandby),
			StandbyName: "standby-a",
		},
		wantKind:   "promotion",
		wantTarget: "standby-a",
		wantState:  "applied",
	}, {
		name: "demote former primary",
		action: antflyv1.HAPlannedActionStatus{
			Kind:        string(haActionDemoteFormerPrimary),
			StandbyName: "primary-a",
		},
		wantKind:   "rejoin_assess",
		wantTarget: "primary-a",
		wantState:  "assessed",
	}, {
		name: "rewind former primary",
		action: antflyv1.HAPlannedActionStatus{
			Kind:        string(haActionRewindFormerPrimary),
			StandbyName: "primary-a",
		},
		wantKind:   "rejoin_rewind",
		wantTarget: "primary-a",
		wantState:  "applied",
	}, {
		name: "reseed former primary",
		action: antflyv1.HAPlannedActionStatus{
			Kind:        string(haActionReseedFormerPrimary),
			StandbyName: "primary-a",
		},
		wantKind:   "rejoin_reseed",
		wantTarget: "primary-a",
		wantState:  "applied",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)
			kind := haActionKind(tt.action.Kind)
			g.Expect(haPlannedActionSupportsDirectAdminAPI(kind)).To(BeTrue())
			g.Expect(haActionRequiresAdminResult(kind)).To(BeTrue())

			gotKind, gotTarget, gotState := haDirectAdminActionReceiptExpectation(tt.action)
			g.Expect(gotKind).To(Equal(tt.wantKind))
			g.Expect(gotTarget).To(Equal(tt.wantTarget))
			g.Expect(gotState).To(Equal(tt.wantState))

			tt.action.AdminURL = "http://planned-admin.default.svc:8081"
			tt.action.AdminResult = &antflyv1.HAAdminActionResultStatus{
				SchemaVersion: 1,
				ActionID:      tt.wantKind + ":" + tt.wantTarget,
				ActionKind:    tt.wantKind,
				ActionTarget:  tt.wantTarget,
				ActionState:   tt.wantState,
				ActionNodeID:  "node-a",
			}
			g.Expect(haDirectAdminActionReceiptMatches(tt.action)).To(BeFalse())

			tt.action.AdminNodeID = "node-a"
			g.Expect(haDirectAdminActionReceiptMatches(tt.action)).To(BeTrue())

			tt.action.AdminResult.ActionState = "already_applied"
			if tt.wantState == "applied" {
				g.Expect(haDirectAdminActionReceiptMatches(tt.action)).To(BeTrue())
			} else {
				g.Expect(haDirectAdminActionReceiptMatches(tt.action)).To(BeFalse())
			}
			tt.action.AdminResult.ActionState = tt.wantState

			tt.action.AdminResult.ActionNodeID = "node-b"
			g.Expect(haDirectAdminActionReceiptMatches(tt.action)).To(BeFalse())
			tt.action.AdminResult.ActionNodeID = "node-a"

			tt.action.AdminResult.ActionTarget = tt.wantTarget + "-other"
			tt.action.AdminResult.ActionID = tt.wantKind + ":" + tt.action.AdminResult.ActionTarget
			g.Expect(haDirectAdminActionReceiptMatches(tt.action)).To(BeFalse())
		})
	}
}

func TestReconcileHAAdminJobsHonorsExplicitDependencyAfterUnrelatedFailure(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	admin := &antflyv1.HAAdminSpec{
		PrimaryURL:            "http://primary-ha.default.svc:8081",
		ExecutePlannedActions: true,
	}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image:           "antfly:test",
			ImagePullPolicy: "IfNotPresent",
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode:  antflyv1.HAModeHotStandby,
				Admin: admin,
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:          string(haActionPauseSlot),
					AdminCommand:  []string{"slot", "pause", "--slot", "old-standby"},
					AdminURL:      "http://primary-ha.default.svc:8081",
					AdminJobPhase: haAdminJobPhaseFailed,
				}, {
					Kind:          string(haActionCreateSlot),
					SlotName:      "standby-a",
					AdminCommand:  []string{"slot", "create", "--slot", "standby-a"},
					AdminURL:      "http://primary-ha.default.svc:8081",
					AdminNodeID:   "primary-a",
					AdminJobPhase: haAdminJobPhaseSucceeded,
					AdminResult: &antflyv1.HAAdminActionResultStatus{
						ActionID:     "replication_slot_create:standby-a",
						ActionKind:   "replication_slot_create",
						ActionTarget: "standby-a",
						ActionState:  "applied",
						ActionNodeID: "primary-a",
						SlotAction:   "create",
						SlotName:     "standby-a",
					},
				}, {
					Kind:         string(haActionSeedStandby),
					DependsOn:    string(haActionCreateSlot),
					StandbyName:  "standby-a",
					AdminCommand: []string{"seed", "begin", "--slot", "standby-a", "--manifest-id", "operator-base-standby-a-7"},
					AdminURL:     "http://primary-ha.default.svc:8081",
					AdminNodeID:  "primary-a",
				}},
			},
		},
	}
	failedPauseJob := buildHAAdminJob(cluster, admin, cluster.Status.HAStatus.PlannedActions[0])
	failedPauseHash := haAdminActionHash(cluster.Status.HAStatus.PlannedActions[0])[:10]
	g.Expect(failedPauseJob.Annotations).To(HaveKeyWithValue("antfly.io/ha-command-hash", haAdminActionHash(cluster.Status.HAStatus.PlannedActions[0])))
	g.Expect(failedPauseJob.Labels).To(HaveKeyWithValue("antfly.io/ha-command-hash", failedPauseHash))
	g.Expect(failedPauseJob.Spec.Template.Labels).To(HaveKeyWithValue("antfly.io/ha-command-hash", failedPauseHash))
	failedPauseJob.Status.Conditions = []batchv1.JobCondition{{
		Type:   batchv1.JobFailed,
		Status: corev1.ConditionTrue,
	}}

	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster, failedPauseJob).Build(),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/base-backups"))
			var payload map[string]any
			g.Expect(json.NewDecoder(req.Body).Decode(&payload)).To(Succeed())
			g.Expect(payload["manifest_id"]).To(Equal("operator-base-standby-a-7"))
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"schema_version":1,"action":{"action_id":"base_backup_begin:operator-base-standby-a-7","action_kind":"base_backup_begin","target":"operator-base-standby-a-7","state":"applied","node_id":"primary-a"},"slot_name":"standby-a","manifest_id":"operator-base-standby-a-7","backup_lsn":7,"start_record_lsn":7}`)),
			}, nil
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(HaveLen(1))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseFailed))
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminJobName).To(BeEmpty())
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminResult).NotTo(BeNil())
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminResult.ManifestID).To(Equal("operator-base-standby-a-7"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminResult.BackupLSN).To(Equal(uint64(7)))
}

func TestReconcileHAPrimaryRouteWaitsForAdminPrerequisites(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Standbys: []antflyv1.HAStandbySpec{{
					Name: "standby-a",
					RouteSelector: map[string]string{
						"app.kubernetes.io/name":      "antfly-database",
						"app.kubernetes.io/component": "standby-a",
						"app.kubernetes.io/instance":  "test-cluster",
					},
				}},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				Mode: antflyv1.HAModeHotStandby,
				PrimaryRoute: antflyv1.HAPrimaryRouteStatus{
					ServiceName:   "test-cluster-public-api",
					CurrentTarget: "primary",
					DesiredTarget: "standby-a",
					Stale:         true,
					Action:        string(haActionUpdatePrimaryRoute),
				},
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:          string(haActionPromoteStandby),
					StandbyName:   "standby-a",
					AdminCommand:  []string{"promote", "--current-fence"},
					AdminURL:      "http://standby-a-ha.default.svc:8081",
					AdminNodeID:   "standby-a",
					AdminJobName:  "promote-job",
					AdminJobPhase: haAdminJobPhasePending,
				}, {
					Kind:            string(haActionUpdatePrimaryRoute),
					StandbyName:     "standby-a",
					RouteFrom:       "primary",
					RouteTo:         "standby-a",
					FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
					FenceGeneration: 7,
				}},
			},
		},
	}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-public-api",
			Namespace: "default",
		},
	}
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster, service).Build()
	reconciler := &AntflyClusterReconciler{Client: client, Scheme: s}

	g.Expect(reconciler.reconcileHAPrimaryRoute(context.Background(), cluster)).To(Succeed())
	observed := &corev1.Service{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: service.Name, Namespace: service.Namespace}, observed)).To(Succeed())
	g.Expect(observed.Annotations).NotTo(HaveKey(haPrimaryRouteTargetAnnotation))
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.CurrentTarget).To(Equal("primary"))

	cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase = haAdminJobPhaseSucceeded
	g.Expect(reconciler.reconcileHAPrimaryRoute(context.Background(), cluster)).To(Succeed())
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: service.Name, Namespace: service.Namespace}, observed)).To(Succeed())
	g.Expect(observed.Annotations).NotTo(HaveKey(haPrimaryRouteTargetAnnotation))
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.CurrentTarget).To(Equal("primary"))

	cluster.Status.HAStatus.PlannedActions[0].AdminResult = haPromotionAdminResult(7, "ha-fence-token", "standby-b")
	g.Expect(reconciler.reconcileHAPrimaryRoute(context.Background(), cluster)).To(Succeed())
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: service.Name, Namespace: service.Namespace}, observed)).To(Succeed())
	g.Expect(observed.Annotations).NotTo(HaveKey(haPrimaryRouteTargetAnnotation))
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.CurrentTarget).To(Equal("primary"))

	cluster.Status.HAStatus.PlannedActions[0].AdminResult = haPromotionAdminResult(6, "ha-fence-token", "standby-a")
	g.Expect(reconciler.reconcileHAPrimaryRoute(context.Background(), cluster)).To(Succeed())
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: service.Name, Namespace: service.Namespace}, observed)).To(Succeed())
	g.Expect(observed.Annotations).NotTo(HaveKey(haPrimaryRouteTargetAnnotation))
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.CurrentTarget).To(Equal("primary"))

	cluster.Status.HAStatus.PlannedActions[0].AdminResult = haPromotionAdminResult(7, "ha-fence-token", "standby-a")
	g.Expect(reconciler.reconcileHAPrimaryRoute(context.Background(), cluster)).To(Succeed())
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: service.Name, Namespace: service.Namespace}, observed)).To(Succeed())
	g.Expect(observed.Annotations).To(HaveKeyWithValue(haPrimaryRouteTargetAnnotation, "standby-a"))
	g.Expect(observed.Annotations).To(HaveKeyWithValue(haPrimaryRouteFenceAuthorityAnnotation, string(antflyv1.HAFencingAuthorityKubernetesLease)))
	g.Expect(observed.Annotations).To(HaveKeyWithValue(haPrimaryRouteFenceGenerationAnnotation, "7"))
	g.Expect(observed.Annotations).To(HaveKeyWithValue(haPrimaryRouteSelectorAnnotation, "true"))
	g.Expect(observed.Spec.Selector).To(HaveKeyWithValue("app.kubernetes.io/component", "standby-a"))
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.CurrentTarget).To(Equal("standby-a"))
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.Stale).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.Action).To(Equal("None"))

	g.Expect(reconciler.observeHAPrimaryRouteStatus(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.CurrentTarget).To(Equal("standby-a"))
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.FenceAuthority).To(Equal(antflyv1.HAFencingAuthorityKubernetesLease))
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.FenceGeneration).To(Equal(uint64(7)))

	observed.Annotations[haPrimaryRouteFenceGenerationAnnotation] = "not-a-generation"
	g.Expect(client.Update(context.Background(), observed)).To(Succeed())
	g.Expect(reconciler.observeHAPrimaryRouteStatus(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.FenceGeneration).To(Equal(uint64(0)))
}

func TestReconcileHAPrimaryRouteRequiresStandbyRouteSelector(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Standbys: []antflyv1.HAStandbySpec{{
					Name: "standby-a",
				}},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				Mode: antflyv1.HAModeHotStandby,
				PrimaryRoute: antflyv1.HAPrimaryRouteStatus{
					CurrentTarget: "primary",
					DesiredTarget: "standby-a",
					Stale:         true,
					Action:        string(haActionUpdatePrimaryRoute),
				},
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:          string(haActionPromoteStandby),
					StandbyName:   "standby-a",
					AdminCommand:  []string{"promote", "--current-fence"},
					AdminURL:      "http://standby-a-ha.default.svc:8081",
					AdminNodeID:   "standby-a",
					AdminJobName:  "promote-job",
					AdminJobPhase: haAdminJobPhaseSucceeded,
					AdminResult:   haPromotionAdminResult(7, "ha-fence-token", "standby-a"),
				}, {
					Kind:            string(haActionUpdatePrimaryRoute),
					StandbyName:     "standby-a",
					RouteFrom:       "primary",
					RouteTo:         "standby-a",
					FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
					FenceGeneration: 7,
				}},
			},
		},
	}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-public-api",
			Namespace: "default",
		},
		Spec: corev1.ServiceSpec{
			Selector: serviceSelectorLabels("test-cluster", "metadata"),
		},
	}
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster, service).Build()
	reconciler := &AntflyClusterReconciler{Client: client, Scheme: s}

	g.Expect(reconciler.reconcileHAPrimaryRoute(context.Background(), cluster)).To(Succeed())

	observed := &corev1.Service{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: service.Name, Namespace: service.Namespace}, observed)).To(Succeed())
	g.Expect(observed.Annotations).NotTo(HaveKey(haPrimaryRouteTargetAnnotation))
	g.Expect(observed.Annotations).NotTo(HaveKey(haPrimaryRouteSelectorAnnotation))
	g.Expect(observed.Spec.Selector).To(Equal(serviceSelectorLabels("test-cluster", "metadata")))
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.CurrentTarget).To(Equal("primary"))
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.DesiredTarget).To(Equal("standby-a"))
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.Stale).To(BeTrue())
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.Action).To(Equal(string(haActionUpdatePrimaryRoute)))
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.Reason).To(Equal(antflyv1.ReasonHAPrimaryRouteSelectorMissing))
	degraded := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	g.Expect(degraded).NotTo(BeNil())
	g.Expect(degraded.Status).To(Equal(metav1.ConditionTrue))
	g.Expect(degraded.Reason).To(Equal(antflyv1.ReasonHAPrimaryRouteSelectorMissing))
}

func TestUpdateHAPrimaryRouteServiceClearsFenceAnnotationsWithoutFence(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{Mode: antflyv1.HAModeHotStandby},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{Mode: antflyv1.HAModeHotStandby},
		},
	}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-public-api",
			Namespace: "default",
			Annotations: map[string]string{
				haPrimaryRouteTargetAnnotation:          "standby-a",
				haPrimaryRouteFenceAuthorityAnnotation:  string(antflyv1.HAFencingAuthorityKubernetesLease),
				haPrimaryRouteFenceGenerationAnnotation: "7",
			},
		},
	}
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster, service).Build()
	reconciler := &AntflyClusterReconciler{Client: client, Scheme: s}

	g.Expect(reconciler.updateHAPrimaryRouteService(context.Background(), cluster, antflyv1.HAPlannedActionStatus{
		Kind:    string(haActionUpdatePrimaryRoute),
		RouteTo: "primary",
	})).To(Succeed())

	observed := &corev1.Service{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: service.Name, Namespace: service.Namespace}, observed)).To(Succeed())
	g.Expect(observed.Annotations).To(HaveKeyWithValue(haPrimaryRouteTargetAnnotation, "primary"))
	g.Expect(observed.Annotations).NotTo(HaveKey(haPrimaryRouteFenceAuthorityAnnotation))
	g.Expect(observed.Annotations).NotTo(HaveKey(haPrimaryRouteFenceGenerationAnnotation))
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.CurrentTarget).To(Equal("primary"))
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.FenceAuthority).To(BeEmpty())
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.FenceGeneration).To(Equal(uint64(0)))
}

func TestReconcileHAPrimaryRouteHonorsExplicitDependencyAfterUnrelatedFailure(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Standbys: []antflyv1.HAStandbySpec{{
					Name: "standby-a",
					RouteSelector: map[string]string{
						"app.kubernetes.io/name":      "antfly-database",
						"app.kubernetes.io/component": "standby-a",
						"app.kubernetes.io/instance":  "test-cluster",
					},
				}},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				Mode: antflyv1.HAModeHotStandby,
				PrimaryRoute: antflyv1.HAPrimaryRouteStatus{
					CurrentTarget: "primary",
					DesiredTarget: "standby-a",
					Stale:         true,
					Action:        string(haActionUpdatePrimaryRoute),
				},
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:          string(haActionPauseSlot),
					AdminCommand:  []string{"slot", "pause"},
					AdminJobPhase: haAdminJobPhaseFailed,
				}, {
					Kind:          string(haActionPromoteStandby),
					StandbyName:   "standby-a",
					AdminCommand:  []string{"promote", "--current-fence"},
					AdminJobPhase: haAdminJobPhaseSucceeded,
					AdminResult:   haPromotionAdminResult(7, "ha-fence-token", "standby-a"),
				}, {
					Kind:            string(haActionUpdatePrimaryRoute),
					DependsOn:       string(haActionPromoteStandby),
					StandbyName:     "standby-a",
					RouteFrom:       "primary",
					RouteTo:         "standby-a",
					FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
					FenceGeneration: 7,
				}},
			},
		},
	}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-public-api",
			Namespace: "default",
		},
	}
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster, service).Build()
	reconciler := &AntflyClusterReconciler{Client: client, Scheme: s}

	g.Expect(reconciler.reconcileHAPrimaryRoute(context.Background(), cluster)).To(Succeed())

	observed := &corev1.Service{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: service.Name, Namespace: service.Namespace}, observed)).To(Succeed())
	g.Expect(observed.Annotations).To(HaveKeyWithValue(haPrimaryRouteTargetAnnotation, "standby-a"))
	g.Expect(observed.Annotations).To(HaveKeyWithValue(haPrimaryRouteFenceAuthorityAnnotation, string(antflyv1.HAFencingAuthorityKubernetesLease)))
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.CurrentTarget).To(Equal("standby-a"))
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.Stale).To(BeFalse())
}

func TestUpdateHALastPromotionFromSucceededPromoteJob(t *testing.T) {
	g := NewWithT(t)

	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID:        100,
					ShardID:          10,
					TableID:          20,
					TimelineID:       4,
					Epoch:            6,
					CurrentPrimaryID: "primary-a",
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:            string(haActionPromoteStandby),
					StandbyName:     "standby-a",
					TargetLSN:       12,
					FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
					FenceGeneration: 3,
					FenceReason:     "LeaseAcquired",
					AdminCommand:    []string{"promote", "--current-fence"},
					AdminJobName:    "promote-job",
					AdminJobPhase:   haAdminJobPhaseSucceeded,
					AdminResult:     haPromotionAdminResult(3, "ha-fence-token", "standby-a"),
				}},
			},
		},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHALastPromotionFromAdminJobs(context.Background(), cluster)

	promotion := cluster.Status.HAStatus.LastPromotion
	g.Expect(promotion).NotTo(BeNil())
	g.Expect(promotion.OldPrimaryID).To(Equal("primary-a"))
	g.Expect(promotion.PromotedStandbyID).To(Equal("standby-a"))
	g.Expect(promotion.ClusterID).To(Equal(uint64(100)))
	g.Expect(promotion.ShardID).To(Equal(uint64(10)))
	g.Expect(promotion.TableID).To(Equal(uint64(20)))
	g.Expect(promotion.ParentTimelineID).To(Equal(uint64(4)))
	g.Expect(promotion.NewTimelineID).To(Equal(uint64(5)))
	g.Expect(promotion.SwitchLSN).To(Equal(uint64(13)))
	g.Expect(promotion.FenceAuthority).To(Equal(antflyv1.HAFencingAuthorityKubernetesLease))
	g.Expect(promotion.FenceGeneration).To(Equal(uint64(3)))
	g.Expect(promotion.FenceReason).To(Equal("LeaseAcquired"))
	g.Expect(promotion.CompletionTime).NotTo(BeNil())
	firstCompletion := promotion.CompletionTime

	reconciler.updateHALastPromotionFromAdminJobs(context.Background(), cluster)
	g.Expect(cluster.Status.HAStatus.LastPromotion.CompletionTime).To(Equal(firstCompletion))

	cluster.Status.HAStatus.LastPromotion.FenceAuthority = ""
	reconciler.updateHALastPromotionFromAdminJobs(context.Background(), cluster)
	g.Expect(cluster.Status.HAStatus.LastPromotion.FenceAuthority).To(Equal(antflyv1.HAFencingAuthorityKubernetesLease))
	g.Expect(cluster.Status.HAStatus.LastPromotion.CompletionTime).To(Equal(firstCompletion))

	cluster.Status.HAStatus.LastPromotion.FenceToken = "token"
	cluster.Status.HAStatus.LastPromotion.ObservedLSN = 13
	reconciler.updateHALastPromotionFromAdminJobs(context.Background(), cluster)
	g.Expect(cluster.Status.HAStatus.LastPromotion.FenceToken).To(Equal("token"))
	g.Expect(cluster.Status.HAStatus.LastPromotion.ObservedLSN).To(Equal(uint64(13)))
	g.Expect(cluster.Status.HAStatus.LastPromotion.CompletionTime).To(Equal(firstCompletion))
}

func TestUpdateHAAdminJobExecutionConditionReportsMissingPromotionReceipt(t *testing.T) {
	g := NewWithT(t)

	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID:        100,
					ShardID:          10,
					TableID:          20,
					TimelineID:       4,
					Epoch:            6,
					CurrentPrimaryID: "primary-a",
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:            string(haActionPromoteStandby),
					StandbyName:     "standby-a",
					TargetLSN:       12,
					FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
					FenceGeneration: 3,
					FenceReason:     "LeaseAcquired",
					AdminCommand:    []string{"promote", "--current-fence"},
					AdminJobName:    "promote-job",
					AdminJobPhase:   haAdminJobPhaseSucceeded,
					AdminResult: &antflyv1.HAAdminActionResultStatus{
						FenceGeneration: 3,
						FenceToken:      "ha-fence-token",
					},
				}},
			},
		},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHALastPromotionFromAdminJobs(context.Background(), cluster)
	reconciler.updateHAAdminJobExecutionCondition(cluster)

	degraded := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	g.Expect(degraded).NotTo(BeNil())
	g.Expect(degraded.Status).To(Equal(metav1.ConditionTrue))
	g.Expect(degraded.Reason).To(Equal(antflyv1.ReasonHAAdminResultMissing))
	g.Expect(degraded.Message).To(ContainSubstring("promote-job"))
	g.Expect(degraded.Message).To(ContainSubstring("dependent HA actions remain blocked"))
}

func TestUpdateHALastPromotionRequiresPriorHAAdminActions(t *testing.T) {
	g := NewWithT(t)

	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID:        100,
					ShardID:          10,
					TableID:          20,
					TimelineID:       4,
					Epoch:            6,
					CurrentPrimaryID: "primary-a",
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:          string(haActionAcquireFence),
					StandbyName:   "standby-a",
					TargetLSN:     12,
					AdminCommand:  []string{"fence", "acquire"},
					AdminJobName:  "fence-job",
					AdminJobPhase: haAdminJobPhasePending,
				}, {
					Kind:            string(haActionPromoteStandby),
					StandbyName:     "standby-a",
					TargetLSN:       12,
					FenceGeneration: 3,
					AdminCommand:    []string{"promote", "--current-fence"},
					AdminJobName:    "promote-job",
					AdminJobPhase:   haAdminJobPhaseSucceeded,
				}},
			},
		},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHALastPromotionFromAdminJobs(context.Background(), cluster)
	g.Expect(cluster.Status.HAStatus.LastPromotion).To(BeNil())

	cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase = haAdminJobPhaseSucceeded
	reconciler.updateHALastPromotionFromAdminJobs(context.Background(), cluster)
	g.Expect(cluster.Status.HAStatus.LastPromotion).To(BeNil())

	cluster.Status.HAStatus.PlannedActions[0].AdminResult = &antflyv1.HAAdminActionResultStatus{
		ActionID:            "fence_acquire:standby-a",
		ActionKind:          "fence_acquire",
		ActionTarget:        "standby-a",
		ActionState:         "applied",
		ActionNodeID:        "standby-a",
		FenceGeneration:     3,
		FenceToken:          "lease-token-3",
		FencePromotedNodeID: "standby-a",
	}
	reconciler.updateHALastPromotionFromAdminJobs(context.Background(), cluster)
	g.Expect(cluster.Status.HAStatus.LastPromotion).To(BeNil())

	cluster.Status.HAStatus.PlannedActions[1].AdminResult = haPromotionAdminResult(3, "lease-token-3", "standby-a")
	reconciler.updateHALastPromotionFromAdminJobs(context.Background(), cluster)
	g.Expect(cluster.Status.HAStatus.LastPromotion).NotTo(BeNil())
	g.Expect(cluster.Status.HAStatus.LastPromotion.PromotedStandbyID).To(Equal("standby-a"))
}

func TestUpdateHALastPromotionHonorsExplicitDependencyAfterUnrelatedFailure(t *testing.T) {
	g := NewWithT(t)

	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID:        100,
					ShardID:          10,
					TableID:          20,
					TimelineID:       4,
					Epoch:            6,
					CurrentPrimaryID: "primary-a",
				},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:          string(haActionPauseSlot),
					AdminCommand:  []string{"slot", "pause"},
					AdminJobPhase: haAdminJobPhaseFailed,
				}, {
					Kind:          string(haActionAcquireFence),
					StandbyName:   "standby-a",
					AdminCommand:  []string{"fence", "acquire"},
					AdminJobPhase: haAdminJobPhaseSucceeded,
					AdminResult: &antflyv1.HAAdminActionResultStatus{
						ActionID:            "fence_acquire:standby-a",
						ActionKind:          "fence_acquire",
						ActionTarget:        "standby-a",
						ActionState:         "applied",
						ActionNodeID:        "standby-a",
						FenceGeneration:     3,
						FenceToken:          "lease-token-3",
						FencePromotedNodeID: "standby-a",
					},
				}, {
					Kind:            string(haActionPromoteStandby),
					DependsOn:       string(haActionAcquireFence),
					StandbyName:     "standby-a",
					TargetLSN:       12,
					FenceGeneration: 3,
					AdminCommand:    []string{"promote", "--current-fence"},
					AdminJobName:    "promote-job",
					AdminJobPhase:   haAdminJobPhaseSucceeded,
					AdminResult:     haPromotionAdminResult(3, "lease-token-3", "standby-a"),
				}},
			},
		},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHALastPromotionFromAdminJobs(context.Background(), cluster)
	g.Expect(cluster.Status.HAStatus.LastPromotion).NotTo(BeNil())
	g.Expect(cluster.Status.HAStatus.LastPromotion.PromotedStandbyID).To(Equal("standby-a"))
}

func TestParseHAPromotionJobResult(t *testing.T) {
	g := NewWithT(t)

	result, ok := parseHAPromotionJobResult(strings.Join([]string{
		"result=promote_current_fence",
		"assessment.required_lsn=12",
		"assessment.received_lsn=12",
		"assessment.applied_lsn=11",
		"assessment.mode=lossy",
		"promotion.node_id=standby-a",
		"promotion.switch_lsn=13",
		"promotion.old_identity.cluster_id=100",
		"promotion.old_identity.shard_id=10",
		"promotion.old_identity.table_id=20",
		"promotion.old_identity.timeline_id=4",
		"promotion.old_identity.epoch=6",
		"promotion.new_identity.cluster_id=100",
		"promotion.new_identity.shard_id=10",
		"promotion.new_identity.table_id=20",
		"promotion.new_identity.timeline_id=5",
		"promotion.new_identity.epoch=7",
		"promotion.forced=true",
		"promotion.data_loss_possible=true",
		"fence_generation=3",
		"fence_token=ha-fence-token",
		"",
	}, "\n"))

	g.Expect(ok).To(BeTrue())
	g.Expect(result.PromotedNodeID).To(Equal("standby-a"))
	g.Expect(result.SwitchLSN).To(Equal(uint64(13)))
	g.Expect(result.ParentTimelineID).To(Equal(uint64(4)))
	g.Expect(result.ParentEpoch).To(Equal(uint64(6)))
	g.Expect(result.NewTimelineID).To(Equal(uint64(5)))
	g.Expect(result.NewEpoch).To(Equal(uint64(7)))
	g.Expect(result.RequiredLSN).To(Equal(uint64(12)))
	g.Expect(result.ObservedLSN).To(Equal(uint64(12)))
	g.Expect(result.FenceGeneration).To(Equal(uint64(3)))
	g.Expect(result.FenceToken).To(Equal("ha-fence-token"))
	g.Expect(result.Forced).To(BeTrue())
	g.Expect(result.PromotionMode).To(Equal("lossy"))
	g.Expect(result.DataLossPossible).To(BeTrue())

	adminResult := haPromotionAdminActionResult(result)
	g.Expect(adminResult.FenceClusterID).To(Equal(uint64(100)))
	g.Expect(adminResult.FenceShardID).To(Equal(uint64(10)))
	g.Expect(adminResult.FenceTableID).To(Equal(uint64(20)))
	g.Expect(adminResult.FencePromotedNodeID).To(Equal("standby-a"))
	g.Expect(adminResult.FenceRequiredLSN).To(Equal(uint64(12)))
	g.Expect(adminResult.FenceObservedLSN).To(Equal(uint64(12)))
	g.Expect(adminResult.FenceForced).To(BeTrue())
	g.Expect(adminResult.PromotionForce).To(BeTrue())
	g.Expect(adminResult.PromotionDataLossPossible).To(BeTrue())

	identity := &antflyv1.HAReplicationIdentitySpec{
		ClusterID:        100,
		ShardID:          10,
		TableID:          20,
		TimelineID:       4,
		Epoch:            6,
		CurrentPrimaryID: "primary-a",
	}
	action := antflyv1.HAPlannedActionStatus{
		StandbyName:     "standby-a",
		TargetLSN:       12,
		FenceGeneration: 3,
		FenceReason:     "LeaseAcquired",
	}
	enrichHAPromotionAdminActionResult(adminResult, identity, action)
	g.Expect(adminResult.FenceOldPrimaryID).To(Equal("primary-a"))
	g.Expect(adminResult.FencePromotedNodeID).To(Equal("standby-a"))
	g.Expect(adminResult.FenceReason).To(Equal("LeaseAcquired"))
	g.Expect(haActionHasRequiredAdminResult(antflyv1.HAPlannedActionStatus{
		Kind:        string(haActionPromoteStandby),
		StandbyName: "standby-a",
		TargetLSN:   12,
		AdminResult: adminResult,
	})).To(BeFalse())

	promotion := &antflyv1.HAPromotionStatus{
		OldPrimaryID:      "primary-a",
		PromotedStandbyID: "standby-a",
	}
	g.Expect(applyHAPromotionJobResultIfMatches(promotion, result, identity, action)).To(BeTrue())
	g.Expect(promotion.ParentTimelineID).To(Equal(uint64(4)))
	g.Expect(promotion.NewEpoch).To(Equal(uint64(7)))
	g.Expect(promotion.FenceToken).To(Equal("ha-fence-token"))
	g.Expect(promotion.DataLossPossible).To(BeTrue())

	wrongNode := result
	wrongNode.PromotedNodeID = "standby-b"
	promotion.FenceToken = "unchanged-token"
	g.Expect(applyHAPromotionJobResultIfMatches(promotion, wrongNode, identity, action)).To(BeFalse())
	g.Expect(promotion.FenceToken).To(Equal("unchanged-token"))
}

func TestParseHAPromotionAPIResultAcceptsOpenAPIAndLegacyShapes(t *testing.T) {
	g := NewWithT(t)

	openAPIResult, ok := parseHAPromotionAPIResult([]byte(haPromotionResponseJSON()))
	g.Expect(ok).To(BeTrue())
	g.Expect(openAPIResult.SchemaVersion).To(Equal(uint32(1)))
	g.Expect(openAPIResult.ActionID).To(Equal("promotion:standby-a"))
	g.Expect(openAPIResult.ActionKind).To(Equal("promotion"))
	g.Expect(openAPIResult.ActionTarget).To(Equal("standby-a"))
	g.Expect(openAPIResult.ActionState).To(Equal("applied"))
	g.Expect(openAPIResult.PromotedNodeID).To(Equal("standby-a"))
	g.Expect(openAPIResult.SwitchLSN).To(Equal(uint64(13)))
	g.Expect(openAPIResult.ParentClusterID).To(Equal(uint64(100)))
	g.Expect(openAPIResult.ParentShardID).To(Equal(uint64(10)))
	g.Expect(openAPIResult.ParentTableID).To(Equal(uint64(20)))
	g.Expect(openAPIResult.ParentTimelineID).To(Equal(uint64(4)))
	g.Expect(openAPIResult.NewClusterID).To(Equal(uint64(100)))
	g.Expect(openAPIResult.NewEpoch).To(Equal(uint64(7)))
	g.Expect(openAPIResult.RequiredLSN).To(Equal(uint64(12)))
	g.Expect(openAPIResult.ObservedLSN).To(Equal(uint64(12)))
	g.Expect(openAPIResult.FenceGeneration).To(Equal(uint64(3)))
	g.Expect(openAPIResult.FenceToken).To(Equal("ha-fence-token"))
	openAPIStatus := haPromotionAdminActionResult(openAPIResult)
	g.Expect(openAPIStatus.SchemaVersion).To(Equal(uint32(1)))
	g.Expect(openAPIStatus.ActionID).To(Equal("promotion:standby-a"))
	g.Expect(openAPIStatus.ActionKind).To(Equal("promotion"))
	g.Expect(openAPIStatus.ActionTarget).To(Equal("standby-a"))
	g.Expect(openAPIStatus.ActionState).To(Equal("applied"))

	_, ok = parseHAPromotionAPIResult([]byte(haPromotionResponseJSONWithoutPath("forced")))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHAPromotionAPIResult([]byte(haPromotionResponseJSONWithoutPath("promotion", "forced")))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHAPromotionAPIResult([]byte(haPromotionResponseJSONWithoutPath("promotion", "data_loss_possible")))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHAPromotionAPIResult([]byte(haPromotionResponseJSONWithoutPath("promotion", "old_identity", "shard_id")))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHAPromotionAPIResult([]byte(haPromotionResponseJSONWithoutPath("promotion", "new_identity", "table_id")))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHAPromotionAPIResult([]byte(`{"schema_version":1,"action":{"action_id":"promotion:standby-a","action_kind":"promotion","target":"standby-a","state":"applied","node_id":"standby-a"},"assessment":{"required_lsn":12,"received_lsn":12,"applied_lsn":12,"has_required_lsn":true,"caught_up_to_received":true,"fencing_confirmed":true,"data_loss_possible":false,"safe":true,"requires_fencing":false,"requires_force":false,"can_promote":true},"promotion":{"node_id":"standby-a","switch_lsn":13,"old_identity":{"cluster_id":100,"shard_id":10,"table_id":20,"timeline_id":4,"epoch":6},"new_identity":{"cluster_id":100,"shard_id":10,"table_id":20,"timeline_id":5,"epoch":7},"forced":false,"data_loss_possible":false},"fence_generation":3,"fence_token":"ha-fence-token","forced":false}`))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHAPromotionAPIResult([]byte(`{"schema_version":1,"assessment":{"required_lsn":12,"received_lsn":12,"applied_lsn":12,"has_required_lsn":true,"caught_up_to_received":true,"fencing_confirmed":true,"force":false,"mode":"safe","data_loss_possible":false,"safe":true,"requires_fencing":false,"requires_force":false,"can_promote":true},"promotion":{"node_id":"standby-a","switch_lsn":13,"old_identity":{"cluster_id":100,"timeline_id":4,"epoch":6},"new_identity":{"cluster_id":100,"timeline_id":5,"epoch":7},"forced":false,"data_loss_possible":false},"fence_generation":3,"fence_token":"ha-fence-token","forced":false}`))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHAPromotionAPIResult([]byte(`{"schema_version":1,"action":{"action_id":"promotion:standby-a","action_kind":"promotion","target":"standby-a"},"assessment":{"required_lsn":12,"received_lsn":12,"applied_lsn":12,"has_required_lsn":true,"caught_up_to_received":true,"fencing_confirmed":true,"force":false,"mode":"safe","data_loss_possible":false,"safe":true,"requires_fencing":false,"requires_force":false,"can_promote":true},"promotion":{"node_id":"standby-a","switch_lsn":13,"old_identity":{"cluster_id":100,"timeline_id":4,"epoch":6},"new_identity":{"cluster_id":100,"timeline_id":5,"epoch":7},"forced":false,"data_loss_possible":false},"fence_generation":3,"fence_token":"ha-fence-token","forced":false}`))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHAPromotionAPIResult([]byte(`{"schema_version":1,"action":{"action_id":"promotion:standby-a","action_kind":"promotion","target":"standby-a","state":"applied","node_id":"standby-a"},"assessment":{"required_lsn":12,"received_lsn":12,"applied_lsn":11,"has_required_lsn":true,"caught_up_to_received":true,"fencing_confirmed":true,"force":false,"mode":"safe","data_loss_possible":false,"safe":true,"requires_fencing":false,"requires_force":false,"can_promote":true},"promotion":{"node_id":"standby-a","switch_lsn":13,"old_identity":{"cluster_id":100,"shard_id":10,"table_id":20,"timeline_id":4,"epoch":6},"new_identity":{"cluster_id":100,"shard_id":10,"table_id":20,"timeline_id":5,"epoch":7},"forced":false,"data_loss_possible":false},"fence_generation":3,"fence_token":"ha-fence-token","forced":false}`))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHAPromotionAPIResult([]byte(strings.ReplaceAll(haPromotionResponseJSON(), `"required_lsn":12`, `"required_lsn":0`)))
	g.Expect(ok).To(BeFalse())

	legacyResult, ok := parseHAPromotionAPIResult([]byte(`{"schema_version":1,"result":{"promote_current_fence":{"assessment":{"required_lsn":15,"received_lsn":14,"applied_lsn":14,"has_required_lsn":false,"caught_up_to_received":true,"fencing_confirmed":true,"force":true,"mode":"lossy","data_loss_possible":true,"safe":false,"requires_fencing":false,"requires_force":false,"can_promote":true},"promotion":{"node_id":"standby-a","switch_lsn":15,"old_identity":{"cluster_id":100,"shard_id":0,"table_id":0,"timeline_id":5,"epoch":7},"new_identity":{"cluster_id":100,"shard_id":0,"table_id":0,"timeline_id":6,"epoch":8},"forced":true,"data_loss_possible":true},"fence_generation":4,"fence_token":"legacy-token","forced":true}}}`))
	g.Expect(ok).To(BeTrue())
	g.Expect(legacyResult.SchemaVersion).To(Equal(uint32(1)))
	g.Expect(legacyResult.PromotedNodeID).To(Equal("standby-a"))
	g.Expect(legacyResult.SwitchLSN).To(Equal(uint64(15)))
	g.Expect(legacyResult.ParentClusterID).To(Equal(uint64(100)))
	g.Expect(legacyResult.ParentShardID).To(Equal(uint64(0)))
	g.Expect(legacyResult.ParentTimelineID).To(Equal(uint64(5)))
	g.Expect(legacyResult.NewEpoch).To(Equal(uint64(8)))
	g.Expect(legacyResult.ObservedLSN).To(Equal(uint64(14)))
	g.Expect(legacyResult.FenceGeneration).To(Equal(uint64(4)))
	g.Expect(legacyResult.FenceToken).To(Equal("legacy-token"))
	g.Expect(legacyResult.Forced).To(BeTrue())
	g.Expect(legacyResult.DataLossPossible).To(BeTrue())
}

func TestHAPromotionSDKResultPreservesRequiredEvidence(t *testing.T) {
	g := NewWithT(t)

	response := adminsdk.HAPromotionResponse{
		SchemaVersion: 1,
		Action: adminsdk.HAActionReceipt{
			ActionId:   "promotion:standby-a",
			ActionKind: adminsdk.HAActionKindPromotion,
			Target:     "standby-a",
			State:      adminsdk.HAActionStateApplied,
			NodeId:     "standby-a",
		},
		Assessment: adminsdk.HAPromotionAssessment{
			RequiredLsn:        12,
			ReceivedLsn:        12,
			AppliedLsn:         12,
			HasRequiredLsn:     true,
			CaughtUpToReceived: true,
			FencingConfirmed:   true,
			Force:              false,
			Mode:               adminsdk.HAPromotionModeSafe,
			DataLossPossible:   false,
			Safe:               true,
			RequiresFencing:    false,
			RequiresForce:      false,
			CanPromote:         true,
		},
		Promotion: adminsdk.HAPromotionResult{
			NodeId:    "standby-a",
			SwitchLsn: 13,
			OldIdentity: adminsdk.HAIdentity{
				ClusterId:  100,
				ShardId:    10,
				TableId:    20,
				TimelineId: 4,
				Epoch:      6,
			},
			NewIdentity: adminsdk.HAIdentity{
				ClusterId:  100,
				ShardId:    10,
				TableId:    20,
				TimelineId: 5,
				Epoch:      7,
			},
			Forced:           false,
			DataLossPossible: false,
		},
		FenceGeneration: 3,
		FenceToken:      "ha-fence-token",
		Forced:          false,
	}
	result := haPromotionJobResultFromSDK(response)
	g.Expect(result.PromotionMode).To(Equal("safe"))

	action := antflyv1.HAPlannedActionStatus{
		Kind:            string(haActionPromoteStandby),
		StandbyName:     "standby-a",
		TargetLSN:       12,
		FenceGeneration: 3,
		FenceReason:     "AutomaticFailoverReady",
	}
	adminResult := haPromotionAdminActionResult(result)
	enrichHAPromotionAdminActionResult(adminResult, &antflyv1.HAReplicationIdentitySpec{
		ClusterID:        100,
		ShardID:          10,
		TableID:          20,
		TimelineID:       4,
		Epoch:            6,
		CurrentPrimaryID: "primary-a",
	}, action)
	action.AdminResult = adminResult

	g.Expect(adminResult.PromotionMode).To(Equal("safe"))
	g.Expect(haActionHasRequiredAdminResult(action)).To(BeTrue())
}

func TestHAAdminSDKActionResultsSatisfyOperatorEvidenceGates(t *testing.T) {
	g := NewWithT(t)

	directPrimaryAction := func(kind haActionKind) antflyv1.HAPlannedActionStatus {
		return antflyv1.HAPlannedActionStatus{
			Kind:         string(kind),
			StandbyName:  "standby-a",
			TargetLSN:    5,
			AdminJobName: haAdminDirectAPIName,
			AdminNodeID:  "primary-a",
		}
	}
	receipt := func(kind adminsdk.HAActionReceiptActionKind, target, state, nodeID string) adminsdk.HAActionReceipt {
		return adminsdk.HAActionReceipt{
			ActionId:   string(kind) + ":" + target,
			ActionKind: kind,
			Target:     target,
			State:      adminsdk.HAActionReceiptState(state),
			NodeId:     nodeID,
		}
	}

	slotAction := directPrimaryAction(haActionCreateSlot)
	slot := haAdminActionResultFromReplicationSlotSDK(adminsdk.HAReplicationSlotActionResponse{
		SchemaVersion: 1,
		Action:        receipt(adminsdk.HAActionKindReplicationSlotCreate, "standby-a", string(adminsdk.HAActionStateApplied), "primary-a"),
		SlotAction:    adminsdk.HAReplicationSlotActionCreate,
		Slot: adminsdk.HAReplicationSlot{
			SlotName:       "standby-a",
			TimelineId:     4,
			RestartLsn:     5,
			ReceivedLsn:    5,
			AppliedLsn:     5,
			SafeReadLsn:    5,
			Active:         true,
			ReseedRequired: false,
			CurrentLsn:     5,
		},
	})
	g.Expect(requireHADirectAdminActionResultStatus(&slotAction, slot)).To(Succeed())
	g.Expect(slotAction.AdminResult.SlotAction).To(Equal("create"))
	g.Expect(slotAction.AdminResult.ActionNodeID).To(Equal("primary-a"))

	seedAction := directPrimaryAction(haActionSeedStandby)
	seedBegin := haAdminActionResultFromBaseBackupBeginSDK(adminsdk.HABaseBackupBeginResponse{
		SchemaVersion:  1,
		Action:         receipt(adminsdk.HAActionKindBaseBackupBegin, "base-standby-a-5", string(adminsdk.HAActionStateApplied), "primary-a"),
		SlotName:       "standby-a",
		ManifestId:     "base-standby-a-5",
		BackupLsn:      5,
		StartRecordLsn: 6,
	})
	g.Expect(requireHADirectAdminActionResultStatus(&seedAction, seedBegin)).To(Succeed())
	g.Expect(seedAction.AdminResult.ManifestID).To(Equal("base-standby-a-5"))
	g.Expect(seedAction.AdminResult.StartRecordLSN).To(Equal(uint64(6)))

	finishAction := directPrimaryAction(haActionFinishStandbySeed)
	seedFinish := haAdminActionResultFromBaseBackupFinishSDK(adminsdk.HABaseBackupFinishResponse{
		SchemaVersion: 1,
		Action:        receipt(adminsdk.HAActionKindBaseBackupFinish, "base-standby-a-5", string(adminsdk.HAActionStateApplied), "primary-a"),
		ManifestId:    "base-standby-a-5",
		BackupLsn:     5,
		EndRecordLsn:  7,
	})
	g.Expect(requireHADirectAdminActionResultStatus(&finishAction, seedFinish)).To(Succeed())
	g.Expect(finishAction.AdminResult.EndRecordLSN).To(Equal(uint64(7)))

	bootstrapAction := directPrimaryAction(haActionBootstrapStandbySeed)
	bootstrapAction.AdminNodeID = "standby-a"
	bootstrap := haAdminActionResultFromStandbyBootstrapSDK(adminsdk.HAStandbyBootstrapResponse{
		SchemaVersion: 1,
		Action:        receipt(adminsdk.HAActionKindStandbyBootstrap, "base-standby-a-5", string(adminsdk.HAActionStateApplied), "standby-a"),
		ManifestId:    "base-standby-a-5",
		BackupLsn:     5,
		CheckpointLsn: 7,
	})
	g.Expect(requireHADirectAdminActionResultStatus(&bootstrapAction, bootstrap)).To(Succeed())
	g.Expect(bootstrapAction.AdminResult.CheckpointLSN).To(Equal(uint64(7)))

	promoteAction := antflyv1.HAPlannedActionStatus{
		Kind:         string(haActionAssessPromotion),
		StandbyName:  "standby-a",
		TargetLSN:    12,
		AdminJobName: haAdminDirectAPIName,
		AdminNodeID:  "standby-a",
	}
	promotionAssess := haAdminActionResultFromPromotionAssessSDK(adminsdk.HAPromotionAssessResponse{
		SchemaVersion: 1,
		Action:        receipt(adminsdk.HAActionKindPromotionAssess, "standby-a", string(adminsdk.HAActionStateAssessed), "standby-a"),
		Assessment: adminsdk.HAPromotionAssessment{
			RequiredLsn:        12,
			ReceivedLsn:        12,
			AppliedLsn:         12,
			HasRequiredLsn:     true,
			CaughtUpToReceived: true,
			FencingConfirmed:   true,
			Force:              false,
			Mode:               adminsdk.HAPromotionModeSafe,
			DataLossPossible:   false,
			Safe:               true,
			RequiresFencing:    false,
			RequiresForce:      false,
			CanPromote:         true,
		},
	})
	g.Expect(requireHADirectPromotionAssessmentResultStatus(&promoteAction, promotionAssess)).To(Succeed())
	g.Expect(promoteAction.AdminResult.PromotionMode).To(Equal("safe"))

	fenceCluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID:        100,
					ShardID:          10,
					TableID:          20,
					TimelineID:       4,
					Epoch:            6,
					CurrentPrimaryID: "primary-a",
				},
			},
		},
	}
	fenceAction := antflyv1.HAPlannedActionStatus{
		Kind:            string(haActionAcquireFence),
		StandbyName:     "standby-a",
		TargetLSN:       12,
		FenceGeneration: 3,
		AdminJobName:    haAdminDirectAPIName,
		AdminNodeID:     "standby-a",
	}
	fence := haAdminActionResultFromFenceSDK(adminsdk.HAFenceResponse{
		SchemaVersion: 1,
		Action:        receipt(adminsdk.HAActionKindFenceAcquire, "standby-a", string(adminsdk.HAActionStateApplied), "standby-a"),
		Receipt: adminsdk.HAFenceReceipt{
			Generation:       3,
			Token:            "ha-fence-token",
			Identity:         adminsdk.HAIdentity{ClusterId: 100, ShardId: 10, TableId: 20, TimelineId: 5, Epoch: 7},
			OldPrimaryId:     "primary-a",
			PromotedNodeId:   "standby-a",
			ParentTimelineId: 4,
			ParentEpoch:      6,
			NewTimelineId:    5,
			NewEpoch:         7,
			RequiredLsn:      12,
			ObservedLsn:      12,
			Forced:           false,
			Reason:           "LeaseAcquired",
		},
	})
	g.Expect(requireHADirectFenceAcquireResultStatus(fenceCluster, &fenceAction, fence)).To(Succeed())
	g.Expect(fenceAction.AdminResult.FenceToken).To(Equal("ha-fence-token"))
}

func TestHARejoinSDKResultSatisfiesOperatorEvidenceGates(t *testing.T) {
	g := NewWithT(t)

	newRejoinCluster := func() *antflyv1.AntflyCluster {
		return &antflyv1.AntflyCluster{
			Status: antflyv1.AntflyClusterStatus{
				HAStatus: &antflyv1.HAStatus{
					LastPromotion: &antflyv1.HAPromotionStatus{
						ClusterID:         100,
						ShardID:           10,
						TableID:           20,
						OldPrimaryID:      "primary-a",
						PromotedStandbyID: "standby-a",
						ParentTimelineID:  4,
						ParentEpoch:       6,
						NewTimelineID:     5,
						NewEpoch:          7,
						SwitchLSN:         12,
						RequiredLSN:       12,
						ObservedLSN:       12,
						FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
						FenceGeneration:   3,
						FenceToken:        "ha-fence-token",
					},
				},
			},
		}
	}

	demoteCluster := &antflyv1.AntflyCluster{
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				LastPromotion: &antflyv1.HAPromotionStatus{
					ClusterID:         9002003,
					ShardID:           1024863633216429947,
					TableID:           7062478063073158706,
					OldPrimaryID:      "primary-a",
					PromotedStandbyID: "standby-a",
					ParentTimelineID:  1,
					ParentEpoch:       1,
					NewTimelineID:     2,
					NewEpoch:          2,
					SwitchLSN:         4,
					RequiredLSN:       3,
					ObservedLSN:       3,
					FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
					FenceGeneration:   1,
					FenceToken:        "ha-fence-token",
				},
			},
		},
	}
	demoteAction := antflyv1.HAPlannedActionStatus{
		Kind:            string(haActionDemoteFormerPrimary),
		StandbyName:     "primary-a",
		TargetLSN:       4,
		ObservedLSN:     4,
		RetainedFromLSN: 3,
		FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
		FenceGeneration: 1,
		AdminJobName:    haAdminDirectAPIName,
		AdminNodeID:     "primary-a",
	}
	demoteResponse := adminsdk.HARejoinAssessResponse{
		SchemaVersion: 1,
		Action: adminsdk.HAActionReceipt{
			ActionId:   "rejoin_assess:primary-a",
			ActionKind: adminsdk.HAActionKindRejoinAssess,
			Target:     "primary-a",
			State:      adminsdk.HAActionStateAssessed,
			NodeId:     "primary-a",
		},
		Assessment: adminsdk.HARejoinAssessment{
			Action:            adminsdk.HARejoinActionRejectUnfenced,
			Reason:            adminsdk.HARejoinReasonNoFence,
			FormerNodeId:      "primary-a",
			TargetTimelineId:  1,
			TargetEpoch:       1,
			ParentClusterId:   9002003,
			ParentShardId:     1024863633216429947,
			ParentTableId:     7062478063073158706,
			ParentTimelineId:  1,
			ParentEpoch:       1,
			ForkLsn:           4,
			FormerLastLsn:     4,
			RetainedFromLsn:   3,
			DataLossDiscarded: false,
		},
	}
	reconciler := &AntflyClusterReconciler{}
	g.Expect(reconciler.applyHADirectRejoinAssessResultFromSDK(demoteCluster, &demoteAction, demoteResponse)).To(BeTrue())
	demoteAction.AdminJobName = haAdminDirectAPIName
	demoteAction.AdminJobPhase = haAdminJobPhaseSucceeded
	g.Expect(demoteAction.AdminResult).NotTo(BeNil())
	g.Expect(demoteAction.AdminResult.RejoinAction).To(Equal("reject_unfenced"))
	g.Expect(haAdminActionSucceededWithStatusEvidence(demoteCluster.Status.HAStatus, demoteAction)).To(BeTrue())
	g.Expect(demoteCluster.Status.HAStatus.FormerPrimary).NotTo(BeNil())
	g.Expect(demoteCluster.Status.HAStatus.FormerPrimary.Action).To(Equal(string(haActionDemoteFormerPrimary)))
	g.Expect(demoteCluster.Status.HAStatus.FormerPrimary.Fenced).To(BeFalse())
	g.Expect(demoteCluster.Status.HAStatus.FormerPrimary.Reason).To(Equal("no_fence"))

	cluster := newRejoinCluster()
	rewindAction := antflyv1.HAPlannedActionStatus{
		Kind:            string(haActionRewindFormerPrimary),
		StandbyName:     "primary-a",
		TargetLSN:       12,
		ObservedLSN:     13,
		RetainedFromLSN: 8,
		FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
		FenceGeneration: 3,
		AdminJobName:    haAdminDirectAPIName,
		AdminNodeID:     "primary-a",
	}
	response := adminsdk.HARejoinAssessResponse{
		SchemaVersion: 1,
		Action: adminsdk.HAActionReceipt{
			ActionId:   "rejoin_rewind:primary-a",
			ActionKind: adminsdk.HAActionKindRejoinRewind,
			Target:     "primary-a",
			State:      adminsdk.HAActionStateApplied,
			NodeId:     "primary-a",
		},
		Assessment: adminsdk.HARejoinAssessment{
			Action:            adminsdk.HARejoinActionRewind,
			Reason:            adminsdk.HARejoinReasonParentTimelineRetained,
			FormerNodeId:      "primary-a",
			TargetTimelineId:  5,
			TargetEpoch:       7,
			ParentClusterId:   100,
			ParentShardId:     10,
			ParentTableId:     20,
			ParentTimelineId:  4,
			ParentEpoch:       6,
			ForkLsn:           12,
			FormerLastLsn:     13,
			RetainedFromLsn:   8,
			DataLossDiscarded: true,
		},
		Rewind: adminsdk.HARejoinRewindResult{
			NodeId:            "primary-a",
			ForkLsn:           12,
			PreviousLastLsn:   13,
			CurrentLastLsn:    12,
			NextLsn:           13,
			DiscardedLsnCount: 1,
			TargetTimelineId:  5,
			TargetEpoch:       7,
			DataLossDiscarded: true,
		},
	}

	g.Expect(reconciler.applyHADirectRejoinAssessResultFromSDK(cluster, &rewindAction, response)).To(BeTrue())
	g.Expect(rewindAction.AdminResult).NotTo(BeNil())
	g.Expect(rewindAction.AdminResult.RejoinAction).To(Equal("rewind"))
	g.Expect(rewindAction.AdminResult.RewindExecuted).To(BeTrue())
	g.Expect(cluster.Status.HAStatus.FormerPrimary).NotTo(BeNil())
	g.Expect(cluster.Status.HAStatus.FormerPrimary.NodeID).To(Equal("primary-a"))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.Action).To(Equal(string(haActionRewindFormerPrimary)))

	staleObservedAction := rewindAction
	staleObservedAction.AdminResult = nil
	staleObservedAction.ObservedLSN = 13
	staleObservedResponse := response
	staleObservedResponse.Assessment.FormerLastLsn = 12
	staleObservedResponse.Assessment.DataLossDiscarded = false
	staleObservedResponse.Rewind.PreviousLastLsn = 12
	staleObservedResponse.Rewind.CurrentLastLsn = 12
	staleObservedResponse.Rewind.DiscardedLsnCount = 0
	staleObservedResponse.Rewind.DataLossDiscarded = false
	staleObservedCluster := newRejoinCluster()
	g.Expect(reconciler.applyHADirectRejoinAssessResultFromSDK(staleObservedCluster, &staleObservedAction, staleObservedResponse)).To(BeTrue())
	g.Expect(staleObservedAction.AdminResult).NotTo(BeNil())
	g.Expect(staleObservedAction.AdminResult.FormerLastLSN).To(Equal(uint64(12)))

	reseedAction := antflyv1.HAPlannedActionStatus{
		Kind:            string(haActionReseedFormerPrimary),
		StandbyName:     "primary-a",
		TargetLSN:       12,
		ObservedLSN:     13,
		RetainedFromLSN: 20,
		FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
		FenceGeneration: 3,
		AdminJobName:    haAdminDirectAPIName,
		AdminNodeID:     "standby-a",
	}
	reseedResponse := adminsdk.HARejoinAssessResponse{
		SchemaVersion: 1,
		Action: adminsdk.HAActionReceipt{
			ActionId:   "rejoin_reseed:primary-a",
			ActionKind: adminsdk.HAActionKindRejoinReseed,
			Target:     "primary-a",
			State:      adminsdk.HAActionStateApplied,
			NodeId:     "standby-a",
		},
		Assessment: adminsdk.HARejoinAssessment{
			Action:            adminsdk.HARejoinActionReseed,
			Reason:            adminsdk.HARejoinReasonParentTimelineWALExpired,
			FormerNodeId:      "primary-a",
			TargetTimelineId:  5,
			TargetEpoch:       7,
			ParentClusterId:   100,
			ParentShardId:     10,
			ParentTableId:     20,
			ParentTimelineId:  4,
			ParentEpoch:       6,
			ForkLsn:           12,
			FormerLastLsn:     13,
			RetainedFromLsn:   20,
			DataLossDiscarded: true,
		},
		Reseed: adminsdk.HARejoinReseedResult{
			NodeId:             "primary-a",
			SlotName:           "primary-a",
			TargetTimelineId:   5,
			TargetEpoch:        7,
			ForkLsn:            12,
			FormerLastLsn:      13,
			ReseedRequired:     true,
			BaseBackupRequired: true,
		},
	}

	reseedCluster := newRejoinCluster()
	g.Expect(reconciler.applyHADirectRejoinAssessResultFromSDK(reseedCluster, &reseedAction, reseedResponse)).To(BeTrue())
	g.Expect(reseedAction.AdminResult).NotTo(BeNil())
	g.Expect(reseedAction.AdminResult.ActionNodeID).To(Equal("standby-a"))
	g.Expect(reseedAction.AdminResult.RejoinAction).To(Equal("reseed"))
	g.Expect(reseedAction.AdminResult.ReseedExecuted).To(BeTrue())
	g.Expect(reseedAction.AdminResult.ReseedSlotName).To(Equal("primary-a"))
	g.Expect(reseedAction.AdminResult.ReseedBaseBackupRequired).To(BeTrue())
	g.Expect(reseedCluster.Status.HAStatus.FormerPrimary).NotTo(BeNil())
	g.Expect(reseedCluster.Status.HAStatus.FormerPrimary.NodeID).To(Equal("primary-a"))
	g.Expect(reseedCluster.Status.HAStatus.FormerPrimary.Action).To(Equal(string(haActionReseedFormerPrimary)))
	g.Expect(reseedCluster.Status.HAStatus.FormerPrimary.ReseedRequired).To(BeTrue())

	incompleteReseedAction := reseedAction
	incompleteReseedAction.AdminResult = nil
	incompleteReseed := reseedResponse
	incompleteReseed.Reseed.BaseBackupRequired = false
	g.Expect(reconciler.applyHADirectRejoinAssessResultFromSDK(newRejoinCluster(), &incompleteReseedAction, incompleteReseed)).To(BeFalse())
	g.Expect(incompleteReseedAction.AdminResult).To(BeNil())
}

func TestParseHADirectAdminActionResultAcceptsOpenAPIAndLegacyShapes(t *testing.T) {
	g := NewWithT(t)

	seed, ok := parseHADirectAdminActionResult([]byte(`{"schema_version":1,"action":{"action_id":"base_backup_begin:base-standby-a-5","action_kind":"base_backup_begin","target":"base-standby-a-5","state":"applied","node_id":"primary-a"},"slot_name":"standby-a","manifest_id":"base-standby-a-5","backup_lsn":5,"start_record_lsn":5}`))
	g.Expect(ok).To(BeTrue())
	g.Expect(seed.SchemaVersion).To(Equal(uint32(1)))
	g.Expect(seed.ActionID).To(Equal("base_backup_begin:base-standby-a-5"))
	g.Expect(seed.ActionKind).To(Equal("base_backup_begin"))
	g.Expect(seed.ActionTarget).To(Equal("base-standby-a-5"))
	g.Expect(seed.ActionState).To(Equal("applied"))
	g.Expect(seed.SlotName).To(Equal("standby-a"))
	g.Expect(seed.ManifestID).To(Equal("base-standby-a-5"))
	g.Expect(seed.BackupLSN).To(Equal(uint64(5)))
	g.Expect(seed.StartRecordLSN).To(Equal(uint64(5)))

	_, ok = parseHADirectAdminActionResult([]byte(`{"action":{"action_id":"base_backup_begin:base-standby-a-5","action_kind":"base_backup_begin","target":"base-standby-a-5","state":"applied","node_id":"primary-a"},"slot_name":"standby-a","manifest_id":"base-standby-a-5","backup_lsn":5,"start_record_lsn":5}`))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHADirectAdminActionResult([]byte(`{"schema_version":1,"action":{"action_id":"base_backup_begin:base-standby-a-5","action_kind":"base_backup_begin","target":"base-standby-a-5","state":"applied","node_id":"primary-a"},"slot_name":"standby-a","manifest_id":"base-standby-a-5","backup_lsn":5}`))
	g.Expect(ok).To(BeFalse())

	promotionAssess, ok := parseHADirectAdminActionResult([]byte(`{"schema_version":1,"action":{"action_id":"promotion_assess:standby-a","action_kind":"promotion_assess","target":"standby-a","state":"assessed","node_id":"standby-a"},"assessment":{"required_lsn":0,"received_lsn":0,"applied_lsn":0,"has_required_lsn":true,"caught_up_to_received":true,"fencing_confirmed":true,"force":false,"mode":"safe","data_loss_possible":false,"safe":true,"requires_fencing":false,"requires_force":false,"can_promote":true}}`))
	g.Expect(ok).To(BeTrue())
	g.Expect(promotionAssess.PromotionRequiredLSN).To(Equal(uint64(0)))
	g.Expect(promotionAssess.PromotionReceivedLSN).To(Equal(uint64(0)))
	g.Expect(promotionAssess.PromotionAppliedLSN).To(Equal(uint64(0)))
	g.Expect(promotionAssess.PromotionSafe).To(BeTrue())

	_, ok = parseHADirectAdminActionResult([]byte(`{"schema_version":1,"action":{"action_id":"promotion_assess:standby-a","action_kind":"promotion_assess","target":"standby-a","state":"assessed","node_id":"standby-a"},"assessment":{"required_lsn":0,"received_lsn":0,"applied_lsn":0,"has_required_lsn":false,"caught_up_to_received":true,"fencing_confirmed":true,"force":false,"mode":"safe","data_loss_possible":false,"safe":true,"requires_fencing":false,"requires_force":false,"can_promote":true}}`))
	g.Expect(ok).To(BeFalse())

	finish, ok := parseHADirectAdminActionResult([]byte(`{"schema_version":1,"action":{"action_id":"base_backup_finish:base-standby-a-5","action_kind":"base_backup_finish","target":"base-standby-a-5","state":"applied","node_id":"primary-a"},"manifest_id":"base-standby-a-5","backup_lsn":5,"end_record_lsn":8}`))
	g.Expect(ok).To(BeTrue())
	g.Expect(finish.ActionID).To(Equal("base_backup_finish:base-standby-a-5"))
	g.Expect(finish.ActionKind).To(Equal("base_backup_finish"))
	g.Expect(finish.ActionTarget).To(Equal("base-standby-a-5"))
	g.Expect(finish.ActionState).To(Equal("applied"))
	g.Expect(finish.ManifestID).To(Equal("base-standby-a-5"))
	g.Expect(finish.BackupLSN).To(Equal(uint64(5)))
	g.Expect(finish.EndRecordLSN).To(Equal(uint64(8)))

	_, ok = parseHADirectAdminActionResult([]byte(`{"schema_version":1,"manifest_id":"base-standby-a-5","backup_lsn":5,"end_record_lsn":8}`))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHADirectAdminActionResult([]byte(`{"schema_version":1,"action":{"action_id":"base_backup_finish:base-standby-a-5","action_kind":"base_backup_finish","target":"base-standby-a-5","state":"applied","node_id":"primary-a"},"manifest_id":"base-standby-a-5","backup_lsn":5}`))
	g.Expect(ok).To(BeFalse())

	legacyFinish, ok := parseHADirectAdminActionResult([]byte(`{"schema_version":1,"result":{"seed_finish":{"manifest_id":"base-standby-a-5","backup_lsn":5,"end_record_lsn":8}}}`))
	g.Expect(ok).To(BeTrue())
	g.Expect(legacyFinish.SchemaVersion).To(Equal(uint32(1)))
	g.Expect(legacyFinish.ManifestID).To(Equal("base-standby-a-5"))
	g.Expect(legacyFinish.BackupLSN).To(Equal(uint64(5)))
	g.Expect(legacyFinish.EndRecordLSN).To(Equal(uint64(8)))

	bootstrap, ok := parseHADirectAdminActionResult([]byte(`{"schema_version":1,"action":{"action_id":"standby_bootstrap:base-standby-a-5","action_kind":"standby_bootstrap","target":"base-standby-a-5","state":"applied","node_id":"standby-a"},"manifest_id":"base-standby-a-5","backup_lsn":5,"checkpoint_lsn":7}`))
	g.Expect(ok).To(BeTrue())
	g.Expect(bootstrap.ActionID).To(Equal("standby_bootstrap:base-standby-a-5"))
	g.Expect(bootstrap.ManifestID).To(Equal("base-standby-a-5"))
	g.Expect(bootstrap.BackupLSN).To(Equal(uint64(5)))
	g.Expect(bootstrap.CheckpointLSN).To(Equal(uint64(7)))

	_, ok = parseHADirectAdminActionResult([]byte(`{"schema_version":1,"action":{"action_id":"standby_bootstrap:base-standby-a-5","action_kind":"standby_bootstrap","target":"base-standby-a-5"},"manifest_id":"base-standby-a-5","backup_lsn":5,"checkpoint_lsn":7}`))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHADirectAdminActionResult([]byte(`{"schema_version":1,"action":{"action_id":"standby_bootstrap:base-standby-a-5","action_kind":"standby_bootstrap","target":"base-standby-a-5","state":"applied","node_id":"standby-a"},"manifest_id":"base-standby-a-5","backup_lsn":5}`))
	g.Expect(ok).To(BeFalse())

	legacyBootstrap, ok := parseHADirectAdminActionResult([]byte(`{"schema_version":1,"result":{"seed_bootstrap":{"manifest_id":"base-standby-a-5","backup_lsn":5,"checkpoint_lsn":7}}}`))
	g.Expect(ok).To(BeTrue())
	g.Expect(legacyBootstrap.SchemaVersion).To(Equal(uint32(1)))
	g.Expect(legacyBootstrap.ManifestID).To(Equal("base-standby-a-5"))
	g.Expect(legacyBootstrap.BackupLSN).To(Equal(uint64(5)))
	g.Expect(legacyBootstrap.CheckpointLSN).To(Equal(uint64(7)))

	slot, ok := parseHADirectAdminActionResult([]byte(haReplicationSlotActionResponseJSON("replication_slot_pause", "pause", "standby-a", "primary-a")))
	g.Expect(ok).To(BeTrue())
	g.Expect(slot.ActionID).To(Equal("replication_slot_pause:standby-a"))
	g.Expect(slot.SlotAction).To(Equal("pause"))
	g.Expect(slot.SlotName).To(Equal("standby-a"))

	_, ok = parseHADirectAdminActionResult([]byte(`{"schema_version":1,"action":{"action_id":"replication_slot_pause:standby-a","action_kind":"replication_slot_pause","target":"standby-a","state":"applied","node_id":"primary-a"},"slot_action":"pause","slot":{"slot_name":"standby-a"}}`))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHADirectAdminActionResult([]byte(`{"schema_version":1,"slot_action":"pause","slot":{"slot_name":"standby-a"}}`))
	g.Expect(ok).To(BeFalse())

	fence, ok := parseHADirectAdminActionResult([]byte(`{"schema_version":1,"result":{"fence_acquire":{"receipt":{"generation":3,"token":"legacy-token"}}}}`))
	g.Expect(ok).To(BeTrue())
	g.Expect(fence.SchemaVersion).To(Equal(uint32(1)))
	g.Expect(fence.FenceGeneration).To(Equal(uint64(3)))
	g.Expect(fence.FenceToken).To(Equal("legacy-token"))

	openAPIFence, ok := parseHADirectAdminActionResult([]byte(haFenceResponseJSON("primary-a", "standby-a", 3, "ha-fence-token")))
	g.Expect(ok).To(BeTrue())
	g.Expect(openAPIFence.SchemaVersion).To(Equal(uint32(1)))
	g.Expect(openAPIFence.FenceClusterID).To(Equal(uint64(100)))
	g.Expect(openAPIFence.FenceShardID).To(Equal(uint64(10)))
	g.Expect(openAPIFence.FenceTableID).To(Equal(uint64(20)))
	g.Expect(openAPIFence.FenceOldPrimaryID).To(Equal("primary-a"))
	g.Expect(openAPIFence.FencePromotedNodeID).To(Equal("standby-a"))
	g.Expect(openAPIFence.FenceParentTimelineID).To(Equal(uint64(4)))
	g.Expect(openAPIFence.FenceNewTimelineID).To(Equal(uint64(5)))
	g.Expect(openAPIFence.FenceRequiredLSN).To(Equal(uint64(12)))
	g.Expect(openAPIFence.FenceObservedLSN).To(Equal(uint64(12)))
	g.Expect(openAPIFence.FenceGeneration).To(Equal(uint64(3)))
	g.Expect(openAPIFence.FenceToken).To(Equal("ha-fence-token"))

	emptyReasonFence, ok := parseHADirectAdminActionResult([]byte(strings.Replace(haFenceResponseJSON("primary-a", "standby-a", 3, "ha-fence-token"), `"reason":"LeaseAcquired"`, `"reason":""`, 1)))
	g.Expect(ok).To(BeTrue())
	g.Expect(emptyReasonFence.FenceReason).To(Equal(""))

	_, ok = parseHADirectAdminActionResult([]byte(haFenceResponseJSONWithoutReceiptPath("forced")))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHADirectAdminActionResult([]byte(haFenceResponseJSONWithoutReceiptPath("reason")))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHADirectAdminActionResult([]byte(haFenceResponseJSONWithoutReceiptPath("generation")))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHADirectAdminActionResult([]byte(haFenceResponseJSONWithoutReceiptPath("identity", "epoch")))
	g.Expect(ok).To(BeFalse())
}

func TestParseHAAdminActionResultTable(t *testing.T) {
	g := NewWithT(t)

	slotResume, ok := parseHAAdminActionResultTable(`{"schema_version":1,"action":{"action_id":"replication_slot_resume:standby-a","action_kind":"replication_slot_resume","target":"standby-a","state":"applied","node_id":"primary-a"},"slot_action":"resume","slot":{"slot_name":"standby-a","timeline_id":1,"restart_lsn":0,"received_lsn":0,"applied_lsn":0,"safe_read_lsn":0,"active":true,"reseed_required":false,"last_error":null,"current_lsn":0,"dropped":false}}`)
	g.Expect(ok).To(BeTrue())
	g.Expect(slotResume.SchemaVersion).To(Equal(uint32(1)))
	g.Expect(slotResume.ActionID).To(Equal("replication_slot_resume:standby-a"))
	g.Expect(slotResume.ActionKind).To(Equal("replication_slot_resume"))
	g.Expect(slotResume.ActionTarget).To(Equal("standby-a"))
	g.Expect(slotResume.ActionState).To(Equal("applied"))
	g.Expect(slotResume.ActionNodeID).To(Equal("primary-a"))
	g.Expect(slotResume.SlotAction).To(Equal("resume"))
	g.Expect(slotResume.SlotName).To(Equal("standby-a"))

	finish, ok := parseHAAdminActionResultTable(strings.Join([]string{
		"result=seed_finish",
		"action.action_id=base_backup_finish:base-standby-a-5",
		"action.action_kind=base_backup_finish",
		"action.target=base-standby-a-5",
		"action.state=applied",
		"action.node_id=primary-a",
		"manifest_id=base-standby-a-5",
		"backup_lsn=5",
		"end_record_lsn=8",
		"",
	}, "\n"))
	g.Expect(ok).To(BeTrue())
	g.Expect(finish.ActionID).To(Equal("base_backup_finish:base-standby-a-5"))
	g.Expect(finish.ActionKind).To(Equal("base_backup_finish"))
	g.Expect(finish.ActionTarget).To(Equal("base-standby-a-5"))
	g.Expect(finish.ActionState).To(Equal("applied"))
	g.Expect(finish.ActionNodeID).To(Equal("primary-a"))
	g.Expect(finish.ManifestID).To(Equal("base-standby-a-5"))
	g.Expect(finish.BackupLSN).To(Equal(uint64(5)))
	g.Expect(finish.EndRecordLSN).To(Equal(uint64(8)))

	bootstrap, ok := parseHAAdminActionResultTable(strings.Join([]string{
		"result=seed_bootstrap",
		"manifest_id=base-standby-a-5",
		"backup_lsn=5",
		"checkpoint_lsn=7",
		"",
	}, "\n"))
	g.Expect(ok).To(BeTrue())
	g.Expect(bootstrap.ManifestID).To(Equal("base-standby-a-5"))
	g.Expect(bootstrap.BackupLSN).To(Equal(uint64(5)))
	g.Expect(bootstrap.CheckpointLSN).To(Equal(uint64(7)))

	fence, ok := parseHAAdminActionResultTable(strings.Join([]string{
		"result=fence_acquire",
		"generation=3",
		"token=ha-fence-token",
		"",
	}, "\n"))
	g.Expect(ok).To(BeTrue())
	g.Expect(fence.FenceGeneration).To(Equal(uint64(3)))
	g.Expect(fence.FenceToken).To(Equal("ha-fence-token"))

	promotion, ok := parseHAAdminActionResultTable(strings.Join([]string{
		"result=promote_current_fence",
		"action.action_id=promotion:standby-a",
		"action.action_kind=promotion",
		"action.target=standby-a",
		"action.state=applied",
		"action.node_id=standby-a",
		"assessment.required_lsn=12",
		"assessment.received_lsn=12",
		"assessment.applied_lsn=12",
		"assessment.mode=safe",
		"promotion.node_id=standby-a",
		"promotion.switch_lsn=13",
		"promotion.old_identity.timeline_id=4",
		"promotion.old_identity.epoch=6",
		"promotion.new_identity.timeline_id=5",
		"promotion.new_identity.epoch=7",
		"promotion.forced=false",
		"promotion.data_loss_possible=false",
		"fence_generation=4",
		"fence_token=promotion-token",
		"",
	}, "\n"))
	g.Expect(ok).To(BeTrue())
	g.Expect(promotion.ActionID).To(Equal("promotion:standby-a"))
	g.Expect(promotion.ActionKind).To(Equal("promotion"))
	g.Expect(promotion.ActionTarget).To(Equal("standby-a"))
	g.Expect(promotion.ActionState).To(Equal("applied"))
	g.Expect(promotion.ActionNodeID).To(Equal("standby-a"))
	g.Expect(promotion.FenceGeneration).To(Equal(uint64(4)))
	g.Expect(promotion.FenceToken).To(Equal("promotion-token"))
	g.Expect(promotion.FencePromotedNodeID).To(Equal("standby-a"))
	g.Expect(promotion.PromotionMode).To(Equal("safe"))
	g.Expect(promotion.FenceParentTimelineID).To(Equal(uint64(4)))
	g.Expect(promotion.FenceNewTimelineID).To(Equal(uint64(5)))
	g.Expect(promotion.FenceRequiredLSN).To(Equal(uint64(12)))
	g.Expect(promotion.FenceObservedLSN).To(Equal(uint64(12)))
	g.Expect(promotion.PromotionForce).To(BeFalse())
	g.Expect(promotion.PromotionDataLossPossible).To(BeFalse())

	promotion, ok = parseHAAdminActionResultTable(strings.Join([]string{
		"result=promote_current_fence",
		"fence_generation=4",
		"fence_token=promotion-token",
		"",
	}, "\n"))
	g.Expect(ok).To(BeFalse())
	g.Expect(promotion).To(BeNil())

	rejoin, ok := parseHAAdminActionResultTable(strings.Join([]string{
		"result=rejoin_assess",
		"action.action_id=rejoin_assess:primary-a",
		"action.action_kind=rejoin_assess",
		"action.target=primary-a",
		"action.state=assessed",
		"action.node_id=primary-a",
		"action=rewind",
		"reason=parent_timeline_retained",
		"former_node_id=primary-a",
		"target_timeline_id=5",
		"target_epoch=7",
		"parent_cluster_id=100",
		"parent_shard_id=10",
		"parent_table_id=20",
		"parent_timeline_id=4",
		"parent_epoch=6",
		"fork_lsn=12",
		"former_last_lsn=13",
		"retained_from_lsn=8",
		"data_loss_discarded=true",
		"",
	}, "\n"))
	g.Expect(ok).To(BeTrue())
	g.Expect(rejoin.ActionID).To(Equal("rejoin_assess:primary-a"))
	g.Expect(rejoin.ActionKind).To(Equal("rejoin_assess"))
	g.Expect(rejoin.ActionTarget).To(Equal("primary-a"))
	g.Expect(rejoin.ActionState).To(Equal("assessed"))
	g.Expect(rejoin.ActionNodeID).To(Equal("primary-a"))
	g.Expect(rejoin.RejoinAction).To(Equal("rewind"))
	g.Expect(rejoin.RejoinReason).To(Equal("parent_timeline_retained"))
	g.Expect(rejoin.FormerNodeID).To(Equal("primary-a"))
	g.Expect(rejoin.TargetTimelineID).To(Equal(uint64(5)))
	g.Expect(rejoin.TargetEpoch).To(Equal(uint64(7)))
	g.Expect(rejoin.ForkLSN).To(Equal(uint64(12)))
	g.Expect(rejoin.FormerLastLSN).To(Equal(uint64(13)))
	g.Expect(rejoin.RetainedFromLSN).To(Equal(uint64(8)))
	g.Expect(rejoin.DataLossDiscarded).To(BeTrue())
}

func TestCompletedSlotAdminJobResultSatisfiesReceiptEvidence(t *testing.T) {
	g := NewWithT(t)

	action := antflyv1.HAPlannedActionStatus{
		Kind:        string(haActionResumeSlot),
		SlotName:    "standby-a",
		AdminNodeID: "primary-a",
		AdminURL:    "http://primary-ha.default.svc:8081",
		AdminMethod: "PUT",
		AdminPath:   "/admin/v1/ha/replication-slots/standby-a/resume",
	}
	action.AdminResult = haCompletedSlotAdminJobResult(action)

	g.Expect(action.AdminResult).NotTo(BeNil())
	g.Expect(action.AdminResult.ActionID).To(Equal("replication_slot_resume:standby-a"))
	g.Expect(action.AdminResult.ActionKind).To(Equal("replication_slot_resume"))
	g.Expect(action.AdminResult.ActionTarget).To(Equal("standby-a"))
	g.Expect(action.AdminResult.ActionState).To(Equal("applied"))
	g.Expect(action.AdminResult.ActionNodeID).To(Equal("primary-a"))
	g.Expect(action.AdminResult.SlotAction).To(Equal("resume"))
	g.Expect(action.AdminResult.SlotName).To(Equal("standby-a"))
	g.Expect(haActionHasRequiredAdminResult(action)).To(BeTrue())
	g.Expect(haDirectAdminActionReceiptMatches(action)).To(BeTrue())
}

func TestParseHARejoinJobResult(t *testing.T) {
	g := NewWithT(t)

	result, ok := parseHARejoinJobResult(strings.Join([]string{
		"result=rejoin_assess",
		"action=rewind",
		"reason=parent_timeline_retained",
		"former_node_id=primary-a",
		"target_timeline_id=5",
		"target_epoch=7",
		"parent_cluster_id=100",
		"parent_shard_id=10",
		"parent_table_id=20",
		"parent_timeline_id=4",
		"parent_epoch=6",
		"fork_lsn=12",
		"former_last_lsn=13",
		"retained_from_lsn=8",
		"data_loss_discarded=true",
		"",
	}, "\n"))

	g.Expect(ok).To(BeTrue())
	g.Expect(result.Action).To(Equal("rewind"))
	g.Expect(result.Reason).To(Equal("parent_timeline_retained"))
	g.Expect(result.FormerNodeID).To(Equal("primary-a"))
	g.Expect(result.TargetTimelineID).To(Equal(uint64(5)))
	g.Expect(result.TargetEpoch).To(Equal(uint64(7)))
	g.Expect(result.ParentClusterID).To(Equal(uint64(100)))
	g.Expect(result.ParentShardID).To(Equal(uint64(10)))
	g.Expect(result.ParentTableID).To(Equal(uint64(20)))
	g.Expect(result.ParentTimelineID).To(Equal(uint64(4)))
	g.Expect(result.ParentEpoch).To(Equal(uint64(6)))
	g.Expect(result.ForkLSN).To(Equal(uint64(12)))
	g.Expect(result.FormerLastLSN).To(Equal(uint64(13)))
	g.Expect(result.RetainedFromLSN).To(Equal(uint64(8)))
	g.Expect(result.DataLossDiscarded).To(BeTrue())

	rewindExecuted, ok := parseHARejoinJobResult(strings.Join([]string{
		"result=rejoin_rewind",
		"assessment.action=rewind",
		"assessment.reason=parent_timeline_retained",
		"assessment.former_node_id=primary-a",
		"assessment.target_timeline_id=5",
		"assessment.target_epoch=7",
		"assessment.parent_cluster_id=100",
		"assessment.parent_shard_id=10",
		"assessment.parent_table_id=20",
		"assessment.parent_timeline_id=4",
		"assessment.parent_epoch=6",
		"assessment.fork_lsn=12",
		"assessment.former_last_lsn=13",
		"assessment.retained_from_lsn=8",
		"assessment.data_loss_discarded=true",
		"rewind.node_id=primary-a",
		"rewind.fork_lsn=12",
		"rewind.previous_last_lsn=13",
		"rewind.current_last_lsn=12",
		"rewind.next_lsn=13",
		"rewind.discarded_lsn_count=1",
		"rewind.target_timeline_id=5",
		"rewind.target_epoch=7",
		"rewind.data_loss_discarded=true",
		"",
	}, "\n"))
	g.Expect(ok).To(BeTrue())
	g.Expect(rewindExecuted.Action).To(Equal("rewind"))
	g.Expect(rewindExecuted.RewindExecuted).To(BeTrue())
	g.Expect(rewindExecuted.RewindPreviousLastLSN).To(Equal(uint64(13)))
	g.Expect(rewindExecuted.RewindCurrentLastLSN).To(Equal(uint64(12)))
	g.Expect(rewindExecuted.RewindNextLSN).To(Equal(uint64(13)))
	g.Expect(rewindExecuted.RewindDiscardedLSNCount).To(Equal(uint64(1)))

	adminResult, ok := parseHAAdminActionResultTable(strings.Join([]string{
		"result=rejoin_rewind",
		"assessment.action=rewind",
		"assessment.reason=parent_timeline_retained",
		"assessment.former_node_id=primary-a",
		"assessment.target_timeline_id=5",
		"assessment.target_epoch=7",
		"assessment.parent_cluster_id=100",
		"assessment.parent_shard_id=10",
		"assessment.parent_table_id=20",
		"assessment.parent_timeline_id=4",
		"assessment.parent_epoch=6",
		"assessment.fork_lsn=12",
		"assessment.former_last_lsn=13",
		"assessment.retained_from_lsn=8",
		"assessment.data_loss_discarded=true",
		"rewind.node_id=primary-a",
		"rewind.fork_lsn=12",
		"rewind.previous_last_lsn=13",
		"rewind.current_last_lsn=12",
		"rewind.next_lsn=13",
		"rewind.discarded_lsn_count=1",
		"rewind.target_timeline_id=5",
		"rewind.target_epoch=7",
		"rewind.data_loss_discarded=true",
		"",
	}, "\n"))
	g.Expect(ok).To(BeTrue())
	g.Expect(adminResult.RejoinAction).To(Equal("rewind"))
	g.Expect(adminResult.RewindExecuted).To(BeTrue())
	g.Expect(adminResult.RewindDiscardedLSNCount).To(Equal(uint64(1)))

	_, ok = parseHARejoinJobResult(strings.Join([]string{
		"result=rejoin_rewind",
		"assessment.action=rewind",
		"assessment.reason=parent_timeline_retained",
		"assessment.former_node_id=primary-a",
		"assessment.target_timeline_id=5",
		"assessment.target_epoch=7",
		"assessment.parent_cluster_id=100",
		"assessment.parent_shard_id=10",
		"assessment.parent_table_id=20",
		"assessment.parent_timeline_id=4",
		"assessment.parent_epoch=6",
		"assessment.fork_lsn=12",
		"assessment.former_last_lsn=13",
		"assessment.retained_from_lsn=8",
		"rewind.node_id=primary-a",
		"rewind.fork_lsn=11",
		"rewind.previous_last_lsn=13",
		"rewind.current_last_lsn=11",
		"rewind.next_lsn=12",
		"rewind.discarded_lsn_count=2",
		"rewind.target_timeline_id=5",
		"rewind.target_epoch=7",
		"",
	}, "\n"))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHARejoinJobResult(strings.Join([]string{
		"result=rejoin_rewind",
		"assessment.action=rewind",
		"assessment.reason=parent_timeline_retained",
		"assessment.former_node_id=primary-a",
		"assessment.target_timeline_id=5",
		"assessment.target_epoch=7",
		"assessment.parent_cluster_id=100",
		"assessment.parent_shard_id=10",
		"assessment.parent_table_id=20",
		"assessment.parent_timeline_id=4",
		"assessment.parent_epoch=6",
		"assessment.fork_lsn=12",
		"assessment.former_last_lsn=13",
		"assessment.retained_from_lsn=8",
		"rewind.node_id=primary-b",
		"rewind.fork_lsn=12",
		"rewind.previous_last_lsn=13",
		"rewind.current_last_lsn=12",
		"rewind.next_lsn=13",
		"rewind.discarded_lsn_count=1",
		"rewind.target_timeline_id=5",
		"rewind.target_epoch=7",
		"",
	}, "\n"))
	g.Expect(ok).To(BeFalse())

	former := &antflyv1.HAFormerPrimaryStatus{}
	applyHARejoinJobResult(former, result)
	g.Expect(former.NodeID).To(Equal("primary-a"))
	g.Expect(former.AssessedAction).To(Equal("rewind"))
	g.Expect(former.AssessedReason).To(Equal("parent_timeline_retained"))
	g.Expect(former.Action).To(Equal(string(haActionRewindFormerPrimary)))
	g.Expect(former.Reason).To(Equal("parent_timeline_retained"))
	g.Expect(former.RejoinRequired).To(BeTrue())
	g.Expect(former.RewindPossible).To(BeTrue())
	g.Expect(former.ReseedRequired).To(BeFalse())
	g.Expect(former.TargetTimelineID).To(Equal(uint64(5)))
	g.Expect(former.TargetEpoch).To(Equal(uint64(7)))
	g.Expect(former.ForkLSN).To(Equal(uint64(12)))
	g.Expect(former.FormerLastLSN).To(Equal(uint64(13)))
	g.Expect(former.ObservedLSN).To(Equal(uint64(13)))
	g.Expect(former.RetainedFromLSN).To(Equal(uint64(8)))
	g.Expect(former.DataLossDiscarded).To(BeTrue())

	result.Action = "reseed"
	result.Reason = "parent_timeline_wal_expired"
	applyHARejoinJobResult(former, result)
	g.Expect(former.Action).To(Equal(string(haActionReseedFormerPrimary)))
	g.Expect(former.RewindPossible).To(BeFalse())
	g.Expect(former.ReseedRequired).To(BeTrue())
	g.Expect(former.Diverged).To(BeTrue())
	g.Expect(former.Reason).To(Equal("parent_timeline_wal_expired"))

	reseedExecuted, ok := parseHARejoinJobResult(strings.Join([]string{
		"result=rejoin_reseed",
		"assessment.action=reseed",
		"assessment.reason=parent_timeline_wal_expired",
		"assessment.former_node_id=primary-a",
		"assessment.target_timeline_id=5",
		"assessment.target_epoch=7",
		"assessment.parent_cluster_id=100",
		"assessment.parent_shard_id=10",
		"assessment.parent_table_id=20",
		"assessment.parent_timeline_id=4",
		"assessment.parent_epoch=6",
		"assessment.fork_lsn=12",
		"assessment.former_last_lsn=13",
		"assessment.retained_from_lsn=14",
		"assessment.data_loss_discarded=false",
		"reseed.node_id=primary-a",
		"reseed.slot_name=primary-a",
		"reseed.target_timeline_id=5",
		"reseed.target_epoch=7",
		"reseed.fork_lsn=12",
		"reseed.former_last_lsn=13",
		"reseed.reseed_required=true",
		"reseed.base_backup_required=true",
		"",
	}, "\n"))
	g.Expect(ok).To(BeTrue())
	g.Expect(reseedExecuted.Action).To(Equal("reseed"))
	g.Expect(reseedExecuted.ReseedExecuted).To(BeTrue())
	g.Expect(reseedExecuted.ReseedSlotName).To(Equal("primary-a"))
	g.Expect(reseedExecuted.ReseedRequired).To(BeTrue())
	g.Expect(reseedExecuted.ReseedBaseBackupRequired).To(BeTrue())

	adminResult, ok = parseHAAdminActionResultTable(strings.Join([]string{
		"result=rejoin_reseed",
		"assessment.action=reseed",
		"assessment.reason=parent_timeline_wal_expired",
		"assessment.former_node_id=primary-a",
		"assessment.target_timeline_id=5",
		"assessment.target_epoch=7",
		"assessment.parent_cluster_id=100",
		"assessment.parent_shard_id=10",
		"assessment.parent_table_id=20",
		"assessment.parent_timeline_id=4",
		"assessment.parent_epoch=6",
		"assessment.fork_lsn=12",
		"assessment.former_last_lsn=13",
		"assessment.retained_from_lsn=14",
		"reseed.node_id=primary-a",
		"reseed.slot_name=primary-a",
		"reseed.target_timeline_id=5",
		"reseed.target_epoch=7",
		"reseed.fork_lsn=12",
		"reseed.former_last_lsn=13",
		"reseed.reseed_required=true",
		"reseed.base_backup_required=true",
		"",
	}, "\n"))
	g.Expect(ok).To(BeTrue())
	g.Expect(adminResult.RejoinAction).To(Equal("reseed"))
	g.Expect(adminResult.ReseedExecuted).To(BeTrue())
	g.Expect(adminResult.ReseedSlotName).To(Equal("primary-a"))

	_, ok = parseHARejoinJobResult(strings.Join([]string{
		"result=rejoin_reseed",
		"assessment.action=reseed",
		"assessment.reason=parent_timeline_wal_expired",
		"assessment.former_node_id=primary-a",
		"assessment.target_timeline_id=5",
		"assessment.target_epoch=7",
		"assessment.parent_cluster_id=100",
		"assessment.parent_shard_id=10",
		"assessment.parent_table_id=20",
		"assessment.parent_timeline_id=4",
		"assessment.parent_epoch=6",
		"assessment.fork_lsn=12",
		"assessment.former_last_lsn=13",
		"assessment.retained_from_lsn=14",
		"reseed.node_id=primary-a",
		"reseed.slot_name=other",
		"reseed.target_timeline_id=5",
		"reseed.target_epoch=7",
		"reseed.fork_lsn=12",
		"reseed.former_last_lsn=13",
		"reseed.reseed_required=true",
		"reseed.base_backup_required=true",
		"",
	}, "\n"))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHARejoinJobResult(strings.Join([]string{
		"result=rejoin_reseed",
		"assessment.action=reseed",
		"assessment.reason=parent_timeline_wal_expired",
		"assessment.former_node_id=primary-a",
		"assessment.target_timeline_id=5",
		"assessment.target_epoch=7",
		"assessment.parent_cluster_id=100",
		"assessment.parent_shard_id=10",
		"assessment.parent_table_id=20",
		"assessment.parent_timeline_id=4",
		"assessment.parent_epoch=6",
		"assessment.fork_lsn=12",
		"assessment.former_last_lsn=13",
		"assessment.retained_from_lsn=14",
		"reseed.node_id=primary-b",
		"reseed.slot_name=primary-a",
		"reseed.target_timeline_id=5",
		"reseed.target_epoch=7",
		"reseed.fork_lsn=12",
		"reseed.former_last_lsn=13",
		"reseed.reseed_required=true",
		"reseed.base_backup_required=true",
		"",
	}, "\n"))
	g.Expect(ok).To(BeFalse())
}

func TestParseHARejoinAPIResultRecordsRewindExecution(t *testing.T) {
	g := NewWithT(t)

	result, ok := parseHARejoinAPIResult([]byte(`{"schema_version":1,"action":{"action_id":"rejoin_rewind:primary-a","action_kind":"rejoin_rewind","target":"primary-a","state":"applied","node_id":"primary-a"},"assessment":{"action":"rewind","reason":"parent_timeline_retained","former_node_id":"primary-a","target_timeline_id":5,"target_epoch":7,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":4,"parent_epoch":6,"fork_lsn":12,"former_last_lsn":13,"retained_from_lsn":8,"data_loss_discarded":true},"rewind":{"node_id":"primary-a","fork_lsn":12,"previous_last_lsn":13,"current_last_lsn":12,"next_lsn":13,"discarded_lsn_count":1,"target_timeline_id":5,"target_epoch":7,"data_loss_discarded":true}}`))
	g.Expect(ok).To(BeTrue())
	g.Expect(result.SchemaVersion).To(Equal(uint32(1)))
	g.Expect(result.ActionID).To(Equal("rejoin_rewind:primary-a"))
	g.Expect(result.ActionKind).To(Equal("rejoin_rewind"))
	g.Expect(result.ActionTarget).To(Equal("primary-a"))
	g.Expect(result.ActionState).To(Equal("applied"))
	g.Expect(result.Action).To(Equal("rewind"))
	g.Expect(result.ParentClusterID).To(Equal(uint64(100)))
	g.Expect(result.ParentShardID).To(Equal(uint64(10)))
	g.Expect(result.ParentTableID).To(Equal(uint64(20)))
	g.Expect(result.ParentTimelineID).To(Equal(uint64(4)))
	g.Expect(result.ParentEpoch).To(Equal(uint64(6)))
	g.Expect(result.RewindExecuted).To(BeTrue())
	g.Expect(result.RewindPreviousLastLSN).To(Equal(uint64(13)))
	g.Expect(result.RewindCurrentLastLSN).To(Equal(uint64(12)))
	g.Expect(result.RewindNextLSN).To(Equal(uint64(13)))
	g.Expect(result.RewindDiscardedLSNCount).To(Equal(uint64(1)))
	g.Expect(result.DataLossDiscarded).To(BeTrue())

	status := haRejoinAdminActionResult(result)
	g.Expect(status.SchemaVersion).To(Equal(uint32(1)))
	g.Expect(status.ActionID).To(Equal("rejoin_rewind:primary-a"))
	g.Expect(status.ActionKind).To(Equal("rejoin_rewind"))
	g.Expect(status.ActionTarget).To(Equal("primary-a"))
	g.Expect(status.ActionState).To(Equal("applied"))
	g.Expect(status.RewindExecuted).To(BeTrue())
	g.Expect(status.RewindPreviousLastLSN).To(Equal(uint64(13)))
	g.Expect(status.RewindCurrentLastLSN).To(Equal(uint64(12)))
	g.Expect(status.RewindNextLSN).To(Equal(uint64(13)))
	g.Expect(status.RewindDiscardedLSNCount).To(Equal(uint64(1)))

	roundTripped, ok := haRejoinJobResultFromAdminResult(status)
	g.Expect(ok).To(BeTrue())
	g.Expect(roundTripped.SchemaVersion).To(Equal(uint32(1)))
	g.Expect(roundTripped.ActionID).To(Equal("rejoin_rewind:primary-a"))
	g.Expect(roundTripped.RewindExecuted).To(BeTrue())
	g.Expect(roundTripped.RewindDiscardedLSNCount).To(Equal(uint64(1)))

	_, ok = parseHARejoinAPIResult([]byte(`{"schema_version":1,"action":{"action_id":"rejoin_assess:primary-a","action_kind":"rejoin_assess","target":"primary-a","state":"assessed","node_id":"primary-a"},"assessment":{"action":"promote","reason":"parent_timeline_retained","former_node_id":"primary-a","target_timeline_id":5,"target_epoch":7,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":4,"parent_epoch":6,"fork_lsn":12,"former_last_lsn":13,"retained_from_lsn":8,"data_loss_discarded":false}}`))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHARejoinAPIResult([]byte(`{"schema_version":1,"action":{"action_id":"rejoin_assess:primary-a","action_kind":"rejoin_assess","target":"primary-a","state":"assessed","node_id":"primary-a"},"assessment":{"action":"rewind","reason":"operator_guess","former_node_id":"primary-a","target_timeline_id":5,"target_epoch":7,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":4,"parent_epoch":6,"fork_lsn":12,"former_last_lsn":13,"retained_from_lsn":8,"data_loss_discarded":false}}`))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHARejoinAPIResult([]byte(`{"schema_version":1,"assessment":{"action":"rewind","reason":"parent_timeline_retained","former_node_id":"primary-a","target_timeline_id":5,"target_epoch":7,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":4,"parent_epoch":6,"fork_lsn":12,"former_last_lsn":13,"retained_from_lsn":8},"rewind":{"node_id":"primary-a","fork_lsn":11,"previous_last_lsn":13,"current_last_lsn":11,"next_lsn":12,"discarded_lsn_count":2,"target_timeline_id":5,"target_epoch":7,"data_loss_discarded":false}}`))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHARejoinAPIResult([]byte(`{"schema_version":1,"action":{"action_id":"rejoin_rewind:primary-a","action_kind":"rejoin_rewind","target":"primary-a","state":"applied","node_id":"primary-a"},"assessment":{"action":"rewind","reason":"parent_timeline_retained","former_node_id":"primary-a","target_timeline_id":5,"target_epoch":7,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":4,"parent_epoch":6,"fork_lsn":12,"former_last_lsn":13,"retained_from_lsn":8},"rewind":{"node_id":"primary-a","fork_lsn":12,"previous_last_lsn":13,"current_last_lsn":12,"next_lsn":13,"discarded_lsn_count":1,"target_timeline_id":5,"target_epoch":7,"data_loss_discarded":false}}`))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHARejoinAPIResult([]byte(`{"schema_version":1,"action":{"action_id":"rejoin_rewind:primary-a","action_kind":"rejoin_rewind","target":"primary-a","state":"applied","node_id":"primary-a"},"assessment":{"action":"rewind","reason":"parent_timeline_retained","former_node_id":"primary-a","target_timeline_id":5,"target_epoch":7,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":4,"parent_epoch":6,"fork_lsn":12,"former_last_lsn":13,"retained_from_lsn":8,"data_loss_discarded":false},"rewind":{"node_id":"primary-a","fork_lsn":12,"previous_last_lsn":13,"current_last_lsn":12,"next_lsn":13,"discarded_lsn_count":1,"target_timeline_id":5,"target_epoch":7}}`))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHARejoinAPIResult([]byte(`{"schema_version":1,"action":{"action_id":"rejoin_rewind:primary-a","action_kind":"rejoin_rewind","target":"primary-a"},"assessment":{"action":"rewind","reason":"parent_timeline_retained","former_node_id":"primary-a","target_timeline_id":5,"target_epoch":7,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":4,"parent_epoch":6,"fork_lsn":12,"former_last_lsn":13,"retained_from_lsn":8},"rewind":{"node_id":"primary-a","fork_lsn":12,"previous_last_lsn":13,"current_last_lsn":12,"next_lsn":13,"discarded_lsn_count":1,"target_timeline_id":5,"target_epoch":7,"data_loss_discarded":false}}`))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHARejoinAPIResult([]byte(`{"schema_version":1,"assessment":{"action":"reseed","reason":"parent_timeline_wal_expired","former_node_id":"primary-a","target_timeline_id":5,"target_epoch":7,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":4,"parent_epoch":6,"fork_lsn":12,"former_last_lsn":13,"retained_from_lsn":14},"rewind":{"node_id":"primary-a","fork_lsn":12,"previous_last_lsn":13,"current_last_lsn":12,"next_lsn":13,"discarded_lsn_count":1,"target_timeline_id":5,"target_epoch":7,"data_loss_discarded":true}}`))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHARejoinAPIResult([]byte(`{"schema_version":1,"assessment":{"action":"rewind","reason":"parent_timeline_retained","former_node_id":"primary-a","target_timeline_id":5,"target_epoch":7,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":4,"parent_epoch":6,"fork_lsn":12,"former_last_lsn":13,"retained_from_lsn":8},"rewind":{"node_id":"primary-b","fork_lsn":12,"previous_last_lsn":13,"current_last_lsn":12,"next_lsn":13,"discarded_lsn_count":1,"target_timeline_id":5,"target_epoch":7,"data_loss_discarded":false}}`))
	g.Expect(ok).To(BeFalse())
}

func TestParseHARejoinAPIResultRecordsReseedExecution(t *testing.T) {
	g := NewWithT(t)

	result, ok := parseHARejoinAPIResult([]byte(`{"schema_version":1,"action":{"action_id":"rejoin_reseed:primary-a","action_kind":"rejoin_reseed","target":"primary-a","state":"applied","node_id":"primary-a"},"assessment":{"action":"reseed","reason":"parent_timeline_wal_expired","former_node_id":"primary-a","target_timeline_id":5,"target_epoch":7,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":4,"parent_epoch":6,"fork_lsn":12,"former_last_lsn":13,"retained_from_lsn":14,"data_loss_discarded":false},"reseed":{"node_id":"primary-a","slot_name":"primary-a","target_timeline_id":5,"target_epoch":7,"fork_lsn":12,"former_last_lsn":13,"reseed_required":true,"base_backup_required":true}}`))
	g.Expect(ok).To(BeTrue())
	g.Expect(result.SchemaVersion).To(Equal(uint32(1)))
	g.Expect(result.ActionID).To(Equal("rejoin_reseed:primary-a"))
	g.Expect(result.ActionKind).To(Equal("rejoin_reseed"))
	g.Expect(result.ActionTarget).To(Equal("primary-a"))
	g.Expect(result.ActionState).To(Equal("applied"))
	g.Expect(result.Action).To(Equal("reseed"))
	g.Expect(result.ReseedExecuted).To(BeTrue())
	g.Expect(result.ReseedSlotName).To(Equal("primary-a"))
	g.Expect(result.ReseedRequired).To(BeTrue())
	g.Expect(result.ReseedBaseBackupRequired).To(BeTrue())

	status := haRejoinAdminActionResult(result)
	g.Expect(status.SchemaVersion).To(Equal(uint32(1)))
	g.Expect(status.ActionID).To(Equal("rejoin_reseed:primary-a"))
	g.Expect(status.ReseedExecuted).To(BeTrue())
	g.Expect(status.ReseedSlotName).To(Equal("primary-a"))
	g.Expect(status.ReseedRequired).To(BeTrue())
	g.Expect(status.ReseedBaseBackupRequired).To(BeTrue())

	roundTripped, ok := haRejoinJobResultFromAdminResult(status)
	g.Expect(ok).To(BeTrue())
	g.Expect(roundTripped.ReseedExecuted).To(BeTrue())
	g.Expect(roundTripped.ReseedSlotName).To(Equal("primary-a"))

	_, ok = parseHARejoinAPIResult([]byte(`{"schema_version":1,"assessment":{"action":"reseed","reason":"parent_timeline_wal_expired","former_node_id":"primary-a","target_timeline_id":5,"target_epoch":7,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":4,"parent_epoch":6,"fork_lsn":12,"former_last_lsn":13,"retained_from_lsn":14},"reseed":{"node_id":"primary-a","slot_name":"other","target_timeline_id":5,"target_epoch":7,"fork_lsn":12,"former_last_lsn":13,"reseed_required":true,"base_backup_required":true}}`))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHARejoinAPIResult([]byte(`{"schema_version":1,"action":{"action_id":"rejoin_reseed:primary-a","action_kind":"rejoin_reseed","target":"primary-a","state":"applied","node_id":"primary-a"},"assessment":{"action":"reseed","reason":"parent_timeline_wal_expired","former_node_id":"primary-a","target_timeline_id":5,"target_epoch":7,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":4,"parent_epoch":6,"fork_lsn":12,"former_last_lsn":13,"retained_from_lsn":14,"data_loss_discarded":false},"reseed":{"node_id":"primary-a","slot_name":"primary-a","target_timeline_id":5,"target_epoch":7,"fork_lsn":12,"former_last_lsn":13,"reseed_required":true}}`))
	g.Expect(ok).To(BeFalse())

	_, ok = parseHARejoinAPIResult([]byte(`{"schema_version":1,"assessment":{"action":"reseed","reason":"parent_timeline_wal_expired","former_node_id":"primary-a","target_timeline_id":5,"target_epoch":7,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":4,"parent_epoch":6,"fork_lsn":12,"former_last_lsn":13,"retained_from_lsn":14},"reseed":{"node_id":"primary-b","slot_name":"primary-a","target_timeline_id":5,"target_epoch":7,"fork_lsn":12,"former_last_lsn":13,"reseed_required":true,"base_backup_required":true}}`))
	g.Expect(ok).To(BeFalse())
}

func TestHARejoinAssessAdminResultAcceptsDispositionActions(t *testing.T) {
	g := NewWithT(t)

	action := antflyv1.HAPlannedActionStatus{
		Kind:        string(haActionDemoteFormerPrimary),
		StandbyName: "primary-a",
		TargetLSN:   12,
		ObservedLSN: 13,
	}
	for _, disposition := range []string{"reject_unfenced", "already_current", "rewind", "reseed"} {
		result := &antflyv1.HAAdminActionResultStatus{
			RejoinAction:     disposition,
			FormerNodeID:     "primary-a",
			TargetTimelineID: 5,
			TargetEpoch:      7,
			ForkLSN:          12,
			FormerLastLSN:    13,
		}
		g.Expect(haActionHasRequiredAdminResult(antflyv1.HAPlannedActionStatus{
			Kind:        action.Kind,
			StandbyName: action.StandbyName,
			TargetLSN:   action.TargetLSN,
			ObservedLSN: action.ObservedLSN,
			AdminResult: result,
		})).To(BeTrue(), "disposition %s", disposition)
	}

	g.Expect(haActionHasRequiredAdminResult(antflyv1.HAPlannedActionStatus{
		Kind:        action.Kind,
		StandbyName: action.StandbyName,
		TargetLSN:   action.TargetLSN,
		ObservedLSN: action.ObservedLSN,
		AdminResult: &antflyv1.HAAdminActionResultStatus{
			RejoinAction:     "unknown",
			FormerNodeID:     "primary-a",
			TargetTimelineID: 5,
			TargetEpoch:      7,
			ForkLSN:          12,
			FormerLastLSN:    13,
		},
	})).To(BeFalse())

	g.Expect(haActionHasRequiredAdminResult(antflyv1.HAPlannedActionStatus{
		Kind:        string(haActionRewindFormerPrimary),
		StandbyName: "primary-a",
		TargetLSN:   12,
		ObservedLSN: 13,
		AdminResult: &antflyv1.HAAdminActionResultStatus{
			RejoinAction:     "rewind",
			FormerNodeID:     "primary-a",
			TargetTimelineID: 5,
			TargetEpoch:      7,
			ForkLSN:          12,
			FormerLastLSN:    13,
		},
	})).To(BeFalse())
}

func TestUpdateHAFormerPrimaryRequiresPriorHAAdminActions(t *testing.T) {
	g := NewWithT(t)

	cluster := &antflyv1.AntflyCluster{
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				LastPromotion: &antflyv1.HAPromotionStatus{
					OldPrimaryID:      "primary-a",
					PromotedStandbyID: "standby-a",
					ParentTimelineID:  4,
					ParentEpoch:       6,
					NewTimelineID:     5,
					NewEpoch:          7,
					SwitchLSN:         12,
					RequiredLSN:       12,
					ObservedLSN:       12,
					FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
					FenceGeneration:   3,
					FenceToken:        "ha-fence-token",
				},
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:          string(haActionPromoteStandby),
					StandbyName:   "standby-a",
					AdminCommand:  []string{"promote", "--current-fence"},
					AdminJobName:  "promote-job",
					AdminJobPhase: haAdminJobPhasePending,
				}, {
					Kind:            string(haActionDemoteFormerPrimary),
					StandbyName:     "primary-a",
					TargetLSN:       12,
					ObservedLSN:     11,
					RetainedFromLSN: 8,
					FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
					FenceHolder:     "standby-a",
					FenceGeneration: 3,
					Reason:          "PromotionPlanned",
					AdminCommand:    []string{"rejoin", "assess"},
					AdminJobName:    "demote-job",
					AdminJobPhase:   haAdminJobPhaseSucceeded,
				}},
			},
		},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAFormerPrimaryFromAdminJobs(context.Background(), cluster)
	g.Expect(cluster.Status.HAStatus.FormerPrimary).To(BeNil())

	cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase = haAdminJobPhaseSucceeded
	cluster.Status.HAStatus.PlannedActions[0].AdminResult = &antflyv1.HAAdminActionResultStatus{
		ActionID:            "fence_acquire:standby-a",
		ActionKind:          "fence_acquire",
		ActionTarget:        "standby-a",
		ActionState:         "applied",
		ActionNodeID:        "standby-a",
		FenceGeneration:     3,
		FenceToken:          "ha-fence-token",
		FencePromotedNodeID: "standby-a",
	}
	reconciler.updateHAFormerPrimaryFromAdminJobs(context.Background(), cluster)
	g.Expect(cluster.Status.HAStatus.FormerPrimary).To(BeNil())

	cluster.Status.HAStatus.PlannedActions[0].AdminResult = haPromotionAdminResult(3, "ha-fence-token", "standby-a")
	cluster.Status.HAStatus.PlannedActions[1].AdminResult = &antflyv1.HAAdminActionResultStatus{
		ActionID:         "rejoin_assess:primary-a",
		ActionKind:       "rejoin_assess",
		ActionTarget:     "primary-a",
		ActionState:      "assessed",
		ActionNodeID:     "primary-a",
		RejoinAction:     "reject_unfenced",
		RejoinReason:     "no_fence",
		FormerNodeID:     "primary-a",
		TargetTimelineID: 5,
		TargetEpoch:      7,
		ForkLSN:          12,
		FormerLastLSN:    11,
		RetainedFromLSN:  8,
	}
	reconciler.updateHAFormerPrimaryFromAdminJobs(context.Background(), cluster)
	g.Expect(cluster.Status.HAStatus.FormerPrimary).NotTo(BeNil())
	g.Expect(cluster.Status.HAStatus.FormerPrimary.NodeID).To(Equal("primary-a"))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.ParentTimelineID).To(Equal(uint64(4)))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.NewTimelineID).To(Equal(uint64(5)))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.SwitchLSN).To(Equal(uint64(12)))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.ObservedLSN).To(Equal(uint64(11)))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.RetainedFromLSN).To(Equal(uint64(8)))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.FenceAuthority).To(Equal(antflyv1.HAFencingAuthorityKubernetesLease))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.FenceHolder).To(Equal("standby-a"))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.FenceGeneration).To(Equal(uint64(3)))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.Action).To(Equal(string(haActionDemoteFormerPrimary)))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.Reason).To(Equal("no_fence"))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.AssessedAction).To(Equal("reject_unfenced"))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.AssessedReason).To(Equal("no_fence"))
}

func TestUpdateHAFormerPrimaryHonorsExplicitDependencyAfterUnrelatedFailure(t *testing.T) {
	g := NewWithT(t)

	cluster := &antflyv1.AntflyCluster{
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				LastPromotion: &antflyv1.HAPromotionStatus{
					OldPrimaryID:      "primary-a",
					PromotedStandbyID: "standby-a",
					ParentTimelineID:  4,
					ParentEpoch:       6,
					NewTimelineID:     5,
					NewEpoch:          7,
					SwitchLSN:         12,
					RequiredLSN:       12,
					ObservedLSN:       12,
					FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
					FenceGeneration:   3,
					FenceToken:        "ha-fence-token",
				},
				PlannedActions: []antflyv1.HAPlannedActionStatus{{
					Kind:          string(haActionPauseSlot),
					AdminCommand:  []string{"slot", "pause"},
					AdminJobPhase: haAdminJobPhaseFailed,
				}, {
					Kind:          string(haActionPromoteStandby),
					StandbyName:   "standby-a",
					AdminCommand:  []string{"promote", "--current-fence"},
					AdminJobPhase: haAdminJobPhaseSucceeded,
					AdminResult:   haPromotionAdminResult(3, "ha-fence-token", "standby-a"),
				}, {
					Kind:          string(haActionDemoteFormerPrimary),
					DependsOn:     string(haActionPromoteStandby),
					StandbyName:   "primary-a",
					TargetLSN:     12,
					AdminCommand:  []string{"rejoin", "assess"},
					AdminJobName:  "demote-job",
					AdminJobPhase: haAdminJobPhaseSucceeded,
					AdminResult: &antflyv1.HAAdminActionResultStatus{
						ActionID:         "rejoin_assess:primary-a",
						ActionKind:       "rejoin_assess",
						ActionTarget:     "primary-a",
						ActionState:      "assessed",
						ActionNodeID:     "primary-a",
						RejoinAction:     "reject_unfenced",
						RejoinReason:     "no_fence",
						FormerNodeID:     "primary-a",
						TargetTimelineID: 5,
						TargetEpoch:      7,
						ForkLSN:          12,
					},
				}},
			},
		},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateHAFormerPrimaryFromAdminJobs(context.Background(), cluster)
	g.Expect(cluster.Status.HAStatus.FormerPrimary).NotTo(BeNil())
	g.Expect(cluster.Status.HAStatus.FormerPrimary.NodeID).To(Equal("primary-a"))
}

func TestObserveHAPrimaryAdminStatus(t *testing.T) {
	g := NewWithT(t)
	t.Setenv("CUSTOM_HA_ADMIN_TOKEN", "operator-token")

	var observedTypedURL string
	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Method).To(Equal(http.MethodGet))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/primary/status"))
			g.Expect(req.Header.Get("Accept")).To(Equal("application/json"))
			g.Expect(req.Header.Get("Authorization")).To(Equal("Bearer operator-token"))
			observedTypedURL = req.URL.String()
			query := req.URL.Query()
			g.Expect(query.Get("max_lag_lsn")).To(Equal("50"))
			g.Expect(query.Get("max_retained_bytes")).To(Equal("4096"))
			g.Expect(query.Get("max_retained_age_ns")).To(Equal("1000000"))
			g.Expect(query.Get("sync_mode")).To(Equal("remote-apply"))
			g.Expect(query.Get("sync_selection")).To(Equal("first"))
			g.Expect(query.Get("sync_required")).To(Equal("1"))
			g.Expect(query["sync_standby"]).To(Equal([]string{"standby-a"}))
			g.Expect(query.Get("sync_failure")).To(Equal("fail-closed"))
			body := `{"schema_version":1,"snapshot":{"role":"primary","node_id":"primary-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"current_lsn":12,"slots":[{"name":"standby-a","timeline_id":4,"active":true,"reseed_required":false,"restart_lsn":7,"received_lsn":12,"applied_lsn":11,"safe_read_lsn":11,"write_lag_lsn":0,"apply_lag_lsn":1,"safe_read_lag_lsn":1,"retention_lag_lsn":5,"status":"healthy","last_error":null}],"retention":{"primary_lsn":12,"oldest_restart_lsn":7,"retained_lsn_count":5,"retained_byte_count":512,"retained_age_ns":400,"active_slots":1,"reseed_recommended":0},"durability":{"status":"would_block","mode":"remote_apply","selection":"first","target_lsn":12,"progress_lsn":11,"missing_lsn_count":1,"satisfied_count":0,"required_count":1,"candidate_count":1}}}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:  "http://primary-ha.default.svc:8081",
					TokenEnvVar: "CUSTOM_HA_ADMIN_TOKEN",
				},
				Retention: &antflyv1.HARetentionPolicy{MaxLagLSN: 50, MaxRetainedBytes: 4096, MaxRetainedAgeNS: 1000000},
				SyncPolicy: &antflyv1.HASyncPolicy{
					Mode:          antflyv1.HADurabilityModeRemoteApply,
					Selection:     antflyv1.HAStandbySelectionFirst,
					Required:      1,
					StandbyNames:  []string{"standby-a"},
					FailurePolicy: antflyv1.HAFailurePolicyFailClosed,
				},
			},
		},
	}

	g.Expect(reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster)).To(Succeed())
	g.Expect(observedTypedURL).To(ContainSubstring("/admin/v1/ha/primary/status?"))
	g.Expect(cluster.Status.HAStatus.PrimaryLSN).To(Equal(uint64(12)))
	g.Expect(cluster.Status.HAStatus.PrimaryAdminReachable).To(BeTrue())
	g.Expect(cluster.Status.HAStatus.PrimaryAdminLastError).To(BeEmpty())
	g.Expect(cluster.Status.HAStatus.Retention.OldestRestartLSN).To(Equal(uint64(7)))
	g.Expect(cluster.Status.HAStatus.Retention.RetainedLSNCount).To(Equal(uint64(5)))
	g.Expect(cluster.Status.HAStatus.Retention.RetainedByteCount).To(Equal(uint64(512)))
	g.Expect(cluster.Status.HAStatus.Retention.RetainedAgeNS).To(Equal(uint64(400)))
	g.Expect(cluster.Status.HAStatus.Retention.ActiveSlots).To(Equal(int32(1)))
	g.Expect(cluster.Status.HAStatus.Standbys).To(HaveLen(1))
	standby := cluster.Status.HAStatus.Standbys[0]
	g.Expect(standby.Name).To(Equal("standby-a"))
	g.Expect(standby.TimelineID).To(Equal(uint64(4)))
	g.Expect(standby.ReceivedLSN).To(Equal(uint64(12)))
	g.Expect(standby.AppliedLSN).To(Equal(uint64(11)))
	g.Expect(standby.ApplyLagLSN).To(Equal(uint64(1)))
	g.Expect(standby.Status).To(Equal("healthy"))
	g.Expect(cluster.Status.HAStatus.Sync.Mode).To(Equal(antflyv1.HADurabilityModeRemoteApply))
	g.Expect(cluster.Status.HAStatus.Sync.Selection).To(Equal(antflyv1.HAStandbySelectionFirst))
	g.Expect(cluster.Status.HAStatus.Sync.Required).To(Equal(int32(1)))
	g.Expect(cluster.Status.HAStatus.Sync.Satisfied).To(Equal(int32(0)))
	g.Expect(cluster.Status.HAStatus.Sync.Candidates).To(Equal(int32(1)))
	g.Expect(cluster.Status.HAStatus.Sync.Degraded).To(BeTrue())
	g.Expect(cluster.Status.HAStatus.Sync.Action).To(Equal("BlockWrites"))

	reconciler.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("primary admin refused connection")
	})}
	g.Expect(reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster)).NotTo(Succeed())
	g.Expect(cluster.Status.HAStatus.PrimaryAdminReachable).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.PrimaryAdminLastError).To(ContainSubstring("primary admin refused connection"))
}

func TestObserveHAPrimaryAdminStatusRejectsMissingSDKFieldEvidence(t *testing.T) {
	g := NewWithT(t)

	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Method).To(Equal(http.MethodGet))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/primary/status"))
			body := `{"schema_version":1,"snapshot":{"role":"primary","node_id":"primary-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"current_lsn":12,"slots":[],"retention":{"primary_lsn":12,"oldest_restart_lsn":12,"retained_lsn_count":0,"retained_byte_count":0,"active_slots":0,"reseed_recommended":0}}}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode:  antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"},
			},
		},
	}

	err := reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster)
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("missing primary status retention field evidence"))
	g.Expect(cluster.Status.HAStatus.PrimaryAdminReachable).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.PrimaryAdminLastError).To(ContainSubstring("missing primary status retention field evidence"))
	g.Expect(cluster.Status.HAStatus.PrimaryLSN).To(Equal(uint64(0)))
}

func TestObserveHAPrimaryAdminStatusOmitsSyncRequiredForAllPolicy(t *testing.T) {
	g := NewWithT(t)

	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Method).To(Equal(http.MethodGet))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/primary/status"))
			query := req.URL.Query()
			g.Expect(query.Get("sync_mode")).To(Equal("remote-apply"))
			g.Expect(query.Get("sync_selection")).To(Equal("all"))
			g.Expect(query).NotTo(HaveKey("sync_required"))
			g.Expect(query["sync_standby"]).To(Equal([]string{"standby-a", "standby-b"}))
			body := `{"schema_version":1,"snapshot":{"role":"primary","node_id":"primary-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"current_lsn":12,"slots":[{"name":"standby-a","timeline_id":4,"active":true,"reseed_required":false,"restart_lsn":7,"received_lsn":12,"applied_lsn":12,"safe_read_lsn":12,"write_lag_lsn":0,"apply_lag_lsn":0,"safe_read_lag_lsn":0,"retention_lag_lsn":5,"status":"healthy","last_error":null},{"name":"standby-b","timeline_id":4,"active":true,"reseed_required":false,"restart_lsn":7,"received_lsn":12,"applied_lsn":11,"safe_read_lsn":11,"write_lag_lsn":0,"apply_lag_lsn":1,"safe_read_lag_lsn":1,"retention_lag_lsn":5,"status":"healthy","last_error":null}],"retention":{"primary_lsn":12,"oldest_restart_lsn":7,"retained_lsn_count":5,"retained_byte_count":512,"retained_age_ns":400,"active_slots":2,"reseed_recommended":0},"durability":{"status":"would_block","mode":"remote_apply","selection":"all","target_lsn":12,"progress_lsn":11,"missing_lsn_count":1,"satisfied_count":1,"required_count":2,"candidate_count":2}}}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode:  antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"},
				SyncPolicy: &antflyv1.HASyncPolicy{
					Mode:          antflyv1.HADurabilityModeRemoteApply,
					Selection:     antflyv1.HAStandbySelectionAll,
					Required:      2,
					StandbyNames:  []string{"standby-a", "standby-b"},
					FailurePolicy: antflyv1.HAFailurePolicyDegradeToAsync,
				},
			},
		},
	}

	g.Expect(reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PrimaryAdminReachable).To(BeTrue())
	g.Expect(cluster.Status.HAStatus.Sync.Selection).To(Equal(antflyv1.HAStandbySelectionAll))
	g.Expect(cluster.Status.HAStatus.Sync.Required).To(Equal(int32(2)))
}

func TestObserveHAPrimaryAdminStatusTargetsPromotedPrimaryAdminURL(t *testing.T) {
	g := NewWithT(t)

	var observedHost string
	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Method).To(Equal(http.MethodGet))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/primary/status"))
			observedHost = req.URL.Host
			body := `{"schema_version":1,"snapshot":{"role":"primary","node_id":"standby-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":5,"epoch":7},"current_lsn":21,"slots":[],"retention":{"primary_lsn":21,"oldest_restart_lsn":21,"retained_lsn_count":0,"retained_byte_count":0,"retained_age_ns":0,"active_slots":0,"reseed_recommended":0}}}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode:  antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://old-primary-ha.default.svc:8081"},
				Standbys: []antflyv1.HAStandbySpec{{
					Name:     "standby-a",
					AdminURL: "http://standby-a-ha.default.svc:8081",
				}},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				LastPromotion: haCompletePromotionReceipt("primary-a", "standby-a"),
			},
		},
	}

	g.Expect(reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster)).To(Succeed())
	g.Expect(observedHost).To(Equal("standby-a-ha.default.svc:8081"))
	g.Expect(cluster.Status.HAStatus.PrimaryLSN).To(Equal(uint64(21)))
	g.Expect(cluster.Status.HAStatus.PrimaryAdminReachable).To(BeTrue())
	g.Expect(cluster.Status.HAStatus.PrimaryAdminLastError).To(BeEmpty())
}

func TestObserveHAPrimaryAdminStatusIgnoresIncompletePromotionForTargeting(t *testing.T) {
	g := NewWithT(t)

	var observedHost string
	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Method).To(Equal(http.MethodGet))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/primary/status"))
			observedHost = req.URL.Host
			body := `{"schema_version":1,"snapshot":{"role":"primary","node_id":"primary-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":5,"epoch":7},"current_lsn":21,"slots":[],"retention":{"primary_lsn":21,"oldest_restart_lsn":21,"retained_lsn_count":0,"retained_byte_count":0,"retained_age_ns":0,"active_slots":0,"reseed_recommended":0}}}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode:  antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://old-primary-ha.default.svc:8081"},
				Standbys: []antflyv1.HAStandbySpec{{
					Name:     "standby-a",
					AdminURL: "http://standby-a-ha.default.svc:8081",
				}},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				LastPromotion: &antflyv1.HAPromotionStatus{PromotedStandbyID: "standby-a"},
			},
		},
	}

	g.Expect(reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster)).To(Succeed())
	g.Expect(observedHost).To(Equal("old-primary-ha.default.svc:8081"))
	g.Expect(cluster.Status.HAStatus.PrimaryAdminReachable).To(BeTrue())
}

func TestObserveHAPrimaryAdminStatusDoesNotFallbackWhenPromotedPrimaryURLMissing(t *testing.T) {
	g := NewWithT(t)

	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			t.Fatalf("promoted primary with no node-local admin URL must not fall back to stale primary URL: %s", req.URL.String())
			return nil, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode:  antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://old-primary-ha.default.svc:8081"},
				Standbys: []antflyv1.HAStandbySpec{{
					Name: "standby-a",
				}},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				PrimaryAdminReachable: true,
				LastPromotion:         haCompletePromotionReceipt("primary-a", "standby-a"),
			},
		},
	}

	g.Expect(reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PrimaryAdminReachable).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.PrimaryAdminLastError).To(ContainSubstring("promoted primary standby-a admin URL is not configured"))
}

func TestObserveHAPrimaryAdminStatusDoesNotFallbackToCommandEndpoint(t *testing.T) {
	g := NewWithT(t)

	var requestCount int
	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			requestCount++
			if req.Method == http.MethodGet {
				g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/primary/status"))
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(strings.NewReader("not found")),
				}, nil
			}
			t.Fatalf("operator status observation must use typed /admin/v1/ha endpoints only, got %s %s", req.Method, req.URL.String())
			return nil, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode:  antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"},
			},
		},
	}

	g.Expect(reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster)).NotTo(Succeed())
	g.Expect(requestCount).To(Equal(1))
	g.Expect(cluster.Status.HAStatus.PrimaryAdminReachable).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.PrimaryAdminStatusCode).To(Equal(http.StatusNotFound))
	g.Expect(cluster.Status.HAStatus.PrimaryAdminLastError).To(ContainSubstring("get HA primary status returned status 404"))
}

func TestObserveHAPrimaryAdminStatusRecordsUnauthorizedStatus(t *testing.T) {
	g := NewWithT(t)

	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Method).To(Equal(http.MethodGet))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/primary/status"))
			return &http.Response{
				StatusCode: http.StatusUnauthorized,
				Body:       io.NopCloser(strings.NewReader("missing bearer token")),
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode:  antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"},
			},
		},
	}

	err := reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster)
	g.Expect(err).To(HaveOccurred())
	g.Expect(cluster.Status.HAStatus.PrimaryAdminReachable).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.PrimaryAdminStatusCode).To(Equal(http.StatusUnauthorized))
	g.Expect(cluster.Status.HAStatus.PrimaryAdminLastError).To(ContainSubstring("status 401"))

	reconciler.updateHAStatusAndConditions(cluster)
	degraded := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	g.Expect(degraded).NotTo(BeNil())
	g.Expect(degraded.Status).To(Equal(metav1.ConditionTrue))
	g.Expect(degraded.Reason).To(Equal(antflyv1.ReasonHAAdminUnauthorized))
	g.Expect(degraded.Message).To(ContainSubstring("status 401"))
}

func TestObserveHAPrimaryAdminStatusRejectsMismatchedIdentityScope(t *testing.T) {
	g := NewWithT(t)

	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Method).To(Equal(http.MethodGet))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/primary/status"))
			body := `{"schema_version":1,"snapshot":{"role":"primary","node_id":"primary-a","identity":{"cluster_id":99,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"current_lsn":12,"slots":[],"retention":{"primary_lsn":12,"oldest_restart_lsn":12,"retained_lsn_count":0,"retained_byte_count":0,"retained_age_ns":0,"active_slots":0,"reseed_recommended":0}}}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode:  antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"},
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID:        1,
					ShardID:          2,
					TableID:          3,
					TimelineID:       4,
					Epoch:            5,
					CurrentPrimaryID: "primary-a",
				},
			},
		},
	}

	err := reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster)
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("identity scope mismatch"))
	g.Expect(cluster.Status.HAStatus.PrimaryAdminReachable).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.PrimaryLSN).To(Equal(uint64(0)))
}

func TestObserveHAPrimaryAdminStatusRejectsStaleTimeline(t *testing.T) {
	g := NewWithT(t)

	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Method).To(Equal(http.MethodGet))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/primary/status"))
			body := `{"schema_version":1,"snapshot":{"role":"primary","node_id":"primary-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":3,"epoch":5},"current_lsn":12,"slots":[],"retention":{"primary_lsn":12,"oldest_restart_lsn":12,"retained_lsn_count":0,"retained_byte_count":0,"retained_age_ns":0,"active_slots":0,"reseed_recommended":0}}}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode:  antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"},
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID:        1,
					ShardID:          2,
					TableID:          3,
					TimelineID:       4,
					Epoch:            5,
					CurrentPrimaryID: "primary-a",
				},
			},
		},
	}

	err := reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster)
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("identity timeline mismatch"))
	g.Expect(cluster.Status.HAStatus.PrimaryAdminReachable).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.PrimaryLSN).To(Equal(uint64(0)))
}

func TestObserveHAPrimaryAdminStatusRejectsMismatchedNodeID(t *testing.T) {
	g := NewWithT(t)

	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Method).To(Equal(http.MethodGet))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/primary/status"))
			body := `{"schema_version":1,"snapshot":{"role":"primary","node_id":"primary-b","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"current_lsn":12,"slots":[],"retention":{"primary_lsn":12,"oldest_restart_lsn":12,"retained_lsn_count":0,"retained_byte_count":0,"retained_age_ns":0,"active_slots":0,"reseed_recommended":0}}}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode:  antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"},
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID:        1,
					ShardID:          2,
					TableID:          3,
					TimelineID:       4,
					Epoch:            5,
					CurrentPrimaryID: "primary-a",
				},
			},
		},
	}

	err := reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster)
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("primary node_id mismatch"))
	g.Expect(cluster.Status.HAStatus.PrimaryAdminReachable).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.PrimaryLSN).To(Equal(uint64(0)))
}

func TestObserveHAPrimaryAdminStatusAcceptsPromotedTimeline(t *testing.T) {
	g := NewWithT(t)

	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Method).To(Equal(http.MethodGet))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/primary/status"))
			g.Expect(req.URL.Host).To(Equal("standby-a-ha.default.svc:8081"))
			body := `{"schema_version":1,"snapshot":{"role":"primary","node_id":"standby-a","identity":{"cluster_id":100,"shard_id":10,"table_id":20,"timeline_id":5,"epoch":7},"current_lsn":21,"slots":[],"retention":{"primary_lsn":21,"oldest_restart_lsn":21,"retained_lsn_count":0,"retained_byte_count":0,"retained_age_ns":0,"active_slots":0,"reseed_recommended":0}}}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode:  antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://old-primary-ha.default.svc:8081"},
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID:        100,
					ShardID:          10,
					TableID:          20,
					TimelineID:       4,
					Epoch:            6,
					CurrentPrimaryID: "primary-a",
				},
				Standbys: []antflyv1.HAStandbySpec{{
					Name:     "standby-a",
					AdminURL: "http://standby-a-ha.default.svc:8081",
				}},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				LastPromotion: haCompletePromotionReceipt("primary-a", "standby-a"),
			},
		},
	}

	g.Expect(reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PrimaryAdminReachable).To(BeTrue())
	g.Expect(cluster.Status.HAStatus.PrimaryLSN).To(Equal(uint64(21)))
}

func TestObserveHAPrimaryAdminStatusRejectsPrePromotionTimelineAfterPromotion(t *testing.T) {
	g := NewWithT(t)

	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Method).To(Equal(http.MethodGet))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/primary/status"))
			body := `{"schema_version":1,"snapshot":{"role":"primary","node_id":"primary-a","identity":{"cluster_id":100,"shard_id":10,"table_id":20,"timeline_id":4,"epoch":6},"current_lsn":21,"slots":[],"retention":{"primary_lsn":21,"oldest_restart_lsn":21,"retained_lsn_count":0,"retained_byte_count":0,"retained_age_ns":0,"active_slots":0,"reseed_recommended":0}}}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL: "http://old-primary-ha.default.svc:8081",
				},
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID:        100,
					ShardID:          10,
					TableID:          20,
					TimelineID:       4,
					Epoch:            6,
					CurrentPrimaryID: "primary-a",
				},
				Standbys: []antflyv1.HAStandbySpec{{
					Name:     "standby-a",
					AdminURL: "http://standby-a-ha.default.svc:8081",
				}},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				LastPromotion: haCompletePromotionReceipt("primary-a", "standby-a"),
			},
		},
	}

	err := reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster)
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("identity timeline mismatch"))
	g.Expect(cluster.Status.HAStatus.PrimaryAdminReachable).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.PrimaryLSN).To(Equal(uint64(0)))
}

func TestObserveHAPrimaryAdminStatusRejectsPromotedTimelineWithoutCompleteReceipt(t *testing.T) {
	g := NewWithT(t)

	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Method).To(Equal(http.MethodGet))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/primary/status"))
			g.Expect(req.URL.Host).To(Equal("old-primary-ha.default.svc:8081"))
			body := `{"schema_version":1,"snapshot":{"role":"primary","node_id":"primary-a","identity":{"cluster_id":100,"shard_id":10,"table_id":20,"timeline_id":5,"epoch":7},"current_lsn":21,"slots":[],"retention":{"primary_lsn":21,"oldest_restart_lsn":21,"retained_lsn_count":0,"retained_byte_count":0,"retained_age_ns":0,"active_slots":0,"reseed_recommended":0}}}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})},
	}
	incompletePromotion := haCompletePromotionReceipt("primary-a", "standby-a")
	incompletePromotion.FenceToken = ""
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode:  antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://old-primary-ha.default.svc:8081"},
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID:        100,
					ShardID:          10,
					TableID:          20,
					TimelineID:       4,
					Epoch:            6,
					CurrentPrimaryID: "primary-a",
				},
				Standbys: []antflyv1.HAStandbySpec{{
					Name:     "standby-a",
					AdminURL: "http://standby-a-ha.default.svc:8081",
				}},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				LastPromotion: incompletePromotion,
			},
		},
	}

	err := reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster)
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("identity timeline mismatch"))
	g.Expect(cluster.Status.HAStatus.PrimaryAdminReachable).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.PrimaryLSN).To(Equal(uint64(0)))
}

func TestObserveHAStandbyAdminStatuses(t *testing.T) {
	g := NewWithT(t)
	t.Setenv(haAdminTokenDefaultEnvVar, "default-operator-token")

	var observedTypedURL string
	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Method).To(Equal(http.MethodGet))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/standby/status"))
			g.Expect(req.Header.Get("Authorization")).To(Equal("Bearer default-operator-token"))
			g.Expect(req.URL.Query().Get("upstream_lsn")).To(Equal("13"))
			observedTypedURL = req.URL.String()
			body := `{"schema_version":1,"snapshot":{"role":"standby","node_id":"standby-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"received_lsn":12,"applied_lsn":11,"safe_read_lsn":11,"upstream_lsn":13,"write_lag_lsn":1,"receive_lag_lsn":1,"apply_lag_lsn":2,"last_error":"ConnectionRefused","last_attempt_ns":1000,"last_success_ns":900,"replication_failures_total":3,"unapplied_lsn_count":1,"caught_up_to_received":false,"can_serve_safe_reads":true}}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Standbys: []antflyv1.HAStandbySpec{{
					Name:     "standby-a",
					SlotName: "slot-a",
					AdminURL: "http://standby-a-ha.default.svc:8081",
				}},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				Mode:       antflyv1.HAModeHotStandby,
				PrimaryLSN: 13,
				Standbys: []antflyv1.HAStandbyStatus{{
					Name:            "standby-a",
					SlotName:        "slot-a",
					Active:          true,
					RestartLSN:      7,
					AdminStatusCode: http.StatusServiceUnavailable,
				}},
			},
		},
	}

	g.Expect(reconciler.observeHAStandbyAdminStatuses(context.Background(), cluster)).To(Succeed())
	g.Expect(observedTypedURL).To(ContainSubstring("/admin/v1/ha/standby/status?"))
	g.Expect(cluster.Status.HAStatus.Standbys).To(HaveLen(1))
	standby := cluster.Status.HAStatus.Standbys[0]
	g.Expect(standby.Name).To(Equal("standby-a"))
	g.Expect(standby.SlotName).To(Equal("slot-a"))
	g.Expect(standby.RestartLSN).To(Equal(uint64(7)))
	g.Expect(standby.TimelineID).To(Equal(uint64(4)))
	g.Expect(standby.ReceivedLSN).To(Equal(uint64(12)))
	g.Expect(standby.AppliedLSN).To(Equal(uint64(11)))
	g.Expect(standby.SafeReadLSN).To(Equal(uint64(11)))
	g.Expect(standby.UpstreamLSN).To(Equal(uint64(13)))
	g.Expect(standby.WriteLagLSN).To(Equal(uint64(1)))
	g.Expect(standby.ReceiveLagLSN).To(Equal(uint64(1)))
	g.Expect(standby.ApplyLagLSN).To(Equal(uint64(2)))
	g.Expect(standby.UnappliedLSNCount).To(Equal(uint64(1)))
	g.Expect(standby.CaughtUpToReceived).To(BeFalse())
	g.Expect(standby.CanServeSafeReads).To(BeTrue())
	g.Expect(standby.LastError).To(Equal("ConnectionRefused"))
	g.Expect(standby.LastAttemptNs).To(Equal(uint64(1000)))
	g.Expect(standby.LastSuccessNs).To(Equal(uint64(900)))
	g.Expect(standby.ReplicationFailuresTotal).To(Equal(uint64(3)))
	g.Expect(standby.AdminStatusCode).To(BeZero())
	g.Expect(standby.Status).To(Equal("unhealthy"))
}

func TestObserveHAStandbyAdminStatusesOmitsZeroUpstreamLSN(t *testing.T) {
	g := NewWithT(t)

	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Method).To(Equal(http.MethodGet))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/standby/status"))
			g.Expect(req.URL.Query()).NotTo(HaveKey("upstream_lsn"))
			body := `{"schema_version":1,"snapshot":{"role":"standby","node_id":"standby-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"received_lsn":12,"applied_lsn":12,"safe_read_lsn":12,"write_lag_lsn":0,"receive_lag_lsn":0,"apply_lag_lsn":0,"unapplied_lsn_count":0,"caught_up_to_received":true,"can_serve_safe_reads":true}}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Standbys: []antflyv1.HAStandbySpec{{
					Name:     "standby-a",
					SlotName: "slot-a",
					AdminURL: "http://standby-a-ha.default.svc:8081",
				}},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				Mode:       antflyv1.HAModeHotStandby,
				PrimaryLSN: 0,
			},
		},
	}

	g.Expect(reconciler.observeHAStandbyAdminStatuses(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.Standbys).To(HaveLen(1))
	g.Expect(cluster.Status.HAStatus.Standbys[0].UpstreamLSN).To(BeZero())
	g.Expect(cluster.Status.HAStatus.Standbys[0].Status).To(Equal("healthy"))
}

func TestObserveHAStandbyAdminStatusesRejectsMissingSDKFieldEvidence(t *testing.T) {
	g := NewWithT(t)

	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Method).To(Equal(http.MethodGet))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/standby/status"))
			body := `{"schema_version":1,"snapshot":{"role":"standby","node_id":"standby-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"received_lsn":12,"applied_lsn":11,"upstream_lsn":13,"write_lag_lsn":1,"receive_lag_lsn":1,"apply_lag_lsn":2,"unapplied_lsn_count":1,"caught_up_to_received":false,"can_serve_safe_reads":true}}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Standbys: []antflyv1.HAStandbySpec{{
					Name:     "standby-a",
					SlotName: "slot-a",
					AdminURL: "http://standby-a-ha.default.svc:8081",
				}},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				Mode:       antflyv1.HAModeHotStandby,
				PrimaryLSN: 13,
			},
		},
	}

	err := reconciler.observeHAStandbyAdminStatuses(context.Background(), cluster)
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("missing standby status progress field evidence"))
	g.Expect(cluster.Status.HAStatus.Standbys).To(HaveLen(1))
	g.Expect(cluster.Status.HAStatus.Standbys[0].Name).To(Equal("standby-a"))
	g.Expect(cluster.Status.HAStatus.Standbys[0].Status).To(Equal("unreachable"))
	g.Expect(cluster.Status.HAStatus.Standbys[0].LastError).To(ContainSubstring("missing standby status progress field evidence"))
}

func TestObserveHAStandbyAdminStatusesRecordsUnauthorizedStatus(t *testing.T) {
	g := NewWithT(t)

	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Method).To(Equal(http.MethodGet))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/standby/status"))
			return &http.Response{
				StatusCode: http.StatusUnauthorized,
				Body:       io.NopCloser(strings.NewReader("missing bearer token")),
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Standbys: []antflyv1.HAStandbySpec{{
					Name:     "standby-a",
					SlotName: "slot-a",
					AdminURL: "http://standby-a-ha.default.svc:8081",
				}},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				Mode:       antflyv1.HAModeHotStandby,
				PrimaryLSN: 13,
			},
		},
	}

	err := reconciler.observeHAStandbyAdminStatuses(context.Background(), cluster)
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("status 401"))
	g.Expect(cluster.Status.HAStatus.Standbys).To(HaveLen(1))
	standby := cluster.Status.HAStatus.Standbys[0]
	g.Expect(standby.Name).To(Equal("standby-a"))
	g.Expect(standby.SlotName).To(Equal("slot-a"))
	g.Expect(standby.Status).To(Equal("unreachable"))
	g.Expect(standby.AdminStatusCode).To(Equal(http.StatusUnauthorized))
	g.Expect(standby.LastError).To(ContainSubstring("status 401"))
}

func TestObserveHAStandbyAdminStatusesMarksFailedProbeUnhealthy(t *testing.T) {
	g := NewWithT(t)

	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Method).To(Equal(http.MethodGet))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/standby/status"))
			return nil, fmt.Errorf("standby admin timeout")
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Standbys: []antflyv1.HAStandbySpec{{
					Name:     "standby-a",
					SlotName: "slot-a",
					AdminURL: "http://standby-a-ha.default.svc:8081",
				}},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				Mode:       antflyv1.HAModeHotStandby,
				PrimaryLSN: 13,
				Standbys: []antflyv1.HAStandbyStatus{{
					Name:               "standby-a",
					SlotName:           "slot-a",
					Active:             true,
					ReceivedLSN:        13,
					AppliedLSN:         13,
					SafeReadLSN:        13,
					CaughtUpToReceived: true,
					CanServeSafeReads:  true,
					Status:             "healthy",
				}},
			},
		},
	}

	err := reconciler.observeHAStandbyAdminStatuses(context.Background(), cluster)
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("standby-a"))
	g.Expect(cluster.Status.HAStatus.Standbys).To(HaveLen(1))
	standby := cluster.Status.HAStatus.Standbys[0]
	g.Expect(standby.Status).To(Equal("unreachable"))
	g.Expect(standby.LastError).To(ContainSubstring("standby admin timeout"))
	g.Expect(standby.CaughtUpToReceived).To(BeFalse())
	g.Expect(standby.CanServeSafeReads).To(BeFalse())

	reconciler.updateHAStatusAndConditions(cluster)
	g.Expect(cluster.Status.HAStatus.UnhealthyStandbyCount).To(Equal(int32(1)))
	g.Expect(cluster.Status.HAStatus.ReadSafeStandbyCount).To(Equal(int32(0)))
}

func TestMarkHAStandbyAdminErrorDoesNotMatchEmptySlotName(t *testing.T) {
	status := &antflyv1.HAStatus{
		Standbys: []antflyv1.HAStandbyStatus{{
			Name:   "standby-a",
			Status: "healthy",
		}},
	}

	markHAStandbyAdminError(status, "standby-b", "", fmt.Errorf("standby admin timeout"))

	if len(status.Standbys) != 2 {
		t.Fatalf("expected distinct standby entry, got %#v", status.Standbys)
	}
	if status.Standbys[0].Name != "standby-a" || status.Standbys[0].Status != "healthy" || status.Standbys[0].LastError != "" {
		t.Fatalf("expected standby-a to remain unchanged, got %#v", status.Standbys[0])
	}
	if status.Standbys[1].Name != "standby-b" || status.Standbys[1].Status != "unreachable" ||
		!strings.Contains(status.Standbys[1].LastError, "standby admin timeout") {
		t.Fatalf("expected standby-b error status, got %#v", status.Standbys[1])
	}
}

func TestObserveHAStandbyAdminStatusesSkipsPromotedPrimary(t *testing.T) {
	g := NewWithT(t)

	var observedHosts []string
	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Method).To(Equal(http.MethodGet))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/standby/status"))
			g.Expect(req.URL.Host).NotTo(Equal("standby-a-ha.default.svc:8081"))
			observedHosts = append(observedHosts, req.URL.Host)
			body := `{"schema_version":1,"snapshot":{"role":"standby","node_id":"standby-b","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":5,"epoch":7},"received_lsn":20,"applied_lsn":20,"safe_read_lsn":20,"upstream_lsn":21,"write_lag_lsn":1,"receive_lag_lsn":1,"apply_lag_lsn":1,"unapplied_lsn_count":0,"caught_up_to_received":true,"can_serve_safe_reads":true}}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Standbys: []antflyv1.HAStandbySpec{{
					Name:     "standby-a",
					SlotName: "slot-a",
					AdminURL: "http://standby-a-ha.default.svc:8081",
				}, {
					Name:     "standby-b",
					SlotName: "slot-b",
					AdminURL: "http://standby-b-ha.default.svc:8081",
				}},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				Mode:          antflyv1.HAModeHotStandby,
				PrimaryLSN:    21,
				LastPromotion: haCompletePromotionReceipt("primary-a", "standby-a"),
			},
		},
	}

	g.Expect(reconciler.observeHAStandbyAdminStatuses(context.Background(), cluster)).To(Succeed())
	g.Expect(observedHosts).To(Equal([]string{"standby-b-ha.default.svc:8081"}))
	g.Expect(cluster.Status.HAStatus.Standbys).To(HaveLen(1))
	g.Expect(cluster.Status.HAStatus.Standbys[0].Name).To(Equal("standby-b"))
	g.Expect(cluster.Status.HAStatus.Standbys[0].SlotName).To(Equal("slot-b"))
}

func TestObserveHAStandbyAdminStatusesRejectsMismatchedIdentityScope(t *testing.T) {
	g := NewWithT(t)

	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Method).To(Equal(http.MethodGet))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/standby/status"))
			body := `{"schema_version":1,"snapshot":{"role":"standby","node_id":"standby-a","identity":{"cluster_id":1,"shard_id":99,"table_id":3,"timeline_id":4,"epoch":5},"received_lsn":12,"applied_lsn":11,"safe_read_lsn":11,"upstream_lsn":13,"write_lag_lsn":1,"receive_lag_lsn":1,"apply_lag_lsn":2,"unapplied_lsn_count":1,"caught_up_to_received":false,"can_serve_safe_reads":true}}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID:        1,
					ShardID:          2,
					TableID:          3,
					TimelineID:       4,
					Epoch:            5,
					CurrentPrimaryID: "primary-a",
				},
				Standbys: []antflyv1.HAStandbySpec{{
					Name:     "standby-a",
					SlotName: "slot-a",
					AdminURL: "http://standby-a-ha.default.svc:8081",
				}},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				Mode:       antflyv1.HAModeHotStandby,
				PrimaryLSN: 13,
			},
		},
	}

	err := reconciler.observeHAStandbyAdminStatuses(context.Background(), cluster)
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("identity scope mismatch"))
	g.Expect(cluster.Status.HAStatus.Standbys).To(HaveLen(1))
	g.Expect(cluster.Status.HAStatus.Standbys[0].Status).To(Equal("unreachable"))
	g.Expect(cluster.Status.HAStatus.Standbys[0].LastError).To(ContainSubstring("identity scope mismatch"))
}

func TestObserveHAStandbyAdminStatusesRejectsStaleTimeline(t *testing.T) {
	g := NewWithT(t)

	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Method).To(Equal(http.MethodGet))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/standby/status"))
			body := `{"schema_version":1,"snapshot":{"role":"standby","node_id":"standby-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":3,"epoch":5},"received_lsn":12,"applied_lsn":11,"safe_read_lsn":11,"upstream_lsn":13,"write_lag_lsn":1,"receive_lag_lsn":1,"apply_lag_lsn":2,"unapplied_lsn_count":1,"caught_up_to_received":false,"can_serve_safe_reads":true}}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID:        1,
					ShardID:          2,
					TableID:          3,
					TimelineID:       4,
					Epoch:            5,
					CurrentPrimaryID: "primary-a",
				},
				Standbys: []antflyv1.HAStandbySpec{{
					Name:     "standby-a",
					SlotName: "slot-a",
					AdminURL: "http://standby-a-ha.default.svc:8081",
				}},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				Mode:       antflyv1.HAModeHotStandby,
				PrimaryLSN: 13,
			},
		},
	}

	err := reconciler.observeHAStandbyAdminStatuses(context.Background(), cluster)
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("identity timeline mismatch"))
	g.Expect(cluster.Status.HAStatus.Standbys).To(HaveLen(1))
	g.Expect(cluster.Status.HAStatus.Standbys[0].Status).To(Equal("unreachable"))
	g.Expect(cluster.Status.HAStatus.Standbys[0].LastError).To(ContainSubstring("identity timeline mismatch"))
}

func TestObserveHAStandbyAdminStatusesRejectsMismatchedNodeID(t *testing.T) {
	g := NewWithT(t)

	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Method).To(Equal(http.MethodGet))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/standby/status"))
			body := `{"schema_version":1,"snapshot":{"role":"standby","node_id":"standby-b","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"received_lsn":12,"applied_lsn":11,"safe_read_lsn":11,"upstream_lsn":13,"write_lag_lsn":1,"receive_lag_lsn":1,"apply_lag_lsn":2,"unapplied_lsn_count":1,"caught_up_to_received":false,"can_serve_safe_reads":true}}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID:        1,
					ShardID:          2,
					TableID:          3,
					TimelineID:       4,
					Epoch:            5,
					CurrentPrimaryID: "primary-a",
				},
				Standbys: []antflyv1.HAStandbySpec{{
					Name:     "standby-a",
					SlotName: "slot-a",
					AdminURL: "http://standby-a-ha.default.svc:8081",
				}},
			},
		},
		Status: antflyv1.AntflyClusterStatus{
			HAStatus: &antflyv1.HAStatus{
				Mode:       antflyv1.HAModeHotStandby,
				PrimaryLSN: 13,
			},
		},
	}

	err := reconciler.observeHAStandbyAdminStatuses(context.Background(), cluster)
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("standby node_id mismatch"))
	g.Expect(cluster.Status.HAStatus.Standbys).To(HaveLen(1))
	g.Expect(cluster.Status.HAStatus.Standbys[0].Status).To(Equal("unreachable"))
	g.Expect(cluster.Status.HAStatus.Standbys[0].LastError).To(ContainSubstring("standby node_id mismatch"))
}

func TestParseHAStatusJSONAcceptsLegacyCommandShape(t *testing.T) {
	g := NewWithT(t)

	primary, err := parseHAPrimaryStatusJSON([]byte(`{"schema_version":1,"result":{"primary_status":{"role":"primary","node_id":"primary-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"current_lsn":12,"slots":[{"name":"standby-a","timeline_id":4,"active":true,"reseed_required":false,"restart_lsn":7,"received_lsn":12,"applied_lsn":11,"safe_read_lsn":11,"write_lag_lsn":0,"apply_lag_lsn":1,"safe_read_lag_lsn":1,"retention_lag_lsn":5,"status":"healthy","last_error":null}],"retention":{"primary_lsn":12,"oldest_restart_lsn":7,"retained_lsn_count":5,"retained_byte_count":512,"retained_age_ns":400,"active_slots":1,"reseed_recommended":0},"durability":{"status":"satisfied","mode":"remote_apply","selection":"first","target_lsn":12,"progress_lsn":12,"missing_lsn_count":0,"satisfied_count":1,"required_count":1,"candidate_count":1}}}}`))
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(primary.PrimaryLSN).To(Equal(uint64(12)))
	g.Expect(primary.Standbys).To(HaveLen(1))
	g.Expect(primary.Standbys[0].Name).To(Equal("standby-a"))
	g.Expect(primary.Retention.OldestRestartLSN).To(Equal(uint64(7)))

	_, err = parseHAPrimaryStatusJSON([]byte(`{"schema_version":1,"snapshot":{"role":"primary","node_id":"primary-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"current_lsn":12,"slots":[{"name":"standby-a","timeline_id":4,"active":true,"reseed_required":false,"restart_lsn":7,"received_lsn":12,"applied_lsn":11,"safe_read_lsn":11,"write_lag_lsn":0,"apply_lag_lsn":1,"safe_read_lag_lsn":1,"retention_lag_lsn":5,"status":"catching_up","last_error":null}],"retention":{"primary_lsn":12,"oldest_restart_lsn":7,"retained_lsn_count":5,"retained_byte_count":512,"retained_age_ns":400,"active_slots":1,"reseed_recommended":0}}}`))
	g.Expect(err).To(HaveOccurred())

	_, err = parseHAPrimaryStatusJSON([]byte(`{"schema_version":1,"snapshot":{"role":"primary","node_id":"primary-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"current_lsn":12,"slots":[],"retention":{"primary_lsn":12,"oldest_restart_lsn":7,"retained_lsn_count":5,"retained_byte_count":512,"retained_age_ns":400,"active_slots":1,"reseed_recommended":0},"durability":{"status":"unknown","mode":"remote_apply","selection":"first","target_lsn":12,"progress_lsn":12,"missing_lsn_count":0,"satisfied_count":1,"required_count":1,"candidate_count":1}}}`))
	g.Expect(err).To(HaveOccurred())

	_, err = parseHAPrimaryStatusJSON([]byte(`{"schema_version":1,"snapshot":{"role":"primary","node_id":"primary-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"current_lsn":12,"slots":[],"retention":{"primary_lsn":12,"oldest_restart_lsn":7,"retained_lsn_count":5,"retained_byte_count":512,"retained_age_ns":400,"active_slots":1,"reseed_recommended":0},"durability":{"status":"satisfied","mode":"remote-apply","selection":"first","target_lsn":12,"progress_lsn":12,"missing_lsn_count":0,"satisfied_count":1,"required_count":1,"candidate_count":1}}}`))
	g.Expect(err).To(HaveOccurred())

	_, err = parseHAPrimaryStatusJSON([]byte(`{"schema_version":1,"snapshot":{"role":"primary","node_id":"primary-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"current_lsn":12,"slots":[],"retention":{"primary_lsn":12,"oldest_restart_lsn":7,"retained_lsn_count":5,"retained_byte_count":512,"retained_age_ns":400,"active_slots":1,"reseed_recommended":0},"durability":{"status":"satisfied","mode":"remote_apply","selection":"priority","target_lsn":12,"progress_lsn":12,"missing_lsn_count":0,"satisfied_count":1,"required_count":1,"candidate_count":1}}}`))
	g.Expect(err).To(HaveOccurred())

	standby, err := parseHAStandbyStatusJSON([]byte(`{"schema_version":1,"result":{"standby_status":{"role":"standby","node_id":"standby-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"received_lsn":12,"applied_lsn":11,"safe_read_lsn":11,"upstream_lsn":13,"write_lag_lsn":1,"receive_lag_lsn":1,"apply_lag_lsn":2,"last_error":"ConnectionRefused","last_attempt_ns":1000,"last_success_ns":900,"replication_failures_total":3,"unapplied_lsn_count":1,"caught_up_to_received":false,"can_serve_safe_reads":true}}}`), "standby-a", "slot-a")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(standby.Name).To(Equal("standby-a"))
	g.Expect(standby.SlotName).To(Equal("slot-a"))
	g.Expect(standby.ReceivedLSN).To(Equal(uint64(12)))
	g.Expect(standby.ApplyLagLSN).To(Equal(uint64(2)))
	g.Expect(standby.LastError).To(Equal("ConnectionRefused"))
	g.Expect(standby.LastAttemptNs).To(Equal(uint64(1000)))
	g.Expect(standby.LastSuccessNs).To(Equal(uint64(900)))
	g.Expect(standby.ReplicationFailuresTotal).To(Equal(uint64(3)))
	g.Expect(standby.Status).To(Equal("unhealthy"))

	_, err = parseHAPrimaryStatusJSON([]byte(`{"schema_version":1,"snapshot":{"role":"primary","node_id":"primary-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"current_lsn":12,"slots":[],"retention":{"primary_lsn":12,"oldest_restart_lsn":7,"active_slots":1,"reseed_recommended":0}}}`))
	g.Expect(err).To(HaveOccurred())

	_, err = parseHAStandbyStatusJSON([]byte(`{"schema_version":1,"snapshot":{"role":"standby","node_id":"standby-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"received_lsn":12,"applied_lsn":11,"safe_read_lsn":11,"upstream_lsn":13,"write_lag_lsn":1,"receive_lag_lsn":1,"apply_lag_lsn":2,"unapplied_lsn_count":1,"caught_up_to_received":true}}`), "standby-a", "slot-a")
	g.Expect(err).To(HaveOccurred())

	_, err = parseHAPrimaryStatusJSON([]byte(`{"snapshot":{"role":"primary","node_id":"primary-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"current_lsn":12,"slots":[],"retention":{"primary_lsn":12,"oldest_restart_lsn":7,"retained_lsn_count":5,"retained_byte_count":512,"retained_age_ns":400,"active_slots":1,"reseed_recommended":0}}}`))
	g.Expect(err).To(HaveOccurred())

	_, err = parseHAPrimaryStatusJSON([]byte(`{"result":{"primary_status":{"role":"primary","node_id":"primary-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"current_lsn":12,"slots":[],"retention":{"primary_lsn":12,"oldest_restart_lsn":7,"retained_lsn_count":5,"retained_byte_count":512,"retained_age_ns":400,"active_slots":1,"reseed_recommended":0}}}}`))
	g.Expect(err).To(HaveOccurred())

	_, err = parseHAStandbyStatusJSON([]byte(`{"snapshot":{"role":"standby","node_id":"standby-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"received_lsn":12,"applied_lsn":11,"safe_read_lsn":11,"upstream_lsn":13,"write_lag_lsn":1,"receive_lag_lsn":1,"apply_lag_lsn":2,"unapplied_lsn_count":1,"caught_up_to_received":true,"can_serve_safe_reads":true}}`), "standby-a", "slot-a")
	g.Expect(err).To(HaveOccurred())

	_, err = parseHAStandbyStatusJSON([]byte(`{"result":{"standby_status":{"role":"standby","node_id":"standby-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"received_lsn":12,"applied_lsn":11,"safe_read_lsn":11,"upstream_lsn":13,"write_lag_lsn":1,"receive_lag_lsn":1,"apply_lag_lsn":2,"unapplied_lsn_count":1,"caught_up_to_received":true,"can_serve_safe_reads":true}}}`), "standby-a", "slot-a")
	g.Expect(err).To(HaveOccurred())
}

// T005: Unit test for applyDefaults() setting PublicAPI.Enabled=false
func TestApplyDefaults_PublicAPIDefaultsFalse(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	err := antflyv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())

	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).Build(),
		Scheme: s,
	}

	// Test Case 1: Cluster without PublicAPI field (nil) — should default to enabled=false
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pubapi-default",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 3,
			},
		},
	}

	reconciler.applyDefaults(cluster)

	g.Expect(cluster.Spec.PublicAPI).ToNot(BeNil())
	g.Expect(cluster.Spec.PublicAPI.Enabled).ToNot(BeNil())
	g.Expect(*cluster.Spec.PublicAPI.Enabled).To(BeFalse(), "PublicAPI.Enabled should default to false")
	g.Expect(*cluster.Spec.PublicAPI.ServiceType).To(Equal(corev1.ServiceTypeLoadBalancer))
	g.Expect(cluster.Spec.PublicAPI.Port).To(Equal(int32(80)))

	// Test Case 2: Cluster with PublicAPI but Enabled=nil — should default to false
	clusterPartial := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pubapi-partial",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 3,
			},
			PublicAPI: &antflyv1.PublicAPIConfig{
				Port: 8080,
			},
		},
	}

	reconciler.applyDefaults(clusterPartial)

	g.Expect(clusterPartial.Spec.PublicAPI.Enabled).ToNot(BeNil())
	g.Expect(*clusterPartial.Spec.PublicAPI.Enabled).To(BeFalse(), "PublicAPI.Enabled should default to false when nil")

	// Test Case 3: Cluster with PublicAPI explicitly enabled — should remain true
	enabledTrue := true
	clusterEnabled := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pubapi-enabled",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas: 3,
			},
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 3,
			},
			PublicAPI: &antflyv1.PublicAPIConfig{
				Enabled: &enabledTrue,
			},
		},
	}

	reconciler.applyDefaults(clusterEnabled)

	g.Expect(*clusterEnabled.Spec.PublicAPI.Enabled).To(BeTrue(), "Explicitly enabled PublicAPI should remain true")
}

func TestReconcileInferencePoolCreatesManagedPool(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := newOperatorTestScheme(g)
	reconciler := &AntflyClusterReconciler{
		Client:                fake.NewClientBuilder().WithScheme(s).Build(),
		Scheme:                s,
		ManageInferencePools:  true,
		DefaultInferenceImage: "ghcr.io/antflydb/antfly:omni-test",
	}
	cluster := baseClusterWithInferenceSpec()

	err := reconciler.reconcileInferencePool(ctx, cluster)
	g.Expect(err).NotTo(HaveOccurred())

	pool := &inferencev1alpha1.InferencePool{}
	err = reconciler.Get(ctx, types.NamespacedName{Name: "test-cluster-inference", Namespace: "default"}, pool)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(pool.Spec.Image).To(Equal("ghcr.io/antflydb/antfly:omni-test"))
	g.Expect(pool.Labels).To(HaveKeyWithValue("app.kubernetes.io/instance", "test-cluster"))
	g.Expect(pool.Labels).To(HaveKeyWithValue("app.kubernetes.io/managed-by", "antfly-operator"))
	g.Expect(metav1.IsControlledBy(pool, cluster)).To(BeTrue())
}

func TestReconcileInferencePoolPreservesCustomImage(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := newOperatorTestScheme(g)
	reconciler := &AntflyClusterReconciler{
		Client:                fake.NewClientBuilder().WithScheme(s).Build(),
		Scheme:                s,
		ManageInferencePools:  true,
		DefaultInferenceImage: "ghcr.io/antflydb/antfly:omni-test",
	}
	cluster := baseClusterWithInferenceSpec()
	cluster.Spec.Inference.ManagedPools[0].Spec.Image = "registry.example.com/antfly:custom-inference"

	err := reconciler.reconcileInferencePool(ctx, cluster)
	g.Expect(err).NotTo(HaveOccurred())

	pool := &inferencev1alpha1.InferencePool{}
	err = reconciler.Get(ctx, types.NamespacedName{Name: "test-cluster-inference", Namespace: "default"}, pool)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(pool.Spec.Image).To(Equal("registry.example.com/antfly:custom-inference"))
}

func TestReconcileInferencePoolDeletesOnlyManagedPool(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := newOperatorTestScheme(g)
	cluster := baseClusterWithInferenceSpec()
	cluster.Spec.Inference = nil
	managedPool := &inferencev1alpha1.InferencePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-inference",
			Namespace: "default",
		},
	}
	g.Expect(controllerutil.SetControllerReference(cluster, managedPool, s)).To(Succeed())
	unmanagedPool := &inferencev1alpha1.InferencePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "other-cluster-inference",
			Namespace: "default",
		},
	}
	reconciler := &AntflyClusterReconciler{
		Client:                fake.NewClientBuilder().WithScheme(s).WithObjects(managedPool, unmanagedPool).Build(),
		Scheme:                s,
		ManageInferencePools:  true,
		DefaultInferenceImage: "ghcr.io/antflydb/antfly:omni-test",
	}

	err := reconciler.reconcileInferencePool(ctx, cluster)
	g.Expect(err).NotTo(HaveOccurred())

	err = reconciler.Get(ctx, types.NamespacedName{Name: "test-cluster-inference", Namespace: "default"}, &inferencev1alpha1.InferencePool{})
	g.Expect(errors.IsNotFound(err)).To(BeTrue())
	err = reconciler.Get(ctx, types.NamespacedName{Name: "other-cluster-inference", Namespace: "default"}, &inferencev1alpha1.InferencePool{})
	g.Expect(err).NotTo(HaveOccurred())
}

func TestReconcileInferencePoolDoesNotAdoptExistingUnmanagedPool(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := newOperatorTestScheme(g)
	cluster := baseClusterWithInferenceSpec()
	existingPool := &inferencev1alpha1.InferencePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-inference",
			Namespace: "default",
			UID:       "existing-pool",
		},
		Spec: inferencev1alpha1.InferencePoolSpec{
			Image: "existing:image",
		},
	}
	reconciler := &AntflyClusterReconciler{
		Client:                fake.NewClientBuilder().WithScheme(s).WithObjects(existingPool).Build(),
		Scheme:                s,
		ManageInferencePools:  true,
		DefaultInferenceImage: "ghcr.io/antflydb/antfly:omni-test",
	}

	err := reconciler.reconcileInferencePool(ctx, cluster)
	g.Expect(err).NotTo(HaveOccurred())

	pool := &inferencev1alpha1.InferencePool{}
	err = reconciler.Get(ctx, types.NamespacedName{Name: "test-cluster-inference", Namespace: "default"}, pool)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(pool.Spec.Image).To(Equal("existing:image"))
	g.Expect(metav1.IsControlledBy(pool, cluster)).To(BeFalse())
	g.Expect(cluster.Status.Conditions).To(ContainElement(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
		"Type":   Equal(antflyv1.TypeInferencePoolReady),
		"Status": Equal(metav1.ConditionFalse),
		"Reason": Equal(antflyv1.ReasonInferencePoolNameConflict),
	})))
}

func TestReconcileInferencePoolNoopsWhenManagementDisabled(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := newOperatorTestScheme(g)
	cluster := baseClusterWithInferenceSpec()
	reconciler := &AntflyClusterReconciler{
		Client:               fake.NewClientBuilder().WithScheme(s).Build(),
		Scheme:               s,
		ManageInferencePools: false,
	}

	err := reconciler.reconcileInferencePool(ctx, cluster)
	g.Expect(err).NotTo(HaveOccurred())
	err = reconciler.Get(ctx, types.NamespacedName{Name: "test-cluster-inference", Namespace: "default"}, &inferencev1alpha1.InferencePool{})
	g.Expect(errors.IsNotFound(err)).To(BeTrue())
	g.Expect(cluster.Status.Conditions).To(ContainElement(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
		"Type":   Equal(antflyv1.TypeInferencePoolReady),
		"Status": Equal(metav1.ConditionUnknown),
		"Reason": Equal(antflyv1.ReasonInferencePoolManagementDisabled),
	})))
}

func TestReconcileInferencePoolManagementDisabledLeavesOwnedPool(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := newOperatorTestScheme(g)
	cluster := baseClusterWithInferenceSpec()
	managedPool := &inferencev1alpha1.InferencePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-inference",
			Namespace: "default",
		},
	}
	g.Expect(controllerutil.SetControllerReference(cluster, managedPool, s)).To(Succeed())
	reconciler := &AntflyClusterReconciler{
		Client:               fake.NewClientBuilder().WithScheme(s).WithObjects(managedPool).Build(),
		Scheme:               s,
		ManageInferencePools: false,
	}

	err := reconciler.reconcileInferencePool(ctx, cluster)
	g.Expect(err).NotTo(HaveOccurred())

	pool := &inferencev1alpha1.InferencePool{}
	err = reconciler.Get(ctx, types.NamespacedName{Name: "test-cluster-inference", Namespace: "default"}, pool)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(metav1.IsControlledBy(pool, cluster)).To(BeTrue())
	g.Expect(cluster.Status.Conditions).To(ContainElement(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
		"Type":   Equal(antflyv1.TypeInferencePoolReady),
		"Status": Equal(metav1.ConditionUnknown),
		"Reason": Equal(antflyv1.ReasonInferencePoolManagementDisabled),
	})))
}

func TestReconcileInferencePoolSharedRefDoesNotCreatePool(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := newOperatorTestScheme(g)
	cluster := baseClusterWithInferenceSpec()
	cluster.Spec.Inference = &antflyv1.AntflyInferenceSpec{
		Mode: antflyv1.AntflyInferenceModeSharedRef,
		SharedPools: []antflyv1.InferencePoolReference{{
			Name:      "customer-shared-embeddings",
			Namespace: "inference",
		}},
	}
	reconciler := &AntflyClusterReconciler{
		Client:               fake.NewClientBuilder().WithScheme(s).Build(),
		Scheme:               s,
		ManageInferencePools: true,
	}

	err := reconciler.reconcileInferencePool(ctx, cluster)
	g.Expect(err).NotTo(HaveOccurred())
	err = reconciler.Get(ctx, types.NamespacedName{Name: "test-cluster-inference", Namespace: "default"}, &inferencev1alpha1.InferencePool{})
	g.Expect(errors.IsNotFound(err)).To(BeTrue())
	g.Expect(cluster.Status.Conditions).To(ContainElement(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
		"Type":   Equal(antflyv1.TypeInferencePoolReady),
		"Status": Equal(metav1.ConditionTrue),
		"Reason": Equal(antflyv1.ReasonInferencePoolReady),
	})))
}

func TestReconcileInferencePoolPlatformSharedDeletesOwnedPool(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := newOperatorTestScheme(g)
	cluster := baseClusterWithInferenceSpec()
	managedPool := &inferencev1alpha1.InferencePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-inference",
			Namespace: "default",
		},
	}
	g.Expect(controllerutil.SetControllerReference(cluster, managedPool, s)).To(Succeed())
	cluster.Spec.Inference = &antflyv1.AntflyInferenceSpec{
		Mode: antflyv1.AntflyInferenceModePlatformShared,
		PlatformPools: []antflyv1.InferencePoolReference{{
			Name: "default-embeddings",
		}},
	}
	reconciler := &AntflyClusterReconciler{
		Client:               fake.NewClientBuilder().WithScheme(s).WithObjects(managedPool).Build(),
		Scheme:               s,
		ManageInferencePools: true,
	}

	err := reconciler.reconcileInferencePool(ctx, cluster)
	g.Expect(err).NotTo(HaveOccurred())
	err = reconciler.Get(ctx, types.NamespacedName{Name: "test-cluster-inference", Namespace: "default"}, &inferencev1alpha1.InferencePool{})
	g.Expect(errors.IsNotFound(err)).To(BeTrue())
}

func newOperatorTestScheme(g *WithT) *runtime.Scheme {
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(inferencev1alpha1.AddToScheme(s)).To(Succeed())
	return s
}

func baseClusterWithInferenceSpec() *antflyv1.AntflyCluster {
	return &antflyv1.AntflyCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: antflyv1.GroupVersion.String(),
			Kind:       "AntflyCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:test",
			Inference: &antflyv1.AntflyInferenceSpec{
				Mode: antflyv1.AntflyInferenceModeManaged,
				ManagedPools: []antflyv1.ManagedInferencePoolSpec{{
					Spec: inferencev1alpha1.InferencePoolSpec{
						Models:   inferencev1alpha1.ModelConfig{},
						Replicas: inferencev1alpha1.ReplicaConfig{Min: 1, Max: 2},
						Hardware: inferencev1alpha1.HardwareConfig{},
					},
				}},
			},
		},
	}
}

func TestApplyDefaults_StandaloneDefaults(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	err := antflyv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())

	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).Build(),
		Scheme: s,
	}

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-standalone",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Mode:       antflyv1.ClusterModeStandalone,
			Image:      "antfly:latest",
			Standalone: &antflyv1.StandaloneSpec{},
			Storage: antflyv1.StorageSpec{
				StorageClass:      "standard",
				StandaloneStorage: "1Gi",
			},
		},
	}

	reconciler.applyDefaults(cluster)

	g.Expect(cluster.Spec.Standalone).ToNot(BeNil())
	g.Expect(cluster.Spec.Standalone.Replicas).To(Equal(int32(1)))
	g.Expect(cluster.Spec.Standalone.NodeID).To(Equal(int32(1)))
	g.Expect(cluster.Spec.Standalone.MetadataAPI.Port).To(Equal(int32(8080)))
	g.Expect(cluster.Spec.Standalone.MetadataRaft.Port).To(Equal(int32(9017)))
	g.Expect(cluster.Spec.Standalone.StoreAPI.Port).To(Equal(int32(12380)))
	g.Expect(cluster.Spec.Standalone.StoreRaft.Port).To(Equal(int32(9021)))
	g.Expect(cluster.Spec.Standalone.Health.Port).To(Equal(int32(4200)))
	g.Expect(cluster.Spec.Standalone.Inference).ToNot(BeNil())
	g.Expect(cluster.Spec.Standalone.Inference.Enabled).To(BeTrue())
	g.Expect(cluster.Spec.Standalone.Inference.APIURL).To(Equal("http://0.0.0.0:11433"))
}

func TestReconcilePVCExpansionReportsInProgress(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default", Generation: 7},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-storage-test-cluster-data-0",
			Namespace: "default",
			Labels: map[string]string{
				"app.kubernetes.io/instance": "test-cluster",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			},
		},
	}
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(pvc).Build(),
		Scheme: s,
	}

	result := reconciler.reconcilePVCExpansion(ctx, cluster, "data", "data-storage", "test-cluster-data", "2Gi")
	reconciler.setPVCExpansionCondition(cluster, []pvcExpansionResult{result})

	cond := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypePVCExpansion)
	g.Expect(cond).NotTo(BeNil())
	g.Expect(cond.Status).To(Equal(metav1.ConditionUnknown))
	g.Expect(cond.Reason).To(Equal(antflyv1.ReasonPVCExpansionInProgress))
	g.Expect(cond.ObservedGeneration).To(Equal(int64(7)))

	updated := &corev1.PersistentVolumeClaim{}
	g.Expect(reconciler.Get(ctx, types.NamespacedName{Name: pvc.Name, Namespace: pvc.Namespace}, updated)).To(Succeed())
	g.Expect(updated.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(resource.MustParse("2Gi")))
}

func TestReconcileStorageAutoGrowRecommendsGrowth(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default", Generation: 8},
		Spec: antflyv1.AntflyClusterSpec{
			Storage: antflyv1.StorageSpec{
				DataStorage: "10Gi",
				StorageAutoGrow: &antflyv1.StorageAutoGrowSpec{
					Enabled:              true,
					MaxDataStorage:       "20Gi",
					GrowThresholdPercent: 80,
					GrowIncrement:        "5Gi",
				},
			},
		},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-storage-test-cluster-data-0",
			Namespace: "default",
			Labels: map[string]string{
				"app.kubernetes.io/instance": "test-cluster",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("10Gi"),
				},
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-data-0",
			Namespace: "default",
			Labels:    serviceSelectorLabels("test-cluster", "data"),
		},
		Spec: corev1.PodSpec{NodeName: "node-1"},
	}
	usedBytes := uint64(9 * 1024 * 1024 * 1024)
	capacityBytes := uint64(10 * 1024 * 1024 * 1024)
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(pvc, pod).Build(),
		Scheme: s,
		NodeStatsFetcher: func(context.Context, string) (*kubeletStatsSummary, error) {
			return &kubeletStatsSummary{
				Pods: []kubeletPodStats{
					{
						PodRef: kubeletPodReference{Name: pod.Name, Namespace: pod.Namespace},
						Volume: []kubeletVolumeStats{
							{
								PVCRef:        &kubeletPVCReference{Name: pvc.Name, Namespace: pvc.Namespace},
								UsedBytes:     &usedBytes,
								CapacityBytes: &capacityBytes,
							},
						},
					},
				},
			}, nil
		},
	}

	recommended := reconciler.reconcileStorageAutoGrow(ctx, cluster, "data", "data-storage", "test-cluster-data", "10Gi", "20Gi")

	g.Expect(recommended).To(Equal("15Gi"))
	g.Expect(cluster.Status.StorageAutoGrowStatus).NotTo(BeNil())
	g.Expect(cluster.Status.StorageAutoGrowStatus.Reason).To(Equal(antflyv1.ReasonStorageAutoGrowInProgress))
	g.Expect(cluster.Status.StorageAutoGrowStatus.UsagePercent).To(Equal(int32(90)))
	cond := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeStorageAutoGrow)
	g.Expect(cond).NotTo(BeNil())
	g.Expect(cond.Status).To(Equal(metav1.ConditionUnknown))
	g.Expect(cond.Reason).To(Equal(antflyv1.ReasonStorageAutoGrowInProgress))
}

func TestSetPVCExpansionConditionReportsComplete(t *testing.T) {
	g := NewWithT(t)
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default", Generation: 3},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.setPVCExpansionCondition(cluster, []pvcExpansionResult{
		{component: "metadata", state: pvcExpansionComplete, message: "metadata complete"},
		{component: "data", state: pvcExpansionComplete, message: "data complete"},
	})

	cond := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypePVCExpansion)
	g.Expect(cond).NotTo(BeNil())
	g.Expect(cond.Status).To(Equal(metav1.ConditionTrue))
	g.Expect(cond.Reason).To(Equal(antflyv1.ReasonPVCExpansionComplete))
}

func TestUpdateRolloutConditionReportsProgressAndComplete(t *testing.T) {
	g := NewWithT(t)
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default", Generation: 4},
	}
	reconciler := &AntflyClusterReconciler{}
	replicas := int32(3)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster-data", Generation: 2},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
		Status: appsv1.StatefulSetStatus{
			ObservedGeneration: 1,
			UpdatedReplicas:    1,
			ReadyReplicas:      1,
		},
	}

	reconciler.updateRolloutCondition(cluster, sts)
	cond := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeRollout)
	g.Expect(cond).NotTo(BeNil())
	g.Expect(cond.Status).To(Equal(metav1.ConditionUnknown))
	g.Expect(cond.Reason).To(Equal(antflyv1.ReasonRolloutInProgress))

	sts.Status.ObservedGeneration = 2
	sts.Status.UpdatedReplicas = 3
	sts.Status.ReadyReplicas = 3
	reconciler.updateRolloutCondition(cluster, sts)
	cond = meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeRollout)
	g.Expect(cond.Status).To(Equal(metav1.ConditionTrue))
	g.Expect(cond.Reason).To(Equal(antflyv1.ReasonRolloutComplete))
}

func TestUpdateRolloutConditionReportsMissingStatefulSet(t *testing.T) {
	g := NewWithT(t)
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default", Generation: 4},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.updateRolloutCondition(cluster, &appsv1.StatefulSet{})

	cond := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeRollout)
	g.Expect(cond).NotTo(BeNil())
	g.Expect(cond.Status).To(Equal(metav1.ConditionUnknown))
	g.Expect(cond.Reason).To(Equal(antflyv1.ReasonRolloutInProgress))
	g.Expect(cond.Message).To(ContainSubstring("not observed"))
}

func TestUpdateRolloutConditionReportsBlockedRevision(t *testing.T) {
	g := NewWithT(t)
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default", Generation: 4},
	}
	reconciler := &AntflyClusterReconciler{}
	replicas := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster-standalone", Generation: 2},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
		Status: appsv1.StatefulSetStatus{
			ObservedGeneration: 2,
			CurrentRevision:    "standalone-old",
			UpdateRevision:     "standalone-new",
			UpdatedReplicas:    0,
			ReadyReplicas:      0,
		},
	}

	reconciler.updateRolloutCondition(cluster, sts)

	cond := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeRollout)
	g.Expect(cond).NotTo(BeNil())
	g.Expect(cond.Status).To(Equal(metav1.ConditionFalse))
	g.Expect(cond.Reason).To(Equal(antflyv1.ReasonRolloutBlocked))
	g.Expect(cond.Message).To(ContainSubstring("test-cluster-standalone has 0/1 updated replicas"))
}

func TestRepairBlockedStatefulSetRolloutDeletesStaleUnhealthyPod(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := runtime.NewScheme()
	g.Expect(corev1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
	}
	replicas := int32(3)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster-metadata", Namespace: "default"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "antfly", Image: "antfly:new"}},
				},
			},
		},
		Status: appsv1.StatefulSetStatus{
			UpdateRevision:  "metadata-new",
			UpdatedReplicas: 0,
		},
	}
	staleUnreadyPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-cluster-metadata-0",
			Namespace:       "default",
			OwnerReferences: []metav1.OwnerReference{statefulSetOwnerRef(sts.Name)},
			Labels: mergeStringMaps(
				serviceSelectorLabels("test-cluster", "metadata"),
				map[string]string{"controller-revision-hash": "metadata-old"},
			),
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "antfly", Image: "antfly:old"}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionFalse,
				Reason: "ContainersNotReady",
			}},
		},
	}

	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(staleUnreadyPod).Build(),
		Scheme: s,
	}

	repaired, err := reconciler.repairBlockedStatefulSetRollout(ctx, cluster, sts, "metadata")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(repaired).To(BeTrue())

	deleted := &corev1.Pod{}
	err = reconciler.Get(ctx, types.NamespacedName{Name: staleUnreadyPod.Name, Namespace: staleUnreadyPod.Namespace}, deleted)
	g.Expect(errors.IsNotFound(err)).To(BeTrue())
}

func TestRepairBlockedStatefulSetRolloutsDeletesStaleUnhealthyStandalonePod(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := runtime.NewScheme()
	g.Expect(corev1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{
			Mode: antflyv1.ClusterModeStandalone,
		},
	}
	replicas := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster-standalone", Namespace: "default"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "antfly", Image: "antfly:new"}},
				},
			},
		},
		Status: appsv1.StatefulSetStatus{
			UpdateRevision:  "standalone-new",
			UpdatedReplicas: 0,
		},
	}
	staleUnreadyPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-cluster-standalone-0",
			Namespace:       "default",
			OwnerReferences: []metav1.OwnerReference{statefulSetOwnerRef(sts.Name)},
			Labels: mergeStringMaps(
				serviceSelectorLabels("test-cluster", "standalone"),
				map[string]string{"controller-revision-hash": "standalone-old"},
			),
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "antfly", Image: "antfly:old"}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionFalse,
				Reason: "ContainersNotReady",
			}},
		},
	}

	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(sts, staleUnreadyPod).Build(),
		Scheme: s,
	}

	repaired, err := reconciler.repairBlockedStatefulSetRollouts(ctx, cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(repaired).To(BeTrue())

	deleted := &corev1.Pod{}
	err = reconciler.Get(ctx, types.NamespacedName{Name: staleUnreadyPod.Name, Namespace: staleUnreadyPod.Namespace}, deleted)
	g.Expect(errors.IsNotFound(err)).To(BeTrue())
}

func TestRepairBlockedStatefulSetRolloutKeepsHealthyStalePod(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := runtime.NewScheme()
	g.Expect(corev1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
	}
	replicas := int32(3)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster-data", Namespace: "default"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "antfly", Image: "antfly:new"}},
				},
			},
		},
		Status: appsv1.StatefulSetStatus{
			UpdateRevision:  "data-new",
			UpdatedReplicas: 1,
		},
	}
	healthyStalePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-cluster-data-0",
			Namespace:       "default",
			OwnerReferences: []metav1.OwnerReference{statefulSetOwnerRef(sts.Name)},
			Labels: mergeStringMaps(
				serviceSelectorLabels("test-cluster", "data"),
				map[string]string{"controller-revision-hash": "data-old"},
			),
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "antfly", Image: "antfly:old"}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
		},
	}

	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(healthyStalePod).Build(),
		Scheme: s,
	}

	repaired, err := reconciler.repairBlockedStatefulSetRollout(ctx, cluster, sts, "data")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(repaired).To(BeFalse())

	existing := &corev1.Pod{}
	g.Expect(reconciler.Get(ctx, types.NamespacedName{Name: healthyStalePod.Name, Namespace: healthyStalePod.Namespace}, existing)).To(Succeed())
}

func TestEffectiveDataReplicaTargetPrefersStatefulSetSpec(t *testing.T) {
	g := NewWithT(t)
	replicas := int32(5)
	sts := &appsv1.StatefulSet{
		Spec: appsv1.StatefulSetSpec{Replicas: &replicas},
		Status: appsv1.StatefulSetStatus{
			Replicas: 3,
		},
	}

	g.Expect(effectiveDataReplicas(sts, true, 1)).To(Equal(int32(3)))
	g.Expect(effectiveDataReplicaTarget(sts, true, 1)).To(Equal(int32(5)))
	g.Expect(max(effectiveDataReplicas(sts, true, 1), effectiveDataReplicaTarget(sts, true, 1))).To(Equal(int32(5)))
}

func TestEffectiveDataNodeReplicasSuspendScalesToZero(t *testing.T) {
	g := NewWithT(t)
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 5,
				Suspend:  true,
			},
		},
	}

	g.Expect(effectiveDataNodeReplicas(cluster)).To(Equal(int32(0)))
}

func TestShouldCancelDataScaleDownForSuspend(t *testing.T) {
	g := NewWithT(t)
	status := &antflyv1.DataScaleDownStatus{
		Phase:           "Draining",
		DrainingOrdinal: 4,
		DrainingNodeID:  "5",
	}

	g.Expect(shouldCancelDataScaleDownForSuspend(status, 5)).To(BeTrue())
	g.Expect(shouldCancelDataScaleDownForSuspend(status, 4)).To(BeFalse())
	status.Phase = "Complete"
	g.Expect(shouldCancelDataScaleDownForSuspend(status, 5)).To(BeFalse())
}

func TestNodeIDForDataOrdinalUsesDecimalID(t *testing.T) {
	g := NewWithT(t)

	g.Expect(nodeIDForDataOrdinal(0)).To(Equal("1"))
	g.Expect(nodeIDForDataOrdinal(9)).To(Equal("10"))
}

func TestBuildHTTPStartupProbeUsesDefaultsAndOverrides(t *testing.T) {
	g := NewWithT(t)

	defaultProbe := buildHTTPStartupProbe(4200, nil)
	g.Expect(defaultProbe.HTTPGet).NotTo(BeNil())
	g.Expect(defaultProbe.HTTPGet.Path).To(Equal("/healthz"))
	g.Expect(defaultProbe.HTTPGet.Port.IntValue()).To(Equal(4200))
	g.Expect(defaultProbe.InitialDelaySeconds).To(Equal(int32(30)))
	g.Expect(defaultProbe.PeriodSeconds).To(Equal(int32(10)))
	g.Expect(defaultProbe.TimeoutSeconds).To(Equal(int32(1)))
	g.Expect(defaultProbe.FailureThreshold).To(Equal(int32(30)))

	failureThreshold := int32(180)
	periodSeconds := int32(5)
	timeoutSeconds := int32(3)
	customProbe := buildHTTPStartupProbe(4300, &antflyv1.ProbeConfig{
		FailureThreshold: &failureThreshold,
		PeriodSeconds:    &periodSeconds,
		TimeoutSeconds:   &timeoutSeconds,
	})
	g.Expect(customProbe.HTTPGet.Port.IntValue()).To(Equal(4300))
	g.Expect(customProbe.PeriodSeconds).To(Equal(periodSeconds))
	g.Expect(customProbe.TimeoutSeconds).To(Equal(timeoutSeconds))
	g.Expect(customProbe.FailureThreshold).To(Equal(failureThreshold))
}

func TestSetDataScaleDownStatusRecordsAutoscalerSource(t *testing.T) {
	g := NewWithT(t)
	reconciler := &AntflyClusterReconciler{}
	cluster := &antflyv1.AntflyCluster{}

	reconciler.setDataScaleDownStatus(cluster, antflyv1.DataScaleDownSourceAutoscaler, 5, 3, 4, 4, "5", "Draining", "draining")

	g.Expect(cluster.Status.DataScaleDownStatus).NotTo(BeNil())
	g.Expect(cluster.Status.DataScaleDownStatus.Source).To(Equal(antflyv1.DataScaleDownSourceAutoscaler))
	g.Expect(cluster.Status.DataScaleDownStatus.FromReplicas).To(Equal(int32(5)))
	g.Expect(cluster.Status.DataScaleDownStatus.TargetReplicas).To(Equal(int32(3)))
	g.Expect(cluster.Status.DataScaleDownStatus.AppliedReplicas).To(Equal(int32(4)))
	g.Expect(cluster.Status.DataScaleDownStatus.DrainingOrdinal).To(Equal(int32(4)))
	g.Expect(cluster.Status.DataScaleDownStatus.DrainingNodeID).To(Equal("5"))
	g.Expect(cluster.Status.DataScaleDownStatus.Phase).To(Equal("Draining"))
}

func TestRequestDataNodeShutdownUsesNodeShutdownAPI(t *testing.T) {
	g := NewWithT(t)
	var methods []string
	var paths []string
	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			methods = append(methods, req.Method)
			paths = append(paths, req.URL.Path)
			body := "accepted"
			if req.Method == http.MethodGet {
				body = `{"node_id":5,"phase":"complete","safe_to_terminate":true}`
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(body)),
				Request:    req,
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{
			MetadataNodes: antflyv1.MetadataNodesSpec{
				MetadataAPI: antflyv1.APISpec{Port: 12377},
			},
		},
	}

	status, err := reconciler.requestDataNodeShutdown(context.Background(), cluster, "5")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(status.SafeToTerminate).To(BeTrue())
	g.Expect(methods).To(Equal([]string{http.MethodPut, http.MethodGet}))
	g.Expect(paths).To(Equal([]string{
		"/internal/v1/nodes/5/shutdown",
		"/internal/v1/nodes/5/shutdown",
	}))
}

func TestRequestDataNodeShutdownDecodesBlockedStatus(t *testing.T) {
	g := NewWithT(t)
	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			body := "accepted"
			if req.Method == http.MethodGet {
				body = `{"node_id":5,"phase":"blocked","safe_to_terminate":false,"blocked":true,"blocked_reason":"InsufficientShardVoters","message":"cannot safely remove voter"}`
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(body)),
				Request:    req,
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{
			MetadataNodes: antflyv1.MetadataNodesSpec{
				MetadataAPI: antflyv1.APISpec{Port: 12377},
			},
		},
	}

	status, err := reconciler.requestDataNodeShutdown(context.Background(), cluster, "5")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(status.SafeToTerminate).To(BeFalse())
	g.Expect(status.Blocked).To(BeTrue())
	g.Expect(status.BlockedReason).To(Equal("InsufficientShardVoters"))
	g.Expect(status.Message).To(Equal("cannot safely remove voter"))
}

func TestCancelDataNodeShutdownUsesNodeShutdownAPI(t *testing.T) {
	g := NewWithT(t)
	var methods []string
	var paths []string
	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			methods = append(methods, req.Method)
			paths = append(paths, req.URL.Path)
			body := `{"status":"canceled"}`
			if req.Method == http.MethodGet {
				body = `{"node_id":5,"phase":"active","safe_to_terminate":false}`
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(body)),
				Request:    req,
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{
			MetadataNodes: antflyv1.MetadataNodesSpec{
				MetadataAPI: antflyv1.APISpec{Port: 12377},
			},
		},
	}

	status, err := reconciler.cancelDataNodeShutdown(context.Background(), cluster, "5")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(status.Phase).To(Equal("active"))
	g.Expect(methods).To(Equal([]string{http.MethodDelete, http.MethodGet}))
	g.Expect(paths).To(Equal([]string{
		"/internal/v1/nodes/5/shutdown",
		"/internal/v1/nodes/5/shutdown",
	}))
}

func TestFinalizeDataNodeShutdownUsesNodeAPI(t *testing.T) {
	g := NewWithT(t)
	var methods []string
	var paths []string
	reconciler := &AntflyClusterReconciler{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			methods = append(methods, req.Method)
			paths = append(paths, req.URL.Path)
			return &http.Response{
				StatusCode: http.StatusAccepted,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"status":"finalized"}`)),
				Request:    req,
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{
			MetadataNodes: antflyv1.MetadataNodesSpec{
				MetadataAPI: antflyv1.APISpec{Port: 12377},
			},
		},
	}

	err := reconciler.finalizeDataNodeShutdown(context.Background(), cluster, "5")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(methods).To(Equal([]string{http.MethodDelete}))
	g.Expect(paths).To(Equal([]string{"/internal/v1/nodes/5"}))
}

func TestShouldCancelDataScaleDownWhenOrdinalDesiredAgain(t *testing.T) {
	g := NewWithT(t)
	status := &antflyv1.DataScaleDownStatus{
		Phase:           "Draining",
		DrainingOrdinal: 4,
		DrainingNodeID:  "5",
	}

	g.Expect(shouldCancelDataScaleDown(status, 5, 5)).To(BeTrue())
	g.Expect(shouldCancelDataScaleDown(status, 5, 4)).To(BeFalse())
	g.Expect(shouldCancelDataScaleDown(status, 4, 5)).To(BeFalse(), "Once the StatefulSet removed the ordinal, finalization owns the transition")
	status.Phase = "Canceling"
	g.Expect(shouldCancelDataScaleDown(status, 5, 5)).To(BeTrue())
	status.Phase = "Scaling"
	g.Expect(shouldCancelDataScaleDown(status, 5, 5)).To(BeTrue())
	status.Phase = "Complete"
	g.Expect(shouldCancelDataScaleDown(status, 5, 5)).To(BeFalse())
}

func TestShouldFinalizeDataScaleDownRetriesOnlyPostShrinkFailures(t *testing.T) {
	g := NewWithT(t)
	status := &antflyv1.DataScaleDownStatus{
		Phase:           "Failed",
		FromReplicas:    5,
		TargetReplicas:  4,
		AppliedReplicas: 4,
		DrainingOrdinal: 4,
		DrainingNodeID:  "5",
	}

	g.Expect(shouldFinalizeDataScaleDown(status, 4, 4, false)).To(BeTrue())
	g.Expect(shouldFinalizeDataScaleDown(status, 5, 4, false)).To(BeFalse(), "StatefulSet has not removed the ordinal yet")
	g.Expect(shouldFinalizeDataScaleDown(status, 4, 3, true)).To(BeFalse(), "A new scale-down step should retry drain, not finalize an old failed state")
	g.Expect(shouldFinalizeDataScaleDown(status, 4, 5, false)).To(BeTrue(), "Once the StatefulSet removed the ordinal, finalization owns the transition")

	status.AppliedReplicas = 5
	g.Expect(shouldFinalizeDataScaleDown(status, 5, 5, false)).To(BeFalse(), "Drain failures before StatefulSet shrink are not finalization failures")
}

func TestReconcileCancelsDataScaleDownWhenOrdinalDesiredAgain(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: antflyv1.GroupVersion.String(),
			Kind:       "AntflyCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "cancel-scale",
			Namespace:  "default",
			Generation: 3,
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:test",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas:    1,
				MetadataAPI: antflyv1.APISpec{Port: 12377},
			},
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 5,
			},
			Storage: antflyv1.StorageSpec{
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
		Status: antflyv1.AntflyClusterStatus{
			ObservedGeneration: 3,
			Conditions: []metav1.Condition{
				{Type: antflyv1.TypeConfigurationValid, Status: metav1.ConditionTrue, Reason: antflyv1.ReasonValidationPassed, ObservedGeneration: 3},
			},
			DataScaleDownStatus: &antflyv1.DataScaleDownStatus{
				Source:          antflyv1.DataScaleDownSourceManual,
				FromReplicas:    5,
				TargetReplicas:  3,
				AppliedReplicas: 5,
				DrainingOrdinal: 4,
				DrainingNodeID:  "5",
				Phase:           "Draining",
			},
		},
	}
	dataReplicas := int32(5)
	dataSts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "cancel-scale-data", Namespace: "default"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &dataReplicas,
		},
		Status: appsv1.StatefulSetStatus{
			Replicas: 5,
		},
	}
	g.Expect(controllerutil.SetControllerReference(cluster, dataSts, s)).To(Succeed())
	client := fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(cluster).
		WithObjects(cluster, dataSts).
		Build()

	var methods []string
	var paths []string
	reconciler := &AntflyClusterReconciler{
		Client: client,
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			methods = append(methods, req.Method)
			paths = append(paths, req.URL.Path)
			body := `{"status":"canceled"}`
			if req.Method == http.MethodGet {
				body = `{"node_id":5,"phase":"active","safe_to_terminate":false}`
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(body)),
				Request:    req,
			}, nil
		})},
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "cancel-scale", Namespace: "default"}})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(result.RequeueAfter).To(Equal(10 * time.Second))
	g.Expect(methods).To(Equal([]string{http.MethodDelete, http.MethodGet}))
	g.Expect(paths).To(Equal([]string{"/internal/v1/nodes/5/shutdown", "/internal/v1/nodes/5/shutdown"}))

	updated := &antflyv1.AntflyCluster{}
	g.Expect(client.Get(ctx, types.NamespacedName{Name: "cancel-scale", Namespace: "default"}, updated)).To(Succeed())
	g.Expect(updated.Status.DataScaleDownStatus).NotTo(BeNil())
	g.Expect(updated.Status.DataScaleDownStatus.Phase).To(Equal("Canceled"))
	cond := meta.FindStatusCondition(updated.Status.Conditions, antflyv1.TypeScaling)
	g.Expect(cond).NotTo(BeNil())
	g.Expect(cond.Status).To(Equal(metav1.ConditionTrue))
}

func TestReconcileWaitsForDataScaleDownCancellationRecovery(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: antflyv1.GroupVersion.String(), Kind: "AntflyCluster"},
		ObjectMeta: metav1.ObjectMeta{Name: "cancel-recovering", Namespace: "default", Generation: 3},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:test",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas:    1,
				MetadataAPI: antflyv1.APISpec{Port: 12377},
			},
			DataNodes: antflyv1.DataNodesSpec{Replicas: 5},
			Storage: antflyv1.StorageSpec{
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
		Status: antflyv1.AntflyClusterStatus{
			ObservedGeneration: 3,
			Conditions: []metav1.Condition{
				{Type: antflyv1.TypeConfigurationValid, Status: metav1.ConditionTrue, Reason: antflyv1.ReasonValidationPassed, ObservedGeneration: 3},
			},
			DataScaleDownStatus: &antflyv1.DataScaleDownStatus{
				Source:          antflyv1.DataScaleDownSourceManual,
				FromReplicas:    5,
				TargetReplicas:  3,
				AppliedReplicas: 5,
				DrainingOrdinal: 4,
				DrainingNodeID:  "5",
				Phase:           "Draining",
			},
		},
	}
	dataReplicas := int32(5)
	dataSts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "cancel-recovering-data", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &dataReplicas},
		Status:     appsv1.StatefulSetStatus{Replicas: 5},
	}
	g.Expect(controllerutil.SetControllerReference(cluster, dataSts, s)).To(Succeed())
	client := fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(cluster).
		WithObjects(cluster, dataSts).
		Build()

	reconciler := &AntflyClusterReconciler{
		Client: client,
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			body := `{"status":"canceled"}`
			if req.Method == http.MethodGet {
				body = `{"node_id":5,"phase":"recovering","safe_to_terminate":false}`
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(body)),
				Request:    req,
			}, nil
		})},
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "cancel-recovering", Namespace: "default"}})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(result.RequeueAfter).To(Equal(10 * time.Second))

	updated := &antflyv1.AntflyCluster{}
	g.Expect(client.Get(ctx, types.NamespacedName{Name: "cancel-recovering", Namespace: "default"}, updated)).To(Succeed())
	g.Expect(updated.Status.DataScaleDownStatus).NotTo(BeNil())
	g.Expect(updated.Status.DataScaleDownStatus.Phase).To(Equal("Canceling"))
	cond := meta.FindStatusCondition(updated.Status.Conditions, antflyv1.TypeScaling)
	g.Expect(cond).NotTo(BeNil())
	g.Expect(cond.Status).To(Equal(metav1.ConditionUnknown))
}

func TestReconcileFinalizesDataScaleDownAfterStatefulSetShrinks(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: antflyv1.GroupVersion.String(), Kind: "AntflyCluster"},
		ObjectMeta: metav1.ObjectMeta{Name: "finalize-scale", Namespace: "default", Generation: 3},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:test",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas:    1,
				MetadataAPI: antflyv1.APISpec{Port: 12377},
			},
			DataNodes: antflyv1.DataNodesSpec{Replicas: 5},
			Storage: antflyv1.StorageSpec{
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
		Status: antflyv1.AntflyClusterStatus{
			ObservedGeneration: 3,
			Conditions: []metav1.Condition{
				{Type: antflyv1.TypeConfigurationValid, Status: metav1.ConditionTrue, Reason: antflyv1.ReasonValidationPassed, ObservedGeneration: 3},
			},
			DataScaleDownStatus: &antflyv1.DataScaleDownStatus{
				Source:          antflyv1.DataScaleDownSourceManual,
				FromReplicas:    5,
				TargetReplicas:  4,
				AppliedReplicas: 4,
				DrainingOrdinal: 4,
				DrainingNodeID:  "5",
				Phase:           "Scaling",
			},
		},
	}
	dataReplicas := int32(4)
	dataSts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "finalize-scale-data", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &dataReplicas},
		Status:     appsv1.StatefulSetStatus{Replicas: 4},
	}
	g.Expect(controllerutil.SetControllerReference(cluster, dataSts, s)).To(Succeed())
	client := fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(cluster).
		WithObjects(cluster, dataSts).
		Build()

	var methods []string
	var paths []string
	reconciler := &AntflyClusterReconciler{
		Client: client,
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			methods = append(methods, req.Method)
			paths = append(paths, req.URL.Path)
			return &http.Response{
				StatusCode: http.StatusAccepted,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"status":"finalized"}`)),
				Request:    req,
			}, nil
		})},
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "finalize-scale", Namespace: "default"}})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(result.RequeueAfter).To(Equal(1 * time.Second))
	g.Expect(methods).To(Equal([]string{http.MethodDelete}))
	g.Expect(paths).To(Equal([]string{"/internal/v1/nodes/5"}))

	updated := &antflyv1.AntflyCluster{}
	g.Expect(client.Get(ctx, types.NamespacedName{Name: "finalize-scale", Namespace: "default"}, updated)).To(Succeed())
	g.Expect(updated.Status.DataScaleDownStatus).NotTo(BeNil())
	g.Expect(updated.Status.DataScaleDownStatus.Phase).To(Equal("Complete"))
}

func TestReconcileRetriesFailedDataScaleDownFinalization(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: antflyv1.GroupVersion.String(), Kind: "AntflyCluster"},
		ObjectMeta: metav1.ObjectMeta{Name: "retry-finalize-scale", Namespace: "default", Generation: 3},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:test",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas:    1,
				MetadataAPI: antflyv1.APISpec{Port: 12377},
			},
			DataNodes: antflyv1.DataNodesSpec{Replicas: 4},
			Storage: antflyv1.StorageSpec{
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
		Status: antflyv1.AntflyClusterStatus{
			ObservedGeneration: 3,
			Conditions: []metav1.Condition{
				{Type: antflyv1.TypeConfigurationValid, Status: metav1.ConditionTrue, Reason: antflyv1.ReasonValidationPassed, ObservedGeneration: 3},
			},
			DataScaleDownStatus: &antflyv1.DataScaleDownStatus{
				Source:          antflyv1.DataScaleDownSourceManual,
				FromReplicas:    5,
				TargetReplicas:  4,
				AppliedReplicas: 4,
				DrainingOrdinal: 4,
				DrainingNodeID:  "5",
				Phase:           "Failed",
				Message:         "previous finalization failed",
			},
		},
	}
	dataReplicas := int32(4)
	dataSts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "retry-finalize-scale-data", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &dataReplicas},
		Status:     appsv1.StatefulSetStatus{Replicas: 4},
	}
	g.Expect(controllerutil.SetControllerReference(cluster, dataSts, s)).To(Succeed())
	client := fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(cluster).
		WithObjects(cluster, dataSts).
		Build()

	var methods []string
	reconciler := &AntflyClusterReconciler{
		Client: client,
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			methods = append(methods, req.Method)
			return &http.Response{
				StatusCode: http.StatusAccepted,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"status":"finalized"}`)),
				Request:    req,
			}, nil
		})},
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "retry-finalize-scale", Namespace: "default"}})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(result.RequeueAfter).To(Equal(1 * time.Second))
	g.Expect(methods).To(Equal([]string{http.MethodDelete}))

	updated := &antflyv1.AntflyCluster{}
	g.Expect(client.Get(ctx, types.NamespacedName{Name: "retry-finalize-scale", Namespace: "default"}, updated)).To(Succeed())
	g.Expect(updated.Status.DataScaleDownStatus).NotTo(BeNil())
	g.Expect(updated.Status.DataScaleDownStatus.Phase).To(Equal("Complete"))
}

func TestReconcileKeepsFailedDataScaleDownFinalizationFailure(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: antflyv1.GroupVersion.String(), Kind: "AntflyCluster"},
		ObjectMeta: metav1.ObjectMeta{Name: "failed-finalize-scale", Namespace: "default", Generation: 3},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:test",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas:    1,
				MetadataAPI: antflyv1.APISpec{Port: 12377},
			},
			DataNodes: antflyv1.DataNodesSpec{Replicas: 4},
			Storage: antflyv1.StorageSpec{
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			Config: "{}",
		},
		Status: antflyv1.AntflyClusterStatus{
			ObservedGeneration: 3,
			Conditions: []metav1.Condition{
				{Type: antflyv1.TypeConfigurationValid, Status: metav1.ConditionTrue, Reason: antflyv1.ReasonValidationPassed, ObservedGeneration: 3},
			},
			DataScaleDownStatus: &antflyv1.DataScaleDownStatus{
				Source:          antflyv1.DataScaleDownSourceManual,
				FromReplicas:    5,
				TargetReplicas:  4,
				AppliedReplicas: 4,
				DrainingOrdinal: 4,
				DrainingNodeID:  "5",
				Phase:           "Failed",
				Message:         "previous finalization failed",
			},
		},
	}
	dataReplicas := int32(4)
	dataSts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "failed-finalize-scale-data", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &dataReplicas},
		Status:     appsv1.StatefulSetStatus{Replicas: 4},
	}
	g.Expect(controllerutil.SetControllerReference(cluster, dataSts, s)).To(Succeed())
	client := fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(cluster).
		WithObjects(cluster, dataSts).
		Build()

	reconciler := &AntflyClusterReconciler{
		Client: client,
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("boom")),
				Request:    req,
			}, nil
		})},
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "failed-finalize-scale", Namespace: "default"}})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(result.RequeueAfter).To(Equal(30 * time.Second))

	updated := &antflyv1.AntflyCluster{}
	g.Expect(client.Get(ctx, types.NamespacedName{Name: "failed-finalize-scale", Namespace: "default"}, updated)).To(Succeed())
	g.Expect(updated.Status.DataScaleDownStatus).NotTo(BeNil())
	g.Expect(updated.Status.DataScaleDownStatus.Phase).To(Equal("Failed"))
	g.Expect(updated.Status.DataScaleDownStatus.Message).To(ContainSubstring("Failed to finalize data-node shutdown"))
}

func TestHTTPClientDefaultHasTimeout(t *testing.T) {
	g := NewWithT(t)
	reconciler := &AntflyClusterReconciler{}

	g.Expect(reconciler.httpClient().Timeout).To(Equal(10 * time.Second))
}

func TestUpdateProductTierStatusReportsClusteredShape(t *testing.T) {
	g := NewWithT(t)
	reconciler := &AntflyClusterReconciler{}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default", Generation: 9},
		Spec: antflyv1.AntflyClusterSpec{
			ProductTier: &antflyv1.ProductTierSpec{
				Name:         "pro",
				Revision:     "2026-05",
				ManagedBy:    "cloudaf",
				MetadataTier: "metadata-small",
				DataTier:     "data-large",
			},
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas: 3,
				Resources: antflyv1.ResourceSpec{
					CPU:    "500m",
					Memory: "1Gi",
				},
			},
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 5,
				Resources: antflyv1.ResourceSpec{
					CPU:    "2",
					Memory: "8Gi",
				},
				AutoScaling: &antflyv1.AutoScalingSpec{
					Enabled:     true,
					MinReplicas: 3,
					MaxReplicas: 8,
				},
			},
			Storage: antflyv1.StorageSpec{
				MetadataStorage: "5Gi",
				DataStorage:     "100Gi",
			},
		},
	}

	reconciler.updateProductTierStatus(cluster)

	g.Expect(cluster.Status.ProductTierStatus).NotTo(BeNil())
	g.Expect(cluster.Status.ProductTierStatus.Name).To(Equal("pro"))
	g.Expect(cluster.Status.ProductTierStatus.Mode).To(Equal(antflyv1.ClusterModeClustered))
	g.Expect(cluster.Status.ProductTierStatus.MetadataResources).To(Equal("cpu=500m memory=1Gi"))
	g.Expect(cluster.Status.ProductTierStatus.DataStorage).To(Equal("100Gi"))
	g.Expect(cluster.Status.ProductTierStatus.DataAutoscaling).To(Equal("enabled min=3 max=8"))
	g.Expect(cluster.Status.ProductTierStatus.ObservedGeneration).To(Equal(int64(9)))
}

// T006: Unit test for public API service deletion when disabled
func TestReconcileServices_DeletesPublicAPIWhenDisabled(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	err := antflyv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())
	err = corev1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())

	// Create a pre-existing public-api service to simulate the scenario
	existingSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-public-api",
			Namespace: "default",
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeLoadBalancer,
			Ports: []corev1.ServicePort{
				{Port: 80},
			},
		},
	}

	client := fake.NewClientBuilder().WithScheme(s).WithObjects(existingSvc).Build()

	reconciler := &AntflyClusterReconciler{
		Client: client,
		Scheme: s,
	}

	enabled := false
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas:     3,
				MetadataAPI:  antflyv1.APISpec{Port: 12377},
				MetadataRaft: antflyv1.APISpec{Port: 9017},
				Health:       antflyv1.APISpec{Port: 4200},
			},
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 3,
				API:      antflyv1.APISpec{Port: 12380},
				Raft:     antflyv1.APISpec{Port: 9021},
				Health:   antflyv1.APISpec{Port: 4200},
			},
			PublicAPI: &antflyv1.PublicAPIConfig{
				Enabled: &enabled,
			},
		},
	}

	// Verify the service exists before reconciliation
	svc := &corev1.Service{}
	err = client.Get(context.Background(), types.NamespacedName{
		Name:      "test-cluster-public-api",
		Namespace: "default",
	}, svc)
	g.Expect(err).NotTo(HaveOccurred(), "Service should exist before reconciliation")

	// Run reconcileServices
	err = reconciler.reconcileServices(context.Background(), cluster)
	g.Expect(err).NotTo(HaveOccurred())

	// Verify the public-api service has been deleted
	err = client.Get(context.Background(), types.NamespacedName{
		Name:      "test-cluster-public-api",
		Namespace: "default",
	}, svc)
	g.Expect(err).To(HaveOccurred(), "Service should be deleted")
	g.Expect(errors.IsNotFound(err)).To(BeTrue(), "Error should be NotFound")
}

// T007: Unit test for reconcileServices when no public-api service exists and publicAPI is disabled
func TestReconcileServices_NoErrorWhenPublicAPIDisabledAndNoService(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	err := antflyv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())
	err = corev1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())

	client := fake.NewClientBuilder().WithScheme(s).Build()

	reconciler := &AntflyClusterReconciler{
		Client: client,
		Scheme: s,
	}

	enabled := false
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas:     3,
				MetadataAPI:  antflyv1.APISpec{Port: 12377},
				MetadataRaft: antflyv1.APISpec{Port: 9017},
				Health:       antflyv1.APISpec{Port: 4200},
			},
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 3,
				API:      antflyv1.APISpec{Port: 12380},
				Raft:     antflyv1.APISpec{Port: 9021},
				Health:   antflyv1.APISpec{Port: 4200},
			},
			PublicAPI: &antflyv1.PublicAPIConfig{
				Enabled: &enabled,
			},
		},
	}

	// Should not error even when no public-api service exists
	err = reconciler.reconcileServices(context.Background(), cluster)
	g.Expect(err).NotTo(HaveOccurred())
}

// Integration tests using envtest
var _ = Describe("AntflyCluster Controller", func() {
	const (
		timeout  = time.Second * 30
		interval = time.Millisecond * 250
	)

	Context("When creating a basic AntflyCluster", func() {
		It("Should create StatefulSets, Services, and ConfigMap", func() {
			clusterName := "test-basic-cluster"
			namespace := "default"

			cluster := &antflyv1.AntflyCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: antflyv1.AntflyClusterSpec{
					Image: "antfly:latest",
					MetadataNodes: antflyv1.MetadataNodesSpec{
						Replicas: 3,
						Resources: antflyv1.ResourceSpec{
							CPU:    "500m",
							Memory: "512Mi",
						},
						MetadataAPI:  antflyv1.APISpec{Port: 12377},
						MetadataRaft: antflyv1.APISpec{Port: 9017},
						Health:       antflyv1.APISpec{Port: 4200},
					},
					DataNodes: antflyv1.DataNodesSpec{
						Replicas: 3,
						Resources: antflyv1.ResourceSpec{
							CPU:    "1000m",
							Memory: "2Gi",
						},
						API:    antflyv1.APISpec{Port: 12380},
						Raft:   antflyv1.APISpec{Port: 9021},
						Health: antflyv1.APISpec{Port: 4200},
					},
					Config: "{}",
					Storage: antflyv1.StorageSpec{
						StorageClass:    "standard",
						MetadataStorage: "1Gi",
						DataStorage:     "10Gi",
					},
				},
			}

			// Create the cluster
			Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

			// Verify metadata StatefulSet is created
			metadataSts := &appsv1.StatefulSet{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      clusterName + "-metadata",
					Namespace: namespace,
				}, metadataSts)
			}, timeout, interval).Should(Succeed())
			Expect(*metadataSts.Spec.Replicas).To(Equal(int32(3)))

			// Verify data StatefulSet is created
			dataSts := &appsv1.StatefulSet{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      clusterName + "-data",
					Namespace: namespace,
				}, dataSts)
			}, timeout, interval).Should(Succeed())
			Expect(*dataSts.Spec.Replicas).To(Equal(int32(3)))

			// Verify ConfigMap is created
			configMap := &corev1.ConfigMap{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      clusterName + "-config",
					Namespace: namespace,
				}, configMap)
			}, timeout, interval).Should(Succeed())
			Expect(configMap.Data).To(HaveKey("config.json"))

			// Verify internal service is created
			internalSvc := &corev1.Service{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      clusterName + "-metadata",
					Namespace: namespace,
				}, internalSvc)
			}, timeout, interval).Should(Succeed())

			// Cleanup
			Expect(k8sClient.Delete(ctx, cluster)).To(Succeed())
		})
	})

	Context("When creating a cluster with service mesh enabled", func() {
		It("Should apply mesh annotations to pod templates", func() {
			clusterName := "mesh-cluster"
			namespace := "default"

			cluster := &antflyv1.AntflyCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: antflyv1.AntflyClusterSpec{
					Image: "antfly:latest",
					MetadataNodes: antflyv1.MetadataNodesSpec{
						Replicas: 3,
						Resources: antflyv1.ResourceSpec{
							CPU:    "500m",
							Memory: "512Mi",
						},
						MetadataAPI:  antflyv1.APISpec{Port: 12377},
						MetadataRaft: antflyv1.APISpec{Port: 9017},
						Health:       antflyv1.APISpec{Port: 4200},
					},
					DataNodes: antflyv1.DataNodesSpec{
						Replicas: 3,
						Resources: antflyv1.ResourceSpec{
							CPU:    "1000m",
							Memory: "2Gi",
						},
						API:    antflyv1.APISpec{Port: 12380},
						Raft:   antflyv1.APISpec{Port: 9021},
						Health: antflyv1.APISpec{Port: 4200},
					},
					Config: "{}",
					Storage: antflyv1.StorageSpec{
						StorageClass:    "standard",
						MetadataStorage: "1Gi",
						DataStorage:     "10Gi",
					},
					ServiceMesh: &antflyv1.ServiceMeshSpec{
						Enabled: true,
						Annotations: map[string]string{
							"sidecar.istio.io/inject": "true",
						},
					},
				},
			}

			// Create the cluster
			Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

			// Verify metadata StatefulSet has mesh annotations
			metadataSts := &appsv1.StatefulSet{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      clusterName + "-metadata",
					Namespace: namespace,
				}, metadataSts)
			}, timeout, interval).Should(Succeed())

			// Check pod template annotations include mesh annotation
			Expect(metadataSts.Spec.Template.Annotations).To(HaveKeyWithValue(
				"sidecar.istio.io/inject", "true",
			))

			// Verify data StatefulSet has mesh annotations
			dataSts := &appsv1.StatefulSet{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      clusterName + "-data",
					Namespace: namespace,
				}, dataSts)
			}, timeout, interval).Should(Succeed())

			Expect(dataSts.Spec.Template.Annotations).To(HaveKeyWithValue(
				"sidecar.istio.io/inject", "true",
			))

			// Cleanup
			Expect(k8sClient.Delete(ctx, cluster)).To(Succeed())
		})
	})
})

// TestApplySchedulingConstraints verifies tolerations, nodeSelector, affinity, and topologySpreadConstraints
func TestApplySchedulingConstraints(t *testing.T) {
	g := NewWithT(t)

	t.Run("applies tolerations", func(t *testing.T) {
		g := NewWithT(t)
		podTemplate := &corev1.PodTemplateSpec{}
		tolerations := []corev1.Toleration{
			{
				Key:      "dedicated",
				Operator: corev1.TolerationOpEqual,
				Value:    "antfly",
				Effect:   corev1.TaintEffectNoSchedule,
			},
		}

		applySchedulingConstraints(podTemplate, tolerations, nil, nil, nil)

		g.Expect(podTemplate.Spec.Tolerations).To(HaveLen(1))
		g.Expect(podTemplate.Spec.Tolerations[0].Key).To(Equal("dedicated"))
		g.Expect(podTemplate.Spec.Tolerations[0].Value).To(Equal("antfly"))
	})

	t.Run("merges nodeSelector", func(t *testing.T) {
		g := NewWithT(t)
		podTemplate := &corev1.PodTemplateSpec{}
		nodeSelector := map[string]string{
			"node-pool": "antfly",
			"disk-type": "ssd",
		}

		applySchedulingConstraints(podTemplate, nil, nodeSelector, nil, nil)

		g.Expect(podTemplate.Spec.NodeSelector).To(HaveLen(2))
		g.Expect(podTemplate.Spec.NodeSelector["node-pool"]).To(Equal("antfly"))
		g.Expect(podTemplate.Spec.NodeSelector["disk-type"]).To(Equal("ssd"))
	})

	t.Run("applies affinity", func(t *testing.T) {
		g := NewWithT(t)
		podTemplate := &corev1.PodTemplateSpec{}
		affinity := &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "topology.kubernetes.io/zone",
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{"us-east-1a", "us-east-1b"},
								},
							},
						},
					},
				},
			},
			PodAntiAffinity: &corev1.PodAntiAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
					{
						Weight: 100,
						PodAffinityTerm: corev1.PodAffinityTerm{
							TopologyKey: "kubernetes.io/hostname",
						},
					},
				},
			},
		}

		applySchedulingConstraints(podTemplate, nil, nil, affinity, nil)

		g.Expect(podTemplate.Spec.Affinity).ToNot(BeNil())
		g.Expect(podTemplate.Spec.Affinity.NodeAffinity).ToNot(BeNil())
		g.Expect(podTemplate.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution).ToNot(BeNil())
		g.Expect(podTemplate.Spec.Affinity.PodAntiAffinity).ToNot(BeNil())
	})

	t.Run("applies topologySpreadConstraints", func(t *testing.T) {
		g := NewWithT(t)
		podTemplate := &corev1.PodTemplateSpec{}
		maxSkew := int32(1)
		constraints := []corev1.TopologySpreadConstraint{
			{
				MaxSkew:           maxSkew,
				TopologyKey:       "topology.kubernetes.io/zone",
				WhenUnsatisfiable: corev1.ScheduleAnyway,
			},
		}

		applySchedulingConstraints(podTemplate, nil, nil, nil, constraints)

		g.Expect(podTemplate.Spec.TopologySpreadConstraints).To(HaveLen(1))
		g.Expect(podTemplate.Spec.TopologySpreadConstraints[0].TopologyKey).To(Equal("topology.kubernetes.io/zone"))
	})

	t.Run("merges with existing affinity", func(t *testing.T) {
		g = NewWithT(t)
		// Simulate existing affinity (as EKS instance type would set)
		podTemplate := &corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Affinity: &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{
						PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{
							{
								Weight: 100,
								Preference: corev1.NodeSelectorTerm{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{
											Key:      "node.kubernetes.io/instance-type",
											Operator: corev1.NodeSelectorOpIn,
											Values:   []string{"m5.large"},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		// User adds required node affinity
		userAffinity := &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "topology.kubernetes.io/zone",
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{"us-east-1a"},
								},
							},
						},
					},
				},
			},
		}

		applySchedulingConstraints(podTemplate, nil, nil, userAffinity, nil)

		// Both should be preserved
		g.Expect(podTemplate.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution).To(HaveLen(1))
		g.Expect(podTemplate.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution).ToNot(BeNil())
	})

	t.Run("combines all fields", func(t *testing.T) {
		g = NewWithT(t)
		podTemplate := &corev1.PodTemplateSpec{}
		tolerations := []corev1.Toleration{
			{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "antfly", Effect: corev1.TaintEffectNoSchedule},
		}
		nodeSelector := map[string]string{"node-pool": "antfly"}
		affinity := &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
					{Weight: 100, PodAffinityTerm: corev1.PodAffinityTerm{TopologyKey: "kubernetes.io/hostname"}},
				},
			},
		}
		maxSkew := int32(1)
		constraints := []corev1.TopologySpreadConstraint{
			{MaxSkew: maxSkew, TopologyKey: "topology.kubernetes.io/zone", WhenUnsatisfiable: corev1.ScheduleAnyway},
		}

		applySchedulingConstraints(podTemplate, tolerations, nodeSelector, affinity, constraints)

		g.Expect(podTemplate.Spec.Tolerations).To(HaveLen(1))
		g.Expect(podTemplate.Spec.NodeSelector).To(HaveLen(1))
		g.Expect(podTemplate.Spec.Affinity).ToNot(BeNil())
		g.Expect(podTemplate.Spec.TopologySpreadConstraints).To(HaveLen(1))
	})
}

// TestGenerateCompleteConfig verifies orchestration URLs use 0-based pod indexing
func TestGenerateCompleteConfig(t *testing.T) {
	g := NewWithT(t)

	// Setup scheme
	s := runtime.NewScheme()
	err := antflyv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())

	// Create reconciler
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).Build(),
		Scheme: s,
	}

	// Create cluster with 3 metadata replicas
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Config: "{}",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas: 3,
				MetadataAPI: antflyv1.APISpec{
					Port: 12377,
				},
			},
		},
	}

	// Generate configuration
	configJSON, err := reconciler.generateCompleteConfig(cluster)
	g.Expect(err).NotTo(HaveOccurred())

	// Parse the generated config to verify orchestration URLs
	var config map[string]any
	err = json.Unmarshal([]byte(configJSON), &config)
	g.Expect(err).NotTo(HaveOccurred())

	// Verify metadata section exists with orchestration_urls
	metadata, ok := config["metadata"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "metadata section should exist")

	orchestrationURLs, ok := metadata["orchestration_urls"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "orchestration_urls should exist")

	// Verify correct 0-based pod indexing for StatefulSet pods
	// ID "1" should map to metadata-0
	url1, ok := orchestrationURLs["1"].(string)
	g.Expect(ok).To(BeTrue())
	g.Expect(url1).To(ContainSubstring("test-cluster-metadata-0"),
		"ID 1 should map to metadata-0 (0-based indexing)")

	// ID "2" should map to metadata-1
	url2, ok := orchestrationURLs["2"].(string)
	g.Expect(ok).To(BeTrue())
	g.Expect(url2).To(ContainSubstring("test-cluster-metadata-1"),
		"ID 2 should map to metadata-1 (0-based indexing)")

	// ID "3" should map to metadata-2
	url3, ok := orchestrationURLs["3"].(string)
	g.Expect(ok).To(BeTrue())
	g.Expect(url3).To(ContainSubstring("test-cluster-metadata-2"),
		"ID 3 should map to metadata-2 (0-based indexing)")

	// Verify the full URL format
	expectedURL := "http://test-cluster-metadata-0.test-cluster-metadata.default.svc.cluster.local:12377"
	g.Expect(url1).To(Equal(expectedURL))
	g.Expect(config["default_shards_per_table"]).To(Equal(float64(1)))
	g.Expect(config["max_shards_per_table"]).To(Equal(float64(0)))
}

func TestGenerateCompleteConfig_Standalone(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	err := antflyv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())

	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).Build(),
		Scheme: s,
	}

	cluster := baseStandaloneControllerCluster()
	cluster.Spec.Config = `{
		  "replication_factor": 3,
		  "deployment_mode": "distributed"
		}`

	configJSON, err := reconciler.generateCompleteConfig(cluster)
	g.Expect(err).NotTo(HaveOccurred())

	var config map[string]any
	err = json.Unmarshal([]byte(configJSON), &config)
	g.Expect(err).NotTo(HaveOccurred())

	g.Expect(config["deployment_mode"]).To(Equal("standalone"))
	g.Expect(config["replication_factor"]).To(Equal(float64(1)))
	g.Expect(config["default_shards_per_table"]).To(Equal(float64(1)))
	g.Expect(config["disable_shard_alloc"]).To(Equal(true))

	storage, ok := config["storage"].(map[string]any)
	g.Expect(ok).To(BeTrue())
	g.Expect(storage["engine"]).To(Equal("local"))
	localStorage, ok := storage["local"].(map[string]any)
	g.Expect(ok).To(BeTrue())
	g.Expect(localStorage["base_dir"]).To(Equal("/antflydb"))

	metadata, ok := config["metadata"].(map[string]any)
	g.Expect(ok).To(BeTrue())
	orchestrationURLs, ok := metadata["orchestration_urls"].(map[string]any)
	g.Expect(ok).To(BeTrue())
	g.Expect(orchestrationURLs["1"]).To(Equal("http://test-standalone-standalone.default.svc.cluster.local:8080"))
}

func TestGenerateCompleteConfig_StandaloneRejectsRawStorageOverride(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).Build(),
		Scheme: s,
	}
	cluster := baseStandaloneControllerCluster()
	cluster.Spec.Config = `{"storage":{"engine":"lite","lite":{"path":"/antflydb/data.aflite"}}}`

	_, err := reconciler.generateCompleteConfig(cluster)
	g.Expect(err).To(MatchError(ContainSubstring("spec.config.storage is operator-managed")))
}

func TestGenerateCompleteConfig_RejectsInvalidTypedStorageWithoutAdmission(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	reconciler := &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(s).Build(), Scheme: s}
	cluster := baseStandaloneControllerCluster()
	cluster.Spec.Storage.Engine = "object"

	_, err := reconciler.generateCompleteConfig(cluster)
	g.Expect(err).To(MatchError(ContainSubstring("spec.storage.engine must be local or lite")))
}

func TestGenerateCompleteConfig_StandaloneLite(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	reconciler := &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(s).Build(), Scheme: s}
	cluster := baseStandaloneControllerCluster()
	cluster.Spec.Storage.Engine = "lite"
	cluster.Spec.Storage.LiteFileName = "production.aflite"

	configJSON, err := reconciler.generateCompleteConfig(cluster)
	g.Expect(err).NotTo(HaveOccurred())
	var config map[string]any
	g.Expect(json.Unmarshal([]byte(configJSON), &config)).To(Succeed())
	storage := config["storage"].(map[string]any)
	g.Expect(storage["engine"]).To(Equal("lite"))
	lite := storage["lite"].(map[string]any)
	g.Expect(lite["path"]).To(Equal("/antflydb/production.aflite"))
	g.Expect(lite["fsync"]).To(BeTrue())
}

func TestGenerateCompleteConfig_ManagedInferenceAPIURL(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	err := antflyv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())

	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).Build(),
		Scheme: s,
	}

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Config: "{}",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas:    1,
				MetadataAPI: antflyv1.APISpec{Port: 12377},
			},
			Inference: &antflyv1.AntflyInferenceSpec{
				Mode: antflyv1.AntflyInferenceModeManaged,
				ManagedPools: []antflyv1.ManagedInferencePoolSpec{{
					Name: "antfly-inference-default",
					Spec: inferencev1alpha1.InferencePoolSpec{
						Models:   inferencev1alpha1.ModelConfig{},
						Replicas: inferencev1alpha1.ReplicaConfig{Min: 1, Max: 1},
						Hardware: inferencev1alpha1.HardwareConfig{},
					},
				}},
			},
		},
	}

	configJSON, err := reconciler.generateCompleteConfig(cluster)
	g.Expect(err).NotTo(HaveOccurred())

	var config map[string]any
	err = json.Unmarshal([]byte(configJSON), &config)
	g.Expect(err).NotTo(HaveOccurred())
	inferenceConfig, ok := config["inference"].(map[string]any)
	g.Expect(ok).To(BeTrue())
	g.Expect(inferenceConfig["api_url"]).To(Equal("http://antfly-inference-default.default.svc.cluster.local:8080"))

	cluster.Spec.Config = `{"inference":{"api_url":"https://custom.example/ai/v1","models_dir":"/models"}}`
	configJSON, err = reconciler.generateCompleteConfig(cluster)
	g.Expect(err).NotTo(HaveOccurred())
	err = json.Unmarshal([]byte(configJSON), &config)
	g.Expect(err).NotTo(HaveOccurred())
	inferenceConfig = config["inference"].(map[string]any)
	g.Expect(inferenceConfig["api_url"]).To(Equal("https://custom.example/ai/v1"))
	g.Expect(inferenceConfig["models_dir"]).To(Equal("/models"))
}

func TestReconcileServices_StandaloneCreatesStandaloneAndPublicAPI(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	err := antflyv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())
	err = corev1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())

	cluster := baseStandaloneControllerCluster()
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build()

	reconciler := &AntflyClusterReconciler{
		Client: client,
		Scheme: s,
	}

	err = reconciler.reconcileServices(context.Background(), cluster)
	g.Expect(err).NotTo(HaveOccurred())

	publicSvc := &corev1.Service{}
	err = client.Get(context.Background(), types.NamespacedName{Name: "test-standalone-public-api", Namespace: "default"}, publicSvc)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(publicSvc.Spec.Selector).To(HaveKeyWithValue("app.kubernetes.io/component", "standalone"))
	g.Expect(publicSvc.Spec.Ports).To(HaveLen(1))
	g.Expect(publicSvc.Spec.Ports[0].TargetPort.IntValue()).To(Equal(8080))

	standaloneSvc := &corev1.Service{}
	err = client.Get(context.Background(), types.NamespacedName{Name: "test-standalone-standalone", Namespace: "default"}, standaloneSvc)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(standaloneSvc.Spec.Ports).To(HaveLen(5))

	err = client.Get(context.Background(), types.NamespacedName{Name: "test-standalone-metadata", Namespace: "default"}, &corev1.Service{})
	g.Expect(errors.IsNotFound(err)).To(BeTrue())
	err = client.Get(context.Background(), types.NamespacedName{Name: "test-standalone-data", Namespace: "default"}, &corev1.Service{})
	g.Expect(errors.IsNotFound(err)).To(BeTrue())
}

func TestCreatePublicAPIService_ClusteredTargetsMetadataAPI(t *testing.T) {
	g := NewWithT(t)

	enabled := true
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			MetadataNodes: antflyv1.MetadataNodesSpec{
				MetadataAPI: antflyv1.APISpec{Port: 12377},
			},
			DataNodes: antflyv1.DataNodesSpec{
				API: antflyv1.APISpec{Port: 12380},
			},
			PublicAPI: &antflyv1.PublicAPIConfig{
				Enabled: &enabled,
				Port:    80,
			},
		},
	}

	reconciler := &AntflyClusterReconciler{}
	svc := reconciler.createPublicAPIService(cluster, false)

	g.Expect(svc).ToNot(BeNil())
	g.Expect(svc.Spec.Selector).To(HaveKeyWithValue("app.kubernetes.io/component", "metadata"))
	g.Expect(svc.Spec.Ports).To(HaveLen(1))
	g.Expect(svc.Spec.Ports[0].TargetPort.IntValue()).To(Equal(12377))
}

func TestReconcileServices_PublicAPIUsesHAPromotedRouteSelector(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := baseStandaloneControllerCluster()
	cluster.Spec.HighAvailability = &antflyv1.HighAvailabilitySpec{
		Mode: antflyv1.HAModeHotStandby,
		Standbys: []antflyv1.HAStandbySpec{{
			Name: "standby-a",
			RouteSelector: map[string]string{
				"app.kubernetes.io/name":      "antfly-database",
				"app.kubernetes.io/component": "standby-a",
				"app.kubernetes.io/instance":  "test-standalone-standby-a",
			},
		}},
	}
	cluster.Status.HAStatus = &antflyv1.HAStatus{
		Mode: antflyv1.HAModeHotStandby,
		PrimaryRoute: antflyv1.HAPrimaryRouteStatus{
			CurrentTarget:   "standby-a",
			FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
			FenceGeneration: 7,
		},
	}
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build()
	reconciler := &AntflyClusterReconciler{Client: client, Scheme: s}

	g.Expect(reconciler.reconcileServices(context.Background(), cluster)).To(Succeed())

	publicSvc := &corev1.Service{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: "test-standalone-public-api", Namespace: "default"}, publicSvc)).To(Succeed())
	g.Expect(publicSvc.Spec.Selector).To(Equal(cluster.Spec.HighAvailability.Standbys[0].RouteSelector))
	g.Expect(publicSvc.Annotations).To(HaveKeyWithValue(haPrimaryRouteTargetAnnotation, "standby-a"))
	g.Expect(publicSvc.Annotations).To(HaveKeyWithValue(haPrimaryRouteFenceAuthorityAnnotation, string(antflyv1.HAFencingAuthorityKubernetesLease)))
	g.Expect(publicSvc.Annotations).To(HaveKeyWithValue(haPrimaryRouteFenceGenerationAnnotation, "7"))
	g.Expect(publicSvc.Annotations).To(HaveKeyWithValue(haPrimaryRouteSelectorAnnotation, "true"))

	cluster.Status.HAStatus.PrimaryRoute = antflyv1.HAPrimaryRouteStatus{}
	g.Expect(reconciler.observeHAPrimaryRouteStatus(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.CurrentTarget).To(Equal("standby-a"))
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.FenceAuthority).To(Equal(antflyv1.HAFencingAuthorityKubernetesLease))
	g.Expect(cluster.Status.HAStatus.PrimaryRoute.FenceGeneration).To(Equal(uint64(7)))
}

func TestReconcileServices_PublicAPIClearsStaleHARouteAnnotationsWhenHAManagementDisabled(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := baseStandaloneControllerCluster()
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-standalone-public-api",
			Namespace: "default",
			Annotations: map[string]string{
				haPrimaryRouteTargetAnnotation:          "standby-a",
				haPrimaryRouteFenceAuthorityAnnotation:  string(antflyv1.HAFencingAuthorityKubernetesLease),
				haPrimaryRouteFenceGenerationAnnotation: "7",
				haPrimaryRouteSelectorAnnotation:        "true",
				"antfly.io/custom":                      "preserve",
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app.kubernetes.io/component": "standby-a",
			},
		},
	}
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster, service).Build()
	reconciler := &AntflyClusterReconciler{Client: client, Scheme: s}

	g.Expect(reconciler.reconcileServices(context.Background(), cluster)).To(Succeed())

	publicSvc := &corev1.Service{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: "test-standalone-public-api", Namespace: "default"}, publicSvc)).To(Succeed())
	g.Expect(publicSvc.Spec.Selector).To(HaveKeyWithValue("app.kubernetes.io/component", "standalone"))
	g.Expect(publicSvc.Annotations).To(HaveKeyWithValue("antfly.io/custom", "preserve"))
	g.Expect(publicSvc.Annotations).NotTo(HaveKey(haPrimaryRouteTargetAnnotation))
	g.Expect(publicSvc.Annotations).NotTo(HaveKey(haPrimaryRouteFenceAuthorityAnnotation))
	g.Expect(publicSvc.Annotations).NotTo(HaveKey(haPrimaryRouteFenceGenerationAnnotation))
	g.Expect(publicSvc.Annotations).NotTo(HaveKey(haPrimaryRouteSelectorAnnotation))
}

func TestReconcileStandaloneStatefulSetMountsSecretStore(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := baseStandaloneControllerCluster()
	cluster.Spec.SecretStore = &antflyv1.SecretStoreSpec{
		SecretName: "cloud-secrets-config",
		Key:        "secrets.json",
		Path:       "/run/antfly/secrets/secrets.json",
	}
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build()

	reconciler := &AntflyClusterReconciler{
		Client: client,
		Scheme: s,
	}

	err := reconciler.reconcileStandaloneStatefulSet(context.Background(), &envFromCache{}, cluster)
	g.Expect(err).NotTo(HaveOccurred())

	sts := &appsv1.StatefulSet{}
	err = client.Get(context.Background(), types.NamespacedName{Name: "test-standalone-standalone", Namespace: "default"}, sts)
	g.Expect(err).NotTo(HaveOccurred())

	container := sts.Spec.Template.Spec.Containers[0]
	g.Expect(container.Args).To(HaveLen(1))
	g.Expect(container.Args[0]).To(ContainSubstring("--secret-store-path '/run/antfly/secrets/secrets.json'"))
	g.Expect(container.Env).To(ContainElement(corev1.EnvVar{
		Name:  antflySecretStoreEnvVar,
		Value: "/run/antfly/secrets/secrets.json",
	}))
	g.Expect(container.VolumeMounts).To(ContainElement(corev1.VolumeMount{
		Name:      antflySecretStoreVolumeName,
		MountPath: "/run/antfly/secrets",
		ReadOnly:  true,
	}))
	g.Expect(sts.Spec.Template.Spec.Volumes).To(ContainElement(corev1.Volume{
		Name: antflySecretStoreVolumeName,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: "cloud-secrets-config",
				Items: []corev1.KeyToPath{{
					Key:  "secrets.json",
					Path: "secrets.json",
				}},
			},
		},
	}))
}

func TestReconcileStandaloneStatefulSetAddsHARuntimeArgs(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := baseStandaloneControllerCluster()
	cluster.Spec.HighAvailability = &antflyv1.HighAvailabilitySpec{
		Mode: antflyv1.HAModeHotStandby,
		Identity: &antflyv1.HAReplicationIdentitySpec{
			ClusterID:        100,
			ShardID:          10,
			TableID:          20,
			TimelineID:       1,
			Epoch:            2,
			CurrentPrimaryID: "primary-a",
		},
		Runtime: &antflyv1.HARuntimeSpec{
			Role:             antflyv1.HARuntimeRolePrimary,
			NodeID:           "primary-a",
			AdminTokenEnvVar: "ANTFLY_HA_ADMIN_TOKEN",
			AdminTokenSecretRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "ha-admin-token"},
				Key:                  "token",
			},
		},
		Retention: &antflyv1.HARetentionPolicy{
			MaxLagLSN:        50,
			MaxRetainedBytes: 4096,
			MaxRetainedAgeNS: 1000000,
		},
		SyncPolicy: &antflyv1.HASyncPolicy{
			Mode:          antflyv1.HADurabilityModeRemoteApply,
			Selection:     antflyv1.HAStandbySelectionFirst,
			Required:      2,
			StandbyNames:  []string{"standby-a", "standby-b"},
			FailurePolicy: antflyv1.HAFailurePolicyFailClosed,
		},
	}
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build()
	reconciler := &AntflyClusterReconciler{Client: client, Scheme: s}

	g.Expect(reconciler.reconcileStandaloneStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())
	sts := &appsv1.StatefulSet{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: "test-standalone-standalone", Namespace: "default"}, sts)).To(Succeed())
	primaryArgs := sts.Spec.Template.Spec.Containers[0].Args[0]
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-primary-log '/antflydb/ha/primary.wal'`))
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-primary-slots '/antflydb/ha/slots'`))
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-primary-node-id 'primary-a'`))
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-fence-wal '/antflydb/ha/fence.wal'`))
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-former-primary-log '/antflydb/ha/primary.wal'`))
	g.Expect(primaryArgs).To(ContainSubstring(`--admin-token-env 'ANTFLY_HA_ADMIN_TOKEN'`))
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-retention-max-lag-lsn 50`))
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-retention-max-retained-bytes 4096`))
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-retention-max-retained-age-ns 1000000`))
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-cluster-id 100`))
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-shard-id 10`))
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-table-id 20`))
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-timeline-id 1`))
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-epoch 2`))
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-sync-mode 'remote-apply'`))
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-sync-selection 'first'`))
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-sync-required 2`))
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-sync-standby 'standby-a'`))
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-sync-standby 'standby-b'`))
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-sync-failure 'fail-closed'`))
	optionalFalse := false
	g.Expect(sts.Spec.Template.Spec.Containers[0].Env).To(ContainElement(corev1.EnvVar{
		Name: "ANTFLY_HA_ADMIN_TOKEN",
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "ha-admin-token"},
				Key:                  "token",
				Optional:             &optionalFalse,
			},
		},
	}))

	cluster.Spec.HighAvailability.Runtime = &antflyv1.HARuntimeSpec{
		Role:                 antflyv1.HARuntimeRoleStandby,
		NodeID:               "standby-a",
		FencePath:            "/antflydb/custom/fence.wal",
		FormerPrimaryLogPath: "/antflydb/custom/former-primary.wal",
		AdminTokenEnvVar:     "CUSTOM_HA_ADMIN_TOKEN",
		AdminTokenSecretRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "custom-ha-admin-token"},
			Key:                  "custom-token",
		},
		Standby: &antflyv1.HAStandbyRuntimeSpec{
			LogPath:      "/antflydb/custom/standby.wal",
			ProgressPath: "/antflydb/custom/progress.wal",
			UpstreamURL:  "http://primary.default.svc:8080",
			SlotName:     "standby-a",
		},
	}
	g.Expect(reconciler.reconcileStandaloneStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: "test-standalone-standalone", Namespace: "default"}, sts)).To(Succeed())
	standbyArgs := sts.Spec.Template.Spec.Containers[0].Args[0]
	g.Expect(standbyArgs).To(ContainSubstring(`--ha-standby-log '/antflydb/custom/standby.wal'`))
	g.Expect(standbyArgs).To(ContainSubstring(`--ha-standby-progress '/antflydb/custom/progress.wal'`))
	g.Expect(standbyArgs).To(ContainSubstring(`--ha-standby-node-id 'standby-a'`))
	g.Expect(standbyArgs).To(ContainSubstring(`--ha-fence-wal '/antflydb/custom/fence.wal'`))
	g.Expect(standbyArgs).To(ContainSubstring(`--ha-former-primary-log '/antflydb/custom/former-primary.wal'`))
	g.Expect(standbyArgs).To(ContainSubstring(`--admin-token-env 'CUSTOM_HA_ADMIN_TOKEN'`))
	g.Expect(standbyArgs).NotTo(ContainSubstring(`--ha-retention-max-lag-lsn`))
	g.Expect(standbyArgs).NotTo(ContainSubstring(`--ha-retention-max-retained-bytes`))
	g.Expect(standbyArgs).NotTo(ContainSubstring(`--ha-retention-max-retained-age-ns`))
	g.Expect(standbyArgs).To(ContainSubstring(`--ha-standby-upstream-url 'http://primary.default.svc:8080'`))
	g.Expect(standbyArgs).To(ContainSubstring(`--ha-standby-slot 'standby-a'`))
	g.Expect(sts.Spec.Template.Spec.Containers[0].Env).To(ContainElement(corev1.EnvVar{
		Name: "CUSTOM_HA_ADMIN_TOKEN",
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "custom-ha-admin-token"},
				Key:                  "custom-token",
				Optional:             &optionalFalse,
			},
		},
	}))
}

func TestStandaloneHAArgsOmitsRequiredForAllSyncPolicy(t *testing.T) {
	g := NewWithT(t)

	args := standaloneHAArgs(&antflyv1.HighAvailabilitySpec{
		Mode: antflyv1.HAModeHotStandby,
		Identity: &antflyv1.HAReplicationIdentitySpec{
			ClusterID:        100,
			TimelineID:       1,
			Epoch:            2,
			CurrentPrimaryID: "primary-a",
		},
		Runtime: &antflyv1.HARuntimeSpec{
			Role:   antflyv1.HARuntimeRolePrimary,
			NodeID: "primary-a",
		},
		SyncPolicy: &antflyv1.HASyncPolicy{
			Mode:         antflyv1.HADurabilityModeRemoteApply,
			Selection:    antflyv1.HAStandbySelectionAll,
			StandbyNames: []string{"standby-a", "standby-b"},
		},
	})

	g.Expect(args).To(ContainSubstring(`--ha-sync-mode 'remote-apply'`))
	g.Expect(args).To(ContainSubstring(`--ha-sync-selection 'all'`))
	g.Expect(args).To(ContainSubstring(`--ha-sync-standby 'standby-a'`))
	g.Expect(args).To(ContainSubstring(`--ha-sync-standby 'standby-b'`))
	g.Expect(args).NotTo(ContainSubstring(`--ha-sync-required`))
}

func TestStandaloneHAArgsShellQuotesRuntimeValues(t *testing.T) {
	g := NewWithT(t)

	args := standaloneHAArgs(&antflyv1.HighAvailabilitySpec{
		Mode: antflyv1.HAModeHotStandby,
		Identity: &antflyv1.HAReplicationIdentitySpec{
			ClusterID:        100,
			TimelineID:       1,
			Epoch:            2,
			CurrentPrimaryID: "primary-a",
		},
		Runtime: &antflyv1.HARuntimeSpec{
			Role:                 antflyv1.HARuntimeRolePrimary,
			NodeID:               "primary-$(touch /tmp/pwned)`x`",
			FormerPrimaryLogPath: "/antflydb/ha/'former.wal",
		},
		SyncPolicy: &antflyv1.HASyncPolicy{
			Mode:         antflyv1.HADurabilityModeRemoteWrite,
			StandbyNames: []string{"standby-$(touch /tmp/pwned)"},
		},
	})

	g.Expect(args).To(ContainSubstring(`--ha-primary-node-id 'primary-$(touch /tmp/pwned)` + "`" + `x` + "`" + `'`))
	g.Expect(args).To(ContainSubstring(`--ha-former-primary-log '/antflydb/ha/'\''former.wal'`))
	g.Expect(args).To(ContainSubstring(`--ha-sync-standby 'standby-$(touch /tmp/pwned)'`))
	g.Expect(args).NotTo(ContainSubstring(`"primary-$(touch /tmp/pwned)`))
	g.Expect(args).NotTo(ContainSubstring(`"standby-$(touch /tmp/pwned)"`))
}

func TestSecretStoreArgShellQuotesPath(t *testing.T) {
	g := NewWithT(t)

	arg := secretStoreArg(&antflyv1.SecretStoreSpec{Path: "/run/antfly/'secrets$(touch /tmp/pwned)"})

	g.Expect(arg).To(ContainSubstring(`--secret-store-path '/run/antfly/'\''secrets$(touch /tmp/pwned)'`))
	g.Expect(arg).NotTo(ContainSubstring(`"/run/antfly/'secrets$(touch /tmp/pwned)"`))
}

func TestReconcileSplitStatefulSetsMountSecretStore(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-split",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas:     1,
				MetadataAPI:  antflyv1.APISpec{Port: 12377},
				MetadataRaft: antflyv1.APISpec{Port: 9017},
				Health:       antflyv1.APISpec{Port: 4200},
			},
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 1,
				API:      antflyv1.APISpec{Port: 12380},
				Raft:     antflyv1.APISpec{Port: 9021},
				Health:   antflyv1.APISpec{Port: 4201},
			},
			Storage: antflyv1.StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "1Gi",
			},
			SecretStore: &antflyv1.SecretStoreSpec{
				SecretName: "cloud-secrets-config",
				Key:        "secrets.json",
				Path:       "/run/antfly/secrets/secrets.json",
			},
			Config: "{}",
		},
	}
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).Build()
	reconciler := &AntflyClusterReconciler{
		Client: client,
		Scheme: s,
	}

	g.Expect(reconciler.reconcileMetadataStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())
	g.Expect(reconciler.reconcileDataStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())

	for _, component := range []string{"metadata", "data"} {
		sts := &appsv1.StatefulSet{}
		key := types.NamespacedName{Name: cluster.Name + "-" + component, Namespace: cluster.Namespace}
		g.Expect(client.Get(context.Background(), key, sts)).To(Succeed())

		container := sts.Spec.Template.Spec.Containers[0]
		g.Expect(container.Args).To(HaveLen(1))
		g.Expect(container.Args[0]).To(ContainSubstring("--secret-store-path '/run/antfly/secrets/secrets.json'"))
		g.Expect(container.Env).To(ContainElement(corev1.EnvVar{
			Name:  antflySecretStoreEnvVar,
			Value: "/run/antfly/secrets/secrets.json",
		}))
		g.Expect(container.VolumeMounts).To(ContainElement(corev1.VolumeMount{
			Name:      antflySecretStoreVolumeName,
			MountPath: "/run/antfly/secrets",
			ReadOnly:  true,
		}))
		g.Expect(sts.Spec.Template.Spec.Volumes).To(ContainElement(corev1.Volume{
			Name: antflySecretStoreVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: "cloud-secrets-config",
					Items: []corev1.KeyToPath{{
						Key:  "secrets.json",
						Path: "secrets.json",
					}},
				},
			},
		}))
	}
}

func TestUpdateStatus_Standalone(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	err := antflyv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())
	err = appsv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())
	err = corev1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())

	cluster := baseStandaloneControllerCluster()
	standaloneSts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-standalone-standalone",
			Namespace: "default",
		},
		Status: appsv1.StatefulSetStatus{
			ReadyReplicas: 1,
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-standalone-standalone-0",
			Namespace: "default",
			Labels:    serviceSelectorLabels("test-standalone", "standalone"),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.10",
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(cluster).
		WithObjects(cluster, standaloneSts, pod).
		Build()

	reconciler := &AntflyClusterReconciler{
		Client: client,
		Scheme: s,
	}

	err = reconciler.updateStatus(context.Background(), cluster)
	g.Expect(err).NotTo(HaveOccurred())

	updated := &antflyv1.AntflyCluster{}
	err = client.Get(context.Background(), types.NamespacedName{Name: "test-standalone", Namespace: "default"}, updated)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(updated.Status.Mode).To(Equal(antflyv1.ClusterModeStandalone))
	g.Expect(updated.Status.ReadyReplicas).To(Equal(int32(1)))
	g.Expect(updated.Status.StandaloneNodesReady).To(Equal(int32(1)))
	g.Expect(updated.Status.Phase).To(Equal("Running"))
	g.Expect(updated.Status.StandaloneStatus).ToNot(BeNil())
	g.Expect(updated.Status.StandaloneStatus.Ready).To(BeTrue())
	g.Expect(updated.Status.StandaloneStatus.PodName).To(Equal("test-standalone-standalone-0"))
	g.Expect(updated.Status.StandaloneStatus.PodIP).To(Equal("10.0.0.10"))
	g.Expect(updated.Status.StandaloneStatus.ObservedConfigHash).ToNot(BeEmpty())
}

func TestDetectSidecarInjectionStatus_ScopedToClusterInstance(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	err := antflyv1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())
	err = corev1.AddToScheme(s)
	g.Expect(err).NotTo(HaveOccurred())

	cluster := baseStandaloneControllerCluster()
	clusterPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-standalone-standalone-0",
			Namespace: "default",
			Labels: map[string]string{
				"app.kubernetes.io/name":     "antfly-database",
				"app.kubernetes.io/instance": "test-standalone",
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "antfly"},
				{Name: "istio-proxy"},
			},
		},
	}
	otherClusterPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "other-cluster-standalone-0",
			Namespace: "default",
			Labels: map[string]string{
				"app.kubernetes.io/name":     "antfly-database",
				"app.kubernetes.io/instance": "other-cluster",
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "antfly"},
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(cluster, clusterPod, otherClusterPod).
		Build()

	reconciler := &AntflyClusterReconciler{
		Client: client,
		Scheme: s,
	}

	podsWithSidecars, totalPods, err := reconciler.detectSidecarInjectionStatus(context.Background(), cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(totalPods).To(Equal(int32(1)))
	g.Expect(podsWithSidecars).To(Equal(int32(1)))
}

// TestPodLabels tests that podLabels returns correct labels including instance
func TestPodLabels(t *testing.T) {
	g := NewWithT(t)

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "my-cluster",
			Labels: map[string]string{
				"cloud.antfly.io/purpose":        "cloud-instance",
				"cloud.antfly.io/instance-id":    "instance-123",
				"app.kubernetes.io/managed-by":   "external-controller",
				"app.kubernetes.io/part-of":      "cloudaf",
				"kubernetes.io/metadata.name":    "default",
				"operator.antfly.io/owned-label": "true",
			},
		},
	}

	labels := podLabels(cluster, "metadata")

	g.Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/name", "antfly-database"))
	g.Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/component", "metadata"))
	g.Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/instance", "my-cluster"))
	g.Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/managed-by", "antfly-operator"))
	g.Expect(labels).To(HaveKeyWithValue("cloud.antfly.io/purpose", "cloud-instance"))
	g.Expect(labels).To(HaveKeyWithValue("cloud.antfly.io/instance-id", "instance-123"))
	g.Expect(labels).To(HaveKeyWithValue("kubernetes.io/metadata.name", "default"))
	g.Expect(labels).To(HaveKeyWithValue("operator.antfly.io/owned-label", "true"))
	g.Expect(labels).NotTo(HaveKey("app.kubernetes.io/part-of"))
}

func TestPodTemplateLabelsUpdateWhenClusterLabelsChange(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "label-update-cluster",
			Namespace: "default",
			Labels: map[string]string{
				"cloud.antfly.io/instance-id": "instance-before",
			},
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				MetadataAPI:  antflyv1.APISpec{Port: 12377},
				MetadataRaft: antflyv1.APISpec{Port: 9017},
				Health:       antflyv1.APISpec{Port: 4200},
			},
			Storage: antflyv1.StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
			},
			Config: "{}",
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(cluster).
		Build()

	reconciler := &AntflyClusterReconciler{
		Client: client,
		Scheme: s,
	}

	g.Expect(reconciler.reconcileMetadataStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())

	sts := &appsv1.StatefulSet{}
	key := types.NamespacedName{Name: cluster.Name + "-metadata", Namespace: cluster.Namespace}
	g.Expect(client.Get(context.Background(), key, sts)).To(Succeed())
	g.Expect(sts.Spec.Template.Labels).To(HaveKeyWithValue("cloud.antfly.io/instance-id", "instance-before"))

	cluster.Labels = map[string]string{
		"cloud.antfly.io/instance-id": "instance-after",
		"cloud.antfly.io/org-id":      "org-123",
	}

	g.Expect(reconciler.reconcileMetadataStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())
	g.Expect(client.Get(context.Background(), key, sts)).To(Succeed())
	g.Expect(sts.Spec.Template.Labels).To(HaveKeyWithValue("cloud.antfly.io/instance-id", "instance-after"))
	g.Expect(sts.Spec.Template.Labels).To(HaveKeyWithValue("cloud.antfly.io/org-id", "org-123"))
	g.Expect(sts.Spec.Template.Labels).To(HaveKeyWithValue("app.kubernetes.io/managed-by", "antfly-operator"))
}

func TestMetadataStatefulSetPreparesPersistentStorageForNonRootRuntime(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "storage-security-cluster",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:latest",
			MetadataNodes: antflyv1.MetadataNodesSpec{
				MetadataAPI:  antflyv1.APISpec{Port: 12377},
				MetadataRaft: antflyv1.APISpec{Port: 9017},
				Health:       antflyv1.APISpec{Port: 4200},
			},
			Storage: antflyv1.StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
			},
			Config: "{}",
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(cluster).
		Build()

	reconciler := &AntflyClusterReconciler{
		Client: client,
		Scheme: s,
	}

	g.Expect(reconciler.reconcileMetadataStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())

	sts := &appsv1.StatefulSet{}
	key := types.NamespacedName{Name: cluster.Name + "-metadata", Namespace: cluster.Namespace}
	g.Expect(client.Get(context.Background(), key, sts)).To(Succeed())

	podSecurityContext := sts.Spec.Template.Spec.SecurityContext
	g.Expect(podSecurityContext).NotTo(BeNil())
	g.Expect(podSecurityContext.FSGroup).NotTo(BeNil())
	g.Expect(*podSecurityContext.FSGroup).To(Equal(antflyRuntimeGID))
	g.Expect(podSecurityContext.FSGroupChangePolicy).NotTo(BeNil())
	g.Expect(*podSecurityContext.FSGroupChangePolicy).To(Equal(corev1.FSGroupChangeOnRootMismatch))

	g.Expect(sts.Spec.Template.Spec.InitContainers).To(HaveLen(1))
	initContainer := sts.Spec.Template.Spec.InitContainers[0]
	g.Expect(initContainer.SecurityContext).NotTo(BeNil())
	g.Expect(initContainer.SecurityContext.RunAsUser).NotTo(BeNil())
	g.Expect(*initContainer.SecurityContext.RunAsUser).To(Equal(int64(0)))
	g.Expect(initContainer.Args).To(HaveLen(1))
	g.Expect(initContainer.Args[0]).NotTo(ContainSubstring("mkdir -p /antflydb/metadata /antflydb/store"))
	g.Expect(initContainer.Args[0]).To(ContainSubstring("chown -R 10001:10001 /antflydb"))
	g.Expect(initContainer.Args[0]).To(ContainSubstring("chmod -R ug+rwX /antflydb"))
}

// TestSelectorLabels tests that selectorLabels includes instance but not managed-by
// TestServiceSelectorLabels tests that serviceSelectorLabels includes instance but not managed-by
func TestServiceSelectorLabels(t *testing.T) {
	g := NewWithT(t)

	labels := serviceSelectorLabels("my-cluster", "metadata")

	g.Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/name", "antfly-database"))
	g.Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/component", "metadata"))
	g.Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/instance", "my-cluster"))
	g.Expect(labels).NotTo(HaveKey("app.kubernetes.io/managed-by"))
}

// TestBuildResourceRequirements tests resource conversion including GPU
func TestBuildResourceRequirements(t *testing.T) {
	g := NewWithT(t)
	r := &AntflyClusterReconciler{}

	t.Run("maps GPU to nvidia.com/gpu", func(t *testing.T) {
		g := NewWithT(t)
		reqs := r.buildResourceRequirements(antflyv1.ResourceSpec{
			CPU:    "500m",
			Memory: "1Gi",
			Limits: antflyv1.ResourceLimits{
				CPU:    "2",
				Memory: "4Gi",
				GPU:    "1",
			},
		})

		g.Expect(reqs.Requests[corev1.ResourceCPU]).To(Equal(resource.MustParse("500m")))
		g.Expect(reqs.Requests[corev1.ResourceMemory]).To(Equal(resource.MustParse("1Gi")))
		g.Expect(reqs.Limits[corev1.ResourceCPU]).To(Equal(resource.MustParse("2")))
		g.Expect(reqs.Limits[corev1.ResourceMemory]).To(Equal(resource.MustParse("4Gi")))
		g.Expect(reqs.Limits[corev1.ResourceName("nvidia.com/gpu")]).To(Equal(resource.MustParse("1")))
	})

	t.Run("omits GPU when empty", func(t *testing.T) {
		g := NewWithT(t)
		reqs := r.buildResourceRequirements(antflyv1.ResourceSpec{
			Limits: antflyv1.ResourceLimits{
				CPU:    "1",
				Memory: "2Gi",
			},
		})

		_, hasGPU := reqs.Limits[corev1.ResourceName("nvidia.com/gpu")]
		g.Expect(hasGPU).To(BeFalse())
	})

	_ = g // satisfy compiler
}

// TestBuildPVCRetentionPolicy tests PVC retention policy mapping
func TestBuildPVCRetentionPolicy(t *testing.T) {
	g := NewWithT(t)

	// nil policy
	result := buildPVCRetentionPolicy(nil)
	g.Expect(result).To(BeNil())

	// Retain/Retain (default)
	result = buildPVCRetentionPolicy(&antflyv1.PVCRetentionPolicy{
		WhenDeleted: antflyv1.PVCRetentionRetain,
		WhenScaled:  antflyv1.PVCRetentionRetain,
	})
	g.Expect(result).NotTo(BeNil())
	g.Expect(result.WhenDeleted).To(Equal(appsv1.RetainPersistentVolumeClaimRetentionPolicyType))
	g.Expect(result.WhenScaled).To(Equal(appsv1.RetainPersistentVolumeClaimRetentionPolicyType))

	// Delete/Retain
	result = buildPVCRetentionPolicy(&antflyv1.PVCRetentionPolicy{
		WhenDeleted: antflyv1.PVCRetentionDelete,
		WhenScaled:  antflyv1.PVCRetentionRetain,
	})
	g.Expect(result).NotTo(BeNil())
	g.Expect(result.WhenDeleted).To(Equal(appsv1.DeletePersistentVolumeClaimRetentionPolicyType))
	g.Expect(result.WhenScaled).To(Equal(appsv1.RetainPersistentVolumeClaimRetentionPolicyType))

	// Delete/Delete
	result = buildPVCRetentionPolicy(&antflyv1.PVCRetentionPolicy{
		WhenDeleted: antflyv1.PVCRetentionDelete,
		WhenScaled:  antflyv1.PVCRetentionDelete,
	})
	g.Expect(result).NotTo(BeNil())
	g.Expect(result.WhenDeleted).To(Equal(appsv1.DeletePersistentVolumeClaimRetentionPolicyType))
	g.Expect(result.WhenScaled).To(Equal(appsv1.DeletePersistentVolumeClaimRetentionPolicyType))

	// Empty strings default to Retain
	result = buildPVCRetentionPolicy(&antflyv1.PVCRetentionPolicy{})
	g.Expect(result).NotTo(BeNil())
	g.Expect(result.WhenDeleted).To(Equal(appsv1.RetainPersistentVolumeClaimRetentionPolicyType))
	g.Expect(result.WhenScaled).To(Equal(appsv1.RetainPersistentVolumeClaimRetentionPolicyType))
}

// TestApplyDefaultZoneTopologySpread tests zone topology spread behavior
func TestApplyDefaultZoneTopologySpread(t *testing.T) {
	t.Run("adds constraint to new StatefulSet", func(t *testing.T) {
		g := NewWithT(t)
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{}, // CreationTimestamp is zero (new)
		}
		podTemplate := &corev1.PodTemplateSpec{}

		applyDefaultZoneTopologySpread(sts, podTemplate, "data", "my-cluster", nil, false)

		g.Expect(podTemplate.Spec.TopologySpreadConstraints).To(HaveLen(1))
		g.Expect(podTemplate.Spec.TopologySpreadConstraints[0].TopologyKey).To(Equal("topology.kubernetes.io/zone"))
		g.Expect(podTemplate.Spec.TopologySpreadConstraints[0].WhenUnsatisfiable).To(Equal(corev1.ScheduleAnyway))
		g.Expect(sts.Annotations[annotationDefaultTopologySpread]).To(Equal("true"))
	})

	t.Run("skips when user has explicit constraints", func(t *testing.T) {
		g := NewWithT(t)
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{annotationDefaultTopologySpread: "true"},
			},
		}
		podTemplate := &corev1.PodTemplateSpec{}
		userConstraints := []corev1.TopologySpreadConstraint{
			{TopologyKey: "kubernetes.io/hostname", MaxSkew: 1},
		}

		applyDefaultZoneTopologySpread(sts, podTemplate, "data", "my-cluster", userConstraints, false)

		g.Expect(podTemplate.Spec.TopologySpreadConstraints).To(HaveLen(0))
		g.Expect(sts.Annotations).NotTo(HaveKey(annotationDefaultTopologySpread))
	})

	t.Run("skips for GKE Autopilot", func(t *testing.T) {
		g := NewWithT(t)
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{},
		}
		podTemplate := &corev1.PodTemplateSpec{}

		applyDefaultZoneTopologySpread(sts, podTemplate, "data", "my-cluster", nil, true)

		g.Expect(podTemplate.Spec.TopologySpreadConstraints).To(HaveLen(0))
	})

	t.Run("re-adds if annotation present on existing StatefulSet", func(t *testing.T) {
		g := NewWithT(t)
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				CreationTimestamp: metav1.Now(), // existing
				Annotations:       map[string]string{annotationDefaultTopologySpread: "true"},
			},
		}
		podTemplate := &corev1.PodTemplateSpec{}

		applyDefaultZoneTopologySpread(sts, podTemplate, "metadata", "my-cluster", nil, false)

		g.Expect(podTemplate.Spec.TopologySpreadConstraints).To(HaveLen(1))
	})

	t.Run("skips existing StatefulSet without annotation", func(t *testing.T) {
		g := NewWithT(t)
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				CreationTimestamp: metav1.Now(), // existing
			},
		}
		podTemplate := &corev1.PodTemplateSpec{}

		applyDefaultZoneTopologySpread(sts, podTemplate, "data", "my-cluster", nil, false)

		g.Expect(podTemplate.Spec.TopologySpreadConstraints).To(HaveLen(0))
	})
}

// TestContainsVolumeAffinityMessage tests the helper function
func TestContainsVolumeAffinityMessage(t *testing.T) {
	g := NewWithT(t)

	g.Expect(containsVolumeAffinityMessage("0/3 nodes are available: 1 volume node affinity conflict, 2 node(s) didn't match")).To(BeTrue())
	g.Expect(containsVolumeAffinityMessage("no matching nodes")).To(BeFalse())
	g.Expect(containsVolumeAffinityMessage("")).To(BeFalse())
}

func baseStandaloneControllerCluster() *antflyv1.AntflyCluster {
	enabled := true
	serviceType := corev1.ServiceTypeClusterIP

	return &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-standalone",
			Namespace: "default",
		},
		Spec: antflyv1.AntflyClusterSpec{
			Mode:  antflyv1.ClusterModeStandalone,
			Image: "antfly:latest",
			Standalone: &antflyv1.StandaloneSpec{
				Replicas:     1,
				NodeID:       1,
				Resources:    antflyv1.ResourceSpec{CPU: "500m", Memory: "1Gi"},
				MetadataAPI:  antflyv1.APISpec{Port: 8080},
				MetadataRaft: antflyv1.APISpec{Port: 9017},
				StoreAPI:     antflyv1.APISpec{Port: 12380},
				StoreRaft:    antflyv1.APISpec{Port: 9021},
				Health:       antflyv1.APISpec{Port: 4200},
				Inference: &antflyv1.StandaloneInferenceSpec{
					Enabled: true,
					APIURL:  "http://0.0.0.0:11433",
				},
			},
			Storage: antflyv1.StorageSpec{
				StorageClass:      "standard",
				StandaloneStorage: "1Gi",
			},
			PublicAPI: &antflyv1.PublicAPIConfig{
				Enabled:     &enabled,
				ServiceType: &serviceType,
				Port:        80,
			},
			Config: "{}",
		},
	}
}
