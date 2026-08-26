package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strings"
	"sync"
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
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	k8stesting "k8s.io/client-go/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

type listRejectingReader struct {
	client.Reader
}

func (r listRejectingReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return fmt.Errorf("unexpected namespace-wide List")
}

type statusUpdateRejectingClient struct {
	client.Client
	err error
}

func (c statusUpdateRejectingClient) Status() client.SubResourceWriter {
	return statusUpdateRejectingWriter{SubResourceWriter: c.Client.Status(), err: c.err}
}

type statusUpdateRejectingWriter struct {
	client.SubResourceWriter
	err error
}

func testInternalServiceAuthSpec() *antflyv1.InternalServiceAuthSpec {
	return &antflyv1.InternalServiceAuthSpec{SecretKeyRef: corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "test-internal-service-auth"},
		Key:                  "secret",
	}}
}

func TestInternalServiceAuthEnvUsesSecretSelectorWithoutReadingSecret(t *testing.T) {
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "tenant-a", UID: "cluster-uid"},
		Spec:       antflyv1.AntflyClusterSpec{InternalServiceAuth: testInternalServiceAuthSpec()},
	}
	env := internalServiceAuthEnv(cluster, internalServiceAuthRolloutMigration, internalServiceAuthKeyRolloutSteady)
	if len(env) != 3 {
		t.Fatalf("expected secret, issuer, and rollout environment, got %#v", env)
	}
	if env[0].Name != antflyInternalServiceSecretEnvVar || env[0].Value != "" || env[0].ValueFrom == nil || env[0].ValueFrom.SecretKeyRef == nil {
		t.Fatalf("expected signing key to use SecretKeyRef only, got %#v", env[0])
	}
	if env[0].ValueFrom.SecretKeyRef.Name != "test-internal-service-auth" || env[0].ValueFrom.SecretKeyRef.Key != "secret" || env[0].ValueFrom.SecretKeyRef.Optional == nil || *env[0].ValueFrom.SecretKeyRef.Optional {
		t.Fatalf("unexpected required Secret selector: %#v", env[0].ValueFrom.SecretKeyRef)
	}
	if env[1].Value != "antfly-cluster:cluster-uid" || env[2].Value != "migration" {
		t.Fatalf("unexpected derived non-secret environment: %#v", env)
	}
}

func TestInternalServiceAuthEnvStagesRotationWithoutReadingSecrets(t *testing.T) {
	cluster := &antflyv1.AntflyCluster{Spec: antflyv1.AntflyClusterSpec{InternalServiceAuth: testInternalServiceAuthSpec()}}
	cluster.Spec.InternalServiceAuth.NextSecretKeyRef = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "next-internal-service-auth"}, Key: "secret",
	}
	prepare := internalServiceAuthEnv(cluster, internalServiceAuthRolloutEnforce, internalServiceAuthKeyRolloutPrepare)
	if prepare[0].ValueFrom.SecretKeyRef.Name != "test-internal-service-auth" ||
		prepare[3].Name != antflyInternalServiceVerificationSecretEnvVar || prepare[3].ValueFrom.SecretKeyRef.Name != "next-internal-service-auth" {
		t.Fatalf("prepare phase must sign old and verify next: %#v", prepare)
	}
	switching := internalServiceAuthEnv(cluster, internalServiceAuthRolloutEnforce, internalServiceAuthKeyRolloutSwitch)
	if switching[0].ValueFrom.SecretKeyRef.Name != "next-internal-service-auth" || switching[3].ValueFrom.SecretKeyRef.Name != "test-internal-service-auth" {
		t.Fatalf("switch phase must sign next and verify old: %#v", switching)
	}
}

func TestDistributedRuntimeNeedsInternalServiceAuthMigration(t *testing.T) {
	distributed := &antflyv1.AntflyCluster{Spec: antflyv1.AntflyClusterSpec{Mode: antflyv1.ClusterModeDistributed}}
	if !distributedRuntimeNeedsInternalServiceAuthMigration(distributed) {
		t.Fatal("legacy distributed cluster must be revalidated before workload mutation")
	}
	distributed.Spec.InternalServiceAuth = testInternalServiceAuthSpec()
	if distributedRuntimeNeedsInternalServiceAuthMigration(distributed) {
		t.Fatal("configured distributed cluster must not remain migration-blocked")
	}
	standalone := &antflyv1.AntflyCluster{Spec: antflyv1.AntflyClusterSpec{Mode: antflyv1.ClusterModeStandalone}}
	if distributedRuntimeNeedsInternalServiceAuthMigration(standalone) {
		t.Fatal("standalone cluster does not use distributed internal service authentication")
	}
}

func TestInternalServiceAuthUpgradeRequiresRuntimeCapabilityBeforeEnforcement(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default", UID: "cluster-uid"},
		Spec: antflyv1.AntflyClusterSpec{
			Mode:                antflyv1.ClusterModeDistributed,
			InternalServiceAuth: testInternalServiceAuthSpec(),
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas:    1,
				MetadataAPI: antflyv1.APISpec{Port: 12377},
			},
			DataNodes: antflyv1.DataNodesSpec{Replicas: 1},
		},
	}

	newReconciler := func(objects ...client.Object) *AntflyClusterReconciler {
		return &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()}
	}
	mode, pending, err := newReconciler().desiredInternalServiceAuthRolloutMode(context.Background(), cluster)
	if err != nil || mode != internalServiceAuthRolloutEnforce || pending {
		t.Fatalf("new cluster should enforce immediately, mode=%q pending=%t err=%v", mode, pending, err)
	}

	completeStatefulSet := func(name string, mode internalServiceAuthRolloutMode) *appsv1.StatefulSet {
		return &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace, Generation: 2},
			Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{internalServiceAuthRolloutAnnotation: string(mode)},
			}}},
			Status: appsv1.StatefulSetStatus{
				ObservedGeneration: 2,
				UpdatedReplicas:    1,
				ReadyReplicas:      1,
				CurrentRevision:    "revision-a",
				UpdateRevision:     "revision-a",
			},
		}
	}
	legacyMetadata := completeStatefulSet("example-metadata", "")
	legacyData := completeStatefulSet("example-data", "")
	mode, pending, err = newReconciler(legacyMetadata, legacyData).desiredInternalServiceAuthRolloutMode(context.Background(), cluster)
	if err != nil || mode != internalServiceAuthRolloutMigration || !pending {
		t.Fatalf("legacy cluster should enter migration, mode=%q pending=%t err=%v", mode, pending, err)
	}

	migrationMetadata := completeStatefulSet("example-metadata", internalServiceAuthRolloutMigration)
	migrationData := completeStatefulSet("example-data", internalServiceAuthRolloutMigration)
	capabilityReconciler := newReconciler(migrationMetadata, migrationData)
	capabilityReconciler.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set(internalServiceAuthCapabilityHeader, internalServiceAuthCapabilityVersion+"; mode=migration")
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`{}`)), Request: req}, nil
	})}
	mode, pending, err = capabilityReconciler.desiredInternalServiceAuthRolloutMode(context.Background(), cluster)
	if err != nil || mode != internalServiceAuthRolloutEnforce || pending {
		t.Fatalf("capability-proven migration should advance to enforce, mode=%q pending=%t err=%v", mode, pending, err)
	}

	missingCapabilityReconciler := newReconciler(migrationMetadata.DeepCopy(), migrationData.DeepCopy())
	missingCapabilityReconciler.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`)), Request: req}, nil
	})}
	mode, pending, err = missingCapabilityReconciler.desiredInternalServiceAuthRolloutMode(context.Background(), cluster)
	if err != nil || mode != internalServiceAuthRolloutMigration || !pending {
		t.Fatalf("missing runtime capability must remain in migration, mode=%q pending=%t err=%v", mode, pending, err)
	}
}

func TestInternalServiceAuthKeyRotationRequiresEveryRuntimeOverlap(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{
			InternalServiceAuth: testInternalServiceAuthSpec(),
			MetadataNodes:       antflyv1.MetadataNodesSpec{Replicas: 1, MetadataAPI: antflyv1.APISpec{Port: 12377}},
			DataNodes:           antflyv1.DataNodesSpec{Replicas: 1, API: antflyv1.APISpec{Port: 12380}},
		},
	}
	cluster.Spec.InternalServiceAuth.NextSecretKeyRef = &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "next-key"}, Key: "secret"}
	target := internalServiceAuthKeyTarget(cluster.Spec.InternalServiceAuth)
	complete := func(name string, keyMode internalServiceAuthKeyRolloutMode) *appsv1.StatefulSet {
		return &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Generation: 1},
			Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
				internalServiceAuthRolloutAnnotation:    string(internalServiceAuthRolloutEnforce),
				internalServiceAuthKeyRolloutAnnotation: string(keyMode),
				internalServiceAuthKeyTargetAnnotation:  target,
			}}}},
			Status: appsv1.StatefulSetStatus{ObservedGeneration: 1, UpdatedReplicas: 1, ReadyReplicas: 1, CurrentRevision: "a", UpdateRevision: "a"},
		}
	}
	newReconciler := func(metadata, data *appsv1.StatefulSet, capability string) *AntflyClusterReconciler {
		r := &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(metadata, data).Build()}
		r.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			header := make(http.Header)
			header.Set(internalServiceAuthCapabilityHeader, capability)
			return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`{}`)), Request: req}, nil
		})}
		return r
	}
	mode, pending, phase, err := newReconciler(complete("example-metadata", internalServiceAuthKeyRolloutSteady), complete("example-data", internalServiceAuthKeyRolloutSteady), "").desiredInternalServiceAuthKeyRollout(context.Background(), cluster, internalServiceAuthRolloutEnforce)
	if err != nil || mode != internalServiceAuthKeyRolloutPrepare || !pending || phase != antflyv1.InternalServiceAuthRotationPreparing {
		t.Fatalf("rotation should begin with verifier preparation: mode=%s pending=%t phase=%s err=%v", mode, pending, phase, err)
	}
	capability := internalServiceAuthCapability(internalServiceAuthRolloutEnforce, true)
	mode, pending, phase, err = newReconciler(complete("example-metadata", internalServiceAuthKeyRolloutPrepare), complete("example-data", internalServiceAuthKeyRolloutPrepare), capability).desiredInternalServiceAuthKeyRollout(context.Background(), cluster, internalServiceAuthRolloutEnforce)
	if err != nil || mode != internalServiceAuthKeyRolloutSwitch || !pending || phase != antflyv1.InternalServiceAuthRotationSwitching {
		t.Fatalf("prepared overlap should switch signing: mode=%s pending=%t phase=%s err=%v", mode, pending, phase, err)
	}
	mode, pending, phase, err = newReconciler(complete("example-metadata", internalServiceAuthKeyRolloutSwitch), complete("example-data", internalServiceAuthKeyRolloutSwitch), capability).desiredInternalServiceAuthKeyRollout(context.Background(), cluster, internalServiceAuthRolloutEnforce)
	if err != nil || mode != internalServiceAuthKeyRolloutSwitch || pending || phase != antflyv1.InternalServiceAuthRotationSwitched {
		t.Fatalf("fully switched overlap should complete: mode=%s pending=%t phase=%s err=%v", mode, pending, phase, err)
	}
}

func TestInternalServiceAuthPublicBoundaryWaitsForEveryEnforcingWorkload(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{
			Mode:          antflyv1.ClusterModeDistributed,
			MetadataNodes: antflyv1.MetadataNodesSpec{Replicas: 1},
			DataNodes:     antflyv1.DataNodesSpec{Replicas: 1},
		},
	}
	complete := func(name string, mode internalServiceAuthRolloutMode) *appsv1.StatefulSet {
		return &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace, Generation: 2},
			Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{internalServiceAuthRolloutAnnotation: string(mode)},
			}}},
			Status: appsv1.StatefulSetStatus{
				ObservedGeneration: 2,
				UpdatedReplicas:    1,
				ReadyReplicas:      1,
				CurrentRevision:    "revision-a",
				UpdateRevision:     "revision-a",
			},
		}
	}
	ready := func(objects ...client.Object) bool {
		reconciler := &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()}
		value, err := reconciler.internalServiceAuthPublicBoundaryReady(context.Background(), cluster)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	if ready() {
		t.Fatal("a new cluster must not publish its API before authenticated workloads are ready")
	}
	if ready(
		complete("example-metadata", internalServiceAuthRolloutMigration),
		complete("example-data", internalServiceAuthRolloutMigration),
	) {
		t.Fatal("migration mode must keep the public API boundary withdrawn")
	}
	if ready(
		complete("example-metadata", internalServiceAuthRolloutEnforce),
		complete("example-data", internalServiceAuthRolloutMigration),
	) {
		t.Fatal("one remaining migration workload must keep the boundary withdrawn")
	}
	if !ready(
		complete("example-metadata", internalServiceAuthRolloutEnforce),
		complete("example-data", internalServiceAuthRolloutEnforce),
	) {
		t.Fatal("the public API may return after every workload is enforcing")
	}
	establishedService := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name:        "example-public-api",
		Namespace:   cluster.Namespace,
		Annotations: map[string]string{internalServiceAuthPublicBoundaryAnnotation: "enforced"},
	}}
	incompleteMetadata := complete("example-metadata", internalServiceAuthRolloutEnforce)
	incompleteMetadata.Status.UpdatedReplicas = 0
	incompleteMetadata.Status.UpdateRevision = "revision-b"
	if !ready(
		establishedService,
		incompleteMetadata,
		complete("example-data", internalServiceAuthRolloutEnforce),
	) {
		t.Fatal("later enforcing-only rollouts must preserve the established public boundary")
	}
}

func TestInternalServiceAuthPublicBoundaryWaitsForEndpointDrain(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := discoveryv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	enabled := true
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec:       antflyv1.AntflyClusterSpec{PublicAPI: &antflyv1.PublicAPIConfig{Enabled: &enabled}},
	}
	endpointSlice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example-public-api-abc",
			Namespace: cluster.Namespace,
			Labels:    map[string]string{discoveryv1.LabelServiceName: "example-public-api"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{"10.0.0.10"}}},
	}
	reconciler := &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(endpointSlice).Build()}
	drained, err := reconciler.internalServiceAuthPublicBoundaryDrained(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if drained {
		t.Fatal("the workload rollout must not start while a stale public endpoint remains")
	}
	if err := reconciler.Delete(context.Background(), endpointSlice); err != nil {
		t.Fatal(err)
	}
	drained, err = reconciler.internalServiceAuthPublicBoundaryDrained(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if !drained {
		t.Fatal("the workload rollout may start after every public endpoint drains")
	}
}

func (w statusUpdateRejectingWriter) Update(context.Context, client.Object, ...client.SubResourceUpdateOption) error {
	return w.err
}

func TestGeneratedConfigHashAnnotationChangesWithRemoteContentConfig(t *testing.T) {
	reconciler := &AntflyClusterReconciler{}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{
			Config: `{"remote_content":{"default_s3":"primary","s3":{"primary":{"region":"us-west-2","access_key_id":"${secret:s3.access_key}","secret_access_key":"${secret:s3.secret_key}"}}}}`,
		},
	}

	first := reconciler.buildPodAnnotations(context.Background(), newEnvFromCache(nil), cluster, nil)
	firstHash := first[generatedConfigHashAnnotation]
	if firstHash == "" {
		t.Fatal("expected generated config hash annotation")
	}

	// A value rotation behind either secret reference is not represented in the
	// AntflyCluster and cannot affect this hash; changing config/routing does.
	cluster.Spec.Config = `{"remote_content":{"default_s3":"archive","s3":{"archive":{"region":"us-east-1","access_key_id":"${secret:s3.access_key}","secret_access_key":"${secret:s3.secret_key}"}}}}`
	second := reconciler.buildPodAnnotations(context.Background(), newEnvFromCache(nil), cluster, nil)
	if second[generatedConfigHashAnnotation] == firstHash {
		t.Fatal("expected remote-content config change to alter pod-template hash")
	}
}

func TestValidateMetadataReplicaTopology(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	newReconciler := func(objects ...client.Object) *AntflyClusterReconciler {
		return &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()}
	}
	newCluster := func(replicas, recorded int32) *antflyv1.AntflyCluster {
		return &antflyv1.AntflyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
			Spec: antflyv1.AntflyClusterSpec{
				Mode: antflyv1.ClusterModeDistributed,
				MetadataNodes: antflyv1.MetadataNodesSpec{
					Replicas:    replicas,
					MetadataAPI: antflyv1.APISpec{Port: 12377},
				},
			},
			Status: antflyv1.AntflyClusterStatus{MetadataTopologyReplicas: recorded},
		}
	}
	statusClient := func(statuses map[string]string) *http.Client {
		return &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			body, ok := statuses[req.URL.Hostname()]
			if !ok {
				return nil, fmt.Errorf("unexpected metadata status host %s", req.URL.Hostname())
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     make(http.Header),
			}, nil
		})}
	}
	statusJSON := func(nodeID uint64, role string, leaderID uint64, incarnation string, voterCount int32) string {
		return fmt.Sprintf(`{"metadata_group_id":1,"metadata_incarnation":%q,"metadata_raft_local_node_id":%d,"metadata_raft_role":%q,"metadata_raft_leader_id":%d,"metadata_raft_local_voter":true,"metadata_raft_voter_count":%d,"metadata_raft_voter_set_fingerprint":%q,"metadata_raft_joint_consensus":false,"metadata_raft_learner_count":0}`,
			incarnation, nodeID, role, leaderID, voterCount, metadataRaftVoterSetFingerprint(voterCount))
	}
	legacyStatusJSON := func(nodeID uint64, role string, leaderID uint64, incarnation string, voterCount int32) string {
		return fmt.Sprintf(`{"metadata_group_id":1,"metadata_incarnation":%q,"metadata_raft_local_node_id":%d,"metadata_raft_role":%q,"metadata_raft_leader_id":%d,"metadata_raft_local_voter":true,"metadata_raft_voter_count":%d}`,
			incarnation, nodeID, role, leaderID, voterCount)
	}
	preLearnerStatusJSON := func(nodeID uint64, role string, leaderID uint64, incarnation string, voterCount int32) string {
		return strings.Replace(statusJSON(nodeID, role, leaderID, incarnation, voterCount), `,"metadata_raft_learner_count":0`, "", 1)
	}
	pvc := func(ordinal int, annotation string) *corev1.PersistentVolumeClaim {
		annotations := map[string]string(nil)
		if annotation != "" {
			annotations = map[string]string{metadataTopologyReplicasAnnotation: annotation}
		}
		return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Name:        fmt.Sprintf("metadata-storage-example-metadata-%d", ordinal),
			Namespace:   "default",
			Annotations: annotations,
		}}
	}

	driftedReplicas := int32(3)
	metadataStatefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "example-metadata", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &driftedReplicas},
	}
	reconciler := newReconciler(metadataStatefulSet)
	cluster := newCluster(1, 1)
	if err := reconciler.validateMetadataReplicaTopology(context.Background(), cluster); err != nil {
		t.Fatalf("expected durable status topology to ignore StatefulSet drift, got: %v", err)
	}

	cluster.Spec.MetadataNodes.Replicas = 3
	err := reconciler.validateMetadataReplicaTopology(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "AntflyCluster status: 1, attempted: 3") {
		t.Fatalf("expected durable status to reject changed topology, got: %v", err)
	}

	legacyReplicas := int32(1)
	legacyStatefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "example-metadata", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &legacyReplicas},
	}
	cluster = newCluster(1, 0)
	legacyReconciler := newReconciler(legacyStatefulSet, pvc(0, ""))
	legacyReconciler.HTTPClient = statusClient(map[string]string{
		"example-metadata-0.example-metadata.default.svc.cluster.local": statusJSON(1, "leader", 1, "0123456789abcdef0123456789abcdef", 1),
	})
	if err := legacyReconciler.validateMetadataReplicaTopology(context.Background(), cluster); err != nil {
		t.Fatalf("expected matching legacy StatefulSet topology to migrate, got: %v", err)
	}
	cluster.Spec.MetadataNodes.Replicas = 3
	err = newReconciler(legacyStatefulSet).validateMetadataReplicaTopology(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "legacy metadata StatefulSet: 1, attempted: 3") {
		t.Fatalf("expected legacy StatefulSet topology to reject a changed count during migration, got: %v", err)
	}

	annotatedPVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:      "metadata-storage-example-metadata-0",
		Namespace: "default",
		Annotations: map[string]string{
			metadataTopologyReplicasAnnotation: "1",
		},
	}}
	cluster = newCluster(3, 0)
	err = newReconciler(annotatedPVC).validateMetadataReplicaTopology(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "metadata PVC metadata-storage-example-metadata-0: 1, attempted: 3") {
		t.Fatalf("expected retained PVC topology to reject same-name recreation at a new count, got: %v", err)
	}
	staleCacheReconciler := newReconciler()
	staleCacheReconciler.BoundaryReader = fake.NewClientBuilder().WithScheme(scheme).WithObjects(annotatedPVC).Build()
	err = staleCacheReconciler.validateMetadataReplicaTopology(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "metadata PVC metadata-storage-example-metadata-0: 1, attempted: 3") {
		t.Fatalf("expected uncached topology boundary to reject a retained PVC hidden from the controller cache, got: %v", err)
	}

	incompletePVC := pvc(0, "3")
	cluster = newCluster(3, 0)
	err = newReconciler(incompletePVC).validateMetadataReplicaTopology(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "missing ordinal 1") {
		t.Fatalf("expected incomplete retained PVC set to fail closed, got: %v", err)
	}

	extraPVCs := []client.Object{pvc(0, "3"), pvc(1, "3"), pvc(2, "3"), pvc(3, "3")}
	err = newReconciler(extraPVCs...).validateMetadataReplicaTopology(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "unexpected ordinal 3") {
		t.Fatalf("expected out-of-range retained PVC to fail closed, got: %v", err)
	}

	foreignPrefixPVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:      "metadata-storage-example-metadata-canary-metadata-0",
		Namespace: "default",
		Labels: map[string]string{
			"app.kubernetes.io/component": "metadata",
			"app.kubernetes.io/instance":  "example-metadata-canary",
		},
		Annotations: map[string]string{metadataTopologyReplicasAnnotation: "1"},
	}}
	if err := newReconciler(foreignPrefixPVC).validateMetadataReplicaTopology(context.Background(), cluster); err != nil {
		t.Fatalf("expected a prefix-related cluster's PVC to be ignored, got: %v", err)
	}
	mislabelledCanonicalPVC := pvc(0, "1")
	mislabelledCanonicalPVC.Labels = map[string]string{
		"app.kubernetes.io/component": "data",
		"app.kubernetes.io/instance":  "another-cluster",
	}
	err = newReconciler(mislabelledCanonicalPVC).validateMetadataReplicaTopology(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "metadata PVC metadata-storage-example-metadata-0: 1, attempted: 3") {
		t.Fatalf("expected a canonically named PVC with stale labels to remain authoritative, got: %v", err)
	}

	unrecordedPVC := annotatedPVC.DeepCopy()
	unrecordedPVC.Annotations = nil
	cluster = newCluster(1, 0)
	err = newReconciler(unrecordedPVC).validateMetadataReplicaTopology(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "retained PVCs predate topology recording") {
		t.Fatalf("expected unrecorded retained PVC topology to fail closed, got: %v", err)
	}

	cluster = newCluster(3, 0)
	if err := newReconciler().validateMetadataReplicaTopology(context.Background(), cluster); err != nil {
		t.Fatalf("expected a cluster without StatefulSets or retained PVCs to pass, got: %v", err)
	}

	legacyThree := int32(3)
	legacyThreeStatefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "example-metadata", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &legacyThree},
	}
	legacyObjects := []client.Object{legacyThreeStatefulSet, pvc(0, ""), pvc(1, ""), pvc(2, "")}
	cluster = newCluster(3, 0)
	splitReconciler := newReconciler(legacyObjects...)
	splitReconciler.HTTPClient = statusClient(map[string]string{
		"example-metadata-0.example-metadata.default.svc.cluster.local": statusJSON(1, "leader", 1, "11111111111111111111111111111111", 1),
		"example-metadata-1.example-metadata.default.svc.cluster.local": statusJSON(2, "follower", 3, "22222222222222222222222222222222", 3),
		"example-metadata-2.example-metadata.default.svc.cluster.local": statusJSON(3, "leader", 3, "22222222222222222222222222222222", 3),
	})
	err = splitReconciler.validateMetadataReplicaTopology(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "voter_count=1") {
		t.Fatalf("expected split legacy runtime topology to fail closed, got: %v", err)
	}

	disagreeingReconciler := newReconciler(legacyObjects...)
	disagreeingReconciler.HTTPClient = statusClient(map[string]string{
		"example-metadata-0.example-metadata.default.svc.cluster.local": statusJSON(1, "follower", 3, "33333333333333333333333333333333", 3),
		"example-metadata-1.example-metadata.default.svc.cluster.local": statusJSON(2, "follower", 3, "44444444444444444444444444444444", 3),
		"example-metadata-2.example-metadata.default.svc.cluster.local": statusJSON(3, "leader", 3, "33333333333333333333333333333333", 3),
	})
	err = disagreeingReconciler.validateMetadataReplicaTopology(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "disagrees on metadata group or incarnation") {
		t.Fatalf("expected disagreeing legacy incarnations to fail closed, got: %v", err)
	}

	mismatchedVoterSetReconciler := newReconciler(legacyObjects...)
	mismatchedVoterSet := strings.Repeat("a", 64)
	mismatchedVoterSetReconciler.HTTPClient = statusClient(map[string]string{
		"example-metadata-0.example-metadata.default.svc.cluster.local": statusJSON(1, "follower", 3, "33333333333333333333333333333333", 3),
		"example-metadata-1.example-metadata.default.svc.cluster.local": strings.Replace(statusJSON(2, "follower", 3, "33333333333333333333333333333333", 3), metadataRaftVoterSetFingerprint(3), mismatchedVoterSet, 1),
		"example-metadata-2.example-metadata.default.svc.cluster.local": statusJSON(3, "leader", 3, "33333333333333333333333333333333", 3),
	})
	err = mismatchedVoterSetReconciler.validateMetadataReplicaTopology(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "voter set fingerprint") {
		t.Fatalf("expected mismatched legacy voter sets to fail closed, got: %v", err)
	}

	jointConsensusReconciler := newReconciler(legacyObjects...)
	jointConsensusReconciler.HTTPClient = statusClient(map[string]string{
		"example-metadata-0.example-metadata.default.svc.cluster.local": strings.Replace(statusJSON(1, "follower", 3, "33333333333333333333333333333333", 3), `"metadata_raft_joint_consensus":false`, `"metadata_raft_joint_consensus":true`, 1),
		"example-metadata-1.example-metadata.default.svc.cluster.local": statusJSON(2, "follower", 3, "33333333333333333333333333333333", 3),
		"example-metadata-2.example-metadata.default.svc.cluster.local": statusJSON(3, "leader", 3, "33333333333333333333333333333333", 3),
	})
	err = jointConsensusReconciler.validateMetadataReplicaTopology(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "joint-consensus") {
		t.Fatalf("expected joint-consensus legacy topology to fail closed, got: %v", err)
	}

	learnerReconciler := newReconciler(legacyObjects...)
	learnerReconciler.HTTPClient = statusClient(map[string]string{
		"example-metadata-0.example-metadata.default.svc.cluster.local": strings.Replace(statusJSON(1, "follower", 3, "33333333333333333333333333333333", 3), `"metadata_raft_learner_count":0`, `"metadata_raft_learner_count":1`, 1),
		"example-metadata-1.example-metadata.default.svc.cluster.local": statusJSON(2, "follower", 3, "33333333333333333333333333333333", 3),
		"example-metadata-2.example-metadata.default.svc.cluster.local": statusJSON(3, "leader", 3, "33333333333333333333333333333333", 3),
	})
	err = learnerReconciler.validateMetadataReplicaTopology(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "learner_count=1") {
		t.Fatalf("expected learner membership to fail closed, got: %v", err)
	}

	preLearnerRuntimeReconciler := newReconciler(legacyObjects...)
	preLearnerRuntimeReconciler.HTTPClient = statusClient(map[string]string{
		"example-metadata-0.example-metadata.default.svc.cluster.local": preLearnerStatusJSON(1, "follower", 3, "33333333333333333333333333333333", 3),
		"example-metadata-1.example-metadata.default.svc.cluster.local": preLearnerStatusJSON(2, "follower", 3, "33333333333333333333333333333333", 3),
		"example-metadata-2.example-metadata.default.svc.cluster.local": preLearnerStatusJSON(3, "leader", 3, "33333333333333333333333333333333", 3),
	})
	err = preLearnerRuntimeReconciler.validateMetadataReplicaTopology(context.Background(), cluster)
	if !stderrors.Is(err, errMetadataRuntimeMembershipStatusUnavailable) {
		t.Fatalf("expected a pre-learner-status runtime to request a capability rollout, got: %v", err)
	}

	legacyRuntimeReconciler := newReconciler(legacyObjects...)
	legacyRuntimeReconciler.HTTPClient = statusClient(map[string]string{
		"example-metadata-0.example-metadata.default.svc.cluster.local": legacyStatusJSON(1, "follower", 3, "33333333333333333333333333333333", 3),
		"example-metadata-1.example-metadata.default.svc.cluster.local": legacyStatusJSON(2, "follower", 3, "33333333333333333333333333333333", 3),
		"example-metadata-2.example-metadata.default.svc.cluster.local": legacyStatusJSON(3, "leader", 3, "33333333333333333333333333333333", 3),
	})
	err = legacyRuntimeReconciler.validateMetadataReplicaTopology(context.Background(), cluster)
	if !stderrors.Is(err, errMetadataRuntimeMembershipStatusUnavailable) {
		t.Fatalf("expected an otherwise healthy legacy runtime to request a capability rollout, got: %v", err)
	}

	malformedReconciler := newReconciler(legacyObjects...)
	malformedReconciler.HTTPClient = statusClient(map[string]string{
		"example-metadata-0.example-metadata.default.svc.cluster.local": `{`,
	})
	err = malformedReconciler.validateMetadataReplicaTopology(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "decode metadata topology") {
		t.Fatalf("expected malformed legacy member status to fail closed, got: %v", err)
	}

	healthyReconciler := newReconciler(legacyObjects...)
	healthyReconciler.HTTPClient = statusClient(map[string]string{
		"example-metadata-0.example-metadata.default.svc.cluster.local": statusJSON(1, "follower", 3, "33333333333333333333333333333333", 3),
		"example-metadata-1.example-metadata.default.svc.cluster.local": statusJSON(2, "follower", 3, "33333333333333333333333333333333", 3),
		"example-metadata-2.example-metadata.default.svc.cluster.local": statusJSON(3, "leader", 3, "33333333333333333333333333333333", 3),
	})
	if err := healthyReconciler.validateMetadataReplicaTopology(context.Background(), cluster); err != nil {
		t.Fatalf("expected an agreeing legacy runtime topology to migrate, got: %v", err)
	}
}

func TestReconcileStopsWhenMetadataTopologyCheckpointConflicts(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := antflyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "checkpoint-conflict", Namespace: "default", Generation: 1},
		Spec: antflyv1.AntflyClusterSpec{
			Mode:                antflyv1.ClusterModeDistributed,
			Image:               "antfly:test",
			InternalServiceAuth: testInternalServiceAuthSpec(),
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas:     3,
				Resources:    antflyv1.ResourceSpec{CPU: "500m", Memory: "512Mi"},
				MetadataAPI:  antflyv1.APISpec{Port: 12377},
				MetadataRaft: antflyv1.APISpec{Port: 9017},
			},
			DataNodes: antflyv1.DataNodesSpec{
				Replicas:  1,
				Resources: antflyv1.ResourceSpec{CPU: "1000m", Memory: "2Gi"},
				API:       antflyv1.APISpec{Port: 12380},
				Raft:      antflyv1.APISpec{Port: 9021},
			},
			Config: "{}",
			Storage: antflyv1.StorageSpec{
				StorageClass:    "standard",
				MetadataStorage: "1Gi",
				DataStorage:     "10Gi",
			},
		},
	}
	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&antflyv1.AntflyCluster{}).
		WithObjects(cluster).
		Build()
	conflictErr := errors.NewConflict(
		antflyv1.GroupVersion.WithResource("antflyclusters").GroupResource(),
		cluster.Name,
		fmt.Errorf("concurrent spec update"),
	)
	reconciler := &AntflyClusterReconciler{
		Client: statusUpdateRejectingClient{Client: baseClient, err: conflictErr},
		Scheme: scheme,
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
	if !errors.IsConflict(err) {
		t.Fatalf("reconcile error = %v, want validation checkpoint conflict", err)
	}

	metadataStatefulSet := &appsv1.StatefulSet{}
	err = baseClient.Get(context.Background(), types.NamespacedName{Name: cluster.Name + "-metadata", Namespace: cluster.Namespace}, metadataStatefulSet)
	if !errors.IsNotFound(err) {
		t.Fatalf("metadata StatefulSet lookup error = %v, want no resources reconciled after checkpoint conflict", err)
	}
}

func TestValidateMetadataRuntimeTopologyCarriesElectionObservationAcrossReconciles(t *testing.T) {
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{MetadataNodes: antflyv1.MetadataNodesSpec{
			Replicas:    3,
			MetadataAPI: antflyv1.APISpec{Port: 12377},
		}},
	}

	var mu sync.Mutex
	requestCounts := make(map[string]int)
	reconciler := &AntflyClusterReconciler{HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		host := req.URL.Hostname()
		mu.Lock()
		requestCounts[host]++
		attempt := requestCounts[host]
		mu.Unlock()

		nodeID := uint64(1)
		if strings.Contains(host, "-1.") {
			nodeID = 2
		} else if strings.Contains(host, "-2.") {
			nodeID = 3
		}
		term := uint64(2)
		leaderID := uint64(3)
		role := "follower"
		if nodeID == leaderID {
			role = "leader"
		}
		// Several concurrent observations straddle an election: ordinal zero
		// still reports the preceding term and leader. A later observation is
		// coherent, after more than the old single retry could absorb.
		if attempt < 4 && nodeID == 1 {
			term = 1
			leaderID = 1
			role = "leader"
		}
		body := fmt.Sprintf(`{"metadata_group_id":1,"metadata_incarnation":"33333333333333333333333333333333","metadata_raft_local_node_id":%d,"metadata_raft_role":%q,"metadata_raft_leader_id":%d,"metadata_raft_term":%d,"metadata_raft_local_voter":true,"metadata_raft_voter_count":3,"metadata_raft_voter_set_fingerprint":%q,"metadata_raft_joint_consensus":false,"metadata_raft_learner_count":0}`,
			nodeID, role, leaderID, term, metadataRaftVoterSetFingerprint(3))
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}}

	startedAt := time.Unix(1_700_000_000, 0)
	for observation := 0; observation < 4; observation++ {
		err := reconciler.validateMetadataRuntimeTopologyAt(
			context.Background(),
			cluster,
			3,
			startedAt.Add(time.Duration(observation)*time.Second),
			250*time.Millisecond,
			7*time.Second,
		)
		if observation < 3 {
			var pending *metadataTopologyObservationPendingError
			if !stderrors.As(err, &pending) {
				t.Fatalf("observation %d error = %v, want pending leadership observation", observation+1, err)
			}
			if pending.retryAfter != 250*time.Millisecond {
				t.Fatalf("observation %d retry = %v, want 250ms", observation+1, pending.retryAfter)
			}
			continue
		}
		if err != nil {
			t.Fatalf("expected the later coherent observation to succeed, got: %v", err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for host, count := range requestCounts {
		if count != 4 {
			t.Fatalf("metadata member %s request count = %d, want 4 observations", host, count)
		}
	}
}

func TestFetchMetadataRuntimeTopologyUsesCompactEndpointWithLegacyFallback(t *testing.T) {
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{MetadataNodes: antflyv1.MetadataNodesSpec{
			MetadataAPI: antflyv1.APISpec{Port: 12377},
		}},
	}
	body := `{"metadata_group_id":1,"metadata_incarnation":"33333333333333333333333333333333","metadata_raft_local_node_id":1}`

	var compactPaths []string
	compact := &AntflyClusterReconciler{HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		compactPaths = append(compactPaths, req.URL.Path)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}}
	status, err := compact.fetchMetadataRuntimeTopology(context.Background(), cluster, 0)
	if err != nil {
		t.Fatal(err)
	}
	if status.MetadataGroupID != 1 {
		t.Fatalf("metadata group = %d, want 1", status.MetadataGroupID)
	}
	if len(compactPaths) != 1 || compactPaths[0] != metadataRuntimeTopologyPath {
		t.Fatalf("steady-state topology request paths = %v, want only compact endpoint", compactPaths)
	}

	var legacyPaths []string
	legacy := &AntflyClusterReconciler{HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		legacyPaths = append(legacyPaths, req.URL.Path)
		if req.URL.Path == metadataRuntimeTopologyPath {
			return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(strings.NewReader("not found")), Header: make(http.Header)}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}}
	if _, err := legacy.fetchMetadataRuntimeTopology(context.Background(), cluster, 0); err != nil {
		t.Fatal(err)
	}
	if len(legacyPaths) != 2 || legacyPaths[0] != metadataRuntimeTopologyPath || legacyPaths[1] != metadataRuntimeStatusPath {
		t.Fatalf("legacy topology request paths = %v, want compact endpoint followed by status", legacyPaths)
	}
}

func TestValidateMetadataRuntimeTopologyExpiresElectionGrace(t *testing.T) {
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default", UID: "example-uid"},
		Spec: antflyv1.AntflyClusterSpec{MetadataNodes: antflyv1.MetadataNodesSpec{
			Replicas:    3,
			MetadataAPI: antflyv1.APISpec{Port: 12377},
		}},
	}
	reconciler := &AntflyClusterReconciler{HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		nodeID := uint64(1)
		if strings.Contains(req.URL.Hostname(), "-1.") {
			nodeID = 2
		} else if strings.Contains(req.URL.Hostname(), "-2.") {
			nodeID = 3
		}
		leaderID := uint64(3)
		term := uint64(2)
		role := "follower"
		switch nodeID {
		case 1:
			leaderID = 1
			term = 1
			role = "leader"
		case 3:
			role = "leader"
		}
		body := fmt.Sprintf(`{"metadata_group_id":1,"metadata_incarnation":"33333333333333333333333333333333","metadata_raft_local_node_id":%d,"metadata_raft_role":%q,"metadata_raft_leader_id":%d,"metadata_raft_term":%d,"metadata_raft_local_voter":true,"metadata_raft_voter_count":3,"metadata_raft_voter_set_fingerprint":%q,"metadata_raft_joint_consensus":false,"metadata_raft_learner_count":0}`,
			nodeID, role, leaderID, term, metadataRaftVoterSetFingerprint(3))
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}}

	startedAt := time.Unix(1_700_000_000, 0)
	err := reconciler.validateMetadataRuntimeTopologyAt(context.Background(), cluster, 3, startedAt, 250*time.Millisecond, 7*time.Second)
	var pending *metadataTopologyObservationPendingError
	if !stderrors.As(err, &pending) {
		t.Fatalf("initial error = %v, want pending leadership observation", err)
	}

	err = reconciler.validateMetadataRuntimeTopologyAt(context.Background(), cluster, 3, startedAt.Add(7*time.Second), 250*time.Millisecond, 7*time.Second)
	if err == nil {
		t.Fatal("expected persistent leadership disagreement to fail after the grace period")
	}
	if stderrors.As(err, &pending) {
		t.Fatalf("expired leadership error remained pending: %v", err)
	}
	var leadershipErr *metadataLeadershipObservationError
	if !stderrors.As(err, &leadershipErr) {
		t.Fatalf("expired error = %v, want leadership observation failure", err)
	}
	if retryAfter := reconciler.metadataTopologyObservationRequeueAfter(cluster); retryAfter != 0 {
		t.Fatalf("expired leadership observation retry = %v, want no busy requeue", retryAfter)
	}
}

func TestValidateMetadataRuntimeTopologyCarriesTransientProbeFailureAcrossReconciles(t *testing.T) {
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default", UID: "example-uid"},
		Spec: antflyv1.AntflyClusterSpec{MetadataNodes: antflyv1.MetadataNodesSpec{
			Replicas:    3,
			MetadataAPI: antflyv1.APISpec{Port: 12377},
		}},
	}

	var mu sync.Mutex
	requestCounts := make(map[string]int)
	var observedProbeTimeout time.Duration
	reconciler := &AntflyClusterReconciler{HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		host := req.URL.Hostname()
		mu.Lock()
		requestCounts[host]++
		attempt := requestCounts[host]
		if deadline, ok := req.Context().Deadline(); ok {
			observedProbeTimeout = time.Until(deadline)
		}
		mu.Unlock()
		if strings.Contains(host, "-0.") && attempt == 1 {
			return nil, fmt.Errorf("temporary connection reset")
		}

		nodeID := uint64(1)
		if strings.Contains(host, "-1.") {
			nodeID = 2
		} else if strings.Contains(host, "-2.") {
			nodeID = 3
		}
		role := "follower"
		if nodeID == 3 {
			role = "leader"
		}
		body := fmt.Sprintf(`{"metadata_group_id":1,"metadata_incarnation":"33333333333333333333333333333333","metadata_raft_local_node_id":%d,"metadata_raft_role":%q,"metadata_raft_leader_id":3,"metadata_raft_term":2,"metadata_raft_local_voter":true,"metadata_raft_voter_count":3,"metadata_raft_voter_set_fingerprint":%q,"metadata_raft_joint_consensus":false,"metadata_raft_learner_count":0}`,
			nodeID, role, metadataRaftVoterSetFingerprint(3))
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}}

	startedAt := time.Unix(1_700_000_000, 0)
	err := reconciler.validateMetadataRuntimeTopologyAt(
		context.Background(), cluster, 3, startedAt,
		metadataLeadershipObservationRetryInterval, metadataLeadershipObservationGracePeriod,
	)
	var pending *metadataTopologyObservationPendingError
	if !stderrors.As(err, &pending) {
		t.Fatalf("initial transient probe error = %v, want pending topology observation", err)
	}
	if pending.conditionReason != antflyv1.ReasonMetadataTopologyObservationPending {
		t.Fatalf("pending reason = %q, want %q", pending.conditionReason, antflyv1.ReasonMetadataTopologyObservationPending)
	}
	if pending.retryAfter != metadataRuntimeTopologyProbeRetryInterval {
		t.Fatalf("pending retry = %v, want %v", pending.retryAfter, metadataRuntimeTopologyProbeRetryInterval)
	}
	mu.Lock()
	probeTimeout := observedProbeTimeout
	mu.Unlock()
	if probeTimeout <= 0 || probeTimeout > metadataRuntimeTopologyProbeTimeout {
		t.Fatalf("probe context timeout = %v, want within (0, %v]", probeTimeout, metadataRuntimeTopologyProbeTimeout)
	}

	if err := reconciler.validateMetadataRuntimeTopologyAt(
		context.Background(), cluster, 3, startedAt.Add(time.Second),
		metadataLeadershipObservationRetryInterval, metadataLeadershipObservationGracePeriod,
	); err != nil {
		t.Fatalf("expected a recovered probe to validate successfully, got: %v", err)
	}
	if retryAfter := reconciler.metadataTopologyObservationRequeueAfter(cluster); retryAfter != 0 {
		t.Fatalf("recovered probe observation retry = %v, want none", retryAfter)
	}
}

func TestValidateMetadataRuntimeTopologyExpiresTransientProbeGrace(t *testing.T) {
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default", UID: "example-uid"},
		Spec: antflyv1.AntflyClusterSpec{MetadataNodes: antflyv1.MetadataNodesSpec{
			Replicas:    3,
			MetadataAPI: antflyv1.APISpec{Port: 12377},
		}},
	}
	reconciler := &AntflyClusterReconciler{HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("temporary connection reset")
	})}}
	startedAt := time.Unix(1_700_000_000, 0)
	err := reconciler.validateMetadataRuntimeTopologyAt(
		context.Background(), cluster, 3, startedAt,
		metadataLeadershipObservationRetryInterval, metadataLeadershipObservationGracePeriod,
	)
	var pending *metadataTopologyObservationPendingError
	if !stderrors.As(err, &pending) {
		t.Fatalf("initial transient probe error = %v, want pending topology observation", err)
	}

	err = reconciler.validateMetadataRuntimeTopologyAt(
		context.Background(), cluster, 3, startedAt.Add(metadataRuntimeTopologyProbeGracePeriod),
		metadataLeadershipObservationRetryInterval, metadataLeadershipObservationGracePeriod,
	)
	if err == nil {
		t.Fatal("expected persistent probe failure to fail after the grace period")
	}
	if stderrors.As(err, &pending) {
		t.Fatalf("expired probe error remained pending: %v", err)
	}
	var probeErr *metadataRuntimeTopologyProbeError
	if !stderrors.As(err, &probeErr) {
		t.Fatalf("expired error = %v, want runtime topology probe failure", err)
	}
	if retryAfter := reconciler.metadataTopologyObservationRequeueAfter(cluster); retryAfter != 0 {
		t.Fatalf("expired probe observation retry = %v, want none", retryAfter)
	}
}

func TestValidateMetadataRuntimeTopologyCountsProbeDurationAgainstGrace(t *testing.T) {
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default", UID: "example-uid"},
		Spec: antflyv1.AntflyClusterSpec{MetadataNodes: antflyv1.MetadataNodesSpec{
			Replicas:    3,
			MetadataAPI: antflyv1.APISpec{Port: 12377},
		}},
	}
	reconciler := &AntflyClusterReconciler{HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("probe timed out")
	})}}
	startedAt := time.Unix(1_700_000_000, 0)
	clockCalls := 0
	clock := func() time.Time {
		clockCalls++
		if clockCalls == 1 {
			return startedAt
		}
		return startedAt.Add(metadataRuntimeTopologyProbeGracePeriod)
	}

	err := reconciler.validateMetadataRuntimeTopologyWithClock(
		context.Background(), cluster, 3, clock,
		metadataLeadershipObservationRetryInterval, metadataLeadershipObservationGracePeriod,
	)
	if clockCalls != 2 {
		t.Fatalf("topology observation clock calls = %d, want start and completion readings", clockCalls)
	}
	var pending *metadataTopologyObservationPendingError
	if stderrors.As(err, &pending) {
		t.Fatalf("probe consuming the full grace remained pending: %v", err)
	}
	var probeErr *metadataRuntimeTopologyProbeError
	if !stderrors.As(err, &probeErr) {
		t.Fatalf("error after probe consumed grace = %v, want runtime topology probe failure", err)
	}
	if retryAfter := reconciler.metadataTopologyObservationRequeueAfter(cluster); retryAfter != 0 {
		t.Fatalf("expired delayed probe observation retry = %v, want none", retryAfter)
	}
}

func TestRecordMetadataTopologyOnPVCs(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:      "metadata-storage-example-metadata-0",
		Namespace: "default",
		Labels: map[string]string{
			"app.kubernetes.io/component": "data",
			"app.kubernetes.io/instance":  "another-cluster",
		},
	}}
	foreignPVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:      "metadata-storage-example-metadata-canary-metadata-0",
		Namespace: "default",
		Labels: map[string]string{
			"app.kubernetes.io/component": "metadata",
			"app.kubernetes.io/instance":  "example-metadata-canary",
		},
		Annotations: map[string]string{metadataTopologyReplicasAnnotation: "3"},
	}}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc, foreignPVC).Build()
	reconciler := &AntflyClusterReconciler{
		Client:         k8sClient,
		BoundaryReader: listRejectingReader{Reader: k8sClient},
	}
	cluster := &antflyv1.AntflyCluster{ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"}}

	if err := reconciler.recordMetadataTopologyOnPVCs(context.Background(), cluster, 1); err != nil {
		t.Fatal(err)
	}
	updated := &corev1.PersistentVolumeClaim{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(pvc), updated); err != nil {
		t.Fatal(err)
	}
	if got := updated.Annotations[metadataTopologyReplicasAnnotation]; got != "1" {
		t.Fatalf("metadata topology annotation = %q, want 1", got)
	}
	foreignUpdated := &corev1.PersistentVolumeClaim{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(foreignPVC), foreignUpdated); err != nil {
		t.Fatal(err)
	}
	if got := foreignUpdated.Annotations[metadataTopologyReplicasAnnotation]; got != "3" {
		t.Fatalf("foreign metadata topology annotation = %q, want unchanged value 3", got)
	}

	if err := reconciler.recordMetadataTopologyOnPVCs(context.Background(), cluster, 3); err == nil {
		t.Fatal("expected an existing PVC topology record to be immutable")
	}

	emptyClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	emptyReconciler := &AntflyClusterReconciler{
		Client:         emptyClient,
		BoundaryReader: listRejectingReader{Reader: emptyClient},
	}
	if err := emptyReconciler.recordMetadataTopologyOnPVCs(context.Background(), cluster, 3); err != nil {
		t.Fatalf("expected asynchronously created metadata PVCs to be retried, got: %v", err)
	}
}

func TestReconcileLegacyMetadataRuntimeMembershipStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	controller := true
	replicas := int32(3)
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default", UID: types.UID("cluster-uid")},
		Spec: antflyv1.AntflyClusterSpec{
			Image:           "antfly:new",
			ImagePullPolicy: string(corev1.PullAlways),
			MetadataNodes:   antflyv1.MetadataNodesSpec{Replicas: replicas},
		},
	}
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example-metadata",
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: antflyv1.GroupVersion.String(),
				Kind:       "AntflyCluster",
				Name:       cluster.Name,
				UID:        cluster.UID,
				Controller: &controller,
			}},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
					metadataMembershipStatusCapabilityAnnotation: "v1",
				}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{
					{Name: "sidecar", Image: "sidecar:old"},
					{Name: "antfly", Image: cluster.Spec.Image, ImagePullPolicy: corev1.PullAlways},
				}},
			},
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(statefulSet).Build()
	reconciler := &AntflyClusterReconciler{Client: k8sClient}

	rolloutState, err := reconciler.reconcileLegacyMetadataRuntimeMembershipStatus(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if rolloutState != metadataMembershipCapabilityRolloutStarted {
		t.Fatalf("rollout state = %v, want started", rolloutState)
	}
	updated := &appsv1.StatefulSet{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(statefulSet), updated); err != nil {
		t.Fatal(err)
	}
	if got := updated.Spec.Template.Spec.Containers[0].Image; got != "sidecar:old" {
		t.Fatalf("sidecar image = %q, want unchanged", got)
	}
	if got := updated.Spec.Template.Spec.Containers[1].Image; got != cluster.Spec.Image {
		t.Fatalf("metadata runtime image = %q, want %q", got, cluster.Spec.Image)
	}
	if got := updated.Spec.Template.Spec.Containers[1].ImagePullPolicy; got != corev1.PullAlways {
		t.Fatalf("metadata runtime pull policy = %q, want %q", got, corev1.PullAlways)
	}
	if got := updated.Spec.Template.Annotations[metadataMembershipStatusCapabilityAnnotation]; got != "v2" {
		t.Fatalf("metadata runtime membership capability annotation = %q, want v2", got)
	}
	if _, ok := updated.Annotations[metadataTopologyReplicasAnnotation]; ok {
		t.Fatal("runtime capability rollout must not record an unverified StatefulSet topology")
	}
	if _, ok := updated.Spec.Template.Annotations[metadataTopologyReplicasAnnotation]; ok {
		t.Fatal("runtime capability rollout must not record an unverified pod-template topology")
	}

	rolloutState, err = reconciler.reconcileLegacyMetadataRuntimeMembershipStatus(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if rolloutState != metadataMembershipCapabilityRolloutPending {
		t.Fatalf("incomplete rollout state = %v, want pending", rolloutState)
	}

	updated.Status.ObservedGeneration = updated.Generation
	updated.Status.UpdatedReplicas = replicas
	updated.Status.ReadyReplicas = replicas
	updated.Status.CurrentRevision = "revision-2"
	updated.Status.UpdateRevision = "revision-2"
	if err := k8sClient.Status().Update(context.Background(), updated); err != nil {
		t.Fatal(err)
	}
	rolloutState, err = reconciler.reconcileLegacyMetadataRuntimeMembershipStatus(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if rolloutState != metadataMembershipCapabilityUnavailable {
		t.Fatalf("completed rollout state = %v, want unavailable", rolloutState)
	}
	if got := rolloutState.requeueAfter(); got != 0 {
		t.Fatalf("completed incapable rollout requeue = %s, want none", got)
	}
}

func TestMetadataMembershipCapabilityRolloutStateRequeue(t *testing.T) {
	for _, state := range []metadataMembershipCapabilityRolloutState{
		metadataMembershipCapabilityRolloutStarted,
		metadataMembershipCapabilityRolloutPending,
	} {
		if got := state.requeueAfter(); got != metadataMembershipCapabilityRolloutInterval {
			t.Fatalf("rollout state %v requeue = %s, want %s", state, got, metadataMembershipCapabilityRolloutInterval)
		}
	}
}

func TestMetadataRaftVoterSetFingerprint(t *testing.T) {
	const want = "dfdd4aa4929437c8f1374d06653ce611ad794fe76e47ec84be3bd85a0d5a3230"
	if got := metadataRaftVoterSetFingerprint(3); got != want {
		t.Fatalf("metadata voter set fingerprint = %q, want %q", got, want)
	}
}

func TestPeriodicRequeueIncludesRecordedMetadataTopologyHealth(t *testing.T) {
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{Mode: antflyv1.ClusterModeDistributed},
		Status: antflyv1.AntflyClusterStatus{
			MetadataTopologyReplicas: 3,
		},
	}
	if got := periodicRequeueAfterAt(cluster, time.Unix(0, 0)); got != 30*time.Second {
		t.Fatalf("periodic metadata topology health requeue = %s, want 30s", got)
	}
}

func TestMetadataTopologyValidationErrorFailsClusterHealth(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := antflyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default", Generation: 7},
		Status: antflyv1.AntflyClusterStatus{
			Phase:              "Running",
			MetadataNodesReady: 3,
			Conditions: []metav1.Condition{
				{Type: antflyv1.TypeConfigurationValid, Status: metav1.ConditionFalse, Reason: antflyv1.ReasonValidationFailed, Message: "split metadata topology"},
				{Type: antflyv1.TypeMetadataReady, Status: metav1.ConditionTrue, Reason: antflyv1.ReasonComponentReady, Message: "metadata is ready"},
				{Type: antflyv1.TypeAvailable, Status: metav1.ConditionTrue, Reason: antflyv1.ReasonAvailable, Message: "Cluster is available"},
			},
		},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&antflyv1.AntflyCluster{}).
		WithObjects(cluster).
		Build()
	reconciler := &AntflyClusterReconciler{Client: k8sClient}
	current := &antflyv1.AntflyCluster{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), current); err != nil {
		t.Fatal(err)
	}
	validationErr := &metadataTopologyValidationError{cause: stderrors.New("split metadata topology")}
	if err := reconciler.updateStatusWithValidationError(context.Background(), current, validationErr); err != nil {
		t.Fatal(err)
	}
	updated := &antflyv1.AntflyCluster{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != "Degraded" {
		t.Fatalf("cluster phase = %q, want Degraded", updated.Status.Phase)
	}
	if updated.Status.MetadataNodesReady != 3 {
		t.Fatalf("ready metadata replica observation = %d, want preserved value 3", updated.Status.MetadataNodesReady)
	}
	for _, conditionType := range []string{antflyv1.TypeConfigurationValid, antflyv1.TypeMetadataReady, antflyv1.TypeAvailable} {
		condition := meta.FindStatusCondition(updated.Status.Conditions, conditionType)
		if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != antflyv1.ReasonValidationFailed {
			t.Fatalf("condition %s = %#v, want ValidationFailed=False", conditionType, condition)
		}
		if condition.ObservedGeneration != cluster.Generation {
			t.Fatalf("condition %s observed generation = %d, want %d", conditionType, condition.ObservedGeneration, cluster.Generation)
		}
	}
}

func TestUpdateStatusRejectsSteadyStateMetadataSplit(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := antflyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default", Generation: 7},
		Spec: antflyv1.AntflyClusterSpec{
			Mode:                antflyv1.ClusterModeDistributed,
			InternalServiceAuth: testInternalServiceAuthSpec(),
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas:    3,
				MetadataAPI: antflyv1.APISpec{Port: 12377},
			},
			DataNodes: antflyv1.DataNodesSpec{Replicas: 1},
		},
		Status: antflyv1.AntflyClusterStatus{
			ObservedGeneration:       7,
			MetadataTopologyReplicas: 3,
			Conditions: []metav1.Condition{{
				Type:               antflyv1.TypeConfigurationValid,
				Status:             metav1.ConditionTrue,
				ObservedGeneration: 7,
				Reason:             antflyv1.ReasonValidationPassed,
				Message:            "All validation rules passed",
			}},
		},
	}
	metadataReplicas := int32(3)
	dataReplicas := int32(1)
	metadataStatefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "example-metadata", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &metadataReplicas},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: metadataReplicas},
	}
	dataStatefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "example-data", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &dataReplicas},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: dataReplicas},
	}

	incarnations := map[string]string{
		"example-metadata-0.example-metadata.default.svc.cluster.local": "11111111111111111111111111111111",
		"example-metadata-1.example-metadata.default.svc.cluster.local": "22222222222222222222222222222222",
		"example-metadata-2.example-metadata.default.svc.cluster.local": "22222222222222222222222222222222",
	}
	httpClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		incarnation, ok := incarnations[req.URL.Hostname()]
		if !ok {
			return nil, fmt.Errorf("unexpected metadata status host %s", req.URL.Hostname())
		}
		nodeID := uint64(1)
		role := "follower"
		if strings.Contains(req.URL.Hostname(), "-1.") {
			nodeID = 2
		} else if strings.Contains(req.URL.Hostname(), "-2.") {
			nodeID = 3
			role = "leader"
		}
		body := fmt.Sprintf(`{"metadata_group_id":1,"metadata_incarnation":%q,"metadata_raft_local_node_id":%d,"metadata_raft_role":%q,"metadata_raft_leader_id":3,"metadata_raft_local_voter":true,"metadata_raft_voter_count":3,"metadata_raft_voter_set_fingerprint":%q,"metadata_raft_joint_consensus":false,"metadata_raft_learner_count":0}`,
			incarnation, nodeID, role, metadataRaftVoterSetFingerprint(3))
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&antflyv1.AntflyCluster{}).
		WithObjects(cluster, metadataStatefulSet, dataStatefulSet).
		Build()
	reconciler := &AntflyClusterReconciler{Client: k8sClient, HTTPClient: httpClient}
	current := &antflyv1.AntflyCluster{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), current); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.updateStatus(context.Background(), current); err != nil {
		t.Fatal(err)
	}

	updated := &antflyv1.AntflyCluster{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != "Degraded" {
		t.Fatalf("cluster phase = %q, want Degraded", updated.Status.Phase)
	}
	for _, conditionType := range []string{antflyv1.TypeMetadataReady, antflyv1.TypeAvailable} {
		condition := meta.FindStatusCondition(updated.Status.Conditions, conditionType)
		if condition == nil {
			t.Fatalf("missing %s condition", conditionType)
		}
		if condition.Status != metav1.ConditionFalse || condition.Reason != antflyv1.ReasonValidationFailed {
			t.Fatalf("%s condition = %#v, want false topology validation failure", conditionType, condition)
		}
		if !strings.Contains(condition.Message, "disagrees on metadata group or incarnation") {
			t.Fatalf("%s condition message = %q, want split-incarnation diagnostic", conditionType, condition.Message)
		}
	}
	configurationValid := meta.FindStatusCondition(updated.Status.Conditions, antflyv1.TypeConfigurationValid)
	if configurationValid == nil || configurationValid.Status != metav1.ConditionTrue {
		t.Fatalf("configuration condition = %#v, want unchanged valid configuration", configurationValid)
	}

	for host := range incarnations {
		incarnations[host] = "22222222222222222222222222222222"
	}
	if err := reconciler.updateStatus(context.Background(), updated); err != nil {
		t.Fatal(err)
	}
	recovered := &antflyv1.AntflyCluster{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), recovered); err != nil {
		t.Fatal(err)
	}
	if recovered.Status.Phase != "Running" {
		t.Fatalf("recovered cluster phase = %q, want Running", recovered.Status.Phase)
	}
	for _, conditionType := range []string{antflyv1.TypeMetadataReady, antflyv1.TypeAvailable} {
		condition := meta.FindStatusCondition(recovered.Status.Conditions, conditionType)
		if condition == nil || condition.Status != metav1.ConditionTrue {
			t.Fatalf("recovered %s condition = %#v, want true", conditionType, condition)
		}
	}
}

func TestUpdateStatusPreservesHealthyStatusDuringMetadataElectionGrace(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := antflyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	metadataReplicas := int32(3)
	dataReplicas := int32(1)
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default", UID: "example-uid", Generation: 1},
		Spec: antflyv1.AntflyClusterSpec{
			Mode: antflyv1.ClusterModeDistributed,
			MetadataNodes: antflyv1.MetadataNodesSpec{
				Replicas:    metadataReplicas,
				MetadataAPI: antflyv1.APISpec{Port: 12377},
			},
			DataNodes: antflyv1.DataNodesSpec{Replicas: dataReplicas},
		},
		Status: antflyv1.AntflyClusterStatus{
			Phase:                    "Running",
			MetadataNodesReady:       metadataReplicas,
			DataNodesReady:           dataReplicas,
			MetadataTopologyReplicas: metadataReplicas,
			Conditions: []metav1.Condition{
				{Type: antflyv1.TypeMetadataReady, Status: metav1.ConditionTrue, Reason: antflyv1.ReasonComponentReady, Message: "metadata is ready"},
				{Type: antflyv1.TypeAvailable, Status: metav1.ConditionTrue, Reason: antflyv1.ReasonAvailable, Message: "Cluster is available"},
			},
		},
	}
	metadataStatefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "example-metadata", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &metadataReplicas},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: metadataReplicas},
	}
	dataStatefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "example-data", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &dataReplicas},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: dataReplicas},
	}
	var requestMu sync.Mutex
	requestCounts := make(map[string]int)
	httpClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requestMu.Lock()
		requestCounts[req.URL.Hostname()]++
		requestMu.Unlock()
		nodeID := uint64(1)
		if strings.Contains(req.URL.Hostname(), "-1.") {
			nodeID = 2
		} else if strings.Contains(req.URL.Hostname(), "-2.") {
			nodeID = 3
		}
		leaderID := uint64(3)
		term := uint64(2)
		role := "follower"
		switch nodeID {
		case 1:
			leaderID = 1
			term = 1
			role = "leader"
		case 3:
			role = "leader"
		}
		body := fmt.Sprintf(`{"metadata_group_id":1,"metadata_incarnation":"33333333333333333333333333333333","metadata_raft_local_node_id":%d,"metadata_raft_role":%q,"metadata_raft_leader_id":%d,"metadata_raft_term":%d,"metadata_raft_local_voter":true,"metadata_raft_voter_count":3,"metadata_raft_voter_set_fingerprint":%q,"metadata_raft_joint_consensus":false,"metadata_raft_learner_count":0}`,
			nodeID, role, leaderID, term, metadataRaftVoterSetFingerprint(3))
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&antflyv1.AntflyCluster{}).
		WithObjects(cluster, metadataStatefulSet, dataStatefulSet).Build()
	reconciler := &AntflyClusterReconciler{Client: k8sClient, HTTPClient: httpClient}

	current := &antflyv1.AntflyCluster{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), current); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.updateStatus(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	requestMu.Lock()
	for host, count := range requestCounts {
		if count != 1 {
			requestMu.Unlock()
			t.Fatalf("metadata member %s request count = %d, want one observation per reconciliation", host, count)
		}
	}
	requestMu.Unlock()

	updated := &antflyv1.AntflyCluster{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != "Running" {
		t.Fatalf("phase during election grace = %q, want preserved Running", updated.Status.Phase)
	}
	for _, conditionType := range []string{antflyv1.TypeMetadataReady, antflyv1.TypeAvailable} {
		condition := meta.FindStatusCondition(updated.Status.Conditions, conditionType)
		if condition == nil || condition.Status != metav1.ConditionTrue {
			t.Fatalf("%s during election grace = %#v, want preserved true", conditionType, condition)
		}
	}
	if retryAfter := reconciler.metadataTopologyObservationRequeueAfter(updated); retryAfter <= 0 || retryAfter > metadataLeadershipObservationRetryInterval {
		t.Fatalf("leadership observation retry = %v, want within (0, %v]", retryAfter, metadataLeadershipObservationRetryInterval)
	}

	// A simultaneous data rollout is independent of the metadata election
	// grace. Its readiness transition must still make aggregate availability
	// pending instead of inheriting the previously healthy status.
	updated.Spec.DataNodes.Replicas = 3
	if err := reconciler.updateStatus(context.Background(), updated); err != nil {
		t.Fatal(err)
	}
	duringDataRollout := &antflyv1.AntflyCluster{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), duringDataRollout); err != nil {
		t.Fatal(err)
	}
	if duringDataRollout.Status.Phase != "Pending" {
		t.Fatalf("phase during overlapping data rollout = %q, want Pending", duringDataRollout.Status.Phase)
	}
	dataReady := meta.FindStatusCondition(duringDataRollout.Status.Conditions, antflyv1.TypeDataReady)
	if dataReady == nil || dataReady.Status != metav1.ConditionFalse || dataReady.Reason != antflyv1.ReasonWaitingForPods {
		t.Fatalf("DataReady during overlapping rollout = %#v, want waiting for pods", dataReady)
	}
	available := meta.FindStatusCondition(duringDataRollout.Status.Conditions, antflyv1.TypeAvailable)
	if available == nil || available.Status != metav1.ConditionFalse || available.Reason != antflyv1.ReasonWaitingForPods {
		t.Fatalf("Available during overlapping rollout = %#v, want waiting for pods", available)
	}

	key := metadataTopologyObservationKey(duringDataRollout)
	observed, ok := reconciler.metadataTopologyObservations.Load(key)
	if !ok {
		t.Fatal("missing pending leadership observation state")
	}
	state := observed.(metadataTopologyObservationState)
	state.deadline = time.Now()
	reconciler.metadataTopologyObservations.Store(key, state)
	if err := reconciler.updateStatus(context.Background(), duringDataRollout); err != nil {
		t.Fatal(err)
	}

	expired := &antflyv1.AntflyCluster{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), expired); err != nil {
		t.Fatal(err)
	}
	if expired.Status.Phase != "Degraded" {
		t.Fatalf("phase after election grace = %q, want Degraded", expired.Status.Phase)
	}
	for _, conditionType := range []string{antflyv1.TypeMetadataReady, antflyv1.TypeAvailable} {
		condition := meta.FindStatusCondition(expired.Status.Conditions, conditionType)
		if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != antflyv1.ReasonValidationFailed {
			t.Fatalf("%s after election grace = %#v, want validation failure", conditionType, condition)
		}
	}
}

func TestReconcileConfigMapPublishesExactGenerationAndFullHash(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := antflyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := &AntflyClusterReconciler{Client: client, Scheme: scheme}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "publication", Namespace: "default", Generation: 9},
		Spec:       antflyv1.AntflyClusterSpec{Config: `{"remote_content":{"default_s3":"primary"}}`},
	}

	if err := reconciler.reconcileConfigMap(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	configMap := &corev1.ConfigMap{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "publication-config", Namespace: "default"}, configMap); err != nil {
		t.Fatal(err)
	}
	wantSum := sha256.Sum256([]byte(configMap.Data["config.json"]))
	if cluster.Status.ConfigPublication == nil {
		t.Fatal("expected config publication status")
	}
	if cluster.Status.ConfigPublication.ObservedGeneration != cluster.Generation {
		t.Fatalf("published generation %d, want %d", cluster.Status.ConfigPublication.ObservedGeneration, cluster.Generation)
	}
	wantHash := fmt.Sprintf("%x", wantSum)
	if cluster.Status.ConfigPublication.SHA256 != wantHash {
		t.Fatalf("published hash %q, want %q", cluster.Status.ConfigPublication.SHA256, wantHash)
	}
	if configMap.Annotations[generatedConfigHashAnnotation] != wantHash[:16] {
		t.Fatalf("pod-template hash %q does not match publication prefix", configMap.Annotations[generatedConfigHashAnnotation])
	}
}

func TestSecretValueOnlyRotationDoesNotChangePodTemplateHash(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cloud-secrets-config", Namespace: "default"},
		Data:       map[string][]byte{"secrets.json": []byte(`{"value":"first"}`)},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	reconciler := &AntflyClusterReconciler{Client: client}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: secret.Namespace},
		Spec:       antflyv1.AntflyClusterSpec{Config: `{}`},
	}
	envFrom := []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{
		LocalObjectReference: corev1.LocalObjectReference{Name: secret.Name},
	}}}
	first := reconciler.buildPodAnnotations(context.Background(), newEnvFromCache(client), cluster, envFrom)

	secret.Data["secrets.json"] = []byte(`{"value":"other"}`)
	if err := client.Update(context.Background(), secret); err != nil {
		t.Fatal(err)
	}
	second := reconciler.buildPodAnnotations(context.Background(), newEnvFromCache(client), cluster, envFrom)
	if !maps.Equal(second, first) {
		t.Fatalf("secret value rotation changed pod-template annotations: before=%v after=%v", first, second)
	}
}

func TestCompatibilityRolloutGenerationPropagatesToPodTemplate(t *testing.T) {
	reconciler := &AntflyClusterReconciler{}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "example",
			Annotations: map[string]string{
				compatibilityRolloutGenerationAnnotation: "secret-sha256-0123456789abcdef",
			},
		},
		Spec: antflyv1.AntflyClusterSpec{Config: `{}`},
	}
	annotations := reconciler.buildPodAnnotations(context.Background(), newEnvFromCache(nil), cluster, nil)
	if got := annotations[compatibilityRolloutGenerationAnnotation]; got != "secret-sha256-0123456789abcdef" {
		t.Fatalf("compatibility rollout generation = %q", got)
	}
}

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

func TestCleanupDeletesExactUIDBoundHAStartupTargetPVC(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{antflyv1.AddToScheme, appsv1.AddToScheme, corev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	cluster := startupGatedStandaloneControllerCluster(false)
	cluster.UID = types.UID("cluster-uid")
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "standby-a-data", Namespace: "default", UID: types.UID("pvc-uid-1"),
		Labels: map[string]string{
			"app.kubernetes.io/instance": cluster.Name,
			labelClusterUID:              string(cluster.UID),
		},
	}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, pvc).Build()
	reconciler := &AntflyClusterReconciler{Client: client}
	result, err := reconciler.cleanupStorageResources(context.Background(), cluster)
	if err != nil || result != nil {
		t.Fatalf("cleanup failed: result=%v err=%v", result, err)
	}
	err = client.Get(context.Background(), types.NamespacedName{Name: pvc.Name, Namespace: pvc.Namespace}, &corev1.PersistentVolumeClaim{})
	if !errors.IsNotFound(err) {
		t.Fatalf("expected exact HA startup target PVC to be deleted, got %v", err)
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

// The default controller-runtime fake client runs updates through structured
// merge, which cannot represent this CRD's uint64 HA evidence fields. The basic
// client-go tracker still deep-copies objects and exercises the same client API,
// without introducing a fake-only product branch or weakening production status
// subresource writes.
func newHAControllerTestClient(t *testing.T, scheme *runtime.Scheme, objects ...client.Object) client.Client {
	t.Helper()
	tracker := k8stesting.NewObjectTracker(scheme, serializer.NewCodecFactory(scheme).UniversalDecoder())
	for _, object := range objects {
		copy := object.DeepCopyObject()
		if copyObject, ok := copy.(client.Object); ok && copyObject.GetResourceVersion() == "" {
			copyObject.SetResourceVersion("1")
		}
		if copyObject, ok := copy.(*antflyv1.AntflyCluster); ok && copyObject.UID == "" {
			copyObject.UID = types.UID("test-antflycluster-uid")
		}
		if err := tracker.Add(copy); err != nil {
			t.Fatalf("add %T to HA controller test tracker: %v", object, err)
		}
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjectTracker(tracker).WithStatusSubresource(&antflyv1.AntflyCluster{}).Build()
}

func reconcileHAAdminJobsUntilIdle(ctx context.Context, reconciler *AntflyClusterReconciler, cluster *antflyv1.AntflyCluster) error {
	for range 64 {
		err := reconciler.reconcileHAAdminJobs(ctx, cluster)
		if stderrors.Is(err, errHAStatusCheckpointed) {
			continue
		}
		if stderrors.Is(err, errHAPlanNeedsPersistence) {
			if persistErr := reconciler.persistHAActionPlanBarrier(ctx, cluster); persistErr != nil {
				return persistErr
			}
			continue
		}
		return err
	}
	return fmt.Errorf("HA admin reconciliation did not become idle after 64 durable checkpoints")
}

func durableHACreateSlotCluster(slotName string, targetLSN uint64) *antflyv1.AntflyCluster {
	action := antflyv1.HAPlannedActionStatus{
		Kind:         string(haActionCreateSlot),
		Phase:        string(haActionPhaseReconcile),
		Executor:     string(haActionExecutorAdminAPI),
		SlotName:     slotName,
		TargetLSN:    targetLSN,
		AdminCommand: []string{"slot", "create", "--slot", slotName, "--initial-lsn", fmt.Sprintf("%d", targetLSN)},
		AdminURL:     "http://primary-ha.default.svc:8081",
		AdminNodeID:  "primary-a",
		AdminMethod:  http.MethodPost,
		AdminPath:    haAdminReplicationSlotsPath,
		Reason:       "SlotMissing",
	}
	action.OperationID = haPlannedActionOperationID(action)
	return &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default", Generation: 1},
		Spec: antflyv1.AntflyClusterSpec{HighAvailability: &antflyv1.HighAvailabilitySpec{
			Mode: antflyv1.HAModeHotStandby,
			Admin: &antflyv1.HAAdminSpec{
				PrimaryURL:            action.AdminURL,
				ExecutePlannedActions: true,
			},
		}},
		Status: antflyv1.AntflyClusterStatus{HAStatus: &antflyv1.HAStatus{
			PlannedActions: []antflyv1.HAPlannedActionStatus{action},
		}},
	}
}

type conflictOnceHAResultClient struct {
	client.Client
	conflicts int
}

type concurrentHAReservationClient struct {
	client.Client
	conflicts int
	expires   time.Time
}

func (c *concurrentHAReservationClient) Status() client.SubResourceWriter {
	return &concurrentHAReservationWriter{SubResourceWriter: c.Client.Status(), client: c}
}

type concurrentHAReservationWriter struct {
	client.SubResourceWriter
	client *concurrentHAReservationClient
}

func (w *concurrentHAReservationWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	c := w.client
	cluster, ok := obj.(*antflyv1.AntflyCluster)
	if !ok || c.conflicts > 0 || cluster.Status.HAStatus == nil || len(cluster.Status.HAStatus.PlannedActions) == 0 {
		return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
	}
	proposed := cluster.Status.HAStatus.PlannedActions[0]
	if proposed.AdminJobPhase != haAdminJobPhaseRunning || proposed.InFlightAttempt == 0 {
		return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
	}
	latest := &antflyv1.AntflyCluster{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(cluster), latest); err != nil {
		return err
	}
	other := &latest.Status.HAStatus.PlannedActions[0]
	other.OperationID = proposed.OperationID
	other.ExecutionStateVersion = 1
	other.AttemptCount = 1
	other.InFlightAttempt = 1
	other.AttemptID = other.OperationID + "/attempt-1/other-controller"
	other.ReservationExpiresAt = haActionTime(c.expires)
	other.AdminJobName = haAdminDirectAPIName
	other.AdminJobPhase = haAdminJobPhaseRunning
	if err := c.Client.Status().Update(ctx, latest); err != nil {
		return err
	}
	c.conflicts++
	return errors.NewConflict(
		antflyv1.GroupVersion.WithResource("antflyclusters").GroupResource(),
		cluster.Name,
		fmt.Errorf("another reconciler reserved the action"),
	)
}

func (c *conflictOnceHAResultClient) Status() client.SubResourceWriter {
	return &conflictOnceHAResultWriter{SubResourceWriter: c.Client.Status(), client: c}
}

type conflictOnceHAResultWriter struct {
	client.SubResourceWriter
	client *conflictOnceHAResultClient
}

func (w *conflictOnceHAResultWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	c := w.client
	cluster, ok := obj.(*antflyv1.AntflyCluster)
	if !ok || c.conflicts > 0 || cluster.Status.HAStatus == nil || len(cluster.Status.HAStatus.PlannedActions) == 0 {
		return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
	}
	action := cluster.Status.HAStatus.PlannedActions[0]
	if action.AdminJobPhase != haAdminJobPhaseSucceeded || action.InFlightAttempt != 0 {
		return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
	}
	latest := &antflyv1.AntflyCluster{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(cluster), latest); err != nil {
		return err
	}
	latest.Status.Phase = "concurrent-status-update"
	if err := c.Client.Status().Update(ctx, latest); err != nil {
		return err
	}
	c.conflicts++
	return errors.NewConflict(
		antflyv1.GroupVersion.WithResource("antflyclusters").GroupResource(),
		cluster.Name,
		fmt.Errorf("injected result checkpoint conflict"),
	)
}

func haFenceResponseJSON(oldPrimaryID, promotedNodeID string, generation uint64, token string) string {
	return haFenceResponseJSONAtBoundary(oldPrimaryID, promotedNodeID, promotedNodeID, generation, token, 12)
}

func haFenceResponseJSONAtBoundary(oldPrimaryID, promotedNodeID, nodeID string, generation uint64, token string, boundary uint64) string {
	body, err := json.Marshal(map[string]any{
		"schema_version": 1,
		"action": map[string]any{
			"action_id":   "fence_acquire:" + promotedNodeID,
			"action_kind": "fence_acquire",
			"target":      promotedNodeID,
			"state":       "applied",
			"node_id":     nodeID,
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
			"required_lsn":       boundary,
			"observed_lsn":       boundary,
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
		Client: newHAControllerTestClient(t, s, cluster),
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
	// The planner and dispatcher are separated by a durable status barrier.
	// This fixture planned after seeding the fake API, so explicitly model the
	// outer reconcile persisting that plan before dispatch is allowed.
	g.Expect(reconciler.persistHAActionPlanBarrier(context.Background(), cluster)).To(Succeed())

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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
		Client: newHAControllerTestClient(t, s, cluster),
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
	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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
		Client: newHAControllerTestClient(t, s, cluster),
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

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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
		Client: newHAControllerTestClient(t, s, cluster),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			called = true
			return nil, fmt.Errorf("unexpected direct admin request: %s %s", req.Method, req.URL.String())
		})},
	}

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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
		Client: newHAControllerTestClient(t, s, cluster),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			called = true
			return nil, fmt.Errorf("unexpected direct admin request: %s %s", req.Method, req.URL.String())
		})},
	}

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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
	g.Expect(container.Env).To(HaveLen(2))
	g.Expect(container.Env[0].Name).To(Equal("MISSING_HA_ADMIN_TOKEN"))
	g.Expect(container.Env[0].ValueFrom).NotTo(BeNil())
	g.Expect(container.Env[0].ValueFrom.SecretKeyRef).NotTo(BeNil())
	g.Expect(container.Env[0].ValueFrom.SecretKeyRef.Name).To(Equal("ha-admin-token"))
	g.Expect(container.Env[0].ValueFrom.SecretKeyRef.Key).To(Equal("token"))
	g.Expect(container.Env[0].ValueFrom.SecretKeyRef.Optional).NotTo(BeNil())
	g.Expect(*container.Env[0].ValueFrom.SecretKeyRef.Optional).To(BeFalse())
	g.Expect(container.Env[1]).To(Equal(haPodUIDEnv()[0]))
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
		Client: newHAControllerTestClient(t, s, cluster),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			called = true
			return nil, fmt.Errorf("unexpected direct admin request: %s %s", req.Method, req.URL.String())
		})},
	}

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
	g.Expect(called).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).NotTo(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhasePending))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(BeEmpty())

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(HaveLen(1))
	container := jobs.Items[0].Spec.Template.Spec.Containers[0]
	g.Expect(container.EnvFrom).To(BeEmpty())
	g.Expect(container.Env).To(HaveLen(2))
	g.Expect(container.Env[0].Name).To(Equal("MISSING_HA_ADMIN_TOKEN"))
	g.Expect(container.Env[0].ValueFrom).NotTo(BeNil())
	g.Expect(container.Env[0].ValueFrom.SecretKeyRef).NotTo(BeNil())
	g.Expect(container.Env[0].ValueFrom.SecretKeyRef.Name).To(Equal("ha-admin-token"))
	g.Expect(container.Env[0].ValueFrom.SecretKeyRef.Key).To(Equal("token"))
	g.Expect(container.Env[0].ValueFrom.SecretKeyRef.Optional).NotTo(BeNil())
	g.Expect(*container.Env[0].ValueFrom.SecretKeyRef.Optional).To(BeFalse())
	g.Expect(container.Env[1]).To(Equal(haPodUIDEnv()[0]))
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
		Client: newHAControllerTestClient(t, s, cluster),
		Scheme: s,
	}

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).NotTo(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhasePending))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(BeEmpty())

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(HaveLen(1))
	container := jobs.Items[0].Spec.Template.Spec.Containers[0]
	g.Expect(container.Env).To(HaveLen(2))
	g.Expect(container.Env[0].Name).To(Equal("MISSING_HA_ADMIN_TOKEN"))
	g.Expect(container.Env[0].ValueFrom.SecretKeyRef.Name).To(Equal("ha-admin-token"))
	g.Expect(container.Env[0].ValueFrom.SecretKeyRef.Key).To(Equal("token"))
	g.Expect(container.Env[1]).To(Equal(haPodUIDEnv()[0]))
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
		Client: newHAControllerTestClient(t, s, cluster, secret),
		Scheme: s,
	}

	_, err := reconciler.haAdminBearerToken(cluster)
	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("configured HA admin token env var MISSING_HA_ADMIN_TOKEN is empty or unset"))
}

func TestReconcileHAAdminJobsRetainsMissingFailedFallbackJobAsTerminal(t *testing.T) {
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
		Client: newHAControllerTestClient(t, s, cluster),
		Scheme: s,
	}

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(Equal(haAdminJobName(cluster, action)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseFailed))

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(BeEmpty(), "a TTL-deleted terminal Job must not be recreated forever")
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
		Client: newHAControllerTestClient(t, s, cluster),
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

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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
		Client: newHAControllerTestClient(t, s, cluster),
		Scheme: s,
	}

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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
		Client: newHAControllerTestClient(t, s, cluster),
		Scheme: s,
	}

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())

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
		Client: newHAControllerTestClient(t, s, cluster),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			t.Fatalf("direct HA admin action without AdminNodeID must not issue HTTP request: %s %s", req.Method, req.URL.Path)
			return nil, nil
		})},
	}

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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
		Client: newHAControllerTestClient(t, s, cluster),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			t.Fatalf("AdminAPI action without typed request inputs must not fall back or issue HTTP request: %s %s", req.Method, req.URL.Path)
			return nil, nil
		})},
	}

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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
		Client: newHAControllerTestClient(t, s, cluster),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			t.Fatalf("blank-executor action must not issue typed admin API request: %s %s", req.Method, req.URL.Path)
			return nil, nil
		})},
	}

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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
		Client: newHAControllerTestClient(t, s, cluster),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			t.Fatalf("CLIJob action must not issue typed admin API request: %s %s", req.Method, req.URL.Path)
			return nil, nil
		})},
	}

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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
		Client: newHAControllerTestClient(t, s, cluster),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/replication-slots"))
			return &http.Response{
				StatusCode: http.StatusConflict,
				Body:       io.NopCloser(strings.NewReader("slot conflict")),
			}, nil
		})},
	}

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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
		Client: newHAControllerTestClient(t, s, cluster),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/replication-slots"))
			return &http.Response{
				StatusCode: http.StatusUnauthorized,
				Body:       io.NopCloser(strings.NewReader("missing bearer token")),
			}, nil
		})},
	}

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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
	now := time.Date(2026, 7, 14, 18, 0, 0, 0, time.UTC)

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
		Client: newHAControllerTestClient(t, s, cluster),
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
		Now: func() time.Time { return now },
	}

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhasePending))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminStatusCode).To(Equal(http.StatusServiceUnavailable))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(ContainSubstring("status 503"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(ContainSubstring("primary restarting"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AttemptCount).To(Equal(int32(1)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].RetryBudgetUsed).To(Equal(int32(1)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].Retryable).To(BeTrue())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].ErrorClass).To(Equal("HTTP503"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].NextRetryAt).NotTo(BeNil())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].NextRetryAt.Time.Equal(now.Add(defaultHADirectAdminRetryBase))).To(BeTrue())
	reconciler.updateHAAdminJobExecutionCondition(cluster)
	degraded := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	g.Expect(degraded).NotTo(BeNil())
	g.Expect(degraded.Status).To(Equal(metav1.ConditionTrue))
	g.Expect(degraded.Reason).To(Equal(antflyv1.ReasonHAAdminActionRetrying))
	g.Expect(degraded.Message).To(ContainSubstring("primary restarting"))

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
	g.Expect(requests).To(Equal(1), "retry must not run before persisted nextRetryAt")
	now = now.Add(defaultHADirectAdminRetryBase)
	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
	g.Expect(requests).To(Equal(2))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminError).To(BeEmpty())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminStatusCode).To(BeZero())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AttemptCount).To(Equal(int32(2)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].RetryBudgetUsed).To(Equal(int32(1)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].Retryable).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].CompletedAt).NotTo(BeNil())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult).NotTo(BeNil())

	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(BeEmpty())
}

func TestReconcileHAAdminJobsFailsClosedAfterRetryBudget(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	retryLimit := int32(2)
	retryBase := int32(1)
	now := time.Date(2026, 7, 14, 19, 0, 0, 0, time.UTC)
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{HighAvailability: &antflyv1.HighAvailabilitySpec{
			Mode: antflyv1.HAModeHotStandby,
			Admin: &antflyv1.HAAdminSpec{
				PrimaryURL: "http://primary-ha.default.svc:8081", ExecutePlannedActions: true,
				DirectRetryLimit: &retryLimit, DirectRetryBaseSeconds: &retryBase,
			},
		}},
		Status: antflyv1.AntflyClusterStatus{HAStatus: &antflyv1.HAStatus{PlannedActions: []antflyv1.HAPlannedActionStatus{{
			Kind: string(haActionCreateSlot), SlotName: "standby-a",
			AdminCommand: []string{"slot", "create", "--slot", "standby-a"},
			AdminURL:     "http://primary-ha.default.svc:8081", AdminNodeID: "primary-a",
		}}}},
	}
	requests := 0
	reconciler := &AntflyClusterReconciler{
		Client: newHAControllerTestClient(t, s, cluster), Scheme: s,
		Now: func() time.Time { return now },
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			requests++
			return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader("primary restarting"))}, nil
		})},
	}

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
	action := &cluster.Status.HAStatus.PlannedActions[0]
	g.Expect(action.AdminJobPhase).To(Equal(haAdminJobPhasePending))
	g.Expect(action.AttemptCount).To(Equal(int32(1)))
	g.Expect(action.RetryBudgetUsed).To(Equal(int32(1)))
	now = action.NextRetryAt.Time
	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
	g.Expect(requests).To(Equal(2))
	g.Expect(action.AdminJobPhase).To(Equal(haAdminJobPhaseFailed))
	g.Expect(action.AttemptCount).To(Equal(retryLimit))
	g.Expect(action.RetryBudgetUsed).To(Equal(retryLimit))
	g.Expect(action.Retryable).To(BeFalse())
	g.Expect(action.ErrorClass).To(Equal("RetryBudgetExhausted"))
	g.Expect(action.CompletedAt).NotTo(BeNil())
	reconciler.updateHAAdminJobExecutionCondition(cluster)
	degraded := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeHADegraded)
	g.Expect(degraded).NotTo(BeNil())
	g.Expect(degraded.Reason).To(Equal(antflyv1.ReasonHAAdminRetryBudgetExhausted))
	g.Expect(degraded.Message).To(ContainSubstring("after 2 attempt(s)"))
	g.Expect(degraded.Message).To(ContainSubstring("class RetryBudgetExhausted"))

	now = now.Add(time.Hour)
	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
	g.Expect(requests).To(Equal(2), "terminal retry exhaustion must survive subsequent reconciles")
}

func TestHADirectActionCheckpointsReservationBeforeDispatchAndStopsAfterOneSideEffect(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())

	cluster := durableHACreateSlotCluster("standby-a", 5)
	second := cluster.Status.HAStatus.PlannedActions[0]
	second.SlotName = "standby-b"
	second.TargetLSN = 6
	second.AdminCommand = []string{"slot", "create", "--slot", "standby-b", "--initial-lsn", "6"}
	second.OperationID = haPlannedActionOperationID(second)
	cluster.Status.HAStatus.PlannedActions = append(cluster.Status.HAStatus.PlannedActions, second)
	apiClient := newHAControllerTestClient(t, s, cluster)
	requests := 0
	reconciler := &AntflyClusterReconciler{
		Client: apiClient,
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			requests++
			var payload map[string]any
			g.Expect(json.NewDecoder(req.Body).Decode(&payload)).To(Succeed())
			slotName, _ := payload["slot_name"].(string)

			persisted := &antflyv1.AntflyCluster{}
			g.Expect(apiClient.Get(req.Context(), client.ObjectKeyFromObject(cluster), persisted)).To(Succeed())
			var reserved *antflyv1.HAPlannedActionStatus
			for i := range persisted.Status.HAStatus.PlannedActions {
				if persisted.Status.HAStatus.PlannedActions[i].SlotName == slotName {
					reserved = &persisted.Status.HAStatus.PlannedActions[i]
					break
				}
			}
			g.Expect(reserved).NotTo(BeNil())
			g.Expect(reserved.AdminJobPhase).To(Equal(haAdminJobPhaseRunning))
			g.Expect(reserved.InFlightAttempt).To(Equal(int32(1)))
			g.Expect(reserved.AttemptID).NotTo(BeEmpty())
			g.Expect(reserved.ReservationExpiresAt).NotTo(BeNil())
			g.Expect(reserved.AdminResult).To(BeNil(), "result evidence cannot exist before the external response")
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(haReplicationSlotActionResponseJSON("replication_slot_create", "create", slotName, "primary-a"))),
			}, nil
		})},
	}

	err := reconciler.reconcileHAAdminJobs(context.Background(), cluster)
	g.Expect(err).To(MatchError(errHAStatusCheckpointed))
	g.Expect(requests).To(Equal(1), "a checkpoint boundary must end the reconcile before a second external action")
	persisted := &antflyv1.AntflyCluster{}
	g.Expect(apiClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), persisted)).To(Succeed())
	first := persisted.Status.HAStatus.PlannedActions[0]
	g.Expect(first.AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
	g.Expect(first.InFlightAttempt).To(BeZero())
	g.Expect(first.AttemptID).To(BeEmpty())
	g.Expect(first.ReservationExpiresAt).To(BeNil())
	g.Expect(first.AdminResult).NotTo(BeNil())
	g.Expect(first.AdminResult.SlotName).To(Equal("standby-a"))
	g.Expect(persisted.Status.HAStatus.PlannedActions[1].AdminJobPhase).To(BeEmpty())

	err = reconciler.reconcileHAAdminJobs(context.Background(), cluster)
	g.Expect(err).To(MatchError(errHAStatusCheckpointed))
	g.Expect(requests).To(Equal(2))
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
}

func TestHADirectActionReservationConflictNeverDispatchesUsingAnotherOwnersLease(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	now := time.Date(2026, 7, 14, 20, 30, 0, 0, time.UTC)
	cluster := durableHACreateSlotCluster("standby-a", 5)
	baseClient := newHAControllerTestClient(t, s, cluster)
	reservationClient := &concurrentHAReservationClient{Client: baseClient, expires: now.Add(time.Minute)}
	requests := 0
	reconciler := &AntflyClusterReconciler{
		Client: reservationClient,
		Scheme: s,
		Now:    func() time.Time { return now },
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			requests++
			return nil, fmt.Errorf("losing reconciler must not dispatch")
		})},
	}

	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	g.Expect(reservationClient.conflicts).To(Equal(1))
	g.Expect(requests).To(BeZero())
	action := cluster.Status.HAStatus.PlannedActions[0]
	g.Expect(action.InFlightAttempt).To(Equal(int32(1)))
	g.Expect(action.AttemptID).To(ContainSubstring("/other-controller"))
	g.Expect(action.ReservationExpiresAt).NotTo(BeNil())
	g.Expect(action.ReservationExpiresAt.After(now)).To(BeTrue())
}

func TestHADirectActionCrashReplayWaitsForLeaseAndUsesExactFrozenPayload(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	now := time.Date(2026, 7, 14, 21, 0, 0, 0, time.UTC)
	reservationSeconds := int32(10)
	cluster := durableHACreateSlotCluster("standby-a", 5)
	cluster.Spec.HighAvailability.Admin.DirectReservationSeconds = &reservationSeconds
	apiClient := newHAControllerTestClient(t, s, cluster)
	requests := 0
	var payloads []map[string]any
	var attemptIDs []string
	reconciler := &AntflyClusterReconciler{
		Client: apiClient,
		Scheme: s,
		Now:    func() time.Time { return now },
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			requests++
			var payload map[string]any
			g.Expect(json.NewDecoder(req.Body).Decode(&payload)).To(Succeed())
			payloads = append(payloads, payload)
			persisted := &antflyv1.AntflyCluster{}
			g.Expect(apiClient.Get(req.Context(), client.ObjectKeyFromObject(cluster), persisted)).To(Succeed())
			attemptIDs = append(attemptIDs, persisted.Status.HAStatus.PlannedActions[0].AttemptID)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(haReplicationSlotActionResponseJSON("replication_slot_create", "create", "standby-a", "primary-a"))),
			}, nil
		})},
	}
	action := &cluster.Status.HAStatus.PlannedActions[0]
	reserved, checkpointed, err := reconciler.reserveHADirectAdminAttempt(context.Background(), cluster, cluster.Spec.HighAvailability.Admin, action, now)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(reserved).To(BeTrue())
	g.Expect(checkpointed).To(BeTrue())
	firstAttemptID := action.AttemptID
	handled, err := reconciler.executeHAPlannedActionTyped(context.Background(), cluster, action)
	g.Expect(handled).To(BeTrue())
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(requests).To(Equal(1))

	// Simulate process death after the external response but before its result
	// checkpoint by discarding the in-memory result and reloading the CR.
	restarted := &antflyv1.AntflyCluster{}
	g.Expect(apiClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), restarted)).To(Succeed())
	g.Expect(restarted.Status.HAStatus.PlannedActions[0].AdminResult).To(BeNil())
	g.Expect(restarted.Status.HAStatus.PlannedActions[0].AttemptID).To(Equal(firstAttemptID))
	now = now.Add(9 * time.Second)
	g.Expect(reconciler.reconcileHAAdminJobs(context.Background(), restarted)).To(Succeed())
	g.Expect(requests).To(Equal(1), "an unexpired reservation must suppress immediate crash replay")

	now = now.Add(time.Second)
	err = reconciler.reconcileHAAdminJobs(context.Background(), restarted)
	g.Expect(err).To(MatchError(errHAStatusCheckpointed))
	g.Expect(requests).To(Equal(2))
	g.Expect(payloads).To(HaveLen(2))
	g.Expect(payloads[1]).To(Equal(payloads[0]), "crash replay must use the exact frozen idempotent request")
	g.Expect(attemptIDs).To(HaveLen(2))
	g.Expect(attemptIDs[0]).NotTo(BeEmpty())
	g.Expect(attemptIDs[1]).NotTo(Equal(attemptIDs[0]), "each replay reservation must have a distinct ownership token")
	g.Expect(restarted.Status.HAStatus.PlannedActions[0].AttemptCount).To(Equal(int32(2)))
	g.Expect(restarted.Status.HAStatus.PlannedActions[0].RetryBudgetUsed).To(Equal(int32(1)), "an expired uncertain dispatch must consume replay budget")
	g.Expect(restarted.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
}

func TestHADirectActionExpiredUncertainLeaseCannotExceedRetryBudget(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	now := time.Date(2026, 7, 14, 21, 30, 0, 0, time.UTC)
	reservationSeconds := int32(1)
	retryLimit := int32(1)
	cluster := durableHACreateSlotCluster("standby-a", 5)
	cluster.Spec.HighAvailability.Admin.DirectReservationSeconds = &reservationSeconds
	cluster.Spec.HighAvailability.Admin.DirectRetryLimit = &retryLimit
	apiClient := newHAControllerTestClient(t, s, cluster)
	requests := 0
	reconciler := &AntflyClusterReconciler{
		Client: apiClient,
		Scheme: s,
		Now:    func() time.Time { return now },
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			requests++
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(haReplicationSlotActionResponseJSON("replication_slot_create", "create", "standby-a", "primary-a"))),
			}, nil
		})},
	}
	action := &cluster.Status.HAStatus.PlannedActions[0]
	reserved, _, err := reconciler.reserveHADirectAdminAttempt(context.Background(), cluster, cluster.Spec.HighAvailability.Admin, action, now)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(reserved).To(BeTrue())
	_, err = reconciler.executeHAPlannedActionTyped(context.Background(), cluster, action)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(requests).To(Equal(1))

	restarted := &antflyv1.AntflyCluster{}
	g.Expect(apiClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), restarted)).To(Succeed())
	now = now.Add(time.Second)
	err = reconciler.reconcileHAAdminJobs(context.Background(), restarted)
	g.Expect(err).To(MatchError(errHAStatusCheckpointed))
	g.Expect(requests).To(Equal(1), "expired uncertainty must fail closed once replay budget is exhausted")
	failed := restarted.Status.HAStatus.PlannedActions[0]
	g.Expect(failed.AttemptCount).To(Equal(int32(1)))
	g.Expect(failed.RetryBudgetUsed).To(Equal(int32(1)))
	g.Expect(failed.AdminJobPhase).To(Equal(haAdminJobPhaseFailed))
	g.Expect(failed.ErrorClass).To(Equal("RetryBudgetExhausted"))
}

func TestHADirectPrerequisiteDeadlineWinsOverLaterPollAndFailureBudget(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	now := time.Date(2026, 7, 14, 21, 45, 0, 0, time.UTC)
	cluster := durableHACreateSlotCluster("standby-a", 5)
	action := &cluster.Status.HAStatus.PlannedActions[0]
	action.ExecutionStateVersion = 1
	action.AdminJobName = haAdminDirectAPIName
	action.AdminJobPhase = haAdminJobPhaseWaitingPrerequisite
	action.AttemptCount = 4
	action.RetryBudgetUsed = 0
	action.NextRetryAt = haActionTime(now.Add(time.Minute))
	action.PrerequisiteDeadlineAt = haActionTime(now)
	apiClient := newHAControllerTestClient(t, s, cluster)
	requests := 0
	reconciler := &AntflyClusterReconciler{
		Client: apiClient,
		Scheme: s,
		Now:    func() time.Time { return now },
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			requests++
			return nil, fmt.Errorf("unexpected request")
		})},
	}

	err := reconciler.reconcileHAAdminJobs(context.Background(), cluster)
	g.Expect(err).To(MatchError(errHAStatusCheckpointed))
	g.Expect(requests).To(BeZero())
	g.Expect(action.AdminJobPhase).To(Equal(haAdminJobPhaseFailed))
	g.Expect(action.ErrorClass).To(Equal("PromotionPrerequisiteTimeout"))
	g.Expect(action.AttemptCount).To(Equal(int32(4)))
	g.Expect(action.RetryBudgetUsed).To(BeZero())

	requeueCluster := cluster.DeepCopy()
	requeueAction := &requeueCluster.Status.HAStatus.PlannedActions[0]
	requeueAction.AdminJobPhase = haAdminJobPhaseWaitingPrerequisite
	requeueAction.CompletedAt = nil
	requeueAction.NextRetryAt = haActionTime(now.Add(time.Minute))
	requeueAction.PrerequisiteDeadlineAt = haActionTime(now.Add(3 * time.Second))
	g.Expect(haDirectAdminRetryRequeueAfter(requeueCluster, now)).To(Equal(3 * time.Second))
}

func TestHADirectActionResultConflictRetriesCheckpointWithoutRepeatingRequest(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	cluster := durableHACreateSlotCluster("standby-a", 5)
	baseClient := newHAControllerTestClient(t, s, cluster)
	conflictClient := &conflictOnceHAResultClient{Client: baseClient}
	requests := 0
	reconciler := &AntflyClusterReconciler{
		Client: conflictClient,
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			requests++
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(haReplicationSlotActionResponseJSON("replication_slot_create", "create", "standby-a", "primary-a"))),
			}, nil
		})},
	}

	err := reconciler.reconcileHAAdminJobs(context.Background(), cluster)
	g.Expect(err).To(MatchError(errHAStatusCheckpointed))
	g.Expect(requests).To(Equal(1))
	g.Expect(conflictClient.conflicts).To(Equal(1))
	persisted := &antflyv1.AntflyCluster{}
	g.Expect(baseClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), persisted)).To(Succeed())
	g.Expect(persisted.Status.Phase).To(Equal("concurrent-status-update"), "narrow result retry must preserve unrelated concurrent status")
	g.Expect(persisted.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
	g.Expect(persisted.Status.HAStatus.PlannedActions[0].AdminResult).NotTo(BeNil())
}

func TestHADirectActionRejectsAbsentOrReplacedPersistedPlanWithoutDispatch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*antflyv1.AntflyCluster)
	}{
		{
			name: "absent",
			mutate: func(cluster *antflyv1.AntflyCluster) {
				cluster.Status.HAStatus.PlannedActions = nil
			},
		},
		{
			name: "replaced",
			mutate: func(cluster *antflyv1.AntflyCluster) {
				action := &cluster.Status.HAStatus.PlannedActions[0]
				action.SlotName = "standby-b"
				action.OperationID = haPlannedActionOperationID(*action)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			s := runtime.NewScheme()
			g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
			cluster := durableHACreateSlotCluster("standby-a", 5)
			apiClient := newHAControllerTestClient(t, s, cluster)
			latest := &antflyv1.AntflyCluster{}
			g.Expect(apiClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), latest)).To(Succeed())
			tc.mutate(latest)
			g.Expect(apiClient.Status().Update(context.Background(), latest)).To(Succeed())
			requests := 0
			reconciler := &AntflyClusterReconciler{
				Client: apiClient,
				Scheme: s,
				HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					requests++
					return nil, fmt.Errorf("unexpected request")
				})},
			}

			err := reconciler.reconcileHAAdminJobs(context.Background(), cluster)
			g.Expect(stderrors.Is(err, errHAPlanNeedsPersistence)).To(BeTrue(), "got %v", err)
			g.Expect(requests).To(BeZero())
		})
	}
}

func TestHADirectReservationRequiresExactCurrentPersistedPayload(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	cluster := durableHACreateSlotCluster("standby-a", 5)
	apiClient := newHAControllerTestClient(t, s, cluster)
	latest := &antflyv1.AntflyCluster{}
	g.Expect(apiClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), latest)).To(Succeed())
	// TargetLSN is intentionally outside the stable operation ID. A concurrent
	// plan can therefore have the same semantic identity but a different exact
	// request payload, which reservation must reject rather than silently retarget.
	latest.Status.HAStatus.PlannedActions[0].TargetLSN = 6
	g.Expect(apiClient.Status().Update(context.Background(), latest)).To(Succeed())
	reconciler := &AntflyClusterReconciler{Client: apiClient, Scheme: s}

	reserved, _, err := reconciler.reserveHADirectAdminAttempt(
		context.Background(),
		cluster,
		cluster.Spec.HighAvailability.Admin,
		&cluster.Status.HAStatus.PlannedActions[0],
		time.Date(2026, 7, 14, 22, 0, 0, 0, time.UTC),
	)
	g.Expect(reserved).To(BeFalse())
	g.Expect(stderrors.Is(err, errHADirectOperationNotPersisted)).To(BeTrue(), "got %v", err)
	persisted := &antflyv1.AntflyCluster{}
	g.Expect(apiClient.Get(context.Background(), client.ObjectKeyFromObject(cluster), persisted)).To(Succeed())
	g.Expect(persisted.Status.HAStatus.PlannedActions[0].InFlightAttempt).To(BeZero())
	g.Expect(persisted.Status.HAStatus.PlannedActions[0].AttemptCount).To(BeZero())
}

func TestHADirectActionLegacyFailedStatusMigratesBeforeOneBoundedReplay(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	now := time.Date(2026, 7, 14, 22, 0, 0, 0, time.UTC)
	retryLimit := int32(2)
	cluster := durableHACreateSlotCluster("standby-a", 5)
	cluster.Spec.HighAvailability.Admin.DirectRetryLimit = &retryLimit
	legacyJSON := `{"kind":"CreateSlot","phase":"Reconcile","executor":"AdminAPI","slotName":"standby-a","targetLSN":5,"adminCommand":["slot","create","--slot","standby-a","--initial-lsn","5"],"adminURL":"http://primary-ha.default.svc:8081","adminNodeID":"primary-a","adminMethod":"POST","adminPath":"/admin/v1/ha/replication-slots","adminJobName":"direct-admin-api","adminJobPhase":"Failed","adminError":"legacy retryable failure","retryable":true,"reason":"SlotMissing"}`
	var legacy antflyv1.HAPlannedActionStatus
	g.Expect(json.Unmarshal([]byte(legacyJSON), &legacy)).To(Succeed())
	legacy.OperationID = haPlannedActionOperationID(legacy)
	cluster.Status.HAStatus.PlannedActions[0] = legacy
	apiClient := newHAControllerTestClient(t, s, cluster)
	requests := 0
	reconciler := &AntflyClusterReconciler{
		Client: apiClient,
		Scheme: s,
		Now:    func() time.Time { return now },
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			requests++
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(haReplicationSlotActionResponseJSON("replication_slot_create", "create", "standby-a", "primary-a"))),
			}, nil
		})},
	}

	err := reconciler.reconcileHAAdminJobs(context.Background(), cluster)
	g.Expect(err).To(MatchError(errHAStatusCheckpointed))
	g.Expect(requests).To(BeZero(), "legacy status migration is its own durable barrier")
	migrated := cluster.Status.HAStatus.PlannedActions[0]
	g.Expect(migrated.ExecutionStateVersion).To(Equal(int32(1)))
	g.Expect(migrated.AttemptCount).To(Equal(int32(1)))
	g.Expect(migrated.RetryBudgetUsed).To(Equal(int32(1)))
	g.Expect(migrated.AdminJobPhase).To(Equal(haAdminJobPhasePending))
	g.Expect(migrated.NextRetryAt).NotTo(BeNil())

	err = reconciler.reconcileHAAdminJobs(context.Background(), cluster)
	g.Expect(err).To(MatchError(errHAStatusCheckpointed))
	g.Expect(requests).To(Equal(1))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AttemptCount).To(Equal(int32(2)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].RetryBudgetUsed).To(Equal(int32(1)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
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
		Client: newHAControllerTestClient(t, s, cluster),
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

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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
		Client: newHAControllerTestClient(t, s, cluster),
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

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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
		Client: newHAControllerTestClient(t, s, cluster),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			t.Fatalf("seed without target LSN must fail before HTTP request, got %s %s", req.Method, req.URL.String())
			return nil, nil
		})},
	}

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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
		Client: newHAControllerTestClient(t, s, cluster),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			t.Fatalf("mismatched typed admin metadata must fail before issuing HTTP request: %s %s", req.Method, req.URL.Path)
			return nil, nil
		})},
	}

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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
	apiClient := newHAControllerTestClient(t, s, cluster)
	reconciler := &AntflyClusterReconciler{
		Client: apiClient,
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

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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

	var persisted antflyv1.AntflyCluster
	g.Expect(reconciler.Get(
		context.Background(),
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace},
		&persisted,
	)).To(Succeed())
	g.Expect(persisted.Status.HAStatus).NotTo(BeNil())
	g.Expect(persisted.Status.HAStatus.LastPromotion).NotTo(BeNil())
	g.Expect(persisted.Status.HAStatus.LastPromotion.FenceToken).To(Equal("ha-fence-token"))
	g.Expect(persisted.Status.Phase).To(BeEmpty())
}

func TestReconcileHAAdminJobsFreezesFormerPrimaryTailBeforeCandidateFenceAndAssessment(t *testing.T) {
	g := NewWithT(t)
	now := time.Date(2026, 7, 14, 20, 0, 0, 0, time.UTC)
	retryLimit := int32(1)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:            "http://primary-ha.default.svc:8081",
					ExecutePlannedActions: true,
					DirectRetryLimit:      &retryLimit,
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
					Kind:            string(haActionFenceFormerPrimary),
					StandbyName:     "primary-a",
					TargetLSN:       12,
					RouteFrom:       "primary-a",
					RouteTo:         "standby-a",
					FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
					FenceHolder:     "standby-a",
					FenceGeneration: 3,
					FenceReason:     "LeaseAcquired",
					AdminURL:        "http://primary-ha.default.svc:8081",
					AdminNodeID:     "primary-a",
					AdminMethod:     http.MethodPost,
					AdminPath:       "/admin/v1/ha/fence",
				}, {
					Kind:            string(haActionAcquireFence),
					DependsOn:       string(haActionFenceFormerPrimary),
					StandbyName:     "standby-a",
					TargetLSN:       12,
					FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
					FenceHolder:     "standby-a",
					FenceGeneration: 3,
					FenceReason:     "LeaseAcquired",
					AdminURL:        "http://standby-a-ha.default.svc:8081",
					AdminNodeID:     "standby-a",
					AdminMethod:     http.MethodPost,
					AdminPath:       "/admin/v1/ha/fence",
				}, {
					Kind:            string(haActionAssessPromotion),
					DependsOn:       string(haActionAcquireFence),
					StandbyName:     "standby-a",
					TargetLSN:       12,
					FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
					FenceHolder:     "standby-a",
					FenceGeneration: 3,
					FenceReason:     "LeaseAcquired",
					AdminURL:        "http://standby-a-ha.default.svc:8081",
					AdminNodeID:     "standby-a",
					AdminMethod:     http.MethodPost,
					AdminPath:       "/admin/v1/ha/promotion/assess",
				}},
			},
		},
	}

	var observed []string
	assessmentCalls := 0
	reconciler := &AntflyClusterReconciler{
		Client: newHAControllerTestClient(t, s, cluster),
		Scheme: s,
		Now:    func() time.Time { return now },
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			observed = append(observed, req.URL.Host+" "+req.Method+" "+req.URL.Path)
			var payload map[string]any
			g.Expect(json.NewDecoder(req.Body).Decode(&payload)).To(Succeed())
			switch {
			case req.URL.Host == "primary-ha.default.svc:8081" && req.URL.Path == "/admin/v1/ha/fence":
				g.Expect(payload["required_lsn"]).To(Equal(float64(12)))
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body: io.NopCloser(strings.NewReader(haFenceResponseJSONAtBoundary(
						"primary-a", "standby-a", "primary-a", 3, "tail-17-token", 17,
					))),
				}, nil
			case req.URL.Host == "standby-a-ha.default.svc:8081" && req.URL.Path == "/admin/v1/ha/fence":
				g.Expect(payload["required_lsn"]).To(Equal(float64(17)))
				g.Expect(payload["observed_lsn"]).To(Equal(float64(17)))
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body: io.NopCloser(strings.NewReader(haFenceResponseJSONAtBoundary(
						"primary-a", "standby-a", "standby-a", 3, "tail-17-token", 17,
					))),
				}, nil
			case req.URL.Host == "standby-a-ha.default.svc:8081" && req.URL.Path == "/admin/v1/ha/promotion/assess":
				g.Expect(payload["required_lsn"]).To(Equal(float64(17)))
				assessmentCalls++
				if assessmentCalls == 1 {
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     http.Header{"Content-Type": []string{"application/json"}},
						Body:       io.NopCloser(strings.NewReader(`{"schema_version":1,"action":{"action_id":"promotion_assess:standby-a","action_kind":"promotion_assess","target":"standby-a","state":"assessed","node_id":"standby-a"},"assessment":{"required_lsn":17,"received_lsn":16,"applied_lsn":16,"has_required_lsn":false,"caught_up_to_received":true,"fencing_confirmed":true,"force":false,"mode":"blocked","data_loss_possible":true,"safe":false,"requires_fencing":false,"requires_force":true,"can_promote":false}}`)),
					}, nil
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(`{"schema_version":1,"action":{"action_id":"promotion_assess:standby-a","action_kind":"promotion_assess","target":"standby-a","state":"assessed","node_id":"standby-a"},"assessment":{"required_lsn":17,"received_lsn":17,"applied_lsn":17,"has_required_lsn":true,"caught_up_to_received":true,"fencing_confirmed":true,"force":false,"mode":"safe","data_loss_possible":false,"safe":true,"requires_fencing":false,"requires_force":false,"can_promote":true}}`)),
				}, nil
			default:
				t.Fatalf("unexpected HA admin request: %s %s", req.URL.Host, req.URL.Path)
				return nil, nil
			}
		})},
	}

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminJobPhase).To(Equal(haAdminJobPhaseWaitingPrerequisite))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminError).To(ContainSubstring("has not applied the frozen former-primary boundary"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AttemptCount).To(Equal(int32(1)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].RetryBudgetUsed).To(BeZero())
	now = cluster.Status.HAStatus.PlannedActions[2].NextRetryAt.Time
	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
	g.Expect(observed).To(Equal([]string{
		"primary-ha.default.svc:8081 POST /admin/v1/ha/fence",
		"standby-a-ha.default.svc:8081 POST /admin/v1/ha/fence",
		"standby-a-ha.default.svc:8081 POST /admin/v1/ha/promotion/assess",
		"standby-a-ha.default.svc:8081 POST /admin/v1/ha/promotion/assess",
	}))
	for i := range cluster.Status.HAStatus.PlannedActions {
		g.Expect(cluster.Status.HAStatus.PlannedActions[i].AdminJobPhase).To(Equal(haAdminJobPhaseSucceeded))
	}
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.FenceRequiredLSN).To(Equal(uint64(17)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].TargetLSN).To(Equal(uint64(17)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminResult.FenceToken).To(Equal("tail-17-token"))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].TargetLSN).To(Equal(uint64(17)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AdminResult.PromotionAppliedLSN).To(Equal(uint64(17)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].AttemptCount).To(Equal(int32(2)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[2].RetryBudgetUsed).To(BeZero(), "valid prerequisite polling must not consume a request-failure budget of one")
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
				Client: newHAControllerTestClient(t, s, cluster),
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

			g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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
		Client: newHAControllerTestClient(t, s, cluster),
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

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
	g.Expect(observed).To(Equal([]string{"POST /admin/v1/ha/fence"}))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobName).To(Equal(haAdminDirectAPIName))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminJobPhase).To(Equal(haAdminJobPhaseFailed))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult).To(BeNil())
	g.Expect(cluster.Status.HAStatus.PlannedActions[1].AdminJobPhase).To(Equal(haAdminJobPhaseWaitingDependency))
	g.Expect(cluster.Status.HAStatus.LastPromotion).To(BeNil())
}

func TestReconcileHAAdminJobsDurablyFencesFormerPrimaryFromPromotionReceipt(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())
	promotion := &antflyv1.HAPromotionStatus{
		ClusterID:         100,
		ShardID:           10,
		TableID:           20,
		OldPrimaryID:      "primary-a",
		PromotedStandbyID: "standby-a",
		ParentTimelineID:  4,
		ParentEpoch:       6,
		NewTimelineID:     5,
		NewEpoch:          7,
		RequiredLSN:       11,
		ObservedLSN:       12,
		SwitchLSN:         13,
		FenceAuthority:    antflyv1.HAFencingAuthorityKubernetesLease,
		FenceGeneration:   3,
		FenceReason:       "LeaseAcquired",
		FenceToken:        "ha-fence-token",
	}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{HighAvailability: &antflyv1.HighAvailabilitySpec{
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
		}},
		Status: antflyv1.AntflyClusterStatus{HAStatus: &antflyv1.HAStatus{
			LastPromotion: promotion,
			PlannedActions: []antflyv1.HAPlannedActionStatus{{
				Kind:            string(haActionFenceFormerPrimary),
				Phase:           string(haActionPhaseFence),
				Executor:        string(haActionExecutorAdminAPI),
				StandbyName:     "primary-a",
				RouteFrom:       "primary-a",
				RouteTo:         "standby-a",
				TargetLSN:       12,
				FenceAuthority:  antflyv1.HAFencingAuthorityKubernetesLease,
				FenceHolder:     "standby-a",
				FenceGeneration: 3,
				FenceReason:     "LeaseAcquired",
				AdminCommand:    []string{"fence", "acquire"},
				AdminURL:        "http://primary-ha.default.svc:8081",
				AdminNodeID:     "primary-a",
				AdminMethod:     http.MethodPost,
				AdminPath:       "/admin/v1/ha/fence",
			}},
		}},
	}
	var request adminsdk.FenceAcquireRequest
	reconciler := &AntflyClusterReconciler{
		Client: newHAControllerTestClient(t, s, cluster),
		Scheme: s,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			g.Expect(req.Method).To(Equal(http.MethodPost))
			g.Expect(req.URL.Path).To(Equal("/admin/v1/ha/fence"))
			g.Expect(json.NewDecoder(req.Body).Decode(&request)).To(Succeed())
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body: io.NopCloser(strings.NewReader(strings.Replace(
					strings.Replace(
						haFenceResponseJSON("primary-a", "standby-a", 3, "ha-fence-token"),
						`"node_id":"standby-a"`, `"node_id":"primary-a"`, 1,
					),
					`"required_lsn":12`, `"required_lsn":11`, 1,
				))),
			}, nil
		})},
	}

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
	g.Expect(request.Identity).To(Equal(adminsdk.HAIdentity{ClusterId: 100, ShardId: 10, TableId: 20, TimelineId: 4, Epoch: 6}))
	g.Expect(string(request.OldPrimaryId)).To(Equal("primary-a"))
	g.Expect(string(request.PromotedNodeId)).To(Equal("standby-a"))
	g.Expect(request.NewTimelineId).To(Equal(uint64(5)))
	g.Expect(request.NewEpoch).To(Equal(uint64(7)))
	g.Expect(request.RequiredLsn).To(Equal(uint64(11)))
	g.Expect(request.ObservedLsn).To(Equal(uint64(12)))
	g.Expect(request.Force).To(BeFalse())
	action := cluster.Status.HAStatus.PlannedActions[0]
	g.Expect(action.AdminJobName).To(Equal(haAdminDirectAPIName))
	if action.AdminJobPhase != haAdminJobPhaseSucceeded {
		t.Fatalf("expected former-primary fence action to succeed, got phase=%q error=%q result=%#v", action.AdminJobPhase, action.AdminError, action.AdminResult)
	}
	g.Expect(action.AdminResult).NotTo(BeNil())
	g.Expect(action.AdminResult.FenceToken).To(Equal(promotion.FenceToken))
	g.Expect(action.AdminResult.ActionNodeID).To(Equal("primary-a"), "fence receipt must identify the node that durably recorded it")
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
		Client: newHAControllerTestClient(t, s, cluster),
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

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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
					TargetLSN:       13,
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
		Client: newHAControllerTestClient(t, s, cluster),
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
				Body:       io.NopCloser(strings.NewReader(`{"schema_version":1,"action":{"action_id":"rejoin_rewind:primary-a","action_kind":"rejoin_rewind","target":"primary-a","state":"applied","node_id":"primary-a"},"assessment":{"action":"rewind","reason":"parent_timeline_retained","former_node_id":"primary-a","target_timeline_id":5,"target_epoch":7,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":4,"parent_epoch":6,"fork_lsn":13,"former_last_lsn":13,"retained_from_lsn":8,"data_loss_discarded":false},"rewind":{"node_id":"primary-a","fork_lsn":13,"previous_last_lsn":13,"current_last_lsn":14,"next_lsn":15,"discarded_lsn_count":0,"target_timeline_id":5,"target_epoch":7,"data_loss_discarded":false}}`)),
			}, nil
		})},
	}

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.ForkLSN).To(Equal(uint64(13)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.FormerLastLSN).To(Equal(uint64(13)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.RetainedFromLSN).To(Equal(uint64(8)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.DataLossDiscarded).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.RewindExecuted).To(BeTrue())
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.RewindPreviousLastLSN).To(Equal(uint64(13)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.RewindCurrentLastLSN).To(Equal(uint64(14)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.RewindNextLSN).To(Equal(uint64(15)))
	g.Expect(cluster.Status.HAStatus.PlannedActions[0].AdminResult.RewindDiscardedLSNCount).To(BeZero())
	g.Expect(cluster.Status.HAStatus.FormerPrimary).NotTo(BeNil())
	g.Expect(cluster.Status.HAStatus.FormerPrimary.NodeID).To(Equal("primary-a"))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.Fenced).To(BeTrue())
	g.Expect(cluster.Status.HAStatus.FormerPrimary.RewindPossible).To(BeTrue())
	g.Expect(cluster.Status.HAStatus.FormerPrimary.TargetTimelineID).To(Equal(uint64(5)))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.TargetEpoch).To(Equal(uint64(7)))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.ForkLSN).To(Equal(uint64(13)))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.FormerLastLSN).To(Equal(uint64(13)))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.RetainedFromLSN).To(Equal(uint64(8)))
	g.Expect(cluster.Status.HAStatus.FormerPrimary.DataLossDiscarded).To(BeFalse())
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
		Client: newHAControllerTestClient(t, s, cluster),
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

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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
		Client: newHAControllerTestClient(t, s, cluster),
		Scheme: s,
	}

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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
			SwitchLSN:         14,
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
		TargetLSN:       13,
		ObservedLSN:     12,
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
		ForkLSN:          13,
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
	result.RewindCurrentLastLSN = 14
	result.RewindNextLSN = 15
	result.RewindDiscardedLSNCount = 0
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
		ForkLSN:          13,
		FormerLastLSN:    15,
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
			FormerLastLSN:           12,
			RetainedFromLSN:         8,
			RewindExecuted:          true,
			RewindPreviousLastLSN:   12,
			RewindCurrentLastLSN:    13,
			RewindNextLSN:           14,
			RewindDiscardedLSNCount: 0,
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
		Client: newHAControllerTestClient(t, s, cluster),
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
	g.Expect(reconciler.persistHAActionPlanBarrier(context.Background(), cluster)).To(Succeed())

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())
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

func TestBuildHAAdminJobRunsPortableArtifactWithoutAdminURLAndInjectsCredentials(t *testing.T) {
	g := NewWithT(t)
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:test",
			HighAvailability: &antflyv1.HighAvailabilitySpec{Standbys: []antflyv1.HAStandbySpec{{
				Name: "standby-a",
				SeedArtifact: &antflyv1.HASeedArtifactSpec{
					Location:             "s3://ha-seeds/cluster-a",
					StagingRoot:          "/target/staging",
					CredentialsSecretRef: &corev1.LocalObjectReference{Name: "ha-seed-credentials"},
					SourcePVC:            &antflyv1.HASeedArtifactPVCSpec{ClaimName: "primary-data", MountPath: "/source"},
					TargetPVC:            &antflyv1.HASeedArtifactPVCSpec{ClaimName: "standby-data", MountPath: "/target"},
				},
			}}},
		},
	}
	action := antflyv1.HAPlannedActionStatus{
		Kind:        string(haActionPublishSeedArtifact),
		Executor:    string(haActionExecutorCLIJob),
		StandbyName: "standby-a",
		SlotName:    "standby-a",
		AdminCommand: []string{
			"artifact", "publish", "--location", "s3://ha-seeds/cluster-a",
		},
	}
	job := buildHAAdminJob(cluster, &antflyv1.HAAdminSpec{}, action)
	container := job.Spec.Template.Spec.Containers[0]
	g.Expect(job.Spec.TTLSecondsAfterFinished).To(BeNil(), "terminal evidence must be checkpointed before TTL cleanup is armed")
	g.Expect(job.Spec.Template.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyNever), "each Job pod must represent one countable process attempt")
	g.Expect(container.Args).To(Equal([]string{
		"ha", "artifact", "publish", "--location", "s3://ha-seeds/cluster-a",
	}))
	g.Expect(container.Args).NotTo(ContainElement("--ha-url"))
	g.Expect(container.EnvFrom).To(HaveLen(1))
	g.Expect(container.EnvFrom[0].SecretRef).NotTo(BeNil())
	g.Expect(container.EnvFrom[0].SecretRef.Name).To(Equal("ha-seed-credentials"))
	g.Expect(container.VolumeMounts).To(Equal([]corev1.VolumeMount{{Name: "ha-seed-source", MountPath: "/source", ReadOnly: true}}))
	g.Expect(job.Spec.Template.Spec.Volumes).To(HaveLen(1))
	g.Expect(job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName).To(Equal("primary-data"))
	g.Expect(haPlannedActionRequiresAdminTarget(action)).To(BeTrue())
	g.Expect(haPlannedActionRequiresAdminURL(action)).To(BeFalse())

	restoreAction := action
	restoreAction.Kind = string(haActionRestoreSeedArtifact)
	restoreJob := buildHAAdminJob(cluster, &antflyv1.HAAdminSpec{}, restoreAction)
	g.Expect(restoreJob.Spec.Template.Spec.Containers[0].VolumeMounts).To(Equal([]corev1.VolumeMount{{Name: "ha-seed-target", MountPath: "/target"}}))
	g.Expect(restoreJob.Spec.Template.Spec.Volumes).To(HaveLen(1))
	g.Expect(restoreJob.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName).To(Equal("standby-data"))

	activateAction := action
	activateAction.Kind = "ActivateSeedArtifact"
	activateJob := buildHAAdminJob(cluster, &antflyv1.HAAdminSpec{}, activateAction)
	g.Expect(activateJob.Spec.Template.Spec.Containers[0].VolumeMounts).To(Equal([]corev1.VolumeMount{{Name: "ha-seed-target", MountPath: "/target"}}))
	g.Expect(activateJob.Spec.Template.Spec.Volumes).To(HaveLen(1))
	g.Expect(activateJob.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName).To(Equal("standby-data"))
}

func TestBuildHAAdminJobFreezesExactSeedIdentityOnEveryPortableWorkload(t *testing.T) {
	g := NewWithT(t)
	digest := strings.Repeat("a", 64)
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:test",
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID: 100, TimelineID: 4, Epoch: 6, CurrentPrimaryID: "primary-a",
				},
				Standbys: []antflyv1.HAStandbySpec{{
					Name: "standby-a",
					SeedArtifact: &antflyv1.HASeedArtifactSpec{
						Location: "s3://ha-seeds/cluster-a", StagingRoot: "/target/staging",
						TopologyID: "topology-a", TopologyGeneration: 7, NodeID: "standby-a", TargetPVCUID: "target-pvc-uid",
						SourcePVC: &antflyv1.HASeedArtifactPVCSpec{ClaimName: "primary-data", MountPath: "/source"},
						TargetPVC: &antflyv1.HASeedArtifactPVCSpec{ClaimName: "standby-data", MountPath: "/target"},
					},
				}},
			},
		},
	}
	base := antflyv1.HAPlannedActionStatus{
		Executor: string(haActionExecutorCLIJob), StandbyName: "standby-a", SlotName: "standby-a", TargetLSN: 10,
		SeedArtifactGeneration: "seed-standby-a-10", TopologyID: "topology-a", TopologyGeneration: 7,
		TopologyNodeID: "standby-a", SourcePVCName: "primary-data", SourcePVCUID: "source-pvc-uid",
		TargetPVCName: "standby-data", TargetPVCUID: "target-pvc-uid",
		AdminCommand: []string{"artifact", "test"},
	}
	capture := base
	capture.Kind = string(haActionCaptureSeedArtifact)
	capture.Executor = string(haActionExecutorAdminAPI)
	capture.AdminNodeID = "primary-a"
	capture.AdminJobName = haAdminDirectAPIName
	capture.AdminJobPhase = haAdminJobPhaseSucceeded
	capture.AdminResult = &antflyv1.HAAdminActionResultStatus{
		SchemaVersion: 1, ActionID: "seed_capture:seed-standby-a-10", ActionKind: "seed_capture",
		ActionTarget: "seed-standby-a-10", ActionState: "applied", ActionNodeID: "primary-a",
		SlotName: "standby-a", ManifestID: "manifest-standby-a-10", BackupLSN: 10, CheckpointLSN: 12, EndRecordLSN: 13,
		SeedArtifactGeneration: "seed-standby-a-10", ManifestSHA256: digest,
		CaptureReceiptSHA256: digest,
		SeedClusterID:        100, SeedTimelineID: 4, SeedEpoch: 6, SeedSourcePlanSHA256: digest, SeedFileCount: 2,
		SeedGenerationRoot: "/antflydb/ha/seed-captures/generations/seed-standby-a-10",
		SeedContentRoot:    "/antflydb/ha/seed-captures/generations/seed-standby-a-10/content",
		SeedManifestPath:   "/antflydb/ha/seed-captures/generations/seed-standby-a-10/manifest.afha",
	}
	// Capture's runtime contract requires manifest ID == immutable generation.
	capture.AdminResult.ManifestID = capture.SeedArtifactGeneration
	g.Expect(haAdminActionSucceededWithEvidence(capture)).To(BeTrue(), "the prerequisite fixture must itself satisfy the typed receipt contract")
	cluster.Status.HAStatus = &antflyv1.HAStatus{PlannedActions: []antflyv1.HAPlannedActionStatus{capture}}

	common := map[string]string{
		"antfly.io/ha-topology-id":          "topology-a",
		"antfly.io/ha-topology-generation":  "7",
		"antfly.io/ha-node-id":              "standby-a",
		"antfly.io/ha-slot-name":            "standby-a",
		"antfly.io/ha-seed-generation":      "seed-standby-a-10",
		"antfly.io/ha-seed-manifest-id":     "seed-standby-a-10",
		"antfly.io/ha-seed-manifest-sha256": digest,
		"antfly.io/ha-seed-checkpoint-lsn":  "12",
	}
	tests := []struct {
		kind       haActionKind
		pvcBinding map[string]string
		role       string
	}{
		{kind: haActionPublishSeedArtifact, pvcBinding: map[string]string{
			"antfly.io/ha-seed-source-pvc-name": "primary-data", "antfly.io/ha-seed-source-pvc-uid": "source-pvc-uid",
		}},
		{kind: haActionGCSourceSeedGenerations, pvcBinding: map[string]string{
			"antfly.io/ha-seed-source-pvc-name": "primary-data", "antfly.io/ha-seed-source-pvc-uid": "source-pvc-uid",
		}},
		{kind: haActionRestoreSeedArtifact, role: "restore", pvcBinding: map[string]string{
			"antfly.io/ha-seed-target-pvc-name": "standby-data", "antfly.io/ha-seed-target-pvc-uid": "target-pvc-uid",
		}},
		{kind: haActionActivateSeedArtifact, pvcBinding: map[string]string{
			"antfly.io/ha-seed-target-pvc-name": "standby-data", "antfly.io/ha-seed-target-pvc-uid": "target-pvc-uid",
		}},
		{kind: haActionGCTargetSeedGenerations, pvcBinding: map[string]string{
			"antfly.io/ha-seed-target-pvc-name": "standby-data", "antfly.io/ha-seed-target-pvc-uid": "target-pvc-uid",
		}},
		{kind: haActionPruneSeedArtifacts, pvcBinding: map[string]string{
			"antfly.io/ha-seed-target-pvc-name": "standby-data", "antfly.io/ha-seed-target-pvc-uid": "target-pvc-uid",
		}},
	}
	for _, tt := range tests {
		t.Run(string(tt.kind), func(t *testing.T) {
			g := NewWithT(t)
			action := base
			action.Kind = string(tt.kind)
			expected := maps.Clone(common)
			maps.Copy(expected, tt.pvcBinding)
			if tt.role != "" {
				expected["antfly.io/ha-seed-role"] = tt.role
			}
			job := buildHAAdminJob(cluster, &antflyv1.HAAdminSpec{}, action)
			g.Expect(testHASeedIdentityAnnotations(job.Annotations)).To(Equal(expected))
			g.Expect(job.Spec.Template.Annotations).To(Equal(expected), "the immutable Job Pod template must carry the same exact identity")
		})
	}

	// Before a capture/publish receipt exists, Kubernetes identity can bind the
	// planned topology and exact PVC incarnation, but it must not fabricate
	// manifest or checkpoint evidence that has not been observed yet.
	preReceiptCluster := cluster.DeepCopy()
	preReceiptCluster.Status.HAStatus = nil
	preReceipt := base
	preReceipt.Kind = string(haActionPublishSeedArtifact)
	preReceiptJob := buildHAAdminJob(preReceiptCluster, &antflyv1.HAAdminSpec{}, preReceipt)
	g.Expect(preReceiptJob.Spec.Template.Annotations).To(Equal(map[string]string{
		"antfly.io/ha-topology-id":          "topology-a",
		"antfly.io/ha-topology-generation":  "7",
		"antfly.io/ha-node-id":              "standby-a",
		"antfly.io/ha-slot-name":            "standby-a",
		"antfly.io/ha-seed-generation":      "seed-standby-a-10",
		"antfly.io/ha-seed-source-pvc-name": "primary-data",
		"antfly.io/ha-seed-source-pvc-uid":  "source-pvc-uid",
	}), "pre-receipt manifest ID/SHA and checkpoint LSN must be omitted, never guessed")
}

func testHASeedIdentityAnnotations(annotations map[string]string) map[string]string {
	keys := []string{
		"antfly.io/ha-topology-id", "antfly.io/ha-topology-generation", "antfly.io/ha-node-id", "antfly.io/ha-slot-name",
		"antfly.io/ha-seed-generation", "antfly.io/ha-seed-manifest-id", "antfly.io/ha-seed-manifest-sha256",
		"antfly.io/ha-seed-source-pvc-name", "antfly.io/ha-seed-source-pvc-uid",
		"antfly.io/ha-seed-target-pvc-name", "antfly.io/ha-seed-target-pvc-uid",
		"antfly.io/ha-seed-checkpoint-lsn", "antfly.io/ha-seed-role",
	}
	result := map[string]string{}
	for _, key := range keys {
		if value, ok := annotations[key]; ok {
			result[key] = value
		}
	}
	return result
}

func TestHASeedCompletionActionsPlanLifecycleGatedLocalGCBeforeRemotePrune(t *testing.T) {
	g := NewWithT(t)
	standby := antflyv1.HAStandbySpec{
		Name: "standby-a",
		SeedArtifact: &antflyv1.HASeedArtifactSpec{
			Location: "s3://ha-seeds/cluster-a", StagingRoot: "/target/staging",
			TopologyID: "topology-a", TopologyGeneration: 7, NodeID: "standby-a", TargetPVCUID: "target-pvc-uid",
			SourcePVC: &antflyv1.HASeedArtifactPVCSpec{ClaimName: "primary-data", MountPath: "/source"},
			TargetPVC: &antflyv1.HASeedArtifactPVCSpec{ClaimName: "standby-data", MountPath: "/target"},
		},
	}

	actions := haSeedCompletionActions(standby, "standby-a", 10, "test", haActionSeedStandby)
	kinds := make([]string, 0, len(actions))
	dependencies := map[string]string{}
	for _, action := range actions {
		kinds = append(kinds, string(action.Kind))
		dependencies[string(action.Kind)] = string(action.DependsOn)
	}
	g.Expect(kinds).To(Equal([]string{
		"CaptureSeedArtifact",
		"PublishSeedArtifact",
		"GCSourceSeedGenerations",
		"RestoreSeedArtifact",
		"ActivateSeedArtifact",
		"ActivateSeededSlot",
		"GCTargetSeedGenerations",
		"PruneSeedArtifacts",
	}))
	g.Expect(dependencies["GCSourceSeedGenerations"]).To(Equal("PublishSeedArtifact"))
	g.Expect(dependencies["RestoreSeedArtifact"]).To(Equal("GCSourceSeedGenerations"))
	g.Expect(dependencies["GCTargetSeedGenerations"]).To(Equal("ActivateSeededSlot"))
	g.Expect(dependencies["PruneSeedArtifacts"]).To(Equal("GCTargetSeedGenerations"), "remote prune must never authorize either local deletion")
}

func TestBuildHAAdminJobScopesLifecycleGCToOneWritablePVCAndDurableReceipt(t *testing.T) {
	g := NewWithT(t)
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:test",
			HighAvailability: &antflyv1.HighAvailabilitySpec{Standbys: []antflyv1.HAStandbySpec{{
				Name: "standby-a",
				SeedArtifact: &antflyv1.HASeedArtifactSpec{
					Location:             "s3://ha-seeds/cluster-a",
					StagingRoot:          "/target/staging",
					CredentialsSecretRef: &corev1.LocalObjectReference{Name: "ha-seed-credentials"},
					SourcePVC:            &antflyv1.HASeedArtifactPVCSpec{ClaimName: "primary-data", MountPath: "/source"},
					TargetPVC:            &antflyv1.HASeedArtifactPVCSpec{ClaimName: "standby-data", MountPath: "/target"},
				},
			}}},
		},
	}
	source := antflyv1.HAPlannedActionStatus{
		Kind: "GCSourceSeedGenerations", Executor: string(haActionExecutorCLIJob),
		StandbyName: "standby-a", SlotName: "standby-a",
		AdminCommand: []string{"artifact", "gc-source", "--capture-root", "/source/seed-captures"},
	}
	sourceJob := buildHAAdminJob(cluster, &antflyv1.HAAdminSpec{}, source)
	sourceContainer := sourceJob.Spec.Template.Spec.Containers[0]
	g.Expect(sourceContainer.Args).To(Equal([]string{"ha", "artifact", "gc-source", "--capture-root", "/source/seed-captures"}))
	g.Expect(sourceContainer.VolumeMounts).To(Equal([]corev1.VolumeMount{{Name: "ha-seed-source", MountPath: "/source"}}))
	g.Expect(sourceJob.Spec.Template.Spec.Volumes).To(HaveLen(1))
	g.Expect(sourceJob.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim).NotTo(BeNil())
	g.Expect(sourceJob.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName).To(Equal("primary-data"))
	g.Expect(sourceJob.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ReadOnly).To(BeFalse())
	g.Expect(sourceContainer.EnvFrom).To(HaveLen(1), "source GC must re-verify the full remote v2 artifact with the configured credentials")

	target := antflyv1.HAPlannedActionStatus{
		Kind: "GCTargetSeedGenerations", Executor: string(haActionExecutorCLIJob),
		StandbyName: "standby-a", SlotName: "standby-a",
		AdminCommand: []string{"artifact", "gc-target", "--target-root", "/target/.antfly-ha/active", "--slot-activation-receipt", "/var/run/antfly-ha/seeded-slot-activation/seeded-slot-activation.json"},
	}
	targetJob := buildHAAdminJob(cluster, &antflyv1.HAAdminSpec{}, target)
	targetContainer := targetJob.Spec.Template.Spec.Containers[0]
	g.Expect(targetContainer.Args).To(Equal(append([]string{"ha"}, target.AdminCommand...)))
	g.Expect(targetContainer.VolumeMounts).To(ContainElements(
		corev1.VolumeMount{Name: "ha-seed-target", MountPath: "/target"},
		corev1.VolumeMount{Name: "ha-seeded-slot-activation", MountPath: "/var/run/antfly-ha/seeded-slot-activation", ReadOnly: true},
	))
	g.Expect(targetJob.Spec.Template.Spec.Volumes).To(HaveLen(2))
	g.Expect(targetJob.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim).NotTo(BeNil())
	g.Expect(targetJob.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName).To(Equal("standby-data"))
	g.Expect(targetJob.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ReadOnly).To(BeFalse())
	g.Expect(targetJob.Spec.Template.Spec.Volumes[1].ConfigMap).NotTo(BeNil())
	g.Expect(targetJob.Spec.Template.Spec.Volumes[1].ConfigMap.Name).NotTo(BeEmpty())
}

func TestReconcileHATargetGCCopiesExactDurableSlotActivationReceiptIdempotently(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	rawReceipt := `{"schema_version":1,"action":{"action_id":"seeded_slot_activate:seed-standby-a-10","action_kind":"seeded_slot_activate","target":"seed-standby-a-10","state":"applied","node_id":"primary-a"},"slot_name":"standby-a","generation":"seed-standby-a-10","manifest_id":"seed-standby-a-10","timeline_id":4,"checkpoint_lsn":10,"seed_receipt_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","capture_receipt_sha256":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","manifest_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","aggregate_sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}`
	encodedResult, err := json.Marshal(map[string]any{
		"schemaVersion": 1, "actionID": "seeded_slot_activate:seed-standby-a-10",
		"actionKind": "seeded_slot_activate", "actionTarget": "seed-standby-a-10",
		"actionState": "applied", "actionNodeID": "primary-a", "slotName": "standby-a",
		"manifestID": "seed-standby-a-10", "checkpointLSN": 10,
		"seedArtifactGeneration": "seed-standby-a-10", "seedTimelineID": 4,
		"seedReceiptSHA256": strings.Repeat("a", 64), "manifestSHA256": strings.Repeat("b", 64),
		"captureReceiptSHA256": strings.Repeat("d", 64),
		"aggregateSHA256":      strings.Repeat("c", 64), "rawReceiptJSON": rawReceipt,
	})
	g.Expect(err).NotTo(HaveOccurred())
	activationResult := &antflyv1.HAAdminActionResultStatus{}
	g.Expect(json.Unmarshal(encodedResult, activationResult)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default", UID: types.UID("cluster-uid")},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:test",
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode:  antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{ExecutePlannedActions: true},
				Runtime: &antflyv1.HARuntimeSpec{StartupGate: &antflyv1.HAStartupGateSpec{
					Policy: antflyv1.HAStartupGatePolicyRequireActivatedSeed, ReceiptMatchPolicy: antflyv1.HAReceiptMatchPolicyExact,
					RequiredReceipt: &antflyv1.HARequiredSeedActivationReceipt{
						TopologyID: "topology-a", TopologyGeneration: 7, NodeID: "standby-a", SlotName: "standby-a",
						Generation: "seed-standby-a-10", TargetPVCName: "standby-data", TargetPVCUID: "standby-pvc-uid",
					},
				}},
				Standbys: []antflyv1.HAStandbySpec{{Name: "standby-a", SeedArtifact: &antflyv1.HASeedArtifactSpec{
					Location: "s3://ha-seeds/cluster-a", StagingRoot: "/target/staging",
					TopologyID: "topology-a", TopologyGeneration: 7, NodeID: "standby-a", TargetPVCUID: "standby-pvc-uid",
					TargetPVC: &antflyv1.HASeedArtifactPVCSpec{ClaimName: "standby-data", MountPath: "/target"},
				}}},
			},
		},
		Status: antflyv1.AntflyClusterStatus{HAStatus: &antflyv1.HAStatus{PlannedActions: []antflyv1.HAPlannedActionStatus{{
			Kind: string(haActionActivateSeededSlot), StandbyName: "standby-a", SlotName: "standby-a",
			SeedArtifactGeneration: "seed-standby-a-10", AdminJobPhase: haAdminJobPhaseSucceeded, AdminResult: activationResult,
		}, {
			Kind: "GCTargetSeedGenerations", Executor: string(haActionExecutorCLIJob), DependsOn: string(haActionActivateSeededSlot),
			StandbyName: "standby-a", SlotName: "standby-a", SeedArtifactGeneration: "seed-standby-a-10",
			TopologyID: "topology-a", TopologyGeneration: 7, TopologyNodeID: "standby-a",
			TargetPVCName: "standby-data", TargetPVCUID: "standby-pvc-uid", OperationID: "haop-test-target-gc",
			AdminCommand: []string{"artifact", "gc-target", "--target-root", "/target/.antfly-ha/active", "--slot-activation-receipt", "/var/run/antfly-ha/seeded-slot-activation/seeded-slot-activation.json"},
		}}}},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "standby-data", Namespace: "default", UID: types.UID("standby-pvc-uid")},
		Spec:       corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}},
	}
	reconciler := &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster, pvc).Build(), Scheme: s}
	action := &cluster.Status.HAStatus.PlannedActions[1]
	g.Expect(reconciler.reconcileHAAdminJob(context.Background(), cluster, cluster.Spec.HighAvailability.Admin, action)).To(Succeed())
	g.Expect(reconciler.reconcileHAAdminJob(context.Background(), cluster, cluster.Spec.HighAvailability.Admin, action)).To(Succeed())

	var receipts corev1.ConfigMapList
	g.Expect(reconciler.List(context.Background(), &receipts)).To(Succeed())
	g.Expect(receipts.Items).To(HaveLen(1))
	g.Expect(receipts.Items[0].Immutable).NotTo(BeNil())
	g.Expect(*receipts.Items[0].Immutable).To(BeTrue())
	g.Expect(receipts.Items[0].Data).To(HaveKeyWithValue("seeded-slot-activation.json", rawReceipt))
	var jobs batchv1.JobList
	g.Expect(reconciler.List(context.Background(), &jobs)).To(Succeed())
	g.Expect(jobs.Items).To(HaveLen(1), "retries must reuse the exact immutable receipt and deterministic Job")
}

func TestHAPortableArtifactJobActionFreezesBothPVCsAndRejectsStaleTopology(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{HighAvailability: &antflyv1.HighAvailabilitySpec{
			Standbys: []antflyv1.HAStandbySpec{{Name: "standby-a", SeedArtifact: &antflyv1.HASeedArtifactSpec{
				Location: "s3://ha-seeds/test", StagingRoot: "/target/staging",
				TopologyID: "topology-a", TopologyGeneration: 7, NodeID: "standby-a", TargetPVCUID: "target-uid-1",
				SourcePVC: &antflyv1.HASeedArtifactPVCSpec{ClaimName: "source-data", MountPath: "/source"},
				TargetPVC: &antflyv1.HASeedArtifactPVCSpec{ClaimName: "target-data", MountPath: "/target"},
			}}},
		}},
	}
	action := antflyv1.HAPlannedActionStatus{
		Kind: string(haActionPublishSeedArtifact), StandbyName: "standby-a", SlotName: "standby-a",
		SeedArtifactGeneration: "seed-standby-a-10", TopologyID: "topology-a", TopologyGeneration: 7,
		TopologyNodeID: "standby-a", TargetPVCName: "target-data", TargetPVCUID: "target-uid-1",
	}
	source := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "source-data", Namespace: "default", UID: types.UID("source-uid-1")}}
	target := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "target-data", Namespace: "default", UID: types.UID("target-uid-1")}}
	reconciler := &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(source, target).Build(), Scheme: s}
	bound, ready, err := reconciler.haPortableArtifactJobAction(context.Background(), cluster, action)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(ready).To(BeTrue())
	g.Expect(bound.SourcePVCName).To(Equal("source-data"))
	g.Expect(bound.SourcePVCUID).To(Equal("source-uid-1"))
	g.Expect(bound.TargetPVCUID).To(Equal("target-uid-1"))

	staleTopology := action
	staleTopology.TopologyGeneration = 6
	_, _, err = reconciler.haPortableArtifactJobAction(context.Background(), cluster, staleTopology)
	g.Expect(err).To(MatchError(ContainSubstring("no longer matches seedArtifact spec")))

	replacedTarget := target.DeepCopy()
	replacedTarget.UID = types.UID("target-uid-2")
	reconciler.Client = fake.NewClientBuilder().WithScheme(s).WithObjects(source, replacedTarget).Build()
	_, _, err = reconciler.haPortableArtifactJobAction(context.Background(), cluster, action)
	g.Expect(err).To(MatchError(ContainSubstring("target PVC UID is stale")))
}

func TestReconcileHAAdminJobsFreezesLiveSourcePVCAuthorityAcrossSeedChain(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := testHASourcePVCAuthorityCluster()
	source := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "primary-data", Namespace: cluster.Namespace, UID: types.UID("source-pvc-uid"),
	}}
	target := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "standby-data", Namespace: cluster.Namespace, UID: types.UID("target-pvc-uid"),
	}}
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster.DeepCopy(), source, target).Build(),
		Scheme: s,
	}

	err := reconciler.reconcileHAAdminJobs(context.Background(), cluster)
	g.Expect(stderrors.Is(err, errHAPlanNeedsPersistence)).To(BeTrue(),
		"live source identity must cross a status persistence barrier before any direct request or Job can start")
	for _, action := range cluster.Status.HAStatus.PlannedActions {
		g.Expect(action.SourcePVCName).To(Equal("primary-data"), action.Kind)
		g.Expect(action.SourcePVCUID).To(Equal("source-pvc-uid"), action.Kind)
	}
}

func TestReconcileHAAdminJobsRejectsReplacedSourcePVCAcrossSeedChain(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())

	cluster := testHASourcePVCAuthorityCluster()
	for i := range cluster.Status.HAStatus.PlannedActions {
		cluster.Status.HAStatus.PlannedActions[i].SourcePVCName = "primary-data"
		cluster.Status.HAStatus.PlannedActions[i].SourcePVCUID = "source-pvc-uid-before-replacement"
	}
	source := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "primary-data", Namespace: cluster.Namespace, UID: types.UID("source-pvc-uid-after-replacement"),
	}}
	target := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "standby-data", Namespace: cluster.Namespace, UID: types.UID("target-pvc-uid"),
	}}
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster.DeepCopy(), source, target).Build(),
		Scheme: s,
	}

	err := reconciler.reconcileHAAdminJobs(context.Background(), cluster)
	g.Expect(err).To(MatchError(ContainSubstring("source PVC identity is stale")))
	for _, action := range cluster.Status.HAStatus.PlannedActions {
		g.Expect(action.SourcePVCUID).To(Equal("source-pvc-uid-before-replacement"),
			"a replacement PVC must never be silently rebound into %s", action.Kind)
	}
}

func testHASourcePVCAuthorityCluster() *antflyv1.AntflyCluster {
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: "default", UID: types.UID("cluster-uid")},
		Spec: antflyv1.AntflyClusterSpec{HighAvailability: &antflyv1.HighAvailabilitySpec{
			Mode:  antflyv1.HAModeHotStandby,
			Admin: &antflyv1.HAAdminSpec{ExecutePlannedActions: true},
			Standbys: []antflyv1.HAStandbySpec{{Name: "standby-a", SlotName: "standby-a", SeedArtifact: &antflyv1.HASeedArtifactSpec{
				Location: "s3://ha-seeds/cluster-a", Generation: "seed-standby-a-10", StagingRoot: "/target/staging",
				TopologyID: "topology-a", TopologyGeneration: 7, NodeID: "standby-a", TargetPVCUID: "target-pvc-uid",
				SourcePVC: &antflyv1.HASeedArtifactPVCSpec{ClaimName: "primary-data", MountPath: "/source"},
				TargetPVC: &antflyv1.HASeedArtifactPVCSpec{ClaimName: "standby-data", MountPath: "/target"},
			}}},
		}},
	}
	chain := []struct {
		kind      haActionKind
		dependsOn haActionKind
	}{
		{haActionCaptureSeedArtifact, haActionReseedFormerPrimary},
		{haActionPublishSeedArtifact, haActionCaptureSeedArtifact},
		{haActionGCSourceSeedGenerations, haActionPublishSeedArtifact},
		{haActionRestoreSeedArtifact, haActionGCSourceSeedGenerations},
		{haActionActivateSeedArtifact, haActionRestoreSeedArtifact},
		{haActionActivateSeededSlot, haActionActivateSeedArtifact},
		{haActionGCTargetSeedGenerations, haActionActivateSeededSlot},
		{haActionPruneSeedArtifacts, haActionGCTargetSeedGenerations},
	}
	cluster.Status.HAStatus = &antflyv1.HAStatus{}
	for _, item := range chain {
		action := antflyv1.HAPlannedActionStatus{
			Kind: string(item.kind), Phase: string(haActionPhaseSeed), Executor: string(haPlannedActionExecutor(item.kind)),
			DependsOn: string(item.dependsOn), StandbyName: "standby-a", SlotName: "standby-a", TargetLSN: 10,
			SeedArtifactLocation: "s3://ha-seeds/cluster-a", SeedArtifactGeneration: "seed-standby-a-10",
			TopologyID: "topology-a", TopologyGeneration: 7, TopologyNodeID: "standby-a",
			TargetPVCName: "standby-data", TargetPVCUID: "target-pvc-uid",
			AdminURL: "http://primary-standalone.default.svc.cluster.local:8080",
		}
		action.OperationID = haPlannedActionOperationID(action)
		cluster.Status.HAStatus.PlannedActions = append(cluster.Status.HAStatus.PlannedActions, action)
	}
	return cluster
}

func TestReconcileHAAdminJobPersistsExactSourcePVCIdentityAndRejectsReplacement(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: "default", UID: types.UID("cluster-uid")},
		Spec: antflyv1.AntflyClusterSpec{Image: "antfly:test", HighAvailability: &antflyv1.HighAvailabilitySpec{
			Standbys: []antflyv1.HAStandbySpec{{Name: "standby-a", SeedArtifact: &antflyv1.HASeedArtifactSpec{
				Location: "s3://ha-seeds/cluster-a", StagingRoot: "/target/staging",
				TopologyID: "topology-a", TopologyGeneration: 7, NodeID: "standby-a", TargetPVCUID: "target-pvc-uid",
				SourcePVC: &antflyv1.HASeedArtifactPVCSpec{ClaimName: "primary-data", MountPath: "/source"},
				TargetPVC: &antflyv1.HASeedArtifactPVCSpec{ClaimName: "standby-data", MountPath: "/target"},
			}}},
		}},
	}
	action := antflyv1.HAPlannedActionStatus{
		Kind: string(haActionPublishSeedArtifact), Executor: string(haActionExecutorCLIJob),
		StandbyName: "standby-a", SlotName: "standby-a", TargetLSN: 10,
		SeedArtifactGeneration: "seed-standby-a-10", TopologyID: "topology-a", TopologyGeneration: 7,
		TopologyNodeID: "standby-a", TargetPVCName: "standby-data", TargetPVCUID: "target-pvc-uid",
		AdminCommand: []string{"artifact", "publish", "--location", "s3://ha-seeds/cluster-a"},
	}
	source := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "primary-data", Namespace: "default", UID: types.UID("source-pvc-uid")},
		Spec:       corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}},
	}
	target := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "standby-data", Namespace: "default", UID: types.UID("target-pvc-uid")},
		Spec:       corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}},
	}
	reconciler := &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(source, target).Build(), Scheme: s}
	g.Expect(reconciler.reconcileHAAdminJob(context.Background(), cluster, &antflyv1.HAAdminSpec{}, &action)).To(Succeed())
	g.Expect(action.SourcePVCName).To(Equal("primary-data"))
	g.Expect(action.SourcePVCUID).To(Equal("source-pvc-uid"))
	g.Expect(action.AdminJobName).To(Equal(haAdminJobName(cluster, action)), "the durable action and deterministic Job hash must freeze the same PVC incarnation")
	created := &batchv1.Job{}
	g.Expect(reconciler.Get(context.Background(), types.NamespacedName{Name: action.AdminJobName, Namespace: cluster.Namespace}, created)).To(Succeed())
	g.Expect(created.Annotations).To(HaveKeyWithValue("antfly.io/ha-seed-source-pvc-uid", "source-pvc-uid"))

	for name, stale := range map[string]antflyv1.HAPlannedActionStatus{
		"name": func() antflyv1.HAPlannedActionStatus {
			copy := action
			copy.SourcePVCName = "replaced-primary-data"
			return copy
		}(),
		"uid": func() antflyv1.HAPlannedActionStatus {
			copy := action
			copy.SourcePVCUID = "replaced-source-pvc-uid"
			return copy
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			g := NewWithT(t)
			_, _, err := reconciler.haPortableArtifactJobAction(context.Background(), cluster, stale)
			g.Expect(err).To(MatchError(ContainSubstring("source PVC identity is stale")))
		})
	}
}

func TestReconcileHAAdminJobRejectsMutablePortableSeedIdentity(t *testing.T) {
	digest := strings.Repeat("a", 64)
	baseCluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: "default", UID: types.UID("cluster-uid")},
		Spec: antflyv1.AntflyClusterSpec{Image: "antfly:test", HighAvailability: &antflyv1.HighAvailabilitySpec{
			Identity: &antflyv1.HAReplicationIdentitySpec{ClusterID: 100, TimelineID: 4, Epoch: 6, CurrentPrimaryID: "primary-a"},
			Standbys: []antflyv1.HAStandbySpec{{Name: "standby-a", SeedArtifact: &antflyv1.HASeedArtifactSpec{
				Location: "s3://ha-seeds/cluster-a", StagingRoot: "/target/staging",
				TopologyID: "topology-a", TopologyGeneration: 7, NodeID: "standby-a", TargetPVCUID: "target-pvc-uid",
				TargetPVC: &antflyv1.HASeedArtifactPVCSpec{ClaimName: "standby-data", MountPath: "/target"},
			}}},
		}},
	}
	baseAction := antflyv1.HAPlannedActionStatus{
		Kind: string(haActionRestoreSeedArtifact), Executor: string(haActionExecutorCLIJob),
		StandbyName: "standby-a", SlotName: "standby-a", TargetLSN: 10,
		SeedArtifactGeneration: "seed-standby-a-10", TopologyID: "topology-a", TopologyGeneration: 7,
		TopologyNodeID: "standby-a", TargetPVCName: "standby-data", TargetPVCUID: "target-pvc-uid",
		AdminCommand: []string{"artifact", "restore", "--location", "s3://ha-seeds/cluster-a"},
	}
	publish := baseAction
	publish.Kind = string(haActionPublishSeedArtifact)
	publish.AdminJobPhase = haAdminJobPhaseSucceeded
	publish.SeedArtifactReceipt = &antflyv1.HASeedArtifactReceiptStatus{
		FormatVersion: 3, Generation: baseAction.SeedArtifactGeneration, SlotName: baseAction.SlotName,
		ClusterID: 100, TimelineID: 4, Epoch: 6, ManifestID: "manifest-standby-a-10",
		BackupLSN: 10, CheckpointLSN: 12, ManifestSHA256: digest, AggregateSHA256: strings.Repeat("b", 64),
		TotalBytes: 42, FileCount: 1, TopologyID: baseAction.TopologyID, TopologyGeneration: baseAction.TopologyGeneration,
		NodeID: baseAction.TopologyNodeID, TargetPVCName: baseAction.TargetPVCName, TargetPVCUID: baseAction.TargetPVCUID,
	}
	baseCluster.Status.HAStatus = &antflyv1.HAStatus{PlannedActions: []antflyv1.HAPlannedActionStatus{publish, baseAction}}

	mutations := map[string]func(*batchv1.Job){
		"job annotation": func(job *batchv1.Job) {
			job.Annotations["antfly.io/ha-seed-target-pvc-uid"] = "stale-target-pvc-uid"
		},
		"pod template annotation": func(job *batchv1.Job) {
			if job.Spec.Template.Annotations == nil {
				job.Spec.Template.Annotations = map[string]string{}
			}
			job.Spec.Template.Annotations["antfly.io/ha-seed-manifest-sha256"] = strings.Repeat("c", 64)
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			g := NewWithT(t)
			s := runtime.NewScheme()
			g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
			g.Expect(batchv1.AddToScheme(s)).To(Succeed())
			g.Expect(corev1.AddToScheme(s)).To(Succeed())
			cluster := baseCluster.DeepCopy()
			action := baseAction
			existing := buildHAAdminJob(cluster, &antflyv1.HAAdminSpec{}, action)
			existing.UID = types.UID("existing-job-uid")
			g.Expect(controllerutil.SetControllerReference(cluster, existing, s)).To(Succeed())
			mutate(existing)
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "standby-data", Namespace: "default", UID: types.UID("target-pvc-uid")},
				Spec:       corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}},
			}
			reconciler := &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(existing, pvc).Build(), Scheme: s}
			err := reconciler.reconcileHAAdminJob(context.Background(), cluster, &antflyv1.HAAdminSpec{}, &action)
			g.Expect(err).To(MatchError(ContainSubstring("immutable seed identity annotations")))
		})
	}
}

func TestPortableArtifactJobFollowsLiveRWOConsumerPod(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "primary", Namespace: "default",
			Labels: map[string]string{
				"cloud.antfly.io/instance-id": "cloud-instance-a",
				"cloud.antfly.io/org-id":      "org-a",
			},
		},
		Spec: antflyv1.AntflyClusterSpec{Image: "antfly:test", HighAvailability: &antflyv1.HighAvailabilitySpec{
			Standbys: []antflyv1.HAStandbySpec{{Name: "standby-a", SeedArtifact: &antflyv1.HASeedArtifactSpec{
				Location: "s3://ha-seeds/cluster-a", StagingRoot: "/target/staging",
				SourcePVC: &antflyv1.HASeedArtifactPVCSpec{ClaimName: "primary-data", MountPath: "/source"},
			}}},
		}},
	}
	action := antflyv1.HAPlannedActionStatus{
		Kind: string(haActionPublishSeedArtifact), StandbyName: "standby-a", SlotName: "standby-a",
		AdminCommand: []string{"artifact", "publish", "--location", "s3://ha-seeds/cluster-a"},
	}
	job := buildHAAdminJob(cluster, &antflyv1.HAAdminSpec{}, action)
	g.Expect(job.Spec.Template.Spec.Affinity).To(BeNil(), "placement depends on live PVC consumers, not claim-name guessing")
	g.Expect(job.Labels).To(HaveKeyWithValue("cloud.antfly.io/instance-id", "cloud-instance-a"))
	g.Expect(job.Labels).To(HaveKeyWithValue("app.kubernetes.io/instance", "primary"))
	g.Expect(job.Spec.Template.Labels).NotTo(HaveKey("cloud.antfly.io/instance-id"),
		"source-PVC Jobs must not match the runtime's required cloud-instance self anti-affinity")
	g.Expect(job.Spec.Template.Labels).NotTo(HaveKey("app.kubernetes.io/instance"),
		"source-PVC Jobs must not inherit runtime identity selectors")
	g.Expect(job.Spec.Template.Labels).To(HaveKeyWithValue("antfly.io/ha-action-kind", "publishseedartifact"))
	g.Expect(job.Spec.Template.Labels).To(HaveKeyWithValue("app.kubernetes.io/component", "ha-admin"))
	gcAction := action
	gcAction.Kind = string(haActionGCSourceSeedGenerations)
	gcJob := buildHAAdminJob(cluster, &antflyv1.HAAdminSpec{}, gcAction)
	g.Expect(gcJob.Labels).To(HaveKeyWithValue("cloud.antfly.io/instance-id", "cloud-instance-a"))
	g.Expect(gcJob.Spec.Template.Labels).NotTo(HaveKey("cloud.antfly.io/instance-id"))
	g.Expect(gcJob.Spec.Template.Labels).NotTo(HaveKey("app.kubernetes.io/instance"))
	g.Expect(gcJob.Spec.Template.Labels).To(HaveKeyWithValue("antfly.io/ha-action-kind", "gcsourceseedgenerations"))

	consumer := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "primary-standalone-0", Namespace: "default",
			Labels: map[string]string{appsv1.StatefulSetPodNameLabel: "primary-standalone-0"},
		},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
			Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "primary-data"}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "primary-data", Namespace: "default"},
		Spec:       corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}},
	}
	reconciler := &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(consumer, pvc).Build()}
	g.Expect(reconciler.bindHAAdminJobToPVCConsumer(context.Background(), job)).To(Succeed())
	g.Expect(job.Annotations).To(HaveKeyWithValue("antfly.io/ha-pvc-consumer", "primary-standalone-0"))
	g.Expect(job.Annotations).To(HaveKeyWithValue("antfly.io/ha-pvc-claim", "primary-data"))
	required := job.Spec.Template.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	g.Expect(required).To(HaveLen(1))
	g.Expect(required[0].TopologyKey).To(Equal(corev1.LabelHostname))
	g.Expect(required[0].LabelSelector.MatchLabels).To(Equal(map[string]string{
		appsv1.StatefulSetPodNameLabel: "primary-standalone-0",
	}))
}

func TestPortableArtifactJobFailsClosedForAmbiguousRWOConsumers(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(corev1.AddToScheme(s)).To(Succeed())
	consumer := func(name string, labels map[string]string, phase corev1.PodPhase) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: labels},
			Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
				Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "primary-data"}},
			}}},
			Status: corev1.PodStatus{Phase: phase},
		}
	}
	job := func() *batchv1.Job {
		return &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "publish", Namespace: "default"},
			Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
				Name: "source", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "primary-data"}},
			}}}}},
		}
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "primary-data", Namespace: "default"},
		Spec:       corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}},
	}

	unlabelled := consumer("primary-standalone-0", nil, corev1.PodRunning)
	reconciler := &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(unlabelled, pvc.DeepCopy()).Build()}
	err := reconciler.bindHAAdminJobToPVCConsumer(context.Background(), job())
	g.Expect(err).To(MatchError(ContainSubstring("lacks its stable StatefulSet pod-name label")))

	first := consumer("primary-standalone-0", map[string]string{appsv1.StatefulSetPodNameLabel: "primary-standalone-0"}, corev1.PodRunning)
	second := consumer("primary-standalone-1", map[string]string{appsv1.StatefulSetPodNameLabel: "primary-standalone-1"}, corev1.PodPending)
	reconciler = &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(first, second, pvc.DeepCopy()).Build()}
	err = reconciler.bindHAAdminJobToPVCConsumer(context.Background(), job())
	g.Expect(err).To(MatchError(ContainSubstring("multiple live consumer pods")))

	terminated := consumer("old-primary-standalone-0", nil, corev1.PodFailed)
	reconciler = &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(terminated, pvc.DeepCopy()).Build()}
	ignored := job()
	g.Expect(reconciler.bindHAAdminJobToPVCConsumer(context.Background(), ignored)).To(Succeed())
	g.Expect(ignored.Spec.Template.Spec.Affinity).To(BeNil(), "terminated consumers must not pin a replacement Job")
}

func TestPortableArtifactJobSkipsPlacementRecalculationForExistingDeterministicJob(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: "default", UID: types.UID("cluster-uid")},
		Spec: antflyv1.AntflyClusterSpec{Image: "antfly:test", HighAvailability: &antflyv1.HighAvailabilitySpec{
			Standbys: []antflyv1.HAStandbySpec{{Name: "standby-a", SeedArtifact: &antflyv1.HASeedArtifactSpec{
				Location: "s3://ha-seeds/cluster-a", StagingRoot: "/target/staging",
				TopologyID: "topology-a", TopologyGeneration: 7, NodeID: "standby-a", TargetPVCUID: "target-pvc-uid",
				SourcePVC: &antflyv1.HASeedArtifactPVCSpec{ClaimName: "primary-data", MountPath: "/source"},
				TargetPVC: &antflyv1.HASeedArtifactPVCSpec{ClaimName: "standby-data", MountPath: "/target"},
			}}},
		}},
	}
	action := antflyv1.HAPlannedActionStatus{
		Kind: string(haActionPublishSeedArtifact), Executor: string(haActionExecutorCLIJob),
		StandbyName: "standby-a", SlotName: "standby-a",
		TopologyID: "topology-a", TopologyGeneration: 7, TopologyNodeID: "standby-a",
		TargetPVCName: "standby-data", TargetPVCUID: "target-pvc-uid",
		SourcePVCName: "primary-data", SourcePVCUID: "source-pvc-uid",
		AdminCommand: []string{"artifact", "publish", "--location", "s3://ha-seeds/cluster-a"},
	}
	existing := buildHAAdminJob(cluster, &antflyv1.HAAdminSpec{}, action)
	existing.UID = types.UID("existing-job-uid")
	g.Expect(controllerutil.SetControllerReference(cluster, existing, s)).To(Succeed())
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "primary-data", Namespace: "default", UID: types.UID("source-pvc-uid")},
		Spec:       corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}},
	}
	// This pod appeared after the Job was created. Recomputing placement would
	// reject an already immutable Job even though there is nothing left to bind.
	lateConsumer := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "unlabelled-late-consumer", Namespace: "default"},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
			Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "primary-data"}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(existing, pvc, &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "standby-data", Namespace: "default", UID: types.UID("target-pvc-uid")},
		}, lateConsumer).Build(),
		Scheme: s,
	}

	g.Expect(reconciler.reconcileHAAdminJob(context.Background(), cluster, &antflyv1.HAAdminSpec{}, &action)).To(Succeed())
	g.Expect(action.AdminJobName).To(Equal(existing.Name))
}

func TestPortableArtifactJobHonorsPVCExclusivityAndConsumerLifecycle(t *testing.T) {
	newJob := func(kind haActionKind) *batchv1.Job {
		return &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name: "portable-job", Namespace: "default", UID: types.UID("current-job-uid"),
				Annotations: map[string]string{"antfly.io/ha-action-kind": string(kind)},
			},
			Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
				Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "artifact-data"}},
			}}}}},
		}
	}
	newPVC := func(mode corev1.PersistentVolumeAccessMode) *corev1.PersistentVolumeClaim {
		return &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "artifact-data", Namespace: "default"},
			Spec:       corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{mode}},
		}
	}
	newConsumer := func(name string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: "default",
				Labels: map[string]string{appsv1.StatefulSetPodNameLabel: name},
			},
			Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
				Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "artifact-data"}},
			}}},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
	}

	t.Run("RWX does not require consumer-derived placement", func(t *testing.T) {
		g := NewWithT(t)
		s := runtime.NewScheme()
		g.Expect(corev1.AddToScheme(s)).To(Succeed())
		unlabelled := newConsumer("unlabelled")
		unlabelled.Labels = nil
		job := newJob(haActionPublishSeedArtifact)
		r := &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(
			newPVC(corev1.ReadWriteMany), unlabelled,
		).Build()}
		g.Expect(r.bindHAAdminJobToPVCConsumer(context.Background(), job)).To(Succeed())
		g.Expect(job.Spec.Template.Spec.Affinity).To(BeNil())
	})

	t.Run("RWOP refuses every live non-owner consumer", func(t *testing.T) {
		g := NewWithT(t)
		s := runtime.NewScheme()
		g.Expect(corev1.AddToScheme(s)).To(Succeed())
		r := &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(
			newPVC(corev1.ReadWriteOncePod), newConsumer("runtime-0"),
		).Build()}
		err := r.bindHAAdminJobToPVCConsumer(context.Background(), newJob(haActionPublishSeedArtifact))
		g.Expect(err).To(MatchError(ContainSubstring("ReadWriteOncePod")))
	})

	t.Run("terminating RWO consumers remain live for placement", func(t *testing.T) {
		g := NewWithT(t)
		s := runtime.NewScheme()
		g.Expect(corev1.AddToScheme(s)).To(Succeed())
		consumer := newConsumer("runtime-0")
		deleting := metav1.NewTime(time.Unix(1700000000, 0))
		consumer.DeletionTimestamp = &deleting
		consumer.Finalizers = []string{"test.antfly.io/hold"}
		job := newJob(haActionPublishSeedArtifact)
		r := &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(
			newPVC(corev1.ReadWriteOnce), consumer,
		).Build()}
		g.Expect(r.bindHAAdminJobToPVCConsumer(context.Background(), job)).To(Succeed())
		g.Expect(job.Spec.Template.Spec.Affinity).NotTo(BeNil())
		g.Expect(job.Spec.Template.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution).To(HaveLen(1))
	})

	t.Run("only the exact current Job owner UID is excluded", func(t *testing.T) {
		g := NewWithT(t)
		s := runtime.NewScheme()
		g.Expect(corev1.AddToScheme(s)).To(Succeed())
		controller := true
		ownPod := newConsumer("portable-job-pod")
		ownPod.Labels = nil
		ownPod.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "batch/v1", Kind: "Job", Name: "portable-job",
			UID: types.UID("current-job-uid"), Controller: &controller,
		}}
		pvc := newPVC(corev1.ReadWriteOnce)
		pvc.Spec.VolumeName = "artifact-pv"
		pvc.Status.Phase = corev1.ClaimBound
		job := newJob(haActionPublishSeedArtifact)
		r := &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(pvc, ownPod).Build()}
		g.Expect(r.bindHAAdminJobToPVCConsumer(context.Background(), job)).To(Succeed())
		g.Expect(job.Spec.Template.Spec.Affinity).To(BeNil())
	})

	t.Run("target restore refuses a live consumer", func(t *testing.T) {
		g := NewWithT(t)
		s := runtime.NewScheme()
		g.Expect(corev1.AddToScheme(s)).To(Succeed())
		r := &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(
			newPVC(corev1.ReadWriteOnce), newConsumer("standby-0"),
		).Build()}
		err := r.bindHAAdminJobToPVCConsumer(context.Background(), newJob(haActionRestoreSeedArtifact))
		g.Expect(err).To(MatchError(ContainSubstring("refusing HA restore/activation")))
	})

	t.Run("RWX target restore still refuses a live consumer", func(t *testing.T) {
		g := NewWithT(t)
		s := runtime.NewScheme()
		g.Expect(corev1.AddToScheme(s)).To(Succeed())
		r := &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(
			newPVC(corev1.ReadWriteMany), newConsumer("standby-0"),
		).Build()}
		err := r.bindHAAdminJobToPVCConsumer(context.Background(), newJob(haActionRestoreSeedArtifact))
		g.Expect(err).To(MatchError(ContainSubstring("refusing HA restore/activation")))
	})

	t.Run("unmounted publish source must be bound to a stable PV", func(t *testing.T) {
		g := NewWithT(t)
		s := runtime.NewScheme()
		g.Expect(corev1.AddToScheme(s)).To(Succeed())
		r := &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(
			newPVC(corev1.ReadWriteOnce),
		).Build()}
		err := r.bindHAAdminJobToPVCConsumer(context.Background(), newJob(haActionPublishSeedArtifact))
		g.Expect(err).To(MatchError(ContainSubstring("is not bound to a stable PV")))
	})
}

func TestHAAdminJobCountsOnlyStartedPodsOwnedByExactJobUID(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: "default", UID: types.UID("cluster-uid")},
		Spec:       antflyv1.AntflyClusterSpec{Image: "antfly:test"},
	}
	action := antflyv1.HAPlannedActionStatus{
		Kind: "LocalMaintenance", Executor: string(haActionExecutorCLIJob),
		AdminURL: "http://primary-ha.default.svc:8081", AdminCommand: []string{"maintenance"},
	}
	job := buildHAAdminJob(cluster, &antflyv1.HAAdminSpec{}, action)
	job.UID = types.UID("current-job-uid")
	job.Status.Active = 1
	g.Expect(controllerutil.SetControllerReference(cluster, job, s)).To(Succeed())
	controller := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pending-job-pod", Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "batch/v1", Kind: "Job", Name: job.Name,
				UID: job.UID, Controller: &controller,
			}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "ha-admin", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}},
			}},
		},
	}
	r := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(job, pod).Build(),
		Scheme: s,
	}

	g.Expect(r.reconcileHAAdminJob(context.Background(), cluster, &antflyv1.HAAdminSpec{}, &action)).To(Succeed())
	g.Expect(action.AdminJobPhase).To(Equal(haAdminJobPhaseRunning))
	g.Expect(action.AttemptCount).To(BeZero(), "an unscheduled/unstarted pod is not an external process attempt")
}

func TestObserveHAAdminJobAttemptsRejectsStaleNameLabelsAndTracksExactPodTimes(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "ha-admin", Namespace: "default", UID: types.UID("current-job-uid"),
	}}
	controller := true
	startedAt := time.Date(2026, 7, 14, 18, 0, 0, 0, time.UTC)
	finishedAt := startedAt.Add(2 * time.Minute)
	ownedPod := func(name string, uid types.UID, state corev1.ContainerState) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: "default",
				Labels: map[string]string{"job-name": job.Name, "batch.kubernetes.io/job-name": job.Name},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "batch/v1", Kind: "Job", Name: job.Name,
					UID: uid, Controller: &controller,
				}},
			},
			Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "ha-admin", State: state}}},
		}
	}
	first := ownedPod("ha-admin-first", job.UID, corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(startedAt)},
	})
	second := ownedPod("ha-admin-second", job.UID, corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{StartedAt: metav1.NewTime(finishedAt), FinishedAt: metav1.NewTime(finishedAt.Add(time.Second))},
	})
	stale := ownedPod("ha-admin-stale", types.UID("deleted-job-uid"), corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(startedAt.Add(-time.Hour))},
	})
	r := &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(first, second, stale).Build()}

	count, firstObserved, lastObserved, err := r.observeHAAdminJobPodAttempts(context.Background(), job)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(count).To(Equal(int32(2)))
	g.Expect(firstObserved).NotTo(BeNil())
	g.Expect(lastObserved).NotTo(BeNil())
	g.Expect(firstObserved.Time.Equal(startedAt)).To(BeTrue())
	g.Expect(lastObserved.Time.Equal(finishedAt)).To(BeTrue())
}

func TestHAAdminJobArmsTTLOnlyAfterTerminalStatusWasCheckpointed(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(batchv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())
	ttl := int32(0)
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{Image: "antfly:test", HighAvailability: &antflyv1.HighAvailabilitySpec{
			Mode: antflyv1.HAModeHotStandby,
			Admin: &antflyv1.HAAdminSpec{
				ExecutePlannedActions:      true,
				JobTTLSecondsAfterFinished: &ttl,
			},
		}},
		Status: antflyv1.AntflyClusterStatus{HAStatus: &antflyv1.HAStatus{PlannedActions: []antflyv1.HAPlannedActionStatus{{
			Kind: "LocalMaintenance", Executor: string(haActionExecutorCLIJob),
			AdminURL: "http://primary-ha.default.svc:8081", AdminCommand: []string{"maintenance"},
			AdminJobName: "terminal-ha-job", AdminJobPhase: haAdminJobPhaseFailed,
		}}}},
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "terminal-ha-job", Namespace: "default"}}
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster, job).Build()
	r := &AntflyClusterReconciler{Client: client, Scheme: s}

	g.Expect(r.reconcileHAAdminJobs(context.Background(), cluster)).To(Succeed())
	observed := &batchv1.Job{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: job.Name, Namespace: job.Namespace}, observed)).To(Succeed())
	g.Expect(observed.Spec.TTLSecondsAfterFinished).NotTo(BeNil())
	g.Expect(*observed.Spec.TTLSecondsAfterFinished).To(Equal(int32(0)))
}

func TestActivationJobBindsReceiptToObservedTargetPVCInstance(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())
	cluster := startupGatedStandaloneControllerCluster(false)
	cluster.Spec.HighAvailability.Runtime.StartupGate.RequiredReceipt.TargetPVCUID = "pvc-uid-1"
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "standby-a-data", Namespace: "default", UID: types.UID("pvc-uid-1"),
	}}
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster, pvc).Build(),
		Scheme: s,
	}
	action := antflyv1.HAPlannedActionStatus{
		Kind: string(haActionActivateSeedArtifact), SlotName: "standby-a",
		SeedArtifactGeneration: "prod-standby-a-10",
		TopologyID:             "test-standalone", TopologyGeneration: 3, TopologyNodeID: "standby-a",
		TargetPVCName: "standby-a-data", TargetPVCUID: "pvc-uid-1", TargetLocalNodeID: 1, TargetReplicaID: 1,
		AdminCommand: []string{
			"artifact", "activate", "--generation", "prod-standby-a-10",
			"--topology-id", "test-standalone", "--topology-generation", "3",
			"--node-id", "standby-a", "--target-pvc-name", "standby-a-data", "--target-pvc-uid", "pvc-uid-1",
			"--target-local-node-id", "1", "--target-replica-id", "1",
		},
	}

	bound, ready, err := reconciler.haActivationJobAction(context.Background(), cluster, action)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(ready).To(BeTrue())
	g.Expect(bound.AdminCommand).To(Equal(action.AdminCommand), "PVC preflight must validate, never mutate, the frozen activation request")
	g.Expect(haAdminActionHash(bound)).To(Equal(haAdminActionHash(action)))

	cluster.Spec.HighAvailability.Runtime.StartupGate.RequiredReceipt.TargetPVCUID = "replacement-pvc"
	_, ready, err = reconciler.haActivationJobAction(context.Background(), cluster, action)
	g.Expect(err).To(MatchError(ContainSubstring("stale relative to the startup gate")))
	g.Expect(ready).To(BeFalse())
}

func TestActivationReceiptCurrentTargetRejectsReplacementPVCAndTopologyGeneration(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())
	cluster := startupGatedStandaloneControllerCluster(true)
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "standby-a-data", Namespace: "default", UID: types.UID("pvc-uid-2"),
	}}
	reconciler := &AntflyClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(cluster, pvc).Build(),
		Scheme: s,
	}
	digest := strings.Repeat("a", 64)
	action := antflyv1.HAPlannedActionStatus{
		Kind: string(haActionActivateSeedArtifact), SlotName: "standby-a",
		SeedArtifactGeneration: "prod-standby-a-10", AdminJobPhase: haAdminJobPhaseSucceeded,
		SeedCaptureReceiptSHA256: strings.Repeat("d", 64), TargetLocalNodeID: 1, TargetReplicaID: 1,
		SeedArtifactReceipt: &antflyv1.HASeedArtifactReceiptStatus{
			FormatVersion: 2,
			TopologyID:    "test-standalone", TopologyGeneration: 3, NodeID: "standby-a", SlotName: "standby-a",
			Generation: "prod-standby-a-10", TargetPVCName: "standby-a-data", TargetPVCUID: "pvc-uid-2",
			ManifestSHA256: digest, CaptureReceiptSHA256: strings.Repeat("d", 64),
			MaterializedReceiptSHA256: strings.Repeat("e", 64), MaterializedAggregateSHA256: strings.Repeat("f", 64),
			TargetLocalNodeID: 1, TargetReplicaID: 1,
			GenerationPath: "live-generations/prod-standby-a-10", RawGenerationPath: "generations/prod-standby-a-10",
		},
	}

	current, err := reconciler.haActivationReceiptMatchesCurrentTarget(context.Background(), cluster, action)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(current).To(BeTrue())

	action.SeedArtifactReceipt.TargetPVCUID = "pvc-uid-1"
	current, err = reconciler.haActivationReceiptMatchesCurrentTarget(context.Background(), cluster, action)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(current).To(BeFalse())
	action.SeedArtifactReceipt.TargetPVCUID = "pvc-uid-2"
	action.SeedArtifactReceipt.TopologyGeneration = 2
	current, err = reconciler.haActivationReceiptMatchesCurrentTarget(context.Background(), cluster, action)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(current).To(BeFalse())
}

func TestExecuteActivateSeededSlotUsesExactTargetActivationReceipt(t *testing.T) {
	g := NewWithT(t)
	digest := strings.Repeat("a", 64)
	dependency := antflyv1.HAPlannedActionStatus{
		Kind:                     string(haActionActivateSeedArtifact),
		Executor:                 string(haActionExecutorCLIJob),
		SlotName:                 "standby-a",
		TargetLSN:                10,
		SeedArtifactGeneration:   "seed-standby-a-10",
		SeedCaptureReceiptSHA256: digest,
		TargetLocalNodeID:        1,
		TargetReplicaID:          1,
		AdminJobName:             "activation-job",
		AdminJobPhase:            haAdminJobPhaseSucceeded,
		SeedArtifactReceipt: &antflyv1.HASeedArtifactReceiptStatus{
			FormatVersion:               2,
			Generation:                  "seed-standby-a-10",
			SlotName:                    "standby-a",
			ClusterID:                   100,
			TimelineID:                  4,
			Epoch:                       6,
			ManifestID:                  "base-standby-a-10",
			BackupLSN:                   10,
			CheckpointLSN:               12,
			SeedReceiptSHA256:           digest,
			CaptureReceiptSHA256:        digest,
			ManifestSHA256:              digest,
			AggregateSHA256:             digest,
			MaterializedReceiptSHA256:   digest,
			MaterializedAggregateSHA256: digest,
			TargetLocalNodeID:           1,
			TargetReplicaID:             1,
			GenerationPath:              "live-generations/seed-standby-a-10",
			RawGenerationPath:           "generations/seed-standby-a-10",
		},
	}
	action := antflyv1.HAPlannedActionStatus{
		Kind:                   string(haActionActivateSeededSlot),
		Executor:               string(haActionExecutorAdminAPI),
		DependsOn:              string(haActionActivateSeedArtifact),
		SlotName:               "standby-a",
		TargetLSN:              10,
		SeedArtifactGeneration: "seed-standby-a-10",
		AdminURL:               "http://primary-ha.default.svc:8081",
		AdminNodeID:            "primary-a",
		AdminMethod:            http.MethodPost,
		AdminPath:              haAdminBaseBackupsActivatePath,
	}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{HighAvailability: &antflyv1.HighAvailabilitySpec{
			Identity: &antflyv1.HAReplicationIdentitySpec{ClusterID: 100, TimelineID: 4, Epoch: 6, CurrentPrimaryID: "primary-a"},
		}},
		Status: antflyv1.AntflyClusterStatus{HAStatus: &antflyv1.HAStatus{PlannedActions: []antflyv1.HAPlannedActionStatus{dependency, action}}},
	}
	requests := 0
	reconciler := &AntflyClusterReconciler{HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests++
		g.Expect(req.Method).To(Equal(http.MethodPost))
		g.Expect(req.URL.Path).To(Equal(haAdminBaseBackupsActivatePath))
		var body adminsdk.SeededSlotActivateRequest
		g.Expect(json.NewDecoder(req.Body).Decode(&body)).To(Succeed())
		g.Expect(body.SlotName).To(Equal("standby-a"))
		g.Expect(body.Generation).To(Equal("seed-standby-a-10"))
		g.Expect(body.ManifestId).To(Equal("base-standby-a-10"))
		g.Expect(body.TimelineId).To(Equal(uint64(4)))
		g.Expect(body.CheckpointLsn).To(Equal(uint64(12)))
		g.Expect(body.SeedReceiptSha256).To(Equal(digest))
		g.Expect(body.CaptureReceiptSha256).To(Equal(digest))
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(strings.NewReader(fmt.Sprintf(
				`{"schema_version":1,"action":{"action_id":"seeded_slot_activate:seed-standby-a-10","action_kind":"seeded_slot_activate","target":"seed-standby-a-10","state":"applied","node_id":"primary-a"},"slot_name":"standby-a","generation":"seed-standby-a-10","manifest_id":"base-standby-a-10","timeline_id":4,"checkpoint_lsn":12,"seed_receipt_sha256":"%s","capture_receipt_sha256":"%s","manifest_sha256":"%s","aggregate_sha256":"%s"}`,
				digest, digest, digest, digest,
			))),
		}, nil
	})}}

	handled, err := reconciler.executeHAPlannedActionTyped(context.Background(), cluster, &action)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(handled).To(BeTrue())
	g.Expect(requests).To(Equal(1))
	g.Expect(action.AdminResult).NotTo(BeNil())
	g.Expect(action.AdminResult.CheckpointLSN).To(Equal(uint64(12)))
	g.Expect(action.AdminResult.SeedArtifactGeneration).To(Equal("seed-standby-a-10"))

	cluster.Status.HAStatus.PlannedActions[0].SeedArtifactReceipt.TimelineID = 5
	requests = 0
	action.AdminResult = nil
	handled, err = reconciler.executeHAPlannedActionTyped(context.Background(), cluster, &action)
	g.Expect(handled).To(BeTrue())
	g.Expect(err).To(MatchError(ContainSubstring("requires matching durable target activation evidence")))
	g.Expect(requests).To(Equal(0))
}

func TestExecuteCaptureSeedArtifactUsesRuntimeOwnedTypedEndpoint(t *testing.T) {
	g := NewWithT(t)
	digest := strings.Repeat("a", 64)
	action := antflyv1.HAPlannedActionStatus{
		Kind:                   string(haActionCaptureSeedArtifact),
		Executor:               string(haActionExecutorAdminAPI),
		StandbyName:            "standby-a",
		SlotName:               "standby-a",
		TargetLSN:              10,
		SeedArtifactGeneration: "seed-standby-a-10",
		AdminURL:               "http://primary-ha.default.svc:8081",
		AdminNodeID:            "primary-a",
		AdminMethod:            http.MethodPost,
		AdminPath:              haAdminBaseBackupsCapturePath,
		TopologyID:             "topology-a",
		TopologyGeneration:     7,
		TopologyNodeID:         "standby-a",
		TargetPVCName:          "standby-data",
		TargetPVCUID:           "target-pvc-uid",
	}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "primary-a", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{HighAvailability: &antflyv1.HighAvailabilitySpec{
			Identity: &antflyv1.HAReplicationIdentitySpec{ClusterID: 100, ShardID: 2, TableID: 3, TimelineID: 4, Epoch: 6, CurrentPrimaryID: "primary-a"},
			Standbys: []antflyv1.HAStandbySpec{{Name: "standby-a", SeedArtifact: &antflyv1.HASeedArtifactSpec{
				Location: "s3://ha-seeds/test", StagingRoot: "/target/staging",
				TopologyID: "topology-a", TopologyGeneration: 7, NodeID: "standby-a", TargetPVCUID: "target-pvc-uid",
				SourcePVC: &antflyv1.HASeedArtifactPVCSpec{ClaimName: "primary-data", MountPath: "/source"},
				TargetPVC: &antflyv1.HASeedArtifactPVCSpec{ClaimName: "standby-data", MountPath: "/target"},
			}}},
		}},
	}
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())
	pvcs := []client.Object{
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "primary-data", Namespace: "default", UID: types.UID("source-pvc-uid")}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "standby-data", Namespace: "default", UID: types.UID("target-pvc-uid")}},
	}
	requests := 0
	timeline := uint64(4)
	reconciler := &AntflyClusterReconciler{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(pvcs...).Build(), Scheme: s, HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests++
		g.Expect(req.Method).To(Equal(http.MethodPost))
		g.Expect(req.URL.Path).To(Equal(haAdminBaseBackupsCapturePath))
		var body adminsdk.SeedArtifactCaptureRequest
		g.Expect(json.NewDecoder(req.Body).Decode(&body)).To(Succeed())
		g.Expect(body).To(Equal(adminsdk.SeedArtifactCaptureRequest{
			SlotName: "standby-a", Generation: "seed-standby-a-10",
			TopologyId: "topology-a", TopologyGeneration: 7, NodeId: "standby-a",
			TargetPvcName: "standby-data", TargetPvcUid: "target-pvc-uid",
		}))
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(strings.NewReader(fmt.Sprintf(
				`{"schema_version":1,"action":{"action_id":"seed_capture:seed-standby-a-10","action_kind":"seed_capture","target":"seed-standby-a-10","state":"applied","node_id":"primary-a"},"slot_name":"standby-a","generation":"seed-standby-a-10","cluster_id":100,"shard_id":2,"table_id":3,"timeline_id":%d,"epoch":6,"manifest_id":"seed-standby-a-10","source_plan_sha256":"%s","backup_lsn":10,"checkpoint_lsn":10,"end_record_lsn":11,"manifest_sha256":"%s","capture_receipt_sha256":"%s","file_count":2,"total_bytes":20,"generation_root":"/antflydb/ha/seed-captures/generations/seed-standby-a-10","content_root":"/antflydb/ha/seed-captures/generations/seed-standby-a-10/content","manifest_path":"/antflydb/ha/seed-captures/generations/seed-standby-a-10/manifest.afha","already_captured":false,"topology_id":"topology-a","topology_generation":7,"node_id":"standby-a","target_pvc_name":"standby-data","target_pvc_uid":"target-pvc-uid"}`,
				timeline, digest, digest, digest,
			))),
		}, nil
	})}}

	handled, err := reconciler.executeHAPlannedActionTyped(context.Background(), cluster, &action)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(handled).To(BeTrue())
	g.Expect(requests).To(Equal(1))
	g.Expect(action.AdminResult).NotTo(BeNil())
	g.Expect(action.AdminResult.SeedManifestPath).To(Equal("/antflydb/ha/seed-captures/generations/seed-standby-a-10/manifest.afha"))
	g.Expect(action.AdminResult.SeedContentRoot).To(Equal("/antflydb/ha/seed-captures/generations/seed-standby-a-10/content"))
	g.Expect(action.AdminResult.CaptureReceiptSHA256).To(Equal(digest))

	timeline = 5
	action.AdminResult = nil
	handled, err = reconciler.executeHAPlannedActionTyped(context.Background(), cluster, &action)
	g.Expect(handled).To(BeTrue())
	g.Expect(err).To(MatchError(ContainSubstring("does not match the planned runtime-owned generation and identity")))
	g.Expect(action.AdminResult).To(BeNil())
}

func TestHAActivateSeededSlotWaitsForCompletedActivationArtifact(t *testing.T) {
	g := NewWithT(t)
	digest := strings.Repeat("a", 64)
	actions := []antflyv1.HAPlannedActionStatus{{
		Kind:                     string(haActionActivateSeedArtifact),
		Executor:                 string(haActionExecutorCLIJob),
		SlotName:                 "standby-a",
		TargetLSN:                1,
		SeedArtifactGeneration:   "initial-standby-a-1",
		SeedCaptureReceiptSHA256: digest,
		TargetLocalNodeID:        1,
		TargetReplicaID:          1,
	}, {
		Kind:                   string(haActionActivateSeededSlot),
		Executor:               string(haActionExecutorAdminAPI),
		DependsOn:              string(haActionActivateSeedArtifact),
		SlotName:               "standby-a",
		TargetLSN:              1,
		SeedArtifactGeneration: "initial-standby-a-1",
	}}

	g.Expect(haPlannedActionDependenciesSucceeded(actions, 1)).To(BeFalse())

	actions[0].AdminJobPhase = haAdminJobPhaseSucceeded
	g.Expect(haPlannedActionDependenciesSucceeded(actions, 1)).To(BeFalse())

	actions[0].SeedArtifactReceipt = &antflyv1.HASeedArtifactReceiptStatus{
		FormatVersion:               2,
		Generation:                  "initial-standby-a-1",
		SlotName:                    "standby-a",
		ManifestID:                  "initial-standby-a-1",
		BackupLSN:                   1,
		CheckpointLSN:               1,
		SeedReceiptSHA256:           digest,
		CaptureReceiptSHA256:        digest,
		ManifestSHA256:              digest,
		AggregateSHA256:             digest,
		MaterializedReceiptSHA256:   digest,
		MaterializedAggregateSHA256: digest,
		TargetLocalNodeID:           1,
		TargetReplicaID:             1,
		GenerationPath:              "live-generations/initial-standby-a-1",
		RawGenerationPath:           "generations/initial-standby-a-1",
	}
	g.Expect(haPlannedActionDependenciesSucceeded(actions, 1)).To(BeTrue())
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

func TestHAPlannedActionDependenciesDoNotAliasAcrossStandbys(t *testing.T) {
	g := NewWithT(t)
	actions := []antflyv1.HAPlannedActionStatus{
		{Kind: string(haActionPauseSlot), StandbyName: "standby-a", SlotName: "standby-a", AdminJobPhase: haAdminJobPhaseSucceeded},
		{Kind: string(haActionPauseSlot), StandbyName: "standby-b", SlotName: "standby-b", AdminJobPhase: haAdminJobPhaseFailed},
		{Kind: string(haActionDropSlot), DependsOn: string(haActionPauseSlot), StandbyName: "standby-b", SlotName: "standby-b"},
	}
	g.Expect(haPlannedActionDependenciesSucceeded(actions, 2)).To(BeFalse())
	actions[1].AdminJobPhase = haAdminJobPhaseSucceeded
	actions[1].AdminResult = &antflyv1.HAAdminActionResultStatus{
		SchemaVersion: 1, ActionID: "replication_slot_pause:standby-b", ActionKind: "replication_slot_pause",
		ActionTarget: "standby-b", ActionState: "applied", ActionNodeID: "primary-a", SlotAction: "pause", SlotName: "standby-b",
	}
	g.Expect(haPlannedActionDependenciesSucceeded(actions, 2)).To(BeTrue())
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
		FormerLastLSN:    12,
		RetainedFromLSN:  8,
	}
	g.Expect(haPlannedActionDependenciesSucceeded(rejoinActions, 1)).To(BeFalse())

	rejoinActions[0].AdminResult.RewindExecuted = true
	rejoinActions[0].AdminResult.RewindPreviousLastLSN = 12
	rejoinActions[0].AdminResult.RewindCurrentLastLSN = 13
	rejoinActions[0].AdminResult.RewindNextLSN = 14
	rejoinActions[0].AdminResult.RewindDiscardedLSNCount = 0
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
		Client: newHAControllerTestClient(t, s, cluster, failedPauseJob),
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

	g.Expect(reconcileHAAdminJobsUntilIdle(context.Background(), reconciler, cluster)).To(Succeed())

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

	digest := strings.Repeat("a", 64)
	activateAction := directPrimaryAction(haActionActivateSeededSlot)
	activateAction.SeedArtifactGeneration = "seed-standby-a-5"
	activation := haAdminActionResultFromSeededSlotActivateSDK(adminsdk.HASeededSlotActivateResponse{
		SchemaVersion:        1,
		Action:               receipt(adminsdk.HAActionKindSeededSlotActivate, "seed-standby-a-5", string(adminsdk.HAActionStateApplied), "primary-a"),
		SlotName:             "standby-a",
		Generation:           "seed-standby-a-5",
		ManifestId:           "base-standby-a-5",
		TimelineId:           4,
		CheckpointLsn:        7,
		SeedReceiptSha256:    digest,
		CaptureReceiptSha256: digest,
		ManifestSha256:       digest,
		AggregateSha256:      digest,
	})
	g.Expect(requireHADirectAdminActionResultStatus(&activateAction, activation)).To(Succeed())
	g.Expect(activateAction.AdminResult.CheckpointLSN).To(Equal(uint64(7)))
	g.Expect(activateAction.AdminResult.SeedArtifactGeneration).To(Equal("seed-standby-a-5"))

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
			FormerLastLsn:     12,
			RetainedFromLsn:   8,
			DataLossDiscarded: false,
		},
		Rewind: adminsdk.HARejoinRewindResult{
			NodeId:            "primary-a",
			ForkLsn:           12,
			PreviousLastLsn:   12,
			CurrentLastLsn:    13,
			NextLsn:           14,
			DiscardedLsnCount: 0,
			TargetTimelineId:  5,
			TargetEpoch:       7,
			DataLossDiscarded: false,
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
	staleObservedResponse.Rewind.CurrentLastLsn = 13
	staleObservedResponse.Rewind.NextLsn = 14
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
		"former_last_lsn=12",
		"retained_from_lsn=8",
		"data_loss_discarded=false",
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
	g.Expect(rejoin.FormerLastLSN).To(Equal(uint64(12)))
	g.Expect(rejoin.RetainedFromLSN).To(Equal(uint64(8)))
	g.Expect(rejoin.DataLossDiscarded).To(BeFalse())
}

func TestParseHASeedArtifactReceiptRequiresMatchingTypedEvidence(t *testing.T) {
	g := NewWithT(t)
	action := antflyv1.HAPlannedActionStatus{
		Kind:                   string(haActionRestoreSeedArtifact),
		SlotName:               "standby-a",
		TargetLSN:              10,
		SeedArtifactGeneration: "seed-standby-a-10",
		AdminJobPhase:          haAdminJobPhaseSucceeded,
	}
	body := fmt.Sprintf(`{"format_version":1,"generation":"seed-standby-a-10","slot_name":"standby-a","cluster_id":100,"shard_id":0,"table_id":0,"timeline_id":4,"epoch":6,"manifest_id":"base-standby-a-10","backup_lsn":10,"checkpoint_lsn":12,"manifest_sha256":"%s","aggregate_sha256":"%s","total_bytes":42,"files":[{"path":"catalog/manifest"}]}`, strings.Repeat("a", 64), strings.Repeat("b", 64))
	receipt := parseHASeedArtifactReceipt(body, action)
	g.Expect(receipt).NotTo(BeNil())
	g.Expect(receipt.Generation).To(Equal("seed-standby-a-10"))
	g.Expect(receipt.FileCount).To(Equal(int32(1)))
	action.SeedArtifactReceipt = receipt
	g.Expect(haAdminActionSucceededWithEvidence(action)).To(BeTrue())

	// The Zig artifact CLI emits the complete per-file and per-chunk receipt.
	// Keep strict unknown-field decoding, but model the actual cross-language
	// wire contract rather than accepting only the older path-only fixture.
	completeBody := fmt.Sprintf(`{"format_version":1,"generation":"seed-standby-a-10","slot_name":"standby-a","cluster_id":100,"shard_id":0,"table_id":0,"timeline_id":4,"epoch":6,"manifest_id":"base-standby-a-10","backup_lsn":10,"checkpoint_lsn":12,"manifest_sha256":"%s","aggregate_sha256":"%s","total_bytes":42,"files":[{"path":"catalog/manifest","size_bytes":42,"crc32":1234,"sha256":"%s","chunks":[{"index":0,"size_bytes":42,"sha256":"%s"}]}]}`, strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64), strings.Repeat("d", 64))
	g.Expect(parseHASeedArtifactReceipt(completeBody, action)).NotTo(BeNil())

	wrongGeneration := strings.Replace(body, "seed-standby-a-10", "seed-standby-a-9", 1)
	g.Expect(parseHASeedArtifactReceipt(wrongGeneration, action)).To(BeNil())

	pruneAction := antflyv1.HAPlannedActionStatus{
		Kind:                          string(haActionPruneSeedArtifacts),
		SlotName:                      "standby-a",
		SeedArtifactGeneration:        "seed-standby-a-10",
		SeedArtifactRetainGenerations: 2,
		AdminJobPhase:                 haAdminJobPhaseSucceeded,
	}
	pruneReceipt := parseHASeedArtifactReceipt(`{"format_version":1,"slot_name":"standby-a","current_generation":"seed-standby-a-10","retained_generations":2,"deleted_generations":1}`, pruneAction)
	g.Expect(pruneReceipt).NotTo(BeNil())
	pruneAction.SeedArtifactReceipt = pruneReceipt
	g.Expect(haAdminActionSucceededWithEvidence(pruneAction)).To(BeTrue())

	activateAction := antflyv1.HAPlannedActionStatus{
		Kind:                     "ActivateSeedArtifact",
		SlotName:                 "standby-a",
		TargetLSN:                10,
		SeedArtifactGeneration:   "seed-standby-a-10",
		SeedCaptureReceiptSHA256: strings.Repeat("d", 64),
		TargetLocalNodeID:        7,
		TargetReplicaID:          1,
		AdminJobPhase:            haAdminJobPhaseSucceeded,
	}
	activation := fmt.Sprintf(`{"format_version":2,"generation":"seed-standby-a-10","slot_name":"standby-a","cluster_id":100,"shard_id":0,"table_id":0,"timeline_id":4,"epoch":6,"manifest_id":"base-standby-a-10","backup_lsn":10,"checkpoint_lsn":12,"seed_receipt_sha256":"%s","capture_receipt_sha256":"%s","manifest_sha256":"%s","aggregate_sha256":"%s","generation_path":"live-generations/seed-standby-a-10","raw_generation_path":"generations/seed-standby-a-10","materialized_receipt_sha256":"%s","materialized_aggregate_sha256":"%s","target_local_node_id":7,"target_replica_id":1,"topology_id":"test-standalone","topology_generation":3,"node_id":"standby-a","target_pvc_name":"standby-a-data","target_pvc_uid":"pvc-uid-1"}`, strings.Repeat("c", 64), strings.Repeat("d", 64), strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("e", 64), strings.Repeat("f", 64))
	activationReceipt := parseHASeedArtifactReceipt(activation, activateAction)
	g.Expect(activationReceipt).NotTo(BeNil())
	g.Expect(activationReceipt.CheckpointLSN).To(Equal(uint64(12)))
	g.Expect(activationReceipt.GenerationPath).To(Equal("live-generations/seed-standby-a-10"))
	g.Expect(activationReceipt.RawGenerationPath).To(Equal("generations/seed-standby-a-10"))
	g.Expect(activationReceipt.CaptureReceiptSHA256).To(Equal(strings.Repeat("d", 64)))
	g.Expect(activationReceipt.MaterializedReceiptSHA256).To(Equal(strings.Repeat("e", 64)))
	g.Expect(activationReceipt.MaterializedAggregateSHA256).To(Equal(strings.Repeat("f", 64)))
	g.Expect(activationReceipt.TargetLocalNodeID).To(Equal(uint64(7)))
	g.Expect(activationReceipt.TargetReplicaID).To(Equal(uint64(1)))
	g.Expect(activationReceipt.TopologyGeneration).To(Equal(int64(3)))
	g.Expect(activationReceipt.TargetPVCUID).To(Equal("pvc-uid-1"))
	activateAction.SeedArtifactReceipt = activationReceipt
	g.Expect(haAdminActionSucceededWithEvidence(activateAction)).To(BeTrue())
}

func TestParseHASeedArtifactReceiptVersionContracts(t *testing.T) {
	g := NewWithT(t)
	manifestDigest := strings.Repeat("a", 64)
	aggregateDigest := strings.Repeat("b", 64)
	seedDigest := strings.Repeat("c", 64)
	captureDigest := strings.Repeat("d", 64)
	body := func(version int) string {
		captureField := ""
		if version == 4 {
			captureField = fmt.Sprintf(`,"capture_receipt_sha256":"%s"`, captureDigest)
		}
		return fmt.Sprintf(`{"format_version":%d,"generation":"seed-standby-a-10","slot_name":"standby-a","cluster_id":100,"shard_id":10,"table_id":20,"timeline_id":4,"epoch":6,"manifest_id":"base-standby-a-10","backup_lsn":10,"checkpoint_lsn":12,"manifest_sha256":"%s","aggregate_sha256":"%s","seed_receipt_sha256":"%s"%s,"generation_path":"generations/seed-standby-a-10","topology_id":"test-standalone","topology_generation":3,"node_id":"standby-a","target_pvc_name":"standby-a-data","target_pvc_uid":"pvc-uid-1","total_bytes":42,"files":[{"path":"catalog/manifest"}]}`, version, manifestDigest, aggregateDigest, seedDigest, captureField)
	}
	for _, kind := range []haActionKind{haActionPublishSeedArtifact, haActionRestoreSeedArtifact} {
		for _, version := range []int{1, 2, 3, 4} {
			action := antflyv1.HAPlannedActionStatus{
				Kind: string(kind), SlotName: "standby-a", TargetLSN: 10,
				SeedArtifactGeneration: "seed-standby-a-10", AdminJobPhase: haAdminJobPhaseSucceeded,
			}
			receipt := parseHASeedArtifactReceipt(body(version), action)
			g.Expect(receipt).NotTo(BeNil(), "%s format v%d", kind, version)
			g.Expect(receipt.FormatVersion).To(Equal(int32(version)))
			g.Expect(receipt.Generation).To(Equal("seed-standby-a-10"))
			g.Expect(receipt.SlotName).To(Equal("standby-a"))
			g.Expect(receipt.ClusterID).To(Equal(uint64(100)))
			g.Expect(receipt.ShardID).To(Equal(uint64(10)))
			g.Expect(receipt.TableID).To(Equal(uint64(20)))
			g.Expect(receipt.TimelineID).To(Equal(uint64(4)))
			g.Expect(receipt.Epoch).To(Equal(uint64(6)))
			g.Expect(receipt.ManifestID).To(Equal("base-standby-a-10"))
			g.Expect(receipt.BackupLSN).To(Equal(uint64(10)))
			g.Expect(receipt.CheckpointLSN).To(Equal(uint64(12)))
			g.Expect(receipt.ManifestSHA256).To(Equal(manifestDigest))
			g.Expect(receipt.AggregateSHA256).To(Equal(aggregateDigest))
			g.Expect(receipt.SeedReceiptSHA256).To(Equal(seedDigest))
			g.Expect(receipt.GenerationPath).To(Equal("generations/seed-standby-a-10"))
			g.Expect(receipt.TopologyID).To(Equal("test-standalone"))
			g.Expect(receipt.TopologyGeneration).To(Equal(int64(3)))
			g.Expect(receipt.NodeID).To(Equal("standby-a"))
			g.Expect(receipt.TargetPVCName).To(Equal("standby-a-data"))
			g.Expect(receipt.TargetPVCUID).To(Equal("pvc-uid-1"))
			g.Expect(receipt.TotalBytes).To(Equal(uint64(42)))
			g.Expect(receipt.FileCount).To(Equal(int32(1)))
			action.SeedArtifactReceipt = receipt
			g.Expect(haAdminActionSucceededWithEvidence(action)).To(BeTrue())
		}
		for _, unsupported := range []int{0, 5} {
			action := antflyv1.HAPlannedActionStatus{
				Kind: string(kind), SlotName: "standby-a", TargetLSN: 10,
				SeedArtifactGeneration: "seed-standby-a-10", AdminJobPhase: haAdminJobPhaseSucceeded,
			}
			g.Expect(parseHASeedArtifactReceipt(body(unsupported), action)).To(BeNil(), "%s format v%d must fail closed", kind, unsupported)
		}
		bound := antflyv1.HAPlannedActionStatus{
			Kind: string(kind), SlotName: "standby-a", TargetLSN: 10, SeedArtifactGeneration: "seed-standby-a-10",
			TopologyID: "test-standalone", TopologyGeneration: 3, TopologyNodeID: "standby-a",
			TargetPVCName: "standby-a-data", TargetPVCUID: "pvc-uid-1", SeedCaptureReceiptSHA256: captureDigest,
		}
		g.Expect(parseHASeedArtifactReceipt(body(4), bound)).NotTo(BeNil())
		g.Expect(parseHASeedArtifactReceipt(body(3), bound)).To(BeNil(), "capture-bound topology transport requires COMPLETE v4")
	}

	activation := antflyv1.HAPlannedActionStatus{
		Kind: string(haActionActivateSeedArtifact), SlotName: "standby-a", TargetLSN: 10,
		SeedArtifactGeneration: "seed-standby-a-10", AdminJobPhase: haAdminJobPhaseSucceeded,
		SeedCaptureReceiptSHA256: captureDigest, TargetLocalNodeID: 7, TargetReplicaID: 1,
	}
	activationBody := func(version int) string {
		return fmt.Sprintf(`{"format_version":%d,"generation":"seed-standby-a-10","slot_name":"standby-a","manifest_id":"base-standby-a-10","backup_lsn":10,"checkpoint_lsn":12,"manifest_sha256":"%s","aggregate_sha256":"%s","seed_receipt_sha256":"%s","capture_receipt_sha256":"%s","generation_path":"live-generations/seed-standby-a-10","raw_generation_path":"generations/seed-standby-a-10","materialized_receipt_sha256":"%s","materialized_aggregate_sha256":"%s","target_local_node_id":7,"target_replica_id":1}`, version, manifestDigest, aggregateDigest, seedDigest, strings.Repeat("d", 64), strings.Repeat("e", 64), strings.Repeat("f", 64))
	}
	g.Expect(parseHASeedArtifactReceipt(activationBody(1), activation)).To(BeNil(), "materialized activation requires v2")
	g.Expect(parseHASeedArtifactReceipt(activationBody(2), activation)).NotTo(BeNil())

	prune := antflyv1.HAPlannedActionStatus{
		Kind: string(haActionPruneSeedArtifacts), SlotName: "standby-a",
		SeedArtifactGeneration: "seed-standby-a-10", SeedArtifactRetainGenerations: 2,
		AdminJobPhase: haAdminJobPhaseSucceeded,
	}
	g.Expect(parseHASeedArtifactReceipt(`{"format_version":1,"slot_name":"standby-a","current_generation":"seed-standby-a-10","retained_generations":2,"deleted_generations":1}`, prune)).NotTo(BeNil())
	g.Expect(parseHASeedArtifactReceipt(`{"format_version":2,"slot_name":"standby-a","current_generation":"seed-standby-a-10","retained_generations":2,"deleted_generations":1}`, prune)).To(BeNil(), "prune receipt schema remains v1-only")
}

func TestParseHALocalGenerationGCReceiptFailsClosedOnScopeDigestOrSchemaDrift(t *testing.T) {
	g := NewWithT(t)
	base := antflyv1.HAPlannedActionStatus{
		Kind: string(haActionGCSourceSeedGenerations), SlotName: "standby-a",
		SeedArtifactGeneration: "seed-standby-a-10", AdminJobPhase: haAdminJobPhaseSucceeded,
	}
	body := fmt.Sprintf(`{"schema_version":1,"action_kind":"gc_local_seed_generations","scope":"source_capture","slot_name":"standby-a","current_generation":"seed-standby-a-10","checkpoint_sha256":"%s","retained_generations":2,"protected_generations":1,"deleted_generations":3,"resumed_tombstones":1,"skipped_ineligible":0}`, strings.Repeat("d", 64))
	receipt := parseHASeedArtifactReceipt(body, base)
	g.Expect(receipt).NotTo(BeNil())
	base.SeedArtifactReceipt = receipt
	g.Expect(haAdminActionSucceededWithEvidence(base)).To(BeTrue())

	target := base
	target.Kind = string(haActionGCTargetSeedGenerations)
	target.SeedArtifactReceipt = nil
	targetBody := strings.Replace(body, `"scope":"source_capture"`, `"scope":"target_activation"`, 1)
	g.Expect(parseHASeedArtifactReceipt(targetBody, target)).NotTo(BeNil())
	g.Expect(parseHASeedArtifactReceipt(body, target)).To(BeNil(), "source deletion evidence cannot authorize target deletion")
	g.Expect(parseHASeedArtifactReceipt(strings.Replace(body, strings.Repeat("d", 64), strings.Repeat("D", 64), 1), base)).To(BeNil())
	g.Expect(parseHASeedArtifactReceipt(strings.TrimSuffix(body, "}")+`,"unexpected":true}`, base)).To(BeNil())
	g.Expect(parseHASeedArtifactReceipt(body+` {}`, base)).To(BeNil())
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
		"former_last_lsn=12",
		"retained_from_lsn=8",
		"data_loss_discarded=false",
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
	g.Expect(result.FormerLastLSN).To(Equal(uint64(12)))
	g.Expect(result.RetainedFromLSN).To(Equal(uint64(8)))
	g.Expect(result.DataLossDiscarded).To(BeFalse())

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
		"assessment.former_last_lsn=12",
		"assessment.retained_from_lsn=8",
		"assessment.data_loss_discarded=false",
		"rewind.node_id=primary-a",
		"rewind.fork_lsn=12",
		"rewind.previous_last_lsn=12",
		"rewind.current_last_lsn=13",
		"rewind.next_lsn=14",
		"rewind.discarded_lsn_count=0",
		"rewind.target_timeline_id=5",
		"rewind.target_epoch=7",
		"rewind.data_loss_discarded=false",
		"",
	}, "\n"))
	g.Expect(ok).To(BeTrue())
	g.Expect(rewindExecuted.Action).To(Equal("rewind"))
	g.Expect(rewindExecuted.RewindExecuted).To(BeTrue())
	g.Expect(rewindExecuted.RewindPreviousLastLSN).To(Equal(uint64(12)))
	g.Expect(rewindExecuted.RewindCurrentLastLSN).To(Equal(uint64(13)))
	g.Expect(rewindExecuted.RewindNextLSN).To(Equal(uint64(14)))
	g.Expect(rewindExecuted.RewindDiscardedLSNCount).To(BeZero())

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
		"assessment.former_last_lsn=12",
		"assessment.retained_from_lsn=8",
		"assessment.data_loss_discarded=false",
		"rewind.node_id=primary-a",
		"rewind.fork_lsn=12",
		"rewind.previous_last_lsn=12",
		"rewind.current_last_lsn=13",
		"rewind.next_lsn=14",
		"rewind.discarded_lsn_count=0",
		"rewind.target_timeline_id=5",
		"rewind.target_epoch=7",
		"rewind.data_loss_discarded=false",
		"",
	}, "\n"))
	g.Expect(ok).To(BeTrue())
	g.Expect(adminResult.RejoinAction).To(Equal("rewind"))
	g.Expect(adminResult.RewindExecuted).To(BeTrue())
	g.Expect(adminResult.RewindDiscardedLSNCount).To(BeZero())

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
	g.Expect(former.FormerLastLSN).To(Equal(uint64(12)))
	g.Expect(former.ObservedLSN).To(Equal(uint64(12)))
	g.Expect(former.RetainedFromLSN).To(Equal(uint64(8)))
	g.Expect(former.DataLossDiscarded).To(BeFalse())

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

	result, ok := parseHARejoinAPIResult([]byte(`{"schema_version":1,"action":{"action_id":"rejoin_rewind:primary-a","action_kind":"rejoin_rewind","target":"primary-a","state":"applied","node_id":"primary-a"},"assessment":{"action":"rewind","reason":"parent_timeline_retained","former_node_id":"primary-a","target_timeline_id":5,"target_epoch":7,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":4,"parent_epoch":6,"fork_lsn":12,"former_last_lsn":12,"retained_from_lsn":8,"data_loss_discarded":false},"rewind":{"node_id":"primary-a","fork_lsn":12,"previous_last_lsn":12,"current_last_lsn":13,"next_lsn":14,"discarded_lsn_count":0,"target_timeline_id":5,"target_epoch":7,"data_loss_discarded":false}}`))
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
	g.Expect(result.RewindPreviousLastLSN).To(Equal(uint64(12)))
	g.Expect(result.RewindCurrentLastLSN).To(Equal(uint64(13)))
	g.Expect(result.RewindNextLSN).To(Equal(uint64(14)))
	g.Expect(result.RewindDiscardedLSNCount).To(BeZero())
	g.Expect(result.DataLossDiscarded).To(BeFalse())

	status := haRejoinAdminActionResult(result)
	g.Expect(status.SchemaVersion).To(Equal(uint32(1)))
	g.Expect(status.ActionID).To(Equal("rejoin_rewind:primary-a"))
	g.Expect(status.ActionKind).To(Equal("rejoin_rewind"))
	g.Expect(status.ActionTarget).To(Equal("primary-a"))
	g.Expect(status.ActionState).To(Equal("applied"))
	g.Expect(status.RewindExecuted).To(BeTrue())
	g.Expect(status.RewindPreviousLastLSN).To(Equal(uint64(12)))
	g.Expect(status.RewindCurrentLastLSN).To(Equal(uint64(13)))
	g.Expect(status.RewindNextLSN).To(Equal(uint64(14)))
	g.Expect(status.RewindDiscardedLSNCount).To(BeZero())

	roundTripped, ok := haRejoinJobResultFromAdminResult(status)
	g.Expect(ok).To(BeTrue())
	g.Expect(roundTripped.SchemaVersion).To(Equal(uint32(1)))
	g.Expect(roundTripped.ActionID).To(Equal("rejoin_rewind:primary-a"))
	g.Expect(roundTripped.RewindExecuted).To(BeTrue())
	g.Expect(roundTripped.RewindDiscardedLSNCount).To(BeZero())

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

func TestObserveHAPrimaryAdminStatusDebouncesTransientFailureBeforeAutomaticFailover(t *testing.T) {
	g := NewWithT(t)
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	failing := true
	reconciler := &AntflyClusterReconciler{
		Now: func() time.Time { return now },
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if failing {
				return nil, fmt.Errorf("transient primary admin timeout")
			}
			body := `{"schema_version":1,"snapshot":{"role":"primary","node_id":"primary-a","identity":{"cluster_id":1,"shard_id":2,"table_id":3,"timeline_id":4,"epoch":5},"current_lsn":12,"slots":[],"retention":{"primary_lsn":12,"oldest_restart_lsn":12,"retained_lsn_count":0,"retained_byte_count":0,"retained_age_ns":0,"active_slots":0,"reseed_recommended":0}}}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})},
	}
	cluster := &antflyv1.AntflyCluster{Spec: antflyv1.AntflyClusterSpec{
		HighAvailability: &antflyv1.HighAvailabilitySpec{
			Mode:  antflyv1.HAModeHotStandby,
			Admin: &antflyv1.HAAdminSpec{PrimaryURL: "http://primary-ha.default.svc:8081"},
			AutomaticFailover: &antflyv1.HAAutomaticFailoverPolicy{
				Enabled:                           true,
				FencingAuthority:                  antflyv1.HAFencingAuthorityKubernetesLease,
				MinimumConsecutiveFailures:        3,
				MinimumUnreachableDurationSeconds: 30,
			},
		},
	}}

	for attempt, advance := range []time.Duration{0, 10 * time.Second, 10 * time.Second} {
		now = now.Add(advance)
		g.Expect(reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster)).NotTo(Succeed())
		g.Expect(cluster.Status.HAStatus.PrimaryAdminConsecutiveFailures).To(Equal(int32(attempt + 1)))
		g.Expect(cluster.Status.HAStatus.PrimaryAdminFailureThresholdMet).To(BeFalse())
		g.Expect(haPrimaryAdminUnavailable(cluster.Status.HAStatus)).To(BeFalse())
	}

	now = now.Add(11 * time.Second)
	g.Expect(reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster)).NotTo(Succeed())
	g.Expect(cluster.Status.HAStatus.PrimaryAdminConsecutiveFailures).To(Equal(int32(4)))
	g.Expect(cluster.Status.HAStatus.PrimaryAdminFailureThresholdMet).To(BeTrue())
	g.Expect(haPrimaryAdminUnavailable(cluster.Status.HAStatus)).To(BeTrue())

	failing = false
	g.Expect(reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.PrimaryAdminReachable).To(BeTrue())
	g.Expect(cluster.Status.HAStatus.PrimaryAdminConsecutiveFailures).To(BeZero())
	g.Expect(cluster.Status.HAStatus.PrimaryAdminUnreachableSince).To(BeNil())
	g.Expect(cluster.Status.HAStatus.PrimaryAdminFailureThresholdMet).To(BeFalse())
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
		DefaultInferenceImage: "ghcr.io/antflydb/antfly:zig-test",
	}
	cluster := baseClusterWithInferenceSpec()

	err := reconciler.reconcileInferencePool(ctx, cluster)
	g.Expect(err).NotTo(HaveOccurred())

	pool := &inferencev1alpha1.InferencePool{}
	err = reconciler.Get(ctx, types.NamespacedName{Name: "test-cluster-inference", Namespace: "default"}, pool)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(pool.Spec.Image).To(Equal("ghcr.io/antflydb/antfly:zig-test"))
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
		DefaultInferenceImage: "ghcr.io/antflydb/antfly:zig-test",
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
		DefaultInferenceImage: "ghcr.io/antflydb/antfly:zig-test",
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
		DefaultInferenceImage: "ghcr.io/antflydb/antfly:zig-test",
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
	g.Expect(defaultProbe.TimeoutSeconds).To(Equal(int32(10)))
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
			Image:               "antfly:test",
			InternalServiceAuth: testInternalServiceAuthSpec(),
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
			Image:               "antfly:test",
			InternalServiceAuth: testInternalServiceAuthSpec(),
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
			Image:               "antfly:test",
			InternalServiceAuth: testInternalServiceAuthSpec(),
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
			Image:               "antfly:test",
			InternalServiceAuth: testInternalServiceAuthSpec(),
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
			Image:               "antfly:test",
			InternalServiceAuth: testInternalServiceAuthSpec(),
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

func TestUpdateProductTierStatusReportsDistributedShape(t *testing.T) {
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
	g.Expect(cluster.Status.ProductTierStatus.Mode).To(Equal(antflyv1.ClusterModeDistributed))
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
	err = reconciler.reconcileServices(context.Background(), cluster, false)
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
	err = reconciler.reconcileServices(context.Background(), cluster, false)
	g.Expect(err).NotTo(HaveOccurred())
}

func TestReconcileServices_DrainsPublicAPIWithoutReplacingService(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())
	client := fake.NewClientBuilder().WithScheme(s).Build()
	reconciler := &AntflyClusterReconciler{Client: client, Scheme: s}
	enabled := true
	serviceType := corev1.ServiceTypeLoadBalancer
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default", UID: "cluster-uid"},
		Spec: antflyv1.AntflyClusterSpec{
			Mode:                antflyv1.ClusterModeDistributed,
			InternalServiceAuth: testInternalServiceAuthSpec(),
			MetadataNodes: antflyv1.MetadataNodesSpec{
				MetadataAPI:  antflyv1.APISpec{Port: 12377},
				MetadataRaft: antflyv1.APISpec{Port: 9017},
			},
			DataNodes: antflyv1.DataNodesSpec{
				API:  antflyv1.APISpec{Port: 12380},
				Raft: antflyv1.APISpec{Port: 9021},
			},
			PublicAPI: &antflyv1.PublicAPIConfig{Enabled: &enabled, ServiceType: &serviceType, Port: 80},
		},
	}

	g.Expect(reconciler.reconcileServices(context.Background(), cluster, true)).To(Succeed())
	key := types.NamespacedName{Name: "test-cluster-public-api", Namespace: "default"}
	service := &corev1.Service{}
	g.Expect(client.Get(context.Background(), key, service)).To(Succeed())
	uid := service.UID
	g.Expect(service.Spec.Selector).To(HaveKeyWithValue(internalServiceAuthPublicBoundaryLabel, "enforce-ready"))
	g.Expect(service.Annotations).To(HaveKeyWithValue(internalServiceAuthPublicBoundaryAnnotation, "suspended"))

	g.Expect(reconciler.reconcileServices(context.Background(), cluster, false)).To(Succeed())
	service = &corev1.Service{}
	g.Expect(client.Get(context.Background(), key, service)).To(Succeed())
	g.Expect(service.UID).To(Equal(uid))
	g.Expect(service.Spec.Selector).NotTo(HaveKey(internalServiceAuthPublicBoundaryLabel))
	g.Expect(service.Annotations).To(HaveKeyWithValue(internalServiceAuthPublicBoundaryAnnotation, "enforced"))
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
					Image:               "antfly:latest",
					InternalServiceAuth: testInternalServiceAuthSpec(),
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
			Expect(metadataSts.Annotations).To(HaveKeyWithValue(metadataTopologyReplicasAnnotation, "3"))
			Expect(metadataSts.Spec.VolumeClaimTemplates).To(HaveLen(1))
			Expect(metadataSts.Spec.VolumeClaimTemplates[0].Annotations).To(HaveKeyWithValue(metadataTopologyReplicasAnnotation, "3"))
			Expect(metadataSts.Spec.Template.Annotations).To(HaveKeyWithValue(metadataMembershipStatusCapabilityAnnotation, "v2"))
			Eventually(func() int32 {
				observed := &antflyv1.AntflyCluster{}
				if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), observed); err != nil {
					return 0
				}
				return observed.Status.MetadataTopologyReplicas
			}, timeout, interval).Should(Equal(int32(3)))

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
			Expect(configMap.Annotations).To(HaveKey(generatedConfigHashAnnotation))
			Expect(metadataSts.Spec.Template.Annotations).To(HaveKeyWithValue(
				generatedConfigHashAnnotation,
				configMap.Annotations[generatedConfigHashAnnotation],
			))
			Expect(dataSts.Spec.Template.Annotations).To(HaveKeyWithValue(
				generatedConfigHashAnnotation,
				configMap.Annotations[generatedConfigHashAnnotation],
			))
			configSum := sha256.Sum256([]byte(configMap.Data["config.json"]))
			wantPublicationHash := fmt.Sprintf("%x", configSum)
			Eventually(func() string {
				observed := &antflyv1.AntflyCluster{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: namespace}, observed); err != nil {
					return ""
				}
				if observed.Status.ConfigPublication == nil || observed.Status.ConfigPublication.ObservedGeneration != observed.Generation {
					return ""
				}
				return observed.Status.ConfigPublication.SHA256
			}, timeout, interval).Should(Equal(wantPublicationHash))

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
					Image:               "antfly:latest",
					InternalServiceAuth: testInternalServiceAuthSpec(),
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

	Context("When metadata replicas change without CEL admission", func() {
		It("Should block resource reconciliation with the controller fallback", func() {
			cluster := &antflyv1.AntflyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-metadata-bypass", Namespace: "default"},
				Spec: antflyv1.AntflyClusterSpec{
					Mode:                antflyv1.ClusterModeDistributed,
					Image:               "antfly:latest",
					InternalServiceAuth: testInternalServiceAuthSpec(),
					MetadataNodes: antflyv1.MetadataNodesSpec{
						Replicas:     1,
						Resources:    antflyv1.ResourceSpec{CPU: "500m", Memory: "512Mi"},
						MetadataAPI:  antflyv1.APISpec{Port: 12377},
						MetadataRaft: antflyv1.APISpec{Port: 9017},
					},
					DataNodes: antflyv1.DataNodesSpec{
						Replicas:  3,
						Resources: antflyv1.ResourceSpec{CPU: "1000m", Memory: "2Gi"},
						API:       antflyv1.APISpec{Port: 12380},
						Raft:      antflyv1.APISpec{Port: 9021},
					},
					Config: "{}",
					Storage: antflyv1.StorageSpec{
						StorageClass:    "standard",
						MetadataStorage: "1Gi",
						DataStorage:     "10Gi",
					},
				},
			}
			Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
			DeferCleanup(func() {
				current := &antflyv1.AntflyCluster{}
				if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), current); err == nil {
					Expect(k8sClient.Delete(ctx, current)).To(Succeed())
				}
			})

			Eventually(func() int32 {
				current := &antflyv1.AntflyCluster{}
				if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), current); err != nil {
					return 0
				}
				return current.Status.MetadataTopologyReplicas
			}, timeout, interval).Should(Equal(int32(1)))

			current := &antflyv1.AntflyCluster{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), current)).To(Succeed())
			current.Spec.MetadataNodes.Replicas = 3
			Expect(k8sClient.Update(ctx, current)).To(Succeed())

			Eventually(func() string {
				observed := &antflyv1.AntflyCluster{}
				if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), observed); err != nil {
					return ""
				}
				condition := meta.FindStatusCondition(observed.Status.Conditions, antflyv1.TypeConfigurationValid)
				if condition == nil || condition.Status != metav1.ConditionFalse {
					return ""
				}
				return condition.Message
			}, timeout, interval).Should(ContainSubstring("metadataNodes.replicas' is immutable"))

			metadataStatefulSet := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: cluster.Name + "-metadata", Namespace: cluster.Namespace,
			}, metadataStatefulSet)).To(Succeed())
			Expect(*metadataStatefulSet.Spec.Replicas).To(Equal(int32(1)))
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

func TestBuildMetadataClusterConfigIncludesRaftAndOrchestrationEndpoints(t *testing.T) {
	g := NewWithT(t)
	reconciler := &AntflyClusterReconciler{}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "antfly-system",
		},
		Spec: antflyv1.AntflyClusterSpec{
			MetadataNodes: antflyv1.MetadataNodesSpec{
				MetadataAPI:  antflyv1.APISpec{Port: 12377},
				MetadataRaft: antflyv1.APISpec{Port: 9017},
			},
		},
	}

	type metadataPeerEndpoints struct {
		RaftURL          string `json:"raft_url"`
		OrchestrationURL string `json:"orchestration_url"`
	}
	var peers map[string]metadataPeerEndpoints
	g.Expect(json.Unmarshal([]byte(reconciler.buildMetadataClusterConfig(cluster, 3)), &peers)).To(Succeed())
	g.Expect(peers).To(HaveLen(3))
	g.Expect(peers["1"]).To(Equal(metadataPeerEndpoints{
		RaftURL:          "http://test-cluster-metadata-0.test-cluster-metadata.antfly-system.svc.cluster.local:9017",
		OrchestrationURL: "http://test-cluster-metadata-0.test-cluster-metadata.antfly-system.svc.cluster.local:12377",
	}))
	g.Expect(peers["3"]).To(Equal(metadataPeerEndpoints{
		RaftURL:          "http://test-cluster-metadata-2.test-cluster-metadata.antfly-system.svc.cluster.local:9017",
		OrchestrationURL: "http://test-cluster-metadata-2.test-cluster-metadata.antfly-system.svc.cluster.local:12377",
	}))
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
	client := newHAControllerTestClient(t, s, cluster)

	reconciler := &AntflyClusterReconciler{
		Client: client,
		Scheme: s,
	}

	err = reconciler.reconcileServices(context.Background(), cluster, false)
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

func TestCreatePublicAPIService_DistributedTargetsMetadataAPI(t *testing.T) {
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
	client := newHAControllerTestClient(t, s, cluster)
	reconciler := &AntflyClusterReconciler{Client: client, Scheme: s}

	g.Expect(reconciler.reconcileServices(context.Background(), cluster, false)).To(Succeed())

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

	g.Expect(reconciler.reconcileServices(context.Background(), cluster, false)).To(Succeed())

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
	client := newHAControllerTestClient(t, s, cluster)

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

func TestReconcileLegacySwarmLayoutRunsStandaloneWithoutReplacingPVCIdentity(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := baseStandaloneControllerCluster()
	cluster.UID = types.UID("cluster-uid")
	cluster.Spec.Mode = antflyv1.ClusterModeSwarm
	cluster.Spec.Swarm = cluster.Spec.Standalone
	cluster.Spec.Standalone = nil
	cluster.Spec.Storage.SwarmStorage = cluster.Spec.Storage.StandaloneStorage
	cluster.Spec.Storage.StandaloneStorage = ""
	cluster.NormalizeLegacySwarm()

	controller := true
	existing := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-standalone-swarm", Namespace: "default", UID: types.UID("legacy-sts-uid"),
			Labels: map[string]string{"app.kubernetes.io/instance": cluster.Name},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: antflyv1.GroupVersion.String(), Kind: "AntflyCluster", Name: cluster.Name,
				UID: cluster.UID, Controller: &controller,
			}},
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName:          "test-standalone-swarm",
			Selector:             &metav1.LabelSelector{MatchLabels: serviceSelectorLabels(cluster.Name, "swarm")},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "swarm-storage"}}},
		},
	}
	client := newHAControllerTestClient(t, s, cluster, existing)
	reconciler := &AntflyClusterReconciler{Client: client, Scheme: s}
	reconciler.applyDefaults(cluster)

	g.Expect(reconciler.ensureTopologyResourcesMatchMode(context.Background(), cluster, topologyModeStandalone)).To(Succeed())
	g.Expect(reconciler.reconcileStandaloneStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())

	observed := &appsv1.StatefulSet{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: existing.Name, Namespace: existing.Namespace}, observed)).To(Succeed())
	g.Expect(observed.UID).To(Equal(existing.UID))
	g.Expect(observed.Spec.ServiceName).To(Equal("test-standalone-swarm"))
	g.Expect(observed.Spec.Selector.MatchLabels).To(HaveKeyWithValue("app.kubernetes.io/component", "swarm"))
	g.Expect(observed.Spec.VolumeClaimTemplates).To(HaveLen(1))
	g.Expect(observed.Spec.VolumeClaimTemplates[0].Name).To(Equal("swarm-storage"))
	g.Expect(observed.Spec.Template.Spec.Containers[0].VolumeMounts).To(ContainElement(corev1.VolumeMount{Name: "swarm-storage", MountPath: "/antflydb"}))
	g.Expect(observed.Spec.Template.Spec.Containers[0].Args[0]).To(ContainSubstring("exec /antfly standalone"))
	g.Expect(observed.Annotations).To(HaveKeyWithValue(annotationStorageEngine, "local"))

	configJSON, err := reconciler.generateCompleteConfig(cluster)
	g.Expect(err).NotTo(HaveOccurred())
	var config map[string]any
	g.Expect(json.Unmarshal([]byte(configJSON), &config)).To(Succeed())
	standaloneConfig, ok := config["metadata"].(map[string]any)
	g.Expect(ok).To(BeTrue())
	orchestrationURLs, ok := standaloneConfig["orchestration_urls"].(map[string]any)
	g.Expect(ok).To(BeTrue())
	g.Expect(orchestrationURLs["1"]).To(Equal("http://test-standalone-swarm.default.svc.cluster.local:8080"))
	g.Expect(reconciler.createPublicAPIService(cluster, true).Spec.Selector).
		To(HaveKeyWithValue("app.kubernetes.io/component", "swarm"))

	currentLayout := &appsv1.StatefulSet{}
	err = client.Get(context.Background(), types.NamespacedName{Name: "test-standalone-standalone", Namespace: "default"}, currentLayout)
	g.Expect(errors.IsNotFound(err)).To(BeTrue())
}

func TestReconcileStandaloneStatefulSetPersistsExtensionPackageStoreOnPVC(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := baseStandaloneControllerCluster()
	client := newHAControllerTestClient(t, s, cluster)
	reconciler := &AntflyClusterReconciler{Client: client, Scheme: s}

	g.Expect(reconciler.reconcileStandaloneStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())
	sts := &appsv1.StatefulSet{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{
		Name: cluster.Name + "-standalone", Namespace: cluster.Namespace,
	}, sts)).To(Succeed())

	g.Expect(sts.Spec.Template.Spec.Containers[0].Env).To(ContainElement(corev1.EnvVar{
		Name:  "ANTFLY_EXTENSION_PACKAGE_STORE",
		Value: "/antflydb/extensions",
	}))
	g.Expect(sts.Spec.Template.Spec.InitContainers).To(HaveLen(1))
	initScript := sts.Spec.Template.Spec.InitContainers[0].Args[0]
	g.Expect(initScript).To(ContainSubstring(`extension_store=/antflydb/extensions`))
	g.Expect(initScript).To(ContainSubstring(`mkdir -p "$extension_store"`))
	g.Expect(initScript).To(ContainSubstring(`chown -R 10001:10001 "$extension_store"`))
	g.Expect(initScript).To(ContainSubstring(`chmod -R ug+rwX "$extension_store"`))
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
	client := newHAControllerTestClient(t, s, cluster)
	reconciler := &AntflyClusterReconciler{Client: client, Scheme: s}

	g.Expect(reconciler.reconcileStandaloneStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())
	sts := &appsv1.StatefulSet{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: "test-standalone-standalone", Namespace: "default"}, sts)).To(Succeed())
	primaryArgs := sts.Spec.Template.Spec.Containers[0].Args[0]
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-primary-log '/antflydb/ha/primary.wal'`))
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-primary-slots '/antflydb/ha/slots'`))
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-primary-node-id 'primary-a'`))
	g.Expect(primaryArgs).To(ContainSubstring(`--ha-seed-capture-root '/antflydb/ha/seed-captures'`))
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
	container := sts.Spec.Template.Spec.Containers[0]
	g.Expect(container.StartupProbe.TimeoutSeconds).To(Equal(int32(10)))
	g.Expect(container.LivenessProbe.TimeoutSeconds).To(Equal(int32(10)))
	g.Expect(container.ReadinessProbe.TimeoutSeconds).To(Equal(int32(10)))

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
	g.Expect(standbyArgs).To(ContainSubstring(`--ha-seed-capture-root '/antflydb/ha/seed-captures'`))
	g.Expect(standbyArgs).To(ContainSubstring(`--ha-fence-wal '/antflydb/custom/fence.wal'`))
	g.Expect(standbyArgs).To(ContainSubstring(`--ha-former-primary-log '/antflydb/custom/former-primary.wal'`))
	g.Expect(standbyArgs).To(ContainSubstring(`--admin-token-env 'CUSTOM_HA_ADMIN_TOKEN'`))
	g.Expect(standbyArgs).NotTo(ContainSubstring(`--ha-retention-max-lag-lsn`))
	g.Expect(standbyArgs).NotTo(ContainSubstring(`--ha-retention-max-retained-bytes`))
	g.Expect(standbyArgs).NotTo(ContainSubstring(`--ha-retention-max-retained-age-ns`))
	g.Expect(standbyArgs).To(ContainSubstring(`--ha-standby-upstream-url 'http://primary.default.svc:8080'`))
	g.Expect(standbyArgs).To(ContainSubstring(`--ha-standby-slot 'standby-a'`))
	g.Expect(standbyArgs).To(ContainSubstring(`--ha-sync-mode 'remote-apply'`))
	g.Expect(standbyArgs).To(ContainSubstring(`--ha-sync-selection 'first'`))
	g.Expect(standbyArgs).To(ContainSubstring(`--ha-sync-required 2`))
	g.Expect(standbyArgs).To(ContainSubstring(`--ha-sync-standby 'standby-a'`))
	g.Expect(standbyArgs).To(ContainSubstring(`--ha-sync-standby 'standby-b'`))
	g.Expect(standbyArgs).To(ContainSubstring(`--ha-sync-failure 'fail-closed'`))
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

func TestReconcileStandaloneStatefulSetStartupGatePrecreatesTargetPVCAndStaysSuspended(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := startupGatedStandaloneControllerCluster(false)
	client := newHAControllerTestClient(t, s, cluster)
	reconciler := &AntflyClusterReconciler{Client: client, Scheme: s}

	g.Expect(reconciler.reconcileStandaloneStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())
	pvc := &corev1.PersistentVolumeClaim{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: "standby-a-data", Namespace: "default"}, pvc)).To(Succeed())
	g.Expect(metav1.GetControllerOf(pvc)).To(BeNil())

	sts := &appsv1.StatefulSet{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: "test-standalone-standalone", Namespace: "default"}, sts)).To(Succeed())
	g.Expect(sts.Spec.Replicas).NotTo(BeNil())
	g.Expect(*sts.Spec.Replicas).To(Equal(int32(0)))
	g.Expect(sts.Spec.VolumeClaimTemplates).To(BeEmpty())
	var storageClaimName string
	for _, volume := range sts.Spec.Template.Spec.Volumes {
		if volume.Name == "standalone-storage" && volume.PersistentVolumeClaim != nil {
			storageClaimName = volume.PersistentVolumeClaim.ClaimName
		}
	}
	g.Expect(storageClaimName).To(Equal("standby-a-data"))
}

func TestLegacyHARuntimeRequiresAuthMigrationBeforeWorkloadReconcile(t *testing.T) {
	cluster := startupGatedStandaloneControllerCluster(false)
	cluster.Spec.HighAvailability.Runtime.AdminTokenEnvVar = ""
	cluster.Spec.HighAvailability.Runtime.AdminTokenSecretRef = nil
	if !haRuntimeNeedsAdminTokenMigration(cluster) {
		t.Fatal("expected legacy HA runtime without an admin token source to require migration")
	}
	cluster.Spec.HighAvailability.Runtime.AdminTokenEnvVar = "ANTFLY_HA_ADMIN_TOKEN"
	if haRuntimeNeedsAdminTokenMigration(cluster) {
		t.Fatal("expected authenticated HA runtime not to require migration")
	}
}

func TestReconcileStandaloneStatefulSetSuspendPolicyAlwaysHoldsZeroWithoutActivationPVC(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := startupGatedStandaloneControllerCluster(false)
	cluster.Spec.HighAvailability.Runtime.StartupGate = &antflyv1.HAStartupGateSpec{
		Policy: antflyv1.HAStartupGatePolicy("Suspend"), RuntimeEligible: false,
	}
	client := newHAControllerTestClient(t, s, cluster)
	reconciler := &AntflyClusterReconciler{Client: client, Scheme: s}

	g.Expect(reconciler.reconcileStandaloneStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())
	sts := &appsv1.StatefulSet{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: "test-standalone-standalone", Namespace: "default"}, sts)).To(Succeed())
	g.Expect(sts.Spec.Replicas).NotTo(BeNil())
	g.Expect(*sts.Spec.Replicas).To(Equal(int32(0)))
	pvc := &corev1.PersistentVolumeClaim{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: "standby-a-data", Namespace: "default"}, pvc)).To(MatchError(ContainSubstring("not found")))
	reconciler.updateHAStartupGateStatus(context.Background(), cluster)
	g.Expect(cluster.Status.HAStatus).NotTo(BeNil())
	g.Expect(cluster.Status.HAStatus.StartupGate).NotTo(BeNil())
	g.Expect(cluster.Status.HAStatus.StartupGate.RuntimeEligible).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.StartupGate.Reason).To(Equal("PolicySuspended"))
	g.Expect(cluster.Status.HAStatus.StartupGate.ActivationReceipt).To(BeNil())

	cluster.Spec.HighAvailability.Runtime.StartupGate.RuntimeEligible = true // malformed/bypassed admission still fails closed
	g.Expect(reconciler.reconcileStandaloneStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: "test-standalone-standalone", Namespace: "default"}, sts)).To(Succeed())
	g.Expect(*sts.Spec.Replicas).To(Equal(int32(0)))
}

func TestReconcileStandaloneStatefulSetSuspendPolicyPreservesExistingStorageTopology(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := startupGatedStandaloneControllerCluster(false)
	cluster.Spec.HighAvailability.Runtime.StartupGate = &antflyv1.HAStartupGateSpec{
		Policy: antflyv1.HAStartupGatePolicySuspend, RuntimeEligible: false,
	}
	one := int32(1)
	existing := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-standalone-standalone", Namespace: "default"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &one,
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
				Name: "standalone-storage", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "activated-generation-pvc"}},
			}}}},
		},
	}
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster, existing).Build()
	reconciler := &AntflyClusterReconciler{Client: client, Scheme: s}

	g.Expect(reconciler.reconcileStandaloneStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())
	observed := &appsv1.StatefulSet{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: existing.Name, Namespace: existing.Namespace}, observed)).To(Succeed())
	g.Expect(observed.Spec.Replicas).NotTo(BeNil())
	g.Expect(*observed.Spec.Replicas).To(Equal(int32(0)))
	g.Expect(observed.Spec.VolumeClaimTemplates).To(BeEmpty())
	g.Expect(observed.Spec.Template.Spec.Volumes).To(ContainElement(corev1.Volume{
		Name: "standalone-storage", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "activated-generation-pvc"}},
	}))
}

func TestReconcileStandaloneStatefulSetStartupGateSuspendsLegacyControllerBeforeClaimHandoff(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := startupGatedStandaloneControllerCluster(false)
	one := int32(1)
	legacy := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-standalone-standalone", Namespace: "default"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &one,
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "standalone-storage"},
			}},
		},
		Status: appsv1.StatefulSetStatus{Replicas: 1, CurrentReplicas: 1, ReadyReplicas: 1},
	}
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster, legacy).Build()
	reconciler := &AntflyClusterReconciler{Client: client, Scheme: s}

	g.Expect(reconciler.reconcileStandaloneStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())
	observed := &appsv1.StatefulSet{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: legacy.Name, Namespace: legacy.Namespace}, observed)).To(Succeed())
	g.Expect(observed.Spec.Replicas).NotTo(BeNil())
	g.Expect(*observed.Spec.Replicas).To(Equal(int32(0)))
	g.Expect(observed.Spec.PersistentVolumeClaimRetentionPolicy).NotTo(BeNil())
	g.Expect(observed.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted).To(Equal(appsv1.RetainPersistentVolumeClaimRetentionPolicyType))
	g.Expect(observed.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled).To(Equal(appsv1.RetainPersistentVolumeClaimRetentionPolicyType))
	g.Expect(observed.Spec.VolumeClaimTemplates).To(HaveLen(1))

	pvc := &corev1.PersistentVolumeClaim{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: "standby-a-data", Namespace: "default"}, pvc)).To(MatchError(ContainSubstring("not found")))
}

func TestReconcileStandaloneStatefulSetStartupGateRequiresExactObservedReceipt(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := startupGatedStandaloneControllerCluster(true)
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "standby-a-data", Namespace: "default", UID: types.UID("pvc-uid-1")},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
		},
	}
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster, pvc).Build()
	reconciler := &AntflyClusterReconciler{Client: client, Scheme: s}

	g.Expect(reconciler.reconcileStandaloneStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())
	sts := &appsv1.StatefulSet{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: "test-standalone-standalone", Namespace: "default"}, sts)).To(Succeed())
	g.Expect(*sts.Spec.Replicas).To(Equal(int32(0)))

	digest := strings.Repeat("a", 64)
	// Decode through the API so the test also proves manifestID is a durable
	// status field rather than an in-memory inference from the generation name.
	observedStatus := &antflyv1.HAStatus{}
	g.Expect(json.Unmarshal([]byte(fmt.Sprintf(`{
		"startupGate": {
			"runtimeEligible": true,
			"activationReceipt": {
				"topologyID": "test-standalone", "topologyGeneration": 3,
				"nodeID": "standby-a", "slotName": "standby-a", "generation": "prod-standby-a-10",
				"manifestID": "manifest-standby-a-10", "targetPVCName": "standby-a-data", "targetPVCUID": "pvc-uid-1",
				"checkpointLSN": 12, "manifestSHA256": %q, "aggregateSHA256": %q, "seedReceiptSHA256": %q,
				"captureReceiptSHA256": %q, "materializedReceiptSHA256": %q, "materializedAggregateSHA256": %q,
				"targetLocalNodeID": 1, "targetReplicaID": 1,
				"generationPath": "live-generations/prod-standby-a-10", "rawGenerationPath": "generations/prod-standby-a-10"
			}
		}
	}`, digest, strings.Repeat("b", 64), strings.Repeat("c", 64), strings.Repeat("d", 64), strings.Repeat("e", 64), strings.Repeat("f", 64))), observedStatus)).To(Succeed())
	cluster.Status.HAStatus = observedStatus
	g.Expect(reconciler.reconcileStandaloneStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: "test-standalone-standalone", Namespace: "default"}, sts)).To(Succeed())
	g.Expect(*sts.Spec.Replicas).To(Equal(int32(1)))
	g.Expect(sts.Spec.Template.Annotations).To(HaveKey("antfly.io/ha-startup-receipt-hash"))
	g.Expect(testHASeedIdentityAnnotations(sts.Spec.Template.Annotations)).To(Equal(map[string]string{
		"antfly.io/ha-seed-role":            "standby-runtime",
		"antfly.io/ha-topology-id":          "test-standalone",
		"antfly.io/ha-topology-generation":  "3",
		"antfly.io/ha-node-id":              "standby-a",
		"antfly.io/ha-slot-name":            "standby-a",
		"antfly.io/ha-seed-generation":      "prod-standby-a-10",
		"antfly.io/ha-seed-manifest-id":     "manifest-standby-a-10",
		"antfly.io/ha-seed-manifest-sha256": digest,
		"antfly.io/ha-seed-target-pvc-name": "standby-a-data",
		"antfly.io/ha-seed-target-pvc-uid":  "pvc-uid-1",
		"antfly.io/ha-seed-checkpoint-lsn":  "12",
	}), "every runtime Pod incarnation must inherit the exact activated seed authority")
	container := sts.Spec.Template.Spec.Containers[0]
	g.Expect(container.Env).To(ContainElement(haPodUIDEnv()[0]))
	runtimeArgs := container.Args[0]
	g.Expect(runtimeArgs).To(ContainSubstring(`--ha-startup-target-root '/antflydb/.antfly-ha/active'`))
	g.Expect(runtimeArgs).To(ContainSubstring(`--ha-startup-topology-id 'test-standalone'`))
	g.Expect(runtimeArgs).To(ContainSubstring(`--ha-startup-topology-generation '3'`))
	g.Expect(runtimeArgs).To(ContainSubstring(`--ha-startup-generation 'prod-standby-a-10'`))
	g.Expect(runtimeArgs).To(ContainSubstring(`--ha-startup-target-pvc-name 'standby-a-data'`))
	g.Expect(runtimeArgs).To(ContainSubstring(`--ha-startup-target-pvc-uid 'pvc-uid-1'`))
	g.Expect(runtimeArgs).To(ContainSubstring(`--ha-startup-seed-receipt-sha256 '` + strings.Repeat("c", 64) + `'`))
	g.Expect(runtimeArgs).To(ContainSubstring(`--ha-startup-capture-receipt-sha256 '` + strings.Repeat("d", 64) + `'`))
	g.Expect(runtimeArgs).To(ContainSubstring(`--ha-startup-materialized-receipt-sha256 '` + strings.Repeat("e", 64) + `'`))
	g.Expect(runtimeArgs).To(ContainSubstring(`--ha-startup-materialized-aggregate-sha256 '` + strings.Repeat("f", 64) + `'`))
	g.Expect(runtimeArgs).To(ContainSubstring(`--ha-startup-target-local-node-id '1'`))
	g.Expect(runtimeArgs).To(ContainSubstring(`--ha-startup-target-replica-id '1'`))
	g.Expect(container.VolumeMounts).To(ContainElement(corev1.VolumeMount{
		Name: "ha-seed-generation", MountPath: "/antflydb/data", SubPath: ".antfly-ha/active/live-generations/prod-standby-a-10",
	}))

	cluster.Status.HAStatus.StartupGate.ActivationReceipt.TargetPVCUID = "stale-pvc-uid"
	g.Expect(reconciler.reconcileStandaloneStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: "test-standalone-standalone", Namespace: "default"}, sts)).To(Succeed())
	g.Expect(*sts.Spec.Replicas).To(Equal(int32(0)))
	cluster.Status.HAStatus.StartupGate.ActivationReceipt.TargetPVCUID = "pvc-uid-1"
	cluster.Status.HAStatus.StartupGate.ActivationReceipt.TopologyGeneration = 2
	g.Expect(reconciler.reconcileStandaloneStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())
	g.Expect(client.Get(context.Background(), types.NamespacedName{Name: "test-standalone-standalone", Namespace: "default"}, sts)).To(Succeed())
	g.Expect(*sts.Spec.Replicas).To(Equal(int32(0)))
}

func TestUpdateHAStartupGateStatusUsesOnlyObservedActivationJobAndPVC(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := startupGatedStandaloneControllerCluster(true)
	digest := strings.Repeat("a", 64)
	cluster.Status.HAStatus = &antflyv1.HAStatus{PlannedActions: []antflyv1.HAPlannedActionStatus{{
		Kind: string(haActionActivateSeedArtifact), StandbyName: "standby-a", SlotName: "standby-a",
		TargetLSN: 10, SeedArtifactGeneration: "prod-standby-a-10", AdminJobName: "activation-job", AdminJobPhase: haAdminJobPhaseSucceeded,
		SeedCaptureReceiptSHA256: strings.Repeat("d", 64), TargetLocalNodeID: 1, TargetReplicaID: 1,
		SeedArtifactReceipt: &antflyv1.HASeedArtifactReceiptStatus{
			FormatVersion: 2, Generation: "prod-standby-a-10", SlotName: "standby-a",
			TopologyID: "test-standalone", TopologyGeneration: 3, NodeID: "standby-a", TargetPVCName: "standby-a-data", TargetPVCUID: "pvc-uid-1",
			ClusterID: 100, TimelineID: 1, Epoch: 1, ManifestID: "prod-standby-a-10",
			BackupLSN: 10, CheckpointLSN: 10, ManifestSHA256: digest, AggregateSHA256: strings.Repeat("b", 64),
			SeedReceiptSHA256: strings.Repeat("c", 64), CaptureReceiptSHA256: strings.Repeat("d", 64),
			MaterializedReceiptSHA256: strings.Repeat("e", 64), MaterializedAggregateSHA256: strings.Repeat("f", 64),
			TargetLocalNodeID: 1, TargetReplicaID: 1,
			GenerationPath: "live-generations/prod-standby-a-10", RawGenerationPath: "generations/prod-standby-a-10",
		},
	}, {
		Kind: string(haActionGCTargetSeedGenerations), StandbyName: "standby-a", SlotName: "standby-a",
		SeedArtifactGeneration: "prod-standby-a-10", AdminJobPhase: haAdminJobPhasePending,
	}}}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "standby-a-data", Namespace: "default", UID: types.UID("pvc-uid-1"),
	}}
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster, pvc).Build()
	reconciler := &AntflyClusterReconciler{Client: client, Scheme: s}

	reconciler.updateHAStartupGateStatus(context.Background(), cluster)
	g.Expect(cluster.Status.HAStatus.StartupGate).NotTo(BeNil())
	g.Expect(cluster.Status.HAStatus.StartupGate.RuntimeEligible).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.StartupGate.Reason).To(Equal("TargetGenerationGCNotObserved"))

	gc := &cluster.Status.HAStatus.PlannedActions[1]
	gc.AdminJobPhase = haAdminJobPhaseSucceeded
	gc.SeedArtifactReceipt = &antflyv1.HASeedArtifactReceiptStatus{
		FormatVersion: 1, ActionKind: "gc_local_seed_generations", Scope: "target_activation",
		Generation: "prod-standby-a-10", SlotName: "standby-a", CheckpointSHA256: strings.Repeat("d", 64),
		RetainedCount: 2,
	}
	reconciler.updateHAStartupGateStatus(context.Background(), cluster)
	g.Expect(cluster.Status.HAStatus.StartupGate.RuntimeEligible).To(BeTrue())
	g.Expect(cluster.Status.HAStatus.StartupGate.ActivationReceipt).NotTo(BeNil())
	g.Expect(cluster.Status.HAStatus.StartupGate.ActivationReceipt.TargetPVCUID).To(Equal("pvc-uid-1"))
	g.Expect(cluster.Status.HAStatus.StartupGate.ActivationReceipt.ManifestSHA256).To(Equal(digest))

	cluster.Spec.HighAvailability.Runtime.StartupGate.RuntimeEligible = false
	reconciler.updateHAStartupGateStatus(context.Background(), cluster)
	g.Expect(cluster.Status.HAStatus.StartupGate.RuntimeEligible).To(BeFalse())
	g.Expect(cluster.Status.HAStatus.StartupGate.Reason).To(Equal("DeclarativelySuspended"))
	g.Expect(cluster.Status.HAStatus.StartupGate.ActivationReceipt).NotTo(BeNil())
}

func TestUpdateHAStartupGateStatusObservesActivationReceiptFromPrimaryCR(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())
	target := startupGatedStandaloneControllerCluster(true)
	target.Name = "antflydb-standby-a"
	target.Status.HAStatus = &antflyv1.HAStatus{}
	digest := strings.Repeat("a", 64)
	primary := target.DeepCopy()
	primary.Name = "antflydb"
	primary.Spec.HighAvailability.Runtime.StartupGate = nil
	primary.Spec.HighAvailability.Runtime.Role = antflyv1.HARuntimeRolePrimary
	primary.Spec.HighAvailability.Runtime.NodeID = "primary-a"
	primary.Status.HAStatus = &antflyv1.HAStatus{PlannedActions: []antflyv1.HAPlannedActionStatus{{
		Kind: string(haActionActivateSeedArtifact), StandbyName: "standby-a", SlotName: "standby-a",
		TargetLSN: 10, SeedArtifactGeneration: "prod-standby-a-10", AdminJobName: "activation-job", AdminJobPhase: haAdminJobPhaseSucceeded,
		SeedCaptureReceiptSHA256: strings.Repeat("d", 64), TargetLocalNodeID: 1, TargetReplicaID: 1,
		SeedArtifactReceipt: &antflyv1.HASeedArtifactReceiptStatus{
			FormatVersion: 2, Generation: "prod-standby-a-10", SlotName: "standby-a", TopologyID: "test-standalone", TopologyGeneration: 3,
			NodeID: "standby-a", TargetPVCName: "standby-a-data", TargetPVCUID: "pvc-uid-1", ClusterID: 100, TimelineID: 1, Epoch: 1,
			ManifestID: "prod-standby-a-10", BackupLSN: 10, CheckpointLSN: 10, ManifestSHA256: digest, AggregateSHA256: strings.Repeat("b", 64),
			SeedReceiptSHA256: strings.Repeat("c", 64), CaptureReceiptSHA256: strings.Repeat("d", 64), MaterializedReceiptSHA256: strings.Repeat("e", 64),
			MaterializedAggregateSHA256: strings.Repeat("f", 64), TargetLocalNodeID: 1, TargetReplicaID: 1,
			GenerationPath: "live-generations/prod-standby-a-10", RawGenerationPath: "generations/prod-standby-a-10",
		},
	}, {
		Kind: string(haActionGCTargetSeedGenerations), StandbyName: "standby-a", SlotName: "standby-a", SeedArtifactGeneration: "prod-standby-a-10",
		AdminJobPhase: haAdminJobPhaseSucceeded, SeedArtifactReceipt: &antflyv1.HASeedArtifactReceiptStatus{
			FormatVersion: 1, ActionKind: "gc_local_seed_generations", Scope: "target_activation", Generation: "prod-standby-a-10",
			SlotName: "standby-a", CheckpointSHA256: strings.Repeat("d", 64), RetainedCount: 2,
		},
	}}}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "standby-a-data", Namespace: "default", UID: types.UID("pvc-uid-1")}}
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(target, primary, pvc).Build()
	reconciler := &AntflyClusterReconciler{Client: client, Scheme: s}
	reconciler.updateHAStartupGateStatus(context.Background(), target)
	g.Expect(target.Status.HAStatus.StartupGate).NotTo(BeNil())
	g.Expect(target.Status.HAStatus.StartupGate.RuntimeEligible).To(BeTrue())
	g.Expect(target.Status.HAStatus.StartupGate.ActivationReceipt).NotTo(BeNil())
	g.Expect(target.Status.HAStatus.StartupGate.ActivationReceipt.TargetPVCUID).To(Equal("pvc-uid-1"))
}

func TestUpdateHAStartupGateStatusSkipsUnrelatedReceiptCollision(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	target := startupGatedStandaloneControllerCluster(true)
	target.Name = "z-standby"
	target.Status.HAStatus = &antflyv1.HAStatus{}
	digest := strings.Repeat("a", 64)
	action := antflyv1.HAPlannedActionStatus{
		Kind: string(haActionActivateSeedArtifact), StandbyName: "standby-a", SlotName: "standby-a",
		TargetLSN: 10, SeedArtifactGeneration: "prod-standby-a-10", AdminJobName: "activation-job", AdminJobPhase: haAdminJobPhaseSucceeded,
		SeedCaptureReceiptSHA256: strings.Repeat("d", 64), TargetLocalNodeID: 1, TargetReplicaID: 1,
		SeedArtifactReceipt: &antflyv1.HASeedArtifactReceiptStatus{
			FormatVersion: 2, Generation: "prod-standby-a-10", SlotName: "standby-a", TopologyID: "test-standalone", TopologyGeneration: 3,
			NodeID: "standby-a", TargetPVCName: "standby-a-data", TargetPVCUID: "pvc-uid-1", ClusterID: 100, TimelineID: 1, Epoch: 1,
			ManifestID: "prod-standby-a-10", BackupLSN: 10, CheckpointLSN: 10, ManifestSHA256: digest, AggregateSHA256: strings.Repeat("b", 64),
			SeedReceiptSHA256: strings.Repeat("c", 64), CaptureReceiptSHA256: strings.Repeat("d", 64), MaterializedReceiptSHA256: strings.Repeat("e", 64),
			MaterializedAggregateSHA256: strings.Repeat("f", 64), TargetLocalNodeID: 1, TargetReplicaID: 1,
			GenerationPath: "live-generations/prod-standby-a-10", RawGenerationPath: "generations/prod-standby-a-10",
		},
	}
	correct := target.DeepCopy()
	correct.Name = "z-primary"
	correct.Spec.HighAvailability.Runtime.StartupGate = nil
	correct.Spec.HighAvailability.Runtime.Role = antflyv1.HARuntimeRolePrimary
	correct.Spec.HighAvailability.Runtime.NodeID = "primary-a"
	correct.Status.HAStatus = &antflyv1.HAStatus{PlannedActions: []antflyv1.HAPlannedActionStatus{action}}
	unrelated := correct.DeepCopy()
	unrelated.Name = "a-unrelated-primary"
	unrelated.Spec.HighAvailability.Identity.ClusterID = 999
	unrelated.Status.HAStatus.PlannedActions[0].SeedArtifactReceipt.ClusterID = 999
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "standby-a-data", Namespace: "default", UID: types.UID("pvc-uid-1")}}
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(target, unrelated, correct, pvc).Build()
	reconciler := &AntflyClusterReconciler{Client: client, Scheme: s}

	reconciler.updateHAStartupGateStatus(context.Background(), target)
	g.Expect(target.Status.HAStatus.StartupGate).NotTo(BeNil())
	g.Expect(target.Status.HAStatus.StartupGate.RuntimeEligible).To(BeTrue())
	g.Expect(target.Status.HAStatus.StartupGate.ActivationReceipt).NotTo(BeNil())
	g.Expect(target.Status.HAStatus.StartupGate.ActivationReceipt.ClusterID).To(Equal(uint64(100)))
}

func startupGatedStandaloneControllerCluster(runtimeEligible bool) *antflyv1.AntflyCluster {
	cluster := baseStandaloneControllerCluster()
	digest := strings.Repeat("a", 64)
	targetOnly := false
	cluster.Spec.HighAvailability = &antflyv1.HighAvailabilitySpec{
		Mode:     antflyv1.HAModeHotStandby,
		Identity: &antflyv1.HAReplicationIdentitySpec{ClusterID: 100, TimelineID: 1, Epoch: 1, CurrentPrimaryID: "primary-a"},
		Runtime: &antflyv1.HARuntimeSpec{
			Role: antflyv1.HARuntimeRoleStandby, NodeID: "standby-a",
			Standby: &antflyv1.HAStandbyRuntimeSpec{UpstreamURL: "http://primary.default.svc:8080", SlotName: "standby-a"},
			StartupGate: &antflyv1.HAStartupGateSpec{
				Policy:             antflyv1.HAStartupGatePolicyRequireActivatedSeed,
				RuntimeEligible:    runtimeEligible,
				ReceiptMatchPolicy: antflyv1.HAReceiptMatchPolicyExact,
				RequiredReceipt: &antflyv1.HARequiredSeedActivationReceipt{
					TopologyID: "test-standalone", TopologyGeneration: 3, NodeID: "standby-a", SlotName: "standby-a",
					Generation: "prod-standby-a-10", TargetPVCName: "standby-a-data", ManifestSHA256: digest,
				},
			},
		},
		Standbys: []antflyv1.HAStandbySpec{{
			Name: "standby-a", Desired: &targetOnly,
			SeedArtifact: &antflyv1.HASeedArtifactSpec{
				Location: "s3://ha-seeds/test-standalone", Generation: "prod-standby-a-10",
				StagingRoot: "/target/.antfly-ha/staging",
				TargetPVC:   &antflyv1.HASeedArtifactPVCSpec{ClaimName: "standby-a-data", MountPath: "/target"},
			},
		}},
	}
	return cluster
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
	client := newHAControllerTestClient(t, s, cluster)
	reconciler := &AntflyClusterReconciler{
		Client: client,
		Scheme: s,
	}

	g.Expect(reconciler.reconcileMetadataStatefulSet(context.Background(), &envFromCache{}, cluster, internalServiceAuthRolloutEnforce, internalServiceAuthKeyRolloutSteady)).To(Succeed())
	g.Expect(reconciler.reconcileDataStatefulSet(context.Background(), &envFromCache{}, cluster, internalServiceAuthRolloutEnforce, internalServiceAuthKeyRolloutSteady)).To(Succeed())

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
		for _, env := range container.Env {
			g.Expect(env.Name).NotTo(Equal("ANTFLY_EXTENSION_PACKAGE_STORE"))
		}
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

func TestDataStatefulSetAdvertisesStablePodDNS(t *testing.T) {
	g := NewWithT(t)

	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "stable-routes", Namespace: "antfly-system"},
		Spec: antflyv1.AntflyClusterSpec{
			Image: "antfly:latest",
			DataNodes: antflyv1.DataNodesSpec{
				Replicas: 3,
				API:      antflyv1.APISpec{Port: 12380},
				Raft:     antflyv1.APISpec{Port: 9021},
				Health:   antflyv1.APISpec{Port: 4200},
			},
			Storage: antflyv1.StorageSpec{StorageClass: "standard", DataStorage: "1Gi"},
			Config:  "{}",
		},
	}
	client := newHAControllerTestClient(t, s, cluster)
	reconciler := &AntflyClusterReconciler{Client: client, Scheme: s, ClusterDomain: "corp.internal"}

	g.Expect(reconciler.reconcileDataStatefulSet(context.Background(), &envFromCache{}, cluster, internalServiceAuthRolloutEnforce, internalServiceAuthKeyRolloutSteady)).To(Succeed())

	sts := &appsv1.StatefulSet{}
	g.Expect(client.Get(context.Background(), types.NamespacedName{
		Name: cluster.Name + "-data", Namespace: cluster.Namespace,
	}, sts)).To(Succeed())
	g.Expect(sts.Spec.ServiceName).To(Equal("stable-routes-data"))
	args := sts.Spec.Template.Spec.Containers[0].Args[0]
	g.Expect(args).To(ContainSubstring("--api-host ${POD_IP}"))
	g.Expect(args).To(ContainSubstring("--raft-host ${POD_IP}"))
	g.Expect(args).To(ContainSubstring("--api-advertise-url http://${HOSTNAME}.stable-routes-data.antfly-system.svc.corp.internal:12380"))
	g.Expect(args).To(ContainSubstring("--raft-advertise-url http://${HOSTNAME}.stable-routes-data.antfly-system.svc.corp.internal:9021"))
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

	g.Expect(reconciler.reconcileMetadataStatefulSet(context.Background(), &envFromCache{}, cluster, internalServiceAuthRolloutEnforce, internalServiceAuthKeyRolloutSteady)).To(Succeed())

	sts := &appsv1.StatefulSet{}
	key := types.NamespacedName{Name: cluster.Name + "-metadata", Namespace: cluster.Namespace}
	g.Expect(client.Get(context.Background(), key, sts)).To(Succeed())
	g.Expect(sts.Spec.Template.Labels).To(HaveKeyWithValue("cloud.antfly.io/instance-id", "instance-before"))

	cluster.Labels = map[string]string{
		"cloud.antfly.io/instance-id": "instance-after",
		"cloud.antfly.io/org-id":      "org-123",
	}

	g.Expect(reconciler.reconcileMetadataStatefulSet(context.Background(), &envFromCache{}, cluster, internalServiceAuthRolloutEnforce, internalServiceAuthKeyRolloutSteady)).To(Succeed())
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

	g.Expect(reconciler.reconcileMetadataStatefulSet(context.Background(), &envFromCache{}, cluster, internalServiceAuthRolloutEnforce, internalServiceAuthKeyRolloutSteady)).To(Succeed())

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
