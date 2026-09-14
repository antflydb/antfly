package controllers

import (
	"context"
	"testing"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	adminsdk "github.com/antflydb/antfly/go/pkg/sdk/admin"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
)

// --- standaloneHAArgs default path rendering under both layouts ---

func TestStandaloneHAArgsRendersLegacyDefaultsForLegacyLayout(t *testing.T) {
	g := NewWithT(t)
	ha := &antflyv1.HighAvailabilitySpec{
		Mode: antflyv1.HAModeHotStandby,
		Identity: &antflyv1.HAReplicationIdentitySpec{
			ClusterID: 100, TimelineID: 1, Epoch: 2, CurrentPrimaryID: "primary-a",
		},
		Runtime: &antflyv1.HARuntimeSpec{Role: antflyv1.HARuntimeRolePrimary, NodeID: "primary-a"},
	}
	args := standaloneHAArgs(ha, "", antflyv1.HADataLayoutLegacy)
	g.Expect(args).To(ContainSubstring(`--ha-primary-log '/antflydb/ha/primary.wal'`))
	g.Expect(args).To(ContainSubstring(`--ha-primary-slots '/antflydb/ha/slots'`))
	g.Expect(args).To(ContainSubstring(`--ha-seed-capture-root '/antflydb/ha/seed-captures'`))
	g.Expect(args).To(ContainSubstring(`--ha-fence-wal '/antflydb/ha/fence.wal'`))
	g.Expect(args).To(ContainSubstring(`--ha-former-primary-log '/antflydb/ha/primary.wal'`))

	ha.Runtime = &antflyv1.HARuntimeSpec{
		Role: antflyv1.HARuntimeRoleStandby, NodeID: "standby-a",
		Standby: &antflyv1.HAStandbyRuntimeSpec{UpstreamURL: "http://primary:8080", SlotName: "standby-a"},
	}
	args = standaloneHAArgs(ha, "", antflyv1.HADataLayoutLegacy)
	g.Expect(args).To(ContainSubstring(`--ha-standby-log '/antflydb/ha/standby.wal'`))
	g.Expect(args).To(ContainSubstring(`--ha-standby-progress '/antflydb/ha/standby-progress.wal'`))

	args = standaloneHAArgs(ha, "gen-1", antflyv1.HADataLayoutLegacy)
	g.Expect(args).To(ContainSubstring(`--ha-standby-log '/antflydb/ha/standby-generations/gen-1/receive.wal'`))
	g.Expect(args).To(ContainSubstring(`--ha-standby-progress '/antflydb/ha/standby-generations/gen-1/progress.wal'`))
}

func TestStandaloneHAArgsRendersStandbyDefaultsForStandbyLayout(t *testing.T) {
	g := NewWithT(t)
	ha := &antflyv1.HighAvailabilitySpec{
		Mode: antflyv1.HAModeHotStandby,
		Identity: &antflyv1.HAReplicationIdentitySpec{
			ClusterID: 100, TimelineID: 1, Epoch: 2, CurrentPrimaryID: "primary-a",
		},
		Runtime: &antflyv1.HARuntimeSpec{Role: antflyv1.HARuntimeRolePrimary, NodeID: "primary-a"},
	}
	args := standaloneHAArgs(ha, "", antflyv1.HADataLayoutStandby)
	g.Expect(args).To(ContainSubstring(`--ha-primary-log '/antflydb/standby/primary.wal'`))
	g.Expect(args).To(ContainSubstring(`--ha-primary-slots '/antflydb/standby/slots'`))
	g.Expect(args).To(ContainSubstring(`--ha-seed-capture-root '/antflydb/standby/seed-captures'`))
	g.Expect(args).To(ContainSubstring(`--ha-fence-wal '/antflydb/standby/fence.wal'`))
	g.Expect(args).To(ContainSubstring(`--ha-former-primary-log '/antflydb/standby/primary.wal'`))
	// The operator keeps emitting the --ha-* flag spellings this release.
	g.Expect(args).NotTo(ContainSubstring(`--hot-standby-`))

	ha.Runtime = &antflyv1.HARuntimeSpec{
		Role: antflyv1.HARuntimeRoleStandby, NodeID: "standby-a",
		Standby: &antflyv1.HAStandbyRuntimeSpec{UpstreamURL: "http://primary:8080", SlotName: "standby-a"},
	}
	args = standaloneHAArgs(ha, "", antflyv1.HADataLayoutStandby)
	g.Expect(args).To(ContainSubstring(`--ha-standby-log '/antflydb/standby/log.wal'`))
	g.Expect(args).To(ContainSubstring(`--ha-standby-progress '/antflydb/standby/progress.wal'`))

	args = standaloneHAArgs(ha, "gen-1", antflyv1.HADataLayoutStandby)
	g.Expect(args).To(ContainSubstring(`--ha-standby-log '/antflydb/standby/standby-generations/gen-1/receive.wal'`))
	g.Expect(args).To(ContainSubstring(`--ha-standby-progress '/antflydb/standby/standby-generations/gen-1/progress.wal'`))
}

func TestStandaloneHAArgsExplicitOverridesWinRegardlessOfLayout(t *testing.T) {
	g := NewWithT(t)
	ha := &antflyv1.HighAvailabilitySpec{
		Mode: antflyv1.HAModeHotStandby,
		Identity: &antflyv1.HAReplicationIdentitySpec{
			ClusterID: 100, TimelineID: 1, Epoch: 2, CurrentPrimaryID: "primary-a",
		},
		Runtime: &antflyv1.HARuntimeSpec{
			Role:   antflyv1.HARuntimeRolePrimary,
			NodeID: "primary-a",
			Primary: &antflyv1.HAPrimaryRuntimeSpec{
				LogPath: "/antflydb/custom/primary.wal", SlotsPath: "/antflydb/custom/slots",
			},
			SeedCaptureRoot: "/antflydb/custom/seed-captures",
			FencePath:       "/antflydb/custom/fence.wal",
		},
	}
	for _, layout := range []antflyv1.HADataLayout{antflyv1.HADataLayoutLegacy, antflyv1.HADataLayoutStandby, ""} {
		args := standaloneHAArgs(ha, "", layout)
		g.Expect(args).To(ContainSubstring(`--ha-primary-log '/antflydb/custom/primary.wal'`), "layout=%q", layout)
		g.Expect(args).To(ContainSubstring(`--ha-primary-slots '/antflydb/custom/slots'`), "layout=%q", layout)
		g.Expect(args).To(ContainSubstring(`--ha-seed-capture-root '/antflydb/custom/seed-captures'`), "layout=%q", layout)
		g.Expect(args).To(ContainSubstring(`--ha-fence-wal '/antflydb/custom/fence.wal'`), "layout=%q", layout)
	}
}

// --- decideHADataLayout / recordHADataLayoutObservation ---

func newHADataLayoutTestCluster() *antflyv1.AntflyCluster {
	return &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-standalone", Namespace: "default"},
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
			},
			Storage: antflyv1.StorageSpec{StorageClass: "standard", StandaloneStorage: "1Gi"},
			Config:  "{}",
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Identity: &antflyv1.HAReplicationIdentitySpec{
					ClusterID: 100, TimelineID: 1, Epoch: 1, CurrentPrimaryID: "primary-a",
				},
				Runtime: &antflyv1.HARuntimeSpec{Role: antflyv1.HARuntimeRolePrimary, NodeID: "primary-a"},
			},
		},
	}
}

func TestDecideHADataLayoutBrandNewClusterDecidesStandbyImmediately(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := newHADataLayoutTestCluster()
	recorder := events.NewFakeRecorder(4)
	testClient := newHAControllerTestClient(t, s, cluster)
	reconciler := &AntflyClusterReconciler{Client: testClient, Scheme: s, Recorder: recorder}

	// No StatefulSet exists yet: this is a brand-new cluster.
	g.Expect(reconciler.reconcileStandaloneStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())

	g.Expect(cluster.Status.HAStatus).NotTo(BeNil())
	g.Expect(cluster.Status.HAStatus.DataLayout).To(Equal(antflyv1.HADataLayoutStandby))

	sts := &appsv1.StatefulSet{}
	g.Expect(testClient.Get(context.Background(), types.NamespacedName{Name: "test-standalone-standalone", Namespace: "default"}, sts)).To(Succeed())
	args := sts.Spec.Template.Spec.Containers[0].Args[0]
	g.Expect(args).To(ContainSubstring(`--ha-primary-log '/antflydb/standby/primary.wal'`))

	select {
	case msg := <-recorder.Events:
		g.Expect(msg).To(ContainSubstring(haDataLayoutStandbyEventReason))
	default:
		t.Fatal("expected a HotStandbyLayoutStandby event for the brand-new cluster decision")
	}
}

func TestDecideHADataLayoutExistingClusterUndecidedRendersLegacy(t *testing.T) {
	g := NewWithT(t)
	s := runtime.NewScheme()
	g.Expect(antflyv1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	cluster := newHADataLayoutTestCluster()
	// Model an existing cluster: its StatefulSet already exists before this
	// reconcile, and status.haStatus.dataLayout is still undecided.
	existingSts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-standalone-standalone", Namespace: "default",
			Annotations: map[string]string{annotationStorageEngine: "local"},
		},
	}
	testClient := newHAControllerTestClient(t, s, cluster, existingSts)
	reconciler := &AntflyClusterReconciler{Client: testClient, Scheme: s}

	g.Expect(reconciler.reconcileStandaloneStatefulSet(context.Background(), &envFromCache{}, cluster)).To(Succeed())

	// decideHADataLayout does not touch status at all for an existing,
	// undecided cluster (it only persists a fresh decision), so HAStatus may
	// still be nil here; either way, no layout has been decided.
	if cluster.Status.HAStatus != nil {
		g.Expect(cluster.Status.HAStatus.DataLayout).To(BeEmpty(), "an existing, undecided cluster must not be marked as either layout")
	}

	sts := &appsv1.StatefulSet{}
	g.Expect(testClient.Get(context.Background(), types.NamespacedName{Name: "test-standalone-standalone", Namespace: "default"}, sts)).To(Succeed())
	args := sts.Spec.Template.Spec.Containers[0].Args[0]
	g.Expect(args).To(ContainSubstring(`--ha-primary-log '/antflydb/ha/primary.wal'`))
}

func TestRecordHADataLayoutObservationFlipsOnceCanonicalPathStylePinned(t *testing.T) {
	g := NewWithT(t)
	t.Setenv(haAdminTokenDefaultEnvVar, "operator-token")
	adminsdk.ResetNegotiatedPathStyles()

	server, _ := pathStyleServer(t, adminsdk.StandbyPath)
	reconciler := &AntflyClusterReconciler{HTTPClient: server.Client(), StandbyAdminPathStyle: adminsdk.PathStyleAuto}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode: antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{
					PrimaryURL:  server.URL,
					TokenEnvVar: haAdminTokenDefaultEnvVar,
				},
			},
		},
	}

	g.Expect(reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.DataLayout).To(Equal(antflyv1.HADataLayoutStandby),
		"a canonical-pinned admin probe must flip the layout to standby")

	// Once flipped, later probes (even against a legacy-only server, or a
	// failure) must never revert the decision.
	legacyServer, _ := pathStyleServer(t, adminsdk.HAPath)
	adminsdk.ResetNegotiatedPathStyles()
	reconciler.HTTPClient = legacyServer.Client()
	cluster.Spec.HighAvailability.Admin.PrimaryURL = legacyServer.URL
	g.Expect(reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.DataLayout).To(Equal(antflyv1.HADataLayoutStandby),
		"a decided standby layout must never revert to ha")
}

func TestRecordHADataLayoutObservationExplicitCanonicalTreatsSuccessAsProof(t *testing.T) {
	g := NewWithT(t)
	t.Setenv(haAdminTokenDefaultEnvVar, "operator-token")

	server, _ := pathStyleServer(t, adminsdk.StandbyPath)
	reconciler := &AntflyClusterReconciler{HTTPClient: server.Client(), StandbyAdminPathStyle: adminsdk.PathStyleCanonical}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode:  antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{PrimaryURL: server.URL, TokenEnvVar: haAdminTokenDefaultEnvVar},
			},
		},
	}

	g.Expect(reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.DataLayout).To(Equal(antflyv1.HADataLayoutStandby))
}

func TestRecordHADataLayoutObservationExplicitLegacyNeverFlips(t *testing.T) {
	g := NewWithT(t)
	t.Setenv(haAdminTokenDefaultEnvVar, "operator-token")

	server, _ := pathStyleServer(t, adminsdk.HAPath)
	reconciler := &AntflyClusterReconciler{HTTPClient: server.Client(), StandbyAdminPathStyle: adminsdk.PathStyleLegacy}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{
				Mode:  antflyv1.HAModeHotStandby,
				Admin: &antflyv1.HAAdminSpec{PrimaryURL: server.URL, TokenEnvVar: haAdminTokenDefaultEnvVar},
			},
		},
	}

	g.Expect(reconciler.observeHAPrimaryAdminStatus(context.Background(), cluster)).To(Succeed())
	g.Expect(cluster.Status.HAStatus.DataLayout).To(BeEmpty(),
		"an operator explicitly pinned to legacy paths must never infer 0.3 migration support")
}
