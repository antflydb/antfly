package controllers

import (
	"context"
	"strings"
	"testing"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestValidateClusterConfigurationRejectsHotStandbyWhenFeatureGateDisabled(t *testing.T) {
	enabled := false
	reconciler := &AntflyClusterReconciler{EnableHotStandbyHA: &enabled}
	cluster := &antflyv1.AntflyCluster{
		Spec: antflyv1.AntflyClusterSpec{
			HighAvailability: &antflyv1.HighAvailabilitySpec{Mode: antflyv1.HAModeHotStandby},
		},
	}

	err := reconciler.validateClusterConfiguration(cluster)
	if err == nil || !strings.Contains(err.Error(), "--enable-hot-standby-ha=true") {
		t.Fatalf("expected disabled hot-standby feature-gate error, got %v", err)
	}
}

func TestHotStandbyFeatureGateKeepsEmbeddedConstructorsBackwardCompatible(t *testing.T) {
	if !(&AntflyClusterReconciler{}).hotStandbyHAEnabled() {
		t.Fatal("nil feature gate should preserve existing embedded controller behavior")
	}
	enabled := true
	if !(&AntflyClusterReconciler{EnableHotStandbyHA: &enabled}).hotStandbyHAEnabled() {
		t.Fatal("explicitly enabled feature gate should allow hot-standby HA")
	}
}

func TestReconcileStopsBeforeTopologyMutationWhenHotStandbyFeatureGateDisabled(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := antflyv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "gated-ha", Namespace: "default"},
		Spec: antflyv1.AntflyClusterSpec{
			Mode:             antflyv1.ClusterModeStandalone,
			HighAvailability: &antflyv1.HighAvailabilitySpec{Mode: antflyv1.HAModeHotStandby},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(cluster).Build()
	enabled := false
	reconciler := &AntflyClusterReconciler{
		Client: client, Scheme: scheme, Recorder: events.NewFakeRecorder(1), EnableHotStandbyHA: &enabled,
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{
		Name: cluster.Name, Namespace: cluster.Namespace,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("expected gated HA reconciliation to back off, got %+v", result)
	}

	updated := &antflyv1.AntflyCluster{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, updated); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(updated.Status.Conditions, antflyv1.TypeConfigurationValid)
	if condition == nil || condition.Status != metav1.ConditionFalse || !strings.Contains(condition.Message, "--enable-hot-standby-ha=true") {
		t.Fatalf("expected feature-gate validation condition, got %+v", condition)
	}
}
