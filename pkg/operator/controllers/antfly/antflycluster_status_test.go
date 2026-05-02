package controllers

import (
	"testing"

	antflyv1 "github.com/antflydb/antfly/pkg/operator/api/antfly/v1"
	"github.com/antflydb/antfly/pkg/operator/controllers/internal/poddiagnostics"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSetComponentConditionReportsImagePullFailure(t *testing.T) {
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "antfly",
			Namespace:  "default",
			Generation: 4,
		},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.setComponentCondition(cluster, antflyv1.TypeDataReady, 0, 1, []poddiagnostics.Finding{{
		Type:      poddiagnostics.FindingImagePullFailed,
		Pod:       "antfly-data-0",
		Container: "antfly",
		Reason:    "ImagePullBackOff",
		Message:   "failed to pull image",
	}}, "data")

	condition := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeDataReady)
	if condition == nil {
		t.Fatalf("expected %s condition", antflyv1.TypeDataReady)
	}
	if condition.Status != metav1.ConditionFalse {
		t.Fatalf("expected condition false, got %s", condition.Status)
	}
	if condition.Reason != antflyv1.ReasonImagePullFailed {
		t.Fatalf("expected image pull reason, got %s", condition.Reason)
	}
}

func TestSetAvailableConditionClearsWhenReady(t *testing.T) {
	cluster := &antflyv1.AntflyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "antfly",
			Namespace:  "default",
			Generation: 5,
		},
	}
	reconciler := &AntflyClusterReconciler{}

	reconciler.setAvailableCondition(cluster, nil, true)

	condition := meta.FindStatusCondition(cluster.Status.Conditions, antflyv1.TypeAvailable)
	if condition == nil {
		t.Fatalf("expected %s condition", antflyv1.TypeAvailable)
	}
	if condition.Status != metav1.ConditionTrue {
		t.Fatalf("expected Available true, got %s", condition.Status)
	}
}
