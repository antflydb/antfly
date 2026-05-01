package controllers

import (
	"context"
	stderrors "errors"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	antflyaiv1alpha1 "github.com/antflydb/antfly/pkg/operator/api/termite/v1alpha1"
)

func TestReconcileHPACreatesAutoscaler(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := newTermiteUnitTestScheme(g)
	pool := baseAutoscaledTermitePool()
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(pool).Build()
	reconciler := &TermitePoolReconciler{
		Client: client,
		Scheme: s,
	}

	g.Expect(reconciler.reconcileHPA(ctx, pool)).To(Succeed())

	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	g.Expect(client.Get(ctx, types.NamespacedName{Name: "autoscaled-pool-hpa", Namespace: "default"}, hpa)).To(Succeed())
	g.Expect(hpa.Spec.ScaleTargetRef.APIVersion).To(Equal("apps/v1"))
	g.Expect(hpa.Spec.ScaleTargetRef.Kind).To(Equal("StatefulSet"))
	g.Expect(hpa.Spec.ScaleTargetRef.Name).To(Equal("autoscaled-pool"))
	g.Expect(*hpa.Spec.MinReplicas).To(Equal(int32(1)))
	g.Expect(hpa.Spec.MaxReplicas).To(Equal(int32(5)))
	g.Expect(hpa.Spec.Metrics).To(HaveLen(2))
	g.Expect(hpa.Spec.Metrics[0].Resource.Name).To(Equal(corev1.ResourceCPU))
	g.Expect(*hpa.Spec.Metrics[0].Resource.Target.AverageUtilization).To(Equal(int32(70)))
	g.Expect(hpa.Spec.Metrics[1].Pods.Metric.Name).To(Equal("queue-depth"))
	g.Expect(hpa.Spec.Behavior.ScaleDown.StabilizationWindowSeconds).NotTo(BeNil())
	g.Expect(*hpa.Spec.Behavior.ScaleDown.StabilizationWindowSeconds).To(Equal(int32(300)))
}

func TestReconcileHPADeletesAutoscalerWhenDisabled(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := newTermiteUnitTestScheme(g)
	pool := baseAutoscaledTermitePool()
	pool.Spec.Autoscaling.Enabled = false
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "autoscaled-pool-hpa",
			Namespace:       "default",
			OwnerReferences: []metav1.OwnerReference{ownerReferenceForTermitePool(pool)},
		},
	}
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(pool, hpa).Build()
	reconciler := &TermitePoolReconciler{
		Client: client,
		Scheme: s,
	}

	g.Expect(reconciler.reconcileHPA(ctx, pool)).To(Succeed())

	err := client.Get(ctx, types.NamespacedName{Name: "autoscaled-pool-hpa", Namespace: "default"}, &autoscalingv2.HorizontalPodAutoscaler{})
	g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
}

func TestReconcileHPARefusesToDeleteUnmanagedAutoscaler(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := newTermiteUnitTestScheme(g)
	pool := baseAutoscaledTermitePool()
	pool.Spec.Autoscaling.Enabled = false
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "autoscaled-pool-hpa",
			Namespace: "default",
			UID:       types.UID("unmanaged-hpa"),
		},
	}
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(pool, hpa).Build()
	reconciler := &TermitePoolReconciler{
		Client: client,
		Scheme: s,
	}

	g.Expect(reconciler.reconcileHPA(ctx, pool)).To(Succeed())

	existing := &autoscalingv2.HorizontalPodAutoscaler{}
	g.Expect(client.Get(ctx, types.NamespacedName{Name: "autoscaled-pool-hpa", Namespace: "default"}, existing)).To(Succeed())
	g.Expect(existing.OwnerReferences).To(BeEmpty())
}

func TestReconcileHPARefusesToAdoptUnmanagedAutoscaler(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := newTermiteUnitTestScheme(g)
	pool := baseAutoscaledTermitePool()
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "autoscaled-pool-hpa",
			Namespace: "default",
			UID:       types.UID("unmanaged-hpa"),
		},
	}
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(pool, hpa).Build()
	reconciler := &TermitePoolReconciler{
		Client: client,
		Scheme: s,
	}

	err := reconciler.reconcileHPA(ctx, pool)
	var conflictErr *hpaNameConflictError
	g.Expect(stderrors.As(err, &conflictErr)).To(BeTrue())

	existing := &autoscalingv2.HorizontalPodAutoscaler{}
	g.Expect(client.Get(ctx, types.NamespacedName{Name: "autoscaled-pool-hpa", Namespace: "default"}, existing)).To(Succeed())
	g.Expect(existing.OwnerReferences).To(BeEmpty())
	g.Expect(existing.Spec.ScaleTargetRef.Name).To(BeEmpty())
}

func TestReconcileStatefulSetDoesNotResetReplicasWhenAutoscaled(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	s := newTermiteUnitTestScheme(g)
	pool := baseAutoscaledTermitePool()
	client := fake.NewClientBuilder().WithScheme(s).WithObjects(pool).Build()
	reconciler := &TermitePoolReconciler{
		Client:      client,
		Scheme:      s,
		AntflyImage: "ghcr.io/antflydb/antfly:omni-test",
	}

	g.Expect(reconciler.reconcileConfigMap(ctx, pool)).To(Succeed())
	g.Expect(reconciler.reconcileStatefulSet(ctx, pool)).To(Succeed())

	key := types.NamespacedName{Name: "autoscaled-pool", Namespace: "default"}
	sts := &appsv1.StatefulSet{}
	g.Expect(client.Get(ctx, key, sts)).To(Succeed())
	scaledReplicas := int32(4)
	sts.Spec.Replicas = &scaledReplicas
	g.Expect(client.Update(ctx, sts)).To(Succeed())

	g.Expect(reconciler.reconcileStatefulSet(ctx, pool)).To(Succeed())
	g.Expect(client.Get(ctx, key, sts)).To(Succeed())
	g.Expect(*sts.Spec.Replicas).To(Equal(int32(4)))
}

func newTermiteUnitTestScheme(g *WithT) *runtime.Scheme {
	s := runtime.NewScheme()
	g.Expect(antflyaiv1alpha1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(autoscalingv2.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())
	return s
}

func baseAutoscaledTermitePool() *antflyaiv1alpha1.TermitePool {
	scaleDownSeconds := metav1.Duration{Duration: 5 * time.Minute}
	return &antflyaiv1alpha1.TermitePool{
		TypeMeta: metav1.TypeMeta{
			APIVersion: antflyaiv1alpha1.GroupVersion.String(),
			Kind:       "TermitePool",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "autoscaled-pool",
			Namespace: "default",
			UID:       types.UID("autoscaled-pool"),
		},
		Spec: antflyaiv1alpha1.TermitePoolSpec{
			WorkloadType: antflyaiv1alpha1.WorkloadTypeGeneral,
			Models: antflyaiv1alpha1.ModelConfig{
				Preload:         []antflyaiv1alpha1.ModelSpec{{Name: "test-model"}},
				LoadingStrategy: antflyaiv1alpha1.LoadingStrategyEager,
			},
			Replicas: antflyaiv1alpha1.ReplicaConfig{
				Min: 1,
				Max: 5,
			},
			Hardware: antflyaiv1alpha1.HardwareConfig{},
			Autoscaling: &antflyaiv1alpha1.AutoscalingConfig{
				Enabled:                true,
				ScaleDownStabilization: &scaleDownSeconds,
				Metrics: []antflyaiv1alpha1.ScalingMetric{
					{Type: antflyaiv1alpha1.MetricTypeCPU, Target: "70%"},
					{Type: antflyaiv1alpha1.MetricTypeQueueDepth, Target: "10"},
				},
			},
		},
	}
}

func ownerReferenceForTermitePool(pool *antflyaiv1alpha1.TermitePool) metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{
		APIVersion: antflyaiv1alpha1.GroupVersion.String(),
		Kind:       "TermitePool",
		Name:       pool.Name,
		UID:        pool.UID,
		Controller: &controller,
	}
}
