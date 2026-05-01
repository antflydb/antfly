package controllers

import (
	"context"
	"testing"

	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	antflyaiv1alpha1 "github.com/antflydb/antfly/pkg/operator/termite/api/v1alpha1"
)

func TestTermitePoolPodLabels(t *testing.T) {
	g := NewWithT(t)

	pool := &antflyaiv1alpha1.TermitePool{
		ObjectMeta: metav1.ObjectMeta{
			Name: "my-pool",
			Labels: map[string]string{
				"cloud.antfly.io/purpose":        "cloud-instance",
				"cloud.antfly.io/instance-id":    "instance-123",
				"app.kubernetes.io/managed-by":   "external-controller",
				"app.kubernetes.io/part-of":      "cloudaf",
				"antfly.io/pool":                 "wrong-pool",
				"antfly.io/workload-type":        "wrong-type",
				"kubernetes.io/metadata.name":    "default",
				"operator.antfly.io/owned-label": "true",
			},
		},
		Spec: antflyaiv1alpha1.TermitePoolSpec{
			WorkloadType: antflyaiv1alpha1.WorkloadTypeGeneral,
		},
	}

	labels := (&TermitePoolReconciler{}).podLabels(pool)

	g.Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/name", "termite"))
	g.Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/component", "termite-pool"))
	g.Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/instance", "my-pool"))
	g.Expect(labels).To(HaveKeyWithValue("antfly.io/pool", "my-pool"))
	g.Expect(labels).To(HaveKeyWithValue("antfly.io/workload-type", "general"))
	g.Expect(labels).To(HaveKeyWithValue("cloud.antfly.io/purpose", "cloud-instance"))
	g.Expect(labels).To(HaveKeyWithValue("cloud.antfly.io/instance-id", "instance-123"))
	g.Expect(labels).To(HaveKeyWithValue("kubernetes.io/metadata.name", "default"))
	g.Expect(labels).To(HaveKeyWithValue("operator.antfly.io/owned-label", "true"))
	g.Expect(labels).NotTo(HaveKey("app.kubernetes.io/part-of"))
	g.Expect(labels).NotTo(HaveKeyWithValue("app.kubernetes.io/managed-by", "external-controller"))
	g.Expect(labels).NotTo(HaveKeyWithValue("antfly.io/pool", "wrong-pool"))
	g.Expect(labels).NotTo(HaveKeyWithValue("antfly.io/workload-type", "wrong-type"))
}

func TestTermitePoolPodTemplateLabelsUpdateWhenPoolLabelsChange(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()

	s := runtime.NewScheme()
	g.Expect(antflyaiv1alpha1.AddToScheme(s)).To(Succeed())
	g.Expect(appsv1.AddToScheme(s)).To(Succeed())
	g.Expect(corev1.AddToScheme(s)).To(Succeed())

	pool := &antflyaiv1alpha1.TermitePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "label-update-pool",
			Namespace: "default",
			Labels: map[string]string{
				"cloud.antfly.io/instance-id": "instance-before",
			},
		},
		Spec: antflyaiv1alpha1.TermitePoolSpec{
			WorkloadType: antflyaiv1alpha1.WorkloadTypeGeneral,
			Models: antflyaiv1alpha1.ModelConfig{
				Preload:         []antflyaiv1alpha1.ModelSpec{{Name: "test-model"}},
				LoadingStrategy: antflyaiv1alpha1.LoadingStrategyEager,
			},
			Replicas: antflyaiv1alpha1.ReplicaConfig{
				Min: 1,
				Max: 3,
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(pool).
		Build()
	reconciler := &TermitePoolReconciler{
		Client:      client,
		Scheme:      s,
		AntflyImage: "antfly/antfly:omni-test",
	}

	g.Expect(reconciler.reconcileConfigMap(ctx, pool)).To(Succeed())
	g.Expect(reconciler.reconcileStatefulSet(ctx, pool)).To(Succeed())

	sts := &appsv1.StatefulSet{}
	key := types.NamespacedName{Name: pool.Name, Namespace: pool.Namespace}
	g.Expect(client.Get(ctx, key, sts)).To(Succeed())
	initialHash := sts.Spec.Template.Annotations["termite.antfly.io/template-hash"]
	g.Expect(initialHash).NotTo(BeEmpty())
	g.Expect(sts.Spec.Template.Labels).To(HaveKeyWithValue("cloud.antfly.io/instance-id", "instance-before"))

	pool.Labels = map[string]string{
		"cloud.antfly.io/instance-id": "instance-after",
		"cloud.antfly.io/org-id":      "org-123",
	}

	g.Expect(reconciler.reconcileStatefulSet(ctx, pool)).To(Succeed())
	g.Expect(client.Get(ctx, key, sts)).To(Succeed())
	g.Expect(sts.Spec.Template.Labels).To(HaveKeyWithValue("cloud.antfly.io/instance-id", "instance-after"))
	g.Expect(sts.Spec.Template.Labels).To(HaveKeyWithValue("cloud.antfly.io/org-id", "org-123"))
	g.Expect(sts.Spec.Template.Annotations["termite.antfly.io/template-hash"]).NotTo(Equal(initialHash))
}
