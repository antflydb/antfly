package proxy

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestEndpointSliceDeleteUsesDiscoveredPort(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{RefreshInterval: time.Minute})
	w := &K8sWatcher{proxy: p}

	p.RegisterEndpoint("http://10.0.0.1:8080", "pool-a", WorkloadTypeGeneral)

	port := int32(8080)
	w.onEndpointSliceDelete(&discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"kubernetes.io/service-name": "inference-pool-a"},
		},
		Ports: []discoveryv1.EndpointPort{
			{Name: strPtr("http"), Port: &port},
		},
		Endpoints: []discoveryv1.Endpoint{
			{Addresses: []string{"10.0.0.1"}},
		},
	})

	if endpoints := p.Registry().GetEndpointsForPool("pool-a"); len(endpoints) != 0 {
		t.Fatalf("expected endpoint to be removed, got %d endpoints", len(endpoints))
	}
}

func TestK8sWatcherActivatesScaleToZeroPool(t *testing.T) {
	t.Parallel()

	pool := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "antfly.io/v1alpha1",
		"kind":       "InferencePool",
		"metadata": map[string]any{
			"name":      "gpu",
			"namespace": "inference",
		},
		"spec": map[string]any{
			"scaleToZero": map[string]any{
				"enabled":           true,
				"idleTimeout":       "10m",
				"activationTimeout": "3m",
			},
		},
	}}
	pool.SetGroupVersionKind(schema.GroupVersionKind{Group: "antfly.io", Version: "v1alpha1", Kind: "InferencePool"})
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), pool)
	w := &K8sWatcher{dynamicClient: client, scalePools: make(map[string]scaleToZeroPool)}
	w.processInferencePool(pool)

	wait, enabled, err := w.Activate(context.Background(), "gpu")
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if !enabled || wait != 3*time.Minute {
		t.Fatalf("Activate = (%s, %t), want (3m, true)", wait, enabled)
	}

	updated, err := client.Resource(InferencePoolGVR).Namespace("inference").Get(context.Background(), "gpu", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get patched pool: %v", err)
	}
	value := updated.GetAnnotations()[activationRequestedAtAnnotation]
	if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
		t.Fatalf("activation annotation %q is not RFC3339: %v", value, err)
	}
}

func TestPodDeleteUsesContainerPort(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{RefreshInterval: time.Minute})
	w := &K8sWatcher{proxy: p}

	p.RegisterEndpoint("http://10.0.0.2:9090", "pool-b", WorkloadTypeGeneral)

	w.onPodDelete(&corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "inference",
					Ports: []corev1.ContainerPort{
						{Name: "http", ContainerPort: 9090},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.2",
		},
	})

	if endpoints := p.Registry().GetEndpointsForPool("pool-b"); len(endpoints) != 0 {
		t.Fatalf("expected endpoint to be removed, got %d endpoints", len(endpoints))
	}
}

func strPtr(s string) *string {
	return &s
}
