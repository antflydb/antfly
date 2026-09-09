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
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
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

func TestEndpointSliceRegistrationPreservesNamespaceIdentity(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{RefreshInterval: time.Minute})
	w := &K8sWatcher{proxy: p}
	ready := true
	port := int32(8080)
	for namespace, address := range map[string]string{
		"team-a": "10.0.0.1",
		"team-b": "10.0.0.2",
	} {
		w.processEndpointSlice(&discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Labels:    map[string]string{"kubernetes.io/service-name": "inference-gpu"},
			},
			Ports: []discoveryv1.EndpointPort{{Name: strPtr("http"), Port: &port}},
			Endpoints: []discoveryv1.Endpoint{{
				Addresses:  []string{address},
				Conditions: discoveryv1.EndpointConditions{Ready: &ready},
			}},
		})
	}

	teamA := p.Registry().GetEndpointsForNamespacedPool("team-a", "gpu")
	teamB := p.Registry().GetEndpointsForNamespacedPool("team-b", "gpu")
	if len(teamA) != 1 || teamA[0].Namespace() != "team-a" || teamA[0].Address() != "http://10.0.0.1:8080" {
		t.Fatalf("team-a endpoints = %#v", teamA)
	}
	if len(teamB) != 1 || teamB[0].Namespace() != "team-b" || teamB[0].Address() != "http://10.0.0.2:8080" {
		t.Fatalf("team-b endpoints = %#v", teamB)
	}
	if standalone := p.Registry().GetEndpointsForPool("gpu"); len(standalone) != 0 {
		t.Fatalf("standalone pool lookup crossed namespace boundary: %#v", standalone)
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
			"uid":       "gpu-uid",
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
	clientset := kubernetesfake.NewSimpleClientset()
	w := &K8sWatcher{clientset: clientset, dynamicClient: client, scalePools: make(map[string]scaleToZeroPool)}
	w.processInferencePool(pool)

	wait, enabled, err := w.Activate(context.Background(), "inference", "gpu")
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if !enabled || wait != 3*time.Minute {
		t.Fatalf("Activate = (%s, %t), want (3m, true)", wait, enabled)
	}

	lease, err := clientset.CoordinationV1().Leases("inference").Get(context.Background(), "gpu", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get activation Lease: %v", err)
	}
	if lease.Labels[activationLeasePoolLabel] != "gpu" {
		t.Fatalf("activation Lease labels = %v", lease.Labels)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "gpu-uid" {
		t.Fatalf("activation Lease holder = %v", lease.Spec.HolderIdentity)
	}
	if lease.Spec.LeaseDurationSeconds == nil || *lease.Spec.LeaseDurationSeconds != int32((10*time.Minute)/time.Second) {
		t.Fatalf("activation Lease duration = %v", lease.Spec.LeaseDurationSeconds)
	}
	if lease.Spec.RenewTime == nil {
		t.Fatal("activation Lease has no renewal time")
	}
	if len(lease.OwnerReferences) != 1 || lease.OwnerReferences[0].UID != "gpu-uid" {
		t.Fatalf("activation Lease owner references = %v", lease.OwnerReferences)
	}
}

func TestK8sWatcherRequiresNamespaceForAmbiguousPoolName(t *testing.T) {
	t.Parallel()
	w := &K8sWatcher{scalePools: map[string]scaleToZeroPool{
		"team-a/gpu": {namespace: "team-a"},
		"team-b/gpu": {namespace: "team-b"},
	}}
	if w.IsEnabled("", "gpu") {
		t.Fatal("ambiguous pool name must not resolve without a namespace")
	}
	if !w.IsEnabled("team-b", "gpu") {
		t.Fatal("exact namespaced pool should resolve")
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
