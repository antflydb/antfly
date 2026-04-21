package proxy

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestEndpointSliceDeleteUsesDiscoveredPort(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{RefreshInterval: time.Minute})
	w := &K8sWatcher{proxy: p}

	p.RegisterEndpoint("http://10.0.0.1:8080", "pool-a", WorkloadTypeGeneral)

	port := int32(8080)
	w.onEndpointSliceDelete(&discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"kubernetes.io/service-name": "termite-pool-a"},
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

func TestPodDeleteUsesContainerPort(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{RefreshInterval: time.Minute})
	w := &K8sWatcher{proxy: p}

	p.RegisterEndpoint("http://10.0.0.2:9090", "pool-b", WorkloadTypeGeneral)

	w.onPodDelete(&corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "termite",
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
