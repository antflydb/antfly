// Copyright 2025 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package proxy implements Kubernetes integration for the Termite proxy.
package proxy

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

// K8sWatcher watches Kubernetes endpoints for Termite pods
type K8sWatcher struct {
	proxy     *Proxy
	clientset *kubernetes.Clientset
	namespace string

	// Label selector for Termite pods
	labelSelector labels.Selector
}

// K8sWatcherConfig holds configuration for the K8s watcher
type K8sWatcherConfig struct {
	Kubeconfig    string
	Namespace     string
	LabelSelector string // e.g., "app.kubernetes.io/name=termite"
}

// NewK8sWatcher creates a new Kubernetes watcher
func NewK8sWatcher(proxy *Proxy, cfg K8sWatcherConfig) (*K8sWatcher, error) {
	var config *rest.Config
	var err error

	if cfg.Kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
	} else {
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to build kubeconfig: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create clientset: %w", err)
	}

	selector, err := labels.Parse(cfg.LabelSelector)
	if err != nil {
		return nil, fmt.Errorf("failed to parse label selector: %w", err)
	}

	return &K8sWatcher{
		proxy:         proxy,
		clientset:     clientset,
		namespace:     cfg.Namespace,
		labelSelector: selector,
	}, nil
}

// Start begins watching Kubernetes endpoints
func (w *K8sWatcher) Start(ctx context.Context) error {
	var factory informers.SharedInformerFactory
	if w.namespace != "" {
		factory = informers.NewSharedInformerFactoryWithOptions(
			w.clientset,
			30*time.Second,
			informers.WithNamespace(w.namespace),
		)
	} else {
		factory = informers.NewSharedInformerFactory(w.clientset, 30*time.Second)
	}

	// Watch EndpointSlices (discovery.k8s.io/v1) for service discovery
	endpointSliceInformer := factory.Discovery().V1().EndpointSlices().Informer()
	_, err := endpointSliceInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    w.onEndpointSliceAdd,
		UpdateFunc: w.onEndpointSliceUpdate,
		DeleteFunc: w.onEndpointSliceDelete,
	})
	if err != nil {
		return fmt.Errorf("failed to add endpointslice handler: %w", err)
	}

	// Watch Pods directly for more detailed info
	podsInformer := factory.Core().V1().Pods().Informer()
	_, err = podsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    w.onPodAdd,
		UpdateFunc: w.onPodUpdate,
		DeleteFunc: w.onPodDelete,
	})
	if err != nil {
		return fmt.Errorf("failed to add pods handler: %w", err)
	}

	factory.Start(ctx.Done())

	// Wait for cache sync
	if !cache.WaitForCacheSync(ctx.Done(), endpointSliceInformer.HasSynced, podsInformer.HasSynced) {
		return fmt.Errorf("failed to sync caches")
	}

	<-ctx.Done()
	return nil
}

func (w *K8sWatcher) onEndpointSliceAdd(obj any) {
	endpointSlice := obj.(*discoveryv1.EndpointSlice)
	w.processEndpointSlice(endpointSlice)
}

func (w *K8sWatcher) onEndpointSliceUpdate(oldObj, newObj any) {
	endpointSlice := newObj.(*discoveryv1.EndpointSlice)
	w.processEndpointSlice(endpointSlice)
}

func (w *K8sWatcher) onEndpointSliceDelete(obj any) {
	endpointSlice := obj.(*discoveryv1.EndpointSlice)
	// Remove all addresses from this EndpointSlice
	for _, endpoint := range endpointSlice.Endpoints {
		for _, addr := range endpoint.Addresses {
			address := fmt.Sprintf("http://%s:11433", addr)
			w.proxy.UnregisterEndpoint(address)
		}
	}
}

func (w *K8sWatcher) processEndpointSlice(endpointSlice *discoveryv1.EndpointSlice) {
	// Get the service name from the kubernetes.io/service-name label
	serviceName := endpointSlice.Labels["kubernetes.io/service-name"]

	// Check if this is a Termite service
	if !strings.HasPrefix(serviceName, "termite-") && endpointSlice.Labels["app.kubernetes.io/name"] != "termite" {
		return
	}

	// Get pool name from service name or labels
	pool := endpointSlice.Labels["antfly.io/pool"]
	if pool == "" {
		pool = strings.TrimPrefix(serviceName, "termite-")
	}

	// Get workload type from labels
	workloadTypeStr := endpointSlice.Labels["antfly.io/workload-type"]
	workloadType := WorkloadType(workloadTypeStr)
	if workloadType == "" {
		workloadType = WorkloadTypeGeneral
	}

	// Get port from EndpointSlice ports
	port := 11433
	for _, p := range endpointSlice.Ports {
		if p.Name != nil && (*p.Name == "http" || *p.Name == "api") {
			if p.Port != nil {
				port = int(*p.Port)
			}
			break
		}
	}

	// Process all endpoints in the slice
	for _, endpoint := range endpointSlice.Endpoints {
		// Check if endpoint is ready
		ready := endpoint.Conditions.Ready != nil && *endpoint.Conditions.Ready

		for _, addr := range endpoint.Addresses {
			address := fmt.Sprintf("http://%s:%d", addr, port)

			if ready {
				w.proxy.RegisterEndpoint(address, pool, workloadType)
			} else {
				w.proxy.UnregisterEndpoint(address)
			}
		}
	}
}

func (w *K8sWatcher) onPodAdd(obj any) {
	pod := obj.(*corev1.Pod)
	w.processPod(pod)
}

func (w *K8sWatcher) onPodUpdate(oldObj, newObj any) {
	pod := newObj.(*corev1.Pod)
	w.processPod(pod)
}

func (w *K8sWatcher) onPodDelete(obj any) {
	pod := obj.(*corev1.Pod)
	if pod.Status.PodIP != "" {
		address := fmt.Sprintf("http://%s:11433", pod.Status.PodIP)
		w.proxy.UnregisterEndpoint(address)
	}
}

func (w *K8sWatcher) processPod(pod *corev1.Pod) {
	// Check if pod matches our selector
	if !w.labelSelector.Matches(labels.Set(pod.Labels)) {
		return
	}

	// Only process ready pods
	if pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
		return
	}

	ready := false
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			ready = true
			break
		}
	}

	pool := pod.Labels["antfly.io/pool"]
	workloadType := WorkloadType(pod.Labels["antfly.io/workload-type"])
	if workloadType == "" {
		workloadType = WorkloadTypeGeneral
	}

	// Get port from container spec
	port := 11433
	for _, container := range pod.Spec.Containers {
		if container.Name == "termite" {
			for _, p := range container.Ports {
				if p.Name == "http" || p.Name == "api" {
					port = int(p.ContainerPort)
					break
				}
			}
		}
	}

	address := fmt.Sprintf("http://%s:%d", pod.Status.PodIP, port)

	if ready {
		w.proxy.RegisterEndpoint(address, pool, workloadType)
	} else {
		w.proxy.UnregisterEndpoint(address)
	}
}
