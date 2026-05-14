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
	"log"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	defaultTermiteAPIPort    int64 = 11433
	defaultTermiteHealthPort int64 = 4200
)

// ExternalTermitePoolGVR is the GroupVersionResource for ExternalTermitePool.
var ExternalTermitePoolGVR = schema.GroupVersionResource{
	Group:    "antfly.io",
	Version:  "v1alpha1",
	Resource: "externaltermitepools",
}

// K8sWatcher watches Kubernetes endpoints for Termite pods
type K8sWatcher struct {
	proxy         *Proxy
	clientset     *kubernetes.Clientset
	dynamicClient dynamic.Interface
	namespace     string

	// Label selector for Termite pods
	labelSelector labels.Selector

	externalMu    sync.Mutex
	externalAddrs map[string][]string
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

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	selector, err := labels.Parse(cfg.LabelSelector)
	if err != nil {
		return nil, fmt.Errorf("failed to parse label selector: %w", err)
	}

	return &K8sWatcher{
		proxy:         proxy,
		clientset:     clientset,
		dynamicClient: dynamicClient,
		namespace:     cfg.Namespace,
		labelSelector: selector,
		externalAddrs: make(map[string][]string),
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

	externalFactory, externalPoolInformer, err := w.externalPoolInformer()
	if err != nil {
		log.Printf("termite proxy: external termite pool watch disabled: %v", err)
	}

	factory.Start(ctx.Done())
	if externalFactory != nil {
		externalFactory.Start(ctx.Done())
		go w.waitForOptionalExternalPoolSync(ctx, externalPoolInformer)
	}

	// Wait for cache sync
	if !cache.WaitForCacheSync(ctx.Done(), endpointSliceInformer.HasSynced, podsInformer.HasSynced) {
		return fmt.Errorf("failed to sync caches")
	}

	<-ctx.Done()
	return nil
}

func (w *K8sWatcher) externalPoolInformer() (dynamicinformer.DynamicSharedInformerFactory, cache.SharedIndexInformer, error) {
	var factory dynamicinformer.DynamicSharedInformerFactory
	if w.namespace != "" {
		factory = dynamicinformer.NewFilteredDynamicSharedInformerFactory(
			w.dynamicClient,
			30*time.Second,
			w.namespace,
			nil,
		)
	} else {
		factory = dynamicinformer.NewDynamicSharedInformerFactory(w.dynamicClient, 30*time.Second)
	}

	informer := factory.ForResource(ExternalTermitePoolGVR).Informer()
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    w.onExternalPoolAdd,
		UpdateFunc: w.onExternalPoolUpdate,
		DeleteFunc: w.onExternalPoolDelete,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to add external pool handler: %w", err)
	}
	return factory, informer, nil
}

func (w *K8sWatcher) waitForOptionalExternalPoolSync(ctx context.Context, informer cache.SharedIndexInformer) {
	syncCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if !cache.WaitForCacheSync(syncCtx.Done(), informer.HasSynced) && ctx.Err() == nil {
		log.Printf("termite proxy: external termite pool watch did not sync; continuing without external pool discovery")
	}
}

func (w *K8sWatcher) onExternalPoolAdd(obj any) {
	w.processExternalPool(obj)
}

func (w *K8sWatcher) onExternalPoolUpdate(oldObj, newObj any) {
	w.processExternalPool(newObj)
}

func (w *K8sWatcher) onExternalPoolDelete(obj any) {
	u, ok := unstructuredFromDelete(obj)
	if !ok {
		return
	}
	w.unregisterExternalPool(externalPoolKey(u))
}

func (w *K8sWatcher) processExternalPool(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}

	content := u.UnstructuredContent()
	spec, ok := content["spec"].(map[string]any)
	if !ok {
		return
	}

	key := externalPoolKey(u)
	w.unregisterExternalPool(key)

	workloadType := WorkloadType(getExternalString(spec, "workloadType", string(WorkloadTypeGeneral)))
	if workloadType == "" {
		workloadType = WorkloadTypeGeneral
	}

	models := externalPoolModelNames(spec)
	endpoints, ok := spec["endpoints"].([]any)
	if !ok {
		return
	}

	addresses := make([]string, 0, len(endpoints))
	for _, raw := range endpoints {
		endpoint, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		apiService := strings.TrimSpace(getExternalString(endpoint, "apiServiceRef", ""))
		if apiService == "" {
			continue
		}
		apiPort := getExternalInt(endpoint, "apiPort", defaultTermiteAPIPort)
		healthService := strings.TrimSpace(getExternalString(endpoint, "healthServiceRef", apiService))
		if healthService == "" {
			healthService = apiService
		}
		healthPort := getExternalInt(endpoint, "healthPort", defaultTermiteHealthPort)

		apiURL := serviceURL(apiService, u.GetNamespace(), apiPort)
		healthURL := serviceURL(healthService, u.GetNamespace(), healthPort) + "/readyz"
		w.proxy.RegisterEndpointWithHealth(apiURL, healthURL, u.GetName(), workloadType)
		w.proxy.registry.registerBootstrapModels(apiURL, models)
		addresses = append(addresses, apiURL)
	}

	w.externalMu.Lock()
	w.externalAddrs[key] = addresses
	w.externalMu.Unlock()
}

func (w *K8sWatcher) unregisterExternalPool(key string) {
	w.externalMu.Lock()
	addresses := w.externalAddrs[key]
	delete(w.externalAddrs, key)
	w.externalMu.Unlock()

	for _, address := range addresses {
		w.proxy.UnregisterEndpoint(address)
	}
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
	endpointSlice, ok := endpointSliceFromDelete(obj)
	if !ok {
		return
	}
	port := endpointSlicePort(endpointSlice)
	// Remove all addresses from this EndpointSlice
	for _, endpoint := range endpointSlice.Endpoints {
		for _, addr := range endpoint.Addresses {
			address := fmt.Sprintf("http://%s:%d", addr, port)
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
	port := endpointSlicePort(endpointSlice)

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
	pod, ok := podFromDelete(obj)
	if !ok {
		return
	}
	if pod.Status.PodIP != "" {
		address := fmt.Sprintf("http://%s:%d", pod.Status.PodIP, podPort(pod))
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
	port := podPort(pod)

	address := fmt.Sprintf("http://%s:%d", pod.Status.PodIP, port)

	if ready {
		w.proxy.RegisterEndpoint(address, pool, workloadType)
	} else {
		w.proxy.UnregisterEndpoint(address)
	}
}

func endpointSlicePort(endpointSlice *discoveryv1.EndpointSlice) int {
	port := 11433
	for _, p := range endpointSlice.Ports {
		if p.Name != nil && (*p.Name == "http" || *p.Name == "api") {
			if p.Port != nil {
				port = int(*p.Port)
			}
			break
		}
	}
	return port
}

func podPort(pod *corev1.Pod) int {
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
	return port
}

func unstructuredFromDelete(obj any) (*unstructured.Unstructured, bool) {
	if u, ok := obj.(*unstructured.Unstructured); ok {
		return u, true
	}
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		return nil, false
	}
	u, ok := tombstone.Obj.(*unstructured.Unstructured)
	return u, ok
}

func externalPoolKey(u *unstructured.Unstructured) string {
	return u.GetNamespace() + "/" + u.GetName()
}

func getExternalString(m map[string]any, key, defaultValue string) string {
	value, ok := m[key].(string)
	if !ok || value == "" {
		return defaultValue
	}
	return value
}

func getExternalInt(m map[string]any, key string, defaultValue int64) int64 {
	switch value := m[key].(type) {
	case int64:
		if value > 0 {
			return value
		}
	case int32:
		if value > 0 {
			return int64(value)
		}
	case int:
		if value > 0 {
			return int64(value)
		}
	case float64:
		if value > 0 {
			return int64(value)
		}
	}
	return defaultValue
}

func serviceURL(name, namespace string, port int64) string {
	if namespace == "" {
		return fmt.Sprintf("http://%s:%d", name, port)
	}
	return fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", name, namespace, port)
}

func externalPoolModelNames(spec map[string]any) []string {
	rawModels, ok := spec["models"].([]any)
	if !ok {
		return nil
	}

	names := make([]string, 0, len(rawModels))
	for _, raw := range rawModels {
		model, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		name := strings.TrimSpace(getExternalString(model, "name", ""))
		if name != "" {
			names = append(names, name)
		}
	}
	return names
}

func endpointSliceFromDelete(obj any) (*discoveryv1.EndpointSlice, bool) {
	if endpointSlice, ok := obj.(*discoveryv1.EndpointSlice); ok {
		return endpointSlice, true
	}
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		return nil, false
	}
	endpointSlice, ok := tombstone.Obj.(*discoveryv1.EndpointSlice)
	return endpointSlice, ok
}

func podFromDelete(obj any) (*corev1.Pod, bool) {
	if pod, ok := obj.(*corev1.Pod); ok {
		return pod, true
	}
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		return nil, false
	}
	pod, ok := tombstone.Obj.(*corev1.Pod)
	return pod, ok
}
