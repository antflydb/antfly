// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package proxy

import (
	"testing"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestRouteWatcherIgnoresInformerResyncUpdate(t *testing.T) {
	t.Parallel()

	routes := NewRouteManager()
	routes.UpsertRoute(&Route{Name: "default/reader", Priority: 1})
	generation := routes.Generation()
	watcher := &RouteWatcher{routeManager: routes, logger: zap.NewNop()}
	oldObject := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"namespace": "default", "name": "reader", "resourceVersion": "7"},
	}}
	newObject := oldObject.DeepCopy()

	// A malformed spec makes this test prove the resync guard ran before route
	// conversion; a normal informer resync must be a complete no-op.
	watcher.onRouteUpdate(oldObject, newObject)
	if got := routes.Generation(); got != generation {
		t.Fatalf("generation = %d after informer resync, want %d", got, generation)
	}
}

func TestRouteWatcherRejectsInvalidHeaderRegex(t *testing.T) {
	t.Parallel()
	watcher := &RouteWatcher{routeManager: NewRouteManager(), logger: zap.NewNop()}
	object := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"namespace": "default", "name": "reader"},
		"spec": map[string]any{
			"match": map[string]any{
				"headers": map[string]any{
					"x-tenant": map[string]any{"regex": "["},
				},
			},
		},
	}}
	if _, err := watcher.convertRoute(object); err == nil {
		t.Fatal("invalid header regex was accepted")
	}
}
