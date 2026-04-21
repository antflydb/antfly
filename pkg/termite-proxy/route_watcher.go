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

// Package proxy implements Kubernetes integration for TermiteRoute watching.
package proxy

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

// TermiteRouteGVR is the GroupVersionResource for TermiteRoute
var TermiteRouteGVR = schema.GroupVersionResource{
	Group:    "antfly.io",
	Version:  "v1alpha1",
	Resource: "termiteroutes",
}

// RouteWatcher watches TermiteRoute CRs and updates the RouteManager
type RouteWatcher struct {
	routeManager *RouteManager
	client       dynamic.Interface
	namespace    string // empty for all namespaces
	logger       *zap.Logger
}

// RouteWatcherConfig holds configuration for the route watcher
type RouteWatcherConfig struct {
	Kubeconfig string
	Namespace  string // empty for all namespaces
}

// NewRouteWatcher creates a new TermiteRoute watcher
func NewRouteWatcher(routeManager *RouteManager, cfg RouteWatcherConfig, logger *zap.Logger) (*RouteWatcher, error) {
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

	client, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	if logger == nil {
		logger, _ = zap.NewProduction()
	}

	return &RouteWatcher{
		routeManager: routeManager,
		client:       client,
		namespace:    cfg.Namespace,
		logger:       logger,
	}, nil
}

// Start begins watching TermiteRoute resources
func (w *RouteWatcher) Start(ctx context.Context) error {
	var factory dynamicinformer.DynamicSharedInformerFactory
	if w.namespace != "" {
		factory = dynamicinformer.NewFilteredDynamicSharedInformerFactory(
			w.client,
			30*time.Second,
			w.namespace,
			nil,
		)
	} else {
		factory = dynamicinformer.NewDynamicSharedInformerFactory(w.client, 30*time.Second)
	}

	informer := factory.ForResource(TermiteRouteGVR).Informer()

	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    w.onRouteAdd,
		UpdateFunc: w.onRouteUpdate,
		DeleteFunc: w.onRouteDelete,
	})
	if err != nil {
		return fmt.Errorf("failed to add event handler: %w", err)
	}

	factory.Start(ctx.Done())

	// Wait for cache sync
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return fmt.Errorf("failed to sync TermiteRoute cache")
	}

	w.logger.Info("TermiteRoute watcher started", zap.String("namespace", w.namespace))

	<-ctx.Done()
	return nil
}

func (w *RouteWatcher) onRouteAdd(obj any) {
	route, err := w.convertRoute(obj)
	if err != nil {
		w.logger.Error("failed to convert TermiteRoute", zap.Error(err))
		return
	}

	w.routeManager.AddRoute(route)
	w.logger.Info("added route", zap.String("name", route.Name), zap.Int32("priority", route.Priority))
}

func (w *RouteWatcher) onRouteUpdate(oldObj, newObj any) {
	route, err := w.convertRoute(newObj)
	if err != nil {
		w.logger.Error("failed to convert TermiteRoute", zap.Error(err))
		return
	}

	w.routeManager.AddRoute(route) // AddRoute handles updates by name
	w.logger.Info("updated route", zap.String("name", route.Name), zap.Int32("priority", route.Priority))
}

func (w *RouteWatcher) onRouteDelete(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		w.logger.Error("failed to cast object to Unstructured")
		return
	}

	name := u.GetNamespace() + "/" + u.GetName()
	w.routeManager.RemoveRoute(name)
	w.logger.Info("removed route", zap.String("name", name))
}

// convertRoute converts an unstructured TermiteRoute to the proxy's Route type
func (w *RouteWatcher) convertRoute(obj any) (*Route, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("object is not Unstructured")
	}

	content := u.UnstructuredContent()
	spec, ok := content["spec"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("spec not found")
	}

	// Build the route name with namespace for uniqueness
	namespace := u.GetNamespace()
	name := u.GetName()
	fullName := namespace + "/" + name

	route := &Route{
		Name:           fullName,
		Priority:       getInt32(spec, "priority", 100),
		Operations:     make(map[OperationType]bool),
		ModelPatterns:  make([]*regexp.Regexp, 0),
		HeaderMatchers: make(map[string]*StringMatcher),
		SourceTables:   make(map[string]bool),
		Destinations:   make([]Destination, 0),
	}

	// Parse match conditions
	if match, ok := spec["match"].(map[string]any); ok {
		// Operations
		if ops, ok := match["operations"].([]any); ok {
			for _, op := range ops {
				if opStr, ok := op.(string); ok {
					route.Operations[OperationType(opStr)] = true
				}
			}
		}

		// Model patterns
		if models, ok := match["models"].([]any); ok {
			for _, model := range models {
				if modelStr, ok := model.(string); ok {
					pattern, err := CompileModelPattern(modelStr)
					if err != nil {
						w.logger.Warn("failed to compile model pattern", zap.String("pattern", modelStr), zap.Error(err))
						continue
					}
					route.ModelPatterns = append(route.ModelPatterns, pattern)
				}
			}
		}

		// Headers
		if headers, ok := match["headers"].(map[string]any); ok {
			for headerName, matchSpec := range headers {
				if matchMap, ok := matchSpec.(map[string]any); ok {
					matcher := &StringMatcher{}
					if exact, ok := matchMap["exact"].(string); ok {
						matcher.Exact = exact
					}
					if prefix, ok := matchMap["prefix"].(string); ok {
						matcher.Prefix = prefix
					}
					if regexStr, ok := matchMap["regex"].(string); ok {
						if regex, err := regexp.Compile(regexStr); err == nil {
							matcher.Regex = regex
						}
					}
					route.HeaderMatchers[headerName] = matcher
				}
			}
		}

		// Source tables
		if source, ok := match["source"].(map[string]any); ok {
			if tables, ok := source["tables"].([]any); ok {
				for _, table := range tables {
					if tableStr, ok := table.(string); ok {
						route.SourceTables[tableStr] = true
					}
				}
			}
		}

		// Time window
		if tw, ok := match["timeWindow"].(map[string]any); ok {
			route.TimeWindow = parseTimeWindow(tw)
		}
	}

	// Parse destinations
	if destinations, ok := spec["route"].([]any); ok {
		for _, destObj := range destinations {
			if destMap, ok := destObj.(map[string]any); ok {
				dest := Destination{
					Pool:   getString(destMap, "pool"),
					Weight: getInt32(destMap, "weight", 100),
				}

				// Parse condition
				if condition, ok := destMap["condition"].(map[string]any); ok {
					if qd, ok := condition["queueDepth"].(string); ok {
						if cond, err := ParseThresholdCondition(qd); err == nil {
							dest.QueueDepthCondition = cond
						}
					}
					if ar, ok := condition["availableReplicas"].(string); ok {
						if cond, err := ParseThresholdCondition(ar); err == nil {
							dest.ReplicaCondition = cond
						}
					}
					if lat, ok := condition["latency"].(string); ok {
						if cond, err := ParseThresholdCondition(lat); err == nil {
							dest.LatencyCondition = cond
						}
					}
					if ml, ok := condition["modelLoaded"].(bool); ok && ml {
						dest.RequireModelLoaded = true
					}
					if tod, ok := condition["timeOfDay"].(map[string]any); ok {
						dest.TimeCondition = parseTimeWindow(tod)
					}
				}

				route.Destinations = append(route.Destinations, dest)
			}
		}
	}

	// Parse fallback
	if fallback, ok := spec["fallback"].(map[string]any); ok {
		route.Fallback = &Fallback{
			Action: getString(fallback, "action"),
		}
		if errResp, ok := fallback["errorResponse"].(map[string]any); ok {
			route.Fallback.StatusCode = int(getInt32(errResp, "statusCode", 503))
			route.Fallback.Message = getString(errResp, "message")
			if ra, ok := errResp["retryAfter"].(float64); ok {
				route.Fallback.RetryAfter = int(ra)
			}
		}
		if rp := getString(fallback, "redirectPool"); rp != "" {
			route.Fallback.RedirectPool = rp
		}
	}

	// Parse rate limiting
	if rl, ok := spec["rateLimiting"].(map[string]any); ok {
		rps := getInt32(rl, "requestsPerSecond", 0)
		burst := getInt32(rl, "burstSize", rps)
		perModel, _ := rl["perModel"].(bool)
		if rps > 0 {
			route.RateLimiter = NewRateLimiter(rps, burst, perModel)
		}
	}

	// Parse retry config
	if retry, ok := spec["retry"].(map[string]any); ok {
		route.RetryAttempts = getInt32(retry, "attempts", 3)

		if retryOn, ok := retry["retryOn"].([]any); ok {
			route.RetryOnStatuses = make(map[int]bool)
			for _, r := range retryOn {
				if rs, ok := r.(string); ok {
					// Handle "5xx" pattern
					if before, ok0 := strings.CutSuffix(rs, "xx"); ok0 {
						prefix := before
						if p, err := strconv.Atoi(prefix); err == nil {
							for i := p * 100; i < (p+1)*100; i++ {
								route.RetryOnStatuses[i] = true
							}
						}
					}
				}
			}
		}
	}

	return route, nil
}

// Helper functions for parsing unstructured data

func getString(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func getInt32(m map[string]any, key string, defaultVal int32) int32 {
	if v, ok := m[key].(float64); ok {
		if v > math.MaxInt32 {
			return math.MaxInt32
		}
		if v < math.MinInt32 {
			return math.MinInt32
		}
		return int32(v)
	}
	if v, ok := m[key].(int64); ok {
		if v > math.MaxInt32 {
			return math.MaxInt32
		}
		if v < math.MinInt32 {
			return math.MinInt32
		}
		return int32(v)
	}
	if v, ok := m[key].(int32); ok {
		return v
	}
	return defaultVal
}

func parseTimeWindow(tw map[string]any) *TimeWindow {
	window := &TimeWindow{
		Days: make(map[int]bool),
	}

	if start, ok := tw["start"].(string); ok {
		parts := strings.Split(start, ":")
		if len(parts) == 2 {
			window.StartHour, _ = strconv.Atoi(parts[0])
			window.StartMinute, _ = strconv.Atoi(parts[1])
		}
	}

	if end, ok := tw["end"].(string); ok {
		parts := strings.Split(end, ":")
		if len(parts) == 2 {
			window.EndHour, _ = strconv.Atoi(parts[0])
			window.EndMinute, _ = strconv.Atoi(parts[1])
		}
	}

	if days, ok := tw["days"].([]any); ok {
		for _, d := range days {
			if day, ok := d.(float64); ok {
				window.Days[int(day)] = true
			}
		}
	}

	return window
}
