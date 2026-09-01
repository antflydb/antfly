// Copyright 2026 Antfly, Inc.
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

// Package proxy implements a model-aware routing proxy for inference instances.
package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

// Metrics for Prometheus/KEDA autoscaling
var (
	requestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "antfly_inference_proxy_requests_total",
			Help: "Total requests by pool, model, and status",
		},
		[]string{"pool", "model", "operation", "status"},
	)

	queueDepth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "antfly_inference_proxy_queue_depth",
			Help: "Current queue depth per pool",
		},
		[]string{"pool"},
	)

	requestLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "antfly_inference_proxy_request_duration_seconds",
			Help:    "Request latency by pool and model",
			Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		},
		[]string{"pool", "model", "operation"},
	)

	modelLoaded = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "antfly_inference_proxy_model_loaded",
			Help: "Whether a model is loaded on an Inference runtime (1=loaded, 0=not loaded)",
		},
		[]string{"pool", "endpoint", "model"},
	)

	endpointHealth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "antfly_inference_proxy_endpoint_healthy",
			Help: "Whether an endpoint is healthy (1=healthy, 0=unhealthy)",
		},
		[]string{"pool", "endpoint"},
	)

	activeConnections = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "antfly_inference_proxy_active_connections",
			Help: "Active connections per endpoint",
		},
		[]string{"pool", "endpoint"},
	)
)

// WorkloadType represents the type of workload a pool handles
type WorkloadType string

const (
	WorkloadTypeReadHeavy  WorkloadType = "read-heavy"
	WorkloadTypeWriteHeavy WorkloadType = "write-heavy"
	WorkloadTypeBurst      WorkloadType = "burst"
	WorkloadTypeGeneral    WorkloadType = "general"
)

// Endpoint represents a single inference instance
type Endpoint struct {
	Address      string
	HealthURL    string
	Pool         string
	WorkloadType WorkloadType
	Models       map[string]*ModelInfo
	// CatalogKnown distinguishes a successfully discovered empty/partial catalog
	// from bootstrap registration, where the proxy has not learned capabilities
	// yet. Only the latter may use the compatibility pool fallback.
	CatalogKnown bool
	QueueDepth   int32
	LastSeen     time.Time
	Healthy      bool
	Connections  int32 // Active connections
}

// ModelInfo contains information about a loaded model
type ModelInfo struct {
	Name          string
	LoadedAt      time.Time
	RequestsTotal int64
	AvgLatencyMs  float64
	Latency       *RollingLatency
	// OperationState separates pre-discovery bootstrap inventory from a
	// successfully discovered legacy entry whose task is unknown. Only explicit
	// task advertisements are eligible for operation-aware routing.
	OperationState ModelOperationState
	Operations     map[OperationType]bool
}

type ModelOperationState uint8

const (
	ModelOperationsBootstrap ModelOperationState = iota
	ModelOperationsTaskUnknown
	ModelOperationsKnown
)

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	failures         int32
	threshold        int32
	timeout          time.Duration
	lastFailure      time.Time
	state            int32 // 0=closed, 1=open, 2=half-open
	halfOpenInFlight int32 // atomic counter for requests in half-open state

	mu sync.RWMutex
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(threshold int32, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold: threshold,
		timeout:   timeout,
	}
}

// Allow returns true if the circuit breaker allows a request
func (cb *CircuitBreaker) Allow() bool {
	return cb.TryAcquire()
}

// CanAttempt reports whether the circuit breaker is currently eligible for selection.
// Unlike TryAcquire, it does not mutate half-open state.
func (cb *CircuitBreaker) CanAttempt() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	state := atomic.LoadInt32(&cb.state)
	switch state {
	case 0: // closed
		return true
	case 1: // open
		return time.Since(cb.lastFailure) > cb.timeout && atomic.LoadInt32(&cb.halfOpenInFlight) == 0
	case 2: // half-open
		return false
	}
	return false
}

// TryAcquire reserves permission to send a request through the circuit breaker.
// In half-open recovery, exactly one caller is allowed to acquire the probe slot.
func (cb *CircuitBreaker) TryAcquire() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	state := atomic.LoadInt32(&cb.state)
	switch state {
	case 0: // closed
		return true
	case 1: // open
		if time.Since(cb.lastFailure) > cb.timeout {
			if atomic.CompareAndSwapInt32(&cb.halfOpenInFlight, 0, 1) {
				atomic.CompareAndSwapInt32(&cb.state, 1, 2) // transition to half-open
				return true
			}
			return false
		}
		return false
	case 2: // half-open
		return false
	}
	return false
}

// RecordSuccess records a successful request
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	atomic.StoreInt32(&cb.failures, 0)
	atomic.StoreInt32(&cb.state, 0)            // close circuit
	atomic.StoreInt32(&cb.halfOpenInFlight, 0) // reset half-open counter
}

// RecordFailure records a failed request
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	failures := atomic.AddInt32(&cb.failures, 1)
	cb.lastFailure = time.Now()

	if failures >= cb.threshold {
		atomic.StoreInt32(&cb.state, 1) // open circuit
	}
	atomic.StoreInt32(&cb.halfOpenInFlight, 0) // reset half-open counter
}

// ReleaseReservation abandons a claimed half-open probe without recording success or failure.
func (cb *CircuitBreaker) ReleaseReservation() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if atomic.LoadInt32(&cb.state) == 2 {
		cb.lastFailure = time.Now()
		atomic.StoreInt32(&cb.state, 1)
		atomic.StoreInt32(&cb.halfOpenInFlight, 0)
	}
}

// ModelRegistry tracks which models are available on which inference runtimes
type ModelRegistry struct {
	endpoints map[string]*Endpoint   // address -> endpoint
	models    map[string][]*Endpoint // model -> endpoints with model
	pools     map[string][]*Endpoint // pool -> endpoints in pool

	circuitBreakers map[string]*CircuitBreaker

	refreshInterval       time.Duration
	client                *http.Client
	upstreamAuthorization string

	mu sync.RWMutex
}

// NewModelRegistry creates a new ModelRegistry
func NewModelRegistry(refreshInterval time.Duration) *ModelRegistry {
	return &ModelRegistry{
		endpoints:       make(map[string]*Endpoint),
		models:          make(map[string][]*Endpoint),
		pools:           make(map[string][]*Endpoint),
		circuitBreakers: make(map[string]*CircuitBreaker),
		refreshInterval: refreshInterval,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// SetUpstreamAuthorization configures the Authorization header used for upstream refreshes and requests.
func (r *ModelRegistry) SetUpstreamAuthorization(value string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.upstreamAuthorization = strings.TrimSpace(value)
}

func (r *ModelRegistry) upstreamAuthorizationValue() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.upstreamAuthorization
}

type PoolConditionStats struct {
	HealthyEndpoints int
	AvgQueueDepth    float64
	ModelLoaded      bool
	P99Latency       time.Duration
	HasLatency       bool
}

// RegisterEndpoint adds or updates an endpoint
func (r *ModelRegistry) RegisterEndpoint(address, pool string, workloadType WorkloadType) {
	r.RegisterEndpointWithHealth(address, "", pool, workloadType)
}

// RegisterEndpointWithHealth adds or updates an endpoint with a distinct operational health URL.
func (r *ModelRegistry) RegisterEndpointWithHealth(address, healthURL, pool string, workloadType WorkloadType) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ep, exists := r.endpoints[address]
	if !exists {
		ep = &Endpoint{
			Address:      address,
			HealthURL:    healthURL,
			Pool:         pool,
			WorkloadType: workloadType,
			Models:       make(map[string]*ModelInfo),
			Healthy:      true,
			LastSeen:     time.Now(),
		}
		r.endpoints[address] = ep
		r.circuitBreakers[address] = NewCircuitBreaker(5, 30*time.Second)
	} else {
		oldPool := ep.Pool
		ep.HealthURL = healthURL
		ep.Pool = pool
		ep.WorkloadType = workloadType
		ep.LastSeen = time.Now()
		if oldPool != pool {
			r.removeEndpointFromPoolLocked(address, oldPool)
		}
	}

	// Add to pool index
	if r.pools[pool] == nil {
		r.pools[pool] = make([]*Endpoint, 0)
	}
	found := false
	for _, indexed := range r.pools[pool] {
		if indexed.Address == address {
			found = true
			break
		}
	}
	if !found {
		r.pools[pool] = append(r.pools[pool], ep)
	}
}

func (r *ModelRegistry) removeEndpointFromPoolLocked(address, pool string) {
	newPoolEndpoints := make([]*Endpoint, 0, len(r.pools[pool]))
	for _, e := range r.pools[pool] {
		if e.Address != address {
			newPoolEndpoints = append(newPoolEndpoints, e)
		}
	}
	r.pools[pool] = newPoolEndpoints
}

func (r *ModelRegistry) registerBootstrapModels(address string, models []string) {
	if len(models) == 0 {
		return
	}
	r.UpdateModels(address, models)
}

func (r *ModelRegistry) endpointHealthURL(address string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if ep, exists := r.endpoints[address]; exists {
		return ep.HealthURL
	}
	return ""
}

// UnregisterEndpoint removes an endpoint
func (r *ModelRegistry) UnregisterEndpoint(address string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ep, exists := r.endpoints[address]
	if !exists {
		return
	}

	// Remove from pool index
	r.removeEndpointFromPoolLocked(address, ep.Pool)

	// Remove from model index
	for model := range ep.Models {
		newModelEndpoints := make([]*Endpoint, 0)
		for _, e := range r.models[model] {
			if e.Address != address {
				newModelEndpoints = append(newModelEndpoints, e)
			}
		}
		r.models[model] = newModelEndpoints
	}

	delete(r.endpoints, address)
	delete(r.circuitBreakers, address)
}

// UpdateModels refreshes the model list for an endpoint
func (r *ModelRegistry) UpdateModels(address string, models []string) {
	r.updateModels(address, models, nil, false)
}

// UpdateModelOperations atomically replaces an endpoint's discovered model and
// task inventory. Routing uses this snapshot so a generator-only node cannot be
// selected for reads merely because it loads a model with the same name.
func (r *ModelRegistry) UpdateModelOperations(address string, operations map[string]map[OperationType]bool) {
	models := make([]string, 0, len(operations))
	for model := range operations {
		models = append(models, model)
	}
	sort.Strings(models)
	r.updateModels(address, models, operations, true)
}

func (r *ModelRegistry) updateModels(address string, models []string, operations map[string]map[OperationType]bool, catalogKnown bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ep, exists := r.endpoints[address]
	if !exists {
		return
	}

	// Track old models for cleanup
	oldModels := make(map[string]bool)
	for m := range ep.Models {
		oldModels[m] = true
	}

	// Update models
	for _, model := range models {
		if _, exists := ep.Models[model]; !exists {
			ep.Models[model] = newModelInfo(model)
		}
		if operations != nil {
			ep.Models[model].Operations = cloneOperations(operations[model])
			if len(ep.Models[model].Operations) == 0 {
				ep.Models[model].OperationState = ModelOperationsTaskUnknown
			} else {
				ep.Models[model].OperationState = ModelOperationsKnown
			}
		}
		delete(oldModels, model)

		// Update model index
		if r.models[model] == nil {
			r.models[model] = make([]*Endpoint, 0)
		}
		found := false
		for _, e := range r.models[model] {
			if e.Address == address {
				found = true
				break
			}
		}
		if !found {
			r.models[model] = append(r.models[model], ep)
		}

		// Update metric
		modelLoaded.WithLabelValues(ep.Pool, address, model).Set(1)
	}

	// Remove old models
	for model := range oldModels {
		delete(ep.Models, model)
		// Remove from model index
		newEndpoints := make([]*Endpoint, 0)
		for _, e := range r.models[model] {
			if e.Address != address {
				newEndpoints = append(newEndpoints, e)
			}
		}
		r.models[model] = newEndpoints
		modelLoaded.WithLabelValues(ep.Pool, address, model).Set(0)
	}

	ep.LastSeen = time.Now()
	if catalogKnown {
		ep.CatalogKnown = true
	}
}

func cloneOperations(source map[OperationType]bool) map[OperationType]bool {
	if source == nil {
		return nil
	}
	result := make(map[OperationType]bool, len(source))
	for operation, supported := range source {
		if supported {
			result[operation] = true
		}
	}
	return result
}

// RecordModelLatency updates rolling per-model latency for an endpoint.
func (r *ModelRegistry) RecordModelLatency(address, model string, duration time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ep, exists := r.endpoints[address]
	if !exists || model == "" {
		return
	}

	info, exists := ep.Models[model]
	if !exists {
		info = newModelInfo(model)
		ep.Models[model] = info
	}
	if info.Latency == nil {
		info.Latency = NewRollingLatency(defaultLatencyWindowSize)
	}

	count := info.RequestsTotal
	latencyMs := float64(duration.Milliseconds())
	info.AvgLatencyMs = ((info.AvgLatencyMs * float64(count)) + latencyMs) / float64(count+1)
	info.RequestsTotal++
	info.Latency.Record(duration)
}

// GetEndpointsForModel returns endpoints that have a specific model loaded
func (r *ModelRegistry) GetEndpointsForModel(model string) []*Endpoint {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.getAvailableEndpointsForModelLocked(model, "", "")
}

// GetEndpointsForModelInPool returns endpoints in a specific pool that have a model loaded.
func (r *ModelRegistry) GetEndpointsForModelInPool(model, pool string) []*Endpoint {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.getAvailableEndpointsForModelLocked(model, pool, "")
}

func (r *ModelRegistry) getAvailableEndpointsForModelOperation(model, pool string, operation OperationType) []*Endpoint {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getAvailableEndpointsForModelLocked(model, pool, operation)
}

func (r *ModelRegistry) getAvailableEndpointsForModelLocked(model, pool string, operation OperationType) []*Endpoint {
	endpoints := r.models[model]
	result := make([]*Endpoint, 0, len(endpoints))
	for _, ep := range endpoints {
		if pool != "" && ep.Pool != pool {
			continue
		}
		if r.isEndpointAvailableLocked(ep) {
			if operation != "" && !modelSupportsOperation(ep.Models[model], operation) {
				continue
			}
			result = append(result, ep)
		}
	}
	return result
}

func modelSupportsOperation(info *ModelInfo, operation OperationType) bool {
	if operation == "" {
		return true
	}
	if info == nil || info.OperationState != ModelOperationsKnown {
		return false
	}
	return info.Operations[operation]
}

func (r *ModelRegistry) getBootstrapEndpointsForPool(pool string) []*Endpoint {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]*Endpoint, 0, len(r.pools[pool]))
	for _, ep := range r.pools[pool] {
		if !ep.CatalogKnown && r.isEndpointAvailableLocked(ep) {
			result = append(result, ep)
		}
	}
	return result
}

// GetEndpointsForPool returns all endpoints in a pool
func (r *ModelRegistry) GetEndpointsForPool(pool string) []*Endpoint {
	r.mu.RLock()
	defer r.mu.RUnlock()

	endpoints := r.pools[pool]
	result := make([]*Endpoint, 0, len(endpoints))
	for _, ep := range endpoints {
		if r.isEndpointAvailableLocked(ep) {
			result = append(result, ep)
		}
	}
	return result
}

func (r *ModelRegistry) GetAvailableEndpoints() []*Endpoint {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]*Endpoint, 0, len(r.endpoints))
	for _, endpoint := range r.endpoints {
		if r.isEndpointAvailableLocked(endpoint) {
			result = append(result, endpoint)
		}
	}
	return result
}

func (r *ModelRegistry) PoolConditionStats(pool, model string) PoolConditionStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats := PoolConditionStats{}
	endpoints := r.pools[pool]
	if len(endpoints) == 0 {
		return stats
	}

	latencyBuckets := make([]int, latencyBucketCount+1)
	var totalQueueDepth int64
	var latencySamples int
	for _, ep := range endpoints {
		if !r.isEndpointAvailableLocked(ep) {
			continue
		}

		stats.HealthyEndpoints++
		totalQueueDepth += int64(atomic.LoadInt32(&ep.QueueDepth))

		info, exists := ep.Models[model]
		if !exists {
			continue
		}
		stats.ModelLoaded = true
		if info.Latency != nil {
			latencySamples += info.Latency.MergeInto(latencyBuckets)
		}
	}

	if stats.HealthyEndpoints > 0 {
		stats.AvgQueueDepth = float64(totalQueueDepth) / float64(stats.HealthyEndpoints)
	}
	if latencySamples > 0 {
		stats.P99Latency, stats.HasLatency = QuantileFromBuckets(latencyBuckets, latencySamples, 0.99)
	}

	return stats
}

func newModelInfo(name string) *ModelInfo {
	return &ModelInfo{
		Name:     name,
		LoadedAt: time.Now(),
		Latency:  NewRollingLatency(defaultLatencyWindowSize),
	}
}

func (r *ModelRegistry) TryAcquireEndpoint(address string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cb := r.circuitBreakers[address]
	if cb == nil {
		return false
	}
	return cb.TryAcquire()
}

func (r *ModelRegistry) isEndpointAvailableLocked(ep *Endpoint) bool {
	if ep == nil || !ep.Healthy {
		return false
	}
	cb := r.circuitBreakers[ep.Address]
	if cb == nil {
		return false
	}
	return cb.CanAttempt()
}

// RefreshEndpoint fetches current model list and health from an endpoint
func (r *ModelRegistry) RefreshEndpoint(ctx context.Context, address string) error {
	authorization := r.upstreamAuthorizationValue()
	if healthURL := r.endpointHealthURL(address); healthURL != "" {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
		if err != nil {
			r.markUnhealthy(address)
			return err
		}
		if authorization != "" {
			req.Header.Set("Authorization", authorization)
		}
		resp, err := r.client.Do(req)
		if err != nil {
			r.markUnhealthy(address)
			return err
		}
		_ = resp.Body.Close()
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusBadRequest {
			r.markUnhealthy(address)
			return fmt.Errorf("health check returned %d", resp.StatusCode)
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, address+"/ai/v1/models", nil)
	if err != nil {
		r.markUnhealthy(address)
		return err
	}
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		r.markUnhealthy(address)
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		r.markUnhealthy(address)
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxModelCatalogBytes+1))
	if err != nil || len(body) > maxModelCatalogBytes {
		r.markUnhealthy(address)
		return errors.New("upstream model catalog is unreadable or too large")
	}
	models, err := extractModelOperations(bytes.NewReader(body))
	if err != nil {
		r.markUnhealthy(address)
		return err
	}

	r.UpdateModelOperations(address, models)
	r.markHealthy(address)
	return nil
}

func extractModelNames(r io.Reader) ([]string, error) {
	operations, err := extractModelOperations(r)
	if err != nil {
		return nil, err
	}
	models := make([]string, 0, len(operations))
	for model := range operations {
		models = append(models, model)
	}
	sort.Strings(models)
	return models, nil
}

var modelCategoryOperations = map[string][]OperationType{
	"embedders":    {"embed", "embeddings"},
	"generators":   {"generate", "generate.batch", "chat.completions"},
	"readers":      {"read"},
	"rerankers":    {"rerank"},
	"chunkers":     {"chunk"},
	"extractors":   {"extract"},
	"rewriters":    {"rewrite"},
	"classifiers":  {"classify"},
	"transcribers": {"transcribe"},
}

func extractModelOperations(r io.Reader) (map[string]map[OperationType]bool, error) {
	var doc map[string]json.RawMessage
	if err := json.NewDecoder(r).Decode(&doc); err != nil {
		return nil, err
	}

	result := make(map[string]map[OperationType]bool)
	add := func(name string, operations []OperationType) {
		name = strings.TrimSpace(name)
		if name == "" {
			return
		}
		if _, exists := result[name]; !exists {
			result[name] = make(map[OperationType]bool)
		}
		for _, operation := range operations {
			result[name][operation] = true
		}
	}
	addFromCollection := func(raw json.RawMessage, operations []OperationType) {
		var entries []map[string]any
		if err := json.Unmarshal(raw, &entries); err == nil {
			for _, entry := range entries {
				for _, key := range []string{"name", "id"} {
					if value, ok := entry[key].(string); ok {
						add(value, operations)
					}
				}
			}
			return
		}
		var stringsList []string
		if err := json.Unmarshal(raw, &stringsList); err == nil {
			for _, name := range stringsList {
				add(name, operations)
			}
			return
		}
		var objectMap map[string]json.RawMessage
		if err := json.Unmarshal(raw, &objectMap); err == nil {
			for name := range objectMap {
				add(name, operations)
			}
		}
	}

	// Generic/OpenAI collections prove model presence but not task support.
	for _, key := range []string{"models", "data"} {
		if raw, ok := doc[key]; ok {
			addFromCollection(raw, nil)
		}
	}
	for category, operations := range modelCategoryOperations {
		if raw, ok := doc[category]; ok {
			addFromCollection(raw, operations)
		}
	}
	return result, nil
}

func (r *ModelRegistry) markHealthy(address string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if ep, exists := r.endpoints[address]; exists {
		ep.Healthy = true
		endpointHealth.WithLabelValues(ep.Pool, address).Set(1)
	}
}

func (r *ModelRegistry) markUnhealthy(address string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if ep, exists := r.endpoints[address]; exists {
		ep.Healthy = false
		endpointHealth.WithLabelValues(ep.Pool, address).Set(0)
	}
}

// GetCircuitBreaker returns the circuit breaker for an endpoint
func (r *ModelRegistry) GetCircuitBreaker(address string) *CircuitBreaker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.circuitBreakers[address]
}

// GetLock returns the registry's read-write lock for external access
func (r *ModelRegistry) GetLock() *sync.RWMutex {
	return &r.mu
}

// GetEndpoints returns all registered endpoints (caller must hold lock)
func (r *ModelRegistry) GetEndpoints() map[string]*Endpoint {
	return r.endpoints
}

// Router handles request routing logic
type Router struct {
	registry     *ModelRegistry
	hashRing     *ConsistentHashRing
	routeManager *RouteManager
	rrCounter    uint64
}

// NewRouter creates a new Router
func NewRouter(registry *ModelRegistry) *Router {
	return &Router{
		registry:     registry,
		hashRing:     NewConsistentHashRing(100), // 100 virtual nodes per endpoint
		routeManager: NewRouteManager(),
	}
}

// RouteRequest selects the best endpoint for a request
func (r *Router) RouteRequest(ctx context.Context, model string, pool string, workloadType WorkloadType, excluded map[string]bool, operations ...OperationType) (*Endpoint, error) {
	endpoints := r.ResolveEndpointCandidates(model, pool, excluded, operations...)
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no healthy endpoints available for model %s", model)
	}

	candidates := make([]*Endpoint, len(endpoints))
	copy(candidates, endpoints)

	for len(candidates) > 0 {
		endpoint, err := r.selectEndpoint(model, workloadType, candidates, true)
		if err != nil {
			return nil, err
		}
		if r.registry.TryAcquireEndpoint(endpoint.Address) {
			return endpoint, nil
		}

		filtered := make([]*Endpoint, 0, len(candidates)-1)
		for _, ep := range candidates {
			if ep.Address != endpoint.Address {
				filtered = append(filtered, ep)
			}
		}
		candidates = filtered
	}

	return nil, fmt.Errorf("no healthy endpoints available for model %s", model)
}

// ResolveEndpointCandidates returns currently eligible endpoint candidates without reserving them.
func (r *Router) ResolveEndpointCandidates(model string, pool string, excluded map[string]bool, operations ...OperationType) []*Endpoint {
	var operation OperationType
	if len(operations) > 0 {
		operation = operations[0]
	}
	var endpoints []*Endpoint
	endpoints = r.registry.getAvailableEndpointsForModelOperation(model, pool, operation)

	if len(endpoints) == 0 && pool != "" {
		// Preserve bootstrap compatibility before the first successful catalog
		// refresh, but never route a known-incompatible discovered endpoint.
		endpoints = r.registry.getBootstrapEndpointsForPool(pool)
	}

	filtered := filterExcludedEndpoints(endpoints, excluded)
	if len(filtered) > 0 {
		return filtered
	}
	return endpoints
}

// RouteManager returns the route manager for advanced routing
func (r *Router) RouteManager() *RouteManager {
	return r.routeManager
}

func (r *Router) selectEndpoint(model string, workloadType WorkloadType, endpoints []*Endpoint, advance bool) (*Endpoint, error) {
	switch workloadType {
	case WorkloadTypeReadHeavy:
		return r.consistentHashWithLeastLoaded(endpoints, model)
	case WorkloadTypeWriteHeavy:
		return r.leastLoaded(endpoints)
	case WorkloadTypeBurst:
		return r.roundRobinWithQueueAwareness(endpoints, 50, advance)
	default:
		return r.leastLoaded(endpoints)
	}
}

func filterExcludedEndpoints(endpoints []*Endpoint, excluded map[string]bool) []*Endpoint {
	if len(endpoints) == 0 || len(excluded) == 0 {
		return endpoints
	}

	filtered := make([]*Endpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint == nil || excluded[endpoint.Address] {
			continue
		}
		filtered = append(filtered, endpoint)
	}
	return filtered
}

// consistentHashWithLeastLoaded uses consistent hashing for model affinity
// but picks the least loaded among top candidates
func (r *Router) consistentHashWithLeastLoaded(endpoints []*Endpoint, model string) (*Endpoint, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints available")
	}

	// Get consistent hash candidates (top 3)
	candidates := r.hashRing.GetN(model, endpoints, 3)
	if len(candidates) == 0 {
		return r.leastLoaded(endpoints)
	}

	// Among candidates, pick least loaded
	return r.leastLoaded(candidates)
}

// leastLoaded selects the endpoint with fewest active connections
func (r *Router) leastLoaded(endpoints []*Endpoint) (*Endpoint, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints available")
	}

	// Sort by connections (ascending)
	sorted := make([]*Endpoint, len(endpoints))
	copy(sorted, endpoints)
	sort.Slice(sorted, func(i, j int) bool {
		return atomic.LoadInt32(&sorted[i].Connections) < atomic.LoadInt32(&sorted[j].Connections)
	})

	return sorted[0], nil
}

// roundRobinWithQueueAwareness distributes load but respects queue limits.
// When advance is false, it peeks the current round-robin choice without mutating router state.
func (r *Router) roundRobinWithQueueAwareness(endpoints []*Endpoint, maxQueue int32, advance bool) (*Endpoint, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints available")
	}

	// Filter out endpoints with full queues
	available := make([]*Endpoint, 0)
	for _, ep := range endpoints {
		if atomic.LoadInt32(&ep.QueueDepth) < maxQueue {
			available = append(available, ep)
		}
	}

	if len(available) == 0 {
		// All queues full, pick the one with shortest queue
		return r.leastLoaded(endpoints)
	}

	// Round robin among available
	index := atomic.LoadUint64(&r.rrCounter)
	if advance {
		index = atomic.AddUint64(&r.rrCounter, 1) - 1
	}
	return available[index%uint64(len(available))], nil
}

// ConsistentHashRing implements consistent hashing for endpoint selection
type ConsistentHashRing struct {
	virtualNodes int
}

// NewConsistentHashRing creates a new consistent hash ring
func NewConsistentHashRing(virtualNodes int) *ConsistentHashRing {
	return &ConsistentHashRing{virtualNodes: virtualNodes}
}

func (r *ConsistentHashRing) hash(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

// GetN returns the top N endpoints for a key using consistent hashing
func (r *ConsistentHashRing) GetN(key string, endpoints []*Endpoint, n int) []*Endpoint {
	if len(endpoints) == 0 {
		return nil
	}
	if n > len(endpoints) {
		n = len(endpoints)
	}

	// Simple consistent hashing: hash key + endpoint addresses, sort by hash
	type scored struct {
		ep    *Endpoint
		score uint32
	}
	scores := make([]scored, len(endpoints))
	keyHash := r.hash(key)

	for i, ep := range endpoints {
		// Combine key hash with endpoint hash
		epHash := r.hash(ep.Address)
		scores[i] = scored{ep: ep, score: keyHash ^ epHash}
	}

	// Sort by score
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].score < scores[j].score
	})

	result := make([]*Endpoint, n)
	for i := 0; i < n; i++ {
		result[i] = scores[i].ep
	}
	return result
}

// Proxy is the main proxy server
type Proxy struct {
	registry     *ModelRegistry
	router       *Router
	routeWatcher *RouteWatcher
	server       *http.Server
	logger       *zap.Logger

	defaultPool         string
	listenAddr          string
	maxRequestBodyBytes int64
}

const defaultMaxProxyRequestBodyBytes int64 = 256 << 20

// Config holds proxy configuration
type Config struct {
	ListenAddr            string
	DefaultPool           string
	RefreshInterval       time.Duration
	EnableRouteWatching   bool        // Enable watching InferenceProxy CRs
	RouteWatchNamespace   string      // Namespace to watch for routes (empty for all)
	RouteWatchKubeconfig  string      // Optional kubeconfig path for route watching
	UpstreamAuthorization string      // Optional Authorization header value for upstream refreshes and requests
	MaxRequestBodyBytes   int64       // Optional retained request-body ceiling; defaults to 256 MiB
	Logger                *zap.Logger // Optional logger (defaults to production logger)
}

// NewProxy creates a new Proxy
func NewProxy(cfg Config) *Proxy {
	registry := NewModelRegistry(cfg.RefreshInterval)
	registry.SetUpstreamAuthorization(cfg.UpstreamAuthorization)
	router := NewRouter(registry)

	logger := cfg.Logger
	if logger == nil {
		logger, _ = zap.NewProduction()
	}

	p := &Proxy{
		registry:    registry,
		router:      router,
		defaultPool: cfg.DefaultPool,
		listenAddr:  cfg.ListenAddr,
		logger:      logger,
	}
	p.maxRequestBodyBytes = cfg.MaxRequestBodyBytes
	if p.maxRequestBodyBytes <= 0 {
		p.maxRequestBodyBytes = defaultMaxProxyRequestBodyBytes
	}

	// Initialize RouteWatcher if enabled
	if cfg.EnableRouteWatching {
		routeWatcher, err := NewRouteWatcher(router.RouteManager(), RouteWatcherConfig{
			Kubeconfig: cfg.RouteWatchKubeconfig,
			Namespace:  cfg.RouteWatchNamespace,
		}, logger)
		if err != nil {
			logger.Error("failed to create RouteWatcher, route-based routing disabled", zap.Error(err))
		} else {
			p.routeWatcher = routeWatcher
		}
	}

	return p
}

// Start starts the proxy server
func (p *Proxy) Start(ctx context.Context) error {
	// Main API mux
	apiMux := http.NewServeMux()
	apiMux.HandleFunc("/ai/v1/embed", p.handleEmbed)
	apiMux.HandleFunc("/ai/v1/embeddings", p.handleEmbeddings)
	apiMux.HandleFunc("/ai/v1/chunk", p.handleChunk)
	apiMux.HandleFunc("/ai/v1/rerank", p.handleRerank)
	apiMux.HandleFunc("/ai/v1/rerank_multimodal", p.handleRerank)
	apiMux.HandleFunc("/ai/v1/extract", p.handleExtract)
	apiMux.HandleFunc("/ai/v1/rewrite", p.handleRewrite)
	apiMux.HandleFunc("/ai/v1/transcribe", p.handleTranscribe)
	apiMux.HandleFunc("/ai/v1/read", p.handleRead)
	apiMux.HandleFunc("/ai/v1/generate", p.handleGenerate)
	apiMux.HandleFunc("/ai/v1/generate/batch", p.handleGenerateBatch)
	apiMux.HandleFunc("/ai/v1/chat/completions", p.handleChatCompletions)
	apiMux.HandleFunc("/ai/v1/models", p.handleModels)
	apiMux.HandleFunc("/healthz", p.handleHealth)
	apiMux.HandleFunc("/readyz", p.handleReady)

	p.server = &http.Server{
		Addr:              p.listenAddr,
		Handler:           apiMux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	p.startBackgroundWorkers(ctx)

	return p.serve(ctx, p.server.ListenAndServe)
}

func (p *Proxy) serve(ctx context.Context, listen func() error) error {
	serverErr := make(chan error, 1)
	go func() {
		err := listen()
		if errors.Is(err, http.ErrServerClosed) {
			serverErr <- nil
			return
		}
		serverErr <- err
	}()

	select {
	case err := <-serverErr:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := p.server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return <-serverErr
	}
}

// Stop gracefully stops the proxy
func (p *Proxy) Stop(ctx context.Context) error {
	return p.server.Shutdown(ctx)
}

// handleEmbed routes embedding requests
func (p *Proxy) handleEmbed(w http.ResponseWriter, r *http.Request) {
	p.proxyRequest(w, r, "embed")
}

// handleEmbeddings routes OpenAI-compatible embedding requests.
func (p *Proxy) handleEmbeddings(w http.ResponseWriter, r *http.Request) {
	p.proxyRequest(w, r, "embeddings")
}

// handleChunk routes chunking requests
func (p *Proxy) handleChunk(w http.ResponseWriter, r *http.Request) {
	p.proxyRequest(w, r, "chunk")
}

// handleRerank routes reranking requests
func (p *Proxy) handleRerank(w http.ResponseWriter, r *http.Request) {
	p.proxyRequest(w, r, "rerank")
}

func (p *Proxy) handleExtract(w http.ResponseWriter, r *http.Request) {
	p.proxyRequest(w, r, "extract")
}

func (p *Proxy) handleRewrite(w http.ResponseWriter, r *http.Request) {
	p.proxyRequest(w, r, "rewrite")
}

func (p *Proxy) handleTranscribe(w http.ResponseWriter, r *http.Request) {
	p.proxyRequest(w, r, "transcribe")
}

func (p *Proxy) handleGenerate(w http.ResponseWriter, r *http.Request) {
	p.proxyRequest(w, r, "generate")
}

func (p *Proxy) handleRead(w http.ResponseWriter, r *http.Request) {
	p.proxyRequest(w, r, "read")
}

func (p *Proxy) handleGenerateBatch(w http.ResponseWriter, r *http.Request) {
	p.proxyRequest(w, r, "generate.batch")
}

func (p *Proxy) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	p.proxyRequest(w, r, "chat.completions")
}

const maxModelCatalogBytes = 8 << 20
const maxMergedModelCatalogBytes = 32 << 20
const maxConcurrentModelCatalogRequests = 8

// handleModels publishes a conservative catalog for one routing scope. Antfly
// clients provide model+task query parameters, allowing the proxy to aggregate
// exactly the operation-eligible endpoints in the selected/default pool.
// Unscoped compatibility requests remain pool-scoped rather than leaking a
// cluster-wide union that the subsequent request router cannot honor.
func (p *Proxy) handleModels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	model := strings.TrimSpace(r.URL.Query().Get("model"))
	task := strings.TrimSpace(r.URL.Query().Get("task"))
	taskScope, taskErr := catalogTaskScopeFor(task)
	if taskErr != nil || (model == "") != (taskScope.Operation == "") {
		http.Error(w, "model and a valid task must be provided together", http.StatusBadRequest)
		return
	}
	pool := strings.TrimSpace(r.Header.Get("X-Antfly-Inference-Pool"))
	if pool == "" {
		pool = p.defaultPool
	}
	var endpoints []*Endpoint
	if model != "" {
		endpoints = p.router.ResolveEndpointCandidates(model, pool, nil, taskScope.Operation)
	} else if pool == "" {
		endpoints = p.registry.GetAvailableEndpoints()
	} else {
		endpoints = p.registry.GetEndpointsForPool(pool)
	}
	addresses := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		addresses = append(addresses, endpoint.Address)
	}
	if len(addresses) == 0 {
		http.Error(w, "no operation-eligible inference endpoints", http.StatusServiceUnavailable)
		return
	}

	type catalogResult struct {
		catalog map[string]json.RawMessage
		err     error
	}
	resultBuffer := min(len(addresses), maxConcurrentModelCatalogRequests)
	results := make(chan catalogResult, resultBuffer)
	catalogSlots := make(chan struct{}, maxConcurrentModelCatalogRequests)
	for _, address := range addresses {
		go func(address string) {
			select {
			case catalogSlots <- struct{}{}:
				defer func() { <-catalogSlots }()
			case <-r.Context().Done():
				results <- catalogResult{err: r.Context().Err()}
				return
			}
			request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, strings.TrimRight(address, "/")+"/ai/v1/models", nil)
			if err != nil {
				results <- catalogResult{err: err}
				return
			}
			if authorization := p.registry.upstreamAuthorizationValue(); authorization != "" {
				request.Header.Set("Authorization", authorization)
			}
			response, err := p.registry.client.Do(request)
			if err != nil {
				results <- catalogResult{err: err}
				return
			}
			defer func() { _ = response.Body.Close() }()
			if response.StatusCode < 200 || response.StatusCode >= 300 {
				results <- catalogResult{err: fmt.Errorf("upstream catalog returned %d", response.StatusCode)}
				return
			}
			body, err := io.ReadAll(io.LimitReader(response.Body, maxModelCatalogBytes+1))
			if err != nil || len(body) > maxModelCatalogBytes {
				results <- catalogResult{err: errors.New("upstream model catalog is unreadable or too large")}
				return
			}
			var catalog map[string]json.RawMessage
			if err := json.Unmarshal(body, &catalog); err != nil {
				results <- catalogResult{err: err}
				return
			}
			if model != "" && !catalogContainsTaskModel(catalog, taskScope, model) {
				results <- catalogResult{err: errors.New("upstream catalog no longer advertises the scoped model task")}
				return
			}
			results <- catalogResult{catalog: catalog}
		}(address)
	}

	categories := map[string]map[string]json.RawMessage{}
	successes := 0
	failures := 0
	mergedTooLarge := false
	for range addresses {
		result := <-results
		if result.err != nil {
			failures++
			continue
		}
		successes++
		if !mergedTooLarge {
			if model != "" {
				mergeScopedModelCatalog(categories, result.catalog, taskScope, model)
			} else {
				mergeModelCatalog(categories, result.catalog)
			}
			mergedTooLarge = modelCatalogEncodedBytes(categories) > maxMergedModelCatalogBytes
		}
	}
	if failures > 0 {
		// Returning a partial catalog is unsafe: any omitted endpoint remains a
		// routing candidate, so its possibly weaker limits would make the merged
		// descriptor an over-promise.
		http.Error(w, "inference model catalog is temporarily incomplete", http.StatusBadGateway)
		return
	}
	if mergedTooLarge {
		http.Error(w, "merged inference model catalog is too large", http.StatusBadGateway)
		return
	}
	if successes == 0 {
		http.Error(w, "inference model catalog is unavailable", http.StatusBadGateway)
		return
	}

	modelNames := map[string]bool{}
	response := make(map[string]any, len(categories)+1)
	for category, models := range categories {
		response[category] = models
		for model := range models {
			modelNames[model] = true
		}
	}
	names := make([]string, 0, len(modelNames))
	for model := range modelNames {
		names = append(names, model)
	}
	sort.Strings(names)
	data := make([]map[string]string, 0, len(names))
	for _, model := range names {
		data = append(data, map[string]string{"id": model})
	}
	response["data"] = data
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		p.logger.Warn("failed to encode merged inference model catalog", zap.Error(err))
	}
}

type catalogTaskScope struct {
	Operation OperationType
	Category  string
}

var catalogTaskScopes = map[string]catalogTaskScope{
	"read":       {Operation: "read", Category: "readers"},
	"generate":   {Operation: "generate.batch", Category: "generators"},
	"embed":      {Operation: "embed", Category: "embedders"},
	"rerank":     {Operation: "rerank", Category: "rerankers"},
	"chunk":      {Operation: "chunk", Category: "chunkers"},
	"extract":    {Operation: "extract", Category: "extractors"},
	"rewrite":    {Operation: "rewrite", Category: "rewriters"},
	"classify":   {Operation: "classify", Category: "classifiers"},
	"transcribe": {Operation: "transcribe", Category: "transcribers"},
}

func catalogTaskScopeFor(raw string) (catalogTaskScope, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return catalogTaskScope{}, nil
	}
	scope, ok := catalogTaskScopes[raw]
	if !ok {
		return catalogTaskScope{}, fmt.Errorf("unsupported inference task %q", raw)
	}
	return scope, nil
}

func catalogContainsTaskModel(catalog map[string]json.RawMessage, scope catalogTaskScope, model string) bool {
	raw, ok := catalog[scope.Category]
	if !ok {
		return false
	}
	var models map[string]json.RawMessage
	if json.Unmarshal(raw, &models) != nil {
		return false
	}
	_, ok = models[model]
	return ok
}

func modelCatalogEncodedBytes(categories map[string]map[string]json.RawMessage) int {
	total := 0
	for category, models := range categories {
		total += len(category)
		for model, descriptor := range models {
			total += len(model) + len(descriptor)
		}
	}
	return total
}

var modelCatalogCategories = []string{
	"embedders", "generators", "readers", "rerankers", "chunkers",
	"extractors", "rewriters", "classifiers", "transcribers",
}

func mergeModelCatalog(target map[string]map[string]json.RawMessage, source map[string]json.RawMessage) {
	for _, category := range modelCatalogCategories {
		raw, ok := source[category]
		if !ok {
			continue
		}
		var models map[string]json.RawMessage
		if err := json.Unmarshal(raw, &models); err != nil {
			continue
		}
		if target[category] == nil {
			target[category] = make(map[string]json.RawMessage)
		}
		for model, descriptor := range models {
			if existing, duplicate := target[category][model]; duplicate {
				target[category][model] = conservativeModelDescriptor(existing, descriptor)
			} else {
				target[category][model] = append(json.RawMessage(nil), descriptor...)
			}
		}
	}
}

func mergeScopedModelCatalog(target map[string]map[string]json.RawMessage, source map[string]json.RawMessage, scope catalogTaskScope, model string) {
	raw := source[scope.Category]
	var models map[string]json.RawMessage
	if json.Unmarshal(raw, &models) != nil {
		return
	}
	descriptor, ok := models[model]
	if !ok {
		return
	}
	if target[scope.Category] == nil {
		target[scope.Category] = make(map[string]json.RawMessage)
	}
	if existing, duplicate := target[scope.Category][model]; duplicate {
		target[scope.Category][model] = conservativeModelDescriptor(existing, descriptor)
	} else {
		target[scope.Category][model] = append(json.RawMessage(nil), descriptor...)
	}
}

func conservativeModelDescriptor(left, right json.RawMessage) json.RawMessage {
	var a, b map[string]any
	if json.Unmarshal(left, &a) != nil || json.Unmarshal(right, &b) != nil {
		return json.RawMessage(`{}`)
	}
	merged := make(map[string]any)
	for key, av := range a {
		bv, ok := b[key]
		if !ok {
			continue
		}
		if key == "inputs" || key == "capabilities" {
			merged[key] = intersectStringValues(av, bv)
			continue
		}
		if key == "inference_capabilities" {
			if capabilities, ok := conservativeInferenceCapabilities(av, bv); ok {
				merged[key] = capabilities
			}
			continue
		}
		if reflect.DeepEqual(av, bv) {
			merged[key] = av
		}
	}
	raw, err := json.Marshal(merged)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}

func conservativeInferenceCapabilities(left, right any) (map[string]any, bool) {
	a, aok := left.(map[string]any)
	b, bok := right.(map[string]any)
	if !aok || !bok || a["task"] != b["task"] {
		return nil, false
	}
	aBatch, aok := a["batch"].(map[string]any)
	bBatch, bok := b["batch"].(map[string]any)
	if !aok || !bok {
		return nil, false
	}
	aMode, aok := aBatch["mode"].(string)
	bMode, bok := bBatch["mode"].(string)
	if !aok || !bok || !validBatchMode(aMode) || !validBatchMode(bMode) {
		return nil, false
	}
	mode := "serial_compatibility"
	if aMode == "none" || bMode == "none" {
		mode = "none"
	} else if aMode == "native" && bMode == "native" {
		mode = "native"
	}
	batch := map[string]any{"mode": mode}
	for _, field := range []string{"preferred_items", "max_items"} {
		value, ok := conservativePositiveLimit(aBatch[field], bBatch[field])
		if !ok {
			return nil, false
		}
		batch[field] = value
	}
	for _, field := range []string{"max_media_parts_per_item"} {
		value, ok := conservativeRequiredLimit(aBatch[field], bBatch[field])
		if !ok {
			return nil, false
		}
		batch[field] = value
	}
	for _, field := range []string{"max_encoded_bytes", "max_decoded_pixels"} {
		value, ok := conservativeOptionalLimit(aBatch[field], bBatch[field])
		if !ok {
			return nil, false
		}
		batch[field] = value
	}
	aFailures, aok := aBatch["per_item_failures"].(bool)
	bFailures, bok := bBatch["per_item_failures"].(bool)
	if !aok || !bok {
		return nil, false
	}
	batch["per_item_failures"] = aFailures && bFailures
	return map[string]any{"task": a["task"], "batch": batch}, true
}

func validBatchMode(mode string) bool {
	return mode == "none" || mode == "serial_compatibility" || mode == "native"
}

func conservativeRequiredLimit(left, right any) (float64, bool) {
	a, aok := nonNegativeInteger(left)
	b, bok := nonNegativeInteger(right)
	if !aok || !bok {
		return 0, false
	}
	return min(a, b), true
}

func conservativePositiveLimit(left, right any) (float64, bool) {
	value, ok := conservativeRequiredLimit(left, right)
	return value, ok && value > 0
}

// Encoded-byte and decoded-pixel ceilings are nullable. Older nodes used zero
// for unknown, so zero is accepted as the legacy spelling of null. A known
// finite ceiling always survives intersection with an unknown value.
func conservativeOptionalLimit(left, right any) (any, bool) {
	a, aKnown, aok := optionalLimit(left)
	b, bKnown, bok := optionalLimit(right)
	if !aok || !bok {
		return nil, false
	}
	switch {
	case aKnown && bKnown:
		return min(a, b), true
	case aKnown:
		return a, true
	case bKnown:
		return b, true
	default:
		return nil, true
	}
}

func optionalLimit(value any) (float64, bool, bool) {
	if value == nil {
		return 0, false, true
	}
	number, ok := nonNegativeInteger(value)
	if !ok {
		return 0, false, false
	}
	return number, number != 0, true
}

func nonNegativeInteger(value any) (float64, bool) {
	number, ok := value.(float64)
	const maxExactJSONInteger = float64(1<<53 - 1)
	if !ok || number < 0 || number > maxExactJSONInteger || number != float64(uint64(number)) {
		return 0, false
	}
	return number, true
}

func intersectStringValues(left, right any) []string {
	a, aok := left.([]any)
	b, bok := right.([]any)
	if !aok || !bok {
		return []string{}
	}
	rightValues := make(map[string]bool, len(b))
	for _, value := range b {
		if text, ok := value.(string); ok {
			rightValues[text] = true
		}
	}
	intersection := make([]string, 0, len(a))
	for _, value := range a {
		if text, ok := value.(string); ok && rightValues[text] {
			intersection = append(intersection, text)
		}
	}
	return intersection
}

func (p *Proxy) proxyRequest(w http.ResponseWriter, r *http.Request, operation string) {
	start := time.Now()

	// Parse request to get model
	if r.ContentLength > p.maxRequestBodyBytes {
		http.Error(w, "inference request body is too large", http.StatusRequestEntityTooLarge)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, p.maxRequestBodyBytes+1))
	if err != nil {
		http.Error(w, "failed to read request", http.StatusBadRequest)
		return
	}
	if int64(len(body)) > p.maxRequestBodyBytes {
		http.Error(w, "inference request body is too large", http.StatusRequestEntityTooLarge)
		return
	}

	model, err := proxyRequestModel(body, operation)
	if err != nil {
		http.Error(w, "invalid inference request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if model == "" {
		http.Error(w, "model is required", http.StatusBadRequest)
		return
	}

	// Build headers map for route matching
	headers := make(map[string]string)
	for k := range r.Header {
		headers[k] = r.Header.Get(k)
	}

	lease, err := p.AcquireRequestResolution(r.Context(), ResolveRequest{
		Operation: OperationType(operation),
		Model:     model,
		Headers:   headers,
		Source: VerifiedSource{
			Table: firstHeader(r, "X-Antfly-Source-Table", "X-Antfly-Table"),
		},
		Timestamp: start,
	})
	if err != nil {
		if resolutionErr, ok := err.(*ResolutionError); ok {
			if resolutionErr.RetryAfter > 0 {
				w.Header().Set("Retry-After", fmt.Sprintf("%d", resolutionErr.RetryAfter))
			}
			http.Error(w, resolutionErr.Message, resolutionErr.StatusCode)
			return
		}
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	resolution := lease.Resolution

	matchedRoute := resolution.Route
	pool := resolution.Pool

	if err := lease.Admit(); err != nil {
		lease.Release()
		if resolutionErr, ok := err.(*ResolutionError); ok {
			if resolutionErr.RetryAfter > 0 {
				w.Header().Set("Retry-After", fmt.Sprintf("%d", resolutionErr.RetryAfter))
			}
			http.Error(w, resolutionErr.Message, resolutionErr.StatusCode)
			return
		}
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	attempts := 1
	if matchedRoute != nil && matchedRoute.RetryAttempts > 1 {
		attempts = int(matchedRoute.RetryAttempts)
	}

	var lastErr error
	var lastResp *http.Response
	var lastEndpoint *Endpoint

	for attempt := 0; attempt < attempts; attempt++ {
		attemptStarted := time.Now()
		endpoint := lease.Resolution.Endpoint
		if attempt > 0 {
			lease, err = lease.NextAttempt(r.Context())
		}
		if err != nil {
			requestsTotal.WithLabelValues(pool, model, operation, "no_endpoint").Inc()
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		if attempt > 0 {
			endpoint = lease.Resolution.Endpoint
		}

		forwardingLease := lease.BeginForwarding()
		resp, reqErr := p.forwardRequest(r, body, endpoint, matchedRoute)
		if reqErr != nil {
			forwardingLease.Finish()
			lastErr = reqErr
			lastEndpoint = endpoint
			lease.RecordFailure()
			if attempt+1 < attempts && shouldRetryRequestError(matchedRoute, reqErr) {
				continue
			}
			p.recordProxyMetrics(endpoint.Pool, model, operation, start, "error")
			http.Error(w, fmt.Sprintf("proxy request failed: %v", reqErr), http.StatusBadGateway)
			return
		}

		lastResp = resp
		lastEndpoint = endpoint
		resp.Body = wrapForwardingBody(resp.Body, forwardingLease)
		if attempt+1 < attempts && shouldRetryStatus(matchedRoute, resp.StatusCode) {
			drainErr := copyResponse(io.Discard, resp)
			p.registry.RecordModelLatency(endpoint.Address, model, time.Since(attemptStarted))
			lease.RecordFailure()
			if drainErr != nil {
				lastErr = drainErr
				lastEndpoint = endpoint
			}
			continue
		}

		streamErr := copyResponse(w, resp)
		p.registry.RecordModelLatency(endpoint.Address, model, time.Since(attemptStarted))

		if resp.StatusCode >= 400 || streamErr != nil {
			lease.RecordFailure()
			p.recordProxyMetrics(endpoint.Pool, model, operation, start, "error")
		} else {
			lease.RecordSuccess()
			p.recordProxyMetrics(endpoint.Pool, model, operation, start, "success")
		}
		return
	}

	if lastResp != nil {
		status := "success"
		if lastResp.StatusCode >= 400 {
			status = "error"
		}
		if lastEndpoint != nil {
			p.recordProxyMetrics(lastEndpoint.Pool, model, operation, start, status)
		}
		_ = copyResponse(w, lastResp)
		return
	}

	p.recordProxyMetrics(pool, model, operation, start, "error")
	http.Error(w, fmt.Sprintf("proxy request failed: %v", lastErr), http.StatusBadGateway)
}

func proxyRequestModel(body []byte, operation string) (string, error) {
	if operation != "generate.batch" {
		var request struct {
			Model string `json:"model"`
		}
		if err := json.Unmarshal(body, &request); err != nil {
			return "", err
		}
		return strings.TrimSpace(request.Model), nil
	}

	var batch struct {
		Requests []struct {
			Body struct {
				Model string `json:"model"`
			} `json:"body"`
		} `json:"requests"`
	}
	if err := json.Unmarshal(body, &batch); err != nil {
		return "", err
	}
	if len(batch.Requests) == 0 {
		return "", errors.New("batch requests must not be empty")
	}
	model := strings.TrimSpace(batch.Requests[0].Body.Model)
	if model == "" {
		return "", errors.New("batch request model is required")
	}
	for _, item := range batch.Requests[1:] {
		if strings.TrimSpace(item.Body.Model) != model {
			return "", errors.New("mixed-model batches must be partitioned before routing")
		}
	}
	return model, nil
}

func (p *Proxy) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (p *Proxy) handleReady(w http.ResponseWriter, r *http.Request) {
	// Ready means the proxy can currently route to at least one endpoint.
	p.registry.mu.RLock()
	hasAvailable := false
	for _, ep := range p.registry.endpoints {
		if p.registry.isEndpointAvailableLocked(ep) {
			hasAvailable = true
			break
		}
	}
	p.registry.mu.RUnlock()

	if hasAvailable {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("no healthy endpoints"))
	}
}

func (p *Proxy) refreshLoop(ctx context.Context) {
	if p.registry.refreshInterval <= 0 {
		return
	}

	ticker := time.NewTicker(p.registry.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.registry.mu.RLock()
			endpoints := make([]string, 0, len(p.registry.endpoints))
			for addr := range p.registry.endpoints {
				endpoints = append(endpoints, addr)
			}
			p.registry.mu.RUnlock()

			for _, addr := range endpoints {
				// Ignore refresh errors - continue to try other endpoints
				_ = p.registry.RefreshEndpoint(ctx, addr)
			}

			// Update pool queue depth metrics
			p.registry.mu.RLock()
			for pool, eps := range p.registry.pools {
				var totalQueue int32
				for _, ep := range eps {
					totalQueue += atomic.LoadInt32(&ep.QueueDepth)
				}
				queueDepth.WithLabelValues(pool).Set(float64(totalQueue))
			}
			p.registry.mu.RUnlock()
		}
	}
}

// Registry returns the model registry for external access
func (p *Proxy) Registry() *ModelRegistry {
	return p.registry
}

// Router returns the router for external access
func (p *Proxy) Router() *Router {
	return p.router
}

// RegisterEndpoint adds an endpoint (called from K8s watcher)
func (p *Proxy) RegisterEndpoint(address, pool string, workloadType WorkloadType) {
	p.registry.RegisterEndpoint(address, pool, workloadType)
}

// RegisterEndpointWithHealth adds an endpoint with a distinct operational health URL.
func (p *Proxy) RegisterEndpointWithHealth(address, healthURL, pool string, workloadType WorkloadType) {
	p.registry.RegisterEndpointWithHealth(address, healthURL, pool, workloadType)
}

// UnregisterEndpoint removes an endpoint (called from K8s watcher)
func (p *Proxy) UnregisterEndpoint(address string) {
	p.registry.UnregisterEndpoint(address)
}

func (p *Proxy) forwardRequest(r *http.Request, body []byte, endpoint *Endpoint, route *Route) (*http.Response, error) {
	attemptCtx := r.Context()
	var cancel context.CancelFunc
	if route != nil && route.RetryTimeout > 0 {
		attemptCtx, cancel = context.WithTimeout(attemptCtx, route.RetryTimeout)
		defer cancel()
	}

	targetURL, err := url.Parse(endpoint.Address)
	if err != nil {
		return nil, err
	}

	outReq := r.Clone(attemptCtx)
	outReq.URL = targetURL.ResolveReference(&url.URL{
		Path:     r.URL.Path,
		RawPath:  r.URL.RawPath,
		RawQuery: r.URL.RawQuery,
	})
	outReq.Host = targetURL.Host
	outReq.RequestURI = ""
	outReq.Body = io.NopCloser(bytes.NewReader(body))
	outReq.ContentLength = int64(len(body))
	if authorization := p.registry.upstreamAuthorizationValue(); authorization != "" {
		outReq.Header.Set("Authorization", authorization)
	}

	return p.registry.client.Do(outReq)
}

func (p *Proxy) recordProxyMetrics(pool, model, operation string, started time.Time, status string) {
	requestsTotal.WithLabelValues(pool, model, operation, status).Inc()
	requestLatency.WithLabelValues(pool, model, operation).Observe(time.Since(started).Seconds())
}

type forwardingBody struct {
	io.ReadCloser
	finish func()
	once   sync.Once
}

func wrapForwardingBody(body io.ReadCloser, lease *ForwardingLease) io.ReadCloser {
	if body == nil || lease == nil {
		return body
	}
	return &forwardingBody{
		ReadCloser: body,
		finish:     lease.Finish,
	}
}

func (b *forwardingBody) Close() error {
	if b == nil || b.ReadCloser == nil {
		return nil
	}
	err := b.ReadCloser.Close()
	b.once.Do(func() {
		if b.finish != nil {
			b.finish()
		}
	})
	return err
}

func copyResponse(w io.Writer, resp *http.Response) error {
	if writer, ok := w.(http.ResponseWriter); ok {
		for key, values := range resp.Header {
			for _, value := range values {
				writer.Header().Add(key, value)
			}
		}
		writer.WriteHeader(resp.StatusCode)
	}

	var copyErr error
	if w != nil {
		_, copyErr = io.Copy(w, resp.Body)
	}
	closeErr := resp.Body.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func shouldRetryStatus(route *Route, statusCode int) bool {
	if route == nil || len(route.RetryOnStatuses) == 0 {
		return false
	}
	return route.RetryOnStatuses[statusCode]
}

func shouldRetryRequestError(route *Route, err error) bool {
	if route == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return route.RetryOnCanceled
	}
	return route.RetryOnRequestErrs || errors.Is(err, context.DeadlineExceeded)
}

func (p *Proxy) waitForQueuedDestination(ctx context.Context, route *Route, req *RouteRequest) (string, error) {
	maxQueueTime := route.Fallback.MaxQueueTime
	if maxQueueTime <= 0 {
		maxQueueTime = 30 * time.Second
	}

	deadline := time.NewTimer(maxQueueTime)
	defer deadline.Stop()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline.C:
			return "", fmt.Errorf("queue timeout exceeded")
		case <-ticker.C:
			req.Timestamp = time.Now()
			dest, err := p.router.RouteManager().SelectDestination(route, req, p.registry)
			if err != nil {
				return "", err
			}
			if dest != nil {
				return dest.Pool, nil
			}
		}
	}
}

func firstHeader(r *http.Request, names ...string) string {
	for _, name := range names {
		if value := r.Header.Get(name); value != "" {
			return value
		}
	}
	return ""
}
