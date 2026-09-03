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
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"mime"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
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
	address      string
	healthURL    string
	pool         string
	workloadType WorkloadType
	models       map[string]*ModelInfo
	// catalogKnown distinguishes a successfully discovered empty/partial catalog
	// from bootstrap registration, where the proxy has not learned capabilities
	// yet. Bootstrap endpoints participate only in task-unscoped legacy lookup;
	// executable routes require a discovered per-model operation.
	catalogKnown bool
	runtime      *endpointRuntime
	lastSeen     time.Time
	healthy      bool
	// incarnation is assigned by the registry and changes whenever an address is
	// replaced or its routing topology changes. It is the authority captured by
	// distributed capability discovery and execution leases.
	incarnation uint64
}

type endpointRuntime struct {
	queueDepth  int32
	connections int32
}

// Address returns the immutable network identity of this endpoint. All other
// registry state is exposed through value snapshots so callers cannot mutate
// routing state behind the registry's incarnation fence.
func (e *Endpoint) Address() string {
	if e == nil {
		return ""
	}
	return e.address
}

// Pool returns the endpoint's immutable routing pool.
func (e *Endpoint) Pool() string {
	if e == nil {
		return ""
	}
	return e.pool
}

// WorkloadType returns the endpoint's immutable scheduling class.
func (e *Endpoint) WorkloadType() WorkloadType {
	if e == nil {
		return ""
	}
	return e.workloadType
}

// EndpointSnapshot is a detached, read-only view of registry state.
type EndpointSnapshot struct {
	Address      string
	HealthURL    string
	Pool         string
	WorkloadType WorkloadType
	Models       []ModelSnapshot
	CatalogKnown bool
	QueueDepth   int32
	LastSeen     time.Time
	Healthy      bool
	Connections  int32
	Incarnation  uint64
}

// ModelSnapshot is the detached model catalog carried by EndpointSnapshot.
// Operations are sorted so snapshots are deterministic and safe to compare.
type ModelSnapshot struct {
	Name           string
	LoadedAt       time.Time
	RequestsTotal  int64
	AvgLatencyMs   float64
	OperationState ModelOperationState
	Operations     []OperationType
}

// endpointRef is an immutable registry identity captured while the registry is
// read-locked. Endpoint pointers and addresses are deliberately insufficient:
// topology changes replace Endpoint objects and addresses may be reused.
type endpointRef struct {
	endpoint    *Endpoint
	incarnation uint64
	breaker     *CircuitBreaker
}

// endpointReservation is the linearized result of selecting and admitting one
// endpoint incarnation. The exact breaker must travel with the reservation so
// completion can never be charged to a replacement registered at the same
// address.
type endpointReservation struct {
	ref endpointRef
}

// EndpointLease is the public reservation API. It exposes the immutable
// endpoint identity while retaining the exact circuit breaker that admitted
// this incarnation.
type EndpointLease struct {
	reservation endpointReservation
	once        sync.Once
}

// Endpoint returns the immutable endpoint identity reserved by this lease.
func (l *EndpointLease) Endpoint() *Endpoint {
	if l == nil {
		return nil
	}
	return l.reservation.ref.endpoint
}

func (l *EndpointLease) finish(fn func(*CircuitBreaker)) {
	if l == nil || l.reservation.ref.breaker == nil {
		return
	}
	l.once.Do(func() { fn(l.reservation.ref.breaker) })
}

// RecordSuccess completes the lease and records successful execution.
func (l *EndpointLease) RecordSuccess() { l.finish(func(cb *CircuitBreaker) { cb.RecordSuccess() }) }

// RecordFailure completes the lease and records failed execution.
func (l *EndpointLease) RecordFailure() { l.finish(func(cb *CircuitBreaker) { cb.RecordFailure() }) }

// Release abandons the lease without recording an execution result.
func (l *EndpointLease) Release() { l.finish(func(cb *CircuitBreaker) { cb.ReleaseReservation() }) }

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
	nextIncarnation uint64

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
			address:      address,
			healthURL:    healthURL,
			pool:         pool,
			workloadType: workloadType,
			models:       make(map[string]*ModelInfo),
			runtime:      &endpointRuntime{},
			healthy:      true,
			lastSeen:     time.Now(),
			incarnation:  r.nextEndpointIncarnationLocked(),
		}
		r.endpoints[address] = ep
		r.circuitBreakers[address] = NewCircuitBreaker(5, 30*time.Second)
	} else {
		oldPool := ep.pool
		if ep.healthURL != healthURL || ep.pool != pool || ep.workloadType != workloadType {
			r.removeEndpointFromPoolLocked(address, oldPool)
			if oldPool != pool {
				endpointHealth.WithLabelValues(oldPool, address).Set(0)
				for model := range ep.models {
					modelLoaded.WithLabelValues(oldPool, address, model).Set(0)
					modelLoaded.WithLabelValues(pool, address, model).Set(1)
				}
			}
			replacement := &Endpoint{
				address:      address,
				healthURL:    healthURL,
				pool:         pool,
				workloadType: workloadType,
				models:       ep.models,
				catalogKnown: ep.catalogKnown,
				runtime:      ep.runtime,
				lastSeen:     time.Now(),
				healthy:      ep.healthy,
				incarnation:  r.nextEndpointIncarnationLocked(),
			}
			for model := range ep.models {
				for i, indexed := range r.models[model] {
					if indexed == ep {
						r.models[model][i] = replacement
					}
				}
			}
			ep = replacement
			r.endpoints[address] = replacement
			r.circuitBreakers[address] = NewCircuitBreaker(5, 30*time.Second)
			healthValue := float64(0)
			if replacement.healthy {
				healthValue = 1
			}
			endpointHealth.WithLabelValues(pool, address).Set(healthValue)
		} else {
			ep.lastSeen = time.Now()
		}
	}

	// Add to pool index
	if r.pools[pool] == nil {
		r.pools[pool] = make([]*Endpoint, 0)
	}
	found := false
	for _, indexed := range r.pools[pool] {
		if indexed.address == address {
			found = true
			break
		}
	}
	if !found {
		r.pools[pool] = append(r.pools[pool], ep)
	}
}

func (r *ModelRegistry) nextEndpointIncarnationLocked() uint64 {
	r.nextIncarnation++
	if r.nextIncarnation == 0 {
		r.nextIncarnation++
	}
	return r.nextIncarnation
}

func (r *ModelRegistry) removeEndpointFromPoolLocked(address, pool string) {
	newPoolEndpoints := make([]*Endpoint, 0, len(r.pools[pool]))
	for _, e := range r.pools[pool] {
		if e.address != address {
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
		return ep.healthURL
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
	r.removeEndpointFromPoolLocked(address, ep.pool)

	// Remove from model index
	for model := range ep.models {
		newModelEndpoints := make([]*Endpoint, 0)
		for _, e := range r.models[model] {
			if e.address != address {
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
	r.updateModelOperationsForEndpoint(address, nil, 0, operations)
}

func (r *ModelRegistry) endpointRefAtIncarnation(address string, incarnation uint64) (endpointRef, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	endpoint := r.endpoints[address]
	if endpoint == nil || endpoint.incarnation != incarnation {
		return endpointRef{}, false
	}
	breaker := r.circuitBreakers[address]
	if breaker == nil {
		return endpointRef{}, false
	}
	return endpointRef{endpoint: endpoint, incarnation: incarnation, breaker: breaker}, true
}

func (r *ModelRegistry) endpointAtIncarnation(address string, incarnation uint64) *Endpoint {
	ref, ok := r.endpointRefAtIncarnation(address, incarnation)
	if !ok {
		return nil
	}
	return ref.endpoint
}

func (r *ModelRegistry) endpointIncarnationSnapshotLocked() map[string]uint64 {
	snapshot := make(map[string]uint64, len(r.endpoints))
	for address, endpoint := range r.endpoints {
		snapshot[address] = endpoint.incarnation
	}
	return snapshot
}

func (r *ModelRegistry) updateModelOperationsForEndpoint(address string, expected *Endpoint, expectedIncarnation uint64, operations map[string]map[OperationType]bool) {
	models := make([]string, 0, len(operations))
	for model := range operations {
		models = append(models, model)
	}
	sort.Strings(models)
	r.updateModelsForEndpoint(address, expected, expectedIncarnation, models, operations, true)
}

func (r *ModelRegistry) updateModels(address string, models []string, operations map[string]map[OperationType]bool, catalogKnown bool) {
	r.updateModelsForEndpoint(address, nil, 0, models, operations, catalogKnown)
}

func (r *ModelRegistry) updateModelsForEndpoint(
	address string,
	expected *Endpoint,
	expectedIncarnation uint64,
	models []string,
	operations map[string]map[OperationType]bool,
	catalogKnown bool,
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ep, exists := r.endpoints[address]
	if !exists || (expected != nil && (ep != expected || ep.incarnation != expectedIncarnation)) {
		return
	}

	// Track old models for cleanup
	oldModels := make(map[string]bool)
	for m := range ep.models {
		oldModels[m] = true
	}

	// Update models
	for _, model := range models {
		if _, exists := ep.models[model]; !exists {
			ep.models[model] = newModelInfo(model)
		}
		if operations != nil {
			ep.models[model].Operations = cloneOperations(operations[model])
			if len(ep.models[model].Operations) == 0 {
				ep.models[model].OperationState = ModelOperationsTaskUnknown
			} else {
				ep.models[model].OperationState = ModelOperationsKnown
			}
		}
		delete(oldModels, model)

		// Update model index
		if r.models[model] == nil {
			r.models[model] = make([]*Endpoint, 0)
		}
		found := false
		for _, e := range r.models[model] {
			if e.address == address {
				found = true
				break
			}
		}
		if !found {
			r.models[model] = append(r.models[model], ep)
		}

		// Update metric
		modelLoaded.WithLabelValues(ep.pool, address, model).Set(1)
	}

	// Remove old models
	for model := range oldModels {
		delete(ep.models, model)
		// Remove from model index
		newEndpoints := make([]*Endpoint, 0)
		for _, e := range r.models[model] {
			if e.address != address {
				newEndpoints = append(newEndpoints, e)
			}
		}
		r.models[model] = newEndpoints
		modelLoaded.WithLabelValues(ep.pool, address, model).Set(0)
	}

	ep.lastSeen = time.Now()
	if catalogKnown {
		ep.catalogKnown = true
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

	info, exists := ep.models[model]
	if !exists {
		info = newModelInfo(model)
		ep.models[model] = info
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
		if pool != "" && ep.pool != pool {
			continue
		}
		if r.isEndpointAvailableLocked(ep) {
			if operation != "" && !modelSupportsOperation(ep.models[model], operation) {
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
		if !ep.catalogKnown && r.isEndpointAvailableLocked(ep) {
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
	return r.poolConditionStatsWithin(pool, model, nil)
}

// poolConditionStatsWithin evaluates route conditions against an optional
// immutable capability-lease endpoint set. This prevents a weighted route from
// selecting a healthy but undiscovered pool and rejecting after selection when
// another leased destination is available.
func (r *ModelRegistry) poolConditionStatsWithin(pool, model string, allowed map[string]endpointRef) PoolConditionStats {
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
		if allowed != nil {
			expected, ok := allowed[ep.address]
			if !ok || expected.endpoint != ep || expected.incarnation != ep.incarnation ||
				expected.breaker != r.circuitBreakers[ep.address] {
				continue
			}
		}

		stats.HealthyEndpoints++
		totalQueueDepth += int64(atomic.LoadInt32(&ep.runtime.queueDepth))

		if allowed != nil {
			// Inclusion in a model/task/operation lease is the caller-authorized proof that
			// this endpoint loaded the model, even when the background service
			// credential cannot see the same tenant-scoped catalog entry.
			stats.ModelLoaded = true
		}
		info, exists := ep.models[model]
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

func (r *ModelRegistry) tryAcquireEndpoint(expected endpointRef) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if expected.endpoint == nil || expected.incarnation == 0 || expected.breaker == nil {
		return false
	}
	current := r.endpoints[expected.endpoint.address]
	if current != expected.endpoint || current.incarnation != expected.incarnation ||
		r.circuitBreakers[current.address] != expected.breaker || !r.isEndpointAvailableLocked(current) {
		return false
	}
	return expected.breaker.TryAcquire()
}

func (r *ModelRegistry) isEndpointAvailableLocked(ep *Endpoint) bool {
	if ep == nil || !ep.healthy {
		return false
	}
	cb := r.circuitBreakers[ep.address]
	if cb == nil {
		return false
	}
	return cb.CanAttempt()
}

// RefreshEndpoint fetches current model list and health from an endpoint
func (r *ModelRegistry) RefreshEndpoint(ctx context.Context, address string) error {
	r.mu.RLock()
	expected := r.endpoints[address]
	var healthURL string
	var expectedIncarnation uint64
	if expected != nil {
		healthURL = expected.healthURL
		expectedIncarnation = expected.incarnation
	}
	r.mu.RUnlock()
	if expected == nil {
		return errors.New("inference endpoint is no longer registered")
	}
	authorization := r.upstreamAuthorizationValue()
	if healthURL != "" {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
		if err != nil {
			r.markEndpointHealth(address, expected, expectedIncarnation, false)
			return err
		}
		if authorization != "" {
			req.Header.Set("Authorization", authorization)
		}
		resp, err := r.client.Do(req)
		if err != nil {
			r.markEndpointHealth(address, expected, expectedIncarnation, false)
			return err
		}
		_ = resp.Body.Close()
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusBadRequest {
			r.markEndpointHealth(address, expected, expectedIncarnation, false)
			return fmt.Errorf("health check returned %d", resp.StatusCode)
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, address+"/ai/v1/models", nil)
	if err != nil {
		r.markEndpointHealth(address, expected, expectedIncarnation, false)
		return err
	}
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		r.markEndpointHealth(address, expected, expectedIncarnation, false)
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		r.markEndpointHealth(address, expected, expectedIncarnation, false)
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxModelCatalogBytes+1))
	if err != nil || len(body) > maxModelCatalogBytes {
		r.markEndpointHealth(address, expected, expectedIncarnation, false)
		return errors.New("upstream model catalog is unreadable or too large")
	}
	models, err := extractModelOperations(bytes.NewReader(body))
	if err != nil {
		r.markEndpointHealth(address, expected, expectedIncarnation, false)
		return err
	}
	r.updateModelOperationsForEndpoint(address, expected, expectedIncarnation, models)
	r.markEndpointHealth(address, expected, expectedIncarnation, true)
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
	"rerankers":    {"rerank", "rerank_multimodal"},
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
		for _, operation := range operations {
			if operation == "chunk" {
				name = canonicalChunkModel(name)
				break
			}
		}
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
	r.markEndpointHealth(address, nil, 0, true)
}

func (r *ModelRegistry) markUnhealthy(address string) {
	r.markEndpointHealth(address, nil, 0, false)
}

func (r *ModelRegistry) markEndpointHealth(address string, expected *Endpoint, expectedIncarnation uint64, healthy bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if ep, exists := r.endpoints[address]; exists {
		if expected != nil && (ep != expected || ep.incarnation != expectedIncarnation) {
			return
		}
		ep.healthy = healthy
		value := float64(0)
		if healthy {
			value = 1
		}
		endpointHealth.WithLabelValues(ep.pool, address).Set(value)
	}
}

func (r *ModelRegistry) circuitBreaker(address string) *CircuitBreaker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.circuitBreakers[address]
}

// EndpointSnapshots returns detached endpoint values in stable address order.
func (r *ModelRegistry) EndpointSnapshots() []EndpointSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	snapshots := make([]EndpointSnapshot, 0, len(r.endpoints))
	for _, endpoint := range r.endpoints {
		models := make([]ModelSnapshot, 0, len(endpoint.models))
		for _, model := range endpoint.models {
			operations := make([]OperationType, 0, len(model.Operations))
			for operation := range model.Operations {
				operations = append(operations, operation)
			}
			sort.Slice(operations, func(i, j int) bool { return operations[i] < operations[j] })
			models = append(models, ModelSnapshot{
				Name:           model.Name,
				LoadedAt:       model.LoadedAt,
				RequestsTotal:  model.RequestsTotal,
				AvgLatencyMs:   model.AvgLatencyMs,
				OperationState: model.OperationState,
				Operations:     operations,
			})
		}
		sort.Slice(models, func(i, j int) bool { return models[i].Name < models[j].Name })
		snapshots = append(snapshots, EndpointSnapshot{
			Address:      endpoint.address,
			HealthURL:    endpoint.healthURL,
			Pool:         endpoint.pool,
			WorkloadType: endpoint.workloadType,
			Models:       models,
			CatalogKnown: endpoint.catalogKnown,
			QueueDepth:   atomic.LoadInt32(&endpoint.runtime.queueDepth),
			LastSeen:     endpoint.lastSeen,
			Healthy:      endpoint.healthy,
			Connections:  atomic.LoadInt32(&endpoint.runtime.connections),
			Incarnation:  endpoint.incarnation,
		})
	}
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].Address < snapshots[j].Address })
	return snapshots
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

// AcquireEndpoint selects and reserves an endpoint incarnation. Callers must
// complete the returned lease exactly once.
func (r *Router) AcquireEndpoint(ctx context.Context, model string, pool string, workloadType WorkloadType, excluded map[string]bool, operations ...OperationType) (*EndpointLease, error) {
	reservation, err := r.routeRequestWithin(ctx, model, pool, workloadType, excluded, nil, operations...)
	if err != nil {
		return nil, err
	}
	return &EndpointLease{reservation: reservation}, nil
}

// routeRequestWithin selects only from the endpoint identities covered by a
// capability lease. A nil allowed map preserves legacy, unfenced routing.
func (r *Router) routeRequestWithin(ctx context.Context, model string, pool string, workloadType WorkloadType, excluded map[string]bool, allowed map[string]endpointRef, operations ...OperationType) (endpointReservation, error) {
	_ = ctx
	candidates := r.resolveEndpointCandidateRefsWithin(model, pool, excluded, allowed, operations...)
	if len(candidates) == 0 {
		return endpointReservation{}, fmt.Errorf("no healthy endpoints available for model %s", model)
	}

	for len(candidates) > 0 {
		endpoints := make([]*Endpoint, len(candidates))
		for i := range candidates {
			endpoints[i] = candidates[i].endpoint
		}
		endpoint, err := r.selectEndpoint(model, workloadType, endpoints, true)
		if err != nil {
			return endpointReservation{}, err
		}
		var selected endpointRef
		for _, candidate := range candidates {
			if candidate.endpoint == endpoint {
				selected = candidate
				break
			}
		}
		if r.registry.tryAcquireEndpoint(selected) {
			return endpointReservation{ref: selected}, nil
		}

		filtered := make([]endpointRef, 0, len(candidates)-1)
		for _, candidate := range candidates {
			if candidate.endpoint != endpoint {
				filtered = append(filtered, candidate)
			}
		}
		candidates = filtered
	}

	return endpointReservation{}, fmt.Errorf("no healthy endpoints available for model %s", model)
}

// ResolveEndpointCandidates returns currently eligible endpoint candidates without reserving them.
func (r *Router) ResolveEndpointCandidates(model string, pool string, excluded map[string]bool, operations ...OperationType) []*Endpoint {
	return r.resolveEndpointCandidatesWithin(model, pool, excluded, nil, operations...)
}

// resolveEndpointCandidatesWithin intersects current operation eligibility
// with the exact endpoint objects captured by a capability lease. Pointer
// identity prevents an address-reused replacement from inheriting a lease.
func (r *Router) resolveEndpointCandidatesWithin(model string, pool string, excluded map[string]bool, allowed map[string]endpointRef, operations ...OperationType) []*Endpoint {
	refs := r.resolveEndpointCandidateRefsWithin(model, pool, excluded, allowed, operations...)
	endpoints := make([]*Endpoint, len(refs))
	for i := range refs {
		endpoints[i] = refs[i].endpoint
	}
	return endpoints
}

func (r *Router) resolveEndpointCandidateRefsWithin(model string, pool string, excluded map[string]bool, allowed map[string]endpointRef, operations ...OperationType) []endpointRef {
	if allowed != nil {
		// A capability lease is an immutable, authorization-scoped proof that
		// these exact endpoint incarnations advertised the semantic task. Do not
		// re-filter it through the registry's service-credential catalog: that
		// global inventory can legitimately differ from the caller's catalog.
		r.registry.mu.RLock()
		endpoints := make([]endpointRef, 0, len(allowed))
		for address, expected := range allowed {
			endpoint := r.registry.endpoints[address]
			if endpoint == nil || endpoint != expected.endpoint || endpoint.incarnation != expected.incarnation ||
				r.registry.circuitBreakers[address] != expected.breaker ||
				(pool != "" && endpoint.pool != pool) || !r.registry.isEndpointAvailableLocked(endpoint) {
				continue
			}
			endpoints = append(endpoints, expected)
		}
		r.registry.mu.RUnlock()
		sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].endpoint.address < endpoints[j].endpoint.address })
		return filterExcludedEndpointRefs(endpoints, excluded)
	}

	var endpoints []*Endpoint
	if len(operations) <= 1 {
		var operation OperationType
		if len(operations) == 1 {
			operation = operations[0]
		}
		endpoints = r.registry.getAvailableEndpointsForModelOperation(model, pool, operation)
		if len(endpoints) == 0 && pool != "" && operation == "" {
			// Bootstrap inventory is usable only by legacy, task-unscoped callers.
			// Every executable inference route carries an operation and must fail
			// closed until the selected endpoint has advertised that exact
			// model/task pair. This keeps a cached capability decision from being
			// rebound to a newly joined, undiscovered node after topology churn.
			endpoints = r.registry.getBootstrapEndpointsForPool(pool)
		}
	} else {
		seen := make(map[*Endpoint]bool)
		for _, operation := range operations {
			for _, endpoint := range r.registry.getAvailableEndpointsForModelOperation(model, pool, operation) {
				if !seen[endpoint] {
					seen[endpoint] = true
					endpoints = append(endpoints, endpoint)
				}
			}
		}
	}

	refs := r.registry.endpointRefs(endpoints)
	filtered := filterExcludedEndpointRefs(refs, excluded)
	if len(filtered) > 0 {
		return filtered
	}
	return refs
}

func (r *ModelRegistry) endpointRefs(endpoints []*Endpoint) []endpointRef {
	r.mu.RLock()
	defer r.mu.RUnlock()
	refs := make([]endpointRef, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint == nil || r.endpoints[endpoint.address] != endpoint {
			continue
		}
		breaker := r.circuitBreakers[endpoint.address]
		if breaker == nil {
			continue
		}
		refs = append(refs, endpointRef{endpoint: endpoint, incarnation: endpoint.incarnation, breaker: breaker})
	}
	return refs
}

func filterExcludedEndpointRefs(endpoints []endpointRef, excluded map[string]bool) []endpointRef {
	if len(endpoints) == 0 || len(excluded) == 0 {
		return endpoints
	}
	filtered := make([]endpointRef, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint.endpoint == nil || excluded[endpoint.endpoint.address] {
			continue
		}
		filtered = append(filtered, endpoint)
	}
	return filtered
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
		if endpoint == nil || excluded[endpoint.address] {
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
		return atomic.LoadInt32(&sorted[i].runtime.connections) < atomic.LoadInt32(&sorted[j].runtime.connections)
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
		if atomic.LoadInt32(&ep.runtime.queueDepth) < maxQueue {
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
		epHash := r.hash(ep.address)
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
	bodyAdmission       *byteAdmission
	catalogAdmission    *byteAdmission
	refreshWake         chan struct{}
	capabilityLeaseMu   sync.Mutex
	capabilityLeases    map[string]proxyCapabilityLease
	capabilityLeaseByID map[[sha256.Size]byte]string
	verifiedSource      VerifiedSourceResolver
}

// VerifiedSourceResolver authenticates source-scoped routing attributes for an
// HTTP request. Raw organization, project, and API-key headers are deliberately
// not treated as trusted identity when no resolver is configured.
type VerifiedSourceResolver func(*http.Request) (VerifiedSource, error)

const defaultMaxProxyRequestBodyBytes int64 = 256 << 20
const defaultMaxProxyRetainedBodyBytes int64 = 768 << 20
const defaultMaxProxyRetainedCatalogBytes int64 = 256 << 20
const capabilityLeaseTTL = 5 * time.Minute
const maxCapabilityLeases = 1024
const capabilityTokenHeader = "X-Antfly-Capability-Token"
const capabilityRevisionHeader = "X-Antfly-Capability-Revision"
const capabilityStaleHeader = "X-Antfly-Capability-Stale"

type leasedEndpoint struct {
	incarnation uint64
	operations  map[OperationType]bool
}

type proxyCapabilityLease struct {
	identity            [sha256.Size]byte
	model               string
	task                string
	operation           OperationType
	routeGeneration     uint64
	descriptorRevision  string
	authorizationDigest [sha256.Size]byte
	endpoints           map[string]leasedEndpoint
	expiresAt           time.Time
}

// Config holds proxy configuration
type Config struct {
	ListenAddr              string
	DefaultPool             string
	RefreshInterval         time.Duration
	EnableRouteWatching     bool                   // Enable watching InferenceProxy CRs
	RouteWatchNamespace     string                 // Namespace to watch for routes (empty for all)
	RouteWatchKubeconfig    string                 // Optional kubeconfig path for route watching
	UpstreamAuthorization   string                 // Optional Authorization header value for upstream refreshes and requests
	MaxRequestBodyBytes     int64                  // Optional retained request-body ceiling; defaults to 256 MiB
	MaxRetainedBodyBytes    int64                  // Optional process-wide body+decode working-set ceiling; defaults to 768 MiB
	MaxRetainedCatalogBytes int64                  // Optional process-wide model-catalog ceiling; defaults to 256 MiB
	Logger                  *zap.Logger            // Optional logger (defaults to production logger)
	VerifiedSourceResolver  VerifiedSourceResolver // Optional authenticated source resolver shared by discovery and execution
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
		registry:            registry,
		router:              router,
		defaultPool:         cfg.DefaultPool,
		listenAddr:          cfg.ListenAddr,
		logger:              logger,
		refreshWake:         make(chan struct{}, 1),
		capabilityLeases:    make(map[string]proxyCapabilityLease),
		capabilityLeaseByID: make(map[[sha256.Size]byte]string),
		verifiedSource:      cfg.VerifiedSourceResolver,
	}
	p.maxRequestBodyBytes = cfg.MaxRequestBodyBytes
	if p.maxRequestBodyBytes <= 0 {
		p.maxRequestBodyBytes = defaultMaxProxyRequestBodyBytes
	}
	maxRetainedBodyBytes := cfg.MaxRetainedBodyBytes
	if maxRetainedBodyBytes <= 0 {
		maxRetainedBodyBytes = defaultMaxProxyRetainedBodyBytes
	}
	minimumBodyAdmission := proxyBodyAdmissionBytes(p.maxRequestBodyBytes)
	if maxRetainedBodyBytes < minimumBodyAdmission {
		maxRetainedBodyBytes = minimumBodyAdmission
	}
	p.bodyAdmission = newByteAdmission(maxRetainedBodyBytes)
	maxRetainedCatalogBytes := cfg.MaxRetainedCatalogBytes
	if maxRetainedCatalogBytes <= 0 {
		maxRetainedCatalogBytes = defaultMaxProxyRetainedCatalogBytes
	}
	p.catalogAdmission = newByteAdmission(maxRetainedCatalogBytes)

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
	apiMux.HandleFunc("/ai/v1/rerank_multimodal", p.handleRerankMultimodal)
	apiMux.HandleFunc("/ai/v1/extract", p.handleExtract)
	apiMux.HandleFunc("/ai/v1/rewrite", p.handleRewrite)
	apiMux.HandleFunc("/ai/v1/classify", p.handleClassify)
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

// handleRerankMultimodal preserves the concrete operation so route rules and
// capability leases can distinguish multimodal rerankers from text rerankers.
func (p *Proxy) handleRerankMultimodal(w http.ResponseWriter, r *http.Request) {
	p.proxyRequest(w, r, "rerank_multimodal")
}

func (p *Proxy) handleExtract(w http.ResponseWriter, r *http.Request) {
	p.proxyRequest(w, r, "extract")
}

func (p *Proxy) handleRewrite(w http.ResponseWriter, r *http.Request) {
	p.proxyRequest(w, r, "rewrite")
}

func (p *Proxy) handleClassify(w http.ResponseWriter, r *http.Request) {
	p.proxyRequest(w, r, "classify")
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

func requestHeaderMap(header http.Header) map[string]string {
	result := make(map[string]string, len(header))
	for name, values := range header {
		if len(values) != 0 {
			result[name] = values[0]
		}
	}
	return result
}

func (p *Proxy) routingContextForRequest(r *http.Request, timestamp time.Time) (RoutingContext, error) {
	headers := requestHeaderMap(r.Header)
	source := VerifiedSource{
		// Table is a request routing attribute, not authenticated caller
		// identity, and retains its legacy header fallback.
		Table: headerValue(headers, "X-Antfly-Source-Table", "X-Antfly-Table"),
	}
	if p.verifiedSource != nil {
		verified, err := p.verifiedSource(r)
		if err != nil {
			return RoutingContext{}, err
		}
		if verified.Table == "" {
			verified.Table = source.Table
		}
		source = verified
	}
	return RoutingContext{
		Headers:      headers,
		Source:       source,
		ExplicitPool: strings.TrimSpace(headerValue(headers, "X-Antfly-Inference-Pool")),
		Timestamp:    timestamp,
	}, nil
}

type catalogEndpointCohort struct {
	endpoints       []*Endpoint
	routeGeneration uint64
}

// catalogEndpointCohortForRequest returns the pools reachable by one concrete
// operation. A task family is not a routing identity: generate, generate.batch,
// and chat.completions can legitimately have different policies and executors.
func (p *Proxy) catalogEndpointCohortForRequest(routing RoutingContext, model string, operation OperationType) catalogEndpointCohort {
	var pools []string
	seenPools := make(map[string]bool)
	cohort := p.router.RouteManager().PotentialCohortFor(routing.routeRequest(operation, model))
	for _, candidate := range cohort.Pools {
		if !seenPools[candidate] {
			seenPools[candidate] = true
			pools = append(pools, candidate)
		}
	}
	// Without a definite route, this operation can fall through to its explicit
	// pool and then the process default. A terminal reject contributes neither.
	fallbackPool := firstNonEmpty(strings.TrimSpace(routing.ExplicitPool), p.defaultPool)
	if !cohort.Terminal && fallbackPool != "" && !seenPools[fallbackPool] {
		seenPools[fallbackPool] = true
		pools = append(pools, fallbackPool)
	}
	seen := make(map[string]bool)
	var endpoints []*Endpoint
	for _, pool := range pools {
		for _, endpoint := range p.registry.GetEndpointsForPool(pool) {
			if endpoint != nil && !seen[endpoint.address] {
				seen[endpoint.address] = true
				endpoints = append(endpoints, endpoint)
			}
		}
	}
	return catalogEndpointCohort{endpoints: endpoints, routeGeneration: cohort.Generation}
}

func (p *Proxy) catalogEndpointsForRequest(routing RoutingContext, model string, operation OperationType) []*Endpoint {
	return p.catalogEndpointCohortForRequest(routing, model, operation).endpoints
}

const maxModelCatalogBytes = 8 << 20
const maxMergedModelCatalogBytes = 32 << 20
const maxConcurrentModelCatalogRequests = 8
const maxModelCatalogWorkingBytes = 3 * maxModelCatalogBytes

func modelCatalogFanoutPlan(endpointCount int, retainedByteLimit int64) (workerCount int, reservationBytes int64, err error) {
	if endpointCount <= 0 {
		return 0, 0, nil
	}
	workingBudget := retainedByteLimit - int64(maxMergedModelCatalogBytes)
	if workingBudget < int64(maxModelCatalogWorkingBytes) {
		return 0, 0, errByteAdmissionRequestTooLarge
	}
	workersByMemory := min(workingBudget/int64(maxModelCatalogWorkingBytes), int64(maxConcurrentModelCatalogRequests))
	workerCount = min(endpointCount, int(workersByMemory))
	reservationBytes = int64(maxMergedModelCatalogBytes) + int64(workerCount)*int64(maxModelCatalogWorkingBytes)
	return workerCount, reservationBytes, nil
}

// handleModels publishes a conservative catalog for one routing scope. Antfly
// clients provide model+task query parameters, allowing the proxy to aggregate
// exactly the operation-eligible endpoints in the matching route cohort.
// Unscoped compatibility requests remain pool-scoped rather than leaking a
// cluster-wide union that the subsequent request router cannot honor.
func (p *Proxy) handleModels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	model := strings.TrimSpace(r.URL.Query().Get("model"))
	task := strings.TrimSpace(r.URL.Query().Get("task"))
	operation := strings.TrimSpace(r.URL.Query().Get("operation"))
	taskScope, taskErr := catalogTaskScopeForOperation(task, operation)
	if taskErr != nil || (model == "") != (taskScope.Operation == "") {
		http.Error(w, "model and a valid task must be provided together", http.StatusBadRequest)
		return
	}
	routing, err := p.routingContextForRequest(r, time.Now())
	if err != nil {
		http.Error(w, "source authentication failed", http.StatusUnauthorized)
		return
	}
	pool := routing.ExplicitPool
	if pool == "" && model == "" {
		pool = p.defaultPool
	}
	authorization := p.upstreamAuthorizationForRequest(r)
	var endpoints []*Endpoint
	var routeGeneration uint64
	if model != "" {
		// Discover only the pools the matching route can actually select. Mixing
		// unrelated pools forces a conservative descriptor to inherit the weakest
		// cluster-wide limits even though execution can never reach those nodes.
		// Caller-scoped upstream catalogs still provide the authorization proof;
		// this is only a route-cohort prefilter.
		routing.ExplicitPool = pool
		cohort := p.catalogEndpointCohortForRequest(routing, model, taskScope.Operation)
		endpoints = cohort.endpoints
		routeGeneration = cohort.routeGeneration
	} else if pool == "" {
		endpoints = p.registry.GetAvailableEndpoints()
	} else {
		endpoints = p.registry.GetEndpointsForPool(pool)
	}
	type catalogTarget struct {
		endpoint    *Endpoint
		address     string
		incarnation uint64
	}
	targets := make([]catalogTarget, 0, len(endpoints))
	p.registry.mu.RLock()
	for _, endpoint := range endpoints {
		current := p.registry.endpoints[endpoint.address]
		if current == endpoint {
			targets = append(targets, catalogTarget{
				endpoint:    endpoint,
				address:     endpoint.address,
				incarnation: endpoint.incarnation,
			})
		}
	}
	p.registry.mu.RUnlock()
	if len(targets) == 0 {
		http.Error(w, "no operation-eligible inference endpoints", http.StatusServiceUnavailable)
		return
	}
	// Turn the process-wide memory ceiling into a concurrency limit. The merged
	// catalog and each active worker's raw body, RawMessage copy, and transient
	// task-inventory parse can coexist; endpoints beyond the admitted worker count
	// are processed by those same workers sequentially.
	workerCount, catalogBytes, planErr := modelCatalogFanoutPlan(len(targets), p.catalogAdmission.Limit())
	if planErr != nil {
		http.Error(w, "inference model catalog memory limit cannot admit one worker", http.StatusServiceUnavailable)
		return
	}
	if err := p.catalogAdmission.Acquire(r.Context(), catalogBytes); err != nil {
		status := http.StatusServiceUnavailable
		if !errors.Is(err, errByteAdmissionRequestTooLarge) {
			status = http.StatusRequestTimeout
		}
		http.Error(w, "inference model catalog admission unavailable", status)
		return
	}
	defer p.catalogAdmission.Release(catalogBytes)

	type catalogResult struct {
		target   catalogTarget
		catalog  map[string]json.RawMessage
		eligible bool
		err      error
	}
	results := make(chan catalogResult)
	jobs := make(chan catalogTarget, len(targets))
	for _, target := range targets {
		jobs <- target
	}
	close(jobs)
	for range workerCount {
		go func() {
			for target := range jobs {
				address := target.address
				request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, strings.TrimRight(address, "/")+"/ai/v1/models", nil)
				if err != nil {
					results <- catalogResult{err: err}
					continue
				}
				if authorization != "" {
					request.Header.Set("Authorization", authorization)
				}
				response, err := p.registry.client.Do(request)
				if err != nil {
					results <- catalogResult{err: err}
					continue
				}
				if response.StatusCode < 200 || response.StatusCode >= 300 {
					_ = response.Body.Close()
					results <- catalogResult{err: fmt.Errorf("upstream catalog returned %d", response.StatusCode)}
					continue
				}
				body, err := io.ReadAll(io.LimitReader(response.Body, maxModelCatalogBytes+1))
				_ = response.Body.Close()
				if err != nil || len(body) > maxModelCatalogBytes {
					results <- catalogResult{err: errors.New("upstream model catalog is unreadable or too large")}
					continue
				}
				var catalog map[string]json.RawMessage
				if err := json.Unmarshal(body, &catalog); err != nil {
					results <- catalogResult{err: err}
					continue
				}
				if model != "" && !catalogContainsTaskModel(catalog, taskScope, model) {
					results <- catalogResult{target: target}
					continue
				}
				results <- catalogResult{target: target, catalog: catalog, eligible: true}
			}
		}()
	}

	categories := map[string]map[string]json.RawMessage{}
	successes := 0
	failures := 0
	mergedTooLarge := false
	leaseEndpoints := make(map[string]leasedEndpoint, len(endpoints))
	for range targets {
		result := <-results
		if result.err != nil {
			failures++
			continue
		}
		if !result.eligible {
			continue
		}
		if p.registry.endpointAtIncarnation(result.target.address, result.target.incarnation) != result.target.endpoint {
			failures++
			continue
		}
		successes++
		// The caller-scoped catalog proves this model/task on the endpoint. The
		// lease deliberately authorizes only the concrete operation requested by
		// the planner, never every compatibility alias in the family.
		operations := map[OperationType]bool{taskScope.Operation: true}
		leaseEndpoints[result.target.address] = leasedEndpoint{
			incarnation: result.target.incarnation,
			operations:  operations,
		}
		if !mergedTooLarge {
			if model != "" {
				mergeScopedModelCatalog(categories, result.catalog, taskScope, model)
			} else {
				mergeModelCatalog(categories, result.catalog)
			}
			mergedTooLarge = modelCatalogEncodedBytes(categories) > maxMergedModelCatalogBytes
		}
	}
	if mergedTooLarge {
		http.Error(w, "merged inference model catalog is too large", http.StatusBadGateway)
		return
	}
	if successes == 0 {
		if failures > 0 {
			http.Error(w, "inference model catalog is unavailable", http.StatusBadGateway)
		} else {
			http.Error(w, "model task is unavailable on healthy inference endpoints", http.StatusServiceUnavailable)
		}
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
	responseBody, err := json.Marshal(response)
	if err != nil {
		http.Error(w, "failed to encode merged inference model catalog", http.StatusInternalServerError)
		return
	}
	descriptorDigest := sha256.Sum256(responseBody)
	descriptorRevision := hex.EncodeToString(descriptorDigest[:])
	if model != "" {
		token, err := p.issueCapabilityLease(
			model,
			task,
			taskScope.Operation,
			routeGeneration,
			descriptorRevision,
			authorization,
			leaseEndpoints,
		)
		if err != nil {
			if writeResolutionError(w, err) {
				return
			}
			if errors.Is(err, errCapabilityLeaseCapacity) {
				w.Header().Set("Retry-After", "1")
				http.Error(w, "inference capability lease capacity is temporarily exhausted", http.StatusServiceUnavailable)
			} else {
				http.Error(w, "failed to issue inference capability lease", http.StatusInternalServerError)
			}
			return
		}
		w.Header().Set(capabilityTokenHeader, token)
		w.Header().Set(capabilityRevisionHeader, descriptorRevision)
		w.Header().Set("Access-Control-Expose-Headers", capabilityTokenHeader+", "+capabilityRevisionHeader)
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(responseBody); err != nil {
		p.logger.Warn("failed to encode merged inference model catalog", zap.Error(err))
	}
}

func (p *Proxy) issueCapabilityLease(model, task string, operation OperationType, routeGeneration uint64, descriptorRevision, authorization string, endpoints map[string]leasedEndpoint) (string, error) {
	if p.router.RouteManager().Generation() != routeGeneration {
		return "", staleCapabilityResolutionError("inference routing policy changed during capability discovery")
	}
	authorizationDigest := sha256.Sum256([]byte(authorization))
	identity := capabilityLeaseIdentity(model, task, operation, routeGeneration, descriptorRevision, authorizationDigest, endpoints)
	allowed := make(map[string]leasedEndpoint, len(endpoints))
	for address, endpoint := range endpoints {
		allowed[address] = leasedEndpoint{
			incarnation: endpoint.incarnation,
			operations:  cloneOperations(endpoint.operations),
		}
	}
	now := time.Now()
	// Registry topology is the outer lock for lease issuance. This establishes a
	// real linearization point: every endpoint in the published lease is still
	// the exact incarnation that answered discovery when the token is minted.
	p.registry.mu.RLock()
	defer p.registry.mu.RUnlock()
	endpointIncarnations := p.registry.endpointIncarnationSnapshotLocked()
	if !leasedEndpointsMatchIncarnations(allowed, endpointIncarnations) {
		return "", staleCapabilityResolutionError("inference endpoint topology changed during capability discovery")
	}
	p.capabilityLeaseMu.Lock()
	defer p.capabilityLeaseMu.Unlock()
	// Recheck after acquiring the table lock. An old discovery waiter must not
	// purge leases issued for a newer generation while it was waiting here.
	if p.router.RouteManager().Generation() != routeGeneration {
		return "", staleCapabilityResolutionError("inference routing policy changed during capability discovery")
	}
	for token, lease := range p.capabilityLeases {
		if !now.Before(lease.expiresAt) || lease.routeGeneration != routeGeneration ||
			!capabilityLeaseEndpointIncarnationsMatch(lease, endpointIncarnations) {
			p.deleteCapabilityLeaseLocked(token, lease)
		}
	}
	var token string
	if existingToken, ok := p.capabilityLeaseByID[identity]; ok {
		if lease, live := p.capabilityLeases[existingToken]; live {
			// Refresh the same immutable capability snapshot in place. Clients poll
			// much more frequently than the execution lease TTL; minting a token per
			// poll turns bounded safety state into an availability leak.
			lease.expiresAt = now.Add(capabilityLeaseTTL)
			p.capabilityLeases[existingToken] = lease
			token = existingToken
		} else {
			delete(p.capabilityLeaseByID, identity)
		}
	}
	if token == "" && len(p.capabilityLeases) >= maxCapabilityLeases {
		// Never invalidate an unexpired lease from the current routing generation
		// to admit a newer caller. Returning bounded backpressure preserves the
		// planner/executor contract for work that has already been admitted.
		return "", errCapabilityLeaseCapacity
	}
	if token == "" {
		var random [32]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", err
		}
		token = hex.EncodeToString(random[:])
		p.capabilityLeases[token] = proxyCapabilityLease{
			identity:            identity,
			model:               model,
			task:                task,
			operation:           operation,
			routeGeneration:     routeGeneration,
			descriptorRevision:  descriptorRevision,
			authorizationDigest: authorizationDigest,
			endpoints:           allowed, expiresAt: now.Add(capabilityLeaseTTL),
		}
		p.capabilityLeaseByID[identity] = token
	}
	// Establish issuance at the captured routing generation. If policy changed
	// while the lease table was being updated, retract the now-dead token instead
	// of publishing a capability response that can never execute.
	if p.router.RouteManager().Generation() != routeGeneration {
		p.deleteCapabilityLeaseLocked(token, p.capabilityLeases[token])
		return "", staleCapabilityResolutionError("inference routing policy changed during capability discovery")
	}
	return token, nil
}

func (p *Proxy) deleteCapabilityLeaseLocked(token string, lease proxyCapabilityLease) {
	delete(p.capabilityLeases, token)
	if p.capabilityLeaseByID[lease.identity] == token {
		delete(p.capabilityLeaseByID, lease.identity)
	}
}

func capabilityLeaseIdentity(model, task string, operation OperationType, routeGeneration uint64, revision string, authorizationDigest [sha256.Size]byte, endpoints map[string]leasedEndpoint) [sha256.Size]byte {
	h := sha256.New()
	writeField := func(value string) {
		_, _ = fmt.Fprintf(h, "%d:", len(value))
		_, _ = h.Write([]byte(value))
	}
	writeField(model)
	writeField(task)
	writeField(string(operation))
	writeField(strconv.FormatUint(routeGeneration, 10))
	writeField(revision)
	_, _ = h.Write(authorizationDigest[:])
	addresses := make([]string, 0, len(endpoints))
	for address := range endpoints {
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)
	for _, address := range addresses {
		endpoint := endpoints[address]
		writeField(address)
		writeField(strconv.FormatUint(endpoint.incarnation, 10))
		operations := make([]string, 0, len(endpoint.operations))
		for operation, enabled := range endpoint.operations {
			if enabled {
				operations = append(operations, string(operation))
			}
		}
		sort.Strings(operations)
		for _, operation := range operations {
			writeField(operation)
		}
	}
	var identity [sha256.Size]byte
	copy(identity[:], h.Sum(nil))
	return identity
}

func capabilityLeaseEndpointIncarnationsMatch(lease proxyCapabilityLease, live map[string]uint64) bool {
	return leasedEndpointsMatchIncarnations(lease.endpoints, live)
}

func leasedEndpointsMatchIncarnations(endpoints map[string]leasedEndpoint, live map[string]uint64) bool {
	for address, expected := range endpoints {
		if live[address] != expected.incarnation || expected.incarnation == 0 {
			return false
		}
	}
	return true
}

func (p *Proxy) validateCapabilityLease(token, revision, model string, operation OperationType, authorization string) error {
	_, err := p.capabilityLeaseEndpoints(token, revision, model, operation, sha256.Sum256([]byte(authorization)))
	return err
}

func (p *Proxy) capabilityLeaseEndpoints(token, revision, model string, operation OperationType, authorizationDigest [sha256.Size]byte) (map[string]endpointRef, error) {
	lease, err := p.validatedCapabilityLease(token, revision, model, operation, authorizationDigest)
	return lease.endpoints, err
}

type validatedCapabilityLease struct {
	endpoints       map[string]endpointRef
	routeGeneration uint64
	scoped          bool
}

func (p *Proxy) validatedCapabilityLease(token, revision, model string, operation OperationType, authorizationDigest [sha256.Size]byte) (validatedCapabilityLease, error) {
	if token == "" {
		if revision != "" {
			return validatedCapabilityLease{}, staleCapabilityResolutionError("inference capability lease is incomplete")
		}
		return validatedCapabilityLease{}, nil
	}
	now := time.Now()
	p.capabilityLeaseMu.Lock()
	lease, ok := p.capabilityLeases[token]
	if ok && !now.Before(lease.expiresAt) {
		p.deleteCapabilityLeaseLocked(token, lease)
		ok = false
	}
	p.capabilityLeaseMu.Unlock()
	if !ok || lease.model != model || lease.task != semanticTaskForOperation(operation) || lease.operation != operation ||
		lease.descriptorRevision == "" || lease.descriptorRevision != revision ||
		lease.authorizationDigest != authorizationDigest {
		return validatedCapabilityLease{}, staleCapabilityResolutionError("inference capability lease is stale")
	}
	if lease.routeGeneration != p.router.RouteManager().Generation() {
		p.deleteCapabilityLeaseIfIdentityMatches(token, lease.identity)
		return validatedCapabilityLease{}, staleCapabilityResolutionError("inference capability lease is stale")
	}
	allowed := make(map[string]endpointRef, len(lease.endpoints))
	for address, expected := range lease.endpoints {
		if !expected.operations[operation] {
			continue
		}
		endpoint, ok := p.registry.endpointRefAtIncarnation(address, expected.incarnation)
		if !ok {
			p.deleteCapabilityLeaseIfIdentityMatches(token, lease.identity)
			return validatedCapabilityLease{}, staleCapabilityResolutionError("inference capability lease is stale")
		}
		allowed[address] = endpoint
	}
	return validatedCapabilityLease{
		endpoints:       allowed,
		routeGeneration: lease.routeGeneration,
		scoped:          true,
	}, nil
}

func (p *Proxy) deleteCapabilityLeaseIfIdentityMatches(token string, identity [sha256.Size]byte) {
	p.capabilityLeaseMu.Lock()
	defer p.capabilityLeaseMu.Unlock()
	lease, ok := p.capabilityLeases[token]
	if ok && lease.identity == identity {
		p.deleteCapabilityLeaseLocked(token, lease)
	}
}

func semanticTaskForOperation(operation OperationType) string {
	switch operation {
	case "generate", "generate.batch", "chat.completions":
		return "generate"
	case "embed", "embeddings":
		return "embed"
	case "rerank", "rerank_multimodal":
		return "rerank"
	default:
		return string(operation)
	}
}

func semanticOperationsForTask(task string, fallback OperationType) []OperationType {
	switch task {
	case "generate":
		return []OperationType{"generate", "generate.batch", "chat.completions"}
	case "embed":
		return []OperationType{"embed", "embeddings"}
	case "rerank":
		return []OperationType{"rerank", "rerank_multimodal"}
	default:
		return []OperationType{fallback}
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

func catalogTaskScopeForOperation(rawTask, rawOperation string) (catalogTaskScope, error) {
	scope, err := catalogTaskScopeFor(rawTask)
	if err != nil || scope.Operation == "" || strings.TrimSpace(rawOperation) == "" {
		if strings.TrimSpace(rawOperation) != "" && scope.Operation == "" && err == nil {
			return catalogTaskScope{}, errors.New("operation requires an inference task")
		}
		return scope, err
	}
	requested := OperationType(strings.TrimSpace(rawOperation))
	valid := false
	for _, candidate := range semanticOperationsForTask(rawTask, scope.Operation) {
		if candidate == requested {
			valid = true
			break
		}
	}
	if !valid {
		return catalogTaskScope{}, fmt.Errorf("operation %q does not belong to inference task %q", rawOperation, rawTask)
	}
	scope.Operation = requested
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
	_, ok = catalogModelDescriptor(models, scope.Category, model)
	return ok
}

func canonicalCatalogModel(category, model string) string {
	if category == "chunkers" {
		return canonicalChunkModel(model)
	}
	return model
}

func catalogModelDescriptor(models map[string]json.RawMessage, category, model string) (json.RawMessage, bool) {
	canonical := canonicalCatalogModel(category, model)
	if descriptor, ok := models[canonical]; ok {
		return descriptor, true
	}
	for candidate, descriptor := range models {
		if canonicalCatalogModel(category, candidate) == canonical {
			return descriptor, true
		}
	}
	return nil, false
}

func modelCatalogEncodedBytes(categories map[string]map[string]json.RawMessage) int {
	total := 0
	for category, models := range categories {
		total += len(category)
		for model, descriptor := range models {
			model = canonicalCatalogModel(category, model)
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
			model = canonicalCatalogModel(category, model)
			descriptor = sanitizeModelDescriptor(descriptor)
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
	descriptor, ok := catalogModelDescriptor(models, scope.Category, model)
	if !ok {
		return
	}
	if target[scope.Category] == nil {
		target[scope.Category] = make(map[string]json.RawMessage)
	}
	model = canonicalCatalogModel(scope.Category, model)
	descriptor = sanitizeModelDescriptor(descriptor)
	if existing, duplicate := target[scope.Category][model]; duplicate {
		target[scope.Category][model] = conservativeModelDescriptor(existing, descriptor)
	} else {
		target[scope.Category][model] = append(json.RawMessage(nil), descriptor...)
	}
}

// Capability descriptors are admission contracts, so a malformed descriptor
// from even one eligible endpoint must poison the merged model instead of
// being silently weakened into a plausible-looking subset.
func sanitizeModelDescriptor(raw json.RawMessage) json.RawMessage {
	var descriptor map[string]any
	if json.Unmarshal(raw, &descriptor) != nil {
		return json.RawMessage(`{}`)
	}
	capabilities, found := descriptor["inference_capabilities"]
	if !found {
		return append(json.RawMessage(nil), raw...)
	}
	capabilityMap, ok := capabilities.(map[string]any)
	if !ok {
		return json.RawMessage(`{}`)
	}
	version, ok := inferenceCapabilitiesVersion(capabilityMap)
	if !ok {
		return json.RawMessage(`{}`)
	}
	task, ok := capabilityMap["task"].(string)
	if !ok || !exactTasks[task] || !validInferenceBatchCapabilities(capabilityMap, version) {
		return json.RawMessage(`{}`)
	}
	if version >= 3 {
		normalized, valid := conservativeInferenceCapabilities(capabilityMap, capabilityMap)
		if !valid {
			return json.RawMessage(`{}`)
		}
		descriptor["inference_capabilities"] = normalized
	}
	normalized, err := json.Marshal(descriptor)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return normalized
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
	if !aok || !bok {
		return nil, false
	}
	aTask, aok := a["task"].(string)
	bTask, bok := b["task"].(string)
	if !aok || !bok || !exactTasks[aTask] || aTask != bTask {
		return nil, false
	}
	aVersion, aok := inferenceCapabilitiesVersion(a)
	bVersion, bok := inferenceCapabilitiesVersion(b)
	if !aok || !bok {
		return nil, false
	}
	if !validInferenceBatchCapabilities(a, aVersion) || !validInferenceBatchCapabilities(b, bVersion) {
		return nil, false
	}
	if aVersion >= 3 && !validExactInferenceCapabilities(a, aVersion) ||
		bVersion >= 3 && !validExactInferenceCapabilities(b, bVersion) {
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
	aEncodedField := "max_encoded_bytes"
	bEncodedField := "max_encoded_bytes"
	if aVersion >= 2 {
		aEncodedField = "max_encoded_media_bytes"
	}
	if bVersion >= 2 {
		bEncodedField = "max_encoded_media_bytes"
	}
	aEncoded, aFound := aBatch[aEncodedField]
	bEncoded, bFound := bBatch[bEncodedField]
	if !aFound || !bFound {
		return nil, false
	}
	value, ok := conservativeOptionalLimit(aEncoded, bEncoded, aVersion < 2, bVersion < 2)
	if !ok {
		return nil, false
	}
	batch["max_encoded_media_bytes"] = value
	aPixels, aFound := aBatch["max_decoded_pixels"]
	bPixels, bFound := bBatch["max_decoded_pixels"]
	if !aFound || !bFound {
		return nil, false
	}
	value, ok = conservativeOptionalLimit(aPixels, bPixels, aVersion < 2, bVersion < 2)
	if !ok {
		return nil, false
	}
	batch["max_decoded_pixels"] = value
	aFailures, aok := aBatch["per_item_failures"].(bool)
	bFailures, bok := bBatch["per_item_failures"].(bool)
	if !aok || !bok {
		return nil, false
	}
	batch["per_item_failures"] = aFailures && bFailures
	version := 2
	result := map[string]any{"version": float64(version), "task": aTask, "batch": batch}
	if aVersion >= 3 && bVersion >= 3 {
		modalities, ok := intersectValidatedStringValues(a["input_modalities"], b["input_modalities"], exactModalities)
		if !ok || len(modalities) == 0 {
			return nil, false
		}
		mimes, ok := intersectValidatedMIMEValues(a["accepted_mime_types"], b["accepted_mime_types"])
		if !ok || len(mimes) == 0 {
			return nil, false
		}
		result["input_modalities"] = modalities
		result["accepted_mime_types"] = mimes
		for _, field := range []string{"input_granularity", "output", "result_cardinality", "prompt_policy"} {
			if a[field] == nil || !reflect.DeepEqual(a[field], b[field]) {
				return nil, false
			}
			result[field] = a[field]
		}
		_, aok := a["borrowed_attachments"].(bool)
		_, bok := b["borrowed_attachments"].(bool)
		if !aok || !bok {
			return nil, false
		}
		// The merged catalog is itself served over HTTP. Borrowing is a
		// property of a linked executor route, not of the model, and cannot be
		// preserved through this proxy even if every upstream describes its
		// local ABI as borrowed.
		result["borrowed_attachments"] = false
		version = 3
		if aVersion >= 4 && bVersion >= 4 {
			aLimits, aok := a["task_limits"].(map[string]any)
			bLimits, bok := b["task_limits"].(map[string]any)
			if !aok || !bok {
				return nil, false
			}
			limits := make(map[string]any)
			for _, field := range taskLimitFields {
				value, ok := conservativeOptionalLimit(aLimits[field], bLimits[field], false, false)
				if !ok {
					return nil, false
				}
				limits[field] = value
			}
			result["task_limits"] = limits
			version = 4
		}
		result["version"] = float64(version)
		if !validExactInferenceCapabilities(result, version) {
			return nil, false
		}
	}
	if !validInferenceBatchCapabilities(result, version) {
		return nil, false
	}
	return result, true
}

var exactTasks = map[string]bool{
	"read": true, "generate": true, "embed": true, "rerank": true, "chunk": true,
	"extract": true, "rewrite": true, "classify": true, "transcribe": true,
}
var exactModalities = map[string]bool{"text": true, "image": true, "audio": true, "document": true}
var canonicalBuiltInMIMETypes = map[string]bool{
	"text/plain": true, "application/json": true, "application/pdf": true,
	"image/png": true, "image/jpeg": true, "image/webp": true,
	"audio/wav": true, "audio/mpeg": true,
}

const (
	maxInferenceMIMEValues           = 24
	maxAdditionalInferenceMIMEValues = 16
	maxInferenceMIMEValueBytes       = 63
)

var exactGranularities = map[string]bool{"item": true, "chunk": true, "page": true, "document": true}
var exactOutputs = map[string]bool{
	"read_result": true, "generated_text": true, "embedding": true, "ranked_items": true,
	"chunks": true, "extraction": true, "rewritten_text": true, "classification": true,
	"transcription": true,
}
var exactCardinalities = map[string]bool{"one_per_item": true, "one_per_request": true}
var exactPromptPolicies = map[string]bool{"explicit": true, "model_default": true, "structured_schema": true}

var taskLimitFields = []string{
	"max_text_bytes_per_item", "max_input_tokens_per_item", "max_output_tokens_per_item",
	"max_candidates_per_request", "max_schema_bytes",
}

func validExactInferenceCapabilities(capabilities map[string]any, version int) bool {
	task, ok := capabilities["task"].(string)
	if !ok || !exactTasks[task] {
		return false
	}
	modalities, ok := validatedStringValues(capabilities["input_modalities"], exactModalities)
	if !ok || len(modalities) == 0 {
		return false
	}
	mimes, ok := validatedMIMEValues(capabilities["accepted_mime_types"])
	if !ok || len(mimes) == 0 {
		return false
	}
	for field, allowed := range map[string]map[string]bool{
		"input_granularity":  exactGranularities,
		"output":             exactOutputs,
		"result_cardinality": exactCardinalities,
		"prompt_policy":      exactPromptPolicies,
	} {
		value, ok := capabilities[field].(string)
		if !ok || !allowed[value] {
			return false
		}
	}
	expectedOutput := map[string]string{
		"read": "read_result", "generate": "generated_text", "embed": "embedding",
		"rerank": "ranked_items", "chunk": "chunks", "extract": "extraction",
		"rewrite": "rewritten_text", "classify": "classification", "transcribe": "transcription",
	}[task]
	if capabilities["output"] != expectedOutput {
		return false
	}
	expectedCardinality := "one_per_item"
	if task == "rerank" || task == "chunk" || task == "transcribe" {
		expectedCardinality = "one_per_request"
	}
	if capabilities["result_cardinality"] != expectedCardinality {
		return false
	}
	modalitySet := make(map[string]bool, len(modalities))
	for _, modality := range modalities {
		modalitySet[modality] = true
	}
	granularity := capabilities["input_granularity"].(string)
	if granularity == "document" && !modalitySet["document"] ||
		granularity == "page" && !modalitySet["image"] ||
		granularity == "chunk" && !modalitySet["text"] {
		return false
	}
	mimeModalities := make(map[string]bool, len(mimes))
	for _, mime := range mimes {
		modality := ""
		switch {
		case mime == "text/plain" || mime == "application/json":
			modality = "text"
		case mime == "application/pdf":
			modality = "document"
		case strings.HasPrefix(mime, "application/"):
			modality = "document"
		case strings.HasPrefix(mime, "image/"):
			modality = "image"
		case strings.HasPrefix(mime, "audio/"):
			modality = "audio"
		}
		if !modalitySet[modality] {
			return false
		}
		mimeModalities[modality] = true
	}
	for _, modality := range modalities {
		if !mimeModalities[modality] {
			return false
		}
	}
	_, ok = capabilities["borrowed_attachments"].(bool)
	if !ok {
		return false
	}
	if version >= 4 {
		limits, ok := capabilities["task_limits"].(map[string]any)
		if !ok || len(limits) != len(taskLimitFields) {
			return false
		}
		for _, field := range taskLimitFields {
			value, found := limits[field]
			if !found {
				return false
			}
			if value == nil {
				continue
			}
			number, valid := nonNegativeInteger(value)
			if !valid || number == 0 {
				return false
			}
		}
	}
	return true
}

func validatedMIMEValues(value any) ([]string, bool) {
	items, ok := stringValues(value)
	if !ok || len(items) == 0 || len(items) > maxInferenceMIMEValues {
		return nil, false
	}
	seen := make(map[string]bool, len(items))
	additional := 0
	for _, item := range items {
		mediaType, params, err := mime.ParseMediaType(item)
		if err != nil || len(item) > maxInferenceMIMEValueBytes || len(params) != 0 || mediaType != item ||
			item == "image/jpg" || item == "audio/x-wav" || seen[item] {
			return nil, false
		}
		if !canonicalBuiltInMIMETypes[item] {
			additional++
			if additional > maxAdditionalInferenceMIMEValues {
				return nil, false
			}
		}
		seen[item] = true
	}
	return items, true
}

func intersectValidatedMIMEValues(left, right any) ([]string, bool) {
	a, ok := validatedMIMEValues(left)
	if !ok {
		return nil, false
	}
	b, ok := validatedMIMEValues(right)
	if !ok {
		return nil, false
	}
	rightValues := make(map[string]bool, len(b))
	for _, value := range b {
		rightValues[value] = true
	}
	intersection := make([]string, 0, len(a))
	for _, value := range a {
		if rightValues[value] {
			intersection = append(intersection, value)
		}
	}
	sort.Strings(intersection)
	return intersection, true
}

func stringValues(value any) ([]string, bool) {
	switch typed := value.(type) {
	case []string:
		return typed, true
	case []any:
		items := make([]string, 0, len(typed))
		for _, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, false
			}
			items = append(items, text)
		}
		return items, true
	default:
		return nil, false
	}
}

func validatedStringValues(value any, allowed map[string]bool) ([]string, bool) {
	var items []string
	switch typed := value.(type) {
	case []string:
		items = typed
	case []any:
		items = make([]string, 0, len(typed))
		for _, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, false
			}
			items = append(items, text)
		}
	default:
		return nil, false
	}
	seen := make(map[string]bool, len(items))
	result := make([]string, 0, len(items))
	for _, item := range items {
		if !allowed[item] || seen[item] {
			return nil, false
		}
		seen[item] = true
		result = append(result, item)
	}
	return result, true
}

func intersectValidatedStringValues(left, right any, allowed map[string]bool) ([]string, bool) {
	a, ok := validatedStringValues(left, allowed)
	if !ok {
		return nil, false
	}
	b, ok := validatedStringValues(right, allowed)
	if !ok {
		return nil, false
	}
	rightValues := make(map[string]bool, len(b))
	for _, value := range b {
		rightValues[value] = true
	}
	intersection := make([]string, 0, len(a))
	for _, value := range a {
		if rightValues[value] {
			intersection = append(intersection, value)
		}
	}
	sort.Strings(intersection)
	return intersection, true
}

func inferenceCapabilitiesVersion(capabilities map[string]any) (int, bool) {
	value, found := capabilities["version"]
	if !found {
		return 1, true
	}
	number, ok := nonNegativeInteger(value)
	if !ok || number < 1 || number > 4 {
		return 0, false
	}
	return int(number), true
}

func validBatchMode(mode string) bool {
	return mode == "none" || mode == "serial_compatibility" || mode == "native"
}

// Batch fields form one semantic admission contract. Validate each endpoint's
// complete contract before taking minima: intersecting individually plausible
// numbers must not launder an internally contradictory source descriptor.
func validInferenceBatchCapabilities(capabilities map[string]any, version int) bool {
	batch, ok := capabilities["batch"].(map[string]any)
	if !ok {
		return false
	}
	mode, ok := batch["mode"].(string)
	if !ok || !validBatchMode(mode) {
		return false
	}
	preferred, ok := nonNegativeInteger(batch["preferred_items"])
	if !ok || preferred == 0 {
		return false
	}
	maximum, ok := nonNegativeInteger(batch["max_items"])
	if !ok || maximum == 0 || preferred > maximum {
		return false
	}
	if mode == "none" && (preferred != 1 || maximum != 1) {
		return false
	}
	if _, ok := nonNegativeInteger(batch["max_media_parts_per_item"]); !ok {
		return false
	}
	encodedField := "max_encoded_bytes"
	if version >= 2 {
		encodedField = "max_encoded_media_bytes"
	}
	encoded, found := batch[encodedField]
	if !found {
		return false
	}
	if _, _, ok := optionalLimit(encoded, version < 2); !ok {
		return false
	}
	pixels, found := batch["max_decoded_pixels"]
	if !found {
		return false
	}
	if _, _, ok := optionalLimit(pixels, version < 2); !ok {
		return false
	}
	_, ok = batch["per_item_failures"].(bool)
	return ok
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

// Optional resource ceilings are nullable. Null means unknown and therefore
// remains unknown when any eligible endpoint cannot prove a safe ceiling.
// Only v1 used zero as an unknown sentinel; v2 preserves zero as disabled.
func conservativeOptionalLimit(left, right any, leftZeroUnknown, rightZeroUnknown bool) (any, bool) {
	a, aKnown, aok := optionalLimit(left, leftZeroUnknown)
	b, bKnown, bok := optionalLimit(right, rightZeroUnknown)
	if !aok || !bok {
		return nil, false
	}
	switch {
	case aKnown && bKnown:
		return min(a, b), true
	default:
		return nil, true
	}
}

func optionalLimit(value any, zeroUnknown bool) (float64, bool, bool) {
	if value == nil {
		return 0, false, true
	}
	number, ok := nonNegativeInteger(value)
	if !ok {
		return 0, false, false
	}
	return number, !zeroUnknown || number != 0, true
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
	sort.Strings(intersection)
	return intersection
}

func (p *Proxy) proxyRequest(w http.ResponseWriter, r *http.Request, operation string) {
	start := time.Now()
	routing, err := p.routingContextForRequest(r, start)
	if err != nil {
		http.Error(w, "source authentication failed", http.StatusUnauthorized)
		return
	}

	// Parse request to get model
	if r.ContentLength > p.maxRequestBodyBytes {
		http.Error(w, "inference request body is too large", http.StatusRequestEntityTooLarge)
		return
	}
	// The body remains replayable for retries while model extraction decodes a
	// second representation. Unknown-length requests initially reserve the hard
	// bound while streaming, then promptly return unused admission once their
	// exact retained size is known.
	retainedBodyBytes := p.maxRequestBodyBytes
	if r.ContentLength >= 0 {
		retainedBodyBytes = r.ContentLength
	}
	reservedAdmissionBytes := proxyBodyAdmissionBytes(retainedBodyBytes)
	if err := p.bodyAdmission.Acquire(r.Context(), reservedAdmissionBytes); err != nil {
		http.Error(w, "request canceled while waiting for inference body admission", http.StatusRequestTimeout)
		return
	}
	defer func() { p.bodyAdmission.Release(reservedAdmissionBytes) }()
	body, err := readProxyRequestBody(r.Body, r.ContentLength, p.maxRequestBodyBytes)
	if err != nil {
		if errors.Is(err, errProxyRequestBodyTooLarge) {
			http.Error(w, "inference request body is too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "failed to read request", http.StatusBadRequest)
		return
	}
	actualAdmissionBytes := proxyMaterializedBodyAdmissionBytes(int64(len(body)), int64(cap(body)))
	if actualAdmissionBytes < reservedAdmissionBytes {
		p.bodyAdmission.Release(reservedAdmissionBytes - actualAdmissionBytes)
		reservedAdmissionBytes = actualAdmissionBytes
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

	lease, err := p.acquireRequestResolution(r.Context(), OperationType(operation), model, routing)
	if err != nil {
		if writeResolutionError(w, err) {
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
		if writeResolutionError(w, err) {
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
			if writeResolutionError(w, err) {
				return
			}
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
			p.recordProxyMetrics(endpoint.pool, model, operation, start, "error")
			http.Error(w, fmt.Sprintf("proxy request failed: %v", reqErr), http.StatusBadGateway)
			return
		}

		lastResp = resp
		lastEndpoint = endpoint
		resp.Body = wrapForwardingBody(resp.Body, forwardingLease)
		if attempt+1 < attempts && shouldRetryStatus(matchedRoute, resp.StatusCode) {
			drainErr := copyResponse(io.Discard, resp)
			p.registry.RecordModelLatency(endpoint.address, model, time.Since(attemptStarted))
			lease.RecordFailure()
			if drainErr != nil {
				lastErr = drainErr
				lastEndpoint = endpoint
			}
			continue
		}

		streamErr := copyResponse(w, resp)
		p.registry.RecordModelLatency(endpoint.address, model, time.Since(attemptStarted))

		if resp.StatusCode >= 400 || streamErr != nil {
			lease.RecordFailure()
			p.recordProxyMetrics(endpoint.pool, model, operation, start, "error")
		} else {
			lease.RecordSuccess()
			p.recordProxyMetrics(endpoint.pool, model, operation, start, "success")
		}
		return
	}

	if lastResp != nil {
		status := "success"
		if lastResp.StatusCode >= 400 {
			status = "error"
		}
		if lastEndpoint != nil {
			p.recordProxyMetrics(lastEndpoint.pool, model, operation, start, status)
		}
		_ = copyResponse(w, lastResp)
		return
	}

	p.recordProxyMetrics(pool, model, operation, start, "error")
	http.Error(w, fmt.Sprintf("proxy request failed: %v", lastErr), http.StatusBadGateway)
}

var errProxyRequestBodyTooLarge = errors.New("inference request body is too large")
var errCapabilityLeaseCapacity = errors.New("inference capability lease capacity is exhausted")

// proxyBodyAdmissionBytes accounts for the retained replay body plus a
// two-body JSON decode allowance (batch slice metadata and decoded strings).
// Saturation keeps configuration arithmetic safe even when a caller supplies
// an unreasonable ceiling.
func proxyBodyAdmissionBytes(bodyBytes int64) int64 {
	if bodyBytes <= 0 {
		return 0
	}
	if bodyBytes > math.MaxInt64/3 {
		return math.MaxInt64
	}
	return bodyBytes * 3
}

func proxyMaterializedBodyAdmissionBytes(bodyBytes, bodyCapacityBytes int64) int64 {
	if bodyBytes <= 0 && bodyCapacityBytes <= 0 {
		return 0
	}
	if bodyBytes < 0 || bodyCapacityBytes < bodyBytes || bodyBytes > (math.MaxInt64-bodyCapacityBytes)/2 {
		return math.MaxInt64
	}
	return bodyCapacityBytes + 2*bodyBytes
}

func readProxyRequestBody(reader io.Reader, contentLength, maxBytes int64) ([]byte, error) {
	if contentLength > maxBytes {
		return nil, errProxyRequestBodyTooLarge
	}
	if maxBytes < 0 || uint64(maxBytes) >= uint64(^uint(0)>>1) {
		return nil, errProxyRequestBodyTooLarge
	}
	if contentLength >= 0 {
		body := make([]byte, int(contentLength))
		readBytes, err := io.ReadFull(reader, body)
		if err != nil {
			return nil, err
		}
		var extra [1]byte
		extraBytes, extraErr := io.ReadFull(reader, extra[:])
		if extraBytes != 0 {
			return nil, errProxyRequestBodyTooLarge
		}
		if extraErr != nil && !errors.Is(extraErr, io.EOF) {
			return nil, extraErr
		}
		return body[:readBytes], nil
	}

	// Grow explicitly so the retained capacity never exceeds maxBytes. ReadAll
	// may geometrically over-allocate past a LimitReader's bound, which would
	// make physical memory exceed the admission reserved above for a request at
	// the configured ceiling. The old and new buffers can briefly coexist while
	// growing, but that peak remains below the three-body streaming reservation.
	initialCapacity := min(maxBytes, 512)
	body := make([]byte, 0, int(initialCapacity))
	for {
		if int64(len(body)) == maxBytes {
			var extra [1]byte
			extraBytes, extraErr := io.ReadFull(reader, extra[:])
			if extraBytes != 0 {
				return nil, errProxyRequestBodyTooLarge
			}
			if extraErr == nil || errors.Is(extraErr, io.EOF) {
				return body, nil
			}
			return nil, extraErr
		}
		if len(body) == cap(body) {
			newCapacity := int64(cap(body)) * 2
			if newCapacity == 0 {
				newCapacity = 1
			}
			newCapacity = min(newCapacity, maxBytes)
			grown := make([]byte, len(body), int(newCapacity))
			copy(grown, body)
			body = grown
		}
		readBytes, readErr := io.ReadFull(reader, body[len(body):cap(body)])
		body = body[:len(body)+readBytes]
		if readErr == nil {
			continue
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			return body, nil
		}
		return nil, readErr
	}
}

func proxyRequestModel(body []byte, operation string) (string, error) {
	if operation == "chunk" {
		var request struct {
			Config *struct {
				Model string `json:"model"`
			} `json:"config"`
		}
		if err := json.Unmarshal(body, &request); err != nil {
			return "", err
		}
		if request.Config == nil || strings.TrimSpace(request.Config.Model) == "" {
			return "fixed", nil
		}
		return canonicalChunkModel(request.Config.Model), nil
	}
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

func canonicalChunkModel(model string) string {
	switch strings.TrimSpace(model) {
	case "", "fixed", "fixed_bert", "fixed_bpe", "fixed-bert-tokenizer":
		return "fixed"
	default:
		return strings.TrimSpace(model)
	}
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
	var ticker *time.Ticker
	var periodic <-chan time.Time
	if p.registry.refreshInterval > 0 {
		ticker = time.NewTicker(p.registry.refreshInterval)
		periodic = ticker.C
		defer ticker.Stop()
	}

	// Cover endpoints registered before Start/StartBackground. Later
	// registrations coalesce on refreshWake.
	p.refreshAllEndpoints(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-periodic:
			p.refreshAllEndpoints(ctx)
		case <-p.refreshWake:
			p.refreshAllEndpoints(ctx)
		}
	}
}

func (p *Proxy) requestEndpointRefresh() {
	select {
	case p.refreshWake <- struct{}{}:
	default:
		// One pending full refresh already covers every registration observed
		// before it snapshots the registry.
	}
}

func (p *Proxy) refreshAllEndpoints(ctx context.Context) {
	p.registry.mu.RLock()
	endpoints := make([]string, 0, len(p.registry.endpoints))
	for addr := range p.registry.endpoints {
		endpoints = append(endpoints, addr)
	}
	p.registry.mu.RUnlock()

	for _, addr := range endpoints {
		// A failed endpoint remains quarantined; other endpoints still refresh.
		_ = p.registry.RefreshEndpoint(ctx, addr)
	}

	p.registry.mu.RLock()
	for pool, eps := range p.registry.pools {
		var totalQueue int32
		for _, ep := range eps {
			totalQueue += atomic.LoadInt32(&ep.runtime.queueDepth)
		}
		queueDepth.WithLabelValues(pool).Set(float64(totalQueue))
	}
	p.registry.mu.RUnlock()
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
	p.requestEndpointRefresh()
}

// RegisterEndpointWithHealth adds an endpoint with a distinct operational health URL.
func (p *Proxy) RegisterEndpointWithHealth(address, healthURL, pool string, workloadType WorkloadType) {
	p.registry.RegisterEndpointWithHealth(address, healthURL, pool, workloadType)
	p.requestEndpointRefresh()
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

	targetURL, err := url.Parse(endpoint.address)
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
	if authorization := p.upstreamAuthorizationForRequest(r); authorization != "" {
		outReq.Header.Set("Authorization", authorization)
	}

	return p.registry.client.Do(outReq)
}

// Use one authorization policy for both model discovery and execution. A
// configured service credential takes precedence; otherwise the caller's
// scoped credential is forwarded unchanged to both upstream operations.
func (p *Proxy) upstreamAuthorizationForRequest(r *http.Request) string {
	if configured := p.registry.upstreamAuthorizationValue(); configured != "" {
		return configured
	}
	return strings.TrimSpace(r.Header.Get("Authorization"))
}

func (p *Proxy) upstreamAuthorizationForHeaders(headers map[string]string) string {
	if configured := p.registry.upstreamAuthorizationValue(); configured != "" {
		return configured
	}
	return strings.TrimSpace(headerValue(headers, "Authorization"))
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

func (p *Proxy) waitForQueuedDestination(ctx context.Context, route *Route, req *RouteRequest, allowed map[string]endpointRef) (string, error) {
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
			dest, err := p.router.RouteManager().selectDestinationWithin(route, req, p.registry, allowed)
			if err != nil {
				return "", err
			}
			if dest != nil {
				return dest.Pool, nil
			}
		}
	}
}
