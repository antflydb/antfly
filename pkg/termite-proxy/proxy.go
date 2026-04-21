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

// Package proxy implements a model-aware routing proxy for Termite TPU instances.
package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
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
			Name: "termite_proxy_requests_total",
			Help: "Total requests by pool, model, and status",
		},
		[]string{"pool", "model", "operation", "status"},
	)

	queueDepth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "termite_proxy_queue_depth",
			Help: "Current queue depth per pool",
		},
		[]string{"pool"},
	)

	requestLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "termite_proxy_request_duration_seconds",
			Help:    "Request latency by pool and model",
			Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		},
		[]string{"pool", "model", "operation"},
	)

	modelLoaded = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "termite_proxy_model_loaded",
			Help: "Whether a model is loaded on a Termite (1=loaded, 0=not loaded)",
		},
		[]string{"pool", "endpoint", "model"},
	)

	endpointHealth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "termite_proxy_endpoint_healthy",
			Help: "Whether an endpoint is healthy (1=healthy, 0=unhealthy)",
		},
		[]string{"pool", "endpoint"},
	)

	activeConnections = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "termite_proxy_active_connections",
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

// Endpoint represents a single Termite instance
type Endpoint struct {
	Address      string
	Pool         string
	WorkloadType WorkloadType
	Models       map[string]*ModelInfo
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
}

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
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	state := atomic.LoadInt32(&cb.state)
	switch state {
	case 0: // closed
		return true
	case 1: // open
		if time.Since(cb.lastFailure) > cb.timeout {
			// Try to become the single "tester" - only 1 allowed in half-open
			if atomic.CompareAndSwapInt32(&cb.halfOpenInFlight, 0, 1) {
				atomic.CompareAndSwapInt32(&cb.state, 1, 2) // transition to half-open
				return true
			}
			return false
		}
		return false
	case 2: // half-open
		// Already being tested, don't allow more
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

// ModelRegistry tracks which models are available on which Termites
type ModelRegistry struct {
	endpoints map[string]*Endpoint   // address -> endpoint
	models    map[string][]*Endpoint // model -> endpoints with model
	pools     map[string][]*Endpoint // pool -> endpoints in pool

	circuitBreakers map[string]*CircuitBreaker

	refreshInterval time.Duration
	client          *http.Client

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

// RegisterEndpoint adds or updates an endpoint
func (r *ModelRegistry) RegisterEndpoint(address, pool string, workloadType WorkloadType) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.endpoints[address]; !exists {
		r.endpoints[address] = &Endpoint{
			Address:      address,
			Pool:         pool,
			WorkloadType: workloadType,
			Models:       make(map[string]*ModelInfo),
			Healthy:      true,
			LastSeen:     time.Now(),
		}
		r.circuitBreakers[address] = NewCircuitBreaker(5, 30*time.Second)
	}

	// Add to pool index
	if r.pools[pool] == nil {
		r.pools[pool] = make([]*Endpoint, 0)
	}
	found := false
	for _, ep := range r.pools[pool] {
		if ep.Address == address {
			found = true
			break
		}
	}
	if !found {
		r.pools[pool] = append(r.pools[pool], r.endpoints[address])
	}
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
	pool := ep.Pool
	newPoolEndpoints := make([]*Endpoint, 0)
	for _, e := range r.pools[pool] {
		if e.Address != address {
			newPoolEndpoints = append(newPoolEndpoints, e)
		}
	}
	r.pools[pool] = newPoolEndpoints

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
			ep.Models[model] = &ModelInfo{
				Name:     model,
				LoadedAt: time.Now(),
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
}

// GetEndpointsForModel returns endpoints that have a specific model loaded
func (r *ModelRegistry) GetEndpointsForModel(model string) []*Endpoint {
	r.mu.RLock()
	defer r.mu.RUnlock()

	endpoints := r.models[model]
	result := make([]*Endpoint, 0, len(endpoints))
	for _, ep := range endpoints {
		if ep.Healthy && r.circuitBreakers[ep.Address].Allow() {
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
		if ep.Healthy && r.circuitBreakers[ep.Address].Allow() {
			result = append(result, ep)
		}
	}
	return result
}

// RefreshEndpoint fetches current model list and health from an endpoint
func (r *ModelRegistry) RefreshEndpoint(ctx context.Context, address string) error {
	resp, err := r.client.Get(address + "/api/models")
	if err != nil {
		r.markUnhealthy(address)
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		r.markUnhealthy(address)
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	var modelsResp struct {
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&modelsResp); err != nil {
		return err
	}

	models := make([]string, len(modelsResp.Models))
	for i, m := range modelsResp.Models {
		models[i] = m.Name
	}

	r.UpdateModels(address, models)
	r.markHealthy(address)
	return nil
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
func (r *Router) RouteRequest(ctx context.Context, model string, pool string, workloadType WorkloadType) (*Endpoint, error) {
	var endpoints []*Endpoint

	// First try to find endpoints with the model already loaded
	endpoints = r.registry.GetEndpointsForModel(model)

	// Filter by pool if specified
	if pool != "" && len(endpoints) > 0 {
		filtered := make([]*Endpoint, 0)
		for _, ep := range endpoints {
			if ep.Pool == pool {
				filtered = append(filtered, ep)
			}
		}
		if len(filtered) > 0 {
			endpoints = filtered
		}
	}

	// If no endpoints with model, fall back to pool endpoints
	if len(endpoints) == 0 && pool != "" {
		endpoints = r.registry.GetEndpointsForPool(pool)
	}

	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no healthy endpoints available for model %s", model)
	}

	// Apply routing strategy based on workload type
	switch workloadType {
	case WorkloadTypeReadHeavy:
		return r.consistentHashWithLeastLoaded(endpoints, model)
	case WorkloadTypeWriteHeavy:
		return r.leastLoaded(endpoints)
	case WorkloadTypeBurst:
		return r.roundRobinWithQueueAwareness(endpoints, 50)
	default:
		return r.leastLoaded(endpoints)
	}
}

// RouteManager returns the route manager for advanced routing
func (r *Router) RouteManager() *RouteManager {
	return r.routeManager
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

// roundRobinWithQueueAwareness distributes load but respects queue limits
func (r *Router) roundRobinWithQueueAwareness(endpoints []*Endpoint, maxQueue int32) (*Endpoint, error) {
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
	return available[0], nil
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

	defaultPool string
	listenAddr  string
}

// Config holds proxy configuration
type Config struct {
	ListenAddr           string
	DefaultPool          string
	RefreshInterval      time.Duration
	EnableRouteWatching  bool        // Enable watching TermiteRoute CRs
	RouteWatchNamespace  string      // Namespace to watch for routes (empty for all)
	RouteWatchKubeconfig string      // Optional kubeconfig path for route watching
	Logger               *zap.Logger // Optional logger (defaults to production logger)
}

// NewProxy creates a new Proxy
func NewProxy(cfg Config) *Proxy {
	registry := NewModelRegistry(cfg.RefreshInterval)
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
	apiMux.HandleFunc("/api/embed", p.handleEmbed)
	apiMux.HandleFunc("/api/chunk", p.handleChunk)
	apiMux.HandleFunc("/api/rerank", p.handleRerank)
	apiMux.HandleFunc("/healthz", p.handleHealth)
	apiMux.HandleFunc("/readyz", p.handleReady)

	p.server = &http.Server{
		Addr:              p.listenAddr,
		Handler:           apiMux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Start background refresh
	go p.refreshLoop(ctx)

	// Start RouteWatcher if configured
	if p.routeWatcher != nil {
		go func() {
			if err := p.routeWatcher.Start(ctx); err != nil {
				p.logger.Error("RouteWatcher stopped", zap.Error(err))
			}
		}()
	}

	return p.server.ListenAndServe()
}

// Stop gracefully stops the proxy
func (p *Proxy) Stop(ctx context.Context) error {
	return p.server.Shutdown(ctx)
}

// handleEmbed routes embedding requests
func (p *Proxy) handleEmbed(w http.ResponseWriter, r *http.Request) {
	p.proxyRequest(w, r, "embed")
}

// handleChunk routes chunking requests
func (p *Proxy) handleChunk(w http.ResponseWriter, r *http.Request) {
	p.proxyRequest(w, r, "chunk")
}

// handleRerank routes reranking requests
func (p *Proxy) handleRerank(w http.ResponseWriter, r *http.Request) {
	p.proxyRequest(w, r, "rerank")
}

func (p *Proxy) proxyRequest(w http.ResponseWriter, r *http.Request, operation string) {
	start := time.Now()

	// Parse request to get model
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request", http.StatusBadRequest)
		return
	}

	var req struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	// Build headers map for route matching
	headers := make(map[string]string)
	for k := range r.Header {
		headers[k] = r.Header.Get(k)
	}

	// Try route-based matching first
	var pool string
	routeReq := &RouteRequest{
		Operation: OperationType(operation),
		Model:     req.Model,
		Headers:   headers,
		Timestamp: start,
	}

	if matchedRoute := p.router.RouteManager().Match(routeReq); matchedRoute != nil {
		// Check rate limiting
		if matchedRoute.RateLimiter != nil && !matchedRoute.RateLimiter.Allow(req.Model) {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		// Select destination from matched route
		dest, err := p.router.RouteManager().SelectDestination(matchedRoute, routeReq, p.registry)
		if err == nil && dest != nil {
			pool = dest.Pool
		} else if matchedRoute.Fallback != nil {
			// Handle fallback
			switch matchedRoute.Fallback.Action {
			case "reject":
				statusCode := matchedRoute.Fallback.StatusCode
				if statusCode == 0 {
					statusCode = 503
				}
				msg := matchedRoute.Fallback.Message
				if msg == "" {
					msg = "no healthy endpoints available"
				}
				if matchedRoute.Fallback.RetryAfter > 0 {
					w.Header().Set("Retry-After", fmt.Sprintf("%d", matchedRoute.Fallback.RetryAfter))
				}
				http.Error(w, msg, statusCode)
				return
			case "redirect":
				pool = matchedRoute.Fallback.RedirectPool
			}
		}
	}

	// Fall back to X-Termite-Pool header or default pool
	if pool == "" {
		pool = r.Header.Get("X-Termite-Pool")
	}
	if pool == "" {
		pool = p.defaultPool
	}

	// Determine workload type from header or infer from operation
	workloadType := WorkloadType(r.Header.Get("X-Termite-Workload-Type"))
	if workloadType == "" {
		switch operation {
		case "embed", "rerank":
			workloadType = WorkloadTypeReadHeavy
		case "chunk":
			workloadType = WorkloadTypeWriteHeavy
		default:
			workloadType = WorkloadTypeGeneral
		}
	}

	// Route the request
	endpoint, err := p.router.RouteRequest(r.Context(), req.Model, pool, workloadType)
	if err != nil {
		requestsTotal.WithLabelValues(pool, req.Model, operation, "no_endpoint").Inc()
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	// Track active connections
	atomic.AddInt32(&endpoint.Connections, 1)
	activeConnections.WithLabelValues(endpoint.Pool, endpoint.Address).Inc()
	defer func() {
		atomic.AddInt32(&endpoint.Connections, -1)
		activeConnections.WithLabelValues(endpoint.Pool, endpoint.Address).Dec()
	}()

	// Proxy the request — endpoint.Address comes from internal service discovery, not user input.
	targetURL, _ := url.Parse(endpoint.Address)
	proxy := httputil.NewSingleHostReverseProxy(targetURL) //nolint:gosec // G704: endpoint address is from trusted internal registry

	// Restore body for proxying
	r.Body = io.NopCloser(&bodyReader{data: body})
	r.ContentLength = int64(len(body))

	// Custom response handler for metrics
	proxy.ModifyResponse = func(resp *http.Response) error {
		duration := time.Since(start).Seconds()
		status := "success"
		if resp.StatusCode >= 400 {
			status = "error"
			if cb := p.registry.GetCircuitBreaker(endpoint.Address); cb != nil {
				cb.RecordFailure()
			}
		} else {
			if cb := p.registry.GetCircuitBreaker(endpoint.Address); cb != nil {
				cb.RecordSuccess()
			}
		}

		requestsTotal.WithLabelValues(endpoint.Pool, req.Model, operation, status).Inc()
		requestLatency.WithLabelValues(endpoint.Pool, req.Model, operation).Observe(duration)
		return nil
	}

	proxy.ServeHTTP(w, r) //nolint:gosec // G704: endpoint address is from trusted internal registry
}

type bodyReader struct {
	data []byte
	pos  int
}

func (b *bodyReader) Read(p []byte) (n int, err error) {
	if b.pos >= len(b.data) {
		return 0, io.EOF
	}
	n = copy(p, b.data[b.pos:])
	b.pos += n
	return n, nil
}

func (p *Proxy) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (p *Proxy) handleReady(w http.ResponseWriter, r *http.Request) {
	// Check if we have any healthy endpoints
	p.registry.mu.RLock()
	hasHealthy := false
	for _, ep := range p.registry.endpoints {
		if ep.Healthy {
			hasHealthy = true
			break
		}
	}
	p.registry.mu.RUnlock()

	if hasHealthy {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("no healthy endpoints"))
	}
}

func (p *Proxy) refreshLoop(ctx context.Context) {
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

// UnregisterEndpoint removes an endpoint (called from K8s watcher)
func (p *Proxy) UnregisterEndpoint(address string) {
	p.registry.UnregisterEndpoint(address)
}
