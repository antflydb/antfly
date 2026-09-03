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

package proxy

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// VerifiedSource carries trusted caller identity from an authenticated edge.
type VerifiedSource struct {
	OrganizationID string
	ProjectID      string
	APIKeyPrefix   string
	Table          string
}

// ResolveRequest describes a routing decision request without proxying the body.
type ResolveRequest struct {
	Operation OperationType
	Model     string
	Headers   map[string]string
	Source    VerifiedSource
	Timestamp time.Time
}

// PoolActivator wakes and refreshes request-driven scale-to-zero pools.
// Activate returns enabled=false for pools that are not managed by the
// activator. When enabled is true, wait is the maximum cold-start window.
type PoolActivator interface {
	IsEnabled(namespace, pool string) bool
	Activate(ctx context.Context, namespace, pool string) (wait time.Duration, enabled bool, err error)
}

// Resolution is the result of routing a request to a specific endpoint.
type Resolution struct {
	Route       *Route
	Destination *Destination
	Endpoint    *Endpoint
	Pool        string
}

// ResolutionLease reserves a resolved endpoint for forwarding and must be completed.
type ResolutionLease struct {
	Resolution *Resolution

	proxy        *Proxy
	model        string
	workloadType WorkloadType
	once         sync.Once
	completed    uint32
	admission    *leaseAdmission
	attempts     *leaseAttempts
}

// ForwardingLease tracks in-flight load for a concrete forwarding attempt.
// Callers must finish it once the request attempt completes.
type ForwardingLease struct {
	endpoint *Endpoint
	once     sync.Once
}

type leaseAdmission struct {
	once sync.Once
	err  error
}

type leaseAttempts struct {
	mu       sync.Mutex
	excluded map[string]bool
}

// ResolutionError is a user-facing routing failure.
type ResolutionError struct {
	StatusCode int
	Message    string
	RetryAfter int
}

func (e *ResolutionError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

// ResolveRequest resolves a request to an endpoint without forwarding it.
func (p *Proxy) ResolveRequest(ctx context.Context, req ResolveRequest) (*Resolution, error) {
	return p.resolveResolveRequest(ctx, req, false)
}

// AcquireRequestResolution resolves and reserves an endpoint for forwarding.
// Callers must Admit the request before forwarding, then finish the returned
// lease with RecordSuccess, RecordFailure, or Release.
func (p *Proxy) AcquireRequestResolution(ctx context.Context, req ResolveRequest) (*ResolutionLease, error) {
	resolution, err := p.resolveResolveRequest(ctx, req, true)
	if err != nil {
		return nil, err
	}
	return &ResolutionLease{
		Resolution:   resolution,
		proxy:        p,
		model:        req.Model,
		workloadType: resolveWorkloadType(req.Operation, req.Headers),
		admission:    &leaseAdmission{},
		attempts:     newLeaseAttempts(),
	}, nil
}

// Admit consumes route admission state for the logical request represented by the lease.
// It is idempotent so retries can share one admission decision.
func (l *ResolutionLease) Admit() error {
	if l == nil || l.Resolution == nil || l.Resolution.Route == nil || l.Resolution.Route.RateLimiter == nil {
		return nil
	}
	if l.admission == nil {
		l.admission = &leaseAdmission{}
	}
	l.admission.once.Do(func() {
		if !l.Resolution.Route.RateLimiter.Allow(l.model) {
			l.admission.err = &ResolutionError{
				StatusCode: http.StatusTooManyRequests,
				Message:    "rate limit exceeded",
			}
		}
	})
	return l.admission.err
}

// NextAttempt reacquires an endpoint for another forwarding attempt under the
// same logical request, preserving the original admission decision.
func (l *ResolutionLease) NextAttempt(ctx context.Context) (*ResolutionLease, error) {
	if l == nil || l.proxy == nil || l.Resolution == nil {
		return nil, &ResolutionError{
			StatusCode: http.StatusServiceUnavailable,
			Message:    "cannot reacquire endpoint for nil resolution lease",
		}
	}
	if atomic.LoadUint32(&l.completed) == 0 {
		return nil, &ResolutionError{
			StatusCode: http.StatusConflict,
			Message:    "cannot reacquire endpoint before completing current attempt",
		}
	}

	if l.attempts == nil {
		l.attempts = newLeaseAttempts()
	}

	excluded := l.attempts.excludeAndSnapshot(l.Resolution.Endpoint)
	endpoint, err := l.proxy.router.RouteRequest(ctx, l.model, l.Resolution.Pool, l.workloadType, excluded)
	if err != nil {
		return nil, err
	}

	return &ResolutionLease{
		Resolution: &Resolution{
			Route:       l.Resolution.Route,
			Destination: l.Resolution.Destination,
			Endpoint:    endpoint,
			Pool:        l.Resolution.Pool,
		},
		proxy:        l.proxy,
		model:        l.model,
		workloadType: l.workloadType,
		admission:    l.admission,
		attempts:     l.attempts,
	}, nil
}

func (l *ResolutionLease) RecordSuccess() {
	l.finish(func(cb *CircuitBreaker) {
		cb.RecordSuccess()
	})
}

func (l *ResolutionLease) RecordFailure() {
	l.finish(func(cb *CircuitBreaker) {
		cb.RecordFailure()
	})
}

func (l *ResolutionLease) Release() {
	l.finish(func(cb *CircuitBreaker) {
		cb.ReleaseReservation()
	})
}

// BeginForwarding marks the resolved endpoint as actively serving a request attempt.
// The returned lease must be finished after the attempt completes.
func (l *ResolutionLease) BeginForwarding() *ForwardingLease {
	if l == nil || l.Resolution == nil || l.Resolution.Endpoint == nil {
		return &ForwardingLease{}
	}

	endpoint := l.Resolution.Endpoint
	atomic.AddInt32(&endpoint.Connections, 1)
	activeConnections.WithLabelValues(endpoint.Pool, endpoint.Address).Inc()

	return &ForwardingLease{endpoint: endpoint}
}

// Finish decrements the in-flight load for the forwarding attempt.
func (f *ForwardingLease) Finish() {
	if f == nil || f.endpoint == nil {
		return
	}
	f.once.Do(func() {
		atomic.AddInt32(&f.endpoint.Connections, -1)
		activeConnections.WithLabelValues(f.endpoint.Pool, f.endpoint.Address).Dec()
	})
}

func (l *ResolutionLease) finish(release func(*CircuitBreaker)) {
	if l == nil || l.proxy == nil || l.Resolution == nil || l.Resolution.Endpoint == nil {
		return
	}
	l.once.Do(func() {
		if cb := l.proxy.registry.GetCircuitBreaker(l.Resolution.Endpoint.Address); cb != nil {
			release(cb)
		}
		atomic.StoreUint32(&l.completed, 1)
	})
}

func newLeaseAttempts() *leaseAttempts {
	return &leaseAttempts{
		excluded: make(map[string]bool),
	}
}

func (a *leaseAttempts) excludeAndSnapshot(endpoint *Endpoint) map[string]bool {
	if a == nil {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if endpoint != nil {
		a.excluded[endpoint.Address] = true
	}

	snapshot := make(map[string]bool, len(a.excluded))
	for address := range a.excluded {
		snapshot[address] = true
	}
	return snapshot
}

func (p *Proxy) resolveResolveRequest(ctx context.Context, req ResolveRequest, reserve bool) (*Resolution, error) {
	timestamp := req.Timestamp
	if timestamp.IsZero() {
		timestamp = time.Now()
	}

	routeReq := &RouteRequest{
		Operation:          req.Operation,
		Model:              req.Model,
		Headers:            req.Headers,
		SourceTable:        firstNonEmpty(req.Source.Table, headerValue(req.Headers, "X-Antfly-Source-Table", "X-Antfly-Table")),
		SourceOrganization: req.Source.OrganizationID,
		SourceProject:      req.Source.ProjectID,
		SourceAPIKey:       req.Source.APIKeyPrefix,
		Timestamp:          timestamp,
	}

	return p.resolve(ctx, routeReq, req.Headers, reserve)
}

// StartBackground starts background refresh and route watching without an HTTP listener.
func (p *Proxy) StartBackground(ctx context.Context) {
	p.startBackgroundWorkers(ctx)
}

func (p *Proxy) startBackgroundWorkers(ctx context.Context) {
	if p.registry.refreshInterval > 0 {
		go p.refreshLoop(ctx)
	}
	if p.routeWatcher == nil {
		return
	}
	go func() {
		if err := p.routeWatcher.Start(ctx); err != nil {
			p.logger.Error("RouteWatcher stopped", zap.Error(err))
		}
	}()
}

func (p *Proxy) resolve(ctx context.Context, routeReq *RouteRequest, headers map[string]string, reserve bool) (*Resolution, error) {
	workloadType := resolveWorkloadType(routeReq.Operation, headers)
	matchedRoute := p.router.RouteManager().Match(routeReq)
	if matchedRoute != nil {
		dest, err := p.router.RouteManager().SelectDestination(matchedRoute, routeReq, p.registry)
		if err != nil {
			return nil, &ResolutionError{
				StatusCode: http.StatusServiceUnavailable,
				Message:    err.Error(),
			}
		}
		if dest == nil {
			dest = p.selectActivationDestination(matchedRoute, routeReq)
		}
		if dest != nil {
			endpoint, resolveErr := p.resolvePoolTarget(ctx, routeNamespace(matchedRoute), dest.Pool, routeReq, workloadType, reserve, dest)
			if resolveErr == nil {
				return &Resolution{
					Route:       matchedRoute,
					Destination: dest,
					Endpoint:    endpoint,
					Pool:        dest.Pool,
				}, nil
			}
			if ctx.Err() != nil {
				return nil, resolutionError(ctx.Err())
			}
			if matchedRoute.Fallback == nil {
				return nil, resolutionError(resolveErr)
			}
		}
		if matchedRoute.Fallback != nil {
			fallbackPool, fallbackErr := p.resolveRouteFallback(ctx, matchedRoute, routeReq)
			if fallbackErr != nil {
				return nil, fallbackErr
			}
			endpoint, resolveErr := p.resolvePoolTarget(ctx, routeNamespace(matchedRoute), fallbackPool, routeReq, workloadType, reserve, nil)
			if resolveErr != nil {
				return nil, resolutionError(resolveErr)
			}
			return &Resolution{Route: matchedRoute, Endpoint: endpoint, Pool: fallbackPool}, nil
		}
		return nil, noEligibleDestinationsError()
	}

	pool := headerValue(headers, "X-Antfly-Inference-Pool")
	if pool == "" {
		pool = p.defaultPool
	}

	endpoint, err := p.resolvePoolTarget(ctx, "", pool, routeReq, workloadType, reserve, nil)
	if err != nil {
		return nil, resolutionError(err)
	}

	return &Resolution{
		Endpoint: endpoint,
		Pool:     pool,
	}, nil
}

func resolutionError(err error) *ResolutionError {
	if typed, ok := err.(*ResolutionError); ok {
		return typed
	}
	return &ResolutionError{StatusCode: http.StatusServiceUnavailable, Message: err.Error()}
}

func (p *Proxy) selectActivationDestination(route *Route, req *RouteRequest) *Destination {
	if p.activator == nil {
		return nil
	}
	namespace := routeNamespace(route)
	return p.router.RouteManager().SelectActivationDestination(route, req, p.registry, func(pool string) bool {
		return p.activator.IsEnabled(namespace, pool)
	})
}

// activateRouteDestination is retained as a small activation primitive for
// callers that only need to select and wake a cold route destination. Request
// resolution itself goes through resolvePoolTarget so every target follows the
// same wake/wait/fallback state machine.
func (p *Proxy) activateRouteDestination(ctx context.Context, route *Route, req *RouteRequest) (*Destination, time.Duration) {
	selected := p.selectActivationDestination(route, req)
	if selected == nil {
		return nil, 0
	}
	namespace := routeNamespace(route)
	wait, enabled, err := p.activatePool(ctx, namespace, selected.Pool)
	if err != nil {
		p.logger.Warn("failed to activate inference route destination", zap.String("namespace", namespace), zap.String("pool", selected.Pool), zap.Error(err))
		return nil, 0
	}
	if !enabled {
		return nil, 0
	}
	return selected, wait
}

// resolvePoolTarget resolves one concrete pool through a single state machine:
// refresh its activation lease, satisfy the selected route's runtime
// conditions when it was genuinely cold, acquire an endpoint, and return the
// result to the caller for fallback handling.
func (p *Proxy) resolvePoolTarget(ctx context.Context, namespace, pool string, req *RouteRequest, workloadType WorkloadType, reserve bool, destination *Destination) (*Endpoint, error) {
	wasCold := p.registry.PoolConditionStats(pool, req.Model).HealthyEndpoints == 0
	activationWait, activationEnabled, activationErr := p.activatePool(ctx, namespace, pool)
	if activationErr != nil {
		p.logger.Warn("failed to refresh inference pool activation", zap.String("namespace", namespace), zap.String("pool", pool), zap.Error(activationErr))
	}
	if wasCold && destination != nil && activationEnabled && activationErr == nil {
		// Activation may make an endpoint visible synchronously. It is still not
		// acquirable until the selected destination's dynamic conditions hold.
		// Keeping that selection and acquisition in this state machine prevents
		// a newly visible endpoint from bypassing model/queue/replica/latency rules.
		if p.waitForRouteDestination(ctx, destination, req, activationWait) == nil {
			return nil, noEligibleDestinationsError()
		}
		if reserve {
			return p.router.AcquireDestinationEndpoint(req, destination, workloadType)
		}
		return p.resolveEndpoint(ctx, req.Model, pool, workloadType, reserve)
	}

	if destination != nil && reserve {
		return p.router.AcquireDestinationEndpoint(req, destination, workloadType)
	}
	endpoint, err := p.resolveEndpoint(ctx, req.Model, pool, workloadType, reserve)
	if err == nil {
		return endpoint, nil
	}
	if !wasCold || !activationEnabled || activationErr != nil {
		return nil, err
	}

	if destination != nil {
		if p.waitForRouteDestination(ctx, destination, req, activationWait) == nil {
			return nil, err
		}
		return p.resolveEndpoint(ctx, req.Model, pool, workloadType, reserve)
	}
	return p.waitForPoolEndpoint(ctx, req.Model, pool, workloadType, reserve, activationWait)
}

func (p *Proxy) activatePool(ctx context.Context, namespace, pool string) (time.Duration, bool, error) {
	if p.activator == nil || pool == "" {
		return 0, false, nil
	}
	return p.activator.Activate(ctx, namespace, pool)
}

func routeNamespace(route *Route) string {
	if route == nil {
		return ""
	}
	namespace, _, found := strings.Cut(route.Name, "/")
	if !found {
		return ""
	}
	return namespace
}

func (p *Proxy) waitForRouteDestination(ctx context.Context, destination *Destination, req *RouteRequest, maxWait time.Duration) *Destination {
	if maxWait <= 0 {
		return nil
	}
	req.Timestamp = time.Now()
	if p.router.RouteManager().evaluateConditions(destination, req, p.registry) {
		return destination
	}
	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			return nil
		case <-ticker.C:
			req.Timestamp = time.Now()
			if p.router.RouteManager().evaluateConditions(destination, req, p.registry) {
				return destination
			}
		}
	}
}

func (p *Proxy) waitForPoolEndpoint(ctx context.Context, model, pool string, workloadType WorkloadType, reserve bool, maxWait time.Duration) (*Endpoint, error) {
	if maxWait <= 0 {
		return p.resolveEndpoint(ctx, model, pool, workloadType, reserve)
	}
	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, &ResolutionError{StatusCode: http.StatusServiceUnavailable, Message: "inference pool activation timed out"}
		case <-ticker.C:
			endpoint, err := p.resolveEndpoint(ctx, model, pool, workloadType, reserve)
			if err == nil {
				return endpoint, nil
			}
			lastErr = err
		}
	}
}

func (p *Proxy) resolveEndpoint(ctx context.Context, model, pool string, workloadType WorkloadType, reserve bool) (*Endpoint, error) {
	if reserve {
		return p.router.RouteRequest(ctx, model, pool, workloadType, nil)
	}

	candidates := p.router.ResolveEndpointCandidates(model, pool, nil)
	if len(candidates) == 0 {
		return nil, &ResolutionError{
			StatusCode: http.StatusServiceUnavailable,
			Message:    "no healthy endpoints available for model " + model,
		}
	}
	return p.router.selectEndpoint(model, workloadType, candidates, false)
}

func (p *Proxy) resolveRouteFallback(ctx context.Context, route *Route, routeReq *RouteRequest) (string, *ResolutionError) {
	switch route.Fallback.Action {
	case "reject":
		statusCode := route.Fallback.StatusCode
		if statusCode == 0 {
			statusCode = http.StatusServiceUnavailable
		}
		msg := route.Fallback.Message
		if msg == "" {
			msg = "no healthy endpoints available"
		}
		return "", &ResolutionError{
			StatusCode: statusCode,
			Message:    msg,
			RetryAfter: route.Fallback.RetryAfter,
		}
	case "redirect":
		return route.Fallback.RedirectPool, nil
	case "queue":
		queuedPool, queueErr := p.waitForQueuedDestination(ctx, route, routeReq)
		if queueErr != nil {
			return "", &ResolutionError{
				StatusCode: http.StatusServiceUnavailable,
				Message:    queueErr.Error(),
			}
		}
		return queuedPool, nil
	default:
		return "", noEligibleDestinationsError()
	}
}

func noEligibleDestinationsError() *ResolutionError {
	return &ResolutionError{
		StatusCode: http.StatusServiceUnavailable,
		Message:    "no eligible destinations for matched route",
	}
}

func resolveWorkloadType(operation OperationType, headers map[string]string) WorkloadType {
	workloadType := WorkloadType(headerValue(headers, "X-Antfly-Inference-Workload-Type"))
	if workloadType != "" {
		return workloadType
	}

	switch operation {
	case "embed", "embeddings", "rerank", "extract":
		return WorkloadTypeReadHeavy
	case "chunk", "generate", "chat.completions":
		return WorkloadTypeWriteHeavy
	default:
		return WorkloadTypeGeneral
	}
}

func headerValue(headers map[string]string, names ...string) string {
	for _, name := range names {
		if value := headers[name]; value != "" {
			return value
		}
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
