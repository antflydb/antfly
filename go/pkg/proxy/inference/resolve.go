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
	"crypto/sha256"
	"errors"
	"net/http"
	"strconv"
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

// RoutingContext is the normalized, authenticated request context shared by
// capability discovery and execution. ExplicitPool is the caller-selected
// fallback when no configured route matches; route policy takes precedence in
// both paths.
type RoutingContext struct {
	Headers      map[string]string
	Source       VerifiedSource
	ExplicitPool string
	Timestamp    time.Time
}

func (c RoutingContext) routeRequest(operation OperationType, model string) *RouteRequest {
	timestamp := c.Timestamp
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	return &RouteRequest{
		Operation:          operation,
		Model:              model,
		Headers:            c.Headers,
		SourceTable:        firstNonEmpty(c.Source.Table, headerValue(c.Headers, "X-Antfly-Source-Table", "X-Antfly-Table")),
		SourceOrganization: c.Source.OrganizationID,
		SourceProject:      c.Source.ProjectID,
		SourceAPIKey:       c.Source.APIKeyPrefix,
		Timestamp:          timestamp,
	}
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

	proxy                         *Proxy
	model                         string
	operation                     OperationType
	capabilityToken               string
	capabilityRevision            string
	capabilityAuthorizationDigest [sha256.Size]byte
	workloadType                  WorkloadType
	reservation                   endpointReservation
	once                          sync.Once
	completed                     uint32
	admission                     *leaseAdmission
	attempts                      *leaseAttempts
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
	StatusCode      int
	Message         string
	RetryAfter      int
	CapabilityStale bool
}

func (e *ResolutionError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

func staleCapabilityResolutionError(message string) *ResolutionError {
	return &ResolutionError{
		StatusCode:      http.StatusConflict,
		Message:         message,
		CapabilityStale: true,
	}
}

// writeResolutionError preserves the routing contract uniformly across initial
// resolution, admission, and retry-time re-resolution.
func writeResolutionError(w http.ResponseWriter, err error) bool {
	var resolutionErr *ResolutionError
	if !errors.As(err, &resolutionErr) {
		return false
	}
	if resolutionErr.CapabilityStale {
		w.Header().Set(capabilityStaleHeader, "true")
	}
	if resolutionErr.RetryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(resolutionErr.RetryAfter))
	}
	http.Error(w, resolutionErr.Message, resolutionErr.StatusCode)
	return true
}

// ResolveRequest resolves a request to an endpoint without forwarding it.
func (p *Proxy) ResolveRequest(ctx context.Context, req ResolveRequest) (*Resolution, error) {
	routing := routingContextForResolveRequest(req)
	resolution, _, err := p.resolve(ctx, routing.routeRequest(req.Operation, req.Model), routing, false)
	if err != nil {
		return nil, err
	}
	return publicResolutionSnapshot(resolution), nil
}

// AcquireRequestResolution resolves and reserves an endpoint for forwarding.
// Callers must Admit the request before forwarding, then finish the returned
// lease with RecordSuccess, RecordFailure, or Release.
func (p *Proxy) AcquireRequestResolution(ctx context.Context, req ResolveRequest) (*ResolutionLease, error) {
	lease, err := p.acquireRequestResolution(ctx, req.Operation, req.Model, routingContextForResolveRequest(req))
	if err != nil {
		return nil, err
	}
	lease.Resolution = publicResolutionSnapshot(lease.Resolution)
	return lease, nil
}

func publicResolutionSnapshot(resolution *Resolution) *Resolution {
	if resolution == nil {
		return nil
	}
	snapshot := *resolution
	snapshot.Route = cloneRoute(resolution.Route, false)
	if resolution.Destination != nil {
		destination := *resolution.Destination
		destination.QueueDepthCondition = cloneThreshold(resolution.Destination.QueueDepthCondition)
		destination.ReplicaCondition = cloneThreshold(resolution.Destination.ReplicaCondition)
		destination.LatencyCondition = cloneThreshold(resolution.Destination.LatencyCondition)
		destination.TimeCondition = cloneTimeWindow(resolution.Destination.TimeCondition)
		snapshot.Destination = &destination
	}
	return &snapshot
}

func (p *Proxy) acquireRequestResolution(ctx context.Context, operation OperationType, model string, routing RoutingContext) (*ResolutionLease, error) {
	resolution, reservation, err := p.resolve(ctx, routing.routeRequest(operation, model), routing, true)
	if err != nil {
		return nil, err
	}
	return &ResolutionLease{
		Resolution:                    resolution,
		proxy:                         p,
		model:                         model,
		operation:                     operation,
		capabilityToken:               headerValue(routing.Headers, capabilityTokenHeader),
		capabilityRevision:            headerValue(routing.Headers, capabilityRevisionHeader),
		capabilityAuthorizationDigest: sha256.Sum256([]byte(p.upstreamAuthorizationForHeaders(routing.Headers))),
		workloadType:                  resolveWorkloadType(operation, routing.Headers),
		reservation:                   reservation,
		admission:                     &leaseAdmission{},
		attempts:                      newLeaseAttempts(),
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
	allowed, err := l.proxy.capabilityLeaseEndpoints(l.capabilityToken, l.capabilityRevision, l.model, l.operation, l.capabilityAuthorizationDigest)
	if err != nil {
		return nil, err
	}
	namespace := l.proxy.defaultPool.Namespace
	if l.Resolution.Route != nil {
		namespace = l.Resolution.Route.Namespace
	}
	reservation, err := l.proxy.router.routeRequestInNamespaceWithin(ctx, l.model, namespace, l.Resolution.Pool, l.workloadType, excluded, allowed, l.operation)
	if err != nil {
		return nil, err
	}

	return &ResolutionLease{
		Resolution: &Resolution{
			Route:       l.Resolution.Route,
			Destination: l.Resolution.Destination,
			Endpoint:    reservation.ref.endpoint,
			Pool:        l.Resolution.Pool,
		},
		proxy:                         l.proxy,
		model:                         l.model,
		operation:                     l.operation,
		capabilityToken:               l.capabilityToken,
		capabilityRevision:            l.capabilityRevision,
		capabilityAuthorizationDigest: l.capabilityAuthorizationDigest,
		workloadType:                  l.workloadType,
		reservation:                   reservation,
		admission:                     l.admission,
		attempts:                      l.attempts,
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
	atomic.AddInt32(&endpoint.runtime.connections, 1)
	activeConnections.WithLabelValues(endpoint.metricPool(), endpoint.address).Inc()

	return &ForwardingLease{endpoint: endpoint}
}

// Finish decrements the in-flight load for the forwarding attempt.
func (f *ForwardingLease) Finish() {
	if f == nil || f.endpoint == nil {
		return
	}
	f.once.Do(func() {
		atomic.AddInt32(&f.endpoint.runtime.connections, -1)
		activeConnections.WithLabelValues(f.endpoint.metricPool(), f.endpoint.address).Dec()
	})
}

func (l *ResolutionLease) finish(release func(*CircuitBreaker)) {
	if l == nil || l.proxy == nil || l.Resolution == nil || l.Resolution.Endpoint == nil {
		return
	}
	l.once.Do(func() {
		if l.reservation.ref.breaker != nil {
			release(l.reservation.ref.breaker)
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
		a.excluded[endpoint.address] = true
	}

	snapshot := make(map[string]bool, len(a.excluded))
	for address := range a.excluded {
		snapshot[address] = true
	}
	return snapshot
}

func routingContextForResolveRequest(req ResolveRequest) RoutingContext {
	return RoutingContext{
		Headers:      req.Headers,
		Source:       req.Source,
		ExplicitPool: headerValue(req.Headers, "X-Antfly-Inference-Pool"),
		Timestamp:    req.Timestamp,
	}
}

// StartBackground starts background refresh and route watching without an HTTP listener.
func (p *Proxy) StartBackground(ctx context.Context) {
	p.startBackgroundWorkers(ctx)
}

func (p *Proxy) startBackgroundWorkers(ctx context.Context) {
	// Registration-driven discovery remains active even when periodic
	// reconciliation is disabled. This lets deployments use event-driven
	// endpoint discovery without making the refresh interval a correctness
	// requirement.
	go p.refreshLoop(ctx)
	if p.routeWatcher == nil {
		return
	}
	go func() {
		if err := p.routeWatcher.Start(ctx); err != nil {
			p.logger.Error("RouteWatcher stopped", zap.Error(err))
		}
	}()
}

func (p *Proxy) resolve(ctx context.Context, routeReq *RouteRequest, routing RoutingContext, reserve bool) (*Resolution, endpointReservation, error) {
	var matchedRoute *Route
	capabilityLease, err := p.validatedCapabilityLease(
		headerValue(routing.Headers, capabilityTokenHeader),
		headerValue(routing.Headers, capabilityRevisionHeader),
		routeReq.Model,
		routeReq.Operation,
		sha256.Sum256([]byte(p.upstreamAuthorizationForHeaders(routing.Headers))),
	)
	if err != nil {
		return nil, endpointReservation{}, err
	}
	allowed := capabilityLease.endpoints

	if capabilityLease.scoped {
		var current bool
		matchedRoute, current = p.router.RouteManager().matchInstalledAtGeneration(routeReq, capabilityLease.routeGeneration)
		if !current {
			return nil, endpointReservation{}, staleCapabilityResolutionError("inference capability lease is stale")
		}
	} else {
		matchedRoute = p.router.RouteManager().matchInstalled(routeReq)
	}
	workloadType := resolveWorkloadType(routeReq.Operation, routing.Headers)
	if matchedRoute != nil {
		dest, err := p.router.RouteManager().selectDestinationWithin(matchedRoute, routeReq, p.registry, allowed)
		if err != nil {
			return nil, endpointReservation{}, &ResolutionError{
				StatusCode: http.StatusServiceUnavailable,
				Message:    err.Error(),
			}
		}
		if dest == nil {
			dest = p.selectActivationDestinationWithin(matchedRoute, routeReq, allowed)
		}
		if dest != nil {
			endpoint, reservation, resolveErr := p.resolvePoolTarget(
				ctx, matchedRoute.Namespace, dest.Pool, routeReq, workloadType,
				routeReq.Operation, reserve, dest, allowed,
			)
			if resolveErr == nil {
				return &Resolution{
					Route:       matchedRoute,
					Destination: dest,
					Endpoint:    endpoint,
					Pool:        dest.Pool,
				}, reservation, nil
			}
			if ctx.Err() != nil {
				return nil, endpointReservation{}, resolutionError(ctx.Err())
			}
			if matchedRoute.Fallback == nil {
				return nil, endpointReservation{}, resolutionError(resolveErr)
			}
		}
		if matchedRoute.Fallback != nil {
			fallbackPool, fallbackErr := p.resolveRouteFallback(ctx, matchedRoute, routeReq, allowed)
			if fallbackErr != nil {
				return nil, endpointReservation{}, fallbackErr
			}
			endpoint, reservation, resolveErr := p.resolvePoolTarget(
				ctx, matchedRoute.Namespace, fallbackPool, routeReq, workloadType,
				routeReq.Operation, reserve, nil, allowed,
			)
			if resolveErr != nil {
				return nil, endpointReservation{}, resolutionError(resolveErr)
			}
			return &Resolution{Route: matchedRoute, Endpoint: endpoint, Pool: fallbackPool}, reservation, nil
		}
		return nil, endpointReservation{}, noEligibleDestinationsError()
	}

	pool := strings.TrimSpace(routing.ExplicitPool)
	if pool == "" {
		pool = p.defaultPool.Pool
	}
	endpoint, reservation, err := p.resolvePoolTarget(
		ctx, p.defaultPool.Namespace, pool, routeReq, workloadType, routeReq.Operation, reserve, nil, allowed,
	)
	if err != nil {
		return nil, endpointReservation{}, resolutionError(err)
	}

	return &Resolution{
		Endpoint: endpoint,
		Pool:     pool,
	}, reservation, nil
}

func resolutionError(err error) *ResolutionError {
	if typed, ok := err.(*ResolutionError); ok {
		return typed
	}
	return &ResolutionError{StatusCode: http.StatusServiceUnavailable, Message: err.Error()}
}

func (p *Proxy) selectActivationDestination(route *Route, req *RouteRequest) *Destination {
	return p.selectActivationDestinationWithin(route, req, nil)
}

func (p *Proxy) selectActivationDestinationWithin(route *Route, req *RouteRequest, allowed map[string]endpointRef) *Destination {
	if p.activator == nil {
		return nil
	}
	namespace := route.Namespace
	return p.router.RouteManager().selectActivationDestinationWithin(route, req, p.registry, allowed, func(pool string) bool {
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
	namespace := route.Namespace
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
func (p *Proxy) resolvePoolTarget(
	ctx context.Context,
	namespace, pool string,
	req *RouteRequest,
	workloadType WorkloadType,
	operation OperationType,
	reserve bool,
	destination *Destination,
	allowed map[string]endpointRef,
) (*Endpoint, endpointReservation, error) {
	wasCold := p.registry.poolConditionStatsInNamespaceWithin(namespace, pool, req.Model, allowed).HealthyEndpoints == 0
	activationWait, activationEnabled, activationErr := time.Duration(0), false, error(nil)
	if allowed == nil || endpointRefsContainNamespacedPool(allowed, namespace, pool) {
		activationWait, activationEnabled, activationErr = p.activatePool(ctx, namespace, pool)
	}
	if activationErr != nil {
		p.logger.Warn("failed to refresh inference pool activation", zap.String("namespace", namespace), zap.String("pool", pool), zap.Error(activationErr))
	}
	if wasCold && destination != nil && activationEnabled && activationErr == nil {
		// Activation may make an endpoint visible synchronously. It is still not
		// acquirable until the selected destination's dynamic conditions hold.
		// Keeping that selection and acquisition in this state machine prevents
		// a newly visible endpoint from bypassing model/queue/replica/latency rules.
		if p.waitForRouteDestination(ctx, namespace, destination, req, allowed, activationWait) == nil {
			return nil, endpointReservation{}, noEligibleDestinationsError()
		}
		if reserve {
			reservation, err := p.router.acquireDestinationEndpointWithin(req, destination, workloadType, namespace, allowed, operation)
			if err != nil {
				return nil, endpointReservation{}, err
			}
			return reservation.ref.endpoint, reservation, nil
		}
		return p.resolveEndpoint(ctx, req.Model, namespace, pool, workloadType, operation, false, allowed)
	}

	if destination != nil && reserve {
		reservation, err := p.router.acquireDestinationEndpointWithin(req, destination, workloadType, namespace, allowed, operation)
		if err != nil {
			return nil, endpointReservation{}, err
		}
		return reservation.ref.endpoint, reservation, nil
	}
	endpoint, reservation, err := p.resolveEndpoint(ctx, req.Model, namespace, pool, workloadType, operation, reserve, allowed)
	if err == nil {
		return endpoint, reservation, nil
	}
	if !wasCold || !activationEnabled || activationErr != nil {
		return nil, endpointReservation{}, err
	}

	if destination != nil {
		if p.waitForRouteDestination(ctx, namespace, destination, req, allowed, activationWait) == nil {
			return nil, endpointReservation{}, err
		}
		return p.resolveEndpoint(ctx, req.Model, namespace, pool, workloadType, operation, reserve, allowed)
	}
	return p.waitForPoolEndpoint(ctx, req.Model, namespace, pool, workloadType, operation, reserve, allowed, activationWait)
}

func (p *Proxy) activatePool(ctx context.Context, namespace, pool string) (time.Duration, bool, error) {
	if p.activator == nil || pool == "" {
		return 0, false, nil
	}
	return p.activator.Activate(ctx, namespace, pool)
}

func (p *Proxy) waitForRouteDestination(ctx context.Context, namespace string, destination *Destination, req *RouteRequest, allowed map[string]endpointRef, maxWait time.Duration) *Destination {
	if maxWait <= 0 {
		return nil
	}
	req.Timestamp = time.Now()
	if p.router.RouteManager().evaluateConditionsInNamespaceWithin(destination, req, p.registry, namespace, allowed) {
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
			if p.router.RouteManager().evaluateConditionsInNamespaceWithin(destination, req, p.registry, namespace, allowed) {
				return destination
			}
		}
	}
}

func (p *Proxy) waitForPoolEndpoint(ctx context.Context, model, namespace, pool string, workloadType WorkloadType, operation OperationType, reserve bool, allowed map[string]endpointRef, maxWait time.Duration) (*Endpoint, endpointReservation, error) {
	if maxWait <= 0 {
		return p.resolveEndpoint(ctx, model, namespace, pool, workloadType, operation, reserve, allowed)
	}
	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		select {
		case <-ctx.Done():
			return nil, endpointReservation{}, ctx.Err()
		case <-timer.C:
			if lastErr != nil {
				return nil, endpointReservation{}, lastErr
			}
			return nil, endpointReservation{}, &ResolutionError{StatusCode: http.StatusServiceUnavailable, Message: "inference pool activation timed out"}
		case <-ticker.C:
			endpoint, reservation, err := p.resolveEndpoint(ctx, model, namespace, pool, workloadType, operation, reserve, allowed)
			if err == nil {
				return endpoint, reservation, nil
			}
			lastErr = err
		}
	}
}

func (p *Proxy) resolveEndpoint(ctx context.Context, model, namespace, pool string, workloadType WorkloadType, operation OperationType, reserve bool, allowed map[string]endpointRef) (*Endpoint, endpointReservation, error) {
	if reserve {
		reservation, err := p.router.routeRequestInNamespaceWithin(ctx, model, namespace, pool, workloadType, nil, allowed, operation)
		return reservation.ref.endpoint, reservation, err
	}

	candidates := p.router.resolveEndpointCandidatesInNamespaceWithin(model, namespace, pool, nil, allowed, operation)
	if len(candidates) == 0 {
		return nil, endpointReservation{}, &ResolutionError{
			StatusCode: http.StatusServiceUnavailable,
			Message:    "no healthy endpoints available for model " + model,
		}
	}
	endpoint, err := p.router.selectEndpoint(model, workloadType, candidates, false)
	return endpoint, endpointReservation{}, err
}

func (p *Proxy) resolveRouteFallback(ctx context.Context, route *Route, routeReq *RouteRequest, allowed map[string]endpointRef) (string, *ResolutionError) {
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
		queuedPool, queueErr := p.waitForQueuedDestination(ctx, route, routeReq, allowed)
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

	switch semanticTaskForOperation(operation) {
	case "embed", "rerank", "extract":
		return WorkloadTypeReadHeavy
	case "chunk", "generate":
		return WorkloadTypeWriteHeavy
	default:
		return WorkloadTypeGeneral
	}
}

func headerValue(headers map[string]string, names ...string) string {
	for _, name := range names {
		if value, ok := lookupHeader(headers, name); ok && value != "" {
			return value
		}
	}
	return ""
}

func lookupHeader(headers map[string]string, name string) (string, bool) {
	if value, ok := headers[name]; ok {
		return value, true
	}
	canonical := http.CanonicalHeaderKey(name)
	if value, ok := headers[canonical]; ok {
		return value, true
	}
	for candidate, value := range headers {
		if strings.EqualFold(candidate, name) {
			return value, true
		}
	}
	return "", false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
