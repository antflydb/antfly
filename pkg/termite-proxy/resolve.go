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

package proxy

import (
	"context"
	"net/http"
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
		SourceTable:        firstNonEmpty(req.Source.Table, headerValue(req.Headers, "X-Termite-Source-Table", "X-Antfly-Table")),
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
	var pool string
	var matchedRoute *Route
	var selectedDest *Destination

	if matchedRoute = p.router.RouteManager().Match(routeReq); matchedRoute != nil {
		dest, err := p.router.RouteManager().SelectDestination(matchedRoute, routeReq, p.registry)
		if err != nil {
			return nil, &ResolutionError{
				StatusCode: http.StatusServiceUnavailable,
				Message:    err.Error(),
			}
		}
		if dest != nil {
			selectedDest = dest
			pool = dest.Pool
		} else if matchedRoute.Fallback != nil {
			fallbackPool, fallbackErr := p.resolveRouteFallback(ctx, matchedRoute, routeReq)
			if fallbackErr != nil {
				return nil, fallbackErr
			}
			pool = fallbackPool
		} else {
			return nil, noEligibleDestinationsError()
		}
	}

	if pool == "" {
		pool = headerValue(headers, "X-Termite-Pool")
	}
	if pool == "" {
		pool = p.defaultPool
	}

	workloadType := resolveWorkloadType(routeReq.Operation, headers)
	endpoint, err := p.resolveEndpoint(ctx, routeReq.Model, pool, workloadType, reserve)
	if err != nil {
		return nil, &ResolutionError{
			StatusCode: http.StatusServiceUnavailable,
			Message:    err.Error(),
		}
	}

	return &Resolution{
		Route:       matchedRoute,
		Destination: selectedDest,
		Endpoint:    endpoint,
		Pool:        pool,
	}, nil
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
	workloadType := WorkloadType(headerValue(headers, "X-Termite-Workload-Type"))
	if workloadType != "" {
		return workloadType
	}

	switch operation {
	case "embed", "rerank":
		return WorkloadTypeReadHeavy
	case "chunk":
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
