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

	return p.resolve(ctx, routeReq, req.Headers)
}

// StartBackground starts background refresh and route watching without an HTTP listener.
func (p *Proxy) StartBackground(ctx context.Context) {
	go p.refreshLoop(ctx)
	if p.routeWatcher != nil {
		go func() {
			if err := p.routeWatcher.Start(ctx); err != nil {
				p.logger.Error("RouteWatcher stopped", zap.Error(err))
			}
		}()
	}
}

func (p *Proxy) resolve(ctx context.Context, routeReq *RouteRequest, headers map[string]string) (*Resolution, error) {
	var pool string
	var matchedRoute *Route
	var selectedDest *Destination

	if matchedRoute = p.router.RouteManager().Match(routeReq); matchedRoute != nil {
		if matchedRoute.RateLimiter != nil && !matchedRoute.RateLimiter.Allow(routeReq.Model) {
			return nil, &ResolutionError{
				StatusCode: http.StatusTooManyRequests,
				Message:    "rate limit exceeded",
			}
		}

		dest, err := p.router.RouteManager().SelectDestination(matchedRoute, routeReq, p.registry)
		if err == nil && dest != nil {
			selectedDest = dest
			pool = dest.Pool
		} else if matchedRoute.Fallback != nil {
			switch matchedRoute.Fallback.Action {
			case "reject":
				statusCode := matchedRoute.Fallback.StatusCode
				if statusCode == 0 {
					statusCode = http.StatusServiceUnavailable
				}
				msg := matchedRoute.Fallback.Message
				if msg == "" {
					msg = "no healthy endpoints available"
				}
				return nil, &ResolutionError{
					StatusCode: statusCode,
					Message:    msg,
					RetryAfter: matchedRoute.Fallback.RetryAfter,
				}
			case "redirect":
				pool = matchedRoute.Fallback.RedirectPool
			case "queue":
				queuedPool, queueErr := p.waitForQueuedDestination(ctx, matchedRoute, routeReq)
				if queueErr != nil {
					return nil, &ResolutionError{
						StatusCode: http.StatusServiceUnavailable,
						Message:    queueErr.Error(),
					}
				}
				pool = queuedPool
			}
		}
	}

	if pool == "" {
		pool = headerValue(headers, "X-Termite-Pool")
	}
	if pool == "" {
		pool = p.defaultPool
	}

	endpoint, err := p.router.RouteRequest(ctx, routeReq.Model, pool, resolveWorkloadType(routeReq.Operation, headers))
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
