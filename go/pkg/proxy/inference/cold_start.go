package proxy

import (
	"context"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// The RPC budget is independent of the model/node readiness timeout. No worker
// polls readiness or renews activity periodically: only requests extend idleness.
const coldStartActivationBudget = 2 * time.Second

func (p *Proxy) useColdStartFallback(route *Route, dest *Destination, req *RouteRequest) bool {
	return p.coldFallbackRoutes[route.Name] && route.Fallback != nil &&
		route.Fallback.Action == "redirect" && route.Fallback.RedirectPool != "" &&
		route.Fallback.RedirectPool != dest.Pool && p.activator != nil &&
		p.activator.IsEnabled(routeNamespace(route), dest.Pool) &&
		p.registry.PoolConditionStats(dest.Pool, req.Model).HealthyEndpoints == 0
}

func (p *Proxy) resolveColdStartFallback(ctx context.Context, route *Route, dest *Destination, req *RouteRequest, workload WorkloadType, reserve bool) (*Resolution, error) {
	if err := ctx.Err(); err != nil {
		return nil, resolutionError(err)
	}
	p.activateColdPool(ctx, routeNamespace(route), dest.Pool)
	// Resolve directly: an unavailable fallback must not start another wake/wait.
	pool := route.Fallback.RedirectPool
	endpoint, err := p.resolveEndpoint(ctx, req.Model, pool, workload, reserve)
	if err != nil {
		return nil, &ResolutionError{StatusCode: http.StatusServiceUnavailable,
			Message: "primary inference pool is waking and fallback is unavailable", RetryAfter: 5}
	}
	return &Resolution{Route: route, Endpoint: endpoint, Pool: pool, ColdStartFallback: true}, nil
}

func (p *Proxy) activateColdPool(ctx context.Context, namespace, pool string) {
	key := namespace + "/" + pool
	p.activationMu.Lock()
	if p.activations[key] {
		p.activationMu.Unlock()
		return
	}
	p.activations[key] = true
	p.activationMu.Unlock()
	// A successful CPU response (or disconnected caller) must not cancel wakeup.
	activationCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), coldStartActivationBudget)
	go func() {
		defer cancel()
		defer func() {
			p.activationMu.Lock()
			delete(p.activations, key)
			p.activationMu.Unlock()
		}()
		_, enabled, err := p.activatePool(activationCtx, namespace, pool)
		if err != nil || !enabled {
			p.logger.Warn("background inference pool activation failed; request uses fallback",
				zap.String("namespace", namespace), zap.String("pool", pool), zap.Bool("enabled", enabled), zap.Error(err))
		}
	}()
}
