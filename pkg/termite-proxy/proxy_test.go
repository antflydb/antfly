package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestProxyStartStopsOnContextCancel(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{Logger: zap.NewNop()})
	p.server = &http.Server{Handler: http.NewServeMux()}

	shutdownCalled := make(chan struct{})
	p.server.RegisterOnShutdown(func() {
		close(shutdownCalled)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- p.serve(ctx, func() error {
			<-shutdownCalled
			return http.ErrServerClosed
		})
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected clean shutdown, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("proxy did not stop after context cancellation")
	}
}

func TestStartBackgroundSkipsRefreshLoopWhenIntervalDisabled(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{Logger: zap.NewNop()})

	ctx, cancel := context.WithCancel(context.Background())
	p.StartBackground(ctx)
	time.Sleep(25 * time.Millisecond)
	cancel()
}

func TestSelectDestinationUsesWeights(t *testing.T) {
	t.Parallel()

	registry := NewModelRegistry(time.Minute)
	registry.RegisterEndpoint("http://pool-a-1", "pool-a", WorkloadTypeGeneral)
	registry.RegisterEndpoint("http://pool-b-1", "pool-b", WorkloadTypeGeneral)

	rm := NewRouteManager()
	route := &Route{
		Name: "default/weighted",
		Destinations: []Destination{
			{Pool: "pool-a", Weight: 80},
			{Pool: "pool-b", Weight: 20},
		},
	}

	var aCount int
	var bCount int
	for i := 0; i < 1000; i++ {
		dest, err := rm.SelectDestination(route, &RouteRequest{
			Operation: OperationType("embed"),
			Model:     "model-a",
			Timestamp: time.Unix(0, int64(i)),
		}, registry)
		if err != nil {
			t.Fatalf("SelectDestination returned error: %v", err)
		}
		switch dest.Pool {
		case "pool-a":
			aCount++
		case "pool-b":
			bCount++
		default:
			t.Fatalf("unexpected pool %q", dest.Pool)
		}
	}

	if aCount <= bCount {
		t.Fatalf("expected weighted routing to favor pool-a, got pool-a=%d pool-b=%d", aCount, bCount)
	}
	if aCount < 700 || aCount > 900 {
		t.Fatalf("expected pool-a to receive roughly 80%% of requests, got %d/1000", aCount)
	}
}

func TestProxyRequestRetriesRetryableStatus(t *testing.T) {
	t.Parallel()

	var attempts int32

	p := NewProxy(Config{
		DefaultPool:     "default",
		RefreshInterval: time.Minute,
		Logger:          zap.NewNop(),
	})
	p.registry.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			current := atomic.AddInt32(&attempts, 1)
			status := http.StatusOK
			body := `{"ok":true}`
			if current == 1 {
				status = http.StatusInternalServerError
				body = `{"error":"transient"}`
			}
			return &http.Response{
				StatusCode: status,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewBufferString(body)),
				Request:    req,
			}, nil
		}),
	}
	p.RegisterEndpoint("http://termite.internal", "primary", WorkloadTypeGeneral)
	p.Router().RouteManager().AddRoute(&Route{
		Name:       "default/retry",
		Operations: map[OperationType]bool{OperationType("embed"): true},
		Destinations: []Destination{
			{Pool: "primary", Weight: 100},
		},
		RetryAttempts:   2,
		RetryOnStatuses: map[int]bool{http.StatusInternalServerError: true},
	})

	req := httptest.NewRequest(http.MethodPost, "/api/embed", bytes.NewBufferString(`{"model":"bge-small-en-v1.5"}`))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	p.handleEmbed(recorder, req)

	resp := recorder.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected retried request to succeed, got %d", resp.StatusCode)
	}
	if got := atomic.LoadInt32(&attempts); got != 2 {
		t.Fatalf("expected 2 backend attempts, got %d", got)
	}
}

func TestProxyRequestRetryFailsOverToDifferentEndpoint(t *testing.T) {
	t.Parallel()

	var hosts []string

	p := NewProxy(Config{
		DefaultPool:     "default",
		RefreshInterval: time.Minute,
		Logger:          zap.NewNop(),
	})
	p.registry.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			hosts = append(hosts, req.URL.Host)
			status := http.StatusOK
			body := `{"ok":true}`
			if req.URL.Host == "primary-a.internal" {
				status = http.StatusInternalServerError
				body = `{"error":"transient"}`
			}
			return &http.Response{
				StatusCode: status,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewBufferString(body)),
				Request:    req,
			}, nil
		}),
	}
	p.RegisterEndpoint("http://primary-a.internal", "primary", WorkloadTypeGeneral)
	p.RegisterEndpoint("http://primary-b.internal", "primary", WorkloadTypeGeneral)
	atomic.StoreInt32(&p.registry.endpoints["http://primary-b.internal"].Connections, 1)
	p.Router().RouteManager().AddRoute(&Route{
		Name:       "default/retry-failover",
		Operations: map[OperationType]bool{OperationType("embed"): true},
		Destinations: []Destination{
			{Pool: "primary", Weight: 100},
		},
		RetryAttempts:   2,
		RetryOnStatuses: map[int]bool{http.StatusInternalServerError: true},
	})

	req := httptest.NewRequest(http.MethodPost, "/api/embed", bytes.NewBufferString(`{"model":"bge-small-en-v1.5"}`))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	p.handleEmbed(recorder, req)

	resp := recorder.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected retried request to succeed, got %d", resp.StatusCode)
	}
	if len(hosts) != 2 {
		t.Fatalf("expected 2 backend attempts, got %d (%v)", len(hosts), hosts)
	}
	if hosts[0] == hosts[1] {
		t.Fatalf("expected retry to fail over to a different endpoint, got %v", hosts)
	}
}

func TestProxyRequestRecordsFailureOnStreamCopyError(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{
		DefaultPool:     "default",
		RefreshInterval: time.Minute,
		Logger:          zap.NewNop(),
	})
	p.registry.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       &errReadCloser{data: []byte(`{"ok":`), err: io.ErrUnexpectedEOF},
				Request:    req,
			}, nil
		}),
	}
	p.RegisterEndpoint("http://primary.internal", "primary", WorkloadTypeGeneral)
	p.Router().RouteManager().AddRoute(&Route{
		Name:       "default/stream-error",
		Operations: map[OperationType]bool{OperationType("embed"): true},
		Destinations: []Destination{
			{Pool: "primary", Weight: 100},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/api/embed", bytes.NewBufferString(`{"model":"bge-small-en-v1.5"}`))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	p.handleEmbed(recorder, req)

	cb := p.registry.GetCircuitBreaker("http://primary.internal")
	if cb == nil {
		t.Fatal("expected circuit breaker for endpoint")
	}
	if failures := atomic.LoadInt32(&cb.failures); failures != 1 {
		t.Fatalf("expected streaming error to record circuit-breaker failure, got %d", failures)
	}
}

func TestResolveRequestUsesVerifiedHostedSource(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{
		DefaultPool:     "default",
		RefreshInterval: time.Minute,
		Logger:          zap.NewNop(),
	})
	p.registry.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(req.URL.Host)),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		}),
	}

	p.RegisterEndpoint("http://default.internal", "default", WorkloadTypeGeneral)
	p.RegisterEndpoint("http://source.internal", "source-pool", WorkloadTypeGeneral)
	p.Router().RouteManager().AddRoute(&Route{
		Name:                "default/source-context",
		Operations:          map[OperationType]bool{OperationType("embed"): true},
		SourceOrganizations: map[string]bool{"org-1": true},
		SourceProjects:      map[string]bool{"project-1": true},
		SourceAPIKeys:       map[string]bool{"deadbeef": true},
		Destinations: []Destination{
			{Pool: "source-pool", Weight: 100},
		},
	})

	resolved, err := p.ResolveRequest(context.Background(), ResolveRequest{
		Operation: OperationType("embed"),
		Model:     "bge-small-en-v1.5",
		Headers:   map[string]string{"Content-Type": "application/json"},
		Source: VerifiedSource{
			OrganizationID: "org-1",
			ProjectID:      "project-1",
			APIKeyPrefix:   "deadbeef",
		},
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("ResolveRequest: %v", err)
	}
	if got := resolved.Endpoint.Address; got != "http://source.internal" {
		t.Fatalf("expected hosted source route to resolve source-pool, got %q", got)
	}
}

func TestResolveRequestStaysInSelectedPoolWhenModelIsLoadedElsewhere(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{
		DefaultPool:     "default",
		RefreshInterval: time.Minute,
		Logger:          zap.NewNop(),
	})

	p.RegisterEndpoint("http://default.internal", "default", WorkloadTypeGeneral)
	p.RegisterEndpoint("http://source.internal", "source-pool", WorkloadTypeGeneral)
	p.registry.UpdateModels("http://default.internal", []string{"bge-small-en-v1.5"})

	p.Router().RouteManager().AddRoute(&Route{
		Name:                "default/source-context",
		Operations:          map[OperationType]bool{OperationType("embed"): true},
		SourceOrganizations: map[string]bool{"org-1": true},
		Destinations: []Destination{
			{Pool: "source-pool", Weight: 100},
		},
	})

	resolved, err := p.ResolveRequest(context.Background(), ResolveRequest{
		Operation: OperationType("embed"),
		Model:     "bge-small-en-v1.5",
		Headers:   map[string]string{"Content-Type": "application/json"},
		Source: VerifiedSource{
			OrganizationID: "org-1",
		},
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("ResolveRequest: %v", err)
	}
	if got := resolved.Endpoint.Address; got != "http://source.internal" {
		t.Fatalf("expected route-selected pool to remain authoritative, got %q", got)
	}
}

func TestProxyRequestDoesNotMatchHostedSourceRouteFromHeadersAlone(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{
		DefaultPool:     "default",
		RefreshInterval: time.Minute,
		Logger:          zap.NewNop(),
	})
	p.registry.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(req.URL.Host)),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		}),
	}

	p.RegisterEndpoint("http://default.internal", "default", WorkloadTypeGeneral)
	p.RegisterEndpoint("http://source.internal", "source-pool", WorkloadTypeGeneral)
	p.Router().RouteManager().AddRoute(&Route{
		Name:                "default/source-context",
		Operations:          map[OperationType]bool{OperationType("embed"): true},
		SourceOrganizations: map[string]bool{"org-1": true},
		SourceProjects:      map[string]bool{"project-1": true},
		SourceAPIKeys:       map[string]bool{"deadbeef": true},
		Destinations: []Destination{
			{Pool: "source-pool", Weight: 100},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/api/embed", bytes.NewBufferString(`{"model":"bge-small-en-v1.5"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Antfly-Project", "project-1")
	req.Header.Set("X-Antfly-Organization", "org-1")
	req.Header.Set("X-Antfly-API-Key-Prefix", "deadbeef")
	recorder := httptest.NewRecorder()

	p.handleEmbed(recorder, req)

	resp := recorder.Result()
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if got := string(body); got != "default.internal" {
		t.Fatalf("expected hosted source route to ignore caller-controlled headers, got %q", got)
	}
}

func TestProxyQueueFallbackWaitsForEligibleDestination(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{
		DefaultPool:     "default",
		RefreshInterval: time.Minute,
		Logger:          zap.NewNop(),
	})
	p.registry.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(req.URL.Host)),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		}),
	}

	p.RegisterEndpoint("http://default.internal", "default", WorkloadTypeGeneral)
	p.Router().RouteManager().AddRoute(&Route{
		Name:       "default/queue",
		Operations: map[OperationType]bool{OperationType("embed"): true},
		Destinations: []Destination{
			{Pool: "queued", Weight: 100},
		},
		Fallback: &Fallback{
			Action:       "queue",
			MaxQueueTime: 300 * time.Millisecond,
		},
	})

	go func() {
		time.Sleep(50 * time.Millisecond)
		p.RegisterEndpoint("http://queued.internal", "queued", WorkloadTypeGeneral)
	}()

	req := httptest.NewRequest(http.MethodPost, "/api/embed", bytes.NewBufferString(`{"model":"bge-small-en-v1.5"}`))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	start := time.Now()
	p.handleEmbed(recorder, req)
	elapsed := time.Since(start)

	resp := recorder.Result()
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if got := string(body); got != "queued.internal" {
		t.Fatalf("expected queued destination to be used, got %q", got)
	}
	if elapsed < 50*time.Millisecond {
		t.Fatalf("expected request to wait for queued destination, only waited %s", elapsed)
	}
}

func TestBurstRoutingDistributesAcrossEligibleEndpoints(t *testing.T) {
	t.Parallel()

	registry := NewModelRegistry(time.Minute)
	registry.RegisterEndpoint("http://burst-a.internal", "burst", WorkloadTypeBurst)
	registry.RegisterEndpoint("http://burst-b.internal", "burst", WorkloadTypeBurst)
	registry.UpdateModels("http://burst-a.internal", []string{"model-a"})
	registry.UpdateModels("http://burst-b.internal", []string{"model-a"})

	router := NewRouter(registry)

	var seenA bool
	var seenB bool
	for i := 0; i < 6; i++ {
		endpoint, err := router.RouteRequest(context.Background(), "model-a", "burst", WorkloadTypeBurst, nil)
		if err != nil {
			t.Fatalf("RouteRequest returned error: %v", err)
		}
		switch endpoint.Address {
		case "http://burst-a.internal":
			seenA = true
		case "http://burst-b.internal":
			seenB = true
		default:
			t.Fatalf("unexpected endpoint %q", endpoint.Address)
		}
	}

	if !seenA || !seenB {
		t.Fatalf("expected burst routing to distribute across both endpoints, sawA=%t sawB=%t", seenA, seenB)
	}
}

func TestResolveRequestDoesNotAdvanceBurstRoundRobinState(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{
		DefaultPool:     "burst",
		RefreshInterval: time.Minute,
		Logger:          zap.NewNop(),
	})
	p.RegisterEndpoint("http://burst-a.internal", "burst", WorkloadTypeBurst)
	p.RegisterEndpoint("http://burst-b.internal", "burst", WorkloadTypeBurst)
	p.registry.UpdateModels("http://burst-a.internal", []string{"model-a"})
	p.registry.UpdateModels("http://burst-b.internal", []string{"model-a"})

	firstLease, err := p.AcquireRequestResolution(context.Background(), ResolveRequest{
		Operation: OperationType("embed"),
		Model:     "model-a",
		Headers: map[string]string{
			"Content-Type":            "application/json",
			"X-Termite-Workload-Type": string(WorkloadTypeBurst),
		},
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("initial AcquireRequestResolution: %v", err)
	}
	firstLease.Release()

	var previewEndpoint string
	for i := 0; i < 3; i++ {
		resolved, err := p.ResolveRequest(context.Background(), ResolveRequest{
			Operation: OperationType("embed"),
			Model:     "model-a",
			Headers: map[string]string{
				"Content-Type":            "application/json",
				"X-Termite-Workload-Type": string(WorkloadTypeBurst),
			},
			Timestamp: time.Now(),
		})
		if err != nil {
			t.Fatalf("ResolveRequest %d: %v", i+1, err)
		}
		if i == 0 {
			previewEndpoint = resolved.Endpoint.Address
			continue
		}
		if resolved.Endpoint.Address != previewEndpoint {
			t.Fatalf("expected burst preview to remain stable at %q, got %q", previewEndpoint, resolved.Endpoint.Address)
		}
	}

	nextLease, err := p.AcquireRequestResolution(context.Background(), ResolveRequest{
		Operation: OperationType("embed"),
		Model:     "model-a",
		Headers: map[string]string{
			"Content-Type":            "application/json",
			"X-Termite-Workload-Type": string(WorkloadTypeBurst),
		},
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("next AcquireRequestResolution: %v", err)
	}
	if nextLease.Resolution.Endpoint.Address != previewEndpoint {
		t.Fatalf("expected ResolveRequest previews not to advance burst selector, got %q want %q", nextLease.Resolution.Endpoint.Address, previewEndpoint)
	}
}

func TestResolveRequestDoesNotFallThroughToDefaultPoolAfterMatchedRoute(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{
		DefaultPool:     "default",
		RefreshInterval: time.Minute,
		Logger:          zap.NewNop(),
	})
	p.RegisterEndpoint("http://default.internal", "default", WorkloadTypeGeneral)
	p.Router().RouteManager().AddRoute(&Route{
		Name:                "default/matched-no-dest",
		Operations:          map[OperationType]bool{OperationType("embed"): true},
		SourceOrganizations: map[string]bool{"org-1": true},
		Destinations: []Destination{
			{Pool: "missing-pool", Weight: 100},
		},
	})

	_, err := p.ResolveRequest(context.Background(), ResolveRequest{
		Operation: OperationType("embed"),
		Model:     "bge-small-en-v1.5",
		Headers:   map[string]string{"Content-Type": "application/json"},
		Source: VerifiedSource{
			OrganizationID: "org-1",
		},
		Timestamp: time.Now(),
	})
	if err == nil {
		t.Fatal("expected resolve to fail when matched route has no eligible destinations")
	}

	var resolutionErr *ResolutionError
	if !errors.As(err, &resolutionErr) {
		t.Fatalf("expected ResolutionError, got %T", err)
	}
	if resolutionErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resolutionErr.StatusCode)
	}
	if resolutionErr.Message != "no eligible destinations for matched route" {
		t.Fatalf("unexpected error message %q", resolutionErr.Message)
	}
}

func TestSelectDestinationHonorsLatencyCondition(t *testing.T) {
	t.Parallel()

	registry := NewModelRegistry(time.Minute)
	registry.RegisterEndpoint("http://slow.internal", "slow", WorkloadTypeGeneral)
	registry.RegisterEndpoint("http://fast.internal", "fast", WorkloadTypeGeneral)
	registry.UpdateModels("http://slow.internal", []string{"model-a"})
	registry.UpdateModels("http://fast.internal", []string{"model-a"})
	for i := 0; i < 98; i++ {
		registry.RecordModelLatency("http://slow.internal", "model-a", 50*time.Millisecond)
		registry.RecordModelLatency("http://fast.internal", "model-a", 50*time.Millisecond)
	}
	registry.RecordModelLatency("http://slow.internal", "model-a", 250*time.Millisecond)
	registry.RecordModelLatency("http://slow.internal", "model-a", 250*time.Millisecond)

	rm := NewRouteManager()
	route := &Route{
		Name: "default/latency",
		Destinations: []Destination{
			{
				Pool:             "slow",
				Weight:           100,
				LatencyCondition: &ThresholdCondition{Operator: ">", Value: 0.2},
			},
			{
				Pool:             "fast",
				Weight:           100,
				LatencyCondition: &ThresholdCondition{Operator: ">", Value: 0.2},
			},
		},
	}

	dest, err := rm.SelectDestination(route, &RouteRequest{
		Operation: OperationType("embed"),
		Model:     "model-a",
		Timestamp: time.Now(),
	}, registry)
	if err != nil {
		t.Fatalf("SelectDestination returned error: %v", err)
	}
	if dest == nil {
		t.Fatal("expected a matching destination")
	}
	if dest.Pool != "slow" {
		t.Fatalf("expected latency condition to match slow pool first, got %q", dest.Pool)
	}
}

func TestSelectDestinationSkipsOpenCircuitPools(t *testing.T) {
	t.Parallel()

	registry := NewModelRegistry(time.Minute)
	registry.RegisterEndpoint("http://open.internal", "open-pool", WorkloadTypeGeneral)
	registry.RegisterEndpoint("http://healthy.internal", "healthy-pool", WorkloadTypeGeneral)
	registry.UpdateModels("http://open.internal", []string{"model-a"})
	registry.UpdateModels("http://healthy.internal", []string{"model-a"})

	openCB := registry.GetCircuitBreaker("http://open.internal")
	for i := 0; i < 5; i++ {
		openCB.RecordFailure()
	}

	rm := NewRouteManager()
	route := &Route{
		Name: "default/circuit-breaker",
		Destinations: []Destination{
			{
				Pool:             "open-pool",
				Weight:           100,
				ReplicaCondition: &ThresholdCondition{Operator: ">=", Value: 1},
			},
			{
				Pool:             "healthy-pool",
				Weight:           100,
				ReplicaCondition: &ThresholdCondition{Operator: ">=", Value: 1},
			},
		},
	}

	dest, err := rm.SelectDestination(route, &RouteRequest{
		Operation: OperationType("embed"),
		Model:     "model-a",
		Timestamp: time.Now(),
	}, registry)
	if err != nil {
		t.Fatalf("SelectDestination returned error: %v", err)
	}
	if dest == nil {
		t.Fatal("expected a matching destination")
	}
	if dest.Pool != "healthy-pool" {
		t.Fatalf("expected open-circuit pool to be skipped, got %q", dest.Pool)
	}
}

func TestRouteRequestClaimsRecoveredCircuitOnce(t *testing.T) {
	t.Parallel()

	registry := NewModelRegistry(time.Minute)
	registry.RegisterEndpoint("http://recovering.internal", "primary", WorkloadTypeGeneral)
	registry.UpdateModels("http://recovering.internal", []string{"model-a"})

	cb := registry.GetCircuitBreaker("http://recovering.internal")
	cb.threshold = 1
	cb.timeout = 10 * time.Millisecond
	cb.RecordFailure()
	time.Sleep(20 * time.Millisecond)

	router := NewRouter(registry)

	endpoint, err := router.RouteRequest(context.Background(), "model-a", "primary", WorkloadTypeGeneral, nil)
	if err != nil {
		t.Fatalf("RouteRequest returned error: %v", err)
	}
	if endpoint.Address != "http://recovering.internal" {
		t.Fatalf("expected recovering endpoint, got %q", endpoint.Address)
	}
	if got := atomic.LoadInt32(&cb.state); got != 2 {
		t.Fatalf("expected recovered request to claim half-open probe, got state=%d", got)
	}
}

func TestResolveRequestDoesNotClaimCircuitBreakerProbe(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{
		DefaultPool:     "primary",
		RefreshInterval: time.Minute,
		Logger:          zap.NewNop(),
	})
	p.RegisterEndpoint("http://recovering.internal", "primary", WorkloadTypeGeneral)
	p.registry.UpdateModels("http://recovering.internal", []string{"model-a"})

	cb := p.registry.GetCircuitBreaker("http://recovering.internal")
	cb.threshold = 1
	cb.timeout = 10 * time.Millisecond
	cb.RecordFailure()
	time.Sleep(20 * time.Millisecond)

	resolved, err := p.ResolveRequest(context.Background(), ResolveRequest{
		Operation: OperationType("embed"),
		Model:     "model-a",
		Headers:   map[string]string{"Content-Type": "application/json"},
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("ResolveRequest: %v", err)
	}
	if resolved.Endpoint.Address != "http://recovering.internal" {
		t.Fatalf("expected resolving endpoint, got %q", resolved.Endpoint.Address)
	}
	if got := atomic.LoadInt32(&cb.state); got != 1 {
		t.Fatalf("expected pure ResolveRequest to leave breaker open, got state=%d", got)
	}

	lease, err := p.AcquireRequestResolution(context.Background(), ResolveRequest{
		Operation: OperationType("embed"),
		Model:     "model-a",
		Headers:   map[string]string{"Content-Type": "application/json"},
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("AcquireRequestResolution: %v", err)
	}
	if got := atomic.LoadInt32(&cb.state); got != 2 {
		t.Fatalf("expected acquired resolution to claim half-open probe, got state=%d", got)
	}
	lease.Release()
	if got := atomic.LoadInt32(&cb.state); got != 1 {
		t.Fatalf("expected released lease to return breaker to open, got state=%d", got)
	}
}

func TestResolveRequestDoesNotConsumeRouteRateLimit(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{
		DefaultPool:     "primary",
		RefreshInterval: time.Minute,
		Logger:          zap.NewNop(),
	})
	p.RegisterEndpoint("http://primary.internal", "primary", WorkloadTypeGeneral)

	route := &Route{
		Name:         "default/rate-limit",
		Operations:   map[OperationType]bool{OperationType("embed"): true},
		Destinations: []Destination{{Pool: "primary", Weight: 100}},
		RateLimiter:  NewRateLimiter(1, 1, false),
	}
	p.Router().RouteManager().AddRoute(route)

	for i := 0; i < 2; i++ {
		if _, err := p.ResolveRequest(context.Background(), ResolveRequest{
			Operation: OperationType("embed"),
			Model:     "model-a",
			Headers:   map[string]string{"Content-Type": "application/json"},
			Timestamp: time.Now(),
		}); err != nil {
			t.Fatalf("ResolveRequest %d unexpectedly failed: %v", i+1, err)
		}
	}

	lease, err := p.AcquireRequestResolution(context.Background(), ResolveRequest{
		Operation: OperationType("embed"),
		Model:     "model-a",
		Headers:   map[string]string{"Content-Type": "application/json"},
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("AcquireRequestResolution: %v", err)
	}
	if err := lease.Admit(); err != nil {
		t.Fatalf("Admit unexpectedly failed: %v", err)
	}

	nextLease, err := p.AcquireRequestResolution(context.Background(), ResolveRequest{
		Operation: OperationType("embed"),
		Model:     "model-a",
		Headers:   map[string]string{"Content-Type": "application/json"},
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("second AcquireRequestResolution: %v", err)
	}
	if err := nextLease.Admit(); err == nil {
		t.Fatal("expected second Admit to be rate limited")
	}
}

func TestAcquireRequestResolutionUsesResolvedModelForPerModelRateLimit(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{
		DefaultPool:     "primary",
		RefreshInterval: time.Minute,
		Logger:          zap.NewNop(),
	})
	p.RegisterEndpoint("http://primary.internal", "primary", WorkloadTypeGeneral)

	route := &Route{
		Name:         "default/per-model-rate-limit",
		Operations:   map[OperationType]bool{OperationType("embed"): true},
		Destinations: []Destination{{Pool: "primary", Weight: 100}},
		RateLimiter:  NewRateLimiter(1, 1, true),
	}
	p.Router().RouteManager().AddRoute(route)

	modelALease, err := p.AcquireRequestResolution(context.Background(), ResolveRequest{
		Operation: OperationType("embed"),
		Model:     "model-a",
		Headers:   map[string]string{"Content-Type": "application/json"},
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("AcquireRequestResolution model-a: %v", err)
	}
	if err := modelALease.Admit(); err != nil {
		t.Fatalf("Admit model-a unexpectedly failed: %v", err)
	}

	modelBLease, err := p.AcquireRequestResolution(context.Background(), ResolveRequest{
		Operation: OperationType("embed"),
		Model:     "model-b",
		Headers:   map[string]string{"Content-Type": "application/json"},
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("AcquireRequestResolution model-b: %v", err)
	}
	if err := modelBLease.Admit(); err != nil {
		t.Fatalf("Admit model-b unexpectedly failed: %v", err)
	}

	secondModelALease, err := p.AcquireRequestResolution(context.Background(), ResolveRequest{
		Operation: OperationType("embed"),
		Model:     "model-a",
		Headers:   map[string]string{"Content-Type": "application/json"},
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("second AcquireRequestResolution model-a: %v", err)
	}
	if err := secondModelALease.Admit(); err == nil {
		t.Fatal("expected second model-a Admit to be rate limited")
	}
}

func TestAcquireRequestResolutionBeginForwardingUpdatesLeastLoadedSelection(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{
		DefaultPool:     "primary",
		RefreshInterval: time.Minute,
		Logger:          zap.NewNop(),
	})
	p.RegisterEndpoint("http://primary-a.internal", "primary", WorkloadTypeGeneral)
	p.RegisterEndpoint("http://primary-b.internal", "primary", WorkloadTypeGeneral)
	p.registry.UpdateModels("http://primary-a.internal", []string{"model-a"})
	p.registry.UpdateModels("http://primary-b.internal", []string{"model-a"})

	firstLease, err := p.AcquireRequestResolution(context.Background(), ResolveRequest{
		Operation: OperationType("embed"),
		Model:     "model-a",
		Headers:   map[string]string{"Content-Type": "application/json"},
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("first AcquireRequestResolution: %v", err)
	}

	inFlight := firstLease.BeginForwarding()
	defer inFlight.Finish()

	secondLease, err := p.AcquireRequestResolution(context.Background(), ResolveRequest{
		Operation: OperationType("embed"),
		Model:     "model-a",
		Headers:   map[string]string{"Content-Type": "application/json"},
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("second AcquireRequestResolution: %v", err)
	}

	if firstLease.Resolution.Endpoint.Address == secondLease.Resolution.Endpoint.Address {
		t.Fatalf("expected in-flight load to shift least-loaded selection, both leases resolved %q", firstLease.Resolution.Endpoint.Address)
	}

	firstLease.Release()
	secondLease.Release()
}

func TestResolutionLeaseNextAttemptSharesAdmissionDecision(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{
		DefaultPool:     "primary",
		RefreshInterval: time.Minute,
		Logger:          zap.NewNop(),
	})
	p.RegisterEndpoint("http://primary-a.internal", "primary", WorkloadTypeGeneral)
	p.RegisterEndpoint("http://primary-b.internal", "primary", WorkloadTypeGeneral)

	route := &Route{
		Name:          "default/retry-admission",
		Operations:    map[OperationType]bool{OperationType("embed"): true},
		Destinations:  []Destination{{Pool: "primary", Weight: 100}},
		RateLimiter:   NewRateLimiter(1, 1, false),
		RetryAttempts: 2,
	}
	p.Router().RouteManager().AddRoute(route)

	firstLease, err := p.AcquireRequestResolution(context.Background(), ResolveRequest{
		Operation: OperationType("embed"),
		Model:     "model-a",
		Headers:   map[string]string{"Content-Type": "application/json"},
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("first AcquireRequestResolution: %v", err)
	}
	if err := firstLease.Admit(); err != nil {
		t.Fatalf("first Admit unexpectedly failed: %v", err)
	}

	firstLease.RecordFailure()

	retryLease, err := firstLease.NextAttempt(context.Background())
	if err != nil {
		t.Fatalf("NextAttempt: %v", err)
	}
	if err := retryLease.Admit(); err != nil {
		t.Fatalf("retry Admit should reuse logical admission, got %v", err)
	}

	newLease, err := p.AcquireRequestResolution(context.Background(), ResolveRequest{
		Operation: OperationType("embed"),
		Model:     "model-a",
		Headers:   map[string]string{"Content-Type": "application/json"},
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("new AcquireRequestResolution: %v", err)
	}
	if err := newLease.Admit(); err == nil {
		t.Fatal("expected new logical request to be rate limited")
	}
}

func TestResolutionLeaseNextAttemptRequiresCompletedCurrentAttempt(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{
		DefaultPool:     "primary",
		RefreshInterval: time.Minute,
		Logger:          zap.NewNop(),
	})
	p.RegisterEndpoint("http://primary-a.internal", "primary", WorkloadTypeGeneral)
	p.RegisterEndpoint("http://primary-b.internal", "primary", WorkloadTypeGeneral)
	p.Router().RouteManager().AddRoute(&Route{
		Name:       "default/retry-ordering",
		Operations: map[OperationType]bool{OperationType("embed"): true},
		Destinations: []Destination{
			{Pool: "primary", Weight: 100},
		},
		RetryAttempts: 2,
	})

	lease, err := p.AcquireRequestResolution(context.Background(), ResolveRequest{
		Operation: OperationType("embed"),
		Model:     "model-a",
		Headers:   map[string]string{"Content-Type": "application/json"},
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("AcquireRequestResolution: %v", err)
	}

	_, err = lease.NextAttempt(context.Background())
	if err == nil {
		t.Fatal("expected NextAttempt to require current attempt completion")
	}

	var resolutionErr *ResolutionError
	if !errors.As(err, &resolutionErr) {
		t.Fatalf("expected ResolutionError, got %T", err)
	}
	if resolutionErr.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 status, got %d", resolutionErr.StatusCode)
	}
	if resolutionErr.Message != "cannot reacquire endpoint before completing current attempt" {
		t.Fatalf("unexpected error message %q", resolutionErr.Message)
	}

	lease.Release()
}

func TestResolutionLeaseNextAttemptExcludesFailedEndpoint(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{
		DefaultPool:     "primary",
		RefreshInterval: time.Minute,
		Logger:          zap.NewNop(),
	})
	p.RegisterEndpoint("http://primary-a.internal", "primary", WorkloadTypeGeneral)
	p.RegisterEndpoint("http://primary-b.internal", "primary", WorkloadTypeGeneral)
	atomic.StoreInt32(&p.registry.endpoints["http://primary-b.internal"].Connections, 1)
	p.Router().RouteManager().AddRoute(&Route{
		Name:       "default/retry-endpoints",
		Operations: map[OperationType]bool{OperationType("embed"): true},
		Destinations: []Destination{
			{Pool: "primary", Weight: 100},
		},
		RetryAttempts: 2,
	})

	firstLease, err := p.AcquireRequestResolution(context.Background(), ResolveRequest{
		Operation: OperationType("embed"),
		Model:     "model-a",
		Headers:   map[string]string{"Content-Type": "application/json"},
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("first AcquireRequestResolution: %v", err)
	}
	firstEndpoint := firstLease.Resolution.Endpoint.Address

	firstLease.RecordFailure()

	retryLease, err := firstLease.NextAttempt(context.Background())
	if err != nil {
		t.Fatalf("NextAttempt: %v", err)
	}
	if retryLease.Resolution.Endpoint.Address == firstEndpoint {
		t.Fatalf("expected retry attempt to exclude failed endpoint %q", firstEndpoint)
	}
	retryLease.Release()
}

func TestProxyRequestKeepsConnectionCountUntilResponseBodyCloses(t *testing.T) {
	t.Parallel()

	bodyReader, bodyWriter := io.Pipe()
	defer func() { _ = bodyWriter.Close() }()

	p := NewProxy(Config{
		DefaultPool:     "primary",
		RefreshInterval: time.Minute,
		Logger:          zap.NewNop(),
	})
	p.registry.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       bodyReader,
				Request:    req,
			}, nil
		}),
	}
	p.RegisterEndpoint("http://primary.internal", "primary", WorkloadTypeGeneral)
	p.registry.UpdateModels("http://primary.internal", []string{"model-a"})

	req := httptest.NewRequest(http.MethodPost, "/api/embed", bytes.NewBufferString(`{"model":"model-a"}`))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		p.handleEmbed(recorder, req)
		close(done)
	}()

	var endpoint *Endpoint
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		p.registry.mu.RLock()
		endpoint = p.registry.endpoints["http://primary.internal"]
		var connections int32
		if endpoint != nil {
			connections = atomic.LoadInt32(&endpoint.Connections)
		}
		p.registry.mu.RUnlock()
		if connections == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if endpoint == nil {
		t.Fatal("expected endpoint to be registered")
	}
	if got := atomic.LoadInt32(&endpoint.Connections); got != 1 {
		t.Fatalf("expected connection count to remain active while response body is open, got %d", got)
	}

	select {
	case <-done:
		t.Fatal("proxy request completed before backend response body closed")
	default:
	}

	if _, err := bodyWriter.Write([]byte(`{"ok":true}`)); err != nil {
		t.Fatalf("bodyWriter.Write: %v", err)
	}
	if err := bodyWriter.Close(); err != nil {
		t.Fatalf("bodyWriter.Close: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("proxy request did not complete after backend body closed")
	}

	if got := atomic.LoadInt32(&endpoint.Connections); got != 0 {
		t.Fatalf("expected connection count to drop after response body close, got %d", got)
	}
}

func TestReadyRequiresRoutableEndpoint(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{
		DefaultPool:     "primary",
		RefreshInterval: time.Minute,
		Logger:          zap.NewNop(),
	})
	p.RegisterEndpoint("http://primary.internal", "primary", WorkloadTypeGeneral)

	cb := p.registry.GetCircuitBreaker("http://primary.internal")
	if cb == nil {
		t.Fatal("expected circuit breaker for endpoint")
	}
	for i := 0; i < int(cb.threshold); i++ {
		cb.RecordFailure()
	}

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recorder := httptest.NewRecorder()

	p.handleReady(recorder, req)

	resp := recorder.Result()
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected readiness to fail when all endpoints are open-circuit, got %d", resp.StatusCode)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

type errReadCloser struct {
	data []byte
	err  error
	read bool
}

func (r *errReadCloser) Read(p []byte) (int, error) {
	if r.read {
		return 0, r.err
	}
	r.read = true
	n := copy(p, r.data)
	return n, nil
}

func (r *errReadCloser) Close() error {
	return nil
}
