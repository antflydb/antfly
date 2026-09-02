package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
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
	p.RegisterEndpoint("http://inference.internal", "primary", WorkloadTypeGeneral)
	p.Router().RouteManager().AddRoute(&Route{
		Name:       "default/retry",
		Operations: map[OperationType]bool{OperationType("embed"): true},
		Destinations: []Destination{
			{Pool: "primary", Weight: 100},
		},
		RetryAttempts:   2,
		RetryOnStatuses: map[int]bool{http.StatusInternalServerError: true},
	})

	req := httptest.NewRequest(http.MethodPost, "/ai/v1/embed", bytes.NewBufferString(`{"model":"bge-small-en-v1.5"}`))
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

	req := httptest.NewRequest(http.MethodPost, "/ai/v1/embed", bytes.NewBufferString(`{"model":"bge-small-en-v1.5"}`))
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

	req := httptest.NewRequest(http.MethodPost, "/ai/v1/embed", bytes.NewBufferString(`{"model":"bge-small-en-v1.5"}`))
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

	req := httptest.NewRequest(http.MethodPost, "/ai/v1/embed", bytes.NewBufferString(`{"model":"bge-small-en-v1.5"}`))
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

	req := httptest.NewRequest(http.MethodPost, "/ai/v1/embed", bytes.NewBufferString(`{"model":"bge-small-en-v1.5"}`))
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

func TestRefreshEndpointSendsUpstreamAuthorization(t *testing.T) {
	t.Parallel()

	const authToken = "Bearer bridge-token"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != authToken {
			http.Error(w, "missing auth", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/readyz":
			w.WriteHeader(http.StatusOK)
		case "/ai/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[{"id":"model-a"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	p := NewProxy(Config{
		RefreshInterval:       time.Minute,
		UpstreamAuthorization: authToken,
		Logger:                zap.NewNop(),
	})
	p.RegisterEndpointWithHealth(server.URL, server.URL+"/readyz", "bridge", WorkloadTypeGeneral)

	if err := p.registry.RefreshEndpoint(context.Background(), server.URL); err != nil {
		t.Fatalf("RefreshEndpoint returned error: %v", err)
	}

	endpoint, err := p.router.RouteRequest(context.Background(), "model-a", "bridge", WorkloadTypeGeneral, nil)
	if err != nil {
		t.Fatalf("RouteRequest returned error: %v", err)
	}
	if endpoint.Address != server.URL {
		t.Fatalf("expected endpoint %q, got %q", server.URL, endpoint.Address)
	}
}

func TestForwardRequestSendsUpstreamAuthorization(t *testing.T) {
	t.Parallel()

	const authorization = "Bearer bridge-token"
	var gotAuthorization atomic.Value

	p := NewProxy(Config{
		DefaultPool:           "default",
		RefreshInterval:       time.Minute,
		UpstreamAuthorization: authorization,
		Logger:                zap.NewNop(),
	})
	p.registry.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			gotAuthorization.Store(req.Header.Get("Authorization"))
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewBufferString(`{"ok":true}`)),
				Request:    req,
			}, nil
		}),
	}
	p.RegisterEndpoint("http://inference.internal", "default", WorkloadTypeGeneral)
	p.registry.UpdateModels("http://inference.internal", []string{"model-a"})

	req := httptest.NewRequest(http.MethodPost, "/ai/v1/embed", bytes.NewBufferString(`{"model":"model-a"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer caller-token")
	recorder := httptest.NewRecorder()

	p.handleEmbed(recorder, req)

	resp := recorder.Result()
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected OK, got %d", resp.StatusCode)
	}
	if got := gotAuthorization.Load(); got != authorization {
		t.Fatalf("expected upstream Authorization %q, got %q", authorization, got)
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
			"Content-Type":                     "application/json",
			"X-Antfly-Inference-Workload-Type": string(WorkloadTypeBurst),
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
				"Content-Type":                     "application/json",
				"X-Antfly-Inference-Workload-Type": string(WorkloadTypeBurst),
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
			"Content-Type":                     "application/json",
			"X-Antfly-Inference-Workload-Type": string(WorkloadTypeBurst),
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

	req := httptest.NewRequest(http.MethodPost, "/ai/v1/embed", bytes.NewBufferString(`{"model":"model-a"}`))
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

func TestExtractModelNamesSupportsCategorizedResponses(t *testing.T) {
	body := strings.NewReader(`{
		"object": "list",
		"data": [{"id": "openai-compatible"}],
		"embedders": {"embedder-a": {"backend": "metal"}},
		"generators": {"generator-a": {"backend": "metal"}},
		"rerankers": ["reranker-a"]
	}`)

	models, err := extractModelNames(body)
	if err != nil {
		t.Fatalf("extractModelNames() error = %v", err)
	}

	want := []string{"embedder-a", "generator-a", "openai-compatible", "reranker-a"}
	if !slices.Equal(models, want) {
		t.Fatalf("extractModelNames() = %#v, want %#v", models, want)
	}
}

func TestExtractModelOperationsPreservesTaskIdentity(t *testing.T) {
	operations, err := extractModelOperations(strings.NewReader(`{
		"data": [{"id": "legacy"}],
		"generators": {"shared": {}},
		"readers": {"shared": {}, "reader-only": {}}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if !operations["shared"]["generate"] || !operations["shared"]["generate.batch"] || !operations["shared"]["read"] {
		t.Fatalf("shared operations = %#v", operations["shared"])
	}
	if operations["reader-only"]["generate"] || !operations["reader-only"]["read"] {
		t.Fatalf("reader-only operations = %#v", operations["reader-only"])
	}
	if len(operations["legacy"]) != 0 {
		t.Fatalf("legacy generic catalog must remain task-unknown, got %#v", operations["legacy"])
	}
}

func TestResolveFiltersDiscoveredModelByOperation(t *testing.T) {
	p := NewProxy(Config{DefaultPool: "primary", RefreshInterval: time.Minute, Logger: zap.NewNop()})
	p.RegisterEndpoint("http://generator.internal", "primary", WorkloadTypeGeneral)
	p.RegisterEndpoint("http://reader.internal", "primary", WorkloadTypeGeneral)
	p.registry.UpdateModelOperations("http://generator.internal", map[string]map[OperationType]bool{
		"shared": {"generate": true, "generate.batch": true},
	})
	p.registry.UpdateModelOperations("http://reader.internal", map[string]map[OperationType]bool{
		"shared": {"read": true},
	})

	readResolution, err := p.ResolveRequest(context.Background(), ResolveRequest{Operation: "read", Model: "shared"})
	if err != nil {
		t.Fatal(err)
	}
	if readResolution.Endpoint.Address != "http://reader.internal" {
		t.Fatalf("read routed to %s", readResolution.Endpoint.Address)
	}
	generateResolution, err := p.ResolveRequest(context.Background(), ResolveRequest{Operation: "generate", Model: "shared"})
	if err != nil {
		t.Fatal(err)
	}
	if generateResolution.Endpoint.Address != "http://generator.internal" {
		t.Fatalf("generation routed to %s", generateResolution.Endpoint.Address)
	}
}

func TestResolveRejectsSuccessfullyDiscoveredTaskUnknownModel(t *testing.T) {
	p := NewProxy(Config{DefaultPool: "primary", RefreshInterval: time.Minute, Logger: zap.NewNop()})
	p.RegisterEndpoint("http://legacy.internal", "primary", WorkloadTypeGeneral)
	p.registry.UpdateModelOperations("http://legacy.internal", map[string]map[OperationType]bool{
		"shared": {},
	})

	_, err := p.ResolveRequest(context.Background(), ResolveRequest{Operation: "read", Model: "shared"})
	if err == nil {
		t.Fatal("task-unknown discovered model must not receive a read request")
	}
}

func TestResolveUsesBootstrapFallbackBeforeCatalogDiscovery(t *testing.T) {
	p := NewProxy(Config{DefaultPool: "primary", RefreshInterval: time.Minute, Logger: zap.NewNop()})
	p.RegisterEndpoint("http://bootstrap.internal", "primary", WorkloadTypeGeneral)
	p.registry.UpdateModels("http://bootstrap.internal", []string{"shared"})

	resolution, err := p.ResolveRequest(context.Background(), ResolveRequest{Operation: "read", Model: "shared"})
	if err != nil {
		t.Fatal(err)
	}
	if resolution.Endpoint.Address != "http://bootstrap.internal" {
		t.Fatalf("read routed to %s", resolution.Endpoint.Address)
	}
}

func TestResolveDoesNotPoolFallbackAfterCatalogDiscovery(t *testing.T) {
	p := NewProxy(Config{DefaultPool: "primary", RefreshInterval: time.Minute, Logger: zap.NewNop()})
	p.RegisterEndpoint("http://known.internal", "primary", WorkloadTypeGeneral)
	p.registry.UpdateModelOperations("http://known.internal", map[string]map[OperationType]bool{
		"model-a": {"generate": true},
	})

	if _, err := p.ResolveRequest(context.Background(), ResolveRequest{Operation: "generate", Model: "missing"}); err == nil {
		t.Fatal("expected discovered endpoint to reject an absent model")
	}
}

func TestProxyRoutesReadAndHomogeneousGenerateBatch(t *testing.T) {
	t.Parallel()

	var pathsMu sync.Mutex
	var paths []string
	p := NewProxy(Config{DefaultPool: "primary", RefreshInterval: time.Minute, Logger: zap.NewNop()})
	p.registry.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		pathsMu.Lock()
		paths = append(paths, req.URL.Path)
		pathsMu.Unlock()
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
			Request:    req,
		}, nil
	})}
	p.RegisterEndpoint("http://inference.internal", "primary", WorkloadTypeGeneral)
	p.registry.UpdateModels("http://inference.internal", []string{"gemma4"})

	read := httptest.NewRequest(http.MethodPost, "/ai/v1/read", strings.NewReader(`{"model":"gemma4"}`))
	readRecorder := httptest.NewRecorder()
	p.handleRead(readRecorder, read)
	if readRecorder.Code != http.StatusOK {
		t.Fatalf("read status = %d, want 200", readRecorder.Code)
	}

	batchBody := `{"mode":"sync","requests":[{"custom_id":"a","body":{"model":"gemma4"}},{"custom_id":"b","body":{"model":"gemma4"}}]}`
	batch := httptest.NewRequest(http.MethodPost, "/ai/v1/generate/batch", strings.NewReader(batchBody))
	batchRecorder := httptest.NewRecorder()
	p.handleGenerateBatch(batchRecorder, batch)
	if batchRecorder.Code != http.StatusOK {
		t.Fatalf("batch status = %d, want 200", batchRecorder.Code)
	}

	pathsMu.Lock()
	defer pathsMu.Unlock()
	if !slices.Equal(paths, []string{"/ai/v1/read", "/ai/v1/generate/batch"}) {
		t.Fatalf("forwarded paths = %v", paths)
	}
}

func TestProxyRejectsMixedModelGenerateBatchBeforeForwarding(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{DefaultPool: "primary", RefreshInterval: time.Minute, Logger: zap.NewNop()})
	forwarded := false
	p.registry.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		forwarded = true
		return nil, errors.New("must not forward")
	})}
	p.RegisterEndpoint("http://inference.internal", "primary", WorkloadTypeGeneral)
	p.registry.UpdateModels("http://inference.internal", []string{"model-a", "model-b"})

	body := `{"requests":[{"custom_id":"a","body":{"model":"model-a"}},{"custom_id":"b","body":{"model":"model-b"}}]}`
	recorder := httptest.NewRecorder()
	p.handleGenerateBatch(recorder, httptest.NewRequest(http.MethodPost, "/ai/v1/generate/batch", strings.NewReader(body)))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
	if forwarded {
		t.Fatal("mixed-model batch was forwarded")
	}
}

func TestProxyBoundsRetainedInferenceRequestBody(t *testing.T) {
	p := NewProxy(Config{DefaultPool: "primary", MaxRequestBodyBytes: 16, Logger: zap.NewNop()})
	p.RegisterEndpoint("http://inference.internal", "primary", WorkloadTypeGeneral)
	p.registry.UpdateModels("http://inference.internal", []string{"model-a"})

	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodPost, "/ai/v1/generate", strings.NewReader(`{"model":"model-a","padding":"large"}`)),
		func() *http.Request {
			req := httptest.NewRequest(http.MethodPost, "/ai/v1/generate", io.NopCloser(strings.NewReader(`{"model":"model-a","padding":"large"}`)))
			req.ContentLength = -1
			return req
		}(),
	} {
		recorder := httptest.NewRecorder()
		p.handleGenerate(recorder, request)
		if recorder.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want 413", recorder.Code)
		}
	}
}

func TestProxyModelCatalogIntersectsDuplicateCapabilities(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{DefaultPool: "primary", RefreshInterval: time.Minute, Logger: zap.NewNop()})
	p.registry.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body := `{"generators":{"gemma4":{"inputs":["text","image"],"capabilities":["native_batch_generate_multimodal","shared"]}}}`
		if req.URL.Host == "serial.internal" {
			body = `{"generators":{"gemma4":{"inputs":["text","image"],"capabilities":["shared"]}}}`
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    req,
		}, nil
	})}
	for _, address := range []string{"http://native.internal", "http://serial.internal"} {
		p.RegisterEndpoint(address, "primary", WorkloadTypeGeneral)
		p.registry.UpdateModels(address, []string{"gemma4"})
	}

	recorder := httptest.NewRecorder()
	p.handleModels(recorder, httptest.NewRequest(http.MethodGet, "/ai/v1/models", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	var response struct {
		Generators map[string]struct {
			Capabilities []string `json:"capabilities"`
		} `json:"generators"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(response.Generators["gemma4"].Capabilities, []string{"shared"}) {
		t.Fatalf("capabilities = %v, want conservative intersection", response.Generators["gemma4"].Capabilities)
	}
}

func TestProxyModelCatalogScopesDiscoveryToSelectedPoolAndTask(t *testing.T) {
	p := NewProxy(Config{DefaultPool: "cpu", RefreshInterval: time.Minute, Logger: zap.NewNop()})
	p.registry.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body := `{"readers":{"florence":{"inputs":["image"]}}}`
		if req.URL.Host == "gpu.internal" {
			body = `{"generators":{"gemma4":{"inputs":["text","image"],"inference_capabilities":{"task":"generate","batch":{"mode":"serial_compatibility","preferred_items":8,"max_items":128,"max_encoded_bytes":104857600,"max_decoded_pixels":1000000,"max_media_parts_per_item":8,"per_item_failures":true}}}}}`
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    req,
		}, nil
	})}
	p.RegisterEndpoint("http://cpu.internal", "cpu", WorkloadTypeGeneral)
	p.RegisterEndpoint("http://gpu.internal", "gpu", WorkloadTypeGeneral)
	p.registry.UpdateModelOperations("http://cpu.internal", map[string]map[OperationType]bool{"florence": {"read": true}})
	p.registry.UpdateModelOperations("http://gpu.internal", map[string]map[OperationType]bool{"gemma4": {"generate.batch": true}})

	request := httptest.NewRequest(http.MethodGet, "/ai/v1/models?model=gemma4&task=generate", nil)
	request.Header.Set("X-Antfly-Inference-Pool", "gpu")
	recorder := httptest.NewRecorder()
	p.handleModels(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), "florence") {
		t.Fatalf("catalog escaped selected pool: %s", recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "gemma4") {
		t.Fatalf("scoped model missing: %s", recorder.Body.String())
	}
}

func TestCatalogTaskScopesCoverEveryRoutableModelFamily(t *testing.T) {
	tests := []struct {
		task      string
		operation OperationType
		category  string
	}{
		{"read", "read", "readers"},
		{"generate", "generate.batch", "generators"},
		{"embed", "embed", "embedders"},
		{"rerank", "rerank", "rerankers"},
		{"chunk", "chunk", "chunkers"},
		{"extract", "extract", "extractors"},
		{"rewrite", "rewrite", "rewriters"},
		{"classify", "classify", "classifiers"},
		{"transcribe", "transcribe", "transcribers"},
	}
	for _, test := range tests {
		t.Run(test.task, func(t *testing.T) {
			scope, err := catalogTaskScopeFor(test.task)
			if err != nil {
				t.Fatal(err)
			}
			if scope.Operation != test.operation || scope.Category != test.category {
				t.Fatalf("scope = %#v, want operation=%q category=%q", scope, test.operation, test.category)
			}
		})
	}
}

func TestProxyRequestModelReadsAndCanonicalizesChunkConfig(t *testing.T) {
	for _, test := range []struct {
		body string
		want string
	}{
		{`{"input":"hello"}`, "fixed"},
		{`{"input":"hello","config":{"model":"fixed-bert-tokenizer"}}`, "fixed"},
		{`{"input":"hello","config":{"model":"owner/semantic-chunker"}}`, "owner/semantic-chunker"},
	} {
		got, err := proxyRequestModel([]byte(test.body), "chunk")
		if err != nil {
			t.Fatal(err)
		}
		if got != test.want {
			t.Fatalf("model = %q, want %q", got, test.want)
		}
	}
}

func TestChunkCatalogAliasesNormalizeAcrossMixedVersions(t *testing.T) {
	scope, err := catalogTaskScopeFor("chunk")
	if err != nil {
		t.Fatal(err)
	}
	legacy := map[string]json.RawMessage{
		"chunkers": json.RawMessage(`{"fixed_bert":{"inputs":["text"]}}`),
	}
	if !catalogContainsTaskModel(legacy, scope, "fixed") {
		t.Fatal("canonical fixed request did not match legacy fixed_bert catalog")
	}
	merged := make(map[string]map[string]json.RawMessage)
	mergeModelCatalog(merged, legacy)
	if _, ok := merged["chunkers"]["fixed"]; !ok {
		t.Fatalf("legacy chunk alias was not normalized: %#v", merged["chunkers"])
	}
	if _, ok := merged["chunkers"]["fixed_bert"]; ok {
		t.Fatalf("legacy alias leaked into merged catalog: %#v", merged["chunkers"])
	}
}

func TestScopedCatalogForwardsCallerAuthorization(t *testing.T) {
	const caller = "Bearer caller-token"
	p := NewProxy(Config{DefaultPool: "primary", RefreshInterval: time.Minute, Logger: zap.NewNop()})
	p.registry.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if got := req.Header.Get("Authorization"); got != caller {
			t.Fatalf("Authorization = %q, want %q", got, caller)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"classifiers":{"owner/classifier":{"inputs":["text"]}}}`)),
			Request:    req,
		}, nil
	})}
	p.RegisterEndpoint("http://classifier.internal", "primary", WorkloadTypeGeneral)
	p.registry.UpdateModelOperations("http://classifier.internal", map[string]map[OperationType]bool{
		"owner/classifier": {"classify": true},
	})
	request := httptest.NewRequest(http.MethodGet, "/ai/v1/models?model=owner%2Fclassifier&task=classify", nil)
	request.Header.Set("Authorization", caller)
	recorder := httptest.NewRecorder()
	p.handleModels(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestConservativeCapabilitiesDoNotOverpromiseAcrossUnknownLimits(t *testing.T) {
	left := map[string]any{
		"task": "read",
		"batch": map[string]any{
			"mode": "native", "preferred_items": float64(8), "max_items": float64(16),
			"max_encoded_bytes": nil, "max_decoded_pixels": float64(0),
			"max_media_parts_per_item": float64(1), "per_item_failures": false,
		},
	}
	right := map[string]any{
		"task": "read",
		"batch": map[string]any{
			"mode": "native", "preferred_items": float64(4), "max_items": float64(8),
			"max_encoded_bytes": float64(1024), "max_decoded_pixels": float64(2048),
			"max_media_parts_per_item": float64(1), "per_item_failures": false,
		},
	}
	merged, ok := conservativeInferenceCapabilities(left, right)
	if !ok {
		t.Fatal("capabilities did not merge")
	}
	batch := merged["batch"].(map[string]any)
	if batch["max_encoded_media_bytes"] != nil || batch["max_decoded_pixels"] != nil {
		t.Fatalf("unknown optional limits were over-promised: %#v", batch)
	}
}

func TestConservativeCapabilitiesV2PreservesDisabledLimit(t *testing.T) {
	left := map[string]any{
		"version": float64(2), "task": "generate",
		"batch": map[string]any{
			"mode": "none", "preferred_items": float64(1), "max_items": float64(1),
			"max_encoded_media_bytes": float64(0), "max_decoded_pixels": float64(0),
			"max_media_parts_per_item": float64(0), "per_item_failures": false,
		},
	}
	right := map[string]any{
		"version": float64(2), "task": "generate",
		"batch": map[string]any{
			"mode": "none", "preferred_items": float64(1), "max_items": float64(1),
			"max_encoded_media_bytes": float64(1024), "max_decoded_pixels": float64(2048),
			"max_media_parts_per_item": float64(0), "per_item_failures": false,
		},
	}
	merged, ok := conservativeInferenceCapabilities(left, right)
	if !ok {
		t.Fatal("capabilities did not merge")
	}
	batch := merged["batch"].(map[string]any)
	if batch["max_encoded_media_bytes"] != float64(0) || batch["max_decoded_pixels"] != float64(0) {
		t.Fatalf("disabled v2 limits were lost: %#v", batch)
	}
}

func TestConservativeCapabilitiesV3PreservesExactContract(t *testing.T) {
	base := func(modalities, mimes []any, borrowed bool) map[string]any {
		return map[string]any{
			"version": float64(3), "task": "extract",
			"input_modalities": modalities, "accepted_mime_types": mimes,
			"input_granularity": "page", "output": "extraction",
			"result_cardinality": "one_per_item", "prompt_policy": "structured_schema",
			"borrowed_attachments": borrowed,
			"batch": map[string]any{
				"mode": "serial_compatibility", "preferred_items": float64(8), "max_items": float64(32),
				"max_encoded_media_bytes": float64(4096), "max_decoded_pixels": float64(8192),
				"max_media_parts_per_item": float64(1), "per_item_failures": false,
			},
		}
	}
	left := base([]any{"image", "text"}, []any{"image/png", "image/jpeg", "text/plain"}, true)
	right := base([]any{"image"}, []any{"image/png"}, false)
	merged, ok := conservativeInferenceCapabilities(left, right)
	if !ok {
		t.Fatal("v3 capabilities did not merge")
	}
	if merged["version"] != float64(3) || !reflect.DeepEqual(merged["input_modalities"], []string{"image"}) ||
		!reflect.DeepEqual(merged["accepted_mime_types"], []string{"image/png"}) || merged["borrowed_attachments"] != false {
		t.Fatalf("exact v3 contract was not conservatively preserved: %#v", merged)
	}
}

func TestConservativeCapabilitiesV3RejectsMalformedExactFields(t *testing.T) {
	valid := func() map[string]any {
		return map[string]any{
			"version": float64(3), "task": "extract",
			"input_modalities": []any{"image"}, "accepted_mime_types": []any{"image/png"},
			"input_granularity": "page", "output": "extraction",
			"result_cardinality": "one_per_item", "prompt_policy": "structured_schema",
			"borrowed_attachments": false,
			"batch": map[string]any{
				"mode": "serial_compatibility", "preferred_items": float64(2), "max_items": float64(8),
				"max_encoded_media_bytes": float64(4096), "max_decoded_pixels": float64(8192),
				"max_media_parts_per_item": float64(1), "per_item_failures": false,
			},
		}
	}
	for name, mutate := range map[string]func(map[string]any){
		"unknown modality":    func(value map[string]any) { value["input_modalities"] = []any{"telepathy"} },
		"duplicate mime":      func(value map[string]any) { value["accepted_mime_types"] = []any{"image/png", "image/png"} },
		"non-string modality": func(value map[string]any) { value["input_modalities"] = []any{float64(1)} },
		"unknown scalar":      func(value map[string]any) { value["prompt_policy"] = "sometimes" },
		"wrong cardinality":   func(value map[string]any) { value["result_cardinality"] = "one_per_request" },
		"mime without modality": func(value map[string]any) {
			value["accepted_mime_types"] = []any{"image/png", "text/plain"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			left := valid()
			mutate(left)
			if _, ok := conservativeInferenceCapabilities(left, valid()); ok {
				t.Fatal("malformed v3 descriptor merged")
			}
			raw, err := json.Marshal(map[string]any{"inference_capabilities": left})
			if err != nil {
				t.Fatal(err)
			}
			if got := string(sanitizeModelDescriptor(raw)); got != "{}" {
				t.Fatalf("malformed singleton descriptor was not poisoned: %s", got)
			}
		})
	}
}

func TestConservativeCapabilitiesPreserveSingletonExecution(t *testing.T) {
	left := map[string]any{
		"task": "chunk",
		"batch": map[string]any{
			"mode": "none", "preferred_items": float64(1), "max_items": float64(1),
			"max_encoded_bytes": nil, "max_decoded_pixels": nil,
			"max_media_parts_per_item": float64(0), "per_item_failures": false,
		},
	}
	right := map[string]any{
		"task": "chunk",
		"batch": map[string]any{
			"mode": "serial_compatibility", "preferred_items": float64(8), "max_items": float64(8),
			"max_encoded_bytes": nil, "max_decoded_pixels": nil,
			"max_media_parts_per_item": float64(0), "per_item_failures": false,
		},
	}
	merged, ok := conservativeInferenceCapabilities(left, right)
	if !ok {
		t.Fatal("capabilities did not merge")
	}
	batch := merged["batch"].(map[string]any)
	if batch["mode"] != "none" || batch["max_items"] != float64(1) {
		t.Fatalf("singleton execution was upgraded: %#v", batch)
	}
}

func TestProxyModelCatalogFailsClosedWhenAnyCandidateFails(t *testing.T) {
	p := NewProxy(Config{DefaultPool: "primary", RefreshInterval: time.Minute, Logger: zap.NewNop()})
	p.registry.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host == "failed.internal" {
			return nil, errors.New("catalog unavailable")
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"generators":{"gemma4":{}}}`)),
			Request:    req,
		}, nil
	})}
	for _, address := range []string{"http://healthy.internal", "http://failed.internal"} {
		p.RegisterEndpoint(address, "primary", WorkloadTypeGeneral)
		p.registry.UpdateModels(address, []string{"gemma4"})
	}

	recorder := httptest.NewRecorder()
	p.handleModels(recorder, httptest.NewRequest(http.MethodGet, "/ai/v1/models", nil))
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", recorder.Code)
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
