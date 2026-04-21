package proxy

import (
	"bytes"
	"context"
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

func TestSelectDestinationHonorsLatencyCondition(t *testing.T) {
	t.Parallel()

	registry := NewModelRegistry(time.Minute)
	registry.RegisterEndpoint("http://slow.internal", "slow", WorkloadTypeGeneral)
	registry.RegisterEndpoint("http://fast.internal", "fast", WorkloadTypeGeneral)
	registry.UpdateModels("http://slow.internal", []string{"model-a"})
	registry.UpdateModels("http://fast.internal", []string{"model-a"})
	registry.RecordModelLatency("http://slow.internal", "model-a", 250*time.Millisecond)
	registry.RecordModelLatency("http://fast.internal", "model-a", 50*time.Millisecond)

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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}
