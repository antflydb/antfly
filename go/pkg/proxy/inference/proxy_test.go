package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
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

func advertiseModelOperation(registry *ModelRegistry, address string, operation OperationType, models ...string) {
	operations := make(map[string]map[OperationType]bool, len(models))
	for _, model := range models {
		operations[model] = map[OperationType]bool{operation: true}
	}
	registry.UpdateModelOperations(address, operations)
}

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

func TestStartBackgroundDiscoversRegisteredEndpointsWhenPeriodicRefreshDisabled(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{DefaultPool: "primary", Logger: zap.NewNop()})
	p.registry.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(strings.NewReader(
				`{"readers":{"owner/reader":{"inference_capabilities":{"version":4,"task":"read","input_modalities":["image"],"accepted_mime_types":["image/png"],"input_granularity":"page","batch":{"mode":"native","preferred_items":1,"max_items":1,"max_encoded_media_bytes":1024,"max_decoded_pixels":1024,"max_media_parts_per_item":1,"per_item_failures":true},"task_limits":{"max_text_bytes_per_item":null,"max_input_tokens_per_item":null,"max_output_tokens_per_item":null,"max_candidates_per_request":null,"max_schema_bytes":null},"output":"read_result","result_cardinality":"one_per_item","prompt_policy":"explicit","borrowed_attachments":false}}}}`,
			)),
			Request: req,
		}, nil
	})}
	p.RegisterEndpoint("http://reader.internal", "primary", WorkloadTypeGeneral)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.StartBackground(ctx)

	deadline := time.Now().Add(time.Second)
	for len(p.router.ResolveEndpointCandidates("owner/reader", "primary", nil, "read")) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("registration-driven discovery did not publish the reader")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRefreshCompletionCannotMutateReregisteredEndpoint(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	p := NewProxy(Config{Logger: zap.NewNop()})
	p.registry.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		close(started)
		<-release
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"readers":{"old-model":{}}}`)),
			Request:    req,
		}, nil
	})}
	const address = "http://reused.internal"
	p.registry.RegisterEndpoint(address, "old-pool", WorkloadTypeGeneral)
	done := make(chan error, 1)
	go func() { done <- p.registry.RefreshEndpoint(context.Background(), address) }()
	<-started
	p.registry.UnregisterEndpoint(address)
	p.registry.RegisterEndpoint(address, "new-pool", WorkloadTypeGeneral)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if endpoints := p.registry.GetEndpointsForModel("old-model"); len(endpoints) != 0 {
		t.Fatalf("stale refresh published into a new endpoint incarnation: %#v", endpoints)
	}
}

func TestRefreshCompletionCannotMutateChangedEndpointTopology(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	p := NewProxy(Config{Logger: zap.NewNop()})
	p.registry.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		close(started)
		<-release
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"readers":{"old-model":{}}}`)),
			Request:    req,
		}, nil
	})}
	const address = "http://stable.internal"
	p.registry.RegisterEndpointWithHealth(address, "", "old-pool", WorkloadTypeGeneral)
	done := make(chan error, 1)
	go func() { done <- p.registry.RefreshEndpoint(context.Background(), address) }()
	<-started
	// Topology is copy-on-write. The old catalog response must not publish into
	// the replacement endpoint identity.
	p.registry.RegisterEndpointWithHealth(address, "", "new-pool", WorkloadTypeReadHeavy)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if endpoints := p.registry.GetEndpointsForModel("old-model"); len(endpoints) != 0 {
		t.Fatalf("stale refresh published across a topology change: %#v", endpoints)
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
	p.RegisterEndpoint("http://inference.internal", "primary", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, "http://inference.internal", "embed", "bge-small-en-v1.5")
	p.Router().RouteManager().UpsertRoute(&Route{
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
	advertiseModelOperation(p.registry, "http://primary-a.internal", "embed", "bge-small-en-v1.5")
	advertiseModelOperation(p.registry, "http://primary-b.internal", "embed", "bge-small-en-v1.5")
	atomic.StoreInt32(&p.registry.endpoints["http://primary-b.internal"].runtime.connections, 1)
	p.Router().RouteManager().UpsertRoute(&Route{
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

func TestProxyRequestRetryPreservesCapabilityStaleResponse(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{DefaultPool: "primary", RefreshInterval: time.Minute, Logger: zap.NewNop()})
	for _, address := range []string{"http://primary-a.internal", "http://primary-b.internal"} {
		p.RegisterEndpoint(address, "primary", WorkloadTypeGeneral)
		advertiseModelOperation(p.registry, address, "embed", "model-a")
	}
	route := &Route{
		Name:               "default/retry-capability",
		Operations:         map[OperationType]bool{"embed": true},
		Destinations:       []Destination{{Pool: "primary", Weight: 100}},
		RetryAttempts:      2,
		RetryOnStatuses:    map[int]bool{http.StatusInternalServerError: true},
		RetryOnRequestErrs: true,
	}
	p.Router().RouteManager().UpsertRoute(route)
	revision := "revision-a"
	endpoints := p.router.ResolveEndpointCandidates("model-a", "primary", nil, "embed")
	token, err := p.issueCapabilityLease(
		"model-a",
		"embed",
		"embed",
		p.router.RouteManager().Generation(),
		revision,
		"",
		capabilityEndpointSet(p.registry, endpoints),
	)
	if err != nil {
		t.Fatal(err)
	}

	var attempts int32
	p.registry.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&attempts, 1)
		// Simulate a policy update after the first attempt has already been
		// admitted. The retry must tell the client to discard the old plan.
		p.Router().RouteManager().UpsertRoute(&Route{
			Name:            route.Name,
			Priority:        1,
			Operations:      route.Operations,
			Destinations:    route.Destinations,
			RetryAttempts:   route.RetryAttempts,
			RetryOnStatuses: route.RetryOnStatuses,
		})
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":"transient"}`)),
			Request:    req,
		}, nil
	})}

	request := httptest.NewRequest(http.MethodPost, "/ai/v1/embed", strings.NewReader(`{"model":"model-a"}`))
	request.Header.Set(capabilityTokenHeader, token)
	request.Header.Set(capabilityRevisionHeader, revision)
	recorder := httptest.NewRecorder()
	p.handleEmbed(recorder, request)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want capability-stale conflict: %s", recorder.Code, recorder.Body.String())
	}
	if recorder.Header().Get(capabilityStaleHeader) != "true" {
		t.Fatalf("headers = %v, want %s=true", recorder.Header(), capabilityStaleHeader)
	}
	if got := atomic.LoadInt32(&attempts); got != 1 {
		t.Fatalf("backend attempts = %d, want retry rejected before second forwarding", got)
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
	advertiseModelOperation(p.registry, "http://primary.internal", "embed", "bge-small-en-v1.5")
	p.Router().RouteManager().UpsertRoute(&Route{
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

	cb := p.registry.circuitBreaker("http://primary.internal")
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
	advertiseModelOperation(p.registry, "http://source.internal", "embed", "bge-small-en-v1.5")
	p.Router().RouteManager().UpsertRoute(&Route{
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
	if got := resolved.Endpoint.address; got != "http://source.internal" {
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
	advertiseModelOperation(p.registry, "http://default.internal", "embed", "bge-small-en-v1.5")
	advertiseModelOperation(p.registry, "http://source.internal", "embed", "bge-small-en-v1.5")

	p.Router().RouteManager().UpsertRoute(&Route{
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
	if got := resolved.Endpoint.address; got != "http://source.internal" {
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
	advertiseModelOperation(p.registry, "http://default.internal", "embed", "bge-small-en-v1.5")
	p.Router().RouteManager().UpsertRoute(&Route{
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
	p.Router().RouteManager().UpsertRoute(&Route{
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
		advertiseModelOperation(p.registry, "http://queued.internal", "embed", "bge-small-en-v1.5")
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
		lease, err := router.AcquireEndpoint(context.Background(), "model-a", "burst", WorkloadTypeBurst, nil)
		if err != nil {
			t.Fatalf("AcquireEndpoint returned error: %v", err)
		}
		endpoint := lease.Endpoint()
		lease.Release()
		switch endpoint.address {
		case "http://burst-a.internal":
			seenA = true
		case "http://burst-b.internal":
			seenB = true
		default:
			t.Fatalf("unexpected endpoint %q", endpoint.address)
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

	lease, err := p.router.AcquireEndpoint(context.Background(), "model-a", "bridge", WorkloadTypeGeneral, nil)
	if err != nil {
		t.Fatalf("AcquireEndpoint returned error: %v", err)
	}
	defer lease.Release()
	endpoint := lease.Endpoint()
	if endpoint.address != server.URL {
		t.Fatalf("expected endpoint %q, got %q", server.URL, endpoint.address)
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
	advertiseModelOperation(p.registry, "http://inference.internal", "embed", "model-a")

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
	advertiseModelOperation(p.registry, "http://burst-a.internal", "embed", "model-a")
	advertiseModelOperation(p.registry, "http://burst-b.internal", "embed", "model-a")

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
			previewEndpoint = resolved.Endpoint.address
			continue
		}
		if resolved.Endpoint.address != previewEndpoint {
			t.Fatalf("expected burst preview to remain stable at %q, got %q", previewEndpoint, resolved.Endpoint.address)
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
	if nextLease.Resolution.Endpoint.address != previewEndpoint {
		t.Fatalf("expected ResolveRequest previews not to advance burst selector, got %q want %q", nextLease.Resolution.Endpoint.address, previewEndpoint)
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
	p.Router().RouteManager().UpsertRoute(&Route{
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

	openCB := registry.circuitBreaker("http://open.internal")
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

func TestAcquireEndpointClaimsRecoveredCircuitOnce(t *testing.T) {
	t.Parallel()

	registry := NewModelRegistry(time.Minute)
	registry.RegisterEndpoint("http://recovering.internal", "primary", WorkloadTypeGeneral)
	registry.UpdateModels("http://recovering.internal", []string{"model-a"})

	cb := registry.circuitBreaker("http://recovering.internal")
	cb.threshold = 1
	cb.timeout = 10 * time.Millisecond
	cb.RecordFailure()
	time.Sleep(20 * time.Millisecond)

	router := NewRouter(registry)

	lease, err := router.AcquireEndpoint(context.Background(), "model-a", "primary", WorkloadTypeGeneral, nil)
	if err != nil {
		t.Fatalf("AcquireEndpoint returned error: %v", err)
	}
	defer lease.Release()
	endpoint := lease.Endpoint()
	if endpoint.address != "http://recovering.internal" {
		t.Fatalf("expected recovering endpoint, got %q", endpoint.address)
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
	advertiseModelOperation(p.registry, "http://recovering.internal", "embed", "model-a")

	cb := p.registry.circuitBreaker("http://recovering.internal")
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
	if resolved.Endpoint.address != "http://recovering.internal" {
		t.Fatalf("expected resolving endpoint, got %q", resolved.Endpoint.address)
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
	advertiseModelOperation(p.registry, "http://primary.internal", "embed", "model-a")

	route := &Route{
		Name:         "default/rate-limit",
		Operations:   map[OperationType]bool{OperationType("embed"): true},
		Destinations: []Destination{{Pool: "primary", Weight: 100}},
		RateLimiter:  NewRateLimiter(1, 1, false),
	}
	p.Router().RouteManager().UpsertRoute(route)

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
	advertiseModelOperation(p.registry, "http://primary.internal", "embed", "model-a", "model-b")

	route := &Route{
		Name:         "default/per-model-rate-limit",
		Operations:   map[OperationType]bool{OperationType("embed"): true},
		Destinations: []Destination{{Pool: "primary", Weight: 100}},
		RateLimiter:  NewRateLimiter(1, 1, true),
	}
	p.Router().RouteManager().UpsertRoute(route)

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
	advertiseModelOperation(p.registry, "http://primary-a.internal", "embed", "model-a")
	advertiseModelOperation(p.registry, "http://primary-b.internal", "embed", "model-a")

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

	if firstLease.Resolution.Endpoint.address == secondLease.Resolution.Endpoint.address {
		t.Fatalf("expected in-flight load to shift least-loaded selection, both leases resolved %q", firstLease.Resolution.Endpoint.address)
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
	advertiseModelOperation(p.registry, "http://primary-a.internal", "embed", "model-a")
	advertiseModelOperation(p.registry, "http://primary-b.internal", "embed", "model-a")

	route := &Route{
		Name:          "default/retry-admission",
		Operations:    map[OperationType]bool{OperationType("embed"): true},
		Destinations:  []Destination{{Pool: "primary", Weight: 100}},
		RateLimiter:   NewRateLimiter(1, 1, false),
		RetryAttempts: 2,
	}
	p.Router().RouteManager().UpsertRoute(route)

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
	advertiseModelOperation(p.registry, "http://primary-a.internal", "embed", "model-a")
	advertiseModelOperation(p.registry, "http://primary-b.internal", "embed", "model-a")
	p.Router().RouteManager().UpsertRoute(&Route{
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
	advertiseModelOperation(p.registry, "http://primary-a.internal", "embed", "model-a")
	advertiseModelOperation(p.registry, "http://primary-b.internal", "embed", "model-a")
	atomic.StoreInt32(&p.registry.endpoints["http://primary-b.internal"].runtime.connections, 1)
	p.Router().RouteManager().UpsertRoute(&Route{
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
	firstEndpoint := firstLease.Resolution.Endpoint.address

	firstLease.RecordFailure()

	retryLease, err := firstLease.NextAttempt(context.Background())
	if err != nil {
		t.Fatalf("NextAttempt: %v", err)
	}
	if retryLease.Resolution.Endpoint.address == firstEndpoint {
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
	advertiseModelOperation(p.registry, "http://primary.internal", "embed", "model-a")

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
			connections = atomic.LoadInt32(&endpoint.runtime.connections)
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
	if got := atomic.LoadInt32(&endpoint.runtime.connections); got != 1 {
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

	if got := atomic.LoadInt32(&endpoint.runtime.connections); got != 0 {
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

	cb := p.registry.circuitBreaker("http://primary.internal")
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
		"readers": {"shared": {}, "reader-only": {}},
		"rerankers": {"multimodal": {}}
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
	if !operations["multimodal"]["rerank"] || !operations["multimodal"]["rerank_multimodal"] {
		t.Fatalf("reranker aliases = %#v, want text and multimodal operations", operations["multimodal"])
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
	if readResolution.Endpoint.address != "http://reader.internal" {
		t.Fatalf("read routed to %s", readResolution.Endpoint.address)
	}
	generateResolution, err := p.ResolveRequest(context.Background(), ResolveRequest{Operation: "generate", Model: "shared"})
	if err != nil {
		t.Fatal(err)
	}
	if generateResolution.Endpoint.address != "http://generator.internal" {
		t.Fatalf("generation routed to %s", generateResolution.Endpoint.address)
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

func TestResolveFailsClosedBeforeCatalogDiscovery(t *testing.T) {
	p := NewProxy(Config{DefaultPool: "primary", RefreshInterval: time.Minute, Logger: zap.NewNop()})
	p.RegisterEndpoint("http://bootstrap.internal", "primary", WorkloadTypeGeneral)
	p.registry.UpdateModels("http://bootstrap.internal", []string{"shared"})

	if _, err := p.ResolveRequest(context.Background(), ResolveRequest{Operation: "read", Model: "shared"}); err == nil {
		t.Fatal("operation-scoped request must wait for endpoint capability discovery")
	}
}

func TestResolveDoesNotRebindCachedCapabilityToUndiscoveredReplacement(t *testing.T) {
	p := NewProxy(Config{DefaultPool: "primary", RefreshInterval: time.Minute, Logger: zap.NewNop()})
	p.RegisterEndpoint("http://reader-a.internal", "primary", WorkloadTypeGeneral)
	p.registry.UpdateModelOperations("http://reader-a.internal", map[string]map[OperationType]bool{
		"shared": {"read": true},
	})
	if _, err := p.ResolveRequest(context.Background(), ResolveRequest{Operation: "read", Model: "shared"}); err != nil {
		t.Fatal(err)
	}

	p.UnregisterEndpoint("http://reader-a.internal")
	p.RegisterEndpoint("http://replacement.internal", "primary", WorkloadTypeGeneral)
	p.registry.UpdateModels("http://replacement.internal", []string{"shared"})
	if _, err := p.ResolveRequest(context.Background(), ResolveRequest{Operation: "read", Model: "shared"}); err == nil {
		t.Fatal("cached capability was rebound to an undiscovered replacement")
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
	p.registry.UpdateModelOperations("http://inference.internal", map[string]map[OperationType]bool{
		"gemma4": {"read": true, "generate.batch": true},
	})

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

func TestCatalogTaskScopeValidatesConcreteOperationFamily(t *testing.T) {
	scope, err := catalogTaskScopeForOperation("generate", "generate.batch")
	if err != nil {
		t.Fatal(err)
	}
	if scope.Operation != "generate.batch" || scope.Category != "generators" {
		t.Fatalf("scope = %#v, want concrete generation batch operation", scope)
	}
	if _, err := catalogTaskScopeForOperation("generate", "embed"); err == nil {
		t.Fatal("cross-family operation was accepted")
	}
	if _, err := catalogTaskScopeForOperation("", "read"); err == nil {
		t.Fatal("operation without task was accepted")
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
	if recorder.Header().Get(capabilityTokenHeader) == "" {
		t.Fatal("scoped catalog did not issue a capability lease")
	}
	if recorder.Header().Get(capabilityRevisionHeader) == "" {
		t.Fatal("scoped catalog did not publish its descriptor revision")
	}
}

func TestScopedCatalogUsesRouteCapabilityCohort(t *testing.T) {
	t.Parallel()
	p := NewProxy(Config{DefaultPool: "cpu", Logger: zap.NewNop()})
	p.registry.RegisterEndpoint("http://cpu.internal", "cpu", WorkloadTypeGeneral)
	p.registry.RegisterEndpoint("http://gpu.internal", "gpu", WorkloadTypeGeneral)
	p.Router().RouteManager().UpsertRoute(&Route{
		Name:          "default/gemma-reader",
		Operations:    map[OperationType]bool{"read": true},
		ModelPatterns: []*RegexPattern{MustRegexPattern(`^owner/reader$`)},
		Destinations:  []Destination{{Pool: "gpu", Weight: 1}},
	})
	request := httptest.NewRequest(http.MethodGet, "/ai/v1/models?model=owner%2Freader&task=read", nil)
	routing, err := p.routingContextForRequest(request, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	endpoints := p.catalogEndpointsForRequest(routing, "owner/reader", "read")
	if len(endpoints) != 1 || endpoints[0].address != "http://gpu.internal" {
		t.Fatalf("route capability cohort = %#v, want gpu endpoint only", endpoints)
	}

	request = httptest.NewRequest(http.MethodGet, "/ai/v1/models?model=owner%2Fother&task=read", nil)
	routing, err = p.routingContextForRequest(request, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	endpoints = p.catalogEndpointsForRequest(routing, "owner/other", "read")
	if len(endpoints) != 1 || endpoints[0].address != "http://cpu.internal" {
		t.Fatalf("default capability cohort = %#v, want cpu endpoint only", endpoints)
	}
}

func TestScopedCatalogIncludesUnknownConditionalRouteContext(t *testing.T) {
	t.Parallel()
	p := NewProxy(Config{DefaultPool: "cpu", Logger: zap.NewNop()})
	p.registry.RegisterEndpoint("http://cpu.internal", "cpu", WorkloadTypeGeneral)
	p.registry.RegisterEndpoint("http://tenant-gpu.internal", "tenant-gpu", WorkloadTypeGeneral)
	p.Router().RouteManager().UpsertRoute(&Route{
		Name:                "tenant-reader",
		Priority:            100,
		Operations:          map[OperationType]bool{"read": true},
		ModelPatterns:       []*RegexPattern{MustRegexPattern(`^owner/reader$`)},
		SourceOrganizations: map[string]bool{"tenant-a": true},
		Destinations:        []Destination{{Pool: "tenant-gpu", Weight: 1}},
	})
	p.Router().RouteManager().UpsertRoute(&Route{
		Name:          "general-reader",
		Operations:    map[OperationType]bool{"read": true},
		ModelPatterns: []*RegexPattern{MustRegexPattern(`^owner/reader$`)},
		Destinations:  []Destination{{Pool: "cpu", Weight: 1}},
	})

	request := httptest.NewRequest(http.MethodGet, "/ai/v1/models?model=owner%2Freader&task=read", nil)
	routing, err := p.routingContextForRequest(request, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	endpoints := p.catalogEndpointsForRequest(routing, "owner/reader", "read")
	addresses := make(map[string]bool, len(endpoints))
	for _, endpoint := range endpoints {
		addresses[endpoint.address] = true
	}
	if !addresses["http://tenant-gpu.internal"] || !addresses["http://cpu.internal"] || len(addresses) != 2 {
		t.Fatalf("unknown-context capability cohort = %#v, want tenant and general endpoints", addresses)
	}

	request.Header.Set("X-Antfly-Source-Organization", "tenant-a")
	routing, err = p.routingContextForRequest(request, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	endpoints = p.catalogEndpointsForRequest(routing, "owner/reader", "read")
	addresses = make(map[string]bool, len(endpoints))
	for _, endpoint := range endpoints {
		addresses[endpoint.address] = true
	}
	if !addresses["http://tenant-gpu.internal"] || !addresses["http://cpu.internal"] || len(addresses) != 2 {
		t.Fatalf("unverified source header narrowed capability cohort = %#v", addresses)
	}

	p.verifiedSource = func(r *http.Request) (VerifiedSource, error) {
		return VerifiedSource{OrganizationID: r.Header.Get("X-Antfly-Source-Organization")}, nil
	}
	routing, err = p.routingContextForRequest(request, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	endpoints = p.catalogEndpointsForRequest(routing, "owner/reader", "read")
	if len(endpoints) != 1 || endpoints[0].address != "http://tenant-gpu.internal" {
		t.Fatalf("verified exact-context capability cohort = %#v, want tenant endpoint only", endpoints)
	}
	advertiseModelOperation(p.registry, "http://cpu.internal", "read", "owner/reader")
	advertiseModelOperation(p.registry, "http://tenant-gpu.internal", "read", "owner/reader")
	resolution, err := p.ResolveRequest(context.Background(), ResolveRequest{
		Operation: "read",
		Model:     "owner/reader",
		Headers:   routing.Headers,
		Source:    routing.Source,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolution.Pool != "tenant-gpu" || resolution.Endpoint.address != "http://tenant-gpu.internal" {
		t.Fatalf("verified execution resolution = %#v, want tenant endpoint", resolution)
	}
}

func TestRoutesPrecedeExplicitPoolForDiscoveryAndExecution(t *testing.T) {
	t.Parallel()
	p := NewProxy(Config{DefaultPool: "cpu", Logger: zap.NewNop()})
	p.registry.RegisterEndpoint("http://cpu.internal", "cpu", WorkloadTypeGeneral)
	p.registry.RegisterEndpoint("http://gpu.internal", "gpu", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, "http://cpu.internal", "read", "owner/reader")
	advertiseModelOperation(p.registry, "http://gpu.internal", "read", "owner/reader")
	p.Router().RouteManager().UpsertRoute(&Route{
		Name:          "force-gpu",
		Operations:    map[OperationType]bool{"read": true},
		ModelPatterns: []*RegexPattern{MustRegexPattern(`^owner/reader$`)},
		Destinations:  []Destination{{Pool: "gpu", Weight: 1}},
	})

	routing := RoutingContext{
		Headers:      map[string]string{"x-antfly-inference-pool": "cpu"},
		ExplicitPool: "cpu",
	}
	endpoints := p.catalogEndpointsForRequest(routing, "owner/reader", "read")
	if len(endpoints) != 1 || endpoints[0].address != "http://gpu.internal" {
		t.Fatalf("routed capability cohort = %#v, want gpu endpoint only", endpoints)
	}
	token, err := issueReaderCapabilityLease(
		p,
		"owner/reader",
		"revision-a",
		"",
		capabilityEndpointSet(p.registry, endpoints),
	)
	if err != nil {
		t.Fatal(err)
	}
	routing.Headers[capabilityTokenHeader] = token
	routing.Headers[capabilityRevisionHeader] = "revision-a"

	resolution, err := p.ResolveRequest(context.Background(), ResolveRequest{
		Operation: "read",
		Model:     "owner/reader",
		Headers:   routing.Headers,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolution.Pool != "gpu" || resolution.Endpoint.address != "http://gpu.internal" || resolution.Route == nil {
		t.Fatalf("routed resolution = %#v, want route-selected gpu resolution", resolution)
	}
}

func TestTerminalRejectRouteDoesNotExposeDefaultPool(t *testing.T) {
	t.Parallel()
	p := NewProxy(Config{DefaultPool: "cpu", Logger: zap.NewNop()})
	p.registry.RegisterEndpoint("http://cpu.internal", "cpu", WorkloadTypeGeneral)
	p.Router().RouteManager().UpsertRoute(&Route{
		Name:          "reject-reader",
		Operations:    map[OperationType]bool{"read": true},
		ModelPatterns: []*RegexPattern{MustRegexPattern(`^owner/reader$`)},
		Fallback:      &Fallback{Action: "reject", StatusCode: http.StatusForbidden},
	})
	routing := RoutingContext{}
	cohort := p.Router().RouteManager().PotentialCohortFor(routing.routeRequest("read", "owner/reader"))
	if !cohort.Matched || !cohort.Terminal || len(cohort.Pools) != 0 {
		t.Fatalf("reject cohort = %#v, want matched terminal cohort without pools", cohort)
	}
	if endpoints := p.catalogEndpointsForRequest(routing, "owner/reader", "read"); len(endpoints) != 0 {
		t.Fatalf("reject catalog = %#v, want no default-pool endpoints", endpoints)
	}
}

func TestCatalogCohortIsBoundToConcreteOperation(t *testing.T) {
	t.Parallel()
	p := NewProxy(Config{DefaultPool: "cpu", Logger: zap.NewNop()})
	p.registry.RegisterEndpoint("http://cpu.internal", "cpu", WorkloadTypeGeneral)
	p.registry.RegisterEndpoint("http://gpu.internal", "gpu", WorkloadTypeGeneral)
	p.Router().RouteManager().UpsertRoute(&Route{
		Name:          "batch-generator",
		Operations:    map[OperationType]bool{"generate.batch": true},
		ModelPatterns: []*RegexPattern{MustRegexPattern(`^owner/generator$`)},
		Destinations:  []Destination{{Pool: "gpu", Weight: 1}},
	})

	routing := RoutingContext{}
	batch := p.catalogEndpointsForRequest(routing, "owner/generator", "generate.batch")
	if len(batch) != 1 || batch[0].address != "http://gpu.internal" {
		t.Fatalf("batch cohort = %#v, want gpu only", batch)
	}
	single := p.catalogEndpointsForRequest(routing, "owner/generator", "generate")
	if len(single) != 1 || single[0].address != "http://cpu.internal" {
		t.Fatalf("single cohort = %#v, want default cpu only", single)
	}
}

func TestTerminalAliasRejectDoesNotFallThroughViaSemanticTask(t *testing.T) {
	t.Parallel()
	p := NewProxy(Config{DefaultPool: "cpu", Logger: zap.NewNop()})
	p.registry.RegisterEndpoint("http://cpu.internal", "cpu", WorkloadTypeGeneral)
	p.Router().RouteManager().UpsertRoute(&Route{
		Name:          "reject-batch-generator",
		Operations:    map[OperationType]bool{"generate.batch": true},
		ModelPatterns: []*RegexPattern{MustRegexPattern(`^owner/generator$`)},
		Fallback:      &Fallback{Action: "reject", StatusCode: http.StatusForbidden},
	})
	if endpoints := p.catalogEndpointsForRequest(RoutingContext{}, "owner/generator", "generate.batch"); len(endpoints) != 0 {
		t.Fatalf("batch reject catalog = %#v, want no alias/default endpoints", endpoints)
	}
}

func TestCapabilityLeaseRejectsRoutePolicyGenerationChange(t *testing.T) {
	t.Parallel()
	p := NewProxy(Config{DefaultPool: "cpu", Logger: zap.NewNop()})
	p.registry.RegisterEndpoint("http://gpu-a.internal", "gpu-a", WorkloadTypeGeneral)
	p.registry.RegisterEndpoint("http://gpu-b.internal", "gpu-b", WorkloadTypeGeneral)
	for _, address := range []string{"http://gpu-a.internal", "http://gpu-b.internal"} {
		advertiseModelOperation(p.registry, address, "generate", "owner/generator")
	}
	p.Router().RouteManager().UpsertRoute(&Route{
		Name:          "generator",
		Operations:    map[OperationType]bool{"generate": true},
		ModelPatterns: []*RegexPattern{MustRegexPattern(`^owner/generator$`)},
		Destinations:  []Destination{{Pool: "gpu-a", Weight: 1}},
	})
	cohort := p.catalogEndpointCohortForRequest(RoutingContext{}, "owner/generator", "generate")
	token, err := p.issueCapabilityLease(
		"owner/generator",
		"generate",
		"generate",
		cohort.routeGeneration,
		"revision-a",
		"",
		capabilityEndpointSet(p.registry, cohort.endpoints),
	)
	if err != nil {
		t.Fatal(err)
	}

	p.Router().RouteManager().UpsertRoute(&Route{
		Name:          "generator",
		Operations:    map[OperationType]bool{"generate": true},
		ModelPatterns: []*RegexPattern{MustRegexPattern(`^owner/generator$`)},
		Destinations:  []Destination{{Pool: "gpu-b", Weight: 1}},
	})
	request := httptest.NewRequest(http.MethodPost, "/ai/v1/generate", strings.NewReader(`{"model":"owner/generator"}`))
	request.Header.Set(capabilityTokenHeader, token)
	request.Header.Set(capabilityRevisionHeader, "revision-a")
	recorder := httptest.NewRecorder()
	p.handleGenerate(recorder, request)
	if recorder.Code != http.StatusConflict || recorder.Header().Get(capabilityStaleHeader) != "true" {
		t.Fatalf("route-change response = %d headers=%v body=%q, want capability-stale conflict", recorder.Code, recorder.Header(), recorder.Body.String())
	}
}

func TestMultimodalRerankHandlerPreservesConcreteOperation(t *testing.T) {
	t.Parallel()
	p := NewProxy(Config{DefaultPool: "cpu", Logger: zap.NewNop()})
	p.registry.RegisterEndpoint("http://cpu.internal", "cpu", WorkloadTypeGeneral)
	p.registry.RegisterEndpoint("http://multimodal.internal", "gpu", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, "http://cpu.internal", "rerank", "owner/reranker")
	advertiseModelOperation(p.registry, "http://multimodal.internal", "rerank_multimodal", "owner/reranker")
	p.Router().RouteManager().UpsertRoute(&Route{
		Name:          "multimodal-rerank",
		Operations:    map[OperationType]bool{"rerank_multimodal": true},
		ModelPatterns: []*RegexPattern{MustRegexPattern(`^owner/reranker$`)},
		Destinations:  []Destination{{Pool: "gpu", Weight: 1}},
	})
	var forwardedHost string
	p.registry.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		forwardedHost = req.URL.Host
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"results":[]}`)),
			Request:    req,
		}, nil
	})}

	request := httptest.NewRequest(http.MethodPost, "/ai/v1/rerank_multimodal", strings.NewReader(`{"model":"owner/reranker","query":{"text":"q"},"documents":[]}`))
	recorder := httptest.NewRecorder()
	p.handleRerankMultimodal(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
	}
	if forwardedHost != "multimodal.internal" {
		t.Fatalf("forwarded host = %q, want multimodal.internal", forwardedHost)
	}
}

func TestCapabilityLeaseConstrainsRoutingAfterEndpointAddition(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{DefaultPool: "primary", Logger: zap.NewNop()})
	p.registry.RegisterEndpoint("http://reader-a.internal", "primary", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, "http://reader-a.internal", "read", "owner/reader")
	endpoints := p.router.ResolveEndpointCandidates("owner/reader", "primary", nil, "read")
	discovered := capabilityEndpointSet(p.registry, endpoints)
	token, err := issueReaderCapabilityLease(p, "owner/reader", "revision-a", "", discovered)
	if err != nil {
		t.Fatal(err)
	}
	// Issuance owns a complete immutable snapshot, including the operation set;
	// later mutation of discovery scratch state must not change admitted work.
	delete(discovered["http://reader-a.internal"].operations, "read")
	if err := p.validateCapabilityLease(token, "revision-a", "owner/reader", "read", ""); err != nil {
		t.Fatalf("fresh capability lease failed: %v", err)
	}

	p.registry.RegisterEndpoint("http://reader-b.internal", "primary", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, "http://reader-b.internal", "read", "owner/reader")
	allowed, err := p.capabilityLeaseEndpoints(token, "revision-a", "owner/reader", "read", sha256.Sum256(nil))
	if err != nil {
		t.Fatalf("endpoint addition invalidated a safely constrainable lease: %v", err)
	}
	candidates := p.router.resolveEndpointCandidatesWithin("owner/reader", "primary", nil, allowed, "read")
	if len(candidates) != 1 || candidates[0].address != "http://reader-a.internal" {
		t.Fatalf("lease-constrained candidates = %#v, want only reader-a", candidates)
	}
}

func TestCapabilityLeaseFiltersWeightedRouteDestinationsBeforeSelection(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{DefaultPool: "primary", Logger: zap.NewNop()})
	for _, endpoint := range []struct{ address, pool string }{
		{address: "http://catalog-failed.internal", pool: "unleased"},
		{address: "http://catalog-ready.internal", pool: "leased"},
	} {
		p.registry.RegisterEndpoint(endpoint.address, endpoint.pool, WorkloadTypeGeneral)
		advertiseModelOperation(p.registry, endpoint.address, "generate", "gemma4")
	}
	p.Router().RouteManager().UpsertRoute(&Route{
		Name:          "default/generator",
		Operations:    map[OperationType]bool{"generate": true},
		ModelPatterns: []*RegexPattern{MustRegexPattern(`^gemma4$`)},
		Destinations: []Destination{
			{Pool: "unleased", Weight: 100},
			{Pool: "leased", Weight: 1},
		},
	})
	covered := p.router.ResolveEndpointCandidates("gemma4", "leased", nil, "generate")
	token, err := p.issueCapabilityLease("gemma4", "generate", "generate", p.router.RouteManager().Generation(), "revision-a", "", capabilityEndpointSet(p.registry, covered))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := p.AcquireRequestResolution(context.Background(), ResolveRequest{
		Operation: "generate",
		Model:     "gemma4",
		Headers: map[string]string{
			capabilityTokenHeader:    token,
			capabilityRevisionHeader: "revision-a",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if lease.Resolution.Pool != "leased" || lease.Resolution.Endpoint.address != "http://catalog-ready.internal" {
		t.Fatalf("resolution = pool %q endpoint %q, want leased catalog responder", lease.Resolution.Pool, lease.Resolution.Endpoint.address)
	}
}

func TestCapabilityLeaseRejectsReregisteredEndpointAtSameAddress(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{DefaultPool: "primary", Logger: zap.NewNop()})
	const address = "http://reader.internal"
	p.registry.RegisterEndpoint(address, "primary", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, address, "read", "owner/reader")
	endpoints := p.router.ResolveEndpointCandidates("owner/reader", "primary", nil, "read")
	token, err := issueReaderCapabilityLease(p, "owner/reader", "revision-a", "", capabilityEndpointSet(p.registry, endpoints))
	if err != nil {
		t.Fatal(err)
	}

	p.registry.UnregisterEndpoint(address)
	p.registry.RegisterEndpoint(address, "primary", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, address, "read", "owner/reader")
	err = p.validateCapabilityLease(token, "revision-a", "owner/reader", "read", "")
	var resolutionErr *ResolutionError
	if !errors.As(err, &resolutionErr) || resolutionErr.StatusCode != http.StatusConflict {
		t.Fatalf("re-registered endpoint error = %#v, want capability-stale conflict", err)
	}
	p.capabilityLeaseMu.Lock()
	_, retained := p.capabilityLeases[token]
	p.capabilityLeaseMu.Unlock()
	if retained {
		t.Fatal("endpoint-incarnation-stale lease was retained after validation")
	}
}

func TestCapabilityLeaseIssuanceReclaimsReplacedEndpointIncarnations(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{DefaultPool: "primary", Logger: zap.NewNop()})
	const address = "http://reader.internal"
	p.registry.RegisterEndpoint(address, "primary", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, address, "read", "owner/reader")
	firstEndpoints := p.router.ResolveEndpointCandidates("owner/reader", "primary", nil, "read")
	oldToken, err := issueReaderCapabilityLease(p, "owner/reader", "revision-a", "", capabilityEndpointSet(p.registry, firstEndpoints))
	if err != nil {
		t.Fatal(err)
	}

	p.registry.UnregisterEndpoint(address)
	p.registry.RegisterEndpoint(address, "primary", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, address, "read", "owner/reader")
	secondEndpoints := p.router.ResolveEndpointCandidates("owner/reader", "primary", nil, "read")
	newToken, err := issueReaderCapabilityLease(p, "owner/reader", "revision-a", "", capabilityEndpointSet(p.registry, secondEndpoints))
	if err != nil {
		t.Fatal(err)
	}
	if newToken == oldToken {
		t.Fatal("replacement endpoint incarnation reused the old lease token")
	}
	p.capabilityLeaseMu.Lock()
	_, oldRetained := p.capabilityLeases[oldToken]
	leaseCount := len(p.capabilityLeases)
	p.capabilityLeaseMu.Unlock()
	if oldRetained || leaseCount != 1 {
		t.Fatalf("stale incarnation reclamation retained_old=%v lease_count=%d, want false/1", oldRetained, leaseCount)
	}
}

func TestCapabilityLeaseRejectsEndpointTopologyChange(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{DefaultPool: "primary", Logger: zap.NewNop()})
	const address = "http://reader.internal"
	p.registry.RegisterEndpointWithHealth(address, address+"/ready", "primary", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, address, "read", "owner/reader")
	endpoints := p.router.ResolveEndpointCandidates("owner/reader", "primary", nil, "read")
	token, err := issueReaderCapabilityLease(p, "owner/reader", "revision-a", "", capabilityEndpointSet(p.registry, endpoints))
	if err != nil {
		t.Fatal(err)
	}

	// The address remains stable, but routing semantics and endpoint identity
	// changed. The explicit incarnation must fence the old capability plan.
	p.registry.RegisterEndpointWithHealth(address, address+"/ready", "secondary", WorkloadTypeReadHeavy)
	err = p.validateCapabilityLease(token, "revision-a", "owner/reader", "read", "")
	var resolutionErr *ResolutionError
	if !errors.As(err, &resolutionErr) || resolutionErr.StatusCode != http.StatusConflict {
		t.Fatalf("topology-change error = %#v, want capability-stale conflict", err)
	}
	p.capabilityLeaseMu.Lock()
	_, retained := p.capabilityLeases[token]
	p.capabilityLeaseMu.Unlock()
	if retained {
		t.Fatal("topology-stale lease was retained after validation")
	}
}

func TestEndpointReservationRejectsChangedIncarnation(t *testing.T) {
	t.Parallel()
	registry := NewModelRegistry(time.Minute)
	const address = "http://reader.internal"
	registry.RegisterEndpointWithHealth(address, address+"/ready", "primary", WorkloadTypeGeneral)
	registry.mu.RLock()
	endpoint := registry.endpoints[address]
	old := endpointRef{
		endpoint:    endpoint,
		incarnation: endpoint.incarnation,
		breaker:     registry.circuitBreakers[address],
	}
	registry.mu.RUnlock()

	registry.RegisterEndpointWithHealth(address, address+"/ready-v2", "primary", WorkloadTypeGeneral)
	if registry.tryAcquireEndpoint(old) {
		t.Fatal("reservation crossed an endpoint incarnation change")
	}
	current := registry.GetEndpointsForPool("primary")
	if len(current) != 1 || current[0] == endpoint {
		t.Fatalf("same-pool topology replacement was not reindexed: %#v", current)
	}
}

func TestResolutionCompletionUsesReservedCircuitBreaker(t *testing.T) {
	t.Parallel()
	p := NewProxy(Config{DefaultPool: "primary", Logger: zap.NewNop()})
	const address = "http://reader.internal"
	p.registry.RegisterEndpoint(address, "primary", WorkloadTypeGeneral)
	p.registry.mu.RLock()
	endpoint := p.registry.endpoints[address]
	reserved := endpointRef{
		endpoint:    endpoint,
		incarnation: endpoint.incarnation,
		breaker:     p.registry.circuitBreakers[address],
	}
	p.registry.mu.RUnlock()

	p.registry.UnregisterEndpoint(address)
	p.registry.RegisterEndpoint(address, "primary", WorkloadTypeGeneral)
	replacement := p.registry.circuitBreaker(address)
	lease := &ResolutionLease{
		Resolution:  &Resolution{Endpoint: endpoint},
		proxy:       p,
		reservation: endpointReservation{ref: reserved},
	}
	lease.RecordFailure()
	if got := atomic.LoadInt32(&reserved.breaker.failures); got != 1 {
		t.Fatalf("reserved breaker failures = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&replacement.failures); got != 0 {
		t.Fatalf("replacement breaker failures = %d, want 0", got)
	}
}

func TestEndpointSnapshotsAreDetachedAndStable(t *testing.T) {
	t.Parallel()
	registry := NewModelRegistry(time.Minute)
	registry.RegisterEndpoint("http://b.internal", "primary", WorkloadTypeGeneral)
	registry.RegisterEndpoint("http://a.internal", "primary", WorkloadTypeReadHeavy)
	registry.UpdateModelOperations("http://a.internal", map[string]map[OperationType]bool{
		"z-model": {"generate": true},
		"a-model": {"read": true},
	})
	snapshots := registry.EndpointSnapshots()
	if len(snapshots) != 2 || snapshots[0].Address != "http://a.internal" ||
		len(snapshots[0].Models) != 2 || snapshots[0].Models[0].Name != "a-model" ||
		len(snapshots[0].Models[0].Operations) != 1 || snapshots[0].Models[0].Operations[0] != "read" {
		t.Fatalf("unexpected endpoint snapshots: %#v", snapshots)
	}
	snapshots[0].Models[0].Name = "mutated"
	snapshots[0].Models[0].Operations[0] = "mutated"
	again := registry.EndpointSnapshots()
	if again[0].Models[0].Name != "a-model" || again[0].Models[0].Operations[0] != "read" {
		t.Fatalf("caller mutation changed registry snapshot: %#v", again)
	}
}

func TestCapabilityLeaseRejectsChangedAuthorization(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{DefaultPool: "primary", Logger: zap.NewNop()})
	const address = "http://reader.internal"
	p.registry.RegisterEndpoint(address, "primary", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, address, "read", "owner/reader")
	endpoints := p.router.ResolveEndpointCandidates("owner/reader", "primary", nil, "read")
	token, err := issueReaderCapabilityLease(p, "owner/reader", "revision-a", "Bearer first", capabilityEndpointSet(p.registry, endpoints))
	if err != nil {
		t.Fatal(err)
	}

	err = p.validateCapabilityLease(token, "revision-a", "owner/reader", "read", "Bearer second")
	var resolutionErr *ResolutionError
	if !errors.As(err, &resolutionErr) || resolutionErr.StatusCode != http.StatusConflict {
		t.Fatalf("changed authorization error = %#v, want capability-stale conflict", err)
	}
}

func TestGeneratorCapabilityLeaseAllowsExplicitRouteVariants(t *testing.T) {
	t.Parallel()
	p := NewProxy(Config{DefaultPool: "primary", Logger: zap.NewNop()})
	const address = "http://generator.internal"
	p.registry.RegisterEndpoint(address, "primary", WorkloadTypeGeneral)
	p.registry.UpdateModelOperations(address, map[string]map[OperationType]bool{
		"gemma4": {"generate": true, "generate.batch": true, "chat.completions": true},
	})
	for _, operation := range []OperationType{"generate", "generate.batch", "chat.completions"} {
		endpoints := p.router.ResolveEndpointCandidates("gemma4", "primary", nil, operation)
		token, err := p.issueCapabilityLease("gemma4", "generate", operation, p.router.RouteManager().Generation(), "revision-a", "", capabilityEndpointSet(p.registry, endpoints))
		if err != nil {
			t.Fatal(err)
		}
		if err := p.validateCapabilityLease(token, "revision-a", "gemma4", operation, ""); err != nil {
			t.Fatalf("semantic generator lease rejected %q: %v", operation, err)
		}
	}
}

func TestCapabilityLeaseRejectsSemanticAliasReuse(t *testing.T) {
	t.Parallel()
	p := NewProxy(Config{DefaultPool: "primary", Logger: zap.NewNop()})
	const address = "http://generator.internal"
	p.registry.RegisterEndpoint(address, "primary", WorkloadTypeGeneral)
	p.registry.UpdateModelOperations(address, map[string]map[OperationType]bool{
		"gemma4": {"generate": true, "generate.batch": true},
	})
	endpoints := p.router.ResolveEndpointCandidates("gemma4", "primary", nil, "generate")
	token, err := p.issueCapabilityLease(
		"gemma4",
		"generate",
		"generate",
		p.router.RouteManager().Generation(),
		"revision-a",
		"",
		capabilityEndpointSet(p.registry, endpoints),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.validateCapabilityLease(token, "revision-a", "gemma4", "generate.batch", ""); err == nil {
		t.Fatal("single-generation lease was reused for the batch alias")
	}
}

func TestGeneratorCapabilityLeaseConstrainsEachRouteVariant(t *testing.T) {
	t.Parallel()
	p := NewProxy(Config{DefaultPool: "primary", Logger: zap.NewNop()})
	p.registry.RegisterEndpoint("http://single.internal", "primary", WorkloadTypeGeneral)
	p.registry.UpdateModelOperations("http://single.internal", map[string]map[OperationType]bool{
		"gemma4": {"generate": true},
	})
	p.registry.RegisterEndpoint("http://batch.internal", "primary", WorkloadTypeGeneral)
	p.registry.UpdateModelOperations("http://batch.internal", map[string]map[OperationType]bool{
		"gemma4": {"generate.batch": true},
	})
	for _, test := range []struct {
		operation OperationType
		address   string
	}{
		{operation: "generate", address: "http://single.internal"},
		{operation: "generate.batch", address: "http://batch.internal"},
	} {
		endpoints := p.router.ResolveEndpointCandidates("gemma4", "primary", nil, test.operation)
		token, err := p.issueCapabilityLease("gemma4", "generate", test.operation, p.router.RouteManager().Generation(), "revision-a", "", capabilityEndpointSet(p.registry, endpoints))
		if err != nil {
			t.Fatal(err)
		}
		allowed, err := p.capabilityLeaseEndpoints(token, "revision-a", "gemma4", test.operation, sha256.Sum256(nil))
		if err != nil {
			t.Fatalf("validate %q: %v", test.operation, err)
		}
		candidates := p.router.resolveEndpointCandidatesWithin("gemma4", "primary", nil, allowed, test.operation)
		if len(candidates) != 1 || candidates[0].address != test.address {
			t.Fatalf("%q candidates = %#v, want only %s", test.operation, candidates, test.address)
		}
	}
}

func TestScopedGeneratorCatalogLeaseExecutesSingleRoute(t *testing.T) {
	t.Parallel()
	p := NewProxy(Config{DefaultPool: "primary", Logger: zap.NewNop()})
	p.registry.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body := `{"generators":{"gemma4":{"inference_capabilities":{"version":3,"task":"generate","input_modalities":["text"],"accepted_mime_types":["text/plain"],"input_granularity":"item","batch":{"mode":"serial_compatibility","preferred_items":1,"max_items":8,"max_encoded_media_bytes":0,"max_decoded_pixels":0,"max_media_parts_per_item":0,"per_item_failures":true},"task_limits":{"max_text_bytes_per_item":null,"max_input_tokens_per_item":null,"max_output_tokens_per_item":null,"max_candidates_per_request":null},"output":"generated_text","result_cardinality":"one_per_item","prompt_policy":"model_default","borrowed_attachments":false}}}}`
		if req.Method == http.MethodPost {
			body = `{"data":[{"text":"ok"}]}`
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    req,
		}, nil
	})}
	const address = "http://generator.internal"
	p.registry.RegisterEndpoint(address, "primary", WorkloadTypeGeneral)
	p.registry.UpdateModelOperations(address, map[string]map[OperationType]bool{
		"gemma4": {"generate": true, "generate.batch": true, "chat.completions": true},
	})

	catalogRequest := httptest.NewRequest(http.MethodGet, "/ai/v1/models?model=gemma4&task=generate&operation=generate", nil)
	catalogRecorder := httptest.NewRecorder()
	p.handleModels(catalogRecorder, catalogRequest)
	if catalogRecorder.Code != http.StatusOK {
		t.Fatalf("catalog status = %d: %s", catalogRecorder.Code, catalogRecorder.Body.String())
	}

	request := httptest.NewRequest(http.MethodPost, "/ai/v1/generate", strings.NewReader(`{"model":"gemma4"}`))
	request.Header.Set(capabilityTokenHeader, catalogRecorder.Header().Get(capabilityTokenHeader))
	request.Header.Set(capabilityRevisionHeader, catalogRecorder.Header().Get(capabilityRevisionHeader))
	recorder := httptest.NewRecorder()
	p.handleGenerate(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("single generation status = %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestScopedCatalogLeaseFollowsNonDefaultRouteAndCallerCatalog(t *testing.T) {
	t.Parallel()
	var executedHost atomic.Value
	p := NewProxy(Config{DefaultPool: "default", Logger: zap.NewNop()})
	p.registry.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body := `{}`
		if req.Method == http.MethodGet && req.URL.Host == "gpu.internal" {
			body = `{"generators":{"tenant/gemma4":{}}}`
		}
		if req.Method == http.MethodPost {
			executedHost.Store(req.URL.Host)
			body = `{"text":"ok"}`
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    req,
		}, nil
	})}
	p.RegisterEndpoint("http://default.internal", "default", WorkloadTypeGeneral)
	p.RegisterEndpoint("http://gpu.internal", "gpu", WorkloadTypeGeneral)
	// The service-credential inventory intentionally does not advertise this
	// tenant-visible model. The caller-authorized scoped catalog is authoritative.
	p.Router().RouteManager().UpsertRoute(&Route{
		Name:          "default/tenant-generator",
		Operations:    map[OperationType]bool{"generate": true},
		ModelPatterns: []*RegexPattern{MustRegexPattern(`^tenant/gemma4$`)},
		Destinations:  []Destination{{Pool: "gpu", Weight: 100}},
	})

	catalogRequest := httptest.NewRequest(http.MethodGet, "/ai/v1/models?model=tenant%2Fgemma4&task=generate&operation=generate", nil)
	catalogRequest.Header.Set("Authorization", "Bearer tenant")
	catalogRecorder := httptest.NewRecorder()
	p.handleModels(catalogRecorder, catalogRequest)
	if catalogRecorder.Code != http.StatusOK {
		t.Fatalf("catalog status = %d: %s", catalogRecorder.Code, catalogRecorder.Body.String())
	}

	request := httptest.NewRequest(http.MethodPost, "/ai/v1/generate", strings.NewReader(`{"model":"tenant/gemma4"}`))
	request.Header.Set("Authorization", "Bearer tenant")
	request.Header.Set(capabilityTokenHeader, catalogRecorder.Header().Get(capabilityTokenHeader))
	request.Header.Set(capabilityRevisionHeader, catalogRecorder.Header().Get(capabilityRevisionHeader))
	recorder := httptest.NewRecorder()
	p.handleGenerate(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("generation status = %d: %s", recorder.Code, recorder.Body.String())
	}
	if host, _ := executedHost.Load().(string); host != "gpu.internal" {
		t.Fatalf("execution host = %q, want gpu.internal", host)
	}
}

func TestCapabilityLeaseRejectsChangedDescriptorRevision(t *testing.T) {
	t.Parallel()
	p := NewProxy(Config{DefaultPool: "primary", Logger: zap.NewNop()})
	const address = "http://reader.internal"
	p.registry.RegisterEndpoint(address, "primary", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, address, "read", "owner/reader")
	endpoints := p.router.ResolveEndpointCandidates("owner/reader", "primary", nil, "read")
	token, err := issueReaderCapabilityLease(p, "owner/reader", "revision-a", "", capabilityEndpointSet(p.registry, endpoints))
	if err != nil {
		t.Fatal(err)
	}
	err = p.validateCapabilityLease(token, "revision-b", "owner/reader", "read", "")
	var resolutionErr *ResolutionError
	if !errors.As(err, &resolutionErr) || resolutionErr.StatusCode != http.StatusConflict {
		t.Fatalf("changed descriptor revision error = %#v, want capability-stale conflict", err)
	}
}

func TestCapabilityLeaseRejectsRevisionWithoutToken(t *testing.T) {
	t.Parallel()
	p := NewProxy(Config{DefaultPool: "primary", Logger: zap.NewNop()})
	err := p.validateCapabilityLease("", "revision-a", "owner/reader", "read", "")
	var resolutionErr *ResolutionError
	if !errors.As(err, &resolutionErr) || resolutionErr.StatusCode != http.StatusConflict {
		t.Fatalf("incomplete lease error = %#v, want conflict", err)
	}
}

func TestCapabilityLeaseCapacityNeverEvictsLiveLease(t *testing.T) {
	t.Parallel()
	p := NewProxy(Config{DefaultPool: "primary", Logger: zap.NewNop()})
	const address = "http://reader.internal"
	p.registry.RegisterEndpoint(address, "primary", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, address, "read", "owner/reader")
	endpoints := capabilityEndpointSet(p.registry, p.router.ResolveEndpointCandidates("owner/reader", "primary", nil, "read"))
	var firstToken string
	for i := 0; i < maxCapabilityLeases; i++ {
		token, err := issueReaderCapabilityLease(p, "owner/reader", fmt.Sprintf("revision-%d", i), "", endpoints)
		if err != nil {
			t.Fatalf("lease %d: %v", i, err)
		}
		if i == 0 {
			firstToken = token
		}
	}
	if _, err := issueReaderCapabilityLease(p, "owner/reader", "overflow-revision", "", endpoints); !errors.Is(err, errCapabilityLeaseCapacity) {
		t.Fatalf("overflow error = %v, want capacity error", err)
	}
	if err := p.validateCapabilityLease(firstToken, "revision-0", "owner/reader", "read", ""); err != nil {
		t.Fatalf("capacity pressure invalidated oldest live lease: %v", err)
	}
}

func TestCapabilityLeaseRefreshReusesImmutableSnapshot(t *testing.T) {
	t.Parallel()
	p := NewProxy(Config{DefaultPool: "primary", Logger: zap.NewNop()})
	const address = "http://reader.internal"
	p.registry.RegisterEndpoint(address, "primary", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, address, "read", "owner/reader")
	endpoints := capabilityEndpointSet(p.registry, p.router.ResolveEndpointCandidates("owner/reader", "primary", nil, "read"))
	first, err := issueReaderCapabilityLease(p, "owner/reader", "revision-a", "Bearer caller", endpoints)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxCapabilityLeases*2; i++ {
		refreshed, refreshErr := issueReaderCapabilityLease(p, "owner/reader", "revision-a", "Bearer caller", endpoints)
		if refreshErr != nil {
			t.Fatalf("refresh %d: %v", i, refreshErr)
		}
		if refreshed != first {
			t.Fatalf("refresh %d minted token %q, want stable %q", i, refreshed, first)
		}
	}
	if len(p.capabilityLeases) != 1 || len(p.capabilityLeaseByID) != 1 {
		t.Fatalf("lease refresh retained %d tokens and %d identities", len(p.capabilityLeases), len(p.capabilityLeaseByID))
	}
}

func TestCapabilityLeaseIssuancePurgesObsoleteRouteGenerations(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{DefaultPool: "primary", Logger: zap.NewNop()})
	const address = "http://reader.internal"
	p.registry.RegisterEndpoint(address, "primary", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, address, "read", "owner/reader")
	p.Router().RouteManager().UpsertRoute(&Route{
		Name:         "reader",
		Operations:   map[OperationType]bool{"read": true},
		Destinations: []Destination{{Pool: "primary", Weight: 1}},
	})
	endpoints := capabilityEndpointSet(p.registry, p.router.ResolveEndpointCandidates("owner/reader", "primary", nil, "read"))
	oldGeneration := p.router.RouteManager().Generation()
	var oldToken string
	for i := 0; i < maxCapabilityLeases; i++ {
		token, err := p.issueCapabilityLease("owner/reader", "read", "read", oldGeneration, fmt.Sprintf("revision-old-%d", i), "", endpoints)
		if err != nil {
			t.Fatalf("old-generation lease %d: %v", i, err)
		}
		if i == 0 {
			oldToken = token
		}
	}

	p.Router().RouteManager().UpsertRoute(&Route{
		Name:         "reader",
		Priority:     1,
		Operations:   map[OperationType]bool{"read": true},
		Destinations: []Destination{{Pool: "primary", Weight: 1}},
	})
	newGeneration := p.router.RouteManager().Generation()
	newToken, err := p.issueCapabilityLease("owner/reader", "read", "read", newGeneration, "revision-new", "", endpoints)
	if err != nil {
		t.Fatal(err)
	}
	if newToken == oldToken {
		t.Fatal("new route generation reused the obsolete token")
	}
	if len(p.capabilityLeases) != 1 || len(p.capabilityLeaseByID) != 1 {
		t.Fatalf("lease tables retained obsolete generation: tokens=%d identities=%d", len(p.capabilityLeases), len(p.capabilityLeaseByID))
	}
	if _, ok := p.capabilityLeases[oldToken]; ok {
		t.Fatal("obsolete route-generation lease remains resident")
	}
	if _, err := p.issueCapabilityLease("owner/reader", "read", "read", oldGeneration, "revision-stale", "", endpoints); err == nil {
		t.Fatal("issuance accepted an already-obsolete routing generation")
	} else {
		var resolutionErr *ResolutionError
		if !errors.As(err, &resolutionErr) || !resolutionErr.CapabilityStale {
			t.Fatalf("stale issuance error = %#v, want typed capability-stale error", err)
		}
	}
}

func TestResolutionErrorWriterMarksOnlyCapabilityStaleness(t *testing.T) {
	t.Parallel()

	ordinary := httptest.NewRecorder()
	if !writeResolutionError(ordinary, &ResolutionError{StatusCode: http.StatusConflict, Message: "attempt still active"}) {
		t.Fatal("typed resolution error was not handled")
	}
	if ordinary.Header().Get(capabilityStaleHeader) != "" {
		t.Fatalf("ordinary conflict was marked capability-stale: %v", ordinary.Header())
	}

	stale := httptest.NewRecorder()
	if !writeResolutionError(stale, staleCapabilityResolutionError("stale plan")) {
		t.Fatal("typed stale error was not handled")
	}
	if stale.Code != http.StatusConflict || stale.Header().Get(capabilityStaleHeader) != "true" {
		t.Fatalf("stale response = %d headers=%v", stale.Code, stale.Header())
	}
}

func TestRouteManagerEquivalentUpdatePreservesGenerationAndState(t *testing.T) {
	t.Parallel()

	newRoute := func() *Route {
		return &Route{
			Name:                "default/full-policy",
			Priority:            7,
			Operations:          map[OperationType]bool{"generate.batch": true},
			ModelPatterns:       []*RegexPattern{MustRegexPattern(`^gemma`)},
			HeaderMatchers:      map[string]*StringMatcher{"x-tenant": {Prefix: "tenant-", Regex: MustRegexPattern(`tenant-[0-9]+`)}},
			SourceTables:        map[string]bool{"documents": true},
			SourceOrganizations: map[string]bool{"org": true},
			SourceProjects:      map[string]bool{"project": true},
			SourceAPIKeys:       map[string]bool{"prefix": true},
			TimeWindow:          &TimeWindow{StartHour: 1, EndHour: 2, Days: map[int]bool{1: true}},
			Destinations:        []Destination{{Pool: "gpu", Weight: 100, QueueDepthCondition: &ThresholdCondition{Operator: "<", Value: 10}, RequireModelLoaded: true}},
			Fallback:            &Fallback{Action: "reject", StatusCode: 429, RetryAfter: 1},
			RateLimiter:         NewRateLimiter(1, 1, true),
			RetryAttempts:       2,
			RetryTimeout:        time.Second,
			RetryOnStatuses:     map[int]bool{500: true},
			RetryOnRequestErrs:  true,
			RetryOnCanceled:     true,
		}
	}

	rm := NewRouteManager()
	first := newRoute()
	if changed, err := rm.UpsertRoute(first); err != nil || !changed {
		t.Fatal("initial route was not added")
	}
	routeRequest := &RouteRequest{
		Operation:          "generate.batch",
		Model:              "gemma4",
		Headers:            map[string]string{"x-tenant": "tenant-1"},
		SourceTable:        "documents",
		SourceOrganization: "org",
		SourceProject:      "project",
		SourceAPIKey:       "prefix",
		Timestamp:          time.Date(2026, time.January, 5, 1, 30, 0, 0, time.UTC),
	}
	installed := rm.Match(routeRequest)
	if installed == nil || installed == first {
		t.Fatal("route manager did not install an owned policy snapshot")
	}
	if installed.RateLimiter == first.RateLimiter {
		t.Fatal("route manager retained caller-owned rate-limiter runtime")
	}
	generation := rm.Generation()
	if changed, err := rm.UpsertRoute(newRoute()); err != nil || changed {
		t.Fatal("equivalent declarative update was treated as a policy change")
	}
	if got := rm.Generation(); got != generation {
		t.Fatalf("generation = %d after no-op update, want %d", got, generation)
	}
	matched := rm.Match(routeRequest)
	if matched == nil || matched.RateLimiter != installed.RateLimiter {
		t.Fatal("equivalent update replaced the live rate-limiter state")
	}
	changed := newRoute()
	changed.Priority++
	if didChange, err := rm.UpsertRoute(changed); err != nil || !didChange {
		t.Fatal("real route policy change was ignored")
	}
	if got := rm.Generation(); got != generation+1 {
		t.Fatalf("generation = %d after policy change, want %d", got, generation+1)
	}
	changedInstalled := rm.Match(routeRequest)
	if changedInstalled == nil || changedInstalled.RateLimiter != installed.RateLimiter {
		t.Fatal("unrelated policy change reset equivalent rate-limiter runtime")
	}
}

func TestRouteManagerOwnsImmutablePolicySnapshots(t *testing.T) {
	t.Parallel()

	rm := NewRouteManager()
	source := &Route{
		Name:               "default/reader",
		Operations:         map[OperationType]bool{"read": true},
		ModelPatterns:      []*RegexPattern{MustRegexPattern(`^reader$`)},
		HeaderMatchers:     map[string]*StringMatcher{"x-tenant": {Exact: "tenant-a"}},
		SourceTables:       map[string]bool{"documents": true},
		Destinations:       []Destination{{Pool: "gpu", Weight: 1, QueueDepthCondition: &ThresholdCondition{Operator: "<", Value: 10}}},
		Fallback:           &Fallback{Action: "redirect", RedirectPool: "backup"},
		RetryOnStatuses:    map[int]bool{503: true},
		RateLimiter:        NewRateLimiter(10, 10, false),
		RetryOnRequestErrs: true,
	}
	if changed, err := rm.UpsertRoute(source); err != nil || !changed {
		t.Fatal("initial route was not installed")
	}
	generation := rm.Generation()

	// Mutating the object supplied by the caller must not alter installed policy.
	source.Operations["read"] = false
	source.ModelPatterns[0].Expression = "^other$"
	source.HeaderMatchers["x-tenant"].Exact = "tenant-b"
	source.SourceTables["documents"] = false
	source.Destinations[0].Pool = "mutated"
	source.Destinations[0].QueueDepthCondition.Value = 0
	source.Fallback.RedirectPool = "mutated"
	source.RetryOnStatuses[503] = false

	req := &RouteRequest{
		Operation:   "read",
		Model:       "reader",
		Headers:     map[string]string{"x-tenant": "tenant-a"},
		SourceTable: "documents",
		Timestamp:   time.Now(),
	}
	matched := rm.Match(req)
	if matched == nil || matched.Destinations[0].Pool != "gpu" || matched.Fallback.RedirectPool != "backup" {
		t.Fatalf("caller mutation changed installed route: %#v", matched)
	}

	// Mutating a returned snapshot must likewise not alter future matches.
	matched.Operations["read"] = false
	matched.ModelPatterns[0].Expression = "^other$"
	matched.Destinations[0].Pool = "returned-mutation"
	matched.Fallback.RedirectPool = "returned-mutation"
	again := rm.Match(req)
	if again == nil || again.Destinations[0].Pool != "gpu" || again.Fallback.RedirectPool != "backup" {
		t.Fatalf("returned route mutation changed installed policy: %#v", again)
	}
	if rm.Generation() != generation {
		t.Fatalf("snapshot-only mutations changed generation: got %d want %d", rm.Generation(), generation)
	}
}

func TestRouteManagerOwnsDeclarativeRegexSemantics(t *testing.T) {
	t.Parallel()
	rm := NewRouteManager()
	base, err := CompileRegexPattern("a|aa", RegexLeftmostFirst)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := rm.UpsertRoute(&Route{Name: "regex", ModelPatterns: []*RegexPattern{base}})
	if err != nil || !changed {
		t.Fatalf("initial regex route changed=%v err=%v", changed, err)
	}
	generation := rm.Generation()
	longest, err := CompileRegexPattern("a|aa", RegexLeftmostLongest)
	if err != nil {
		t.Fatal(err)
	}
	changed, err = rm.UpsertRoute(&Route{Name: "regex", ModelPatterns: []*RegexPattern{longest}})
	if err != nil || !changed {
		t.Fatalf("regex syntax update changed=%v err=%v", changed, err)
	}
	if rm.Generation() != generation+1 {
		t.Fatalf("generation = %d, want %d", rm.Generation(), generation+1)
	}
	if _, err := rm.UpsertRoute(&Route{
		Name:          "invalid",
		ModelPatterns: []*RegexPattern{{Expression: "["}},
	}); err == nil {
		t.Fatal("invalid declarative regex was installed")
	}
}

func TestRouteManagerRejectsNilDeclarativeMatchers(t *testing.T) {
	t.Parallel()
	rm := NewRouteManager()

	for name, route := range map[string]*Route{
		"model pattern": {
			Name:          "nil-model-pattern",
			ModelPatterns: []*RegexPattern{nil},
		},
		"header matcher": {
			Name:           "nil-header-matcher",
			HeaderMatchers: map[string]*StringMatcher{"x-tenant": nil},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if changed, err := rm.UpsertRoute(route); err == nil || changed {
				t.Fatalf("malformed route changed=%v err=%v, want rejection", changed, err)
			}
		})
	}

	if got := rm.Generation(); got != 0 {
		t.Fatalf("rejected routes changed generation to %d", got)
	}
}

func TestRouteManagerInstalledMatchDoesNotAllocate(t *testing.T) {
	rm := NewRouteManager()
	if changed, err := rm.UpsertRoute(&Route{
		Name:       "readers",
		Operations: map[OperationType]bool{"read": true},
	}); err != nil || !changed {
		t.Fatalf("install route changed=%v err=%v", changed, err)
	}
	req := &RouteRequest{Operation: "read", Timestamp: time.Now()}
	if allocations := testing.AllocsPerRun(1000, func() {
		if rm.matchInstalled(req) == nil {
			panic("route disappeared")
		}
	}); allocations != 0 {
		t.Fatalf("installed route match allocations = %v, want 0", allocations)
	}
}

func TestCapabilityLeaseKeepsImmutableSnapshotAcrossCatalogRefresh(t *testing.T) {
	t.Parallel()
	p := NewProxy(Config{DefaultPool: "primary", Logger: zap.NewNop()})
	const address = "http://reader.internal"
	p.registry.RegisterEndpoint(address, "primary", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, address, "read", "owner/reader")
	endpoints := p.router.ResolveEndpointCandidates("owner/reader", "primary", nil, "read")
	token, err := issueReaderCapabilityLease(p, "owner/reader", "revision-a", "", capabilityEndpointSet(p.registry, endpoints))
	if err != nil {
		t.Fatal(err)
	}
	p.registry.updateModelOperationsForEndpoint(
		address,
		endpoints[0],
		endpoints[0].incarnation,
		map[string]map[OperationType]bool{"owner/reader": {"read": true}},
	)
	err = p.validateCapabilityLease(token, "revision-a", "owner/reader", "read", "")
	if err != nil {
		t.Fatalf("immutable lease was invalidated by a later catalog snapshot: %v", err)
	}
}

func capabilityEndpointSet(registry *ModelRegistry, endpoints []*Endpoint) map[string]leasedEndpoint {
	_ = registry
	result := make(map[string]leasedEndpoint, len(endpoints))
	for _, endpoint := range endpoints {
		operations := make(map[OperationType]bool)
		for _, info := range endpoint.models {
			for operation := range info.Operations {
				operations[operation] = true
			}
		}
		result[endpoint.address] = leasedEndpoint{
			incarnation: endpoint.incarnation,
			operations:  operations,
		}
	}
	return result
}

func issueReaderCapabilityLease(p *Proxy, model, revision, authorization string, endpoints map[string]leasedEndpoint) (string, error) {
	return p.issueCapabilityLease(
		model,
		"read",
		"read",
		p.router.RouteManager().Generation(),
		revision,
		authorization,
		endpoints,
	)
}

func TestProxyBodyAdmissionAccountsForDecodeAndExactRetention(t *testing.T) {
	t.Parallel()
	const body = `{"model":"gemma4"}`
	known, err := readProxyRequestBody(strings.NewReader(body), int64(len(body)), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(known) != len(body) || cap(known) != len(body) {
		t.Fatalf("known body len/cap = %d/%d, want %d/%d", len(known), cap(known), len(body), len(body))
	}
	if got := proxyBodyAdmissionBytes(int64(len(body))); got != int64(3*len(body)) {
		t.Fatalf("admission = %d, want %d", got, 3*len(body))
	}
	unknown, err := readProxyRequestBody(strings.NewReader(body), -1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if string(unknown) != body || cap(unknown) >= 1<<20 {
		t.Fatalf("unknown-length body len/cap = %d/%d, want retained bytes without full-ceiling allocation", len(unknown), cap(unknown))
	}
	if got, want := proxyMaterializedBodyAdmissionBytes(int64(len(unknown)), int64(cap(unknown))), int64(cap(unknown)+2*len(unknown)); got != want {
		t.Fatalf("materialized admission = %d, want capacity-aware %d", got, want)
	}
	nearLimit, err := readProxyRequestBody(strings.NewReader(strings.Repeat("x", 1000)), -1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if cap(nearLimit) > 1024 || proxyMaterializedBodyAdmissionBytes(int64(len(nearLimit)), int64(cap(nearLimit))) > proxyBodyAdmissionBytes(1024) {
		t.Fatalf("near-limit body escaped streaming reservation: len/cap=%d/%d", len(nearLimit), cap(nearLimit))
	}
}

func TestScopedCatalogRequiresProcessWideByteAdmission(t *testing.T) {
	t.Parallel()

	p := NewProxy(Config{
		DefaultPool:             "primary",
		RefreshInterval:         time.Minute,
		MaxRetainedCatalogBytes: 1,
		Logger:                  zap.NewNop(),
	})
	p.RegisterEndpoint("http://reader.internal", "primary", WorkloadTypeGeneral)
	advertiseModelOperation(p.registry, "http://reader.internal", "read", "owner/reader")

	request := httptest.NewRequest(http.MethodGet, "/ai/v1/models?model=owner%2Freader&task=read", nil)
	recorder := httptest.NewRecorder()
	p.handleModels(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	if used := p.catalogAdmission.Used(); used != 0 {
		t.Fatalf("catalog admission leaked %d bytes", used)
	}
}

func TestScopedCatalogReducesFanoutToFitRetainedByteLimit(t *testing.T) {
	t.Parallel()

	const retainedLimit = int64(64 << 20)
	workers, reservation, err := modelCatalogFanoutPlan(2, retainedLimit)
	if err != nil {
		t.Fatal(err)
	}
	if workers != 1 || reservation != 56<<20 {
		t.Fatalf("plan = %d workers, %d bytes; want 1 worker, %d bytes", workers, reservation, 56<<20)
	}

	p := NewProxy(Config{
		DefaultPool:             "primary",
		RefreshInterval:         time.Minute,
		MaxRetainedCatalogBytes: retainedLimit,
		Logger:                  zap.NewNop(),
	})
	p.registry.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"readers":{"owner/reader":{}}}`)),
			Request:    req,
		}, nil
	})}
	for _, address := range []string{"http://reader-a.internal", "http://reader-b.internal"} {
		p.RegisterEndpoint(address, "primary", WorkloadTypeGeneral)
		advertiseModelOperation(p.registry, address, "read", "owner/reader")
	}

	request := httptest.NewRequest(http.MethodGet, "/ai/v1/models?model=owner%2Freader&task=read", nil)
	recorder := httptest.NewRecorder()
	p.handleModels(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	if used := p.catalogAdmission.Used(); used != 0 {
		t.Fatalf("catalog admission leaked %d bytes", used)
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

	// The proxy's outward catalog describes its HTTP route, so even two
	// linked-capable upstreams cannot advertise borrowed memory downstream.
	left = base([]any{"image"}, []any{"image/png"}, true)
	right = base([]any{"image"}, []any{"image/png"}, true)
	merged, ok = conservativeInferenceCapabilities(left, right)
	if !ok || merged["borrowed_attachments"] != false {
		t.Fatalf("proxy leaked an upstream borrowed-memory claim: %#v", merged)
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
		"non-string task":     func(value map[string]any) { value["task"] = []any{"extract"} },
		"unknown scalar":      func(value map[string]any) { value["prompt_policy"] = "sometimes" },
		"wrong cardinality":   func(value map[string]any) { value["result_cardinality"] = "one_per_request" },
		"preferred exceeds maximum": func(value map[string]any) {
			value["batch"].(map[string]any)["preferred_items"] = float64(9)
		},
		"none mode is not singleton": func(value map[string]any) {
			batch := value["batch"].(map[string]any)
			batch["mode"] = "none"
			batch["preferred_items"] = float64(1)
			batch["max_items"] = float64(8)
		},
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

func TestConservativeCapabilitiesV4PreservesExtensibleMIMEAndTaskLimits(t *testing.T) {
	base := func(textBytes any, outputTokens any) map[string]any {
		return map[string]any{
			"version": float64(4), "task": "extract",
			"input_modalities": []any{"image"}, "accepted_mime_types": []any{"image/png", "image/tiff"},
			"input_granularity": "page", "output": "extraction",
			"result_cardinality": "one_per_item", "prompt_policy": "structured_schema",
			"borrowed_attachments": true,
			"task_limits": map[string]any{
				"max_text_bytes_per_item": textBytes, "max_input_tokens_per_item": nil,
				"max_output_tokens_per_item": outputTokens, "max_candidates_per_request": nil,
				"max_schema_bytes": float64(4096),
			},
			"batch": map[string]any{
				"mode": "serial_compatibility", "preferred_items": float64(8), "max_items": float64(32),
				"max_encoded_media_bytes": float64(4096), "max_decoded_pixels": float64(8192),
				"max_media_parts_per_item": float64(1), "per_item_failures": false,
			},
		}
	}
	merged, ok := conservativeInferenceCapabilities(base(float64(2048), float64(512)), base(float64(1024), nil))
	if !ok {
		t.Fatal("v4 capabilities did not merge")
	}
	if merged["version"] != float64(4) || !reflect.DeepEqual(merged["accepted_mime_types"], []string{"image/png", "image/tiff"}) {
		t.Fatalf("extensible v4 MIME contract was lost: %#v", merged)
	}
	limits := merged["task_limits"].(map[string]any)
	if limits["max_text_bytes_per_item"] != float64(1024) || limits["max_output_tokens_per_item"] != nil {
		t.Fatalf("v4 limits were not conservatively intersected: %#v", limits)
	}

	malformed := base(float64(1024), nil)
	malformed["accepted_mime_types"] = []any{"image/tiff;codec=raw"}
	if _, ok := conservativeInferenceCapabilities(malformed, base(float64(1024), nil)); ok {
		t.Fatal("catalog accepted a parameterized MIME capability")
	}
	alias := base(float64(1024), nil)
	alias["accepted_mime_types"] = []any{"image/jpg"}
	if _, ok := conservativeInferenceCapabilities(alias, base(float64(1024), nil)); ok {
		t.Fatal("catalog accepted a non-canonical MIME alias")
	}
	excess := base(float64(1024), nil)
	mimes := make([]any, maxAdditionalInferenceMIMEValues+1)
	for index := range mimes {
		mimes[index] = fmt.Sprintf("image/x-test-%d", index)
	}
	excess["accepted_mime_types"] = mimes
	if _, ok := conservativeInferenceCapabilities(excess, base(float64(1024), nil)); ok {
		t.Fatal("catalog accepted more MIME extensions than a peer can retain")
	}
}

func TestSanitizeModelDescriptorRejectsContradictoryLegacyBatch(t *testing.T) {
	raw := json.RawMessage(`{"inference_capabilities":{"version":2,"task":"generate","batch":{"mode":"none","preferred_items":1,"max_items":2,"max_encoded_media_bytes":0,"max_decoded_pixels":0,"max_media_parts_per_item":0,"per_item_failures":false}}}`)
	if got := string(sanitizeModelDescriptor(raw)); got != "{}" {
		t.Fatalf("malformed legacy singleton descriptor was not poisoned: %s", got)
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

func TestProxyModelCatalogLeasesOnlySuccessfulCandidates(t *testing.T) {
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
	p.handleModels(recorder, httptest.NewRequest(http.MethodGet, "/ai/v1/models?model=gemma4&task=generate", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	token := recorder.Header().Get(capabilityTokenHeader)
	revision := recorder.Header().Get(capabilityRevisionHeader)
	allowed, err := p.capabilityLeaseEndpoints(token, revision, "gemma4", "generate.batch", sha256.Sum256(nil))
	if err != nil {
		t.Fatal(err)
	}
	if endpoint, ok := allowed["http://healthy.internal"]; len(allowed) != 1 || !ok || endpoint.endpoint == nil {
		t.Fatalf("leased endpoints = %#v, want only healthy catalog responder", allowed)
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
