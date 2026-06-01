// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package proxy

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGatewayResolve(t *testing.T) {
	gateway := NewGateway(NewRouter([]NamespaceRoute{
		{
			Tenant:             "t1",
			Table:              "docs",
			Namespace:          "docs",
			AllowServerless:    true,
			ServerlessQueryURL: "http://serverless-query.default.svc:8080",
			ServerlessAPIURL:   "http://serverless-api.default.svc:8080",
		},
	}))

	resolved, err := gateway.Resolve(context.Background(), RequestContext{
		Tenant:       "t1",
		Table:        "docs",
		Operation:    OperationRead,
		RequireGraph: true,
		Policy: RequestPolicy{
			View: ViewPublished,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resolved.Backend != BackendServerless {
		t.Fatalf("got %q want %q", resolved.Backend, BackendServerless)
	}
	if resolved.TargetURL != "http://serverless-query.default.svc:8080" {
		t.Fatalf("got target %q", resolved.TargetURL)
	}
}

func TestGatewayServeHTTP(t *testing.T) {
	gateway := NewGateway(NewRouter([]NamespaceRoute{
		{
			Tenant:             "t1",
			Table:              "docs",
			Namespace:          "docs",
			AllowServerless:    true,
			ServerlessQueryURL: "http://serverless-query.default.svc:8080",
			ServerlessAPIURL:   "http://serverless-api.default.svc:8080",
		},
	}))

	req := httptest.NewRequest(http.MethodGet, "/resolve?tenant=t1&table=docs&operation=read&graph=true&view=latest", nil)
	rec := httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d body=%s", rec.Code, rec.Body.String())
	}

	var resolved ResolvedTarget
	if err := json.Unmarshal(rec.Body.Bytes(), &resolved); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resolved.Backend != BackendServerless {
		t.Fatalf("got backend %q", resolved.Backend)
	}
	if resolved.View != ViewLatest {
		t.Fatalf("got view %q", resolved.View)
	}
}

func TestGatewayRejectsNonTablePublicPath(t *testing.T) {
	gateway := NewGateway(NewRouter([]NamespaceRoute{
		{
			Tenant:             "t1",
			Table:              "docs",
			Namespace:          "docs",
			AllowServerless:    true,
			ServerlessQueryURL: "http://serverless-query.default.svc:8080",
			ServerlessAPIURL:   "http://serverless-api.default.svc:8080",
		},
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/t1/namespaces/docs/search", nil)
	rec := httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("got status %d body=%s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "tenant and table are required\n" {
		t.Fatalf("unexpected body %q", rec.Body.String())
	}
}

func TestResolveRequestOperationEnforcesInferredMinimum(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
		hint   string
		want   OperationKind
	}{
		{
			name:   "table root mutations require admin",
			method: http.MethodPost,
			path:   "/",
			hint:   "write",
			want:   OperationAdmin,
		},
		{
			name:   "admin paths cannot be downgraded",
			method: http.MethodDelete,
			path:   "/admin/indexes/title",
			hint:   "write",
			want:   OperationAdmin,
		},
		{
			name:   "query post remains read",
			method: http.MethodPost,
			path:   "/query/search",
			want:   OperationRead,
		},
		{
			name:   "known read paths ignore stricter hints",
			method: http.MethodPost,
			path:   "/query/search",
			hint:   "admin",
			want:   OperationRead,
		},
		{
			name:   "hints can request stricter auth for generic paths",
			method: http.MethodPost,
			path:   "/ingest-batch",
			hint:   "admin",
			want:   OperationAdmin,
		},
		{
			name:   "generic mutating paths cannot be downgraded",
			method: http.MethodPut,
			path:   "/ingest-batch",
			hint:   "read",
			want:   OperationWrite,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveRequestOperation(tt.method, tt.path, tt.hint)
			if err != nil {
				t.Fatalf("resolve operation: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}

func TestGatewayRequiresAdminForTableRootMutation(t *testing.T) {
	gateway := NewGateway(NewRouter([]NamespaceRoute{
		{
			Tenant:        "t1",
			Table:         "docs",
			Namespace:     "docs",
			AllowStateful: true,
			StatefulURL:   "http://stateful.invalid/db/v1/tables/docs",
		},
	}))
	gateway.authenticator = StaticBearerAuthenticator{
		Required: true,
		Tokens: map[string]Principal{
			"test-token": {
				Subject:           "user-1",
				Tenant:            "t1",
				AllowedTables:     []string{"docs"},
				AllowedOperations: []OperationKind{OperationWrite},
			},
		},
	}

	req := httptest.NewRequest(http.MethodPost, "/proxy/", strings.NewReader(`{"columns":[]}`))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("X-Antfly-Tenant", "t1")
	req.Header.Set("X-Antfly-Table", "docs")
	req.Header.Set("X-Antfly-Operation", "write")
	rec := httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("got status %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `cannot perform operation "admin"`) {
		t.Fatalf("unexpected body %q", rec.Body.String())
	}
}

func TestGatewayServeHTTPProxyForward(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tables/docs/query/search" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		if r.URL.RawQuery != "max_lag_records=25&required_version=7&view=published" {
			t.Fatalf("unexpected forwarded query %q", r.URL.RawQuery)
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Fatalf("authorization header not forwarded")
		}
		if r.Header.Get("X-Antfly-Required-Version") != "7" {
			t.Fatalf("required version header not forwarded")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Upstream", "ok")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"namespace":"docs","version":9,"view":"published","freshness_lag_records":2,"hit_count":1,"hits":[{"doc_id":"doc-1","body":{"title":"Doc 1"},"score":17}]}`))
	}))
	defer backend.Close()

	gateway := NewGateway(NewRouter([]NamespaceRoute{
		{
			Tenant:             "t1",
			Table:              "docs",
			Namespace:          "docs",
			AllowServerless:    true,
			ServerlessQueryURL: backend.URL,
			ServerlessAPIURL:   backend.URL,
		},
	}))
	gateway.authenticator = StaticBearerAuthenticator{
		Required: true,
		Tokens: map[string]Principal{
			"test-token": {Subject: "user-1", Tenant: "t1"},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/t1/tables/docs/search?graph=false&max_lag_records=25&required_version=7", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d body=%s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-Antfly-Backend") != string(BackendServerless) {
		t.Fatalf("missing normalized backend header")
	}
	if rec.Header().Get("X-Antfly-Table") != "docs" {
		t.Fatalf("missing normalized table header")
	}
	if rec.Header().Get("X-Antfly-Namespace") != "" {
		t.Fatalf("serving namespace should not be exposed publicly")
	}
	if rec.Header().Get("X-Antfly-Required-Version") != "7" {
		t.Fatalf("missing normalized required version header")
	}
	if rec.Header().Get("X-Upstream") != "ok" {
		t.Fatalf("expected upstream header passthrough")
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal normalized payload: %v", err)
	}
	if payload["kind"] != "query.search" || payload["backend"] != string(BackendServerless) {
		t.Fatalf("unexpected payload: %#v", payload)
	}
	if payload["table"] != "docs" || payload["view"] != ViewPublished {
		t.Fatalf("unexpected payload metadata: %#v", payload)
	}
	hits, ok := payload["hits"].([]any)
	if !ok || len(hits) != 1 {
		t.Fatalf("unexpected hits payload: %#v", payload["hits"])
	}
}

func TestGatewayServeHTTPProxyForwardNormalizesBackendError(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream exploded", http.StatusBadGateway)
	}))
	defer backend.Close()

	gateway := NewGateway(NewRouter([]NamespaceRoute{
		{
			Tenant:             "t1",
			Table:              "docs",
			Namespace:          "docs",
			AllowServerless:    true,
			ServerlessQueryURL: backend.URL,
			ServerlessAPIURL:   backend.URL,
		},
	}))
	gateway.authenticator = StaticBearerAuthenticator{
		Required: true,
		Tokens: map[string]Principal{
			"test-token": {Subject: "user-1", Tenant: "t1"},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/t1/tables/docs/search", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("got status %d body=%s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("got content type %q", rec.Header().Get("Content-Type"))
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal error payload: %v", err)
	}
	if payload["backend"] != string(BackendServerless) {
		t.Fatalf("unexpected backend payload: %#v", payload)
	}
	if payload["tenant"] != "t1" || payload["table"] != "docs" {
		t.Fatalf("unexpected tenant/table payload: %#v", payload)
	}
}

func TestGatewayServeHTTPProxyForwardNormalizesStatefulSearchResponse(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/query/search" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"total":2,"fusion_result":{"total":2,"hits":[{"id":"doc-a","score":0.91,"fields":{"title":"A"},"index_scores":{"full_text":0.8}},{"id":"doc-b","score":0.77,"fields":{"title":"B"}}]}}`))
	}))
	defer backend.Close()

	gateway := NewGateway(NewRouter([]NamespaceRoute{
		{
			Tenant:        "t1",
			Table:         "docs",
			Namespace:     "docs",
			AllowStateful: true,
			StatefulURL:   backend.URL,
		},
	}))
	gateway.authenticator = StaticBearerAuthenticator{
		Required: true,
		Tokens: map[string]Principal{
			"test-token": {Subject: "user-1", Tenant: "t1"},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/t1/tables/docs/search", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d body=%s", rec.Code, rec.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal normalized payload: %v", err)
	}
	if payload["backend"] != string(BackendStateful) || payload["kind"] != "query.search" {
		t.Fatalf("unexpected payload: %#v", payload)
	}
	hits, ok := payload["hits"].([]any)
	if !ok || len(hits) != 2 {
		t.Fatalf("unexpected hits payload: %#v", payload["hits"])
	}
}

func TestGatewayServeHTTPProxyForwardNormalizesGraphResponse(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tables/docs/query/graph/neighbors" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"namespace":"docs","version":5,"freshness_lag_records":1,"node_id":"root","direction":"out","neighbor_count":1,"neighbors":[{"doc_id":"child","edge_type":"child","weight":1,"direction":"out"}]}`))
	}))
	defer backend.Close()

	gateway := NewGateway(NewRouter([]NamespaceRoute{
		{
			Tenant:             "t1",
			Table:              "docs",
			Namespace:          "docs",
			AllowStateful:      true,
			AllowServerless:    true,
			StatefulURL:        "http://stateful.invalid",
			ServerlessQueryURL: backend.URL,
			ServerlessAPIURL:   backend.URL,
		},
	}))
	gateway.authenticator = StaticBearerAuthenticator{
		Required: true,
		Tokens: map[string]Principal{
			"test-token": {Subject: "user-1", Tenant: "t1"},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/t1/tables/docs/graph/neighbors", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d body=%s", rec.Code, rec.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal normalized payload: %v", err)
	}
	if payload["backend"] != string(BackendServerless) || payload["kind"] != "graph.neighbors" {
		t.Fatalf("unexpected payload: %#v", payload)
	}
	if payload["total"].(float64) != 1 {
		t.Fatalf("unexpected total: %#v", payload)
	}
}

func TestGatewayServeHTTPProxyForwardRoutesServerlessWritesToAPI(t *testing.T) {
	apiBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("unexpected method %q", r.Method)
		}
		if r.URL.Path != "/tables/docs/ingest-batch" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Fatalf("authorization header not forwarded")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"accepted":1}`))
	}))
	defer apiBackend.Close()

	gateway := NewGateway(NewRouter([]NamespaceRoute{
		{
			Tenant:             "t1",
			Table:              "docs",
			Namespace:          "docs",
			AllowStateful:      false,
			AllowServerless:    true,
			ServerlessQueryURL: "http://serverless-query.invalid",
			ServerlessAPIURL:   apiBackend.URL,
		},
	}))
	gateway.authenticator = StaticBearerAuthenticator{
		Required: true,
		Tokens: map[string]Principal{
			"test-token": {Subject: "user-1", Tenant: "t1", AllowedTables: []string{"docs"}, AllowedOperations: []OperationKind{OperationWrite}},
		},
	}

	req := httptest.NewRequest(http.MethodPut, "/v1/tenants/t1/tables/docs/ingest-batch", strings.NewReader(`{"records":[{"id":"doc-1"}]}`))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d body=%s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-Antfly-Backend") != string(BackendServerless) {
		t.Fatalf("missing normalized backend header")
	}
	if rec.Body.String() != `{"accepted":1}` {
		t.Fatalf("unexpected body %q", rec.Body.String())
	}
}

func TestGatewayDeniesJoinedTableOutsideScope(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("backend should not be called")
	}))
	defer backend.Close()

	gateway := NewGateway(NewRouter([]NamespaceRoute{
		{
			Tenant:             "t1",
			Table:              "orders",
			Namespace:          "orders",
			AllowServerless:    true,
			ServerlessQueryURL: backend.URL,
			ServerlessAPIURL:   backend.URL,
		},
	}))
	gateway.authenticator = StaticBearerAuthenticator{
		Required: true,
		Tokens: map[string]Principal{
			"test-token": {
				Subject:           "user-1",
				Tenant:            "t1",
				AllowedTables:     []string{"orders"},
				AllowedOperations: []OperationKind{OperationRead},
			},
		},
	}

	body := `{"join":{"right_table":"customers","on":{"left_field":"customer_id","right_field":"id"}}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/t1/tables/orders/query/search", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("got status %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `cannot access table "customers"`) {
		t.Fatalf("unexpected body %q", rec.Body.String())
	}
}

func TestGatewayInjectsJoinedTableRowFilters(t *testing.T) {
	var forwardedBody string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read forwarded body: %v", err)
		}
		forwardedBody = string(body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"namespace":"orders","version":1,"view":"published","hit_count":0,"hits":[]}`))
	}))
	defer backend.Close()

	gateway := NewGateway(NewRouter([]NamespaceRoute{
		{
			Tenant:             "t1",
			Table:              "orders",
			Namespace:          "orders",
			AllowServerless:    true,
			ServerlessQueryURL: backend.URL,
			ServerlessAPIURL:   backend.URL,
		},
	}))
	gateway.authenticator = StaticBearerAuthenticator{
		Required: true,
		Tokens: map[string]Principal{
			"test-token": {
				Subject:           "user-1",
				Tenant:            "t1",
				AllowedTables:     []string{"orders", "customers"},
				AllowedOperations: []OperationKind{OperationRead},
				RowFilter: map[string]json.RawMessage{
					"orders":    json.RawMessage(`{"term":{"tenant_id":"t1"}}`),
					"customers": json.RawMessage(`{"term":{"region":"na"}}`),
				},
			},
		},
	}

	body := `{"filter_query":{"query":"status:pending"},"join":{"right_table":"customers","right_filters":{"filter_query":{"query":"tier:premium"}}}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/t1/tables/orders/query/search", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d body=%s", rec.Code, rec.Body.String())
	}
	var parsed map[string]json.RawMessage
	if err := json.Unmarshal([]byte(forwardedBody), &parsed); err != nil {
		t.Fatalf("parse forwarded body: %v body=%s", err, forwardedBody)
	}
	var primary map[string][]json.RawMessage
	if err := json.Unmarshal(parsed["filter_query"], &primary); err != nil {
		t.Fatalf("parse primary filter: %v", err)
	}
	if len(primary["conjuncts"]) != 2 {
		t.Fatalf("expected primary conjunction, got %s", parsed["filter_query"])
	}
	var join map[string]json.RawMessage
	if err := json.Unmarshal(parsed["join"], &join); err != nil {
		t.Fatalf("parse join: %v", err)
	}
	var rightFilters map[string]json.RawMessage
	if err := json.Unmarshal(join["right_filters"], &rightFilters); err != nil {
		t.Fatalf("parse right filters: %v", err)
	}
	var right map[string][]json.RawMessage
	if err := json.Unmarshal(rightFilters["filter_query"], &right); err != nil {
		t.Fatalf("parse right conjunction: %v", err)
	}
	if len(right["conjuncts"]) != 2 {
		t.Fatalf("expected right conjunction, got %s", rightFilters["filter_query"])
	}
}
