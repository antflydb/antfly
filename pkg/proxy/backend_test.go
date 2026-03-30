package proxy

import (
	"net/http"
	"testing"
)

func TestStatefulBackendAdapter(t *testing.T) {
	adapter := StatefulBackendAdapter{}
	route := NamespaceRoute{
		Tenant:        "acme",
		Table:         "docs",
		Namespace:     "docs",
		AllowStateful: true,
		StatefulURL:   "http://stateful.default.svc:8080",
	}
	req := RequestContext{
		Tenant:    "acme",
		Table:     "docs",
		Operation: OperationRead,
	}

	if !adapter.CanServe(req, route) {
		t.Fatal("expected stateful adapter to serve plain read")
	}
	if _, err := adapter.BaseURL(req, route); err != nil {
		t.Fatalf("expected stateful base URL, got: %v", err)
	}
}

func TestServerlessBackendAdapter(t *testing.T) {
	adapter := ServerlessBackendAdapter{}
	route := NamespaceRoute{
		Tenant:             "acme",
		Table:              "docs",
		Namespace:          "docs",
		AllowServerless:    true,
		ServerlessQueryURL: "http://serverless-query.default.svc:8080",
		ServerlessAPIURL:   "http://serverless-api.default.svc:8080",
	}
	req := RequestContext{
		Tenant:       "acme",
		Table:        "docs",
		Operation:    OperationRead,
		RequireGraph: true,
	}

	if !adapter.CanServe(req, route) {
		t.Fatal("expected serverless adapter to serve graph read")
	}
	if _, err := adapter.BaseURL(req, route); err != nil {
		t.Fatalf("expected serverless base URL, got: %v", err)
	}
}

func TestServerlessBackendAdapterUsesAPIURLForWrites(t *testing.T) {
	adapter := ServerlessBackendAdapter{}
	route := NamespaceRoute{
		Tenant:             "acme",
		Table:              "docs",
		Namespace:          "docs",
		AllowServerless:    true,
		ServerlessQueryURL: "http://serverless-query.default.svc:8080",
		ServerlessAPIURL:   "http://serverless-api.default.svc:8080",
	}
	req := RequestContext{
		Tenant:    "acme",
		Table:     "docs",
		Operation: OperationWrite,
	}

	baseURL, err := adapter.BaseURL(req, route)
	if err != nil {
		t.Fatalf("expected serverless api base URL, got: %v", err)
	}
	if baseURL != "http://serverless-api.default.svc:8080" {
		t.Fatalf("got base URL %q", baseURL)
	}
}

func TestBackendRewriteRequest(t *testing.T) {
	requiredVersion := new(uint64)
	*requiredVersion = 42

	tests := []struct {
		name    string
		adapter BackendAdapter
		path    string
		want    string
		query   string
	}{
		{
			name:    "stateful keeps stripped proxy path",
			adapter: StatefulBackendAdapter{},
			path:    "/proxy/query/search",
			want:    "/query/search",
			query:   "max_lag_records=12&required_version=42&view=published",
		},
		{
			name:    "serverless prefixes namespace for query paths",
			adapter: ServerlessBackendAdapter{},
			path:    "/proxy/query/search",
			want:    "/tables/docs/query/search",
			query:   "max_lag_records=12&required_version=42&view=published",
		},
		{
			name:    "serverless maps graph paths under query",
			adapter: ServerlessBackendAdapter{},
			path:    "/proxy/graph/neighbors",
			want:    "/tables/docs/query/graph/neighbors",
			query:   "max_lag_records=12&required_version=42&view=published",
		},
		{
			name:    "serverless maps version reads to internal namespace",
			adapter: ServerlessBackendAdapter{},
			path:    "/proxy/versions/7/search",
			want:    "/_internal/namespaces/docs-serving/query/versions/7/query/search",
			query:   "max_lag_records=12&required_version=42&view=published",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inbound, _ := http.NewRequest(http.MethodGet, "http://proxy.local"+tt.path, nil)
			outReq, _ := http.NewRequest(http.MethodGet, "http://backend.local", nil)
			err := tt.adapter.RewriteRequest(outReq, inbound, RequestContext{
				Tenant:    "t1",
				Table:     "docs",
				Namespace: "docs-serving",
				Operation: OperationRead,
				Policy: RequestPolicy{
					View:            ViewPublished,
					MaxLagRecords:   12,
					RequiredVersion: requiredVersion,
				},
			}, NamespaceRoute{
				Tenant:    "t1",
				Table:     "docs",
				Namespace: "docs-serving",
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if outReq.URL.Path != tt.want {
				t.Fatalf("got path %q want %q", outReq.URL.Path, tt.want)
			}
			if outReq.URL.RawQuery != tt.query {
				t.Fatalf("got query %q want %q", outReq.URL.RawQuery, tt.query)
			}
			if outReq.Header.Get("X-Antfly-Required-Version") != "42" {
				t.Fatalf("missing required version header")
			}
		})
	}
}
