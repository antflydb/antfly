// Copyright 2025 Antfly, Inc.
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
	"testing"
)

func TestRouterResolve(t *testing.T) {
	router := NewRouter([]NamespaceRoute{
		{
			Tenant:             "t1",
			Table:              "docs",
			Namespace:          "docs-serving",
			PreferredBackend:   BackendServerless,
			AllowStateful:      true,
			AllowServerless:    true,
			StatefulURL:        "http://stateful.default.svc:8080",
			ServerlessQueryURL: "http://serverless-query.default.svc:8080",
			ServerlessAPIURL:   "http://serverless-api.default.svc:8080",
		},
		{
			Tenant:           "t1",
			Table:            "system",
			Namespace:        "system",
			PreferredBackend: BackendStateful,
			AllowStateful:    true,
			AllowServerless:  false,
			StatefulURL:      "http://stateful.default.svc:8080",
		},
		{
			Tenant:             "t1",
			Table:              "logs",
			Namespace:          "logs-serving",
			PreferredBackend:   BackendServerless,
			AllowStateful:      false,
			AllowServerless:    true,
			ServerlessQueryURL: "http://serverless-query.default.svc:8080",
			ServerlessAPIURL:   "http://serverless-api.default.svc:8080",
		},
	})

	tests := []struct {
		name string
		req  RequestContext
		want BackendKind
		err  bool
	}{
		{
			name: "writes prefer stateful when available",
			req: RequestContext{
				Tenant:    "t1",
				Table:     "docs",
				Operation: OperationWrite,
			},
			want: BackendStateful,
		},
		{
			name: "writes use serverless api when stateful is unavailable",
			req: RequestContext{
				Tenant:    "t1",
				Table:     "logs",
				Operation: OperationWrite,
			},
			want: BackendServerless,
		},
		{
			name: "graph reads prefer serverless when allowed",
			req: RequestContext{
				Tenant:       "t1",
				Table:        "docs",
				Operation:    OperationRead,
				RequireGraph: true,
			},
			want: BackendServerless,
		},
		{
			name: "latest reads prefer serverless when allowed",
			req: RequestContext{
				Tenant:      "t1",
				Table:       "docs",
				Operation:   OperationRead,
				AllowLatest: true,
			},
			want: BackendServerless,
		},
		{
			name: "stateful only namespace stays stateful",
			req: RequestContext{
				Tenant:    "t1",
				Table:     "system",
				Operation: OperationRead,
			},
			want: BackendStateful,
		},
		{
			name: "missing route errors",
			req: RequestContext{
				Tenant:    "t1",
				Table:     "missing",
				Operation: OperationRead,
			},
			err: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := router.Resolve(context.Background(), tt.req)
			if tt.err {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}

func TestRouterResolveBackend(t *testing.T) {
	router := NewRouter([]NamespaceRoute{
		{
			Tenant:             "t1",
			Table:              "docs",
			Namespace:          "docs-serving",
			AllowServerless:    true,
			ServerlessQueryURL: "http://serverless-query.default.svc:8080",
			ServerlessAPIURL:   "http://serverless-api.default.svc:8080",
		},
	})

	kind, adapter, route, err := router.ResolveBackend(context.Background(), RequestContext{
		Tenant:       "t1",
		Table:        "docs",
		Operation:    OperationRead,
		RequireGraph: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kind != BackendServerless {
		t.Fatalf("got %q want %q", kind, BackendServerless)
	}
	if adapter == nil || adapter.Kind() != BackendServerless {
		t.Fatalf("expected serverless adapter, got %#v", adapter)
	}
	if route.Table != "docs" || route.Namespace != "docs-serving" {
		t.Fatalf("got route %+v", route)
	}
}
