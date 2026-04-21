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

func TestLoadGatewayEnvConfig(t *testing.T) {
	getenv := func(key string) string {
		switch key {
		case "ANTFLY_PROXY_ROUTES_JSON":
			return `[{"tenant":"t1","table":"docs","serving_namespace":"docs-serving","preferred_backend":"serverless","allow_stateful":true,"allow_serverless":true,"stateful_url":"http://stateful","serverless_query_url":"http://serverless-query","serverless_api_url":"http://serverless-api"}]`
		case "ANTFLY_PROXY_REQUIRE_AUTH":
			return "true"
		case "ANTFLY_PROXY_PUBLIC_ADDR":
			return ":9090"
		case "ANTFLY_PROXY_BEARER_TOKENS_JSON":
			return `{"token-1":{"subject":"user-1","tenant":"t1","admin":false,"tables":["docs"],"operations":["read"]}}`
		default:
			return ""
		}
	}

	cfg, err := LoadGatewayEnvConfig(getenv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.PublicAddr != ":9090" {
		t.Fatalf("got addr %q", cfg.PublicAddr)
	}
	if !cfg.RequireAuth {
		t.Fatal("expected auth to be required")
	}
	if len(cfg.Routes) != 1 {
		t.Fatalf("got %d routes", len(cfg.Routes))
	}
	if cfg.Tokens["token-1"].Tenant != "t1" {
		t.Fatalf("unexpected token tenant")
	}
	if len(cfg.Tokens["token-1"].AllowedTables) != 1 || cfg.Tokens["token-1"].AllowedTables[0] != "docs" {
		t.Fatalf("unexpected table scope: %+v", cfg.Tokens["token-1"].AllowedTables)
	}
}

func TestNewGatewayFromEnv(t *testing.T) {
	getenv := func(key string) string {
		switch key {
		case "ANTFLY_PROXY_ROUTES_JSON":
			return `[{"tenant":"t1","table":"docs","serving_namespace":"docs-serving","allow_serverless":true,"serverless_query_url":"http://serverless-query","serverless_api_url":"http://serverless-api"}]`
		default:
			return ""
		}
	}

	gateway, err := NewGatewayFromEnv(getenv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resolved, err := gateway.Resolve(context.Background(), RequestContext{
		Tenant:       "t1",
		Table:        "docs",
		Operation:    OperationRead,
		RequireGraph: true,
	})
	if err != nil {
		t.Fatalf("unexpected resolve error: %v", err)
	}
	if resolved.Backend != BackendServerless {
		t.Fatalf("got backend %q", resolved.Backend)
	}
}
