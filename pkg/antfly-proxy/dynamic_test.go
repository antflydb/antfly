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
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileCatalogReloadsRoutes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "routes.json")
	if err := os.WriteFile(path, []byte(`[{"tenant":"t1","table":"docs","serving_namespace":"docs-serving","allow_serverless":true,"serverless_query_url":"http://serverless-query-a","serverless_api_url":"http://serverless-api-a"}]`), 0o600); err != nil {
		t.Fatalf("write routes: %v", err)
	}

	catalog := NewFileCatalog(path)
	route, err := catalog.ResolveRoute(context.Background(), "t1", "docs")
	if err != nil {
		t.Fatalf("resolve initial route: %v", err)
	}
	if route.ServerlessQueryURL != "http://serverless-query-a" || route.ServerlessAPIURL != "http://serverless-api-a" {
		t.Fatalf("got serverless URLs query=%q api=%q", route.ServerlessQueryURL, route.ServerlessAPIURL)
	}

	time.Sleep(1100 * time.Millisecond)
	if err := os.WriteFile(path, []byte(`[{"tenant":"t1","table":"docs","serving_namespace":"docs-serving","allow_serverless":true,"serverless_query_url":"http://serverless-query-b","serverless_api_url":"http://serverless-api-b"}]`), 0o600); err != nil {
		t.Fatalf("rewrite routes: %v", err)
	}

	route, err = catalog.ResolveRoute(context.Background(), "t1", "docs")
	if err != nil {
		t.Fatalf("resolve reloaded route: %v", err)
	}
	if route.ServerlessQueryURL != "http://serverless-query-b" || route.ServerlessAPIURL != "http://serverless-api-b" {
		t.Fatalf("got reloaded serverless URLs query=%q api=%q", route.ServerlessQueryURL, route.ServerlessAPIURL)
	}
}

func TestReloadingBearerAuthenticatorReloadsTokens(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bearer_tokens.json")
	if err := os.WriteFile(path, []byte(`{"token-a":{"subject":"user-a","tenant":"t1","admin":false}}`), 0o600); err != nil {
		t.Fatalf("write tokens: %v", err)
	}

	authenticator := &ReloadingBearerAuthenticator{
		Required: true,
		Path:     path,
	}

	req, err := http.NewRequest(http.MethodGet, "http://example.com", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer token-a")

	principal, err := authenticator.Authenticate(req)
	if err != nil {
		t.Fatalf("authenticate initial token: %v", err)
	}
	if principal.Subject != "user-a" {
		t.Fatalf("got subject %q", principal.Subject)
	}

	time.Sleep(1100 * time.Millisecond)
	if err := os.WriteFile(path, []byte(`{"token-b":{"subject":"user-b","tenant":"t2","admin":true}}`), 0o600); err != nil {
		t.Fatalf("rewrite tokens: %v", err)
	}

	req.Header.Set("Authorization", "Bearer token-b")
	principal, err = authenticator.Authenticate(req)
	if err != nil {
		t.Fatalf("authenticate reloaded token: %v", err)
	}
	if principal.Subject != "user-b" || principal.Tenant != "t2" || !principal.Admin {
		t.Fatalf("unexpected principal: %+v", principal)
	}
}
