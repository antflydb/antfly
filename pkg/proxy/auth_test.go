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
	"net/http/httptest"
	"testing"
)

func TestStaticBearerAuthenticator(t *testing.T) {
	auth := StaticBearerAuthenticator{
		Required: true,
		Tokens: map[string]Principal{
			"good-token": {Subject: "user-1", Tenant: "t1", AllowedTables: []string{"docs"}, AllowedOperations: []OperationKind{OperationRead}},
		},
	}

	req := httptest.NewRequest("GET", "/proxy/query/search", nil)
	req.Header.Set("Authorization", "Bearer good-token")

	principal, err := auth.Authenticate(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if principal.Tenant != "t1" {
		t.Fatalf("got principal tenant %q", principal.Tenant)
	}
	if len(principal.AllowedTables) != 1 || principal.AllowedTables[0] != "docs" {
		t.Fatalf("unexpected table scope: %+v", principal.AllowedTables)
	}
}

func TestTenantAuthorizer(t *testing.T) {
	authz := TenantAuthorizer{}
	err := authz.Authorize(&Principal{
		Subject:           "user-1",
		Tenant:            "t1",
		AllowedTables:     []string{"docs"},
		AllowedOperations: []OperationKind{OperationRead},
	}, RequestContext{
		Tenant:    "t1",
		Table:     "docs",
		Operation: OperationRead,
	}, NamespaceRoute{
		Tenant:    "t1",
		Table:     "docs",
		Namespace: "docs-serving",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTenantAuthorizerRejectsNamespaceOrOperationOutsideScope(t *testing.T) {
	authz := TenantAuthorizer{}
	principal := &Principal{
		Subject:           "user-1",
		Tenant:            "t1",
		AllowedTables:     []string{"docs"},
		AllowedOperations: []OperationKind{OperationRead},
	}

	if err := authz.Authorize(principal, RequestContext{
		Tenant:    "t1",
		Table:     "analytics",
		Operation: OperationRead,
	}, NamespaceRoute{
		Tenant:    "t1",
		Table:     "analytics",
		Namespace: "analytics-serving",
	}); err == nil {
		t.Fatal("expected table authorization error")
	}

	if err := authz.Authorize(principal, RequestContext{
		Tenant:    "t1",
		Table:     "docs",
		Operation: OperationWrite,
	}, NamespaceRoute{
		Tenant:    "t1",
		Table:     "docs",
		Namespace: "docs-serving",
	}); err == nil {
		t.Fatal("expected operation authorization error")
	}
}
