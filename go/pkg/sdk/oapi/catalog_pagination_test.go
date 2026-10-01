// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package oapi

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestCatalogPaginationOptionalQueryParameters(t *testing.T) {
	unpaged, err := NewListTablesRequest("http://localhost", &ListTablesParams{Prefix: "events"})
	if err != nil {
		t.Fatal(err)
	}
	if unpaged.URL.Query().Has("limit") || unpaged.URL.Query().Has("cursor") {
		t.Fatalf("unpaged request unexpectedly opts into pagination: %s", unpaged.URL)
	}
	limit := int32(25)
	cursor := "opaque-token"
	paged, err := NewListNamespaceTablesRequest("http://localhost", "tenant", "public", &ListNamespaceTablesParams{Limit: &limit, Cursor: &cursor})
	if err != nil {
		t.Fatal(err)
	}
	if paged.URL.Query().Get("limit") != "25" || paged.URL.Query().Get("cursor") != cursor {
		t.Fatalf("pagination parameters lost: %s", paged.URL)
	}
}

func TestCatalogPaginationConflictResponse(t *testing.T) {
	response := func() *http.Response {
		return &http.Response{
			StatusCode: 409,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":"catalog changed; restart pagination"}`)),
		}
	}
	unscoped, err := ParseListTablesResponse(response())
	if err != nil {
		t.Fatal(err)
	}
	if unscoped.JSON409 == nil || unscoped.JSON409.Error != "catalog changed; restart pagination" {
		t.Fatalf("missing conflict: %#v", unscoped)
	}
	scoped, err := ParseListNamespaceTablesResponse(response())
	if err != nil {
		t.Fatal(err)
	}
	if scoped.JSON409 == nil || scoped.JSON409.Error != "catalog changed; restart pagination" {
		t.Fatalf("missing scoped conflict: %#v", scoped)
	}
}

func TestCatalogMutationVisibilityAndErrorResponses(t *testing.T) {
	response := func(status int, body string) *http.Response {
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}
	}
	pending, err := ParseCreateDatabaseResponse(response(202, `{"status":"committed_visibility_pending"}`))
	if err != nil {
		t.Fatal(err)
	}
	if pending.JSON202 == nil || string(pending.JSON202.Status) != "committed_visibility_pending" {
		t.Fatalf("missing committed outcome: %#v", pending)
	}
	missing, err := ParseGetDatabaseResponse(response(404, `{"error":"CatalogNotFound","code":"CatalogNotFound"}`))
	if err != nil {
		t.Fatal(err)
	}
	if missing.JSON404 == nil || missing.JSON404.Error != "CatalogNotFound" || missing.JSON404.Code != "CatalogNotFound" {
		t.Fatalf("missing catalog error: %#v", missing)
	}
}
