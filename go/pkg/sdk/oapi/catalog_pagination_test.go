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

import "testing"

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
