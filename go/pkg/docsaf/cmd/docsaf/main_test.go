/*
Copyright 2026 The Antfly Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCreateTableWithIndexesReturnsNonConflictError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "invalid create table request", http.StatusBadRequest)
	}))
	defer server.Close()

	err := createTableWithIndexes(context.Background(), server.URL, "", nil, "docs", 1, map[string]any{})
	if err == nil {
		t.Fatal("createTableWithIndexes error = nil, want create-table error")
	}
	if !strings.Contains(err.Error(), "HTTP 400") || !strings.Contains(err.Error(), "invalid create table request") {
		t.Fatalf("createTableWithIndexes error = %q, want status and response body", err)
	}
}
