// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

package sources

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
)

func TestRunnerRetiresCatalogWithoutPurgingContent(t *testing.T) {
	runner, _, backend := runnerFixture(t)
	if err := runner.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	runner.Profile.Sources = nil
	if err := runner.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if len(backend.catalogRecords) != 0 || len(backend.records) != 1 {
		t.Fatal("profile removal must retire only its catalog binding")
	}
}
func TestCatalogRejectsIdentityFilterMismatchAndUncommittedBatch(t *testing.T) {
	id := testSourceID
	record := NewCatalogRecord(Binding{ID: id, Table: "docs", DocumentField: "grep", Filter: SourceFilter("other"), Status: "syncing"}, ProviderS3, "", Permissions{Public: true})
	if record.Validate() == nil {
		t.Fatal("catalog identity must bind the selection filter")
	}
	for _, body := range []string{`{"status":"committed_pending"}`, `{"failed":[{"id":"source","error":"rejected"}]}`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }))
		publisher := Publisher{Endpoint: server.URL}
		if err := publisher.Batch(t.Context(), "docs", map[string]any{"source": record}, nil); err == nil {
			t.Fatal("uncommitted publication was accepted")
		}
		server.Close()
	}
}
func TestNativeCatalogPaginationIsCompleteAndSnapshotBound(t *testing.T) {
	binary := os.Getenv("ANTFLY_GREP_TEST_BINARY")
	if binary == "" {
		t.Skip("set ANTFLY_GREP_TEST_BINARY")
	}
	runner, _, backend := runnerFixture(t)
	if err := runner.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	original, _ := json.Marshal(backend.catalogRecords[testSourceID])
	for i := 0; i < 70; i++ {
		var record CatalogRecord
		_ = json.Unmarshal(original, &record)
		id := fmt.Sprintf("22222222-2222-4222-8222-%012d", i)
		record.Source.ID = id
		record.Source.Alias = fmt.Sprint("other-", i)
		record.Source.Filter = SourceFilter(id)
		raw, _ := json.Marshal(record)
		var value map[string]any
		_ = json.Unmarshal(raw, &value)
		backend.catalogRecords[id] = value
	}
	backend.mu.Unlock()
	for _, scenario := range []string{"complete", "changed", "failed"} {
		t.Run(scenario, func(t *testing.T) {
			var contentQueries, catalogQueries atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				isCatalog := strings.Contains(r.URL.Path, "/tables/"+DefaultCatalogTable+"/")
				if !isCatalog {
					contentQueries.Add(1)
					backend.ServeHTTP(w, r)
					return
				}
				catalogQueries.Add(1)
				var body map[string]any
				_ = json.NewDecoder(r.Body).Decode(&body)
				raw, _ := json.Marshal(body)
				r.Body = io.NopCloser(bytes.NewReader(raw))
				if body["remote_snapshot"] != nil && scenario == "failed" {
					http.Error(w, "failed", 503)
					return
				}
				recorder := httptest.NewRecorder()
				backend.ServeHTTP(recorder, r)
				if body["remote_snapshot"] != nil && scenario == "changed" {
					var envelope map[string]any
					_ = json.Unmarshal(recorder.Body.Bytes(), &envelope)
					envelope["responses"].([]any)[0].(map[string]any)["remote_snapshot"] = "changed"
					_ = json.NewEncoder(w).Encode(envelope)
					return
				}
				w.WriteHeader(recorder.Code)
				_, _ = w.Write(recorder.Body.Bytes())
			}))
			defer server.Close()
			command := exec.CommandContext(t.Context(), binary, "--source-endpoint", server.URL+"/api/v1", "--source-principal", "user:self", "--source", "manuals", "retry budget")
			for _, entry := range os.Environ() {
				if !strings.HasPrefix(entry, "ANTFLY_GREP_") && !strings.HasPrefix(entry, "RIPGREP_CONFIG_PATH=") {
					command.Env = append(command.Env, entry)
				}
			}
			command.Env = append(command.Env, "ANTFLY_GREP_CONFIG_PATH=", "RIPGREP_CONFIG_PATH=", "ANTFLY_GREP_TABLE_TOKEN=database-test-token", "PATH="+t.TempDir())
			output, err := command.CombinedOutput()
			if scenario == "complete" {
				if err != nil || !strings.Contains(string(output), "retry budget") {
					t.Fatalf("complete traversal: %v: %s", err, output)
				}
				if catalogQueries.Load() != 3 {
					t.Fatalf("expected 2 discovery pages plus a publication check, got %d", catalogQueries.Load())
				}
			} else {
				if err == nil || contentQueries.Load() != 0 {
					t.Fatalf("partial/changed discovery was used: %v %s", err, output)
				}
			}
		})
	}
}

func TestPublisherCreatesCatalogThroughExistingTableRoute(t *testing.T) {
	created := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/db/v1/tables/antfly_sources" {
			t.Errorf("unexpected table API route %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if r.Method == http.MethodGet {
			if !created {
				http.NotFound(w, r)
				return
			}
			fmt.Fprint(w, `{}`)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "unsupported", 405)
			return
		}
		created = true
		fmt.Fprint(w, `{}`)
	}))
	defer server.Close()
	publisher := Publisher{Endpoint: server.URL + "/db/v1"}
	if err := publisher.EnsureTable(t.Context(), DefaultCatalogTable); err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("catalog table was not created")
	}
}
