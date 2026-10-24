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
	"context"
	"encoding/json"
	"fmt"
	"github.com/gofrs/flock"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

const testSourceID = "11111111-1111-4111-8111-111111111111"

type fixtureConnector struct {
	inventory    Inventory
	data         map[string][]byte
	enumerations int
	absenceErr   error
}

func (c *fixtureConnector) Enumerate(context.Context, Request) (Inventory, error) {
	c.enumerations++
	return c.inventory, nil
}
func (c *fixtureConnector) Read(_ context.Context, _ Request, o Item) ([]byte, error) {
	return c.data[o.ID], nil
}
func (c *fixtureConnector) CheckAccess(context.Context, Request, Item) error            { return nil }
func (c *fixtureConnector) ValidateInventory(context.Context, Request, Inventory) error { return nil }
func (c *fixtureConnector) ValidateAbsentSources(context.Context, Request, []string) error {
	return c.absenceErr
}

type publicationFixture struct {
	mu              sync.Mutex
	records         map[string]map[string]any
	failAfterWrite  bool
	deletes         int
	selectedHeaders []string
}

func (f *publicationFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if r.Header.Get("Authorization") != "Bearer database-test-token" {
		http.Error(w, "denied", 403)
		return
	}
	if r.Method == "GET" {
		fmt.Fprint(w, `{"name":"sources"}`)
		return
	}
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid", 400)
		return
	}
	if strings.HasSuffix(r.URL.Path, "/batch") {
		if body["sync_level"] != "write" {
			http.Error(w, "missing write barrier", 400)
			return
		}
		if inserts, ok := body["inserts"].(map[string]any); ok {
			for id, raw := range inserts {
				f.records[id] = raw.(map[string]any)
			}
		}
		if deletes, ok := body["deletes"].([]any); ok {
			for _, id := range deletes {
				delete(f.records, id.(string))
				f.deletes++
			}
		}
		if f.failAfterWrite {
			f.failAfterWrite = false
			http.Error(w, "write ack lost", 500)
			return
		}
		fmt.Fprint(w, `{"inserted":1}`)
		return
	}
	if !strings.HasSuffix(r.URL.Path, "/query") {
		http.NotFound(w, r)
		return
	}
	f.selectedHeaders = append(f.selectedHeaders, r.Header.Get("X-Antfly-Source-ID"))
	selected := []string{}
	if q, ok := body["query"].(map[string]any); ok {
		if ids, ok := q["ids"].([]any); ok {
			for _, id := range ids {
				selected = append(selected, id.(string))
			}
		}
	}
	filterBytes, _ := json.Marshal(body["filter_query"])
	if len(selected) == 0 {
		for id, record := range f.records {
			if strings.Contains(string(filterBytes), `"field":"source_id"`) && !strings.Contains(string(filterBytes), record["source_id"].(string)) {
				continue
			}
			selected = append(selected, id)
		}
	}
	sort.Strings(selected)
	var after string
	if cursor, ok := body["search_after"].([]any); ok && len(cursor) > 0 {
		after, _ = cursor[0].(string)
	}
	limit := 128
	if n, ok := body["limit"].(float64); ok {
		limit = int(n)
	}
	hits := []any{}
	for _, id := range selected {
		if id <= after {
			continue
		}
		record, ok := f.records[id]
		if !ok {
			continue
		}
		if len(hits) >= limit {
			break
		}
		hits = append(hits, map[string]any{"_id": id, "_sort": []string{id}, "_source": record})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"responses": []any{map[string]any{"status": 200, "remote_snapshot": "snapshot-1", "hits": map[string]any{"hits": hits}}}})
}
func runnerFixture(t *testing.T) (*Runner, *fixtureConnector, *publicationFixture) {
	t.Helper()
	root := t.TempDir()
	backend := &publicationFixture{records: map[string]map[string]any{}}
	server := httptest.NewServer(backend)
	t.Cleanup(server.Close)
	id := "s3://job/" + testSourceID + "/docs/manual.txt"
	connector := &fixtureConnector{inventory: Inventory{Authoritative: true, Objects: []Item{{ID: id, Path: "manual.txt", Filename: "manual.txt", Revision: "revision-1", URL: "s3://docs/manual.txt", SourceKind: ProviderS3, Size: 13}}}, data: map[string][]byte{id: []byte("retry budget\n")}}
	runner := &Runner{Profile: Profile{Version: 1, Endpoint: server.URL + "/api/v1", Principals: []string{"user:self"}, Connections: map[string]Connection{"docs": {Provider: ProviderS3}}, Sources: []Source{{ID: testSourceID, Alias: "manuals", Connection: "docs", Table: "sources", Config: json.RawMessage(`{"bucket":"docs"}`), Permissions: &Permissions{Readers: []string{"user:self"}}}}}, StateDir: filepath.Join(root, "state"), CatalogPath: filepath.Join(root, "catalog.json"), Factory: func(context.Context, Connection) (Connector, error) { return connector, nil }, Publisher: &Publisher{Endpoint: server.URL + "/api/v1", Token: "database-test-token", Client: server.Client()}}
	return runner, connector, backend
}
func TestRunnerRecoversLostAcknowledgementAndRetiresOnlyOwnedRows(t *testing.T) {
	runner, connector, backend := runnerFixture(t)
	backend.failAfterWrite = true
	if err := runner.Sync(t.Context()); err == nil {
		t.Fatal("expected interrupted publication")
	}
	var catalog Catalog
	if err := loadJSON(runner.CatalogPath, &catalog, 32<<20); err != nil {
		t.Fatal(err)
	}
	if catalog.Sources[0].Status != "stale" {
		t.Fatal("failed sync must not remain searchable")
	}
	backend.mu.Lock()
	backend.records["unowned-application-record"] = map[string]any{"id": "unowned-application-record", "source_id": "unowned"}
	backend.mu.Unlock()
	connector.inventory.Objects = nil
	runner.Restart = true
	if err := runner.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if len(backend.records) != 1 || backend.records["unowned-application-record"] == nil {
		t.Fatalf("unexpected reconciliation: %v", backend.records)
	}
	if backend.deletes != 1 {
		t.Fatal("lost acknowledgement must not lose ownership of the attempted write")
	}
}
func TestRunnerRejectsIncompleteAbsenceAndRetriesPinnedSelection(t *testing.T) {
	runner, connector, backend := runnerFixture(t)
	if err := runner.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	connector.inventory.Objects = nil
	connector.absenceErr = ErrInventoryIncomplete
	if err := runner.Sync(t.Context()); err == nil {
		t.Fatal("expected incomplete inventory failure")
	}
	backend.mu.Lock()
	if len(backend.records) != 1 || backend.deletes != 0 {
		t.Fatal("incomplete inventory retired prior data")
	}
	backend.mu.Unlock()
	connector.absenceErr = nil
	if err := runner.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if connector.enumerations != 2 {
		t.Fatal("retry must use its pinned enumeration")
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if len(backend.records) != 0 {
		t.Fatal("confirmed empty inventory should retire owned data")
	}
}
func TestRunnerCatalogNativeGrepWithoutCloud(t *testing.T) {
	binary := os.Getenv("ANTFLY_GREP_TEST_BINARY")
	if binary == "" {
		t.Skip("set ANTFLY_GREP_TEST_BINARY for the native self-hosted contract")
	}
	runner, _, _ := runnerFixture(t)
	if err := runner.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), binary, "--source-catalog", runner.CatalogPath, "--source", "manuals", "retry budget")
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "ANTFLY_GREP_") && !strings.HasPrefix(entry, "RIPGREP_CONFIG_PATH=") {
			command.Env = append(command.Env, entry)
		}
	}
	command.Env = append(command.Env, "ANTFLY_GREP_CONFIG_PATH=", "RIPGREP_CONFIG_PATH=", "ANTFLY_GREP_TABLE_TOKEN=database-test-token", "PATH="+t.TempDir())
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("native self-hosted search: %v: %s", err, output)
	}
	if !strings.Contains(string(output), "retry budget") {
		t.Fatalf("missing normalized text: %s", output)
	}
	data, err := os.ReadFile(runner.CatalogPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "database-test-token") {
		t.Fatal("catalog persisted a data-plane credential")
	}
}
func TestRunnerRejectsDuplicateInventoryBeforePublication(t *testing.T) {
	runner, connector, backend := runnerFixture(t)
	connector.inventory.Objects = append(connector.inventory.Objects, connector.inventory.Objects[0])
	if err := runner.Sync(t.Context()); err == nil {
		t.Fatal("duplicate identities accepted")
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if len(backend.records) != 0 {
		t.Fatal("invalid inventory published records")
	}
}
func TestOwnershipJournalRecoversInterruptedAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owned")
	id := "s3://job/" + testSourceID + "/docs/manual.txt"
	if err := journalOwnership(path, ownershipEvent{ID: id, Revision: "one"}); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.WriteString(`{"ID":"torn`)
	_ = file.Close()
	owned := map[string]string{}
	if err := replayOwnership(path, "job/"+testSourceID, owned); err != nil {
		t.Fatal(err)
	}
	if owned[id] != "one" {
		t.Fatal("lost durable ownership")
	}
}
func TestRunnerStateLockHonorsCancellation(t *testing.T) {
	runner, _, _ := runnerFixture(t)
	if err := os.MkdirAll(runner.StateDir, 0700); err != nil {
		t.Fatal(err)
	}
	lock := flock.New(filepath.Join(runner.StateDir, "runner.lock"))
	if err := lock.Lock(); err != nil {
		t.Fatal(err)
	}
	defer lock.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if err := runner.Sync(ctx); err == nil {
		t.Fatal("concurrent runner bypassed state lock")
	}
}

func TestDeletionReadinessWaitsForFreshSnapshot(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		hits := []any{}
		if calls == 1 {
			hits = append(hits, map[string]any{"_id": "retired"})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"responses": []any{map[string]any{"status": 200, "remote_snapshot": "fresh", "hits": map[string]any{"hits": hits}}}})
	}))
	defer server.Close()
	publisher := Publisher{Endpoint: server.URL}
	if err := publisher.ReadyAbsent(t.Context(), "sources", []string{"retired"}); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("expected a new snapshot after stale deletion, got %d queries", calls)
	}
}
